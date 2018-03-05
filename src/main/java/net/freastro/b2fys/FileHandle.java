package net.freastro.b2fys;

import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2DownloadByNameRequest;
import com.backblaze.b2.util.B2ByteRange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import sun.awt.Mutex;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import jnr.constants.platform.Errno;

class FileHandle {

    private static final Logger log = LogManager.getLogger(FileHandle.class);

    private static final int MAX_READAHEAD = 100 * 1024 * 1024;
    private static final int READAHEAD_CHUNK = 20 * 1024 * 1024;

    Inode inode;

    String mpuKey;
    boolean dirty;
    Future writeInit;
    CountDownLatch mpuWG;
    String[] etags;

    Mutex mu = new Mutex();
    String mpuId;
    long nextWriteOffset;
    int lastPartId;

    BufferPool poolHandle;
    MBuf buf;

    int lastWriteError;

    // read
    InputStream reader;
    long readBufOffset;

    // parallel read
    List<S3ReadBuffer> buffers = new ArrayList<>();
    int existingReadahead;
    long seqReadAmount;
    long numOOORead; // number of out of order read

    FileHandle(Inode in) {
        this.inode = in;
    }

    int readFromReadAhead(long offset, ByteBuffer buf, AtomicInteger err) {
        int bytesRead = 0;

        int nread = 0;
        while (buffers != null && buffers.size() != 0) {
            nread = buffers.get(0).read(offset + bytesRead, buf, err);
            bytesRead += nread;
            if (err.get() != 0) {
                return bytesRead;
            }

            if (buffers.get(0).size == 0) {
                // we've exhausted the first buffer
                buffers.get(0).buf.close(new AtomicInteger());
                buffers.remove(0);
            }

//            buf = buf[nread:];

            if (buf.remaining() == 0) {
                // we've filled the user buffer
                return bytesRead;
            }
        }

        return bytesRead;
    }

    int readFile(long offset, ByteBuffer buf, AtomicInteger err) {
        int bytesRead = 0;
        inode.logFuse("ReadFile", offset, buf.remaining());

        mu.lock();

        int nwant = buf.remaining();
        int nread = 0;

        while (bytesRead < nwant && err.get() == 0) {
            nread = readFile2(offset + bytesRead, buf, err);
            if (nread > 0) {
                bytesRead += nread;
            }
        }

        inode.logFuse("< ReadFile", bytesRead, err);

        if (err.get() != 0) {
            if (err.get() == -1) {
                err.set(0);
            }
        }

        mu.unlock();
        return bytesRead;
    }

    void readAhead(long offset, int needAtLeast, AtomicInteger err) {
        existingReadahead = 0;
        for (S3ReadBuffer b : buffers) {
            existingReadahead += b.size;
        }

        int readAheadAmount = MAX_READAHEAD;

        while (readAheadAmount - existingReadahead >= READAHEAD_CHUNK) {
            long off = offset + existingReadahead;
            long remaining = inode.attributes.size - off;

            // only read up to readahead chunk each time
            int size = Math.min(readAheadAmount - existingReadahead, READAHEAD_CHUNK);
            // but don't read past the file
            size = (int) Math.min(size, remaining);

            if (size != 0) {
                inode.logFuse("readahead", off, size, existingReadahead);

                S3ReadBuffer readAheadBuf = S3ReadBuffer.init(this, off, size);
                if (readAheadBuf != null) {
                    buffers.add(readAheadBuf);
                    existingReadahead += size;
                } else {
                    if (existingReadahead != 0) {
                        // don't do more readahead now, but don't fail, cross our
                        // fingers that we will be able to allocate the buffers
                        // later
                        err.set(0);
                        return;
                    } else {
                        err.set(-Errno.ENOMEM.intValue());
                    }
                }
            }

            if (size != READAHEAD_CHUNK) {
                // that was the last remaining chunk to readahead
                break;
            }
        }
    }

    int readFile2(long offset, ByteBuffer buf, AtomicInteger err) {
        int bytesRead = 0;

        if (offset >= inode.attributes.size) {
            // nothing to read
            if (inode.invalid) {
                err.set(-Errno.ENOENT.intValue());
            } else if (inode.knownSize == 0) {
                err.set(-1);
            } else {
                err.set(-1);
            }
            return bytesRead;
        }

        B2FuseFilesystem fs = inode.fs;

        if (poolHandle == null) {
            poolHandle = fs.bufferPool;
        }

        if (readBufOffset != offset) {
            // XXX out of order read, maybe disable prefetching
            inode.logFuse("out of order read", offset, readBufOffset);

            readBufOffset = offset;
            seqReadAmount = 0;
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // ignored
                }
                reader = null;
            }

            if (buffers != null) {
                // we misdetected
                numOOORead++;
            }

            for (S3ReadBuffer b : buffers) {
                b.buf.close(new AtomicInteger());
            }
            buffers.clear();
        }

        if (!fs.flags.cheap && seqReadAmount >= READAHEAD_CHUNK && numOOORead < 3) {
            if (reader != null) {
                inode.logFuse("cutover to the parallel algorithm");
                try {
                    reader.close();
                } catch (IOException e) {
                    // ignored
                }
                reader = null;
            }

            readAhead(offset, buf.remaining(), err);
            if (err.get() == 0) {
                bytesRead = readFromReadAhead(offset, buf, err);
            } else {
                // fall back to read serially
                inode.logFuse("not enough memory, fallback to serial read");
                seqReadAmount = 0;
                for (S3ReadBuffer b : buffers) {
                    b.buf.close(new AtomicInteger());
                }
                buffers = null;

                bytesRead = readFromStream(offset, buf, err);
            }
        } else {
            bytesRead = readFromStream(offset, buf, err);
        }

        if (bytesRead > 0) {
            readBufOffset += bytesRead;
            seqReadAmount += bytesRead;
        }

        inode.logFuse("< readFile", bytesRead, err);
        return bytesRead;
    }

    void release() {
        // read buffers
        for (S3ReadBuffer b : buffers) {
            b.buf.close(new AtomicInteger());
        }
        buffers.clear();

        if (reader != null) {
            try {
                reader.close();
            } catch (final IOException e) {
                // ignored
            }
        }

        // write buffers
        if (poolHandle != null) {
            if (buf != null && buf.buffers != null) {
                if (lastWriteError == 0) {
                    log.error("buf not freed but error is nil");
                    System.exit(1);
                }

                buf.free();
                // the other in-flight multipart PUT buffers will be
                // freed when they finish/error out
            }
        }

        inode.mu.lock();

        assert inode.fileHandles != 0;

        inode.fileHandles -= 1;
        inode.mu.unlock();
    }

    int readFromStream(long offset, ByteBuffer buf, AtomicInteger err) {
        int bytesRead = 0;

        if (offset >= inode.attributes.size) {
            // nothing to read
            if (inode.fs.flags.debugFuse) {
                inode.logFuse("< readFromStream", bytesRead);
            }
            return bytesRead;
        }

        B2FuseFilesystem fs = inode.fs;

        if (reader == null) {
            String fileName = fs.key(inode.fullName());
            B2DownloadByNameRequest.Builder params = B2DownloadByNameRequest
                    .builder(fs.bucket.getBucketName(), fileName);

            if (offset != 0) {
                B2ByteRange bytes = B2ByteRange.between(offset, offset + buf.remaining() - 1);
                params.setRange(bytes);
            }

            try {
                reader = fs.b2sc.streamByName(params.build());
            } catch (B2Exception e) {
                err.set(-Errno.EIO.intValue());
                if (inode.fs.flags.debugFuse) {
                    inode.logFuse("< readFromStream", bytesRead);
                }
                return bytesRead;
            }
        }

        bytesRead = MBuf.read(buf, reader, err);
        if (err.get() != 0) {
            if (err.get() != -1) {
                inode.logFuse("< readFromStream error", bytesRead, err.get());
            }
            // always retry error on read
            try {
                reader.close();
            } catch (IOException e) {
                // ignored
            }
            reader = null;
            err.set(0);
        }

        if (inode.fs.flags.debugFuse) {
            inode.logFuse("< readFromStream", bytesRead);
        }
        return bytesRead;
    }
}
