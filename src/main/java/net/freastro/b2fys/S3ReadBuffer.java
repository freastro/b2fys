package net.freastro.b2fys;

import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2DownloadByNameRequest;
import com.backblaze.b2.util.B2ByteRange;

import sun.misc.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jnr.constants.platform.Errno;

class S3ReadBuffer {

    private static final int ErrUnexpectedEOF = 1;

    B2StorageClient b2;
    long offset;
    int size;
    Buffer buf;

    private S3ReadBuffer() {

    }

    static S3ReadBuffer init(FileHandle fh, long offset, int size) {
        S3ReadBuffer b = new S3ReadBuffer();
        B2FuseFilesystem fs = fh.inode.fs;
        b.b2 = fs.b2;
        b.offset = offset;
        b.size = size;

        MBuf mbuf = MBuf.init(fh.poolHandle, size, false);
        if (mbuf == null) {
            return null;
        }

        b.buf = Buffer.init(mbuf, (error) -> {
            String fileName = fs.key(fh.inode.fullName()).replace(" ", "%20");
            B2DownloadByNameRequest.Builder params = B2DownloadByNameRequest
                    .builder(fs.bucket.getBucketName(), fileName);

            B2ByteRange bytes = B2ByteRange.between(offset, offset + size - 1);
            params.setRange(bytes);

            AtomicReference<InputStream> body = new AtomicReference<>();
            try {
                fs.b2.downloadByName(params.build(), (response, in) -> {
                    byte[] buf = IOUtils.readFully(in, size, false);
                    body.set(new ByteArrayInputStream(buf));
                });
            } catch (B2Exception e) {
                error.set(-Errno.EIO.intValue());
                return null;
            }

            error.set(0);
            return body.get();
        });

        return b;
    }

    int read(long offset, ByteBuffer p, AtomicInteger err) {
        int n = 0;

        if (this.offset == offset) {
            n = readFull(this.buf, p, err);
            if (n != 0 && err.get() == ErrUnexpectedEOF) {
                err.set(0);
            }
            if (n > 0) {
                if (n > size) {
                    System.err.println("read more than available " + n + " " + size);
                    System.exit(1);
                }

                this.offset += n;
                size -= n;
            }

            return n;
        } else {
            System.err.println("not the right buffer, expecting " + this.offset + " got " +
                               offset + ", " + size + " left");
            System.exit(1);
            return n;
        }
    }

    /**
     * ReadAtLeast reads r into buf until it has read at least min bytes.
     * It returns the number of bytes copied and an error if fewer bytes were read.
     * The error is EOF only if no bytes were read.
     * If an EOF happens after reading fewer than min bytes,
     * ReadAtLeast returns ErrUnexpectedEOF.
     * If min is greater than the length of buf, ReadAtLeast returns ErrShortBuffer.
     * On return, n >= min if and only if err == nil.
     */
    private int readAtLeast(Buffer r, ByteBuffer buf, int min, AtomicInteger err) {
        int n = 0;

        if (buf.remaining() < min) {
            err.set(-Errno.EIO.intValue());
            return 0;
        }
        while (n < min && err.get() == 0) {
            int nn = 0;
            nn = r.read(buf, err);
            n += nn;
        }
        if (n >= min) {
            err.set(0);
        } else if (n > 0 && err.get() == -1) {
            err.set(ErrUnexpectedEOF);
        }
        return n;
    }

    /**
     * ReadFull reads exactly len(buf) bytes from r into buf.
     * It returns the number of bytes copied and an error if fewer bytes were read.
     * The error is EOF only if no bytes were read.
     * If an EOF happens after reading some but not all the bytes,
     * ReadFull returns ErrUnexpectedEOF.
     * On return, n == len(buf) if and only if err == nil.
     */
    private int readFull(Buffer r, ByteBuffer buf, AtomicInteger err) {
        return readAtLeast(r, buf, buf.remaining(), err);
    }
}
