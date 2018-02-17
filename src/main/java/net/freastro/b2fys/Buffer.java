package net.freastro.b2fys;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import jnr.constants.platform.Errno;

class Buffer {

    private static final Logger log = LogManager.getLogger(Buffer.class);

    private static final int ErrUnexpectedEOF = 1;

    Lock mu = new ReentrantLock();
    Condition cond;

    MBuf buf;
    InputStream reader;
    int err;

    private Buffer() {

    }

    static Buffer init(MBuf buf, ReaderProvider r) {
        Buffer b = new Buffer();

        b.buf = buf;
        b.cond = b.mu.newCondition();

        new Thread(() -> b.readLoop(r)).start();

        return b;
    }

    void readLoop(ReaderProvider r) {
        while (true) {
            mu.lock();
            if (reader == null) {
                AtomicInteger err = new AtomicInteger(0);
                reader = r.func(err);
                this.err = err.get();
                cond.signalAll();
                if (this.err != 0) {
                    mu.unlock();
                    break;
                }
            }

            if (buf == null) {
                // buffer was drained
                mu.unlock();
                break;
            }

            AtomicInteger err = new AtomicInteger(0);
            int nread = buf.writeFrom(reader, err);
            if (err.get() != 0) {
                this.err = err.get();
                mu.unlock();
                break;
            }
            log.debug("wrote {} into buffer", nread);

            if (nread == 0) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // ignored
                }
                mu.unlock();
                break;
            }

            mu.unlock();
            // if we got here we've read _something_, bounce this goroutine
            // to allow another one to read
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // ignored
            }
        }
        log.debug("<-- readLoop()");
    }

    int readFromStream(ByteBuffer p, AtomicInteger err) {
        int n = 0;

        log.debug("reading {} from stream", p.remaining());

        n = MBuf.read(p, reader, err);
        if (n != 0 && err.get() == ErrUnexpectedEOF) {
            err.set(0);
        } else {
            log.debug("read {} fream stream", n);
        }
        return n;
    }

    int read(ByteBuffer p, AtomicInteger err) {
        int n = 0;

        log.debug("Buffer.Read({})", p.remaining());

        mu.lock();

        while (reader == null && this.err == 0) {
            log.debug("waiting for stream");
            cond.awaitUninterruptibly();
            if (this.err != 0) {
                err.set(this.err);
                mu.unlock();
                return n;
            }
        }

        // we could have received the err before Read was called
        if (reader == null) {
            if (this.err == 0) {
                System.err.println("reader and err are both nil");
                System.exit(1);
            }
            err.set(this.err);
            return n;
        }

        if (buf != null) {
            log.debug("reading {} from buffer", p.remaining());

            n = buf.read(p, err);
            if (n == 0) {
                buf.free();
                buf = null;
                log.debug("drained buffer");
                n = readFromStream(p, err);
            } else {
                log.debug("read {} from buffer", n);
            }
        } else if (this.err != 0) {
            err.set(this.err);
        } else {
            n = readFromStream(p, err);
        }

        mu.unlock();
        return n;
    }

    void close(AtomicInteger err) {
        mu.lock();

        if (reader != null) {
            try {
                reader.close();
            } catch (final IOException e) {
                err.set(-Errno.EAGAIN.intValue());
            }
        }

        if (buf != null) {
            buf.free();
            buf = null;
        }

        mu.unlock();
    }
}
