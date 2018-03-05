package net.freastro.b2fys;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import jnr.constants.platform.Errno;

class MBuf {

    private static final Logger log = LogManager.getLogger(MBuf.class);

    BufferPool pool;
    List<ByteBuffer> buffers;
    int rbuf;
    int wbuf;
    int rp;
    int wp;

    private MBuf() {

    }

    static MBuf init(BufferPool h, long size, boolean block) {
        MBuf mb = new MBuf();
        mb.pool = h;

        if (size != 0) {
            mb.buffers = h.requestMultiple(size, block);
            if (mb.buffers == null || mb.buffers.isEmpty()) {
                return null;
            }
        }

        return mb;
    }

    int read(ByteBuffer p, AtomicInteger err) {
        int n = 0;

        if (rbuf == wbuf && rp == wp) {
            err.set(-1);
            return n;
        }

        if (rp == buffers.get(rbuf).limit()) {
            rbuf++;
            rp = 0;
        }

        if (rbuf == buffers.size()) {
            err.set(-1);
            return n;
        } else if (rbuf > buffers.size()) {
            System.err.println("mb.cur > len(mb.buffers)");
            System.exit(1);
        }

        ByteBuffer r = buffers.get(rbuf);
        int oldLimit = r.limit();
        int oldPosition = r.position();
        r.limit(Math.min(r.position() + p.remaining(), oldLimit));
        p.put(r);
        r.limit(oldLimit);
        n = r.position() - oldPosition;
        rp += n;

        return n;
    }

    int writeFrom(InputStream r, AtomicInteger err) {
        int n = 0;

        ByteBuffer b = buffers.get(wbuf);

        if (wp == b.limit()) {
            if (wbuf + 1 == buffers.size()) {
                return n;
            }
            wbuf++;
            b = buffers.get(wbuf);
            wp = 0;
        } else if (wp > b.limit()) {
            System.err.println("mb.wp > cap(b)");
            System.exit(1);
        }

        n = read(b, r, err);
        wp += n;
        // resize the buffer to account for what we just read
        buffers.get(wbuf).flip();

        return n;
    }

    void free() {
        for (ByteBuffer b : buffers) {
            pool.free(b);
        }

        buffers = null;
    }

    static int read(ByteBuffer dst, InputStream src, AtomicInteger err) {
        byte[] buf = new byte[4096];
        int n = 0;
        int size;

        while (dst.remaining() > 0) {
            try {
                size = src.read(buf, 0, Math.min(dst.remaining(), buf.length));
            } catch (IOException e) {
                log.error(e);
                err.set(Errno.EIO.intValue());
                return n;
            }

            if (size == -1) {
                err.set(-1);
                return n;
            }

            dst.put(buf, 0, size);
            n += size;
        }

        return n;
    }
}
