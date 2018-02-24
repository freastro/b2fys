package net.freastro.b2fys;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

class BufferPool {

    private static final Logger log = LogManager.getLogger(BufferPool.class);

    static final int BUF_SIZE = 5 * 1024 * 1024;

    ReentrantLock mu = new ReentrantLock();
    Condition cond;

    long numBuffers;
    long maxBuffers;

    long totalBuffers;
    long computedMaxBuffers;

    ObjectPool pool;

    BufferPool init() {
        cond = mu.newCondition();

        computedMaxBuffers = maxBuffers;

        final GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setBlockWhenExhausted(false);
        poolConfig.setMaxTotal(-1);
        pool = new GenericObjectPool(new BasePooledObjectFactory() {
            @Override
            public Object create() {
                return ByteBuffer.allocate(BUF_SIZE);
            }

            @Override
            public PooledObject wrap(Object o) {
                return new DefaultPooledObject<>(o);
            }
        }, poolConfig);

        return this;
    }

    void recomputeBufferLimit() {
        if (maxBuffers == 0) {
            computedMaxBuffers = maxMemToUse(numBuffers);
            if (computedMaxBuffers == 0) {
                System.err.println("OOM");
                System.exit(1);
            }
        }
    }

    List<ByteBuffer> requestMultiple(long size, boolean block) {
        List<ByteBuffer> buffers = new ArrayList<>();

        int nPages = pages(size, BUF_SIZE);

        mu.lock();

        if (totalBuffers % 10 == 0) {
            recomputeBufferLimit();
        }

        while (numBuffers + nPages > computedMaxBuffers) {
            if (block) {
                if (numBuffers == 0) {
                    maybeGC();
                    recomputeBufferLimit();
                    if (numBuffers + nPages > computedMaxBuffers) {
                        // we don't have any in use buffers, and we've made attempts to
                        // free memory AND correct our limits, yet we still can't allocate.
                        // it's likely that we are simply asking for too much
                        log.error("Unable to allocate {} bytes, limit is {} bytes",
                                  nPages * BUF_SIZE, computedMaxBuffers * BUF_SIZE);
                        System.err.println("OOM");
                        System.exit(1);
                    }
                }
                cond.awaitUninterruptibly();
            } else {
                mu.unlock();
                return buffers;
            }
        }

        for (int i = 0; i < nPages; ++i) {
            numBuffers++;
            totalBuffers++;
            ByteBuffer buf = null;
            try {
                buf = (ByteBuffer) pool.borrowObject();
            } catch (Exception e) {
                System.err.println(e);
                System.exit(1);
            }
            buffers.add(buf);
        }

        mu.unlock();
        return buffers;
    }

    void maybeGC() {
        if (numBuffers == 0) {
            Runtime.getRuntime().gc();
        }
    }

    void free(ByteBuffer buf) {
        mu.lock();

        buf.clear();
        try {
            pool.returnObject(buf);
        } catch (Exception e) {
            System.err.println(e);
            System.exit(1);
        }
        numBuffers--;
        cond.signal();

        mu.unlock();
    }

    private static long maxMemToUse(long buffersNow) {
        Runtime m = Runtime.getRuntime();

        long available = m.maxMemory() - m.totalMemory() + m.freeMemory();
        log.debug("amount of available memory: {}", available / 1024 / 1024);

        long alloc = m.totalMemory() - m.freeMemory();

        log.debug("amount of allocated memory: {} ", alloc / 1024 / 1024);

        long max = available / 2;
        long maxbuffers = Math.max(max / BUF_SIZE, 1);
        log.debug("using up to {} {}MB buffers, now is {}", maxbuffers, BUF_SIZE / 1024 / 1024,
                  buffersNow);
        return maxbuffers;
    }

    private static int pages(long size, int pageSize) {
        return (int) ((size + (long) (pageSize) - 1) / (long) (pageSize));
    }
}
