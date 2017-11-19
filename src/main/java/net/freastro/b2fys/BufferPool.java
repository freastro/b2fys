package net.freastro.b2fys;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

import sun.awt.Mutex;
import sun.misc.ConditionLock;

class BufferPool {

    static final int BUF_SIZE = 5 * 1024 * 1024;

    Mutex mu;
    ConditionLock cond;

    long numBuffers;
    long maxBuffers;

    long totalBuffers;
    long computedMaxBuffers;

    ObjectPool pool;

    BufferPool init() {
        cond = new ConditionLock();

        computedMaxBuffers = maxBuffers;
        pool = new GenericObjectPool(new BasePooledObjectFactory() {
            @Override
            public Object create() throws Exception {
                return new byte[BUF_SIZE];
            }

            @Override
            public PooledObject wrap(Object o) {
                return new DefaultPooledObject<>(o);
            }
        });

        return this;
    }
}
