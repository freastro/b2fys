package net.freastro.b2fys;

import sun.awt.Mutex;
import sun.misc.ConditionLock;

import java.io.InputStream;

class Buffer {

    Mutex mu = new Mutex();
    ConditionLock cond;

    MBuf buf;
    InputStream reader;
    int err;
}
