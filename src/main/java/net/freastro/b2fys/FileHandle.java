package net.freastro.b2fys;

import sun.awt.Mutex;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

class FileHandle {

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
    S3ReadBuffer[] buffers;
    int existingReadahead;
    long seqReadAmount;
    long numOOORead; // number of out of order read
}
