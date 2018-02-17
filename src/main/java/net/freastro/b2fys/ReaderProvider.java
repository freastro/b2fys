package net.freastro.b2fys;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

public interface ReaderProvider {

    InputStream func(AtomicInteger error);
}
