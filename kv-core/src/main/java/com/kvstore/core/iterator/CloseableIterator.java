package com.kvstore.core.iterator;

import java.io.IOException;
import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {
    @Override
    void close() throws IOException;
}
