package com.kvstore.core.storage;

import com.kvstore.core.iterator.CloseableIterator;
import com.kvstore.core.model.KVEntry;

import java.io.IOException;

public interface Storage {
    void put (byte[] key, byte[] value) throws IOException;
    byte[] get(byte[] key) throws IOException;
    void delete(byte[] key) throws IOException;
    CloseableIterator<KVEntry> scan() throws IOException;
    void close() throws IOException;
}
