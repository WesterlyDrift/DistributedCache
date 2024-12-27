package com.kvstore.core.storage.LSMTree.memtable;

import com.kvstore.core.storage.LSMTree.types.ByteArrayPair;
import com.kvstore.core.storage.LSMTree.utils.UniqueSortedIterator;

import java.util.Iterator;

public class Memtable implements Iterable<ByteArrayPair> {

    SkipList list;
    long byteSize;

    public Memtable() {
        list = new SkipList();
        byteSize = 0L;
    }

    public void put(ByteArrayPair pair) {
        list.put(pair);
        byteSize += pair.size();
    }

    public byte[] get(byte[] key) {
        return list.get(key);
    }

    public void delete(byte[] key) {
        list.put(key, new byte[]{});
    }

    public long byteSize() {
        return byteSize;
    }

    @Override
    public Iterator<ByteArrayPair> iterator() {
        return new UniqueSortedIterator<>(list.iterator());
    }

    @Override
    public String toString() {
        return list.toString();
    }
}
