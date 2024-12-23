package com.kvstore.core.storage.LSMTree.utils;

import java.util.Iterator;

public class UniqueSortedIterator<T extends Comparable<T>> implements Iterator<T> {

    Iterator<T> iterator;
    private T last;

    public UniqueSortedIterator(Iterator<T> iterator) {
        this.iterator = iterator;
        this.last = iterator.next();
    }

    @Override
    public boolean hasNext() {
        return last != null;
    }

    @Override
    public T next() {
        T next = iterator.next();
        while(next != null && last.compareTo(next) == 0) {
            next = iterator.next();
        }
        T res = last;
        last = next;

        return res;
    }
}
