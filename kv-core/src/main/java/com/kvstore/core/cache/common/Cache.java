package com.kvstore.core.cache.common;

public interface Cache<K, V> {
    V get(K key);
    void put(K key, V value);
    void remove(K key);
    void clear();
    int size();
    boolean isEmpty();
}
