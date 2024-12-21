package com.kvstore.core.cache;

public interface Cache<K, V> {
    V get(K key);
    void put(K key, V value);
    V remove(K key);
    void clear();
    int size();
}
