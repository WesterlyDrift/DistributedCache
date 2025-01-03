package com.kvstore.core.cache.shard;

import com.kvstore.core.cache.cacheimpl.DoublyLinkedList;
import com.kvstore.core.cache.cacheimpl.Node;
import com.kvstore.core.cache.common.Cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class CacheShard<K, V> implements Cache<K, V> {
    private final int capacity;
    private final Map<K, Node<K, V>> cache;
    private DoublyLinkedList<K, V> lruList;
    private final ReentrantLock lock;

    public CacheShard(int capacity) {
        this.capacity = capacity;
        this.cache = new HashMap<>();
        this.lruList = new DoublyLinkedList<>();
        this.lock = new ReentrantLock();
    }

    @Override
    public void put(K key, V value) {
        lock.lock();
        try {
            Node<K, V> node = cache.get(key);
            if(node != null) {
                node.value = value;
                lruList.moveToFront(node);
            } else {
                node = new Node<>(key, value);
                cache.put(key, node);
                lruList.addFirst(node);

                if(cache.size() > capacity) {
                    Node<K, V> lru = lruList.removeLast();
                    cache.remove(lru.key);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V get(K key) {
        lock.lock();
        try {
            Node<K, V> node = cache.get(key);
            if(node == null) {
                return null;
            }
            lruList.moveToFront(node);
            return node.value;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void remove(K key) {
        lock.lock();
        try {
            Node<K, V> node = cache.remove(key);
            if (node != null) {
                lruList.moveToFront(node);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            cache.clear();
            lruList = new DoublyLinkedList<>();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public boolean isEmpty() {
        return cache.isEmpty();
    }
}
