package com.kvstore.core.cache;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class LRUCache<K, V> implements Cache<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(LRUCache.class);

    private final int capacity;
    private final Map<K, Node<K, V>> cache;
    private final Node<K, V> head;
    private final Node<K, V> tail;


    private static class Node<K, V> {
        K key;
        V value;
        Node<K, V> prev;
        Node<K, V> next;

        public Node(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public Node() {}
    }

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.cache = Maps.newHashMap();
        this.head = new Node<>();
        this.tail = new Node<>();
    }

    @Override
    public V get(K key) {
        Node<K, V> node = cache.get(key);
        if(node == null) {
            return null;
        }

        moveToHead(node);
        return node.value;
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public void put(K key, V value) {
        Node<K, V> node = cache.get(key);
        if(node != null) {
            node.value = value;
            moveToHead(node);
        } else {
            Node<K, V> newNode = new Node<>(key, value);
            cache.put(key, newNode);
            add(newNode);
            if(cache.size() > capacity) {
                Node<K, V> tail = removeTail();
                if (tail != null) {
                    cache.remove(tail.key);
                }
            }
        }
    }

    @Override
    public V remove(K key) {
        Node<K, V> node = cache.get(key);
        if(node != null) {
            removeNode(node);
            cache.remove(key);
            return node.value;
        }
        return null;
    }

    @Override
    public void clear() {
        cache.clear();
        head.next = tail;
        tail.prev = head;
    }

    private void moveToHead(Node<K, V> node) {
        removeNode(node);
        add(node);
    }

    private void removeNode(Node<K, V> node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void add(Node<K, V> node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private Node<K, V> removeTail() {
        Node<K, V> res = tail.prev;
        if(res == head) {
            return null;
        }
        removeNode(res);
        return res;
    }
}
