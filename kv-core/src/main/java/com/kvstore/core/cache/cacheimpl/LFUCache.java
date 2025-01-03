package com.kvstore.core.cache.cacheimpl;

import com.google.common.collect.Maps;
import com.kvstore.core.cache.common.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.TreeMap;

public class LFUCache<K, V> implements Cache<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(LFUCache.class);

    private final int capacity;
    private final Map<K, Node<K, V>> cache;
    private final TreeMap<Integer, LinkedHashSet<Node<K, V>>> frequencies;
    private int minimumFrequency;

    private static class Node<K, V> {
        K key;
        V value;
        int frequency;

        Node(K key, V value) {
            this.key = key;
            this.value = value;
            this.frequency = 1;
        }
    }

    public LFUCache(int capacity) {
        this.capacity = capacity;
        this.cache = Maps.newHashMap();
        this.frequencies = Maps.newTreeMap();
        this.minimumFrequency = 0;
    }

    @Override
    public V get(K key) {
        Node<K, V> node = cache.get(key);
        if(node == null) {
            return null;
        }
        incrementFrequency(node);
        return node.value;
    }

    @Override
    public void put(K key, V value) {
        if(capacity <= 0) {
            return;
        }

        Node<K, V> node = cache.get(key);
        if(node != null) {
            node.value = value;
            incrementFrequency(node);
        } else {
            if(cache.size() >= capacity) {
                LinkedHashSet<Node<K, V>> nodes = frequencies.get(minimumFrequency);
                Node<K, V> nodeToRemove = nodes.iterator().next();
                nodes.remove(nodeToRemove);
                cache.remove(nodeToRemove.key);
                if(nodes.isEmpty()) {
                    frequencies.remove(minimumFrequency);
                }
            }

            node = new Node<>(key, value);
            cache.put(key, node);
            minimumFrequency = 1;
            frequencies.computeIfAbsent(1, k -> new LinkedHashSet<>()).add(node);
        }
    }

    private void incrementFrequency(Node<K, V> node) {
        LinkedHashSet<Node<K, V>> nodes = frequencies.get(node.frequency);
        nodes.remove(node);
        if(nodes.isEmpty()) {
            frequencies.remove(node.frequency);
            if(minimumFrequency == node.frequency) {
                minimumFrequency = frequencies.isEmpty() ? node.frequency + 1 : frequencies.firstKey();
            }
        }

        node.frequency++;
        frequencies.computeIfAbsent(node.frequency, k -> new LinkedHashSet<>()).add(node);
    }

    @Override
    public void remove(K key) {
        Node<K, V> node = cache.get(key);
        if(node != null) {
            frequencies.get(node.frequency).remove(node);
            if(frequencies.get(node.frequency).isEmpty()) {
                frequencies.remove(node.frequency);
            }
            cache.remove(key);
        }
    }

    @Override
    public void clear() {
        cache.clear();
        frequencies.clear();
        minimumFrequency = 0;
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
