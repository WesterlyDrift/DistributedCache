package com.kvstore.core.cache.shard;

import com.kvstore.core.cache.common.Cache;

public class ShardedCache<K, V> implements Cache<K, V> {
    private static final int DEFAULT_SHARD_COUNT = 16;
    private final CacheShard<K, V>[] shards;

    @SuppressWarnings("unchecked")
    public ShardedCache(int capacity) {
        shards = new CacheShard[DEFAULT_SHARD_COUNT];
        int perShardCapacity = capacity / DEFAULT_SHARD_COUNT;
        for (int i = 0; i < DEFAULT_SHARD_COUNT; i++) {
            shards[i] = new CacheShard<>(perShardCapacity);
        }
    }

    private int shardFor(K key) {
        return Math.abs(key.hashCode() % DEFAULT_SHARD_COUNT);
    }

    @Override
    public void put(K key, V value) {
        shards[shardFor(key)].put(key, value);
    }

    @Override
    public V get(K key) {
        return shards[shardFor(key)].get(key);
    }

    @Override
    public void remove(K key) {
        shards[shardFor(key)].remove(key);
    }

    @Override
    public void clear() {
        for (CacheShard<K, V> shard : shards) {
            shard.clear();
        }
    }

    @Override
    public int size() {
        int total = 0;
        for (CacheShard<K, V> shard : shards) {
            total += shard.size();
        }
        return total;
    }

    @Override
    public boolean isEmpty() {
        for (CacheShard<K, V> shard : shards) {
            if (!shard.isEmpty()) {
                return false;
            }
        }
        return true;
    }
}
