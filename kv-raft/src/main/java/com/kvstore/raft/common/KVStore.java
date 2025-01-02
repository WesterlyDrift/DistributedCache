package com.kvstore.raft.common;

public interface KVStore {
    String get(String key);
    void put(String key, String value);
    void delete(String key);
    void clear();
}
