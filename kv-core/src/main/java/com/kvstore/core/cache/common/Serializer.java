package com.kvstore.core.cache.common;

public interface Serializer<T> {
    byte[] serialize(T obj);
    T deserialize(byte[] bytes);
}
