package com.kvstore.network.serializer.spi;

import com.caucho.hessian.io.ObjectSerializer;
import com.google.common.collect.Maps;

import java.util.Map;

public interface Serializer {
    byte[] serialize(Object obj);

    Object deserialize(byte[] bytes, int messageType);

    int getType();

    static Serializer getSerializer(int code) {
        Map<Integer, Serializer> serializerMap = Maps.newHashMap();
        serializerMap.put(0, new ObjectSerializer());
        serializerMap.put(1, new JsonSerializer());
        serializerMap.put(2, new KryoSerializer());
        serializerMap.put(3, new HessianSerializer());
        serializerMap.put(4, new ProtostuffSerializer());

        return serializerMap.get(code);
    }
}
