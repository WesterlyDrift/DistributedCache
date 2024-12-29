package com.kvstore.network.serializer.codec;


import io.netty.handler.codec.MessageToByteEncoder;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class RpcEncoder extends MessageToByteEncoder {
    private Serializer serializer;
}
