package com.kvstore.network.api.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class RpcResponse implements Serializable {
    private int code;
    private String message;
    private Class<?> dataType;
    private Object data;

    public static RpcResponse success(Object data) {
        return RpcResponse.builder()
                .code(200)
                .dataType(data.getClass())
                .data(data)
                .build();
    }

    public static RpcResponse fail(String message) {
        return RpcResponse.builder()
                .code(500)
                .message(message)
                .build();
    }

}
