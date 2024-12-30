package com.kvstore.raft.common;

import lombok.Data;

@Data
public class NodeAddress {
    private final String host;
    private final int port;

    public NodeAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }
}
