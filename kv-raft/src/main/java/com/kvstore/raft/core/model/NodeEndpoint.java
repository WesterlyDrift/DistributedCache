package com.kvstore.raft.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class NodeEndpoint {

    private final String id;
    private final String host;
    private final int port;

}
