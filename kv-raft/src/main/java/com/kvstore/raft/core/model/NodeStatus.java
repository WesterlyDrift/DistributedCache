package com.kvstore.raft.core.model;

import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.Data;

import java.util.Collections;
import java.util.List;

@Data
public class NodeStatus {
    private final long term;
    private final RaftRole role;
    private final String leaderId;
    private final long commitIndex;
    private final long lastApplied;

    private final NodeEndpoint self;

    private final List<NodeEndpoint> peers;

    @Builder
    public NodeStatus(long term, RaftRole role, String leaderId, long commitIndex, long lastApplied, NodeEndpoint self, List<NodeEndpoint> peers) {
        this.term = term;
        this.role = role;
        this.leaderId = leaderId;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
        this.self = self;
        this.peers = Collections.unmodifiableList(Lists.newArrayList(peers));
    }
}
