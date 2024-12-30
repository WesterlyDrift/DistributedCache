package com.kvstore.raft.storage;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class RaftState implements Serializable {
    private final long currentTerm;
    private final String votedFor;

    public RaftState(long currentTerm, String votedFor) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
    }
}
