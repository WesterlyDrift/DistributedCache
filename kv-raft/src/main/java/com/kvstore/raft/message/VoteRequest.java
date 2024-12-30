package com.kvstore.raft.message;

import lombok.Data;
import lombok.Getter;

@Getter
public class VoteRequest implements RaftMessage {
    private long term;
    private String candidateId;
    private int lastLogIndex;
    private long lastLogTerm;

    public VoteRequest(long term, String candidateId, int lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    @Override
    public long getTerm() {
        return term;
    }

    @Override
    public void setTerm(long term) {
        this.term = term;
    }
}
