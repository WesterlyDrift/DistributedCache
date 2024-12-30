package com.kvstore.raft.message;

import lombok.Getter;

@Getter
public class VoteResponse implements RaftMessage {
    private long term;
    private boolean voteGranted;

    public VoteResponse(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
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
