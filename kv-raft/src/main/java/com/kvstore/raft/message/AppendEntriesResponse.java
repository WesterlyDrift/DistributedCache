package com.kvstore.raft.message;

import lombok.Getter;

@Getter
public class AppendEntriesResponse implements RaftMessage {
    private long term;
    private final boolean success;

    public AppendEntriesResponse(long term, boolean success) {
        this.term = term;
        this.success = success;
    }

    @Override
    public void setTerm(long term) {
        this.term = term;
    }

}
