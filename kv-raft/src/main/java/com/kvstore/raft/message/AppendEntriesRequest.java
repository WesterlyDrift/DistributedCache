package com.kvstore.raft.message;

import com.kvstore.raft.storage.LogEntry;
import lombok.Data;
import lombok.Getter;

import java.util.List;

@Getter
public class AppendEntriesRequest implements RaftMessage {
    private long term;
    private String leaderId;
    private int prevLogIndex;
    private long prevLogTerm;
    private List<LogEntry> entries;
    private int leaderCommit;

    public AppendEntriesRequest(long term, String leaderId, int prevLogIndex, long prevLogTerm, List<LogEntry> entries, int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    @Override
    public void setTerm(long term) {
        this.term = term;
    }
}
