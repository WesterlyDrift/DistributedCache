package com.kvstore.raft.core.rpc.model;

public class AppendRequest {
    private final long term;
    private final String leaderId;
    private final long prevLogIndex;
    private final long prevLogTerm;
    private final List<LogEntry> entries;
    private final long leaderCommit;
}
