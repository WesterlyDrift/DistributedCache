package com.kvstore.raft.snapshot;

import com.kvstore.raft.storage.LogEntry;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Getter
public class Snapshot implements Serializable {
    private final List<LogEntry> logEntries;
    private final int lastIncludedIndex;
    private final long lastIncludedTerm;
    private final long createTimestamp;

    public Snapshot(List<LogEntry> logEntries, int lastIncludedIndex, long lastIncludedTerm) {
        this.logEntries = logEntries;
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.createTimestamp = System.currentTimeMillis();
    }
}
