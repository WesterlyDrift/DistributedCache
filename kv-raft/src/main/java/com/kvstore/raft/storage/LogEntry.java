package com.kvstore.raft.storage;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class LogEntry implements Serializable {
    private final long term;
    private final int index;
    private final String operation;
    private final String key;
    private final String value;

    public LogEntry(long term, int index, String operation, String key, String value) {
        this.term = term;
        this.index = index;
        this.operation = operation;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("LogEntry{term=%d, index=%d, operation=%s, key=%s, value=%s}",
                term, index, operation, key, value);
    }
}
