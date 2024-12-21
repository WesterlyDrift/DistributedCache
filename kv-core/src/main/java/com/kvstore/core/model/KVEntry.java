package com.kvstore.core.model;
import lombok.Data;

@Data
public class KVEntry {
    private final byte[] key;
    private final byte[] value;
    private final long timestamp;
    private final EntryType type;

    public enum EntryType {
        PUT,
        DELETE
    }

    public KVEntry(byte[] key, byte[] value) {
        this(key, value, System.currentTimeMillis(), EntryType.PUT);
    }

    public KVEntry(byte[] key, byte[] value, long timestamp, EntryType type) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.type = type;
    }
}
