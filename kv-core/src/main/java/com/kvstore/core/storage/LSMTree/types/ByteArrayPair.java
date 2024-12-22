package com.kvstore.core.storage.LSMTree.types;

import com.kvstore.core.storage.LSMTree.comparator.ByteArrayComparator;

import java.util.Arrays;

public record ByteArrayPair(byte[] key, byte[] value) implements Comparable<ByteArrayPair> {

    public int size() {
        return key.length + value.length;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }

    @Override
    public int compareTo(ByteArrayPair obj) {
        return ByteArrayComparator.compare(key, obj.key);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for(byte b : key) {
            sb.append(b);
        }
        sb.append(", ");
        for(byte b : value) {
            sb.append(b);
        }
        sb.append(")");
        return sb.toString();
    }
}
