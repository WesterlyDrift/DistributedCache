package com.kvstore.core.storage.LSMTree.memtable;

import com.kvstore.core.storage.LSMTree.comparator.ByteArrayComparator;
import com.kvstore.core.storage.LSMTree.types.ByteArrayPair;
import com.kvstore.core.storage.Storage;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class SkipList implements Iterable<ByteArrayPair> {

    static final int DEFAULT_ELEMENTS = 1 << 10;

    final Node sentinel;

    private final Node[] buffer;
    private final Random random;

    int levels;
    int size;

    public SkipList() {
        this(DEFAULT_ELEMENTS);
    }

    public SkipList(int capacity) {
        this.levels = (int) Math.ceil(Math.log(capacity) / Math.log(2));
        this.size = 0;
        this.sentinel = new Node(null, levels);
        this.random = new Random(System.currentTimeMillis());
        this.buffer = new Node[levels];
    }

    public int size() {
        return this.size;
    }

    private int randomLevel() {
        int level = 1;
        long num = random.nextLong();
        while(level < this.levels && (num & 1L << level) != 0) {
            level++;
        }
        return level;
    }

    public byte[] get(byte[] key) {
        Node current = this.sentinel;
        for(int i = levels - 1; i >= 0; i--) {
            while(current.next[i] != null && ByteArrayComparator.compare(current.next[i].val.key(), key) < 0) {
                current = current.next[i];
            }
            buffer[i] = current;
        }

        if(current.next[0] != null && ByteArrayComparator.compare(current.next[0].val.key(), key) == 0) {
            return current.next[0].val.value();
        }
        return null;
    }

    public void delete(byte[] key) {
        Node current = this.sentinel;
        for(int i = levels - 1; i >= 0; i--) {
            while(current.next[i] != null && ByteArrayComparator.compare(current.next[i].val.key(), key) < 0) {
                current = current.next[i];
            }
            buffer[i] = current;
        }

        if(current.next[0] != null && ByteArrayComparator.compare(current.next[0].val.key(), key) == 0) {
            boolean last = current.next[0].next[0] == null;
            for(int i = 0; i < levels - 1; i++) {
                if(buffer[i].next[i] != current.next[0]) {
                    break;
                }
                buffer[i].next[i] = last ? null : current.next[0].next[i];
            }
            this.size--;
        }
    }

    public void close() throws IOException {

    }

    public void put(ByteArrayPair pair) {
        put(pair.key(), pair.value());
    }

    public void put(byte[] key, byte[] value) {
        ByteArrayPair pair = new ByteArrayPair(key, value);
        Node current = this.sentinel;
        for(int i = levels - 1; i >= 0; i--) {
            while(current.next[i] != null && current.next[i].val.compareTo(pair) < 0) {
                current = current.next[i];
            }
            buffer[i] = current;
        }

        if(current.next[0] != null && current.next[0].val.compareTo(pair) == 0) {
            current.next[0].val = pair;
            return;
        }

        Node newnode = new Node(pair, levels);
        for(int i = 0; i < randomLevel(); i++) {
            newnode.next[i] = buffer[i].next[i];
            buffer[i].next[i] = newnode;
        }
        size++;
    }

    public void clear() {
        this.sentinel.next = new Node[levels];
        this.size = 0;
    }


    public static class Node {
        ByteArrayPair val;
        Node[] next;

        Node(ByteArrayPair val, int numLevels) {
            this.val = val;
            this.next = new Node[numLevels];
        }
    }

    @Override
    public Iterator<ByteArrayPair> iterator() {
        return new SkipListIterator(sentinel);
    }

    private static class SkipListIterator implements Iterator<ByteArrayPair> {

        Node node;

        SkipListIterator(Node node) {
            this.node = node;
        }

        @Override
        public boolean hasNext() {
            return node.next[0] != null;
        }

        @Override
        public ByteArrayPair next() {
            if(node == null || node.next[0] == null) {
                return null;
            }
            ByteArrayPair res =  node.next[0].val;
            node = node.next[0];
            return res;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SkipList {\n");
        sb.append("  Levels: ").append(levels).append("\n");
        sb.append("  Size: ").append(size).append("\n");

        // 打印每一层的数据
        for (int i = levels - 1; i >= 0; i--) {
            sb.append(String.format("  Level %2d: ", i));
            Node current = sentinel;
            int nodesInLevel = 0;

            while (current.next[i] != null) {
                sb.append(current.next[i].val).append(" -> ");
                current = current.next[i];
                nodesInLevel++;

            }
            sb.append("END\n");
        }

        return sb.toString();
    }

}
