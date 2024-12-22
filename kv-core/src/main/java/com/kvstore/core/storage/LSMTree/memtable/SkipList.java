package com.kvstore.core.storage.LSMTree.memtable;

import com.kvstore.core.storage.LSMTree.types.ByteArrayPair;

import java.util.Iterator;
import java.util.Random;

public class SkipList implements Iterable<ByteArrayPair> {

    static final int DEFAULT_ELEMENTS = 1 << 20;

    final Node sentinel;

    private final Node[] buffer;
    private final Random random;

    int levels;
    int size;

    public SkipList() {
        this(DEFAULT_ELEMENTS);
    }

    public SkipList(int numElements) {
        levels = (int) Math.ceil(Math.log(numElements) / Math.log(2));
        size = 0;
        sentinel = new Node(null, levels);
        random = new Random();
        buffer = new Node[levels];
    }

    public void add(ByteArrayPair item) {
        Node current = sentinel;
        for(int i = levels - 1; i >= 0; i--) {
            while(current.next[i] != null && current.next[i].val.compareTo(item) < 0) {
                current = current.next[i];
            }
            buffer[i] = current;
        }

        if(current.next[0] != null && current.next[0].val.compareTo(item) == 0) {
            current.next[0].val = item;
            return;
        }

        Node newNode = new Node(item, levels);
    }



    private static final class Node {
        ByteArrayPair val;
        Node[] next;

        Node(ByteArrayPair val, int levels) {
            this.val = val;
            this.next = new Node[levels];
        }
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
            if(node.next[0] == null) {
                return null;
            }

            ByteArrayPair res = node.next[0].val;
            node = node.next[0];

            return res;
        }
    }
}
