package com.kvstore.raft.common;

import com.kvstore.core.storage.LSMTree.tree.LSMTree;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

public class LSMTreeAdapter implements KVStore {
    private final LSMTree lsmTree;
    private final ReentrantLock lock;

    public LSMTreeAdapter() {
        this.lsmTree = new LSMTree();
        this.lock = new ReentrantLock();
    }

    @Override
    public String get(String key) {
        lock.lock();
        try {
            return Arrays.toString(lsmTree.get(key.getBytes()));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(String key, String value) {
        lock.lock();
        try {
            lsmTree.add(key.getBytes(), value.getBytes());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) {
        lock.lock();
        try {
            lsmTree.delete(key.getBytes());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            lsmTree.clear();
        } finally {
            lock.unlock();
        }
    }

}
