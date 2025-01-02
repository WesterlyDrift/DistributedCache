package com.kvstore.raft.snapshot;

import com.kvstore.raft.common.KVStore;
import com.kvstore.raft.storage.LogEntry;
import lombok.Getter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SnapshotManager {
    private final String snapshotDir;
    @Getter
    private final int snapshotThreshold;
    private final ReadWriteLock lock;
    private final File snapshotFile;
    private final File tempSnapshotFile;

    public SnapshotManager(String snapshotDir) {
        this(snapshotDir, 1000);
    }

    public SnapshotManager(String snapshotDir, int snapshotThreshold) {
        this.snapshotDir = snapshotDir;
        this.snapshotThreshold = snapshotThreshold;
        this.lock = new ReentrantReadWriteLock();

        new File(snapshotDir).mkdirs();
        this.snapshotFile = new File(snapshotDir, "snapshot.bin");
        this.tempSnapshotFile = new File(snapshotDir, "snapshot.tmp");
    }

    public void saveSnapshot(List<LogEntry> logEntries, int lastIndex, long lastTerm) {
        lock.writeLock().lock();
        try {
            Snapshot snapshot = new Snapshot(logEntries, lastIndex, lastTerm);

            try (ObjectOutputStream out = new ObjectOutputStream(
                    new FileOutputStream(tempSnapshotFile)
            )) {
                out.writeObject(snapshot);
            }

            // 移动临时文件为快照文件
            Files.move(tempSnapshotFile.toPath(), snapshotFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save snapshot", e);
        } finally {
            lock.writeLock().unlock();
            if (tempSnapshotFile.exists()) {
                tempSnapshotFile.delete();
            }
        }
    }

    public Optional<Snapshot> loadSnapshot() {
        lock.readLock().lock();
        try {
            if(!snapshotFile.exists()) {
                return Optional.empty();
            }

            try (ObjectInputStream in = new ObjectInputStream(
                    new FileInputStream(snapshotFile))) {
                return Optional.of((Snapshot) in.readObject());
            }
        } catch(IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to load snapshot", e);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void restoreFromSnapshot(Snapshot snapshot, KVStore kvStore) {
        kvStore.clear();

        snapshot.getLogEntries().forEach(logEntry -> {
            switch (logEntry.getOperation()) {
                case "PUT" -> kvStore.put(logEntry.getKey(), logEntry.getValue());
                case "DELETE" -> kvStore.delete(logEntry.getKey());
            }
        });
    }

    public boolean shouldTakeSnapshot(int logSize) {
        return logSize >= snapshotThreshold;
    }

    public void deleteSnapshot() {
        lock.writeLock().lock();
        try {
            if (snapshotFile.exists()) {
                if (!snapshotFile.delete()) {
                    throw new IOException("Failed to delete snapshot file");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete snapshot", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long getSnapshotSize() {
        lock.readLock().lock();
        try {
            return snapshotFile.exists() ? snapshotFile.length() : 0;
        } finally {
            lock.readLock().unlock();
        }
    }

}
