package com.kvstore.raft.storage;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RaftStorage {
    private final String storageDir;
    private final ObjectMapper objectMapper;
    private final ReadWriteLock lock;
    private final File stateFile;
    private final File logFile;

    public RaftStorage(String storageDir) {
        this.storageDir = storageDir;
        this.objectMapper = new ObjectMapper();
        this.lock = new ReentrantReadWriteLock();

        new File(storageDir).mkdirs();
        this.stateFile = new File(storageDir, "state.json");
        this.logFile = new File(storageDir, "log.bin");
    }

    public void saveState(RaftState state) {
        lock.writeLock().lock();
        try {
            objectMapper.writeValue(stateFile, state);
        } catch(IOException e) {
            throw new RuntimeException("Failed to save state", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public RaftState loadState() {
        lock.readLock().lock();
        try {
            if(!stateFile.exists()) {
                return new RaftState(0, null);
            }
            return objectMapper.readValue(stateFile, RaftState.class);
        } catch(IOException e) {
            throw new RuntimeException("Failed to load state", e);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void appendLog(LogEntry entry) {
        lock.writeLock().lock();
        try (ObjectOutputStream out = new ObjectOutputStream(
                new FileOutputStream(logFile, true)
        )) {
            out.writeObject(entry);
        } catch(IOException e) {
            throw new RuntimeException("Failed to append log", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void saveLog(List<LogEntry> entries) {
        lock.writeLock().lock();
        try (ObjectOutputStream out = new ObjectOutputStream(
                new FileOutputStream(logFile)
        )) {
            for(LogEntry entry : entries) {
                out.writeObject(entry);
            }
        } catch(IOException e) {
            throw new RuntimeException("Failed to save log", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<LogEntry> loadLog() {
        lock.readLock().lock();
        try {
            List<LogEntry> log = new ArrayList<>();
            if(!logFile.exists()) {
                return log;
            }

            try (ObjectInputStream in = new ObjectInputStream(
                    new FileInputStream(logFile)
            )) {
                while(true) {
                    LogEntry entry = (LogEntry) in.readObject();
                    log.add(entry);
                }
            } catch(EOFException ignored) {

            }
            return log;
        } catch(IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to load log", e);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clearLog() {
        lock.writeLock().lock();
        try {
            if (logFile.exists()) {
                if (!logFile.delete()) {
                    throw new IOException("Failed to delete log file");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear log", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
