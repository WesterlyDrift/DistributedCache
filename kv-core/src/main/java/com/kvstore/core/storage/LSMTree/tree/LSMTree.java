package com.kvstore.core.storage.LSMTree.tree;

import com.kvstore.core.storage.LSMTree.memtable.Memtable;
import com.kvstore.core.storage.LSMTree.sstable.SSTable;
import com.kvstore.core.storage.LSMTree.types.ByteArrayPair;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LSMTree {

    static final long DEFAULT_MEMTABLE_MAX_BYTE_SIZE = 1024 * 1024 * 32;
    static final int DEFAULT_LEVEL_ZERO_MAX_SIZE = 2;
    static final double LEVEL_INCR_FACTOR = 1.75;

    static final String DEFAULT_DATA_DIRECTORY = "LSM-data";

    final Object mutableMemtableLock = new Object();
    final Object immutableMemtableLock = new Object();
    final Object tableLock = new Object();

    final Long mutableMemtableMaxSize;
    final int maxLevelZeroSstNumber;
    final long maxLevelZeroSstByteSize;
    final String dataDir;

    Memtable mutableMemtable;
    LinkedList<Memtable> immutableMemtables;
    ObjectArrayList<ObjectArrayList<SSTable>> levels;

    ScheduledExecutorService memtableFlusher;
    ScheduledExecutorService tableCompactor;

    public LSMTree() {
        this(DEFAULT_MEMTABLE_MAX_BYTE_SIZE, DEFAULT_LEVEL_ZERO_MAX_SIZE, DEFAULT_DATA_DIRECTORY);
    }

    public LSMTree(long mutableMemtableMaxByteSize, int maxLevelZeroSstNumber, String dataDir) {
        this.mutableMemtableMaxSize = mutableMemtableMaxByteSize;
        this.maxLevelZeroSstNumber = maxLevelZeroSstNumber;
        this.maxLevelZeroSstByteSize = mutableMemtableMaxByteSize * 2;
        this.dataDir = dataDir;
        createDataDir();

        mutableMemtable = new Memtable();
        immutableMemtables = new LinkedList<>();
        levels = new ObjectArrayList<>();
        levels.add(new ObjectArrayList<>());

        memtableFlusher = Executors.newSingleThreadScheduledExecutor();
        memtableFlusher.scheduleAtFixedRate(this::flushMemtable, 50, 50, TimeUnit.MILLISECONDS);

        tableCompactor = Executors.newSingleThreadScheduledExecutor();
        tableCompactor.scheduleAtFixedRate(this::levelCompaction, 200, 200, TimeUnit.MILLISECONDS);
    }

    public void add(ByteArrayPair pair) {
        synchronized(mutableMemtableLock) {
            mutableMemtable.put(pair);
            checkMemtableSize();
        }
    }

    public void delete(byte[] key) {
        synchronized(mutableMemtableLock) {
            mutableMemtable.delete(key);
            checkMemtableSize();
        }
    }


    public byte[] get(byte[] key) {
        byte[] result;

        synchronized(mutableMemtableLock) {
            if((result = mutableMemtable.get(key)) != null) {
                return result;
            }
        }

        synchronized(immutableMemtableLock) {
            for(Memtable memtable : immutableMemtables) {
                if((result = memtable.get(key)) != null) {
                    return result;
                }
            }
        }

        synchronized(tableLock) {
            for(ObjectArrayList<SSTable> level : levels) {
                for(SSTable table : level) {
                    if((result = table.get(key)) != null) {
                        return result;
                    }
                }
            }
        }
        return null;
    }

    public void stop() {
        memtableFlusher.shutdown();
        tableCompactor.shutdownNow();
    }

    private void createDataDir() {
        try {
            Files.createDirectory(Path.of(dataDir));
        } catch(IOException e) {
            throw new RuntimeException("Could not create data directory", e);
        }
    }

    private void flushMemtable() {
        Memtable memtableToFlush;
        synchronized(immutableMemtableLock) {
            if(immutableMemtables.isEmpty()) {
                return;
            }

            memtableToFlush = immutableMemtables.getLast();
        }

        SSTable table = new SSTable(dataDir, memtableToFlush.iterator(), mutableMemtableMaxSize * 2);

        synchronized(tableLock) {
            levels.get(0).add(0, table);
        }

        synchronized(immutableMemtableLock) {
            immutableMemtables.removeLast();
        }
    }

    private void levelCompaction() {
        synchronized(tableLock) {
            int n = levels.size();

            int maxLevelSize = maxLevelZeroSstNumber;
            long sstMaxSize = maxLevelZeroSstByteSize;

            for(int i = 0; i < n; i++) {
                ObjectArrayList<SSTable> level = levels.get(i);

                if(levels.size() > maxLevelSize) {
                    if(i == n - 1) {
                        levels.add(new ObjectArrayList<>());
                    }

                    ObjectArrayList<SSTable> nextLevel = levels.get(i + 1);
                    ObjectArrayList<SSTable> merge = new ObjectArrayList<>();
                    merge.addAll(level);
                    merge.addAll(nextLevel);

                    var sortedRun = SSTable.sortedRun(dataDir, sstMaxSize, merge.toArray(SSTable[]::new));

                    level.forEach(SSTable::closeAndDelete);
                    level.clear();
                    nextLevel.forEach(SSTable::closeAndDelete);
                    nextLevel.clear();

                    nextLevel.addAll(sortedRun);
                }

                maxLevelSize = (int)(maxLevelSize * LEVEL_INCR_FACTOR);
                sstMaxSize = (int) (sstMaxSize * LEVEL_INCR_FACTOR);
            }
        }
    }

    private void checkMemtableSize() {
        if(mutableMemtable.byteSize() <= mutableMemtableMaxSize) {
            return;
        }

        synchronized(immutableMemtableLock) {
            immutableMemtables.addFirst(mutableMemtable);
            mutableMemtable = new Memtable();
        }
    }


    @Override
    public String toString() {

        var s = new StringBuilder();
        s.append("LSM-Tree {\n");
        s.append("\tmemtable: ");
        s.append(mutableMemtable.byteSize() / 1024.0 / 1024.0);
        s.append(" mb\n");
        s.append("\timmutable memtables: ");
        s.append(immutableMemtables);
        s.append("\n\tsst levels:\n");

        int i = 0;
        for (var level : levels) {
            s.append(String.format("\t\t- %d: ", i));
            level.stream()
                    .map(st -> String.format("[ %s, size: %d ] ", st.filename, st.size))
                    .forEach(s::append);
            s.append("\n");
            i += 1;
        }

        s.append("}");
        return s.toString();
    }
}
