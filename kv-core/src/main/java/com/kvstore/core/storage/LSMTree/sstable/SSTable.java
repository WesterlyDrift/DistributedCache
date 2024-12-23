package com.kvstore.core.storage.LSMTree.sstable;

import com.kvstore.core.storage.LSMTree.types.ByteArrayPair;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.concurrent.atomic.AtomicLong;

public class SSTable implements Iterable<ByteArrayPair> {

    public static final String DATA_FILE_EXTENSION = ".data";
    public static final String BLOOM_FILE_EXTENSION = ".bloom";
    public static final String INDEX_FILE_EXTENSION = ".index";

    private static final int DEFAULT_SAMPLE_SIZE = 1000;

    static final AtomicLong SST_COUNTER = new AtomicLong(0L);

    public String filename;
    ExtendedInputStream is;
    public int size;

    LongArrayList sparseOffsets;
    IntArrayList sparseSizeCount;
    ObjectArrayList<byte[]> sparseKeys;
    BloomFilter bloomFilter;

}
