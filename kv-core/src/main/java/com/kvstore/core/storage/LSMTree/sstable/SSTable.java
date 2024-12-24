package com.kvstore.core.storage.LSMTree.sstable;

import com.kvstore.core.storage.LSMTree.bloom.BloomFilter;
import com.kvstore.core.storage.LSMTree.io.ExtendedInputStream;
import com.kvstore.core.storage.LSMTree.io.ExtendedOutputStream;
import com.kvstore.core.storage.LSMTree.types.ByteArrayPair;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Iterator;
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

    byte[] minKey;
    byte[] maxKey;

    public SSTable(String directory, Iterator<ByteArrayPair> items, int sampleSize) {
        this()
    }

    public SSTable(String directory, Iterator<ByteArrayPair> items) {

    }

    public SSTable(String directory, Iterator<ByteArrayPair> items, long maxByteSize) {

    }

    public SSTable(String filename, Iterator<ByteArrayPair> items, int sampleSize, long maxByteSize) {
        this.filename = filename;
        writeItems(filename, items, sampleSize, maxByteSize);
        is = new ExtendedInputStream(filename + DATA_FILE_EXTENSION);
    }

    public SSTable(String filename) {
        this.filename = filename;
        initializeFromDisk(filename);
    }

    private void initializeFromDisk(String filename) {
        is = new ExtendedInputStream(filename + DATA_FILE_EXTENSION);

        sparseOffsets = new LongArrayList();
        sparseSizeCount = new IntArrayList();
        sparseKeys = new ObjectArrayList<>();

        ExtendedInputStream indexIs = new ExtendedInputStream(filename + INDEX_FILE_EXTENSION);
        size = indexIs.readVByteInt();

        int sparseSize = indexIs.readVByteInt();
        long offsetsCumulative = 0;
        sparseOffsets.add(offsetsCumulative);
        for(int i = 0; i < sparseSize - 1; i++) {
            offsetsCumulative += indexIs.readVByteLong();
            sparseOffsets.add(offsetsCumulative);
        }

        int sizeCumulative = 0;
        sparseSizeCount.add(sizeCumulative);
        for(int i = 0; i < sparseSize - 1; i++) {
            sizeCumulative += indexIs.readVByteInt();
            sparseSizeCount.add(sizeCumulative);
        }

        for(int i = 0; i < sparseSize; i++) {
            sparseKeys.add(indexIs.readNBytes(indexIs.readVByteInt()));
        }

        is.close();

        bloomFilter = BloomFilter.readFromFile(filename + BLOOM_FILE_EXTENSION);
    }

    private void writeItems(String filename, Iterator<ByteArrayPair> items, int sampleSize, long maxByteSize) {
        ExtendedOutputStream ios = new ExtendedOutputStream(filename + DATA_FILE_EXTENSION);

        sparseOffsets = new LongArrayList();
        sparseSizeCount = new IntArrayList();
        sparseKeys = new ObjectArrayList<>();
        bloomFilter = new BloomFilter();

        int numElements = 0;
        long offset = 0L;
        long byteSize = 0L;

        while(items.hasNext() && byteSize < maxByteSize) {
            ByteArrayPair item = items.next();

            if(minKey == null) {
                minKey = item.key();
            }

            maxKey = item.key();

            if(numElements % sampleSize == 0) {
                sparseOffsets.add(offset);
                sparseSizeCount.add(numElements);
                sparseKeys.add(item.key());
            }

            bloomFilter.add(item.key());

            offset += ios.writeByteArrayPair(item);
            numElements++;

            byteSize += item.size();
        }

        ios.close();

        if(numElements == 0) {
            throw new IllegalArgumentException("Attempted to create an SSTable from an empty iterator");
        }

        this.size = numElements;

        bloomFilter.writeToFile(filename + BLOOM_FILE_EXTENSION);

        ExtendedOutputStream indexOs = new ExtendedOutputStream(filename + INDEX_FILE_EXTENSION);
        indexOs.writeVByteInt(numElements);

        int sparseSize = sparseOffsets.size();
        indexOs.writeVByteInt(sparseSize);

        long prevOffset = 0L;
        for(int i = 1; i < sparseSize; i++) {
            indexOs.writeVByteLong(sparseOffsets.getLong(i) - prevOffset);
            prevOffset = sparseOffsets.getLong(i);
        }

        int prevSize = 0;
        for(int i = 1; i < sparseSize; i++) {
            indexOs.writeVByteInt(sparseSizeCount.getInt(i) - prevSize);
            prevSize = sparseSizeCount.getInt(i);
        }

        for(byte[] key : sparseKeys) {
            indexOs.writeVByteInt(key.length);
            indexOs.write(key);
        }

        indexOs.close();
    }

    private static class SSTableIterator implements Iterator<ByteArrayPair> {

        private final SSTable table;
        int remaining;

        public SSTableIterator(SSTable table) {
            this.table = table;
            this.remaining = table.size;
        }

        @Override
        public boolean hasNext() {
            return remaining > 0;
        }

        @Override
        public ByteArrayPair next() {
            remaining--;

            return table.is.readBytePair();
        }
    }

}
