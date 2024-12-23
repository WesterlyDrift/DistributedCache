package com.kvstore.core.storage.LSMTree.bloom;

public class BloomFilter {

    static final int DEFAULT_SIZE = 1 << 20;

    final int size;
    final int hashCount;
    final long[] bits;

    public BloomFilter() {
        this(DEFAULT_SIZE, 0.001);
    }

    public BloomFilter(int expectedInsertions) {
        this(expectedInsertions, 0.001);
    }

    public BloomFilter(int expectedInsertions, double falsePositiveRate) {
        this.size = (int)(-expectedInsertions * Math.log(falsePositiveRate) / (Math.log(2) * Math.log(2)));
        this.hashCount = (int)Math.ceil(-Math.log(falsePositiveRate) / Math.log(2));
        this.bits = new long[(int)Math.ceil(size / 64.0)];
    }

    public BloomFilter(int size, int hashCount, long[] bits) {
        this.size = size;
        this.hashCount = hashCount;
        this.bits = bits;
    }

    public static BloomFilter readFromFile(String filename) {
        ExtendedInputStream is = new ExtendedInputStream(filename);
    }
}
