package com.kvstore.core.storage.LSMTree.bloom;

import com.kvstore.core.storage.LSMTree.io.ExtendedInputStream;
import com.kvstore.core.storage.LSMTree.io.ExtendedOutputStream;
import it.unimi.dsi.fastutil.longs.LongLongMutablePair;
import it.unimi.dsi.fastutil.longs.LongLongPair;
import org.apache.commons.codec.digest.MurmurHash3;

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
        try {
            int size = is.readVByteInt();
            int hashCount = is.readVByteInt();
            int bitsLength = is.readVByteInt();

            long[] bits = new long[bitsLength];

            for(int i = 0; i < bitsLength; i++) {
                bits[i] = is.readLong();
            }

            is.close();
            return new BloomFilter(size, hashCount, bits);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void add(byte[] key) {
        LongLongPair hash = getHash(key);
        long h1 = hash.leftLong();
        long h2 = hash.rightLong();

        for(int i = 0; i < hashCount; i++) {
            int bit = (int) Math.abs((h1 + i * h2) % size);
            bits[bit / 64] |= 1L << (bit % 64);
        }
    }

    public boolean mightContain(byte[] key) {
        LongLongPair hash = getHash(key);
        long h1 = hash.leftLong();
        long h2 = hash.rightLong();

        for(int i = 0; i < hashCount; i++) {
            int bit = (int) Math.abs((h1 + i * h2) % size);
            if((bits[bit / 64] & (1L << (bit % 64))) == 0) {
                return false;
            }
        }

        return true;
    }

    private LongLongPair getHash(byte[] key) {
        long[] hashes = MurmurHash3.hash128x64(key, 0, key.length, 0);
        return LongLongMutablePair.of(hashes[0], hashes[1]);
    }

    public void writeToFile(String filename) {
        ExtendedOutputStream os = new ExtendedOutputStream(filename);

        os.writeVByteLong(size);
        os.writeVByteLong(hashCount);

        os.writeVByteInt(bits.length);

        for(var b : bits) {
            os.writeLong(b);
        }

        os.close();
    }
}
