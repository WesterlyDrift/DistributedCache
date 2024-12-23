package com.kvstore.core.storage.LSMTree.io;

import com.kvstore.core.storage.LSMTree.types.ByteArrayPair;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;

import java.io.FileOutputStream;

public class ExtendedOutputStream {

    private static final byte[] VBYTE_BUFFER = new byte[10];
    private final FastBufferedOutputStream fos;

    public ExtendedOutputStream(String filename) {
        try {
            fos = new FastBufferedOutputStream(new FileOutputStream(filename));
            fos.position(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int write(byte[] bytes) {
        try {
            fos.write(bytes);
            return bytes.length;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int writeVByteInt(int n) {
        return write(intToVByte(n))''
    }

    public int writeVByteLong(long n) {

    }

    public int writeLong(long n) {
        return write(longToBytes(n));
    }

    public int writeByteArrayPair(ByteArrayPair pair) {
        byte[] key = pair.key();
        byte[] keyBytes = intToVByte
    }
}
