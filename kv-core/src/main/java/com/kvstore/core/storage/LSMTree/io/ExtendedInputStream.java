package com.kvstore.core.storage.LSMTree.io;

import java.io.FileInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;

public class ExtendedInputStream {

    private final FastBufferedInputStream fis;

    public ExtendedInputStream(String filename) {
        try {
            fis = new FastBufferedInputStream(new FileInputStream(filename));
            fis.position(0);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int readVByteLong() {

    }
}
