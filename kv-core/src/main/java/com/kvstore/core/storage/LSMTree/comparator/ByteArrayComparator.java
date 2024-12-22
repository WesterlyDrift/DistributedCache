package com.kvstore.core.storage.LSMTree.comparator;

public class ByteArrayComparator {
    public static int compare(byte[] a, byte[] b) {
        if(a == null) {
            return b == null ? 0 : -1;
        }
        int alen = a.length;
        int blen = b.length;

        if(alen != blen) {
            return alen - blen;
        }

        for(int i = 0; i < alen; i++) {
            byte abyte = a[i];
            byte bbyte = b[i];
            if(abyte != bbyte) {
                return abyte - bbyte;
            }
        }
        return 0;
    }
}
