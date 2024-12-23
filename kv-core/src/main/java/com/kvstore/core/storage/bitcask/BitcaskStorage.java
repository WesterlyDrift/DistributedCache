package com.kvstore.core.storage.bitcask;

import com.kvstore.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class BitcaskStorage implements Storage {
    private static final Logger logger = LoggerFactory.getLogger(BitcaskStorage.class);

    private final File directory;
    private final Map<String, IndexEntry> index;
    private DataFile activeFile;
    private final List<DataFile> olderFiles;
    private final StorageConfig config;

}
