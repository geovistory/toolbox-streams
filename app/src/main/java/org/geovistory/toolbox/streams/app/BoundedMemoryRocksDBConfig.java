package org.geovistory.toolbox.streams.app;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;

import java.util.Map;

public class BoundedMemoryRocksDBConfig implements RocksDBConfigSetter {

    public static final String TOTAL_OFF_HEAP_SIZE_MB = "rocksdb.total_offheap_size_mb";
    public static final String TOTAL_MEMTABLE_MB = "rocksdb.total_memtable_mb";

    private final static long BYTE_FACTOR = 1;
    private final static long KB_FACTOR = 1024 * BYTE_FACTOR;
    private final static long MB_FACTOR = 1024 * KB_FACTOR;

    private static org.rocksdb.Cache cache;
    private static org.rocksdb.WriteBufferManager writeBufferManager;

    @Override
    public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
        if (cache == null || writeBufferManager == null) initCacheOnce(configs);

        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();

        // These three options in combination will limit the memory used by RocksDB to the size passed to the block cache (TOTAL_OFF_HEAP_MEMORY)
        tableConfig.setBlockCache(cache);
        tableConfig.setCacheIndexAndFilterBlocks(true);
        options.setWriteBufferManager(writeBufferManager);

        // These options are recommended to be set when bounding the total memory
        tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
        tableConfig.setPinTopLevelIndexAndFilter(true);
        // tableConfig.setBlockSize(BLOCK_SIZE);
        // options.setMaxWriteBufferNumber(N_MEMTABLES);
        // options.setWriteBufferSize(MEMTABLE_SIZE);

        options.setTableFormatConfig(tableConfig);
    }

    @Override
    public void close(final String storeName, final Options options) {
        // Cache and WriteBufferManager should not be closed here, as the same objects are shared by every store instance.
    }

    public synchronized void initCacheOnce(final Map<String, Object> configs) {
        // already initialized
        if (cache != null && writeBufferManager != null)
            return;

        long offHeapMb = Long.parseLong(configs.get(TOTAL_OFF_HEAP_SIZE_MB).toString());
        long totalMemTableMemMb = Long.parseLong(configs.get(TOTAL_MEMTABLE_MB).toString());

        if (cache == null) {
            cache = new org.rocksdb.LRUCache(offHeapMb * MB_FACTOR, -1, false);
        }
        if (writeBufferManager == null)
            writeBufferManager = new org.rocksdb.WriteBufferManager(totalMemTableMemMb * MB_FACTOR, cache);
    }
}