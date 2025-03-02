package io.phial;

import java.util.HashMap;
import java.util.Map;

public class Config {
    public static class Builder {
        private final Config config = new Config();

        public Builder setCoreTaskPoolSize(int corePoolSize) {
            this.config.coreTaskPoolSize = corePoolSize;
            return this;
        }

        public Builder setTaskPoolKeepAliveSeconds(int keepAliveSeconds) {
            this.config.taskPoolKeepAliveSeconds = keepAliveSeconds;
            return this;
        }

        public Builder setMaxTaskPoolSize(int maxPoolSize) {
            this.config.maxTaskPoolSize = maxPoolSize;
            return this;
        }

        public Builder setCommitBatchSize(int commitBatchSize) {
            this.config.commitBatchSize = commitBatchSize;
            return this;
        }

        public Builder setEntityStoreGcIntervalMillis(int entityStoreGcIntervalMillis) {
            this.config.entityStoreGcIntervalMillis = entityStoreGcIntervalMillis;
            return this;
        }

        public Builder setMemoryArenaNumber(int memoryArenaNumber) {
            this.config.memoryArenaNumber = memoryArenaNumber;
            return this;
        }

        public Builder setMemoryThreadCacheGcIntervalMillis(int memoryThreadCacheGcIntervalMillis) {
            this.config.memoryThreadCacheGcIntervalMillis = memoryThreadCacheGcIntervalMillis;
            return this;
        }

        public Builder setMemoryThreadCacheFlushThreshold(int memoryThreadCacheFlushThreshold) {
            this.config.memoryThreadCacheFlushThreshold = memoryThreadCacheFlushThreshold;
            return this;
        }

        public Builder setMemoryThreadCacheWatermark(
                Map<Integer, Map.Entry<Integer, Integer>> memoryThreadCacheWatermark) {
            this.config.memoryThreadCacheWatermark.clear();
            this.config.memoryThreadCacheWatermark.putAll(memoryThreadCacheWatermark);
            return this;
        }

        public Builder setMemoryThreadCacheWatermark(int slabSize, int lowWatermark, int highWatermark) {
            this.config.memoryThreadCacheWatermark.put(slabSize, Map.entry(lowWatermark, highWatermark));
            return this;
        }

        public Builder setMemoryThreadCacheWatermarkDecayRate(double memoryThreadCacheWatermarkDecayRate) {
            this.config.memoryThreadCacheWatermarkDecayRate = memoryThreadCacheWatermarkDecayRate;
            return this;
        }

        public Builder setMemoryRunFreeListWatermark(Map<Integer, Integer> memoryRunFreeListWatermark) {
            this.config.memoryRunFreeListWatermark.clear();
            this.config.memoryRunFreeListWatermark.putAll(memoryRunFreeListWatermark);
            return this;
        }

        public Builder setMemoryRunFreeListWatermark(int memoryRunFreeListWatermark) {
            this.config.memoryRunFreeListWatermark.clear();
            this.config.memoryRunFreeListWatermark.put(Integer.MAX_VALUE, memoryRunFreeListWatermark);
            return this;
        }

        public Config build() {
            return new Config(this.config);
        }
    }

    private int coreTaskPoolSize = Runtime.getRuntime().availableProcessors();
    private int maxTaskPoolSize = Runtime.getRuntime().availableProcessors() * 2;
    private int taskPoolKeepAliveSeconds = 60;
    private int commitBatchSize = 100;
    private int entityStoreGcIntervalMillis = 100;
    private int memoryArenaNumber = Runtime.getRuntime().availableProcessors();
    private int memoryThreadCacheGcIntervalMillis = 100;
    private int memoryThreadCacheFlushThreshold = 8192;
    private final Map<Integer, Map.Entry<Integer, Integer>> memoryThreadCacheWatermark = new HashMap<>();
    private double memoryThreadCacheWatermarkDecayRate = 0.75;
    private final Map<Integer, Integer> memoryRunFreeListWatermark = new HashMap<>();

    private Config() {
        this.memoryRunFreeListWatermark.put(Integer.MAX_VALUE, 16);
        this.memoryThreadCacheWatermark.put(8, Map.entry(64, 256));
        for (int i = 1; i <= 8; ++i) {
            int low = i <= 4 ? 32 : 16;
            this.memoryThreadCacheWatermark.put(16 * i, Map.entry(low, low * 4));
        }
        for (int i = 3; i <= 8; ++i) {
            int low = i <= 4 ? 8 : 4;
            this.memoryThreadCacheWatermark.put(64 * i, Map.entry(low, low * 4));
        }
        for (int i = 3; i <= 15; ++i) {
            int low = i <= 4 ? 2 : 1;
            this.memoryThreadCacheWatermark.put(256 * i, Map.entry(low, low * 4));
        }
    }

    private Config(Config config) {
        this.coreTaskPoolSize = config.coreTaskPoolSize;
        this.maxTaskPoolSize = config.maxTaskPoolSize;
        this.taskPoolKeepAliveSeconds = config.taskPoolKeepAliveSeconds;
        this.commitBatchSize = config.commitBatchSize;
        this.entityStoreGcIntervalMillis = config.entityStoreGcIntervalMillis;
        this.memoryArenaNumber = config.memoryArenaNumber;
        this.memoryThreadCacheGcIntervalMillis = config.memoryThreadCacheGcIntervalMillis;
        this.memoryThreadCacheFlushThreshold = config.memoryThreadCacheFlushThreshold;
        this.memoryThreadCacheWatermark.putAll(config.memoryThreadCacheWatermark);
        this.memoryThreadCacheWatermarkDecayRate = config.memoryThreadCacheWatermarkDecayRate;
        this.memoryRunFreeListWatermark.putAll(config.memoryRunFreeListWatermark);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public int getCoreTaskPoolSize() {
        return this.coreTaskPoolSize;
    }

    public int getTaskPoolKeepAliveSeconds() {
        return this.taskPoolKeepAliveSeconds;
    }

    public int getMaxTaskPoolSize() {
        return this.maxTaskPoolSize;
    }

    public int getCommitBatchSize() {
        return this.commitBatchSize;
    }

    public int getEntityStoreGcIntervalMillis() {
        return this.entityStoreGcIntervalMillis;
    }

    public int getMemoryArenaNumber() {
        return this.memoryArenaNumber;
    }

    public int getMemoryThreadCacheGcIntervalMillis() {
        return this.memoryThreadCacheGcIntervalMillis;
    }

    public int getMemoryThreadCacheFlushThreshold() {
        return this.memoryThreadCacheFlushThreshold;
    }

    public Map<Integer, Map.Entry<Integer, Integer>> getMemoryThreadCacheWatermark() {
        return this.memoryThreadCacheWatermark;
    }

    public Map<Integer, Integer> getMemoryRunFreeListWatermark() {
        return this.memoryRunFreeListWatermark;
    }

    public double getMemoryThreadCacheWatermarkDecayRate() {
        return this.memoryThreadCacheWatermarkDecayRate;
    }
}
