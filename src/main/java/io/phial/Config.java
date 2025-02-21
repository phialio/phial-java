package io.phial;

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

        public Config build() {
            return new Config(this.config);
        }
    }

    private int coreTaskPoolSize = Runtime.getRuntime().availableProcessors();
    private int maxTaskPoolSize = Runtime.getRuntime().availableProcessors() * 2;
    private int taskPoolKeepAliveSeconds = 60;
    private int commitBatchSize = 100;

    private Config() {
    }

    private Config(Config config) {
        this.coreTaskPoolSize = config.coreTaskPoolSize;
        this.maxTaskPoolSize = config.maxTaskPoolSize;
        this.taskPoolKeepAliveSeconds = config.taskPoolKeepAliveSeconds;
        this.commitBatchSize = config.commitBatchSize;
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
}
