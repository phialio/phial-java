package io.phial.memory;

import java.util.TreeSet;
import java.util.function.Consumer;

public class ThreadCache {
    private final TreeSet<Long> cache = new TreeSet<>();
    private final Consumer<Long> freer;
    private final int lowWatermark;
    private final int highWatermark;
    private final int flushThreshold;
    private final double watermarkDecayRate;
    private int watermark;
    private int refused;
    private int getCounter;

    public ThreadCache(Consumer<Long> freer,
                       int flushThreshold,
                       int lowWatermark,
                       int highWatermark,
                       double watermarkDecayRate) {
        this.freer = freer;
        this.lowWatermark = lowWatermark;
        this.highWatermark = highWatermark;
        this.flushThreshold = flushThreshold;
        this.watermarkDecayRate = watermarkDecayRate;
        this.watermark = (this.lowWatermark + this.highWatermark) / 2;
    }

    public TreeSet<Long> getCache() {
        return this.cache;
    }

    public long get() {
        var result = this.cache.pollFirst();
        if (++this.getCounter >= this.flushThreshold) {
            this.getCounter = 0;
            this.flush();
        }
        if (result != null) {
            return result;
        }
        if (this.refused > 0) {
            --this.refused;
            this.watermark = Math.min(this.highWatermark, this.watermark + 1);
        }
        return -1;
    }

    public boolean put(long address) {
        if (this.cache.size() < this.watermark) {
            this.cache.add(address);
            return true;
        }
        this.refused = Math.min(this.highWatermark - this.watermark, this.refused + 1);
        return false;
    }

    private void flush() {
        int expectedWatermark = Math.max(this.lowWatermark, (int) (this.watermark * this.watermarkDecayRate));
        if (this.cache.size() > expectedWatermark) {
            this.watermark = expectedWatermark;
            var it = this.cache.descendingIterator();
            for (int i = this.cache.size(); i > this.watermark; --i) {
                this.freer.accept(it.next());
                it.remove();
            }
        }
    }
}
