package io.phial.memory;

import io.phial.Phial;

public class TinySlabAllocator extends ThreadCachedSlabAllocator {
    private static final int SLAB_SIZE = SizeClass.S8.getSlabSize();
    private static final int RUN_SIZE = SizeClass.S8.getRunSize();
    private static final int TINY_RUN_SIZE = SLAB_SIZE * 64;
    private static final int MAX_TINY_RUN_COUNT = RUN_SIZE / TINY_RUN_SIZE - 1;

    public TinySlabAllocator(RunAllocator runAllocator,
                             SizeClass sizeClass,
                             int cacheFlushThreshold,
                             int cacheLowWatermark,
                             int cacheHighWatermark,
                             double cacheWatermarkDecayRate) {
        super(runAllocator,
                sizeClass,
                cacheFlushThreshold,
                cacheLowWatermark,
                cacheHighWatermark,
                cacheWatermarkDecayRate);
    }

    @Override
    protected int getSlabOffset() {
        return 0;
    }

    @Override
    protected long getBitmapAddress(long tinyRun) {
        long run = tinyRun & -RUN_SIZE;
        return run + (tinyRun & (RUN_SIZE - 1)) / TINY_RUN_SIZE * SLAB_SIZE;
    }

    @Override
    protected long getRunAddress(long address) {
        return address & -TINY_RUN_SIZE;
    }

    @Override
    protected long initRun(long run) {
        Phial.UNSAFE.setMemory(run + 8, MAX_TINY_RUN_COUNT, (byte) -1);
        for (long i = 2; i <= MAX_TINY_RUN_COUNT; ++i) {
            this.availableRuns.add(run + i * TINY_RUN_SIZE);
        }
        return run + TINY_RUN_SIZE;
    }

    @Override
    protected void tryFreeRun(long tinyRun, long bitmap) {
        if (bitmap == -1) {
            long run = tinyRun & -RUN_SIZE;
            for (long i = 1; i <= MAX_TINY_RUN_COUNT; ++i) {
                if (Phial.UNSAFE.getLong(run + i * SLAB_SIZE) != -1) {
                    return;
                }
            }
            this.availableLock.lock();
            try {
                if (!this.availableRuns.containsRange(
                        run + TINY_RUN_SIZE,
                        run + (long) MAX_TINY_RUN_COUNT * TINY_RUN_SIZE,
                        TINY_RUN_SIZE)) {
                    return;
                }
                for (long i = 1; i <= MAX_TINY_RUN_COUNT; ++i) {
                    this.availableRuns.remove(run + i * TINY_RUN_SIZE);
                }
            } finally {
                this.availableLock.unlock();
            }
            long chunk = Phial.UNSAFE.getLong(run);
            this.runAllocator.free(chunk, run);
        }
    }
}
