package io.phial.memory;

import io.phial.Phial;

public class TinySlabAllocator extends ThreadCachedSlabAllocator {
    private static final int SLAB_SIZE = SizeClass.S8.getSlabSize();
    private static final int RUN_SIZE = SizeClass.S8.getRunSize();
    private static final int TINY_RUN_SIZE = SLAB_SIZE * 64;
    private static final int MAX_RUN_COUNT = RUN_SIZE / TINY_RUN_SIZE - 1;

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
    protected long getBitmapAddress(long run) {
        long page = run & -RUN_SIZE;
        return page + (run & RUN_SIZE) / TINY_RUN_SIZE * 8;
    }

    @Override
    protected long getRunAddress(long address) {
        return address & -TINY_RUN_SIZE;
    }

    @Override
    protected void initRun(long run) {
        Phial.UNSAFE.setMemory(run + 8, MAX_RUN_COUNT, (byte) -1);
    }
}
