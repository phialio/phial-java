package io.phial.memory;

import io.phial.Phial;

public class SmallSlabAllocator extends ThreadCachedSlabAllocator {
    private final int slabOffset;
    private final long initialBitmap;

    public SmallSlabAllocator(RunAllocator runAllocator,
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
        int maxSlabCount = (this.runSize - 16) / this.slabSize;
        this.slabOffset = this.runSize - maxSlabCount * this.slabSize;
        this.initialBitmap = -1L << (64 - maxSlabCount);
    }

    @Override
    protected int getSlabOffset() {
        return this.slabOffset;
    }

    @Override
    protected long getBitmapAddress(long run) {
        return run + 8;
    }

    @Override
    protected long getRunAddress(long address) {
        return address & -this.runSize;
    }

    @Override
    protected long initRun(long run) {
        Phial.UNSAFE.putLong(run + 8, this.initialBitmap);
        return run;
    }

    @Override
    protected void tryFreeRun(long run, long bitmap) {
        if (bitmap == this.initialBitmap) {
            this.availableLock.lock();
            try {
                if (!this.availableRuns.remove(run)) {
                    return;
                }
            } finally {
                this.availableLock.unlock();
            }
            long chunk = Phial.UNSAFE.getLong(run);
            this.runAllocator.free(chunk, run);
        }
    }
}
