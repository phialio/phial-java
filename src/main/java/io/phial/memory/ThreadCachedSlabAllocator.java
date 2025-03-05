package io.phial.memory;

import io.phial.Phial;

import java.lang.ref.ReferenceQueue;
import java.util.HashSet;
import java.util.Set;

public abstract class ThreadCachedSlabAllocator extends AbstractSlabAllocator {
    private static final long ACTIVE_RUN_FIELD_OFFSET;

    static {
        try {
            ACTIVE_RUN_FIELD_OFFSET =
                    Phial.UNSAFE.objectFieldOffset(ThreadCachedSlabAllocator.class.getDeclaredField("activeRun"));
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    protected final RunAllocator runAllocator;
    private final int cacheFlushThreshold;
    private final int cacheLowWatermark;
    private final int cacheHighWatermark;
    private final double cacheWatermarkDecayRate;
    private final ReferenceQueue<ThreadCache> referenceQueue;
    private final ThreadLocal<ThreadCache> cache = new ThreadLocal<>();
    private final Set<ThreadCacheReference> referenceSet = new HashSet<>();
    private long activeRun;
    protected final InternalRedBlackTree availableRuns = new InternalRedBlackTree();
    protected final SimpleLock availableLock = new SimpleLock();

    public ThreadCachedSlabAllocator(RunAllocator runAllocator,
                                     SizeClass sizeClass,
                                     int cacheFlushThreshold,
                                     int cacheLowWatermark,
                                     int cacheHighWatermark,
                                     double cacheWatermarkDecayRate) {
        super(sizeClass);
        this.runAllocator = runAllocator;
        this.cacheLowWatermark = cacheLowWatermark;
        this.cacheHighWatermark = cacheHighWatermark;
        this.cacheFlushThreshold = cacheFlushThreshold;
        this.cacheWatermarkDecayRate = cacheWatermarkDecayRate;
        this.referenceQueue = new ReferenceQueue<>();
    }

    @Override
    public long allocate() {
        var localCache = this.cache.get();
        if (localCache == null) {
            localCache = new ThreadCache(this::globalFree,
                    this.cacheFlushThreshold,
                    this.cacheLowWatermark,
                    this.cacheHighWatermark,
                    this.cacheWatermarkDecayRate);
            this.referenceSet.add(new ThreadCacheReference(localCache, this.referenceQueue));
            this.cache.set(localCache);
        }
        long result = localCache.get();
        if (result > 0) {
            return result;
        }
        return this.globalAllocate();
    }

    @Override
    public void free(long address) {
        var localCache = this.cache.get();
        assert localCache != null;
        if (!localCache.put(address)) {
            this.globalFree(address);
        }
    }

    public void freeGarbageCollectedThreadCaches() {
        for (; ; ) {
            var ref = this.referenceQueue.poll();
            if (ref == null) {
                return;
            }
            for (var address : ((ThreadCacheReference) ref).getCache()) {
                this.globalFree(address);
            }
            this.referenceSet.remove(ref);
        }
    }

    protected abstract int getSlabOffset();

    protected abstract long getBitmapAddress(long run);

    protected abstract long getRunAddress(long address);

    protected abstract long initRun(long run);

    protected abstract void tryFreeRun(long run, long bitmap);

    private long globalAllocate() {
        for (; ; ) {
            long run = this.activeRun;
            long bitmapAddress = this.getBitmapAddress(run);
            long bitmap = Phial.UNSAFE.getLong(bitmapAddress);
            if (bitmap == 0) {
                this.updateActiveRun();
                continue;
            }
            long firstFreePosition = Long.numberOfLeadingZeros(bitmap);
            long mask = ~(1L << (63 - firstFreePosition));
            long newBitmap = bitmap & mask;
            if (!Phial.UNSAFE.compareAndSwapLong(null, bitmapAddress, bitmap, newBitmap)) {
                continue;
            }
            if (newBitmap == 0) {
                this.updateActiveRun();
            }
            return run + this.getSlabOffset() + firstFreePosition * this.slabSize;
        }
    }

    private void updateActiveRun() {
        this.availableLock.lock();
        long bitmapAddress = this.getBitmapAddress(this.activeRun);
        try {
            long bitmap = Phial.UNSAFE.getLong(bitmapAddress);
            if (bitmap == 0) { // check again before update
                long nextRun = this.availableRuns.pollFirst();
                if (nextRun == 0) {
                    long newRun = this.runAllocator.allocate();
                    nextRun = this.initRun(newRun);
                }
                this.activeRun = nextRun;
            }
        } finally {
            this.availableLock.unlock();
        }
    }

    private void globalFree(long address) {
        long run = this.getRunAddress(address);
        long slabIndex = (address - this.getSlabOffset()) / this.slabSize;
        long bitmapAddress = this.getBitmapAddress(run);
        for (; ; ) {
            long bitmap = Phial.UNSAFE.getLong(bitmapAddress);
            long newBitmap = bitmap | (1L << (63 - slabIndex));
            if (!Phial.UNSAFE.compareAndSwapLong(null, bitmapAddress, bitmap, newBitmap)) {
                continue;
            }
            if (bitmap == 0) {
                this.availableLock.lock();
                try {
                    if (run != this.activeRun && Phial.UNSAFE.getLong(bitmapAddress) != 0) { // check again
                        this.availableRuns.add(run);
                    }
                } finally {
                    this.availableLock.unlock();
                }
            } else {
                this.tryFreeRun(run, newBitmap);
            }
            return;
        }
    }
}

