package io.phial.memory;

import io.phial.Phial;

import java.lang.ref.ReferenceQueue;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.LockSupport;

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
    private final ConcurrentSkipListSet<Long> availableRuns = new ConcurrentSkipListSet<>();
    private final SimpleLock runLock = new SimpleLock();

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

    protected abstract void initRun(long run);

    private long globalAllocate() {
        for (; ; ) {
            long activeRun = this.activeRun;
            long bitmapAddress = this.getBitmapAddress(activeRun);
            if (activeRun > 0) {
                long bitmap = Phial.UNSAFE.getLong(bitmapAddress);
                if (bitmap != 0) {
                    long firstFreePosition = Long.numberOfLeadingZeros(bitmap);
                    long mask = ~(1L << (63 - firstFreePosition));
                    if (Phial.UNSAFE.compareAndSwapLong(null, bitmapAddress, bitmap, bitmap & mask)) {
                        return activeRun + this.getSlabOffset() + firstFreePosition * this.slabSize;
                    }
                    continue;
                }
            }
            var run = this.availableRuns.pollFirst();
            if (run == null) {
                // try to allocate a new run
                if (this.runLock.tryLock()) {
                    try {
                        this.initRun(this.runAllocator.allocate());
                    } finally {
                        this.runLock.unlock();
                    }
                } else {
                    // spin
                    LockSupport.parkNanos(100);
                }
            } else if (Phial.UNSAFE.compareAndSwapLong(this, ACTIVE_RUN_FIELD_OFFSET, activeRun, run)) {
                // check again before throw away
                if (activeRun > 0 && Phial.UNSAFE.getLong(bitmapAddress) != 0) {
                    this.availableRuns.add(activeRun);
                }
            } else {
                // put it back
                this.availableRuns.add(run);
            }
        }
    }

    private void globalFree(long address) {
        long run = this.getRunAddress(address);
        long slabIndex = (address - this.getSlabOffset()) / this.slabSize;
        long bitmapAddress = this.getBitmapAddress(run);
        for (; ; ) {
            long bitmap = Phial.UNSAFE.getLong(bitmapAddress);
            long newBitmap = bitmap | (1L << (63 - slabIndex));
            if (Phial.UNSAFE.compareAndSwapLong(null, bitmapAddress, bitmap, newBitmap)) {
                if (bitmap == 0) {
                    this.availableRuns.add(run);
                }
                return;
            }
        }
    }
}
