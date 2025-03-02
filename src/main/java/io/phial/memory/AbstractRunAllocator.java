package io.phial.memory;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

public abstract class AbstractRunAllocator implements RunAllocator {
    protected final int runSize;
    protected final int watermark;
    protected final ConcurrentSkipListSet<Long> freeList = new ConcurrentSkipListSet<>();
    private final SimpleLock lock = new SimpleLock();

    public AbstractRunAllocator(int runSize, int watermark) {
        this.runSize = runSize;
        this.watermark = watermark;
    }

    @Override
    public int getRunSize() {
        return this.runSize;
    }

    @Override
    public long allocate() {
        for (; ; ) {
            var run = this.freeList.pollFirst();
            if (run == null) {
                if (this.lock.tryLock()) {
                    try {
                        if (this.freeList.isEmpty()) { // check again
                            this.populateFreeList(this.watermark);
                        }
                    } finally {
                        this.lock.unlock();
                    }
                } else {
                    LockSupport.parkNanos(ThreadLocalRandom.current().nextInt(100, 200));
                }
                continue;
            }
            return run;
        }
    }

    protected abstract void populateFreeList(int count);
}
