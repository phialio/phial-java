package io.phial.memory;

public abstract class AbstractRunAllocator implements RunAllocator {
    public static int runAllocate;
    protected final int runSize;
    protected final int watermark;
    protected final InternalRedBlackTree freeList = new InternalRedBlackTree();
    protected final SimpleLock lock = new SimpleLock();

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
        ++runAllocate;
        this.lock.lock();
        try {
            var run = this.freeList.pollFirst();
            if (run <= 0) {
                // empty
                if (this.freeList.size() != 0) {
                    throw new Error();
                }
                return this.populateFreeListAndReturnOne(this.watermark);
            }
            return run;
        } finally {
            this.lock.unlock();
        }
    }

    protected abstract long populateFreeListAndReturnOne(int count);
}
