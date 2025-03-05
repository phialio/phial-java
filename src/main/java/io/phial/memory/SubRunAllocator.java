package io.phial.memory;

import io.phial.Phial;

public class SubRunAllocator extends AbstractRunAllocator {
    public static int subPopulate;
    public static int subFree;
    private final RunAllocator parent;

    public SubRunAllocator(RunAllocator parent, int runSize, int watermark) {
        super(runSize, watermark);
        this.parent = parent;
    }


    @Override
    public void free(long chunk, long run) {
        ++subFree;
        long buddy = chunk + ((run - chunk) ^ this.runSize);
        this.lock.lock();
        try {
            if (this.freeList.addOrRemoveBuddy(run, buddy) != -1) {
                return;
            }
        } finally {
            this.lock.unlock();
        }
        this.parent.free(chunk, Math.min(run, buddy));
    }

    @Override
    protected long populateFreeListAndReturnOne(int count) {
        ++subPopulate;
        long result = 0;
        for (int i = 0; i < count; i += 2) {
            long run = this.parent.allocate();
            long buddy = run + this.runSize;
            Phial.UNSAFE.putLong(buddy, Phial.UNSAFE.getLong(run));
            if (i == 0) {
                result = run;
            } else {
                this.freeList.add(run);
            }
            this.freeList.add(buddy);
        }
        return result;
    }
}
