package io.phial.memory;

import io.phial.Phial;

public class SubRunAllocator extends AbstractRunAllocator {
    private final RunAllocator parent;

    public SubRunAllocator(RunAllocator parent, int runSize, int watermark) {
        super(runSize, watermark);
        this.parent = parent;
    }


    @Override
    public void free(long chunk, long run) {
        long buddy = chunk + ((run - chunk) ^ this.runSize);
        if (this.freeList.remove(buddy)) {
            this.parent.free(chunk, Math.min(run, buddy));
            return;
        }
        this.freeList.add(run);
    }

    @Override
    protected void populateFreeList(int count) {
        for (int i = 0; i < count; i += 2) {
            long run = this.parent.allocate();
            long buddy = run + this.runSize;
            Phial.UNSAFE.putLong(buddy, Phial.UNSAFE.getLong(run));
            this.freeList.add(run);
            this.freeList.add(buddy);
        }
    }
}
