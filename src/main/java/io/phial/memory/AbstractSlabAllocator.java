package io.phial.memory;

abstract class AbstractSlabAllocator implements SlabAllocator {
    protected final int slabSize;
    protected final int runSize;

    public AbstractSlabAllocator(SizeClass sizeClass) {
        this.slabSize = sizeClass.getSlabSize();
        this.runSize = sizeClass.getRunSize();
    }

    @Override
    public long getSlabSize() {
        return this.slabSize;
    }

    public int getRunSize() {
        return this.runSize;
    }
}
