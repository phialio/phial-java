package io.phial.memory;

public interface SlabAllocator {
    long allocate();

    void free(long address);

    long getSlabSize();
}
