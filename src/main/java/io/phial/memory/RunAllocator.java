package io.phial.memory;

public interface RunAllocator {
    int getRunSize();

    long allocate();

    void free(long chunk, long run);
}
