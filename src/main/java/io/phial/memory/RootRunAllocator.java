package io.phial.memory;

import io.phial.Phial;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RootRunAllocator extends AbstractRunAllocator {
    private final Map<Long, Long> allocated = new ConcurrentHashMap<>();

    public RootRunAllocator(int watermark) {
        super(MemoryArena.CHUNK_SIZE, watermark);
    }

    public void freeAllAllocated() {
        for (long address : this.allocated.values()) {
            Phial.UNSAFE.freeMemory(address);
        }
    }

    @Override
    protected void populateFreeList(int count) {
        for (int i = 0; i < count; ++i) {
            long allocated = Phial.UNSAFE.allocateMemory(MemoryArena.CHUNK_SIZE + MemoryArena.PAGE_SIZE);
            long chunk = ((allocated - 1) & -MemoryArena.PAGE_SIZE) + MemoryArena.PAGE_SIZE;
            this.allocated.put(chunk, allocated);
            Phial.UNSAFE.putLong(chunk, chunk);
            this.freeList.add(chunk);
        }
    }

    @Override
    public void free(long chunk, long run) {
        this.freeList.add(run);
        while (this.freeList.size() > this.watermark) {
            var allocated = this.allocated.get(this.freeList.pollLast());
            assert allocated != null;
            Phial.UNSAFE.freeMemory(allocated);
        }
    }
}
