package io.phial.memory;

import io.phial.Phial;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RootRunAllocator extends AbstractRunAllocator {
    public static int rootPopulate;
    public static int rootFree;
    public static int memAllocated;
    public static int memFreed;
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
    protected long populateFreeListAndReturnOne(int count) {
        ++rootPopulate;
        long result = 0;
        for (int i = 0; i < count; ++i) {
            ++memAllocated;
            long allocated = Phial.UNSAFE.allocateMemory(MemoryArena.CHUNK_SIZE + MemoryArena.PAGE_SIZE);
            long chunk = ((allocated - 1) & -MemoryArena.PAGE_SIZE) + MemoryArena.PAGE_SIZE;
            this.allocated.put(chunk, allocated);
            Phial.UNSAFE.putLong(chunk, chunk);
            if (i == 0) {
                result = chunk;
            } else {
                this.freeList.add(chunk);
            }
        }
        return result;
    }

    @Override
    public void free(long chunk, long run) {
        ++rootFree;
        this.lock.lock();
        long[] toFree = null;
        try {
            this.freeList.add(run);
            if (this.freeList.size() > this.watermark) {
                toFree = new long[this.freeList.size() - this.watermark];
                for (int i = 0; i < toFree.length; ++i) {
                    var allocated = this.allocated.get(this.freeList.pollFirst());
                    assert allocated != null;
                    toFree[i] = allocated;
                }
            }
        } finally {
            this.lock.unlock();
        }
        if (toFree != null) {
            for (long address : toFree) {
                Phial.UNSAFE.freeMemory(address);
            }
        }
    }
}
