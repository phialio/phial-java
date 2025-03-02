package io.phial.memory;

import io.phial.Phial;

import java.util.ArrayList;

public class LargeSlabAllocator extends AbstractSlabAllocator {
    private final RunAllocator allocateFrom;
    private final RunAllocator[] remainderTo;
    private final RunAllocator[] freeTo;
    private final SizeClass sizeClass;

    public LargeSlabAllocator(RunAllocator[] runAllocators, SizeClass sizeClass) {
        super(sizeClass);
        this.sizeClass = sizeClass;
        int runSizeLevel = MemoryArena.getRunSizeLevel(sizeClass.getRunSize());

        this.allocateFrom = runAllocators[runSizeLevel];

        var a = new ArrayList<RunAllocator>();
        var remainder = this.allocateFrom.getRunSize() - sizeClass.getRunSize();
        for (int i = runAllocators.length - 1; i >= 0; --i) {
            var runAllocator = runAllocators[i];
            if ((remainder & runAllocator.getRunSize()) > 0) {
                a.add(runAllocator);
            }
        }
        this.remainderTo = a.toArray(RunAllocator[]::new);

        a.clear();
        for (var runAllocator : runAllocators) {
            if ((sizeClass.getRunSize() & runAllocator.getRunSize()) > 0) {
                a.add(runAllocator);
            }
        }
        this.freeTo = a.toArray(RunAllocator[]::new);
    }

    @Override
    public long allocate() {
        long result = this.allocateFrom.allocate();
        long chunk = Phial.UNSAFE.getLong(result);
        long run = result + this.sizeClass.getRunSize();
        for (var runAllocator : this.remainderTo) {
            runAllocator.free(chunk, run);
            run += runAllocator.getRunSize();
        }
        long page = result & -MemoryArena.PAGE_SIZE;
        long pageIndex = (page - chunk) / MemoryArena.PAGE_SIZE; // 0<=pageIndex<64
        return result | pageIndex;
    }

    @Override
    public void free(long address) {
        int mask = MemoryArena.CHUNK_SIZE / MemoryArena.PAGE_SIZE;
        long page = address & -MemoryArena.PAGE_SIZE;
        long pageIndex = address & (mask - 1);
        long chunk = page - pageIndex * MemoryArena.PAGE_SIZE;
        address &= -mask;
        for (var runAllocator : this.freeTo) {
            runAllocator.free(chunk, address);
            address += runAllocator.getRunSize();
        }
    }
}
