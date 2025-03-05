package io.phial.memory;

import io.phial.Phial;

import java.util.ArrayList;

public class LargeSlabAllocator extends AbstractSlabAllocator {
    private final RunAllocator allocateFrom;
    private final RunAllocator[] remainderTo;
    private final RunAllocator[] freeTo;
    public static int largeAllocate;
    public static int largeFree;

    public LargeSlabAllocator(RunAllocator[] runAllocators, SizeClass sizeClass) {
        super(sizeClass);
        int runSizeLevel = MemoryArena.getRunSizeLevel(this.slabSize);

        if (runAllocators[runSizeLevel].getRunSize() == this.slabSize) {
            this.allocateFrom = runAllocators[runSizeLevel - 1];
        } else {
            this.allocateFrom = runAllocators[runSizeLevel];
        }

        var a = new ArrayList<RunAllocator>();
        var remainder = this.allocateFrom.getRunSize() - this.slabSize;
        for (var runAllocator : runAllocators) {
            if ((remainder & runAllocator.getRunSize()) > 0) {
                a.add(runAllocator);
            }
        }
        this.remainderTo = a.toArray(RunAllocator[]::new);

        a.clear();
        for (int i = runAllocators.length - 1; i >= 0; --i) {
            var runAllocator = runAllocators[i];
            if ((this.slabSize & runAllocator.getRunSize()) > 0) {
                a.add(runAllocator);
            }
        }
        this.freeTo = a.toArray(RunAllocator[]::new);
    }

    @Override
    public long allocate() {
        ++largeAllocate;
        long run = this.allocateFrom.allocate();
        long chunk = Phial.UNSAFE.getLong(run);
        long result = run + this.allocateFrom.getRunSize() - this.slabSize;
        for (var runAllocator : this.remainderTo) {
            runAllocator.free(chunk, run);
            run += runAllocator.getRunSize();
        }
        return result;
    }

    @Override
    public void free(long address) {
        ++largeFree;
        long page = address & -MemoryArena.PAGE_SIZE;
        long chunk = Phial.UNSAFE.getLong(page);
        for (var runAllocator : this.freeTo) {
            runAllocator.free(chunk, address);
            address += runAllocator.getRunSize();
        }
    }
}
