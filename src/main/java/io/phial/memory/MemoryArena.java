package io.phial.memory;

import io.phial.Phial;

import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;

import static io.phial.memory.Constants.KB;
import static io.phial.memory.Constants.MB;

public class MemoryArena {
    public static final int CHUNK_SIZE = 8 * MB;
    public static final int PAGE_SIZE = 128 * KB;

    private final SlabAllocator[] slabAllocators = new SlabAllocator[SizeClass.SIZE_CLASSES.length];
    private final RootRunAllocator rootRunAllocator;

    public MemoryArena(int cacheFlushThreshold,
                       SortedMap<Integer, Map.Entry<Integer, Integer>> cacheWatermarkMap,
                       double cacheWatermarkDecayRate,
                       SortedMap<Integer, Integer> runFreeListWatermarkMap) {
        int minRunSize = Arrays.stream(SizeClass.SIZE_CLASSES).mapToInt(SizeClass::getRunSize).min().orElse(0);
        var runAllocators = new RunAllocator[MemoryArena.getRunSizeLevel(minRunSize) + 1];
        this.rootRunAllocator =
                new RootRunAllocator(MemoryArena.getMappedValue(runFreeListWatermarkMap, CHUNK_SIZE));
        runAllocators[0] = this.rootRunAllocator;
        for (int i = 1; i < runAllocators.length; ++i) {
            var runSize = CHUNK_SIZE >> i;
            runAllocators[i] = new SubRunAllocator(
                    runAllocators[i - 1],
                    runSize,
                    MemoryArena.getMappedValue(runFreeListWatermarkMap, runSize));
        }
        for (int i = 0; i < SizeClass.SIZE_CLASSES.length; ++i) {
            var sizeClass = SizeClass.SIZE_CLASSES[i];
            var runAllocator = runAllocators[MemoryArena.getRunSizeLevel(sizeClass.getRunSize())];
            var cacheWatermark = MemoryArena.getMappedValue(cacheWatermarkMap, sizeClass.getSlabSize());
            if (sizeClass.getSlabSize() < 16) {
                this.slabAllocators[i] = new TinySlabAllocator(
                        runAllocator,
                        sizeClass,
                        cacheFlushThreshold,
                        cacheWatermark.getKey(),
                        cacheWatermark.getValue(),
                        cacheWatermarkDecayRate);
            } else if (sizeClass.getSlabSize() < 4096) {
                this.slabAllocators[i] = new SmallSlabAllocator(
                        runAllocator,
                        sizeClass,
                        cacheFlushThreshold,
                        cacheWatermark.getKey(),
                        cacheWatermark.getValue(),
                        cacheWatermarkDecayRate);
            } else {
                this.slabAllocators[i] = new LargeSlabAllocator(runAllocators, sizeClass);
            }
        }
    }

    public static int getRunSizeLevel(int runSize) {
        int normalized = Integer.highestOneBit(runSize);
        if (normalized < runSize) {
            normalized <<= 1;
        }
        return Integer.numberOfTrailingZeros(CHUNK_SIZE / normalized);
    }

    public long allocate(int size) {
        if (size > 64 * KB) {
            return Phial.UNSAFE.allocateMemory(size);
        }
        int sizeClassIndex = SizeClass.getSizeClassIndex(size);
        return this.slabAllocators[sizeClassIndex].allocate();
    }

    public void free(long address, int size) {
        if (size > 64 * KB) {
            Phial.UNSAFE.freeMemory(address);
        } else {
            int sizeClassIndex = SizeClass.getSizeClassIndex(size);
            this.slabAllocators[sizeClassIndex].free(address);
        }
    }

    public void freeGarbageCollectedThreadCaches() {
        for (var slabAllocator : this.slabAllocators) {
            if (slabAllocator instanceof ThreadCachedSlabAllocator) {
                ((ThreadCachedSlabAllocator) slabAllocator).freeGarbageCollectedThreadCaches();
            }
        }
    }

    public void close() {
        this.rootRunAllocator.freeAllAllocated();
    }

    private static <K, V> V getMappedValue(SortedMap<K, V> map, K key) {
        var tailMap = map.tailMap(key);
        if (tailMap.isEmpty()) {
            return map.get(map.lastKey());
        }
        return tailMap.get(tailMap.firstKey());
    }
}
