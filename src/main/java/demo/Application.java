package demo;

import io.phial.Config;
import io.phial.memory.AbstractRunAllocator;
import io.phial.memory.LargeSlabAllocator;
import io.phial.memory.MemoryArena;
import io.phial.memory.RootRunAllocator;
import io.phial.memory.SubRunAllocator;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class Application {
    static class Node {
        long a;
        long b;
        long c;
    }

    public static void main(String[] args) {
        var config = Config.newBuilder().build();
        var arena = new MemoryArena(
                config.getMemoryThreadCacheFlushThreshold(),
                new TreeMap<>(config.getMemoryThreadCacheWatermark()),
                config.getMemoryThreadCacheWatermarkDecayRate(),
                new TreeMap<>(config.getMemoryRunFreeListWatermark()));
        var allocated = new ArrayList<Map.Entry<Long, Integer>>();
        var random = new Random();
        long sum = 0;
        int count = 0;
        long start = System.currentTimeMillis();
        try {
            for (int i = 0; i < 100000; ++i) {
                int size = random.nextInt(65536 - 4096) + 4096;
                long address = arena.allocate(size);
                //Phial.UNSAFE.setMemory(address, size, (byte) (size & 0xFF));
                allocated.add(Map.entry(address, size));
                sum += size;
                ++count;
            }
            for (int i = 0; i < 10000000; ++i) {
                if (random.nextInt(2) == 0) {
                    int size = random.nextInt(65536 - 4096) + 4096;
                    long address = arena.allocate(size);
                    //Phial.UNSAFE.setMemory(address, size, (byte) (size & 0xFF));
                    allocated.add(Map.entry(address, size));
                    sum += size;
                    ++count;
                } else {
                    int index = random.nextInt(allocated.size());
                    var entry = allocated.get(index);
                    arena.free(entry.getKey(), entry.getValue());
                    allocated.set(index, allocated.get(allocated.size() - 1));
                    allocated.remove(allocated.size() - 1);
                }
            }
        } finally {
            long end = System.currentTimeMillis();
            System.out.println((end - start) / 1000.0);
            System.out.println(count + " " + sum);
            System.out.printf(
                    "largeAllocate=%d largeFree=%d runAllocate=%d subPopulate=%d subFree=%d rootPopulate=%d rootFree=%d memAllocated=%d memFreed=%d",
                    LargeSlabAllocator.largeAllocate,
                    LargeSlabAllocator.largeFree,
                    AbstractRunAllocator.runAllocate,
                    SubRunAllocator.subPopulate,
                    SubRunAllocator.subFree,
                    RootRunAllocator.rootPopulate,
                    RootRunAllocator.rootFree,
                    RootRunAllocator.memAllocated,
                    RootRunAllocator.memFreed);
        }
    }
}
