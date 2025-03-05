package io.phial;

import io.phial.memory.MemoryArena;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

public class ArenaTest {
    private MemoryArena arena;

    @BeforeEach
    public void setUp() {
        var config = Config.newBuilder().build();
        this.arena = new MemoryArena(
                config.getMemoryThreadCacheFlushThreshold(),
                new TreeMap<>(config.getMemoryThreadCacheWatermark()),
                config.getMemoryThreadCacheWatermarkDecayRate(),
                new TreeMap<>(config.getMemoryRunFreeListWatermark()));

    }

    @AfterEach
    public void tearDown() {
        this.arena.close();
    }

    @Test
    public void testSingleThread() {
        var allocated = new ArrayList<Map.Entry<Long, Integer>>();
        var random = new Random();
        var a = new TreeSet<Long>();
        for (int i = 0; i < 10000000; ++i) {
            long v = random.nextLong();
            a.add(v);
            a.remove(v);
        }
        if (!a.isEmpty()) {
            return;
        }
        long sum = 0;
        int count = 0;
        long start = System.currentTimeMillis();
        try {
            for (int i = 0; i < 100000; ++i) {
                int size = random.nextInt(65536) + 1;
                long address = this.arena.allocate(size);
                //Phial.UNSAFE.setMemory(address, size, (byte) (size & 0xFF));
                allocated.add(Map.entry(address, size));
                sum += size;
                ++count;
            }
            for (int i = 0; i < 10000000; ++i) {
                if (random.nextInt(2) == 0) {
                    int size = random.nextInt(65536) + 1;
                    long address = this.arena.allocate(size);
                    //Phial.UNSAFE.setMemory(address, size, (byte) (size & 0xFF));
                    allocated.add(Map.entry(address, size));
                    sum += size;
                    ++count;
                } else {
                    int index = random.nextInt(allocated.size());
                    var entry = allocated.get(index);
                    this.arena.free(entry.getKey(), entry.getValue());
                }
            }
        } finally {
            long end = System.currentTimeMillis();
            System.out.println((end - start) / 1000.0);
            System.out.println(count + " " + sum);
        }
    }
}
