package io.phial.memory;

import java.util.Arrays;
import java.util.stream.Stream;

import static io.phial.memory.Constants.KB;

public class SizeClass {
    static final SizeClass S8 = new SizeClass(8, 32 * KB);
    private static final int S16_START = 1;
    private static final SizeClass[] S16 = new SizeClass[]{
            new SizeClass(16, KB),
            new SizeClass(16 * 2, 2 * KB),
            new SizeClass(16 * 3, 2 * KB),
            new SizeClass(16 * 4, 4 * KB),
            new SizeClass(16 * 5, 4 * KB),
            new SizeClass(16 * 6, 4 * KB),
            new SizeClass(16 * 7, 4 * KB),
            new SizeClass(16 * 8, 8 * KB),
    };
    private static final int S64_START = S16_START + S16.length;
    private static final SizeClass[] S64 = new SizeClass[]{
            new SizeClass(64 * 3, 8 * KB),
            new SizeClass(64 * 4, 16 * KB),
            new SizeClass(64 * 5, 16 * KB),
            new SizeClass(64 * 6, 16 * KB),
            new SizeClass(64 * 7, 16 * KB),
            new SizeClass(64 * 8, 32 * KB),
    };
    private static final int S256_START = S64_START + S64.length;
    private static final SizeClass[] S256 = new SizeClass[]{
            new SizeClass(256 * 3, 32 * KB),
            new SizeClass(256 * 4, 64 * KB),
            new SizeClass(256 * 5, 64 * KB),
            new SizeClass(256 * 6, 64 * KB),
            new SizeClass(256 * 7, 64 * KB),
            new SizeClass(256 * 8, 128 * KB),
            new SizeClass(256 * 9, 128 * KB),
            new SizeClass(256 * 10, 128 * KB),
            new SizeClass(256 * 11, 128 * KB),
            new SizeClass(256 * 12, 128 * KB),
            new SizeClass(256 * 13, 128 * KB),
            new SizeClass(256 * 14, 128 * KB),
            new SizeClass(256 * 15, 128 * KB),
            new SizeClass(256 * 16, 256 * KB),
    };
    private static final int S4K_START = S256_START + S256.length;
    private static final SizeClass[] S4K = new SizeClass[]{
            new SizeClass(4 * KB * 2, 256 * KB),
            new SizeClass(4 * KB * 3, 256 * KB),
            new SizeClass(4 * KB * 4, 256 * KB),
            new SizeClass(4 * KB * 5, 256 * KB),
            new SizeClass(4 * KB * 6, 256 * KB),
            new SizeClass(4 * KB * 7, 256 * KB),
            new SizeClass(4 * KB * 8, 256 * KB),
            new SizeClass(4 * KB * 9, 256 * KB),
            new SizeClass(4 * KB * 10, 256 * KB),
            new SizeClass(4 * KB * 11, 256 * KB),
            new SizeClass(4 * KB * 12, 256 * KB),
            new SizeClass(4 * KB * 13, 256 * KB),
            new SizeClass(4 * KB * 14, 256 * KB),
            new SizeClass(4 * KB * 15, 256 * KB),
            new SizeClass(4 * KB * 16, 256 * KB),
    };

    static SizeClass[] SIZE_CLASSES = Stream.of(new SizeClass[]{S8}, S16, S64, S256, S4K)
            .flatMap(Arrays::stream)
            .toArray(SizeClass[]::new);

    private final int slabSize;
    private final int runSize;

    private SizeClass(int slabSize, int runSize) {
        this.slabSize = slabSize;
        this.runSize = runSize;
    }

    public static int getSizeClassIndex(int size) {
        if (size <= 512) {
            if (size <= 128) {
                return S16_START + ((size - 1) & -16) / 16;
            } else {
                return S64_START + ((size - 1) & -64) / 64 - 2;
            }
        } else if (size <= 4096) {
            return S256_START + ((size - 1) & -256) / 256 - 2;
        } else {
            return S4K_START + ((size - 1) & -(4 * KB)) / (4 * KB) - 1;
        }
    }

    public int getSlabSize() {
        return this.slabSize;
    }

    public int getRunSize() {
        return this.runSize;
    }
}
