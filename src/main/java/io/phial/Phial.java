package io.phial;

import io.phial.memory.FreedMemoryGcManager;
import io.phial.memory.MemoryArena;
import io.phial.specs.EntityTableSpec;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Phial {
    public static final Unsafe UNSAFE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private static Phial instance;
    private final EntityStore entityStore = new EntityStore();
    private final MemoryArena[] memoryArenas;
    private final AtomicInteger memoryArenaIndex = new AtomicInteger();
    private final ThreadLocal<MemoryArena> localMemoryArena = new ThreadLocal<>();
    private final FreedMemoryGcManager freedMemoryGcManager;
    private final ExecutorService executorService;
    private final ScheduledExecutorService backgroundExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final TransactionCommitter transactionCommitter;
    private long revision = 0;
    private long nextRevision = 1;
    private long nextTransactionId = 1;
    private boolean inGarbageCollection;

    private final List<Long> committingTransactions = new LinkedList<>();

    public Phial() {
        this(Config.newBuilder().build());
    }

    public Phial(Config config) {
        this.executorService = new ThreadPoolExecutor(
                config.getCoreTaskPoolSize(),
                config.getMaxTaskPoolSize(),
                config.getTaskPoolKeepAliveSeconds(),
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        this.transactionCommitter = new TransactionCommitter(config, this.executorService);
        this.freedMemoryGcManager = new FreedMemoryGcManager();
        this.memoryArenas = new MemoryArena[config.getMemoryArenaNumber()];
        for (int i = 0; i < this.memoryArenas.length; ++i) {
            this.memoryArenas[i] = new MemoryArena(
                    config.getMemoryThreadCacheFlushThreshold(),
                    new TreeMap<>(config.getMemoryThreadCacheWatermark()),
                    config.getMemoryThreadCacheWatermarkDecayRate(),
                    new TreeMap<>(config.getMemoryRunFreeListWatermark()));
        }
        this.backgroundExecutorService.scheduleAtFixedRate(
                this.freedMemoryGcManager::runOnce,
                10000,
                config.getEntityStoreGcIntervalMillis(),
                TimeUnit.MILLISECONDS);
        this.backgroundExecutorService.scheduleAtFixedRate(
                () -> {
                    for (var arena : this.memoryArenas) {
                        arena.freeGarbageCollectedThreadCaches();
                    }
                },
                10000,
                config.getMemoryThreadCacheGcIntervalMillis(),
                TimeUnit.MILLISECONDS);
    }

    public static void init(Config config) {
        synchronized (Phial.class) {
            if (Phial.instance != null) {
                throw new IllegalStateException("can not init after the TransactionManager instance is created");
            }
            Phial.instance = new Phial(config);
        }
    }

    public static Phial getInstance() {
        synchronized (Phial.class) {
            if (Phial.instance == null) {
                Phial.instance = new Phial();
            }
            return Phial.instance;
        }
    }

    public void createTable(EntityTableSpec entityTableSpec) {
        var table = this.entityStore.createTable(entityTableSpec.getClazz());
        for (var indexSpec : entityTableSpec.getIndexes()) {
            table.createIndex(indexSpec.getRecordComparator(), indexSpec.isUnique());
        }
    }

    synchronized public Transaction newTransaction() {
        return new Transaction(this, this.entityStore, this.nextTransactionId++, this.revision);
    }

    public void shutdown() throws InterruptedException {
        this.executorService.shutdown();
        this.executorService.awaitTermination(1000, TimeUnit.DAYS);
        this.backgroundExecutorService.shutdown();
        this.backgroundExecutorService.awaitTermination(1000, TimeUnit.DAYS);
        for (var arena : this.memoryArenas) {
            arena.close();
        }
    }

    void commit(long transactionId) throws InterruptedException {
        var tables = this.entityStore.getAllTables();
        long revision;
        var futures = new ArrayList<CompletableFuture<Void>>();
        synchronized (this) {
            revision = this.nextRevision++;
            for (var table : tables) {
                futures.add(this.transactionCommitter.commit(table, transactionId, revision));
            }
        }
        try {
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        } catch (Exception e) {
            for (var future : futures) {
                future.cancel(false);
            }
            throw new RuntimeException("failed to commit transaction " + transactionId, e);
        }
        synchronized (this) {
            if (this.revision < revision) {
                this.revision = revision;
                if (!this.inGarbageCollection) {
                    this.inGarbageCollection = true;
                    this.executorService.submit(this::garbageCollection);
                }
            }
        }
    }

    void garbageCollection() {
        long lastRevision = 0;
        for (; ; ) {
            synchronized (this) {
                if (lastRevision == this.revision) {
                    this.inGarbageCollection = false;
                    return;
                }
                lastRevision = this.revision;
            }
            this.entityStore.garbageCollection(lastRevision);
        }
    }

    long allocate(int size) {
        var arena = this.localMemoryArena.get();
        if (arena == null) {
            arena = this.memoryArenas[this.memoryArenaIndex.getAndIncrement() % this.memoryArenas.length];
            this.localMemoryArena.set(arena);
        }
        return arena.allocate(size);
    }

    void free(long address, int size) {
        var arena = this.localMemoryArena.get();
        assert arena != null;
        arena.free(address, size);
    }
}
