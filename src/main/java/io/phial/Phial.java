package io.phial;

import io.phial.specs.EntityTableSpec;

import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Phial {
    public static final String[] EMPTY_STRING_ARRAY = new String[0];
    public static final Date[] EMPTY_DATE_ARRAY = new Date[0];
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final short[] EMPTY_SHORT_ARRAY = new short[0];
    public static final int[] EMPTY_INT_ARRAY = new int[0];
    public static final long[] EMPTY_LONG_ARRAY = new long[0];
    public static final float[] EMPTY_FLOAT_ARRAY = new float[0];
    public static final double[] EMPTY_DOUBLE_ARRAY = new double[0];
    private static Phial instance;
    private final EntityStore entityStore;
    private final ExecutorService executorService;
    private final TransactionCommitter transactionCommitter;
    private long revision = 0;
    private long nextRevision = 1;
    private long nextTransactionId = 1;
    private boolean inGarbageCollection;

    private final LinkedHashSet<Transaction> activeTransactions = new LinkedHashSet<>();
    private final List<Long> committingTransactions = new LinkedList<>();

    public Phial(Config config, EntityStore entityStore) {
        this.entityStore = entityStore;
        this.executorService = new ThreadPoolExecutor(
                config.getCoreTaskPoolSize(),
                config.getMaxTaskPoolSize(),
                config.getTaskPoolKeepAliveSeconds(),
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        this.transactionCommitter = new TransactionCommitter(config, this.executorService);
    }

    public static void init(Config config) {
        synchronized (Phial.class) {
            if (Phial.instance != null) {
                throw new IllegalStateException("can not init after the TransactionManager instance is created");
            }
            Phial.instance = new Phial(config, new EntityStore());
        }
    }

    public static Phial getInstance() {
        synchronized (Phial.class) {
            if (Phial.instance == null) {
                Phial.instance = new Phial(Config.newBuilder().build(), new EntityStore());
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
        var transaction = new Transaction(this, this.entityStore, this.nextTransactionId++, this.revision);
        this.activeTransactions.add(transaction);
        return transaction;
    }

    void commit(long transactionId) throws InterruptedException {
        var tables = this.entityStore.getAllTables();
        var countDownLatch = new CountDownLatch(tables.size());
        long revision;
        synchronized (this) {
            revision = this.nextRevision++;
            for (var table : tables) {
                this.transactionCommitter.commit(countDownLatch, table, transactionId, revision);
            }
        }
        countDownLatch.await();
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
}
