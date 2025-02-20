package io.phial;

import io.phial.specs.RecordTableSpec;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Phial {
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

    public void createTable(RecordTableSpec recordTableSpec) {
        var table = this.entityStore.createTable(recordTableSpec.getClazz());
        for (var indexSpec : recordTableSpec.getIndexes()) {
            table.createIndex(indexSpec.getRecordComparator(), indexSpec.isUnique());
        }
    }

    synchronized public Transaction newTransaction() {
        var transaction = new Transaction(this, this.entityStore, this.nextTransactionId++, this.revision);
        this.activeTransactions.add(transaction);
        return transaction;
    }

    void commit(long transactionId) throws InterruptedException {
        long revision;
        synchronized (this) {
            this.committingTransactions.add(transactionId);
            while (transactionId != this.committingTransactions.get(0)) {
                this.wait();
            }
            revision = this.nextRevision++;
        }
        var tables = this.entityStore.getAllTables();
        var countDownLatch = new CountDownLatch(tables.size());
        for (var table : tables) {
            this.transactionCommitter.commit(countDownLatch, table, transactionId, revision);
        }
        countDownLatch.await();
        synchronized (this) {
            this.committingTransactions.remove(0);
            this.notifyAll();
        }
    }

    void closeTransaction(Transaction transaction) {
        synchronized (this) {
            this.activeTransactions.remove(transaction);
            var newRevision = this.activeTransactions.isEmpty()
                    ? this.nextRevision - 1
                    : this.activeTransactions.iterator().next().getSnapshotRevision();
            if (this.revision < newRevision) {
                this.revision = newRevision;
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
