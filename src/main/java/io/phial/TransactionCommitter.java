package io.phial;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

class TransactionCommitter {
    private static final Logger logger = Logger.getLogger(TransactionCommitter.class.getName());

    private static class CommitInfo {
        CompletableFuture<Void> future;
        long transactionId;
        long revision;
    }

    private final ExecutorService executor;
    private final int commitBatchSize;
    private final Map<EntityTable, Queue<CommitInfo>> committingTables = new HashMap<>();

    public TransactionCommitter(Config config, ExecutorService executor) {
        this.executor = executor;
        this.commitBatchSize = config.getCommitBatchSize();
    }

    public CompletableFuture<Void> commit(EntityTable table, long transactionId, long revision) {
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("transaction: " + transactionId + " commiting table " + table.getName());
        }
        var info = new CommitInfo();
        info.future = new CompletableFuture<>();
        info.transactionId = transactionId;
        info.revision = revision;
        synchronized (committingTables) {
            var infoQueue = committingTables.get(table);
            if (infoQueue != null) {
                synchronized (infoQueue) {
                    infoQueue.add(info);
                }
            } else {
                var newInfoQueue = new LinkedList<CommitInfo>();
                newInfoQueue.push(info);
                committingTables.put(table, newInfoQueue);
                this.executor.submit(() -> process(table, newInfoQueue));
            }
        }
        return info.future;
    }

    private void process(EntityTable table, Queue<CommitInfo> infoQueue) {
        for (int i = 0; i < this.commitBatchSize; ++i) {
            CommitInfo info;
            synchronized (infoQueue) {
                info = infoQueue.poll();
            }
            if (info == null) {
                synchronized (committingTables) {
                    synchronized (infoQueue) {
                        info = infoQueue.poll(); // check again
                    }
                    if (info == null) {
                        committingTables.remove(table);
                        return;
                    }
                }
            }
            if (!info.future.isCancelled()) {
                try {
                    table.commit(info.transactionId, info.revision);
                    info.future.complete(null);
                } catch (Exception e) {
                    info.future.completeExceptionally(e);
                }
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("transaction " + info.transactionId + ": table " + table.getName() + " committed");
                }
            } else {
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine(
                            "transaction " + info.transactionId + ": table " + table.getName() + " commitment is canceled");
                }
            }
        }
        // yield
        this.executor.submit(() -> process(table, infoQueue));
    }
}
