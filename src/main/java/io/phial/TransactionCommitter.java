package io.phial;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

class TransactionCommitter {
    private static class CommitInfo {
        CountDownLatch countDownLatch;
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

    public void commit(CountDownLatch countDownLatch, EntityTable table, long transactionId, long revision) {
        var info = new CommitInfo();
        info.countDownLatch = countDownLatch;
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
            table.commit(info.transactionId, info.revision);
            info.countDownLatch.countDown();
        }
        // yield
        this.executor.submit(() -> process(table, infoQueue));
    }
}
