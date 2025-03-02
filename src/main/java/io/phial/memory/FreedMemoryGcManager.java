package io.phial.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class FreedMemoryGcManager {
    private static class EpochNode {
        AtomicInteger activeTransactions = new AtomicInteger();
        Queue<Map.Entry<SlabAllocator, Long>> garbageList = new ConcurrentLinkedQueue<>();
    }

    private final List<EpochNode> nodes = new ArrayList<>();
    private volatile EpochNode currentNode = new EpochNode();

    public FreedMemoryGcManager() {
        this.nodes.add(this.currentNode);
    }

    public void joinEpoch() {
        for (; ; ) {
            if (this.currentNode.activeTransactions.getAndIncrement() >= 0) {
                break;
            }
        }
    }

    public void leaveEpoch() {
        this.currentNode.activeTransactions.getAndDecrement();
    }

    public void putGarbage(SlabAllocator slabAllocator, long address) {
        this.currentNode.garbageList.add(Map.entry(slabAllocator, address));
    }

    public void runOnce() {
        if (!this.currentNode.garbageList.isEmpty()) {
            this.newEpoch();
        }
        this.garbageCollection();
    }

    private void newEpoch() {
        EpochNode newNode = new EpochNode();
        this.nodes.add(newNode);
        this.currentNode = newNode;
    }

    private void garbageCollection() {
        int i = 0;
        for (var node : nodes) {
            if (node != currentNode && node.activeTransactions.get() == 0) {
                if (node.activeTransactions.getAndAdd(-Integer.MAX_VALUE) > 0) {
                    // some transactions joined the epoch just before this if statement
                    node.activeTransactions.getAndAdd(Integer.MAX_VALUE);
                } else {
                    for (var entry : node.garbageList) {
                        entry.getKey().free(entry.getValue());
                    }
                    // garbage collected, skip this node
                    continue;
                }
            }
            this.nodes.set(i, node);
            ++i;
        }
    }

}
