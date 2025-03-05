package io.phial.memory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ThreadLocalRandom;

public class MemoryAddressSkipList {
    private static class IndexNode {
        BaseNode baseNode;
        IndexNode down;
        IndexNode right;
    }

    private static class BaseNode {
        long key;
        BaseNode next;
    }

    private IndexNode headIndexNode;

    private int size;

    public int size() {
        return this.size;
    }

    public long pollFirst() {
        var baseNode = this.getFirstBaseNode();
        if (baseNode == null) {
            return 0;
        }
        var nextBaseNode = baseNode.next;
        if (nextBaseNode == null) {
            return 0;
        }
        long result = nextBaseNode.key;
        if (result > 0 && BASE_NODE_KEY.compareAndSet(nextBaseNode, result, 0)) {
            this.addSize(-1);
            MemoryAddressSkipList.unlinkBaseNode(baseNode, nextBaseNode);
            return result;
        }
        return -1;
    }

    public void put(long key) {
        start:
        for (; ; ) {
            VarHandle.acquireFence();

            var firstIndexNode = this.getFirstIndexNode();
            // search the index for the first possible insertion point
            int levels = 0; // how many levels goes down when searching the index
            BaseNode firstBaseNode;
            for (var indexNode = firstIndexNode; ; ) {
                for (; ; ) {
                    var rightIndexNode = indexNode.right;
                    if (rightIndexNode == null) {
                        break;
                    }
                    var rightBaseNode = rightIndexNode.baseNode;
                    long rightKey;
                    if (rightBaseNode == null || (rightKey = rightBaseNode.key) == 0) {
                        INDEX_NODE_RIGHT.compareAndSet(indexNode, rightIndexNode, rightIndexNode.right);
                    } else if (key > rightKey) {
                        indexNode = rightIndexNode;
                    } else {
                        break;
                    }
                }
                var downIndexNode = indexNode.down;
                if (downIndexNode == null) {
                    firstBaseNode = indexNode.baseNode;
                    break;
                }
                indexNode = downIndexNode;
                ++levels;
            }

            for (var baseNode = firstBaseNode; ; ) {
                var nextBaseNode = baseNode.next;
                if (nextBaseNode != null) {
                    var nextKey = nextBaseNode.key;
                    if (nextKey == -1) {
                        // the current base node is removed and can not be appended,
                        // restart from the very beginning
                        continue start;
                    }
                    if (nextKey == 0) {
                        // help unlink the base node to avoid busy wait
                        MemoryAddressSkipList.unlinkBaseNode(baseNode, nextBaseNode);
                        continue;
                    }
                    if (key > nextKey) {
                        baseNode = nextBaseNode;
                        continue;
                    }
                    if (key == nextKey) {
                        // duplicated key
                        return;
                    }
                }
                // try to append
                var newBaseNode = new BaseNode();
                newBaseNode.key = key;
                newBaseNode.next = nextBaseNode;
                if (!BASE_NODE_NEXT.compareAndSet(baseNode, nextBaseNode, newBaseNode)) {
                    // another base node is inserted, or the base node is removed, retry
                    continue;
                }
                this.addSize(1);
                this.updateIndex(newBaseNode, firstIndexNode, levels);
                return;
            }
        }
    }

    public boolean remove(long key) {
        VarHandle.acquireFence();
        start:
        for (; ; ) {
            var prevBaseNode = this.findNearestBaseNode(key);
            if (prevBaseNode == null) {
                // empty index
                return false;
            }
            for (; ; ) {
                var currentBaseNode = prevBaseNode.next;
                if (currentBaseNode == null) {
                    // not found
                    return false;
                }
                long currentKey = currentBaseNode.key;
                if (currentKey == 0) {
                    // help unlink
                    MemoryAddressSkipList.unlinkBaseNode(prevBaseNode, currentBaseNode);
                    continue;
                }
                if (currentKey == -1) {
                    // the previous base node is removed, restart from the very beginning
                    continue start;
                }
                if (key == currentKey) {
                    boolean result = BASE_NODE_KEY.compareAndSet(currentBaseNode, currentKey, 0);
                    if (result) {
                        this.addSize(-1);
                        MemoryAddressSkipList.unlinkBaseNode(prevBaseNode, currentBaseNode);
                        // traverse index to clean up unnecessary index nodes
                        this.findPredecessorByIndex(key);
                        this.tryReduceIndexLevel();
                    }
                    return result;
                }
                if (key < currentKey) {
                    // not found
                    return false;
                }
                prevBaseNode = currentBaseNode;
            }
        }
    }

    private void addSize(int delta) {
        for (; ; ) {
            int oldSize = this.size;
            if (SIZE.compareAndSet(this, oldSize, oldSize + delta)) {
                return;
            }
        }
    }

    private IndexNode getFirstIndexNode() {
        for (; ; ) {
            var indexNode = this.headIndexNode;
            if (indexNode == null) {
                indexNode = new IndexNode();
                indexNode.baseNode = new BaseNode();
                if (!HEAD.compareAndSet(this, null, indexNode)) {
                    continue;
                }
            }
            return indexNode;
        }
    }

    private void updateIndex(BaseNode baseNode, IndexNode firstIndexNode, int maxLevels) {
        var random = ThreadLocalRandom.current();
        int lowRand = random.nextInt();
        if ((lowRand & 0x3) == 0) { // insert indexes for quarter of the base nodes
            int highRand = random.nextInt();
            long rand = ((long) highRand << 32) | ((long) lowRand & 0xffffffffL);
            int skips = maxLevels;
            IndexNode topIndexNodeToInsert = null;
            for (; ; ) {
                var newIndexNode = new IndexNode();
                newIndexNode.baseNode = baseNode;
                newIndexNode.down = topIndexNodeToInsert;
                topIndexNodeToInsert = newIndexNode;
                if (rand >= 0L || --skips < 0) {
                    break;
                } else {
                    rand <<= 1;
                }
            }
            if (this.addIndexes(firstIndexNode, skips, topIndexNodeToInsert)
                    && skips < 0
                    && this.headIndexNode == firstIndexNode) {
                // try to add new level
                var newIndexNode = new IndexNode();
                newIndexNode.baseNode = baseNode;
                newIndexNode.down = topIndexNodeToInsert;
                var newHeadIndexNode = new IndexNode();
                newHeadIndexNode.baseNode = firstIndexNode.baseNode;
                newHeadIndexNode.down = firstIndexNode;
                newHeadIndexNode.right = newIndexNode;
                HEAD.compareAndSet(this, firstIndexNode, newHeadIndexNode);
            }
        }
    }

    private boolean addIndexes(IndexNode indexNode, int skips, IndexNode indexNodeToInsert) {
        var baseNodeToInsert = indexNodeToInsert.baseNode;
        if (baseNodeToInsert == null) {
            return false;
        }
        long keyToInsert = baseNodeToInsert.key;
        if (keyToInsert == 0) {
            return false;
        }
        boolean downInserted = indexNodeToInsert.down == null;
        for (; ; ) {
            var rightIndexNode = indexNode.right;
            if (rightIndexNode != null) {
                var rightBaseNode = rightIndexNode.baseNode;
                long rightKey;
                if (rightBaseNode == null
                        || (rightKey = rightBaseNode.key) == 0
                        || rightKey == -1) {
                    INDEX_NODE_RIGHT.compareAndSet(indexNode, rightIndexNode, rightIndexNode.right);
                    continue;
                }
                if (keyToInsert > rightKey) {
                    indexNode = rightIndexNode;
                    continue;
                }
                if (keyToInsert == rightKey) {
                    return false;
                }
            }
            var downIndexNode = indexNode.down;
            if (downIndexNode != null) {
                if (skips > 0) {
                    --skips;
                    indexNode = downIndexNode;
                    continue;
                }
                if (!downInserted) {
                    if (!addIndexes(downIndexNode, 0, indexNodeToInsert.down)) {
                        return false;
                    }
                    downInserted = true;
                }
            }
            indexNodeToInsert.right = rightIndexNode;
            if (INDEX_NODE_RIGHT.compareAndSet(indexNode, rightIndexNode, indexNodeToInsert)) {
                return true;
            }
            // indexNode is updated by another thread, retry
        }
    }

    // simply copy the algorithm from java.util.ConcurrentSkipListMap.tryReduceLevel
    // please read its document for implementation details
    private void tryReduceIndexLevel() {
        IndexNode head, down, down2;
        if ((head = this.headIndexNode) != null && head.right == null &&
                (down = head.down) != null && down.right == null &&
                (down2 = down.down) != null && down2.right == null &&
                HEAD.compareAndSet(this, head, down) &&
                head.right != null) {
            HEAD.compareAndSet(this, down, head);
        }
    }

    private BaseNode getFirstBaseNode() {
        var head = this.headIndexNode;
        return head == null ? null : head.baseNode;
    }

    private static void unlinkBaseNode(BaseNode prev, BaseNode node) {
        BaseNode next;
        for (; ; ) {
            next = node.next;
            if (next != null && next.key == -1) {
                next = next.next;
                break;
            } else {
                var marker = new BaseNode();
                marker.key = -1;
                marker.next = next;
                if (BASE_NODE_NEXT.compareAndSet(node, next, marker)) {
                    break;
                }
            }
        }
        BASE_NODE_NEXT.compareAndSet(prev, node, next);
    }

    private BaseNode findPredecessorByIndex(long key) {
        VarHandle.acquireFence();
        var indexNode = this.headIndexNode;
        if (indexNode == null) {
            return null;
        }
        for (; ; ) {
            for (; ; ) {
                var rightIndexNode = indexNode.right;
                if (rightIndexNode == null) {
                    break;
                }
                var rightBaseNode = rightIndexNode.baseNode;
                long rightKey;
                if (rightBaseNode == null || (rightKey = rightBaseNode.key) == 0) {
                    INDEX_NODE_RIGHT.compareAndSet(indexNode, rightIndexNode, rightIndexNode.right);
                } else if (key > rightKey) {
                    indexNode = rightIndexNode;
                } else {
                    break;
                }
            }
            var downIndexNode = indexNode.down;
            if (downIndexNode != null) {
                indexNode = downIndexNode;
            } else {
                return indexNode.baseNode;
            }
        }
    }

    private BaseNode findNearestBaseNode(long key) {
        if (key == 0) {
            var result = this.getFirstBaseNode();
            if (result == null) {
                return null;
            }
            return result.next;
        }
        start:
        for (; ; ) {
            var baseNode = findPredecessorByIndex(key);
            if (baseNode == null) {
                return null;
            }
            for (; ; ) {
                var nextBaseNode = baseNode.next;
                if (nextBaseNode == null) {
                    return baseNode.key != -1 ? baseNode : null;
                }
                long nextKey = nextBaseNode.key;
                if (nextKey == -1) {
                    // the current base node is removed, restart from the very beginning
                    continue start;
                }
                if (nextKey == 0) {
                    // help unlink the base node to avoid busy wait
                    MemoryAddressSkipList.unlinkBaseNode(baseNode, nextBaseNode);
                    continue;
                }
                if (key <= nextKey) {
                    return baseNode;
                }
                baseNode = nextBaseNode;
            }
        }
    }

    private static final VarHandle HEAD;
    private static final VarHandle SIZE;
    private static final VarHandle INDEX_NODE_RIGHT;
    private static final VarHandle BASE_NODE_NEXT;
    private static final VarHandle BASE_NODE_KEY;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            HEAD = lookup.findVarHandle(MemoryAddressSkipList.class, "headIndexNode", IndexNode.class);
            SIZE = lookup.findVarHandle(MemoryAddressSkipList.class, "size", int.class);
            BASE_NODE_NEXT = lookup.findVarHandle(BaseNode.class, "next", BaseNode.class);
            BASE_NODE_KEY = lookup.findVarHandle(BaseNode.class, "key", long.class);
            INDEX_NODE_RIGHT = lookup.findVarHandle(IndexNode.class, "right", IndexNode.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
