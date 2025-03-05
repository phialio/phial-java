package io.phial;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class EntityTableSkipListIndex implements EntityTableSortedIndex {
    private final boolean unique;
    private final EntityComparator entityComparator;

    private final static EntityNode ENTITY_NODE_MARKER = new EntityNode();
    private final static Entity ENTITY_MARKER = new NullEntity();

    private static class EntityNode {
        Entity entity;
        EntityNode next;
    }

    private static class IndexNode {
        BaseNode baseNode;
        IndexNode down;
        IndexNode right;
    }

    private static class BaseNode {
        EntityNode entityNode;
        BaseNode next;
    }

    private IndexNode headIndexNode;

    public EntityTableSkipListIndex(boolean unique, EntityComparator entityComparator) {
        this.unique = unique;
        this.entityComparator = entityComparator;
    }

    @Override
    public boolean isUnique() {
        return this.unique;
    }

    @Override
    public EntityComparator getEntityComparator() {
        return this.entityComparator;
    }

    @Override
    public Entity get(long transactionId, long snapshotRevision, Entity key) {
        return this.getEntitySnapshot(snapshotRevision, this.getFirstEntityNode(key));
    }

    @Override
    public Stream<Entity> query(long transactionId,
                                long revision,
                                Entity from,
                                boolean fromInclusive,
                                Entity to,
                                boolean toInclusive) {
        VarHandle.acquireFence();
        var iterator = new Iterator<Entity>() {
            BaseNode baseNode = EntityTableSkipListIndex.this.findNearestBaseNode(from, fromInclusive ? EQUAL : 0);
            Entity next = this.getNext();

            Entity getNext() {
                while (this.baseNode != null) {
                    var entityNode = baseNode.entityNode;
                    this.baseNode = this.baseNode.next;
                    if (entityNode != null && entityNode != ENTITY_NODE_MARKER) {
                        var entity = EntityTableSkipListIndex.this.getEntitySnapshot(revision, entityNode);
                        if (entity != null) {
                            var c = to == null ? -1 :
                                    EntityTableSkipListIndex.this.entityComparator.compare(entity, to);
                            if (c < 0 || c == 0 && toInclusive) {
                                return entity;
                            }
                            return null;
                        }
                    }
                }
                return null;
            }

            @Override
            public boolean hasNext() {
                return this.next != null;
            }

            @Override
            public Entity next() {
                var result = this.next;
                this.next = this.getNext();
                return result;
            }
        };
        var spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public Entity put(Entity entity, boolean linkEntity, boolean mergeEntity) {
        var newEntityNode = new EntityNode();
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
                    EntityNode rightEntityNode;
                    if (rightBaseNode == null || (rightEntityNode = rightBaseNode.entityNode) == null) {
                        INDEX_NODE_RIGHT.compareAndSet(indexNode, rightIndexNode, rightIndexNode.right);
                    } else if (this.entityComparator.compare(entity, rightEntityNode.entity) > 0) {
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

            // iterate over the base node list, update the base node if a matching entity is found, or insert a new one
            for (var baseNode = firstBaseNode; ; ) {
                var nextBaseNode = baseNode.next;
                if (nextBaseNode != null) {
                    var nextEntityNode = nextBaseNode.entityNode;
                    if (nextEntityNode == ENTITY_NODE_MARKER) {
                        // the current base node is removed and can not be appended,
                        // restart from the very beginning
                        continue start;
                    }
                    if (nextEntityNode == null) {
                        // help unlink the base node to avoid busy wait
                        EntityTableSkipListIndex.unlinkBaseNode(baseNode, nextBaseNode);
                        continue;
                    }
                    var nextEntity = (AbstractEntity) nextEntityNode.entity;
                    if (nextEntity == null) {
                        // the next entity node is removed, help unlink to avoid busy wait
                        EntityTableSkipListIndex.unlinkEntityNode(nextBaseNode, nextEntityNode);
                        continue;
                    }
                    var c = this.entityComparator.compare(entity, nextEntity);
                    if (c > 0) {
                        baseNode = nextBaseNode;
                        continue;
                    }
                    if (c == 0) {
                        if (entity.getId() != nextEntity.getId()) {
                            // entity and nextEntity have the same key and different ids
                            var nextRevisionEntity = (AbstractEntity) nextEntity.nextRevisionEntity;
                            if (nextRevisionEntity == null
                                    || nextRevisionEntity.getRevision() <= ((AbstractEntity) entity).getRevision()) {
                                // check if nextEntity is overwritten by other revisions.
                                // all modified entities are inserted to the main index before secondary indexes are
                                // updated, so it is OK to have the equal sign in the condition.
                                throw new DuplicatedKeyException(this.entityComparator.getKeyString(entity));
                            }
                        }
                        newEntityNode.next = nextEntityNode;
                        if (!BASE_NODE_ENTITY_NODE.compareAndSet(nextBaseNode, nextEntityNode, newEntityNode)) {
                            // the next base node is removed, retry
                            continue;
                        }
                        if (linkEntity) {
                            nextEntity.setNextRevisionEntity(entity);
                        }
                        if (mergeEntity) {
                            newEntityNode.entity = ((AbstractEntity) entity).merge(nextEntity);
                        } else {
                            newEntityNode.entity = entity;
                        }
                        // no need to update the index
                        return newEntityNode.entity;
                    }
                }
                // try to append
                if (mergeEntity) {
                    newEntityNode.entity = ((AbstractEntity) entity).merge(null);
                    if (newEntityNode.entity == null) {
                        // the entity is for update rather than insertion, and the original one is removed.
                        return null;
                    }
                } else {
                    newEntityNode.entity = entity;
                }
                var newBaseNode = new BaseNode();
                newBaseNode.entityNode = newEntityNode;
                newBaseNode.next = nextBaseNode;
                if (!BASE_NODE_NEXT.compareAndSet(baseNode, nextBaseNode, newBaseNode)) {
                    // another base node is inserted, or the base node is removed, retry
                    continue;
                }
                this.updateIndex(newBaseNode, firstIndexNode, levels);
                return newEntityNode.entity;
            }
        }
    }

    @Override
    public void remove(Entity entity) {
        VarHandle.acquireFence();
        start:
        for (; ; ) {
            var prevBaseNode = this.findNearestBaseNode(entity, LESS_THAN);
            if (prevBaseNode == null) {
                // empty index
                throw new RuntimeException(
                        "not found, key=" + this.entityComparator.getKeyString(entity));
            }
            for (; ; ) {
                var currentBaseNode = prevBaseNode.next;
                if (currentBaseNode == null) {
                    // not found
                    throw new RuntimeException(
                            "not found, key=" + this.entityComparator.getKeyString(entity));
                }
                var currentEntityNode = currentBaseNode.entityNode;
                if (currentEntityNode == null) {
                    // help unlink
                    EntityTableSkipListIndex.unlinkBaseNode(prevBaseNode, currentBaseNode);
                    continue;
                }
                if (currentEntityNode == ENTITY_NODE_MARKER) {
                    // the previous base node is removed, restart from the very beginning
                    continue start;
                }
                var currentEntity = currentEntityNode.entity;
                if (currentEntity == null) {
                    // the current base node is removed, retry
                    continue;
                }
                var c = this.entityComparator.compare(entity, currentEntity);
                if (c == 0) {
                    EntityNode prevEntityNode = null;
                    do {
                        currentEntity = currentEntityNode.entity;
                        if (currentEntity != null && currentEntity != ENTITY_MARKER) {
                            var rev1 = ((AbstractEntity) entity).getRevision();
                            var rev2 = ((AbstractEntity) currentEntity).getRevision();
                            if (rev1 == rev2) {
                                // remove the current entity node
                                if (prevEntityNode == null) {
                                    // it is the first entity node
                                    EntityTableSkipListIndex.unlinkEntityNode(currentBaseNode, currentEntityNode);
                                    if (currentBaseNode.entityNode == null) {
                                        // all entity nodes are removed, remove the base node
                                        EntityTableSkipListIndex.unlinkBaseNode(prevBaseNode, currentBaseNode);
                                        // traverse index to clean up unnecessary index nodes
                                        this.findPredecessorByIndex(entity);
                                        this.tryReduceIndexLevel();
                                    }
                                } else {
                                    EntityTableSkipListIndex.unlinkEntityNode(prevEntityNode, currentEntityNode);
                                }
                                return;
                            } else if (rev1 > rev2) {
                                // not found
                                throw new RuntimeException(
                                        "not found, key=" + this.entityComparator.getKeyString(entity));
                            }
                        }
                        prevEntityNode = currentEntityNode;
                        currentEntityNode = currentEntityNode.next;
                    } while (currentEntityNode != null);
                    // not found
                    throw new RuntimeException(
                            "not found, key=" + this.entityComparator.getKeyString(entity));
                }
                if (c < 0) {
                    // not found
                    throw new RuntimeException(
                            "not found, key=" + this.entityComparator.getKeyString(entity));
                }
                prevBaseNode = currentBaseNode;
            }
        }
    }

    @Override
    public void garbageCollection(long revision) {
        var prevBaseNode = this.getFirstBaseNode();
        if (prevBaseNode == null) {
            // empty index
            return;
        }
        for (; ; ) {
            var currentBaseNode = prevBaseNode.next;
            if (currentBaseNode == null) {
                return;
            }
            var currentEntityNode = currentBaseNode.entityNode;
            if (currentEntityNode == null) {
                // help unlink
                EntityTableSkipListIndex.unlinkBaseNode(prevBaseNode, currentBaseNode);
                continue;
            }
            if (currentEntityNode != ENTITY_NODE_MARKER) { // ignore marker
                EntityNode prevEntityNode = null;
                do {
                    var entity = (AbstractEntity) currentEntityNode.entity;
                    if (entity.getRevision() <= revision) {
                        AbstractEntity nextRevisionEntity;
                        if (entity.isNull()
                                || (nextRevisionEntity = (AbstractEntity) entity.getNextRevisionEntity()) != null
                                && nextRevisionEntity.getRevision() <= revision) {
                            if (prevEntityNode == null) { // the base node can be removed
                                if (!BASE_NODE_ENTITY_NODE.compareAndSet(currentBaseNode, currentEntityNode, null)) {
                                    // new revisions inserted, retry
                                    continue;
                                }
                                EntityTableSkipListIndex.unlinkBaseNode(prevBaseNode, currentBaseNode);
                                // traverse index to clean up unnecessary index nodes
                                this.findPredecessorByIndex(entity);
                                this.tryReduceIndexLevel();
                                break;
                            } else {
                                prevEntityNode.next = null;
                            }
                        } else {
                            currentEntityNode.next = null;
                        }
                        break;
                    }
                    prevEntityNode = currentEntityNode;
                    currentEntityNode = currentEntityNode.next;
                } while (currentEntityNode != null);
            }
            prevBaseNode = currentBaseNode;
        }
    }

    private EntityNode getFirstEntityNode(Entity key) {
        VarHandle.acquireFence();
        var indexNode = this.headIndexNode;
        if (indexNode == null) {
            return null;
        }
        for (; ; ) {
            for (; ; ) {
                var right = indexNode.right;
                if (right == null) {
                    break;
                }
                var baseNode = right.baseNode;
                EntityNode entityNode;
                if (baseNode == null || (entityNode = baseNode.entityNode) == null) {
                    INDEX_NODE_RIGHT.compareAndSet(indexNode, right, right.right);
                } else {
                    var c = this.entityComparator.compare(key, entityNode.entity);
                    if (c > 0) {
                        indexNode = right;
                    } else if (c == 0) {
                        return entityNode;
                    } else {
                        break;
                    }
                }
            }
            var down = indexNode.down;
            if (down != null) {
                indexNode = down;
            } else {
                var baseNode = indexNode.baseNode;
                for (; ; ) {
                    var nextBaseNode = baseNode.next;
                    if (nextBaseNode == null) {
                        return null;
                    }
                    var nextEntityNode = nextBaseNode.entityNode;
                    if (nextEntityNode != null && nextEntityNode != ENTITY_NODE_MARKER) {
                        var c = this.entityComparator.compare(key, nextEntityNode.entity);
                        if (c == 0) {
                            return nextEntityNode;
                        } else if (c < 0) {
                            return null;
                        }
                    }
                    baseNode = nextBaseNode;
                }
            }
        }
    }

    private Entity getEntitySnapshot(long revision, EntityNode entityNode) {
        while (entityNode != null) {
            var entity = (AbstractEntity) entityNode.entity;
            if (entity != null && entity != ENTITY_MARKER && entity.getRevision() <= revision) {
                var nextRevisionEntity = (AbstractEntity) entity.getNextRevisionEntity();
                if (nextRevisionEntity != null && nextRevisionEntity.getRevision() <= revision) {
                    return null;
                }
                return entity;
            }
            entityNode = entityNode.next;
        }
        return null;
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
        var entityNodeToInsert = baseNodeToInsert.entityNode;
        if (entityNodeToInsert == null) {
            return false;
        }
        var entityToInsert = entityNodeToInsert.entity;
        boolean downInserted = indexNodeToInsert.down == null;
        for (; ; ) {
            var rightIndexNode = indexNode.right;
            if (rightIndexNode != null) {
                var rightBaseNode = rightIndexNode.baseNode;
                EntityNode rightEntityNode;
                if (rightBaseNode == null
                        || (rightEntityNode = rightBaseNode.entityNode) == null
                        || rightEntityNode == ENTITY_NODE_MARKER) {
                    INDEX_NODE_RIGHT.compareAndSet(indexNode, rightIndexNode, rightIndexNode.right);
                    continue;
                }
                var c = this.entityComparator.compare(entityToInsert, rightEntityNode.entity);
                if (c > 0) {
                    indexNode = rightIndexNode;
                    continue;
                }
                if (c == 0) {
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
            if (next.next != null && next.entityNode == ENTITY_NODE_MARKER) {
                next = next.next;
                break;
            } else {
                var marker = new BaseNode();
                marker.entityNode = ENTITY_NODE_MARKER;
                marker.next = next;
                if (BASE_NODE_NEXT.compareAndSet(node, next, marker)) {
                    break;
                }
            }
        }
        BASE_NODE_NEXT.compareAndSet(prev, node, next);
    }

    private static void unlinkEntityNode(BaseNode baseNode, EntityNode entityNode) {
        EntityNode next;
        for (; ; ) {
            next = entityNode.next;
            if (next.next != null && next.entity == ENTITY_MARKER) {
                next = next.next;
                break;
            } else {
                var marker = new EntityNode();
                marker.entity = ENTITY_MARKER;
                marker.next = next;
                if (ENTITY_NODE_NEXT.compareAndSet(entityNode, next, marker)) {
                    break;
                }
            }
        }
        BASE_NODE_ENTITY_NODE.compareAndSet(baseNode, entityNode, next);
    }

    private static void unlinkEntityNode(EntityNode prev, EntityNode node) {
        EntityNode next;
        for (; ; ) {
            next = node.next;
            if (next.next != null && next.entity == ENTITY_MARKER) {
                next = next.next;
                break;
            } else {
                var marker = new EntityNode();
                marker.entity = ENTITY_MARKER;
                marker.next = next;
                if (ENTITY_NODE_NEXT.compareAndSet(node, next, marker)) {
                    break;
                }
            }
        }
        ENTITY_NODE_NEXT.compareAndSet(prev, node, next);
    }

    private BaseNode findPredecessorByIndex(Entity key) {
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
                EntityNode rightEntityNode;
                if (rightBaseNode == null || (rightEntityNode = rightBaseNode.entityNode) == null) {
                    INDEX_NODE_RIGHT.compareAndSet(indexNode, rightIndexNode, rightIndexNode.right);
                } else if (this.entityComparator.compare(key, rightEntityNode.entity) > 0) {
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

    private static final int EQUAL = 1;
    private static final int LESS_THAN = 2;

    private BaseNode findNearestBaseNode(Entity key, int op) {
        if (key == null) {
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
                    return ((op & LESS_THAN) != 0 && baseNode.entityNode != ENTITY_NODE_MARKER) ? baseNode : null;
                }
                var nextEntityNode = nextBaseNode.entityNode;
                if (nextEntityNode == ENTITY_NODE_MARKER) {
                    // the current base node is removed, restart from the very beginning
                    continue start;
                }
                if (nextEntityNode == null) {
                    // help unlink the base node to avoid busy wait
                    EntityTableSkipListIndex.unlinkBaseNode(baseNode, nextBaseNode);
                    continue;
                }
                var c = this.entityComparator.compare(key, nextEntityNode.entity);
                if (c == 0 && (op & EQUAL) != 0 || (c < 0) && (op & LESS_THAN) == 0) {
                    return nextBaseNode;
                } else if (c <= 0 && (op & LESS_THAN) != 0) {
                    return (baseNode.entityNode != ENTITY_NODE_MARKER) ? baseNode : null;
                }
                baseNode = nextBaseNode;
            }
        }
    }

    private static final VarHandle HEAD;
    private static final VarHandle INDEX_NODE_RIGHT;
    private static final VarHandle BASE_NODE_NEXT;
    private static final VarHandle BASE_NODE_ENTITY_NODE;
    private static final VarHandle ENTITY_NODE_NEXT;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            HEAD = lookup.findVarHandle(EntityTableSkipListIndex.class, "headIndexNode", IndexNode.class);
            BASE_NODE_NEXT = lookup.findVarHandle(BaseNode.class, "next", BaseNode.class);
            BASE_NODE_ENTITY_NODE = lookup.findVarHandle(BaseNode.class, "entityNode", EntityNode.class);
            INDEX_NODE_RIGHT = lookup.findVarHandle(IndexNode.class, "right", IndexNode.class);
            ENTITY_NODE_NEXT = lookup.findVarHandle(EntityNode.class, "next", EntityNode.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
