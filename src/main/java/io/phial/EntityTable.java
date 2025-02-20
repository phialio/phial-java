package io.phial;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EntityTable {
    private final AtomicLong nextId = new AtomicLong(1);

    private final List<EntityTableIndex> indexes = new ArrayList<>();

    private class TransactionPatch {
        final List<TransactionEntityTableIndex> indexes = EntityTable.this.indexes.stream()
                .map(TransactionEntityTableIndex::new)
                .collect(Collectors.toList());
        final Map<Long, Object> removedIds = new ConcurrentHashMap<>();
    }

    private final Map<Long, TransactionPatch> transactionPatches = new ConcurrentHashMap<>();

    public EntityTable() {
        var comp = new EntityComparator() {
            @Override
            public String getKeyString(Entity entity) {
                return "id:" + entity.getId();
            }

            @Override
            public int compare(Entity entity1, Entity entity2) {
                return Long.compare(entity1.getId(), entity2.getId());
            }
        };
        this.indexes.add(new EntityTableIndexImpl(comp));
    }

    public void createIndex(EntityComparator comparator, boolean unique) {
        var comp = unique ? comparator : new EntityComparator() {
            @Override
            public String getKeyString(Entity entity) {
                return "id:" + entity.getId() + " " + comparator.getKeyString(entity);
            }

            @Override
            public int compare(Entity entity1, Entity entity2) {
                var c = comparator.compare(entity1, entity2);
                if (c == 0) {
                    c = Long.compare(entity1.getId(), entity2.getId());
                }
                return c;
            }
        };
        this.indexes.add(new EntityTableIndexImpl(comp));
    }

    public long getNextId() {
        return this.nextId.getAndIncrement();
    }

    public Entity getByIndex(long transactionId, int indexId, long snapshotRevision, Entity key) {
        var transactionPatch = this.getTransactionPatch(transactionId, false);
        EntityTableIndex index;
        if (transactionPatch == null) {
            index = this.indexes.get(indexId - 1);
        } else {
            index = transactionPatch.indexes.get(indexId - 1);
        }
        var entity = index.get(snapshotRevision, key);
        if (transactionPatch != null && transactionPatch.removedIds.containsKey(entity.getId())) {
            return null;
        }
        return entity;
    }

    public Stream<Entity> queryByIndex(long transactionId,
                                       int indexId,
                                       long snapshotRevision,
                                       Entity from,
                                       boolean fromInclusive,
                                       Entity to,
                                       boolean toInclusive) {
        var transactionPatch = this.getTransactionPatch(transactionId, false);
        EntityTableIndex index;
        if (transactionPatch == null) {
            index = this.indexes.get(indexId - 1);
        } else {
            index = transactionPatch.indexes.get(indexId - 1);
        }
        var stream = index.query(snapshotRevision, from, fromInclusive, to, toInclusive)
                .filter(entity -> !((AbstractEntity) entity).isNull());
        if (transactionPatch != null) {
            return stream.filter(entity -> !transactionPatch.removedIds.containsKey(entity.getId()));
        }
        return stream;
    }

    public void put(long transactionId, List<EntityUpdate> entities) {
        var transactionPatch = this.getTransactionPatch(transactionId, true);
        var mainIndex = transactionPatch.indexes.get(0);
        for (var entity : entities) {
            ((AbstractEntity) entity).setRevision(0);
            var id = entity.getId();
            if (id == 0) {
                entity.setId(this.nextId.getAndIncrement());
            } else {
                transactionPatch.removedIds.remove(id);
            }
            entity = entity.clone();
            for (var index : transactionPatch.indexes) {
                index.put(entity, index == mainIndex, false);
            }
        }
    }

    public boolean remove(long transactionId, long revision, List<Long> ids) {
        var ret = false;
        var transactionPatch = this.getTransactionPatch(transactionId, true);
        var mainIndex = transactionPatch.indexes.get(0);
        var nullEntity = new NullEntity();
        nullEntity.setRevision(Long.MAX_VALUE);
        for (var id : ids) {
            if (transactionPatch.removedIds.containsKey(id)) {
                continue;
            }
            nullEntity.setId(id);
            var entity = mainIndex.get(revision, nullEntity);
            if (entity != null) {
                ret = true;
                mainIndex.put(nullEntity, true, false);
                transactionPatch.removedIds.put(id, 0);
            }
        }
        return ret;
    }

    public void commit(long transactionId, long revision) {
        var transactionPatch = this.getTransactionPatch(transactionId, false);
        if (transactionPatch == null) {
            return;
        }
        var mainIndex = this.indexes.get(0);
        var mainPatchIndex = transactionPatch.indexes.get(0).getPatch();
        var from = new NullEntity();
        var to = new NullEntity();
        to.setId(Long.MAX_VALUE);
        mainPatchIndex.query(0, from, true, to, true)
                .forEach(entity -> {
                    ((AbstractEntity) entity).setRevision(revision);
                    for (var index : EntityTable.this.indexes) {
                        if (index == mainIndex) {
                            entity = index.put(entity, true, true);
                        } else {
                            index.put(entity, false, false);
                        }
                    }
                });
        for (var entry : transactionPatch.removedIds.entrySet()) {
            var id = entry.getKey();
            var entity = new NullEntity();
            entity.setId(id);
            entity.setRevision(revision);
            mainIndex.put(entity, true, false);
        }
    }

    public void rollback(long transactionId) {
        this.transactionPatches.remove(transactionId);
    }

    public void garbageCollection(long revision) {
        for (var index : this.indexes) {
            index.garbageCollection(revision);
        }
    }

    private TransactionPatch getTransactionPatch(long transactionId, boolean createIfAbsent) {
        if (createIfAbsent) {
            return this.transactionPatches.computeIfAbsent(transactionId, id -> new TransactionPatch());
        }
        return this.transactionPatches.get(transactionId);
    }
}
