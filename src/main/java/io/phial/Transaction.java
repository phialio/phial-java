package io.phial;

import java.util.List;
import java.util.stream.Stream;

public class Transaction {
    private final Phial phial;
    private final EntityStore store;
    private final long transactionId;
    private final long snapshotRevision;
    private boolean readOnly = true;

    Transaction(Phial phial, EntityStore store, long transactionId, long revision) {
        this.phial = phial;
        this.store = store;
        this.transactionId = transactionId;
        this.snapshotRevision = revision;
    }

    long getSnapshotRevision() {
        return this.snapshotRevision;
    }

    public long getNextId(Class<?> clazz) {
        return this.store.getTable(clazz).getNextId();
    }

    public void createOrUpdateEntities(Class<?> clazz, List<EntityUpdate> entities) {
        this.store.getTable(clazz).put(this.transactionId, entities);
        this.readOnly = false;
    }

    public void removeEntitiesById(Class<?> clazz, List<Long> ids) {
        if (this.store.getTable(clazz).remove(this.transactionId, this.snapshotRevision, ids)) {
            this.readOnly = false;
        }
    }

    public Entity getEntityById(Class<?> clazz, long id) {
        var entity = new NullEntity();
        entity.setId(id);
        return this.getEntityByIndex(clazz, 1, entity);
    }

    public Entity getEntityByIndex(Class<?> clazz, int indexId, Entity key) {
        var table = this.store.getTable(clazz);
        var entity = table.getByIndex(this.transactionId, indexId, this.snapshotRevision, key);
        if (entity == null) {
            return null;
        }
        return ((AbstractEntity) entity).isNull() ? null : entity;
    }

    public Stream<Entity> queryEntitiesByIndex(
            Class<?> clazz,
            int indexId,
            Entity from,
            boolean fromInclusive,
            Entity to,
            boolean toInclusive) {
        var table = this.store.getTable(clazz);
        return table.queryByIndex(
                this.transactionId,
                indexId,
                this.snapshotRevision,
                from,
                fromInclusive,
                to,
                toInclusive);
    }

    public void commit() throws InterruptedException {
        if (!this.readOnly) {
            this.phial.commit(this.transactionId);
        }
    }

    public void rollback() {
        if (!this.readOnly) {
            for (var table : this.store.getAllTables()) {
                table.rollback(this.transactionId);
            }
        }
        this.phial.closeTransaction(this);
    }
}
