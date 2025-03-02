package io.phial;

import java.util.List;
import java.util.stream.Stream;

public class Transaction {
    private final Phial phial;
    private final EntityStore store;
    private final long transactionId;
    private final long snapshotRevision;
    private boolean readOnly = true;
    private static final int COMMITTED = 1;
    private static final int ROLLED_BACK = 2;
    private int status;

    Transaction(Phial phial, EntityStore store, long transactionId, long revision) {
        this.phial = phial;
        this.store = store;
        this.transactionId = transactionId;
        this.snapshotRevision = revision;
    }

    public long getNextId(Class<?> clazz) {
        if (this.status != 0) {
            if (this.status == COMMITTED) {
                throw new IllegalStateException("transaction is committed");
            }
            throw new IllegalStateException("transaction is rolled back");
        }
        return this.store.getTable(clazz).getNextId();
    }

    public void createOrUpdateEntities(Class<?> clazz, List<EntityUpdate> entities) {
        if (this.status != 0) {
            if (this.status == COMMITTED) {
                throw new IllegalStateException("transaction is committed");
            }
            throw new IllegalStateException("transaction is rolled back");
        }
        this.store.getTable(clazz).put(this.transactionId, entities);
        this.readOnly = false;
    }

    public void removeEntitiesById(Class<?> clazz, List<Long> ids) {
        if (this.status != 0) {
            if (this.status == COMMITTED) {
                throw new IllegalStateException("transaction is committed");
            }
            throw new IllegalStateException("transaction is rolled back");
        }
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
        if (this.status != 0) {
            if (this.status == COMMITTED) {
                throw new IllegalStateException("transaction is committed");
            }
            throw new IllegalStateException("transaction is rolled back");
        }
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
        if (this.status != 0) {
            if (this.status == COMMITTED) {
                throw new IllegalStateException("transaction is committed");
            }
            throw new IllegalStateException("transaction is rolled back");
        }
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
        if (this.status != 0) {
            if (this.status == COMMITTED) {
                throw new IllegalStateException("can not commit a transaction twice");
            }
            throw new IllegalStateException("can not commit a rolled back transaction");
        }
        if (!this.readOnly) {
            this.phial.commit(this.transactionId);
        }
        this.status = COMMITTED;
    }

    public void rollback() {
        if (this.status == COMMITTED) {
            throw new IllegalStateException("can not rollback a committed transaction");
        }
        if (!this.readOnly) {
            for (var table : this.store.getAllTables()) {
                table.rollback(this.transactionId);
            }
        }
        this.status = ROLLED_BACK;
    }
}
