package io.phial;

public class EntityTableBpTreeIndex implements EntityTableIndex {
    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public Entity get(long transactionId, long snapshotRevision, Entity key) {
        return null;
    }

    @Override
    public Entity put(Entity entity, boolean linkEntity, boolean mergeEntity) {
        return null;
    }

    @Override
    public void remove(Entity entity) {

    }

    @Override
    public void garbageCollection(long revision) {

    }
}
