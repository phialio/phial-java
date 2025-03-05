package io.phial;

public interface EntityTableIndex {
    boolean isUnique();

    Entity get(long transactionId, long snapshotRevision, Entity key);

    Entity put(Entity entity, boolean linkEntity, boolean mergeEntity);

    void remove(Entity entity);

    void garbageCollection(long revision);
}
