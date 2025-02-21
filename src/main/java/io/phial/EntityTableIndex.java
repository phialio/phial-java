package io.phial;

import java.util.stream.Stream;

public interface EntityTableIndex {
    boolean isUnique();

    EntityComparator getEntityComparator();

    Entity get(long snapshotRevision, Entity key);

    Stream<Entity> query(long revision,
                         Entity from,
                         boolean fromInclusive,
                         Entity to,
                         boolean toInclusive);

    Entity put(Entity entity, boolean linkEntity, boolean mergeEntity);

    void garbageCollection(long revision);
}
