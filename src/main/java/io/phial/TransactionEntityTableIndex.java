package io.phial;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TransactionEntityTableIndex implements EntityTableIndex {

    private final EntityTableIndex base;
    private final EntityTableIndex patch;

    public TransactionEntityTableIndex(EntityTableIndex base) {
        this.base = base;
        this.patch = new EntityTableIndexImpl(base.getEntityComparator());
    }

    public EntityTableIndex getPatch() {
        return this.patch;
    }

    @Override
    public EntityComparator getEntityComparator() {
        return this.patch.getEntityComparator();
    }

    @Override
    public Entity get(long snapshotRevision, Entity key) {
        var record = this.patch.get(0, key);
        return record == null ? this.base.get(snapshotRevision, key) : record;
    }

    @Override
    public Stream<Entity> query(long revision, Entity from, boolean fromInclusive, Entity to, boolean toInclusive) {
        return this.merge(this.base.query(revision, from, fromInclusive, to, toInclusive),
                this.patch.query(revision, from, fromInclusive, to, toInclusive));
    }

    @Override
    public Entity put(Entity entity, boolean linkEntity, boolean mergeEntity) {
        if (this.base.get(Long.MAX_VALUE, entity) != null) {
            throw new DuplicatedKeyException(this.base.getEntityComparator().getKeyString(entity));
        }
        return this.patch.put(entity, linkEntity, mergeEntity);
    }

    @Override
    public void garbageCollection(long revision) {
        // meaningless for this class, do nothing
    }

    private Stream<Entity> merge(Stream<Entity> stream1, Stream<Entity> stream2) {
        var iterator1 = stream1.iterator();
        var iterator2 = stream2.iterator();
        var comparator = this.base.getEntityComparator();
        var mergedIterator = new Iterator<Entity>() {
            private Entity next1 = iterator1.hasNext() ? iterator1.next() : null;
            private Entity next2 = iterator2.hasNext() ? iterator2.next() : null;

            @Override
            public boolean hasNext() {
                return next1 != null || next2 != null;
            }

            @Override
            public Entity next() {
                int c = 1;
                if (next2 == null ||
                        next1 != null
                                && (c = comparator.compare(next1, next2)) < 0) {
                    var result = next1;
                    next1 = iterator1.hasNext() ? iterator1.next() : null;
                    return result;
                } else {
                    var result = next2;
                    if (c == 0) {
                        next1 = iterator1.hasNext() ? iterator1.next() : null;
                    }
                    next2 = iterator2.hasNext() ? iterator2.next() : null;
                    return result;
                }
            }
        };
        var spliterator = Spliterators.spliteratorUnknownSize(mergedIterator, Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false);
    }
}
