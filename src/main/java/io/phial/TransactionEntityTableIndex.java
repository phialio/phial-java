package io.phial;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TransactionEntityTableIndex implements EntityTableIndex {

    private final EntityTableIndex base;
    private final EntityTableIndex patch;
    private final EntityTableIndex mainPatchIndex;

    public TransactionEntityTableIndex(EntityTableIndex base, EntityTableIndex mainPatchIndex) {
        this.base = base;
        this.patch = new EntityTableIndexImpl(base.isUnique(), base.getEntityComparator());
        this.mainPatchIndex = mainPatchIndex;
    }

    public EntityTableIndex getPatch() {
        return this.patch;
    }

    @Override
    public boolean isUnique() {
        return this.base.isUnique();
    }

    @Override
    public EntityComparator getEntityComparator() {
        return this.patch.getEntityComparator();
    }

    @Override
    public Entity get(long snapshotRevision, Entity key) {
        var entity = this.patch.get(0, key);
        if (entity != null) {
            return entity;
        }
        entity = this.base.get(snapshotRevision, key);
        if (this.mainPatchIndex != null && this.mainPatchIndex.get(0, entity) != null) {
            return null;
        }
        return entity;
    }

    @Override
    public Stream<Entity> query(long revision, Entity from, boolean fromInclusive, Entity to, boolean toInclusive) {
        var stream1 = this.base.query(revision, from, fromInclusive, to, toInclusive);
        var stream2 = this.patch.query(revision, from, fromInclusive, to, toInclusive);
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
                for (; ; ) {
                    int c = 1;
                    if (next2 == null ||
                            next1 != null
                                    && (c = comparator.compare(next1, next2)) < 0) {
                        var result = next1;
                        next1 = iterator1.hasNext() ? iterator1.next() : null;
                        if (TransactionEntityTableIndex.this.mainPatchIndex != null
                                && TransactionEntityTableIndex.this.mainPatchIndex.get(0, result) != null) {
                            continue;
                        }
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
            }
        };
        var spliterator = Spliterators.spliteratorUnknownSize(mergedIterator, Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public Entity put(Entity entity, boolean linkEntity, boolean mergeEntity) {
        var e = this.base.get(Long.MAX_VALUE, entity);
        if (e != null && e.getId() != entity.getId()) {
            throw new DuplicatedKeyException(this.base.getEntityComparator().getKeyString(entity));
        }
        return this.patch.put(entity, linkEntity, mergeEntity);
    }

    @Override
    public void garbageCollection(long revision) {
        // meaningless for this class, do nothing
    }
}
