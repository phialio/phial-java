package io.phial;

import java.util.stream.Stream;

public interface EntityTableSortedIndex extends EntityTableIndex {
    EntityComparator getEntityComparator();

    Stream<Entity> query(long transactionId,
                         long revision,
                         Entity from,
                         boolean fromInclusive,
                         Entity to,
                         boolean toInclusive);
}
