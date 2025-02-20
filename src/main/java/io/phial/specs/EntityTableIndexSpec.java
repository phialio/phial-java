package io.phial.specs;

import io.phial.EntityComparator;

public class EntityTableIndexSpec {
    private final boolean unique;
    private final EntityComparator entityComparator;

    public EntityTableIndexSpec(boolean unique, EntityComparator entityComparator) {
        this.unique = unique;
        this.entityComparator = entityComparator;
    }

    public boolean isUnique() {
        return this.unique;
    }

    public EntityComparator getRecordComparator() {
        return this.entityComparator;
    }
}
