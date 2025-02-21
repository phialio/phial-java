package io.phial.specs;

public class EntityTableSpec {
    private final Class<?> clazz;
    private final EntityTableIndexSpec[] indexes;

    public EntityTableSpec(Class<?> clazz, EntityTableIndexSpec... indexes) {
        this.clazz = clazz;
        this.indexes = indexes;
    }

    public Class<?> getClazz() {
        return this.clazz;
    }

    public EntityTableIndexSpec[] getIndexes() {
        return this.indexes;
    }
}
