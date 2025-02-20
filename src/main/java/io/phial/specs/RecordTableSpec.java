package io.phial.specs;

public class RecordTableSpec {
    private final Class<?> clazz;
    private final RecordTableIndexSpec[] indexes;

    public RecordTableSpec(Class<?> clazz, RecordTableIndexSpec... indexes) {
        this.clazz = clazz;
        this.indexes = indexes;
    }

    public Class<?> getClazz() {
        return this.clazz;
    }

    public RecordTableIndexSpec[] getIndexes() {
        return this.indexes;
    }
}
