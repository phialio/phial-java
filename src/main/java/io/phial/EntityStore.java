package io.phial;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EntityStore {
    private final Map<Class<?>, EntityTable> tables = new ConcurrentHashMap<>();

    public EntityTable createTable(Class<?> clazz) {
        var table = new EntityTable();
        if (this.tables.putIfAbsent(clazz, table) != null) {
            throw new IllegalArgumentException("table " + clazz.getSimpleName() + " exists");
        }
        return table;
    }

    public Collection<EntityTable> getAllTables() {
        return Collections.unmodifiableCollection(tables.values());
    }

    public EntityTable getTable(Class<?> clazz) {
        return this.tables.get(clazz);
    }

    public void garbageCollection(long revision) {
        for (var table : this.tables.values()) {
            table.garbageCollection(revision);
        }
    }
}
