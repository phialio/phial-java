package io.phial;

import java.util.Comparator;

public interface EntityComparator extends Comparator<Entity> {
    String getKeyString(Entity entity);
}
