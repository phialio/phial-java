package io.phial;

public interface EntityUpdate extends Entity {
    void setId(long id);

    EntityUpdate clone();
}
