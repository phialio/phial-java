package io.phial;

public class NullEntity extends AbstractEntity implements EntityUpdate {
    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    public Entity merge(Entity base) {
        return this;
    }
}
