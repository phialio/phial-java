package io.phial;

public abstract class AbstractEntity implements Entity {
    protected long id;

    protected long revision;

    protected Entity nextRevisionEntity;

    @Override
    public long getId() {
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getRevision() {
        return this.revision;
    }

    public void setRevision(long revision) {
        this.revision = revision;
    }

    public Entity getNextRevisionEntity() {
        return this.nextRevisionEntity;
    }

    public void setNextRevisionEntity(Entity nextRevisionEntity) {
        this.nextRevisionEntity = nextRevisionEntity;
    }

    public boolean isNull() {
        return false;
    }

    public Entity merge(Entity base) {
        return base;
    }

    @Override
    public AbstractEntity clone() {
        try {
            return (AbstractEntity) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
