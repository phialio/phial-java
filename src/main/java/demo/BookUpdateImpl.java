package demo;

import io.phial.EntityUpdate;

public class BookUpdateImpl extends BookEntity implements BookUpdate {
    @Override
    public BookUpdate withId(long id) {
        this.setId(id);
        return this;
    }
}
