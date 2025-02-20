package demo;

import io.phial.EntityUpdate;

public interface BookUpdate extends Book, EntityUpdate {
    static BookUpdate newInstance() {
        return new BookUpdateImpl();
    }

    BookUpdate withId(long id);
}
