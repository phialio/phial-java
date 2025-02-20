package demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BookView implements Book {
    private final Transaction transaction;
    private final BookEntity entity;

    public BookView(Transaction transaction, BookEntity entity) {
        this.transaction = transaction;
        this.entity = entity;
    }

    @Override
    public long getId() {
        return this.entity.getId();
    }
}
