package demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PersonView implements Person {
    private final Transaction transaction;
    private final PersonEntity entity;
    private Book book;
    private List<Book> books;

    public PersonView(Transaction transaction, PersonEntity entity) {
        this.transaction = transaction;
        this.entity = entity;
    }

    @Override
    public long getId() {
        return this.entity.getId();
    }

    @Override
    public String getName() {
        return this.entity.getName();
    }

    @Override
    public byte getGender() {
        return this.entity.getGender();
    }

    @Override
    public short getAge() {
        return this.entity.getAge();
    }

    @Override
    public Book getBook() {
        if (this.book == null) {
            var id = this.entity.getBookId();
            if (id > 0) {
                this.book = this.transaction.getBookById(id);
            }
        }
        return this.book;
    }

    @Override
    public List<Book> getBooks() {
        if (this.books == null) {
            this.books = new ArrayList<>();
            for (var id : this.entity.getBooksId()) {
                if (id > 0) {
                    this.books.add(this.transaction.getBookById(id));
                }
            }
        }
        return Collections.unmodifiableList(this.books);
    }

    @Override
    public PersonUpdateImpl update() {
        return new PersonUpdateImpl(this.entity);
    }
}
