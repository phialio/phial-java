package demo;

import io.phial.Entity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PersonUpdateImpl extends PersonEntity implements Person, PersonUpdate {
    private long fieldMarker;
    private final PersonEntity base;
    private Book book;
    private List<Book> books;

    public PersonUpdateImpl() {
        this.base = null;
    }

    public PersonUpdateImpl(PersonEntity base) {
        this.base = base;
    }

    public PersonUpdateImpl(PersonUpdateImpl other) {
        this.fieldMarker = other.fieldMarker;
        this.base = other.base;
        this.name = other.name;
        this.gender = other.gender;
        this.age = other.age;
    }

    @Override
    public PersonUpdate withId(long id) {
        this.setId(id);
        return this;
    }

    @Override
    public String getName() {
        return this.base != null && (this.fieldMarker & 0x1) == 0 ? this.base.getName() : this.name;
    }

    @Override
    public PersonUpdate withName(String name) {
        this.name = name;
        this.fieldMarker |= 0x1;
        return this;
    }

    @Override
    public byte getGender() {
        return this.base != null && (this.fieldMarker & 0x2) == 0 ? this.base.getGender() : this.gender;
    }

    @Override
    public PersonUpdate withGender(byte gender) {
        this.gender = gender;
        this.fieldMarker |= 0x2;
        return this;
    }

    @Override
    public short getAge() {
        return this.base != null && (this.fieldMarker & 0x4) == 0 ? this.base.getAge() : this.age;
    }

    @Override
    public PersonUpdate withAge(short age) {
        this.age = age;
        this.fieldMarker |= 0x4;
        return this;
    }

    @Override
    public long getBookId() {
        return this.base != null && (this.fieldMarker & 0x8) == 0 ? this.base.getBookId() : super.bookId;
    }

    @Override
    public Book getBook() {
        return this.book;
    }

    @Override
    public PersonUpdate withBook(Book book) {
        if (book != null && book.getId() == 0) {
            throw new IllegalArgumentException("book id should not be 0");
        }
        this.book = book;
        super.bookId = book == null ? 0 : book.getId();
        this.fieldMarker |= 0x8;
        return this;
    }

    @Override
    public long[] getBooksId() {
        if (this.base != null && (this.fieldMarker & 0x10) == 0) {
            return this.base.getBooksId();
        } else {
            if (this.books != null && super.booksId == Entity.EMPTY_LONG_ARRAY) {
                super.booksId = this.books.stream().mapToLong(book -> book.getId()).toArray();
            }
            return super.booksId;
        }
    }

    @Override
    public List<Book> getBooks() {
        return Collections.unmodifiableList(this.books);
    }

    @Override
    public PersonUpdate withBooks(List<Book> books) {
        this.books = new ArrayList<>(books);
        super.booksId = Entity.EMPTY_LONG_ARRAY;
        this.fieldMarker |= 0x10;
        return this;
    }

    @Override
    public Entity merge(Entity base) {
        if (base == null) {
            if (this.base != null) {
                return null;
            }
            return new PersonEntity(this.name, this.gender, this.age, this.getBookId(), this.getBooksId());
        }
        var basePerson = (PersonEntity) base;
        var entity = new PersonEntity(
                (this.fieldMarker & 0x1) == 0 ? basePerson.getName() : this.name,
                (this.fieldMarker & 0x2) == 0 ? basePerson.getGender() : this.gender,
                (this.fieldMarker & 0x4) == 0 ? basePerson.getAge() : this.age,
                (this.fieldMarker & 0x8) == 0 ? basePerson.getBookId() : this.getBookId(),
                (this.fieldMarker & 0x10) == 0 ? basePerson.getBooksId() : this.getBooksId());
        entity.setId(this.getId());
        entity.setRevision(this.getRevision());
        return entity;
    }

    @Override
    public PersonUpdate update() {
        return this;
    }
}
