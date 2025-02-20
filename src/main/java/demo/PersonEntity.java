package demo;

import io.phial.AbstractEntity;
import io.phial.Entity;

public class PersonEntity extends AbstractEntity {
    protected String name;
    protected byte gender;
    protected short age;
    protected long bookId;
    protected long[] booksId;

    public PersonEntity() {
        this.name = "";
        this.booksId = Entity.EMPTY_LONG_ARRAY;
    }

    public PersonEntity(String name, byte gender, short age, long bookId, long[] booksId) {
        this.name = name;
        this.gender = gender;
        this.age = age;
        this.bookId = bookId;
        this.booksId = booksId;
    }

    public String getName() {
        return this.name;
    }

    public byte getGender() {
        return this.gender;
    }

    public short getAge() {
        return this.age;
    }

    public long getBookId() {
        return this.bookId;
    }

    public long[] getBooksId() {
        return this.booksId;
    }
}
