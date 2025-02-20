package demo;

import io.phial.Entity;

import java.util.List;

public interface Person extends Entity {
    String getName();

    byte getGender();

    short getAge();

    Book getBook();

    List<Book> getBooks();

    PersonUpdate update();
}
