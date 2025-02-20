package demo;

import io.phial.EntityUpdate;

import java.util.List;

public interface PersonUpdate extends Person, EntityUpdate {
    static PersonUpdate newInstance() {
        return new PersonUpdateImpl();
    }

    PersonUpdate withId(long id);

    PersonUpdate withName(String name);

    PersonUpdate withGender(byte gender);

    PersonUpdate withAge(short age);

    PersonUpdate withBook(Book book);

    PersonUpdate withBooks(List<Book> books);
}
