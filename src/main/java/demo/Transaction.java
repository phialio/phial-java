package demo;

import io.phial.Entity;
import io.phial.EntityComparator;
import io.phial.Phial;
import io.phial.specs.RecordTableIndexSpec;
import io.phial.specs.RecordTableSpec;

import java.util.List;
import java.util.stream.Stream;

public class Transaction {
    private static final Phial PHIAL = Phial.getInstance();
    private final io.phial.Transaction trx;

    static {
        PHIAL.createTable(new RecordTableSpec(
                Person.class,
                new RecordTableIndexSpec(true, new EntityComparator() {
                    @Override
                    public String getKeyString(Entity entity) {
                        return "name:" + ((Person) entity).getName();
                    }

                    @Override
                    public int compare(Entity entity1, Entity entity2) {
                        return ((PersonEntity) entity1).getName().compareTo(((PersonEntity) entity2).getName());
                    }
                })));
    }

    public static Transaction newInstance() {
        return new Transaction(PHIAL.newTransaction());
    }

    public Transaction(io.phial.Transaction trx) {
        this.trx = trx;
    }

    public long getNextPersonId() {
        return this.trx.getNextId(Person.class);
    }

    public void createOrUpdatePerson(PersonUpdate person) {
        this.trx.createOrUpdateEntities(Person.class, List.of(person));
    }

    public void removePersonById(long id) {
        this.trx.removeEntitiesById(Person.class, List.of(id));
    }

    public Stream<Person> getAllPerson() {
        return this.queryPerson(1, null, false, null, false);
    }

    public Person getPersonById(long id) {
        var entity = this.trx.getEntityById(Person.class, id);
        return entity == null ? null : new PersonView(this, (PersonEntity) entity);
    }

    public Person getPersonByName(String name) {
        return this.getPersonByIndex(2, new PersonUpdateImpl().withName(name));
    }

    public Stream<Person> getAllPersonWithHigherName(String name) {
        return this.queryPerson(1, new PersonUpdateImpl().withName(name), false, null, false);
    }

    public Stream<Person> getAllPersonWithNameOrHigher(String name) {
        return this.queryPerson(1, new PersonUpdateImpl().withName(name), true, null, false);
    }

    public Stream<Person> getAllPersonWithLowerName(String name) {
        return this.queryPerson(1, null, false, new PersonUpdateImpl().withName(name), false);
    }

    public Stream<Person> getAllPersonWithNameOrLower(String name) {
        return this.queryPerson(1, null, false, new PersonUpdateImpl().withName(name), true);
    }

    public Stream<Person> getAllPersonWithinNameRange(String from,
                                                      boolean fromInclusive,
                                                      String to,
                                                      boolean toInclusive) {
        return this.queryPerson(
                1,
                new PersonUpdateImpl().withName(from),
                fromInclusive,
                new PersonUpdateImpl().withName(to),
                toInclusive);
    }

    private Person getPersonByIndex(int indexId, Person key) {
        var entity = this.trx.getEntityByIndex(Person.class, indexId, key);
        return entity == null ? null : new PersonView(this, (PersonEntity) entity);
    }

    private Stream<Person> queryPerson(int indexId,
                                       Person from,
                                       boolean fromInclusive,
                                       Person to,
                                       boolean toInclusive) {
        return this.trx.queryEntitiesByIndex(Person.class, indexId, from, fromInclusive, to, toInclusive)
                .map(entity -> new PersonView(this, (PersonEntity) entity));
    }

    public long getNextBookId() {
        return this.trx.getNextId(Book.class);
    }

    public Book getBookById(long id) {
        var entity = this.trx.getEntityById(Book.class, id);
        return entity == null ? null : new BookView(this, (BookEntity) entity);
    }

    public void commit() throws InterruptedException {
        this.trx.commit();
    }

    public void rollback() {
        this.trx.rollback();
    }
}
