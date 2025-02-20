package demo;

public class Application {
    public static void main(String[] args) {
        var transaction = Transaction.newInstance();
        transaction.createOrUpdatePerson(PersonUpdate.newInstance()
                .withName("t")
                .withGender((byte) 0)
                .withAge((short) 1)
                .withBook(BookUpdate.newInstance().withId(transaction.getNextBookId())));
    }
}
