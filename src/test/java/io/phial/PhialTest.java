package io.phial;

import demo.AccountUpdate;
import demo.PersonUpdate;
import demo.Transaction;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class PhialTest {
    @Test
    public void testNormal() throws InterruptedException {
        var trx = Transaction.newInstance();
        for (int i = 0; i < 100; ++i) {
            var person = PersonUpdate.newInstance()
                    .withPersonId("" + i)
                    .withName("name" + i);
            trx.createOrUpdatePerson(person);
            for (int j = 0; j < i % 5 + 1; ++j) {
                var account = AccountUpdate.newInstance()
                        .withAccountId("" + i * 10 + j)
                        .withBalance(0)
                        .withOwnerId(person.getId());
                trx.createOrUpdateAccount(account);
            }
            trx.createOrUpdatePerson(person);
        }
        for (int i = 1; i < 100; i += 10) {
            trx.removePersonById(i);
        }
        trx.commit();
        trx = Transaction.newInstance();
        for (int i = 1; i <= 10; ++i) {
            var person = trx.getPersonById(i);
            if (person != null) {
                trx.createOrUpdatePerson(person.update().withName("nn" + i));
            }
        }
        for (int i = 3; i < 1000; i += 10) {
            trx.removeAccountById(i);
        }
        for (int i = 0; i <= 1000; ++i) {
            var account = trx.getAccountById(i);
            if (account != null) {
                trx.createOrUpdateAccount(
                        account.update().withBalance(account.getBalance() + i / 2));
            }
        }
        var accounts = trx.getAllAccountWithLowerBalance(10).collect(Collectors.toList());
        assertThat(accounts.stream().map(Entity::getId).collect(Collectors.toList()),
                is(List.of(1L, 2L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 14L, 15L, 16L, 17L, 18L, 19L)));
        trx.commit();
        trx = Transaction.newInstance();
        accounts = trx.getAllAccountWithLowerBalance(10).collect(Collectors.toList());
        assertThat(accounts.stream().map(Entity::getId).collect(Collectors.toList()),
                is(List.of(1L, 2L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 14L, 15L, 16L, 17L, 18L, 19L)));
        accounts = trx.getAllAccountWithHigherBalance(140).collect(Collectors.toList());
        assertThat(accounts.stream().map(Entity::getId).collect(Collectors.toList()),
                is(List.of(282L, 284L, 285L, 286L, 287L, 288L, 289L, 290L, 291L, 292L, 294L, 295L, 296L, 297L, 298L,
                        299L, 300L)));
    }

    @Test
    public void testDuplicatedKey() {
    }
}
