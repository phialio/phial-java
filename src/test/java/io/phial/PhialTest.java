package io.phial;

import demo.AccountUpdate;
import demo.PersonUpdate;
import demo.Transaction;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
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
            var accountIds = new ArrayList<Long>();
            for (int j = 0; j < i % 5 + 1; ++j) {
                var account = AccountUpdate.newInstance()
                        .withAccountId("" + i * 10 + j)
                        .withBalance(0)
                        .withCreatedAt(new Date())
                        .withOwnerId(person.getId());
                trx.createOrUpdateAccount(account);
                accountIds.add(account.getId());
            }
            person.withAccountIds(accountIds.stream().mapToLong(id -> id).toArray());
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
                        account.update().withBalance(account.getBalance() + i));
            }
        }
        var accounts = trx.getAllAccountWithLowerBalance(10).collect(Collectors.toList());
        assertThat(accounts.stream().map(Entity::getId).collect(Collectors.toList()),
                is(List.of(1L, 2L, 4L, 5L, 6L, 7L, 8L, 9L)));
        trx.commit();
    }
}
