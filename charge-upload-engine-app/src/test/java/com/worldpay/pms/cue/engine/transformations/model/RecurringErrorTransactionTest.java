package com.worldpay.pms.cue.engine.transformations.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.cue.engine.recurring.RecurringErrorTransaction;
import com.worldpay.pms.cue.engine.recurring.RecurringResultRow;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

public class RecurringErrorTransactionTest {

  @Test
  void testErrorTransactionWhenFailsWithException() {
    RecurringErrorTransaction errorTransaction =
        RecurringErrorTransaction.of(
            RecurringResultRow.builder()
                .frequencyIdentifier("WPDY")
                .currency("GBM")
                .recurringSourceId("12345")
                .txnHeaderId("test")
                .partyId("PO4000398254")
                .build(),
            new SQLException("cannot insert null value in sub_acct"));
    assertThat(errorTransaction.getMessage()).isEqualTo("cannot insert null value in sub_acct");
    assertThat(errorTransaction.isIgnored()).isTrue();
  }
}
