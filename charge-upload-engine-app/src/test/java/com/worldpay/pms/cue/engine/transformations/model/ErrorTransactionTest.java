package com.worldpay.pms.cue.engine.transformations.model;

import static com.worldpay.pms.cue.domain.common.ErrorCatalog.INVALID_CURRENCY;
import static com.worldpay.pms.cue.domain.common.ErrorCatalog.INVALID_FREQUENCYIDENTIFIER;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.cue.domain.PendingBillableChargeError;
import com.worldpay.pms.cue.engine.pbc.ErrorTransaction;
import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeRow;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.List;
import java.sql.Date;
import java.sql.SQLException;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class ErrorTransactionTest {

  @Test
  void testErrorTransactionFactory() {
    ErrorTransaction errorTransaction =
        ErrorTransaction.of(
            PendingBillableChargeRow.builder()
                .txnHeaderId("test")
                .partyId("PO4000398254")
                .subAccountType("CHRG")
                .firstFailureAt(Date.valueOf("2020-01-01"))
                .retryCount(2)
                .build(),
            PendingBillableChargeError.ignored(
                List.of(
                    DomainError.of(INVALID_CURRENCY, "invalid currency"),
                    DomainError.of(INVALID_FREQUENCYIDENTIFIER, "invalid frequencyIdentifier"))));
    String reason = errorTransaction.getMessage();
    assertThat(reason)
        .isEqualTo(
            String.join("\n", Arrays.asList("invalid currency", "invalid frequencyIdentifier")));
    assertThat(errorTransaction.getCode())
        .isEqualTo(String.join("\n", Arrays.asList(INVALID_CURRENCY, INVALID_FREQUENCYIDENTIFIER)));
    assertThat(errorTransaction.getRetryCount()).isEqualTo(999);
  }

  @Test
  void testErrorTransactionWhenFailsWithException() {
    ErrorTransaction errorTransaction =
        ErrorTransaction.of(
            PendingBillableChargeRow.builder()
                .txnHeaderId("test")
                .partyId("PO4000398254")
                .firstFailureAt(Date.valueOf("2020-01-01"))
                .retryCount(998)
                .build(),
            new SQLException("cannot insert null value in sub_acct"));
    assertThat(errorTransaction.getMessage()).isEqualTo("cannot insert null value in sub_acct");
    assertThat(errorTransaction.isIgnored()).isTrue();
  }
}
