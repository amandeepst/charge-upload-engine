package com.worldpay.pms.cue.engine.transformations.model;

import static com.worldpay.pms.cue.domain.common.LineCalculationType.PI_RECUR;
import static com.worldpay.pms.cue.domain.common.Status.ACTIVE;
import static com.worldpay.pms.cue.domain.common.Status.INACTIVE;
import static com.worldpay.pms.cue.engine.recurring.RecurringChargeRow.of;
import static com.worldpay.pms.cue.engine.recurring.RecurringChargeRow.reduce;
import static com.worldpay.pms.cue.engine.samples.Transactions.*;
import static com.worldpay.pms.cue.engine.samples.Transactions.RECR_TRANSITIONAL_ROW_WITH_SOURCE_ID_850747;
import static com.worldpay.pms.cue.engine.samples.Transactions.RECR_TRANSITIONAL_ROW_WITH_SOURCE_ID_850851;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.worldpay.pms.cue.domain.ChargingService.Charge;
import com.worldpay.pms.spark.core.PMSException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import org.junit.jupiter.api.Test;

class RecurringChargeRowTest {
  private static final Charge CHARGE = new Charge(PI_RECUR.name(), BigDecimal.TEN);

  @Test
  void testReduceWithDifferentSourceId() {
    assertThatThrownBy(
            () ->
                reduce(
                    RECR_TRANSITIONAL_ROW_WITH_SOURCE_ID_850747,
                    RECR_TRANSITIONAL_ROW_WITH_SOURCE_ID_850851))
        .isInstanceOf(PMSException.class)
        .hasMessage(
            "Can't reduce RecurringCharge with different keys {} and {}", "850747", "850851");
  }

  @Test
  void testReduceWithSameSourceId() {
    // picks latest txn_header_id if same sourceId
    assertThat(
            reduce(
                    RECR_TRANSITIONAL_ROW_WITH_SOURCE_ID_850851.withTxnHeaderId("819203"),
                    RECR_TRANSITIONAL_ROW_WITH_SOURCE_ID_850851.withTxnHeaderId("829203"))
                .getTxnHeaderId())
        .isEqualTo("829203");
  }

  @Test
  void testRecurringChargeStatus() {
    assertThat(
            of(RECR_PENDING_CHARGE.withValidTo(Timestamp.valueOf("2020-01-01 00:00:00")))
                .getStatus())
        .describedAs("with same valid_from and valid_to")
        .isEqualTo(ACTIVE.name());
    assertThat(of(RECR_PENDING_CHARGE).getStatus())
        .describedAs("with valid date range")
        .isEqualTo(ACTIVE.name());

    assertThat(
            of(RECR_PENDING_CHARGE.withValidTo(Timestamp.valueOf("2000-01-01 00:00:00.123")))
                .getStatus())
        .describedAs("valid_to marked with default timestamp")
        .isEqualTo(ACTIVE.name());
    assertThat(
            of(RECR_PENDING_CHARGE.withValidTo(Timestamp.valueOf("2100-01-01 00:00:00.523")))
                .getStatus())
        .describedAs("valid_to marked with next century time")
        .isEqualTo(ACTIVE.name());
    assertThat(
            of(RECR_PENDING_CHARGE.withValidTo(Timestamp.valueOf("1900-01-01 00:00:00.1254")))
                .getStatus())
        .describedAs("valid_to marked with previous century time")
        .isEqualTo(INACTIVE.name());
    assertThat(
            of(RECR_PENDING_CHARGE.withValidTo(Timestamp.valueOf("2000-01-01 00:00:01")))
                .getStatus())
        .describedAs("valid_to not marked as default timestamp then normal check")
        .isEqualTo(INACTIVE.name());

    assertThat(of(RECR_PENDING_CHARGE_WITH_VALID_DATE_AND_INVALID_STATUS).getStatus())
        .describedAs("valid date range but can_flg marked as Y")
        .isEqualTo(INACTIVE.name());
    assertThat(of(RECR_PENDING_CHARGE_WITH_VALID_DATE_AND_NULL_STATUS).getStatus())
        .describedAs("valid date range but can_flg marked as null")
        .isEqualTo(ACTIVE.name());
  }
}
