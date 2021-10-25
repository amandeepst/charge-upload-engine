package com.worldpay.pms.cue.domain.integration;

import static com.worldpay.pms.cue.domain.samples.Transactions.CHRG_PENDING_CHARGE;
import static com.worldpay.pms.cue.domain.samples.Transactions.INVALID_NON_RECR_PENDING_CHARGE_ALL_INVALID_FIELDS;
import static com.worldpay.pms.cue.domain.samples.Transactions.INVALID_RECR_PENDING_CHARGE_ALL_INVALID_FIELDS;
import static com.worldpay.pms.cue.domain.samples.Transactions.INVALID_RECURRING_CHARGE_ROW;
import static com.worldpay.pms.cue.domain.samples.Transactions.INVALID_RECURRING_CHARGE_ROW_NULL_SOURCE_ID;
import static com.worldpay.pms.cue.domain.samples.Transactions.INVALID_RECURRING_RESULT;
import static com.worldpay.pms.cue.domain.samples.Transactions.INVALID_RECURRING_RESULT_WITH_NULL_FIELDS;
import static com.worldpay.pms.cue.domain.samples.Transactions.VALID_RECURRING_CHARGE_ROW;
import static com.worldpay.pms.cue.domain.samples.Transactions.VALID_RECURRING_RESULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Sets;
import com.worldpay.pms.cue.domain.ChargingService;
import com.worldpay.pms.cue.domain.ChargingService.Charge;
import com.worldpay.pms.cue.domain.PendingBillableChargeError;
import com.worldpay.pms.cue.domain.TestPendingBillableCharge;
import com.worldpay.pms.cue.domain.TestRecurringResult;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class EndToEndTransactionTest {

  private static final ChargingService CHARGING_SERVICE =
      Scenario.service(
          scenario ->
              scenario
                  .chargeTypes(Collections.emptySet())
                  .currencyCodes(Sets.newHashSet("GBP", "EUR"))
                  .priceItems(Sets.newHashSet("ADHOCCHG", "MIGCHBK"))
                  .subAccountTypes(Sets.newHashSet("CHRG", "RECR", "CHBK"))
                  .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO")));

  @Test
  void failWhenInputRowIsNull() {
    TestPendingBillableCharge pendingBillableCharge = null;

    assertThatThrownBy(() -> CHARGING_SERVICE.calculateNonRecurringCharge(pendingBillableCharge))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void failWhenInputRecuringResultIsNull() {
    TestRecurringResult recurringResult = null;

    assertThatThrownBy(() -> CHARGING_SERVICE.charge(recurringResult))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void failWhenInputRecuringChargeIsNull() {
    TestPendingBillableCharge recurringRow = null;

    assertThatThrownBy(() -> CHARGING_SERVICE.calculateRecurringCharge(recurringRow))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void failWhenInvalidRecurringCharge() {
    assertInvalid(
        CHARGING_SERVICE.calculateRecurringCharge(INVALID_RECURRING_CHARGE_ROW),
        Arrays.asList(
            "invalid productIdentifier 'null'",
            "invalid subAccountType 'null'",
            "invalid currency 'null'",
            "invalid frequencyIdentifier 'null'",
            "Unexpected null field `PRICE`",
            "Unexpected null field `SOURCEID`",
            "Unexpected null field `LEGALCOUNTERPARTY`"));

    assertInvalid(
        CHARGING_SERVICE.calculateRecurringCharge(INVALID_RECURRING_CHARGE_ROW_NULL_SOURCE_ID),
        Arrays.asList(
            "invalid productIdentifier 'null'",
            "invalid subAccountType 'null'",
            "invalid currency 'null'",
            "invalid frequencyIdentifier 'null'",
            "Unexpected null field `PRICE`",
            "Unexpected null field `SOURCEID`",
            "Unexpected null field `LEGALCOUNTERPARTY`"));
  }

  @Test
  void failWhenInvalidTransaction() {
    assertInvalidPendingCharge(
        CHARGING_SERVICE.calculateNonRecurringCharge(
            INVALID_NON_RECR_PENDING_CHARGE_ALL_INVALID_FIELDS),
        Arrays.asList(
            "invalid productIdentifier 'null'",
            "invalid subAccountType 'TEST'",
            "invalid currency 'null'",
            "Unexpected null field `PRICE`"));
    assertInvalidPendingCharge(
        CHARGING_SERVICE.calculateNonRecurringCharge(
            INVALID_RECR_PENDING_CHARGE_ALL_INVALID_FIELDS),
        Arrays.asList(
            "invalid productIdentifier 'ADHOCCH_G'",
            "invalid currency 'GB_P'",
            "Unexpected null field `PRICE`"));
  }

  @Test
  void chargeDeterminationForPendingChargeRow() {
    Validation<PendingBillableChargeError, Charge> chargeOrError =
        CHARGING_SERVICE.calculateNonRecurringCharge(CHRG_PENDING_CHARGE);
    assertThat(chargeOrError.isInvalid()).isFalse();
    assertValid(chargeOrError.get(), BigDecimal.valueOf(30), "PI_MBA");
  }

  @Test
  void chargeDeterminationForRecurringChargeRow() {
    Validation<Seq<DomainError>, Charge> chargeOrError =
        CHARGING_SERVICE.calculateRecurringCharge(VALID_RECURRING_CHARGE_ROW);
    assertThat(chargeOrError.isInvalid()).isFalse();
    assertValid(chargeOrError.get(), BigDecimal.valueOf(1), "PI_RECUR");
  }

  @Test
  void chargeDeterminationForRecurringResultRow() {
    Validation<Seq<DomainError>, Charge> chargeOrError =
        CHARGING_SERVICE.charge(VALID_RECURRING_RESULT);
    assertThat(chargeOrError.isInvalid()).isFalse();
    assertValid(chargeOrError.get(), BigDecimal.valueOf(2), "PI_RECUR");
  }

  @Test
  void chargeDeterminationForPendingRecurringChargeRow() {
    Validation<Seq<DomainError>, Charge> chargeOrError =
        CHARGING_SERVICE.charge(INVALID_RECURRING_RESULT);
    assertThat(chargeOrError.isInvalid()).isTrue();
  }

  @Test
  void chargeDeterminationForRecurringResultChargeRow() {
    Validation<Seq<DomainError>, Charge> chargeOrError =
        CHARGING_SERVICE.charge(INVALID_RECURRING_RESULT_WITH_NULL_FIELDS);
    System.out.println(chargeOrError.getError());
    assertThat(chargeOrError.isInvalid()).isTrue();
  }

  private void assertValid(Charge charge, BigDecimal amount, String calcType) {
    assertThat(charge.getLineCalcType()).isEqualTo(calcType);
    assertThat(charge.getLineAmount()).isEqualTo(amount);
  }

  private <T> void assertInvalidPendingCharge(
      Validation<PendingBillableChargeError, T> result, List<String> errors) {
    assertThat(result.isInvalid()).isTrue();
    assertThat(DomainError.concatAll(result.getError().getErrors()).getMessage())
        .isEqualTo(String.join("\n", errors));
  }

  private <T> void assertInvalid(Validation<Seq<DomainError>, T> result, List<String> errors) {
    assertThat(result.isInvalid()).isTrue();
    assertThat(DomainError.concatAll(result.getError()).getMessage())
        .isEqualTo(String.join("\n", errors));
  }
}
