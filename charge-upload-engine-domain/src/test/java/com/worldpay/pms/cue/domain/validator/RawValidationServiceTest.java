package com.worldpay.pms.cue.domain.validator;

import static com.worldpay.pms.cue.domain.samples.Transactions.CHRG_PENDING_CHARGE;
import static com.worldpay.pms.cue.domain.samples.Transactions.INVALID_CHRG_PENDING_CHARGE;
import static com.worldpay.pms.cue.domain.samples.Transactions.INVALID_NON_RECR_PENDING_CHARGE_ALL_INVALID_FIELDS;
import static com.worldpay.pms.cue.domain.samples.Transactions.INVALID_PENDING_CHARGE_NO_CURRENCY_AND_PRICE;
import static com.worldpay.pms.cue.domain.samples.Transactions.INVALID_RECR_PENDING_CHARGE_ALL_INVALID_FIELDS;
import static com.worldpay.pms.cue.domain.samples.Transactions.INVALID_RECURRING_RESULT;
import static com.worldpay.pms.cue.domain.samples.Transactions.NULL_LCP_PENDING_CHARGE;
import static com.worldpay.pms.cue.domain.samples.Transactions.VALID_RECURRING_CHARGE_ROW;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;
import com.worldpay.pms.cue.domain.PendingBillableCharge;
import com.worldpay.pms.cue.domain.PendingBillableChargeError;
import com.worldpay.pms.cue.domain.RecurringResult;
import com.worldpay.pms.cue.domain.validator.RawValidationService.NonRecurringChargeValidator;
import com.worldpay.pms.cue.domain.validator.RawValidationService.RecurringChargeValidator;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class RawValidationServiceTest {

  static RawValidationService validator;

  @BeforeAll
  public static void initService() {
    validator =
        new RawValidationService(
            Sets.newHashSet("GBP", "EUR"),
            Sets.newHashSet("ADHOCCHG", "MIGCHBK"),
            Sets.newHashSet("CHRG", "RECR", "CHBK"),
            Sets.newHashSet("WPDY"));
  }

  @Test
  void testValidationService() {
    NonRecurringChargeValidator nonRecurringChargeValidator =
        new NonRecurringChargeValidator(
            Sets.newHashSet("GBP", "EUR"),
            Sets.newHashSet("ADHOCCHG", "MIGCHBK"),
            Sets.newHashSet("CHRG", "RECR", "CHBK"));

    RecurringChargeValidator recurringChargeValidator =
        new RecurringChargeValidator(
            Sets.newHashSet("GBP", "EUR"),
            Sets.newHashSet("ADHOCCHG", "MIGCHBK"),
            Sets.newHashSet("CHRG", "RECR", "CHBK"),
            Sets.newHashSet("WPDY"));

    assertThat(recurringChargeValidator).isNotNull();
    assertThat(recurringChargeValidator.validateRow(VALID_RECURRING_CHARGE_ROW).get()).isNotNull();
    assertThat(nonRecurringChargeValidator.validateRow(CHRG_PENDING_CHARGE).get()).isNotNull();
  }

  @Test
  void txnPassesWithValidPendingCharge() {
    Validation<PendingBillableChargeError, PendingBillableCharge> validatedRow =
        validator.validatePendingRow(CHRG_PENDING_CHARGE);

    PendingBillableCharge e = validatedRow.getOrNull();
    assertThat(e).isNotNull();
  }

  @Test
  void txnFailsWithInvalidLegalCounterParty() {
    Validation<PendingBillableChargeError, PendingBillableCharge> validatedRow =
        validator.validatePendingRow(NULL_LCP_PENDING_CHARGE);
    assertThat(validatedRow.getError()).isNotNull();
  }

  @Test
  void PendingChargeWithInvalidAccount() {
    Validation<PendingBillableChargeError, PendingBillableCharge> validatedRow =
        validator.validatePendingRow(INVALID_CHRG_PENDING_CHARGE);
    assertThat(validatedRow.getError()).isNotNull();
  }

  @Test
  void RecurringResultWithInvalidAccount() {
    Validation<Seq<DomainError>, RecurringResult> validatedRow =
        validator.validateRow(INVALID_RECURRING_RESULT);
    assertThat(validatedRow.getError()).isNotNull();
  }

  @Test
  void txnFailsWithInvalidCurrencyAndPricePendingCharge() {
    Validation<PendingBillableChargeError, PendingBillableCharge> validatedRow =
        validator.validatePendingRow(INVALID_PENDING_CHARGE_NO_CURRENCY_AND_PRICE);

    DomainError error = DomainError.concatAll(validatedRow.getError().getErrors());
    assertThat(error.getCode())
        .isEqualTo(String.join("\n", Arrays.asList("INVALID_CURRENCY", "NULL")));
    assertThat(error.getMessage())
        .isEqualTo(
            String.join(
                "\n", Arrays.asList("invalid currency 'GB_P'", "Unexpected null field `PRICE`")));
  }

  @Test
  void txnFailsWithInvalidRecurringAndNonRecurringPendingCharge() {
    Validation<PendingBillableChargeError, PendingBillableCharge> nonRecurringValidatedRow =
        validator.validatePendingRow(INVALID_NON_RECR_PENDING_CHARGE_ALL_INVALID_FIELDS);
    Validation<PendingBillableChargeError, PendingBillableCharge> recurringValidatedRow =
        validator.validatePendingRow(INVALID_RECR_PENDING_CHARGE_ALL_INVALID_FIELDS);

    DomainError nonRecurringError =
        DomainError.concatAll(nonRecurringValidatedRow.getError().getErrors());
    DomainError recurringError =
        DomainError.concatAll(recurringValidatedRow.getError().getErrors());

    assertThat(nonRecurringError.getCode())
        .isEqualTo(String.join("\n", invalidNonRecurringChargeCode));
    assertThat(nonRecurringError.getMessage())
        .isEqualTo(String.join("\n", invalidNonRecurringChargeMessage));

    assertThat(recurringError.getCode()).isEqualTo(String.join("\n", invalidRecurringChargeCode));
    assertThat(recurringError.getMessage())
        .isEqualTo(String.join("\n", invalidRecurringChargeMessage));
  }

  @Test
  void txnNonRecrChargeFailsWithAccountNotFound() {
    Validation<PendingBillableChargeError, PendingBillableCharge> nonRecurringValidatedRow =
        validator.validatePendingRow(INVALID_CHRG_PENDING_CHARGE);

    DomainError nonRecurringError =
        DomainError.concatAll(nonRecurringValidatedRow.getError().getErrors());
    assertThat(nonRecurringError.getCode()).isEqualTo(String.join("\n", "ACCOUNT_NOT_FOUND"));
    assertThat(nonRecurringError.getMessage())
        .isEqualTo(String.join("\n", "Could not determine account"));
  }

  List<String> invalidNonRecurringChargeCode =
      Arrays.asList(
          "INVALID_PRODUCT_IDENTIFIER", "INVALID_SUBACCOUNT_TYPE", "INVALID_CURRENCY", "NULL");

  List<String> invalidRecurringChargeCode =
      Arrays.asList("INVALID_PRODUCT_IDENTIFIER", "INVALID_CURRENCY", "NULL");

  List<String> invalidNonRecurringChargeMessage =
      Arrays.asList(
          "invalid productIdentifier 'null'",
          "invalid subAccountType 'TEST'",
          "invalid currency 'null'",
          "Unexpected null field `PRICE`");

  List<String> invalidRecurringChargeMessage =
      Arrays.asList(
          "invalid productIdentifier 'ADHOCCH_G'",
          "invalid currency 'GB_P'",
          "Unexpected null field `PRICE`");
}
