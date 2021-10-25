package com.worldpay.pms.cue.domain.validator;

import static com.google.common.base.Strings.nullToEmpty;
import static io.vavr.control.Validation.valid;

import com.worldpay.pms.cue.domain.PendingBillableCharge;
import com.worldpay.pms.cue.domain.PendingBillableChargeError;
import com.worldpay.pms.cue.domain.RecurringResult;
import com.worldpay.pms.cue.domain.common.ErrorCatalog;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;
import java.util.Set;
import lombok.NonNull;
import lombok.Value;

public class RawValidationService implements ValidationService {

  @NonNull private final RecurringChargeValidator recurringChargeValidator;
  @NonNull private final NonRecurringChargeValidator nonRecurringChargeValidator;
  @NonNull private final RecurringChargeAccountValidator recurringChargeAccountValidator;

  public RawValidationService(
      @NonNull Set<String> currencyCodes,
      @NonNull Set<String> priceItems,
      @NonNull Set<String> subAccountTypes,
      @NonNull Set<String> billPeriodCodes) {
    recurringChargeValidator =
        new RecurringChargeValidator(currencyCodes, priceItems, subAccountTypes, billPeriodCodes);
    nonRecurringChargeValidator =
        new NonRecurringChargeValidator(currencyCodes, priceItems, subAccountTypes);
    recurringChargeAccountValidator = new RecurringChargeAccountValidator();
  }

  @Override
  public Validation<PendingBillableChargeError, PendingBillableCharge> validatePendingRow(
      PendingBillableCharge row) {
    return nonRecurringChargeValidator.validateRow(row);
  }

  @Override
  public Validation<Seq<DomainError>, PendingBillableCharge> validateRecurringRow(
      @NonNull PendingBillableCharge row) {
    return recurringChargeValidator.validateRow(row);
  }

  @Override
  public Validation<Seq<DomainError>, RecurringResult> validateRow(@NonNull RecurringResult row) {
    return recurringChargeAccountValidator.validateRow(row);
  }

  public static class NonRecurringChargeValidator {

    @NonNull private final Set<String> currencyCodes;
    @NonNull private final Set<String> priceItems;
    @NonNull private final Set<String> subAccountTypes;

    public NonRecurringChargeValidator(
        @NonNull Set<String> currencyCodes,
        @NonNull Set<String> priceItems,
        @NonNull Set<String> subAccountTypes) {
      this.currencyCodes = currencyCodes;
      this.priceItems = priceItems;
      this.subAccountTypes = subAccountTypes;
    }

    Validation<PendingBillableChargeError, PendingBillableCharge> validateRow(
        PendingBillableCharge row) {

      return Validation.combine(
              validatePriceItem(row),
              validateSubAccountType(row),
              validateCurrencyCode(row),
              validatePrice(row),
              validateLegalCounterParty(row))
          .ap((a, b, c, d, e) -> row)
          .mapError(PendingBillableChargeError::ignored)
          .flatMap(
              x ->
                  validateAccount(x)
                      .mapError(
                          domainError -> PendingBillableChargeError.error(List.of(domainError))));
    }

    private Validation<DomainError, PendingBillableCharge> validatePriceItem(
        PendingBillableCharge row) {
      return priceItems.contains(nullToEmpty(row.getProductIdentifier()).trim())
          ? valid(row)
          : ErrorCatalog.invalidProductIdentifier(row.getProductIdentifier()).asValidation();
    }

    private Validation<DomainError, PendingBillableCharge> validateSubAccountType(
        PendingBillableCharge row) {
      return subAccountTypes.contains(nullToEmpty(row.getSubAccountType()).trim())
          ? valid(row)
          : ErrorCatalog.invalidSubAccountType(row.getSubAccountType()).asValidation();
    }

    private Validation<DomainError, PendingBillableCharge> validateCurrencyCode(
        PendingBillableCharge row) {
      return currencyCodes.contains(nullToEmpty(row.getCurrency()).trim())
          ? valid(row)
          : ErrorCatalog.invalidCurrency(row.getCurrency()).asValidation();
    }

    private Validation<DomainError, PendingBillableCharge> validateLegalCounterParty(
        PendingBillableCharge row) {
      return row.getLcp() != null
          ? valid(row)
          : ErrorCatalog.unexpectedNull("LegalCounterParty").asValidation();
    }

    private Validation<DomainError, PendingBillableCharge> validateAccount(
        PendingBillableCharge row) {
      return nullToEmpty(row.getAccountId()).trim().isEmpty()
              || nullToEmpty(row.getSubAccountId()).trim().isEmpty()
          ? ErrorCatalog.accountNotFound().asValidation()
          : valid(row);
    }

    private Validation<DomainError, PendingBillableCharge> validatePrice(
        PendingBillableCharge row) {
      return row.getPrice() != null
          ? valid(row)
          : ErrorCatalog.unexpectedNull("price").asValidation();
    }
  }

  @Value
  public static class RecurringChargeAccountValidator {

    Validation<Seq<DomainError>, RecurringResult> validateRow(RecurringResult row) {
      return Validation.combine(validateAccount(row), validateSourceId(row)).ap((a, b) -> row);
    }

    private Validation<DomainError, RecurringResult> validateAccount(RecurringResult row) {
      return nullToEmpty(row.getAccountId()).trim().isEmpty()
              || nullToEmpty(row.getSubAccountId()).trim().isEmpty()
          ? ErrorCatalog.accountNotFound().asValidation()
          : valid(row);
    }

    private Validation<DomainError, RecurringResult> validateSourceId(RecurringResult row) {
      return row.getRecurringSourceId() != null
          ? valid(row)
          : ErrorCatalog.unexpectedNull("sourceId").asValidation();
    }
  }

  @Value
  public static class RecurringChargeValidator {
    @NonNull Set<String> currencyCodes;
    @NonNull Set<String> priceItems;
    @NonNull Set<String> subAccountTypes;
    @NonNull Set<String> billPeriodCodes;

    public RecurringChargeValidator(
        @NonNull Set<String> currencyCodes,
        @NonNull Set<String> priceItems,
        @NonNull Set<String> subAccountTypes,
        @NonNull Set<String> billPeriodCodes) {
      this.currencyCodes = currencyCodes;
      this.priceItems = priceItems;
      this.subAccountTypes = subAccountTypes;
      this.billPeriodCodes = billPeriodCodes;
    }

    Validation<Seq<DomainError>, PendingBillableCharge> validateRow(PendingBillableCharge row) {
      return Validation.combine(
              validatePriceItem(row),
              validateSubAccountType(row),
              validateCurrencyCode(row),
              validateFrequencyIdentifier(row),
              validateRecurringRate(row),
              validateSourceId(row),
              validateLegalCounterParty(row))
          .ap((a, b, c, d, e, f, g) -> row);
    }

    private Validation<DomainError, PendingBillableCharge> validatePriceItem(
        PendingBillableCharge row) {
      return priceItems.contains(nullToEmpty(row.getProductIdentifier()).trim())
          ? valid(row)
          : ErrorCatalog.invalidProductIdentifier(row.getProductIdentifier()).asValidation();
    }

    private Validation<DomainError, PendingBillableCharge> validateSubAccountType(
        PendingBillableCharge row) {
      return subAccountTypes.contains(nullToEmpty(row.getSubAccountType()).trim())
          ? valid(row)
          : ErrorCatalog.invalidSubAccountType(row.getSubAccountType()).asValidation();
    }

    private Validation<DomainError, PendingBillableCharge> validateCurrencyCode(
        PendingBillableCharge row) {
      return currencyCodes.contains(nullToEmpty(row.getCurrency()).trim())
          ? valid(row)
          : ErrorCatalog.invalidCurrency(row.getCurrency()).asValidation();
    }

    private Validation<DomainError, PendingBillableCharge> validateFrequencyIdentifier(
        PendingBillableCharge row) {
      return billPeriodCodes.contains(nullToEmpty(row.getFrequencyIdentifier()).trim())
          ? valid(row)
          : ErrorCatalog.invalidFrequencyIdentifier(row.getFrequencyIdentifier()).asValidation();
    }

    private Validation<DomainError, PendingBillableCharge> validateRecurringRate(
        PendingBillableCharge row) {
      return row.getRecurringRate() != null
          ? valid(row)
          : ErrorCatalog.unexpectedNull("price").asValidation();
    }

    private Validation<DomainError, PendingBillableCharge> validateSourceId(
        PendingBillableCharge row) {
      return row.getRecurringIdentifier() != null && !row.getRecurringIdentifier().trim().isEmpty()
          ? valid(row)
          : ErrorCatalog.unexpectedNull("sourceId").asValidation();
    }

    private Validation<DomainError, PendingBillableCharge> validateLegalCounterParty(
        PendingBillableCharge row) {
      return row.getLcp() != null
          ? valid(row)
          : ErrorCatalog.unexpectedNull("LegalCounterParty").asValidation();
    }
  }
}
