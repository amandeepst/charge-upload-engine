package com.worldpay.pms.cue.engine.integration;

import static io.vavr.control.Validation.invalid;
import static io.vavr.control.Validation.valid;

import com.worldpay.pms.cue.domain.ChargingService;
import com.worldpay.pms.cue.domain.PendingBillableCharge;
import com.worldpay.pms.cue.domain.PendingBillableChargeError;
import com.worldpay.pms.cue.domain.RecurringResult;
import com.worldpay.pms.cue.domain.common.ErrorCatalog;
import com.worldpay.pms.cue.domain.validator.RawValidationService;
import com.worldpay.pms.cue.domain.validator.ValidationService;
import com.worldpay.pms.cue.engine.common.Factory;
import com.worldpay.pms.cue.engine.staticdata.StaticDataRepository;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.control.Try;
import io.vavr.control.Validation;
import lombok.NonNull;

public class MockedTransactionChargingServiceFactory implements Factory<ChargingService> {

  private final ChargingService chargingService;

  public MockedTransactionChargingServiceFactory(ChargingService chargingService) {
    this.chargingService = chargingService;
  }

  @Override
  public Try<ChargingService> build() {
    return Try.success(new DumbChargingService(chargingService));
  }

  public static class InvalidTransactionChargingService implements ChargingService {

    public InvalidTransactionChargingService() {}

    @Override
    public Validation<PendingBillableChargeError, Charge> calculateNonRecurringCharge(
        @NonNull PendingBillableCharge row) {
      return invalid(PendingBillableChargeError.error(List.of(ErrorCatalog.accountNotFound())));
    }

    @Override
    public Validation<Seq<DomainError>, Charge> charge(@NonNull RecurringResult row) {
      return null;
    }

    @Override
    public Validation<Seq<DomainError>, Charge> calculateRecurringCharge(
        @NonNull PendingBillableCharge row) {
      return null;
    }
  }

  public static class ValidTransactionChargingService implements ChargingService {

    private Charge charge;

    public ValidTransactionChargingService(Charge charge) {
      this.charge = charge;
    }

    public ValidTransactionChargingService() {}

    @Override
    public Validation<PendingBillableChargeError, Charge> calculateNonRecurringCharge(
        @NonNull PendingBillableCharge row) {
      return valid(charge);
    }

    @Override
    public Validation<Seq<DomainError>, Charge> charge(@NonNull RecurringResult row) {
      return valid(charge);
    }

    @Override
    public Validation<Seq<DomainError>, Charge> calculateRecurringCharge(
        @NonNull PendingBillableCharge row) {
      return valid(charge);
    }
  }

  public static class MockTransactionChargingServiceWithValidation implements ChargingService {

    private final ValidationService validationService;
    private final Charge charge;

    public MockTransactionChargingServiceWithValidation(
        Charge charge, StaticDataRepository staticDataRepository) {
      this.charge = charge;
      validationService =
          new RawValidationService(
              staticDataRepository.getCurrencyCodes(),
              staticDataRepository.getPriceItems(),
              staticDataRepository.getSubAccountTypes(),
              staticDataRepository.getBillPeriodCodes());
    }

    @Override
    public Validation<PendingBillableChargeError, Charge> calculateNonRecurringCharge(
        @NonNull PendingBillableCharge row) {
      return validationService.validatePendingRow(row).map(pendingBillableCharge -> charge);
    }

    @Override
    public Validation<Seq<DomainError>, Charge> charge(@NonNull RecurringResult row) {
      return validationService.validateRow(row).map(recurringResult -> charge);
    }

    @Override
    public Validation<Seq<DomainError>, Charge> calculateRecurringCharge(
        @NonNull PendingBillableCharge row) {
      return validationService.validateRecurringRow(row).map(recurringRow -> charge);
    }
  }

  public static class DumbChargingService implements ChargingService {

    private final ChargingService concrete;

    public DumbChargingService(ChargingService concrete) {
      this.concrete = concrete;
    }

    @Override
    public Validation<PendingBillableChargeError, Charge> calculateNonRecurringCharge(
        @NonNull PendingBillableCharge row) {
      return concrete.calculateNonRecurringCharge(row);
    }

    @Override
    public Validation<Seq<DomainError>, Charge> charge(@NonNull RecurringResult row) {
      return concrete.charge(row);
    }

    @Override
    public Validation<Seq<DomainError>, Charge> calculateRecurringCharge(
        @NonNull PendingBillableCharge row) {
      return concrete.calculateRecurringCharge(row);
    }
  }
}
