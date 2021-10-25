package com.worldpay.pms.cue.domain;

import static com.worldpay.pms.cue.domain.common.LineCalculationType.PI_MBA;
import static com.worldpay.pms.cue.domain.common.LineCalculationType.PI_RECUR;

import com.worldpay.pms.cue.domain.validator.ValidationService;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;
import lombok.NonNull;

public class DefaultChargingService implements ChargingService {
  private final ValidationService validationService;

  public DefaultChargingService(ValidationService validationService) {
    this.validationService = validationService;
  }

  @Override
  public Validation<PendingBillableChargeError, Charge> calculateNonRecurringCharge(
      @NonNull PendingBillableCharge pendingBillableCharge) {
    return validationService.validatePendingRow(pendingBillableCharge).map(this::calculateCharge);
  }

  @Override
  public Validation<Seq<DomainError>, Charge> charge(@NonNull RecurringResult row) {
    return validationService.validateRow(row).map(this::calculateCharge);
  }

  @Override
  public Validation<Seq<DomainError>, Charge> calculateRecurringCharge(
      @NonNull PendingBillableCharge pendingBillableCharge) {
    return validationService.validateRecurringRow(pendingBillableCharge).map(this::calculateCharge);
  }

  private Charge calculateCharge(RecurringResult row) {
    return new Charge(PI_RECUR.name(), row.calculateCharge());
  }

  private Charge calculateCharge(PendingBillableCharge pendingBillableCharge) {
    if (pendingBillableCharge.isRecurring()) {
      return new Charge(PI_RECUR.name(), pendingBillableCharge.getRecurringRate());
    }
    return new Charge(PI_MBA.name(), pendingBillableCharge.calculateCharge());
  }
}
