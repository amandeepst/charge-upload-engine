package com.worldpay.pms.cue.domain;

import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;
import java.math.BigDecimal;
import lombok.NonNull;
import lombok.Value;

public interface ChargingService {
  Validation<PendingBillableChargeError, Charge> calculateNonRecurringCharge(
      @NonNull PendingBillableCharge row);

  Validation<Seq<DomainError>, Charge> charge(@NonNull RecurringResult row);

  Validation<Seq<DomainError>, Charge> calculateRecurringCharge(@NonNull PendingBillableCharge row);

  @Value
  class Charge {
    @NonNull String lineCalcType;
    @NonNull BigDecimal lineAmount;
  }
}
