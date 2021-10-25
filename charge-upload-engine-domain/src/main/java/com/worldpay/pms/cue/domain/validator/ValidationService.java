package com.worldpay.pms.cue.domain.validator;

import com.worldpay.pms.cue.domain.PendingBillableCharge;
import com.worldpay.pms.cue.domain.PendingBillableChargeError;
import com.worldpay.pms.cue.domain.RecurringResult;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;
import lombok.NonNull;

public interface ValidationService {

  Validation<PendingBillableChargeError, PendingBillableCharge> validatePendingRow(
      @NonNull PendingBillableCharge row);

  Validation<Seq<DomainError>, PendingBillableCharge> validateRecurringRow(
      @NonNull PendingBillableCharge row);

  Validation<Seq<DomainError>, RecurringResult> validateRow(@NonNull RecurringResult row);
}
