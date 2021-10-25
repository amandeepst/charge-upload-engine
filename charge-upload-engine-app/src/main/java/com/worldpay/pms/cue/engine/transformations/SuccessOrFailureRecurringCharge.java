package com.worldpay.pms.cue.engine.transformations;

import com.worldpay.pms.cue.engine.recurring.RecurringErrorTransaction;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SuccessOrFailureRecurringCharge {

  IntermediateRecurringCharge successRecurringChargeRow;
  RecurringErrorTransaction recurringFailure;

  public static SuccessOrFailureRecurringCharge ofError(
      RecurringErrorTransaction recurringFailure) {
    return new SuccessOrFailureRecurringCharge(null, recurringFailure);
  }

  public static SuccessOrFailureRecurringCharge ofSuccess(
      IntermediateRecurringCharge successRecurringChargeRow) {
    return new SuccessOrFailureRecurringCharge(successRecurringChargeRow, null);
  }

  public boolean isSuccess() {
    return successRecurringChargeRow != null;
  }

  public boolean isFailure() {
    return recurringFailure != null;
  }
}
