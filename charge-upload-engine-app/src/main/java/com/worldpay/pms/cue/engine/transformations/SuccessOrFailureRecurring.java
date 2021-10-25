package com.worldpay.pms.cue.engine.transformations;

import com.worldpay.pms.cue.engine.recurring.RecurringErrorTransaction;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SuccessOrFailureRecurring {

  RecurringCharge successRow;
  RecurringErrorTransaction recurringFailure;

  public static SuccessOrFailureRecurring ofError(RecurringErrorTransaction recurringFailure) {
    return new SuccessOrFailureRecurring(null, recurringFailure);
  }

  public static SuccessOrFailureRecurring ofSuccess(RecurringCharge successRow) {
    return new SuccessOrFailureRecurring(successRow, null);
  }

  public boolean isSuccess() {
    return successRow != null;
  }

  public boolean isFailure() {
    return recurringFailure != null;
  }
}
