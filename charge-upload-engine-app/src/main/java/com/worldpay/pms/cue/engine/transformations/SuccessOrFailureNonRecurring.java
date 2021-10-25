package com.worldpay.pms.cue.engine.transformations;

import com.worldpay.pms.cue.engine.pbc.ErrorTransaction;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SuccessOrFailureNonRecurring {

  ErrorTransaction failure;
  ChargedTransaction success;

  public static SuccessOrFailureNonRecurring ofError(ErrorTransaction error) {
    return new SuccessOrFailureNonRecurring(error, null);
  }

  public static SuccessOrFailureNonRecurring ofSuccess(ChargedTransaction success) {
    return new SuccessOrFailureNonRecurring(null, success);
  }

  public boolean isSuccess() {
    return success != null;
  }

  public boolean isFailure() {
    return failure != null;
  }
}
