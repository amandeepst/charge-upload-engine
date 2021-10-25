package com.worldpay.pms.cue.domain;

import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.Seq;
import lombok.Value;

@Value
public class PendingBillableChargeError {
  boolean ignored;
  Seq<DomainError> errors;

  public static PendingBillableChargeError ignored(Seq<DomainError> errors) {
    return new PendingBillableChargeError(true, errors);
  }

  public static PendingBillableChargeError error(Seq<DomainError> errors) {
    return new PendingBillableChargeError(false, errors);
  }
}
