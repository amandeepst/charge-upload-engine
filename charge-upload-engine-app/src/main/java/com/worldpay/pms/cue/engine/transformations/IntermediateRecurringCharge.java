package com.worldpay.pms.cue.engine.transformations;

import com.worldpay.pms.cue.domain.ChargingService.Charge;
import com.worldpay.pms.cue.engine.recurring.RecurringChargeRow;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class IntermediateRecurringCharge {

  RecurringChargeRow recurringChargeRow;
  Charge charge;

  public static IntermediateRecurringCharge of(
      RecurringChargeRow recurringChargeRow, Charge charge) {
    return new IntermediateRecurringCharge(recurringChargeRow, charge);
  }
}
