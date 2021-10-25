package com.worldpay.pms.cue.engine.transformations;

import com.worldpay.pms.cue.domain.ChargingService.Charge;
import com.worldpay.pms.cue.engine.recurring.RecurringResultRow;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class RecurringCharge {

  RecurringResultRow recurringResultRow;
  Charge charge;

  public static RecurringCharge of(RecurringResultRow recurringResultRow, Charge charge) {
    return new RecurringCharge(recurringResultRow, charge);
  }
}
