package com.worldpay.pms.cue.engine.transformations;

import com.worldpay.pms.cue.domain.ChargingService.Charge;
import com.worldpay.pms.cue.engine.mbi.MiscBillableItem;
import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeRow;
import com.worldpay.pms.cue.engine.recurring.RecurringResultRow;
import java.sql.Date;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ChargedTransaction {

  private MiscBillableItem miscBillableItem;

  public static ChargedTransaction of(
      PendingBillableChargeRow nonRecurringRow, Charge charge, Date logicalDate) {
    return new ChargedTransaction(MiscBillableItem.build(nonRecurringRow, charge, logicalDate));
  }

  public static ChargedTransaction of(RecurringResultRow recurringRow, Charge charge) {
    return new ChargedTransaction(MiscBillableItem.build(recurringRow, charge));
  }
}
