package com.worldpay.pms.cue.engine.transformations;

import static com.worldpay.pms.cue.engine.encoder.Encoders.PENDING_BILLABLE_CHARGE_ROW_ENCODER;
import static com.worldpay.pms.cue.engine.encoder.Encoders.RECURRING_RESULT_ROW_ENCODER;
import static com.worldpay.pms.cue.engine.utils.Constant.CURRENCY;
import static com.worldpay.pms.cue.engine.utils.Constant.DIVISION;
import static com.worldpay.pms.cue.engine.utils.Constant.LEGALCOUNTERPARTY;
import static com.worldpay.pms.cue.engine.utils.Constant.PARTYID;
import static com.worldpay.pms.cue.engine.utils.Constant.SUBACCOUNTTYPE;

import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeRow;
import com.worldpay.pms.cue.engine.recurring.RecurringResultRow;
import com.worldpay.pms.cue.engine.vwm.VwmBcuAccountRow;
import org.apache.spark.sql.Dataset;

public class VmwBcuAccountTransformation {

  private VmwBcuAccountTransformation() {
    throw new IllegalStateException("Utility class can't be instantiated");
  }

  public static Dataset<PendingBillableChargeRow> leftJoinWithBcuAccountOnpendingCharge(
      Dataset<PendingBillableChargeRow> pendingCharge,
      Dataset<VwmBcuAccountRow> vwmBcuAccountRowDataset) {

    return pendingCharge
        .joinWith(
            vwmBcuAccountRowDataset,
            pendingCharge
                .col(PARTYID)
                .equalTo(vwmBcuAccountRowDataset.col(PARTYID))
                .and(pendingCharge.col("currency").equalTo(vwmBcuAccountRowDataset.col(CURRENCY)))
                .and(
                    pendingCharge
                        .col("subAccountType")
                        .equalTo(vwmBcuAccountRowDataset.col(SUBACCOUNTTYPE)))
                .and(
                    pendingCharge
                        .col(DIVISION)
                        .equalTo(vwmBcuAccountRowDataset.col(LEGALCOUNTERPARTY))),
            "left")
        .map(PendingBillableChargeRow::fromVwmBcuAccountRow, PENDING_BILLABLE_CHARGE_ROW_ENCODER);
  }

  public static Dataset<RecurringResultRow> leftJoinWithBcuAccountOnrecurringCharge(
      Dataset<RecurringResultRow> recurringCharge,
      Dataset<VwmBcuAccountRow> vwmBcuAccountRowDataset) {

    return recurringCharge
        .joinWith(
            vwmBcuAccountRowDataset,
            recurringCharge
                .col(PARTYID)
                .equalTo(vwmBcuAccountRowDataset.col(PARTYID))
                .and(recurringCharge.col("currency").equalTo(vwmBcuAccountRowDataset.col(CURRENCY)))
                .and(
                    recurringCharge
                        .col("subAccount")
                        .equalTo(vwmBcuAccountRowDataset.col(SUBACCOUNTTYPE)))
                .and(
                    recurringCharge
                        .col(DIVISION)
                        .equalTo(vwmBcuAccountRowDataset.col(LEGALCOUNTERPARTY))),
            "left")
        .map(RecurringResultRow::fromVwmBcuAccountRow, RECURRING_RESULT_ROW_ENCODER);
  }
}
