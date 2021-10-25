package com.worldpay.pms.cue.engine.recurring;

import com.worldpay.pms.cue.domain.RecurringResult;
import com.worldpay.pms.cue.engine.vwm.VwmBcuAccountRow;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import scala.Tuple2;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@With
public class RecurringResultRow implements RecurringResult {

  String recurringChargeId;
  String txnHeaderId;
  String productIdentifier;
  String legalCounterParty;
  String division;
  String partyId;
  String subAccount;
  String accountId;
  String subAccountId;
  String frequencyIdentifier;
  String currency;
  BigDecimal price;
  long quantity;
  Timestamp startDate;
  Timestamp endDate;
  String status;
  String recurringSourceId;
  Timestamp cutoffDate;
  Date firstFailureAt;
  int retryCount;

  long partitionId;

  public static RecurringResultRow fromVwmBcuAccountRow(
      Tuple2<RecurringResultRow, VwmBcuAccountRow> tuple2) {
    if (Objects.nonNull(tuple2._2)) {
      return tuple2
          ._1
          .withSubAccountId(tuple2._2.getSubaccountId())
          .withAccountId(tuple2._2.getAccountId());
    }
    return tuple2._1;
  }
}
