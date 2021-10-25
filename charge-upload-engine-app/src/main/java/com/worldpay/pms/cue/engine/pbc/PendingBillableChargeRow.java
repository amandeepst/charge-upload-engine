package com.worldpay.pms.cue.engine.pbc;

import com.worldpay.pms.cue.domain.PendingBillableCharge;
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
public class PendingBillableChargeRow implements PendingBillableCharge {

  String txnHeaderId;
  String subAccountType;
  String partyId;
  String division;
  String lcp;
  String currency;
  Timestamp validFrom;
  Timestamp validTo;
  String frequencyIdentifier;
  String productIdentifier;
  long quantity;
  String adhocBillIndicator;
  BigDecimal price;
  BigDecimal recurringRate;
  String recurringIdentifier;
  String fastestSettlementIndicator;
  String caseIdentifier;
  String paymentNarrative;
  String individualPaymentIndicator;
  String releaseReserverIndicator;
  String releaseWafIndicator;
  Timestamp ilmDate;
  Timestamp debtDate;
  String sourceType;
  String sourceId;
  String cancellationFlag;
  String subAccountId;
  String accountId;
  String recurringIdentifierForUpdation;
  String eventId;
  Timestamp billAfterDate;

  Date firstFailureAt;
  int retryCount;
  long partitionId;

  public static PendingBillableChargeRow fromVwmBcuAccountRow(
      Tuple2<PendingBillableChargeRow, VwmBcuAccountRow> tuple2) {
    if (Objects.nonNull(tuple2._2)) {
      return tuple2
          ._1
          .withSubAccountId(tuple2._2.getSubaccountId())
          .withAccountId(tuple2._2.getAccountId());
    }
    return tuple2._1;
  }
}
