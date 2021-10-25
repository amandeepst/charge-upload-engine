package com.worldpay.pms.cue.domain;

import java.math.BigDecimal;
import java.sql.Timestamp;
import lombok.Builder;
import lombok.Data;
import lombok.With;

@Data
@With
@Builder
public class TestPendingBillableCharge implements PendingBillableCharge {

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
  String eventId;
  String cancellationFlag;
  String subAccountId;
  String accountId;
  String recurringIdentifierForUpdation;
  Timestamp billAfterDate;
}
