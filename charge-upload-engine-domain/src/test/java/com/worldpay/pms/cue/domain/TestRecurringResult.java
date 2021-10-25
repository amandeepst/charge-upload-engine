package com.worldpay.pms.cue.domain;

import java.math.BigDecimal;
import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class TestRecurringResult implements RecurringResult {

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
  long partitionId;
}
