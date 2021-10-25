package com.worldpay.pms.cue.domain;

import java.math.BigDecimal;
import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class TestRecurringRow implements RecurringRow {

  String recurringChargeIdentifier;
  String txnHeaderId;
  String productIdentifier;
  String legalCounterparty;
  String division;
  String partyIdentifier;
  String subAccount;
  String frequencyIdentifier;
  String currency;
  BigDecimal price;
  long quantity;
  Timestamp validFrom;
  Timestamp validTo;
  String status;
  String sourceId;
  String recurringIdentifierForUpdation;
}
