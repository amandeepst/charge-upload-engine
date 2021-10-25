package com.worldpay.pms.cue.engine.utils.data.model;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class TestRecurringChargeRow {
  String recChargeId;
  String txnHeaderId;
  String productId;
  String legalCounterParty;
  String division;
  String partyId;
  String subAccount;
  String acctId;
  String subAccountId;
  String frequencyId;
  String currency;
  BigDecimal price;
  int quantity;
  String validFrom;
  String validTo;
  String status;
  String sourceId;
  String creditDttm;
  String lastUpateDttm;
  String ilmArchiveSwitch;
  String ilmDate;
  String batchCode;
  int batchAttempt;
  long partitionId;
}
