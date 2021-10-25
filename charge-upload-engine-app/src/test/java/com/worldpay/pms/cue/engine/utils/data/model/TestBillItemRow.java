package com.worldpay.pms.cue.engine.utils.data.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class TestBillItemRow {
  String miscBillItemId;
  String partyId;
  String division;
  String subAcct;
  String acctId;
  String subAccountId;
  String currency;
  String status;
  String productId;
  int qty;
  String sourceId;
  String frequencyId;
  String cutoffDate;
  String createdDttm;
  String ilmDttm;
  String ilmArchSw;
}
