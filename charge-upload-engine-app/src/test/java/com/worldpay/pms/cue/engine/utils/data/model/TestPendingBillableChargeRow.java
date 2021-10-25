package com.worldpay.pms.cue.engine.utils.data.model;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class TestPendingBillableChargeRow {
  String txnHeaderId;
  String priceItem;
  String status;
  String perIdNbr;
  String division;
  String saTypeCode;
  BigDecimal serviceQty;
  String startDate;
  String endDate;
  String ilmDate;
  String adhocSw;
  String recurringIdentifier;
}
