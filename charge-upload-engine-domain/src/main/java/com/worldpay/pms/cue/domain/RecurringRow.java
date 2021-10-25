package com.worldpay.pms.cue.domain;

import java.math.BigDecimal;
import java.sql.Timestamp;

public interface RecurringRow {

  String getRecurringChargeIdentifier();

  String getTxnHeaderId();

  String getProductIdentifier();

  String getLegalCounterparty();

  String getDivision();

  String getPartyIdentifier();

  String getSubAccount();

  String getFrequencyIdentifier();

  String getCurrency();

  BigDecimal getPrice();

  long getQuantity();

  Timestamp getValidFrom();

  Timestamp getValidTo();

  String getStatus();

  String getSourceId();

  String getRecurringIdentifierForUpdation();

  default BigDecimal calculateCharge() {
    return getPrice().multiply(BigDecimal.valueOf(getQuantity()));
  }
}
