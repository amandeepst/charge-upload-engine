package com.worldpay.pms.cue.domain;

import java.math.BigDecimal;
import java.sql.Timestamp;

/** Represents an processed Recr charge event as we've received it */
public interface RecurringResult {

  String getRecurringChargeId();

  String getTxnHeaderId();

  String getProductIdentifier();

  String getLegalCounterParty();

  String getDivision();

  String getPartyId();

  String getSubAccount();

  String getAccountId();

  String getSubAccountId();

  String getFrequencyIdentifier();

  String getCurrency();

  BigDecimal getPrice();

  long getQuantity();

  Timestamp getStartDate();

  Timestamp getEndDate();

  String getStatus();

  String getRecurringSourceId();

  Timestamp getCutoffDate();

  long getPartitionId();

  default BigDecimal calculateCharge() {
    return getPrice().multiply(BigDecimal.valueOf(getQuantity()));
  }
}
