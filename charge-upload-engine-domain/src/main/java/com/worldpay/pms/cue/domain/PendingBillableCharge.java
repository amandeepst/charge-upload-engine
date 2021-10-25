package com.worldpay.pms.cue.domain;

import com.worldpay.pms.cue.domain.common.Strings;
import java.math.BigDecimal;
import java.sql.Timestamp;

/** Represents an upstream charge event as we've received it */
public interface PendingBillableCharge {

  String getTxnHeaderId();

  String getSubAccountType();

  String getPartyId();

  String getDivision();

  String getLcp();

  String getCurrency();

  Timestamp getValidFrom();

  Timestamp getValidTo();

  String getFrequencyIdentifier();

  String getProductIdentifier();

  long getQuantity();

  String getAdhocBillIndicator();

  BigDecimal getPrice();

  BigDecimal getRecurringRate();

  String getRecurringIdentifier();

  String getFastestSettlementIndicator();

  String getCaseIdentifier();

  String getPaymentNarrative();

  String getIndividualPaymentIndicator();

  String getReleaseReserverIndicator();

  String getReleaseWafIndicator();

  Timestamp getIlmDate();

  Timestamp getDebtDate();

  String getSourceType();

  String getSourceId();

  String getEventId();

  String getCancellationFlag();

  String getSubAccountId();

  String getAccountId();

  String getRecurringIdentifierForUpdation();

  Timestamp getBillAfterDate();

  default boolean isRecurring() {
    return Strings.isNotNullOrEmptyOrWhitespace(getRecurringIdentifier());
  }

  default BigDecimal calculateCharge() {
    return getPrice().multiply(BigDecimal.valueOf(getQuantity()));
  }

  default boolean isNonRecurring() {
    return !isRecurring();
  }
}
