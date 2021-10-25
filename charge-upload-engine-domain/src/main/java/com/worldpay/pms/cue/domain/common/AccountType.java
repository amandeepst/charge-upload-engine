package com.worldpay.pms.cue.domain.common;

import io.vavr.collection.Array;

public enum AccountType {
  CHARGING("CHRG"),
  FUNDING("FUND"),
  CHARGEBACK("CHBK"),
  CARD_REWARD("CRWD");

  public final String code;

  AccountType(String code) {
    this.code = code;
  }

  public static AccountType from(String accountTypeCode) {
    for (AccountType accountType : AccountType.values()) {
      if (accountType.code.equalsIgnoreCase(accountTypeCode)) {
        return accountType;
      }
    }
    throw new InvalidConfigurationException(
        "Invalid account type code `%s`, was expecting one of %s",
        accountTypeCode, Array.of(AccountType.values()).mkString(", "));
  }
}
