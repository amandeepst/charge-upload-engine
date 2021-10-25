package com.worldpay.pms.cue.domain.common;

import static java.lang.String.format;

import com.worldpay.pms.pce.common.DomainError;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ErrorCatalog {

  public static final String ACCOUNT_NOT_FOUND = "ACCOUNT_NOT_FOUND";
  public static final String INVALID_CURRENCY = "INVALID_CURRENCY";
  public static final String INVALID_SUBACCOUNTTYPE = "INVALID_SUBACCOUNT_TYPE";
  public static final String INVALID_PRICEITEM = "INVALID_PRODUCT_IDENTIFIER";
  public static final String INVALID_FREQUENCYIDENTIFIER = "INVALID_FREQUENCY_IDENTIFIER";
  public static final String NULL = "NULL";

  public static DomainError accountNotFound() {
    return DomainError.of(ACCOUNT_NOT_FOUND, "Could not determine account");
  }

  public static DomainError invalidCurrency(String currency) {
    return DomainError.of(INVALID_CURRENCY, format("invalid currency '%s'", currency));
  }

  public static DomainError invalidSubAccountType(String subAccountType) {
    return DomainError.of(
        INVALID_SUBACCOUNTTYPE, format("invalid subAccountType '%s'", subAccountType));
  }

  public static DomainError invalidProductIdentifier(String productIdentifier) {
    return DomainError.of(
        INVALID_PRICEITEM, format("invalid productIdentifier '%s'", productIdentifier));
  }

  public static DomainError invalidFrequencyIdentifier(String frequencyIdentifier) {
    return DomainError.of(
        INVALID_FREQUENCYIDENTIFIER,
        format("invalid frequencyIdentifier '%s'", frequencyIdentifier));
  }

  //
  // generic
  //
  public static DomainError unexpectedNull(String field) {
    return DomainError.of(NULL, "Unexpected null field `%s`", field.toUpperCase());
  }
}
