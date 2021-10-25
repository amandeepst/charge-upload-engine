package com.worldpay.pms.cue.engine.recurring;

import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeRow;
import com.worldpay.pms.pce.common.DomainError;
import com.worldpay.pms.spark.core.ErrorEvent;
import io.vavr.collection.Seq;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RecurringErrorTransaction implements ErrorEvent {

  private static final String UNHANDLED_EXCEPTION = "UNHANDLED_EXCEPTION";
  public static final int IGNORED_RETRY_COUNT = 999;
  private static final String FAILED_WITH_CODE_AND_REASON = "failed with code=`{}` reason=`{}` ";

  String recurringChargeId;
  String txnHeaderId;
  String productIdentifier;
  String legalCounterParty;
  String division;
  String partyId;
  String subAccount;
  String frequencyIdentifier;
  String currency;
  BigDecimal price;
  long quantity;
  Timestamp startDate;
  Timestamp endDate;
  String status;
  String recurringSourceId;
  Timestamp cutoffDate;

  Date firstFailureAt;
  int retryCount;
  String code;
  String message;
  String stackTrace;

  public static RecurringErrorTransaction of(RecurringResultRow row, Seq<DomainError> errors) {
    DomainError domainError = DomainError.concatAll(errors);
    log.warn(
        "Transaction txnHeaderId=`{}` subAccountType=`{}` perIdNbr={} firstFailure={} isFirstFailure={} attempts={} "
            + FAILED_WITH_CODE_AND_REASON,
        row.getTxnHeaderId(),
        row.getSubAccount(),
        row.getPartyId(),
        null,
        true,
        1,
        domainError.getCode(),
        domainError.getMessage());

    return new RecurringErrorTransaction(
        row.getRecurringChargeId(),
        row.getTxnHeaderId(),
        row.getProductIdentifier(),
        row.getLegalCounterParty(),
        row.getDivision(),
        row.getPartyId(),
        row.getSubAccount(),
        row.getFrequencyIdentifier(),
        row.getCurrency(),
        row.getPrice(),
        row.getQuantity(),
        row.getStartDate(),
        row.getEndDate(),
        row.getStatus(),
        row.getRecurringSourceId(),
        row.getCutoffDate(),
        row.getFirstFailureAt(),
        row.getRetryCount() + 1,
        domainError.getCode(),
        domainError.getMessage(),
        null);
  }

  public static RecurringErrorTransaction of(
      PendingBillableChargeRow row, Seq<DomainError> errors) {
    DomainError domainError = DomainError.concatAll(errors);
    log.warn(
        "Transaction txnHeaderId=`{}` subAccountType=`{}` perIdNbr={} firstFailure={} isFirstFailure={} attempts={} "
            + FAILED_WITH_CODE_AND_REASON,
        row.getTxnHeaderId(),
        row.getSubAccountType(),
        row.getPartyId(),
        null,
        true,
        1,
        domainError.getCode(),
        domainError.getMessage());

    return new RecurringErrorTransaction(
        UUID.randomUUID().toString(),
        row.getTxnHeaderId(),
        row.getProductIdentifier(),
        row.getLcp(),
        row.getDivision(),
        row.getPartyId(),
        row.getSubAccountType(),
        row.getFrequencyIdentifier(),
        row.getCurrency(),
        row.getRecurringRate(),
        row.getQuantity(),
        row.getValidFrom(),
        row.getValidTo(),
        "INACTIVE",
        row.getRecurringIdentifier(),
        null,
        Date.valueOf(LocalDate.now()),
        IGNORED_RETRY_COUNT,
        domainError.getCode(),
        domainError.getMessage(),
        null);
  }

  public static RecurringErrorTransaction of(RecurringResultRow row, Throwable ex) {

    log.warn(
        "Recurring Transaction  txnHeaderId=`{}` perIdNbr={} " + FAILED_WITH_CODE_AND_REASON,
        row.getTxnHeaderId(),
        row.getPartyId(),
        UNHANDLED_EXCEPTION,
        ex.getMessage());

    return new RecurringErrorTransaction(
        row.getRecurringChargeId(),
        row.getTxnHeaderId(),
        row.getProductIdentifier(),
        row.getLegalCounterParty(),
        row.getDivision(),
        row.getPartyId(),
        row.getSubAccount(),
        row.getFrequencyIdentifier(),
        row.getCurrency(),
        row.getPrice(),
        row.getQuantity(),
        row.getStartDate(),
        row.getEndDate(),
        row.getStatus(),
        row.getRecurringSourceId(),
        row.getCutoffDate(),
        null,
        IGNORED_RETRY_COUNT,
        UNHANDLED_EXCEPTION,
        ex.getMessage(),
        ExceptionUtils.getStackTrace(ex));
  }

  public static RecurringErrorTransaction of(PendingBillableChargeRow row, Throwable ex) {

    log.warn(
        "Recurring Transaction  txnHeaderId=`{}` perIdNbr={} " + FAILED_WITH_CODE_AND_REASON,
        row.getTxnHeaderId(),
        row.getPartyId(),
        UNHANDLED_EXCEPTION,
        ex.getMessage());

    return new RecurringErrorTransaction(
        UUID.randomUUID().toString(),
        row.getTxnHeaderId(),
        row.getProductIdentifier(),
        row.getLcp(),
        row.getDivision(),
        row.getPartyId(),
        row.getSubAccountType(),
        row.getFrequencyIdentifier(),
        row.getCurrency(),
        row.getRecurringRate(),
        row.getQuantity(),
        row.getValidFrom(),
        row.getValidTo(),
        "INACTIVE",
        row.getRecurringIdentifier(),
        null,
        null,
        IGNORED_RETRY_COUNT,
        UNHANDLED_EXCEPTION,
        ex.getMessage(),
        ExceptionUtils.getStackTrace(ex));
  }

  @Override
  public boolean isIgnored() {
    return retryCount == IGNORED_RETRY_COUNT;
  }

  @Override
  public boolean isFirstFailure() {
    return getFirstFailureAt() == null
        || getFirstFailureAt().toLocalDate().isEqual(LocalDate.now());
  }
}
