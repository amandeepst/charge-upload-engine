package com.worldpay.pms.cue.engine.pbc;

import com.worldpay.pms.cue.domain.PendingBillableChargeError;
import com.worldpay.pms.cue.domain.common.AccountType;
import com.worldpay.pms.cue.engine.recurring.RecurringResultRow;
import com.worldpay.pms.pce.common.DomainError;
import com.worldpay.pms.spark.core.ErrorEvent;
import java.sql.Date;
import java.time.LocalDate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ErrorTransaction implements ErrorEvent {

  private static final String UNHANDLED_EXCEPTION = "UNHANDLED_EXCEPTION";
  public static final int IGNORED_RETRY_COUNT = 999;
  private static final String FAILED_WITH_CODE_AND_REASON = "failed with code=`{}` reason=`{}` ";

  String txnHeaderId;
  String perIdNbr;
  String subAccountType;
  Date firstFailureAt;
  int retryCount;
  String code;
  String message;
  String stackTrace;

  public static ErrorTransaction of(
      PendingBillableChargeRow row, PendingBillableChargeError pendingBillableChargeError) {
    DomainError domainError = DomainError.concatAll(pendingBillableChargeError.getErrors());
    log.warn(
        "Transaction txnHeaderId=`{}` subAccountType=`{}` perIdNbr={} firstFailure={} isFirstFailure={} attempts={} "
            + FAILED_WITH_CODE_AND_REASON,
        row.getTxnHeaderId(),
        row.getSubAccountType(),
        row.getPartyId(),
        row.getFirstFailureAt(),
        row.getFirstFailureAt() == null,
        pendingBillableChargeError.isIgnored() ? IGNORED_RETRY_COUNT : row.getRetryCount(),
        domainError.getCode(),
        domainError.getMessage());

    return new ErrorTransaction(
        row.getTxnHeaderId(),
        row.getPartyId(),
        row.getSubAccountType(),
        row.getFirstFailureAt(),
        pendingBillableChargeError.isIgnored() ? IGNORED_RETRY_COUNT : (row.getRetryCount() + 1),
        domainError.getCode(),
        domainError.getMessage(),
        null);
  }

  public static ErrorTransaction of(PendingBillableChargeRow row, Throwable ex) {

    log.warn(
        "Transaction txnHeaderId=`{}` subAccountType=`{}` perIdNbr={} firstFailure={} isFirstFailure={} attempts={} "
            + FAILED_WITH_CODE_AND_REASON,
        row.getTxnHeaderId(),
        row.getSubAccountType(),
        row.getPartyId(),
        row.getFirstFailureAt(),
        row.getFirstFailureAt() == null,
        row.getRetryCount(),
        UNHANDLED_EXCEPTION,
        ex.getMessage());
    return new ErrorTransaction(
        row.getTxnHeaderId(),
        row.getPartyId(),
        row.getSubAccountType(),
        null,
        IGNORED_RETRY_COUNT,
        UNHANDLED_EXCEPTION,
        ex.getMessage(),
        ExceptionUtils.getStackTrace(ex));
  }

  public static ErrorTransaction of(RecurringResultRow row, Throwable ex) {

    log.warn(
        "Recurring Transaction  txnHeaderId=`{}` perIdNbr={} " + FAILED_WITH_CODE_AND_REASON,
        row.getRecurringChargeId(),
        row.getPartyId(),
        UNHANDLED_EXCEPTION,
        ex.getMessage());

    return new ErrorTransaction(
        row.getRecurringChargeId(),
        row.getPartyId(),
        AccountType.CHARGING.code,
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
