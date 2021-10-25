package com.worldpay.pms.cue.engine.batch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.worldpay.pms.spark.core.batch.BatchRunResult;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;

@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Value
public class ChargingBatchRunResult implements BatchRunResult {

  @NonFinal long recurringChargeCount;
  @NonFinal long recurringChargeFailureCount;
  @NonFinal long nonRecurringChargeFailureCount;
  @NonFinal long miscellaneousBillableItemCount;
  @NonFinal long recurringChargeCountForAudit;
  @NonFinal long recurringIdentifierCount;
  @NonFinal long failedTodayTransactions;
  @NonFinal long ignoredTransactionsCount;

  @JsonIgnore
  @Override
  public long getErrorTransactionsCount() {
    return failedTodayTransactions;
  }

  @JsonIgnore
  @Override
  public long getSuccessTransactionsCount() {
    return miscellaneousBillableItemCount;
  }
}
