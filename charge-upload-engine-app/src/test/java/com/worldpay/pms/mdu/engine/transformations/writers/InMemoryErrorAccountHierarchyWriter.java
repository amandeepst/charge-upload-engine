package com.worldpay.pms.mdu.engine.transformations.writers;

import com.worldpay.pms.mdu.engine.encoder.Encoders;
import com.worldpay.pms.mdu.engine.transformations.ErrorAccountHierarchy;
import com.worldpay.pms.spark.core.ErrorEvent;
import com.worldpay.pms.spark.core.ErrorWriter;
import com.worldpay.pms.spark.core.batch.Batch;
import java.time.LocalDateTime;
import lombok.Getter;
import lombok.experimental.Delegate;
import org.apache.spark.sql.Dataset;

public class InMemoryErrorAccountHierarchyWriter<T extends ErrorEvent> implements ErrorWriter<T> {
  @Getter @Delegate private Dataset<T> dataset;

  @Override
  public long getIgnoredCount() {
    return this.dataset
        .map(row -> (ErrorAccountHierarchy) row, Encoders.ERROR_HIER_TRANSACTION_ENCODER)
        .filter(ErrorAccountHierarchy::isIgnored)
        .coalesce(1)
        .count();
  }

  @Override
  public long getFirstFailureCount() {
    return this.dataset
        .map(row -> (ErrorAccountHierarchy) row, Encoders.ERROR_HIER_TRANSACTION_ENCODER)
        .filter(ErrorAccountHierarchy::isFirstFailure)
        .coalesce(1)
        .count();
  }

  @Override
  public long write(Batch.BatchId batchId, LocalDateTime startedAt, Dataset<T> ds) {
    this.dataset = ds.coalesce(1).cache();
    return this.dataset.count() - getIgnoredCount();
  }
}
