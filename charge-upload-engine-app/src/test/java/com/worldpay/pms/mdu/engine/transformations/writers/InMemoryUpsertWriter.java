package com.worldpay.pms.mdu.engine.transformations.writers;

import com.worldpay.pms.mdu.domain.model.UpdateEvent;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import java.time.LocalDateTime;
import lombok.Getter;
import lombok.experimental.Delegate;
import org.apache.spark.sql.Dataset;

public class InMemoryUpsertWriter<T extends UpdateEvent> implements UpsertWriter<T> {
  @Getter @Delegate private Dataset<T> dataset;

  @Override
  public long getUpsertCount() {
    return this.dataset.filter(T::isUpdated).coalesce(1).count();
  }

  @Override
  public long write(BatchId batchId, LocalDateTime startedAt, Dataset<T> ds) {
    this.dataset = ds.coalesce(1).cache();
    return this.dataset.count();
  }
}
