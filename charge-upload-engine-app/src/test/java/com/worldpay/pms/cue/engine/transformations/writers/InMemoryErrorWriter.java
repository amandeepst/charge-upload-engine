package com.worldpay.pms.cue.engine.transformations.writers;

import com.worldpay.pms.spark.core.ErrorEvent;
import com.worldpay.pms.spark.core.ErrorWriter;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.spark.sql.Dataset;

public class InMemoryErrorWriter<T extends ErrorEvent> implements ErrorWriter<T> {

  private List<T> datasetAsList;

  @Override
  public long getIgnoredCount() {
    return datasetAsList.stream().filter(ErrorEvent::isIgnored).count();
  }

  @Override
  public long getFirstFailureCount() {
    return datasetAsList.stream().filter(ErrorEvent::isFirstFailure).count();
  }

  @Override
  public long write(BatchId batchId, LocalDateTime startedAt, Dataset<T> ds) {
    this.datasetAsList = ds.collectAsList();
    return this.datasetAsList.size() - getIgnoredCount();
  }

  public long count() {
    return datasetAsList.size();
  }

  public List<T> collectAsList() {
    return datasetAsList;
  }
}
