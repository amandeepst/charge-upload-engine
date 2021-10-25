package com.worldpay.pms.cue.engine.utils;

import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.Batch.Watermark;
import com.worldpay.pms.testing.stereotypes.WithSpark;
import java.time.LocalDate;
import java.time.LocalDateTime;

@WithSpark
public interface WithDatabaseAndSpark extends WithDatabase {
  //
  // helper methods
  //
  default BatchId createBatchId(LocalDate from, LocalDate to) {
    return createBatchId(from.atStartOfDay(), to.atStartOfDay());
  }

  default BatchId createBatchId(LocalDateTime from, LocalDateTime to) {
    return new BatchId(new Watermark(from, to), 3);
  }
}
