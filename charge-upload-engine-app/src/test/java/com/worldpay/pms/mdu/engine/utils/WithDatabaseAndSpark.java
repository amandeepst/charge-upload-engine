package com.worldpay.pms.mdu.engine.utils;

import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.Batch.Watermark;
import com.worldpay.pms.testing.stereotypes.WithSparkHeavyUsage;
import java.time.LocalDate;
import java.time.LocalDateTime;

@WithSparkHeavyUsage
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
