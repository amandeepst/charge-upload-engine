package com.worldpay.pms.cue.engine.recurring;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import org.sql2o.Query;

public class RecurringIdentifierWriter extends JdbcWriter<RecurringIdentifierRow> {

  public RecurringIdentifierWriter(JdbcWriterConfiguration conf) {
    super(conf);
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<RecurringIdentifierRow> writer(
      BatchId batchId, Timestamp startedAt, JdbcWriterConfiguration conf) {
    return new RecurringIdentifierWriter.Writer(batchId, startedAt, conf);
  }

  private static class Writer
      extends JdbcBatchPartitionWriterFunction.Simple<RecurringIdentifierRow> {

    public Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf) {
      super(batchCode, batchStartedAt, conf);
    }

    @Override
    protected String name() {
      return "recurring-identifier";
    }

    @Override
    protected String getStatement() {
      return resourceAsString("sql/outputs/insert-recurring-identifier.sql");
    }

    @Override
    protected void bindAndAdd(RecurringIdentifierRow rcr, Query stmt) {
      stmt.addParameter("sourceId", rcr.getSourceId())
          .addParameter("cutoffDate", rcr.getCutoffDate())
          .addParameter("frequencyIdentifier", rcr.getFrequencyIdentifier())
          .addToBatch();
    }
  }
}
