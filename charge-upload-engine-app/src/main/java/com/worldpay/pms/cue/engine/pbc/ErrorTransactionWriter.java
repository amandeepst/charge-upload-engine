package com.worldpay.pms.cue.engine.pbc;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcErrorWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Date;
import java.sql.Timestamp;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.sql2o.Connection;
import org.sql2o.Query;

public class ErrorTransactionWriter extends JdbcErrorWriter<ErrorTransaction> {

  private static final int MAX_ERROR_LEN = 2048;
  private static final int MAX_STACKTRACE_LEN = 4000;

  public ErrorTransactionWriter(JdbcWriterConfiguration conf, SparkSession spark) {
    super(conf, spark);
  }

  @Override
  protected ErrorPartitionWriter<ErrorTransaction> createWriter(
      BatchId batchId,
      Timestamp startedAt,
      JdbcWriterConfiguration conf,
      LongAccumulator firstFailures,
      LongAccumulator ignored) {

    return new Writer(batchId, startedAt, conf, firstFailures, ignored);
  }

  private static class Writer extends JdbcErrorWriter.ErrorPartitionWriter<ErrorTransaction> {

    Writer(
        BatchId batchCode,
        Timestamp batchStartedAt,
        JdbcWriterConfiguration conf,
        LongAccumulator firstFailures,
        LongAccumulator ignored) {
      super(batchCode, batchStartedAt, conf, firstFailures, ignored);
    }

    @Override
    protected String name() {
      return "error-transaction";
    }

    @Override
    protected void bindAndAdd(ErrorTransaction error, Timestamp startedAt, Query stmt) {
      stmt.addParameter("txnHeaderId", error.getTxnHeaderId())
          .addParameter("perIdNbr", error.getPerIdNbr())
          .addParameter("subAccountType", error.getSubAccountType())
          .addParameter("retryCount", error.getRetryCount())
          .addParameter("code", StringUtils.left(error.getCode(), MAX_ERROR_LEN))
          .addParameter("reason", StringUtils.left(error.getMessage(), MAX_ERROR_LEN))
          .addParameter("stackTrace", StringUtils.left(error.getStackTrace(), MAX_STACKTRACE_LEN))
          .addParameter(
              "firstFailureAt",
              error.getFirstFailureAt() == null
                  ? Date.valueOf(startedAt.toLocalDateTime().toLocalDate())
                  : error.getFirstFailureAt())
          .addToBatch();
    }

    @Override
    protected Query createStatement(Connection connection) {
      return connection.createQuery(resourceAsString("sql/outputs/insert-error-transaction.sql"));
    }
  }
}
