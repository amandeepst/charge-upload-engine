package com.worldpay.pms.cue.engine.recurring;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.jdbc.JdbcErrorWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Date;
import java.sql.Timestamp;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.sql2o.Connection;
import org.sql2o.Query;

public class RecurringErrorWriter extends JdbcErrorWriter<RecurringErrorTransaction> {
  private static final int MAX_ERROR_LEN = 2048;
  private static final int MAX_STACKTRACE_LEN = 4000;

  public RecurringErrorWriter(JdbcWriterConfiguration conf, SparkSession spark) {
    super(conf, spark);
  }

  @Override
  protected JdbcErrorWriter.ErrorPartitionWriter<RecurringErrorTransaction> createWriter(
      Batch.BatchId batchId,
      Timestamp startedAt,
      JdbcWriterConfiguration conf,
      LongAccumulator firstFailures,
      LongAccumulator ignored) {

    return new RecurringErrorWriter.Writer(batchId, startedAt, conf, firstFailures, ignored);
  }

  private static class Writer
      extends JdbcErrorWriter.ErrorPartitionWriter<RecurringErrorTransaction> {

    Writer(
        Batch.BatchId batchCode,
        Timestamp batchStartedAt,
        JdbcWriterConfiguration conf,
        LongAccumulator firstFailures,
        LongAccumulator ignored) {
      super(batchCode, batchStartedAt, conf, firstFailures, ignored);
    }

    @Override
    protected String name() {
      return "transitional-recurring-error-transaction";
    }

    @Override
    protected void bindAndAdd(RecurringErrorTransaction error, Timestamp startedAt, Query stmt) {
      stmt.addParameter("rec_chg_id", error.getRecurringChargeId())
          .addParameter("txn_header_id", error.getTxnHeaderId())
          .addParameter("product_id", error.getProductIdentifier())
          .addParameter("lcp", error.getLegalCounterParty())
          .addParameter("party_id", error.getPartyId())
          .addParameter("frequency_id", error.getFrequencyIdentifier())
          .addParameter("currency_cd", error.getCurrency())
          .addParameter("price", error.getPrice())
          .addParameter("quantity", error.getQuantity())
          .addParameter("valid_from", error.getStartDate())
          .addParameter("valid_to", error.getEndDate())
          .addParameter("status", error.getStatus())
          .addParameter("source_id", error.getRecurringSourceId())
          .addParameter("division", error.getDivision())
          .addParameter("sub_acct", error.getSubAccount())
          .addParameter("cutoff_dt", error.getCutoffDate())
          .addParameter("retrycount", error.getRetryCount())
          .addParameter("code", StringUtils.left(error.getCode(), MAX_ERROR_LEN))
          .addParameter("reason", StringUtils.left(error.getMessage(), MAX_ERROR_LEN))
          .addParameter("stacktrace", StringUtils.left(error.getStackTrace(), MAX_STACKTRACE_LEN))
          .addParameter(
              "firstfailureat",
              error.getFirstFailureAt() == null
                  ? Date.valueOf(startedAt.toLocalDateTime().toLocalDate())
                  : error.getFirstFailureAt())
          .addToBatch();
    }

    @Override
    protected Query createStatement(Connection connection) {
      return connection.createQuery(
          resourceAsString("sql/outputs/insert-recurring-error-transaction.sql"));
    }
  }
}
