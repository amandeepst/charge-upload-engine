package com.worldpay.pms.cue.engine.recurring;

import static com.worldpay.pms.cue.engine.encoder.Encoders.RECURRING_RESULT_ROW_ENCODER;
import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.jdbc.JdbcDataSource.PartitionParameters.byColumn;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.worldpay.pms.cue.engine.ChargingUploadConfig.ErrorRecurringChargeSourceConfiguration;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcDataSource;
import com.worldpay.pms.spark.core.jdbc.JdbcSourceConfiguration;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;

public class ErrorRecurringChargeSource extends JdbcDataSource.For<RecurringResultRow> {

  private static final String QUERY = resourceAsString("sql/get-view-recurring-error.sql");
  private final JdbcSourceConfiguration conf;
  private final int maxAttempts;

  public ErrorRecurringChargeSource(
      ErrorRecurringChargeSourceConfiguration config, int maxAttempts) {
    super("look-up-recurring-error-records", config, RECURRING_RESULT_ROW_ENCODER);
    this.conf = config;
    this.maxAttempts = maxAttempts;
  }

  @Override
  protected String sql(BatchId batchId) {
    return QUERY
        .replace(":hints", conf.getHints())
        .replace(":partitions", String.valueOf(conf.getPartitionHighBound()));
  }

  @Override
  protected PartitionParameters getPartitionParameters() {
    return byColumn("partitionId", conf.getPartitionCount(), conf.getPartitionHighBound());
  }

  @Override
  protected Map<String, Serializable> getBindArguments(BatchId batchId) {
    Builder<String, Serializable> args =
        ImmutableMap.<String, Serializable>builder()
            .put("max_attempt", maxAttempts)
            .put("low", Timestamp.valueOf(batchId.watermark.low))
            .put("high", Timestamp.valueOf(batchId.watermark.high));
    return args.build();
  }
}
