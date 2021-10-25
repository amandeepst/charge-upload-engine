package com.worldpay.pms.cue.engine.recurring;

import static com.worldpay.pms.cue.engine.encoder.Encoders.RECURRING_RESULT_ROW_ENCODER;
import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.jdbc.JdbcDataSource.PartitionParameters.byColumn;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.worldpay.pms.cue.engine.ChargingUploadConfig.RecurringResultSourceConfiguration;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcDataSource;
import java.io.Serializable;
import java.sql.Date;
import java.util.Map;

public class RecurringResultSource extends JdbcDataSource.For<RecurringResultRow> {

  private static final String QUERY = resourceAsString("sql/get-recurring-charges.sql");
  private final RecurringResultSourceConfiguration conf;
  private final Date logicalDate;
  private static final int BIND_ARGUMENTS_SIZE = 1;

  public RecurringResultSource(RecurringResultSourceConfiguration config, Date logicalDate) {
    super("look-up-recurring-records", config, RECURRING_RESULT_ROW_ENCODER);
    this.conf = config;
    this.logicalDate = logicalDate;
  }

  @Override
  protected String sql(BatchId batchId) {
    return QUERY
        .replace(":hints", conf.getHints())
        .replace(":partitions", String.valueOf(conf.getPartitionHighBound()));
  }

  @Override
  protected Map<String, Serializable> getBindArguments(BatchId batchId) {
    Builder<String, Serializable> args =
        ImmutableMap.<String, Serializable>builderWithExpectedSize(BIND_ARGUMENTS_SIZE);
    args.put("logical_date", logicalDate);
    return args.build();
  }

  @Override
  protected PartitionParameters getPartitionParameters() {
    return byColumn("partitionId", conf.getPartitionCount(), conf.getPartitionHighBound());
  }
}
