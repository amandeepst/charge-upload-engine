package com.worldpay.pms.cue.engine.pbc;

import static com.worldpay.pms.cue.engine.encoder.Encoders.PENDING_BILLABLE_CHARGE_ROW_ENCODER;
import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.jdbc.JdbcDataSource.PartitionParameters.byColumn;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.worldpay.pms.cue.engine.ChargingUploadConfig.BillableChargeSourceConfiguration;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcDataSource;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;

public class PendingBillableChargeSource extends JdbcDataSource.For<PendingBillableChargeRow> {
  private static final String QUERY = resourceAsString("sql/get-pending-billable-charges.sql");
  private final BillableChargeSourceConfiguration conf;
  private final int maxAttempts;

  public PendingBillableChargeSource(BillableChargeSourceConfiguration conf, int maxAttempts) {
    super("PendingBillableCharge", conf, PENDING_BILLABLE_CHARGE_ROW_ENCODER);
    this.conf = conf;
    this.maxAttempts = maxAttempts;
  }

  @Override
  protected String sql(BatchId batchId) {
    return QUERY
        .replace(":hints", conf.getHints())
        .replace(":retry-hints", conf.getRetryHints())
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
            .put("low", Timestamp.valueOf(batchId.watermark.low))
            .put("high", Timestamp.valueOf(batchId.watermark.high))
            .put("max_attempt", maxAttempts);
    return args.build();
  }
}
