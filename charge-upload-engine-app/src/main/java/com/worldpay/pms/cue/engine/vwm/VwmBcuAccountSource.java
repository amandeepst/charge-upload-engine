package com.worldpay.pms.cue.engine.vwm;

import static com.worldpay.pms.cue.engine.encoder.Encoders.VWM_BCU_ACCOUNT_ROW_ENCODER;
import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.jdbc.JdbcDataSource.PartitionParameters.byColumn;

import com.worldpay.pms.cue.engine.ChargingUploadConfig.VwmBcuAccountSourceConfiguration;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcDataSource;

public class VwmBcuAccountSource extends JdbcDataSource.For<VwmBcuAccountRow> {

  private static final String QUERY = resourceAsString("sql/get-vmw-bcu-account.sql");
  private final VwmBcuAccountSourceConfiguration conf;

  public VwmBcuAccountSource(VwmBcuAccountSourceConfiguration config) {
    super("look-up-vwm-bcu-account", config, VWM_BCU_ACCOUNT_ROW_ENCODER);
    this.conf = config;
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
}
