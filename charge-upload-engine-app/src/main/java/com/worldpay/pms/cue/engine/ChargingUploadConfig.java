package com.worldpay.pms.cue.engine;

import static com.worldpay.pms.utils.PrettyPrinter.prettyToString;

import com.typesafe.config.Optional;
import com.worldpay.pms.config.PmsConfiguration;
import com.worldpay.pms.spark.core.batch.BatchHistoryConfig;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcSourceConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Delegate;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ChargingUploadConfig implements PmsConfiguration {
  private int maxAttempts;
  private BatchHistoryConfig history;
  private DataWriters writers;
  private DataSources sources;
  private double failedRowsThresholdPercent;
  @Optional private long failedRowsThresholdCount = Long.MAX_VALUE;
  @Optional private boolean publishOnThresholdFailure = false;
  @Optional private PublisherConfiguration publishers = new PublisherConfiguration();

  @Override
  public String toString() {
    return prettyToString(this);
  }

  @Data
  public static class DataWriters {
    private JdbcWriterConfiguration failedTransactions;
    private JdbcWriterConfiguration billableCharges;
    private JdbcWriterConfiguration transitionalRecurringCharges;
    private JdbcWriterConfiguration recurringErrorCharges;
    private JdbcWriterConfiguration transitionalRecurringChargesAudit;
    private JdbcWriterConfiguration recurringIdentifiers;
  }

  @Data
  public static class DataSources {
    private BillableChargeSourceConfiguration chargeEvents;
    private RecurringResultSourceConfiguration recurringSource;
    private VwmBcuAccountSourceConfiguration vwmBcuAccountSource;
    private ChargingRepositoryConfiguration staticData;
    private RecurringIdentifierSourceConfiguration recurringIdentifierSource;
    private ErrorRecurringChargeSourceConfiguration errorRecurringChargeSource;
  }

  @Data
  public static class PublisherConfiguration {
    // insert hints:select hints
    private static final String PARALLEL_32 = "ENABLE_PARALLEL_DML PARALLEL(32):PARALLEL(32)";
    @Optional private String billableChargeHints = PARALLEL_32;
    @Optional private String billableChargeLineHints = PARALLEL_32;
    @Optional private String billableChargeCharHints = PARALLEL_32;
    @Optional private String billableChargeLineCharHints = PARALLEL_32;
  }

  @Data
  public static class ErrorRecurringChargeSourceConfiguration extends JdbcSourceConfiguration {
    @Optional String retryHints = "";

    @Delegate @Optional private JdbcSourceConfiguration conf = new JdbcSourceConfiguration();
  }

  @Data
  public static class BillableChargeSourceConfiguration extends JdbcSourceConfiguration {
    @Optional String retryHints = "";

    @Delegate @Optional private JdbcSourceConfiguration conf = new JdbcSourceConfiguration();
  }

  @Data
  public static class RecurringResultSourceConfiguration extends JdbcSourceConfiguration {
    @Optional String retryHints = "";

    @Delegate @Optional private JdbcSourceConfiguration conf = new JdbcSourceConfiguration();
  }

  @Data
  public static class RecurringIdentifierSourceConfiguration extends JdbcSourceConfiguration {
    @Optional String retryHints = "";

    @Delegate @Optional private JdbcSourceConfiguration conf = new JdbcSourceConfiguration();
  }

  @Data
  public static class VwmBcuAccountSourceConfiguration extends JdbcSourceConfiguration {
    @Optional String retryHints = "";

    @Delegate @Optional private JdbcSourceConfiguration conf = new JdbcSourceConfiguration();
  }

  @Data
  public static class ChargingRepositoryConfiguration implements Serializable {

    public static final String NO_HINTS = "";
    public static final String PARALLEL = "PARALLEL(2)";
    @Delegate @Optional private JdbcConfiguration conf = new JdbcConfiguration();

    @Optional String accountDetailsHints = PARALLEL;

    public static ChargingRepositoryConfiguration of(JdbcConfiguration conf) {
      ChargingRepositoryConfiguration config = new ChargingRepositoryConfiguration();
      config.setConf(conf);
      return config;
    }
  }
}
