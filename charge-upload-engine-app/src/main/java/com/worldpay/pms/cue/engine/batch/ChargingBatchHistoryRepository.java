package com.worldpay.pms.cue.engine.batch;

import static com.worldpay.pms.cue.engine.ChargingUploadConfig.PublisherConfiguration;
import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.SparkUtils.timed;

import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.repositories.JdbcBatchHistoryRepository;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Connection;
import org.sql2o.Query;

@Slf4j
public class ChargingBatchHistoryRepository
    extends JdbcBatchHistoryRepository<ChargingBatchRunResult> {

  private static final String CHARGING_BATCH_HISTORY_TABLE = "batch_history";
  private static final String BATCH_CODE_COL_NAME = "batch_code";
  private static final String BATCH_ATTEMPT_COL_NAME = "batch_attempt";
  private final PublisherConfiguration publisherConfig;

  public ChargingBatchHistoryRepository(
      JdbcConfiguration conf, PublisherConfiguration publisherConfig) {
    super(CHARGING_BATCH_HISTORY_TABLE, ChargingBatchRunResult.class, conf);
    this.publisherConfig = publisherConfig;
  }

  public long generateRunId(BatchId batchId) {
    return db.exec(
        "generate-run-id",
        conn -> {
          conn.createQuery(resourceAsString("sql/batch/insert-batch-seed.sql"))
              .addParameter(BATCH_CODE_COL_NAME, batchId.code)
              .addParameter(BATCH_ATTEMPT_COL_NAME, batchId.attempt)
              .executeUpdate();

          return conn.createQuery(resourceAsString("sql/batch/get-batch-seed.sql"))
              .addParameter(BATCH_CODE_COL_NAME, batchId.code)
              .addParameter(BATCH_ATTEMPT_COL_NAME, batchId.attempt)
              .executeScalar(long.class);
        });
  }

  @Override
  public void onBatchCompleted(Connection conn, Batch batch) {
    publishAll(publisherConfig, conn, batch);
  }

  private static void publishAll(PublisherConfiguration conf, Connection conn, Batch batch) {
    timed(
        "publish-outputs",
        () -> {
          publish("billable-charge", conf.getBillableChargeHints(), conn, batch);
          publish("billable-charge-line", conf.getBillableChargeLineHints(), conn, batch);
          publish("billable-charge-char", conf.getBillableChargeCharHints(), conn, batch);
          publish("billable-charge-line-char", conf.getBillableChargeLineCharHints(), conn, batch);
        });
  }

  private static void publish(String name, String hints, Connection conn, Batch batch) {
    String sql =
        interpolateHints(
            resourceAsString(String.format("sql/publish/publish-%s.sql", name)), hints);

    log.info("Executing `{}`...\n{}", name, sql);
    timed(
        String.format("publish-%s", name),
        () -> {
          try (Query stmt = conn.createQuery(sql)) {
            stmt.addParameter(BATCH_CODE_COL_NAME, batch.id.code)
                .addParameter(BATCH_ATTEMPT_COL_NAME, batch.id.attempt)
                .addParameter("ilm_dt", Timestamp.valueOf(batch.createdAt))
                .executeUpdate();
          }
        });
  }

  static String interpolateHints(String sql, String hints) {
    String[] h = hints == null ? new String[0] : hints.split(":");
    return sql.replace(":insert-hints", h.length > 0 ? h[0] : "")
        .replace(":select-hints", h.length > 1 ? h[1] : "");
  }
}
