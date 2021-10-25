package com.worldpay.pms.mdu.engine;

import static com.worldpay.pms.spark.core.SparkApp.run;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.worldpay.pms.cli.CommandLineApp;
import com.worldpay.pms.cli.PmsParameterDelegate;
import com.worldpay.pms.config.ApplicationConfiguration;
import com.worldpay.pms.mdu.engine.batch.MerchantUploadBatchHistoryRepository;
import com.worldpay.pms.mdu.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.PMSException;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import java.sql.Timestamp;
import java.util.function.Function;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MerchantUploadEngineTest implements WithDatabase {
  private static String oldValue;
  private static Timestamp highWaterMark = new Timestamp(System.currentTimeMillis());
  private SqlDb db;

  @BeforeAll
  static void setUp() {
    oldValue = System.getProperty("config.resource");
    System.setProperty("config.resource", "application-test.conf");
  }

  @AfterAll
  static void cleanUpAndRestore() {
    if (oldValue != null) {
      System.setProperty("config.resource", oldValue);
    } else {
      System.clearProperty("config.resource");
    }
  }

  @Override
  public void bindMerchantUploadJdbcConfiguration(JdbcConfiguration conf) {
    db = SqlDb.simple(conf);
  }

  @Test
  void testCanOverrideRunIdFromCommandLine() {
    MerchantUploadEngine merchantUploadEngine =
        new MerchantUploadEngine(ConfigFactory.load()) {
          @Override
          protected MerchantUploadBatchHistoryRepository buildBatchHistoryRepository(
              ApplicationConfiguration applicationConfiguration, MerchantUploadConfig settings) {
            return new MerchantUploadBatchHistoryRepository(getConf().getDb()) {
              @Override
              public long generateRunId(Batch.BatchId batchId) {
                return 100L;
              }
            };
          }

          @Override
          public Batch run(Batch batch) {
            assertThat(generateRunId.apply(batch.id)).isEqualTo(12L);
            return batch;
          }
        };

    run(cfg -> merchantUploadEngine, "submit", "--force", "--run-id", "12");
  }

  @Test
  void testWhenNoRunIdOverrideThenValueIsAssignedAndTakenFromTable() {
    MerchantUploadEngine merchantUploadEngine =
        new MerchantUploadEngine(ConfigFactory.load()) {
          @Override
          protected MerchantUploadBatchHistoryRepository buildBatchHistory() {
            return new MerchantUploadBatchHistoryRepository(getConf().getDb()) {
              @Override
              public long generateRunId(Batch.BatchId batchId) {
                return 100L;
              }
            };
          }

          @Override
          public Batch run(Batch batch) {
            assertThat(generateRunId.apply(batch.id)).isEqualTo(100L);
            return batch;
          }
        };

    run(cfg -> merchantUploadEngine, "submit", "--force");
  }

  @Test
  void testFailWhenParamDelegate() {
    MerchantUploadEngine merchantUploadEngine =
        new MerchantUploadEngine(ConfigFactory.load()) {
          @Override
          public PmsParameterDelegate getParameterDelegate() {
            return new PmsParameterDelegate() {};
          }
        };
    Function<Config, CommandLineApp> app = cfg -> merchantUploadEngine;

    assertThatThrownBy(() -> run(app, "submit", "--force")).isInstanceOf(PMSException.class);
  }

  @AfterEach
  void cleanUpBatchHistory() {
    String sql = "delete from batch_history where watermark_high >= :p1";
    db.exec(
        "BatchHistory cleanup",
        connection -> connection.createQueryWithParams(sql, highWaterMark).executeUpdate());
  }
}
