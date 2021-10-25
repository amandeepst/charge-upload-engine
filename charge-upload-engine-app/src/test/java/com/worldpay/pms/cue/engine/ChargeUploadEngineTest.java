package com.worldpay.pms.cue.engine;

import static com.worldpay.pms.spark.core.SparkApp.run;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.beust.jcommander.ParameterException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.worldpay.pms.cli.CommandLineApp;
import com.worldpay.pms.cli.PmsParameterDelegate;
import com.worldpay.pms.config.ApplicationConfiguration;
import com.worldpay.pms.cue.engine.ChargingUploadConfig.PublisherConfiguration;
import com.worldpay.pms.cue.engine.batch.ChargingBatchHistoryRepository;
import com.worldpay.pms.cue.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.PMSException;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import java.sql.Timestamp;
import java.util.function.Function;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ChargeUploadEngineTest implements WithDatabase {

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
  public void bindChargingJdbcConfiguration(JdbcConfiguration conf) {
    db = SqlDb.simple(conf);
  }

  @Test
  void testCanOverrideRunIdFromCommandLine() {
    ChargingUploadEngine chargingEngine =
        new ChargingUploadEngine(ConfigFactory.load()) {
          @Override
          protected ChargingBatchHistoryRepository buildBatchHistoryRepository(
              ApplicationConfiguration applicationConfiguration, ChargingUploadConfig settings) {
            return new ChargingBatchHistoryRepository(
                getConf().getDb(), new PublisherConfiguration()) {
              @Override
              public long generateRunId(BatchId batchId) {
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

    run(
        cfg -> chargingEngine,
        "submit",
        "--force",
        "--logical-date",
        "2020-06-16",
        "--run-id",
        "12");
  }

  @Test
  void testWhenNoRunIdOverrideThenValueIsAssignedAndTakenFromTable() {
    ChargingUploadEngine chargingEngine =
        new ChargingUploadEngine(ConfigFactory.load()) {
          @Override
          protected ChargingBatchHistoryRepository buildBatchHistory() {
            return new ChargingBatchHistoryRepository(
                getConf().getDb(), new PublisherConfiguration()) {
              @Override
              public long generateRunId(BatchId batchId) {
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

    run(cfg -> chargingEngine, "submit", "--force", "--logical-date", "2020-06-16");
  }

  @Test
  void testLogicalDateParamIsPresent() {
    ChargingUploadEngine chargingEngine =
        new ChargingUploadEngine(ConfigFactory.load()) {
          @Override
          public Batch run(Batch batch) {
            return batch;
          }
        };

    Function<Config, CommandLineApp> app = cfg -> chargingEngine;

    run(app, "submit", "--force", "--logical-date", "2020-06-16");
    assertThat("2020-06-16")
        .describedAs("The logical date provided as param should be in app.")
        .isEqualTo(chargingEngine.logicalDate.toString());
  }

  @Test
  void testLogicalDateAcceptsCtrlMFormat() {
    ChargingUploadEngine chargingEngine =
        new ChargingUploadEngine(ConfigFactory.load()) {
          @Override
          public Batch run(Batch batch) {
            return batch;
          }
        };

    Function<Config, CommandLineApp> app = cfg -> chargingEngine;

    run(app, "submit", "--force", "--logical-date", "16/06/2020");
    assertThat("2020-06-16")
        .describedAs("The logical date provided as param should be in app.")
        .isEqualTo(chargingEngine.logicalDate.toString());

    run(app, "submit", "--force", "--logical-date", "30/09/2020");
    assertThat("2020-09-30")
        .describedAs("The logical date provided as param should be in app.")
        .isEqualTo(chargingEngine.logicalDate.toString());
  }

  @Test
  void testFailWhenLogicalDateParamIsNotPresent() {
    assertThatThrownBy(() -> ChargingUploadEngine.main("submit", "--force"))
        .isInstanceOf(ParameterException.class);
  }

  @Test
  void testFailWhenParamDelegate() {
    ChargingUploadEngine chargingEngine =
        new ChargingUploadEngine(ConfigFactory.load()) {
          @Override
          public PmsParameterDelegate getParameterDelegate() {
            return new PmsParameterDelegate() {};
          }
        };
    Function<Config, CommandLineApp> app = cfg -> chargingEngine;

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
