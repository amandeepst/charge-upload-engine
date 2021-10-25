package com.worldpay.pms.cue.engine.jdbc;

import static com.worldpay.pms.cue.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.cue.engine.ChargingUploadConfig;
import com.worldpay.pms.cue.engine.ChargingUploadConfig.RecurringIdentifierSourceConfiguration;
import com.worldpay.pms.cue.engine.recurring.RecurringIdentifierRow;
import com.worldpay.pms.cue.engine.recurring.RecurringIdentifierSource;
import com.worldpay.pms.cue.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import java.time.LocalDateTime;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sql2o.Sql2oQuery;

public class RecurringIdentifierSourceTest implements WithDatabaseAndSpark {

  private SqlDb appuserDb;
  private RecurringIdentifierSource source;
  private RecurringIdentifierSourceConfiguration sourceConfiguration;

  @Override
  public void bindChargingConfiguration(ChargingUploadConfig conf) {
    this.sourceConfiguration = conf.getSources().getRecurringIdentifierSource();
    this.sourceConfiguration.setPartitionCount(4);
    this.source = new RecurringIdentifierSource(this.sourceConfiguration);
  }

  @Override
  public void bindChargingJdbcConfiguration(JdbcConfiguration conf) {
    this.appuserDb = SqlDb.simple(conf);
  }

  @BeforeEach
  void setUp() {
    appuserDb.execQuery("delete cm_rec_idfr", "DELETE FROM CM_REC_IDFR", Sql2oQuery::executeUpdate);
    appuserDb.execQuery(
        "delete batch_history", "DELETE FROM BATCH_HISTORY", Sql2oQuery::executeUpdate);
  }

  @AfterEach
  void cleanUp() {
    appuserDb.execQuery("delete cm_rec_idfr", "DELETE FROM CM_REC_IDFR", Sql2oQuery::executeUpdate);
    appuserDb.execQuery(
        "delete batch_history", "DELETE FROM BATCH_HISTORY", Sql2oQuery::executeUpdate);
  }

  @Test
  void pickOnlyLastCompletedBatchRecordsFromRecrIdfr(SparkSession spark) {

    // insert batch history entries
    readFromCsvFileAndWriteToExistingTable(
        appuserDb,
        "input/RecurringIdentifierSourceTest/batch_history_scenario1.csv",
        "batch_history");

    // insert cm_rec_idfr entries
    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/RecurringIdentifierSourceTest/cm_rec_idfr_scenario1.csv", "cm_rec_idfr");

    BatchId batchId =
        createBatchId(
            LocalDateTime.of(2021, 3, 16, 10, 51, 37), LocalDateTime.of(2021, 3, 17, 10, 50, 37));
    Dataset<RecurringIdentifierRow> txns = source.load(spark, batchId);

    assertThat(txns.count()).isEqualTo(1L);
  }
}
