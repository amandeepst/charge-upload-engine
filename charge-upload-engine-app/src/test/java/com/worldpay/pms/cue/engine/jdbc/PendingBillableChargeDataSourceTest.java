package com.worldpay.pms.cue.engine.jdbc;

import static com.worldpay.pms.cue.engine.jdbc.SourceTestUtil.deleteBatchHistoryAndOnBatchCompleted;
import static com.worldpay.pms.cue.engine.jdbc.SourceTestUtil.insertBatchHistoryAndOnBatchCompleted;
import static com.worldpay.pms.cue.engine.utils.DatabaseCsvUtils.readFromCsvFileAndDeleteFromTable;
import static com.worldpay.pms.cue.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.cue.engine.ChargingUploadConfig;
import com.worldpay.pms.cue.engine.ChargingUploadConfig.BillableChargeSourceConfiguration;
import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeRow;
import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeSource;
import com.worldpay.pms.cue.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import java.sql.Timestamp;
import java.time.LocalDate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PendingBillableChargeDataSourceTest implements WithDatabaseAndSpark {

  private SqlDb appuserDb;
  private SqlDb cisadmDb;
  private PendingBillableChargeSource source;
  private BillableChargeSourceConfiguration sourceConfiguration;

  @Override
  public void bindChargingConfiguration(ChargingUploadConfig conf) {
    this.sourceConfiguration = conf.getSources().getChargeEvents();
    this.sourceConfiguration.setPartitionCount(4);
    this.sourceConfiguration.setPartitionHighBound(8);
    this.sourceConfiguration.setFetchSize(3);
    this.source = new PendingBillableChargeSource(this.sourceConfiguration, conf.getMaxAttempts());
  }

  @Override
  public void bindChargingJdbcConfiguration(JdbcConfiguration conf) {
    this.appuserDb = SqlDb.simple(conf);
  }

  @Override
  public void bindOrmbJdbcConfiguration(JdbcConfiguration conf) {
    this.cisadmDb = SqlDb.simple(conf);
  }

  @BeforeEach
  void cleanUp() {
    // cleanup
    readFromCsvFileAndDeleteFromTable(
        cisadmDb,
        "input/PendingBillableChargeSourceTest/cm_bchg_stg.csv",
        "cm_bchg_stg",
        "TXN_HEADER_ID");
    readFromCsvFileAndDeleteFromTable(
        appuserDb,
        "input/PendingBillableChargeSourceTest/error_transactions.csv",
        "error_transaction",
        "TXN_HEADER_ID");
    deleteBatchHistoryAndOnBatchCompleted(appuserDb, "test-run");
    readFromCsvFileAndDeleteFromTable(
        appuserDb,
        "input/PendingBillableChargeSourceTest/cm_rec_chg.csv",
        "cm_rec_chg",
        "REC_CHG_ID");
  }

  @Test
  void canFetchPendingChargeEventsWhenNoChargeEventsInDatabase(SparkSession spark) {

    BatchId batchId = createBatchId(LocalDate.of(2019, 1, 1), LocalDate.of(2019, 12, 1));
    Dataset<PendingBillableChargeRow> txns = source.load(spark, batchId);
    assertThat(txns.count()).isEqualTo(0L);
  }

  @Test
  void canFetchPendingChargeEventWhenThereAreChargeEvents(SparkSession spark) {

    readFromCsvFileAndWriteToExistingTable(
        cisadmDb, "input/PendingBillableChargeSourceTest/cm_bchg_stg.csv", "cm_bchg_stg");

    BatchId batchId = createBatchId(LocalDate.of(2010, 4, 22), LocalDate.of(2020, 4, 25));
    Dataset<PendingBillableChargeRow> txns = source.load(spark, batchId);

    assertThat(txns.rdd().getNumPartitions()).isEqualTo(4);
    assertThat(txns.count()).isEqualTo(11L);
    txns.foreachPartition(iterator -> assertThat(iterator.hasNext()).isTrue());
  }

  @Test
  void canFetchPreviouslyFailedChargeEvents(SparkSession session) {

    // Use APPUSER connection for error_transaction and batch_history table insert/delete
    readFromCsvFileAndWriteToExistingTable(
        appuserDb,
        "input/PendingBillableChargeSourceTest/error_transactions.csv",
        "error_transaction");
    insertBatchHistoryAndOnBatchCompleted(
        appuserDb,
        "test-run",
        "COMPLETED",
        Timestamp.valueOf("2013-06-03 00:00:00"),
        Timestamp.valueOf("2013-06-03 01:00:00"),
        new Timestamp(System.currentTimeMillis()));

    // Use CISADM connection for staging table insert/delete
    readFromCsvFileAndWriteToExistingTable(
        cisadmDb, "input/PendingBillableChargeSourceTest/cm_bchg_stg.csv", "cm_bchg_stg");

    BatchId batchId =
        createBatchId(
            LocalDate.of(2013, 6, 03).atStartOfDay(), LocalDate.of(2020, 4, 24).atStartOfDay());
    Dataset<PendingBillableChargeRow> txns = source.load(session, batchId);

    // 4 out of 5 failed events with valid retry count (one is ignore because of retry-count over
    // threshold
    assertThat(txns.count()).isEqualTo(4L);
  }

  @Test
  void testLeftOuterJoinWithRecurringChargeTable(SparkSession session) {
    // Use APPUSER connection for and batch_history and cm_rec_chg table insert/delete
    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/PendingBillableChargeSourceTest/cm_rec_chg.csv", "cm_rec_chg");
    insertBatchHistoryAndOnBatchCompleted(
        appuserDb,
        "test-run",
        "COMPLETED",
        Timestamp.valueOf("2013-06-03 00:00:00"),
        Timestamp.valueOf("2013-06-03 01:00:00"),
        Timestamp.valueOf("2013-06-03 00:00:00"));

    // Use CISADM connection for staging table insert/delete
    readFromCsvFileAndWriteToExistingTable(
        cisadmDb, "input/PendingBillableChargeSourceTest/cm_bchg_stg.csv", "cm_bchg_stg");

    BatchId batchId =
        createBatchId(
            LocalDate.of(2020, 4, 22).atStartOfDay(), LocalDate.of(2020, 4, 25).atStartOfDay());

    Dataset<PendingBillableChargeRow> txns = source.load(session, batchId);

    assertThat(txns.collectAsList())
        .extracting(PendingBillableChargeRow::getRecurringIdentifierForUpdation)
        .filteredOn(row -> row != null)
        .hasSize(2);
  }
}
