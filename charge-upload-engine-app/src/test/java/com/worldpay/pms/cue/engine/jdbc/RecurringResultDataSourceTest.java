package com.worldpay.pms.cue.engine.jdbc;

import static com.typesafe.config.ConfigBeanFactory.create;
import static com.typesafe.config.ConfigFactory.load;
import static com.worldpay.pms.cue.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static java.sql.Date.valueOf;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.cue.engine.ChargingUploadConfig;
import com.worldpay.pms.cue.engine.ChargingUploadConfig.RecurringResultSourceConfiguration;
import com.worldpay.pms.cue.engine.recurring.RecurringResultRow;
import com.worldpay.pms.cue.engine.recurring.RecurringResultSource;
import com.worldpay.pms.cue.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import java.time.LocalDate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sql2o.Query;
import org.sql2o.Sql2oQuery;

public class RecurringResultDataSourceTest implements WithDatabaseAndSpark {

  private SqlDb appuserDb;
  private static SqlDb cisadmDb;
  private RecurringResultSourceConfiguration sourceConfiguration;

  @Override
  public void bindChargingConfiguration(ChargingUploadConfig conf) {
    this.sourceConfiguration = conf.getSources().getRecurringSource();
    this.sourceConfiguration.setPartitionCount(4);
  }

  @Override
  public void bindChargingJdbcConfiguration(JdbcConfiguration conf) {
    this.appuserDb = SqlDb.simple(conf);
  }

  @BeforeEach
  void cleanUp() {
    appuserDb.execQuery("delete cm_rec_chg", "DELETE FROM CM_REC_CHG", Sql2oQuery::executeUpdate);
    appuserDb.execQuery(
        "delete cm_misc_bill_item", "DELETE FROM CM_MISC_BILL_ITEM", Sql2oQuery::executeUpdate);
    appuserDb.execQuery("delete cm_rec_idfr", "DELETE FROM CM_REC_IDFR", Sql2oQuery::executeUpdate);
    appuserDb.execQuery(
        "delete batch_history", "DELETE FROM BATCH_HISTORY", Sql2oQuery::executeUpdate);
  }

  @BeforeAll
  static void setUpData() {
    cisadmDb =
        SqlDb.simple(create(load("conf/db.conf").getConfig("cisadm"), JdbcConfiguration.class));
    cisadmDb.execQuery(
        "delete table ci_bill_cyc_sch", "delete from ci_bill_cyc_sch", Query::executeUpdate);
    readFromCsvFileAndWriteToExistingTable(
        cisadmDb, "input/PendingBillableChargeSourceTest/ci_bill_cycle.csv", "ci_bill_cyc_sch");
  }

  @AfterAll
  static void cleanUpData() {
    cisadmDb.execQuery(
        "delete table ci_bill_cyc_sch", "delete from ci_bill_cyc_sch", Query::executeUpdate);
  }

  @Test
  void whenOnePreviousMonthRecordPresentInRecChargeAndNewRecordsFromBchgStg(SparkSession spark) {
    // Scenario-1 : previous month entry already existing in cm_rec_chg table and new entries
    // added in cm_rec_chg for month-end
    // one 'WPDY' cycle code entry also inserted for cre_dttm 01 sept
    // and one 'WPDY' cycle code entry also inserted for cre_dttm 02 sept but with INACTIVE status,
    // wont be picked
    // and logical date is less than submit date then create charge uptill  logical-date
    RecurringResultSource source =
        new RecurringResultSource(sourceConfiguration, valueOf("2020-09-03"));
    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/RecurringResultSourceTest/batch_history_scenario1.csv", "batch_history");
    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/RecurringResultSourceTest/cm_rec_chg_scenario1.csv", "cm_rec_chg");

    // same entries for that sourceId already processed and present in cm_rec_idfr
    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/RecurringResultSourceTest/cm_rec_idfr_scenario1.csv", "cm_rec_idfr");

    BatchId batchId = createBatchId(LocalDate.of(2020, 1, 1), LocalDate.of(2020, 9, 3));
    Dataset<RecurringResultRow> txns = source.load(spark, batchId);
    // then that old entries not pick by dataset
    assertThat(txns.count()).isEqualTo(6L);
  }

  @Test
  void whenNoSourceIdMatchInBillItemAsTheyWereIssuedOnPrevCutoffDate(SparkSession spark) {
    // scenario 2 :  When there is no match entry exists in bill item table w.r.t source_id
    RecurringResultSource source =
        new RecurringResultSource(sourceConfiguration, valueOf("2020-09-30"));
    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/RecurringResultSourceTest/cm_rec_chg_scenario2.csv", "cm_rec_chg");
    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/RecurringResultSourceTest/batch_history_scenario2.csv", "batch_history");

    BatchId batchId = createBatchId(LocalDate.of(2020, 1, 1), LocalDate.of(2020, 9, 30));
    Dataset<RecurringResultRow> txns = source.load(spark, batchId);
    assertThat(txns.count()).isEqualTo(33L);
  }

  @Test
  void whenSomeSourceIdMatchInBillItemAsTheyWereIssuedOnPrevCutoffDate(SparkSession spark) {
    // scenario 3 :  When There are SomeSourceId entry exists in bill item table
    RecurringResultSource source =
        new RecurringResultSource(sourceConfiguration, valueOf("2020-09-30"));
    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/RecurringResultSourceTest/cm_rec_chg_scenario3.csv", "cm_rec_chg");
    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/RecurringResultSourceTest/batch_history_scenario3.csv", "batch_history");

    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/RecurringResultSourceTest/cm_rec_idfr_scenario3.csv", "cm_rec_idfr");

    BatchId batchId = createBatchId(LocalDate.of(2020, 1, 1), LocalDate.of(2020, 9, 30));
    Dataset<RecurringResultRow> txns = source.load(spark, batchId);
    assertThat(txns.count()).isEqualTo(10L);
  }

  @Test
  void whenSomeSourceIdMatchInBillItemsThenPickOnlyLatestCutoffDateChargeWithinValidDateRange(
      SparkSession spark) {
    // scenario 4 :  When multiple entries exist in cm_misc_bill_item with same source_id,
    // then pick only latest recurring charge whose cutoff_dt is greater than with max cutoff date
    // in misc_bill_item
    // but upto validTo of cm_rec_chg
    RecurringResultSource source =
        new RecurringResultSource(sourceConfiguration, valueOf("2020-09-15"));

    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/RecurringResultSourceTest/cm_rec_chg_scenario4.csv", "cm_rec_chg");
    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/RecurringResultSourceTest/batch_history_scenario4.csv", "batch_history");

    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/RecurringResultSourceTest/cm_rec_idfr_scenario4.csv", "cm_rec_idfr");

    BatchId batchId = createBatchId(LocalDate.of(2020, 1, 1), LocalDate.of(2020, 6, 15));
    Dataset<RecurringResultRow> txns = source.load(spark, batchId);
    assertThat(txns.count()).isEqualTo(11L);
  }

  @Test
  void noRecurringChargePickedWhenJobParameterIsOutOfValidDateRange(SparkSession spark) {
    // scenario 5 :  None recurring charge picked if job parameter date does not lies within
    // validFrom and validTo
    RecurringResultSource source1 =
        new RecurringResultSource(sourceConfiguration, valueOf("2020-09-09"));
    RecurringResultSource source2 =
        new RecurringResultSource(sourceConfiguration, valueOf("2020-09-16"));
    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/RecurringResultSourceTest/cm_rec_chg_scenario5.csv", "cm_rec_chg");
    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/RecurringResultSourceTest/batch_history_scenario5.csv", "batch_history");

    // when job parameter date is before valid range
    BatchId batchIdBefore = createBatchId(LocalDate.of(2020, 1, 1), LocalDate.of(2020, 6, 15));
    Dataset<RecurringResultRow> txnsWithBeforeValidDate = source1.load(spark, batchIdBefore);
    assertThat(txnsWithBeforeValidDate.count()).isEqualTo(0L);

    // when job parameter date is after valid range
    BatchId batchIdAfter = createBatchId(LocalDate.of(2020, 1, 1), LocalDate.of(2020, 6, 15));
    Dataset<RecurringResultRow> txnsWithAfterValidDate = source2.load(spark, batchIdAfter);
    assertThat(txnsWithAfterValidDate.count()).isEqualTo(0L);
  }
}
