package com.worldpay.pms.mdu.engine.transformations.sources;

import static com.worldpay.pms.mdu.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.mdu.engine.MerchantUploadConfig;
import com.worldpay.pms.mdu.engine.MerchantUploadConfig.MerchantDataSourceConfiguration;
import com.worldpay.pms.mdu.engine.transformations.model.input.MerchantDataRow;
import com.worldpay.pms.mdu.engine.utils.DbUtils;
import com.worldpay.pms.mdu.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import java.time.LocalDate;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MerchantDataSourceTest implements WithDatabaseAndSpark {

  private SqlDb appuserDb;
  private SqlDb cisadmDb;
  private MerchantDataSourceConfiguration sourceConfiguration;
  private MerchantDataSource source;

  @Override
  public void bindMerchantUploadConfiguration(MerchantUploadConfig conf) {
    this.sourceConfiguration = conf.getSources().getMerchantDataSource();
    this.sourceConfiguration.setPartitionCount(4);
    this.source = new MerchantDataSource(sourceConfiguration);
  }

  @Override
  public void bindMerchantUploadJdbcConfiguration(JdbcConfiguration conf) {
    this.appuserDb = SqlDb.simple(conf);
  }

  @Override
  public void bindOrmbJdbcConfiguration(JdbcConfiguration conf) {
    this.cisadmDb = SqlDb.simple(conf);
  }

  @BeforeEach
  void cleanUp() {
    DbUtils.cleanUp(cisadmDb, "cm_acct_stg", "cm_merch_char", "cm_merch_stg");
  }

  @Test
  void canFetchPendingTransactionsWhenNoTransactionInDatabase(SparkSession spark) {
    Batch.BatchId batchId = createBatchId(LocalDate.of(2021, 1, 1), LocalDate.of(2021, 12, 1));
    Dataset<MerchantDataRow> txns = source.load(spark, batchId);
    assertThat(txns.count()).isEqualTo(0L);
  }

  @Test
  void canFetchTransactionsWhenTransactionExistInDatabase(SparkSession spark) {

    readFromCsvFileAndWriteToExistingTable(
        cisadmDb, "input/SourceTest/MerchantDataSourceTest/cm_merch_stg.csv", "cm_merch_stg");
    readFromCsvFileAndWriteToExistingTable(
        cisadmDb, "input/SourceTest/MerchantDataSourceTest/cm_merch_char.csv", "cm_merch_char");
    readFromCsvFileAndWriteToExistingTable(
        cisadmDb, "input/SourceTest/MerchantDataSourceTest/cm_acct_stg.csv", "cm_acct_stg");

    List<MerchantDataRow> txns =
        source
            .load(spark, createBatchId(LocalDate.of(2021, 1, 20), LocalDate.of(2021, 01, 22)))
            .collectAsList();
    assertThat(txns.size()).isEqualTo(10);

    // each party staged should have account mapped
    assertThat(txns).filteredOn(row -> row.getAccountType() != null).hasSize(10);

    // waf flg exist for party PO4008759561
    assertThat(txns)
        .filteredOn(row -> row.getPartyId().equals("PO4008759561"))
        .extracting(row -> row.getWafFlag())
        .contains("PO1300000002");
  }
}
