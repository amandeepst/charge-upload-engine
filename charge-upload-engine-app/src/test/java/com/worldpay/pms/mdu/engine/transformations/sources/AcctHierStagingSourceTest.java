package com.worldpay.pms.mdu.engine.transformations.sources;

import static com.worldpay.pms.mdu.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.mdu.engine.MerchantUploadConfig;
import com.worldpay.pms.mdu.engine.MerchantUploadConfig.AccountHierarchySourceConfiguration;
import com.worldpay.pms.mdu.engine.transformations.model.input.AccountHierarchyDataRow;
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

public class AcctHierStagingSourceTest implements WithDatabaseAndSpark {

  private SqlDb appuserDb;
  private SqlDb cisadmDb;
  private AccountHierarchySourceConfiguration sourceConfiguration;
  private AccountHierStagingSource source;

  @Override
  public void bindMerchantUploadConfiguration(MerchantUploadConfig conf) {
    this.sourceConfiguration = conf.getSources().getAccountHierarchyDataSource();
    this.sourceConfiguration.setPartitionCount(1);
    this.source = new AccountHierStagingSource(sourceConfiguration);
  }

  @Override
  public void bindOrmbJdbcConfiguration(JdbcConfiguration conf) {
    this.cisadmDb = SqlDb.simple(conf);
  }

  @Override
  public void bindMerchantUploadJdbcConfiguration(JdbcConfiguration conf) {
    this.appuserDb = SqlDb.simple(conf);
  }

  @BeforeEach
  void cleanUp() {
    DbUtils.cleanUp(cisadmDb, "cm_inv_grp_stg");
  }

  @Test
  void canFetchPendingTransactionsWhenNoTransactionInDatabase(SparkSession spark) {
    Batch.BatchId batchId = createBatchId(LocalDate.of(2021, 1, 1), LocalDate.of(2021, 12, 1));
    Dataset<AccountHierarchyDataRow> txns = source.load(spark, batchId);
    assertThat(txns.count()).isEqualTo(0L);
  }

  @Test
  void canFetchTransactionsWhenTransactionExistInDatabase(SparkSession spark) {

    readFromCsvFileAndWriteToExistingTable(
        cisadmDb,
        "input/SourceTest/AccountHierarchySourceTest/cm_inv_grp_stg.csv",
        "cm_inv_grp_stg");

    List<AccountHierarchyDataRow> txns =
        source
            .load(spark, createBatchId(LocalDate.of(2021, 01, 20), LocalDate.of(2021, 02, 22)))
            .collectAsList();
    assertThat(txns.size()).isEqualTo(4);
  }
}
