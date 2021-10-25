package com.worldpay.pms.mdu.engine.transformations.sources;

import static com.worldpay.pms.mdu.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static com.worldpay.pms.mdu.engine.utils.DbUtils.insertCompletedBatch;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.mdu.domain.model.output.AccountHierarchy;
import com.worldpay.pms.mdu.engine.MerchantUploadConfig;
import com.worldpay.pms.mdu.engine.utils.DbUtils;
import com.worldpay.pms.mdu.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcSourceConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AccountHierarchySourceTest implements WithDatabaseAndSpark {

  private SqlDb appuserDb;
  private SqlDb cisadmDb;
  private JdbcSourceConfiguration sourceConfiguration;
  private AccountHierarchySource source;
  private static final String ACCOUNT_HIER = "ACCOUNT_HIERARCHY";
  public static final LocalDateTime ILM_DT = LocalDateTime.of(2021, 3, 1, 0, 0, 0);
  public static final LocalDateTime LOW_WATERMARK = LocalDateTime.of(2021, 3, 1, 0, 6, 1);
  public static final LocalDateTime HIGH_WATERMARK = LocalDateTime.of(2021, 3, 1, 0, 6, 2);

  @Override
  public void bindOrmbJdbcConfiguration(JdbcConfiguration conf) {
    this.cisadmDb = SqlDb.simple(conf);
  }

  @Override
  public void bindMerchantUploadJdbcConfiguration(JdbcConfiguration conf) {
    this.appuserDb = SqlDb.simple(conf);
  }

  @Override
  public void bindMerchantUploadConfiguration(MerchantUploadConfig conf) {
    this.sourceConfiguration = conf.getSources().getAccountHierarchySource();
    this.sourceConfiguration.setPartitionCount(1);
    this.source = new AccountHierarchySource(sourceConfiguration);
  }

  @BeforeEach
  void cleanUp() {
    DbUtils.cleanUp(appuserDb, "acct_hier", "batch_history", "outputs_registry");
  }

  @Test
  void readEmptyDatasetWhenNoRecordsFound(SparkSession spark) {
    Batch.BatchId batchId = createBatchId(LocalDate.of(2021, 1, 1), LocalDate.of(2021, 12, 1));
    Dataset<AccountHierarchy> txns = source.load(spark, batchId);
    assertThat(txns.count()).isEqualTo(0L);
  }

  @Test
  void readCurrentAccountHierarchyDataset(SparkSession spark) {

    readFromCsvFileAndWriteToExistingTable(
        appuserDb, "input/SourceTest/CurrentAcctHierarchySourceTest/acct_hier.csv", "acct_hier");

    insertCompletedBatch(
        appuserDb, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, ACCOUNT_HIER);

    Batch.BatchId batchId = createBatchId(LOW_WATERMARK, HIGH_WATERMARK);
    List<AccountHierarchy> txns = source.load(spark, batchId).collectAsList();
    assertThat(txns.size()).isEqualTo(3L);
  }
}
