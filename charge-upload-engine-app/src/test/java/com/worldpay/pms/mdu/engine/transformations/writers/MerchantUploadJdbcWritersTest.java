package com.worldpay.pms.mdu.engine.transformations.writers;

import com.worldpay.pms.config.ApplicationConfiguration;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.Batch.Watermark;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.testing.junit.JdbcWriterBaseTest;
import com.worldpay.pms.testing.stereotypes.WithSpark;
import java.time.LocalDateTime;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sql2o.Sql2oQuery;

@WithSpark
public abstract class MerchantUploadJdbcWritersTest<T> extends JdbcWriterBaseTest<T> {

  private static final LocalDateTime WATERMARK_LOW = LocalDateTime.parse("2020-08-10T00:00:00");
  private static final LocalDateTime WATERMARK_HIGH = LocalDateTime.parse("2020-08-11T00:00:00");
  static final BatchId BATCH_ID =
      new BatchId("xyz", new Watermark(WATERMARK_LOW, WATERMARK_HIGH), 1);
  static final LocalDateTime STARTED_AT = LocalDateTime.parse("2020-08-10T10:00:00");
  protected static SparkSession spark;

  @BeforeAll
  static void initSpark(SparkSession sparkSession) {
    spark = sparkSession;
  }

  @BeforeEach
  void setUp() {
    db.execQuery(
        "clean partition_history", "DELETE FROM PARTITION_HISTORY", Sql2oQuery::executeUpdate);
    init();
  }

  @AfterEach
  void cleanUp() {
    db.execQuery(
        "clean partition_history", "DELETE FROM PARTITION_HISTORY", Sql2oQuery::executeUpdate);
    init();
  }

  @Test
  void canWriteNonEmptyPartition(ApplicationConfiguration conf, SparkSession spark) {
    JdbcWriter<T> writer = createWriter(conf.getDb(), true);
    writer.write(BATCH_ID, STARTED_AT, spark.createDataset(provideSamples(), encoder()));

    assertRowsWritten();
  }

  protected abstract void init();

  protected abstract void assertRowsWritten();
}
