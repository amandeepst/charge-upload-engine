package com.worldpay.pms.cue.engine.transformations.sources;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.cue.engine.utils.data.Db;
import com.worldpay.pms.cue.engine.utils.data.model.TestBillItemRow;
import com.worldpay.pms.cue.engine.utils.data.model.TestPendingBillableChargeRow;
import com.worldpay.pms.cue.engine.utils.data.model.TestRecurringChargeRow;
import com.worldpay.pms.spark.core.DataSource;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.Batch.Watermark;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcSourceConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.stereotypes.WithInMemoryDb;
import com.worldpay.pms.testing.stereotypes.WithSpark;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@WithSpark
@WithInMemoryDb(init = "h2/init.sql")
@Slf4j
public abstract class TransactionSourceTest<T> {

  DataSource<T> source;
  protected SqlDb db;
  private static final int MAX_ATTEMPT = 3;
  private static SimpleDateFormat ORACLE_DATETIMEFORMAT =
      new SimpleDateFormat("dd-MMM-yy hh.mm.ss");
  private static SimpleDateFormat ORACLE_DATEFORMAT = new SimpleDateFormat("dd-MMM-yy");

  @BeforeEach
  void setUp(JdbcConfiguration h2, SqlDb db) {
    JdbcSourceConfiguration source =
        new JdbcSourceConfiguration() {
          {
            setDataSource(h2);
            setPartitionCount(4);
            setPrintExecutionPlan(false);
          }
        };

    this.db = db;
    this.source = createSource(source, MAX_ATTEMPT);
  }

  @Test
  void whenNoInputDataThenNoRowsAreRead() {
    assertThat(read().count()).isEqualTo(0L);
  }

  protected abstract DataSource<T> createSource(JdbcSourceConfiguration conf, int maxAttempt);

  Dataset<T> read() {
    return read("batch", 1, Watermark.from(LocalDate.of(2005, 1, 1)));
  }

  Dataset<T> read(String batchCode, int batchAttempt, Watermark wm) {
    return source
        .load(SparkSession.getActiveSession().get(), new BatchId(batchCode, wm, batchAttempt))
        .cache();
  }

  void insertCharge(TestPendingBillableChargeRow row) {
    Db.insertCharge(db, row);
  }

  void insertError(
      String txnheaderId,
      String perIdNbr,
      String saTypeCode,
      int retryCount,
      String batchCode,
      int batchAttempt,
      long partition,
      String ilmDate) {
    Db.insertError(
        db,
        txnheaderId,
        perIdNbr,
        saTypeCode,
        retryCount,
        batchCode,
        batchAttempt,
        partition,
        ilmDate);
  }

  void insertRecurring(TestRecurringChargeRow row) {
    Db.insertRecurring(db, row);
  }

  void insertBillItem(TestBillItemRow row) {
    Db.insertBillItem(db, row);
  }

  void insertBatch(String code, int attempt) {
    db.exec(
        "insert-batch",
        conn ->
            conn.createQuery(
                    resourceAsString("sql/batch-history/insert-batch-history.sql")
                        .replace("{{table_name}}", "batch_history"))
                .addParameter("batch_code", code)
                .addParameter("attempt", attempt)
                .addParameter("state", "COMPLETED")
                .addParameter("wm_low", Timestamp.from(Instant.now()))
                .addParameter("wm_high", Timestamp.from(Instant.now()))
                .addParameter("comments", (String) null)
                .addParameter("metadata", (String) null)
                .addParameter("created_at", Timestamp.from(Instant.now()))
                .executeUpdate());
  }

  @SneakyThrows
  public static Timestamp timestamp(String value) {
    if (value == null || value.isEmpty()) return null;
    try {
      return new Timestamp(ORACLE_DATETIMEFORMAT.parse(value).getTime());
    } catch (ParseException e) {
      return new Timestamp(ORACLE_DATEFORMAT.parse(value).getTime());
    }
  }
}
