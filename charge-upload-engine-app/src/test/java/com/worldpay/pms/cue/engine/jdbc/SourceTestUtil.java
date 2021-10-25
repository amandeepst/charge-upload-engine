package com.worldpay.pms.cue.engine.jdbc;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.spark.core.PMSException;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.Batch.BatchStep;
import com.worldpay.pms.spark.core.batch.Batch.Watermark;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import java.sql.Timestamp;
import lombok.experimental.UtilityClass;
import org.sql2o.Connection;
import org.sql2o.Sql2oException;
import org.sql2o.Sql2oQuery;

@UtilityClass
public class SourceTestUtil {

  public static void insertBatchHistoryAndOnBatchCompleted(
      SqlDb sqlDb,
      String batchCode,
      String state,
      Timestamp lowWatermark,
      Timestamp highWatermark,
      Timestamp createdAt) {
    Batch batch =
        new Batch(
            new BatchId(
                batchCode,
                new Watermark(lowWatermark.toLocalDateTime(), highWatermark.toLocalDateTime()),
                1),
            BatchStep.valueOf(state),
            createdAt.toLocalDateTime(),
            null,
            null);

    sqlDb.exec("insert_batch_history", conn -> insertBatchHistory(conn, batch));
  }

  public static void deleteBatchHistoryAndOnBatchCompleted(SqlDb db, String batchCode) {
    db.execQuery(
        "delete-batch-history",
        resourceAsString("sql/batch_history/delete_batch_history_by_batch_code.sql"),
        (query) -> query.addParameter("batch_code", batchCode).executeUpdate());
  }

  private Batch insertBatchHistory(Connection conn, Batch batch) {
    Sql2oQuery query =
        new Sql2oQuery(conn, resourceAsString("sql/batch_history/insert_batch_history.sql"));

    try {
      query
          .addParameter("batch_code", batch.id.code)
          .addParameter("attempt", batch.id.attempt)
          .addParameter("state", batch.step.name())
          .addParameter("watermark_low", Timestamp.valueOf(batch.id.watermark.low))
          .addParameter("watermark_high", Timestamp.valueOf(batch.id.watermark.high))
          .addParameter("comments", "")
          .addParameter("metadata", "")
          .addParameter("created_at", Timestamp.valueOf(batch.createdAt))
          .executeUpdate();
    } catch (Sql2oException e) {
      throw new PMSException(e, "Unhandled exception when executing query:insert batch history");
    }

    return batch;
  }
}
