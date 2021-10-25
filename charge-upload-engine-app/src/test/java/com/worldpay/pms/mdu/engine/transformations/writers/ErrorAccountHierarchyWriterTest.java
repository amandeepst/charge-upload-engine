package com.worldpay.pms.mdu.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static org.apache.spark.sql.SparkSession.getActiveSession;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.mdu.engine.encoder.Encoders;
import com.worldpay.pms.mdu.engine.transformations.ErrorAccountHierarchy;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Encoder;

public class ErrorAccountHierarchyWriterTest
    extends MerchantUploadJdbcWritersTest<ErrorAccountHierarchy> {

  private static final String QUERY_COUNT_ERROR_HIER_BY_ID =
      resourceAsString("sql/count_error_hier.sql");
  private static final String QUERY_DELETE_ERROR_HIER =
      resourceAsString("sql/delete_error_hier_by_id.sql");
  private static final String TX_HEADER_ID_1 = "txHeaderId-1";

  private static final ErrorAccountHierarchy ERROR_HIER_TRANSACTION =
      new ErrorAccountHierarchy(
          TX_HEADER_ID_1,
          "perIdNbr1",
          "perIdNbr2",
          "GBP",
          "00001",
          "CHRG",
          "test-code1",
          "test-message1",
          "stackTrace",
          false,
          true);

  @Override
  protected void init() {
    db.execQuery(
        "delete-error-hier-transaction",
        QUERY_DELETE_ERROR_HIER,
        query -> query.addParameter("txnHeaderId", TX_HEADER_ID_1).executeUpdate());
  }

  @Override
  protected void assertRowsWritten() {
    assertThat(countErrorHierTransaction()).isEqualTo(1L);
  }

  @Override
  protected void assertNoRowsWritten() {
    assertThat(countHierError(TX_HEADER_ID_1)).isZero();
  }

  @Override
  protected List<ErrorAccountHierarchy> provideSamples() {
    return Collections.unmodifiableList(Collections.singletonList(ERROR_HIER_TRANSACTION));
  }

  @Override
  protected Encoder<ErrorAccountHierarchy> encoder() {
    return Encoders.ERROR_HIER_TRANSACTION_ENCODER;
  }

  @Override
  protected JdbcWriter<ErrorAccountHierarchy> createWriter(
      JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new ErrorAccountHierarchyWriter(jdbcWriterConfiguration, getActiveSession().get());
  }

  private long countHierError(String txnHeaderId) {
    return db.execQuery(
        "count-error-hier",
        "select count(*) from error_acct_hier where trim(txn_header_id) = :txnHeaderId",
        query -> query.addParameter("txnHeaderId", txnHeaderId).executeScalar(Long.TYPE));
  }

  private long countErrorHierTransaction() {
    return db.execQuery(
        "count-error-hier-transaction-by-all-fields",
        QUERY_COUNT_ERROR_HIER_BY_ID,
        query ->
            query
                .addParameter("txnHeaderId", ERROR_HIER_TRANSACTION.getTxnHeaderId())
                .addParameter("parentPartyId", ERROR_HIER_TRANSACTION.getParentPartyId())
                .addParameter("reason", ERROR_HIER_TRANSACTION.getMessage())
                .addParameter("stackTrace", ERROR_HIER_TRANSACTION.getStackTrace())
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .addParameter("ilmDt", Timestamp.valueOf(STARTED_AT))
                .executeScalar(Long.TYPE));
  }
}
