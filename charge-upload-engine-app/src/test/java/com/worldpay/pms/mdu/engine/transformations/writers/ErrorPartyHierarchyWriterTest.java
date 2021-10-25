package com.worldpay.pms.mdu.engine.transformations.writers;

import static com.worldpay.pms.mdu.engine.encoder.Encoders.ERROR_PARTY_HIER_TRANSACTION_ENCODER;
import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static org.apache.spark.sql.SparkSession.getActiveSession;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.mdu.engine.transformations.ErrorPartyHierarchy;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Encoder;

public class ErrorPartyHierarchyWriterTest
    extends MerchantUploadJdbcWritersTest<ErrorPartyHierarchy> {

  private static final String QUERY_COUNT_ERROR_HIER_BY_ID =
      resourceAsString("sql/count_error_party_hier.sql");
  private static final String QUERY_DELETE_ERROR_HIER =
      resourceAsString("sql/delete_error_party_hier_by_id.sql");
  private static final String TX_HEADER_ID_1 = "txHeaderId-1";

  private static final ErrorPartyHierarchy ERROR_HIER_TRANSACTION =
      new ErrorPartyHierarchy(
          TX_HEADER_ID_1,
          "perIdNbr1",
          "perIdNbr2",
          "test-code1",
          "test-message1",
          "stackTrace",
          false,
          true);

  @Override
  protected void init() {
    db.execQuery(
        "delete-error-party-hier-transaction",
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
  protected List<ErrorPartyHierarchy> provideSamples() {
    return Collections.unmodifiableList(Collections.singletonList(ERROR_HIER_TRANSACTION));
  }

  @Override
  protected Encoder<ErrorPartyHierarchy> encoder() {
    return ERROR_PARTY_HIER_TRANSACTION_ENCODER;
  }

  @Override
  protected JdbcWriter<ErrorPartyHierarchy> createWriter(
      JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new ErrorPartyHierarchyWriter(jdbcWriterConfiguration, getActiveSession().get());
  }

  private long countHierError(String txnHeaderId) {
    return db.execQuery(
        "count-error-hier",
        "select count(*) from error_party_hier where trim(txn_header_id) = :txnHeaderId",
        query -> query.addParameter("txnHeaderId", txnHeaderId).executeScalar(Long.TYPE));
  }

  private long countErrorHierTransaction() {
    return db.execQuery(
        "count-error-party-hier-transaction-by-all-fields",
        QUERY_COUNT_ERROR_HIER_BY_ID,
        query ->
            query
                .addParameter("txnHeaderId", ERROR_HIER_TRANSACTION.getTxnHeaderId())
                .addParameter("parentPartyId", ERROR_HIER_TRANSACTION.getParentPartyId())
                .addParameter("childPartyId", ERROR_HIER_TRANSACTION.getChildPartyId())
                .addParameter("reason", ERROR_HIER_TRANSACTION.getMessage())
                .addParameter("stackTrace", ERROR_HIER_TRANSACTION.getStackTrace())
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .addParameter("ilmDt", Timestamp.valueOf(STARTED_AT))
                .executeScalar(Long.TYPE));
  }
}
