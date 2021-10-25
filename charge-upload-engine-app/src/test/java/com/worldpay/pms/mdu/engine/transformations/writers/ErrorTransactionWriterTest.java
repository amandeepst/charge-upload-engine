package com.worldpay.pms.mdu.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static org.apache.spark.sql.SparkSession.getActiveSession;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.mdu.engine.encoder.Encoders;
import com.worldpay.pms.mdu.engine.transformations.ErrorTransaction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Encoder;

public class ErrorTransactionWriterTest extends MerchantUploadJdbcWritersTest<ErrorTransaction> {

  private static final String QUERY_COUNT_PENDING_BILLABLE_CHARGE_ERROR_BY_ALL_FIELDS =
      resourceAsString("sql/count_error_transaction_by_all_fields.sql");

  private static final String TX_HEADER_ID_1 = "txHeaderId-1";

  private static final String QUERY_DELETE_ERROR_TRANSACTION_BY_ID =
      resourceAsString("sql/delete_error_transaction_by_id.sql");

  private static final ErrorTransaction ERROR_TRANSACTION =
      new ErrorTransaction(
          TX_HEADER_ID_1, "perIdNbr", "test-code1", "test-message1", "stackTrace", false, true);

  @Override
  protected void init() {
    db.execQuery(
        "delete-error-transaction",
        QUERY_DELETE_ERROR_TRANSACTION_BY_ID,
        query -> query.addParameter("txnHeaderId", TX_HEADER_ID_1).executeUpdate());
  }

  @Override
  protected void assertRowsWritten() {
    assertThat(countErrorTransaction()).isZero();
  }

  @Override
  protected List<ErrorTransaction> provideSamples() {
    return Collections.unmodifiableList(Collections.singletonList(ERROR_TRANSACTION));
  }

  @Override
  protected Encoder<ErrorTransaction> encoder() {
    return Encoders.ERROR_TRANSACTION_ENCODER;
  }

  @Override
  protected JdbcWriter<ErrorTransaction> createWriter(JdbcWriterConfiguration conf) {
    return new ErrorTransactionWriter(conf, getActiveSession().get());
  }

  @Override
  protected void assertNoRowsWritten() {
    assertThat(countMerchantUploadError(TX_HEADER_ID_1)).isZero();
  }

  private long countMerchantUploadError(String txnHeaderId) {
    return db.execQuery(
        "count-pending-billable-charge-error",
        "select count(*) from error_transaction where trim(txn_header_id) = :txnHeaderId",
        query -> query.addParameter("txnHeaderId", txnHeaderId).executeScalar(Long.TYPE));
  }

  private long countErrorTransaction() {
    return db.execQuery(
        "count-error-transaction-by-all-fields",
        QUERY_COUNT_PENDING_BILLABLE_CHARGE_ERROR_BY_ALL_FIELDS,
        query ->
            query
                .addParameter("txnHeaderId", ERROR_TRANSACTION.getTxnHeaderId())
                .addParameter("partyId", ERROR_TRANSACTION.getTxnHeaderId())
                .addParameter("reason", ERROR_TRANSACTION.getMessage())
                .addParameter("stackTrace", ERROR_TRANSACTION.getStackTrace())
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .addParameter("ilmDt", Timestamp.valueOf(STARTED_AT))
                .executeScalar(Long.TYPE));
  }
}
