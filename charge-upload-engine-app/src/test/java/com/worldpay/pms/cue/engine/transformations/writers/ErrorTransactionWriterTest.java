package com.worldpay.pms.cue.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static org.apache.spark.sql.SparkSession.getActiveSession;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.cue.domain.common.AccountType;
import com.worldpay.pms.cue.engine.encoder.Encoders;
import com.worldpay.pms.cue.engine.pbc.ErrorTransaction;
import com.worldpay.pms.cue.engine.pbc.ErrorTransactionWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Encoder;

class ErrorTransactionWriterTest extends ChargingJdbcWritersTest<ErrorTransaction> {

  private static final String QUERY_COUNT_PENDING_BILLABLE_CHARGE_ERROR_BY_ALL_FIELDS =
      resourceAsString("sql/count_error_transaction_by_all_fields.sql");

  private static final String TX_HEADER_ID_1 = "txHeaderId-1";
  private static final String TX_HEADER_ID_2 = "txHeaderId-2";
  private static final String QUERY_DELETE_ERROR_TRANSACTION_BY_ID =
      resourceAsString("sql/delete_error_transaction_by_id.sql");

  private static final ErrorTransaction ERROR_TRANSACTION =
      new ErrorTransaction(
          TX_HEADER_ID_1,
          "perIdNbr",
          AccountType.FUNDING.code,
          null,
          1,
          "test-code1",
          "test-message1",
          "stackTrace");

  private static final ErrorTransaction RE_ERROR_TRANSACTION =
      new ErrorTransaction(
          TX_HEADER_ID_2,
          "perIdNbr",
          AccountType.CHARGING.code,
          Date.valueOf("2020-08-10"),
          2,
          "test-code2",
          "test-message2",
          null);

  @Override
  protected void init() {
    db.execQuery(
        "delete-error-transaction",
        QUERY_DELETE_ERROR_TRANSACTION_BY_ID,
        query -> query.addParameter("txnHeaderId", TX_HEADER_ID_1).executeUpdate());
    db.execQuery(
        "delete-error-transaction",
        QUERY_DELETE_ERROR_TRANSACTION_BY_ID,
        query -> query.addParameter("txnHeaderId", TX_HEADER_ID_2).executeUpdate());
  }

  @Override
  protected void assertNoRowsWritten() {
    assertThat(countPendingRecurringChargeError(TX_HEADER_ID_1, AccountType.FUNDING.code)).isZero();
    assertThat(countPendingRecurringChargeError(TX_HEADER_ID_2, AccountType.CHARGING.code))
        .isZero();
  }

  @Override
  protected void assertRowsWritten() {
    assertThat(countErrorTransaction()).isEqualTo(1);
    assertThat(countReErrorTransaction()).isEqualTo(1);
  }

  @Override
  protected List<ErrorTransaction> provideSamples() {
    return Collections.unmodifiableList(Arrays.asList(ERROR_TRANSACTION, RE_ERROR_TRANSACTION));
  }

  @Override
  protected Encoder<ErrorTransaction> encoder() {
    return Encoders.ERROR_TRANSACTION_ENCODER;
  }

  @Override
  protected JdbcWriter<ErrorTransaction> createWriter(JdbcWriterConfiguration conf) {
    return new ErrorTransactionWriter(conf, getActiveSession().get());
  }

  private long countPendingRecurringChargeError(String txnHeaderId, String subAccountType) {
    return db.execQuery(
        "count-pending-billable-charge-error",
        "select count(*) from error_transaction where trim(txn_header_id) = :txnHeaderId and trim(sa_type_cd) = :subAccountType",
        query ->
            query
                .addParameter("txnHeaderId", txnHeaderId)
                .addParameter("subAccountType", subAccountType)
                .executeScalar(Long.TYPE));
  }

  private long countErrorTransaction() {
    return db.execQuery(
        "count-error-transaction-by-all-fields",
        QUERY_COUNT_PENDING_BILLABLE_CHARGE_ERROR_BY_ALL_FIELDS,
        query ->
            query
                .addParameter("txnHeaderId", ERROR_TRANSACTION.getTxnHeaderId())
                .addParameter("perIdNbr", ERROR_TRANSACTION.getPerIdNbr())
                .addParameter("subAccountType", ERROR_TRANSACTION.getSubAccountType())
                .addParameter("retryCount", ERROR_TRANSACTION.getRetryCount())
                .addParameter("reason", ERROR_TRANSACTION.getMessage())
                .addParameter("stackTrace", ERROR_TRANSACTION.getStackTrace())
                .addParameter("firstFailureAt", Date.valueOf(STARTED_AT.toLocalDate()))
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .addParameter("ilmDt", Timestamp.valueOf(STARTED_AT))
                .executeScalar(Long.TYPE));
  }

  private long countReErrorTransaction() {
    return db.execQuery(
        "count-error-transaction-by-all-fields",
        QUERY_COUNT_PENDING_BILLABLE_CHARGE_ERROR_BY_ALL_FIELDS,
        query ->
            query
                .addParameter("txnHeaderId", RE_ERROR_TRANSACTION.getTxnHeaderId())
                .addParameter("perIdNbr", RE_ERROR_TRANSACTION.getPerIdNbr())
                .addParameter("subAccountType", RE_ERROR_TRANSACTION.getSubAccountType())
                .addParameter("retryCount", RE_ERROR_TRANSACTION.getRetryCount())
                .addParameter("reason", RE_ERROR_TRANSACTION.getMessage())
                .addParameter("stackTrace", RE_ERROR_TRANSACTION.getStackTrace())
                .addParameter("firstFailureAt", RE_ERROR_TRANSACTION.getFirstFailureAt())
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .addParameter("ilmDt", Timestamp.valueOf(STARTED_AT))
                .executeScalar(Long.TYPE));
  }
}
