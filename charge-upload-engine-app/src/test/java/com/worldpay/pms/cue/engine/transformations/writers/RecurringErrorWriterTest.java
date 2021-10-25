package com.worldpay.pms.cue.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static org.apache.spark.sql.SparkSession.getActiveSession;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.cue.domain.common.AccountType;
import com.worldpay.pms.cue.engine.encoder.Encoders;
import com.worldpay.pms.cue.engine.recurring.RecurringErrorTransaction;
import com.worldpay.pms.cue.engine.recurring.RecurringErrorWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Encoder;

public class RecurringErrorWriterTest extends ChargingJdbcWritersTest<RecurringErrorTransaction> {

  private static final String QUERY_COUNT_RECURRING_CHARGE_ERROR_BY_ALL_FIELDS =
      resourceAsString("sql/count_rec_error_transaction_by_all_fields.sql");

  private static final String TX_HEADER_ID_1 = "txHeaderId-1";
  private static final String QUERY_DELETE_ERROR_TRANSACTION_BY_ID =
      resourceAsString("sql/delete_rec_error_transaction_by_id.sql");

  private static final RecurringErrorTransaction ERROR_TRANSACTION =
      new RecurringErrorTransaction(
          TX_HEADER_ID_1,
          "perIdNbr",
          AccountType.CHARGING.code,
          "P01101",
          "0001",
          "PARTY",
          "RECR",
          "WPMO",
          "GBP",
          BigDecimal.ONE,
          1,
          Timestamp.valueOf("2020-08-10 00:00:00"),
          null,
          "ACTIVE",
          "12345",
          Timestamp.valueOf("2020-08-10 00:00:00"),
          Date.valueOf("2020-08-10"),
          1,
          "test-code1",
          "test-message1",
          "stackTrace");

  @Override
  protected void init() {
    db.execQuery(
        "delete-rec-error-transaction",
        QUERY_DELETE_ERROR_TRANSACTION_BY_ID,
        query -> query.addParameter("txnHeaderId", TX_HEADER_ID_1).executeUpdate());
  }

  @Override
  protected void assertNoRowsWritten() {
    assertThat(countPendingRecurringChargeError(TX_HEADER_ID_1, AccountType.CHARGING.code))
        .isZero();
  }

  @Override
  protected void assertRowsWritten() {
    assertThat(countErrorTransaction()).isEqualTo(1);
  }

  @Override
  protected List<RecurringErrorTransaction> provideSamples() {
    return Collections.unmodifiableList(Collections.singletonList(ERROR_TRANSACTION));
  }

  @Override
  protected Encoder<RecurringErrorTransaction> encoder() {
    return Encoders.RECURRING_ERROR_TRANSACTION_ENCODER;
  }

  @Override
  protected JdbcWriter<RecurringErrorTransaction> createWriter(JdbcWriterConfiguration conf) {
    return new RecurringErrorWriter(conf, getActiveSession().get());
  }

  private long countPendingRecurringChargeError(String txnHeaderId, String subAccountType) {
    return db.execQuery(
        "count-rec-charge-error",
        "select count(*) from cm_rec_chg_err where trim(txn_header_id) = :txnHeaderId and trim(SUB_ACCT) = :subAccountType",
        query ->
            query
                .addParameter("txnHeaderId", txnHeaderId)
                .addParameter("subAccountType", subAccountType)
                .executeScalar(Long.TYPE));
  }

  private long countErrorTransaction() {
    return db.execQuery(
        "count-rec-error-transaction-by-all-fields",
        QUERY_COUNT_RECURRING_CHARGE_ERROR_BY_ALL_FIELDS,
        query ->
            query
                .addParameter("txnHeaderId", ERROR_TRANSACTION.getTxnHeaderId())
                .addParameter("perIdNbr", ERROR_TRANSACTION.getPartyId())
                .addParameter("subAccountType", ERROR_TRANSACTION.getSubAccount())
                .addParameter("retryCount", ERROR_TRANSACTION.getRetryCount())
                .addParameter("reason", ERROR_TRANSACTION.getMessage())
                .addParameter("stackTrace", ERROR_TRANSACTION.getStackTrace())
                .addParameter("firstFailureAt", Date.valueOf(STARTED_AT.toLocalDate()))
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .addParameter("ilmDt", Timestamp.valueOf(STARTED_AT))
                .executeScalar(Long.TYPE));
  }
}
