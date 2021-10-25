package com.worldpay.pms.cue.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.cue.engine.encoder.Encoders;
import com.worldpay.pms.cue.engine.recurring.RecurringChargeAuditWriter;
import com.worldpay.pms.cue.engine.recurring.RecurringChargeRow;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Encoder;

public class RecurringChargeAuditWriterTest extends ChargingJdbcWritersTest<RecurringChargeRow> {

  private static final String QUERY_COUNT_RECURRING_CHARGE_BY_ALL_FIELDS =
      resourceAsString("sql/count_recurring_charge_audit_by_all_fields.sql");

  private static final String RECURRING_CHARGE_ID_1 = "id-1";
  private static final String RECURRING_CHARGE_ID_2 = "id-2";
  private static final String TXN_HEADER_ID_1 = "txnId-1";
  private static final String TXN_HEADER_ID_2 = "txnId-2";

  private static final RecurringChargeRow RECURRING_CHARGE_ROW_INSERTION =
      new RecurringChargeRow(
          RECURRING_CHARGE_ID_1,
          TXN_HEADER_ID_1,
          "productIdentifier",
          "legalCounterparty",
          "00001",
          "partyIdentifier",
          "RECR",
          "frequencyId",
          "GBP",
          BigDecimal.ONE,
          1,
          Timestamp.valueOf("2020-08-10 00:00:00"),
          Timestamp.valueOf("2020-08-11 00:00:00"),
          "status",
          "sourceId",
          null);

  private static final RecurringChargeRow RECURRING_CHARGE_ROW_INSERTION_2 =
      new RecurringChargeRow(
          RECURRING_CHARGE_ID_2,
          TXN_HEADER_ID_2,
          "productIdentifier",
          "legalCounterparty",
          "00001",
          "partyIdentifier",
          "RECR",
          "frequencyId",
          "GBP",
          BigDecimal.ONE,
          1,
          Timestamp.valueOf("2020-08-10 00:00:00"),
          Timestamp.valueOf("2020-08-11 00:00:00"),
          "status",
          "sourceId",
          null);

  @Override
  protected void init() {
    db.execQuery(
        "delete-recurring-charge-audit",
        "delete from cm_rec_chg_audit where rec_chg_id in (:p1,:p2)",
        query -> query.withParams(RECURRING_CHARGE_ID_1, RECURRING_CHARGE_ID_2).executeUpdate());
  }

  @Override
  protected void assertRowsWritten() {
    assertThat(countRecurringChargeByAllFields(RECURRING_CHARGE_ROW_INSERTION)).isOne();
    assertThat(countRecurringChargeByAllFields(RECURRING_CHARGE_ROW_INSERTION_2)).isOne();
  }

  @Override
  protected void assertNoRowsWritten() {
    assertThat(countRecurringCharge()).isZero();
  }

  @Override
  protected List<RecurringChargeRow> provideSamples() {
    return Collections.unmodifiableList(
        Arrays.asList(RECURRING_CHARGE_ROW_INSERTION, RECURRING_CHARGE_ROW_INSERTION_2));
  }

  @Override
  protected Encoder<RecurringChargeRow> encoder() {
    return Encoders.RECURRING_CHARGE_ROW_ENCODER;
  }

  @Override
  protected JdbcWriter<RecurringChargeRow> createWriter(JdbcWriterConfiguration conf) {
    return new RecurringChargeAuditWriter(conf);
  }

  private long countRecurringCharge() {
    return db.execQuery(
        "count-recurring-charge-audit",
        "select count(*) from cm_rec_chg_audit where rec_chg_id in (:p1,:p2)",
        query ->
            query
                .withParams(RECURRING_CHARGE_ID_1, RECURRING_CHARGE_ID_2)
                .executeScalar(Long.TYPE));
  }

  private long countRecurringChargeByAllFields(RecurringChargeRow recurringRow) {
    return db.execQuery(
        "count-recurring-charge-audit-by-all-fields",
        QUERY_COUNT_RECURRING_CHARGE_BY_ALL_FIELDS,
        query ->
            query
                .addParameter(
                    "recurringChargeIdentifier", recurringRow.getRecurringChargeIdentifier())
                .addParameter("txnHeaderId", recurringRow.getTxnHeaderId())
                .addParameter("productIdentifier", recurringRow.getProductIdentifier())
                .addParameter("legalCounterparty", recurringRow.getLegalCounterparty())
                .addParameter("division", recurringRow.getDivision())
                .addParameter("partyIdentifier", recurringRow.getPartyIdentifier())
                .addParameter("subAccountType", recurringRow.getSubAccount())
                .addParameter("frequencyIdentifier", recurringRow.getFrequencyIdentifier())
                .addParameter("currency", recurringRow.getCurrency())
                .addParameter("price", recurringRow.getPrice())
                .addParameter("quantity", recurringRow.getQuantity())
                .addParameter("validFrom", recurringRow.getValidFrom())
                .addParameter("validTo", recurringRow.getValidTo())
                .addParameter("status", recurringRow.getStatus())
                .addParameter("sourceId", recurringRow.getSourceId())
                .addParameter("ilmDateTime", Timestamp.valueOf(STARTED_AT))
                .addParameter("ilmArchiveSwitch", "Y")
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .executeScalar(Long.TYPE));
  }
}
