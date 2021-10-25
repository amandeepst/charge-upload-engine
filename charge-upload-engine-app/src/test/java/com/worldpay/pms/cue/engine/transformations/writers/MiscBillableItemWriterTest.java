package com.worldpay.pms.cue.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.cue.engine.encoder.Encoders;
import com.worldpay.pms.cue.engine.mbi.MiscBillableItem;
import com.worldpay.pms.cue.engine.mbi.MiscBillableItemLineRow;
import com.worldpay.pms.cue.engine.mbi.MiscBillableItemRow;
import com.worldpay.pms.cue.engine.mbi.MiscBillableItemWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.Encoder;
import scala.Tuple2;

class MiscBillableItemWriterTest extends ChargingJdbcWritersTest<Tuple2<String, MiscBillableItem>> {

  private static final String BILLABLE_ITEM_LINE_ID = "lineId-1";
  private static final String TXN_HEADER_ID = "txnId-1";
  private static final String QUERY_COUNT_MISC_BILLABLE_ITEM_BY_ALL_FIELDS =
      resourceAsString("sql/count_misc_billable_item_by_all_fields.sql");
  private static final String QUERY_COUNT_MISC_BILLABLE_ITEM_LINE_BY_ALL_FIELDS =
      resourceAsString("sql/count_misc_billable_item_line_by_all_fields.sql");
  private static final String BILLABLE_ITEM_ID = "id-1";
  private static final MiscBillableItemRow MISC_BILL_ITEM_ROW = newMiscBillableItemRow();
  private static final MiscBillableItemLineRow MISC_BILL_ITEM_LN_ROW = newMiscBillableItemLineRow();

  private static MiscBillableItemRow newMiscBillableItemRow() {
    return new MiscBillableItemRow(
        TXN_HEADER_ID,
        "partyId",
        "lcp",
        "00001",
        "CHRG",
        "acctId",
        "saId",
        "GBP",
        "UPLD",
        "productId",
        "paymentNarrative",
        "sourceType",
        "sourceId",
        "eventId",
        1,
        "Y",
        "Y",
        "Y",
        "Y",
        "Y",
        "frequencyId",
        "caseIdentifier",
        Timestamp.valueOf("2020-08-10 00:00:00"),
        Timestamp.valueOf("2020-08-10 00:00:00"),
        Timestamp.valueOf("2020-08-10 00:00:00"),
        "N",
        "YN123");
  }

  private static MiscBillableItemLineRow newMiscBillableItemLineRow() {
    return new MiscBillableItemLineRow(
        BILLABLE_ITEM_LINE_ID,
        "lct",
        BigDecimal.valueOf(11.994),
        BigDecimal.valueOf(11.994),
        "GBP",
        1);
  }

  @Override
  protected void init() {
    db.execQuery(
        "delete-misc-billable-item",
        "delete from cm_misc_bill_item where trim(misc_bill_item_id) = :id",
        query -> query.addParameter("id", BILLABLE_ITEM_ID).executeUpdate());

    db.execQuery(
        "delete-misc-billable-item-line",
        "delete from cm_misc_bill_item_ln where trim(bill_item_line_id) = :id",
        query -> query.addParameter("id", BILLABLE_ITEM_LINE_ID).executeUpdate());
  }

  @Override
  protected void assertRowsWritten() {
    assertThat(countMiscBillableItemByAllFields()).describedAs("item").isOne();
    assertThat(countMiscBillableItemLineByAllFields()).describedAs("item-ln").isOne();
  }

  @Override
  protected void assertNoRowsWritten() {
    assertThat(countMiscBillableItem()).isZero();
    assertThat(countMiscBillableItemLine()).isZero();
  }

  @Override
  protected List<Tuple2<String, MiscBillableItem>> provideSamples() {
    return Collections.singletonList(
        Tuple2.apply(
            BILLABLE_ITEM_ID, new MiscBillableItem(MISC_BILL_ITEM_ROW, MISC_BILL_ITEM_LN_ROW)));
  }

  @Override
  protected Encoder<Tuple2<String, MiscBillableItem>> encoder() {
    return Encoders.MISC_BILLABLE_ITEM_WITH_ID_ENCODER;
  }

  @Override
  protected JdbcWriter<Tuple2<String, MiscBillableItem>> createWriter(
      JdbcWriterConfiguration conf) {
    return new MiscBillableItemWriter(conf, Collections.emptyList(), Collections.emptyList());
  }

  private long countMiscBillableItem() {
    return db.execQuery(
        "count-misc-billable-item",
        "select count(*) from cm_misc_bill_item where trim(misc_bill_item_id) = :id",
        query -> query.addParameter("id", BILLABLE_ITEM_ID).executeScalar(Long.TYPE));
  }

  private long countMiscBillableItemLine() {
    return db.execQuery(
        "count-misc-billable-item-line",
        "select count(*) from cm_misc_bill_item_ln where trim(bill_item_line_id) = :id",
        query -> query.addParameter("id", BILLABLE_ITEM_LINE_ID).executeScalar(Long.TYPE));
  }

  private long countMiscBillableItemByAllFields() {
    return db.execQuery(
        "count-misc-billable-item-by-all-fields",
        QUERY_COUNT_MISC_BILLABLE_ITEM_BY_ALL_FIELDS,
        query ->
            query
                .addParameter("billableItemId", BILLABLE_ITEM_ID)
                .addParameter("txnHeaderId", TXN_HEADER_ID)
                .addParameter("partyId", MISC_BILL_ITEM_ROW.getPartyId())
                .addParameter("legalCounterparty", MISC_BILL_ITEM_ROW.getLegalCounterparty())
                .addParameter("division", MISC_BILL_ITEM_ROW.getDivision())
                .addParameter("subAccountType", MISC_BILL_ITEM_ROW.getSubAccount())
                .addParameter("accountId", MISC_BILL_ITEM_ROW.getAccountId())
                .addParameter("subAccountId", MISC_BILL_ITEM_ROW.getSubAccountId())
                .addParameter("currency", MISC_BILL_ITEM_ROW.getCurrency())
                .addParameter("status", MISC_BILL_ITEM_ROW.getStatus())
                .addParameter("productIdentifier", MISC_BILL_ITEM_ROW.getProductIdentifier())
                .addParameter("adhocBillIndicator", MISC_BILL_ITEM_ROW.getAdhocBillIndicator())
                .addParameter("quantity", MISC_BILL_ITEM_ROW.getQuantity())
                .addParameter(
                    "fastestSettlementIndicator",
                    MISC_BILL_ITEM_ROW.getFastestSettlementIndicator())
                .addParameter(
                    "individualPaymentIndicator",
                    MISC_BILL_ITEM_ROW.getIndividualPaymentIndicator())
                .addParameter("paymentNarrative", MISC_BILL_ITEM_ROW.getPaymentNarrative())
                .addParameter(
                    "releaseReserverIndicator", MISC_BILL_ITEM_ROW.getReleaseReserverIndicator())
                .addParameter("releaseWafIndicator", MISC_BILL_ITEM_ROW.getReleaseWafIndicator())
                .addParameter("caseIdentifier", MISC_BILL_ITEM_ROW.getCaseIdentifier())
                .addParameter("debtDate", MISC_BILL_ITEM_ROW.getDebtDate())
                .addParameter("sourceType", MISC_BILL_ITEM_ROW.getSourceType())
                .addParameter("sourceId", MISC_BILL_ITEM_ROW.getSourceId())
                .addParameter("eventId", MISC_BILL_ITEM_ROW.getEventId())
                .addParameter("frequencyIdentifier", MISC_BILL_ITEM_ROW.getFrequencyIdentifier())
                .addParameter("cutOffDate", MISC_BILL_ITEM_ROW.getCutOffDate())
                .addParameter("accruedDate", MISC_BILL_ITEM_ROW.getAccruedDate())
                .addParameter("ilmArchiveSwitch", "Y")
                .addParameter("ilmDateTime", Timestamp.valueOf(STARTED_AT))
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .addParameter("partitionId", TaskContext.getPartitionId())
                .addParameter("caseFlag", MISC_BILL_ITEM_ROW.getCaseFlag())
                .addParameter("hashString", MISC_BILL_ITEM_ROW.getHashString())
                .executeScalar(Long.TYPE));
  }

  private long countMiscBillableItemLineByAllFields() {
    return db.execQuery(
        "count-misc-billable-item-line-by-all-fields",
        QUERY_COUNT_MISC_BILLABLE_ITEM_LINE_BY_ALL_FIELDS,
        query ->
            query
                .addParameter("billableItemLineId", MISC_BILL_ITEM_LN_ROW.getBillableItemLineId())
                .addParameter("billableItemId", BILLABLE_ITEM_ID)
                .addParameter("lineCalculationType", MISC_BILL_ITEM_LN_ROW.getLineCalculationType())
                .addParameter("currency", MISC_BILL_ITEM_LN_ROW.getCurrency())
                .addParameter("lineAmount", BigDecimal.valueOf(11.99))
                .addParameter("price", BigDecimal.valueOf(11.994))
                .addParameter("quantity", MISC_BILL_ITEM_LN_ROW.getQuantity())
                .addParameter("ilmDateTime", Timestamp.valueOf(STARTED_AT))
                .addParameter("ilmArchiveSwitch", "Y")
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .addParameter("partitionId", TaskContext.getPartitionId())
                .executeScalar(Long.TYPE));
  }
}
