package com.worldpay.pms.cue.engine.mbi;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.SparkUtils.timed;

import com.worldpay.pms.cue.domain.Currency;
import com.worldpay.pms.cue.domain.Product;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.UUID;
import org.sql2o.Connection;
import org.sql2o.Query;
import scala.Tuple2;

public class MiscBillableItemWriter extends JdbcWriter<Tuple2<String, MiscBillableItem>> {

  public static final RoundingMode DEFAULT_ROUNDING_MODE = RoundingMode.HALF_UP;
  private static final short CHARGE_AMOUNT_SCALE = 2;

  private final Map<String, Currency> currencies;
  private final Map<String, Product> products;

  public MiscBillableItemWriter(
      JdbcWriterConfiguration conf, Iterable<Currency> currencies, Iterable<Product> products) {
    super(conf);
    this.currencies =
        Stream.ofAll(currencies)
            .map(currency -> new io.vavr.Tuple2<>(currency.getCurrencyCode(), currency))
            .collect(HashMap.collector());
    this.products =
        Stream.ofAll(products)
            .map(product -> new io.vavr.Tuple2<>(product.getProductCode(), product))
            .collect(HashMap.collector());
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<Tuple2<String, MiscBillableItem>> writer(
      BatchId batchId, Timestamp startedAt, JdbcWriterConfiguration conf) {
    return new Writer(batchId, startedAt, conf, this.currencies, this.products);
  }

  private static class Writer
      extends JdbcBatchPartitionWriterFunction<Tuple2<String, MiscBillableItem>> {

    private final Map<String, Currency> currencies;
    private final Map<String, Product> products;

    public Writer(
        BatchId batchCode,
        Timestamp batchStartedAt,
        JdbcWriterConfiguration conf,
        Map<String, Currency> currencies,
        Map<String, Product> products) {
      super(batchCode, batchStartedAt, conf);
      this.currencies = currencies;
      this.products = products;
    }

    @Override
    protected String name() {
      return "misc-billable-item";
    }

    @Override
    protected void write(Connection conn, Iterator<Tuple2<String, MiscBillableItem>> partition) {
      try (Query insertItem =
              createQueryWithDefaults(
                  conn, resourceAsString("sql/outputs/insert-misc-billable-item.sql"));
          Query insertItemLine =
              createQueryWithDefaults(
                  conn, resourceAsString("sql/outputs/insert-misc-billable-item-line.sql"))) {
        bindAndExecute(partition, insertItem, insertItemLine, currencies, products);
      }
    }

    private static void bindAndExecute(
        Iterator<Tuple2<String, MiscBillableItem>> partition,
        Query stmt,
        Query stmtLine,
        Map<String, Currency> currencies,
        Map<String, Product> products) {

      partition.forEachRemaining(
          row -> {
            String billItemId = row._1;
            MiscBillableItem billItemRow = row._2;
            MiscBillableItemRow itemRow = billItemRow.getMiscBillableItemRow();
            MiscBillableItemLineRow itemLineRow = billItemRow.getMiscBillableItemLineRow();
            String billableItemId = UUID.randomUUID().toString();
            BigDecimal amount =
                itemLineRow
                    .getLineAmount()
                    .setScale(
                        getCurrencyRounding(itemLineRow.getCurrency(), currencies),
                        DEFAULT_ROUNDING_MODE);
            String productCategory =
                products
                    .get(itemRow.getProductIdentifier())
                    .map(Product::getProductCategory)
                    .getOrElse(Product.NONE.getProductCategory());

            bindAndAddMiscItem(billItemId, billableItemId, amount, productCategory, itemRow, stmt);
            bindAndAddMiscItemLine(billItemId, billableItemId, amount, itemLineRow, stmtLine);
          });

      timed("insert-misc-billable-item", stmt::executeBatch);
      timed("insert-misc-billable-item-line", stmtLine::executeBatch);
    }

    private static short getCurrencyRounding(String currency, Map<String, Currency> currencies) {
      return (short)
          Math.min(
              CHARGE_AMOUNT_SCALE,
              currencies
                  .get(currency)
                  .map(Currency::getRoundingScale)
                  .getOrElse(CHARGE_AMOUNT_SCALE));
    }

    private static void bindAndAddMiscItemLine(
        String billItemId,
        String billableItemId,
        BigDecimal amount,
        MiscBillableItemLineRow miscItemLineRow,
        Query stmtLine) {

      stmtLine
          .addParameter("billItemLineId", miscItemLineRow.getBillableItemLineId())
          .addParameter("billItemId", billItemId)
          .addParameter("billableItemId", billableItemId)
          .addParameter("currency", miscItemLineRow.getCurrency())
          .addParameter("lineCalculationType", miscItemLineRow.getLineCalculationType())
          .addParameter("lineAmount", amount)
          .addParameter("price", miscItemLineRow.getPrice())
          .addParameter("quantity", miscItemLineRow.getQuantity())
          .addToBatch();
    }

    private static void bindAndAddMiscItem(
        String billItemId,
        String billableItemId,
        BigDecimal amount,
        String productCategory,
        MiscBillableItemRow miscItemRow,
        Query stmt) {
      stmt.addParameter("billItemId", billItemId)
          .addParameter("billableItemId", billableItemId)
          .addParameter("txnHeaderId", miscItemRow.getTxnHeaderId())
          .addParameter("accountId", miscItemRow.getAccountId())
          .addParameter("subAccountId", miscItemRow.getSubAccountId())
          .addParameter("partyId", miscItemRow.getPartyId())
          .addParameter("legalCounterparty", miscItemRow.getLegalCounterparty())
          .addParameter("subAccountType", miscItemRow.getSubAccount())
          .addParameter("division", miscItemRow.getDivision())
          .addParameter("currency", miscItemRow.getCurrency())
          .addParameter("status", miscItemRow.getStatus())
          .addParameter("productIdentifier", miscItemRow.getProductIdentifier())
          .addParameter("productCategory", productCategory)
          .addParameter("adhocBillIndicator", miscItemRow.getAdhocBillIndicator())
          .addParameter("quantity", miscItemRow.getQuantity())
          .addParameter("fastestSettlementIndicator", miscItemRow.getFastestSettlementIndicator())
          .addParameter("individualPaymentIndicator", miscItemRow.getIndividualPaymentIndicator())
          .addParameter("paymentNarrative", miscItemRow.getPaymentNarrative())
          .addParameter("releaseReserverIndicator", miscItemRow.getReleaseReserverIndicator())
          .addParameter("releaseWafIndicator", miscItemRow.getReleaseWafIndicator())
          .addParameter("caseIdentifier", miscItemRow.getCaseIdentifier())
          .addParameter("debtDate", miscItemRow.getDebtDate())
          .addParameter("sourceType", miscItemRow.getSourceType())
          .addParameter("sourceId", miscItemRow.getSourceId())
          .addParameter("eventId", miscItemRow.getEventId())
          .addParameter("frequencyIdentifier", miscItemRow.getFrequencyIdentifier())
          .addParameter("cutOffDate", miscItemRow.getCutOffDate())
          .addParameter("accruedDate", miscItemRow.getAccruedDate())
          .addParameter("caseFlag", miscItemRow.getCaseFlag())
          .addParameter("amount", amount)
          .addParameter("hashString", miscItemRow.getHashString())
          .addToBatch();
    }
  }
}
