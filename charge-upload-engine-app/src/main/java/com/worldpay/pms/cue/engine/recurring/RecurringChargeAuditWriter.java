package com.worldpay.pms.cue.engine.recurring;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import org.sql2o.Query;

public class RecurringChargeAuditWriter extends JdbcWriter<RecurringChargeRow> {

  public RecurringChargeAuditWriter(JdbcWriterConfiguration conf) {
    super(conf);
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<RecurringChargeRow> writer(
      BatchId batchId, Timestamp startedAt, JdbcWriterConfiguration conf) {
    return new Writer(batchId, startedAt, conf);
  }

  private static class Writer extends JdbcBatchPartitionWriterFunction.Simple<RecurringChargeRow> {

    public Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf) {
      super(batchCode, batchStartedAt, conf);
    }

    @Override
    protected String name() {
      return "recurring-charge-audit";
    }

    @Override
    protected String getStatement() {
      return resourceAsString("sql/outputs/insert-recurring-charge-audit.sql");
    }

    @Override
    protected void bindAndAdd(RecurringChargeRow rcr, Query stmt) {
      stmt.addParameter("recurringChargeIdentifier", rcr.getRecurringChargeIdentifier())
          .addParameter("txnHeaderId", rcr.getTxnHeaderId())
          .addParameter("productIdentifier", rcr.getProductIdentifier())
          .addParameter("legalCounterparty", rcr.getLegalCounterparty())
          .addParameter("division", rcr.getDivision())
          .addParameter("partyIdentifier", rcr.getPartyIdentifier())
          .addParameter("subAccountType", rcr.getSubAccount())
          .addParameter("frequencyIdentifier", rcr.getFrequencyIdentifier())
          .addParameter("currency", rcr.getCurrency())
          .addParameter("price", rcr.getPrice())
          .addParameter("quantity", rcr.getQuantity())
          .addParameter("validFrom", rcr.getValidFrom())
          .addParameter("validTo", rcr.getValidTo())
          .addParameter("status", rcr.getStatus())
          .addParameter("sourceId", rcr.getSourceId())
          .addToBatch();
    }
  }
}
