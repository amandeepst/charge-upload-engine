package com.worldpay.pms.cue.engine.integration;

import static com.typesafe.config.ConfigFactory.load;
import static com.worldpay.pms.cue.engine.integration.Constants.OUTPUT_TABLES;
import static java.lang.String.format;

import com.worldpay.pms.cue.domain.ChargingService;
import com.worldpay.pms.cue.engine.batch.ChargingBatchRunResult;
import com.worldpay.pms.cue.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.JSONUtils;
import com.worldpay.pms.spark.core.SparkApp;
import com.worldpay.pms.spark.core.batch.BatchMetadata;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.stereotypes.WithSpark;
import io.vavr.collection.List;
import org.sql2o.Query;
import org.sql2o.Sql2oQuery;
import org.sql2o.data.Row;
import org.sql2o.data.Table;

@WithSpark
public class EndToEndTestBase implements WithDatabase {

  /** Charging Upload db where for input and output */
  protected SqlDb cue;

  protected SqlDb cisadmDb;

  @Override
  public void bindChargingJdbcConfiguration(JdbcConfiguration conf) {
    cue = SqlDb.simple(conf);
    cue.exec(
        "clear tables - user",
        conn -> {
          OUTPUT_TABLES.forEach(
              out -> {
                try (Query q = conn.createQuery(format("DELETE %s", out))) {
                  q.executeUpdate();
                }
              });
          return true;
        });
  }

  @Override
  public void bindOrmbJdbcConfiguration(JdbcConfiguration conf) {
    cisadmDb = SqlDb.simple(conf);
    cisadmDb.execQuery("clear input table", "delete from  cm_bchg_stg", Query::executeUpdate);
    cisadmDb.execQuery("clear input table", "delete from  ci_bill_cyc_sch", Query::executeUpdate);
  }

  //
  // helper methods
  //
  protected List<Row> readErrorView(String where) {
    return List.ofAll(readTable("vw_error_transaction", where).rows());
  }

  protected List<Row> readErrors(String where) {
    return List.ofAll(readTable("error_transaction", where).rows());
  }

  protected List<Row> readBillItems(String where) {
    return List.ofAll(readTable("cm_misc_bill_item", where).rows());
  }

  protected List<Row> readBillItemLines(String where) {
    return List.ofAll(readTable("cm_misc_bill_item_ln", where).rows());
  }

  protected List<Row> readRecurringCharges(String where) {
    return List.ofAll(readTable("cm_rec_chg", where).rows());
  }

  protected List<Row> readRecurringChargesForAudit(String where) {
    return List.ofAll(readTable("cm_rec_chg_audit", where).rows());
  }

  protected List<Row> readBillableChargeFromCISADM(String where) {
    return List.ofAll(readTableFromCISADM("cm_bill_chg", where).rows());
  }

  protected List<Row> readBillableChargeLinesFromCISADM(String where) {
    return List.ofAll(readTableFromCISADM("cm_b_chg_line", where).rows());
  }

  protected List<Row> readBillableChargeAttibutesFromCISADM(String where) {
    return List.ofAll(readTableFromCISADM("cm_bchg_attributes_map", where).rows());
  }

  protected List<Row> readBillableChargeCharFromCISADM(String where) {
    return List.ofAll(readTableFromCISADM("cm_bill_chg_char", where).rows());
  }

  protected List<Row> readBillableChargeLineCharFromCISADM(String where) {
    return List.ofAll(readTableFromCISADM("cm_b_ln_char", where).rows());
  }

  protected ChargingBatchRunResult readLastSuccessfulBatch() {
    BatchMetadata<ChargingBatchRunResult> metadata =
        JSONUtils.unmarshal(
            readTable(
                    "batch_history",
                    "state = 'COMPLETED' ORDER BY created_at DESC FETCH FIRST 1 ROWS ONLY")
                .rows()
                .get(0)
                .getString("metadata"),
            tf ->
                tf.constructParametrizedType(
                    BatchMetadata.class, BatchMetadata.class, ChargingBatchRunResult.class));

    return metadata.getBatchRunResult();
  }

  protected static void run(ChargingService domain, String... args) {
    SparkApp.run(cfg -> new MockedDomainChargingApp(load("application-test.conf"), domain), args);
  }

  protected Table readTable(String tableName, String where) {
    return cue.execQuery(
        tableName,
        format("SELECT * FROM %s WHERE %s", tableName, where == null ? "1=1" : where),
        Sql2oQuery::executeAndFetchTable);
  }

  protected Table readTableFromCISADM(String tableName, String where) {
    return cisadmDb.execQuery(
        tableName,
        format("SELECT * FROM %s WHERE %s", tableName, where == null ? "1=1" : where),
        Sql2oQuery::executeAndFetchTable);
  }
}
