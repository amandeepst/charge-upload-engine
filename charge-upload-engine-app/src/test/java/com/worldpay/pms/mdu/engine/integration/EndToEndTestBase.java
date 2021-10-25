package com.worldpay.pms.mdu.engine.integration;

import static com.typesafe.config.ConfigFactory.load;
import static com.worldpay.pms.mdu.engine.integration.Constants.OUTPUT_TABLES;
import static java.lang.String.format;

import com.worldpay.pms.mdu.engine.DummyFactory;
import com.worldpay.pms.mdu.engine.data.StaticDataRepository;
import com.worldpay.pms.mdu.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.SparkApp;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import io.vavr.collection.List;
import org.sql2o.Query;
import org.sql2o.Sql2oQuery;
import org.sql2o.data.Row;
import org.sql2o.data.Table;

public class EndToEndTestBase implements WithDatabase {
  SqlDb mdu;

  @Override
  public void bindMerchantUploadJdbcConfiguration(JdbcConfiguration conf) {
    mdu = SqlDb.simple(conf);
    mdu.exec(
        "clear output tables",
        conn -> {
          OUTPUT_TABLES.forEach(
              table -> {
                try (Query q = conn.createQuery("DELETE FROM " + table)) {
                  q.executeUpdate();
                }
              });
          return true;
        });
  }

  public static void run(StaticDataRepository staticRepository, String... args) {
    SparkApp.run(
        cfg ->
            new MerchantUploadApp(
                load("application-test.conf"), new DummyFactory<>(staticRepository)),
        args);
  }

  protected List<Row> readErrors(String where) {
    return List.ofAll(readTable("error_transaction", where).rows());
  }

  protected List<Row> readParty(String where) {
    return List.ofAll(readTable("vw_party", where).rows());
  }

  protected List<Row> readAccount(String where) {
    return List.ofAll(readTable("vw_acct", where).rows());
  }

  protected List<Row> readSubAccount(String where) {
    return List.ofAll(readTable("vw_sub_acct", where).rows());
  }

  protected List<Row> readWithholdFunds(String where) {
    return List.ofAll(readTable("vw_withhold_funds", where).rows());
  }

  protected List<Row> readAccountHierarchy(String where) {
    return List.ofAll(readTable("vw_acct_hier", where).rows());
  }

  protected List<Row> readAccountHierarchyErrors(String where) {
    return List.ofAll(readTable("vw_error_hier", where).rows());
  }

  protected Table readTable(String tableName, String where) {
    return mdu.execQuery(
        tableName,
        format("SELECT * FROM %s WHERE %s", tableName, where == null ? "1=1" : where),
        Sql2oQuery::executeAndFetchTable);
  }
}
