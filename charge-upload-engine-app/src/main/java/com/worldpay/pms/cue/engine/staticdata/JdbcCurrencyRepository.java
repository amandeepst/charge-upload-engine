package com.worldpay.pms.cue.engine.staticdata;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.cue.domain.Currency;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.repositories.Sql2oRepository;

public class JdbcCurrencyRepository extends Sql2oRepository {

  public JdbcCurrencyRepository(JdbcConfiguration conf) {
    super(conf);
  }

  public Iterable<Currency> getCurrencies() {
    return db.execQuery(
        "get-all-currency",
        resourceAsString("sql/static/get-currency-codes.sql"),
        query -> query.executeAndFetch(Currency.class));
  }
}
