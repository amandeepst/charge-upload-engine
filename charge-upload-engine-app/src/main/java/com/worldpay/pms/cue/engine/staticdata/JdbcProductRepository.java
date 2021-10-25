package com.worldpay.pms.cue.engine.staticdata;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.cue.domain.Product;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.repositories.Sql2oRepository;

public class JdbcProductRepository extends Sql2oRepository {

  public JdbcProductRepository(JdbcConfiguration conf) {
    super(conf);
  }

  public Iterable<Product> getProducts() {
    return db.execQuery(
        "get-all-product-categories",
        resourceAsString("sql/static/get-product-categories.sql"),
        query -> query.executeAndFetch(Product.class));
  }
}
