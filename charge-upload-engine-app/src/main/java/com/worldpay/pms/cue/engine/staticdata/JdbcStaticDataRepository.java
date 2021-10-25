package com.worldpay.pms.cue.engine.staticdata;

import static com.google.common.collect.Iterables.transform;
import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.google.common.collect.Sets;
import com.worldpay.pms.cue.domain.Currency;
import com.worldpay.pms.cue.domain.Product;
import com.worldpay.pms.cue.engine.ChargingUploadConfig.ChargingRepositoryConfiguration;
import com.worldpay.pms.cue.engine.common.Factory;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.repositories.Sql2oRepository;
import io.vavr.control.Try;
import java.util.HashSet;
import java.util.Set;

public class JdbcStaticDataRepository extends Sql2oRepository implements StaticDataRepository {

  private JdbcCurrencyRepository currencyRepository;
  private JdbcProductRepository productRepository;

  public JdbcStaticDataRepository(JdbcConfiguration conf) {
    super(conf);
  }

  public JdbcStaticDataRepository(ChargingRepositoryConfiguration conf) {
    super(conf.getConf());
    this.currencyRepository = new JdbcCurrencyRepository(conf.getConf());
    this.productRepository = new JdbcProductRepository(conf.getConf());
  }

  @Override
  public Set<String> getBillPeriodCodes() {
    return db.execQuery(
        "get-all-bill-period-codes",
        resourceAsString("sql/static/get-bill-period-codes.sql"),
        query -> hashSet(query.executeAndFetch(String.class)));
  }

  @Override
  public Set<String> getChargeTypes() {
    return db.execQuery(
        "get-all-charge-type",
        resourceAsString("sql/static/get-charge-types.sql"),
        query -> hashSet(query.executeAndFetch(String.class)));
  }

  @Override
  public Set<String> getCurrencyCodes() {
    return hashSet(transform(currencyRepository.getCurrencies(), Currency::getCurrencyCode));
  }

  @Override
  public Set<String> getPriceItems() {
    return db.execQuery(
        "get-all-priceitems",
        resourceAsString("sql/static/get-priceitems.sql"),
        query -> hashSet(query.executeAndFetch(String.class)));
  }

  @Override
  public Set<String> getSubAccountTypes() {
    return db.execQuery(
        "get-all-subaccount-types",
        resourceAsString("sql/static/get-subaccount-types.sql"),
        query -> hashSet(query.executeAndFetch(String.class)));
  }

  public Set<Product> getProduct() {
    return db.execQuery(
        "get-all-product-classes",
        resourceAsString("sql/static/get-product-categories.sql"),
        query -> hashSet(query.executeAndFetch(Product.class)));
  }

  public Set<String> getProductCategory() {
    return hashSet(transform(productRepository.getProducts(), Product::getProductCategory));
  }

  private <T> HashSet<T> hashSet(Iterable<T> items) {
    return Sets.newHashSet(items);
  }

  public static class RepositoryFactory implements Factory<StaticDataRepository> {

    private final ChargingRepositoryConfiguration conf;

    public RepositoryFactory(ChargingRepositoryConfiguration conf) {
      this.conf = conf;
    }

    @Override
    public Try<StaticDataRepository> build() {
      return Try.of(() -> new JdbcStaticDataRepository(conf));
    }
  }
}
