package com.worldpay.pms.cue.engine.spark.data;

import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.cue.domain.Product;
import com.worldpay.pms.cue.engine.ChargingUploadConfig;
import com.worldpay.pms.cue.engine.staticdata.JdbcStaticDataRepository;
import com.worldpay.pms.cue.engine.utils.DatabaseCsvUtils.Database;
import com.worldpay.pms.cue.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JdbcStaticDataRepositoryTest implements WithDatabase {

  private Database cisadm;
  private JdbcStaticDataRepository target;
  private static final String ROOT = "input/JdbcStaticDataRepositoryTest/";

  @Override
  public void bindChargingConfiguration(ChargingUploadConfig settings) {
    this.target = new JdbcStaticDataRepository(settings.getSources().getStaticData());
  }

  @Override
  public void bindOrmbJdbcConfiguration(JdbcConfiguration conf) {
    this.cisadm = new Database(conf, ROOT);
  }

  @Test
  void canLoadBillPeriodCodes() {
    cisadm.run(
        () -> {
          Set<String> billPeriodCodes = target.getBillPeriodCodes();
          assertThat(billPeriodCodes.size()).isPositive();
          assertThat(billPeriodCodes).doesNotContainNull();
          assertThat(billPeriodCodes).contains("WPDY", "WPMO");
        },
        "ci_bill_period");
  }

  @Test
  void canLoadChargeTypes() {
    cisadm.run(
        () -> {
          Set<String> chargeTypes = target.getChargeTypes();
          assertThat(chargeTypes.size()).isPositive();
          assertThat(chargeTypes).doesNotContainNull();
          assertThat(chargeTypes).contains("RECURCHG");
        },
        "ci_chg_type");
  }

  @Test
  void canLoadCurrencyCodes() {
    cisadm.run(
        () -> {
          Set<String> currencyCodes = target.getCurrencyCodes();
          assertThat(currencyCodes.size()).isPositive();
          assertThat(currencyCodes).doesNotContainNull();
          assertThat(currencyCodes).contains("KMF", "CUP");
        },
        "ci_currency_cd");
  }

  @Test
  void canLoadPriceItems() {
    cisadm.run(
        () -> {
          Set<String> priceItems = target.getPriceItems();
          assertThat(priceItems.size()).isPositive();
          assertThat(priceItems).doesNotContainNull();
          assertThat(priceItems).contains("MADH00JF", "MADH0210", "MAH40102");
        },
        "ci_priceitem");
  }

  @Test
  void canLoadSubAccountTypes() {
    cisadm.run(
        () -> {
          Set<String> subAccountTypes = target.getSubAccountTypes();
          assertThat(subAccountTypes.size()).isPositive();
          assertThat(subAccountTypes).doesNotContainNull();
          assertThat(subAccountTypes).contains("CHRG", "FUND", "RECR", "CHBK");
        },
        "ci_sa_type");
  }

  @Test
  void canLoadProduct() {
    cisadm.run(
        () -> {
          Set<Product> productClass = target.getProduct();
          assertThat(productClass.size()).isPositive();
          assertThat(productClass).doesNotContainNull();
          assertThat(productClass).contains(new Product("MIGCHRG", "MIGRATION"));
        },
        "ci_priceitem_char");
  }

  @Test
  void canLoadProductCategories() {
    cisadm.run(
        () -> {
          Set<String> productCategory = target.getProductCategory();
          assertThat(productCategory.size()).isPositive();
          assertThat(productCategory).doesNotContainNull();
          assertThat(productCategory).contains("MIGRATION");
        },
        "ci_priceitem_char");
  }
}
