package com.worldpay.pms.mdu.engine.data;

import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.mdu.engine.MerchantUploadConfig;
import com.worldpay.pms.mdu.engine.utils.DatabaseCsvUtils.Database;
import com.worldpay.pms.mdu.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JdbcStaticRepositoryTest implements WithDatabaseAndSpark {
  private Database cisadm;
  private Database appuserDb;
  private JdbcStaticDataRepository target;
  private static final String ROOT = "input/JdbcStaticRepositoryTest/";

  @Override
  public void bindMerchantUploadConfiguration(MerchantUploadConfig settings) {
    this.target = new JdbcStaticDataRepository(settings.getSources().getStaticData());
  }

  @Override
  public void bindMerchantUploadJdbcConfiguration(JdbcConfiguration conf) {
    appuserDb = new Database(conf, ROOT);
  }

  @Override
  public void bindOrmbJdbcConfiguration(JdbcConfiguration conf) {
    this.cisadm = new Database(conf, ROOT);
  }

  @Test
  void canLoadBillCycleCode() {
    cisadm.run(
        () -> {
          Set<String> billCycleCode = target.getBillCycleCode();
          assertThat(billCycleCode).hasSizeGreaterThan(0);
          assertThat(billCycleCode).contains("MMO1", "WPDY", "YR02");
        },
        "ci_bill_cyc");
  }

  @Test
  void canLoadCountryCode() {
    cisadm.run(
        () -> {
          Set<String> countryCode = target.getCountryCode();
          assertThat(countryCode).hasSizeGreaterThan(0);
          assertThat(countryCode).contains("LUX", "BEL", "IND");
        },
        "ci_country");
  }

  @Test
  void canLoadState() {
    cisadm.run(
        () -> {
          Set<String> state = target.getState();
          assertThat(state).hasSizeGreaterThan(0);
          assertThat(state).contains("UT", "MP", "OH");
        },
        "ci_state");
  }

  @Test
  void canLoadDivision() {
    cisadm.run(
        () -> {
          Set<String> division = target.getCisDivision();
          assertThat(division).hasSizeGreaterThan(0);
          assertThat(division).contains("00004", "00015", "00001");
        },
        "ci_cis_div_char");
  }

  @Test
  void canLoadCurrencyCodes() {
    cisadm.run(
        () -> {
          Set<String> currency = target.getCurrency();
          assertThat(currency).hasSizeGreaterThan(0);
          assertThat(currency).contains("GBP", "USD", "CAD", "INR", "AUD");
        },
        "ci_currency_cd");
  }

  @Test
  void canLoadSubAccountLookupData() {
    appuserDb.run(
        () -> {
          Map<String, String[]> subAccountType = target.getSubAccountType();
          assertThat(subAccountType).hasSize(3);
          assertThat(subAccountType.get("FUND")).containsExactlyInAnyOrder("FUND", "WAF");
        },
        "sub_acct_lookup");
  }
}
