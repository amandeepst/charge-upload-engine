package com.worldpay.pms.mdu.engine.utils;

import static com.typesafe.config.ConfigBeanFactory.create;
import static com.typesafe.config.ConfigFactory.load;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.worldpay.pms.mdu.engine.MerchantUploadConfig;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import org.junit.jupiter.api.BeforeEach;

public interface WithDatabase {

  @BeforeEach
  default void init() {
    // load the db info for those that want it
    ConfigFactory.invalidateCaches();
    Config conf = load("application-test.conf");
    bindOrmbJdbcConfiguration(
        create(load("conf/db.conf").getConfig("cisadm"), JdbcConfiguration.class));
    bindMerchantUploadJdbcConfiguration(create(conf.getConfig("db"), JdbcConfiguration.class));
    bindMerchantUploadConfiguration(create(conf.getConfig("settings"), MerchantUploadConfig.class));
  }

  default void bindOrmbJdbcConfiguration(JdbcConfiguration conf) {
    // do nothing by default.
  }

  default void bindMerchantUploadJdbcConfiguration(JdbcConfiguration conf) {
    // do nothing by default.
  }

  default void bindMerchantUploadConfiguration(MerchantUploadConfig conf) {
    // do nothing by default.
  }
}
