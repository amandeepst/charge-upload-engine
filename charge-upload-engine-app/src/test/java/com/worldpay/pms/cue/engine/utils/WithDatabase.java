package com.worldpay.pms.cue.engine.utils;

import static com.typesafe.config.ConfigBeanFactory.create;
import static com.typesafe.config.ConfigFactory.load;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.worldpay.pms.cue.engine.ChargingUploadConfig;
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
    bindChargingJdbcConfiguration(create(conf.getConfig("db"), JdbcConfiguration.class));
    bindChargingConfiguration(create(conf.getConfig("settings"), ChargingUploadConfig.class));
  }

  default void bindOrmbJdbcConfiguration(JdbcConfiguration conf) {
    // do nothing by default.
  }

  default void bindChargingJdbcConfiguration(JdbcConfiguration conf) {
    // do nothing by default.
  }

  default void bindChargingConfiguration(ChargingUploadConfig conf) {
    // do nothing by default.
  }
}
