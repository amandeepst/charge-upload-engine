package com.worldpay.pms.mdu.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.mdu.engine.utils.WithDatabase;
import org.junit.jupiter.api.Test;

class MerchantUploadConfigTest implements WithDatabase {

  private MerchantUploadConfig settings;

  @Override
  public void bindMerchantUploadConfiguration(MerchantUploadConfig conf) {
    this.settings = conf;
  }

  @Test
  void passwordsAreNotPrinted() {
    assertThat(settings.toString().toLowerCase()).doesNotContain("password");
  }
}
