package com.worldpay.pms.cue.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.cue.engine.utils.WithDatabase;
import org.junit.jupiter.api.Test;

class ChargingUploadConfigTest implements WithDatabase {

  private ChargingUploadConfig settings;

  @Override
  public void bindChargingConfiguration(ChargingUploadConfig conf) {
    this.settings = conf;
  }

  @Test
  void passwordsAreNotPrinted() {
    assertThat(settings.toString().toLowerCase()).doesNotContain("password");
  }
}
