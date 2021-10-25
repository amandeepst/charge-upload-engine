package com.worldpay.pms.cue.engine.integration;

import com.typesafe.config.Config;
import com.worldpay.pms.cue.domain.ChargingService;
import com.worldpay.pms.cue.engine.ChargingUploadConfig.ChargingRepositoryConfiguration;
import com.worldpay.pms.cue.engine.ChargingUploadEngine;
import com.worldpay.pms.cue.engine.common.Factory;

public class MockedDomainChargingApp extends ChargingUploadEngine {

  private MockedTransactionChargingServiceFactory mockedTransactionChargingServiceFactory;

  MockedDomainChargingApp(Config config, ChargingService chargingService) {
    super(config);
    this.mockedTransactionChargingServiceFactory =
        new MockedTransactionChargingServiceFactory(chargingService);
  }

  @Override
  protected Factory<ChargingService> getChargingService(ChargingRepositoryConfiguration conf) {
    return mockedTransactionChargingServiceFactory;
  }
}
