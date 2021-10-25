package com.worldpay.pms.cue.engine.scenario;

import static com.typesafe.config.ConfigFactory.load;

import com.typesafe.config.Config;
import com.worldpay.pms.cue.domain.ChargingService;
import com.worldpay.pms.cue.engine.ChargingUploadConfig.ChargingRepositoryConfiguration;
import com.worldpay.pms.cue.engine.ChargingUploadEngine;
import com.worldpay.pms.cue.engine.DummyFactory;
import com.worldpay.pms.cue.engine.TransactionChargingServiceFactory;
import com.worldpay.pms.cue.engine.common.Factory;
import com.worldpay.pms.cue.engine.staticdata.StaticDataRepository;

public class ScenarioApp extends ChargingUploadEngine {

  private final StaticDataRepository staticDataRepository;

  public ScenarioApp(Config config, StaticDataRepository staticDataRepository) {
    super(load("end-to-end-ft3.conf"));
    this.staticDataRepository = staticDataRepository;
  }

  @Override
  protected Factory<ChargingService> getChargingService(ChargingRepositoryConfiguration conf) {
    return new TransactionChargingServiceFactory(new DummyFactory<>(staticDataRepository));
  }
}
