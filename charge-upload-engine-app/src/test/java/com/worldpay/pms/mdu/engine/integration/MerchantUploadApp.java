package com.worldpay.pms.mdu.engine.integration;

import com.typesafe.config.Config;
import com.worldpay.pms.mdu.domain.MerchantDataProcessingService;
import com.worldpay.pms.mdu.engine.MerchantDataProcessingFactory;
import com.worldpay.pms.mdu.engine.MerchantUploadConfig;
import com.worldpay.pms.mdu.engine.MerchantUploadEngine;
import com.worldpay.pms.mdu.engine.common.Factory;
import com.worldpay.pms.mdu.engine.data.StaticDataRepository;

public class MerchantUploadApp extends MerchantUploadEngine {

  Factory<StaticDataRepository> staticRepository;

  MerchantUploadApp(Config config, Factory<StaticDataRepository> staticRepository) {
    super(config);
    this.staticRepository = staticRepository;
  }

  @Override
  protected Factory<MerchantDataProcessingService> getMerchantDataProcessingService(
      MerchantUploadConfig.MerchantUploadRepositoryConfiguration conf) {
    return new MerchantDataProcessingFactory(staticRepository);
  }
}
