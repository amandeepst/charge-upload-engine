package com.worldpay.pms.mdu.engine.utils.junit;

import com.esotericsoftware.kryo.Kryo;
import com.worldpay.pms.mdu.engine.InMemoryStaticDataRepository;
import com.worldpay.pms.mdu.engine.MerchantUploadKryoRegistrator;
import com.worldpay.pms.mdu.engine.transformations.PartyTransformationTest;

public class TestingKryoRegistrator extends MerchantUploadKryoRegistrator {
  @Override
  public void registerClasses(Kryo kryo) {
    super.registerClasses(kryo);
    kryo.register(InMemoryStaticDataRepository.class);

    kryo.register(PartyTransformationTest.FailedMerchantDataProcessingService.class);
  }
}
