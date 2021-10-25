package com.worldpay.pms.cue.engine.utils.junit;

import com.esotericsoftware.kryo.Kryo;
import com.worldpay.pms.cue.engine.ChargingKryoRegistrator;
import com.worldpay.pms.cue.engine.InMemoryStaticDataRepository;
import com.worldpay.pms.cue.engine.integration.MockedTransactionChargingServiceFactory;
import com.worldpay.pms.cue.engine.integration.MockedTransactionChargingServiceFactory.InvalidTransactionChargingService;
import com.worldpay.pms.cue.engine.integration.MockedTransactionChargingServiceFactory.MockTransactionChargingServiceWithValidation;
import com.worldpay.pms.cue.engine.integration.MockedTransactionChargingServiceFactory.ValidTransactionChargingService;
import java.util.LinkedHashSet;
import lombok.SneakyThrows;

public class TestingKryoRegistrator extends ChargingKryoRegistrator {

  @SneakyThrows
  @Override
  public void registerClasses(Kryo kryo) {
    super.registerClasses(kryo);

    kryo.register(InMemoryStaticDataRepository.class);
    // lombok @Singular uses LinkedHashSet internally
    kryo.register(LinkedHashSet.class);

    // testing specific
    kryo.register(MockedTransactionChargingServiceFactory.class);
    kryo.register(MockedTransactionChargingServiceFactory.DumbChargingService.class);
    kryo.register(InvalidTransactionChargingService.class);
    kryo.register(ValidTransactionChargingService.class);
    kryo.register(MockTransactionChargingServiceWithValidation.class);
  }
}
