package com.worldpay.pms.cue.engine;

import static com.worldpay.pms.spark.core.SparkUtils.timed;

import com.worldpay.pms.cue.domain.ChargingService;
import com.worldpay.pms.cue.domain.DefaultChargingService;
import com.worldpay.pms.cue.domain.validator.RawValidationService;
import com.worldpay.pms.cue.engine.common.Factory;
import com.worldpay.pms.cue.engine.staticdata.StaticDataRepository;
import io.vavr.control.Try;

public class TransactionChargingServiceFactory implements Factory<ChargingService> {

  private final Factory<StaticDataRepository> repositoryFactory;

  public TransactionChargingServiceFactory(Factory<StaticDataRepository> repositoryFactory) {
    this.repositoryFactory = repositoryFactory;
  }

  private static ChargingService build(StaticDataRepository source) {
    return new DefaultChargingService(buildValidationService(source));
  }

  @Override
  public Try<ChargingService> build() {
    return repositoryFactory.build().map(TransactionChargingServiceFactory::build);
  }

  private static RawValidationService buildValidationService(StaticDataRepository source) {
    return timed(
        "RawValidationService",
        () ->
            new RawValidationService(
                source.getCurrencyCodes(),
                source.getPriceItems(),
                source.getSubAccountTypes(),
                source.getBillPeriodCodes()));
  }
}
