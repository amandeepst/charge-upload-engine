package com.worldpay.pms.cue.domain.integration;

import com.worldpay.pms.cue.domain.ChargingService;
import com.worldpay.pms.cue.domain.DefaultChargingService;
import com.worldpay.pms.cue.domain.validator.RawValidationService;
import java.util.Set;
import java.util.function.Function;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Scenario {

  Set<String> billPeriodCodes;
  Set<String> chargeTypes;
  Set<String> currencyCodes;
  Set<String> priceItems;
  Set<String> subAccountTypes;

  public RawValidationService buildValidationService() {

    return new RawValidationService(currencyCodes, priceItems, subAccountTypes, billPeriodCodes);
  }

  public static ChargingService service(Function<ScenarioBuilder, ScenarioBuilder> expr) {
    return service(expr.apply(Scenario.builder()).build());
  }

  public static ChargingService service(Scenario scenario) {
    return new DefaultChargingService(scenario.buildValidationService());
  }
}
