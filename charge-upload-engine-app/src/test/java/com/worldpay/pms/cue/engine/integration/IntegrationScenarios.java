package com.worldpay.pms.cue.engine.integration;

import com.worldpay.pms.cue.domain.ChargingService;
import com.worldpay.pms.cue.domain.ChargingService.Charge;
import com.worldpay.pms.cue.engine.integration.MockedTransactionChargingServiceFactory.InvalidTransactionChargingService;
import com.worldpay.pms.cue.engine.integration.MockedTransactionChargingServiceFactory.ValidTransactionChargingService;
import java.math.BigDecimal;
import lombok.experimental.UtilityClass;

@UtilityClass
public class IntegrationScenarios {

  public static final ChargingService FAIL_WITH_DOMAIN_ERROR =
      new InvalidTransactionChargingService();
  public static final ChargingService NO_ACCOUNT = new ValidTransactionChargingService();

  public static final ChargingService NON_RECR_CHRG =
      new ValidTransactionChargingService(new Charge("PI_MBA", new BigDecimal("50")));
  public static final ChargingService RECR_CHRG =
      new ValidTransactionChargingService(new Charge("PI_RECUR", new BigDecimal("10")));
}
