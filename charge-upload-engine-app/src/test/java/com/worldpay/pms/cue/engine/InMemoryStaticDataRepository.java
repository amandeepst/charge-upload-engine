package com.worldpay.pms.cue.engine;

import com.worldpay.pms.cue.engine.staticdata.StaticDataRepository;
import java.util.Set;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class InMemoryStaticDataRepository implements StaticDataRepository {

  Set<String> billPeriodCodes;
  Set<String> chargeTypes;
  Set<String> currencyCodes;
  Set<String> priceItems;
  Set<String> subAccountTypes;
}
