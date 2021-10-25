package com.worldpay.pms.mdu.engine;

import com.worldpay.pms.mdu.engine.data.StaticDataRepository;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class InMemoryStaticDataRepository implements StaticDataRepository {
  Set<String> billCycleCode;

  Set<String> countryCode;

  Set<String> cisDivision;

  Set<String> state;

  Set<String> currency;

  Map<String, String[]> subAccountType;
}
