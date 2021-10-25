package com.worldpay.pms.cue.engine.staticdata;

import java.util.Set;

public interface StaticDataRepository {

  Set<String> getBillPeriodCodes();

  Set<String> getChargeTypes();

  Set<String> getCurrencyCodes();

  Set<String> getPriceItems();

  Set<String> getSubAccountTypes();
}
