package com.worldpay.pms.mdu.engine.integration;

import io.vavr.collection.List;
import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {
  public static final List<String> OUTPUT_TABLES =
      List.of(
          "party",
          "acct",
          "sub_acct",
          "withhold_funds",
          "error_transaction",
          "acct_hier",
          "error_acct_hier",
          "batch_history",
          "outputs_registry");
}
