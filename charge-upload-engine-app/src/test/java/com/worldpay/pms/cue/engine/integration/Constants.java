package com.worldpay.pms.cue.engine.integration;

import io.vavr.collection.List;
import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {
  /** tables to clean before running on our billable-charge-upload schema */
  public static final List<String> OUTPUT_TABLES =
      List.of(
          "cm_rec_chg",
          "cm_rec_chg_audit",
          "cm_rec_chg_err",
          "batch_history",
          "cm_rec_idfr",
          "error_transaction",
          "cm_misc_bill_item",
          "cm_misc_bill_item_ln");

  public static final List<String> CISADM_OUTPUT_TABLES =
      List.of(
          "cm_b_chg_line",
          "cm_b_ln_char",
          "cm_bill_chg_char",
          "cm_bill_chg",
          "cm_bchg_attributes_map");
}
