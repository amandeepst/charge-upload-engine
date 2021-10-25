package com.worldpay.pms.cue.engine.transformations;

import static com.worldpay.pms.cue.engine.encoder.Encoders.MISC_BILLABLE_ITEM_ENCODER;
import static com.worldpay.pms.cue.engine.encoder.Encoders.MISC_BILLABLE_ITEM_WITH_ID_ENCODER;

import com.worldpay.pms.cue.engine.mbi.MiscBillableItem;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;

@UtilityClass
public class MiscellaneousBillItems {

  public static Dataset<Tuple2<String, MiscBillableItem>> toMiscellaneousBillItem(
      long runId, Dataset<ChargedTransaction> success) {
    return success
        .map(ChargedTransaction::getMiscBillableItem, MISC_BILLABLE_ITEM_ENCODER)
        .mapPartitions(
            p -> MiscBillableItem.generateBillItemId(runId, p), MISC_BILLABLE_ITEM_WITH_ID_ENCODER);
  }
}
