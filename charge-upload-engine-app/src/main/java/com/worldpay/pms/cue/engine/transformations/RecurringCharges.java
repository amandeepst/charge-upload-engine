package com.worldpay.pms.cue.engine.transformations;

import static com.worldpay.pms.cue.engine.encoder.Encoders.RECURRING_CHARGE_ROW_ENCODER;
import static org.apache.spark.sql.Encoders.STRING;

import com.worldpay.pms.cue.engine.recurring.RecurringChargeRow;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;

@UtilityClass
public class RecurringCharges {

  public static Dataset<RecurringChargeRow> pickLatestRecurringFromDuplicateSourceId(
      Dataset<RecurringChargeRow> recurring) {
    return recurring
        .groupByKey(RecurringChargeRow::getSourceId, STRING())
        .reduceGroups(RecurringChargeRow::reduce)
        .map(Tuple2::_2, RECURRING_CHARGE_ROW_ENCODER);
  }
}
