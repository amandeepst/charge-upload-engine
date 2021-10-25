package com.worldpay.pms.cue.engine.recurring;

import static com.worldpay.pms.cue.engine.encoder.Encoders.RECURRING_IDENTIFIER_KEY_ENCODER;
import static com.worldpay.pms.cue.engine.encoder.Encoders.RECURRING_IDENTIFIER_ROW_ENCODER;
import static com.worldpay.pms.cue.engine.encoder.Encoders.RECURRING_IDENTIFIER_WRAPPER_ENCODER;

import com.worldpay.pms.cue.engine.transformations.SafeCheckUtils.RecurringIdentifierWrapper;
import com.worldpay.pms.cue.engine.transformations.SuccessOrFailureRecurring;
import lombok.val;
import org.apache.spark.sql.Dataset;

public class RecurringIdentifierTransformation {

  private RecurringIdentifierTransformation() {
    throw new IllegalStateException("Utility class can't be instantiated");
  }

  public static Dataset<RecurringIdentifierRow> mapToAggregateRecurringIdentifier(
      Dataset<SuccessOrFailureRecurring> successRecrOrError,
      Dataset<RecurringIdentifierRow> currentRecurringIdentifier) {
    return currentRecurringIdentifier
        .union(unionSuccessOrErrorRecurring(successRecrOrError))
        .map(
            RecurringIdentifierWrapper::fromRecurringIdentifier,
            RECURRING_IDENTIFIER_WRAPPER_ENCODER)
        .groupByKey(RecurringIdentifierWrapper::getKey, RECURRING_IDENTIFIER_KEY_ENCODER)
        .reduceGroups(RecurringIdentifierWrapper::reduceRecurringIdentifierRow)
        .map(t -> t._2.getRow(), RECURRING_IDENTIFIER_ROW_ENCODER);
  }

  private static Dataset<RecurringIdentifierRow> unionSuccessOrErrorRecurring(
      Dataset<SuccessOrFailureRecurring> successRecrOrError) {

    val errorRecurring =
        successRecrOrError
            .filter(SuccessOrFailureRecurring::isFailure)
            .map(
                successRecurringOrError ->
                    RecurringIdentifierRow.of(successRecurringOrError.getRecurringFailure()),
                RECURRING_IDENTIFIER_ROW_ENCODER);

    val successRecurring =
        successRecrOrError
            .filter(SuccessOrFailureRecurring::isSuccess)
            .map(
                successRecurringOrError ->
                    RecurringIdentifierRow.of(successRecurringOrError.getSuccessRow()),
                RECURRING_IDENTIFIER_ROW_ENCODER);

    return errorRecurring.union(successRecurring);
  }
}
