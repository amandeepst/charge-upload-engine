package com.worldpay.pms.cue.engine.transformations;

import static com.worldpay.pms.cue.engine.encoder.Encoders.CHARGED_AND_FAILED_TRANSACTIONS_ENCODERS;
import static com.worldpay.pms.cue.engine.encoder.Encoders.SUCCESS_OR_FAILURE_RECURRING_CHARGE_ENCODER;
import static com.worldpay.pms.cue.engine.encoder.Encoders.SUCCESS_RECURRING_OR_ERROR_ENCODER;
import static com.worldpay.pms.spark.core.TransformationsUtils.setMDCAndCallFunction;

import com.worldpay.pms.cue.domain.ChargingService;
import com.worldpay.pms.cue.domain.ChargingService.Charge;
import com.worldpay.pms.cue.domain.PendingBillableChargeError;
import com.worldpay.pms.cue.engine.pbc.ErrorTransaction;
import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeRow;
import com.worldpay.pms.cue.engine.recurring.RecurringChargeRow;
import com.worldpay.pms.cue.engine.recurring.RecurringErrorTransaction;
import com.worldpay.pms.cue.engine.recurring.RecurringResultRow;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;
import java.sql.Date;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;

public final class Charging {

  private Charging() {
    throw new AssertionError("Charging cannot be instantiated!");
  }

  public static Dataset<SuccessOrFailureNonRecurring> mapToMiscellaneousCharge(
      Dataset<PendingBillableChargeRow> nonRecurringCharge,
      Dataset<RecurringCharge> recurringCharge,
      Broadcast<ChargingService> domain,
      final Date logicalDate) {

    return nonRecurringCharge
        .map(
            setMDCAndCallFunction(
                (MapFunction<PendingBillableChargeRow, SuccessOrFailureNonRecurring>)
                    (nonRecurring -> fromNonRecurringCharge(nonRecurring, domain, logicalDate))),
            CHARGED_AND_FAILED_TRANSACTIONS_ENCODERS)
        .union(
            recurringCharge.map(
                rec ->
                    SuccessOrFailureNonRecurring.ofSuccess(
                        ChargedTransaction.of(rec.getRecurringResultRow(), rec.getCharge())),
                CHARGED_AND_FAILED_TRANSACTIONS_ENCODERS));
  }

  public static Dataset<SuccessOrFailureRecurringCharge> mapRecurringChargeToSucessorFailure(
      Dataset<PendingBillableChargeRow> pendingBillableChargeRow,
      Broadcast<ChargingService> domain) {

    return pendingBillableChargeRow.map(
        setMDCAndCallFunction(
            (MapFunction<PendingBillableChargeRow, SuccessOrFailureRecurringCharge>)
                (recurring -> mapRecurringChargeToSuccessorFailure(recurring, domain))),
        SUCCESS_OR_FAILURE_RECURRING_CHARGE_ENCODER);
  }

  public static SuccessOrFailureRecurringCharge mapRecurringChargeToSuccessorFailure(
      PendingBillableChargeRow pendingBillableChargeRow, Broadcast<ChargingService> domain) {

    Validation<Seq<DomainError>, Charge> chargeorError =
        domain.getValue().calculateRecurringCharge(pendingBillableChargeRow);
    try {
      return chargeorError.fold(
          error ->
              SuccessOrFailureRecurringCharge.ofError(
                  RecurringErrorTransaction.of(pendingBillableChargeRow, error)),
          charge ->
              SuccessOrFailureRecurringCharge.ofSuccess(
                  IntermediateRecurringCharge.of(
                      RecurringChargeRow.of(pendingBillableChargeRow), charge)));
    } catch (Exception ex) {
      return SuccessOrFailureRecurringCharge.ofError(
          RecurringErrorTransaction.of(pendingBillableChargeRow, ex));
    }
  }

  public static Dataset<SuccessOrFailureRecurring> mapRecrChargesToSuccessorError(
      Dataset<RecurringResultRow> recurringCharge, Broadcast<ChargingService> domain) {
    return recurringCharge.map(
        setMDCAndCallFunction(
            (MapFunction<RecurringResultRow, SuccessOrFailureRecurring>)
                (recurring -> mapRecrChargesToSuccessorError(recurring, domain))),
        SUCCESS_RECURRING_OR_ERROR_ENCODER);
  }

  private static SuccessOrFailureRecurring mapRecrChargesToSuccessorError(
      RecurringResultRow recurring, Broadcast<ChargingService> domain) {

    Validation<Seq<DomainError>, Charge> chargeorError = domain.getValue().charge(recurring);
    try {
      return chargeorError.fold(
          error ->
              SuccessOrFailureRecurring.ofError(RecurringErrorTransaction.of(recurring, error)),
          charge -> SuccessOrFailureRecurring.ofSuccess(RecurringCharge.of(recurring, charge)));
    } catch (Exception ex) {
      return SuccessOrFailureRecurring.ofError(RecurringErrorTransaction.of(recurring, ex));
    }
  }

  private static SuccessOrFailureNonRecurring fromNonRecurringCharge(
      PendingBillableChargeRow nonRecurring, Broadcast<ChargingService> domain, Date logicalDate) {
    try {
      Validation<PendingBillableChargeError, Charge> chargeorError =
          domain.getValue().calculateNonRecurringCharge(nonRecurring);
      return chargeorError.fold(
          error -> SuccessOrFailureNonRecurring.ofError(ErrorTransaction.of(nonRecurring, error)),
          charge ->
              SuccessOrFailureNonRecurring.ofSuccess(
                  ChargedTransaction.of(nonRecurring, charge, logicalDate)));

    } catch (Exception ex) {
      return SuccessOrFailureNonRecurring.ofError(ErrorTransaction.of(nonRecurring, ex));
    }
  }
}
