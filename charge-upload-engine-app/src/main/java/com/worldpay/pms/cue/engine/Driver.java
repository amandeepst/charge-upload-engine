package com.worldpay.pms.cue.engine;

import static com.worldpay.pms.cue.engine.encoder.Encoders.CHARGED_TRANSACTION_ENCODER;
import static com.worldpay.pms.cue.engine.encoder.Encoders.ERROR_TRANSACTION_ENCODER;
import static com.worldpay.pms.cue.engine.encoder.Encoders.INTERMEDIATE_RECURRING_CHARGE_ENCODER;
import static com.worldpay.pms.cue.engine.encoder.Encoders.RECURRING_CHARGE_ENCODER;
import static com.worldpay.pms.cue.engine.encoder.Encoders.RECURRING_CHARGE_ROW_ENCODER;
import static com.worldpay.pms.cue.engine.encoder.Encoders.RECURRING_ERROR_TRANSACTION_ENCODER;
import static com.worldpay.pms.cue.engine.recurring.RecurringIdentifierTransformation.mapToAggregateRecurringIdentifier;
import static com.worldpay.pms.cue.engine.transformations.Charging.mapRecrChargesToSuccessorError;
import static com.worldpay.pms.cue.engine.transformations.Charging.mapRecurringChargeToSucessorFailure;
import static com.worldpay.pms.cue.engine.transformations.Charging.mapToMiscellaneousCharge;
import static com.worldpay.pms.cue.engine.transformations.MiscellaneousBillItems.toMiscellaneousBillItem;
import static com.worldpay.pms.cue.engine.transformations.RecurringCharges.pickLatestRecurringFromDuplicateSourceId;
import static com.worldpay.pms.cue.engine.transformations.VmwBcuAccountTransformation.leftJoinWithBcuAccountOnpendingCharge;
import static com.worldpay.pms.cue.engine.transformations.VmwBcuAccountTransformation.leftJoinWithBcuAccountOnrecurringCharge;
import static com.worldpay.pms.spark.core.SparkUtils.timed;
import static org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER;

import com.worldpay.pms.cue.domain.ChargingService;
import com.worldpay.pms.cue.engine.batch.ChargingBatchRunResult;
import com.worldpay.pms.cue.engine.common.Factory;
import com.worldpay.pms.cue.engine.mbi.MiscBillableItem;
import com.worldpay.pms.cue.engine.pbc.ErrorTransaction;
import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeRow;
import com.worldpay.pms.cue.engine.recurring.RecurringChargeRow;
import com.worldpay.pms.cue.engine.recurring.RecurringErrorTransaction;
import com.worldpay.pms.cue.engine.recurring.RecurringIdentifierRow;
import com.worldpay.pms.cue.engine.recurring.RecurringResultRow;
import com.worldpay.pms.cue.engine.transformations.ChargedTransaction;
import com.worldpay.pms.cue.engine.transformations.IntermediateRecurringCharge;
import com.worldpay.pms.cue.engine.transformations.SuccessOrFailureNonRecurring;
import com.worldpay.pms.cue.engine.transformations.SuccessOrFailureRecurring;
import com.worldpay.pms.cue.engine.transformations.SuccessOrFailureRecurringCharge;
import com.worldpay.pms.cue.engine.vwm.VwmBcuAccountRow;
import com.worldpay.pms.spark.core.DataSource;
import com.worldpay.pms.spark.core.DataWriter;
import com.worldpay.pms.spark.core.ErrorWriter;
import com.worldpay.pms.spark.core.JavaDriver;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import java.sql.Date;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

@Slf4j
public class Driver extends JavaDriver<ChargingBatchRunResult> {

  private final Factory<ChargingService> chargingServiceFactory;
  private final DataSource<PendingBillableChargeRow> pendingChargeSource;
  private final DataSource<RecurringResultRow> recurringChargeSource;
  private final DataSource<VwmBcuAccountRow> vwmBcuAccountRowDataSource;
  private final DataSource<RecurringIdentifierRow> recurringIdentifierRowDataSource;
  private final DataSource<RecurringResultRow> errorRecurringChargeRowDataSource;
  private final DataWriter<RecurringChargeRow> transitionalRecurringChargeWriter;
  private final ErrorWriter<RecurringErrorTransaction> recurringChargeErrorWriter;
  private final DataWriter<RecurringChargeRow> transitionalReccuringChargeAuditWriter;
  private final DataWriter<Tuple2<String, MiscBillableItem>> miscellaneousBillableItemWriter;
  private final DataWriter<RecurringIdentifierRow> recurringIdentifierDataWriter;
  private final ErrorWriter<ErrorTransaction> errorTransactionWriter;
  private final Function<BatchId, Long> runIdGenerator;
  private final Date logicalDate;

  public Driver(
      SparkSession spark,
      Function<BatchId, Long> runIdProvider,
      Date logicalDate,
      Factory<ChargingService> chargingServiceFactory,
      DataSource<PendingBillableChargeRow> pendingChargeSource,
      DataSource<RecurringResultRow> recurringChargeSource,
      DataSource<VwmBcuAccountRow> vwmBcuAccountRowDataSource,
      DataSource<RecurringIdentifierRow> recurringIdentifierRowDataSource,
      DataSource<RecurringResultRow> errorRecurringChargeRowDataSource,
      DataWriter<RecurringChargeRow> transitionalReccuringChargeWriter,
      ErrorWriter<RecurringErrorTransaction> recurringChargeErrorWriter,
      DataWriter<RecurringChargeRow> transitionalReccuringChargeAuditWriter,
      DataWriter<Tuple2<String, MiscBillableItem>> miscellaneousBillableItemWriter,
      DataWriter<RecurringIdentifierRow> recurringIdentifierDataWriter,
      ErrorWriter<ErrorTransaction> errorTransactionWriter) {
    super(spark);
    this.runIdGenerator = runIdProvider;
    this.logicalDate = logicalDate;
    this.chargingServiceFactory = chargingServiceFactory;
    this.pendingChargeSource = pendingChargeSource;
    this.recurringChargeSource = recurringChargeSource;
    this.vwmBcuAccountRowDataSource = vwmBcuAccountRowDataSource;
    this.recurringIdentifierRowDataSource = recurringIdentifierRowDataSource;
    this.errorRecurringChargeRowDataSource = errorRecurringChargeRowDataSource;
    this.transitionalRecurringChargeWriter = transitionalReccuringChargeWriter;
    this.recurringChargeErrorWriter = recurringChargeErrorWriter;
    this.transitionalReccuringChargeAuditWriter = transitionalReccuringChargeAuditWriter;
    this.miscellaneousBillableItemWriter = miscellaneousBillableItemWriter;
    this.recurringIdentifierDataWriter = recurringIdentifierDataWriter;
    this.errorTransactionWriter = errorTransactionWriter;
  }

  @Override
  public ChargingBatchRunResult run(com.worldpay.pms.spark.core.batch.Batch batch) {
    log.info(
        "Running batch={} attempt={} from={} to={} ilm_dt={} logical_date={}",
        batch.id.code,
        batch.id.attempt,
        batch.id.watermark.low,
        batch.id.watermark.high,
        batch.createdAt,
        logicalDate);

    Dataset<PendingBillableChargeRow> pendingCharge = pendingChargeSource.load(spark, batch.id);

    Dataset<VwmBcuAccountRow> vwmBcuAccountRowDataset =
        vwmBcuAccountRowDataSource.load(spark, batch.id).persist(MEMORY_AND_DISK_SER());

    // Load last completed partition
    Dataset<RecurringIdentifierRow> currentRecurringIdentifier =
        recurringIdentifierRowDataSource.load(spark, batch.id).persist(MEMORY_AND_DISK_SER());

    // Load last completed Batch error records
    Dataset<RecurringResultRow> errorRecurringChargeRow =
        errorRecurringChargeRowDataSource.load(spark, batch.id);

    val pendingBillableWithBcuAccount =
        leftJoinWithBcuAccountOnpendingCharge(pendingCharge, vwmBcuAccountRowDataset)
            .persist(MEMORY_AND_DISK_SER());

    Broadcast<ChargingService> domain =
        timed(
            "Broadcast Charging Service",
            () -> jsc.broadcast(chargingServiceFactory.build().get()));

    Dataset<PendingBillableChargeRow> nonRecurringCharge =
        pendingBillableWithBcuAccount.filter(PendingBillableChargeRow::isNonRecurring);

    val transitionalRecurringCharge =
        pendingBillableWithBcuAccount.filter(PendingBillableChargeRow::isRecurring);

    val successOrFailureRecurringCharge =
        mapRecurringChargeToSucessorFailure(transitionalRecurringCharge, domain)
            .persist(MEMORY_AND_DISK_SER());

    val validRecurringCharge =
        successOrFailureRecurringCharge
            .filter(SuccessOrFailureRecurringCharge::isSuccess)
            .map(
                SuccessOrFailureRecurringCharge::getSuccessRecurringChargeRow,
                INTERMEDIATE_RECURRING_CHARGE_ENCODER)
            .map(IntermediateRecurringCharge::getRecurringChargeRow, RECURRING_CHARGE_ROW_ENCODER)
            .persist(MEMORY_AND_DISK_SER());

    long transitionalRecurringChargeAuditCount =
        writeTransitionalRecurringChargeAudit(validRecurringCharge, batch);

    long transitionalRecurringChargeCount =
        writeTransitionalRecurringCharge(
            pickLatestRecurringFromDuplicateSourceId(validRecurringCharge), batch);

    Dataset<RecurringResultRow> recurringCharge =
        namedAction(
            "Load Recurring Charge Source Dataset",
            sc -> recurringChargeSource.load(spark, batch.id));

    val successOrerrorRecurringResultRow = recurringCharge.union(errorRecurringChargeRow);

    val recurringResultWithBcuAccount =
        leftJoinWithBcuAccountOnrecurringCharge(
            successOrerrorRecurringResultRow, vwmBcuAccountRowDataset);

    Dataset<SuccessOrFailureRecurring> successRecrOrError =
        mapRecrChargesToSuccessorError(recurringResultWithBcuAccount, domain)
            .persist(MEMORY_AND_DISK_SER());

    val succesRecCharges =
        successRecrOrError
            .filter(SuccessOrFailureRecurring::isSuccess)
            .map(SuccessOrFailureRecurring::getSuccessRow, RECURRING_CHARGE_ENCODER)
            .persist(MEMORY_AND_DISK_SER());

    // pass only success recurring to mapToMiscellaneousCharge
    Dataset<SuccessOrFailureNonRecurring> successOrError =
        mapToMiscellaneousCharge(nonRecurringCharge, succesRecCharges, domain, logicalDate)
            .persist(MEMORY_AND_DISK_SER());

    Dataset<ChargedTransaction> success =
        successOrError
            .filter(SuccessOrFailureNonRecurring::isSuccess)
            .map(SuccessOrFailureNonRecurring::getSuccess, CHARGED_TRANSACTION_ENCODER);

    Dataset<ErrorTransaction> failure =
        successOrError
            .filter(SuccessOrFailureNonRecurring::isFailure)
            .map(SuccessOrFailureNonRecurring::getFailure, ERROR_TRANSACTION_ENCODER);

    val invalidRecurringCharge =
        successOrFailureRecurringCharge
            .filter(SuccessOrFailureRecurringCharge::isFailure)
            .map(
                SuccessOrFailureRecurringCharge::getRecurringFailure,
                RECURRING_ERROR_TRANSACTION_ENCODER);

    Dataset<RecurringErrorTransaction> recurringFailure =
        successRecrOrError
            .filter(SuccessOrFailureRecurring::isFailure)
            .map(
                SuccessOrFailureRecurring::getRecurringFailure, RECURRING_ERROR_TRANSACTION_ENCODER)
            .union(invalidRecurringCharge)
            .persist(MEMORY_AND_DISK_SER());

    Dataset<RecurringIdentifierRow> recurringIdentifierRowDataset =
        mapToAggregateRecurringIdentifier(successRecrOrError, currentRecurringIdentifier);

    // map charging result to bill item
    long runId = runIdGenerator.apply(batch.id);
    log.info("Generated seed id `{}` for batch `{}`", runId, batch.id);

    long miscellaneousBillableItemCount =
        writeMiscellaneousBillableItem(toMiscellaneousBillItem(runId, success), batch);

    long errorTransactionCount = writeErrorTransaction(failure, batch);

    long recurringErrorTransactionCount = writeRecurringErrorTransaction(recurringFailure, batch);

    long recurringIdentifierCount = writeRecurringIdentifier(recurringIdentifierRowDataset, batch);

    long failedTodaytransacationCount =
        recurringChargeErrorWriter.getFirstFailureCount()
            + errorTransactionWriter.getFirstFailureCount();

    long ignoreTransactionCount =
        recurringChargeErrorWriter.getIgnoredCount() + errorTransactionWriter.getIgnoredCount();

    return ChargingBatchRunResult.builder()
        .recurringChargeCount(transitionalRecurringChargeCount)
        .recurringChargeCountForAudit(transitionalRecurringChargeAuditCount)
        .recurringChargeFailureCount(recurringErrorTransactionCount)
        .nonRecurringChargeFailureCount(errorTransactionCount)
        .miscellaneousBillableItemCount(miscellaneousBillableItemCount)
        .recurringIdentifierCount(recurringIdentifierCount)
        .failedTodayTransactions(failedTodaytransacationCount)
        .ignoredTransactionsCount(ignoreTransactionCount)
        .build();
  }

  private long writeTransitionalRecurringCharge(
      Dataset<RecurringChargeRow> transitionalRecurringCharge,
      com.worldpay.pms.spark.core.batch.Batch batch) {
    return namedAction(
        "Writing Transitional Recurring Charge Transactions",
        sparkContext ->
            transitionalRecurringChargeWriter.write(
                batch.id, batch.createdAt, transitionalRecurringCharge));
  }

  private long writeTransitionalRecurringChargeAudit(
      Dataset<RecurringChargeRow> transitionalRecurringCharge,
      com.worldpay.pms.spark.core.batch.Batch batch) {
    return namedAction(
        "Writing Recurring Charge Transactions For Audit",
        sparkContext ->
            transitionalReccuringChargeAuditWriter.write(
                batch.id, batch.createdAt, transitionalRecurringCharge));
  }

  private long writeMiscellaneousBillableItem(
      Dataset<Tuple2<String, MiscBillableItem>> miscBillableItem,
      com.worldpay.pms.spark.core.batch.Batch batch) {
    return namedAction(
        "Writing Miscellaneous Billable Item Transactions",
        sparkContext ->
            miscellaneousBillableItemWriter.write(batch.id, batch.createdAt, miscBillableItem));
  }

  private long writeErrorTransaction(
      Dataset<ErrorTransaction> errorTransaction, com.worldpay.pms.spark.core.batch.Batch batch) {
    return namedAction(
        "Writing Error Transactions",
        sparkContext -> errorTransactionWriter.write(batch.id, batch.createdAt, errorTransaction));
  }

  private long writeRecurringErrorTransaction(
      Dataset<RecurringErrorTransaction> recurringErrorTransaction,
      com.worldpay.pms.spark.core.batch.Batch batch) {
    return namedAction(
        "Writing recurringError Transactions",
        sparkContext ->
            recurringChargeErrorWriter.write(batch.id, batch.createdAt, recurringErrorTransaction));
  }

  private long writeRecurringIdentifier(
      Dataset<RecurringIdentifierRow> recurringIdentifierRowDataset,
      com.worldpay.pms.spark.core.batch.Batch batch) {
    return namedAction(
        "Writing recurringIdentifierRowDataset",
        sparkContext ->
            recurringIdentifierDataWriter.write(
                batch.id, batch.createdAt, recurringIdentifierRowDataset));
  }
}
