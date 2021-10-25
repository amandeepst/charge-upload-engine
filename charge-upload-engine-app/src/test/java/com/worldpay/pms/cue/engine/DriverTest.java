package com.worldpay.pms.cue.engine;

import static com.worldpay.pms.cue.engine.encoder.Encoders.PENDING_BILLABLE_CHARGE_ROW_ENCODER;
import static com.worldpay.pms.cue.engine.encoder.Encoders.RECURRING_IDENTIFIER_ROW_ENCODER;
import static com.worldpay.pms.cue.engine.encoder.Encoders.RECURRING_RESULT_ROW_ENCODER;
import static com.worldpay.pms.cue.engine.encoder.Encoders.VWM_BCU_ACCOUNT_ROW_ENCODER;
import static com.worldpay.pms.cue.engine.samples.Transactions.CHBK_PENDING_CHARGE;
import static com.worldpay.pms.cue.engine.samples.Transactions.CHRG_PENDING_CHARGE;
import static com.worldpay.pms.cue.engine.samples.Transactions.INVALID_PENDING_CHARGE_3;
import static com.worldpay.pms.cue.engine.samples.Transactions.INVALID_PENDING_CHARGE_4;
import static com.worldpay.pms.cue.engine.samples.Transactions.INVALID_PENDING_CHARGE_WITHOUT_SUBACCOUNT;
import static com.worldpay.pms.cue.engine.samples.Transactions.INVALID_PENDING_CHARGE_WITH_NULL_FIELDS;
import static com.worldpay.pms.cue.engine.samples.Transactions.INVALID_RECR_CHARGE;
import static com.worldpay.pms.cue.engine.samples.Transactions.INVALID_RECR_CHARGE_2;
import static com.worldpay.pms.cue.engine.samples.Transactions.INVALID_RECR_CHARGE_WITH_NULL_SOURCE_ID;
import static com.worldpay.pms.cue.engine.samples.Transactions.RECR_CHARGE;
import static com.worldpay.pms.cue.engine.samples.Transactions.RECR_PENDING_CHARGE;
import static com.worldpay.pms.cue.engine.samples.Transactions.RECR_PENDING_CHARGE_WITH_INVALID_DATE_AND_VALID_STATUS;
import static com.worldpay.pms.cue.engine.samples.Transactions.RECR_PENDING_CHARGE_WITH_VALID_DATE_AND_INVALID_STATUS;
import static com.worldpay.pms.cue.engine.samples.Transactions.RECR_PENDING_CHARGE_WITH_VALID_DATE_AND_NULL_STATUS;
import static com.worldpay.pms.cue.engine.samples.Transactions.VWM_BCU_ACCOUNT_ROW_1;
import static com.worldpay.pms.cue.engine.samples.Transactions.VWM_BCU_ACCOUNT_ROW_2;
import static com.worldpay.pms.cue.engine.samples.Transactions.VWM_BCU_ACCOUNT_ROW_3;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;
import com.worldpay.pms.cue.engine.batch.ChargingBatchRunResult;
import com.worldpay.pms.cue.engine.mbi.MiscBillableItem;
import com.worldpay.pms.cue.engine.pbc.ErrorTransaction;
import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeRow;
import com.worldpay.pms.cue.engine.recurring.RecurringChargeRow;
import com.worldpay.pms.cue.engine.recurring.RecurringErrorTransaction;
import com.worldpay.pms.cue.engine.recurring.RecurringIdentifierRow;
import com.worldpay.pms.cue.engine.recurring.RecurringResultRow;
import com.worldpay.pms.cue.engine.transformations.writers.InMemoryErrorWriter;
import com.worldpay.pms.cue.engine.vwm.VwmBcuAccountRow;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.Batch.Watermark;
import com.worldpay.pms.testing.stereotypes.WithSpark;
import com.worldpay.pms.testing.utils.InMemoryDataSource;
import com.worldpay.pms.testing.utils.InMemoryWriter;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.Collections;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

@WithSpark
class DriverTest {

  private SparkSession spark;
  private InMemoryDataSource.Builder<PendingBillableChargeRow> pendingChargeSourceBuilder;
  private InMemoryDataSource.Builder<RecurringResultRow> recurringSourceBuilder;
  private InMemoryDataSource.Builder<VwmBcuAccountRow> vwmBcuAccountRowBuilder;
  private InMemoryDataSource.Builder<RecurringIdentifierRow> recurringIdentifierRowBuilder;
  private InMemoryDataSource.Builder<RecurringResultRow> recurringErrorResultRowBuilder;
  private InMemoryStaticDataRepository.InMemoryStaticDataRepositoryBuilder staticData;
  private InMemoryWriter<RecurringChargeRow> recurringChargeWriter;
  private InMemoryErrorWriter<RecurringErrorTransaction> recurringChargeErrorWriter;
  private InMemoryWriter<RecurringChargeRow> recurringChargeWriterForAudit;
  private InMemoryWriter<RecurringIdentifierRow> recurringIdentifierWriter;
  private InMemoryErrorWriter<ErrorTransaction> errorTransactionWriter;
  private InMemoryWriter<Tuple2<String, MiscBillableItem>> miscBillableItemWriter;
  private static final Date LOGICAL_DATE = Date.valueOf("2020-09-30");

  @BeforeEach
  void setup(SparkSession spark) {
    this.spark = spark;
    pendingChargeSourceBuilder = InMemoryDataSource.builder(PENDING_BILLABLE_CHARGE_ROW_ENCODER);
    recurringSourceBuilder = InMemoryDataSource.builder(RECURRING_RESULT_ROW_ENCODER);
    vwmBcuAccountRowBuilder = InMemoryDataSource.builder(VWM_BCU_ACCOUNT_ROW_ENCODER);
    recurringIdentifierRowBuilder = InMemoryDataSource.builder(RECURRING_IDENTIFIER_ROW_ENCODER);
    recurringErrorResultRowBuilder = InMemoryDataSource.builder(RECURRING_RESULT_ROW_ENCODER);
    staticData = InMemoryStaticDataRepository.builder();
    recurringChargeWriter = new InMemoryWriter<>();
    recurringChargeErrorWriter = new InMemoryErrorWriter<>();
    recurringChargeWriterForAudit = new InMemoryWriter<>();
    errorTransactionWriter = new InMemoryErrorWriter<>();
    miscBillableItemWriter = new InMemoryWriter<>();
    recurringIdentifierWriter = new InMemoryWriter<>();
  }

  @Test
  void whenNoPendingBillableChargeThenNoOutputsAreProduced() {
    staticData
        .chargeTypes(Collections.emptySet())
        .currencyCodes(Collections.emptySet())
        .billPeriodCodes(Collections.emptySet())
        .priceItems(Collections.emptySet())
        .subAccountTypes(Collections.emptySet());

    ChargingBatchRunResult chargingBatchRunResultRunResult =
        createDriverAndRun(
            pendingChargeSourceBuilder.build(),
            recurringSourceBuilder.build(),
            vwmBcuAccountRowBuilder.build(),
            recurringIdentifierRowBuilder.build(),
            recurringErrorResultRowBuilder.build());
    assertTrasactionsCount(chargingBatchRunResultRunResult, 0, 0, 0);
  }

  @Test
  void whenNoStaticDataThenValidPendingChargeRowFails() {
    staticData
        .chargeTypes(Collections.emptySet())
        .currencyCodes(Collections.emptySet())
        .billPeriodCodes(Collections.emptySet())
        .priceItems(Collections.emptySet())
        .subAccountTypes(Collections.emptySet());

    pendingChargeSourceBuilder.addRows(CHBK_PENDING_CHARGE, CHRG_PENDING_CHARGE);
    vwmBcuAccountRowBuilder.addRows(VWM_BCU_ACCOUNT_ROW_2, VWM_BCU_ACCOUNT_ROW_1);

    ChargingBatchRunResult chargingBatchRunResultRunResult =
        createDriverAndRun(
            pendingChargeSourceBuilder.build(),
            recurringSourceBuilder.build(),
            vwmBcuAccountRowBuilder.build(),
            recurringIdentifierRowBuilder.build(),
            recurringErrorResultRowBuilder.build());

    assertTrasactionsCount(chargingBatchRunResultRunResult, 0, 0, 2);
  }

  @Test
  void whenInvalidInputTransactionThenShouldMoveToError() {
    staticData
        .chargeTypes(Collections.emptySet())
        .currencyCodes(Sets.newHashSet("GBP", "EUR"))
        .priceItems(Sets.newHashSet("ADHOCCHG", "MIGCHBK"))
        .subAccountTypes(Sets.newHashSet("CHRG", "RECR", "CHBK"))
        .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO"));

    pendingChargeSourceBuilder.addRows(PendingBillableChargeRow.builder().build());
    recurringSourceBuilder.addRows(
        RecurringResultRow.builder()
            .frequencyIdentifier("WPDY")
            .recurringSourceId("12345")
            .currency("GBP")
            .build(),
        INVALID_RECR_CHARGE,
        INVALID_RECR_CHARGE_WITH_NULL_SOURCE_ID);
    vwmBcuAccountRowBuilder.addRows(VWM_BCU_ACCOUNT_ROW_2, VWM_BCU_ACCOUNT_ROW_1);

    ChargingBatchRunResult chargingBatchRunResultRunResult =
        createDriverAndRun(
            pendingChargeSourceBuilder.build(),
            recurringSourceBuilder.build(),
            vwmBcuAccountRowBuilder.build(),
            recurringIdentifierRowBuilder.build(),
            recurringErrorResultRowBuilder.build());

    assertTrasactionsCount(chargingBatchRunResultRunResult, 0, 3, 1);
  }

  @Test
  void whenValidInputTransactionThenShouldMoveToSuccess() {
    staticData
        .chargeTypes(Collections.emptySet())
        .currencyCodes(Sets.newHashSet("GBP", "EUR"))
        .priceItems(Sets.newHashSet("ADHOCCHG", "MIGCHBK"))
        .subAccountTypes(Sets.newHashSet("CHRG", "RECR", "CHBK"))
        .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO"));

    pendingChargeSourceBuilder.addRows(CHBK_PENDING_CHARGE, CHRG_PENDING_CHARGE);
    recurringSourceBuilder.addRows(RECR_CHARGE);
    vwmBcuAccountRowBuilder.addRows(
        VWM_BCU_ACCOUNT_ROW_2, VWM_BCU_ACCOUNT_ROW_1, VWM_BCU_ACCOUNT_ROW_3);

    ChargingBatchRunResult chargingBatchRunResultRunResult =
        createDriverAndRun(
            pendingChargeSourceBuilder.build(),
            recurringSourceBuilder.build(),
            vwmBcuAccountRowBuilder.build(),
            recurringIdentifierRowBuilder.build(),
            recurringErrorResultRowBuilder.build());

    assertTrasactionsCount(chargingBatchRunResultRunResult, 3, 0, 0);
  }

  @Test
  void whenPendingRecurringChargeThenMapToTransitionalRecurringCharge() {
    staticData
        .chargeTypes(Collections.emptySet())
        .currencyCodes(Sets.newHashSet("GBP", "EUR"))
        .priceItems(Sets.newHashSet("ADHOCCHG", "MIGCHBK"))
        .subAccountTypes(Sets.newHashSet("CHRG", "RECR", "CHBK"))
        .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO"));

    pendingChargeSourceBuilder.addRows(
        RECR_PENDING_CHARGE,
        RECR_PENDING_CHARGE_WITH_VALID_DATE_AND_INVALID_STATUS,
        RECR_PENDING_CHARGE_WITH_VALID_DATE_AND_NULL_STATUS,
        RECR_PENDING_CHARGE_WITH_INVALID_DATE_AND_VALID_STATUS);
    vwmBcuAccountRowBuilder.addRows(VWM_BCU_ACCOUNT_ROW_2, VWM_BCU_ACCOUNT_ROW_1);

    ChargingBatchRunResult chargingBatchRunResultRunResult =
        createDriverAndRun(
            pendingChargeSourceBuilder.build(),
            recurringSourceBuilder.build(),
            vwmBcuAccountRowBuilder.build(),
            recurringIdentifierRowBuilder.build(),
            recurringErrorResultRowBuilder.build());

    assertThat(chargingBatchRunResultRunResult.getRecurringChargeCount()).isEqualTo(4);
  }

  @Test
  void whenMixedTransactionThenSuccessAndFailAccordingly() {
    staticData
        .chargeTypes(Collections.emptySet())
        .currencyCodes(Sets.newHashSet("GBP", "EUR"))
        .priceItems(Sets.newHashSet("ADHOCCHG", "MIGCHBK"))
        .subAccountTypes(Sets.newHashSet("CHRG", "RECR", "CHBK"))
        .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO"));

    pendingChargeSourceBuilder.addRows(
        INVALID_PENDING_CHARGE_3,
        INVALID_PENDING_CHARGE_4,
        INVALID_PENDING_CHARGE_WITHOUT_SUBACCOUNT,
        INVALID_PENDING_CHARGE_WITH_NULL_FIELDS,
        CHRG_PENDING_CHARGE,
        RECR_PENDING_CHARGE);
    recurringSourceBuilder.addRows(INVALID_RECR_CHARGE, INVALID_RECR_CHARGE_2, RECR_CHARGE);

    vwmBcuAccountRowBuilder.addRows(
        VWM_BCU_ACCOUNT_ROW_2, VWM_BCU_ACCOUNT_ROW_1, VWM_BCU_ACCOUNT_ROW_3);

    ChargingBatchRunResult chargingBatchRunResultRunResult =
        createDriverAndRun(
            pendingChargeSourceBuilder.build(),
            recurringSourceBuilder.build(),
            vwmBcuAccountRowBuilder.build(),
            recurringIdentifierRowBuilder.build(),
            recurringErrorResultRowBuilder.build());
    assertTrasactionsCount(chargingBatchRunResultRunResult, 2, 2, 4);
  }

  ChargingBatchRunResult createDriverAndRun(
      InMemoryDataSource<PendingBillableChargeRow> eventSource,
      InMemoryDataSource<RecurringResultRow> recurringSource,
      InMemoryDataSource<VwmBcuAccountRow> vwmBcuAccountSource,
      InMemoryDataSource<RecurringIdentifierRow> recurringIdentifierSource,
      InMemoryDataSource<RecurringResultRow> recurringErrorSource) {
    Driver driver =
        new Driver(
            spark,
            id -> 1L,
            LOGICAL_DATE,
            new TransactionChargingServiceFactory(new DummyFactory<>(staticData.build())),
            eventSource,
            recurringSource,
            vwmBcuAccountSource,
            recurringIdentifierSource,
            recurringErrorSource,
            recurringChargeWriter,
            recurringChargeErrorWriter,
            recurringChargeWriterForAudit,
            miscBillableItemWriter,
            recurringIdentifierWriter,
            errorTransactionWriter);
    return driver.run(getGenericBatch());
  }

  com.worldpay.pms.spark.core.batch.Batch getGenericBatch() {
    Watermark watermark = new Watermark(LocalDateTime.MIN, LocalDateTime.MAX);
    BatchId id = new BatchId(watermark, 1);
    LocalDateTime createdAt = LocalDateTime.now();

    return new com.worldpay.pms.spark.core.batch.Batch(id, null, createdAt, "", null);
  }

  private void assertTrasactionsCount(
      ChargingBatchRunResult result, long success, long error, long ignore) {
    assertThat(result.getSuccessTransactionsCount()).isEqualTo(success);
    assertThat(result.getNonRecurringChargeFailureCount() + result.getRecurringChargeFailureCount())
        .isEqualTo(error);
    assertThat(result.getIgnoredTransactionsCount()).isEqualTo(ignore);
  }
}
