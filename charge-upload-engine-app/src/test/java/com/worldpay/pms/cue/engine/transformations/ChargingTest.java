package com.worldpay.pms.cue.engine.transformations;

import static com.worldpay.pms.cue.engine.encoder.Encoders.PENDING_BILLABLE_CHARGE_ROW_ENCODER;
import static com.worldpay.pms.cue.engine.encoder.Encoders.RECURRING_CHARGE_ENCODER;
import static com.worldpay.pms.cue.engine.samples.Transactions.CHBK_PENDING_CHARGE;
import static com.worldpay.pms.cue.engine.samples.Transactions.CHRG_PENDING_CHARGE;
import static com.worldpay.pms.cue.engine.samples.Transactions.INVALID_PENDING_CHARGE_2;
import static com.worldpay.pms.cue.engine.samples.Transactions.INVALID_PENDING_CHARGE_3;
import static com.worldpay.pms.cue.engine.samples.Transactions.INVALID_PENDING_CHARGE_4;
import static com.worldpay.pms.cue.engine.samples.Transactions.INVALID_PENDING_CHARGE_6;
import static com.worldpay.pms.cue.engine.samples.Transactions.INVALID_PENDING_CHARGE_7;
import static com.worldpay.pms.cue.engine.samples.Transactions.INVALID_PENDING_CHARGE_8;
import static com.worldpay.pms.cue.engine.samples.Transactions.INVALID_PENDING_CHARGE_WITH_NULL_FIELDS;
import static com.worldpay.pms.cue.engine.samples.Transactions.RECURRING_CHARGE;
import static com.worldpay.pms.cue.engine.transformations.Charging.mapToMiscellaneousCharge;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;
import com.worldpay.pms.cue.domain.ChargingService;
import com.worldpay.pms.cue.engine.DummyFactory;
import com.worldpay.pms.cue.engine.InMemoryStaticDataRepository;
import com.worldpay.pms.cue.engine.TransactionChargingServiceFactory;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.Batch.Watermark;
import com.worldpay.pms.testing.junit.SparkContext;
import com.worldpay.pms.testing.stereotypes.WithSpark;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.Collections;
import lombok.val;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// TODO refactor such that Validator/Charging class don not need spark in tests
@WithSpark
class ChargingTest {

  private static Broadcast<ChargingService> service;
  private static JavaSparkContext jsc;
  private static BatchId batch;
  private static final Date LOGICAL_DATE = Date.valueOf("2020-09-30");

  @BeforeAll
  static void setUp(JavaSparkContext javaSparkContext) {
    jsc = javaSparkContext;
    InMemoryStaticDataRepository staticData =
        InMemoryStaticDataRepository.builder()
            .chargeTypes(Collections.emptySet())
            .currencyCodes(Sets.newHashSet("GBP", "EUR"))
            .priceItems(Sets.newHashSet("ADHOCCHG", "MIGCHBK"))
            .subAccountTypes(Sets.newHashSet("CHRG", "RECR", "CHBK"))
            .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO"))
            .build();

    batch =
        new BatchId(
            "test-batch",
            new Watermark(
                LocalDateTime.parse("2020-01-01T00:00:00"),
                LocalDateTime.parse("2020-12-31T00:00:00")),
            1);
    service =
        jsc.broadcast(
            new TransactionChargingServiceFactory(new DummyFactory<>(staticData)).build().get());
  }

  static <T> Dataset<T> datasetOf(Encoder<T> encoder, T... rows) {
    return SparkContext.datasetOf(encoder, rows).coalesce(1);
  }

  @Test
  void whenNonTransactionsThenResultingDatasetIsEmpty() {
    val result =
        mapToMiscellaneousCharge(
            SparkContext.emptyDataset(PENDING_BILLABLE_CHARGE_ROW_ENCODER),
            SparkContext.emptyDataset(RECURRING_CHARGE_ENCODER),
            service,
            LOGICAL_DATE);

    assertSuccessAndFailedCounts(result, 0, 0);
  }

  @Test
  void whenMultipleTransactionsThenResultsAreMappedThrough() {
    val result =
        mapToMiscellaneousCharge(
            datasetOf(
                PENDING_BILLABLE_CHARGE_ROW_ENCODER, CHRG_PENDING_CHARGE, CHBK_PENDING_CHARGE),
            datasetOf(RECURRING_CHARGE_ENCODER, RECURRING_CHARGE),
            service,
            LOGICAL_DATE);

    // force execution and assert values
    assertSuccessAndFailedCounts(result, 3, 0);
  }

  @Test
  void whenEmptyStaticDataThenValidAndInvalidTransactionAreMarkedAsFailed() {
    val result =
        mapToMiscellaneousCharge(
            datasetOf(
                PENDING_BILLABLE_CHARGE_ROW_ENCODER, CHRG_PENDING_CHARGE, CHBK_PENDING_CHARGE),
            datasetOf(RECURRING_CHARGE_ENCODER, RECURRING_CHARGE),
            jsc.broadcast(
                new TransactionChargingServiceFactory(
                        new DummyFactory<>(
                            InMemoryStaticDataRepository.builder()
                                .chargeTypes(Collections.emptySet())
                                .currencyCodes(Collections.emptySet())
                                .billPeriodCodes(Collections.emptySet())
                                .priceItems(Collections.emptySet())
                                .subAccountTypes(Collections.emptySet())
                                .build()))
                    .build()
                    .get()),
            LOGICAL_DATE);

    assertSuccessAndFailedCounts(result, 1, 2);
  }

  @Test
  void whenValidAndInvalidTransactionThenSuccessOrFailAccordingly() {
    val result =
        mapToMiscellaneousCharge(
            datasetOf(
                PENDING_BILLABLE_CHARGE_ROW_ENCODER,
                INVALID_PENDING_CHARGE_6,
                INVALID_PENDING_CHARGE_7,
                INVALID_PENDING_CHARGE_8,
                INVALID_PENDING_CHARGE_2,
                INVALID_PENDING_CHARGE_3,
                INVALID_PENDING_CHARGE_4,
                INVALID_PENDING_CHARGE_WITH_NULL_FIELDS,
                CHRG_PENDING_CHARGE),
            datasetOf(RECURRING_CHARGE_ENCODER, RECURRING_CHARGE),
            service,
            LOGICAL_DATE);
    assertSuccessAndFailedCounts(result, 2, 7);
  }

  void assertSuccessAndFailedCounts(
      Dataset<SuccessOrFailureNonRecurring> ds, long success, long failed) {
    ds = ds.cache();
    assertThat(ds.count()).isEqualTo(success + failed);
    assertThat(ds.filter(SuccessOrFailureNonRecurring::isSuccess).count()).isEqualTo(success);
    assertThat(ds.filter(SuccessOrFailureNonRecurring::isFailure).count()).isEqualTo(failed);
  }
}
