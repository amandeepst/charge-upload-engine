package com.worldpay.pms.mdu.engine;

import static com.worldpay.pms.mdu.engine.encoder.Encoders.*;
import static com.worldpay.pms.mdu.engine.encoder.InputEncoders.ACCOUNT_HIERARCHY_DATA_ROW_ENCODER;
import static com.worldpay.pms.mdu.engine.encoder.InputEncoders.MERCHANT_DATA_ENCODER;
import static com.worldpay.pms.mdu.engine.samples.Transactions.*;
import static com.worldpay.pms.mdu.engine.transformations.PartyTransformationTest.createSubAccountTypeMap;
import static com.worldpay.pms.mdu.engine.utils.DbUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;
import com.worldpay.pms.mdu.domain.model.output.AccountHierarchy;
import com.worldpay.pms.mdu.domain.model.output.Party;
import com.worldpay.pms.mdu.engine.batch.MerchantUploadBatchRunResult;
import com.worldpay.pms.mdu.engine.transformations.ErrorAccountHierarchy;
import com.worldpay.pms.mdu.engine.transformations.ErrorTransaction;
import com.worldpay.pms.mdu.engine.transformations.model.input.AccountHierarchyDataRow;
import com.worldpay.pms.mdu.engine.transformations.model.input.MerchantDataRow;
import com.worldpay.pms.mdu.engine.transformations.writers.InMemoryErrorAccountHierarchyWriter;
import com.worldpay.pms.mdu.engine.transformations.writers.InMemoryErrorWriter;
import com.worldpay.pms.mdu.engine.transformations.writers.InMemoryUpsertWriter;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.Batch.Watermark;
import com.worldpay.pms.testing.stereotypes.WithSparkHeavyUsage;
import com.worldpay.pms.testing.utils.InMemoryDataSource;
import java.time.LocalDateTime;
import java.util.Collections;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@WithSparkHeavyUsage
public class DriverTest {

  private SparkSession spark;
  private InMemoryDataSource.Builder<MerchantDataRow> merchantDataRowBuilder;
  private InMemoryDataSource.Builder<AccountHierarchyDataRow> accountHierarchyDataRowBuilder;
  private InMemoryDataSource.Builder<Party> currentPartyBuilder;
  private InMemoryDataSource.Builder<AccountHierarchy> currentAccountHierarchyBuilder;
  private InMemoryStaticDataRepository.InMemoryStaticDataRepositoryBuilder staticData;
  private InMemoryUpsertWriter<Party> partyWriter;
  private InMemoryUpsertWriter<AccountHierarchy> accountHierarchyWriter;
  private InMemoryErrorWriter<ErrorTransaction> errorWriter;
  private InMemoryErrorAccountHierarchyWriter<ErrorAccountHierarchy> errorAccountHierarchyWriter;

  @BeforeEach
  void setup(SparkSession spark) {
    this.spark = spark;
    merchantDataRowBuilder = InMemoryDataSource.builder(MERCHANT_DATA_ENCODER);
    accountHierarchyDataRowBuilder = InMemoryDataSource.builder(ACCOUNT_HIERARCHY_DATA_ROW_ENCODER);
    currentAccountHierarchyBuilder = InMemoryDataSource.builder(ACCOUNT_HIERARCHY_ENCODER);
    currentPartyBuilder = InMemoryDataSource.builder(PARTY_ENCODER);
    staticData = InMemoryStaticDataRepository.builder();
    partyWriter = new InMemoryUpsertWriter<>();
    accountHierarchyWriter = new InMemoryUpsertWriter<>();
    errorWriter = new InMemoryErrorWriter<>();
    errorAccountHierarchyWriter = new InMemoryErrorAccountHierarchyWriter<>();
  }

  @Test
  void whenNoUpdateThenNoOutput() {
    staticData
        .billCycleCode(Collections.emptySet())
        .cisDivision(Collections.emptySet())
        .countryCode(Collections.emptySet())
        .state(Collections.emptySet())
        .currency(Collections.emptySet())
        .subAccountType(Collections.emptyMap());

    MerchantUploadBatchRunResult merchantUploadBatchRunResult =
        createDriverAndRun(
            merchantDataRowBuilder.build(),
            currentPartyBuilder.build(),
            accountHierarchyDataRowBuilder.build(),
            currentAccountHierarchyBuilder.build());
    assertTransactionsCount(merchantUploadBatchRunResult, 0, 0, 0, 0);
  }

  @Test
  void whenNoStaticDataThenValidUpdateFails() {
    staticData
        .billCycleCode(Collections.emptySet())
        .cisDivision(Collections.emptySet())
        .countryCode(Collections.emptySet())
        .state(Collections.emptySet())
        .currency(Collections.emptySet())
        .subAccountType(Collections.emptyMap());

    merchantDataRowBuilder.addRows(
        MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_FUND,
        MERCHANT_DATA_WITH_PARTY_PO00000002_AND_ACCOUNT_FUND);

    accountHierarchyDataRowBuilder.addRows(
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000003_AND_PO00000005);

    MerchantUploadBatchRunResult merchantUploadBatchRunResult =
        createDriverAndRun(
            merchantDataRowBuilder.build(),
            currentPartyBuilder.build(),
            accountHierarchyDataRowBuilder.build(),
            currentAccountHierarchyBuilder.build());
    assertTransactionsCount(merchantUploadBatchRunResult, 0, 4, 0, 0);
  }

  @Test
  void whenNewValidUpdatesUpdatesForPartyAndAccountHierarchy() {
    staticData
        .billCycleCode(Sets.newHashSet("WPDY", "WPMO"))
        .cisDivision(Sets.newHashSet("00001", "00015"))
        .countryCode(Sets.newHashSet("GBR", "USA"))
        .state(Sets.newHashSet("OT", "OH"))
        .currency(Sets.newHashSet("GBP", "USD", "CAN"))
        .subAccountType(createSubAccountTypeMap("FUND", "CHRG", "CHBK", "CRWD"));

    merchantDataRowBuilder.addRows(
        MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_FUND,
        MERCHANT_DATA_WITH_PARTY_PO00000002_AND_ACCOUNT_FUND_WITH_GBP);

    accountHierarchyDataRowBuilder.addRows(
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002);

    MerchantUploadBatchRunResult merchantUploadBatchRunResult =
        createDriverAndRun(
            merchantDataRowBuilder.build(),
            currentPartyBuilder.build(),
            accountHierarchyDataRowBuilder.build(),
            currentAccountHierarchyBuilder.build());
    assertTransactionsCount(merchantUploadBatchRunResult, 3, 0, 2, 1);
  }

  @Test
  void whenNewMixedValidAndInvalidUpdateThenMoveToSuccessOrErrorAccordingly() {
    staticData
        .billCycleCode(Sets.newHashSet("WPDY", "WPMO"))
        .cisDivision(Sets.newHashSet("00001", "00015", "00002"))
        .countryCode(Sets.newHashSet("GBR", "USA"))
        .state(Sets.newHashSet("OT", "OH"))
        .currency(Sets.newHashSet("GBP", "USD", "CAN"))
        .subAccountType(createSubAccountTypeMap("FUND", "CHRG", "CHBK", "CRWD"));

    merchantDataRowBuilder.addRows(
        MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_FUND,
        MERCHANT_DATA_WITH_PARTY_PO00000002_AND_ACCOUNT_FUND_WITH_GBP,
        INVALID_COUNTRY_MERCHANT_DATA,
        INVALID_STATE_AND_DIVISION_MERCHANT_DATA,
        MERCHANT_DATA_WITH_BUSINESS_PARTY_PO00000003_AND_NO_ACCOUNT,
        // same party with different division will be reduced to one party
        MERCHANT_DATA_WITH_GROUPED_PARTY_PO00000004_AND_DIVSION_00001,
        MERCHANT_DATA_WITH_GROUPED_PARTY_PO00000004_AND_DIVSION_00002);

    accountHierarchyDataRowBuilder.addRows(
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000003_AND_PO00000005);

    MerchantUploadBatchRunResult merchantUploadBatchRunResult =
        createDriverAndRun(
            merchantDataRowBuilder.build(),
            currentPartyBuilder.build(),
            accountHierarchyDataRowBuilder.build(),
            currentAccountHierarchyBuilder.build());
    assertTransactionsCount(merchantUploadBatchRunResult, 5, 3, 4, 1);
  }

  @Test
  void whenNoUpdateForActivePartyAndAccountHierarchy() {
    staticData
        .billCycleCode(Sets.newHashSet("WPDY", "WPMO"))
        .cisDivision(Sets.newHashSet("00001", "00015"))
        .countryCode(Sets.newHashSet("GBR", "USA"))
        .state(Sets.newHashSet("OT", "OH"))
        .currency(Sets.newHashSet("GBP", "USD", "CAN"))
        .subAccountType(createSubAccountTypeMap("FUND", "CHRG", "CHBK", "CRWD"));

    currentPartyBuilder.addRows(PARTY_PO00000001_WITH_ALL_ACCOUNTS);
    currentAccountHierarchyBuilder.addRows(
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
    MerchantUploadBatchRunResult merchantUploadBatchRunResult =
        createDriverAndRun(
            merchantDataRowBuilder.build(),
            currentPartyBuilder.build(),
            accountHierarchyDataRowBuilder.build(),
            currentAccountHierarchyBuilder.build());
    assertTransactionsCount(merchantUploadBatchRunResult, 0, 0, 1, 1);
  }

  @Test
  void whenPartialUpdateForCurrentParty() {
    staticData
        .billCycleCode(Sets.newHashSet("WPDY", "WPMO"))
        .cisDivision(Sets.newHashSet("00001", "00015", "00002"))
        .countryCode(Sets.newHashSet("GBR", "USA"))
        .state(Sets.newHashSet("OT", "OH"))
        .currency(Sets.newHashSet("GBP", "USD", "CAN"))
        .subAccountType(createSubAccountTypeMap("FUND", "CHRG", "CHBK", "CRWD"));

    currentPartyBuilder.addRows(PARTY_PO00000001_WITH_ALL_ACCOUNTS);
    merchantDataRowBuilder.addRows(
        MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_FUND,
        MERCHANT_DATA_WITH_PARTY_PO00000001_AND_INVALID_CHRG_ACCOUNT,
        MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_CHBK,
        MERCHANT_DATA_WITH_PARTY_PO00000001_AND_INVALID_CRWD_ACCOUNT);

    MerchantUploadBatchRunResult merchantUploadBatchRunResult =
        createDriverAndRun(
            merchantDataRowBuilder.build(),
            currentPartyBuilder.build(),
            accountHierarchyDataRowBuilder.build(),
            currentAccountHierarchyBuilder.build());
    // partial update failure at party is not accounted in partyCount
    assertTransactionsCount(merchantUploadBatchRunResult, 0, 1, 1, 0);
  }

  @Test
  void whenMultipleUpdateForMultipleActiveHierarchyWithCrossOverValidity() {
    // current A -> B 01-Jan 31-Jan
    // current A -> B 01-Feb null
    // update A -> B 21-Jan 31-Jan
    // update A -> B 01-Feb 28-Feb
    // op
    // current A -> B 01-Jan 31-Jan(success)
    // current A -> B 01-Feb 28-Feb(success)
    // update A -> B 21-Jan 31-Jan(error)
    // update A -> B 01-Feb 28-Feb(error)
    staticData
        .billCycleCode(Sets.newHashSet("WPDY", "WPMO"))
        .cisDivision(Sets.newHashSet("00001", "00015", "00002"))
        .countryCode(Sets.newHashSet("GBR", "USA"))
        .state(Sets.newHashSet("OT", "OH"))
        .currency(Sets.newHashSet("GBP", "USD", "CAN"))
        .subAccountType(createSubAccountTypeMap("FUND", "CHRG", "CHBK", "CRWD"));

    currentPartyBuilder.addRows(
        PARTY_PO00000001_WITH_FUND_ACCOUNTS, PARTY_PO00000002_WITH_FUND_ACCOUNTS);
    currentAccountHierarchyBuilder.addRows(
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY);
    accountHierarchyDataRowBuilder.addRows(
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CROSS_OVER_VALIDITY,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_FEB_VALIDITY_EXPIRED);

    MerchantUploadBatchRunResult merchantUploadBatchRunResult =
        createDriverAndRun(
            merchantDataRowBuilder.build(),
            currentPartyBuilder.build(),
            accountHierarchyDataRowBuilder.build(),
            currentAccountHierarchyBuilder.build());
    assertTransactionsCount(merchantUploadBatchRunResult, 0, 2, 2, 2);
  }

  @Test
  void whenValidAndInvalidUpdatesForActivePartyAndAccountHierarchy() {
    staticData
        .billCycleCode(Sets.newHashSet("WPDY", "WPMO"))
        .cisDivision(Sets.newHashSet("00001", "00015"))
        .countryCode(Sets.newHashSet("GBR", "USA"))
        .state(Sets.newHashSet("OT", "OH"))
        .currency(Sets.newHashSet("GBP", "USD", "CAN"))
        .subAccountType(createSubAccountTypeMap("FUND", "CHRG", "CHBK", "CRWD"));

    currentPartyBuilder.addRows(
        PARTY_PO00000001_WITH_FUND_ACCOUNTS, PARTY_PO00000002_WITH_FUND_ACCOUNTS);
    merchantDataRowBuilder.addRows(
        INVALID_COUNTRY_MERCHANT_DATA, MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_FUND);

    currentAccountHierarchyBuilder.addRows(
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000005);
    accountHierarchyDataRowBuilder.addRows(
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_JAN_VALIDITY_EXPIRED,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000003_AND_PO00000005);

    MerchantUploadBatchRunResult merchantUploadBatchRunResult =
        createDriverAndRun(
            merchantDataRowBuilder.build(),
            currentPartyBuilder.build(),
            accountHierarchyDataRowBuilder.build(),
            currentAccountHierarchyBuilder.build());
    assertTransactionsCount(merchantUploadBatchRunResult, 2, 2, 2, 2);
  }

  @Test
  void whenActiveHierarchyIsCancelledAndChildOfHierarchyIsStampedAsParentOfNewHierarchy() {
    staticData
        .billCycleCode(Sets.newHashSet("WPDY", "WPMO"))
        .cisDivision(Sets.newHashSet("00001", "00015"))
        .countryCode(Sets.newHashSet("GBR", "USA"))
        .state(Sets.newHashSet("OT", "OH"))
        .currency(Sets.newHashSet("GBP", "USD", "CAN"))
        .subAccountType(createSubAccountTypeMap("FUND", "CHRG", "CHBK", "CRWD"));

    currentPartyBuilder.addRows(
        PARTY_PO00000001_WITH_FUND_ACCOUNTS,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS,
        PARTY_PO00000003_WITH_FUND_ACCOUNTS);

    currentAccountHierarchyBuilder.addRows(
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
    accountHierarchyDataRowBuilder.addRows(
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CANCEL_FLG,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003);

    MerchantUploadBatchRunResult merchantUploadBatchRunResult =
        createDriverAndRun(
            merchantDataRowBuilder.build(),
            currentPartyBuilder.build(),
            accountHierarchyDataRowBuilder.build(),
            currentAccountHierarchyBuilder.build());
    assertTransactionsCount(merchantUploadBatchRunResult, 2, 0, 3, 2);
  }

  MerchantUploadBatchRunResult createDriverAndRun(
      InMemoryDataSource<MerchantDataRow> merchantDataDataSource,
      InMemoryDataSource<Party> partySource,
      InMemoryDataSource<AccountHierarchyDataRow> accountHierarchyDataSource,
      InMemoryDataSource<AccountHierarchy> currentAccountHierarchyDataSource) {
    Driver driver =
        new Driver(
            spark,
            merchantDataDataSource,
            partySource,
            accountHierarchyDataSource,
            currentAccountHierarchyDataSource,
            partyWriter,
            errorWriter,
            accountHierarchyWriter,
            errorAccountHierarchyWriter,
            new MerchantDataProcessingFactory(new DummyFactory<>(staticData.build())));
    return driver.run(getGenericBatch());
  }

  Batch getGenericBatch() {
    Watermark watermark = new Watermark(LocalDateTime.MIN, LocalDateTime.MAX);
    BatchId id = new BatchId(watermark, 1);
    LocalDateTime createdAt = LocalDateTime.now();

    return new Batch(id, null, createdAt, "", null);
  }

  private void assertTransactionsCount(
      MerchantUploadBatchRunResult result,
      long success,
      long error,
      long totalPartyCount,
      long totalAccountHierarchyCount) {
    assertThat(result.getSuccessTransactionsCount()).isEqualTo(success);
    assertThat(result.getTotalPartyCount()).isEqualTo(totalPartyCount);
    assertThat(result.getTotalAccountHierarchyCount()).isEqualTo(totalAccountHierarchyCount);
    assertThat(result.getErrorTransactionsCount()).isEqualTo(error);
  }
}
