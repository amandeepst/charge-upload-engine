package com.worldpay.pms.mdu.engine.integration;

import static com.worldpay.pms.mdu.engine.samples.Transactions.*;
import static com.worldpay.pms.mdu.engine.transformations.PartyTransformationTest.createSubAccountTypeMap;
import static com.worldpay.pms.mdu.engine.utils.DbUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;
import com.worldpay.pms.mdu.domain.model.output.AccountHierarchy;
import com.worldpay.pms.mdu.engine.InMemoryStaticDataRepository;
import com.worldpay.pms.mdu.engine.transformations.ErrorAccountHierarchy;
import com.worldpay.pms.mdu.engine.utils.DbUtils;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.stereotypes.WithSparkHeavyUsage;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@WithSparkHeavyUsage
public class AccountHierarchyFunctionalTest extends EndToEndTestBase {
  // this test class will include test scenarios for account hierarchy

  private static final LocalDateTime ILM_DT = LocalDateTime.of(2021, 3, 15, 0, 2, 30);
  private static final LocalDateTime LOW_WATERMARK = LocalDateTime.of(2021, 3, 15, 0, 0, 0);
  private static final LocalDateTime HIGH_WATERMARK = LocalDateTime.of(2021, 3, 15, 0, 2, 0);
  private static final String[] MERCH_AND_HIER_DS_IDS =
      new String[] {"MERCHANT_DATA", "ACCOUNT_HIERARCHY"};
  private static final InMemoryStaticDataRepository staticRepository =
      InMemoryStaticDataRepository.builder()
          .billCycleCode(Sets.newHashSet("WPDY", "WPMO"))
          .countryCode(Sets.newHashSet("GBR", "USA"))
          .cisDivision(Sets.newHashSet("00001", "00002", "00003"))
          .state(Sets.newHashSet("OT", "OH"))
          .currency(Sets.newHashSet("GBP", "USD", "CAN"))
          .subAccountType(createSubAccountTypeMap("FUND", "CHRG", "CHBK", "CRWD"))
          .build();

  private SqlDb cisadm;

  @Override
  public void bindOrmbJdbcConfiguration(JdbcConfiguration conf) {
    cisadm = SqlDb.simple(conf);
  }

  @BeforeEach
  void cleanUp() {
    DbUtils.cleanUp(cisadm, "cm_acct_stg", "cm_merch_char", "cm_merch_stg", "cm_inv_grp_stg");
  }

  @Test
  void whenNoAccountFoundOnNewAccountHierarchy() {
    insertAccountHierarchiesStage(
        cisadm, ILM_DT, ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(0, 1);
  }

  @Test
  void whenEitherParentOrChildAccountNotFoundOnNewAccountHierarchy() {
    insertCompletedBatch(mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, "MERCHANT_DATA");
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000001_WITH_CANCELLEDFUND_ACCOUNTS,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(0, 1);
  }

  @Test
  void whenNewAccountHierarchy() {
    insertMerchantPartiesWithAccounts(
        cisadm,
        ILM_DT,
        true,
        MERCHANT_PARTY_PO00000002_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000001_WITH_FUND_VALID_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm, ILM_DT, ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(1, 0);
  }

  @Test
  void whenMultipleNewAccountHierarchyWithSameParentAndDifferentChild() {
    // B -> D 21-Jan 31-Jan
    // B -> C 01-Feb 28-Feb
    // op
    // B -> D success
    // B -> C success
    insertMerchantPartiesWithAccounts(
        cisadm,
        ILM_DT,
        true,
        MERCHANT_PARTY_PO00000002_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000003_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000004_WITH_FUND_VALID_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        ILM_DT,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000004);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 0);
    assertAccountHierarchiesWithAttributes(
        false,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000004);
  }

  @Test
  void whenSameNewAccountHierarchyWithSuccessValidity() {
    // A -> B 01-Jan 31-Jan
    // A -> B 01-Feb null
    // op
    // A -> B 01-Jan 31-Jan(success)
    // A -> B 01-Feb null(success)
    insertMerchantPartiesWithAccounts(
        cisadm,
        ILM_DT,
        true,
        MERCHANT_PARTY_PO00000001_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000002_WITH_FUND_VALID_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        ILM_DT,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 0);
    assertAccountHierarchiesWithAttributes(
        false,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY);
  }

  @Test
  void whenMultipleNewAccountHierarchyWithChildComesAsParentAndCrossOverValidity() {
    // A -> B 01-Jan 31-Jan
    // B -> D 21-Jan 31-Jan
    // B -> C 01-Feb 28-Feb
    // op -
    // A -> B(error)
    // B -> C(error)
    // B -> D(error)
    insertMerchantPartiesWithAccounts(
        cisadm,
        ILM_DT,
        true,
        MERCHANT_PARTY_PO00000001_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000002_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000003_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000004_WITH_FUND_VALID_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        ILM_DT,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000004);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(0, 3);
  }

  @Test
  void whenSameNewHierarchyWithCrossOverValidity() {
    // A -> B 01-Jan null
    // A -> B 01-Feb null
    // op
    // A -> B 01-Jan null(error)
    // A -> B 01-Feb null(error)
    insertMerchantPartiesWithAccounts(
        cisadm,
        ILM_DT,
        true,
        MERCHANT_PARTY_PO00000001_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000002_WITH_FUND_VALID_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        ILM_DT,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(0, 2);
  }

  @Test
  void whenMultipleNewAccountHierarchyWithChildComesAsParentWithSuccessValidity() {
    // A -> B 01-Jan 31-Jan
    // B -> C 01-Feb 28-Feb
    // op -
    // A -> B(success)
    // B -> C(success)
    insertMerchantPartiesWithAccounts(
        cisadm,
        ILM_DT,
        true,
        MERCHANT_PARTY_PO00000001_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000002_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000003_WITH_FUND_VALID_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        ILM_DT,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 0);
    assertAccountHierarchiesWithAttributes(
        false,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003);
  }

  @Test
  void whenUpdateWithNoAccountFoundOnForActiveAccountHierarchy() {
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertAccountHierarchies(
        mdu, "test_run", ILM_DT, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
    insertAccountHierarchiesStage(
        cisadm, ILM_DT, ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(1, 1);
    assertAccountHierarchiesWithAttributes(
        true, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
  }

  @Test
  void whenMultipleNewAccountHierarchyWithSameChildAndDifferentParentWithSuccessValidity() {
    // D -> C 21-Jan 31-Jan
    // B -> C 01-Feb 28-Feb
    // op
    // D -> C success
    // B -> C success
    insertMerchantPartiesWithAccounts(
        cisadm,
        ILM_DT,
        true,
        MERCHANT_PARTY_PO00000002_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000003_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000004_WITH_FUND_VALID_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        ILM_DT,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000004_AND_PO00000003);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 0);
    assertAccountHierarchiesWithAttributes(
        false,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000004_AND_PO00000003);
  }

  @Test
  void whenMultipleNewAccountHierarchyWithSameChildAndDifferentParentAndCrossOverValidity() {
    // D -> C 21-Jan 31-Jan
    // B -> C 01-Feb 28-Feb
    // F -> C 25-Jan 25-Feb
    // op
    // D -> C error
    // B -> C error
    // F -> C error
    insertMerchantPartiesWithAccounts(
        cisadm,
        ILM_DT,
        true,
        MERCHANT_PARTY_PO00000006_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000002_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000003_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000004_WITH_FUND_VALID_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        ILM_DT,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000004_AND_PO00000003,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000006_AND_PO00000003);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(0, 3);
    assertErrorAccountHierarchiesWithAttributes(
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000004_AND_PO00000003,
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000006_AND_PO00000003);
  }

  @Test
  void whenMultipleNewAccountHierarchyWithParentComesAsChildAndCrossOverValidity() {
    // C -> E 01-Jan 31-Jan
    // B -> C 01-Feb 28-Feb
    // D -> C 21-Jan 31-Jan
    // op -
    // C -> E(error)
    // B -> C(error)
    // D -> C(error)
    insertMerchantPartiesWithAccounts(
        cisadm,
        ILM_DT,
        true,
        MERCHANT_PARTY_PO00000005_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000002_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000003_WITH_FUND_VALID_ACCOUNTS,
        MERCHANT_PARTY_PO00000004_WITH_FUND_VALID_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        ILM_DT,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000003_AND_PO00000005,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000004_AND_PO00000003);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(0, 3);
    assertErrorAccountHierarchiesWithAttributes(
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000004_AND_PO00000003,
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000005);
  }

  @Test
  void whenNoUpdateForActiveAccountHierarchy() {
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertAccountHierarchies(
        mdu, "test_run", ILM_DT, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000001_WITH_FUND_ACCOUNTS,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(1, 0);
    assertAccountHierarchiesWithAttributes(
        true, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
  }

  @Test
  void whenUpdateWithEitherParentOrChildAccountNotFoundForActiveAccountHierarchy() {
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertPartiesWithAccounts(mdu, "test_run", ILM_DT, PARTY_PO00000001_WITH_FUND_ACCOUNTS);
    insertAccountHierarchies(
        mdu, "test_run", ILM_DT, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(1, 1);
    assertAccountHierarchiesWithAttributes(
        true, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
  }

  @Test
  void whenPartialValidUpdateForActiveAccountHierarchyWithSamePartyAndDifferentChild() {
    // current B -> C 01-Feb 28-Feb
    // update  B -> C 01-Mar null
    // update  B -> D 21-Jan 31-Jan error
    // op
    // B -> C 01-Feb 28-Feb
    // B -> C 01-Mar null
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertAccountHierarchies(
        mdu, "test_run", ILM_DT, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000003_WITH_FUND_ACCOUNTS,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000004,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003_UPDATE);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 1);
    assertErrorAccountHierarchiesWithAttributes(
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000004);
    assertAccountHierarchiesWithAttributes(
        false,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003_AND_NEW_VALIDITY);
  }

  @Test
  void whenSameAccountHierarchyUpdateForActiveHierarchyWithSuccessValidity() {
    // current A -> B 01-Jan 31-Jan
    // update A -> B 01-Feb null
    // op
    // A -> B 01-Jan 31-Jan(success)
    // A -> B 01-Feb null(success)
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertAccountHierarchies(
        mdu, "test_run", ILM_DT, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000001_WITH_FUND_ACCOUNTS,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 0);
    assertAccountHierarchiesWithAttributes(
        false,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY);
  }

  @Test
  void whenSameAccountHierarchyUpdateForActiveHierarchyWithSameAndDifferentValidFrom() {
    // current A -> B 01-Jan null
    // update A -> B 01-Jan 31-Jan
    // update A -> B 01-Feb null
    // op
    // A -> B 01-Jan 31-Jan(updated)
    // A -> B 01-Feb null(new)
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertAccountHierarchies(
        mdu,
        "test_run",
        ILM_DT,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000001_WITH_FUND_ACCOUNTS,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_JAN_VALIDITY_EXPIRED,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 0);
    assertAccountHierarchiesWithAttributes(
        false,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATED_AND_JAN_VALIDITY_EXPIRED,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY);
  }

  @Test
  void whenMultipleSameAccountHierarchyAndUpdateForLatestActiveHierarchyWithSameValidFrom() {
    // current A -> B 01-Jan 31-Jan
    // current A -> B 01-Feb null
    // update A -> B 01-Feb 28-Feb
    // op
    // A -> B 01-Jan 31-Jan(success)
    // A -> B 01-Feb 28-Feb(updated)
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertAccountHierarchies(
        mdu,
        "test_run",
        ILM_DT,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000001_WITH_FUND_ACCOUNTS,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_FEB_VALIDITY_EXPIRED);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 0);
    assertAccountHierarchiesWithAttributes(
        true,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATED_AND_FEB_VALIDITY_EXPIRED);
  }

  @Test
  void
      whenMultipleSameAccountHierarchyAndMultipleUpdateForLatestActiveHierarchyWithCrossOverValidity() {
    // current A -> B 01-Jan 31-Jan
    // current A -> B 01-Feb null
    // update A -> B 21-Jan 31-Jan
    // update A -> B 01-Feb 28-Feb
    // op
    // current A -> B 01-Jan 31-Jan(success)
    // current A -> B 01-Feb null(success)
    // update A -> B 21-Jan 31-Jan(error)
    // update A -> B 01-Feb 28-Feb(error)
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertAccountHierarchies(
        mdu,
        "test_run",
        ILM_DT,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000001_WITH_FUND_ACCOUNTS,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CROSS_OVER_VALIDITY,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_FEB_VALIDITY_EXPIRED);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 2);
    assertErrorAccountHierarchiesWithAttributes(
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CROSS_OVER_VALIDITY);
    assertAccountHierarchiesWithAttributes(
        true,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY);
  }

  @Test
  void whenUpdateForActiveAccountHierarchyWithChildComesAsParentAndSuccessValidity() {
    // current A -> B 01-Jan-21 31-Jan-21
    // update B -> C 01-Feb-21 null
    // op
    // A -> B success
    // B -> C success
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertAccountHierarchies(
        mdu, "test_run", ILM_DT, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000001_WITH_FUND_ACCOUNTS,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS,
        PARTY_PO00000003_WITH_FUND_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 0);
    assertAccountHierarchiesWithAttributes(
        false,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003);
  }

  @Test
  void whenUpdateForActiveAccountHierarchyWithChildComesAsParentAndCrossOverValidity() {
    // current - A -> B 01-Jan 31-Jan
    // current - G -> B 01-Feb 28-Feb
    // update - B -> C 01-Feb 28-Feb
    // update - B -> D 21-Jan 31-Jan
    // op
    // A -> B success
    // G -> B success
    // B -> C error
    // B -> D error
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertAccountHierarchies(
        mdu,
        "test_run",
        ILM_DT,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000007_AND_PO00000002);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000004_WITH_FUND_ACCOUNTS,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS,
        PARTY_PO00000003_WITH_FUND_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000004);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 2);
    assertErrorAccountHierarchiesWithAttributes(
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000004);
    assertAccountHierarchiesWithAttributes(
        // check rolled over hierarchy has same cre_dttm
        true,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000007_AND_PO00000002);
  }

  @Test
  void whenUpdateForActiveAccountHierarchyWithParentComesAsChildWithSuccessValidity() {
    // current - C -> E 01-Jan 31-Jan
    // update - B -> C 01-Feb 28-Feb
    // op
    // C -> E success
    // B -> C success
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertAccountHierarchies(
        mdu, "test_run", ILM_DT, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000005);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000005_WITH_FUND_ACCOUNTS,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS,
        PARTY_PO00000003_WITH_FUND_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 0);
    assertAccountHierarchiesWithAttributes(
        false,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000005);
  }

  @Test
  void whenUpdateForActiveAccountHierarchyWithParentComesAsChildAndCrossOverValidity() {
    // current - C -> E 01-Jan 31-Jan
    // current - C -> H 01-Feb 28-Feb
    // update - B -> C 01-Feb 28-Feb
    // update - D -> C 21-Jan 31-Jan
    // op
    // C -> E success
    // C -> H success
    // D -> C error
    // B -> C error
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertAccountHierarchies(
        mdu,
        "test_run",
        ILM_DT,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000005,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000008);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000004_WITH_FUND_ACCOUNTS,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS,
        PARTY_PO00000003_WITH_FUND_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000004_AND_PO00000003);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 2);
    assertErrorAccountHierarchiesWithAttributes(
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000004_AND_PO00000003);
    assertAccountHierarchiesWithAttributes(
        // check rolled over hierarchy has same cre_dttm
        true,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000005,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000008);
  }

  @Test
  void whenUpdateForActiveAccountHierarchyWithSameChildButDifferentParentAndCrossOverValidity() {
    // current - F -> C 25-Jan 25-Feb
    // update - D -> C 21-Jan 31-Jan
    // update - B -> C 01-Feb 28-Feb
    // op
    // F -> C success
    // D -> C error
    // B -> C error
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertAccountHierarchies(
        mdu, "test_run", ILM_DT, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000006_AND_PO00000003);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000004_WITH_FUND_ACCOUNTS,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS,
        PARTY_PO00000003_WITH_FUND_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000004_AND_PO00000003);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(1, 2);
    assertErrorAccountHierarchiesWithAttributes(
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000004_AND_PO00000003);
    assertAccountHierarchiesWithAttributes(
        // check rolled over hierarchy has same cre_dttm
        true, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000006_AND_PO00000003);
  }

  @Test
  void whenTwoLevelHierarchyExistBetweenActiveHierarchyAndUpdatesAndCrossOverValidity() {
    // current A -> B 01-Jan 31-Jan
    // current D -> C 21-Jan 31-Jan
    // update B -> D 21-Jan 31-Jan
    // op
    // A -> B 01-Jan 31-Jan success
    // D -> C 21-Jan 31-Jan success
    // B -> D 21-Jan 31-Jan error
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertAccountHierarchies(
        mdu,
        "test_run",
        ILM_DT,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000004_AND_PO00000003);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000004_WITH_FUND_ACCOUNTS,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS,
        PARTY_PO00000003_WITH_FUND_ACCOUNTS,
        PARTY_PO00000001_WITH_FUND_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000004);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 1);
    assertErrorAccountHierarchiesWithAttributes(
        ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000004);
    assertAccountHierarchiesWithAttributes(
        // check rolled over hierarchy has same cre_dttm
        true,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000004_AND_PO00000003);
  }

  @Test
  void whenMultipleUpdatesWithAndWithoutCancellationFlag() {
    // update A -> B 01-Jan 31-Jan can_flg Y
    // update A -> B 01-Feb 28-Feb can_flg Y
    // update A -> B 01-Jan null can_flg N
    // op
    // update A -> B 01-Jan 31-Jan error
    // update A -> B 01-Feb 28-Feb error
    // update A -> B 01-Jan null success
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS,
        PARTY_PO00000001_WITH_FUND_ACCOUNTS);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CANCEL_FLG,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_WITH_FEB_VALIDITY_AND_CANCEL_FLG,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(1, 2);
    assertAccountHierarchiesWithAttributes(
        // check rolled over hierarchy has same cre_dttm
        false, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL);
  }

  @Test
  void whenActiveUpdateComesForCancelledHierarchy() {
    // current A -> B 01-Jan 31-Jan inactive
    // update A -> B 01-Feb null can_flg Y
    // op
    // current A -> B 01-Jan 31-Jan inactive
    // update A -> B 01-Feb null success-new
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS,
        PARTY_PO00000001_WITH_FUND_ACCOUNTS);
    insertAccountHierarchies(
        mdu,
        "test_run",
        ILM_DT,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_JAN_CANCELLED);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 0);
    assertAccountHierarchiesWithAttributes(
        // check rolled over hierarchy has same cre_dttm
        false,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_JAN_CANCELLED,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY);
  }

  @Test
  void whenMultipleOverlappingUpdatesForMultipleActiveHierarchy() {
    // current A -> B 01-Jan 31-Jan
    // current A -> B 01-Feb null
    // update A -> B 01-Jan 31-Jan can_flg Y
    // update A -> B 01-Feb 28-Feb can_flg Y
    // op
    // current A -> B 01-Jan 31-Jan inactive
    // current A -> B 01-Feb 28-Feb inactive
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS,
        PARTY_PO00000001_WITH_FUND_ACCOUNTS);
    insertAccountHierarchies(
        mdu,
        "test_run",
        ILM_DT,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CANCEL_FLG,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_WITH_FEB_VALIDITY_AND_CANCEL_FLG);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 0);
    assertAccountHierarchiesWithAttributes(
        // check rolled over hierarchy has same cre_dttm
        false,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_JAN_CANCELLED,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_FEB_CANCELLED);
  }

  @Test
  void whenActiveHierarchyIsCancelledAndNewlyCreatedInSameRun() {
    // current A -> B 01-Jan null
    // update A -> B 01-Jan 31-Jan can_flg Y
    // update A -> B 01-Feb null can_flg N
    // op
    // current A -> B 01-Jan 31-Jan inactive
    // update A -> B 01-Feb null success-new
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS,
        PARTY_PO00000001_WITH_FUND_ACCOUNTS);
    insertAccountHierarchies(
        mdu, "test_run", ILM_DT, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CANCEL_FLG,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 0);
    assertAccountHierarchiesWithAttributes(
        // check rolled over hierarchy has same cre_dttm
        false,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_JAN_CANCELLED,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY);
  }

  @Test
  void whenActiveHierarchyIsCancelledAndChildOfHierarchyIsStampedAsParentOfNewHierarchy() {
    // current A -> B 01-Jan null
    // update A -> B 01-Jan 31-Jan can_flg Y
    // update B -> C 01-Feb null
    // op
    // current A -> B 01-Jan 31-Jan inactive
    // update B -> C 01-Feb null success
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS,
        PARTY_PO00000001_WITH_FUND_ACCOUNTS,
        PARTY_PO00000003_WITH_FUND_ACCOUNTS);
    insertAccountHierarchies(
        mdu, "test_run", ILM_DT, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CANCEL_FLG,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 0);
    assertAccountHierarchiesWithAttributes(
        // check rolled over hierarchy has same cre_dttm
        false,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_JAN_CANCELLED,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003);
  }

  @Test
  void whenHierarchyIsUpdatedAndChildIsMovedToDifferentParentInSingleRun() {
    // current A -> B 01-Jan null
    // update A -> B 01-Jan 31-Jan
    // update C -> B 01-Feb 28-Feb
    // op
    // A -> B 01-Jan 31-Jan(success)
    // C -> B 01-Feb 28-Feb(success)
    insertCompletedBatch(
        mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCH_AND_HIER_DS_IDS);
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000002_WITH_FUND_ACCOUNTS,
        PARTY_PO00000001_WITH_FUND_ACCOUNTS,
        PARTY_PO00000003_WITH_FUND_ACCOUNTS);
    insertAccountHierarchies(
        mdu,
        "test_run",
        ILM_DT,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL);
    insertAccountHierarchiesStage(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_JAN_VALIDITY_EXPIRED,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000003_AND_PO00000002);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertAccountHierarchyAndErrorCount(2, 0);
    assertAccountHierarchiesWithAttributes(
        // check rolled over hierarchy has same cre_dttm
        false,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATED_AND_JAN_VALIDITY_EXPIRED,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000002);
  }

  private void assertAccountHierarchyAndErrorCount(int accountHierarchies, int errors) {
    assertThat(readAccountHierarchyErrors(null)).hasSize(errors);
    assertThat(readAccountHierarchy(null)).hasSize(accountHierarchies);
  }

  private void assertAccountHierarchiesWithAttributes(
      boolean isUpdated, AccountHierarchy... accountHierarchies) {
    for (AccountHierarchy accountHierarchy : accountHierarchies) {
      assertThat(countAccountHierarchyByAttributes(mdu, accountHierarchy, isUpdated)).isEqualTo(1);
    }
  }

  private void assertErrorAccountHierarchiesWithAttributes(
      ErrorAccountHierarchy... errorAccountHierarchies) {
    for (ErrorAccountHierarchy errorAccountHierarchy : errorAccountHierarchies) {
      assertThat(countErrorAccountHierarchyByAttributes(mdu, errorAccountHierarchy)).isEqualTo(1);
    }
  }
}
