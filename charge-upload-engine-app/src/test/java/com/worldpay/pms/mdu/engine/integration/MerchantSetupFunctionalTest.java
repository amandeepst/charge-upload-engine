package com.worldpay.pms.mdu.engine.integration;

import static com.worldpay.pms.mdu.engine.samples.Transactions.*;
import static com.worldpay.pms.mdu.engine.transformations.PartyTransformationTest.createSubAccountTypeMap;
import static com.worldpay.pms.mdu.engine.utils.DbUtils.*;
import static java.util.Objects.nonNull;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;
import com.worldpay.pms.mdu.domain.model.output.Account;
import com.worldpay.pms.mdu.domain.model.output.Party;
import com.worldpay.pms.mdu.domain.model.output.SubAccount;
import com.worldpay.pms.mdu.domain.model.output.WithholdFund;
import com.worldpay.pms.mdu.engine.InMemoryStaticDataRepository;
import com.worldpay.pms.mdu.engine.utils.DbUtils;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.stereotypes.WithSparkHeavyUsage;
import io.vavr.collection.List;
import java.time.LocalDateTime;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@WithSparkHeavyUsage
public class MerchantSetupFunctionalTest extends EndToEndTestBase {

  private static final LocalDateTime ILM_DT = LocalDateTime.of(2021, 3, 15, 0, 2, 30);
  private static final LocalDateTime LOW_WATERMARK = LocalDateTime.of(2021, 3, 15, 0, 0, 0);
  private static final LocalDateTime HIGH_WATERMARK = LocalDateTime.of(2021, 3, 15, 0, 2, 0);
  private static final String SUB_ACCOUNT_AND_WAF_CONDITION =
      "acct_id in (SELECT acct_id from vw_acct)";

  private static final InMemoryStaticDataRepository staticRepository =
      InMemoryStaticDataRepository.builder()
          .billCycleCode(Sets.newHashSet("WPDY", "WPMO"))
          .countryCode(Sets.newHashSet("GBR", "USA"))
          .cisDivision(Sets.newHashSet("00001", "00002", "00003"))
          .state(Sets.newHashSet("OT", "OH"))
          .currency(Sets.newHashSet("GBP", "USD", "CAN"))
          .subAccountType(createSubAccountTypeMap("FUND", "CHRG", "CHBK", "CRWD"))
          .build();
  private static final InMemoryStaticDataRepository emptyStaticRepository =
      InMemoryStaticDataRepository.builder()
          .billCycleCode(Collections.emptySet())
          .cisDivision(Collections.emptySet())
          .countryCode(Collections.emptySet())
          .state(Collections.emptySet())
          .currency(Collections.emptySet())
          .subAccountType(Collections.emptyMap())
          .build();
  private static final InMemoryStaticDataRepository newSubAccountStaticRepository =
      InMemoryStaticDataRepository.builder()
          .billCycleCode(Sets.newHashSet("WPDY", "WPMO"))
          .countryCode(Sets.newHashSet("GBR", "USA"))
          .cisDivision(Sets.newHashSet("00001", "00002", "00003"))
          .state(Sets.newHashSet("OT", "OH"))
          .currency(Sets.newHashSet("GBP", "USD", "CAN"))
          .subAccountType(createSubAccountTypeMap("FUND~DRES", "CHRG", "CHBK", "CRWD"))
          .build();

  private SqlDb cisadm;

  @Override
  public void bindOrmbJdbcConfiguration(JdbcConfiguration conf) {
    cisadm = SqlDb.simple(conf);
  }

  @BeforeEach
  void cleanUp() {
    DbUtils.cleanUp(cisadm, "cm_acct_stg", "cm_merch_char", "cm_merch_stg");
  }

  @Test
  void whenNewValidPartyAndAccountsButNoStaticData() {
    insertMerchantPartiesWithAccounts(
        cisadm, ILM_DT, true, MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS);

    run(
        emptyStaticRepository,
        "submit",
        "--force",
        "--set-high-watermark-to",
        "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(0, 0, 0, 0, 1);
  }

  @Test
  void whenNewValidPartyWithoutAccounts() {
    insertMerchantPartiesWithAccounts(cisadm, ILM_DT, true, MERCHANT_PARTY_PO00000001);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 0, 0, 0, 0);
  }

  @Test
  void whenNewValidPartyAndAllInvalidAccounts() {
    insertMerchantPartiesWithAccounts(
        cisadm, ILM_DT, true, MERCHANT_PARTY_PO00000001_WITH_ALL_INVALID_ACCOUNTS);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 0, 0, 0, 1);
  }

  @Test
  void whenNewInvalidPartyAndValidAccounts() {
    insertMerchantPartiesWithAccounts(
        cisadm, ILM_DT, true, INVALID_STATE_AND_DIVISION_MERCHANT_PARTY);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(0, 0, 0, 0, 1);
  }

  @Test
  void whenNewValidPartyAndAccountsAreFederatedWithOtherExistingParty() {
    insertCompletedBatch(mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, "MERCHANT_DATA");
    insertPartiesWithAccounts(mdu, "test_run", ILM_DT, PARTY_PO00000002_WITH_ALL_ACCOUNTS);

    insertMerchantPartiesWithAccounts(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        true,
        MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(2, 8, 8, 2, 0);
  }

  @Test
  void whenNewValidPartyWithPartialValidAccounts() {
    insertMerchantPartiesWithAccounts(
        cisadm,
        ILM_DT,
        true,
        MERCHANT_PARTY_PO00000001_WITH_VALID_FUND_CHBK_AND_INVALID_CHRG_CHBK_ACCOUNT);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 2, 2, 1, 1);
  }

  @Test
  void whenMultipleNewPartialValidPartyWithSamePartyId() {
    insertMerchantPartiesWithAccounts(
        cisadm,
        ILM_DT,
        true,
        MERCHANT_PARTY_PO00000001,
        INVALID_COUNTRY_MERCHANT_PARTY,
        MERCHANT_PARTY_PO00000001_WITH_DIVISION_00002,
        MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 0, 0, 0, 1);
  }

  @Test
  void whenMultiplePartialValidAccountsWithDifferentDivisionAndCurrencyCombinationForNewParty() {
    insertMerchantPartiesWithAccounts(
        cisadm,
        ILM_DT,
        true,
        MERCHANT_PARTY_PO00000001_WITH_VALID_FUND_CHBK_AND_INVALID_CHRG_CHBK_ACCOUNT,
        MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003_AND_VALID_FUND_CHBK_AND_INVALID_CHRG_CRWD_ACCOUNT);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 4, 4, 2, 2);
  }

  @Test
  void whenInvalidPartyUpdateForExistingPartyAndAccounts() {
    insertCompletedBatch(mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, "MERCHANT_DATA");
    insertPartiesWithAccounts(mdu, "test_run", ILM_DT, PARTY_PO00000001_WITH_ALL_ACCOUNTS);

    insertMerchantParty(
        cisadm, HIGH_WATERMARK.plusMinutes(1), true, INVALID_STATE_AND_DIVISION_MERCHANT_PARTY);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 4, 4, 1, 1);
    // check active party cre_dttm is not updated
    assertPartyAccountWithAttributes(PARTY_PO00000001_WITH_ALL_ACCOUNTS);
  }

  @Test
  void whenNewAccountsAreFederatedForExistingParty() {
    insertCompletedBatch(mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, "MERCHANT_DATA");
    insertParty(mdu, "test_run", ILM_DT, false, PARTY_WITH_ID_PO00000001);

    insertMerchantPartiesWithAccounts(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        true,
        MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS_AND_ALL_FIELDS_UPDATE);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 4, 4, 1, 0);
    // not comparing accounts as new accounts have newly created acct_id
    assertThat(countPartyByAttributes(mdu, PARTY_WITH_ID_PO00000001_UPDATED_WITH_ALL_FIELDS))
        .isEqualTo(1);
  }

  @Test
  void whenValidPartyUpdateAndPartialInvalidAccountUpdateForExistingParty() {
    insertCompletedBatch(mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, "MERCHANT_DATA");
    insertPartiesWithAccounts(mdu, "test_run", ILM_DT, PARTY_PO00000001_WITH_ALL_ACCOUNTS);

    insertMerchantPartiesWithAccounts(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        true,
        MERCHANT_PARTY_PO00000001_WITH_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNTS);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 4, 4, 1, 1);
    assertPartyAccountWithAttributes(PARTY_PO00000001_WITH_UPDATED_FUND_AND_CHBK_ACCOUNT);
  }

  @Test
  void whenMultiplePartialValidPartyUpdateForExistingParty() {
    insertCompletedBatch(mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, "MERCHANT_DATA");
    insertPartiesWithAccounts(mdu, "test_run", ILM_DT, PARTY_WITH_ID_PO00000001);

    insertMerchantPartiesWithAccounts(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        true,
        MERCHANT_PARTY_PO00000001,
        INVALID_COUNTRY_MERCHANT_PARTY,
        MERCHANT_PARTY_PO00000001_WITH_DIVISION_00002,
        MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003);
    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 0, 0, 0, 1);
    assertPartyAccountWithAttributes(PARTY_WITH_ID_PO00000001_AND_DIVISION_00003);
  }

  @Test
  void
      whenMultiplePartialValidAccountsWithDifferentDivisionAndCurrencyCombinationForExistingParty() {
    insertCompletedBatch(mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, "MERCHANT_DATA");
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000001_WITH_FUND_CHBK_ACCOUNTS_FOR_DIVISION_00001_AND_00003);

    insertMerchantPartiesWithAccounts(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        true,
        MERCHANT_PARTY_PO00000001_WITH_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNTS,
        MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003_AND_VALID_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNT);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 4, 4, 2, 2);
    assertPartyAccountWithAttributes(
        PARTY_PO00000001_WITH_FUND_CHBK_ACCOUNTS_UPDATE_FOR_DIVISION_00001_AND_00003);
  }

  @Test
  void whenNewSubAccountTypeIsMarkedAsActiveForExistingAccount() {
    insertCompletedBatch(mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, "MERCHANT_DATA");
    insertPartiesWithAccounts(mdu, "test_run", ILM_DT, PARTY_PO00000001_WITH_ALL_ACCOUNTS);

    insertMerchantPartiesWithAccounts(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        true,
        MERCHANT_PARTY_PO00000001_WITH_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNTS);

    run(
        newSubAccountStaticRepository,
        "submit",
        "--force",
        "--set-high-watermark-to",
        "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 4, 5, 1, 1);
    // not asserting DRES sub-account as its sub_acct_id will be newly generated id
    assertPartyAccountWithAttributes(PARTY_PO00000001_WITH_UPDATED_FUND_AND_CHBK_ACCOUNT);
  }

  @Test
  void whenWafIsMarkedAsInactiveForExistingActiveWaf() {
    insertCompletedBatch(mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, "MERCHANT_DATA");
    insertPartiesWithAccounts(mdu, "test_run", ILM_DT, PARTY_PO00000001_WITH_ALL_ACCOUNTS);

    insertMerchantPartiesWithAccounts(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        false,
        MERCHANT_PARTY_PO00000001_WITH_FUND_CHBK_UPDATE_AND_WAF_INACTIVE_AND_INVALID_CHRG_CRWD_ACCOUNTS);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");
    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 4, 4, 1, 1);
    assertPartyAccountWithAttributes(
        PARTY_PO00000001_WITH_UPDATED_FUND_AND_CHBK_ACCOUNT_AND_INACTIVE_WAF);
  }

  @Test
  void whenWafIsMarkedActiveForExistingInactiveWaf() {
    insertCompletedBatch(mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, "MERCHANT_DATA");
    insertPartiesWithAccounts(
        mdu,
        "test_run",
        ILM_DT,
        PARTY_PO00000001_WITH_UPDATED_FUND_AND_CHBK_ACCOUNT_AND_INACTIVE_WAF);

    insertMerchantPartiesWithAccounts(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        true,
        MERCHANT_PARTY_PO00000001_WITH_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNTS);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 4, 4, 1, 1);
    assertPartyAccountWithAttributes(
        PARTY_PO00000001_WITH_UPDATED_FUND_AND_CHBK_ACCOUNT_AND_WAF_UPDATE_FROM_INACTIVE_TO_ACTIVE);
  }

  @Test
  void whenAccountUpdateForMultipleDuplicateAccountsAndSubAccounts() {
    insertCompletedBatch(mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, "MERCHANT_DATA");
    insertPartiesWithAccounts(
        mdu, "test_run", ILM_DT, PARTY_PO00000001_WITH_DUPLICATE_ACCOUNTS_AND_SUB_ACCOUNTS);

    insertMerchantPartiesWithAccounts(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        true,
        MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS_AND_ALL_FIELDS_UPDATE);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 4, 4, 1, 0);
  }

  @Test
  void whenPartialNewAccountsWithCancelFlg() {
    insertMerchantPartiesWithAccounts(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        true,
        MERCHANT_PARTY_PO12345_WITH_ALL_ACCOUNTS_FUND_AND_CHBK_CANCEL_FLG_UPDATE);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 2, 2, 0, 1);
  }

  @Test
  void whenPartialAccountUpdateWithCancelFlgForActiveAccount() {
    insertCompletedBatch(mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, "MERCHANT_DATA");
    insertPartiesWithAccounts(mdu, "test_run", ILM_DT, PARTY_PO00000001_WITH_ALL_ACCOUNTS);

    insertMerchantPartiesWithAccounts(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        true,
        MERCHANT_PARTY_PO00000001_WITH_FUND_AND_CHBK_CANCEL_FLG_UPDATE);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 4, 4, 1, 0);
    assertPartyAccountWithAttributes(
        PARTY_PO00000001_WITH_ALL_ACCOUNTS_AND_CANCELED_FUND_CHBK_ACCOUNT);
  }

  @Test
  void whenAccountUpdateWithCancelFlgForInactiveAccount() {
    insertCompletedBatch(mdu, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, "MERCHANT_DATA");
    insertPartiesWithAccounts(
        mdu, "test_run", ILM_DT, PARTY_PO00000001_WITH_ALL_ACCOUNTS_AND_CANCELED_FUND_CHBK_ACCOUNT);

    insertMerchantPartiesWithAccounts(
        cisadm,
        HIGH_WATERMARK.plusMinutes(1),
        true,
        MERCHANT_PARTY_PO00000001_WITH_FUND_AND_CHBK_CANCEL_FLG_UPDATE);

    run(staticRepository, "submit", "--force", "--set-high-watermark-to", "2021-03-16 00:00:00");

    assertPartyAccountSubAccountWithholdFundsAndErrorsCount(1, 4, 4, 1, 1);
    assertPartyAccountWithAttributes(
        PARTY_PO00000001_WITH_ALL_ACCOUNTS_AND_CANCELED_FUND_CHBK_ACCOUNT);
  }

  private void assertPartyAccountSubAccountWithholdFundsAndErrorsCount(
      int parties, int accounts, int subAccounts, int withholdFunds, int errors) {
    // check count of vw_party and vw_acct, vw_sub_acct, vw_withhold_fund
    assertThat(readErrors(null)).hasSize(errors);
    assertThat(readParty(null)).hasSize(parties);
    assertThat(readAccount(null)).hasSize(accounts);
    assertThat(readSubAccount(SUB_ACCOUNT_AND_WAF_CONDITION)).hasSize(subAccounts);
    assertThat(readWithholdFunds(SUB_ACCOUNT_AND_WAF_CONDITION)).hasSize(withholdFunds);
  }

  private void assertPartyAccountWithAttributes(Party party) {
    assertThat(countPartyByAttributes(mdu, party)).isEqualTo(1);
    List.of(party.getAccounts())
        .forEach(
            account -> {
              assertAccountWithAttributes(account);
              List.of(account.getSubAccounts()).forEach(this::assertSubAccountWithAttributes);
              if (nonNull(account.getWithholdFund())) {
                assertWithholdFundsWithAttributes(account.getWithholdFund());
              }
            });
  }

  private void assertAccountWithAttributes(Account account) {
    assertThat(countAccountByAttributes(mdu, account)).isEqualTo(1);
  }

  private void assertSubAccountWithAttributes(SubAccount subAccount) {
    assertThat(countSubAccountByAttributes(mdu, subAccount)).isEqualTo(1);
  }

  private void assertWithholdFundsWithAttributes(WithholdFund withholdFund) {
    assertThat(countWithholdFundsByAttributes(mdu, withholdFund)).isEqualTo(1);
  }
}
