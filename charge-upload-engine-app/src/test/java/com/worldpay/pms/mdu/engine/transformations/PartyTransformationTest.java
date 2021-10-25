package com.worldpay.pms.mdu.engine.transformations;

import static com.worldpay.pms.mdu.engine.encoder.Encoders.*;
import static com.worldpay.pms.mdu.engine.encoder.InputEncoders.MERCHANT_DATA_PARTY_ROW_ENCODER;
import static com.worldpay.pms.mdu.engine.samples.Transactions.*;
import static com.worldpay.pms.mdu.engine.transformations.PartyTransformation.mapCurrentPartyAndUpdatesToSuccessOrError;
import static com.worldpay.pms.testing.junit.SparkContextHeavyUsage.datasetOf;
import static io.vavr.control.Option.none;
import static io.vavr.control.Option.of;
import static java.util.Comparator.comparing;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;
import com.worldpay.pms.mdu.domain.MerchantDataProcessingService;
import com.worldpay.pms.mdu.domain.model.MerchantDataAccount;
import com.worldpay.pms.mdu.domain.model.MerchantDataParty;
import com.worldpay.pms.mdu.domain.model.output.Account;
import com.worldpay.pms.mdu.domain.model.output.Party;
import com.worldpay.pms.mdu.engine.DummyFactory;
import com.worldpay.pms.mdu.engine.InMemoryStaticDataRepository;
import com.worldpay.pms.mdu.engine.MerchantDataProcessingFactory;
import com.worldpay.pms.mdu.engine.common.Factory;
import com.worldpay.pms.mdu.engine.data.StaticDataRepository;
import com.worldpay.pms.pce.common.DomainError;
import com.worldpay.pms.testing.stereotypes.WithSparkHeavyUsage;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.control.Option;
import io.vavr.control.Validation;
import java.sql.Date;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import lombok.NonNull;
import lombok.val;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@WithSparkHeavyUsage
public class PartyTransformationTest {
  private static final String[] UPDATE_FLG_AND_CREATION_DATE = {"creationDate", "updated"};
  private static final String[] IGNORE_OVERRIDDEN_EQUALS_FOR_FIELDS =
      new String[] {"accounts", "accounts.subAccounts", "accounts.withholdFund"};
  private static final String[] IGNORE_FIELD_IDS =
      new String[] {
        "accounts.accountId",
        "accounts.subAccounts.subAccountId",
        "accounts.subAccounts.accountId",
        "accounts.withholdFund.accountId"
      };
  private static final String[] IGNORE_UPDATE_FLG_AND_CREATION_DATE_FIELDS =
      new String[] {
        "updated",
        "creationDate",
        "accounts.creationDate",
        "accounts.subAccounts.creationDate",
        "accounts.subAccounts.creationDate",
        "accounts.withholdFund.creationDate"
      };
  private static Broadcast<MerchantDataProcessingService> service;
  private static JavaSparkContext jsc;
  static final String BAD_STUFF_HAPPENED = "Something bad happened.";
  private static final InMemoryStaticDataRepository staticData = getStaticData();
  private static Factory<StaticDataRepository> dummy = new DummyFactory<>(staticData);

  private static InMemoryStaticDataRepository getStaticData() {
    return InMemoryStaticDataRepository.builder()
        .billCycleCode(Sets.newHashSet("WPDY", "WPMO"))
        .countryCode(Sets.newHashSet("GBR", "USA"))
        .cisDivision(Sets.newHashSet("00001", "00002", "00003"))
        .state(Sets.newHashSet("OT", "OH"))
        .currency(Sets.newHashSet("GBP", "USD", "CAN"))
        .subAccountType(createSubAccountTypeMap("FUND", "CHRG", "CHBK", "CRWD"))
        .build();
  }

  @BeforeEach
  void setup(JavaSparkContext javaSparkContext) {
    jsc = javaSparkContext;
    service = jsc.broadcast(new MerchantDataProcessingFactory(dummy).build().get());
  }

  @Nested
  class PartyUpdateTest {

    @Test
    void whenNewInvalidParty() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER),
                  datasetOf(MERCHANT_DATA_PARTY_ROW_ENCODER, INVALID_COUNTRY_MERCHANT_PARTY),
                  service)
              .collectAsList();

      assertErrorUpdates(
          result.get(0),
          errors ->
              assertErrorMessage(errors[0].getMessage(), List.of("invalid countryCode 'GB_'")));
    }

    @Test
    void whenMultipleNewValidParty() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001,
                      MERCHANT_PARTY_PO00000002),
                  service)
              .collectAsList();

      assertThat(result).hasSize(2);
      assertThat(result).allMatch(SuccessPartyOrError::isSuccessParty);
      assertThat(result)
          .extracting(SuccessPartyOrError::getParty)
          .usingElementComparatorIgnoringFields(UPDATE_FLG_AND_CREATION_DATE)
          .containsExactlyInAnyOrder(PARTY_WITH_ID_PO00000001, PARTY_WITH_ID_PO00000002);
    }

    @Test
    void
        whenMultipleNewValidPartyWithSamePartyIdAndSameOrDifferentDivisionThenPickOneWithGreaterTxnHeaderId() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001.withValidTo(Date.valueOf("2021-01-01")),
                      MERCHANT_PARTY_PO00000001_WITH_DIVISION_00002,
                      MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isError()).isFalse();
      assertThat(result.get(0).isSuccessParty()).isTrue();
      assertThat(result.get(0).getParty())
          .isEqualToIgnoringGivenFields(
              PARTY_WITH_ID_PO00000001_AND_DIVISION_00003, UPDATE_FLG_AND_CREATION_DATE);
    }

    @Test
    void whenNothingStagedForExistingPartyThenReturnExistingParty() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER, PARTY_WITH_ID_PO00000001),
                  datasetOf(MERCHANT_DATA_PARTY_ROW_ENCODER),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isError()).isFalse();
      assertThat(result.get(0).isSuccessParty()).isTrue();
      assertThat(result.get(0).getParty()).isEqualTo(PARTY_WITH_ID_PO00000001);
    }

    @Test
    void whenUpdateForExistingPartyWithInvalidParty() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER, PARTY_WITH_ID_PO00000001),
                  datasetOf(MERCHANT_DATA_PARTY_ROW_ENCODER, INVALID_COUNTRY_MERCHANT_PARTY),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertErrorUpdates(
          result.get(0),
          errors ->
              assertErrorMessage(errors[0].getMessage(), List.of("invalid countryCode 'GB_'")));

      assertThat(result.get(0).isSuccessParty()).isTrue();
      assertThat(result.get(0).getParty()).isEqualTo(PARTY_WITH_ID_PO00000001);
    }

    @Test
    void whenUpdateForExistingPartyWithValidParty() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER, PARTY_WITH_ID_PO00000001),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_ALL_FIELDS_UPDATE),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isError()).isFalse();
      assertThat(result.get(0).isSuccessParty()).isTrue();
      assertThat(result.get(0).getParty())
          .isEqualToIgnoringGivenFields(
              PARTY_WITH_ID_PO00000001_UPDATED_WITH_ALL_FIELDS, UPDATE_FLG_AND_CREATION_DATE);
    }

    @Test
    void
        whenUpdateForExistingPartyWithSamePartyIdAndSameOrDifferentDivisionThenUpdateWithGreaterTxnHeaderId() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER, PARTY_WITH_ID_PO00000001),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001.withValidTo(Date.valueOf("2021-01-01")),
                      MERCHANT_PARTY_PO00000001_WITH_DIVISION_00002,
                      MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isError()).isFalse();
      assertThat(result.get(0).isSuccessParty()).isTrue();
      assertThat(result.get(0).getParty())
          .isEqualToIgnoringGivenFields(
              PARTY_WITH_ID_PO00000001_AND_DIVISION_00003, UPDATE_FLG_AND_CREATION_DATE);
    }

    @Test
    void
        whenUnhandledErrorWithDomainServiceThenTaskDoesNotFailAndMerchantDataPartyRowIsMarkedAsFailed() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER, PARTY_WITH_ID_PO00000001),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_ALL_FIELDS_UPDATE,
                      MERCHANT_PARTY_PO00000002),
                  jsc.broadcast(new FailedMerchantDataProcessingService()))
              .collectAsList();

      assertThat(result).hasSize(2);
      assertThat(result).allMatch(SuccessPartyOrError::isError);
      assertThat(result.get(0).getErrors()[0].getMessage()).isEqualTo(BAD_STUFF_HAPPENED);
      assertThat(result.get(1).getErrors()[0].getMessage()).isEqualTo(BAD_STUFF_HAPPENED);

      // existing party should be returned as success, if any error occurs
      assertThat(result).anyMatch(SuccessPartyOrError::isSuccessParty);
      assertThat(result)
          .filteredOn(SuccessPartyOrError::isSuccessParty)
          .extracting(SuccessPartyOrError::getParty)
          .containsExactlyInAnyOrder(PARTY_WITH_ID_PO00000001);
    }

    @Test
    void
        whenUnhandledErrorWhileMappingSuccessOrErrorThenTaskDoesNotFailAndAllUpdatesWrtPartyAreMarkedAsFailed() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER, PARTY_WITH_ID_PO00000001),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_DIVISION_00002,
                      MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003,
                      MERCHANT_PARTY_PO00000001_WITH_ALL_FIELDS_UPDATE,
                      MERCHANT_PARTY_PO00000002),
                  // marking broadcast as null, to return NullPointer
                  null)
              .collectAsList();
      assertThat(result).hasSize(2);
      assertThat(result).allMatch(SuccessPartyOrError::isError);
      assertThat(result.get(0).getErrors()[0].getMessage()).isNull();
      assertThat(result.get(1).getErrors()[0].getMessage()).isNull();

      // existing party should be returned as success, if any error occurs
      assertThat(result).anyMatch(SuccessPartyOrError::isSuccessParty);
      assertThat(result)
          .filteredOn(SuccessPartyOrError::isSuccessParty)
          .extracting(SuccessPartyOrError::getParty)
          .containsExactlyInAnyOrder(PARTY_WITH_ID_PO00000001);
    }
  }

  @Nested
  class AccountUpdateTest {

    @Test
    void whenNewInvalidPartyAndValidAccounts() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      INVALID_MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isSuccessParty()).isFalse();
      assertErrorUpdates(
          result.get(0),
          errors ->
              assertErrorMessage(errors[0].getMessage(), List.of("invalid countryCode 'GB_'")));
    }

    @Test
    void whenNewValidPartyAndAllInvalidAccounts() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_ALL_INVALID_ACCOUNTS),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertErrorUpdates(
          result.get(0),
          errors ->
              assertErrorMessage(
                  errors[0].getMessage(),
                  List.of(
                      "invalid accountType 'FND'",
                      "invalid accountType 'CHG'",
                      "invalid accountType 'CHK'",
                      "invalid accountType 'CRD'")));

      assertUpdatedParty(result.get(0), Option.none(), false, PARTY_WITH_ID_PO00000001);
    }

    @Test
    void whenNewValidPartyAndAccounts() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isError()).isFalse();

      assertUpdatedParty(result.get(0), Option.none(), true, PARTY_PO00000001_WITH_ALL_ACCOUNTS);
    }

    @Test
    void whenNewValidPartyWithPartialValidAccounts() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_VALID_FUND_CHBK_AND_INVALID_CHRG_CHBK_ACCOUNT),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertErrorUpdates(
          result.get(0),
          errors ->
              assertErrorMessage(
                  errors[0].getMessage(),
                  List.of("invalid accountType 'CHG'", "invalid accountType 'CRD'")));

      assertUpdatedParty(
          result.get(0), Option.none(), true, PARTY_PO00000001_WITH_FUND_AND_CHBK_ACCOUNT);
    }

    @Test
    void whenMultipleNewValidPartyAndAccountsWithSamePartyIdDivisionAndCurrency() {
      // creation and updation is done based on txnHeaderId order
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS,
                      MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS_AND_ALL_FIELDS_UPDATE),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isError()).isFalse();
      assertUpdatedParty(
          result.get(0),
          of(comparing(Account::getAccountType)),
          true,
          PARTY_PO00000001_WITH_ALL_ACCOUNTS_AND_ALL_FIELDS_UPDATE);
    }

    @Test
    void whenNewValidPartyWithPartialInvalidAccountsForDivisionAndCurrencyCombination() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_VALID_FUND_CHBK_AND_INVALID_CHRG_CHBK_ACCOUNT,
                      MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003_AND_VALID_FUND_CHBK_AND_INVALID_CHRG_CRWD_ACCOUNT),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertErrorUpdates(
          result.get(0),
          errors -> {
            assertErrorMessage(
                errors[0].getMessage(),
                List.of("invalid accountType 'CHG'", "invalid accountType 'CRD'"));
            assertErrorMessage(
                errors[1].getMessage(),
                List.of("invalid accountType 'CHG'", "invalid accountType 'CRD'"));
          });

      assertUpdatedParty(
          result.get(0),
          Option.none(),
          true,
          PARTY_PO00000001_WITH_FUND_CHBK_ACCOUNTS_FOR_DIVISION_00001_AND_00003);
    }

    @Test
    void whenUpdateForExistingAccountsWithInvalidParty() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER, PARTY_PO00000001_WITH_ALL_ACCOUNTS),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      INVALID_MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertErrorUpdates(
          result.get(0),
          errors ->
              assertErrorMessage(errors[0].getMessage(), List.of("invalid countryCode 'GB_'")));

      assertUpdatedParty(result.get(0), Option.none(), false, PARTY_PO00000001_WITH_ALL_ACCOUNTS);
    }

    @Test
    void whenUpdateForExistingAccountsWithAllInvalidAccounts() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER, PARTY_PO00000001_WITH_ALL_ACCOUNTS),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_ALL_FIELDS_UPDATE_AND_ALL_INVALID_ACCOUNTS),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertErrorUpdates(
          result.get(0),
          errors ->
              assertErrorMessage(
                  errors[0].getMessage(),
                  List.of(
                      "invalid accountType 'FND'",
                      "invalid accountType 'CHG'",
                      "invalid accountType 'CHK'",
                      "invalid accountType 'CRD'")));

      assertUpdatedParty(
          result.get(0),
          Option.none(),
          false,
          PARTY_PO00000001_UPDATED_WITH_ALL_FIELDS_AND_OLD_ACCOUNTS);
    }

    @Test
    void whenUpdateForExistingAccountsWithPartialValidAccounts() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER, PARTY_PO00000001_WITH_ALL_ACCOUNTS),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNTS),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertErrorUpdates(
          result.get(0),
          errors ->
              assertErrorMessage(
                  errors[0].getMessage(),
                  List.of("invalid accountType 'CHG'", "invalid accountType 'CRD'")));
      assertUpdatedParty(
          result.get(0), Option.none(), false, PARTY_PO00000001_WITH_UPDATED_FUND_AND_CHBK_ACCOUNT);
    }

    @Test
    void whenMultipleUpdateForExistingAccountsWithSameDivisionAndCurrency() {
      // updation is done based on txnHeaderId order
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER, PARTY_PO00000001_WITH_ALL_ACCOUNTS),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS,
                      MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS_AND_ALL_FIELDS_UPDATE),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isError()).isFalse();

      assertUpdatedParty(
          result.get(0),
          of(comparing(Account::getAccountType)),
          false,
          PARTY_PO00000001_WITH_ALL_ACCOUNTS_AND_ALL_FIELDS_UPDATE);
    }

    @Test
    void whenUpdateForExistingAccountsWithPartialValidAccountsForDivisionAndCurrencyCombination() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(
                      PARTY_ENCODER,
                      PARTY_PO00000001_WITH_FUND_CHBK_ACCOUNTS_FOR_DIVISION_00001_AND_00003),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNTS,
                      MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003_AND_VALID_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNT),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertErrorUpdates(
          result.get(0),
          errors -> {
            assertErrorMessage(
                errors[0].getMessage(),
                List.of("invalid accountType 'CHG'", "invalid accountType 'CRD'"));
            assertErrorMessage(
                errors[1].getMessage(),
                List.of("invalid accountType 'CHG'", "invalid accountType 'CRD'"));
          });
      assertUpdatedParty(
          result.get(0),
          Option.none(),
          false,
          PARTY_PO00000001_WITH_FUND_CHBK_ACCOUNTS_UPDATE_FOR_DIVISION_00001_AND_00003);
    }

    @Test
    void whenUpdateForExistingAccountsWithNewAccounts() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER, PARTY_PO00000001_WITH_FUND_AND_CHBK_ACCOUNT),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS_AND_ALL_FIELDS_UPDATE),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isError()).isFalse();

      assertUpdatedParty(
          result.get(0),
          of(comparing(Account::getAccountType)),
          true,
          PARTY_PO00000001_WITH_ALL_ACCOUNTS_AND_ALL_FIELDS_UPDATE);
    }

    @Test
    void whenAccountUpdateForMultipleDuplicateAccountsAndSubAccounts() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(
                      PARTY_ENCODER, PARTY_PO00000001_WITH_DUPLICATE_ACCOUNTS_AND_SUB_ACCOUNTS),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS_AND_ALL_FIELDS_UPDATE),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isError()).isFalse();
      assertUpdatedParty(
          result.get(0),
          of(comparing(Account::getAccountType)),
          true,
          PARTY_PO00000001_WITH_ALL_ACCOUNTS_AND_ALL_FIELDS_UPDATE);
    }

    @Test
    void whenPartialNewAccountsWithCancelFlg() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO12345_WITH_ALL_ACCOUNTS_FUND_AND_CHBK_CANCEL_FLG_UPDATE),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isError()).isTrue();
      assertErrorUpdates(
          result.get(0),
          errors ->
              assertErrorMessage(
                  errors[0].getMessage(),
                  List.of(
                      "No active account having accountType 'FUND' found for cancellation",
                      "No active account having accountType 'CHBK' found for cancellation")));
      assertUpdatedParty(
          result.get(0), none(), true, PARTY_PO00000001_CANCEL_FLG_WITH_CHRG_AND_CRWD_ACCOUNT);
    }

    @Test
    void whenPartialAccountUpdateWithCancelFlgForActiveAccount() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER, PARTY_PO00000001_WITH_ALL_ACCOUNTS),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_FUND_AND_CHBK_CANCEL_FLG_UPDATE),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isError()).isFalse();
      assertUpdatedParty(
          result.get(0),
          none(),
          false,
          PARTY_PO00000001_WITH_ALL_ACCOUNTS_AND_CANCELED_FUND_CHBK_ACCOUNT);
    }

    @Test
    void whenAccountUpdateWithCancelFlgForInactiveAccount() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(
                      PARTY_ENCODER,
                      PARTY_PO00000001_WITH_ALL_ACCOUNTS_AND_CANCELED_FUND_CHBK_ACCOUNT),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_FUND_AND_CHBK_CANCEL_FLG_UPDATE),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isError()).isTrue();
      assertErrorUpdates(
          result.get(0),
          errors ->
              assertErrorMessage(
                  errors[0].getMessage(),
                  List.of(
                      "No active account having accountType 'FUND' found for cancellation",
                      "No active account having accountType 'CHBK' found for cancellation")));
      assertUpdatedParty(
          result.get(0),
          of(Comparator.comparing(Account::getAccountId)),
          false,
          PARTY_PO00000001_WITH_ALL_ACCOUNTS_AND_CANCELED_FUND_CHBK_ACCOUNT);
    }

    @Test
    void whenNewAccountUpdateWithoutCancelFlgForInactiveAccount() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(PARTY_ENCODER, PARTY_PO00000001_WITH_CANCELED_FUND_CHBK_ACCOUNT),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNTS),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isError()).isTrue();
      assertErrorUpdates(
          result.get(0),
          errors ->
              assertErrorMessage(
                  errors[0].getMessage(),
                  List.of("invalid accountType 'CHG'", "invalid accountType 'CRD'")));
      assertUpdatedParty(
          result.get(0),
          of(Comparator.comparing(Account::getTxnHeaderId)),
          true,
          PARTY_PO00000001_WITH_NEW_AND_CANCELED_FUND_CHBK_ACCOUNT);
    }

    @Test
    void whenNewAccountUpdateWithCancelFlgForActiveAndInactiveAccount() {
      val result =
          mapCurrentPartyAndUpdatesToSuccessOrError(
                  datasetOf(
                      PARTY_ENCODER, PARTY_PO00000001_WITH_NEW_AND_CANCELED_FUND_CHBK_ACCOUNT),
                  datasetOf(
                      MERCHANT_DATA_PARTY_ROW_ENCODER,
                      MERCHANT_PARTY_PO00000001_WITH_FUND_AND_CHBK_CANCEL_FLG_UPDATE),
                  service)
              .collectAsList();

      assertThat(result).hasSize(1);
      assertThat(result.get(0).isError()).isFalse();
      assertUpdatedParty(
          result.get(0),
          none(),
          false,
          PARTY_PO00000001_WITH_NEW_AND_MULTIPLE_SAME_CANCELED_FUND_CHBK_ACCOUNT);
    }
  }

  //
  // helper
  //
  public static class FailedMerchantDataProcessingService implements MerchantDataProcessingService {
    @Override
    public Validation<Seq<DomainError>, MerchantDataParty> validateParty(
        @NonNull MerchantDataParty row) {
      throw new RuntimeException(BAD_STUFF_HAPPENED);
    }

    @Override
    public Validation<Seq<DomainError>, String[]> processAccount(@NonNull MerchantDataAccount row) {
      throw new RuntimeException(BAD_STUFF_HAPPENED);
    }
  }

  public static Map<String, String[]> createSubAccountTypeMap(String... subAccountType) {
    LinkedHashMap<String, String[]> subAccountTypes = new LinkedHashMap<>();
    for (String subAccount : subAccountType) {
      String[] subAccounts = subAccount.split("~");
      subAccountTypes.put(subAccounts[0], subAccounts);
    }
    return subAccountTypes;
  }

  private void assertErrorMessage(String message, List<String> errors) {
    assertThat(message).isEqualTo(String.join("\n", errors));
  }

  private void assertErrorUpdates(SuccessPartyOrError result, Consumer<ErrorTransaction[]> errors) {
    assertThat(result.isError()).isTrue();
    errors.accept(result.getErrors());
  }

  private void assertUpdatedParty(
      SuccessPartyOrError result,
      Option<Comparator<Account>> sortAccounts,
      boolean ignoreIdComparison,
      Party party) {
    assertThat(result.isSuccessParty()).isTrue();
    // sort accounts in party to make comparison easy
    sortAccounts.peek(comparator -> Arrays.sort(result.getParty().getAccounts(), comparator));

    assertThat(result.getParty())
        .usingRecursiveComparison(getRecursiveComparisonConfiguration(ignoreIdComparison))
        .isEqualTo(party);
  }

  private RecursiveComparisonConfiguration getRecursiveComparisonConfiguration(
      boolean ignoreIdComparison) {
    RecursiveComparisonConfiguration recursiveComparisonConf =
        new RecursiveComparisonConfiguration();
    recursiveComparisonConf.ignoreOverriddenEqualsForFields(IGNORE_OVERRIDDEN_EQUALS_FOR_FIELDS);
    // ignore creationDate by default
    recursiveComparisonConf.ignoreFields(IGNORE_UPDATE_FLG_AND_CREATION_DATE_FIELDS);
    if (ignoreIdComparison) {
      recursiveComparisonConf.ignoreFields(IGNORE_FIELD_IDS);
    }
    return recursiveComparisonConf;
  }
}
