package com.worldpay.pms.mdu.engine.samples;

import com.worldpay.pms.mdu.domain.model.output.*;
import com.worldpay.pms.mdu.domain.model.output.Account.AccountBuilder;
import com.worldpay.pms.mdu.domain.model.output.Party.PartyBuilder;
import com.worldpay.pms.mdu.domain.model.output.SubAccount.SubAccountBuilder;
import com.worldpay.pms.mdu.domain.model.output.WithholdFund.WithholdFundBuilder;
import com.worldpay.pms.mdu.engine.transformations.ErrorAccountHierarchy;
import com.worldpay.pms.mdu.engine.transformations.model.input.*;
import com.worldpay.pms.mdu.engine.transformations.model.input.AccountHierarchyDataRow.AccountHierarchyDataRowBuilder;
import com.worldpay.pms.mdu.engine.transformations.model.input.MerchantDataAccountRow.MerchantDataAccountRowBuilder;
import com.worldpay.pms.mdu.engine.transformations.model.input.MerchantDataPartyRow.MerchantDataPartyRowBuilder;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

public class Transactions {
  private static final Account[] EMPTY_ACCOUNT_ARRAY = new Account[0];
  private static final SubAccount[] EMPTY_SUB_ACCOUNT_ARRAY = new SubAccount[0];
  private static final MerchantDataAccountRow[] EMPTY_MERCHANT_ACCOUNT_ARRAY =
      new MerchantDataAccountRow[0];
  private static final long PARTITION_ID_NOT_APPLICABLE = -1L;
  private static final Timestamp CREATION_DT = Timestamp.valueOf("2021-03-15 00:02:30");

  public static MerchantDataRow.MerchantDataRowBuilder INVALID_MERCHANT_DATA =
      MerchantDataRow.builder()
          .partyValidFrom(Date.valueOf("2021-01-01"))
          .accountValidFrom(Date.valueOf("2021-01-01"))
          .personOrBusinessFlag("B")
          .division("00001")
          .partyId("PO00000001")
          .state(null)
          .taxRegistrationNumber("GB12345")
          .currency("GBP")
          .billCycleCode("WPDY")
          .businessUnit("PO1100000001")
          .wafFlag("N");

  public static MerchantDataRow.MerchantDataRowBuilder MERCHANT_DATA =
      MerchantDataRow.builder()
          .partyValidFrom(Date.valueOf("2021-01-01"))
          .accountValidFrom(Date.valueOf("2021-01-01"))
          .personOrBusinessFlag("B")
          .division("00001")
          .country("GBR")
          .state(null)
          .taxRegistrationNumber("GB12345")
          .currency("GBP")
          .billCycleCode("WPDY")
          .settlementRegionId("SETTLEMENT")
          .businessUnit("PO1100000001")
          .wafFlag("y ");

  public static MerchantDataRow.MerchantDataRowBuilder MERCHANT_DATA_WITHOUT_ACCOUNT =
      MerchantDataRow.builder()
          .partyValidFrom(Date.valueOf("2021-01-01"))
          .accountValidFrom(null)
          .accountType(null)
          .personOrBusinessFlag("B")
          .division("00001")
          .country("GBR")
          .state(null)
          .taxRegistrationNumber("GB12345")
          .currency(null)
          .billCycleCode(null)
          .businessUnit("PO1100000001")
          .wafFlag("y ");

  public static final MerchantDataRow INVALID_COUNTRY_MERCHANT_DATA =
      INVALID_MERCHANT_DATA.txnHeaderId("900001").country("GB_R").accountType("FUND").build();

  public static final MerchantDataRow INVALID_STATE_AND_DIVISION_MERCHANT_DATA =
      INVALID_MERCHANT_DATA
          .txnHeaderId("900002")
          .country("GBR")
          .state("TEST")
          .division("00099")
          .build();

  public static final MerchantDataRow MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_FUND =
      MERCHANT_DATA.txnHeaderId("100001").partyId("PO00000001").accountType("FUND").build();

  public static final MerchantDataRow MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_CHRG =
      MERCHANT_DATA.txnHeaderId("100001").partyId("PO00000001").accountType("CHRG").build();

  public static final MerchantDataRow MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_CHBK =
      MERCHANT_DATA.txnHeaderId("100001").partyId("PO00000001").accountType("CHBK").build();

  public static final MerchantDataRow MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_CRWD =
      MERCHANT_DATA.txnHeaderId("100001").partyId("PO00000001").accountType("CRWD").build();

  public static final MerchantDataRow MERCHANT_DATA_WITH_PARTY_PO00000002_AND_ACCOUNT_FUND =
      MERCHANT_DATA
          .txnHeaderId("100002")
          .partyId("PO00000002")
          .accountType("FUND")
          .country("USA")
          .state("OH")
          .currency("USD")
          .build();

  public static final MerchantDataRow
      MERCHANT_DATA_WITH_PARTY_PO00000002_AND_ACCOUNT_FUND_WITH_GBP =
          MERCHANT_DATA
              .txnHeaderId("100002")
              .partyId("PO00000002")
              .accountType("FUND")
              .country("USA")
              .state("OH")
              .currency("GBP")
              .build();

  public static final MerchantDataRow MERCHANT_DATA_WITH_PARTY_PO00000002_AND_ACCOUNT_CHRG =
      MERCHANT_DATA
          .txnHeaderId("100002")
          .partyId("PO00000002")
          .accountType("CHRG")
          .country("USA")
          .state("OH")
          .currency("USD")
          .build();

  public static final MerchantDataRow MERCHANT_DATA_WITH_PARTY_PO00000002_AND_ACCOUNT_CHBK =
      MERCHANT_DATA
          .txnHeaderId("100002")
          .partyId("PO00000002")
          .accountType("CHBK")
          .country("USA")
          .state("OH")
          .currency("USD")
          .build();

  public static final MerchantDataRow MERCHANT_DATA_WITH_PARTY_PO00000002_AND_ACCOUNT_CRWD =
      MERCHANT_DATA
          .txnHeaderId("100002")
          .partyId("PO00000002")
          .accountType("CRWD")
          .country("USA")
          .state("OH")
          .currency("USD")
          .build();

  public static final MerchantDataRow MERCHANT_WITH_PARTY_PO00000001 =
      MERCHANT_DATA_WITHOUT_ACCOUNT.txnHeaderId("100001").partyId("PO00000001").build();

  public static final MerchantDataRow MERCHANT_DATA_WITH_BUSINESS_PARTY_PO00000003_AND_NO_ACCOUNT =
      MERCHANT_DATA_WITHOUT_ACCOUNT.txnHeaderId("100003").partyId("PO00000003").build();

  public static MerchantDataRow MERCHANT_DATA_WITH_GROUPED_PARTY_PO00000004 =
      MERCHANT_DATA_WITHOUT_ACCOUNT.txnHeaderId("100004").partyId("PO00000004").build();

  public static final MerchantDataRow
      MERCHANT_DATA_WITH_GROUPED_PARTY_PO00000004_AND_DIVSION_00001 =
          MERCHANT_DATA_WITH_GROUPED_PARTY_PO00000004.withDivision("00001");

  public static final MerchantDataRow
      MERCHANT_DATA_WITH_GROUPED_PARTY_PO00000004_AND_DIVSION_00002 =
          MERCHANT_DATA_WITH_GROUPED_PARTY_PO00000004
              .withTxnHeaderId("100006")
              .withDivision("00002");

  public static final MerchantDataRow MERCHANT_DATA_WITH_PARTY_PO00000001_AND_INVALID_CHRG_ACCOUNT =
      MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_CHRG.withAccountType("CHG");

  public static final MerchantDataRow MERCHANT_DATA_WITH_PARTY_PO00000001_AND_INVALID_CRWD_ACCOUNT =
      MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_CRWD.withAccountType("CRD");

  // new merchantDataParty structure
  public static MerchantDataPartyRowBuilder INVALID_MERCHANT_PARTY =
      MerchantDataPartyRow.builder()
          .validTo(Date.valueOf("2021-01-01"))
          .personOrBusinessFlag("B")
          .division("00001")
          .partyId("PO00000001")
          .state(null)
          .taxRegistrationNumber("GB12345")
          .businessUnit("PO1100000001")
          .accounts(EMPTY_MERCHANT_ACCOUNT_ARRAY)
          .wafFlag("N");

  public static MerchantDataPartyRowBuilder MERCHANT_PARTY =
      MerchantDataPartyRow.builder()
          .validFrom(Date.valueOf("2021-01-01"))
          .personOrBusinessFlag("B")
          .division("00001")
          .country("GBR")
          .state(null)
          .taxRegistrationNumber("GB12345")
          .businessUnit("PO1100000001")
          .accounts(EMPTY_MERCHANT_ACCOUNT_ARRAY)
          .wafFlag("y ");

  public static final MerchantDataPartyRow INVALID_COUNTRY_MERCHANT_PARTY =
      INVALID_MERCHANT_PARTY.txnHeaderId("900001").country("GB_").build();

  public static final MerchantDataPartyRow INVALID_STATE_AND_DIVISION_MERCHANT_PARTY =
      INVALID_MERCHANT_PARTY
          .txnHeaderId("100001")
          .country("GBR")
          .state("TEST")
          .division("00099")
          .build();

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000001 =
      MERCHANT_PARTY.txnHeaderId("100001").partyId("PO00000001").build();

  public static final MerchantDataPartyRow MERCHANT_PARTY__PO00000002 =
      MERCHANT_PARTY.txnHeaderId("100002").partyId("PO00000002").build();

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000003 =
      MERCHANT_PARTY.txnHeaderId("100003").partyId("PO00000003").build();

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000004 =
      MERCHANT_PARTY.txnHeaderId("100004").partyId("PO00000004").build();

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000005 =
      MERCHANT_PARTY.txnHeaderId("100005").partyId("PO00000005").build();

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000006 =
      MERCHANT_PARTY.txnHeaderId("100006").partyId("PO00000006").build();

  public static final MerchantDataPartyRow INVALID_MERCHANT_PARTY_PO00000001 =
      MERCHANT_PARTY_PO00000001.withTxnHeaderId("100011").withValidFrom(null);

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000001_WITH_ACCOUNT_UPDATE =
      MERCHANT_PARTY_PO00000001.withTxnHeaderId("100015");

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000001_WITH_DIVISION_00002 =
      MERCHANT_PARTY_PO00000001
          .withTxnHeaderId("100012")
          .withDivision("00002")
          .withTaxRegistrationNumber("GB54321")
          .withValidTo(Date.valueOf("2021-03-01"));

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003 =
      MERCHANT_PARTY_PO00000001
          .withTxnHeaderId("100013")
          .withDivision("00003")
          .withBusinessUnit("PO1100000015")
          .withValidTo(Date.valueOf("2021-02-01"));

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003_UPDATE =
      MERCHANT_PARTY_PO00000001
          .withTxnHeaderId("100023")
          .withDivision("00003")
          .withBusinessUnit("PO1100000015")
          .withValidTo(Date.valueOf("2021-02-01"));

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000001_WITH_ALL_FIELDS_UPDATE =
      MERCHANT_PARTY_PO00000001
          .withTxnHeaderId("100014")
          .withCountry("USA")
          .withState("OH")
          .withBusinessUnit("PO1100000015")
          .withTaxRegistrationNumber("GB54321")
          .withValidTo(Date.valueOf("2021-02-01"));

  public static final MerchantDataPartyRow
      MERCHANT_PARTY_PO00000001_WITH_ALL_FIELDS_UPDATE_ACCOUNT_CANCEL_FLG =
          MERCHANT_PARTY_PO00000001_WITH_ALL_FIELDS_UPDATE.withTxnHeaderId("100015");

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000002 =
      MERCHANT_PARTY.txnHeaderId("100002").partyId("PO00000002").country("USA").state("OH").build();

  public static MerchantDataAccountRowBuilder MERCHANT_ACCOUNT =
      MerchantDataAccountRow.builder()
          .currency("GBP")
          .billCycleCode("WPDY")
          .settlementRegionId("SETTLEMENT")
          .validFrom(Date.valueOf("2021-01-01"));

  public static final MerchantDataAccountRow ONLY_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001 =
      MERCHANT_ACCOUNT.accountType("FUND").partyId("PO00000001").txnHeaderId("100001").build();

  public static final MerchantDataAccountRow ONLY_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000002 =
      MERCHANT_ACCOUNT.accountType("FUND").partyId("PO00000002").txnHeaderId("100002").build();

  public static final MerchantDataAccountRow ONLY_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000003 =
      MERCHANT_ACCOUNT.accountType("FUND").partyId("PO00000003").txnHeaderId("100003").build();

  public static final MerchantDataAccountRow ONLY_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000004 =
      MERCHANT_ACCOUNT.accountType("FUND").partyId("PO00000004").txnHeaderId("100004").build();

  public static final MerchantDataAccountRow ONLY_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000005 =
      MERCHANT_ACCOUNT.accountType("FUND").partyId("PO00000005").txnHeaderId("100005").build();

  public static final MerchantDataAccountRow ONLY_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000006 =
      MERCHANT_ACCOUNT.accountType("FUND").partyId("PO00000006").txnHeaderId("100006").build();

  public static final MerchantDataAccountRow FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001 =
      MERCHANT_ACCOUNT.accountType("FUND").partyId("PO00000001").txnHeaderId("100001").build();
  public static final MerchantDataAccountRow CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001 =
      MERCHANT_ACCOUNT.accountType("CHRG").partyId("PO00000001").txnHeaderId("100001").build();
  public static final MerchantDataAccountRow CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001 =
      MERCHANT_ACCOUNT.accountType("CHBK").partyId("PO00000001").txnHeaderId("100001").build();
  public static final MerchantDataAccountRow CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001 =
      MERCHANT_ACCOUNT.accountType("CRWD").partyId("PO00000001").txnHeaderId("100001").build();

  public static final MerchantDataAccountRow INVALID_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001 =
      FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001.withAccountType("FND");
  public static final MerchantDataAccountRow INVALID_CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001 =
      CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001.withAccountType("CHG");
  public static final MerchantDataAccountRow INVALID_CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001 =
      CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001.withAccountType("CRD");
  public static final MerchantDataAccountRow INVALID_CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001 =
      CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001.withAccountType("CHK");

  public static final MerchantDataAccountRow
      FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE =
          FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001
              .withTxnHeaderId("100014")
              .withBillCycleCode("WPMO")
              .withValidTo(Date.valueOf("2021-02-01"))
              .withSettlementRegionId("SETTLEMENT_UPDATE");
  public static final MerchantDataAccountRow
      CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE =
          CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001
              .withTxnHeaderId("100014")
              .withBillCycleCode("WPMO")
              .withValidTo(Date.valueOf("2021-02-01"))
              .withSettlementRegionId("SETTLEMENT_UPDATE");
  public static final MerchantDataAccountRow
      CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE =
          CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001
              .withTxnHeaderId("100014")
              .withBillCycleCode("WPMO")
              .withValidTo(Date.valueOf("2021-02-01"))
              .withSettlementRegionId("SETTLEMENT_UPDATE");
  public static final MerchantDataAccountRow
      CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE =
          CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001
              .withTxnHeaderId("100014")
              .withBillCycleCode("WPMO")
              .withValidTo(Date.valueOf("2021-02-01"))
              .withSettlementRegionId("SETTLEMENT_UPDATE");

  public static final MerchantDataAccountRow
      INVALID_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_UPDATE =
          FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE.withAccountType("FND");
  public static final MerchantDataAccountRow
      INVALID_CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_UPDATE =
          CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE.withAccountType("CHG");
  public static final MerchantDataAccountRow
      INVALID_CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_UPDATE =
          CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE
              .withTxnHeaderId("100014")
              .withAccountType("CRD");
  public static final MerchantDataAccountRow
      INVALID_CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_UPDATE =
          CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE
              .withTxnHeaderId("100014")
              .withAccountType("CHK");

  public static final MerchantDataAccountRow
      FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003 =
          FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001
              .withTxnHeaderId("100013")
              .withValidTo(Date.valueOf("2021-02-01"));
  public static final MerchantDataAccountRow
      INVALID_CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003 =
          CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001
              .withTxnHeaderId("100013")
              .withAccountType("CHG")
              .withValidTo(Date.valueOf("2021-02-01"));
  public static final MerchantDataAccountRow
      CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003 =
          CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001
              .withTxnHeaderId("100013")
              .withValidTo(Date.valueOf("2021-02-01"));
  public static final MerchantDataAccountRow
      INVALID_CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003 =
          CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001
              .withTxnHeaderId("100013")
              .withAccountType("CRD")
              .withValidTo(Date.valueOf("2021-02-01"));

  public static final MerchantDataAccountRow
      FUND_MERCHANT_ACCOUNT_UPDATE_WITH_PARTY_PO00000001_AND_DIVISION_00003 =
          FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003
              .withTxnHeaderId("100023")
              .withBillCycleCode("WPMO")
              .withValidTo(Date.valueOf("2021-03-01"));
  public static final MerchantDataAccountRow
      INVALID_CHRG_MERCHANT_ACCOUNT_UPDATE_WITH_PARTY_PO00000001_AND_DIVISION_00003 =
          INVALID_CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003.withTxnHeaderId(
              "100023");
  public static final MerchantDataAccountRow
      CHBK_MERCHANT_ACCOUNT_UPDATE_WITH_PARTY_PO00000001_AND_DIVISION_00003 =
          CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003
              .withTxnHeaderId("100023")
              .withBillCycleCode("WPMO")
              .withValidTo(Date.valueOf("2021-03-01"));
  public static final MerchantDataAccountRow
      INVALID_CRWD_MERCHANT_ACCOUNT_UPDATE_WITH_PARTY_PO00000001_AND_DIVISION_00003 =
          INVALID_CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003.withTxnHeaderId(
              "100023");

  public static final MerchantDataAccountRow
      FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE =
          FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE
              .withTxnHeaderId("100015")
              .withCancellationFlag("Y");

  public static final MerchantDataAccountRow
      CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE =
          CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE
              .withTxnHeaderId("100015")
              .withCancellationFlag("Y");

  public static final MerchantDataAccountRow[] FUND_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001 =
      new MerchantDataAccountRow[] {ONLY_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001};

  public static final MerchantDataAccountRow[] FUND_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000002 =
      new MerchantDataAccountRow[] {ONLY_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000002};

  public static final MerchantDataAccountRow[] FUND_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000003 =
      new MerchantDataAccountRow[] {ONLY_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000003};

  public static final MerchantDataAccountRow[] FUND_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000004 =
      new MerchantDataAccountRow[] {ONLY_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000004};

  public static final MerchantDataAccountRow[] FUND_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000005 =
      new MerchantDataAccountRow[] {ONLY_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000005};

  public static final MerchantDataAccountRow[] FUND_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000006 =
      new MerchantDataAccountRow[] {ONLY_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000006};

  public static final MerchantDataAccountRow[] ALL_INVALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001 =
      new MerchantDataAccountRow[] {
        INVALID_FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001,
        INVALID_CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001,
        INVALID_CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001,
        INVALID_CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001
      };

  public static final MerchantDataAccountRow[] ALL_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001 =
      new MerchantDataAccountRow[] {
        FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001,
        CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001,
        CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001,
        CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001
      };

  public static final MerchantDataAccountRow[]
      VALID_FUND_CHBK_AND_INVALID_CHRG_CRWD_ACCOUNT_WITH_PARTY_PO00000001 =
          new MerchantDataAccountRow[] {
            FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001,
            INVALID_CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001,
            CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001,
            INVALID_CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001
          };

  public static final MerchantDataAccountRow[]
      VALID_FUND_CHBK_AND_INVALID_CHRG_CRWD_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003 =
          new MerchantDataAccountRow[] {
            FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003,
            INVALID_CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003,
            CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003,
            INVALID_CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003
          };

  public static final MerchantDataAccountRow[]
      ALL_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE =
          new MerchantDataAccountRow[] {
            FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE,
            CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE,
            CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE,
            CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE
          };

  public static final MerchantDataAccountRow[]
      VALID_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNT_WITH_PARTY_PO00000001 =
          new MerchantDataAccountRow[] {
            FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE,
            INVALID_CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_UPDATE,
            CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE,
            INVALID_CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_UPDATE
          };

  public static final MerchantDataAccountRow[]
      VALID_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003 =
          new MerchantDataAccountRow[] {
            FUND_MERCHANT_ACCOUNT_UPDATE_WITH_PARTY_PO00000001_AND_DIVISION_00003,
            INVALID_CHRG_MERCHANT_ACCOUNT_UPDATE_WITH_PARTY_PO00000001_AND_DIVISION_00003,
            CHBK_MERCHANT_ACCOUNT_UPDATE_WITH_PARTY_PO00000001_AND_DIVISION_00003,
            INVALID_CRWD_MERCHANT_ACCOUNT_UPDATE_WITH_PARTY_PO00000001_AND_DIVISION_00003
          };

  public static final MerchantDataAccountRow[]
      ALL_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_WITH_FUND_AND_CHBK_CANCEL_FLG_UPDATE =
          new MerchantDataAccountRow[] {
            FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE,
            CHRG_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001.withTxnHeaderId("100015"),
            CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE,
            CRWD_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001.withTxnHeaderId("100015")
          };

  public static final MerchantDataAccountRow[]
      MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_WITH_FUND_AND_CHBK_CANCEL_FLG_UPDATE =
          new MerchantDataAccountRow[] {
            FUND_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE,
            CHBK_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE
          };

  public static final MerchantDataPartyRow
      INVALID_MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS =
          INVALID_COUNTRY_MERCHANT_PARTY.withAccounts(
              ALL_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001);

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000001_WITH_FUND_VALID_ACCOUNTS =
      MERCHANT_PARTY_PO00000001.withAccounts(FUND_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001);

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000002_WITH_FUND_VALID_ACCOUNTS =
      MERCHANT_PARTY__PO00000002.withAccounts(FUND_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000002);

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000003_WITH_FUND_VALID_ACCOUNTS =
      MERCHANT_PARTY_PO00000003.withAccounts(FUND_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000003);

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000004_WITH_FUND_VALID_ACCOUNTS =
      MERCHANT_PARTY_PO00000004.withAccounts(FUND_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000004);

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000005_WITH_FUND_VALID_ACCOUNTS =
      MERCHANT_PARTY_PO00000005.withAccounts(FUND_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000005);

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000006_WITH_FUND_VALID_ACCOUNTS =
      MERCHANT_PARTY_PO00000006.withAccounts(FUND_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000006);

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000001_WITH_ALL_INVALID_ACCOUNTS =
      MERCHANT_PARTY_PO00000001.withAccounts(ALL_INVALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001);

  public static final MerchantDataPartyRow MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS =
      MERCHANT_PARTY_PO00000001.withAccounts(ALL_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001);

  public static final MerchantDataPartyRow
      MERCHANT_PARTY_PO00000001_WITH_VALID_FUND_CHBK_AND_INVALID_CHRG_CHBK_ACCOUNT =
          MERCHANT_PARTY_PO00000001.withAccounts(
              VALID_FUND_CHBK_AND_INVALID_CHRG_CRWD_ACCOUNT_WITH_PARTY_PO00000001);

  public static final MerchantDataPartyRow
      MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003_AND_VALID_FUND_CHBK_AND_INVALID_CHRG_CRWD_ACCOUNT =
          MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003.withAccounts(
              VALID_FUND_CHBK_AND_INVALID_CHRG_CRWD_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003);

  public static final MerchantDataPartyRow
      MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS_AND_ALL_FIELDS_UPDATE =
          MERCHANT_PARTY_PO00000001_WITH_ALL_FIELDS_UPDATE.withAccounts(
              ALL_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE);

  public static final MerchantDataPartyRow
      MERCHANT_PARTY_PO00000001_WITH_ALL_FIELDS_UPDATE_AND_ALL_INVALID_ACCOUNTS =
          MERCHANT_PARTY_PO00000001_WITH_ALL_FIELDS_UPDATE.withAccounts(
              ALL_INVALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001);

  public static final MerchantDataPartyRow
      MERCHANT_PARTY_PO00000001_WITH_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNTS =
          MERCHANT_PARTY_PO00000001_WITH_ALL_FIELDS_UPDATE.withAccounts(
              VALID_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNT_WITH_PARTY_PO00000001);

  public static final MerchantDataPartyRow
      MERCHANT_PARTY_PO00000001_WITH_FUND_CHBK_UPDATE_AND_WAF_INACTIVE_AND_INVALID_CHRG_CRWD_ACCOUNTS =
          MERCHANT_PARTY_PO00000001_WITH_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNTS
              .withWafFlag("N");

  public static final MerchantDataPartyRow
      MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003_AND_VALID_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNT =
          MERCHANT_PARTY_PO00000001_WITH_DIVISION_00003_UPDATE.withAccounts(
              VALID_FUND_CHBK_UPDATE_AND_INVALID_CHRG_CRWD_ACCOUNT_WITH_PARTY_PO00000001_AND_DIVISION_00003);

  public static final MerchantDataPartyRow
      MERCHANT_PARTY_PO12345_WITH_ALL_ACCOUNTS_FUND_AND_CHBK_CANCEL_FLG_UPDATE =
          MERCHANT_PARTY_PO00000001_WITH_ALL_FIELDS_UPDATE_ACCOUNT_CANCEL_FLG.withAccounts(
              ALL_VALID_MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_WITH_FUND_AND_CHBK_CANCEL_FLG_UPDATE);

  public static final MerchantDataPartyRow
      MERCHANT_PARTY_PO00000001_WITH_FUND_AND_CHBK_CANCEL_FLG_UPDATE =
          MERCHANT_PARTY_PO00000001_WITH_ALL_FIELDS_UPDATE_ACCOUNT_CANCEL_FLG.withAccounts(
              MERCHANT_ACCOUNT_WITH_PARTY_PO00000001_WITH_FUND_AND_CHBK_CANCEL_FLG_UPDATE);

  public static PartyBuilder PARTY =
      Party.builder()
          .countryCode("GBR")
          .state(null)
          .businessUnit("PO1100000001")
          .taxRegistrationNumber("GB12345")
          .validFrom(Date.valueOf("2021-01-01"))
          .accounts(EMPTY_ACCOUNT_ARRAY)
          .creationDate(CREATION_DT);

  public static final Party PARTY_WITH_ID_PO00000001 =
      PARTY.partyId("PO00000001").txnHeaderId("100001").build();

  public static final Party PARTY_WITH_ID_PO00000001_AND_DIVISION_00003 =
      PARTY_WITH_ID_PO00000001
          .withTxnHeaderId("100013")
          .withBusinessUnit("PO1100000015")
          .withValidTo(Date.valueOf("2021-02-01"));

  public static final Party PARTY_WITH_ID_PO00000001_UPDATE_AND_DIVISION_00003 =
      PARTY_WITH_ID_PO00000001_AND_DIVISION_00003.withTxnHeaderId("100023");

  public static final Party PARTY_WITH_ID_PO00000001_UPDATED_WITH_ALL_FIELDS =
      PARTY_WITH_ID_PO00000001
          .withTxnHeaderId("100014")
          .withCountryCode("USA")
          .withState("OH")
          .withBusinessUnit("PO1100000015")
          .withTaxRegistrationNumber("GB54321")
          .withValidTo(Date.valueOf("2021-02-01"));

  public static final Party
      PARTY_WITH_ID_PO00000001_UPDATED_WITH_ALL_FIELDS_AND_ACCOUNT_CANCEL_FLG =
          PARTY_WITH_ID_PO00000001_UPDATED_WITH_ALL_FIELDS.withTxnHeaderId("100015");

  public static final Party PARTY_WITH_ID_PO00000003 =
      PARTY.partyId("PO00000003").txnHeaderId("100003").build();

  public static final Party PARTY_WITH_ID_PO00000004 =
      PARTY.partyId("PO00000004").txnHeaderId("100004").build();

  public static final Party PARTY_WITH_ID_PO00000002 =
      PARTY.partyId("PO00000002").txnHeaderId("100002").countryCode("USA").state("OH").build();

  public static AccountBuilder ACCOUNT =
      Account.builder()
          .legalCounterParty("PO1100000001")
          .currency("GBP")
          .billCycleIdentifier("WPDY")
          .status("ACTIVE")
          .settlementRegionId("SETTLEMENT")
          .validFrom(Date.valueOf("2021-01-01"))
          .creationDate(CREATION_DT)
          .subAccounts(EMPTY_SUB_ACCOUNT_ARRAY);

  public static final Account FUND_ACCOUNT_WITH_PARTY_PO00000001 =
      ACCOUNT
          .accountId("50001")
          .partyId("PO00000001")
          .txnHeaderId("100001")
          .accountType("FUND")
          .build();
  public static final Account CHRG_ACCOUNT_WITH_PARTY_PO00000001 =
      ACCOUNT
          .accountId("50002")
          .partyId("PO00000001")
          .txnHeaderId("100001")
          .accountType("CHRG")
          .build();
  public static final Account CHBK_ACCOUNT_WITH_PARTY_PO00000001 =
      ACCOUNT
          .accountId("50003")
          .partyId("PO00000001")
          .txnHeaderId("100001")
          .accountType("CHBK")
          .build();
  public static final Account CRWD_ACCOUNT_WITH_PARTY_PO00000001 =
      ACCOUNT
          .accountId("50004")
          .partyId("PO00000001")
          .txnHeaderId("100001")
          .accountType("CRWD")
          .build();

  public static final Account DUPLICATE_FUND_ACCOUNT_WITH_PARTY_PO00000001 =
      FUND_ACCOUNT_WITH_PARTY_PO00000001.withAccountId("50011");

  public static final Account DUPLICATE_CHRG_ACCOUNT_WITH_PARTY_PO00000001 =
      CHRG_ACCOUNT_WITH_PARTY_PO00000001.withAccountId("50012");

  public static final Account DUPLICATE_CHBK_ACCOUNT_WITH_PARTY_PO00000001 =
      CHBK_ACCOUNT_WITH_PARTY_PO00000001.withAccountId("50013");

  public static final Account DUPLICATE_CRWD_ACCOUNT_WITH_PARTY_PO00000001 =
      CRWD_ACCOUNT_WITH_PARTY_PO00000001.withAccountId("50014");

  public static final Account FUND_ACCOUNT_WITH_PARTY_PO00000002 =
      ACCOUNT
          .accountId("60001")
          .partyId("PO00000002")
          .txnHeaderId("100002")
          .accountType("FUND")
          .build();
  public static final Account CHRG_ACCOUNT_WITH_PARTY_PO00000002 =
      ACCOUNT
          .accountId("60002")
          .partyId("PO00000002")
          .txnHeaderId("100002")
          .accountType("CHRG")
          .build();
  public static final Account CHBK_ACCOUNT_WITH_PARTY_PO00000002 =
      ACCOUNT
          .accountId("60003")
          .partyId("PO00000002")
          .txnHeaderId("100002")
          .accountType("CHBK")
          .build();
  public static final Account CRWD_ACCOUNT_WITH_PARTY_PO00000002 =
      ACCOUNT
          .accountId("60004")
          .partyId("PO00000002")
          .txnHeaderId("100002")
          .accountType("CRWD")
          .build();

  public static final Account FUND_ACCOUNT_WITH_PARTY_PO00000003 =
      FUND_ACCOUNT_WITH_PARTY_PO00000001
          .withAccountId("70001")
          .withPartyId("PO00000003")
          .withTxnHeaderId("100003");

  public static final Account FUND_ACCOUNT_WITH_PARTY_PO00000004 =
      FUND_ACCOUNT_WITH_PARTY_PO00000001
          .withAccountId("80001")
          .withPartyId("PO00000004")
          .withTxnHeaderId("100004");

  public static final Account FUND_ACCOUNT_WITH_PARTY_PO00000005 =
      FUND_ACCOUNT_WITH_PARTY_PO00000001
          .withAccountId("90001")
          .withPartyId("PO00000005")
          .withTxnHeaderId("100005");

  public static final Account FUND_ACCOUNT_WITH_PARTY_PO00000006 =
      FUND_ACCOUNT_WITH_PARTY_PO00000001
          .withAccountId("100001")
          .withPartyId("PO00000006")
          .withTxnHeaderId("100006");

  public static final Account FAILED_ACCOUNT_WITH_PARTY_PO00000003 =
      ACCOUNT
          .accountId("99999")
          .partyId("PO00000003")
          .txnHeaderId("900001")
          .accountType("CRWD")
          .build();

  public static final Account FUND_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE =
      FUND_ACCOUNT_WITH_PARTY_PO00000001
          .withTxnHeaderId("100014")
          .withAccountId("50001")
          .withBillCycleIdentifier("WPMO")
          .withValidTo(Date.valueOf("2021-02-01"))
          .withSettlementRegionId("SETTLEMENT_UPDATE");

  public static final Account CHRG_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE =
      CHRG_ACCOUNT_WITH_PARTY_PO00000001
          .withTxnHeaderId("100014")
          .withAccountId("50002")
          .withBillCycleIdentifier("WPMO")
          .withValidTo(Date.valueOf("2021-02-01"))
          .withSettlementRegionId("SETTLEMENT_UPDATE");

  public static final Account CHBK_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE =
      CHBK_ACCOUNT_WITH_PARTY_PO00000001
          .withTxnHeaderId("100014")
          .withAccountId("50003")
          .withBillCycleIdentifier("WPMO")
          .withValidTo(Date.valueOf("2021-02-01"))
          .withSettlementRegionId("SETTLEMENT_UPDATE");

  public static final Account CRWD_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE =
      CRWD_ACCOUNT_WITH_PARTY_PO00000001
          .withTxnHeaderId("100014")
          .withAccountId("50004")
          .withBillCycleIdentifier("WPMO")
          .withValidTo(Date.valueOf("2021-02-01"))
          .withSettlementRegionId("SETTLEMENT_UPDATE");

  public static final Account FUND_ACCOUNT_WITH_PARTY_PO00000001_AND_LCP_PO1100000003 =
      FUND_ACCOUNT_WITH_PARTY_PO00000001
          .withAccountId("50013")
          .withTxnHeaderId("100013")
          .withLegalCounterParty("PO1100000003")
          .withValidTo(Date.valueOf("2021-02-01"));

  public static final Account CHBK_ACCOUNT_WITH_PARTY_PO00000001_AND_LCP_PO1100000003 =
      CHBK_ACCOUNT_WITH_PARTY_PO00000001
          .withAccountId("50033")
          .withTxnHeaderId("100013")
          .withLegalCounterParty("PO1100000003")
          .withValidTo(Date.valueOf("2021-02-01"));

  public static final Account FUND_ACCOUNT_UPDATE_WITH_PARTY_PO00000001_AND_LCP_PO1100000003 =
      FUND_ACCOUNT_WITH_PARTY_PO00000001_AND_LCP_PO1100000003
          .withTxnHeaderId("100023")
          .withBillCycleIdentifier("WPMO")
          .withValidTo(Date.valueOf("2021-03-01"));

  public static final Account CHBK_ACCOUNT_UPDATE_WITH_PARTY_PO00000001_AND_LCP_PO1100000003 =
      CHBK_ACCOUNT_WITH_PARTY_PO00000001_AND_LCP_PO1100000003
          .withTxnHeaderId("100023")
          .withBillCycleIdentifier("WPMO")
          .withValidTo(Date.valueOf("2021-03-01"));

  public static final Account ONLY_FUND_ACCOUNT_WITH_PARTY_PO00000001 =
      ACCOUNT
          .accountId("50001")
          .partyId("PO00000001")
          .txnHeaderId("100001")
          .accountType("FUND")
          .build();

  public static final Account ONLY_FUND_ACCOUNT_WITH_PARTY_PO00000002 =
      ACCOUNT
          .accountId("60001")
          .partyId("PO00000002")
          .txnHeaderId("100002")
          .accountType("FUND")
          .build();

  public static final Account ONLY_FUND_ACCOUNT_WITH_PARTY_PO00000003 =
      ACCOUNT
          .accountId("70001")
          .partyId("PO00000003")
          .txnHeaderId("100003")
          .accountType("FUND")
          .build();

  public static final Account ONLY_FUND_ACCOUNT_WITH_PARTY_PO00000004 =
      ACCOUNT
          .accountId("80001")
          .partyId("PO00000004")
          .txnHeaderId("100004")
          .accountType("FUND")
          .build();

  public static final Account ONLY_FUND_ACCOUNT_WITH_PARTY_PO00000005 =
      ACCOUNT
          .accountId("90001")
          .partyId("PO00000005")
          .txnHeaderId("100005")
          .accountType("FUND")
          .build();

  public static final Account FUND_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE =
      FUND_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE
          .withTxnHeaderId("100015")
          .withStatus("INACTIVE");
  public static final Account CHBK_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE =
      CHBK_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE
          .withTxnHeaderId("100015")
          .withStatus("INACTIVE");

  public static SubAccountBuilder SUB_ACCOUNT =
      SubAccount.builder()
          .status("ACTIVE")
          .validFrom(Date.valueOf("2021-01-01"))
          .creationDate(CREATION_DT);

  public static final SubAccount FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_50001 =
      SUB_ACCOUNT.subAccountId("51001").accountId("50001").subAccountType("FUND").build();
  public static final SubAccount CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_50002 =
      SUB_ACCOUNT.subAccountId("51002").accountId("50002").subAccountType("CHRG").build();
  public static final SubAccount CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_50003 =
      SUB_ACCOUNT.subAccountId("51003").accountId("50003").subAccountType("CHBK").build();
  public static final SubAccount CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_50004 =
      SUB_ACCOUNT.subAccountId("51004").accountId("50004").subAccountType("CRWD").build();

  public static final SubAccount FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_60001 =
      SUB_ACCOUNT.subAccountId("61001").accountId("60001").subAccountType("FUND").build();
  public static final SubAccount CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_60002 =
      SUB_ACCOUNT.subAccountId("61002").accountId("60002").subAccountType("CHRG").build();
  public static final SubAccount CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_60003 =
      SUB_ACCOUNT.subAccountId("61003").accountId("60003").subAccountType("CHBK").build();
  public static final SubAccount CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_60004 =
      SUB_ACCOUNT.subAccountId("61004").accountId("60004").subAccountType("CRWD").build();

  public static final SubAccount FAILED_SUB_ACCOUNT_WITH_ACCOUNT_ID_99999 =
      SUB_ACCOUNT.subAccountId("91001").accountId("99999").subAccountType("CRWD").build();

  public static final SubAccount FUND_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50001 =
      FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_50001.withValidTo(Date.valueOf("2021-02-01"));
  public static final SubAccount CHRG_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50002 =
      CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_50002.withValidTo(Date.valueOf("2021-02-01"));
  public static final SubAccount CHBK_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50003 =
      CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_50003.withValidTo(Date.valueOf("2021-02-01"));
  public static final SubAccount CRWD_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50004 =
      CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_50004.withValidTo(Date.valueOf("2021-02-01"));

  public static final SubAccount FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_50013 =
      FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_50001
          .withAccountId("50013")
          .withSubAccountId("51013")
          .withValidTo(Date.valueOf("2021-02-01"));
  public static final SubAccount CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_50033 =
      CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_50003
          .withAccountId("50033")
          .withSubAccountId("51033")
          .withValidTo(Date.valueOf("2021-02-01"));

  public static final SubAccount FUND_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50013 =
      FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_50013.withValidTo(Date.valueOf("2021-03-01"));
  public static final SubAccount CHBK_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50033 =
      CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_50033.withValidTo(Date.valueOf("2021-03-01"));

  public static final SubAccount DRES_SUB_ACCOUNT_WITH_ID_50001_UPDATE =
      FUND_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50001
          .withSubAccountId("51005")
          .withSubAccountType("DRES");

  public static final SubAccount DUPLICATE_FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_50011 =
      FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_50001.withSubAccountId("51011").withAccountId("50011");

  public static final SubAccount DUPLICATE_CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_50012 =
      CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_50002.withSubAccountId("51021").withAccountId("50012");

  public static final SubAccount DUPLICATE_CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_50013 =
      CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_50003.withSubAccountId("51031").withAccountId("50013");

  public static final SubAccount DUPLICATE_CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_50014 =
      CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_50004.withSubAccountId("51041").withAccountId("50014");

  public static final SubAccount FUND_SUB_ACCOUNT_WITH_ID_50001_CANCEL_FLG_UPDATE =
      FUND_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50001.withStatus("INACTIVE");
  public static final SubAccount CHBK_SUB_ACCOUNT_WITH_ID_50003_CANCEL_FLG_UPDATE =
      CHBK_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50003.withStatus("INACTIVE");

  public static WithholdFundBuilder WITHHOLD_FUND =
      WithholdFund.builder()
          .withholdFundPercentage(100D)
          .withholdFundType("WAF")
          .status("ACTIVE")
          .validFrom(Date.valueOf("2021-01-01"))
          .creationDate(CREATION_DT);

  public static final WithholdFund WITHHOLD_FUND_WITH_ACCOUNT_ID_50001 =
      WITHHOLD_FUND.accountId("50001").build();

  public static final WithholdFund WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_UPDATE =
      WITHHOLD_FUND.accountId("50001").validTo(Date.valueOf("2021-02-01")).build();

  public static final WithholdFund INACTIVE_WITHHOLD_FUND_WITH_ACCOUNT_ID_50001 =
      WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_UPDATE
          .withStatus("INACTIVE")
          .withValidTo(Date.valueOf(LocalDate.now()));

  public static final WithholdFund
      WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_UPDATE_FROM_INACTIVE_TO_ACTIVE =
          WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_UPDATE.withValidFrom(Date.valueOf(LocalDate.now()));

  public static final WithholdFund WITHHOLD_FUND_WITH_ACCOUNT_ID_60001 =
      WITHHOLD_FUND.accountId("60001").build();

  public static final WithholdFund FAILED_WITHHOLD_FUND_WITH_ACCOUNT_ID_99999 =
      WITHHOLD_FUND.accountId("99999").build();

  public static final WithholdFund WITHHOLD_FUND_WITH_ACCOUNT_ID_50013 =
      WITHHOLD_FUND_WITH_ACCOUNT_ID_50001
          .withAccountId("50013")
          .withValidTo(Date.valueOf("2021-02-01"));

  public static final WithholdFund WITHHOLD_FUND_WITH_ACCOUNT_ID_50013_UPDATE_FOR_DIVISION_00003 =
      WITHHOLD_FUND_WITH_ACCOUNT_ID_50013.withValidTo(Date.valueOf("2021-03-01"));

  public static final WithholdFund DUPLICATE_WITHHOLD_FUND_WITH_ACCOUNT_ID_50011 =
      WITHHOLD_FUND_WITH_ACCOUNT_ID_50001.withAccountId("50011");

  public static final WithholdFund WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_CANCEL_FLG_UPDATE =
      WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_UPDATE.withStatus("INACTIVE");

  public static final Account[] ALL_ACCOUNTS_WITH_PARTY_PO00000001 =
      new Account[] {
        FUND_ACCOUNT_WITH_PARTY_PO00000001
            .withSubAccounts(new SubAccount[] {FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_50001})
            .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50001),
        CHRG_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
            new SubAccount[] {CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_50002}),
        CHBK_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
            new SubAccount[] {CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_50003}),
        CRWD_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
            new SubAccount[] {CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_50004})
      };

  public static final Account[] ALL_ACCOUNTS_WITH_PARTY_PO00000001_AND_INACTIVE_WAF =
      new Account[] {
        FUND_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE
            .withSubAccounts(new SubAccount[] {FUND_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50001})
            .withWithholdFund(INACTIVE_WITHHOLD_FUND_WITH_ACCOUNT_ID_50001),
        CHRG_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
            new SubAccount[] {CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_50002}),
        CHBK_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE.withSubAccounts(
            new SubAccount[] {CHBK_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50003}),
        CRWD_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
            new SubAccount[] {CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_50004})
      };

  public static final Account[]
      ALL_ACCOUNTS_WITH_PARTY_PO00000001_AND_UPDATED_WAF_FROM_INACTIVE_TO_ACTIVE =
          new Account[] {
            FUND_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE
                .withSubAccounts(new SubAccount[] {FUND_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50001})
                .withWithholdFund(
                    WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_UPDATE_FROM_INACTIVE_TO_ACTIVE),
            CHRG_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
                new SubAccount[] {CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_50002}),
            CHBK_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE.withSubAccounts(
                new SubAccount[] {CHBK_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50003}),
            CRWD_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
                new SubAccount[] {CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_50004})
          };

  public static final Account[] ALL_ACCOUNTS_WITH_PARTY_PO00000002 =
      new Account[] {
        FUND_ACCOUNT_WITH_PARTY_PO00000002
            .withSubAccounts(new SubAccount[] {FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_60001})
            .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_60001),
        CHRG_ACCOUNT_WITH_PARTY_PO00000002.withSubAccounts(
            new SubAccount[] {CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_60002}),
        CHBK_ACCOUNT_WITH_PARTY_PO00000002.withSubAccounts(
            new SubAccount[] {CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_60003}),
        CRWD_ACCOUNT_WITH_PARTY_PO00000002.withSubAccounts(
            new SubAccount[] {CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_60004})
      };

  public static final Account[] FUND_AND_CHBK_ACCOUNT_WITH_PARTY_PO00000001 =
      new Account[] {
        FUND_ACCOUNT_WITH_PARTY_PO00000001
            .withSubAccounts(new SubAccount[] {FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_50001})
            .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50001),
        CHBK_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
            new SubAccount[] {CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_50003})
      };

  public static final Account[] CHRG_AND_CRWD_ACCOUNT_WITH_PARTY_PO00000001 =
      new Account[] {
        CHRG_ACCOUNT_WITH_PARTY_PO00000001
            .withTxnHeaderId("100015")
            .withSubAccounts(new SubAccount[] {CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_50002}),
        CRWD_ACCOUNT_WITH_PARTY_PO00000001
            .withTxnHeaderId("100015")
            .withSubAccounts(new SubAccount[] {CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_50004})
      };

  public static final Account[] ALL_ACCOUNTS_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE =
      new Account[] {
        CHBK_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE.withSubAccounts(
            new SubAccount[] {CHBK_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50003}),
        CHRG_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE.withSubAccounts(
            new SubAccount[] {CHRG_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50002}),
        CRWD_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE.withSubAccounts(
            new SubAccount[] {CRWD_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50004}),
        FUND_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE
            .withSubAccounts(new SubAccount[] {FUND_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50001})
            .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_UPDATE)
      };

  public static final Account[]
      FUND_AND_CHBK_WITH_PARTY_PO00000001_AND_LCP_PO1100000001_AND_PO1100000003 =
          new Account[] {
            FUND_ACCOUNT_WITH_PARTY_PO00000001
                .withSubAccounts(new SubAccount[] {FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_50001})
                .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50001),
            CHBK_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
                new SubAccount[] {CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_50003}),
            FUND_ACCOUNT_WITH_PARTY_PO00000001_AND_LCP_PO1100000003
                .withSubAccounts(new SubAccount[] {FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_50013})
                .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50013),
            CHBK_ACCOUNT_WITH_PARTY_PO00000001_AND_LCP_PO1100000003.withSubAccounts(
                new SubAccount[] {CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_50033})
          };

  public static final Account[] ALL_ACCOUNTS_WITH_PARTY_PO00000001_AND_UPDATED_FUND_CHBK_ACCOUNTS =
      new Account[] {
        FUND_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE
            .withSubAccounts(new SubAccount[] {FUND_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50001})
            .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_UPDATE),
        CHRG_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
            new SubAccount[] {CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_50002}),
        CHBK_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE.withSubAccounts(
            new SubAccount[] {CHBK_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50003}),
        CRWD_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
            new SubAccount[] {CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_50004})
      };

  public static final Account[]
      FUND_AND_CHBK_UPDATE_WITH_PARTY_PO00000001_AND_LCP_PO1100000001_AND_PO1100000003 =
          new Account[] {
            FUND_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE
                .withSubAccounts(new SubAccount[] {FUND_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50001})
                .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_UPDATE),
            CHBK_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE.withSubAccounts(
                new SubAccount[] {CHBK_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50003}),
            FUND_ACCOUNT_UPDATE_WITH_PARTY_PO00000001_AND_LCP_PO1100000003
                .withSubAccounts(new SubAccount[] {FUND_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50013})
                .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50013_UPDATE_FOR_DIVISION_00003),
            CHBK_ACCOUNT_UPDATE_WITH_PARTY_PO00000001_AND_LCP_PO1100000003.withSubAccounts(
                new SubAccount[] {CHBK_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50033})
          };

  public static final Account[] ALL_DUPLICATE_ACCOUNTS_WITH_PARTY_PO00000001 =
      new Account[] {
        FUND_ACCOUNT_WITH_PARTY_PO00000001
            .withSubAccounts(new SubAccount[] {FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_50001})
            .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50001),
        DUPLICATE_FUND_ACCOUNT_WITH_PARTY_PO00000001
            .withSubAccounts(new SubAccount[] {DUPLICATE_FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_50011})
            .withWithholdFund(DUPLICATE_WITHHOLD_FUND_WITH_ACCOUNT_ID_50011),
        CHRG_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
            new SubAccount[] {CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_50002}),
        DUPLICATE_CHRG_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
            new SubAccount[] {DUPLICATE_CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_50012}),
        CHBK_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
            new SubAccount[] {CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_50003}),
        DUPLICATE_CHBK_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
            new SubAccount[] {DUPLICATE_CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_50013}),
        CRWD_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
            new SubAccount[] {CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_50004}),
        DUPLICATE_CRWD_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
            new SubAccount[] {DUPLICATE_CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_50014})
      };

  public static final Account[] ALL_ACCOUNTS_WITH_PARTY_PO00000001_AND_FUND_CHBK_CANCEL_FLG_UPDATE =
      new Account[] {
        FUND_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE
                .withSubAccounts(
                    new SubAccount[] {FUND_SUB_ACCOUNT_WITH_ID_50001_CANCEL_FLG_UPDATE})
                .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_CANCEL_FLG_UPDATE),
            CHRG_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
                new SubAccount[] {CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_50002}),
        CHBK_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE.withSubAccounts(
                new SubAccount[] {CHBK_SUB_ACCOUNT_WITH_ID_50003_CANCEL_FLG_UPDATE}),
            CRWD_ACCOUNT_WITH_PARTY_PO00000001.withSubAccounts(
                new SubAccount[] {CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_50004})
      };

  public static final Account[] ACCOUNTS_WITH_PARTY_PO00000001_AND_FUND_CHBK_CANCEL_FLG_UPDATE =
      new Account[] {
        FUND_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE
            .withSubAccounts(new SubAccount[] {FUND_SUB_ACCOUNT_WITH_ID_50001_CANCEL_FLG_UPDATE})
            .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_CANCEL_FLG_UPDATE),
        CHBK_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE.withSubAccounts(
            new SubAccount[] {CHBK_SUB_ACCOUNT_WITH_ID_50003_CANCEL_FLG_UPDATE})
      };

  public static final Account[]
      ACCOUNTS_WITH_PARTY_PO00000001_AND_FUND_CHBK_NEW_AND_CANCEL_FLG_UPDATE =
          new Account[] {
            FUND_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE
                .withSubAccounts(new SubAccount[] {FUND_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50001})
                .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_UPDATE),
            CHBK_ACCOUNT_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE.withSubAccounts(
                new SubAccount[] {CHBK_SUB_ACCOUNT_UPDATE_WITH_ACCOUNT_ID_50003}),
            FUND_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE
                .withSubAccounts(
                    new SubAccount[] {FUND_SUB_ACCOUNT_WITH_ID_50001_CANCEL_FLG_UPDATE})
                .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_CANCEL_FLG_UPDATE),
            CHBK_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE.withSubAccounts(
                new SubAccount[] {CHBK_SUB_ACCOUNT_WITH_ID_50003_CANCEL_FLG_UPDATE})
          };

  public static final Account[]
      ACCOUNTS_WITH_PARTY_PO00000001_AND_MULTIPLE_SAME_FUND_CHBK_CANCEL_FLG_UPDATE =
          new Account[] {
            FUND_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE
                .withSubAccounts(
                    new SubAccount[] {FUND_SUB_ACCOUNT_WITH_ID_50001_CANCEL_FLG_UPDATE})
                .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_CANCEL_FLG_UPDATE),
            CHBK_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE.withSubAccounts(
                new SubAccount[] {CHBK_SUB_ACCOUNT_WITH_ID_50003_CANCEL_FLG_UPDATE}),
            FUND_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE
                .withSubAccounts(
                    new SubAccount[] {FUND_SUB_ACCOUNT_WITH_ID_50001_CANCEL_FLG_UPDATE})
                .withWithholdFund(WITHHOLD_FUND_WITH_ACCOUNT_ID_50001_CANCEL_FLG_UPDATE),
            CHBK_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE.withSubAccounts(
                new SubAccount[] {CHBK_SUB_ACCOUNT_WITH_ID_50003_CANCEL_FLG_UPDATE})
          };

  public static final Party PARTY_PO00000001_WITH_ALL_ACCOUNTS =
      PARTY_WITH_ID_PO00000001.withAccounts(ALL_ACCOUNTS_WITH_PARTY_PO00000001);

  public static final Party PARTY_PO00000002_WITH_ALL_ACCOUNTS =
      PARTY_WITH_ID_PO00000002.withAccounts(ALL_ACCOUNTS_WITH_PARTY_PO00000002);

  public static final Party PARTY_PO00000001_WITH_FUND_AND_CHBK_ACCOUNT =
      PARTY_WITH_ID_PO00000001.withAccounts(FUND_AND_CHBK_ACCOUNT_WITH_PARTY_PO00000001);

  public static final Party PARTY_PO00000001_CANCEL_FLG_WITH_CHRG_AND_CRWD_ACCOUNT =
      PARTY_WITH_ID_PO00000001_UPDATED_WITH_ALL_FIELDS_AND_ACCOUNT_CANCEL_FLG.withAccounts(
          CHRG_AND_CRWD_ACCOUNT_WITH_PARTY_PO00000001);

  public static final Party PARTY_PO00000001_WITH_FUND_CHBK_ACCOUNTS_FOR_DIVISION_00001_AND_00003 =
      PARTY_WITH_ID_PO00000001_AND_DIVISION_00003.withAccounts(
          FUND_AND_CHBK_WITH_PARTY_PO00000001_AND_LCP_PO1100000001_AND_PO1100000003);

  public static final Party PARTY_PO00000001_WITH_ALL_ACCOUNTS_AND_ALL_FIELDS_UPDATE =
      PARTY_WITH_ID_PO00000001_UPDATED_WITH_ALL_FIELDS.withAccounts(
          ALL_ACCOUNTS_WITH_PARTY_PO00000001_AND_ALL_FIELDS_UPDATE);

  public static final Party PARTY_PO00000001_UPDATED_WITH_ALL_FIELDS_AND_OLD_ACCOUNTS =
      PARTY_WITH_ID_PO00000001_UPDATED_WITH_ALL_FIELDS.withAccounts(
          ALL_ACCOUNTS_WITH_PARTY_PO00000001);

  public static final Party PARTY_PO00000001_WITH_UPDATED_FUND_AND_CHBK_ACCOUNT =
      PARTY_WITH_ID_PO00000001_UPDATED_WITH_ALL_FIELDS.withAccounts(
          ALL_ACCOUNTS_WITH_PARTY_PO00000001_AND_UPDATED_FUND_CHBK_ACCOUNTS);

  public static final Party
      PARTY_PO00000001_WITH_FUND_CHBK_ACCOUNTS_UPDATE_FOR_DIVISION_00001_AND_00003 =
          PARTY_WITH_ID_PO00000001_UPDATE_AND_DIVISION_00003.withAccounts(
              FUND_AND_CHBK_UPDATE_WITH_PARTY_PO00000001_AND_LCP_PO1100000001_AND_PO1100000003);

  public static final Party PARTY_PO00000001_WITH_UPDATED_FUND_AND_CHBK_ACCOUNT_AND_INACTIVE_WAF =
      PARTY_WITH_ID_PO00000001_UPDATED_WITH_ALL_FIELDS.withAccounts(
          ALL_ACCOUNTS_WITH_PARTY_PO00000001_AND_INACTIVE_WAF);

  public static final Party
      PARTY_PO00000001_WITH_UPDATED_FUND_AND_CHBK_ACCOUNT_AND_WAF_UPDATE_FROM_INACTIVE_TO_ACTIVE =
          PARTY_WITH_ID_PO00000001_UPDATED_WITH_ALL_FIELDS.withAccounts(
              ALL_ACCOUNTS_WITH_PARTY_PO00000001_AND_UPDATED_WAF_FROM_INACTIVE_TO_ACTIVE);

  public static final Party PARTY_PO00000001_WITH_FUND_ACCOUNTS =
      PARTY
          .partyId("PO00000001")
          .txnHeaderId("100001")
          .build()
          .withAccounts(new Account[] {ONLY_FUND_ACCOUNT_WITH_PARTY_PO00000001});

  public static final Party PARTY_PO00000002_WITH_FUND_ACCOUNTS =
      PARTY
          .partyId("PO00000002")
          .txnHeaderId("100002")
          .build()
          .withAccounts(new Account[] {ONLY_FUND_ACCOUNT_WITH_PARTY_PO00000002});

  public static final Party PARTY_PO00000003_WITH_FUND_ACCOUNTS =
      PARTY
          .partyId("PO00000003")
          .txnHeaderId("100003")
          .build()
          .withAccounts(new Account[] {ONLY_FUND_ACCOUNT_WITH_PARTY_PO00000003});

  public static final Party PARTY_PO00000004_WITH_FUND_ACCOUNTS =
      PARTY
          .partyId("PO00000004")
          .txnHeaderId("100004")
          .build()
          .withAccounts(new Account[] {ONLY_FUND_ACCOUNT_WITH_PARTY_PO00000004});

  public static final Party PARTY_PO00000001_WITH_CANCELLEDFUND_ACCOUNTS =
      PARTY
          .partyId("PO00000005")
          .txnHeaderId("100005")
          .build()
          .withAccounts(new Account[] {ONLY_FUND_ACCOUNT_WITH_PARTY_PO00000005});

  public static final Party PARTY_PO00000005_WITH_FUND_ACCOUNTS =
      PARTY
          .partyId("PO00000005")
          .txnHeaderId("100005")
          .build()
          .withAccounts(new Account[] {FUND_ACCOUNT_WITH_PARTY_PO00000001_CANCEL_FLG_UPDATE});

  public static final Party PARTY_PO00000001_WITH_DUPLICATE_ACCOUNTS_AND_SUB_ACCOUNTS =
      PARTY_WITH_ID_PO00000001.withAccounts(ALL_DUPLICATE_ACCOUNTS_WITH_PARTY_PO00000001);

  public static final Party PARTY_PO00000001_WITH_ALL_ACCOUNTS_AND_CANCELED_FUND_CHBK_ACCOUNT =
      PARTY_WITH_ID_PO00000001_UPDATED_WITH_ALL_FIELDS_AND_ACCOUNT_CANCEL_FLG.withAccounts(
          ALL_ACCOUNTS_WITH_PARTY_PO00000001_AND_FUND_CHBK_CANCEL_FLG_UPDATE);

  public static final Party PARTY_PO00000001_WITH_CANCELED_FUND_CHBK_ACCOUNT =
      PARTY_WITH_ID_PO00000001.withAccounts(
          ACCOUNTS_WITH_PARTY_PO00000001_AND_FUND_CHBK_CANCEL_FLG_UPDATE);

  public static final Party PARTY_PO00000001_WITH_NEW_AND_CANCELED_FUND_CHBK_ACCOUNT =
      PARTY_WITH_ID_PO00000001_UPDATED_WITH_ALL_FIELDS.withAccounts(
          ACCOUNTS_WITH_PARTY_PO00000001_AND_FUND_CHBK_NEW_AND_CANCEL_FLG_UPDATE);

  public static final Party PARTY_PO00000001_WITH_NEW_AND_MULTIPLE_SAME_CANCELED_FUND_CHBK_ACCOUNT =
      PARTY_WITH_ID_PO00000001_UPDATED_WITH_ALL_FIELDS_AND_ACCOUNT_CANCEL_FLG.withAccounts(
          ACCOUNTS_WITH_PARTY_PO00000001_AND_MULTIPLE_SAME_FUND_CHBK_CANCEL_FLG_UPDATE);

  // account hierarchy transactions
  public static final AccountHierarchyDataRowBuilder ACCOUNT_HIERARCHY_DATA =
      AccountHierarchyDataRow.builder().currency("GBP").cisDivision("00001");

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002 =
          ACCOUNT_HIERARCHY_DATA
              .txnHeaderId("500001")
              .parentPartyId("PO00000001")
              .childPartyId("PO00000002")
              .accountType("FUND")
              .parentAccountId("50001")
              .childAccountId("60001")
              .validFrom(Date.valueOf("2021-01-01"))
              .validTo(Date.valueOf("2021-01-31"))
              .build();

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003 =
          ACCOUNT_HIERARCHY_DATA
              .txnHeaderId("500002")
              .parentPartyId("PO00000002")
              .childPartyId("PO00000003")
              .accountType("FUND")
              .parentAccountId("60001")
              .childAccountId("70001")
              .validFrom(Date.valueOf("2021-02-01"))
              .validTo(Date.valueOf("2021-02-28"))
              .build();

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000004 =
          ACCOUNT_HIERARCHY_DATA
              .txnHeaderId("500003")
              .parentPartyId("PO00000002")
              .childPartyId("PO00000004")
              .accountType("FUND")
              .parentAccountId("60001")
              .childAccountId("80001")
              .validFrom(Date.valueOf("2021-01-21"))
              .validTo(Date.valueOf("2021-01-31"))
              .build();

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000003_AND_PO00000005 =
          ACCOUNT_HIERARCHY_DATA
              .txnHeaderId("500004")
              .parentPartyId("PO00000003")
              .childPartyId("PO00000005")
              .accountType("FUND")
              .parentAccountId("70001")
              .childAccountId("90001")
              .validFrom(Date.valueOf("2021-01-01"))
              .validTo(Date.valueOf("2021-01-31"))
              .build();

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000004_AND_PO00000003 =
          ACCOUNT_HIERARCHY_DATA
              .txnHeaderId("500005")
              .parentPartyId("PO00000004")
              .childPartyId("PO00000003")
              .accountType("FUND")
              .parentAccountId("80001")
              .childAccountId("70001")
              .validFrom(Date.valueOf("2021-01-21"))
              .validTo(Date.valueOf("2021-01-31"))
              .build();

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000006_AND_PO00000003 =
          ACCOUNT_HIERARCHY_DATA
              .txnHeaderId("500006")
              .parentPartyId("PO00000006")
              .childPartyId("PO00000003")
              .accountType("FUND")
              .parentAccountId("100001")
              .childAccountId("70001")
              .validFrom(Date.valueOf("2021-01-25"))
              .validTo(Date.valueOf("2021-02-25"))
              .build();

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000003_AND_PO00000002 =
          ACCOUNT_HIERARCHY_DATA
              .txnHeaderId("500007")
              .parentPartyId("PO00000003")
              .childPartyId("PO00000002")
              .accountType("FUND")
              .parentAccountId("70001")
              .childAccountId("60001")
              .validFrom(Date.valueOf("2021-02-01"))
              .validTo(Date.valueOf("2021-02-28"))
              .build();

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE =
          ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002
              .withTxnHeaderId("510001")
              .withValidFrom(Date.valueOf("2021-02-01"))
              .withValidTo(null);

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003_UPDATE =
          ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003
              .withTxnHeaderId("510002")
              .withValidFrom(Date.valueOf("2021-03-01"))
              .withValidTo(null);

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL =
          ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002
              .withTxnHeaderId("510003")
              .withValidFrom(Date.valueOf("2021-01-01"))
              .withValidTo(null);

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_INVALID_UPDATE_NULL_POINTER =
          ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002
              .withTxnHeaderId("510004")
              .withValidFrom(null)
              .withValidTo(null);

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CROSS_OVER_VALIDITY =
          ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002
              .withTxnHeaderId("510005")
              .withValidFrom(Date.valueOf("2021-01-21"))
              .withValidTo(Date.valueOf("2021-01-31"));

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_JAN_VALIDITY_EXPIRED =
          ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002
              .withTxnHeaderId("510006")
              .withValidFrom(Date.valueOf("2021-01-01"))
              .withValidTo(Date.valueOf("2021-01-31"));

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_FEB_VALIDITY_EXPIRED =
          ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002
              .withTxnHeaderId("510007")
              .withValidFrom(Date.valueOf("2021-02-01"))
              .withValidTo(Date.valueOf("2021-02-28"));

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_TXN_HEADER_ID_NULL =
          ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002
              .withTxnHeaderId(null)
              .withValidFrom(Date.valueOf("2021-01-01"))
              .withValidTo(Date.valueOf("2021-01-31"));

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CANCEL_FLG =
          ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002
              .withTxnHeaderId("520001")
              .withCancellationFlag("Y");

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_WITH_FEB_VALIDITY_AND_CANCEL_FLG =
          ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_FEB_VALIDITY_EXPIRED
              .withTxnHeaderId("520002")
              .withCancellationFlag("Y");

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_WITH_JAN_TO_FEB_CANCEL_FLG =
          ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_FEB_VALIDITY_EXPIRED
              .withTxnHeaderId("520003")
              .withValidFrom(Date.valueOf("2021-01-01"))
              .withValidTo(Date.valueOf("2021-02-28"))
              .withCancellationFlag("Y");

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_CANCEL_FLG_AND_TXN_HEADER_ID_NULL =
          ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_TXN_HEADER_ID_NULL
              .withCancellationFlag("Y");

  public static final AccountHierarchyDataRow
      ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_CANCEL_FLG_AND_VALID_FROM_NULL =
          ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_TXN_HEADER_ID_NULL
              .withTxnHeaderId("520004")
              .withValidFrom(null)
              .withCancellationFlag("Y");

  public static final AccountHierarchy ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002 =
      AccountHierarchy.builder()
          .txnHeaderId("500001")
          .parentPartyId("PO00000001")
          .childPartyId("PO00000002")
          .parentAccountId("50001")
          .childAccountId("60001")
          .status("ACTIVE")
          .validFrom(Date.valueOf("2021-01-01"))
          .validTo(Date.valueOf("2021-01-31"))
          .creationDate(CREATION_DT)
          .partitionId(PARTITION_ID_NOT_APPLICABLE)
          .build();

  public static final AccountHierarchy
      ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL =
          ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002
              .withTxnHeaderId("510003")
              .withValidTo(null);

  public static final AccountHierarchy ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003 =
      AccountHierarchy.builder()
          .txnHeaderId("500002")
          .parentPartyId("PO00000002")
          .childPartyId("PO00000003")
          .parentAccountId("60001")
          .childAccountId("70001")
          .status("ACTIVE")
          .validFrom(Date.valueOf("2021-02-01"))
          .validTo(Date.valueOf("2021-02-28"))
          .creationDate(CREATION_DT)
          .partitionId(PARTITION_ID_NOT_APPLICABLE)
          .build();

  public static final AccountHierarchy ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000004 =
      AccountHierarchy.builder()
          .txnHeaderId("500003")
          .parentPartyId("PO00000002")
          .childPartyId("PO00000004")
          .parentAccountId("60001")
          .childAccountId("80001")
          .status("ACTIVE")
          .validFrom(Date.valueOf("2021-01-21"))
          .validTo(Date.valueOf("2021-01-31"))
          .creationDate(CREATION_DT)
          .partitionId(PARTITION_ID_NOT_APPLICABLE)
          .build();

  public static final AccountHierarchy ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000005 =
      AccountHierarchy.builder()
          .txnHeaderId("500004")
          .parentPartyId("PO00000003")
          .childPartyId("PO00000005")
          .parentAccountId("70001")
          .childAccountId("90001")
          .status("ACTIVE")
          .validFrom(Date.valueOf("2021-01-01"))
          .validTo(Date.valueOf("2021-01-31"))
          .creationDate(CREATION_DT)
          .partitionId(PARTITION_ID_NOT_APPLICABLE)
          .build();

  public static final AccountHierarchy ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000004_AND_PO00000003 =
      AccountHierarchy.builder()
          .txnHeaderId("500005")
          .parentPartyId("PO00000004")
          .childPartyId("PO00000003")
          .parentAccountId("80001")
          .childAccountId("70001")
          .status("ACTIVE")
          .validFrom(Date.valueOf("2021-01-21"))
          .validTo(Date.valueOf("2021-01-31"))
          .creationDate(CREATION_DT)
          .partitionId(PARTITION_ID_NOT_APPLICABLE)
          .build();

  public static final AccountHierarchy ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000007_AND_PO00000002 =
      AccountHierarchy.builder()
          .txnHeaderId("500027")
          .parentPartyId("PO00000007")
          .childPartyId("PO00000002")
          .parentAccountId("110001")
          .childAccountId("60001")
          .status("ACTIVE")
          .validFrom(Date.valueOf("2021-02-01"))
          .validTo(Date.valueOf("2021-02-28"))
          .creationDate(CREATION_DT)
          .partitionId(PARTITION_ID_NOT_APPLICABLE)
          .build();

  public static final AccountHierarchy ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000008 =
      AccountHierarchy.builder()
          .txnHeaderId("500023")
          .parentPartyId("PO00000003")
          .childPartyId("PO00000008")
          .parentAccountId("70001")
          .childAccountId("120001")
          .status("ACTIVE")
          .validFrom(Date.valueOf("2021-02-01"))
          .validTo(Date.valueOf("2021-02-28"))
          .creationDate(CREATION_DT)
          .partitionId(PARTITION_ID_NOT_APPLICABLE)
          .build();

  public static final AccountHierarchy ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000006_AND_PO00000003 =
      AccountHierarchy.builder()
          .txnHeaderId("500006")
          .parentPartyId("PO00000006")
          .childPartyId("PO00000003")
          .parentAccountId("100001")
          .childAccountId("70001")
          .status("ACTIVE")
          .validFrom(Date.valueOf("2021-01-25"))
          .validTo(Date.valueOf("2021-02-25"))
          .creationDate(CREATION_DT)
          .partitionId(PARTITION_ID_NOT_APPLICABLE)
          .build();

  public static final AccountHierarchy ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000002 =
      AccountHierarchy.builder()
          .txnHeaderId("500007")
          .parentPartyId("PO00000003")
          .childPartyId("PO00000002")
          .parentAccountId("70001")
          .childAccountId("60001")
          .status("ACTIVE")
          .validFrom(Date.valueOf("2021-02-01"))
          .validTo(Date.valueOf("2021-02-28"))
          .creationDate(CREATION_DT)
          .partitionId(PARTITION_ID_NOT_APPLICABLE)
          .build();

  public static final AccountHierarchy
      ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY =
          ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002
              .withTxnHeaderId("510001")
              .withValidFrom(Date.valueOf("2021-02-01"))
              .withValidTo(null);

  public static final AccountHierarchy
      ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003_AND_NEW_VALIDITY =
          ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003
              .withTxnHeaderId("510002")
              .withValidFrom(Date.valueOf("2021-03-01"))
              .withValidTo(null);

  public static final AccountHierarchy
      ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATED_AND_JAN_VALIDITY_EXPIRED =
          ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002
              .withTxnHeaderId("510006")
              .withValidFrom(Date.valueOf("2021-01-01"))
              .withValidTo(Date.valueOf("2021-01-31"));

  public static final AccountHierarchy
      ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATED_AND_FEB_VALIDITY_EXPIRED =
          ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002
              .withTxnHeaderId("510007")
              .withValidFrom(Date.valueOf("2021-02-01"))
              .withValidTo(Date.valueOf("2021-02-28"));

  public static final AccountHierarchy
      ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_JAN_CANCELLED =
          ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002
              .withTxnHeaderId("520001")
              .withStatus("INACTIVE");

  public static final AccountHierarchy
      ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_FEB_CANCELLED =
          ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATED_AND_FEB_VALIDITY_EXPIRED
              .withTxnHeaderId("520002")
              .withStatus("INACTIVE");

  public static final AccountHierarchy
      ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_JAN_CANCELLED_WITH_JAN_TO_FEB_RANGE =
          ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002
              .withTxnHeaderId("520003")
              .withValidTo(Date.valueOf("2021-02-28"))
              .withStatus("INACTIVE");

  public static final AccountHierarchy
      ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_FEB_CANCELLED_WITH_JAN_TO_FEB_RANGE =
          ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATED_AND_FEB_VALIDITY_EXPIRED
              .withTxnHeaderId("520003")
              .withStatus("INACTIVE");

  public static final ErrorAccountHierarchy
      ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000004 =
          ErrorAccountHierarchy.builder()
              .txnHeaderId("500003")
              .accountType("FUND")
              .childPartyId("PO00000004")
              .parentPartyId("PO00000002")
              .currency("GBP")
              .division("00001")
              .build();

  public static final ErrorAccountHierarchy
      ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000004_AND_PO00000003 =
          ErrorAccountHierarchy.builder()
              .txnHeaderId("500005")
              .accountType("FUND")
              .childPartyId("PO00000003")
              .parentPartyId("PO00000004")
              .currency("GBP")
              .division("00001")
              .build();

  public static final ErrorAccountHierarchy
      ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000006_AND_PO00000003 =
          ErrorAccountHierarchy.builder()
              .txnHeaderId("500006")
              .accountType("FUND")
              .childPartyId("PO00000003")
              .parentPartyId("PO00000006")
              .currency("GBP")
              .division("00001")
              .build();

  public static final ErrorAccountHierarchy
      ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000005 =
          ErrorAccountHierarchy.builder()
              .txnHeaderId("500004")
              .accountType("FUND")
              .childPartyId("PO00000005")
              .parentPartyId("PO00000003")
              .currency("GBP")
              .division("00001")
              .build();

  public static final ErrorAccountHierarchy
      ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003 =
          ErrorAccountHierarchy.builder()
              .txnHeaderId("500002")
              .accountType("FUND")
              .childPartyId("PO00000003")
              .parentPartyId("PO00000002")
              .currency("GBP")
              .division("00001")
              .build();

  public static final ErrorAccountHierarchy
      ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002 =
          ErrorAccountHierarchy.builder()
              .txnHeaderId("510003")
              .accountType("FUND")
              .childPartyId("PO00000002")
              .parentPartyId("PO00000001")
              .currency("GBP")
              .division("00001")
              .build();

  public static final ErrorAccountHierarchy
      ERROR_ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CROSS_OVER_VALIDITY =
          ErrorAccountHierarchy.builder()
              .txnHeaderId("510005")
              .accountType("FUND")
              .childPartyId("PO00000002")
              .parentPartyId("PO00000001")
              .currency("GBP")
              .division("00001")
              .build();
}
