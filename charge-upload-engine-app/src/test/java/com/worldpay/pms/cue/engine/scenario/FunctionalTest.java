package com.worldpay.pms.cue.engine.scenario;

import static com.worldpay.pms.cue.engine.integration.Constants.CISADM_OUTPUT_TABLES;
import static com.worldpay.pms.cue.engine.integration.Constants.OUTPUT_TABLES;
import static com.worldpay.pms.cue.engine.transformations.sources.TransactionSourceTest.timestamp;
import static com.worldpay.pms.cue.engine.utils.DatabaseCsvUtils.readFromCsvFileAndDeleteFromTable;
import static com.worldpay.pms.cue.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.SparkApp.run;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import com.worldpay.pms.cue.engine.InMemoryStaticDataRepository;
import com.worldpay.pms.cue.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import io.vavr.collection.List;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import lombok.Builder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sql2o.Query;
import org.sql2o.Sql2oQuery;
import org.sql2o.data.Row;
import org.sql2o.data.Table;

public class FunctionalTest implements WithDatabaseAndSpark {

  private SqlDb appuserDb;
  private static final SqlDb cisadmAcct =
      SqlDb.simple(
          ConfigBeanFactory.create(
              ConfigFactory.load("conf/db.conf").getConfig("cisadm"), JdbcConfiguration.class));
  private static final SqlDb cbemduownerAcct =
      SqlDb.simple(
          ConfigBeanFactory.create(
              ConfigFactory.load("conf/db.conf").getConfig("cbemduowner"),
              JdbcConfiguration.class));

  @Override
  public void bindChargingJdbcConfiguration(JdbcConfiguration conf) {
    this.appuserDb = SqlDb.simple(conf);
  }

  // create pendingbill row
  private final PendingBillBuilder recurringDailyAdhocCharge =
      pendingBill()
          .txnHeaderId("T1")
          .boStatusCd("UPLD")
          .party("PO4000050118")
          .legalCounterParty("00001")
          .subAccountType("RECR")
          .validFrom("02-JAN-00 00.00.00")
          .validTo("02-DEC-21 00.00.00")
          .billPeriodCode("WPDY")
          .product("ADHOCCHG")
          .sqiCode("NUMBER")
          .chargeTypeCode("RECURCHG")
          .serviceQty(1)
          .adhocSwitch("N")
          .chargeAmount(BigDecimal.valueOf(10.23))
          .canFlag("N")
          .recrIdfr("12345")
          .recrRate(BigDecimal.valueOf(50))
          .eventId(null)
          .sourceType(null)
          .sourceId(null);

  private final String VALID_FROM =
      LocalDate.of(2000, Month.JANUARY, 2).format(DateTimeFormatter.ofPattern("dd-MMM-yy"));
  private final String VALID_TO =
      LocalDate.of(2021, Month.DECEMBER, 2).format(DateTimeFormatter.ofPattern("dd-MMM-yy"));
  private final String NOW = LocalDate.now().format(DateTimeFormatter.ofPattern("dd-MMM-yy"));
  private final String NOWAddOne =
      LocalDate.now().plusDays(1).format(DateTimeFormatter.ofPattern("dd-MMM-yy"));
  private final String NOWAddTwo =
      LocalDate.now().plusDays(2).format(DateTimeFormatter.ofPattern("dd-MMM-yy"));
  private final String NOWAddThree =
      LocalDate.now().plusDays(3).format(DateTimeFormatter.ofPattern("dd-MMM-yy"));

  // invalid currency recr error
  private final RecrErrorExistsBuilder recrErrorBuilder =
      recrErrorExists()
          .product("ADHOCCHG")
          .lcp("PO1100000001")
          .partyId("PO4000050118")
          .frequencyId("WPDY")
          .currency("GBB")
          .price(BigDecimal.valueOf(50))
          .quantity(1)
          .validFrom(VALID_FROM)
          .validTo(VALID_TO)
          .status("ACTIVE")
          .sourceId("12345")
          .division("00001")
          .subAccount("CHRG")
          .txnHeaderId("T1")
          .cutOffDate(NOW)
          .retyCount(1);

  // billable charge item
  private final billItemExistsBuilder billItemExistsBuilder =
      billItemExists()
          .partyId("PO4000050118")
          .lcp("PO1100000001")
          .subAccount("CHRG")
          .currency("GBP")
          .status("ACTIVE")
          .product("ADHOCCHG")
          .productCategory("MISCELLANEOUS")
          .quantity(1)
          .sourceId("12345")
          .frequencyId("WPDY")
          .cutOffDate(NOW)
          .accountId("7338544032")
          .subAccount("CHRG")
          .subAccountId("7338544288")
          .txnHeaderId("T1")
          .amount(BigDecimal.valueOf(50));

  // recurring identifier
  private final RecrIdfrExistsBuilder recrIdfrExistsBuilder =
      recrIdfrExists().cutOffDate(NOW).sourceId("12345").frequencyId("WPDY");

  // error transaction
  private final ErrorTransactionExistsbuilder errorTransactionExistsbuilder =
      errorTransactionExists()
          .txnHeaderId("NONRECR2")
          .saTypeCode("CHBK")
          .retryCount(1)
          .partyId("PO4000123151");

  private InMemoryStaticDataRepository.InMemoryStaticDataRepositoryBuilder staticData;

  @BeforeEach
  void setup() {
    staticData = InMemoryStaticDataRepository.builder();
    setUpDays(LocalDate.now(), 10);
  }

  @Test
  public void checkTempTableIsPopulatedForValidRecrRecordAndUpdateRecrChgScnerio() {
    recurringDailyAdhocCharge.currency("GBP").txnHeaderId("recr-1").add();
    staticData
        .chargeTypes(Collections.emptySet())
        .currencyCodes(Sets.newHashSet("GBP", "EUR"))
        .priceItems(Sets.newHashSet("ADHOCCHG", "MIGCHBK"))
        .subAccountTypes(Sets.newHashSet("CHRG", "RECR", "CHBK"))
        .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO"));

    submit(LocalDate.now());

    assertThat(recrErrorBuilder.txnHeaderId("recr-1").check()).isEqualTo(0L);
    assertThat(
            billItemExistsBuilder
                .txnHeaderId("recr-1")
                .subAccount("CHRG")
                .subAccountId("7338544288")
                .check())
        .isEqualTo(1L);
    assertBillableChargeCount(1); // run for today so only today's billable charge created
    assertThat(recrIdfrExistsBuilder.sourceId("12345").check()).isEqualTo(1L);
    assertRecrSourceIdentifer(1); // today's max_cutt_off_dt entry

    recurringDailyAdhocCharge
        .currency("GBP")
        .txnHeaderId("recr-2")
        .recrRate(BigDecimal.valueOf(100)) // same recr chag amount updated
        .add();

    submit(LocalDate.now().plusDays(1));
    assertThat(recrErrorBuilder.txnHeaderId("recr-2").check()).isEqualTo(0L);
    assertThat(
            billItemExistsBuilder
                .txnHeaderId("recr-2")
                .subAccount("CHRG")
                .subAccountId("7338544288")
                .cutOffDate(NOWAddOne)
                .amount(BigDecimal.valueOf(100))
                .check())
        .isEqualTo(1L);
    assertBillableChargeCount(2); // run for two days so two charges created
    assertThat(recrIdfrExistsBuilder.sourceId("12345").cutOffDate(NOWAddOne).check()).isEqualTo(1L);
    assertRecrSourceIdentifer(2);
  }

  @Test
  public void WhenTwoInvalidNonRecrChargeWithAccountDetmAndInvalidCurrencyError() {

    // invalid account
    recurringDailyAdhocCharge
        .txnHeaderId("NONRECR1")
        .subAccountType("CHBK")
        .party("PO4000123151")
        .currency("GBP")
        .recrIdfr(null)
        .add();

    // invalid currency
    recurringDailyAdhocCharge
        .txnHeaderId("NONRECR2")
        .subAccountType("CHBK")
        .party("PO4000123151")
        .currency("GBB")
        .recrIdfr(null)
        .add();

    // destroy account data
    destroyData();
    staticData
        .chargeTypes(Collections.emptySet())
        .currencyCodes(Sets.newHashSet("GBP", "EUR"))
        .priceItems(Sets.newHashSet("ADHOCCHG", "MIGCHBK"))
        .subAccountTypes(Sets.newHashSet("CHRG", "RECR", "CHBK"))
        .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO"));

    submit(LocalDate.now());
    assertThat(errorTransactionExistsbuilder.txnHeaderId("NONRECR1").retryCount(1).check())
        .isEqualTo(1L);
    assertThat(errorTransactionExistsbuilder.txnHeaderId("NONRECR2").retryCount(999).check())
        .isEqualTo(1L);
    assertErrorCount(2); // total error count
    // check output table
    assertBillableChargeCount(0);

    refederateData();
    submit(LocalDate.now());
    // check output table for both non-recr
    assertThat(
            billItemExistsBuilder
                .txnHeaderId("NONRECR1")
                .subAccount("CHBK")
                .partyId("PO4000123151")
                .currency("GBP")
                .sourceId(null)
                .frequencyId(null)
                .cutOffDate(null)
                .accountId("9548554128")
                .subAccountId("9548554200")
                .amount(BigDecimal.valueOf(10.23))
                .check())
        .isEqualTo(1L);
    assertThat(
            billItemExistsBuilder
                .txnHeaderId("NONRECR2")
                .subAccount("CHBK")
                .partyId("PO4000123151")
                .sourceId(null)
                .frequencyId(null)
                .currency("GBB")
                .cutOffDate(null)
                .accountId("9548554128")
                .subAccountId("9548554200")
                .amount(BigDecimal.valueOf(10.23))
                .check())
        .isEqualTo(0L);
  }

  @Test
  public void whenInvalidRecrAndNonRecrRecordWithNullLcp() {
    recurringDailyAdhocCharge
        .currency("GBP")
        .txnHeaderId("RECR1")
        .legalCounterParty("null")
        .recrIdfr("12345")
        .add();
    recurringDailyAdhocCharge
        .txnHeaderId("NONRECR1")
        .subAccountType("CHBK")
        .party("PO4000123151")
        .legalCounterParty("null")
        .currency("GBP")
        .recrIdfr(null)
        .add();
    staticData
        .chargeTypes(Collections.emptySet())
        .currencyCodes(Sets.newHashSet("GBP", "EUR"))
        .priceItems(Sets.newHashSet("ADHOCCHG", "MIGCHBK"))
        .subAccountTypes(Sets.newHashSet("CHRG", "RECR", "CHBK"))
        .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO"));

    submit(LocalDate.now());

    assertRecrChargeCount(0); // only valid recr count
    assertErrorCount(1);
    assertRecrErrorCount(1);
    assertThat(errorTransactionExistsbuilder.txnHeaderId("NONRECR1").retryCount(999).check())
        .isEqualTo(1L);
    assertBillableChargeCount(0);
  }

  @Test
  public void checkTempTableIsPopulatedForValidorInvalidRecrAndNonRecrRecord() {
    // different source_id so they wont reduce into one
    recurringDailyAdhocCharge.currency("GBP").txnHeaderId("RECR1").recrIdfr("12345").add();
    recurringDailyAdhocCharge
        .currency(null)
        .billPeriodCode(null)
        .txnHeaderId("RECR2")
        .recrRate(null)
        .recrIdfr("12345")
        .add();
    recurringDailyAdhocCharge
        .txnHeaderId("NONRECR1")
        .subAccountType("CHBK")
        .party("PO4000123151")
        .currency("GBP")
        .recrIdfr(null)
        .add();
    recurringDailyAdhocCharge
        .txnHeaderId("NONRECR2")
        .subAccountType("CHBK")
        .party("PO4000123151")
        .currency(null)
        .billPeriodCode(null)
        .recrIdfr(null)
        .add();
    staticData
        .chargeTypes(Collections.emptySet())
        .currencyCodes(Sets.newHashSet("GBP", "EUR"))
        .priceItems(Sets.newHashSet("ADHOCCHG", "MIGCHBK"))
        .subAccountTypes(Sets.newHashSet("CHRG", "RECR", "CHBK"))
        .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO"));

    submit(LocalDate.now());

    assertRecrChargeCount(1); // only valid recr count
    assertThat(
            recrErrorBuilder
                .txnHeaderId("RECR2")
                .sourceId(null)
                .frequencyId(null)
                .currency(null)
                .retyCount(999)
                .status("INACTIVE")
                .cutOffDate(null)
                .price(null)
                .check())
        .isEqualTo(1L);
    assertRecrErrorCount(1);
    assertThat(errorTransactionExistsbuilder.txnHeaderId("NONRECR2").retryCount(999).check())
        .isEqualTo(1L);
    assertErrorCount(1); //  1 entry for invalid non-recr2
    assertThat(billItemExistsBuilder.txnHeaderId("RECR1").check()).isEqualTo(1L);
    assertThat(
            billItemExistsBuilder
                .txnHeaderId("NONRECR1")
                .subAccount("CHBK")
                .partyId("PO4000123151")
                .sourceId(null)
                .frequencyId(null)
                .cutOffDate(null)
                .accountId("9548554128")
                .subAccountId("9548554200")
                .amount(BigDecimal.valueOf(10.23))
                .check())
        .isEqualTo(1L);
    assertBillableChargeCount(2); // valid RECR1 for now , valid NON-RECR1
    assertThat(recrIdfrExistsBuilder.sourceId("12345").check()).isEqualTo(1L);
    // no entry for invalid recr record
    assertThat(recrIdfrExistsBuilder.sourceId("12346").check()).isEqualTo(0L);
    assertRecrSourceIdentifer(1);
  }

  @Test
  public void WhenInCorrectRecrChargeThenNoBillChargeCreatedAndGoesintoRecrError() {
    recurringDailyAdhocCharge.currency("GBB").add();
    staticData
        .chargeTypes(Collections.emptySet())
        .currencyCodes(Sets.newHashSet("GBP", "EUR"))
        .priceItems(Sets.newHashSet("ADHOCCHG", "MIGCHBK"))
        .subAccountTypes(Sets.newHashSet("CHRG", "RECR", "CHBK"))
        .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO"));

    submit(LocalDate.now().plusDays(1));
    assertRecrChargeCount(0); // no entry for invalid recr charge
    assertThat(recrErrorBuilder.retyCount(999).cutOffDate(null).status("INACTIVE").check())
        .isEqualTo(1L);
    assertRecrErrorCount(1); // One entry for invalid recr charge with null cutoff_dt
    assertBillableChargeCount(0);
    assertThat(billItemExistsBuilder.sourceId("12345").check()).isEqualTo(0);
    assertRecrSourceIdentifer(0); // no max_cuttof_dt entry for invalid recr record in temp table
    assertThat(recrIdfrExistsBuilder.sourceId("12345").cutOffDate(NOWAddOne).check()).isEqualTo(0L);
    assertThat(errorTransactionExistsbuilder.txnHeaderId("T1").check()).isEqualTo(0);
    assertErrorCount(0); // no error entry in non-recr error table
  }

  @Test
  public void
      WhenInCorrectRecrChargeAndBatchTriggerDailyUptoThreeDaysAndThenCorrectRecrChargeRecord() {
    recurringDailyAdhocCharge.currency("GBP").add();
    staticData
        .chargeTypes(Collections.emptySet())
        .currencyCodes(Sets.newHashSet("GBP", "EUR"))
        .priceItems(Sets.newHashSet("ADHOCCHG", "MIGCHBK"))
        .subAccountTypes(Sets.newHashSet("CHRG", "RECR", "CHBK"))
        .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO"));

    // before run  we destroy account set up data
    destroyData();
    LocalDate now = LocalDate.now();
    submit(now);
    assertThat(billItemExistsBuilder.txnHeaderId("T1").check()).isEqualTo(0L);
    assertBillableChargeCount(0);
    assertThat(recrErrorBuilder.currency("GBP").txnHeaderId("T1").cutOffDate(NOW).check())
        .isEqualTo(1L);
    assertRecrErrorCount(1);
    assertThat(recrIdfrExistsBuilder.sourceId("12345").frequencyId("WPDY").cutOffDate(NOW).check())
        .isEqualTo(1);
    // total count
    assertRecrSourceIdentifer(1);

    submit(now.plusDays(1));
    assertThat(billItemExistsBuilder.txnHeaderId("T1").check()).isEqualTo(0L);
    assertBillableChargeCount(0);

    assertThat(
            recrErrorBuilder.currency("GBP").txnHeaderId("T1").cutOffDate(NOW).retyCount(2).check())
        .isEqualTo(1L);
    assertThat(
            recrErrorBuilder
                .currency("GBP")
                .txnHeaderId("T1")
                .cutOffDate(NOWAddOne)
                .retyCount(1)
                .check())
        .isEqualTo(1L);

    assertThat(
            recrIdfrExistsBuilder
                .sourceId("12345")
                .frequencyId("WPDY")
                .cutOffDate(NOWAddOne)
                .check())
        .isEqualTo(1);

    // total count
    assertRecrSourceIdentifer(2);

    submit(now.plusDays(2));
    assertThat(billItemExistsBuilder.txnHeaderId("T1").check()).isEqualTo(0L);
    assertBillableChargeCount(0);

    assertThat(
            recrErrorBuilder.currency("GBP").txnHeaderId("T1").cutOffDate(NOW).retyCount(3).check())
        .isEqualTo(1L);
    assertThat(
            recrErrorBuilder
                .currency("GBP")
                .txnHeaderId("T1")
                .cutOffDate(NOWAddOne)
                .retyCount(2)
                .check())
        .isEqualTo(1L);
    assertThat(
            recrErrorBuilder
                .currency("GBP")
                .txnHeaderId("T1")
                .cutOffDate(NOWAddTwo)
                .retyCount(1)
                .check())
        .isEqualTo(1L);

    assertThat(
            recrIdfrExistsBuilder
                .sourceId("12345")
                .frequencyId("WPDY")
                .cutOffDate(NOWAddTwo)
                .check())
        .isEqualTo(1);

    assertRecrSourceIdentifer(3); // 3 entries of max_cut_off now, now+1, now+2

    // refederate account set up data
    refederateData();

    submit(now.plusDays(3));

    assertRecrChargeCount(1);

    assertThat(recrErrorBuilder.currency("GBP").txnHeaderId("T1").cutOffDate(NOW).check())
        .isEqualTo(0L);
    assertThat(recrErrorBuilder.currency("GBP").txnHeaderId("T1").cutOffDate(NOWAddOne).check())
        .isEqualTo(0L);
    assertThat(recrErrorBuilder.currency("GBP").txnHeaderId("T1").cutOffDate(NOWAddTwo).check())
        .isEqualTo(0L);

    assertThat(billItemExistsBuilder.txnHeaderId("T1").cutOffDate(NOW).check()).isEqualTo(1L);
    assertThat(billItemExistsBuilder.txnHeaderId("T1").cutOffDate(NOWAddOne).check()).isEqualTo(1L);
    assertThat(billItemExistsBuilder.txnHeaderId("T1").cutOffDate(NOWAddTwo).check()).isEqualTo(1L);
    assertThat(billItemExistsBuilder.txnHeaderId("T1").cutOffDate(NOWAddThree).check())
        .isEqualTo(1L);

    assertBillableChargeCount(
        4); // 4 entries created for each day billable charge now, now+1, now+2,  now+3

    assertThat(
            recrIdfrExistsBuilder
                .sourceId("12345")
                .frequencyId("WPDY")
                .cutOffDate(NOWAddThree)
                .check())
        .isEqualTo(1);

    // total count
    assertRecrSourceIdentifer(
        4); // 4 entries for each day ,max cuttoff_dt recr charge now, now+1, now+2,  now+3
  }

  @Test
  public void checkProductClassCoulmnPopulatedAndRecurringChargeChanges() {
    recurringDailyAdhocCharge.currency("GBP").txnHeaderId("recr-1").add();
    recurringDailyAdhocCharge
        .currency("GBP")
        .txnHeaderId("recr-2")
        .product("ABC") // invalid product
        .add();

    recurringDailyAdhocCharge
        .txnHeaderId("NONRECR1")
        .subAccountType("CHBK")
        .party("PO4000123151")
        .currency("GBP")
        .product("MIGCHRG")
        .eventId("123456")
        .recrIdfr(null)
        .add();
    recurringDailyAdhocCharge
        .txnHeaderId("NONRECR3")
        .subAccountType("CHBK")
        .party("PO4000123151")
        .currency("GBP")
        .product(" ")
        .recrIdfr(" ")
        .add();
    recurringDailyAdhocCharge
        .txnHeaderId("NONRECR4")
        .subAccountType("CHRG")
        .party("PO4000050118")
        .currency("GBP")
        .product("AAUTHFEE")
        .recrIdfr(" ")
        .eventId(null)
        .sourceType("ADJUSTMENT")
        .sourceId("SRC123456")
        .add();

    recurringDailyAdhocCharge
        .txnHeaderId("NONRECR2")
        .subAccountType("CHBK")
        .party("PO4000123151")
        .currency("GBP")
        .product("AFUND")
        .recrIdfr(null)
        .add();
    staticData
        .chargeTypes(Collections.emptySet())
        .currencyCodes(Sets.newHashSet("GBP", "EUR"))
        .priceItems(Sets.newHashSet("ADHOCCHG", "MIGCHRG", "AFUND", "AAUTHFEE"))
        .subAccountTypes(Sets.newHashSet("CHRG", "RECR", "CHBK"))
        .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO"));

    submit(LocalDate.now());
    assertThat(
            billItemExistsBuilder
                .txnHeaderId("recr-1")
                .product("ADHOCCHG")
                .productCategory("MISCELLANEOUS")
                .check())
        .isEqualTo(1L);
    assertThat(
            billItemExistsBuilder
                .txnHeaderId("NONRECR1")
                .subAccount("CHBK")
                .partyId("PO4000123151")
                .sourceId("123456")
                .frequencyId(null)
                .cutOffDate(null)
                .accountId("9548554128")
                .subAccountId("9548554200")
                .amount(BigDecimal.valueOf(10.23))
                .product("MIGCHRG")
                .productCategory("MIGRATION")
                .check())
        .isEqualTo(1L);

    assertThat(
            billItemExistsBuilder
                .txnHeaderId("NONRECR2")
                .subAccount("CHBK")
                .partyId("PO4000123151")
                .sourceId(null)
                .frequencyId(null)
                .cutOffDate(null)
                .accountId("9548554128")
                .subAccountId("9548554200")
                .amount(BigDecimal.valueOf(10.23))
                .product("AFUND")
                .productCategory("NONE")
                .check())
        .isEqualTo(1L);
    // assert non recurring charges on CHRG account
    assertThat(
            billItemExistsBuilder
                .txnHeaderId("NONRECR4")
                .subAccount("CHRG")
                .partyId("PO4000050118")
                .sourceId("SRC123456")
                .frequencyId(null)
                .cutOffDate(null)
                .accountId("7338544032")
                .subAccountId("7338544288")
                .amount(BigDecimal.valueOf(10.23))
                .product("AAUTHFEE")
                .check())
        .isEqualTo(1L);

    assertThat(
            errorTransactionExistsbuilder
                .txnHeaderId("NONRECR3")
                .partyId("PO4000123151")
                .retryCount(999)
                .saTypeCode("CHBK")
                .code("INVALID_PRODUCT_IDENTIFIER")
                .check())
        .isEqualTo(1L);
    // assert that CM_REC_CHG_ERR has sub acct CHRG for recrring charges
    // and invalid product goes to error
    assertThat(
            recrErrorBuilder
                .txnHeaderId("recr-2")
                .retyCount(999)
                .status("INACTIVE")
                .product("ABC")
                .cutOffDate(null)
                .currency("GBP")
                .subAccount("CHRG")
                .check())
        .isEqualTo(1L);
    assertSpecificColumnValue(1, "PRODUCT_ID", "ADHOCCHG");
    // to check RECR charges have sub acct CHRG in CM_REC_CHG
    assertSpecificColumnValue(0, "SUB_ACCT", "RECR");
    assertSpecificColumnValue(1, "SUB_ACCT", "CHRG");
    String where = "EVENT_ID IS NOT NULl AND SOURCE_TYPE = " + "'EVENT'";
    assertSpecificColumnValueForMiscBillItems(1, where);
    where = "EVENT_ID IS NULl AND SOURCE_TYPE = " + "'ADJUSTMENT'";
    assertSpecificColumnValueForMiscBillItems(2, where);

    assertBillableChargeCount(4);
  }

  private void destroyData() {
    readFromCsvFileAndDeleteFromTable(
        cbemduownerAcct,
        "input/ChargingEndToEndTest/account_determination/acct.csv",
        "acct",
        "acct_id");
    readFromCsvFileAndDeleteFromTable(
        cbemduownerAcct,
        "input/ChargingEndToEndTest/account_determination/sub_acct.csv",
        "sub_acct",
        "sub_acct_id");

    // refresh materialize view to effect
    cisadmAcct.execQuery(
        "refresh materialized view",
        "{CALL CBE_CUE_OWNER.PKG_CHARGING_ENGINE.prc_refresh_bcu_account()}",
        Sql2oQuery::executeUpdate);
  }

  private void refederateData() {
    readFromCsvFileAndWriteToExistingTable(
        cbemduownerAcct, "input/ChargingEndToEndTest/account_determination/acct.csv", "acct");
    readFromCsvFileAndWriteToExistingTable(
        cbemduownerAcct,
        "input/ChargingEndToEndTest/account_determination/sub_acct.csv",
        "sub_acct");

    // refresh materialize view to effect
    cisadmAcct.execQuery(
        "refresh materialized view",
        "{CALL CBE_CUE_OWNER.PKG_CHARGING_ENGINE.prc_refresh_bcu_account()}",
        Sql2oQuery::executeUpdate);
  }

  private void assertRecrErrorCount(int i) {
    List<Row> errors = readErrorsForRecurringCharge(null);
    assertThat(errors.size()).isEqualTo(i);
  }

  private void assertRecrSourceIdentifer(int i) {
    List<Row> errors = readRecurringSourceIdentifier(null);
    assertThat(errors.size()).isEqualTo(i);
  }

  private void assertErrorCount(int i) {
    List<Row> errors = readErrors(null);
    assertThat(errors.size()).isEqualTo(i);
  }

  private void assertBillableChargeCount(int i) {
    List<Row> billItems = readBillItems(null);
    assertThat(billItems.size()).isEqualTo(i);
  }

  private void assertRecrChargeCount(int i) {
    List<Row> recurringCharges = readRecurringCharges(null);
    assertThat(recurringCharges.size()).isEqualTo(i);
  }

  private void assertSpecificColumnValue(int i, String column, String value) {
    String where = column + "='" + value + "'";
    List<Row> recurringCharges = readRecurringCharges(where);
    assertThat(recurringCharges.size()).isEqualTo(i);
  }

  private void assertSpecificColumnValueForMiscBillItems(int i, String where) {
    List<Row> recurringCharges = readBillItems(where);
    assertThat(recurringCharges.size()).isEqualTo(i);
  }

  private void submit(LocalDate logicalDate) {
    submit(logicalDate.toString());
  }

  private void submit(String logicalDate) {
    run(
        cfg -> new ScenarioApp(cfg, staticData.build()),
        "submit",
        "--force",
        "--logical-date",
        logicalDate);
  }

  private void setUpDays(LocalDate val, int numberofDays) {
    for (int i = 0; i <= numberofDays; i++) {
      billCycleSchduler(val, i);
    }
  }

  private void billCycleSchduler(LocalDate val, int numberofDays) {

    cisadmAcct.exec(
        "insert-ci-bill-cyc-sch",
        conn ->
            conn.createQuery(
                    "Insert into CI_BILL_CYC_SCH (BILL_CYC_CD,WIN_START_DT,WIN_END_DT,ACCOUNTING_DT,EST_DT,"
                        + "FREEZE_COMPLETE_SW,VERSION) values ('WPDY', :startDate,:endDate, :acctDate, :estDate, 'N', 1) ")
                .addParameter("startDate", Date.valueOf(val.plusDays(numberofDays)))
                .addParameter("endDate", Date.valueOf(val.plusDays(numberofDays)))
                .addParameter("acctDate", Date.valueOf(val.plusDays(numberofDays)))
                .addParameter("estDate", Date.valueOf(val.plusDays(numberofDays)))
                .executeUpdate());
  }

  @Builder(
      builderMethodName = "pendingBill",
      buildMethodName = "add",
      builderClassName = "PendingBillBuilder")
  private void insertBchgSt(
      String txnHeaderId,
      String boStatusCd,
      String party,
      String legalCounterParty,
      String subAccountType,
      String validFrom,
      String validTo,
      String billPeriodCode,
      String product,
      String sqiCode,
      Integer serviceQty,
      String adhocSwitch,
      String currency,
      String canFlag,
      String recrIdfr,
      BigDecimal recrRate,
      String chargeTypeCode,
      BigDecimal chargeAmount,
      String eventId,
      String sourceType,
      String sourceId) {

    cisadmAcct.exec(
        "insert-bchg-event",
        conn ->
            conn.createQuery(
                    "insert into cm_bchg_stg(txn_header_id, bo_status_cd, per_id_nbr, cis_division, sa_type_cd"
                        + ", start_dt, end_dt, bill_period_cd, priceitem_cd, sqi_cd, svc_qty, adhoc_sw, currency_cd, can_flg"
                        + ",  recr_idfr, recr_rate, chg_type_cd, charge_amt, ilm_dt,event_id,source_type,source_id) "
                        + "values (:txn_header_id, :bo_status_cd, :per_id_nbr, :cis_division, :sa_type_cd"
                        + ", :start_dt, :end_dt, :bill_period_cd, :priceitem_cd, :sqi_cd, :svc_qty, :adhoc_sw, :currency_cd, :can_flg"
                        + ", :recr_idfr, :recr_rate, :chg_type_cd, :charge_amt, :ilm_dt,:event_id,:source_type,:source_id)")
                .addParameter("txn_header_id", txnHeaderId)
                .addParameter("bo_status_cd", boStatusCd)
                .addParameter("per_id_nbr", party)
                .addParameter("cis_division", legalCounterParty)
                .addParameter("sa_type_cd", subAccountType)
                .addParameter("start_dt", timestamp(validFrom))
                .addParameter("end_dt", timestamp(validTo))
                .addParameter("bill_period_cd", billPeriodCode)
                .addParameter("priceitem_cd", product)
                .addParameter("sqi_cd", sqiCode)
                .addParameter("svc_qty", serviceQty)
                .addParameter("adhoc_sw", adhocSwitch)
                .addParameter("currency_cd", currency)
                .addParameter("can_flg", canFlag)
                .addParameter("recr_idfr", recrIdfr)
                .addParameter("recr_rate", recrRate)
                .addParameter("chg_type_cd", chargeTypeCode)
                .addParameter("charge_amt", chargeAmount)
                .addParameter("ilm_dt", Timestamp.from(Instant.now()))
                .addParameter("event_id", eventId)
                .addParameter("source_type", sourceType)
                .addParameter("source_id", sourceId)
                .executeUpdate());
  }

  @Builder(
      builderMethodName = "errorTransactionExists",
      buildMethodName = "check",
      builderClassName = "ErrorTransactionExistsbuilder")
  public Long checkErrorTransactionExists(
      String txnHeaderId,
      String saTypeCode,
      int retryCount,
      String partyId,
      String code,
      String reason) {
    return appuserDb.execQuery(
        "check errorTransaction exists",
        resourceAsString("input/scenario/count_error_transaction_by_all_fields.sql"),
        query ->
            query
                .addParameter("txnHeaderId", txnHeaderId)
                .addParameter("saTypeCode", saTypeCode)
                .addParameter("retryCount", retryCount)
                .addParameter("partyId", partyId)
                .addParameter("code", code)
                .addParameter("reason", reason)
                .executeScalar(Long.TYPE));
  }

  @Builder(
      builderMethodName = "recrIdfrExists",
      buildMethodName = "check",
      builderClassName = "RecrIdfrExistsBuilder")
  public Long checkRecrIdfrExists(String cutOffDate, String sourceId, String frequencyId) {
    return appuserDb.execQuery(
        "check recrIdfrExists",
        resourceAsString("input/scenario/count_cm_rec_idfr_by_all_fields.sql"),
        query ->
            query
                .addParameter("cutOffDate", cutOffDate)
                .addParameter("sourceId", sourceId)
                .addParameter("frequencyId", frequencyId)
                .executeScalar(Long.TYPE));
  }

  @Builder(
      builderMethodName = "recrErrorExists",
      buildMethodName = "check",
      builderClassName = "RecrErrorExistsBuilder")
  public Long checkRecrErrorExists(
      String product,
      String lcp,
      String partyId,
      String frequencyId,
      String currency,
      BigDecimal price,
      Integer quantity,
      String validFrom,
      String validTo,
      String status,
      String sourceId,
      String division,
      String subAccount,
      String txnHeaderId,
      String cutOffDate,
      String code,
      String reason,
      int retyCount) {

    return appuserDb.execQuery(
        "check recrError exists",
        resourceAsString("input/scenario/count_rec_error_by_all_fields.sql"),
        query ->
            query
                .addParameter("product", product)
                .addParameter("lcp", lcp)
                .addParameter("partyId", partyId)
                .addParameter("frequencyId", frequencyId)
                .addParameter("currency", currency)
                .addParameter("price", price)
                .addParameter("quantity", quantity)
                .addParameter("validFrom", validFrom)
                .addParameter("validTo", validTo)
                .addParameter("status", status)
                .addParameter("sourceId", sourceId)
                .addParameter("division", division)
                .addParameter("subAccount", subAccount)
                .addParameter("txnHeaderId", txnHeaderId)
                .addParameter("cutOffDate", cutOffDate)
                .addParameter("code", code)
                .addParameter("reason", reason)
                .addParameter("retyCount", retyCount)
                .executeScalar(Long.TYPE));
  }

  @Builder(
      builderMethodName = "billItemExists",
      buildMethodName = "check",
      builderClassName = "billItemExistsBuilder")
  public Long checkbillItemExists(
      String partyId,
      String lcp,
      String subAccount,
      String currency,
      String status,
      String product,
      String productCategory,
      Integer quantity,
      String sourceId,
      String frequencyId,
      String cutOffDate,
      String accountId,
      String subAccountId,
      String txnHeaderId,
      BigDecimal amount) {

    return appuserDb.execQuery(
        "check billItem exists",
        resourceAsString("input/scenario/count_bill_item_by_all_fields.sql"),
        query ->
            query
                .addParameter("partyId", partyId)
                .addParameter("lcp", lcp)
                .addParameter("subAccount", subAccount)
                .addParameter("currency", currency)
                .addParameter("status", status)
                .addParameter("product", product)
                .addParameter("productCategory", productCategory)
                .addParameter("quantity", quantity)
                .addParameter("sourceId", sourceId)
                .addParameter("frequencyId", frequencyId)
                .addParameter("cutoffdate", cutOffDate)
                .addParameter("accountId", accountId)
                .addParameter("subAccountId", subAccountId)
                .addParameter("txnHeaderId", txnHeaderId)
                .addParameter("amount", amount)
                .executeScalar(Long.TYPE));
  }

  protected List<Row> readErrors(String where) {
    return List.ofAll(readTable("error_transaction", where).rows());
  }

  protected List<Row> readErrorsForRecurringCharge(String where) {
    return List.ofAll(readTable("cm_rec_chg_err", where).rows());
  }

  protected List<Row> readRecurringSourceIdentifier(String where) {
    return List.ofAll(readTable("cm_rec_idfr", where).rows());
  }

  protected List<Row> readBillItems(String where) {
    return List.ofAll(readTable("cm_misc_bill_item", where).rows());
  }

  protected List<Row> readRecurringCharges(String where) {
    return List.ofAll(readTable("cm_rec_chg", where).rows());
  }

  protected Table readTable(String tableName, String where) {
    return appuserDb.execQuery(
        tableName,
        format("SELECT * FROM %s WHERE %s", tableName, where == null ? "1=1" : where),
        Sql2oQuery::executeAndFetchTable);
  }

  @BeforeAll
  static void setUpAccounts() {

    readFromCsvFileAndDeleteFromTable(
        cbemduownerAcct,
        "input/ChargingEndToEndTest/account_determination/acct.csv",
        "acct",
        "acct_id");
    readFromCsvFileAndDeleteFromTable(
        cbemduownerAcct,
        "input/ChargingEndToEndTest/account_determination/sub_acct.csv",
        "sub_acct",
        "sub_acct_id");
    readFromCsvFileAndDeleteFromTable(
        cbemduownerAcct,
        "input/ChargingEndToEndTest/account_determination/outputs_registry.csv",
        "outputs_registry",
        "batch_code");

    readFromCsvFileAndDeleteFromTable(
        cisadmAcct,
        "input/ChargingEndToEndTest/account_determination/ci_priceitem_char.csv",
        "ci_priceitem_char",
        "priceitem_cd");

    readFromCsvFileAndWriteToExistingTable(
        cbemduownerAcct, "input/ChargingEndToEndTest/account_determination/acct.csv", "acct");
    readFromCsvFileAndWriteToExistingTable(
        cbemduownerAcct,
        "input/ChargingEndToEndTest/account_determination/sub_acct.csv",
        "sub_acct");
    readFromCsvFileAndWriteToExistingTable(
        cbemduownerAcct,
        "input/ChargingEndToEndTest/account_determination/outputs_registry.csv",
        "outputs_registry");

    readFromCsvFileAndWriteToExistingTable(
        cisadmAcct,
        "input/ChargingEndToEndTest/account_determination/ci_priceitem_char.csv",
        "ci_priceitem_char");

    cisadmAcct.execQuery(
        "refresh materialized view",
        "{CALL CBE_CUE_OWNER.PKG_CHARGING_ENGINE.prc_refresh_bcu_account()}",
        Sql2oQuery::executeUpdate);
  }

  @AfterEach
  void cleanUp() {
    appuserDb.exec(
        "clear tables - user",
        conn -> {
          OUTPUT_TABLES.forEach(
              out -> {
                try (Query q = conn.createQuery(format("DELETE %s", out))) {
                  q.executeUpdate();
                }
              });
          return true;
        });

    cisadmAcct.exec(
        "clear tables - cisadm",
        conn -> {
          CISADM_OUTPUT_TABLES.forEach(
              out -> {
                try (Query q = conn.createQuery(format("DELETE %s", out))) {
                  q.executeUpdate();
                }
              });
          return true;
        });

    cisadmAcct.execQuery("clear input table", "delete from  cm_bchg_stg", Query::executeUpdate);
    cisadmAcct.execQuery("clear input table", "delete from  ci_bill_cyc_sch", Query::executeUpdate);
  }

  @BeforeEach
  void clean() {
    appuserDb.exec(
        "clear tables - user",
        conn -> {
          OUTPUT_TABLES.forEach(
              out -> {
                try (Query q = conn.createQuery(format("DELETE %s", out))) {
                  q.executeUpdate();
                }
              });
          return true;
        });
    cisadmAcct.exec(
        "clear tables - cisadm",
        conn -> {
          CISADM_OUTPUT_TABLES.forEach(
              out -> {
                try (Query q = conn.createQuery(format("DELETE %s", out))) {
                  q.executeUpdate();
                }
              });
          return true;
        });
    cisadmAcct.execQuery("clear input table", "delete from  cm_bchg_stg", Query::executeUpdate);
    cisadmAcct.execQuery("clear input table", "delete from  ci_bill_cyc_sch", Query::executeUpdate);
  }
}
