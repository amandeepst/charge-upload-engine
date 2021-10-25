package com.worldpay.pms.cue.engine.integration;

import static com.worldpay.pms.cue.engine.integration.Constants.CISADM_OUTPUT_TABLES;
import static com.worldpay.pms.cue.engine.integration.Constants.OUTPUT_TABLES;
import static com.worldpay.pms.cue.engine.integration.IntegrationScenarios.FAIL_WITH_DOMAIN_ERROR;
import static com.worldpay.pms.cue.engine.integration.IntegrationScenarios.NO_ACCOUNT;
import static com.worldpay.pms.cue.engine.integration.IntegrationScenarios.RECR_CHRG;
import static com.worldpay.pms.cue.engine.jdbc.SourceTestUtil.insertBatchHistoryAndOnBatchCompleted;
import static com.worldpay.pms.cue.engine.utils.DatabaseCsvUtils.readFromCsvFileAndDeleteFromTable;
import static com.worldpay.pms.cue.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import com.worldpay.pms.cue.domain.ChargingService.Charge;
import com.worldpay.pms.cue.engine.InMemoryStaticDataRepository;
import com.worldpay.pms.cue.engine.batch.ChargingBatchRunResult;
import com.worldpay.pms.cue.engine.integration.MockedTransactionChargingServiceFactory.MockTransactionChargingServiceWithValidation;
import com.worldpay.pms.cue.engine.integration.MockedTransactionChargingServiceFactory.ValidTransactionChargingService;
import com.worldpay.pms.spark.core.Resource;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.stereotypes.WithSpark;
import io.vavr.collection.List;
import java.math.BigDecimal;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.Alphanumeric;
import org.sql2o.Query;
import org.sql2o.Sql2oQuery;
import org.sql2o.data.Row;

/** For this tests, the input will be from db rather than h2 */
@Slf4j
@WithSpark
@TestMethodOrder(Alphanumeric.class)
class MockedDomainChargingEndToEndTest extends EndToEndTestBase {

  private static final SqlDb cisadmAcct =
      SqlDb.simple(
          ConfigBeanFactory.create(
              ConfigFactory.load("conf/db.conf").getConfig("cisadm"), JdbcConfiguration.class));
  private static final SqlDb mduOwnerAcct =
      SqlDb.simple(
          ConfigBeanFactory.create(
              ConfigFactory.load("conf/db.conf").getConfig("cbemduowner"),
              JdbcConfiguration.class));

  private InMemoryStaticDataRepository.InMemoryStaticDataRepositoryBuilder staticData;

  @BeforeAll
  static void setUpAccounts() {

    readFromCsvFileAndDeleteFromTable(
        mduOwnerAcct,
        "input/ChargingEndToEndTest/account_determination/acct.csv",
        "acct",
        "acct_id");
    readFromCsvFileAndDeleteFromTable(
        mduOwnerAcct,
        "input/ChargingEndToEndTest/account_determination/sub_acct.csv",
        "sub_acct",
        "sub_acct_id");
    readFromCsvFileAndDeleteFromTable(
        mduOwnerAcct,
        "input/ChargingEndToEndTest/account_determination/outputs_registry.csv",
        "outputs_registry",
        "batch_code");

    readFromCsvFileAndWriteToExistingTable(
        mduOwnerAcct, "input/ChargingEndToEndTest/account_determination/acct.csv", "acct");
    readFromCsvFileAndWriteToExistingTable(
        mduOwnerAcct, "input/ChargingEndToEndTest/account_determination/sub_acct.csv", "sub_acct");
    readFromCsvFileAndWriteToExistingTable(
        mduOwnerAcct,
        "input/ChargingEndToEndTest/account_determination/outputs_registry.csv",
        "outputs_registry");

    cisadmAcct.execQuery(
        "refresh materialized view",
        "{CALL CBE_CUE_OWNER.PKG_CHARGING_ENGINE.prc_refresh_bcu_account()}",
        Sql2oQuery::executeUpdate);
  }

  @Test
  void testGettingInputsFromInputViewAndErrorTransactionView() {
    // insert valid event in input table
    cisadmDb.execQuery(
        "insert_bch_event",
        Resource.resourceAsString(
            "input/ChargingEndToEndTest/first_failure_at_testing_scenario/insert_bch_event.sql"),
        Sql2oQuery::executeUpdate);

    // submit application so that it fails the transaction once
    run(
        FAIL_WITH_DOMAIN_ERROR,
        "submit",
        "--force",
        "--set-high-watermark-to",
        "2020-06-01 12:00:00",
        "--logical-date",
        "2020-06-01");
    List<Row> errors = readErrorView(null);
    assertThat(errors.size()).isEqualTo(1);
    assertThat(errors.head().getInteger("retry_count")).isEqualTo(1);
    assertThat(errors.head().getString("first_failure_at")).isNotNull();
    String firstFailureOn = errors.head().getString("first_failure_at");
    ChargingBatchRunResult run1 = readLastSuccessfulBatch();
    assertThat(run1.getErrorTransactionsCount()).isEqualTo(1);
    assertThat(run1.getNonRecurringChargeFailureCount()).isEqualTo(1);

    // fail a second time, first failure is same date as the original failure
    run(FAIL_WITH_DOMAIN_ERROR, "submit", "--logical-date", "2020-06-01");
    errors = readErrorView("txn_header_id = 00000000003346 AND retry_count = 2");
    assertThat(errors.size()).isEqualTo(1);
    assertThat(errors.head().getString("first_failure_at")).isEqualTo(firstFailureOn);
    ChargingBatchRunResult run2 = readLastSuccessfulBatch();
    assertThat(run2.getErrorTransactionsCount()).isEqualTo(1);
    assertThat(run2.getNonRecurringChargeFailureCount()).isEqualTo(1);

    // mock the domain in order to generate valid charges for the input event
    // now we expect that the transaction is not visible anymore
    run(
        new ValidTransactionChargingService(new Charge("PI_MBA", new BigDecimal("50"))),
        "submit",
        "--force",
        "--logical-date",
        "2020-06-01");
    errors = readErrorView("txn_header_id = 00000000003346 AND retry_count = 3");
    assertThat(errors.isEmpty()).isTrue();
    ChargingBatchRunResult run3 = readLastSuccessfulBatch();
    assertThat(run3.getErrorTransactionsCount()).isEqualTo(0);
    assertThat(run3.getNonRecurringChargeFailureCount()).isEqualTo(0);
  }

  @Test
  void testValidationService() {

    staticData = InMemoryStaticDataRepository.builder();
    staticData
        .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO"))
        .currencyCodes(Sets.newHashSet("GBP", "USD"))
        .priceItems(Sets.newHashSet("MREC0001", "MREC0002", "MREC0003", "MREC0004", "MREC0005"))
        .subAccountTypes(Sets.newHashSet("RECR", "CHRG", "CHBK"))
        .build();

    readFromCsvFileAndWriteToExistingTable(
        cisadmDb,
        "input/ChargingEndToEndTest/validation_scenario/input_events_missing_fields.csv",
        "cm_bchg_stg");
    run(
        new MockTransactionChargingServiceWithValidation(
            new Charge("PI_RECUR", new BigDecimal("10")),
            staticData
                .billPeriodCodes(Sets.newHashSet("WPDY", "WPMO"))
                .currencyCodes(Sets.newHashSet("GBP", "USD"))
                .priceItems(
                    Sets.newHashSet("MREC0001", "MREC0002", "MREC0003", "MREC0004", "MREC0005"))
                .subAccountTypes(Sets.newHashSet("RECR", "CHRG", "CHBK"))
                .build()),
        "submit",
        "--set-high-watermark-to",
        "2020-08-25 14:30:00",
        "--logical-date",
        "2020-08-25");
    List<Row> errorRows = readErrors("retry_count=999 and retry_count<>1");
    List<String> validationErrors =
        List.of(
            "invalid productIdentifier 'XYZ'",
            "invalid subAccountType 'XYZ'",
            "invalid currency 'XYZ'",
            "Unexpected null field `PRICE`");
    assertThat(errorRows.size()).isEqualTo(5);

    errorRows
        .filter(row -> row.getString("TXN_HEADER_ID").equals("00000000005920"))
        .forEach(
            row ->
                assertThat(row.getString("REASON")).isEqualTo(String.join("\n", validationErrors)));
  }

  @Test
  void whenMulitpleRecurringRecordsOfSameSourceIdPresent() {
    String whereSubAccountChrg = "SUB_ACCT='CHRG'";
    String whereSubAccountRecr = "SUB_ACCT='RECR'";
    // insert a completed batch
    insertBatchHistoryAndOnBatchCompleted(
        cue,
        "test-run",
        "COMPLETED",
        Timestamp.valueOf("2020-09-24 00:00:00"),
        Timestamp.valueOf("2020-09-24 00:01:00"),
        Timestamp.valueOf("2020-09-24 00:01:00"));

    // insert some valid event's in input table
    readFromCsvFileAndWriteToExistingTable(
        cisadmDb,
        "input/ChargingEndToEndTest/reduce_recurring/input_cm_bchg_stg.csv",
        "cm_bchg_stg");

    // insert into ci_bill_cyc_sch
    readFromCsvFileAndWriteToExistingTable(
        cisadmDb,
        "input/ChargingEndToEndTest/input_scenario_1/input_ci_bill_cyc_sch.csv",
        "ci_bill_cyc_sch");

    // insert an already existing WPDY entry in cm_rec_chg for updation scenario
    readFromCsvFileAndWriteToExistingTable(
        cue, "input/ChargingEndToEndTest/reduce_recurring/input_cm_rec_chg.csv", "cm_rec_chg");

    // delete from ci_priceitem_char
    readFromCsvFileAndDeleteFromTable(
        cisadmDb,
        "input/ChargingEndToEndTest/replay_scenario/ci_priceitem_char.csv",
        "ci_priceitem_char",
        "priceitem_cd");

    // insert into ci_priceitem_char
    readFromCsvFileAndWriteToExistingTable(
        cisadmDb,
        "input/ChargingEndToEndTest/reduce_recurring/ci_priceitem_char.csv",
        "ci_priceitem_char");

    // running job
    run(
        RECR_CHRG,
        "submit",
        "--set-high-watermark-to",
        "2020-09-30 14:30:00",
        "--logical-date",
        "2020-09-30");

    // output structure assertion
    List<Row> errors = readErrorView(null);
    assertThat(errors.size()).isEqualTo(0);
    List<Row> recurringChargesForAudit = readRecurringChargesForAudit(null);
    assertThat(recurringChargesForAudit.size()).isEqualTo(8);
    List<Row> recurringChargesForAuditHavingSubAccountChrg =
        readRecurringChargesForAudit(whereSubAccountChrg);
    assertThat(recurringChargesForAuditHavingSubAccountChrg.size()).isEqualTo(8);
    List<Row> recurringChargesForAuditHavingSubAccountRECR =
        readRecurringChargesForAudit(whereSubAccountRecr);
    assertThat(recurringChargesForAuditHavingSubAccountRECR.size()).isEqualTo(0);

    List<Row> recurringCharges = readRecurringCharges(null);
    assertThat(recurringCharges.size()).isEqualTo(2);
    List<Row> recurringChargesWithSubAccountChrg = readRecurringCharges(whereSubAccountChrg);
    assertThat(recurringChargesWithSubAccountChrg.size()).isEqualTo(2);
    List<Row> recurringChargesWithSubAccountRecr = readRecurringCharges(whereSubAccountRecr);
    assertThat(recurringChargesWithSubAccountRecr.size()).isEqualTo(0);

    // check that only max txn_header_id present in cm_rec_chg
    List<String> maxTxnId = List.of("00000000005918", "00000000008918");
    recurringCharges.forEach(row -> assertThat(row.getString("TXN_HEADER_ID")).isIn(maxTxnId));

    List<Row> billItems = readBillItems(null);
    assertThat(billItems.size()).isEqualTo(6);
    List<Row> billItemLines = readBillItemLines(null);
    assertThat(billItemLines.size()).isEqualTo(6);
    List<Row> cisadmBillableCharge = readBillableChargeFromCISADM(null);
    assertThat(cisadmBillableCharge.size()).isEqualTo(6);
    List<Row> checkcisadmBillableChargeSubaccount =
        readBillableChargeFromCISADM(" SA_ID='7338544288'");
    assertThat(checkcisadmBillableChargeSubaccount.size()).isEqualTo(6);

    List<Row> cisadmBillableChargeLines = readBillableChargeLinesFromCISADM(null);
    assertThat(cisadmBillableChargeLines.size()).isEqualTo(6);
    // no entry published in cm_bchg_attributes_map for recurring
    List<Row> cisadmBillableChargeAttibutes = readBillableChargeAttibutesFromCISADM(null);
    assertThat(cisadmBillableChargeAttibutes.size()).isEqualTo(0);
    List<Row> cisadmBillableChargeChar = readBillableChargeCharFromCISADM(null);
    assertThat(cisadmBillableChargeChar.size()).isEqualTo(6);
    List<Row> cisadmBillableChargeLineChar = readBillableChargeLineCharFromCISADM(null);
    assertThat(cisadmBillableChargeLineChar.size()).isEqualTo(24);
  }

  @Test
  void whenNoInputsThenNoResults() {
    run(NO_ACCOUNT, "submit", "--logical-date", "2020-09-30");
    assertThat(readErrorView(null)).isEmpty();
    assertThat(readBillItemLines(null)).isEmpty();
    assertThat(readBillItems(null)).isEmpty();
    assertThat(readRecurringCharges(null)).isEmpty();
    assertThat(readRecurringChargesForAudit(null)).isEmpty();
    assertThat(readBillableChargeFromCISADM(null)).isEmpty();
    assertThat(readBillableChargeLinesFromCISADM(null)).isEmpty();
    assertThat(readBillableChargeLinesFromCISADM(null)).isEmpty();
    assertThat(readBillableChargeLinesFromCISADM(null)).isEmpty();
    assertThat(readBillableChargeLinesFromCISADM(null)).isEmpty();
  }

  @AfterEach
  void cleanUp() {
    cue.exec(
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
    cisadmDb.exec(
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
    cisadmDb.execQuery("clear input table", "delete from  cm_bchg_stg", Query::executeUpdate);
    cisadmDb.execQuery("clear input table", "delete from  ci_bill_cyc_sch", Query::executeUpdate);
  }
}
