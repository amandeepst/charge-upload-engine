package com.worldpay.pms.mdu.engine.transformations.writers;

import static com.worldpay.pms.mdu.engine.encoder.Encoders.PARTY_ENCODER;
import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.mdu.domain.model.output.Account;
import com.worldpay.pms.mdu.domain.model.output.Party;
import com.worldpay.pms.mdu.domain.model.output.SubAccount;
import com.worldpay.pms.mdu.domain.model.output.WithholdFund;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import io.vavr.collection.Array;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Encoder;

public class PartyWriterTest extends MerchantUploadJdbcWritersTest<Party> {

  private static final Account[] EMPTY_ACCOUNT_ARRAY = new Account[0];
  private static final SubAccount[] EMPTY_SUB_ACCOUNT_ARRAY = new SubAccount[0];
  private static final WithholdFund WITHHOLD_FUND_1 = createWithholdFunds();
  private static final WithholdFund WITHHOLD_FUND_2 = createAnotherWithholdFunds();
  private static final SubAccount[] SUB_ACCOUNTS = createSubAccounts();
  private static final Account[] ACCOUNTS = createAccounts();
  private static final Party PARTY_1 = createParty_1();
  private static final Party PARTY_2 = createParty_2();

  private static final String ACCOUNT_ID_1 = "acct_1";
  private static final String ACCOUNT_ID_2 = "acct_2";
  private static final String ACCOUNT_ID_3 = "acct_3";
  private static final String PARTY_ID_1 = "P047877901";
  private static final String PARTY_ID_2 = "P047877902";

  private static final String QUERY_DELETE_WITHHOLD_FUND_BY_ACCT_ID =
      resourceAsString("sql/party_writer/delete_withholdfund_by_acct_id.sql");
  private static final String QUERY_DELETE_SUB_ACCOUNTS_BY_ACCT_ID =
      resourceAsString("sql/party_writer/delete_subaccounts_by_acct_id.sql");
  private static final String QUERY_DELETE_ACCOUNTS_BY_PARTY_ID =
      resourceAsString("sql/party_writer/delete_accounts_by_party_id.sql");
  private static final String QUERY_DELETE_PARTY_BY_PARTY_ID =
      resourceAsString("sql/party_writer/delete_party_by_party_id.sql");

  private static final String QUERY_COUNT_WITHHOLD_FUND_BY_ACCOUNT_ID =
      resourceAsString("sql/party_writer/count_withholdfunds_by_acct_id.sql");
  private static final String QUERY_COUNT_SUB_ACCOUNTS_BY_ACCOUNT_ID =
      resourceAsString("sql/party_writer/count_subaccounts_by_acct_id.sql");
  private static final String QUERY_COUNT_ACCOUNTS_BY_PARTY_ID =
      resourceAsString("sql/party_writer/count_accounts_by_party_id.sql");
  private static final String QUERY_COUNT_PARTY_BY_ALL_FIELDS =
      resourceAsString("sql/party_writer/count_party_by_all_fields.sql");

  @Override
  protected void init() {
    db.execQuery(
        "delete-withholdfund-by-acctid",
        QUERY_DELETE_WITHHOLD_FUND_BY_ACCT_ID,
        (query) -> query.withParams(ACCOUNT_ID_1, ACCOUNT_ID_3).executeUpdate());

    db.execQuery(
        "delete-sub-accounts-by-acctid",
        QUERY_DELETE_SUB_ACCOUNTS_BY_ACCT_ID,
        (query) -> query.withParams(ACCOUNT_ID_1, ACCOUNT_ID_3).executeUpdate());

    db.execQuery(
        "delete_party_by_party_id",
        QUERY_DELETE_PARTY_BY_PARTY_ID,
        (query) -> query.withParams(PARTY_ID_1, PARTY_ID_2).executeUpdate());

    db.execQuery(
        "delete_accounts_by_party_id",
        QUERY_DELETE_ACCOUNTS_BY_PARTY_ID,
        (query) -> query.withParams(PARTY_ID_1).executeUpdate());
  }

  @Override
  protected void assertRowsWritten() {

    // assert party created or not
    long businessPartyCount = getParty(PARTY_1);
    assertThat(businessPartyCount, is(1L));

    // assert party created or not
    long groupedPartyCount = getParty(PARTY_2);
    assertThat(groupedPartyCount, is(1L));

    // assert accounts created or not on party
    long businessPartyAccountCount = getAccounts(PARTY_ID_1);
    assertThat(businessPartyAccountCount, is(3L));

    // assert accounts created or not on party
    long groupedPartyAccountCount = getAccounts(PARTY_ID_2);
    assertThat(groupedPartyAccountCount, is(0L));

    // ACCOUNT 1  validation
    long subaccount_1 = getSubAccounts(ACCOUNT_ID_1);
    assertThat(subaccount_1, is(3L));
    long withholdfund_1 = getWithHoldFund(ACCOUNT_ID_1);
    assertThat(withholdfund_1, is(1L));

    // ACCOUNT 2  validation
    long subaccount_2 = getSubAccounts(ACCOUNT_ID_2);
    assertThat(subaccount_2, is(0L));
    long withholdfund_2 = getWithHoldFund(ACCOUNT_ID_2);
    assertThat(withholdfund_2, is(0L));

    // ACCOUNT 3  validation
    long subaccount_3 = getSubAccounts(ACCOUNT_ID_3);
    assertThat(subaccount_3, is(3L));
    long withholdfund_3 = getWithHoldFund(ACCOUNT_ID_3);
    assertThat(withholdfund_3, is(1L));
  }

  @Override
  protected void assertNoRowsWritten() {
    long count =
        db.execQuery(
            "count-party-by-id",
            "select count(*) from party where party_id=:party",
            (query) -> query.addParameter("party", PARTY_ID_1).executeScalar(Long.TYPE));
    assertThat(count, is(0L));
  }

  @Override
  protected List<Party> provideSamples() {
    return Arrays.asList(PARTY_1, PARTY_2);
  }

  @Override
  protected Encoder<Party> encoder() {
    return PARTY_ENCODER;
  }

  @Override
  protected JdbcWriter<Party> createWriter(JdbcWriterConfiguration conf) {
    return new PartyWriter(conf, spark, Collections.emptyMap());
  }

  private long getSubAccounts(String accountId) {
    return db.execQuery(
        "count_subaccounts_by_acct_id",
        QUERY_COUNT_SUB_ACCOUNTS_BY_ACCOUNT_ID,
        (query) ->
            query
                .addParameter("accountId", accountId)
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .addParameter("ilmDateTime", Timestamp.valueOf(STARTED_AT))
                .addParameter("ilmArchiveSwitch", "Y")
                .addParameter("createdAt", Timestamp.valueOf(STARTED_AT))
                .executeScalar(Long.TYPE));
  }

  private long getWithHoldFund(String accountId) {
    return db.execQuery(
        "count_withholdfunds_by_all_fields",
        QUERY_COUNT_WITHHOLD_FUND_BY_ACCOUNT_ID,
        (query) ->
            query
                .addParameter("accountId", accountId)
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .addParameter("ilmDateTime", Timestamp.valueOf(STARTED_AT))
                .addParameter("ilmArchiveSwitch", "Y")
                .addParameter("createdAt", Timestamp.valueOf(STARTED_AT))
                .executeScalar(Long.TYPE));
  }

  private long getAccounts(String partyId) {

    return db.execQuery(
        "count_accounts_by_party_id",
        QUERY_COUNT_ACCOUNTS_BY_PARTY_ID,
        (query) ->
            query
                .addParameter("partyId", partyId)
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .addParameter("ilmDateTime", Timestamp.valueOf(STARTED_AT))
                .addParameter("ilmArchiveSwitch", "Y")
                .addParameter("createdAt", Timestamp.valueOf(STARTED_AT))
                .executeScalar(Long.TYPE));
  }

  private long getParty(Party party) {
    return db.execQuery(
        "count_party_by_all_fields",
        QUERY_COUNT_PARTY_BY_ALL_FIELDS,
        (query) ->
            query
                .addParameter("partyId", party.getPartyId())
                .addParameter("countryCode", party.getCountryCode())
                .addParameter("state", party.getState())
                .addParameter("businessUnit", party.getBusinessUnit())
                .addParameter("merchantTaxRegistration", party.getTaxRegistrationNumber())
                .addParameter("txnHeaderId", party.getTxnHeaderId())
                .addParameter("validFrom", party.getValidFrom())
                .addParameter("validTo", party.getValidTo())
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .addParameter("ilmDateTime", Timestamp.valueOf(STARTED_AT))
                .addParameter("ilmArchiveSwitch", "Y")
                .addParameter("createdAt", Timestamp.valueOf(STARTED_AT))
                .executeScalar(Long.TYPE));
  }

  private static Party createParty_1() {
    return Party.builder()
        .partyId("P047877901")
        .businessUnit("WPBU")
        .countryCode("UK")
        .state("ACTIVE")
        .taxRegistrationNumber("1098765")
        .txnHeaderId("txn_1")
        .validFrom(Date.valueOf("2020-01-01"))
        .validTo(Date.valueOf("2020-02-01"))
        .accounts(ACCOUNTS)
        .build();
  }

  private static Party createParty_2() {
    return Party.builder()
        .partyId("P047877902")
        .businessUnit("WPBU")
        .countryCode("UK")
        .state("ACTIVE")
        .taxRegistrationNumber("1098765")
        .txnHeaderId("txn_2")
        .validFrom(Date.valueOf("2020-01-01"))
        .validTo(Date.valueOf("2020-02-01"))
        .accounts(EMPTY_ACCOUNT_ARRAY)
        .build();
  }

  private static Account[] createAccounts() {
    return Array.of(
            Account.builder()
                .accountId("acct_1")
                .accountType("CHRG")
                .billCycleIdentifier("WPMO")
                .currency("GBP")
                .legalCounterParty("PO1100000001")
                .partyId("P047877901")
                .settlementRegionId("999900")
                .status("ACTIVE")
                .txnHeaderId("txn_1")
                .validFrom(Date.valueOf("2020-01-01"))
                .validTo(Date.valueOf("2020-02-01"))
                .subAccounts(SUB_ACCOUNTS)
                .withholdFund(WITHHOLD_FUND_1)
                .build(),
            Account.builder()
                .accountId("acct_2")
                .accountType("RECR")
                .billCycleIdentifier("WPDY")
                .currency("EUR")
                .legalCounterParty("PO1100000002")
                .partyId("P047877901")
                .settlementRegionId("1111100")
                .status("INACTIVE")
                .txnHeaderId("txn_1")
                .validFrom(Date.valueOf("2020-01-01"))
                .validTo(Date.valueOf("2020-02-01"))
                .subAccounts(EMPTY_SUB_ACCOUNT_ARRAY)
                .withholdFund(null)
                .build(),
            Account.builder()
                .accountId("acct_3")
                .accountType("FUND")
                .billCycleIdentifier("WPMO")
                .currency("GBP")
                .legalCounterParty("00003")
                .partyId("P047877901")
                .settlementRegionId("1111100")
                .status("INACTIVE")
                .txnHeaderId("txn_1")
                .validFrom(Date.valueOf("2020-01-01"))
                .validTo(Date.valueOf("2020-02-01"))
                .subAccounts(createAnotherSubAccounts())
                .withholdFund(WITHHOLD_FUND_2)
                .build())
        .toJavaArray(Account[]::new);
  }

  private static SubAccount[] createSubAccounts() {
    return Array.of(
            SubAccount.builder()
                .subAccountId("12345")
                .status("ACTIVE")
                .validFrom(Date.valueOf("2020-01-01"))
                .validTo(Date.valueOf("2020-02-01"))
                .subAccountType("CHRG")
                .accountId(ACCOUNT_ID_1)
                .build(),
            SubAccount.builder()
                .subAccountId("12346")
                .status("ACTIVE")
                .validFrom(Date.valueOf("2020-01-01"))
                .validTo(Date.valueOf("2020-02-01"))
                .subAccountType("CHBK")
                .accountId(ACCOUNT_ID_1)
                .build(),
            SubAccount.builder()
                .subAccountId("12347")
                .status("INACTIVE")
                .validFrom(Date.valueOf("2020-01-01"))
                .validTo(Date.valueOf("2020-02-01"))
                .subAccountType("FUND")
                .accountId(ACCOUNT_ID_1)
                .build())
        .toJavaArray(SubAccount[]::new);
  }

  private static SubAccount[] createAnotherSubAccounts() {
    return Array.of(
            SubAccount.builder()
                .subAccountId("22345")
                .status("ACTIVE")
                .validFrom(Date.valueOf("2020-01-01"))
                .validTo(Date.valueOf("2020-02-01"))
                .subAccountType("CHRG")
                .accountId(ACCOUNT_ID_3)
                .build(),
            SubAccount.builder()
                .subAccountId("22346")
                .status("ACTIVE")
                .validFrom(Date.valueOf("2020-01-01"))
                .validTo(Date.valueOf("2020-02-01"))
                .subAccountType("CHBK")
                .accountId(ACCOUNT_ID_3)
                .build(),
            SubAccount.builder()
                .subAccountId("22347")
                .status("INACTIVE")
                .validFrom(Date.valueOf("2020-01-01"))
                .validTo(Date.valueOf("2020-02-01"))
                .subAccountType("FUND")
                .accountId(ACCOUNT_ID_3)
                .build())
        .toJavaArray(SubAccount[]::new);
  }

  private static WithholdFund createWithholdFunds() {
    return WithholdFund.builder()
        .withholdFundPercentage(Double.parseDouble("10.25"))
        .withholdFundTarget("TARGET")
        .withholdFundType("WAF")
        .status("ACTIVE")
        .validFrom(Date.valueOf("2020-01-01"))
        .validTo(Date.valueOf("2020-02-01"))
        .accountId(ACCOUNT_ID_1)
        .build();
  }

  private static WithholdFund createAnotherWithholdFunds() {
    return WithholdFund.builder()
        .withholdFundPercentage(Double.parseDouble("10.25"))
        .withholdFundTarget(null)
        .withholdFundType("WAF")
        .status("ACTIVE")
        .validFrom(Date.valueOf("2020-01-01"))
        .validTo(Date.valueOf("2020-02-01"))
        .accountId(ACCOUNT_ID_3)
        .build();
  }
}
