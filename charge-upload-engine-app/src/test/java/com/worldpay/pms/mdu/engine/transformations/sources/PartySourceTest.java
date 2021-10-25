package com.worldpay.pms.mdu.engine.transformations.sources;

import static com.worldpay.pms.mdu.engine.samples.Transactions.*;
import static com.worldpay.pms.mdu.engine.samples.Transactions.WITHHOLD_FUND_WITH_ACCOUNT_ID_60001;
import static com.worldpay.pms.mdu.engine.utils.DbUtils.*;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.mdu.domain.model.output.Account;
import com.worldpay.pms.mdu.domain.model.output.Party;
import com.worldpay.pms.mdu.domain.model.output.SubAccount;
import com.worldpay.pms.mdu.engine.MerchantUploadConfig;
import com.worldpay.pms.mdu.engine.utils.DbUtils;
import com.worldpay.pms.mdu.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import io.vavr.collection.List;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PartySourceTest implements WithDatabaseAndSpark {
  private SqlDb appuserDb;
  private PartySource source;

  public static final LocalDateTime ILM_DT = LocalDateTime.of(2021, 3, 1, 0, 6, 1);
  public static final LocalDateTime LOW_WATERMARK = LocalDateTime.of(2021, 3, 1, 0, 6, 1);
  public static final LocalDateTime HIGH_WATERMARK = LocalDateTime.of(2021, 3, 1, 0, 6, 2);

  private static final List<Party> PARTY_WITH_ACCOUNT_AND_WITHOUT_SUB_ACCOUNT_AND_WITHHOLD_FUND =
      createPartyWithAccountAndWithoutSubAccountAndWithholdFund();
  private static final List<Party> PARTY_WITH_ACCOUNT_SUB_ACCOUNT_WITHHOLD_FUND =
      createPartyWithAccountSubAccountWithholdFund();
  private static final Account[] ACCOUNTS = createAccounts();
  private static final SubAccount[] SUB_ACCOUNTS = createSubAccounts();
  private static final String[] ACCOUNT_TYPE = new String[] {"FUND", "CHRG", "CHBK", "CRWD"};
  private static final String MERCHANT_DATA = "MERCHANT_DATA";

  @Override
  public void bindMerchantUploadConfiguration(MerchantUploadConfig conf) {
    this.source =
        new PartySource(
            conf.getSources().getPartySource(),
            conf.getSources().getAccountSource(),
            conf.getSources().getSubAccountSource(),
            conf.getSources().getWithholdFundSource());
  }

  @Override
  public void bindMerchantUploadJdbcConfiguration(JdbcConfiguration conf) {
    this.appuserDb = SqlDb.simple(conf);
  }

  @BeforeEach
  void cleanUp() {
    DbUtils.cleanUp(
        appuserDb,
        "party",
        "acct",
        "sub_acct",
        "withhold_funds",
        "batch_history",
        "outputs_registry");
  }

  @Test
  void readEmptyDatasetWhenNoRecordsFound(SparkSession spark) {
    Batch.BatchId batchId = createBatchId(LocalDate.of(2021, 1, 1), LocalDate.of(2021, 12, 1));
    Dataset<Party> txns = source.load(spark, batchId);
    assertThat(txns.count()).isEqualTo(0L);
  }

  @Test
  void readPartyWhenNoAccountRelatedEntryExist(SparkSession spark) {

    insertParties(
        appuserDb, "test_run", ILM_DT, PARTY_WITH_ID_PO00000001, PARTY_WITH_ID_PO00000002);
    // insert party with batch marked as failed
    insertParty(appuserDb, "test_run", ILM_DT, true, PARTY_WITH_ID_PO00000003);

    insertCompletedBatch(
        appuserDb, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCHANT_DATA);

    Batch.BatchId batchId = createBatchId(LOW_WATERMARK, HIGH_WATERMARK);
    java.util.List<Party> txns = source.load(spark, batchId).collectAsList();
    assertPartyAccountSubAccountWithholdFundRowsCount(txns, 2, 0, 0, 0);
    assertThat(txns).containsExactlyInAnyOrder(PARTY_WITH_ID_PO00000001, PARTY_WITH_ID_PO00000002);
  }

  @Test
  void readLastPartitionPartyWhenDifferentPartyIsInsertedInSuccessiveRun(SparkSession spark) {
    insertParties(
        appuserDb, "test_run", ILM_DT, PARTY_WITH_ID_PO00000001, PARTY_WITH_ID_PO00000002);
    insertParties(
        appuserDb,
        "test_run1",
        ILM_DT.plusMinutes(10),
        PARTY_WITH_ID_PO00000003,
        PARTY_WITH_ID_PO00000004);

    insertCompletedBatch(
        appuserDb, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCHANT_DATA);
    insertCompletedBatch(
        appuserDb,
        "test_run1",
        LOW_WATERMARK.plusMinutes(10),
        HIGH_WATERMARK.plusMinutes(10),
        ILM_DT.plusMinutes(10),
        MERCHANT_DATA);

    // irrespective of batch low and high watermark, it should read party from last run
    Batch.BatchId batchId = createBatchId(LOW_WATERMARK, HIGH_WATERMARK);
    java.util.List<Party> txns = source.load(spark, batchId).collectAsList();
    assertPartyAccountSubAccountWithholdFundRowsCount(txns, 2, 0, 0, 0);
    assertThat(txns).containsExactlyInAnyOrder(PARTY_WITH_ID_PO00000003, PARTY_WITH_ID_PO00000004);
  }

  @Test
  void readPartyWhenAccountExistButNoSubAccountAndWithhold(SparkSession spark) {
    insertParties(
        appuserDb, "test_run", ILM_DT, PARTY_WITH_ID_PO00000001, PARTY_WITH_ID_PO00000002);
    insertAccounts(appuserDb, "test_run", ILM_DT, ACCOUNTS);
    // insert party and account with batch marked as failed
    insertParty(appuserDb, "test_run", ILM_DT, true, PARTY_WITH_ID_PO00000003);
    insertAccount(appuserDb, "test_run", ILM_DT, true, FAILED_ACCOUNT_WITH_PARTY_PO00000003);

    insertCompletedBatch(
        appuserDb, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCHANT_DATA);

    Batch.BatchId batchId = createBatchId(LOW_WATERMARK, HIGH_WATERMARK);
    java.util.List<Party> txns = source.load(spark, batchId).collectAsList();

    assertPartyAccountSubAccountWithholdFundRowsCount(txns, 2, 8, 0, 0);
    // sorting account array to make comparison with in-mem objects compatible
    txns.forEach(
        party -> Arrays.sort(party.getAccounts(), Comparator.comparing(Account::getAccountId)));
    assertThat(txns)
        .containsExactlyInAnyOrderElementsOf(
            PARTY_WITH_ACCOUNT_AND_WITHOUT_SUB_ACCOUNT_AND_WITHHOLD_FUND);
  }

  @Test
  void readPartyWhenOnlyAccountExistButNoParty(SparkSession spark) {
    // not ideal scenario, just covering anomalous behaviour

    insertAccounts(appuserDb, "test_run", ILM_DT, ACCOUNTS);
    // insert account with batch marked as failed
    insertAccount(appuserDb, "test_run", ILM_DT, true, FAILED_ACCOUNT_WITH_PARTY_PO00000003);

    insertCompletedBatch(
        appuserDb, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCHANT_DATA);

    Batch.BatchId batchId = createBatchId(LOW_WATERMARK, HIGH_WATERMARK);
    java.util.List<Party> txns = source.load(spark, batchId).collectAsList();
    assertPartyAccountSubAccountWithholdFundRowsCount(txns, 0, 0, 0, 0);
  }

  @Test
  void readPartyWhenSubAccountExistWithoutAccount(SparkSession spark) {
    // not ideal scenario, just covering anomalous behaviour

    insertParties(
        appuserDb, "test_run", ILM_DT, PARTY_WITH_ID_PO00000001, PARTY_WITH_ID_PO00000002);
    insertSubAccounts(appuserDb, "test_run", ILM_DT, SUB_ACCOUNTS);
    // insert party and sub-account with failed batch
    insertParty(appuserDb, "test_run", ILM_DT, true, PARTY_WITH_ID_PO00000003);
    insertSubAccount(appuserDb, "test_run", ILM_DT, true, FAILED_SUB_ACCOUNT_WITH_ACCOUNT_ID_99999);

    insertCompletedBatch(
        appuserDb, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCHANT_DATA);

    Batch.BatchId batchId = createBatchId(LOW_WATERMARK, HIGH_WATERMARK);
    java.util.List<Party> txns = source.load(spark, batchId).collectAsList();
    assertPartyAccountSubAccountWithholdFundRowsCount(txns, 2, 0, 0, 0);
    assertThat(txns).containsExactlyInAnyOrder(PARTY_WITH_ID_PO00000001, PARTY_WITH_ID_PO00000002);
  }

  @Test
  void readPartyWhenAccountSubAccountAndWithholdFundExist(SparkSession spark) {

    insertParties(
        appuserDb, "test_run", ILM_DT, PARTY_WITH_ID_PO00000001, PARTY_WITH_ID_PO00000002);
    insertAccounts(appuserDb, "test_run", ILM_DT, ACCOUNTS);
    insertSubAccounts(appuserDb, "test_run", ILM_DT, SUB_ACCOUNTS);
    insertWithholdFunds(
        appuserDb,
        "test_run",
        ILM_DT,
        WITHHOLD_FUND_WITH_ACCOUNT_ID_50001,
        WITHHOLD_FUND_WITH_ACCOUNT_ID_60001);

    // insert party, account, sub-account and waf with failed batch
    insertParty(appuserDb, "test_run", ILM_DT, true, PARTY_WITH_ID_PO00000003);
    insertAccount(appuserDb, "test_run", ILM_DT, true, FAILED_ACCOUNT_WITH_PARTY_PO00000003);
    insertSubAccount(appuserDb, "test_run", ILM_DT, true, FAILED_SUB_ACCOUNT_WITH_ACCOUNT_ID_99999);
    insertWithholdFund(
        appuserDb, "test_run", ILM_DT, true, FAILED_WITHHOLD_FUND_WITH_ACCOUNT_ID_99999);

    insertCompletedBatch(
        appuserDb, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCHANT_DATA);

    Batch.BatchId batchId = createBatchId(LOW_WATERMARK, HIGH_WATERMARK);
    java.util.List<Party> txns = source.load(spark, batchId).collectAsList();
    assertPartyAccountSubAccountWithholdFundRowsCount(txns, 2, 8, 8, 2);
    // sorting account array to make in-mem objects comparison compatible
    txns.forEach(
        party -> Arrays.sort(party.getAccounts(), Comparator.comparing(Account::getAccountId)));
    assertThat(txns)
        .containsExactlyInAnyOrderElementsOf(PARTY_WITH_ACCOUNT_SUB_ACCOUNT_WITHHOLD_FUND);
  }

  @Test
  void readPicksAllTablePartitionAndSubPartitions(SparkSession spark) {

    insertPartyAndAccountRelatedRowsInEveryPartition();

    insertCompletedBatch(
        appuserDb, "test_run", LOW_WATERMARK, HIGH_WATERMARK, ILM_DT, MERCHANT_DATA);

    Batch.BatchId batchId = createBatchId(LOW_WATERMARK, HIGH_WATERMARK);
    Dataset<Party> txns = source.load(spark, batchId).cache();
    assertPartyAccountSubAccountWithholdFundRowsCount(txns.collectAsList(), 8, 32, 32, 8);
    // assert each partition is non empty
    txns.foreachPartition(iterator -> assertThat(iterator.hasNext()).isTrue());
  }

  private void insertPartyAndAccountRelatedRowsInEveryPartition() {
    for (int i = 1, k = 1; i <= 8; i++) {
      String txnHeaderId = format("0000%s", i);
      String partyId = format("PO0000000%s", i);
      insertParty(
          appuserDb,
          "test_run",
          ILM_DT,
          false,
          PARTY.partyId(partyId).txnHeaderId(txnHeaderId).build());
      for (int j = 1; j <= 4; j++, k++) {
        insertAccount(
            appuserDb,
            "test_run",
            ILM_DT,
            false,
            ACCOUNT
                .accountId(format("1000%s", k))
                .partyId(partyId)
                .txnHeaderId(txnHeaderId)
                .accountType(ACCOUNT_TYPE[j - 1])
                .build());
        insertSubAccount(
            appuserDb,
            "test_run",
            ILM_DT,
            false,
            SUB_ACCOUNT
                .subAccountId(format("2000%s", k))
                .accountId(format("1000%s", k))
                .subAccountType(ACCOUNT_TYPE[j - 1])
                .build());
      }
      insertWithholdFund(
          appuserDb,
          "test_run",
          ILM_DT,
          false,
          WITHHOLD_FUND.accountId(format("1000%s", i)).build());
    }
  }

  private void assertPartyAccountSubAccountWithholdFundRowsCount(
      java.util.List<Party> result, int party, int account, int subAccount, int withholdFund) {
    Supplier<Stream<Account>> successAccounts =
        () ->
            result.stream()
                .map(Party::getAccounts)
                .filter(Objects::nonNull)
                .flatMap(Arrays::stream);

    assertThat(result.size()).isEqualTo(party);
    assertThat(successAccounts.get().count()).isEqualTo(account);
    assertThat(
            successAccounts
                .get()
                .map(Account::getSubAccounts)
                .filter(Objects::nonNull)
                .flatMap(Arrays::stream)
                .count())
        .isEqualTo(subAccount);
    assertThat(successAccounts.get().map(Account::getWithholdFund).filter(Objects::nonNull).count())
        .isEqualTo(withholdFund);
  }

  private static List<Party> createPartyWithAccountAndWithoutSubAccountAndWithholdFund() {
    return List.of(
        PARTY_WITH_ID_PO00000001.withAccounts(
            new Account[] {
              FUND_ACCOUNT_WITH_PARTY_PO00000001,
              CHRG_ACCOUNT_WITH_PARTY_PO00000001,
              CHBK_ACCOUNT_WITH_PARTY_PO00000001,
              CRWD_ACCOUNT_WITH_PARTY_PO00000001
            }),
        PARTY_WITH_ID_PO00000002.withAccounts(
            new Account[] {
              FUND_ACCOUNT_WITH_PARTY_PO00000002,
              CHRG_ACCOUNT_WITH_PARTY_PO00000002,
              CHBK_ACCOUNT_WITH_PARTY_PO00000002,
              CRWD_ACCOUNT_WITH_PARTY_PO00000002
            }));
  }

  private static List<Party> createPartyWithAccountSubAccountWithholdFund() {
    return List.of(
        PARTY_WITH_ID_PO00000001.withAccounts(
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
            }),
        PARTY_WITH_ID_PO00000002.withAccounts(
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
            }));
  }

  private static Account[] createAccounts() {
    return new Account[] {
      FUND_ACCOUNT_WITH_PARTY_PO00000001,
      CHRG_ACCOUNT_WITH_PARTY_PO00000001,
      CHBK_ACCOUNT_WITH_PARTY_PO00000001,
      CRWD_ACCOUNT_WITH_PARTY_PO00000001,
      FUND_ACCOUNT_WITH_PARTY_PO00000002,
      CHRG_ACCOUNT_WITH_PARTY_PO00000002,
      CHBK_ACCOUNT_WITH_PARTY_PO00000002,
      CRWD_ACCOUNT_WITH_PARTY_PO00000002
    };
  }

  private static SubAccount[] createSubAccounts() {
    return new SubAccount[] {
      FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_50001,
      CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_50002,
      CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_50003,
      CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_50004,
      FUND_SUB_ACCOUNT_WITH_ACCOUNT_ID_60001,
      CHRG_SUB_ACCOUNT_WITH_ACCOUNT_ID_60002,
      CHBK_SUB_ACCOUNT_WITH_ACCOUNT_ID_60003,
      CRWD_SUB_ACCOUNT_WITH_ACCOUNT_ID_60004
    };
  }
}
