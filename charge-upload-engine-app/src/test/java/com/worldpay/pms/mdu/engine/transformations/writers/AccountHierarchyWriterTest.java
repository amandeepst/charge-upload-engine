package com.worldpay.pms.mdu.engine.transformations.writers;

import static com.worldpay.pms.mdu.engine.encoder.Encoders.ACCOUNT_HIERARCHY_ENCODER;
import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.mdu.domain.model.output.AccountHierarchy;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Encoder;

public class AccountHierarchyWriterTest extends MerchantUploadJdbcWritersTest<AccountHierarchy> {

  private static final AccountHierarchy ACCT_HIER = createAcctHier_1();
  private static final String QUERY_DELETE_ACCT_HIER_BY_PARTY_ID =
      resourceAsString("sql/account_hierarchy/delete_acct_hier.sql");
  private static final String QUERY_COUNT_ACCT_HIER =
      resourceAsString("sql/account_hierarchy/count_acct_hier.sql");
  private static final String PARTY_ID_1 = "P047877901";

  @Override
  protected void init() {
    db.execQuery(
        "delete-acct-hier-by-party-id",
        QUERY_DELETE_ACCT_HIER_BY_PARTY_ID,
        (query) -> query.withParams(PARTY_ID_1).executeUpdate());
  }

  @Override
  protected void assertRowsWritten() {
    long businessAcctHierCount = getAccountHier(ACCT_HIER);
    assertThat(businessAcctHierCount, is(1L));
  }

  @Override
  protected void assertNoRowsWritten() {
    long count =
        db.execQuery(
            "count-acct-hier-by-id",
            "select count(*) from acct_hier where parent_acct_party_id=:parentPartyId",
            (query) -> query.addParameter("parentPartyId", PARTY_ID_1).executeScalar(Long.TYPE));
    assertThat(count, is(0L));
  }

  @Override
  protected List<AccountHierarchy> provideSamples() {
    return Arrays.asList(ACCT_HIER);
  }

  @Override
  protected Encoder<AccountHierarchy> encoder() {
    return ACCOUNT_HIERARCHY_ENCODER;
  }

  @Override
  protected JdbcWriter<AccountHierarchy> createWriter(
      JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new AccountHierarchyWriter(jdbcWriterConfiguration, spark);
  }

  private long getAccountHier(AccountHierarchy acctHier) {
    return db.execQuery(
        "count_acct_hier",
        QUERY_COUNT_ACCT_HIER,
        (query) ->
            query
                .addParameter("txnHeaderId", acctHier.getTxnHeaderId())
                .addParameter("parentPartyId", acctHier.getParentPartyId())
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .addParameter("ilmDateTime", Timestamp.valueOf(STARTED_AT))
                .addParameter("ilmArchiveSwitch", "Y")
                .addParameter("createdAt", Timestamp.valueOf(STARTED_AT))
                .executeScalar(Long.TYPE));
  }

  private static AccountHierarchy createAcctHier_1() {
    return AccountHierarchy.builder()
        .txnHeaderId("11111")
        .parentPartyId("P047877901")
        .parentAccountId("A1")
        .childPartyId("P2")
        .childAccountId("A2")
        .status("ACTIVE")
        .validFrom(Date.valueOf("2021-03-01"))
        .validTo(Date.valueOf("2021-04-01"))
        .build();
  }
}
