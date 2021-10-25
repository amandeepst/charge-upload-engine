package com.worldpay.pms.mdu.engine.transformations.writers;

import static com.worldpay.pms.mdu.engine.encoder.Encoders.PARTY_HIERARCHY_ENCODER;
import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.mdu.domain.model.output.PartyHierarchy;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Encoder;

public class PartyHierarchyWriterTest extends MerchantUploadJdbcWritersTest<PartyHierarchy> {

  private static final PartyHierarchy PARTY_HIER = createPartyHierarchy();
  private static final PartyHierarchy PARTY_HIER_2 = createPartyHierarchy2();
  private static final String QUERY_DELETE_PARTY_HIER_BY_PARTY_ID =
      resourceAsString("sql/party_hierarchy/delete_party_hier.sql");
  private static final String QUERY_COUNT_PARTY_HIER =
      resourceAsString("sql/party_hierarchy/count_party_hier.sql");
  private static final String PARENT_PARTY_ID = "P047877900";
  private static final String CHILD_PARTY_ID = "P047877901";

  @Override
  protected void init() {
    db.execQuery(
        "delete-party-hier-by-party-id",
        QUERY_DELETE_PARTY_HIER_BY_PARTY_ID,
        (query) -> query.withParams(PARENT_PARTY_ID).executeUpdate());
  }

  @Override
  protected void assertRowsWritten() {
    long businessPartyHierCount = getPartyHier(PARTY_HIER);
    assertThat(businessPartyHierCount, is(1L));
    long businessPartyHierCount2 = getPartyHier(PARTY_HIER_2);
    assertThat(businessPartyHierCount2, is(1L));
  }

  @Override
  protected void assertNoRowsWritten() {
    long count =
        db.execQuery(
            "count-party-hier-by-id",
            "select count(*) from party_hier where PARENT_PARTY_ID=:parentPartyId and CHILD_PARTY_ID =:childPartyId",
            (query) ->
                query
                    .addParameter("parentPartyId", PARENT_PARTY_ID)
                    .addParameter("childPartyId", CHILD_PARTY_ID)
                    .executeScalar(Long.TYPE));
    assertThat(count, is(0L));
  }

  @Override
  protected List<PartyHierarchy> provideSamples() {
    return Arrays.asList(PARTY_HIER);
  }

  @Override
  protected Encoder<PartyHierarchy> encoder() {
    return PARTY_HIERARCHY_ENCODER;
  }

  @Override
  protected JdbcWriter<PartyHierarchy> createWriter(
      JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new PartyHierarchyWriter(jdbcWriterConfiguration, spark);
  }

  private long getPartyHier(PartyHierarchy partyHier) {
    return db.execQuery(
        "count_party_hier",
        QUERY_COUNT_PARTY_HIER,
        (query) ->
            query
                .addParameter("txnHeaderId", partyHier.getTxnHeaderId())
                .addParameter("parentPartyId", partyHier.getParentPartyId())
                .addParameter("childPartyId", partyHier.getChildPartyId())
                .addParameter("batchCode", BATCH_ID.code)
                .addParameter("batchAttempt", BATCH_ID.attempt)
                .addParameter("ilmDateTime", Timestamp.valueOf(STARTED_AT))
                .addParameter("ilmArchiveSwitch", "Y")
                .addParameter("createdAt", Timestamp.valueOf(STARTED_AT))
                .executeScalar(Long.TYPE));
  }

  private static PartyHierarchy createPartyHierarchy() {
    return PartyHierarchy.builder()
        .childPartyId("P047877901")
        .parentPartyId("P047877900")
        .status("ACTIVE")
        .validFrom(Date.valueOf("2021-03-01"))
        .validTo(Date.valueOf("2021-04-01"))
        .txnHeaderId("11111")
        .partyHierType("SAMPLE")
        .build();
  }

  private static PartyHierarchy createPartyHierarchy2() {
    return PartyHierarchy.builder()
        .childPartyId("P047877901")
        .parentPartyId("P047877900")
        .status("ACTIVE")
        .validFrom(Date.valueOf("2021-03-01"))
        .validTo(Date.valueOf("2021-04-01"))
        .txnHeaderId("11111")
        .partyHierType("SAMPLE")
        .updated(true)
        .creationDate(Timestamp.valueOf(LocalDateTime.now()))
        .build();
  }
}
