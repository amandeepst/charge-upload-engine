package com.worldpay.pms.mdu.engine.utils;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static java.util.Objects.nonNull;

import com.worldpay.pms.mdu.domain.exception.MerchantUploadException;
import com.worldpay.pms.mdu.domain.model.output.Account;
import com.worldpay.pms.mdu.domain.model.output.AccountHierarchy;
import com.worldpay.pms.mdu.domain.model.output.Party;
import com.worldpay.pms.mdu.domain.model.output.SubAccount;
import com.worldpay.pms.mdu.domain.model.output.WithholdFund;
import com.worldpay.pms.mdu.engine.transformations.ErrorAccountHierarchy;
import com.worldpay.pms.mdu.engine.transformations.model.input.AccountHierarchyDataRow;
import com.worldpay.pms.mdu.engine.transformations.model.input.MerchantDataAccountRow;
import com.worldpay.pms.mdu.engine.transformations.model.input.MerchantDataPartyRow;
import com.worldpay.pms.spark.core.PMSException;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import io.vavr.collection.List;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2oException;
import org.sql2o.Sql2oQuery;

@UtilityClass
public class DbUtils {

  public static void cleanUp(SqlDb db, String... tables) {
    List.of(tables).forEach(deleteFromTable(db));
  }

  private static Consumer<String> deleteFromTable(SqlDb db) {
    return table ->
        db.exec(
            "clear " + table,
            conn -> {
              try (Query q = conn.createQuery("DELETE FROM " + table)) {
                q.executeUpdate();
              }
              return true;
            });
  }

  public static Batch insertBatchHistoryAndOnBatchCompleted(
      SqlDb sqlDb,
      String batchCode,
      String state,
      Timestamp lowWatermark,
      Timestamp highWatermark,
      Timestamp createdAt,
      Timestamp ilmDt,
      String... datasetId) {
    Batch batch =
        new Batch(
            new Batch.BatchId(
                batchCode,
                new Batch.Watermark(
                    lowWatermark.toLocalDateTime(), highWatermark.toLocalDateTime()),
                1),
            Batch.BatchStep.valueOf(state),
            createdAt.toLocalDateTime(),
            null,
            null);

    sqlDb.exec(
        "insert_batch_history_and_on_batch_completed",
        conn -> {
          if ("COMPLETED".equals(state)) {
            onBatchCompleted(conn, batch, ilmDt, datasetId);
          }
          return insertBatchHistory(conn, batch);
        });

    return batch;
  }

  public static void insertCompletedBatch(
      SqlDb appuserDb,
      String batchCode,
      LocalDateTime lowWatermark,
      LocalDateTime highWatermark,
      LocalDateTime ilmDate,
      String... datasetId) {
    insertBatchHistoryAndOnBatchCompleted(
        appuserDb,
        batchCode,
        "COMPLETED",
        Timestamp.valueOf(lowWatermark),
        Timestamp.valueOf(highWatermark),
        Timestamp.valueOf(ilmDate),
        Timestamp.valueOf(ilmDate),
        datasetId);
  }

  public static void deleteBatchHistoryAndOnBatchCompleted(SqlDb db, String batchCode) {
    db.execQuery(
        "delete-batch-history",
        resourceAsString("sql/batch_history/delete_batch_history_by_batch_code.sql"),
        (query) -> query.addParameter("batch_code", batchCode).executeUpdate());
    db.execQuery(
        "delete-batch-history",
        resourceAsString("sql/outputs_registry/delete_outputs_registry_by_batch_code.sql"),
        (query) -> query.addParameter("batch_code", batchCode).executeUpdate());
  }

  private void onBatchCompleted(
      Connection conn, Batch batch, Timestamp ilmDt, String... datasetId) {
    Sql2oQuery query =
        new Sql2oQuery(conn, resourceAsString("sql/outputs_registry/insert_outputs_registry.sql"));
    List.of(datasetId)
        .forEach(
            id -> {
              try {
                query
                    .addParameter("batch_code", batch.id.code)
                    .addParameter("batch_attempt", batch.id.attempt)
                    .addParameter("ilm_dt", ilmDt)
                    .addParameter("dataset_id", id)
                    .executeUpdate();

              } catch (Sql2oException e) {
                throw new MerchantUploadException(
                    e, "Unhandled exception when executing query:insert on completed batch");
              }
            });
  }

  private Batch insertBatchHistory(Connection conn, Batch batch) {
    Sql2oQuery query =
        new Sql2oQuery(conn, resourceAsString("sql/batch_history/insert_batch_history.sql"));

    try {
      query
          .addParameter("batch_code", batch.id.code)
          .addParameter("attempt", batch.id.attempt)
          .addParameter("state", batch.step.name())
          .addParameter("watermark_low", Timestamp.valueOf(batch.id.watermark.low))
          .addParameter("watermark_high", Timestamp.valueOf(batch.id.watermark.high))
          .addParameter("comments", "")
          .addParameter("metadata", "")
          .addParameter("created_at", Timestamp.valueOf(batch.createdAt))
          .executeUpdate();
    } catch (Sql2oException e) {
      throw new PMSException(e, "Unhandled exception when executing query:insert batch history");
    }

    return batch;
  }

  public void insertPartiesWithAccounts(
      SqlDb db, String batchCode, LocalDateTime ilmDate, Party... parties) {
    List.of(parties)
        .forEach(
            party -> {
              insertParty(db, batchCode, ilmDate, false, party);
              List.of(party.getAccounts())
                  .forEach(
                      account -> {
                        insertAccount(db, batchCode, ilmDate, false, account);
                        insertSubAccounts(db, batchCode, ilmDate, account.getSubAccounts());
                        if (nonNull(account.getWithholdFund())) {
                          insertWithholdFund(
                              db, batchCode, ilmDate, false, account.getWithholdFund());
                        }
                      });
            });
  }

  public void insertParties(SqlDb db, String batchCode, LocalDateTime ilmDate, Party... parties) {
    List.of(parties).forEach(party -> insertParty(db, batchCode, ilmDate, false, party));
  }

  public void insertAccounts(
      SqlDb db, String batchCode, LocalDateTime ilmDate, Account... accounts) {
    List.of(accounts).forEach(account -> insertAccount(db, batchCode, ilmDate, false, account));
  }

  public void insertSubAccounts(
      SqlDb db, String batchCode, LocalDateTime ilmDate, SubAccount... subAccounts) {
    List.of(subAccounts)
        .forEach(subAccount -> insertSubAccount(db, batchCode, ilmDate, false, subAccount));
  }

  public void insertWithholdFunds(
      SqlDb db, String batchCode, LocalDateTime ilmDate, WithholdFund... withholdFunds) {
    List.of(withholdFunds)
        .forEach(withholdFund -> insertWithholdFund(db, batchCode, ilmDate, false, withholdFund));
  }

  public void insertParty(
      SqlDb db, String batchCode, LocalDateTime ilmDate, boolean isFailed, Party party) {
    db.execQuery(
        "insert-party",
        resourceAsString("sql/outputs/insert_party.sql"),
        query ->
            query
                .addParameter("party_id", party.getPartyId())
                .addParameter("txn_header_id", party.getTxnHeaderId())
                .addParameter("country_cd", party.getCountryCode())
                .addParameter("state", party.getState())
                .addParameter("business_unit", party.getBusinessUnit())
                .addParameter("merch_tax_reg", party.getTaxRegistrationNumber())
                .addParameter("valid_from", party.getValidFrom())
                .addParameter("cre_dttm", party.getCreationDate())
                .addParameter("valid_to", party.getValidTo())
                .addParameter("batch_code", isFailed ? "failed_batch" : batchCode)
                .addParameter("batch_attempt", 1)
                .addParameter("ilm_dt", Timestamp.valueOf(ilmDate))
                .addParameter(
                    "partition_id",
                    Integer.parseInt(party.getPartyId().substring(party.getPartyId().length() - 1)))
                .executeUpdate());
  }

  public void insertAccount(
      SqlDb db, String batchCode, LocalDateTime ilmDate, boolean isFailed, Account account) {
    db.execQuery(
        "insert-account",
        resourceAsString("sql/outputs/insert_acct.sql"),
        query ->
            query
                .addParameter("acct_id", account.getAccountId())
                .addParameter("txn_header_id", account.getTxnHeaderId())
                .addParameter("party_id", account.getPartyId())
                .addParameter("acct_type", account.getAccountType())
                .addParameter("lcp", account.getLegalCounterParty())
                .addParameter("currency_cd", account.getCurrency())
                .addParameter("status", account.getStatus())
                .addParameter("bill_cyc_id", account.getBillCycleIdentifier())
                .addParameter("settlement_region_id", account.getSettlementRegionId())
                .addParameter("valid_from", account.getValidFrom())
                .addParameter("valid_to", account.getValidTo())
                .addParameter("cre_dttm", account.getCreationDate())
                .addParameter("batch_code", isFailed ? "failed_batch" : batchCode)
                .addParameter("batch_attempt", 1)
                .addParameter("ilm_dt", Timestamp.valueOf(ilmDate))
                .addParameter(
                    "partition_id",
                    Integer.parseInt(
                        account.getAccountId().substring(account.getAccountId().length() - 2)))
                .executeUpdate());
  }

  public void insertSubAccount(
      SqlDb db, String batchCode, LocalDateTime ilmDate, boolean isFailed, SubAccount subAccount) {
    db.execQuery(
        "insert-sub-acct",
        resourceAsString("sql/outputs/insert_sub_acct.sql"),
        query ->
            query
                .addParameter("sub_acct_id", subAccount.getSubAccountId())
                .addParameter("acct_id", subAccount.getAccountId())
                .addParameter("sub_acct_type", subAccount.getSubAccountType())
                .addParameter("status", subAccount.getStatus())
                .addParameter("valid_from", subAccount.getValidFrom())
                .addParameter("valid_to", subAccount.getValidTo())
                .addParameter("cre_dttm", subAccount.getCreationDate())
                .addParameter("batch_code", isFailed ? "failed_batch" : batchCode)
                .addParameter("batch_attempt", 1)
                .addParameter("ilm_dt", Timestamp.valueOf(ilmDate))
                .addParameter(
                    "partition_id",
                    Integer.parseInt(
                        subAccount
                            .getAccountId()
                            .substring(subAccount.getAccountId().length() - 2)))
                .executeUpdate());
  }

  public void insertWithholdFund(
      SqlDb db,
      String batchCode,
      LocalDateTime ilmDate,
      boolean isFailed,
      WithholdFund withholdFund) {
    db.execQuery(
        "insert-sub-acct",
        resourceAsString("sql/outputs/insert_withhold_funds.sql"),
        query ->
            query
                .addParameter("acct_id", withholdFund.getAccountId())
                .addParameter("withhold_fund_type", withholdFund.getWithholdFundType())
                .addParameter("withhold_fund_target", withholdFund.getWithholdFundTarget())
                .addParameter("withhold_fund_prcnt", withholdFund.getWithholdFundPercentage())
                .addParameter("status", withholdFund.getStatus())
                .addParameter("valid_from", withholdFund.getValidFrom())
                .addParameter("valid_to", withholdFund.getValidTo())
                .addParameter("cre_dttm", withholdFund.getCreationDate())
                .addParameter("batch_code", isFailed ? "failed_batch" : batchCode)
                .addParameter("batch_attempt", 1)
                .addParameter("ilm_dt", Timestamp.valueOf(ilmDate))
                .addParameter(
                    "partition_id",
                    Integer.parseInt(
                        withholdFund
                            .getAccountId()
                            .substring(withholdFund.getAccountId().length() - 1)))
                .executeUpdate());
  }

  public void insertAccountHierarchies(
      SqlDb db, String batchCode, LocalDateTime ilmDate, AccountHierarchy... hierarchies) {
    List.of(hierarchies)
        .forEach(
            accountHierarchy -> insertAccountHierarchy(db, batchCode, ilmDate, accountHierarchy));
  }

  private static void insertAccountHierarchy(
      SqlDb db, String batchCode, LocalDateTime ilmDate, AccountHierarchy accountHierarchy) {
    db.execQuery(
        "insert-account-hierarchy-output",
        resourceAsString("sql/outputs/insert_account_hierarchy.sql"),
        query ->
            query
                .addParameter("parent_acct_party_id", accountHierarchy.getParentPartyId())
                .addParameter("parent_acct_id", accountHierarchy.getParentAccountId())
                .addParameter("child_acct_party_id", accountHierarchy.getChildPartyId())
                .addParameter("child_acct_id", accountHierarchy.getChildAccountId())
                .addParameter("txn_header_id", accountHierarchy.getTxnHeaderId())
                .addParameter("status", accountHierarchy.getStatus())
                .addParameter("valid_from", accountHierarchy.getValidFrom())
                .addParameter("valid_to", accountHierarchy.getValidTo())
                .addParameter("cre_dttm", accountHierarchy.getCreationDate())
                .addParameter("batch_code", batchCode)
                .addParameter("batch_attempt", 1)
                .addParameter("ilm_dt", Timestamp.valueOf(ilmDate))
                .addParameter(
                    "partition_id",
                    Integer.parseInt(
                        accountHierarchy
                            .getParentAccountId()
                            .substring(accountHierarchy.getParentAccountId().length() - 1)))
                .executeUpdate());
  }

  public static void insertMerchantPartiesWithAccounts(
      SqlDb db, LocalDateTime ilmDate, boolean isWafActive, MerchantDataPartyRow... rows) {
    List.of(rows)
        .forEach(
            merchantParty -> {
              insertMerchantParty(db, ilmDate, isWafActive, merchantParty);
              insertMerchantAccounts(db, merchantParty.getAccounts());
            });
  }

  public static void insertMerchantParties(
      SqlDb db, LocalDateTime ilmDate, boolean isWafActive, MerchantDataPartyRow... rows) {
    List.of(rows).forEach(row -> insertMerchantParty(db, ilmDate, isWafActive, row));
  }

  public static void insertMerchantAccounts(SqlDb db, MerchantDataAccountRow... rows) {
    Stream.of(rows).forEach(row -> insertMerchantAccount(db, row));
  }

  public static void insertMerchantParty(
      SqlDb db, LocalDateTime ilmDate, boolean isWafActive, MerchantDataPartyRow row) {
    db.execQuery(
        "insert-merch-stg",
        resourceAsString("sql/inputs/insert_cm_merch_stg.sql"),
        query ->
            query
                .addParameter("txn_header_id", row.getTxnHeaderId())
                .addParameter("per_or_bus_flg", row.getPersonOrBusinessFlag())
                .addParameter("division", row.getDivision())
                .addParameter("per_id_nbr", row.getPartyId())
                .addParameter("tax_registration_nbr", row.getTaxRegistrationNumber())
                .addParameter("country", row.getCountry())
                .addParameter("state", row.getState())
                .addParameter("valid_from", row.getValidFrom())
                .addParameter("valid_to", row.getValidTo())
                .addParameter("ilm_dt", Timestamp.valueOf(ilmDate))
                .executeUpdate());
    insertMerchantChar(db, row.getTxnHeaderId(), "WPBU", row.getBusinessUnit());
    insertMerchantChar(db, row.getTxnHeaderId(), "IND WAF", isWafActive ? "Y" : "N");
  }

  private static void insertMerchantChar(
      SqlDb db, String txnHeaderId, String charTypeCode, String charValue) {
    db.execQuery(
        "insert-merch-char",
        resourceAsString("sql/inputs/insert_cm_merch_char.sql"),
        query ->
            query
                .addParameter("txn_header_id", txnHeaderId)
                .addParameter("char_type_cd", charTypeCode)
                .addParameter("char_val", charValue)
                .executeUpdate());
  }

  public static void insertMerchantAccount(SqlDb db, MerchantDataAccountRow row) {
    db.execQuery(
        "insert-merch-acct",
        resourceAsString("sql/inputs/insert_cm_acct_stg.sql"),
        query ->
            query
                .addParameter("txn_header_id", row.getTxnHeaderId())
                .addParameter("account_type", row.getAccountType())
                .addParameter("currency", row.getCurrency())
                .addParameter("bill_cyc_cd", row.getBillCycleCode())
                .addParameter("valid_from", row.getValidFrom())
                .addParameter("valid_to", row.getValidTo())
                .addParameter("settlement_region_id", row.getSettlementRegionId())
                .addParameter("cancellation_flag", row.getCancellationFlag())
                .executeUpdate());
  }

  public static void insertAccountHierarchiesStage(
      SqlDb db, LocalDateTime ilmDate, AccountHierarchyDataRow... rows) {
    List.of(rows).forEach(row -> insertAccountHeirarchyStage(db, ilmDate, row));
  }

  public static void insertAccountHeirarchyStage(
      SqlDb db, LocalDateTime ilmDate, AccountHierarchyDataRow row) {
    db.execQuery(
        "insert-acct-hier-stg",
        resourceAsString("sql/inputs/insert_cm_inv_grp_stg.sql"),
        query ->
            query
                .addParameter("txnHeaderId", row.getTxnHeaderId())
                .addParameter("txnDetailId", row.getTxnHeaderId())
                .addParameter("accountType", row.getAccountType())
                .addParameter("parentPartyId", row.getParentPartyId())
                .addParameter("childPartyId", row.getChildPartyId())
                .addParameter("currency", row.getCurrency())
                .addParameter("startDate", row.getValidFrom())
                .addParameter("endDate", row.getValidTo())
                .addParameter("cisDivsion", "00001")
                .addParameter("cancellationFlag", row.getCancellationFlag())
                .addParameter("ilmDate", Timestamp.valueOf(ilmDate))
                .executeUpdate());
  }

  public static long countPartyByAttributes(SqlDb db, Party party) {
    return db.execQuery(
        "count-party-by-attributes",
        resourceAsString("sql/count_by_attributes/count_party_by_all_fields.sql"),
        query ->
            query
                .addParameter("partyId", party.getPartyId())
                .addParameter("countryCode", party.getCountryCode())
                .addParameter("state", party.getState())
                .addParameter("businessUnit", party.getBusinessUnit())
                .addParameter("merchantTaxRegistration", party.getTaxRegistrationNumber())
                .addParameter("txnHeaderId", party.getTxnHeaderId())
                .addParameter("validFrom", party.getValidFrom())
                .addParameter("validTo", party.getValidTo())
                .addParameter("cre_dttm", party.getCreationDate())
                .addParameter("ilmArchiveSwitch", "Y")
                .executeScalar(Long.class));
  }

  public static long countAccountByAttributes(SqlDb db, Account account) {
    return db.execQuery(
        "count-account-by-attributes",
        resourceAsString("sql/count_by_attributes/count_acct_by_all_fields.sql"),
        query ->
            query
                .addParameter("accountId", account.getAccountId())
                .addParameter("partyId", account.getPartyId())
                .addParameter("accountType", account.getAccountType())
                .addParameter("lcp", account.getLegalCounterParty())
                .addParameter("currencyCode", account.getCurrency())
                .addParameter("status", account.getStatus())
                .addParameter("billCycleId", account.getBillCycleIdentifier())
                .addParameter("settlementRegionId", account.getSettlementRegionId())
                .addParameter("txnHeaderId", account.getTxnHeaderId())
                .addParameter("validFrom", account.getValidFrom())
                .addParameter("validTo", account.getValidTo())
                .addParameter("cre_dttm", account.getCreationDate())
                .addParameter("ilmArchiveSwitch", "Y")
                .executeScalar(Long.class));
  }

  public static long countSubAccountByAttributes(SqlDb db, SubAccount subAccount) {
    return db.execQuery(
        "count-sub-account-by-attributes",
        resourceAsString("sql/count_by_attributes/count_sub_acct_by_all_fields.sql"),
        query ->
            query
                .addParameter("subAccountId", subAccount.getSubAccountId())
                .addParameter("accountId", subAccount.getAccountId())
                .addParameter("subAccountType", subAccount.getSubAccountType())
                .addParameter("status", subAccount.getStatus())
                .addParameter("validFrom", subAccount.getValidFrom())
                .addParameter("validTo", subAccount.getValidTo())
                .addParameter("cre_dttm", subAccount.getCreationDate())
                .addParameter("ilmArchiveSwitch", "Y")
                .executeScalar(Long.class));
  }

  public static long countWithholdFundsByAttributes(SqlDb db, WithholdFund withholdFund) {
    return db.execQuery(
        "count-withhold-funds-by-attributes",
        resourceAsString("sql/count_by_attributes/count_withhold_funds_by_all_fields.sql"),
        query ->
            query
                .addParameter("accountId", withholdFund.getAccountId())
                .addParameter("withholdFundType", withholdFund.getWithholdFundType())
                .addParameter("withholdFundTarget", withholdFund.getWithholdFundTarget())
                .addParameter("withholdFundPercentage", withholdFund.getWithholdFundPercentage())
                .addParameter("status", withholdFund.getStatus())
                .addParameter("validFrom", withholdFund.getValidFrom())
                .addParameter("validTo", withholdFund.getValidTo())
                .addParameter("cre_dttm", withholdFund.getCreationDate())
                .addParameter("ilmArchiveSwitch", "Y")
                .executeScalar(Long.class));
  }

  public static long countAccountHierarchyByAttributes(
      SqlDb db, AccountHierarchy accountHierarchy, boolean isUpdated) {
    return db.execQuery(
        "count-account-hier-by-attributes",
        resourceAsString("sql/count_by_attributes/count_acct_hier_by_all_fields.sql"),
        query ->
            query
                .addParameter("parent_acct_party_id", accountHierarchy.getParentPartyId())
                .addParameter("child_acct_party_id", accountHierarchy.getChildPartyId())
                .addParameter("status", accountHierarchy.getStatus())
                .addParameter("valid_from", accountHierarchy.getValidFrom())
                .addParameter("valid_to", accountHierarchy.getValidTo())
                .addParameter("cre_dttm", isUpdated ? accountHierarchy.getCreationDate() : null)
                .addParameter("ilm_arch_sw", "Y")
                .addParameter("txn_header_id", accountHierarchy.getTxnHeaderId())
                .executeScalar(Long.class));
  }

  public static long countErrorAccountHierarchyByAttributes(
      SqlDb db, ErrorAccountHierarchy errorAccountHierarchy) {
    return db.execQuery(
        "count-error-account-hier-by-attributes",
        resourceAsString("sql/count_by_attributes/count_error_acct_hier_by_all_fields.sql"),
        query ->
            query
                .addParameter("txn_header_id", errorAccountHierarchy.getTxnHeaderId())
                .addParameter("parent_party_id", errorAccountHierarchy.getParentPartyId())
                .addParameter("child_party_id", errorAccountHierarchy.getChildPartyId())
                .addParameter("currency", errorAccountHierarchy.getCurrency())
                .addParameter("acct_type", errorAccountHierarchy.getAccountType())
                .addParameter("division", errorAccountHierarchy.getDivision())
                .addParameter("ilm_arch_sw", "Y")
                .executeScalar(Long.class));
  }
}
