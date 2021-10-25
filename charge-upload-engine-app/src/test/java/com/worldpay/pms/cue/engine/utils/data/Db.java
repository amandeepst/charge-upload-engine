package com.worldpay.pms.cue.engine.utils.data;

import static com.worldpay.pms.cue.engine.transformations.sources.TransactionSourceTest.timestamp;

import com.worldpay.pms.cue.engine.utils.data.model.TestBillItemRow;
import com.worldpay.pms.cue.engine.utils.data.model.TestPendingBillableChargeRow;
import com.worldpay.pms.cue.engine.utils.data.model.TestRecurringChargeRow;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class Db {

  public void insertCharge(SqlDb db, TestPendingBillableChargeRow row) {
    db.exec(
        "insert-transaction",
        conn ->
            conn.createQuery(
                    " insert into cm_bchg_stg(TXN_HEADER_ID, BO_STATUS_CD, PER_ID_NBR, CIS_DIVISION,"
                        + " SA_TYPE_CD, SVC_QTY, START_DT, END_DT, ILM_DT, ADHOC_SW, RECR_IDFR, PRICEITEM_CD)  Values(:txnHeaderId, :status, :perIdNbr, :divison, "
                        + " :saTypeCode, :serviceQty, :startDate, :endDate, :ilmDate, :adhocSw, :recurringIdentifier, :priceItem)")
                .addParameter("txnHeaderId", row.getTxnHeaderId())
                .addParameter("status", row.getStatus())
                .addParameter("perIdNbr", row.getPerIdNbr())
                .addParameter("divison", row.getDivision())
                .addParameter("saTypeCode", row.getSaTypeCode())
                .addParameter("serviceQty", row.getServiceQty())
                .addParameter("startDate", timestamp(row.getStartDate()))
                .addParameter("endDate", timestamp(row.getEndDate()))
                .addParameter("ilmDate", timestamp(row.getIlmDate()))
                .addParameter("adhocSw", row.getAdhocSw())
                .addParameter("recurringIdentifier", row.getRecurringIdentifier())
                .addParameter("priceItem", row.getPriceItem())
                .executeUpdate());
  }

  public void insertError(
      SqlDb db,
      String txnheaderId,
      String perIdNbr,
      String saTypeCode,
      int retryCount,
      String batchCode,
      int batchAttempt,
      long partition,
      String ilmDate) {
    db.exec(
        "insert-error-transaction",
        conn ->
            conn.createQuery(
                    "insert into error_transaction(txn_header_id, per_id_nbr, sa_type_cd, retry_count, batch_code, batch_attempt"
                        + ", partition_id, ilm_dt) "
                        + "values (:txnheaderId, :saTypeCode, :retryCount, :batchCode, :batchAttempt, :partition, :ilmDate)")
                .addParameter("txnheaderId", txnheaderId)
                .addParameter("perIdNbr", perIdNbr)
                .addParameter("saTypeCode", saTypeCode)
                .addParameter("retryCount", retryCount)
                .addParameter("batchCode", batchCode)
                .addParameter("batchAttempt", batchAttempt)
                .addParameter("partition", partition)
                .addParameter("ilmDate", timestamp(ilmDate))
                .executeUpdate());
  }

  public void insertRecurring(SqlDb db, TestRecurringChargeRow row) {
    db.exec(
        "insert-recurring",
        conn ->
            conn.createQuery(
                    "insert into cm_rec_chg(rec_chg_id,txn_header_id,product_id,lcp,division,party_id,sub_acct,frequency_id,currency_cd,price,quantity,valid_from,valid_to,status,source_id,cre_dttm,last_upd_dttm,ilm_arch_sw,ilm_dt,batch_code,batch_attempt,partition_id) "
                        + "values(:recurringChargeId, :txnHeaderId, :product_id,:lcp, :division,  :partyId, :subAccount, :frequencyId, :currency_cd, :price, :quantity, :valid_from, :valid_to,:status, :source_id, :cre_dttm, :last_upd_dttm, :ilm_arch_sw,:ilm_dt,:batch_code,:batch_attempt,:partition_id)")
                .addParameter("recurringChargeId", row.getRecChargeId())
                .addParameter("txnHeaderId", row.getTxnHeaderId())
                .addParameter("product_id", row.getProductId())
                .addParameter("lcp", row.getLegalCounterParty())
                .addParameter("division", row.getDivision())
                .addParameter("partyId", row.getPartyId())
                .addParameter("subAccount", row.getSubAccount())
                .addParameter("frequencyId", row.getFrequencyId())
                .addParameter("currency_cd", row.getCurrency())
                .addParameter("price", row.getPrice())
                .addParameter("quantity", row.getQuantity())
                .addParameter("valid_from", timestamp(row.getValidFrom()))
                .addParameter("valid_to", timestamp(row.getValidTo()))
                .addParameter("status", row.getStatus())
                .addParameter("source_id", row.getSourceId())
                .addParameter("cre_dttm", timestamp(row.getCreditDttm()))
                .addParameter("last_upd_dttm", timestamp(row.getLastUpateDttm()))
                .addParameter("ilm_arch_sw", row.getIlmArchiveSwitch())
                .addParameter("ilm_dt", timestamp(row.getIlmDate()))
                .addParameter("batch_code", row.getBatchCode())
                .addParameter("batch_attempt", row.getBatchAttempt())
                .addParameter("partition_id", row.getPartitionId())
                .executeUpdate());
  }

  public void insertBillItem(SqlDb db, TestBillItemRow row) {
    db.exec(
        "insert-bill-item",
        conn ->
            conn.createQuery(
                    "insert into cm_misc_bill_item(misc_bill_item_id, party_id, division, sub_acct, account_id, sub_account_id, currency_cd, status ,product_id, qty, source_id,frequency_id,cutoff_dt, cre_dttm, ilm_dttm, ilm_arch_sw) "
                        + "values(:billItemId, :party_id, :division, :subAccount, :acctId,:subAccountId, :currency_cd, :status, :product_id, :qty, :source_id, :frequency_id, :cutoff_dt, :cre_dttm, :ilm_dttm, :ilm_arch_sw )")
                .addParameter("billItemId", row.getMiscBillItemId())
                .addParameter("party_id", row.getPartyId())
                .addParameter("division", row.getDivision())
                .addParameter("subAccount", row.getSubAcct())
                .addParameter("acctId", row.getAcctId())
                .addParameter("subAccountId", row.getSubAccountId())
                .addParameter("currency_cd", row.getCurrency())
                .addParameter("status", row.getStatus())
                .addParameter("product_id", row.getProductId())
                .addParameter("qty", row.getQty())
                .addParameter("source_id", row.getSourceId())
                .addParameter("frequency_id", row.getFrequencyId())
                .addParameter("cutoff_dt", timestamp(row.getCutoffDate()))
                .addParameter("cre_dttm", timestamp(row.getCreatedDttm()))
                .addParameter("ilm_dttm", timestamp(row.getIlmDttm()))
                .addParameter("ilm_arch_sw", row.getIlmArchSw())
                .executeUpdate());
  }
}
