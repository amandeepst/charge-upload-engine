MERGE /*+ :hints */ INTO CM_REC_CHG recr
 USING DUAL
   ON (recr.rec_chg_id = :recurringIdentifierForUpdation)
WHEN MATCHED THEN
  UPDATE
  SET recr.price        = :price,
    recr.quantity       = :quantity,
    recr.valid_from     = :validFrom,
    recr.valid_to       = :validTo,
    recr.status         = :status,
    recr.last_upd_dttm  = SYSTIMESTAMP,
    recr.txn_header_id  = :txnHeaderId
  WHERE recr.rec_chg_id = :recurringIdentifierForUpdation
  WHEN NOT MATCHED THEN
  INSERT
    (
      REC_CHG_ID,
      TXN_HEADER_ID,
      PRODUCT_ID,
      DIVISION,
      LCP,
      PARTY_ID,
      SUB_ACCT,
      FREQUENCY_ID,
      CURRENCY_CD,
      PRICE,
      QUANTITY,
      VALID_FROM,
      VALID_TO,
      STATUS,
      SOURCE_ID,
      CRE_DTTM,
      LAST_UPD_DTTM,
      ILM_DT,
      ILM_ARCH_SW,
      BATCH_CODE,
      BATCH_ATTEMPT,
      PARTITION_ID
    )
    VALUES
    (
      :recurringChargeIdentifier,
      :txnHeaderId,
      :productIdentifier,
      :division,
      :legalCounterparty,
      :partyIdentifier,
      :subAccountType,
      :frequencyIdentifier,
      :currency,
      :price,
      :quantity,
      :validFrom,
      :validTo,
      :status,
      :sourceId,
      SYSTIMESTAMP,
      SYSTIMESTAMP,
      :ilm_dt,
      'Y',
      :batch_code,
      :batch_attempt,
      :partition_id
    )