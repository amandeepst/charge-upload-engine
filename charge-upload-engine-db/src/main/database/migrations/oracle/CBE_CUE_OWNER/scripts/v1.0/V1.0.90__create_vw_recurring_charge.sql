CREATE OR REPLACE VIEW VW_RECURRING_CHARGE
AS
WITH max_cutoff_date AS (
SELECT MAX(m.CUTOFF_DT) cutoff_date,m.SOURCE_ID FROM CM_MISC_BILL_ITEM m WHERE trim(m.SUB_ACCT)='RECR' AND
EXISTS(SELECT 1 FROM vw_batch_history h WHERE m.batch_code = h.batch_code AND m.batch_attempt = h.attempt AND m.ilm_dt=h.created_at)
GROUP BY source_id),
rec_chg AS(
SELECT
  r.rec_chg_id,
  r.txn_header_id,
  r.product_id,
  r.lcp,
  r.division,
  r.party_id,
  r.sub_acct,
  r.account_id,
  r.sub_account_id,
  r.frequency_id,
  r.currency_cd,
  r.price,
  r.quantity,
  r.valid_from,
  r.valid_to,
  r.status,
  r.source_id,
  r.cre_dttm
FROM  cm_rec_chg r WHERE EXISTS
(SELECT 1 FROM vw_batch_history h WHERE r.batch_code = h.batch_code AND r.batch_attempt = h.attempt AND r.ilm_dt=h.created_at)
UNION
SELECT
  r.rec_chg_id,
  r.txn_header_id,
  r.product_id,
  r.lcp,
  r.division,
  r.party_id,
  r.sub_acct,
  r.account_id,
  r.sub_account_id,
  r.frequency_id,
  r.currency_cd,
  r.price,
  r.quantity,
  r.valid_from,
  r.valid_to,
  r.status,
  r.source_id,
  r.cre_dttm
FROM  cm_rec_chg r WHERE EXISTS
(SELECT 1 FROM vw_recent_batch_history h WHERE r.batch_code = h.batch_code AND r.batch_attempt = h.attempt AND r.ilm_dt=h.created_at)
)
SELECT
  rec_chg.rec_chg_id                                                        AS recurringChargeId,
  rec_chg.txn_header_id                                                     AS txnHeaderId,
  rec_chg.product_id                                                        AS productIdentifier ,
  rec_chg.lcp                                                               AS legalCounterParty ,
  rec_chg.division                                                          AS division,
  rec_chg.party_id                                                          AS partyId ,
  rec_chg.sub_acct                                                          AS subAccount,
  rec_chg.account_id                                                        AS accountId,
  rec_chg.sub_account_id                                                    AS subAccountId,
  rec_chg.frequency_id                                                      AS frequencyIdentifier ,
  rec_chg.currency_cd                                                       AS currency ,
  rec_chg.price                                                             AS price ,
  rec_chg.quantity                                                          AS quantity ,
  rec_chg.valid_from                                                        AS startDate ,
  rec_chg.valid_to                                                          AS endDate ,
  rec_chg.status                                                            AS status ,
  rec_chg.source_id                                                         AS recurringSourceId ,
  csh.win_end_dt                                                            AS cutoffDate,
  max_cutoff_date.cutoff_date                                               AS maxCutoffDate,
  cre_dttm                                                                  AS creationDate
  FROM rec_chg
INNER JOIN ci_bill_cyc_sch csh ON csh.bill_cyc_cd = rec_chg.frequency_id
LEFT OUTER JOIN max_cutoff_date ON max_cutoff_date.source_id = rec_chg.source_id
AND rec_chg.status ='ACTIVE';