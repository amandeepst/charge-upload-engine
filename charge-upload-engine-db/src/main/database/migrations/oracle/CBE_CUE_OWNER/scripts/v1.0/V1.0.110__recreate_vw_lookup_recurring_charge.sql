CREATE OR REPLACE VIEW VW_LOOKUP_RECURRING_CHARGE
AS
WITH
rec_chg AS(
SELECT * from vw_recurring_charge
UNION
SELECT
  r.rec_chg_id,
  r.txn_header_id,
  r.product_id,
  r.lcp,
  r.division,
  r.party_id,
  r.sub_acct,
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
  rec_chg.frequency_id                                                      AS frequencyIdentifier ,
  rec_chg.currency_cd                                                       AS currency ,
  rec_chg.price                                                             AS price ,
  rec_chg.quantity                                                          AS quantity ,
  rec_chg.valid_from                                                        AS startDate ,
  rec_chg.valid_to                                                          AS endDate ,
  rec_chg.status                                                            AS status ,
  trim(rec_chg.source_id)                                                   AS recurringSourceId ,
  csh.win_end_dt                                                            AS cutoffDate,
  vw.cutoff_dt                                                              AS maxCutoffDate,
  cre_dttm                                                                  AS creationDate
  FROM rec_chg
INNER JOIN ci_bill_cyc_sch csh ON trim(csh.bill_cyc_cd) = trim(rec_chg.frequency_id)
LEFT OUTER JOIN vw_rec_idfr vw ON trim(vw.source_id) = trim(rec_chg.source_id)
WHERE trim(rec_chg.status) ='ACTIVE';
