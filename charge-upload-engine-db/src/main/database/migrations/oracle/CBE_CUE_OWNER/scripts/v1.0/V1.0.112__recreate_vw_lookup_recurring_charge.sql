CREATE OR REPLACE VIEW VW_LOOKUP_RECURRING_CHARGE
AS
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
  FROM vw_recurring_charge rec_chg
INNER JOIN ci_bill_cyc_sch csh ON trim(csh.bill_cyc_cd) = trim(rec_chg.frequency_id)
LEFT OUTER JOIN vw_rec_idfr vw ON trim(vw.source_id) = trim(rec_chg.source_id)
WHERE trim(rec_chg.status) ='ACTIVE';
