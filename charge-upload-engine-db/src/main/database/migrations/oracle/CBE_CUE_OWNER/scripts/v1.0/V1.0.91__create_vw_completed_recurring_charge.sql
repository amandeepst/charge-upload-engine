CREATE OR REPLACE VIEW vw_completed_recurring_charge
AS
SELECT
  rec_chg_id,
  txn_header_id,
  product_id,
  lcp,
  division,
  party_id,
  sub_acct,
  account_id,
  sub_account_id,
  frequency_id,
  currency_cd,
  price,
  quantity,
  valid_from,
  valid_to,
  status,
  source_id,
  cre_dttm
FROM  cm_rec_chg r WHERE EXISTS
(SELECT 1 FROM vw_batch_history h WHERE r.batch_code = h.batch_code AND r.batch_attempt = h.attempt AND r.ilm_dt=h.created_at);