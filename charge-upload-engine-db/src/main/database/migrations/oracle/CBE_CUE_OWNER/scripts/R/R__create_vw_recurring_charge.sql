CREATE OR REPLACE VIEW vw_recurring_charge
AS
SELECT
  rec_chg_id,
  txn_header_id,
  product_id,
  lcp,
  division,
  party_id,
  sub_acct,
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
(SELECT 1 FROM vw_batch_history h WHERE r.batch_code = h.batch_code AND r.batch_attempt = h.attempt)

UNION ALL

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
(SELECT 1 FROM vw_recent_batch_history h WHERE r.batch_code = h.batch_code AND r.batch_attempt = h.attempt AND r.ilm_dt=h.created_at);
