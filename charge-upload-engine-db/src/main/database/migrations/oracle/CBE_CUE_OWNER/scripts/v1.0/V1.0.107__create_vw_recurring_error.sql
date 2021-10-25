
CREATE OR REPLACE VIEW vw_recurring_error as
SELECT
  rec_chg_id as recurringChargeId,
  txn_header_id as txnHeaderId,
  product_id as productIdentifier,
  lcp as legalCounterParty,
  division as division,
  party_id as partyId,
  sub_acct as subAccount,
  frequency_id as frequencyIdentifier,
  currency_cd as currency,
  price as price,
  quantity as quantity,
  valid_from as startDate,
  valid_to as endDate,
  status as status,
  source_id as recurringSourceId,
  cutoff_dt cutoffDate,
  first_failure_at as firstFailureAt,
  retry_count as retryCount,
  ilm_dt as ilm_dt
FROM  CM_REC_CHG_ERR b WHERE  retry_count <> 999 -- filtering ignored txns
  AND EXISTS(
        SELECT 1
        FROM vw_batch_history h
        WHERE b.batch_code = h.batch_code
          AND b.batch_attempt = h.attempt
    );