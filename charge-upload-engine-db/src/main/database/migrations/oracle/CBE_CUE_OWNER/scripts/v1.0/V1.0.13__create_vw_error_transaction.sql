CREATE OR REPLACE VIEW vw_error_transaction
AS
SELECT txn_header_id,
       sa_type_cd,
       retry_count,
       first_failure_at,
       ilm_dt
FROM error_transaction b
WHERE retry_count <> 999 -- filtering ignored txns
  AND EXISTS(
        SELECT 1
        FROM vw_batch_history h
        WHERE b.batch_code = h.batch_code
          AND b.batch_attempt = h.attempt
    );