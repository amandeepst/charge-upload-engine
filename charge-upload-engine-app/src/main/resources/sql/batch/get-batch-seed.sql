SELECT MAX(run_id)
FROM batch_seed
WHERE batch_code = :batch_code
  AND batch_attempt = :batch_attempt
