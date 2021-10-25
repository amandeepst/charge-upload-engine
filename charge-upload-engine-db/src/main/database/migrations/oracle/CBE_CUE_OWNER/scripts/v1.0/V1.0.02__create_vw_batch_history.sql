CREATE OR REPLACE VIEW vw_batch_history
AS
SELECT batch_code,
       MAX(attempt) AS attempt
FROM batch_history
WHERE state = 'COMPLETED'
GROUP BY batch_code;
