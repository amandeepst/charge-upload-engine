CREATE OR REPLACE VIEW VW_RECENT_BATCH_HISTORY
AS
SELECT batch_code, attempt, created_at
FROM batch_history
WHERE state = 'STARTED'
ORDER BY created_at DESC
FETCH FIRST 1 ROWS ONLY;