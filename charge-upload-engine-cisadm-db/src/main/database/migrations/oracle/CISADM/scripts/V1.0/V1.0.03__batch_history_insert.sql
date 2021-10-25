INSERT INTO CBE_CUE_OWNER.batch_history (batch_code, attempt, state, watermark_low, watermark_high, comments, metadata, created_at)
SELECT 'MIGRATION', 1, 'COMPLETED', TO_DATE('01-JAN-20', 'DD-MON-YY'), SYSTIMESTAMP, 'Baseline Migration', '{}', SYSTIMESTAMP FROM DUAL
WHERE NOT EXISTS(SELECT 1 from cbe_cue_owner.batch_history);
