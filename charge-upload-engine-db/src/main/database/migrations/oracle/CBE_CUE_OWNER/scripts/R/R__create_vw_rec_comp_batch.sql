 CREATE OR REPLACE VIEW vw_rec_comp_batch AS

 with completed_batch AS (
  SELECT created_at, batch_code, attempt
    FROM batch_history
   WHERE state = 'COMPLETED'
   ORDER BY created_at
   DESC  FETCH FIRST 1 ROWS ONLY

)

SELECT
  nvl(b.created_at, c.created_at) AS started_at,
  c.batch_code                    AS batch_code,
  c.attempt                       AS batch_attempt

FROM completed_batch c LEFT JOIN batch_history b
  ON b.batch_code = c.batch_code
 AND b.attempt    = c.attempt
 AND b.state      = 'STARTED';
