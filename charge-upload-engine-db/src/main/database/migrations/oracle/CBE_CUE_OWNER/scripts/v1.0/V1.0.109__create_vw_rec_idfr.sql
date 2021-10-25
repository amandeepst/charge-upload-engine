create or replace view vw_rec_idfr
AS
SELECT source_id,
cutoff_dt,
ilm_dt,
frequency_id
from cm_rec_idfr r
WHERE EXISTS
(SELECT 1 FROM vw_rec_comp_history h WHERE r.batch_code = h.batch_code AND r.batch_attempt = h.attempt);
