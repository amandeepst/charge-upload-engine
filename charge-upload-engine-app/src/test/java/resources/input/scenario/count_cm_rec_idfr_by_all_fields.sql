select count(*) from cm_rec_idfr where
(cutoff_dt=:cutOffDate
and source_id=:sourceId
and frequency_id =:frequencyId
and batch_code = (SELECT batch_code FROM  batch_history WHERE
 state = 'COMPLETED' ORDER BY  created_at DESC FETCH FIRST 1 ROWS ONLY))