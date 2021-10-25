insert into cm_rec_idfr
(source_id, cutoff_dt, frequency_id, ilm_dt, batch_code, batch_attempt, cre_dttm, partition_id)
with tbl1 as (
select source_id from cm_rec_idfr)
select a.source_id, max(a.cutoff_dt) cutoff_dt, a.frequency_id, b.started_at ilm_dt,
b.batch_code batch_code, b.batch_attempt batch_attempt, max(a.cre_dttm) cre_dttm, 1 partition_id
from cm_misc_bill_item a, vw_rec_comp_batch b where source_type = 'REC_CHG' and source_id not in (select source_id from tbl1)
group by a.source_id, a.frequency_id, b.started_at, b.batch_code, b.batch_attempt, 1