INSERT /*+ :insert-hints */ ALL
INTO cm_b_chg_line (billable_chg_id,line_seq,descr_on_bill,charge_amt,currency_cd,show_on_bill_sw,
app_in_summ_sw,dst_id,version,memo_sw,agg_parm_grp_id,ilm_dt,precs_charge_amt
) VALUES (billable_chg_id,line_seq,descr_on_bill,charge_amt,currency_cd,show_on_bill_sw,
app_in_summ_sw,dst_id,version,memo_sw,agg_parm_grp_id,ilm_dt,precs_charge_amt)

SELECT  /*+ :select-hints */
       bill_item_id                   AS billable_chg_id,
       '1'                            AS line_seq,
       amount                         AS charge_amt,
       currency_cd,
       'Billable Charge Upload'       AS descr_on_bill,
       'Y'                            AS show_on_bill_sw,
       'BASE_CHG  '                   AS dst_id,
       amount                         AS precs_charge_amt,
       'N'                            AS app_in_summ_sw,
       'N'                            AS memo_sw,
       0                              AS agg_parm_grp_id,
       1                              AS version,
       ilm_dt
from cm_misc_bill_item_ln
WHERE batch_code = :batch_code
AND batch_attempt = :batch_attempt
AND ilm_dt = :ilm_dt