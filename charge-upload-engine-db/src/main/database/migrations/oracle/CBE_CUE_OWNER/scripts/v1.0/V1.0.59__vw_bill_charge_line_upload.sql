CREATE OR REPLACE VIEW vw_bill_charge_line_upload
AS
SELECT a.bill_item_id                 AS billable_chg_id,
       '1'                            AS line_seq,
       a.amount                       AS charge_amt,
       a.currency_cd,
       'Billable Charge Upload'       AS descr_on_bill,
       'Y'                            AS show_on_bill_sw,
       'BASE_CHG  '                   AS dst_id,
        a.amount                      AS precs_charge_amt,
       'N'                            AS app_in_summ_sw,
       'N'                            AS memo_sw,
       0                              AS agg_parm_grp_id,
       1                              AS version
from cm_misc_bill_item_ln a;
