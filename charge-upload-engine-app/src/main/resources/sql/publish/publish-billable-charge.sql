INSERT /*+ :insert-hints */ ALL
WHEN 1=1 THEN
INTO cm_bill_chg (
billable_chg_id, sa_id,start_dt,end_dt,descr_on_bill,billable_chg_stat,version,chg_type_cd,recurring_flg,bill_period_cd,
policy_invoice_freq_cd,start_tm,end_tm,priceitem_cd,tou_cd,price_asgn_id,pa_acct_id,pa_per_id,pa_pricelist_id,grp_ref_val,bill_after_dt,
adhoc_bill_sw,feed_source_flg,priceitem_parm_grp_id,cre_dt,agg_parm_grp_id,ilm_dt,ilm_arch_sw
) VALUES (billable_chg_id,sa_id,start_dt,end_dt,descr_on_bill,billable_chg_stat,version,chg_type_cd,recurring_flg,bill_period_cd,
policy_invoice_freq_cd,start_tm,end_tm,priceitem_cd,tou_cd,price_asgn_id,pa_acct_id,pa_per_id,pa_pricelist_id,grp_ref_val,bill_after_dt,
adhoc_bill_sw,feed_source_flg,priceitem_parm_grp_id,cre_dt,agg_parm_grp_id,ilm_dt,ilm_arch_sw)
WHEN (SOURCE_TYPE!='REC_CHG' or SOURCE_TYPE IS NULL) THEN
INTO cm_bchg_attributes_map (
billable_chg_id,priceitem_cd,svc_qty,adhoc_sw,charge_amt,fast_pay_val,case_identifier,pay_narrative,is_ind_flg,
rel_reserve_flg,rel_waf_flg,event_id,debt_dt,source_type,source_id,granularity_hash
) VALUES (billable_chg_id,priceitem_cd,svc_qty,adhoc_sw,charge_amt,fast_pay_val,case_identifier,
pay_narrative,is_ind_flg,rel_reserve_flg,rel_waf_flg,event_id,debt_dt,source_type,source_id,granularity_hash
)
SELECT /*+ :select-hints */
               a.misc_bill_item_id                                      AS billable_chg_id,
               a.sub_account_id                                         AS sa_id,
               trunc(a.accrued_dt)                                      AS start_dt,
               trunc(a.accrued_dt)                                        AS end_dt,
               'Billable Charge Upload'                                 AS descr_on_bill,
               DECODE(a.status,'ACTIVE','10','20')                      AS billable_chg_stat,
               NULL                                                     AS recurring_flg,
               NVL(a.frequency_id,' ')                                  AS bill_period_cd,
               a.product_id                                             AS priceitem_cd,
               ' '                                                      AS price_asgn_id,
               trunc(a.accrued_dt)                                      AS bill_after_dt,
               a.adhoc_bill_flg                                         AS adhoc_bill_sw,
               ' '                                                      AS feed_source_flg,
               a.cre_dttm                                               AS cre_dt,
               a.ilm_dt,
               a.ilm_arch_sw,
               1                                                        AS version,
               ' '                                                      AS chg_type_cd,
               ' '                                                      AS policy_invoice_freq_cd,
               DATE '1900-01-01'                                        AS start_tm,
               DATE '1900-01-01'                                        AS end_tm,
               ' '                                                      AS tou_cd,
               ' '                                                      AS pa_acct_id,
               ' '                                                      AS pa_per_id,
               ' '                                                      AS pa_pricelist_id,
               DECODE(a.adhoc_bill_flg,'Y','891581313662',' ')          AS grp_ref_val,
               0                                                        AS priceitem_parm_grp_id,
               0                                                        AS agg_parm_grp_id,
               a.qty                                                    AS svc_qty,
               a.adhoc_bill_flg                                         AS adhoc_sw,
               a.amount                                                 AS charge_amt,
               a.fastest_payment_flg                                    AS fast_pay_val,
               a.case_flg                                               AS case_identifier,
               a.pay_narrative                                          AS pay_narrative,
               a.ind_payment_flg                                        AS is_ind_flg,
               a.rel_reserve_flg                                        AS rel_reserve_flg,
               a.rel_waf_flg                                            AS rel_waf_flg,
               a.event_id                                               AS event_id,
               a.debt_dt                                                AS debt_dt,
               a.source_type                                            AS source_type,
               a.source_id                                              AS source_id,
               a.sub_acct,
               CASE WHEN a.adhoc_bill_flg='N' AND trim(a.pay_narrative)='N' AND a.rel_reserve_flg='N'
               AND a.rel_waf_flg='N' AND a.fastest_payment_flg='N' AND a.case_flg='N' THEN 0
               ELSE ORA_HASH(a.hash_string)
               END granularity_hash

        FROM cm_misc_bill_item a
        WHERE a.batch_code = :batch_code
        AND a.batch_attempt = :batch_attempt
        AND a.ilm_dt = :ilm_dt