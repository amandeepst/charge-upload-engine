CREATE OR REPLACE VIEW vw_bill_charge_upload
AS
SELECT misc_bill_item_id                                      AS billable_chg_id,
       sub_account_id                                         AS sa_id,
       trunc(valid_from)                                      AS start_dt,
       trunc(valid_to)                                        AS end_dt,
       'Billable Charge Upload'                               AS descr_on_bill,
       DECODE(status,'ACTIVE','10','20')                      AS billable_chg_stat,
       NULL                                                   AS recurring_flg,
       NVL(frequency_id,' ')                                  AS bill_period_cd,
       product_id                                             AS priceitem_cd,
       ' '                                                    AS price_asgn_id,
       trunc(accrued_dt)                                      AS bill_after_dt,
       adhoc_bill_flg                                         AS adhoc_bill_sw,
       ' '                                                    AS feed_source_flg,
       cre_dttm                                               AS cre_dt,
       ilm_dt,
       ilm_arch_sw,
       1                                                      AS version,
       ' '                                                    AS chg_type_cd,
       ' '                                                    AS policy_invoice_freq_cd,
       DATE '1900-01-01'                                      AS start_tm,
       DATE '1900-01-01'                                      AS end_tm,
       ' '                                                    AS tou_cd,
       ' '                                                    AS pa_acct_id,
       ' '                                                    AS pa_per_id,
       ' '                                                    AS pa_pricelist_id,
       DECODE(adhoc_bill_flg,'Y','891581313662',' ')    AS grp_ref_val,
       0                                                AS priceitem_parm_grp_id,
       0                                                AS agg_parm_grp_id
FROM cm_misc_bill_item b
WHERE EXISTS(
              SELECT 1
              FROM vw_batch_history r
              WHERE r.batch_code = b.batch_code
                AND r.attempt = b.batch_attempt
          );