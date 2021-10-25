CREATE OR REPLACE VIEW vw_misc_bill_item
AS
SELECT
    b.misc_bill_item_id,
    b.sub_account_id,
    b.lcp,
    b.accrued_dt,
    b.adhoc_bill_flg,
    b.currency_cd ,
    b.product_id,
    b.product_class,
    b.qty,
    b.rel_waf_flg,
    b.rel_reserve_flg,
    b.fastest_payment_flg,
    b.case_id,
    b.ind_payment_flg,
    b.pay_narrative,
    b.debt_dt,
    b.source_type,
    b.status,
    b.ilm_dt,
    b.source_id
FROM cm_misc_bill_item b
WHERE EXISTS(
              SELECT 1
              FROM vw_batch_history r
              WHERE r.batch_code = b.batch_code
                AND r.attempt = b.batch_attempt
          );