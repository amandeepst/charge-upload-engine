CREATE OR REPLACE VIEW vw_bill_chg_attributes
AS
SELECT a.misc_bill_item_id AS billable_chg_id,
  a.product_id             AS priceitem_cd,
  a.qty                    AS svc_qty,
  a.adhoc_bill_flg         AS adhoc_sw,
  b.charge_amt             AS charge_amt,
  a.fastest_payment_flg    AS fast_pay_val,
  a.case_id                AS case_identifier,
  a.pay_narrative          AS pay_narrative,
  a.ind_payment_flg        AS is_ind_flg,
  a.rel_reserve_flg        AS rel_reserve_flg,
  a.rel_waf_flg            AS rel_waf_flg,
  a.event_id               AS event_id,
  a.debt_dt                AS debt_dt,
  a.source_type            AS source_type,
  a.source_id              AS source_id
FROM cm_misc_bill_item a
INNER JOIN vw_bill_charge_line_upload b
ON b.billable_chg_id = a.misc_bill_item_id;