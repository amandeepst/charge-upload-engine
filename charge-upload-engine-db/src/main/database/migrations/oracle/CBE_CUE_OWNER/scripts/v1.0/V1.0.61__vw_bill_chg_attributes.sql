CREATE OR REPLACE VIEW vw_bill_chg_attributes
AS
select
a.misc_bill_item_id billable_chg_id,
a.product_id priceitem_cd,
a.qty svc_qty,
a.adhoc_bill_flg adhoc_sw,
b.amount charge_amt,
a.fastest_payment_flg fast_pay_val,
a.case_id case_identifier,
a.pay_narrative pay_narrative,
a.ind_payment_flg is_ind_flg,
a.rel_reserve_flg rel_reserve_flg,
a.rel_waf_flg rel_waf_flg,
a.event_id,
a.debt_dt,
a.source_type,
DECODE(sub_acct,'RECR',null,a.source_id)  source_id
from cm_misc_bill_item a, cm_misc_bill_item_ln b
where b.bill_item_id = a.misc_bill_item_id;