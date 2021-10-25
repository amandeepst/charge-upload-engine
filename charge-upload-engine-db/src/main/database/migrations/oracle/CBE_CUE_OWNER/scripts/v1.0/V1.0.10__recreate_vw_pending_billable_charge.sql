create or replace view vw_pending_billable_charge
as
with long_legal_counterparty as (
    select cis_division, char_val as long_lcp from CI_CIS_DIV_CHAR where char_type_cd = 'BOLE'
)
SELECT b.txn_header_id,
       b.sa_type_cd,
       b.per_id_nbr,
       b.cis_division,
       b.currency_cd,
       b.priceitem_cd as priceitem,
       b.adhoc_sw,
       b.svc_qty,
       b.fast_pay_val,
       b.is_ind_flg,
       b.pay_narrative,
       b.rel_waf_flg,
       b.rel_reserve_flg,
       b.case_identifier,
       b.debt_dt,
       b.source_type,
       b.source_id,
       r.source_id as source_id_for_filtering,
       b.bill_period_cd,
       b.charge_amt,
       b.recr_rate,
       b.recr_idfr,
       b.start_dt,
       b.ilm_dt,
       0                                              as retry_count,
       l.long_lcp                                     as lcp
FROM cm_bchg_stg b
         left outer join long_legal_counterparty l
                         on trim(b.cis_division) = trim(l.cis_division)
         left outer join cm_rec_chg r
                         on trim(b.recr_idfr) = trim(r.source_id)
WHERE b.bo_status_cd = 'UPLD';
