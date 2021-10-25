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
       b.priceitem_cd as priceitem_original,
       p.priceitem_cd as priceitem_for_validation,
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
       b.bill_period_cd,
       b.charge_amt,
       b.recr_rate,
       b.start_dt,
       b.ilm_dt,
       0                                              as retry_count,
       l.long_lcp                                     as lcp,
       nvl(a.childsubaccountid, a.parentsubaccountid) as account
FROM cm_bchg_stg b
         left outer join long_legal_counterparty l
                         on trim(b.cis_division) = trim(l.cis_division)
         left outer join ci_priceitem p
                         on trim(b.priceitem_cd) = trim(p.priceitem_cd)
         left outer join vw_billing_account a
                         on trim(b.currency_cd) = trim(a.currencycode)
                             and trim(b.cis_division) = trim(a.legalcounterparty)
                             and trim(b.sa_type_cd) = trim(a.subaccounttype)
                             and trim(b.per_id_nbr) = trim(a.childpartyid)
WHERE b.bo_status_cd = 'UPLD';
