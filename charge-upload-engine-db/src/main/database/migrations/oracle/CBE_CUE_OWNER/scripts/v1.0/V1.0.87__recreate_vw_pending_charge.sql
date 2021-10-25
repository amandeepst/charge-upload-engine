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
       trim(b.adhoc_sw) as adhoc_sw,
       b.svc_qty,
       trim(b.fast_pay_val) as fast_pay_val,
       b.is_ind_flg,
       trim(b.pay_narrative) as pay_narrative,
       trim(b.rel_waf_flg) as rel_waf_flg,
       trim(b.rel_reserve_flg) as rel_reserve_flg,
       trim(b.case_identifier) as case_identifier,
       b.debt_dt,
       b.source_type,
       b.source_id,
       b.event_id,
       b.bill_period_cd,
       b.charge_amt,
       b.recr_rate,
       b.recr_idfr,
       b.start_dt,
       b.end_dt,
       b.ilm_dt,
       0                                              as retry_count,
       l.long_lcp                                     as lcp,
       b.can_flg,
       acct.accountid                                 as account_id,
       acct.subaccountid                              as sub_account_id,
       r.rec_chg_id,
       b.bill_after_dt
FROM cm_bchg_stg b
         left outer join long_legal_counterparty l
                         on trim(b.cis_division) = trim(l.cis_division)
         left outer join cm_rec_chg r
                         on trim(b.recr_idfr) = trim(r.source_id)
         left outer join vwm_bcu_account acct
                         on trim(b.per_id_nbr)   = trim(acct.partyid) and
                            trim(b.currency_cd)  = trim(acct.currencycode) and
                            trim(b.cis_division) = trim(acct.legalcounterparty) and
                            trim(b.sa_type_cd)   = trim(acct.subaccounttype)
WHERE b.bo_status_cd = 'UPLD';