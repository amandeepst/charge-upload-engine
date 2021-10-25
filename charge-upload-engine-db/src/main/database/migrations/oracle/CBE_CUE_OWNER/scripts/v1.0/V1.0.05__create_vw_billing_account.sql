CREATE VIEW vw_billing_account AS
WITH hier AS
         (SELECT sa1.cis_division AS lcp,
                 anbr.acct_nbr       AS acct_type,
                 sa1.sa_type_cd      AS acct_type,
                 sa1.sa_type_cd      AS sa_type,
                 per1.per_id_nbr     AS parent_party,
                 sa1.sa_id           AS parent_sa,
                 acct1.acct_id       AS parent_acct_id,
                 sa2.sa_id           AS child_sa
          FROM ci_sa sa1,
               ci_sa sa2,
               ci_sa_char sc,
               ci_acct acct1,
               ci_acct_per aper1,
               ci_per_id per1,
               ci_acct_nbr anbr
          WHERE sa2.sa_id                = Trim(sc.char_val_fk1)
            AND sc.char_type_cd       = 'C1_SAFCD'
            AND sc.sa_id              = sa1.sa_id
            AND sa1.acct_id           = acct1.acct_id
            AND aper1.per_id          = per1.per_id
            AND acct1.acct_id         = aper1.acct_id
            AND acct1.acct_id         = anbr.acct_id
            AND per1.id_type_cd       = 'EXPRTYID'
            AND anbr.acct_nbr_type_cd = 'ACCTTYPE'
            AND sa1.sa_status_flg    <> '70'
            AND sa2.sa_status_flg    <> '70'
            AND anbr.acct_nbr        IN ( 'CHRG', 'FUND', 'CHBK' )
         ),
     CHILD AS
         (SELECT sa2.cis_division AS lcp,
                 anbr.acct_nbr       AS acct_type,
                 sa2.sa_type_cd      AS sa_type,
                 sa2.sa_id,
                 acct2.acct_id acct_id,
                 per2.per_id_nbr AS child_party,
                 acct2.currency_cd,
                 acct2.bill_cyc_cd
          FROM ci_sa sa2,
               ci_acct acct2,
               ci_acct_per aper2,
               ci_per_id per2,
               ci_acct_nbr anbr
          WHERE acct2.acct_id            = sa2.acct_id
            AND aper2.per_id          = per2.per_id
            AND acct2.acct_id         = aper2.acct_id
            AND acct2.acct_id         = anbr.acct_id
            AND per2.id_type_cd       = 'EXPRTYID'
            AND anbr.acct_nbr_type_cd = 'ACCTTYPE'
            AND sa2.sa_status_flg    <> '70'
            AND anbr.acct_nbr        IN ( 'CHRG', 'FUND', 'CHBK' )
         )
SELECT b.lcp AS legalCounterparty,
       b.currency_cd AS currencyCode,
       b.acct_type AS accountType,
       b.child_party AS childPartyId,
       b.acct_id AS childAccountId,
       b.sa_id   AS childSubAccountId,
       b.sa_type AS subAccountType,
       b.bill_cyc_cd AS billCycleCode,
       NVL(a.parent_party, b.child_party) AS parentPartyId,
       NVL(a.parent_sa, b.sa_id)          AS parentSubAccountId,
       NVL(A.parent_acct_id, b.acct_id)   AS parentAccountId
FROM hier a,
     child b
WHERE b.sa_id = a.child_sa (+);