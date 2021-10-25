CREATE OR REPLACE FORCE EDITIONABLE VIEW VW_BCU_ACCOUNT ("LEGALCOUNTERPARTY", "CURRENCYCODE", "ACCOUNTTYPE",
    "SUBACCOUNTTYPE", "BILLCYCLECODE", "PARTYID", "SUBACCOUNTID","ACCOUNTID", "PERSONID") AS
     SELECT sa1.cis_division AS lcp,
          acct1.currency_cd as currency,
          anbr.acct_nbr       AS acct_type,
          sa1.sa_type_cd      AS sa_type,
          acct1.bill_cyc_cd   AS BILL_CYC_CD,
          per1.per_id_nbr     AS party,
          sa1.sa_id           AS sa_id,
          acct1.acct_id       AS acct_id,
          aper1.per_id AS per_id
     FROM ci_sa sa1,
          ci_acct acct1,
          ci_acct_per aper1,
          ci_per_id per1,
          ci_acct_nbr anbr
     WHERE  sa1.acct_id           = acct1.acct_id
          AND aper1.per_id          = per1.per_id
          AND acct1.acct_id         = aper1.acct_id
          AND acct1.acct_id         = anbr.acct_id
          AND per1.id_type_cd       = 'EXPRTYID'
          AND anbr.acct_nbr_type_cd = 'ACCTTYPE'
          AND sa1.sa_status_flg    <> '70'
          AND anbr.acct_nbr  IN ( 'CHRG', 'FUND', 'CHBK' )
          AND aper1.acct_rel_type_cd <> 'INACTIVE';