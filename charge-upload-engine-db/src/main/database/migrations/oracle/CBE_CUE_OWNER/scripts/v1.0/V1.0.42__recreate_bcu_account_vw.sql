CREATE OR REPLACE FORCE EDITIONABLE VIEW VW_BCU_ACCOUNT ("LEGALCOUNTERPARTY", "CURRENCYCODE",
 "SUBACCOUNTTYPE", "BILLCYCLECODE", "PARTYID", "SUBACCOUNTID","ACCOUNTID", "PERSONID") AS

SELECT  sa1.cis_division AS lcp,
        acct1.currency_cd as currency,
        sa1.sa_type_cd      AS sa_type,
        acct1.bill_cyc_cd   AS BILL_CYC_CD,
        per1.per_id_nbr     AS party,
        max(sa1.sa_id)      AS sa_id,
        acct1.acct_id       AS acct_id,
        aper1.per_id AS per_id
FROM ci_sa sa1
         INNER JOIN ci_acct acct1 ON sa1.acct_id=acct1.acct_id
         INNER JOIN ci_acct_per aper1 ON aper1.acct_id=acct1.acct_id
         INNER JOIN ci_per_id per1 ON per1.per_id=aper1.per_id
         INNER JOIN ci_sa_char schar ON sa1.sa_id=schar.sa_id
WHERE  per1.id_type_cd = 'EXPRTYID'
  AND sa1.sa_status_flg IN ('20','30')
  AND sa1.sa_type_cd in ('CHRG','FUND','CHBK','RECR')
  AND schar.char_type_cd = 'SA_ID'
group by sa1.cis_division,acct1.currency_cd,sa1.sa_type_cd,acct1.bill_cyc_cd,
 per1.per_id_nbr,acct1.acct_id,aper1.per_id
