   CREATE OR REPLACE FORCE EDITIONABLE VIEW VW_BCU_ACCOUNT ("LEGALCOUNTERPARTY", "CURRENCYCODE", "SUBACCOUNTTYPE", "BILLCYCLECODE", "PARTYID", "SUBACCOUNTID", "ACCOUNTID") AS
 SELECT  substr(TRIM(acct1.lcp),-5) AS lcp,
         acct1.currency_cd AS currency,
         sa1.sub_acct_type      AS sa_type,
         acct1.bill_cyc_id   AS bill_cyc_cd,
         acct1.party_id     AS party,
         sa1.sub_acct_id      AS sa_id,
         acct1.acct_id       AS acct_id
 FROM vw_sub_acct sa1
          INNER JOIN vw_acct acct1 ON sa1.acct_id=acct1.acct_id
 WHERE
 (sa1.valid_to  IS NULL OR sa1.valid_to > TRUNC(SYSDATE))
 GROUP BY  substr(TRIM(acct1.lcp),-5),acct1.currency_cd,sa1.sub_acct_type,acct1.bill_cyc_id,
 acct1.party_id,sa1.sub_acct_id,acct1.acct_id;