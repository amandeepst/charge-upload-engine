CREATE MATERIALIZED VIEW vwm_bcu_account AS
    SELECT
        legalcounterparty,
        currencycode,
        subaccounttype,
        billcyclecode,
        partyid,
        subaccountid,
        accountid,
        personid
    FROM
        vw_bcu_account;