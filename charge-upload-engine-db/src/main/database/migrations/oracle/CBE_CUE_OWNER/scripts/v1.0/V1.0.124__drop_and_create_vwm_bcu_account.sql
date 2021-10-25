DROP MATERIALIZED VIEW vwm_bcu_account;

CREATE MATERIALIZED VIEW vwm_bcu_account AS
    SELECT
        legalcounterparty,
        currencycode,
        subaccounttype,
        billcyclecode,
        partyid,
        subaccountid,
        accountid
    FROM
        vw_bcu_account;
