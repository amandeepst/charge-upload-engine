CREATE INDEX vwm_bcu_account_idx on VWM_BCU_ACCOUNT (PARTYID, CURRENCYCODE, LEGALCOUNTERPARTY, SUBACCOUNTTYPE) PARALLEL;
ALTER INDEX vwm_bcu_account_idx NOPARALLEL;