select /*+ :hints */
trim(acct.partyid) as partyId,
trim(acct.currencycode) as currencyCode,
trim(acct.accountid)  as accountId,
trim(acct.subaccountid) as subaccountId,
trim(acct.legalcounterparty) as legalcounterparty,
trim(acct.subaccounttype) as subaccountType,
ora_hash(acct.partyid, :partitions) AS partitionId
from
vwm_bcu_account acct