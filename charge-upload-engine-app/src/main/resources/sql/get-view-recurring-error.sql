SELECT /*+ :hints */
 recurringChargeId,
  txnHeaderId,
  productIdentifier ,
  legalCounterParty ,
  division,
  partyId ,
  subAccount,
  frequencyIdentifier ,
  currency ,
  price ,
  quantity ,
  startDate ,
  endDate ,
  status ,
  recurringSourceId ,
  cutoffDate,
  firstFailureAt,
  retryCount,
  ora_hash(txnHeaderId, :partitions) AS partitionId
FROM vw_recurring_error rec_error
                        where rec_error.retryCount<= :max_attempt
                        AND rec_error.ilm_dt > :low
                        AND rec_error.ilm_dt <= :high

