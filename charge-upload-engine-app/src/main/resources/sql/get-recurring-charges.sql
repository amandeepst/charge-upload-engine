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
  null as firstFailureAt,
  0 as retryCount,
  ora_hash(txnHeaderId, :partitions) AS partitionId
FROM vw_lookup_recurring_charge
 WHERE
  (CASE
    WHEN maxCutoffDate IS NULL AND TRUNC(creationDate) >= TRUNC(startDate)
    AND cutoffDate       >= TRUNC(creationDate)
    AND cutoffDate       <= TRUNC(CAST(:logical_date AS Date))
   THEN 1
   WHEN maxCutoffDate IS NULL AND TRUNC(creationDate) < TRUNC(startDate)
    AND cutoffDate       >= TRUNC(startDate)
    AND cutoffDate       <= TRUNC(CAST(:logical_date AS Date))
   THEN 1
    WHEN maxCutoffDate IS NOT NULL
    AND cutoffDate       > TRUNC(maxCutoffDate)
    AND cutoffDate       <= TRUNC(CAST(:logical_date AS Date))
    THEN 1
    ELSE 0
  END) = 1
  AND TRUNC(startDate) <= TRUNC(CAST(:logical_date AS Date))
AND endDate >= TRUNC(CAST(:logical_date AS Date))
