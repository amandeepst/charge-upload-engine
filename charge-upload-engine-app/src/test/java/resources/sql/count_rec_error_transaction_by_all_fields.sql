select count(*)
from cm_rec_chg_err
where trim(TXN_HEADER_ID) =  :txnHeaderId
  and trim(PARTY_ID) = :perIdNbr
  and trim(SUB_ACCT) =  :subAccountType
  and RETRY_COUNT =  :retryCount
  and trim(REASON) =  :reason
  and ((STACK_TRACE IS NULL AND :stackTrace IS NULL) OR (trim(STACK_TRACE) = :stackTrace))
  and CREATED_AT IS NOT NULL
  and FIRST_FAILURE_AT = :firstFailureAt
  and BATCH_CODE =  :batchCode
  and BATCH_ATTEMPT =  :batchAttempt
  and PARTITION_ID IS NOT NULL
  and ILM_DT =  :ilmDt
