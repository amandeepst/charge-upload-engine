select count(*)
from error_transaction
where trim(txn_header_id) =  :txnHeaderId
  and trim(per_id_nbr) = :perIdNbr
  and trim(sa_type_cd) =  :subAccountType
  and retry_count =  :retryCount
  and trim(reason) =  :reason
  and ((stack_trace IS NULL AND :stackTrace IS NULL) OR (trim(stack_trace) = :stackTrace))
  and created_at IS NOT NULL
  and first_failure_at = :firstFailureAt
  and batch_code =  :batchCode
  and batch_attempt =  :batchAttempt
  and partition_id IS NOT NULL
  and ilm_dt =  :ilmDt
