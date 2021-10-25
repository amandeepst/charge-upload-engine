select count(*) from error_transaction where (
trim(txn_header_id)=:txnHeaderId
and trim(sa_type_cd)=:saTypeCode
and retry_count=:retryCount
and trim(per_id_nbr)=:partyId
and (code=:code OR :code is null) and (reason=:reason OR :reason is null)
and batch_code = (SELECT batch_code FROM  batch_history WHERE   state = 'COMPLETED'
ORDER BY  created_at DESC FETCH FIRST 1 ROWS ONLY))