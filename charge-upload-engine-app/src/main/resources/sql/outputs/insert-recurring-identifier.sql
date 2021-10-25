INSERT /*+ :hints */ INTO CM_REC_IDFR
    (
      SOURCE_ID,
      CUTOFF_DT,
      FREQUENCY_ID,
      CRE_DTTM,
      ILM_DT,
      BATCH_CODE,
      BATCH_ATTEMPT,
      PARTITION_ID
    )
    VALUES
    (
      :sourceId,
      :cutoffDate,
      :frequencyIdentifier,
      SYSTIMESTAMP,
      :ilm_dt,
      :batch_code,
      :batch_attempt,
      :partition_id
    )