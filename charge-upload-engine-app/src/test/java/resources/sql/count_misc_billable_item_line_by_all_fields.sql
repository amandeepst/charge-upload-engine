select count(*)
from cm_misc_bill_item_ln
where trim(BILL_ITEM_LINE_ID) = :billableItemLineId
  and trim(BILL_ITEM_ID) = :billableItemId
  and trim(LINE_CALC_TYPE) = :lineCalculationType
  and trim(CURRENCY_CD) = :currency
  and QTY = :quantity
  and AMOUNT = :lineAmount
  and PRICE = :price
  and CRE_DTTM IS NOT NULL
  and ILM_DT = :ilmDateTime
  and trim(ILM_ARCH_SW) = :ilmArchiveSwitch
  and trim(BATCH_CODE) = :batchCode
  and trim(BATCH_ATTEMPT) = :batchAttempt
  and trim(PARTITION_ID) = :partitionId