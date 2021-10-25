select count(*)
from cm_misc_bill_item
where trim(MISC_BILL_ITEM_ID) = :billableItemId
  and trim(TXN_HEADER_ID) = :txnHeaderId
  and trim(PARTY_ID) = :partyId
   and trim(LCP) = :legalCounterparty
   and trim(DIVISION) = :division
   and trim(SUB_ACCT) = :subAccountType
   and trim(ACCOUNT_ID) = :accountId
   and trim(SUB_ACCOUNT_ID) = :subAccountId
   and trim(CURRENCY_CD) = :currency
  and trim(STATUS) = :status
  and trim(PRODUCT_ID) = :productIdentifier
  and trim(ADHOC_BILL_FLG) = :adhocBillIndicator
  and trim(QTY) = :quantity
  and trim(FASTEST_PAYMENT_FLG) = :fastestSettlementIndicator
  and trim(IND_PAYMENT_FLG) = :individualPaymentIndicator
  and trim(PAY_NARRATIVE) = :paymentNarrative
  and trim(REL_RESERVE_FLG) = :releaseReserverIndicator
  and trim(REL_WAF_FLG) = :releaseWafIndicator
  and trim(CASE_ID) = :caseIdentifier
  and trim(DEBT_DT) = :debtDate
  and trim(SOURCE_TYPE) = :sourceType
  and trim(SOURCE_ID) = :sourceId
  and trim(EVENT_ID) = :eventId
  and trim(FREQUENCY_ID) = :frequencyIdentifier
  and trim(CUTOFF_DT) = :cutOffDate
  and CRE_DTTM IS NOT NULL
  and ILM_DT = :ilmDateTime
  and trim(ILM_ARCH_SW) = :ilmArchiveSwitch
  and trim(BATCH_CODE) = :batchCode
  and trim(BATCH_ATTEMPT) = :batchAttempt
  and trim(PARTITION_ID) = :partitionId
  and accrued_dt = :accruedDate
  and trim(case_flg) = :caseFlag
  and trim(HASH_STRING) = :hashString
