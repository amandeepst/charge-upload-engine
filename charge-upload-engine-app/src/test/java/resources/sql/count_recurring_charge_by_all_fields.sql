select count(*)
from cm_rec_chg
where trim(REC_CHG_ID) = :recurringChargeIdentifier
  and trim(TXN_HEADER_ID) = :txnHeaderId
  and PRODUCT_ID = :productIdentifier
  and LCP = :legalCounterparty
  and trim(DIVISION) = :division
  and PARTY_ID = :partyIdentifier
  and trim(SUB_ACCT) = :subAccountType
  and FREQUENCY_ID = :frequencyIdentifier
  and trim(CURRENCY_CD) = :currency
  and PRICE = :price
  and QUANTITY = :quantity
  and VALID_FROM = :validFrom
  and VALID_TO = :validTo
  and STATUS = :status
  and SOURCE_ID = :sourceId
  and CRE_DTTM IS NOT NULL
  and LAST_UPD_DTTM IS NOT NULL
  and ILM_DT = :ilmDateTime
  and trim(ILM_ARCH_SW) = :ilmArchiveSwitch
  and BATCH_CODE = :batchCode
  and BATCH_ATTEMPT = :batchAttempt
  and PARTITION_ID IS NOT NULL