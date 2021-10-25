SELECT
    count(*)
FROM
    cm_misc_bill_item
WHERE
    ( party_id = :partyId
      AND lcp = :lcp
      AND sub_acct = :subAccount
      AND currency_cd = :currency
      AND status = :status
      AND product_id = :product
      AND (trim(product_class) =:productCategory OR :productCategory is null )
      AND qty = :quantity
      AND (source_id = :sourceId OR :sourceId is null)
      AND (frequency_id = :frequencyId OR :frequencyId is null)
      AND (cutoff_dt = :cutoffdate OR  :cutoffdate is null)
      AND account_id = :accountId
      AND sub_account_id = :subAccountId
      AND TRIM(txn_header_id) = :txnHeaderId
      AND amount = :amount )