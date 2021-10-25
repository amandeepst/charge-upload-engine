SELECT /*+ :hints */
    p.txn_header_id AS txnHeaderId,
    CASE p.sa_type_cd WHEN 'RECR' THEN 'CHRG'
    ELSE p.sa_type_cd END AS subAccountType,
    p.per_id_nbr AS partyId,
    p.cis_division AS division,
    TRIM(l.char_val) AS lcp,
    p.currency_cd AS currency,
    p.priceitem_cd AS productIdentifier,
    p.adhoc_sw AS adhocBillIndicator,
    p.svc_qty AS quantity,
    p.fast_pay_val AS fastestSettlementIndicator,
    p.is_ind_flg AS individualPaymentIndicator,
    p.pay_narrative AS paymentNarrative,
    p.rel_waf_flg AS releaseWafIndicator,
    p.rel_reserve_flg AS releaseReserverIndicator,
    p.case_identifier AS caseIdentifier,
    p.debt_dt AS debtDate,
    p.source_type AS sourceType,
    p.source_id AS sourceId,
    p.bill_period_cd AS frequencyIdentifier,
    p.charge_amt AS price,
    p.recr_rate AS recurringRate,
    p.recr_idfr AS recurringIdentifier,
    p.start_dt AS validFrom,
    p.end_dt AS validTo,
    p.can_flg AS cancellationFlag,
    p.ilm_dt AS ilmDate,
    0 AS retryCount,
    ora_hash(
        p.txn_header_id,
        :partitions
    ) AS partitionId,
    null AS firstFailureAt,
    NULL AS subAccountId,
    NULL AS accountId,
    r.rec_chg_id AS recurringIdentifierForUpdation,
    p.bill_after_dt AS billAfterDate,
    p.event_id AS eventId
FROM
    cm_bchg_stg p
      LEFT OUTER JOIN ci_cis_div_char l ON
        p.cis_division = l.cis_division
    AND
        char_type_cd = 'BOLE'
    LEFT OUTER JOIN vw_recurring_charge r ON
        TRIM(p.recr_idfr) = TRIM(r.source_id)
        where
     p.ilm_dt >:low
    AND
        p.ilm_dt <=:high
        AND
         TRIM(p.bo_status_cd) = 'UPLD'

        UNION ALL

    SELECT /*+ :retry-hints */
    p.txn_header_id AS txnHeaderId,
    p.sa_type_cd AS subAccountType,
    p.per_id_nbr AS partyId,
    p.cis_division AS division,
    TRIM(l.char_val) AS lcp,
    p.currency_cd AS currency,
    p.priceitem_cd AS productIdentifier,
    p.adhoc_sw AS adhocBillIndicator,
    p.svc_qty AS quantity,
    p.fast_pay_val AS fastestSettlementIndicator,
    p.is_ind_flg AS individualPaymentIndicator,
    p.pay_narrative AS paymentNarrative,
    p.rel_waf_flg AS releaseWafIndicator,
    p.rel_reserve_flg AS releaseReserverIndicator,
    p.case_identifier AS caseIdentifier,
    p.debt_dt AS debtDate,
    p.source_type AS sourceType,
    p.source_id AS sourceId,
    p.bill_period_cd AS frequencyIdentifier,
    p.charge_amt AS price,
    p.recr_rate AS recurringRate,
    p.recr_idfr AS recurringIdentifier,
    p.start_dt AS validFrom,
    p.end_dt AS validTo,
    p.can_flg AS cancellationFlag,
    p.ilm_dt AS ilmDate,
    s.retry_count AS retryCount,
    ora_hash(
        p.txn_header_id,
        :partitions
    ) AS partitionId,
    s.first_failure_at AS firstFailureAt,
    NULL AS subAccountId,
    NULL AS accountId,
    null AS recurringIdentifierForUpdation,
    p.bill_after_dt AS billAfterDate,
    p.event_id AS eventId
FROM
    cm_bchg_stg p

    INNER JOIN vw_error_transaction s ON
        p.txn_header_id = s.txn_header_id AND p.sa_type_cd = s.sa_type_cd
    AND
        s.ilm_dt >:low
    AND
        s.ilm_dt <=:high
    AND
        s.retry_count <=:max_attempt
    LEFT OUTER JOIN ci_cis_div_char l ON
        p.cis_division = l.cis_division
    AND
        char_type_cd = 'BOLE'
