INSERT /*+ :insert-hints */ ALL
INTO cm_b_ln_char (BILLABLE_CHG_ID, LINE_SEQ ,CHAR_TYPE_CD,CHAR_VAL,ADHOC_CHAR_VAL,ILM_DT, VERSION
) VALUES (billable_chg_id,line_seq,char_type_cd,char_val,adhoc_char_val,ilm_dt,version)
WITH tbl AS (
        SELECT /*+ :select-hints */
            a.bill_item_id,
            a.line_calc_type,
            a.price,
            a.qty,
            a.ilm_dt
        FROM
            cm_misc_bill_item_ln a
            WHERE a.batch_code = :batch_code
            AND a.batch_attempt = :batch_attempt
            AND a.ilm_dt = :ilm_dt
    ) SELECT /*+ :select-hints */
        bill_item_id AS billable_chg_id,
        1 AS line_seq,
        'BCL-TYPE' AS char_type_cd,
        line_calc_type AS char_val,
        ' ' AS adhoc_char_val,
        1 AS version,
        ilm_dt
      FROM
        tbl b
    UNION ALL
    SELECT /*+ :select-hints */
        bill_item_id AS billable_chg_id,
        1 AS line_seq,
        'DNTRERT ' AS char_type_cd,
        'Y' AS char_val,
        ' ' AS adhoc_char_val,
        1 AS version,
        ilm_dt
    FROM
        tbl b
    UNION ALL
    SELECT /*+ :select-hints */
        bill_item_id AS billable_chg_id,
        1 AS line_seq,
        'TXN_VOL ' AS char_type_cd,
        ' ' AS char_val,
        TO_CHAR(qty) AS adhoc_char_val,
        1 AS version,
        ilm_dt
    FROM
        tbl b
    WHERE
        b.line_calc_type <> 'PI_RECUR'
    UNION ALL
    SELECT /*+ :select-hints */
        bill_item_id AS billable_chg_id,
        1 AS line_seq,
        'RECRRATE' AS char_type_cd,
        ' ' AS char_val,
        TO_CHAR(price) AS adhoc_char_val,
        1 AS version,
        ilm_dt
    FROM
        tbl b
    WHERE
        b.line_calc_type = 'PI_RECUR'
    UNION ALL
    SELECT /*+ :select-hints */
        bill_item_id AS billable_chg_id,
        1 AS line_seq,
        'NUMBER  ' AS char_type_cd,
        ' ' AS char_val,
        TO_CHAR(qty) AS adhoc_char_val,
        1 AS version,
        ilm_dt
    FROM
        tbl b
    WHERE
        b.line_calc_type = 'PI_RECUR'