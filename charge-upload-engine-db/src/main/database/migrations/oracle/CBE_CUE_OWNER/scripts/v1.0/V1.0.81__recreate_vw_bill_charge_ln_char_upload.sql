CREATE OR REPLACE FORCE EDITIONABLE VIEW VW_BILL_CHARGE_LN_CHAR_UPLOAD (
    "BILLABLE_CHG_ID",
    "LINE_SEQ",
    "CHAR_TYPE_CD",
    "CHAR_VAL",
    "ADHOC_CHAR_VAL",
    "VERSION",
    "BATCH_CODE",
    "BATCH_ATTEMPT",
    "ILM_DT"
) AS
    WITH tbl AS (
        SELECT
            a.bill_item_id,
            a.line_calc_type,
            a.price,
            b.qty,
            a.batch_code,
            a.batch_attempt,
            a.ilm_dt
        FROM
            cm_misc_bill_item_ln a INNER JOIN
            cm_misc_bill_item b ON a.bill_item_id = b.misc_bill_item_id
    ) SELECT
        bill_item_id AS billable_chg_id,
        1 AS line_seq,
        'BCL-TYPE' AS char_type_cd,
        line_calc_type AS char_val,
        ' ' AS adhoc_char_val,
        1 AS version,
        batch_code,
        batch_attempt,
        ilm_dt
      FROM
        tbl b
    UNION ALL
    SELECT
        bill_item_id AS billable_chg_id,
        1 AS line_seq,
        'DNTRERT ' AS char_type_cd,
        'Y' AS char_val,
        ' ' AS adhoc_char_val,
        1 AS version,
        batch_code,
        batch_attempt,
        ilm_dt
    FROM
        tbl b
    UNION ALL
    SELECT
        bill_item_id AS billable_chg_id,
        1 AS line_seq,
        'TXN_VOL ' AS char_type_cd,
        ' ' AS char_val,
        TO_CHAR(qty) AS adhoc_char_val,
        1 AS version,
        batch_code,
        batch_attempt,
        ilm_dt
    FROM
        tbl b
    WHERE
        b.line_calc_type <> 'PI_RECUR'
    UNION ALL
    SELECT
        bill_item_id AS billable_chg_id,
        1 AS line_seq,
        'RECRRATE' AS char_type_cd,
        ' ' AS char_val,
        TO_CHAR(price) AS adhoc_char_val,
        1 AS version,
        batch_code,
        batch_attempt,
        ilm_dt
    FROM
        tbl b
    WHERE
        b.line_calc_type = 'PI_RECUR'
    UNION ALL
    SELECT
        bill_item_id AS billable_chg_id,
        1 AS line_seq,
        'NUMBER  ' AS char_type_cd,
        ' ' AS char_val,
        TO_CHAR(qty) AS adhoc_char_val,
        1 AS version,
        batch_code,
        batch_attempt,
        ilm_dt
    FROM
        tbl b
    WHERE
        b.line_calc_type = 'PI_RECUR';