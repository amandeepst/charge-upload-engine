INSERT /*+ :insert-hints */ ALL
INTO cm_bill_chg_char (billable_chg_id, char_type_cd, effdt,char_val, adhoc_char_val, version,char_val_fk1,
char_val_fk2,char_val_fk3, char_val_fk4, char_val_fk5, srch_char_val,ilm_dt
) VALUES (billable_chg_id,char_type_cd,effdt,char_val,adhoc_char_val,version,char_val_fk1,char_val_fk2,
char_val_fk3,char_val_fk4,char_val_fk5,srch_char_val,ilm_dt
)
SELECT  /*+ :select-hints */
     misc_bill_item_id      AS billable_chg_id,
     'NON_ZERO'             AS char_type_cd,
     accrued_dt               AS effdt,
     ' '                    AS char_val,
     accrued_dt             AS ADHOC_CHAR_VAL,
     1                      AS version,
     ' '                    AS char_val_fk1,
     ' '                    AS char_val_fk2,
     ' '                    AS char_val_fk3,
     ' '                    AS char_val_fk4,
     ' '                    AS char_val_fk5,
     ' '                    AS srch_char_val,
     ilm_dt
FROM cm_misc_bill_item
WHERE EXISTS
  (SELECT 1
  FROM ci_priceitem_char
  WHERE trim(priceitem_cd) = product_id
  AND char_type_cd        ='NON_ZERO'
  )
AND batch_code = :batch_code
AND batch_attempt = :batch_attempt
AND ilm_dt = :ilm_dt