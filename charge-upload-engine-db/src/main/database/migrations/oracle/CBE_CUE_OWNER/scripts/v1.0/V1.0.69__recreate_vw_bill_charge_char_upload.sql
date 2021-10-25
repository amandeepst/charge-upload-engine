CREATE OR REPLACE VIEW vw_bill_charge_char_upload
AS
SELECT a.billable_chg_id AS billable_chg_id,
  'NON_ZERO'             AS char_type_cd,
  a.end_dt               AS effdt,
  ' '                    AS char_val,
  start_dt               AS ADHOC_CHAR_VAL,
  1                      AS version,
  ' '                    AS char_val_fk1,
  ' '                    AS char_val_fk2,
  ' '                    AS char_val_fk3,
  ' '                    AS char_val_fk4,
  ' '                    AS char_val_fk5,
  ' '                    AS srch_char_val,
  a.ilm_dt               AS ilm_dt
FROM vw_bill_charge_upload a
WHERE EXISTS
  (SELECT 1
  FROM ci_priceitem_char
  WHERE trim(priceitem_cd)=a.priceitem_cd
  AND char_type_cd        ='NON_ZERO'
  );