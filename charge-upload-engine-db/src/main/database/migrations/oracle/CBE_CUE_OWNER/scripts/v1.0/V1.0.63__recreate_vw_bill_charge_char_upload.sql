CREATE OR REPLACE VIEW vw_bill_charge_char_upload
AS
SELECT
misc_bill_item_id billable_chg_id,
'NON_ZERO' char_type_cd,
trunc(a.valid_from) effdt,
' ' char_val,
trunc(a.valid_from) ADHOC_CHAR_VAL,
1 version,
' ' char_val_fk1,
' ' char_val_fk2,
' ' char_val_fk3,
' ' char_val_fk4,
' ' char_val_fk5,
' ' srch_char_val,
a.ilm_dt
FROM cm_misc_bill_item a
 WHERE EXISTS(SELECT 1 FROM ci_priceitem_char WHERE trim(priceitem_cd)=a.product_id and char_type_cd='NON_ZERO');