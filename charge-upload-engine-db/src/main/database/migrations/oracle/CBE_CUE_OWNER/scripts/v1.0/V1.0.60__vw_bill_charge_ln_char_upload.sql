CREATE OR REPLACE VIEW vw_bill_charge_ln_char_upload
AS
SELECT bill_item_id   as billable_chg_id,
       1            as line_seq,
       'BCL-TYPE'     as char_type_cd,
       LINE_CALC_TYPE as char_val,
       ' '            as adhoc_char_val,
       1              AS version
FROM cm_misc_bill_item_ln b;
