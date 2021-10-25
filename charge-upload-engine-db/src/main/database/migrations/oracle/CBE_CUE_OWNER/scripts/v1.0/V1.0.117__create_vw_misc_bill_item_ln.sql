CREATE OR REPLACE VIEW vw_misc_bill_item_ln
AS
SELECT
  bill_item_id,
  line_calc_type,
  amount,
  price,
  ilm_dt
from cm_misc_bill_item_ln a
WHERE EXISTS(
              SELECT 1
              FROM vw_batch_history r
              WHERE r.batch_code = a.batch_code
                AND r.attempt = a.batch_attempt
          );