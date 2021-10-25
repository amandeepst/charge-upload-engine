 CREATE OR REPLACE VIEW vw_rec_comp_history AS
    SELECT
        batch_code,
        attempt,
        created_at
    FROM
        batch_history
    WHERE
        state = 'COMPLETED'
    ORDER BY
        created_at DESC
    FETCH FIRST 1 ROWS ONLY;