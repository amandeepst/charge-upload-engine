CREATE TABLE pending_billable_charge_error
(
    txn_header_id    CHAR(14)       NOT NULL ENABLE, -- part of the composite primary key from cm_bchg_stg
    sa_type_cd       CHAR(8)        NOT NULL ENABLE, -- part of the composite primary key from cm_bchg_stg
    retry_count      NUMBER(3)      NOT NULL ENABLE,
    reason           VARCHAR2(2048) NOT NULL ENABLE,
    stack_trace      VARCHAR2(4000),
    created_at       TIMESTAMP      NOT NULL ENABLE,
    first_failure_at TIMESTAMP      NOT NULL ENABLE,
    batch_code       VARCHAR2(128)  NOT NULL ENABLE,
    batch_attempt    NUMBER(3, 0)   NOT NULL ENABLE,
    partition_id     INTEGER        NOT NULL ENABLE,
    ilm_dt           TIMESTAMP      NOT NULL ENABLE
);
