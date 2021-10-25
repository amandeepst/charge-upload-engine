CREATE TABLE batch_history
(
    batch_code     VARCHAR2(128) NOT NULL ENABLE,
    attempt        NUMBER(3, 0)  NOT NULL ENABLE,
    state          VARCHAR2(64)  NOT NULL ENABLE,
    watermark_low  TIMESTAMP(9)  NOT NULL ENABLE,
    watermark_high TIMESTAMP(9)  NOT NULL ENABLE,
    comments       CLOB,
    metadata       CLOB,
    created_at     TIMESTAMP(9)  NOT NULL ENABLE,

    CONSTRAINT batch_history_pk PRIMARY KEY (batch_code, attempt, state)
);