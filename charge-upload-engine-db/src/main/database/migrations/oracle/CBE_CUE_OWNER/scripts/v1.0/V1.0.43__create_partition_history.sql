CREATE TABLE partition_history
(
    writer VARCHAR2(128)  NOT NULL ENABLE,
    partition_id NUMBER(8)  NOT NULL ENABLE,
    batch_code VARCHAR2(64) NOT NULL ENABLE,
    batch_attempt NUMBER(3) NOT NULL ENABLE,
    ilm_dt TIMESTAMP        NOT NULL ENABLE,
    cre_dttm TIMESTAMP      NOT NULL ENABLE,

    CONSTRAINT partition_history_pk PRIMARY KEY (writer, partition_id, batch_code, batch_attempt, ilm_dt)
);