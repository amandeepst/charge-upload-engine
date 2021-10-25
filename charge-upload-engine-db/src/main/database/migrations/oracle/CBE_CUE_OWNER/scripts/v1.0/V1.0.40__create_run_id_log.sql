CREATE TABLE batch_seed (
   run_id         NUMBER        GENERATED ALWAYS AS IDENTITY,
   batch_code     VARCHAR2(128) NOT NULL ENABLE,
   batch_attempt  NUMBER(3, 0)  NOT NULL ENABLE,
   created_at     TIMESTAMP     DEFAULT SYSTIMESTAMP NOT NULL
);

ALTER TABLE batch_seed ADD CONSTRAINT batch_seed_pk PRIMARY KEY (run_id);