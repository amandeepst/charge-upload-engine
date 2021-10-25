CREATE TABLE CM_REC_CHG (
    "REC_CHG_ID"       VARCHAR2(36 BYTE)    NOT NULL ENABLE,
    "PRODUCT_ID"       VARCHAR2(30 BYTE)    NOT NULL ENABLE,
    "LCP"              VARCHAR2(30 BYTE)    NOT NULL ENABLE,
    "PARTY_ID"         VARCHAR2(30 BYTE)    NOT NULL ENABLE,
    "FREQUENCY_ID"     VARCHAR2(15 BYTE)    NOT NULL ENABLE,
    "CURRENCY_CD"      CHAR(3 BYTE)         NOT NULL ENABLE,
    "PRICE"            NUMBER(36, 18)       NOT NULL ENABLE,
    "QUANTITY"         NUMBER(36, 0)        NOT NULL ENABLE,
    "VALID_FROM"       DATE                 NOT NULL ENABLE,
    "VALID_TO"         DATE,
    "STATUS"           VARCHAR2(15 BYTE)    NOT NULL ENABLE,
    "SOURCE_ID"        VARCHAR2(15 BYTE)    NOT NULL ENABLE,
    "CRE_DTTM"         TIMESTAMP            NOT NULL ENABLE,
    "LAST_UPD_DTTM"    TIMESTAMP            NOT NULL ENABLE,
    "BATCH_CODE"       VARCHAR2(128 BYTE)   NOT NULL ENABLE,
    "BATCH_ATTEMPT"    NUMBER(3, 0)         NOT NULL ENABLE,
    "PARTITION_ID"     NUMBER(*, 0)         NOT NULL ENABLE,
    "ILM_DT"           TIMESTAMP            NOT NULL ENABLE,
    "ILM_ARCH_SW"      CHAR(1 BYTE)         NOT NULL ENABLE,
    "DIVISION"         CHAR(5 BYTE)         NOT NULL ENABLE,
    "SUB_ACCT"         VARCHAR2(15 BYTE)    NOT NULL ENABLE,
    "ACCOUNT_ID"       CHAR(10 BYTE)        NOT NULL ENABLE,
    "SUB_ACCOUNT_ID"   CHAR(10 BYTE)        NOT NULL ENABLE,
    "TXN_HEADER_ID"    CHAR(14 BYTE)        NOT NULL ENABLE,
	"PARTITION"        NUMBER(5,0)          GENERATED ALWAYS AS (ORA_HASH(SOURCE_ID, 15)) VIRTUAL,
	CONSTRAINT "CM_REC_CHG_PK" PRIMARY KEY ("REC_CHG_ID")
)
TABLESPACE CBE_CUE_DATA
    PARTITION BY RANGE (ilm_dt) INTERVAL(NUMTODSINTERVAL(1, 'DAY'))
SUBPARTITION BY LIST(PARTITION)
SUBPARTITION TEMPLATE(
    SUBPARTITION p1 VALUES(0,1,2,3),
    SUBPARTITION p2 VALUES(4,5,6,7),
    SUBPARTITION p3 VALUES(8,9,10,11),
    SUBPARTITION p4 VALUES(12,13,14,15)
)
(PARTITION before_2020 VALUES LESS THAN (TO_DATE('01-JAN-2020', 'dd-MON-yyyy')))
ENABLE ROW MOVEMENT;

CREATE TABLE CM_REC_CHG_AUDIT (
    "REC_CHG_ID"       VARCHAR2(36 BYTE)    NOT NULL ENABLE,
    "PRODUCT_ID"       VARCHAR2(30 BYTE)    NOT NULL ENABLE,
    "LCP"              VARCHAR2(30 BYTE)    NOT NULL ENABLE,
    "PARTY_ID"         VARCHAR2(30 BYTE)    NOT NULL ENABLE,
    "FREQUENCY_ID"     VARCHAR2(15 BYTE)    NOT NULL ENABLE,
    "CURRENCY_CD"      CHAR(3 BYTE)         NOT NULL ENABLE,
    "PRICE"            NUMBER(36, 18)       NOT NULL ENABLE,
    "QUANTITY"         NUMBER(36, 0)        NOT NULL ENABLE,
    "VALID_FROM"       DATE                 NOT NULL ENABLE,
    "VALID_TO"         DATE,
    "STATUS"           VARCHAR2(15 BYTE)    NOT NULL ENABLE,
    "SOURCE_ID"        VARCHAR2(15 BYTE)    NOT NULL ENABLE,
    "CRE_DTTM"         TIMESTAMP            NOT NULL ENABLE,
    "LAST_UPD_DTTM"    TIMESTAMP            NOT NULL ENABLE,
    "BATCH_CODE"       VARCHAR2(128 BYTE)   NOT NULL ENABLE,
    "BATCH_ATTEMPT"    NUMBER(3, 0)         NOT NULL ENABLE,
    "PARTITION_ID"     NUMBER(*, 0)         NOT NULL ENABLE,
    "ILM_DT"           TIMESTAMP            NOT NULL ENABLE,
    "ILM_ARCH_SW"      CHAR(1 BYTE)         NOT NULL ENABLE,
    "DIVISION"         CHAR(5 BYTE)         NOT NULL ENABLE,
    "SUB_ACCT"         VARCHAR2(15 BYTE)    NOT NULL ENABLE,
    "ACCOUNT_ID"       CHAR(10 BYTE)        NOT NULL ENABLE,
    "SUB_ACCOUNT_ID"   CHAR(10 BYTE)        NOT NULL ENABLE,
    "TXN_HEADER_ID"    CHAR(14 BYTE)        NOT NULL ENABLE,
	CONSTRAINT "CM_REC_CHG_PK_AUDIT" PRIMARY KEY ("REC_CHG_ID")
)
TABLESPACE CBE_CUE_DATA
    PARTITION BY RANGE (ilm_dt) INTERVAL(NUMTODSINTERVAL(1, 'DAY'))
SUBPARTITION BY hash(batch_code, batch_attempt, partition_id) SUBPARTITIONS 16 (
  PARTITION before_2020 VALUES LESS THAN (TO_DATE('01-JAN-2020', 'dd-MON-yyyy'))
);

CREATE TABLE CM_MISC_BILL_ITEM (
    "MISC_BILL_ITEM_ID"     CHAR(12 BYTE)                           NOT NULL ENABLE,
    "PARTY_ID"              VARCHAR2(30 BYTE)                       NOT NULL ENABLE,
    "LCP"                   VARCHAR2(30 BYTE)                       NOT NULL ENABLE,
    "SUB_ACCT"              VARCHAR2(15 BYTE)                       NOT NULL ENABLE,
    "CURRENCY_CD"           CHAR(3 BYTE)                            NOT NULL ENABLE,
    "STATUS"                VARCHAR2(15 BYTE)                       NOT NULL ENABLE,
    "PRODUCT_ID"            VARCHAR2(30 BYTE)                       NOT NULL ENABLE,
    "ADHOC_BILL_FLG"        CHAR(1 BYTE) DEFAULT ON NULL 'N'        NOT NULL ENABLE,
    "QTY"                   NUMBER(36, 0)                           NOT NULL ENABLE,
    "FASTEST_PAYMENT_FLG"   CHAR(1 BYTE) DEFAULT ON NULL 'N'        NOT NULL ENABLE,
    "IND_PAYMENT_FLG"       CHAR(1 BYTE) DEFAULT ON NULL 'N'        NOT NULL ENABLE,
    "PAY_NARRATIVE"         VARCHAR2(18 BYTE) DEFAULT ON NULL 'N'   NOT NULL ENABLE,
    "REL_RESERVE_FLG"       CHAR(1 BYTE) DEFAULT ON NULL 'N'        NOT NULL ENABLE,
    "REL_WAF_FLG"           CHAR(1 BYTE) DEFAULT ON NULL 'N'        NOT NULL ENABLE,
    "CASE_ID"               VARCHAR2(15 BYTE),
    "DEBT_DT"               DATE,
    "SOURCE_TYPE"           VARCHAR2(30 BYTE),
    "SOURCE_ID"             VARCHAR2(15 BYTE),
    "FREQUENCY_ID"          VARCHAR2(15 BYTE),
    "CUTOFF_DT"             DATE,
    "VALID_FROM"            DATE                                    NOT NULL ENABLE,
    "VALID_TO"              DATE,
    "CRE_DTTM"              TIMESTAMP                               NOT NULL ENABLE,
    "BATCH_CODE"            VARCHAR2(128 BYTE)                      NOT NULL ENABLE,
    "BATCH_ATTEMPT"         NUMBER(3, 0)                            NOT NULL ENABLE,
    "PARTITION_ID"          NUMBER(*, 0)                            NOT NULL ENABLE,
    "ILM_DT"                TIMESTAMP                               NOT NULL ENABLE,
    "ILM_ARCH_SW"           CHAR(1 BYTE)                            NOT NULL ENABLE,
    "DIVISION"              CHAR(5 BYTE)                            NOT NULL ENABLE,
    "ACCOUNT_ID"            CHAR(10 BYTE)                           NOT NULL ENABLE,
    "SUB_ACCOUNT_ID"        CHAR(10 BYTE)                           NOT NULL ENABLE,
    "TXN_HEADER_ID"         CHAR(14 BYTE)                           NOT NULL ENABLE,
    "BILLABLE_ITEM_ID"      VARCHAR2(36 BYTE)                       NOT NULL ENABLE,
    "ACCRUED_DT"            DATE                                    NOT NULL ENABLE,
    "EVENT_ID"              VARCHAR2(60 BYTE),
    CONSTRAINT "MISC_BILL_ITEM_PK" PRIMARY KEY ( "MISC_BILL_ITEM_ID" )
)
TABLESPACE CBE_CUE_DATA
    PARTITION BY RANGE (ilm_dt) INTERVAL(NUMTODSINTERVAL(1, 'DAY'))
SUBPARTITION BY hash(sub_acct, source_id) SUBPARTITIONS 16
(PARTITION before_2020 VALUES LESS THAN (TO_DATE('01-JAN-2020', 'dd-MON-yyyy')));

CREATE TABLE CM_MISC_BILL_ITEM_LN (
    "BILL_ITEM_LINE_ID"   VARCHAR2(36 BYTE)         NOT NULL ENABLE,
    "BILL_ITEM_ID"        CHAR(12 BYTE)             NOT NULL ENABLE,
    "LINE_CALC_TYPE"      VARCHAR2(8 BYTE)          NOT NULL ENABLE,
    "AMOUNT"              NUMBER(36, 18)            NOT NULL ENABLE,
    "CRE_DTTM"            TIMESTAMP                 NOT NULL ENABLE,
    "BATCH_CODE"          VARCHAR2(128 BYTE)        NOT NULL ENABLE,
    "BATCH_ATTEMPT"       NUMBER(3, 0)              NOT NULL ENABLE,
    "PARTITION_ID"        NUMBER(*, 0)              NOT NULL ENABLE,
    "ILM_DT"              TIMESTAMP                 NOT NULL ENABLE,
    "ILM_ARCH_SW"         CHAR(1 BYTE)              NOT NULL ENABLE,
    "BILLABLE_ITEM_ID"    VARCHAR2(36 BYTE)         NOT NULL ENABLE,
    "PRICE"               NUMBER(36, 18)            NOT NULL ENABLE,
    "CURRENCY_CD"         CHAR(3 BYTE)              NOT NULL ENABLE,
    CONSTRAINT "MISC_BILL_ITEM_LN_PK" PRIMARY KEY ( "BILL_ITEM_LINE_ID" )
)
TABLESPACE CBE_CUE_DATA
    PARTITION BY RANGE (ilm_dt) INTERVAL(NUMTODSINTERVAL(1, 'DAY'))
SUBPARTITION BY hash(batch_code, batch_attempt, partition_id) SUBPARTITIONS 16 (
  PARTITION before_2020 VALUES LESS THAN (TO_DATE('01-JAN-2020', 'dd-MON-yyyy'))
);