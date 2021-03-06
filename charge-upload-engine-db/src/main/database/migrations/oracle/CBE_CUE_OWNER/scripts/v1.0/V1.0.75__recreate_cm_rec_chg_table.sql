DROP TABLE CM_REC_CHG;
CREATE TABLE CM_REC_CHG
(
    "REC_CHG_ID" VARCHAR2(36 BYTE) NOT NULL ENABLE,
	"PRODUCT_ID" VARCHAR2(30 BYTE) NOT NULL ENABLE,
	"LCP" VARCHAR2(30 BYTE) NOT NULL ENABLE,
	"PARTY_ID" VARCHAR2(30 BYTE) NOT NULL ENABLE,
	"FREQUENCY_ID" VARCHAR2(15 BYTE) NOT NULL ENABLE,
	"CURRENCY_CD" CHAR(3 BYTE) NOT NULL ENABLE,
	"PRICE" NUMBER(36,18) NOT NULL ENABLE,
	"QUANTITY" NUMBER(36,0) NOT NULL ENABLE,
	"VALID_FROM" DATE NOT NULL ENABLE,
	"VALID_TO" DATE,
	"STATUS" VARCHAR2(15 BYTE) NOT NULL ENABLE,
	"SOURCE_ID" VARCHAR2(15 BYTE) NOT NULL ENABLE,
	"CRE_DTTM" DATE NOT NULL ENABLE,
	"LAST_UPD_DTTM" DATE NOT NULL ENABLE,
	"BATCH_CODE" VARCHAR2(128 BYTE) NOT NULL ENABLE,
	"BATCH_ATTEMPT" NUMBER(3,0) NOT NULL ENABLE,
	"PARTITION_ID" NUMBER(*,0) NOT NULL ENABLE,
	"ILM_DT" DATE NOT NULL ENABLE,
	"ILM_ARCH_SW" CHAR(1 BYTE) NOT NULL ENABLE,
	"DIVISION" CHAR(5 BYTE) NOT NULL ENABLE,
	"SUB_ACCT" VARCHAR2(15 BYTE) NOT NULL ENABLE,
	"ACCOUNT_ID" CHAR(10 BYTE) NOT NULL ENABLE,
	"SUB_ACCOUNT_ID" CHAR(10 BYTE) NOT NULL ENABLE,
	"TXN_HEADER_ID" CHAR(14 BYTE) NOT NULL ENABLE,
	"PARTITION"     NUMBER(5,0)    GENERATED ALWAYS AS (ORA_HASH(SOURCE_ID, 31)) VIRTUAL,
	CONSTRAINT "CM_REC_CHG_PK" PRIMARY KEY ("REC_CHG_ID")
)
TABLESPACE CBE_CUE_DATA
    PARTITION BY RANGE (ilm_dt) INTERVAL(NUMTODSINTERVAL(1, 'DAY'))
SUBPARTITION BY LIST(PARTITION)
SUBPARTITION TEMPLATE(
    SUBPARTITION p1 VALUES(0,1,2,3),
    SUBPARTITION p2 VALUES(4,5,6,7),
    SUBPARTITION p3 VALUES(8,9,10,11),
    SUBPARTITION p4 VALUES(12,13,14,15),
    SUBPARTITION p5 VALUES(16,17,18,19),
    SUBPARTITION p6 VALUES(20,21,22,23),
    SUBPARTITION p7 VALUES(24,25,26,27),
    SUBPARTITION p8 VALUES(28,29,30,31)
)
(PARTITION before_p0 VALUES LESS THAN (TO_DATE('2020-12-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')) SEGMENT CREATION DEFERRED)
ENABLE ROW MOVEMENT;