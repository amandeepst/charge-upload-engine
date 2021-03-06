DROP TABLE CM_REC_CHG_ERR;

CREATE TABLE CM_REC_CHG_ERR
(
    "REC_CHG_ID" VARCHAR2(36 BYTE)  ,
	"PRODUCT_ID" VARCHAR2(30 BYTE)  ,
	"LCP" VARCHAR2(30 BYTE)  ,
	"PARTY_ID" VARCHAR2(30 BYTE)  ,
	"FREQUENCY_ID" VARCHAR2(15 BYTE)  ,
	"CURRENCY_CD" CHAR(3 BYTE)  ,
	"PRICE" NUMBER(36,18)  ,
	"QUANTITY" NUMBER(36,0)  ,
	"VALID_FROM" DATE  ,
	"VALID_TO" DATE,
	"STATUS" VARCHAR2(15 BYTE)  ,
	"SOURCE_ID" VARCHAR2(15 BYTE)  ,
	"BATCH_CODE" VARCHAR2(128 BYTE)  ,
	"BATCH_ATTEMPT" NUMBER(3,0)  ,
	"PARTITION_ID" NUMBER(*,0)  ,
	"ILM_DT" TIMESTAMP  ,
	"DIVISION" CHAR(5 BYTE)  ,
	"SUB_ACCT" VARCHAR2(15 BYTE)  ,
	"TXN_HEADER_ID" CHAR(14 BYTE)  ,
     "CUTOFF_DT"      DATE,
   "RETRY_COUNT"      NUMBER(3)       ,
   "FIRST_FAILURE_AT"  DATE      ,
    "CODE"             VARCHAR2(2048),
    "REASON"           VARCHAR2(2048)  ,
    "STACK_TRACE"      VARCHAR2(4000),
    "CREATED_AT"       TIMESTAMP       ,
    "PARTITION"        NUMBER(5,0)          GENERATED ALWAYS AS (ORA_HASH(TXN_HEADER_ID, 15)) VIRTUAL
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