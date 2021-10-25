create table CM_REC_CHG
(
    REC_CHG_ID    VARCHAR2(15)   NOT NULL ENABLE,
    PRODUCT_ID    VARCHAR2(30)   NOT NULL ENABLE,
    LCP           VARCHAR2(30)   NOT NULL ENABLE,
    PARTY_ID      VARCHAR2(30)   NOT NULL ENABLE,
    FREQUENCY_ID  VARCHAR2(15)   NOT NULL ENABLE,
    CURRENCY_CD   CHAR(3)        NOT NULL ENABLE,
    PRICE         NUMBER(36, 18) NOT NULL ENABLE,
    QUANTITY      NUMBER(36)     NOT NULL ENABLE,
    VALID_FROM    DATE           NOT NULL ENABLE,
    VALID_TO      DATE,
    STATUS        VARCHAR2(15)   NOT NULL ENABLE,
    SOURCE_ID     VARCHAR2(15)   NOT NULL ENABLE,
    CRE_DTTM      DATE           NOT NULL ENABLE,
    LAST_UPD_DTTM DATE           NOT NULL ENABLE
);