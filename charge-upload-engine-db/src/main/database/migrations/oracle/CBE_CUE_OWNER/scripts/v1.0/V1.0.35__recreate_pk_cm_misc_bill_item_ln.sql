ALTER TABLE CM_MISC_BILL_ITEM_LN
DROP CONSTRAINT MISC_BILL_ITEM_LN_PK;

ALTER TABLE CM_MISC_BILL_ITEM_LN
ADD CONSTRAINT MISC_BILL_ITEM_LN_PK PRIMARY KEY (BILL_ITEM_LINE_ID);
