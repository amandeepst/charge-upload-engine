ALTER TABLE CM_MISC_BILL_ITEM ADD
    ( billable_item_id   VARCHAR(36)  NOT NULL ENABLE);

ALTER TABLE CM_MISC_BILL_ITEM_LN ADD
     ( billable_item_id   VARCHAR(36)  NOT NULL ENABLE);

create index  cm_misc_bill_item_id on CM_MISC_BILL_ITEM(billable_item_id);
create index  cm_misc_bill_item_ln_id on CM_MISC_BILL_ITEM_LN(billable_item_id);