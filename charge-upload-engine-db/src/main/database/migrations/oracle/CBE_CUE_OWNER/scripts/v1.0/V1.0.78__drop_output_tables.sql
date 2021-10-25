DECLARE

TYPE ARRAY_OUTPUT_TABLES is VARRAY(4) of VARCHAR(50);
ARRAY ARRAY_OUTPUT_TABLES := ARRAY_OUTPUT_TABLES('CM_REC_CHG',
                                                 'CM_REC_CHG_AUDIT',
                                                 'CM_MISC_BILL_ITEM',
                                                 'CM_MISC_BILL_ITEM_LN'
                                                 );
BEGIN
    FOR i IN 1..ARRAY.COUNT LOOP
        BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE ' || ARRAY(i);
        EXCEPTION
             WHEN OTHERS THEN dbms_output.put_line('Ignored Error: ' || SQLERRM);
        END;
    END LOOP;
END;
/