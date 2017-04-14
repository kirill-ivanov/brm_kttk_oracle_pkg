CREATE OR REPLACE TRIGGER ORDER_BODY_T_BD_TRG
BEFORE DELETE
ON PIN.ORDER_BODY_T
REFERENCING OLD AS OLD
FOR EACH ROW
DECLARE
    v_count        INTEGER;
BEGIN
    -- проверяем нет ли записей в ITEM_T для удаляемой строки
    -- constraint повесить не можем, т.к. уже накосячили  
    SELECT COUNT(*) INTO v_count
      FROM ITEM_T I
     WHERE I.ORDER_BODY_ID = :OLD.ORDER_BODY_ID;
    IF v_count > 1 THEN
        Pk01_Syslog.Write_msg('Order_id = '||:OLD.ORDER_ID||','||
                              ' Order_body_id = "'||:OLD.ORDER_BODY_ID||
                              ' - delete error, reference to the entry in the ITEM_T exists'||:NEW.RATE_RULE_ID, 
                              'ORDER_BODY_T_BD_TRG', 
                               Pk01_Syslog.L_err );

        RAISE_APPLICATION_ERROR(-20100, 'Невозможно удалить запись, т.к. для нее уже была создана позиция счета.');
    END IF;

EXCEPTION
   WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
END ORDER_BODY_T_BD_TRG;
/
