CREATE OR REPLACE TRIGGER ITEM_T_BUID_STATUS_TRG
BEFORE INSERT OR UPDATE OR DELETE
ON PIN.ITEM_T
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
c_tg_name     CONSTANT VARCHAR2(30) := 'ITEM_T_BUID_STATUS_TRG'; 
v_period_id   INTEGER;
v_bill_id     INTEGER;
v_bill_status VARCHAR2(10);
v_bill_type   VARCHAR2(1);
v_item_id     INTEGER;
v_billing_id  INTEGER;

/******************************************************************************
запрещаем работу над позициями сформированных счетов
******************************************************************************/
BEGIN
   IF UPDATING OR INSERTING THEN  
      v_period_id := :NEW.REP_PERIOD_ID;
      v_bill_id   := :NEW.BILL_ID;
      v_item_id   := :NEW.ITEM_ID;
      :NEW.LAST_MODIFIED := SYSDATE;
      :NEW.MODIFIED_BY   := Pk01_Syslog.g_OS_USER;
   ELSIF DELETING THEN
      v_period_id := :OLD.REP_PERIOD_ID;
      v_bill_id   := :OLD.BILL_ID;
      v_item_id   := :OLD.ITEM_ID;
   END IF;   
   -- проверяем статус счета
   SELECT B.BILL_STATUS, B.BILL_TYPE, A.BILLING_ID
     INTO v_bill_status, v_bill_type, v_billing_id
     FROM BILL_T B, ACCOUNT_T A
    WHERE B.BILL_ID       = v_bill_id
      AND B.REP_PERIOD_ID = v_period_id
      AND B.ACCOUNT_ID    = A.ACCOUNT_ID;
      
   -- фиксируем ошибку работать с item-s можно только в открытом счете
   IF v_bill_status != 'OPEN' THEN
     -- позже уберу - это массовая замена статусов при закрытии периода
     IF NOT (:OLD.ITEM_STATUS = 'READY' AND :NEW.ITEM_STATUS = 'CLOSED') THEN
        -- для тестовых биллингов можно все
       IF v_billing_id IN (2001,2002,2003,2006) THEN
          Pk01_Syslog.Write_msg('Incorrect bill_status.'||
                                ' Bill_status = "'||v_bill_status||'"'||
                                ' Bill_id = '||v_bill_id||','||
                                ' Period_id = '||v_period_id||','||
                                ' Item_id = '||v_item_id,
                                c_tg_name, Pk01_Syslog.L_err );
            
          RAISE_APPLICATION_ERROR(-20100, 'Bill status must be OPEN.');
       END IF;
     END IF;
   END IF;

END ITEM_T_BUID_STATUS_TRG;
/
