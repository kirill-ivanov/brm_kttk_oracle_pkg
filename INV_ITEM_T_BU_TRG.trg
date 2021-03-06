CREATE OR REPLACE TRIGGER INV_ITEM_T_BU_TRG
BEFORE INSERT OR UPDATE OR DELETE
ON PIN.INVOICE_ITEM_T
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
c_tg_name     CONSTANT VARCHAR2(30) := 'INV_ITEM_T_BU_TRG';
/******************************************************************************
��������� ��������� ���������� �������������� ������ 
��������� �������� ������ ����� ����������������
******************************************************************************/
BEGIN
   IF UPDATING THEN  
       IF :NEW.TOTAL      != :OLD.TOTAL OR 
          :NEW.GROSS      != :OLD.GROSS OR 
          :NEW.TAX        != :OLD.TAX OR
          :NEW.VAT        != :OLD.VAT OR
          :NEW.SERVICE_ID != :OLD.SERVICE_ID
       THEN
          Pk01_Syslog.Write_msg('Can not update INVOICE_ITEM_T (bill_id = '||:NEW.BILL_ID||').',
                                c_tg_name, Pk01_Syslog.L_err );
          RAISE_APPLICATION_ERROR(-20100, 'Can not update INVOICE_ITEM_T');
       END IF;
   END IF;   

END INV_ITEM_T_BU_TRG;
/
