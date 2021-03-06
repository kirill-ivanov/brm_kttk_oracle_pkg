CREATE OR REPLACE TRIGGER BILL_T_BU_TRG
BEFORE INSERT OR UPDATE OR DELETE
ON PIN.BILL_T
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
c_tg_name  CONSTANT VARCHAR2(30) := 'BILL_T_BU_TRG';
v_position PERIOD_T.POSITION%TYPE;
v_billing_id INTEGER;
/******************************************************************************
��������� ��������� ���������� �������������� ������ 
��������� �������� ������ ����� ����������������
******************************************************************************/
BEGIN

   IF UPDATING THEN  
      IF (:NEW.BILL_STATUS = :OLD.BILL_STATUS AND :NEW.BILL_STATUS IN ('READY','CLOSED')) THEN
         IF :NEW.TOTAL != :OLD.TOTAL OR :NEW.GROSS != :OLD.GROSS OR :NEW.TAX != :OLD.TAX THEN
            Pk01_Syslog.Write_msg('Can not update CLOSED bill (bill_no = '||:NEW.BILL_NO||').',
                                  c_tg_name, Pk01_Syslog.L_err );
            RAISE_APPLICATION_ERROR(-20100, 'Can not update CLOSED bill (bill_no = '||:NEW.BILL_NO||').');
         END IF;
      ELSIF :OLD.BILL_STATUS = 'CLOSED' THEN
         -- ������ �������� �� ������� �������� ��������
         SELECT P.POSITION INTO v_position
           FROM PERIOD_T P
           WHERE P.PERIOD_ID = :OLD.REP_PERIOD_ID;
         IF v_position NOT IN ('BILL','OPEN') THEN
            SELECT A.BILLING_ID
              INTO v_billing_id 
              FROM ACCOUNT_T A
             WHERE A.ACCOUNT_ID = :NEW.ACCOUNT_ID;
            IF v_billing_id IN (2001,2002,2003,2006) THEN
              Pk01_Syslog.Write_msg('Can not update CLOSED bill (bill_no = '||:NEW.BILL_NO||').',
                                    c_tg_name, Pk01_Syslog.L_err );
              RAISE_APPLICATION_ERROR(-20100, 'Can not update CLOSED bill (bill_no = '||:NEW.BILL_NO||').');
            END IF;
         END IF;
      END IF;
   ELSIF DELETING AND :OLD.BILL_STATUS = 'CLOSED' THEN
     -- ������ �������� �� ������� �������� ��������
     SELECT P.POSITION INTO v_position
       FROM PERIOD_T P
       WHERE P.PERIOD_ID = :OLD.REP_PERIOD_ID;
     IF v_position NOT IN ('BILL','OPEN') THEN
        SELECT A.BILLING_ID
          INTO v_billing_id 
          FROM ACCOUNT_T A
         WHERE A.ACCOUNT_ID = :OLD.ACCOUNT_ID;
        -- ��� �������� ��������� ����� ���
        IF v_billing_id IN (2001,2002,2003,2006) THEN
          Pk01_Syslog.Write_msg('Can not delete CLOSED bill (bill_no = '||:NEW.BILL_NO||').',
                                c_tg_name, Pk01_Syslog.L_err );
          RAISE_APPLICATION_ERROR(-20100, 'Can not delete CLOSED bill (bill_no = '||:NEW.BILL_NO||').');
        END IF;
     END IF;
   END IF;   

END BILL_T_BU_TRG;
/
