CREATE OR REPLACE TRIGGER ORDER_BODY_T_BIU_TRG
BEFORE INSERT OR UPDATE
ON PIN.ORDER_BODY_T
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
    v_ACCOUNT_TYPE CHAR(1 CHAR);
    v_flag         BOOLEAN;
BEGIN
    -- ����������� ���� ���������
    :NEW.MODIFY_DATE := SYSDATE;
    v_flag := false;
    -- ����������� ������� ����������� ���� ��� �� ������
    IF :NEW.RATE_RULE_ID IS NULL THEN
      IF :NEW.CHARGE_TYPE = 'REC' THEN
        :NEW.RATE_RULE_ID := 2402;  -- c_RATE_RULE_ABP_STD -- ����������� ���������� ���������
        v_flag := true;
      ELSIF :NEW.CHARGE_TYPE = 'MIN' THEN
        :NEW.RATE_RULE_ID := 2401;  -- c_RATE_RULE_MIN_STD -- ����������� ���������� �� ����������� �����
        v_flag := true;
      ELSIF :NEW.CHARGE_TYPE = 'IDL' THEN
        :NEW.RATE_RULE_ID := 2404;  -- c_RATE_RULE_IDL_STD -- ����������� ��������, ����������� �����
        v_flag := true;  
      END IF;
      -- ��������� �����
      IF v_flag = true THEN
        Pk01_Syslog.Write_msg('Order_id = '||:NEW.ORDER_ID||','||
                              ' Order_body_id = "'||:NEW.ORDER_BODY_ID||'",'||
                              ' Subservice_id = "'||:NEW.SUBSERVICE_ID||'",'||
                              ' Rate_rule_id is NULL set to "'||:NEW.RATE_RULE_ID, 
                              'ORDER_BODY_T_BIU_TRG', 
                               Pk01_Syslog.L_err );
      END IF;
    END IF;
    -- ��������� ������� �������� TAX_INCL (��������� ������� � �����)
    IF :NEW.TAX_INCL IS NULL AND :NEW.CHARGE_TYPE IN ('REC','MIN','IDL') THEN
        -- ����������� �� ������������ ������� 'J' - 'N', 'P' - 'Y'
        SELECT DISTINCT ACCOUNT_TYPE 
          INTO v_ACCOUNT_TYPE
          FROM ORDER_T O, ACCOUNT_T A
         WHERE O.ACCOUNT_ID = A.ACCOUNT_ID
           AND O.ORDER_ID   = :NEW.ORDER_ID;
        --
        IF v_ACCOUNT_TYPE = 'P' THEN
          :NEW.TAX_INCL := 'Y';
        ELSIF v_ACCOUNT_TYPE = 'J' THEN
          :NEW.TAX_INCL := 'N';
        END IF;
        -- ��������� �����
        Pk01_Syslog.Write_msg('Order_id = '||:NEW.ORDER_ID||','||
                              ' Order_body_id = "'||:NEW.ORDER_BODY_ID||'",'||
                              ' Tax_incl is NULL set to "'||:NEW.TAX_INCL, 
                              'ORDER_BODY_T_BIU_TRG', 
                              Pk01_Syslog.L_err );
    END IF;

EXCEPTION
   WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
END ORDER_BODY_T_BIU_TRG;
/
