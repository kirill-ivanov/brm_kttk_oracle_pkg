CREATE OR REPLACE TRIGGER COMPANY_T_BUIDR_TRG
BEFORE INSERT OR UPDATE OR DELETE
ON PIN.COMPANY_T
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
    v_count    INTEGER;
    v_date_max DATE;
    v_new_actual CHAR;
    v_old_actual CHAR;
BEGIN
    IF NOT PK92_COMPANY_TG.latch THEN
      IF UPDATING OR INSERTING THEN
        -- ����������� id ��������
        IF :NEW.COMPANY_ID IS NULL THEN 
           :NEW.COMPANY_ID := SQ_CLIENT_ID.NEXTVAL;
        END IF;  
        --
        -- ��������� �� ������������ ����������� ���
        IF :NEW.DATE_TO IS NOT NULL AND :NEW.DATE_FROM > :NEW.DATE_TO THEN
          Pk01_Syslog.Write_msg(
                'Contract_id = '||:NEW.CONTRACT_ID||','||
                ' Date_from = "'||TO_CHAR(:NEW.DATE_FROM,'dd.mm.yyyy hh24:mi:ss')||'",'||
                ' Date_to = "'||TO_CHAR(:NEW.DATE_TO,'dd.mm.yyyy hh24:mi:ss')||'"'||
                ' - interval error'
                , 'COMPANY_T_BIU_TRG', Pk01_Syslog.L_err );
          RAISE_APPLICATION_ERROR(-20100, 
                'Contract_id = '||:NEW.CONTRACT_ID||','||
                ' Date_from = "'||TO_CHAR(:NEW.DATE_FROM,'dd.mm.yyyy hh24:mi:ss')||'",'||
                ' Date_to = "'||TO_CHAR(:NEW.DATE_TO,'dd.mm.yyyy hh24:mi:ss')||'"'||
                ' - interval error');
        END IF;

        -- ��������� ������� � ���������� ������� ��������� ��������� (������� �����))
        PK92_COMPANY_TG.add_company(p_company_id => :NEW.COMPANY_ID,
                                    p_contract_id=> :NEW.CONTRACT_ID);
        /*
        -- ��������� ������ �� ������������
        IF :NEW.ACTUAL IS NULL 
           AND :NEW.DATE_FROM < SYSDATE 
           AND (:NEW.DATE_TO IS NULL OR SYSDATE < :NEW.DATE_TO) 
        THEN
          :NEW.ACTUAL := 'Y';
        END IF;
        
        -- ���������� ������������ 
        IF :NEW.ACTUAL = 'Y' THEN
          PK92_COMPANY_TG.add_actual(p_company_id => :NEW.COMPANY_ID,
                                     p_contract_id=> :NEW.CONTRACT_ID);
        END IF;
        
        -- ��������� �� ����������� ����������
        IF INSERTING THEN
          PK92_COMPANY_TG.add_company(p_company_id => :NEW.COMPANY_ID,
                                      p_contract_id=> :NEW.CONTRACT_ID);
        ELSIF UPDATING THEN
          v_date_max := TO_DATE('01.01.2050','dd.mm.yyyy');
          IF :NEW.DATE_FROM != :OLD.DATE_FROM OR
             NVL(:NEW.DATE_TO, v_date_max) != NVL(:OLD.DATE_TO, v_date_max)
          THEN
            PK92_COMPANY_TG.add_company(p_company_id => :NEW.COMPANY_ID,
                                        p_contract_id=> :NEW.CONTRACT_ID);
          END IF;
        END IF;
        */
      ELSIF DELETING THEN
        -- �������� ����������, ������ ���� ������� ���������� ��������
        IF :OLD.ACTUAL = 'Y' THEN
          PK92_COMPANY_TG.del_company(p_company_id => :OLD.COMPANY_ID,
                                        p_contract_id=> :OLD.CONTRACT_ID);
        END IF;
      END IF;
    END IF;
EXCEPTION
   WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
END;
/
