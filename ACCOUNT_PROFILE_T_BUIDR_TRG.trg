CREATE OR REPLACE TRIGGER ACCOUNT_PROFILE_T_BUIDR_TRG
BEFORE INSERT OR UPDATE OR DELETE
ON PIN.ACCOUNT_PROFILE_T
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
    v_billing_id   INTEGER;
    --v_date_max DATE;
BEGIN
    IF NOT PK91_ACCOUNT_PROFILE_TG.latch THEN
      IF UPDATING OR INSERTING THEN

        -- получаем значени€ дл€ billing_id  
        SELECT 
          CASE 
            WHEN CT.CONTRACTOR LIKE '%(–ѕ)'       THEN 2006
            WHEN CT.CONTRACTOR LIKE '%(–ѕ, тест)' THEN 2008
            ELSE NULL
          END INTO v_billing_id
          FROM CONTRACTOR_T CT
         WHERE CT.CONTRACTOR_ID = :NEW.CONTRACTOR_ID;
        --
        -- исправл€ем billing_id, если нужно
        IF v_billing_id IS NOT NULL THEN
            UPDATE ACCOUNT_T A
               SET A.BILLING_ID = v_billing_id
             WHERE A.ACCOUNT_ID = :NEW.ACCOUNT_ID;
        END IF;
        --
        -- проставл€ем дату создани€/изменени€ записи
        :NEW.MODIFY_DATE := SYSDATE;
        --
        -- сохран€ем информацию о пользователе создавшем/изменившем запись
        SELECT SYS_CONTEXT('USERENV', 'OS_USER')
          INTO :NEW.MODIFIED_BY
          FROM dual;
        --
        -- принудительно выравниваем дату окончани€ периода по концу суток
        IF :NEW.DATE_TO IS NOT NULL THEN
          :NEW.DATE_TO := TRUNC(:NEW.DATE_TO)+86399/86400;  
          --
          -- провер€ем на правильность расстановки дат
          IF :NEW.DATE_FROM > :NEW.DATE_TO THEN
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
        END IF;
        
        -- заполн€ем таблицу с профил€ми которых коснулись изменени€ (неважно какие))
        PK91_ACCOUNT_PROFILE_TG.add_profile(p_profile_id => :NEW.PROFILE_ID,
                                            p_account_id=> :NEW.ACCOUNT_ID);
        /*
        -- провер€ем запись на актуальность
        IF :NEW.ACTUAL IS NULL 
           AND :NEW.DATE_FROM < SYSDATE 
           AND (:NEW.DATE_TO IS NULL OR SYSDATE < :NEW.DATE_TO) 
        THEN
          :NEW.ACTUAL := 'Y';
        END IF;
        
        -- сбрасываем неактуальные 
        IF :NEW.ACTUAL = 'Y' THEN
          PK91_ACCOUNT_PROFILE_TG.add_actual(p_profile_id => :NEW.PROFILE_ID,
                                             p_account_id=> :NEW.ACCOUNT_ID);
        END IF;
        
        -- провер€ем на пересечение интервалов
        IF INSERTING THEN
          PK91_ACCOUNT_PROFILE_TG.add_profile(p_profile_id => :NEW.PROFILE_ID,
                                              p_account_id=> :NEW.ACCOUNT_ID);
        ELSIF UPDATING THEN
          v_date_max := TO_DATE('01.01.2050','dd.mm.yyyy');
          IF :NEW.DATE_FROM != :OLD.DATE_FROM OR
             NVL(:NEW.DATE_TO, v_date_max) != NVL(:OLD.DATE_TO, v_date_max)
          THEN
            PK91_ACCOUNT_PROFILE_TG.add_profile(p_profile_id => :NEW.PROFILE_ID,
                                                p_account_id=> :NEW.ACCOUNT_ID);
          END IF;
        END IF;
        */
      ELSIF DELETING THEN
        -- операци€ интересена, только если удал€ем актуальное значение
        IF :OLD.ACTUAL = 'Y' THEN
          PK91_ACCOUNT_PROFILE_TG.del_profile(p_profile_id => :NEW.PROFILE_ID,
                                              p_account_id=> :NEW.ACCOUNT_ID);
        END IF;
      END IF;
    END IF;
EXCEPTION
   WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
END;
/
