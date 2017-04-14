CREATE OR REPLACE TRIGGER ACCOUNT_PROFILE_T_BIU_TRG
BEFORE INSERT OR UPDATE
ON PIN.ACCOUNT_PROFILE_T
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
    v_billing_id   INTEGER;
    v_count        INTEGER := 0;
BEGIN
    -- получаем значения для billing_id  
    SELECT 
      CASE 
        WHEN CT.CONTRACTOR LIKE '%(РП)'       THEN 2006
        WHEN CT.CONTRACTOR LIKE '%(РП, тест)' THEN 2008
        ELSE NULL
      END INTO v_billing_id
      FROM CONTRACTOR_T CT
     WHERE CT.CONTRACTOR_ID = :NEW.CONTRACTOR_ID;
    --
    -- исправляем billing_id, если нужно
    IF v_billing_id IS NOT NULL THEN
        UPDATE ACCOUNT_T A
           SET A.BILLING_ID = v_billing_id
         WHERE A.ACCOUNT_ID = :NEW.ACCOUNT_ID;
    END IF;
    --
    -- проставляем дату создания/изменения записи
    :NEW.MODIFY_DATE := SYSDATE;
    --
    -- сохраняем информацию о пользователе создавшем/изменившем запись
    SELECT SYS_CONTEXT('USERENV', 'OS_USER')
      INTO :NEW.MODIFIED_BY
      FROM dual;
    --
    -- принудительно выравниваем дату окончания периода по концу суток
    :NEW.DATE_TO := TRUNC(:NEW.DATE_TO)+86399/86400;  
   
    -- проверяем на правильность расстановки дат
    IF :NEW.DATE_TO IS NOT NULL AND :NEW.DATE_FROM > :NEW.DATE_TO THEN
        Pk01_Syslog.Write_msg(
              'Account_id = '||:NEW.ACCOUNT_ID||','||
              ' Date_from = "'||TO_CHAR(:NEW.DATE_FROM,'dd.mm.yyyy hh24:mi:ss')||'",'||
              ' Date_to = "'||TO_CHAR(:NEW.DATE_TO,'dd.mm.yyyy hh24:mi:ss')||'"'||
              ' - interval error'
              , 'COMPANY_T_BIU_TRG', Pk01_Syslog.L_err );
        RAISE_APPLICATION_ERROR(-20100, 
              'Account_id = '||:NEW.ACCOUNT_ID||','||
              ' Date_from = "'||TO_CHAR(:NEW.DATE_FROM,'dd.mm.yyyy hh24:mi:ss')||'",'||
              ' Date_to = "'||TO_CHAR(:NEW.DATE_TO,'dd.mm.yyyy hh24:mi:ss')||'"'||
              ' - interval error');
    END IF;
    /*
    IF INSERTING THEN 
        -- проверяем на пересечение интервалов
        SELECT COUNT(*) INTO v_count 
          FROM ACCOUNT_PROFILE_T P
         WHERE :NEW.ACCOUNT_ID  = P.ACCOUNT_ID
           AND :NEW.PROFILE_ID != P.PROFILE_ID
           AND (:NEW.DATE_FROM BETWEEN P.DATE_FROM AND NVL(P.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
            OR NVL(:NEW.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy')) BETWEEN P.DATE_FROM AND NVL(P.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
            OR P.DATE_FROM BETWEEN :NEW.DATE_FROM AND NVL(:NEW.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
          );
        IF v_count > 0 THEN
            Pk01_Syslog.Write_msg(
                  'Account_id = '||:NEW.ACCOUNT_ID||','||
                  ' Date_from = "'||TO_CHAR(:NEW.DATE_FROM,'dd.mm.yyyy hh24:mi:ss')||'",'||
                  ' Date_to = "'||TO_CHAR(:NEW.DATE_TO,'dd.mm.yyyy hh24:mi:ss')||'"'||
                  ' - interval intersection'
                  , 'ACCOUNT_PROFILE_T_BIU_TRG', Pk01_Syslog.L_err );
            RAISE_APPLICATION_ERROR(-20100, 
                  'Account_id = '||:NEW.ACCOUNT_ID||','||
                  ' Date_from = "'||TO_CHAR(:NEW.DATE_FROM,'dd.mm.yyyy hh24:mi:ss')||'",'||
                  ' Date_to = "'||TO_CHAR(:NEW.DATE_TO,'dd.mm.yyyy hh24:mi:ss')||'"'||
                  ' - interval intersection');
        END IF;
    END IF;
    */
    -- проставляем признак актуальности
    IF :NEW.DATE_FROM <= SYSDATE AND (:NEW.DATE_TO IS NULL OR :NEW.DATE_TO > SYSDATE) THEN
       :NEW.ACTUAL := 'Y';
       --
       /*
       IF INSERTING THEN 
           UPDATE ACCOUNT_PROFILE_T AP
              SET AP.ACTUAL = NULL
            WHERE AP.PROFILE_ID != :NEW.PROFILE_ID
              AND AP.ACCOUNT_ID  = :NEW.ACCOUNT_ID
              AND AP.ACTUAL = 'Y'
           ;
       END IF;
       */
       -- 
    END IF;
EXCEPTION
   WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
END ACCOUNT_PROFILE_T_BIU_TRG;
/
