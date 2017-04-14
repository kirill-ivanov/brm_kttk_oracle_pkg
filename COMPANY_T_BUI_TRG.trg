CREATE OR REPLACE TRIGGER COMPANY_T_BUI_TRG
BEFORE INSERT OR UPDATE
ON PIN.COMPANY_T
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
    v_count INTEGER;
BEGIN
    -- проставляем id компании
    IF :NEW.COMPANY_ID IS NULL THEN 
       :NEW.COMPANY_ID := SQ_CLIENT_ID.NEXTVAL;
    END IF;  
    --
    -- проверяем на правильность расстановки дат
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
    IF INSERTING THEN
        -- проверяем на пересечение интервалов
        SELECT COUNT(*) INTO v_count 
          FROM COMPANY_T D
         WHERE :NEW.CONTRACT_ID = D.CONTRACT_ID
           AND :NEW.COMPANY_ID != D.COMPANY_ID
           AND (:NEW.DATE_FROM BETWEEN D.DATE_FROM AND NVL(D.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
            OR NVL(:NEW.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy')) BETWEEN D.DATE_FROM AND NVL(D.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
            OR D.DATE_FROM BETWEEN :NEW.DATE_FROM AND NVL(:NEW.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
          );
        IF v_count > 0 THEN
            Pk01_Syslog.Write_msg(
                  'Contract_id = '||:NEW.CONTRACT_ID||','||
                  ' Date_from = "'||TO_CHAR(:NEW.DATE_FROM,'dd.mm.yyyy hh24:mi:ss')||'",'||
                  ' Date_to = "'||TO_CHAR(:NEW.DATE_TO,'dd.mm.yyyy hh24:mi:ss')||'"'||
                  ' - interval intersection'
                  , 'COMPANY_T_BIU_TRG', Pk01_Syslog.L_err );
            RAISE_APPLICATION_ERROR(-20100, 
                  'Account_id = '||:NEW.CONTRACT_ID||','||
                  ' Date_from = "'||TO_CHAR(:NEW.DATE_FROM,'dd.mm.yyyy hh24:mi:ss')||'",'||
                  ' Date_to = "'||TO_CHAR(:NEW.DATE_TO,'dd.mm.yyyy hh24:mi:ss')||'"'||
                  ' - interval intersection');
        END IF;
    END IF;
    -- проставляем признак актуальности
    IF :NEW.DATE_FROM <= SYSDATE AND (:NEW.DATE_TO IS NULL OR :NEW.DATE_TO > SYSDATE) THEN
       :NEW.ACTUAL := 'Y';
       --
       IF INSERTING THEN
         UPDATE COMPANY_T CM
            SET CM.ACTUAL = NULL
          WHERE CM.COMPANY_ID != :NEW.COMPANY_ID
            AND CM.CONTRACT_ID = :NEW.CONTRACT_ID
            AND CM.ACTUAL = 'Y'
         ;
       END IF;
       -- 
    END IF;
EXCEPTION
   WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
END ACCOUNT_PROFILE_T_BIU_TRG;
/
