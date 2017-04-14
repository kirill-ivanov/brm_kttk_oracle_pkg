CREATE OR REPLACE PROCEDURE SVM_TEST IS
    v_prcName   CONSTANT VARCHAR2(30) := 'SVM_TEST';
    v_period_id CONSTANT INTEGER := 201504;
    v_profile_id INTEGER;
    v_count      INTEGER := 0;
BEGIN
  --  
  Pk01_Syslog.Write_msg('Start', v_prcName, Pk01_Syslog.L_info );
  /*
  UPDATE COMPANY_T SET DATE_FROM = TO_DATE('11.04.2016','dd.mm.yyyy')
   WHERE contract_id = 1522037
     AND COMPANY_ID  = 1798641;
  */
  UPDATE ACCOUNT_PROFILE_T AP SET AP.DATE_TO = NULL
   WHERE AP.ACCOUNT_ID = 2389325;
  
  /*
  DELETE FROM COMPANY_T
   WHERE CONTRACT_ID = 1522037
--     AND COMPANY_ID = 1798157
     AND COMPANY_ID = 1798593;
  */
  /*
  INSERT INTO COMPANY_TEST (COMPANY_ID, CONTRACT_ID, COMPANY_NAME, DATE_FROM, DATE_TO)
  SELECT SQ_CLIENT_ID.NEXTVAL COMPANY_ID, CONTRACT_ID, COMPANY_NAME, DATE_TO + 1/86400, NULL DATE_TO 
    FROM COMPANY_TEST CM
   WHERE CONTRACT_ID = 1432864;
  */
  /*
  UPDATE COMPANY_TEST CM SET DATE_TO = TRUNC(SYSDATE)-1/86400
   WHERE CONTRACT_ID = 1432864;
  */
  /*
  INSERT INTO COMPANY_TEST (COMPANY_ID, CONTRACT_ID, COMPANY_NAME, DATE_FROM, DATE_TO)
  SELECT SQ_CLIENT_ID.NEXTVAL COMPANY_ID, CONTRACT_ID, COMPANY_NAME, DATE_FROM, DATE_TO 
    FROM COMPANY_TEST CM
   WHERE CONTRACT_ID = 1432864;
  */
  /*
  INSERT INTO COMPANY_T (CONTRACT_ID,
                         COMPANY_NAME,
                         SHORT_NAME,
                         DATE_FROM,
                         DATE_TO)
     SELECT 418931945,
            CS.CUSTOMER,
            CS.SHORT_NAME,
            TO_DATE ('01.04.2016', 'DD.MM.YYYY'),
            NULL
       FROM CUSTOMER_T CS
      WHERE CS.CUSTOMER_ID = 1558172;
 */
  /*  
  UPDATE ACCOUNT_PROFILE_T
  SET CUSTOMER_ID = 1 
  WHERE ACCOUNT_ID = 1946670;
  */
  
  /*
  INSERT INTO COMPANY_TEST(CONTRACT_ID, COMPANY_NAME, DATE_FROM, DATE_TO)
  SELECT CONTRACT_ID, COMPANY_NAME, 
         --DATE_FROM, DATE_TO 
         TO_DATE('01.01.2000','dd.mm.yyyy') DATE_FROM, TO_DATE('01.01.2000','dd.mm.yyyy') DATE_TO
    FROM COMPANY_TEST
   WHERE CONTRACT_ID = 1432834;
  */
  /*
  INSERT INTO ACCOUNT_PROFILE_TEST(PROFILE_ID, ACCOUNT_ID, CONTRACT_ID, DATE_FROM, DATE_TO)
  SELECT PK02_POID.NEXT_ACCOUNT_PROFILE_ID 
         PROFILE_ID, ACCOUNT_ID, CONTRACT_ID,  
         DATE_FROM, DATE_TO 
    FROM ACCOUNT_PROFILE_TEST AP
   WHERE AP.ACCOUNT_ID = 2265964
     AND AP.PROFILE_ID = 2265965;
  */
  /*
  --
  -- Филиал ЗАО «Компания ТрансТелеКом»«Макрорегион Юго-Восток»(1524383) и 
  -- Филиал ЗАО «Компания ТрансТелеКом» «Макрорегион СПАРК»(1524399), 
  -- добавить с 1/1/16 новый профиль с новым поставщиком 
  -- Филиал ЗАО «Компания ТрансТелеКом» «Макрорегион Центр»(1524378). 
  -- Новые реквизиты должны начать применяться только к январским счетам.
  --
  FOR rp IN (
      SELECT AP.PROFILE_ID
        FROM ACCOUNT_PROFILE_T AP
       WHERE AP.CONTRACTOR_ID IN (1524383,1524399)
         --AND AP.DATE_FROM < TO_DATE('01.12.2016','dd.mm.yyyy')
         AND AP.DATE_TO IS NULL
    )
    LOOP
        --        
        UPDATE ACCOUNT_PROFILE_T AP
           SET AP.DATE_TO    = TO_DATE('31.12.2015 23:59:59','dd.mm.yyyy hh24:mi:ss')
         WHERE AP.PROFILE_ID = rp.Profile_Id;
        --
        v_profile_id := Pk02_Poid.Next_account_profile_id;
        --
        INSERT INTO ACCOUNT_PROFILE_T AP(
                PROFILE_ID, ACCOUNT_ID, CONTRACT_ID, 
                CUSTOMER_ID, SUBSCRIBER_ID, CONTRACTOR_ID, 
                BRANCH_ID, AGENT_ID, CONTRACTOR_BANK_ID, VAT, DATE_FROM, DATE_TO, 
                CUSTOMER_PAYER_ID, BRAND_ID
        )
        SELECT  v_profile_id PROFILE_ID, ACCOUNT_ID, CONTRACT_ID, 
                CUSTOMER_ID, SUBSCRIBER_ID,  1524378 CONTRACTOR_ID, 
                BRANCH_ID, AGENT_ID, 1524379 CONTRACTOR_BANK_ID, VAT, 
                TO_DATE('01.01.2016','dd.mm.yyyy') DATE_FROM, NULL DATE_TO, 
                CUSTOMER_PAYER_ID, BRAND_ID
          FROM ACCOUNT_PROFILE_T AP
         WHERE AP.PROFILE_ID = rp.Profile_Id;
        --
        UPDATE BILL_T B
           SET B.PROFILE_ID = v_profile_id, 
               B.CONTRACTOR_ID = 1524378, 
               B.CONTRACTOR_BANK_ID = 1524379
         WHERE B.REP_PERIOD_ID = 201601
           AND B.PROFILE_ID = rp.Profile_Id;

        v_count := v_count + 1;
    END LOOP;
  */
  --
  Pk01_Syslog.Write_msg('Stop, '||v_count||' rows processed', v_prcName, Pk01_Syslog.L_info );
  --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', v_prcName );
END;
/
