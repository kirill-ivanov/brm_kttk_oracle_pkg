CREATE OR REPLACE PACKAGE PK402_BCR_FILE
IS
    --
    -- Д А Н Н Ы Е   Д Л Я   Э К С П О Р Т А   В   B C R  ( А. Ю. Гуров )
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK402_BCR_FILE';
    -- ==============================================================================
    c_RET_OK      CONSTANT INTEGER     := 0;
    c_RET_ER		  CONSTANT INTEGER     :=-1;
    с_BS_DATA_DIR CONSTANT VARCHAR2(11):= 'BS_DATA_DIR';
    с_BCR_DIR     CONSTANT VARCHAR2(7) := 'BCR_DIR';
    с_AGENT_DIR   CONSTANT VARCHAR2(9) := 'AGENT_DIR';
    
    type t_refc is ref cursor;
    
    -- --------------------------------------------------------------------------------- --
    -- получить координаты продавца
    -- --------------------------------------------------------------------------------- --
    FUNCTION Get_sales_curator (
               p_branch_id     IN INTEGER,
               p_agent_id      IN INTEGER,
               p_contract_id   IN INTEGER,
               p_account_id    IN INTEGER,
               p_order_id      IN INTEGER,
               p_date          IN DATE
             ) RETURN VARCHAR2;

    -- PHONES
    PROCEDURE Brm_phones_to_file;
    
    -- --------------------------------------------------------------------------------
    -- Заполнить все таблицы BCR 
    -- --------------------------------------------------------------------------------
    PROCEDURE Load_bcr_tables(p_period_id IN INTEGER);
    
    -- INFRANET.BRM_ITEM_T;
    PROCEDURE Brm_items_to_table(p_period_id IN INTEGER);

    -- INFRANET.BRM_BILL_T;
    PROCEDURE Brm_bills_to_table(p_period_id IN INTEGER);

    -- INFRANET.BRM_ORDER_T;
    PROCEDURE Brm_orders_to_table;
    
    -- INFRANET.BRM_ACCOUNT_T;
    PROCEDURE Brm_accounts_to_table;

    -- INFRANET.BRM_CLIENT_T;
    PROCEDURE Brm_clients_to_table;
    
    -- INFRANET.BRM_DELIVERY_T;
    PROCEDURE Brm_delivery_to_table;
    
END PK402_BCR_FILE;
/
CREATE OR REPLACE PACKAGE BODY PK402_BCR_FILE
IS

-- --------------------------------------------------------------------------------- --
-- получить координаты продавца
-- --------------------------------------------------------------------------------- --
FUNCTION Get_sales_curator (
           p_branch_id     IN INTEGER,
           p_agent_id      IN INTEGER,
           p_contract_id   IN INTEGER,
           p_account_id    IN INTEGER,
           p_order_id      IN INTEGER,
           p_date          IN DATE
         ) RETURN VARCHAR2
IS
    v_mgr VARCHAR2(300);
BEGIN
      SELECT TRIM(
             LAST_NAME||' '||
             SUBSTR(UPPER(FIRST_NAME),1,1)||DECODE(FIRST_NAME,NULL,'','.')||
             SUBSTR(UPPER(MIDDLE_NAME),1,1)||DECODE(MIDDLE_NAME,NULL,'','.')
             ) MGR_NAME
        INTO v_mgr
        FROM (
          SELECT M.LAST_NAME, M.FIRST_NAME, M.MIDDLE_NAME,
                 CASE 
                   WHEN SC.CONTRACTOR_ID = p_branch_id THEN 1
                   WHEN SC.CONTRACTOR_ID = p_agent_id  THEN 2
                   WHEN SC.CONTRACT_ID   IS NOT NULL   THEN 3
                   WHEN SC.ACCOUNT_ID    IS NOT NULL   THEN 4
                   WHEN SC.ORDER_ID      IS NOT NULL   THEN 5
                   ELSE 0
                 END  WT
            FROM SALE_CURATOR_T SC, MANAGER_T M
           WHERE M.MANAGER_ID = SC.MANAGER_ID
--             AND NVL(TRUNC(p_date),TRUNC(SYSDATE)) BETWEEN SC.DATE_FROM AND NVL(SC.DATE_TO,SYSDATE)
             AND TRUNC(NVL(p_date,SYSDATE)) BETWEEN SC.DATE_FROM AND NVL(SC.DATE_TO,SYSDATE) 
             AND (SC.CONTRACTOR_ID = p_branch_id   OR
                  SC.CONTRACTOR_ID = p_agent_id    OR
                  SC.CONTRACT_ID   = p_contract_id OR 
                  SC.ACCOUNT_ID    = p_account_id  OR 
                  SC.ORDER_ID      = p_order_id )
          ORDER BY WT DESC
      )
      WHERE ROWNUM = 1
    ;  
    RETURN v_mgr;
EXCEPTION 
  WHEN NO_DATA_FOUND THEN
    RETURN NULL;
END;

-- --------------------------------------------------------------------------------
-- ITEMS
-- --------------------------------------------------------------------------------
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- INFRANET.BRM_ITEM_T;
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Brm_items_to_table(p_period_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Brm_items_to_table';
    v_count       INTEGER;
    v_period_from DATE;
    v_period_to   DATE;
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
  
    v_period_from := Pk04_Period.Period_From(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);

    DELETE FROM INFRANET.BRM_ITEM_T;

    INSERT INTO INFRANET.BRM_ITEM_T (
        REP_PERIOD, BILL_ID, BILL_NO, ORDER_ID, ORDER_NO, 
        INV_ITEM_ID, INV_ITEM_NAME, IV_DATE_FROM, IV_DATE_TO, 
        SERVICE_ID, SERVICE, SERVICE_CODE, SERVICE_REP_ID, 
        SUBSERVICE_ID, SUBSERVICE, SUBSERVICE_KEY, 
        ITEM_ID, ITEM_TYPE, CHARGE_TYPE, I_DATE_FROM, I_DATE_TO, 
        REP_GROSS, REP_TAX, MINS, SALES_MANAGER,
        PAID_SPEED, EXCESS_SPEED
    )
    WITH BDR AS( -- трафик минуты
            -- клиенты Юр. лица
            SELECT DJ.ITEM_ID, SUM(MINUTES) MINS 
              FROM DETAIL_MMTS_T_JUR DJ
             WHERE DJ.REP_PERIOD_ID = p_period_id
             GROUP BY DJ.ITEM_ID
            UNION ALL
            -- клиенты Физ. лица
            SELECT DF.ITEM_ID, SUM(DF.MINS_SUM) MINS
              FROM DETAIL_MMTS_T_FIZ DF
             WHERE DF.REP_PERIOD_ID = p_period_id
             GROUP BY DF.ITEM_ID
            UNION ALL
            -- операторы
            SELECT ITEM_ID, SUM(BILL_MINUTES) MINS
              FROM BDR_OPER_T
             WHERE REP_PERIOD BETWEEN v_period_from AND v_period_to
            GROUP BY ITEM_ID 
            UNION ALL
            -- billing_server
            SELECT ITEM_ID, SUM(MINS) MINS
              FROM DETAIL_BSRV_T
             WHERE PERIOD_ID = p_period_id
               AND ITEM_ID IS NOT NULL
             GROUP BY ITEM_ID
         ),
         SD AS ( -- трафик CCAD, тип BURST
            SELECT ITEM_ID, PAID_SPEED, EXCESS_SPEED 
              FROM ( 
              SELECT ROW_NUMBER() OVER (PARTITION BY ITEM_ID ORDER BY ITEM_ID) RN, 
                     BC.REP_PERIOD_ID, BC.ITEM_ID, 
                     BC.PAID_SPEED||' '||DP.NAME PAID_SPEED, 
                     BC.EXCESS_SPEED||' '||DE.NAME EXCESS_SPEED
                FROM BDR_CCAD_T BC, DICTIONARY_T DP, DICTIONARY_T DE
               WHERE BC.PAID_SPEED_UNIT   = DP.KEY_ID
                 AND BC.EXCESS_SPEED_UNIT = DE.KEY_ID
                 AND BC.REP_PERIOD_ID     = p_period_id
                 AND BC.SUBSERVICE_ID     = 40
                 AND BC.ITEM_ID    IS NOT NULL
             )
             WHERE RN = 1
         ),
         PF AS (
            SELECT AP.ACCOUNT_ID, 
                   AP.PROFILE_ID,
                   AP.CONTRACT_ID,
                   AP.CONTRACTOR_ID, 
                   CA.CONTRACTOR, 
                   AP.CONTRACTOR_BANK_ID BANK_ID, 
                   CB.NOTES BANK,
                   NULL BRAND_ID, -- AP.BRAND_ID, -- ДАННЫЕ НЕ ВЕДУТСЯ В BRAND_T!!!
                   NULL BRAND,    -- BR.BRAND,    -- ДАННЫЕ НЕ ВЕДУТСЯ В BRAND_T!!!
                   BRANCH.CONTRACTOR_ID XTTK_ID, 
                   BRANCH.CONTRACTOR XTTK, 
                   BRANCH.EXTERNAL_ID XTTK_OBJ_ID0,
                   AGENT.CONTRACTOR_ID AGENT_ID,      
                   AGENT.CONTRACTOR  AGENT, 
                   AGENT.EXTERNAL_ID AGENT_OBJ_ID0,
                   AP.DATE_FROM, 
                   NVL(AP.DATE_TO,SYSDATE) DATE_TO
              FROM ACCOUNT_PROFILE_T AP, --BRAND_T BR, 
                   CONTRACTOR_BANK_T CB,
                   CONTRACTOR_T BRANCH, 
                   CONTRACTOR_T AGENT, 
                   CONTRACTOR_T CA  
             WHERE AP.CONTRACTOR_ID      = CA.CONTRACTOR_ID 
               --AND AP.BRAND_ID           = BR.BRAND_ID(+)
               AND DECODE(AP.BRANCH_ID,200,11,AP.BRANCH_ID)= BRANCH.CONTRACTOR_ID(+)
               AND DECODE(AP.BRANCH_ID,200,AP.BRANCH_ID,NVL(AP.AGENT_ID,AP.BRANCH_ID)) = AGENT.CONTRACTOR_ID(+)
               AND AP.CONTRACTOR_BANK_ID = CB.BANK_ID(+)
        )
        SELECT TRUNC(v_period_to) REP_PERIOD, 
               B.BILL_ID,
               B.BILL_NO,
               O.ORDER_ID,
               O.ORDER_NO,
               IV.INV_ITEM_ID,
               IV.INV_ITEM_NAME,
               IV.DATE_FROM IV_DATE_FROM,
               TRUNC(IV.DATE_TO) IV_DATE_TO,
               I.SERVICE_ID,
               S.SERVICE,
               S.SERVICE_CODE,
               NVL(S.EXTERNAL_ID,9999) SERVICE_REP_ID,
               I.SUBSERVICE_ID,
               SS.SUBSERVICE,
               SS.SUBSERVICE_KEY,
               I.ITEM_ID,
               I.ITEM_TYPE,
               CASE
                 WHEN I.CHARGE_TYPE = 'SLA' THEN 'IDL'
                 ELSE I.CHARGE_TYPE
               END CHARGE_TYPE,
               I.DATE_FROM I_DATE_FROM, 
               TRUNC(I.DATE_TO) I_DATE_TO, 
               I.REP_GROSS, 
               I.REP_TAX,
               ROUND(BDR.MINS,6) MINS,
               PK402_BCR_DATA.Get_sales_curator (
                       p_branch_id      => PF.XTTK_ID,
                       p_agent_id       => PF.AGENT_ID,
                       p_contract_id    => PF.CONTRACT_ID,
                       p_account_id     => B.ACCOUNT_ID,
                       p_order_id       => O.ORDER_ID,
                       p_date           => v_period_to
                     ) SALES_MANAGER,
               SD.PAID_SPEED, 
               SD.EXCESS_SPEED
          FROM BILL_T B, 
               ACCOUNT_T A, 
               ITEM_T I, 
               ORDER_T O, 
               BDR, 
               PF, 
               SD,
               INVOICE_ITEM_T IV, 
               SERVICE_T S, 
               SUBSERVICE_T SS
         WHERE B.REP_PERIOD_ID = p_period_id
           AND B.ACCOUNT_ID    = A.ACCOUNT_ID
           AND B.BILL_ID       = I.BILL_ID
           AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
           AND O.ORDER_ID      = I.ORDER_ID
           AND B.PROFILE_ID    = PF.PROFILE_ID
           AND I.ITEM_ID       = BDR.ITEM_ID(+)
           AND I.ITEM_ID       = SD.ITEM_ID(+)
           AND I.REP_PERIOD_ID = IV.REP_PERIOD_ID
           AND I.INV_ITEM_ID   = IV.INV_ITEM_ID
           AND I.SERVICE_ID    = S.SERVICE_ID
           AND I.SUBSERVICE_ID = SS.SUBSERVICE_ID
           AND B.TOTAL <> 0 
           AND B.BILL_TYPE != 'I'
           AND B.BILL_STATUS IN ( 
                   Pk00_Const.c_BILL_STATE_READY,
                   Pk00_Const.c_BILL_STATE_CLOSED
               )
--           AND A.BILLING_ID IN (2000,2001,2002,2003,2006,2009) 
           AND A.BILLING_ID != 2008
           AND A.STATUS IN ( Pk00_Const.c_ACC_STATUS_BILL, -- 'B'
                             Pk00_Const.c_ACC_STATUS_CLOSED  ) -- 'C'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.BRM_ITEM_T: '||v_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
           
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------------------
-- BILLS
-- --------------------------------------------------------------------------------
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- INFRANET.BRM_BILL_T;
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Brm_bills_to_table(p_period_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Brm_bills_to_table';
    v_count       INTEGER;
    v_period_to   DATE;
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
  
    v_period_to   := Pk04_Period.Period_to(p_period_id);

    DELETE FROM INFRANET.BRM_BILL_T;

    INSERT INTO INFRANET.BRM_BILL_T (
        REP_PERIOD, BILL_ID, ACCOUNT_STATUS, PREV_BILL_ID, NEXT_BILL_ID, 
        BILL_TYPE, BILL_NO, BILL_DATE, ACT_DATE_FROM, ACT_DATE_TO, 
        CURRENCY_ID, TOTAL, GROSS, TAX, 
        ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, 
        CONTRACTOR_ID, CONTRACTOR, XTTK_ID, XTTK, XTTK_OBJ_ID0, 
        AGENT_ID, AGENT, AGENT_OBJ_ID0, BANK_ID, BANK 
    )
    SELECT TRUNC(v_period_to) REP_PERIOD, 
           B.BILL_ID, 
           A.STATUS ACCOUNT_STATUS,
           B.PREV_BILL_ID,
           B.NEXT_BILL_ID,
           B.BILL_TYPE,
           B.BILL_NO,
           TRUNC(B.BILL_DATE), 
           B.ACT_DATE_FROM,
           TRUNC(B.ACT_DATE_TO),
           B.CURRENCY_ID,
           B.TOTAL,
           B.GROSS,
           B.TAX,
           B.ACCOUNT_ID, 
           A.ACCOUNT_NO,
           A.ACCOUNT_TYPE,
           CA.CONTRACTOR_ID,
           CA.CONTRACTOR,
           BRANCH.CONTRACTOR_ID XTTK_ID, 
           BRANCH.CONTRACTOR    XTTK, 
           BRANCH.EXTERNAL_ID   XTTK_OBJ_ID0,
           AGENT.CONTRACTOR_ID  AGENT_ID, 
           AGENT.CONTRACTOR AGENT, 
           AGENT.EXTERNAL_ID AGENT_OBJ_ID0,
           B.CONTRACTOR_BANK_ID BANK_ID, 
           CB.NOTES BANK
      FROM BILL_T B, 
           ACCOUNT_T A, 
           ACCOUNT_PROFILE_T AP, 
           CONTRACTOR_BANK_T CB,
           CONTRACTOR_T BRANCH, 
           CONTRACTOR_T AGENT, 
           CONTRACTOR_T CA
     WHERE B.REP_PERIOD_ID = p_period_id
       AND B.ACCOUNT_ID    = A.ACCOUNT_ID
       AND B.PROFILE_ID    = AP.PROFILE_ID
       AND B.CONTRACTOR_ID = CA.CONTRACTOR_ID
       AND B.CONTRACTOR_BANK_ID = CB.BANK_ID
       AND DECODE(AP.BRANCH_ID,200,11,AP.BRANCH_ID) = BRANCH.CONTRACTOR_ID(+)
       AND DECODE(AP.BRANCH_ID,200,AP.BRANCH_ID,NVL(AP.AGENT_ID,AP.BRANCH_ID)) = AGENT.CONTRACTOR_ID(+)
       AND B.TOTAL <> 0 
       AND B.BILL_TYPE != 'I'
       AND B.BILL_STATUS IN ( 
               Pk00_Const.c_BILL_STATE_READY,
               Pk00_Const.c_BILL_STATE_CLOSED
           )
--       AND A.BILLING_ID IN (2000,2001,2002,2003,2006,2009)
       AND A.BILLING_ID != 2008 --IN (2000,2001,2002,2003,2005,2006,2007,2009) -- С.Макеев 03.03.2017
       AND A.STATUS IN ( Pk00_Const.c_ACC_STATUS_BILL,    -- 'B'
                         Pk00_Const.c_ACC_STATUS_CLOSED ) -- 'C'
       ORDER BY B.BILL_NO
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.BRM_BILL_T: '||v_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
           
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------------------
-- ORDERS
-- --------------------------------------------------------------------------------
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- INFRANET.BRM_ORDER_T;
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Brm_orders_to_table
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Brm_orders_to_table';
    v_count       INTEGER;
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
  
    DELETE FROM INFRANET.BRM_ORDER_T;

    INSERT INTO INFRANET.BRM_ORDER_T (
      ORDER_ID, ACCOUNT_ID, ORDER_NO, DATE_FROM, DATE_TO, 
      SERVICE_ID, SERVICE_CODE, SERVICE_REP_ID, 
      RATEPLAN_ID, RATEPLAN_NAME, AGENT_PERCENT, 
      POINT_SRC, POINT_DST, SPEED_STR, SPEED_VALUE, TIME_ZONE, BILLING_ID, CFO 
    )
    WITH D AS (
        SELECT 
            O.ORDER_ID,
            O.ACCOUNT_ID,
            O.ORDER_NO, 
            O.DATE_FROM,
            TRUNC(DECODE(SIGN(O.DATE_TO-O.DATE_FROM),-1,O.DATE_FROM,O.DATE_TO),'DD') DATE_TO, 
            O.SERVICE_ID,
            S.SERVICE_CODE,
            S.EXTERNAL_ID SERVICE_REP_ID,
            P.RATEPLAN_ID,
            P.RATEPLAN_NAME,
            P.AGENT_PERCENT,
            ROW_NUMBER() OVER (PARTITION BY OB.ORDER_ID ORDER BY DECODE(OB.CHARGE_TYPE, 'USG', 1, 'REC', 2, 3)) RN,
            I.POINT_SRC, I.POINT_DST, I.SPEED_STR, 
            I.SPEED_VALUE * POWER(1024, 2 - d.external_id) SPEED_VALUE,
            O.TIME_ZONE,
            A.BILLING_ID,
            CFO.NAME CFO_NAME
         FROM ORDER_T O, 
              SERVICE_T S, 
              ACCOUNT_T A, 
              ORDER_BODY_T OB, 
              RATEPLAN_T P,
              ORDER_INFO_T I,
              DICTIONARY_T D,
              DICTIONARY_T CFO
        WHERE O.SERVICE_ID = S.SERVICE_ID
          AND O.ORDER_NO IS NOT NULL
          AND O.ACCOUNT_ID = A.ACCOUNT_ID         
--          AND A.BILLING_ID IN (2000,2001,2002,2003,2006,2009) 
          AND A.BILLING_ID != 2008 --IN (2000,2001,2002,2003,2005,2006,2007,2009) -- С.Макеев 03.03.2017
          AND O.ORDER_ID = OB.ORDER_ID
          AND OB.RATEPLAN_ID = P.RATEPLAN_ID (+)
          AND O.ORDER_ID = I.ORDER_ID (+)
          AND D.PARENT_ID(+) = 67
          AND I.SPEED_UNIT_ID = D.KEY_ID (+)
          AND O.CFO_ID = CFO.KEY_ID(+)
    )
    SELECT ORDER_ID,
           ACCOUNT_ID,
           ORDER_NO,
           DATE_FROM,
           DATE_TO,
           SERVICE_ID,
           SERVICE_CODE,
           SERVICE_REP_ID,
           RATEPLAN_ID,
           RATEPLAN_NAME,
           AGENT_PERCENT,
           POINT_SRC,
           POINT_DST,
           SPEED_STR,
           SPEED_VALUE,
           TIME_ZONE,
           BILLING_ID,
           CFO_NAME
      FROM D
     WHERE D.RN = 1
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.BRM_ORDER_T: '||v_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
           
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------------------
-- ACCOUNTS
-- --------------------------------------------------------------------------------
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- INFRANET.BRM_ACCOUNT_T;
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Brm_accounts_to_table
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Brm_accounts_to_table';
    v_count       INTEGER;
    v_period_to   DATE;
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
  
    v_period_to := LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE),-1));

    DELETE FROM INFRANET.BRM_ACCOUNT_T;

    INSERT INTO INFRANET.BRM_ACCOUNT_T 
    (
        ACCOUNT_ID, ACCOUNT_TYPE, ACCOUNT_NO, CONTRACT_ID, CONTRACT_NO, 
        CUST_NAME, CLIENT_ID, DATE_FROM, DATE_TO, 
        MSEG_ID, MSEG_NAME, TYPE_ID, TYPE_NAME, 
        BRAND_ID, BRAND, CONTRACTOR_ID, CONTRACTOR, 
        BANK_ID, BANK, XTTK_ID, XTTK, XTTK_OBJ_ID0, 
        AGENT_ID, AGENT, AGENT_OBJ_ID0, CURRENCY_ID, VAT, 
        ERP_CODE, INN, KPP, 
        DLV_COUNTRY, DLV_ZIP, DLV_STATE, DLV_CITY, DLV_ADDRESS, 
        DLV_PERSON, DLV_PHONES, DLV_EMAIL, DLV_NOTES, 
        BILLING_ID, SALES_MANAGER, DLV_METHOD, CFU, CFO 
    )
    WITH 
      A AS ( -- л/с подлежащие выгрузке 349.912
          SELECT A.ACCOUNT_ID, A.ACCOUNT_TYPE, 
                 A.ACCOUNT_NO, A.CURRENCY_ID,
                 A.BILLING_ID
            FROM ACCOUNT_T A
--           WHERE A.BILLING_ID IN (2000,2001,2002,2003,2006,2009)
           WHERE A.BILLING_ID != 2008  -- С.Макеев 06.03.2017
             AND A.STATUS     IN ('B','C')
      ),
      AP AS ( -- актуальный профиль л/с зарегистрированный в BRM  349.879
          SELECT A.ACCOUNT_ID, AP.PROFILE_ID, AP.CONTRACT_ID, 
                 AP.CONTRACTOR_ID, AP.BRANCH_ID, AP.AGENT_ID,
                 AP.CUSTOMER_ID, AP.SUBSCRIBER_ID, AP.CONTRACTOR_BANK_ID,
                 AP.VAT, AP.KPP
            FROM ACCOUNT_PROFILE_T AP, A
           WHERE AP.ACCOUNT_ID = A.ACCOUNT_ID
             AND AP.ACTUAL    = 'Y'
      ),
      C AS ( -- догогвор с компанией, которому принадлежит л/с 349.879
          SELECT AP.ACCOUNT_ID, 
                 CT.CONTRACT_ID, CT.CONTRACT_NO,
                 CT.DATE_FROM, CT.DATE_TO, 
                 CM.COMPANY_NAME, CM.ERP_CODE, CM.INN, CM.KPP, 
                 CT.CLIENT_ID, CL.CLIENT_NAME,
                 CT.MARKET_SEGMENT_ID, CT.CLIENT_TYPE_ID,
                 DU.NAME CFU, DO.NAME CFO
            FROM CONTRACT_T CT, COMPANY_T CM, CLIENT_T CL, AP,
                 DICTIONARY_T DU, DICTIONARY_T DO
           WHERE AP.CONTRACT_ID = CT.CONTRACT_ID
             AND AP.CONTRACT_ID = CM.CONTRACT_ID(+)
             AND CM.ACTUAL(+)   = 'Y'
             AND CT.CLIENT_ID   = CL.CLIENT_ID
             AND CT.CFU_ID      = DU.KEY_ID(+) 
             AND CT.CFO_ID      = DO.KEY_ID(+)
      ),
      AP_CSS AS ( -- профиль ЦСС
          SELECT AP.ACCOUNT_ID, AP.CONTRACTOR_ID, AP.BRANCH_ID, AP.AGENT_ID, CT.CONTRACTOR AGENT
            FROM AP, CONTRACTOR_T CT
           WHERE AP.BRANCH_ID = 200
             AND AP.AGENT_ID  = CT.CONTRACTOR_ID
      ),
      D_SEG AS ( -- сегмент рынка (справочник)
          SELECT KEY_ID MSEG_ID, NAME MSEG_NAME FROM PIN.DICTIONARY_T D 
           WHERE D.PARENT_ID = 63
      ), 
      D_TYP AS ( -- тип клиента (справочник)
          SELECT KEY_ID TYPE_ID, NAME TYPE_NAME FROM PIN.DICTIONARY_T D 
           WHERE D.PARENT_ID = 64
      ),
      D_DLV AS ( -- способ доставки счета в формате csv
          SELECT ACCOUNT_ID, MAX(DLVS) DLV_METHOD
          FROM (
              SELECT ACCOUNT_ID, DLV, LTRIM(SYS_CONNECT_BY_PATH(DLV,','),',') DLVs
                FROM (
                  SELECT ACCOUNT_ID, DLV, DELIVERY_METHOD_ID,
                         LAG(DELIVERY_METHOD_ID) OVER (PARTITION BY ACCOUNT_ID ORDER BY DLV) PREV_DLV_ID
                    FROM (
                      SELECT AD.ACCOUNT_ID, AD.DELIVERY_METHOD_ID, D.NAME DLV -- ||'-'||COUNT(*) DLV  
                        FROM ACCOUNT_DOCUMENTS_T AD, DICTIONARY_T D
                       WHERE AD.DELIVERY_METHOD_ID = D.KEY_ID
                         AND D.PARENT_ID    = 65
                         AND AD.DOC_BILL    = 'Y'
                       GROUP BY AD.ACCOUNT_ID, AD.DELIVERY_METHOD_ID, D.NAME
                  )
              )
              START WITH PREV_DLV_ID IS NULL
              CONNECT BY PRIOR DELIVERY_METHOD_ID = PREV_DLV_ID AND ACCOUNT_ID = PRIOR ACCOUNT_ID 
          )
          GROUP BY ACCOUNT_ID
      ), 
      A_DLV AS ( -- адрес доставки
          SELECT * 
          FROM (
              SELECT ROW_NUMBER() OVER (PARTITION BY AC.ACCOUNT_ID ORDER BY AC.DATE_FROM DESC) RN, 
                     AC.ACCOUNT_ID, 
                     AC.COUNTRY DLV_COUNTRY,
                     AC.ZIP     DLV_ZIP,
                     AC.STATE   DLV_STATE,
                     AC.CITY    DLV_CITY,
                     AC.ADDRESS DLV_ADDRESS,
                     AC.PERSON  DLV_PERSON,
                     AC.PHONES  DLV_PHONES,
                     AC.EMAIL   DLV_EMAIL,
                     AC.NOTES   DLV_NOTES
                FROM ACCOUNT_CONTACT_T AC, ACCOUNT_T A
               WHERE AC.ACCOUNT_ID > 0 
                 AND AC.ADDRESS_TYPE = 'DLV' 
                 AND AC.ACCOUNT_ID = A.ACCOUNT_ID
                 AND A.ACCOUNT_TYPE = 'J'
          ) 
          WHERE RN = 1
      ),
      ACC AS ( -- информация по л/с (технологическая)
          SELECT DISTINCT 
                 A.ACCOUNT_ID, 
                 A.ACCOUNT_TYPE, 
                 A.ACCOUNT_NO, 
                 C.CONTRACT_ID, 
                 C.CONTRACT_NO, 
                 DECODE(A.ACCOUNT_TYPE, 
                        'P', SU.LAST_NAME ||' '||SU.FIRST_NAME||' '||SU.MIDDLE_NAME, 
                        'J', C.COMPANY_NAME , NULL) CUST_NAME,
                 C.CLIENT_ID, 
                 C.DATE_FROM, 
                 C.DATE_TO, 
                 D_SEG.MSEG_ID, 
                 D_SEG.MSEG_NAME, 
                 D_TYP.TYPE_ID, 
                 D_TYP.TYPE_NAME,
                 AP_CSS.AGENT_ID BRAND_ID, 
                 AP_CSS.AGENT    BRAND, 
                 CT.CONTRACTOR_ID, 
                 CT.CONTRACTOR, 
                 CB.BANK_ID, 
                 CB.NOTES BANK,
                 XTTK.CONTRACTOR_ID XTTK_ID, 
                 XTTK.CONTRACTOR XTTK, 
                 XTTK.EXTERNAL_ID XTTK_OBJ_ID0,
                 AG.CONTRACTOR_ID AGENT_ID, 
                 AG.CONTRACTOR AGENT, 
                 AG.EXTERNAL_ID AGENT_OBJ_ID0,
                 A.CURRENCY_ID, 
                 AP.VAT, 
                 C.ERP_CODE, 
                 C.INN, 
                 NVL(AP.KPP, C.KPP) KPP,
                 A_DLV.DLV_COUNTRY, 
                 A_DLV.DLV_ZIP, 
                 A_DLV.DLV_STATE, 
                 A_DLV.DLV_CITY, 
                 A_DLV.DLV_ADDRESS, 
                 A_DLV.DLV_PERSON, 
                 A_DLV.DLV_PHONES, 
                 A_DLV.DLV_EMAIL, 
                 A_DLV.DLV_NOTES, 
                 A.BILLING_ID,
                 PK402_BCR_DATA.Get_sales_curator (
                                   XTTK.CONTRACTOR_ID, 
                                   AG.CONTRACTOR_ID, 
                                   C.CONTRACT_ID, 
                                   A.ACCOUNT_ID, 
                                   NULL,
                                   v_period_to
                               ) SALES_MANAGER, 
                 D_DLV.DLV_METHOD,
                 C.CFU,
                 C.CFO
            FROM A, 
                 AP, 
                 C,
                 D_SEG, 
                 D_TYP, 
                 D_DLV, 
                 A_DLV,
                 AP_CSS,
                 SUBSCRIBER_T SU,
                 CONTRACTOR_T CT, 
                 CONTRACTOR_T XTTK, 
                 CONTRACTOR_T AG, 
                 CONTRACTOR_BANK_T CB
           WHERE A.ACCOUNT_ID        = AP.ACCOUNT_ID
             AND A.ACCOUNT_ID        = AP_CSS.ACCOUNT_ID(+)
             AND A.ACCOUNT_ID        = C.ACCOUNT_ID
             AND A.ACCOUNT_ID        = D_DLV.ACCOUNT_ID(+)
             AND A.ACCOUNT_ID        = A_DLV.ACCOUNT_ID(+)
             AND AP.SUBSCRIBER_ID    = SU.SUBSCRIBER_ID(+)
             AND AP.CONTRACTOR_ID    = CT.CONTRACTOR_ID(+)
             AND DECODE(AP.BRANCH_ID,200,11,AP.BRANCH_ID) = XTTK.CONTRACTOR_ID(+)
             AND DECODE(AP.BRANCH_ID,200,AP.BRANCH_ID,NVL(AP.AGENT_ID,AP.BRANCH_ID)) = AG.CONTRACTOR_ID(+)
             AND AP.CONTRACTOR_BANK_ID = CB.BANK_ID(+)
             AND C.MARKET_SEGMENT_ID = D_SEG.MSEG_ID(+)
             AND C.CLIENT_TYPE_ID    = D_TYP.TYPE_ID(+)
      )
      SELECT -- информация по л/с (официальное представление)
            ACCOUNT_ID, 
            ACCOUNT_TYPE,
            ACCOUNT_NO,
            CONTRACT_ID,
            CONTRACT_NO,
            CUST_NAME,
            CLIENT_ID,
            DATE_FROM,
            DATE_TO,
            MSEG_ID,
            MSEG_NAME,
            TYPE_ID,
            TYPE_NAME,
            BRAND_ID,
            BRAND,
            CONTRACTOR_ID,
            CONTRACTOR,
            BANK_ID,
            BANK,
            XTTK_ID,
            XTTK,
            XTTK_OBJ_ID0,
            AGENT_ID,
            AGENT,
            AGENT_OBJ_ID0,
            CURRENCY_ID,
            VAT,
            ERP_CODE,
            INN,
            KPP,
            DLV_COUNTRY,
            DLV_ZIP,
            DLV_STATE,
            DLV_CITY,
            DLV_ADDRESS,
            DLV_PERSON,
            DLV_PHONES,
            DLV_EMAIL,
            DLV_NOTES,
            BILLING_ID,
            SALES_MANAGER,
            DLV_METHOD,
            CFU,
            CFO
        FROM ACC
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.BRM_ACCOUNT_T: '||v_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
           
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------------------
-- CLIENTS
-- --------------------------------------------------------------------------------

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- INFRANET.BRM_CLIENT_T;
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Brm_clients_to_table
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Brm_client_to_table';
    v_count       INTEGER;
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );


    DELETE FROM INFRANET.BRM_CLIENT_T;

    INSERT INTO INFRANET.BRM_CLIENT_T (
        CLIENT_ID, CLIENT_NAME
    )
    SELECT CLIENT_ID,
           CLIENT_NAME 
      FROM CLIENT_T
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.BRM_CLIENT_T: '||v_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;       
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------------------
-- PHONES
-- --------------------------------------------------------------------------------
PROCEDURE Brm_phones_to_file
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Brm_phones_to_file';
    v_output      UTL_FILE.file_type;
    v_dir         VARCHAR2(100)      := с_BCR_DIR;
    v_file_name   VARCHAR2(100 CHAR) := 'phones.csv';
    v_file_tmp    VARCHAR2(100 CHAR) := 'phones.tmp';
    v_count       INTEGER;
    v_hdr         VARCHAR2(2000);
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ------------------------------------------------------------------ --
    -- записываем информацию в Файл
    -- ------------------------------------------------------------------ --
    v_count := 0;
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W' );

    -- сохраняем заголовок
    v_hdr := 'ORDER_ID;ORDER_NO;PHONE_NUMBER;DATE_FROM;DATE_TO';
    
    UTL_FILE.put_line(v_output, CONVERT(v_hdr,'CL8MSWIN1251'));
    
    -- сохраняем строки
    FOR phone IN (
        SELECT O.ORDER_ID||';'|| 
               '"'||REPLACE(O.ORDER_NO,'"','""')||'";'||
               '"'||REPLACE(OP.PHONE_NUMBER,'"','""')||'";'||
               TO_CHAR(OP.DATE_FROM,'yyyy.mm.dd')||';'||
               TO_CHAR(OP.DATE_TO,'yyyy.mm.dd') 
            AS TXT
          FROM ORDER_PHONES_T OP, 
               ORDER_T O, 
               ACCOUNT_T A
         WHERE OP.ORDER_ID = O.ORDER_ID
           AND O.ACCOUNT_ID = A.ACCOUNT_ID         
--           AND A.BILLING_ID IN (2000,2001,2002,2003,2006,2009) 
           AND A.BILLING_ID != 2008 --IN (2000,2001,2002,2003,2005,2006,2007,2009) -- С.Макеев 03.03.2017
    ) LOOP
        UTL_FILE.put_line( v_output, CONVERT(phone.txt,'CL8MSWIN1251')) ;
        v_count := v_count + 1;
        
        IF MOD(v_count,10000) = 0 THEN
            Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


-- --------------------------------------------------------------------------------
-- DELIVERY
-- --------------------------------------------------------------------------------
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- INFRANET.BRM_DELIVERY_T;
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Brm_delivery_to_table
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Brm_delivery_to_table';
    v_count       INTEGER;
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    DELETE FROM INFRANET.BRM_DELIVERY_T;

    INSERT INTO INFRANET.BRM_DELIVERY_T (
        ACCOUNT_ID, DLV_ID, DLV_NAME, DLV_OBJ_ID0
    )
    SELECT A.ACCOUNT_ID, 
           D.KEY_ID DLV_ID, 
           D.NAME DLV_NAME, 
           D.EXTERNAL_ID DLV_OBJ_ID0 
      FROM ACCOUNT_DOCUMENTS_T a, DICTIONARY_T d
    WHERE D.PARENT_ID = 65
      AND A.DELIVERY_METHOD_ID = D.KEY_ID
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.BRM_DELIVERY_T: '||v_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;       
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------------------
-- Заполнить все таблицы BCR 
-- --------------------------------------------------------------------------------
PROCEDURE Load_bcr_tables(p_period_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Load_bcr_tables';
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);

    -- INFRANET.BRM_ITEM_T;
    Brm_items_to_table(p_period_id);

    -- INFRANET.BRM_BILL_T;
    Brm_bills_to_table(p_period_id);

    -- INFRANET.BRM_ORDER_T;
    Brm_orders_to_table;
    
    -- INFRANET.BRM_ACCOUNT_T;
    Brm_accounts_to_table;

    -- INFRANET.BRM_CLIENT_T;
    Brm_clients_to_table;
    
    -- INFRANET.BRM_DELIVERY_T;
    Brm_delivery_to_table;
    
    COMMIT;
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

END PK402_BCR_FILE;
/
