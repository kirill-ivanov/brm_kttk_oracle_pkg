CREATE OR REPLACE PACKAGE PK402_BCR_DATA
IS
    --
    -- � � � � � �   � � �   � � � � � � � �   �   B C R  ( �. �. ����� )
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK402_BCR_DATA';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- --------------------------------------------------------------------------------- --
    -- �������� ���������� ��������
    -- --------------------------------------------------------------------------------- --
    FUNCTION Get_sales_curator (
               p_branch_id     IN INTEGER,
               p_agent_id      IN INTEGER,
               p_contract_id   IN INTEGER,
               p_account_id    IN INTEGER,
               p_order_id      IN INTEGER,
               p_date          IN DATE
             ) RETURN VARCHAR2;

    -- --------------------------------------------------------------------------------- --
    -- ��� ��� ���� ��� BCR:
    -- 
    -- 1. ������� (����������): client_id, name
    -- 2. �/�(����������): account_id, client_id, N_�/�, N_��������, ���� ����������, �����, ��������, ������, ���, ��/��
    -- 3. ������ c ������������ (����������): service_id, ������, ����������
    -- 4. ����� �� ������ (item�): item_poid, ����� �����, ���� �����, account_id, ������, �����, ����� ��� ���, ���, ����� � ���, ������������ �������, ����� ������, ���� ������, service_id, ��� ������� (ont,rec,usg), ����� �������������, �������� �������������, �����, xTTK, ������ �������� ������, ������� �������������, ��������, ����� ��������� ������.
    -- 
    -- With best regards,
    -- Alexander Yu. Gurov
    -- Head of Data Collection and Processing Management
    -- 06.06.2014 16:34
    -- --------------------------------------------------------------------------------- --
    -- 1. ����������� / �������� (����������): client_id, name
    PROCEDURE Contractors( 
                   p_recordset OUT t_refc
               );

    -- 2. ����������� / ������� (����������): client_id, name
    PROCEDURE Customers( 
                   p_recordset OUT t_refc
               );

    -- 3. �/�(����������): account_id, client_id, N_�/�, N_��������, ���� ����������, 
    --    �����, ��������, ������, ���, ��/��
    PROCEDURE Accounts( 
                   p_recordset OUT t_refc
               );

    -- 4. ���������� �����
    PROCEDURE Services( 
                   p_recordset OUT t_refc
               );

    -- 5. ���������� ���������� ��� ������ (NULL - ��� ���� �����
    PROCEDURE Service_subservices( 
                   p_recordset OUT t_refc,
                   p_service_id IN INTEGER DEFAULT NULL
               );


    -- 6. ����� �� ������ (item�): 
    -- item_poid, ����� �����, ���� �����, account_id, ������, �����, 
    -- ����� ��� ���, ���, ����� � ���, ������������ �������, 
    -- ����� ������, ���� ������, service_id, ��� ������� (ont,rec,usg), 
    -- ����� �������������, �������� �������������, 
    -- �����, xTTK, ������ �������� ������, ������� �������������, 
    -- ��������, ����� ��������� ������.
    PROCEDURE Bills( 
                   p_recordset OUT t_refc,
                   p_period_id IN INTEGER
               );

    -- 7. ������ �������� ������ �� ��������� ������:
    PROCEDURE Delivery_address( 
                   p_recordset OUT t_refc,
                   p_period_id IN INTEGER
               );

    -- 8. ���������� ������ � �������
    PROCEDURE Order_phones( 
                   p_recordset OUT t_refc
               );
    
END PK402_BCR_DATA;
/
CREATE OR REPLACE PACKAGE BODY PK402_BCR_DATA
IS

-- --------------------------------------------------------------------------------- --
-- �������� ���������� ��������
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
             AND NVL(p_date,SYSDATE) BETWEEN SC.DATE_FROM AND NVL(SC.DATE_TO,SYSDATE) 
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

-- --------------------------------------------------------------------------------- --
-- 1. ����������� / �������� (����������): client_id, name
-- --------------------------------------------------------------------------------- --
PROCEDURE Contractors( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Contractors';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT LEVEL LVL, 
               CONTRACTOR_ID, PARENT_ID,ERP_CODE, INN, KPP, CONTRACTOR, SHORT_NAME,  
               CONTRACTOR_TYPE, EXTERNAL_ID, NOTES
          FROM CONTRACTOR_T CT
        CONNECT BY PRIOR CONTRACTOR_ID = PARENT_ID
        START WITH PARENT_ID IS NULL;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- --------------------------------------------------------------------------------- --
-- 2. ����������� / ������� (����������): client_id, name
-- --------------------------------------------------------------------------------- --
PROCEDURE Customers( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Customers';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
      SELECT CUSTOMER_ID, PARENT_ID, ERP_CODE, INN, KPP, CUSTOMER, SHORT_NAME, NOTES 
        FROM CUSTOMER_T;
        /*    
        WITH CLIENT AS (
          SELECT DISTINCT CL.CLIENT_ID, CL.CLIENT_NAME, AP.CUSTOMER_ID
            FROM CLIENT_T CL, CONTRACT_T C, ACCOUNT_PROFILE_T AP, ACCOUNT_T A
           WHERE CL.CLIENT_ID = C.CLIENT_ID
             AND C.CONTRACT_ID = AP.CONTRACT_ID 
             AND A.ACCOUNT_ID  = AP.ACCOUNT_ID
             AND A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_J
        )
        SELECT DISTINCT CLIENT.CLIENT_ID, CLIENT.CLIENT_NAME, 
                        CU.CUSTOMER_ID, CU.CUSTOMER, ERP_CODE, INN, KPP,
                        COUNT(*) OVER (PARTITION BY CLIENT.CLIENT_NAME ) CLIENT_NO,
                        COUNT(*) OVER (PARTITION BY CU.CUSTOMER) CUSTOMER_NO
          FROM CLIENT, CUSTOMER_T CU
         WHERE CLIENT.CUSTOMER_ID(+) = CU.CUSTOMER_ID  
        ORDER BY CLIENT.CLIENT_NAME, CU.CUSTOMER;
        */
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- --------------------------------------------------------------------------------- --
-- 3. �/�(����������): account_id, client_id, N_�/�, N_��������, ���� ����������, 
--    �����, ��������, ������, ���, ��/��
-- --------------------------------------------------------------------------------- --
PROCEDURE Accounts( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Accounts';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
      WITH D_SEG AS (
          SELECT KEY_ID MSEG_ID, NAME MSEG_NAME FROM DICTIONARY_T D 
           WHERE D.PARENT_ID = 63
      )
      SELECT A.ACCOUNT_ID, A.ACCOUNT_NO, CU.CUSTOMER_ID, CU.CUSTOMER, 
             SU.SUBSCRIBER_ID, SU.LAST_NAME ||' '||SU.FIRST_NAME||' '||SU.MIDDLE_NAME SUBSCRIBER, 
             C.CONTRACT_ID, C.CONTRACT_NO, C.DATE_FROM, C.DATE_TO, BR.BRAND_ID, BR.BRAND, 
             A.CURRENCY_ID, AP.VAT, A.ACCOUNT_TYPE, C.MARKET_SEGMENT_ID, D_SEG.MSEG_NAME 
        FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, BRAND_T BR, CONTRACT_T C, CUSTOMER_T CU, SUBSCRIBER_T SU, D_SEG
       WHERE AP.ACCOUNT_ID  = A.ACCOUNT_ID 
         AND AP.BRAND_ID    = BR.BRAND_ID
         AND AP.CONTRACT_ID = C.CONTRACT_ID
         AND AP.CUSTOMER_ID = CU.CUSTOMER_ID(+)
         AND AP.SUBSCRIBER_ID = SU.SUBSCRIBER_ID(+)
         AND C.MARKET_SEGMENT_ID = D_SEG.MSEG_ID(+)
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- --------------------------------------------------------------------------------- --
-- 4. ���������� �����
-- --------------------------------------------------------------------------------- --
PROCEDURE Services( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Services';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
      SELECT LEVEL LVL, SERVICE_ID, SERVICE, SERVICE_CODE, ERP_PRODCODE, SERVICE_SHORT, PARENT_ID 
        FROM SERVICE_T
      CONNECT BY PRIOR SERVICE_ID = PARENT_ID
      START WITH PARENT_ID IS NULL;
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- --------------------------------------------------------------------------------- --
-- 5. ���������� ���������� ��� ������ (NULL - ��� ���� �����)
-- --------------------------------------------------------------------------------- --
PROCEDURE Service_subservices( 
               p_recordset OUT t_refc,
               p_service_id IN INTEGER DEFAULT NULL
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Service_subservices';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
      WITH SS AS (
      SELECT S2S.SERVICE_ID, SS.SUBSERVICE_ID, SS.SUBSERVICE_KEY, SS.SUBSERVICE 
        FROM SERVICE_SUBSERVICE_T S2S, SUBSERVICE_T SS
       WHERE S2S.SUBSERVICE_ID = SS.SUBSERVICE_ID
      )
      SELECT S.SERVICE_ID, S.SERVICE_CODE, S.SERVICE,
             SS.SUBSERVICE_ID, SS.SUBSERVICE_KEY, SS.SUBSERVICE
        FROM  SERVICE_T S, SS
       WHERE S.SERVICE_ID = SS.SERVICE_ID(+)
         AND (p_service_id IS NULL OR S.SERVICE_ID = p_service_id);
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- --------------------------------------------------------------------------------- --
-- 6. ����� �� ������ (item�): 
-- item_poid, ����� �����, ���� �����, account_id, ������, �����, 
-- ����� ��� ���, ���, ����� � ���, ������������ �������, 
-- ����� ������, ���� ������, service_id, ��� ������� (ont,rec,usg), 
-- ����� �������������, �������� �������������, 
-- �����, xTTK, ������ �������� ������, ������� �������������, 
-- ��������, ����� ��������� ������.
-- --------------------------------------------------------------------------------- --
PROCEDURE Bills( 
               p_recordset OUT t_refc,
               p_period_id IN INTEGER
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Bills';
    v_retcode       INTEGER;
    v_period_from   DATE := Pk04_Period.Period_from(p_period_id);
    v_period_to     DATE := Pk04_Period.Period_to(p_period_id);
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        WITH D AS(
          SELECT REP_PERIOD_ID, ITEM_ID, SUM(MINS_SUM) MINS
            FROM detail_mmts_t_fiz
          GROUP BY REP_PERIOD_ID, ITEM_ID
          UNION SELECT REP_PERIOD_ID, ITEM_ID, SUM(MINUTES) MINS
            FROM detail_mmts_t_jur
          GROUP BY REP_PERIOD_ID, ITEM_ID),
         PF AS (
            -- ������ � �������
            SELECT AP.ACCOUNT_ID,
                   AP.CONTRACT_ID,
                   AP.CONTRACTOR_ID, CA.CONTRACTOR, 
                   AP.BRAND_ID,      BR.BRAND,
                   AP.BRANCH_ID,     BRANCH.CONTRACTOR BRANCH,
                   AP.AGENT_ID,      AGENT.CONTRACTOR  AGENT
              FROM ACCOUNT_PROFILE_T AP, BRAND_T BR, 
                   CONTRACTOR_T BRANCH, CONTRACTOR_T AGENT, CONTRACTOR_T CA  
             WHERE AP.CONTRACTOR_ID = CA.CONTRACTOR_ID 
               AND AP.BRAND_ID    = BR.BRAND_ID(+)
               AND AP.BRANCH_ID   = BRANCH.CONTRACTOR_ID(+)
               AND AP.AGENT_ID    = AGENT.CONTRACTOR_ID(+)
               AND AP.DATE_FROM <= v_period_from
               AND (AP.DATE_TO IS NULL OR v_period_to <= AP.DATE_TO )
        )
        SELECT B.REP_PERIOD_ID, 
               B.BILL_ID, 
               B.BILL_TYPE,
               B.BILL_NO, 
               B.BILL_DATE, 
               B.CURRENCY_ID, 
               B.TOTAL, 
               B.GROSS, 
               B.TAX,
               -- 
               D.MINS,
               --
               B.ACCOUNT_ID, 
               A.ACCOUNT_NO, 
               --
               O.ORDER_ID, 
               O.ORDER_NO, 
               O.DATE_FROM 
               O_DATE_FROM, 
               O.RATEPLAN_ID, 
               --
               IV.INV_ITEM_NAME,
               --
               I.SERVICE_ID, 
               I.SUBSERVICE_ID, 
               I.ITEM_TYPE, 
               I.CHARGE_TYPE,
               I.DATE_FROM I_DATE_FROM, 
               I.DATE_TO I_DATE_TO, 
               I.REP_GROSS, 
               I.REP_TAX,
               --
               PF.CONTRACTOR_ID, PF.CONTRACTOR, 
               PF.BRAND_ID,      PF.BRAND,
               PF.BRANCH_ID,     PF.BRANCH,
               PF.AGENT_ID,      PF.AGENT,
               PK402_BCR_DATA.Get_sales_curator (
                       p_branch_id => AP.BRANCH_ID,
                       p_agent_id => AP.AGENT_ID,
                       p_contract_id => PF.CONTRACT_ID,
                       p_account_id  => B.ACCOUNT_ID,
                       p_order_id    => O.ORDER_ID,
                       p_date        => v_period_to
                     ) SALES_MANAGER
          FROM BILL_T B, ACCOUNT_T A, ITEM_T I, ORDER_T O, PF, D, INVOICE_ITEM_T IV,
               ACCOUNT_PROFILE_T AP
         WHERE B.REP_PERIOD_ID = p_period_id
           AND B.ACCOUNT_ID    = A.ACCOUNT_ID
           AND B.BILL_ID       = I.BILL_ID
           AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
           AND O.ORDER_ID      = I.ORDER_ID
           AND B.ACCOUNT_ID    = PF.ACCOUNT_ID
           AND I.REP_PERIOD_ID = D.REP_PERIOD_ID(+)
           AND I.ITEM_ID       = D.ITEM_ID(+)
           AND I.REP_PERIOD_ID = IV.REP_PERIOD_ID
           AND I.INV_ITEM_ID   = IV.INV_ITEM_ID
           AND AP.ACCOUNT_ID   = A.ACCOUNT_ID
           AND AP.DATE_FROM < B.BILL_DATE
           AND (AP.DATE_TO IS NULL OR B.BILL_DATE < AP.DATE_TO );
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- --------------------------------------------------------------------------------- --
-- 7. ������ �������� ������ �� ��������� ������:
-- --------------------------------------------------------------------------------- --
PROCEDURE Delivery_address( 
               p_recordset OUT t_refc,
               p_period_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delivery_address';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
      SELECT B.REP_PERIOD_ID, B.ACCOUNT_ID, B.BILL_ID, B.BILL_NO, B.BILL_DATE, 
             C.CONTRACT_NO,
             AD.COUNTRY DLV_COUNTRY, AD.ZIP, AD.STATE, AD.CITY,AD.ADDRESS DLV_ADDRES, 
             AD.PERSON DLV_PERSON, AD.PHONES DLV_PHONES, AD.EMAIL DLV_EMAIL, AD.NOTES DLV_NOTES  
        FROM BILL_T B, ACCOUNT_CONTACT_T AD, ACCOUNT_PROFILE_T AP, CONTRACT_T C
       WHERE B.ACCOUNT_ID = AD.ACCOUNT_ID
         AND AD.ADDRESS_TYPE = PK00_CONST.c_ADDR_TYPE_DLV
         AND REP_PERIOD_ID = p_period_id 
         AND AP.ACCOUNT_ID = B.ACCOUNT_ID
         AND AP.DATE_FROM <= B.BILL_DATE
         AND (AP.DATE_TO IS NULL OR B.BILL_DATE <= AP.DATE_TO )
         AND AP.CONTRACT_ID = C.CONTRACT_ID
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- --------------------------------------------------------------------------------- --
-- 8. ���������� ������ � �������
-- --------------------------------------------------------------------------------- --
PROCEDURE Order_phones( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Order_phones';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
      SELECT O.ORDER_ID, O.ORDER_NO, OP.PHONE_NUMBER, OP.DATE_FROM, OP.DATE_TO 
        FROM ORDER_PHONES_T OP, ORDER_T O
       WHERE OP.ORDER_ID = O.ORDER_ID
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- --------------------------------------------------------------------------------- --
-- 9. ���������� �� �������� �� ������
-- ��������� ������� y� 201407 - �������������� ������� ����� �� �/� 'ACC000000002'
-- � 1 �������� �������, ��� �������� �����
-- --------------------------------------------------------------------------------- --
PROCEDURE Payments( 
               p_recordset OUT t_refc,
               p_period_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delivery_address';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
      SELECT A.ACCOUNT_ID, A.ACCOUNT_NO, 
             PS.PAYSYSTEM_ID, PS.PAYSYSTEM_NAME, 
             P.REP_PERIOD_ID, P.PAYMENT_ID, P.PAYMENT_DATE,
             P.DOC_ID, P.PAY_DESCR, P.RECVD, P.TRANSFERED, P.REFUND, P.BALANCE
        FROM PAYMENT_T P, PAYSYSTEM_T PS, ACCOUNT_T A
       WHERE P.REP_PERIOD_ID = 201407
         AND P.PAYSYSTEM_ID = PS.PAYSYSTEM_ID
         AND P.ACCOUNT_ID = A.ACCOUNT_ID
         --AND A.ACCOUNT_NO = 'ACC000000002' -- ��� ��������������
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;






END PK402_BCR_DATA;
/
