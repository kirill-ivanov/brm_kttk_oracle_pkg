CREATE OR REPLACE PACKAGE PK27_AUDIT_REPORT
IS
    --
    -- C������ ������ �� ���������� �������� � �������� �������������
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK26_OPERATOR_REPORT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    -- ������ ������������� � �������� ������� �� ������� �/��� ������� ������
    c_SERVICE_OPLOCL CONSTANT INTEGER := 7;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1. ������ ������� ����������
    --
    PROCEDURE Branch_List( 
                   p_recordset    OUT t_refc,
                   p_period_id    IN INTEGER
               );    

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2. ������ ��������� ���������� 
    --
    PROCEDURE Contract_List( 
                   p_recordset    OUT t_refc
               );

    -- ========================================================================= --
    --                    � � � � � � �   � � � � � �
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3. ����� ����-��
    PROCEDURE Client_info_j( 
                   p_recordset      OUT t_refc,
                   p_branch_id  IN VARCHAR2              
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 4. ���������� - ��
    PROCEDURE Bill_info_j( 
                   p_recordset    OUT t_refc,
                   p_branch_id  IN VARCHAR2
               );

END PK27_AUDIT_REPORT;
/
CREATE OR REPLACE PACKAGE BODY PK27_AUDIT_REPORT
IS
-- ========================================================================= --
-- C������ ������ �� �������� ��� ������ (������ �. ��������� - ������)
-- ========================================================================= --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 1. ������ ������� ����������
--
PROCEDURE Branch_List( 
               p_recordset    OUT t_refc,
               p_period_id    IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Branch_List';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR    
        WITH CL AS (
            SELECT A.ACCOUNT_ID, C.CONTRACT_ID, C.CONTRACT_NO,
                   CT.CONTRACTOR_ID, CT.CONTRACTOR, 
                   CS.CUSTOMER_ID, CS.CUSTOMER,
                   AP.PROFILE_ID, 
                   P.PERIOD_ID, P.PERIOD_FROM, P.PERIOD_TO 
              FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, 
                   CONTRACT_T C, CONTRACTOR_T CT, CUSTOMER_T CS, 
                   PERIOD_T P
             WHERE A.BILLING_ID IN(2006,2007,2008)
               AND A.STATUS       = 'B'
               AND P.PERIOD_ID    = p_period_id
               AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
               AND AP.DATE_FROM   < P.PERIOD_TO
               AND (AP.DATE_TO IS NULL OR AP.DATE_TO > P.PERIOD_FROM)
               AND AP.BRANCH_ID   = CT.CONTRACTOR_ID
               AND AP.CONTRACT_ID = C.CONTRACT_ID
               AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
               AND EXISTS (
                 SELECT * FROM ORDER_T O
                  WHERE O.ACCOUNT_ID = A.ACCOUNT_ID
                    AND O.SERVICE_ID IN (
                        7--, 128,142 --,125,140,127
                    )
               )
        ),
        EM AS (
           SELECT 
            CONTRACTOR_ID, EMAIL 
           FROM account_documents_t
              WHERE doc_detail = 'MEGOP_REPORT'
              AND DELIVERY_METHOD_ID = 6501
        )
        SELECT 
              CL.CONTRACTOR_ID, 
              CONTRACTOR, 
              COUNT(*) NUM, 
              EM.EMAIL
          FROM CL, EM
         WHERE CL.CONTRACTOR_ID = EM.CONTRACTOR_ID(+)
         GROUP BY CL.CONTRACTOR_ID, CONTRACTOR, EM.EMAIL
         ORDER BY CL.CONTRACTOR
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 2. ������ ��������� ���������� 
--
PROCEDURE Contract_List( 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Contract_List';
    v_retcode    INTEGER;
    v_period_id  INTEGER;
BEGIN
    -- �������� ID �����������
    SELECT PERIOD_ID 
      INTO v_period_id
      FROM (
        SELECT PERIOD_ID 
          FROM PERIOD_T P
         WHERE P.POSITION IN ('OPEN','BILL')
        ORDER BY DECODE('OPEN', 2,'BILL', 1,3)
     )
     WHERE ROWNUM = 1;
    -- ���������� ������
    OPEN p_recordset FOR
        WITH CL AS (
            SELECT A.ACCOUNT_ID, C.CONTRACT_ID, C.CONTRACT_NO,
                   CT.CONTRACTOR_ID, CT.CONTRACTOR, 
                   CS.CUSTOMER_ID, CS.CUSTOMER,
                   AP.PROFILE_ID, 
                   P.PERIOD_ID, P.PERIOD_FROM, P.PERIOD_TO 
              FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, 
                   CONTRACT_T C, CONTRACTOR_T CT, CUSTOMER_T CS, 
                   PERIOD_T P
             WHERE A.BILLING_ID IN(2006,2007,2008)
               AND A.STATUS       = 'B'
               AND P.PERIOD_ID    = v_period_id
               AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
               AND AP.DATE_FROM   < P.PERIOD_TO
               AND (AP.DATE_TO IS NULL OR AP.DATE_TO > P.PERIOD_FROM)
               AND AP.BRANCH_ID   = CT.CONTRACTOR_ID
               AND AP.CONTRACT_ID = C.CONTRACT_ID
               AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
               AND EXISTS (
                 SELECT * FROM ORDER_T O
                  WHERE O.ACCOUNT_ID = A.ACCOUNT_ID
                    AND O.SERVICE_ID IN (
                        7--, 128,142 --,125,140,127
                    )
               )
        )
        SELECT CONTRACTOR_ID, CUSTOMER_ID, CUSTOMER, CONTRACT_NO 
          FROM CL
         GROUP BY CONTRACTOR_ID, CUSTOMER_ID, CUSTOMER, CONTRACT_NO
         ORDER BY CONTRACTOR_ID, CUSTOMER_ID, CUSTOMER, CONTRACT_NO
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 3. ����� ����-��
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Client_info_j( 
               p_recordset  OUT t_refc,
               p_branch_id  IN VARCHAR2              
           )
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Client_info_j';
    v_retcode      INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        -- --------------------------------------------------------------- --
        /*
        ���������� ����� �������� � ���    ����� �������� ����� �������� � ������� ���    
        ����� ��������    ��� ��������    ���� ���������� �������� �� �������������� �����    
        �������� ��    ���/���    
        �������    �����    �����    ����� ����    ����� �������    ����� �����    ����� �����    
        ���  ������  - ��� , ���  (ADSL, FTTB) ��������� � ��.    
        ���� ������ ������ ��������    ���� ��������� ������ ��������    
        ������ �������� �����    
        ���� ���������� ������� �/�����    
        ������ ����������� �������� (�������): -��������; -����������������; - ���������������; -�����; - ����������� � �.�.    
        ���� ���������� ������� ��������    
        ������������ ��������� ����� ��������    
        ���� ���������� ��������� ����� ��������    
        ������ ��������
        */
        WITH A AS (
            SELECT A.ACCOUNT_ID, A.ACCOUNT_NO, A.STATUS ACCOUNT_STATUS, 
                   AP.CONTRACT_ID, AP.KPP 
              FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP
             WHERE A.ACCOUNT_ID     = AP.ACCOUNT_ID
               AND AP.BRANCH_ID     = p_branch_id 
        ), C AS (
            SELECT C.CONTRACT_ID, C.CONTRACT_NO, C.DATE_FROM CONTRACT_DATE, 
                   CM.COMPANY_NAME, CM.INN, CM.KPP, DT.NAME CONTRACT_TYPE 
              FROM CONTRACT_T C, COMPANY_T CM, DICTIONARY_T DT
             WHERE C.CONTRACT_ID = CM.CONTRACT_ID
               AND CM.ACTUAL = 'Y'
               AND C.CONTRACT_TYPE_ID = DT.KEY_ID(+)
        ), L AS (
            SELECT O.ORDER_ID, O.DATE_FROM STATUS_DATE, '������' ORDER_STATUS 
              FROM ORDER_T O
            UNION ALL
            SELECT L.ORDER_ID, L.DATE_FROM STATUS_DATE, L.LOCK_REASON 
              FROM ORDER_LOCK_T L
            UNION ALL
            SELECT L.ORDER_ID, L.DATE_TO STATUS_DATE, L.UNLOCK_REASON 
              FROM ORDER_LOCK_T L
        ), O AS (
            SELECT O.ACCOUNT_ID, O.ORDER_ID, O.ORDER_NO, S.SERVICE||'.'||SS.SUBSERVICE SERVICE, 
                   R.RATEPLAN_NAME, OB.DATE_FROM RATEPLAN_FROM 
              FROM ORDER_T O, ORDER_BODY_T OB, SERVICE_T S, SUBSERVICE_T SS, RATEPLAN_T R
             WHERE O.ORDER_ID = OB.ORDER_ID
               AND O.SERVICE_ID = S.SERVICE_ID
               AND OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
               AND OB.RATEPLAN_ID   = R.RATEPLAN_ID(+)
        ), I AS (
            SELECT I.ORDER_ID, MIN(DATE_FROM) FIRST_EVT, MAX(DATE_TO) LAST_EVT 
              FROM ITEM_T I
             GROUP BY I.ORDER_ID
        ), AD AS (
            SELECT ACCOUNT_ID, STATE REGION, CITY, ADDRESS
              FROM (
                SELECT ROW_NUMBER() OVER (PARTITION BY D.ACCOUNT_ID ORDER BY D.DATE_FROM DESC ) RN, 
                       D.ACCOUNT_ID, D.STATE, D.CITY, D.ADDRESS 
                  FROM ACCOUNT_CONTACT_T D  
                 WHERE D.ADDRESS_TYPE = 'DLV'
              )
             WHERE RN = 1
        )
        SELECT     
            A.ACCOUNT_ID      "ID �������� � ���",
            A.ACCOUNT_NO      "����� �/� � ���", 
            C.CONTRACT_NO     "����� ��������", 
            C.CONTRACT_TYPE   "��� ��������",
            C.CONTRACT_DATE   "���� ��������",
            C.COMPANY_NAME    "�������� ��", 
            C.INN             "���",
            NVL(A.KPP, C.KPP) "���", 
            AD.REGION         "�������", 
            AD.CITY           "�����", 
            AD.ADDRESS        "�����", 
            O.ORDER_NO        "����� ������",
            O.SERVICE         "������",
            I.FIRST_EVT       "������ �������", 
            I.LAST_EVT        "�������. �������", 
            A.ACCOUNT_STATUS  "������ �/�", 
            C.CONTRACT_DATE   "���� ������� �/�",
            L.ORDER_STATUS    "������ ������",
            L.STATUS_DATE     "���� ������� �.",
            O.RATEPLAN_NAME   "�������� ����", 
            O.RATEPLAN_FROM   "���� ��",
            '������� � �����' "������ ��������" 
          FROM A, O, L, I, AD, C
         WHERE A.ACCOUNT_ID = O.ACCOUNT_ID
           AND O.ORDER_ID   = L.ORDER_ID
           AND O.ORDER_ID   = I.ORDER_ID
           AND A.ACCOUNT_ID = AD.ACCOUNT_ID(+)
           AND A.CONTRACT_ID= C.CONTRACT_ID
        ORDER BY CONTRACT_NO, ACCOUNT_NO, ORDER_NO
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 4. ���������� - ��
PROCEDURE Bill_info_j( 
               p_recordset  OUT t_refc,
               p_branch_id  IN VARCHAR2
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_debt';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        /*
        ���������� ����� �������� � ���    
        ����� �������� ����� �������� � ������� ���    
        ����� ��������    ��� ��������    
        ���� ���������� �������� �� �������������� �����    
        �������� ��    
        ���/���    
        ���  ������  - ��� , ���  (ADSL, FTTB) ��������� � ��.    
        ������������ ��������� ����� ��������    
        --
        ������ ����������    
        ���� ����������    
        ����� ���������� (� ���)    
        ����� ���������� (��� ���)
        */
        WITH A AS (
            SELECT A.ACCOUNT_ID, A.ACCOUNT_NO, A.STATUS ACCOUNT_STATUS, 
                   AP.CONTRACT_ID, AP.KPP 
              FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP
             WHERE A.ACCOUNT_ID     = AP.ACCOUNT_ID
               AND AP.BRANCH_ID     = p_branch_id
        ), C AS (
            SELECT C.CONTRACT_ID, C.CONTRACT_NO, C.DATE_FROM CONTRACT_DATE, 
                   CM.COMPANY_NAME, CM.INN, CM.KPP, DT.NAME CONTRACT_TYPE 
              FROM CONTRACT_T C, COMPANY_T CM, DICTIONARY_T DT
             WHERE C.CONTRACT_ID = CM.CONTRACT_ID
               AND CM.ACTUAL = 'Y'
               AND C.CONTRACT_TYPE_ID = DT.KEY_ID(+)
        ), O AS (
            SELECT O.ACCOUNT_ID, O.ORDER_ID, O.ORDER_NO, S.SERVICE||'.'||SS.SUBSERVICE SERVICE, 
                   R.RATEPLAN_NAME, OB.DATE_FROM RATEPLAN_FROM 
              FROM ORDER_T O, ORDER_BODY_T OB, SERVICE_T S, SUBSERVICE_T SS, RATEPLAN_T R
             WHERE O.ORDER_ID = OB.ORDER_ID
               AND O.SERVICE_ID = S.SERVICE_ID
               AND OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
               AND OB.RATEPLAN_ID   = R.RATEPLAN_ID(+)
        ), B AS (
            SELECT B.ACCOUNT_ID, B.BILL_NO, B.REP_PERIOD_ID, B.BILL_DATE, I.ORDER_ID,
                   SUM(I.REP_GROSS) REP_GROSS, 
                   SUM(I.REP_TAX) REP_TAX 
              FROM BILL_T B, ITEM_T I
             WHERE B.REP_PERIOD_ID = I.REP_PERIOD_ID
               AND B.BILL_ID = I.BILL_ID
               AND B.BILL_STATUS IN ('READY','CLOSE')
             GROUP BY B.ACCOUNT_ID, B.BILL_NO, B.REP_PERIOD_ID, B.BILL_DATE, I.ORDER_ID
        )
        SELECT     
            A.ACCOUNT_ID      "ID �������� � ���",
            A.ACCOUNT_NO      "����� �/� � ���", 
            C.CONTRACT_NO     "����� ��������", 
            C.CONTRACT_TYPE   "��� ��������",
            C.CONTRACT_DATE   "���� ��������",
            C.COMPANY_NAME    "�������� ��", 
            C.INN             "���",
            NVL(A.KPP, C.KPP) "���", 
            O.ORDER_NO        "����� ������",
            O.SERVICE         "������",
            O.RATEPLAN_NAME   "�������� ����",
            B.REP_PERIOD_ID,
            B.BILL_NO, 
            B.BILL_DATE,
            B.REP_GROSS,
            B.REP_TAX
          FROM A, O, C, B
         WHERE A.ACCOUNT_ID = O.ACCOUNT_ID
           AND A.CONTRACT_ID= C.CONTRACT_ID
           AND B.ACCOUNT_ID = A.ACCOUNT_ID
           AND B.ORDER_ID   = O.ORDER_ID
        ORDER BY CONTRACT_NO, ACCOUNT_NO, ORDER_NO, REP_PERIOD_ID, BILL_NO
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK27_AUDIT_REPORT;
/
