CREATE OR REPLACE PACKAGE PK104_TOPS
IS
    --
    -- ����� ��� ������ � ���������� �������������� ���������� ����� ( ���� )
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK104_TOPS';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- ID ������������ ���� (DICTIONARY_T)
    �_RATESYS_ID CONSTANT INTEGER := Pk00_Const.c_RATESYS_TOPS_ID;

    -- ID ������ ������� � ������� �����
    c_SERVICE_ID CONSTANT INTEGER := Pk00_Const.c_SERVICE_CALL_LZ;

    -- �������� ������ � ���������� ����������� ������ (SUBSERVICE_ID)
    --   - ��� ������ ���������� ����������
    PROCEDURE Subservice_list( 
                   p_recordset  OUT t_refc, 
                   p_service_id IN INTEGER DEFAULT PK104_TOPS.c_SERVICE_ID
               );

    -- �������� ����������� ������ (SUBSERVICE_ID)
    --   - ��� ������ ���������� ����������
    PROCEDURE Add_Subservice( 
                   p_subservice_id  IN INTEGER,    -- ID
                   p_subservice_key IN VARCHAR2,   -- ��� - ������� ��� ���������� ������
                   p_subservice     IN VARCHAR2,   -- ������ ��� ���������� ������
                   p_service_id     IN INTEGER DEFAULT PK104_TOPS.c_SERVICE_ID
               );

    -- �������� ������ ������������ ��� ���������� xTTK
    PROCEDURE Switch_list(
                   p_recordset  OUT t_refc, 
                   p_xttk_id    IN INTEGER       -- CONTRACTOR_T.CONTRACTOR_ID
               );

    -- �������� ���������� � ������� �����
    PROCEDURE Account_info (
                   p_recordset  OUT t_refc, 
                   p_account_no IN VARCHAR2      -- ACCOUNT_T.ACCOUNT_NO
               );

    -- �������� ���������� �� ��������
    PROCEDURE Contract_info (
                   p_recordset  OUT t_refc, 
                   p_contract_no IN VARCHAR2     -- CONTRACT_T.CONTRACT_NO
               );

    -- ������ ������� �� �/�
    PROCEDURE Order_list (
                   p_recordset  OUT t_refc, 
                   p_account_id IN INTEGER       -- ACCOUNT_T.ACCOUNT_ID
               );

    -- ������ ����������� ����� �� ������
    PROCEDURE Order_body (
                   p_recordset  OUT t_refc, 
                   p_order_id   IN INTEGER       -- ORDER_T.ORDER_ID
               );

    -- ������ ��������� ����� ������������ �� ������
    PROCEDURE Order_TG (
                   p_recordset  OUT t_refc, 
                   p_order_id   IN INTEGER       -- ORDER_T.ORDER_ID
               );

    -- ���� ������� ��� �������� ������������� �� ��������
    PROCEDURE Contract_pools (
                   p_recordset   OUT t_refc, 
                   p_contract_id IN INTEGER      -- CONTRACT_T.CONTRACT_ID
               );

    -- ������ ����� ������� ��� �������� ������������� �� ��������
    PROCEDURE Pool_phones (
                   p_recordset   OUT t_refc, 
                   p_pool_id     IN INTEGER      -- OP_CONTRACT_POOLS_T.POOL_ID
               );
    
END PK104_TOPS;
/
CREATE OR REPLACE PACKAGE BODY PK104_TOPS
IS

-- �������� ������ � ���������� ����������� ������ (SUBSERVICE_ID)
--   - ��� ������ ���������� ����������
PROCEDURE Subservice_list( 
               p_recordset  OUT t_refc, 
               p_service_id IN INTEGER DEFAULT PK104_TOPS.c_SERVICE_ID
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Subservice_list';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT SS.SUBSERVICE_ID, SS.SUBSERVICE_KEY, SS.SUBSERVICE  
          FROM SERVICE_SUBSERVICE_T SSS, SUBSERVICE_T SS
         WHERE SSS.SERVICE_ID = p_service_id
           AND SSS.SUBSERVICE_ID = SS.SUBSERVICE_ID
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- �������� ����������� ������ (SUBSERVICE_ID)
--   - ��� ������ ���������� ����������
PROCEDURE Add_Subservice( 
               p_subservice_id  IN INTEGER,    -- ID
               p_subservice_key IN VARCHAR2,   -- ��� - ������� ��� ���������� ������
               p_subservice     IN VARCHAR2,   -- ������ ��� ���������� ������
               p_service_id     IN INTEGER DEFAULT PK104_TOPS.c_SERVICE_ID
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Add_Subservice';
    v_retcode    INTEGER;
BEGIN
    -- ��������� ���������
    INSERT INTO SUBSERVICE_T SS (SS.SUBSERVICE_ID, SS.SUBSERVICE_KEY, SS.SUBSERVICE)
    VALUES (p_subservice_id, p_subservice_key, p_subservice);
    -- ����������� �������� � ������ 
    INSERT INTO SERVICE_SUBSERVICE_T(SERVICE_ID, SUBSERVICE_ID)
    VALUES (p_service_id, p_subservice_id);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- �������� ������ ������������ ��� ���������� xTTK
PROCEDURE Switch_list(
               p_recordset  OUT t_refc, 
               p_xttk_id    IN INTEGER       -- CONTRACTOR_T.CONTRACTOR_ID
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Switch_list';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT SW.SWITCH_ID, SW.SWITCH_NAME, C.CONTRACTOR_ID, C.SHORT_NAME 
          FROM SWITCH_T SW, CONTRACTOR_T C
         WHERE SW.CONTRACTOR_ID = C.CONTRACTOR_ID
           AND C.CONTRACTOR_TYPE = 'BRAND'
           AND (p_xttk_id IS NULL OR C.CONTRACTOR_ID = p_xttk_id)
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- �������� ���������� � ������� �����
PROCEDURE Account_info (
               p_recordset  OUT t_refc, 
               p_account_no IN VARCHAR2      -- ACCOUNT_T.ACCOUNT_NO
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Account_info';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT A.ACCOUNT_ID, C.CONTRACT_ID, C.CONTRACT_NO, CU.CUSTOMER_ID, CU.SHORT_NAME,
               CT.CONTRACTOR_ID, CT.CONTRACTOR, 
               BR.CONTRACTOR_ID BR_CONTRACTOR_ID, BR.CONTRACTOR BR_CONTRACTOR,
               AG.CONTRACTOR_ID AG_CONTRACTOR_ID, AG.CONTRACTOR AG_CONTRACTOR
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, CUSTOMER_T CU,
               CONTRACTOR_T CT, CONTRACTOR_T BR, CONTRACTOR_T AG
         WHERE A.ACCOUNT_NO = p_account_no
           AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND AP.CUSTOMER_ID = CU.CUSTOMER_ID
           AND AP.CONTRACTOR_ID = CT.CONTRACTOR_ID
           AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
           AND AP.AGENT_ID  = AG.CONTRACTOR_ID(+)
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- �������� ���������� �� ��������
PROCEDURE Contract_info (
               p_recordset  OUT t_refc, 
               p_contract_no IN VARCHAR2      -- CONTRACT_T.CONTRACT_NO
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Contract_info';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT A.ACCOUNT_ID, C.CONTRACT_ID, C.CONTRACT_NO, CU.CUSTOMER_ID, CU.SHORT_NAME,
               CT.CONTRACTOR_ID, CT.CONTRACTOR, 
               BR.CONTRACTOR_ID BR_CONTRACTOR_ID, BR.CONTRACTOR BR_CONTRACTOR,
               AG.CONTRACTOR_ID AG_CONTRACTOR_ID, AG.CONTRACTOR AG_CONTRACTOR
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, CUSTOMER_T CU,
               CONTRACTOR_T CT, CONTRACTOR_T BR, CONTRACTOR_T AG
         WHERE C.CONTRACT_NO  = p_contract_no
           AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND AP.CUSTOMER_ID = CU.CUSTOMER_ID
           AND AP.CONTRACTOR_ID = CT.CONTRACTOR_ID
           AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
           AND AP.AGENT_ID  = AG.CONTRACTOR_ID(+)
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ������ ������� �� �/�
PROCEDURE Order_list (
               p_recordset  OUT t_refc, 
               p_account_id IN INTEGER       -- ACCOUNT_T.ACCOUNT_ID
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Order_list';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT O.ORDER_ID, O.ORDER_NO, O.DATE_FROM, O.DATE_TO, O.NOTE,
               O.SERVICE_ID, S.SERVICE, 
               O.RATEPLAN_ID, R.RATESYSTEM_ID, R.RATEPLAN_NAME, R.RATEPLAN_CODE
          FROM ORDER_T O, RATEPLAN_T R, SERVICE_T S
         WHERE O.SERVICE_ID = S.SERVICE_ID
           AND O.RATEPLAN_ID= R.RATEPLAN_ID  
           AND O.ACCOUNT_ID = p_account_id
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ������ ����������� ����� �� ������
PROCEDURE Order_body (
               p_recordset  OUT t_refc, 
               p_order_id   IN INTEGER       -- ORDER_T.ORDER_ID
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Order_body';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT OB.SUBSERVICE_ID, SS.SUBSERVICE_KEY, SS.SUBSERVICE, 
               OB.CHARGE_TYPE, OB.DATE_FROM, OB.DATE_TO 
          FROM ORDER_BODY_T OB, SUBSERVICE_T SS
         WHERE OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
           AND OB.ORDER_ID = p_order_id
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ������ ��������� ����� ������������ �� ������
PROCEDURE Order_TG (
               p_recordset  OUT t_refc, 
               p_order_id   IN INTEGER       -- ORDER_T.ORDER_ID
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Order_TG';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT TG.SWITCH_ID, SW.SWITCH_NAME, 
               TG.TRUNKGROUP, TG.TRUNKGROUP_NO, TG.DATE_FROM, TG.DATE_TO 
          FROM ORDER_SWTG_T TG, SWITCH_T SW
         WHERE TG.ORDER_ID = p_order_id
           AND TG.SWITCH_ID = SW.SWITCH_ID
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ���� ������� ��� �������� ������������� �� ��������
PROCEDURE Contract_pools (
               p_recordset   OUT t_refc, 
               p_contract_id IN INTEGER      -- CONTRACT_T.CONTRACT_NO
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Contract_pools';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT OP.POOL_ID, OP.POOL_CODE, OP.POOL_NAME 
          FROM OP_CONTRACT_POOLS_T OP
         WHERE OP.CONTRACT_ID = p_contract_id
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ������ ����� ������� ��� �������� ������������� �� ��������
PROCEDURE Pool_phones (
               p_recordset   OUT t_refc, 
               p_pool_id     IN INTEGER      -- OP_CONTRACT_POOLS_T.POOL_ID
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Pool_phones';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT OPP.PHONE_FROM, OPP.PHONE_TO, OPP.DATE_FROM, OPP.DATE_TO 
          FROM OP_POOL_PHONES_T OPP
         WHERE OPP.POOL_ID = pool_id
         ORDER BY OPP.PHONE_FROM
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


END PK104_TOPS;
/
