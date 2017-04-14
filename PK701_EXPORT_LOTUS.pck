CREATE OR REPLACE PACKAGE PK701_EXPORT_LOTUS
IS
    --
    -- ����� ��� ������ ����� ����������� ���
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK701_EXPORT_LOTUS';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    --
    -- ��������� �������� ������ � ��������
    PROCEDURE Export_clients ( 
             p_recordset OUT t_refc,
             p_branch_id IN INTEGER
          );
    
    -- --------------------------------------------------------------------------------- --
    -- ��������� �������� ������ � �������� (�� - ����)
    -- --------------------------------------------------------------------------------- --
    --
    PROCEDURE Export_clients_RP_test ( 
             p_recordset OUT t_refc,
             p_branch_id IN INTEGER
          );  
    
    
END PK701_EXPORT_LOTUS;
/
CREATE OR REPLACE PACKAGE BODY PK701_EXPORT_LOTUS
IS

-- ========================================================================= --
--  ������� ������ � �����
-- ========================================================================= --
--
-- ��������� �������� ������ � ��������
--
PROCEDURE Export_clients ( 
             p_recordset OUT t_refc,
             p_branch_id IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_clients';
    v_retcode    INTEGER;
BEGIN
    -- ��������� ������
    OPEN p_recordset FOR
    SELECT 
        CT.CONTRACTOR BRANCH,       --  ������ 
        CS.ERP_CODE ERP_CODE,       -- ��� ������� � 1�
        CS.CUSTOMER CLIENT,         -- ��� ������
        CS.INN,                     -- ��� �������
        CS.KPP,                     -- ��� �������
        C.CONTRACT_NO CONTRACT_NO,  -- � ��������
        C.DATE_FROM CONTRACT_DATE,  -- ���� ��������
        A.ACCOUNT_ID,               -- id �������� ����� � �������� (��� ��������) 
        A.ACCOUNT_NO,               -- � �������� ����� � ��������
        O.ORDER_ID,                 -- id ������ � �������� (��� ��������)
        O.ORDER_NO,                 -- � ������ � ��������
        O.DATE_FROM ORDER_DATE_FROM,-- ���� ������ �������� ������
        O.DATE_TO   ORDER_DATE_TO,  -- ���� ��������� �������� ������ (����� ���������� �������� � 23:59:59)
        S.SERVICE_ID,               -- id ������ � ����������� �������� (��� ��������)
        S.SERVICE,                  -- ������ �� ����������� � ��������
        LS.SERVICE_OM,              -- ������ �� ����������� � �� �����
        SA.SRV_NAME SERVICE_ALIAS,  -- ������, ��� ��� ����� ���������� � �����
        OI.POINT_SRC,               -- �������� �����
        OI.POINT_DST,               -- �������� �����
        OI.SPEED_STR,               -- �������� ������
        OB.SUBSERVICE_ID,           -- id ���������� �������� ����������� � �������� (��� ��������)
        SS.SUBSERVICE,              -- ���������� ������ �� ����������� � ��������
        OB.CHARGE_TYPE,             -- ��� ����������: REC(������)/USG(������)/MIN(���������) (��� ��������)
        OB.RATE_VALUE,              -- ��������� / ������� �� ����������� ���������, � ����������� �� CHARGE_TYPE 
        OB.CURRENCY_ID,             -- id ������ � ��������, ��� � Portal6.5
        D.NAME RATE_RULE,           -- ������� ���������� ����������
        OB.DATE_FROM OB_DATE_FROM,  -- ���� ������ �������� ���������� ������
        OB.DATE_TO OB_DATE_TO,      -- ���� ��������� �������� ���������� ������
        OB.RATEPLAN_ID RATEPLAN_ID, -- id ��������� ����� � �������� (��� ��������)
        P.RATEPLAN_NAME,            -- ��� ��������� �����  � ��������
        DECODE(SCA.MANAGER_ID, NULL, MC.LAST_NAME||' '||MC.FIRST_NAME||MC.MIDDLE_NAME, 
                                     MA.LAST_NAME||' '||MA.FIRST_NAME||MA.MIDDLE_NAME) MANAGER -- ��������
      FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, ORDER_T O, ORDER_INFO_T OI, RATEPLAN_T P,  
           ORDER_BODY_T OB, SUBSERVICE_T SS, DICTIONARY_T D, 
           SERVICE_T S, EXT01_LOTUS_SERVICES LS, SERVICE_ALIAS_T SA,
           CONTRACT_T C, CONTRACTOR_T CT, CUSTOMER_T CS, SALE_CURATOR_T SCA, SALE_CURATOR_T SCC,
           MANAGER_T MC, MANAGER_T MA  
     WHERE A.BILLING_ID     = 2006  -- ������ ������� �� �������
       AND A.ACCOUNT_ID     = AP.ACCOUNT_ID
       AND A.ACCOUNT_ID     = O.ACCOUNT_ID
       AND O.SERVICE_ID     = S.SERVICE_ID
       AND S.SERVICE_ID     = LS.SERVICE_ID(+)
       --AND LS.SERVICE_OM NOT IN ('ZVI','��������� Free Phone','��������� ��/�� (� �������� ����������)')
       AND OI.ORDER_ID      = O.ORDER_ID
       AND O.ORDER_ID       = OB.ORDER_ID
       AND OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
       AND OB.RATE_RULE_ID  = D.KEY_ID(+)
       AND OB.RATEPLAN_ID   = P.RATEPLAN_ID(+)
       AND AP.CONTRACT_ID   = C.CONTRACT_ID
       AND AP.ACTUAL        = 'Y'
       AND AP.BRANCH_ID     = CT.CONTRACTOR_ID
       --AND CT.CONTRACTOR  != '������ ������������ ����� (�� ����� ������)'
       --AND CT.CONTRACTOR   = '������ ������������ ������ (��)'
       --AND CT.CONTRACTOR   = '������ ������������ ������ (��, ����)'
       --AND CT.CONTRACTOR   LIKE '%������%'
       AND CT.CONTRACTOR_ID = p_branch_id
       AND AP.CUSTOMER_ID   = CS.CUSTOMER_ID
       AND A.ACCOUNT_ID     = SCA.ACCOUNT_ID(+)
       AND C.CONTRACT_ID    = SCC.CONTRACT_ID(+)
       AND MC.MANAGER_ID(+) =  SCC.MANAGER_ID
       AND MA.MANAGER_ID(+) =  SCA.MANAGER_ID
       AND SA.SERVICE_ID(+) = O.SERVICE_ID
       AND SA.ACCOUNT_ID(+) = O.ACCOUNT_ID
       ORDER BY CT.CONTRACTOR,       --  ������ 
                CS.CUSTOMER,         -- ��� ������
                A.ACCOUNT_NO,
                O.ORDER_NO,
                S.SERVICE,
                SS.SUBSERVICE
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
-- ��������� �������� ������ � �������� (�� - ����)
-- --------------------------------------------------------------------------------- --
--
PROCEDURE Export_clients_RP_test ( 
             p_recordset OUT t_refc,
             p_branch_id IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_clients_RP_test';
    v_retcode    INTEGER;
BEGIN
    -- ��������� ������
    OPEN p_recordset FOR
    WITH ADR AS (
        SELECT A.ACCOUNT_ID, 
               AD.COUNTRY||', '||AD.ZIP||', '||AD.STATE||', '||AD.CITY||', '||AD.ADDRESS||', Email: '||AD.EMAIL DLV_ADDERESS, 
               AJ.COUNTRY||', '||AJ.ZIP||', '||AJ.STATE||', '||AJ.CITY||', '||AJ.ADDRESS JUR_ADDERESS
          FROM ACCOUNT_T A,
               ACCOUNT_CONTACT_T AD, 
               ACCOUNT_CONTACT_T AJ 
         WHERE A.ACCOUNT_ID IN (
            SELECT AP.ACCOUNT_ID 
              FROM ACCOUNT_PROFILE_T AP
             WHERE AP.BRANCH_ID = p_branch_id
         )
         AND AD.ADDRESS_TYPE(+) = 'DLV'
         AND AJ.ADDRESS_TYPE(+) = 'JUR'
         AND A.ACCOUNT_ID = AD.ACCOUNT_ID(+)
         AND A.ACCOUNT_ID = AJ.ACCOUNT_ID(+)
         AND AD.DATE_TO IS NULL
         AND AJ.DATE_TO IS NULL
    )
    SELECT 
        CT.CONTRACTOR BRANCH,       --  ������ 
        CS.ERP_CODE ERP_CODE,       -- ��� ������� � 1�
        CS.CUSTOMER CLIENT,         -- ��� ������
        CS.INN,                     -- ��� �������
        CS.KPP,                     -- ��� �������
        C.CONTRACT_NO CONTRACT_NO,  -- � ��������
        C.DATE_FROM CONTRACT_DATE,  -- ���� ��������
        A.ACCOUNT_ID,               -- id �������� ����� � �������� (��� ��������) 
        A.ACCOUNT_NO,               -- � �������� ����� � ��������
        O.ORDER_ID,                 -- id ������ � �������� (��� ��������)
        O.ORDER_NO,                 -- � ������ � ��������
        O.DATE_FROM ORDER_DATE_FROM,-- ���� ������ �������� ������
        O.DATE_TO   ORDER_DATE_TO,  -- ���� ��������� �������� ������ (����� ���������� �������� � 23:59:59)
        S.SERVICE_ID,               -- id ������ � ����������� �������� (��� ��������)
        S.SERVICE,                  -- ������ �� ����������� � ��������
        LS.SERVICE_OM,              -- ������ �� ����������� � �� �����
        SA.SRV_NAME SERVICE_ALIAS,  -- ������, ��� ��� ����� ���������� � �����
        OI.POINT_SRC,               -- �������� �����
        OI.POINT_DST,               -- �������� �����
        OI.SPEED_STR,               -- �������� ������
        OB.SUBSERVICE_ID,           -- id ���������� �������� ����������� � �������� (��� ��������)
        SS.SUBSERVICE,              -- ���������� ������ �� ����������� � ��������
        OB.CHARGE_TYPE,             -- ��� ����������: REC(������)/USG(������)/MIN(���������) (��� ��������)
        OB.RATE_VALUE,              -- ��������� / ������� �� ����������� ���������, � ����������� �� CHARGE_TYPE 
        OB.CURRENCY_ID,             -- id ������ � ��������, ��� � Portal6.5
        D.NAME RATE_RULE,           -- ������� ���������� ����������
        OB.DATE_FROM OB_DATE_FROM,  -- ���� ������ �������� ���������� ������
        OB.DATE_TO OB_DATE_TO,      -- ���� ��������� �������� ���������� ������
        OB.RATEPLAN_ID RATEPLAN_ID, -- id ��������� ����� � �������� (��� ��������)
        P.RATEPLAN_NAME,            -- ��� ��������� �����  � ��������
        DECODE(SCA.MANAGER_ID, NULL, MC.LAST_NAME||' '||MC.FIRST_NAME||MC.MIDDLE_NAME, 
                                     MA.LAST_NAME||' '||MA.FIRST_NAME||MA.MIDDLE_NAME) MANAGER, -- ��������
        ADR.JUR_ADDERESS,
        ADR.DLV_ADDERESS
      FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, 
           ORDER_T O, ORDER_INFO_T OI, RATEPLAN_T P,  
           ORDER_BODY_T OB, SUBSERVICE_T SS, DICTIONARY_T D, 
           SERVICE_T S, EXT01_LOTUS_SERVICES LS, SERVICE_ALIAS_T SA,
           CONTRACT_T C, CONTRACTOR_T CT, CUSTOMER_T CS, 
           SALE_CURATOR_T SCA, SALE_CURATOR_T SCC,
           MANAGER_T MC, MANAGER_T MA, ADR
     WHERE A.BILLING_ID     = 2008  -- �������� ������� �� �������
       AND A.ACCOUNT_ID     = AP.ACCOUNT_ID
       AND A.ACCOUNT_ID     = O.ACCOUNT_ID
       AND O.SERVICE_ID     = S.SERVICE_ID
       AND S.SERVICE_ID     = LS.SERVICE_ID(+)
       --AND LS.SERVICE_OM NOT IN ('ZVI','��������� Free Phone','��������� ��/�� (� �������� ����������)')
       AND OI.ORDER_ID      = O.ORDER_ID
       AND O.ORDER_ID       = OB.ORDER_ID
       AND OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
       AND OB.RATE_RULE_ID  = D.KEY_ID(+)
       AND OB.RATEPLAN_ID   = P.RATEPLAN_ID(+)
       AND AP.CONTRACT_ID   = C.CONTRACT_ID
       AND AP.ACTUAL        = 'Y'
       AND AP.BRANCH_ID     = CT.CONTRACTOR_ID
       --AND CT.CONTRACTOR  != '������ ������������ ����� (�� ����� ������)'
       --AND CT.CONTRACTOR   = '������ ������������ ������ (��)'
       --AND CT.CONTRACTOR   = '������ ������������ ������ (��, ����)'
       --AND CT.CONTRACTOR   LIKE '%������%'
       AND CT.CONTRACTOR_ID = p_branch_id
       AND AP.CUSTOMER_ID   = CS.CUSTOMER_ID
       AND A.ACCOUNT_ID     = SCA.ACCOUNT_ID(+)
       AND C.CONTRACT_ID    = SCC.CONTRACT_ID(+)
       AND MC.MANAGER_ID(+) =  SCC.MANAGER_ID
       AND MA.MANAGER_ID(+) =  SCA.MANAGER_ID
       AND SA.SERVICE_ID(+) = O.SERVICE_ID
       AND SA.ACCOUNT_ID(+) = O.ACCOUNT_ID
       AND A.ACCOUNT_ID     = ADR.ACCOUNT_ID(+)
       ORDER BY CT.CONTRACTOR,       --  ������ 
                CS.CUSTOMER,         -- ��� ������
                A.ACCOUNT_NO,
                O.ORDER_NO,
                S.SERVICE,
                SS.SUBSERVICE
       ;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;



END PK701_EXPORT_LOTUS;
/
