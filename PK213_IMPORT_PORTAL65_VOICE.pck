CREATE OR REPLACE PACKAGE PK213_IMPORT_PORTAL65_VOICE
IS
    --
    -- ����� ��� �������� �������� ������ xTTK ������-������������� �������һ
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK213_IMPORT_PORTAL65_VOICE';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- =====================================================================
    -- ������� � ������� ��������� ��������, ��� �������� ����� ��������
    c_BILLING_ID          CONSTANT INTEGER := 2002; -- ��� ������������
    c_LOAD_CODE_START     CONSTANT INTEGER := 0;    -- ��������
--    c_LOAD_CODE_PROGRESS  CONSTANT INTEGER := 1;    -- ������
--    c_LOAD_CODE_OK        CONSTANT INTEGER := 2;    -- ��
    c_LOAD_CODE_ERR       CONSTANT INTEGER :=-1;    -- ������
    c_LOAD_CODE_DBL       CONSTANT INTEGER :=-2;    -- ������ ��� ���� � BRM
    -- ���� ���������� ���������
    c_LOAD_CODE_ACC       CONSTANT INTEGER := 1;    -- ������ �/�
    c_LOAD_CODE_CLN       CONSTANT INTEGER := 2;    -- ������ ������
    c_LOAD_CODE_MGR       CONSTANT INTEGER := 3;    -- ������ sale-curator
    c_LOAD_CODE_CTR       CONSTANT INTEGER := 4;    -- ������ �������
    c_LOAD_CODE_CST       CONSTANT INTEGER := 5;    -- ������ ����������-����������
    c_LOAD_CODE_APF       CONSTANT INTEGER := 6;    -- ������ ������� �/�
    c_LOAD_CODE_AJR       CONSTANT INTEGER := 7;    -- ������ ����� �����������
    c_LOAD_CODE_ADL       CONSTANT INTEGER := 8;    -- ������ ����� ��������
    c_LOAD_CODE_ORD       CONSTANT INTEGER := 9;    -- ������ �����    
    c_LOAD_CODE_SAL       CONSTANT INTEGER :=10;    -- ������ service-alias
    c_LOAD_CODE_SUB       CONSTANT INTEGER :=11;    -- �������� �������� �����
    c_LOAD_CODE_FIN       CONSTANT INTEGER :=12;    -- �����

    -- ������ �������� ����������
    c_DLV_METHOD_AP CONSTANT INTEGER := 6512;   -- ����������
    
    -- ��������� ������������
    �_RATESYSTEM_ID       CONSTANT INTEGER := 1209; -- ����������� � billngServer �.������������
    c_RATERULE_BILSRV_ID  CONSTANT INTEGER := Pk00_Const.c_RR_BILSRV;
    c_RATEPLAN_BILSRV_ID  CONSTANT INTEGER := 6;  -- '����� BillingServer'
    
    c_LDSTAT_DBL_ORD CONSTANT INTEGER := -1;   -- ����� ��� ���� � BRM
    c_LDSTAT_NOT_SRV CONSTANT INTEGER := -2;   -- ������ �� �������
    
    c_MAX_DATE_TO    CONSTANT DATE := TO_DATE('01.01.2050','dd.mm.yyyy');

    --============================================================================================
    -- ����� ������� ������������ ��������� �/� �� c_BILLING_NPL -> Pk00_Const.c_BILLING_OLD
    --============================================================================================
    PROCEDURE Create_list;

    --============================================================================================
    -- ����� ������� ������������ ��������� �/� �� c_BILLING_NPL -> Pk00_Const.c_BILLING_OLD
    --============================================================================================
    PROCEDURE Change_billing_id;

    -- ----------------------------------------------------------------------------- --
    -- ������ ��������� ������� ��������, ������ ���������� � ���� ���������
    PROCEDURE Import_contracts;
    
    -- ----------------------------------------------------------------------------- --
    -- �������� ���������� �� �������, ��� ����������� ���������
    PROCEDURE Import_orders;

    -- ������ ������� �� ������ � ������������ ���������, ���������� ��� ��������� �������
    PROCEDURE Import_orders_by_list;

    -- ������ �������������� ������ �� Portal 6.5
    PROCEDURE Import_add_data;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� BRM_CONTRACTOR_ID, BRM_CONTRACTOR_BANK_ID, BRM_BRANCH_ID, BRM_AGENT_ID
    PROCEDURE Set_contractor_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� MANAGER_ID ������� ����� ������������ ��� �������� ��������
    PROCEDURE Set_manager_id;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� BRM_CLIENT_ID ������� ����� ������������ ��� �������� ��������
    PROCEDURE Set_client_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� BRM_CUSTOMER_ID ������� ����� ������������ ��� �������� ��������
    PROCEDURE Set_customer_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� CONTRACT_ID - ������� ����� ������������ ��� �������� ��������
    PROCEDURE Set_contract_id;

    -- ����������� �������������� ���������� �� ��������
    -- ��� �������, ������� �����, ������
    PROCEDURE Set_contract_info;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ACCOUNT_ID - ������� ����� ������������ ��� �������� ��������
    PROCEDURE Set_account_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ORDER_ID
    PROCEDURE Set_order_id;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ���������� ��� ORDER_INFO_T
    PROCEDURE Set_order_info;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ID ������
    PROCEDURE Set_service_id;
      
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ID ��������� ������
    PROCEDURE Set_subservice_id;

    -- ----------------------------------------------------------------------------- --
    -- ������� ������ �� ������� �������� �� ������ '.SPB TTK Brand'
    -- � ������ �������� � �������� �������������
    PROCEDURE Import_data;
    
    --============================================================================================
    -- �������� ������ �� ������� ������
    PROCEDURE Load_accounts;

    -- �������� ������ �� �������
    PROCEDURE Load_orders;
    
    -- �������� ������������ ������������ ����������� �����
    PROCEDURE Check_subservice( 
                 p_recordset    OUT t_refc
             );
        
    --============================================================================================
    -- ��������� ����������� ������ � �����
    --============================================================================================
    PROCEDURE Move_to_archive;
    
    -- =========================================================== --
    -- ������������ ������
    -- =========================================================== --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������������� ������ ��� �������� �������
    -- ��������� ��� ������� �� ����������� ���������
    -- � ����������� ������� p_period_id
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Create_new_bills(p_period_id IN INTEGER);
    
    -- ------------------------------------------------------------------------- --
    -- ������������ ����� �������� ��.��� �������� (�� ���� ����������) 
    -- ------------------------------------------------------------------------- --
    PROCEDURE Make_bills( p_period_id IN INTEGER );
    
    -- ----------------------------------------------------------------------------- --
    -- �������� ������ ������� ������
    CURSOR c_CTR IS (
       SELECT 
          ROW_NUMBER() OVER (ORDER BY ACCOUNT_ID, CONTRACT_NO) RN,
          ACCOUNT_ID,
          CONTRACT_NO,
          ACCOUNT_NO,
          CLIENT_ID,
          KONTRAGENT,
          CLIENT,
          CUST_TYPE,
          CUSTDATE,
          JUR_ZIP,
          JUR_CITY,
          JUR_ADDRESS,
          PHIS_ZIP,
          PHIS_CITY,
          PHIS_ADDRESS,
          PHONE,
          FAX,
          EMAIL_ADDR,
          PHIS_NAME,
          INN,
          OKONH,
          OKPO,
          BANK,
          SETTLEMENT,
          CORR,
          BIC,
          KPP,
          COMPANY,
          CURRENCY,
          CURRENCY_SECONDARY,
          ORIGINAL,
          TAX_VAT,
          TAX_SALES,
          SALES_NAME,
          DIRECTORATE,
          MARKET_SEG,
          BILLING_CURATOR,
          GL_SEGMENT,
          IACCOUNT,
          AGENT_CODE,
          AGENT_NAME,
          PPTS_FLAG,
          DELIVERY,
          BRM_CONTRACT_NO,
          BRM_CONTRACT_ID,
          BRM_ACCOUNT_NO,
          BRM_ACCOUNT_ID,
          BRM_PROFILE_ID,
          BRM_CONTRACTOR_ID,
          DECODE(BRM_CONTRACTOR_BANK_ID, 3, 4, BRM_CONTRACTOR_BANK_ID) BRM_CONTRACTOR_BANK_ID,
          BRM_BRANCH_ID,
          BRM_AGENT_ID,
          BRM_CLIENT_ID,
          BRM_CUSTOMER_ID,
          BRM_SALE_CURATOR_ID,
          BRM_BILLING_CURATOR_ID,
          BRM_DLV_ADDRESS_ID,
          BRM_JUR_ADDRESS_ID,
          BRM_BC_LASTNAME,
          BRM_BC_FIRSTNAME,
          BRM_BC_MIDDLENAME,
          BRM_SC_LASTNAME,
          BRM_SC_FIRSTNAME,
          BRM_SC_MIDDLENAME,
          BRM_MARKET_SEGMENT_ID,
          BRM_CLIENT_TYPE_ID,
          BRM_CURRENCY_ID,
          BRM_CURRENCY_CONVERSION_ID,
          BRM_DELIVERY_METHOD_ID,
          BRM_COMMENTARY,
          IMPORT_DATE,
          LOAD_DATE,
          LOAD_CODE,
          LOAD_STATUS
         FROM PK213_PINDB_ALL_CONTRACTS_T 
        WHERE LOAD_CODE = 0
    )FOR UPDATE;
        
    -- �������� ������ �������    
    CURSOR c_ORD IS (
       SELECT 
          ROW_NUMBER() OVER (ORDER BY ACCOUNT_POID, CONTRACT_NO) RN,
          --BRAND,
          ACCOUNT_POID,
          CONTRACT_NO,
          ACCOUNT_NO,
          --COMPANY,
          SERVICE_POID,
          ORDER_NO,
          STATUS,
          RATE_PLAN,
          PRODUCT_NAME,
          EVENT_TYPE,
          IP_USAGE_RATE_PLAN,
          IP_TARIFF_TYPE,
          CYCLE_FEE_AMT,
          CURRENCY,
          CURRENCY_SECONDARY,
          CYCLE_START_T,  -- order_t.date_from
          CYCLE_END_T,    -- order_t.date_to
          SMC_START_T,    -- ������ ���� ������� �����, ���� ������, �� ������������
          SMC_END_T,      -- ������ ����� �������� ������, 
          ORDER_DATE,     -- order_t.date_from - �������
          S_RGN,
          D_RGN,
          SERVICE_NAME,
          SPEED_STR,
          FREE_DOWNTIME,
          BRM_ACCOUNT_ID,
          BRM_ORDER_NO,
          BRM_ORDER_ID,
          BRM_SERVICE_ID,
          BRM_ORDER_BODY_ID,
          BRM_SUBSERVICE_ID,
          BRM_CHARGE_TYPE,
          BRM_RATERULE_ID,
          BRM_RATEPLAN_ID,
          BRM_SPEED_VALUE,
          BRM_SPEED_UNIT_ID,
          BRM_CURRENCY_ID,
          IMPORT_DATE,
          LOAD_DATE,
          LOAD_CODE,
          LOAD_STATUS
         FROM PK213_PINDB_ORDERS_T 
        WHERE LOAD_CODE = 0
    )FOR UPDATE;
    
END PK213_IMPORT_PORTAL65_VOICE;
/
CREATE OR REPLACE PACKAGE BODY PK213_IMPORT_PORTAL65_VOICE
IS

--============================================================================================
-- ����� ������� ������������ ��������� �/� �� c_BILLING_NPL -> Pk00_Const.c_BILLING_OLD
--============================================================================================
PROCEDURE Change_billing_id
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Check_data';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE ACCOUNT_T A
       SET A.BILLING_ID = Pk00_Const.c_BILLING_OLD
     WHERE A.BILLING_ID = c_BILLING_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T.BILLING_ID: '||v_count||' rows c_BILLING_NPL -> c_BILLING_OLD', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- ����� ������� ������������ ��������� �/� �� c_BILLING_NPL -> Pk00_Const.c_BILLING_OLD
--============================================================================================
PROCEDURE Create_list
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Create_list';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������ �� �������
    DELETE FROM PK213_PINDB_LIST_T;

    -- ������� ����� ������
    INSERT INTO PK213_PINDB_LIST_T(CONTRACT_NO, ACCOUNT_NO, COMPANY)
    SELECT DISTINCT CONTRACT_NO, ACCOUNT_NO, COMPANY
      FROM ( 
        select  
             s.login , i2d@pindb (ap.cycle_end_t) cycle_end_t,
             S.POID_TYPE, Pro.NAME pname, Pro.PROD_CODE pcode, 
             SS.NAME ssname, SS.CATEGORY, SS.SERV_CODE sscode, i2d@pindb(max(i.created_t)),
             a.gl_segment,  ci.auto_no CONTRACT_NO, A.LOCALE,
             a.account_no , an.company,-- s.poid_id0,   S.POID_TYPE,
             A.CURRENCY, A.CURRENCY_SECONDARY , a.merchant
        from account_t@pindb  a 
             inner join account_products_t@pindb  ap on a.poid_id0 = ap.obj_id0
             inner join account_nameinfo_t@pindb  an on a.poid_id0 = an.obj_id0 and an.rec_id = 1
             inner join plan_t@pindb  p on ap.plan_obj_id0 = p.poid_id0
             inner join product_t@pindb  d on ap.product_obj_id0 = d.poid_id0
             inner join rate_plan_t@pindb  r on ap.product_obj_id0 = r.product_obj_id0
             inner join service_t@pindb  s on ap.service_obj_id0 = s.poid_id0
             inner join profile_t@pindb  pr on a.poid_id0 = pr.account_obj_id0
             inner join contract_info_t@pindb  ci on pr.poid_id0 = ci.obj_id0
             inner join voice_t@pindb v on S.POID_ID0=V.OBJ_ID0
             inner join subservices_t@pindb ss on ss.rec_id=v.subservice_id
             inner join products_t@pindb pro on pro.rec_id = ss.product_id
             inner join item_t@pindb i on I.SERVICE_OBJ_ID0=s.poid_id0 and I.DUE<>0
           where   a.merchant not like '%MIGR%' 
             and ( ap.cycle_end_t =0 or to_char(i2d@pindb (ap.cycle_end_t),'mm.yyyy')='06.2016')
             and S.POID_TYPE   like '%oice%'
         group by s.login , i2d@pindb (ap.cycle_end_t),S.POID_TYPE, 
                  Pro.NAME, Pro.PROD_CODE, SS.NAME, SS.CATEGORY, SS.SERV_CODE,
                  a.gl_segment,  ci.auto_no, A.LOCALE,
                  a.account_no , an.company, A.CURRENCY, A.CURRENCY_SECONDARY , a.merchant
        order by s.login,ss.name
    ) T
    WHERE EXISTS (
      SELECT * FROM PK213_ORDERS_LIST L
       WHERE L.ORDER_NO   = T.LOGIN
         AND L.ACCOUNT_NO = T.ACCOUNT_NO
    )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_LIST_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PK213_PINDB_LIST_T');
    COMMIT;   

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- ������ ��������� ������� ��������, ������ ���������� � ���� ���������
--
PROCEDURE Import_contracts
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_contracts';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    DELETE FROM PK213_PINDB_ALL_CONTRACTS_T;
    --
    -- account_no - ����� ��������,
    -- custno - ����� �/�
    -- ��������� - ����������. ���� ���, ���������.
    --
    INSERT INTO PK213_PINDB_ALL_CONTRACTS_T(
        ACCOUNT_ID, CONTRACT_NO, ACCOUNT_NO, 
        CLIENT_ID, KONTRAGENT, CLIENT, CUST_TYPE, CUSTDATE, 
        JUR_ZIP, JUR_CITY, JUR_ADDRESS, 
        PHIS_ZIP, PHIS_CITY, PHIS_ADDRESS, 
        PHONE, FAX, EMAIL_ADDR, PHIS_NAME, 
        INN, OKONH, OKPO, BANK, 
        SETTLEMENT, CORR, BIC, KPP, 
        COMPANY, CURRENCY, CURRENCY_SECONDARY, ORIGINAL, TAX_VAT, TAX_SALES, 
        SALES_NAME, DIRECTORATE, MARKET_SEG, BILLING_CURATOR, 
        GL_SEGMENT, IACCOUNT, AGENT_CODE, AGENT_NAME, PPTS_FLAG, DELIVERY
    )
    SELECT 
        ACCOUNT_ID, TRIM(ACCOUNT_NO), TRIM(CUSTNO), 
        CLIENT_ID, TRIM(KONTRAGENT), TRIM(CLIENT), CUST_TYPE, CUSTDATE, 
        TRIM(JUR_ZIP), TRIM(JUR_CITY), TRIM(JUR_ADDRESS), 
        TRIM(PHIS_ZIP), TRIM(PHIS_CITY), TRIM(PHIS_ADDRESS), 
        TRIM(PHONE), TRIM(FAX), TRIM(EMAIL_ADDR), TRIM(PHIS_NAME), 
        TRIM(INN), TRIM(OKONH), TRIM(OKPO), 
        TRIM(BANK), TRIM(SETTLEMENT), TRIM(CORR), TRIM(BIC), TRIM(KPP), 
        TRIM(COMPANY), TRIM(CURRENCY), CURRENCY_SECONDARY, ORIGINAL, TAX_VAT, TAX_SALES, 
        TRIM(SALES_NAME), TRIM(DIRECTORATE), TRIM(MARKET_SEG), TRIM(BILLING_CURATOR), 
        TRIM(GL_SEGMENT), IACCOUNT, AGENT_CODE, TRIM(AGENT_NAME), PPTS_FLAG, TRIM(DELIVERY) 
      FROM PIN.V_ALL_CONTRACTS@PINDB.WORLD C 
     WHERE EXISTS (
           SELECT * FROM PK213_PINDB_LIST_T L
            WHERE 1=1
              AND L.CONTRACT_NO  = C.ACCOUNT_NO
              AND L.ACCOUNT_NO   = C.CUSTNO
     )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_ALL_CONTRACTS_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PK213_PINDB_ALL_CONTRACTS_T');
    COMMIT;

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;  

--============================================================================================
-- �������� ���������� �� �������, ��� ����������� ���������
--============================================================================================
PROCEDURE Import_orders
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_orders';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    --
    DELETE FROM PK213_PINDB_ORDERS_T;
    --
    -- ������ �������� �� ������� �.�.������
    --
    INSERT INTO PK213_PINDB_ORDERS_T (
           BRAND, CONTRACT_NO, ACCOUNT_NO, COMPANY, ORDER_NO, 
           PRODUCT_NAME, EVENT_TYPE, 
           CURRENCY, CURRENCY_SECONDARY, 
           CYCLE_START_T,  ORDER_DATE,
           SERVICE_NAME, SERVICE_POID 
    )
    SELECT T.GL_SEGMENT, T.CONTRACT_NO, T.ACCOUNT_NO, T.COMPANY, T.LOGIN ORDER_NO, 
           T.CATEGORY PRODUCT_NAME, T.POID_TYPE EVENT_TYPE,
           T.CURRENCY, T.CURRENCY_SECONDARY, 
           T.CYCLE_START_T, T.CYCLE_START_T,
           T.SSNAME, T.SSCODE SERVICE_POID
     -- ------------------------------------------------------------------- --
     FROM (
        select s.login, 
               i2d@pindb(ap.cycle_start_t) cycle_start_t,
               i2d@pindb(ap.cycle_end_t) cycle_end_t,
               S.POID_TYPE, Pro.NAME pname, Pro.PROD_CODE pcode, 
               SS.NAME ssname, SS.CATEGORY, SS.SERV_CODE sscode, 
               a.gl_segment,  ci.auto_no CONTRACT_NO, A.LOCALE,
               a.account_no , an.company,
               A.CURRENCY, A.CURRENCY_SECONDARY, a.merchant
          from account_t@pindb  a 
               inner join account_products_t@pindb  ap on a.poid_id0 = ap.obj_id0
               inner join account_nameinfo_t@pindb  an on a.poid_id0 = an.obj_id0 and an.rec_id = 1
               inner join plan_t@pindb  p on ap.plan_obj_id0 = p.poid_id0
               inner join rate_plan_t@pindb  r on ap.product_obj_id0 = r.product_obj_id0
               inner join product_t@pindb  d on ap.product_obj_id0 = d.poid_id0
               inner join service_t@pindb  s on ap.service_obj_id0 = s.poid_id0
               inner join profile_t@pindb  pr on a.poid_id0 = pr.account_obj_id0
               inner join contract_info_t@pindb  ci on pr.poid_id0 = ci.obj_id0
               inner join voice_t@pindb v on S.POID_ID0=V.OBJ_ID0
               inner join subservices_t@pindb ss on ss.rec_id=v.subservice_id
               inner join products_t@pindb pro on pro.rec_id = ss.product_id
              where   a.merchant not like '%MIGR%' 
                and ( ap.cycle_end_t =0 or to_char(i2d@pindb (ap.cycle_end_t),'mm.yyyy')='06.2016')
                and S.POID_TYPE   like '%oice%'
               group by s.login , i2d@pindb(ap.cycle_end_t), i2d@pindb(ap.cycle_start_t), S.POID_TYPE, 
                        Pro.NAME, Pro.PROD_CODE, SS.NAME, SS.CATEGORY, SS.SERV_CODE,
                        a.gl_segment,  ci.auto_no, A.LOCALE,
                        a.account_no , an.company, A.CURRENCY, A.CURRENCY_SECONDARY , a.merchant
               order by s.login,ss.name
     ) T
     -- ------------------------------------------------------------------- --
      /*
      FROM ( 
        select  
             s.login , 
             i2d@pindb(ap.cycle_start_t) cycle_start_t,
             i2d@pindb (ap.cycle_end_t) cycle_end_t,
             S.POID_TYPE, Pro.NAME pname, Pro.PROD_CODE pcode, 
             SS.NAME ssname, SS.CATEGORY, SS.SERV_CODE sscode, 
             i2d@pindb(max(i.created_t)) DATE_CREATED,
             a.gl_segment,  ci.auto_no CONTRACT_NO, A.LOCALE,
             a.account_no , an.company,-- s.poid_id0,   S.POID_TYPE,
             A.CURRENCY, A.CURRENCY_SECONDARY , a.merchant
        from account_t@pindb  a 
             inner join account_products_t@pindb  ap on a.poid_id0 = ap.obj_id0
             inner join account_nameinfo_t@pindb  an on a.poid_id0 = an.obj_id0 and an.rec_id = 1
             inner join plan_t@pindb  p on ap.plan_obj_id0 = p.poid_id0
             inner join product_t@pindb  d on ap.product_obj_id0 = d.poid_id0
             inner join rate_plan_t@pindb  r on ap.product_obj_id0 = r.product_obj_id0
             inner join service_t@pindb  s on ap.service_obj_id0 = s.poid_id0
             inner join profile_t@pindb  pr on a.poid_id0 = pr.account_obj_id0
             inner join contract_info_t@pindb  ci on pr.poid_id0 = ci.obj_id0
             inner join voice_t@pindb v on S.POID_ID0=V.OBJ_ID0
             inner join subservices_t@pindb ss on ss.rec_id=v.subservice_id
             inner join products_t@pindb pro on pro.rec_id = ss.product_id
             inner join item_t@pindb i on I.SERVICE_OBJ_ID0=s.poid_id0 and I.DUE<>0
           where   a.merchant not like '%MIGR%' 
             and ( ap.cycle_end_t =0 or to_char(i2d@pindb (ap.cycle_end_t),'mm.yyyy')='06.2016')
             and S.POID_TYPE   like '%oice%'
         group by s.login , i2d@pindb (ap.cycle_end_t),S.POID_TYPE, 
                  Pro.NAME, Pro.PROD_CODE, SS.NAME, SS.CATEGORY, SS.SERV_CODE,
                  a.gl_segment,  ci.auto_no, A.LOCALE,
                  a.account_no , an.company, A.CURRENCY, A.CURRENCY_SECONDARY , a.merchant
        order by s.login,ss.name
    ) T
    */
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_ORDERS_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ��� IP ������
    MERGE INTO PK213_PINDB_ORDERS_T XO
    USING (
        SELECT tt.name, tt.type, tt.min_amount FROM ttc_tariffs@PINDB.WORLD tt
    ) tt
    ON (
        XO.IP_USAGE_RATE_PLAN = tt.name
    )
    WHEN MATCHED THEN UPDATE SET XO.IP_TARIFF_TYPE = tt.type, XO.BRM_MIN_VALUE = tt.min_amount;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_ORDERS_T.IP_TARIFF_TYPE: '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- �������� ����������
    Gather_Table_Stat(l_Tab_Name => 'PK213_PINDB_ORDERS_T');
    COMMIT;

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------- --
-- ������ ������� �� ������ � ������������ ���������
-- ���������� ��� ��������� �������
-- ------------------------------------------------------------------- --
PROCEDURE Import_orders_by_list
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_orders_by_list';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    --
    DELETE FROM PK213_PINDB_ORDERS_T;
    --
    -- ������ �������� �� ������� �.�.������ (29.08.2016)
    -- � ������ ������, ����� ���� ��� ���������� �������� ��������� � �������, 
    -- ����� ������������ ������ ����������� ��� �������������� ��������:
    --
    INSERT INTO PK213_PINDB_ORDERS_T (
           BRAND, CONTRACT_NO, ACCOUNT_NO, COMPANY, ORDER_NO, 
           PRODUCT_NAME, EVENT_TYPE, 
           CURRENCY, CURRENCY_SECONDARY, 
           CYCLE_START_T,  ORDER_DATE,
           SERVICE_NAME, SERVICE_POID 
    )
    SELECT T.GL_SEGMENT, T.CONTRACT_NO, T.ACCOUNT_NO, T.COMPANY, T.LOGIN ORDER_NO, 
           T.CATEGORY PRODUCT_NAME, T.POID_TYPE EVENT_TYPE,
           T.CURRENCY, T.CURRENCY_SECONDARY, 
           T.CYCLE_START_T, T.CYCLE_START_T,
           T.SSNAME, T.SSCODE SERVICE_POID
     -- ------------------------------------------------------------------- --
     FROM (
        select s.login, 
               i2d@pindb(ap.cycle_start_t) cycle_start_t,
               i2d@pindb(ap.cycle_end_t) cycle_end_t,
               S.POID_TYPE, Pro.NAME pname, Pro.PROD_CODE pcode, 
               SS.NAME ssname, SS.CATEGORY, SS.SERV_CODE sscode, 
               a.gl_segment,  ci.auto_no CONTRACT_NO, A.LOCALE,
               a.account_no , an.company,
               A.CURRENCY, A.CURRENCY_SECONDARY, a.merchant
          from account_t@pindb  a 
               inner join account_products_t@pindb  ap on a.poid_id0 = ap.obj_id0
               inner join account_nameinfo_t@pindb  an on a.poid_id0 = an.obj_id0 and an.rec_id = 1
               inner join plan_t@pindb  p on ap.plan_obj_id0 = p.poid_id0
               inner join rate_plan_t@pindb  r on ap.product_obj_id0 = r.product_obj_id0
               inner join product_t@pindb  d on ap.product_obj_id0 = d.poid_id0
               inner join service_t@pindb  s on ap.service_obj_id0 = s.poid_id0
               inner join profile_t@pindb  pr on a.poid_id0 = pr.account_obj_id0
               inner join contract_info_t@pindb  ci on pr.poid_id0 = ci.obj_id0
               inner join voice_t@pindb v on S.POID_ID0=V.OBJ_ID0
               inner join subservices_t@pindb ss on ss.rec_id=v.subservice_id
               inner join products_t@pindb pro on pro.rec_id = ss.product_id
              where S.POID_TYPE   like '%oice%'
               group by s.login , i2d@pindb(ap.cycle_end_t), i2d@pindb(ap.cycle_start_t), S.POID_TYPE, 
                        Pro.NAME, Pro.PROD_CODE, SS.NAME, SS.CATEGORY, SS.SERV_CODE,
                        a.gl_segment,  ci.auto_no, A.LOCALE,
                        a.account_no , an.company, A.CURRENCY, A.CURRENCY_SECONDARY , a.merchant
               order by s.login,ss.name
     ) T
     WHERE EXISTS (
        SELECT * FROM PK213_ORDERS_LIST L
         WHERE 1=1
           AND L.ORDER_NO   = T.LOGIN
           AND L.ACCOUNT_NO = T.ACCOUNT_NO
     )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_ORDERS_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ��� IP ������
    MERGE INTO PK213_PINDB_ORDERS_T XO
    USING (
        SELECT tt.name, tt.type, tt.min_amount FROM ttc_tariffs@PINDB.WORLD tt
    ) tt
    ON (
        XO.IP_USAGE_RATE_PLAN = tt.name
    )
    WHEN MATCHED THEN UPDATE SET XO.IP_TARIFF_TYPE = tt.type, XO.BRM_MIN_VALUE = tt.min_amount;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_ORDERS_T.IP_TARIFF_TYPE: '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- �������� ����������
    Gather_Table_Stat(l_Tab_Name => 'PK213_PINDB_ORDERS_T');
    COMMIT;

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


--============================================================================================
-- ������ �������������� ������ �� Portal 6.5
--
PROCEDURE Import_add_data
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_add_data';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ����������� ����������� � �/�
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    MERGE INTO PK213_PINDB_ALL_CONTRACTS_T X
    USING (
      SELECT OBJ_ID0, LAST_NAME
        FROM 
      (
      select obj_id0,last_name, 
             ROW_NUMBER() OVER (PARTITION BY OBJ_ID0 ORDER BY LAST_NAME) RN  
        from account_nameinfo_t@PINDB.WORLD 
       where last_name is not null
      )
      WHERE RN = 1
    ) N
    ON (
       X.ACCOUNT_ID = N.OBJ_ID0
    )
    WHEN MATCHED THEN UPDATE SET X.BRM_COMMENTARY = N.LAST_NAME;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_ALL_CONTRACTS_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ����������� ����� ����������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK213_PINDB_CONTRACTOR_BANK_T DROP STORAGE';

    INSERT INTO PK213_PINDB_CONTRACTOR_BANK_T( 
        ACCOUNT_POID, ACCOUNT_NO, CONTRACTOR_POID, CONTRACTOR, BANK, SETTLEMENT
    )
    select A.POID_ID0 account_poid, A.ACCOUNT_NO, C.IACC_OBJ_ID0 contractor_poid, 
           PN.COMPANY CONTRACTOR, PC.BANK, PC.SETTLEMENT
    from account_t@PINDB.WORLD a 
        inner join profile_t@PINDB.WORLD p on A.POID_ID0 = P.ACCOUNT_OBJ_ID0
        inner join contract_info_t@PINDB.WORLD c on P.POID_ID0 = C.OBJ_ID0
        inner join account_nameinfo_t@PINDB.WORLD pn on C.IACC_OBJ_ID0 = PN.OBJ_ID0 and PN.REC_ID = 1
        inner join profile_t@PINDB.WORLD pr on C.IACC_OBJ_ID0 = PR.ACCOUNT_OBJ_ID0
        inner join contract_info_t@PINDB.WORLD pc on PR.POID_ID0 = PC.OBJ_ID0
    WHERE EXISTS (
        SELECT * FROM PINDB_ALL_CONTRACTS_T PC
         WHERE PC.ACCOUNT_NO = A.ACCOUNT_NO
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_CONTRACTOR_BANK_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ����������� ���� ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK213_PINDB_CONTRACT_TYPE_T DROP STORAGE';
   
    INSERT INTO PK213_PINDB_CONTRACT_TYPE_T ( CONTRACT_NO, CONTRACT_TYPE_ID )
    SELECT DISTINCT b.auto_no CONTRACT_NO, b.client_cat_id CONTRACT_TYPE_ID
      from contract_info_t@PINDB.WORLD b --where b.client_cat_id = 4
     WHERE CLIENT_CAT_ID > 0
       AND EXISTS (
          SELECT * 
            FROM PK213_PINDB_ALL_CONTRACTS_T PC
           WHERE PC.ACCOUNT_NO = b.AUTO_NO
       )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_CONTRACT_TYPE_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� BRM_CONTRACTOR_ID, BRM_CONTRACTOR_BANK_ID, BRM_BRANCH_ID, BRM_AGENT_ID
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_contractor_id
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Set_contractor_id';
    v_count     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� BRM_CONTRACTOR_ID � BRM_CONTRACTOR_BANK_ID (����� �.� 24.08.2015)
    MERGE INTO PK213_PINDB_ALL_CONTRACTS_T X
    USING (
        SELECT DISTINCT X.ACCOUNT_NO, X.IACCOUNT, PCI.SETTLEMENT, CB.CONTRACTOR_ID, CB.BANK_ID  
          FROM PK213_PINDB_ALL_CONTRACTS_T X, CONTRACT_INFO_T@PINDB.WORLD PCI, CONTRACTOR_BANK_T CB
         WHERE PCI.AUTO_NO = X.IACCOUNT
           AND CB.BANK_SETTLEMENT = PCI.SETTLEMENT
    ) CB
    ON (
        CB.ACCOUNT_NO   = X.ACCOUNT_NO
        AND CB.IACCOUNT = X.IACCOUNT
        AND X.LOAD_CODE IS NULL
    )
    WHEN MATCHED THEN UPDATE SET X.BRM_CONTRACTOR_ID = CB.CONTRACTOR_ID,
                                 X.BRM_CONTRACTOR_BANK_ID = CB.BANK_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_ALL_CONTRACTS_T.BRM_CONTRACTOR_ID '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� BRM_BRANCH_ID (����� �.� 24.08.2015)
    MERGE INTO PK213_PINDB_ALL_CONTRACTS_T X
    USING (
        SELECT BRAND, CONTRACTOR_ID FROM PORTAL_BRAND_T
    ) BR
    ON (
        X.GL_SEGMENT = BR.BRAND
        AND X.LOAD_CODE IS NULL
    ) 
    WHEN MATCHED THEN UPDATE SET X.BRM_BRANCH_ID = BR.CONTRACTOR_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_ALL_CONTRACTS_T.BRM_BRANCH_ID '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ����������� BRM_AGENT_ID (����� �.� 24.08.2015)
    -- agent_code = 'DT295' --> agent_id = 1559598 (contractor_id)
    UPDATE PK213_PINDB_ALL_CONTRACTS_T X
       SET X.BRM_AGENT_ID = 1559598
     WHERE X.AGENT_CODE = 'DT295'
       AND X.LOAD_CODE IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_ALL_CONTRACTS_T.BRM_AGENT_ID '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� MANAGER_ID ������� ����� ������������ ��� �������� ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_manager_id
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Set_manager_id';
    --v_client_id   INTEGER;
    v_count       INTEGER := 0;
    v_last_name   VARCHAR2(100);
    v_first_name  VARCHAR2(100);
    v_middle_name VARCHAR2(100);
    v_pos         INTEGER;
    v_len         INTEGER;
    v_manager_id  INTEGER;
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������-��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FOR r_mgr IN (
      SELECT DISTINCT X.BRM_CONTRACTOR_ID, TRIM( X.BILLING_CURATOR ) MANAGER
        FROM PK213_PINDB_ALL_CONTRACTS_T X
       WHERE LOAD_CODE IS NULL
         AND BRM_BILLING_CURATOR_ID IS NULL
         AND TRIM( X.BILLING_CURATOR ) IS NOT NULL
    ) LOOP
      -- �������� ���������� ����������:
      -- '������ ������ ������������'
      -- '������ �.�.'
      -- '������ �.'
      -- '������ �'
      -- '������'
      --
      -- ������� --------------------------------------------------------
      v_len := INSTR(r_mgr.manager,' ',1);
      IF v_len > 0 THEN
        v_last_name := LTRIM(SUBSTR(r_mgr.manager, 1, v_len-1));
      ELSE
        v_last_name := TRIM(r_mgr.manager);
      END IF;
      
      -- ��� ------------------------------------------------------------
      v_first_name := SUBSTR(LTRIM(SUBSTR(r_mgr.manager, v_len)),1,1);
      IF v_first_name IS NOT NULL THEN 
        v_first_name := v_first_name||'.';
        
        -- �������� -----------------------------------------------------
        v_pos := INSTR(r_mgr.manager,' ',1,2);
        IF v_pos > 0 THEN
          v_middle_name := SUBSTR(LTRIM(SUBSTR(r_mgr.manager, v_pos+1)),1,1);
        ELSE
          v_pos := INSTR(r_mgr.manager,'.',1,1);
          IF v_pos > 0 THEN 
            v_middle_name := SUBSTR(LTRIM(SUBSTR(r_mgr.manager, v_pos+1)),1,1);
          END IF;
        END IF;
        IF v_middle_name IS NOT NULL THEN
          v_middle_name := v_middle_name||'.';
        END IF;
        
      END IF;

      -- ����� ����������� ��������� � ��
      SELECT MIN(MANAGER_ID) INTO v_manager_id
       FROM MANAGER_T M
      WHERE M.LAST_NAME     = v_last_name
        AND M.FIRST_NAME    = v_first_name
        AND M.MIDDLE_NAME   = v_middle_name
        AND M.CONTRACTOR_ID = r_mgr.brm_contractor_id;

      -- ���������� ID ���������
      IF v_manager_id IS NULL THEN
        v_manager_id := Pk02_Poid.Next_manager_id;
      END IF;

      -- ����������� �.�.� - BRM-��������� � �������: '������ �.�.'
      UPDATE PK213_PINDB_ALL_CONTRACTS_T X
         SET BRM_BILLING_CURATOR_ID = v_manager_id,
             BRM_BC_LASTNAME   = v_last_name,
             BRM_BC_FIRSTNAME  = v_first_name,
             BRM_BC_MIDDLENAME = v_middle_name
       WHERE LOAD_CODE IS NULL
         AND BRM_BILLING_CURATOR_ID IS NULL
         AND BRM_BC_LASTNAME IS NULL
         AND BILLING_CURATOR = r_mgr.manager;

      -- ����������� ������� ����������
      v_count := v_count + 1; 

    END LOOP;  

    Pk01_Syslog.Write_msg(v_count||' - distinct brm_billing_curator_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FOR r_mgr IN (
      SELECT DISTINCT X.BRM_CONTRACTOR_ID, TRIM( X.SALES_NAME ) MANAGER
        FROM PK213_PINDB_ALL_CONTRACTS_T X
       WHERE LOAD_CODE IS NULL
         AND BRM_SALE_CURATOR_ID IS NULL
         AND TRIM( X.SALES_NAME ) IS NOT NULL
    ) LOOP
    
      if instr(r_mgr.manager,'(') >0 then
    
          v_last_name  := trim(r_mgr.manager);
          v_first_name := '';
          v_first_name := '';
      else
          -- �������� ���������� ����������:
          -- '������ ������ ������������'
          -- '������ �.�.'
          -- '������ �.'
          -- '������ �'
          -- '������'
          --
          -- ������� --------------------------------------------------------
          v_len := INSTR(r_mgr.manager,' ',1);
          IF v_len > 0 THEN
            v_last_name := LTRIM(SUBSTR(r_mgr.manager, 1, v_len-1));
          ELSE
            v_last_name := TRIM(r_mgr.manager);
          END IF;
          
          -- ��� ------------------------------------------------------------
          v_first_name := SUBSTR(LTRIM(SUBSTR(r_mgr.manager, v_len)),1,1);
          IF v_first_name IS NOT NULL THEN 
            v_first_name := v_first_name||'.';
            
            -- �������� -----------------------------------------------------
            v_pos := INSTR(r_mgr.manager,' ',1,2);
            IF v_pos > 0 THEN
              v_middle_name := SUBSTR(LTRIM(SUBSTR(r_mgr.manager, v_pos+1)),1,1);
            ELSE
              v_pos := INSTR(r_mgr.manager,'.',1,1);
              IF v_pos > 0 THEN 
                v_middle_name := SUBSTR(LTRIM(SUBSTR(r_mgr.manager, v_pos+1)),1,1);
              END IF;
            END IF;
            IF v_middle_name IS NOT NULL THEN
              v_middle_name := v_middle_name||'.';
            END IF;
            
          END IF;

      end if;
      
      -- ����� ����������� ��������� � ��
      SELECT MIN(MANAGER_ID) INTO v_manager_id
       FROM MANAGER_T M
      WHERE M.LAST_NAME     = v_last_name
        AND nvl(M.FIRST_NAME,'-')    = nvl(v_first_name,'-')
        AND nvl(M.MIDDLE_NAME,'-')   = nvl(v_middle_name,'-')
        AND M.CONTRACTOR_ID = r_mgr.brm_contractor_id;

      -- ���������� ID ���������
      IF v_manager_id IS NULL THEN
        v_manager_id := Pk02_Poid.Next_manager_id;
      END IF;

      -- ����������� �.�.� - BRM-��������� � �������: '������ �.�.'
      UPDATE PK213_PINDB_ALL_CONTRACTS_T X
         SET BRM_SALE_CURATOR_ID = v_manager_id,
             BRM_SC_LASTNAME   = v_last_name,
             BRM_SC_FIRSTNAME  = v_first_name,
             BRM_SC_MIDDLENAME = v_middle_name
       WHERE LOAD_CODE IS NULL
         AND BRM_SALE_CURATOR_ID IS NULL
         AND BRM_SC_LASTNAME IS NULL
         AND SALES_NAME = r_mgr.manager;

      -- ����������� ������� ����������
      v_count := v_count + 1;

    END LOOP;  

    Pk01_Syslog.Write_msg(v_count||' - distinct brm_sale_curator_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� BRM_CLIENT_ID ������� ����� ������������ ��� �������� ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_client_id
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Set_client_id';
    v_client_id INTEGER;
    v_count     INTEGER := 0;
    v_count_new INTEGER := 0;
BEGIN
    FOR r_cln IN (
      SELECT DISTINCT CLIENT CLIENT  
        FROM PK213_PINDB_ALL_CONTRACTS_T X
       WHERE LOAD_CODE IS NULL
         AND BRM_CLIENT_ID IS NULL
         AND CLIENT IS NOT NULL  
    ) LOOP
      --
      -- ���� ����� ������������ �����������
      SELECT MIN(CL.CLIENT_ID) 
        INTO v_client_id  
        FROM CLIENT_T CL
       WHERE LOWER(CL.CLIENT_NAME) = LOWER(r_cln.CLIENT);
        
      IF v_client_id IS NULL THEN 
         v_client_id := PK02_POID.NEXT_CLIENT_ID;
         v_count_new := v_count_new + 1;
      END IF;
      --
      UPDATE PK213_PINDB_ALL_CONTRACTS_T X
         SET X.BRM_CLIENT_ID = v_client_id
       WHERE X.CLIENT        = r_cln.CLIENT
         AND X.LOAD_CODE     IS NULL
         AND X.BRM_CLIENT_ID IS NULL;  
      --
      v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg(v_count||' - distinct client_id, '||v_count_new||' - new', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� BRM_CUSTOMER_ID ������� ����� ������������ ��� �������� ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_customer_id
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Set_customer_id';
    v_customer_id INTEGER;
    v_count       INTEGER := 0;
    v_count_new   INTEGER := 0;
BEGIN
    FOR r_cst IN (
      SELECT KONTRAGENT ERP_CODE, COMPANY, INN, KPP  
        FROM PK213_PINDB_ALL_CONTRACTS_T X
       WHERE LOAD_CODE IS NULL
         AND BRM_CUSTOMER_ID IS NULL
         AND COMPANY IS NOT NULL
       GROUP BY KONTRAGENT, COMPANY, INN, KPP
    ) LOOP
      --
      -- ���� ����� ������������ �����������
      SELECT MIN(CS.CUSTOMER_ID) 
        INTO v_customer_id  
        FROM CUSTOMER_T CS
       WHERE CS.ERP_CODE = r_cst.ERP_CODE  
         AND NVL(CS.INN,'0') = NVL(r_cst.INN,'0')
         AND NVL(CS.KPP,'0') = NVL(r_cst.KPP,'0')
         AND LOWER(CS.CUSTOMER) = LOWER(r_cst.COMPANY);
        
      IF v_customer_id IS NULL THEN 
         v_customer_id := PK02_POID.NEXT_CUSTOMER_ID;
         v_count_new := v_count_new + 1;
      END IF;
      --
      UPDATE PK213_PINDB_ALL_CONTRACTS_T X
         SET X.BRM_CUSTOMER_ID = v_customer_id
       WHERE X.KONTRAGENT   = r_cst.ERP_CODE  
         AND NVL(X.INN,'0') = NVL(r_cst.INN,'0')
         AND NVL(X.KPP,'0') = NVL(r_cst.KPP,'0')
         AND X.COMPANY      = r_cst.COMPANY
         AND X.LOAD_CODE IS NULL
         AND X.BRM_CUSTOMER_ID IS NULL;  
      --
      v_count := v_count + 1;
    END LOOP;
    --
    Pk01_Syslog.Write_msg(v_count||' - distinct customer_id, '||v_count_new||' - new', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� CONTRACT_ID - ������� ����� ������������ ��� �������� ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_contract_id
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Set_contract_id';
    v_contract_id INTEGER;
    v_count       INTEGER := 0;
    v_exists      INTEGER := 0;
BEGIN
    -- ����������� contract_id
    FOR r_ctr IN (
      SELECT DISTINCT CONTRACT_NO  
        FROM PK213_PINDB_ALL_CONTRACTS_T X
       WHERE LOAD_CODE       IS NULL
         AND BRM_CONTRACT_ID IS NULL
         AND CONTRACT_NO     IS NOT NULL  
    ) LOOP
      --
      -- ��������� ������� �������� � BRM
      SELECT MIN(C.CONTRACT_ID)
        INTO v_contract_id
        FROM CONTRACT_T C
       WHERE C.CONTRACT_NO = r_ctr.contract_no;
      IF v_contract_id IS NULL THEN
          v_contract_id := PK02_POID.NEXT_CONTRACT_ID;
      ELSE
          v_exists := v_exists + 1;
      END IF;
      
      -- ����������� ID
      UPDATE PK213_PINDB_ALL_CONTRACTS_T X
         SET X.BRM_CONTRACT_ID = v_contract_id,
             X.BRM_CONTRACT_NO = r_ctr.contract_no
       WHERE X.CONTRACT_NO = r_ctr.contract_no
         AND X.LOAD_CODE       IS NULL
         AND X.BRM_CONTRACT_ID IS NULL;  
      --
      v_count := v_count + 1;
      --
    END LOOP;
    --
    Pk01_Syslog.Write_msg(v_count||' - distinct contract_id, '||v_exists||' contracts already exist in BRM', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� �������������� ���������� �� ��������
-- ��� �������, ������� �����, ������
PROCEDURE Set_contract_info
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Set_contract_info';
    v_count       INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������� ����� (V_PINDB_ALL_CONTRACTS_T.CUST_TYPE - ���������� � ����� �������)
    MERGE INTO PK213_PINDB_ALL_CONTRACTS_T X
    USING (
    SELECT DM.NAME, DM.KEY_ID 
      FROM DICTIONARY_T DM
     WHERE DM.PARENT_ID = 63 -- MARKET_SEGMENT
    ) DM
    ON (
        DM.NAME = X.CUST_TYPE
        AND X.LOAD_CODE IS NULL
    )
    WHEN MATCHED THEN UPDATE SET X.BRM_MARKET_SEGMENT_ID = DM.KEY_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_MARKET_SEGMENT_ID '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- ��� ������� (V_PINDB_ALL_CONTRACTS_T.MARKET_SEGMENT - ���������� � ��������� �����)
    MERGE INTO PK213_PINDB_ALL_CONTRACTS_T X
    USING (
    SELECT DM.NAME, DM.KEY_ID 
      FROM DICTIONARY_T DM
     WHERE DM.PARENT_ID = 64 -- CLIENT_TYPE
    ) DM
    ON (
        DM.NAME = X.MARKET_SEG
        AND X.LOAD_CODE IS NULL
    )
    WHEN MATCHED THEN UPDATE SET X.BRM_CLIENT_TYPE_ID = DM.KEY_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_CLIENT_TYPE_ID '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������������� ������ ��������
    UPDATE PK213_PINDB_ALL_CONTRACTS_T
       SET BRM_CURRENCY_ID = 
         CASE
            WHEN CURRENCY = 810 OR CURRENCY_SECONDARY = 286 THEN 810
            ELSE CURRENCY
         END,
           BRM_CURRENCY_CONVERSION_ID = 
         CASE
            WHEN CURRENCY = 810 AND CURRENCY_SECONDARY = 0 THEN 2601
            WHEN CURRENCY_SECONDARY = 286 THEN 2604
            ELSE 2602
         END
     WHERE LOAD_CODE IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_CURRENCY_ID '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ����������� ������ 
    UPDATE PK213_PINDB_ALL_CONTRACTS_T X
       SET X.LOAD_CODE = -1,
           X.LOAD_STATUS = '�� ���������� ������'
     WHERE BRM_CURRENCY_ID IS NULL
       AND X.LOAD_CODE IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_CURRENCY_ID '||v_count||' rows is null', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );
    
    -- ����������� ����� �������� �����
    MERGE INTO PK213_PINDB_ALL_CONTRACTS_T X
    USING (
        SELECT ACCOUNT_ID, KEY_ID
          FROM (
            SELECT X.ACCOUNT_ID, D.KEY_ID, 
                   ROW_NUMBER() OVER (PARTITION BY X.ACCOUNT_ID ORDER BY CONTRACT_NO) RN 
              FROM PK213_PINDB_ALL_CONTRACTS_T X, DICTIONARY_T D
             WHERE D.PARENT_ID = 65
               AND (X.DELIVERY = D.NAME OR X.DELIVERY = D.NOTES)
         )
         WHERE RN = 1
    ) D
    ON (
       X.ACCOUNT_ID =  D.ACCOUNT_ID
       AND X.LOAD_CODE IS NULL
    )
    WHEN MATCHED THEN UPDATE SET X.BRM_DELIVERY_METHOD_ID = D.KEY_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DELIVERY_METHOD_ID '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� ACCOUNT_ID - ������� ����� ������������ ��� �������� ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_account_id
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Set_account_id';
    v_account_id     INTEGER;
    v_profile_id     INTEGER;
    v_jur_address_id INTEGER;
    v_dlv_address_id INTEGER;
    v_count          INTEGER := 0;
BEGIN
    -- ��������� ��� ������ �/� ��������� � ��������
    UPDATE PK213_PINDB_ALL_CONTRACTS_T X
       SET LOAD_CODE = -1,
           LOAD_STATUS = 'ACCOUNT_NO - ������������ � Portal 6.5'
     WHERE EXISTS (
        SELECT * FROM PK213_PINDB_ALL_CONTRACTS_T X1
         WHERE X1.ACCOUNT_ID != X.ACCOUNT_ID
           AND X1.ACCOUNT_NO = X.ACCOUNT_NO
       )
       AND X.LOAD_CODE IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - account_no duplicated in Portal 6.5', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );

    -- ��������� ��� ������� �/� ��� � BRM, 
    -- account_id ����������� ��� �������� ������� �� ������������ �/�
    MERGE INTO PK213_PINDB_ALL_CONTRACTS_T X
    USING (
        SELECT A.ACCOUNT_ID, A.ACCOUNT_NO, AP.PROFILE_ID 
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AP.ACTUAL = 'Y'
    ) A
    ON (
        X.ACCOUNT_NO = A.ACCOUNT_NO
    )
    WHEN MATCHED THEN UPDATE 
                         SET X.BRM_ACCOUNT_ID = A.ACCOUNT_ID,
                             X.BRM_ACCOUNT_NO = A.ACCOUNT_NO,
                             X.BRM_PROFILE_ID = A.PROFILE_ID,
                             X.LOAD_CODE      = -2,
                             X.LOAD_STATUS    = 'ACCOUNT_NO - ��� ���������� � BRM'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - account_no already exists in BRM', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );

    -- ����������� ACCOUNT_ID - ������� ����� ������������ ��� �������� ��������
    FOR r_acc IN (
      SELECT DISTINCT ACCOUNT_NO  
        FROM PK213_PINDB_ALL_CONTRACTS_T X
       WHERE LOAD_CODE  IS NULL
         AND BRM_ACCOUNT_ID IS NULL
         AND ACCOUNT_NO IS NOT NULL
    ) LOOP
      --
      v_account_id     := PK02_POID.NEXT_ACCOUNT_ID;
      v_profile_id     := PK02_POID.NEXT_ACCOUNT_PROFILE_ID;
      v_jur_address_id := PK02_POID.NEXT_ADDRESS_ID;
      v_dlv_address_id := PK02_POID.NEXT_ADDRESS_ID;
      --
      UPDATE PK213_PINDB_ALL_CONTRACTS_T X
         SET X.BRM_ACCOUNT_NO     = r_acc.account_no,
             X.BRM_ACCOUNT_ID     = v_account_id,
             X.BRM_PROFILE_ID     = v_profile_id,
             X.BRM_JUR_ADDRESS_ID = v_jur_address_id,
             X.BRM_DLV_ADDRESS_ID = v_dlv_address_id
       WHERE X.ACCOUNT_NO         = r_acc.account_no
         AND X.LOAD_CODE      IS NULL
         AND X.BRM_ACCOUNT_ID IS NULL;  
      --
      v_count := v_count + 1;
      --
    END LOOP;
    Pk01_Syslog.Write_msg(v_count||' - distinct account_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- ����������� ORDER_ID
PROCEDURE Set_order_id
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Set_order_id';
    v_count          INTEGER := 0;
    v_count_orders   INTEGER := 0;
    v_count_errors   INTEGER := 0;
    v_count_correct  INTEGER := 0;
    v_order_id       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    -- ����������� ������, ��� ����� �������� � �������� � ���� ����
    UPDATE PK213_PINDB_ORDERS_T XO
       SET XO.LOAD_CODE = -1,
           XO.LOAD_STATUS = '��������� ���� �������� � �������� ������'
     WHERE CYCLE_START_T >= CYCLE_END_T
       AND XO.LOAD_CODE IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - CYCLE_START_T >= CYCLE_END_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������, ��� ����� �������� � �������� � ���� ����
    UPDATE PK213_PINDB_ORDERS_T XO
       SET XO.LOAD_CODE = -1,
           XO.LOAD_STATUS = '����������� ���� ������ �������� ������'
     WHERE XO.CYCLE_START_T IS NULL
       AND XO.LOAD_CODE IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - CYCLE_START_T >= CYCLE_END_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ID �������� �����
    MERGE INTO PK213_PINDB_ORDERS_T XO
    USING (
        SELECT DISTINCT X.ACCOUNT_NO, X.BRM_ACCOUNT_ID
          FROM PK213_PINDB_ALL_CONTRACTS_T X
         WHERE X.LOAD_CODE IN ( c_LOAD_CODE_START, c_LOAD_CODE_DBL )
    ) X
    ON (
       XO.ACCOUNT_NO = X.ACCOUNT_NO
       AND XO.LOAD_CODE IS NULL
    )
    WHEN MATCHED THEN UPDATE SET XO.BRM_ACCOUNT_ID = X.BRM_ACCOUNT_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_ACCOUNT_ID '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������, ���� ���� ������ �� �/�
    UPDATE PK213_PINDB_ORDERS_T XO
       SET XO.LOAD_CODE = -1,
           XO.LOAD_STATUS = '������ ��� ���������� ���� PK213_PINDB_ALL_CONTRACTS_T.ACCOUNT_ID'
     WHERE XO.BRM_ACCOUNT_ID IS NULL
       AND XO.LOAD_CODE IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - ACCOUNT_ID - have status error in BRM', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� �� ������������ ������� ������� � �������� SQL-�������
    MERGE INTO PK213_PINDB_ORDERS_T XO
    USING (
        SELECT RID
          FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY XO.ORDER_NO ORDER BY ORDER_DATE DESC) RN,
                   ROWID RID, 
                   XO.* 
              FROM PK213_PINDB_ORDERS_T XO
             WHERE XO.LOAD_CODE IS NULL
        )
        WHERE RN = 2
    ) XOE
    ON (XOE.RID = XO.ROWID)
    WHEN MATCHED THEN UPDATE 
                         SET XO.LOAD_CODE = -1, 
                             XO.LOAD_STATUS = '������������ ������� ������� � �������� SQL-�������'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - ORDER_NO - duplicated in source SQL', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� �� ������������ ������� ������� � BRM
    UPDATE PK213_PINDB_ORDERS_T XO
       SET LOAD_CODE = -1, 
           LOAD_STATUS = '������, ����� ��� ���������� � BRM'
     WHERE EXISTS (
        SELECT * FROM ORDER_T O
         WHERE O.ORDER_NO = XO.ORDER_NO
      )
      AND XO.LOAD_CODE IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - ORDER_NO - duplicated in BRM', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� order_id
    UPDATE PK213_PINDB_ORDERS_T XO 
       SET XO.BRM_ORDER_NO = SUBSTR(TRIM(XO.ORDER_NO),1,100), 
           XO.BRM_ORDER_ID = PK02_POID.NEXT_ORDER_ID,
           XO.BRM_ORDER_BODY_ID = PK02_POID.NEXT_ORDER_BODY_ID
     WHERE LOAD_CODE IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - ORDER_ID - set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- ����������� ���������� ��� ORDER_INFO_T
PROCEDURE Set_order_info
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Set_order_info';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    MERGE INTO PK213_PINDB_ORDERS_T XO
    USING (
        SELECT DISTINCT PO.SPEED_STR, 
               TO_NUMBER( NVL(TRIM(SUBSTR(LTRIM( REPLACE(PO.SPEED_STR,'.',',') ), 1, INSTR(LTRIM(PO.SPEED_STR),' '))), 0) ) SPEED_VALUE,
               D.KEY_ID SPEED_UNIT_ID
          FROM PK213_PINDB_ORDERS_T PO, DICTIONARY_T D 
         WHERE D.PARENT_ID(+) = 67
           AND D.NAME = TRIM(SUBSTR(LTRIM(PO.SPEED_STR), INSTR(LTRIM(PO.SPEED_STR),' ')))
           AND PO.LOAD_STATUS IS NULL
           --AND (PO.RATE_PLAN LIKE 'IP Routing%' OR PO.RATE_PLAN LIKE 'IP Burst%')
           --AND PO.IP_USAGE_RATE_PLAN = 'IP 0'
           AND PO.SPEED_STR IS NOT NULL
    ) XD
    ON (
        XO.SPEED_STR = XD.SPEED_STR
        AND XO.LOAD_CODE IS NULL
    )
    WHEN MATCHED THEN UPDATE SET XO.BRM_SPEED_VALUE = XD.SPEED_VALUE, 
                                 XO.BRM_SPEED_UNIT_ID = XD.SPEED_UNIT_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_INFO: '||v_count||' - rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������������� ������ ������
    UPDATE PK213_PINDB_ORDERS_T XO
       SET BRM_CURRENCY_ID = 
         CASE
            WHEN CURRENCY = 810 AND CURRENCY_SECONDARY = 0 THEN 810
            WHEN CURRENCY_SECONDARY = 286 THEN 286
            ELSE CURRENCY 
         END
     WHERE XO.LOAD_CODE IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_CURRENCY_ID '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- ���������� ID ������
PROCEDURE Set_service_id
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Set_service_id';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    -- ����������� service_id
    MERGE INTO PK213_PINDB_ORDERS_T XO
    USING (
        SELECT DISTINCT S.SERVICE, S.SERVICE_ID 
          FROM PK213_PINDB_ORDERS_T XO, SERVICE_T S -- 2.970
         WHERE XO.SERVICE_NAME = S.SERVICE
           AND S.SERVICE_ID NOT IN (152)
    ) S
    ON (
        XO.SERVICE_NAME = S.SERVICE
        AND XO.LOAD_CODE IS NULL
    )
    WHEN MATCHED THEN UPDATE SET XO.BRM_SERVICE_ID = S.SERVICE_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SERVICE_ID '||v_count||' - rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������, ��� service_id �����������
    UPDATE PK213_PINDB_ORDERS_T XO
       SET LOAD_CODE = -1, 
           LOAD_STATUS = '��� ������ � SERVICE_T'
     WHERE BRM_SERVICE_ID IS NULL
       AND XO.LOAD_CODE IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SERVICE_ID '||v_count||' - rows unknown', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- ���������� ID ��������� ������
PROCEDURE Set_subservice_id
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Set_subservice_id';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  
    
    -- ����������� ���������� �����
    UPDATE PK213_PINDB_ORDERS_T XO
       SET BRM_CHARGE_TYPE = 'USG',
           BRM_SUBSERVICE_ID = 
             CASE 
                WHEN XO.BRM_SERVICE_ID = 142 THEN NULL-- '�����������, �� ��������'
                WHEN XO.BRM_SERVICE_ID = 125 THEN 5 -- '������ ������� ������'
                WHEN XO.BRM_SERVICE_ID IN (140, 167) THEN 6 -- '������ ������� ������'
                ELSE 1 -- '��-��' 2 - ��������� ��� ������������ order_body
             END
     WHERE XO.BRM_SUBSERVICE_ID IS NULL
       AND XO.LOAD_CODE IS NULL;  
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SUBSERVICE_ID '||v_count||' - rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- �������� �/�, ������ ������� �� ����� ����������
PROCEDURE Set_account_error
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Set_account_error';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ��������� �/� � �������� � �������
    MERGE INTO PK213_PINDB_ALL_CONTRACTS_T XC
    USING (
       SELECT DISTINCT XO.ACCOUNT_NO
         FROM PK213_PINDB_ORDERS_T XO
        WHERE XO.LOAD_CODE != 0
    ) XO   
    ON (
       XC.ACCOUNT_NO = XO.ACCOUNT_NO
    )
    WHEN MATCHED THEN UPDATE SET XC.LOAD_CODE = -1, 
                                 XC.LOAD_STATUS = '������ ��� ��������� ������ ������';
    -- 
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


--============================================================================================
-- ������ ������ �� ������� �������� 
--
PROCEDURE Import_data
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_data';
BEGIN

    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  
/*
    Import_contracts;
    Import_orders;
    Import_add_data;
*/
    Set_contractor_id;
    Set_manager_id;
    Set_client_id;
    Set_customer_id;
    Set_contract_id;
    Set_contract_info;
    Set_account_id;
    UPDATE PK213_PINDB_ALL_CONTRACTS_T SET IMPORT_DATE = SYSDATE;
    UPDATE PK213_PINDB_ALL_CONTRACTS_T SET LOAD_CODE = 0 WHERE LOAD_CODE IS NULL;
/*    
    Set_order_id;
    Set_order_info;    
    Set_service_id;
    Set_subservice_id;
    UPDATE PK213_PINDB_ORDERS_T SET IMPORT_DATE = SYSDATE; 
    UPDATE PK213_PINDB_ORDERS_T SET LOAD_CODE = 0 WHERE LOAD_CODE IS NULL;
*/
    -- �������� �/�, ������ ������� �� ����� ����������
    --Set_account_error;

    Gather_Table_Stat(l_Tab_Name => 'PK213_PINDB_ALL_CONTRACTS_T');
    Gather_Table_Stat(l_Tab_Name => 'PK213_PINDB_ORDERS_T');

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- �������� ������
--============================================================================================
-- ����������� ����� ���������� �������
FUNCTION view_step (p_step IN INTEGER) RETURN VARCHAR2
IS 
BEGIN
  RETURN CASE
           WHEN p_step = c_LOAD_CODE_ACC THEN '�������� �/�'
           WHEN p_step = c_LOAD_CODE_CLN THEN '�������� �������'
           WHEN p_step = c_LOAD_CODE_MGR THEN '�������� sale-curator'
           WHEN p_step = c_LOAD_CODE_CTR THEN '�������� ��������'
           WHEN p_step = c_LOAD_CODE_CST THEN '�������� �����������-����������'
           WHEN p_step = c_LOAD_CODE_APF THEN '�������� ������� �/�'
           WHEN p_step = c_LOAD_CODE_AJR THEN '�������� ������ ������������'
           WHEN p_step = c_LOAD_CODE_ADL THEN '�������� ������ ��������'
           WHEN p_step = c_LOAD_CODE_ORD THEN '�������� ������'    
           WHEN p_step = c_LOAD_CODE_SAL THEN '�������� service-alias'
           WHEN p_step = c_LOAD_CODE_SUB THEN '�������� ��������� ������'
           WHEN p_step = c_LOAD_CODE_FIN THEN '�����'
           ELSE '����������� ���'
         END;
END;

-- ------------------------------------------------------------------
-- �������� ������������ ������������ ����������� �����
-- ------------------------------------------------------------------
PROCEDURE Check_subservice( 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_subservice';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
        SELECT SERVICE_ID, SERVICE, SUBSERVICE_ID, SUBSERVICE, COUNT(*) CNT
          FROM (
        SELECT A.ACCOUNT_NO, 
               O.ORDER_NO, O.SERVICE_ID, S.SERVICE, 
               OB.SUBSERVICE_ID, SS.SUBSERVICE
          FROM ACCOUNT_T A, 
               ORDER_T O, SERVICE_T S, 
               ORDER_BODY_T OB, SUBSERVICE_T SS
         WHERE A.BILLING_ID = 2009
           AND A.ACCOUNT_ID = O.ACCOUNT_ID
           AND O.SERVICE_ID = S.SERVICE_ID
           AND O.ORDER_ID   = OB.ORDER_ID(+)
           AND OB.SUBSERVICE_ID = SS.SUBSERVICE_ID(+)
           --AND O.SERVICE_ID NOT IN (125, 140, 167)
         ORDER BY A.ACCOUNT_NO, O.ORDER_NO, OB.SUBSERVICE_ID 
        )
        GROUP BY SERVICE_ID, SERVICE, SUBSERVICE_ID, SUBSERVICE
        ORDER BY SERVICE_ID, SUBSERVICE_ID
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
    

--============================================================================================
-- �������� ������ �� ������� ������
--============================================================================================
PROCEDURE Load_accounts
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Load_accounts';
    v_count           INTEGER := 0;
    v_step            INTEGER := 1;
    v_ok              INTEGER := 0;
    v_error           INTEGER := 0;
    v_load_status     VARCHAR2(1000);
    v_kpp             VARCHAR2(10 CHAR);
    v_inn             VARCHAR2(12 CHAR);
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR r_abn IN c_CTR LOOP
      
      BEGIN
        -- ������� -----------------------------------------------------------------
        --IF r_abn.account_no = 'MS000526' THEN
        --   NULL;
        --END IF;  
      
        -- �������� ��� � ����������� ���� (��� ��������� � ������������ ����������)
        IF LENGTH(r_abn.kpp) > 10 THEN
          v_kpp := NULL;
        ELSE
          v_kpp := r_abn.kpp;
        END IF;
        -- �������� ��� � ����������� ���� (��� ��������� � ������������ ����������)
        IF LENGTH(r_abn.inn) > 12 THEN
          v_inn := NULL;
        ELSE
          v_inn := r_abn.inn;
        END IF;
      
        -- -------------------------------------------------------- --
        -- ������� �/�
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_ACC;        
        SELECT COUNT(*) INTO v_count
          FROM ACCOUNT_T A
         WHERE A.ACCOUNT_ID = r_abn.brm_account_id;
        IF v_count = 0 THEN
            -- ������� ������ �������� �����
            INSERT INTO ACCOUNT_T A(
               ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, 
               CURRENCY_ID, CURRENCY_CONVERSION_ID,
               STATUS, PARENT_ID, NOTES,
               BALANCE, BALANCE_DATE, CREATE_DATE, BILLING_ID,
               EXTERNAL_ID, EXTERNAL_NO, COMMENTARY 
            )VALUES(
               r_abn.brm_account_id, r_abn.brm_account_no, 
               Pk00_Const.c_ACC_TYPE_J, 
               r_abn.brm_currency_id, r_abn.brm_currency_conversion_id,
               'NEW', NULL, 
               'ACCOUNT_NO='||r_abn.ACCOUNT_NO||' ������������� �� Portal65-XTTK'|| TO_CHAR(SYSDATE,'dd.mm.yyyy'), 
               0, SYSDATE, SYSDATE, c_BILLING_ID, 
               r_abn.account_id, r_abn.account_no, r_abn.brm_commentary
            );
            -- ������� ��������� ������ � ������ �������� �����
            Pk07_Bill.New_billinfo (
                         p_account_id    => r_abn.brm_account_id,       -- ID �������� �����
                         p_currency_id   => Pk00_Const.c_CURRENCY_RUB,  -- ID ������ �����
                         p_delivery_id   => r_abn.brm_delivery_method_id,-- ID ������� �������� �����
                         p_days_for_payment => 30           -- ���-�� ���� �� ������ �����
                     );

        END IF;
    
        -- -------------------------------------------------------- --
        -- ������� ������� ��������
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_CLN;
        SELECT COUNT(*) INTO v_count
          FROM CLIENT_T CL
         WHERE CLIENT_ID = r_abn.brm_client_id
        ;
        IF v_count = 0 THEN
            INSERT INTO CLIENT_T (CLIENT_ID, CLIENT_NAME)
            VALUES(r_abn.brm_client_id, r_abn.client);
        END IF;
       
        -- -------------------------------------------------------- --
        -- ������� sale-�������� ��������
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_MGR;
        SELECT COUNT(*) INTO v_count
          FROM MANAGER_T M
         WHERE M.MANAGER_ID = r_abn.brm_sale_curator_id
        ;
        IF v_count = 0 AND r_abn.brm_sale_curator_id IS NOT NULL THEN
            INSERT INTO MANAGER_T (
                MANAGER_ID, CONTRACTOR_ID, 
                LAST_NAME, FIRST_NAME, MIDDLE_NAME, 
                DATE_FROM 
            )VALUES(
                r_abn.brm_sale_curator_id, r_abn.brm_contractor_id, 
                r_abn.brm_sc_lastname, r_abn.brm_sc_firstname, r_abn.brm_sc_middlename, 
                TO_DATE('01.01.2000','dd.mm.yyyy')
            );
        END IF;

        /*
        -- -------------------------------------------------------- --
        -- ������� billing-�������� ��������
        -- -------------------------------------------------------- --
        SELECT COUNT(*) INTO v_count
          FROM MANAGER_T M
         WHERE M.MANAGER_ID = r_abn.brm_billing_curator_id
        ;
        IF v_count = 0 AND r_abn.brm_billing_curator_id IS NOT NULL THEN
            INSERT INTO MANAGER_T (
                MANAGER_ID, CONTRACTOR_ID, 
                LAST_NAME, FIRST_NAME, MIDDLE_NAME, 
                DATE_FROM 
            )VALUES(
                r_abn.brm_billing_curator_id, r_abn.brm_contractor_id, 
                r_abn.brm_bc_lastname, r_abn.brm_bc_firstname, r_abn.brm_bc_middlename, 
                TO_DATE('01.01.2000','dd.mm.yyyy')
            );
        END IF;
        */
        -- -------------------------------------------------------- --
        -- ������� �������
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_CTR;
        SELECT COUNT(*) INTO v_count
          FROM CONTRACT_T C
         WHERE C.CONTRACT_ID = r_abn.brm_contract_id
        ;
        IF v_count = 0 THEN
            INSERT INTO CONTRACT_T C (
              CONTRACT_ID, CONTRACT_NO, 
              MARKET_SEGMENT_ID, CLIENT_TYPE_ID, 
              DATE_FROM, DATE_TO, 
              CLIENT_ID, 
              NOTES
            )VALUES(
              r_abn.brm_contract_id, r_abn.brm_contract_no, 
              r_abn.brm_market_segment_id, r_abn.brm_client_type_id,
              r_abn.custdate, Pk00_Const.c_DATE_MAX, 
              r_abn.brm_client_id, 
              '������������� �� Portal65-XTTK'|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );
            -- ����������� ��������� � �������� (������������ �.������ - 06.06.2016)
            --IF r_abn.brm_sale_curator_id IS NOT NULL THEN 
            --    INSERT INTO SALE_CURATOR_T (MANAGER_ID, CONTRACT_ID, DATE_FROM, DATE_TO)
            --    VALUES(r_abn.brm_sale_curator_id, r_abn.brm_contract_id, 
            --           r_abn.custdate, Pk00_Const.c_DATE_MAX)
            --    ;
            --END IF;
            -- ����������� ��������� � �������� ����� (�.������ - 06.06.2016)
            IF r_abn.brm_sale_curator_id IS NOT NULL THEN 
                INSERT INTO SALE_CURATOR_T (MANAGER_ID, ACCOUNT_ID, DATE_FROM, DATE_TO)
                VALUES(r_abn.brm_sale_curator_id, r_abn.brm_account_id, 
                       r_abn.custdate, Pk00_Const.c_DATE_MAX)
                ;
            END IF;
            /* �� ��������� (�.������ - 17.06.2016)
            -- ����������� ���������� ��� � ��������
            IF r_abn.brm_billing_curator_id IS NOT NULL THEN 
                INSERT INTO BILLING_CURATOR_T (MANAGER_ID, CONTRACT_ID)
                VALUES(r_abn.brm_billing_curator_id, r_abn.brm_contract_id)
                ;
            END IF;
            */
            -- ����������� ��� �������� � ��������
            INSERT INTO COMPANY_T CM(
              CONTRACT_ID, COMPANY_NAME, SHORT_NAME, DATE_FROM, DATE_TO,
              INN, KPP, ERP_CODE
            )VALUES(
              r_abn.brm_contract_id, r_abn.company, r_abn.company, 
              r_abn.custdate, Pk00_Const.c_DATE_MAX,
              v_inn,
              v_kpp,
              r_abn.kontragent
            );
            
        END IF;
    
        -- -------------------------------------------------------- --
        -- ������� ����������
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_CST;
        SELECT COUNT(*) INTO v_count
          FROM CUSTOMER_T CS
         WHERE CUSTOMER_ID = r_abn.brm_customer_id
        ;
        IF v_count = 0 THEN
            INSERT INTO CUSTOMER_T (
                   CUSTOMER_ID, ERP_CODE, INN, KPP, 
                   CUSTOMER, SHORT_NAME, 
                   NOTES
                   )
            VALUES(r_abn.brm_customer_id, r_abn.kontragent, 
                   v_inn,
                   v_kpp, 
                   r_abn.company, r_abn.company, 
                   '������������� �� Portal65-XTTK '||TO_CHAR(SYSDATE,'dd.mm.yyyy')
                   )  
            ;
        END IF;
    
        -- -------------------------------------------------------- --
        -- ������� ������� �/�
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_APF;
        SELECT COUNT(*) INTO v_count
          FROM ACCOUNT_PROFILE_T AP
         WHERE AP.PROFILE_ID = r_abn.brm_profile_id;
        IF v_count = 0 THEN
            INSERT INTO ACCOUNT_PROFILE_T (
               PROFILE_ID, ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID,
               CONTRACTOR_ID, BRANCH_ID, AGENT_ID,  
               CONTRACTOR_BANK_ID, VAT, DATE_FROM, DATE_TO, KPP, ERP_CODE)
            VALUES
               (r_abn.brm_profile_id, 
                r_abn.brm_account_id, r_abn.brm_contract_id, r_abn.brm_customer_id, 
                r_abn.brm_contractor_id, r_abn.brm_branch_id, r_abn.brm_agent_id, 
                r_abn.brm_contractor_bank_id, Pk00_Const.c_VAT, 
                r_abn.custdate, NULL,
                v_kpp, 
                r_abn.kontragent
                )
            ;
        END IF;
   
        -- -------------------------------------------------------- --
        -- ������� ����������� �����
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_AJR;
        SELECT COUNT(*) INTO v_count
          FROM ACCOUNT_CONTACT_T AC
         WHERE 
            (AC.CONTACT_ID = r_abn.brm_jur_address_id)
            or
            (
                AC.ACCOUNT_ID = r_abn.brm_account_id
                and
                AC.ADDRESS_TYPE = PK00_CONST.c_ADDR_TYPE_JUR
            )
        ;
        IF v_count = 0 THEN
            INSERT INTO ACCOUNT_CONTACT_T (   
                CONTACT_ID,ADDRESS_TYPE,ACCOUNT_ID,
                COUNTRY,ZIP,STATE,CITY,ADDRESS,DATE_FROM,
                NOTES
            )VALUES(
                r_abn.brm_jur_address_id, PK00_CONST.c_ADDR_TYPE_JUR, r_abn.brm_account_id,
                '��', r_abn.jur_zip, NULL, 
                r_abn.jur_city, r_abn.jur_address, r_abn.custdate, 
                '������������� �� Portal65-XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );  
        END IF;
        
        -- -------------------------------------------------------- --
        -- ������� ����� ��������
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_ADL;
        SELECT COUNT(*) INTO v_count
          FROM ACCOUNT_CONTACT_T AC
         WHERE 
            (AC.CONTACT_ID = r_abn.brm_dlv_address_id)
            or
            (
                AC.ACCOUNT_ID = r_abn.brm_account_id
                and
                AC.ADDRESS_TYPE = PK00_CONST.c_ADDR_TYPE_DLV
            )
        ;
        IF v_count = 0 THEN
            INSERT INTO ACCOUNT_CONTACT_T (   
                CONTACT_ID,ADDRESS_TYPE,ACCOUNT_ID,
                COUNTRY,ZIP,STATE,CITY,ADDRESS,EMAIL,DATE_FROM,
                NOTES,
                PERSON, PHONES, FAX
            )VALUES(
                r_abn.brm_dlv_address_id, PK00_CONST.c_ADDR_TYPE_DLV, r_abn.brm_account_id,
                '��', r_abn.phis_zip, NULL, 
                r_abn.phis_city, r_abn.phis_address, r_abn.email_addr, r_abn.custdate, 
                '������������� �� Portal65-XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy'),
                r_abn.PHIS_NAME, r_abn.PHONE, r_abn.FAX
            );  
        END IF;
    
        -- -------------------------------------------------------- --
        -- ������� ������� ����������� �������
        v_ok := v_ok + 1;
        
        UPDATE PK213_PINDB_ALL_CONTRACTS_T
           SET LOAD_CODE   = v_step,
               LOAD_STATUS = 'OK'
         WHERE CURRENT OF c_CTR;
        
        IF MOD(v_ok, 100) = 0 THEN
            Pk01_Syslog.Write_msg(v_ok||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
      EXCEPTION
         -- -------------------------------------------------------- --
         -- ��������� ������ �������� ������ 
         WHEN OTHERS THEN
            v_load_status := 'ERROR, ��� => '||view_step(v_step)||'. '
                            ||Pk01_Syslog.get_OraErrTxt(c_PkgName||'.'||v_prcName);
            UPDATE PK213_PINDB_ALL_CONTRACTS_T
               SET LOAD_STATUS = v_load_status,
                   LOAD_CODE   = -v_step
             WHERE CURRENT OF c_CTR;

            Pk01_Syslog.Write_msg('contract_no='||r_abn.CONTRACT_NO||
                                ', account_no=' ||r_abn.ACCOUNT_NO||
                                ' => '||v_load_status
                                , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );            

            v_error := v_error + 1;
      END;
    END LOOP;

    Pk01_Syslog.Write_msg('Report: '||v_ok||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE PK213_PINDB_ALL_CONTRACTS_T SET LOAD_DATE = SYSDATE;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- �������� ������ �� �������
--============================================================================================
PROCEDURE Load_orders
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Load_orders';
    v_count           INTEGER := 0;
    v_step            INTEGER := 1;
    v_ok              INTEGER := 0;
    v_error           INTEGER := 0;
    v_load_status     VARCHAR2(1000);
   
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR r_ord IN c_ORD LOOP
      
      BEGIN
        -- -------------------------------------------------------- --
        -- ������� �����, ������ ���������� �����
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_ORD;
        SELECT COUNT(*) INTO v_count
          FROM ORDER_T O
         WHERE O.ORDER_ID = r_ord.brm_order_id
        ;
        IF v_count = 0 THEN
            -- ������� ������ ������
            INSERT INTO ORDER_T (
               ORDER_ID, ORDER_NO, ACCOUNT_ID, SERVICE_ID, 
               RATEPLAN_ID, 
               DATE_FROM, DATE_TO, 
               CREATE_DATE, MODIFY_DATE, 
               TIME_ZONE, NOTES
            )VALUES(
               r_ord.brm_order_id, r_ord.brm_order_no, r_ord.brm_account_id, 
               r_ord.brm_service_id, 
               c_RATEPLAN_BILSRV_ID, -- ����������� �����������, ��� ������� �������� ������������
               NVL(r_ord.order_date, r_ord.cycle_start_t),
               CASE 
                 WHEN r_ord.cycle_end_t IS NULL THEN TO_DATE('01.01.2050','dd.mm.yyyy')
                 ELSE r_ord.cycle_end_t-1/86400
               END,
               SYSDATE, SYSDATE, NULL, 
               '������������� �� Portal65-XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );
            -- ��������� ���������� � ������ �����������
            INSERT INTO ORDER_INFO_T( 
                   ORDER_ID, POINT_SRC, POINT_DST, 
                   SPEED_STR, SPEED_VALUE, SPEED_UNIT_ID, 
                   DOWNTIME_FREE )
            VALUES( 
                   r_ord.brm_order_id, r_ord.s_rgn, r_ord.d_rgn, 
                   r_ord.speed_str, r_ord.brm_speed_value, r_ord.brm_speed_unit_id,
                   r_ord.free_downtime );
        END IF;

        -- -------------------------------------------------------- --
        -- ��������� ���������� ������
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_SUB;
  
        IF r_ord.brm_subservice_id    = 1 THEN -- '��-��' ��������� 2 ���������� ��(1) � ��(2)
            -- ���������� '��' - cdzpm
            INSERT INTO ORDER_BODY_T(
                ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
                DATE_FROM, DATE_TO, 
                RATE_RULE_ID, RATEPLAN_ID,
                CREATE_DATE, MODIFY_DATE
            ) VALUES (
                r_ord.brm_order_body_id, 
                r_ord.brm_order_id, 
                r_ord.brm_subservice_id, 
                r_ord.brm_charge_type,
                r_ord.cycle_start_t, 
                CASE
                  WHEN r_ord.cycle_end_t IS NULL THEN Pk00_Const.c_DATE_MAX
                  ELSE r_ord.cycle_end_t-1/86400
                END, 
                c_RATERULE_BILSRV_ID, c_RATEPLAN_BILSRV_ID,
                SYSDATE, SYSDATE
            );
            -- ���������� '��' - �����
            INSERT INTO ORDER_BODY_T(
                ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
                DATE_FROM, DATE_TO, 
                RATE_RULE_ID, RATEPLAN_ID,
                CREATE_DATE, MODIFY_DATE
            ) VALUES (
                PK02_POID.NEXT_ORDER_BODY_ID, 
                r_ord.brm_order_id, 
                2, 
                r_ord.brm_charge_type,
                r_ord.cycle_start_t, 
                CASE
                  WHEN r_ord.cycle_end_t IS NULL THEN Pk00_Const.c_DATE_MAX
                  ELSE r_ord.cycle_end_t-1/86400
                END, 
                c_RATERULE_BILSRV_ID, c_RATEPLAN_BILSRV_ID,
                SYSDATE, SYSDATE
            );

        ELSIF r_ord.brm_subservice_id = 5 OR   -- ������� ����������
              r_ord.brm_subservice_id = 6 THEN -- ������������� �������������
            --
            INSERT INTO ORDER_BODY_T(
                ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
                DATE_FROM, DATE_TO, 
                RATE_RULE_ID, RATEPLAN_ID,
                CREATE_DATE, MODIFY_DATE
            ) VALUES (
                r_ord.brm_order_body_id, 
                r_ord.brm_order_id, 
                r_ord.brm_subservice_id, 
                r_ord.brm_charge_type,
                r_ord.cycle_start_t, 
                CASE
                  WHEN r_ord.cycle_end_t IS NULL THEN Pk00_Const.c_DATE_MAX
                  ELSE r_ord.cycle_end_t-1/86400
                END, 
                c_RATERULE_BILSRV_ID, c_RATEPLAN_BILSRV_ID,
                SYSDATE, SYSDATE
            );
        ELSIF r_ord.brm_subservice_id IS NULL THEN
           NULL; -- SERVICE_ID = 142, '������������� � ������� ������� �������� ������'
                 -- ������ ORDER_BODY_T ����������� '�����������, �� ��������'
                 -- ��� �������� �������
        END IF;

        -- -------------------------------------------------------- --
        -- ������� ������� ����������� �������
        v_ok := v_ok + 1;
        
        UPDATE PK213_PINDB_ORDERS_T
           SET LOAD_CODE   = v_step,
               LOAD_STATUS = 'OK'
         WHERE CURRENT OF c_ORD;
        
        IF MOD(v_ok, 100) = 0 THEN
            Pk01_Syslog.Write_msg(v_ok||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
      EXCEPTION
         -- -------------------------------------------------------- --
         -- ��������� ������ �������� ������ 
         WHEN OTHERS THEN
            v_load_status := 'ERROR, ��� => '||view_step(v_step)||'. '
                            ||Pk01_Syslog.get_OraErrTxt(c_PkgName||'.'||v_prcName);
            UPDATE PK213_PINDB_ORDERS_T
               SET LOAD_STATUS = v_load_status,
                   LOAD_CODE   = -v_step
             WHERE CURRENT OF c_ORD;

            Pk01_Syslog.Write_msg('contract_no='||r_ord.CONTRACT_NO||
                                ', account_no=' ||r_ord.ACCOUNT_NO||
                                ', order_no='   ||r_ord.ORDER_NO||
                                ' => '||v_load_status
                                , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );            

            v_error := v_error + 1;
      END;
    END LOOP;

    Pk01_Syslog.Write_msg('Report: '||v_ok||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    UPDATE PK213_PINDB_ORDERS_T SET LOAD_DATE = SYSDATE;
    
    -- - - - - - - - - - - - - - - -- - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- ��������� ����������� ������ � �����
--============================================================================================
PROCEDURE Move_to_archive
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Move_to_archive';
    v_count           INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - -- - - - - - - - - - - - - - - - - - - - - - - - --
    INSERT INTO PK213_PINDB_ALL_CONTRACTS_ARX
    SELECT * FROM PK213_PINDB_ALL_CONTRACTS_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_ALL_CONTRACTS_ARX '||v_count||' - rows insert', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    DELETE FROM PK213_PINDB_ALL_CONTRACTS_T;
    
    INSERT INTO PK213_PINDB_ORDERS_ARX
    SELECT * FROM PK213_PINDB_ORDERS_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_ORDERS_ARX '||v_count||' - rows insert', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    DELETE FROM PK213_PINDB_ORDERS_T;
    -- - - - - - - - - - - - - - - -- - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

-- =========================================================== --
-- ������������ ������
-- =========================================================== --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������� ������ ��� �������� �������
-- ��������� ��� ������� �� ����������� ���������
-- � ����������� ������� p_period_id
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Create_new_bills(p_period_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Create_new_bills';
    v_bill_id       INTEGER;
    v_count         INTEGER;
    v_error         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������� ��������� ������ ��� �� ���
    v_count := 0;
    FOR bi IN (
      SELECT DISTINCT X.BRM_ACCOUNT_ID  
        FROM PK213_PINDB_ALL_CONTRACTS_T X
       WHERE X.LOAD_CODE > 0
         AND NOT EXISTS (
            SELECT * FROM BILLINFO_T BI
             WHERE BI.ACCOUNT_ID = X.BRM_ACCOUNT_ID
         )
    )
    LOOP
       -- ������� ��������� ������ � ������ �������� �����
       Pk07_Bill.New_billinfo (
                   p_account_id    => bi.brm_account_id,   -- ID �������� �����
                   p_currency_id   => Pk00_Const.c_CURRENCY_RUB,  -- ID ������ �����
                   p_delivery_id   => c_DLV_METHOD_AP,-- ID ������� �������� �����
                   p_days_for_payment => 30           -- ���-�� ���� �� ������ �����
               );  
    
       v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('Billinfo_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������������� ����� � ����������� ������� ��� �/� ��� �� ���
    v_count := 0;
    v_error := 0;
    --
    FOR rb IN (
      SELECT DISTINCT X.BRM_ACCOUNT_ID  
        FROM PK213_PINDB_ALL_CONTRACTS_T X
       WHERE X.LOAD_CODE > 0
         AND NOT EXISTS (
            SELECT * FROM BILL_T B
             WHERE B.ACCOUNT_ID    = X.BRM_ACCOUNT_ID
               AND B.REP_PERIOD_ID = p_period_id
               AND B.BILL_TYPE     = PK00_CONST.c_BILL_TYPE_REC
         )
    )LOOP
      BEGIN
      v_bill_id := Pk07_BIll.Next_recuring_bill (
               p_account_id    => rb.brm_account_id, -- ID �������� �����
               p_rep_period_id => p_period_id    -- ID ���������� ������� YYYYMM
           );
      EXCEPTION WHEN OTHERS THEN
          Pk01_Syslog.Write_error( 'ERROR', c_PkgName||'.'||v_prcName );
          v_error := v_error + 1;
      END;
      v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('Bill_t: '||v_count||' rows created, '||v_error||' - error', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- ������������ ����� �������� ��.��� �������� (�� ���� ����������) 
-- ------------------------------------------------------------------------- --
PROCEDURE Make_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_bills';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������������ ��� �/�
    UPDATE ACCOUNT_T A SET STATUS = 'B'
     WHERE A.BILLING_ID = 2008
       AND A.STATUS != 'B';
    
    Pk30_Billing.Make_Region_bills(p_period_id);
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


END PK213_IMPORT_PORTAL65_VOICE;
/
