CREATE OR REPLACE PACKAGE PK39_BILLING_DISCOUNT_IMPORT
IS
    --
    -- ����� ��� �������� �������� ������ xTTK ������-������������� �������һ
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK39_BILLING_DISCOUNT_IMPORT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- ������� � ������� ��������� �������� NPL, ��� ������������
    c_BILLING_DISC   CONSTANT INTEGER := 2097; -- �������� ������� ����
    
    -- ����������, ������� ����� �������� �������� ������� ��������
    c_CONTRACTOR_ID  CONSTANT INTEGER := 1;
    c_BANK_ID        CONSTANT INTEGER := 4;
    -- ��������� ������������
    �_RATESYS_ABP_ID CONSTANT INTEGER := 1207; -- ����������� ������ ���������, ����� � ORDER_BODY_T
    -- ��������� �������� ������
    c_RATEPLAN_NPL_RUR        CONSTANT INTEGER := 80043; -- 'NPL_RUR'
    c_RATEPLAN_NPL            CONSTANT INTEGER := 80045; -- 'NPL'
    c_RATEPLAN_RRW_RUR        CONSTANT INTEGER := 80046; -- 'RRW RUR'
    c_RATEPLAN_IP_ROUTING_RUR CONSTANT INTEGER := 80047; -- 'IP Routing RUR'
    
    c_LDSTAT_DBL_ORD CONSTANT INTEGER := -1;   -- ����� ��� ���� � BRM
    c_LDSTAT_NOT_SRV CONSTANT INTEGER := -2;   -- ������ �� �������
    
    c_MAX_DATE_TO    CONSTANT DATE := TO_DATE('01.01.2050','dd.mm.yyyy');
    

    --============================================================================================
    -- ����� ������� ������������ ��������� �/� �� c_BILLING_NPL -> Pk00_Const.c_BILLING_OLD
    --============================================================================================
    PROCEDURE Change_billing_id;

    --============================================================================================
    -- ��������� �������� ������������ ������ PINDB � BRM.AG_IMPORT_NPL_T
    --============================================================================================
    PROCEDURE Check_data;    
    
    -- ----------------------------------------------------------------------------- --
    -- ������� ������ �� ������� �������� �� ������ '.SPB TTK Brand'
    -- � ������ �������� � �������� �������������
    --
    PROCEDURE Import_data;
    
    -- �������� ���������� � ����������
    PROCEDURE Load_managers;
    
    -- �������� ���������� � ��������
    PROCEDURE Load_clients;
    
    -- �������� ���������� � ��������� - �����������
    PROCEDURE Load_customers;
    
    -- �������� ���������� � ���������
    PROCEDURE Load_contracts;
    
    -- �������� ���������� � ������� ������ ��������
    PROCEDURE Load_accounts;
    
    -- �������� ���������� � �������
    PROCEDURE Load_orders;

    -- ����� ������
    PROCEDURE Rollback_bills;
      
    -- ����� ���������� � �������
    PROCEDURE Rollback_orders;
    
    -- ����� ���������� � ��������
    PROCEDURE Rollback_contracts;
    
    -- ����� ���������� � ������� ������
    PROCEDURE Rollback_accounts;
    
    --============================================================================================
    -- ������������ ������ �� �������� ������
    --============================================================================================
    PROCEDURE Make_report;

    -- �������� ������ � ����������� ��������
    FUNCTION View_result( 
                   p_recordset    OUT t_refc
               ) RETURN INTEGER;
    
END PK39_BILLING_DISCOUNT_IMPORT;
/
CREATE OR REPLACE PACKAGE BODY PK39_BILLING_DISCOUNT_IMPORT
IS

--============================================================================================
-- ����� ������� ������������ ��������� �/� �� c_BILLING_DISC -> Pk00_Const.c_BILLING_OLD
--============================================================================================
PROCEDURE Change_billing_id
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Check_data';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE ACCOUNT_T A
       SET A.BILLING_ID = Pk00_Const.c_BILLING_OLD
     WHERE A.BILLING_ID = c_BILLING_DISC;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T.BILLING_ID: '||v_count||' rows c_BILLING_DISC -> c_BILLING_OLD', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- ��������� �������� ������������ ������ PINDB � BRM.AG_IMPORT_NPL_T
--============================================================================================
PROCEDURE Check_data
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Check_data';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� BRANCH_ID, ������� ��� ���� � BRM:
    MERGE INTO AG_IMPORT_NPL_T N 
    USING (
        SELECT BRAND, CONTRACTOR_ID FROM BRAND_T
    ) BR 
    ON (
        BR.BRAND = N.BRAND
    )
    WHEN MATCHED THEN UPDATE SET N.BRANCH_ID = BR.CONTRACTOR_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('AG_IMPORT_NPL_T.BRANCH_ID: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� CLIENT_ID, ������� ��� ���� � BRM:
    MERGE INTO AG_IMPORT_NPL_T N
    USING (
     SELECT CLIENT_NAME, CLIENT_ID
       FROM (
          SELECT CLIENT_NAME, CLIENT_ID, MAX(CLIENT_ID) OVER (PARTITION BY CLIENT_NAME) MAX_CLIENT_ID 
            FROM CLIENT_T
       )
      WHERE CLIENT_ID = MAX_CLIENT_ID
    ) CL
    ON (
      N.COMPANY = CL.CLIENT_NAME
    )
    WHEN MATCHED THEN UPDATE SET N.CLIENT_ID = CL.CLIENT_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('AG_IMPORT_NPL_T.CLIENT_ID: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );   

    -- ����������� CUSTOMER_ID, ������� ��� ���� � BRM:
    MERGE INTO AG_IMPORT_NPL_T N
    USING (
     SELECT CUSTOMER, CUSTOMER_ID
       FROM (
          SELECT CUSTOMER, CUSTOMER_ID, MAX(CUSTOMER_ID) OVER (PARTITION BY CUSTOMER) MAX_CUSTOMER_ID 
            FROM CUSTOMER_T
       )
      WHERE CUSTOMER_ID = MAX_CUSTOMER_ID
    ) CS
    ON (
      N.COMPANY = CS.CUSTOMER
    )
    WHEN MATCHED THEN UPDATE SET N.CUSTOMER_ID = CS.CUSTOMER_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('AG_IMPORT_NPL_T.CUSTOMER_ID: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� CONTRACT_ID, ������� ��� ���� � BRM:
    MERGE INTO AG_IMPORT_NPL_T N
    USING (
     SELECT CONTRACT_NO, CONTRACT_ID
       FROM CONTRACT_T
    ) CT
    ON (
      N.ACCOUNT_NO = CT.CONTRACT_NO
    )
    WHEN MATCHED THEN UPDATE SET N.CONTRACT_ID = CT.CONTRACT_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('AG_IMPORT_NPL_T.CONTRACT_ID: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ACCOUNT_ID, ������� ��� ���� � BRM:
    MERGE INTO AG_IMPORT_NPL_T N
    USING (
      SELECT ACCOUNT_NO, ACCOUNT_ID 
        FROM ACCOUNT_T
    ) A
    ON (
      N.CUSTNO = A.ACCOUNT_NO
    )
    WHEN MATCHED THEN UPDATE SET N.ACCOUNT_ID = A.ACCOUNT_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('AG_IMPORT_NPL_T.ACCOUNT_ID: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ORDER_ID, ������� ��� ���� � BRM:
    MERGE INTO AG_IMPORT_NPL_T N
    USING (
      SELECT ORDER_NO, ORDER_ID 
        FROM ORDER_T
    ) O
    ON (
      N.ORDER_NO = O.ORDER_NO
    )
    WHEN MATCHED THEN UPDATE SET N.ORDER_ID = O.ORDER_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('AG_IMPORT_NPL_T.ORDER_ID: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������, ������� ����� �������� �������� ������� ��������
    UPDATE AG_IMPORT_NPL_T SET CONTRACTOR_ID = c_CONTRACTOR_ID,
                               CONTRACTOR_BANK_ID = c_BANK_ID;

    -- �������� ���������� �� AG_IMPORT_NPL_T
    Gather_Table_Stat(l_Tab_Name => 'AG_IMPORT_NPL_T');

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- ������� ������ �� ������� �������� �� ������ '.SPB TTK Brand'
-- � ������ �������� � �������� �������������
--
PROCEDURE Import_data
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_data';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ����������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    --
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PINDB_NPL_ORDERS_T DROP STORAGE';
    --
    -- ������ �������� �� ������� �.�.������
    --
    INSERT INTO PINDB_NPL_ORDERS_T (
      BRAND, ACCOUNT_NO, CUSTNO, COMPANY, ORDER_NO, STATUS,
      RATE_PLAN,IP_USAGE_RATE_PLAN, CYCLE_FEE_AMT, CURRENCY, CURRENCY_SECONDARY,
      CYCLE_START_T, CYCLE_END_T, SMC_START_T, SMC_END_T, ORDER_DATE,
      S_RGN, D_RGN, SERVICE_NAME, SPEED_STR, FREE_DOWNTIME)
    select a.gl_segment brand,/*a.poid_id0 account_poid,*/ ''''||ci.auto_no account_no, a.account_no custno, an.company, /*s.poid_id0 service_poid,*/ s.login order_no, 
            ap.status, p.name rate_plan, ap.descr ip_usage_rate_plan,
           ap.cycle_fee_amt, A.CURRENCY, A.CURRENCY_SECONDARY, 
           i2d@PINDB(ap.cycle_start_t) cycle_start_t, i2d@PINDB(ap.cycle_end_t) cycle_end_t,
           i2d@PINDB(ap.smc_start_t) smc_start_t, i2d@PINDB(ap.smc_end_t) smc_end_t, 
           VS.ORDER_DATE, VS.S_RGN, VS.D_RGN, VS.SERVICE_NAME, VS.SPEED_STR, VS.FREE_DOWNTIME
    from account_t@PINDB a 
         inner join account_products_t@PINDB ap on a.poid_id0 = ap.obj_id0
         inner join account_nameinfo_t@PINDB an on a.poid_id0 = an.obj_id0 and an.rec_id = 1
         inner join plan_t@PINDB p on ap.plan_obj_id0 = p.poid_id0
         inner join service_t@PINDB s on ap.service_obj_id0 = s.poid_id0
         inner join profile_t@PINDB pr on a.poid_id0 = pr.account_obj_id0
         inner join contract_info_t@PINDB ci on pr.poid_id0 = ci.obj_id0
         inner join v_all_data_serv_plus@PINDB vs on S.POID_ID0 = VS.POID_ID0
    where a.gl_segment not like '%RP%' 
      and A.ACCOUNT_NO not like 'RP%' 
      and a.poid_id0 <> a.brand_obj_id0 
      and ap.status = 1 -- �.������� ������� ��� ����������� ���=����
      and a.merchant <> 'MIGRATION'
        --and a.poid_id0 not in (select distinct account_obj_id0 from service_t s where S.POID_TYPE <> '/service/npl') 
    order by a.gl_segment, ci.auto_no, s.login;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_NPL_ORDERS_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������ ������
    DELETE FROM PINDB_NPL_ORDERS_T PO
     WHERE PO.ORDER_NO NOT IN (
        -- 1) VPN
        --'09K032831/11',
        --'09K032831/4', 
        --'09K032831/24',
        --'09K032831/1',
        --'09K032831/7',
        --'09K032831/20',
        --'09K032831/12',
        --'09K032831/10', --
        --'09K032831/17', -- 
        --'07K2751/14',
        -- 2)  IP-access - �������������� �� ������.
        '2/4K013555/CON'
        -- 3)  BURST (��� � ���� ���� � ����� ����)
        --'15K003248',
        --'15K003252',
        --'15K003464',
        --'15K003557',
        --'15K003572',
        --'15K003582',
        --'15K003681',
        --'15K003874',
        --'15K004144',
        --'15K004451'
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_NPL_ORDERS_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    Gather_Table_Stat(l_Tab_Name => 'PINDB_NPL_ORDERS_T');
    COMMIT;

    -- ��������� �����
    SELECT COUNT(*) INTO v_count
      FROM PINDB_NPL_ORDERS_T;
    Pk01_Syslog.Write_msg('PINDB_NPL_ORDERS_T: '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PINDB_NPL_ORDERS_T');

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ����������� ������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PINDB_ALL_CONTRACTS_T DROP STORAGE';
    --
    -- account_no - ����� ��������,
    -- custno - ����� �/�
    -- ��������� - ����������. ���� ���, ���������.
    --
    INSERT INTO PINDB_ALL_CONTRACTS_T(
        ACCOUNT_ID, ACCOUNT_NO, CUSTNO, 
        CLIENT_ID, KONTRAGENT, CLIENT, CUST_TYPE, CUSTDATE, 
        JUR_ZIP, JUR_CITY, JUR_ADDRESS, 
        PHIS_ZIP, PHIS_CITY, PHIS_ADDRESS, PHONE, FAX, EMAIL_ADDR, PHIS_NAME, 
        INN, OKONH, OKPO, BANK, SETTLEMENT, CORR, BIC, KPP, 
        COMPANY, CURRENCY, CURRENCY_SECONDARY, ORIGINAL, TAX_VAT, TAX_SALES, 
        SALES_NAME, DIRECTORATE, MARKET_SEG, BILLING_CURATOR, 
        GL_SEGMENT, IACCOUNT, AGENT_CODE, AGENT_NAME, PPTS_FLAG, DELIVERY
    )
    SELECT * 
      from PIN.V_ALL_CONTRACTS@PINDB c 
     WHERE CUSTNO IN (
           SELECT DISTINCT CUSTNO FROM PINDB_NPL_ORDERS_T PO
       )
       AND NOT EXISTS (
            SELECT * FROM ACCOUNT_T A
             WHERE C.CUSTNO = A.ACCOUNT_NO
       );

    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_ALL_CONTRACTS_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PINDB_ALL_CONTRACTS_T');

    -- ��������� �������������� �������� (������)
    /* ���� �� ���� ��� ������ � ������������� ����������
    GL_SEGMENT
    .Chita TTK RP Brand
    .Sever TTK RP Brand
    .Sever TTK RP Access
    .Kaliningrad TTK RP Brand
    .Centre TTK RP Brand
    .Samara TTK RP Brand
    .Sakhalin TTK RP Brand
    */
    MERGE INTO PINDB_ALL_CONTRACTS_T AC
    USING (
        SELECT BRAND, CONTRACTOR_ID FROM PORTAL_BRAND_T
    ) BR
    ON (
        AC.GL_SEGMENT = BR.BRAND
    ) 
    WHEN MATCHED THEN UPDATE SET AC.BRANCH_ID = BR.CONTRACTOR_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_ALL_CONTRACTS_T.BRANCH_ID: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- �������� ��������� �������������� ����������
    UPDATE PINDB_ALL_CONTRACTS_T SET CONTRACTOR_ID = Pk00_Const.c_CONTRACTOR_KTTK_ID;
    UPDATE PINDB_ALL_CONTRACTS_T SET CONTRACTOR_BANK_ID = 4;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ����������� ����� ����������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PINDB_CONTRACTOR_BANK_T DROP STORAGE';

    INSERT INTO PINDB_CONTRACTOR_BANK_T( 
        ACCOUNT_POID, ACCOUNT_NO, CONTRACTOR_POID, CONTRACTOR, BANK, SETTLEMENT
    )
    select A.POID_ID0 account_poid, A.ACCOUNT_NO, C.IACC_OBJ_ID0 contractor_poid, 
           PN.COMPANY CONTRACTOR, PC.BANK, PC.SETTLEMENT
    from account_t@PINDB a 
        inner join profile_t@PINDB p on A.POID_ID0 = P.ACCOUNT_OBJ_ID0
        inner join contract_info_t@PINDB c on P.POID_ID0 = C.OBJ_ID0
        inner join account_nameinfo_t@PINDB pn on C.IACC_OBJ_ID0 = PN.OBJ_ID0 and PN.REC_ID = 1
        inner join profile_t@PINDB pr on C.IACC_OBJ_ID0 = PR.ACCOUNT_OBJ_ID0
        inner join contract_info_t@PINDB pc on PR.POID_ID0 = PC.OBJ_ID0
    WHERE EXISTS (
        SELECT * FROM PINDB_ALL_CONTRACTS_T PC
         WHERE PC.CUSTNO = A.ACCOUNT_NO
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_CONTRACTOR_BANK_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ����������� ���� ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PINDB_CONTRACT_TYPE_T DROP STORAGE';
   
    INSERT INTO PINDB_CONTRACT_TYPE_T ( CONTRACT_NO, CONTRACT_TYPE_ID )
    SELECT b.auto_no CONTRACT_NO, b.client_cat_id CONTRACT_TYPE_ID
      from contract_info_t@pindb b --where b.client_cat_id = 4
     WHERE CLIENT_CAT_ID > 0
       AND EXISTS (
          SELECT * 
            FROM PINDB_ALL_CONTRACTS_T PC
           WHERE PC.ACCOUNT_NO = b.AUTO_NO
       )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_CONTRACT_TYPE_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );


    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- �������� ���������� � ����������
--============================================================================================
PROCEDURE Load_managers
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Load_managers';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- -------------------------------------------------------------------------- --
    -- ������� ������������ ���������� (�������������)
    -- -------------------------------------------------------------------------- --
    INSERT INTO MANAGER_T M (MANAGER_ID, CONTRACTOR_ID, LAST_NAME, DATE_FROM)
    SELECT SQ_MANAGER_ID.NEXTVAL MANAGER_ID, BRANCH_ID, SALES_NAME, TO_DATE('01.01.2015','dd.mm.yyyy') DATE_FROM 
      FROM (
        SELECT DISTINCT
               BRANCH_ID,
               SALES_NAME, 
               SALES_NAME LAST_NAME,
               NULL FIRST_NAME,
               NULL MIDDLE_NAME 
          FROM PINDB_ALL_CONTRACTS_T P
        WHERE SALES_NAME != 'Intercompany'
          AND SALES_NAME LIKE '%������%'  -- ��� ������
          AND NOT EXISTS (
            SELECT * FROM MANAGER_T M
             WHERE P.SALES_NAME LIKE M.LAST_NAME||'%'
               AND M.LAST_NAME IS NOT NULL
          )
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('MANAGER_T.SALES_CURATOR: '||v_count||' rows inserted (������ ���������� ������, ��� �������)', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- -------------------------------------------------------------------------- --
    -- ������� ���������� ���������� (SALE_CURATOR)
    -- -------------------------------------------------------------------------- --
    INSERT INTO MANAGER_T M (MANAGER_ID, CONTRACTOR_ID, DEPARTMENT, LAST_NAME, FIRST_NAME, MIDDLE_NAME, DATE_FROM)
    SELECT SQ_MANAGER_ID.NEXTVAL MANAGER_ID, BRANCH_ID, DIRECTORATE, 
           LAST_NAME, FIRST_NAME, MIDDLE_NAME, TO_DATE('01.01.2015','dd.mm.yyyy') DATE_FROM 
      FROM (
     SELECT BRANCH_ID, SALES_NAME, 
            NVL(LAST_NAME, SALES_NAME) LAST_NAME,
            DECODE(LAST_NAME, NULL, NULL, FIRST_NAME) FIRST_NAME,
            DECODE(LAST_NAME, NULL, NULL, MIDDLE_NAME) MIDDLE_NAME,
            DIRECTORATE
       FROM (    
        SELECT DISTINCT
               1 BRANCH_ID,
               SALES_NAME, 
               SUBSTR(SALES_NAME, 1, INSTR(SALES_NAME,' ',1)-1) LAST_NAME,
               SUBSTR(SALES_NAME, INSTR(SALES_NAME,' ',1)+1, 2) FIRST_NAME,
               SUBSTR(SALES_NAME, INSTR(SALES_NAME,'.',1)+1, 2) MIDDLE_NAME,
               DIRECTORATE
          FROM PINDB_ALL_CONTRACTS_T P
        WHERE SALES_NAME != 'Intercompany'
          AND SALES_NAME NOT LIKE '%������%'  -- ��� ������
          AND NOT EXISTS (
            SELECT * FROM MANAGER_T M
             WHERE P.SALES_NAME LIKE M.LAST_NAME||'%'
               AND M.LAST_NAME IS NOT NULL
          )
       )
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('MANAGER_T.SALES_CURATOR: '||v_count||' rows inserted (������ ����������)', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- -------------------------------------------------------------------------- --
    -- ������� ���������� ����������� ��� (BILLING_CURATOR)
    -- -------------------------------------------------------------------------- --
    INSERT INTO MANAGER_T M (MANAGER_ID, CONTRACTOR_ID, DEPARTMENT, LAST_NAME, FIRST_NAME, MIDDLE_NAME, DATE_FROM)
    SELECT SQ_MANAGER_ID.NEXTVAL MANAGER_ID, CONTRACTOR_ID, DIRECTORATE, 
           LAST_NAME, FIRST_NAME, MIDDLE_NAME, TO_DATE('01.01.2015','dd.mm.yyyy') DATE_FROM 
      FROM (
     SELECT CONTRACTOR_ID, BILLING_CURATOR, 
            NVL(LAST_NAME, BILLING_CURATOR) LAST_NAME,
            DECODE(LAST_NAME, NULL, NULL, FIRST_NAME) FIRST_NAME,
            DECODE(LAST_NAME, NULL, NULL, MIDDLE_NAME) MIDDLE_NAME,
            DIRECTORATE
       FROM (
        SELECT DISTINCT
               1 CONTRACTOR_ID,
               BILLING_CURATOR, 
               SUBSTR(BILLING_CURATOR, 1, INSTR(BILLING_CURATOR,' ',1)-1) LAST_NAME,
               SUBSTR(BILLING_CURATOR, INSTR(BILLING_CURATOR,' ',1)+1, 2) FIRST_NAME,
               SUBSTR(BILLING_CURATOR, INSTR(BILLING_CURATOR,'.',1)+1, 2) MIDDLE_NAME,
               '���' DIRECTORATE
          FROM PINDB_ALL_CONTRACTS_T P
        WHERE BILLING_CURATOR != 'Intercompany'
          AND BILLING_CURATOR NOT LIKE '%������%'  -- ��� ������
          AND NOT EXISTS (
            SELECT * FROM MANAGER_T M
             WHERE P.BILLING_CURATOR LIKE M.LAST_NAME||'%'
               AND M.LAST_NAME IS NOT NULL
          )
       )
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('MANAGER_T.BILLING_CURATOR: '||v_count||' rows inserted (������ ����������, �������-���������)', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- -------------------------------------------------------------------------- --
    -- ����������� �������� ��� SALE_CURATOR_ID
    -- -------------------------------------------------------------------------- --
    MERGE INTO PINDB_ALL_CONTRACTS_T P
    USING (
      SELECT MANAGER_ID, SALES_NAME
        FROM (
            SELECT MANAGER_ID, SALES_NAME, 
                   MAX(MANAGER_ID) OVER (PARTITION BY SALES_NAME) MAX_MANAGER_ID 
             FROM (
               SELECT M.MANAGER_ID, M.LAST_NAME, P.SALES_NAME, 
                      MAX(LENGTH(M.LAST_NAME)) MAX_LEN_NAME 
                 FROM MANAGER_T M, PINDB_ALL_CONTRACTS_T P
                WHERE P.SALES_NAME LIKE M.LAST_NAME||'%'
                  AND M.LAST_NAME IS NOT NULL
                GROUP BY M.MANAGER_ID, M.LAST_NAME, P.SALES_NAME
             ) PM
             WHERE LENGTH(PM.LAST_NAME) = MAX_LEN_NAME
        )
        WHERE MANAGER_ID = MAX_MANAGER_ID
    ) PM
    ON (
       P.SALES_NAME = PM.SALES_NAME
    )
    WHEN MATCHED THEN UPDATE SET P.SALE_CURATOR_ID = PM.MANAGER_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_ALL_CONTRACTS_T.SALE_CURATOR: '||v_count||' rows merged (�������� ���������)', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- -------------------------------------------------------------------------- --
    -- ����������� �������� ��� BILLING_CURATOR_ID
    -- -------------------------------------------------------------------------- --
    MERGE INTO PINDB_ALL_CONTRACTS_T P
    USING (
      SELECT MANAGER_ID, BILLING_CURATOR
        FROM (
            SELECT MANAGER_ID, BILLING_CURATOR, 
                   MAX(MANAGER_ID) OVER (PARTITION BY BILLING_CURATOR) MAX_MANAGER_ID 
             FROM (
               SELECT M.MANAGER_ID, M.LAST_NAME, P.BILLING_CURATOR, 
                      MAX(LENGTH(M.LAST_NAME)) MAX_LEN_NAME 
                 FROM MANAGER_T M, PINDB_ALL_CONTRACTS_T P
                WHERE P.BILLING_CURATOR LIKE M.LAST_NAME||'%'
                  AND M.LAST_NAME IS NOT NULL
                GROUP BY M.MANAGER_ID, M.LAST_NAME, P.BILLING_CURATOR
             ) PM
             WHERE LENGTH(PM.LAST_NAME) = MAX_LEN_NAME
        )
        WHERE MANAGER_ID = MAX_MANAGER_ID
    ) PM
    ON (
       P.BILLING_CURATOR = PM.BILLING_CURATOR
    )
    WHEN MATCHED THEN UPDATE SET P.BILLING_CURATOR_ID = PM.MANAGER_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_ALL_CONTRACTS_T.BILLING_CURATOR: '||v_count||' rows merged (�������� ����������� ���)', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Gather_Table_Stat(l_Tab_Name => 'MANAGER_T');

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- �������� ���������� � ��������
--============================================================================================
PROCEDURE Load_clients
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Load_clients';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������ ������� �� CLIENT_ID, � BRM ��������� ������ ID
    --
    INSERT INTO CLIENT_T CL(CLIENT_ID, CLIENT_NAME, EXTERNAL_ID)
    SELECT DISTINCT CLIENT_ID, CLIENT, CLIENT_ID EXTERNAL_ID 
      FROM PINDB_ALL_CONTRACTS_T PC
     WHERE NOT EXISTS (
        SELECT * FROM CLIENT_T CL
         WHERE PC.CLIENT_ID = CL.CLIENT_ID -- ����� �� �� ������������������, � ������������
           AND PC.CLIENT    = CL.CLIENT_NAME
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CLIENT_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'CLIENT_T');
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- �������� ���������� � ��������� - �����������
--============================================================================================
PROCEDURE Load_customers
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Load_customers';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ����������� CUSTOMER_ID, ��� �����������, ������� ��� ���� � ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    MERGE INTO PINDB_ALL_CONTRACTS_T PCL
    USING (
      SELECT CUSTOMER_ID, CUSTOMER, INN, KPP, ERP_CODE
        FROM (
            SELECT CUSTOMER, INN, KPP, ERP_CODE, 
                   MAX(CUSTOMER_ID) OVER (PARTITION BY CUSTOMER, INN, KPP, ERP_CODE) MAX_CUSTOMER_ID,
                   CUSTOMER_ID 
              FROM CUSTOMER_T CS
             WHERE CUSTOMER IS NOT NULL
        )
       WHERE CUSTOMER_ID = MAX_CUSTOMER_ID
    ) CS
    ON (
         PCL.COMPANY                = CS.CUSTOMER             AND 
         NVL(PCL.INN,'NULL')        = NVL(CS.INN,'NULL')      AND
         NVL(PCL.KPP,'NULL')        = NVL(CS.KPP,'NULL')      AND
         NVL(PCL.KONTRAGENT,'NULL') = NVL(CS.ERP_CODE,'NULL')
    )
    WHEN MATCHED THEN UPDATE SET PCL.CUSTOMER_ID = CS.CUSTOMER_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CUSTOMER_T: '||v_count||' rows exists', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������� ������ ��� ����� �����������,����������� CUSTOMER_ID 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PINDB_ALL_CONTRACTS_TMP DROP STORAGE';
    --
    INSERT INTO PINDB_ALL_CONTRACTS_TMP(COMPANY, INN, KPP, ERP_CODE) 
    SELECT DISTINCT
           NVL(COMPANY,    'NULL') COMPANY, 
           NVL(INN,        'NULL') INN, 
           NVL(KPP,        'NULL') KPP, 
           NVL(KONTRAGENT, 'NULL') ERP_CODE
      FROM PINDB_ALL_CONTRACTS_T PCL
     WHERE CUSTOMER_ID IS NULL   -- ������ ��� �����
    GROUP BY NVL(COMPANY,  'NULL'), 
           NVL(INN,        'NULL'), 
           NVL(KPP,        'NULL'), 
           NVL(KONTRAGENT, 'NULL');
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CUSTOMER_T: '||v_count||' rows not found', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    UPDATE PINDB_ALL_CONTRACTS_TMP SET CUSTOMER_ID = SQ_CLIENT_ID.NEXTVAL;
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� ����� ����������� � �������, 
    INSERT INTO CUSTOMER_T (
      CUSTOMER_ID, ERP_CODE, INN, KPP, CUSTOMER, SHORT_NAME, NOTES
    )
    SELECT CUSTOMER_ID, ERP_CODE, INN, SUBSTR(KPP,1,10) KPP, 
           COMPANY, COMPANY SHORT_NAME, 
           '������������� �� "�������" �������� ' || TO_CHAR(SYSDATE,'dd.mm.yyyy')
      FROM PINDB_ALL_CONTRACTS_TMP;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CUSTOMER_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'CUSTOMER_T');
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ����������� CUSTOMER_ID � ������� �������� �������
    MERGE INTO PINDB_ALL_CONTRACTS_T PCL
    USING (
        SELECT CUSTOMER_ID, COMPANY, INN, KPP, ERP_CODE FROM PINDB_ALL_CONTRACTS_TMP
    ) PCG
    ON (
      PCL.COMPANY                = PCG.COMPANY             AND 
      NVL(PCL.INN,'NULL')        = NVL(PCG.INN,'NULL')     AND 
      NVL(PCL.KPP,'NULL')        = NVL(PCG.KPP,'NULL')     AND 
      NVL(PCL.KONTRAGENT,'NULL') = NVL(PCG.ERP_CODE,'NULL')
    )
    WHEN MATCHED THEN UPDATE SET PCL.CUSTOMER_ID = PCG.CUSTOMER_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_ALL_CONTRACTS_T.CUSTOMER_ID: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- �������� ���������� � ���������
--============================================================================================
PROCEDURE Load_contracts
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Load_contracts';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������, ��� �������� � ���������� �������� ��� ����� ���� � �������
    --
    UPDATE PINDB_ALL_CONTRACTS_T PCL SET DBL_CONTRACT_ID = NULL;
    --
    MERGE INTO PINDB_ALL_CONTRACTS_T PCL
    USING (
        SELECT DISTINCT CONTRACT_ID, CONTRACT_NO 
          FROM CONTRACT_T C
         WHERE EXISTS (
            SELECT * FROM PINDB_ALL_CONTRACTS_T PCL
             WHERE PCL.ACCOUNT_NO = C.CONTRACT_NO
         )
    ) CT
    ON (
      PCL.ACCOUNT_NO = CT.CONTRACT_NO
    )
    WHEN MATCHED THEN UPDATE SET DBL_CONTRACT_ID = CT.CONTRACT_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_ALL_CONTRACTS_T.DBL_CONTRACT_ID: '||v_count||' rows duplicated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ����������� ��������, � ����������� ��������
    --
    INSERT INTO CONTRACT_T (
        CONTRACT_ID, EXTERNAL_ID, 
        CONTRACT_NO, DATE_FROM, DATE_TO, 
        CLIENT_ID, MARKET_SEGMENT_ID, CLIENT_TYPE_ID, NOTES
    )
    SELECT CONTRACT_ID, EXTERNAL_ID, CONTRACT_NO, DATE_FROM, DATE_TO, 
           CLIENT_ID,MARKET_SEGMENT_ID, CLIENT_TYPE_ID, NOTES
      FROM (
      SELECT ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NO ORDER BY ACCOUNT_ID) RN,
             ACCOUNT_ID CONTRACT_ID, ACCOUNT_ID EXTERNAL_ID, 
             ACCOUNT_NO CONTRACT_NO, CUSTDATE DATE_FROM, NULL DATE_TO, 
             CLIENT_ID, MRK.KEY_ID MARKET_SEGMENT_ID, CST.KEY_ID CLIENT_TYPE_ID,    
             '������������� �� "�������" �������� '|| TO_CHAR(SYSDATE,'dd.mm.yyyy') NOTES
        FROM PINDB_ALL_CONTRACTS_T PCL, DICTIONARY_T MRK, DICTIONARY_T CST
       WHERE PCL.MARKET_SEG = MRK.NAME(+)
         AND MRK.PARENT_ID(+) = 64
         AND PCL.CUST_TYPE  = CST.NAME(+)
         AND CST.PARENT_ID(+) = 63
         AND DBL_CONTRACT_ID IS NULL
      ORDER BY CONTRACT_NO
     )
     WHERE RN = 1  -- � ������ �������� ���� �������� ��������� ����� �� ���� ����� ��������
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CONTRACT_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'CONTRACT_T');

    --
    -- ����������� ��� ��������
    -- 
    MERGE INTO CONTRACT_T C
    USING (
      SELECT CONTRACT_NO, MIN(CONTRACT_TYPE_ID) CONTRACT_TYPE_ID, 
             DECODE(MIN(CONTRACT_TYPE_ID), 4, 1, NULL) GOVERMENT_TYPE
        FROM PINDB_CONTRACT_TYPE_T
       GROUP BY CONTRACT_NO
    ) CT
    ON (
       C.CONTRACT_NO = CT.CONTRACT_NO
    )
    WHEN MATCHED THEN UPDATE SET C.CONTRACT_TYPE_ID = CT.CONTRACT_TYPE_ID,
                                 C.GOVERMENT_TYPE   = CT.GOVERMENT_TYPE;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CONTRACT_T: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    --
    -- ����������� CONTRACTOR_ID � �������� �������
    --
    MERGE INTO PINDB_ALL_CONTRACTS_T PC
    USING (
        SELECT CONTRACT_ID, CONTRACT_NO FROM CONTRACT_T C
    ) C
    ON (
        C.CONTRACT_NO = PC.ACCOUNT_NO
    )
    WHEN MATCHED THEN UPDATE SET PC.CONTRACT_ID = C.CONTRACT_ID
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_ALL_CONTRACTS_T.CONTRACT_ID: '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- ����������� �������-�������� �� �������
    --
    MERGE INTO BILLING_CURATOR_T BC
    USING (
        SELECT DISTINCT PC.CONTRACT_ID, PC.BILLING_CURATOR_ID 
          FROM PINDB_ALL_CONTRACTS_T PC
         WHERE PC.DBL_CONTRACT_ID IS NULL
    ) MG
    ON (
        BC.CONTRACT_ID = MG.CONTRACT_ID
    )
    WHEN MATCHED THEN UPDATE SET BC.MANAGER_ID = BILLING_CURATOR_ID
    WHEN NOT MATCHED THEN INSERT (BC.CONTRACT_ID, BC.MANAGER_ID) 
                          VALUES (MG.CONTRACT_ID, BILLING_CURATOR_ID)
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_CURATOR_T: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILLING_CURATOR_T');
    --
    -- ����������� ��������-�������� �� �������
    --
    MERGE INTO SALE_CURATOR_T SC
    USING (
      SELECT DISTINCT PC.CONTRACT_ID, PC.SALE_CURATOR_ID, PC.CUSTDATE 
        FROM PINDB_ALL_CONTRACTS_T PC
       WHERE PC.DBL_CONTRACT_ID IS NULL
    ) MG
    ON (
        SC.CONTRACT_ID = MG.CONTRACT_ID
    )
    WHEN MATCHED THEN UPDATE SET SC.MANAGER_ID = MG.SALE_CURATOR_ID, SC.DATE_FROM = MG.CUSTDATE 
    WHEN NOT MATCHED THEN INSERT (SC.CONTRACT_ID, SC.MANAGER_ID, SC.DATE_FROM) 
                          VALUES (MG.CONTRACT_ID, MG.SALE_CURATOR_ID, MG.CUSTDATE)
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SALE_CURATOR_T: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'SALE_CURATOR_T');
  
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- �������� ���������� � ������� ������ ��������
--============================================================================================
PROCEDURE Load_accounts
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Load_accounts';
    v_count          INTEGER := 0;
    v_account_id     INTEGER;
    v_address_id     INTEGER;
    v_profile_id     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR ra IN (
      SELECT P.CUSTNO ACCOUNT_NO, P.CUSTDATE DATE_FROM,
             P.ACCOUNT_ID, -- ��� ACCOUNT_T.EXTERNAL_ID 
             P.CONTRACT_ID,
             p.CONTRACTOR_ID,
             p.CONTRACTOR_BANK_ID,
             P.CLIENT_ID, 
             P.BRANCH_ID, 
             P.CUSTOMER_ID,
             DECODE(P.CURRENCY_SECONDARY, 286, 286, P.CURRENCY) CURRENCY,
--             P.CURRENCY, 
--             P.CURRENCY_SECONDARY, 
             P.TAX_VAT, 
             P.DELIVERY, D.KEY_ID DELIVERY_ID,
             SUBSTR(P.JUR_ZIP,1,20)JUR_ZIP,  P.JUR_CITY,  P.JUR_ADDRESS,
             SUBSTR(P.PHIS_ZIP,1,20) PHIS_ZIP, P.PHIS_CITY, P.PHIS_ADDRESS, 
             P.PHONE, P.FAX, P.EMAIL_ADDR, P.PHIS_NAME
        FROM PINDB_ALL_CONTRACTS_T P, DICTIONARY_T D
       WHERE 1=1 --P.DBL_CONTRACT_ID IS NULL
         AND P.CUSTOMER_ID IS NOT NULL
--         AND P.CURRENCY = Pk00_Const.c_CURRENCY_RUB             -- 810
--         AND P.CURRENCY_SECONDARY = 0
         AND P.DELIVERY = D.NOTES(+)
         AND D.PARENT_ID(+) = Pk00_Const.k_DICT_DELIVERY_METHOD -- 65  
    )
    LOOP
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- ������� �/�
      v_account_id := Pk05_Account.New_account(
                   p_account_no    => ra.account_no,
                   p_account_type  => Pk00_Const.c_ACC_TYPE_J,
                   p_currency_id   => ra.currency,
                   p_status        => 'NEW', --Pk00_Const.c_ACC_STATUS_BILL,
                   p_parent_id     => NULL,
                   p_notes         => '������������� �� "�������" �������� '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
               );
      -- ����������� �������������� ������� ��������
      Pk05_Account.Set_billing(
                   p_account_id => v_account_id,
                   p_billing_id => c_BILLING_DISC  -- -> Pk00_Const.c_BILLING_OLD
               );
      
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- ������� ����������� �����
      v_address_id := PK05_ACCOUNT.Add_address(
                  p_account_id    => v_account_id,
                  p_address_type  => PK00_CONST.c_ADDR_TYPE_JUR,
                  p_country       => '��',
                  p_zip           => ra.jur_zip,
                  p_state         => NULL,
                  p_city          => ra.jur_city,
                  p_address       => ra.jur_address,
                  p_person        => NULL,
                  p_phones        => NULL,
                  p_fax           => NULL,
                  p_email         => NULL,
                  p_date_from     => ra.date_from,
                  p_date_to       => NULL,
                  p_notes         => '������������� �� "�������" �������� '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
             );

      -- - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- ������� ����� ��������
      v_address_id := PK05_ACCOUNT.Add_address(
                  p_account_id    => v_account_id,
                  p_address_type  => PK00_CONST.c_ADDR_TYPE_DLV,
                  p_country       => '��',
                  p_zip           => ra.phis_zip,
                  p_state         => NULL,
                  p_city          => ra.phis_city,
                  p_address       => ra.phis_address,
                  p_person        => ra.phis_name,
                  p_phones        => ra.phone,
                  p_fax           => ra.fax,
                  p_email         => ra.email_addr,
                  p_date_from     => ra.date_from,
                  p_date_to       => NULL,
                  p_notes         => '������������� �� "�������" �������� '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
             );

      -- - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- ������� ������� �������� �����
      v_profile_id := Pk05_Account.Set_profile(
                 p_account_id         => v_account_id,
                 p_brand_id           => NULL,
                 p_contract_id        => ra.Contract_Id,
                 p_customer_id        => ra.Customer_Id,
                 p_subscriber_id      => NULL,
                 p_contractor_id      => ra.Contractor_id,
                 p_branch_id          => ra.Branch_Id,
                 p_agent_id           => NULL,
                 p_contractor_bank_id => ra.Contractor_Bank_Id,
                 p_vat                => ra.Tax_Vat,
                 p_date_from          => ra.Date_From,
                 p_date_to            => NULL
             );

      -- ����������� ����������� � ����� 
      MERGE INTO ACCOUNT_PROFILE_T AP
      USING (
          SELECT A.ACCOUNT_ID, CB.CONTRACTOR_ID, CB.BANK_ID, 
                 CT.CONTRACTOR, PB.CONTRACTOR PB_CONTRACTOR --PB.* 
            FROM PINDB_CONTRACTOR_BANK_T PB, ACCOUNT_T A, 
                 CONTRACTOR_BANK_T CB, CONTRACTOR_T CT 
           WHERE PB.ACCOUNT_NO = A.ACCOUNT_NO
             AND CB.BANK_SETTLEMENT = PB.SETTLEMENT
             AND CB.CONTRACTOR_ID = CT.CONTRACTOR_ID
             AND EXISTS ( -- �� ������ ������
               SELECT * FROM PINDB_ALL_CONTRACTS_T PC
                WHERE PC.CUSTNO = A.ACCOUNT_NO
             )
      ) CB
      ON (
          AP.ACCOUNT_ID = CB.ACCOUNT_ID
      )
      WHEN MATCHED THEN UPDATE SET AP.CONTRACTOR_ID = CB.CONTRACTOR_ID,
                                   AP.CONTRACTOR_BANK_ID = CB.BANK_ID
      ;
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- ������� ��������� ������ ��� ������ �/�
      Pk07_Bill.New_billinfo (
                 p_account_id    => v_account_id,   -- ID �������� �����
                 p_currency_id   => ra.currency,     -- ID ������ �����
                 p_delivery_id   => ra.delivery_id, -- ID ������� �������� �����
                 p_days_for_payment => 30           -- ���-�� ���� �� ������ �����
             );

      -- - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- ������� ������ �������� �����
      --
      INSERT INTO ACCOUNT_DOCUMENTS_T(ACCOUNT_ID, DOC_BILL, DELIVERY_METHOD_ID)
      VALUES(v_account_id, 'Y', ra.delivery_id);

      v_count := v_count + 1;
      
    END LOOP;
    
    Pk01_Syslog.Write_msg('ACCOUNT_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ����������� ������ �� ACCOUNT_ID ����� ������ � BRM ����������
    MERGE INTO ACCOUNT_T A
    USING (
        SELECT ACCOUNT_ID, CUSTNO FROM PINDB_ALL_CONTRACTS_T 
    ) PA
    ON (
      A.ACCOUNT_NO = PA.CUSTNO AND
      A.BILLING_ID = Pk00_Const.c_BILLING_OLD -- 2002
    )
    WHEN MATCHED THEN UPDATE SET A.EXTERNAL_ID = PA.ACCOUNT_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ACCOUNT_T');
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;


--============================================================================================
-- �������� ���������� � �������
--============================================================================================
PROCEDURE Load_orders
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Load_orders';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������ ������� �������, ������� ��� ���� � BRM
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    UPDATE PINDB_NPL_ORDERS_T PO SET PO.LOAD_STATUS = c_LDSTAT_DBL_ORD -- ����� ��� ���� � BRM
    WHERE EXISTS (
        SELECT * FROM ORDER_T O
         WHERE O.ORDER_NO = PO.ORDER_NO
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_NPL_ORDERS_T: '||v_count||' order_no duplicated in BRM', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������ �������, ��� ������� �� ����� ������ � �����������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    UPDATE PINDB_NPL_ORDERS_T PO SET PO.LOAD_STATUS = c_LDSTAT_NOT_SRV -- ������ �� �������
    WHERE NOT EXISTS (
        SELECT * FROM SERVICE_T S
         WHERE PO.SERVICE_NAME = S.SERVICE
    )
    AND PO.LOAD_STATUS IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_NPL_ORDERS_T: '||v_count||' service not found in SERVICE_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    COMMIT;
        
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    INSERT INTO ORDER_T(
        ORDER_ID, EXTERNAL_ID, ORDER_NO, ACCOUNT_ID, SERVICE_ID, RATEPLAN_ID, 
        DATE_FROM, DATE_TO, CREATE_DATE, MODIFY_DATE, STATUS, NOTES
    )
    WITH PO AS (
        SELECT 
            CUSTNO, ORDER_NO, STATUS,
            RATE_PLAN, IP_USAGE_RATE_PLAN, 
            TO_NUMBER(REPLACE(CYCLE_FEE_AMT,',','.')) CYCLE_FEE_AMT,
            DECODE(CURRENCY_SECONDARY,286,286,CURRENCY) CURRENCY_ID,
            CYCLE_START_T, CYCLE_END_T-1/86400 CYCLE_END_T, -- ������ � ��������� ���������� ��������� 
            --SMC_START_T, SMC_END_T-1/86400,     -- ������ ������ (�����)
            ORDER_DATE,                         -- ���� ���������� ������
            S_RGN, D_RGN, 
            SERVICE_NAME, SPEED_STR, FREE_DOWNTIME
         FROM PINDB_NPL_ORDERS_T
        WHERE 1=1 --IP_USAGE_RATE_PLAN IS NULL
          AND LOAD_STATUS IS NULL    -- ������ ������ ������� �� ������������
    )
    SELECT SQ_ORDER_ID.NEXTVAL ORDER_ID, 
           NULL EXTERNAL_ID, PO.ORDER_NO,
           A.ACCOUNT_ID, S.SERVICE_ID SERVICE_ID,
           /*
           CASE
             WHEN PO.RATE_PLAN = 'NPL_RUR'        THEN 80043 
             WHEN PO.RATE_PLAN = 'NPL'            THEN 80045 
             WHEN PO.RATE_PLAN = 'RRW RUR'        THEN 80046 
             WHEN PO.RATE_PLAN = 'IP Routing RUR' THEN 80047 
           END 
           */
           NULL RATEPLAN_ID, 
           PO.ORDER_DATE DATE_FROM, NVL(PO.CYCLE_END_T, c_MAX_DATE_TO) DATE_TO, 
           PO.ORDER_DATE CREATE_DATE, SYSDATE MODIFY_DATE, 
           DECODE(PO.CYCLE_END_T, NULL, 'OPEN', 'CLOSE') STATUS, 
           '������������� �� "�������" �������� '|| TO_CHAR(SYSDATE,'dd.mm.yyyy') NOTES
      FROM PO, SERVICE_T S, ACCOUNT_T A
     WHERE NOT EXISTS (
             SELECT * FROM ORDER_T O
              WHERE O.ORDER_NO = PO.ORDER_NO
           )
       --AND PO.CURRENCY_ID != 36 
       --AND IP_USAGE_RATE_PLAN IS NULL
       AND PO.SERVICE_NAME = S.SERVICE(+)
       AND PO.CUSTNO = A.ACCOUNT_NO    
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ORDER_T');
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ���������� ����� ��� ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    INSERT INTO ORDER_BODY_T (
        ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, DATE_FROM, DATE_TO,
        RATEPLAN_ID, RATE_VALUE, RATE_RULE_ID, RATE_LEVEL_ID, TAX_INCL, QUANTITY,
        CREATE_DATE, MODIFY_DATE, CURRENCY_ID, NOTES
    )
    SELECT SQ_ORDER_ID.NEXTVAL ORDER_BODY_ID, O.ORDER_ID,
           Pk00_Const.c_SUBSRV_REC SUBSERVICE_ID,   -- c_SUBSRV_REC CONSTANT integer := 41;  -- ����������� ������
           Pk00_Const.c_CHARGE_TYPE_REC CHARGE_TYPE,-- c_CHARGE_TYPE_REC :=  'REC'
           PO.CYCLE_START_T DATE_FROM, 
           NVL(PO.CYCLE_END_T, c_MAX_DATE_TO) DATE_TO,
           O.RATEPLAN_ID, 
           TO_NUMBER(REPLACE(CYCLE_FEE_AMT,',','.')) RATE_VALUE,
           Pk00_Const.c_RATE_RULE_ABP_STD RATE_RULE_ID, -- c_RATE_RULE_ABP_STD    CONSTANT INTEGER := 2402; -- ����������� ���������� ���������  
           Pk00_Const.c_RATE_LEVEL_ORDER RATE_LAVEL_ID, -- c_RATE_LEVEL_ORDER     CONSTANT INTEGER := 2302; -- ����� ������ �� �����
           'N' TAX_INCL,
           1 QUANTITY,
           SYSDATE CREATE_DATE,
           SYSDATE MODIFY_DATE,
           DECODE(PO.CURRENCY_SECONDARY,286,286,PO.CURRENCY) CURRENCY_ID,
           '������������� �� "�������" �������� '|| TO_CHAR(SYSDATE,'dd.mm.yyyy') NOTES
      FROM PINDB_NPL_ORDERS_T PO, ORDER_T O
     WHERE O.ORDER_NO = PO.ORDER_NO 
       AND PO.CYCLE_FEE_AMT <> 0
    ; 
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T: '||v_count||' REC rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� �������������� ���������� �� ������� �������������� ������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    INSERT INTO IP_CHANNEL_INFO_T (
        ORDER_BODY_ID, POINT_SRC, POINT_DST, SPEED_STR, ROUTER_ZONE, SPEED_VALUE, SPEED_UNIT_ID
    )
    SELECT
           OB.ORDER_BODY_ID,
           PO.S_RGN POINT_SRC,
           PO.D_RGN POINT_DST,
           PO.SPEED_STR,
           NULL ROUTER_ZONE,
           TO_NUMBER( NVL(TRIM(SUBSTR(LTRIM( REPLACE(PO.SPEED_STR,',','.') ), 1, INSTR(LTRIM(PO.SPEED_STR),' '))), 0) ) SPEED_VALUE,
           D.KEY_ID SPEED_UNIT_ID
      FROM PINDB_NPL_ORDERS_T PO, ORDER_T O, ORDER_BODY_T OB, DICTIONARY_T D 
     WHERE O.ORDER_NO = PO.ORDER_NO
       AND O.ORDER_ID = OB.ORDER_ID
       AND OB.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_REC   -- c_CHARGE_TYPE_REC := 'REC'
       AND D.PARENT_ID(+) = 67
       AND D.NAME = TRIM(SUBSTR(LTRIM(PO.SPEED_STR), INSTR(LTRIM(PO.SPEED_STR),' ')))
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('IP_CHANNEL_INFO_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ���������� ����� ��� ����������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    INSERT INTO ORDER_BODY_T (
        ORDER_BODY_ID,
        ORDER_ID,
        SUBSERVICE_ID,
        CHARGE_TYPE,
        DATE_FROM,
        DATE_TO,
        FREE_VALUE,
        RATE_RULE_ID, 
        RATE_LEVEL_ID,
        TAX_INCL,
        QUANTITY,
        CREATE_DATE,
        MODIFY_DATE,
        CURRENCY_ID,
        NOTES
    )
    SELECT SQ_ORDER_ID.NEXTVAL ORDER_BODY_ID, O.ORDER_ID,
           Pk00_Const.c_SUBSRV_IDL SUBSERVICE_ID,    -- c_SUBSRV_IDL    CONSTANT integer := 36;  -- ����������� ��������
           Pk00_Const.c_CHARGE_TYPE_IDL CHARGE_TYPE, -- c_CHARGE_TYPE_IDL := 'IDL'
           PO.CYCLE_START_T DATE_FROM, 
           NVL(PO.CYCLE_END_T, c_MAX_DATE_TO) DATE_TO,
           PO.FREE_DOWNTIME FREE_VALUE,
           Pk00_Const.c_RATE_RULE_IDL_STD RATE_RULE_ID,  -- c_RATE_RULE_IDL_STD    CONSTANT INTEGER := 2404; -- ����������� ��������, ����������� �����  
           Pk00_Const.c_RATE_LEVEL_ORDER RATE_LAVEL_ID, -- c_RATE_LEVEL_ORDER     CONSTANT INTEGER := 2302; -- ����� ������ �� �����
           'N' TAX_INCL,
           1 QUANTITY,
           SYSDATE CREATE_DATE,
           SYSDATE MODIFY_DATE,
           DECODE(PO.CURRENCY_SECONDARY,286,286,PO.CURRENCY) CURRENCY_ID,
           '������������� �� "�������" �������� '|| TO_CHAR(SYSDATE,'dd.mm.yyyy') NOTES
      FROM PINDB_NPL_ORDERS_T PO, ORDER_T O
     WHERE O.ORDER_NO = PO.ORDER_NO
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T: '||v_count||' IDL rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    Gather_Table_Stat(l_Tab_Name => 'ORDER_BODY_T');
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� �������������� ������������ ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    INSERT INTO SERVICE_ALIAS_T(SERVICE_ID, ACCOUNT_ID, SRV_NAME)
    SELECT DISTINCT O.SERVICE_ID, O.ACCOUNT_ID, SAA.NAME SERVICE_NAME
         FROM SUBSERVICES_NAME_ALIAS_T@PINDB SAA, 
              SERVICE_T@PINDB S, 
              (SELECT O.SERVICE_ID, O.ACCOUNT_ID, O.ORDER_NO
                 FROM ORDER_T O
                WHERE EXISTS
                     (SELECT 1
                        FROM PINDB_NPL_ORDERS_T PO
                       WHERE PO.LOAD_STATUS IS NULL AND PO.ORDER_NO = O.ORDER_NO)
                  AND ROWNUM > 0     
              ) o          
        WHERE  S.POID_ID0 = SAA.SERVICE_OBJ_ID0
            AND O.ORDER_NO = S.LOGIN
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SERVICE_ALIAS_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� ��������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    UPDATE PINDB_NPL_ORDERS_T PO SET PO.LOAD_STATUS = 1
     WHERE PO.LOAD_STATUS IS NULL
       AND EXISTS (
           SELECT * FROM ORDER_T O
            WHERE O.ORDER_NO = PO.ORDER_NO
       )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_NPL_ORDERS_T: '||v_count||' rows loaded', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- ����� ���� ���������� � ������
--============================================================================================
PROCEDURE Rollback_bills
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Rollback_bills';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� BDR
    DELETE FROM BDR_DOWNTIME_T BDR
    WHERE EXISTS (
        SELECT * FROM ACCOUNT_T A, BILL_T B
         WHERE A.BILLING_ID = c_BILLING_DISC
           AND A.ACCOUNT_ID = B.ACCOUNT_ID
           AND BDR.BILL_ID    = B.BILL_ID
    )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_DOWNTIME_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� ITEMs
    DELETE FROM ITEM_T I
    WHERE EXISTS (
        SELECT * FROM ACCOUNT_T A, BILL_T B
         WHERE A.BILLING_ID = c_BILLING_DISC
           AND A.ACCOUNT_ID = B.ACCOUNT_ID
           AND I.BILL_ID    = B.BILL_ID
    )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� INVOICE_ITEMs
    DELETE FROM INVOICE_ITEM_T V
    WHERE EXISTS (
        SELECT * FROM ACCOUNT_T A, BILL_T B
         WHERE A.BILLING_ID = c_BILLING_DISC
           AND A.ACCOUNT_ID = B.ACCOUNT_ID
           AND V.BILL_ID    = B.BILL_ID
    )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� �����
    DELETE FROM BILL_T B
    WHERE EXISTS (
        SELECT * FROM ACCOUNT_T A
         WHERE A.BILLING_ID = c_BILLING_DISC
           AND A.ACCOUNT_ID = B.ACCOUNT_ID
    )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;


--============================================================================================
-- ����� ���������� � �������
--============================================================================================
PROCEDURE Rollback_orders
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Rollback_orders';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������� �������������� ������������ ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM SERVICE_ALIAS_T SA
     WHERE EXISTS (
           SELECT * 
             FROM ACCOUNT_T A, ORDER_T O
            WHERE A.BILLING_ID = c_BILLING_DISC
              AND A.ACCOUNT_ID = SA.ACCOUNT_ID
              AND A.ACCOUNT_ID = O.ACCOUNT_ID
              AND O.SERVICE_ID = SA.SERVICE_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SERVICE_ALIAS_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� �������������� ���������� �� ������� �������������� ������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM IP_CHANNEL_INFO_T I
     WHERE EXISTS (
           SELECT * 
             FROM ACCOUNT_T A, ORDER_T O, ORDER_BODY_T OB
            WHERE A.BILLING_ID    = c_BILLING_DISC
              AND A.ACCOUNT_ID    = O.ACCOUNT_ID
              AND O.ORDER_ID      = OB.ORDER_ID
              AND I.ORDER_BODY_ID = OB.ORDER_BODY_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('IP_CHANNEL_INFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ���������� � �������� SLA 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    /*
    DELETE FROM SLA_PERCENT_T SP
     WHERE EXISTS (
       SELECT * 
         FROM PINDB_NPL_ORDERS_T PO, ORDER_T O, ORDER_BODY_T OB
        WHERE PO.ORDER_NO      = O.ORDER_NO
          AND PO.LOAD_STATUS   = 1
          AND O.ORDER_ID       = OB.ORDER_ID
          AND SP.ORDER_BODY_ID = OB.ORDER_BODY_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SLA_PERCENT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    */
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ���������� � ����������� �������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM DOWNTIME_T DT
     WHERE EXISTS (
           SELECT * 
             FROM ACCOUNT_T A, ORDER_T O, ORDER_BODY_T OB
            WHERE A.BILLING_ID    = c_BILLING_DISC
              AND A.ACCOUNT_ID    = O.ACCOUNT_ID
              AND O.ORDER_ID      = OB.ORDER_ID
              AND DT.ORDER_BODY_ID= OB.ORDER_BODY_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('DOWNTIME_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ���������� � BDR ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM BDR_DOWNTIME_T BD
     WHERE EXISTS (
           SELECT * 
             FROM ACCOUNT_T A, ORDER_T O, ORDER_BODY_T OB
            WHERE A.BILLING_ID    = c_BILLING_DISC
              AND A.ACCOUNT_ID    = O.ACCOUNT_ID
              AND O.ORDER_ID      = OB.ORDER_ID
              AND BD.ORDER_BODY_ID= OB.ORDER_BODY_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_DOWNTIME_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ���������� ����� ��� ��������� � ����������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM ORDER_BODY_T OB
    WHERE EXISTS (
           SELECT * 
             FROM ACCOUNT_T A, ORDER_T O
            WHERE A.BILLING_ID    = c_BILLING_DISC
              AND A.ACCOUNT_ID    = O.ACCOUNT_ID
              AND O.ORDER_ID      = OB.ORDER_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� �������������� ���������� ������ (�� ������ ������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM ORDER_INFO_T OI
    WHERE EXISTS (
           SELECT * 
             FROM ACCOUNT_T A, ORDER_T O
            WHERE A.BILLING_ID    = c_BILLING_DISC
              AND A.ACCOUNT_ID    = O.ACCOUNT_ID
              AND O.ORDER_ID      = OI.ORDER_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_INFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ���������� ������ (�� ������ ������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM ORDER_LOCK_T OL
    WHERE EXISTS (
           SELECT * 
             FROM ACCOUNT_T A, ORDER_T O
            WHERE A.BILLING_ID    = c_BILLING_DISC
              AND A.ACCOUNT_ID    = O.ACCOUNT_ID
              AND O.ORDER_ID      = OL.ORDER_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_LOCK_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ------------------------------------------------------------ --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM ORDER_T O
    WHERE EXISTS (
           SELECT * 
             FROM ACCOUNT_T A
            WHERE A.BILLING_ID    = c_BILLING_DISC
              AND A.ACCOUNT_ID    = O.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- ����� ���������� � ��������
--============================================================================================
PROCEDURE Rollback_contracts
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Rollback_contracts';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �������� �������� � ��������
    DELETE 
      FROM SALE_CURATOR_T SC
     WHERE CONTRACT_ID IN (
         SELECT AP.CONTRACT_ID 
           FROM ACCOUNT_PROFILE_T AP
          WHERE EXISTS (
              SELECT * FROM ACCOUNT_T A
               WHERE A.ACCOUNT_ID = AP.ACCOUNT_ID
                 AND A.BILLING_ID = c_BILLING_DISC
            )
            AND NOT EXISTS (
              SELECT * FROM ACCOUNT_T A
               WHERE A.ACCOUNT_ID = AP.ACCOUNT_ID
                 AND A.BILLING_ID != c_BILLING_DISC
            )
       )
       AND ACCOUNT_ID IS NULL
       AND ORDER_ID IS NULL
       AND CONTRACTOR_ID IS NULL
     ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SALE_CURATOR_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� �������� �������� � ������� ��������
    DELETE 
      FROM BILLING_CURATOR_T
     WHERE CONTRACT_ID IN (
         SELECT AP.CONTRACT_ID 
           FROM ACCOUNT_PROFILE_T AP
          WHERE EXISTS (
              SELECT * FROM ACCOUNT_T A
               WHERE A.ACCOUNT_ID = AP.ACCOUNT_ID
                 AND A.BILLING_ID = c_BILLING_DISC
            )
            AND NOT EXISTS (
              SELECT * FROM ACCOUNT_T A
               WHERE A.ACCOUNT_ID = AP.ACCOUNT_ID
                 AND A.BILLING_ID != c_BILLING_DISC
            )
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_CURATOR_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ���������� ������� �� �������
    UPDATE ACCOUNT_PROFILE_T AP SET AP.CONTRACT_ID = NULL
    WHERE EXISTS (
        SELECT * FROM ACCOUNT_T A
         WHERE A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND A.BILLING_ID = c_BILLING_DISC
      )
      AND NOT EXISTS (
        SELECT * FROM ACCOUNT_T A
         WHERE A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND A.BILLING_ID != c_BILLING_DISC
      );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_PROFILE_T AP: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� �������
    DELETE FROM CONTRACT_T C
     WHERE CONTRACT_ID IN (
         SELECT AP.CONTRACT_ID 
           FROM ACCOUNT_PROFILE_T AP
          WHERE EXISTS (
              SELECT * FROM ACCOUNT_T A
               WHERE A.ACCOUNT_ID = AP.ACCOUNT_ID
                 AND A.BILLING_ID = c_BILLING_DISC
            )
            AND NOT EXISTS (
              SELECT * FROM ACCOUNT_T A
               WHERE A.ACCOUNT_ID = AP.ACCOUNT_ID
                 AND A.BILLING_ID != c_BILLING_DISC
            )
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CONTRACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- ����� ���������� � ������� ������
--============================================================================================
PROCEDURE Rollback_accounts
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Rollback_accounts';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ��������� ������ ����������
    DELETE FROM ACCOUNT_DOCUMENTS_T AD
     WHERE EXISTS (
         SELECT * 
           FROM ACCOUNT_T A
          WHERE AD.ACCOUNT_ID = A.ACCOUNT_ID
            AND A.BILLING_ID  = c_BILLING_DISC
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_DOCUMENTS_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������ �/�
    DELETE FROM ACCOUNT_CONTACT_T AC
     WHERE EXISTS (
         SELECT * 
           FROM ACCOUNT_T A
          WHERE AC.ACCOUNT_ID = A.ACCOUNT_ID
            AND A.BILLING_ID  = c_BILLING_DISC
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_CONTACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ��������� ������� ���������� �/�
    DELETE FROM ACCOUNT_DOCUMENTS_T AD
     WHERE EXISTS (
         SELECT * 
           FROM ACCOUNT_T A
          WHERE AD.ACCOUNT_ID = A.ACCOUNT_ID
            AND A.BILLING_ID  = c_BILLING_DISC
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_DOCUMENTS_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ��������� ������
    DELETE FROM BILLINFO_T BI
     WHERE EXISTS (
         SELECT * 
           FROM ACCOUNT_T A
          WHERE BI.ACCOUNT_ID = A.ACCOUNT_ID
            AND A.BILLING_ID  = c_BILLING_DISC
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLINFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������� �� �/�
    DELETE FROM REP_PERIOD_INFO_T RP
     WHERE EXISTS (
         SELECT * 
           FROM ACCOUNT_T A
          WHERE RP.ACCOUNT_ID = A.ACCOUNT_ID
            AND A.BILLING_ID  = c_BILLING_DISC
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLINFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������� �������� �����
    DELETE FROM ACCOUNT_PROFILE_T AP
     WHERE EXISTS (
         SELECT * 
           FROM ACCOUNT_T A
          WHERE AP.ACCOUNT_ID = A.ACCOUNT_ID
            AND A.BILLING_ID  = c_BILLING_DISC
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_PROFILE_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������� �����
    DELETE FROM ACCOUNT_T A
     WHERE A.BILLING_ID = c_BILLING_DISC;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_PROFILE_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;





--============================================================================================
-- ������������ ������ �� �������� ������
--============================================================================================
PROCEDURE Make_report
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Make_report';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ID ��������
    MERGE INTO AG_IMPORT_NPL_T PO
    USING (
        SELECT CONTRACT_ID, CONTRACT_NO FROM CONTRACT_T
    ) C
    ON (
        PO.ACCOUNT_NO = C.CONTRACT_NO 
    )
    WHEN MATCHED THEN UPDATE SET PO.CONTRACT_ID = C.CONTRACT_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('AG_IMPORT_NPL_T.CONTRACT: '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ID �/�
    MERGE INTO AG_IMPORT_NPL_T PO
    USING (
        SELECT ACCOUNT_ID, ACCOUNT_NO FROM ACCOUNT_T
    ) A
    ON (
        PO.CUSTNO = A.ACCOUNT_NO 
    )
    WHEN MATCHED THEN UPDATE SET PO.ACCOUNT_ID = A.ACCOUNT_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('AG_IMPORT_NPL_T.ACCOUNT: '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ����� ������
    MERGE INTO AG_IMPORT_NPL_T PO
    USING (
        SELECT ORDER_ID, ORDER_NO FROM ORDER_T
    ) O
    ON (
        PO.ORDER_NO = O.ORDER_NO 
    )
    WHEN MATCHED THEN UPDATE SET PO.ORDER_ID = O.ORDER_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('AG_IMPORT_NPL_T.ORDER: '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ���������� �� �������
    MERGE INTO AG_IMPORT_NPL_T PO
    USING (
        SELECT AP.ACCOUNT_ID, C.CLIENT_ID, AP.CUSTOMER_ID, 
               AP.BRANCH_ID, AP.CONTRACTOR_ID, AP.CONTRACTOR_BANK_ID 
          FROM ACCOUNT_PROFILE_T AP, CONTRACT_T C
         WHERE AP.CONTRACT_ID = C.CONTRACT_ID
    ) AP
    ON (
        PO.ACCOUNT_ID = AP.ACCOUNT_ID 
    )
    WHEN MATCHED THEN UPDATE SET 
        PO.CLIENT_ID   = AP.CLIENT_ID, 
        PO.CUSTOMER_ID = AP.CUSTOMER_ID, 
        PO.BRANCH_ID   = AP.BRANCH_ID, 
        PO.CONTRACTOR_ID = AP.CONTRACTOR_ID, 
        PO.CONTRACTOR_BANK_ID = AP.CONTRACTOR_BANK_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('AG_IMPORT_NPL_T.PROFILE: '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        
    -- ����������� ������� ������� ��������� ������
    UPDATE AG_IMPORT_NPL_T GI SET GI.DSC_TYPE = 1
     WHERE GI.CUSTNO IN  (
        SELECT ACCOUNT_NO FROM SVM_DISC_GROUP_T 
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('AG_IMPORT_NPL_T.DSC_TYPE: '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������ � ����������� ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION View_result( 
               p_recordset    OUT t_refc
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'View_result';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT * 
          FROM AG_IMPORT_NPL_T
         WHERE DSC_TYPE    IS NULL
           AND CONTRACT_ID IS NOT NULL
           AND ACCOUNT_ID  IS NOT NULL
           AND ORDER_ID    IS NOT NULL
        ORDER BY ACCOUNT_NO, CUSTNO
        ;
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


END PK39_BILLING_DISCOUNT_IMPORT;
/
