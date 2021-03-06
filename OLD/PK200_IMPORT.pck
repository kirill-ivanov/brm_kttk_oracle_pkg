CREATE OR REPLACE PACKAGE PK200_IMPORT
IS
    --
    -- ����� ��� ��������� ������� ������ �� ��
    -- event_t
    --
    -- ==============================================================================
    c_PkgName   CONSTANT varchar2(30) := 'PK200_IMPORT';
    -- ==============================================================================
    c_RET_OK    CONSTANT integer := 0;
    c_RET_ER		CONSTANT integer :=-1;
    
    TYPE t_refc IS REF CURSOR;
    
    --=============================================================================--
    -- ID �������� ��� ����������� ���� CONTRACTOR_T (�������� ���� �� ����)
    c_CONTRACTOR_KTTK_ID CONSTANT integer := 1;
    -- ID ��������� ��� ����������� ���� MANAGER_T (���� ���� �� ����)
    c_MANAGER_SIEBEL_ID  CONSTANT integer := 1;
    
    -- �������� ������ ��� ������� �� ��
    PROCEDURE Load_data;
    
    -- ������ ����
    PROCEDURE Import_XTTK;
    
    -- ������ ������� ��� �������
    PROCEDURE Import_AGENTS;

    -- �������������� ������ ���������� �� ��������� ������� DB MMTDB
    -- ����������� ������ PK02_EXPORT_P@MMTDB
    PROCEDURE Load_pk02_export_mmtdb;

    -- ������ �������
    PROCEDURE Import_Persons;

    -- ----------------------------------------------------------------------- --    
    -- ������� �������� �������� �� 01.01.2014
    -- ��������� ������� ����������� PK02_EXPORT_P.Exp_open_balance@MMTDB
    -- p_max_period_id - ��������� (�������)��������������� ������, �� ��� �� ������
    -- � ����� ������ ��� ��������� close_balance ����������� �������
    PROCEDURE Import_open_balance;
    
    -- ����� �������� �������� �� 01.01.2014
    PROCEDURE Rollback_open_balance;

    -- ���� ��������� �������� �������� �� 01.01.2014
    PROCEDURE Test_open_balance;
    
    --
    PROCEDURE Transfer_to_bill;
    
    -- ----------------------------------------------------------------------- --    
    
    -- ��������� ������ ������������ (������������)
    --   - ���-�� �������
    --   - ������������� - id ��������� �� ����� � L01
    FUNCTION Contractor_tree(p_recordset OUT t_refc) RETURN INTEGER;
    
    -- ������� ����� ��� ���� �/� ��� ����������� �������� �� PERIOD_T
    PROCEDURE Make_Bill_For_Periods;
    
    -- ������� ��������� ������� �������� �����
    PROCEDURE New_billinfo (
                   p_account_id    IN INTEGER
               );    
    
    -- ������������� ������ ����� �������� ������
    PROCEDURE Correct_rateplan;
    
    --=========================================================================--
    -- ������� ��������
    -- ��������� ������� ����������� PK02_EXPORT_P.Payments@MMTDB
    PROCEDURE Import_payments;
    
    --=========================================================================--
    -- ������ ����������� �������� �� �/� �� ��������
    PROCEDURE Imp_period_payments;

    -- ������ ����������� ���������� �� �/� �� ��������
    PROCEDURE Imp_period_totals;

    -- ������ ����������� ������������� �� �/� �� ��������
    PROCEDURE Imp_period_adjust;
    
    -- ������ ������������ ������ �� �/� � �� ��������
    PROCEDURE Imp_bills;
    
    -- ������ ������� ������������ ������ �� �/� � �� ��������
    PROCEDURE Imp_items;
    
    --=========================================================================--
    -- ����� ��������� ���.���
    PROCEDURE Rollback_persons;
    
    --=========================================================================--
    -- ������� ����� � ������� � ��� ��� �������� ��������
    PROCEDURE Create_open_bill;
    --
    -- ������� ��������� ������� ��� �������� ��������
    PROCEDURE Create_open_payment;
    
    --
    --select * from v_fiz_export_abon@MMTDB v where v.account_no = 'ACC000335284';
    --
    --select * from v_fiz_export_abon_phones@MMTDB v where v.account_no = 'ACC000335284';
    
    --create table svm_fiz_export_abon as
    --select * from v_fiz_export_abon@MMTDB v

    --create table svm_fiz_export_abon_phones as
    --select * from v_fiz_export_abon_phones@MMTDB v
    
END PK200_IMPORT;
/
CREATE OR REPLACE PACKAGE BODY PK200_IMPORT
IS

--=========================================================================--
-- �������� ������ �� �� ���������
--=========================================================================--
--  
PROCEDURE Load_data 
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Load_data';
    v_count         INTEGER;
BEGIN
    --
    DELETE FROM SVM_BRANCH;
    --
    INSERT INTO SVM_BRANCH 
    SELECT DISTINCT a.brand_name, ni.company, ni.country, ni.zip, ni.state, ni.city, ni.address
    FROM v_fiz_export_abon@MMTDB a, account_t@MMTDB b, account_nameinfo_t@MMTDB ni
      WHERE a.brand_name = b.NAME
      AND ni.obj_id0 = b.poid_id0
      AND b.poid_id0 = b.brand_obj_id0
      AND LENGTH(lineage) < 40
      AND brand_name <> 'JSC PayTest';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SVM_BRANCH insert '||v_count||' rows', c_PkgName||'.'||v_prcName);

    --
    DELETE FROM SVM_AGENTS;
    --
    INSERT INTO SVM_AGENTS
    SELECT DISTINCT a.brand_name, ni.company, ni.country, ni.zip, ni.state, ni.city, ni.address
      FROM v_fiz_export_abon@MMTDB a, account_t@MMTDB b, account_nameinfo_t@MMTDB ni
      WHERE a.brand_name = b.NAME
      AND ni.obj_id0 = b.poid_id0
      AND b.poid_id0 = b.brand_obj_id0
      AND LENGTH(lineage) > 40
      AND brand_name <> 'JSC PayTest';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SVM_AGENTS insert '||v_count||' rows', c_PkgName||'.'||v_prcName);
    --
    DELETE FROM SVM_BRAND_AGENTS;
    --
    INSERT INTO SVM_BRAND_AGENTS
    SELECT a.NAME, NVL(b.NAME,'����') brand
    FROM account_t@MMTDB a, account_t@MMTDB b
    WHERE a.poid_id0 = a.brand_obj_id0
    AND LENGTH(a.lineage) > 40
    AND b.poid_id0(+) = REPLACE(
    SUBSTR(REPLACE(REPLACE(REPLACE(a.lineage,'0.0.0.1:',''),'B',''),'/113259/',''),
    0,INSTR(REPLACE(REPLACE(REPLACE(a.lineage,'0.0.0.1:',''),'B',''),'/113259/',''),
    '/')),'/','');
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SVM_BRAND_AGENTS insert '||v_count||' rows', c_PkgName||'.'||v_prcName);
    --
    DELETE FROM SVM_FIZ_EXPORT_ABON;
    --
    INSERT INTO SVM_FIZ_EXPORT_ABON
    SELECT ACCOUNT_NO, LAST_NAME, FIRST_NAME, MIDDLE_NAME, 
           CONTRACT_NO, CONTRACT_DATE, BRAND_NAME, SERVICE_PROVIDER, 
           REG_ZIP, REG_REG, REG_CITY, REG_ADDR, 
           BILL_ZIP, BILL_REG, BILL_CITY, BILL_ADDR, 
           SET_ZIP, SET_REG, SET_CITY, SET_ADDR, 
           CONTACT_PHONE, EXT_SOURCE, EXT_ID 
    FROM v_FIZ_EXPORT_ABON@MMTDB;
    --
    DELETE FROM SVM_FIZ_EXPORT_ABON_PHONES;
    --
    INSERT INTO SVM_FIZ_EXPORT_ABON_PHONES
    SELECT ACCOUNT_NO, ANUMBER, ORDER_NO, ORDER_DATE, PLAN_NAME, CURATOR_FIO
    FROM v_FIZ_EXPORT_ABON_PHONES@MMTDB;
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--=========================================================================--
-- ������ ����
--=========================================================================--
--
PROCEDURE Import_XTTK
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Import_XTTK';
    v_contractor_id INTEGER;
    v_address_id    INTEGER;
    v_count         INTEGER := 0;
    v_ok            INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName);
    FOR l_cur IN (
        SELECT BRAND_NAME, COMPANY, COUNTRY, ZIP, STATE, CITY, ADDRESS FROM SVM_BRANCH
    )
    LOOP
        v_count := v_count + 1;
        v_contractor_id := Pk14_Contractor.New_contractor(
               p_type        => 'XTTK',
               p_erp_code    => NULL,
               p_inn         => NULL,
               p_kpp         => NULL, 
               p_name        => l_cur.company,
               p_short_name  => l_cur.brand_name,
               p_parent_id   => 1,
               p_notes       => NULL
           );
        IF v_contractor_id > 0 THEN
            v_address_id := Pk14_Contractor.Set_address(
               p_contractor_id => v_contractor_id,
               p_address_type  => Pk00_Const.c_ADDR_TYPE_JUR,
               p_country       => '��', 
               p_zip           => l_cur.zip,
               p_state         => l_cur.state,
               p_city          => l_cur.city, 
               p_address       => l_cur.address,
               p_phone_account => NULL,
               p_phone_billing => NULL,
               p_fax           => NULL,
               p_email         => NULL,
               p_date_from     => TO_DATE('01.01.2013','dd.mm.yyyy'),
               p_date_to       => NULL
           );  
           v_ok := v_ok + 1;
        END IF;
    END LOOP;
    Pk01_Syslog.Write_msg('End. Loaded '||v_ok||' branchs from '||v_count, c_PkgName||'.'||v_prcName);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--=========================================================================--
-- ������ ������� ��� �������
--=========================================================================--
--
PROCEDURE Import_AGENTS
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Import_AGENTS';
    v_contractor_id INTEGER;
    v_address_id    INTEGER;
    v_count         INTEGER := 0;
    v_ok            INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName);
    FOR l_cur IN (
        SELECT C.CONTRACTOR_ID, BA.BRAND, A.BRAND_NAME, A.COMPANY, A.COUNTRY, A.ZIP, A.STATE, A.CITY, A.ADDRESS 
        FROM SVM_AGENTS A,  SVM_BRAND_AGENTS BA, CONTRACTOR_T C
        WHERE A.BRAND_NAME = BA.NAME
          AND C.CONTRACTOR(+) = BA.BRAND  
    )
    LOOP
        v_count := v_count + 1;
        IF l_cur.contractor_id IS NULL THEN
            Pk01_Syslog.Write_msg(l_cur.brand||' : contractor_id - not found', 
                                  c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err);
        ELSE
            v_contractor_id := Pk14_Contractor.New_contractor(
                   p_type        => 'AGENT',
                   p_erp_code    => NULL,
                   p_inn         => NULL,
                   p_kpp         => NULL, 
                   p_name        => l_cur.company,
                   p_short_name  => l_cur.brand_name,
                   p_parent_id   => l_cur.contractor_id,
                   p_notes       => NULL
               );
            IF v_contractor_id > 0 THEN
                v_address_id := Pk14_Contractor.Set_address(
                   p_contractor_id => v_contractor_id,
                   p_address_type  => Pk00_Const.c_ADDR_TYPE_JUR,
                   p_country       => '��', 
                   p_zip           => l_cur.zip,
                   p_state         => l_cur.state,
                   p_city          => l_cur.city, 
                   p_address       => l_cur.address,
                   p_phone_account => NULL,
                   p_phone_billing => NULL,
                   p_fax           => NULL,
                   p_email         => NULL,
                   p_date_from     => TO_DATE('01.01.2013','dd.mm.yyyy'),
                   p_date_to       => NULL
               );  
               v_ok := v_ok + 1;
            END IF;
        END IF;
    END LOOP;
    Pk01_Syslog.Write_msg('End. Loaded '||v_ok||' agents from '||v_count, c_PkgName||'.'||v_prcName);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--=========================================================================--
-- ��������� ������ ������������ (������������)
--   - ���-�� �������
--   - ������������� - id ��������� �� ����� � L01
--=========================================================================--
FUNCTION Contractor_tree(p_recordset OUT t_refc) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Contractor_tree';
    v_retcode    INTEGER := 0;
BEGIN
    -- �������� ���-�� �������
    SELECT COUNT(*) INTO v_retcode 
      FROM CONTRACTOR_T
    CONNECT BY PRIOR CONTRACTOR_ID = PARENT_ID
    START WITH PARENT_ID IS NULL;
    -- ��������� ������
    OPEN p_recordset FOR
        SELECT LEVEL, CONTRACTOR_TYPE, SYS_CONNECT_BY_PATH(SHORT_NAME, ' - ') CONTRACTOR_PATH,
               CONTRACTOR_ID, PARENT_ID, CONTRACTOR, SHORT_NAME, ERP_CODE, INN, KPP 
          FROM CONTRACTOR_T
        CONNECT BY PRIOR CONTRACTOR_ID = PARENT_ID
        START WITH PARENT_ID IS NULL
        ORDER SIBLINGS BY SHORT_NAME;
    --
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RETURN(-v_retcode);
END;

-- =============================================================== --
-- �������������� ������ ���������� �� ��������� ������� DB MMTDB
-- ����������� ������ PK02_EXPORT_P@MMTDB
-- =============================================================== --
PROCEDURE Load_pk02_export_mmtdb
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Load_pk02_export_mmtdb';
    v_count     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������ ��������� ���.���
    -- ��������� ������� ����������� PK02_EXPORT_P.Exp_subs_info@MMTDB
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    EXECUTE IMMEDIATE 'TRUNCATE TABLE MMTDB_P_SUBS_INFO_T DROP STORAGE';
    INSERT INTO MMTDB_P_SUBS_INFO_T
    SELECT * FROM MDV_ADM.P_SUBS_INFO_T@MMTDB;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('MMTDB_P_SUBS_INFO_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'MMTDB_P_SUBS_INFO_T');
    
    -- ������� ������ � ������� �������� ���.����
    -- ��������� ������� ����������� PK02_EXPORT_P.Exp_subs_order@MMTDB
    EXECUTE IMMEDIATE 'TRUNCATE TABLE MMTDB_P_SUBS_ORDER_T DROP STORAGE';
    INSERT INTO MMTDB_P_SUBS_ORDER_T
    SELECT * FROM MDV_ADM.P_SUBS_ORDER_T@MMTDB;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('MMTDB_P_SUBS_ORDER_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'MMTDB_P_SUBS_ORDER_T');
    
    -- ������� ������ � �������� ��������, �� ��������� ������
    -- ��������� ������� ����������� PK02_EXPORT_P.Exp_payments(p_month)@MMTDB
    EXECUTE IMMEDIATE 'TRUNCATE TABLE MMTDB_P_PAYMENT_T DROP STORAGE';
    INSERT INTO MMTDB_P_PAYMENT_T
    SELECT * FROM MDV_ADM.P_PAYMENT_T@MMTDB;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('MMTDB_P_PAYMENT_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'MMTDB_P_PAYMENT_T');
    
    -- ������� ������ � �������� ������� �� ������ ���������� ������
    -- ��������� ������� ����������� PK02_EXPORT_P.Exp_open_balance(p_month)@MMTDB
    EXECUTE IMMEDIATE 'TRUNCATE TABLE MMTDB_P_SUBS_PERIOD_INFO_T DROP STORAGE';
    INSERT INTO MMTDB_P_SUBS_PERIOD_INFO_T
    SELECT * FROM MDV_ADM.P_SUBS_PERIOD_INFO_T@MMTDB;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('MMTDB_P_SUBS_PERIOD_INFO_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'MMTDB_P_SUBS_PERIOD_INFO_T');
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--=========================================================================--
-- ������ �������
-- ��������� ������� ����������� PK02_EXPORT_P.Exp_subs_info@MMTDB
--=========================================================================--
PROCEDURE Import_Persons
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Import_Persons';
    v_brand_id      INTEGER;
    v_contract_id   INTEGER;
    v_account_id    INTEGER;
    v_profile_id    INTEGER;
    v_contractor_id INTEGER; 
    v_subscriber_id INTEGER;
    v_parent_id     INTEGER;
    v_address_id    INTEGER;
    v_order_id      INTEGER;
    v_order_body_id INTEGER;
    v_rateplan_id   INTEGER;
    v_row_id        VARCHAR2(200);
    v_count         INTEGER := 0;
    v_all           INTEGER := 0;
    v_ok            INTEGER := 0;
    v_err           INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName);
    --
    FOR l_cur IN (
       SELECT ACCOUNT_NO, 
              LAST_NAME, FIRST_NAME, MIDDLE_NAME, 
              CONTRACT_NO, CONTRACT_DATE, 
              BRAND_NAME, SERVICE_PROVIDER, 
              REG_ZIP, REG_REG, REG_CITY, REG_ADDR, 
              BILL_ZIP, BILL_REG, BILL_CITY, BILL_ADDR, 
              SET_ZIP, SET_REG, SET_CITY, SET_ADDR, 
              CONTACT_PHONE, 
              EXT_SOURCE, EXT_ID, 
              ACCOUNT_STATUS  
         FROM MMTDB_P_SUBS_INFO_T
         WHERE ACCOUNT_NO  IS NOT NULL
           AND CONTRACT_NO IS NOT NULL
    )
    LOOP
        v_all := v_all + 1;
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- CONTRACT_T - ������� �������
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        BEGIN
            -- �������� ID ��������
            SELECT CONTRACT_ID INTO v_contract_id
              FROM CONTRACT_T
             WHERE CONTRACT_NO = l_cur.CONTRACT_NO;
        EXCEPTION WHEN NO_DATA_FOUND THEN
            v_contract_id := PK12_CONTRACT.Open_contract(
                                 p_contract_no=> l_cur.Contract_No,
                                 p_date_from  => l_cur.Contract_Date,
                                 p_date_to    => NULL,
                                 p_client_id  => PK00_CONST.c_CLIENT_PERSON_ID,
                                 p_manager_id => c_MANAGER_SIEBEL_ID
                               );
        END;

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- ������� ������� ���.����
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        v_subscriber_id := PK21_SUBSCRIBER.New_subscriber(
               p_last_name   => l_cur.last_name,   -- �������
               p_first_name  => l_cur.first_name,   -- ��� 
               p_middle_name => l_cur.middle_name,  -- ��������
               p_category    => Pk00_Const.c_SUBS_RESIDENT  -- ��������� 1/2 = ��������/����������
           );

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- �������� ����� ����������� ���.����
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        --v_document_id := PK21_SUBSCRIBER.Add_document(
        --                         p_subscriber_id => v_subscriber_id,
        --                         p_doc_type      => NULL,
        --                         p_doc_serial    => NULL,
        --                         p_doc_no        => NULL,
        --                         p_doc_issuer    => NULL,
        --                         p_doc_issue_date=> NULL
        --\                       );

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- ACCOUNT_T - ������� ������� ����
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        v_account_id := PK05_ACCOUNT.New_account(
                                 p_account_no   => l_cur.Account_No,
                                 p_account_type => PK00_CONST.c_ACC_TYPE_P,
                                 p_currency_id  => PK00_CONST.c_CURRENCY_RUB,
                                 p_status       => PK00_CONST.c_ACC_STATUS_BILL,
                                 p_parent_id    => NULL
                               );
        
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- ACCOUNT_PROFILE_T - ������� ������� �������� �����
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- �������� ������ � ������� � ������
        BEGIN
            SELECT CONTRACTOR_ID, PARENT_ID 
              INTO v_contractor_id, v_parent_id
              FROM CONTRACTOR_T
             WHERE SHORT_NAME = l_cur.Brand_Name;
        EXCEPTION WHEN NO_DATA_FOUND THEN
            -- �� ����� - ����� ��������
            v_contractor_id := NULL;
            v_parent_id := NULL;
        END;
            
        -- �������� ������ � ������
        BEGIN
            SELECT BRAND_ID 
              INTO v_brand_id
              FROM BRAND_T
             WHERE BRAND = l_cur.Brand_Name;
        EXCEPTION WHEN NO_DATA_FOUND THEN
            -- �� ����� - ����� ��������
            v_brand_id := NULL;
        END;
        
        
        -- ������� ������� �������� �����
        v_profile_id := PK05_ACCOUNT.Set_profile(
                             p_account_id    => v_account_id,
                             p_brand_id      => v_brand_id,
                             p_contract_id   => v_contract_id,
                             p_customer_id   => PK00_CONST.c_CUSTOMER_PERSON_ID,
                             p_subscriber_id => v_subscriber_id,
                             p_contractor_id => c_CONTRACTOR_KTTK_ID,
                             p_branch_id     => v_parent_id,
                             p_agent_id      => v_contractor_id,
                             p_contractor_bank_id => Pk00_Const.c_KTTK_P_BANK_ID,
                             p_vat           => Pk00_Const.c_VAT,
                             p_date_from     => l_cur.Contract_Date,
                             p_date_to       => NULL
                           );

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- ACCOUNT_CONTACT_T - �������� ������ �� �/�
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- �������� ����� �����������
        v_address_id := PK05_ACCOUNT.Add_address(
                             p_account_id   => v_account_id,
                             p_address_type => PK00_CONST.c_ADDR_TYPE_REG,
                             p_country      => '��',
                             p_zip          => l_cur.reg_zip,
                             p_state        => l_cur.reg_reg,
                             p_city         => l_cur.reg_city,
                             p_address      => l_cur.reg_addr,
                             p_person       => l_cur.last_name||' '||l_cur.first_name||' '||l_cur.middle_name,
                             p_phones       => l_cur.contact_phone,
                             p_fax          => NULL,
                             p_email        => NULL,
                             p_date_from    => l_cur.Contract_Date,
                             p_date_to      => NULL
                          );
                          
        -- �������� ����� �������� �����
        v_address_id := PK05_ACCOUNT.Add_address(
                             p_account_id   => v_account_id,
                             p_address_type => PK00_CONST.c_ADDR_TYPE_DLV,
                             p_country      => '��',
                             p_zip          => l_cur.bill_zip,
                             p_state        => l_cur.bill_reg,
                             p_city         => l_cur.bill_city,
                             p_address      => l_cur.bill_addr,
                             p_person       => l_cur.last_name||' '||l_cur.first_name||' '||l_cur.middle_name,
                             p_phones       => l_cur.contact_phone,
                             p_fax          => NULL,
                             p_email        => NULL,
                             p_date_from    => l_cur.Contract_Date,
                             p_date_to      => NULL
                          );
                                  
        -- �������� ����� ��������� ������������
        v_address_id := PK05_ACCOUNT.Add_address(
                             p_account_id   => v_account_id,
                             p_address_type => PK00_CONST.c_ADDR_TYPE_SET,
                             p_country      => '��',
                             p_zip          => l_cur.set_zip,
                             p_state        => l_cur.set_reg,
                             p_city         => l_cur.set_city,
                             p_address      => l_cur.set_addr,
                             p_person       => l_cur.last_name||' '||l_cur.first_name||' '||l_cur.middle_name,
                             p_phones       => NULL,
                             p_fax          => NULL,
                             p_email        => NULL,
                             p_date_from    => l_cur.Contract_Date,
                             p_date_to      => NULL
                          );
               
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- ORDER_T - �������� ������ �� �/�
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        FOR l_order IN (
            SELECT ACCOUNT_NO, ANUMBER, ORDER_NO, ORDER_DATE, PLAN_NAME, CURATOR_FIO
              FROM MMTDB_P_SUBS_ORDER_T
             WHERE ACCOUNT_NO = l_cur.ACCOUNT_NO
               AND PLAN_COUNT > 1
        )
        LOOP
            -- ��������� ���� �� ����� � ��������� �������
            SELECT COUNT(*) INTO v_count 
              FROM ORDER_T
             WHERE ORDER_NO = l_order.order_no;
            IF v_count = 0 THEN
                -- �������� ��������� ��������� �����
                BEGIN
                    SELECT RATEPLAN_ID INTO v_rateplan_id
                      FROM RATEPLAN_T
                     WHERE RATEPLAN_NAME = l_order.plan_name;
                EXCEPTION WHEN NO_DATA_FOUND THEN
                    v_rateplan_id := 0; -- ����������� ��
                END;  
                -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
                -- ������� ����� �� ������ ��/�� �����
                v_order_id := PK06_ORDER.New_order(
                                  p_account_id => v_account_id,
                                  p_order_no   => l_order.order_no,
                                  p_service_id => PK00_CONST.c_SERVICE_CALL_MGMN,
                                  p_time_zone  => NULL,
                                  p_rateplan_id=> v_rateplan_id,
                                  p_date_from  => l_order.order_date,
                                  p_date_to    => NULL);
                -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
                -- ������� ������ ������ ��� ��
                v_order_body_id := PK06_ORDER.Add_subservice(
                               p_order_id      => v_order_id,
                               p_subservice_id => PK00_CONST.c_SUBSRV_MG,
                               p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
                               p_date_from     => l_order.order_date,
                               p_date_to       => NULL
                           );
                -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
                -- ������� ������ ������ ��� ��
                v_order_body_id := PK06_ORDER.Add_subservice(
                               p_order_id      => v_order_id,
                               p_subservice_id => PK00_CONST.c_SUBSRV_MN,
                               p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
                               p_date_from     => l_order.order_date,
                               p_date_to       => NULL
                           );
           
            ELSE
                -- ������ ID ������
                SELECT ORDER_ID INTO v_order_id
                FROM ORDER_T
                WHERE ORDER_NO = l_order.order_no;
            END IF;
            -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
            -- ��������� � ������ - ���������� ������
            BEGIN 
                v_row_id := PK18_RESOURCE.Add_phone(
                               p_order_id  => v_order_id,
                               p_phone     => l_order.anumber,
                               p_date_from => l_order.order_date,
                               p_date_to   => NULL
                           );
                v_ok := v_ok + 1;
            EXCEPTION
                WHEN OTHERS THEN
                    v_err := v_err + 1;
            END;   
            --
        END LOOP;
        -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
        -- ������� �����������
        IF MOD(v_all, 1000) = 0 THEN
            Pk01_Syslog.Write_msg('���������� '||v_all||' �����', c_PkgName||'.'||v_prcName );
        END IF;
        --
    END LOOP;    
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.Write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- ������� ��������� ������� �������� �����
PROCEDURE New_billinfo (
               p_account_id    IN INTEGER
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'New_billinfo';
    v_period_id      INTEGER;
    v_last_period_id INTEGER;
    v_next_period_id INTEGER;
    v_period_from    DATE;
    v_period_to      DATE;
    v_period_length  INTEGER := 1;
    v_bill_id        INTEGER;
    v_next_bill_id   INTEGER;
    v_last_bill_id   INTEGER;
    v_bill_no        BILL_T.BILL_NO%TYPE := NULL;
    v_prev_bill_id   INTEGER;
    v_currency_id    INTEGER := PK00_CONST.c_CURRENCY_RUB;
BEGIN
    -- ---------------------------------------------------------- --
    -- ������ ����� �������: LAST ��� BILL 
    -- ---------------------------------------------------------- --
    -- �������� ID ������������ �������
    BEGIN
        -- ID ����� ����������� �������
        v_prev_bill_id := NULL;
        -- �������� ��������� �������
        SELECT PERIOD_ID, PERIOD_FROM, PERIOD_TO
          INTO v_last_period_id, v_period_from, v_period_to
          FROM PERIOD_T
         WHERE POSITION = PK00_CONST.c_PERIOD_BILL;  
        -- �������� ����� ����� ��� �������
        v_bill_no := Pk07_Bill.Make_bill_no( p_account_id, v_last_period_id);
        -- ������� ���� ��� �������
        v_last_bill_id := Pk07_Bill.Open_recuring_bill (
                   p_account_id,    -- ID �������� �����
                   v_last_period_id,-- ID ���������� ������� YYYYMM
                   v_bill_no,       -- ����� �����
                   v_currency_id,   -- ID ������ �����
                   v_period_to      -- ���� ����� (������������ �������)
               );
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- �������� ��������� �������
            SELECT PERIOD_ID, PERIOD_FROM, PERIOD_TO
              INTO v_last_period_id, v_period_from, v_period_to
              FROM PERIOD_T
             WHERE POSITION = PK00_CONST.c_PERIOD_LAST;  
            -- �������� ����� ����� ��� �������
            v_bill_no := Pk07_Bill.Make_bill_no( p_account_id, v_last_period_id);
            -- ������� ���� ��� �������
            v_last_bill_id := Pk07_Bill.Open_recuring_bill (
                       p_account_id,    -- ID �������� �����
                       v_last_period_id,-- ID ���������� ������� YYYYMM
                       v_bill_no,       -- ����� �����
                       v_currency_id,   -- ID ������ �����
                       v_period_to      -- ���� ����� (������������ �������)
                   );
    END;
    -- ---------------------------------------------------------- --
    -- ������� ������
    -- ---------------------------------------------------------- --
    -- ID ����� ����������� �������
    v_prev_bill_id := v_last_bill_id;
    -- �������� ��������� �������
    SELECT PERIOD_ID, PERIOD_FROM, PERIOD_TO
      INTO v_period_id, v_period_from, v_period_to
      FROM PERIOD_T
     WHERE POSITION = PK00_CONST.c_PERIOD_OPEN;  
    -- �������� ����� ����� ��� �������
    v_bill_no := Pk07_Bill.Make_bill_no( p_account_id, v_period_id);
    -- ������� ���� ��� �������
    v_bill_id := Pk07_Bill.Open_recuring_bill (
               p_account_id,   -- ID �������� �����
               v_period_id,    -- ID ���������� ������� YYYYMM
               v_bill_no,      -- ����� �����
               v_currency_id,  -- ID ������ �����
               v_period_to     -- ���� ����� (������������ �������)
           );
           
    -- ---------------------------------------------------------- --
    -- ��������� ������
    -- ---------------------------------------------------------- --
    -- �������� ID ����������� ���������� �������
    v_prev_bill_id := v_bill_id;
    -- �������� ID ���������� ������� �����
    SELECT PERIOD_ID, PERIOD_FROM, PERIOD_TO
      INTO v_next_period_id, v_period_from, v_period_to
      FROM PERIOD_T
     WHERE POSITION = PK00_CONST.c_PERIOD_NEXT;
    -- �������� ����� ����� �������� �������
    v_bill_no := Pk07_Bill.Make_bill_no( p_account_id, v_next_period_id);
    -- ������� ���� ��� �������� �������
    v_next_bill_id := Pk07_Bill.Open_recuring_bill (
               p_account_id,    -- ID �������� �����
               v_next_period_id,-- ID ���������� ������� YYYYMM
               v_bill_no,       -- ����� �����
               v_currency_id,   -- ID ������ �����
               v_period_to      -- ���� ����� (������������ �������)
           ); 
    -- - - - - - - - - --
    -- ������� �������������� ������ � ������ ��� ����� ���������� �/�
    INSERT INTO BILLINFO_T ( 
        ACCOUNT_ID, 
        LAST_PERIOD_ID, PERIOD_ID, NEXT_PERIOD_ID, 
        LAST_BILL_ID, BILL_ID, NEXT_BILL_ID, 
        PERIOD_LENGTH, CURRENCY_ID 
    ) VALUES (
        p_account_id, 
        v_last_period_id, v_period_id, v_next_period_id,
        v_last_bill_id, v_bill_id, v_next_bill_id, 
        v_period_length, v_currency_id
    );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,account_id='||p_account_id, c_PkgName||'.'||v_prcName );
END;

--=========================================================================--
-- ������� ����� ��� ���� �/� ��� ����������� �������� �� PERIOD_T
--=========================================================================--
PROCEDURE Make_Bill_For_Periods 
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Make_Bill_For_Periods';
    v_bill_id  INTEGER;
    v_all      INTEGER := 0;
    v_ok       INTEGER := 0;
    v_err      INTEGER := 0;
    --
    v_date_from       DATE;
    --
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    --
    FOR l_cur IN ( SELECT ACCOUNT_ID, CURRENCY_ID 
                     FROM ACCOUNT_T 
                    WHERE STATUS = Pk00_Const.c_ACC_STATUS_BILL 
                      AND ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_P )
    LOOP
        v_all := v_all + 1;
        BEGIN
            -- �������� ��������� ������ � ����� ������ ��� ������ �/�
            New_billinfo ( p_account_id    => l_cur.account_id ); -- ID �������� �����
            v_ok := v_ok + 1;
        EXCEPTION
            WHEN OTHERS THEN
              Pk01_Syslog.Write_msg(
                 p_Msg  => l_cur.account_id || ' - error',
                 p_Src  => c_PkgName||'.'||v_prcName,
                 p_Level=> Pk01_Syslog.L_err );
              v_err := v_err + 1;
        END;
        -- ������� �����������
        IF MOD(v_all, 1000) = 0 THEN
            Pk01_Syslog.Write_msg('Processed: '||v_ok||'-��, '||v_err||'-err from '||v_all, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        --
    END LOOP;
    --
    Pk01_Syslog.Write_msg('Processed: '||v_ok||'-��, '||v_err||'-err from '||v_all, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--=========================================================================--
-- ������� ��������
-- ��������� ������� ����������� PK02_EXPORT_P.Payments@MMTDB
--=========================================================================--
PROCEDURE Import_payments 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Import_payments';
    v_payment_id INTEGER;
    v_all        INTEGER := 0;
    v_ok         INTEGER := 0;
    v_err        INTEGER := 0;
    v_count      INTEGER := 0;
    --
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������� ������ � ������� �������� ���.����
    -- ��������� ������� ����������� PK02_EXPORT_P.Exp_subs_order@MMTDB
    EXECUTE IMMEDIATE 'TRUNCATE TABLE MMTDB_P_PAYMENT_T DROP STORAGE';
    INSERT INTO MMTDB_P_PAYMENT_T (
           PAY_POID_ID0, REP_PERIOD_ID, PAYMENT_DATE, 
           ACCOUNT_NO, PAY_AMOUNT, PAY_BALANCE, PAY_TRANSFERED, 
           BANK_CODE, DOC_ID, CREATED_T, MODIFY_T, PAY_DESCR)
    SELECT PAY_POID_ID0, REP_PERIOD_ID, PAYMENT_DATE, 
           ACCOUNT_NO, PAY_AMOUNT, PAY_BALANCE, PAY_TRANSFERED, 
           BANK_CODE, DOC_ID, CREATED_T, MODIFY_T, PAY_DESCR
      FROM MDV_ADM.M_PAYMENTS_T@MMTDB;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('MMTDB_P_PAYMENT_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'MMTDB_P_PAYMENT_T');
    --
    FOR r_pay IN ( SELECT P.PAY_POID_ID0, P.REP_PERIOD_ID, P.PAYMENT_DATE, 
                          A.ACCOUNT_ID, P.ACCOUNT_NO, 
                          P.PAY_AMOUNT, P.PAY_BALANCE, P.PAY_TRANSFERED, 
                          P.BANK_CODE, P.DOC_ID, P.CREATED_T, P.MODIFY_T, P.PAY_DESCR
                     FROM MMTDB_P_PAYMENT_T P, ACCOUNT_T A
                    WHERE A.ACCOUNT_NO = P.ACCOUNT_NO
                      --AND P.REP_PERIOD_ID IN (201402)
                      --AND P.PAY_POID_ID0 IN (32481175852,32330951794)
                 )
    LOOP
        v_all := v_all + 1;
        BEGIN
            -- �������� ������ �� �/� ������� (����� ����� ����������� � ������� �/�)
            v_payment_id := PK10_PAYMENT.Add_payment (
                p_account_id      => r_pay.account_id,   -- ID �������� ����� �������
                p_rep_period_id   => r_pay.rep_period_id,-- ID ��������� ������� ���� ����������� ������
                p_payment_dat�    => r_pay.payment_date, -- ���� �������
                p_payment_type    => NULL,               -- ��� �������
                p_recvd           => r_pay.pay_amount,   -- ����� �������
                p_paysystem_id    => NULL,               -- ID ��������� �������
                p_doc_id          => r_pay.doc_id,       -- ID ��������� � ��������� �������
                p_status          => NULL,               -- ������ �������
                p_manager    	  => r_pay.bank_code,    -- �.�.�. ��������� ��������������� ������ �� �/�
                p_notes           => r_pay.pay_descr    -- ���������� � �������  
           ); 
            v_ok := v_ok + 1;
        EXCEPTION
            WHEN OTHERS THEN
              Pk01_Syslog.Write_msg(
                 p_Msg  => 'pay_poid_id0='||r_pay.pay_poid_id0 || ' - error',
                 p_Src  => c_PkgName||'.'||v_prcName,
                 p_Level=> Pk01_Syslog.L_err );
              v_err := v_err + 1;
        END;
        -- ������� �����������
        IF MOD(v_all, 1000) = 0 THEN
            Pk01_Syslog.Write_msg('Processed: '||v_ok||'-��, '||v_err||'-err from '||v_all, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        --
    END LOOP;
    --
    Pk01_Syslog.Write_msg('Processed: '||v_ok||'-��, '||v_err||'-err from '||v_all, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    COMMIT;    
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--=========================================================================--
-- ������� �������� �������� �� 01.01.2014 �� ������� MDV_ADM.G_BALANCE_T@MMTDB,
-- ������� ��������� �.�.�����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ����� �������� �/� ������� ��� � ���� �� ������ �������:
-- SELECT GB.ACCOUNT_NO, AMOUNT, PAYMENTS, BALANCE_20140101 
--   FROM MDV_ADM.G_BALANCE_T@MMTDB GB
--  WHERE NOT EXISTS (
--     SELECT * FROM ACCOUNT_T A 
--     WHERE A.ACCOUNT_NO = GB.ACCOUNT_NO
--  )
--=========================================================================--
PROCEDURE Import_open_balance
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Import_open_balance';
    v_count           INTEGER := 0;
    v_min_period_id   INTEGER;
    v_start_period_id INTEGER;
    v_item_id         INTEGER := 0;
    v_item_type       ITEM_T.ITEM_TYPE%TYPE;
    v_period_to       DATE := TO_DATE('31.12.2013 23:59:59','dd.mm.yyyy hh24:mi:ss');
    v_rep_period_id   INTEGER := 201312;
    v_vat             NUMBER := 18;
    v_transfer_id     INTEGER;
    v_value           NUMBER;
    v_open_balance    NUMBER;
    v_close_balance   NUMBER;
    v_bill_due        NUMBER;
    --
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ���������� �� ��� ������� �� 01.01.2014
    INSERT INTO ITEM_T (
       BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, 
       ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE,
       ITEM_TOTAL, ADJUSTED, RECVD, 
       DATE_FROM, DATE_TO, LAST_MODIFIED,
       INV_ITEM_ID, ITEM_STATUS, TAX_INCL
    )
    SELECT B.BILL_ID, B.REP_PERIOD_ID, ROWNUM ITEM_ID, PK00_CONST.c_ITEM_TYPE_BILL,
           NULL ORDER_ID, PK00_CONST.c_SERVICE_CALL_MGMN, 
           NULL SUBSERVICE_ID, PK00_CONST.c_CHARGE_TYPE_USG,       
           GB.AMOUNT, 0 ADJUSTED, 0 RECVD,
           NULL DATE_FROM, v_period_to DATE_TO, SYSDATE LAST_MODIFIED,
           NULL INV_ITEM_ID, PK00_CONST.c_ITEM_STATE_OPEN, 
           PK00_CONST.c_RATEPLAN_TAX_INCL
      FROM MDV_ADM.G_BALANCE_T@MMTDB GB, ACCOUNT_T A, BILL_T B
     WHERE B.REP_PERIOD_ID = v_rep_period_id
       AND B.ACCOUNT_ID = A.ACCOUNT_ID
       AND A.ACCOUNT_NO = GB.ACCOUNT_NO;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Inserted: '||v_count||'- items', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ������������� ���� ������ ����������
    UPDATE ITEM_T I
       SET DATE_FROM = (
           SELECT MIN(DATE_FROM)
             FROM ACCOUNT_PROFILE_T AP, BILL_T B
            WHERE B.BILL_ID = I.BILL_ID
              AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND AP.ACCOUNT_ID = B.ACCOUNT_ID
       ),
       REP_GROSS = (I.ITEM_TOTAL - PK09_INVOICE.ALLOCATE_TAX(I.ITEM_TOTAL, v_vat)),
       REP_TAX = (PK09_INVOICE.ALLOCATE_TAX(I.ITEM_TOTAL, v_vat))
     WHERE I.REP_PERIOD_ID = v_rep_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Updated: '||v_count||'- items', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ �������� �� ��� ������� �� 01.01.2014 
    -- c�������� ���������� � �������
    INSERT INTO PAYMENT_T (
        PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE,
        PAYMENT_DATE, ACCOUNT_ID, RECVD,
        ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED,
        DATE_FROM, DATE_TO,
        PAYSYSTEM_ID, DOC_ID,
        STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED,
        CREATED_BY, NOTES, 
        PREV_PAYMENT_ID, PREV_PERIOD_ID
    )
    SELECT 
        ROWNUM PAYMENT_ID, v_rep_period_id REP_PERIOD_ID, 'ALL' PAYMENT_TYPE,
        v_period_to PAYMENT_DATE, A.ACCOUNT_ID, GB.PAYMENTS RECVD,
        GB.PAYMENTS ADVANCE, v_period_to ADVANCE_DATE, 
        GB.PAYMENTS BALANCE, 0 TRANSFERED,
        NULL DATE_FROM, v_period_to DATE_TO,
        0 PAYSYSTEM_ID, ROWNUM DOC_ID,
        'IMPORT' STATUS, v_period_to STATUS_DATE, 
        SYSDATE CREATE_DATE, NULL LAST_MODIFIED,
        'A.Y. Gurov' CREATED_BY, '������ ������ ����� �������� �� �� �� 01.01.2014' NOTES, 
        NULL PREV_PAYMENT_ID, NULL PREV_PERIOD_ID
      FROM MDV_ADM.G_BALANCE_T@MMTDB GB, ACCOUNT_T A
     WHERE A.ACCOUNT_NO = GB.ACCOUNT_NO
       AND GB.PAYMENTS IS NOT NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Inserted: '||v_count||'- payments', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info ); 
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ����� ��� �������������� ������-������ �� 31.12.2013 23:59:59, ���������� ����� ���� ����������
    INSERT INTO INVOICE_ITEM_T (
       BILL_ID, REP_PERIOD_ID,
       INV_ITEM_ID, INV_ITEM_NO, SERVICE_ID, INV_ITEM_NAME, 
       VAT,         -- ������ ��� � ���������
       TOTAL,       -- ����� ���������� � �������
       GROSS,       -- ����� ���������� ��� ������
       TAX,         -- ����� ������
       DATE_FROM, DATE_TO
    )
    SELECT 
       I.BILL_ID, I.REP_PERIOD_ID,
       I.ITEM_ID, 1 INV_ITEM_NO, I.SERVICE_ID, '������ ���������� �� 01.01.2014' INV_ITEM_NAME,
       v_vat VAT, I.ITEM_TOTAL, 
       I.ITEM_TOTAL - PK09_INVOICE.Allocate_tax(I.ITEM_TOTAL, v_vat) GROSS,
       PK09_INVOICE.Calc_tax(I.ITEM_TOTAL, v_vat),
       I.DATE_FROM, I.DATE_TO
      FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = v_rep_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Inserted: '||v_count||'- invoice_items', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ �������������� ������ �� 31.12.2013 23:59:59, ���������� ����� ���� ����������
    UPDATE BILL_T B 
       SET (B.TOTAL, B.GROSS, B.TAX, B.DUE, B.DUE_DATE, B.CALC_DATE) = (
        SELECT NVL(SUM(II.TOTAL), 0), 
               NVL(SUM(II.GROSS), 0), 
               NVL(SUM(II.TAX),   0),
               NVL(-SUM(II.TOTAL),0), 
               ADD_MONTHS(B.BILL_DATE, 1) DUE_DATE, SYSDATE CALC_DATE
          FROM INVOICE_ITEM_T II
         WHERE II.BILL_ID = B.BILL_ID
           AND II.REP_PERIOD_ID = B.REP_PERIOD_ID

        )
    WHERE B.REP_PERIOD_ID = v_rep_period_id;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||' - bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- 
    -- ��������� �������������� ������� ����� ��� ������ �� 31.12.2013 23:59:59
    --
    INSERT INTO ITEM_T (
        BILL_ID, REP_PERIOD_ID, 
        ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
        ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
        ITEM_TOTAL, ADJUSTED, RECVD, 
        DATE_FROM, DATE_TO, 
        ITEM_STATUS, 
        CREATE_DATE, LAST_MODIFIED, 
        REP_GROSS, REP_TAX, TAX_INCL)
    SELECT 
        B.BILL_ID, B.REP_PERIOD_ID,
        SQ_ITEM_ID.NEXTVAL, 
        'P' ITEM_TYPE, 
        NULL INV_ITEM_ID, NULL ORDER_ID, 
        NULL SERVICE_ID, NULL SUBSERVICE_ID, NULL CHARGE_TYPE,
        0 ITEM_TOTAL, 0 ADJUSTED, 
        CASE
          WHEN P.RECVD >= B.TOTAL THEN B.TOTAL
          ELSE P.RECVD 
        END RECVD,
        TO_DATE('01.12.2013','dd.mm.yyyy') DATE_FROM,
        TO_DATE('31.12.2013 23:59:59','dd.mm.yyyy hh24:mi:ss') DATE_TO,
        'OPEN' ITEM_STATUS,
        SYSDATE CREATE_DATE,
        NULL LAST_MODIFIED, 
        NULL REP_GROSS,
        NULL REP_TAX,
        'Y' TAX_INCL
      FROM BILL_T B, PAYMENT_T P, ACCOUNT_T A
     WHERE P.ACCOUNT_ID = A.ACCOUNT_ID
       AND A.ACCOUNT_TYPE = 'P'
       AND P.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND B.REP_PERIOD_ID = 201312
       AND B.ACCOUNT_ID = A.ACCOUNT_ID
       AND B.TOTAL != 0
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Inserted: '||v_count||'- pay_item', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������� ��������
    --
    INSERT INTO PAY_TRANSFER_T (
        TRANSFER_ID, PAYMENT_ID, PAY_PERIOD_ID, 
        BILL_ID, REP_PERIOD_ID, ITEM_ID, 
        TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE, 
        TRANSFER_DATE, PREV_TRANSFER_ID, NOTES
    )
    SELECT 
        P.PAYMENT_ID TRANSFER_ID, -- SQ_TRANSFER_ID.NEXTVAL 
        P.PAYMENT_ID, P.REP_PERIOD_ID PAY_PERIOD_ID, 
        B.BILL_ID, B.REP_PERIOD_ID, I.ITEM_ID, 
        I.RECVD TRANSFER_TOTAL, P.RECVD OPEN_BALANCE, (P.RECVD - I.RECVD) CLOSE_BALANCE, 
        SYSDATE TRANSFER_DATE, 
        NULL PREV_TRANSFER_ID, '�������� ��������� �������' NOTES
      FROM ITEM_T I, BILL_T B, ACCOUNT_T A, PAYMENT_T P
     WHERE I.ITEM_TYPE     = 'P'
       AND I.REP_PERIOD_ID = 201312
       AND I.BILL_ID       = B.BILL_ID
       AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND A.ACCOUNT_ID    = B.ACCOUNT_ID
       AND A.ACCOUNT_TYPE  = 'P' 
       AND P.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND P.ACCOUNT_ID    = A.ACCOUNT_ID
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Inserted: '||v_count||'- pay_transfer', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� �������� ������� �� �������
    UPDATE PAYMENT_T P
       SET P.BALANCE = (
         SELECT T.CLOSE_BALANCE 
           FROM PAY_TRANSFER_T T
          WHERE T.PAYMENT_ID = P.PAYMENT_ID
            AND P.REP_PERIOD_ID = 201312
       ) 
    WHERE EXISTS ( 
        SELECT * FROM PAY_TRANSFER_T T
         WHERE T.PAYMENT_ID = P.PAYMENT_ID
           AND P.REP_PERIOD_ID = 201312
    )
    AND P.REP_PERIOD_ID = 201312;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Update: '||v_count||'- payment_t.balance', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ������� ��������� � ����� ����� �������
    UPDATE PAYMENT_T P SET P.TRANSFERED = (P.RECVD - P.BALANCE), 
                           P.ADVANCE    =  P.BALANCE
     WHERE P.REP_PERIOD_ID = 201312;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Update: '||v_count||'- payment_t.advance', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ������� �� ������������ �����
    UPDATE BILL_T B
       SET RECVD = (
         SELECT I.RECVD FROM ITEM_T I
          WHERE I.BILL_ID = B.BILL_ID
            AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
            AND I.ITEM_TYPE = 'P'
       )
     WHERE B.REP_PERIOD_ID = 201312
       AND EXISTS (
         SELECT * FROM ITEM_T I
          WHERE I.BILL_ID = B.BILL_ID
            AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
            AND I.ITEM_TYPE = 'P'
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Update: '||v_count||'- bill_t.recvd', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������������� ������� ������
    UPDATE BILL_T B SET DUE = (RECVD - TOTAL)
     WHERE B.REP_PERIOD_ID = 201312;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Update: '||v_count||'- bill_t.due', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
         
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ �������� �������� �� 01.01.2014
    INSERT INTO REP_PERIOD_INFO_T(
        REP_PERIOD_ID, ACCOUNT_ID, OPEN_BALANCE, CLOSE_BALANCE, 
        TOTAL, GROSS, RECVD, ADVANCE, LAST_MODIFIED
    )
    SELECT v_rep_period_id, A.ACCOUNT_ID, 0 OPEN_BALANCE, BALANCE_20140101 CLOSE_BALANCE,
           AMOUNT TOTAL, 
           AMOUNT - PK09_INVOICE.Allocate_tax(AMOUNT, v_vat) GROSS, 
           NVL(PAYMENTS, 0) RECVD, 
           CASE
           WHEN BALANCE_20140101 > 0 THEN BALANCE_20140101 
           ELSE 0 
           END ADVANCE, 
           SYSDATE LAST_MODIFIED
      FROM MDV_ADM.G_BALANCE_T@MMTDB GB, ACCOUNT_T A
     WHERE GB.ACCOUNT_NO = A.ACCOUNT_NO;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Inserted: '||v_count||'- rep_period_info', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Gather_Table_Stat(l_Tab_Name => 'REP_PERIOD_INFO_T');
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------------- --
-- ����� �������� �������� �� 01.01.2014
-- ----------------------------------------------------------------------- --
PROCEDURE Rollback_open_balance
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_open_balance';
    v_rep_period_id INTEGER := 201312;
    v_count         INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ����� �������� �������� �� 01.01.2014
    DELETE FROM REP_PERIOD_INFO_T PI  WHERE PI.REP_PERIOD_ID = v_rep_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('deleted: '||v_count||' rows from rep_period_info_t', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����� ������ � �������� ��������
    DELETE FROM PAY_TRANSFER_T WHERE REP_PERIOD_ID = v_rep_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('deleted: '||v_count||' rows from pay_transfer_t', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����� invoice_item_t
    DELETE FROM INVOICE_ITEM_T WHERE REP_PERIOD_ID = v_rep_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('deleted: '||v_count||' rows from invoice_item_t', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- ����� item_t
    DELETE FROM ITEM_T WHERE REP_PERIOD_ID = v_rep_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('deleted: '||v_count||' rows from item_t', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ����� payment_t
    DELETE FROM PAYMENT_T WHERE REP_PERIOD_ID = v_rep_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('deleted: '||v_count||' rows from payment_t', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ����� ���������� �� ������
    UPDATE BILL_T
       SET TOTAL = 0, GROSS = 0, TAX = 0, RECVD = 0, DUE = 0, BILL_STATUS = 'OPEN'
     WHERE REP_PERIOD_ID = v_rep_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('updated: '||v_count||' rows bill_t', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------------- --
-- ���� ��������� �������� �������� �� 01.01.2014
-- ----------------------------------------------------------------------- --
PROCEDURE Test_open_balance
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Test_open_balance';
    v_rep_period_id INTEGER := 201312;
    v_count         INTEGER;
    v_total         NUMBER;
    v_gross         NUMBER;
    v_tax           NUMBER;
    v_recvd         NUMBER;
    v_balance       NUMBER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start '||v_rep_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- �������� �������� �������� �� 01.01.2014
    SELECT COUNT(*), SUM(TOTAL), SUM(RECVD) 
      INTO v_count, v_total, v_recvd
      FROM REP_PERIOD_INFO_T
     WHERE REP_PERIOD_ID = v_rep_period_id;
    Pk01_Syslog.Write_msg('rep_period_info_t < '||
                          'count='||v_count||', total='||v_total||', recvd='||v_recvd||' >',
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- �������� ������ � �������� ��������
    SELECT COUNT(*), SUM(TRANSFER_TOTAL)
      INTO v_count, v_total
      FROM PAY_TRANSFER_T  
     WHERE REP_PERIOD_ID = v_rep_period_id;
    Pk01_Syslog.Write_msg('pay_transfer_t < '||
                          'count='||v_count||', total='||v_total||' >',
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- �������� invoice_item_t
    SELECT COUNT(*), SUM(TOTAL), SUM(GROSS), SUM(TAX)
      INTO v_count, v_total, v_gross, v_tax
      FROM INVOICE_ITEM_T  
     WHERE REP_PERIOD_ID = v_rep_period_id;
    Pk01_Syslog.Write_msg('invoice_item_t < '||
                          'count='||v_count||', total='||v_total||', '||
                          'gross='||v_gross||', tax='||v_tax||' >',
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- �������� item_t
    SELECT COUNT(*), SUM(ITEM_TOTAL), SUM(RECVD)
      INTO v_count, v_total, v_recvd
      FROM ITEM_T  
     WHERE REP_PERIOD_ID = v_rep_period_id;
    Pk01_Syslog.Write_msg('item_t <'||
                          'count='||v_count||', total='||v_total||', '||
                          'recvd='||v_recvd||' >',
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- �������� payment_t
    SELECT COUNT(*), SUM(RECVD), SUM(BALANCE)
      INTO v_count, v_recvd, v_balance
      FROM PAYMENT_T
     WHERE REP_PERIOD_ID = v_rep_period_id;
    Pk01_Syslog.Write_msg('payment_t <'||
                          'count='||v_count||', recvd='||v_recvd||', '||
                          'balance='||v_balance||' >',
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- �������� ���������� �� ������
    SELECT COUNT(*), SUM(TOTAL), SUM(RECVD), SUM(DUE)
      INTO v_count, v_total, v_recvd, v_balance
      FROM BILL_T
     WHERE REP_PERIOD_ID = v_rep_period_id;
    Pk01_Syslog.Write_msg('bill_t <'||
                          'count='||v_count||', total='||v_total||', '||
                          'recvd='||v_recvd||', due='||v_balance||' >',
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--
PROCEDURE Transfer_to_bill
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Transfer_to_bill';
    v_rep_period_id INTEGER := 201312;
    v_ok            INTEGER := 0;
    v_err           INTEGER := 0;
    v_count         INTEGER := 0;
    v_value         NUMBER;
    v_transfer_id   INTEGER;
    v_open_balance  NUMBER;
    v_close_balance NUMBER;
    v_bill_due      NUMBER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start '||v_rep_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� �������� �� �������������� �����, ������������ �������, � ��������
    FOR r_payment IN (
      SELECT P.PAYMENT_ID, P.REP_PERIOD_ID, P.ACCOUNT_ID, P.BALANCE, B.BILL_ID 
        FROM PAYMENT_T P, BILL_T B
       WHERE P.REP_PERIOD_ID = v_rep_period_id
         AND P.REP_PERIOD_ID = B.REP_PERIOD_ID
         AND P.ACCOUNT_ID    = B.ACCOUNT_ID  
    )
    LOOP
       -- ---------------------------------------------------------------- --
       SAVEPOINT X;  -- ����� ���������� ������ ��� �������� �����
       v_count := v_count + 1;
       BEGIN
           v_value := r_payment.balance;
           v_transfer_id := PK10_PAYMENT.Transfer_to_bill(
               p_payment_id    => r_payment.payment_id,   -- ID ������� - ��������� �������
               p_pay_period_id => r_payment.rep_period_id,-- ID ��������� ������� ���� ����������� ������
               p_bill_id       => r_payment.bill_id,      -- ID ������������� �����
               p_rep_period_id => r_payment.rep_period_id,-- ID ��������� ������� �����
               p_notes         => '�������� ��������� �������', -- ���������� � ��������
               p_value         => v_value,        -- ����� ������� ����� ���������, NULL - ������� �����
               p_open_balance  => v_open_balance, -- ����� �� ������� �� ���������� ��������
               p_close_balance => v_close_balance,-- ����� �� ������� ����� ���������� ��������
               p_bill_due      => v_bill_due      -- ���������� ���� �� ����� ����� ��������
           );
           v_ok := v_ok + 1;
       EXCEPTION 
         WHEN OTHERS THEN
           -- ����� ��������� ��� �������� �����
           ROLLBACK TO X;
           Pk01_Syslog.Write_msg('payment_id => '||r_payment.payment_id||', '||
                                 'bill_id => '||r_payment.bill_id, 
                                 c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );
           v_err := v_err + 1;
       END;
        IF MOD(v_count, 10000) = 0 THEN
            Pk01_Syslog.Write_msg('Processed: '||v_count||'-rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
    END LOOP;

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


/*
--=========================================================================--
-- ������� �������� ��������
-- ��������� ������� ����������� PK02_EXPORT_P.Exp_open_balance@MMTDB
--=========================================================================--
PROCEDURE Import_open_balance
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Import_open_balance';
    v_count    INTEGER := 0;
    v_min_period_id   INTEGER;
    v_start_period_id INTEGER;
    --
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������� ������ � ������� �������� ���.����
    -- ��������� ������� ����������� PK02_EXPORT_P.Exp_subs_order@MMTDB
    EXECUTE IMMEDIATE 'TRUNCATE TABLE MMTDB_P_SUBS_PERIOD_INFO_T DROP STORAGE';
    INSERT INTO MMTDB_P_SUBS_PERIOD_INFO_T
    SELECT * FROM MDV_ADM.P_SUBS_PERIOD_INFO_T@MMTDB;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('MMTDB_P_SUBS_PERIOD_INFO_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'MMTDB_P_SUBS_PERIOD_INFO_T');
    --
    INSERT INTO REP_PERIOD_INFO_T(
        REP_PERIOD_ID, ACCOUNT_ID, OPEN_BALANCE, CLOSE_BALANCE, 
        TOTAL, GROSS, RECVD, ADVANCE, LAST_MODIFIED
    )
    SELECT  
        REP_PERIOD_ID, ACCOUNT_ID, OPEN_BALANCE, CLOSE_BALANCE, TOTAL, 
        (ROUND(TOTAL /(1 + 18 / 100),2)) GROSS, 
        RECVD, ADVANCE, LAST_MODIFIED
    FROM 
    (
        SELECT FIRST_PERIOD,
               REP_PERIOD_ID, ACCOUNT_ID, 
               OPEN_BALANCE, CLOSE_BALANCE,
               (BILLED-BILLED_PREV)+(ADJUSTED-ADJUSTED_PREV) TOTAL,
               (RECVD-RECVD_PREV) RECVD,
               0 ADVANCE,
               SYSDATE LAST_MODIFIED
        FROM (
            SELECT B.*,
                   LAG(CLOSE_BALANCE, 1, 0) OVER (PARTITION BY ACCOUNT_ID ORDER BY REP_PERIOD_ID) AS OPEN_BALANCE,
                   LAG(BILLED, 1, 0) OVER (PARTITION BY ACCOUNT_ID ORDER BY REP_PERIOD_ID) AS BILLED_PREV,
                   LAG(ADJUSTED, 1, 0) OVER (PARTITION BY ACCOUNT_ID ORDER BY REP_PERIOD_ID) AS ADJUSTED_PREV,
                   LAG(RECVD, 1, 0) OVER (PARTITION BY ACCOUNT_ID ORDER BY REP_PERIOD_ID) AS RECVD_PREV,
                   MIN(REP_PERIOD_ID) OVER (PARTITION BY ACCOUNT_ID) FIRST_PERIOD
            FROM ( 
                SELECT TO_CHAR(ADD_MONTHS(M.PERIOD,-1),'YYYYMM') REP_PERIOD_ID, A.ACCOUNT_ID,
                       M.OPEN_BALANCE CLOSE_BALANCE, M.BILLED, M.ADJUSTED, M.RECVD, A.ACCOUNT_NO
                  FROM MMTDB_P_SUBS_PERIOD_INFO_T M, ACCOUNT_T A
                 WHERE M.ACCOUNT_NO = A.ACCOUNT_NO(+)
                   AND A.STATUS = 'B'
                   AND A.ACCOUNT_TYPE = 'P'
--                   AND A.ACCOUNT_ID = 1346128
            ) B
            ORDER BY REP_PERIOD_ID
        )
    )
    ORDER BY ACCOUNT_ID, REP_PERIOD_ID;
    v_count := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg('Processed: '||v_count||'-rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Gather_Table_Stat(l_Tab_Name => 'REP_PERIOD_INFO_T');
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;
*/

--==================================================================================
-- ������������� ������ ����� �������� ������
--==================================================================================
PROCEDURE Correct_rateplan
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Correct_rateplan';
    v_rateplan_id   INTEGER; 
    v_rateplan_name RATEPLAN_T.RATEPLAN_NAME%TYPE;
BEGIN
    --
    FOR l_cur IN (
        SELECT E.ORDER_NO, E.PLAN_NAME, R.RATEPLAN_ID 
        FROM SVM_FIZ_EXPORT_ABON_PHONES E, RATEPLAN_T R
        WHERE E.PLAN_NAME = R.RATEPLAN_NAME
        AND R.RATEPLAN_ID < 230
    ) 
    LOOP
        UPDATE ORDER_T
        SET RATEPLAN_ID = l_cur.rateplan_id
        WHERE ORDER_NO = l_cur.order_no;
    END LOOP;
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================
-- ������ ����������� �������� �� �/� �� ��������
--==================================================================================
PROCEDURE Imp_period_payments
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Imp_period_payments';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName);
    --
    INSERT INTO MMTS_PERIOD_PAYMENTS(
        ACCOUNT_NO, RECVD, PERIOD
    )
    SELECT 
        a.account_no,
        NVL(SUM(i.item_total + i.recvd), 0),
        TRUNC(u2d(ebpp.tstamp_val),'MM')
      FROM item_t@MMTDB i, 
        event_bal_impacts_t@MMTDB ebi, 
        event_billing_payment_payord_t@MMTDB ebpp, 
        account_t@MMTDB a
    WHERE a.poid_id0 = i.account_obj_id0
      AND i.poid_id0 = ebi.item_obj_id0
      AND ebi.obj_id0 = ebpp.obj_id0
      AND ebpp.rec_id = 0
      AND ebi.rec_id = 0
      AND i.bill_obj_id0 = 0
      AND i.poid_type = '/item/payment'
      AND a.business_type = 1
      AND a.poid_id0 <> '251460'
      --and a.account_no = 'ACC000024273'
      GROUP BY a.account_no, TRUNC(u2d(ebpp.tstamp_val),'MM');
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||'-rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.Write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================
-- ������ ����������� ���������� �� �/� �� ��������
--==================================================================================
PROCEDURE Imp_period_totals
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Imp_period_totals';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName);
    --
    INSERT INTO MMTS_PERIOD_TOTALS(
        ACCOUNT_NO, TOTAL, PERIOD
    )
    SELECT 
      a.account_no,
      NVL(SUM(b.current_total + b.subords_total), 0),
      TRUNC(u2d(b.end_t)-1,'MM')
     FROM bill_t@MMTDB b, account_t@MMTDB a
    WHERE a.poid_id0 = b.account_obj_id0
      AND a.business_type = 1
      AND a.poid_id0 <> '251460'
      AND b.invoice_obj_id0  <> 0 
      --and a.account_no = 'ACC000024273'
      GROUP BY a.account_no, TRUNC(u2d(b.end_t)-1,'MM');
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||'-rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.Write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================
-- �������������
-- ������ ����������� ������������� �� �/� �� ��������
--==================================================================================
PROCEDURE Imp_period_adjust
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Imp_period_adjust';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName);
    --
    INSERT INTO MMTS_PERIOD_ADJUST(
        ACCOUNT_NO, ADJUST, PERIOD
    )
    SELECT 
      a.account_no,
      NVL(SUM(i.item_total), 0),
      TRUNC(u2d(i.effective_t),'MM')
     FROM item_t@MMTDB i, account_t@MMTDB a
    WHERE a.poid_id0 = i.account_obj_id0
      AND i.bill_obj_id0 = 0
      AND i.poid_type = '/item/adjustment'
      AND a.business_type = 1
      AND a.poid_id0 <> '251460'
      GROUP BY a.account_no, TRUNC(u2d(i.effective_t),'MM');
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||'-rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.Write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================
-- ������ ������������ ������ �� �/� � �� ��������
--==================================================================================
PROCEDURE Imp_bills
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Imp_bills';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName);
    --
    EXECUTE IMMEDIATE 'TRUNCATE TABLE MMTDB_P_BILLS DROP STORAGE';
    Gather_Table_Stat(l_Tab_Name => 'MMTDB_P_BILLS');
    --
    INSERT INTO MMTDB_P_BILLS(
      ACCOUNT_NO,
      BILL_NO,
      TOTAL,
      ADJUSTED,
      RECVD,
      DUE,
      BILL_DATE
    )
    SELECT 
      a.account_no,
      b.bill_no, 
      b.current_total+b.subords_total total,
      b.adjusted,
      b.recvd,
      b.due,
      TRUNC(u2d(b.end_t))-1 bill_date
      FROM bill_t@MMTDB b, account_t@MMTDB a
    WHERE b.account_obj_id0 = a.poid_id0
    AND b.invoice_obj_id0 <> 0
    AND a.business_type = 1
    AND a.poid_id0 <> '251460'
    AND b.current_total+b.subords_total <> 0;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||'-rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.Write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ������ ������� ������������ ������ �� �/� � �� ��������
PROCEDURE Imp_items
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Imp_items';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName);
    --
    EXECUTE IMMEDIATE 'TRUNCATE TABLE MMTDB_P_ITEMS DROP STORAGE';
    Gather_Table_Stat(l_Tab_Name => 'MMTDB_P_ITEMS');
    --
    INSERT INTO MMTDB_P_ITEMS
    SELECT 
        a.account_no,
        b.bill_no,
        TRUNC(u2d(b.end_t))-1 bill_date,
        SUM(i.item_total) item_total,
        SUM(i.adjusted) adjusted,
        SUM(i.recvd) recvd,
        SUM(i.transfered) transfered,
        SUM(i.due) due,
        pc.order_num,
        i.NAME
    FROM bill_t@MMTDB b, account_t@MMTDB a, item_t@MMTDB i,
         profile_t@MMTDB pr,
         profile_contract_info_t@MMTDB pc
    WHERE b.account_obj_id0 = a.poid_id0
      AND b.invoice_obj_id0 <> 0
      AND a.business_type = 1
      AND a.poid_id0 <> '251460'
    --and a.account_no like 'ACC0000242%'
      AND i.ar_bill_obj_id0 = b.poid_id0
      AND b.current_total+b.subords_total <> 0
      AND pc.obj_id0 = pr.poid_id0
      AND pr.account_obj_id0 = i.account_obj_id0
      AND i.item_total+i.adjusted <> 0
    GROUP  BY a.account_no, b.bill_no, TRUNC(u2d(b.end_t))-1, pc.order_num, i.NAME;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||'-rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.Write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================
-- ����� ��������� ���.���
--==================================================================================
PROCEDURE Rollback_persons
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_persons';
    v_rateplan_id   INTEGER; 
    v_rateplan_name RATEPLAN_T.RATEPLAN_NAME%TYPE;
    v_count         INTEGER;
    -- ��������� ��������� - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Bill_t_drop_fk
    IS
        v_prcName       CONSTANT VARCHAR2(30) := 'Bill_t_drop_fk';
    BEGIN 
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ITEM_T DROP CONSTRAINT ITEM_T_BILL_T_FK';
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.INVOICE_ITEM_T DROP CONSTRAINT INVOICE_ITEM_T_BILL_T_FK';
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.BILLINFO_T DROP CONSTRAINT BILLINFO_T_BILL_T_FK';
        COMMIT;
    EXCEPTION WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
    END;

    PROCEDURE Bill_t_add_fk
    IS
        v_prcName       CONSTANT VARCHAR2(30) := 'Bill_t_add_fk';
    BEGIN 
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ITEM_T ADD (
          CONSTRAINT ITEM_T_BILL_T_FK 
          FOREIGN KEY (BILL_ID, REP_PERIOD_ID) 
          REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
          ENABLE VALIDATE )';
          
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.INVOICE_ITEM_T ADD (
          CONSTRAINT INVOICE_ITEM_T_BILL_T_FK 
          FOREIGN KEY (BILL_ID, REP_PERIOD_ID) 
          REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
          ENABLE VALIDATE )';
          
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.BILLINFO_T ADD (
          CONSTRAINT BILLINFO_T_BILL_T_FK 
          FOREIGN KEY (BILL_ID, PERIOD_ID) 
          REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
          ENABLE VALIDATE)';
    EXCEPTION WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
    END;
    
    PROCEDURE Account_t_drop_fk
    IS
        v_prcName       CONSTANT VARCHAR2(30) := 'Account_t_drop_fk';
    BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.REP_PERIOD_INFO_T DROP CONSTRAINT REP_PERIOD_INFO_T_ACC_T_FK';
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ACCOUNT_T DROP CONSTRAINT ACCOUNT_T_ACCOUNT_T_FK';
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ORDER_T DROP  CONSTRAINT ORDER_T_ACCOUNT_T_FK';
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.BILL_T DROP CONSTRAINT BILL_T_ACCOUNT_T_FK';
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ACCOUNT_PROFILE_T DROP CONSTRAINT ACCOUNT_PROFILE_ACCOUNT_T_FK';
    EXCEPTION WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
    END;

    PROCEDURE Account_t_add_fk
    IS
        v_prcName       CONSTANT VARCHAR2(30) := 'Account_t_add_fk';
    BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.REP_PERIOD_INFO_T ADD (
            CONSTRAINT REP_PERIOD_INFO_T_ACC_T_FK 
            FOREIGN KEY (ACCOUNT_ID) 
            REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
            ENABLE VALIDATE)';
  
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ACCOUNT_T ADD (
            CONSTRAINT ACCOUNT_T_ACCOUNT_T_FK 
            FOREIGN KEY (PARENT_ID) 
            REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
            ENABLE VALIDATE)';
  
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ORDER_T ADD (
            CONSTRAINT ORDER_T_ACCOUNT_T_FK 
            FOREIGN KEY (ACCOUNT_ID) 
            REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
            ENABLE VALIDATE)';
  
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.BILL_T ADD (
            CONSTRAINT BILL_T_ACCOUNT_T_FK 
            FOREIGN KEY (ACCOUNT_ID) 
            REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
            ENABLE VALIDATE)';
  
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ACCOUNT_PROFILE_T ADD (
            CONSTRAINT ACCOUNT_PROFILE_ACCOUNT_T_FK 
            FOREIGN KEY (ACCOUNT_ID) 
            REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
            ENABLE VALIDATE)';

    EXCEPTION WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
    END;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ��������� ������� ������ ��.��� (�� �������)
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK200_ACC_BILL_P DROP STORAGE';
    INSERT INTO PK200_ACC_BILL_P P
    SELECT A.ACCOUNT_ID, AP.CONTRACT_ID, B.BILL_ID, B.REP_PERIOD_ID 
      FROM ACCOUNT_T A, BILL_T B, ACCOUNT_PROFILE_T AP
     WHERE A.ACCOUNT_TYPE = PK00_CONST.c_ACC_TYPE_J
       AND A.ACCOUNT_ID = B.ACCOUNT_ID
       AND A.ACCOUNT_ID = AP.ACCOUNT_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK200_ACC_BILL_P: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PK200_ACC_BILL_P'); 
    COMMIT;   
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- ���������� ����������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ����������� ���������� �� �����
    DELETE FROM DETAIL_MMTS_T D
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P  WHERE P.BILL_ID = D.BILL_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('DETAIL_MMTS_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'DETAIL_MMTS_T');
    COMMIT;
    
    -- ���������� �������� �������� ��������
    DELETE FROM PAY_TRANSFER_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAY_TRANSFER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PAY_TRANSFER_T');
    COMMIT;
    
    -- ���������� �������
    DELETE FROM PAYMENT_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAYMENT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info ); 
    Gather_Table_Stat(l_Tab_Name => 'PAYMENT_T');
    COMMIT;
    
    -- ���������� ������ ������-������
    UPDATE ITEM_T I SET INV_ITEM_ID = NULL
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P  WHERE P.BILL_ID = I.BILL_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    
    --
    DELETE FROM INVOICE_ITEM_T II
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P  WHERE P.BILL_ID = II.BILL_ID
    ); 
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'INVOICE_ITEM_T');
    COMMIT;
    
    -- ���������� ������ ���������� �� ������
    DELETE FROM ITEM_T I
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P  WHERE P.BILL_ID = I.BILL_ID
    ); 
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ITEM_T');
    COMMIT;
    
    -- ������� ��������� ������
    DELETE FROM BILLINFO_T BI
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P  WHERE P.ACCOUNT_ID = BI.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLINFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILLINFO_T');
    COMMIT;
    
    -- ������� �����
    Bill_t_drop_fk;
    DELETE FROM BILL_T B
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P  WHERE P.ACCOUNT_ID = B.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILL_T');
    Bill_t_add_fk;
    COMMIT;
    
    -- ������� ������� �� ��������
    DELETE FROM REP_PERIOD_INFO_T BI
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P  WHERE P.ACCOUNT_ID = BI.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('REP_PERIOD_INFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'REP_PERIOD_INFO_T');
    COMMIT;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- ������� �������� �� ������
    DELETE FROM ORDER_PHONES_T OP
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P, ORDER_T O  
         WHERE OP.ORDER_ID = O.ORDER_ID
           AND P.ACCOUNT_ID= O.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_PHONES_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ORDER_PHONES_T');
    COMMIT;
    
    -- ������� ���������� �������
    DELETE FROM ORDER_LOCK_T OL
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P, ORDER_T O  
         WHERE OL.ORDER_ID = O.ORDER_ID
           AND P.ACCOUNT_ID= O.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_LOCK_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ORDER_LOCK_T');
    COMMIT;
    
    -- ������� ��������� �������
    DELETE FROM ORDER_BODY_T OB
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P, ORDER_T O  
         WHERE OB.ORDER_ID = O.ORDER_ID
           AND P.ACCOUNT_ID= O.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ORDER_BODY_T');
    COMMIT;
    
    -- ������� ��������� �������
    DELETE FROM ORDER_T O
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P  WHERE P.ACCOUNT_ID = O.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ORDER_T');
    COMMIT;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� �������
    DELETE FROM ACCOUNT_PROFILE_T AP
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P  WHERE P.ACCOUNT_ID = AP.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_PROFILE_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    Gather_Table_Stat(l_Tab_Name => 'ACCOUNT_PROFILE_T');
    COMMIT;
    
    -- ������� ��������� �������
    DELETE FROM SUBSCRIBER_DOC_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SUBSCRIBER_DOC_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'SUBSCRIBER_DOC_T');
    COMMIT;
    
    -- ������� �������
    DELETE FROM SUBSCRIBER_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SUBSCRIBER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'SUBSCRIBER_T');
    COMMIT;
    
    -- ������� �������� �������
    DELETE FROM CONTRACT_T C
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P  WHERE C.CONTRACT_ID =  P.CONTRACT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CONTRACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    Gather_Table_Stat(l_Tab_Name => 'CONTRACT_T');
    COMMIT;
    
    -- ������� ������ �������� �����
    DELETE FROM ACCOUNT_CONTACT_T AC
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P  WHERE P.ACCOUNT_ID = AC.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_CONTACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ACCOUNT_CONTACT_T');
    COMMIT;
    
    -- ������� ������� �����
    Account_t_drop_fk;
    DELETE FROM ACCOUNT_T A
     WHERE NOT EXISTS (
        SELECT * FROM PK200_ACC_BILL_P P  WHERE P.ACCOUNT_ID = A.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ACCOUNT_T');
    Account_t_add_fk;
    COMMIT;
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--=========================================================================--
-- � � � � � � � �    � � � � � � �   � �  01.01.2014
--=========================================================================--
--
-- ������� ����� � ������� � ��� ��� �������� ��������
--
PROCEDURE Create_open_bill
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Create_open_bill';
    v_count         INTEGER;
    v_bill_no       BILL_T.BILL_NO%TYPE := NULL;
    v_rep_period_id INTEGER := 201312;
    v_date_from     DATE    := TO_DATE('01.01.2000 00:00:00','dd.mm.yyyy hh24:mi:ss');
    v_period_to     DATE    := TO_DATE('31.12.2013 23:59:59','dd.mm.yyyy hh24:mi:ss');
    v_item_id       INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    FOR r_acc IN (
        SELECT TO_NUMBER(SUBSTR(ACCOUNT_NO,4)) BILL_ID, 
               CASE
                 WHEN RPI.CLOSE_BALANCE > 0 THEN RPI.CLOSE_BALANCE ELSE 0  
               END TOTAL,
               CASE
                 WHEN RPI.CLOSE_BALANCE < 0 THEN -RPI.CLOSE_BALANCE ELSE 0  
               END RECVD,
               RPI.CLOSE_BALANCE,
               RPI.REP_PERIOD_ID, 
               A.ACCOUNT_ID, A.ACCOUNT_NO, A.ACCOUNT_TYPE, A.BALANCE, A.CURRENCY_ID
         FROM ACCOUNT_T A, REP_PERIOD_INFO_T RPI 
         WHERE ACCOUNT_TYPE != 'J'
           AND A.ACCOUNT_ID = RPI.ACCOUNT_ID
           AND RPI.REP_PERIOD_ID = 201312
    )
    LOOP
        v_count := v_count + 1;
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- ������� ����� �� ������ 01.12.2013 (�������������� ������),
        -- ������� ����� � ���������������� �������� ��� �������� ���������� 
        -- � ������ �������� �� ������ ������� 01.01.2014 (����� ���)
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- �������� ����� ����� ��� �������
        v_bill_no := Pk07_Bill.Make_bill_no( r_acc.account_id, v_rep_period_id );
        -- ������� ���� ��� �������
        INSERT INTO BILL_T (
            ACCOUNT_ID,      -- ID �������� �����
            BILL_ID,         -- ID �������� �����
            REP_PERIOD_ID,   -- ID ���������� ������� YYYYMM
            BILL_TYPE,       -- ��� �����
            BILL_NO,         -- ����� �����
            CURRENCY_ID,     -- ID ������ �����
            BILL_DATE,       -- ���� ����� (������������ �������)
            BILL_STATUS,     -- ��������� �����
            TOTAL,           -- ��������� ������ �� �����������
            RECVD,           -- ��������� ������ �� ��������
            DUE              -- �������� ������ �� �������� �����
        )VALUES(
            r_acc.account_id,
            r_acc.bill_id,
            v_rep_period_id,
            PK00_CONST.c_BILL_TYPE_REC,
            v_bill_no,
            r_acc.currency_id,
            v_period_to,
            PK00_CONST.c_BILL_STATE_CLOSED,
            r_acc.total,
            r_acc.recvd,
            r_acc.close_balance
        );
        -- ������� �������������� ������� ����� - ���� �������� ������ �� �������
        IF r_acc.close_balance <> 0 THEN
            -- id - ��������� �� �� ������������������
            v_item_id := v_item_id + 1;
            -- ������� ������ ������� ������������� �����
            INSERT INTO ITEM_T (
               BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE,  
               ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE,
               ITEM_TOTAL, ADJUSTED, RECVD, 
               DATE_FROM, DATE_TO,
               INV_ITEM_ID
            )VALUES(
               r_acc.bill_id, v_rep_period_id, v_item_id, PK00_CONST.c_ITEM_TYPE_ADJUST,
               NULL, NULL, NULL, NULL,
               r_acc.total, 0, r_acc.recvd,
               v_date_from, v_period_to,
               NULL 
            );
        END IF;
        -- ������� �����������
        IF MOD(v_count, 1000) = 0 THEN
            Pk01_Syslog.Write_msg('���������� '||v_count||' �����', c_PkgName||'.'||v_prcName );
        END IF;
    END LOOP;
    --
    Pk01_Syslog.Write_msg('���������� '||v_count||' �����', c_PkgName||'.'||v_prcName );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--
-- ������� ��������� ������� ��� �������� ��������
--
PROCEDURE Create_open_payment
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Create_open_payment';
    v_count         INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    FOR r_pay IN (
        SELECT REP_PERIOD_ID, ACCOUNT_ID, RECVD, 
               TO_DATE('31.12.2013 23:59:59','dd.mm.yyyy hh24:mi:ss') PAY_DATE  
          FROM REP_PERIOD_INFO_T -- 32 217
         WHERE RECVD != 0
           AND REP_PERIOD_ID = 201312  
    )
    LOOP
        v_count := v_count + 1;
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- ������� ��������� ������� �� 31.12.2013 23:59:59 (�������������� ������),
        -- ������� ����� � ��� ������������� �������
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        INSERT INTO PAYMENT_T (
            PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE,
            PAYMENT_DATE, ACCOUNT_ID, RECVD,
            ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED,
            DATE_FROM, DATE_TO,
            PAYSYSTEM_ID, DOC_ID,
            STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED,
            CREATED_BY, NOTES, 
            PREV_PAYMENT_ID, PREV_PERIOD_ID
        )VALUES(
            v_count, r_pay.rep_period_id, PK00_CONST.c_PAY_TYPE_OPEN,
            r_pay.pay_date, r_pay.account_id, r_pay.recvd,
            r_pay.recvd, r_pay.pay_date, r_pay.recvd, 0,
            NULL, NULL,
            PK00_CONST.c_PS_KTTK_201401, r_pay.account_id,
            PK00_CONST.c_PAY_STATE_OPEN, r_pay.pay_date, r_pay.pay_date, SYSDATE,
            NULL, NULL, 
            NULL, NULL
        );
    END LOOP;
    --
    Pk01_Syslog.Write_msg('���������� '||v_count||' �����', c_PkgName||'.'||v_prcName );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

END PK200_IMPORT;
/
