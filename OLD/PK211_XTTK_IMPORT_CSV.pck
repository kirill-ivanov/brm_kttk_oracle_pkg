CREATE OR REPLACE PACKAGE PK211_XTTK_IMPORT_CSV
IS
    --
    -- ����� ��� �������� �������� ����� ����������
    -- ��������������� � ���� ����� *.csv �� ����
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK211_XTTK_IMPORT_CSV';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- =====================================================================
    -- ������� � ������� ��������� ��������, ��� �������� ����� ��������
    c_BILLING_XTTK        CONSTANT INTEGER := 2008; -- ������� �������� ����� ���������� �� ����
    c_LOAD_CODE_START     CONSTANT INTEGER := 0;    -- ��������
    c_LOAD_CODE_PROGRESS  CONSTANT INTEGER := 1;    -- ������
    c_LOAD_CODE_OK        CONSTANT INTEGER := 2;    -- ��
    c_LOAD_CODE_ERR       CONSTANT INTEGER :=-1;    -- ������
    c_LOAD_CODE_DBL       CONSTANT INTEGER :=-2;    -- ������ ��� ���� � BRM

    -- ������ �������� ����������
    c_DLV_METHOD_AP CONSTANT INTEGER := 6512;   -- ����������

    c_CONTRACTOR_ID       CONSTANT INTEGER := 1;

    -- ������������ (xttk_id)
    c_mbl CONSTANT INTEGER := 1524374; -- ������������ ������
    c_mvv CONSTANT INTEGER := 1524395; -- ������������ ��������������
    c_mdv CONSTANT INTEGER := 1524387; -- ������������ ������� ������
    c_mkz CONSTANT INTEGER := 1524389; -- ������������ ������
    c_mkl CONSTANT INTEGER := 1524391; -- ������������ �����������
    c_msp CONSTANT INTEGER := 1524399; -- ������������ ����ʻ
    c_msh CONSTANT INTEGER := 1524393; -- ������������ �������
    c_msv CONSTANT INTEGER := 1524380; -- ������������ �����
    c_msz CONSTANT INTEGER := 1520993; -- ������������ ������-�����
    c_msr CONSTANT INTEGER := 1524376; -- ������������ ��������������
    c_mur CONSTANT INTEGER := 1524365; -- ������������ ����
    c_mct CONSTANT INTEGER := 1524378; -- ������������ �����
    c_mch CONSTANT INTEGER := 1524397; -- ������������ ����
    c_muv CONSTANT INTEGER := 1524383; -- ������������ ���-������
    c_muu CONSTANT INTEGER := 1524385; -- ������������ ����� ����

    -- ������ ������������ ��������� (agent_id)
    c_ag_mbl CONSTANT INTEGER := 297; -- ������������ ������ (��)
    c_ag_mvv CONSTANT INTEGER := 298; -- ������������ �������������� (��)
    c_ag_mdv CONSTANT INTEGER := 299; -- ������������ ������� ������ (��)
    c_ag_mkz CONSTANT INTEGER := 300; -- ������������ ������ (��)
    c_ag_mkl CONSTANT INTEGER := 301; -- ������������ ����������� (��)
    c_ag_msp CONSTANT INTEGER := 302; -- ������������ ����ʻ (��)
    c_ag_msh CONSTANT INTEGER := 303; -- ������������ ������� (��)
    c_ag_msv CONSTANT INTEGER := 304; -- ������������ ����� (��)
    c_ag_msz CONSTANT INTEGER := 305; -- ������������ ������-����� (��)
    c_ag_msr CONSTANT INTEGER := 306; -- ������������ �������������� (��)
    c_ag_mur CONSTANT INTEGER := 307; -- ������������ ���� (��)
    c_ag_mct CONSTANT INTEGER := 308; -- ������������ ����� (��)
    c_ag_mch CONSTANT INTEGER := 309; -- ������������ ���� (��)
    c_ag_muv CONSTANT INTEGER := 310; -- ������������ ���-������ (��)
    c_ag_muu CONSTANT INTEGER := 311; -- ������������ ����� ���� (��)

    -- ����� �������� (bank_id)
    c_bank_mbl CONSTANT INTEGER := 1524375; -- ������������ ������
    c_bank_mvv CONSTANT INTEGER := 1524396; -- ������������ ��������������
    c_bank_mdv CONSTANT INTEGER := 1524388; -- ������������ ������� ������
    c_bank_mkz CONSTANT INTEGER := 1524390; -- ������������ ������
    c_bank_mkl CONSTANT INTEGER := 1524392; -- ������������ �����������
    c_bank_msp CONSTANT INTEGER := 1524400; -- ������������ ����ʻ
    c_bank_msh CONSTANT INTEGER := 1524394; -- ������������ �������
    c_bank_msv CONSTANT INTEGER := 1524381; -- ������������ �����
    c_bank_msz CONSTANT INTEGER := 10;      -- ������������ ������-�����
    c_bank_msr CONSTANT INTEGER := 1524377; -- ������������ ��������������
    c_bank_mur CONSTANT INTEGER := 1524367; -- ������������ ����
    c_bank_mct CONSTANT INTEGER := 1524379; -- ������������ �����
    c_bank_mch CONSTANT INTEGER := 1524398; -- ������������ ����
    c_bank_muv CONSTANT INTEGER := 1524384; -- ������������ ���-������
    c_bank_muu CONSTANT INTEGER := 1524386; -- ������������ ����� ����

    -- �������� ������ ��������������� ����������    
    CURSOR c_FILE_CSV IS (
       SELECT 
          ERP_CODE,         -- ��� �����    
          CLIENT,           -- ������ (��. ��������)    
          CONTRACT_NO,      -- �������    
          CONTRACT_DATE,    -- ���� ��������    
          ACCOUNT_NO,       -- ����� �/�    
          INN,              -- ���    
          KPP,              -- ���    
          -- +��. �����
          JUR_ZIP,          -- ������    
          JUR_REGION,       -- �������/������    
          JUR_CITY,         -- �����    
          JUR_ADDRESS,      -- �����                
          -- +����� ��� �������� ������                                                            
          DLV_ZIP,          -- ������    
          DLV_REGION,       -- �������/������    
          DLV_CITY,         -- �����    
          DLV_ADDRESS,      -- �����    
          ORDER_NO,         -- ����� ������    
          ORDER_DATE,       -- ���� ������    
          SERVICE,          -- ������ �� �������-�������� ��� (�� ������� ����� ��)    
          SERVICE_ALIAS,    -- �������� ������ �� ��������    
          POINT_SRC,        -- ����� ����������� 1    
          POINT_DST,        -- ����� ����������� 2    
          SPEED,            -- ��������     
          ABP_VALUE,        -- ����. �����     
          QUANTITY,         -- ���-��    
          MANAGER,          -- ��������    
          NOTES,            -- �����������    
          ACCOUNT_ID, 
          PROFILE_ID, 
          CONTRACT_ID, 
          CLIENT_ID,
          CUSTOMER_ID,
          CONTRACTOR_ID,
          CONTRACTOR_BANK_ID,
          XTTK_ID,
          AGENT_ID,
          JUR_ADDRESS_ID,
          DLV_ADDRESS_ID,
          ORDER_ID, 
          SERVICE_ID,
          ORDER_BODY_ID,
          ORDER_BODY_2_ID,
          MANAGER_ID,
          ABP_NUMBER,
          REGION,
          LOAD_STATUS,
          LOAD_CODE
         FROM PK211_XTTK_IMPORT_T 
        WHERE LOAD_CODE = c_LOAD_CODE_PROGRESS
    )FOR UPDATE;
    
    --============================================================================================
    -- ����� ������� ������������ ��������� �/� �� c_BILLING_NPL -> Pk00_Const.c_BILLING_OLD
    --============================================================================================
    PROCEDURE Change_billing_id;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������ �� ��������� ������� PK211_XTTK_IMPORT_TMP
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Import_data(p_seller_id IN INTEGER);

    -- �������� ���������� � ������� ������ ��������
    PROCEDURE Load_data;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ������ �� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ �������������� XTTK-�� 
    PROCEDURE Import_mvv;
    
END PK211_XTTK_IMPORT_CSV;
/
CREATE OR REPLACE PACKAGE BODY PK211_XTTK_IMPORT_CSV
IS

--============================================================================================
-- ����� ������� ������������ ��������� �/� �� 2007 -> ...
--============================================================================================
PROCEDURE Change_billing_id
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Check_data';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE ACCOUNT_T A
       SET A.BILLING_ID = 2003
     WHERE A.BILLING_ID = 2007;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T.BILLING_ID: '||v_count||' rows c_BILLING_NPL -> c_BILLING_OLD', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������ �� ��������� ������� PK211_XTTK_IMPORT_TMP
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Import_data(p_seller_id IN INTEGER)
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_data';
    v_count          INTEGER := 0;
    v_contractor_id  INTEGER;
    v_xttk_id        INTEGER;
    v_agent_id       INTEGER;
    v_bank_id        INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, seller_id=' ||p_seller_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  
                               
    -- ==========================================================================--
    -- ����� ��������� � ��������
    SELECT CONTRACTOR_ID, XTTK_ID, AGENT_ID, BANK_ID --, ERP_CODE, CONTRACTOR
      INTO v_contractor_id, v_xttk_id, v_agent_id, v_bank_id
      FROM (
        SELECT CT.CONTRACTOR_ID, CT.XTTK_ID, CA.CONTRACTOR_ID AGENT_ID, CB.BANK_ID, CT.ERP_CODE, CT.CONTRACTOR,
               ROW_NUMBER() OVER (PARTITION BY CT.CONTRACTOR_ID ORDER BY CB.BANK_ID) RN  
          FROM CONTRACTOR_T CT, CONTRACTOR_BANK_T CB, CONTRACTOR_T CA 
         WHERE CT.CONTRACTOR_TYPE  = 'SELLER'
           AND CT.CONTRACTOR_ID = CB.CONTRACTOR_ID
           AND CA.PARENT_ID = CT.CONTRACTOR_ID
           AND CA.CONTRACTOR LIKE '%(��)'
           AND CT.CONTRACTOR_ID = p_seller_id
    )
    WHERE RN = 1
    ORDER BY CONTRACTOR;
    
    Pk01_Syslog.Write_msg('contractor_id=' ||v_contractor_id||
                          ', xttk_id='  ||v_xttk_id||
                          ', agent_id=' ||v_agent_id||
                          ', bank_id='  ||v_bank_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
                               
/*
    -- ��������� ������ �� ��������� �������
    INSERT INTO PK211_XTTK_IMPORT_T (
        ERP_CODE,CLIENT,    
        CONTRACT_NO, CONTRACT_DATE,    
        ACCOUNT_NO,    
        INN,KPP,    
        JUR_ZIP, JUR_REGION, JUR_CITY, JUR_ADDRESS,                
        DLV_ZIP, DLV_REGION, DLV_CITY, DLV_ADDRESS,    
        ORDER_NO, ORDER_DATE,SERVICE,SERVICE_ALIAS,
        POINT_SRC,POINT_DST,SPEED, ABP_VALUE,QUANTITY,    
        MANAGER, NOTES, 
        CONTRACTOR_ID, XTTK_ID, AGENT_ID, CONTRACTOR_BANK_ID, LOAD_CODE
    )
    SELECT 
        ERP_CODE,CLIENT,    
        CONTRACT_NO, TO_DATE(CONTRACT_DATE,'dd.mm.yyyy') CONTRACT_DATE,    
        ACCOUNT_NO,    
        INN,KPP,    
        JUR_ZIP, JUR_REGION, JUR_CITY, JUR_ADDRESS,
        DLV_ZIP, DLV_REGION, DLV_CITY, DLV_ADDRESS,
        ORDER_NO, ORDER_DATE,SERVICE,SERVICE_ALIAS,
        POINT_SRC,POINT_DST,SPEED, 
        ABP_VALUE,
        QUANTITY,
        MANAGER, NOTES,
        v_contractor_id, v_xttk_id, v_agent_id, v_bank_id, c_LOAD_CODE_START
    FROM PK211_XTTK_IMPORT_TMP;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
*/

    UPDATE PK211_XTTK_IMPORT_T X
       SET X.CONTRACTOR_ID      = v_contractor_id,
           X.CONTRACTOR_BANK_ID = v_bank_id,
           X.XTTK_ID            = v_xttk_id, 
           X.AGENT_ID           = v_agent_id, 
           X.LOAD_CODE          = c_LOAD_CODE_START
     WHERE X.LOAD_CODE IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ��������� � �����, � ����� ����� ���� ��� ������
    UPDATE PK211_XTTK_IMPORT_T 
       SET ABP_NUMBER = TO_NUMBER(REPLACE(REPLACE(RTRIM(ABP_VALUE,'�. '),',','.'),' ',''))
     WHERE LTRIM(RTRIM(ABP_VALUE,'�. '),'1234567890., ') IS NULL 
       AND INSTR(ABP_VALUE,',',1,2) = 0
       AND ABP_NUMBER IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T.ABP_NUMBER '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ������
    UPDATE PK211_XTTK_IMPORT_T X
       SET X.LOAD_CODE = c_LOAD_CODE_ERR, 
           X.LOAD_STATUS = '�� �������� �������� � ���� ABP_VALUE'
     WHERE ABP_NUMBER IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T.ABP_NUMBER '||v_count||' rows error', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ����������� �� CONTRACT_NO
    MERGE INTO PK211_XTTK_IMPORT_T X
    USING (
        SELECT C.CONTRACT_ID, C.CONTRACT_NO 
          FROM CONTRACT_T C, PK211_XTTK_IMPORT_T X
         WHERE X.CONTRACT_NO = C.CONTRACT_NO 
           AND X.LOAD_CODE   = c_LOAD_CODE_START 
           AND X.CONTRACTOR_ID = v_contractor_id
    ) C
    ON(
       X.CONTRACT_NO = C.CONTRACT_NO
    )
    WHEN MATCHED THEN UPDATE SET X.CONTRACT_ID = C.CONTRACT_ID, 
                                 X.LOAD_CODE   = c_LOAD_CODE_DBL,
                                 X.LOAD_STATUS = '��������� ������� ���������';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T.CONTRACT_NO '||v_count||' rows exists', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ����������� �� ACCOUNT_NO
    MERGE INTO PK211_XTTK_IMPORT_T X
    USING (
        SELECT A.ACCOUNT_ID, A.ACCOUNT_NO 
          FROM ACCOUNT_T A, PK211_XTTK_IMPORT_T X
         WHERE X.ACCOUNT_NO  = A.ACCOUNT_NO 
           AND X.LOAD_CODE   = c_LOAD_CODE_START
           AND X.CONTRACTOR_ID = v_contractor_id 
    ) A
    ON(
        X.ACCOUNT_NO  = A.ACCOUNT_NO
    )
    WHEN MATCHED THEN UPDATE SET X.ACCOUNT_ID  = A.ACCOUNT_ID, 
                                 X.LOAD_CODE   = c_LOAD_CODE_DBL,
                                 X.LOAD_STATUS = '��������� ������� �/�';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T.ACCOUNT_NO '||v_count||' rows exists', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ����������� �� ORDER_NO
    MERGE INTO PK211_XTTK_IMPORT_T X
    USING (
        SELECT O.ORDER_ID, O.ORDER_NO 
          FROM ORDER_T O, PK211_XTTK_IMPORT_T X
         WHERE X.ORDER_NO  = O.ORDER_NO
           AND X.LOAD_CODE = c_LOAD_CODE_START
           AND X.CONTRACTOR_ID = v_contractor_id
    ) O
    ON(
        X.ORDER_NO  = O.ORDER_NO
    )
    WHEN MATCHED THEN UPDATE SET X.ORDER_ID = O.ORDER_ID, 
                                 X.LOAD_CODE   = c_LOAD_CODE_DBL,
                                 X.LOAD_STATUS = '��������� ������� �������';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T.ORDER_NO '||v_count||' rows exists', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE PK211_XTTK_IMPORT_T X SET X.LOAD_CODE = c_LOAD_CODE_PROGRESS
     WHERE X.LOAD_CODE = c_LOAD_CODE_START
       AND X.CONTRACTOR_ID = v_contractor_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. PK211_XTTK_IMPORT_T '||v_count||' rows ok', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������ ������ �� ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������������ �������������� XTTK-�� 
PROCEDURE Import_mvv IS
BEGIN
    Import_data(p_seller_id => c_mvv);
END;



--============================================================================================
-- �������� ������
--============================================================================================
PROCEDURE Load_data
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Load_data';
    v_count           INTEGER := 0;
    v_error           INTEGER := 0;
    v_contract_id     INTEGER;
    v_client_id       INTEGER;
    v_customer_id     INTEGER;
    v_profile_id      INTEGER;
    v_account_id      INTEGER;
    v_account_no      ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_jur_address_id  INTEGER;
    v_dlv_address_id  INTEGER;
    v_order_id        INTEGER;
    v_rec_ob_id       INTEGER;
    v_service_id      INTEGER;
    v_manager_id      INTEGER;    
    v_m_last_name     MANAGER_T.LAST_NAME%TYPE;
    v_m_first_name    MANAGER_T.FIRST_NAME%TYPE;
    v_m_middle_name   MANAGER_T.MIDDLE_NAME%TYPE;
    v_load_status     VARCHAR2(1000);
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR r_abn IN c_FILE_CSV LOOP
      
      BEGIN
        -- -------------------------------------------------------- --
        -- ������� �/�
        -- -------------------------------------------------------- --
        SELECT MIN(ACCOUNT_ID), MIN(ACCOUNT_NO), COUNT(*) 
          INTO v_account_id, v_account_no,  v_count
          FROM ACCOUNT_T
         WHERE EXTERNAL_NO = r_abn.ACCOUNT_NO
        ;
        
        IF v_account_id IS NULL THEN
            -- ����� �/� ������� � ����������� �������
            v_account_no := 'XJ'||LPAD(SQ_ACCOUNT_NO.NEXTVAL,6,'0');
            
            -- ������� �/�
            v_account_id := Pk05_Account.New_account(
                         p_account_no    => v_account_no,
                         p_account_type  => Pk00_Const.c_ACC_TYPE_J,
                         p_currency_id   => Pk00_Const.c_CURRENCY_RUB,
                         p_status        => 'NEW', --Pk00_Const.c_ACC_STATUS_BILL,
                         p_parent_id     => NULL,
                         p_notes         => 'ACCOUNT_NO='||r_abn.ACCOUNT_NO||' ������������� �� XTTK'|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
                     );
             
            -- ����������� ������� ������������� �/� (����� ��������)
            UPDATE ACCOUNT_T
               SET EXTERNAL_NO = r_abn.ACCOUNT_NO
             WHERE ACCOUNT_ID  = v_account_id
               AND LTRIM(RTRIM(r_abn.ACCOUNT_NO,'�. '),'1234567890., ') IS NULL;

            -- ����������� �������������� �������� XTTK
            Pk05_Account.Set_billing(
                         p_account_id => v_account_id,
                         p_billing_id => c_BILLING_XTTK
                     );
                 
            -- ������� ��������� ������ � ������ �������� �����
            Pk07_Bill.New_billinfo (
                         p_account_id    => v_account_id,   -- ID �������� �����
                         p_currency_id   => Pk00_Const.c_CURRENCY_RUB,  -- ID ������ �����
                         p_delivery_id   => c_DLV_METHOD_AP,-- ID ������� �������� �����
                         p_days_for_payment => 30           -- ���-�� ���� �� ������ �����
                     );

        END IF;
        -- ��������� ID �/�
        UPDATE PK211_XTTK_IMPORT_T
           SET ACCOUNT_ID = v_account_id
         WHERE CURRENT OF c_FILE_CSV;
    
        -- -------------------------------------------------------- --
        -- ������� ������� ��������
        -- -------------------------------------------------------- --
        SELECT MIN(CLIENT_ID), COUNT(*) 
          INTO v_client_id, v_count
          FROM CLIENT_T CL
         WHERE CL.CLIENT_NAME = r_abn.CLIENT;
        IF v_client_id IS NULL THEN
           v_client_id := PK11_CLIENT.New_client(r_abn.CLIENT);
        END IF;
        -- ��������� ID
        UPDATE PK211_XTTK_IMPORT_T
           SET CLIENT_ID = v_client_id
         WHERE CURRENT OF c_FILE_CSV;
        
        -- -------------------------------------------------------- --
        -- ������� sale-�������� ��������
        -- -------------------------------------------------------- --
        v_m_last_name   := SUBSTR(r_abn.MANAGER, 1, INSTR(r_abn.MANAGER,' ',1)-1);
        v_m_first_name  := SUBSTR(r_abn.MANAGER, INSTR(r_abn.MANAGER,' ',1)+1, 1)||'.';
        v_m_middle_name := SUBSTR(r_abn.MANAGER, INSTR(r_abn.MANAGER,'.',1)+1, 1)||'.';

        SELECT MIN(MANAGER_ID), COUNT(*) 
          INTO v_manager_id, v_count
          FROM MANAGER_T M
         WHERE M.CONTRACTOR_ID = r_abn.XTTK_ID
           AND M.LAST_NAME   = v_m_last_name
           AND M.FIRST_NAME  = v_m_last_name
           AND M.MIDDLE_NAME = v_m_middle_name
        ;

        IF v_manager_id IS NULL THEN
           v_manager_id := PK15_MANAGER.New_manager(
               p_contractor_id    => r_abn.XTTK_ID,
               p_department       => NULL,
               p_position         => NULL, 
               p_last_name        => v_m_last_name,   -- �������
               p_first_name       => v_m_first_name,  -- ��� 
               p_middle_name      => v_m_middle_name, -- ��������
               p_phones           => NULL,
               p_email            => NULL,
               p_date_from        => TO_DATE('01.01.2000','dd.mm.yyyy'),
               p_date_to          => NULL
           );
        END IF;
        -- ��������� ID
        UPDATE PK211_XTTK_IMPORT_T
           SET MANAGER_ID = v_manager_id
         WHERE CURRENT OF c_FILE_CSV;

        -- -------------------------------------------------------- --
        -- ������� �������
        -- -------------------------------------------------------- --
        SELECT MIN(CONTRACT_ID), COUNT(*) 
          INTO v_contract_id, v_count
          FROM CONTRACT_T C
         WHERE C.CONTRACT_NO = r_abn.CONTRACT_NO
        ;
        IF v_contract_id IS NULL THEN
            v_contract_id := Pk12_Contract.Open_contract(
               p_contract_no => r_abn.CONTRACT_NO, 
               p_date_from   => r_abn.CONTRACT_DATE,
               p_date_to     => Pk00_Const.c_DATE_MAX,
               p_client_id   => v_client_id,
               p_manager_id  => v_manager_id
            );
        END IF;
        -- ��������� ID
        UPDATE PK211_XTTK_IMPORT_T
           SET CONTRACT_ID = v_contract_id
         WHERE CURRENT OF c_FILE_CSV;
    
        -- -------------------------------------------------------- --
        -- ������� ����������
        -- -------------------------------------------------------- --
        SELECT MIN(CUSTOMER_ID), COUNT(*) 
          INTO v_customer_id, v_count
          FROM CUSTOMER_T CS
         WHERE CS.ERP_CODE = r_abn.ERP_CODE
           AND CS.INN      = r_abn.INN
           AND CS.KPP      = r_abn.KPP
        ;
        IF v_customer_id IS NULL THEN
           v_customer_id := Pk13_Customer.New_customer(
               p_erp_code    => r_abn.ERP_CODE,
               p_inn         => r_abn.INN,
               p_kpp         => r_abn.KPP, 
               p_name        => r_abn.CLIENT,
               p_short_name  => r_abn.CLIENT,
               p_notes       => '������������� �� XTTK '||TO_CHAR(SYSDATE,'dd.mm.yyyy')
           );
        END IF;
        -- ��������� ID
        UPDATE PK211_XTTK_IMPORT_T
           SET CUSTOMER_ID = v_customer_id
         WHERE CURRENT OF c_FILE_CSV;
    
        -- -------------------------------------------------------- --
        -- ������� ������� �/�
        -- -------------------------------------------------------- --
        IF r_abn.PROFILE_ID IS NULL THEN
            v_profile_id := Pk05_Account.Set_profile(
                 p_account_id         => v_account_id,
                 p_brand_id           => NULL,
                 p_contract_id        => v_contract_id,
                 p_customer_id        => v_customer_id,
                 p_subscriber_id      => NULL,
                 p_contractor_id      => r_abn.CONTRACTOR_ID,
                 p_branch_id          => r_abn.XTTK_ID,
                 p_agent_id           => r_abn.AGENT_ID,
                 p_contractor_bank_id => r_abn.CONTRACTOR_BANK_ID,
                 p_vat                => Pk00_Const.c_VAT,
                 p_date_from          => r_abn.CONTRACT_DATE,
                 p_date_to            => NULL
             );
            -- ��������� ID
            UPDATE PK211_XTTK_IMPORT_T
               SET PROFILE_ID = v_profile_id
             WHERE CURRENT OF c_FILE_CSV;    
        ELSE
            v_profile_id := r_abn.PROFILE_ID;
        END IF;
    
        -- -------------------------------------------------------- --
        -- ������� ����������� �����
        -- -------------------------------------------------------- --
        IF r_abn.JUR_ADDRESS_ID IS NULL THEN
            SELECT MIN(AC.CONTACT_ID), COUNT(*) 
              INTO v_jur_address_id, v_count
              FROM ACCOUNT_CONTACT_T AC
             WHERE AC.ACCOUNT_ID   = v_account_id
               AND AC.ADDRESS_TYPE = PK00_CONST.c_ADDR_TYPE_JUR
            ;
            IF v_jur_address_id IS NULL THEN
                v_jur_address_id := PK05_ACCOUNT.Add_address(
                            p_account_id    => v_account_id,
                            p_address_type  => PK00_CONST.c_ADDR_TYPE_JUR,
                            p_country       => '��',
                            p_zip           => r_abn.JUR_ZIP,
                            p_state         => r_abn.JUR_REGION,
                            p_city          => r_abn.JUR_CITY,
                            p_address       => r_abn.JUR_ADDRESS,
                            p_person        => NULL,
                            p_phones        => NULL,
                            p_fax           => NULL,
                            p_email         => NULL,
                            p_date_from     => r_abn.CONTRACT_DATE,
                            p_date_to       => NULL,
                            p_notes         => '������������� �� XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
                       );
            END IF;
            -- ��������� ID
            UPDATE PK211_XTTK_IMPORT_T
               SET JUR_ADDRESS_ID = v_jur_address_id
             WHERE CURRENT OF c_FILE_CSV;
        ELSE
            v_jur_address_id := r_abn.JUR_ADDRESS_ID;
        END IF;
    
        -- -------------------------------------------------------- --
        -- ������� ����� ��������
        -- -------------------------------------------------------- --
        IF r_abn.DLV_ADDRESS_ID IS NULL THEN
            SELECT MIN(AC.CONTACT_ID), COUNT(*) 
              INTO v_dlv_address_id, v_count
              FROM ACCOUNT_CONTACT_T AC
             WHERE AC.ACCOUNT_ID   = v_account_id
               AND AC.ADDRESS_TYPE = PK00_CONST.c_ADDR_TYPE_JUR
            ;
            IF v_dlv_address_id IS NULL THEN  
                v_dlv_address_id := PK05_ACCOUNT.Add_address(
                            p_account_id    => v_account_id,
                            p_address_type  => PK00_CONST.c_ADDR_TYPE_DLV,
                            p_country       => '��',
                            p_zip           => r_abn.DLV_ZIP,
                            p_state         => r_abn.DLV_REGION,
                            p_city          => r_abn.DLV_CITY,
                            p_address       => r_abn.DLV_ADDRESS,
                            p_person        => NULL,
                            p_phones        => NULL,
                            p_fax           => NULL,
                            p_email         => NULL,
                            p_date_from     => r_abn.CONTRACT_DATE,
                            p_date_to       => NULL,
                            p_notes         => '������������� �� XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
                       );
            END IF;
            -- ��������� ID
            UPDATE PK211_XTTK_IMPORT_T
               SET DLV_ADDRESS_ID = v_dlv_address_id
             WHERE CURRENT OF c_FILE_CSV;
        ELSE
            v_dlv_address_id := r_abn.DLV_ADDRESS_ID;
        END IF;
    
        -- -------------------------------------------------------- --
        -- ���������� ������ (��� - ����������)
        -- -------------------------------------------------------- --
        SELECT S.SERVICE_ID INTO v_service_id 
          FROM SERVICE_T S
         WHERE S.SERVICE = r_abn.SERVICE;
        
        UPDATE PK211_XTTK_IMPORT_T
           SET SERVICE_ID = v_service_id
         WHERE CURRENT OF c_FILE_CSV;
        
        -- -------------------------------------------------------- --
        -- ������� �����
        -- -------------------------------------------------------- --
        IF r_abn.ORDER_ID IS NULL THEN
            v_order_id := Pk06_Order.New_order(
               p_account_id   => v_account_id,       -- ID �������� �����
               p_order_no     => r_abn.ORDER_NO,     -- ����� ������, ��� �� ������
               p_service_id   => v_service_id,       -- ID ������ �� ������� SERVICE_T
               p_rateplan_id  => NULL,               -- ID ��������� ����� �� RATEPLAN_T
               p_time_zone    => NULL,               -- GMT               
               p_date_from    => NVL(r_abn.ORDER_DATE, r_abn.CONTRACT_DATE), -- ���� ������ �������� ������
               p_date_to      => Pk00_Const.c_DATE_MAX,
               p_create_date  => SYSDATE,
               p_note         => '������������� �� XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy') 
            );
        ELSE
            v_order_id := r_abn.ORDER_ID;
        END IF;

        UPDATE PK211_XTTK_IMPORT_T
           SET ORDER_ID = v_order_id
         WHERE CURRENT OF c_FILE_CSV;

        -- ��������� ������������ ������, ��� ��� �������� � ������
        IF r_abn.SERVICE_ALIAS IS NOT NULL AND r_abn.SERVICE != r_abn.SERVICE_ALIAS THEN
            SELECT COUNT(*) INTO v_count
              FROM SERVICE_ALIAS_T SA
             WHERE SA.ACCOUNT_ID = v_account_id
               AND SA.SERVICE_ID = v_service_id
            ;
            IF v_count = 0 THEN 
                INSERT INTO SERVICE_ALIAS_T (SERVICE_ID, ACCOUNT_ID, SRV_NAME)
                VALUES(v_service_id, v_account_id, r_abn.SERVICE_ALIAS);
            END IF;
        END IF;

        -- -------------------------------------------------------- --
        -- ��������� ���������� � ������ �����������
        -- -------------------------------------------------------- --
        INSERT INTO ORDER_INFO_T( ORDER_ID, POINT_SRC, POINT_DST, SPEED_STR )
        VALUES( v_order_id, r_abn.POINT_SRC, r_abn.POINT_DST, r_abn.SPEED);
    
        -- -------------------------------------------------------- --
        -- ��������� ���������� ������
        -- -------------------------------------------------------- --
        -- ���������
        IF r_abn.ABP_NUMBER IS NOT NULL THEN
            IF r_abn.ORDER_BODY_ID IS NULL THEN
                v_rec_ob_id := Pk06_order.Add_subs_abon (
                     p_order_id      => v_order_id,               -- ID ������ - ������
                     p_subservice_id => Pk00_Const.c_SUBSRV_REC,  -- ID ���������� ������
                     p_value         => r_abn.ABP_NUMBER,         -- ����� ���������
                     p_tax_incl      => 'N',                      -- ������� �� ����� � ����� ���������
                     p_currency_id   => Pk00_Const.c_CURRENCY_RUB,-- ������
                     p_quantity      => r_abn.QUANTITY,           -- ���-�� ������ � ����������� ���������
                     p_date_from     => NVL(r_abn.ORDER_DATE, r_abn.CONTRACT_DATE),
                     p_date_to       => Pk00_Const.c_DATE_MAX
                );
                UPDATE PK211_XTTK_IMPORT_T
                   SET ORDER_BODY_ID = v_rec_ob_id
                 WHERE CURRENT OF c_FILE_CSV;

                Pk06_order.Add_subs_downtime (
                     p_order_id      => v_order_id,               -- ID ������ - ������
                     p_charge_type   => Pk00_Const.c_CHARGE_TYPE_IDL,
                     p_free_value    => 43,  -- ���-�� ���������������� ����� ��������
                     p_descr         => NULL,
                     p_date_from     => NVL(r_abn.ORDER_DATE, r_abn.CONTRACT_DATE)
                 );  

            END IF;

        END IF;
    
        -- -------------------------------------------------------- --
        -- ������� ������� ����������� �������
        v_count := v_count + 1;
        
        IF MOD(v_count, 100) = 0 THEN
            Pk01_Syslog.Write_msg(v_count||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
      EXCEPTION
         -- -------------------------------------------------------- --
         -- ��������� ������ �������� ������ 
         -- -------------------------------------------------------- --
         WHEN OTHERS THEN
            v_load_status := Pk01_Syslog.get_OraErrTxt(c_PkgName||'.'||v_prcName);
            UPDATE PK211_XTTK_IMPORT_T
               SET LOAD_STATUS = v_load_status
             WHERE CURRENT OF c_FILE_CSV;

            v_error := v_error + 1;
      END;
    END LOOP;

    Pk01_Syslog.Write_msg('Report: '||v_count||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;



END PK211_XTTK_IMPORT_CSV;
/
