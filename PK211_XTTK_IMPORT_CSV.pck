CREATE OR REPLACE PACKAGE PK211_XTTK_IMPORT_CSV
IS
    --
    -- ����� ��� �������� ������������� �������� �������� ����� ����������
    -- ��������������� � ���� ����� *.csv �� ����
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK211_XTTK_IMPORT_CSV';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;

    e_APP_EXCEPTION CONSTANT NUMBER := -20100;  -- ����� ���������� � ���������� ������������
    
    type t_refc is ref cursor;

    TYPE t_number_table IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
    TYPE t_varchar_table IS TABLE OF VARCHAR2(40) INDEX BY PLS_INTEGER;

    -- =====================================================================
    -- ������� � ������� ��������� ��������, ��� �������� ����� ��������
    c_BILLING_XTTK        CONSTANT INTEGER := 2008; -- ������� �������� ����� ���������� �� ����
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
    c_LOAD_CODE_ABP       CONSTANT INTEGER :=11;    -- �������� ��������� ����� - ���������
    c_LOAD_CODE_USG       CONSTANT INTEGER :=12;    -- �������� ��������� ����� - ������
    c_LOAD_CODE_FIN       CONSTANT INTEGER :=13;    -- �����

    -- ������ �������� ����������
    c_DLV_METHOD_AP CONSTANT INTEGER := 6512;   -- ����������

    c_CONTRACTOR_ID       CONSTANT INTEGER := 1;

    -- ������������ (contractor_id)
    c_mbl CONSTANT INTEGER := 1524374; -- ������������ ������
    c_mvv CONSTANT INTEGER := 1524395; -- ������������ ��������������
    c_mdv CONSTANT INTEGER := 1524387; -- ������������ ������� ������
    c_mkz CONSTANT INTEGER := 1524389; -- ������������ ������
    c_mkl CONSTANT INTEGER := 1524391; -- ������������ �����������
    c_msp CONSTANT INTEGER := 1524399; -- ������������ ����ʻ
    c_msh CONSTANT INTEGER := 1524393; -- ������������ ��������
    c_msv CONSTANT INTEGER := 1524380; -- ������������ �����
    c_msz CONSTANT INTEGER := 1520993; -- ������������ ������-�����
    c_msr CONSTANT INTEGER := 1524376; -- ������������ ��������������
    c_mur CONSTANT INTEGER := 1524365; -- ������������ ����
    c_mct CONSTANT INTEGER := 1524378; -- ������������ �����
    c_mch CONSTANT INTEGER := 1524397; -- ������������ ����
    c_muv CONSTANT INTEGER := 1524383; -- ������������ ���-������
    c_muu CONSTANT INTEGER := 1524385; -- ������������ ����� ����
    c_vlg CONSTANT INTEGER := 23;      -- ������ ��ʻ

    -- H����������� �������� � �������� (branch_id)
    c_br_mbl CONSTANT INTEGER  := 297; -- ������������ ������ (��)
    c_br_mvv CONSTANT INTEGER  := 298; -- ������������ �������������� (��)
    c_br_mdv CONSTANT INTEGER  := 299; -- ������������ ������� ������ (��)
    c_br_mkz CONSTANT INTEGER  := 300; -- ������������ ������ (��)
    c_br_mkl CONSTANT INTEGER  := 301; -- ������������ ����������� (��)
    c_br_msp CONSTANT INTEGER  := 302; -- ������������ ����ʻ (��)
    c_br_msh CONSTANT INTEGER  := 303; -- ������������ �������� (��)
    c_br_msv CONSTANT INTEGER  := 304; -- ������������ ����� (��)
    c_br_msvd CONSTANT INTEGER := 312; -- ������������ ����� (�� ���� ������)
    c_br_msz CONSTANT INTEGER  := 305; -- ������������ ������-����� (��)
    c_br_msr CONSTANT INTEGER  := 306; -- ������������ �������������� (��)
    c_br_mur CONSTANT INTEGER  := 307; -- ������������ ���� (��)
    c_br_mct CONSTANT INTEGER  := 308; -- ������������ ����� (��)
    c_br_mch CONSTANT INTEGER  := 309; -- ������������ ���� (��)
    c_br_muv CONSTANT INTEGER  := 310; -- ������������ ���-������ (��)
    c_br_muu CONSTANT INTEGER  := 311; -- ������������ ����� ���� (��)
    c_br_vlg CONSTANT INTEGER  := 314;  -- ������ ��ʻ (��)

    -- ����� ������������� (bank_id)
    c_bank_mbl CONSTANT INTEGER := 1524375; -- ������������ ������
    c_bank_mvv CONSTANT INTEGER := 1524396; -- ������������ ��������������
    c_bank_mdv CONSTANT INTEGER := 1524388; -- ������������ ������� ������
    c_bank_mkz CONSTANT INTEGER := 1524390; -- ������������ ������
    c_bank_mkl CONSTANT INTEGER := 1524392; -- ������������ �����������
    c_bank_msp CONSTANT INTEGER := 1524400; -- ������������ ����ʻ
    c_bank_msh CONSTANT INTEGER := 1524394; -- ������������ ��������
    c_bank_msv CONSTANT INTEGER := 1524381; -- ������������ �����
    c_bank_msz CONSTANT INTEGER := 10;      -- ������������ ������-�����
    c_bank_msr CONSTANT INTEGER := 1524377; -- ������������ ��������������
    c_bank_mur CONSTANT INTEGER := 1524367; -- ������������ ����
    c_bank_mct CONSTANT INTEGER := 1524379; -- ������������ �����
    c_bank_mch CONSTANT INTEGER := 1524398; -- ������������ ����
    c_bank_muv CONSTANT INTEGER := 1524384; -- ������������ ���-������
    c_bank_vlg CONSTANT INTEGER := 1;       -- ������ ��ʻ (��)

    -- �������� ������ ��������������� ����������    
    CURSOR c_FILE_CSV IS (
       SELECT 
          ROW_NUMBER() OVER (ORDER BY REGION, ERP_CODE, CLIENT, ORDER_NO, SERVICE) RN,
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
          EMAIL,  
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
          RATEPLAN,
          SUBSERVICE,
          CHARGE_TYPE,
          NOTES,
          -- ������ ��� �������� � BRM
          BRM_CONTRACT_ID,
          BRM_CONTRACT_NO,
          BRM_ACCOUNT_ID,
          BRM_ACCOUNT_NO,
          BRM_ORDER_ID,
          BRM_ORDER_NO,
          BRM_PROFILE_ID,
          BRM_CLIENT_ID,
          BRM_CUSTOMER_ID,
          BRM_CONTRACTOR_ID,
          BRM_CONTRACTOR_BANK_ID,
          BRM_XTTK_ID,
          BRM_AGENT_ID,
          BRM_JUR_ADDRESS_ID,
          BRM_DLV_ADDRESS_ID,
          BRM_EMAIL,
          --
          BRM_SERVICE_ID,
          BRM_CHARGE_TYPE,
          BRM_ORDER_BODY_ID,
          BRM_SUBSERVICE_ID,
          BRM_ORDER_BODY_2_ID,
          BRM_ABP_NUMBER,  
          BRM_SPEED_VALUE,
          BRM_SPEED_UNIT_ID,
          --
          BRM_MANAGER_ID,
          BRM_MGR_LAST,
          BRM_MGR_FIRST,
          BRM_MGR_MIDDLE,  
          --
          REGION,
          LOAD_STATUS,
          LOAD_DATE,
          LOAD_CODE,
          DATE_IMPORT
         FROM PK211_XTTK_IMPORT_T 
        WHERE LOAD_CODE = c_LOAD_CODE_START
    )FOR UPDATE;
    
    --============================================================================================
    -- ����� ������� ������������ ��������� �/� �� c_BILLING_NPL -> Pk00_Const.c_BILLING_OLD
    --============================================================================================
    PROCEDURE Change_billing_id;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ��������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Clear_tmp; 
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ��������� ������� ��������������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Set_manager_id;
    PROCEDURE Set_client_id;
    PROCEDURE Set_customer_id;
    PROCEDURE Set_contract_id;
    PROCEDURE Set_account_id;
    PROCEDURE Set_order_id;
    PROCEDURE Set_order_body_id;
    PROCEDURE Set_service_id;
    PROCEDURE Set_subservice_id;
    PROCEDURE Set_abp_number;
    --
    PROCEDURE Set_ext_data;   -- ���������� ��� ���������� ���������
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������ �� ��������� ������� PK211_XTTK_IMPORT_TMP
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Import_data(
                  p_contractor_id  INTEGER,
                  p_xttk_id        INTEGER,
                  p_agent_id       INTEGER,
                  p_bank_id        INTEGER,
                  p_region         CONTRACTOR_T.CONTRACTOR%TYPE
              );
    
    PROCEDURE Import_data_seller(p_seller_id IN INTEGER);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������ �� PORTAL 6.5 � �������  PK211_XTTK_IMPORT_T
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Import_data_65(p_seller_id IN INTEGER);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ���������� � ������� ������ ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Load_data;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����� � �������� �� ������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Report(p_recordset out t_refc, p_seller_id IN INTEGER);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ������ �� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ �������������� XTTK-�� 
    PROCEDURE Import_mvv;
    
    -- ������������ ��������
    PROCEDURE Import_msh;
    
    -- ������������ ������� ������
    PROCEDURE Import_mdv;
    
    -- ������������ ���-������
    PROCEDURE Import_muv; 
    
    -- ������������ ����
    PROCEDURE Import_mur;

    -- ������������ ������
    PROCEDURE Import_mbl;

    -- ������������ ������
    PROCEDURE Import_mkz;
  
    -- ������������� �������:

    -- ������ ��� (��)�
    PROCEDURE Import_vlg;

    -- ���� ��� (��)�
    PROCEDURE Import_sib;


    -- ========================================================================== --
    -- � � � � � � � � � � �   � � � � � �
    -- ========================================================================== --
    PROCEDURE Correct_profile;

    -- =========================================================== --
    -- ������������ ������
    -- =========================================================== --
    -- ------------------------------------------------------------------------- --
    -- ������� ����� ��� ���������� ������� (�� ���� ����������) 
    -- ------------------------------------------------------------------------- --
    PROCEDURE Create_bills( p_period_id IN INTEGER, p_branch_id IN INTEGER );

    -- ------------------------------------------------------------------------- --
    -- ������������ ����� �������� ��.��� �������� (�� ���� ����������) 
    -- ------------------------------------------------------------------------- --
    PROCEDURE Make_bills( p_period_id IN INTEGER, p_branch_id IN INTEGER );
    
    -- =========================================================== --
    -- �����
    -- =========================================================== --
    -- ����� ����������� ������ ��� �������� 2008, 
    PROCEDURE Rollback_bills(p_period_id IN INTEGER, p_branch_id IN INTEGER);

    -- ����� ����������� ������, �� ������ ������� PK211_XTTK_IMPORT_T
    PROCEDURE Rollback_data(p_branch_id IN INTEGER);
    
    --========================================================================== --
    -- ��������� ����������� ������ � �����
    --========================================================================== --    --============================================================================================
    PROCEDURE Move_to_archive;
    
    --========================================================================== --
    -- � � � � � � � � �     � � � � � � � � � 
    --========================================================================== --
    PROCEDURE Account_profile_t_drop_fk;    
    PROCEDURE Account_profile_t_add_fk;
    PROCEDURE Billinfo_t_drop_fk;    
    PROCEDURE Billinfo_t_add_fk;
    
END PK211_XTTK_IMPORT_CSV;
/
CREATE OR REPLACE PACKAGE BODY PK211_XTTK_IMPORT_CSV
IS

--============================================================================================
-- ����� ������� ������������ ��������� �/� �� 2007 -> ...
--============================================================================================
PROCEDURE Change_billing_id
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Change_billing_id';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

--    UPDATE ACCOUNT_T A
--       SET A.BILLING_ID = 2003
--     WHERE A.BILLING_ID = 2007;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T.BILLING_ID: '||v_count||' rows c_BILLING_NPL -> c_BILLING_OLD', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� ��������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Clear_tmp 
IS 
    v_prcName        CONSTANT VARCHAR2(30) := 'Clear_tmp';
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK211_XTTK_IMPORT_TMP DROP STORAGE';
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
    FOR r_mgr IN (
      SELECT DISTINCT X.BRM_CONTRACTOR_ID, TRIM(MANAGER) MANAGER
        FROM PK211_XTTK_IMPORT_T X
       WHERE LOAD_CODE      IS NULL
         AND BRM_MANAGER_ID IS NULL
         AND MANAGER        IS NOT NULL
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
      WHERE M.LAST_NAME  = v_last_name
        AND M.FIRST_NAME = v_first_name
        AND M.MIDDLE_NAME= v_middle_name
        AND M.CONTRACTOR_ID = r_mgr.brm_contractor_id;

      -- ���������� ID ���������
      IF v_manager_id IS NULL THEN
        v_manager_id := Pk02_Poid.Next_manager_id;
      END IF;

      -- ����������� �.�.� - BRM-��������� � �������: '������ �.�.'
      UPDATE PK211_XTTK_IMPORT_T X
         SET BRM_MANAGER_ID = v_manager_id,
             BRM_MGR_LAST   = v_last_name,
             BRM_MGR_FIRST  = v_first_name,
             BRM_MGR_MIDDLE = v_middle_name
       WHERE LOAD_CODE      IS NULL
         AND BRM_MANAGER_ID IS NULL
         AND BRM_MGR_LAST IS NULL
         AND MANAGER = r_mgr.manager;

    END LOOP;  

    Pk01_Syslog.Write_msg(v_count||' - distinct manager_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� CLIENT_ID ������� ����� ������������ ��� �������� ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_client_id
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Set_client_id';
    v_client_id INTEGER;
    v_count     INTEGER := 0;
BEGIN
    -- ������� ������ �������
    UPDATE PK211_XTTK_IMPORT_T X
       SET X.CLIENT = TRIM(X.CLIENT)
     WHERE LOAD_CODE IS NULL
       AND BRM_CLIENT_ID IS NULL;

    FOR r_cln IN (
      SELECT DISTINCT CLIENT CLIENT  
        FROM PK211_XTTK_IMPORT_T X
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
      END IF;
      --
      UPDATE PK211_XTTK_IMPORT_T X
         SET X.BRM_CLIENT_ID = v_client_id
       WHERE X.CLIENT        = r_cln.CLIENT
         AND X.LOAD_CODE     IS NULL
         AND X.BRM_CLIENT_ID IS NULL;  
      --
      v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg(v_count||' - distinct client_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �����������  CUSTOMER_ID - ������� ����� ������������ ��� �������� �������� � �/�
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_customer_id
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Set_customer_id';
    v_customer_id INTEGER;
    v_count       INTEGER := 0;
BEGIN
    -- ������� ������ �������
    UPDATE PK211_XTTK_IMPORT_T X
       SET X.CLIENT = TRIM(X.CLIENT)
     WHERE LOAD_CODE       IS NULL
       AND BRM_CUSTOMER_ID IS NULL;

    FOR r_cst IN (
      SELECT ERP_CODE, CLIENT CLIENT, INN, KPP  
        FROM PK211_XTTK_IMPORT_T X
       WHERE LOAD_CODE       IS NULL
         AND BRM_CUSTOMER_ID IS NULL
         AND CLIENT          IS NOT NULL
       GROUP BY ERP_CODE, CLIENT, INN, KPP
    ) LOOP
      --
      -- ���� ����� ������������ �����������
      SELECT MIN(CS.CUSTOMER_ID) 
        INTO v_customer_id  
        FROM CUSTOMER_T CS
       WHERE CS.ERP_CODE = r_cst.ERP_CODE  
         AND NVL(CS.INN,'0') = NVL(r_cst.INN,'0')
         AND NVL(CS.KPP,'0') = NVL(r_cst.KPP,'0')
         AND LOWER(CS.CUSTOMER) = LOWER(r_cst.CLIENT);
        
      IF v_customer_id IS NULL THEN 
         v_customer_id := PK02_POID.NEXT_CUSTOMER_ID;
      END IF;
      --
      UPDATE PK211_XTTK_IMPORT_T X
         SET X.BRM_CUSTOMER_ID = v_customer_id
       WHERE X.ERP_CODE        = r_cst.ERP_CODE  
         AND NVL(X.INN,'0')    = NVL(r_cst.INN,'0')
         AND NVL(X.KPP,'0')    = NVL(r_cst.KPP,'0')
         AND TRIM(X.CLIENT)    = r_cst.CLIENT
         AND X.LOAD_CODE       IS NULL
         AND X.BRM_CUSTOMER_ID IS NULL;  
      --
      v_count := v_count + 1;
    END LOOP;
    --
    Pk01_Syslog.Write_msg(v_count||' - distinct customer_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
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
    v_exists      INTEGER;
BEGIN
    -- ������� ������ �������
    UPDATE PK211_XTTK_IMPORT_T X
       SET X.CONTRACT_NO = TRIM(X.CONTRACT_NO)
     WHERE LOAD_CODE       IS NULL
       AND BRM_CONTRACT_ID IS NULL
       AND CONTRACT_NO     IS NOT NULL;
        
    FOR r_ctr IN (
      SELECT DISTINCT CONTRACT_NO  
        FROM PK211_XTTK_IMPORT_T X
       WHERE LOAD_CODE       IS NULL
         AND BRM_CONTRACT_ID IS NULL
         AND CONTRACT_NO     IS NOT NULL
    ) LOOP
      --
      -- ��������� �� ������� ������ � BRM
      SELECT COUNT(*) INTO v_exists
        FROM CONTRACT_T C
       WHERE C.CONTRACT_NO = r_ctr.contract_no; 
       
      IF v_exists = 0 THEN
          v_contract_id := PK02_POID.NEXT_CONTRACT_ID;
          -- ����������� ID
          UPDATE PK211_XTTK_IMPORT_T X
             SET X.BRM_CONTRACT_ID = v_contract_id,
                 X.BRM_CONTRACT_NO = X.CONTRACT_NO
           WHERE X.CONTRACT_NO     = r_ctr.contract_no
             AND X.LOAD_CODE       IS NULL
             AND X.BRM_CONTRACT_ID IS NULL;
      ELSE
          UPDATE PK211_XTTK_IMPORT_T X
             SET X.LOAD_CODE       = c_LOAD_CODE_DBL,
                 X.NOTES           = '����� �������� ��� ���������� � BRM'
           WHERE X.CONTRACT_NO     = r_ctr.contract_no
             AND X.LOAD_CODE       IS NULL
             AND X.BRM_CONTRACT_ID IS NULL;
      END IF;
      --
      v_count := v_count + 1;
      --
    END LOOP;
    --
    Pk01_Syslog.Write_msg(v_count||' - distinct contract_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
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
    v_account_no     VARCHAR2(100);
    v_count          INTEGER := 0;
BEGIN
    FOR r_acc IN (
        SELECT DISTINCT X.CONTRACT_NO, NVL(X.ACCOUNT_NO,'NULL') ACCOUNT_NO
          FROM PK211_XTTK_IMPORT_T X
         WHERE X.LOAD_CODE      IS NULL
           AND X.BRM_ACCOUNT_ID IS NULL
    )  
    LOOP
        v_account_no     := Pk05_Account.New_rp_account_no;
        v_account_id     := PK02_POID.NEXT_ACCOUNT_ID;
        v_profile_id     := PK02_POID.NEXT_ACCOUNT_PROFILE_ID;
        v_jur_address_id := PK02_POID.NEXT_ADDRESS_ID;
        v_dlv_address_id := PK02_POID.NEXT_ADDRESS_ID;

        UPDATE PK211_XTTK_IMPORT_T X
         SET X.BRM_ACCOUNT_NO     = v_account_no,
             X.BRM_ACCOUNT_ID     = v_account_id,
             X.BRM_PROFILE_ID     = v_profile_id,
             X.BRM_JUR_ADDRESS_ID = v_jur_address_id,
             X.BRM_DLV_ADDRESS_ID = v_dlv_address_id
         WHERE X.LOAD_CODE      IS NULL
           AND X.BRM_ACCOUNT_ID IS NULL
           AND X.CONTRACT_NO    = r_acc.contract_no
           AND NVL(X.ACCOUNT_NO,'NULL') = r_acc.account_no;

        v_count := v_count + 1;
        --
    END LOOP;
    Pk01_Syslog.Write_msg(v_count||' - distinct account_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� ORDER_ID - ������� ����� ������������ 
-- ��� �������� ������ �� ������ � ����������� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_order_id
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Set_order_id';
    v_order_id    INTEGER;
    v_order_no    VARCHAR2(100);
    v_count       INTEGER := 0;
BEGIN
    -- ������� ������ �������
    UPDATE PK211_XTTK_IMPORT_T X
       SET X.ORDER_NO = TRIM(X.ORDER_NO)
     WHERE LOAD_CODE    IS NULL
       AND BRM_ORDER_ID IS NULL;
       
    -- ��������� ����� ������ 
    FOR r_ord IN (
      SELECT DISTINCT X.ORDER_NO, X.BRM_ACCOUNT_NO 
        FROM PK211_XTTK_IMPORT_T X
       WHERE LOAD_CODE    IS NULL
         AND BRM_ORDER_ID IS NULL
       ORDER BY BRM_CONTRACT_NO
    )
    LOOP
      -- �������� ����� ������
      IF r_ord.order_no IS NULL THEN
        v_order_no := r_ord.brm_account_no;
      ELSE
        v_order_no := r_ord.brm_account_no||'-'||r_ord.order_no;
      END IF;
      
      v_order_id := PK02_POID.NEXT_ORDER_ID;
      
      UPDATE PK211_XTTK_IMPORT_T X
         SET X.BRM_ORDER_ID = v_order_id,
             X.BRM_ORDER_NO = v_order_no
       WHERE LOAD_CODE    IS NULL
         AND BRM_ORDER_ID IS NULL
         AND BRM_ORDER_NO IS NOT NULL
         AND BRM_ACCOUNT_NO = r_ord.brm_account_no
         AND NVL(ORDER_NO,'NULL') = NVL(r_ord.order_no,'NULL');
      
      v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg(v_count||' - distinct order_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� ORDER_BODY_ID - ������� ����� ������������ ��� �������� ������ ����������� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_order_body_id
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Set_order_body_id';
    v_count         INTEGER := 0;
BEGIN
    --
    UPDATE PK211_XTTK_IMPORT_T X
       SET BRM_ORDER_BODY_ID   = PK02_POID.NEXT_ORDER_BODY_ID,
           BRM_ORDER_BODY_2_ID = PK02_POID.NEXT_ORDER_BODY_ID
     WHERE LOAD_CODE IS NULL
       AND BRM_ORDER_BODY_ID IS NULL;
    --
    v_count := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg(v_count||' - distinct order_body_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� SERVICE_ID
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_service_id
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Set_service_id';
    v_count         INTEGER := 0;
BEGIN
    -- ������� ������ �������
    UPDATE PK211_XTTK_IMPORT_T X
       SET SERVICE   = TRIM(SERVICE)
     WHERE LOAD_CODE   IS NULL;
     
    -- ����������� ������ ��� ���� ������ �� ���������
    UPDATE PK211_XTTK_IMPORT_T X
       SET LOAD_STATUS = '������ �� �������',
           LOAD_CODE   = c_LOAD_CODE_ERR
     WHERE LOAD_CODE   IS NULL
       AND SERVICE     IS NULL;
       
    -- ����� ������
    FOR r_srv IN (
      SELECT DISTINCT X.SERVICE, S.SERVICE_ID  
        FROM PK211_XTTK_IMPORT_T X, SERVICE_T S
       WHERE LOWER(X.SERVICE) = LOWER(S.SERVICE(+))
         AND X.LOAD_CODE      IS NULL
         AND X.BRM_SERVICE_ID IS NULL  
         AND X.SERVICE        IS NOT NULL
    ) LOOP
      --
      IF r_srv.service_id  IS NOT NULL THEN
        UPDATE PK211_XTTK_IMPORT_T X
           SET BRM_SERVICE_ID  = r_srv.service_id
         WHERE SERVICE         = r_srv.service
           AND LOAD_CODE       IS NULL
           AND BRM_SERVICE_ID  IS NULL;
      ELSE
        UPDATE PK211_XTTK_IMPORT_T X
           SET LOAD_STATUS = '������ �� �������',
               LOAD_CODE   = c_LOAD_CODE_ERR
         WHERE SERVICE     = r_srv.service
           AND LOAD_CODE       IS NULL
           AND BRM_SERVICE_ID  IS NULL;
      END IF;
      --
      v_count := v_count + 1;
      --
    END LOOP;
    
    Pk01_Syslog.Write_msg(v_count||' - distinct service_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� SUBSERVICE_ID
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_subservice_id
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Set_subservice_id';
    v_count         INTEGER := 0;
BEGIN
    UPDATE PK211_XTTK_IMPORT_T X
       SET X.SUBSERVICE = TRIM(X.SUBSERVICE)
     WHERE LOAD_CODE           IS NULL
       AND X.SUBSERVICE        IS NOT NULL
       AND X.BRM_SUBSERVICE_ID IS NULL;

    FOR r_ss IN (
      SELECT DISTINCT X.CHARGE_TYPE, X.SUBSERVICE, SS.SUBSERVICE_ID
        FROM PK211_XTTK_IMPORT_T X, SUBSERVICE_T SS
       WHERE LOAD_CODE           IS NULL
         AND X.SUBSERVICE        IS NOT NULL
         AND X.BRM_SUBSERVICE_ID IS NULL
         AND LOWER(X.SUBSERVICE) = LOWER(SS.SUBSERVICE)
    )
    LOOP
      UPDATE PK211_XTTK_IMPORT_T X
         SET X.BRM_SUBSERVICE_ID = r_ss.subservice_id,
             X.BRM_CHARGE_TYPE   = r_ss.charge_type
       WHERE LOAD_CODE           IS NULL
         AND X.SUBSERVICE        IS NOT NULL
         AND X.BRM_SUBSERVICE_ID IS NULL
         AND X.SUBSERVICE  = r_ss.subservice
         AND X.CHARGE_TYPE = r_ss.Charge_Type;
    END LOOP;  

    UPDATE PK211_XTTK_IMPORT_T X
       SET LOAD_CODE = c_LOAD_CODE_ERR,
           NOTES     = '�� ������ ��������� ������'
     WHERE LOAD_CODE           IS NULL
       AND X.SUBSERVICE        IS NOT NULL
       AND X.BRM_SUBSERVICE_ID IS NULL; 


    FOR r_srv IN (
      SELECT BRM_ORDER_ID, SERVICE, ORDER_CNT, SERVICE_CNT 
        FROM (
          SELECT BRM_ORDER_ID, ORDER_NO, BRM_ORDER_NO,   
                 COUNT(*) OVER (PARTITION BY BRM_ORDER_ID) ORDER_CNT,
                 COUNT(*) OVER (PARTITION BY BRM_ORDER_ID, SERVICE) SERVICE_CNT,
                 BRM_SERVICE_ID, SERVICE, LOAD_CODE, LOAD_STATUS 
            FROM PK211_XTTK_IMPORT_T X
           WHERE LOAD_CODE IS NULL
      )
      WHERE BRM_SERVICE_ID IS NOT NULL  
    ) LOOP
      --
      IF r_srv.order_cnt = r_srv.service_cnt  THEN
        UPDATE PK211_XTTK_IMPORT_T X
           SET BRM_SUBSERVICE_ID = Pk00_Const.c_SUBSRV_REC,
               BRM_CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_REC
         WHERE SERVICE      = r_srv.service
           AND BRM_ORDER_ID = r_srv.brm_order_id
           AND ABP_VALUE         IS NOT NULL
           AND LOAD_CODE         IS NULL
           AND BRM_SUBSERVICE_ID IS NULL;
      ELSE
        UPDATE PK211_XTTK_IMPORT_T X
           SET LOAD_STATUS = '����� ����� ������ �� ������',
               LOAD_CODE   = c_LOAD_CODE_ERR
         WHERE SERVICE     = r_srv.service
           AND BRM_ORDER_ID    = r_srv.brm_order_id
           AND LOAD_CODE     IS NULL
           AND BRM_SUBSERVICE_ID IS NULL;
      END IF;
      --
      v_count := v_count + 1;
      --
    END LOOP;
    Pk01_Syslog.Write_msg(v_count||' - distinct service_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� �������� � �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_abp_number
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Set_abp_number';
    v_count         INTEGER := 0;
BEGIN
    -- ��������� ��������� � �����, � ����� ����� ���� ��� ������
    UPDATE PK211_XTTK_IMPORT_T 
       SET BRM_ABP_NUMBER = TO_NUMBER(REPLACE(REPLACE(RTRIM(ABP_VALUE,'�. '),',','.'),' ',''))
     WHERE LTRIM(RTRIM(ABP_VALUE,'�. '),'1234567890., ') IS NULL 
       AND INSTR(ABP_VALUE,',',1,2) = 0
       AND BRM_ABP_NUMBER IS NULL;
--       AND LOAD_CODE  IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T.ABP_NUMBER '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������
    UPDATE PK211_XTTK_IMPORT_T 
       SET LOAD_STATUS = '� ���� ABP_VALUE �� �������� ��������',
           LOAD_CODE   = c_LOAD_CODE_ERR
     WHERE BRM_ABP_NUMBER IS NULL
       AND LOAD_CODE  IS NULL;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� ���������� � �������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_speed_info
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Set_speed_info';
    v_count         INTEGER := 0;
BEGIN
UPDATE PK211_XTTK_IMPORT_T X
   SET BRM_SPEED_VALUE = TRIM(SUBSTR(SPEED, 1, INSTR(SPEED,' '))),
       BRM_SPEED_UNIT_ID = (       
           CASE
           WHEN TRIM(SUBSTR(SPEED, INSTR(SPEED,' ')+1)) = '����/�' THEN 6700
           WHEN TRIM(SUBSTR(SPEED, INSTR(SPEED,' ')+1)) = '����/�' THEN 6701
           WHEN TRIM(SUBSTR(SPEED, INSTR(SPEED,' ')+1)) = '����/�' THEN 6702
           END
           );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T.SPEED_VALUE '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ����������� ������� ��������������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_ext_data
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Set_ext_data';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Set_manager_id;
    Set_client_id;
    Set_customer_id;
    Set_contract_id;
    Set_account_id;
    Set_order_id;
    Set_order_body_id;
    Set_service_id;
    Set_subservice_id;
    Set_abp_number;
    Set_speed_info;

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������ �� ��������� ������� PK211_XTTK_IMPORT_TMP
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Import_data(
              p_contractor_id IN INTEGER,
              p_xttk_id       IN INTEGER,
              p_agent_id      IN INTEGER,
              p_bank_id       IN INTEGER,
              p_region        IN CONTRACTOR_T.CONTRACTOR%TYPE
          )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_data';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('contractor_id=' ||p_contractor_id||
                          ', xttk_id='     ||p_xttk_id||
                          ', agent_id='    ||p_agent_id||
                          ', bank_id='     ||p_bank_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
                               
    -- ��������� ������ �� ��������� ������� PK211_XTTK_IMPORT_TMP
    INSERT INTO PK211_XTTK_IMPORT_T (
        ERP_CODE,CLIENT,    
        CONTRACT_NO, CONTRACT_DATE,    
        ACCOUNT_NO,    
        INN,KPP,    
        JUR_ZIP, JUR_REGION, JUR_CITY, JUR_ADDRESS,                
        DLV_ZIP, DLV_REGION, DLV_CITY, DLV_ADDRESS,
        EMAIL, BRM_EMAIL,  
        ORDER_NO, ORDER_DATE,SERVICE,SERVICE_ALIAS,
        POINT_SRC,POINT_DST,SPEED, ABP_VALUE,QUANTITY,    
        MANAGER, 
        RATEPLAN,
        SUBSERVICE,
        CHARGE_TYPE,
        NOTES, 
        BRM_CONTRACTOR_ID, BRM_XTTK_ID, BRM_AGENT_ID, BRM_CONTRACTOR_BANK_ID, REGION, 
        LOAD_CODE
    )
    SELECT 
        TRIM(ERP_CODE),
        TRIM(CLIENT),
        TRIM(CONTRACT_NO), 
        TO_DATE(CONTRACT_DATE,'dd.mm.yyyy') CONTRACT_DATE,    
        TRIM(ACCOUNT_NO),    
        TRIM(INN),TRIM(KPP),    
        SUBSTR(TRIM(JUR_ZIP),1,20), TRIM(JUR_REGION), TRIM(JUR_CITY), TRIM(JUR_ADDRESS),
        SUBSTR(TRIM(DLV_ZIP),1,20), TRIM(DLV_REGION), TRIM(DLV_CITY), TRIM(DLV_ADDRESS),
        TRIM(EMAIL), TRIM(EMAIL),
        TRIM(ORDER_NO), 
        TO_DATE(ORDER_DATE,'dd.mm.yyyy'),
        TRIM(SERVICE),TRIM(SERVICE_ALIAS),
        TRIM(POINT_SRC),TRIM(POINT_DST),TRIM(SPEED), 
        TRIM(ABP_VALUE),
        CASE
          WHEN LTRIM(RTRIM(QUANTITY,' '),'1234567890') IS NULL THEN QUANTITY
          ELSE '1'
        END QUANTITY,
        TRIM(MANAGER),
        TRIM(RATEPLAN),
        TRIM(SUBSERVICE),
        TRIM(CHARGE_TYPE),
        NOTES,
        p_contractor_id, p_xttk_id, p_agent_id, p_bank_id, p_region,
        NULL
    FROM PK211_XTTK_IMPORT_TMP;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;

    -- ��������� ����������� ������� ��������������� �������
    Set_ext_data;

    -- ��������� � ������ �������� ������
    UPDATE PK211_XTTK_IMPORT_T X 
       SET X.LOAD_CODE     = c_LOAD_CODE_START,
           X.LOAD_STATUS   = 'IMPORTED',
           X.DATE_IMPORT   = SYSDATE
     WHERE X.BRM_CONTRACTOR_ID = p_contractor_id
       AND X.LOAD_CODE IS NULL
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. PK211_XTTK_IMPORT_T '||v_count||' rows ok', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE Import_data_seller(p_seller_id IN INTEGER)
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_data_seller';
    v_contractor_id  INTEGER;
    v_xttk_id        INTEGER;
    v_agent_id       INTEGER;
    v_bank_id        INTEGER;
    v_region         CONTRACTOR_T.CONTRACTOR%TYPE;
BEGIN
    Pk01_Syslog.Write_msg('Start, seller_id=' ||p_seller_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  
                               
    -- ==========================================================================--
    -- ����� ��������-����������� ����� � ��������
    --
    SELECT SELLER_ID, BRANCH_ID, AGENT_ID, BANK_ID, CONTRACTOR
      INTO v_contractor_id, v_xttk_id, v_agent_id, v_bank_id, v_region
      FROM (
        SELECT BS.SELLER_ID, BS.BRANCH_ID, NULL AGENT_ID, CB.BANK_ID,
               LTRIM(REPLACE(SELLER,'������ ��� ��������� ������������','')) CONTRACTOR,
               ROW_NUMBER() OVER (PARTITION BY CB.CONTRACTOR_ID ORDER BY CB.BANK_ID) RN
          FROM BRANCH_SELLER_TMP BS, 
               CONTRACTOR_BANK_T CB
         WHERE BS.SELLER_ID = p_seller_id
           AND BS.BRANCH LIKE '%(��)'
           AND BS.SELLER_ID = CB.CONTRACTOR_ID
    )
    WHERE RN = 1;

/*  ������������� �������:
    v_contractor_id := 1; 
    v_xttk_id  := 5; 
    v_agent_id := 313; 
    v_bank_id  := 2; 
    v_region  := '��� ��� (��)';
    
    v_contractor_id := 1; 
    v_xttk_id  := 23; 
    v_agent_id := 314; 
    v_bank_id  := 2; 
    v_region  := '����� ��� (��)';
*/
    Import_data(
              p_contractor_id => v_contractor_id,
              p_xttk_id       => v_xttk_id,
              p_agent_id      => v_agent_id,
              p_bank_id       => v_bank_id,
              p_region        => v_region
          );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

/*
PROCEDURE Import_data(p_seller_id IN INTEGER)
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_data';
    v_count          INTEGER := 0;
    v_contractor_id  INTEGER;
    v_xttk_id        INTEGER;
    v_agent_id       INTEGER;
    v_bank_id        INTEGER;
    v_region         CONTRACTOR_T.CONTRACTOR%TYPE;
BEGIN
    Pk01_Syslog.Write_msg('Start, seller_id=' ||p_seller_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  
                               
    -- ==========================================================================--
    -- ����� ��������� � ��������
    SELECT CONTRACTOR_ID, XTTK_ID, AGENT_ID, BANK_ID, CONTRACTOR --, ERP_CODE, CONTRACTOR
      INTO v_contractor_id, v_xttk_id, v_agent_id, v_bank_id, v_region
      FROM (
        SELECT CT.CONTRACTOR_ID, CT.XTTK_ID, CA.CONTRACTOR_ID AGENT_ID, 
               CB.BANK_ID, CT.ERP_CODE, 
               LTRIM(REPLACE(CT.CONTRACTOR,'������ ��� ��������� ������������','')) CONTRACTOR, 
               ROW_NUMBER() OVER (PARTITION BY CT.CONTRACTOR_ID ORDER BY CB.BANK_ID) RN  
          FROM CONTRACTOR_T CT, CONTRACTOR_BANK_T CB, CONTRACTOR_T CA 
         WHERE CT.CONTRACTOR_TYPE  = 'SELLER'
           AND CT.CONTRACTOR_ID = CB.CONTRACTOR_ID
           AND CA.PARENT_ID = CT.CONTRACTOR_ID
           AND CA.CONTRACTOR LIKE '%(��)'
           AND CT.CONTRACTOR_ID = p_seller_id
    )
    WHERE RN = 1;

    -- ������������� �������:
    -- v_contractor_id := 1; 
    -- v_xttk_id  := 5; 
    -- v_agent_id := 313; 
    -- v_bank_id  := 2; 
    -- v_region  := '��� ��� (��)';
    -- 
    -- v_contractor_id := 1; 
    -- v_xttk_id  := 23; 
    -- v_agent_id := 314; 
    -- v_bank_id  := 2; 
    -- v_region  := '����� ��� (��)';
    -- 

    Pk01_Syslog.Write_msg('contractor_id=' ||v_contractor_id||
                          ', xttk_id='  ||v_xttk_id||
                          ', agent_id=' ||v_agent_id||
                          ', bank_id='  ||v_bank_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
                               
    -- ��������� ������ �� ��������� ������� PK211_XTTK_IMPORT_TMP
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
        CONTRACTOR_ID, XTTK_ID, AGENT_ID, CONTRACTOR_BANK_ID, REGION, 
        LOAD_CODE
    )
    SELECT 
        ERP_CODE,CLIENT,    
        CONTRACT_NO, 
        TO_DATE(CONTRACT_DATE,'dd.mm.yyyy') CONTRACT_DATE,    
        NVL(ACCOUNT_NO, CONTRACT_NO) ACCOUNT_NO,    
        INN,KPP,    
        SUBSTR(JUR_ZIP,1,20), JUR_REGION, JUR_CITY, JUR_ADDRESS,
        SUBSTR(DLV_ZIP,1,20), DLV_REGION, DLV_CITY, DLV_ADDRESS,
        LTRIM(REPLACE(ORDER_NO,'�����', '')) ORDER_NO, 
        TO_DATE(ORDER_DATE,'dd.mm.yyyy'),
        SERVICE,SERVICE_ALIAS,
        POINT_SRC,POINT_DST,SPEED, 
        ABP_VALUE,
        CASE
          WHEN LTRIM(RTRIM(QUANTITY,' '),'1234567890') IS NULL THEN QUANTITY
          ELSE '1'
        END QUANTITY,
        MANAGER, NOTES,
        v_contractor_id, v_xttk_id, v_agent_id, v_bank_id, v_region,
        c_LOAD_CODE_START
    FROM PK211_XTTK_IMPORT_TMP;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ������� ������ � SERVICE_T
    UPDATE PK211_XTTK_IMPORT_T X 
       SET  X.LOAD_CODE = c_LOAD_CODE_ERR,
            X.LOAD_STATUS = '������ �� �������'
     WHERE X.CONTRACTOR_ID = p_seller_id
       AND NOT EXISTS (
        SELECT * FROM SERVICE_T S
         WHERE S.SERVICE = X.SERVICE 
     );

    -- ��������� ��������� � �����, � ����� ����� ���� ��� ������
    UPDATE PK211_XTTK_IMPORT_T 
       SET ABP_NUMBER = TO_NUMBER(REPLACE(REPLACE(RTRIM(ABP_VALUE,'�. '),',','.'),' ',''))
     WHERE LTRIM(RTRIM(ABP_VALUE,'�. '),'1234567890., ') IS NULL 
       AND INSTR(ABP_VALUE,',',1,2) = 0
       AND ABP_NUMBER IS NULL
       AND CONTRACTOR_ID = p_seller_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T.ABP_NUMBER '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ������
    UPDATE PK211_XTTK_IMPORT_T X
       SET X.LOAD_CODE   = c_LOAD_CODE_ERR, 
           X.LOAD_STATUS = '�� �������� �������� � ���� ABP_VALUE'
     WHERE ABP_NUMBER IS NULL
       AND CONTRACTOR_ID = p_seller_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T.ABP_NUMBER '||v_count||' rows error', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ����������� �� CONTRACT_NO
    MERGE INTO PK211_XTTK_IMPORT_T X
    USING (
        SELECT DISTINCT C.CONTRACT_ID, C.CONTRACT_NO 
          FROM CONTRACT_T C, PK211_XTTK_IMPORT_T X
         WHERE X.CONTRACT_NO   = C.CONTRACT_NO 
           AND X.LOAD_CODE     = c_LOAD_CODE_START 
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
        SELECT DISTINCT A.ACCOUNT_ID, A.ACCOUNT_NO 
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

    UPDATE PK211_XTTK_IMPORT_T X SET X.LOAD_CODE = c_LOAD_CODE_PROGRESS
     WHERE X.LOAD_CODE = c_LOAD_CODE_START
       AND X.CONTRACTOR_ID = v_contractor_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. PK211_XTTK_IMPORT_T '||v_count||' rows ok', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������ �� PORTAL 6.5 � �������  PK211_XTTK_IMPORT_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Import_data_65(p_seller_id IN INTEGER)
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_data_65';
    v_count          INTEGER := 0;
    v_brand          VARCHAR2(100);
BEGIN
    Pk01_Syslog.Write_msg('Start, seller_id=' ||p_seller_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    IF    p_seller_id = c_msv THEN v_brand := '.Sever TTK RP Brand';      -- \����� ���\������������ �������
    --ELSIF p_seller_id = c_msv THEN v_brand := '.Sever TTK RP Access';     -- \����� ���\������������ ������� (���� ������)
    ELSIF p_seller_id = c_mch THEN v_brand := '.Chita TTK RP Brand';      -- \��� ����\������������ �������
    ELSIF p_seller_id = c_mkl THEN v_brand := '.Kaliningrad TTK RP Brand';-- \����������� ���\������������ �������
    ELSIF p_seller_id = c_msr THEN v_brand := '.Samara TTK RP Brand';     -- \������ ���\������������ �������
    ELSIF p_seller_id = c_mct THEN v_brand := '.Centre TTK RP Brand';     -- \����� ���\������������ �������
    ELSE
        Pk01_Syslog.Write_msg('Unknown seller', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );
        RETURN;
    END IF;

    Pk01_Syslog.Write_msg('brand=' ||v_brand, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ��������� �������
    Clear_tmp;

    -- ��������� ������ �� ��������� �������
    INSERT INTO PK211_XTTK_IMPORT_TMP (
        ERP_CODE,CLIENT,    
        CONTRACT_NO, CONTRACT_DATE,    
        ACCOUNT_NO,    
        INN,KPP,    
        JUR_ZIP, JUR_REGION, JUR_CITY, JUR_ADDRESS,                
        DLV_ZIP, DLV_REGION, DLV_CITY, DLV_ADDRESS,    
        ORDER_NO, ORDER_DATE,SERVICE,SERVICE_ALIAS,
        POINT_SRC,POINT_DST,SPEED, ABP_VALUE,QUANTITY,    
        MANAGER, NOTES
    )
    WITH XC AS (
          SELECT GL_SEGMENT,
                 KONTRAGENT ERP_CODE, 
                 CLIENT CLIENT, 
                 ACCOUNT_NO CONTRACT_NO, 
                 CUSTDATE CONTRACT_DATE, 
                 CUSTNO ACCOUNT_NO, 
                 INN, 
                 KPP, 
                 JUR_ZIP, JUR_CITY, JUR_ADDRESS, 
                 PHIS_ZIP DLV_ZIP, PHIS_CITY DLV_CITY, PHIS_ADDRESS DLV_ADDRESS 
            FROM v_all_contracts@PINDB.WORLD 
           WHERE client_id != 1 
             AND gl_segment like '%RP%'
             AND gl_segment = v_brand
      ), XO AS (
           SELECT  a.gl_segment BRAND, 
                   a.poid_id0   ACCOUNT_POID, 
                   ci.auto_no   CONTRACT_NO, 
                   a.account_no ACCOUNT_NO, 
                   an.company   COMPANY, 
                   s.poid_id0   SERVICE_POID, 
                   s.login      ORDER_NO, 
                   ap.status, 
                   p.name       RATE_PLAN, 
                   d.name       PRODUCT_NAME, 
                   r.event_type, 
                   ap.descr     IP_USAGE_RATE_PLAN,
                   ap.cycle_fee_amt ABP_NUMBER, 
                   A.CURRENCY, 
                   A.CURRENCY_SECONDARY, 
                   i2d@PINDB.WORLD(ap.cycle_start_t) cycle_start_t, 
                   i2d@PINDB.WORLD(ap.cycle_end_t) cycle_end_t,
                   --i2d@PINDB.WORLD(ap.smc_start_t) smc_start_t, 
                   --i2d@PINDB.WORLD(ap.smc_end_t) smc_end_t, 
                   VS.ORDER_DATE, 
                   VS.S_RGN, 
                   VS.D_RGN, 
                   VS.SERVICE_NAME, 
                   VS.SPEED_STR, 
                   VS.FREE_DOWNTIME
              FROM account_t@PINDB.WORLD a 
                   inner join account_products_t@PINDB.WORLD ap on a.poid_id0 = ap.obj_id0
                   inner join account_nameinfo_t@PINDB.WORLD an on a.poid_id0 = an.obj_id0 and an.rec_id = 1
                   inner join plan_t@PINDB.WORLD p on ap.plan_obj_id0 = p.poid_id0
                   inner join product_t@PINDB.WORLD d on ap.product_obj_id0 = d.poid_id0
                   inner join rate_plan_t@PINDB.WORLD r on ap.product_obj_id0 = r.product_obj_id0
                   inner join service_t@PINDB.WORLD s on ap.service_obj_id0 = s.poid_id0
                   inner join profile_t@PINDB.WORLD pr on a.poid_id0 = pr.account_obj_id0
                   inner join contract_info_t@PINDB.WORLD ci on pr.poid_id0 = ci.obj_id0
                   inner join v_all_data_serv_plus@PINDB.WORLD vs on S.POID_ID0 = VS.POID_ID0
             WHERE a.gl_segment like '%RP%' 
                --and A.ACCOUNT_NO not like 'RP%' 
               AND a.poid_id0 <> a.brand_obj_id0
               AND a.gl_segment = v_brand
    )
    SELECT DISTINCT XC.ERP_CODE, XC.CLIENT, XC.CONTRACT_NO, 
           TO_CHAR(XC.CONTRACT_DATE,'dd.mm.yyyy') CONTRACT_DATE, 
           XC.ACCOUNT_NO, XC.INN, XC.KPP,
           XC.JUR_ZIP, NULL JUR_REGION, XC.JUR_CITY, XC.JUR_ADDRESS, 
           XC.DLV_ZIP, NULL DLV_REGION, XC.DLV_CITY, XC.DLV_ADDRESS,
           XO.ORDER_NO, TO_CHAR(XO.ORDER_DATE,'dd.mm.yyyy') ORDER_DATE, 
           XO.SERVICE_NAME SERVICE, NULL SERVICE_ALIAS,
           XO.S_RGN POINT_SRC, 
           XO.D_RGN POINT_DST, 
           XO.SPEED_STR,
           ABP_NUMBER ABP_VALUE,
           1 QUANTITY,
           NULL MANAGER,
           'RP ������������� �� PORTAL 6.5 '||TO_CHAR(SYSDATE,'dd.mm.yyyy') NOTES
      FROM XO, XC
     WHERE XC.ACCOUNT_NO  = XO.ACCOUNT_NO
       AND XC.CONTRACT_NO = XO.CONTRACT_NO;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_TMP '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- PK211_XTTK_IMPORT_TMP -> PK211_XTTK_IMPORT_T
    Import_data_seller(p_seller_id);

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
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
    Import_data_seller(p_seller_id => c_mvv);
END;

-- ������������ ��������
PROCEDURE Import_msh IS
BEGIN
    Import_data_seller(p_seller_id => c_msh);
END;

-- ������������ ������� ������
PROCEDURE Import_mdv IS
BEGIN
    Import_data_seller(p_seller_id => c_mdv);
END;

-- ������������ ���-������
PROCEDURE Import_muv IS
BEGIN
    Import_data_seller(p_seller_id => c_muv);
END;

-- ������������ ����
PROCEDURE Import_mur IS
BEGIN
    Import_data_seller(p_seller_id => c_mur);
END;

-- ������������ ������
PROCEDURE Import_mbl IS
BEGIN
    Import_data_seller(p_seller_id => c_mbl);
END;

-- ������������ ������
PROCEDURE Import_mkz IS
BEGIN
    Import_data_seller(p_seller_id => c_mkz);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������������� �������:
--
-- ������ ��� (��)�
PROCEDURE Import_vlg IS
BEGIN
    Import_data(
              p_contractor_id  => 1564524, --1,
              p_xttk_id        => 314,     -- 23,
              p_agent_id       => NULL,    --314,
              p_bank_id        => 1564525, --2,
              p_region         => '����� ��� (��)'
          );
END;

--
-- ���� ��� (��)�
PROCEDURE Import_sib IS
BEGIN
    Import_data(
              p_contractor_id  => 1,
              p_xttk_id        => 5,
              p_agent_id       => 313,
              p_bank_id        => 2,
              p_region         => '��� ��� (��)'
          );
END;

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
           WHEN p_step = c_LOAD_CODE_ABP THEN '�������� ��������� ����� - ���������'
           WHEN p_step = c_LOAD_CODE_USG THEN '�������� ��������� ����� - ������'
           WHEN p_step = c_LOAD_CODE_FIN THEN '�����'
           ELSE '����������� ���'
         END;
END;

--============================================================================================
-- �������� ������
--============================================================================================
PROCEDURE Load_data
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Load_data';
    v_count           INTEGER := 0;
    v_step            INTEGER := 1;
    v_ok              INTEGER := 0;
    v_error           INTEGER := 0;
    v_account_id      INTEGER;
    v_load_status     VARCHAR2(1000);
   
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR r_abn IN c_FILE_CSV LOOP
      
      BEGIN
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
               ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, CURRENCY_ID, 
               STATUS, PARENT_ID, NOTES,
               BALANCE, BALANCE_DATE, CREATE_DATE, BILLING_ID,
               EXTERNAL_NO
            )VALUES(
               r_abn.brm_account_id, r_abn.brm_account_no, 
               Pk00_Const.c_ACC_TYPE_J, Pk00_Const.c_CURRENCY_RUB, 
               'B', NULL, 
               'ACCOUNT_NO='||r_abn.ACCOUNT_NO||' ������������� �� XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy'), 
               0, SYSDATE, SYSDATE, c_BILLING_XTTK, 
               NVL(r_abn.ACCOUNT_NO, r_abn.contract_no)
            );
            -- ������� ��������� ������ � ������ �������� �����
            Pk07_Bill.New_billinfo (
                         p_account_id    => v_account_id,   -- ID �������� �����
                         p_currency_id   => Pk00_Const.c_CURRENCY_RUB,  -- ID ������ �����
                         p_delivery_id   => 6502,           -- �������� �� �����
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
         WHERE M.MANAGER_ID = r_abn.brm_manager_id
        ;
        IF v_count = 0 AND r_abn.brm_manager_id IS NOT NULL THEN
            INSERT INTO MANAGER_T (
                MANAGER_ID, CONTRACTOR_ID, 
                LAST_NAME, FIRST_NAME, MIDDLE_NAME, 
                DATE_FROM 
            )VALUES(
                r_abn.brm_manager_id, r_abn.brm_contractor_id, 
                r_abn.brm_mgr_last, r_abn.brm_mgr_first, r_abn.brm_mgr_middle, 
                TO_DATE('01.01.2000','dd.mm.yyyy')
            );
        END IF;

        -- -------------------------------------------------------- --
        -- ������� �������
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_CTR;
        SELECT COUNT(*) INTO v_count
          FROM CONTRACT_T C
         WHERE C.CONTRACT_ID = r_abn.brm_contract_id
        ;
        IF v_count = 0 THEN
            INSERT INTO CONTRACT_T (
              CONTRACT_ID, CONTRACT_NO, 
              DATE_FROM, DATE_TO, 
              CLIENT_ID, 
              NOTES
            )VALUES(
              r_abn.brm_contract_id, r_abn.brm_contract_no, 
              r_abn.contract_date, Pk00_Const.c_DATE_MAX, 
              r_abn.brm_client_id, 
              '������������� �� XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );
            -- ����������� ��������� � ��������
            IF r_abn.brm_manager_id IS NOT NULL THEN 
                INSERT INTO SALE_CURATOR_T (MANAGER_ID, CONTRACT_ID, DATE_FROM, DATE_TO)
                VALUES(r_abn.brm_manager_id, r_abn.brm_contract_id, 
                       r_abn.contract_date, Pk00_Const.c_DATE_MAX)
                ;
            END IF;
            -- ����������� ��� �������� � ��������
            INSERT INTO COMPANY_T CM(
              CONTRACT_ID, COMPANY_NAME, SHORT_NAME, DATE_FROM, DATE_TO
            )VALUES(
              r_abn.brm_contract_id, r_abn.client, r_abn.client, 
              r_abn.contract_date, Pk00_Const.c_DATE_MAX
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
            VALUES(r_abn.brm_customer_id, r_abn.erp_code, r_abn.inn, r_abn.kpp, 
                   r_abn.client, r_abn.client, 
                   '������������� �� XTTK '||TO_CHAR(SYSDATE,'dd.mm.yyyy')
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
               CONTRACTOR_BANK_ID, VAT, DATE_FROM, DATE_TO)
            VALUES
               (r_abn.brm_profile_id, 
                r_abn.brm_account_id, r_abn.brm_contract_id, r_abn.brm_customer_id, 
                r_abn.brm_contractor_id, r_abn.brm_xttk_id, r_abn.brm_agent_id, 
                r_abn.brm_contractor_bank_id, Pk00_Const.c_VAT, 
                r_abn.contract_date, NULL
                )
            ;
        END IF;
   
        -- -------------------------------------------------------- --
        -- ������� ����������� �����
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_AJR;
        SELECT COUNT(*) INTO v_count
          FROM ACCOUNT_CONTACT_T AC
         WHERE AC.CONTACT_ID = r_abn.brm_jur_address_id
        ;
        IF v_count = 0 THEN
            INSERT INTO ACCOUNT_CONTACT_T (   
                CONTACT_ID,ADDRESS_TYPE,ACCOUNT_ID,
                COUNTRY,ZIP,STATE,CITY,ADDRESS,DATE_FROM,
                NOTES
            )VALUES(
                r_abn.brm_jur_address_id, PK00_CONST.c_ADDR_TYPE_JUR, r_abn.brm_account_id,
                '��', r_abn.jur_zip, r_abn.jur_region, 
                r_abn.jur_city, r_abn.jur_address, r_abn.contract_date, 
                '������������� �� XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );  
        END IF;
        
        -- -------------------------------------------------------- --
        -- ������� ����� ��������
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_ADL;
        SELECT COUNT(*) INTO v_count
          FROM ACCOUNT_CONTACT_T AC
         WHERE AC.CONTACT_ID = r_abn.brm_dlv_address_id
        ;
        IF v_count = 0 THEN
            INSERT INTO ACCOUNT_CONTACT_T (   
                CONTACT_ID,ADDRESS_TYPE,ACCOUNT_ID,
                COUNTRY,ZIP,STATE,CITY,ADDRESS, EMAIL,
                DATE_FROM,
                NOTES
            )VALUES(
                r_abn.brm_dlv_address_id, PK00_CONST.c_ADDR_TYPE_DLV, r_abn.brm_account_id,
                '��', r_abn.dlv_zip, r_abn.dlv_region, 
                r_abn.dlv_city, r_abn.dlv_address, r_abn.brm_email,
                r_abn.contract_date, 
                '������������� �� XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );  
        END IF;
    
        -- -------------------------------------------------------- --
        -- ������� �����, ������ ���������� �����
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_ORD;
        SELECT COUNT(*) INTO v_count
          FROM ORDER_T O
         WHERE O.ORDER_ID = r_abn.brm_order_id
        ;
        IF v_count = 0 THEN
            -- ������� ������ ������
            INSERT INTO ORDER_T (
               ORDER_ID, ORDER_NO, ACCOUNT_ID, SERVICE_ID, RATEPLAN_ID, 
               DATE_FROM, DATE_TO, 
               CREATE_DATE, MODIFY_DATE, 
               TIME_ZONE, NOTES
            )VALUES(
               r_abn.brm_order_id, r_abn.brm_order_no, r_abn.brm_account_id, r_abn.brm_service_id, NULL,
               NVL(r_abn.order_date, r_abn.contract_date), Pk00_Const.c_DATE_MAX,
               SYSDATE, SYSDATE, NULL, 
               '������������� �� XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );
            -- ��������� ���������� � ������ �����������
            INSERT INTO ORDER_INFO_T( ORDER_ID, POINT_SRC, POINT_DST, 
                                      SPEED_STR, SPEED_VALUE, SPEED_UNIT_ID )
            VALUES( r_abn.brm_order_id, r_abn.POINT_SRC, r_abn.POINT_DST, 
                    SUBSTR(r_abn.SPEED,1,40), r_abn.BRM_SPEED_VALUE, r_abn.BRM_SPEED_UNIT_ID );
            
        END IF;

        -- -------------------------------------------------------- --
        -- ��������� ������������ ������, ��� ��� �������� � ������
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_SAL;
        IF r_abn.SERVICE_ALIAS IS NOT NULL AND r_abn.SERVICE != r_abn.SERVICE_ALIAS THEN
            SELECT COUNT(*) INTO v_count
              FROM SERVICE_ALIAS_T SA
             WHERE SA.ACCOUNT_ID = r_abn.brm_account_id
               AND SA.SERVICE_ID = r_abn.brm_service_id
            ;
            IF v_count = 0 THEN 
                INSERT INTO SERVICE_ALIAS_T (SERVICE_ID, ACCOUNT_ID, SRV_NAME)
                VALUES(r_abn.brm_service_id, r_abn.brm_account_id, r_abn.SERVICE_ALIAS);
            END IF;
        END IF;
    
        -- -------------------------------------------------------- --
        -- ��������� ���������� ������
        -- -------------------------------------------------------- --
        -- ���������
        IF r_abn.BRM_ABP_NUMBER IS NOT NULL 
           AND r_abn.brm_subservice_id IS NOT NULL
           AND r_abn.brm_charge_type = 'REC' 
        THEN
            -- ������� ���������� ������ ���������
            v_step := c_LOAD_CODE_ABP;
            INSERT INTO ORDER_BODY_T(
                ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
                DATE_FROM, DATE_TO, 
                RATE_VALUE, RATE_LEVEL_ID,
                TAX_INCL, CURRENCY_ID,
                QUANTITY, RATE_RULE_ID
            ) VALUES (
                r_abn.brm_order_body_id, r_abn.brm_order_id, 
                r_abn.brm_subservice_id, Pk00_Const.c_CHARGE_TYPE_REC,
                NVL(r_abn.order_date, r_abn.contract_date), Pk00_Const.c_DATE_MAX,
                r_abn.brm_abp_number, Pk00_Const.c_RATE_LEVEL_ORDER, 
                'N', Pk00_Const.c_CURRENCY_RUB, 
                NVL(r_abn.quantity,1), Pk00_Const.c_RATE_RULE_ABP_STD
            );
            --
        ELSIF  r_abn.brm_subservice_id IS NOT NULL
           AND r_abn.brm_charge_type = 'USG' 
           AND r_abn.rateplan IS NULL
        THEN
            -- ������� ���������� ������ ������
            v_step := c_LOAD_CODE_USG;
            INSERT INTO ORDER_BODY_T(
                ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
                DATE_FROM, DATE_TO, 
                RATE_VALUE, RATE_LEVEL_ID,
                TAX_INCL, CURRENCY_ID,
                QUANTITY, RATE_RULE_ID,
                RATEPLAN_ID
            ) SELECT 
                r_abn.brm_order_body_id, r_abn.brm_order_id, 
                r_abn.brm_subservice_id, Pk00_Const.c_CHARGE_TYPE_USG,
                NVL(r_abn.order_date, r_abn.contract_date), Pk00_Const.c_DATE_MAX,
                r_abn.brm_abp_number, Pk00_Const.c_RATE_LEVEL_ORDER, 
                'N', Pk00_Const.c_CURRENCY_RUB, 
                NVL(r_abn.quantity,1), NULL,
                P.RATEPLAN_ID
              FROM RATEPLAN_T P
             WHERE P.RATEPLAN_NAME = r_abn.rateplan;

        END IF;
    
        -- -------------------------------------------------------- --
        -- ������� ������� ����������� �������
        v_ok := v_ok + 1;
        
        UPDATE PK211_XTTK_IMPORT_T
           SET LOAD_CODE   = v_step,
               LOAD_STATUS = 'OK'
         WHERE CURRENT OF c_FILE_CSV;
        
        IF MOD(v_ok, 100) = 0 THEN
            Pk01_Syslog.Write_msg(v_ok||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
      EXCEPTION
         -- -------------------------------------------------------- --
         -- ��������� ������ �������� ������ 
         WHEN OTHERS THEN
            v_load_status := 'ERROR, ��� => '||view_step(v_step)||'. '
                            ||Pk01_Syslog.get_OraErrTxt(c_PkgName||'.'||v_prcName);
            UPDATE PK211_XTTK_IMPORT_T
               SET LOAD_STATUS = v_load_status,
                   LOAD_CODE   = -v_step
             WHERE CURRENT OF c_FILE_CSV;

            Pk01_Syslog.Write_msg('contract_no='||r_abn.CONTRACT_NO||
                                ', account_no=' ||r_abn.ACCOUNT_NO||
                                ', order_no='   ||r_abn.ORDER_NO||
                                ' => '||v_load_status
                                , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );            

            v_error := v_error + 1;
      END;
    END LOOP;

    Pk01_Syslog.Write_msg('Report: '||v_ok||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- �������� ������ � ���������
--============================================================================================
PROCEDURE Load_phones
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Load_phones';
    v_count           INTEGER := 0;
  
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    MERGE INTO PK211_XTTK_IMPORT_PHONES_T XP
    USING (
        SELECT CONTRACT_NO, NVL(ORDER_NO,'NULL') ORDER_NO, BRM_ORDER_ID
          FROM PK211_XTTK_IMPORT_T
    ) X
    ON(
      X.CONTRACT_NO = XP.CONTRACT_NO AND
      X.ORDER_NO    = NVL(XP.ORDER_NO,'NULL') AND
      XP.LOAD_CODE IS NULL
    )
    WHEN MATCHED THEN UPDATE SET XP.BRM_ORDER_ID = X.BRM_ORDER_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('set '||v_count||' order_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    UPDATE PK211_XTTK_IMPORT_PHONES_T XP
       SET XP.LOAD_CODE = c_LOAD_CODE_ERR,
           XP.LOAD_STATUS = '�� ������ �����'
     WHERE XP.LOAD_CODE IS NULL
       AND XP.BRM_ORDER_ID IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - order_id not found', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ��������    
    INSERT INTO ORDER_PHONES_T PH (
      ORDER_ID, PHONE_NUMBER, DATE_FROM, DATE_TO
    )
    SELECT XP.BRM_ORDER_ID, XP.PHONE, XP.DATE_FROM, XP.DATE_TO 
      FROM PK211_XTTK_IMPORT_PHONES_T XP
     WHERE XP.LOAD_CODE IS NULL
       AND XP.BRM_ORDER_ID IS NOT NULL
    ;    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - phones inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

/*
-- ������ ������ ----------------------------------------------------------------
PROCEDURE Load_data
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Load_data';
    v_count           INTEGER := 0;
    v_ok              INTEGER := 0;
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
    v_order_no        ORDER_T.ORDER_NO%TYPE;
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

        IF v_manager_id IS NULL AND v_m_last_name IS NOT NULL THEN
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
               AND AC.ADDRESS_TYPE = PK00_CONST.c_ADDR_TYPE_DLV
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
            -- ��������� �� ������� ������ ������ �� ������ �/�
            SELECT MIN(O.ORDER_ID), COUNT(*) 
              INTO v_order_id, v_count
              FROM ORDER_T O
             WHERE O.ORDER_NO    = r_abn.ORDER_NO
               AND O.ACCOUNT_ID != v_account_id
            ;
            -- �������� ��������� �� ������������ � ��������
            IF v_order_id IS NULL THEN
                v_order_no := r_abn.ORDER_NO;
            ELSE
                v_order_no := v_account_no||'-'||r_abn.ORDER_NO;
                
                -- ��������� �� ������� ������ ������ �� �������� �/�
                SELECT MIN(O.ORDER_ID), COUNT(*) 
                  INTO v_order_id, v_count
                  FROM ORDER_T O
                 WHERE O.ORDER_NO   = v_order_no
                   AND O.ACCOUNT_ID = v_account_id
                ;
                IF v_order_id IS NOT NULL THEN
                   RAISE_APPLICATION_ERROR(e_APP_EXCEPTION, 'DBL.����� �� ��������� �����');
                END IF;
         END IF;
            -- 
            v_order_id := Pk06_Order.New_order(
               p_account_id   => v_account_id,   -- ID �������� �����
               p_order_no     => v_order_no,     -- ����� ������, ��� �� ������
               p_service_id   => v_service_id,   -- ID ������ �� ������� SERVICE_T
               p_rateplan_id  => NULL,           -- ID ��������� ����� �� RATEPLAN_T
               p_time_zone    => NULL,           -- GMT               
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
        VALUES( v_order_id, r_abn.POINT_SRC, r_abn.POINT_DST, SUBSTR(r_abn.SPEED,1,40));
    
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
        v_ok := v_ok + 1;
        
        UPDATE PK211_XTTK_IMPORT_T
           SET LOAD_CODE = c_LOAD_CODE_OK,
               LOAD_STATUS = 'OK'
         WHERE CURRENT OF c_FILE_CSV;
        
        IF MOD(v_ok, 100) = 0 THEN
            Pk01_Syslog.Write_msg(v_ok||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
      EXCEPTION
         -- -------------------------------------------------------- --
         -- ��������� ������ �������� ������ 
         WHEN OTHERS THEN
            v_load_status := Pk01_Syslog.get_OraErrTxt(c_PkgName||'.'||v_prcName);
            UPDATE PK211_XTTK_IMPORT_T
               SET LOAD_STATUS = LOAD_STATUS||'. '||v_load_status,
                   LOAD_CODE   = c_LOAD_CODE_ERR
             WHERE CURRENT OF c_FILE_CSV;

            Pk01_Syslog.Write_msg('account_no='||r_abn.ACCOUNT_NO||
                                  ', order_no='||r_abn.ORDER_NO||
                                  ' => '||v_load_status
                                  , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );            

            v_error := v_error + 1;
      END;
    END LOOP;

    Pk01_Syslog.Write_msg('Report: '||v_ok||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ����� �� ������� �� ��������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
PROCEDURE Report_tmp(p_recordset out t_refc)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Report_tmp';
    v_retcode    INTEGER;
BEGIN
    -- ��������� ������
    OPEN p_recordset FOR
      SELECT  X.BRM_CONTRACT_ID, 
              X.BRM_CONTRACT_NO, 
              X.BRM_ACCOUNT_ID, 
              X.BRM_ACCOUNT_NO, 
              X.BRM_ORDER_ID, 
              X.BRM_ORDER_NO, 
              X.BRM_PROFILE_ID, 
              X.BRM_CLIENT_ID, 
              X.BRM_CUSTOMER_ID, 
              X.BRM_CONTRACTOR_ID, 
              X.BRM_CONTRACTOR_BANK_ID, 
              X.BRM_XTTK_ID, 
              X.BRM_AGENT_ID, 
              X.BRM_JUR_ADDRESS_ID, 
              X.BRM_DLV_ADDRESS_ID, 
              X.BRM_EMAIL, 
              --
              X.BRM_SERVICE_ID, 
              X.BRM_ORDER_BODY_ID, 
              X.BRM_SUBSERVICE_ID, 
              X.BRM_ORDER_BODY_2_ID, 
              X.BRM_ABP_NUMBER, 
              X.BRM_SPEED_VALUE, 
              X.BRM_SPEED_UNIT_ID, 
              --
              X.BRM_MANAGER_ID, 
              X.BRM_MGR_LAST, 
              X.BRM_MGR_FIRST, 
              X.BRM_MGR_MIDDLE
              --
        FROM PK211_XTTK_IMPORT_T X
       WHERE LOAD_CODE = 0
         AND (
              X.BRM_CONTRACT_ID IS NULL OR 
              X.BRM_CONTRACT_NO IS NULL OR 
              X.BRM_ACCOUNT_ID IS NULL OR 
              X.BRM_ACCOUNT_NO IS NULL OR 
              X.BRM_ORDER_ID IS NULL OR 
              X.BRM_ORDER_NO IS NULL OR 
              X.BRM_PROFILE_ID IS NULL OR 
              X.BRM_CLIENT_ID IS NULL OR 
              X.BRM_CUSTOMER_ID IS NULL OR 
              X.BRM_CONTRACTOR_ID IS NULL OR 
              X.BRM_CONTRACTOR_BANK_ID IS NULL OR 
              X.BRM_XTTK_ID IS NULL OR 
              X.BRM_AGENT_ID IS NULL OR 
              X.BRM_JUR_ADDRESS_ID IS NULL OR 
              X.BRM_DLV_ADDRESS_ID IS NULL OR 
              X.BRM_EMAIL IS NULL OR 
              --
              X.BRM_SERVICE_ID IS NULL OR 
              X.BRM_ORDER_BODY_ID IS NULL OR 
              X.BRM_SUBSERVICE_ID IS NULL OR 
              X.BRM_ORDER_BODY_2_ID IS NULL OR 
              X.BRM_ABP_NUMBER IS NULL OR 
              X.BRM_SPEED_VALUE IS NULL OR 
              X.BRM_SPEED_UNIT_ID IS NULL OR 
              --
              X.BRM_MANAGER_ID IS NULL OR 
              X.BRM_MGR_LAST IS NULL OR 
              X.BRM_MGR_FIRST IS NULL OR 
              X.BRM_MGR_MIDDLE IS NULL 
         )
    ;
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ����� � �������� �� ������������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
PROCEDURE Report(p_recordset out t_refc, p_seller_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Report';
    v_retcode    INTEGER;
BEGIN
    -- ��������� ������
    OPEN p_recordset FOR
      SELECT ERP_CODE, CLIENT, CONTRACT_NO, CONTRACT_DATE, ACCOUNT_NO, 
             INN, KPP, 
             JUR_ZIP, JUR_REGION, JUR_CITY, JUR_ADDRESS, 
             DLV_ZIP, DLV_REGION, DLV_CITY, DLV_ADDRESS, 
             ORDER_NO, ORDER_DATE, 
             SERVICE, SERVICE_ALIAS, 
             POINT_SRC, POINT_DST, SPEED, 
             ABP_VALUE, QUANTITY, MANAGER, NOTES, 
             REGION, LOAD_STATUS, LOAD_DATE, LOAD_CODE
        FROM PK211_XTTK_IMPORT_T
       WHERE BRM_CONTRACTOR_ID = p_seller_id
       ORDER BY CLIENT, CONTRACT_NO, ACCOUNT_NO, ORDER_NO
    ;
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- ========================================================================== --
-- � � � � � � � � � � �   � � � � � �
-- ========================================================================== --
PROCEDURE Correct_profile
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Correct_profile';
    v_count      INTEGER := 0;
    v_profile_id INTEGER;
    
    CURSOR c_FILE_ERR IS (
       SELECT 
          CONTRACT_DATE,
          BRM_ACCOUNT_ID, 
          BRM_PROFILE_ID, 
          BRM_CONTRACT_ID, 
          BRM_CUSTOMER_ID,
          BRM_CONTRACTOR_ID,
          BRM_CONTRACTOR_BANK_ID,
          BRM_XTTK_ID,
          BRM_AGENT_ID,
          LOAD_STATUS,
          LOAD_CODE
         FROM PK211_XTTK_IMPORT_T X
        WHERE LOAD_CODE = 2
          AND NOT EXISTS (
            SELECT * FROM ACCOUNT_PROFILE_T AP
             WHERE X.BRM_ACCOUNT_ID = AP.ACCOUNT_ID
               AND X.BRM_PROFILE_ID = AP.PROFILE_ID
          )
    )FOR UPDATE;
    
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR r_abn IN c_FILE_ERR LOOP
        IF r_abn.BRM_PROFILE_ID IS NOT NULL AND r_abn.LOAD_CODE = 2 THEN
            --
            v_profile_id := Pk05_Account.Set_profile(
                 p_account_id         => r_abn.brm_account_id,
                 p_brand_id           => NULL,
                 p_contract_id        => r_abn.brm_contract_id,
                 p_customer_id        => r_abn.brm_customer_id,
                 p_subscriber_id      => NULL,
                 p_contractor_id      => r_abn.brm_CONTRACTOR_ID,
                 p_branch_id          => r_abn.brm_XTTK_ID,
                 p_agent_id           => r_abn.brm_AGENT_ID,
                 p_contractor_bank_id => r_abn.brm_CONTRACTOR_BANK_ID,
                 p_vat                => Pk00_Const.c_VAT,
                 p_date_from          => r_abn.CONTRACT_DATE,
                 p_date_to            => NULL
             );
            -- ��������� ID
            UPDATE PK211_XTTK_IMPORT_T
               SET BRM_PROFILE_ID = v_profile_id
             WHERE CURRENT OF c_FILE_ERR;
            --
            v_count := v_count + 1;
            --
        END IF;

    END LOOP;
    
    Pk01_Syslog.Write_msg(v_count||'rows processed', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
--                  � � � � � � � � �     � � � � � � � � � 
--============================================================================================
PROCEDURE Run_DDL(p_ddl IN VARCHAR2) IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Run_DDL';
BEGIN
    Pk01_Syslog.Write_msg(p_ddl, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    EXECUTE IMMEDIATE p_ddl;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.Write_error('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  ACCOUNT_PROFILE_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Account_profile_t_drop_fk
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    v_prcName       CONSTANT VARCHAR2(30) := 'Account_profile_t_drop_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Run_DDL('ALTER TABLE PIN.ACCOUNT_PROFILE_T DROP 
      CONSTRAINT ACCOUNT_PROFILE_ACCOUNT_T_FK');
      
    Run_DDL('ALTER TABLE PIN.ACCOUNT_PROFILE_T DROP 
      CONSTRAINT ACCOUNT_PROFILE_BANK_T_FK');
      
    Run_DDL('ALTER TABLE PIN.ACCOUNT_PROFILE_T DROP 
      CONSTRAINT ACCOUNT_PROFILE_BRAND_T_FK');
      
    Run_DDL('ALTER TABLE PIN.ACCOUNT_PROFILE_T DROP 
      CONSTRAINT ACCOUNT_PROFILE_CONTRACT_T_FK');
      
    Run_DDL('ALTER TABLE PIN.ACCOUNT_PROFILE_T DROP
      CONSTRAINT ACCOUNT_PROFILE_CUSTOMER_T_FK');
      
    Run_DDL('ALTER TABLE PIN.ACCOUNT_PROFILE_T DROP
      CONSTRAINT ACCOUNT_PROFILE_SUBS_T_FK');
      
    Run_DDL('ALTER TABLE PIN.ACCOUNT_PROFILE_T DROP 
      CONSTRAINT ACC_PROFILE_CONTRACTOR_T_FK');
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
PROCEDURE Account_profile_t_add_fk
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    v_prcName       CONSTANT VARCHAR2(30) := 'Account_profile_t_add_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    Run_DDL('ALTER TABLE PIN.ACCOUNT_PROFILE_T ADD 
      CONSTRAINT ACCOUNT_PROFILE_ACCOUNT_T_FK 
      FOREIGN KEY (ACCOUNT_ID) 
      REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
      ENABLE VALIDATE');
      
    Run_DDL('ALTER TABLE PIN.ACCOUNT_PROFILE_T ADD 
      CONSTRAINT ACCOUNT_PROFILE_BANK_T_FK 
      FOREIGN KEY (CONTRACTOR_BANK_ID, CONTRACTOR_ID) 
      REFERENCES PIN.CONTRACTOR_BANK_T (BANK_ID,CONTRACTOR_ID)
      ENABLE VALIDATE');
      
    Run_DDL('ALTER TABLE PIN.ACCOUNT_PROFILE_T ADD 
      CONSTRAINT ACCOUNT_PROFILE_BRAND_T_FK 
      FOREIGN KEY (BRAND_ID) 
      REFERENCES PIN.BRAND_T (BRAND_ID)
      ENABLE VALIDATE');
      
    Run_DDL('ALTER TABLE PIN.ACCOUNT_PROFILE_T ADD 
      CONSTRAINT ACCOUNT_PROFILE_CONTRACT_T_FK 
      FOREIGN KEY (CONTRACT_ID) 
      REFERENCES PIN.CONTRACT_T (CONTRACT_ID)
      ENABLE VALIDATE');
      
    Run_DDL('ALTER TABLE PIN.ACCOUNT_PROFILE_T ADD
      CONSTRAINT ACCOUNT_PROFILE_CUSTOMER_T_FK 
      FOREIGN KEY (CUSTOMER_ID) 
      REFERENCES PIN.CUSTOMER_T (CUSTOMER_ID)
      ENABLE VALIDATE');
      
    Run_DDL('ALTER TABLE PIN.ACCOUNT_PROFILE_T ADD
      CONSTRAINT ACCOUNT_PROFILE_SUBS_T_FK 
      FOREIGN KEY (SUBSCRIBER_ID) 
      REFERENCES PIN.SUBSCRIBER_T (SUBSCRIBER_ID)
      ENABLE VALIDATE');
      
    Run_DDL('ALTER TABLE PIN.ACCOUNT_PROFILE_T ADD 
      CONSTRAINT ACC_PROFILE_CONTRACTOR_T_FK 
      FOREIGN KEY (CONTRACTOR_ID) 
      REFERENCES PIN.CONTRACTOR_T (CONTRACTOR_ID)
      ENABLE VALIDATE');

    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  BILLINFO_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Billinfo_t_drop_fk
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    v_prcName       CONSTANT VARCHAR2(30) := 'Billinfo_t_drop_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Run_DDL('ALTER TABLE PIN.BILLINFO_T DROP CONSTRAINT BILLINFO_T_ACCOUNT_T_FK');
    Run_DDL('ALTER TABLE PIN.BILLINFO_T DROP CONSTRAINT BILLINFO_T_CURRENCY_T_FK');
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
PROCEDURE Billinfo_t_add_fk
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    v_prcName       CONSTANT VARCHAR2(30) := 'Billinfo_t_add_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    Run_DDL( 'ALTER TABLE PIN.BILLINFO_T ADD 
              CONSTRAINT BILLINFO_T_ACCOUNT_T_FK 
              FOREIGN KEY (ACCOUNT_ID) 
              REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
              ENABLE VALIDATE');

    Run_DDL( 'ALTER TABLE PIN.BILLINFO_T ADD
              CONSTRAINT BILLINFO_T_CURRENCY_T_FK 
              FOREIGN KEY (CURRENCY_ID) 
              REFERENCES PIN.CURRENCY_T (CURRENCY_ID)
              ENABLE VALIDATE');

    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- =========================================================== --
-- ������������ ������
-- =========================================================== --
-- ------------------------------------------------------------------------- --
-- ������� ����� ��� ���������� ������� (�� ���� ����������) 
-- ------------------------------------------------------------------------- --
PROCEDURE Create_bills( p_period_id IN INTEGER, p_branch_id IN INTEGER )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Create_bills';
    v_count   INTEGER := 0;
    v_bill_id INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������� ��������� ������ ��� �� ���
    v_count := 0;
    FOR bi IN (
      SELECT DISTINCT A.ACCOUNT_ID  
        FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A
       WHERE AP.BRANCH_ID  = p_branch_id
         AND AP.ACCOUNT_ID = A.ACCOUNT_ID
         AND A.BILLING_ID = 2008
         AND NOT EXISTS (
            SELECT * FROM BILLINFO_T BI
             WHERE BI.ACCOUNT_ID = A.ACCOUNT_ID
         )
    )
    LOOP
       -- ������� ��������� ������ � ������ �������� �����
       Pk07_Bill.New_billinfo (
                   p_account_id    => bi.account_id,   -- ID �������� �����
                   p_currency_id   => Pk00_Const.c_CURRENCY_RUB,  -- ID ������ �����
                   p_delivery_id   => c_DLV_METHOD_AP,-- ID ������� �������� �����
                   p_days_for_payment => 30           -- ���-�� ���� �� ������ �����
               );  
    
       v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('Billinfo_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
   
    -- ������� ������������� �����
    v_count := 0;
    FOR aid IN (
      SELECT DISTINCT A.ACCOUNT_ID  
        FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A
       WHERE AP.BRANCH_ID  = p_branch_id
         AND AP.ACCOUNT_ID = A.ACCOUNT_ID
         AND A.BILLING_ID = 2008
         AND NOT EXISTS (
            SELECT * FROM BILL_T B
             WHERE B.ACCOUNT_ID    = A.ACCOUNT_ID
               AND B.REP_PERIOD_ID = p_period_id
               AND B.BILL_TYPE     = PK00_CONST.c_BILL_TYPE_REC
         )
    )
    LOOP
        v_bill_id := Pk07_BIll.Next_recuring_bill (
                 p_account_id    => aid.account_id, -- ID �������� �����
                 p_rep_period_id => p_period_id     -- ID ���������� ������� YYYYMM
             );
        v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('Bill_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- ������������ ����� �������� ��.���, ���������� ������� �������� (�� ���� ����������) 
-- ------------------------------------------------------------------------- --
PROCEDURE Make_bills( p_period_id IN INTEGER, p_branch_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_bills';
    v_task_id    INTEGER;
    v_count      INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������������ ��� �/�
    UPDATE ACCOUNT_T A SET STATUS = 'B'
     WHERE A.BILLING_ID = 2008
       AND A.STATUS != 'B'
       AND EXISTS (
        SELECT *
          FROM ACCOUNT_PROFILE_T AP
         WHERE AP.ACCOUNT_ID    = A.ACCOUNT_ID
           AND AP.BRANCH_ID     = p_branch_id
           AND A.BILLING_ID     = 2008
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Account_t: '||v_count||' rows set status B', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ��������� ������
    v_task_id := Pk30_Billing_Queue.Open_task;
    
    -- ������� ������� ��� ������������ ������
    INSERT INTO BILLING_QUEUE_T
    SELECT B.BILL_ID, B.ACCOUNT_ID, NULL ORDER_ID, v_task_id TASK_ID, B.REP_PERIOD_ID, B.REP_PERIOD_ID
      FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A, BILL_T B
     WHERE AP.ACCOUNT_ID    = A.ACCOUNT_ID
       AND AP.BRANCH_ID     = p_branch_id
       AND A.ACCOUNT_ID     = B.ACCOUNT_ID
       AND B.REP_PERIOD_ID  = p_period_id
       AND A.ACCOUNT_ID     = 2008
       AND EXISTS (
          SELECT * FROM BILL_T B
           WHERE B.REP_PERIOD_ID = p_period_id
             AND B.ACCOUNT_ID = A.ACCOUNT_ID
       );
    v_count := SQL%ROWCOUNT;    
    Pk01_Syslog.Write_msg('Billing_Queue_t: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    
    -- ���������������� �����, ��� ��������� ��������� ����������
    Pk30_Billing.Rollback_billing( v_task_id );

    -- �������������� ������, � ��������� ����������� ������������� ����������
    Pk30_Billing.Remake_billing( v_task_id );
    
    -- ��������� ������
    Pk30_Billing_Queue.Close_task(v_task_id);
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- =========================================================== --
-- �����
-- =========================================================== --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ����������� ������ ��� �������� 2008, 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_bills(p_period_id IN INTEGER, p_branch_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_bills';
    v_count         INTEGER;
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Start, bill_period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� �����
    --
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_period_id
       AND EXISTS (
           SELECT * 
             FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
            WHERE A.BILLING_ID     = 2008
              AND A.ACCOUNT_ID     = AP.ACCOUNT_ID
              AND A.ACCOUNT_ID     = B.ACCOUNT_ID
              AND B.REP_PERIOD_ID  = p_period_id
              AND I.BILL_ID        = B.BILL_ID
              AND AP.BRANCH_ID     = p_branch_id
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('item_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
       
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ������ ������
    DELETE FROM INVOICE_ITEM_T II
     WHERE II.REP_PERIOD_ID = p_period_id
       AND EXISTS (
           SELECT *
             FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
            WHERE A.BILLING_ID     = 2008
              AND A.ACCOUNT_ID     = AP.ACCOUNT_ID
              AND A.ACCOUNT_ID     = B.ACCOUNT_ID
              AND B.REP_PERIOD_ID  = p_period_id
              AND II.BILL_ID       = B.BILL_ID
              AND AP.BRANCH_ID     = p_branch_id
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('invoice_item_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������������ ����� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    DELETE FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_period_id
       AND EXISTS (
           SELECT *
             FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
            WHERE A.BILLING_ID     = 2008
              AND A.ACCOUNT_ID     = AP.ACCOUNT_ID
              AND A.ACCOUNT_ID     = B.ACCOUNT_ID
              AND B.REP_PERIOD_ID  = p_period_id
              AND B.BILL_ID        = B.BILL_ID
              AND AP.BRANCH_ID     = p_branch_id
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('bill_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ����������� ������, �� ������ ������� PK211_XTTK_IMPORT_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_data(p_branch_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_data';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���������� ������ 
    DELETE FROM ORDER_BODY_T OB
     WHERE EXISTS (
        SELECT * 
          FROM ORDER_T O, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE A.BILLING_ID = 2008 
           AND O.ACCOUNT_ID = A.ACCOUNT_ID
           AND O.ORDER_ID   = OB.ORDER_ID
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AP.BRANCH_ID = p_branch_id
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('order_body_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    UPDATE PK211_XTTK_IMPORT_T X SET X.BRM_ORDER_BODY_ID = NULL
     WHERE NOT EXISTS (
        SELECT * FROM ORDER_BODY_T OB
         WHERE OB.ORDER_BODY_ID = X.BRM_ORDER_BODY_ID
       )
       AND X.BRM_AGENT_ID = p_branch_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('x.order_body_t '||v_count||' rows set null', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE PK211_XTTK_IMPORT_T X SET X.BRM_ORDER_BODY_2_ID = NULL
     WHERE NOT EXISTS (
        SELECT * FROM ORDER_BODY_T OB
         WHERE OB.ORDER_BODY_ID = X.BRM_ORDER_BODY_2_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('x.order_body_2_t '||v_count||' rows set null', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���.���������� � ������ 
    DELETE FROM ORDER_INFO_T OI
     WHERE EXISTS (
        SELECT * 
          FROM ORDER_T O, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE A.BILLING_ID = 2008 
           AND O.ACCOUNT_ID = A.ACCOUNT_ID
           AND O.ORDER_ID   = OI.ORDER_ID
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AP.BRANCH_ID = p_branch_id
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('order_body_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
     
    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������
    DELETE FROM ORDER_T O
     WHERE EXISTS (
        SELECT * 
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE A.BILLING_ID = 2008 
           AND O.ACCOUNT_ID = A.ACCOUNT_ID
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AP.BRANCH_ID = p_branch_id
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('order_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE PK211_XTTK_IMPORT_T X SET X.BRM_ORDER_ID = NULL
     WHERE NOT EXISTS (
        SELECT * FROM ORDER_T OB
         WHERE OB.ORDER_ID = X.BRM_ORDER_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('x.order_body_t '||v_count||' rows set null', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������������� ����� ��� ��������
    DELETE
      FROM SERVICE_ALIAS_T SA
     WHERE EXISTS (
        SELECT * 
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE A.BILLING_ID = 2008 
           AND A.ACCOUNT_ID = SA.ACCOUNT_ID
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AP.BRANCH_ID = p_branch_id
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('service_alias_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������
    DELETE 
      FROM ACCOUNT_CONTACT_T AD
     WHERE EXISTS (
        SELECT * 
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE A.BILLING_ID = 2008 
           AND A.ACCOUNT_ID = AD.ACCOUNT_ID
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AP.BRANCH_ID = p_branch_id
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('account_contact_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE PK211_XTTK_IMPORT_T X SET X.BRM_DLV_ADDRESS_ID = NULL
     WHERE NOT EXISTS (
        SELECT * FROM ACCOUNT_CONTACT_T AD
         WHERE AD.CONTACT_ID = X.BRM_DLV_ADDRESS_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('x.dlv_address_id '||v_count||' rows set null', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE PK211_XTTK_IMPORT_T X SET X.BRM_JUR_ADDRESS_ID = NULL
     WHERE NOT EXISTS (
        SELECT * FROM ACCOUNT_CONTACT_T AD
         WHERE AD.CONTACT_ID = X.BRM_JUR_ADDRESS_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('x.jur_address_id '||v_count||' rows set null', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �������� ����������
    DELETE FROM ACCOUNT_DOCUMENTS_T AD
     WHERE EXISTS (
        SELECT * 
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE A.BILLING_ID = 2008 
           AND A.ACCOUNT_ID = AD.ACCOUNT_ID
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AP.BRANCH_ID = p_branch_id
    );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� �� �/c
    DELETE 
      FROM REP_PERIOD_INFO_T RP
     WHERE EXISTS (
        SELECT * 
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE A.BILLING_ID = 2008 
           AND A.ACCOUNT_ID = RP.ACCOUNT_ID
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AP.BRANCH_ID = p_branch_id
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('rep_period_info_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ��������� ������
    
    Billinfo_t_drop_fk;
    
    DELETE 
      FROM BILLINFO_T BI
     WHERE EXISTS (
        SELECT * 
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP 
         WHERE A.BILLING_ID = 2008 
           AND A.ACCOUNT_ID = BI.ACCOUNT_ID
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AP.BRANCH_ID = p_branch_id
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('billinfo_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Billinfo_t_add_fk;

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� �/�
    
    Account_profile_t_drop_fk;
    
    DELETE 
      FROM ACCOUNT_PROFILE_T P
     WHERE EXISTS (
        SELECT * 
          FROM ACCOUNT_T A
         WHERE A.BILLING_ID = 2008 
           AND A.ACCOUNT_ID = P.ACCOUNT_ID
       )
       AND P.BRANCH_ID = p_branch_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('account_profile_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE PK211_XTTK_IMPORT_T X SET X.BRM_PROFILE_ID = NULL
     WHERE NOT EXISTS (
        SELECT * FROM ACCOUNT_PROFILE_T AP
         WHERE AP.PROFILE_ID = X.BRM_PROFILE_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('x.profile_id '||v_count||' rows set null', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Account_profile_t_add_fk;

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �/�
    DELETE 
      FROM ACCOUNT_T A
     WHERE A.BILLING_ID = 2008
       AND EXISTS (
           SELECT * 
             FROM PK211_XTTK_IMPORT_T X
            WHERE X.BRM_ACCOUNT_ID = A.ACCOUNT_ID
              AND X.BRM_PROFILE_ID IS NULL
       )
       ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('account_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE PK211_XTTK_IMPORT_T X SET X.BRM_ACCOUNT_ID = NULL
     WHERE NOT EXISTS (
        SELECT * FROM ACCOUNT_T A
         WHERE A.ACCOUNT_ID = X.BRM_ACCOUNT_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('x.profile_id '||v_count||' rows set null', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���� ����������
    DELETE FROM CUSTOMER_BANK_T CB
     WHERE EXISTS (
           SELECT * FROM PK211_XTTK_IMPORT_T X
            WHERE X.BRM_CUSTOMER_ID = CB.CUSTOMER_ID
              AND X.BRM_ACCOUNT_ID IS NULL
       )
       AND NOT EXISTS (
           SELECT * FROM ACCOUNT_PROFILE_T AP
            WHERE AP.CUSTOMER_ID = CB.CUSTOMER_ID
       );

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���������� �� �/�
    DELETE FROM CUSTOMER_T CS
     WHERE EXISTS (
           SELECT * FROM PK211_XTTK_IMPORT_T X
            WHERE X.BRM_CUSTOMER_ID = CS.CUSTOMER_ID
              AND X.BRM_ACCOUNT_ID IS NULL
       )
       AND NOT EXISTS (
           SELECT * FROM ACCOUNT_PROFILE_T AP
            WHERE AP.CUSTOMER_ID = CS.CUSTOMER_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('custmer_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );


    UPDATE PK211_XTTK_IMPORT_T X SET X.BRM_CUSTOMER_ID = NULL
     WHERE NOT EXISTS (
        SELECT * FROM CUSTOMER_T CS
         WHERE CS.CUSTOMER_ID = X.BRM_CUSTOMER_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('x.customer_id '||v_count||' rows set null', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- � � � � � � � �
    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    /*
    SELECT DISTINCT CONTRACT_ID 
      BULK COLLECT INTO a_contract_id
      FROM PK211_XTTK_IMPORT_T X;
      
    SELECT DISTINCT CLIENT_ID 
      BULK COLLECT INTO a_client_id
      FROM PK211_XTTK_IMPORT_T X;
    
    FORALL id in a_contract_id.first..a_contract_id.last
    DELETE FROM SALE_CURATOR_T SC
     WHERE SC.CONTRACT_ID = a_contract_id(id);
    */

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �������� ��������
    -- PS: ����� ���������� �� �������
    DELETE FROM SALE_CURATOR_T SC
     WHERE EXISTS (
       SELECT * FROM PK211_XTTK_IMPORT_T X
        WHERE X.BRM_CONTRACT_ID = SC.CONTRACT_ID
          AND X.BRM_ACCOUNT_ID IS NULL
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('sale_curator_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� �� ��������
    DELETE FROM COMPANY_T CM
     WHERE EXISTS (
           SELECT * FROM PK211_XTTK_IMPORT_T X
            WHERE X.BRM_CONTRACT_ID = CM.CONTRACT_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('company_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �������
    DELETE FROM CONTRACT_T C
     WHERE EXISTS (
           SELECT * FROM PK211_XTTK_IMPORT_T X
            WHERE X.BRM_CONTRACT_ID = C.CONTRACT_ID
              AND X.BRM_ACCOUNT_ID IS NULL
       )
       AND NOT EXISTS (
           SELECT * FROM ACCOUNT_PROFILE_T AP
            WHERE C.CONTRACT_ID = AP.CONTRACT_ID
       ); 
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('contract_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    UPDATE PK211_XTTK_IMPORT_T X SET X.BRM_CONTRACT_ID = NULL
     WHERE NOT EXISTS (
        SELECT * FROM CONTRACT_T C
         WHERE C.CONTRACT_ID = X.BRM_CONTRACT_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('x.contract_id '||v_count||' rows set null', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���������� �� ��������
    --
    DELETE FROM CLIENT_T CL
     WHERE EXISTS (
           SELECT * FROM PK211_XTTK_IMPORT_T X
            WHERE X.BRM_CLIENT_ID = CL.CLIENT_ID
              AND X.BRM_CONTRACT_ID IS NULL
       )
       AND NOT EXISTS (
           SELECT * FROM CONTRACT_T C
            WHERE C.CLIENT_ID = CL.CLIENT_ID
       ); 
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('client_t '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    UPDATE PK211_XTTK_IMPORT_T X SET X.BRM_CLIENT_ID = NULL
     WHERE NOT EXISTS (
        SELECT * FROM CONTRACT_T C
         WHERE C.CLIENT_ID = X.BRM_CLIENT_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('x.client_id '||v_count||' rows set null', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    ROLLBACK;
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
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
    --INSERT INTO PK211_XTTK_IMPORT_T_ARX
    --SELECT * FROM PK211_XTTK_IMPORT_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T_ARX '||v_count||' - rows insert', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - -- - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;



END PK211_XTTK_IMPORT_CSV;
/
