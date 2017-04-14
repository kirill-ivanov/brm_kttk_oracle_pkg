CREATE OR REPLACE PACKAGE PK214_IMPORT_TTK_URAL
IS
    --
    -- ����� ��� �������� �������� ��� ��� ������ xTTK-����
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK213_IMPORT_TTK_URAL';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- =====================================================================
    -- ������� � ������� ��������� ��������, ��� �������� ����� ��������
    c_MAX_DATE_TO         CONSTANT DATE := TO_DATE('01.01.2050','dd.mm.yyyy');
    c_BILLING_ID          CONSTANT INTEGER := 2007;
    c_CLIENT_ID           CONSTANT INTEGER := 1;       -- ���.����
    c_CUSTOMER_ID         CONSTANT INTEGER := 1;       -- ���.����
    c_CONTRACTOR_ID       CONSTANT INTEGER := 1;       -- ��� � 2003 �������� 
    c_CONTRACTOR_BANK_ID  CONSTANT INTEGER := 2;       -- ��� � 2003 ��������
    c_BRANCH_ID           CONSTANT INTEGER := 337;     -- ������ ������������ ���� (��-������)
    c_RATEPLAN_ID         CONSTANT INTEGER := 91293;   -- "��_���_������������_21.03.2016"
    c_SERVICE_ID          CONSTANT INTEGER := 1;       -- ������ ������������� � ������������� ���������� �����
    c_SUBSERVICE_MG_ID    CONSTANT INTEGER := 1;
    c_SUBSERVICE_MN_ID    CONSTANT INTEGER := 2;
    c_BRM_CURRENCY_ID     CONSTANT INTEGER := 810;     -- �����
    c_BRM_CURRENCY_CONVERSION_ID CONSTANT INTEGER := 2601;
    c_BRM_DELIVERY_METHOD_ID CONSTANT INTEGER   := 6512; -- ������ ����
    c_BRM_VAT             CONSTANT INTEGER      := 18;
    c_BRM_TAX_INCL        CONSTANT VARCHAR2(1)  := 'Y';
    c_BRM_PHONE_PREFIX    CONSTANT VARCHAR2(10) := '7343';
    -- =====================================================================
        
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
    c_LOAD_CODE_SUB_MG    CONSTANT INTEGER :=10;    -- ������ service-alias
    c_LOAD_CODE_SUB_MN    CONSTANT INTEGER :=11;    -- �������� �������� �����
    c_LOAD_CODE_FIN       CONSTANT INTEGER :=12;    -- �����

    -- ������ �������� ����������
    c_DLV_METHOD_AP CONSTANT INTEGER := 6512;   -- ����������
    
    -- ��������� ������������
    �_RATESYS_ABP_ID CONSTANT INTEGER := 1207; -- ����������� ������ ���������, ����� � ORDER_BODY_T
    -- ��������� �������� ������
    c_RATEPLAN_NPL_RUR        CONSTANT INTEGER := 80043; -- 'NPL_RUR'
    c_RATEPLAN_NPL            CONSTANT INTEGER := 80045; -- 'NPL'
    c_RATEPLAN_RRW_RUR        CONSTANT INTEGER := 80046; -- 'RRW RUR'
    c_RATEPLAN_IP_ROUTING_RUR CONSTANT INTEGER := 80047; -- 'IP Routing RUR'
    
    c_LDSTAT_DBL_ORD CONSTANT INTEGER := -1;   -- ����� ��� ���� � BRM
    c_LDSTAT_NOT_SRV CONSTANT INTEGER := -2;   -- ������ �� �������
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ����������, ��� ��������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Bind_clients( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����� ����� �������� ��� �������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Clean_bind_clients( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ����������, ��� ��������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Bind_phones( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����� ����� �������� ��� ��������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Clean_bind_phones( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� � �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Bind_balance( p_task_id IN INTEGER, p_balance_date IN DATE );
    
    --============================================================================================
    -- �������� ������ �� ������� ������
    PROCEDURE Load_accounts( p_task_id IN INTEGER );

    -- �������� ������ �� �������
    PROCEDURE Load_orders( p_task_id IN INTEGER );
        
    -- �������� ���������� �������
    PROCEDURE Load_phones(p_task_id IN INTEGER);
    
    -- �������� �������� �������� �� ��������� � ������� ������ ����
    PROCEDURE Load_balance(p_task_id IN INTEGER);

    --============================================================================================
    -- ��������� ����������� ������ � �����
    --============================================================================================
    --PROCEDURE Move_to_archive;
    
    --============================================================================================
    -- ��������� ����������� � BRM ��������� � ��������-��������� ���������� 01.10.2016
    --
    PROCEDURE Loaded_abonets_vs_osb (
                   p_recordset    OUT t_refc 
               );
    
    --============================================================================================
    -- ��������� ������ �� ������, ���� ����� ������ ��������� ����� � ���������
    -- ��� ������� �������!!!
    --============================================================================================
    PROCEDURE Check_balans_for_period (
                   p_recordset    OUT t_refc 
               );
    
    
    -- =========================================================== --
    -- ������������ ������
    -- =========================================================== --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������������� ������ ��� �������� �������
    -- ��������� ��� ������� �� ����������� ���������
    -- � ����������� ������� p_period_id
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --PROCEDURE Create_new_bills(p_period_id IN INTEGER);
    
    -- ------------------------------------------------------------------------- --
    -- ������������ ����� �������� ��.��� �������� (�� ���� ����������) 
    -- ------------------------------------------------------------------------- --
    --PROCEDURE Make_bills( p_period_id IN INTEGER );
    
    -- ----------------------------------------------------------------------------- --
    -- �������� ������ ������� ������
    CURSOR c_CTR(p_task_id INTEGER, p_load_status INTEGER) IS (
      SELECT
          PERSON_ID, 
          BASE_ACCOUNT_ID, 
          BASE_ACCOUNT_NUMBER, 
          BASE_ACCOUNT_NOTIF_EMAIL, 
          TTK_ACCOUNT_ID, 
          TTK_ACCOUNT_NUMBER, 
          CONTRACT_NUMBER, 
          CONTRACT_DATE, 
          LAST_NAME, FIRST_NAME, SECOND_NAME, 
          PASSPORT_SERIES, PASSPORT_NUMBER, PASSPORT_DATE, PASSPORT_ISSUER, 
          ZIP, COUNTRY, STATE, CITY, STREET, NUM, BUILDING, BLOCK, FLAT, 
          ZIP_REG, COUNTRY_REG, STATE_REG, CITY_REG, STREET_REG, NUM_REG, BUILDING_REG, BLOCK_REG, FLAT_REG, 
          BRM_CONTRACT_ID, BRM_CONTRACT_NO, BRM_CONTRACT_DATE,
          BRM_ACCOUNT_ID, BRM_ACCOUNT_NO, BRM_PROFILE_ID, 
          BRM_SUBSCRIBER_ID, BRM_DLV_ADDR_ID, BRM_REG_ADDR_ID, 
          BRM_CONTRACTOR_ID, BRM_CONTRACTOR_BANK_ID, BRM_BRANCH_ID, 
          BRM_ORDER_ID, BRM_ORDER_NO, BRM_SERVICE_ID, BRM_RATEPLAN_ID, 
          BRM_OB_MG_ID, BRM_OB_MN_ID, 
          LOAD_STATUS, LOAD_NOTES, STATUS_DATE, TASK_ID, BRM_PASSPORT_DATE 
        FROM PK214_XTTK_URAL_F 
       WHERE LOAD_STATUS = p_load_status
         AND TASK_ID     = p_task_id
      ) FOR UPDATE;
        
    -- �������� �� ������� ������    
    CURSOR c_PHONES(p_task_id INTEGER) IS (
      SELECT ACCOUNT_NO, PHONE, RATEPLAN, 
             BRM_ACCOUNT_ID, BRM_ACCOUNT_NO, 
             BRM_ORDER_ID, BRM_DATE_FROM, BRM_DATE_TO, BRM_PHONE, 
             LOAD_STATUS, LOAD_NOTES, STATUS_DATE, TASK_ID  
        FROM PK214_XTTK_URAL_PHONE_F
       WHERE LOAD_STATUS = 0 
         AND TASK_ID     = p_task_id
    )FOR UPDATE;
    
    -- �������� �������
    CURSOR c_BAL(p_task_id INTEGER) IS (
      SELECT UB.BRM_ACCOUNT_NO, 
             UB.BRM_ACCOUNT_ID, 
             UB.BRM_BALANCE_DATE, 
             UB.BRM_BALANCE, 
             UB.LOAD_STATUS, 
             UB.LOAD_NOTES, 
             UB.STATUS_DATE 
        FROM PK214_XTTK_URAL_BALANCE UB
       WHERE UB.LOAD_STATUS = 0
         AND UB.TASK_ID     = p_task_id
    )FOR UPDATE;
    
    -- ������� �� �/�
    CURSOR c_PAY(p_task_id INTEGER) IS (
      SELECT UP.BRM_ACCOUNT_NO,
             UP.BRM_ACCOUNT_ID,
             UP.BRM_PAYMENT_DATE,
             UP.BRM_PAY_AMOUNT,
             UP.PAYMENT_TYPE,
             UP.LOAD_STATUS, 
             UP.LOAD_NOTES, 
             UP.STATUS_DATE
        FROM PK214_XTTK_URAL_PAYMENT UP
       WHERE UP.LOAD_STATUS = 0
         AND UP.TASK_ID     = p_task_id
    )FOR UPDATE;
    
--
-- ������� �� ������� - ������� ������
-- ��� ������� �������!!!
PROCEDURE Oboroty_201610 (
               p_recordset    OUT t_refc 
           );
  
PROCEDURE Oboroty_ttk_fiz (
               p_recordset    OUT t_refc,
               p_period_id    IN  NUMBER
           );
  
END PK214_IMPORT_TTK_URAL;
/
CREATE OR REPLACE PACKAGE BODY PK214_IMPORT_TTK_URAL
IS

/*
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
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ����������, ��� ��������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Bind_clients( p_task_id IN INTEGER )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Bind_clients';
    v_count     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ����������� ���������
    UPDATE PK214_XTTK_URAL_F F
       SET F.BRM_CONTRACTOR_ID      = c_CONTRACTOR_ID,
           F.BRM_CONTRACTOR_BANK_ID = c_CONTRACTOR_BANK_ID,
           F.BRM_BRANCH_ID          = c_BRANCH_ID,
           F.LOAD_STATUS            = NULL,
           F.LOAD_NOTES             = NULL
     WHERE F.TASK_ID                = p_task_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_F '||v_count||' rows - set contractor_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������ �/� � BRM
    UPDATE PK214_XTTK_URAL_F F 
       SET F.BRM_ACCOUNT_NO = PK05_ACCOUNT.New_std_account_no('P'),
           F.BRM_ACCOUNT_ID = PK02_POID.Next_account_id
     WHERE F.TASK_ID        = p_task_id
       AND F.BRM_ACCOUNT_ID IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_F '||v_count||' rows - set account_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������ ������� � BRM
    UPDATE PK214_XTTK_URAL_F F 
       SET F.BRM_ORDER_NO   = F.BRM_ACCOUNT_NO,
           F.BRM_ORDER_ID   = PK02_POID.Next_order_id,
           F.BRM_SERVICE_ID = c_SERVICE_ID,
           F.BRM_RATEPLAN_ID= c_RATEPLAN_ID
     WHERE F.TASK_ID        = p_task_id
       AND F.BRM_ORDER_ID   IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_F '||v_count||' rows - set order_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� �� ��������� ������� ���������
    UPDATE PK214_XTTK_URAL_F F
       SET F.LOAD_STATUS = -1,
           F.LOAD_NOTES  = '������������ ������ ���������',
           F.STATUS_DATE = SYSDATE
     WHERE F.TASK_ID = p_task_id
       AND EXISTS (
        SELECT * FROM CONTRACT_T C
         WHERE C.CONTRACT_NO = F.CONTRACT_NUMBER
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_F '||v_count||' rows - error duplicate contract_no', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );

    -- ����������� ������ ���������
    UPDATE PK214_XTTK_URAL_F F
       SET F.BRM_CONTRACT_NO   = F.CONTRACT_NUMBER,
           F.BRM_CONTRACT_ID   = PK02_POID.Next_contract_id,
           F.BRM_CONTRACT_DATE = TO_DATE(F.CONTRACT_DATE,'dd.mm.yyyy'),
           F.BRM_PASSPORT_DATE = TO_DATE(F.PASSPORT_DATE,'dd.mm.yyyy')
     WHERE F.TASK_ID = p_task_id
       AND F.BRM_CONTRACT_ID  IS NULL
       AND F.LOAD_STATUS      IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_F '||v_count||' rows - set contract_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������ ������ ID
    UPDATE PK214_XTTK_URAL_F F
       SET  F.BRM_PROFILE_ID    = PK02_POID.Next_account_profile_id, 
            F.BRM_SUBSCRIBER_ID = PK02_POID.Next_subscriber_id, 
            F.BRM_DLV_ADDR_ID   = PK02_POID.Next_address_id, 
            F.BRM_REG_ADDR_ID   = PK02_POID.Next_address_id, 
            F.BRM_OB_MG_ID      = PK02_POID.Next_order_body_id, 
            F.BRM_OB_MN_ID      = PK02_POID.Next_order_body_id,
            F.STATUS_DATE       = SYSDATE
     WHERE F.TASK_ID = p_task_id
       AND F.LOAD_STATUS       IS NULL
       AND F.BRM_PROFILE_ID    IS NULL 
       AND F.BRM_SUBSCRIBER_ID IS NULL
       AND F.BRM_DLV_ADDR_ID   IS NULL
       AND F.BRM_REG_ADDR_ID   IS NULL
       AND F.BRM_OB_MG_ID      IS NULL
       AND F.BRM_OB_MN_ID      IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_F '||v_count||' rows - set other id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������� �������� ��������
    UPDATE PK214_XTTK_URAL_F F
       SET F.LOAD_STATUS = 0,
           F.LOAD_NOTES  = 'BIND',
           F.STATUS_DATE = SYSDATE
     WHERE F.TASK_ID     = p_task_id
       AND F.LOAD_STATUS IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_F '||v_count||' rows - set bind', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ����� �������� ��� �������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Clean_bind_clients( p_task_id IN INTEGER )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Clean_bind_clients';
    v_count     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    UPDATE PK214_XTTK_URAL_F
       SET LOAD_STATUS            = NULL,
           LOAD_NOTES             = NULL,
           STATUS_DATE            = SYSDATE,
           BRM_CONTRACT_ID        = NULL,
           BRM_CONTRACT_NO        = NULL,
           BRM_CONTRACT_DATE      = NULL,
           BRM_ACCOUNT_ID         = NULL,
           BRM_ACCOUNT_NO         = NULL,
           BRM_PROFILE_ID         = NULL,
           BRM_SUBSCRIBER_ID      = NULL,
           BRM_DLV_ADDR_ID        = NULL,
           BRM_REG_ADDR_ID        = NULL,
           BRM_CONTRACTOR_ID      = NULL,
           BRM_CONTRACTOR_BANK_ID = NULL,
           BRM_BRANCH_ID          = NULL,
           BRM_ORDER_ID           = NULL,
           BRM_ORDER_NO           = NULL,
           BRM_SERVICE_ID         = NULL,
           BRM_RATEPLAN_ID        = NULL,
           BRM_OB_MG_ID           = NULL,
           BRM_OB_MN_ID           = NULL
     WHERE TASK_ID     = p_task_id
       AND LOAD_STATUS <= 0;   -- ���������� ���� ������ �� �������� - ����� � �������� ��� ������
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_F '||v_count||' rows - reset', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ����������, ��� ��������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Bind_phones( p_task_id IN INTEGER )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Bind_phones';
    v_count     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������ ��������� � ������� �� �/�
    MERGE INTO PK214_XTTK_URAL_PHONE_F P
    USING (
        SELECT P.ACCOUNT_NO, P.PHONE, F.BRM_ACCOUNT_ID, F.BRM_ACCOUNT_NO, 
               F.BRM_ORDER_ID, F.BRM_CONTRACT_DATE
          FROM PK214_XTTK_URAL_F F, PK214_XTTK_URAL_PHONE_F P
         WHERE F.BASE_ACCOUNT_NUMBER = P.ACCOUNT_NO
           AND F.TASK_ID = p_task_id
    ) F
    ON (
        P.ACCOUNT_NO = F.ACCOUNT_NO AND 
        P.PHONE      = F.PHONE
    )
    WHEN MATCHED THEN UPDATE 
                         SET P.BRM_ACCOUNT_ID = F.BRM_ACCOUNT_ID, 
                             P.BRM_ACCOUNT_NO = F.BRM_ACCOUNT_NO, 
                             P.BRM_ORDER_ID   = F.BRM_ORDER_ID,
                             P.BRM_DATE_FROM  = F.BRM_CONTRACT_DATE,
                             P.BRM_DATE_TO    = c_MAX_DATE_TO,
                             P.BRM_PHONE      = c_BRM_PHONE_PREFIX||F.PHONE,
                             P.LOAD_STATUS    = 0,
                             P.LOAD_NOTES     = 'BIND',
                             P.TASK_ID        = p_task_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_PHONE_F '||v_count||' rows - bind phones', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� �� ����������� ��������� ������ ��������� ���-����
    MERGE INTO PK214_XTTK_URAL_PHONE_F F
    USING (
        SELECT PHONE 
          FROM PK214_XTTK_URAL_PHONE_F
         GROUP BY PHONE
         HAVING COUNT(*) > 1
    ) FF
    ON (
        F.PHONE = FF.PHONE AND
        F.TASK_ID = p_task_id
    )
    WHEN MATCHED THEN UPDATE 
                         SET F.LOAD_STATUS = -2, 
                             F.LOAD_NOTES  = '��������� ������� ���������',
                             F.STATUS_DATE = SYSDATE;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_PHONE_F '||v_count||' rows - phones duplicated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ����� �������� ��� ��������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Clean_bind_phones( p_task_id IN INTEGER )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Clean_bind_phones';
    v_count     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    UPDATE PK214_XTTK_URAL_PHONE_F
       SET LOAD_STATUS    = NULL,
           LOAD_NOTES     = NULL,
           STATUS_DATE    = SYSDATE,
           BRM_ACCOUNT_ID = NULL,
           BRM_ACCOUNT_NO = NULL,
           BRM_ORDER_ID   = NULL,
           BRM_DATE_FROM  = NULL,
           BRM_DATE_TO    = NULL,
           BRM_PHONE      = NULL
     WHERE TASK_ID     = p_task_id
       AND LOAD_STATUS <= 0;   -- ���������� ���� ������ �� �������� - ����� � �������� ��� ������
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_F '||v_count||' rows - reset', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������� � �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Bind_balance( p_task_id IN INTEGER, p_balance_date IN DATE )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Bind_balance';
    v_count     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    MERGE INTO PK214_XTTK_URAL_BALANCE UB
    USING (
      SELECT UB.TTK_ACCOUNT_NUMBER, UB.BALANCE, F.BRM_ACCOUNT_NO, F.BRM_ACCOUNT_ID,
             TO_NUMBER(UB.BALANCE, '99999D99') BRM_BALANCE
        FROM PK214_XTTK_URAL_BALANCE UB, PK214_XTTK_URAL_F F
       WHERE UB.TTK_ACCOUNT_NUMBER = F.TTK_ACCOUNT_NUMBER
         AND UB.TASK_ID = p_task_id
    ) FB
    ON (
       UB.TTK_ACCOUNT_NUMBER = FB.TTK_ACCOUNT_NUMBER AND 
       UB.TASK_ID = p_task_id
    )
    WHEN MATCHED THEN UPDATE SET 
      UB.BRM_ACCOUNT_NO   = FB.BRM_ACCOUNT_NO,
      UB.BRM_ACCOUNT_ID   = FB.BRM_ACCOUNT_ID,
      UB.BRM_BALANCE_DATE = p_balance_date,
      UB.BRM_BALANCE      = FB.BRM_BALANCE,
      UB.LOAD_STATUS      = 0, 
      UB.LOAD_NOTES       = 'BIND', 
      UB.STATUS_DATE      = SYSDATE  
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_BALANCE '||v_count||' rows - merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE PK214_XTTK_URAL_BALANCE UB
       SET UB.LOAD_STATUS = -1,
           UB.LOAD_NOTES  = '�� ������ ������� ���� ��� �������', 
           UB.STATUS_DATE = SYSDATE
     WHERE UB.LOAD_STATUS IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_BALANCE '||v_count||' rows - error', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������� � ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Bind_payments( p_task_id IN INTEGER )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Bind_payments';
    v_count     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    MERGE INTO PK214_XTTK_URAL_PAYMENT UP
    USING (
        SELECT UP.ROWID RID, UP.TTK_ACCOUNT_NUMBER,  F.BRM_ACCOUNT_NO, F.BRM_ACCOUNT_ID,
               TO_DATE(UP.PAYMENT_DATE, 'dd.mm.yyyy') BRM_PAYMENT_DATE,
               UP.PAY_AMOUNT, 
               SUBSTR(UP.PAY_AMOUNT, 1, INSTR(UP.PAY_AMOUNT,' ',1)-1) PAY_STR,
               TO_NUMBER(SUBSTR(UP.PAY_AMOUNT, 1, INSTR(UP.PAY_AMOUNT,' ',1)-1),'99999.9999') BRM_PAY_AMOUNT
          FROM PK214_XTTK_URAL_PAYMENT UP, PK214_XTTK_URAL_F F
         WHERE SUBSTR(UP.TTK_ACCOUNT_NUMBER,1,9) = F.BASE_ACCOUNT_NUMBER
           AND UP.TASK_ID = p_task_id
    ) FP
    ON (
        UP.ROWID   = FP.RID AND
        UP.TASK_ID = p_task_id
    )
    WHEN MATCHED THEN UPDATE SET
          UP.BRM_ACCOUNT_NO   = FP.BRM_ACCOUNT_NO,
          UP.BRM_ACCOUNT_ID   = FP.BRM_ACCOUNT_ID,
          UP.BRM_PAYMENT_DATE = FP.BRM_PAYMENT_DATE,
          UP.BRM_PAY_AMOUNT   = FP.BRM_PAY_AMOUNT,
          UP.LOAD_STATUS      = 0, 
          UP.LOAD_NOTES       = 'BIND', 
          UP.STATUS_DATE      = SYSDATE;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_PAYMENT '||v_count||' rows - bind', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE PK214_XTTK_URAL_PAYMENT UP
       SET UP.LOAD_STATUS = -1,
           UP.LOAD_NOTES  = '�� ������ ������� ���� ��� �������',
           UP.STATUS_DATE = SYSDATE
     WHERE UP.TASK_ID = 1
       AND UP.LOAD_STATUS IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK214_XTTK_URAL_PAYMENT '||v_count||' rows - bind error', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

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
           WHEN p_step = c_LOAD_CODE_SUB_MG THEN '�������� ��������� ������ ��'
           WHEN p_step = c_LOAD_CODE_SUB_MN THEN '�������� ��������� ������ ��'
           WHEN p_step = c_LOAD_CODE_FIN THEN '�����'
           ELSE '����������� ���'
         END;
END;

--============================================================================================
-- �������� ������ �� ������� ������
--============================================================================================
PROCEDURE Load_accounts(p_task_id IN INTEGER)
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Load_accounts';
    v_count           INTEGER := 0;
    v_step            INTEGER := 1;
    v_ok              INTEGER := 0;
    v_error           INTEGER := 0;
    v_load_notes      VARCHAR2(1000);
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR r_abn IN c_CTR( p_task_id, 0 ) LOOP
      
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
               ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, 
               CURRENCY_ID, CURRENCY_CONVERSION_ID,
               STATUS, PARENT_ID, NOTES,
               BALANCE, BALANCE_DATE, CREATE_DATE, BILLING_ID,
               EXTERNAL_ID, EXTERNAL_NO, COMMENTARY 
            )VALUES(
               r_abn.brm_account_id, r_abn.brm_account_no, Pk00_Const.c_ACC_TYPE_P, 
               c_BRM_CURRENCY_ID, c_BRM_CURRENCY_CONVERSION_ID,
               'NEW', NULL, 
               'TTK_ACCOUNT_ID = '||r_abn.ttk_account_id||' ������������� �� �������� ���-����, '|| TO_CHAR(SYSDATE,'dd.mm.yyyy'), 
               0, SYSDATE, SYSDATE, c_BILLING_ID, 
               r_abn.base_account_id, r_abn.base_account_number, NULL
            );
            -- ������� ��������� ������ � ������ �������� �����
            Pk07_Bill.New_billinfo (
                         p_account_id    => r_abn.brm_account_id,    -- ID �������� �����
                         p_currency_id   => c_BRM_CURRENCY_ID,       -- ID ������ �����
                         p_delivery_id   => c_BRM_DELIVERY_METHOD_ID,-- ID ������� �������� �����
                         p_days_for_payment => 30           -- ���-�� ���� �� ������ �����
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
            INSERT INTO CONTRACT_T C (
              CONTRACT_ID, CONTRACT_NO, 
              MARKET_SEGMENT_ID, CLIENT_TYPE_ID, 
              DATE_FROM, DATE_TO, 
              CLIENT_ID, 
              NOTES
            )VALUES(
              r_abn.brm_contract_id, r_abn.brm_contract_no, 
              NULL, NULL,
              r_abn.brm_contract_date, Pk00_Const.c_DATE_MAX, 
              c_CLIENT_ID, 
              '������������� �� �������� ���-����, '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );

        END IF;
    
        -- -------------------------------------------------------- --
        -- ������� ����������
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_CST;
        SELECT COUNT(*) INTO v_count
          FROM SUBSCRIBER_T SB
         WHERE SB.SUBSCRIBER_ID = r_abn.brm_subscriber_id
        ;
        IF v_count = 0 THEN
           INSERT INTO SUBSCRIBER_T SB (
              SUBSCRIBER_ID, 
              LAST_NAME, FIRST_NAME, MIDDLE_NAME, CATEGORY, MODIFY_DATE, 
              PASSPORT_SERIES, PASSPORT_NUMBER, PASSPORT_DATE, PASSPORT_ISSUER
           )
           VALUES(
              r_abn.BRM_SUBSCRIBER_ID, 
              r_abn.LAST_NAME, r_abn.FIRST_NAME, r_abn.SECOND_NAME, 1, SYSDATE,
              r_abn.PASSPORT_SERIES, r_abn.PASSPORT_NUMBER, r_abn.BRM_PASSPORT_DATE, r_abn.PASSPORT_ISSUER
           );
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
               PROFILE_ID, ACCOUNT_ID, CONTRACT_ID, 
               SUBSCRIBER_ID, CUSTOMER_ID,
               CONTRACTOR_ID, BRANCH_ID, AGENT_ID,  
               CONTRACTOR_BANK_ID, VAT, DATE_FROM, DATE_TO, KPP)
            VALUES
               (r_abn.brm_profile_id, 
                r_abn.brm_account_id, r_abn.brm_contract_id, 
                r_abn.brm_subscriber_id, c_CUSTOMER_ID,
                r_abn.brm_contractor_id, r_abn.brm_branch_id, NULL, 
                r_abn.brm_contractor_bank_id, c_BRM_VAT,  
                r_abn.brm_contract_date, NULL, NULL
                )
            ;
        END IF;
   
        -- -------------------------------------------------------- --
        -- ������� ����� �����������
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_AJR;
        SELECT COUNT(*) INTO v_count
          FROM ACCOUNT_CONTACT_T AC
         WHERE 
            (AC.CONTACT_ID = r_abn.brm_reg_addr_id)
            or
            (
                AC.ACCOUNT_ID = r_abn.brm_account_id
                and
                AC.ADDRESS_TYPE = 'REG'
            )
        ;
        IF v_count = 0 THEN
            INSERT INTO ACCOUNT_CONTACT_T (   
                CONTACT_ID,ADDRESS_TYPE,ACCOUNT_ID,
                COUNTRY,ZIP,STATE,CITY,
                ADDRESS,DATE_FROM,
                NOTES
            )VALUES(
                r_abn.brm_reg_addr_id, 'REG', r_abn.brm_account_id,
                r_abn.COUNTRY_REG, r_abn.ZIP_REG, r_abn.STATE_REG, r_abn.CITY_REG, 
                r_abn.STREET_REG||','||r_abn.NUM_REG||
                DECODE(r_abn.BUILDING_REG, NULL, NULL, ', �������� '||r_abn.BUILDING_REG)||
                DECODE(r_abn.BLOCK_REG, NULL, NULL, ', ������ '||r_abn.BLOCK_REG)||
                ', '||r_abn.FLAT_REG,
                r_abn.brm_contract_date, 
                '������������� �� �������� ���-���� '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );  
        END IF;
        
        -- -------------------------------------------------------- --
        -- ������� ����� ��������
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_ADL;
        SELECT COUNT(*) INTO v_count
          FROM ACCOUNT_CONTACT_T AC
         WHERE 
            (AC.CONTACT_ID = r_abn.brm_dlv_addr_id)
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
                COUNTRY,ZIP,STATE,CITY,
                ADDRESS,DATE_FROM, 
                EMAIL,
                NOTES
            )VALUES(
                r_abn.brm_dlv_addr_id, 'DLV', r_abn.brm_account_id,
                r_abn.COUNTRY, r_abn.ZIP, r_abn.STATE, r_abn.CITY, 
                r_abn.STREET||','||r_abn.NUM||
                DECODE(r_abn.BUILDING, NULL, NULL, ', �������� '||r_abn.BUILDING)||
                DECODE(r_abn.BLOCK, NULL, NULL, ', ������ '||r_abn.BLOCK)||
                ', '||r_abn.FLAT,
                r_abn.brm_contract_date, 
                r_abn.base_account_notif_email,
                '������������� �� �������� ���-���� '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );  
        END IF;
    
        -- -------------------------------------------------------- --
        -- ������� ������� ����������� �������
        v_ok := v_ok + 1;
        
        UPDATE PK214_XTTK_URAL_F
           SET LOAD_STATUS = v_step,
               LOAD_NOTES  = 'OK'
         WHERE CURRENT OF c_CTR;
        
        IF MOD(v_ok, 100) = 0 THEN
            Pk01_Syslog.Write_msg(v_ok||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
      EXCEPTION
         -- -------------------------------------------------------- --
         -- ��������� ������ �������� ������ 
         WHEN OTHERS THEN
            v_load_notes := 'ERROR, ��� => '||view_step(v_step)||'. '
                            ||Pk01_Syslog.get_OraErrTxt(c_PkgName||'.'||v_prcName);
            UPDATE PK214_XTTK_URAL_F
               SET LOAD_NOTES  = v_load_notes,
                   LOAD_STATUS = -v_step
             WHERE CURRENT OF c_CTR;

            Pk01_Syslog.Write_msg('contract_no='||r_abn.CONTRACT_NUMBER||
                                ', account_no=' ||r_abn.BASE_ACCOUNT_NUMBER||
                                ' => '||v_load_notes
                                , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );            

            v_error := v_error + 1;
      END;
    END LOOP;

    Pk01_Syslog.Write_msg('Report: '||v_ok||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE PK214_XTTK_URAL_F SET STATUS_DATE = SYSDATE
     WHERE TASK_ID = p_task_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- �������� ������ �� �������
--============================================================================================
PROCEDURE Load_orders(p_task_id IN INTEGER)
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Load_orders';
    v_count           INTEGER := 0;
    v_step            INTEGER := 1;
    v_ok              INTEGER := 0;
    v_error           INTEGER := 0;
    v_load_notes      VARCHAR2(1000);
   
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR r_ord IN c_CTR( p_task_id, c_LOAD_CODE_ORD-1 ) LOOP
      
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
               ORDER_ID, ORDER_NO, ACCOUNT_ID, SERVICE_ID, RATEPLAN_ID, 
               DATE_FROM, DATE_TO, 
               CREATE_DATE, MODIFY_DATE, 
               TIME_ZONE, NOTES
            )VALUES(
               r_ord.brm_order_id, r_ord.brm_order_no, r_ord.brm_account_id, 
               c_SERVICE_ID, NULL,
               r_ord.brm_contract_date, c_MAX_DATE_TO,
               SYSDATE, SYSDATE, NULL, 
               '������������� �� �������� ���-���� '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );
            -- ��������� ���������� � ������ ����������� (������ ��������)
            INSERT INTO ORDER_INFO_T( 
                   ORDER_ID, POINT_SRC, POINT_DST, 
                   SPEED_STR, SPEED_VALUE, SPEED_UNIT_ID, 
                   DOWNTIME_FREE )
            VALUES( 
                   r_ord.brm_order_id, NULL, NULL, 
                   NULL, NULL, NULL,
                   NULL );
        END IF;

        -- -------------------------------------------------------- --
        -- ��������� ���������� ������ ��
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_SUB_MG;
        INSERT INTO ORDER_BODY_T(
            ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
            DATE_FROM, DATE_TO, 
            RATE_VALUE, RATE_LEVEL_ID,
            TAX_INCL, CURRENCY_ID,
            RATEPLAN_ID
        ) VALUES (
            r_ord.brm_ob_mg_id, r_ord.brm_order_id, 
            c_SUBSERVICE_MG_ID, 'USG',
            r_ord.brm_contract_date, 
            c_MAX_DATE_TO,
            NULL,
            NULL,
            c_BRM_TAX_INCL, c_BRM_CURRENCY_ID, 
            c_RATEPLAN_ID
        );

        -- -------------------------------------------------------- --
        -- ��������� ���������� ������ ��
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_SUB_MN;
        INSERT INTO ORDER_BODY_T(
            ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
            DATE_FROM, DATE_TO, 
            RATE_VALUE, RATE_LEVEL_ID,
            TAX_INCL, CURRENCY_ID,
            RATEPLAN_ID
        ) VALUES (
            r_ord.brm_ob_mn_id, r_ord.brm_order_id, 
            c_SUBSERVICE_MN_ID, 'USG',
            r_ord.brm_contract_date, 
            c_MAX_DATE_TO,
            NULL,
            NULL,
            c_BRM_TAX_INCL, c_BRM_CURRENCY_ID, 
            c_RATEPLAN_ID
        );

        -- -------------------------------------------------------- --
        -- ������� ������� ����������� �������
        v_ok := v_ok + 1;
        
        UPDATE PK214_XTTK_URAL_F
           SET LOAD_STATUS  = v_step,
               LOAD_NOTES   = 'OK',
               STATUS_DATE  = SYSDATE
         WHERE CURRENT OF c_CTR;
        
        IF MOD(v_ok, 100) = 0 THEN
            Pk01_Syslog.Write_msg(v_ok||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
      EXCEPTION
         -- -------------------------------------------------------- --
         -- ��������� ������ �������� ������ 
         WHEN OTHERS THEN
            v_load_notes := 'ERROR, ��� => '||view_step(v_step)||'. '
                            ||Pk01_Syslog.get_OraErrTxt(c_PkgName||'.'||v_prcName);
            UPDATE PK214_XTTK_URAL_F
               SET LOAD_NOTES  = v_load_notes,
                   LOAD_STATUS = -v_step,
                   STATUS_DATE = SYSDATE
             WHERE CURRENT OF c_CTR;

            Pk01_Syslog.Write_msg('contract_no='||r_ord.CONTRACT_NUMBER||
                                ', account_no=' ||r_ord.BASE_ACCOUNT_NUMBER||
                                ' => '||v_load_notes
                                , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );            

            v_error := v_error + 1;
      END;
    END LOOP;

    Pk01_Syslog.Write_msg('Report: '||v_ok||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    --UPDATE PK214_XTTK_URAL_F SET STATUS_DATE = SYSDATE
    -- WHERE TASK_ID = p_task_id;
    
    -- - - - - - - - - - - - - - - -- - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- �������� ���������� �������
--============================================================================================
PROCEDURE Load_phones(p_task_id IN INTEGER)
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Load_phones';
    v_count           INTEGER := 0;
    v_step            INTEGER := 1;
    v_ok              INTEGER := 0;
    v_error           INTEGER := 0;
   
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR r_ph IN c_PHONES(p_task_id) LOOP
      BEGIN
         -- ��������� ����� �������� �� �����
         INSERT INTO ORDER_PHONES_T (ORDER_ID, PHONE_NUMBER, DATE_FROM, DATE_TO)
         VALUES (r_ph.BRM_ORDER_ID, r_ph.BRM_PHONE, r_ph.BRM_DATE_FROM, r_ph.BRM_DATE_TO);
    
          -- ������� ������� ����������� �������
          v_ok := v_ok + 1;
          
          UPDATE PK214_XTTK_URAL_PHONE_F
             SET LOAD_STATUS  = v_step,
                 LOAD_NOTES   = 'OK',
                 STATUS_DATE  = SYSDATE
           WHERE CURRENT OF c_PHONES;
          
          IF MOD(v_ok, 100) = 0 THEN
              Pk01_Syslog.Write_msg(v_ok||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
          END IF;
      
      EXCEPTION
         -- -------------------------------------------------------- --
         -- ��������� ������ �������� ������ 
         WHEN OTHERS THEN
            UPDATE PK214_XTTK_URAL_PHONE_F
               SET LOAD_NOTES  = '������ ��� ���������� ������ �� �����',
                   LOAD_STATUS = -v_step,
                   STATUS_DATE = SYSDATE
             WHERE CURRENT OF c_CTR;
            Pk01_Syslog.Write_msg('brm_account_no='||r_ph.BRM_ACCOUNT_NO||
                                ', brm_phone=' ||r_ph.BRM_PHONE||
                                ' => ������ ��� ���������� ������ �� �����'
                                , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );            

            v_error := v_error + 1;
      END;
    END LOOP;
    
    Pk01_Syslog.Write_msg('Report: '||v_ok||' - ��, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    --UPDATE PK214_XTTK_URAL_PHONE_F SET STATUS_DATE = SYSDATE
    -- WHERE TASK_ID = p_task_id;
    
    -- - - - - - - - - - - - - - - -- - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- �������� �������� �������� �� ��������� � ������� ������ ����
--============================================================================================
PROCEDURE Load_balance(p_task_id IN INTEGER)
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Load_balance';
    v_count           INTEGER := 0;
    v_error           INTEGER := 0;
    v_ok              INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR r_bl IN c_BAL(p_task_id) LOOP
      BEGIN
          -- ��������� ������������� ��������� �������
          SELECT COUNT(*) INTO v_count 
            FROM INCOMING_BALANCE_T IB
           WHERE IB.ACCOUNT_ID = r_bl.brm_account_id;

          IF v_count = 1 THEN
              -- ������� ������ � �������� ������� � ������������� ����� � �������
              PK05_ACCOUNT_BALANCE_NEW.Delete_incomming_balance( r_bl.brm_account_id );
          END IF;
          -- ��������� ����� ������
          PK05_ACCOUNT_BALANCE_NEW.Set_incomming_balance (
                                 p_account_id   => r_bl.brm_account_id,
                                 p_balance      => r_bl.brm_balance,     -- �������� ������ �� ������ ����� ������ 00:00:00
                                 p_balance_date => r_bl.brm_balance_date -- ������ ����� ������ 00:00:00 � ������� ����� � ��������
                             );

          -- ��������� ���������
          UPDATE PK214_XTTK_URAL_BALANCE
             SET LOAD_STATUS  = 1,
                 LOAD_NOTES   = 'OK',
                 STATUS_DATE  = SYSDATE
           WHERE CURRENT OF c_BAL;
          --
          v_ok := v_ok + 1;
          --
      EXCEPTION
         -- -------------------------------------------------------- --
         -- ��������� ������ �������� ������ 
         WHEN OTHERS THEN
            UPDATE PK214_XTTK_URAL_BALANCE
               SET LOAD_NOTES  = '������',
                   LOAD_STATUS = -1,
                   STATUS_DATE = SYSDATE
             WHERE CURRENT OF c_BAL;
            Pk01_Syslog.Write_msg('brm_account_no='||r_bl.BRM_ACCOUNT_NO||
                                ' => ������ ��� ��������� ��������� �������'
                                , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );            

            v_error := v_error + 1;
      END;

      -- �����
      IF MOD(v_ok + v_error, 100) = 0 THEN
          Pk01_Syslog.Write_msg((v_ok + v_error)||' rows processed', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      END IF;

    END LOOP;
    
    Pk01_Syslog.Write_msg('Report: '||v_ok||' - OK, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - -- - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;
 
--============================================================================================
-- ��������� ����������� ������ � �����
--============================================================================================
/*
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
    DELETE FROM PK213_PINDB_ALL_CONTRACTS_T;
    
    INSERT INTO PK213_PINDB_ORDERS_ARX
    SELECT * FROM PK213_PINDB_ORDERS_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK213_PINDB_ORDERS_ARX '||v_count||' - rows insert', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    DELETE FROM PK213_PINDB_ORDERS_T;

    -- - - - - - - - - - - - - - - -- - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;
*/

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


-- ------------------------------------------------------------------------- --
-- ��������� ����������� � BRM ��������� � ��������-��������� ���������� 01.10.2016
--
PROCEDURE Loaded_abonets_vs_osb (
               p_recordset    OUT t_refc 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Loaded_abonets_vs_osb';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
      SELECT O.*, 
             F.PERSON_ID, F.BASE_ACCOUNT_ID, F.BASE_ACCOUNT_NUMBER, 
             F.BASE_ACCOUNT_NOTIF_EMAIL, F.TTK_ACCOUNT_ID, F.TTK_ACCOUNT_NUMBER, 
             F.CONTRACT_NUMBER, F.CONTRACT_DATE, F.LAST_NAME, F.FIRST_NAME, 
             F.SECOND_NAME, F.PASSPORT_SERIES, F.PASSPORT_NUMBER, F.PASSPORT_DATE, 
             F.PASSPORT_ISSUER, F.ZIP, F.COUNTRY, F.STATE, F.CITY, F.STREET, F.NUM, 
             F.BUILDING, F.BLOCK, F.FLAT, F.ZIP_REG, F.COUNTRY_REG, 
             F.STATE_REG, F.CITY_REG, F.STREET_REG, 
             F.NUM_REG, F.BUILDING_REG, F.BLOCK_REG, F.FLAT_REG
        FROM PK214_XTTK_OCB_20161001 O FULL OUTER JOIN PK214_XTTK_URAL_F F
          ON ( O.CLIENT LIKE F.LAST_NAME||'%' )
       ORDER BY CLIENT, LAST_NAME 
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------- --
-- ��������� ������ �� ������, ���� ����� ������ ��������� ����� � ���������
-- ��� ������� �������!!!
PROCEDURE Check_balans_for_period (
               p_recordset    OUT t_refc 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_balans_for_period';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
      WITH F AS (
          SELECT BRM_ACCOUNT_ID, SUM(AMOUNT) OUT_BALANCE 
            FROM (
              SELECT F.BRM_ACCOUNT_ID, NVL(IB.BALANCE,0) AMOUNT 
                FROM PK214_XTTK_URAL_F F, INCOMING_BALANCE_T IB
               WHERE F.BRM_ACCOUNT_ID = IB.ACCOUNT_ID(+)
              UNION ALL
              SELECT F.BRM_ACCOUNT_ID, -SUM(NVL(B.TOTAL,0)) AMOUNT 
                FROM PK214_XTTK_URAL_F F, BILL_T B
               WHERE F.BRM_ACCOUNT_ID = B.ACCOUNT_ID(+)
                 AND B.REP_PERIOD_ID(+) = 201609
               GROUP BY F.BRM_ACCOUNT_ID
              UNION ALL 
              SELECT F.BRM_ACCOUNT_ID, SUM(NVL(P.RECVD,0)) AMOUNT
                FROM PK214_XTTK_URAL_F F, PAYMENT_T P
               WHERE F.BRM_ACCOUNT_ID = P.ACCOUNT_ID(+)
                 AND P.REP_PERIOD_ID(+) = 201609
               GROUP BY F.BRM_ACCOUNT_ID
          )
          GROUP BY BRM_ACCOUNT_ID
      )
      SELECT F.OUT_BALANCE, V.BALANCE, V.CLIENT, V.ACCOUNT_NO, V.CONTRACT_NO 
        FROM F, PK214_XTTK_URAL_VEDOMOST V
       WHERE F.BRM_ACCOUNT_ID = V.BRM_ACCOUNT_ID
         AND F.OUT_BALANCE != V.BRM_BALANCE
       ORDER BY ABS(F.OUT_BALANCE - V.BRM_BALANCE)
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

--
-- ������� �� ������� - ������� ������
-- ��� ������� �������!!!
PROCEDURE Oboroty_201610 (
               p_recordset    OUT t_refc 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Oboroty_201610';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
      WITH CL AS (
      SELECT BRM_ACCOUNT_ID, F.BRM_ACCOUNT_NO, F.BRM_CONTRACT_NO, LAST_NAME||' '||FIRST_NAME||' '||SECOND_NAME CLIENT 
        FROM PK214_XTTK_URAL_F F
       WHERE LOAD_STATUS = 11
      ), IB AS (
          SELECT CL.BRM_ACCOUNT_ID, NVL(IB.BALANCE,0) IN_BALANCE
            FROM INCOMING_BALANCE_T IB, CL
           WHERE CL.BRM_ACCOUNT_ID = IB.ACCOUNT_ID(+)
      ), B AS (
          SELECT CL.BRM_ACCOUNT_ID, NVL(SUM(B.TOTAL),0) BILL_TOTAL
            FROM BILL_T B, CL
           WHERE B.REP_PERIOD_ID(+) = 201610
             AND B.BILL_TYPE(+) != 'I'
             AND CL.BRM_ACCOUNT_ID = B.ACCOUNT_ID(+)
            GROUP BY CL.BRM_ACCOUNT_ID
      ), P AS (
          SELECT CL.BRM_ACCOUNT_ID, NVL(SUM(P.RECVD),0) RECVD
            FROM PAYMENT_T P, CL
           WHERE P.REP_PERIOD_ID(+) = 201610
             AND P.PAYMENT_TYPE(+) != 'INBAL'
             AND CL.BRM_ACCOUNT_ID = P.ACCOUNT_ID(+)
            GROUP BY CL.BRM_ACCOUNT_ID
      )
      SELECT CL.*, IB.IN_BALANCE, B.BILL_TOTAL, P.RECVD, ( IB.IN_BALANCE - B.BILL_TOTAL + P.RECVD ) OUT_BALANCE  
        FROM IB, B, P, CL
       WHERE IB.BRM_ACCOUNT_ID = B.BRM_ACCOUNT_ID
         AND IB.BRM_ACCOUNT_ID = P.BRM_ACCOUNT_ID
         AND IB.BRM_ACCOUNT_ID = CL.BRM_ACCOUNT_ID
       ORDER BY CLIENT
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ������� �� ������� - ������� ������
-- ��� ������� �������!!!
PROCEDURE Oboroty_ttk_fiz (
               p_recordset    OUT t_refc,
               p_period_id    IN  NUMBER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Oboroty_ttk_fiz';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
      WITH PI AS (
          SELECT ACCOUNT_NO, ACCOUNT_ID, CONTRACT_ID, SUBSCRIBER_ID,
                 NVL(REP_PERIOD_ID, 201610) REP_PERIOD_ID, 
                 NVL(OPEN_BALANCE, 0)  OPEN_BALANCE, 
                 NVL(CLOSE_BALANCE, 0) CLOSE_BALANCE, 
                 NVL(TOTAL, 0) TOTAL,
                 NVL(GROSS, 0) GROSS,
                 NVL(RECVD, 0) RECVD
            FROM (
              SELECT A.ACCOUNT_NO, A.ACCOUNT_ID, AP.CONTRACT_ID, AP.SUBSCRIBER_ID, 
                     PI.REP_PERIOD_ID, PI.OPEN_BALANCE, PI.CLOSE_BALANCE, PI.TOTAL, PI.GROSS, PI.RECVD,
                     MAX(PI.REP_PERIOD_ID) OVER (PARTITION BY PI.ACCOUNT_ID) MAX_PERIOD_ID 
                FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, REP_PERIOD_INFO_T PI
               WHERE A.ACCOUNT_TYPE = 'P'
                 AND A.ACCOUNT_ID   = AP.ACCOUNT_ID
                 AND AP.ACTUAL      = 'Y'
                 AND AP.BRANCH_ID   = 337
                 AND A.ACCOUNT_ID   = PI.ACCOUNT_ID(+)
                 AND PI.REP_PERIOD_ID(+) <= p_period_id
           )
           WHERE (REP_PERIOD_ID = MAX_PERIOD_ID OR REP_PERIOD_ID IS NULL)
      )
      SELECT --PI.REP_PERIOD_ID,
             PI.ACCOUNT_ID BRM_ACCOUNT_ID,
             PI.ACCOUNT_NO BRM_ACCOUNT_NO,
             C.CONTRACT_NO,
             SUB.LAST_NAME || ' ' || SUB.FIRST_NAME || ' ' || SUB.MIDDLE_NAME SUBSCRIBER,
             CASE
                 WHEN PI.REP_PERIOD_ID < p_period_id THEN PI.CLOSE_BALANCE
                 ELSE PI.OPEN_BALANCE
             END IN_BALANCE,
             --PI.OPEN_BALANCE IN_BALANCE,
             CASE
                 WHEN PI.REP_PERIOD_ID < p_period_id THEN 0
                 ELSE PI.TOTAL
             END TOTAL,
             --PI.TOTAL,
             CASE
                 WHEN PI.REP_PERIOD_ID < p_period_id THEN 0
                 ELSE PI.RECVD
             END RECVD,
             --PI.RECVD,
             CASE
                 WHEN PI.REP_PERIOD_ID < p_period_id THEN PI.CLOSE_BALANCE
                 ELSE PI.CLOSE_BALANCE
             END OUT_BALANCE
             --PI.CLOSE_BALANCE OUT_BALANCE
        FROM PI, CONTRACT_T C, SUBSCRIBER_T SUB
       WHERE PI.CONTRACT_ID   = C.CONTRACT_ID
         AND PI.SUBSCRIBER_ID = SUB.SUBSCRIBER_ID
      ORDER BY SUB.LAST_NAME || ' ' || SUB.FIRST_NAME || ' ' || SUB.MIDDLE_NAME
      /*
      SELECT a.account_id BRM_ACCOUNT_ID,
               A.ACCOUNT_NO BRM_ACCOUNT_NO,
               C.CONTRACT_NO,
               SUB.LAST_NAME || ' ' || SUB.FIRST_NAME || ' ' || SUB.MIDDLE_NAME,
               NVL (PER.OPEN_BALANCE, 0) IN_BALANCE,
               NVL (PER.TOTAL, 0) TOTAL,
               NVL (PER.RECVD, 0) RECVD,
               NVL (PER.CLOSE_BALANCE, 0) OUT_BALANCE
          FROM account_profile_t ap,
               account_t a,
               subscriber_t sub,
               contract_t c,
               REP_PERIOD_INFO_T per
         WHERE     ap.account_Id = a.account_id
               AND sub.subscriber_id = ap.subscriber_id
               AND C.CONTRACT_ID = ap.CONTRACT_ID
               AND A.ACCOUNT_ID = per.account_ID(+)
               AND PER.REP_PERIOD_ID(+) = p_period_id
               AND ap.branch_id = 337
               AND ap.actual = 'Y'
               AND a.account_type = 'P'
      ORDER BY SUB.LAST_NAME || ' ' || SUB.FIRST_NAME || ' ' || SUB.MIDDLE_NAME
      */
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


END PK214_IMPORT_TTK_URAL;
/
