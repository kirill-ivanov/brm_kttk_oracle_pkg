CREATE OR REPLACE PACKAGE PK05_ACCOUNT
IS
    --
    -- ����� ��� ������ � �������� "������� ����", �������:
    -- account_t, account_profile_t, billinfo_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK05_ACCOUNT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- ��������!!! ������� ����� ����� �������� ����� �� �������� �������� 
    FUNCTION New_account_no RETURN VARCHAR2;
    
    -- ������� ����� ����� �������� ����� �� �������� INFRANET
    FUNCTION New_account_no(
                       p_contract_no  IN VARCHAR2,
                       p_account_type IN CHAR     -- 'J'/'P'
                   ) RETURN VARCHAR2;
    
    -- ������� ����� ������� ����, ���������� ��������
    --   - ������������� - ID �������� ����� �������, 
    --   - ��� ������ ���������� ����������
    FUNCTION New_account(
                   p_account_no    IN VARCHAR2,
                   p_account_type  IN VARCHAR2,
                   p_currency_id   IN INTEGER,
                   p_status        IN VARCHAR2,
                   p_parent_id     IN INTEGER  DEFAULT NULL,
                   p_notes         IN VARCHAR2 DEFAULT NULL,
                   p_curr_conv_id  IN INTEGER  DEFAULT NULL,  -- ������� ����������� ������
                   p_comment       IN VARCHAR2 DEFAULT NULL   -- ����������� � �����
               ) RETURN INTEGER;

    -- �������� ��������� ������ �������� �����, ���������� ��������
    --   - ������������� - OK 
    --   - ��� ������ ���������� ����������
    PROCEDURE Edit_account(
                   p_account_id    IN INTEGER,
                   p_account_no    IN VARCHAR2,
                   p_account_type  IN VARCHAR2,
                   p_currency_id   IN INTEGER,
                   p_status        IN VARCHAR2,
                   p_parent_id     IN INTEGER,
                   p_notes         IN VARCHAR2
               );
               
    -- ������� ������� ����, �������� ��������, ������ ��� ����� ��������� �/�
    --   - ��� ������ ���������� ����������
    PROCEDURE Delete_account(
                   p_account_id     IN INTEGER
               );
               
    -- ����� ID � �������� �/� �� ������ ������ (����� � ���������)
    --   - ������������� - ���-�� �����
    --   - NULL - �� ������
    --   - ��� ������ ���������� ����������
    FUNCTION Find_account_by_no( 
                   p_account_no  IN VARCHAR2
                 ) RETURN INTEGER;
    
    --
    -- �������� ������ �� �������� ����� �� ��������� �������
    --   - ��� ������ ���������� ���������
    PROCEDURE Period_info( 
                   p_recordset     OUT t_refc,
                   p_account_id     IN INTEGER,
                   p_period_id_from IN INTEGER
               );
    
    -- �������� ����� �������� �����
    --   - ������ - ����� �������� ����� 
    --   - NULL - ������
    FUNCTION Get_account_no(
                   p_account_id     IN INTEGER
               ) RETURN VARCHAR2;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������� ������� �������� �����, ���������� ��������
    --   - ������������� - id ������� 
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Set_profile(
                   p_account_id         IN INTEGER,
                   p_brand_id           IN INTEGER,
                   p_contract_id        IN INTEGER,
                   p_customer_id        IN INTEGER,
                   p_subscriber_id      IN INTEGER,
                   p_contractor_id      IN INTEGER,
                   p_branch_id          IN INTEGER,
                   p_agent_id           IN INTEGER,
                   p_contractor_bank_id IN INTEGER,
                   p_vat                IN NUMBER,
                   p_date_from          IN DATE,
                   p_date_to            IN DATE
               ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ���������� ����� � �������� �����, ���������� ��������
    --   - ������������� - id �������� 
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Add_address(
                   p_account_id    IN INTEGER,
                   p_address_type  IN VARCHAR2,
                   p_country       IN VARCHAR2,
                   p_zip           IN VARCHAR2,
                   p_state         IN VARCHAR2,
                   p_city          IN VARCHAR2,
                   p_address       IN VARCHAR2,
                   p_person        IN VARCHAR2,
                   p_phones        IN VARCHAR2,
                   p_fax           IN VARCHAR2,
                   p_email         IN VARCHAR2,
                   p_date_from     IN DATE,
                   p_date_to       IN DATE DEFAULT NULL,
                   p_notes         IN VARCHAR2 DEFAULT NULL
               ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������������� ���������� ����� � �������� �����, ���������� ��������
    --   - ��� ������ ���������� ����������
    --
    PROCEDURE Edit_address(
                   p_contact_id    IN INTEGER,
                   p_address_type  IN VARCHAR2,
                   p_country       IN VARCHAR2,
                   p_zip           IN VARCHAR2,
                   p_state         IN VARCHAR2,
                   p_city          IN VARCHAR2,
                   p_address       IN VARCHAR2,
                   p_person        IN VARCHAR2,
                   p_phones        IN VARCHAR2,
                   p_fax           IN VARCHAR2,
                   p_email         IN VARCHAR2,
                   p_date_from     IN DATE,
                   p_date_to       IN DATE DEFAULT NULL ,
                   p_notes         IN VARCHAR2 DEFAULT NULL
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ���������� ����� � �������� �����, ���������� ��������
    --   - ��� ������ ���������� ����������
    --
    PROCEDURE Delete_address(
                   p_contact_id    IN INTEGER
               );
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ������ ������� (���������) �� ������� �����
    --   - ������������� - ���-�� �����
    --   - ��� ������ ���������� ���������
    --
    FUNCTION Address_list( 
                   p_recordset   OUT t_refc,
                   p_account_id   IN INTEGER
               ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������������� �������� �������� �����
    --   - ������������� - id �������� 
    --   - ��� ������ ���������� ����������
    --
    PROCEDURE Edit_contact(
               p_account_id    IN INTEGER,
               p_phones        IN VARCHAR2,
               p_fax           IN VARCHAR2,
               p_email         IN VARCHAR2
           );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ����� ��� ����� ���������� �������� �����, ����������
    --   - ������������� - ���-�� ��������� �������
    --   - ��� ������ ���������� ���������
    FUNCTION Bills_list ( 
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,   -- ID ������� �����
                   p_date_from  IN DATE DEFAULT TO_DATE('01.01.2000','dd.mm.yyyy'),
                   p_date_to    IN DATE DEFAULT TO_DATE('01.01.2050','dd.mm.yyyy')
               ) RETURN INTEGER;
    
    -- �������� ������ ������� �� ������� �����
    --   - ������������� - ���-�� �������
    --   - ��� ������ ���������� ���������
    --
    FUNCTION Order_list( 
                   p_recordset   OUT t_refc,
                   p_account_id   IN INTEGER
               ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� ��������� �������������� �/�, ���������� ��������
    -- - ��� ������ ���������� ����������
    PROCEDURE Set_manager (
                   p_account_id IN INTEGER,
                   p_manager_id IN INTEGER,
                   p_date_from  IN DATE DEFAULT SYSDATE
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ID ��������� �������������� �/�, ���������� ��������
    -- - ������������� - ID ���������
    -- - NULL - ��� ������
    -- - ��� ������ ���������� ����������
    FUNCTION Get_manager_id (
                   p_account_id  IN INTEGER,
                   p_date        IN DATE DEFAULT SYSDATE
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ���� �������� �������� (� �������� �����)    
    PROCEDURE Edit_contact_datecreate( 
              p_account_id     IN INTEGER,
              p_datecreate     IN DATE
          );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� ������� � ������� ������������� �/�
    -- - ��� ������ ���������� ����������
    PROCEDURE Set_billing (
               p_account_id IN INTEGER,
               p_billing_id IN INTEGER
           );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� �� �/� ����������� ��� �����-�������
    -- - ��� ������ ���������� ����������
    PROCEDURE Set_payer (
               p_account_id IN INTEGER,
               p_payer_id   IN INTEGER
           );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� ���������� ������ �� �/�
    PROCEDURE Change_seller (
                p_account_no         IN VARCHAR2,
                p_contractor_id      IN INTEGER,
                p_contractor_bank_id IN INTEGER,
                p_period_id          IN INTEGER  -- ��� ������ ������� ���������
           );
   
   
END PK05_ACCOUNT;
/
CREATE OR REPLACE PACKAGE BODY PK05_ACCOUNT
IS

-- ������� ����� ����� �������� ����� �� �������� ���������
FUNCTION New_account_no RETURN VARCHAR2 
IS
    v_account_no ACCOUNT_T.ACCOUNT_NO%TYPE;
BEGIN
    SELECT 'ACC'||LPAD(SQ_ACCOUNT_NO.NEXTVAL, 9,'0') INTO v_account_no FROM DUAL;
    RETURN v_account_no;
END;

-- ������� ����� ����� �������� ����� �� �������� INFRANET
FUNCTION New_account_no(
                   p_contract_no  IN VARCHAR2,
                   p_account_type IN CHAR     -- 'J'/'P'
               ) RETURN VARCHAR2 
IS
    v_account_no ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_count  INTEGER;
BEGIN
    v_account_no := SUBSTR(p_contract_no, 1, 8);
    -- ��������� �� ������� ����� �/� � �������
    SELECT COUNT(*) INTO v_count 
      FROM ACCOUNT_T A  
     WHERE A.ACCOUNT_NO = v_account_no;
    -- ���� �/� ��� ����������, �������� ����������� �����
    IF v_count > 0 THEN
      SELECT 'X'||p_account_type||LPAD(SQ_ACCOUNT_NO.NEXTVAL,6,'0') 
        INTO v_account_no
        FROM dual;
    END IF;
    RETURN v_account_no;
END;

-- ������� ����� ������� ����, ���������� ��������
--   - ������������� - ID �������� ����� �������, 
--   - ��� ������ ���������� ����������
FUNCTION New_account(
               p_account_no    IN VARCHAR2,
               p_account_type  IN VARCHAR2,
               p_currency_id   IN INTEGER,
               p_status        IN VARCHAR2,
               p_parent_id     IN INTEGER  DEFAULT NULL,
               p_notes         IN VARCHAR2 DEFAULT NULL,
               p_curr_conv_id  IN INTEGER  DEFAULT NULL,  -- ������� ����������� ������
               p_comment       IN VARCHAR2 DEFAULT NULL   -- ����������� � �����
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'New_account';
    v_account_id  INTEGER;
    v_billing_id  INTEGER;
BEGIN
    -- ��� ���. ���� ������� � �������� ���������, �.�. ������ ��� ����������� �������
    IF p_account_type = Pk00_Const.c_ACC_TYPE_P THEN
       v_billing_id := Pk00_Const.c_BILLING_MMTS;
    ELSE
       v_billing_id := Pk00_Const.c_BILLING_KTTK; --c_BILLING_MMTS;
    END IF;  
    -- ������� ������ �������� �����
    INSERT INTO ACCOUNT_T A(
       ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, CURRENCY_ID, 
       STATUS, PARENT_ID, NOTES,
       BALANCE, BALANCE_DATE, CREATE_DATE, BILLING_ID,
       CURRENCY_CONVERSION_ID, COMMENTARY
    )VALUES(
       Pk02_Poid.Next_account_id, p_account_no, p_account_type, p_currency_id, 
       p_status, p_parent_id, p_notes, 0, SYSDATE, SYSDATE, v_billing_id,
       p_curr_conv_id, p_comment
    )
    RETURNING ACCOUNT_ID INTO v_account_id;
    RETURN v_account_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- �������� ��������� ������ �������� �����, ���������� ��������
--   - ������������� - OK 
--   - ��� ������ ���������� ����������
PROCEDURE Edit_account(
               p_account_id    IN INTEGER,
               p_account_no    IN VARCHAR2,
               p_account_type  IN VARCHAR2,
               p_currency_id   IN INTEGER,
               p_status        IN VARCHAR2,
               p_parent_id     IN INTEGER,
               p_notes         IN VARCHAR2
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Edit_account';
BEGIN
    UPDATE ACCOUNT_T 
       SET ACCOUNT_NO   = NVL(p_account_no, ACCOUNT_NO),
           ACCOUNT_TYPE = NVL(p_account_type, ACCOUNT_TYPE),
           CURRENCY_ID  = NVL(p_currency_id, CURRENCY_ID),
           STATUS       = NVL(p_status, STATUS),
           PARENT_ID    = NVL(p_parent_id, PARENT_ID),
           NOTES        = NVL(p_notes, NOTES)
     WHERE ACCOUNT_ID   = p_account_id;  
    IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '� ������� ACCOUNT_T ��� ������ � ACCOUNT_ID='||p_account_id);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ������� ������� ����, �������� ��������, ������ ��� ����� ��������� �/�
--   - ��� ������ ���������� ����������
PROCEDURE Delete_account(
               p_account_id     IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_account';
BEGIN
    -- ������� ������� �������� �����
    DELETE ACCOUNT_PROFILE_T WHERE ACCOUNT_ID = p_account_id;
    -- ������� ���������� � ������ ��� ���������� �������� �����
    DELETE BILLINFO_T WHERE ACCOUNT_ID = p_account_id;
    -- ������� ������� ����, ���� �� ���� ���� ������� ������, ����� ������� ����������    
    DELETE ACCOUNT_T  WHERE ACCOUNT_ID = p_account_id;
    IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '� ������� ACCOUNT_T ��� ������ � ACCOUNT_ID='||p_account_id);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ����� ID � �������� �/� �� ������ ������ (����� � ���������)
--   - ������������� - ���-�� �����
--   - NULL - �� ������
--   - ��� ������ ���������� ����������
FUNCTION Find_account_by_no( 
               p_account_no  IN VARCHAR2
             ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_account';
    v_account_id INTEGER;
BEGIN
    SELECT ACCOUNT_ID  INTO v_account_id
      FROM ACCOUNT_T
     WHERE ACCOUNT_NO = p_account_no;
    RETURN v_account_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--
-- �������� ������ �� �������� ����� �� ��������� �������
--   - ��� ������ ���������� ���������
PROCEDURE Period_info( 
               p_recordset     OUT t_refc,
               p_account_id     IN INTEGER,
               p_period_id_from IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Period_info';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR
          SELECT REP_PERIOD_ID, ACCOUNT_ID, OPEN_BALANCE, CLOSE_BALANCE, 
                 TOTAL, GROSS, RECVD, ADVANCE, LAST_MODIFIED 
            FROM REP_PERIOD_INFO_T
           WHERE ACCOUNT_ID = p_account_id
             AND REP_PERIOD_ID >= p_period_id_from
          ORDER BY REP_PERIOD_ID
          ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- �������� ����� �������� �����
--   - ������ - ����� �������� ����� 
--   - ��� ������ ���������� ����������
FUNCTION Get_account_no(
               p_account_id     IN INTEGER
           ) RETURN VARCHAR2
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_account_no';
    v_account_no ACCOUNT_T.ACCOUNT_NO%TYPE;
BEGIN
    SELECT ACCOUNT_NO INTO v_account_no 
      FROM ACCOUNT_T
     WHERE ACCOUNT_ID = p_account_id;
    RETURN v_account_no;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ���������� ������� �������� �����, ���������� ��������
--   - ������������� - id ������� 
--   - ��� ������ ���������� ����������
FUNCTION Set_profile(
               p_account_id         IN INTEGER,
               p_brand_id           IN INTEGER,
               p_contract_id        IN INTEGER,
               p_customer_id        IN INTEGER,
               p_subscriber_id      IN INTEGER,
               p_contractor_id      IN INTEGER,
               p_branch_id          IN INTEGER,
               p_agent_id           IN INTEGER,
               p_contractor_bank_id IN INTEGER,
               p_vat                IN NUMBER,
               p_date_from          IN DATE,
               p_date_to            IN DATE
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_profile';
    v_profile_id INTEGER;
    v_billing_id INTEGER;
BEGIN
    -- ��������� �������� ���������� ��� Billing_id = 2003 
    SELECT BILLING_ID INTO v_billing_id
      FROM ACCOUNT_T A
     WHERE A.ACCOUNT_ID = p_account_id;
    IF v_billing_id = Pk00_Const.c_BILLING_MMTS AND p_contractor_bank_id NOT IN (1,2) THEN
        Pk01_Syslog.Raise_user_exception('account_id='||p_account_id||
                      'bank_id='||p_contractor_bank_id||' - ������� ������� ���������'
                     , c_PkgName||'.'||v_prcName );
    END IF;
    
    -- ��������� ������� ������, ���� ��� ����
    UPDATE ACCOUNT_PROFILE_T AP
       SET DATE_TO = p_date_from - 1/86400
     WHERE AP.ACCOUNT_ID = p_account_id
       AND (AP.DATE_TO IS NULL OR p_date_from <= AP.DATE_TO);
   
    -- ������������� ����� �����:
    INSERT INTO ACCOUNT_PROFILE_T (
       PROFILE_ID, ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, SUBSCRIBER_ID,
       CONTRACTOR_ID, BRANCH_ID, AGENT_ID,  
       CONTRACTOR_BANK_ID, VAT, BRAND_ID, DATE_FROM, DATE_TO)
    VALUES
       (Pk02_Poid.Next_account_profile_id, 
        p_account_id, p_contract_id, p_customer_id, p_subscriber_id,
        p_contractor_id, p_branch_id, p_agent_id, 
        p_contractor_bank_id, p_vat, p_brand_id, p_date_from, p_date_to)
    RETURNING PROFILE_ID INTO v_profile_id;
    RETURN v_profile_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ���������� ����� � �������� �����, ���������� ��������
--   - ������������� - id �������� 
--   - ��� ������ ���������� ����������
--
FUNCTION Add_address(
               p_account_id    IN INTEGER,
               p_address_type  IN VARCHAR2,
               p_country       IN VARCHAR2,
               p_zip           IN VARCHAR2,
               p_state         IN VARCHAR2,
               p_city          IN VARCHAR2,
               p_address       IN VARCHAR2,
               p_person        IN VARCHAR2,
               p_phones        IN VARCHAR2,
               p_fax           IN VARCHAR2,
               p_email         IN VARCHAR2,
               p_date_from     IN DATE,
               p_date_to       IN DATE DEFAULT NULL ,
               p_notes         IN VARCHAR2 DEFAULT NULL
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Add_address';
    v_contact_id INTEGER;
    v_date_from  DATE;
BEGIN       
    -- ��������� ������� ������, ���� ��� ����
    UPDATE ACCOUNT_CONTACT_T
       SET DATE_TO = p_date_from - 1/86400
     WHERE ACCOUNT_ID   = p_account_id
       AND ADDRESS_TYPE = p_address_type
       AND ( DATE_TO IS NULL OR DATE_TO >= p_date_from )
     RETURNING CONTACT_ID, DATE_FROM INTO v_contact_id, v_date_from;
    -- ����������� � ����������� ����������   
    IF p_date_from <= v_date_from THEN
       RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
                                  '���� ������ �������� ����� ������ '
                                 ||TO_CHAR(p_date_from,'dd.mm.yyyy')
                                 ||' ������ ��� �����, ��� ������� ������ '
                                 ||TO_CHAR(v_date_from,'dd.mm.yyyy')
                                 ||' , ACCOUNT_ID='||p_account_id
                                 ||' , ADDRESS_TYPE='||p_address_type
                               );
    END IF;  
    -- ��������� ����� ������
    INSERT INTO ACCOUNT_CONTACT_T (   
        CONTACT_ID,ADDRESS_TYPE,ACCOUNT_ID,
        COUNTRY,ZIP,STATE,CITY,ADDRESS,PERSON,
        PHONES,FAX,EMAIL,DATE_FROM,DATE_TO,NOTES
    )VALUES(
        SQ_ADDRESS_ID.NEXTVAL, p_address_type, p_account_id,
        p_country, p_zip, p_state, p_city, p_address, p_person,
        p_phones, p_fax, p_email, p_date_from, p_date_to, p_notes
    ) RETURNING CONTACT_ID INTO v_contact_id;
    RETURN v_contact_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ������������� ���������� ����� � �������� �����
--   - ��� ������ ���������� ����������
PROCEDURE Edit_address(
               p_contact_id    IN INTEGER,
               p_address_type  IN VARCHAR2,
               p_country       IN VARCHAR2,
               p_zip           IN VARCHAR2,
               p_state         IN VARCHAR2,
               p_city          IN VARCHAR2,
               p_address       IN VARCHAR2,
               p_person        IN VARCHAR2,
               p_phones        IN VARCHAR2,
               p_fax           IN VARCHAR2,
               p_email         IN VARCHAR2,
               p_date_from     IN DATE,
               p_date_to       IN DATE DEFAULT NULL ,
               p_notes         IN VARCHAR2 DEFAULT NULL
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Edit_address';
BEGIN
    UPDATE ACCOUNT_CONTACT_T 
       SET ADDRESS_TYPE = NVL(p_address_type, ADDRESS_TYPE),
           COUNTRY      = p_country,
           ZIP          = p_zip,
           STATE        = p_state,
           CITY         = p_city,
           ADDRESS      = p_address,
           PERSON       = NVL(p_person, PERSON),
           PHONES       = NVL(p_phones, PHONES),
           FAX          = NVL(p_fax, FAX),
           EMAIL        = NVL(p_email, EMAIL),
           DATE_FROM    = NVL(p_date_from, DATE_FROM),
           DATE_TO      = NVL(p_date_to, DATE_TO),
           NOTES        = NVL(p_notes, NOTES),
           MODIFY_DATE  = SYSDATE
     WHERE CONTACT_ID    = p_contact_id;  
    IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '� ������� ACCOUNT_NAME_INFO_T ��� ������ � CONTACT_ID='||p_contact_id);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������� ���������� ����� � �������� �����
--   - ��� ������ ���������� ����������
PROCEDURE Delete_address(
               p_contact_id    IN INTEGER
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_address';
BEGIN
    DELETE FROM ACCOUNT_CONTACT_T WHERE CONTACT_ID = p_contact_id;
    IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '� ������� ACCOUNT_NAME_INFO_T ��� ������ � CONTACT_ID='||p_contact_id);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������ ������� (���������) �� ������� �����
--   - ������������� - ���-�� �����
--   - ��� ������ ���������� ���������
--
FUNCTION Address_list( 
               p_recordset   OUT t_refc,
               p_account_id   IN INTEGER
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Address_list';
    v_retcode    INTEGER := 0;
BEGIN
    -- �������� ���-�������� � ������
    SELECT COUNT(*) INTO v_retcode
      FROM ACCOUNT_CONTACT_T
     WHERE ACCOUNT_ID = p_account_id;
    -- ��������� ������
    OPEN p_recordset FOR
         SELECT CONTACT_ID,ADDRESS_TYPE,ACCOUNT_ID,
                COUNTRY,ZIP,STATE,CITY,ADDRESS,PERSON,
                PHONES,FAX,EMAIL,NOTES
           FROM ACCOUNT_CONTACT_T
          WHERE ACCOUNT_ID = p_account_id
          ORDER BY ADDRESS_TYPE;
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������������� �������� �������� �����
--   - ������������� - id �������� 
--   - ��� ������ ���������� ����������
--
PROCEDURE Edit_contact(
               p_account_id    IN INTEGER,
               p_phones        IN VARCHAR2,
               p_fax           IN VARCHAR2,
               p_email         IN VARCHAR2
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Edit_contact';
    v_cnt        INTEGER;
BEGIN       
    SELECT COUNT(*) 
           INTO v_cnt FROM ACCOUNT_CONTACT_T
    WHERE 
           ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_DLV           
           AND ACCOUNT_ID = p_account_id
           AND (SYSDATE BETWEEN DATE_FROM AND DATE_TO OR (DATE_FROM <= SYSDATE AND DATE_TO IS NULL));
    
    IF v_cnt > 0 THEN
       UPDATE ACCOUNT_CONTACT_T
           SET PHONES = p_phones,
               FAX = p_fax,
               EMAIL = p_email
    WHERE 
           ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_DLV           
           AND ACCOUNT_ID = p_account_id
           AND (SYSDATE BETWEEN DATE_FROM AND DATE_TO OR (DATE_FROM <= SYSDATE AND DATE_TO IS NULL));
    ELSE
       INSERT INTO ACCOUNT_CONTACT_T (   
            CONTACT_ID,ADDRESS_TYPE,ACCOUNT_ID,
            PHONES,FAX,EMAIL,DATE_FROM
        )VALUES(
            SQ_ADDRESS_ID.NEXTVAL, Pk00_Const.c_ADDR_TYPE_DLV, p_account_id,            
            p_phones, p_fax, p_email, SYSDATE
        ); 
    END IF;   
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ��� ����� ���������� �������� �����, ����������
--   - ������������� - ���-�� ��������� �������
--   - ��� ������ ���������� ���������
--
FUNCTION Bills_list ( 
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID ������� �����
               p_date_from  IN DATE DEFAULT TO_DATE('01.01.2000','dd.mm.yyyy'),
               p_date_to    IN DATE DEFAULT TO_DATE('01.01.2050','dd.mm.yyyy')
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bills_list';
    v_count      INTEGER;
    v_retcode    INTEGER;
BEGIN
    -- �������� ���-�� �����, ������� ������� ������
    SELECT COUNT(*) INTO v_count 
      FROM BILL_T 
     WHERE ACCOUNT_ID = p_account_id
       AND BILL_DATE BETWEEN p_date_from AND p_date_to;
    -- ��������� ������
    OPEN p_recordset FOR
         SELECT BILL_ID, ACCOUNT_ID, BILL_NO, BILL_DATE, BILL_TYPE, BILL_STATUS,
                CURRENCY_ID, REP_PERIOD_ID, TOTAL, 
                RECVD, DUE, DUE_DATE, 
                NOTES
           FROM BILL_T
          WHERE ACCOUNT_ID = p_account_id
            AND BILL_DATE BETWEEN p_date_from AND p_date_to
           ORDER BY BILL_DATE;
    RETURN v_count;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- �������� ������ ������� �� ������� �����
--   - ������������� - ���-�� �������
--   - ��� ������ ���������� ���������
--
FUNCTION Order_list( 
               p_recordset   OUT t_refc,
               p_account_id   IN INTEGER
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Order_list';
    v_retcode    INTEGER := c_RET_OK;
BEGIN
    SELECT COUNT(*) INTO v_retcode
      FROM ORDER_T
     WHERE ACCOUNT_ID = p_account_id;

    OPEN p_recordset FOR
         SELECT ORDER_ID, ORDER_NO, ACCOUNT_ID, SERVICE_ID, RATEPLAN_ID, DATE_FROM, DATE_TO 
           FROM ORDER_T
          WHERE ACCOUNT_ID = p_account_id
          ORDER BY DATE_FROM;
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ��������� ��������� �������������� �/�, ���������� ��������
-- - ��� ������ ���������� ����������
PROCEDURE Set_manager (
               p_account_id IN INTEGER,
               p_manager_id IN INTEGER,
               p_date_from  IN DATE DEFAULT SYSDATE
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_manager';
    v_date_from  DATE := TRUNC(p_date_from);
BEGIN
    -- ��������� ���������� ������
    UPDATE SALE_CURATOR_T
       SET DATE_TO = v_date_from - 1/86400
     WHERE MANAGER_ID != p_manager_id
       AND ACCOUNT_ID = p_account_id
       AND DATE_TO IS NULL;
    -- ��������� ����� ������
    INSERT INTO SALE_CURATOR_T (MANAGER_ID, ACCOUNT_ID, DATE_FROM)
    VALUES(p_manager_id, p_account_id, v_date_from);
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ID ��������� �������������� �/�, ���������� ��������
-- - ������������� - ID ���������
-- - NULL - ��� ������
-- - ��� ������ ���������� ����������
FUNCTION Get_manager_id (
               p_account_id  IN INTEGER,
               p_date      IN DATE DEFAULT SYSDATE
           ) RETURN INTEGER 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_manager_id';
    v_manager_id INTEGER;
BEGIN
    SELECT MANAGER_ID INTO v_manager_id
      FROM SALE_CURATOR_T
     WHERE ACCOUNT_ID = p_account_id
       AND p_date BETWEEN DATE_FROM AND DATE_TO;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ���� �������� �������� (� �������� �����)
PROCEDURE Edit_contact_datecreate( 
               p_account_id     IN INTEGER,
               p_datecreate     IN DATE
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Edit_contact_datecreate';
BEGIN
    -- ��������� ������� � �������� �����
    UPDATE account_profile_t
       SET date_from = p_datecreate
     WHERE profile_id = (SELECT PROFILE_ID
                           FROM (SELECT pr.*,
                                        ROW_NUMBER () OVER (ORDER BY date_from)
                                           AS rn
                                   FROM account_profile_t pr
                                  WHERE account_id = p_account_id)
                          WHERE rn = 1);
                          
    -- ��������� ���� ��������
    UPDATE contract_t
       SET date_from = p_datecreate
     WHERE contract_id = (SELECT contract_id
                       FROM (SELECT pr.*,
                                    ROW_NUMBER () OVER (ORDER BY date_from)
                                       AS rn
                               FROM account_profile_t pr
                              WHERE account_id = p_account_id)
                      WHERE rn = 1);
     EXCEPTION
    WHEN OTHERS THEN        
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION,   c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ��������� ������� � ������� ������������� �/�
-- - ��� ������ ���������� ����������
PROCEDURE Set_billing (
               p_account_id IN INTEGER,
               p_billing_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_manager';
BEGIN
    UPDATE ACCOUNT_T
       SET BILLING_ID = p_billing_id
     WHERE ACCOUNT_ID = p_account_id;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� �� �/� ����������� ��� �����-�������
-- - ��� ������ ���������� ����������
PROCEDURE Set_payer (
               p_account_id IN INTEGER,
               p_payer_id   IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_payer';
BEGIN
    UPDATE ACCOUNT_PROFILE_T
       SET CUSTOMER_PAYER_ID = p_payer_id
     WHERE ACCOUNT_ID = p_account_id;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- -------------------------------------------------------------------------- --
-- ��������� ���������� ������ �� �/�
-- -------------------------------------------------------------------------- --
PROCEDURE Change_seller (
            p_account_no         IN VARCHAR2,
            p_contractor_id      IN INTEGER,
            p_contractor_bank_id IN INTEGER,
            p_period_id          IN INTEGER  -- ��� ������ ������� ���������
          )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'Change_seller';
    v_region_id          INTEGER;
    v_date_from          DATE;
    v_date_to            DATE; 
    v_profile_id         INTEGER;
    v_account_id         INTEGER;
    v_profile_id_old     INTEGER;
    v_date_to_old        DATE;
    v_region_id_old      INTEGER;
    v_count              INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, account_no='||p_account_no
                             ||', contractor_id='||p_contractor_id
                             ||', contractor_bank_id='||p_contractor_bank_id
                             ||', period_id='||p_period_id, 
                           c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ���� ������ �������� �������
    v_date_from := PK04_PERIOD.Period_from(p_period_id);
    -- ���� ��������� ����������� �������
    v_date_to   := v_date_from - 1/86400; 
    -- ����� ������ 
    SELECT CT.REGION_ID INTO v_region_id 
      FROM CONTRACTOR_T CT
     WHERE CT.CONTRACTOR_ID = p_contractor_id;
    --
    Pk01_Syslog.Write_msg('new region_id='||v_region_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    SELECT A.ACCOUNT_ID, AP.PROFILE_ID, AP.DATE_TO, CT.REGION_ID
      INTO v_account_id, v_profile_id_old, v_date_to_old, v_region_id_old
      FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A, CONTRACTOR_T CT
     WHERE AP.CONTRACTOR_ID = CT.CONTRACTOR_ID
       AND AP.ACCOUNT_ID    = A.ACCOUNT_ID
    --   AND AP.DATE_FROM   < TO_DATE('28.02.2015 23:59:59','dd.mm.yyyy hh24:mi:ss')
       AND ( AP.DATE_TO IS NULL OR v_date_from <  AP.DATE_TO )
       AND A.ACCOUNT_NO = p_account_no;
    --
    Pk01_Syslog.Write_msg('old profile_id='||v_profile_id_old||', old region_id='||v_region_id_old, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
     -- ��������� ������ �������
    UPDATE ACCOUNT_PROFILE_T AP SET DATE_TO = v_date_to
     WHERE AP.PROFILE_ID = v_profile_id_old
       AND AP.ACCOUNT_ID = v_account_id
    ;
    v_count := SQL%ROWCOUNT;
     
    -- ������� ����� �������
    v_profile_id := SQ_ACCOUNT_ID.NEXTVAL;
          
    INSERT INTO ACCOUNT_PROFILE_T (
       PROFILE_ID, ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, SUBSCRIBER_ID, 
       CONTRACTOR_ID, BRANCH_ID, AGENT_ID, CONTRACTOR_BANK_ID, VAT, 
       DATE_FROM, DATE_TO, CUSTOMER_PAYER_ID, BRAND_ID
    )
    SELECT v_profile_id, ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, SUBSCRIBER_ID, 
           p_contractor_id, BRANCH_ID, AGENT_ID, 
           p_contractor_bank_id, VAT, 
           v_date_from DATE_FROM, v_date_to_old DATE_TO, 
           CUSTOMER_PAYER_ID, BRAND_ID 
      FROM ACCOUNT_PROFILE_T
      WHERE PROFILE_ID = v_profile_id_old
    ;
    Pk01_Syslog.Write_msg('new profile_id='||v_profile_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- �������� ���� �����  
    UPDATE BILL_T B 
       SET B.CONTRACTOR_ID = p_contractor_id,
           B.CONTRACTOR_BANK_ID = p_contractor_bank_id,
           B.PROFILE_ID = v_profile_id,
           B.BILL_NO = 
           CASE 
              WHEN v_region_id_old IS NULL THEN
                LPAD(TO_CHAR(v_region_id), 4,'0')||'/'||B.BILL_NO
              ELSE
                LPAD(TO_CHAR(v_region_id), 4,'0')||'/'||SUBSTR(B.BILL_NO,6)
           END
     WHERE B.REP_PERIOD_ID >= p_period_id
       AND B.ACCOUNT_ID = v_account_id
       AND B.PROFILE_ID = v_profile_id_old;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ���������� ID �������� ��������
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


END PK05_ACCOUNT;
/
