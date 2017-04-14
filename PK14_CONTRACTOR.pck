CREATE OR REPLACE PACKAGE PK14_CONTRACTOR
IS
    --
    -- ����� ��� ������ � �������� "����������", �������:
    -- contractor_t, contractor_addres_t, contractor_bank_t, signer_t, signature_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK14_CONTRACTOR';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- ������� ������ �����������, ���������� ��������
    --   - ������������� - ID �����������, 
    --   - ��� ������ ���������� ����������
    FUNCTION New_contractor(
                   p_type        IN VARCHAR2,
                   p_erp_code    IN VARCHAR2,
                   p_inn         IN VARCHAR2,
                   p_kpp         IN VARCHAR2, 
                   p_name        IN VARCHAR2,
                   p_short_name  IN VARCHAR2,
                   p_parent_id   IN INTEGER,
                   p_notes       IN VARCHAR2 DEFAULT NULL
               ) RETURN INTEGER;
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ��������� �����������, ���������� ��������
    --   - ��� ������ ���������� ����������
    --
    PROCEDURE Edit_contractor(
                   p_contractor_id IN INTEGER,
                   p_parent_id     IN INTEGER,
                   p_type          IN VARCHAR2, 
                   p_erp_code      IN VARCHAR2,
                   p_inn           IN VARCHAR2,
                   p_kpp           IN VARCHAR2, 
                   p_name          IN VARCHAR2,
                   p_short_name    IN VARCHAR2,
                   p_notes         IN VARCHAR2 DEFAULT NULL
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    -- ����� ID ����������� �� ������ ����� (����� � ���������), ���, ���
    --   - ������������� - ���-�� �������
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Find( 
                   p_recordset OUT t_refc, 
                   p_name       IN VARCHAR2,
                   p_inn        IN VARCHAR2,
                   p_kpp        IN VARCHAR2
                 ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� �����������, ���������� ��������
    --   - ��� ������ ���������� ���������� 
    PROCEDURE Delete_contractor(
                   p_contractor_id IN INTEGER
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������� ����������� ����� �����������, ���������� ��������
    --   - ������������� - id ������ ������ 
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Set_address(
                   p_contractor_id IN INTEGER,
                   p_address_type  IN VARCHAR2,
                   p_country       IN VARCHAR2, 
                   p_zip           IN VARCHAR2,
                   p_state         IN VARCHAR2,
                   p_city          IN VARCHAR2, 
                   p_address       IN VARCHAR2,
                   p_phone_account IN VARCHAR2,
                   p_phone_billing IN VARCHAR2,
                   p_fax           IN VARCHAR2,
                   p_email         IN VARCHAR2,
                   p_date_from     IN DATE,
                   p_date_to       IN DATE
               ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    -- �������� ����������� ����� ����������� �� contractor_id � ����
    --   - ��� ������ ���������� ����������
    --
    PROCEDURE Get_address( 
                   p_recordset     OUT t_refc, 
                   p_contractor_id  IN INTEGER,
                   p_date           IN DATE DEFAULT SYSDATE
                 );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������� ���������� ��������� �����������, ���������� ��������
    --   - ������������� - id ������ ��������� ���������� ����������� 
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Set_bank(
                   p_contractor_id     IN INTEGER,
                   p_bank_name         IN VARCHAR2, -- ������������ �����
                   p_bank_code         IN VARCHAR2, -- ���
                   p_bank_corr_account IN VARCHAR2, -- ����������������� ����
                   p_bank_settlement   IN VARCHAR2, -- ��������� ���� ����������� � �����
                   p_date_from         IN DATE,
                   p_date_to           IN DATE
               ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    -- �������� ���������� ��������� ����������� �� contractor_id � ����
    --   - ��� ������ ���������� ����������
    --
    PROCEDURE Get_bank( 
                   p_recordset     OUT t_refc, 
                   p_contractor_id  IN INTEGER,
                   p_date           IN DATE DEFAULT SYSDATE
                 );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������� ���������� �� �����������, ���������� ��������
    --   - ������������� - id ������ ���������� 
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Set_signer(
                   p_contractor_id     IN INTEGER,  -- ID ����������
                   p_signer_name       IN VARCHAR2, -- ��� ����������
                   p_attorney_no       IN VARCHAR2, -- ����� ������������
                   p_signer_role       IN VARCHAR2, -- ���� ����������: '������������...'/'��. ���������'
                   p_date_from         IN DATE,
                   p_date_to           IN DATE
               ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    -- �������� �������� ����������� ����������� �� contractor_id � ����
    --   - ��� ������ ���������� ����������
    --
    PROCEDURE Get_signer( 
                   p_recordset     OUT t_refc, 
                   p_contractor_id  IN INTEGER,
                   p_date           IN DATE DEFAULT SYSDATE
                 );
    
    -- ��������� ������ ������������ (������������)
    --  - ���-�� �������
    --  - ��� ������ ���������� ����������
    FUNCTION Contractor_tree(p_recordset out t_refc) RETURN INTEGER;
    
END PK14_CONTRACTOR;
/
CREATE OR REPLACE PACKAGE BODY PK14_CONTRACTOR
IS

-- ������� ������ �����������, ���������� ��������
--   - ������������� - ID �����������, 
--   - ��� ������ ���������� ����������
FUNCTION New_contractor(
               p_type        IN VARCHAR2,
               p_erp_code    IN VARCHAR2,
               p_inn         IN VARCHAR2,
               p_kpp         IN VARCHAR2, 
               p_name        IN VARCHAR2,
               p_short_name  IN VARCHAR2,
               p_parent_id   IN INTEGER,
               p_notes       IN VARCHAR2 DEFAULT NULL
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'New_contractor';
    v_contractor_id INTEGER;
BEGIN
    INSERT INTO CONTRACTOR_T (
        CONTRACTOR_ID, CONTRACTOR_TYPE, ERP_CODE, INN, KPP, 
        CONTRACTOR, SHORT_NAME, PARENT_ID, NOTES
    )VALUES(
        SQ_CLIENT_ID.NEXTVAL, p_type, p_erp_code, p_inn, p_kpp, 
        p_name, p_short_name, p_parent_id, p_notes
    )RETURNING CONTRACTOR_ID INTO v_contractor_id;
    RETURN v_contractor_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

               
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ��������� �����������, ���������� ��������
--   - ��� ������ ���������� ����������
PROCEDURE Edit_contractor(
               p_contractor_id IN INTEGER,
               p_parent_id     IN INTEGER,
               p_type          IN VARCHAR2, 
               p_erp_code      IN VARCHAR2,
               p_inn           IN VARCHAR2,
               p_kpp           IN VARCHAR2, 
               p_name          IN VARCHAR2,
               p_short_name    IN VARCHAR2,
               p_notes         IN VARCHAR2 DEFAULT NULL
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Edit_contractor';
BEGIN
    UPDATE CONTRACTOR_T 
       SET PARENT_ID  = NVL(p_parent_id, PARENT_ID),
           CONTRACTOR_TYPE = NVL(p_type, CONTRACTOR_TYPE),
           ERP_CODE   = NVL(p_erp_code, ERP_CODE), 
           INN        = NVL(p_inn, INN), 
           KPP        = NVL(p_kpp, KPP),  
           CONTRACTOR = NVL(p_name, CONTRACTOR), 
           SHORT_NAME = NVL(p_short_name , SHORT_NAME),
           NOTES      = NVL(p_notes, NOTES)
     WHERE CONTRACTOR_ID = p_contractor_id;  
    IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(-20000, '� ������� CONTRACTOR_T ��� ������ � CONTRACTOR_ID='||p_contractor_id);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- ����� ID ����������� �� ������ ����� (����� � ���������), ���, ���
--   - ������������� - ���-�� �������
--   - ��� ������ ���������� ����������
--
FUNCTION Find( 
               p_recordset OUT t_refc, 
               p_name       IN VARCHAR2,
               p_inn        IN VARCHAR2,
               p_kpp        IN VARCHAR2
             ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_by_name';
    v_retcode    INTEGER := c_RET_OK;
BEGIN
    SELECT COUNT(*) INTO v_retcode 
      FROM CONTRACTOR_T
     WHERE UPPER(CONTRACTOR) LIKE UPPER(p_name)||'%'
       AND INN = NVL(p_inn, INN)
       AND KPP = NVL(p_kpp, KPP);  

    OPEN p_recordset FOR
         SELECT CONTRACTOR_ID, PARENT_ID, CONTRACTOR_TYPE,
                ERP_CODE, INN, KPP, CONTRACTOR 
           FROM CONTRACTOR_T
          WHERE UPPER(CONTRACTOR) LIKE UPPER(p_name)||'%'
            AND INN = NVL(p_inn, INN)
            AND KPP = NVL(p_kpp, KPP)
          ORDER BY CONTRACTOR;
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
-- ������� �����������, ���������� ��������
--   - ��� ������ ���������� ����������
--
PROCEDURE Delete_contractor(
               p_contractor_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_contractor';
BEGIN
    -- ������� ������ ������������ ������, ���������� 
    DELETE CONTRACTOR_ADDRESS_T WHERE CONTRACTOR_ID = p_contractor_id;
    -- ������� ����������, ���� �� ���� ���� ������� ������, ����� ������� ����������
    DELETE CONTRACTOR_T WHERE CONTRACTOR_ID = p_contractor_id;
    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '� ������� CONTRACTOR_T ��� ������ � CONTRACTOR_ID='||p_contractor_id);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ���������� ����������� ����� �����������, ���������� ��������
--   - ������������� - id ������ ������ 
--   - ��� ������ ���������� ����������
--
FUNCTION Set_address(
               p_contractor_id IN INTEGER,
               p_address_type  IN VARCHAR2,
               p_country       IN VARCHAR2, 
               p_zip           IN VARCHAR2,
               p_state         IN VARCHAR2,
               p_city          IN VARCHAR2, 
               p_address       IN VARCHAR2,
               p_phone_account IN VARCHAR2,
               p_phone_billing IN VARCHAR2,
               p_fax           IN VARCHAR2,
               p_email         IN VARCHAR2,
               p_date_from     IN DATE,
               p_date_to       IN DATE
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_address';
    v_address_id INTEGER;
    v_date_from  DATE;
BEGIN
    -- ��������� ������� ������, ���� ��� ����
    UPDATE CONTRACTOR_ADDRESS_T
       SET DATE_TO = p_date_from - 1/86400
     WHERE CONTRACTOR_ID = p_contractor_id
       AND ADDRESS_TYPE = p_address_type
       AND ( DATE_TO IS NULL OR DATE_TO < =p_date_from )
     RETURNING ADDRESS_ID, DATE_FROM INTO v_address_id, v_date_from;
    -- ����������� � ����������� ����������   
    IF p_date_from <= v_date_from THEN
       RAISE_APPLICATION_ERROR( Pk01_Syslog.n_APP_EXCEPTION, 
                                 '���� ������ �������� ����� ������ '
                                ||TO_DATE(p_date_from,'dd.mm.yyyy')
                                ||' ������, ��� ������� ������ '
                                ||TO_DATE(v_date_from,'dd.mm.yyyy')
                                ||' , CONTRACTOR_ID='||p_contractor_id
                                ||' , ADDRESS_TYPE='||p_address_type
                               );
    END IF;
    -- ������������� ����� �����:
    INSERT INTO CONTRACTOR_ADDRESS_T (
       ADDRESS_ID, ADDRESS_TYPE, COUNTRY, ZIP, STATE, CITY, ADDRESS, 
       PHONE_ACCOUNT, PHONE_BILLING, FAX, EMAIL,
       DATE_FROM, DATE_TO, CONTRACTOR_ID
    )VALUES(
       SQ_ADDRESS_ID.NEXTVAL, p_address_type, p_country, p_zip, p_state, p_city, p_address, 
       p_phone_account, p_phone_billing, p_fax, p_email,
       p_date_from, p_date_to, p_contractor_id
    ) RETURNING ADDRESS_ID INTO v_address_id;
    RETURN v_address_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- ����� ����������� ����� ����������� �� contractor_id � ����
-- - ��� ������ ���������� ����������
--
PROCEDURE Get_address( 
               p_recordset     OUT t_refc, 
               p_contractor_id  IN INTEGER,
               p_date           IN DATE DEFAULT SYSDATE
             )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_address';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR
         SELECT ADDRESS_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS,
                PHONE_ACCOUNT, PHONE_BILLING, FAX, EMAIL, 
                DATE_FROM, DATE_TO, CONTRACTOR_ID 
           FROM CONTRACTOR_ADDRESS_T
          WHERE CONTRACTOR_ID = p_contractor_id
            AND p_date BETWEEN DATE_FROM AND DATE_TO
          ORDER BY DATE_FROM;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
           
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ���������� ���������� ��������� �����������, ���������� ��������
--   - ������������� - id ������ ��������� ���������� ����������� 
--   - ��� ������ ���������� ����������
--
FUNCTION Set_bank(
               p_contractor_id     IN INTEGER,
               p_bank_name         IN VARCHAR2, -- ������������ �����
               p_bank_code         IN VARCHAR2, -- ���
               p_bank_corr_account IN VARCHAR2, -- ����������������� ����
               p_bank_settlement   IN VARCHAR2, -- ��������� ���� ����������� � �����
               p_date_from         IN DATE,
               p_date_to           IN DATE
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_bank';
    v_bank_id    INTEGER;
    v_date_from  DATE;
BEGIN
    -- ��������� ������� ������, ���� ��� ����
    UPDATE CONTRACTOR_BANK_T
       SET DATE_TO = p_date_from - 1/86400
     WHERE CONTRACTOR_ID = p_contractor_id
       AND ( DATE_TO IS NULL OR DATE_TO < =p_date_from )
     RETURNING BANK_ID, DATE_FROM INTO v_bank_id, v_date_from;
    -- ������� ���������� ������, ���� ��� ��������   
    IF v_date_from <= p_date_from THEN
       DELETE FROM CONTRACTOR_BANK_T WHERE BANK_ID = v_bank_id;
    END IF;
    -- ������������� ����� �����:
    INSERT INTO CONTRACTOR_BANK_T 
       (BANK_ID, BANK_NAME, BANK_CODE, BANK_CORR_ACCOUNT, BANK_SETTLEMENT, DATE_FROM, DATE_TO, CONTRACTOR_ID)
    VALUES
       (SQ_CLIENT_ID.NEXTVAL, p_bank_name, p_bank_code, p_bank_corr_account,
        p_bank_settlement, p_date_from, p_date_to, p_contractor_id)
    RETURNING BANK_ID INTO v_bank_id;
    RETURN v_bank_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- �������� ���������� ��������� ����������� �� contractor_id � ����
--   - ��� ������ ���������� ����������
--
PROCEDURE Get_bank( 
               p_recordset     OUT t_refc, 
               p_contractor_id  IN INTEGER,
               p_date           IN DATE DEFAULT SYSDATE
             )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_bank';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR
         SELECT BANK_ID, 
                BANK_NAME,             -- ������������ �����
                BANK_CODE,             -- ���
                BANK_CORR_ACCOUNT,     -- ����������������� ����
                BANK_SETTLEMENT,       -- ��������� ���� ����������� � �����
                DATE_FROM, DATE_TO 
           FROM CONTRACTOR_BANK_T
          WHERE CONTRACTOR_ID = p_contractor_id
            AND p_date BETWEEN DATE_FROM AND DATE_TO
          ORDER BY DATE_FROM;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ���������� ���������� �� �����������, ���������� ��������
--   - ������������� - id ������ ��������� ���������� ����������� 
--   - ��� ������ ���������� ����������
--
FUNCTION Set_signer(
               p_contractor_id     IN INTEGER, -- ID ����������
               p_signer_name       IN VARCHAR2, -- ��� ����������
               p_attorney_no       IN VARCHAR2, -- ����� ������������
               p_signer_role       IN VARCHAR2, -- ���� ����������: '������������...'/'��. ���������'
               p_date_from         IN DATE,
               p_date_to           IN DATE
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_signer';
    v_signer_id  INTEGER;
    v_date_from  DATE;
BEGIN
    -- ��������� ������� ������, ���� ��� ����
    UPDATE SIGNER_T
       SET DATE_TO = p_date_from - 1/86400
     WHERE CONTRACTOR_ID = p_contractor_id
       AND ( DATE_TO IS NULL OR DATE_TO < =p_date_from )
     RETURNING SIGNER_ID, DATE_FROM INTO v_signer_id, v_date_from;
    -- ������� ���������� ������, ���� ��� ��������   
    IF v_date_from <= p_date_from THEN
       DELETE FROM SIGNER_T WHERE SIGNER_ID = v_signer_id;
    END IF;
    -- ������������� ����� �����:
    INSERT INTO SIGNER_T 
       (SIGNER_ID, SIGNER_NAME, ATTORNEY_NO, SIGNER_ROLE, CONTRACTOR_ID, DATE_FROM, DATE_TO)
    VALUES
       (SQ_CLIENT_ID.NEXTVAL, p_signer_name, p_attorney_no, p_signer_role,
        p_contractor_id, p_date_from, p_date_to)
    RETURNING SIGNER_ID INTO v_signer_id;
    RETURN v_signer_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- �������� �������� ����������� ����������� �� contractor_id � ����
--   - ��� ������ ���������� ����������
--
PROCEDURE Get_signer( 
               p_recordset     OUT t_refc, 
               p_contractor_id  IN INTEGER,
               p_date           IN DATE DEFAULT SYSDATE
             )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_signer';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR
         SELECT SIGNER_ID, 
                SIGNER_NAME,
                ATTORNEY_NO, 
                SIGNER_ROLE,
                DATE_FROM, DATE_TO 
           FROM SIGNER_T
          WHERE CONTRACTOR_ID = p_contractor_id
            AND p_date BETWEEN DATE_FROM AND DATE_TO
          ORDER BY DATE_FROM;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ��������� ������ ������������ (������������)
--   - ���-�� �������
--   - ��� ������ ���������� ����������
FUNCTION Contractor_tree(p_recordset out t_refc) RETURN INTEGER
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
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK14_CONTRACTOR;
/
