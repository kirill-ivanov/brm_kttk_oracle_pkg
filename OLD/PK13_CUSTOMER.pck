CREATE OR REPLACE PACKAGE PK13_CUSTOMER
IS
    --
    -- ����� ��� ������ � �������� "����������", �������:
    -- customer_t, customer_addres_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK13_CUSTOMER';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- ������� ������ ����������, ���������� ��������
    --   - ������������� - ID ����������, 
    --   - ��� ������ ���������� ����������
    FUNCTION New_customer(
                   p_erp_code    IN VARCHAR2,
                   p_inn         IN VARCHAR2,
                   p_kpp         IN VARCHAR2, 
                   p_name        IN VARCHAR2,
                   p_short_name  IN VARCHAR2,
                   p_notes       IN VARCHAR2 DEFAULT NULL
               ) RETURN INTEGER;
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ��������� ����������, ���������� ��������
    --   - ��� ������ ���������� ����������
    --
    PROCEDURE Edit_customer(
                   p_customer_id IN INTEGER,
                   p_parent_id   IN INTEGER, 
                   p_erp_code    IN VARCHAR2,
                   p_inn         IN VARCHAR2,
                   p_kpp         IN VARCHAR2, 
                   p_name        IN VARCHAR2,
                   p_short_name  IN VARCHAR2,
                   p_notes       IN VARCHAR2
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ����������, ���������� ��������
    --   - ��� ������ ���������� ����������
    --
    PROCEDURE Delete_customer(
                   p_customer_id IN INTEGER
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    -- ����� ID ���������� �� ������ ����� (����� � ���������)
    --   - ������������� - ���-�� �������
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Find_by_name( 
                   p_recordset OUT t_refc, 
                   p_name       IN VARCHAR2,
                   p_short_name IN VARCHAR2
                 ) RETURN INTEGER;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- ����� ID ����������� �� ������ ����� (����� � ���������), ���, ���
--
PROCEDURE Find_customer( 
          p_recordset OUT t_refc, 
          p_erpcode    IN VARCHAR2,
          p_name       IN VARCHAR2,
          p_inn        IN VARCHAR2,
          p_kpp        IN VARCHAR2
);

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- ����� ����������� �� ��� ID
--
PROCEDURE Find_customer( 
          p_recordset OUT t_refc, 
          p_id       IN INTEGER
);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������� ���������� ��������� ����������, ���������� ��������
    --   - ��� ������ ���������� ����������
    --
    PROCEDURE Set_bank(
                   p_customer_id       IN INTEGER,
                   p_bank_name         IN VARCHAR2, -- ������������ �����
                   p_bank_code         IN VARCHAR2, -- ���
                   p_bank_corr_account IN VARCHAR2, -- ����������������� ����
                   p_bank_settlement   IN VARCHAR2  -- ��������� ���� ����������� � �����
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������� ����������� ����� ����������, ���������� ��������
    --   - ������������� - id ������ ������ 
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Set_address(
                   p_customer_id  IN VARCHAR2,
                   p_address_type IN VARCHAR2,
                   p_country      IN VARCHAR2, 
                   p_zip          IN VARCHAR2,
                   p_state        IN VARCHAR2,
                   p_city         IN VARCHAR2, 
                   p_address      IN VARCHAR2,
                   p_date_from    IN DATE,
                   p_date_to      IN DATE
               ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    -- ����� ����������� ����� ���������� �� customer_id � ����
    --   - ��� ������ ���������� ����������
    --
    PROCEDURE Get_address( 
                   p_recordset   OUT t_refc, 
                   p_customer_id  IN INTEGER,
                   p_date         IN DATE DEFAULT SYSDATE
                 );

    
END PK13_CUSTOMER;
/
CREATE OR REPLACE PACKAGE BODY PK13_CUSTOMER
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������� ������ ����������, ���������� ��������
--   - ������������� - ID ����������, 
--   - ��� ������ ���������� ����������
--
FUNCTION New_customer(
               p_erp_code    IN VARCHAR2,
               p_inn         IN VARCHAR2,
               p_kpp         IN VARCHAR2, 
               p_name        IN VARCHAR2,
               p_short_name  IN VARCHAR2,
               p_notes       IN VARCHAR2 DEFAULT NULL
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'New_customer';
    v_customer_id INTEGER;
BEGIN
    INSERT INTO CUSTOMER_T (CUSTOMER_ID, ERP_CODE, INN, KPP, CUSTOMER, SHORT_NAME, NOTES)
    VALUES(SQ_CLIENT_ID.NEXTVAL, p_erp_code, p_inn, p_kpp, p_name, p_short_name, p_notes)
    RETURNING CUSTOMER_ID INTO v_customer_id;
    RETURN v_customer_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.'||p_short_name, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ��������� ����������, ���������� ��������
--   - ��� ������ ���������� ����������
--
PROCEDURE Edit_customer(
               p_customer_id IN INTEGER,
               p_parent_id   IN INTEGER, 
               p_erp_code    IN VARCHAR2,
               p_inn         IN VARCHAR2,
               p_kpp         IN VARCHAR2, 
               p_name        IN VARCHAR2,
               p_short_name  IN VARCHAR2,
               p_notes       IN VARCHAR2
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Edit_customer';
BEGIN
    UPDATE CUSTOMER_T 
       SET PARENT_ID  = NVL(p_parent_id, PARENT_ID),
           ERP_CODE   = NVL(p_erp_code, ERP_CODE), 
           INN        = NVL(p_inn, INN), 
           KPP        = NVL(p_kpp, KPP),  
           CUSTOMER   = NVL(p_name,CUSTOMER), 
           SHORT_NAME = NVL(p_short_name , SHORT_NAME),
           NOTES      = NVL(p_notes, NOTES)
     WHERE CUSTOMER_ID = p_customer_id;  
    IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(-20000, '� ������� CUSTOMER_T ��� ������ � CUSTOMER_ID='||p_customer_id);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������� ����������, ���������� ��������
--   - ��� ������ ���������� ����������
--
PROCEDURE Delete_customer(
               p_customer_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_customer';
BEGIN
    -- ������� ������ ������������ ������, ���������� 
    DELETE CUSTOMER_ADDRESS_T WHERE CUSTOMER_ID = p_customer_id;
    -- ������� ����������, ���� �� ���� ���� ������� ������, ����� ������� ����������
    DELETE CUSTOMER_T WHERE CUSTOMER_ID = p_customer_id;
    IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '� ������� CUSTOMER_T ��� ������ � CUSTOMER_ID='||p_customer_id);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. customer_id='||p_customer_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- ����� ID ���������� �� ������ ����� (����� � ���������)
--   - ������������� - ���-�� �������
--   - ��� ������ ���������� ����������
--
FUNCTION Find_by_name( 
               p_recordset OUT t_refc, 
               p_name       IN VARCHAR2,
               p_short_name IN VARCHAR2
             ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_by_name';
    v_retcode    INTEGER := c_RET_OK;
BEGIN
    SELECT COUNT(*) INTO v_retcode  
      FROM CUSTOMER_T
     WHERE UPPER(CUSTOMER) LIKE UPPER(p_name)||'%'
       AND UPPER(SHORT_NAME) LIKE UPPER(p_short_name)||'%';
            
    OPEN p_recordset FOR
         SELECT CUSTOMER_ID, ERP_CODE, INN, KPP, CUSTOMER, SHORT_NAME 
           FROM CUSTOMER_T
          WHERE UPPER(CUSTOMER) LIKE UPPER(p_name)||'%'
            AND UPPER(SHORT_NAME) LIKE UPPER(p_short_name)||'%'
          ORDER BY CUSTOMER;
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
-- ����� ID ����������� �� ������ ����� (����� � ���������), ���, ���, ERP-����
--   - ������������� - ���-�� �������
--   - ��� ������ ���������� ����������
--
PROCEDURE Find_customer( 
          p_recordset OUT t_refc, 
          p_erpcode    IN VARCHAR2,     
          p_name       IN VARCHAR2,
          p_inn        IN VARCHAR2,
          p_kpp        IN VARCHAR2
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_customer';
    v_retcode    INTEGER := c_RET_OK;
    v_sql        VARCHAR2(2000);
BEGIN
    v_sql := ' SELECT CUSTOMER_ID, PARENT_ID CUSTOMER_PARENT_ID, SHORT_NAME CUSTOMER_NAME_SHORT,
                ERP_CODE CUSTOMER_ERP_CODE, INN CUSTOMER_INN, KPP CUSTOMER_KPP, CUSTOMER CUSTOMER_NAME
           FROM CUSTOMER_T WHERE 1=1';
    
    IF p_name IS NOT NULL THEN
       v_sql := v_sql || ' AND UPPER(CUSTOMER) LIKE ''%' ||UPPER(p_name) ||'%''';
    END IF;
    
    IF p_inn IS NOT NULL THEN
       v_sql := v_sql || ' AND NVL(INN,''###'') = NVL('''|| p_inn||''', ''###'')';
    END IF;
    
    IF p_kpp IS NOT NULL THEN
       v_sql := v_sql || ' AND NVL(KPP,''###'') = NVL('''|| p_kpp||''', ''###'')';
    END IF;

    IF p_erpcode IS NOT NULL THEN
       v_sql := v_sql || ' AND NVL(ERP_CODE,''###'') = NVL('''|| p_erpcode||''', ''###'')';
    END IF;

    v_sql := v_sql || ' ORDER BY CUSTOMER';

    OPEN p_recordset FOR v_sql;    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- ����� ����������� �� ��� ID
--
PROCEDURE Find_customer( 
          p_recordset OUT t_refc, 
          p_id       IN INTEGER
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_customer';
    v_retcode    INTEGER := c_RET_OK;
BEGIN
    OPEN p_recordset FOR
         SELECT CUSTOMER_ID, PARENT_ID CUSTOMER_PARENT_ID, SHORT_NAME CUSTOMER_NAME_SHORT,
                ERP_CODE CUSTOMER_ERP_CODE, INN CUSTOMER_INN, KPP CUSTOMER_KPP, CUSTOMER CUSTOMER_NAME
           FROM CUSTOMER_T
          WHERE CUSTOMER_ID = p_id;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ���������� ���������� ��������� ����������, ���������� ��������
--   - ��� ������ ���������� ����������
--
PROCEDURE Set_bank(
               p_customer_id       IN INTEGER,
               p_bank_name         IN VARCHAR2, -- ������������ �����
               p_bank_code         IN VARCHAR2, -- ���
               p_bank_corr_account IN VARCHAR2, -- ����������������� ����
               p_bank_settlement   IN VARCHAR2  -- ��������� ���� ����������� � �����
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_bank';
    v_count      INTEGER;
BEGIN
    UPDATE CUSTOMER_BANK_T CB
       SET CB.BANK_NAME = p_bank_name, 
           CB.BANK_CODE = p_bank_code,
           CB.BANK_CORR_ACCOUNT = p_bank_corr_account,
           CB.BANK_SETTLEMENT   = p_bank_settlement
     WHERE CB.CUSTOMER_ID = p_customer_id;
    -- 
    v_count := SQL%ROWCOUNT;
    --
    IF v_count = 0 THEN
        INSERT INTO CUSTOMER_BANK_T CB(
            CB.CUSTOMER_ID,
            CB.BANK_NAME, 
            CB.BANK_CODE,
            CB.BANK_CORR_ACCOUNT,
            CB.BANK_SETTLEMENT
        )VALUES(
            p_customer_id,
            p_bank_name, 
            p_bank_code,
            p_bank_corr_account,
            p_bank_settlement
        );
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ���������� ����������� ����� ����������, ���������� ��������
--   - ������������� - id ������ ������ 
--   - ��� ������ ���������� ����������
--
FUNCTION Set_address(
               p_customer_id  IN VARCHAR2,
               p_address_type IN VARCHAR2,
               p_country      IN VARCHAR2, 
               p_zip          IN VARCHAR2,
               p_state        IN VARCHAR2,
               p_city         IN VARCHAR2, 
               p_address      IN VARCHAR2,
               p_date_from    IN DATE,
               p_date_to      IN DATE
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_address';
    v_address_id INTEGER;
    v_date_from  DATE;
BEGIN
    -- ��������� ������� ������, ���� ��� ����
    UPDATE CUSTOMER_ADDRESS_T
       SET DATE_TO = p_date_from - 1/86400
     WHERE CUSTOMER_ID = p_customer_id
       AND ADDRESS_TYPE = p_address_type
       AND ( DATE_TO IS NULL OR DATE_TO < =p_date_from )
     RETURNING ADDRESS_ID, DATE_FROM INTO v_address_id, v_date_from;
    -- ����������� � ����������� ����������   
    IF p_date_from <= v_date_from THEN
       RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
                                  '���� ������ �������� ����� ������ '
                                  ||TO_DATE(p_date_from,'dd.mm.yyyy')
                                  ||' ������, ��� ������� ������ '
                                  ||TO_DATE(v_date_from,'dd.mm.yyyy')
                                  ||' , CUSTOMER_ID='||p_customer_id
                                  ||' , ADDRESS_TYPE='||p_address_type
                               );
    END IF;
    -- ������������� ����� �����:
    INSERT INTO CUSTOMER_ADDRESS_T (
       ADDRESS_ID, ADDRESS_TYPE, COUNTRY, ZIP, STATE, CITY, ADDRESS, 
       DATE_FROM, DATE_TO, CUSTOMER_ID
    )VALUES(
       SQ_ADDRESS_ID.NEXTVAL, p_address_type, p_country, p_zip, p_state, p_city, p_address, 
       p_date_from, p_date_to, p_customer_id
    )RETURNING ADDRESS_ID INTO v_address_id;
    RETURN v_address_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- ����� ����������� ����� ���������� �� customer_id � ����
--   - ��� ������ ���������� ����������
--
PROCEDURE Get_address( 
               p_recordset   OUT t_refc, 
               p_customer_id  IN INTEGER,
               p_date         IN DATE DEFAULT SYSDATE
             )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_address';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR
         SELECT ADDRESS_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, 
                DATE_FROM, DATE_TO, CUSTOMER_ID 
           FROM CUSTOMER_ADDRESS_T
          WHERE CUSTOMER_ID = p_customer_id
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


END PK13_CUSTOMER;
/
