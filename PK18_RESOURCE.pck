CREATE OR REPLACE PACKAGE PK18_RESOURCE
IS
    --
    -- ����� ��� ������ � �������� "����������� �������", �������:
    -- resource_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK07_BILL';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ID ������ �� ������� ������ ����� �� ��������� ����
    --   - ������������� ID ������ 
    --   - NULL - �� ������
    --   - ��� ������ ���������� ����������
    FUNCTION Get_phone (
                   p_phone  IN VARCHAR2,   -- ����� ��������
                   p_date   IN DATE DEFAULT SYSDATE
               ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� � ������, ����������� ������ - ����� ��������
    --   - ��� ������ ���������� ���������� 
    --
    FUNCTION Add_phone(
               p_order_id     IN INTEGER,    -- ID ������
               p_phone        IN VARCHAR2,   -- ����� ��������
               p_date_from    IN DATE,       -- ���� ������ ��������
               p_date_to      IN DATE DEFAULT PK00_CONST.c_DATE_MAX
           ) RETURN VARCHAR2;
    
    PROCEDURE Add_phone(
               p_order_id     IN INTEGER,    -- ID ������
               p_phone        IN VARCHAR2,   -- ����� ��������
               p_date_from    IN DATE,       -- ���� ������ ��������
               p_date_to      IN DATE DEFAULT PK00_CONST.c_DATE_MAX
           );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� �� ������, ����������� ������ - ����� ��������
    --   - ��� ������ ���������� ����������
    --
    PROCEDURE Close_phone (
                   p_order_id     IN INTEGER,    -- ID ������
                   p_phone        IN INTEGER,    -- ����� ��������
                   p_date_to      IN DATE        -- ���� ��������� ��������
               );
    
    -- �������� ������ ��������� �� ������
    --   - ������������� - ���-�� �������
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Phone_list( 
                   p_recordset   OUT t_refc,
                   p_order_id   IN INTEGER
               ) RETURN INTEGER;
               
    -- �������� �������� ������ ���������� ������� ��������� �� ���� p_date ��� ������ p_order_id
    --   - ��� ������ ���������� ����������
    --
    PROCEDURE Phone_ranges( 
                   p_recordset   OUT t_refc,
                   p_order_id     IN INTEGER,
                   p_date         IN DATE DEFAULT SYSDATE
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- �������� ����� ��������� ��������
    --   - ������������� - ID ������ 
    --   - ��� ������ ���������� ����������
    FUNCTION Add_phone_address(
                   p_phone         IN VARCHAR2,  -- ����� ��������
                   p_country       IN VARCHAR2,  -- '��' - ������, 99.9999999999% ������� ��
                   p_zip           IN VARCHAR2,  -- �������� ������
                   p_state         IN VARCHAR2,  -- ������ (������� )
                   p_city          IN VARCHAR2,  -- �����
                   p_address       IN VARCHAR2,  -- ����� � ���� ������
                   p_date          IN DATE
               ) RETURN INTEGER;                 -- ID ������ ������
    
    
END PK18_RESOURCE;
/
CREATE OR REPLACE PACKAGE BODY PK18_RESOURCE
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ID ������ �� ������� ������ ����� �� ��������� ����
--   - ������������� ID ������ 
--   - NILL - �� ������
--   - ��� ������ ���������� ����������
FUNCTION Get_phone (
               p_phone  IN VARCHAR2,   -- ����� ��������
               p_date   IN DATE DEFAULT SYSDATE
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_phone';
    v_order_id   INTEGER;
BEGIN
    SELECT ORDER_ID INTO v_order_id
      FROM ORDER_PHONES_T
     WHERE PHONE_NUMBER = p_phone
       AND (DATE_TO IS NULL OR (p_date BETWEEN DATE_FROM AND DATE_TO));
    RETURN v_order_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� � ������, ����������� ������ - ����� ��������
--   - ��� ������ ���������� ����������
--
FUNCTION Add_phone(
               p_order_id     IN INTEGER,    -- ID ������
               p_phone        IN VARCHAR2,   -- ����� ��������
               p_date_from    IN DATE,       -- ���� ������ ��������
               p_date_to      IN DATE DEFAULT PK00_CONST.c_DATE_MAX
           ) return varchar2
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Add_phone';
    v_count     INTEGER;
		v_rowid			VARCHAR2(100);
BEGIN
    -- ��������� �� ������ �� ������� �� ����� ������ ������, ������ ���� �� ������
    SELECT COUNT(*) INTO v_count
      FROM ORDER_PHONES_T t
     WHERE PHONE_NUMBER = p_phone
       AND t.date_from<=p_date_to and t.date_to >= p_date_from;
    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '����� '||p_phone||' - ��� ������������');
    END IF;
    -- ��������� ������
    INSERT INTO ORDER_PHONES_T (
        ORDER_ID, PHONE_NUMBER, DATE_FROM, DATE_TO
    )VALUES(
        p_order_id, p_phone, p_date_from, p_date_to
    ) RETURNING ROWIDTOCHAR(ROWID) INTO v_rowid;
		RETURN v_rowid;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,order_id='||p_order_id||
                                    ',phone='||p_phone, c_PkgName||'.'||v_prcName );
END;

PROCEDURE Add_phone(
           p_order_id     IN INTEGER,    -- ID ������
           p_phone        IN VARCHAR2,   -- ����� ��������
           p_date_from    IN DATE,       -- ���� ������ ��������
           p_date_to      IN DATE DEFAULT PK00_CONST.c_DATE_MAX
       )
IS
		v_rowid			VARCHAR2(100);
BEGIN
    v_rowid := Add_phone( p_order_id, p_phone, p_date_from, p_date_to );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������� �� ������, ����������� ������ - ����� ��������
--   - ��� ������ ���������� ����������
--
PROCEDURE Close_phone (
               p_order_id     IN INTEGER,    -- ID ������
               p_phone        IN INTEGER,    -- ����� ��������
               p_date_to      IN DATE        -- ���� ��������� ��������
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Close_phone';
BEGIN
    UPDATE ORDER_PHONES_T 
       SET DATE_TO     = p_date_to
     WHERE ORDER_ID    = p_order_id 
       AND PHONE_NUMBER= p_phone
       AND DATE_TO IS NULL; 
    IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '����� '||p_phone||' - �� ������ �� ������ ORDER_ID='||p_order_id);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
           
-- �������� ������ ��������� �� ������
--   - ������������� - ���-�� �������
--   - ��� ������ ���������� ����������
--
FUNCTION Phone_list( 
               p_recordset   OUT t_refc,
               p_order_id     IN INTEGER
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Phone_list';
    v_retcode    INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_retcode
      FROM ORDER_PHONES_T
     WHERE ORDER_ID = p_order_id;

    OPEN p_recordset FOR
         SELECT ORDER_ID, PHONE_NUMBER, DATE_FROM, DATE_TO
           FROM ORDER_PHONES_T
          WHERE ORDER_ID = p_order_id
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

-- �������� �������� ������ ���������� ������� ��������� �� ���� p_date ��� ������ p_order_id
--   - c_RET_OK - OK,
--   - ������������� - id ��������� �� ����� � L01
--
PROCEDURE Phone_ranges( 
               p_recordset   OUT t_refc,
               p_order_id     IN INTEGER,
               p_date         IN DATE DEFAULT SYSDATE
           )
IS
    v_prcName    CONSTANT varchar2(30) := 'Phone_ranges';
    v_retcode    INTEGER := c_RET_OK;
BEGIN
    
    OPEN p_recordset FOR
        SELECT PHONE_FROM, DECODE(PHONE_TO, PHONE_FROM, NULL, PHONE_TO) PHONE_TO
            FROM (
            SELECT MIN(PHONE_NUMBER) PHONE_FROM, MAX(PHONE_NUMBER) PHONE_TO 
              FROM ORDER_PHONES_T
             WHERE p_date BETWEEN DATE_FROM AND DATE_TO
               AND ORDER_ID = p_order_id 
            GROUP BY (PHONE_NUMBER - ROWNUM + 1)
        ) ORDER BY PHONE_TO;
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- �������� ����� ��������� ��������
--   - ������������� - ID ������ 
--   - ��� ������ ���������� ����������
FUNCTION Add_phone_address(
               p_phone         IN VARCHAR2,  -- ����� ��������
               p_country       IN VARCHAR2,  -- '��' - ������, 99.9999999999% ������� ��
               p_zip           IN VARCHAR2,  -- �������� ������
               p_state         IN VARCHAR2,  -- ������ (������� )
               p_city          IN VARCHAR2,  -- �����
               p_address       IN VARCHAR2,  -- ����� � ���� ������
               p_date          IN DATE
           ) RETURN INTEGER                  -- ID ������ ������
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_phone_address';
    v_address_id  INTEGER;
BEGIN
    -- ������� ������ ������ ���������
    v_address_id := PK02_POID.Next_address_id;
    INSERT INTO PHONE_ADDRESS_T(
        ADDRESS_ID, ADDRESS_TYPE, COUNTRY, ZIP, STATE, CITY,ADDRESS
    )VALUES(
        v_address_id, PK00_CONST.c_ADDR_TYPE_SET, p_country, 
        p_zip, p_state, p_city, p_address
    );
    -- �������� ����� � ������
    UPDATE ORDER_PHONES_T
       SET ADDRESS_ID = v_address_id
     WHERE PHONE_NUMBER = p_phone
       AND DATE_FROM <= p_date
       AND (DATE_TO IS NULL OR p_date <= DATE_TO);

    RETURN v_address_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;



END PK18_RESOURCE;
/
