CREATE OR REPLACE PACKAGE PK17_RATEPLANE
IS
    --
    -- ����� ��� ������ � �������� "�������� �����", �������:
    -- rateplan_t
    --
    -- ==============================================================================
    c_PkgName   CONSTANT varchar2(30) := 'PK17_RATEPLANE';
    -- ==============================================================================
    c_RET_OK    CONSTANT integer := 0;
    c_RET_ER		CONSTANT integer :=-1;
    
    TYPE t_refc IS REF CURSOR;
    
    -- ���������������� ����� �������� ���� � ��������, ���������� ��������
    -- - ������������� RATEPLAN_ID 
    -- - ��� ������ ���������� ����������
    FUNCTION Add_rateplan(
               p_rateplan_id    IN INTEGER,  -- ID ��������� ����� � ������� ������� �������
               p_tax_incl       IN CHAR,     -- ������ �������� � ��: "Y/N"
               p_rateplan_name  IN VARCHAR2, -- ��� ��������� �����
               p_ratesystem_id  IN INTEGER,  -- ID ��������� �������
               p_service_id     IN INTEGER,  -- ID ������
               p_subservice_id  IN INTEGER DEFAULT NULL,  -- ID ���������� ������
               p_rateplan_code  IN VARCHAR2 DEFAULT NULL, -- ��� - ��� ������ ��������
               p_tariff_abn     IN NUMBER DEFAULT NULL,   -- ������ ���������
               p_tariff_min     IN NUMBER DEFAULT NULL,   -- ������ ����������� ������
               p_currency_id    IN INTEGER DEFAULT NULL,  -- id ������
               p_agent_percent  IN NUMBER DEFAULT NULL    -- ������� ���������� ��������������
           ) RETURN INTEGER;
    
    -- �������� ������ �������� ������ � ��������, �� �����
    -- - ������������� - ���-�� �������,
    -- - ��� ������ ���������� ����������
    --
    FUNCTION List_rateplanes(
                   p_recordset    OUT t_refc, 
                   p_rateplan_name IN VARCHAR2
               ) RETURN INTEGER;
    
    -- ���������� ������������, ������ ��� ������� ������� CCAD (����������)
    FUNCTION Update_rateplan(
                   p_rateplan_id    IN INTEGER,  -- ID ��������� ����� � ������� ������� �������
                   p_tax_incl       IN CHAR,     -- ������ �������� � ��: "Y/N"
                   p_rateplan_name  IN VARCHAR2  -- ��� ��������� �����
               ) RETURN INTEGER;

    -- ���������� ������������, ������ ��� ������� ������� ���� (����������)
    FUNCTION Update_rateplan_MGMN(
               p_rateplan_id    IN INTEGER,  -- ID ��������� ����� � ������� ������� �������
               p_tax_incl       IN CHAR,     -- ������ �������� � ��: "Y/N"
               p_rateplan_name  IN VARCHAR2,  -- ��� ��������� �����
               p_rateplan_code  IN VARCHAR2, -- ��� - ��� ������ ��������
               p_currency_id    IN INTEGER   -- id ������
           ) RETURN INTEGER;
    -- �������� ��� ������� ������� CCAD (����������)
    FUNCTION Delete_rateplan(
                   p_rateplan_id    IN INTEGER  -- ID ��������� ����� � ������� ������� �������
               ) RETURN INTEGER;


END PK17_RATEPLANE;
/
CREATE OR REPLACE PACKAGE BODY PK17_RATEPLANE
IS

-- ���������������� ����� �������� ���� � ��������, ���������� ��������
-- - ������������� - RATEPLAN_ID 
-- - ��� ������ ���������� ����������
FUNCTION Add_rateplan(
               p_rateplan_id    IN INTEGER,  -- ID ��������� ����� � ������� ������� �������
               p_tax_incl       IN CHAR,     -- ������ �������� � ��: "Y/N"
               p_rateplan_name  IN VARCHAR2, -- ��� ��������� �����
               p_ratesystem_id  IN INTEGER,  -- ID ��������� �������
               p_service_id     IN INTEGER,  -- ID ������
               p_subservice_id  IN INTEGER DEFAULT NULL,  -- ID ���������� ������
               p_rateplan_code  IN VARCHAR2 DEFAULT NULL, -- ��� - ��� ������ ��������
               p_tariff_abn     IN NUMBER DEFAULT NULL,   -- ������ ���������
               p_tariff_min     IN NUMBER DEFAULT NULL,   -- ������ ����������� ������
               p_currency_id    IN INTEGER DEFAULT NULL,  -- id ������
               p_agent_percent  IN NUMBER DEFAULT NULL    -- ������� ���������� ��������������
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_rateplan';
    l_Result      INTEGER;
    v_currency_id INTEGER;
BEGIN
    --
    IF p_currency_id IS NOT NULL THEN
      v_currency_id := p_currency_id;
    ELSE
      v_currency_id := Pk00_Const.c_CURRENCY_RUB;
    END IF;
    --
    INSERT INTO RATEPLAN_T (
           RATEPLAN_ID, 
           RATEPLAN_NAME, 
           RATESYSTEM_ID, 
           RATEPLAN_CODE,
           SERVICE_ID, 
           SUBSERVICE_ID, 
           TAX_INCL, 
           CURRENCY_ID,
           AGENT_PERCENT
           )
    VALUES(NVL(p_rateplan_id,SQ_RATEPLAN_ID.NEXTVAL), 
           p_rateplan_name, 
           p_ratesystem_id, 
           p_rateplan_code,
           p_service_id, 
           p_subservice_id, 
           p_tax_incl, 
           v_currency_id,
           p_agent_percent
           )
    RETURNING rateplan_id INTO l_Result;
    -- ������ ������������� ����� 
    -- ������ ���������
    IF p_tariff_abn IS NOT NULL THEN   
        INSERT INTO FIX_RATE_T(
            FIX_RATE_ID,CHARGE_TYPE,VALUE,RATE_PLAN_ID,TAX_INCL,QUANTITY
        )VALUES(
            SQ_RATEPLAN_ID.NEXTVAL, 
            Pk00_Const.c_CHARGE_TYPE_REC,p_tariff_abn, l_Result, p_tax_incl, 1
        );
    END IF;
    -- ������ ����������� ������
    IF p_tariff_min IS NOT NULL THEN   
        INSERT INTO FIX_RATE_T(
            FIX_RATE_ID,CHARGE_TYPE,VALUE,RATE_PLAN_ID,TAX_INCL
        )VALUES(
            SQ_RATEPLAN_ID.NEXTVAL, 
            Pk00_Const.c_CHARGE_TYPE_MIN,p_tariff_abn, l_Result, p_tax_incl
        );
    END IF;  
    -- 
    RETURN l_Result;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
  
-- �������� ������ �������� ������ � ��������, �� �����
--   - ������������� - ���-�� �������,
--   - ��� ������ ���������� ����������
--
FUNCTION List_rateplanes(
               p_recordset    OUT t_refc, 
               p_rateplan_name IN VARCHAR2
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'List_rateplanes';
    v_retcode    INTEGER := c_RET_OK;
BEGIN
    SELECT COUNT(*) INTO v_retcode
      FROM RATEPLAN_T
     WHERE UPPER(RATEPLAN_NAME) LIKE UPPER(p_rateplan_name)||'%';

    OPEN p_recordset FOR
         SELECT RATEPLAN_ID, RATEPLAN_NAME, RATESYSTEM_ID, RATEPLAN_CODE, RATESYSTEM_ID RATEPLAN_SYSTEM_ID
           FROM RATEPLAN_T
          WHERE UPPER(RATEPLAN_NAME) LIKE UPPER(p_rateplan_name)||'%'
          ORDER BY RATEPLAN_NAME;
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


  -- ���������� ������������, ������ ��� ������� ������� CCAD (����������)
FUNCTION Update_rateplan(
               p_rateplan_id    IN INTEGER,  -- ID ��������� ����� � ������� ������� �������
               p_tax_incl       IN CHAR,     -- ������ �������� � ��: "Y/N"
               p_rateplan_name  IN VARCHAR2  -- ��� ��������� �����
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Update_rateplan';
BEGIN

    update RATEPLAN_T
        set 
            RATEPLAN_NAME   = p_rateplan_name,
            TAX_INCL        = p_tax_incl
    where  
            RATEPLAN_ID     = p_rateplan_id; 
           
    RETURN p_rateplan_id;
    
--EXCEPTION
--    WHEN OTHERS THEN
--        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
  

  -- ���������� ������������, ������ ��� ������� ������� ���� (����������)
FUNCTION Update_rateplan_MGMN(
               p_rateplan_id    IN INTEGER,  -- ID ��������� ����� � ������� ������� �������
               p_tax_incl       IN CHAR,     -- ������ �������� � ��: "Y/N"
               p_rateplan_name  IN VARCHAR2,  -- ��� ��������� �����
               p_rateplan_code  IN VARCHAR2, -- ��� - ��� ������ ��������
               p_currency_id    IN INTEGER   -- id ������
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Update_rateplan_MGMN';
BEGIN

    update RATEPLAN_T
        set 
            RATEPLAN_NAME   = p_rateplan_name,
            TAX_INCL        = p_tax_incl,
            RATEPLAN_CODE   = p_rateplan_code,
            CURRENCY_ID     = p_currency_id  
    where  
            RATEPLAN_ID     = p_rateplan_id; 
           
    RETURN p_rateplan_id;
    
--EXCEPTION
--    WHEN OTHERS THEN
--        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


  -- �������� ��� ������� ������� CCAD (����������)
FUNCTION Delete_rateplan(
               p_rateplan_id    IN INTEGER  -- ID ��������� ����� � ������� ������� �������
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_rateplan';
    v_rp RATEPLAN_T%rowtype;
BEGIN

    begin
        select * into v_rp from RATEPLAN_T where  RATEPLAN_ID = p_rateplan_id; 
    exception when no_data_found then
        return p_rateplan_id;
    end;  

    delete RATEPLAN_T where  RATEPLAN_ID = p_rateplan_id;
    
    RETURN p_rateplan_id;
    
--EXCEPTION
--    WHEN OTHERS THEN
--        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
  


END PK17_RATEPLANE;
/
