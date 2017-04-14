CREATE OR REPLACE PACKAGE PK12_CONTRACT
IS
    --
    -- ����� ��� ������ � �������� "�������", �������:
    -- contract_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK12_CONTRACT';
    -- ==============================================================================
    c_RET_OK    constant integer := 1;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- ������� ����� ����� ��������
    FUNCTION New_contract_no RETURN VARCHAR2;
    
    -- ������� �������, ���������� ��������
    --   - ID �������, 
    --   - ��� ������ ���������� ����������
    FUNCTION Open_contract(
                   p_contract_no IN VARCHAR2, 
                   p_date_from   IN DATE,
                   p_date_to     IN DATE,
                   p_client_id   IN INTEGER,
                   p_manager_id  IN INTEGER
               ) RETURN INTEGER;
 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� �������, ���������� ��������
    --   - ID �������, 
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Open_contract(
               p_contract_no       IN VARCHAR2, 
               p_date_from         IN DATE,
               p_date_to           IN DATE,
               p_client_id         IN INTEGER,
               p_manager_id        IN INTEGER,
               p_market_segment_id IN INTEGER,
               p_client_type_id    IN INTEGER,
               p_notes             IN VARCHAR2 DEFAULT NULL
           ) RETURN INTEGER;   
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ��������� ��������, ���������� ��������
    --   - ��� ������ ���������� ���������� 
    PROCEDURE Edit_contract(
                   p_contract_id IN INTEGER,
                   p_contract_no IN VARCHAR2, 
                   p_date_from   IN DATE,
                   p_date_to     IN DATE,
                   p_client_id   IN INTEGER
               );
    
    -- ������� �������, ���������� ��������
    --   - ��� ������ ���������� ����������
    PROCEDURE Delete_contract(p_contract_id IN INTEGER);

-- ����� ID �������� ���� �� ID, ���� �� ������ ������ (����� � ���������)
--   - ������������� - ���-�� �������
--   - ��� ������ ���������� ����������
--
PROCEDURE Find_contract(
        p_recordset      OUT t_refc, 
        p_contract_id    IN  INTEGER,
        p_contract_no    IN  VARCHAR2
);

    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� ��������� �������������� �������, ���������� ��������
    -- - ��� ������ ���������� ����������
    PROCEDURE Set_manager (
                   p_contract_id IN INTEGER,
                   p_manager_id  IN INTEGER,
                   p_date_from   IN DATE DEFAULT SYSDATE
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ID ��������� �������������� �������, ���������� ��������
    -- - ������������� - ID ���������
    -- - NULL - ��� ������
    -- - ��� ������ ���������� ����������
    FUNCTION Get_manager_id (
                   p_contract_id IN INTEGER,
                   p_date        IN DATE DEFAULT SYSDATE
               ) RETURN INTEGER;
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� ��������� �������������� �������, ���������� ��������
    -- - ��� ������ ���������� ����������
    PROCEDURE Set_billing_curator (
                   p_contract_id IN INTEGER,
                   p_manager_id  IN INTEGER
                );
    
END PK12_CONTRACT;
/
CREATE OR REPLACE PACKAGE BODY PK12_CONTRACT
IS

-- ������� ����� ����� ��������
FUNCTION New_contract_no RETURN VARCHAR2 IS
    v_contract_no CONTRACT_T.CONTRACT_NO%TYPE;
BEGIN
    SELECT LPAD(SQ_CONTRACT_NO.NEXTVAL, 9,'0') INTO v_contract_no FROM DUAL;
    RETURN v_contract_no;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ����� ������ ������� ��������������. ��� ����� ����� �������
-- ������� �������, ���������� ��������
--   - ID �������, 
--   - ��� ������ ���������� ����������
--
FUNCTION Open_contract(
               p_contract_no IN VARCHAR2, 
               p_date_from   IN DATE,
               p_date_to     IN DATE,
               p_client_id   IN INTEGER,
               p_manager_id  IN INTEGER
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Open_contract';
    v_contract_id INTEGER;
BEGIN
    INSERT INTO CONTRACT_T (
      CONTRACT_ID, CONTRACT_NO, DATE_FROM, DATE_TO, CLIENT_ID
    )VALUES(
      SQ_CLIENT_ID.NEXTVAL, p_contract_no, p_date_from, p_date_to, p_client_id
    )
    RETURNING CONTRACT_ID INTO v_contract_id;
    -- ����������� ��������� � ��������
    IF p_manager_id IS NOT NULL THEN
        INSERT INTO SALE_CURATOR_T (MANAGER_ID, CONTRACT_ID, DATE_FROM, DATE_TO)
        VALUES(p_manager_id, v_contract_id, p_date_from, p_date_to);
    END IF;
    --
    RETURN v_contract_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������� �������, ���������� ��������
--   - ID �������, 
--   - ��� ������ ���������� ����������
--
FUNCTION Open_contract(
               p_contract_no       IN VARCHAR2, 
               p_date_from         IN DATE,
               p_date_to           IN DATE,
               p_client_id         IN INTEGER,
               p_manager_id        IN INTEGER,
               p_market_segment_id IN INTEGER,
               p_client_type_id    IN INTEGER,
               p_notes             IN VARCHAR2 DEFAULT NULL
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Open_contract';
    v_contract_id INTEGER;
BEGIN
    INSERT INTO CONTRACT_T (
      CONTRACT_ID, CONTRACT_NO, DATE_FROM, DATE_TO, CLIENT_ID, 
      MARKET_SEGMENT_ID, CLIENT_TYPE_ID, NOTES
    )VALUES(
      SQ_CLIENT_ID.NEXTVAL, p_contract_no, p_date_from, p_date_to, p_client_id, 
      p_market_segment_id, p_client_type_id, p_notes
    )
    RETURNING CONTRACT_ID INTO v_contract_id;
    -- ����������� ��������� � ��������
    IF p_manager_id IS NOT NULL THEN
        INSERT INTO SALE_CURATOR_T (MANAGER_ID, CONTRACT_ID, DATE_FROM, DATE_TO)
        VALUES(p_manager_id, v_contract_id, p_date_from, p_date_to);
    END IF;
    --
    RETURN v_contract_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
  
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ��������� ��������, ���������� ��������
--   - ��� ������ ���������� ����������
PROCEDURE Edit_contract(
               p_contract_id IN INTEGER,
               p_contract_no IN VARCHAR2, 
               p_date_from   IN DATE,
               p_date_to     IN DATE,
               p_client_id   IN INTEGER
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Edit_contract';
    v_count       PLS_INTEGER;
BEGIN
    UPDATE CONTRACT_T 
       SET CONTRACT_NO = NVL(p_contract_no, CONTRACT_NO), 
             DATE_FROM = NVL(p_date_from, DATE_FROM), 
             DATE_TO   = NVL(p_date_to, DATE_TO), 
             CLIENT_ID = NVL(p_client_id, CLIENT_ID)
     WHERE CONTRACT_ID = p_contract_id;  
    v_count := SQL%ROWCOUNT;
    IF v_count = 0 THEN
       RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '� ������� CONTRACT_T ��� ������ � CONTRACT_ID='||p_contract_id);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- ������� �������, ���������� ��������
--   - ��� ������ ���������� ����������
PROCEDURE Delete_contract(p_contract_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_contract';
BEGIN
    DELETE FROM CONTRACT_T WHERE CONTRACT_ID = p_contract_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
  
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- ����� ID �������� ���� �� ID, ���� �� ������ ������ (����� � ���������)
--   - ������������� - ���-�� �������
--   - ��� ������ ���������� ����������
--
PROCEDURE Find_contract(
        p_recordset      OUT t_refc, 
        p_contract_id    IN  INTEGER,
        p_contract_no    IN  VARCHAR2
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_contract';
    v_retcode    INTEGER := c_RET_OK;
BEGIN
    IF p_contract_id IS NOT NULL THEN
        OPEN p_recordset FOR
             SELECT contr.CONTRACT_ID, contr.CONTRACT_NO, contr.DATE_FROM, contr.DATE_TO, 
                    contr.CLIENT_ID,cl.CLIENT_NAME
               FROM CONTRACT_T contr, CLIENT_T cl
              WHERE contr.CLIENT_ID = cl.CLIENT_ID
                    AND contr.CONTRACT_ID = p_contract_id
              ORDER BY contr.CONTRACT_NO;
    ELSE
        OPEN p_recordset FOR
             SELECT contr.CONTRACT_ID, contr.CONTRACT_NO, contr.DATE_FROM, contr.DATE_TO, 
                    contr.CLIENT_ID,cl.CLIENT_NAME
               FROM CONTRACT_T contr, CLIENT_T cl
              WHERE contr.CLIENT_ID = cl.CLIENT_ID
                    AND UPPER(CONTRACT_NO) LIKE UPPER(p_contract_no)
              ORDER BY CONTRACT_NO;      
    END IF;             
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ��������� ��������� �������������� �������, ���������� ��������
-- - ��� ������ ���������� ����������
PROCEDURE Set_manager (
               p_contract_id IN INTEGER,
               p_manager_id  IN INTEGER,
               p_date_from   IN DATE DEFAULT SYSDATE
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_manager';
    v_date_from  DATE := TRUNC(p_date_from);
BEGIN
    -- ��������� ���������� ������
    UPDATE SALE_CURATOR_T
       SET DATE_TO = v_date_from - 1/86400
     WHERE MANAGER_ID != p_manager_id
       AND CONTRACT_ID = p_contract_id
       AND DATE_TO IS NULL;
    -- ��������� ����� ������
    INSERT INTO SALE_CURATOR_T (MANAGER_ID, CONTRACT_ID, DATE_FROM)
    VALUES(p_manager_id, p_contract_id, v_date_from);
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ID ��������� �������������� �������, ���������� ��������
-- - ������������� - ID ���������
-- - NULL - ��� ������
-- - ��� ������ ���������� ����������
FUNCTION Get_manager_id (
               p_contract_id IN INTEGER,
               p_date        IN DATE DEFAULT SYSDATE
           ) RETURN INTEGER 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_manager_id';
    v_manager_id INTEGER;
BEGIN
    SELECT MANAGER_ID INTO v_manager_id
      FROM SALE_CURATOR_T
     WHERE CONTRACT_ID = p_contract_id
       AND p_date BETWEEN DATE_FROM AND DATE_TO;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ��������� ��������� �������������� �������, ���������� ��������
-- - ��� ������ ���������� ����������
PROCEDURE Set_billing_curator (
               p_contract_id IN INTEGER,
               p_manager_id  IN INTEGER
            )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_billing_curator';
BEGIN
    -- ��������� ����� ������
    MERGE INTO BILLING_CURATOR_T BC
    USING (
      SELECT p_contract_id CONTRACT_ID, p_manager_id MANAGER_ID FROM DUAL
    ) VR
    ON (
      BC.CONTRACT_ID = VR.CONTRACT_ID
    )
    WHEN MATCHED THEN UPDATE SET BC.MANAGER_ID = VR.MANAGER_ID
    WHEN NOT MATCHED THEN INSERT ( BC.CONTRACT_ID, BC.MANAGER_ID ) 
                          VALUES ( VR.CONTRACT_ID, VR.MANAGER_ID );
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

END PK12_CONTRACT;
/
