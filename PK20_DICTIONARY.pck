CREATE OR REPLACE PACKAGE PK20_DICTIONARY
IS
    --
    -- ����� ��� ������ � �������� "�������", �������:
    -- dictionary_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK20_DICTIONARY';
    -- ==============================================================================
    type t_refc is ref cursor;
    
    -- ������� ����, ���������� ��������
    --   - ������������� - ID �����, 
    --   - ��� ������ ���������� ����������
    FUNCTION New_key(p_key IN VARCHAR2, p_name IN VARCHAR2) RETURN INTEGER;
    
    -- �������� �������� � �����, ���������� ��������
    --   - ������������� - ID ������,
    --   - ��� ������ ���������� ����������
    FUNCTION Add_value(p_key IN VARCHAR2, p_value IN VARCHAR2, p_notes IN VARCHAR2) RETURN INTEGER;
    
    -- �������� ������ ������ � �������
    --   - ���-�� �������
    --   - ��� ������ ���������� ����������
    FUNCTION Key_list(p_recordset out t_refc) RETURN INTEGER;
    
    -- ����� ������ ������� �� ����� � ������� (NULL - ��� ������)
    PROCEDURE List_by_key(p_recordset out t_refc, p_key IN VARCHAR2 DEFAULT NULL);
    
END PK20_DICTIONARY;
/
CREATE OR REPLACE PACKAGE BODY PK20_DICTIONARY
IS

-- ������� ����, ���������� ��������
--   - ������������� - ID �����, 
--   - ������������� - id ��������� �� ����� � L01
FUNCTION New_key(p_key IN VARCHAR2, p_name IN VARCHAR2) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'New_key';
    v_key_id     INTEGER;
BEGIN
    SELECT MAX(PARENT_ID) + 1
      INTO v_key_id
      FROM DICTIONARY_T;
    --
    INSERT INTO DICTIONARY_T(KEY_ID, PARENT_ID, KEY, NAME)
    VALUES (v_key_id, NULL, p_key, p_name);
    --
    RETURN v_key_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- �������� �������� � �����, ���������� ��������
--   - ������������� - ID ������,
--   - ������������� - id ��������� �� ����� � L01
FUNCTION Add_value(
	p_key IN VARCHAR2, 
	p_value IN VARCHAR2,
	p_notes IN VARCHAR2) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Add_value';
    v_prn_id     INTEGER;
    v_key_id     INTEGER;
BEGIN
    -- ������� ������ ����������, ����� ���������, ��� ���� ����������
    SELECT KEY_ID INTO v_prn_id
      FROM DICTIONARY_T 
     WHERE KEY = p_key
       AND PARENT_ID IS NULL;
    -- ������� ���� ������
    SELECT MAX(KEY_ID) + 1
      INTO v_key_id
      FROM DICTIONARY_T
     WHERE PARENT_ID = v_prn_id;
    IF v_key_id IS NULL THEN
        v_key_id := v_prn_id * 100 + 1;
    END IF;
    -- ��������� ������
    INSERT INTO DICTIONARY_T (KEY_ID, PARENT_ID, NAME, NOTES)
    VALUES (v_key_id, v_prn_id, p_value, p_notes);
    --
    RETURN v_key_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- �������� ������ ������ � �������
--   - ���-�� �������
--   - ������������� - id ��������� �� ����� � L01
FUNCTION Key_list(p_recordset out t_refc) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Key_list';
    v_retcode    INTEGER := 0;
BEGIN
    -- �������� ���-�� �������
    SELECT COUNT(*) INTO v_retcode
      FROM DICTIONARY_T
     WHERE PARENT_ID IS NULL;
    -- ��������� ������
    OPEN p_recordset FOR
        SELECT KEY_ID, KEY, NAME 
          FROM DICTIONARY_T d
         WHERE PARENT_ID IS NULL
         ORDER BY KEY_ID;
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

-- ����� ������ ������� �� ����� � ������� (NULL - ��� ������)
PROCEDURE List_by_key(p_recordset out t_refc, p_key IN VARCHAR2 DEFAULT NULL)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'List_by_key';
    v_retcode    INTEGER;
BEGIN
    -- ��������� ������
    IF p_key IS NULL THEN
        OPEN p_recordset FOR
            SELECT LEVEL, PARENT_ID, KEY_ID, KEY, NAME FROM DICTIONARY_T d
            CONNECT BY PRIOR KEY_ID = PARENT_ID
              START WITH PARENT_ID IS NULL;
    ELSE
        OPEN p_recordset FOR
            SELECT LEVEL, PARENT_ID, KEY_ID, KEY, NAME FROM DICTIONARY_T d
            CONNECT BY PRIOR KEY_ID = PARENT_ID
              START WITH PARENT_ID = p_key;
    END IF;
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK20_DICTIONARY;
/
