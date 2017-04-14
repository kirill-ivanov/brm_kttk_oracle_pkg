CREATE OR REPLACE PACKAGE PK11_CLIENT
IS
    --
    -- Пакет для работы с объектом "КЛИЕНТ", таблицы:
    -- client_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK11_CLIENT';
    -- ==============================================================================
    c_RET_OK    constant integer := 1;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- создать клиента, возвращает значения
    --   - положительное - ID клиента, 
    --   - при ошибке выставляет исключение
    FUNCTION New_client(p_name IN VARCHAR2) RETURN INTEGER;
    
    -- удалить клиента, возвращает значения
    --   - при ошибке выставляет исключение
    PROCEDURE Delete_client(p_client_id IN INTEGER);
    
    -- найти ID клиента по началу имени (выбор с донабором)
    --   - кол-во записей
    --   - при ошибке выставляет исключение
    FUNCTION Find_client(p_recordset out t_refc, p_name IN VARCHAR2) RETURN INTEGER;
    
END PK11_CLIENT;
/
CREATE OR REPLACE PACKAGE BODY PK11_CLIENT
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- создать клиента, возвращает значения
--   - положительное - ID клиента, 
--   - при ошибке выставляет исключение
--
FUNCTION New_client(p_name IN VARCHAR2) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'New_client';
    v_client_id  INTEGER;
BEGIN
    INSERT INTO CLIENT_T (CLIENT_ID, CLIENT_NAME)
    VALUES(SQ_CLIENT_ID.NEXTVAL, p_name)
    RETURNING CLIENT_ID INTO v_client_id;
    RETURN v_client_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
  
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- удалить клиента, возвращает значения
-- - при ошибке выставляет исключение
--
PROCEDURE Delete_client(p_client_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_client';
    v_retcode    INTEGER := c_RET_OK;
BEGIN
    DELETE FROM CLIENT_T WHERE CLIENT_ID = p_client_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
  
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- найти ID клиента по началу имени (выбор с донабором)
--   - кол-во записей
--   - при ошибке выставляет исключение
--
FUNCTION Find_client(p_recordset out t_refc, p_name IN VARCHAR2) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_client';
    v_retcode    INTEGER := c_RET_OK;
BEGIN
    -- получаем кол-во записей
    SELECT COUNT(*) INTO v_retcode
      FROM CLIENT_T
     WHERE UPPER(CLIENT_NAME) LIKE UPPER(p_name)||'%';
    -- открываем курсор
    OPEN p_recordset FOR
         SELECT CLIENT_ID, CLIENT_NAME 
           FROM CLIENT_T
          WHERE UPPER(CLIENT_NAME) LIKE UPPER(p_name)||'%'
          ORDER BY CLIENT_NAME;
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK11_CLIENT;
/
