CREATE OR REPLACE PACKAGE PK66_DRU
IS
    --
    -- Пакет для ручных работ сотрудников ДРУ
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK66_DRU';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    --
    PROCEDURE AG10_ORDERS_DESCR;
    --
    --
    -- Просмотр результатов по системе логирования
    --
    PROCEDURE View_log ( p_recordset OUT t_refc );
    
  
END PK66_DRU;
/
CREATE OR REPLACE PACKAGE BODY PK66_DRU
IS

-- ========================================================================= --
--                               Д Р У
-- ========================================================================= --
--
PROCEDURE AG10_ORDERS_DESCR
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'AG10_ORDERS_DESCR';
    v_count     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    DELETE PIN.AG10_ORDERS_DESCR WHERE DESCR IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('AG10_ORDERS_DESCR: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    MERGE INTO ORDER_PHONES_T Q
    USING (
        SELECT O.ORDER_ID, A.ANUMBER, A.DESCR 
          FROM ORDER_T O, AG10_ORDERS_DESCR A 
         WHERE O.ORDER_NO = A.ORDER_NO
    ) W
    ON (Q.ORDER_ID = W.ORDER_ID AND Q.PHONE_NUMBER = W.ANUMBER)
    WHEN MATCHED THEN UPDATE SET Q.DESCR = W.DESCR;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_PHONES_T: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE AG10_ORDERS_DESCR DROP STORAGE';

    -- Подтверждаем изменения
    COMMIT;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

--
-- Просмотр результатов по системе логирования
--
PROCEDURE View_log ( p_recordset OUT t_refc )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'View_log';
    v_retcode    INTEGER;
BEGIN
    -- построить курсор
    OPEN p_recordset FOR
        SELECT MSG_LEVEL, MSG_DATE, MSG_SRC, MESSAGE, OS_USER, HOST_NAME, PROGRAM, L01_ID
          FROM L01_MESSAGES 
           WHERE MSG_SRC LIKE c_PkgName||'%'
        ORDER BY L01_ID DESC;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


END PK66_DRU;
/
