CREATE OR REPLACE PACKAGE PK02_BRM_CALL IS
  
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK02_BRM_CALL';
    -- ==============================================================================
    -- »нтерфейс дл€ выгрузки данных в INFRANET
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- IP: 10.110.32.160
    -- protocol: FTP
    -- user: billsrv
    -- passwd: portal6.5
    -- /home/billsrv/data   -- (BS_DATA_DIR)
    -- /home/billsrv/bcr    -- (BCR_DIR) данные дл€ BCR
    -- /home/billsrv/agent  -- (AGENT_DIR) экспорт агентских BDR
    -- ==============================================================================

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Ёкспорт данных в систему подготовки данных по просто€м и SLA
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    TYPE t_disc IS RECORD (
              ORDER_NO       VARCHAR2(100), 
              ACCOUNT_NO     VARCHAR2(40), 
              COMPANY        VARCHAR2(1024), 
              SPEED          VARCHAR2(20), 
              S_POINT        VARCHAR2(1000), 
              D_POINT        VARCHAR2(1000),
              ORDER_DATE     DATE,
              NAME           VARCHAR2(400),
              CYCLE_START_T  DATE, 
              CYCLE_END_T    DATE,
              PURCHASE_START_T DATE,
              PURCHASE_END_T DATE,
              USAGE_START_T  DATE,
              USAGE_END_T    DATE,
              FREE_DOWNTIME  NUMBER,
              EARNED_TYPE    NUMBER
         );
    TYPE rc_disc IS REF CURSOR RETURN t_disc;
    
    -- временно оставил до перехода на обмен через файлы
    PROCEDURE Export_for_discount( 
                   p_recordset  OUT rc_disc
               );
  
    -- –езультат: /home/billsrv/data/for_idl.csv
    PROCEDURE Export_for_idl_to_file;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Ёкспорт информации о заказах
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    TYPE t_order IS RECORD (
             ACCOUNT_NO  VARCHAR2(40), 
             ORDER_NO    VARCHAR2(100),  
             DATE_FROM   DATE, 
             DATE_TO     DATE, 
             CREATE_DATE DATE, 
             SERVICE     VARCHAR2(400), 
             SERVICE_ID  INTEGER, 
             STATUS      VARCHAR2(10)
         );
    TYPE rc_order IS REF CURSOR RETURN t_order;
    
    -- временно оставил до перехода на обмен через файлы
    PROCEDURE Orders( 
                   p_recordset  OUT rc_order --SYS_REFCURSOR -- rc_order
               );

    -- –езультат: /home/billsrv/data/orders.csv
    PROCEDURE Orders_to_file;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- —писок заказов с двойной тарификацией
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    TYPE t_ag_order IS RECORD (
             CONTRACT_NO VARCHAR2(100),
             ACCOUNT_NO  VARCHAR2(100),
             ACCOUNT_ID  INTEGER, 
             ORDER_NO    VARCHAR2(100),  
             ORDER_ID    INTEGER,
             DATE_FROM   DATE, 
             DATE_TO     DATE
         );
    TYPE rc_ag_order IS REF CURSOR RETURN t_ag_order;
    
    -- временно оставил до перехода на обмен через файлы
    PROCEDURE List_ag_orders( 
                   p_recordset IN OUT rc_ag_order, --SYS_REFCURSOR, --rc_ag_order
                   p_date      IN DATE
               );

    -- –езультат: /home/billsrv/agent/ag_orders.csv
    PROCEDURE List_ag_orders_to_file( 
                   p_date      IN DATE
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- BDR заказов с двойной тарификацией (втора€ часть)
    -- если p_order_no is null - выгружаетс€ весь мес€ц
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    TYPE t_ag_bdr IS RECORD (
             START_TIME   DATE,
             LOCAL_TIME   DATE,
             ABN_A        VARCHAR2(40),
             ABN_B        VARCHAR2(40),
             DURATION_SEC NUMBER,
             BILL_MINUTES NUMBER,
             AMOUNT       NUMBER,
             PRICE        NUMBER,
             ACCOUNT_ID   INTEGER,
             ORDER_ID     INTEGER,
             PREFIX_A     VARCHAR2(34),
             INIT_Z_NAME  VARCHAR2(255),
             PREFIX_B     VARCHAR2(34),
             TERM_Z_NAME  VARCHAR2(255)
         );
    TYPE rc_ag_bdr IS REF CURSOR RETURN t_ag_bdr;
    
    -- временно оставил до перехода на обмен через файлы
    PROCEDURE Export_ag_bdr( 
                   p_recordset IN OUT rc_ag_bdr,        --SYS_REFCURSOR, --rc_ag_bdr
                   p_period_id IN INTEGER,              -- формат YYYYMM (201505 - май 2015)
                   p_order_id  IN INTEGER DEFAULT NULL  -- NULL - дл€ всех закакзов сразу
               );

    -- –езультат: /home/billsrv/agent/ag_bdr.csv
    PROCEDURE Export_ag_bdr_to_file( 
                   p_period_id IN INTEGER,              -- формат YYYYMM (201505 - май 2015)
                   p_order_no  IN VARCHAR2 DEFAULT NULL -- NULL - дл€ всех закакзов сразу
               );

    -- экспорт сгруппированных агентских BDR в файл
    -- –езультат: /home/billsrv/agent/ag_bdr_group.csv
    PROCEDURE Export_group_ag_bdr_to_file( 
                   p_period_id IN INTEGER               -- формат YYYYMM (201505 - май 2015)
               );



END PK02_BRM_CALL;
/
CREATE OR REPLACE PACKAGE BODY PK02_BRM_CALL IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Ёкспорт данных в систему подготовки данных по просто€м и SLA
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Export_for_discount( 
               p_recordset OUT rc_disc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_for_discount';
    v_retcode    INTEGER;
BEGIN
    PIN.PK406_INFRANET_DATA.EXPORT_FOR_IDL(p_recordset);
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := PIN.Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(PIN.Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


PROCEDURE Export_for_idl_to_file
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_for_idl_to_file';
BEGIN
    PIN.PK406_INFRANET_DATA.EXPORT_FOR_IDL_TO_FILE;
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Ёкспорт информации о заказах
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Orders( 
               p_recordset OUT rc_order
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Orders';
    v_retcode    INTEGER;
BEGIN
    PIN.PK406_INFRANET_DATA.EXPORT_ORDERS(p_recordset);
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := PIN.Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(PIN.Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

PROCEDURE Orders_to_file
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Orders_to_file';
BEGIN
    PIN.PK406_INFRANET_DATA.EXPORT_ORDERS_TO_FILE;
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- —писок заказов двойной тарификацией
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE List_ag_orders( 
               p_recordset IN OUT rc_ag_order,
               p_date      IN DATE
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'List_ag_orders';
    v_retcode    INTEGER;
BEGIN
    PIN.PK406_INFRANET_DATA.LIST_AG_ORDERS(p_recordset, p_date);  
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := PIN.Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(PIN.Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

PROCEDURE List_ag_orders_to_file(p_date IN DATE)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'List_ag_orders_to_file';
BEGIN
    PIN.PK406_INFRANET_DATA.LIST_AG_ORDERS_TO_FILE(p_date);
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- BDR заказов с двойной тарификацией (втора€ часть)
-- если p_order_no is null - выгружаетс€ весь мес€ц
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Export_ag_bdr( 
                   p_recordset IN OUT rc_ag_bdr,
                   p_period_id IN INTEGER,              -- формат YYYYMM (201505 - май 2015)
                   p_order_id  IN INTEGER DEFAULT NULL  -- NULL - дл€ всех закакзов сразу
               )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_ag_bdr';
    v_retcode    INTEGER;
BEGIN
    PIN.PK406_INFRANET_DATA.EXPORT_AG_BDR(p_recordset, p_period_id, p_order_id);
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := PIN.Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(PIN.Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

PROCEDURE Export_ag_bdr_to_file( 
               p_period_id IN INTEGER,               -- формат YYYYMM (201505 - май 2015)
               p_order_no  IN VARCHAR2 DEFAULT NULL  -- NULL - дл€ всех закакзов сразу
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_ag_bdr_to_file';
BEGIN
    PIN.PK406_INFRANET_DATA.EXPORT_AG_BDR_TO_FILE(p_period_id, p_order_no);
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- экспорт сгруппированных агентских BDR в файл
PROCEDURE Export_group_ag_bdr_to_file( 
               p_period_id IN INTEGER               -- формат YYYYMM (201505 - май 2015)
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_ag_bdr_to_file';
BEGIN
    PIN.PK406_INFRANET_DATA.EXPORT_GROUP_AG_BDR_TO_FILE(p_period_id);
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


END PK02_BRM_CALL;
/
