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
              ORDER_NO       VARCHAR2(200), 
              ACCOUNT_NO     VARCHAR2(40), 
              COMPANY        VARCHAR2(1024), 
              SPEED          VARCHAR2(100), 
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
              EARNED_TYPE    NUMBER,
              IDL_TYPE       VARCHAR2(3)
         );
    TYPE rc_disc IS REF CURSOR RETURN t_disc;

    -- –езультат: INFRANET.ORDERS_TIMEOUTS
    PROCEDURE Export_for_idl_to_table;
    
    -- –езультат: /home/billsrv/data/for_idl.csv
    --PROCEDURE Export_for_idl_to_file;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Ёкспорт информации о заказах
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    TYPE t_order IS RECORD (
             ACCOUNT_NO  VARCHAR2(40), 
             ORDER_NO    VARCHAR2(200),  
             DATE_FROM   DATE, 
             DATE_TO     DATE, 
             CREATE_DATE DATE, 
             SERVICE     VARCHAR2(400), 
             SERVICE_ID  INTEGER, 
             STATUS      VARCHAR2(10)
         );
    TYPE rc_order IS REF CURSOR RETURN t_order;

    -- –езультат: INFRANET.BRM_ORDERS
    PROCEDURE Orders_to_table;

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
               
    -- –езультат в таблице INFRANET.BRM_AG_ORDER_T
    PROCEDURE List_ag_orders_to_table;

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

    -- –езультат в таблице INFRANET.BRM_AG_BDR_T
    PROCEDURE List_ag_bdr_to_table( 
                       p_period_id IN INTEGER,              -- формат YYYYMM (201505 - май 2015)
                       p_order_id  IN INTEGER DEFAULT NULL  -- NULL - дл€ всех закакзов сразу
                   );

END PK02_BRM_CALL;
/
CREATE OR REPLACE PACKAGE BODY PK02_BRM_CALL IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Ёкспорт данных в систему подготовки данных по просто€м и SLA
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- –езультат: INFRANET.ORDERS_TIMEOUTS
PROCEDURE Export_for_idl_to_table
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_for_idl_to_table';
BEGIN
    PIN.PK406_INFRANET_DATA.EXPORT_FOR_IDL_TO_TABLE;
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Ёкспорт информации о заказах
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Orders_to_table
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Orders_to_table';
BEGIN
    PIN.PK406_INFRANET_DATA.EXPORT_ORDERS_TO_TABLE;
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
    PIN.PK406_INFRANET_DATA.LIST_AG_ORDERS(p_recordset/*, p_date*/);  
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := PIN.Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(PIN.Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- –езультат в таблице INFRANET.BRM_AG_ORDER_T
PROCEDURE List_ag_orders_to_table
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'List_ag_orders_to_table';
BEGIN
    PIN.PK406_INFRANET_DATA.LIST_AG_ORDERS_TO_TABLE;  
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

-- –езультат в таблице INFRANET.BRM_AG_BDR_T
PROCEDURE List_ag_bdr_to_table( 
                   p_period_id IN INTEGER,              -- формат YYYYMM (201505 - май 2015)
                   p_order_id  IN INTEGER DEFAULT NULL  -- NULL - дл€ всех закакзов сразу
               )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'List_ag_bdr_to_table';
BEGIN
    PIN.PK406_INFRANET_DATA.EXPORT_AG_BDR_TO_TABLE(p_period_id, p_order_id);
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

END PK02_BRM_CALL;
/
