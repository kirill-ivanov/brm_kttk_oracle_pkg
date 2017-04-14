CREATE OR REPLACE PACKAGE PK406_INFRANET_DATA
IS
    --
    -- ƒјЌЌџ≈   ƒЋя   Ё —ѕќ–“ј   ¬   B I L L I N G   S E R V E R   ( ¬. ћалиновский )
    --
    -- 10.110.32.160
    -- user: billsrv
    -- passwd: portal6.5
    -- /home/billsrv/data     -- (BS_DATA_DIR)
    -- /home/billsrv/bcr      -- (BCR_DIR) данные дл€ BCR
    -- /home/billsrv/agent    -- (AGENT_DIR) экспорт агентских BDR
    -- подготовка каталогов в Ѕƒ
    -- CREATE OR REPLACE DIRECTORY BCR_DIR AS '/home/billsrv/bcr';
    -- GRANT EXECUTE, READ, WRITE ON DIRECTORY BCR_DIR TO PIN WITH GRANT OPTION;
  
    -- ==============================================================================
    c_PkgName   CONSTANT VARCHAR2(30) := 'PK406_INFRANET_DATA';
    -- ==============================================================================
    TYPE t_refc IS REF CURSOR;
    
    c_RET_OK      CONSTANT INTEGER     := 0;
    c_RET_ER		  CONSTANT INTEGER     :=-1;
    с_BS_DATA_DIR CONSTANT VARCHAR2(11):= 'BS_DATA_DIR';
    с_BCR_DIR     CONSTANT VARCHAR2(7) := 'BCR_DIR';
    с_AGENT_DIR   CONSTANT VARCHAR2(9) := 'AGENT_DIR';
    
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
    
    PROCEDURE Export_for_idl_to_table;
               
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Ёкспорт информации о заказах
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    TYPE t_order IS RECORD (
             CONTRACT_NO VARCHAR2(100),
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
    
    PROCEDURE Export_orders_to_table;

    
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
    
    PROCEDURE List_ag_orders( 
                   p_recordset IN OUT SYS_REFCURSOR --rc_ag_order
               );
   
    PROCEDURE List_ag_orders_to_table;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- BDR заказов с двойной тарификацией (втора€ часть)
    -- если p_order_id is null - выгружаетс€ весь мес€ц
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

    -- просморт результата запроса через курсор    
    PROCEDURE Export_ag_bdr( 
                   p_recordset IN OUT SYS_REFCURSOR,    --rc_ag_bdr
                   p_period_id IN INTEGER,              -- формат YYYYMM (201505 - май 2015)
                   p_order_no  IN VARCHAR2 DEFAULT NULL -- номер заказа, NULL - дл€ всех закакзов сразу
               );

    PROCEDURE Export_ag_bdr_to_table( 
                   p_period_id IN INTEGER,              -- формат YYYYMM (201505 - май 2015)
                   p_order_no  IN VARCHAR2 DEFAULT NULL -- номер заказа, NULL - дл€ всех закакзов сразу
               );
    
END PK406_INFRANET_DATA;
/
CREATE OR REPLACE PACKAGE BODY PK406_INFRANET_DATA
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Ёкспорт данных в систему подготовки данных по просто€м и SLA
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Ёкспорт данных в систему подготовки данных по просто€м и SLA
-- по согласованию с ¬.ћалиновским убрал контроль дат
PROCEDURE Export_for_idl_to_table
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_for_idl_to_table';
    v_count      INTEGER;
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- чистим таблицу
    DELETE FROM INFRANET.TIMEOUTS_ORDERS;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.TIMEOUTS_ORDERS '||v_count||' - rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    -- перезаливаем данные
    INSERT INTO INFRANET.TIMEOUTS_ORDERS
          SELECT DISTINCT
             O.ORDER_NO, 
             A.ACCOUNT_NO, 
             CO.COMPANY_NAME  COMPANY, 
             OI.SPEED_STR     SPEED, 
             OI.POINT_SRC     S_POINT, 
             OI.POINT_DST     D_POINT,
             O.DATE_FROM      ORDER_DATE,
             S.SERVICE        NAME,
             OB.DATE_FROM     CYCLE_START_T, 
             OB.DATE_TO       CYCLE_END_T,
             O.DATE_FROM      PURCHASE_START_T,
             O.DATE_TO        PURCHASE_END_T,
             O.DATE_FROM      USAGE_START_T,
             O.DATE_TO        USAGE_END_T,
             OI.DOWNTIME_FREE,
             1                EARNED_TYPE,
             NVL(OBI.CHARGE_TYPE,'IDL') IDL_TYPE 
        FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, COMPANY_T CO,
             ORDER_T O, ORDER_INFO_T OI, SERVICE_T S,
             ORDER_BODY_T OB, ORDER_BODY_T OBI 
       WHERE A.STATUS         = 'B'
         AND A.ACCOUNT_TYPE   = 'J'
         --
         AND AP.ACCOUNT_ID    = A.ACCOUNT_ID
         --AND AP.DATE_FROM    <= SYSDATE
         --AND (AP.DATE_TO IS NULL OR SYSDATE <= AP.DATE_TO )
         --
         AND AP.CONTRACT_ID   = CO.CONTRACT_ID(+)
         --AND CO.DATE_FROM(+) <= SYSDATE
         --AND (CO.DATE_TO IS NULL OR SYSDATE <= CO.DATE_TO )
         -- 
         AND O.ACCOUNT_ID     = A.ACCOUNT_ID
         AND O.ORDER_ID       = OI.ORDER_ID(+)
         AND O.SERVICE_ID     = S.SERVICE_ID
         AND O.SERVICE_ID NOT IN (0,1,2,7)
         --
         AND OB.ORDER_ID      = O.ORDER_ID
         AND OB.CHARGE_TYPE  IN ('REC', 'MIN')   
         --AND OB.DATE_FROM    <= SYSDATE
         --AND (OB.DATE_TO IS NULL OR SYSDATE <= OB.DATE_TO )
         --
         AND OBI.ORDER_ID(+)   = O.ORDER_ID
         AND OBI.CHARGE_TYPE(+)= 'SLA'
         --AND (OBI.DATE_FROM IS NULL OR  OBI.DATE_FROM <= SYSDATE )
         --AND (OBI.DATE_TO IS NULL OR SYSDATE <= OBI.DATE_TO )
        ORDER BY CO.COMPANY_NAME, A.ACCOUNT_NO, O.ORDER_NO;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.TIMEOUTS_ORDERS '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Ёкспорт информации о заказах
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Export_orders_to_table
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_orders_to_table';
    v_retcode    INTEGER;
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- чистим таблицу
    DELETE FROM INFRANET.BRM_ORDERS;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.BRM_ORDERS '||v_count||' - rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- перезаливаем данные
    INSERT INTO INFRANET.BRM_ORDERS
    SELECT C.CONTRACT_NO, A.ACCOUNT_NO, O.ORDER_NO, 
           O.DATE_FROM, O.DATE_TO, O.CREATE_DATE, S.SERVICE, S.SERVICE_ID, 
           O.STATUS, O.TIME_ZONE, A.BILLING_ID 
      FROM ORDER_T O, ACCOUNT_T A, SERVICE_T S, 
           ACCOUNT_PROFILE_T AP, CONTRACT_T C
     WHERE O.ACCOUNT_ID = A.ACCOUNT_ID
       AND A.BILLING_ID IN (Pk00_Const.c_BILLING_KTTK, 
                            Pk00_Const.c_BILLING_OLD,
                            Pk00_Const.c_BILLING_MMTS,
                            Pk00_Const.c_BILLING_SPB,
                            Pk00_Const.c_BILLING_RP,
                            Pk00_Const.c_BILLING_RP_VOICE,
                            Pk00_Const.c_BILLING_OLD_NO_1C,
                            2009)
       AND O.SERVICE_ID = S.SERVICE_ID
       AND A.ACCOUNT_ID = AP.ACCOUNT_ID
       AND AP.DATE_FROM <= SYSDATE
       AND (AP.DATE_TO IS NULL OR SYSDATE <= AP.DATE_TO)
       AND AP.CONTRACT_ID = C.CONTRACT_ID
    ORDER BY A.ACCOUNT_NO, O.ORDER_NO, O.DATE_FROM
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.BRM_ORDERS '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- —писок заказов с двойной тарификацией
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE List_ag_orders( 
           p_recordset IN OUT SYS_REFCURSOR --rc_ag_order
       )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'List_ag_orders';
    v_retcode    INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- возвращаем курсор
    OPEN p_recordset FOR
      SELECT c.contract_no,  
             a.account_no, 
             a.account_id, 
             o.order_no, 
             order_id, o.date_from, o.date_to   
        FROM account_t a,
             account_profile_t p,
             contract_t c,
             customer_t cl,
             order_t o
      WHERE a.account_id  = p.account_id
        AND p.contract_id = c.contract_id
        AND p.customer_id = cl.customer_id
        AND a.account_id  = o.account_id 
        AND o.agent_rateplan_id IS NOT NULL
    ORDER BY A.ACCOUNT_NO, O.ORDER_NO, O.DATE_FROM
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

PROCEDURE List_ag_orders_to_table 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'List_ag_orders_to_table';
    v_retcode    INTEGER;
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- чистим таблицу
    DELETE FROM INFRANET.BRM_AG_ORDER_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.BRM_AG_ORDER_T'||v_count||' - rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- перезаливаем данные
    INSERT INTO INFRANET.BRM_AG_ORDER_T
    SELECT c.contract_no,  
           a.account_no, 
           a.account_id, 
           o.order_no, 
           order_id, o.date_from, o.date_to   
      FROM account_t a,
           account_profile_t p,
           contract_t c,
           customer_t cl,
           order_t o
    WHERE a.account_id  = p.account_id
      AND p.contract_id = c.contract_id
      AND p.customer_id = cl.customer_id
      AND a.account_id  = o.account_id 
      AND o.agent_rateplan_id IS NOT NULL
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.BRM_AG_ORDER_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- BDR заказов с двойной тарификацией (втора€ часть)
-- если p_order_id is null - выгружаетс€ весь мес€ц
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Export_ag_bdr( 
           p_recordset IN OUT SYS_REFCURSOR,    --rc_ag_bdr
           p_period_id IN INTEGER,              -- формат YYYYMM (201505 - май 2015)
           p_order_no  IN VARCHAR2 DEFAULT NULL -- номер заказа, NULL - дл€ всех закакзов сразу
       )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_ag_bdr';
    v_retcode    INTEGER;
    v_date_from  DATE;
    v_date_to    DATE;
    v_order_id   INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- период должен быть задан
    IF p_period_id IS NULL THEN
       Pk01_Syslog.Raise_user_exception('Error, p_period_id is null' , c_PkgName||'.'||v_prcName);
    END IF;
    v_date_from  := Pk04_Period.Period_from(p_period_id);
    v_date_to    := Pk04_Period.Period_to(p_period_id);
        
    -- получаем ID заказа
    IF p_order_no IS NOT NULL THEN
       SELECT O.ORDER_ID 
         INTO v_order_id 
         FROM ORDER_T O
        WHERE O.ORDER_NO = p_order_no;
    END IF;
    
    -- возвращаем курсор
    OPEN p_recordset FOR
      SELECT /*+parallel BDR_AGENT_T 10*/ 
             start_time, 
             local_time, 
             abn_a, abn_b, 
             duration seconds, bill_minutes, amount, price, 
             account_id, order_id, 
             prefix_a, init_z_name, prefix_b, term_z_name
        FROM BDR_AGENT_T
       WHERE (v_order_id IS NULL OR order_id = v_order_id)
         AND rep_period BETWEEN v_date_from AND v_date_to
      ;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- BDR заказов с двойной тарификацией (втора€ часть)
-- если p_order_id is null - выгружаетс€ весь мес€ц
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Export_ag_bdr_to_table( 
           p_period_id IN INTEGER,              -- формат YYYYMM (201505 - май 2015)
           p_order_no  IN VARCHAR2 DEFAULT NULL -- номер заказа, NULL - дл€ всех закакзов сразу
       )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_ag_bdr_to_table';
    v_retcode    INTEGER;
    v_date_from  DATE;
    v_date_to    DATE;
    v_order_id   INTEGER;
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- чистим таблицу
    DELETE FROM INFRANET.BRM_AG_BDR_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.BRM_AG_BDR_T '||v_count||' - rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- период должен быть задан
    IF p_period_id IS NULL THEN
       Pk01_Syslog.Raise_user_exception('Error, p_period_id is null' , c_PkgName||'.'||v_prcName);
    END IF;
    v_date_from  := Pk04_Period.Period_from(p_period_id);
    v_date_to    := Pk04_Period.Period_to(p_period_id);
        
    -- получаем ID заказа
    IF p_order_no IS NOT NULL THEN
       SELECT O.ORDER_ID 
         INTO v_order_id 
         FROM ORDER_T O
        WHERE O.ORDER_NO = p_order_no;
    END IF;
    
    -- перезаливаем данные
    INSERT INTO INFRANET.BRM_AG_BDR_T
    SELECT /*+parallel BDR_AGENT_T 10*/ 
           start_time, 
           local_time, 
           abn_a, abn_b, 
           duration seconds, bill_minutes, amount, price, 
           account_id, order_id, 
           prefix_a, init_z_name, prefix_b, term_z_name
      FROM BDR_AGENT_T
     WHERE (v_order_id IS NULL OR order_id = v_order_id)
       AND rep_period BETWEEN v_date_from AND v_date_to
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.BRM_AG_BDR_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;  

END PK406_INFRANET_DATA;
/
