CREATE OR REPLACE PACKAGE PK406_INFRANET_DATA
IS
    --
    -- ДАННЫЕ   ДЛЯ   ЭКСПОРТА   В   B I L L I N G   S E R V E R   ( В. Малиновский )
    --
    -- 10.110.32.160
    -- user: billsrv
    -- passwd: portal6.5
    -- /home/billsrv/data     -- (BS_DATA_DIR)
    -- /home/billsrv/bcr      -- (BCR_DIR) данные для BCR
    -- /home/billsrv/agent    -- (AGENT_DIR) экспорт агентских BDR
    -- подготовка каталогов в БД
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
    -- Экспорт данных в систему подготовки данных по простоям и SLA
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
    
    PROCEDURE Export_for_idl( 
                   p_recordset  OUT t_refc
               );
               
    PROCEDURE Export_for_idl_to_file;
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Экспорт информации о заказах
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
    
    PROCEDURE Export_orders( 
                   p_recordset IN OUT SYS_REFCURSOR --rc_order
               );
               
    PROCEDURE Export_orders_to_file;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Список заказов с двойной тарификацией
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
                   p_recordset IN OUT SYS_REFCURSOR, --rc_ag_order
                   p_date      IN DATE
               );
    
    PROCEDURE List_ag_orders_to_file( 
                   p_date      IN DATE
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- BDR заказов с двойной тарификацией (вторая часть)
    -- если p_order_id is null - выгружается весь месяц
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
                   p_order_no  IN VARCHAR2 DEFAULT NULL -- номер заказа, NULL - для всех закакзов сразу
               );
               
    -- экспорт данных в файл
    PROCEDURE Export_ag_bdr_to_file( 
                   p_period_id IN INTEGER,  -- формат YYYYMM (201505 - май 2015)
                   p_order_no  IN VARCHAR2 DEFAULT NULL -- номер заказа, NULL - для всех закакзов сразу
               );
               
    -- экспорт сгруппированных агентских BDR в файл
    PROCEDURE Export_group_ag_bdr_to_file( 
                   p_period_id IN INTEGER               -- формат YYYYMM (201505 - май 2015)
               );
    
END PK406_INFRANET_DATA;
/
CREATE OR REPLACE PACKAGE BODY PK406_INFRANET_DATA
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Экспорт данных в систему подготовки данных по простоям и SLA
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Export_for_idl( 
               p_recordset  OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_for_idl';
    v_retcode    INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT O.ORDER_NO, 
               A.ACCOUNT_NO, 
               CS.CUSTOMER    COMPANY, 
               CH.SPEED_STR   SPEED, 
               CH.POINT_SRC   S_POINT, 
               CH.POINT_DST   D_POINT,
               O.DATE_FROM    ORDER_DATE,
               S.SERVICE      NAME,
               OB.DATE_FROM   CYCLE_START_T, 
               OB.DATE_TO     CYCLE_END_T,
               O.DATE_FROM    PURCHASE_START_T,
               O.DATE_TO      PURCHASE_END_T,
               O.DATE_FROM    USAGE_START_T,
               O.DATE_TO      USAGE_END_T,
               OBI.FREE_VALUE FREE_DOWNTIME,
               1              EARNED_TYPE
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CUSTOMER_T CS,
               ORDER_T O, ORDER_BODY_T OB, ORDER_BODY_T OBI, 
               ORDER_INFO_T CH, SERVICE_T S 
         WHERE A.STATUS         = 'B'
           AND A.ACCOUNT_TYPE   = 'J'
           AND A.BILLING_ID IN (2001,2002)   
           --   
           AND O.ACCOUNT_ID     = A.ACCOUNT_ID
           AND O.SERVICE_ID     = S.SERVICE_ID
           AND O.SERVICE_ID NOT IN (0,1,2,7)
           AND O.ORDER_ID       = CH.ORDER_ID
           --
           AND AP.ACCOUNT_ID    = A.ACCOUNT_ID
           AND AP.CUSTOMER_ID   = CS.CUSTOMER_ID
           AND AP.DATE_FROM    <= SYSDATE
           AND (AP.DATE_TO IS NULL OR SYSDATE <= AP.DATE_TO )
           --
           AND OB.ORDER_ID      = O.ORDER_ID
           AND OB.CHARGE_TYPE   = 'REC'   
           AND OB.DATE_FROM    <= SYSDATE
           AND (OB.DATE_TO IS NULL OR SYSDATE <= OB.DATE_TO )
           --
           AND OBI.ORDER_ID(+)= O.ORDER_ID
           AND OBI.CHARGE_TYPE(+)  IN ('IDL', 'SLA')
           AND OBI.DATE_FROM(+)    <= SYSDATE
           AND (OBI.DATE_TO IS NULL OR SYSDATE <= OBI.DATE_TO )
        ORDER BY CS.CUSTOMER, A.ACCOUNT_NO, O.ORDER_NO
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- Экспорт данных в систему подготовки данных по простоям и SLA
PROCEDURE Export_for_idl_to_file
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_for_idl_to_file';
    v_output     UTL_FILE.file_type;
    v_dir        VARCHAR2(100)      := с_BS_DATA_DIR;
    v_file_name  VARCHAR2(100 CHAR) := 'for_idl.csv';
    v_file_tmp   VARCHAR2(100 CHAR) := 'for_idl.tmp';
    v_count      INTEGER;
    
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ------------------------------------------------------------------ --
    -- записываем информацию в Файл
    -- ------------------------------------------------------------------ --
    v_count := 0;
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W', 32767 );

    -- сохраняем заголовок
    UTL_FILE.put_line( v_output, 'ORDER_NO;ACCOUNT_NO;COMPANY;SPEED;S_POINT;D_POINT;ORDER_DATE;NAME;CYCLE_START_T;CYCLE_END_T;PURCHASE_START_T;PURCHASE_END_T;USAGE_START_T;USAGE_END_T;FREE_DOWNTIME;EARNED_TYPE');
    
    -- сохраняем строки
    FOR ord IN (
        SELECT /*+ ordered */ 
               O.ORDER_NO||';'||
               A.ACCOUNT_NO||';'||
               CS.CUSTOMER||';'|| 
               CH.SPEED_STR||';'|| 
               CH.POINT_SRC||';'|| 
               CH.POINT_DST||';'||
               TO_CHAR(O.DATE_FROM,'yyyy.mm.dd hh24:mi:ss')||';'||
               S.SERVICE||';'||
               TO_CHAR(OB.DATE_FROM,'yyyy.mm.dd hh24:mi:ss')||';'||
               TO_CHAR(OB.DATE_TO,'yyyy.mm.dd hh24:mi:ss')||';'||
               TO_CHAR(O.DATE_FROM,'yyyy.mm.dd hh24:mi:ss')||';'||
               TO_CHAR(O.DATE_TO,'yyyy.mm.dd hh24:mi:ss')||';'||
               TO_CHAR(O.DATE_FROM,'yyyy.mm.dd hh24:mi:ss')||';'||
               TO_CHAR(O.DATE_TO,'yyyy.mm.dd hh24:mi:ss')||';'||
               OBI.FREE_VALUE||';'||
               1 
               AS TXT
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CUSTOMER_T CS,
               ORDER_T O, ORDER_BODY_T OB, ORDER_BODY_T OBI, 
               ORDER_INFO_T CH, SERVICE_T S 
         WHERE A.STATUS         = 'B'
           AND A.ACCOUNT_TYPE   = 'J'
           AND A.BILLING_ID IN (2001,2002)   
           --   
           AND O.ACCOUNT_ID     = A.ACCOUNT_ID
           AND O.SERVICE_ID     = S.SERVICE_ID
           AND O.SERVICE_ID NOT IN (0,1,2,7)
           AND O.ORDER_ID       = CH.ORDER_ID
           --
           AND AP.ACCOUNT_ID    = A.ACCOUNT_ID
           AND AP.CUSTOMER_ID   = CS.CUSTOMER_ID
           AND AP.DATE_FROM    <= SYSDATE
           AND (AP.DATE_TO IS NULL OR SYSDATE <= AP.DATE_TO )
           --
           AND OB.ORDER_ID      = O.ORDER_ID
           AND OB.CHARGE_TYPE   = 'REC'   
           AND OB.DATE_FROM    <= SYSDATE
           AND (OB.DATE_TO IS NULL OR SYSDATE <= OB.DATE_TO )
           --
           AND OBI.ORDER_ID(+)= O.ORDER_ID
           AND OBI.CHARGE_TYPE(+)  IN ('IDL', 'SLA')
           AND OBI.DATE_FROM(+)    <= SYSDATE
           AND (OBI.DATE_TO IS NULL OR SYSDATE <= OBI.DATE_TO )
        ORDER BY CS.CUSTOMER, A.ACCOUNT_NO, O.ORDER_NO
    ) LOOP
        UTL_FILE.put_line( v_output, ord.txt ) ;
        v_count := v_count + 1;

        IF MOD(v_count,10000) = 0 THEN
            Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Экспорт информации о заказах
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Export_orders( 
               p_recordset  IN OUT SYS_REFCURSOR -- rc_order
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_orders';
    v_retcode    INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT A.ACCOUNT_NO, O.ORDER_NO, O.DATE_FROM, O.DATE_TO, O.CREATE_DATE, S.SERVICE, S.SERVICE_ID, O.STATUS 
          FROM ORDER_T O, ACCOUNT_T A, SERVICE_T S
         WHERE O.ACCOUNT_ID = A.ACCOUNT_ID
           AND A.BILLING_ID IN (Pk00_Const.c_BILLING_KTTK, 
                                Pk00_Const.c_BILLING_OLD,
                                Pk00_Const.c_BILLING_MMTS,
                                Pk00_Const.c_BILLING_OLD_NO_1C)
           AND O.SERVICE_ID = S.SERVICE_ID
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

-- Список всех заказов с в файл
PROCEDURE Export_orders_to_file
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_orders_to_file';
    v_output     UTL_FILE.file_type;
    v_dir        VARCHAR2(100)      := с_BS_DATA_DIR;
    v_file_name  VARCHAR2(100 CHAR) := 'orders.csv';
    v_file_tmp   VARCHAR2(100 CHAR) := 'orders.tmp';
    v_count      INTEGER;
    
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ------------------------------------------------------------------ --
    -- записываем информацию в Файл
    -- ------------------------------------------------------------------ --
    v_count := 0;
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W', 32767 );

    -- сохраняем заголовок
    UTL_FILE.put_line( v_output, 'ACCOUNT_NO;ORDER_NO;DATE_FROM;DATE_TO;CREATE_DATE;SERVICE;SERVICE_ID;STATUS');

    -- сохраняем строки
    FOR ord IN (
        SELECT /*+ ordered */ 
             A.ACCOUNT_NO||';'||
             O.ORDER_NO||';'||
             TO_CHAR(O.DATE_FROM,'yyyy.mm.dd hh24:mi:ss')||';'||
             TO_CHAR(O.DATE_TO,'yyyy.mm.dd hh24:mi:ss')||';'||
             TO_CHAR(O.CREATE_DATE,'yyyy.mm.dd hh24:mi:ss')||';'||
             S.SERVICE||';'||
             S.SERVICE_ID||';'||
             O.STATUS 
             AS TXT
          FROM ORDER_T O, ACCOUNT_T A, SERVICE_T S
         WHERE O.ACCOUNT_ID = A.ACCOUNT_ID
           AND A.BILLING_ID IN (Pk00_Const.c_BILLING_KTTK, 
                              Pk00_Const.c_BILLING_OLD,
                              Pk00_Const.c_BILLING_MMTS,
                              Pk00_Const.c_BILLING_OLD_NO_1C)
           AND O.SERVICE_ID = S.SERVICE_ID
        ORDER BY A.ACCOUNT_NO, O.ORDER_NO, O.DATE_FROM
    ) LOOP
        UTL_FILE.put_line( v_output, ord.txt ) ;
        v_count := v_count + 1;
        
        IF MOD(v_count,10000) = 0 THEN
            Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Список заказов с двойной тарификацией
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE List_ag_orders( 
           p_recordset IN OUT SYS_REFCURSOR, --rc_ag_order
           p_date      IN DATE
       )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'List_ag_orders';
    v_retcode    INTEGER;
    v_date       DATE := NVL(p_date, SYSDATE);
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
        AND v_date BETWEEN o.date_from AND NVL(o.date_to, TO_DATE('2999','yyyy'))
        AND v_date BETWEEN p.date_from AND NVL(p.date_to, TO_DATE('2999','yyyy'))
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

-- Список заказов с двойной тарификацией в файл
PROCEDURE List_ag_orders_to_file( 
               p_date      IN DATE
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'List_ag_orders_to_file';
    v_date       DATE := NVL(p_date, SYSDATE);
    v_output     UTL_FILE.file_type;
    v_dir        VARCHAR2(100)      := с_AGENT_DIR;
    v_file_name  VARCHAR2(100 CHAR) := 'ag_orders.csv';
    v_file_tmp   VARCHAR2(100 CHAR) := 'ag_orders.tmp';
    v_count      INTEGER;
    
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ------------------------------------------------------------------ --
    -- записываем информацию в Файл
    -- ------------------------------------------------------------------ --
    v_count := 0;
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W', 32767 );

    -- сохраняем заголовок
    UTL_FILE.put_line( v_output, 'CONTRACT_NO;ACCOUNT_NO;ACCOUNT_ID;ORDER_NO;ORDER_ID;DATE_FROM;DATE_TO');

    -- сохраняем строки
    FOR ord IN (
        SELECT /*+ ordered */ 
               c.contract_no||';'||
               a.account_no||';'||
               a.account_id||';'||
               o.order_no||';'||
               order_id||';'||
               TO_CHAR(o.date_from,'yyyy.mm.dd hh24:mi:ss')||';'||
               TO_CHAR(o.date_to,'yyyy.mm.dd hh24:mi:ss')||';' 
               AS TXT
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
          AND v_date BETWEEN o.date_from AND NVL(o.date_to, TO_DATE('2999','yyyy'))
          AND v_date BETWEEN p.date_from AND NVL(p.date_to, TO_DATE('2999','yyyy'))
        ORDER BY A.ACCOUNT_NO, O.ORDER_NO, O.DATE_FROM  
    ) LOOP
        UTL_FILE.put_line( v_output, ord.txt ) ;
        v_count := v_count + 1;
        
        IF MOD(v_count,10000) = 0 THEN
            Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- BDR заказов с двойной тарификацией (вторая часть)
-- если p_order_id is null - выгружается весь месяц
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Export_ag_bdr( 
           p_recordset IN OUT SYS_REFCURSOR,    --rc_ag_bdr
           p_period_id IN INTEGER,              -- формат YYYYMM (201505 - май 2015)
           p_order_no  IN VARCHAR2 DEFAULT NULL -- номер заказа, NULL - для всех закакзов сразу
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
             
--  
-- экспорт агентских BDR в файл
--
PROCEDURE Export_ag_bdr_to_file( 
               p_period_id IN INTEGER,              -- формат YYYYMM (201505 - май 2015)
               p_order_no  IN VARCHAR2 DEFAULT NULL -- номер заказа, NULL - для всех закакзов сразу
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_ag_bdr_to_file';
    v_date_from  DATE;
    v_date_to    DATE;
    v_order_id   INTEGER := NULL;
    --
    v_output     UTL_FILE.file_type;
    v_dir        VARCHAR2(100)      := с_AGENT_DIR;
    v_file_name  VARCHAR2(100 CHAR) := 'ag_bdr.csv';
    v_file_tmp   VARCHAR2(100 CHAR) := 'ag_bdr.tmp';
    v_count      INTEGER;
    --
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id='||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

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
    
    -- ------------------------------------------------------------------ --
    -- записываем информацию в Файл
    -- ------------------------------------------------------------------ --
    v_count := 0;
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W', 32767 );

    -- сохраняем заголовок
    UTL_FILE.put_line( v_output, 'START_TIME;LOCAL_TIME;ABN_A;ABN_B;DURATION;BILL_MINUTES;AMOUNT;PRICE;ACCOUNT_ID;ORDER_ID;PREFIX_A;INIT_Z_NAME;PREFIX_B;TERM_Z_NAME');

    -- сохраняем строки
    FOR bdr IN (
        SELECT /*+ ordered */ 
               TO_CHAR(START_TIME,'yyyy.mm.dd hh24:mi:ss')||';'|| -- время вызова по GMT
               TO_CHAR(LOCAL_TIME,'yyyy.mm.dd hh24:mi:ss')||';'|| -- время вызова местное
               abn_a||';'|| 
               abn_b||';'||
               duration||';'||
               bill_minutes||';'|| 
               amount||';'||
               price||';'||
               account_id||';'||
               order_id||';'||
               prefix_a||';'||
               init_z_name||';'||
               prefix_b||';'||
               term_z_name AS TXT
          FROM BDR_AGENT_T
         WHERE (v_order_id IS NULL OR order_id = v_order_id)
           AND rep_period BETWEEN v_date_from AND v_date_to
        ORDER BY START_TIME
    ) LOOP
        UTL_FILE.put_line( v_output, bdr.txt ) ;
        v_count := v_count + 1;
        
        IF MOD(v_count,10000) = 0 THEN
            Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--  
-- экспорт сгруппированных агентских BDR в файл
--
PROCEDURE Export_group_ag_bdr_to_file( 
               p_period_id IN INTEGER               -- формат YYYYMM (201505 - май 2015)
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_group_ag_bdr_to_file';
    v_date_from  DATE;
    v_date_to    DATE;
    v_order_id   INTEGER := NULL;
    --
    v_output     UTL_FILE.file_type;
    v_dir        VARCHAR2(100)      := с_AGENT_DIR;
    v_file_name  VARCHAR2(100 CHAR) := 'ag_bdr_group.csv';
    v_file_tmp   VARCHAR2(100 CHAR) := 'ag_bdr_group.tmp';
    v_count      INTEGER;
    --
BEGIN
    Pk01_Syslog.Write_msg('Start,period_id='||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- период должен быть задан
    IF p_period_id IS NULL THEN
       Pk01_Syslog.Raise_user_exception('Error, p_period_id is null' , c_PkgName||'.'||v_prcName);
    END IF;
    v_date_from  := Pk04_Period.Period_from(p_period_id);
    v_date_to    := Pk04_Period.Period_to(p_period_id);
        
    -- ------------------------------------------------------------------ --
    -- записываем информацию в Файл
    -- ------------------------------------------------------------------ --
    v_count := 0;
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W' );

    -- сохраняем заголовок
    UTL_FILE.put_line( v_output, 'ACCOUNT_ID;ORDER_ID;;LOCAL_TIME;BILL_MINUTES;AMOUNT');

    -- сохраняем строки
    FOR bdr IN (
        SELECT /*+ ordered */ 
               account_id||';'||
               order_id||';'||
               TO_CHAR(TRUNC(LOCAL_TIME,'mm'),'yyyy.mm.dd hh24:mi:ss')||';'|| -- время вызова местное
               SUM(bill_minutes)||';'|| 
               SUM(amount)||';'  AS TXT
          FROM BDR_AGENT_T
         WHERE (v_order_id IS NULL OR order_id = v_order_id)
           AND rep_period BETWEEN v_date_from AND v_date_to
        GROUP BY ACCOUNT_ID, ORDER_ID, TRUNC(LOCAL_TIME,'mm')
    ) LOOP
        UTL_FILE.put_line( v_output, bdr.txt ) ;
        v_count := v_count + 1;
        
        IF MOD(v_count,10000) = 0 THEN
            Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


END PK406_INFRANET_DATA;
/
