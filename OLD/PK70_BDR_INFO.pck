CREATE OR REPLACE PACKAGE PK70_BDR_INFO
IS
    --
    -- Пакет для работы с тарификационными записями о вызовах (BDR) сети ММТС
    -- PIN.E01_BDR_MMTS_T
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK70_BDR_INFO';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc    is ref cursor;
    -- создан глобальный тип
    --create or replace type vc100_table is table of varchar2(100)

    -- получить ID - заказа по его номеру
    FUNCTION Get_order_id_by_no(p_order_no IN VARCHAR2) RETURN INTEGER DETERMINISTIC;
    
    -- разбить строку на массив подстрок
    PROCEDURE Split_string (
            p_table  OUT vc100_table_t,
            p_list   VARCHAR2,
            p_delim  VARCHAR2 DEFAULT ','
        );

    -- получить тарификационные записи о вызовах (BDR)
    --   - при ошибке выставляет исключение
    PROCEDURE View_BDR( 
           p_recordset      OUT t_refc, 
           p_period_id       IN INTEGER,              -- id отчетного периода    
           p_date_from       IN DATE,                 -- диапазон дат
           p_date_to         IN DATE,                 --   начала вызовов
           p_list_order_no   IN VARCHAR2 DEFAULT NULL,-- список номеров заказов
           p_list_abn_a      IN VARCHAR2 DEFAULT NULL,-- список вызывающих абонентов
           p_abc             IN VARCHAR2 DEFAULT NULL,-- префикс вызываемого номера
           p_direction       IN VARCHAR2 DEFAULT NULL,-- направление вызова
           p_min_duration    IN NUMBER   DEFAULT NULL,-- минимальная длительность вызова
           p_max_duration    IN NUMBER   DEFAULT NULL,-- минимальная длительность вызова
           p_min_amount      IN NUMBER   DEFAULT NULL,-- минимальная сумма за вызов
           p_max_amount      IN NUMBER   DEFAULT NULL -- минимальная сумма за вызов
       );
    
    PROCEDURE View_BDR_dyn( 
           p_recordset      OUT t_refc, 
           p_period_id       IN INTEGER,              -- id отчетного периода    
           p_date_from       IN DATE,                 -- диапазон дат
           p_date_to         IN DATE,                 --   начала вызовов
           p_list_order_no   IN VARCHAR2 DEFAULT NULL,-- список номеров заказов
           p_list_abn_a      IN VARCHAR2 DEFAULT NULL,-- список вызывающих абонентов
           p_abc             IN VARCHAR2 DEFAULT NULL,-- префикс вызываемого номера
           p_direction       IN VARCHAR2 DEFAULT NULL,-- направление вызова
           p_min_duration    IN NUMBER   DEFAULT NULL,-- минимальная длительность вызова
           p_max_duration    IN NUMBER   DEFAULT NULL,-- минимальная длительность вызова
           p_min_amount      IN NUMBER   DEFAULT NULL,-- минимальная сумма за вызов
           p_max_amount      IN NUMBER   DEFAULT NULL -- минимальная сумма за вызов
       );
    
    
END PK70_BDR_INFO;
/
CREATE OR REPLACE PACKAGE BODY PK70_BDR_INFO
IS

-- получить ID - заказа по его номеру
FUNCTION Get_order_id_by_no(p_order_no IN VARCHAR2) RETURN INTEGER DETERMINISTIC
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Get_order_id_by_no';
    v_order_id    INTEGER;
BEGIN
    SELECT ORDER_ID INTO v_order_id
      FROM ORDER_T
     WHERE ORDER_NO = p_order_no;
    RETURN v_order_id;  
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.order_no='||p_order_no, c_PkgName||'.'||v_prcName );
END;

-- Разбить строку с разделителями на части
FUNCTION Get_token(
        p_list   VARCHAR2,
        p_index  NUMBER,
        p_delim  VARCHAR2 DEFAULT ','
    ) RETURN VARCHAR2
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Get_token';
    v_start_pos NUMBER;
    v_end_pos   NUMBER;
BEGIN
    IF p_index = 1 THEN
        v_start_pos := 1;
    ELSE
        v_start_pos := INSTR(p_list, p_delim, 1, p_index - 1);
        IF v_start_pos > 0 THEN
            v_start_pos := v_start_pos + LENGTH(p_delim);            
        ELSE
            RETURN NULL;
        END IF;
    END IF;
    v_end_pos := INSTR(p_list, p_delim, v_start_pos, 1);
    IF v_end_pos > 0 THEN
        RETURN SUBSTR(p_list, v_start_pos, v_end_pos - v_start_pos);
    ELSE
        RETURN SUBSTR(p_list, v_start_pos);        
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.', c_PkgName||'.'||v_prcName );
END;

-- разбить строку на массив подстрок
PROCEDURE Split_string (
        p_table  OUT vc100_table_t,
        p_list   VARCHAR2,
        p_delim  VARCHAR2 DEFAULT ','
    )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Split_string';
    v_token     VARCHAR2(40);
    i           PLS_INTEGER := 1 ;
BEGIN
    -- на всякий случай чистим таблицу
    p_table := vc100_table_t();
    IF p_list IS NOT NULL THEN
        LOOP
            v_token := get_token( p_list, i , ',') ;
            EXIT WHEN v_token IS NULL ;
            p_table.EXTEND;
            p_table(i) := v_token;
            i := i + 1 ;
        END LOOP ;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_list='||p_list, c_PkgName||'.'||v_prcName );
END;


-- получить тарификационные записи о вызовах (BDR)
--   - при ошибке выставляет исключение
PROCEDURE View_BDR( 
               p_recordset      OUT t_refc, 
               p_period_id       IN INTEGER,              -- id отчетного периода    
               p_date_from       IN DATE,                 -- диапазон дат
               p_date_to         IN DATE,                 --   начала вызовов
               p_list_order_no   IN VARCHAR2 DEFAULT NULL,-- список номеров заказов
               p_list_abn_a      IN VARCHAR2 DEFAULT NULL,-- список вызывающих абонентов
               p_abc             IN VARCHAR2 DEFAULT NULL,-- префикс вызываемого номера
               p_direction       IN VARCHAR2 DEFAULT NULL,-- направление вызова
               p_min_duration    IN NUMBER   DEFAULT NULL,-- минимальная длительность вызова
               p_max_duration    IN NUMBER   DEFAULT NULL,-- минимальная длительность вызова
               p_min_amount      IN NUMBER   DEFAULT NULL,-- минимальная сумма за вызов
               p_max_amount      IN NUMBER   DEFAULT NULL -- минимальная сумма за вызов
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'View_BDR';
    v_retcode       INTEGER;
    v_period_from   DATE ;
    v_period_to     DATE;
    v_min_duration  NUMBER := NVL(p_min_duration,-1000000);
    v_max_duration  NUMBER := NVL(p_max_duration, 1000000);
    v_min_amount    NUMBER := NVL(p_min_amount,  -1000000);
    v_max_amount    NUMBER := NVL(p_max_amount,   1000000);
    v_abn_table     vc100_table_t := vc100_table_t ();
    v_order_table   vc100_table_t := vc100_table_t ();
BEGIN
    v_period_from := TO_DATE(p_period_id,'YYYYMM');
    v_period_to   := LAST_DAY(v_period_from) + INTERVAL '00 23:59:59' DAY TO SECOND;
    --
    Split_string ( v_abn_table, p_list_abn_a, ',');
    Split_string ( v_order_table, p_list_order_no, ',');
    --    
    OPEN p_recordset FOR
          WITH list_abn_b AS (SELECT /*+ materialize */ * FROM TABLE(v_abn_table) ),
               list_order AS (SELECT /*+ materialize */ * FROM TABLE(v_order_table) )
          SELECT b.LOCAL_TIME CALL_DATE, b.ABN_A, b.ABN_B, b.DURATION,
                 b.ABC_A, a.DIRECTION_NAME DIR_FROM, b.ABC_B, bd.DIRECTION_NAME DIR_TO,
                 b.BILL_MINUTES, b.TARIFF, b.AMOUNT, o.ORDER_NO
            FROM PIN.E01_BDR_MMTS_T b,
                 TARIFF_CB.TRF02_DIRECTION a,
                 TARIFF_CB.TRF02_DIRECTION bd,
                 ORDER_T o
           WHERE b.DIR_A_ID = a.DIRECTION_ID(+)
             AND b.DIR_B_ID = bd.DIRECTION_ID(+)
             AND b.order_id = o.order_id
             AND b.REP_PERIOD BETWEEN v_period_from AND v_period_to
             AND b.LOCAL_TIME BETWEEN p_date_from AND p_date_to
             AND (p_list_order_no IS NULL OR o.ORDER_NO IN (SELECT * FROM list_order))
             AND (p_list_abn_a IS NULL OR b.ABN_A IN (SELECT * FROM list_abn_b))
             AND (p_abc IS NULL OR b.ABC_A LIKE p_abc||'%')
             AND (p_direction IS NULL OR LOWER(bd.DIRECTION_NAME) LIKE LOWER(p_direction||'%'))
             AND b.DURATION BETWEEN v_min_duration AND v_max_duration
             AND b.AMOUNT BETWEEN v_min_amount AND v_max_amount
          ORDER BY b.LOCAL_TIME
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- Пример динамического формирования запроса
-- получить тарификационные записи о вызовах (BDR)
--   - при ошибке выставляет исключение
PROCEDURE View_BDR_dyn( 
               p_recordset      OUT t_refc, 
               p_period_id       IN INTEGER,              -- id отчетного периода    
               p_date_from       IN DATE,                 -- диапазон дат
               p_date_to         IN DATE,                 --   начала вызовов
               p_list_order_no   IN VARCHAR2 DEFAULT NULL,-- список номеров заказов
               p_list_abn_a      IN VARCHAR2 DEFAULT NULL,-- список вызывающих абонентов
               p_abc             IN VARCHAR2 DEFAULT NULL,-- префикс вызываемого номера
               p_direction       IN VARCHAR2 DEFAULT NULL,-- направление вызова
               p_min_duration    IN NUMBER   DEFAULT NULL,-- минимальная длительность вызова
               p_max_duration    IN NUMBER   DEFAULT NULL,-- минимальная длительность вызова
               p_min_amount      IN NUMBER   DEFAULT NULL,-- минимальная сумма за вызов
               p_max_amount      IN NUMBER   DEFAULT NULL -- минимальная сумма за вызов
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'View_BDR_dyn';
    v_retcode       INTEGER;
    v_period_from   DATE ;
    v_period_to     DATE;
    v_SQL           VARCHAR2(2000);
BEGIN
    v_period_from := TO_DATE(p_period_id,'YYYYMM');
    v_period_to   := LAST_DAY(v_period_from) + INTERVAL '00 23:59:59' DAY TO SECOND;
    --
    v_SQL := 'SELECT b.LOCAL_TIME CALL_DATE, b.ABN_A, b.ABN_B, b.DURATION, ' || CHR(10) ||
             '       b.ABC_A, a.DIRECTION_NAME DIR_FROM, b.ABC_B, bd.DIRECTION_NAME DIR_TO, ' || CHR(10) ||
             '       b.BILL_MINUTES, b.TARIFF, b.AMOUNT, o.ORDER_NO ' || CHR(10) ||
             '  FROM PIN.E01_BDR_MMTS_T b, ' || CHR(10) ||
             '       TARIFF_CB.TRF02_DIRECTION a, ' || CHR(10) ||
             '       TARIFF_CB.TRF02_DIRECTION bd, ' || CHR(10) ||
             '       ORDER_T o ' || CHR(10) ||
             ' WHERE b.DIR_A_ID = a.DIRECTION_ID(+) ' || CHR(10) ||
             '   AND b.DIR_B_ID = bd.DIRECTION_ID(+) ' || CHR(10) ||
             '   AND b.order_id = o.order_id ' || CHR(10) ||
             '   AND b.REP_PERIOD BETWEEN :v_period_from AND :v_period_to '|| CHR(10) ||
             '   AND b.LOCAL_TIME BETWEEN :p_date_from AND :p_date_to ';
    IF p_list_order_no IS NOT NULL THEN
        v_SQL := v_SQL || CHR(10) ||
             '   AND o.ORDER_NO IN ( '||p_list_order_no||' )';
    END IF;
    IF p_list_abn_a IS NOT NULL THEN
        v_SQL := v_SQL || CHR(10) ||
             '   AND b.ABN_A IN ( '||p_list_abn_a||' )';
    END IF;
    IF p_abc IS NOT NULL THEN
        v_SQL := v_SQL || CHR(10) ||
             '   AND b.ABC_A LIKE '''||p_abc||'%''';
    END IF;
    IF p_direction IS NOT NULL THEN
        v_SQL := v_SQL || CHR(10) ||
             '   AND LOWER(bd.DIRECTION_NAME) LIKE LOWER('''||p_direction||''')'||'||''%''';
    END IF;
    IF p_min_duration IS NOT NULL AND p_max_duration IS NOT NULL THEN
        v_SQL := v_SQL || CHR(10) ||
             '   AND b.DURATION BETWEEN '||p_min_duration||' AND '||p_max_duration;
    ELSIF p_min_duration IS NOT NULL THEN
        v_SQL := v_SQL || CHR(10) ||
             '   AND b.DURATION BETWEEN >= '||p_min_duration;
    ELSIF p_max_duration IS NOT NULL THEN
        v_SQL := v_SQL || CHR(10) ||
             '   AND b.DURATION BETWEEN <= '||p_max_duration;
    END IF;
    IF p_min_amount IS NOT NULL AND p_max_amount IS NOT NULL THEN
        v_SQL := v_SQL || CHR(10) ||
             '   AND b.AMOUNT '||p_min_amount||' AND '||p_max_amount;
    ELSIF p_min_amount IS NOT NULL THEN
        v_SQL := v_SQL || CHR(10) ||
             '   AND b.AMOUNT BETWEEN >= '||p_min_amount;
    ELSIF p_max_amount IS NOT NULL THEN
        v_SQL := v_SQL || CHR(10) ||
             '   AND b.AMOUNT BETWEEN <= '||p_max_amount;
    END IF;
    -- -----------------------------------------------------------------------------------
    OPEN p_recordset FOR v_SQL 
    USING v_period_from, v_period_to, 
          p_date_from, p_date_to; 
    -- -----------------------------------------------------------------------------------
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR.v_SQL='||v_SQL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
    
END;



END PK70_BDR_INFO;
/
