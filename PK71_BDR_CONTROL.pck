CREATE OR REPLACE PACKAGE PK71_BDR_CONTROL
IS
    --
    -- Пакет для контроля тарификационных записей о вызовах (BDR) сети ММТС
    -- PIN.E04_BDR_MMTS_T
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK71_BDR_CONTROL';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc    is ref cursor;

    -- ---------------------------------------------------------- --
    -- Получить статистику по ошибкам BDR за указанный период
    -- ---------------------------------------------------------- --
    PROCEDURE MMTS_BDR_errors_stat( 
                   p_recordset      OUT t_refc, 
                   p_period_id       IN INTEGER              -- id отчетного периода    
               );


    -- ---------------------------------------------------------- --
    -- Получить статистику по ошибкам BDR за указанный период,
    -- для указанного заказа или для всех заказов (p_order_no = NULL)
    -- ---------------------------------------------------------- --
    PROCEDURE MMTS_BDR_order_errors_stat( 
                   p_recordset  OUT t_refc, 
                   p_period_id   IN INTEGER,  -- id отчетного периода
                   p_order_no    IN INTEGER DEFAULT NULL -- номер заказа (NULL - по всем)
               );

    -- ---------------------------------------------------------- --
    -- Получить детализацию по BDR с оштбками за указанный период,
    -- для указанного заказа или для всех заказов (p_order_no = NULL)
    -- ---------------------------------------------------------- --
    PROCEDURE MMTS_BDR_order_errors_detail( 
                   p_recordset  OUT t_refc, 
                   p_period_id   IN INTEGER,  -- id отчетного периода
                   p_order_no    IN INTEGER DEFAULT NULL -- номер заказа (NULL - по всем)
               );
               
    -- ---------------------------------------------------------- --
    -- Статистика по привязке л/счетов. МГМН
    -- ---------------------------------------------------------- --
    PROCEDURE MMTS_CDR_bind_stat( 
                   p_recordset  OUT t_refc,
                   p_date_from  DATE,
                   p_date_to    DATE 
               );

    -- ---------------------------------------------------------- --
    -- Статистика по привязке л/счетов ЗОНОВОЙ сети Москвы
    -- ---------------------------------------------------------- --
    PROCEDURE ZONES_CDR_bind_stat( 
                   p_recordset  OUT t_refc,
                   p_date_from  DATE,
                   p_date_to    DATE 
               );
               

END PK71_BDR_CONTROL;
/
CREATE OR REPLACE PACKAGE BODY PK71_BDR_CONTROL
IS

-- ---------------------------------------------------------- --
-- Получить статистику по ошибкам BDR за указанный период
-- ---------------------------------------------------------- --
PROCEDURE MMTS_BDR_errors_stat( 
               p_recordset      OUT t_refc, 
               p_period_id       IN INTEGER              -- id отчетного периода    
           )

IS
    v_prcName       CONSTANT VARCHAR2(30) := 'MMTS_BDR_errors_stat';
    v_retcode       INTEGER;
    v_period_from   DATE ;
    v_period_to     DATE;
BEGIN
    v_period_from := TO_DATE(p_period_id,'YYYYMM');
    v_period_to   := LAST_DAY(v_period_from) + INTERVAL '00 23:59:59' DAY TO SECOND;
    OPEN p_recordset FOR
        SELECT NVL(d.NAME,'Ошибка не определена') trf_status,
               b.bdr_status err_code, b.calls, b.seconds, 
               ROUND((b.calls/b.cnt)*100,2) Percent_of_calls
          FROM ( 
                SELECT b.bdr_status, COUNT(1) calls, SUM(duration) seconds, 
                       SUM(COUNT(1)) OVER() cnt
                  FROM E04_BDR_MMTS_T b
                 WHERE b.rep_period BETWEEN v_period_from AND v_period_to
                 GROUP BY bdr_status  
               ) b,
               DICTIONARY_T d    
         WHERE d.parent_id(+) = 21
           AND b.bdr_status = d.external_id(+)
        ORDER BY err_code DESC;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ---------------------------------------------------------- --
-- Получить статистику по ошибкам BDR за указанный период,
-- для указанного заказа или для всех заказов (p_order_no = NULL)
-- ---------------------------------------------------------- --
PROCEDURE MMTS_BDR_order_errors_stat( 
               p_recordset  OUT t_refc, 
               p_period_id   IN INTEGER,  -- id отчетного периода
               p_order_no    IN INTEGER DEFAULT NULL -- номер заказа (NULL - по всем)
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'MMTS_BDR_order_errors_stat';
    v_retcode       INTEGER;
    v_period_from   DATE ;
    v_period_to     DATE;
BEGIN
    v_period_from := TO_DATE(p_period_id,'YYYYMM');
    v_period_to   := LAST_DAY(v_period_from) + INTERVAL '00 23:59:59' DAY TO SECOND;
    OPEN p_recordset FOR
        SELECT B.ORDER_ID, B.ORDER_NO,
               B.BDR_STATUS ERR_CODE, B.CALLS, B.SECONDS, 
               ROUND((B.CALLS/B.CNT)*100,2) REPCENT_OF_CALLS,
               NVL(D.NAME,'Ошибка не определена') TRF_STATUS
          FROM ( 
                SELECT B.ORDER_ID, B.ORDER_NO, B.BDR_STATUS, 
                       COUNT(1) calls, SUM(duration) seconds, SUM(COUNT(1)) OVER() cnt
                  FROM E04_BDR_MMTS_T B
                 WHERE B.REP_PERIOD BETWEEN v_period_from AND v_period_to
                   AND B.BDR_STATUS != 0
                   AND (p_order_no IS NULL OR B.ORDER_NO = p_order_no)
                 GROUP BY B.ORDER_ID, B.ORDER_NO, B.BDR_STATUS  
               ) B,
               DICTIONARY_T D    
         WHERE D.PARENT_ID(+) = 21
           AND B.BDR_STATUS = D.EXTERNAL_ID(+)
        ORDER BY REPCENT_OF_CALLS DESC, ORDER_NO, ERR_CODE DESC;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ---------------------------------------------------------- --
-- Получить детализацию по BDR с оштбками за указанный период,
-- для указанного заказа или для всех заказов (p_order_no = NULL)
-- ---------------------------------------------------------- --
PROCEDURE MMTS_BDR_order_errors_detail( 
               p_recordset  OUT t_refc, 
               p_period_id   IN INTEGER,  -- id отчетного периода
               p_order_no    IN INTEGER DEFAULT NULL -- номер заказа (NULL - по всем)
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'MMTS_BDR_order_errors_detail';
    v_retcode       INTEGER;
    v_period_from   DATE ;
    v_period_to     DATE;
BEGIN
    v_period_from := TO_DATE(p_period_id,'YYYYMM');
    v_period_to   := LAST_DAY(v_period_from) + INTERVAL '00 23:59:59' DAY TO SECOND;
    OPEN p_recordset FOR
      SELECT B.REP_PERIOD, B.START_TIME, B.LOCAL_TIME, 
             B.DURATION, B.BILL_MINUTES, 
             B.ABN_A, B.ABN_B, B.ORDER_NO, B.BDR_STATUS,
             B.PREFIX_A, B.PREFIX_B, B.INIT_Z_NAME, B.TERM_Z_NAME,
             NVL(D.NAME,'Ошибка не определена') TRF_STATUS
        FROM ( 
              SELECT B.*
                FROM E04_BDR_MMTS_T B
               WHERE B.REP_PERIOD BETWEEN v_period_from AND v_period_to
                 AND B.BDR_STATUS != 0
                 AND B.ORDER_NO = '14LD016791'
             ) B,
             DICTIONARY_T D    
       WHERE D.PARENT_ID(+) = 21
         AND B.BDR_STATUS = D.EXTERNAL_ID(+)
      ORDER BY B.START_TIME;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ---------------------------------------------------------- --
-- Статистика по привязке л/счетов. МГМН
-- ---------------------------------------------------------- --
PROCEDURE MMTS_CDR_bind_stat( 
               p_recordset  OUT t_refc,
               p_date_from  DATE,
               p_date_to    DATE 
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'MMTS_CDR_bind_stat';
    v_retcode       INTEGER;
BEGIN
    OPEN p_recordset FOR
      SELECT NVL(d.NAME,'Ошибка не определена') bind_status,
             c.bind_status err_code,
             c.calls, c.seconds,
             c.first_event, c.last_event
        FROM ( 
              SELECT (CASE WHEN TO_NUMBER(c.abn_a) > 0 THEN 1
                           WHEN c.abn_a IS NULL THEN 0
                           ELSE TO_NUMBER(c.abn_a)
                     END) bind_status, 
                     COUNT(1) calls, SUM(c.i_conversation_time) seconds,
                     MIN(i_ans_time) first_event,
                     MAX(i_ans_time) last_event
                FROM mdv.t03_mmts_cdr c
               WHERE c.i_ans_time BETWEEN p_date_from AND p_date_to
               GROUP BY (CASE WHEN TO_NUMBER(c.abn_a) > 0 THEN 1
                              WHEN c.abn_a IS NULL THEN 0
                              ELSE TO_NUMBER(c.abn_a)
                         END)       
             ) c,
             dictionary_t d    
       WHERE d.parent_id(+) = 22
         AND c.bind_status = d.external_id(+);

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- ---------------------------------------------------------- --
-- Статистика по привязке л/счетов ЗОНОВОЙ сети Москвы
-- ---------------------------------------------------------- --
PROCEDURE ZONES_CDR_bind_stat( 
               p_recordset  OUT t_refc,
               p_date_from  DATE,
               p_date_to    DATE 
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'ZONES_CDR_bind_stat';
    v_retcode       INTEGER;
BEGIN
    OPEN p_recordset FOR
      -- Статистика по привязке л/счетов. Зоновый трафик
      SELECT NVL(d.NAME,'Ошибка не определена') bind_status,
             c.bind_status err_code,
             c.calls, c.seconds,
             c.first_event, c.last_event
        FROM ( 
              SELECT (CASE WHEN TO_NUMBER(c.abn_a) > 0 THEN 1
                           WHEN c.abn_a IS NULL THEN 0
                           ELSE TO_NUMBER(c.abn_a)
                     END) bind_status, 
                     COUNT(1) calls, SUM(c.conversation_time) seconds,
                     MIN(ans_time) first_event,
                     MAX(ans_time) last_event
                FROM mdv.Z03_ZONE_CDR c
               WHERE c.ans_time BETWEEN p_date_from AND p_date_to
                 AND NVL(c.cdr_state,0) != -1
               GROUP BY (CASE WHEN TO_NUMBER(c.abn_a) > 0 THEN 1
                              WHEN c.abn_a IS NULL THEN 0
                              ELSE TO_NUMBER(c.abn_a)
                         END)       
             ) c,
             dictionary_t d    
       WHERE d.parent_id(+) = 22
         AND c.bind_status = d.external_id(+);

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK71_BDR_CONTROL;
/
