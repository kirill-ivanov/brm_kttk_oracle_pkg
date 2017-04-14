CREATE OR REPLACE PACKAGE PK38_BILLING_DOWNTIME
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK38_BILLING_DOWNTIME';
    -- ==============================================================================
    type t_refc is ref cursor;
    -- 
    -- задача для начисления компенсации за простои
    c_TASK_DOWNTIME_ID CONSTANT INTEGER := 0;
    --
    -- Расчет суммы компенсации простоев канала
    -- таблицы : DOWNTIME_T, SLA_PERCENT_T
    -- ВНИМАНИЕ!!! Расчеты ведется после начисления абонплаты
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_FLAG_H  CONSTANT INTEGER := 0;  -- простои заданы в часах
    c_FLAG_K  CONSTANT INTEGER := 1;  -- задан коэффициент доступности канала

    c_DEFAULT_FREE_DOWNTIME CONSTANT INTEGER := 43; -- стандартная величина некомпенсируемого простоя
    
    -- статусы этапов начисления
    c_DS_BIND_OB_ER      CONSTANT INTEGER := -2;  -- ошибка привязки к компоненте услуги заказа
    c_DS_BIND_O_ER       CONSTANT INTEGER := -1;  -- ошибка привязки к заказу
    c_DS_BIND_ORDER      CONSTANT INTEGER :=  0;  -- данные о простое привязаны к заказу
    c_DS_BIND_ORDER_BODY CONSTANT INTEGER :=  1;  -- данные о простое привязаны к компоненте услуги заказа
    c_DS_OK              CONSTANT INTEGER :=  2;  -- начисления проведены

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Включение в счета компенсаций за простои 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    -- ---------------------------------------------------------------------- --
    PROCEDURE Downtime_processing( p_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Заполнение таблицы простоев
    -- Возвращает:
    --   0 - заказ не найден
    --   1 - данные загружены (или обнаружен дубликат записи)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Load_Downtime (
                  p_downtime_id IN INTEGER, -- ID (чтобы 2 раза не грузить, присваивают в ДРУ при создании записи)
                  p_order_no  IN VARCHAR2,   -- номер заказа
                  p_date_from IN DATE,      -- дата начала периода
                  p_date_to   IN DATE,      -- дата окончания периода
                  p_value     IN NUMBER,    -- простой в часах (с десятыми и сотыми, а не с минутами)
                  p_flag      IN INTEGER    -- показывает в чем прстой, в часах или К.доступности. 
              ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Привязка данных о простоях к заказам
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Bind_Downtime( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем очередь из счетов, которым необходима корректировка 
    -- по компенсации простоев
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Mark_bills( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Досоздаем стандартные компоненты услуги для компенсации простоев, 
    -- где это необходимо 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_order_body( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расчет компенсации абонплаты на время простоя, заданного в часах,
    -- без указания SLA
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Charge_Downtime( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расчет компенсации абонплаты на время простоя, заданного в часах,
    -- с указанием SLA
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Charge_SLA_H( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расчет компенсации абонплаты на время простоя, 
    -- заданного коэффициентом доступности (К),
    -- с указанием SLA
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Charge_SLA_K( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Экспорт данных в систему подготовки данных по простоям и SLA
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Export_to_pindb( 
                   p_recordset  IN OUT t_refc
               );
    
    -- --------------------------------------------------------------------- --
    --              Р А Б О Т А    Н А Д    О Ш И Б К А М И                  --
    -- --------------------------------------------------------------------- --
    -- Расформирование компенсаций за простои
    PROCEDURE Rollback_downtimes( p_task_id IN INTEGER );
    --
    -- Перефомирование компенсаций за простои
    PROCEDURE Recharge_downtimes( p_task_id IN INTEGER );
    
END PK38_BILLING_DOWNTIME;
/
CREATE OR REPLACE PACKAGE BODY PK38_BILLING_DOWNTIME
IS

-- ---------------------------------------------------------------------- --
-- Включение в счета компенсаций за простои 
-- ---------------------------------------------------------------------- --
PROCEDURE Downtime_processing( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Downtime_processing';
    v_task_id    CONSTANT INTEGER      := c_TASK_DOWNTIME_ID;
    v_retcode    INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1) Привязка данных о простоях к заказам
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Bind_Downtime( p_period_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2) готовим список счетов и досоздаем недостающие компоненты
    Mark_bills( p_period_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3) расформировываем счета
    Pk30_Billing_Queue.Rollback_bills(v_task_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- 4) производим компенсацию простое
    -- Расчет компенсации абонплаты на время простоя, заданного в часах,
    Charge_Downtime( v_task_id );
       
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 5) Расчет компенсации абонплаты на время простоя, заданного в часах,
    -- с указанием SLA
    Charge_SLA_H( v_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 6) Расчет компенсации абонплаты на время простоя, 
    -- заданного коэффициентом доступности (К),
    -- с указанием SLA
    Charge_SLA_K( v_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 7) формируем счета
    PK30_BILLING_BASE.Make_bills( v_task_id );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Заполнение таблицы простоев
-- Возвращает:
--   0 - заказ не найден
--   1 - данные загружены
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Load_Downtime (
              p_downtime_id IN INTEGER, -- ID (чтобы 2 раза не грузить, присваивают в ДРУ при создании записи)
              p_order_no  IN VARCHAR2,  -- номер заказа
              p_date_from IN DATE,      -- дата начала периода
              p_date_to   IN DATE,      -- дата окончания периода
              p_value     IN NUMBER,    -- простой в часах (с десятыми и сотыми, а не с минутами)
              p_flag      IN INTEGER    -- показывает в чем прстой, в часах или К.доступности. 
          ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Load_Downtime';
    v_period_id     INTEGER := NULL; -- проставим во время привязки
    v_count         INTEGER;
BEGIN
    -- добавляем запись о простоях
    INSERT INTO DOWNTIME_T (
        DOWNTIME_ID, REP_PERIOD_ID, ORDER_ID, ORDER_NO, 
        DATE_FROM, DATE_TO, VALUE, FLAGS, STATUS, CREATE_DATE)
    SELECT p_downtime_id, v_period_id, O.ORDER_ID, O.ORDER_NO,
           p_date_from, p_date_to, p_value, p_flag, NULL, SYSDATE     
      FROM ORDER_T O
     WHERE O.ORDER_NO  = p_order_no
       AND O.DATE_FROM < p_date_to
       AND (O.DATE_TO IS NULL OR p_date_from < O.DATE_TO);
    v_count := SQL%ROWCOUNT;
    -- возвращаем кол-во добавленных записей
    RETURN v_count;
EXCEPTION 
    WHEN DUP_VAL_ON_INDEX THEN
       RETURN 1;  -- запись нашли, далее проблемы мои
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

/*
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Работа со строками таблици SLA_PERCENT_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Add_SLA_percent (
              p_order_body_id IN INTEGER,
              p_percent       IN NUMBER,
              p_k_min         IN NUMBER,
              p_k_max         IN NUMBER
          ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Add_SLA_percent';
    v_percent_ID    INTEGER;
BEGIN
    v_percent_id := 0;
    INSERT INTO SLA_PERCENT_T (PERCENT_ID, ORDER_BODY_ID, SLA_PERCENT, K_MIN, K_MAX)
    VALUES (v_percent_id, p_order_body_id, p_percent, p_k_min, p_k_max);
    RETURN v_percent_id;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE Edit_SLA_percent(
              p_percent_id IN INTEGER,
              p_percent       IN NUMBER,
              p_k_min         IN NUMBER,
              p_k_max         IN NUMBER
          )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Edit_SLA_percent';
BEGIN
    UPDATE SLA_PERCENT_T SET SLA_PERCENT = p_percent, K_MIN = p_k_min, K_MAX = p_k_max
     WHERE PERCENT_ID = p_percent_id;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE Del_SLA_percent(
              p_percent_id IN INTEGER
          )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Del_SLA_percent';
BEGIN
    DELETE FROM SLA_PERCENT_T WHERE PERCENT_ID = p_percent_id;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Привязка данных о простоях к заказам
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Bind_Downtime( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Charge_Downtime';
    v_period_from   DATE;
    v_period_to     DATE;
    v_ok            INTEGER;
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);
    --
    
    -- 1) Добавляем компоненты услуг для простоев где они отсутствуют
    -- добавление строк с SLA - смысла не имеет, т.к нужно заполнить таблицу
    INSERT INTO ORDER_BODY_T(
        ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, DATE_FROM, DATE_TO, 
        RATEPLAN_ID, RATE_VALUE, FREE_VALUE, RATE_RULE_ID, RATE_LEVEL_ID, 
        TAX_INCL, QUANTITY, CREATE_DATE, MODIFY_DATE, CURRENCY_ID, NOTES
    )
    SELECT PK02_POID.NEXT_ORDER_BODY_ID ORDER_BODY_ID, O.ORDER_ID, 
           Pk00_Const.c_SUBSRV_IDL, 
           Pk00_Const.c_CHARGE_TYPE_IDL CHARGE_TYPE,
           OB.DATE_FROM, OB.DATE_TO, NULL RATEPLAN_ID, NULL RATE_VALUE, 43 FREE_VALUE, 
           Pk00_Const.c_RATE_RULE_IDL_STD RATE_RULE_ID,
           Pk00_Const.c_RATE_LEVEL_ORDER RATE_LEVEL_ID, 
           OB.TAX_INCL, 1 QUANTITY, 
           SYSDATE CREATE_DATE, SYSDATE MODIFY_DATE, OB.CURRENCY_ID, 
           'Создано автоматически при расчете компенсации за просои' NOTES
      FROM DOWNTIME_T T, ORDER_T O, ORDER_BODY_T OB
     WHERE T.REP_PERIOD_ID IS NULL
       AND T.ORDER_NO = O.ORDER_NO 
       AND O.DATE_FROM < v_period_to
       AND (O.DATE_TO IS NULL OR v_period_from < O.DATE_TO)
       AND O.ORDER_ID = OB.ORDER_ID
       AND OB.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_REC, 
                              Pk00_Const.c_CHARGE_TYPE_MIN)
       AND FLAGS = 0 -- Добавляем только простои заданные в часах
       AND NOT EXISTS (
          SELECT * FROM ORDER_BODY_T IDL
           WHERE IDL.ORDER_ID = OB.ORDER_ID
             AND IDL.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_IDL, 
                                     Pk00_Const.c_CHARGE_TYPE_SLA)
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T '||v_count||' записей добавлено', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- 1) Привязка данных о простоях к заказу:
    UPDATE DOWNTIME_T T 
       SET (T.ACCOUNT_ID, T.ORDER_ID) = (
           SELECT O.ACCOUNT_ID, O.ORDER_ID
             FROM ORDER_T O
            WHERE O.ORDER_NO = T.ORDER_NO
              AND O.DATE_FROM < v_period_to
              AND (O.DATE_TO IS NULL OR v_period_from < O.DATE_TO)
       )
     WHERE T.ITEM_ID IS NULL
       AND T.REP_PERIOD_ID IS NULL 
       AND T.STATUS IS NULL;
    v_ok := SQL%ROWCOUNT;
    --
    UPDATE DOWNTIME_T T 
       SET T.STATUS = DECODE(T.ORDER_ID, NULL, c_DS_BIND_O_ER, c_DS_BIND_ORDER)
     WHERE T.ITEM_ID IS NULL
       AND T.REP_PERIOD_ID IS NULL 
       AND T.STATUS IS NULL;
    v_count := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg('Привязано к заказам '||v_ok||' записей из '||v_count, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- 2) Привязка данных о простоях к компоненте услуги
    UPDATE DOWNTIME_T T 
       SET (T.ORDER_BODY_ID, T.CHARGE_TYPE) = (
           SELECT OB.ORDER_BODY_ID, OB.CHARGE_TYPE
             FROM ORDER_BODY_T OB
            WHERE OB.ORDER_ID = T.ORDER_ID
              AND OB.DATE_FROM < v_period_to
              AND (OB.DATE_TO IS NULL OR v_period_from < OB.DATE_TO)
              AND OB.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_IDL, 
                                     Pk00_Const.c_CHARGE_TYPE_SLA)
       )
     WHERE T.STATUS = c_DS_BIND_ORDER
       AND T.REP_PERIOD_ID IS NULL 
       AND T.ITEM_ID IS NULL;
    v_count := SQL%ROWCOUNT;
    --
    UPDATE DOWNTIME_T T 
       SET T.STATUS = DECODE(T.ORDER_BODY_ID, NULL, c_DS_BIND_OB_ER, c_DS_BIND_ORDER_BODY)
     WHERE T.STATUS = c_DS_BIND_ORDER
       AND T.REP_PERIOD_ID IS NULL 
       AND T.ITEM_ID IS NULL;

    UPDATE DOWNTIME_T T SET T.REP_PERIOD_ID = p_period_id
     WHERE T.STATUS = c_DS_BIND_ORDER_BODY
       AND T.REP_PERIOD_ID IS NULL 
       AND T.ITEM_ID       IS NULL;
    v_ok := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg('Привязано к компонентам услуг '||v_ok||' записей из '||v_count, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Создаем очередь из счетов, которым необходима корректировка 
-- по компенсации простоев
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Mark_bills( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Mark_bills';
    v_task_id       CONSTANT INTEGER      := c_TASK_DOWNTIME_ID;
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||v_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- чистим очередь от возможных записей предыдущих работ
    DELETE FROM BILLING_QUEUE_T Q
     WHERE TASK_ID = v_task_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ставим периодические ('B') счета в очередь
    INSERT INTO BILLING_QUEUE_T Q (
      Q.BILL_ID, Q.ACCOUNT_ID, Q.BILLING_ID, Q.PROFILE_ID, 
      Q.TASK_ID, Q.REP_PERIOD_ID, Q.DATA_PERIOD_ID
    )
    SELECT DISTINCT
           B.BILL_ID, B.ACCOUNT_ID, A.BILLING_ID, B.PROFILE_ID, 
           v_task_id TASK_ID, p_period_id REP_PERIOD_ID, p_period_id DATA_PERIOD_ID 
      FROM DOWNTIME_T DT, BILL_T B, ACCOUNT_T A
     WHERE DT.STATUS        = c_DS_BIND_ORDER_BODY
       AND DT.REP_PERIOD_ID = p_period_id
       -- для страховки
       AND DT.ACCOUNT_ID    IS NOT NULL
       AND DT.ORDER_ID      IS NOT NULL
       AND DT.ORDER_BODY_ID IS NOT NULL
       AND DT.ITEM_ID       IS NULL
       -- 
       AND DT.ACCOUNT_ID    = B.ACCOUNT_ID
       AND DT.REP_PERIOD_ID = B.REP_PERIOD_ID
       --
       AND A.ACCOUNT_ID     = B.ACCOUNT_ID
       AND B.BILL_TYPE      = PK00_CONST.c_BILL_TYPE_REC;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    
    -- Собираем статистику по таблице очереди
    Gather_Table_Stat(l_Tab_Name => 'BILLING_QUEUE_T');
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Досоздаем стандартные компоненты услуги для компенсации простоев, 
    -- чтобы не забыть 
    Make_order_body( v_task_id );
    
    -- подтверждаем изменения
    COMMIT;
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Досоздаем стандартные компоненты услуги для компенсации простоев, 
-- где это необходимо 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Make_order_body( p_task_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Mark_bills';
    v_task_id       CONSTANT INTEGER      := c_TASK_DOWNTIME_ID;
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||v_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- создаем компоненты услуг для компенсации простоев
    INSERT INTO ORDER_BODY_T (
        ORDER_BODY_ID,
        ORDER_ID,
        SUBSERVICE_ID,
        CHARGE_TYPE,
        DATE_FROM,
        DATE_TO,
        FREE_VALUE,
        RATE_RULE_ID, 
        RATE_LEVEL_ID,
        TAX_INCL,
        QUANTITY,
        CREATE_DATE,
        MODIFY_DATE,
        CURRENCY_ID,
        NOTES
    )
    SELECT SQ_ORDER_ID.NEXTVAL ORDER_BODY_ID, ORDER_ID,
           Pk00_Const.c_SUBSRV_IDL SUBSERVICE_ID,    -- c_SUBSRV_IDL    CONSTANT integer := 36;  -- Компенсация простоев
           Pk00_Const.c_CHARGE_TYPE_IDL CHARGE_TYPE, -- c_CHARGE_TYPE_IDL := 'IDL'
           DATE_FROM, 
           DATE_TO,
           c_DEFAULT_FREE_DOWNTIME FREE_VALUE,       -- 43
           Pk00_Const.c_RATE_RULE_IDL_STD RATE_RULE_ID,  -- c_RATE_RULE_IDL_STD    CONSTANT INTEGER := 2404; -- Компенсация простоев, стандартная схема  
           Pk00_Const.c_RATE_LEVEL_ORDER RATE_LAVEL_ID, -- c_RATE_LEVEL_ORDER     CONSTANT INTEGER := 2302; -- Тариф указан на заказ
           'N' TAX_INCL,
           1 QUANTITY,
           SYSDATE CREATE_DATE,
           SYSDATE MODIFY_DATE,
           CURRENCY_ID,
           'создано при выставлении счета' NOTES      
      FROM ( 
        SELECT ROW_NUMBER() OVER (PARTITION BY O.ORDER_ID ORDER BY OB.DATE_FROM) RN, 
               O.ORDER_ID, O.DATE_FROM, O.DATE_TO, OB.CURRENCY_ID 
          FROM ORDER_T O, ORDER_BODY_T OB
         WHERE O.ORDER_ID = OB.ORDER_ID
           AND OB.CHARGE_TYPE = 'REC'
           AND NOT EXISTS (
            SELECT * FROM ORDER_BODY_T OB
             WHERE O.ORDER_ID = OB.ORDER_ID
               AND OB.CHARGE_TYPE = 'IDL'
               AND OB.ORDER_ID = O.ORDER_ID
           )
           AND EXISTS (
              SELECT * FROM BILLING_QUEUE_T Q
               WHERE O.ACCOUNT_ID = Q.ACCOUNT_ID
                AND Q.TASK_ID = 1        
           )
    )
    WHERE RN = 1;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T: '||v_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  Расчет скидки для абонплаты на время простоя 
--  для биллингового периода p_period_id
-- ВНИМАНИЕ!!! Расчет ведется после начисления абонплаты
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_Downtime( p_task_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Charge_Downtime';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем позиции компенсации начислений на абонплату за простои
    -- для счетов биллингового периода
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    INSERT INTO ITEM_T (
        BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
        ORDER_ID, ORDER_BODY_ID,
        SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
        ITEM_TOTAL, RECVD, DATE_FROM, DATE_TO, 
        ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
        REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, DESCR
    )
    WITH IDL AS (  
        SELECT Q.BILL_ID, Q.REP_PERIOD_ID, OB.SUBSERVICE_ID,
               DT.ORDER_ID, DT.ORDER_BODY_ID, 
               CASE
                 WHEN DT.VALUE < 0.5 THEN ROUND(DT.VALUE,1)
                 ELSE ROUND(DT.VALUE) 
               END HOURS, 
               OB.RATE_VALUE RATE_VALUE,
               CASE
                 WHEN OB.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX 
                   THEN ROUND((OB.RATE_VALUE * 28.6 * ROUND(DT.VALUE) / 720), 2)
                 ELSE   ROUND((OB.RATE_VALUE * ROUND(DT.VALUE) / 720), 2)
               END  DISC_IDL,
               DT.DATE_FROM, DT.DATE_TO
          FROM DOWNTIME_T DT, ORDER_BODY_T OB, BILLING_QUEUE_T Q 
         WHERE OB.CHARGE_TYPE IN (PK00_CONST.c_CHARGE_TYPE_REC, PK00_CONST.c_CHARGE_TYPE_MIN)
           AND OB.ORDER_ID     = DT.ORDER_ID   -- используем результаты BIND_DOWNTIME
           AND DT.STATUS       = c_DS_BIND_ORDER_BODY
           AND DT.FLAGS        = c_FLAG_H
           AND DT.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_IDL
           AND Q.ACCOUNT_ID    = DT.ACCOUNT_ID
           AND Q.REP_PERIOD_ID = DT.REP_PERIOD_ID
           AND Q.TASK_ID       = p_task_id
    ), I AS (
    SELECT I.BILL_ID, I.REP_PERIOD_ID, I.ITEM_ID, I.ITEM_TYPE, I.INV_ITEM_ID, I.ORDER_ID, 
           I.SERVICE_ID, I.SUBSERVICE_ID, I.CHARGE_TYPE, I.ITEM_TOTAL, I.RECVD, 
           I.DATE_FROM, I.DATE_TO, I.ITEM_STATUS, I.CREATE_DATE, I.LAST_MODIFIED, 
           I.REP_GROSS, I.REP_TAX, I.TAX_INCL, I.EXTERNAL_ID, I.NOTES
      FROM ITEM_T I, BILLING_QUEUE_T Q
     WHERE I.CHARGE_TYPE  IN (PK00_CONST.c_CHARGE_TYPE_REC, PK00_CONST.c_CHARGE_TYPE_MIN)
       AND I.ITEM_STATUS   = Pk00_Const.c_ITEM_STATE_OPEN
       AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID 
       AND Q.BILL_ID       = I.BILL_ID
       AND Q.TASK_ID       = p_task_id
    )
    SELECT I.BILL_ID, I.REP_PERIOD_ID, Pk02_Poid.Next_item_id ITEM_ID, 
           I.ITEM_TYPE, NULL INV_ITEM_ID, 
           I.ORDER_ID, IDL.ORDER_BODY_ID,
           I.SERVICE_ID, IDL.SUBSERVICE_ID, Pk00_Const.c_CHARGE_TYPE_IDL CHARGE_TYPE,
           CASE
            WHEN IDL.DISC_IDL <= I.ITEM_TOTAL THEN -IDL.DISC_IDL
            ELSE -I.ITEM_TOTAL
           END ITEM_TOTAL, 
           0 RECVD, 
           IDL.DATE_FROM, IDL.DATE_TO, I.ITEM_STATUS, 
           SYSDATE, SYSDATE, 
           0 REP_GROSS, 0 REP_TAX, I.TAX_INCL, NULL EXTERNAL_ID, NULL NOTES,
           '( Простои '||IDL.HOURS||' час.)' DESCR
      FROM IDL, I
     WHERE IDL.ORDER_ID      = I.ORDER_ID
       AND IDL.REP_PERIOD_ID = I.REP_PERIOD_ID
       AND IDL.BILL_ID       = I.BILL_ID
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- делаем отметку в записях
    MERGE INTO DOWNTIME_T DT
    USING ( 
        SELECT DT.DOWNTIME_ID, I.ITEM_ID, DT.REP_PERIOD_ID, DT.ORDER_BODY_ID 
          FROM BILLING_QUEUE_T Q, ITEM_T I, DOWNTIME_T DT 
         WHERE Q.TASK_ID        = p_task_id
           AND Q.REP_PERIOD_ID  = I.REP_PERIOD_ID
           AND Q.BILL_ID        = I.BILL_ID
           AND I.CHARGE_TYPE    = 'IDL'
           AND I.REP_PERIOD_ID  = DT.REP_PERIOD_ID
           AND I.ORDER_BODY_ID  = DT.ORDER_BODY_ID
           AND I.DATE_FROM      = DT.DATE_FROM
           AND I.DATE_TO        = DT.DATE_TO
           AND DT.STATUS        = c_DS_BIND_ORDER_BODY
    ) DI
    ON (
        DT.DOWNTIME_ID = DI.DOWNTIME_ID 
    )
    WHEN MATCHED THEN UPDATE SET DT.ITEM_ID = DI.ITEM_ID, 
                                 DT.STATUS = c_DS_OK;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('DOWNTIME_T: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расчет компенсации абонплаты на время простоя, заданного в часах,
-- с указанием SLA
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_SLA_H( p_task_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Charge_Downtime';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем позиции компенсации начислений на абонплату за простои
    --  с указанием SLA для счетов биллингового периода
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    INSERT INTO ITEM_T (
        BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
        ORDER_ID, ORDER_BODY_ID,
        SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
        ITEM_TOTAL, RECVD, DATE_FROM, DATE_TO, 
        ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
        REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, DESCR
    )
    WITH SLA AS (
        SELECT Q.BILL_ID, Q.REP_PERIOD_ID, 
               DT.ORDER_ID, DT.ORDER_BODY_ID,
               ROUND(DT.VALUE) HOURS,
               DT.DATE_FROM, DT.DATE_TO,
               SP.SLA_PERCENT
          FROM DOWNTIME_T DT, SLA_PERCENT_T SP,
               BILLING_QUEUE_T Q, PERIOD_T P,
               ORDER_BODY_T OB
         WHERE DT.STATUS       = c_DS_BIND_ORDER_BODY
           AND DT.ORDER_BODY_ID= OB.ORDER_BODY_ID
           AND SP.RATEPLAN_ID  = OB.RATEPLAN_ID   -- используем результаты BIND_DOWNTIME
           AND (100 * (1 - (ROUND(DT.VALUE)/(24 * (P.PERIOD_TO-P.PERIOD_FROM+1/86400) )))) 
               BETWEEN SP.K_MIN AND SP.K_MAX
           AND DT.FLAGS        = c_FLAG_H
           AND DT.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_SLA
           AND Q.ACCOUNT_ID    = DT.ACCOUNT_ID
           AND Q.REP_PERIOD_ID = DT.REP_PERIOD_ID
           AND Q.TASK_ID       = p_task_id
           AND P.PERIOD_ID     = NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID)
    ), I AS (
    SELECT I.BILL_ID, I.REP_PERIOD_ID, I.ITEM_ID, I.ITEM_TYPE, I.INV_ITEM_ID, I.ORDER_ID, 
           I.SERVICE_ID, I.SUBSERVICE_ID, I.CHARGE_TYPE, I.ITEM_TOTAL, I.RECVD, 
           I.DATE_FROM, I.DATE_TO, I.ITEM_STATUS, I.CREATE_DATE, I.LAST_MODIFIED, 
           I.REP_GROSS, I.REP_TAX, I.TAX_INCL, I.EXTERNAL_ID, I.NOTES
      FROM ITEM_T I, BILLING_QUEUE_T Q
     WHERE I.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_REC
       AND I.ITEM_STATUS   = Pk00_Const.c_ITEM_STATE_REАDY
       AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID 
       AND Q.BILL_ID       = I.BILL_ID
       AND Q.TASK_ID       = p_task_id
    )
    SELECT I.BILL_ID, I.REP_PERIOD_ID, Pk02_Poid.Next_item_id ITEM_ID, 
           I.ITEM_TYPE, NULL INV_ITEM_ID, 
           I.ORDER_ID, SLA.ORDER_BODY_ID,
           I.SERVICE_ID, Pk00_Const.c_SUBSRV_IDL SUBSERVICE_ID, 
           Pk00_Const.c_CHARGE_TYPE_SLA CHARGE_TYPE,
           (I.ITEM_TOTAL * SLA_PERCENT) ITEM_TOTAL,
           0 RECVD, 
           SLA.DATE_FROM, SLA.DATE_TO, I.ITEM_STATUS, 
           SYSDATE, SYSDATE, 
           0 REP_GROSS, 0 REP_TAX, I.TAX_INCL, NULL EXTERNAL_ID, NULL NOTES,
           '('||SLA.HOURS||' час.)' DESCR
      FROM SLA, I
     WHERE SLA.ORDER_ID      = I.ORDER_ID
       AND SLA.REP_PERIOD_ID = I.REP_PERIOD_ID
       AND SLA.BILL_ID       = I.BILL_ID
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расчет компенсации абонплаты на время простоя, 
-- заданного коэффициентом доступности (К),
-- с указанием SLA
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_SLA_K( p_task_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Charge_Downtime';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем позиции компенсации начислений на абонплату за простои
    --  с указанием SLA для счетов биллингового периода
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    INSERT INTO ITEM_T (
        BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
        ORDER_ID, ORDER_BODY_ID,
        SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
        ITEM_TOTAL, RECVD, DATE_FROM, DATE_TO, 
        ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
        REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, DESCR
    )
    WITH SLA AS (
        SELECT Q.BILL_ID, Q.REP_PERIOD_ID,
               DT.ORDER_ID, DT.ORDER_BODY_ID, DT.VALUE K_SLA,
               DT.DATE_FROM, DT.DATE_TO,
               P.SLA_PERCENT
          FROM DOWNTIME_T DT, SLA_PERCENT_T P, BILLING_QUEUE_T Q, ORDER_BODY_T OB
         WHERE DT.ORDER_BODY_ID = OB.ORDER_BODY_ID
           AND P.RATEPLAN_ID   = OB.RATEPLAN_ID   -- используем результаты BIND_DOWNTIME
           AND DT.STATUS       = c_DS_BIND_ORDER_BODY
           AND DT.VALUE BETWEEN P.K_MIN AND P.K_MAX
           AND DT.FLAGS        = c_FLAG_K
           AND DT.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_SLA
           AND DT.REP_PERIOD_ID= NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID)
           AND DT.ACCOUNT_ID   = Q.ACCOUNT_ID
           AND Q.TASK_ID       = p_task_id
    ), I AS (
    SELECT I.BILL_ID, I.REP_PERIOD_ID, I.ITEM_ID, I.ITEM_TYPE, I.INV_ITEM_ID, I.ORDER_ID, 
           I.SERVICE_ID, I.SUBSERVICE_ID, I.CHARGE_TYPE, I.ITEM_TOTAL, I.RECVD, 
           I.DATE_FROM, I.DATE_TO, I.ITEM_STATUS, I.CREATE_DATE, I.LAST_MODIFIED, 
           I.REP_GROSS, I.REP_TAX, I.TAX_INCL, I.EXTERNAL_ID, I.NOTES
      FROM ITEM_T I, BILLING_QUEUE_T Q
     WHERE I.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_REC
       AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_REАDY
       AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID 
       AND Q.BILL_ID       = I.BILL_ID
       AND Q.TASK_ID       = p_task_id
    )
    SELECT I.BILL_ID, I.REP_PERIOD_ID, Pk02_Poid.Next_item_id ITEM_ID, 
           I.ITEM_TYPE, NULL INV_ITEM_ID, 
           I.ORDER_ID, SLA.ORDER_BODY_ID,
           I.SERVICE_ID, Pk00_Const.c_SUBSRV_IDL SUBSERVICE_ID, 
           Pk00_Const.c_CHARGE_TYPE_SLA CHARGE_TYPE,
           (I.ITEM_TOTAL * K_SLA) ITEM_TOTAL,
           0 RECVD, 
           SLA.DATE_FROM, SLA.DATE_TO, I.ITEM_STATUS, 
           SYSDATE, SYSDATE, 
           0 REP_GROSS, 0 REP_TAX, I.TAX_INCL, NULL EXTERNAL_ID, NULL NOTES,
           '( K = '||SLA.K_SLA||' )' DESCR
      FROM SLA, I
     WHERE SLA.ORDER_ID      = I.ORDER_ID
       AND SLA.REP_PERIOD_ID = I.REP_PERIOD_ID
       AND SLA.BILL_ID       = I.BILL_ID
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- ---------------------------------------------------------------------- --
-- Экспорт данных в PORTAL 6.5 для В. Малиновского 
-- ---------------------------------------------------------------------- --
PROCEDURE Export_to_Portal( 
               p_recordset    OUT t_refc, 
               p_bill_id       IN INTEGER,   -- ID позиции счета
               p_rep_period_id IN INTEGER    -- ID периода счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_to_Portal';
    v_retcode    INTEGER;
BEGIN
    -- вычисляем кол-во записей
    SELECT COUNT(*) INTO v_retcode
      FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- возвращаем курсор
    OPEN p_recordset FOR
         SELECT ITEM_ID, ITEM_TYPE, BILL_ID, 
                ORDER_ID, SERVICE_ID, CHARGE_TYPE,  
                ITEM_TOTAL, RECVD,  
                DATE_FROM, DATE_TO, INV_ITEM_ID, ITEM_STATUS
           FROM ITEM_T
          WHERE BILL_ID = p_bill_id
            AND REP_PERIOD_ID = p_rep_period_id
          ORDER BY ITEM_ID;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ---------------------------------------------------------------------- --
-- Импорт данных SLA из PORTAL 6.5 (для тестов)
-- в рабочем режиме таблицу заполняет ДРУ (Никитина)
-- ---------------------------------------------------------------------- --
PROCEDURE Import_SLA_data
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Import_SLA_data';
    v_count         INTEGER;
BEGIN
    -- -------------------------------------------------------------------------- -- 
    -- регистрируем тарифные планы компенсации SLA 
    INSERT INTO RATEPLAN_T (
        RATEPLAN_ID, RATEPLAN_NAME, RATESYSTEM_ID, RATEPLAN_CODE, TAX_INCL, CURRENCY_ID, NOTE
    )
    SELECT SQ_RATEPLAN_ID.NEXTVAL RATEPLAN_ID, 'SLA-'||obj_id0 RATEPLAN_NAME, 
           1208 RATESYSTEM_ID, obj_id0 RATEPLAN_CODE, 'N' TAX_INCL, 810 CURRENCY_ID,
           'Компенсация простоев по SLA'
    FROM (
        SELECT distinct a.obj_id0
          FROM sla_services_t@pindb a, service_t@pindb s
          where s.poid_id0 = a.service_obj_id0
            AND NOT EXISTS (
                SELECT * FROM RATEPLAN_T P
                 WHERE P.RATEPLAN_CODE = A.OBJ_ID0
                   AND P.RATESYSTEM_ID = 1208
            )
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('RATEPLAN_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- -------------------------------------------------------------------------- -- 
    -- создаем таблицы тарифных планов компенсации SLA
    v_count := 0;
    --
    FOR c_sla IN (
        SELECT P.RATEPLAN_ID, P.RATEPLAN_CODE 
          FROM RATEPLAN_T P
         WHERE RATESYSTEM_ID = 1208
           AND NOT EXISTS (
              SELECT * FROM SLA_PERCENT_T SP
               WHERE SP.RATEPLAN_ID = P.RATEPLAN_ID
           )
    )
    LOOP
        INSERT INTO SLA_PERCENT_T(RATEPLAN_ID, REC_ID, SLA_PERCENT, K_MIN, K_MAX)
        SELECT P.RATEPLAN_ID,
               a.rec_id REC_ID,
               a.percent SLA_PERCENT, 
               round(a.step_min,3)  K_MIN,
               round(a.step_max,3)  K_MAX 
          FROM percents_t@pindb a, RATEPLAN_T P
         WHERE P.RATEPLAN_CODE = a.obj_id0
           AND P.RATESYSTEM_ID = 1208
           AND P.RATEPLAN_CODE = c_sla.RATEPLAN_CODE
           AND P.RATEPLAN_ID   = c_sla.RATEPLAN_ID;
        v_count := v_count + 1;
    END LOOP; 
    --
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- -------------------------------------------------------------------------- -- 
    --проставляем номера тарифных планов в заказах
    MERGE INTO ORDER_BODY_T OB
    USING (
        SELECT a.obj_id0 RATEPLAN_CODE, s.login ORDER_NO, R.RATEPLAN_ID, O.ORDER_ID, OB.ORDER_BODY_ID
          FROM sla_services_t@pindb a, service_t@pindb s, 
               RATEPLAN_T R, ORDER_T O, ORDER_BODY_T OB
          where s.poid_id0 = a.service_obj_id0
            AND R.RATEPLAN_CODE = TO_CHAR(a.obj_id0)
            AND R.RATESYSTEM_ID = Pk00_Const.с_RATESYS_SLA_ID -- 1208 
            AND O.ORDER_NO = s.login
            AND O.ORDER_ID = OB.ORDER_ID
            AND OB.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_IDL, Pk00_Const.c_CHARGE_TYPE_SLA)
            AND OB.RATEPLAN_ID IS NULL
    ) SL 
    ON (
        OB.ORDER_BODY_ID = SL.ORDER_BODY_ID
    )
    WHEN MATCHED THEN UPDATE SET OB.RATEPLAN_ID = SL.RATEPLAN_ID, 
                                 OB.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_SLA;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
--              Р А Б О Т А    Н А Д    О Ш И Б К А М И                  --
-- --------------------------------------------------------------------- --
--
-- Расформирование компенсаций за простои
--
PROCEDURE Rollback_downtimes( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_downtimes';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    DELETE FROM ITEM_T I
     WHERE I.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_IDL, 
                             Pk00_Const.c_CHARGE_TYPE_SLA)
       AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
       AND I.EXTERNAL_ID IS NULL -- на всякий случай
       AND EXISTS (
        SELECT * FROM BILLING_QUEUE_T Q
         WHERE Q.BILL_ID       = I.BILL_ID
           AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID
           AND Q.TASK_ID       = p_task_id
    );
    v_count := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg('Stop. '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--
-- Перефомирование компенсаций за простои
--
PROCEDURE Recharge_downtimes( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Recharge_downtimes';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- 4) производим компенсацию простое
    -- Расчет компенсации абонплаты на время простоя, заданного в часах,
    Charge_Downtime( p_task_id );
       
    -- Расчет компенсации абонплаты на время простоя, заданного в часах,
    -- с указанием SLA
    Charge_SLA_H( p_task_id );

    -- Расчет компенсации абонплаты на время простоя, 
    -- заданного коэффициентом доступности (К),
    -- с указанием SLA
    Charge_SLA_K( p_task_id );
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Экспорт данных в систему подготовки данных по простоям и SLA
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Export_to_pindb( 
               p_recordset  IN OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_to_pindb';
    v_retcode    INTEGER;
BEGIN
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



/************************************************************************************
1. Простои.

- Все простои в ДРУ собираются в Access базу и из нее загружаются в АСР.
Вот пример этой базы

Перед записью в базу все простои агрегируются по заказам за месяц.
В БД поля

ID (чтобы 2 раза не грузить, присваивают в ДРУ при создании записи)
номер заказа
дата начала периода
дата окончания периода
простой в часах (с десятыми и сотыми, а не с минутами)
flags    - показывает в чем прстой, в часах или К.доступности. О нем ниже ...

- Простои из БД опкодом грузятся в АСР. Есть спец АРМ для такой загрузки.
	в опкоде :
- по номеру заказа определяется, нет ли этого заказа в таблицах SLA. Если нет, то это обычный простой.
Тогда: 

- по номеру заказа находится его абонплата (для каналов) или
	минималка (для VPN и интернет).
- длительность простоя округляется до целых часов (причем целые полчаса округляются в меньшую сторону)
- абонплата (или минималка) делится на 720 (среднее число часов в месяце) и умножается на часы простоя (целые).
	получаем вычет за простой.


Вычет за прстой загружается в АСР со знаком минус, создается запись с доп. информацией. Например:
cycle_part		Downtime (без округления)      Percent		Flag
17.080555555555555	0.699999999999999		0		NULL
Эта информация для деталировки.

Для случая SLA Percent и Flag имеют другие значения.

Есть договор исключение (со Сбербанком) у которого длительность простоя не округляется.
а стоимость простоя вычисляется по неокругленной длительности.

2. SLA.
Если заказ опкодом найден в списке заказов SLA. То возвращаемая часть абонплаты или минималки вычисляется по-другому.
В этом случае по длительности простоя вычисляется коэффициент доступности.
коэф. доступности вычисляется по формуле
(100 * (1 - (часы простоя / (24 * дней в месяце)))
полученный результат находится в пределах от 0 до 1.
В таблице SLA для каждого заказа вычисляется (введенные заранее параметры) пороги скидки в процентах
за определенный коэффициент доступности. Пример :

Процент скидки	Макс К			Мин К
			
1 			99.69                                    	99.65
3 			99.64                                    	99.6 
5 			99.59                                    	99.55
7 			99.54                                    	99.5 
10			99.489			0   .

При вычислении интревала, в котрый попал К доступности определяем процент.
- Вычисляем абонплату или минималку и делаем вычет на величину полученного процента от них.

В этом случае в АСР грузится в поле Percent - величина вычисленного процента скидки.

Есть случай когда в БД Аccess записан уже К.Доступности. И в поле flags стоит 1.
Тогда К доступности не вычисляем, как в предыдущем случае, а сразу
ищем интервал длоя определения процента скидки.


SLA работает именно на заказ. Т.к. не все заказы в договоре могут иметь соглашение с клиентом на SLA.
Но. Чащще всего вносятся по списку заказов и т.д. Удобство ведения и конструкцию этой базы (заказы, пороги и т.д.)
нужно бы продумать отдельно. может и не так, как сделано сейчас.
************************************************************************************/


/***********************************************************************************
Экспорт порогов из СТАРОГО БИЛЛИНГА

1. Все заказы на котрые подписан SLA

SELECT a.obj_id0, s.login
  FROM sla_services_t a, service_t s 
  where s.poid_id0 = a.service_obj_id0

Здесь a.obj_id0 - id связки на таблицу с порогами
процентами по данному заказу.
s.login - № Заказа.

- Связь может быть многие ко многим, т.е. на набор порогом может быть
несколько заказов.


1. Пороги-проценты

SELECT a.obj_id0, a.rec_id, a.percent, round(a.step_max,3), round(a.step_min,3)
  FROM percents_t a 

Здесь a.obj_id0 - id связки на таблицу с номерами заказов
a.rec_id  - id массива
a.percent - % скидки
round(a.step_max,3) - верхнее значение интервала
round(a.step_min,3) - нижнее значение интервала

С уважением, К.Федоров
************************************************************************************/

END PK38_BILLING_DOWNTIME;
/
