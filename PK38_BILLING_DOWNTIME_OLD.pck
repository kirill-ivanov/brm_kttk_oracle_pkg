CREATE OR REPLACE PACKAGE PK38_BILLING_DOWNTIME_OLD
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
    c_DS_DISABLE         CONSTANT INTEGER := -5;  -- компенсация простоев для л/с запрещена
    c_DS_NULL            CONSTANT INTEGER := -4;  -- простой нулевой
    c_DS_ERROR           CONSTANT INTEGER := -3;  -- прочие ошибки
    c_DS_BIND_OB_ER      CONSTANT INTEGER := -2;  -- ошибка привязки к компоненте услуги заказа
    c_DS_BIND_O_ER       CONSTANT INTEGER := -1;  -- ошибка привязки к заказу
    c_DS_BIND_ORDER      CONSTANT INTEGER :=  0;  -- данные о простое привязаны к заказу
    c_DS_BIND_ORDER_BODY CONSTANT INTEGER :=  1;  -- данные о простое привязаны к компоненте услуги заказа
    c_DS_OK              CONSTANT INTEGER :=  2;  -- начисления проведены

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Включение в счета периода компенсации за простои 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Period_processing( p_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Включение в счета компенсаций за простои для указанной задачи
    PROCEDURE Task_processing( p_task_id IN INTEGER );

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

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создать очередь, для обработки указанного периода 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Make_period_queue( p_period_id IN INTEGER ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Досоздаем стандартные компоненты услуги для компенсации простоев, 
    -- где это необходимо 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_order_body( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Привязать информацию о простоях к данным очереди 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Bind_data_queue( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расчет скидки для абонплаты на время простоя 
    -- для биллингового периода p_period_id
    -- ВНИМАНИЕ!!! Расчет ведется после начисления абонплаты, в валюте тарифа
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Charge_Downtime( p_task_id IN INTEGER );
    
    -- ---------------------------------------------------------------------- --
    -- Удаляем нулевые простои из начислений
    -- ---------------------------------------------------------------------- --
    PROCEDURE Delete_empty_items( p_task_id IN INTEGER );
  
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
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассчитатать и применить компенсацию за простои для Дебет-ноты
    -- Дебет-нота должна быть в состоянии: 'OPEN'
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Recharge_downtime_for_debet( 
                 p_dbt_bill_id    IN INTEGER,  -- id - дебетового счета, в котором учтется скидка
                 p_dbt_period_id  IN INTEGER,  -- период дебетового счета
                 p_crd_period_id  IN INTEGER   -- период кредитового счета, для которого расчитывается скидка
              );
    
END PK38_BILLING_DOWNTIME_OLD;
/
CREATE OR REPLACE PACKAGE BODY PK38_BILLING_DOWNTIME_OLD
IS

-- ---------------------------------------------------------------------- --
-- Включение в счета периода компенсации за простои 
-- ---------------------------------------------------------------------- --
PROCEDURE Period_processing( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Downtime_processing';
    v_task_id    INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1) Создать очередь, для обработки указанного периода 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_task_id := Make_period_queue( p_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2) Обработать очередь 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Task_processing( v_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3) Чистим очередь задачи
    Pk30_Billing_Queue.Close_task( v_task_id );
    
EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------- --
-- Включение в счета компенсаций за простои 
-- ---------------------------------------------------------------------- --
PROCEDURE Task_processing( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Task_processing';
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id='||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1) Досоздаем стандартные компоненты услуги для компенсации простоев, где это необходимо 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Make_order_body( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2) Привязать информацию о простоях к данным очереди 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Bind_data_queue( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3) расформировываем счета
    Pk30_Billing_Base.Rollback_bills(p_task_id);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- 4) производим компенсацию простоев
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Charge_Downtime( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 5) Удаляем нулевые простои из начислений
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Delete_empty_items( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 6) формируем счета
    Pk30_Billing_Base.Make_bills( p_task_id );

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
           p_date_from, p_date_to, p_value, p_flag, NULL STATUS, 
           SYSDATE     
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

-- ---------------------------------------------------------------------- --
-- Создать очередь, для обработки указанного периода 
-- ---------------------------------------------------------------------- --
FUNCTION Make_period_queue( p_period_id IN INTEGER ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_period_queue';
    v_task_id    INTEGER;
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period='||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
   
    v_task_id := PK30_BILLING_QUEUE.Open_task;
    
    -- заполняем очередь с заказами
    INSERT INTO BILLING_QUEUE_T( BILL_ID, ACCOUNT_ID, ORDER_ID, TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID )
    WITH D AS (
        SELECT DISTINCT O.ACCOUNT_ID 
          FROM DOWNTIME_T D, ORDER_T O, PERIOD_T P
         WHERE P.PERIOD_ID    = p_period_id
           AND D.DATE_FROM   <= P.PERIOD_TO  -- разрешаем ставить в очередь простои прошедших периодов
           --AND P.PERIOD_FROM <= D.DATE_TO 
           AND O.ORDER_NO     = D.ORDER_NO
           --AND D.CREATE_DATE  > P.PERIOD_TO -- чтобы случайно не зацепить ранее созданные  записи 
           AND D.STATUS IS NULL
    )
    SELECT B.BILL_ID, B.ACCOUNT_ID, NULL ORDER_ID, v_task_id TASK_ID, 
           B.REP_PERIOD_ID, B.REP_PERIOD_ID
      FROM D, BILL_T B 
     WHERE B.REP_PERIOD_ID = p_period_id
       AND D.ACCOUNT_ID    = B.ACCOUNT_ID
       AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_REC;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    RETURN v_task_id;
    --
EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Досоздаем стандартные компоненты услуги для компенсации простоев, 
-- где это необходимо 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Make_order_body( p_task_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Make_order_body';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- корректирум даты закрытия IDL, на всякий случай
    UPDATE ORDER_BODY_T OB
       SET OB.DATE_TO = (
           SELECT O.DATE_TO
             FROM ORDER_T O
            WHERE O.ORDER_ID = OB.ORDER_ID
       )
     WHERE OB.CHARGE_TYPE = 'IDL'
       AND OB.DATE_TO != TO_DATE('01.01.2050','dd.mm.yyyy')
       AND EXISTS (
           SELECT * 
             FROM ORDER_T O
            WHERE O.ORDER_ID = OB.ORDER_ID
              AND O.DATE_TO != OB.DATE_TO 
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T.DATE_TO: '||v_count||' corrected', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

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
    SELECT 
           SQ_ORDER_ID.NEXTVAL ORDER_BODY_ID, 
           ORDER_ID,
           Pk00_Const.c_SUBSRV_IDL SUBSERVICE_ID,       -- c_SUBSRV_IDL    CONSTANT integer := 36;  -- Компенсация простоев
           Pk00_Const.c_CHARGE_TYPE_IDL CHARGE_TYPE,    -- c_CHARGE_TYPE_IDL := 'IDL'
           DATE_FROM, 
           DATE_TO,
           c_DEFAULT_FREE_DOWNTIME FREE_VALUE,          -- 43
           Pk00_Const.c_RATE_RULE_IDL_STD RATE_RULE_ID, -- c_RATE_RULE_IDL_STD    CONSTANT INTEGER := 2404; -- Компенсация простоев, стандартная схема  
           Pk00_Const.c_RATE_LEVEL_ORDER RATE_LAVEL_ID, -- c_RATE_LEVEL_ORDER     CONSTANT INTEGER := 2302; -- Тариф указан на заказ
           TAX_INCL,
           1 QUANTITY,
           SYSDATE CREATE_DATE,
           SYSDATE MODIFY_DATE,
           CURRENCY_ID,
           'Создано автоматически при расчете компенсации за простои' NOTES
      FROM (
          SELECT ROW_NUMBER() OVER (PARTITION BY O.ORDER_ID ORDER BY OB.DATE_FROM) RN,
                 OB.ORDER_ID,
                 OB.DATE_FROM, 
                 Pk00_Const.c_DATE_MAX DATE_TO,
                 OB.TAX_INCL,
                 OB.CURRENCY_ID
            FROM BILLING_QUEUE_T Q, ORDER_T O, ORDER_BODY_T OB, PERIOD_T P, DOWNTIME_T D 
           WHERE Q.TASK_ID    = p_task_id
             AND Q.DATA_PERIOD_ID  = P.PERIOD_ID
             AND Q.ACCOUNT_ID = O.ACCOUNT_ID
             AND O.ORDER_NO   = D.ORDER_NO
             AND O.ORDER_ID   = OB.ORDER_ID
             AND OB.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_REC,
                                    Pk00_Const.c_CHARGE_TYPE_MIN)
             AND OB.DATE_FROM <= D.DATE_TO
             AND (OB.DATE_TO IS NULL OR D.DATE_FROM <= OB.DATE_TO)
             AND NOT EXISTS (
                SELECT * FROM ORDER_BODY_T IDL
                 WHERE IDL.ORDER_ID = O.ORDER_ID
                   AND IDL.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_IDL,
                                          Pk00_Const.c_CHARGE_TYPE_SLA)
             )
       )
     WHERE RN = 1
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T: '||v_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------- --
-- Привязать информацию о простоях к данным очереди 
-- ---------------------------------------------------------------------- --
PROCEDURE Bind_data_queue( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bind_data_queue';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id='||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- обогащаем DOWNTIME_T
    MERGE INTO DOWNTIME_T D
    USING (
      SELECT * FROM (
        SELECT D.DOWNTIME_ID, Q.ACCOUNT_ID, O.ORDER_ID, OB.ORDER_BODY_ID, OB.CHARGE_TYPE, 
               ROW_NUMBER() OVER (PARTITION BY D.DOWNTIME_ID ORDER BY OB.DATE_FROM) RN 
          FROM BILLING_QUEUE_T Q, 
               ORDER_T O, ORDER_BODY_T OB, 
               PERIOD_T P, DOWNTIME_T D 
         WHERE Q.TASK_ID        = p_task_id
           AND Q.DATA_PERIOD_ID = P.PERIOD_ID
           AND Q.ACCOUNT_ID     = O.ACCOUNT_ID
           AND O.ORDER_NO       = D.ORDER_NO
           AND O.ORDER_ID       = OB.ORDER_ID
           AND OB.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_IDL,
                                  Pk00_Const.c_CHARGE_TYPE_SLA)
           --AND OB.DATE_FROM    <= P.PERIOD_TO  
           --AND (OB.DATE_TO IS NULL OR P.PERIOD_FROM <= OB.DATE_TO)
           AND OB.DATE_FROM    <= D.DATE_TO  
           AND (OB.DATE_TO IS NULL OR D.DATE_FROM <= OB.DATE_TO)
           AND D.DATE_FROM     <= P.PERIOD_TO
           AND D.STATUS         IS NULL        -- берем все записи с пустым статусом
           --AND D.CREATE_DATE    > P.PERIOD_TO  -- чтобы случайно не зацепить ранее созданные  записи
      )
      WHERE RN = 1
    ) Q
    ON (
        D.DOWNTIME_ID = Q.DOWNTIME_ID
    )
    WHEN MATCHED THEN UPDATE SET D.ACCOUNT_ID    = Q.ACCOUNT_ID, 
                                 D.ORDER_ID      = Q.ORDER_ID, 
                                 D.ORDER_BODY_ID = Q.ORDER_BODY_ID, 
                                 D.CHARGE_TYPE   = Q.CHARGE_TYPE, 
                                 D.STATUS        = c_DS_BIND_ORDER_BODY, 
                                 D.LAST_MODIFIED = SYSDATE
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('DOWNTIME_T = '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
  
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- фиксируем ошибки
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE DOWNTIME_T D 
       SET D.NOTES  = 'компенсация простоев запрещена условиями договора',
           D.STATUS = c_DS_DISABLE
     WHERE NOT EXISTS (
           SELECT * 
             FROM ACCOUNT_T A
            WHERE A.ACCOUNT_ID = D.ACCOUNT_ID
              AND A.IDL_ENB = 'Y' 
       )
       AND EXISTS (
           SELECT * FROM BILLING_QUEUE_T Q
            WHERE Q.ACCOUNT_ID = D.ACCOUNT_ID
              AND Q.TASK_ID = p_task_id
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('DOWNTIME_T = '||v_count||' rows ACCOUNT_T.IDL_ENB=N', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    UPDATE DOWNTIME_T D 
       SET D.NOTES  = 'в заказе отсутсвует компонента услуги REC/MIN',
           D.STATUS = c_DS_BIND_OB_ER
     WHERE D.STATUS IS NULL
       AND NOT EXISTS (
           SELECT * 
             FROM ORDER_T O, ORDER_BODY_T OB
            WHERE OB.ORDER_ID = O.ORDER_ID 
              AND OB.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_MIN,
                                     Pk00_Const.c_CHARGE_TYPE_REC)
              AND D.ORDER_NO = O.ORDER_NO
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('DOWNTIME_T.OB_ID = '||v_count||' rows OB.MIN/REC not found', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расчет скидки для абонплаты на время простоя 
-- для биллингового периода p_period_id
-- ВНИМАНИЕ!!! Расчет ведется после начисления абонплаты, в валюте тарифа
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_Downtime_old( p_task_id IN INTEGER )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Charge_Downtime';
    v_count       INTEGER;
    v_item_id     INTEGER;
    v_count_idl   INTEGER := 0;
    v_count_sla_h INTEGER := 0;
    v_count_sla_k INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
   
    -- Досоздаем стандартные компоненты услуги для компенсации простоев, где это необходимо 
    Make_order_body( p_task_id );

    -- Привязать информацию о простоях к данным очереди 
    Bind_data_queue( p_task_id );
    
    FOR idl IN (
      SELECT BILL_ID, REP_PERIOD_ID, ACCOUNT_ID, 
             PERIOD_FROM, PERIOD_TO, ORDER_ID, 
             ORDER_BODY_ID, CHARGE_TYPE, FLAGS, 
             SUBSERVICE_ID, RATE_RULE_ID, RATEPLAN_ID, 
             --CASE
             --  WHEN IDL_VALUE < 0.5 THEN ROUND(IDL_VALUE,1)
             --  ELSE ROUND(IDL_VALUE) 
             --END IDL_VALUE,
             ROUND(IDL_VALUE) IDL_VALUE,
             DATE_FROM, DATE_TO
        FROM (             
          SELECT Q.BILL_ID, Q.REP_PERIOD_ID, Q.ACCOUNT_ID, 
                 P.PERIOD_FROM, P.PERIOD_TO, D.ORDER_ID, 
                 D.ORDER_BODY_ID, D.CHARGE_TYPE, D.FLAGS, 
                 OB.SUBSERVICE_ID, OB.RATE_RULE_ID, OB.RATEPLAN_ID, 
                 SUM(D.VALUE) IDL_VALUE, 
                 MIN(D.DATE_FROM) DATE_FROM, MAX(D.DATE_TO) DATE_TO
            FROM DOWNTIME_T D, BILLING_QUEUE_T Q, PERIOD_T P, ORDER_BODY_T OB
           WHERE Q.TASK_ID        = p_task_id
             AND D.ACCOUNT_ID     = Q.ACCOUNT_ID 
             AND Q.DATA_PERIOD_ID = P.PERIOD_ID
             AND (Q.ORDER_ID IS NULL OR Q.ORDER_ID = D.ORDER_ID)
             AND OB.ORDER_BODY_ID = D.ORDER_BODY_ID
             AND D.STATUS         = c_DS_BIND_ORDER_BODY
             --AND D.CREATE_DATE    > P.PERIOD_TO  -- чтобы случайно не зацепить ранее созданные  записи
           GROUP BY Q.BILL_ID, Q.REP_PERIOD_ID, Q.ACCOUNT_ID, 
                 P.PERIOD_FROM, P.PERIOD_TO, D.ORDER_ID, 
                 D.ORDER_BODY_ID, D.CHARGE_TYPE, D.FLAGS,
                 OB.SUBSERVICE_ID, OB.RATE_RULE_ID, OB.RATEPLAN_ID  
          ) 
    )
    LOOP
      -- если будет сормировано более одной позиции компенсации, 
      -- возникнет исключение, будем думать как быть дальше (например sum(item))
      v_item_id := Pk02_Poid.Next_item_id;
      
      IF idl.charge_type = Pk00_Const.c_CHARGE_TYPE_IDL THEN
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- только компенсация простоев
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- формируем item для компенсации простоев
        INSERT INTO ITEM_T (
            BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
            ORDER_ID, ORDER_BODY_ID,
            SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
            ITEM_TOTAL, ITEM_CURRENCY_ID, RECVD, DATE_FROM, DATE_TO, 
            ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
            REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, DESCR
        )
        SELECT 
            BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
            ORDER_ID, ORDER_BODY_ID,
            SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
            CASE
              WHEN DISC_IDL <= ITEM_TOTAL THEN -DISC_IDL
              ELSE -ITEM_TOTAL
            END ITEM_TOTAL,
            ITEM_CURRENCY_ID, RECVD, DATE_FROM, DATE_TO, 
            ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
            REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, DESCR
        FROM (
          SELECT 
              idl.bill_id, 
              idl.rep_period_id, 
              v_item_id ITEM_ID, 
              Pk00_Const.c_ITEM_TYPE_BILL ITEM_TYPE, 
              NULL INV_ITEM_ID, 
              idl.Order_Id, 
              idl.Order_Body_Id,
              SERVICE_ID, 
              Pk00_Const.c_SUBSRV_IDL SUBSERVICE_ID, 
              Pk00_Const.c_CHARGE_TYPE_IDL CHARGE_TYPE,
              DISC_IDL,
              ITEM_TOTAL,
              ITEM_CURRENCY_ID,
              0 RECVD, 
              idl.DATE_FROM, idl.DATE_TO, 
              ITEM_STATUS, 
              SYSDATE CREATE_DATE, 
              SYSDATE LAST_MODIFIED, 
              0 REP_GROSS, 0 REP_TAX, 
              TAX_INCL, 
              NULL EXTERNAL_ID, 
              '( Простои '||idl.IDL_VALUE||' час.)'  NOTES,
              NULL DESCR
            FROM (
              -- если позиций фиксированных оплат больше одной суммируем их             
              SELECT 
                  I.SERVICE_ID,
                  SUM((OB.RATE_VALUE * idl.IDL_VALUE / 720)) DISC_IDL,
                  SUM(I.ITEM_TOTAL) ITEM_TOTAL,
                  I.ITEM_CURRENCY_ID,
                  I.ITEM_STATUS, I.TAX_INCL
                FROM ITEM_T I, ORDER_BODY_T OB 
               WHERE I.BILL_ID       = idl.bill_id
                 AND I.REP_PERIOD_ID = idl.rep_period_id
                 AND I.ORDER_ID      = idl.Order_Id
                 AND I.DATE_FROM    <= idl.Date_To
                 AND I.DATE_TO      >= idl.Date_From
                 AND I.CHARGE_TYPE IN (PK00_CONST.c_CHARGE_TYPE_REC, 
                                       PK00_CONST.c_CHARGE_TYPE_MIN)
                 AND I.ITEM_TYPE     = Pk00_Const.c_ITEM_TYPE_BILL
                 AND I.ORDER_BODY_ID = OB.ORDER_BODY_ID 
               GROUP BY   
                  I.SERVICE_ID,
                  I.ITEM_CURRENCY_ID,
                  I.ITEM_STATUS, I.TAX_INCL 
            ) 
        );
        v_count := SQL%ROWCOUNT;
        v_count_idl := v_count_idl + 1;
        --
      ELSIF idl.charge_type = Pk00_Const.c_CHARGE_TYPE_SLA THEN
        IF idl.flags = c_FLAG_H THEN
          -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
          -- Расчет компенсации абонплаты на время простоя, заданного в часах,
          -- с указанием SLA
          -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
          INSERT INTO ITEM_T (
              BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
              ORDER_ID, ORDER_BODY_ID,
              SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
              ITEM_TOTAL, ITEM_CURRENCY_ID, RECVD, DATE_FROM, DATE_TO, 
              ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
              REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, DESCR
          )
          WITH SLA AS (
              SELECT idl.BILL_ID, idl.REP_PERIOD_ID, 
                     idl.ORDER_ID, idl.ORDER_BODY_ID,
                     idl.IDL_VALUE HOURS,
                     idl.DATE_FROM, idl.DATE_TO,
                     SP.SLA_PERCENT
                FROM SLA_PERCENT_T SP
               WHERE SP.RATEPLAN_ID  = idl.RATEPLAN_ID   -- используем результаты BIND_DOWNTIME
                 AND SP.K_MIN <= (100 * (1 - (idl.IDL_VALUE/(24 * (idl.PERIOD_TO-idl.PERIOD_FROM+1/86400) )))) 
                 AND SP.K_MAX >  (100 * (1 - (idl.IDL_VALUE/(24 * (idl.PERIOD_TO-idl.PERIOD_FROM+1/86400) ))))
                 AND idl.flags       = c_FLAG_H
          ), I AS (
          SELECT I.BILL_ID, I.REP_PERIOD_ID, I.ITEM_ID, I.ITEM_TYPE, I.INV_ITEM_ID, 
                 I.ORDER_ID, I.SERVICE_ID, I.SUBSERVICE_ID, I.CHARGE_TYPE, 
                 I.ITEM_TOTAL, I.ITEM_CURRENCY_ID, I.RECVD, 
                 I.DATE_FROM, I.DATE_TO, I.ITEM_STATUS, I.CREATE_DATE, I.LAST_MODIFIED,
                 I.REP_GROSS, I.REP_TAX, I.TAX_INCL, I.EXTERNAL_ID, I.NOTES
            FROM ITEM_T I
           WHERE I.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_REC, Pk00_Const.c_CHARGE_TYPE_MIN)
             AND I.ITEM_STATUS   = Pk00_Const.c_ITEM_STATE_OPEN
             AND I.REP_PERIOD_ID = idl.Rep_Period_Id
             AND I.BILL_ID       = idl.Bill_Id
             AND I.DATE_FROM    <= idl.Date_To
             AND I.DATE_TO      >= idl.Date_From
          )
          SELECT I.BILL_ID, I.REP_PERIOD_ID, v_item_id ITEM_ID, 
                 I.ITEM_TYPE, NULL INV_ITEM_ID, 
                 I.ORDER_ID, SLA.ORDER_BODY_ID,
                 I.SERVICE_ID, Pk00_Const.c_SUBSRV_IDL SUBSERVICE_ID, 
                 Pk00_Const.c_CHARGE_TYPE_SLA CHARGE_TYPE,
                 -(I.ITEM_TOTAL * SLA_PERCENT/100) ITEM_TOTAL,
                 I.ITEM_CURRENCY_ID,
                 0 RECVD, 
                 SLA.DATE_FROM, SLA.DATE_TO, I.ITEM_STATUS, 
                 SYSDATE, SYSDATE, 
                 0 REP_GROSS, 0 REP_TAX, I.TAX_INCL, NULL EXTERNAL_ID, 
                 '('||SLA.HOURS||' час.)' NOTES,
                 NULL DESCR
            FROM SLA, I
           WHERE SLA.ORDER_ID      = I.ORDER_ID
             AND SLA.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND SLA.BILL_ID       = I.BILL_ID
          ;
          v_count := SQL%ROWCOUNT;
          v_count_sla_h := v_count_sla_h + 1;
          --
        ELSIF idl.flags = c_FLAG_K THEN
          -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
          -- Расчет компенсации абонплаты на время простоя, 
          -- заданного коэффициентом доступности (К),
          -- с указанием SLA
          -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
          INSERT INTO ITEM_T (
              BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
              ORDER_ID, ORDER_BODY_ID,
              SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
              ITEM_TOTAL, ITEM_CURRENCY_ID, RECVD, DATE_FROM, DATE_TO, 
              ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
              REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, DESCR
          )
          WITH SLA AS (
              SELECT idl.BILL_ID,  idl.REP_PERIOD_ID,
                     idl.ORDER_ID, idl.ORDER_BODY_ID, idl.IDL_VALUE K_SLA,
                     idl.DATE_FROM, idl.DATE_TO,
                     P.SLA_PERCENT
                FROM SLA_PERCENT_T P
               WHERE P.RATEPLAN_ID   = idl.Rateplan_Id  -- используем результаты BIND_DOWNTIME
                 AND P.K_MIN        <= idl.IDL_VALUE 
                 AND P.K_MAX        >  idl.IDL_VALUE
          ), I AS (
          SELECT I.BILL_ID, I.REP_PERIOD_ID, I.ITEM_ID, I.ITEM_TYPE, I.INV_ITEM_ID, I.ORDER_ID, 
                 I.SERVICE_ID, I.SUBSERVICE_ID, I.CHARGE_TYPE, 
                 I.ITEM_TOTAL, I.ITEM_CURRENCY_ID, I.RECVD, 
                 I.DATE_FROM, I.DATE_TO, I.ITEM_STATUS, I.CREATE_DATE, I.LAST_MODIFIED, 
                 I.REP_GROSS, I.REP_TAX, I.TAX_INCL, I.EXTERNAL_ID, I.NOTES
            FROM ITEM_T I
           WHERE I.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_REC, Pk00_Const.c_CHARGE_TYPE_MIN)
             AND I.REP_PERIOD_ID = idl.REP_PERIOD_ID 
             AND I.BILL_ID       = idl.BILL_ID
             AND I.DATE_FROM    <= idl.Date_To
             AND I.DATE_TO      >= idl.Date_From
          )
          SELECT I.BILL_ID, I.REP_PERIOD_ID, v_item_id ITEM_ID, 
                 I.ITEM_TYPE, NULL INV_ITEM_ID, 
                 I.ORDER_ID, SLA.ORDER_BODY_ID,
                 I.SERVICE_ID, Pk00_Const.c_SUBSRV_IDL SUBSERVICE_ID, 
                 Pk00_Const.c_CHARGE_TYPE_SLA CHARGE_TYPE,
                 -(I.ITEM_TOTAL * K_SLA) ITEM_TOTAL,
                 I.ITEM_CURRENCY_ID,
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
          v_count_sla_k := v_count_sla_k + 1;
          --
        ELSE
          v_count := 0;
        END IF;
      END IF;
      --
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- отмечаем результат работы в DISCOUNT_T
      UPDATE DOWNTIME_T D 
         SET D.STATUS        = DECODE(v_count, 0, c_DS_ERROR, c_DS_OK),
             D.ITEM_ID       = DECODE(v_count, 0, NULL, v_item_id),
             D.REP_PERIOD_ID = idl.rep_period_id
       WHERE D.STATUS        = c_DS_BIND_ORDER_BODY
         AND D.ORDER_ID      = idl.order_id
         AND D.ORDER_BODY_ID = idl.order_body_id
      ;
      
    END LOOP;    
    --
    -- формируем отчет для системы логирования
    Pk01_Syslog.Write_msg('DISCOUNT_T: '
                         ||'idl = '||v_count_idl||' rows,'
                         ||'sla_h = '||v_count_sla_h||' rows,'
                         ||'sla_k = '||v_count_sla_k||' rows'
                         , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расчет скидки для абонплаты на время простоя 
-- для биллингового периода p_period_id
-- ВНИМАНИЕ!!! Расчет ведется после начисления абонплаты, в валюте тарифа
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_Downtime( p_task_id IN INTEGER )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Charge_Downtime';
    v_count       INTEGER;
    v_item_id     INTEGER;
    v_count_idl   INTEGER := 0;
    v_count_sla_h INTEGER := 0;
    v_count_sla_k INTEGER := 0;
    v_count_err   INTEGER := 0;
    v_percent     NUMBER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
   
    -- Досоздаем стандартные компоненты услуги для компенсации простоев, где это необходимо 
    --Make_order_body( p_task_id );

    -- Привязать информацию о простоях к данным очереди 
    --Bind_data_queue( p_task_id );
    
    FOR idl IN (
      SELECT ACCOUNT_ID, BILL_ID, REP_PERIOD_ID, PERIOD_FROM, PERIOD_TO, 
             ORDER_ID, ORDER_BODY_ID, CHARGE_TYPE, FLAGS, 
             SUBSERVICE_ID, RATE_RULE_ID, RATEPLAN_ID, 
             DATA_PERIOD_ID, DATA_BILL_ID,
             ROUND(IDL_VALUE) IDL_VALUE,
             DATE_FROM, DATE_TO
        FROM (             
            SELECT DD.ACCOUNT_ID, DD.BILL_ID, DD.REP_PERIOD_ID, DD.PERIOD_FROM, DD.PERIOD_TO, 
                   DD.ORDER_ID, DD.ORDER_BODY_ID, DD.CHARGE_TYPE, DD.FLAGS, 
                   DD.SUBSERVICE_ID, DD.RATE_RULE_ID, DD.RATEPLAN_ID,
                   DD.DATA_PERIOD_ID,
                   BD.BILL_ID        DATA_BILL_ID,
                   SUM(DD.VALUE)     IDL_VALUE, 
                   MIN(DD.DATE_FROM) DATE_FROM, 
                   MAX(DD.DATE_TO)   DATE_TO
              FROM (
                  SELECT Q.BILL_ID, Q.REP_PERIOD_ID, Q.ACCOUNT_ID, 
                         P.PERIOD_FROM, P.PERIOD_TO, 
                         D.ORDER_ID, D.ORDER_BODY_ID, D.CHARGE_TYPE, D.FLAGS, 
                         OB.SUBSERVICE_ID, OB.RATE_RULE_ID, OB.RATEPLAN_ID,
                         Pk04_Period.Period_id(D.DATE_FROM) DATA_PERIOD_ID, -- разделяем периоды
                         CASE
                            WHEN OB.RATE_RULE_ID = 2421 THEN CEIL(D.VALUE)  -- округляем в большую сторону до часа
                            ELSE D.VALUE
                         END VALUE, 
                         D.DATE_FROM, 
                         D.DATE_TO
                    FROM DOWNTIME_T D, 
                         BILLING_QUEUE_T Q, -- счет в который включим простои 
                         PERIOD_T P, 
                         ORDER_BODY_T OB
                   WHERE Q.TASK_ID        = p_task_id
                     AND D.ACCOUNT_ID     = Q.ACCOUNT_ID 
                     AND Q.DATA_PERIOD_ID = P.PERIOD_ID
                     AND (Q.ORDER_ID IS NULL OR Q.ORDER_ID = D.ORDER_ID)
                     AND OB.ORDER_BODY_ID = D.ORDER_BODY_ID
                     AND D.STATUS         = c_DS_BIND_ORDER_BODY
                     --AND D.CREATE_DATE    > P.PERIOD_TO  -- чтобы случайно не зацепить ранее созданные  записи
                ) DD, 
                BILL_T BD  -- B-счет по данным которого считаем простои, м.б. прошедший период
             WHERE DD.ACCOUNT_ID    = BD.ACCOUNT_ID
               AND BD.REP_PERIOD_ID = DD.DATA_PERIOD_ID
               AND BD.BILL_TYPE     = PK00_CONST.c_BILL_TYPE_REC
             GROUP BY 
                   DD.ACCOUNT_ID, DD.BILL_ID, DD.REP_PERIOD_ID, DD.PERIOD_FROM, DD.PERIOD_TO, 
                   DD.ORDER_ID, DD.ORDER_BODY_ID, DD.CHARGE_TYPE, DD.FLAGS, 
                   DD.SUBSERVICE_ID, DD.RATE_RULE_ID, DD.RATEPLAN_ID,
                   DD.DATA_PERIOD_ID,
                   BD.BILL_ID
          )
    )
    LOOP
      -- если будет сформировано более одной позиции компенсации, 
      -- возникнет исключение, будем думать как быть дальше (например sum(item))
      v_item_id := Pk02_Poid.Next_item_id;
      
      IF idl.charge_type = Pk00_Const.c_CHARGE_TYPE_IDL THEN
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- только компенсация простоев
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- формируем item для компенсации простоев
        INSERT INTO ITEM_T (
            BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
            ORDER_ID, ORDER_BODY_ID,
            SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
            ITEM_TOTAL, ITEM_CURRENCY_ID, RECVD, DATE_FROM, DATE_TO, 
            ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
            REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, DESCR
        )
        SELECT 
            BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
            ORDER_ID, ORDER_BODY_ID,
            SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
            CASE
              WHEN DISC_IDL <= ITEM_TOTAL THEN -DISC_IDL
              ELSE -ITEM_TOTAL
            END ITEM_TOTAL,
            ITEM_CURRENCY_ID, RECVD, DATE_FROM, DATE_TO, 
            ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
            REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, DESCR
        FROM (
          SELECT 
              idl.bill_id, 
              idl.rep_period_id, 
              v_item_id ITEM_ID, 
              Pk00_Const.c_ITEM_TYPE_BILL ITEM_TYPE, 
              NULL INV_ITEM_ID, 
              idl.Order_Id, 
              idl.Order_Body_Id,
              SERVICE_ID, 
              Pk00_Const.c_SUBSRV_IDL SUBSERVICE_ID, 
              Pk00_Const.c_CHARGE_TYPE_IDL CHARGE_TYPE,
              DISC_IDL,
              ITEM_TOTAL,
              ITEM_CURRENCY_ID,
              0 RECVD, 
              idl.DATE_FROM, idl.DATE_TO, 
              ITEM_STATUS, 
              SYSDATE CREATE_DATE, 
              SYSDATE LAST_MODIFIED, 
              0 REP_GROSS, 0 REP_TAX, 
              TAX_INCL, 
              NULL EXTERNAL_ID, 
              '( Простои '||idl.IDL_VALUE||' час.)'  NOTES,
              NULL DESCR
            FROM (
              -- если позиций фиксированных начислений больше одной суммируем их             
              SELECT 
                  I.SERVICE_ID,
                  SUM (OB.RATE_VALUE * idl.IDL_VALUE / 720) DISC_IDL,
                  SUM(I.ITEM_TOTAL) ITEM_TOTAL,
                  I.ITEM_CURRENCY_ID,
                  PK00_CONST.c_ITEM_STATE_OPEN ITEM_STATUS,
                  I.TAX_INCL
                FROM ITEM_T I, ORDER_BODY_T OB 
               WHERE I.BILL_ID       = idl.data_bill_id
                 AND I.REP_PERIOD_ID = idl.data_period_id
                 AND I.ORDER_ID      = idl.Order_Id
                 AND I.DATE_FROM    <= idl.Date_To
                 AND I.DATE_TO      >= idl.Date_From
                 AND I.CHARGE_TYPE IN (PK00_CONST.c_CHARGE_TYPE_REC, 
                                       PK00_CONST.c_CHARGE_TYPE_MIN)
                 AND I.ITEM_TYPE     = Pk00_Const.c_ITEM_TYPE_BILL
                 AND I.ORDER_BODY_ID = OB.ORDER_BODY_ID 
               GROUP BY   
                  I.SERVICE_ID,
                  I.ITEM_CURRENCY_ID,
                  I.ITEM_STATUS, I.TAX_INCL 
            ) 
        );
        v_count := SQL%ROWCOUNT;
        v_count_idl := v_count_idl + 1;
        --
      ELSIF idl.charge_type = Pk00_Const.c_CHARGE_TYPE_SLA THEN
        IF idl.flags = c_FLAG_H THEN
          -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
          -- Расчет компенсации абонплаты на время простоя, заданного в часах,
          -- с указанием SLA
          -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
          BEGIN
              -- получаем значение SLA:
              SELECT SP.SLA_PERCENT
                INTO v_percent
                FROM SLA_PERCENT_T SP
               WHERE SP.RATEPLAN_ID  = idl.RATEPLAN_ID   -- используем результаты BIND_DOWNTIME
                 AND SP.K_MIN <= (100 * (1 - (idl.IDL_VALUE/(24 * (idl.PERIOD_TO-idl.PERIOD_FROM+1/86400) )))) 
                 AND SP.K_MAX >  (100 * (1 - (idl.IDL_VALUE/(24 * (idl.PERIOD_TO-idl.PERIOD_FROM+1/86400) ))))
              ;
              --
              -- создаем ITEM
              INSERT INTO ITEM_T (
                  BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
                  ORDER_ID, ORDER_BODY_ID,
                  SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
                  ITEM_TOTAL, ITEM_CURRENCY_ID, RECVD, DATE_FROM, DATE_TO, 
                  ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
                  REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, DESCR
              )
              SELECT  
                     idl.bill_id, 
                     idl.rep_period_id, 
                     v_item_id ITEM_ID,
                     I.ITEM_TYPE, 
                     NULL INV_ITEM_ID, 
                     idl.Order_Id, 
                     idl.ORDER_BODY_ID,
                     I.SERVICE_ID, 
                     Pk00_Const.c_SUBSRV_IDL SUBSERVICE_ID, 
                     Pk00_Const.c_CHARGE_TYPE_SLA CHARGE_TYPE,
                     -(I.ITEM_TOTAL * v_percent/100) ITEM_TOTAL,
                     I.ITEM_CURRENCY_ID,
                     0 RECVD, 
                     idl.Date_From, 
                     idl.Date_To, 
                     I.ITEM_STATUS, 
                     SYSDATE, SYSDATE, 
                     0 REP_GROSS, 0 REP_TAX, I.TAX_INCL, NULL EXTERNAL_ID, 
                     '('||idl.Idl_Value||' час.)' NOTES,
                     NULL DESCR
                FROM ITEM_T I
               WHERE I.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_REC, 
                                       Pk00_Const.c_CHARGE_TYPE_MIN)
                 AND I.ITEM_TYPE     = Pk00_Const.c_ITEM_TYPE_BILL
                 AND I.REP_PERIOD_ID = idl.Data_period_Id
                 AND I.BILL_ID       = idl.Data_bill_Id
                 AND I.DATE_FROM    <= idl.Date_To
                 AND I.DATE_TO      >= idl.Date_From
                 AND I.ORDER_ID      = idl.Order_id
              ;
              v_count := SQL%ROWCOUNT;
          EXCEPTION 
            WHEN OTHERS THEN
              v_count := 0;
              v_count_err := v_count_err + 1;
          END;
          v_count_sla_h := v_count_sla_h + 1;
          --
        ELSIF idl.flags = c_FLAG_K THEN
          -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
          -- Расчет компенсации абонплаты на время простоя, 
          -- заданного коэффициентом доступности (К),
          -- с указанием SLA
          -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
          BEGIN
              -- получаем значение SLA-K:
              SELECT P.SLA_PERCENT
                INTO v_percent
                FROM SLA_PERCENT_T P
               WHERE P.RATEPLAN_ID   = idl.Rateplan_Id  -- используем результаты BIND_DOWNTIME
                 AND P.K_MIN        <= idl.IDL_VALUE 
                 AND P.K_MAX        >  idl.IDL_VALUE;

              -- создаем ITEM
              INSERT INTO ITEM_T (
                  BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
                  ORDER_ID, ORDER_BODY_ID,
                  SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
                  ITEM_TOTAL, ITEM_CURRENCY_ID, RECVD, DATE_FROM, DATE_TO, 
                  ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
                  REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, DESCR
              )
              SELECT idl.bill_id, 
                     idl.rep_period_id, 
                     v_item_id ITEM_ID,
                     I.ITEM_TYPE, 
                     NULL INV_ITEM_ID, 
                     I.ORDER_ID, 
                     idl.ORDER_BODY_ID,
                     I.SERVICE_ID, 
                     Pk00_Const.c_SUBSRV_IDL SUBSERVICE_ID, 
                     Pk00_Const.c_CHARGE_TYPE_SLA CHARGE_TYPE,
                     -(I.ITEM_TOTAL * v_percent) ITEM_TOTAL,
                     I.ITEM_CURRENCY_ID,
                     0 RECVD, 
                     idl.Date_From, 
                     idl.Date_To, 
                     I.ITEM_STATUS, 
                     SYSDATE, SYSDATE, 
                     0 REP_GROSS, 0 REP_TAX, I.TAX_INCL, NULL EXTERNAL_ID, NULL NOTES,
                     '( K = '||idl.Idl_Value||' )' DESCR
                FROM ITEM_T I
               WHERE I.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_REC, 
                                       Pk00_Const.c_CHARGE_TYPE_MIN)
                 AND I.REP_PERIOD_ID = idl.DATA_PERIOD_ID 
                 AND I.BILL_ID       = idl.DATA_BILL_ID
                 AND I.DATE_FROM    <= idl.Date_To
                 AND I.DATE_TO      >= idl.Date_From
              ;
              v_count := SQL%ROWCOUNT;
          EXCEPTION
            WHEN OTHERS THEN
              v_count := 0;
              v_count_err := v_count_err + 1;
          END;
          v_count_sla_k := v_count_sla_k + 1;
          --
        ELSE
          v_count := 0;
          v_count_err := v_count_err + 1;
        END IF;
      END IF;
      --
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- отмечаем результат работы в DISCOUNT_T
      UPDATE DOWNTIME_T D 
         SET D.STATUS        = DECODE(v_count, 0, c_DS_ERROR, c_DS_OK),
             D.ITEM_ID       = DECODE(v_count, 0, NULL, v_item_id),
             D.REP_PERIOD_ID = idl.rep_period_id
       WHERE D.STATUS        = c_DS_BIND_ORDER_BODY
         AND D.ORDER_ID      = idl.order_id
         AND D.ORDER_BODY_ID = idl.order_body_id
      ;
      
    END LOOP;    
    --
    -- формируем отчет для системы логирования
    Pk01_Syslog.Write_msg('DISCOUNT_T: '
                         ||'idl = '||v_count_idl    ||' rows, '
                         ||'sla_h = '||v_count_sla_h||' rows, '
                         ||'sla_k = '||v_count_sla_k||' rows, '
                         ||'error = '||v_count_err  ||' rows'
                         , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------- --
-- Удаляем нулевые простои из начислений
-- ---------------------------------------------------------------------- --
PROCEDURE Delete_empty_items( p_task_id IN INTEGER )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Delete_empty_items';
    v_count    INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- проставляем код ошибки в DISCOUNT_T
    --
    UPDATE DOWNTIME_T D
       SET D.STATUS = c_DS_NULL
     WHERE D.STATUS = c_DS_OK
       AND EXISTS (
            SELECT * 
              FROM BILLING_QUEUE_T Q, PERIOD_T P, ITEM_T I
             WHERE Q.TASK_ID        = p_task_id
               AND Q.DATA_PERIOD_ID = P.PERIOD_ID
               AND Q.ACCOUNT_ID     = D.ACCOUNT_ID
               AND Q.BILL_ID        = I.BILL_ID
               AND I.REP_PERIOD_ID  = D.REP_PERIOD_ID
               AND I.ITEM_ID        = D.ITEM_ID
               AND I.ITEM_TOTAL     = 0
               AND I.CHARGE_TYPE IN ( Pk00_Const.c_CHARGE_TYPE_IDL,
                                      Pk00_Const.c_CHARGE_TYPE_SLA )
               AND D.CREATE_DATE    > P.PERIOD_TO  -- чтобы случайно не зацепить ранее созданные  записи
        );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('DOWNTIME_T.STATUS = c_DS_NULL '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- удаляем пустые позиции счетов
    --
    DELETE FROM ITEM_T I
     WHERE I.ITEM_TOTAL = 0
       AND I.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_IDL,
                             Pk00_Const.c_CHARGE_TYPE_SLA)
       AND EXISTS (
            SELECT * 
              FROM BILLING_QUEUE_T Q, PERIOD_T P, DOWNTIME_T D
             WHERE Q.TASK_ID        = p_task_id
               AND Q.DATA_PERIOD_ID = P.PERIOD_ID
               AND Q.ACCOUNT_ID     = D.ACCOUNT_ID
               AND I.BILL_ID        = Q.BILL_ID
               AND I.REP_PERIOD_ID  = D.REP_PERIOD_ID
               AND I.ITEM_ID        = D.ITEM_ID
               AND D.CREATE_DATE    > P.PERIOD_TO  -- чтобы случайно не зацепить ранее созданные  записи
        );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T.ITEM_TOTAL = 0 '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

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
    -- возвращаем курсор
    OPEN p_recordset FOR
         SELECT ITEM_ID, ITEM_TYPE, BILL_ID, 
                ORDER_ID, SERVICE_ID, CHARGE_TYPE,  
                BILL_TOTAL, RECVD,  
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
          FROM sla_services_t@PINDB.WORLD a, service_t@PINDB.WORLD s
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
          FROM percents_t@PINDB.WORLD a, RATEPLAN_T P
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
          FROM sla_services_t@PINDB.WORLD a, service_t@PINDB.WORLD s, 
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
-- счет должен быть в состоянии 'OPEN'
PROCEDURE Rollback_downtimes( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_downtimes';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- отвязываем события простоев от позиций начислений
    UPDATE DOWNTIME_T D
       SET D.ITEM_ID = NULL, D.STATUS = c_DS_BIND_ORDER_BODY
     WHERE EXISTS (
        SELECT * 
          FROM BILLING_QUEUE_T Q, ITEM_T I, BILL_T B, PERIOD_T P
         WHERE Q.BILL_ID       = I.BILL_ID
           AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID
           AND Q.TASK_ID       = p_task_id
           AND D.ITEM_ID       = I.ITEM_ID
           AND Q.DATA_PERIOD_ID= P.PERIOD_ID
           AND I.DATE_FROM    <= P.PERIOD_TO 
           AND I.DATE_TO      >= P.PERIOD_FROM
           AND Q.BILL_ID       = B.BILL_ID
           AND Q.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND (Q.ORDER_ID IS NULL OR Q.ORDER_ID = D.ORDER_ID)
           AND B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_OPEN
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Downtime_t: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    -- удаляем Item-ы компенсации за простои
    DELETE FROM ITEM_T I
     WHERE I.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_IDL, 
                             Pk00_Const.c_CHARGE_TYPE_SLA)
       AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
       AND I.ITEM_TYPE   = Pk00_Const.c_ITEM_TYPE_BILL  -- корректировки не удаляем
       AND I.EXTERNAL_ID IS NULL -- на всякий случай
       AND EXISTS (
        SELECT * FROM BILLING_QUEUE_T Q, PERIOD_T P, BILL_T B
         WHERE Q.BILL_ID       = I.BILL_ID
           AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID
           AND Q.DATA_PERIOD_ID= P.PERIOD_ID
           AND Q.TASK_ID       = p_task_id
           AND I.DATE_FROM    <= P.PERIOD_TO 
           AND I.DATE_TO      >= P.PERIOD_FROM
           AND Q.BILL_ID       = B.BILL_ID
           AND Q.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND (Q.ORDER_ID IS NULL OR Q.ORDER_ID = I.ORDER_ID)
           AND B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_OPEN
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
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --   
    --  производим компенсацию простоев
    Charge_Downtime( p_task_id );
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Рассчитатать и применить компенсацию за простои для Дебет-ноты
-- Дебет-нота должна быть в состоянии: 'OPEN'
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Recharge_downtime_for_debet( 
             p_dbt_bill_id    IN INTEGER,  -- id - дебетового счета, в котором учтется скидка
             p_dbt_period_id  IN INTEGER,  -- период дебетового счета
             p_crd_period_id  IN INTEGER   -- период кредитового счета, для которого расчитывается скидка
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Recharge_downtime_for_debet';
    v_task_id    INTEGER;
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --   
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1) Создать очередь, для обработки указанного периода 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_task_id := PK30_BILLING_QUEUE.Open_task;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2) заполняем очередь с заказами, для компенсации простоев
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    INSERT INTO BILLING_QUEUE_T( BILL_ID, ACCOUNT_ID, ORDER_ID, TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID )
    WITH D AS (
        SELECT DISTINCT O.ACCOUNT_ID 
          FROM DOWNTIME_T D, ORDER_T O, PERIOD_T P
         WHERE P.PERIOD_ID    = p_crd_period_id
           AND D.DATE_FROM   <= P.PERIOD_TO
           AND P.PERIOD_FROM <= D.DATE_TO 
           AND O.ORDER_NO     = D.ORDER_NO
    )
    SELECT B.BILL_ID, B.ACCOUNT_ID, NULL ORDER_ID,  
           v_task_id TASK_ID, B.REP_PERIOD_ID, p_crd_period_id 
      FROM D, BILL_T B 
     WHERE B.REP_PERIOD_ID = p_dbt_period_id
       AND B.BILL_ID       = p_dbt_bill_id
       AND D.ACCOUNT_ID    = B.ACCOUNT_ID
       AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_DBT
       AND B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_OPEN;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    IF v_count > 0 THEN
        -- 3) Расформирование позиций компенсаций за простои
        Rollback_downtimes( v_task_id );
        -- 4) отрабатываем задачу формирования компенсаций за простои
        Task_processing( v_task_id );

    END IF;
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 5) Чистим очередь задачи
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk30_Billing_Queue.Close_task( v_task_id ); 
    
    --  производим компенсацию простоев
    Charge_Downtime( v_task_id );
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

END PK38_BILLING_DOWNTIME_OLD;
/
