CREATE OR REPLACE PACKAGE PK30_BILLING_QUEUE
IS
    --
    -- Пакет для поддержки процесса выставления формирования/переформирования
    -- очереди счетов
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK30_BILLING_QUEUE';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Перед использоанием ф-й пакета, необходимо заполнить очередь:
    -- TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE
    -- INSERT INTO BILLING_QUEUE_T (BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID) ...
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Получить номер задачи для постановки в очередь
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Open_task RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Завершить задачу - удаляет данные из очереди (ВЫПОЛНЯТЬ ОБЯЗАТЕЛЬНО)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Close_task(p_task_id IN INTEGER);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ПРИМЕР запроленеия очереди данными о счетах, в которых
    -- появились открытые ITEM-ы, например в результате пересчета
    --
    Procedure Fill_queue_sample(p_task_id IN INTEGER, p_period_id IN INTEGER);
    
    /*
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расформирование счетов стоящих в очереди
    --
    Procedure Rollback_queue(p_period_id IN INTEGER, p_task_id IN INTEGER);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Закрытие биллингового периода для счетов поставленных в очередь
    --
    PROCEDURE Close_period_queue(p_period_id IN INTEGER, p_task_id IN INTEGER);
    */
    -- ===================================================================== --
    -- Работа со счетами
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расформирование счетов стоящих в очереди
    --
    Procedure Rollback_bills(p_task_id IN INTEGER);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Формирование счетов поставленных в очередь
    --
    PROCEDURE Close_bills(p_task_id IN INTEGER);
    
    -- ===================================================================== --
    -- Работа с фиксированными начислениями абонка + трафик
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расформирование фиксированных начислений
    --
    Procedure Rollback_fixrates(p_task_id IN INTEGER);
    
END PK30_BILLING_QUEUE;
/
CREATE OR REPLACE PACKAGE BODY PK30_BILLING_QUEUE
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Получить номер задачи для постановки в очередь
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Open_task RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Open_task';
    v_task_id     INTEGER;
BEGIN
    -- получаем номер задачи
    SELECT SQ_BILLING_QUEUE_T.NEXTVAL INTO v_task_id FROM DUAL;
    -- сохраняем для разборок в системе логирования
    Pk01_Syslog.Write_msg('task_id = '||v_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- возвращаем результат
    RETURN v_task_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Завершить задачу - удаляет данные из очереди (ВЫПОЛНЯТЬ ОБЯЗАТЕЛЬНО)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Close_task(p_task_id IN INTEGER)
IS
--    PRAGMA AUTONOMOUS_TRANSACTION;
    v_prcName     CONSTANT VARCHAR2(30) := 'Close_task';
    v_count       INTEGER := 0;
BEGIN
    -- удаляем задачу из очереди
    DELETE FROM BILLING_QUEUE_T Q
     WHERE Q.TASK_ID = p_task_id;
    v_count := SQL%ROWCOUNT;
--    COMMIT;
    -- сохраняем для разборок в системе логирования
    Pk01_Syslog.Write_msg('task_id = '||p_task_id||', deleted '||v_count||' rows', 
                                   c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Task_id = '||p_task_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Перед использоанием ф-й пакета, необходимо заполнить очередь:
-- TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE
-- INSERT INTO BILLING_QUEUE_T (BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID) ...
--
-- ПРИМЕР запроленеия очереди данными о счетах, в которых
-- появились открытые ITEM-ы, например в результате пересчета
Procedure Fill_queue_sample(p_task_id IN INTEGER, p_period_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Fill_queue_sample';
    v_period_from DATE;
    v_period_to   DATE;
    v_count       INTEGER;
    v_task_id     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);    
    --
    INSERT INTO BILLING_QUEUE_T(BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, 
                TASK_ID, REP_PERIOD_ID)
    SELECT B.BILL_ID, B.ACCOUNT_ID, A.BILLING_ID, B.PROFILE_ID,
           p_task_id, B.REP_PERIOD_ID
      FROM ITEM_T I, BILL_T B, ACCOUNT_T A
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND I.BILL_ID       = B.BILL_ID
       AND I.ITEM_STATUS   = Pk00_Const.c_ITEM_STATE_OPEN
       AND A.ACCOUNT_ID    = B.ACCOUNT_ID
    ;
    v_count := SQL%ROWCOUNT;
    
    Pk01_Syslog.Write_msg('Stop, '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ===================================================================== --
-- Работа со счетами
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расформирование счетов стоящих в очереди
--
PROCEDURE Rollback_bills(p_task_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_bills';
    v_count      INTEGER;
    v_last_period_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- отвязать позиции счетов фактур от позиции счета и привести в исходное состояние
    UPDATE ITEM_T I
       SET I.INV_ITEM_ID = NULL,
           I.REP_GROSS   = NULL,
           I.REP_TAX     = NULL,
           I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
     WHERE I.ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, Pk00_Const.c_ITEM_TYPE_ADJUST)
       AND EXISTS (
           SELECT * FROM BILLING_QUEUE_T Q
            WHERE Q.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND Q.BILL_ID       = I.BILL_ID
              AND Q.TASK_ID       = p_task_id
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('update item_t '||v_count||' rows - отвязать позиции счетов фактур от позиции счета', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
       
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- удалить позиции счетов фактур
    DELETE FROM INVOICE_ITEM_T II
     WHERE EXISTS (
           SELECT * FROM BILLING_QUEUE_T Q
            WHERE Q.REP_PERIOD_ID = II.REP_PERIOD_ID
              AND Q.BILL_ID       = II.BILL_ID
              AND Q.TASK_ID       = p_task_id
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('delete invoice_item_t '||v_count||' rows - удалить позиции счетов фактур', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Откатить биллинг 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- приводим в исходное состояние запись счета        
    UPDATE BILL_T B
       SET B.TOTAL = 0,
           B.GROSS = 0,
           B.TAX   = 0,
           B.RECVD = 0,
           B.DUE   = 0,
           B.DUE_DATE = NULL,
           B.PAID_TO  = NULL,
           B.CALC_DATE = NULL,
           B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN
     WHERE EXISTS (
         SELECT * FROM BILLING_QUEUE_T Q
          WHERE Q.REP_PERIOD_ID = B.REP_PERIOD_ID
            AND Q.BILL_ID = B.BILL_ID
            AND Q.TASK_ID = p_task_id
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('update bill_t '||v_count||' rows - приводим в исходное состояние запись счета', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Пересчитать балансы, всех лицевых счетов (счета READY - входят в баланс)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ID последнего закрытого периода
    v_last_period_id := PK04_PERIOD.Last_period_id;
    -- получаем входящий остаток от последнего закрытого периода
    UPDATE ACCOUNT_T A
       SET (A.BALANCE, A.BALANCE_DATE) = (
         SELECT 
         (
            -- получаем исходящий баланс от последнего закрытого периода
            SELECT NVL(SUM(R.CLOSE_BALANCE),0)
              FROM REP_PERIOD_INFO_T R
             WHERE R.ACCOUNT_ID = A.ACCOUNT_ID
               AND R.REP_PERIOD_ID = v_last_period_id
         )-(
            -- получаем полную задолженность по выставленным счетам
            SELECT NVL(SUM(B.TOTAL),0)
              FROM BILL_T B
             WHERE B.ACCOUNT_ID = A.ACCOUNT_ID
               AND B.BILL_STATUS IN (PK00_CONST.c_BILL_STATE_CLOSED, PK00_CONST.c_BILL_STATE_READY)
               AND B.REP_PERIOD_ID > v_last_period_id
         )+(
            -- получаем сумму поступивших за период платежей
            SELECT NVL(SUM(P.RECVD),0)
              FROM PAYMENT_T P
             WHERE P.ACCOUNT_ID = A.ACCOUNT_ID
               AND P.REP_PERIOD_ID > v_last_period_id
         ) BALANCE, SYSDATE
         FROM DUAL
       )
    WHERE EXISTS (
        SELECT * FROM BILLING_QUEUE_T Q
         WHERE Q.ACCOUNT_ID = A.ACCOUNT_ID
           AND Q.TASK_ID    = p_task_id
    );
    v_count := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg('Пересчитаны балансы '||v_count||' Л/С', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --   
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Формирование счетов поставленных в очередь
--
PROCEDURE Close_bills(p_task_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_bills';
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Формирование счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Make_bills( p_task_id => p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассчитать оборты за период для всех лицевых счетов, где были обороты за месяц
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Period_info( p_task_id => p_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ===================================================================== --
-- Работа с фиксированными начислениями абонка + трафик
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расформирование фиксированных начислений,
-- за исключением тех что сформировал тарификатор трафика
--
Procedure Rollback_fixrates( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_fixrates';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    PK36_BILLING_FIXRATE.Rollback_fixrates( p_task_id   => p_task_id );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

END PK30_BILLING_QUEUE;
/
