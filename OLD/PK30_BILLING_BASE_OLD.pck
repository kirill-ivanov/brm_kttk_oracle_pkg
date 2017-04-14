CREATE OR REPLACE PACKAGE PK30_BILLING_BASE_OLD
IS
    --
    -- Пакет содержит базовые методы для поддержки процесса выставления 
    -- счетов и закрытия периода
    -- ВНИМАНИЕ: Закрываются только счета стоящие в очереди: BILLING_QUEUE_T !!!
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK30_BILLING_BASE';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Переход биллинга на следующий биллинговый период (переход через месяц)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Next_Bill_Period;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- получить закрываемый период (берем предыдущий незакрытый период) 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Period_for_close RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Установить пользовательскую блокировку ресурса
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Lock_resource;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Снять пользовательскую блокировку ресурса
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Unlock_resource;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- поставить в очередь на выставление счетов 
    -- клиентов указанного биддинга юр.лиц или физ лиц
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Mark_bills( p_period_id    IN INTEGER, 
                          p_billing_id   IN INTEGER, 
                          p_task_id      IN INTEGER DEFAULT NULL,
                          p_account_type IN VARCHAR2 DEFAULT PK00_CONST.c_ACC_TYPE_J);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Подготовка позиций счета к биллингованию
    -- перевести счет в состояние READY
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Prep_items( p_task_id IN INTEGER, p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Формирование строк счетов-фактур по сформированным счетам 
    -- за закрываемый период (last_period)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Invoicing( p_task_id IN INTEGER, p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Биллинг - заполнить поля счета
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Billing( p_task_id IN INTEGER, p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- Разнести все что осталось на платежах за закрываемый период на сформированные счета
    -- разноска внутри л/с по позициям баланс Л/С не изменяет
    -- разноска для 'Ф' автоматическая FIFO, 'Ю'-ручная через АРМ платежей
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Payment_processing( p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расчитать авансы по платежам на начало месяца
    PROCEDURE Calc_advance( p_task_id IN INTEGER, p_bill_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассчитать оборты за период для всех лицевых счетов
    PROCEDURE Period_info( p_task_id IN INTEGER, p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Пересчитать балансы, всех лицевых счетов
    -- В баланс входят входящее сальдо + выставленные счета + все платежи текущего периода
    PROCEDURE Refresh_balance( p_task_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Проведение постоянно-периодических начисления
    -- для счетов поставленных в очередь
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Charge_fixrates( p_task_id IN INTEGER, p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Формирование счетов биллингового периода
    -- для счетов поставленных в очередь
    PROCEDURE Make_bills( p_task_id IN INTEGER, p_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Закрытие биллингового периода
    -- для счетов поставленных в очередь
    PROCEDURE Close_period( p_task_id IN INTEGER, p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Закрытие финансового периода
    -- закрыть готовые счета прошлого периода (изменить статус на CLOSED - запретить в них все изменения)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Close_Financial_Period;

    -- ----------------------------------------------------------------------- --
    -- В С П О М О Г А Т Е Л Ь Н Ы Е   Ф У Н К Ц И И
    -- ----------------------------------------------------------------------- --
    -- Корректировка признака включения НДС в счет
    PROCEDURE Correct_tax_incl( p_period_id IN INTEGER );
    
    -- Проверяем/Исправляем коды регионов в счетах
    PROCEDURE Correct_region_bill( p_period_id IN INTEGER );
    
END PK30_BILLING_BASE_OLD;
/
CREATE OR REPLACE PACKAGE BODY PK30_BILLING_BASE_OLD
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- получить закрываемый период (берем предыдущий незакрытый период) 
--
FUNCTION Period_for_close RETURN INTEGER
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Period_for_close';
    v_period_id INTEGER;
BEGIN
    SELECT PERIOD_ID
      INTO v_period_id
      FROM PERIOD_T
     WHERE CLOSE_FIN_PERIOD IS NULL
       AND POSITION = PK00_CONST.c_PERIOD_BILL;
    RETURN v_period_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Переход биллинга на следующий период
-- Позже переделаю на массовые операции, сначала 
-- UPDATE BILLINFO_T со смещением счетов last <- bill, bill <- next, next <- null
-- затем, неторопясь, создаем счета для next
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 
PROCEDURE Next_Bill_Period 
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Next_Bill_Period';
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Переводим систему на следующий биллинговый период
    Pk04_Period.Next_bill_period;
    --
    COMMIT;
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Установить пользовательскую блокировку ресурса
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Lock_resource
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    
    v_prcName    CONSTANT VARCHAR2(30) := 'Lock_resource';
    v_result     NUMBER;
    v_handle     VARCHAR2(128);
    v_lock_name  VARCHAR2(16) := Pk00_Const.c_Bill_Lock;
BEGIN
    -- получаем блокировку с заданным именем
    DBMS_LOCK.ALLOCATE_UNIQUE(v_lock_name, v_handle, 86400);
    -- устанавливаем блокировку
    v_result := DBMS_LOCK.REQUEST (
                    lockhandle        => v_handle,
                    lockmode          => DBMS_LOCK.S_MODE,
                    timeout           => 10,   -- период в сек., в течении которого пытаемся получить блокировку
                    release_on_commit => FALSE
                );
    -- контролируем результат
    CASE v_result
      WHEN 0 THEN                       
          Pk01_Syslog.Write_Msg(p_Msg => 'The lock has been got successfully (' || v_lock_name || '). ',
                                p_Src => c_PkgName||v_prcName );
      WHEN 1 THEN      
          Pk01_Syslog.Write_Msg(p_Msg => 'The lock is busy (' || v_lock_name || ').',
                                p_Src => c_PkgName||v_prcName );
      WHEN 2 THEN  -- критическая ошибка
          Pk01_Syslog.Raise_user_Exception('Deadlock detected (' || v_lock_name || ').', c_PkgName||'.'||v_prcName );
      WHEN 3 THEN         
          Pk01_Syslog.Write_Msg(p_Msg => 'Parameter error.',	
                                p_Src => c_PkgName||v_prcName );
      WHEN 4 THEN
          Pk01_Syslog.Write_Msg(p_Msg => 'Already own lock specified by lockhandle. (' ||v_lock_name|| ')',
                                p_Src => c_PkgName||v_prcName );
      WHEN 5 THEN
          Pk01_Syslog.Raise_user_Exception('Illegal lock handle (' || v_lock_name || ').', c_PkgName||'.'||v_prcName );
    END CASE;
    -- подтверждаем результат
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Снять пользовательскую блокировку ресурса
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Unlock_resource
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    
    v_prcName    CONSTANT VARCHAR2(30) := 'Unlock_resource';
    v_result     NUMBER;
    v_handle     VARCHAR2(128);
    v_lock_name  VARCHAR2(16) := Pk00_Const.c_Bill_Lock;
BEGIN
  
    -- получаем блокировку с заданным именем
    DBMS_LOCK.ALLOCATE_UNIQUE(v_lock_name, v_handle, 86400);

    v_result := DBMS_LOCK.RELEASE(lockhandle => v_handle);
    CASE v_result
        WHEN 0 THEN     
            Pk01_Syslog.Write_Msg(p_Msg => 'The lock has been released successfully (' || v_lock_name || ').',
                                  p_Src => c_PkgName||v_prcName );
        WHEN 3 THEN         
            Pk01_Syslog.Write_Msg(p_Msg => 'Parameter error.',    
                                  p_Src => c_PkgName||v_prcName );
        WHEN 4 THEN
            Pk01_Syslog.Write_Msg(p_Msg => 'Do not own lock specified by lockhandle (' || v_lock_name || ').', 
                                  p_Src => c_PkgName||v_prcName );
        WHEN 5 THEN
            Pk01_Syslog.Write_Msg(p_Msg => 'Illegal lock handle (' || v_lock_name || ').',    
                                  p_Src => c_PkgName||v_prcName );        
    END CASE;
    -- подтверждаем результат
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- поставить в очередь на выставление счетов 
-- клиентов указанного биддинга юр.лиц или физ лиц
-- ------------------------------------------------------------------------- --
PROCEDURE Mark_bills( p_period_id    IN INTEGER,
                      p_billing_id   IN INTEGER, 
                      p_task_id      IN INTEGER DEFAULT NULL,
                      p_account_type IN VARCHAR2 DEFAULT PK00_CONST.c_ACC_TYPE_J)
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Mark_bills';
    v_count       INTEGER;
    v_task_id     INTEGER;
BEGIN
    -- если id задачи не указан, считаем его равным id биллинга  
    IF p_task_id IS NOT NULL THEN
        v_task_id := p_task_id;
    ELSE
        v_task_id := p_billing_id;
    END IF;

    -- на всякий случай чистим очередь от возможных хвостов
    DELETE FROM BILLING_QUEUE_T Q 
     WHERE Q.TASK_ID    = v_task_id
       AND Q.BILLING_ID = p_billing_id;
    --
    -- Новый биллинг и BRM-KTTK
    INSERT INTO BILLING_QUEUE_T (BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, TASK_ID, REP_PERIOD_ID)
    SELECT BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, v_task_id, p_period_id
      FROM (
        SELECT B.BILL_ID, B.ACCOUNT_ID, A.BILLING_ID, AP.PROFILE_ID, 
               AP.DATE_FROM, MAX(AP.DATE_FROM) OVER (PARTITION BY AP.ACCOUNT_ID) MAX_DATE_FROM
          FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE B.REP_PERIOD_ID = p_period_id
           AND A.ACCOUNT_ID    = B.ACCOUNT_ID
           AND A.BILLING_ID    = p_billing_id
           AND A.ACCOUNT_TYPE  = p_account_type
           AND A.STATUS        = Pk00_Const.c_ACC_STATUS_BILL
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_OPEN, Pk00_Const.c_BILL_STATE_EMPTY)
           AND AP.ACCOUNT_ID   = A.ACCOUNT_ID
           AND AP.DATE_FROM   <= B.BILL_DATE
           AND (AP.DATE_TO IS NULL OR B.BILL_DATE <= AP.DATE_TO )
    )
    WHERE DATE_FROM = MAX_DATE_FROM;
    v_count := SQL%ROWCOUNT;
    COMMIT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILLING_QUEUE_T');
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Подготовка позиций счета к биллингованию
-- перевести счет в состояние READY
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Prep_items( p_task_id IN INTEGER, p_bill_period_id IN INTEGER )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Prep_items';
    v_count       INTEGER;
    v_period_from DATE;
    v_period_to   DATE;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    --
    v_period_from := Pk04_Period.Period_from(p_bill_period_id);
    v_period_to   := Pk04_Period.Period_to(p_bill_period_id);
    --
    -- переводим позиции начислений и корректировок в состояние READY
    -- заполняем поля налогов позиций начислений и корректировок для системы отчетов
    UPDATE ITEM_T I 
       SET (I.REP_GROSS, I.REP_TAX, I.ITEM_STATUS) = (
            SELECT 
                   CASE
                      WHEN I.TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN
                        (I.ITEM_TOTAL - PK09_INVOICE.ALLOCATE_TAX(I.ITEM_TOTAL, AP.VAT))
                      WHEN I.TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_NOT_INCL THEN
                        I.ITEM_TOTAL
                      ELSE 
                        NULL
                   END REP_GROSS,
                   CASE
                      WHEN I.TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN
                        (PK09_INVOICE.ALLOCATE_TAX(I.ITEM_TOTAL, AP.VAT))
                      WHEN I.TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_NOT_INCL THEN
                        PK09_INVOICE.CALC_TAX(I.ITEM_TOTAL, AP.VAT)
                      ELSE 
                        NULL
                   END REP_TAX,
                   Pk00_Const.c_ITEM_STATE_REАDY                    
              FROM BILL_T B, (
                SELECT ACCOUNT_ID, VAT
                  FROM (
                    SELECT ACCOUNT_ID, AP.VAT, DATE_FROM,
                           MAX(DATE_FROM) OVER (PARTITION BY ACCOUNT_ID) MAX_DATE_FROM 
                      FROM ACCOUNT_PROFILE_T AP
                     WHERE DATE_FROM <= v_period_to
                       AND (DATE_TO IS NULL OR v_period_from <= DATE_TO )
                     )WHERE DATE_FROM = MAX_DATE_FROM
                ) AP
             WHERE B.REP_PERIOD_ID = I.REP_PERIOD_ID
               AND B.BILL_ID       = I.BILL_ID
               AND B.ACCOUNT_ID    = AP.ACCOUNT_ID
       ) 
    WHERE I.REP_PERIOD_ID = p_bill_period_id
      AND I.ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, 
                          Pk00_Const.c_ITEM_TYPE_ADJUST)
      AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
      AND EXISTS (
          SELECT * FROM BILLING_QUEUE_T BQ
           WHERE I.BILL_ID = BQ.BILL_ID
             AND BQ.TASK_ID = p_task_id
      );
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Move status for '||v_count||' - items to '
                          || Pk00_Const.c_ITEM_STATE_REАDY, 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- проставляем статусы READY и EMPTY, пустые счета в дальнейших расчетах не участвуют
    -- (проходить в цикле долго, а их достаточно много)
    UPDATE BILL_T B
       SET (B.BILL_STATUS, B.TOTAL, B.GROSS, B.TAX, B.DUE, B.DUE_DATE, B.CALC_DATE) = (
           SELECT
               CASE 
                   WHEN EXISTS (
                       SELECT * FROM ITEM_T I 
                        WHERE I.BILL_ID = B.BILL_ID
                          AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
                          AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_REАDY
                   )
                   THEN Pk00_Const.c_BILL_STATE_READY
                   ELSE Pk00_Const.c_BILL_STATE_EMPTY
               END, 0, 0, 0, 0, SYSDATE, SYSDATE
           FROM DUAL 
       )
    WHERE B.REP_PERIOD_ID = p_bill_period_id
      AND B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN
      AND EXISTS (
          SELECT * FROM BILLING_QUEUE_T BQ
           WHERE B.BILL_ID = BQ.BILL_ID
             AND BQ.TASK_ID = p_task_id
      );
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||' - bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Формирование строк счетов-фактур за период (last_period)
-- из позиций готовых счетов
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Invoicing( p_task_id IN INTEGER, p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Invoicing';
    v_ok         INTEGER := 0;     -- кол-во сформированных инвойсов (не пустых)
    v_err        INTEGER := 0;     -- кол-во ошибок при формировании инвойсов
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- формируем инвойсы для всех открытых периодических счетов финанасового периода
    FOR r_bill IN (
        SELECT B.BILL_ID
          FROM BILL_T B
         WHERE B.REP_PERIOD_ID= p_bill_period_id 
           AND B.BILL_STATUS  = Pk00_Const.c_BILL_STATE_READY
           AND EXISTS (
               SELECT * FROM BILLING_QUEUE_T BQ
                WHERE B.BILL_ID = BQ.BILL_ID
                  AND BQ.TASK_ID = p_task_id
           )
      )
    LOOP
        SAVEPOINT X;  -- точка сохранения данных для лицевого счета
        BEGIN
            v_count := Pk09_Invoice.Calc_invoice(
                             p_bill_id       => r_bill.bill_id,
                             p_rep_period_id => p_bill_period_id
                          );
            v_ok := v_ok + 1;         -- инвойс создан успешно
        EXCEPTION
            WHEN OTHERS THEN
              -- откат изменений для лицевого счета
              ROLLBACK TO X;
              -- фиксируем ошибку в системе логирования
              Pk01_Syslog.Write_msg(
                 p_Msg  => 'bill_id = ' || r_bill.bill_id || ' - error',
                 p_Src  => c_PkgName||'.'||v_prcName,
                 p_Level=> Pk01_Syslog.L_err );
              v_err := v_err + 1;
            -- изменяем статус счета на ОШИБКА:
            UPDATE BILL_T 
               SET BILL_STATUS   = Pk00_Const.c_BILL_STATE_ERROR
             WHERE BILL_ID       = r_bill.bill_id
               AND REP_PERIOD_ID = p_bill_period_id;
        END;
        -- индикация выполнения
        v_count := v_ok + v_err;
        IF MOD(v_count, 10000) = 0 THEN
            Pk01_Syslog.Write_msg('count='||v_count, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
    END LOOP;    
    --
    Pk01_Syslog.Write_msg('Processed: '||v_ok||'-ок, '||v_err||'-err from '||v_count, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Биллинг - заполнить поля счета
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Billing( p_task_id IN INTEGER, p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing';
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ------------------------------------------------------------------------- --
    -- Заполняем поля счета, для непустых счетов 
    -- и устанавливаем максимальную дату оплаты счета, по умолчанию один календарный месяц
    UPDATE BILL_T B 
       SET (B.TOTAL, B.GROSS, B.TAX, B.DUE, B.ACT_DATE_FROM, B.ACT_DATE_TO) = (
          SELECT NVL(SUM(II.TOTAL),0), NVL(SUM(II.GROSS),0), 
                 NVL(SUM(II.TAX),0),  -NVL(SUM(II.TOTAL),0),
                 MIN(II.DATE_FROM), MAX(II.DATE_TO)
            FROM INVOICE_ITEM_T II
           WHERE II.BILL_ID = B.BILL_ID
             AND II.REP_PERIOD_ID = B.REP_PERIOD_ID
        ),
        (B.PAID_TO) = (
          SELECT CASE 
                   WHEN BI.DAYS_FOR_PAYMENT IS NULL THEN -- "месяц" - значение по умолчанию
                     ADD_MONTHS(B.BILL_DATE, 1)
                   ELSE
                     B.BILL_DATE + BI.DAYS_FOR_PAYMENT
                 END PAID_TO
            FROM BILLINFO_T BI
           WHERE BI.ACCOUNT_ID = B.ACCOUNT_ID
        ),
        (B.PROFILE_ID, B.CONTRACT_ID, B.CONTRACTOR_ID, B.CONTRACTOR_BANK_ID) = (
          SELECT AP.PROFILE_ID, AP.CONTRACT_ID, AP.CONTRACTOR_ID, AP.CONTRACTOR_BANK_ID
            FROM ACCOUNT_PROFILE_T AP
           WHERE AP.ACCOUNT_ID = B.ACCOUNT_ID
             AND AP.DATE_FROM <= B.BILL_DATE 
             AND (AP.DATE_TO IS NULL OR  B.BILL_DATE <= AP.DATE_TO)
             AND ROWNUM = 1
        ),
        B.DUE_DATE  = B.BILL_DATE,
        B.CALC_DATE = SYSDATE
    WHERE B.REP_PERIOD_ID = p_bill_period_id
      AND B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_READY
      AND EXISTS (
          SELECT * FROM BILLING_QUEUE_T BQ
           WHERE B.BILL_ID = BQ.BILL_ID
             AND BQ.TASK_ID = p_task_id
      )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||' - bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- Корректируем значение задолженности, для счетов где установлено поле корректировки
    UPDATE BILL_T B SET B.DUE = (B.RECVD - (B.TOTAL - B.ADJUSTED))
    WHERE B.REP_PERIOD_ID = p_bill_period_id
      AND B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_READY
      AND B.ADJUSTED     != 0
      AND EXISTS (
          SELECT * FROM BILLING_QUEUE_T BQ
           WHERE B.BILL_ID = BQ.BILL_ID
             AND BQ.TASK_ID = p_task_id
      );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Correct due for: '||v_count||' - bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- Заполняем поля счета, для пустых счетов
    UPDATE BILL_T B
       SET B.PAID_TO   = NULL, 
           B.DUE_DATE  = SYSDATE, 
           B.CALC_DATE = SYSDATE
     WHERE B.REP_PERIOD_ID = p_bill_period_id
       AND B.BILL_STATUS = Pk00_Const.c_BILL_STATE_EMPTY
       AND EXISTS (
          SELECT * FROM BILLING_QUEUE_T BQ
           WHERE B.BILL_ID = BQ.BILL_ID
             AND BQ.TASK_ID = p_task_id
       )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||' - empty bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
-- Разнести все что осталось на платежах за предбиллинговые и биллинговый периоды 
-- на сформированные счета
-- разноска внутри л/с по позициям баланс Л/С не изменяет
-- разноска для 'Ф' автоматическая FIFO, 'Ю'-ручная через АРМ платежей
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Payment_processing( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Payment_processing';
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id <= '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- Разносим все платежи методом FIFO (быстрая версия)
    PK10_PAYMENTS_TRANSFER.Method_fifo;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расчитать авансы по платежам на начало следующего месяца
-- На текущий момент авансом считается сумма платежа, которая не пошла 
-- на покрытие счетов выставленных, в период платежа или более ранние периоды 
--
PROCEDURE Calc_advance( p_task_id IN INTEGER, p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Calc_advance';
    v_count      INTEGER;
    v_period     DATE;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    v_period := PK04_PERIOD.Period_from(p_bill_period_id);
    --
    -- сбрасываем что насчитали раньше
    UPDATE PAYMENT_T P SET P.ADVANCE = 0, P.ADVANCE_DATE = v_period
     WHERE P.REP_PERIOD_ID= p_bill_period_id
       AND EXISTS (
           SELECT * FROM BILLING_QUEUE_T BQ
            WHERE P.ACCOUNT_ID = BQ.ACCOUNT_ID
              AND BQ.TASK_ID = p_task_id
       )
     ;
    MERGE INTO PAYMENT_T P
    USING (
        SELECT PAYMENT_ID, PAY_PERIOD_ID, SUM(TRANSFER_TOTAL) FOR_SERVICE 
          FROM PAY_TRANSFER_T T
        WHERE PAY_PERIOD_ID >= REP_PERIOD_ID     -- за оказанные услуги
          AND PAY_PERIOD_ID = p_bill_period_id
        GROUP BY PAYMENT_ID, PAY_PERIOD_ID
    ) T
    ON (P.PAYMENT_ID = T.PAYMENT_ID
        AND P.REP_PERIOD_ID = T.PAY_PERIOD_ID
        AND EXISTS (
             SELECT * FROM BILLING_QUEUE_T BQ
              WHERE P.ACCOUNT_ID = BQ.ACCOUNT_ID
                AND BQ.TASK_ID = p_task_id
          )
       )
    WHEN MATCHED THEN UPDATE SET P.ADVANCE = P.RECVD-T.FOR_SERVICE, 
         P.ADVANCE_DATE = ADD_MONTHS(TRUNC(P.PAYMENT_DATE,'mm'),1)-1/86400;
    v_count := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg('Stop, processed: '||v_count||' payments', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Рассчитать оборты за период для всех лицевых счетов,
-- где были обороты за месяц
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Period_info( p_task_id IN INTEGER, p_period_id IN INTEGER )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Period_info';
    v_count          INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- пересчитываем обороты по всем л/с имеющим статус 'BILL'
    MERGE INTO REP_PERIOD_INFO_T RP
    USING (
        WITH MONTH_PERIOD AS ( 
            SELECT REP_PERIOD_ID, ACCOUNT_ID,
                   SUM(BILL_TOTAL) BILL_TOTAL, SUM(GROSS) GROSS,
                   SUM(RECVD) RECVD, SUM(ADVANCE) ADVANCE    
             FROM (
                SELECT B.REP_PERIOD_ID, B.ACCOUNT_ID,  
                       B.TOTAL BILL_TOTAL, B.GROSS,  
                       0 RECVD, 0 ADVANCE 
                  FROM BILL_T B, BILLING_QUEUE_T Q
                 WHERE B.TOTAL <> 0 -- отсекаем секцию с пустыми счетами
                   AND B.REP_PERIOD_ID = p_period_id
                   AND Q.BILL_ID = B.BILL_ID
                   AND Q.ACCOUNT_ID = B.ACCOUNT_ID
                   AND Q.TASK_ID = p_task_id
                --
                UNION ALL
                -- получаем сумму поступивших за период платежей
                SELECT P.REP_PERIOD_ID, P.ACCOUNT_ID, 
                       0 BILL_TOTAL, 0 GROSS,
                       P.RECVD, P.ADVANCE  
                  FROM PAYMENT_T P
                 WHERE P.REP_PERIOD_ID = p_period_id
                   AND EXISTS ( -- формально может быть более одной записи с account_id
                       SELECT * FROM BILLING_QUEUE_T BQ
                        WHERE BQ.ACCOUNT_ID = P.ACCOUNT_ID
                          AND BQ.TASK_ID    = p_task_id
                   )
            )
            GROUP BY REP_PERIOD_ID, ACCOUNT_ID
        ), 
        PREV_PERIOD AS (
            SELECT ACCOUNT_ID, REP_PERIOD_ID, CLOSE_BALANCE 
            FROM (
                SELECT ACCOUNT_ID, 
                       MAX(REP_PERIOD_ID) OVER (PARTITION BY ACCOUNT_ID) MAX_PERIOD_ID,
                       REP_PERIOD_ID, CLOSE_BALANCE
                  FROM REP_PERIOD_INFO_T R
                 WHERE EXISTS ( -- формально может быть более одной записи с account_id
                       SELECT * FROM BILLING_QUEUE_T BQ
                        WHERE BQ.ACCOUNT_ID = R.ACCOUNT_ID
                          AND BQ.TASK_ID    = p_task_id
                   )
            ) WHERE REP_PERIOD_ID = MAX_PERIOD_ID
        )
        SELECT MP.REP_PERIOD_ID, MP.ACCOUNT_ID, 
               NVL(PP.CLOSE_BALANCE,0) OPEN_BALANCE,
               NVL(PP.CLOSE_BALANCE,0)+MP.RECVD-MP.BILL_TOTAL CLOSE_BALANCE,
               MP.BILL_TOTAL, MP.GROSS, MP.RECVD, MP.ADVANCE,
               SYSDATE LAST_MODIFIED
          FROM MONTH_PERIOD MP, PREV_PERIOD PP 
         WHERE MP.ACCOUNT_ID = PP.ACCOUNT_ID(+)
    ) PI
    ON (
        RP.REP_PERIOD_ID = PI.REP_PERIOD_ID  AND
        RP.ACCOUNT_ID    = PI.ACCOUNT_ID
    )
    WHEN MATCHED THEN UPDATE SET RP.TOTAL   = PI.BILL_TOTAL,
                                 RP.GROSS   = PI.GROSS,
                                 RP.RECVD   = PI.RECVD,
                                 RP.ADVANCE = PI.ADVANCE
    WHEN NOT MATCHED THEN INSERT ( 
                RP.REP_PERIOD_ID, RP.ACCOUNT_ID, RP.OPEN_BALANCE, RP.CLOSE_BALANCE, 
                RP.TOTAL, RP.GROSS, RP.RECVD, RP.ADVANCE, RP.LAST_MODIFIED
            )VALUES(
                PI.REP_PERIOD_ID, PI.ACCOUNT_ID, PI.OPEN_BALANCE, PI.CLOSE_BALANCE, 
                PI.BILL_TOTAL, PI.GROSS, PI.RECVD, PI.ADVANCE, PI.LAST_MODIFIED
            )
    ;
    --    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Сформированы обороты по '||v_count||' л/с', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Пересчитать балансы, всех лицевых счетов
-- В баланс входят входящее сальдо + выставленные счета + все платежи текущего периода
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Refresh_balance(p_task_id IN INTEGER)
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Refresh_balance';
    v_count          INTEGER;
    v_last_period_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ID последнего ЗАКРЫТОГО периода 
    v_last_period_id := PK04_PERIOD.Last_period_id; 
    -- получаем входящий остаток от последнего закрытого периода + начисления и платежи
    -- правильный запрос для получения баллансаот указанного REP_PERIOD_INFO
    MERGE INTO ACCOUNT_T A
    USING (
        WITH D AS (
            -- получаем строки задолженности по выставленным счетам
            SELECT B.ACCOUNT_ID, B.TOTAL BILL_TOTAL, B.ADJUSTED, 0 RECVD, 0 OPEN_BALANCE, BILL_DATE BALANCE_DATE
              FROM BILL_T B, BILLING_QUEUE_T BQ
             WHERE B.REP_PERIOD_ID > v_last_period_id
               AND B.TOTAL <> 0 -- отсекаем секцию с пустыми счетами
               AND BQ.BILL_ID    = B.BILL_ID
               AND BQ.ACCOUNT_ID = B.ACCOUNT_ID
               AND BQ.TASK_ID    = p_task_id
            --
            UNION ALL
            -- получаем строки поступивших за период платежей
            SELECT P.ACCOUNT_ID, 0 BILL_TOTAL, 0 ADJUSTED, P.RECVD, 0 OPEN_BALANCE, P.PAYMENT_DATE BALANCE_DATE  
            FROM PAYMENT_T P
            WHERE P.REP_PERIOD_ID > v_last_period_id
             AND EXISTS (
                 SELECT * FROM BILLING_QUEUE_T BQ
                  WHERE BQ.ACCOUNT_ID = P.ACCOUNT_ID
                    AND BQ.TASK_ID = p_task_id
             )
            --    
            UNION ALL
            -- сумма входящего остатка из оборотов
            SELECT ACCOUNT_ID, 0 BILL_TOTAL, 0 ADJUSTED, 0 RECVD, CLOSE_BALANCE OPEN_BALANCE, 
                   TO_DATE(TO_CHAR(REP_PERIOD_ID),'yyyymm') BALANCE_DATE
              FROM (
                SELECT PI.ACCOUNT_ID, PI.REP_PERIOD_ID, PI.CLOSE_BALANCE, 
                       MAX(PI.REP_PERIOD_ID) OVER (PARTITION BY PI.ACCOUNT_ID) MAX_PERIOD_ID  
                  FROM REP_PERIOD_INFO_T PI
                 WHERE PI.REP_PERIOD_ID <= v_last_period_id
                   AND EXISTS (
                       SELECT * FROM BILLING_QUEUE_T BQ
                        WHERE BQ.ACCOUNT_ID = PI.ACCOUNT_ID
                          AND BQ.TASK_ID = p_task_id
                   )
            ) WHERE REP_PERIOD_ID = MAX_PERIOD_ID
        ) 
        SELECT ACCOUNT_ID, 
               SUM(OPEN_BALANCE+ADJUSTED+RECVD-BILL_TOTAL ) BALANCE, 
               MAX(BALANCE_DATE) BALANCE_DATE --  
          FROM D
        GROUP BY ACCOUNT_ID
    )BAL
    ON( BAL.ACCOUNT_ID = A.ACCOUNT_ID )
    WHEN MATCHED THEN UPDATE 
      SET A.BALANCE = BAL.BALANCE, A.BALANCE_DATE = BAL.BALANCE_DATE;
    --
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
-- Исправляем флаг, включения налогов в начисления
-- до тех пор пока работаем через экспорт - это актуально
--
PROCEDURE Correct_tax_incl( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Correct_tax_incl';
    v_count      INTEGER;
BEGIN
    -- для Физ.лиц все начисления всегда вкючают налоги
    UPDATE ITEM_T I
       SET I.TAX_INCL = 'Y'
     WHERE (I.TAX_INCL = 'N' OR I.TAX_INCL IS NULL)
       AND EXISTS (
           SELECT * FROM ACCOUNT_T A, BILL_T B
            WHERE A.ACCOUNT_TYPE = 'P'
              AND A.ACCOUNT_ID = B.ACCOUNT_ID
              AND I.BILL_ID = B.BILL_ID
              AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
         )  
       AND I.REP_PERIOD_ID = p_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Исправлены '||v_count||' item для физ.лиц', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- для Юр.лиц начисления НЕ вкючают налоги
    UPDATE ITEM_T I
       SET I.TAX_INCL = 'N'
     WHERE (I.TAX_INCL = 'Y' OR I.TAX_INCL IS NULL)
       AND EXISTS (
           SELECT * FROM ACCOUNT_T A, BILL_T B
            WHERE A.ACCOUNT_TYPE = 'J'
              AND A.ACCOUNT_ID = B.ACCOUNT_ID
              AND I.BILL_ID = B.BILL_ID
              AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
         )  
       AND I.REP_PERIOD_ID = p_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Исправлены '||v_count||' item для юр.лиц', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Проверяем/Исправляем коды регионов в счетах
PROCEDURE Correct_region_bill( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Correct_region_bill';
    v_count      INTEGER;
BEGIN
    -- ============================================================================       
    -- отработка изменений, для счетов с установленным некорректным префиксом  
    MERGE INTO BILL_T B
    USING (       
        SELECT AP.ACCOUNT_ID, AP.PROFILE_ID, CR.REGION_ID, B.BILL_ID, B.REP_PERIOD_ID, B.BILL_NO 
          FROM ACCOUNT_PROFILE_T AP, CONTRACTOR_T CR, BILL_T B
         WHERE AP.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND CR.REGION_ID IS NOT NULL
           AND B.REP_PERIOD_ID = 201503
           AND B.PROFILE_ID = AP.PROFILE_ID
           AND B.ACCOUNT_ID = AP.ACCOUNT_ID
           AND CR.REGION_ID != SUBSTR(B.BILL_NO,1,4)
           AND SUBSTR(B.BILL_NO,5,1) = '/'
    ) P
    ON (
        B.REP_PERIOD_ID = P.REP_PERIOD_ID
        AND B.BILL_ID   = P.BILL_ID
    )
    WHEN MATCHED THEN UPDATE SET B.BILL_NO = LPAD(TO_CHAR(P.REGION_ID), 4,'0')||SUBSTR(B.BILL_NO,5);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Коды региона измененыдля '||v_count||' счетов', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ============================================================================       
    -- отработка изменений, для счетов с неустановленным префиксом
    MERGE INTO BILL_T B
    USING (       
        SELECT AP.ACCOUNT_ID, AP.PROFILE_ID, CR.REGION_ID, B.BILL_ID, B.REP_PERIOD_ID, B.BILL_NO 
          FROM ACCOUNT_PROFILE_T AP, CONTRACTOR_T CR, BILL_T B
         WHERE AP.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND CR.REGION_ID IS NOT NULL
           AND B.REP_PERIOD_ID = p_period_id
           AND B.PROFILE_ID = AP.PROFILE_ID
           AND B.ACCOUNT_ID = AP.ACCOUNT_ID
           AND SUBSTR(B.BILL_NO,5,1) != '/' -- префикс не установлен
    ) P
    ON (
        B.REP_PERIOD_ID = P.REP_PERIOD_ID
        AND B.BILL_ID   = P.BILL_ID
    )
    WHEN MATCHED THEN UPDATE SET B.BILL_NO = LPAD(TO_CHAR(P.REGION_ID), 4,'0')||'/'||B.BILL_NO;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Коды региона проставлены для '||v_count||' счетов', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ============================================================================       
    -- снять префиксы, где они были установлены ошибочно
    MERGE INTO BILL_T B
    USING (       
        SELECT AP.ACCOUNT_ID, AP.PROFILE_ID, CR.REGION_ID, B.BILL_ID, B.REP_PERIOD_ID, B.BILL_NO 
          FROM ACCOUNT_PROFILE_T AP, CONTRACTOR_T CR, BILL_T B
         WHERE AP.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND CR.REGION_ID IS NULL         -- регион не установлен
           AND B.REP_PERIOD_ID  = p_period_id
           AND B.PROFILE_ID     = AP.PROFILE_ID
           AND B.ACCOUNT_ID     = AP.ACCOUNT_ID
           AND SUBSTR(B.BILL_NO,5,1) = '/'   -- префикс установлен
    ) P
    ON (
        B.REP_PERIOD_ID = P.REP_PERIOD_ID
        AND B.BILL_ID   = P.BILL_ID
    )
    WHEN MATCHED THEN UPDATE SET B.BILL_NO = SUBSTR(B.BILL_NO, 6);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Коды региона удалены для '||v_count||' счетов', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Проведение постоянно-периодических начисления, для счетов 
-- поставленных в очередь
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_fixrates(p_task_id IN INTEGER, p_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Charge_fixrates';
BEGIN

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создание периодических счетов для клиентов имеющих
    -- абонплату или доплату до минимальной стоимости
    -- в биллинговом периоде p_period_id
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --PK36_BILLING_FIXRATE.Make_bills_for_fixrates( p_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- проводим начисления
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK36_BILLING_FIXRATE.Charge_fixrates(
          p_task_id       => p_task_id,             -- ID задачи
          p_rep_period_id => p_period_id,           -- период выставляемго счета
          p_data_period_id=> p_period_id            -- период данных  
       );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Формирование счетов биллингового периода
--
PROCEDURE Make_bills(p_task_id IN INTEGER, p_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_bills';
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Подготовка позиций счета к биллингованию, перевести счет в состояние READY
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Prep_items( p_task_id, p_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Формирование строк счетов-фактур за период (last_period) из позиций готовых счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Invoicing( p_task_id, p_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Биллинг - включить в счет расходные ITEMs, перевести счет в состояние READY
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Billing( p_task_id, p_period_id );

    --     
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Финальные процедуры закрытия периода
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Close_period( p_task_id IN INTEGER, p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_period';
BEGIN

    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расчитать авансы по платежам на начало следующего месяца
    -- На текущий момент авансом считается сумма платежа, которая не пошла 
    -- на покрытие счетов выставленных, в период платежа или более ранние периоды 
    --
    Calc_advance( p_task_id, p_period_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассчитать оборты за период для всех лицевых счетов
    -- (нужны для печати квитанций физ.лицам)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Period_info( p_task_id, p_period_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Пересчитать балансы, всех лицевых счетов (счета READY - входят в баланс)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Refresh_balance( p_task_id );
    --
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Закрытие финансового периода
-- закрыть готовые счета прошлого периода (изменить статус на CLOSED - запретить в них все изменения)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Close_Financial_Period
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_Financial_Period';
    v_period_id  INTEGER;
    v_count      INTEGER;
BEGIN
    -- получаем дату периода (берем предыдущий незакрытый период)
    v_period_id := Period_for_close;
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||v_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- закрыть финансовый период
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk04_Period.Close_fin_period;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- закрыть отчетный (управленческий) период (пока синхронно с финансовым)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk04_Period.Close_rep_period;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- закрыть готовые позиции счета прошлого периода (запретить в них все изменения)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE ITEM_T
       SET ITEM_STATUS = Pk00_Const.c_ITEM_STATE_CLOSED
     WHERE REP_PERIOD_ID = v_period_id
       AND ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, Pk00_Const.c_ITEM_TYPE_ADJUST);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Закрыто '||v_count||' позиций счетов', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- закрыть готовые счета прошлого периода (запретить в них все изменения)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE BILL_T 
       SET BILL_STATUS = Pk00_Const.c_BILL_STATE_CLOSED
     WHERE REP_PERIOD_ID = v_period_id
       AND BILL_STATUS IN ( Pk00_Const.c_BILL_STATE_READY, PK00_CONST.c_BILL_STATE_EMPTY);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Закрыто '||v_count||' счетов', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расчитать авансы по платежам на начало следующего месяца
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --Calc_advance( v_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассчитать оборты за период для всех лицевых счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --Period_info( v_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Пересчитать балансы, всех лицевых счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --Refresh_balance;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

END PK30_BILLING_BASE_OLD;
/
