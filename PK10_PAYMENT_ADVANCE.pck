CREATE OR REPLACE PACKAGE PK10_PAYMENT_ADVANCE
IS
    --
    -- Пакет для работы с Авансовыми платежами
    -- --------------------------------------------------------------------------- --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_PAYMENT_ADVANCE';
    -- ==============================================================================
   
    type t_refc is ref cursor;
    
    -- ======================================================================== --
    --                     С Л У Ж Е Б Н Ы Е   Ф У Н К Ц И И
    -- ======================================================================== --
    -- Начать новую задачу (получить id задачи для постановки в очередь)
    FUNCTION Open_task RETURN INTEGER;

    -- Завершить задачу - удаляет данные из очереди (ВЫПОЛНЯТЬ ОБЯЗАТЕЛЬНО)
    PROCEDURE Close_task(p_task_id IN INTEGER);

    -- Поставить в очередь платеж
    PROCEDURE Add_payment_to_queue (
            p_task_id      IN INTEGER,
            p_payment_id   IN INTEGER,
            p_period_id    IN INTEGER,
            p_advance_date IN DATE DEFAULT NULL,
            p_advance_no   IN VARCHAR2 DEFAULT NULL
        );
        
    -- Поставить в очередь все платежи периода, на которых есть аванс
    PROCEDURE Add_period_payments_to_queue (
            p_task_id      IN INTEGER,
            p_period_id    IN INTEGER
        );
    
    -- ------------------------------------------------------------------------ --
    -- Получить следующий номер счета-фактуры на аванс в BRM
    -- номера счетов формируются по единому правилу: A_REGIONID/YYMM(№ Л/С)/[1-9]
    -- ПРИДУМАЛ САМ, УТОЧНИТЬ!!!
    --
    FUNCTION Next_advance_no(
         p_payment_id IN INTEGER,
         p_period_id  IN INTEGER
     ) RETURN VARCHAR2;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- создать авансовые счета-фактуры для платежей стоящих в очереди
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_advance(p_task_id IN INTEGER);
  
    -- ======================================================================== --
    -- Формирование счетов-фактур на аванс
    -- ======================================================================== --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Сформировать аванс для указанного платежа
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_payment_advance(
                 p_period_id  IN INTEGER,
                 p_payment_id IN INTEGER
              );
              
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Сформировать аванс для всех платежей указанного периода
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_period_advance(
                 p_period_id  IN INTEGER
              );


END PK10_PAYMENT_ADVANCE;
/
CREATE OR REPLACE PACKAGE BODY PK10_PAYMENT_ADVANCE
IS

-- ======================================================================== --
--                     С Л У Ж Е Б Н Ы Е   Ф У Н К Ц И И
-- ======================================================================== --
-- Начать новую задачу (получить id задачи для постановки в очередь)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Open_task RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Open_task';
    v_task_id     INTEGER;
BEGIN
    -- получаем номер задачи
    SELECT SQ_BILLING_QUEUE_T.NEXTVAL INTO v_task_id FROM DUAL;
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
BEGIN
    -- удаляем задачу из очереди
    DELETE FROM ADVANCE_QUEUE_T Q
     WHERE Q.TASK_ID = p_task_id;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Task_id = '||p_task_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Поставить в очередь платеж
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Add_payment_to_queue (
        p_task_id      IN INTEGER,
        p_payment_id   IN INTEGER,
        p_period_id    IN INTEGER,
        p_advance_date IN DATE DEFAULT NULL,
        p_advance_no   IN VARCHAR2 DEFAULT NULL
    ) 
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_payment_to_queue';
BEGIN
    -- сохраняем данные в очереди
    INSERT INTO ADVANCE_QUEUE_T (
           TASK_ID, PAY_PERIOD_ID, PAYMENT_ID, 
           ADVANCE_ID, ADVANCE_DATE, ADVANCE_NO
    ) VALUES (
           p_task_id, p_period_id, p_payment_id, 
           NULL, p_advance_date, p_advance_no
    );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Поставить в очередь все платежи периода, на которых есть аванс
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Add_period_payments_to_queue (
        p_task_id      IN INTEGER,
        p_period_id    IN INTEGER
    ) 
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_period_payments_to_queue';
    v_count       INTEGER;
BEGIN
    -- сохраняем данные в очереди
    INSERT INTO ADVANCE_QUEUE_T (TASK_ID, PAY_PERIOD_ID, PAYMENT_ID)
    SELECT p_task_id TASK_ID, P.REP_PERIOD_ID, P.PAYMENT_ID
      FROM PAYMENT_T P, PAY_TRANSFER_T T
     WHERE P.REP_PERIOD_ID = p_period_id
       AND P.REP_PERIOD_ID = T.PAY_PERIOD_ID(+) -- разносок может и не быть
       AND P.PAYMENT_ID    = T.PAYMENT_ID(+)
       AND T.REP_PERIOD_ID(+)<= T.PAY_PERIOD_ID(+)
       AND P.ADVANCE_ID IS NULL
       AND P.ACCOUNT_ID    > 5 -- исключаем служебные л/с
     GROUP BY P.ACCOUNT_ID, P.REP_PERIOD_ID, P.PAYMENT_ID, P.RECVD
     HAVING P.RECVD > NVL(SUM(T.TRANSFER_TOTAL),0);
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ADVANCE_QUEUE_T - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Получить следующий номер счета-фактуры на аванс в BRM
-- номера счетов формируются по единому правилу: A_REGIONID/YYMM(№ Л/С)/[1-9]
-- ПРИДУМАЛ САМ, УТОЧНИТЬ!!!
--
FUNCTION Next_advance_no(
               p_payment_id IN INTEGER,
               p_period_id  IN INTEGER
           ) RETURN VARCHAR2
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Next_advance_no';
    v_billing_id   INTEGER;
    v_region_id    INTEGER;
    v_advance_no   ADVANCE_T.ADVANCE_NO%TYPE:=NULL;
    v_account_no   ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_count        INTEGER;
    v_next         INTEGER;
BEGIN
    -- получаем допольнительную информацию
    SELECT A.ACCOUNT_NO, A.BILLING_ID, CR.REGION_ID
      INTO v_account_no, v_billing_id, v_region_id
      FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACTOR_T CR, PAYMENT_T P
     WHERE A.ACCOUNT_ID     = P.ACCOUNT_ID
       AND A.ACCOUNT_ID     = AP.ACCOUNT_ID
       AND AP.ACTUAL        = 'Y'
       AND AP.CONTRACTOR_ID = CR.CONTRACTOR_ID
       AND P.PAYMENT_ID     = p_payment_id
       AND P.REP_PERIOD_ID  = p_period_id;

    -- формируем номер счета
    v_advance_no := SUBSTR(TO_CHAR(p_period_id),3,4)||v_account_no;
    -- проверяем на уникальность
    v_next := 1;
    LOOP
        -- проверяем существует ли номер
        SELECT COUNT(*) INTO v_count
          FROM ADVANCE_T B
         WHERE B.ADVANCE_NO = v_advance_no;  
        EXIT WHEN v_count = 0;  -- все нормально, выходим из цикла
        --
        -- формируем следующий по порядку счет    
        v_advance_no := SUBSTR(TO_CHAR(p_period_id),3,4)||v_account_no||'/'||v_next;
        --
        v_next := v_next + 1;
    END LOOP;
    
    -- с переходом на филиальную структуру, добавляем номер региона
    IF v_region_id IS NOT NULL THEN
        v_advance_no := 'A'||v_region_id||'/'||v_advance_no;
    ELSE
        v_advance_no := 'A'||v_advance_no;
    END IF;
    
    RETURN v_advance_no;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR(payment_id=' ||p_payment_id||
                                        ', period_id='  ||p_period_id||')'
                                    , c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- создать авансовые счета-фактуры для платежей стоящих в очереди
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Make_advance(p_task_id IN INTEGER) 
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Make_advance';
    v_count       INTEGER;
BEGIN
    -- проставляем ID счетов-фактур на аванс
    UPDATE ADVANCE_QUEUE_T Q
       SET Q.ADVANCE_ID   = Pk02_Poid.Next_bill_id,
           Q.ADVANCE_DATE = NVL(Q.ADVANCE_DATE, Pk04_Period.Period_to(Q.PAY_PERIOD_ID)),
           Q.ADVANCE_NO   = NVL(Q.ADVANCE_NO, 
                                Pk10_Payment_Advance.Next_advance_no(
                                    Q.PAYMENT_ID, Q.PAY_PERIOD_ID
                                )
                            )
     WHERE Q.TASK_ID      = p_task_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ADVANCE_QUEUE_T - '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- формируем счета-фактуры на аванс
    INSERT INTO ADVANCE_T (
        ADVANCE_ID, REP_PERIOD_ID, ACCOUNT_ID, ADVANCE_NO, ADVANCE_DATE, 
        ADVANCE_STATUS, ADVANCE_TYPE,
        CURRENCY_ID, TOTAL, GROSS, TAX, VAT, CREATE_DATE, PROFILE_ID
    )
    WITH PA AS (
        SELECT Q.ADVANCE_ID, P.REP_PERIOD_ID, P.ACCOUNT_ID, Q.ADVANCE_NO, Q.ADVANCE_DATE, P.RECVD,   
               P.RECVD - NVL(SUM(T.TRANSFER_TOTAL),0) ADVANCE
          FROM ADVANCE_QUEUE_T Q, PAYMENT_T P, PAY_TRANSFER_T T  
         WHERE Q.TASK_ID       = p_task_id
           AND P.PAYMENT_ID    = Q.PAYMENT_ID
           AND P.REP_PERIOD_ID = Q.PAY_PERIOD_ID
           AND P.ADVANCE_ID IS NULL
           AND P.PAYMENT_ID    = T.PAYMENT_ID(+)
           AND T.REP_PERIOD_ID(+) <= T.PAY_PERIOD_ID(+)
         GROUP BY Q.ADVANCE_ID, P.REP_PERIOD_ID, P.ACCOUNT_ID, Q.ADVANCE_NO, Q.ADVANCE_DATE, P.RECVD
         HAVING P.RECVD > NVL(SUM(T.TRANSFER_TOTAL),0)
    )
    SELECT PA.ADVANCE_ID, PA.REP_PERIOD_ID, PA.ACCOUNT_ID, PA.ADVANCE_NO, PA.ADVANCE_DATE, 
           'READY' ADVANCE_STATUS, 'P' ADVANCE_TYPE,
           A.CURRENCY_ID, PA.ADVANCE TOTAL, 
           PK09_INVOICE.CALC_GROSS(PA.ADVANCE, 'Y', AP.VAT) GROSS, 
           PK09_INVOICE.CALC_TAX(PA.ADVANCE, 'Y', AP.VAT) TAX,
           AP.VAT, SYSDATE CREATE_DATE, AP.PROFILE_ID
      FROM PA, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
     WHERE PA.ACCOUNT_ID = A.ACCOUNT_ID
       AND PA.ACCOUNT_ID = AP.ACCOUNT_ID
       AND AP.ACTUAL     = 'Y'; 
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ADVANCE_T - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- проставляем в платежах ссылки на счета-фактуры на аванс
    --
    MERGE INTO PAYMENT_T P
    USING (
        SELECT Q.PAY_PERIOD_ID, Q.PAYMENT_ID, 
               AV.ADVANCE_ID, AV.TOTAL, AV.ADVANCE_DATE 
          FROM ADVANCE_QUEUE_T Q, ADVANCE_T AV
         WHERE Q.TASK_ID = p_task_id
           AND Q.ADVANCE_ID = AV.ADVANCE_ID
    ) QA
    ON (
        P.REP_PERIOD_ID = QA.PAY_PERIOD_ID AND
        P.PAYMENT_ID    = QA.PAYMENT_ID
    )
    WHEN MATCHED THEN UPDATE 
                         SET P.ADVANCE      = QA.TOTAL, 
                             P.ADVANCE_DATE = QA.ADVANCE_DATE,
                             P.ADVANCE_ID   = QA.ADVANCE_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAYMENT_T - '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ======================================================================== --
-- Формирование счетов-фактур на аванс
-- ======================================================================== --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Сформировать аванс для указанного платежа
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Make_payment_advance(
             p_period_id  IN INTEGER,
             p_payment_id IN INTEGER
          )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Make_payment_advance';
    v_task_id     INTEGER;
BEGIN
    -- получить id задачи
    v_task_id := Open_task;
    
    -- Поставить в очередь платеж
    Add_payment_to_queue (
            p_task_id      => v_task_id,
            p_payment_id   => p_payment_id,
            p_period_id    => p_period_id,
            p_advance_date => NULL,
            p_advance_no   => NULL
        ); 

    -- создать авансовый счет-фактуру для платежа стоящих в очереди
    Make_advance( v_task_id );

    -- очистить временную таблицу
    Close_task( v_task_id );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Payment_id = '||p_payment_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Сформировать аванс для всех платежей указанного периода
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Make_period_advance(
             p_period_id  IN INTEGER
          )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Make_period_advance';
    v_task_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- получить id задачи
    v_task_id := Open_task;
    
    -- Поставить в очередь платеж
    Add_period_payments_to_queue (
        p_task_id   => v_task_id,
        p_period_id => p_period_id
    );

    -- создать авансовые счет-фактуры для платежей стоящих в очереди
    Make_advance( v_task_id );

    -- очистить временную таблицу
    Close_task( v_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Period_id = '||v_task_id, c_PkgName||'.'||v_prcName );
END;

END PK10_PAYMENT_ADVANCE;
/
