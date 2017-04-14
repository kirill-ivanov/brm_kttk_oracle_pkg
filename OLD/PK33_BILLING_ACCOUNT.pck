CREATE OR REPLACE PACKAGE PK33_BILLING_ACCOUNT
IS
    --
    -- Пакет для поддержки процесса формирование/переформирования счетов
    -- для указанного лицевого счета
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK33_BILLING_ACCOUNT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- ===================================================================================
    -- Поставить счет в очередь на обработку
    -- ===================================================================================
    FUNCTION  push_Bill( 
                 p_bill_id        IN INTEGER,
                 p_bill_period_id IN INTEGER,
                 p_data_period_id IN INTEGER DEFAULT NULL
              ) RETURN INTEGER;

    -- ===================================================================================
    -- Перевыставление указанного счета, только в рамках биллингового периода
    -- ===================================================================================
    -- Расформироать счет биллингового периода
    --
    PROCEDURE Rollback_Bill(
                p_bill_id IN INTEGER,
                p_period_id IN INTEGER
              );

    -- ==================================================================================== --
    -- Формирование указанного счета
    -- ==================================================================================== --
    PROCEDURE Make_Bill(
                p_bill_id   IN INTEGER,
                p_period_id IN INTEGER
              );

    -- ===================================================================== --
    -- Работа с фиксированными начислениями абонка + трафик
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расформирование фиксированных начислений,
    -- за исключением тех что сформировал тарификатор трафика
    --
    PROCEDURE Rollback_fixrates(
                p_bill_id   IN INTEGER,
                p_period_id IN INTEGER
              );
        
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Начисление абонентской платы и доплаты до минимальной суммы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Charge_fixrates(
                p_bill_id        IN INTEGER,
                p_rep_period_id  IN INTEGER,
                p_data_period_id IN INTEGER
              );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассчитать оборты за период для лицевого счета
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Account_period_info (
                p_account_id     IN INTEGER,
                p_period_id      IN INTEGER
              );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- пересчет баланса л/с
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Refresh_balance (
              p_account_id     IN INTEGER
        );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Вернуть разнесенные на счет средства, обратно на платежи
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Delete_paytransfer_from_bill (
                p_bill_id    IN INTEGER,
                p_period_id  IN INTEGER
              );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Пересчет сформированного счета
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Recalc_bill (
                p_bill_id         IN INTEGER, -- ID дебет-ноты
                p_bill_period_id  IN INTEGER, -- ID расчетного периода дебет-ноты YYYYMM
                p_data_period_id  IN INTEGER  -- ID расчетного периода кредит-ноты YYYYMM
              );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Список правил формирования строк счтов-фактур, для указанного счета
    PROCEDURE Invoice_rules_list( 
                p_recordset IN OUT SYS_REFCURSOR,
                p_bill_id   IN INTEGER DEFAULT NULL -- если счет не задан получаем полный список
              );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Задать для счета правило формирования строк счтов-фактур
    PROCEDURE Set_bill_invoice_rule( 
                p_bill_id         IN INTEGER,
                p_bill_period_id  IN INTEGER,
                p_invoice_rule_id IN INTEGER  -- DICTIONARY_T(77)
              );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Задать для л/счета правило формирования строк счтов-фактур (тестирование)
    PROCEDURE Set_account_invoice_rule( 
                p_account_id      IN INTEGER,
                p_invoice_rule_id IN INTEGER  -- DICTIONARY_T(77)
              );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Исправляем флаг, включения налогов в начисления
    -- до тех пор пока работаем через экспорт - это актуально
    --
    PROCEDURE Correct_tax_incl( 
                p_bill_id         IN INTEGER,
                p_bill_period_id  IN INTEGER
              );
    
    
END PK33_BILLING_ACCOUNT;
/
CREATE OR REPLACE PACKAGE BODY PK33_BILLING_ACCOUNT
IS

-- ===================================================================================
-- Поставить счет в очередь на обработку
-- ===================================================================================
FUNCTION  push_Bill( 
             p_bill_id        IN INTEGER,
             p_bill_period_id IN INTEGER,
             p_data_period_id IN INTEGER DEFAULT NULL
          ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'push_Bill';
    v_task_id    INTEGER;
BEGIN
    --
    v_task_id := PK30_BILLING_QUEUE.Open_task;
    --    
    INSERT INTO BILLING_QUEUE_T (
           BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, 
           TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID
       )
    SELECT B.BILL_ID, B.ACCOUNT_ID, A.BILLING_ID, AP.PROFILE_ID, 
           v_task_id, B.REP_PERIOD_ID, NVL(p_data_period_id, p_bill_period_id)
      FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
     WHERE B.REP_PERIOD_ID = p_bill_period_id
       AND B.BILL_ID       = p_bill_id
       AND B.ACCOUNT_ID    = A.ACCOUNT_ID
       AND B.ACCOUNT_ID    = AP.ACCOUNT_ID
       AND AP.DATE_FROM   <= B.BILL_DATE
       AND (AP.DATE_TO IS NULL OR B.BILL_DATE <= AP.DATE_TO)
    ;
    RETURN v_task_id;
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ===================================================================================
-- Поставить л/счет в очередь на обработку
-- ===================================================================================
FUNCTION  push_Account( 
             p_account_id IN INTEGER,
             p_period_id  IN INTEGER
          ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'push_Account';
    v_task_id    INTEGER;
BEGIN
    --
    v_task_id := PK30_BILLING_QUEUE.Open_task;
    --    
    INSERT INTO BILLING_QUEUE_T(BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, TASK_ID, REP_PERIOD_ID)
    SELECT B.BILL_ID, B.ACCOUNT_ID, A.BILLING_ID, B.PROFILE_ID, v_task_id, B.REP_PERIOD_ID 
      FROM BILL_T B, ACCOUNT_T A
     WHERE B.REP_PERIOD_ID = p_period_id
       AND A.ACCOUNT_ID    = p_account_id
       AND B.ACCOUNT_ID    = A.ACCOUNT_ID
    ;
    RETURN v_task_id;
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


-- ===================================================================================
-- Перевыставление указанного счета, только в рамках биллингового периода
-- ===================================================================================
-- Расформироать счет биллингового периода
--
PROCEDURE Rollback_Bill(
             p_bill_id IN INTEGER,
             p_period_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_Bill';
    v_task_id    INTEGER;
    v_msg_id     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start bill_id = '||p_bill_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ставим счет в очередь на обработку
    v_task_id := push_Bill(p_bill_id, p_period_id);
    
    -- откатить счет
    Pk30_Billing_Queue.Rollback_bills(v_task_id );
    
    -- освобождаем очередь
    Pk30_Billing_Queue.Close_task(v_task_id);
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        v_msg_id := Pk01_Syslog.Fn_write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
        Pk30_Billing_Queue.Close_task(v_task_id);
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'msg_id='||v_msg_id||':'||c_PkgName||'.'||v_prcName);
END;

-- ==================================================================================== --
-- Формирование указанного счета
-- ==================================================================================== --
PROCEDURE Make_Bill(
            p_bill_id   IN INTEGER,
            p_period_id IN INTEGER
          )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Make_Bill';
    v_task_id     INTEGER;
    v_account_id  INTEGER;
    v_msg_id      INTEGER;
    v_balance     NUMBER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start bill_id = '||p_bill_id||', period_id = '||p_period_id, 
                                     c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- корректируем включение налога в сумму тарифа (на всякий случай)
    Correct_tax_incl(p_bill_id, p_period_id);

    -- ставим счет в очередь на обработку
    v_task_id := push_Bill(p_bill_id, p_period_id);

    --  Заполнить поля необходимые для получения детализации по абонплате
    Pk36_Billing_Fixrate.Put_ABP_detail( v_task_id );

    -- формируем счет
    Pk30_Billing_Queue.Close_bills(v_task_id );

    -- восстановить баланс лицевого счета
    SELECT B.ACCOUNT_ID
      INTO v_account_id
      FROM BILL_T B
     WHERE B.BILL_ID = p_bill_id
       AND B.REP_PERIOD_ID = p_period_id;
    
    v_balance := PK05_ACCOUNT_BALANCE.Refresh_balance(v_account_id);
    
    -- освобождаем очередь
    Pk30_Billing_Queue.Close_task(v_task_id);

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        v_msg_id := Pk01_Syslog.Fn_write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
        Pk30_Billing_Queue.Close_task(v_task_id);
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'msg_id='||v_msg_id||':'||c_PkgName||'.'||v_prcName);
END;

-- ===================================================================== --
-- Работа с фиксированными начислениями абонка + трафик
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расформирование фиксированных начислений,
-- за исключением тех что сформировал тарификатор трафика
--
PROCEDURE Rollback_fixrates(
            p_bill_id    IN INTEGER,
            p_period_id  IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_fixrates';
    v_task_id    INTEGER;
    v_msg_id     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ставим счет в очередь на обработку
    v_task_id := push_Bill(p_bill_id, p_period_id);
    
    Pk30_Billing_Queue.Rollback_fixrates(v_task_id );
    --
    -- освобождаем очередь
    Pk30_Billing_Queue.Close_task(v_task_id);
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        v_msg_id := Pk01_Syslog.Fn_write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
        Pk30_Billing_Queue.Close_task(v_task_id);
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'msg_id='||v_msg_id||':'||c_PkgName||'.'||v_prcName);
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Начисление абонентской платы и доплаты до минимальной суммы
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_fixrates(
            p_bill_id        IN INTEGER,
            p_rep_period_id  IN INTEGER,
            p_data_period_id IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Charge_fixrates';
    v_task_id    INTEGER;
    v_msg_id     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ставим счет в очередь на обработку
    v_task_id := push_Bill(p_bill_id, p_rep_period_id, p_data_period_id);
    
    Pk36_Billing_Fixrate.Charge_fixrates( v_task_id );

    -- освобождаем очередь
    Pk30_Billing_Queue.Close_task(v_task_id);
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        v_msg_id := Pk01_Syslog.Fn_write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
        Pk30_Billing_Queue.Close_task(v_task_id);
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'msg_id='||v_msg_id||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Рассчитать оборты за период для лицевого счета
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Account_period_info (
          p_account_id     IN INTEGER,
          p_period_id      IN INTEGER
    )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Account_period_info';
    v_task_id    INTEGER;
    v_msg_id     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ставим счет в очередь на обработку
    v_task_id := push_Account(p_account_id, p_period_id);

    -- Рассчитать оборты за период для всех лицевых счетов, где были обороты за месяц
    PK30_BILLING_BASE.Period_info( v_task_id );
    
    -- освобождаем очередь
    Pk30_Billing_Queue.Close_task(v_task_id);
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        v_msg_id := Pk01_Syslog.Fn_write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
        Pk30_Billing_Queue.Close_task(v_task_id);
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'msg_id='||v_msg_id||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- пересчет баланса л/с
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Refresh_balance (
          p_account_id     IN INTEGER
    )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Refresh_balance';
    v_balance    NUMBER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_balance := Pk05_Account_Balance.Refresh_balance ( p_account_id );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Вернуть разнесенные на счет средства, обратно на платежи
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Delete_paytransfer_from_bill (
               p_bill_id    IN INTEGER,
               p_period_id  IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_paytransfer_from_bill';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    PK10_PAYMENTS_TRANSFER.Delete_transfer_bill(p_period_id, p_bill_id);
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Пересчет сформированного счета
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Recalc_bill (
               p_bill_id         IN INTEGER, -- ID дебет-ноты
               p_bill_period_id  IN INTEGER, -- ID расчетного периода дебет-ноты YYYYMM
               p_data_period_id  IN INTEGER  -- ID расчетного периода кредит-ноты YYYYMM
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Recalc_bill';
    v_task_id  INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start dbt_bill_id = '||p_bill_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- корректируем включение налога в сумму тарифа (на всякий случай)
    Correct_tax_incl(p_bill_id, p_bill_period_id);

    -- ставим счет в очередь на обработку
    v_task_id := push_Bill(
                      p_bill_id        => p_bill_id,
                      p_bill_period_id => p_bill_period_id,
                      p_data_period_id => p_data_period_id );

    -- Вернуть разнесенные на счет средства, обратно на платежи
    Pk10_Payments_Transfer.Delete_transfer_bill(
                   p_period_id  => p_bill_period_id,
                   p_bill_id    => p_bill_id );
    
    -- расформировать счет, не трогая позиции счета
    Pk30_Billing_Queue.Rollback_bills(v_task_id );

    -- Расформирование фиксированных начислений,
    -- за исключением тех что сформировал тарификатор трафика
    Pk30_Billing_Queue.Rollback_fixrates(v_task_id );

    -- Начисление абонентской платы и доплаты до минимальной суммы
    Pk36_Billing_Fixrate.Charge_fixrates(v_task_id);

    -- формируем счет 
    Pk30_Billing_Queue.Close_bills(v_task_id );

    -- пересчитать баланс лицевого счета
--    Pk30_Billing_Base.Refresh_balance(v_task_id);

    -- освобождаем очередь
    Pk30_Billing_Queue.Close_task(v_task_id);
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Список правил формирования строк счтов-фактур, для указанного счета
--
PROCEDURE Invoice_rules_list( 
               p_recordset IN OUT SYS_REFCURSOR,
               p_bill_id   IN INTEGER DEFAULT NULL -- если счет не задан получаем полный список
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Invoice_rules_list';
    v_retcode    INTEGER;
    v_bill_type  VARCHAR2(1);
BEGIN
    -- проверяем это счет на местное присоединение
    SELECT MIN(B.BILL_TYPE), COUNT(*) 
      INTO v_bill_type, v_retcode
      FROM BILL_T B, ORDER_T O
     WHERE B.BILL_ID = p_bill_id
       AND B.ACCOUNT_ID = O.ACCOUNT_ID
       AND O.SERVICE_ID = Pk00_Const.c_SERVICE_OP_LOCAL;
    -- для не периодических счетов показываем все
    IF v_bill_type != Pk00_Const.c_BILL_TYPE_REC THEN
        PK09_INVOICE.Invoice_rules_list(p_recordset);
    ELSIF v_retcode > 0 THEN
        -- местное присоединения
        OPEN p_recordset FOR
            SELECT KEY_ID INVOICE_RULE_ID, KEY INVOICE_RULE_KEY, NAME INVOICE_RULE, NOTES 
              FROM DICTIONARY_T
             WHERE PARENT_ID = Pk00_Const.k_DICT_INV_RULE
               AND KEY_ID IN (
                  Pk00_Const.c_INVOICE_RULE_SUB_STD,
                  Pk00_Const.c_INVOICE_RULE_SUB_BIL,
                  Pk00_Const.c_INVOICE_RULE_SUB_EXT
               )
             ORDER BY 1;
    ELSE
        -- все остальные
        OPEN p_recordset FOR
            SELECT KEY_ID INVOICE_RULE_ID, KEY INVOICE_RULE_KEY, NAME INVOICE_RULE, NOTES 
              FROM DICTIONARY_T
             WHERE PARENT_ID = Pk00_Const.k_DICT_INV_RULE
               AND KEY_ID NOT IN (
                  Pk00_Const.c_INVOICE_RULE_SUB_STD,
                  Pk00_Const.c_INVOICE_RULE_SUB_BIL,
                  Pk00_Const.c_INVOICE_RULE_SUB_EXT
               )
             ORDER BY 1;
    END IF;
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := PIN.Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(PIN.Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Задать для счета правило формирования строк счтов-фактур
PROCEDURE Set_bill_invoice_rule( 
            p_bill_id         IN INTEGER,
            p_bill_period_id  IN INTEGER,
            p_invoice_rule_id IN INTEGER  -- DICTIONARY_T(77)
          )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Set_bill_invoice_rule';
BEGIN
    UPDATE BILL_T B
       SET B.INVOICE_RULE_ID = p_invoice_rule_id
     WHERE B.BILL_ID         = p_bill_id
       AND B.REP_PERIOD_ID   = p_bill_period_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Задать для л/счета правило формирования строк счтов-фактур (тестирование)
PROCEDURE Set_account_invoice_rule( 
            p_account_id      IN INTEGER,
            p_invoice_rule_id IN INTEGER  -- DICTIONARY_T(77)
          )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Set_account_invoice_rule';
BEGIN
    UPDATE BILLINFO_T BI
       SET BI.INVOICE_RULE_ID = p_invoice_rule_id
     WHERE BI.ACCOUNT_ID      = p_account_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Исправляем флаг, включения налогов в начисления
-- до тех пор пока работаем через экспорт - это актуально
--
PROCEDURE Correct_tax_incl( 
            p_bill_id         IN INTEGER,
            p_bill_period_id  IN INTEGER
          )
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Correct_tax_incl';
BEGIN
    -- налог для Ф - включен, для Ю - не включен
    MERGE INTO ITEM_T I
    USING (
      SELECT B.BILL_ID, B.REP_PERIOD_ID, 
             DECODE(A.ACCOUNT_TYPE,'P','Y','N') TAX_INCL
        FROM ACCOUNT_T A, BILL_T B
       WHERE A.ACCOUNT_ID = B.ACCOUNT_ID
         AND B.BILL_ID    = p_bill_id
         AND B.REP_PERIOD_ID = p_bill_period_id
    ) B
    ON (
       I.REP_PERIOD_ID = B.REP_PERIOD_ID AND
       I.BILL_ID = B.BILL_ID
    )   
    WHEN MATCHED THEN UPDATE SET I.TAX_INCL = B.TAX_INCL;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


END PK33_BILLING_ACCOUNT;
/
