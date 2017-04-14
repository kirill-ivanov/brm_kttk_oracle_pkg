CREATE OR REPLACE PACKAGE PK30_BILLING_PERSONS
IS
    --
    -- Пакет для поддержки процесса выставления счетов и закрытия периода
    -- биллинга BRM-KTTK ( и импортированных данных из "Микротест" )
    -- для клиентов Физ.лиц
    -- Закрываются только счета стоящие в очереди: BILLING_QUEUE_T
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK30_BILLING_PERSONS';
    -- ==============================================================================
    c_TASK_ID   constant integer := Pk00_Const.c_BILLING_MMTS;
    
    type t_refc is ref cursor;

    -- ------------------------------------------------------------------------- --
    -- В Ы Б О Р   Б И Л Л И Н Г А
    -- ------------------------------------------------------------------------- --
    -- заполнить очередь на выставление счетов
    PROCEDURE Mark_bills( p_period_id IN INTEGER );
   
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- Разнести все что осталось на платежах за закрываемый период на сформированные счета
    -- разноска внутри л/с по позициям баланс Л/С не изменяет
    -- разноска для 'Ф' автоматическая FIFO, 'Ю'-ручная через АРМ платежей
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Payment_processing( p_bill_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Закрытие биллингового периода
    PROCEDURE Close_period( p_period_id IN INTEGER );

    
END PK30_BILLING_PERSONS;
/
CREATE OR REPLACE PACKAGE BODY PK30_BILLING_PERSONS
IS

-- ------------------------------------------------------------------------- --
-- поставить в очередь на выставление счетов клиентов физ.лиц
-- ПРИМЕЧАНИЕ:
-- что бы не было путаницы, всех физ.лиц ведем в биллинге ММТС "Микротест" (2003),
-- т.к. по согласованию с ДРУ BRM_KTTK (2001) работает по правилам 
-- "старого" биллинга (2002), т.е. без учета платежей и ведения балансов
-- ------------------------------------------------------------------------- --
PROCEDURE Mark_bills( p_period_id IN INTEGER )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Mark_bills';
    v_count     INTEGER;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
    -- Новый биллинг и BRM-KTTK (на всякий случай, потом уберу)
    INSERT INTO BILLING_QUEUE_T(BILL_ID, ACCOUNT_ID, BILLING_ID, TASK_ID)
    SELECT B.BILL_ID, B.ACCOUNT_ID, A.BILLING_ID, c_TASK_ID 
      FROM BILL_T B, ACCOUNT_T A
     WHERE B.REP_PERIOD_ID= p_period_id
       AND A.ACCOUNT_ID   = B.ACCOUNT_ID
       AND A.BILLING_ID   = Pk00_Const.c_BILLING_MMTS
       AND A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_P
       AND A.STATUS       = Pk00_Const.c_ACC_STATUS_BILL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILLING_QUEUE_T');
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Разнести все что осталось на платежах за предбиллинговые и биллинговый периоды 
-- на сформированные счета
-- разноска внутри л/с по позициям баланс Л/С не изменяет
-- разноска для 'Ф' автоматическая FIFO, 'Ю'-ручная через АРМ платежей
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Payment_processing( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Payment_processing';
    v_pay_due    NUMBER;     -- остаток на платежне после разноски
    v_ok         INTEGER;    
    v_err        INTEGER;
    v_prev_period_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id <= '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- Разносим все платежи методом FIFO (быстрая версия)
    PK10_PAYMENTS_TRANSFER.Method_fifo;

    -- Разносим методом FIFO платежи Физических лиц (медленная версия, образец)
    --Pk10_Payment.Payment_processing_fifo(p_from_period_id => p_bill_period_id);
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Закрытие биллингового периода
--
PROCEDURE Close_period(p_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_period';
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Устанавливаем пользовательскую блокировку:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- поставить в очередь на выставление счетов клиентов физ.лиц
    -- BRM_KTTK, включая клиентов импортированных из биллинга ММТС "Микротест"
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Mark_bills( p_period_id );
     
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Начисление абонентской платы и доплаты до минимальной суммы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Charge_fixrates( p_period_id,  c_TASK_ID);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Формирование счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Make_bills(p_period_id, c_TASK_ID);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- Разнести все что осталось на платежах за закрываемый период на сформированные счета
    -- разноска внутри л/с по позициям баланс Л/С не изменяет
    -- разноска для 'Ф' автоматическая FIFO, 'Ю'-ручная через АРМ платежей
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Payment_processing( p_period_id );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассчитать оборты за период для всех лицевых счетов
    -- (нужны для печати квитанций физ.лицам)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Period_info( p_period_id, c_TASK_ID );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Пересчитать балансы, всех лицевых счетов (счета READY - входят в баланс)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Refresh_balance( c_TASK_ID );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Снимаем пользовательскую блокировку:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        -- Снимаем пользовательскую блокировку:
        -- ...
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


END PK30_BILLING_PERSONS;
/
