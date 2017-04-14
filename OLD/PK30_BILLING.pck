CREATE OR REPLACE PACKAGE PK30_BILLING
IS
    --
    -- Пакет для поддержки процесса выставления счетов и закрытия периода
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK30_BILLING';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Порядок проведения закрытия периода:
    -- 1) Next_Bill_Period - выполняется в 00:01 UTS - каждого первого чила месяца
    -- 2) Billing - запускается в 02:00 UTS (по окончании загрузки и тарификации остатков трафика за предыдущий месяц)
    -- 3) Счета в состоянии 'READY' - выверяются, печатаются, отправляются клиентам
    -- 4) Close_Financial_Period - где-то 6-7 числа закрывается фин. период, при этом:
    -- 4.1) Pk04_Period.Close_fin_period - закрыть финансовый период: FIN_PERIOD = BILL_PERIOD_LAST    
    -- 4.2) закрыть готовые счета прошлого периода (запретить в них все изменения)
    -- 4.3) Calc_advance - Расчитать авансы по платежам на начало следующего месяца
    -- 4.4) Period_info - Рассчитать оборты за период для всех лицевых счетов
    -- 4.5) Refresh_balance - Пересчитать балансы, всех лицевых счетов
    -- В закрытых периодах, 
    --   * проведение изменения счетов, 
    --   * позиций счетов, 
    --   * удаление операций разноски платежей 
    --   * изменение авансовой составляющей счета 
    -- НЕВОЗМОЖНЫ!!!
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Переход биллинга на следующий биллинговый период (переход через месяц)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Next_Bill_Period;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Процедура формирования счетов для всех биллингов, обычно выполняю по частям
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Billing( p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Подготовительные операции (выполнять ОБЯЗАТЕЛЬНО!!!)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Begin_Billing( p_bill_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Заключительные операции
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE End_Billing( p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Перевести счета в статус ГОТОВ к печати (READY) из статуса ПРОВЕРКА (CHECK)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Set_billstatus_ready( p_bill_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- поставить в очередь на выставление счетов 
    -- клиентов Физ. лиц
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_person_bills( p_period_id  IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- поставить в очередь на выставление счетов 
    -- клиентов Юр.лиц биллинга ММТС (Микротест)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_MMTS_bills( p_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- поставить в очередь на выставление счетов 
    -- клиентов Юр.лиц биллинга КТТК (PORTAL 6.5 + новые)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_KTTK_bills( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- поставить в очередь на выставление счетов 
    -- клиентов Юр.лиц филиала в СПб 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_SPB_bills( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- поставить в очередь на выставление счетов 
    -- клиентов Юр.лиц Новтелекома (блок Доступ) 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_NTK_bills( p_period_id IN INTEGER );
    
    -- ------------------------------------------------------------------------- --
    -- Применить групповые скидки 
    -- ------------------------------------------------------------------------- --
    PROCEDURE Make_discounts( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Включение в счета компенсаций за простои 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Downtime_processing( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Закрытие финансового периода
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Close_Financial_Period;

    
    -- ------------------------------------------------------------------------- --
    --                   С Л У Ж Е Б Н Ы Е   П Р О Ц Е Д У Р Ы
    -- ------------------------------------------------------------------------- --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- получить закрываемый период (берем предыдущий незакрытый период) 
    --
    FUNCTION Period_for_close RETURN INTEGER;
    
    -- ------------------------------------------------------------------------- --
    -- Формирование счетов для клиентов Физ.лиц
    -- ------------------------------------------------------------------------- --
    PROCEDURE Billing_person( p_task_id IN INTEGER DEFAULT Pk00_Const.c_BILLING_MMTS );
                              
    -- ------------------------------------------------------------------------- --
    -- Формирование счетов для клиентов Юр.лиц с учетом баланса в биллинге
    -- ------------------------------------------------------------------------- --
    PROCEDURE Billing_jur_balance( p_task_id IN INTEGER DEFAULT Pk00_Const.c_BILLING_MMTS );
                                   
    -- ------------------------------------------------------------------------- --
    -- Формирование счетов для клиентов Юр.лиц БЕЗ учета баланса в биллинге
    -- ------------------------------------------------------------------------- --
    PROCEDURE Billing_jur( p_task_id IN INTEGER );                 
    
    -- ------------------------------------------------------------------------- --
    -- для каждого лицевого счета должен быть создана запись в BILLINFO_T
    -- досоздаем записи описателей счетов
    -- ------------------------------------------------------------------------- --
    PROCEDURE Check_Billinfo;
    
    -- ------------------------------------------------------------------------- --
    -- для счетов выставленных по договору местного присоединения 
    -- строки счетов фактур формируются по правилу 7704,
    -- группируются по компонентам услуг
    -- Это правило общее, поэтому заполняем автоматически
    -- Со временем уберем
    -- ------------------------------------------------------------------------- --
    PROCEDURE Check_invoice_rule( p_bill_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расчитать авансы по платежам на начало следующего месяца
    -- На текущий момент авансом считается сумма платежа, которая не пошла 
    -- на покрытие счетов выставленных, в период платежа или более ранние периоды 
    --
    PROCEDURE Calc_advance( p_bill_period_id IN INTEGER );
    
    -- ===================================================================== --
    --              Р А Б О Т А    Н А Д    О Ш И Б К А М И                  --
    -- ===================================================================== --
    -- Расформирование счетов, без удаления фиксированных начислений 
    PROCEDURE Rollback_bills( p_task_id IN INTEGER );

    -- Перефомирование счетов, без проведения начислений
    PROCEDURE Remake_bills( p_task_id IN INTEGER );

    -- Удаление операций разноски платежей, для биллинга ММТС
    PROCEDURE Rollback_paytransfer( p_task_id IN INTEGER );

    -- Расформирование счетов и удаление позиций фиксированных начислений
    PROCEDURE Rollback_billing( p_task_id IN INTEGER );

    -- Переначисление счетов, с повторным проведением фиксированных начислений
    PROCEDURE Remake_billing( p_task_id IN INTEGER );

   
END PK30_BILLING;
/
CREATE OR REPLACE PACKAGE BODY PK30_BILLING
IS

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

-- ----------------------------------------------------------------------- --
--                               Б И Л Л И Н Г
-- ----------------------------------------------------------------------- --
-- Процедура формирования счетов для всех биллингов,
-- обычно выполняю по частям
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Billing( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Begin_Billing';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- Подготовительные операции (выполнять ОБЯЗАТЕЛЬНО!!!)
    Begin_Billing( p_bill_period_id );

    -- сформировать счета клиентов Физ. лиц
    Make_person_bills( p_bill_period_id );
    
    -- сформировать счета клиентов Юр.лиц биллинга ММТС (Микротест)
    Make_MMTS_bills( p_bill_period_id );

    -- сформировать счета клиентов Юр.лиц биллинга КТТК (PORTAL 6.5 + новые)
    Make_KTTK_bills( p_bill_period_id );

    -- сформировать счета клиентов Юр.лиц филиала в СПб 
    Make_SPB_bills( p_bill_period_id );
    
    -- сформировать счета клиентов Юр.лиц Новтелекома (блок Доступ) 
    Make_NTK_bills( p_bill_period_id );

    -- Включение в счета компенсаций за простои 
    Downtime_processing( p_period_id => p_bill_period_id );

    -- расчет групповых скидок
    Make_discounts( p_bill_period_id );

    -- Заключительные операции
    End_Billing( p_bill_period_id );
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Подготовительные операции (выполнять ОБЯЗАТЕЛЬНО!!!)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Begin_Billing( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Begin_Billing';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- для каждого лицевого счета должен быть создана запись в BILLINFO_T
    -- досоздаем записи описателей счетов
    Check_Billinfo;
    
    -- для счетов выставленных по договору местного присоединения 
    -- строки счетов фактур формируются по правилу 7704,
    -- группируются по компонентам услуг
    -- Это правило общее, поэтому заполняем автоматически
    -- Со временем уберем
    Check_invoice_rule( p_bill_period_id => p_bill_period_id );
    
    --    
    -- Исправляем флаг, включения налогов в начисления
    -- до тех пор пока есть экспорт - это актуально
    PK30_BILLING_BASE.Correct_tax_incl( p_period_id => p_bill_period_id );
    --
    -- Проверяем/Исправляем коды регионов в счетах
    PK30_BILLING_BASE.Correct_region_bill( p_period_id  => p_bill_period_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создание периодических счетов для клиентов имеющих
    -- абонплату или доплату до минимальной стоимости
    -- в биллинговом периоде p_period_id
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK36_BILLING_FIXRATE.Make_bills_for_fixrates(p_period_id => p_bill_period_id );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Загрузка BDR из КСАД и формирование Item-ов
    PK24_CCAD.Load_BDRs(p_period_id => p_bill_period_id);
    --
   
    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
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
-- Заключительные операции
-- Полные пересчеты - временная мера, до окончания миграции из PORTAL6.5
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE End_Billing( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'End_Billing';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассчитать оборты за период для всех лицевых счетов
    -- Полный пересчет оборотов, до указанного периода включительно
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK34_BILLING_UNOFFICIAL.Recalc_all_period_info ( p_bill_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Пересчитать балансы, всех лицевых счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK34_BILLING_UNOFFICIAL.Recalc_all_balances;
    --
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Перевести счета в статус ГОТОВ к печати (READY) из статуса ПРОВЕРКА (CHECK)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_billstatus_ready( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_billstate_ready';
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- Переводим сформированные и счета из статуса CHECK в READY
    UPDATE BILL_T B  
       SET B.BILL_STATUS = Pk00_Const.c_BILL_STATE_READY 
    WHERE B.BILL_STATUS  = Pk00_Const.c_BILL_STATE_CHECK
      AND B.REP_PERIOD_ID = p_bill_period_id
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||' - bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- сформировать счета клиентов Физ. лиц
-- ------------------------------------------------------------------------- --
PROCEDURE Make_person_bills( p_period_id  IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_person_bills';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- Формируем счета
    Billing_person ( p_task_id   => Pk00_Const.c_BILLING_MMTS );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- сформировать счета клиентов Юр.лиц биллинга ММТС (Микротест)
-- ------------------------------------------------------------------------- --
PROCEDURE Make_MMTS_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_MMTS_bills';
    v_task_id    CONSTANT INTEGER := Pk00_Const.c_BILLING_MMTS;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
    -- Ставим в очередь на формирование счета Юр.лиц биллинга ММТС (Микротест)
    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => PK00_CONST.c_BILLING_MMTS, 
                                  p_task_id      => PK00_CONST.c_BILLING_MMTS,
                                  p_account_type => PK00_CONST.c_ACC_TYPE_J);
    -- Формируем счета
    Billing_jur_balance( p_task_id  =>  v_task_id);
    
    -- Перевести счета в статус проверка (CHECK) - примассовом выставлении счетов
    Pk30_Billing_Base.Set_billstatus_check( p_task_id => v_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- сформировать счета клиентов Юр.лиц биллинга КТТК (PORTAL 6.5 + новые)
-- ------------------------------------------------------------------------- --
PROCEDURE Make_KTTK_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_KTTK_bills';
    v_task_id    CONSTANT INTEGER := Pk00_Const.c_BILLING_KTTK;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
    -- клиенты "старого" биллинга (PORTAL 6.5)
    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => PK00_CONST.c_BILLING_OLD,
                                  p_task_id      => PK00_CONST.c_BILLING_KTTK, 
                                  p_account_type => PK00_CONST.c_ACC_TYPE_J);
    -- клиенты введенные непосредственно в BRM
    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => PK00_CONST.c_BILLING_KTTK, 
                                  p_task_id      => PK00_CONST.c_BILLING_KTTK,
                                  p_account_type => PK00_CONST.c_ACC_TYPE_J);

    -- Формируем счета
    Billing_jur( p_task_id   => v_task_id );

    -- Перевести счета в статус проверка (CHECK) - примассовом выставлении счетов
    Pk30_Billing_Base.Set_billstatus_check( p_task_id => v_task_id );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- сформировать счета клиентов Юр.лиц филиала в СПб 
-- ------------------------------------------------------------------------- --
PROCEDURE Make_SPB_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_SPB_bills';
    v_task_id    CONSTANT INTEGER := Pk00_Const.c_BILLING_SPB;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';

    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => PK00_CONST.c_BILLING_SPB,
                                  p_task_id      => PK00_CONST.c_BILLING_SPB, 
                                  p_account_type => PK00_CONST.c_ACC_TYPE_J);
    -- Формируем счета
    Billing_jur( p_task_id   => v_task_id );
    
    -- Перевести счета в статус проверка (CHECK) - примассовом выставлении счетов
    Pk30_Billing_Base.Set_billstatus_check( p_task_id => v_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- сформировать счета клиентов Юр.лиц Новтелекома (блок Доступ) 
-- ------------------------------------------------------------------------- --
PROCEDURE Make_NTK_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_NTK_bills';
    v_task_id    CONSTANT INTEGER := Pk00_Const.c_BILLING_ACCESS;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';

    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => PK00_CONST.c_BILLING_ACCESS, 
                                  p_task_id      => PK00_CONST.c_BILLING_ACCESS,
                                  p_account_type => PK00_CONST.c_ACC_TYPE_J);
    
    -- Формируем счета
    Billing_jur( p_task_id   => v_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- Применить групповые скидки 
-- ------------------------------------------------------------------------- --
PROCEDURE Make_discounts( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_discounts';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- формирование позиций скидок    
    PK39_BILLING_DISCOUNT.Apply_discounts( p_period_id => p_period_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------- --
-- Включение в счета компенсаций за простои 
-- ---------------------------------------------------------------------- --
PROCEDURE Downtime_processing( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_Financial_Period';
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    PK38_BILLING_DOWNTIME.Downtime_processing( p_period_id );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
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
    Calc_advance( v_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассчитать оборты за период для всех лицевых счетов
    -- Полный пересчет оборотов, до указанного периода включительно
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk34_Billing_Unofficial.Recalc_all_period_info ( v_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Пересчитать балансы, всех лицевых счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk34_Billing_Unofficial.Recalc_all_balances;
    --
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
--                   С Л У Ж Е Б Н Ы Е   П Р О Ц Е Д У Р Ы
-- ------------------------------------------------------------------------- --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
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

-- ------------------------------------------------------------------------- --
-- Формирование счетов для клиентов Физ.лиц
-- ------------------------------------------------------------------------- --
PROCEDURE Billing_person( p_task_id IN INTEGER DEFAULT Pk00_Const.c_BILLING_MMTS )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing_person';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Начисление абонентской платы и доплаты до минимальной суммы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Charge_fixrates( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Формирование счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Make_bills( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- Разнести все что осталось на платежах за закрываемый период на сформированные счета
    -- разноска внутри л/с по позициям баланс Л/С не изменяет
    -- разноска для 'Ф' автоматическая FIFO, 'Ю'-ручная через АРМ платежей
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK10_PAYMENTS_TRANSFER.Method_fifo;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расчитать авансы по платежам на начало следующего месяца
    -- На текущий момент авансом считается сумма платежа, которая не пошла 
    -- на покрытие счетов выставленных, в период платежа или более ранние периоды 
    --
    --PK30_BILLING_BASE.Calc_advance( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассчитать оборты за период для всех лицевых счетов
    -- Полный пересчет оборотов, до указанного периода включительно
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --PK34_BILLING_UNOFFICIAL.Recalc_all_period_info ( p_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Пересчитать балансы, всех лицевых счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --PK34_BILLING_UNOFFICIAL.Recalc_all_balances;
    --
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- Формирование счетов для клиентов Юр.лиц с учетом баланса в биллинге
-- ------------------------------------------------------------------------- --
PROCEDURE Billing_jur_balance( p_task_id IN INTEGER DEFAULT Pk00_Const.c_BILLING_MMTS )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing_jur_balance';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Начисление абонентской платы и доплаты до минимальной суммы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Charge_fixrates( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Формирование счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Make_bills( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Финальные процедуры закрытия периода (здесь менее строго чем у физ.лиц)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Close_period( p_task_id );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- 
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- Формирование счетов для клиентов Юр.лиц БЕЗ учета баланса в биллинге
-- ------------------------------------------------------------------------- --
PROCEDURE Billing_jur( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing_jur';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Начисление абонентской платы и доплаты до минимальной суммы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Charge_fixrates( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Формирование счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Make_bills( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассчитать оборты за период для л/с задачи, имеющих счета
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Period_info( p_task_id ); 
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- для каждого лицевого счета должен быть создана запись в BILLINFO_T
-- досоздаем записи описателей счетов
-- ------------------------------------------------------------------------- --
PROCEDURE Check_Billinfo
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_Billinfo';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    MERGE INTO BILLINFO_T BI
    USING (
        SELECT A.ACCOUNT_ID, A.CURRENCY_ID 
          FROM ACCOUNT_T A
         WHERE A.STATUS = PK00_CONST.c_ACC_STATUS_BILL
    ) AA
    ON ( AA.ACCOUNT_ID = BI.ACCOUNT_ID)
    WHEN NOT MATCHED THEN 
        INSERT (
            ACCOUNT_ID,      -- ID лицевого счета
            PERIOD_LENGTH,
            CURRENCY_ID,
            DAYS_FOR_PAYMENT
        ) VALUES (
            AA.ACCOUNT_ID,
            1,
            AA.CURRENCY_ID,
            NULL    -- по умолчанию месяц    
        );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLINFO_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- для счетов выставленных по договору местного присоединения 
-- строки счетов фактур формируются по правилу 7704,
-- группируются по компонентам услуг
-- Это правило общее, поэтому заполняем автоматически
-- Со временем уберем
-- ------------------------------------------------------------------------- --
PROCEDURE Check_invoice_rule( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_invoice_rule';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- проставляем правила для счетов
    UPDATE BILL_T B
       SET B.INVOICE_RULE_ID = Pk00_Const.c_INVOICE_RULE_SUB_STD
     WHERE EXISTS (
       SELECT * 
         FROM ORDER_T O
        WHERE O.ACCOUNT_ID = B.ACCOUNT_ID
          AND O.SERVICE_ID = Pk00_Const.c_SERVICE_OP_LOCAL -- 7
       )
       AND B.REP_PERIOD_ID    = p_bill_period_id
       AND B.BILL_TYPE        = Pk00_Const.c_BILL_TYPE_REC -- 'B';
       AND B.INVOICE_RULE_ID != Pk00_Const.c_INVOICE_RULE_SUB_STD
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- проставляем правила для л/с
    UPDATE BILLINFO_T BI
       SET BI.INVOICE_RULE_ID = Pk00_Const.c_INVOICE_RULE_SUB_STD
     WHERE EXISTS (
       SELECT * 
         FROM ORDER_T O
        WHERE O.ACCOUNT_ID = BI.ACCOUNT_ID
          AND O.SERVICE_ID = Pk00_Const.c_SERVICE_OP_LOCAL -- 7
       )
       AND BI.INVOICE_RULE_ID != Pk00_Const.c_INVOICE_RULE_SUB_STD
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLINFO_T '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
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
PROCEDURE Calc_advance( p_bill_period_id IN INTEGER )
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

-- --------------------------------------------------------------------- --
--              Р А Б О Т А    Н А Д    О Ш И Б К А М И                  --
--     обязательно завершается процедурой PK30_BILLING.END_BILLING       --
-- --------------------------------------------------------------------- --
-- Расформирование счетов, без удаления фиксированных начислений 
PROCEDURE Rollback_bills( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_bills';
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk30_Billing_Queue.Rollback_bills(p_task_id);
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Перефомирование счетов, без проведения начислений
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Remake_bills( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Remake_bills';
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk30_Billing_Base.Make_bills(p_task_id);
    
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расформирование счетов и удаление позиций фиксированных начислений
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_billing( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_billing';
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- расформировываем счета
    Pk30_Billing_Queue.Rollback_bills(p_task_id);
    -- удаляем фиксированные начисления - абонку и минималку
    Pk36_Billing_Fixrate.Rollback_fixrates(p_task_id);
    -- удаляем компенсации за простои
    Pk38_Billing_Downtime.Rollback_downtimes(p_task_id);
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Переначисление счетов, с повторным проведением фиксированных начислений
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Remake_billing( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Remake_billing';
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- проводим фиксированные начисления: абонку и минималку
    Pk36_Billing_Fixrate.Charge_fixrates(p_task_id);
    -- проводим перерасчет компенсации за просои
    Pk38_Billing_Downtime.Recharge_downtimes(p_task_id);
    -- формируем счета
    Pk30_Billing_Base.Make_bills(p_task_id);
    
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Удаление операций разноски платежей, для биллинга ММТС
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_paytransfer( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Remake_billing';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем все операции разноски
    FOR tr IN (
        SELECT T.TRANSFER_ID, T.PAY_PERIOD_ID, T.PAYMENT_ID 
          FROM PAY_TRANSFER_T T, BILLING_QUEUE_T Q
         WHERE Q.BILL_ID       = T.BILL_ID
           AND Q.REP_PERIOD_ID = T.REP_PERIOD_ID
           AND Q.TASK_ID       = p_task_id
      )
    LOOP
      PK10_PAYMENTS_TRANSFER.Delete_from_chain (
               p_pay_period_id => tr.pay_period_id,
               p_payment_id    => tr.payment_id,
               p_transfer_id   => tr.transfer_id
           );
       v_count := v_count + 1;
    END LOOP;

    Pk01_Syslog.Write_msg('pay_transfer_t '|| v_count ||' rows deleted',
                        c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;



END PK30_BILLING;
/
