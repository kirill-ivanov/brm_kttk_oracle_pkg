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
    -- Загрузка данных из внешних систем CCSAD и BillingServer 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Load_extern_data( p_bill_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Заключительные операции
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE End_Billing( p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Очистить биллинговый период от нулевых счетов (по необходимости)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Clean_billing( p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Перевести счета в статус ГОТОВ к печати (READY) из статуса ПРОВЕРКА (CHECK)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Set_billstatus_ready( p_bill_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- РЖД - сформировать счета клиентов Юр.лиц 
    -- MK001105 - зоновая связь  РЖД
    -- MS107643 - ЦСС РЖД
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_RZD_voice_bills( p_period_id IN INTEGER );
    
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
    -- сформировать счета клиентов Юр.лиц болка ДОСТУП: 
    -- Новтелекома и Телесот(Оренбург) 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_Access_bills( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- сформировать счета клиентов Юр.лиц регионов (РП блок Магистраль) 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_Region_bills( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- сформировать счета клиентов Юр.лиц регионов (РП - голос блок Магистраль) 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_RP_voice_bills( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- сформировать счета клиентов Юр.лиц  регионов (РП блок Магистраль) 
    -- для которых ведется баланс
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_RP_balance_bills( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- сформировать счета клиентов Юр.лиц, которых не нужно грузить в 1С  
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    -- ------------------------------------------------------------------------- --
    PROCEDURE Make_no1c_bills( p_period_id IN INTEGER );
    
    -- ------------------------------------------------------------------------- --
    -- сформировать счета клиентов Юр.лиц операторов, 
    -- по данным биллинг-сервера В.Малиновского 
    -- ------------------------------------------------------------------------- --
    PROCEDURE Make_BSRV_voice_bills( p_period_id IN INTEGER );
    
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
    -- проверка привязки открытых счетов к профилям
    -- ------------------------------------------------------------------------- --
    PROCEDURE Check_bill_profile;
    
    -- ------------------------------------------------------------------------- --
    -- для счетов выставленных по договору местного присоединения 
    -- строки счетов фактур формируются по правилу 7704,
    -- группируются по компонентам услуг
    -- Это правило общее, поэтому заполняем автоматически
    -- Со временем уберем
    -- ------------------------------------------------------------------------- --
    PROCEDURE Check_invoice_rule( p_bill_period_id IN INTEGER );
    
    -- ------------------------------------------------------------------------- --
    -- проверить наличие курса валюты в таблице CURRENCY_RATE_T 
    -- на дату выставления счетов (последний день месяца)
    -- ------------------------------------------------------------------------- --
    PROCEDURE Check_currency_rate( p_bill_period_id IN INTEGER );
    
    -- ------------------------------------------------------------------------- --
    -- Проверить поле CFO_ID, для заказов физ.лиц, 
    -- при отсутствии установить значения по умолчанию 3101 -> 'B2C'
    -- ------------------------------------------------------------------------- --
    PROCEDURE Check_CFO;
    
    -- ------------------------------------------------------------------------- --
    -- Удалить пустые счета 'DA%'  - 2000 биллинга
    -- ------------------------------------------------------------------------- --
    PROCEDURE Delete_empty_DA_bills( p_bill_period_id IN INTEGER );
    
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
    
    -- Откат групповых скидок для л/с указанной задачи
    PROCEDURE Rollback_discounts( p_task_id IN INTEGER );
    
    -- Перерасчет скидки для л/с указанной задачи
    PROCEDURE Remake_discounts( p_task_id IN INTEGER );

    -- ===================================================================== --
    --                           В Р Е М Е Н Н Ы Е                           --
    -- ===================================================================== --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Обновление invoice_items для всех счетов периода в у.е. на дату оплаты
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Refresh_invoice_cu( p_period_id IN INTEGER );

   
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
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- Подготовительные операции (выполнять ОБЯЗАТЕЛЬНО!!!)
    Begin_Billing( p_bill_period_id );
    commit;

    -- РЖД - сформировать счета клиентов Юр.лиц 
    -- MK001105 - зоновая связь  РЖД
    -- MS107643 - ЦСС РЖД
    Make_RZD_voice_bills( p_bill_period_id );
    commit;

    -- Загрузка данных из внешних систем CCSAD и BillingServer 
    Load_extern_data( p_bill_period_id );
    commit;

    -- 2003: сформировать счета клиентов Физ. лиц
    Make_person_bills( p_bill_period_id );
    commit;

    -- 2003: сформировать счета клиентов Юр.лиц биллинга ММТС (Микротест)
    Make_MMTS_bills( p_bill_period_id );
    commit;
    
    -- 2001,2002: сформировать счета клиентов Юр.лиц биллинга КТТК (PORTAL 6.5 + новые)
    Make_KTTK_bills( p_bill_period_id );
    commit;

    -- 2006: сформировать счета клиентов Юр.лиц филиала в СПб 
    Make_SPB_bills( p_bill_period_id );
    commit;
    
    -- 2004: сформировать счета клиентов Юр.лиц болка ДОСТУП: Новтелекома и Телесот(Оренбург)
    Make_Access_bills( p_bill_period_id );
    commit;
    
    -- 2005: сформировать счета клиентов Юр.лиц регионов (РП - голос блок Магистраль) 
    Make_RP_voice_bills( p_bill_period_id );
    commit;
    
    -- 2007: сформировать счета клиентов Юр.лиц  регионов (РП блок Магистраль) 
    -- для которых ведется баланс
    Make_RP_balance_bills( p_bill_period_id );
    commit;

    -- 2008: сформировать счета клиентов Юр.лиц регионов (РП блок Магистраль) 
    Make_Region_bills( p_bill_period_id );
    commit;
    
    -- 2000: сформировать счета клиентов Юр.лиц, которых не нужно грузить в 1С  
    -- Была не формировать №754054
    --!!!--Make_no1c_bills( p_bill_period_id );
    --commit;
   
    -- 2009: сформировать счета клиентов Юр.лиц операторов, 
    -- по данным биллинг-сервера В.Малиновского 
    Make_BSRV_voice_bills( p_bill_period_id );
    commit;

    -- Включение в счета компенсаций за простои 
    Downtime_processing( p_bill_period_id );
    commit;
    
    -- расчет групповых скидок
    Make_discounts( p_bill_period_id );
    commit;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Обновление invoice_items для всех счетов периода в у.е. на дату оплаты
    -- ВРЕМЕННО, потом что-нибудь придумаю
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Refresh_invoice_cu( p_bill_period_id );
    commit;
       
    -- Заключительные операции
    End_Billing( p_bill_period_id );
    commit;
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
    
    -- проверка привязки открытых счетов к профилям
    Check_bill_profile;
    
    -- для счетов выставленных по договору местного присоединения 
    -- строки счетов фактур формируются по правилу 7704,
    -- группируются по компонентам услуг
    -- Это правило общее, поэтому заполняем автоматически
    -- Пока убрали, далее посмотрим
    --Check_invoice_rule( p_bill_period_id => p_bill_period_id );
   
    -- проверить наличие курса валюты в таблице CURRENCY_RATE_T 
    -- на дату выставления счетов (последний день месяца)
    Check_currency_rate( p_bill_period_id => p_bill_period_id );
    
    -- Проверить поле CFO_ID, для заказов физ.лиц, 
    -- при отсутствии установить значения по умолчанию 3101 -> 'B2C'
    Check_CFO;
    
    -- Удалить пустые счета 'DA%'  - 2000 биллинга
    Delete_empty_DA_bills( p_bill_period_id => p_bill_period_id );
    
    --    
    -- Исправляем флаг, включения налогов в начисления
    -- до тех пор пока есть экспорт - это актуально
    --PK30_BILLING_BASE.Correct_tax_incl( p_period_id => p_bill_period_id );
    --
    -- Проверяем/Исправляем коды регионов в счетах
    -- Перенес в Pk30_Billing_base.Make_bills!!!
    --PK30_BILLING_BASE.Correct_region_bill( p_period_id  => p_bill_period_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создание периодических счетов для клиентов имеющих
    -- абонплату или доплату до минимальной стоимости
    -- в биллинговом периоде p_period_id
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK36_BILLING_FIXRATE.Make_bills_for_fixrates(p_period_id => p_bill_period_id );
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
-- Загрузка данных из внешних систем CCSAD и BillingServer 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Load_extern_data( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Load_extern_data';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Загрузка BDR из КСАД и формирование Item-ов
    PK24_CCAD.Load_BDRs(p_period_id => p_bill_period_id);
    --
    -- Загрузка BDR из BillingServer и формирование Item-ов
    PK23_BILLSRV.Processing(p_period_id => p_bill_period_id);
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
    -- Переводим сформированные счета в статус проверка 
    -- статус упраздняем по просьбе ДРУ
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --UPDATE BILL_T B  
    --   SET B.BILL_STATUS = Pk00_Const.c_BILL_STATE_CHECK 
    --WHERE B.BILL_STATUS  = Pk00_Const.c_BILL_STATE_READY
    --  AND B.REP_PERIOD_ID = p_bill_period_id
    --  AND EXISTS (
    --     SELECT * FROM ACCOUNT_T A
    --      WHERE A.ACCOUNT_ID = B.ACCOUNT_ID
    --        AND A.BILLING_ID NOT IN (2007)
    --  );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Выгружаем отчет для BillingServer
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK23_BILLSRV.Report( p_bill_period_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассчитать оборты за период для всех лицевых счетов
    -- Полный пересчет оборотов, до указанного периода включительно
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK34_BILLING_UNOFFICIAL.Recalc_all_period_info;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Пересчитать балансы, всех лицевых счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK34_BILLING_UNOFFICIAL.Recalc_all_balances;
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Полный пересчет балансов, всех высталенных счтеов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK34_BILLING_UNOFFICIAL.Recalc_due_for_all_bills;
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Очистить биллинговый период от нулевых счетов (по необходимости)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Clean_billing( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Clean_billing';
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    DELETE 
      FROM ITEM_T I
     WHERE I.ITEM_TOTAL = 0
       AND I.REP_PERIOD_ID = p_bill_period_id
       AND I.ITEM_STATUS = 'READY'
       AND EXISTS (
            SELECT * 
              FROM BILL_T B
             WHERE B.TOTAL = 0
               AND B.REP_PERIOD_ID = p_bill_period_id
               AND B.BILL_TYPE     = 'B'
               AND B.BILL_STATUS   = 'READY'
               AND I.BILL_ID       = B.BILL_ID
               AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' - rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    DELETE 
      FROM INVOICE_ITEM_T V
     WHERE V.TOTAL = 0
       AND V.REP_PERIOD_ID = p_bill_period_id
       AND EXISTS (
            SELECT * 
              FROM BILL_T B
             WHERE B.TOTAL = 0
               AND B.REP_PERIOD_ID = p_bill_period_id
               AND B.BILL_TYPE = 'B'
               AND B.BILL_STATUS = 'READY'
               AND V.BILL_ID = B.BILL_ID
               AND V.REP_PERIOD_ID = B.REP_PERIOD_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Invoice_item_t: '||v_count||' - rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    DELETE 
      FROM BILL_T B
     WHERE B.TOTAL = 0
       AND B.REP_PERIOD_ID = p_bill_period_id
       AND B.BILL_TYPE = 'B'
       AND B.BILL_STATUS = 'READY';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Bill_t: '||v_count||' - rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
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
-- РЖД - сформировать счета клиентов Юр.лиц 
-- MK001105 - зоновая связь  РЖД
-- MS107643 - ЦСС РЖД
-- ------------------------------------------------------------------------- --
PROCEDURE Make_RZD_voice_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_RZD_voice_bills';
    v_task_id    CONSTANT INTEGER := 1;
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
    -- добавляем заказы в очередь
    INSERT INTO BILLING_QUEUE_T (BILL_ID, ACCOUNT_ID, TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID)
    SELECT BILL_ID, ACCOUNT_ID, v_task_id TASK_ID, REP_PERIOD_ID, REP_PERIOD_ID
      FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_period_id
       AND B.ACCOUNT_ID IN (
        2391604,    -- РЖД-зона
        1937070     -- ЦСС
       )
       AND B.BILL_STATUS = 'OPEN';
    v_count := SQL%ROWCOUNT;
    COMMIT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- для ЦСС устанавливаем правило формирования invoice_item (на всякий случай)
    UPDATE BILL_T B SET B.INVOICE_RULE_ID = 7707
     WHERE B.REP_PERIOD_ID = p_period_id
       AND B.ACCOUNT_ID = 1937070;

    -- Формируем счета
    Billing_jur( p_task_id   => v_task_id );

    -- чистим очередь
    DELETE FROM BILLING_QUEUE_T Q WHERE Q.TASK_ID = v_task_id; 
    
    -- формируем фиксированную скидку
    Pk39_Billing_Discount_Nonstd.DG_MS107643_RZD(p_period_id => p_period_id);
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- сформировать счета клиентов Физ. лиц
-- ------------------------------------------------------------------------- --
PROCEDURE Make_person_bills( p_period_id  IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_person_bills';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
    -- Ставим в очередь на формирование счета Физ.лиц
    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => PK00_CONST.c_BILLING_MMTS,
                                  p_task_id      => PK00_CONST.c_BILLING_MMTS,
                                  p_account_type => PK00_CONST.c_ACC_TYPE_P);
    -- Формируем счета
    Billing_person ( p_task_id   => Pk00_Const.c_BILLING_MMTS );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- Разнести все что осталось на платежах за закрываемый период на сформированные счета
    -- разноска внутри л/с по позициям баланс Л/С не изменяет
    -- разноска для 'Ф' автоматическая FIFO, 'Ю'-ручная через АРМ платежей
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK10_PAYMENTS_TRANSFER.Method_fifo;
    
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
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
    -- заполняем очередь клиентами двух биллингов
    -- клиенты "старого" биллинга (PORTAL 6.5)
    -- клиенты введенные непосредственно в BRM
    INSERT INTO BILLING_QUEUE_T (
        BILL_ID, ACCOUNT_ID, TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID
    )
    SELECT B.BILL_ID, B.ACCOUNT_ID, 
           v_task_id, p_period_id, p_period_id
      FROM BILL_T B, ACCOUNT_T A
     WHERE B.REP_PERIOD_ID = p_period_id
       AND A.ACCOUNT_ID    = B.ACCOUNT_ID
       AND A.BILLING_ID IN ( PK00_CONST.c_BILLING_OLD,
                             PK00_CONST.c_BILLING_KTTK )
       AND A.ACCOUNT_TYPE  = PK00_CONST.c_ACC_TYPE_J
       AND A.STATUS        = Pk00_Const.c_ACC_STATUS_BILL
       AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_OPEN)
    ;
    v_count := SQL%ROWCOUNT;
    COMMIT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILLING_QUEUE_T');

    -- Формируем счета
    Billing_jur( p_task_id   => v_task_id );

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
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- сформировать счета клиентов Юр.лиц болка ДОСТУП: Новтелекома и Телесот(Оренбург)
-- ------------------------------------------------------------------------- --
PROCEDURE Make_Access_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_Access_bills';
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
-- сформировать счета клиентов Юр.лиц регионов (РП блок Магистраль) 
-- ------------------------------------------------------------------------- --
PROCEDURE Make_Region_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_Region_bills';
    v_task_id    CONSTANT INTEGER := Pk00_Const.c_BILLING_RP; -- 2008
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';

    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => v_task_id, 
                                  p_task_id      => v_task_id,
                                  p_account_type => PK00_CONST.c_ACC_TYPE_J);
    
    -- Формируем счета
    Billing_jur( p_task_id   => v_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- сформировать счета клиентов Юр.лиц регионов (РП - голос блок Магистраль) 
-- ------------------------------------------------------------------------- --
PROCEDURE Make_RP_voice_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_RP_voice_bills';
    v_task_id    CONSTANT INTEGER := Pk00_Const.c_BILLING_RP_VOICE; -- 2005
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';

    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => v_task_id, 
                                  p_task_id      => v_task_id,
                                  p_account_type => PK00_CONST.c_ACC_TYPE_J);
    
    -- Формируем счета
    Billing_jur( p_task_id   => v_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- сформировать счета клиентов Юр.лиц  регионов (РП блок Магистраль) 
-- для которых ведется баланс
-- ------------------------------------------------------------------------- --
PROCEDURE Make_RP_balance_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_Region_bills';
    v_task_id    CONSTANT INTEGER := Pk00_Const.c_BILLING_RP_BALANCE; -- 2007
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';

    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => v_task_id, 
                                  p_task_id      => v_task_id,
                                  p_account_type => PK00_CONST.c_ACC_TYPE_J);
    
    -- Формируем счета и изменяем баланс л/с
    Billing_jur_balance( p_task_id   => v_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- ------------------------------------------------------------------------- --
-- сформировать счета клиентов Юр.лиц, которых не нужно грузить в 1С  
-- ------------------------------------------------------------------------- --
PROCEDURE Make_no1c_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_no1c_bills';
    v_task_id    CONSTANT INTEGER := Pk00_Const.c_BILLING_OLD_NO_1C;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';

    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => v_task_id, 
                                  p_task_id      => v_task_id,
                                  p_account_type => PK00_CONST.c_ACC_TYPE_J);
    
    -- Формируем счета
    Billing_jur( p_task_id   => v_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- сформировать счета клиентов Юр.лиц операторов, 
-- по данным биллинг-сервера В.Малиновского 
-- ------------------------------------------------------------------------- --
PROCEDURE Make_BSRV_voice_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_BSRV_voice_bills';
    v_task_id    CONSTANT INTEGER := 2009;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';

    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => v_task_id, 
                                  p_task_id      => v_task_id,
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

    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';

    -- формирование позиций скидок    
    PK39_BILLING_DISCOUNT.Apply_discounts( p_period_id => p_period_id );
    
    -- формирование позиций скидок для Beeline
    PK39_BILLING_DISCOUNT_BEE.Apply_discounts(p_period_id => p_period_id);
    
    -- формирование позиций нестандартных скидок
    PK39_BILLING_DISCOUNT_NONSTD.Apply_discounts(p_period_id => p_period_id);
    
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
    v_prcName    CONSTANT VARCHAR2(30) := 'Downtime_processing';
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- Чистим очередь 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';

    -- Расчет компенсаций за простои (BRM_DOWNTIME_T)
    PK38_BILLING_DOWNTIME.Period_processing( p_period_id );

    -- Coca-Cola - расчет компенсаций за простои, заданных коэффициентом (BRM_SLA_K_T S)
    PK38_BILLING_SLA_K.Period_processing( p_period_id );
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
    /*
    -- удалить нулевые счета у которых нет начислений
    DELETE FROM BILL_T B
     WHERE B.REP_PERIOD_ID = v_period_id
       AND B.BILL_STATUS IN ( 'READY', 'OPEN' )
       AND B.TOTAL = 0 
       AND NOT EXISTS (
            SELECT * FROM ITEM_T I
             WHERE I.REP_PERIOD_ID = B.REP_PERIOD_ID
               AND I.BILL_ID = B.BILL_ID
               AND I.ITEM_TOTAL != 0
       )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Удалено '||v_count||' пустых счетов', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    */
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
    UPDATE ITEM_T I
       SET ITEM_STATUS = Pk00_Const.c_ITEM_STATE_CLOSED
     WHERE REP_PERIOD_ID = v_period_id
       AND ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, Pk00_Const.c_ITEM_TYPE_ADJUST)
       AND ITEM_STATUS = Pk00_Const.c_ITEM_STATE_REАDY
       --AND NOT EXISTS (
       --    SELECT * 
       --      FROM ACCOUNT_PROFILE_T AP, BILL_T B
       --     WHERE AP.BRANCH_ID IN (298, 314)
       --       AND AP.ACCOUNT_ID = B.ACCOUNT_ID
       --       AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
       --       AND B.BILL_ID = I.BILL_ID
       --)
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Закрыто '||v_count||' позиций счетов', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- закрыть готовые счета прошлого периода (запретить в них все изменения)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE BILL_T B
       SET BILL_STATUS = Pk00_Const.c_BILL_STATE_CLOSED
     WHERE REP_PERIOD_ID = v_period_id
       AND BILL_STATUS IN ( Pk00_Const.c_BILL_STATE_READY )
       --AND NOT EXISTS (
       --    SELECT * 
       --      FROM ACCOUNT_PROFILE_T AP
       --     WHERE AP.BRANCH_ID IN (298, 314)
       --       AND AP.ACCOUNT_ID = B.ACCOUNT_ID
       --)
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Закрыто '||v_count||' счетов', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расчитать авансы по платежам на начало следующего месяца
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --Calc_advance( v_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассчитать оборты за период для всех лицевых счетов
    -- Полный пересчет оборотов, до указанного периода включительно
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk34_Billing_Unofficial.Recalc_all_period_info;

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
    -- Пересчет балансов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Refresh_balance( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- Разнести все что осталось на платежах за закрываемый период на сформированные счета
    -- разноска внутри л/с по позициям баланс Л/С не изменяет
    -- разноска для 'Ф' автоматическая FIFO, 'Ю'-ручная через АРМ платежей
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --PK10_PAYMENTS_TRANSFER.Method_fifo;
    
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
    -- Пересчет балансов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Refresh_balance( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расчитать авансы по платежам на начало следующего месяца
    -- На текущий момент авансом считается сумма платежа, которая не пошла 
    -- на покрытие счетов выставленных, в период платежа или более ранние периоды 
    --
    PK30_BILLING_BASE.Calc_advance( p_task_id ); 
    
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
    --PK30_BILLING_BASE.Period_info( p_task_id ); 
    
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
            DAYS_FOR_PAYMENT
        ) VALUES (
            AA.ACCOUNT_ID,
            1,
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

-- ----------------------------------------------------------------- --
-- проверка привязки открытых счетов к профилям
-- ----------------------------------------------------------------- --
PROCEDURE Check_bill_profile
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_bill_profile';
    v_count      INTEGER;
BEGIN
    MERGE INTO BILL_T B
    USING (
        SELECT B.BILL_ID, B.REP_PERIOD_ID, B.BILL_DATE, B.BILL_TYPE, 
               --B.PROFILE_ID, B.CONTRACT_ID, B.CONTRACTOR_ID, B.CONTRACTOR_BANK_ID,
               --AP.PROFILE_ID, AP.CONTRACT_ID, AP.CONTRACTOR_ID, AP.CONTRACTOR_BANK_ID
               APN.PROFILE_ID, APN.CONTRACT_ID, APN.CONTRACTOR_ID, APN.CONTRACTOR_BANK_ID 
          FROM BILL_T B, PERIOD_T P, ACCOUNT_PROFILE_T AP, ACCOUNT_PROFILE_T APN
         WHERE B.REP_PERIOD_ID = P.PERIOD_ID
           AND P.POSITION IN ('OPEN','BILL')
           AND B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN
           AND B.BILL_TYPE   = Pk00_Const.c_BILL_TYPE_REC  -- на всякий случай
           AND AP.ACCOUNT_ID = B.ACCOUNT_ID
           AND AP.DATE_FROM <= B.BILL_DATE
           AND (AP.DATE_TO IS NULL OR B.BILL_DATE <= AP.DATE_TO)
           AND (B.PROFILE_ID    != AP.PROFILE_ID
            OR  B.CONTRACT_ID   != AP.CONTRACT_ID
            OR  B.CONTRACTOR_ID != AP.CONTRACTOR_ID
            OR  B.CONTRACTOR_BANK_ID != AP.CONTRACTOR_BANK_ID
           )
           AND  B.ACCOUNT_ID   = APN.ACCOUNT_ID   
           AND  APN.DATE_FROM <= B.BILL_DATE
           AND (APN.DATE_TO IS NULL OR B.BILL_DATE <= APN.DATE_TO)
    ) AP
    ON (
      B.BILL_ID       = AP.BILL_ID AND 
      B.REP_PERIOD_ID = AP.REP_PERIOD_ID
    )
    WHEN MATCHED THEN UPDATE SET B.PROFILE_ID         = AP.PROFILE_ID, 
                                 B.CONTRACT_ID        = AP.CONTRACT_ID, 
                                 B.CONTRACTOR_ID      = AP.CONTRACTOR_ID,  
                                 B.CONTRACTOR_BANK_ID = AP.CONTRACTOR_BANK_ID
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T.PROFILE_ID was changed for '||v_count||' rows', v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
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

-- ------------------------------------------------------------------------- --
-- проверить наличие курса валюты в таблице CURRENCY_RATE_T 
-- на дату выставления счетов (последний день месяца)
-- ------------------------------------------------------------------------- --
PROCEDURE Check_currency_rate( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_currency_rate';
    v_usd        NUMBER;
    v_eur        NUMBER;
    v_bill_date  DATE;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- дата выставления периодических счетов 
    v_bill_date := TRUNC(Pk04_Period.Period_to(p_bill_period_id));

    -- курс доллара
    SELECT R.RATE_VALUE INTO v_usd 
      FROM CURRENCY_RATE_T R
     WHERE DATE_RATE   = v_bill_date
       AND CURRENCY_ID = Pk00_Const.c_CURRENCY_USD; -- 840 
    Pk01_Syslog.Write_msg('CURRENCY_RATE_T: USD/RUB = '||v_usd||' - '||TO_CHAR(v_bill_date,'dd.mm.yyyy'), c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- курс Евро
    SELECT R.RATE_VALUE INTO v_eur 
      FROM CURRENCY_RATE_T R
     WHERE DATE_RATE   = v_bill_date
       AND CURRENCY_ID = Pk00_Const.c_CURRENCY_EUR; -- 978
    Pk01_Syslog.Write_msg('CURRENCY_RATE_T: EUR/RUB = '||v_eur||' - '||TO_CHAR(v_bill_date,'dd.mm.yyyy'), c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN NO_DATA_FOUND THEN
      IF v_usd IS NULL THEN
        Pk01_Syslog.raise_Exception('CURRENCY_RATE_T: '||TO_CHAR(v_bill_date,'dd.mm.yyyy')||' - USD/RUB = NULL', c_PkgName||'.'||v_prcName );
      ELSIF v_eur IS NULL THEN
        Pk01_Syslog.raise_Exception('CURRENCY_RATE_T: '||TO_CHAR(v_bill_date,'dd.mm.yyyy')||' - EUR/RUB = NULL', c_PkgName||'.'||v_prcName );
      END IF;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- Проверить поле CFO_ID, для заказов физ.лиц, 
-- при отсутствии установить значения по умолчанию 3101 -> 'B2C'
-- ------------------------------------------------------------------------- --
PROCEDURE Check_CFO IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_CFO';
    v_count      INTEGER;
BEGIN  
    UPDATE ORDER_T O SET O.CFO_ID = 3101
     WHERE O.CFO_ID IS NULL
       AND EXISTS (
         SELECT * FROM ACCOUNT_T A
          WHERE A.ACCOUNT_ID = O.ACCOUNT_ID
            AND A.ACCOUNT_TYPE = 'P'
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_T.SFO_ID '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- Удалить пустые счета 'DA%'  - 2000 биллинга
-- ------------------------------------------------------------------------- --
PROCEDURE Delete_empty_DA_bills( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_empty_DA_bills';
    v_count      INTEGER;
BEGIN  
    DELETE FROM BILL_T B
     WHERE B.BILL_ID IN (
        SELECT B.BILL_ID 
          FROM ACCOUNT_T A, BILL_T B
         WHERE A.BILLING_ID    = 2000
           AND B.REP_PERIOD_ID = p_bill_period_id
           AND A.ACCOUNT_ID    = B.ACCOUNT_ID
           AND B.BILL_TYPE     = 'B'
           AND NOT EXISTS (
               SELECT * FROM ITEM_T I
                WHERE I.REP_PERIOD_ID = B.REP_PERIOD_ID
                  AND I.BILL_ID = B.BILL_ID
           )
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T.BILL_NO like "DA" - '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
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
    Pk30_Billing_Base.Rollback_bills(p_task_id);
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
    -- формируем счета
    Pk30_Billing_Base.Make_bills(p_task_id);
    -- пересчитываем балансы
    Pk30_Billing_Base.Refresh_balance(p_task_id);
    --    
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
    Pk30_Billing_Base.Rollback_bills(p_task_id);
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
    -- пересчитываем балансы
    Pk30_Billing_Base.Refresh_balance(p_task_id);
    
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Откат групповых скидок для л/с указанной задачи
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_discounts( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_discounts';
    v_count      INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    FOR dg IN (
      SELECT DISTINCT DG_ID, PERIOD_ID
        FROM DG_ACCOUNT_T DA, PERIOD_T P, BILLING_QUEUE_T Q
       WHERE Q.ACCOUNT_ID = DA.ACCOUNT_ID
         AND Q.REP_PERIOD_ID = P.PERIOD_ID
         AND Q.TASK_ID = p_task_id
         AND DA.DATE_FROM < P.PERIOD_TO
         AND (DA.DATE_TO IS NULL OR P.PERIOD_FROM < DA.DATE_TO )  
    )
    LOOP
        -- удаляем л/с группы из очереди
        DELETE FROM BILLING_QUEUE_T Q
         WHERE Q.TASK_ID = 1
           AND EXISTS (
                SELECT * FROM DG_ACCOUNT_T DA
                 WHERE DA.ACCOUNT_ID = Q.ACCOUNT_ID
           );  
    
        -- отменяем скидку для группы
        PK39_BILLING_DISCOUNT.Rollback_group_discount( dg.dg_id, dg.Period_id );
        v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg(v_count||' - std_discounts', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расчет скидки для л/с указанной задачи
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Remake_discounts( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Remake_discounts';
    v_count      INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    FOR dg IN (
      SELECT DISTINCT DG_ID, PERIOD_ID
        FROM DG_ACCOUNT_T DA, PERIOD_T P, BILLING_QUEUE_T Q
       WHERE Q.ACCOUNT_ID = DA.ACCOUNT_ID
         AND Q.REP_PERIOD_ID = P.PERIOD_ID
         AND Q.TASK_ID = p_task_id
         AND DA.DATE_FROM < P.PERIOD_TO
         AND (DA.DATE_TO IS NULL OR P.PERIOD_FROM < DA.DATE_TO )  
    )
    LOOP
        -- удаляем л/с группы из очереди
        DELETE FROM BILLING_QUEUE_T Q
         WHERE Q.TASK_ID = 1
           AND EXISTS (
                SELECT * FROM DG_ACCOUNT_T DA
                 WHERE DA.ACCOUNT_ID = Q.ACCOUNT_ID
           );
        -- применяем скидку для группы
        PK39_BILLING_DISCOUNT.Apply_group_discount( dg.dg_id, dg.period_id );
        v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg(v_count||' - std_discounts', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

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

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Обновление invoice_items для всех счетов периода в у.е. на дату оплаты
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Refresh_invoice_cu( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Refresh_invoice_cu';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- обновить invoice_items
    MERGE INTO INVOICE_ITEM_CU_T U
    USING (
        SELECT INV_ITEM_ID, 
               ROUND(TOTAL * CURRENCY_RATE, 2) TOTAL,  
               ROUND(GROSS * CURRENCY_RATE, 2) GROSS,
               ROUND(TOTAL * CURRENCY_RATE, 2) - ROUND(GROSS * CURRENCY_RATE, 2) TAX,
               810 CURRENCY_ID  
          FROM (
            SELECT V.*, Pk30_Billing_Base.Currency_rate( 840, 810, V.DATE_TO ) CURRENCY_RATE 
              FROM INVOICE_ITEM_T V
             WHERE EXISTS (
                SELECT * FROM BILL_T B
                 WHERE B.REP_PERIOD_ID = p_period_id
                   AND B.CURRENCY_ID   = 36
                   AND V.REP_PERIOD_ID = B.REP_PERIOD_ID
                   AND V.BILL_ID = B.BILL_ID
             )
             AND V.REP_PERIOD_ID = p_period_id
        )
    ) V
    ON (
       U.INV_ITEM_ID = V.INV_ITEM_ID 
    )
    WHEN MATCHED THEN UPDATE SET U.TOTAL = V.TOTAL, U.GROSS = V.GROSS, U.TAX = V.TAX, U.CURRENCY_ID = V.CURRENCY_ID
    WHEN NOT MATCHED THEN INSERT (U.INV_ITEM_ID, U.TOTAL, U.GROSS, U.TAX, U.CURRENCY_ID) 
                          VALUES (V.INV_ITEM_ID, V.TOTAL, V.GROSS, V.TAX, V.CURRENCY_ID)
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_CU_T: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  
    
    -- удалить не актуальные invoice_items
    DELETE FROM INVOICE_ITEM_CU_T U
     WHERE NOT EXISTS (
        SELECT * FROM INVOICE_ITEM_T V
         WHERE U.INV_ITEM_ID = V.INV_ITEM_ID 
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_CU_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    --
    -- обновить items
    MERGE INTO ITEM_CU_T U
    USING (
        SELECT ITEM_ID, 
               ROUND((REP_GROSS + REP_TAX) * CURRENCY_RATE, 2) TOTAL,  
               ROUND(REP_GROSS * CURRENCY_RATE, 2) GROSS,
               ROUND(REP_TAX * CURRENCY_RATE, 2) TAX,
               810 CURRENCY_ID  
          FROM (
            SELECT I.*, Pk30_Billing_Base.Currency_rate( 840, 810, I.DATE_TO ) CURRENCY_RATE 
              FROM ITEM_T I
             WHERE EXISTS (
                SELECT * FROM BILL_T B
                 WHERE B.REP_PERIOD_ID = p_period_id
                   AND B.CURRENCY_ID   = 36
                   AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
                   AND I.BILL_ID       = B.BILL_ID
             )
             AND I.REP_PERIOD_ID = p_period_id
        )
    ) I
    ON (
       U.ITEM_ID = I.ITEM_ID 
    )
    WHEN MATCHED THEN UPDATE SET U.TOTAL = I.TOTAL, U.GROSS = I.GROSS, U.TAX = I.TAX, U.CURRENCY_ID = I.CURRENCY_ID
    WHEN NOT MATCHED THEN INSERT (U.ITEM_ID, U.TOTAL, U.GROSS, U.TAX, U.CURRENCY_ID) 
                          VALUES (I.ITEM_ID, I.TOTAL, I.GROSS, I.TAX, I.CURRENCY_ID);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_CU_T: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  
    
    -- удалить не актуальные invoice_items
    DELETE FROM ITEM_CU_T U
     WHERE NOT EXISTS (
        SELECT * FROM ITEM_T V
         WHERE U.ITEM_ID = V.ITEM_ID 
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_CU_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

END PK30_BILLING;
/
