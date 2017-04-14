CREATE OR REPLACE PACKAGE PK10_PAYMENT_EISUP_REPORT
IS
    --
    -- Пакет для поддержки импорта из ЕИСУП
    -- Контроль загрузки платежей
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_PAYMENT_EISUP_REPORT';
    -- ==============================================================================
   
    type t_refc is ref cursor;

    -- ------------------------------------------------------------------------ --
    -- Заполняем таблицу дебиторами юр.лицами (без фильтрации)
    -- ------------------------------------------------------------------------ --
    PROCEDURE Fill_debitors_tmp;

    -- ------------------------------------------------------------------------ --
    -- 1) Детализированный отчет о ДЗ по ЮЛ 
    -- аналог PK400_DEBITORS.Cust_detail_report(?)
    -- ------------------------------------------------------------------------ --
    PROCEDURE Cust_detail_report (
                   p_recordset OUT t_refc,
                   p_branch_id IN INTEGER DEFAULT NULL
               );

    -- ------------------------------------------------------------------------ --
    -- 2) Детализированный отчет о ДЗ по ЮЛ (сгруппированный)
    -- аналог PK400_DEBITORS.Cust_detail_report_group(?)
    -- ------------------------------------------------------------------------ --
    PROCEDURE Cust_detail_report_group (
                   p_recordset    OUT t_refc, 
                   p_branch_id IN INTEGER DEFAULT NULL
               );
    
    -- ------------------------------------------------------------------------ --
    -- 3) Акт Сверки 
    -- ------------------------------------------------------------------------ --
    -- 3.1 по договору - PK400_DEBITORS.Get_Act
    PROCEDURE Get_act_by_contract_no (
                   p_recordset      OUT t_refc, 
                   p_start_bill_date IN DATE, 
                   p_end_bill_date   IN DATE,
                   p_contract_no     IN VARCHAR2
               );

    -- Получить входящее сальдо (для акта сверки)
    FUNCTION GetIncomeBalance (
                   p_start_bill_date IN DATE, 
                   p_contract_no     IN VARCHAR2
               ) RETURN NUMBER;

    -- 3.1 по договору - PK400_DEBITORS.Get_Act_account
    PROCEDURE Get_act_by_account_no (
                   p_recordset      OUT t_refc, 
                   p_start_bill_date IN DATE, 
                   p_end_bill_date   IN DATE,
                   p_account_no      IN VARCHAR2
               );
               
    -- Получить входящее сальдо по ЛС (для акта сверки)
    FUNCTION GetIncomeBalanceAcc (
                   p_start_bill_date IN DATE, 
                   p_account_no      IN VARCHAR2
               ) RETURN NUMBER;
         
    -- ------------------------------------------------------------------------ --
    -- 4) ОБоротно сальдовая ведомость
    -- аналог PK400_DEBITORS.DZ_JL_OBOROTY(?,?,?,?)
    -- ------------------------------------------------------------------------ --
    PROCEDURE Get_jur_oboroty (
                   p_recordset OUT t_refc,
                   p_period_id IN INTEGER,
                   p_branch_id IN INTEGER,
                   p_agent_id  IN INTEGER DEFAULT NULL
               );

    -- ------------------------------------------------------------------------ --
    -- список брендов
    -- ------------------------------------------------------------------------ --
    PROCEDURE Branch_lst (
                   p_recordset    OUT t_refc
               );

    --------------------------------------------------
    -- Получаем список Агентов по ID поставщика
    --------------------------------------------------
    PROCEDURE GetAgentsByContractorID(p_recordset OUT t_refc, p_contractor_id IN INTEGER);

    -- ================================================================== --
    -- дебиторская задолженность по договорам
    -- ================================================================== --
    PROCEDURE Contracts_report(
                   p_recordset OUT t_refc,
                   p_branch_id IN INTEGER DEFAULT NULL
               );
               
    -- проверка расчета дебиторской задолженности по договорам
    PROCEDURE Contracts_report_check(
                       p_recordset OUT t_refc,
                       p_contract_no IN INTEGER DEFAULT NULL
                   );
                   
    -- ================================================================== --
    -- Список договоров для сверки с 1С
    -- ================================================================== --
    PROCEDURE Contracts_list(
                       p_recordset OUT t_refc
                   );

END PK10_PAYMENT_EISUP_REPORT;
/
CREATE OR REPLACE PACKAGE BODY PK10_PAYMENT_EISUP_REPORT
IS

-- ------------------------------------------------------------------------ --
-- Заполняем таблицу дебиторами юр.лицами (без фильтрации)
-- ------------------------------------------------------------------------ --
PROCEDURE Fill_debitors_tmp
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Fill_debitors_tmp';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- удаляем данные для сеанса, на всякий случай
    DELETE FROM DEBITORS_TMP;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('DEBITORS_TMP '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- заполняем таблицу
    INSERT INTO DEBITORS_TMP
    -- =================================================================================== --
    WITH BL AS (
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- статистика по выставленным счетам, начиная с времени утверждения входящего баланса
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT B.ACCOUNT_ID, B.BILL_ID, B.BILL_NO, B.BILL_DATE, 
               B.REP_PERIOD_ID, B.BILL_STATUS, B.TOTAL, 
               NVL(IB.BALANCE,0) IN_BALANCE, 
               NVL(IB.BALANCE_DATE, 
                   MIN(B.BILL_DATE) 
                       OVER (PARTITION BY B.ACCOUNT_ID 
                             ORDER BY B.REP_PERIOD_ID, B.BILL_DATE, B.BILL_ID)
                             ) IN_BALANCE_DATE,
               SUM(TOTAL) 
                       OVER (PARTITION BY B.ACCOUNT_ID 
                             ORDER BY B.REP_PERIOD_ID, B.BILL_DATE, B.BILL_ID
                       ) CURR_TOTAL, -- начислено с нарастающим итогом
               SUM(TOTAL) OVER (PARTITION BY B.ACCOUNT_ID) FULL_TOTAL, -- начислено всего
               (SYSDATE-BI.DAYS_FOR_PAYMENT) DUE_DATE -- дата наступления задолженности
          FROM BILL_T B, INCOMING_BALANCE_T IB, BILLINFO_T BI
         WHERE B.BILL_STATUS IN ('CLOSED','READY')
           AND B.ACCOUNT_ID = BI.ACCOUNT_ID
           AND B.TOTAL > 0 -- отрицательные счета и кредит-ноты учитываем как платежи
           AND B.ACCOUNT_ID = IB.ACCOUNT_ID(+)
           AND B.BILL_DATE <= (SYSDATE-BI.DAYS_FOR_PAYMENT)
           AND CASE
                WHEN IB.ACCOUNT_ID IS NULL THEN 1
                WHEN IB.ACCOUNT_ID IS NOT NULL AND B.BILL_DATE     > IB.BALANCE_DATE  THEN 1
                WHEN IB.ACCOUNT_ID IS NOT NULL AND B.REP_PERIOD_ID > IB.REP_PERIOD_ID THEN 1
                ELSE 0
               END = 1
         ORDER BY B.REP_PERIOD_ID DESC
    ), PY AS (
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- статистика по принятым платежам, начиная с времени утверждения входящего баланса
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT ACCOUNT_ID, LST_PAYMENT_ID, LST_PAY_PERIOD_ID, LST_PAYMENT_DATE, LST_RECVD, SUM_RECVD
          FROM (
            SELECT P.ACCOUNT_ID,
                   P.PAYMENT_ID     LST_PAYMENT_ID,
                   P.REP_PERIOD_ID  LST_PAY_PERIOD_ID,
                   P.PAYMENT_DATE   LST_PAYMENT_DATE,
                   P.RECVD          LST_RECVD,
                   ROW_NUMBER() OVER (PARTITION BY P.ACCOUNT_ID ORDER BY P.PAYMENT_DATE DESC) RN, 
                   SUM(RECVD) OVER (PARTITION BY P.ACCOUNT_ID) SUM_RECVD
              FROM (
                SELECT P.ACCOUNT_ID,
                       P.PAYMENT_ID,
                       P.REP_PERIOD_ID,
                       P.PAYMENT_DATE,
                       P.RECVD
                  FROM PAYMENT_T P
                 WHERE P.ACCOUNT_ID != 2
                UNION ALL
                -- отрицательные счета - приравниваем к платежам (в том числе и кредит ноты)
                SELECT B.ACCOUNT_ID, 
                       NULL            PAYMENT_ID,    
                       B.REP_PERIOD_ID PAY_PERIOD,
                       B.BILL_DATE     PAYMENT_DATE,
                       -B.TOTAL        RECVD
                  FROM BILL_T B 
                 WHERE B.BILL_STATUS IN ('CLOSED','READY')
                   AND B.TOTAL < 0
              ) P, INCOMING_BALANCE_T IB
             WHERE P.ACCOUNT_ID != 2
              AND P.ACCOUNT_ID = IB.ACCOUNT_ID(+)
              AND CASE
                     WHEN IB.ACCOUNT_ID IS NULL THEN 1
                     WHEN IB.ACCOUNT_ID IS NOT NULL AND P.REP_PERIOD_ID > IB.REP_PERIOD_ID THEN 1
                     ELSE 0
                  END = 1
          )
          WHERE RN = 1
    ), BAL AS (
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- баланс по лицевым счетам
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT 
               ACCOUNT_ID,                              -- ID номер л/с
               (IN_BALANCE + SUM_RECVD) - FULL_TOTAL DUE,-- задолженность по л/с
               IN_BALANCE, SUM_RECVD, FULL_TOTAL, CURR_TOTAL, -- для отладки  
               MAX_BILL_DATE,                           -- дата последнего выставленного счета
               MIN_BILL_DATE,                           -- дата первого неоплаченного счета
               MIN_BILL_NO,                             -- номер первого неоплаченного счета
               LST_PAYMENT_DATE,                        -- дата последнего платежа
               LST_RECVD,                               -- сумма последнего платежа
               DUE_MONS,                                -- кол-во месяцев, с момента последней оплаты  
               DUE_DAYS,                                -- кол-во дней, с момента последней оплаты
               CASE
                 WHEN DUE_DAYS BETWEEN 1  AND 30    THEN '1..30 days'
                 WHEN DUE_DAYS BETWEEN 31 AND 90    THEN '31..90 days'
                 WHEN DUE_DAYS BETWEEN 31 AND 180   THEN '91..180 days'
                 WHEN DUE_DAYS > 180 AND DUE_MONS <= 36 THEN '< 3 year'
                 WHEN DUE_MONS > 36 THEN '> 3 year'
               END DUE_ITV                              -- интервал задолженности
          FROM (
            SELECT  ROW_NUMBER() OVER (PARTITION BY BL.ACCOUNT_ID ORDER BY BL.BILL_DATE, BL.BILL_ID) RN, 
                    BL.ACCOUNT_ID, 
                    BL.IN_BALANCE,
                    BL.CURR_TOTAL,
                    BL.FULL_TOTAL, 
                    NVL(PY.SUM_RECVD,0) SUM_RECVD,
                    BL.BILL_ID          MIN_BILL_ID, 
                    BL.BILL_NO          MIN_BILL_NO, 
                    BL.BILL_DATE        MIN_BILL_DATE, 
                    MAX(BL.BILL_DATE) OVER (PARTITION BY BL.ACCOUNT_ID) MAX_BILL_DATE,
                    LST_PAYMENT_ID, 
                    LST_PAY_PERIOD_ID, 
                    LST_PAYMENT_DATE, 
                    LST_RECVD,
                    ROUND(MONTHS_BETWEEN(BL.DUE_DATE, BL.BILL_DATE)) DUE_MONS,
                    ROUND(BL.DUE_DATE - BL.BILL_DATE)                DUE_DAYS
              FROM BL, PY
             WHERE BL.ACCOUNT_ID = PY.ACCOUNT_ID(+)
               AND (NVL(PY.SUM_RECVD,0) + BL.IN_BALANCE) < BL.CURR_TOTAL
         )
         WHERE RN = 1 
    ), ST AS (
        -- ------------------------------------------------------------------------------- --
        -- описание клиента на текущий момент времени
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- статус л/с в зависимости от кол-ва открытых, не заблокированных заказов
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT ACCOUNT_ID,
                   CASE
                     WHEN ORDERS_NUM = 0 THEN 'Неактивный'
                     WHEN CLOSED_NUM = ORDERS_NUM THEN 'Закрыт' -- > 0 AND LK.LOCKS_NUM = 0 THEN 'Закрыт'
                     WHEN LOCKS_NUM  = 0 AND (LOCKS_NUM + CLOSED_NUM) < ORDERS_NUM THEN 'Активный'
                     WHEN LOCKS_NUM  > 0 AND (LOCKS_NUM + CLOSED_NUM) = ORDERS_NUM THEN 'Заблокирован'
                     WHEN LOCKS_NUM  > 0 AND (LOCKS_NUM + CLOSED_NUM) < ORDERS_NUM THEN 'Частично заблокирован'
                   END ACCOUNT_STATUS
              FROM (
              SELECT O.ACCOUNT_ID, 
                     SUM(DECODE(L.ORDER_ID, NULL, 0, 1)) LOCKS_NUM,
                     SUM(DECODE(O.STATUS, 'CLOSED',1,0)) CLOSED_NUM,
                     COUNT(*) ORDERS_NUM
                FROM ORDER_T O, ORDER_LOCK_T L
               WHERE O.ORDER_ID = L.ORDER_ID(+)
                 AND L.DATE_TO(+) IS NULL
                 AND L.LOCK_TYPE_ID(+) != 901
                 AND O.DATE_FROM <= SYSDATE
                 AND (O.DATE_TO IS NULL OR SYSDATE < O.DATE_TO)
              GROUP BY O.ACCOUNT_ID
            )
    ), AP AS (
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- последний профиль на лицевом счете
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT ACCOUNT_ID, PROFILE_ID, 
               CONTRACT_ID, CUSTOMER_ID, 
               CONTRACTOR_ID, BRANCH_ID, AGENT_ID
          FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY DATE_FROM DESC, PROFILE_ID DESC) RN,
                   AP.* 
              FROM ACCOUNT_PROFILE_T AP
          )
         WHERE RN = 1
    ), CM AS (  
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- последняя компания на договоре
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT CONTRACT_ID, COMPANY_NAME
          FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY CONTRACT_ID ORDER BY DATE_FROM DESC) RN,
                   CM.* 
              FROM COMPANY_T CM
          )
         WHERE RN = 1
    ), SR AS ( 
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- перечень услуг на л/с в формате *.csv
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT ACCOUNT_ID, MAX(SRVs) SRVs
          FROM (
            SELECT ACCOUNT_ID, SRV, LTRIM(SYS_CONNECT_BY_PATH(SRV, ','), ',') SRVs
              FROM (  
                SELECT ACCOUNT_ID, SRV,  SERVICE_ID,
                       LAG(SERVICE_ID) OVER (PARTITION BY ACCOUNT_ID ORDER BY SRV) PREV_SERVICE_ID
                  FROM (
                    SELECT O.ACCOUNT_ID, O.SERVICE_ID, S.SERVICE_CODE||'-'||COUNT(*) SRV 
                      FROM ORDER_T O, SERVICE_T S
                     WHERE O.SERVICE_ID = S.SERVICE_ID
                     GROUP BY O.ACCOUNT_ID, O.SERVICE_ID, S.SERVICE_CODE
                )
              ) OSRV
              START WITH PREV_SERVICE_ID IS NULL
              CONNECT BY PRIOR SERVICE_ID = PREV_SERVICE_ID AND ACCOUNT_ID = PRIOR ACCOUNT_ID
          )
         GROUP BY ACCOUNT_ID
    ), CL AS (   
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- описание клиента на текущий момент времени
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT 
             A.BILLING_ID,                  -- номер биллинга
             A.ACCOUNT_ID,                  -- ID л/с
             A.ACCOUNT_NO,                  -- номер л/с
             A.BALANCE,                     -- баланс на л.с
             NVL(ST.ACCOUNT_STATUS, 'Неактивный') ACCOUNT_STATUS, -- статус, в зависимости от состояния заказов на л/с
             SR.SRVs,                       -- перечень услуг на л/с в csv-формате
             AP.PROFILE_ID,                 -- текущий профиль нп л/с
             C.CONTRACT_NO,                 -- номер договора
             C.CONTRACT_ID,                 -- ID договора
             CM.COMPANY_NAME,               -- Наименование компании по договору  
             CS.INN,                        -- ИНН контрагента
             CS.KPP,                        -- КПП контрагента
             CS.ERP_CODE,                   -- код контрагента в 1С
             AA.CITY,                       -- город
             AA.ADDRESS,                    -- адрес
             AA.PHONES,                     -- телефон
             AP.BRANCH_ID,                  -- ID бренда продавца
             BR.CONTRACTOR BRAND_NAME,      -- бренд продавца
             AP.AGENT_ID,                   -- ID агента
             AG.CONTRACTOR AGENT_NAME,      -- агент
             DECODE(C.CLIENT_TYPE_ID, 6409, 'VIP', 'Юридические лица') VIP, -- VIP клиент
             PK402_BCR_DATA.Get_sales_curator(
                  AP.BRANCH_ID, 
                  AP.AGENT_ID, 
                  AP.CONTRACT_ID, 
                  AP.ACCOUNT_ID, 
                  NULL, 
                  NULL) MANAGER
          FROM 
             ACCOUNT_T A, 
             CONTRACT_T C, CUSTOMER_T CS,
             CONTRACTOR_T BR, CONTRACTOR_T AG,
             ACCOUNT_CONTACT_T AA,
             AP,
             CM,
             ST,
             SR
         WHERE A.ACCOUNT_TYPE  = 'J'
           --AND A.BILLING_ID IN (2001,2002) -- 2003 - отдельно
           AND A.ACCOUNT_ID    = AP.ACCOUNT_ID
           AND AP.CONTRACT_ID  = C.CONTRACT_ID
           AND AP.CUSTOMER_ID  = CS.CUSTOMER_ID
           AND AP.BRANCH_ID    = BR.CONTRACTOR_ID(+)
           AND NVL(AP.AGENT_ID, AP.BRANCH_ID)  = AG.CONTRACTOR_ID(+)
           AND A.ACCOUNT_ID    = AA.ACCOUNT_ID
           AND AA.ADDRESS_TYPE = 'JUR'
           AND A.STATUS       <> 'T'
           AND C.CONTRACT_ID   = CM.CONTRACT_ID
           AND A.ACCOUNT_ID    = ST.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID    = SR.ACCOUNT_ID(+)
    )
    -- =================================================================================== --
    -- ИТОГ:
    -- =================================================================================== --
    SELECT  CL.BILLING_ID,         -- номер биллинга
            CL.ACCOUNT_ID,         -- ID л/с
            CL.ACCOUNT_NO,         -- номер л/с
            CL.ACCOUNT_STATUS,     -- статус, в зависимости от состояния заказов на л/с
            CL.BALANCE,            -- баланс на л.с
            --
            BAL.DUE,               -- задолженность по л/с
            BAL.MAX_BILL_DATE,     -- дата последнего выставленного счета
            BAL.MIN_BILL_DATE,     -- дата первого неоплаченного счета
            BAL.MIN_BILL_NO,       -- номер первого неоплаченного счета
            BAL.LST_PAYMENT_DATE,  -- дата последнего платежа
            BAL.LST_RECVD,         -- сумма последнего платежа
            BAL.DUE_MONS,          -- кол-во месяцев, с момента последней оплаты  
            BAL.DUE_DAYS,          -- кол-во дней, с момента последней оплаты
            BAL.DUE_ITV,           -- интервал задолженности
            --
            CL.PROFILE_ID,         -- текущий профиль нп л/с
            CL.CONTRACT_ID,        -- ID договора
            CL.CONTRACT_NO,        -- номер договора
            CL.SRVs,               -- перечень услуг на л/с в csv-формате
            CL.COMPANY_NAME,       -- Наименование компании по договору  
            CL.INN,                -- ИНН контрагента
            CL.KPP,                -- КПП контрагента
            CL.ERP_CODE,           -- код контрагента в 1С
            CL.CITY,               -- город
            CL.ADDRESS,            -- адрес
            CL.PHONES,             -- телефон
            CL.BRANCH_ID,          -- ID бренда продавца
            CL.BRAND_NAME,         -- бренд продавца
            CL.AGENT_ID,           -- ID агента
            CL.AGENT_NAME,         -- агент
            CL.VIP,                -- VIP клиент
            CL.MANAGER             -- Продавец (sales curator)
      FROM BAL, CL
     WHERE BAL.ACCOUNT_ID = CL.ACCOUNT_ID;
        
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('DEBITORS_TMP '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- сохраняем для разборок в системе логирования
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- ------------------------------------------------------------------------ --
-- 1) Детализированный отчет о ДЗ по ЮЛ 
-- аналог PK400_DEBITORS.Cust_detail_report(?)
-- ------------------------------------------------------------------------ --
PROCEDURE Cust_detail_report (
               p_recordset OUT t_refc,
               p_branch_id IN INTEGER DEFAULT NULL
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Cust_detail_report';
    v_retcode    INTEGER;
BEGIN
    -- заполняем временную таблицу данными
    Fill_debitors_tmp;
    
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
          SELECT 
            BILLING_ID,         -- номер биллинга
            ACCOUNT_ID,         -- ID л/с
            ACCOUNT_NO,         -- номер л/с
            ACCOUNT_STATUS,     -- статус, в зависимости от состояния заказов на л/с
            BALANCE,            -- баланс на л.с
            --
            DUE,                -- задолженность по л/с
            MAX_BILL_DATE,      -- дата последнего выставленного счета
            MIN_BILL_DATE,      -- дата первого неоплаченного счета
            MIN_BILL_NO,        -- номер первого неоплаченного счета
            LST_PAYMENT_DATE,   -- дата последнего платежа
            LST_RECVD,          -- сумма последнего платежа
            DUE_MONS,           -- кол-во месяцев, с момента последней оплаты  
            DUE_DAYS,           -- кол-во дней, с момента последней оплаты
            DUE_ITV,            -- интервал задолженности
            --
            PROFILE_ID,         -- текущий профиль нп л/с
            CONTRACT_ID,        -- ID договора
            CONTRACT_NO,        -- номер договора
            SRVs,               -- перечень услуг на л/с в csv-формате
            COMPANY_NAME,       -- Наименование компании по договору  
            INN,                -- ИНН контрагента
            KPP,                -- КПП контрагента
            ERP_CODE,           -- код контрагента в 1С
            CITY,               -- город
            ADDRESS,            -- адрес
            PHONES,             -- телефон
            BRANCH_ID,          -- ID бренда продавца
            BRAND_NAME,         -- бренд продавца
            AGENT_ID,           -- ID агента
            AGENT_NAME,         -- агент
            VIP,                -- VIP клиент
            MANAGER             -- Продавец (sales curator) 
           FROM DEBITORS_TMP
          WHERE (p_branch_id IS NULL OR BRANCH_ID = p_branch_id)
          ORDER BY BRANCH_ID, DUE     
         ;     

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- 2) Детализированный отчет о ДЗ по ЮЛ (сгруппированный)
-- аналог PK400_DEBITORS.Cust_detail_report_group(?)
-- ------------------------------------------------------------------------ --
PROCEDURE Cust_detail_report_group (
               p_recordset    OUT t_refc,
               p_branch_id IN INTEGER DEFAULT NULL 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Cust_detail_report_group';
    v_retcode    INTEGER;
BEGIN
    -- заполняем временную таблицу данными
    Fill_debitors_tmp;
    
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
        SELECT 
            CONTRACT_NO,             -- номер договора
            BILLING_ID,              -- номер биллинга
            COUNT(ACCOUNT_ID) ACCOUNT_NUM,         -- ID л/с
            SUM(BALANCE) BALANCE,            -- баланс на л.с
            --
            SUM(DUE) DUE,                -- задолженность по л/с
            MAX(MAX_BILL_DATE) MAX_BILL_DATE,      -- дата последнего выставленного счета
            MIN(MIN_BILL_DATE) MIN_BILL_DATE,      -- дата первого неоплаченного счета
            MAX(LST_PAYMENT_DATE) LST_PAYMENT_DATE,   -- дата последнего платежа
            MAX(DUE_MONS) DUE_MONS,           -- кол-во месяцев, с момента последней оплаты  
            MAX(DUE_DAYS) DUE_DAYS,           -- кол-во дней, с момента последней оплаты
            MAX(DUE_ITV) DUE_ITV,            -- интервал задолженности
            --
            MAX(COMPANY_NAME) COMPANY_NAME,       -- Наименование компании по договору  
            MAX(INN) INN,                -- ИНН контрагента
            MAX(KPP) KPP,                -- КПП контрагента
            MAX(ERP_CODE) ERP_CODE,           -- код контрагента в 1С
            
            MAX(CITY) CITY,               -- город
            MAX(ADDRESS) ADDRESS,            -- адрес
            MAX(PHONES) PHONES,             -- телефон
            BRANCH_ID,               -- ID бренда продавца
            MAX(BRAND_NAME) BRAND_NAME,          -- бренд продавца
            MAX(AGENT_NAME) AGENT_NAME           -- агент
          FROM (          
            SELECT * 
             FROM DEBITORS_TMP
            WHERE (p_branch_id IS NULL OR BRANCH_ID = p_branch_id)
        )
        GROUP BY BILLING_ID, CONTRACT_NO, BRANCH_ID
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- 3) Акт Сверки 
-- ------------------------------------------------------------------------ --
-- 3.1 по договору - PK400_DEBITORS.Get_Act
PROCEDURE Get_act_by_contract_no (
               p_recordset      OUT t_refc, 
               p_start_bill_date IN DATE, 
               p_end_bill_date   IN DATE,
               p_contract_no     IN VARCHAR2
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_act_by_contract_no';
    v_retcode         INTEGER;
    v_start_period_id INTEGER;
    v_end_period_id   INTEGER;
BEGIN
    v_start_period_id := PK04_PERIOD.Period_id (p_start_bill_date);
    v_end_period_id   := PK04_PERIOD.Period_id (p_end_bill_date);
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
        SELECT * FROM (
           SELECT 
               B.BILL_NO,
               TRUNC (B.BILL_DATE) BILL_DATE,
               B.TOTAL BILL_TOTAL,
               0 SUM_PAY,
               '' PAY_NUM,
               TO_DATE ('01.01.1970', 'dd.mm.yyyy') PAY_DATE
          FROM BILL_T B,
               ACCOUNT_T A,
               ACCOUNT_PROFILE_T AP,
               CONTRACT_T C,
               INCOMING_BALANCE_T IB
         WHERE B.REP_PERIOD_ID BETWEEN v_start_period_id AND v_end_period_id
           AND A.ACCOUNT_TYPE = 'J'
           AND B.ACCOUNT_ID  = A.ACCOUNT_ID
           AND AP.ACCOUNT_ID = A.ACCOUNT_ID
           AND AP.PROFILE_ID = B.PROFILE_ID
           AND AP.CONTRACT_ID= C.CONTRACT_ID
           AND C.CONTRACT_NO = p_contract_no
           AND TRUNC (b.BILL_DATE) >= p_start_bill_date
           AND TRUNC (b.BILL_DATE) <=p_end_bill_date
           AND A.ACCOUNT_ID  = IB.ACCOUNT_ID(+)
           AND CASE
                 WHEN IB.ACCOUNT_ID IS NULL THEN 1
                 WHEN IB.ACCOUNT_ID IS NOT NULL 
                  AND IB.REP_PERIOD_ID < B.REP_PERIOD_ID
                  AND IB.BALANCE_DATE  < B.BILL_DATE
                 THEN  1
                 ELSE 0
               END = 1
        UNION ALL
        SELECT '' BILL_NO,
               TRUNC (P.PAYMENT_DATE) PAYMENT_DATE,
               0 BILL_TOTAL,
               P.RECVD SUM_PAY,
               NVL(SUBSTR (P.DOC_ID, 1, INSTR (P.DOC_ID, '-') - 1), '-') PAY_NUM,
               TRUNC (P.PAYMENT_DATE) PAY_DATE
          FROM ACCOUNT_T A, 
               ACCOUNT_PROFILE_T AP, 
               CONTRACT_T C,
               PAYMENT_T P,
               INCOMING_BALANCE_T IB
         WHERE  P.REP_PERIOD_ID BETWEEN v_start_period_id AND v_end_period_id
           AND A.ACCOUNT_TYPE = 'J'
           AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND P.ACCOUNT_ID   = AP.ACCOUNT_ID
           AND C.CONTRACT_NO  = p_contract_no
           AND TRUNC (p.PAYMENT_DATE) >= p_start_bill_date
           AND TRUNC (p.PAYMENT_DATE) <= p_end_bill_date
           AND A.ACCOUNT_ID   = IB.ACCOUNT_ID(+)
           AND CASE
                 WHEN IB.ACCOUNT_ID IS NULL THEN 1
                 WHEN IB.ACCOUNT_ID IS NOT NULL 
                  AND IB.REP_PERIOD_ID < P.REP_PERIOD_ID
                  AND IB.BALANCE_DATE  < P.PAYMENT_DATE
                 THEN  1
                 ELSE 0
               END = 1
        ) ORDER BY BILL_DATE
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- 3.1 по договору - PK400_DEBITORS.Get_Act_account
PROCEDURE Get_act_by_account_no (
               p_recordset      OUT t_refc, 
               p_start_bill_date IN DATE, 
               p_end_bill_date   IN DATE,
               p_account_no      IN VARCHAR2
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_act_by_account_no';
    v_retcode    INTEGER;
    v_start_period_id INTEGER;
    v_end_period_id   INTEGER;
BEGIN
    v_start_period_id := PK04_PERIOD.Period_id (p_start_bill_date);
    v_end_period_id   := PK04_PERIOD.Period_id (p_end_bill_date);

    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
      SELECT * FROM 
      (
        SELECT B.BILL_NO,
               TRUNC (B.BILL_DATE) BILL_DATE,
               B.TOTAL BILL_TOTAL,
               0 SUM_PAY,
               '' PAY_NUM,
               TO_DATE ('01.01.1970', 'dd.mm.yyyy') PAY_DATE
          FROM BILL_T B,
               ACCOUNT_T A,
               ACCOUNT_PROFILE_T AP,
               INCOMING_BALANCE_T IB
         WHERE B.REP_PERIOD_ID BETWEEN v_start_period_id AND v_end_period_id
           AND B.ACCOUNT_ID    = A.ACCOUNT_ID
           AND A.ACCOUNT_TYPE  = 'J'
           AND A.ACCOUNT_NO    = p_account_no
           AND AP.ACCOUNT_ID   = A.ACCOUNT_ID
           AND TRUNC(b.BILL_DATE) >= p_start_bill_date
           AND TRUNC(b.BILL_DATE) <= p_end_bill_date
           AND A.ACCOUNT_ID    = IB.ACCOUNT_ID(+)
           AND CASE
                 WHEN IB.ACCOUNT_ID IS NULL THEN 1
                 WHEN IB.ACCOUNT_ID IS NOT NULL 
                  AND IB.REP_PERIOD_ID < B.REP_PERIOD_ID
                  AND IB.BALANCE_DATE  < B.BILL_DATE
                 THEN  1
                 ELSE 0
               END = 1
        UNION
        SELECT '' BILL_NO,
               TRUNC (P.PAYMENT_DATE),
               0,
               P.RECVD SUM_PAY,
               SUBSTR (P.DOC_ID, 1, INSTR (P.DOC_ID, '-') - 1) PAY_NUM,
               TRUNC (P.PAYMENT_DATE) PAY_DATE
          FROM ACCOUNT_T A, 
               ACCOUNT_PROFILE_T AP, 
               PAYMENT_T P,
               INCOMING_BALANCE_T IB
         WHERE P.REP_PERIOD_ID BETWEEN v_start_period_id AND v_end_period_id
           AND A.ACCOUNT_NO    = p_account_no
           AND A.ACCOUNT_TYPE  = 'J'
           AND AP.ACCOUNT_ID   = A.ACCOUNT_ID
           AND AP.ACCOUNT_ID   = P.ACCOUNT_ID
           AND TRUNC(p.PAYMENT_DATE) >= p_start_bill_date
           AND TRUNC(p.PAYMENT_DATE) <= p_end_bill_date
           AND A.ACCOUNT_ID    = IB.ACCOUNT_ID(+)
           AND CASE
                 WHEN IB.ACCOUNT_ID IS NULL THEN 1
                 WHEN IB.ACCOUNT_ID IS NOT NULL 
                  AND IB.REP_PERIOD_ID < P.REP_PERIOD_ID
                  AND IB.BALANCE_DATE  < P.PAYMENT_DATE
                 THEN  1
                 ELSE 0
               END = 1
      ) ORDER BY BILL_DATE;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- Получить входящее сальдо (для акта сверки)
-- ------------------------------------------------------------------------ --
FUNCTION GetIncomeBalance (
             p_start_bill_date IN DATE, 
             p_contract_no     IN VARCHAR2
         ) RETURN NUMBER
IS
    v_prcName             CONSTANT VARCHAR2(30) := 'GetIncomeBalance';
    v_income_balance      NUMBER;
BEGIN
    WITH A AS (
      SELECT DISTINCT AP.ACCOUNT_ID
        FROM ACCOUNT_PROFILE_T AP, CONTRACT_T C
       WHERE AP.CONTRACT_ID = C.CONTRACT_ID
         AND AP.DATE_FROM < p_start_bill_date
         AND (AP.DATE_TO IS NULL OR p_start_bill_date < AP.DATE_TO)
         AND C.CONTRACT_NO = p_contract_no
    )    
    SELECT SUM(TOTAL) INTO v_income_balance
    FROM (
        -- получаем полную задолженность по выставленным счетам
        SELECT -B.TOTAL TOTAL 
          FROM BILL_T B, A
         WHERE B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_READY, 
                                 Pk00_Const.c_BILL_STATE_CLOSED)
           AND B.ACCOUNT_ID = A.ACCOUNT_ID
           AND B.BILL_DATE  < p_start_bill_date 
        UNION ALL
        -- получаем сумму поступивших за период платежей
        SELECT P.RECVD TOTAL
          FROM PAYMENT_T P, A
         WHERE P.ACCOUNT_ID   = A.ACCOUNT_ID
           AND P.PAYMENT_DATE < p_start_bill_date
        -- учитываем входящий баланс
        UNION ALL
        SELECT IB.BALANCE TOTAL
          FROM INCOMING_BALANCE_T IB, A
         WHERE IB.ACCOUNT_ID   = A.ACCOUNT_ID
           AND IB.BALANCE_DATE < p_start_bill_date
    );
    RETURN -1 * v_income_balance;  
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Получить входящее сальдо по ЛС (для акта сверки)
-- ------------------------------------------------------------------------ --
FUNCTION GetIncomeBalanceAcc (
            p_start_bill_date IN DATE, 
            p_account_no      IN VARCHAR2
         ) RETURN NUMBER
IS
    v_prcName           CONSTANT VARCHAR2(30) := 'GetIncomeBalanceAcc';
    v_income_balance    NUMBER;
BEGIN
    WITH A AS (
      SELECT ACCOUNT_ID 
        FROM ACCOUNT_T 
       WHERE ACCOUNT_NO = p_account_no
    )    
    SELECT SUM(TOTAL) INTO v_income_balance
    FROM (
        -- получаем полную задолженность по выставленным счетам
        SELECT -B.TOTAL TOTAL 
          FROM BILL_T B, A
         WHERE B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_READY, 
                                 Pk00_Const.c_BILL_STATE_CLOSED)
           AND B.ACCOUNT_ID = A.ACCOUNT_ID
           AND B.BILL_DATE  < p_start_bill_date 
        UNION ALL
        -- получаем сумму поступивших за период платежей
        SELECT P.RECVD TOTAL
          FROM PAYMENT_T P, A
         WHERE P.ACCOUNT_ID   = A.ACCOUNT_ID
           AND P.PAYMENT_DATE < p_start_bill_date
        -- учитываем входящий баланс
        UNION ALL
        SELECT IB.BALANCE TOTAL
          FROM INCOMING_BALANCE_T IB, A
         WHERE IB.ACCOUNT_ID   = A.ACCOUNT_ID
           AND IB.BALANCE_DATE < p_start_bill_date
    );
    RETURN -1 * v_income_balance;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- 4) ОБоротно сальдовая ведомость
-- аналог PK400_DEBITORS.DZ_JL_OBOROTY(?,?,?,?)
-- ------------------------------------------------------------------------ --
PROCEDURE Get_jur_oboroty (
               p_recordset OUT t_refc,
               p_period_id IN INTEGER,
               p_branch_id IN INTEGER,
               p_agent_id  IN INTEGER DEFAULT NULL
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_jur_oboroty';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
      WITH IBL AS (
          SELECT IB.ACCOUNT_ID, IB.BALANCE, IB.REP_PERIOD_ID
            FROM INCOMING_BALANCE_T IB
      ), IDB AS (
          SELECT B.ACCOUNT_ID, SUM(B.TOTAL) IN_BILL_TOTAL
            FROM BILL_T B
           WHERE B.REP_PERIOD_ID < p_period_id
             AND EXISTS (
                 SELECT * FROM INCOMING_BALANCE_T IB
                  WHERE B.ACCOUNT_ID = IB.ACCOUNT_ID
                    AND B.REP_PERIOD_ID > IB.REP_PERIOD_ID
             )
           GROUP BY B.ACCOUNT_ID
      ), ICR AS (
          SELECT P.ACCOUNT_ID, SUM(P.RECVD) IN_PAY_TOTAL 
            FROM PAYMENT_T P 
           WHERE P.REP_PERIOD_ID < p_period_id
             AND EXISTS (
                 SELECT * FROM INCOMING_BALANCE_T IB
                  WHERE P.ACCOUNT_ID = IB.ACCOUNT_ID
                    AND P.REP_PERIOD_ID > IB.REP_PERIOD_ID
             )
           GROUP BY P.ACCOUNT_ID
      ), BIL AS (
          SELECT B.ACCOUNT_ID, SUM(B.TOTAL) BILL_TOTAL
            FROM BILL_T B 
           WHERE B.REP_PERIOD_ID = p_period_id
             AND EXISTS (
                 SELECT * FROM INCOMING_BALANCE_T IB
                  WHERE B.ACCOUNT_ID = IB.ACCOUNT_ID
                    AND B.REP_PERIOD_ID > IB.REP_PERIOD_ID
                    AND B.BILL_DATE     > IB.BALANCE_DATE
             )
           GROUP BY B.ACCOUNT_ID
      ), PAY AS (
          SELECT P.ACCOUNT_ID, SUM(P.RECVD) PAY_TOTAL 
            FROM PAYMENT_T P 
           WHERE P.REP_PERIOD_ID = p_period_id
             AND EXISTS (
                 SELECT * FROM INCOMING_BALANCE_T IB
                  WHERE P.ACCOUNT_ID = IB.ACCOUNT_ID
                    AND P.REP_PERIOD_ID > IB.REP_PERIOD_ID
                    AND P.PAYMENT_DATE  > IB.BALANCE_DATE
             )
           GROUP BY P.ACCOUNT_ID
      )
      SELECT CM.COMPANY_NAME,                    -- Клиент
             A.ACCOUNT_NO,                       -- Лицевой счет
             --A.ACCOUNT_ID,  
             CASE
               WHEN IBL.BALANCE < 0 THEN NVL(IDB.IN_BILL_TOTAL, 0) - IBL.BALANCE
               ELSE NVL(IDB.IN_BILL_TOTAL, 0) 
             END IN_DEBET,                       -- Начальное сальдо (дебет)
             CASE 
               WHEN IBL.BALANCE > 0 THEN NVL(ICR.IN_PAY_TOTAL, 0) + IBL.BALANCE
               ELSE NVL(ICR.IN_PAY_TOTAL, 0)
             END IN_CREDIT,                      -- Начальное сальдо (кредит)
             NVL(BIL.BILL_TOTAL, 0) BILL_TOTAL,  -- Оборот (дебет)
             NVL(PAY.PAY_TOTAL, 0) PAY_TOTAL,    -- Оборот (кредит)
             NVL(IDB.IN_BILL_TOTAL, 0) + NVL(BIL.BILL_TOTAL, 0) OUT_DEBET, -- Конечное сальдо (дебет)
             NVL(ICR.IN_PAY_TOTAL, 0) + NVL(PAY.PAY_TOTAL, 0) OUT_CREDIT   -- Конечное сальдо (кредит)
        FROM ACCOUNT_T A, IBL, IDB, ICR, BIL, PAY,
             ACCOUNT_PROFILE_T AP,
             COMPANY_T CM,
             PERIOD_T P 
       WHERE A.ACCOUNT_TYPE = 'J'
         AND A.STATUS     IN ('B','C')
         AND A.ACCOUNT_ID   = IBL.ACCOUNT_ID  -- обязательно
         AND A.ACCOUNT_ID   = IDB.ACCOUNT_ID(+)
         AND A.ACCOUNT_ID   = ICR.ACCOUNT_ID(+)
         AND A.ACCOUNT_ID   = BIL.ACCOUNT_ID(+)
         AND A.ACCOUNT_ID   = PAY.ACCOUNT_ID(+)
         AND A.ACCOUNT_ID   = AP.ACCOUNT_ID
         AND P.PERIOD_ID    = p_period_id
         AND AP.DATE_FROM   < P.PERIOD_TO
         AND (
                CASE 
                  WHEN AP.DATE_TO IS NOT NULL AND AP.DATE_TO > P.PERIOD_FROM THEN 1
                  WHEN AP.DATE_TO IS NULL THEN 1
                  ELSE 0
                END
             ) = 1
         AND AP.CONTRACT_ID = CM.CONTRACT_ID
         AND CM.DATE_FROM   < P.PERIOD_TO
         AND (
                CASE 
                  WHEN CM.DATE_TO IS NOT NULL AND CM.DATE_TO > P.PERIOD_FROM THEN 1
                  WHEN CM.DATE_TO IS NULL THEN 1
                  ELSE 0
                END
             ) = 1
         AND (AP.BRANCH_ID  = p_branch_id OR p_branch_id IS NULL)
         AND ( AP.AGENT_ID  = p_agent_id OR p_agent_id IS NULL)
       ORDER BY CM.COMPANY_NAME, A.ACCOUNT_NO
       ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


      
-- =========================================================================== --
-- Служебные функции
-- =========================================================================== --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- получить список услуг на лицевом счете в одну строку
--
FUNCTION Get_AccSrv_list(p_account_id IN VARCHAR2) RETURN VARCHAR2
IS
    v_services   VARCHAR2(200);
    v_count      INTEGER := 0;
BEGIN
    FOR r_ord IN (
         SELECT S.SERVICE_CODE, COUNT(*) NBR
           FROM ORDER_T O, SERVICE_T S
          WHERE O.SERVICE_ID = S.SERVICE_ID
            AND O.ACCOUNT_ID = p_account_id
            AND SYSDATE BETWEEN O.DATE_FROM AND O.DATE_TO
            AND O.STATUS <> 'LOCK'
          GROUP BY S.SERVICE_CODE
      )
    LOOP
        IF v_count > 0 THEN
            v_services := v_services||',';
        END IF;
        v_services := SUBSTR(v_services || r_ord.service_code ||'-'||r_ord.nbr, 1, 200);
        v_count := v_count + 1;
    END LOOP;
    RETURN v_services;
END Get_AccSrv_list;

-- ------------------------------------------------------------------------ --
-- список брендов
-- ------------------------------------------------------------------------ --
PROCEDURE Branch_lst (
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Branch_lst';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
        SELECT CONTRACTOR_ID, CONTRACTOR 
          FROM CONTRACTOR_T CT
         WHERE CT.CONTRACTOR_TYPE = 'XTTK'
         ORDER BY CONTRACTOR
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
--------------------------------------------------
-- Получаем список Агентов по ID поставщика
--------------------------------------------------
PROCEDURE GetAgentsByContractorID(p_recordset OUT t_refc, p_contractor_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetAgentsByContractorID';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      SELECT CONTRACTOR_ID, CONTRACTOR, SHORT_NAME
      FROM  CONTRACTOR_T
      WHERE CONTRACTOR_TYPE = 'AGENT'
      AND PARENT_ID = p_contractor_id
      ORDER BY CONTRACTOR;
      
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ================================================================== --
-- дебиторская задолженность по договорам
-- ================================================================== --
PROCEDURE Contracts_report(
                   p_recordset OUT t_refc,
                   p_branch_id IN INTEGER DEFAULT NULL
               )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Contracts_report';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
    WITH AP AS (
        SELECT AP.PROFILE_ID, AP.ACCOUNT_ID, AP.CONTRACT_ID, AP.BRANCH_ID, AP.AGENT_ID 
          FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A
         WHERE A.ACCOUNT_ID   = AP.ACCOUNT_ID
           AND A.ACCOUNT_TYPE = 'J'
           AND AP.ACTUAL      = 'Y'
           AND A.BILLING_ID IN (2001,2002)
           AND (p_branch_id IS NULL OR AP.BRANCH_ID = p_branch_id)
           --AND AP.CONTRACT_ID = 75134194
    ), BL AS (
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- статистика по выставленным счетам, начиная с времени утверждения входящего баланса
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT B.CONTRACT_ID, B.BILL_ID, B.BILL_NO, B.BILL_DATE, 
               B.REP_PERIOD_ID, B.BILL_STATUS, B.TOTAL,
               SUM(NVL(IB.BALANCE,0)) OVER (PARTITION BY B.CONTRACT_ID) IN_BALANCE,
               SUM(TOTAL) 
                       OVER (PARTITION BY B.CONTRACT_ID 
                             ORDER BY B.REP_PERIOD_ID, B.BILL_DATE, B.BILL_ID
                       ) CURR_TOTAL, -- начислено с нарастающим итогом по договору
               SUM(TOTAL) OVER (PARTITION BY B.CONTRACT_ID) FULL_TOTAL, -- начислено всего
               (SYSDATE-BI.DAYS_FOR_PAYMENT) DUE_DATE -- дата наступления задолженности
          FROM BILL_T B, INCOMING_BALANCE_T IB, BILLINFO_T BI, AP
         WHERE B.BILL_STATUS IN ('CLOSED','READY')
           AND B.ACCOUNT_ID = BI.ACCOUNT_ID
           AND B.TOTAL > 0 -- отрицательные счета и кредит-ноты учитываем как платежи
           AND B.ACCOUNT_ID = IB.ACCOUNT_ID(+)
           AND B.BILL_DATE <= (SYSDATE-BI.DAYS_FOR_PAYMENT)
           AND CASE
                WHEN IB.ACCOUNT_ID IS NULL THEN 1
                WHEN IB.ACCOUNT_ID IS NOT NULL AND B.BILL_DATE     > IB.BALANCE_DATE  THEN 1
                WHEN IB.ACCOUNT_ID IS NOT NULL AND B.REP_PERIOD_ID > IB.REP_PERIOD_ID THEN 1
                ELSE 0
               END = 1
           AND B.PROFILE_ID = AP.PROFILE_ID
         ORDER BY B.CONTRACT_ID, B.REP_PERIOD_ID DESC
    ), PY AS (
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- статистика по принятым платежам, начиная с времени утверждения входящего баланса
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT PM.CONTRACT_ID, PM.LST_PAYMENT_ID, PM.LST_PAY_PERIOD_ID, PM.LST_PAYMENT_DATE, PM.LST_RECVD, PM.SUM_RECVD
          FROM 
          (
            SELECT P.CONTRACT_ID,
                   P.PAYMENT_ID     LST_PAYMENT_ID,
                   P.REP_PERIOD_ID  LST_PAY_PERIOD_ID,
                   P.PAYMENT_DATE   LST_PAYMENT_DATE,
                   P.RECVD          LST_RECVD,
                   ROW_NUMBER() OVER (PARTITION BY P.CONTRACT_ID ORDER BY P.PAYMENT_DATE DESC) RN, 
                   SUM(RECVD) OVER (PARTITION BY P.CONTRACT_ID) SUM_RECVD
              FROM (
                SELECT AP.CONTRACT_ID,
                       P.ACCOUNT_ID,
                       P.PAYMENT_ID,
                       P.REP_PERIOD_ID,
                       P.PAYMENT_DATE,
                       P.RECVD
                  FROM PAYMENT_T P, AP
                 WHERE P.ACCOUNT_ID!= 2
                   AND P.ACCOUNT_ID = AP.ACCOUNT_ID
                UNION ALL
                -- отрицательные счета - приравниваем к платежам (в том числе и кредит ноты)
                SELECT B.CONTRACT_ID,
                       B.ACCOUNT_ID, 
                       NULL            PAYMENT_ID,    
                       B.REP_PERIOD_ID PAY_PERIOD,
                       B.BILL_DATE     PAYMENT_DATE,
                       -B.TOTAL        RECVD
                  FROM BILL_T B, AP 
                 WHERE B.BILL_STATUS IN ('CLOSED','READY')
                   AND B.TOTAL < 0
                   AND B.PROFILE_ID = AP.PROFILE_ID
              ) P, INCOMING_BALANCE_T IB
             WHERE P.ACCOUNT_ID != 2
              AND P.ACCOUNT_ID = IB.ACCOUNT_ID(+)
              AND CASE
                     WHEN IB.ACCOUNT_ID IS NULL THEN 1
                     WHEN IB.ACCOUNT_ID IS NOT NULL AND P.REP_PERIOD_ID > IB.REP_PERIOD_ID THEN 1
                     ELSE 0
                  END = 1
          ) PM
          WHERE RN = 1
    ), BAL AS (
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- баланс по лицевым счетам
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT 
               CONTRACT_ID,                              -- ID номер л/с
               (IN_BALANCE + SUM_RECVD) - FULL_TOTAL DUE,-- задолженность по л/с
               IN_BALANCE, SUM_RECVD, FULL_TOTAL, CURR_TOTAL, -- для отладки  
               MAX_BILL_DATE,                           -- дата последнего выставленного счета
               MIN_BILL_DATE,                           -- дата первого неоплаченного счета
               MIN_BILL_NO,                             -- номер первого неоплаченного счета
               LST_PAYMENT_DATE,                        -- дата последнего платежа
               LST_RECVD,                               -- сумма последнего платежа
               DUE_MONS,                                -- кол-во месяцев, с момента последней оплаты  
               DUE_DAYS,                                -- кол-во дней, с момента последней оплаты
               CASE
                 WHEN DUE_DAYS BETWEEN 1  AND 30    THEN '1..30 days'
                 WHEN DUE_DAYS BETWEEN 31 AND 90    THEN '31..90 days'
                 WHEN DUE_DAYS BETWEEN 31 AND 180   THEN '91..180 days'
                 WHEN DUE_DAYS > 180 AND DUE_MONS <= 36 THEN '< 3 year'
                 WHEN DUE_MONS > 36 THEN '> 3 year'
               END DUE_ITV                              -- интервал задолженности
          FROM (
            SELECT  ROW_NUMBER() OVER (PARTITION BY BL.CONTRACT_ID ORDER BY BL.BILL_DATE, BL.BILL_ID) RN, 
                    BL.CONTRACT_ID, 
                    BL.IN_BALANCE,
                    BL.CURR_TOTAL,
                    BL.FULL_TOTAL, 
                    NVL(PY.SUM_RECVD,0) SUM_RECVD,
                    BL.BILL_ID          MIN_BILL_ID, 
                    BL.BILL_NO          MIN_BILL_NO, 
                    BL.BILL_DATE        MIN_BILL_DATE, 
                    MAX(BL.BILL_DATE) OVER (PARTITION BY BL.CONTRACT_ID) MAX_BILL_DATE,
                    LST_PAYMENT_ID, 
                    LST_PAY_PERIOD_ID, 
                    LST_PAYMENT_DATE, 
                    LST_RECVD,
                    ROUND(MONTHS_BETWEEN(BL.DUE_DATE, BL.BILL_DATE)) DUE_MONS,
                    ROUND(BL.DUE_DATE - BL.BILL_DATE)                DUE_DAYS
              FROM BL, PY
             WHERE BL.CONTRACT_ID = PY.CONTRACT_ID(+)
               AND (NVL(PY.SUM_RECVD,0) + BL.IN_BALANCE) < BL.CURR_TOTAL
         )
         WHERE RN = 1 
    ), ST AS (
        -- ------------------------------------------------------------------------------- --
        -- описание клиента на текущий момент времени
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- статус договора в зависимости от кол-ва открытых, не заблокированных заказов
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT CONTRACT_ID,
                   CASE
                     WHEN ORDERS_NUM = 0 THEN 'Неактивный'
                     WHEN CLOSED_NUM = ORDERS_NUM THEN 'Закрыт' -- > 0 AND LK.LOCKS_NUM = 0 THEN 'Закрыт'
                     WHEN LOCKS_NUM  = 0 AND (LOCKS_NUM + CLOSED_NUM) < ORDERS_NUM THEN 'Активный'
                     WHEN LOCKS_NUM  > 0 AND (LOCKS_NUM + CLOSED_NUM) = ORDERS_NUM THEN 'Заблокирован'
                     WHEN LOCKS_NUM  > 0 AND (LOCKS_NUM + CLOSED_NUM) < ORDERS_NUM THEN 'Частично заблокирован'
                   END CONTRACT_STATUS
              FROM (
              SELECT AP.CONTRACT_ID, 
                     SUM(DECODE(L.ORDER_ID, NULL, 0, 1)) LOCKS_NUM,
                     SUM(DECODE(O.STATUS, 'CLOSED',1,0)) CLOSED_NUM,
                     COUNT(*) ORDERS_NUM
                FROM ORDER_T O, ORDER_LOCK_T L, AP
               WHERE O.ORDER_ID = L.ORDER_ID(+)
                 AND L.DATE_TO(+) IS NULL
                 AND L.LOCK_TYPE_ID(+) != 901
                 AND O.DATE_FROM <= SYSDATE
                 AND (O.DATE_TO IS NULL OR SYSDATE < O.DATE_TO)
                 AND AP.ACCOUNT_ID = O.ACCOUNT_ID
              GROUP BY AP.CONTRACT_ID
            )
    ), SR AS (
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- перечень услуг на договоре в формате *.csv
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT CONTRACT_ID, MAX(SRVs) SRVs
          FROM (
            SELECT CONTRACT_ID, SRV, LTRIM(SYS_CONNECT_BY_PATH(SRV, ','), ',') SRVs
              FROM (  
                SELECT CONTRACT_ID, SRV,  SERVICE_ID,
                       LAG(SERVICE_ID) OVER (PARTITION BY CONTRACT_ID ORDER BY SRV) PREV_SERVICE_ID
                  FROM (
                    SELECT AP.CONTRACT_ID, O.SERVICE_ID, S.SERVICE_CODE||'-'||COUNT(*) SRV 
                      FROM ORDER_T O, SERVICE_T S, AP
                     WHERE O.SERVICE_ID  = S.SERVICE_ID
                       AND AP.ACCOUNT_ID = O.ACCOUNT_ID
                     GROUP BY AP.CONTRACT_ID, O.SERVICE_ID, S.SERVICE_CODE
                )
              ) OSRV
              START WITH PREV_SERVICE_ID IS NULL
              CONNECT BY PRIOR SERVICE_ID = PREV_SERVICE_ID AND CONTRACT_ID = PRIOR CONTRACT_ID
          )
         GROUP BY CONTRACT_ID
    ), APC AS (
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- назначаем договору профиль (произвольно)
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT CONTRACT_ID,
               BRANCH, 
               AGENT,
               CITY,                       -- город
               ADDRESS,                    -- адрес
               PHONES,                     -- телефон
               MANAGER
          FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY AP.CONTRACT_ID ORDER BY PROFILE_ID DESC) RN,
                   AP.CONTRACT_ID,
                   BR.CONTRACTOR BRANCH, 
                   AG.CONTRACTOR AGENT,
                   AA.CITY,                       -- город
                   AA.ADDRESS,                    -- адрес
                   AA.PHONES,                     -- телефон
                   PK402_BCR_DATA.Get_sales_curator(
                          AP.BRANCH_ID, 
                          AP.AGENT_ID, 
                          AP.CONTRACT_ID, 
                          AP.ACCOUNT_ID, 
                          NULL, 
                          NULL
                   ) MANAGER
              FROM AP, CONTRACTOR_T BR, CONTRACTOR_T AG, ACCOUNT_CONTACT_T AA
             WHERE AP.BRANCH_ID    = BR.CONTRACTOR_ID
               AND AP.AGENT_ID     = AG.CONTRACTOR_ID(+) 
               AND AP.ACCOUNT_ID   = AA.ACCOUNT_ID
               AND AA.ADDRESS_TYPE = 'JUR'
        ) WHERE RN = 1
    ), CL AS (
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- описание клиента на текущий момент времени
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT  
             NVL(ST.CONTRACT_STATUS, 'Неактивный') CONTRACT_STATUS, -- статус, в зависимости от состояния заказов на л/с
             SR.SRVs,                       -- перечень услуг на л/с в csv-формате
             C.CONTRACT_NO,                 -- номер договора
             C.CONTRACT_ID,                 -- ID договора
             CM.COMPANY_NAME,               -- Наименование компании по договору  
             CM.INN,                        -- ИНН контрагента
             CM.KPP,                        -- КПП контрагента
             CM.ERP_CODE,                   -- код контрагента в 1С
             DECODE(C.CLIENT_TYPE_ID, 6409, 'VIP', 'Юридические лица') VIP, -- VIP клиент
             APC.BRANCH, 
             APC.AGENT,
             APC.CITY,                       -- город
             APC.ADDRESS,                    -- адрес
             APC.PHONES,                     -- телефон
             APC.MANAGER             
          FROM 
             CONTRACT_T C,
             COMPANY_T CM,
             APC,
             ST,
             SR
         WHERE CM.CONTRACT_ID  = C.CONTRACT_ID 
           AND CM.ACTUAL       = 'Y'
           AND APC.CONTRACT_ID  = C.CONTRACT_ID
           AND APC.CONTRACT_ID   = ST.CONTRACT_ID(+)
           AND APC.CONTRACT_ID   = SR.CONTRACT_ID(+)
    )
    -- =================================================================================== --
    -- ИТОГ:
    -- =================================================================================== --
    SELECT  --CL.BILLING_ID,         -- номер биллинга
--            CL.ACCOUNT_ID,         -- ID л/с
--            CL.ACCOUNT_NO,         -- номер л/с
            CL.CONTRACT_NO,        -- номер договора
            CL.CONTRACT_STATUS,     -- статус, в зависимости от состояния заказов на л/с
--            CL.BALANCE,            -- баланс на л.с
            --
            BAL.DUE,               -- задолженность по л/с
            BAL.MAX_BILL_DATE,     -- дата последнего выставленного счета
            BAL.MIN_BILL_DATE,     -- дата первого неоплаченного счета
            BAL.MIN_BILL_NO,       -- номер первого неоплаченного счета
            BAL.LST_PAYMENT_DATE,  -- дата последнего платежа
            BAL.LST_RECVD,         -- сумма последнего платежа
            BAL.DUE_MONS,          -- кол-во месяцев, с момента последней оплаты  
            BAL.DUE_DAYS,          -- кол-во дней, с момента последней оплаты
            BAL.DUE_ITV,           -- интервал задолженности
            --
--            CL.PROFILE_ID,         -- текущий профиль нп л/с
--            CL.CONTRACT_ID,        -- ID договора
            CL.SRVs,               -- перечень услуг на л/с в csv-формате
            CL.COMPANY_NAME,       -- Наименование компании по договору  
            CL.INN,                -- ИНН контрагента
            CL.KPP,                -- КПП контрагента
            CL.ERP_CODE,           -- код контрагента в 1С
            CL.CITY,               -- город
            CL.ADDRESS,            -- адрес
            CL.PHONES,             -- телефон
--            CL.BRANCH_ID,          -- ID бренда продавца
            CL.BRANCH,             -- бренд продавца
--            CL.AGENT_ID,           -- ID агента
            CL.AGENT,              -- агент
            CL.VIP,                -- VIP клиент
            CL.MANAGER             -- Продавец (sales curator)
      FROM BAL, CL
     WHERE BAL.CONTRACT_ID = CL.CONTRACT_ID
     ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ================================================================== --
-- проверка расчета дебиторской задолженности по договорам
-- ================================================================== --
PROCEDURE Contracts_report_check(
                   p_recordset OUT t_refc,
                   p_contract_no IN INTEGER DEFAULT NULL
               )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Contracts_report_check';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
    WITH AP AS (
        SELECT AP.PROFILE_ID, AP.ACCOUNT_ID, AP.CONTRACT_ID, AP.BRANCH_ID, AP.AGENT_ID, 
               A.ACCOUNT_NO, C.CONTRACT_NO 
          FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A, CONTRACT_T C
         WHERE A.ACCOUNT_ID   = AP.ACCOUNT_ID
           AND A.ACCOUNT_TYPE = 'J'
           AND AP.ACTUAL      = 'Y'
           AND A.BILLING_ID IN (2001,2002)
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND C.CONTRACT_NO  = 'SA152032' 
    ), BL AS (
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- статистика по выставленным счетам, начиная с времени утверждения входящего баланса
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT AP.CONTRACT_NO, AP.ACCOUNT_NO, AP.ACCOUNT_ID,
               B.CONTRACT_ID, B.BILL_ID, B.BILL_NO, B.BILL_DATE, 
               B.REP_PERIOD_ID, B.BILL_STATUS, B.TOTAL,
               SUM(NVL(IB.BALANCE,0)) OVER (PARTITION BY B.CONTRACT_ID) IN_BALANCE,
               SUM(TOTAL) 
                       OVER (PARTITION BY B.CONTRACT_ID 
                             ORDER BY B.REP_PERIOD_ID, B.BILL_DATE, B.BILL_ID
                       ) CURR_TOTAL, -- начислено с нарастающим итогом по договору
               SUM(TOTAL) OVER (PARTITION BY B.CONTRACT_ID) FULL_TOTAL, -- начислено всего
               (SYSDATE-BI.DAYS_FOR_PAYMENT) DUE_DATE -- дата наступления задолженности
          FROM BILL_T B, INCOMING_BALANCE_T IB, BILLINFO_T BI, AP
         WHERE B.BILL_STATUS IN ('CLOSED','READY')
           AND B.ACCOUNT_ID = BI.ACCOUNT_ID
           AND B.TOTAL > 0 -- отрицательные счета и кредит-ноты учитываем как платежи
           AND B.ACCOUNT_ID = IB.ACCOUNT_ID(+)
           AND B.BILL_DATE <= (SYSDATE-BI.DAYS_FOR_PAYMENT)
           AND CASE
                WHEN IB.ACCOUNT_ID IS NULL THEN 1
                WHEN IB.ACCOUNT_ID IS NOT NULL AND B.BILL_DATE     > IB.BALANCE_DATE  THEN 1
                WHEN IB.ACCOUNT_ID IS NOT NULL AND B.REP_PERIOD_ID > IB.REP_PERIOD_ID THEN 1
                ELSE 0
               END = 1
           AND B.PROFILE_ID = AP.PROFILE_ID
         ORDER BY B.CONTRACT_ID, B.REP_PERIOD_ID DESC
    ), PY AS (
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- статистика по принятым платежам, начиная с времени утверждения входящего баланса
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT PM.CONTRACT_ID, 
               PM.LST_PAYMENT_ID, 
               PM.LST_PAY_PERIOD_ID, 
               PM.LST_PAYMENT_DATE, 
               PM.LST_RECVD, 
               PM.SUM_RECVD
          FROM 
          (
            SELECT P.CONTRACT_ID,
                   P.PAYMENT_ID     LST_PAYMENT_ID,
                   P.REP_PERIOD_ID  LST_PAY_PERIOD_ID,
                   P.PAYMENT_DATE   LST_PAYMENT_DATE,
                   P.RECVD          LST_RECVD,
                   ROW_NUMBER() OVER (PARTITION BY P.CONTRACT_ID ORDER BY P.PAYMENT_DATE DESC) RN, 
                   SUM(RECVD) OVER (PARTITION BY P.CONTRACT_ID) SUM_RECVD
              FROM (
                SELECT AP.CONTRACT_ID,
                       P.ACCOUNT_ID,
                       P.PAYMENT_ID,
                       P.REP_PERIOD_ID,
                       P.PAYMENT_DATE,
                       P.RECVD
                  FROM PAYMENT_T P, AP
                 WHERE P.ACCOUNT_ID!= 2
                   AND P.ACCOUNT_ID = AP.ACCOUNT_ID
                UNION ALL
                -- отрицательные счета - приравниваем к платежам (в том числе и кредит ноты)
                SELECT B.CONTRACT_ID,
                       B.ACCOUNT_ID, 
                       NULL            PAYMENT_ID,    
                       B.REP_PERIOD_ID PAY_PERIOD,
                       B.BILL_DATE     PAYMENT_DATE,
                       -B.TOTAL        RECVD
                  FROM BILL_T B, AP 
                 WHERE B.BILL_STATUS IN ('CLOSED','READY')
                   AND B.TOTAL < 0
                   AND B.PROFILE_ID = AP.PROFILE_ID
              ) P, INCOMING_BALANCE_T IB
             WHERE P.ACCOUNT_ID != 2
              AND P.ACCOUNT_ID = IB.ACCOUNT_ID(+)
              AND CASE
                     WHEN IB.ACCOUNT_ID IS NULL THEN 1
                     WHEN IB.ACCOUNT_ID IS NOT NULL AND P.REP_PERIOD_ID > IB.REP_PERIOD_ID THEN 1
                     ELSE 0
                  END = 1
          ) PM
          WHERE RN = 1
    ), BAL AS (
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- баланс по лицевым счетам
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        SELECT RN,
               CONTRACT_NO,
               ACCOUNT_NO,
               BILL_DATE,
               BILL_NO,
               BILL_TOTAL,
               CURR_TOTAL,
               FULL_TOTAL,
               SUM_RECVD,
               IN_BALANCE,
               (IN_BALANCE + SUM_RECVD) - CURR_TOTAL CURR_DUE,-- задолженность по л/с
               (IN_BALANCE + SUM_RECVD) - FULL_TOTAL FULL_DUE,-- задолженность по л/с
               LST_PAYMENT_DATE,                        -- дата последнего платежа
               LST_RECVD,                               -- сумма последнего платежа
               CONTRACT_ID,                             -- ID договора
               ACCOUNT_ID,
               BILL_ID,
               REP_PERIOD_ID
          FROM (
            SELECT  ROW_NUMBER() OVER (PARTITION BY BL.CONTRACT_ID ORDER BY BL.BILL_DATE, BL.BILL_ID) RN, 
                    BL.CONTRACT_ID,
                    BL.CONTRACT_NO,
                    BL.ACCOUNT_ID,
                    BL.ACCOUNT_NO,
                    BL.IN_BALANCE,
                    BL.TOTAL BILL_TOTAL,
                    BL.CURR_TOTAL,
                    BL.FULL_TOTAL, 
                    NVL(PY.SUM_RECVD,0) SUM_RECVD,
                    BL.BILL_ID, 
                    BL.REP_PERIOD_ID,
                    BL.BILL_NO, 
                    BL.BILL_DATE, 
                    MAX(BL.BILL_DATE) OVER (PARTITION BY BL.CONTRACT_ID) MAX_BILL_DATE,
                    LST_PAYMENT_ID, 
                    LST_PAY_PERIOD_ID, 
                    LST_PAYMENT_DATE, 
                    LST_RECVD
              FROM BL, PY
             WHERE BL.CONTRACT_ID = PY.CONTRACT_ID(+)
--               AND (NVL(PY.SUM_RECVD,0) + BL.IN_BALANCE) < BL.CURR_TOTAL
         )
--         WHERE RN = 1 
    )
    SELECT * FROM BAL
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- ================================================================== --
-- Список договоров для сверки с 1С
-- ================================================================== --
PROCEDURE Contracts_list(
                   p_recordset OUT t_refc
               )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Contracts_list';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
        WITH B AS (
            SELECT CONTRACT_ID, BILL_NO
              FROM (
                SELECT B.CONTRACT_ID, B.BILL_NO, 
                       ROW_NUMBER() OVER (PARTITION BY B.CONTRACT_ID ORDER BY B.REP_PERIOD_ID DESC, B.BILL_NO DESC ) RN 
                  FROM BILL_T B, ACCOUNT_T A
                 WHERE B.ACCOUNT_ID   = A.ACCOUNT_ID
                   AND A.ACCOUNT_TYPE = 'J'
                   AND B.BILL_TYPE    = 'B'
              )
             WHERE RN = 1  
        )
        SELECT CONTRACT_NO, CONTRACT_ID, ERP_CODE, BILL_NO, COMPANY_NAME
          FROM (
            SELECT C.CONTRACT_NO, CM.CONTRACT_ID, CM.ERP_CODE, B.BILL_NO, CM.COMPANY_NAME,
                   ROW_NUMBER() OVER (PARTITION BY CONTRACT_NO ORDER BY AP.DATE_FROM DESC) RN 
              FROM CONTRACT_T C, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, COMPANY_T CM, B
             WHERE A.ACCOUNT_TYPE = 'J'
               AND A.ACCOUNT_ID   = AP.ACCOUNT_ID
               AND AP.ACTUAL      = 'Y'
               AND AP.CONTRACT_ID = C.CONTRACT_ID
               AND AP.CONTRACT_ID = CM.CONTRACT_ID
               AND CM.ACTUAL      = 'Y'
               AND C.CONTRACT_ID  = B.CONTRACT_ID(+)
               --AND C.CONTRACT_NO  = '*06-336'
        ) 
        WHERE RN = 1
        ORDER BY ERP_CODE, CONTRACT_NO
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;    


END PK10_PAYMENT_EISUP_REPORT;
/
