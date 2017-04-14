CREATE OR REPLACE PACKAGE PK36_BILLING_FIXRATE
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK36_BILLING_FIXRATE';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;

    --=============================================================================--
    --                РАСЧЕТ НАЧИСЛЕНИЙ ПО ПРОСТЫМ ТАРИФАМ                         --
    --=============================================================================--
    -- Создание периодических счетов для клиентов имеющих
    -- абонплату или доплату до минимальной стоимости
    -- в биллинговом периоде p_period_id
    PROCEDURE Make_bills_for_fixrates(p_period_id IN INTEGER);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --   С Т А Н Д А Р Т Н Ы Й   Р А С Ч Е Т   А Б О Н П Л А Т Ы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Стандартный расчет за отработанные дни в расчетном периоде
    --
    -- RATE_RULE_ID IN ( Pk00_Const.c_RATE_RULE_ABP_STD,       -- 2402
    --                   Pk00_Const.c_RATE_RULE_ABP_FREE_MIN ) -- 2403
    --
    FUNCTION Calc_ABP_std (
                p_rate_value IN NUMBER, 
                p_date_from  IN DATE, 
                p_date_to    IN DATE
             ) RETURN NUMBER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  Расчет абонплаты, из расчета что 1 день стоит 1/30 абонки
    -- ITEM_TOTAL = Т/30 дней* N, где
    -- Т - тариф
    -- N - кол-во отработанных дней в календарном месяце
    -- Т.е. при определении стоимости одного дня участвует 
    -- не кол-во дней в календарном месяце, а всегда 30 дней
    --
    -- RATE_RULE_ID = Pk00_Const.c_RATE_RULE_ABP_30DAYS -- 2417
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Calc_ABP_30days (
                p_rate_value IN NUMBER, 
                p_date_from  IN DATE, 
                p_date_to    IN DATE
             ) RETURN NUMBER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  Расчет абонплаты по тарифам укзанным 
    -- индивидуально для каждого месяца (MONTH_TARIFF_T),
    -- такой тариф иногда требуют бюджетные организации, 
    -- заключающие договор на один год, затем тендер и новый договор
    --
    -- RATE_RULE_ID = Pk00_Const.c_RATE_RULE_ABP_MON -- 2405
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Calc_ABP_by_month_tariff (
                p_rateplan_id IN INTEGER, 
                p_period_id   IN INTEGER,
                p_date_from   IN DATE, 
                p_date_to     IN DATE
             ) RETURN NUMBER;

    --  Стандарный расчет абонплаты (subscriber fee)
    PROCEDURE Charge_ABP( p_task_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  Заполнить поля необходимые для получения детализации по абонплате
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Put_ABP_detail( p_task_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  Расчет доплаты до мимнимальной суммы ЗАКАЗА 
    -- (стандартный вариант)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Сharge_MIN( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расформирование фиксированных начислений,
    -- за исключением тех что сформировал тарификатор трафика
    PROCEDURE Rollback_fixrates( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Начисление абонентской платы и доплаты до минимальной суммы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Charge_fixrates( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Нестандартные правила начисления АБП и Доплаты до минималки
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Charge_non_std_fixrates( p_task_id IN INTEGER );
       
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --     В С П О М О Г А Т Е Л Ь Н Ы Е   Ф У Н К Ц И И
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Функция возвращающая кол-во дней в периоде
    FUNCTION Get_period_days( p_period_id IN INTEGER) RETURN INTEGER DETERMINISTIC;
    
    -- Функция для вычисления кол-во дней, когда предоставлялась услуга,
    -- в биллинговом периоде, без учета блокировок
    FUNCTION Get_order_days(
             p_period_id     IN INTEGER,
             p_order_body_id IN INTEGER
      ) RETURN NUMBER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Функция вычисления понижающего коэффициента к ежемесячному платежу, 
    -- по кол-ву дней, когда услуга реально предоставлялась
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --FUNCTION Period_ratio(
    --         p_period_id     IN INTEGER,
    --         p_order_body_id IN INTEGER
    --  ) RETURN NUMBER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Процедура компенсации времени блокировок в начислениях абонплаты
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Lock_abp_processing(p_task_id IN INTEGER);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Процедура компенсации времени блокировок в доплате до минимальной стоимости
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Lock_min_processing(p_task_id IN INTEGER);    
    
END PK36_BILLING_FIXRATE;
/
CREATE OR REPLACE PACKAGE BODY PK36_BILLING_FIXRATE
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Создание периодических счетов для клиентов имеющих
-- абонплату или доплату до минимальной стоимости
-- в биллинговом периоде p_period_id
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Make_bills_for_fixrates(p_period_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Make_bills_for_fixrates';
    v_bill_id       INTEGER;
    v_count_ok      INTEGER := 0;
    v_count_err     INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем периодические счета в биллинговом периоде для л/с где их нет
    --
    FOR rb IN (
      SELECT DISTINCT A.ACCOUNT_ID 
        FROM ORDER_BODY_T OB, ORDER_T O, ACCOUNT_T A, PERIOD_T P
       WHERE OB.CHARGE_TYPE IN (PK00_CONST.c_CHARGE_TYPE_MIN, PK00_CONST.c_CHARGE_TYPE_REC)
         AND OB.ORDER_ID = O.ORDER_ID 
         AND A.ACCOUNT_ID = O.ACCOUNT_ID
         AND A.STATUS = Pk00_Const.c_ACC_STATUS_BILL
         AND O.DATE_FROM <= P.PERIOD_TO
         AND ( O.DATE_TO IS NULL OR P.PERIOD_FROM <= O.DATE_TO) 
         AND OB.DATE_FROM <= P.PERIOD_TO
         AND ( OB.DATE_TO IS NULL OR P.PERIOD_FROM <=  OB.DATE_TO)
         AND P.PERIOD_ID = p_period_id
         AND NOT EXISTS (
            SELECT * FROM BILL_T B
             WHERE B.REP_PERIOD_ID = P.PERIOD_ID
               AND B.ACCOUNT_ID    = A.ACCOUNT_ID
               AND B.BILL_TYPE     = PK00_CONST.c_BILL_TYPE_REC
         )
         AND NOT EXISTS (  -- заказ не должен быть заблокирован весь период
             SELECT * 
               FROM ORDER_LOCK_T L
              WHERE L.DATE_FROM <= P.PERIOD_FROM
                AND (L.DATE_TO IS NULL OR L.DATE_TO >= P.PERIOD_TO)
                AND L.ORDER_ID = O.ORDER_ID
         )
    )LOOP
      BEGIN
          v_bill_id := Pk07_BIll.Next_recuring_bill (
                   p_account_id    => rb.account_id, -- ID лицевого счета
                   p_rep_period_id => p_period_id    -- ID расчетного периода YYYYMM
               );
          v_count_ok := v_count_ok + 1;
      EXCEPTION WHEN OTHERS THEN
          Pk01_Syslog.Write_error('ERROR', c_PkgName||'.'||v_prcName );
          v_count_err := v_count_err + 1;
      END;
    END LOOP;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count_ok||'/'||v_count_err||' ok/err rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--   С Т А Н Д А Р Т Н Ы Й   Р А С Ч Е Т   А Б О Н П Л А Т Ы
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Стандартный расчет за отработанные дни в расчетном периоде
--
-- RATE_RULE_ID IN ( Pk00_Const.c_RATE_RULE_ABP_STD,       -- 2402
--                   Pk00_Const.c_RATE_RULE_ABP_FREE_MIN ) -- 2403
--
FUNCTION Calc_ABP_std (
            p_rate_value IN NUMBER, 
            p_date_from  IN DATE, 
            p_date_to    IN DATE
         ) RETURN NUMBER IS
BEGIN
    RETURN p_rate_value * 
         ROUND(p_date_to - p_date_from) / TO_NUMBER(TO_CHAR(LAST_DAY(p_date_to), 'DD'));
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  Расчет абонплаты, из расчета что 1 день стоит 1/30 абонки
-- ITEM_TOTAL = Т/30 дней* N, где
-- Т - тариф
-- N - кол-во отработанных дней в календарном месяце
-- Т.е. при определении стоимости одного дня участвует 
-- не кол-во дней в календарном месяце, а всегда 30 дней
--
-- RATE_RULE_ID = Pk00_Const.c_RATE_RULE_ABP_30DAYS -- 2417
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Calc_ABP_30days (
            p_rate_value IN NUMBER, 
            p_date_from  IN DATE, 
            p_date_to    IN DATE
         ) RETURN NUMBER IS
BEGIN
    -- нестандартный расчет только за неполный месяц
    IF ROUND(p_date_to-p_date_from) < ADD_MONTHS(p_date_from,1)-p_date_from THEN
        RETURN ROUND(p_rate_value/30, 2) * ROUND(p_date_to - p_date_from);
    ELSE -- иначе стандартный
        RETURN Calc_ABP_std (
            p_rate_value, 
            p_date_from, 
            p_date_to
         );
    END IF;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  Расчет абонплаты по тарифам укзанным 
-- индивидуально для каждого месяца (MONTH_TARIFF_T),
-- такой тариф иногда требуют бюджетные организации, 
-- заключающие договор на один год, затем тендер и новый договор
--
-- RATE_RULE_ID = Pk00_Const.c_RATE_RULE_ABP_MON -- 2405
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Calc_ABP_by_month_tariff (
            p_rateplan_id IN INTEGER, 
            p_period_id   IN INTEGER,
            p_date_from   IN DATE, 
            p_date_to     IN DATE
         ) RETURN NUMBER 
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Calc_ABP_by_month_tariff';
    v_rate_value  NUMBER;
    v_period_days INTEGER;
BEGIN
    -- получаем тариф из таблицы
    SELECT T.PRICE 
      INTO v_rate_value
      FROM MONTH_TARIFF_T T
     WHERE T.RATEPLAN_ID = p_rateplan_id
       AND PERIOD_ID     = p_period_id;  
    -- кол-во дней в месяце
    v_period_days := TO_NUMBER(TO_CHAR(LAST_DAY(p_date_to), 'DD'));
    -- возвращаем абонплату
    RETURN v_rate_value * ROUND(p_date_to - p_date_from) / v_period_days;
EXCEPTION 
    WHEN NO_DATA_FOUND THEN
        Pk01_Syslog.Write_error('rate_plan_id='||p_rateplan_id||', '||
                                'period_id='||p_period_id||' - price not found '
                                , c_PkgName||'.'||v_prcName );
        RETURN 0;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  Расчет абонплаты (subscriber fee)
-- для биллингового периода p_period_id
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_ABP( p_task_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Charge_ABP';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем позиции начислений на абонплату для счетов биллингового периода
    -- в валюте тарифа
    INSERT INTO ITEM_T I(
       BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
       CHARGE_TYPE, ITEM_TOTAL, ITEM_CURRENCY_ID, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
       ITEM_STATUS, ORDER_BODY_ID
    )
    WITH IT AS (   -- заказы, имющие компоменту услуги - АБОНПЛАТА
        SELECT B.BILL_ID, B.REP_PERIOD_ID, Pk02_Poid.Next_item_id ITEM_ID,
               O.ORDER_ID, O.SERVICE_ID, OB.SUBSERVICE_ID, OB.CHARGE_TYPE,
               OB.RATE_VALUE * OB.QUANTITY RATE_VALUE,
               OB.CURRENCY_ID,
               TRUNC( GREATEST(O.DATE_FROM, OB.DATE_FROM, P.PERIOD_FROM)) DATE_FROM,
               TRUNC( LEAST(NVL(O.DATE_TO,P.PERIOD_TO),
                      NVL(OB.DATE_TO,P.PERIOD_TO),
                      P.PERIOD_TO 
                     )) + 86399/86400 DATE_TO,
               OB.TAX_INCL, 
               PK00_CONST.c_ITEM_TYPE_BILL ITEM_TYPE,
               Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS, 
               OB.ORDER_BODY_ID,
               OB.RATEPLAN_ID,
               OB.RATE_RULE_ID
          FROM ORDER_BODY_T OB, ORDER_T O, 
               PERIOD_T P, BILL_T B, 
               BILLING_QUEUE_T Q
         WHERE OB.CHARGE_TYPE       = Pk00_Const.c_CHARGE_TYPE_REC
           AND OB.RATE_RULE_ID IN  (  
                                      Pk00_Const.c_RATE_RULE_ABP_STD,      -- 2402
                                      Pk00_Const.c_RATE_RULE_ABP_FREE_MIN, -- 2403
                                      Pk00_Const.c_RATE_RULE_ABP_MON,      -- 2405
                                      Pk00_Const.c_RATE_RULE_ABP_30DAYS    -- 2417
                                    ) 
           AND O.ORDER_ID           = OB.ORDER_ID
           AND P.PERIOD_ID          = NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID) 
           AND Q.ACCOUNT_ID         = O.ACCOUNT_ID
           AND P.PERIOD_TO         >= GREATEST(O.DATE_FROM, OB.DATE_FROM) 
           AND P.PERIOD_FROM       <= LEAST(NVL(O.DATE_TO,P.PERIOD_TO), NVL(OB.DATE_TO,P.PERIOD_TO))
           AND Q.TASK_ID            = p_task_id
           AND Q.REP_PERIOD_ID      = B.REP_PERIOD_ID
           AND Q.BILL_ID            = B.BILL_ID
           AND OB.RATE_VALUE   IS NOT NULL
           AND OB.QUANTITY     IS NOT NULL
           AND B.BILL_TYPE     IN (Pk00_Const.c_BILL_TYPE_REC, 
                                   Pk00_Const.c_BILL_TYPE_DBT,
                                   Pk00_Const.c_BILL_TYPE_ADS,
                                   Pk00_Const.c_BILL_TYPE_OLD)
           AND NOT EXISTS (   -- не должно быть начислений абонплаты за указанный период
               SELECT * 
                 FROM ITEM_T I
                WHERE I.REP_PERIOD_ID = B.REP_PERIOD_ID
                  AND I.BILL_ID       = B.BILL_ID
                  AND I.ORDER_BODY_ID = OB.ORDER_BODY_ID
                  AND I.DATE_FROM BETWEEN P.PERIOD_FROM AND P.PERIOD_TO
                  AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_REC
                  AND I.ITEM_TYPE     = Pk00_Const.c_ITEM_TYPE_BILL
           )
           AND NOT EXISTS (  -- заказ не должен быть заблокирован весь период
               SELECT * 
                 FROM ORDER_LOCK_T L
                WHERE L.DATE_FROM <= P.PERIOD_FROM
                  AND (L.DATE_TO IS NULL OR L.DATE_TO >= P.PERIOD_TO)
                  AND L.ORDER_ID = O.ORDER_ID
           )
    )
    SELECT * 
      FROM (
        SELECT 
            BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
            CHARGE_TYPE, 
            -- стандартные варианты расчета абонплаты
            CASE
               WHEN RATE_RULE_ID IN (Pk00_Const.c_RATE_RULE_ABP_STD,           -- 2402
                                     Pk00_Const.c_RATE_RULE_ABP_FREE_MIN) THEN -- 2403
                  PK36_BILLING_FIXRATE.Calc_ABP_std (
                      p_rate_value  => RATE_VALUE, 
                      p_date_from   => DATE_FROM, 
                      p_date_to     => DATE_TO
                  )
               WHEN RATE_RULE_ID = Pk00_Const.c_RATE_RULE_ABP_MON    THEN      -- 2405
                  PK36_BILLING_FIXRATE.Calc_ABP_by_month_tariff (
                      p_rateplan_id => RATEPLAN_ID, 
                      p_period_id   => REP_PERIOD_ID,
                      p_date_from   => DATE_FROM, 
                      p_date_to     => DATE_TO
                  )
               WHEN RATE_RULE_ID = Pk00_Const.c_RATE_RULE_ABP_30DAYS THEN      -- 2417
                  PK36_BILLING_FIXRATE.Calc_ABP_30days (
                      p_rate_value  => RATE_VALUE, 
                      p_date_from   => DATE_FROM, 
                      p_date_to     => DATE_TO
                  )
               ELSE 0
            END ITEM_TOTAL, 
            CURRENCY_ID, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
            ITEM_STATUS, ORDER_BODY_ID 
          FROM IT
       )
     WHERE ITEM_TOTAL > 0
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  Заполнить поля необходимые для получения детализации по абонплате
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Put_ABP_detail( p_task_id IN INTEGER )    
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Put_ABP_detail';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Заполняем поля описания начислений абонплаты для стандартной детализации
    MERGE INTO ITEM_T I
    USING (
        SELECT I.ITEM_ID,
               CASE 
                    WHEN I.SERVICE_ID IN (101, 133, 126, 103, 131, 129, 181) 
                      THEN -- 2 точки
                        INF.POINT_SRC||DECODE(INF.POINT_DST, NULL, NULL,' - '||INF.POINT_DST)||', '||INF.SPEED_STR                        
                    WHEN I.SERVICE_ID = 104 
                      THEN 'IP port, '||INF.POINT_SRC||', '||INF.SPEED_STR
                    WHEN I.SERVICE_ID IN (149,106,146,103,159,108,160,122,172,150,165)  
                      THEN INF.POINT_SRC||', '||INF.SPEED_STR
                    ELSE NULL
               END STR
          FROM ORDER_INFO_T INF, ITEM_T I, BILLING_QUEUE_T Q
        WHERE 1=1
          -- KiriLL изменил от 02.12.2015. 
          -- Если ограничить - не проставляются примечания, 
          -- которые были сделаны руками для других типов.
          --AND I.CHARGE_TYPE   IN ( Pk00_Const.c_CHARGE_TYPE_REC, 
          --                        Pk00_Const.c_CHARGE_TYPE_USG )  
          AND I.CHARGE_TYPE NOT IN (
                                --Pk00_Const.c_CHARGE_TYPE_MIN,
                                Pk00_Const.c_CHARGE_TYPE_DIS,
                                Pk00_Const.c_CHARGE_TYPE_IDL,
                                Pk00_Const.c_CHARGE_TYPE_SLA
                            )
          AND INF.ORDER_ID    = I.ORDER_ID
          AND Q.BILL_ID       = I.BILL_ID
          AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID
          AND Q.TASK_ID       = p_task_id
          AND INF.POINT_SRC   IS NOT NULL
    ) INF
    ON (
        I.ITEM_ID = INF.ITEM_ID AND INF.STR IS NOT NULL
    )
    WHEN MATCHED THEN UPDATE SET I.DESCR = INF.STR || DECODE(I.NOTES, NULL, NULL,'.'||I.NOTES);    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows, set desc ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расчет доплаты до мимнимальной суммы ЗАКАЗА (стандартный вариант)
-- Считаем, что валюта тармфа услуги за трафик совпадает с валютой минималки
-- Формируем начисления в валюте тарифа
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Сharge_MIN( p_task_id IN INTEGER )    
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Charge_MIN';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем позиции начислений доплаты до мимнимальной суммы ЗАКАЗА
    --
    INSERT INTO ITEM_T(
        BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
        CHARGE_TYPE, ITEM_TOTAL, ITEM_CURRENCY_ID, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
        ITEM_STATUS, ORDER_BODY_ID
    )
    WITH MP AS (   -- заказы, имеющие компоменту услуги - Доплата до МИНимальной стоимости
        SELECT BILL_ID, BILL_DATE, REP_PERIOD_ID, DATA_PERIOD_ID,
               ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE,
               MIN_VALUE * ROUND( DATE_TO - DATE_FROM )
                         / Pk36_Billing_Fixrate.Get_period_days(DATA_PERIOD_ID) MIN_VALUE,
               DATE_FROM,
               DATE_TO,
               TAX_INCL, 
               ITEM_TYPE,
               ITEM_STATUS, 
               ORDER_BODY_ID,
               CURRENCY_ID
          FROM (
            SELECT B.BILL_ID, B.BILL_DATE, B.REP_PERIOD_ID, P.PERIOD_ID DATA_PERIOD_ID,
                   O.ORDER_ID, O.SERVICE_ID, OB.SUBSERVICE_ID, OB.CHARGE_TYPE,
                   OB.RATE_VALUE MIN_VALUE,
                   TRUNC( GREATEST(O.DATE_FROM, OB.DATE_FROM, P.PERIOD_FROM)) DATE_FROM,
                   TRUNC( LEAST(NVL(O.DATE_TO,P.PERIOD_TO),
                          NVL(OB.DATE_TO,P.PERIOD_TO),
                          P.PERIOD_TO 
                         )) + 86399/86400 DATE_TO,
                   OB.TAX_INCL, 
                   PK00_CONST.c_ITEM_TYPE_BILL ITEM_TYPE,
                   Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS, 
                   OB.ORDER_BODY_ID,
                   OB.CURRENCY_ID
              FROM ORDER_BODY_T OB, ORDER_T O, 
                   PERIOD_T P, BILL_T B, 
                   BILLING_QUEUE_T Q
             WHERE OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_MIN      -- 'MIN'
               AND OB.RATE_RULE_ID = Pk00_Const.c_RATE_RULE_MIN_STD    -- 2401
               AND O.ORDER_ID      = OB.ORDER_ID
               AND P.PERIOD_ID     = NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID) 
               AND P.PERIOD_TO    >= GREATEST(O.DATE_FROM, OB.DATE_FROM) 
               AND P.PERIOD_FROM  <= LEAST(NVL(O.DATE_TO,P.PERIOD_TO), NVL(OB.DATE_TO,P.PERIOD_TO))
               AND Q.TASK_ID       = p_task_id
               AND Q.ACCOUNT_ID    = O.ACCOUNT_ID
               AND Q.REP_PERIOD_ID = B.REP_PERIOD_ID
               AND Q.BILL_ID       = B.BILL_ID
               AND OB.RATE_VALUE   IS NOT NULL
               AND B.BILL_TYPE     IN (Pk00_Const.c_BILL_TYPE_REC,     -- 'B' 
                                       Pk00_Const.c_BILL_TYPE_ADS,     -- 'A'
                                       Pk00_Const.c_BILL_TYPE_DBT,     -- 'D'
                                       Pk00_Const.c_BILL_TYPE_OLD)     -- 'O'
               AND NOT EXISTS (   -- не должно быть начислений минималки за указанный период
                   SELECT * 
                     FROM ITEM_T I
                    WHERE I.REP_PERIOD_ID = B.REP_PERIOD_ID
                      AND I.BILL_ID       = B.BILL_ID
                      AND I.ORDER_BODY_ID = OB.ORDER_BODY_ID
                      AND I.DATE_FROM BETWEEN P.PERIOD_FROM AND P.PERIOD_TO
                      AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_MIN
                      AND I.ITEM_TYPE     = Pk00_Const.c_ITEM_TYPE_BILL
               )
               AND NOT EXISTS (  -- заказ не должен быть заблокирован весь период
                   SELECT * 
                     FROM ORDER_LOCK_T L
                    WHERE L.DATE_FROM <= P.PERIOD_FROM
                      AND (L.DATE_TO IS NULL OR L.DATE_TO >= P.PERIOD_TO)
                      AND L.ORDER_ID = O.ORDER_ID
               )
          )
    ), IT AS ( -- начисления по заказам имеющим компонент Доплата до МИНимальной стоимости
        SELECT I.BILL_ID, I.REP_PERIOD_ID, I.ORDER_ID, I.SERVICE_ID,
               P.PERIOD_ID DATA_PERIOD_ID, 
               SUM( -- приводим начисления к TAX_INCL компоненты минималки       
                 CASE 
                   WHEN OB.TAX_INCL = 'Y' AND I.TAX_INCL = 'N'
                     THEN I.ITEM_TOTAL + (I.ITEM_TOTAL * B.VAT / 100) 
                   WHEN OB.TAX_INCL = 'N' AND I.TAX_INCL = 'Y'
                     THEN I.ITEM_TOTAL /(1 + B.VAT / 100)
                   ELSE I.ITEM_TOTAL 
                 END
               ) SUM_ITEM_TOTAL
          FROM ORDER_BODY_T OB,
               ITEM_T I, BILL_T B, 
               PERIOD_T P, BILLING_QUEUE_T BQ
         WHERE OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_MIN -- 'MIN'
           AND OB.RATE_RULE_ID = Pk00_Const.c_RATE_RULE_MIN_STD
           AND OB.DATE_FROM   <= P.PERIOD_TO
           AND (P.PERIOD_FROM <= OB.DATE_TO OR OB.DATE_TO IS NULL)
           AND OB.ORDER_ID     = I.ORDER_ID
           AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG -- 'USG'    -- только трафик 
           AND I.DATE_FROM    <= P.PERIOD_TO     -- рассматриваем только item-s
           AND I.DATE_TO      >= P.PERIOD_FROM   -- указанного периода
           AND I.BILL_ID       = BQ.BILL_ID
           AND I.REP_PERIOD_ID = BQ.REP_PERIOD_ID
           AND B.BILL_ID       = BQ.BILL_ID
           AND B.REP_PERIOD_ID = BQ.REP_PERIOD_ID
           AND P.PERIOD_ID     = NVL(BQ.DATA_PERIOD_ID, BQ.REP_PERIOD_ID)
           AND BQ.TASK_ID      = p_task_id
         GROUP BY I.BILL_ID, I.REP_PERIOD_ID, I.ORDER_ID, I.SERVICE_ID, P.PERIOD_ID
    ), ITM AS (-- объединяем описания и начисления
      SELECT 
           MP.BILL_ID, MP.REP_PERIOD_ID,  Pk02_Poid.Next_item_id ITEM_ID, 
           MP.ORDER_ID, MP.SERVICE_ID, Pk00_Const.c_SUBSRV_MIN SUBSERVICE_ID, 
           MP.CHARGE_TYPE,
           -- считаем что валюта трафика и минималки совпадают, начиления в валюте тарифа
           (MP.MIN_VALUE - NVL(IT.SUM_ITEM_TOTAL, 0)) ITEM_TOTAL,
           MP.CURRENCY_ID,
           MP.DATE_FROM, MP.DATE_TO, MP.TAX_INCL, 
           PK00_CONST.c_ITEM_TYPE_BILL,
           Pk00_Const.c_ITEM_STATE_OPEN,
           MP.ORDER_BODY_ID
        FROM MP, IT
       WHERE MP.BILL_ID        = IT.BILL_ID(+)
         AND MP.REP_PERIOD_ID  = IT.REP_PERIOD_ID(+)
         AND MP.DATA_PERIOD_ID = IT.DATA_PERIOD_ID(+)
         AND MP.ORDER_ID       = IT.ORDER_ID(+)
         AND MP.SERVICE_ID     = IT.SERVICE_ID(+)
         AND MP.MIN_VALUE > 0
    )
    SELECT * 
      FROM ITM
     WHERE ITEM_TOTAL > 0
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расформирование фиксированных начислений,
-- за исключением тех что сформировал тарификатор трафика
--
PROCEDURE Rollback_fixrates( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_fixrates';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    DELETE FROM ITEM_T I
     WHERE I.CHARGE_TYPE IN (
                           Pk00_Const.c_CHARGE_TYPE_MIN, 
                           Pk00_Const.c_CHARGE_TYPE_REC)
       AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
       AND I.ITEM_TYPE   = PK00_CONST.c_ITEM_TYPE_BILL
       AND I.EXTERNAL_ID IS NULL -- это важно, чтобы не зацепить мимнималки, сформированные предбиллингом
       AND EXISTS (
        SELECT * FROM BILLING_QUEUE_T Q
         WHERE Q.BILL_ID       = I.BILL_ID
           AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID
           AND (Q.DATA_PERIOD_ID IS NULL 
                OR Q.DATA_PERIOD_ID = PK04_PERIOD.Period_id(I.DATE_FROM)
                )
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

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Нестандартные правила начисления АБП и Доплаты до минималки
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_non_std_fixrates( p_task_id IN INTEGER )
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Charge_non_std_fixrates';
    v_count        INTEGER;
    v_item_total   NUMBER;
    v_bill_minutes NUMBER;
    v_charge_value NUMBER;
    v_min_value    NUMBER;
    v_note         ITEM_T.NOTES%TYPE;
    --
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    FOR rq IN (
        SELECT Q.BILL_ID, Q.ACCOUNT_ID, Q.REP_PERIOD_ID, 
               O.ORDER_ID, O.ORDER_NO, OB.ORDER_BODY_ID, OB.RATE_RULE_ID, 
               O.SERVICE_ID, OB.SUBSERVICE_ID, 
               OB.RATE_VALUE, OB.TAX_INCL, OB.CURRENCY_ID,
               P.PERIOD_ID DATA_PERIOD_ID, P.PERIOD_FROM, P.PERIOD_TO
          FROM BILLING_QUEUE_T Q, ORDER_T O, ORDER_BODY_T OB, PERIOD_T P
         WHERE Q.TASK_ID    = p_task_id
           AND P.PERIOD_ID  = NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID)
           AND Q.ACCOUNT_ID = O.ACCOUNT_ID
           AND O.ORDER_ID   = OB.ORDER_ID
           AND OB.RATE_RULE_ID IN (
             Pk00_Const.c_RATE_RULE_MIN_SUBS, -- := 2415; -- Начисление до минимальной суммы по компоненту услуги
             Pk00_Const.c_RATE_RULE_MIN_ACC,  -- := 2416; -- Начисление до минимальной суммы по всему л/с
             Pk00_Const.c_RATE_RULE_RP001190, -- := 2430; -- СПб: л/с RP001190 ООО "Смарт Телеком" 
             Pk00_Const.c_RATE_RULE_RP001186, -- := 2431; -- СПб: л/с RP001186 ЗАО "VMB-Сервис"
             Pk00_Const.c_RATE_RULE_IP_FIX_VOLIN,-- 2406; -- Фиксированный с учетом объема исходящего трафика' 
             Pk00_Const.c_RR_IP_BURST_VOLIN_REC  -- 2419; -- Абонплата для BURST с учетом объема исходящего трафика
           ) -- для страховки
      )
    LOOP
      IF rq.Rate_Rule_Id = Pk00_Const.c_RATE_RULE_MIN_SUBS THEN
          -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
          -- 2415. Начисление до минимальной суммы по компоненту услуги
          -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
          -- получаем сумму начислений по указанной компоненте
          SELECT SUM(I.ITEM_TOTAL) INTO v_item_total 
            FROM ITEM_T I
           WHERE I.BILL_ID = rq.Bill_Id
             AND I.REP_PERIOD_ID = rq.Rep_Period_Id
             AND I.SUBSERVICE_ID = rq.Subservice_Id
             AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG;
          --
          -- получаем откорректированное значение минималки
          v_min_value := rq.rate_value 
                         * Get_order_days(rq.Rep_Period_Id,rq.Order_Body_Id )
                         / Get_period_days(rq.Rep_Period_Id);
          --
          IF v_min_value > v_item_total THEN
              --
              v_charge_value := (v_min_value-v_item_total);
              --
              INSERT INTO ITEM_T I(
                 BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
                 CHARGE_TYPE, ITEM_TOTAL, ITEM_CURRENCY_ID, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
                 ITEM_STATUS, ORDER_BODY_ID
              ) VALUES (
                 rq.bill_id, rq.rep_period_id, Pk02_Poid.Next_item_id, rq.order_id,
                 rq.Service_Id, rq.Subservice_Id, -- решение А.Ю.Гурова
                 Pk00_Const.c_CHARGE_TYPE_MIN, 
                 v_charge_value, rq.Currency_Id,
                 rq.Period_From, rq.Period_To, rq.Tax_Incl, Pk00_Const.c_ITEM_TYPE_BILL,
                 Pk00_Const.c_ITEM_STATE_OPEN, rq.order_body_id
              );
          END IF;
      ELSIF rq.Rate_Rule_Id = Pk00_Const.c_RATE_RULE_MIN_ACC THEN    
          -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
          -- 2416. Начисление до минимальной суммы по всему л/с
          -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
          -- получаем сумму начислений по трафику для указанного л/с
          SELECT SUM(I.ITEM_TOTAL) INTO v_item_total 
            FROM ITEM_T I
           WHERE I.BILL_ID = rq.Bill_Id
             AND I.REP_PERIOD_ID = rq.Rep_Period_Id
             AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG;
          --
          -- получаем откорректированное значение минималки
          v_min_value := rq.rate_value 
                         * Get_order_days(rq.Rep_Period_Id,rq.Order_Body_Id )
                         / Get_period_days(rq.Rep_Period_Id);
          --
          IF v_min_value > v_item_total  THEN
              --
              v_charge_value := (v_min_value-v_item_total);
              --
              INSERT INTO ITEM_T I(
                 BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
                 CHARGE_TYPE, ITEM_TOTAL, ITEM_CURRENCY_ID, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
                 ITEM_STATUS, ORDER_BODY_ID
              ) VALUES (
                 rq.bill_id, rq.rep_period_id, Pk02_Poid.Next_item_id, rq.order_id,
                 rq.Service_Id, Pk00_Const.c_SUBSRV_MIN,
                 Pk00_Const.c_CHARGE_TYPE_MIN, 
                 v_charge_value, rq.Currency_Id,
                 rq.Period_From, rq.Period_To, rq.Tax_Incl, Pk00_Const.c_ITEM_TYPE_BILL,
                 Pk00_Const.c_ITEM_STATE_OPEN, rq.order_body_id
              );
          END IF;
          
      ELSIF rq.Rate_Rule_Id = Pk00_Const.c_RATE_RULE_RP001190 THEN
          -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
          -- RATE_RULE_ID = 2430
          -- a.    л/с RP001190 ООО "Смарт Телеком"
          -- гарантированный объем трафика считается по двум статьям:
          -- LTERM019 (10050) Местное завершение вызова на сеть другого оператора
          -- LTERM022 (10010) Местное завершение вызова на нумерацию сети связи ОАО «Ростелеком» в коде 812
          -- и составляет 30 000 мин * 15 Е1 = 450 000 мин, недобор выставляется по тарифу 0.25 р/мин
          --
          -- получаем кол-во минут
          SELECT --+ parallel(b 10)
             --TRUNC(b.local_time,'mm') debt_month,
             -- account_id, order_id,   
             --(CASE trf_type  
             --   WHEN 1 THEN 'Завершение'
             --   WHEN 2 THEN 'Инициирование'
             --   WHEN 5 THEN 'Инициирование на платформу'
             -- END) srv_name,
              --COUNT(1) calls,
              ROUND(SUM(bill_minutes),2) bill_minutes
              --ROUND(SUM(duration),2) cdr_seconds,
              --ROUND(SUM(amount),2) amount,
              --MIN(local_time) first_call,
              --MAX(local_time) last_call
              INTO v_bill_minutes
              FROM bdr_oper_t b
             WHERE rep_period between rq.period_from AND rq.period_to 
               AND bdr_status    = 0
               AND bdr_type_id   = 3
               AND trf_type      IN (1,2,5)
               AND account_id    = rq.account_id
               --AND order_id      = rq.order_id
               AND subservice_id IN (10050, 10010)
             --GROUP BY TRUNC(b.local_time,'mm'), account_id, order_id, trf_type
          ;
          --
          IF v_bill_minutes < 450000 THEN
              v_charge_value := (450000 - v_bill_minutes) * 0.25;
              -- проводим начисления
              INSERT INTO ITEM_T I(
                 BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
                 CHARGE_TYPE, ITEM_TOTAL, ITEM_CURRENCY_ID, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
                 ITEM_STATUS, ORDER_BODY_ID, NOTES
              ) VALUES (
                 rq.bill_id, rq.rep_period_id, Pk02_Poid.Next_item_id, rq.order_id,
                 Pk00_Const.c_SERVICE_OP_LOCAL, Pk00_Const.c_SUBSRV_MIN,
                 Pk00_Const.c_CHARGE_TYPE_MIN, v_charge_value, rq.Currency_Id,
                 rq.Period_From, rq.Period_To, 'N', Pk00_Const.c_ITEM_TYPE_BILL,
                 Pk00_Const.c_ITEM_STATE_OPEN, rq.order_body_id,  
                 'Доплата за '||(450000 - v_bill_minutes)||' минут'
              );
              --
              Pk01_Syslog.Write_msg('Bill_id='||rq.bill_id||' RR=2430 - charged.'||
                                    'value='||v_charge_value||' rur, '||
                                    (450000 - v_bill_minutes)||' min'
                                    , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
          END IF;      
      ELSIF rq.Rate_Rule_Id = Pk00_Const.c_RATE_RULE_RP001186 THEN
          -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
          -- RATE_RULE_ID = 2431
          -- b.    л/с RP001186 ЗАО "VMB-Сервис"
          -- гарантированный объем трафика считается по всем статьям завершения на СПС:
          -- ZTERM007(10015) Зоновое завершение вызова на СПС сеть "МТС" и "Мегафон"
          -- ZTERM003(10052) Зоновое завершение вызова на сети СПС «Санкт-Петербург Телеком» (Теле-2)
          -- ZTERM012(10076) Зоновое завершение на СПС ОАО "МТТ"
          -- ZTERM001(10053) Зоновое завершение вызова на сети СПС «Вымпелком»
          -- и составляет OB.RATE_VALUE  руб/мес
          --
          -- получаем сумму начислений
          SELECT SUM(ITEM_TOTAL) 
            INTO v_item_total
            FROM ITEM_T I
           WHERE I.BILL_ID       = rq.bill_id
             AND I.REP_PERIOD_ID = rq.rep_period_id
             AND I.SUBSERVICE_ID IN (10053,10052,10015,10076);
          --
          IF v_item_total < rq.rate_value THEN
              v_charge_value := rq.rate_value - v_item_total;
              -- проводим начисления
              INSERT INTO ITEM_T I(
                 BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
                 CHARGE_TYPE, ITEM_TOTAL, ITEM_CURRENCY_ID, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
                 ITEM_STATUS, ORDER_BODY_ID
              ) VALUES (
                 rq.bill_id, rq.rep_period_id, Pk02_Poid.Next_item_id, rq.order_id,
                 Pk00_Const.c_SERVICE_OP_LOCAL, Pk00_Const.c_SUBSRV_MIN,
                 Pk00_Const.c_CHARGE_TYPE_MIN, v_charge_value, rq.Currency_Id,
                 rq.Period_From, rq.Period_To, 'N', Pk00_Const.c_ITEM_TYPE_BILL,
                 Pk00_Const.c_ITEM_STATE_OPEN, rq.order_body_id
              );
              --
              Pk01_Syslog.Write_msg('Bill_id='||rq.bill_id||' RR=2431 - charged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );          
          END IF;

      ELSIF rq.Rate_Rule_Id IN (
             Pk00_Const.c_RATE_RULE_IP_FIX_VOLIN,-- 2406; -- Фиксированный с учетом объема исходящего трафика' 
             Pk00_Const.c_RR_IP_BURST_VOLIN_REC )-- 2419; -- Абонплата для BURST с учетом объема исходящего трафика
      THEN
        -- процедура расчета абонплаты  для rate_rule_id in ( 2406 , 2419 )
        -- используется для Макеева Сергея при его стандартном расчете абонки.
        PK24_CCAD.Sp_abp_count (
                p_rep_perod         => rq.rep_period_id, -- период расчета 
                p_order_id          => rq.order_id,      -- только для указанного заказа
                p_order_body_id     => rq.order_body_id, -- только для указанного компонента услуги
                --
                o_ITEM_TOTAL        => v_charge_value,   -- amount 
                o_note              => v_note            -- пояснение к расчету
        );
        IF v_charge_value IS NOT NULL THEN
            v_charge_value := v_charge_value
                         * Get_order_days(rq.Rep_Period_Id,rq.Order_Body_Id )
                         / Get_period_days(rq.Rep_Period_Id);
        ELSE
            v_charge_value := 0;
        END IF;
        -- проводим начисления
        INSERT INTO ITEM_T I(
           BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
           CHARGE_TYPE, ITEM_TOTAL, ITEM_CURRENCY_ID, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
           ITEM_STATUS, ORDER_BODY_ID, NOTES
        ) VALUES (
           rq.bill_id, rq.rep_period_id, Pk02_Poid.Next_item_id, rq.order_id,
           rq.service_id, rq.subservice_id,
           Pk00_Const.c_CHARGE_TYPE_REC, v_charge_value, rq.currency_id,
           rq.Period_From, rq.Period_To, rq.tax_incl, Pk00_Const.c_ITEM_TYPE_BILL,
           Pk00_Const.c_ITEM_STATE_OPEN, rq.order_body_id, v_note
        );
        Pk01_Syslog.Write_msg('Bill_id='||rq.bill_id||' RR='||rq.Rate_Rule_Id||' - charged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      END IF;
      v_count := v_count + 1;
    END LOOP;

    --
    Pk01_Syslog.Write_msg('Stop. '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Начисление абонентской платы и доплаты до минимальной суммы
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_fixrates( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Charge_fixrates';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Стандартные способы начисление абонентской платы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Charge_ABP( p_task_id => p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Начисление доплаты до минимальной суммы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Сharge_MIN( p_task_id => p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Нестандартные правила начисления АБП и Доплаты до минималки
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Charge_non_std_fixrates( p_task_id => p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Процедура компенсации времени блокировок в фиксированных начислениях
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Lock_abp_processing( p_task_id => p_task_id );
    Lock_min_processing( p_task_id => p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  Заполнить поля необходимые для получения детализации по абонплате
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Put_abp_detail( p_task_id => p_task_id );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--     В С П О М О Г А Т Е Л Ь Н Ы Е   Ф У Н К Ц И И
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Функция возвращающая кол-во дней в периоде
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Get_period_days( p_period_id IN INTEGER ) RETURN INTEGER DETERMINISTIC IS 
BEGIN
  RETURN TO_CHAR(LAST_DAY(TO_DATE(p_period_id,'yyyymm')), 'DD');
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Функция для вычисления кол-во дней, когда предоставлялась услуга,
-- в биллинговом периоде, без учета блокировок
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Get_order_days(
         p_period_id     IN INTEGER,
         p_order_body_id IN INTEGER
  ) RETURN NUMBER
IS 
    v_prcName  CONSTANT VARCHAR2(30) := 'Get_order_days';
    v_ord_days INTEGER;
BEGIN
    -- получаем кол-во дней в которые оказывалась услуга по заказу
    SELECT ROUND(DATE_TO-DATE_FROM) ORD_DAYS
      INTO v_ord_days
      FROM ( 
        SELECT 
            O.ORDER_ID,
            TRUNC( GREATEST(O.DATE_FROM, OB.DATE_FROM, P.PERIOD_FROM)) DATE_FROM, -- yyyy.mm.dd 00:00:00
            TRUNC( LEAST(NVL(O.DATE_TO,P.PERIOD_TO),
                   NVL(OB.DATE_TO,P.PERIOD_TO),
                   P.PERIOD_TO 
                  )) + 86399/86400 DATE_TO -- yyyy.mm.dd 23:59:59
          FROM ORDER_T O, ORDER_BODY_T OB, PERIOD_T P
         WHERE OB.ORDER_ID = O.ORDER_ID
           AND OB.ORDER_BODY_ID = p_order_body_id
           AND P.PERIOD_ID      = p_period_id
    );
    RETURN v_ord_days;
EXCEPTION 
    WHEN OTHERS THEN
      Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

/*
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Функция вычисления понижающего коэффициента к ежемесячному платежу, 
-- по кол-ву дней, когда услуга реально предоставлялась
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Period_ratio(
         p_period_id     IN INTEGER,
         p_order_body_id IN INTEGER
  ) RETURN NUMBER
IS 
    v_prcName  CONSTANT VARCHAR2(30) := 'Period_ratio';
    v_ratio    NUMBER;
    v_ord_days NUMBER; 
    v_mon_days NUMBER;
    v_lck_days NUMBER;
    v_order_id INTEGER;
BEGIN
    --
    -- получаем кол-во дней в которые оказывалась услуга по заказу
    SELECT ORDER_ID, ROUND(DATE_TO-DATE_FROM) ORD_DAYS, MON_DAYS
      INTO v_order_id, v_ord_days, v_mon_days
      FROM ( 
        SELECT 
            O.ORDER_ID,
            TRUNC( GREATEST(O.DATE_FROM, OB.DATE_FROM, P.PERIOD_FROM)) DATE_FROM, -- yyyy.mm.dd 00:00:00
            TRUNC( LEAST(NVL(O.DATE_TO,P.PERIOD_TO),
                   NVL(OB.DATE_TO,P.PERIOD_TO),
                   P.PERIOD_TO 
                  )) + 86399/86400 DATE_TO, -- yyyy.mm.dd 23:59:59
            ROUND(P.PERIOD_TO - P.PERIOD_FROM) MON_DAYS
          FROM ORDER_T O, ORDER_BODY_T OB, PERIOD_T P
         WHERE OB.ORDER_ID = O.ORDER_ID
           AND OB.ORDER_BODY_ID = p_order_body_id
           AND P.PERIOD_ID      = p_period_id
    );
    --
    -- получаем кол-во дней периода, когда заказ был заблокирован
    -- по требованию А.Ю.Гурова:
    --  DATE_FROM - заказ заблокирован с 00:00
    --  DATE_TO   - заказ разблокирован с 00:00
    SELECT NVL(ROUND(SUM(LOCK_DAYS)),0) LCK_DAYS
      INTO v_lck_days
      FROM (
        SELECT CASE 
                -- проверка на некорректный ввод и так бывает
                WHEN L.DATE_TO < L.DATE_FROM 
                THEN 0
                -- заблокирован весь месяц:  DATE_FROM---[-------]---DATE_TO
                WHEN L.DATE_FROM <= P.PERIOD_FROM 
                 AND (P.PERIOD_TO <= L.DATE_TO OR L.DATE_TO IS NULL) 
                THEN P.PERIOD_TO - P.PERIOD_FROM + 1/86400
                -- заблокирован внутри месяца: [--DATE_FROM-----DATE_TO--]
                WHEN P.PERIOD_FROM < L.DATE_FROM 
                 AND L.DATE_TO < P.PERIOD_TO 
                THEN TRUNC(L.DATE_TO) - TRUNC(L.DATE_FROM)
                -- заблокирован в предыдущем периоде, открыт в текущем: ---DATE_FROM-- [---DATE_TO---]
                WHEN L.DATE_FROM <= P.PERIOD_FROM 
                 AND L.DATE_TO < P.PERIOD_TO 
                THEN TRUNC(L.DATE_TO) - P.PERIOD_FROM
                -- заблокирован в текущем периоде, и остается до конца периода:---[--DATE_FROM--]---DATE_TO---  
                WHEN P.PERIOD_FROM < L.DATE_FROM  
                 AND (P.PERIOD_TO <= L.DATE_TO OR L.DATE_TO IS NULL)
                THEN P.PERIOD_TO - TRUNC(L.DATE_FROM)
                -- возможем некорректный ввод
                ELSE 0
               END LOCK_DAYS
          FROM ORDER_LOCK_T L, PERIOD_T P
        WHERE L.DATE_FROM    <= P.PERIOD_TO                     -- только блокировки 
          AND (L.DATE_TO IS NULL OR P.PERIOD_FROM <= L.DATE_TO) -- действовавшие в периоде
          AND L.ORDER_ID      = v_order_id
          AND P.PERIOD_ID     = p_period_id
    );
    -- вычисляем понижающий коэффициент
    IF v_ord_days < v_lck_days THEN 
        v_ratio := 0;
    ELSE
        v_ratio := (v_ord_days - v_lck_days)/v_mon_days;
    END IF;
    RETURN v_ratio;
EXCEPTION 
    WHEN OTHERS THEN
      Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Процедура компенсации времени блокировок в фиксированных начислениях
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Lock_abp_processing(p_task_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Lock_abp_processing';
    -- переменные для INSERT
    v_i_item_id     INTEGER := NULL;
    v_i_item_total  NUMBER  := 0;
    v_i_date_from   DATE;
    v_i_date_to     DATE;
    v_i_count       INTEGER := 0;
    -- переменные для UPDATE    
    v_u_item_id     INTEGER := NULL;
    v_u_item_total  NUMBER  := 0;
    v_u_date_from   DATE;
    v_u_date_to     DATE;
    v_u_count       INTEGER := 0;
    
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Цикл для обработки блокировок заказов
    --
    FOR rl IN (
        SELECT 
               ROUND(L_DATE_TO - L_DATE_FROM) L_DAYS,
               SUM(ROUND(L_DATE_TO - L_DATE_FROM)) OVER (PARTITION BY ORDER_ID) L_ALLDAYS,
               ITEM_TOTAL/ROUND(DATE_TO - DATE_FROM) DAY_TOTAL,
               LAG (L_DATE_TO)   OVER(PARTITION BY ORDER_ID ORDER BY L_DATE_FROM) PREV_DATE_TO, 
               LEAD(L_DATE_FROM) OVER(PARTITION BY ORDER_ID ORDER BY L_DATE_FROM) NEXT_DATE_FROM,
               IL.* 
          FROM (        
            SELECT ROW_NUMBER() OVER (PARTITION BY L.ORDER_ID ORDER BY L.DATE_FROM) RN,
                   COUNT(*) OVER (PARTITION BY L.ORDER_ID) CNT,
                   -- выравниваем блокировку по началу суток
                   GREATEST(TRUNC(L.DATE_FROM), I.DATE_FROM) L_DATE_FROM, 
                   -- приводим блокировку к концу суток из-за I.DATE_TO = 23:59:59, секунда учтется при ROUND
                   LEAST(NVL(TRUNC(L.DATE_TO)-1/86400, I.DATE_TO), I.DATE_TO) L_DATE_TO, 
                   I.*
              FROM ORDER_LOCK_T L, ITEM_T I
             WHERE L.ORDER_ID = I.ORDER_ID
               AND L.DATE_FROM < I.DATE_TO
               AND (L.DATE_TO IS NULL OR L.DATE_TO > I.DATE_FROM )
               AND I.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_REC
               AND EXISTS (
                  SELECT * FROM BILLING_QUEUE_T Q, BILL_T B
                   WHERE Q.TASK_ID       = p_task_id
                     AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID
                     AND Q.BILL_ID       = I.BILL_ID
                     AND Q.REP_PERIOD_ID = B.REP_PERIOD_ID
                     AND Q.BILL_ID       = B.BILL_ID
                     AND B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_OPEN
               )
          ) IL
          WHERE L_DATE_FROM + 86399/86400 <= L_DATE_TO  -- блокировки меньше 1 суток исключаем, но 00 -23:59:59 - оставляем
    )LOOP
        -- переменные для INSERT
        v_i_item_id    := NULL;
        v_i_item_total := 0;
        v_i_date_from  := NULL;
        v_i_date_to    := NULL;
        -- переменные для UPDATE    
        v_u_item_id    := NULL;
        v_u_item_total := 0;
        v_u_date_from  := NULL;
        v_u_date_to    := NULL;
        --
        IF rl.prev_date_to IS NULL AND rl.next_date_from IS NULL THEN
           IF rl.l_date_to = rl.date_to THEN
               -- 1)  ----------------|+++++++++++++(====U===|=======)---------
               -- UPDATE
               v_u_item_id   := rl.item_id;
               v_u_date_from := rl.date_from;  
               v_u_date_to   := rl.l_date_from - 1/86400;
               v_u_item_total:= ROUND(v_u_date_to - v_u_date_from) * rl.day_total;
               --
           ELSIF  rl.l_date_from = rl.date_from THEN
               -- 2)  -------(========|===U===)++++++++++++++|-----------------
               -- UPDATE
               v_u_item_id   := rl.item_id;
               v_u_date_from := rl.l_date_to + 1/86400;  
               v_u_date_to   := rl.date_to;
               v_u_item_total:= ROUND(v_u_date_to - v_u_date_from) * rl.day_total;
               --
           ELSE
               -- 3)  ----------------|+++++(===I/U==)+++++++|-----------------
               -- INSERT
               v_i_item_id   := Pk02_Poid.Next_item_id;
               v_i_date_from := rl.date_from ;  
               v_i_date_to   := rl.l_date_from;
               v_i_item_total:= ROUND(v_i_date_to - v_i_date_from) * rl.day_total;
               --
               -- UPDATE
               v_u_item_id   := rl.item_id;
               v_u_date_from := rl.l_date_to + 1/86400;  
               v_u_date_to   := rl.date_to;
               v_u_item_total:= ROUND(v_u_date_to - v_u_date_from) * rl.day_total;
               --
           END IF;
        ELSIF rl.prev_date_to IS NULL AND rl.next_date_from IS NOT NULL THEN
           IF rl.l_date_from = rl.date_from THEN
               -- 4)  -------(========|===U===)+++++(========|=======)---------
               -- UPDATE
               v_u_item_id   := rl.item_id;
               v_u_date_from := rl.l_date_to + 1/86400;  
               v_u_date_to   := rl.next_date_from;
               v_u_item_total:= ROUND(v_u_date_to - v_u_date_from) * rl.day_total;
               --
           ELSE 
               -- 5)  ----------------|++(==I/U==)++++(======|=======)---------
               -- INSERT
               v_i_item_id   := Pk02_Poid.Next_item_id;
               v_i_date_from := rl.date_from;  
               v_i_date_to   := rl.l_date_from;
               v_i_item_total:= ROUND(v_i_date_to - v_i_date_from) * rl.day_total;
               --
               -- UPDATE
               v_u_item_id   := rl.item_id;
               v_u_date_from := rl.l_date_to + 1/86400;  
               v_u_date_to   := rl.next_date_from;
               v_u_item_total:= ROUND(v_u_date_to - v_u_date_from) * rl.day_total;
               --
           END IF;
        ELSIF rl.prev_date_to IS NOT NULL AND rl.next_date_from IS NULL THEN 
           IF rl.l_date_to = rl.date_to THEN
               -- 6)  -------(========|=======)-----(====N===|=======)---------
               NULL;
           ELSE
               -- 7)  -------(========|=======)-----(==U==)++|-----------------
               -- UPDATE
               v_u_item_id   := rl.item_id;
               v_u_date_from := rl.l_date_to + 1/86400;  
               v_u_date_to   := rl.date_to;
               v_u_item_total:= ROUND(v_u_date_to - v_u_date_from) * rl.day_total;
               --
           END IF;
        ELSIF rl.prev_date_to IS NOT NULL AND rl.next_date_from IS NOT NULL THEN 
               -- 8)  -------(========|==)----(==U==)+++++(==|=======)---------
               -- UPDATE
               v_u_item_id   := rl.item_id;
               v_u_date_from := rl.prev_date_to + 1/86400;  
               v_u_date_to   := rl.next_date_from;
               v_u_item_total:= ROUND(v_u_date_to - v_u_date_from) * rl.day_total;
               --
        END IF;
        
        IF v_i_item_id IS NOT NULL THEN
            -- создание новой записи
            INSERT INTO ITEM_T (
                BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
                ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
                ITEM_TOTAL, RECVD, 
                DATE_FROM, DATE_TO, 
                ITEM_STATUS, 
                CREATE_DATE, LAST_MODIFIED, 
                REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, 
                NOTES, ORDER_BODY_ID, DESCR, QUANTITY, ITEM_CURRENCY_ID, BILL_TOTAL
            )VALUES(
                rl.BILL_ID, rl.REP_PERIOD_ID, v_i_item_id, rl.ITEM_TYPE, rl.INV_ITEM_ID,
                rl.ORDER_ID, rl.SERVICE_ID, rl.SUBSERVICE_ID, rl.CHARGE_TYPE, 
                v_i_item_total, rl.RECVD, 
                v_i_date_from, v_i_date_to, 
                rl.ITEM_STATUS, 
                SYSDATE, SYSDATE, 
                rl.REP_GROSS, rl.REP_TAX, rl.TAX_INCL, rl.EXTERNAL_ID, 
                rl.NOTES, rl.ORDER_BODY_ID, rl.DESCR, rl.QUANTITY, rl.ITEM_CURRENCY_ID, 
                rl.BILL_TOTAL
            );
            v_i_count := v_i_count + SQL%ROWCOUNT;
        END IF;
        
        IF v_u_item_id IS NOT NULL THEN
            -- изменение существующей записи
            UPDATE ITEM_T I 
               SET I.ITEM_TOTAL = v_u_item_total,
                   I.DATE_FROM  = v_u_date_from,
                   I.DATE_TO    = v_u_date_to
             WHERE I.ITEM_ID    = v_u_item_id
               AND I.REP_PERIOD_ID = rl.Rep_Period_Id;
            v_u_count := v_u_count + SQL%ROWCOUNT;
        END IF;

    END LOOP;
    
    Pk01_Syslog.Write_msg('ITEM_T: '||v_u_count||' rows updated, '
                                    ||v_i_count||' rows inserted'
                          , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Процедура компенсации времени блокировок в доплате до минимальной стоимости
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Lock_min_processing(p_task_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Lock_min_processing';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- корректируем значение доплаты до MIN с учетом корректировок
    MERGE INTO ITEM_T I
    USING (
          WITH IL AS (
              SELECT REP_PERIOD_ID, BILL_ID, ITEM_ID, ORDER_ID,  
                     RATE_VALUE * (1  - LOCK_DAYS / PK04_PERIOD.PERIOD_DAYS(REP_PERIOD_ID)) MIN_VALUE
                FROM (
                  SELECT BILL_ID, ITEM_ID, REP_PERIOD_ID, ORDER_ID, RATE_VALUE,
                         ROUND(SUM(L_DATE_TO - L_DATE_FROM)) LOCK_DAYS 
                    FROM (
                        SELECT I.BILL_ID, I.ITEM_ID, I.REP_PERIOD_ID, I.ORDER_ID, 
                               OB.RATE_VALUE, 
                               -- выравниваем блокировку по началу суток
                               GREATEST(TRUNC(L.DATE_FROM), I.DATE_FROM) L_DATE_FROM, 
                               -- приводим блокировку к концу суток из-за I.DATE_TO = 23:59:59, секунда учтется при ROUND
                               LEAST(NVL(TRUNC(L.DATE_TO)-1/86400, I.DATE_TO), I.DATE_TO) L_DATE_TO
                          FROM ORDER_LOCK_T L, ITEM_T I, ORDER_BODY_T OB
                         WHERE L.ORDER_ID = I.ORDER_ID
                           AND L.DATE_FROM < I.DATE_TO
                           AND (L.DATE_TO IS NULL OR L.DATE_TO > I.DATE_FROM )
                           AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_MIN
                           AND I.ORDER_BODY_ID = OB.ORDER_BODY_ID
                           AND EXISTS (
                              SELECT * FROM BILLING_QUEUE_T Q, BILL_T B
                               WHERE Q.TASK_ID       = p_task_id
                                 AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID
                                 AND Q.BILL_ID       = I.BILL_ID
                                 AND Q.REP_PERIOD_ID = B.REP_PERIOD_ID
                                 AND Q.BILL_ID       = B.BILL_ID
                                 AND B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_OPEN
                           )
                    )
                    WHERE L_DATE_FROM != L_DATE_TO
                    GROUP BY REP_PERIOD_ID, BILL_ID, ITEM_ID, ORDER_ID, RATE_VALUE
                )
           )
           SELECT REP_PERIOD_ID, ITEM_ID, 
                  CASE 
                  WHEN MIN_VALUE > NVL(USG_TOTAL,0) THEN (MIN_VALUE - NVL(USG_TOTAL,0)) 
                  ELSE 0 
                  END ITEM_TOTAL
             FROM (
               SELECT IL.REP_PERIOD_ID, IL.BILL_ID, IL.ITEM_ID, IL.MIN_VALUE, SUM(I.ITEM_TOTAL) USG_TOTAL
                 FROM IL, ITEM_T I
                WHERE I.REP_PERIOD_ID(+) = IL.REP_PERIOD_ID
                  AND I.BILL_ID(+)       = IL.BILL_ID
                  AND I.ORDER_ID(+)      = IL.ORDER_ID
                  AND I.CHARGE_TYPE(+)   = Pk00_Const.c_CHARGE_TYPE_USG
                GROUP BY IL.REP_PERIOD_ID, IL.BILL_ID, IL.ITEM_ID, IL.MIN_VALUE
              )
    ) LL
    ON (
        I.ITEM_ID = LL.ITEM_ID AND 
        I.REP_PERIOD_ID = LL.REP_PERIOD_ID
    )         
    WHEN MATCHED THEN UPDATE SET I.ITEM_TOTAL = LL.ITEM_TOTAL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('MIN changed for '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


END PK36_BILLING_FIXRATE;
/
