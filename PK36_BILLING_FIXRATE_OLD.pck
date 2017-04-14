CREATE OR REPLACE PACKAGE PK36_BILLING_FIXRATE_OLD
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

    --  Расчет абонплаты (subscriber fee)
    PROCEDURE Charge_ABP( p_task_id IN INTEGER );

    --  Расчет абонплаты (subscriber fee) по тарифам укзанным 
    -- индивидуально для каждого месяца (MONTH_TARIFF_T),
    -- такой тариф иногда требуют бюджетные организации, 
    -- заключающие договор на один год, затем тендер и новый договор
    -- для биллингового периода p_period_id
    PROCEDURE Charge_ABP_by_month_tariff( p_task_id IN INTEGER );

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
    FUNCTION Period_ratio(
             p_period_id     IN INTEGER,
             p_order_body_id IN INTEGER
      ) RETURN NUMBER;
    
END PK36_BILLING_FIXRATE_OLD;
/
CREATE OR REPLACE PACKAGE BODY PK36_BILLING_FIXRATE_OLD
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
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем периодические счета в биллинговом периоде для л/с где их нет
    v_count := 0;
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
    )LOOP
      v_bill_id := Pk07_BIll.Next_recuring_bill (
               p_account_id    => rb.account_id, -- ID лицевого счета
               p_rep_period_id => p_period_id    -- ID расчетного периода YYYYMM
           );
      v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('Bill_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
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
               ( Period_ratio(P.PERIOD_ID , OB.ORDER_BODY_ID )
                     * OB.RATE_VALUE 
                     * OB.QUANTITY
               ) ITEM_TOTAL,
               OB.CURRENCY_ID,
               TRUNC( GREATEST(O.DATE_FROM, OB.DATE_FROM, P.PERIOD_FROM)) DATE_FROM,
               TRUNC( LEAST(NVL(O.DATE_TO,P.PERIOD_TO),
                      NVL(OB.DATE_TO,P.PERIOD_TO),
                      P.PERIOD_TO 
                     )) + 86399/86400 DATE_TO,
               OB.TAX_INCL, 
               PK00_CONST.c_ITEM_TYPE_BILL ITEM_TYPE,
               Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS, 
               OB.ORDER_BODY_ID
          FROM ORDER_BODY_T OB, ORDER_T O, 
               PERIOD_T P, BILL_T B, 
               BILLING_QUEUE_T Q
         WHERE OB.CHARGE_TYPE       = Pk00_Const.c_CHARGE_TYPE_REC
           AND OB.RATE_RULE_ID IN  (  Pk00_Const.c_RATE_RULE_ABP_STD,       -- 2402
                                      Pk00_Const.c_RATE_RULE_ABP_FREE_MIN ) -- 2403
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
    )
    SELECT * FROM IT
     WHERE IT.ITEM_TOTAL > 0
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  Расчет абонплаты, из расчета что 1 день стоит 1/30 абонки
-- ITEM_TOTAL = Т/30 дней* N, где
-- Т - тариф
-- N - кол-во отработанных дней в календарном месяце
-- Т.е. при определении стоимости одного дня участвует 
-- не кол-во дней в календарном месяце, а всегда 30 дней
-- Клиент пока Инфранете один - Центральный банк, считаем вручную
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_ABP_by_30days( p_task_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Charge_ABP_by_30days';
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
               (   OB.RATE_VALUE * OB.QUANTITY 
                 * Get_order_days(P.PERIOD_ID , OB.ORDER_BODY_ID )/30
               ) ITEM_TOTAL,
               OB.CURRENCY_ID,
               TRUNC( GREATEST(O.DATE_FROM, OB.DATE_FROM, P.PERIOD_FROM)) DATE_FROM,
               TRUNC( LEAST(NVL(O.DATE_TO,P.PERIOD_TO),
                      NVL(OB.DATE_TO,P.PERIOD_TO),
                      P.PERIOD_TO 
                     )) + 86399/86400 DATE_TO,
               OB.TAX_INCL, 
               PK00_CONST.c_ITEM_TYPE_BILL ITEM_TYPE,
               Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS, 
               OB.ORDER_BODY_ID
          FROM ORDER_BODY_T OB, ORDER_T O, 
               PERIOD_T P, BILL_T B, 
               BILLING_QUEUE_T Q
         WHERE OB.CHARGE_TYPE       = Pk00_Const.c_CHARGE_TYPE_REC
           AND OB.RATE_RULE_ID      = Pk00_Const.c_RATE_RULE_ABP_30DAYS -- 2417
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
    )
    SELECT * FROM IT
     WHERE IT.ITEM_TOTAL > 0
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  Расчет абонплаты (subscriber fee) по тарифам укзанным 
-- индивидуально для каждого месяца (MONTH_TARIFF_T),
-- такой тариф иногда требуют бюджетные организации, 
-- заключающие договор на один год, затем тендер и новый договор
-- для биллингового периода p_period_id
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_ABP_by_month_tariff(p_task_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Charge_ABP_by_month_tariff';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

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
               ( Period_ratio(P.PERIOD_ID , OB.ORDER_BODY_ID )
                     * T.PRICE 
                     * OB.QUANTITY
               ) ITEM_TOTAL,
               OB.CURRENCY_ID,
               TRUNC( GREATEST(O.DATE_FROM, OB.DATE_FROM, P.PERIOD_FROM)) DATE_FROM,
               TRUNC( LEAST(NVL(O.DATE_TO,P.PERIOD_TO),
                      NVL(OB.DATE_TO,P.PERIOD_TO),
                      P.PERIOD_TO 
                     )) + 86399/86400 DATE_TO,
               R.TAX_INCL, 
               PK00_CONST.c_ITEM_TYPE_BILL ITEM_TYPE,
               Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS, 
               OB.ORDER_BODY_ID
          FROM ORDER_BODY_T OB, ORDER_T O, 
               PERIOD_T P, BILL_T B, 
               RATEPLAN_T R, MONTH_TARIFF_T T,
               BILLING_QUEUE_T Q
         WHERE OB.CHARGE_TYPE       = Pk00_Const.c_CHARGE_TYPE_REC
           AND OB.RATE_RULE_ID      = Pk00_Const.c_RATE_RULE_ABP_MON
           AND OB.RATEPLAN_ID       = R.RATEPLAN_ID
           AND R.RATESYSTEM_ID      = Pk00_Const.с_RATESYS_MON_TRF_ID
           AND R.RATEPLAN_ID        = T.RATEPLAN_ID
           AND T.PERIOD_ID          = NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID)
           AND O.ORDER_ID           = OB.ORDER_ID
           AND P.PERIOD_ID          = NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID) 
           AND Q.ACCOUNT_ID         = O.ACCOUNT_ID
           AND P.PERIOD_TO         >= GREATEST(O.DATE_FROM, OB.DATE_FROM) 
           AND P.PERIOD_FROM       <= LEAST(NVL(O.DATE_TO,P.PERIOD_TO), NVL(OB.DATE_TO,P.PERIOD_TO))
           AND Q.TASK_ID            = p_task_id
           AND Q.REP_PERIOD_ID      = B.REP_PERIOD_ID
           AND Q.BILL_ID            = B.BILL_ID
           AND OB.QUANTITY     IS NOT NULL
           AND B.BILL_TYPE     IN (Pk00_Const.c_BILL_TYPE_REC, 
                                   Pk00_Const.c_BILL_TYPE_DBT, 
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
    )
    SELECT * FROM IT
     WHERE IT.ITEM_TOTAL > 0
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
    -- 1) Услуга NPL
    -- 2) Услуга IP_ACCESS 
    -- 3) Услуга EPL
    -- 4) Услуга DPL
    -- 5) Услуга LM
    MERGE INTO ITEM_T I
    USING (
        SELECT I.ITEM_ID,
               CASE 
                    WHEN I.SERVICE_ID = Pk00_Const.c_SERVICE_IP_ACCESS -- 104 
                      THEN 'IP port, '||INF.POINT_SRC||', '||INF.SPEED_STR
                    WHEN I.SERVICE_ID IN ( Pk00_Const.c_SERVICE_VPN, Pk00_Const.c_SERVICE_LM) -- 106, 108 
                      THEN INF.POINT_SRC||', '||INF.SPEED_STR
                    ELSE INF.POINT_SRC||DECODE(INF.POINT_DST, NULL, NULL,' - '||INF.POINT_DST)||', '||INF.SPEED_STR
               END STR
          FROM ORDER_INFO_T INF, ITEM_T I, BILLING_QUEUE_T Q
        WHERE 1=1
--          AND I.CHARGE_TYPE   IN ( Pk00_Const.c_CHARGE_TYPE_REC, Pk00_Const.c_CHARGE_TYPE_USG )  -- KiriLL изменил от 02.12.2015. Если ограничить - не проставляются примечания, которые были сделаны руками для других типов.
          AND INF.ORDER_ID    = I.ORDER_ID
          AND Q.BILL_ID       = I.BILL_ID
          AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID
          AND Q.TASK_ID       = p_task_id
          AND INF.POINT_SRC   IS NOT NULL
    ) INF
    ON (
        I.ITEM_ID = INF.ITEM_ID 
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
        SELECT B.BILL_ID, B.BILL_DATE, B.REP_PERIOD_ID, P.PERIOD_ID DATA_PERIOD_ID,
               O.ORDER_ID, O.SERVICE_ID, OB.SUBSERVICE_ID, OB.CHARGE_TYPE,
               Period_ratio(P.PERIOD_ID, OB.ORDER_BODY_ID)*OB.RATE_VALUE  MIN_VALUE,
               TRUNC( GREATEST(O.DATE_FROM, OB.DATE_FROM, P.PERIOD_FROM)) DATE_FROM,
               TRUNC( LEAST(NVL(O.DATE_TO,P.PERIOD_TO),
                      NVL(OB.DATE_TO,P.PERIOD_TO),
                      P.PERIOD_TO 
                     )) + 86399/86400 DATE_TO,
               OB.TAX_INCL, 
               PK00_CONST.c_ITEM_TYPE_BILL ITEM_TYPE,
               Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS, 
               OB.ORDER_BODY_ID,
               OB.CURRENCY_ID--,  -- валюта компоненты услуги 'MIN'
               --B.CURRENCY_ID BILL_CURRENCY_ID -- пока всегда Рубль - это на будущее
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
    v_task_id      INTEGER;
    v_item_total   NUMBER;
    v_bill_minutes NUMBER;
    v_charge_value NUMBER;
    v_min_value    NUMBER;

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
             Pk00_Const.c_RATE_RULE_RP001186  -- := 2431; -- СПб: л/с RP001186 ЗАО "VMB-Сервис"
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
          v_min_value := rq.rate_value * Period_ratio(rq.Rep_Period_Id,rq.Order_Body_Id );
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
          v_min_value := rq.rate_value * Period_ratio(rq.Rep_Period_Id,rq.Order_Body_Id );
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
          -- и составляет 30 000 руб/мес
          --
          -- получаем сумму начислений
          SELECT SUM(ITEM_TOTAL) 
            INTO v_item_total
            FROM ITEM_T I
           WHERE I.BILL_ID       = rq.bill_id
             AND I.REP_PERIOD_ID = rq.rep_period_id
             AND I.SUBSERVICE_ID IN (10053,10052,10015,10076);
          --
          IF v_item_total < 30000 THEN
              v_charge_value := 30000 - v_item_total;
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
       END IF;
       v_count := v_count + 1;
    END LOOP;

    PK30_BILLING_QUEUE.Close_task(p_task_id => v_task_id);
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
    -- Начисление абонентской платы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Charge_ABP( p_task_id => p_task_id );

    -- Расчет абонплаты, из расчета что 1 день стоит 1/30 абонки
    -- не кол-во дней в календарном месяце, а всегда 30 дней
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Charge_ABP_by_30days( p_task_id => p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  Расчет абонплаты (subscriber fee) по тарифам укзанным 
    -- индивидуально для каждого месяца (MONTH_TARIFF_T),
    -- такой тариф иногда требуют бюджетные организации, 
    -- заключающие договор на один год, затем тендер и новый договор
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Charge_ABP_by_month_tariff( p_task_id => p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Начисление доплаты до минимальной суммы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Сharge_MIN( p_task_id => p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Нестандартные правила начисления АБП и Доплаты до минималки
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Charge_non_std_fixrates( p_task_id => p_task_id );

    --  Заполнить поля необходимые для получения детализации по абонплате
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
-- Функция для вычисления кол-во дней, когда предоставлялась услуга,
-- в биллинговом периоде, без учета блокировок
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

END PK36_BILLING_FIXRATE_OLD;
/
