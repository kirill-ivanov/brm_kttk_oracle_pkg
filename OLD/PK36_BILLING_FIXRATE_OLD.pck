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
    PROCEDURE Charge_ABP(
          p_task_id       IN INTEGER,              -- ID задачи
          p_rep_period_id IN INTEGER,              -- период выставляемго счета
          p_period_id     IN INTEGER DEFAULT NULL  -- период данных
      );

    --  Расчет абонплаты (subscriber fee) по тарифам укзанным 
    -- индивидуально для каждого месяца (MONTH_TARIFF_T),
    -- такой тариф иногда требуют бюджетные организации, 
    -- заключающие договор на один год, затем тендер и новый договор
    -- для биллингового периода p_period_id
    PROCEDURE Charge_ABP_by_month_tariff(
          p_task_id       IN INTEGER,              -- ID задачи
          p_rep_period_id IN INTEGER,              -- период выставляемго счета
          p_period_id     IN INTEGER DEFAULT NULL  -- период данных
      );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  Заполнить поля необходимые для получения детализации по абонплате
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Put_ABP_detail(
              p_task_id       IN INTEGER,              -- ID задачи
              p_rep_period_id IN INTEGER               -- период выставляемго счета
           );

    --  Расчет доплаты до мимнимальной суммы КОМПОМЕНТА УСЛУГИ ЗАКАЗА
    PROCEDURE Subservice_charge_MIN(
          p_task_id       IN INTEGER,              -- ID задачи
          p_rep_period_id IN INTEGER,              -- период выставляемго счета
          p_period_id     IN INTEGER DEFAULT NULL  -- период данных  
      );

    --  Расчет доплаты до мимнимальной суммы ЗАКАЗА
    PROCEDURE Order_charge_MIN(
          p_task_id       IN INTEGER,              -- ID задачи
          p_rep_period_id IN INTEGER,              -- период выставляемго счета
          p_period_id     IN INTEGER DEFAULT NULL  -- период данных
      );
    
    --  Расчет доплаты до мимнимальной суммы Лицевого счета
    PROCEDURE Account_charge_MIN(
          p_task_id       IN INTEGER,              -- ID задачи
          p_rep_period_id IN INTEGER,              -- период выставляемго счета
          p_period_id     IN INTEGER DEFAULT NULL  -- период данных
      );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Расформирование фиксированных начислений,
    -- за исключением тех что сформировал тарификатор трафика
    PROCEDURE Rollback_fixrates(p_task_id IN INTEGER, p_period_id IN INTEGER);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Начисление абонентской платы и доплаты до минимальной суммы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Charge_fixrates(
          p_task_id        IN INTEGER,              -- ID задачи
          p_rep_period_id  IN INTEGER,              -- период выставляемго счета
          p_data_period_id IN INTEGER DEFAULT NULL  -- период данных  
       );
    
       
    
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
    v_period_from   DATE;
    v_period_to     DATE;
    v_bill_id       INTEGER;
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем периодические счета в биллинговом периоде для л/с где их нет
    v_count := 0;
    --
    FOR rb IN (
      SELECT DISTINCT A.ACCOUNT_ID 
        FROM ORDER_BODY_T OB, ORDER_T O, ACCOUNT_T A
       WHERE OB.CHARGE_TYPE IN (PK00_CONST.c_CHARGE_TYPE_MIN, PK00_CONST.c_CHARGE_TYPE_REC)
         AND OB.ORDER_ID = O.ORDER_ID 
         AND A.ACCOUNT_ID = O.ACCOUNT_ID
         AND A.STATUS = Pk00_Const.c_ACC_STATUS_BILL
         AND O.DATE_FROM <= v_period_to
         AND ( O.DATE_TO IS NULL OR v_period_from <= O.DATE_TO) 
         AND OB.DATE_FROM <= v_period_to
         AND ( OB.DATE_TO IS NULL OR v_period_from <=  OB.DATE_TO)
         AND NOT EXISTS (
            SELECT * FROM BILL_T B
             WHERE B.REP_PERIOD_ID = p_period_id
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
-- Компенсация местного трафика вошедшего в абонплату
-- эту функцию убираем, концепция переменилась, 
-- задача выполняется в предбиллинге
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
/*
PROCEDURE Fix_ABP_traffic(
          p_task_id       IN INTEGER,              -- ID задачи
          p_rep_period_id IN INTEGER,              -- период выставляемго счета
          p_period_id     IN INTEGER DEFAULT NULL  -- период данных
       )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Fix_ABP_traffic';
    v_count         INTEGER;
    v_period_id     INTEGER;
    v_period_from   DATE;
    v_period_to     DATE;
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_rep_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    --  если период данных не задан считаем его равным периоду счета  
    IF p_period_id IS NULL THEN
       v_period_id := p_rep_period_id;
    ELSE
       v_period_id := p_period_id;
    END IF;
    --
    v_period_from := Pk04_Period.Period_from(v_period_id);
    v_period_to   := Pk04_Period.Period_to(v_period_id);
    --
    --    
    -- компенсируем трафик вощедший в абонплату
    INSERT INTO ITEM_T
    SELECT BILL_ID, REP_PERIOD_ID, SQ_ITEM_ID.NEXTVAL ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
           ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, ITEM_TOTAL, RECVD, DATE_FROM, DATE_TO, 
           ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES
    FROM (
      SELECT I.BILL_ID, I.REP_PERIOD_ID, NULL ITEM_ID, I.ITEM_TYPE, NULL INV_ITEM_ID, 
             I.ORDER_ID, I.SERVICE_ID, 
             37 SUBSERVICE_ID,    -- возврат начислений за трафик, включенный в абонплату c_SUBSRV_BACK
             I.CHARGE_TYPE, 
             CASE 
                 WHEN (PR.PRICE_0 * F.VALUE) > I.ITEM_TOTAL THEN -I.ITEM_TOTAL
                 ELSE -(PR.PRICE_0 * F.VALUE)  
             END ITEM_TOTAL, 
             0 RECVD, I.DATE_FROM, I.DATE_TO, 
             I.ITEM_STATUS, SYSDATE CREATE_DATE, SYSDATE LAST_MODIFIED, 
             0 REP_GROSS, 0 REP_TAX, I.TAX_INCL, NULL EXTERNAL_ID, NULL NOTES
       FROM FIX_RATE_T F, ORDER_T O,
            TARIFF_PH.D41_TRF_HEADER TH,
            TARIFF_PH.D42_TRF_PRICE  PR,
            ITEM_T I, BILLING_QUEUE_T Q
      WHERE F.ORDER_ID = O.ORDER_ID
        AND F.FREE_TRAFFIC IS NOT NULL
        AND O.SERVICE_ID = Pk00_Const.c_SERVICE_CALL_LOCAL -- 125  -- Местная связь 
        AND TH.RATEPLAN_ID = O.RATEPLAN_ID
        AND PR.TRF_ID = TH.TRF_ID
        AND I.ORDER_ID = O.ORDER_ID
        AND I.SERVICE_ID = O.SERVICE_ID
        AND I.REP_PERIOD_ID = p_rep_period_id
        AND I.DATE_FROM    <= v_period_to        -- рассматриваем только item-s
        AND I.DATE_TO      >= v_period_from      -- указанного периода
        AND I.BILL_ID = Q.BILL_ID
        AND Q.TASK_ID = p_task_id
      )
    WHERE ITEM_TOTAL != 0
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  Расчет абонплаты (subscriber fee)
-- для биллингового периода p_period_id
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_ABP(
          p_task_id       IN INTEGER,              -- ID задачи
          p_rep_period_id IN INTEGER,              -- период выставляемго счета
          p_period_id     IN INTEGER DEFAULT NULL  -- период данных
       )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Charge_ABP';
    v_period_id     INTEGER;
    v_period_from   DATE;
    v_period_to     DATE;
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    --  если период данных не задан считаем его равным периоду счета  
    IF p_period_id IS NULL THEN
       v_period_id := p_rep_period_id;
    ELSE
       v_period_id := p_period_id;
    END IF;
    --
    v_period_from := Pk04_Period.Period_from(v_period_id);
    v_period_to   := Pk04_Period.Period_to(v_period_id);
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем позиции начислений на абонплату для счетов биллингового периода
    --
    INSERT INTO ITEM_T I(
       BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
       CHARGE_TYPE, ITEM_TOTAL, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
       ITEM_STATUS, ORDER_BODY_ID
    )
    WITH ORD AS (   -- заказы, имющие компоменту услуги - АБОНПЛАТА
        SELECT  ACCOUNT_ID, ORDER_ID, ORDER_BODY_ID, QUANTITY, VALUE, TAX_INCL, 
                CHARGE_TYPE, SERVICE_ID, SUBSERVICE_ID,
                DATE_FROM, DATE_TO,
                ROUND(DATE_TO-DATE_FROM) ORD_DAYS,
                ROUND(v_period_to - v_period_from) MON_DAYS
          FROM (      
            SELECT O.ACCOUNT_ID, OB.ORDER_ID, OB.ORDER_BODY_ID, 
                   OB.QUANTITY, 
                   CASE
                     WHEN OB.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN (OB.RATE_VALUE * 28.6)
                     ELSE OB.RATE_VALUE
                   END VALUE,
                   OB.TAX_INCL, 
                   OB.CHARGE_TYPE, O.SERVICE_ID, OB.SUBSERVICE_ID,
                   CASE
                    WHEN GREATEST(O.DATE_FROM, OB.DATE_FROM) <= v_period_from  
                    THEN v_period_from ELSE GREATEST(O.DATE_FROM, OB.DATE_FROM) 
                   END DATE_FROM,
                   CASE
                    WHEN LEAST(NVL(O.DATE_TO,v_period_to), NVL(OB.DATE_TO,v_period_to)) >= v_period_to  
                    THEN v_period_to  ELSE LEAST(NVL(O.DATE_TO,v_period_to), NVL(OB.DATE_TO,v_period_to))
                   END DATE_TO
              FROM ORDER_BODY_T OB, ORDER_T O
             WHERE OB.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_REC
               AND OB.RATEPLAN_ID IS NULL   -- тариф входит в описание строки заказа
               AND O.ORDER_ID = OB.ORDER_ID
               AND OB.DATE_FROM   <= v_period_to
               AND (v_period_from <= OB.DATE_TO OR OB.DATE_TO IS NULL)
               AND O.DATE_FROM    <= v_period_to
               AND (v_period_from <= O.DATE_TO OR O.DATE_TO IS NULL)
            )
    ), LCK AS (    -- блокировки, биллингового периода
        SELECT ORDER_ID, ROUND(SUM(LOCK_DAYS)) LCK_DAYS
          FROM (
            SELECT ORDER_ID,
                   CASE 
                    -- заблокирован весь месяц:  DATE_FROM---[-------]---DATE_TO
                    WHEN DATE_FROM <= v_period_from 
                     AND (v_period_to <= DATE_TO OR DATE_TO IS NULL) 
                     THEN v_period_to - v_period_from
                    -- заблокирован внутри месяца: [--DATE_FROM-----DATE_TO--]
                    WHEN v_period_from < DATE_FROM 
                     AND DATE_TO < v_period_to 
                     THEN DATE_TO - DATE_FROM
                    -- заблокирован в предыдущем периоде, открыт в текущем: ---DATE_FROM-- [---DATE_TO---]
                    WHEN DATE_FROM <= v_period_from 
                     AND DATE_TO < v_period_to 
                     THEN DATE_TO - v_period_from
                    -- заблокирован в текущем периоде, и остается до конца периода:---[--DATE_FROM--]---DATE_TO---  
                    WHEN v_period_from < DATE_FROM  
                     AND (v_period_to <= DATE_TO OR DATE_TO IS NULL)
                     THEN v_period_to - DATE_FROM
                    -- возможем некорректный ввод
                    ELSE 0
                   END LOCK_DAYS
              FROM ORDER_LOCK_T L
            WHERE DATE_FROM <= v_period_to   -- только блокировки 
              AND (DATE_TO IS NULL OR v_period_from <= DATE_TO)    -- действовавшие в периоде
        )
        GROUP BY ORDER_ID
    ), ABP AS (
        SELECT ORD.ACCOUNT_ID, ORD.ORDER_ID, ORD.ORDER_BODY_ID, 
               ORD.QUANTITY, ORD.VALUE, ORD.TAX_INCL, 
               ORD.CHARGE_TYPE, ORD.SERVICE_ID, ORD.SUBSERVICE_ID, 
               ORD.DATE_FROM, ORD.DATE_TO,
               ORD.ORD_DAYS, ORD.MON_DAYS, NVL(LCK.LCK_DAYS,0) LCK_DAYS,
               CASE
                WHEN ORD.ORD_DAYS < NVL(LCK.LCK_DAYS,0) THEN 1
                ELSE ((ORD.ORD_DAYS - NVL(LCK.LCK_DAYS,0))/MON_DAYS)
               END K_DAYS
          FROM ORD, LCK
         WHERE ORD.ORDER_ID = LCK.ORDER_ID(+)
    )
    SELECT B.BILL_ID, B.REP_PERIOD_ID, SQ_ITEM_ID.NEXTVAL ITEM_ID,
           ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE,  
           ROUND((K_DAYS * QUANTITY * VALUE),2) ITEM_TOTAL,
           DATE_FROM, DATE_TO, TAX_INCL, 
           PK00_CONST.c_ITEM_TYPE_BILL,
           Pk00_Const.c_ITEM_STATE_OPEN,
           ABP.ORDER_BODY_ID
      FROM ABP, BILL_T B, BILLING_QUEUE_T BQ
     WHERE ABP.ACCOUNT_ID  = B.ACCOUNT_ID
       AND B.REP_PERIOD_ID = p_rep_period_id
       AND B.BILL_ID       = BQ.BILL_ID
       AND B.ACCOUNT_ID    = BQ.ACCOUNT_ID
       AND B.BILL_TYPE     IN (Pk00_Const.c_BILL_TYPE_REC, 
                               Pk00_Const.c_BILL_TYPE_DBT, 
                               Pk00_Const.c_BILL_TYPE_OLD)
       AND BQ.TASK_ID      = p_task_id
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
PROCEDURE Charge_ABP_by_month_tariff(
          p_task_id       IN INTEGER,              -- ID задачи
          p_rep_period_id IN INTEGER,              -- период выставляемго счета
          p_period_id     IN INTEGER DEFAULT NULL  -- период данных
       )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Charge_ABP_by_month_tariff';
    v_period_id     INTEGER;
    v_period_from   DATE;
    v_period_to     DATE;
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    --  если период данных не задан считаем его равным периоду счета  
    IF p_period_id IS NULL THEN
       v_period_id := p_rep_period_id;
    ELSE
       v_period_id := p_period_id;
    END IF;
    --
    v_period_from := Pk04_Period.Period_from(v_period_id);
    v_period_to   := Pk04_Period.Period_to(v_period_id);
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем позиции начислений на абонплату для счетов биллингового периода
    --
    INSERT INTO ITEM_T I(
       BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
       CHARGE_TYPE, ITEM_TOTAL, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
       ITEM_STATUS, ORDER_BODY_ID
    )
    WITH ORD AS (   -- заказы, имющие компоменту услуги - АБОНПЛАТА
        SELECT  ACCOUNT_ID, ORDER_ID, ORDER_BODY_ID, QUANTITY, VALUE, TAX_INCL, 
                CHARGE_TYPE, SERVICE_ID, SUBSERVICE_ID,
                DATE_FROM, DATE_TO,
                ROUND(DATE_TO-DATE_FROM) ORD_DAYS,
                ROUND(v_period_to - v_period_from) MON_DAYS
          FROM (      
            SELECT O.ACCOUNT_ID, OB.ORDER_ID, OB.ORDER_BODY_ID, 
                   OB.QUANTITY, 
                   CASE
                     WHEN R.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN (T.PRICE * 28.6)
                     ELSE T.PRICE
                   END VALUE,
                   R.TAX_INCL, 
                   OB.CHARGE_TYPE, O.SERVICE_ID, OB.SUBSERVICE_ID,
                   CASE
                    WHEN GREATEST(O.DATE_FROM, OB.DATE_FROM) <= v_period_from  
                    THEN v_period_from ELSE GREATEST(O.DATE_FROM, OB.DATE_FROM) 
                   END DATE_FROM,
                   CASE
                    WHEN LEAST(NVL(O.DATE_TO,v_period_to), NVL(OB.DATE_TO,v_period_to)) >= v_period_to  
                    THEN v_period_to  ELSE LEAST(NVL(O.DATE_TO,v_period_to), NVL(OB.DATE_TO,v_period_to))
                   END DATE_TO
              FROM ORDER_BODY_T OB, ORDER_T O, RATEPLAN_T R, MONTH_TARIFF_T T
             WHERE OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_REC
               AND OB.RATEPLAN_ID  = R.RATEPLAN_ID
               AND R.RATESYSTEM_ID = Pk00_Const.с_RATESYS_MON_TRF_ID
               AND R.RATEPLAN_ID   = T.RATEPLAN_ID
               AND T.PERIOD_ID     = v_period_id
               AND O.ORDER_ID      = OB.ORDER_ID
               AND OB.DATE_FROM   <= v_period_to
               AND (v_period_from <= OB.DATE_TO OR OB.DATE_TO IS NULL)
               AND O.DATE_FROM    <= v_period_to
               AND (v_period_from <= O.DATE_TO OR O.DATE_TO IS NULL)
            )
    ), LCK AS (    -- блокировки, биллингового периода
        SELECT ORDER_ID, ROUND(SUM(LOCK_DAYS)) LCK_DAYS
          FROM (
            SELECT ORDER_ID,
                   CASE 
                    -- заблокирован весь месяц:  DATE_FROM---[-------]---DATE_TO
                    WHEN DATE_FROM <= v_period_from 
                     AND (v_period_to <= DATE_TO OR DATE_TO IS NULL) 
                     THEN v_period_to - v_period_from
                    -- заблокирован внутри месяца: [--DATE_FROM-----DATE_TO--]
                    WHEN v_period_from < DATE_FROM 
                     AND DATE_TO < v_period_to 
                     THEN DATE_TO - DATE_FROM
                    -- заблокирован в предыдущем периоде, открыт в текущем: ---DATE_FROM-- [---DATE_TO---]
                    WHEN DATE_FROM <= v_period_from 
                     AND DATE_TO < v_period_to 
                     THEN DATE_TO - v_period_from
                    -- заблокирован в текущем периоде, и остается до конца периода:---[--DATE_FROM--]---DATE_TO---  
                    WHEN v_period_from < DATE_FROM  
                     AND (v_period_to <= DATE_TO OR DATE_TO IS NULL)
                     THEN v_period_to - DATE_FROM
                    -- возможем некорректный ввод
                    ELSE 0
                   END LOCK_DAYS
              FROM ORDER_LOCK_T L
            WHERE DATE_FROM <= v_period_to   -- только блокировки 
              AND (DATE_TO IS NULL OR v_period_from <= DATE_TO)    -- действовавшие в периоде
        )
        GROUP BY ORDER_ID
    ), ABP AS (
        SELECT ORD.ACCOUNT_ID, ORD.ORDER_ID, ORD.ORDER_BODY_ID, 
               ORD.QUANTITY, ORD.VALUE, ORD.TAX_INCL, 
               ORD.CHARGE_TYPE, ORD.SERVICE_ID, ORD.SUBSERVICE_ID, 
               ORD.DATE_FROM, ORD.DATE_TO,
               ORD.ORD_DAYS, ORD.MON_DAYS, NVL(LCK.LCK_DAYS,0) LCK_DAYS,
               CASE
                WHEN ORD.ORD_DAYS < NVL(LCK.LCK_DAYS,0) THEN 1
                ELSE ((ORD.ORD_DAYS - NVL(LCK.LCK_DAYS,0))/MON_DAYS)
               END K_DAYS
          FROM ORD, LCK
         WHERE ORD.ORDER_ID = LCK.ORDER_ID(+)
    )
    SELECT B.BILL_ID, B.REP_PERIOD_ID, SQ_ITEM_ID.NEXTVAL ITEM_ID,
           ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE,  
           -- на текущий момент ITEM для Ю всегда считается без налогов
           -- возможно позже исправлю
           CASE 
             WHEN TAX_INCL = Pk00_Const.c_RATEPLAN_TAX_INCL THEN 
               ROUND((K_DAYS * QUANTITY * VALUE/1.18),2)
             ELSE
               ROUND((K_DAYS * QUANTITY * VALUE),2)
           END ITEM_TOTAL,
           DATE_FROM, DATE_TO, 
           Pk00_Const.c_RATEPLAN_TAX_NOT_INCL, -- начисления для Ю по безналоговому тарифу
           PK00_CONST.c_ITEM_TYPE_BILL,
           Pk00_Const.c_ITEM_STATE_OPEN,
           ABP.ORDER_BODY_ID
      FROM ABP, BILL_T B, BILLING_QUEUE_T BQ
     WHERE ABP.ACCOUNT_ID  = B.ACCOUNT_ID
       AND B.REP_PERIOD_ID = p_rep_period_id
       AND B.BILL_ID       = BQ.BILL_ID
       AND B.ACCOUNT_ID    = BQ.ACCOUNT_ID
       AND B.BILL_TYPE     IN (Pk00_Const.c_BILL_TYPE_REC, 
                               Pk00_Const.c_BILL_TYPE_DBT, 
                               Pk00_Const.c_BILL_TYPE_OLD)
       AND BQ.TASK_ID      = p_task_id
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
PROCEDURE Put_ABP_detail(
          p_task_id       IN INTEGER,              -- ID задачи
          p_rep_period_id IN INTEGER               -- период выставляемго счета
       )    
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Subservice_charge_MIN';
    v_period_id     INTEGER;
    v_period_from   DATE;
    v_period_to     DATE;
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_rep_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Заполняем поля описания начислений абонплаты для стандартной детализации
    -- 1) Услуга NPL
    -- 2) Услуга IP_ACCESS 
    -- 3) Услуга EPL
    -- 4) Услуга DPL
    -- 5) Услуга LM
    UPDATE ITEM_T I
       SET (I.DESCR) = (
           SELECT CASE
                  WHEN I.SERVICE_ID = Pk00_Const.c_SERVICE_NPL THEN
                    INF.POINT_SRC||' - '||INF.POINT_DST||', '||INF.SPEED_VALUE||' '||D.NAME
                  WHEN I.SERVICE_ID = Pk00_Const.c_SERVICE_SYNC THEN
                    INF.POINT_SRC||' - '||INF.POINT_DST||', '||INF.SPEED_VALUE||' '||D.NAME
                  WHEN I.SERVICE_ID = Pk00_Const.c_SERVICE_IP_ACCESS THEN
                    'IP port, '||INF.POINT_SRC||', '||INF.SPEED_VALUE||' '||D.NAME
                  WHEN I.SERVICE_ID = Pk00_Const.c_SERVICE_EPL THEN
                    INF.POINT_SRC||', '||INF.SPEED_VALUE||' '||D.NAME
                  WHEN I.SERVICE_ID = Pk00_Const.c_SERVICE_DPL THEN
                    INF.POINT_SRC||', '||INF.SPEED_VALUE||' '||D.NAME
                  WHEN I.SERVICE_ID = Pk00_Const.c_SERVICE_LM THEN
                    INF.POINT_SRC||', '||INF.SPEED_VALUE||' '||D.NAME
                  ELSE NULL
                  END DESCR 
             FROM IP_CHANNEL_INFO_T INF, DICTIONARY_T D
            WHERE INF.ORDER_BODY_ID = I.ORDER_BODY_ID
              AND INF.SPEED_UNIT = D.KEY_ID
              AND D.PARENT_ID = Pk00_Const.k_DICT_SPEED_UNIT
       ) 
     WHERE I.REP_PERIOD_ID = p_rep_period_id
       AND I.SERVICE_ID IN ( 
                Pk00_Const.c_SERVICE_NPL,       -- Предоставление магистральных цифровых каналов связи (МЦКС/NPL)
                Pk00_Const.c_SERVICE_IP_ACCESS, -- Доступ в Интернет
                Pk00_Const.c_SERVICE_EPL,       -- Виртуальный канал Ethernet (EPL))
                Pk00_Const.c_SERVICE_LM,        -- Предоставление канала доступа к Единой магистральной цифровой сети связи (ЕМЦСС)
                Pk00_Const.c_SERVICE_DPL,       -- Предоставление цифровых каналов связи (ЦКС)
                Pk00_Const.c_SERVICE_SYNC       -- Синхронизация сетей связи
           )
       AND I.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_REC
       AND EXISTS (
           SELECT * 
             FROM BILLING_QUEUE_T Q
            WHERE Q.BILL_ID = I.BILL_ID
              AND Q.TASK_ID = p_task_id
       )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows, set desc ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  Расчет доплаты до мимнимальной суммы КОМПОМЕНТА УСЛУГИ ЗАКАЗА
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Subservice_charge_MIN(
          p_task_id       IN INTEGER,              -- ID задачи
          p_rep_period_id IN INTEGER,              -- период выставляемго счета
          p_period_id     IN INTEGER DEFAULT NULL  -- период данных
       )    
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Subservice_charge_MIN';
    v_period_id     INTEGER;
    v_period_from   DATE;
    v_period_to     DATE;
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    --  если период данных не задан считаем его равным периоду счета  
    IF p_period_id IS NULL THEN
       v_period_id := p_rep_period_id;
    ELSE
       v_period_id := p_period_id;
    END IF;
    --
    v_period_from := Pk04_Period.Period_from(v_period_id);
    v_period_to   := Pk04_Period.Period_to(v_period_id);
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем позиции начислений доплаты до мимнимальной суммы ЗАКАЗА
    --
    INSERT INTO ITEM_T(
        BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
        CHARGE_TYPE, ITEM_TOTAL, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
        ITEM_STATUS, ORDER_BODY_ID
    )
    WITH LCK AS (
        SELECT ORDER_ID, ROUND(SUM(LOCK_DAYS)) LCK_DAYS
          FROM (
            SELECT ORDER_ID,
                   CASE 
                    -- заблокирован весь месяц:  DATE_FROM---[-------]---DATE_TO
                    WHEN DATE_FROM <= v_period_from 
                     AND (v_period_to <= DATE_TO OR DATE_TO IS NULL) 
                     THEN v_period_to - v_period_from
                    -- заблокирован внутри месяца: [--DATE_FROM-----DATE_TO--]
                    WHEN v_period_from < DATE_FROM 
                     AND DATE_TO < v_period_to 
                     THEN DATE_TO - DATE_FROM
                    -- заблокирован в предыдущем периоде, открыт в текущем: ---DATE_FROM-- [---DATE_TO---]
                    WHEN DATE_FROM <= v_period_from 
                     AND DATE_TO < v_period_to 
                     THEN DATE_TO - v_period_from
                    -- заблокирован в текущем периоде, и остается до конца периода:---[--DATE_FROM--]---DATE_TO---  
                    WHEN v_period_from < DATE_FROM  
                     AND (v_period_to <= DATE_TO OR DATE_TO IS NULL)
                     THEN v_period_to - DATE_FROM
                    -- возможем некорректный ввод
                    ELSE 0
                   END LOCK_DAYS
              FROM ORDER_LOCK_T L
            WHERE DATE_FROM <= v_period_to   -- только блокировки 
              AND (DATE_TO IS NULL OR v_period_from <= DATE_TO)    -- действовавшие в периоде
        )
        GROUP BY ORDER_ID
    ), MP AS (
        SELECT BILL_ID, REP_PERIOD_ID, ACCOUNT_ID, ORDER_ID, ORDER_BODY_ID,
               SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
               DATE_FROM, DATE_TO, TAX_INCL, MIN_VALUE, VAT,
               ROUND(DATE_TO - DATE_FROM) ORD_DAYS,
               ROUND(v_period_to - v_period_from) MON_DAYS
          FROM ( 
            SELECT B.BILL_ID, B.REP_PERIOD_ID, O.ACCOUNT_ID, 
                   O.ORDER_ID, O.SERVICE_ID, 
                   OB.ORDER_BODY_ID, OB.SUBSERVICE_ID, OB.CHARGE_TYPE,
                   CASE
                    WHEN v_period_from <= O.DATE_FROM THEN O.DATE_FROM
                    ELSE v_period_from
                   END DATE_FROM, 
                   CASE
                    WHEN O.DATE_TO <= v_period_to THEN O.DATE_TO
                    ELSE v_period_to
                   END DATE_TO, 
                   OB.TAX_INCL, OB.RATE_VALUE MIN_VALUE, AP.VAT 
              FROM ORDER_T O, ORDER_BODY_T OB, 
                   BILL_T B, ACCOUNT_PROFILE_T AP, BILLING_QUEUE_T BQ
             WHERE (v_period_from <= O.DATE_TO OR O.DATE_TO IS NULL)
               AND O.DATE_FROM    <= v_period_to
               AND (v_period_from <= OB.DATE_TO OR OB.DATE_TO IS NULL)
               AND OB.DATE_FROM    <= v_period_to
               AND OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_MIN -- 'MIN'
               AND OB.RATE_LEVEL_ID= Pk00_Const.c_RATE_LEVEL_SUBSRV
               AND OB.ORDER_ID     = O.ORDER_ID
               AND B.ACCOUNT_ID    = O.ACCOUNT_ID
               AND B.ACCOUNT_ID    = AP.ACCOUNT_ID
               AND B.REP_PERIOD_ID = p_rep_period_id
               AND B.ACCOUNT_ID    = BQ.ACCOUNT_ID
               AND B.BILL_ID       = BQ.BILL_ID
               AND AP.PROFILE_ID   = BQ.PROFILE_ID
               AND B.BILL_TYPE     IN (Pk00_Const.c_BILL_TYPE_REC, 
                                       Pk00_Const.c_BILL_TYPE_DBT, 
                                       Pk00_Const.c_BILL_TYPE_OLD)
               AND BQ.TASK_ID      = p_task_id
        )
    ), IT AS (
        SELECT I.BILL_ID, I.REP_PERIOD_ID, 
               I.ORDER_ID, I.SERVICE_ID, I.SUBSERVICE_ID,
               I.TAX_INCL, SUM(I.ITEM_TOTAL) SUM_ITEM_TOTAL
          FROM ITEM_T I, ORDER_BODY_T OB, BILLING_QUEUE_T BQ
         WHERE OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_MIN -- 'MIN'
           AND OB.RATE_LEVEL_ID= Pk00_Const.c_RATE_LEVEL_SUBSRV
           AND OB.DATE_FROM   <= v_period_to
           AND (v_period_from <= OB.DATE_TO OR OB.DATE_TO IS NULL)
           AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG -- 'USG'    -- только трафик 
           AND I.REP_PERIOD_ID = p_rep_period_id
           AND I.DATE_FROM    <= v_period_to        -- рассматриваем только item-s
           AND I.DATE_TO      >= v_period_from      -- указанного периода
           AND I.SUBSERVICE_ID = OB.SUBSERVICE_ID
           AND I.BILL_ID       = BQ.BILL_ID
           AND BQ.TASK_ID      = p_task_id
         GROUP BY I.BILL_ID, I.REP_PERIOD_ID, 
                  I.ORDER_ID, I.SERVICE_ID, I.SUBSERVICE_ID, I.TAX_INCL
    ), ITM AS ( 
      SELECT 
           MP.ACCOUNT_ID, MP.ORDER_ID, MP.ORDER_BODY_ID,
           MP.SERVICE_ID, MP.SUBSERVICE_ID,
           MP.DATE_FROM, MP.DATE_TO, MP.CHARGE_TYPE, 
           MP.BILL_ID, MP.REP_PERIOD_ID, 
           NVL(IT.SUM_ITEM_TOTAL,0) SUM_ITEM_TOTAL, 
           NVL(IT.TAX_INCL, MP.TAX_INCL) IT_TAX_INCL,
           CASE 
             WHEN IT.TAX_INCL = 'N' AND MP.TAX_INCL = 'Y' THEN ROUND(MP.MIN_VALUE /(1 + MP.VAT / 100),2) 
             WHEN IT.TAX_INCL = 'Y' AND MP.TAX_INCL = 'N' THEN MP.MIN_VALUE + ROUND(MP.MIN_VALUE * MP.VAT / 100, 2)
             ELSE MP.MIN_VALUE 
           END MIN_VALUE,
           NVL(LCK.LCK_DAYS,0) LCK_DAYS,
           ((MP.ORD_DAYS - NVL(LCK.LCK_DAYS,0))/MON_DAYS) K_DAYS
        FROM MP, IT, LCK
       WHERE MP.BILL_ID      = IT.BILL_ID(+)
         AND MP.REP_PERIOD_ID= IT.REP_PERIOD_ID(+)
         AND MP.ORDER_ID     = IT.ORDER_ID(+)
         AND MP.SERVICE_ID   = IT.SERVICE_ID(+)
         AND MP.SUBSERVICE_ID= IT.SUBSERVICE_ID(+)
         AND MP.ORDER_ID     = LCK.ORDER_ID(+)
    )
    SELECT BILL_ID, REP_PERIOD_ID, SQ_ITEM_ID.NEXTVAL ITEM_ID, ORDER_ID, 
           SERVICE_ID, 
           --Pk00_Const.c_SUBSRV_MIN SUBSERVICE_ID, -- 
           SUBSERVICE_ID, --(решение А.Ю.Гурова)
           CHARGE_TYPE,
           ROUND((K_DAYS * MIN_VALUE - SUM_ITEM_TOTAL),2) ITEM_TOTAL,
           DATE_FROM, DATE_TO, IT_TAX_INCL, 
           PK00_CONST.c_ITEM_TYPE_BILL,
           Pk00_Const.c_ITEM_STATE_OPEN,
           ORDER_BODY_ID 
      FROM ITM
     WHERE K_DAYS > 0
       AND ROUND((K_DAYS * MIN_VALUE - SUM_ITEM_TOTAL),2) > 0
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  Расчет доплаты до мимнимальной суммы ЗАКАЗА
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Order_charge_MIN(
          p_task_id       IN INTEGER,              -- ID задачи
          p_rep_period_id IN INTEGER,              -- период выставляемго счета
          p_period_id     IN INTEGER DEFAULT NULL  -- период данных
       )    
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Order_charge_MIN';
    v_period_id     INTEGER;
    v_period_from   DATE;
    v_period_to     DATE;
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    --  если период данных не задан считаем его равным периоду счета  
    IF p_period_id IS NULL THEN
       v_period_id := p_rep_period_id;
    ELSE
       v_period_id := p_period_id;
    END IF;
    --
    v_period_from := Pk04_Period.Period_from(v_period_id);
    v_period_to   := Pk04_Period.Period_to(v_period_id);
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем позиции начислений доплаты до мимнимальной суммы ЗАКАЗА
    --
    INSERT INTO ITEM_T(
        BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
        CHARGE_TYPE, ITEM_TOTAL, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
        ITEM_STATUS, ORDER_BODY_ID
    )
    WITH LCK AS (
        SELECT ORDER_ID, ROUND(SUM(LOCK_DAYS)) LCK_DAYS
          FROM (
            SELECT ORDER_ID,
                   CASE 
                    -- заблокирован весь месяц:  DATE_FROM---[-------]---DATE_TO
                    WHEN DATE_FROM <= v_period_from 
                     AND (v_period_to <= DATE_TO OR DATE_TO IS NULL) 
                     THEN v_period_to - v_period_from
                    -- заблокирован внутри месяца: [--DATE_FROM-----DATE_TO--]
                    WHEN v_period_from < DATE_FROM 
                     AND DATE_TO < v_period_to 
                     THEN DATE_TO - DATE_FROM
                    -- заблокирован в предыдущем периоде, открыт в текущем: ---DATE_FROM-- [---DATE_TO---]
                    WHEN DATE_FROM <= v_period_from 
                     AND DATE_TO < v_period_to 
                     THEN DATE_TO - v_period_from
                    -- заблокирован в текущем периоде, и остается до конца периода:---[--DATE_FROM--]---DATE_TO---  
                    WHEN v_period_from < DATE_FROM  
                     AND (v_period_to <= DATE_TO OR DATE_TO IS NULL)
                     THEN v_period_to - DATE_FROM
                    -- возможем некорректный ввод
                    ELSE 0
                   END LOCK_DAYS
              FROM ORDER_LOCK_T L
            WHERE DATE_FROM <= v_period_to   -- только блокировки 
              AND (DATE_TO IS NULL OR v_period_from <= DATE_TO)    -- действовавшие в периоде
        )
        GROUP BY ORDER_ID
    ), MP AS (
        SELECT BILL_ID, REP_PERIOD_ID, 
               ACCOUNT_ID, ORDER_ID, ORDER_BODY_ID,
               SERVICE_ID, CHARGE_TYPE, 
               DATE_FROM, DATE_TO, TAX_INCL, MIN_VALUE, VAT,
               ROUND(DATE_TO - DATE_FROM) ORD_DAYS,
               ROUND(v_period_to - v_period_from) MON_DAYS
          FROM ( 
            SELECT B.BILL_ID, B.REP_PERIOD_ID, 
                   O.ACCOUNT_ID, O.ORDER_ID, OB.ORDER_BODY_ID,
                   O.SERVICE_ID, OB.CHARGE_TYPE,
                   CASE
                    WHEN v_period_from <= O.DATE_FROM THEN O.DATE_FROM
                    ELSE v_period_from
                   END DATE_FROM, 
                   CASE
                    WHEN O.DATE_TO <= v_period_to THEN O.DATE_TO
                    ELSE v_period_to
                   END DATE_TO, 
                   OB.TAX_INCL, OB.RATE_VALUE MIN_VALUE, AP.VAT 
              FROM ORDER_T O, ORDER_BODY_T OB, 
                   BILL_T B, ACCOUNT_PROFILE_T AP, BILLING_QUEUE_T BQ
             WHERE OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_MIN -- 'MIN'
               AND OB.RATE_LEVEL_ID= Pk00_Const.c_RATE_LEVEL_ORDER
               AND OB.DATE_FROM   <= v_period_to
               AND (v_period_from <= OB.DATE_TO OR OB.DATE_TO IS NULL)
               AND OB.ORDER_ID     = O.ORDER_ID
               AND O.DATE_FROM    <= v_period_to             
               AND (v_period_from <= O.DATE_TO OR O.DATE_TO IS NULL)
               AND B.ACCOUNT_ID    = O.ACCOUNT_ID
               AND B.ACCOUNT_ID    = AP.ACCOUNT_ID
               AND B.REP_PERIOD_ID = p_rep_period_id
               AND B.ACCOUNT_ID    = BQ.ACCOUNT_ID
               AND B.BILL_ID       = BQ.BILL_ID
               AND AP.PROFILE_ID   = BQ.PROFILE_ID
               AND B.BILL_TYPE     IN (Pk00_Const.c_BILL_TYPE_REC, 
                                       Pk00_Const.c_BILL_TYPE_DBT, 
                                       Pk00_Const.c_BILL_TYPE_OLD)
               AND BQ.TASK_ID      = p_task_id
        )
    ), IT AS (
        SELECT I.BILL_ID, I.REP_PERIOD_ID, I.ORDER_ID, I.SERVICE_ID, I.TAX_INCL, SUM(I.ITEM_TOTAL) SUM_ITEM_TOTAL
          FROM ITEM_T I, ORDER_BODY_T OB, BILLING_QUEUE_T BQ
         WHERE OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_MIN -- 'MIN'
           AND OB.RATE_LEVEL_ID= Pk00_Const.c_RATE_LEVEL_ORDER
           AND OB.DATE_FROM   <= v_period_to
           AND (v_period_from <= OB.DATE_TO OR OB.DATE_TO IS NULL)
           AND OB.ORDER_ID     = I.ORDER_ID
           AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG -- 'USG'    -- только трафик 
           AND I.REP_PERIOD_ID = p_rep_period_id
           AND I.DATE_FROM    <= v_period_to        -- рассматриваем только item-s
           AND I.DATE_TO      >= v_period_from      -- указанного периода
           AND I.BILL_ID       = BQ.BILL_ID
           AND BQ.TASK_ID      = p_task_id
         GROUP BY I.BILL_ID, I.REP_PERIOD_ID, I.ORDER_ID, I.SERVICE_ID, I.TAX_INCL
    ), ITM AS ( 
      SELECT 
           MP.ACCOUNT_ID, MP.ORDER_ID, MP.ORDER_BODY_ID, MP.SERVICE_ID, 
           MP.DATE_FROM, MP.DATE_TO, MP.CHARGE_TYPE, 
           MP.BILL_ID, MP.REP_PERIOD_ID, 
           NVL(IT.SUM_ITEM_TOTAL,0) SUM_ITEM_TOTAL, 
           NVL(IT.TAX_INCL, MP.TAX_INCL) IT_TAX_INCL,
           CASE 
             WHEN IT.TAX_INCL = 'N' AND MP.TAX_INCL = 'Y' THEN ROUND(MP.MIN_VALUE /(1 + MP.VAT / 100),2) 
             WHEN IT.TAX_INCL = 'Y' AND MP.TAX_INCL = 'N' THEN MP.MIN_VALUE + ROUND(MP.MIN_VALUE * MP.VAT / 100, 2)
             ELSE MP.MIN_VALUE 
           END MIN_VALUE,
           NVL(LCK.LCK_DAYS,0) LCK_DAYS,
           ((MP.ORD_DAYS - NVL(LCK.LCK_DAYS,0))/MON_DAYS) K_DAYS
        FROM MP, IT, LCK
       WHERE MP.BILL_ID      = IT.BILL_ID(+)
         AND MP.REP_PERIOD_ID= IT.REP_PERIOD_ID(+)
         AND MP.ORDER_ID     = IT.ORDER_ID(+)
         AND MP.SERVICE_ID   = IT.SERVICE_ID(+)
         AND MP.ORDER_ID     = LCK.ORDER_ID(+)
    )
    SELECT BILL_ID, REP_PERIOD_ID, SQ_ITEM_ID.NEXTVAL ITEM_ID, ORDER_ID, 
           SERVICE_ID, Pk00_Const.c_SUBSRV_MIN SUBSERVICE_ID, CHARGE_TYPE,
           ROUND((K_DAYS * MIN_VALUE - SUM_ITEM_TOTAL),2) ITEM_TOTAL,
           DATE_FROM, DATE_TO, IT_TAX_INCL, 
           PK00_CONST.c_ITEM_TYPE_BILL,
           Pk00_Const.c_ITEM_STATE_OPEN,
           ORDER_BODY_ID 
      FROM ITM
     WHERE K_DAYS > 0
       AND ROUND((K_DAYS * MIN_VALUE - SUM_ITEM_TOTAL),2) > 0
       
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------- --
--  Расчет доплаты до мимнимальной суммы Лицевого счета
-- ----------------------------------------------------------------- --
PROCEDURE Account_charge_MIN(
          p_task_id       IN INTEGER,              -- ID задачи
          p_rep_period_id IN INTEGER,              -- период выставляемго счета
          p_period_id     IN INTEGER DEFAULT NULL  -- период данных
       )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Account_charge_MIN';
    v_period_id     INTEGER;
    v_period_from   DATE;
    v_period_to     DATE;
    v_bill_id       INTEGER;
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    --  если период данных не задан считаем его равным периоду счета  
    IF p_period_id IS NULL THEN
       v_period_id := p_rep_period_id;
    ELSE
       v_period_id := p_period_id;
    END IF;
    --
    v_period_from := Pk04_Period.Period_from(v_period_id);
    v_period_to   := Pk04_Period.Period_to(v_period_id);
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем позиции начислений доплаты до мимнимальной суммы ПЕРИОДИЧЕСКОГО СЧЕТА по Л/С
    --
    INSERT INTO ITEM_T(
        BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
        CHARGE_TYPE, ITEM_TOTAL, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
        ITEM_STATUS, ORDER_BODY_ID
    )
    WITH LCK AS (
        SELECT ORDER_ID, ROUND(SUM(LOCK_DAYS)) LCK_DAYS
          FROM (
            SELECT ORDER_ID,
                   CASE 
                    -- заблокирован весь месяц:  DATE_FROM---[-------]---DATE_TO
                    WHEN DATE_FROM <= v_period_from 
                     AND (v_period_to <= DATE_TO OR DATE_TO IS NULL) 
                     THEN v_period_to - v_period_from
                    -- заблокирован внутри месяца: [--DATE_FROM-----DATE_TO--]
                    WHEN v_period_from < DATE_FROM 
                     AND DATE_TO < v_period_to 
                     THEN DATE_TO - DATE_FROM
                    -- заблокирован в предыдущем периоде, открыт в текущем: ---DATE_FROM-- [---DATE_TO---]
                    WHEN DATE_FROM <= v_period_from 
                     AND DATE_TO < v_period_to 
                     THEN DATE_TO - v_period_from
                    -- заблокирован в текущем периоде, и остается до конца периода:---[--DATE_FROM--]---DATE_TO---  
                    WHEN v_period_from < DATE_FROM  
                     AND (v_period_to <= DATE_TO OR DATE_TO IS NULL)
                     THEN v_period_to - DATE_FROM
                    -- возможем некорректный ввод
                    ELSE 0
                   END LOCK_DAYS
              FROM ORDER_LOCK_T L
            WHERE DATE_FROM <= v_period_to   -- только блокировки 
              AND (DATE_TO IS NULL OR v_period_from <= DATE_TO)    -- действовавшие в периоде
        )
        GROUP BY ORDER_ID
    ), MP AS (
        SELECT BILL_ID, REP_PERIOD_ID, 
               ACCOUNT_ID, ORDER_ID, ORDER_BODY_ID,
               SERVICE_ID, CHARGE_TYPE, 
               DATE_FROM, DATE_TO, TAX_INCL, MIN_VALUE, VAT,
               ROUND(DATE_TO - DATE_FROM) ORD_DAYS,
               ROUND(v_period_to - v_period_from) MON_DAYS
          FROM ( 
            SELECT B.BILL_ID, B.REP_PERIOD_ID, 
                   O.ACCOUNT_ID, O.ORDER_ID, OB.ORDER_BODY_ID,
                   O.SERVICE_ID, OB.CHARGE_TYPE,
                   CASE
                    WHEN v_period_from <= AP.DATE_FROM THEN AP.DATE_FROM
                    ELSE v_period_from
                   END DATE_FROM, 
                   CASE
                    WHEN AP.DATE_TO IS NULL THEN v_period_to
                    WHEN AP.DATE_TO <= v_period_to THEN AP.DATE_TO
                    ELSE v_period_to
                   END DATE_TO, 
                   OB.TAX_INCL, OB.RATE_VALUE MIN_VALUE, AP.VAT 
              FROM ORDER_T O, ORDER_BODY_T OB, 
                   BILL_T B, ACCOUNT_PROFILE_T AP, 
                   BILLING_QUEUE_T BQ
             WHERE OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_MIN -- 'MIN'
               AND OB.RATE_LEVEL_ID= Pk00_Const.c_RATE_LEVEL_ACCOUNT
               AND OB.DATE_FROM   <= v_period_to
               AND (v_period_from <= OB.DATE_TO OR OB.DATE_TO IS NULL)
               AND OB.ORDER_ID     = O.ORDER_ID
               AND B.ACCOUNT_ID    = O.ACCOUNT_ID
               AND B.ACCOUNT_ID    = AP.ACCOUNT_ID
               AND B.ACCOUNT_ID    = AP.ACCOUNT_ID
               AND B.REP_PERIOD_ID = p_rep_period_id
               AND B.ACCOUNT_ID    = BQ.ACCOUNT_ID
               AND B.BILL_ID       = BQ.BILL_ID
               AND AP.PROFILE_ID   = BQ.PROFILE_ID
               AND B.BILL_TYPE     IN (Pk00_Const.c_BILL_TYPE_REC, 
                                       Pk00_Const.c_BILL_TYPE_DBT, 
                                       Pk00_Const.c_BILL_TYPE_OLD)
               AND BQ.TASK_ID      = p_task_id
        )
    ), IT AS (
        SELECT I.BILL_ID, I.REP_PERIOD_ID, I.TAX_INCL, 
               SUM(I.ITEM_TOTAL) SUM_ITEM_TOTAL
          FROM ITEM_T I, ORDER_BODY_T OB, ORDER_T O, BILLING_QUEUE_T BQ
         WHERE OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_MIN -- 'MIN'
           AND OB.RATE_LEVEL_ID= Pk00_Const.c_RATE_LEVEL_ACCOUNT
           AND OB.DATE_FROM   <= v_period_to
           AND (v_period_from <= OB.DATE_TO OR OB.DATE_TO IS NULL)
           AND OB.ORDER_ID     = O.ORDER_ID 
           AND O.ACCOUNT_ID    = BQ.ACCOUNT_ID
           AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG -- 'USG'    -- только трафик
           AND I.REP_PERIOD_ID = p_rep_period_id
           AND I.DATE_FROM    <= v_period_to        -- рассматриваем только item-s
           AND I.DATE_TO      >= v_period_from      -- указанного периода
           AND I.BILL_ID       = BQ.BILL_ID
           AND BQ.TASK_ID      = p_task_id
         GROUP BY I.BILL_ID, I.REP_PERIOD_ID, I.TAX_INCL
    ), ITM AS ( 
      SELECT 
           MP.ACCOUNT_ID, MP.ORDER_ID, MP.ORDER_BODY_ID, MP.SERVICE_ID, 
           MP.DATE_FROM, MP.DATE_TO, MP.CHARGE_TYPE, 
           MP.BILL_ID, MP.REP_PERIOD_ID, 
           NVL(IT.SUM_ITEM_TOTAL,0) SUM_ITEM_TOTAL, 
           NVL(IT.TAX_INCL, MP.TAX_INCL) IT_TAX_INCL,
           CASE 
             WHEN IT.TAX_INCL = 'N' AND MP.TAX_INCL = 'Y' THEN ROUND(MP.MIN_VALUE /(1 + MP.VAT / 100),2) 
             WHEN IT.TAX_INCL = 'Y' AND MP.TAX_INCL = 'N' THEN MP.MIN_VALUE + ROUND(MP.MIN_VALUE * MP.VAT / 100, 2)
             ELSE MP.MIN_VALUE 
           END MIN_VALUE,
           NVL(LCK.LCK_DAYS,0) LCK_DAYS,
           ((MP.ORD_DAYS - NVL(LCK.LCK_DAYS,0))/MON_DAYS) K_DAYS
        FROM MP, IT, LCK
       WHERE MP.BILL_ID      = IT.BILL_ID(+)
         AND MP.REP_PERIOD_ID= IT.REP_PERIOD_ID(+)
         AND MP.ORDER_ID     = LCK.ORDER_ID(+)
    )
    SELECT BILL_ID, REP_PERIOD_ID, SQ_ITEM_ID.NEXTVAL ITEM_ID, ORDER_ID, 
           SERVICE_ID, Pk00_Const.c_SUBSRV_MIN SUBSERVICE_ID, CHARGE_TYPE,
           ROUND((K_DAYS * MIN_VALUE - SUM_ITEM_TOTAL),2) ITEM_TOTAL,
           DATE_FROM, DATE_TO, IT_TAX_INCL, 
           PK00_CONST.c_ITEM_TYPE_BILL,
           Pk00_Const.c_ITEM_STATE_OPEN,
           ORDER_BODY_ID 
      FROM ITM
     WHERE K_DAYS > 0
       AND ROUND((K_DAYS * MIN_VALUE - SUM_ITEM_TOTAL),2) > 0
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
PROCEDURE Rollback_fixrates(p_task_id IN INTEGER, p_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_fixrates';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    DELETE FROM ITEM_T I
    WHERE I.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_MIN, 
                            Pk00_Const.c_CHARGE_TYPE_REC)
    AND I.REP_PERIOD_ID = p_period_id
    AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
    AND I.EXTERNAL_ID IS NULL
    AND EXISTS (
        SELECT * FROM BILLING_QUEUE_T Q
         WHERE Q.BILL_ID = I.BILL_ID
           AND Q.TASK_ID = p_task_id
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
-- Начисление абонентской платы и доплаты до минимальной суммы
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_fixrates(
          p_task_id        IN INTEGER,              -- ID задачи
          p_rep_period_id  IN INTEGER,              -- период выставляемго счета
          p_data_period_id IN INTEGER DEFAULT NULL  -- период данных  
       )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Charge_fixrates';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Начисление абонентской платы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Charge_ABP( 
            p_task_id       => p_task_id,
            p_rep_period_id => p_rep_period_id,
            p_period_id     => p_data_period_id 
         );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  Расчет абонплаты (subscriber fee) по тарифам укзанным 
    -- индивидуально для каждого месяца (MONTH_TARIFF_T),
    -- такой тариф иногда требуют бюджетные организации, 
    -- заключающие договор на один год, затем тендер и новый договор
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Charge_ABP_by_month_tariff(
            p_task_id       => p_task_id,
            p_rep_period_id => p_rep_period_id,
            p_period_id     => p_data_period_id
         );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Начисление доплаты до минимальной суммы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  Расчет доплаты до мимнимальной суммы КОМПОМЕНТА УСЛУГИ ЗАКАЗА
    Subservice_charge_MIN( 
            p_task_id       => p_task_id,
            p_rep_period_id => p_rep_period_id,
            p_period_id     => p_data_period_id 
         );

    --  Расчет доплаты до мимнимальной суммы ЗАКАЗА
    Order_charge_MIN( 
            p_task_id       => p_task_id,
            p_rep_period_id => p_rep_period_id,
            p_period_id     => p_data_period_id 
         );
    
    --  Расчет доплаты до мимнимальной суммы Лицевого счета
    Account_charge_MIN( 
            p_task_id       => p_task_id,
            p_rep_period_id => p_rep_period_id,
            p_period_id     => p_data_period_id 
         );

    --  Заполнить поля необходимые для получения детализации по абонплате
    Put_abp_detail(
            p_task_id       => p_task_id,
            p_rep_period_id => p_rep_period_id
         );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

END PK36_BILLING_FIXRATE_OLD;
/
