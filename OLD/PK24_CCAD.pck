CREATE OR REPLACE PACKAGE PK24_CCAD
IS
    --
    -- Пакет для работы с КСАД
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK24_CCAD';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- Ошибки тарификации КСАД
    c_BERR_OK          			  CONSTANT integer :=  0;   -- OK
    c_BERR_RATEPLAN_NotFound  CONSTANT integer := -1;   -- Не найден rateplan
    c_BERR_VPN_PRICE_NotFound CONSTANT integer := -2;   -- Не найдена цена для qos,zone,rzone
    c_BERR_VOL_PRICE_NotFound CONSTANT integer := -3;   -- Не найдена цена для объемного тарифа 
    c_BERR_NotDefined         CONSTANT integer := -100; -- Неопознанная ошибка

    -- --------------------------------------------------------------------- --
    --  Формирование позиций счетов по BDR CCAD
    -- --------------------------------------------------------------------- --
    PROCEDURE Load_BDRs(
                   p_period_id IN INTEGER
               );
           
    -- --------------------------------------------------------------------- --
    -- BURST (service_id = 104, subservice_id = 40)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Bdr2Item_BURST(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER DEFAULT NULL
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- BURST - удалить начисления за указанный период
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Del_Items_BURST(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER
               );

    -- --------------------------------------------------------------------- --
    -- IP_VOLUME  - ступенчатый по объему (service_id = 104, subservice_id = 39)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    PROCEDURE Bdr2Item_IP_VOLUME(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER DEFAULT NULL
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- IP_VOLUME - удалить начисления за указанный период
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Del_Items_IP_VOLUME(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER
               );

    -- --------------------------------------------------------------------- --
    -- RT - раздельная тарификация (service_id = 104, subservice_id = 31)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    PROCEDURE Bdr2Item_IP_RT(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER DEFAULT NULL
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- RT - удалить начисления за указанный период
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Del_Items_IP_RT(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER
               );

    -- --------------------------------------------------------------------- --
    -- VPN (service_id = 106, subservice_id = 39)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    PROCEDURE Bdr2Item_VPN(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER DEFAULT NULL
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- VPN - удалить начисления за указанный период
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Del_Items_VPN(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER
               );

    -- --------------------------------------------------------------------- --
    --  Данные для заполнения очереди на тарификацию счетов
    -- --------------------------------------------------------------------- --
    PROCEDURE Billing_queue_list( 
                   p_recordset OUT t_refc, 
                   p_period_id IN INTEGER    -- ID отчетного периода счета
               );

    -- --------------------------------------------------------------------- --
    --  коэффициент кол-ва времени действия заказа в течение сесяца    
    FUNCTION Correction_factor(
                p_order_id  IN INTEGER,
                p_month     IN DATE   -- для определения месяца, для которого рассчитываем коэффициент
            ) RETURN NUMBER;  -- retun correction_factor

    
END PK24_CCAD;
/
CREATE OR REPLACE PACKAGE BODY PK24_CCAD
IS

-- --------------------------------------------------------------------- --
--  Формирование позиций счетов по BDR CCAD
-- --------------------------------------------------------------------- --
PROCEDURE Load_BDRs(
               p_period_id IN INTEGER
           ) 
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Load_BDRs';
    v_count    INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- BURST (service_id = 104,133, subservice_id = 40)
    Bdr2Item_BURST( p_period_id );

    -- IP_VOLUME  - ступенчатый по объему (service_id = 104, subservice_id = 39)
    Bdr2Item_IP_VOLUME( p_period_id );
    
    -- RT - раздельная тарификация (service_id = 104, subservice_id = 31)
    Bdr2Item_IP_RT( p_period_id );
    
    -- VPN (service_id = 106, subservice_id = 39)
    Bdr2Item_VPN( p_period_id );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

/*
Описания полей BDR_CCAD_BURST_T:
PAID_SPEED 		    – предоплаченная скорость
PAID_SPEED_UNIT 	– единицы измерения предоплаченной скорости
EXCESS_SPEED 		  – превышение скорости 
EXCESS_SPEED_UNIT - единицы измерения превышения скорости
PRICE 			      – стоимость 1 единицы измерения превышения скорости
CF 			          – correction factor
AMOUNT 		        – сумма доплаты (EXCESS_SPEED* PRICE * CF)
INFO_WHEN 		    – когда произошло событие превышения скорости
INFO_ROUTER_IP 	  – на каком маршрутизаторе произошло событие превышения скорости
INFO_DIRECTION 	  – на каком направлении 0-входящий на клиента 1-исходящий от клиента
CURRENCY_ID 		  – валюта тарифа
TAX_INCL 		      – 'Y’ = налог включен
*/
-- -------------------------------------------------------------------- --
-- IP_BURST (service_id = 104, subservice_id = 40)
-- -------------------------------------------------------------------- --
PROCEDURE Bdr2Item_BURST(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER DEFAULT NULL
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Bdr2Item_BURST';
    v_count    INTEGER := 0;
    v_bill_id  INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- Создаем ежемесячные счета, если необходимо
    FOR rb IN (
      SELECT DISTINCT CB.REP_PERIOD_ID, O.ACCOUNT_ID 
        FROM BDR_CCAD_BURST_T CB, ORDER_T O
       WHERE CB.ORDER_ID      = O.ORDER_ID
         AND CB.REP_PERIOD_ID = p_period_id
         AND CB.BDR_STATUS_ID = c_BERR_OK
         AND CB.ITEM_ID IS NULL
         AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
         AND NOT EXISTS (
              SELECT * FROM BILL_T B
               WHERE B.REP_PERIOD_ID = CB.REP_PERIOD_ID
                 AND B.ACCOUNT_ID    = O.ACCOUNT_ID
                 AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_REC -- 'B'
         )
    )LOOP
         v_bill_id := PK07_BILL.Next_recuring_bill (
               p_account_id    => rb.account_id,   -- ID лицевого счета
               p_rep_period_id => rb.rep_period_id -- ID расчетного периода YYYYMM
           );
         v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- Формируем item-ы
    INSERT INTO ITEM_T I (
        I.REP_PERIOD_ID,
        I.BILL_ID,
        I.ITEM_ID,
        I.ITEM_TYPE,
        I.INV_ITEM_ID,
        I.ORDER_ID, 
        I.SERVICE_ID,
        I.ORDER_BODY_ID,
        I.SUBSERVICE_ID,
        I.CHARGE_TYPE,
        I.ITEM_TOTAL,
        I.DATE_FROM,
        I.DATE_TO,
        I.ITEM_STATUS,
        I.TAX_INCL,
        I.CREATE_DATE
    )
    WITH BDR AS (
        SELECT CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_BURST_T CB
         WHERE CB.REP_PERIOD_ID = p_period_id
           AND CB.BDR_STATUS_ID = c_BERR_OK
           --AND CB.SERVICE_ID    = Pk00_Const.c_SERVICE_IP_ACCESS
           --AND CB.SUBSERVICE_ID = Pk00_Const.c_SUBSRV_BURST
           AND CB.ITEM_ID IS NULL
           AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        B.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,
        CASE
          WHEN OB.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
          ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE
      FROM BDR, ORDER_T O, ORDER_BODY_T OB, BILL_T B
     WHERE BDR.ORDER_ID      = O.ORDER_ID
       AND BDR.ORDER_BODY_ID = OB.ORDER_BODY_ID
       AND BDR.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND O.ACCOUNT_ID      = B.ACCOUNT_ID
       AND B.BILL_TYPE       = PK00_CONST.c_BILL_TYPE_REC   -- 'B'
       AND B.BILL_STATUS     = PK00_CONST.c_BILL_STATE_OPEN -- 'OPEN'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- проставляем указатели на item-ы
    MERGE INTO BDR_CCAD_BURST_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.BILL_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_BURST_T CB
           WHERE I.REP_PERIOD_ID  = p_period_id
             AND I.CHARGE_TYPE    = Pk00_Const.c_CHARGE_TYPE_USG
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
             AND CB.BILL_ID IS NULL
             AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID, CB.BILL_ID = I.BILL_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- IP_BURST - удалить начисления за указанный период
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Del_Items_BURST(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Del_Items_IP_BURST';
    v_count    INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id
                          ||', order_id = '||p_order_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем ITEM-ы 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.ORDER_ID      = p_order_id
       AND EXISTS (
           SELECT * FROM BDR_CCAD_BURST_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.ITEM_ID  = I.ITEM_ID
              AND CB.ORDER_ID = I.ORDER_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем ссылки на ITEM-ы в BDR
    UPDATE BDR_CCAD_BURST_T CB SET CB.ITEM_ID = NULL
    WHERE CB.REP_PERIOD_ID = p_period_id
      AND CB.ORDER_ID      = p_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. BDR_CCAD_BURST_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
-- IP_VOLUME  - ступенчатый по объему (service_id = 104, subservice_id = 39)
-- --------------------------------------------------------------------- --
PROCEDURE Bdr2Item_IP_VOLUME(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER DEFAULT NULL
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Bdr2Item_IP_VOLUME';
    v_count    INTEGER := 0;
    v_bill_id  INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- Создаем ежемесячные счета, если необходимо
    FOR rb IN (
      SELECT DISTINCT CB.REP_PERIOD_ID, O.ACCOUNT_ID 
        FROM BDR_CCAD_VOL_T CB, ORDER_T O
       WHERE CB.ORDER_ID      = O.ORDER_ID
         AND CB.REP_PERIOD_ID = p_period_id
         AND CB.BDR_STATUS_ID = c_BERR_OK
         AND CB.ITEM_ID IS NULL
         AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
         AND NOT EXISTS (
              SELECT * FROM BILL_T B
               WHERE B.REP_PERIOD_ID = CB.REP_PERIOD_ID
                 AND B.ACCOUNT_ID    = O.ACCOUNT_ID
                 AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_REC -- 'B'
         )
    )LOOP
         v_bill_id := PK07_BILL.Next_recuring_bill (
               p_account_id    => rb.account_id,   -- ID лицевого счета
               p_rep_period_id => rb.rep_period_id -- ID расчетного периода YYYYMM
           );
         v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- Формируем item-ы
    INSERT INTO ITEM_T I (
        I.REP_PERIOD_ID,
        I.BILL_ID,
        I.ITEM_ID,
        I.ITEM_TYPE,
        I.INV_ITEM_ID,
        I.ORDER_ID, 
        I.SERVICE_ID,
        I.ORDER_BODY_ID,
        I.SUBSERVICE_ID,
        I.CHARGE_TYPE,
        I.ITEM_TOTAL,
        I.DATE_FROM,
        I.DATE_TO,
        I.ITEM_STATUS,
        I.TAX_INCL,
        I.CREATE_DATE,
        I.EXTERNAL_ID
    )
    WITH BDR AS (
        SELECT CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_VOL_T CB
         WHERE CB.REP_PERIOD_ID = p_period_id
           AND (CB.BDR_STATUS_ID = c_BERR_OK OR CB.BDR_STATUS_ID IS NULL)
           --AND CB.SERVICE_ID    = Pk00_Const.c_SERVICE_IP_ACCESS
           --AND CB.SUBSERVICE_ID = Pk00_Const.c_SUBSRV_VOLUME
           AND CB.ITEM_ID IS NULL
           AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        B.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_REC CHARGE_REC,  -- абонплата, зависящая от трафика
        CASE
           WHEN OB.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
           ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE,
        Pk00_Const.c_RATESYS_CCAD_ID    -- тарификатор КСАД 
      FROM BDR, ORDER_T O, ORDER_BODY_T OB, BILL_T B
     WHERE BDR.ORDER_ID      = O.ORDER_ID
       AND BDR.ORDER_BODY_ID = OB.ORDER_BODY_ID
       AND BDR.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND O.ACCOUNT_ID      = B.ACCOUNT_ID
       AND B.BILL_TYPE       = PK00_CONST.c_BILL_TYPE_REC   -- 'B'
       AND B.BILL_STATUS     = PK00_CONST.c_BILL_STATE_OPEN -- 'OPEN'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- проставляем указатели на item-ы
    MERGE INTO BDR_CCAD_VOL_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.BILL_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_VOL_T CB
           WHERE I.REP_PERIOD_ID  = p_period_id
             AND I.CHARGE_TYPE  IN (Pk00_Const.c_CHARGE_TYPE_USG, Pk00_Const.c_CHARGE_TYPE_REC)
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
             AND CB.BILL_ID IS NULL
             AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID, CB.BILL_ID = I.BILL_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
/*
Описания полей BDR_CCAD_VOL_T:
CF 			        – correction factor
BYTES			      - объем оплачиваемого трафика в байтах
VOLUME			    - объем оплачиваемого трафика в VOLUME_UNIT_ID (в единицах тарифа)
VOLUME_UNIT_ID	- единицы измерения трафика VOLUME
PRICE			      - цена за 1 VOLUME_UNIT_ID
AMOUNT			    - сумма к оплате = bdr_rec.VOLUME * bdr_rec.price
[STEP_MIN_BYTES - STEP_MAX_BYTES] – диапазон объема в тарифе с учетом CF
CURRENCY_ID 		– валюта тарифа
TAX_INCL 		    –‘Y’ = налог включен
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- IP_VOLUME - удалить начисления за указанный период
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Del_Items_IP_VOLUME(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Del_Items_IP_VOLUME';
    v_count    INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id
                          ||', order_id = '||p_order_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем ITEM-ы 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.ORDER_ID      = p_order_id
       AND EXISTS (
           SELECT * FROM BDR_CCAD_VOL_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.ITEM_ID  = I.ITEM_ID
              AND CB.ORDER_ID = I.ORDER_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем ссылки на ITEM-ы в BDR
    UPDATE BDR_CCAD_VOL_T CB SET CB.ITEM_ID = NULL
    WHERE CB.REP_PERIOD_ID = p_period_id
      AND CB.ORDER_ID      = p_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. BDR_CCAD_VOL_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
-- RT - раздельная тарификация (service_id = 104, subservice_id = 31)
-- --------------------------------------------------------------------- --
PROCEDURE Bdr2Item_IP_RT(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER DEFAULT NULL
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Bdr2Item_IP_RT';
    v_count    INTEGER := 0;
    v_bill_id  INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- Создаем ежемесячные счета
    FOR rb IN (
      SELECT DISTINCT CB.REP_PERIOD_ID, O.ACCOUNT_ID 
        FROM BDR_CCAD_RT_T CB, ORDER_T O
       WHERE CB.ORDER_ID      = O.ORDER_ID
         AND CB.REP_PERIOD_ID = p_period_id
         AND CB.BDR_STATUS_ID = c_BERR_OK
         AND CB.ITEM_ID IS NULL
         AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
         AND NOT EXISTS (
              SELECT * FROM BILL_T B
               WHERE B.REP_PERIOD_ID = CB.REP_PERIOD_ID
                 AND B.ACCOUNT_ID    = O.ACCOUNT_ID
                 AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_REC -- 'B'
         )
    )LOOP
         v_bill_id := PK07_BILL.Next_recuring_bill (
               p_account_id    => rb.account_id,   -- ID лицевого счета
               p_rep_period_id => rb.rep_period_id -- ID расчетного периода YYYYMM
           );
         v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- Формируем позиции счетов ITEM-s
    INSERT INTO ITEM_T I (
        I.REP_PERIOD_ID,
        I.BILL_ID,
        I.ITEM_ID,
        I.ITEM_TYPE,
        I.INV_ITEM_ID,
        I.ORDER_ID, 
        I.SERVICE_ID,
        I.ORDER_BODY_ID,
        I.SUBSERVICE_ID,
        I.CHARGE_TYPE,
        I.ITEM_TOTAL,
        I.DATE_FROM,
        I.DATE_TO,
        I.ITEM_STATUS,
        I.TAX_INCL,
        I.CREATE_DATE
    )
    WITH BDR AS (
        SELECT CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_RT_T CB
         WHERE CB.REP_PERIOD_ID = p_period_id
           AND CB.BDR_STATUS_ID = c_BERR_OK
           --AND CB.SERVICE_ID    = 149 --Pk00_Const.c_SERVICE_IP_ACCESS_IC
           --AND CB.SUBSERVICE_ID = Pk00_Const.c_SUBSRV_IRT
           AND CB.ITEM_ID IS NULL
           AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        B.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,
        CASE
           WHEN OB.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
           ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE
      FROM BDR, ORDER_T O, ORDER_BODY_T OB, BILL_T B
     WHERE BDR.ORDER_ID      = O.ORDER_ID
       AND BDR.ORDER_BODY_ID = OB.ORDER_BODY_ID
       AND BDR.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND O.ACCOUNT_ID      = B.ACCOUNT_ID
       AND B.BILL_TYPE       = PK00_CONST.c_BILL_TYPE_REC   -- 'B'
       AND B.BILL_STATUS     = PK00_CONST.c_BILL_STATE_OPEN -- 'OPEN'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- проставляем указатели на item-ы
    MERGE INTO BDR_CCAD_RT_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.BILL_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_RT_T CB
           WHERE I.REP_PERIOD_ID  = p_period_id
             AND I.CHARGE_TYPE    = Pk00_Const.c_CHARGE_TYPE_USG
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
             AND CB.BILL_ID IS NULL
             AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID, CB.BILL_ID = I.BILL_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
/*
Описания полей BDR_CCAD_RT_T:
GR_1_BYTES,  		-  объем локального трафика (в байтах)
GR_2_BYTES, 		- объем глобального трафика (в байтах)
GR_31_BYTES, 	  - объем трафика контент-ресурсов группы 1
GR_32_BYTES,    - объем трафика контент-ресурсов группы 2
GR_33_BYTES, 		- объем трафика контент-ресурсов группы 3
GR_34_BYTES, 		- объем трафика контент-ресурсов группы 4
GR_1_PRICE, 		- цена для локального
GR_2_PRICE, 		- цена для глобального
GR_31_PRICE, 		- цена для контент-ресурсов группы 1
GR_32_PRICE, 		- цена для контент-ресурсов группы 2
GR_33_PRICE, 		- цена для контент-ресурсов группы 3
GR_34_PRICE, 		- цена для контент-ресурсов группы 4

SPEED			      - скорость (рассчитанная или фиксированная в тарифе)
SPEED_UNIT		  - единица измерения скорости
AMOUNT			    - сумма к оплате.
CF			        - correction factor
CURRENCY_ID 		– валюта тарифа
TAX_INCL 		    – ‘Y’ = налог включен
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- RT - удалить начисления за указанный период
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Del_Items_IP_RT(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Del_Items_IP_RT';
    v_count    INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id
                          ||', order_id = '||p_order_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем ITEM-ы 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.ORDER_ID      = p_order_id
       AND EXISTS (
           SELECT * FROM BDR_CCAD_RT_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.ITEM_ID  = I.ITEM_ID
              AND CB.ORDER_ID = I.ORDER_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем ссылки на ITEM-ы в BDR
    UPDATE BDR_CCAD_VOL_T CB SET CB.ITEM_ID = NULL
    WHERE CB.REP_PERIOD_ID = p_period_id
      AND CB.ORDER_ID      = p_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. BDR_CCAD_VOL_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
  
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
-- VPN (service_id = 106, subservice_id = 39)
-- --------------------------------------------------------------------- --
PROCEDURE Bdr2Item_VPN(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER DEFAULT NULL
           ) 
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Bdr2Item_VPN';
    v_count    INTEGER := 0;
    v_bill_id  INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- Создаем ежемесячные счета, если необходимо
    FOR rb IN (
      SELECT DISTINCT CB.REP_PERIOD_ID, O.ACCOUNT_ID 
        FROM BDR_CCAD_VPN_T CB, ORDER_T O
       WHERE CB.ORDER_ID      = O.ORDER_ID
         AND CB.REP_PERIOD_ID = p_period_id
         AND CB.BDR_STATUS_ID = c_BERR_OK
         AND CB.ITEM_ID IS NULL
         AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
         AND NOT EXISTS (
              SELECT * FROM BILL_T B
               WHERE B.REP_PERIOD_ID = CB.REP_PERIOD_ID
                 AND B.ACCOUNT_ID    = O.ACCOUNT_ID
                 AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_REC -- 'B'
         )
    )LOOP
         v_bill_id := PK07_BILL.Next_recuring_bill (
               p_account_id    => rb.account_id,   -- ID лицевого счета
               p_rep_period_id => rb.rep_period_id -- ID расчетного периода YYYYMM
           );
         v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- Формируем item-ы
    INSERT INTO ITEM_T I (
        I.REP_PERIOD_ID,
        I.BILL_ID,
        I.ITEM_ID,
        I.ITEM_TYPE,
        I.INV_ITEM_ID,
        I.ORDER_ID, 
        I.SERVICE_ID,
        I.ORDER_BODY_ID,
        I.SUBSERVICE_ID,
        I.CHARGE_TYPE,
        I.ITEM_TOTAL,
        I.DATE_FROM,
        I.DATE_TO,
        I.ITEM_STATUS,
        I.TAX_INCL,
        I.CREATE_DATE
    )
    WITH BDR AS (
        SELECT CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_VPN_T CB
         WHERE CB.REP_PERIOD_ID = p_period_id
           AND CB.BDR_STATUS_ID = c_BERR_OK
           --AND CB.SERVICE_ID    = Pk00_Const.c_SERVICE_VPN
           --AND CB.SUBSERVICE_ID = Pk00_Const.c_SUBSRV_VOLUME
           AND CB.ITEM_ID IS NULL
           AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        B.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,
        CASE
           WHEN OB.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
           ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE
      FROM BDR, ORDER_T O, ORDER_BODY_T OB, BILL_T B
     WHERE BDR.ORDER_ID      = O.ORDER_ID
       AND BDR.ORDER_BODY_ID = OB.ORDER_BODY_ID
       AND BDR.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND O.ACCOUNT_ID      = B.ACCOUNT_ID
       AND B.BILL_TYPE       = PK00_CONST.c_BILL_TYPE_REC   -- 'B'
       AND B.BILL_STATUS     = PK00_CONST.c_BILL_STATE_OPEN -- 'OPEN'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 

    -- проставляем указатели на item-ы    
    MERGE INTO BDR_CCAD_VPN_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.BILL_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_VPN_T CB
           WHERE I.REP_PERIOD_ID  = p_period_id
             AND I.CHARGE_TYPE    = Pk00_Const.c_CHARGE_TYPE_USG
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
             AND CB.BILL_ID IS NULL
             AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID, CB.BILL_ID = I.BILL_ID;
    v_count := SQL%ROWCOUNT;

    Pk01_Syslog.Write_msg('Stop. '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
/*
Описания полей BDR_CCAD_VPN_T:
QUALITY_ID	   - qos
ZONE_OUT		   - из какой зоны
ZONE_IN		     - в какую зону

VOLUME			   - объем трафика в единицах VOLUME_UNIT_ID
VOLUME_UNIT_ID - единица измерения объема трафика
PRICE			     - цена 1 VOLUME_UNIT_ID
AMOUNT			   - сумма

BYTES			     - объем в байтах

CF			       - correction factor
CURRENCY_ID 	 – валюта тарифа
TAX_INCL 		   – ‘Y’ = налог включен
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- VPN - удалить начисления за указанный период
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Del_Items_VPN(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Del_Items_VPN';
    v_count    INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id
                          ||', order_id = '||p_order_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем ITEM-ы 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.ORDER_ID      = p_order_id
       AND EXISTS (
           SELECT * FROM BDR_CCAD_VPN_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.ITEM_ID  = I.ITEM_ID
              AND CB.ORDER_ID = I.ORDER_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем ссылки на ITEM-ы в BDR
    UPDATE BDR_CCAD_VOL_T CB SET CB.ITEM_ID = NULL
    WHERE CB.REP_PERIOD_ID = p_period_id
      AND CB.ORDER_ID      = p_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. BDR_CCAD_VOL_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
 
-- --------------------------------------------------------------------- --
--  Данные для заполнения очереди на тарификацию счетов
-- --------------------------------------------------------------------- --
PROCEDURE Billing_queue_list( 
               p_recordset OUT t_refc, 
               p_period_id IN INTEGER    -- ID отчетного периода счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing_queue_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        WITH BC AS (
            SELECT B.BILL_ID, B.REP_PERIOD_ID, B.BILL_NO, B.PROFILE_ID, 
                   A.ACCOUNT_ID, A.ACCOUNT_NO, A.BILLING_ID, O.ORDER_ID, O.ORDER_NO, O.SERVICE_ID, S.SERVICE_CODE,  
                   OB.ORDER_BODY_ID, OB.SUBSERVICE_ID, OB.CHARGE_TYPE, OB.RATE_RULE_ID, SS.SUBSERVICE, SS.SUBSERVICE_KEY  
              FROM ACCOUNT_T A, ORDER_T O, ORDER_BODY_T OB, SERVICE_T S, SUBSERVICE_T SS, BILL_T B
             WHERE A.ACCOUNT_ID = O.ACCOUNT_ID
               AND O.ORDER_ID   = OB.ORDER_ID
               AND O.SERVICE_ID = S.SERVICE_ID
               AND O.SERVICE_ID IN (101,103, 104, 106, 108, 133, 149)
               AND SS.SUBSERVICE_ID = OB.SUBSERVICE_ID
               AND OB.RATE_RULE_ID IN (
                   2406,    -- IP, IP AccessIC - абонплата, зависящая от направления и объема трафика
                   2407,    -- по объему трафика 
                   2408,    -- IP VPN Тарификация по объему
                   2409,    -- IP, IP AccessIC - Тарификация по полосе BURST
                   2410,    -- Раздельная тарификация
                   2411     -- EPL BURST Тарификация по полосе 
               ) 
               AND A.ACCOUNT_ID = B.ACCOUNT_ID(+)
               AND B.REP_PERIOD_ID(+) = p_period_id
            ORDER BY A.ACCOUNT_NO, O.ORDER_NO, OB.CHARGE_TYPE
        )
        SELECT DISTINCT BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, 
               TO_NUMBER(NULL) TASK_ID, 
               REP_PERIOD_ID, REP_PERIOD_ID DATA_PERIOD_ID 
          FROM BC;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- --------------------------------------------------------------------- --
--  коэффициент кол-ва времени действия заказа в течение сесяца
-- --------------------------------------------------------------------- --
FUNCTION Correction_factor(
            p_order_id  IN INTEGER,
            p_month     IN DATE   -- для определения месяца, для которого рассчитываем коэффициент
        ) RETURN NUMBER  -- retun correction_factor
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Correction_factor';
    v_start_d   DATE := trunc(p_month);
    v_end_d     DATE := trunc(LAST_DAY(p_month))+1-1/(24*60*60);
    v_days_lock NUMBER; -- дней блокировки в отчетном периоде

    v_date_from     date; -- начало действия заказа в отчетном периоде
    v_date_to       date; -- окончание действия заказа в отчетном периоде
    
    v_m             number; -- кол-во дней в месяце
    v_p             number; -- кол-во дней в периоде p_dt_from-p_dt_to

    v_cf            number; -- correction factor
BEGIN
    select GREATEST(trunc(DATE_FROM),v_start_d), LEAST(trunc(DATE_TO)+1-1/(24*60*60),v_end_d)
    into v_date_from,v_date_to
    from ORDER_T
    where ORDER_ID = p_order_id;
    
    -- считаем время блокировок 
    select sum(least(trunc(ol.DATE_TO), v_date_to)-greatest((trunc( ol.DATE_FROM )+1),v_date_from)) -- days_lock  
    into v_days_lock
    from ORDER_LOCK_T ol
    where 
        ORDER_ID = p_order_id
        and
        least(trunc(ol.DATE_TO), v_date_to)-greatest((trunc( ol.DATE_FROM )+1),v_date_from)>0
        and
        (
          ol.DATE_FROM between  v_start_d and  v_end_d
          or 
          ol.DATE_TO between  v_start_d and  v_end_d
          or 
          v_start_d between ol.DATE_FROM and ol.DATE_TO
          or 
          v_end_d between ol.DATE_FROM and ol.DATE_TO
        );

     if v_days_lock is null then
        v_days_lock := 0;
     end if;
    
      v_m := add_months(trunc(p_month,'mm'),1)-trunc(p_month,'mm');
      v_p := trunc(v_date_to)-trunc(v_date_from)+1 - v_days_lock;

      if (v_p>0) then
        v_cf := v_p/v_m;
        return v_cf;
      else
        return 0;
      end if; 

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

END PK24_CCAD;
/
