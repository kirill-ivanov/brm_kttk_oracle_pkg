CREATE OR REPLACE PACKAGE PK501_IP_SERVICE
AS
    --
    -- Пакет для записи в ITEM-ы информации об IP-услугах (КСАД)
    --
    c_PkgName         constant varchar2(30) := 'PK501_IP_SERVICE';

    c_BDR_Source_ID   constant number := 6801;
    c_Service_ID      constant number := 149;
    c_SubService_ID   constant number := 31;

    -- формирование счетов из IP_BDR_RT - 
    PROCEDURE Export_bdr_ss31_to_bill(p_dt in date);
    
    --==================================================================================--
    -- удалить позицию счета, для которой не сформирована позиция инвойса, 
    -- т.е. она не вошла в закрытый счет
    --   - при ошибке выставляет исключение
    -- (когда все устоится добавлю ограничения на удаление в триггер)
    PROCEDURE Delete_item (
                   p_bill_id       IN INTEGER,   -- ID счета
                   p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
                   p_item_id       IN INTEGER    -- ID позиции счета
              );
        

END PK501_IP_SERVICE;
/
CREATE OR REPLACE PACKAGE BODY PK501_IP_SERVICE
AS

-- формирование счетов из IP_BDR_RT - 
PROCEDURE Export_bdr_ss31_to_bill(p_dt IN DATE)
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Export_bdr_ss31_to_bill';
    v_period_from    DATE;
    v_period_to      DATE;
    v_rep_period_id  INTEGER;
    v_bill_id        INTEGER; 
    v_item_id        INTEGER;
    v_account_id     INTEGER; 
    v_currency_id    INTEGER;
    v_ac_currency_id INTEGER;
    v_tax_incl       RATEPLAN_T.TAX_INCL%TYPE;
    --
BEGIN
    -- определяем период к которому принаждлежит запись, варианты: BILL,OPEN,NEXT
    SELECT PERIOD_ID, PERIOD_FROM, PERIOD_TO
      INTO v_rep_period_id, v_period_from, v_period_to 
      FROM ( 
            SELECT *
              FROM PERIOD_T P
             WHERE p_dt <= P.PERIOD_TO
               AND P.CLOSE_REP_PERIOD IS NULL
             ORDER BY P.PERIOD_FROM ASC
           )
    WHERE ROWNUM = 1;

    -- создаем ITEM-ы для BDR, один BDR -> один ITEM
    FOR bdr IN (
        SELECT
            REP_PERIOD, START_TIME, ITEM_ID, RATEPLAN_ID, ORDER_NO, ORDER_ID, 
                GR_1_BYTES, GR_2_BYTES, GR_31_BYTES, GR_32_BYTES, GR_33_BYTES, GR_34_BYTES, 
                GR_1_PRICE, GR_2_PRICE, GR_31_PRICE, GR_32_PRICE, GR_33_PRICE, GR_34_PRICE, 
                SPEED_MBITSEC, 
                SUMMA,
                DATE_FROM, DATE_TO, 
                ROWID rid 
        FROM IP_BDR_RT
        WHERE REP_PERIOD BETWEEN  v_period_from AND v_period_to
          AND ITEM_ID IS NULL
        FOR UPDATE of ITEM_ID
    ) LOOP
            
        BEGIN
            --  зачитываем полезную информацию и проверяем наличеи тарифного плана в системе
            SELECT A.ACCOUNT_ID, A.CURRENCY_ID, R.TAX_INCL
              INTO v_account_id, v_ac_currency_id, v_tax_incl
              FROM ORDER_T O, ACCOUNT_T A, RATEPLAN_T R
             WHERE O.ACCOUNT_ID = A.ACCOUNT_ID
               AND O.RATEPLAN_ID = R.RATEPLAN_ID
               AND O.ORDER_ID = bdr.ORDER_ID
               AND R.RATEPLAN_ID = bdr.RATEPLAN_ID;
 
            -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
            -- уточняеми валюту счета и проверяем наличие описателя счетов. если нет, то создаем 
            -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
            BEGIN
                SELECT CURRENCY_ID INTO v_currency_id 
                  FROM BILLINFO_T
                 WHERE ACCOUNT_ID = v_account_id;
                 
            EXCEPTION WHEN NO_DATA_FOUND THEN
                -- по умолчанию выравниваем с валютой л/с
                v_currency_id := v_ac_currency_id;
                -- надо создать billinfo_t
                Pk07_Bill.New_billinfo (
                               p_account_id    => v_account_id,   -- ID лицевого счета
                               p_currency_id   => v_currency_id,  -- ID валюты счета
                               p_delivery_id   => NULL,           -- ID способа доставки счета
                               p_days_for_payment => 30
                           );
            END;
            -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
            -- создаем счет 
            -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
            BEGIN
                v_bill_id := PK07_BILL.NEXT_RECURING_BILL(
                    p_account_id    => v_account_id,
                    p_rep_period_id => v_rep_period_id
                );
                -- создаем ITEM
                v_item_id := PK02_POID.Next_Item_Id;
                INSERT INTO ITEM_T
                (
                    BILL_ID, 
                    REP_PERIOD_ID, 
                    ITEM_ID, 
                    ITEM_TYPE, 
                    INV_ITEM_ID, 
                    ORDER_ID, 
                    SERVICE_ID, 
                    SUBSERVICE_ID, 
                    CHARGE_TYPE, 
                    ITEM_TOTAL, 
                    RECVD, 
                    DATE_FROM, 
                    DATE_TO, 
                    ITEM_STATUS, 
                    CREATE_DATE, 
                    LAST_MODIFIED, 
                    REP_GROSS, 
                    REP_TAX, 
                    TAX_INCL, 
                    EXTERNAL_ID, 
                    NOTES
                )
                VALUES
                (
                    v_bill_id,                      --BILL_ID, 
                    v_rep_period_id,                --REP_PERIOD_ID, 
                    v_item_id,                      --ITEM_ID, 
                    pk00_const.c_ITEM_TYPE_BILL,    --ITEM_TYPE, 
                    null,                           --INV_ITEM_ID, 
                    bdr.ORDER_ID,                   --ORDER_ID, 
                    c_Service_ID,                   --SERVICE_ID, 
                    c_SubService_ID,                --SUBSERVICE_ID, 
                    pk00_const.c_CHARGE_TYPE_USG,   --CHARGE_TYPE, 
                    bdr.summa,                      --ITEM_TOTAL, – сумма что начислено 
                    0,                              --RECVD, 
                    bdr.DATE_FROM,                  --DATE_FROM, 
                    bdr.DATE_TO,                    --DATE_TO, 
                    pk00_const.c_ITEM_STATE_OPEN,   --ITEM_STATUS, 
                    SYSDATE,                        --CREATE_DATE, 
                    SYSDATE,                        --LAST_MODIFIED, 
                    0,                              --REP_GROSS, 
                    0,                              --REP_TAX, 
                    v_tax_incl,                     -- TAX_INCL, 
                    c_BDR_Source_ID,                --EXTERNAL_ID, 
                    NULL                            --NOTES
                );
                    
                -- проставляем в BDR координаты ITEM-a куда он вошел                  
                UPDATE IP_BDR_RT
                   SET BILL_ID = v_bill_id,
                       ITEM_ID = v_item_id
                 WHERE 
                    ROWID = bdr.rid
                ; 
                 
            END;
                
        EXCEPTION WHEN OTHERS THEN
            Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        END;        
    END LOOP;       
        
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================--
-- удалить позицию счета, для которой не сформирована позиция инвойса, 
-- т.е. она не вошла в закрытый счет
--   - при ошибке выставляет исключение
-- (когда все устоится добавлю ограничения на удаление в триггер)
PROCEDURE Delete_item (
               p_bill_id       IN INTEGER,   -- ID счета
               p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
               p_item_id       IN INTEGER    -- ID позиции счета
          ) 
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Delete_item';
BEGIN
    --
    PK08_ITEM.Delete_item(p_bill_id, p_rep_period_id, p_item_id);
    --
EXCEPTION
  WHEN OTHERS THEN
      Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

END PK501_IP_SERVICE;
/
