CREATE OR REPLACE PACKAGE PK102_RECEIPT_FOR_PAYMENT
IS
    --
    -- Пакет для печати КВИТАНЦИЙ на оплату для ФИЗИЧЕСКИХ ЛИЦ
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK102_RECEIPT_FOR_PAYMENT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- Общая информация счет + данные продавца + данные клиента:
    --   - при ошибке выставляет исключение
    PROCEDURE Document_header( 
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,      -- ID лицевого счета            
                   p_period_id  IN INTEGER       -- ID отчетного периода
               );

    -- получить данные для печати: "Итоги за период"
    --   - при ошибке выставляет исключение
    PROCEDURE Total_items( 
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,      -- ID лицевого счета
                   p_period_id  IN INTEGER       -- ID отчетного периода
               );
    
    -- получить данные для печати детализации: "Начисления, руб"
    --   - при ошибке выставляет исключение
    PROCEDURE Invoice_items( 
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,      -- ID лицевого счета
                   p_period_id  IN INTEGER       -- ID отчетного периода
               );
    
    -- получить данные для печати краткой детализации трафика
    --   - при ошибке выставляет исключение
    PROCEDURE Detail( 
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,      -- ID счета
                   p_period_id  IN INTEGER       -- ID отчетного периода
               );
    
    
END PK102_RECEIPT_FOR_PAYMENT;
/
CREATE OR REPLACE PACKAGE BODY PK102_RECEIPT_FOR_PAYMENT
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Общая информация счет + данные продавца + данные клиента:
--   - при ошибке выставляет исключение
PROCEDURE Document_header( 
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,    -- ID лицевого счета            
               p_period_id  IN INTEGER     -- ID отчетного периода
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Document_header';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        -- Общая информация счет + данные продавца + данные клиента:
         SELECT 
               AP.ACCOUNT_ID,                -- ID л/с клиента
               A.ACCOUNT_NO,                 -- № лицевого счета
               AP.CONTRACTOR_ID,             -- ID продавца
               CR.SHORT_NAME COMPANY_NAME,   -- Компания продавец
               CR.INN COMPANY_INN,           -- ИНН продавца
               CB.BANK_NAME,                 -- Банк
               CB.BANK_SETTLEMENT,           -- № Р/с 
               CB.BANK_CORR_ACCOUNT,         -- № К/с
               CB.BANK_CODE,                 -- БИК
               AC.PERSON,                    -- Ф.И.О.
               AC.ZIP||','||AC.STATE||','||AC.CITY||','||AC.ADDRESS  DLV_ADDR, -- Адрес доставки
               AC.PHONES CLIENT_PHONE,
               CRA.PHONE_BILLING,
               CRA.PHONE_ACCOUNT,              
               P.PERIOD_FROM, 
               P.PERIOD_TO,               
               A.CURRENCY_ID                 -- ID валюты счета               
          FROM ACCOUNT_T A,
               ACCOUNT_PROFILE_T AP,
               CONTRACTOR_T CR,
               CONTRACTOR_BANK_T CB,
               CONTRACTOR_ADDRESS_T CRA,
               ACCOUNT_CONTACT_T AC,
               PERIOD_T P
         WHERE 1=1--P.POSITION = 'BILL'
           AND P.PERIOD_ID = p_period_id
           AND A.ACCOUNT_ID = p_account_id
           AND A.ACCOUNT_TYPE = 'P'
           AND AP.ACCOUNT_ID = A.ACCOUNT_ID
           AND AC.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AC.DATE_FROM <= P.PERIOD_TO
           AND (AC.DATE_TO IS NULL OR P.PERIOD_FROM < AC.DATE_TO )
           AND AC.ADDRESS_TYPE = 'DLV'
           AND AP.DATE_FROM <= P.PERIOD_TO 
           AND (AP.DATE_TO IS NULL OR P.PERIOD_FROM < AP.DATE_TO )
           AND CR.CONTRACTOR_ID = AP.CONTRACTOR_ID
           AND CRA.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND CB.CONTRACTOR_ID = AP.CONTRACTOR_ID
           AND CB.DATE_FROM <= P.PERIOD_TO 
           AND (CB.DATE_TO IS NULL OR P.PERIOD_FROM < CB.DATE_TO )
         ;    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- получить данные для печати: "Итоги за период"
-- позиции счета и ивойса сформированы, платежи разнесены, авансы сформированы, 
-- новый биллинговый период открыт
--   - при ошибке выставляет исключение
PROCEDURE Total_items( 
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,    -- ID лицевого счета
               p_period_id  IN INTEGER     -- ID отчетного периода
           )
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Total_items';
    v_balance         NUMBER; 
    v_open_balance    NUMBER;  -- входящий баланс на начало периода выставления счета
    v_open_due        NUMBER;  -- задолженность за предыдущий период
    v_bill_total      NUMBER;  -- начислено за текущий период
    v_recvd           NUMBER;  -- принято платежей за период счета
    v_last_period_id  INTEGER;
    v_retcode         INTEGER;
BEGIN
    -- получаем ID предыдущего периода
    v_last_period_id := PK04_PERIOD.Make_prev_id(p_period_id);
    -- получаем входящий баланс периода
    SELECT NVL(SUM(CLOSE_BALANCE),0)
      INTO v_open_balance
      FROM REP_PERIOD_INFO_T  
     WHERE REP_PERIOD_ID = v_last_period_id
       AND ACCOUNT_ID = p_account_id;
    -- получаем сумму начислений по счетам за период (кроме основного могут быть корректировки)
    SELECT NVL(SUM(B.TOTAL),0)
      INTO v_bill_total
      FROM BILL_T B
     WHERE B.ACCOUNT_ID = p_account_id
       AND B.REP_PERIOD_ID = p_period_id;
    -- сумма платежей в период, за который выставлен счет 
    SELECT NVL(SUM(P.RECVD),0)
      INTO v_recvd 
      FROM PAYMENT_T P
     WHERE P.ACCOUNT_ID = p_account_id
       AND P.REP_PERIOD_ID = p_period_id;
    -- к оплате
    v_balance := v_open_balance + v_bill_total - v_recvd;
    v_open_due := v_open_balance - v_recvd;
    -- возвращаем курсор
    OPEN p_recordset FOR
         SELECT v_open_balance,      -- долг (входящий баланс от предыдущего периода)
                v_recvd,        -- принято платежей за период
                v_open_due,     -- задолженность за предыдущий период
                v_bill_total,   -- начисления по счетам за период
                v_balance       -- к оплате (v_opening_balance + v_bill_total - recvd)
           FROM DUAL;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- получить данные для печати детализации: "Начисления, руб"
--   - при ошибке выставляет исключение
PROCEDURE Invoice_items( 
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID лицевого счета
               p_period_id  IN INTEGER    -- ID отчетного периода
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Invoice_items';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT S.SERVICE SERVICE_NAME, 
               II.GROSS, II.TAX, II.TOTAL   
          FROM BILL_T B, INVOICE_ITEM_T II, SERVICE_T S
         WHERE B.ACCOUNT_ID    = p_account_id
           AND B.REP_PERIOD_ID = p_period_id
           AND B.BILL_STATUS  != PK00_CONST.c_BILL_STATE_EMPTY -- пустые не интересны
           AND II.BILL_ID      = B.BILL_ID
           AND II.REP_PERIOD_ID= B.REP_PERIOD_ID 
           AND II.SERVICE_ID   = S.SERVICE_ID
        ORDER BY INV_ITEM_NO;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- получить данные для печати краткой детализации трафика
--   - при ошибке выставляет исключение
PROCEDURE Detail( 
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,    -- ID лицевого счета
               p_period_id  IN INTEGER     -- ID отчетного периода
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Detail';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор 
    OPEN p_recordset FOR
        SELECT D.ORDER_NO, D.DATE_BEGIN, D.DATE_END, D.PREFIX, D.ZONE, D.CALLS_NUM, D.MINS,
               D.TARIFF, D.GROSS, D.DIRECTION, D.INTERNATIONAL, D.DATE_OF_TARIFF,
               D.CURRENCY_ID, D.MONTH
          FROM DETAIL_MMTS_T D
         WHERE D.BILL_ID IN (
            SELECT B.BILL_ID 
              FROM BILL_T B
             WHERE B.ACCOUNT_ID    = p_account_id
               AND B.REP_PERIOD_ID = p_period_id
               AND B.BILL_STATUS  != PK00_CONST.c_BILL_STATE_EMPTY
        ) 
        ORDER BY D.ITEM_ID, D.LINE_ID;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


END PK102_RECEIPT_FOR_PAYMENT;
/
