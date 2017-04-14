CREATE OR REPLACE PACKAGE PK05_ACCOUNT_BALANCE
IS
    --
    -- Пакет для работы с объектом "ЛИЦЕВОЙ СЧЕТ", таблицы:
    -- account_t, account_profile_t, billinfo_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK05_ACCOUNT_BALANCE';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- 1) Просмотр всех составляющих баланса
    PROCEDURE View_balance ( 
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER
               );

    -- 2) получить баланс лицевого счета
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- В баланс входят: сумма всех выставленных счетов - сумма всех принятых платежей
    --   - положительное/отрицательное - баланс 
    --   - при ошибке выставляем исключение
    FUNCTION Get_balance(
                   p_account_id     IN INTEGER
               ) RETURN NUMBER;

    -- 3) Получить баланс л/с на текущий момент 
    FUNCTION Get_current_balance(p_account_id IN INTEGER) RETURN NUMBER;

    -- 4) баланс для разблокировки, баланс по счетам для которых не подошел срок оплаты  
    FUNCTION Get_balance_for_unlock(p_account_id IN INTEGER) RETURN NUMBER;

    -- 5) Пересчитать баланс по всем выставленным счетам и оплатам 
    --   - при ошибке выставляем исключение
    FUNCTION Refresh_balance ( 
                   p_account_id IN INTEGER   -- ID позиции счета
               ) RETURN NUMBER;

    -- 6) установить входящий баланс лицевого счета
    PROCEDURE Set_incomming_balance(
                   p_account_id     IN INTEGER,
                   p_balance        IN NUMBER,
                   p_balance_date   IN DATE
               );

    -- 7) удалить входящий баланс лицевого счета
    PROCEDURE Delete_incomming_balance(
                   p_account_id     IN INTEGER
               );
               
    -- ------------------------------------------------------------------------- --
    -- История формирования баланса л/с (основа для акта сверки)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Account_balance_history(
                   p_recordset    OUT t_refc,
                   p_account_id   IN INTEGER 
               );
           
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- История формирования баланса по договору (основа для акта сверки)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Contract_balance_history (
                   p_recordset    OUT t_refc,
                   p_contract_id   IN INTEGER 
               );
    
    -- ------------------------------------------------------------------------- --
    -- Обороты по л/с за указанный период
    -- ------------------------------------------------------------------------- --
    PROCEDURE Account_period_info (
                   p_recordset    OUT t_refc,
                   p_period_id    IN  NUMBER,
                   p_account_id   IN  INTEGER
               );
    
END PK05_ACCOUNT_BALANCE;
/
CREATE OR REPLACE PACKAGE BODY PK05_ACCOUNT_BALANCE
IS

-- ----------------------------------------------------------------------------
-- 1) Просмотр всех составляющих баланса
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE View_balance ( 
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'View_balance';
    v_retcode    INTEGER;
BEGIN
    -- построить курсор
    OPEN p_recordset FOR
    WITH CUR AS (
        SELECT B.ACCOUNT_ID,
               SUM (
                   CASE
                     WHEN B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN 
                       AND I.TAX_INCL = Pk00_Const.c_RATEPLAN_TAX_INCL 
                       THEN I.ITEM_TOTAL
                     WHEN B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN 
                       AND I.TAX_INCL = Pk00_Const.c_RATEPLAN_TAX_NOT_INCL 
                       THEN I.ITEM_TOTAL * (1+NVL(AP.VAT,0)/100)
                     ELSE 0
                   END 
               ) CUR_TOTAL
          FROM ITEM_T I, BILL_T B, ACCOUNT_PROFILE_T AP, PERIOD_T P
         WHERE B.REP_PERIOD_ID > P.PERIOD_ID
           AND I.BILL_ID = B.BILL_ID
           AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND B.ACCOUNT_ID = AP.ACCOUNT_ID
           AND P.POSITION = Pk00_Const.c_PERIOD_LAST
        GROUP BY B.ACCOUNT_ID
    ), DBT AS (
        SELECT ACCOUNT_ID, DBT_DUE, DECODE(DBT_DUE,0,TRUNC(SYSDATE),DBT_DATE) DBT_DATE
          FROM (
            SELECT B.ACCOUNT_ID, 
                   SUM(B.DUE) DBT_DUE, 
                   MIN(B.DUE_DATE) DBT_DATE 
              FROM BILL_T B
             WHERE B.PAID_TO < SYSDATE
               AND B.DUE != 0 -- именно не равно, иначе не учтутся корректировки в старом стиле
            GROUP BY B.ACCOUNT_ID
        )
    ), PAY AS (
        SELECT P.ACCOUNT_ID, SUM(P.BALANCE) NOT_TRANSFERED 
          FROM PAYMENT_T P
        WHERE  P.BALANCE != 0
        GROUP BY P.ACCOUNT_ID
    )
    SELECT A.ACCOUNT_ID, (A.BALANCE-NVL(CUR.CUR_TOTAL,0)) CUR_BALANCE, 
           A.BALANCE, A.BALANCE_DATE, 
           NVL(DBT.DBT_DUE,0) DBT_DUE, DBT.DBT_DATE,
           FLOOR(TO_NUMBER(SYSDATE-DBT.DBT_DATE)) DBT_DAYS, 
           FLOOR(MONTHS_BETWEEN( SYSDATE, DBT.DBT_DATE )) DBT_MONTHS,
           NVL(PAY.NOT_TRANSFERED, 0) NOT_TRANSFERED 
      FROM ACCOUNT_T A, CUR, DBT, PAY
     WHERE A.ACCOUNT_ID = CUR.ACCOUNT_ID(+)
       AND A.ACCOUNT_ID = DBT.ACCOUNT_ID(+)
       AND A.ACCOUNT_ID = PAY.ACCOUNT_ID(+)
       AND A.ACCOUNT_ID = p_account_id
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ----------------------------------------------------------------------------
-- 2) получить баланс лицевого счета
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- В баланс входят: сумма всех выставленных счетов - сумма всех принятых платежей
--   - положительное/отрицательное - баланс 
--   - при ошибке выставляем исключение
FUNCTION Get_balance(
               p_account_id     IN INTEGER
           ) RETURN NUMBER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_balance';
    v_balance    NUMBER;
BEGIN
    SELECT BALANCE INTO v_balance 
      FROM ACCOUNT_T
     WHERE ACCOUNT_ID = p_account_id;
    RETURN v_balance;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------------------
-- 3) Получить баланс л/с на текущий момент 
-- ----------------------------------------------------------------------------
FUNCTION Get_current_balance(p_account_id IN INTEGER) RETURN NUMBER 
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Get_current_balance';
    v_balance     NUMBER;
BEGIN
    --
    WITH IT AS (
        SELECT B.ACCOUNT_ID,
               SUM (
                   CASE
                     WHEN B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN
                       AND I.TAX_INCL = Pk00_Const.c_RATEPLAN_TAX_INCL 
                       THEN I.ITEM_TOTAL
                     WHEN B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN
                       AND I.TAX_INCL = Pk00_Const.c_RATEPLAN_TAX_NOT_INCL 
                       THEN ROUND(I.ITEM_TOTAL * (1+NVL(B.VAT,0)/100),2)
                     ELSE 0
                   END 
               ) TOTAL
          FROM ITEM_T I, BILL_T B, PERIOD_T P
         WHERE B.REP_PERIOD_ID > P.PERIOD_ID
           AND I.BILL_ID       = B.BILL_ID
           AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND P.POSITION = Pk00_Const.c_PERIOD_LAST
        GROUP BY B.ACCOUNT_ID
    ) 
    SELECT (A.BALANCE - IT.TOTAL) INTO v_balance 
      FROM IT, ACCOUNT_T A
     WHERE IT.ACCOUNT_ID = A.ACCOUNT_ID
       AND A.ACCOUNT_ID  = p_account_id;
    --
    RETURN v_balance;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------------------
--4) баланс для разблокировки, баланс по счетам для которых не подошел срок оплаты  
--
FUNCTION Get_balance_for_unlock(p_account_id IN INTEGER) RETURN NUMBER 
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Get_balance_for_unlock';
    v_balance     NUMBER;
BEGIN
    --
    WITH IB AS (
        SELECT p_account_id ACCOUNT_ID, 
               NVL(BALANCE, 0) BALANCE,
               NVL(BALANCE_DATE,TO_DATE('01.01.2000','dd.mm.yyyy')) BALANCE_DATE, 
               NVL(REP_PERIOD_ID, 200001) REP_PERIOD_ID, 
               BILL_ID, 
               PAYMENT_ID
          FROM (
            SELECT MIN(IB.BALANCE)    BALANCE,
                   MIN(BALANCE_DATE)  BALANCE_DATE, 
                   MIN(REP_PERIOD_ID) REP_PERIOD_ID, 
                   MIN(BILL_ID)       BILL_ID, 
                   MIN(PAYMENT_ID)    PAYMENT_ID
              FROM INCOMING_BALANCE_T IB
             WHERE IB.ACCOUNT_ID    = p_account_id
        )
    ), BL AS (
        SELECT B.ACCOUNT_ID, SUM(B.TOTAL) BILL_TOTAL 
          FROM BILL_T B, IB
         WHERE B.ACCOUNT_ID     = IB.ACCOUNT_ID
           AND B.REP_PERIOD_ID >= IB.REP_PERIOD_ID
           AND B.REP_PERIOD_ID >= TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE,'mm'),-3),'yyyymm') -- rep_period_id
           AND B.PAID_TO       > SYSDATE
        GROUP BY B.ACCOUNT_ID
    )
    SELECT (A.BALANCE - BL.BILL_TOTAL) INTO v_balance 
      FROM BL, ACCOUNT_T A
     WHERE BL.ACCOUNT_ID = A.ACCOUNT_ID;
    --
    RETURN v_balance;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------------------
-- 5) Пересчитать баланс по всем выставленным счетам и оплатам 
--   - при ошибке выставляем исключение
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Refresh_balance ( 
               p_account_id IN INTEGER   -- ID позиции счета
           ) RETURN NUMBER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Refresh_balance';
    v_balance    NUMBER;
BEGIN
    -- вычисляем баланс с учетом входящего остатка
    MERGE INTO ACCOUNT_T A
    USING (
      WITH IB AS (
          SELECT p_account_id ACCOUNT_ID, 
                 NVL(BALANCE, 0) BALANCE,
                 NVL(BALANCE_DATE,TO_DATE('01.01.2000','dd.mm.yyyy')) BALANCE_DATE, 
                 NVL(REP_PERIOD_ID, 200001) REP_PERIOD_ID, 
                 BILL_ID, 
                 PAYMENT_ID
            FROM (
              SELECT MIN(IB.BALANCE)    BALANCE,
                     MIN(BALANCE_DATE)  BALANCE_DATE, 
                     MIN(REP_PERIOD_ID) REP_PERIOD_ID, 
                     MIN(BILL_ID)       BILL_ID, 
                     MIN(PAYMENT_ID)    PAYMENT_ID
                FROM INCOMING_BALANCE_T IB
               WHERE IB.ACCOUNT_ID    = p_account_id
          )
      ), BP AS (
          SELECT ACCOUNT_ID, REP_PERIOD_ID, BP_DATE, BILL_TOTAL, RECVD
            FROM (
              -- получаем полную задолженность по выставленным счетам
              SELECT B.ACCOUNT_ID, 
                     B.REP_PERIOD_ID,
                     B.BILL_DATE BP_DATE,
                     B.TOTAL BILL_TOTAL, 
                     0 RECVD
                FROM BILL_T B, IB
               WHERE B.ACCOUNT_ID     = IB.ACCOUNT_ID
                 AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_READY, 
                                       Pk00_Const.c_BILL_STATE_CLOSED)
                 AND B.REP_PERIOD_ID >= IB.REP_PERIOD_ID
              UNION ALL
              -- получаем сумму поступивших за периоды платежей
              SELECT P.ACCOUNT_ID, 
                     P.REP_PERIOD_ID,
                     P.PAYMENT_DATE BP_DATE,
                     0 BILL_TOTAL,
                     P.RECVD 
                FROM PAYMENT_T P, IB
               WHERE P.ACCOUNT_ID     = IB.ACCOUNT_ID
                 AND P.REP_PERIOD_ID >= IB.REP_PERIOD_ID
          )
      )
      SELECT ACCOUNT_ID, SUM(RECVD) - SUM(BILL_TOTAL) BALANCE, MAX(BP_DATE) BALANCE_DATE
        FROM BP
       GROUP BY ACCOUNT_ID
    ) T
    ON (A.ACCOUNT_ID = T.ACCOUNT_ID)
    WHEN MATCHED THEN UPDATE SET A.BALANCE_DATE = T.BALANCE_DATE, A.BALANCE = T.BALANCE;
    --    
    SELECT BALANCE INTO v_balance
      FROM ACCOUNT_T
     WHERE ACCOUNT_ID = p_account_id;
    --
    RETURN v_balance;
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------------------
-- 6) установить входящий баланс лицевого счета
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_incomming_balance(
               p_account_id     IN INTEGER, --
               p_balance        IN NUMBER,  -- входящий баланс на первое число месяца 00:00:00
               p_balance_date   IN DATE     -- первое число месяца 00:00:00 в который вошли с балансом
           )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'Set_incomming_balance';
    v_paysystem_id       CONSTANT INTEGER := 27; -- платежные система, для входящих балансов
    v_service_id         CONSTANT INTEGER := 0;  -- услуга корректировка
    v_subservice_id      CONSTANT INTEGER := 0;  -- компонета услуги корректировка
    v_account_no         ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_period_id          INTEGER;
    v_payment_id         INTEGER := NULL;
    v_bill_id            INTEGER := NULL;
    v_bill_no            BILL_T.BILL_NO%TYPE;
    v_balance_date       DATE := TRUNC(p_balance_date, 'mm');
    v_balance            NUMBER;
    v_bill_total         NUMBER;
    v_bill_gross         NUMBER;
    v_bill_tax           NUMBER;
    -- - - - - - - - - - - - - -- 
    v_currency_id        INTEGER; 
    v_profile_id         INTEGER; 
    v_contractor_id      INTEGER; 
    v_contractor_bank_id INTEGER; 
    v_contract_id        INTEGER; 
    v_vat                NUMBER;
    v_days_for_payment   INTEGER; 
    v_invoice_rule_id    INTEGER;
    -- - - - - - - - - - - - - --
    v_inv_item_id        INTEGER;
    v_item_id            INTEGER;
    v_order_id           INTEGER; 
    v_order_body_id      INTEGER;
    v_date_from          DATE;
    v_date_to            DATE;
    
BEGIN
    -- период в который вошли с балансом
    v_period_id := Pk04_Period.Period_id( v_balance_date );
    v_date_from := TO_DATE('01.01.2000','dd.mm.yyyy');
    v_date_to   := v_balance_date - 1/86400;
    --
    IF p_balance != 0 THEN
        IF p_balance < 0 THEN
            -- получаем данные 
            SELECT A.ACCOUNT_NO,
                   A.CURRENCY_ID, 
                   AP.PROFILE_ID,
                   AP.CONTRACTOR_ID, 
                   AP.CONTRACTOR_BANK_ID,
                   AP.CONTRACT_ID,
                   AP.VAT,  
                   BI.DAYS_FOR_PAYMENT,
                   BI.INVOICE_RULE_ID
              INTO v_account_no, v_currency_id, v_profile_id, 
                   v_contractor_id, v_contractor_bank_id, v_contract_id, v_vat,
                   v_days_for_payment, v_invoice_rule_id
              FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A, BILLINFO_T BI  
             WHERE AP.DATE_FROM <= v_balance_date
               AND (AP.DATE_TO IS NULL OR v_balance_date < AP.DATE_TO)
               AND AP.ACCOUNT_ID = A.ACCOUNT_ID
               AND AP.ACCOUNT_ID = BI.ACCOUNT_ID
               AND A.ACCOUNT_ID  = p_account_id;  
            -- добавить корректировочный счет с суммой входящего баланса 
            v_bill_id := Pk02_Poid.Next_bill_id;
            -- получаем номер очередного периодического счета
            v_bill_no := Pk07_Bill.Next_rec_bill_no( p_account_id, v_period_id );
            -- преобразкем в номер счета входящего баланса
            v_bill_no := v_bill_no || 'i';
            -- сумма счета
            v_bill_total := -p_balance;
            v_bill_gross := ROUND(v_bill_total /(1 + v_vat / 100), 2);
            v_bill_tax   := v_bill_total - v_bill_gross;
            
            -- создаем счет
            INSERT INTO BILL_T B (
                BILL_ID, REP_PERIOD_ID, ACCOUNT_ID, 
                BILL_NO, BILL_DATE, 
                BILL_TYPE, BILL_STATUS, CURRENCY_ID, 
                TOTAL, GROSS, TAX, RECVD, DUE, DUE_DATE, PAID_TO, 
                CALC_DATE, NOTES, 
                ADJUSTED,  
                VAT, CONTRACT_ID, PROFILE_ID, CONTRACTOR_ID, CONTRACTOR_BANK_ID,
                CREATE_DATE,  
                INVOICE_RULE_ID,
                ACT_DATE_FROM, ACT_DATE_TO
            ) VALUES (
                v_bill_id, v_period_id, p_account_id, 
                v_bill_no, v_balance_date, 
                'I', 'CLOSED', v_currency_id, 
                v_bill_total, v_bill_gross, v_bill_tax, 0, -v_bill_total,
                v_balance_date + v_days_for_payment,
                v_balance_date + v_days_for_payment, 
                SYSDATE, 'л/с '||v_account_no||' входящий баланс', 
                0,  
                v_vat, v_contract_id, v_profile_id, 
                v_contractor_id, v_contractor_bank_id,
                SYSDATE,  
                v_invoice_rule_id,
                v_date_from, v_date_to
            );
            -- создаем стоки счета-фактуры
            v_inv_item_id := Pk02_Poid.Next_invoice_item_id;
            --
            INSERT INTO INVOICE_ITEM_T V(
                BILL_ID, REP_PERIOD_ID, INV_ITEM_ID, INV_ITEM_NO, 
                SERVICE_ID, TOTAL, GROSS, TAX, VAT, 
                INV_ITEM_NAME, DATE_FROM, DATE_TO
            )VALUES(
                v_bill_id, v_period_id, v_inv_item_id, 1,
                0, v_bill_total, v_bill_gross, v_bill_tax, v_vat,
                'Входящий баланс', v_date_from, v_date_to
            );
            -- создаем строки счета
            SELECT ORDER_ID, ORDER_BODY_ID 
              INTO v_order_id, v_order_body_id
              FROM (
                SELECT O.ACCOUNT_ID, O.ORDER_ID, OB.ORDER_BODY_ID,
                       ROW_NUMBER() OVER (PARTITION BY O.ACCOUNT_ID ORDER BY O.DATE_FROM ) RN 
                  FROM ORDER_T O, ORDER_BODY_T OB
                 WHERE O.ORDER_ID = OB.ORDER_ID
                   AND O.ACCOUNT_ID = p_account_id
            ) WHERE RN = 1;
            --
            v_item_id := Pk02_Poid.Next_item_id;
            --
            INSERT INTO ITEM_T(
                BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
                ORDER_ID,  SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
                ITEM_TOTAL, RECVD, DATE_FROM, DATE_TO, ITEM_STATUS, 
                CREATE_DATE, LAST_MODIFIED, REP_GROSS, REP_TAX, TAX_INCL, 
                EXTERNAL_ID, NOTES, 
                ORDER_BODY_ID, DESCR, QUANTITY, 
                ITEM_CURRENCY_ID, BILL_TOTAL, 
                ITEM_CURRENCY_RATE, MODIFIED_BY
            )VALUES(
                v_bill_id, v_period_id, v_item_id, 'A', v_inv_item_id,
                v_order_id, v_service_id, v_subservice_id, 'BAL',
                v_bill_total, 0, v_date_from, v_date_to, 'CLOSED',
                SYSDATE, SYSDATE, v_bill_gross, v_bill_tax, 'N',
                NULL, 'входящий баланс',
                v_order_body_id, 'входящий баланс', NULL,
                v_currency_id, v_bill_total, 1, 'Макеев С.В.'
            );
            --
        ELSE
            -- добавить корректировочный платеж с суммой входящего баланса
            v_payment_id := PK02_POID.Next_payment_id;
            -- cохраняем информацию о платеже
            INSERT INTO PAYMENT_T (
                PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE,
                PAYMENT_DATE, ACCOUNT_ID, RECVD,
                ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED, REFUND,
                DATE_FROM, DATE_TO,
                PAYSYSTEM_ID, PAYSYSTEM_CODE,
                DOC_ID,
                STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED,
                CREATED_BY, NOTES, Pay_Descr
            )SELECT 
                v_payment_id, v_period_id, 'INBAL',
                v_balance_date, p_account_id, p_balance,
                p_balance, v_balance_date, p_balance, 0, 0,
                NULL, NULL,
                v_paysystem_id, 'IN_BALANCE',
                'л/с '||A.ACCOUNT_NO||' - входящий баланс',
                'IMPORT', SYSDATE, SYSDATE, SYSDATE,
                'Макеев С.В.', 
                'INBAL', 'л/с '||A.ACCOUNT_NO||' - входящий баланс'
              FROM ACCOUNT_T A
             WHERE A.ACCOUNT_ID = p_account_id;
            --
            -- Изменяем баланс лицевого счета на величину аванса клиента КТТК
            UPDATE ACCOUNT_T
               SET BALANCE      = BALANCE + p_balance,
                   BALANCE_DATE = v_balance_date
             WHERE ACCOUNT_ID   = p_account_id;
            --
        END IF;
        --
        -- добавляем строку в описание входящего баланса 
        INSERT INTO INCOMING_BALANCE_T IB (
            ACCOUNT_ID, BALANCE, BALANCE_DATE, REP_PERIOD_ID, BILL_ID, PAYMENT_ID, CREATE_DATE
        )
        VALUES(
            p_account_id, p_balance, v_balance_date, v_period_id, v_bill_id, v_payment_id, SYSDATE
        );
        --
    ELSE
        -- фиксируем нулевой входящий остаток
        -- добавляем строку в описание входящего баланса 
        INSERT INTO INCOMING_BALANCE_T IB (
            ACCOUNT_ID, BALANCE, BALANCE_DATE, REP_PERIOD_ID, BILL_ID, PAYMENT_ID, CREATE_DATE
        )
        VALUES(
            p_account_id, p_balance, v_balance_date, v_period_id, NULL, NULL, SYSDATE
        );
    END IF;
    --
    -- пересчитываем баланс на л/с
    v_balance := Refresh_balance ( p_account_id );
    --
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------------------
-- 7) удалить входящий баланс лицевого счета
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Delete_incomming_balance(
               p_account_id     IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_incomming_balance';
    v_period_id  INTEGER; 
    v_bill_id    INTEGER; 
    v_payment_id INTEGER;
    v_balance    NUMBER;
BEGIN
    -- получаем исходные данные
    SELECT IB.REP_PERIOD_ID, IB.BILL_ID, IB.PAYMENT_ID
      INTO v_period_id, v_bill_id, v_payment_id 
      FROM INCOMING_BALANCE_T IB
     WHERE IB.ACCOUNT_ID = p_account_id;
    
    -- удаляем платежи входящего баланса
    IF v_payment_id IS NOT NULL THEN
        DELETE FROM PAYMENT_T P
         WHERE P.REP_PERIOD_ID = v_period_id
           AND P.PAYMENT_ID    = v_payment_id;
    END IF;
    
    IF v_bill_id IS NOT NULL THEN
        -- удаляем строки начислений
        DELETE FROM ITEM_T I
         WHERE I.REP_PERIOD_ID = v_period_id
           AND I.BILL_ID       = v_bill_id;
        -- удаляем строки счетов-фактур
        DELETE FROM INVOICE_ITEM_T V
         WHERE V.REP_PERIOD_ID = v_period_id
           AND V.BILL_ID       = v_bill_id;
        -- удаляем счета
        DELETE FROM BILL_T B
         WHERE B.REP_PERIOD_ID = v_period_id
           AND B.BILL_ID       = v_bill_id;
    END IF;
    
    -- удаляем описатель входящего баланса
    DELETE FROM INCOMING_BALANCE_T IB
     WHERE IB.ACCOUNT_ID = p_account_id;
     
    -- пересчитываем баланс на л/с
    v_balance := Refresh_balance ( p_account_id );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- История формирования баланса л/с
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Account_balance_history (
               p_recordset    OUT t_refc,
               p_account_id   IN INTEGER 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Account_balance_history';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
      WITH BP AS (
      SELECT ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID
                                ORDER BY REP_PERIOD_ID, NVL(BILL_DATE, PAYMENT_DATE)) RN,
             BP.*             
        FROM (
          SELECT B.ACCOUNT_ID, B.REP_PERIOD_ID,  
                 B.BILL_NO, B.BILL_DATE, B.TOTAL, B.GROSS, 
                 NULL DOC_ID, NULL PAYMENT_DATE, 0 RECVD
            FROM BILL_T B
          UNION ALL
          SELECT P.ACCOUNT_ID, P.REP_PERIOD_ID, 
                 NULL BILL_NO, NULL BILL_DATE, 0 TOTAL, 0 GROSS, 
                 P.DOC_ID, P.PAYMENT_DATE, P.RECVD
            FROM PAYMENT_T P
         ) BP
         WHERE ACCOUNT_ID = p_account_id
           AND NOT EXISTS (
              SELECT * 
                FROM INCOMING_BALANCE_T IB
               WHERE IB.ACCOUNT_ID    = BP.ACCOUNT_ID
                 AND IB.REP_PERIOD_ID > BP.REP_PERIOD_ID 
            )
      )
      SELECT RN, ACCOUNT_ID, REP_PERIOD_ID,  
             BILL_NO, BILL_DATE, 
             --TOTAL, GROSS,
             TO_CHAR(TOTAL,'999G999G999G990D99') BILL_TOTAL,
             DOC_ID, PAYMENT_DATE, 
             --RECVD,
             TO_CHAR(RECVD,'999G999G999G990D99') PAY_RECVD,
             --SUM(RECVD - TOTAL) OVER (PARTITION BY ACCOUNT_ID ORDER BY REP_PERIOD_ID, RN) BALANCE,
             TO_CHAR(SUM(RECVD - TOTAL) OVER (PARTITION BY ACCOUNT_ID ORDER BY RN),'999G999G999G990D99') CUR_BALANCE
        FROM BP 
        ORDER BY RN
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------- --
-- История формирования баланса по договору
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Contract_balance_history (
               p_recordset    OUT t_refc,
               p_contract_id   IN INTEGER 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Contract_balance_history';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
      WITH CT AS (
          SELECT PERIOD_ID, PROFILE_ID, ACCOUNT_ID, ACCOUNT_NO, CONTRACT_ID, CONTRACT_NO
            FROM (
              SELECT ROW_NUMBER() OVER (PARTITION BY AP.ACCOUNT_ID, P.PERIOD_ID ORDER BY AP.DATE_FROM DESC) RN,
                     P.PERIOD_ID, AP.PROFILE_ID, AP.ACCOUNT_ID, A.ACCOUNT_NO, AP.CONTRACT_ID, C.CONTRACT_NO  
                FROM PERIOD_T P, ACCOUNT_PROFILE_T AP, ACCOUNT_T A, CONTRACT_T C
               WHERE AP.DATE_FROM < P.PERIOD_TO
                 AND (AP.DATE_TO IS NULL OR P.PERIOD_FROM < AP.DATE_TO)
                 AND AP.ACCOUNT_ID = A.ACCOUNT_ID
                 AND AP.CONTRACT_ID = C.CONTRACT_ID
                 AND AP.CONTRACT_ID = p_contract_id
             )
             WHERE RN = 1
      ), BP AS (
          SELECT ROW_NUMBER() OVER (PARTITION BY CT.CONTRACT_ID
                                    ORDER BY BP.REP_PERIOD_ID, NVL(BP.BILL_DATE, BP.PAYMENT_DATE)) RN,
                 CT.CONTRACT_ID, CT.CONTRACT_NO, CT.ACCOUNT_NO,
                 BP.* 
            FROM (
              SELECT B.ACCOUNT_ID, B.REP_PERIOD_ID,  
                     B.BILL_NO, B.BILL_DATE, B.TOTAL, B.GROSS, 
                     NULL DOC_ID, NULL PAYMENT_DATE, 0 RECVD
                FROM BILL_T B
              UNION ALL
              SELECT P.ACCOUNT_ID, P.REP_PERIOD_ID, 
                     NULL BILL_NO, NULL BILL_DATE, 0 TOTAL, 0 GROSS, 
                     P.DOC_ID, P.PAYMENT_DATE, P.RECVD
                FROM PAYMENT_T P
             ) BP, CT
             WHERE BP.ACCOUNT_ID = CT.ACCOUNT_ID
               AND NOT EXISTS (
                  SELECT * 
                    FROM INCOMING_BALANCE_T IB
                   WHERE IB.ACCOUNT_ID    = BP.ACCOUNT_ID
                     AND IB.REP_PERIOD_ID > BP.REP_PERIOD_ID 
                )
               AND BP.REP_PERIOD_ID = CT.PERIOD_ID
               AND BP.ACCOUNT_ID = CT.ACCOUNT_ID
      )
      SELECT RN, CONTRACT_ID, CONTRACT_NO, ACCOUNT_ID, ACCOUNT_NO, REP_PERIOD_ID,  
             BILL_NO, BILL_DATE, 
             --TOTAL, GROSS,
             TO_CHAR(TOTAL,'999G999G999G990D99') BILL_TOTAL,
             DOC_ID, PAYMENT_DATE, 
             --RECVD,
             TO_CHAR(RECVD,'999G999G999G990D99') PAY_RECVD,
             --SUM(RECVD - TOTAL) OVER (PARTITION BY ACCOUNT_ID ORDER BY REP_PERIOD_ID, RN) BALANCE,
             TO_CHAR(SUM(RECVD - TOTAL) OVER (PARTITION BY CONTRACT_ID ORDER BY RN),'999G999G999G990D99') CUR_BALANCE
        FROM BP 
        ORDER BY RN
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------- --
-- Обороты по л/с за указанный период
-- ------------------------------------------------------------------------- --
PROCEDURE Account_period_info (
               p_recordset    OUT t_refc,
               p_period_id    IN  NUMBER,
               p_account_id   IN  INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Account_period_info';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
      WITH PI AS (
          SELECT ACCOUNT_NO, ACCOUNT_ID, 
                 NVL(REP_PERIOD_ID, p_period_id) REP_PERIOD_ID, 
                 NVL(OPEN_BALANCE, 0)  OPEN_BALANCE, 
                 NVL(CLOSE_BALANCE, 0) CLOSE_BALANCE, 
                 NVL(TOTAL, 0) TOTAL,
                 NVL(GROSS, 0) GROSS,
                 NVL(RECVD, 0) RECVD
            FROM (
              SELECT A.ACCOUNT_NO, A.ACCOUNT_ID, 
                     PI.REP_PERIOD_ID, PI.OPEN_BALANCE, PI.CLOSE_BALANCE, PI.TOTAL, PI.GROSS, PI.RECVD,
                     MAX(PI.REP_PERIOD_ID) OVER (PARTITION BY PI.ACCOUNT_ID) MAX_PERIOD_ID 
                FROM ACCOUNT_T A, REP_PERIOD_INFO_T PI
               WHERE A.ACCOUNT_ID   = PI.ACCOUNT_ID(+)
                 AND PI.REP_PERIOD_ID(+) <= p_period_id
                 AND A.ACCOUNT_ID = p_account_id
           )
           WHERE (REP_PERIOD_ID = MAX_PERIOD_ID OR REP_PERIOD_ID IS NULL)
      )
      SELECT --PI.REP_PERIOD_ID,
             PI.ACCOUNT_ID,
             PI.ACCOUNT_NO,
             CASE
                 WHEN PI.REP_PERIOD_ID < p_period_id THEN PI.CLOSE_BALANCE
                 ELSE PI.OPEN_BALANCE
             END IN_BALANCE,
             CASE
                 WHEN PI.REP_PERIOD_ID < p_period_id THEN 0
                 ELSE PI.TOTAL
             END TOTAL,
             CASE
                 WHEN PI.REP_PERIOD_ID < p_period_id THEN 0
                 ELSE PI.RECVD
             END RECVD,
             CASE
                 WHEN PI.REP_PERIOD_ID < p_period_id THEN PI.CLOSE_BALANCE
                 ELSE PI.CLOSE_BALANCE
             END OUT_BALANCE
        FROM PI
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK05_ACCOUNT_BALANCE;
/
