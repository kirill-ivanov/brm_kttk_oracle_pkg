CREATE OR REPLACE PACKAGE PK05_ACCOUNT_BALANCE_OLD
IS
    --
    -- Пакет для работы с объектом "ЛИЦЕВОЙ СЧЕТ", таблицы:
    -- account_t, account_profile_t, billinfo_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK05_ACCOUNT_BALANCE_OLD';
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

    --
    -- 5) Пересчитать баланс по всем выставленным счетам и оплатам 
    --   - при ошибке выставляем исключение
    FUNCTION Refresh_balance ( 
                   p_account_id IN INTEGER   -- ID позиции счета
               ) RETURN NUMBER;

    --
    -- 6) Пересчитать баланс от последней записи в REP_PERIOD_INFO_T 
    --   - при ошибке выставляем исключение
    FUNCTION Refresh_balance_last( 
                   p_account_id IN INTEGER   -- ID позиции счета
               ) RETURN NUMBER;
    
END PK05_ACCOUNT_BALANCE_OLD;
/
CREATE OR REPLACE PACKAGE BODY PK05_ACCOUNT_BALANCE_OLD
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
                       THEN ROUND(I.ITEM_TOTAL * (1+NVL(AP.VAT,0)/100),2)
                     ELSE 0
                   END 
               ) TOTAL
          FROM ITEM_T I, BILL_T B, ACCOUNT_PROFILE_T AP, PERIOD_T P
         WHERE B.REP_PERIOD_ID > P.PERIOD_ID
           AND I.BILL_ID = B.BILL_ID
           AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND B.ACCOUNT_ID = AP.ACCOUNT_ID
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
    WITH BL AS (
        SELECT B.ACCOUNT_ID, SUM(B.TOTAL) BILL_TOTAL 
          FROM BILL_T B
         WHERE B.REP_PERIOD_ID >=  TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE,'mm'),-3),'yyyymm') -- rep_period_id
           AND B.PAID_TO > SYSDATE
           AND B.ACCOUNT_ID = p_account_id
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

--
-- Пересчитать баланс по всем выставленным счетам и оплатам 
--   - при ошибке выставляем исключение
FUNCTION Refresh_balance ( 
               p_account_id IN INTEGER   -- ID позиции счета
           ) RETURN NUMBER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Refresh_balance';
    v_balance    NUMBER;
BEGIN
    UPDATE ACCOUNT_T
      SET BALANCE = 0, BALANCE_DATE = SYSDATE
     WHERE ACCOUNT_ID = p_account_id;
    
    MERGE INTO ACCOUNT_T A
    USING (
       /*
       SELECT BP.ACCOUNT_ID, 
              MIN(NVL(IB.BALANCE, 0))+SUM(BP.RECVD-BP.BILL_TOTAL) BALANCE,
              GREATEST( NVL(MAX(IB.BALANCE_DATE), TO_DATE('01.01.2000','dd.mm.yyyy')), 
                            MAX(BP.BILL_DATE), 
                            MAX(BP.PAYMENT_DATE)) BALANCE_DATE
        FROM (
            -- получаем полную задолженность по выставленным счетам
            SELECT B.ACCOUNT_ID, 
                   B.REP_PERIOD_ID,
                   B.TOTAL BILL_TOTAL, 
                   BILL_DATE, 
                   0 RECVD, TO_DATE('01.01.2000','dd.mm.yyyy') PAYMENT_DATE 
              FROM BILL_T B
             WHERE B.ACCOUNT_ID = p_account_id
               AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_READY, 
                                     Pk00_Const.c_BILL_STATE_CLOSED)
            UNION ALL
            -- получаем сумму поступивших за период платежей
            SELECT P.ACCOUNT_ID, 
                   P.REP_PERIOD_ID,
                   0 BILL_TOTAL, TO_DATE('01.01.2000','dd.mm.yyyy') BILL_DATE,
                   P.RECVD, 
                   P.PAYMENT_DATE  
              FROM PAYMENT_T P
             WHERE P.ACCOUNT_ID = p_account_id
        ) BP, INCOMING_BALANCE_T IB 
       WHERE BP.ACCOUNT_ID = IB.ACCOUNT_ID(+)
       AND CASE
            WHEN IB.ACCOUNT_ID IS NULL THEN 1
            WHEN IB.ACCOUNT_ID IS NOT NULL AND BP.BILL_DATE     > IB.BALANCE_DATE  THEN 1
            WHEN IB.ACCOUNT_ID IS NOT NULL AND BP.REP_PERIOD_ID > IB.REP_PERIOD_ID THEN 1
            ELSE 0
           END = 1
        GROUP BY BP.ACCOUNT_ID
       */
        WITH BP AS (      
          SELECT ACCOUNT_ID, REP_PERIOD_ID, BP_DATE, BILL_TOTAL, RECVD
            FROM (
                -- получаем полную задолженность по выставленным счетам
                SELECT B.ACCOUNT_ID, 
                       B.REP_PERIOD_ID,
                       B.BILL_DATE BP_DATE,
                       B.TOTAL BILL_TOTAL, 
                       0 RECVD
                  FROM BILL_T B
                 WHERE B.ACCOUNT_ID = p_account_id
                   AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_READY, 
                                         Pk00_Const.c_BILL_STATE_CLOSED)
                UNION ALL
                -- получаем сумму поступивших за период платежей
                SELECT P.ACCOUNT_ID, 
                       P.REP_PERIOD_ID,
                       P.PAYMENT_DATE BP_DATE,
                       0 BILL_TOTAL,
                       P.RECVD 
                  FROM PAYMENT_T P
                 WHERE P.ACCOUNT_ID = p_account_id
             ) BP
           WHERE NOT EXISTS (
            SELECT * FROM INCOMING_BALANCE_T IB
             WHERE BP.ACCOUNT_ID = IB.ACCOUNT_ID
               AND BP.BP_DATE   <= IB.BALANCE_DATE
          )  
        ), BPI AS (
            SELECT ACCOUNT_ID, 
                   REP_PERIOD_ID,
                   BP_DATE,
                   BILL_TOTAL,
                   RECVD,
                   0 INBAL
              FROM BP
            UNION ALL  
            SELECT ACCOUNT_ID, 
                   TO_NUMBER(TO_CHAR(TRUNC(IB.BALANCE_DATE,'mm'),'yyyymm')) REP_PERIOD_ID,
                   IB.BALANCE_DATE BP_DATE,
                   0 BILL_TOTAL,
                   0 RECVD,
                   IB.BALANCE
              FROM INCOMING_BALANCE_T IB   
             WHERE IB.ACCOUNT_ID = p_account_id
        )
        SELECT ACCOUNT_ID, SUM(RECVD) - SUM(BILL_TOTAL) + SUM(INBAL) BALANCE, MAX(BP_DATE) BALANCE_DATE 
          FROM BPI
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

--
-- Пересчитать баланс от последней записи в REP_PERIOD_INFO_T 
--   - при ошибке выставляем исключение
FUNCTION Refresh_balance_last( 
               p_account_id IN INTEGER   -- ID позиции счета
           ) RETURN NUMBER
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Refresh_balance_last';
    v_balance        NUMBER;
    v_last_period_id INTEGER;
BEGIN
    -- ID последнего закрытого периода
    v_last_period_id := PK04_PERIOD.Last_period_id; 
    -- получаем входящий остаток от последнего закрытого периода + начисления и платежи
    MERGE INTO ACCOUNT_T A
    USING (
      SELECT R.ACCOUNT_ID, (R.CLOSE_BALANCE + BALANCE) BALANCE, BALANCE_DATE
        FROM REP_PERIOD_INFO_T R,
             (SELECT ACCOUNT_ID, SUM(RECVD-BILL_TOTAL) BALANCE,
                     CASE
                         WHEN MAX(BILL_DATE) > MAX(PAYMENT_DATE) THEN MAX(BILL_DATE)
                         ELSE MAX(PAYMENT_DATE)
                     END BALANCE_DATE 
              FROM (
                  -- получаем полную задолженность по выставленным счетам
                  SELECT B.ACCOUNT_ID, 
                         (B.TOTAL + B.ADJUSTED) BILL_TOTAL, BILL_DATE, 
                         0 RECVD, TO_DATE('01.01.2000','dd.mm.yyyy') PAYMENT_DATE 
                    FROM BILL_T B
                   WHERE B.REP_PERIOD_ID > v_last_period_id
                     AND B.TOTAL <> 0 -- отсекаем секцию с пустыми счетами
                     AND B.ACCOUNT_ID = p_account_id
                  UNION ALL
                  -- получаем сумму поступивших за период платежей
                  SELECT P.ACCOUNT_ID, 
                         0 BILL_TOTAL, TO_DATE('01.01.2000','dd.mm.yyyy') BILL_DATE,
                         P.RECVD, P.PAYMENT_DATE  
                    FROM PAYMENT_T P
                   WHERE P.REP_PERIOD_ID > v_last_period_id
                     AND P.ACCOUNT_ID = p_account_id
              )
              GROUP BY ACCOUNT_ID) T
      WHERE R.ACCOUNT_ID = T.ACCOUNT_ID
        AND R.REP_PERIOD_ID = v_last_period_id
        AND R.ACCOUNT_ID = p_account_id
    ) D
    ON (A.ACCOUNT_ID = D.ACCOUNT_ID)
    WHEN MATCHED THEN UPDATE SET A.BALANCE_DATE = D.BALANCE_DATE, A.BALANCE = D.BALANCE;
    -- читаем расчитанный баланс
    SELECT A.BALANCE INTO v_balance
      FROM ACCOUNT_T A
     WHERE A.ACCOUNT_ID = p_account_id;
    --
    RETURN v_balance;
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


END PK05_ACCOUNT_BALANCE_OLD;
/
