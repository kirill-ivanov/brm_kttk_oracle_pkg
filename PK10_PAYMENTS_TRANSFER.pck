CREATE OR REPLACE PACKAGE PK10_PAYMENTS_TRANSFER
IS
    --
    -- Обслуживание массовой разноски платежей при выставлении счетов
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_PAYMENTS_TRANSFER';
    -- ==============================================================================
   
    type t_refc is ref cursor;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Процедура массовой разноски платежей методом FIFO для физ.лиц
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Method_fifo;
    
    -- ======================================================================== --
    -- удаление операций разноски
    -- ------------------------------------------------------------------------ --
    -- 1) Удаление операции разноски из цепочки
    PROCEDURE Delete_from_chain (
                   p_pay_period_id IN INTEGER,
                   p_payment_id    IN INTEGER,
                   p_transfer_id   IN INTEGER
               );
               
    -- 2) Удаление всей цепочки разноски платежа
    PROCEDURE Delete_transfer_chain (
                   p_pay_period_id IN INTEGER,
                   p_payment_id    IN INTEGER
               );
    
    -- 3) Удаление цепочки разноски платежей на счет
    PROCEDURE Delete_transfer_bill (
                   p_period_id     IN INTEGER,
                   p_bill_id       IN INTEGER
               );
    
    -- ------------------------------------------------------------------------ --
    -- 4) Сторнирование цепочки разноски платежей при создании Кредит-ноты
    --    (перенос со знаком минус на сторнирующий счет)
    -- ------------------------------------------------------------------------ --
    PROCEDURE Credit_bill_transfer (
                   p_period_id     IN INTEGER,
                   p_bill_id       IN INTEGER,
                   p_crd_period_id IN INTEGER,
                   p_crd_bill_id   IN INTEGER
               );
    
    -- 5) Сторнирование цепочки разноски указанного платежа
    PROCEDURE Revers_transfer_chain (
                   p_period_id     IN INTEGER,
                   p_payment_id    IN INTEGER
               );
    
    -- ======================================================================== --
    -- проверка корректности разноски платежей на счета
    -- ------------------------------------------------------------------------ --
    -- 1) Проверка целостности цепочек разноски платежей
    PROCEDURE Check_transfer_chain (
                   p_recordset    OUT t_refc
               );
               
    -- 2) Проверка правильности начисления авансов 
    PROCEDURE Check_advance (
                   p_recordset    OUT t_refc
               );
               
    -- 3) Корректировка авансов
    PROCEDURE Correct_advance;
    
    -- 4) Просмотр цепочки  
    PROCEDURE View_transfer_chain (
                   p_recordset    OUT t_refc,
                   p_pay_period_id IN INTEGER,
                   p_payment_id    IN INTEGER
               );

    -- 5) проверка правильности проставления баланса платей
    PROCEDURE Check_payments_balance (p_recordset    OUT t_refc);

    -- 6) проверка соответствия операций разноски платежам
    PROCEDURE Check_payments_transfer (p_recordset    OUT t_refc);
     
    -- 7) проверка соответствия операций разноски счетам
    PROCEDURE Check_payments_bills (p_recordset    OUT t_refc);


END PK10_PAYMENTS_TRANSFER;
/
CREATE OR REPLACE PACKAGE BODY PK10_PAYMENTS_TRANSFER
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Процедура массовой разноски платежей методом FIFO для физ.лиц
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Method_fifo
IS
    v_prcName   CONSTANT varchar2(16) := 'Method_fifo';
    v_step      INTEGER;
    v_count     INTEGER;
    v_ok        INTEGER;
    v_err       INTEGER;
    v_period_id INTEGER; -- период в который будет выполнена разноска
BEGIN

    v_step := 0;
        
    -- если открыт биллинговый период помещаем записи туда, если нет в открытый
    SELECT PERIOD_ID 
      INTO v_period_id
      FROM ( 
        SELECT PERIOD_ID 
          FROM PERIOD_T P
         WHERE POSITION IN ('OPEN','BILL')
         ORDER BY PERIOD_ID
     )
     WHERE ROWNUM = 1
    ;
    Pk01_Syslog.Write_Msg('Start, period_id='||v_period_id, c_PkgName||'.'||v_prcName);
    
    LOOP
      -- ------------------------------------------------------------------------- --
      -- заполняем временную таблицу:
      -- ------------------------------------------------------------------------- --
      EXECUTE IMMEDIATE 'TRUNCATE TABLE PK10_PAYMENTS_TRANSFER_TMP DROP STORAGE';

      INSERT INTO PK10_PAYMENTS_TRANSFER_TMP (
          ACCOUNT_ID, BILL_ID, REP_PERIOD_ID, BILL_DATE, DUE, 
          PAYMENT_ID, PAY_PERIOD_ID, PAY_BALANCE, 
          TR_NEW_TRANSFER_TOTAL, 
          TR_STATUS,
          PAY_NEW_BALANCE, 
          PAY_NEW_DATE_FROM, 
          PAY_NEW_DATE_TO, 
          BL_NEW_DUE
      )
      WITH PAY AS (
        -- выбираем самые старые неразнесенные платежи
        SELECT ACCOUNT_ID, PAYMENT_ID, REP_PERIOD_ID PAY_PERIOD_ID, BALANCE, 
               PAYMENT_TYPE, PAY_DATE_FROM, PAY_DATE_TO 
          FROM ( 
              SELECT P.ACCOUNT_ID, 
                     P.PAYMENT_ID, P.REP_PERIOD_ID, P.BALANCE, P.PAYMENT_TYPE, 
                     P.DATE_FROM PAY_DATE_FROM, P.DATE_TO PAY_DATE_TO,
                     ROW_NUMBER() OVER (PARTITION BY P.ACCOUNT_ID ORDER BY P.REP_PERIOD_ID ASC, P.PAYMENT_DATE ASC) RN 
                FROM PAYMENT_T P, INCOMING_BALANCE_T IB
               WHERE P.BALANCE > 0 -- еще не все разнесли
                 AND P.RECVD   > 0 -- отрицательные платежи не разносим
                 AND P.REP_PERIOD_ID <= v_period_id 
                 AND P.ACCOUNT_ID    > 2
                 AND P.ACCOUNT_ID     = IB.ACCOUNT_ID(+)
                 AND P.REP_PERIOD_ID >= IB.REP_PERIOD_ID(+) 
        )
        WHERE RN=1
      ), BL AS (
        -- выбираем самые старые незакрытые счета
        SELECT ACCOUNT_ID, BILL_ID, REP_PERIOD_ID, BILL_DATE, DUE
          FROM (
              SELECT B.ACCOUNT_ID, B.BILL_ID, B.REP_PERIOD_ID, B.BILL_DATE, B.DUE,
                 ROW_NUMBER() OVER (PARTITION BY B.ACCOUNT_ID ORDER BY B.REP_PERIOD_ID ASC, B.BILL_DATE ASC) RN 
                FROM BILL_T B, INCOMING_BALANCE_T IB
               WHERE B.TOTAL   > 0  -- отсекаем секцию с пустыми счетами
                 AND B.DUE     < 0  -- есть непогашенная задолженность
                 AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_CLOSED, 
                                       Pk00_Const.c_BILL_STATE_READY) -- 'CLOSED', 'READY'
      --           AND B.REP_PERIOD_ID <= 201408 
                 AND B.ACCOUNT_ID     = IB.ACCOUNT_ID(+)
                 AND B.REP_PERIOD_ID >= IB.REP_PERIOD_ID(+)
        )
        WHERE RN=1
      )
      SELECT BL.ACCOUNT_ID, BL.BILL_ID, BL.REP_PERIOD_ID, BL.BILL_DATE, BL.DUE,
             PAY.PAYMENT_ID, PAY.PAY_PERIOD_ID, PAY.BALANCE PAY_BALANCE,
             -- проставляем сумму операции разноски
             CASE 
                 WHEN PAY.BALANCE >= ABS(BL.DUE) THEN ABS(BL.DUE) -- разносим только часть платежа
                 ELSE PAY.BALANCE   -- разносим весь(остаток) платеж
             END TR_NEW_TRANSFER_TOTAL, 
             'OK' TR_STATUS,
             -- проставляем остаток на платеже после разноски
             CASE 
                 WHEN PAY.BALANCE >= ABS(BL.DUE) THEN (PAY.BALANCE + BL.DUE) -- разносим только часть платежа
                 ELSE 0            -- разносим весь(остаток) платеж
             END PAY_NEW_BALANCE,
             -- проставляем диапазон дат счетов которые закрыл платеж
             CASE 
                 WHEN PAY.PAY_DATE_FROM IS NULL THEN TRUNC(BL.BILL_DATE,'mm')
                 WHEN BL.BILL_DATE < PAY.PAY_DATE_FROM THEN TRUNC(BL.BILL_DATE,'mm') 
                 ELSE PAY.PAY_DATE_FROM
             END PAY_NEW_DATE_FROM,
             --
             CASE 
                 WHEN PAY.PAY_DATE_TO IS NULL THEN BL.BILL_DATE
                 WHEN PAY.PAY_DATE_TO < BL.BILL_DATE THEN BL.BILL_DATE 
                 ELSE PAY.PAY_DATE_TO
             END PAY_NEW_DATE_TO,
             -- проставляем задолженность счета после разноски
             CASE 
                 WHEN PAY.BALANCE >= ABS(BL.DUE) THEN 0 -- разносим только часть платежа
                 ELSE (PAY.BALANCE + BL.DUE)            -- разносим весь(остаток) платеж
             END BL_NEW_DUE
        FROM PAY, BL, ACCOUNT_T A
       WHERE A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_P   -- только для физ.лиц
         AND PAY.ACCOUNT_ID = A.ACCOUNT_ID
         AND PAY.ACCOUNT_ID = BL.ACCOUNT_ID
         AND A.BILLING_ID IN (PK00_CONST.c_BILLING_KTTK, PK00_CONST.c_BILLING_MMTS) 
      ;
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('PK10_PAYMENTS_TRANSFER_TMP '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );      
      COMMIT;

      -- анализируем загруженную таблицу
      DBMS_STATS.gather_table_stats(ownname => 'PIN',
                                    tabname => 'PK10_PAYMENTS_TRANSFER_TMP',
                                    granularity => 'ALL',
                                    CASCADE => TRUE,
                                    no_invalidate => FALSE);

      -- ------------------------------------------------------------------------- --
      -- проверяем на ошибки и условия выхода 
      -- ------------------------------------------------------------------------- --
      SELECT COUNT(*) INTO v_ok
        FROM PK10_PAYMENTS_TRANSFER_TMP
       WHERE TR_STATUS = 'OK';
       
      -- пишем информацию об ошибках в систему логирования
      v_err := v_count - v_ok;
      IF v_err > 0 THEN
        FOR err IN (
            SELECT T.PAYMENT_ID, T.PAY_PERIOD_ID 
              FROM PK10_PAYMENTS_TRANSFER_TMP T
             WHERE TR_STATUS != 'OK'
          )
        LOOP
          Pk01_Syslog.Write_msg('payment_id='||err.payment_id||', '||
                                'period_id='||err.pay_period_id||' - error', 
                                c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END LOOP;
      END IF;
      
      -- выходим, полезной информации больше нет       
      EXIT WHEN v_ok = 0;

      -- ------------------------------------------------------------------------- --
      -- Обработка информации
      -- ------------------------------------------------------------------------- --
      -- изменяем сумму задолженности на счетах
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      MERGE INTO BILL_T B
      USING (
          SELECT BILL_ID, REP_PERIOD_ID, BL_NEW_DUE
            FROM PK10_PAYMENTS_TRANSFER_TMP
           WHERE TR_STATUS = 'OK'  
      ) T
      ON ( B.BILL_ID = T.BILL_ID
       AND B.REP_PERIOD_ID = T.REP_PERIOD_ID
      )
      WHEN MATCHED THEN UPDATE SET B.DUE = T.BL_NEW_DUE, 
                                   B.RECVD = B.TOTAL + T.BL_NEW_DUE,
                                   B.DUE_DATE = SYSDATE;
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('BILL_T '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- изменяем платежи
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      MERGE INTO PAYMENT_T P
      USING (
          SELECT PAYMENT_ID,
                 PAY_PERIOD_ID,
                 PAY_NEW_BALANCE,
                 PAY_NEW_DATE_FROM,
                 PAY_NEW_DATE_TO
            FROM PK10_PAYMENTS_TRANSFER_TMP
           WHERE TR_STATUS = 'OK'
      ) T
      ON ( P.PAYMENT_ID = T.PAYMENT_ID
       AND P.REP_PERIOD_ID = T.PAY_PERIOD_ID
      )
      WHEN MATCHED THEN UPDATE SET 
           P.BALANCE    = T.PAY_NEW_BALANCE, 
           P.TRANSFERED = (P.RECVD - P.REFUND - T.PAY_NEW_BALANCE),
           P.DATE_FROM  = T.PAY_NEW_DATE_FROM,
           P.DATE_TO    = T.PAY_NEW_DATE_TO;
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('PAYMENT_T '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
              
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --              
      -- формируем операции разноски
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      INSERT INTO PAY_TRANSFER_T (
             PERIOD_ID, TRANSFER_ID, 
             PAYMENT_ID, PAY_PERIOD_ID, BILL_ID, REP_PERIOD_ID, 
             TRANSFER_TOTAL, TRANSFER_DATE, NOTES
      )  
      SELECT v_period_id, SQ_TRANSFER_ID.NEXTVAL TRANSFER_ID, 
             PAYMENT_ID, PAY_PERIOD_ID, BILL_ID, REP_PERIOD_ID, 
             TR_NEW_TRANSFER_TOTAL TRANSFER_TOTAL, SYSDATE TRANSFER_DATE,
             NULL NOTES 
      FROM PK10_PAYMENTS_TRANSFER_TMP  
      WHERE TR_STATUS = 'OK';
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('PAY_TRANSFER_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      
      COMMIT;
      
      -- переходим к следующему шагу        
      v_step := v_step + 1;
      Pk01_Syslog.Write_msg('step '||v_step||' : '||v_ok||'-ok, '||v_err||'-err', 
                                c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      COMMIT; 
        
    END LOOP;              

    COMMIT;

    Pk01_Syslog.Write_Msg('Stop', c_PkgName||'.'||v_prcName);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- ======================================================================== --
-- проверка корректности разноски платежей на счета
-- ------------------------------------------------------------------------ --
-- 1) Проверка целостности цепочек разноски платежей
--   - при ошибке выставляет исключение
PROCEDURE Check_transfer_chain (
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_transfer_chain';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR 
      SELECT SYSDATE FROM DUAL
      /*
      WITH PT AS (
          SELECT LVL, PAYMENT_ID, PAY_PERIOD_ID, TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE,
                 NVL(LAG(CLOSE_BALANCE) OVER(PARTITION BY PAYMENT_ID ORDER BY LVL),OPEN_BALANCE) PREV_CLOSE_BALANCE,
                 REP_PERIOD_ID,
                 NVL(LAG(REP_PERIOD_ID) OVER(PARTITION BY PAYMENT_ID ORDER BY LVL), REP_PERIOD_ID) PREV_REP_PERIOD_ID
          FROM (
             SELECT LEVEL LVL,  
               PAYMENT_ID, PAY_PERIOD_ID, REP_PERIOD_ID, TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE
               FROM PAY_TRANSFER_T T
             CONNECT BY PRIOR TRANSFER_ID = PREV_TRANSFER_ID 
             START WITH PREV_TRANSFER_ID IS NULL
          )
      ) 
      SELECT LVL, PAYMENT_ID, PAY_PERIOD_ID, TRANSFER_TOTAL, 
             PREV_CLOSE_BALANCE, OPEN_BALANCE, CLOSE_BALANCE, 
             REP_PERIOD_ID, PREV_REP_PERIOD_ID
        FROM PT
      WHERE CLOSE_BALANCE != (OPEN_BALANCE - TRANSFER_TOTAL)  -- проверка правильности ведения балансов
         OR OPEN_BALANCE  != PREV_CLOSE_BALANCE               -- проверка связанности записей по балансам
         OR REP_PERIOD_ID < PREV_REP_PERIOD_ID                -- проверка последовательности распределения платежей по периодам (при сторнировании могут быть вопросы)
      */
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
-- 2) Проверка правильности начисления авансов 
-- ------------------------------------------------------------------------ --
PROCEDURE Check_advance (
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_advance';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR 
      WITH PT AS (
          SELECT PAYMENT_ID, PAY_PERIOD_ID, SUM(TRANSFER_TOTAL) ADVANCE 
            FROM PAY_TRANSFER_T
          WHERE PAY_PERIOD_ID < REP_PERIOD_ID     -- авансовая разноска
          GROUP BY PAYMENT_ID, PAY_PERIOD_ID
      )
      SELECT PT.ADVANCE EXP_ADVANCE, P.*
        FROM PT, PAYMENT_T P
       WHERE PT.PAYMENT_ID = P.PAYMENT_ID
         AND PT.PAY_PERIOD_ID = P.REP_PERIOD_ID
         AND PT.ADVANCE != P.ADVANCE
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
-- 3) Корректировка авансов
-- ------------------------------------------------------------------------ --
PROCEDURE Correct_advance
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Correct_advance';
    v_count     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- сбрасываем все авансы
    UPDATE PAYMENT_T P SET P.ADVANCE = 0;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Updated to 0 '||v_count||' rows in PAYMENT_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- пересчитываем все авансы    
    MERGE INTO PAYMENT_T P
    USING (
        SELECT PAYMENT_ID, PAY_PERIOD_ID, SUM(TRANSFER_TOTAL) FOR_SERVICE 
          FROM PAY_TRANSFER_T
        WHERE PAY_PERIOD_ID >= REP_PERIOD_ID     -- за оказанные услуги
        GROUP BY PAYMENT_ID, PAY_PERIOD_ID
    ) T
    ON (P.PAYMENT_ID = T.PAYMENT_ID AND P.REP_PERIOD_ID = T.PAY_PERIOD_ID)
    WHEN MATCHED THEN UPDATE SET P.ADVANCE = (P.RECVD-T.FOR_SERVICE), 
         P.ADVANCE_DATE = ADD_MONTHS(TRUNC(P.PAYMENT_DATE,'mm'),1)-1/86400;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Updated '||v_count||' rows in PAYMENT_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- ------------------------------------------------------------------------ --
-- 4) Просмотр цепочки разноски платежа
-- ------------------------------------------------------------------------ --
PROCEDURE View_transfer_chain (
               p_recordset    OUT t_refc,
               p_pay_period_id IN INTEGER,
               p_payment_id    IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'View_transfer_chain';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR 
      SELECT 
         TRANSFER_ID, PAYMENT_ID, PAY_PERIOD_ID, BILL_ID, REP_PERIOD_ID, 
         TRANSFER_TOTAL, TRANSFER_DATE, NOTES
        FROM PAY_TRANSFER_T T
       WHERE T.PAYMENT_ID    = p_payment_id
         AND T.PAY_PERIOD_ID = p_pay_period_id
       ORDER BY T.TRANSFER_DATE, T.TRANSFER_ID
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
-- 5) Удаление операции разноски из цепочки разноски платежа
-- ------------------------------------------------------------------------ --
PROCEDURE Delete_from_chain (
               p_pay_period_id IN INTEGER,
               p_payment_id    IN INTEGER,
               p_transfer_id   IN INTEGER
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Delete_from_chain';
    v_count            INTEGER;
    v_date_from        DATE; 
    v_date_to          DATE;
    --
    v_transfer_id      INTEGER;
    v_bill_id          INTEGER;
    v_rep_period_id    INTEGER;
    v_transfer_total   NUMBER;
    --
BEGIN
    --
    Pk01_Syslog.Write_msg('Start '|| 
                          'pay_period_id=' || p_pay_period_id||
                          ',payment_id='    || p_payment_id||
                          ',transfer_id='   || p_transfer_id,
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- Читаем данные об удаляемой операции
    SELECT
        T.TRANSFER_ID, T.BILL_ID, T.REP_PERIOD_ID, T.TRANSFER_TOTAL 
      INTO
        v_transfer_id, v_bill_id, v_rep_period_id, v_transfer_total
      FROM PAY_TRANSFER_T T
     WHERE T.PAY_PERIOD_ID = p_pay_period_id
       AND T.PAYMENT_ID    = p_payment_id
       AND T.TRANSFER_ID   = p_transfer_id
     ;

    -- удаляем указанную запись из цепочки разноски
    DELETE 
      FROM PAY_TRANSFER_T
     WHERE PAY_PERIOD_ID = p_pay_period_id
       AND PAYMENT_ID    = p_payment_id
       AND TRANSFER_ID   = p_transfer_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('pay_transfer_t '|| v_count ||' rows deleted',
                        c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- изменяем состояние счета, с которого сняли разноску
    UPDATE BILL_T B
      SET B.RECVD = (B.RECVD - v_transfer_total),
          B.DUE   = (B.DUE - v_transfer_total),
          B.DUE_DATE = SYSDATE
    WHERE B.BILL_ID = v_bill_id
      AND B.REP_PERIOD_ID = v_rep_period_id;
    Pk01_Syslog.Write_msg('bill_id='|| v_bill_id ||' - updated, '||
                      'transfer_total='|| v_transfer_total,
                      c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
/*
    -- уточняем временные границы разноски
    SELECT MIN(B.BILL_DATE) DATE_FROM, MAX(B.BILL_DATE) DATE_TO
      INTO v_date_from, v_date_to
      FROM PAY_TRANSFER_T PT, BILL_T B
     WHERE PT.PAYMENT_ID    = p_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND PT.BILL_ID       = B.BILL_ID
       AND PT.REP_PERIOD_ID = B.REP_PERIOD_ID;
*/
    -- изменяем поля платежа
    UPDATE PAYMENT_T P
      SET P.TRANSFERED    = (P.TRANSFERED - v_transfer_total),
          P.BALANCE       = (P.BALANCE + v_transfer_total),
          P.ADVANCE       = CASE
                            WHEN P.REP_PERIOD_ID > v_rep_period_id THEN (P.ADVANCE + v_transfer_total)
                            ELSE P.ADVANCE
                            END,
          P.LAST_MODIFIED = SYSDATE
		  /*,
         P.DATE_FROM     = v_date_from,
          P.DATE_TO       = v_date_to*/
    WHERE P.REP_PERIOD_ID = p_pay_period_id
      AND P.PAYMENT_ID    = p_payment_id;
    Pk01_Syslog.Write_msg('payment_id='|| p_payment_id ||' - updated, '||
                      'transfer_total='|| v_transfer_total,
                      c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- ------------------------------------------------------------------------ --
-- 6) Удаление всей цепочки разноски платежа
-- ------------------------------------------------------------------------ --
PROCEDURE Delete_transfer_chain (
               p_pay_period_id IN INTEGER,
               p_payment_id    IN INTEGER
           )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Delete_transfer_chain';
    v_count   INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start '|| 
                          'pay_period_id=' || p_pay_period_id||
                          ',payment_id='    || p_payment_id,
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- изменяем поля платежа
    UPDATE PAYMENT_T P
       SET P.TRANSFERED = 0,
           P.BALANCE       = P.RECVD,
           P.ADVANCE       = P.RECVD,
           P.LAST_MODIFIED = SYSDATE,
           P.DATE_FROM     = NULL,
           P.DATE_TO       = NULL
     WHERE P.REP_PERIOD_ID = p_pay_period_id
       AND P.PAYMENT_ID    = p_payment_id;
    Pk01_Syslog.Write_msg('payment_id = '|| p_payment_id ||' - updated set transfer = 0',
                        c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- изменяем балансы затронутых счетов
    MERGE INTO BILL_T B
    USING (
      SELECT REP_PERIOD_ID, BILL_ID, SUM(TRANSFER_TOTAL) TRANSFER_TOTAL
        FROM PAY_TRANSFER_T
       WHERE PAY_PERIOD_ID = p_pay_period_id
         AND PAYMENT_ID    = p_payment_id
       GROUP BY REP_PERIOD_ID, BILL_ID
    ) T
    ON( B.REP_PERIOD_ID = T.REP_PERIOD_ID AND
         B.BILL_ID = T.BILL_ID )
    WHEN MATCHED THEN UPDATE 
      SET B.RECVD = B.RECVD - T.TRANSFER_TOTAL,
          B.DUE   = B.DUE - T.TRANSFER_TOTAL,
          B.DUE_DATE = SYSDATE;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('updated '|| v_count ||' bills',
                        c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем всю цепочку разноски
    DELETE 
      FROM PAY_TRANSFER_T
     WHERE PAY_PERIOD_ID = p_pay_period_id
       AND PAYMENT_ID    = p_payment_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('pay_transfer_t '|| v_count ||' rows deleted',
                        c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- ------------------------------------------------------------------------ --
-- 3) Удаление цепочки разноски платежей на счет
-- ------------------------------------------------------------------------ --
PROCEDURE Delete_transfer_bill (
               p_period_id  IN INTEGER,
               p_bill_id    IN INTEGER
           )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Delete_transfer_bill';
    v_count   INTEGER := 0;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start '|| 
                          'period_id=' || p_period_id||
                          ',bill_id='  || p_bill_id,
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- удаляем все операции разноски
    FOR tr IN (
        SELECT TRANSFER_ID, PAY_PERIOD_ID, PAYMENT_ID 
          FROM PAY_TRANSFER_T
         WHERE BILL_ID       = p_bill_id
           AND REP_PERIOD_ID = p_period_id
      )
    LOOP
      Delete_from_chain (
               p_pay_period_id => tr.pay_period_id,
               p_payment_id    => tr.payment_id,
               p_transfer_id   => tr.transfer_id
           );
       v_count := v_count + 1;
    END LOOP;

    Pk01_Syslog.Write_msg('pay_transfer_t '|| v_count ||' rows deleted',
                        c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;
    
-- ------------------------------------------------------------------------ --
-- 4) Сторнирование цепочки разноски платежей при создании Кредит-ноты
--    (перенос со знаком минус на сторнирующий счет)
-- ------------------------------------------------------------------------ --
PROCEDURE Credit_bill_transfer (
               p_period_id     IN INTEGER,
               p_bill_id       IN INTEGER,
               p_crd_period_id IN INTEGER,
               p_crd_bill_id   IN INTEGER
           )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Credit_bill_transfer';
    v_count   INTEGER := 0;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start '|| 
                          ',bill_id='  || p_bill_id||
                          ',bill_crd_id='  || p_crd_bill_id,
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- пересчет балансов платежей с учетом возврата разносок
    MERGE INTO PAYMENT_T P
    USING (
        SELECT T.PAYMENT_ID, T.PAY_PERIOD_ID, SUM(T.TRANSFER_TOTAL) TR_TOTAL
          FROM PAY_TRANSFER_T T
         WHERE T.REP_PERIOD_ID = p_period_id
           AND T.BILL_ID       = p_bill_id
        GROUP BY PAYMENT_ID, PAY_PERIOD_ID
    ) T
    ON (P.PAYMENT_ID = T.PAYMENT_ID AND P.REP_PERIOD_ID = T.PAY_PERIOD_ID)
    WHEN MATCHED THEN UPDATE 
         SET P.BALANCE = P.BALANCE + T.TR_TOTAL,
             P.TRANSFERED = P.TRANSFERED - T.TR_TOTAL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Merge '||v_count||' rows in PAYMENT_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
 
    -- перенос разносок со знаком минус на сторнирующий счет
    INSERT INTO PAY_TRANSFER_T (
        PERIOD_ID, TRANSFER_ID, 
        PAYMENT_ID, PAY_PERIOD_ID, 
        BILL_ID, REP_PERIOD_ID, 
        TRANSFER_TOTAL, TRANSFER_DATE
    )
    SELECT p_crd_period_id PERIOD_ID, PK02_POID.NEXT_TRANSFER_ID TRANSFER_ID, 
           PAYMENT_ID, PAY_PERIOD_ID, 
           p_crd_bill_id, p_crd_period_id, -TRANSFER_TOTAL, SYSDATE TRANSFER_DATE 
      FROM PAY_TRANSFER_T T
     WHERE T.REP_PERIOD_ID = p_period_id
       AND T.BILL_ID = p_bill_id;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Insert '||v_count||' rows in PAYMENT_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --    
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- ------------------------------------------------------------------------ --
-- 5) Сторнирование цепочки разноски указанного платежа
-- ------------------------------------------------------------------------ --
PROCEDURE Revers_transfer_chain (
               p_period_id     IN INTEGER,
               p_payment_id    IN INTEGER
           )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Revers_transfer_chain';
    v_count   INTEGER := 0;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, payment_id=' || p_payment_id,
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- изменяем балансы счетов
    MERGE INTO BILL_T B
    USING (
      SELECT T.REP_PERIOD_ID, T.BILL_ID, SUM(T.TRANSFER_TOTAL) TRANSFER_TOTAL 
        FROM PAY_TRANSFER_T T
       WHERE T.PAY_PERIOD_ID = p_period_id
         AND T.PAYMENT_ID    = p_payment_id
       GROUP BY T.REP_PERIOD_ID, T.BILL_ID
    ) T
    ON (
       B.REP_PERIOD_ID = T.REP_PERIOD_ID AND
       B.BILL_ID = T.BILL_ID
    )
    WHEN MATCHED THEN UPDATE 
      SET B.DUE = B.DUE - T.TRANSFER_TOTAL, 
          B.DUE_DATE = SYSDATE;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    
    -- копируем разноски со знаком минус
    INSERT INTO PAY_TRANSFER_T (
      PERIOD_ID, TRANSFER_ID, PAYMENT_ID, PAY_PERIOD_ID, BILL_ID, REP_PERIOD_ID, 
      TRANSFER_TOTAL, TRANSFER_DATE
    )
    SELECT PERIOD_ID, TRANSFER_ID, PAYMENT_ID, PAY_PERIOD_ID, BILL_ID, REP_PERIOD_ID, 
           -TRANSFER_TOTAL, SYSDATE
      FROM PAY_TRANSFER_T T
     WHERE T.PAY_PERIOD_ID = p_period_id
       AND T.PAYMENT_ID    = p_payment_id
     ORDER BY T.REP_PERIOD_ID; 
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAY_TRANSFER_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
 
    -- изменяем баланс платежа
    UPDATE PAYMENT_T P
       SET P.BALANCE       = P.RECVD, 
           P.TRANSFERED    = 0, 
           P.LAST_MODIFIED = SYSDATE
     WHERE P.REP_PERIOD_ID = p_period_id
       AND P.PAYMENT_ID    = p_payment_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAYMENT_T '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;


-- ============================================================================
-- Проверка правильности разноски платежей
-- ============================================================================
-- проверка правильности проставления баланса платей
-- ------------------------------------------------------------------------ --
PROCEDURE Check_payments_balance (
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_payments_balance';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR 
      SELECT PAYMENT_ID, REP_PERIOD_ID, TRANSFERED, REFUND, RECVD, BALANCE 
        FROM PAYMENT_T P
      WHERE RECVD != (BALANCE + TRANSFERED + REFUND)
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
-- проверка соответствия операций разноски платежам
PROCEDURE Check_payments_transfer (
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_payments_transfer';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR 
        WITH PAY AS (
            SELECT PAYMENT_ID, PAY_PERIOD_ID, SUM(TRANSFER_TOTAL) TRANSFER_TOTAL,
                   COUNT(*) OP_NUM
              FROM PAY_TRANSFER_T
             GROUP BY PAYMENT_ID, PAY_PERIOD_ID
        )
        SELECT 
               CASE 
                 WHEN P.TRANSFERED != PAY.TRANSFER_TOTAL THEN 1
                 ELSE 0
               END ERR,
               P.PAYMENT_ID, PAY.PAY_PERIOD_ID, 
               P.RECVD, P.TRANSFERED, P.REFUND, P.BALANCE,
               PAY.TRANSFER_TOTAL, PAY.OP_NUM
          FROM PAY, PAYMENT_T P
         WHERE P.PAYMENT_ID = PAY.PAYMENT_ID 
           AND P.REP_PERIOD_ID = PAY.PAY_PERIOD_ID 
           -- 
           AND (P.TRANSFERED != PAY.TRANSFER_TOTAL)
        ORDER BY 1,3,2
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
-- проверка соответствия операций разноски счетам
PROCEDURE Check_payments_bills (
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_payments_bills';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR 
        WITH PAY AS (
            SELECT BILL_ID, REP_PERIOD_ID, 
                   SUM(TRANSFER_TOTAL) TRANSFER_TOTAL,
                   COUNT(*) OP_NUM
              FROM PAY_TRANSFER_T
             GROUP BY BILL_ID, REP_PERIOD_ID
        )
        SELECT B.BILL_ID, B.REP_PERIOD_ID, B.TOTAL, B.RECVD,
               PAY.TRANSFER_TOTAL 
          FROM PAY, BILL_T B
        WHERE B.BILL_ID = PAY.BILL_ID
          AND B.REP_PERIOD_ID = PAY.REP_PERIOD_ID
          AND B.RECVD != PAY.TRANSFER_TOTAL
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


END PK10_PAYMENTS_TRANSFER;
/
