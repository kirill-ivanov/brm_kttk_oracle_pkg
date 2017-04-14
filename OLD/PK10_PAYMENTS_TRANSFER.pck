CREATE OR REPLACE PACKAGE PK10_PAYMENTS_TRANSFER
IS
    --
    -- Обслуживание массовой разноски платежей при выставлении счетов
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_PAYMENTS_TRANSFER';
    -- ==============================================================================
   
    type t_refc is ref cursor;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Процедура массовой разноски платежей методом FIFO
    -- до указанного периода включительно
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Method_fifo(p_To_Period_Id number);
    
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

    -- 5) Удаление операции разноски из цепочки
    PROCEDURE Delete_from_chain (
                   p_pay_period_id IN INTEGER,
                   p_payment_id    IN INTEGER,
                   p_transfer_id   IN INTEGER
               );
               
    -- 6) Удаление всей цепочки разноски платежей
    PROCEDURE Delete_transfer_chain (
                   p_pay_period_id IN INTEGER,
                   p_payment_id    IN INTEGER
               );
   

END PK10_PAYMENTS_TRANSFER;
/
CREATE OR REPLACE PACKAGE BODY PK10_PAYMENTS_TRANSFER
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Процедура массовой разноски платежей методом FIFO
-- до указанного периода включительно
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Method_fifo(p_To_Period_Id number)
IS
    v_prcName CONSTANT varchar2(16) := 'Method_fifo';
        
    l_Step       number;
    l_Upd_Bill   number;
    l_Upd_Payms  number;
    l_Ins_Transf number; 
    l_Bill_From  number;
    l_Bill_To    number;
    l_Pay_From   number;
    l_Pay_To     number;
BEGIN

    l_Step := 0;
        
    pin.Pk01_Syslog.Write_Msg(p_Msg => 'Start', p_Src => c_PkgName||'.'||v_prcName);                

    LOOP

        -- очищаем все промежуточные данные предыдущих расчетов
        EXECUTE IMMEDIATE 'TRUNCATE TABLE PK10_TRANSF_PAYMENTS_TMP DROP STORAGE';

        INSERT INTO PIN.PK10_TRANSF_PAYMENTS_TMP (
               account_id, payment_id, pay_period_id, 
               balance, payment_type, bill_id, bill_period_id, due, bill_date, 
               transfered, 
               curr_balance,
               rest_balance,  
               grp_rownum, 
               calc_date) 
        SELECT t.account_id, t.payment_id, t.pay_period_id, 
               t.balance, t.payment_type, t.bill_id, t.bill_period_id, t.due, t.bill_date,
               (CASE   
                    WHEN t.rest_balance < 0 THEN -- остатка не хватает покрыть весь счет, списываем остаток целиком
                         ABS(t.due) - ABS(rest_balance)
                    WHEN t.rest_balance >= 0 THEN 
                         ABS(t.due) -- т.е. если списать весь счет ещё что-то останется 
                END) transfered,
               NVL(lag(t.rest_balance) OVER (PARTITION BY t.payment_id, t.pay_period_id 
                                             ORDER BY t.grp_rownum)
                   , t.balance) curr_balance,                  
               t.rest_balance,
               t.grp_rownum,
               SYSDATE 
          FROM (
                SELECT p.account_id, 
                       p.payment_id, 
                       p.rep_period_id pay_period_id, 
                       p.balance,
                       p.payment_type,
                       b.bill_id, 
                       b.rep_period_id bill_period_id, 
                       b.due, b.bill_date,
                       -- получаем остаток после каждой из операций списания.
                       -- приоритет счетов, но которые раскладывается платеж:
                       -- 1. если сумма платежа равна сумме счёта (только для постоплаты)
                       -- 2. по времени счёта, т.е. более старые оплачиваются первыми
                       p.balance - SUM(ABS(b.due)) OVER (PARTITION BY p.payment_id, p.rep_period_id 
                                                             ORDER BY (CASE WHEN (p.balance + b.due) = 0 AND
                                                                                  p.rep_period_id >= b.rep_period_id
                                                                            THEN 0  
                                                                            ELSE b.rep_period_id 
                                                                       END),
                                                                       b.bill_id, b.rep_period_id) rest_balance,
                        row_number() OVER (PARTITION BY p.payment_id, p.rep_period_id 
                                               ORDER BY (CASE WHEN (p.balance + b.due) = 0 AND
                                                                    p.rep_period_id >= b.rep_period_id 
                                                              THEN 0  
                                                              ELSE b.rep_period_id 
                                                          END),
                                                          b.bill_id, b.rep_period_id) grp_rownum                                                                       
                  FROM (-- получаем платежи, у которых баланс > 0
                        SELECT P.ACCOUNT_ID, 
                               P.PAYMENT_ID, P.REP_PERIOD_ID, P.BALANCE, P.PAYMENT_TYPE,
                               row_number() OVER (PARTITION BY p.account_id ORDER BY p.rep_period_id ASC, p.payment_date ASC) rn 
                          FROM PAYMENT_T P, 
                               ACCOUNT_T A  
                         WHERE P.ACCOUNT_ID   = A.ACCOUNT_ID
                           AND P.BALANCE > 0
                           AND P.RECVD > 0 -- отрицательные платежи не разносим
                           AND P.REP_PERIOD_ID <= p_To_Period_Id
                       ) p,
                       BILL_T b
                 WHERE p.rn = 1 -- выбираем только самые первые платежи у л/счетов
                   AND B.ACCOUNT_ID   = P.ACCOUNT_ID
                   AND B.TOTAL   > 0  -- отсекаем секцию с пустыми счетами
                   AND B.DUE     < 0  -- есть непогашенная задолженность
                   AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_CLOSED, Pk00_Const.c_BILL_STATE_READY) -- 'CLOSED', 'READY' 
                   AND B.REP_PERIOD_ID <= p_To_Period_Id
              ) t
        WHERE t.due < t.rest_balance; -- выбираем только те счета, на которые хватит суммы выбранного платежа

        EXIT WHEN SQL%ROWCOUNT = 0;
        
        l_Step := l_Step + 1;
        
        -- проставляем id опреации переноса
        UPDATE PK10_TRANSF_PAYMENTS_TMP
           SET transfer_id = SQ_TRANSFER_ID.NEXTVAL
         WHERE transfer_id IS NULL;
             
        COMMIT; 
             
        dbms_stats.gather_table_stats(ownname => 'PIN',
                                      tabname => 'PK10_TRANSF_PAYMENTS_TMP',
                                      granularity => 'ALL',
                                      CASCADE => TRUE,
                                      no_invalidate => FALSE);         
             
        -- если платеж разносится на несколько счетов, то выстраиваем ссылками на платежи цепочку транзакций
        UPDATE PK10_TRANSF_PAYMENTS_TMP p
           SET p.prev_transfer_id = (SELECT pt.transfer_id
                                       FROM PK10_TRANSF_PAYMENTS_TMP pt
                                      WHERE pt.payment_id    = p.payment_id
                                        AND pt.pay_period_id = p.pay_period_id
                                        AND pt.grp_rownum    = p.grp_rownum - 1
                                    )   
         WHERE p.prev_transfer_id IS NULL; 
             
        COMMIT; 
             
        -- для ограничения партиций в дальнейшем
        SELECT MIN(bill_period_id), MAX(bill_period_id),
               MIN(pay_period_id), MAX(pay_period_id)
          INTO l_Bill_From, l_Bill_To,
               l_Pay_From, l_Pay_To
          FROM PK10_TRANSF_PAYMENTS_TMP;
                      
        -- обновляем данные по счетам
        MERGE INTO bill_t b
        USING (SELECT bill_id, bill_period_id, transfered
                 FROM PK10_TRANSF_PAYMENTS_TMP
              ) t
           ON (b.rep_period_id BETWEEN l_Bill_From AND l_Bill_To
                AND 
               b.bill_id = t.bill_id 
                AND
               b.rep_period_id = t.bill_period_id
              )
        WHEN MATCHED THEN UPDATE
         SET b.due   = b.due + t.transfered, -- остаток
             b.recvd = b.recvd + t.transfered;         -- сумма, оплаченная по данному счету
                 
        l_Upd_Bill := SQL%ROWCOUNT;     
                 
        -- добавляем записи об операциях разноски                    
        INSERT INTO PAY_TRANSFER_T (
               TRANSFER_ID, PREV_TRANSFER_ID,
               PAYMENT_ID, PAY_PERIOD_ID,
               BILL_ID, REP_PERIOD_ID,
               TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE,
               TRANSFER_DATE, NOTES)
        SELECT transfer_id, prev_transfer_id,
               payment_id, pay_period_id,
               bill_id, bill_period_id,
               transfered, curr_balance, curr_balance - transfered,
               calc_date, NULL 
          FROM PK10_TRANSF_PAYMENTS_TMP;                       
                    
        l_Ins_Transf := SQL%ROWCOUNT;  
              
        -- обновляем данные платежей  
        MERGE INTO payment_t p
        USING (SELECT payment_id, pay_period_id,
                      SUM(transfered) total_transf,
                      SUM((CASE WHEN t.pay_period_id < t.bill_period_id THEN transfered
                                ELSE 0
                           END)) advance, -- все что в будущие периоды - аванс      
                      MIN(bill_date) date_from, MAX(bill_date) date_to 
                 FROM PK10_TRANSF_PAYMENTS_TMP t
                GROUP BY payment_id, pay_period_id
              ) np
           ON (p.rep_period_id BETWEEN l_Pay_From AND l_Pay_To
                AND 
               p.payment_id = np.payment_id
                AND 
               p.rep_period_id = np.pay_period_id
              )
         WHEN MATCHED THEN UPDATE      
             SET p.balance    = p.balance - np.total_transf,
                 p.transfered = p.transfered + np.total_transf,
                 p.advance    = p.advance + np.advance, -- если за прошлые периоды уже есть авнсы, то суммируем
                 p.date_from  = LEAST(p.date_from, np.date_from), 
                 p.date_to    = GREATEST(p.date_to, np.date_to);        
                  
        l_Upd_Payms := SQL%ROWCOUNT;      
                  
        COMMIT;
        
        pin.Pk01_Syslog.Write_Msg(p_Msg => 'Step ' || TO_CHAR(l_Step) || ': ' ||
                                           'Upd.Bill: ' || TO_CHAR(l_Upd_Bill) ||    
                                           ', Ins.Transf: ' || TO_CHAR(l_Ins_Transf) || 
                                           ', Upd.Pay: ' || TO_CHAR(l_Upd_Payms),
                                   p_Src => c_PkgName||'.'||v_prcName);            
        
    END LOOP;              

    COMMIT;

    pin.Pk01_Syslog.Write_Msg(p_Msg => 'OK', p_Src => c_PkgName||'.'||v_prcName);

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
    MERGE INTO PAYMENT_T P
    USING (
        SELECT PAYMENT_ID, PAY_PERIOD_ID, SUM(TRANSFER_TOTAL) ADVANCE 
          FROM PAY_TRANSFER_T
        WHERE PAY_PERIOD_ID < REP_PERIOD_ID     -- авансовая разноска
        GROUP BY PAYMENT_ID, PAY_PERIOD_ID
    ) T
    ON (P.PAYMENT_ID = T.PAYMENT_ID AND P.REP_PERIOD_ID = T.PAY_PERIOD_ID)
    WHEN MATCHED THEN UPDATE SET P.ADVANCE = T.ADVANCE, 
         P.ADVANCE_DATE = ADD_MONTHS(TO_DATE(T.PAY_PERIOD_ID, 'yyyymm'),1)-1/86400;
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
      SELECT LEVEL LVL, 
          TRANSFER_ID, PAYMENT_ID, PAY_PERIOD_ID, BILL_ID, REP_PERIOD_ID, 
          TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE, 
          TRANSFER_DATE, PREV_TRANSFER_ID, NOTES
        FROM PAY_TRANSFER_T T
       WHERE T.PAYMENT_ID    = p_payment_id
         AND T.PAY_PERIOD_ID = p_pay_period_id
      CONNECT BY PRIOR TRANSFER_ID = PREV_TRANSFER_ID 
        START WITH PREV_TRANSFER_ID IS NULL
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
    v_open_balance     NUMBER;
    v_cloce_balance    NUMBER;
    --
    v_p_transfer_id    INTEGER;
    v_p_bill_id        INTEGER;
    v_p_rep_period_id  INTEGER;
    v_p_transfer_total NUMBER;
    v_p_open_balance   NUMBER;
    v_p_cloce_balance  NUMBER;
    --
    v_n_transfer_id    INTEGER;
    v_n_bill_id        INTEGER;
    v_n_rep_period_id  INTEGER;
    v_n_transfer_total NUMBER;
    v_n_open_balance   NUMBER;
    v_n_cloce_balance  NUMBER;
    
BEGIN
    --
    Pk01_Syslog.Write_msg('Start '|| 
                          'pay_period_id=' || p_pay_period_id||
                          ',payment_id='    || p_payment_id||
                          ',transfer_id='   || p_transfer_id,
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- Читаем данные удаляемой и смежных операциях
    SELECT
        --
        P.TRANSFER_ID P_TRANSFER_ID, P.BILL_ID P_BILL_ID, P.REP_PERIOD_ID P_REP_PERIOD_ID, 
        P.TRANSFER_TOTAL P_TRANSFER_TOTAL, P.OPEN_BALANCE P_OPEN_BALANCE, P.CLOSE_BALANCE P_CLOSE_BALANCE,
        --
        T.TRANSFER_ID, T.BILL_ID, T.REP_PERIOD_ID, 
        T.TRANSFER_TOTAL, T.OPEN_BALANCE, T.CLOSE_BALANCE, 
        --
        N.TRANSFER_ID N_TRANSFER_ID, N.BILL_ID N_BILL_ID, N.REP_PERIOD_ID N_REP_PERIOD_ID, 
        N.TRANSFER_TOTAL N_TRANSFER_TOTAL, N.OPEN_BALANCE N_OPEN_BALANCE, N.CLOSE_BALANCE N_CLOSE_BALANCE
        --
      INTO
        --
        v_p_transfer_id, v_p_bill_id, v_p_rep_period_id, 
        v_p_transfer_total, v_p_open_balance, v_p_cloce_balance,
        --
        v_transfer_id, v_bill_id, v_rep_period_id,
        v_transfer_total, v_open_balance, v_cloce_balance,
        --
        v_n_transfer_id, v_n_bill_id, v_n_rep_period_id,
        v_n_transfer_total, v_n_open_balance, v_n_cloce_balance
        --
      FROM PAY_TRANSFER_T T, PAY_TRANSFER_T P, PAY_TRANSFER_T N
     WHERE T.PAY_PERIOD_ID = p_pay_period_id
       AND T.PAYMENT_ID    = p_payment_id
       AND T.PAY_PERIOD_ID = P.PAY_PERIOD_ID(+) 
       AND T.PAYMENT_ID    = P.PAYMENT_ID(+)
       AND T.PREV_TRANSFER_ID = P.TRANSFER_ID(+) 
       AND T.PAY_PERIOD_ID = N.PAY_PERIOD_ID(+) 
       AND T.PAYMENT_ID    = N.PAYMENT_ID(+)
       AND T.TRANSFER_ID   = N.PREV_TRANSFER_ID(+);

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

    -- уточняем временные границы разноски
    SELECT MIN(B.BILL_DATE) DATE_FROM, MAX(B.BILL_DATE) DATE_TO
      INTO v_date_from, v_date_to
      FROM PAY_TRANSFER_T PT, BILL_T B
     WHERE PT.PAYMENT_ID    = p_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND PT.BILL_ID       = B.BILL_ID
       AND PT.REP_PERIOD_ID = B.REP_PERIOD_ID;

    -- изменяем поля платежа
    UPDATE PAYMENT_T P
      SET P.TRANSFERED    = (P.TRANSFERED - v_transfer_total),
          P.BALANCE       = (P.BALANCE + v_transfer_total),
          P.ADVANCE       = CASE
                            WHEN P.REP_PERIOD_ID > v_rep_period_id THEN (P.ADVANCE + v_transfer_total)
                            ELSE P.ADVANCE
                            END,
          P.LAST_MODIFIED = SYSDATE,
          P.DATE_FROM     = v_date_from,
          P.DATE_TO       = v_date_to
    WHERE P.REP_PERIOD_ID = p_pay_period_id
      AND P.PAYMENT_ID    = p_payment_id;
    Pk01_Syslog.Write_msg('payment_id='|| p_payment_id ||' - updated, '||
                      'transfer_total='|| v_transfer_total,
                      c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- если есть последующие записи 
    IF v_n_transfer_id IS NOT NULL THEN
       -- присоединяем следующую запись к предыдущей
       UPDATE PAY_TRANSFER_T N 
          SET N.PREV_TRANSFER_ID = v_p_transfer_id
       WHERE N.PAY_PERIOD_ID = p_pay_period_id
         AND N.PAYMENT_ID    = p_payment_id
         AND N.TRANSFER_ID   = v_n_transfer_id;
       
       -- изменяем величины для всех записей в цепочке ниже указанной
       v_count := 0;
       FOR rt IN (
          SELECT LEVEL LVL, ROWID ROW_ID,
              TRANSFER_ID, PAYMENT_ID, PAY_PERIOD_ID, BILL_ID, REP_PERIOD_ID, 
              TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE, 
              TRANSFER_DATE, PREV_TRANSFER_ID, NOTES
            FROM PAY_TRANSFER_T T
           WHERE T.PAY_PERIOD_ID = p_pay_period_id
             AND T.PAYMENT_ID    = p_payment_id
          CONNECT BY PRIOR TRANSFER_ID   = PREV_TRANSFER_ID 
            START WITH ( T.TRANSFER_ID   = v_n_transfer_id AND 
                         T.PAY_PERIOD_ID = p_pay_period_id AND 
                         T.PAYMENT_ID    = p_payment_id )
       )
       LOOP
         UPDATE PAY_TRANSFER_T T
            SET OPEN_BALANCE  = OPEN_BALANCE  - v_transfer_total, 
                CLOSE_BALANCE = CLOSE_BALANCE - v_transfer_total
          WHERE ROWID = rt.ROW_ID;
       END LOOP;
       Pk01_Syslog.Write_msg('Updated '||v_count||' rows in chain', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    END IF;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- ------------------------------------------------------------------------ --
-- 6) Удаление всей цепочки разноски платежей
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
    Pk01_Syslog.Write_msg('payment_id='|| p_payment_id ||' - updated set transfer = 0',
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
    Pk01_Syslog.Write_msg('pay_transfer_t '|| v_count ||' rows updated',
                        c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;



END PK10_PAYMENTS_TRANSFER;
/
