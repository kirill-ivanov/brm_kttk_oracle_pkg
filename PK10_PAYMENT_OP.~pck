CREATE OR REPLACE PACKAGE PK10_PAYMENT_OP
IS
    --
    -- Пакет для работы с объектом "ПЛАТЕЖ", таблицы:
    -- payment_t, pay_transfer_t
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_PAYMENT_OP';
    -- ==============================================================================
   
    type t_refc is ref cursor;

    -- ---------------------------------------------------------------------
    -- id ПС (платежной системы) для проведения корректировок
    c_PAYSYSTEM_CORRECT_ID INTEGER := PK00_CONST.c_PAYSYSTEM_CORRECT_ID; -- 12;
    c_PAYSYSTEM_CORRECT_CODE VARCHAR2(14) := PK00_CONST.c_PAYSYSTEM_CORRECT_CODE; -- 'Корректировка';

    -- ========================================================================== --
    -- Операции над платежами
    -- ========================================================================== --
    -- ------------------------------------------------------------------------ --
    -- Откатить (удалить) платеж, 
    -- ------------------------------------------------------------------------ --
    -- при условии что период в котором поступил платеж еще не закрыт
    -- (если период закрыт, то уже сформирован аванс для отчетности)
    --   - при ошибке выставляет исключение
    PROCEDURE Delete_payment(
                   p_payment_id    IN INTEGER,   -- платеж
                   p_pay_period_id IN INTEGER,   -- ID периода платежа
                   p_app_user      IN VARCHAR2   -- пользователь приложения
               );

    -- ------------------------------------------------------------------------ --
    -- Операция сторнирования платежа
    -- ------------------------------------------------------------------------ --
    -- Сторнировать платеж с Л/С клиента (сумма сразу учитывается в балансе Л/С)
    --   - положительное - ID платежного документа (PAYMENT.PAYMENT_ID) в биллинге, 
    --   - при ошибке выставляет исключение
    --
    FUNCTION Revers_payment (
                   p_src_payment_id IN INTEGER,   -- ID сторнируемого платежа                        
                   p_src_period_id  IN INTEGER,   -- ID отчетного периода, когда был зарегистрирован платеж
                   p_dst_period_id  IN INTEGER,   -- ID отчетного периода, сторнирующего платежа
                   p_manager        IN VARCHAR2,  -- менеджер проводивший операцию
                   p_notes          IN VARCHAR2 DEFAULT NULL -- примечание к операции
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- Операция возврата денег с платежа
    -- ------------------------------------------------------------------------ --
    --   - положительное - ID платежного документа (PAYMENT.PAYMENT_ID) в биллинге, 
    --   - при ошибке выставляет исключение
    FUNCTION Refund (
                   p_src_payment_id IN INTEGER,   -- ID корректируемого платежа
                   p_src_period_id  IN INTEGER,   -- ID отчетного периода, когда был зарегистрирован платеж
                   p_dst_period_id  IN INTEGER,   -- ID отчетного периода, корректирующего платежа
                   p_value          IN NUMBER,    -- заявленная сумма возврата
                   p_date           IN DATE,      -- дата возврата платежа
                   p_manager        IN VARCHAR2,  -- менеджер проводивший операцию
                   p_notes          IN VARCHAR2 DEFAULT NULL -- примечание к операции
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- Перенести платеж с одного лицевого счета на другой
    --   - положительное - ID платежного документа (PAYMENT.PAYMENT_ID) в биллинге, 
    --   - при ошибке выставляет исключение
    --
    FUNCTION Move_payment (
                   p_src_payment_id IN INTEGER,  -- ID платежа источника
                   p_src_period_id  IN INTEGER,  -- ID отчетного периода источника
                   p_dst_account_id IN INTEGER,  -- ID платежа источника
                   p_dst_period_id  IN INTEGER,  -- ID отчетного периода источника
                   p_manager        IN VARCHAR2, -- менеджер проводивший операцию
                   p_notes          IN VARCHAR2 DEFAULT NULL -- примечание к операции
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- Список платежей по лицевому счету
    PROCEDURE Operation_list (
                   p_recordset OUT t_refc, 
                   p_date_from  IN DATE,
                   p_date_to    IN DATE 
               );


END PK10_PAYMENT_OP;
/
CREATE OR REPLACE PACKAGE BODY PK10_PAYMENT_OP
IS

-- ========================================================================== --
-- Операции над платежами
-- ========================================================================== --

-- ------------------------------------------------------------------------ --
-- Откатить платеж, 
-- при условии что период в котором поступил платеж еще не закрыт
-- (если период закрыт, то уже сформирован аванс для отчетности)
--   - при ошибке выставляет исключение
PROCEDURE Delete_payment(
               p_payment_id    IN INTEGER,   -- платеж
               p_pay_period_id IN INTEGER,   -- ID периода платежа
               p_app_user      IN VARCHAR2   -- пользователь приложения
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Delete_payment';
    v_paysystem_id   INTEGER;
    v_payment_date   DATE;
    v_doc_id         INTEGER;
    v_total          NUMBER;
    
BEGIN
    -- Платеж должен быть из открытого финансового периода 
    IF Pk04_Period.Is_closed(p_pay_period_id) = TRUE THEN
        Pk01_Syslog.raise_Exception( 'Платеж '||p_payment_id||' - принадлежит закрытому финансовому периоду: '||
                                      p_pay_period_id, c_PkgName||'.'||v_prcName );  
    END IF;

    -- Удаляем все операции разноски платежа
    PK10_PAYMENTS_TRANSFER.Delete_transfer_chain(p_pay_period_id ,p_payment_id);

    -- получаем данные об удаляемом платеже для отчета   
    SELECT P.PAYSYSTEM_ID, PAYMENT_DATE, DOC_ID, RECVD
      INTO v_paysystem_id, v_payment_date, v_doc_id, v_total
      FROM PAYMENT_T P
     WHERE PAYMENT_ID = p_payment_id
       AND REP_PERIOD_ID = p_pay_period_id;
    
    -- Удаляем платеж
    DELETE FROM PAYMENT_T 
     WHERE PAYMENT_ID = p_payment_id
       AND REP_PERIOD_ID = p_pay_period_id;
    
    -- Фиксируем факт удаления в системе логирования
    Pk01_Syslog.Write_msg(p_Msg => 'Удален платеж PAYSYSTEM_ID='||v_paysystem_id||
                                ', DOC_ID='||v_doc_id||
                                ', DATE='||TO_DATE(v_payment_date,'dd.mm.yyyy')||
                                ', TOTAL='||v_total,
                                p_Src => c_PkgName||'.'||v_prcName,
                                p_Level => Pk01_Syslog.L_info, p_AppUsr => p_app_user );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Операция сторнирования платежа
-- ------------------------------------------------------------------------ --
-- Сторнировать платеж с Л/С клиента (сумма сразу учитывается в балансе Л/С)
--   - положительное - ID платежного документа (PAYMENT.PAYMENT_ID) в биллинге, 
--   - при ошибке выставляет исключение
--
FUNCTION Revers_payment (
               p_src_payment_id IN INTEGER,   -- ID сторнируемого платежа                        
               p_src_period_id  IN INTEGER,   -- ID отчетного периода, когда был зарегистрирован платеж
               p_dst_period_id  IN INTEGER,   -- ID отчетного периода, сторнирующего платежа
               p_manager        IN VARCHAR2,  -- менеджер проводивший операцию
               p_notes          IN VARCHAR2 DEFAULT NULL -- примечание к операции
           ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Revers_transfer';
    r_payment        PAYMENT_T%ROWTYPE;
    v_dst_payment_id INTEGER;
		v_dst_doc_id		 VARCHAR2(100);
    v_advance        NUMBER;
    v_payment_id     INTEGER;
BEGIN
    v_dst_payment_id := PK02_POID.Next_payment_id;
    -- читаем данные сторнируемого платежа
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id
       AND REFUND = 0  -- два раза сторнировать нельзя
       AND STATUS != Pk00_Const.c_PAY_STATE_REVERS;
    -- получить номер транзакции для сторнирующего платежа
		v_dst_doc_id := pk02_poid.Next_Payment_Doc_Id();
    --    
    -- расчет аванса, для текущего периода компенсруем, для предыдущих - без изменений
    IF p_src_period_id = p_dst_period_id THEN
        v_advance := -r_payment.ADVANCE;
    ELSE
        v_advance := 0;
    END IF;
    --
    -- получить номер транзакции для сторнирующего платежа
    v_dst_doc_id := pk02_poid.Next_Payment_Doc_Id();
    v_payment_id := PK02_POID.Next_payment_id;
    --
    -- формируем сторнирующий платеж в текущем периоде со статусом "закрыт"
    -- и нулевым авансом и привязкой к нераспознанному платежу
    INSERT INTO PAYMENT_T (
        PAYMENT_ID, REP_PERIOD_ID, ACCOUNT_ID, 
        RECVD, ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED, 
        DATE_FROM, DATE_TO, PAYMENT_DATE, 
        PAYMENT_TYPE, PAYSYSTEM_ID, 
        DOC_ID, STATUS, STATUS_DATE, 
        CREATE_DATE, CREATED_BY, LAST_MODIFIED, 
        PAYSYSTEM_CODE, PAY_DESCR, 
        PREV_PAYMENT_ID, PREV_PERIOD_ID, 
        REFUND, MODIFIED_BY, NOTES, EXTERNAL_ID
    )VALUES(
        v_payment_id, p_dst_period_id, r_payment.ACCOUNT_ID,
        -r_payment.RECVD, 0, SYSDATE, 0, r_payment.TRANSFERED,
        r_payment.DATE_FROM, r_payment.DATE_TO, r_payment.PAYMENT_DATE,
        PK00_CONST.c_PAY_TYPE_REVERS, c_PAYSYSTEM_CORRECT_ID,
        v_dst_doc_id, PK00_CONST.c_PAY_STATE_CLOSE, SYSDATE,
        SYSDATE, p_manager, SYSDATE, 
        c_PAYSYSTEM_CORRECT_CODE, r_payment.PAY_DESCR,
        p_src_payment_id, p_src_period_id, 
        r_payment.REFUND, NULL, r_payment.NOTES, r_payment.EXTERNAL_ID
    );

    -- удаляем цепочку разноски платежа
    PK10_PAYMENTS_TRANSFER.Delete_transfer_chain (
             p_pay_period_id => p_src_period_id,
             p_payment_id    => p_src_payment_id
         );
      
    -- изменяем параметры сторнируемого платежа
    UPDATE PAYMENT_T
       SET BALANCE = 0,
           STATUS  = Pk00_Const.c_PAY_STATE_REVERS
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id;    

    -- откатываем изменение баланса лицевого счета на величину платежа
    UPDATE ACCOUNT_T
       SET BALANCE = BALANCE - r_payment.RECVD,
           BALANCE_DATE = SYSDATE  
     WHERE ACCOUNT_ID = r_payment.ACCOUNT_ID;

    -- фиксируем операцию сторнирования платежа
    INSERT INTO PAYMENT_OPERATION_T O (
        O.OPER_ID, O.OPER_TYPE_ID, O.OPER_DATE, O.OPER_TOTAL, 
        O.SRC_PAYMENT_ID, O.SRC_REP_PERIOD_ID,
        O.DST_PAYMENT_ID, O.DST_REP_PERIOD_ID,
        O.CREATED_BY, O.NOTES )
    VALUES(
        Pk02_Poid.Next_transfer_id, Pk00_Const.c_PAY_OP_REVERS, SYSDATE, r_payment.RECVD,
        p_src_payment_id, p_src_period_id, v_dst_payment_id, p_dst_period_id,
        p_manager, p_notes
    );
    --
    RETURN v_dst_payment_id;
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.Payment_id='||p_src_payment_id, c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Операция возврата денег с платежа
-- ------------------------------------------------------------------------ --
--   - положительное - ID платежного документа (PAYMENT.PAYMENT_ID) в биллинге, 
--   - при ошибке выставляет исключение
FUNCTION Refund (
               p_src_payment_id IN INTEGER,   -- ID корректируемого платежа
               p_src_period_id  IN INTEGER,   -- ID отчетного периода, когда был зарегистрирован платеж
               p_dst_period_id  IN INTEGER,   -- ID отчетного периода, корректирующего платежа
               p_value          IN NUMBER,    -- заявленная сумма возврата
               p_date           IN DATE,      -- дата возврата платежа
               p_manager        IN VARCHAR2,  -- менеджер проводивший операцию
               p_notes          IN VARCHAR2 DEFAULT NULL -- примечание к операции
           ) RETURN INTEGER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Refund';
    r_payment      PAYMENT_T%ROWTYPE;
    v_payment_id   INTEGER;
		v_doc_id			 PAYMENT_T.DOC_ID%TYPE;
BEGIN
 		v_doc_id := PK02_POID.Next_Payment_Doc_Id(c_PAYSYSTEM_CORRECT_ID);  

    -- читаем данные корректируемого платежа
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id;

    -- проверяем достаточно ли средств   
    IF r_payment.Balance < p_value THEN
        Pk01_Syslog.raise_Exception('Не достаточно средств для возврата. '||
                                    'Остаток на платеже '||r_payment.Balance||' руб, '||
                                    'запрос на возврат '||p_value||' руб, '||
                                    'PAYMENT_ID='||p_src_payment_id
                                    , c_PkgName||'.'||v_prcName );
    END IF;
    
    -- фиксируем возврат денег с платежа источника
    UPDATE PAYMENT_T P
       SET P.REFUND       = p_value,
           P.BALANCE      = P.BALANCE - p_value
     WHERE P.PAYMENT_ID   = p_src_payment_id 
       AND P.REP_PERIOD_ID= p_src_period_id;
    
    -- добавляем отрицательный платеж возврата 
    v_payment_id := PK10_PAYMENT.Add_payment (
              p_account_id      => r_payment.Account_Id,        -- ID лицевого счета клиента
              p_rep_period_id   => p_dst_period_id,             -- ID отчетного периода куда распределен платеж
              p_payment_datе    => p_date,                      -- дата платежа
              p_payment_type    => Pk00_Const.c_PAY_TYPE_REFUND,-- тип платежа возврат денег
              p_recvd           => -p_value,                    -- сумма платежа
              p_paysystem_id    => c_PAYSYSTEM_CORRECT_ID,      -- ID платежной системы
              p_doc_id          => v_doc_id,                    -- ID документа в платежной системе
              p_status          => Pk00_Const.c_PAY_STATE_OPEN, -- статус платежа
              p_manager    		  => p_manager,                   -- Ф.И.О. менеджера распределившего платеж на л/с
              p_notes           => p_notes                      -- примечание к платежу  
           );
           
    -- фиксируем операцию возврата средств с платежа
    INSERT INTO PAYMENT_OPERATION_T O (
        O.OPER_ID, O.OPER_TYPE_ID, O.OPER_DATE, O.OPER_TOTAL, 
        O.SRC_PAYMENT_ID, O.SRC_REP_PERIOD_ID,
        O.DST_PAYMENT_ID, O.DST_REP_PERIOD_ID,
        O.CREATED_BY, O.NOTES )
    VALUES(
        Pk02_Poid.Next_transfer_id, Pk00_Const.c_PAY_OP_REFUND, SYSDATE, r_payment.RECVD,
        p_src_payment_id, p_src_period_id, v_payment_id, p_dst_period_id,
        p_manager, p_notes
    );
    --
    RETURN v_payment_id;  
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.Payment_id='||p_src_payment_id, c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Перенести платеж с одного лицевого счета на другой
--   - положительное - ID платежного документа (PAYMENT.PAYMENT_ID) в биллинге, 
--   - при ошибке выставляет исключение
--
FUNCTION Move_payment (
               p_src_payment_id IN INTEGER,  -- ID платежа источника
               p_src_period_id  IN INTEGER,  -- ID отчетного периода источника
               p_dst_account_id IN INTEGER,  -- ID платежа источника
               p_dst_period_id  IN INTEGER,  -- ID отчетного периода источника
               p_manager        IN VARCHAR2, -- менеджер проводивший операцию
               p_notes          IN VARCHAR2 DEFAULT NULL -- примечание к операции
           ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Move_payment';
    r_payment        PAYMENT_T%ROWTYPE;
    v_rev_payment_id INTEGER;
    v_dst_payment_id INTEGER;
BEGIN
    -- читаем данные переносимого платежа
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id;

    -- сторнируем платеж источник
    v_rev_payment_id := Revers_payment (
               p_src_payment_id,  -- ID сторнируемого платежа                        
               p_src_period_id,   -- ID отчетного периода, когда был зарегистрирован платеж
               p_dst_period_id,   -- ID отчетного периода, сторнирующего платежа
               p_manager,         -- менеджер проводивший операцию
               p_notes            -- примечание к операции
           );
    -- добавляем платеж с переносом средств
    v_dst_payment_id := PK10_PAYMENT.Add_payment (
              p_account_id      => p_dst_account_id,           -- ID лицевого счета клиента
              p_rep_period_id   => p_dst_period_id,            -- ID отчетного периода куда распределен платеж
              p_payment_datе    => r_payment.Payment_Date,     -- дата платежа
              p_payment_type    => Pk00_Const.c_PAY_TYPE_MOVE, -- тип платежа
              p_recvd           => r_payment.Recvd,            -- сумма платежа
              p_paysystem_id    => c_PAYSYSTEM_CORRECT_ID,     -- ID платежной системы
              p_doc_id          => r_payment.Doc_Id,           -- ID документа в платежной системе
              p_status          => Pk00_Const.c_PAY_STATE_OPEN,-- статус платежа
              p_manager    		  => p_manager,  -- Ф.И.О. менеджера распределившего платеж на л/с
              p_notes           => p_notes     -- примечание к платежу  
           );
           
    UPDATE PAYMENT_T P
       SET P.PREV_PAYMENT_ID = p_src_payment_id,
           P.PREV_PERIOD_ID  = p_src_period_id
     WHERE PAYMENT_ID   = v_dst_payment_id 
       AND REP_PERIOD_ID= p_dst_period_id;
           
    -- фиксируем операцию сторнирования платежа
    INSERT INTO PAYMENT_OPERATION_T O (
        O.OPER_ID, O.OPER_TYPE_ID, O.OPER_DATE, O.OPER_TOTAL, 
        O.SRC_PAYMENT_ID, O.SRC_REP_PERIOD_ID,
        O.DST_PAYMENT_ID, O.DST_REP_PERIOD_ID,
        O.CREATED_BY, O.NOTES )
    VALUES(
        Pk02_Poid.Next_transfer_id, PK00_CONST.c_PAY_OP_MOVE, SYSDATE, r_payment.RECVD,
        p_src_payment_id, p_src_period_id, v_dst_payment_id, p_dst_period_id,
        p_manager, p_notes
    );
           
    RETURN v_dst_payment_id;  
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.Payment_id='||p_src_payment_id, c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Список платежей по лицевому счету
PROCEDURE Operation_list (
               p_recordset OUT t_refc, 
               p_date_from  IN DATE,
               p_date_to    IN DATE 
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Operation_list';
    v_retcode       INTEGER;
    v_min_period_id INTEGER;
    v_max_period_id INTEGER;
    v_date_from     DATE;
    v_date_to       DATE;
BEGIN
    -- выставляем границы диапазона
    IF p_date_from IS NOT NULL THEN
        v_date_from := p_date_from;
    ELSE
        v_date_from := TO_DATE('01.01.2000','dd.mm.yyyy');
    END IF;
    --
    IF p_date_to IS NOT NULL THEN
        v_date_to := p_date_to;
    ELSE
        v_date_to := SYSDATE+1;
    END IF;
    -- вычисляем границы сегмента, где хранятся данные платежа для указанного счета
    v_min_period_id := Pk04_Period.Period_id(v_date_from);
    v_max_period_id := Pk04_Period.Period_id(v_date_to);

    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
        SELECT O.OPER_ID, O.OPER_TYPE_ID, O.OPER_DATE, O.OPER_TOTAL, 
               O.SRC_PAYMENT_ID, O.SRC_REP_PERIOD_ID,
               O.DST_PAYMENT_ID, O.DST_REP_PERIOD_ID,
               O.CREATED_BY, O.NOTES
          FROM PAYMENT_OPERATION_T O    
         WHERE O.OPER_DATE BETWEEN v_date_from AND v_date_to
         ORDER BY O.OPER_ID DESC;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;  


END PK10_PAYMENT_OP;
/
