CREATE OR REPLACE PACKAGE PKXX_SVM_TEST
IS
    --
    -- Пакет для работы с объектом "ПЛАТЕЖ", таблицы:
    -- payment_t, pay_transfer_t
    -- --------------------------------------------------------------------------- --
    -- ОБЩИЕ ПРИНЦИПЫ РАБОТЫ :
    -- На первом этапе он-лайн платежи на не интересны, берем данные из реестра платежей
    -- 1) Для принятого платежа, определяется ACCOUNT_T.ACCOUNT_ID 
    --    задолженность которого он гасит, используются ф-ии семейства "Find_..."
    -- 2) Платеж регистрируется на найденном ACCOUNT_ID и его сумма сразу входит 
    --    в баланс Л/С ACCOUNT_T.BALANCE: ф-ия "Add_payment(...)"
    -- 3) Платеж или чать платежа гасит указанный выставленный счет (BILL_T.STATUS = 'CLOSED')
    --    полностью или частично: ф-ия Transfer_to_bill()
    -- 4) Платеж разносится по выставленным счетам (BILL_T.STATUS = 'CLOSED') методом
    --    FIFO: ф-ия "Transfer_to_account_fifo(...)"
    -- 5) Если после закрытия периода, в который поступил платеж, на нем остались
    --    средства - они фиксируются в поле PAYMENT_T.ADVANCE 
    --    с указанием даты PAYMENT_T.ADVANCE_DATE
    -- 6) Сторнирование
    -- 7) Корректировка
    --
    --    Таблицы :
    -- PAYMENT_T - содержит платеж с привязкой к лицевому счету на дату поступления платежа
    -- PAY_TRANSFER_T - содержит операции по разноске платежа PAYMENT_T на 
    --                  платежные позиции выставленных счетов (BILL_T.STATUS = 'CLOSED')
    -- ITEM_T - содержит позиции платежей ITEM(P) выставленных счетов, 
    --          на каждый счет - одна платежная позиция
    -- --------------------------------------------------------------------------- --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_PAYMENT';
    -- ==============================================================================
   
    type t_refc is ref cursor;
    
    -- ------------------------------------------------------------------------ --
    -- Добавить платеж на Л/С клиента (сумма сразу учитывается в балансе Л/С)
    --   - положительное - ID платежного документа (PAYMENT.PAYMENT_ID) в биллинге, 
    --   - при ошибке выставляет исключение
    --
    FUNCTION Add_payment (
                  p_account_id      IN INTEGER,   -- ID лицевого счета клиента
                  p_rep_period_id   IN INTEGER,   -- ID отчетного периода куда распределен платеж
                  p_payment_datе    IN DATE,      -- дата платежа
                  p_payment_type    IN VARCHAR2,  -- тип платежа
                  p_recvd           IN NUMBER,    -- сумма платежа
                  p_paysystem_id    IN INTEGER,   -- ID платежной системы
                  p_doc_id          IN VARCHAR2,  -- ID документа в платежной системе
                  p_status          IN VARCHAR2,  -- статус платежа
                  p_manager    		  IN VARCHAR2,  -- Ф.И.О. менеджера распределившего платеж на л/с
                  p_notes           IN VARCHAR2,  -- примечание к платежу  
									p_descr						IN VARCHAR2		DEFAULT NULL -- описание платежа
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- Перенос части (или всей суммы) платежа на ITEM(P) оплаты укзаноого 
    -- выставленного периодические счета,
    -- если позиции нет - она создается
    -- для закрытия задолженности по периодам, возвращает:
    --   > 0  - PAY_TRANSFER.TRANSFER_ID описатель операции разноски 
    --   NULL - попытка разнесения платежа: 
    --        * на ранее оплаченный счет
    --        * нет средств на платеже: p_open_balance = 0
    --        * p_total < 0 - так не должно быть
    --   - при ошибке выставляет исключение
    FUNCTION Transfer_to_bill(
                   p_payment_id    IN INTEGER,    -- ID платежа - источника средств
                   p_pay_period_id IN INTEGER,    -- ID отчетного периода куда распределен платеж
                   p_bill_id       IN INTEGER,    -- ID выставленного счета
                   p_rep_period_id IN INTEGER,    -- ID отчетного периода счета               
                   p_notes         IN VARCHAR2,   -- примечания к операции
                   p_value         IN NUMBER,     -- сумма которую хотим перенести, NULL - сколько нужно               
                   p_open_balance  OUT NUMBER,    -- сумма на платеже до проведения операции
                   p_close_balance OUT NUMBER,    -- сумма на платеже после проведения операции
                   p_bill_due      OUT NUMBER     -- оставшийся долг по счету после операции
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- Откатить последнюю операцию разноски платежа, 
    -- при условии что период в котором поступил платеж еще не закрыт
    -- (если период закрыт, то уже сформирован аванс для отчетности)
    --   - остаток неразнесенных средств на платеже 
    --   - при ошибке выставляет исключение
    FUNCTION Rollback_transfer(
                   p_transfer_id   IN INTEGER,   -- ID платежа
                   p_pay_period_id IN INTEGER    -- ID периода платежа
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- Откатить платеж, 
    -- при условии что период в котором поступил платеж еще не закрыт
    -- (если период закрыт, то уже сформирован аванс для отчетности)
    --   - остаток неразнесенных средств на платеже 
    --   - при ошибке выставляет исключение
    PROCEDURE Rollback_payment(
                   p_payment_id    IN INTEGER,   -- платеж
                   p_pay_period_id IN INTEGER,   -- ID периода платежа
                   p_app_user      IN VARCHAR2   -- пользователь приложения
               );

    -- ------------------------------------------------------------------------ --
    -- Сторнировать операцию разноски платежа, когда по каким-то причинам откатить нельзя
    -- при условии что период в котором поступил платеж еще не закрыт
    -- возвращает:
    --   - ID сторнирующей записи 
    --   - при ошибке выставляет исключение
    FUNCTION Revers_transfer(
                   p_transfer_id   IN INTEGER,   -- ID сторнируемой операции разноски платежа
                   p_pay_period_id IN INTEGER,   -- ID периода платежа
                   p_notes         IN VARCHAR2   -- примечание
               ) RETURN INTEGER;
               
    -- ------------------------------------------------------------------------ --
    -- Автоматическая разноска платежа методом FIFO на позиции (payment item) 
    -- выставленных ранее периодических счетов (item-ы не закрываем)
    -- для закрытия задолженности по периодам, возвращает:
    --   - остаток неразнесенных средств на платеже 
    --   - при ошибке выставляет исключение
    FUNCTION Transfer_to_account_fifo(
                   p_payment_id    IN INTEGER,  -- платеж
                   p_pay_period_id IN INTEGER,  -- ID отчетного периода счета
                   p_account_id    IN INTEGER   -- лицевой счет, счета которого погашаются
               ) RETURN NUMBER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- Разнести на сформированные счета все что осталось на платежах принятых 
    -- до указанного периода включительно.
    -- разноска внутри л/с по позициям баланс Л/С не изменяет
    -- разноска для 'Ф' автоматическая FIFO, 'Ю'-ручная через АРМ платежей
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Payment_processing_fifo( p_from_period_id IN INTEGER );

    -- ========================================================================== --
    -- Операции над платежами
    -- ========================================================================== --
    -- ------------------------------------------------------------------------ --
    -- Операция сторнирования платежа
    -- ------------------------------------------------------------------------ --
    -- Сторнировать платеж с Л/С клиента (сумма сразу учитывается в балансе Л/С)
    --   - положительное - ID платежного документа (PAYMENT.PAYMENT_ID) в биллинге, 
    --   - при ошибке выставляет исключение
    --
    FUNCTION OP_revers_payment (
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
    FUNCTION OP_refund (
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
    FUNCTION OP_move_payment (
                   p_src_payment_id IN INTEGER,  -- ID платежа источника
                   p_src_period_id  IN INTEGER,  -- ID отчетного периода источника
                   p_dst_account_id IN INTEGER,  -- ID платежа источника
                   p_dst_period_id  IN INTEGER,  -- ID отчетного периода источника
                   p_manager        IN VARCHAR2, -- менеджер проводивший операцию
                   p_notes          IN VARCHAR2 DEFAULT NULL -- примечание к операции
               ) RETURN INTEGER;
               
    -- ========================================================================== --
    -- Аванс
    -- ========================================================================== --
    -- ------------------------------------------------------------------------ --
    -- Фиксируем часть платежа как аванс
    --   - размер аванса
    --   - при ошибке выставляет исключение
    FUNCTION Fix_advance(
                   p_payment_id    IN INTEGER,   -- ID платежа
                   p_pay_period_id IN INTEGER    -- ID периода платежа
               ) RETURN NUMBER;

    -- ------------------------------------------------------------------------ --
    -- пересчитать авансовые составляющие платежей для указанного периода
    -- процедуру можно выполнять после закрытия биллингового периода и до закрытия финансового
    -- ВНИМАНИЕ: пересчитывать балансы закрытых финансовых периодов КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО!!!
    PROCEDURE Refresh_advance(
                   p_pay_period_id IN INTEGER    -- ID периода платежа
               );

    -- ========================================================================== --
    -- Ф-ии поиска
    -- ========================================================================== --
    -- ------------------------------------------------------------------------ --
    -- поиск лицевого счета по номеру телефона, возвращает:
    --   > 0 - ID лицевого счета в биллинге
    --   NULL - значение не найдено 
    --   - при ошибке выставляет исключение
    FUNCTION Find_account_by_phone (
                   p_phone         IN VARCHAR2,  -- номер телефона
                   p_date          IN DATE       -- дата на которую ищем соответствие
               ) RETURN INTEGER;
               
    -- поиск ID лицевого счета по номеру выставленного счета
    --   > 0 - положительное - ID лицевого счета в биллинге 
    --   NULL - зачение не найдено
    --   - при ошибке выставляет исключение
    FUNCTION Find_account_by_billno (
                   p_bill_no       IN VARCHAR2   -- номер выставленного счета
               ) RETURN INTEGER;
               
    -- поиск ID счета по номеру счета, возвращает:
    --   > 0 - положительное - ID счета в биллинге 
    --   NULL - зачение не найдено
    --   - при ошибке выставляет исключение
    FUNCTION Find_id_by_billno (
                   p_bill_no       IN VARCHAR2   -- номер счета
               ) RETURN INTEGER;

    -- поиск ID лицевого счета по номеру лицевого счета, возвращает:
    --   > 0 - ID лицевого счета в биллинге 
    --   NULL - зачение не найдено
    --   - при ошибке выставляет исключение
    FUNCTION Find_id_by_accountno (
                   p_account_no    IN VARCHAR2   -- номер телефона
               ) RETURN INTEGER;

    -- ========================================================================== --
    -- Ф-ии отображения
    -- ========================================================================== --
    -- ------------------------------------------------------------------------ --
    -- Список платежей по лицевому счету
    --   - при ошибке выставляет исключение
    PROCEDURE Account_payment_list (
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,   -- ID лицевого счета
                   p_date_from  IN DATE,
                   p_date_to    IN DATE 
               );

    -- ------------------------------------------------------------------------ --
    -- Список платежей по лицевому счету
    --   - при ошибке выставляет исключение
    PROCEDURE Account_payment_list (
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID лицевого счета
               p_period_id  IN INTEGER
           ); 

    -- ------------------------------------------------------------------------ --              
    -- Список оплат покрывающий счет
    --   - при ошибке выставляет исключение
    PROCEDURE Bill_pay_list (
                   p_recordset    OUT t_refc, 
                   p_bill_id       IN INTEGER,    -- ID платежа
                   p_rep_period_id IN INTEGER     -- ID отчетного периода счета
               );

    -- ------------------------------------------------------------------------ --
    -- Список оплат покрывающий счета за определенный период по определенному лицевому счету
    --   - при ошибке выставляет исключение
    PROCEDURE Bill_pay_list_by_account (
                   p_recordset    OUT t_refc, 
                   p_account_id   IN INTEGER,    -- ID лицевого счета
                   p_rep_period_id IN INTEGER     -- ID отчетного периода счета
               );  
            
    -- Просмотр разноски платежа по счетам
    --   - при ошибке выставляет исключение
    PROCEDURE Transfer_list (
                   p_recordset    OUT t_refc, 
                   p_payment_id    IN INTEGER,   -- ID платежа
                   p_pay_period_id IN INTEGER    -- ID отчетного периода счета
               );

    -- ------------------------------------------------------------------------ --
    -- Получить последнюю запись в цепочке разноски платежа
    -- возвращает:
    --  ID   - последней записи разноски
    --  NULL - если записей нет
    FUNCTION Get_transfer_tail (
                   p_payment_id    IN INTEGER,   -- ID корректируемого платежа
                   p_pay_period_id IN INTEGER    -- ID отчетного периода, когда был зарегистрирован платеж
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- Получить первую запись в цепочке разноски платежа
    -- возвращает:
    --  ID - последней записи разноски
    --  NULL - если записей нет
    FUNCTION Get_transfer_head (
                   p_payment_id    IN INTEGER,   -- ID корректируемого платежа
                   p_pay_period_id IN INTEGER    -- ID отчетного периода, когда был зарегистрирован платеж
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- Рассчет границ времени которые покрывает платеж
    --
    PROCEDURE Payment_bound_time (
                   p_payment_id    IN INTEGER,   -- ID корректируемого платежа
                   p_pay_period_id IN INTEGER    -- ID отчетного периода, когда был зарегистрирован платеж
               );

    --=========================================================================
    PROCEDURE xTTK_to_Saler;

END PKXX_SVM_TEST;
/
CREATE OR REPLACE PACKAGE BODY PKXX_SVM_TEST
IS

-- ---------------------------------------------------------------------
-- id ПС для корректировок
v_paysystem_correct NUMBER := 12;

--============================================================================================
--                  С Л У Ж Е Б Н Ы Е     П Р О Ц Е Д У Р Ы 
--============================================================================================
-- Собрать статистику по таблице
--
PROCEDURE Gather_Table_Stat(l_Tab_Name varchar2)
IS
    PRAGMA AUTONOMOUS_TRANSACTION; 
BEGIN 
    DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'PIN',
                                  TABNAME => l_Tab_Name,
                                  DEGREE  => 5,
                                  CASCADE => TRUE,
                                  NO_INVALIDATE => FALSE
                                 ); 
END;

--============================================================================================
PROCEDURE Run_DDL(p_ddl IN VARCHAR2) IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Run_DDL';

BEGIN
    EXECUTE IMMEDIATE p_ddl;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.Write_error('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  PAY_TRANSFER_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Pay_transfer_t_drop_fk
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Pay_transfer_t_drop_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Run_DDL('ALTER TABLE PIN.PAY_TRANSFER_T DROP CONSTRAINT PAY_TRANSFER_ID_BILL_T_FK');
    Run_DDL('ALTER TABLE PIN.PAY_TRANSFER_T DROP CONSTRAINT PAY_TRANSFER_ID_PAYMENT_T_FK');
    Run_DDL('ALTER TABLE PIN.PAY_TRANSFER_T DROP CONSTRAINT PAY_TRANSFER_T_FK');
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
PROCEDURE Pay_transfer_t_add_fk
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Pay_transfer_t_add_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    EXECUTE IMMEDIATE 'ALTER TABLE PIN.PAY_TRANSFER_T ADD (
      CONSTRAINT PAY_TRANSFER_ID_BILL_T_FK 
      FOREIGN KEY (BILL_ID, REP_PERIOD_ID) 
      REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
      ENABLE VALIDATE,
      CONSTRAINT PAY_TRANSFER_ID_PAYMENT_T_FK 
      FOREIGN KEY (PAYMENT_ID, PAY_PERIOD_ID) 
      REFERENCES PIN.PAYMENT_T (PAYMENT_ID,REP_PERIOD_ID)
      ENABLE VALIDATE,
      CONSTRAINT PAY_TRANSFER_T_FK 
      FOREIGN KEY (PREV_TRANSFER_ID, PAY_PERIOD_ID) 
      REFERENCES PIN.PAY_TRANSFER_T (TRANSFER_ID,PAY_PERIOD_ID)
      ENABLE VALIDATE)';
    --  
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;



-- ------------------------------------------------------------------------ --
-- Добавить платеж на Л/С клиента 
-- сумма сразу учитывается в балансе Л/С и учитывается в авнсе, 
-- разноска платежа по счетам, выставленным в период платежа или более ранние периоды 
-- будет уменьшать аванс.
--   - положительное - ID платежного документа (PAYMENT.PAYMENT_ID) в биллинге, 
--   - при ошибке выставляет исключение
--
FUNCTION Add_payment (
              p_account_id      IN INTEGER,   -- ID лицевого счета клиента
              p_rep_period_id   IN INTEGER,   -- ID отчетного периода куда распределен платеж
              p_payment_datе    IN DATE,      -- дата платежа
              p_payment_type    IN VARCHAR2,  -- тип платежа
              p_recvd           IN NUMBER,    -- сумма платежа
              p_paysystem_id    IN INTEGER,   -- ID платежной системы
              p_doc_id          IN VARCHAR2,  -- ID документа в платежной системе
              p_status          IN VARCHAR2,  -- статус платежа
              p_manager    		  IN VARCHAR2,  -- Ф.И.О. менеджера распределившего платеж на л/с
              p_notes           IN VARCHAR2,  -- примечание к платежу  
							p_descr						IN VARCHAR2	DEFAULT NULL	-- описание платежа
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_payment';
    v_payment_id  INTEGER;
BEGIN
    v_payment_id := PK02_POID.Next_payment_id;
    -- cохраняем информацию о платеже
    INSERT INTO PAYMENT_T (
        PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE,
        PAYMENT_DATE, ACCOUNT_ID, RECVD,
        ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED, REFUND,
        DATE_FROM, DATE_TO,
        PAYSYSTEM_ID, DOC_ID,
        STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED,
        CREATED_BY, NOTES, Pay_Descr
    )VALUES(
        v_payment_id, p_rep_period_id, p_payment_type,
        p_payment_datе, p_account_id, p_recvd,
        p_recvd, p_payment_datе, p_recvd, 0, 0,
        NULL, NULL,
        p_paysystem_id, p_doc_id,
        p_status, SYSDATE, SYSDATE, SYSDATE,
        p_manager, p_notes, p_descr
    );
    -- Изменяем баланс лицевого счета на величину платежа
    UPDATE ACCOUNT_T
       SET BALANCE = BALANCE + p_recvd,
           BALANCE_DATE = p_payment_datе  
     WHERE ACCOUNT_ID = p_account_id;
    --
    RETURN v_payment_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Перенос части (или всей суммы) платежа на укзаный выставленный счет
-- PS: тупая разноска, сколько указано - столько и перенесли думает о суммах тот кто инициирует операцию
-- разноска платежа по счетам, выставленным в период платежа или более ранние периоды 
-- будет уменьшать аванс,
-- возвращает:
--   - PAY_TRANSFER.TRANSFER_ID описатель операции разноски 
--   - NULL - если сумма разноски равна 0
--   - при ошибке выставляет исключение
FUNCTION Transfer_to_bill(
               p_payment_id    IN INTEGER,    -- ID платежа - источника средств
               p_pay_period_id IN INTEGER,    -- ID отчетного периода куда распределен платеж
               p_bill_id       IN INTEGER,    -- ID выставленного счета
               p_rep_period_id IN INTEGER,    -- ID отчетного периода счета               
               p_notes         IN VARCHAR2,   -- примечания к операции
               p_value         IN NUMBER,     -- сумма которую хотим перенести               
               p_open_balance  OUT NUMBER,    -- сумма на платеже до проведения операции
               p_close_balance OUT NUMBER,    -- сумма на платеже после проведения операции
               p_bill_due      OUT NUMBER     -- оставшийся долг по счету после операции
           ) RETURN INTEGER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Transfer_to_bill';
    v_transfer_id  INTEGER := NULL;
    v_bill_date    DATE;
    v_date_from    DATE; 
    v_date_to      DATE;
    v_advance      NUMBER;
    v_balance      NUMBER;
    v_transfered   NUMBER;
    v_payment_type PAYMENT_T.PAYMENT_TYPE%TYPE;
BEGIN
    -- начальная установка переменных
    p_open_balance := 0;
    p_close_balance:= 0;
    p_bill_due := 0;
    
    -- если сумма равна нулю, выходим
    IF p_value = 0 THEN
        RETURN NULL;
    END IF;
    
    -- получаем неразнесенный остаток на платеже (входящий остаток)
    -- Возможно нужен FOR_UPDATE для блокировки записи - потом пойму, не содаст ли блокировки
    -- при массовой разноске и commit в конце
    SELECT P.BALANCE, P.BALANCE, P.TRANSFERED, P.ADVANCE, 
           P.PAYMENT_TYPE, P.DATE_FROM, P.DATE_TO
      INTO p_open_balance, v_balance, v_transfered, v_advance, 
           v_payment_type, v_date_from, v_date_to
      FROM PAYMENT_T P
     WHERE P.REP_PERIOD_ID = p_pay_period_id
       AND P.PAYMENT_ID = p_payment_id;
    
    -- разносим сумму на выставленный счет 
    UPDATE BILL_T B 
       SET DUE   = DUE   + p_value, 
           RECVD = RECVD + p_value
     WHERE BILL_ID       = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
    RETURNING DUE, BILL_DATE INTO p_bill_due, v_bill_date;
    
    -- создаем операцию разноски платежа на счет и додавляем ее в хвост цепочки разноски
    v_transfer_id := Pk02_Poid.Next_transfer_id;
    --    
    p_close_balance := p_open_balance - p_value;
    --
    INSERT INTO PAY_TRANSFER_T (
           TRANSFER_ID, 
           PAYMENT_ID, PAY_PERIOD_ID,
           BILL_ID, REP_PERIOD_ID,
           TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE,
           TRANSFER_DATE, NOTES, PREV_TRANSFER_ID
    )
    SELECT v_transfer_id, 
           p_payment_id, p_pay_period_id,
           p_bill_id, p_rep_period_id, 
           p_value, p_open_balance, p_close_balance,
           SYSDATE, p_notes, 
           MAX(TRANSFER_ID) -- чтобы исключение NO_DATA_FOUND не выскочило
      FROM PAY_TRANSFER_T PT
     WHERE PT.PAYMENT_ID    = p_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND NOT EXISTS (
           SELECT *
            FROM PAY_TRANSFER_T T
           WHERE T.PAYMENT_ID       = PT.PAYMENT_ID
             AND T.PAY_PERIOD_ID    = PT.PAY_PERIOD_ID
             AND T.PREV_TRANSFER_ID = PT.TRANSFER_ID
       )
    ;
    -- уточняем временные границы разноски
    IF p_value > 0 THEN   -- при отрицательных суммах диапазон дат без изменений
        IF v_bill_date < v_date_from THEN
            v_date_from := v_bill_date;
        ELSIF v_date_to < v_bill_date THEN
            v_date_to := v_bill_date;
        END IF;
    END IF;
    /*
    -- способ честный, но тяжелый для массовой загрузки    
    SELECT MIN(B.BILL_DATE), MAX(B.BILL_DATE)
      INTO v_date_from, v_date_to
      FROM PAY_TRANSFER_T PT, BILL_T B
     WHERE PT.PAYMENT_ID    = p_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND PT.BILL_ID       = B.BILL_ID
       AND PT.REP_PERIOD_ID = B.REP_PERIOD_ID;
    */
    -- вычисляем изменения на платеже
    v_balance    := v_balance - p_value;
    v_transfered := v_transfered + p_value;

    -- аванс изменям только при разноске на счета в текущем и предыдущих периодах
    -- для платежей незакрытых периодов 
    IF v_payment_type IN (PK00_CONST.c_PAY_TYPE_ADJUST, PK00_CONST.c_PAY_TYPE_REVERS) THEN
        v_advance := 0;
    ELSIF p_pay_period_id >= p_rep_period_id AND Pk04_Period.Is_closed(p_pay_period_id) = FALSE THEN
        v_advance:= v_advance - p_value;
    END IF;
    -- изменяем данные в описании платежа
    UPDATE PAYMENT_T 
       SET BALANCE   = v_balance,
           TRANSFERED= v_transfered,
           ADVANCE   = v_advance,
           DATE_FROM = v_date_from, 
           DATE_TO   = v_date_to
     WHERE PAYMENT_ID= p_payment_id
       AND REP_PERIOD_ID = p_pay_period_id;

    -- возвращаем ID операции разноски
    RETURN v_transfer_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Откатить последнюю операцию разноски платежа, 
-- при условии что период в котором поступил платеж еще не закрыт
-- (если период закрыт, то уже сформирован аванс для отчетности)
--   - ID предыдущей операции разноски 
--   - при ошибке выставляет исключение
FUNCTION Rollback_transfer(
               p_transfer_id   IN INTEGER,   -- ID платежа
               p_pay_period_id IN INTEGER    -- ID периода платежа
           ) RETURN INTEGER
IS
    v_prcName          CONSTANT VARCHAR2(30) := 'Rollback_transfer';
    v_payment_id       INTEGER;
    v_bill_id          INTEGER;
    v_rep_period_id    INTEGER;
    v_open_balance     NUMBER := 0; -- сумма на платеже до проведения операции
    v_transfer_total   NUMBER := 0; -- сумма операции разноски платежа
    v_advance_back     NUMBER := 0; -- возврат на аванс при откате операции разноски 
    v_count            INTEGER:= 0;
    v_date_from        DATE;
    v_date_to          DATE;
    v_prev_transfer_id INTEGER;
BEGIN
    -- финансовый период не должен быть закрыт  
    IF Pk04_Period.Is_closed(p_pay_period_id) = TRUE THEN
        Pk01_Syslog.raise_Exception( 'Платеж принадлежит закрытому финансовому периоду: '||
                                      p_pay_period_id, c_PkgName||'.'||v_prcName );
    END IF;
    
    -- удаляемая операция должна быть последняя в цепочке разноски
    SELECT COUNT(*)
      INTO v_count
      FROM PAY_TRANSFER_T
     WHERE PREV_TRANSFER_ID = p_transfer_id
       AND REP_PERIOD_ID = p_pay_period_id;
    IF v_count > 0 THEN
        Pk01_Syslog.raise_Exception( 'Операция '||p_transfer_id||' - не последняя в цепочке разноски платежа'||
                                      p_pay_period_id, c_PkgName||'.'||v_prcName );
    END IF;
 
    -- получаем данные операции разноски
    SELECT PT.BILL_ID, PT.REP_PERIOD_ID, PT.PREV_TRANSFER_ID,
           PT.OPEN_BALANCE, PT.TRANSFER_TOTAL, PT.PAYMENT_ID
      INTO v_bill_id, v_rep_period_id, v_prev_transfer_id,
           v_open_balance, v_transfer_total, v_payment_id
      FROM PAY_TRANSFER_T PT
     WHERE PT.TRANSFER_ID = p_transfer_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id;

    -- изменяем задолженность по счету, тоже с полюсом
    UPDATE BILL_T B 
       SET DUE     = DUE     + v_transfer_total,
           RECVD   = RECVD   - v_transfer_total
     WHERE BILL_ID = v_bill_id
       AND REP_PERIOD_ID = v_rep_period_id;

    -- удаляем операцию разноски
    DELETE FROM PAY_TRANSFER_T
     WHERE TRANSFER_ID = p_transfer_id
       AND PAY_PERIOD_ID = p_pay_period_id;
     
    -- уточняем временные границы разноски
    SELECT MIN(B.BILL_DATE), MAX(B.BILL_DATE)
      INTO v_date_from, v_date_to
      FROM PAY_TRANSFER_T PT, BILL_T B
     WHERE PT.PAYMENT_ID    = v_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND PT.BILL_ID       = B.BILL_ID
       AND PT.REP_PERIOD_ID = B.REP_PERIOD_ID;

    -- аванс откатываем только для операций разноски на счета в текущем и предыдущих периодах
    -- для платежей незакрытых периодов 
    IF p_pay_period_id >= v_rep_period_id AND Pk04_Period.Is_closed(p_pay_period_id) = FALSE THEN
        v_advance_back := v_transfer_total;
    ELSE
        v_advance_back := 0;
    END IF;

    -- возвращаем деньги на платеж
    UPDATE PAYMENT_T 
       SET BALANCE   = v_open_balance,
           TRANSFERED= TRANSFERED - v_transfer_total,
           ADVANCE   = ADVANCE + v_advance_back,
           DATE_FROM = v_date_from, 
           DATE_TO   = v_date_to,
           LAST_MODIFIED = SYSDATE
     WHERE PAYMENT_ID = v_payment_id
       AND REP_PERIOD_ID = p_pay_period_id;

    RETURN v_prev_transfer_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Откатить платеж, 
-- при условии что период в котором поступил платеж еще не закрыт
-- (если период закрыт, то уже сформирован аванс для отчетности)
--   - остаток неразнесенных средств на платеже 
--   - при ошибке выставляет исключение
PROCEDURE Rollback_payment(
               p_payment_id    IN INTEGER,   -- платеж
               p_pay_period_id IN INTEGER,   -- ID периода платежа
               p_app_user      IN VARCHAR2   -- пользователь приложения
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Rollback_payment';
    v_value          INTEGER;
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
    FOR c_transfer IN (
            SELECT TRANSFER_ID
              FROM PAY_TRANSFER_T
             WHERE PAYMENT_ID = p_payment_id
               AND PAY_PERIOD_ID = p_pay_period_id
            ORDER BY TRANSFER_ID DESC 
        )
    LOOP
        v_value := Rollback_transfer( c_transfer.transfer_id, p_pay_period_id );
    END LOOP;

    -- получаем данные об удаляемом платеже    
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
-- Сторнировать операцию разноски платежа, когда по каким-то причинам откатить нельзя
-- при условии что период в котором поступил платеж еще не закрыт
-- возвращает:
--   - ID сторнирующей записи 
--   - при ошибке выставляет исключение
FUNCTION Revers_transfer(
               p_transfer_id   IN INTEGER,   -- ID сторнируемой операции разноски платежа
               p_pay_period_id IN INTEGER,   -- ID периода платежа
               p_notes         IN VARCHAR2   -- примечание
           ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Rollback_transfer';
    v_payment_id     INTEGER;
    v_bill_id        INTEGER;
    v_rep_period_id  INTEGER;
    v_transfer_total NUMBER := 0; -- сумма операции разноски платежа
    v_open_balance   NUMBER := 0; -- сумма на платеже до проведения операции
    v_close_balance  NUMBER := 0;
    v_bill_due       NUMBER := 0;
    v_transfer_id    INTEGER;
BEGIN
    -- финансовый период не должен быть закрыт  
    IF Pk04_Period.Is_closed(p_pay_period_id) = TRUE THEN
        Pk01_Syslog.raise_Exception( 'Платеж принадлежит закрытому финансовому периоду: '||
                                      p_pay_period_id, c_PkgName||'.'||v_prcName );
    END IF;  

    -- получаем данные операции разноски, которую нужно сторнировать
    SELECT PT.BILL_ID, PT.REP_PERIOD_ID, PT.OPEN_BALANCE, PT.TRANSFER_TOTAL, PT.PAYMENT_ID
      INTO v_bill_id, v_rep_period_id, v_open_balance, v_transfer_total, v_payment_id
      FROM PAY_TRANSFER_T PT
     WHERE PT.TRANSFER_ID   = p_transfer_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id;
       
    v_transfer_id := Transfer_to_bill(
               p_payment_id    => v_payment_id,    -- ID платежа - источника средств
               p_pay_period_id => p_pay_period_id, -- ID отчетного периода куда распределен платеж
               p_bill_id       => v_bill_id,       -- ID выставленного счета
               p_rep_period_id => v_rep_period_id, -- ID отчетного периода счета               
               p_notes         => p_notes,         -- примечания к операции
               p_value         => -v_transfer_total,-- сумма которую хотим сторнировать
               p_open_balance  => v_open_balance,  -- сумма на платеже до проведения операции
               p_close_balance => v_close_balance, -- сумма на платеже после проведения операции
               p_bill_due      => v_bill_due       -- оставшийся долг по счету после операции
           );

    RETURN v_transfer_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Автоматическая разноска платежа методом FIFO на позиции (payment item) 
-- выставленных ранее периодических счетов (item-ы не закрываем)
-- для закрытия задолженности по периодам, возвращает:
--   - остаток неразнесенных средств на платеже 
--   - при ошибке выставляет исключение
FUNCTION Transfer_to_account_fifo(
               p_payment_id    IN INTEGER,  -- платеж
               p_pay_period_id IN INTEGER,  -- ID отчетного периода счета
               p_account_id    IN INTEGER   -- лицевой счет, счета которого погашаются
           ) RETURN NUMBER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Transfer_to_account_fifo';
    v_transfer_id   INTEGER;
    v_value         NUMBER := 0; -- сумма которую хотим перенести, NULL - сколько нужно               
    v_open_balance  NUMBER := 0; -- сумма на платеже до проведения операции
    v_close_balance NUMBER := 0; -- сумма на платеже после проведения операции
    v_bill_due      NUMBER := 0; -- оставшийся долг по счету после операции
BEGIN
    -- Получаем информацию о платеже до начала разноски
    SELECT P.BALANCE INTO v_value
      FROM PAYMENT_T P
     WHERE P.PAYMENT_ID    = p_payment_id
       AND P.REP_PERIOD_ID = p_pay_period_id;

    -- Получаем список (FIFO) выставленных, но неоплаченных счетов
    FOR r_bill IN ( 
        SELECT BILL_ID, REP_PERIOD_ID, DUE 
          FROM BILL_T
         WHERE ACCOUNT_ID = p_account_id
           AND TOTAL > 0         -- отсекаем секцию с пустыми счетами
           AND DUE   < 0         -- есть непогашенная задолженность
           AND BILL_STATUS IN (PK00_CONST.c_BILL_STATE_CLOSED, PK00_CONST.c_BILL_STATE_READY)
         ORDER BY BILL_DATE )
    LOOP
       -- расчитываем сумму разноски
       IF (v_value + r_bill.due) > 0 THEN  -- средств хватает на покрытие долга по счету
           v_value := -r_bill.due;
       END IF; 
       -- разносим платеж на неоплаченные счета в порядке их выставления
       v_transfer_id := Transfer_to_bill(
               p_payment_id    => p_payment_id,   -- ID платежа - источника средств
               p_pay_period_id => p_pay_period_id,-- ID отчетного периода куда распределен платеж
               p_bill_id       => r_bill.bill_id, -- ID выставленного счета
               p_rep_period_id => r_bill.rep_period_id, -- ID отчетного периода счета
               p_notes         => NULL,           -- примечания к операции
               p_value         => v_value,        -- сумма которую хотим перенести, NULL - сколько нужно
               p_open_balance  => v_open_balance, -- сумма на платеже до проведения операции
               p_close_balance => v_close_balance,-- сумма на платеже после проведения операции
               p_bill_due      => v_bill_due      -- оставшийся долг по счету после операции
           );
       EXIT WHEN v_close_balance <= 0;            -- меньше для страховки
       -- готовимся к следующему циклу
       v_value := v_close_balance;
       --
    END LOOP; 
    RETURN v_close_balance;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
-- Разнести на сформированные счета все что осталось на платежах принятых 
-- до указанного периода включительно.
-- разноска внутри л/с по позициям баланс Л/С не изменяет
-- разноска для 'Ф' автоматическая FIFO, 'Ю'-ручная через АРМ платежей
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Payment_processing_fifo( p_from_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Payment_processing_fifo';
    v_ok            INTEGER;    
    v_err           INTEGER;
    v_zero          INTEGER;
    v_count         INTEGER;
    v_transfer_id   INTEGER;
    v_value         NUMBER := 0; -- сумма которую хотим перенести, NULL - сколько нужно               
    v_open_balance  NUMBER := 0; -- сумма на платеже до проведения операции
    v_close_balance NUMBER := 0; -- сумма на платеже после проведения операции
    v_bill_due      NUMBER := 0; -- оставшийся долг по счету после операции
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, from period_id <= '||p_from_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_ok  := 0;    
    v_err := 0;
    v_zero:= 0;
    
    -- исправление ошибок
    UPDATE PAYMENT_T P SET REP_PERIOD_ID = SUBSTR(REP_PERIOD_ID,1,6);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAYMENT_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    
    Gather_Table_Stat(l_Tab_Name => 'BILL_T');
    Gather_Table_Stat(l_Tab_Name => 'PAYMENT_T');
    Gather_Table_Stat(l_Tab_Name => 'PAY_TRANSFER_T');
    --
    Pay_transfer_t_drop_fk;
    --
    -- разносим авансы прошлых периодов на подготовленные в биллинговом периоде счета Физ. лиц
    FOR r_pay IN (
        SELECT P.ACCOUNT_ID, 
               P.PAYMENT_ID, P.REP_PERIOD_ID PAY_PERIOD_ID, 
               B.BILL_ID, B.REP_PERIOD_ID  
          FROM PAYMENT_T P, BILL_T B, ACCOUNT_T A
         WHERE 1=1 --A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_P
           AND P.ACCOUNT_ID   = A.ACCOUNT_ID
           AND B.ACCOUNT_ID   = A.ACCOUNT_ID
           AND P.BALANCE > 0
           AND B.TOTAL   > 0  -- отсекаем секцию с пустыми счетами
           AND B.DUE     < 0  -- есть непогашенная задолженность
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_CLOSED, Pk00_Const.c_BILL_STATE_READY)
           AND P.REP_PERIOD_ID <= p_from_period_id 
           AND B.REP_PERIOD_ID <= p_from_period_id
        ORDER BY B.BILL_DATE, P.PAYMENT_DATE
      )
    LOOP
        SAVEPOINT X;  -- точка сохранения данных для лицевого счета
        BEGIN
            -- Получаем информацию о платеже до начала разноски
            SELECT P.BALANCE INTO v_value
              FROM PAYMENT_T P
             WHERE P.PAYMENT_ID    = r_pay.payment_id
               AND P.REP_PERIOD_ID = r_pay.pay_period_id;
            
            -- могли уже все средства с платежа разнести
            IF v_value > 0 THEN
                -- Получаем информацию о счете до начала разноски
                SELECT B.DUE INTO v_bill_due
                  FROM BILL_T B
                 WHERE B.BILL_ID       = r_pay.bill_id
                   AND B.REP_PERIOD_ID = r_pay.rep_period_id;
                -- счет уже могли закрыть платежами
                IF v_bill_due < 0 THEN
                    -- расчитываем сумму разноски
                    IF (v_value + v_bill_due) > 0 THEN  -- средств хватает на покрытие долга по счету
                        v_value := -v_bill_due;         -- и еще останется
                    END IF;
                    -- разносим платеж (или часть) на неоплаченный счет
                    v_transfer_id := Transfer_to_bill(
                             p_payment_id    => r_pay.payment_id,   -- ID платежа - источника средств
                             p_pay_period_id => r_pay.pay_period_id,-- ID отчетного периода куда распределен платеж
                             p_bill_id       => r_pay.bill_id,      -- ID выставленного счета
                             p_rep_period_id => r_pay.rep_period_id,-- ID отчетного периода счета
                             p_notes         => NULL,               -- примечания к операции
                             p_value         => v_value,        -- сумма которую хотим перенести, NULL - сколько нужно
                             p_open_balance  => v_open_balance, -- сумма на платеже до проведения операции
                             p_close_balance => v_close_balance,-- сумма на платеже после проведения операции
                             p_bill_due      => v_bill_due      -- оставшийся долг по счету после операции
                         );
                    v_ok := v_ok + 1;         -- разноска произведена успешно
                END IF;   
            ELSE
                -- платеж уже полностью разнесен
                v_zero := v_zero + 1;
                /*
                Pk01_Syslog.Write_msg(
                   p_Msg  => 'account_id='  ||r_pay.account_id
                          || ', period_id=' ||r_pay.rep_period_id
                          || ', payment_id='||r_pay.payment_id
                          || ', bill_id='   ||r_pay.bill_id 
                          || ' - payment already transfered',
                   p_Src  => c_PkgName||'.'||v_prcName,
                   p_Level=> Pk01_Syslog.L_err );
                */
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
              -- откат изменений для лицевого счета
              ROLLBACK TO X;
              -- фиксируем ошибку в системе логирования
              Pk01_Syslog.Write_msg(
                 p_Msg  => 'account_id='  ||r_pay.account_id
                        || ', period_id=' ||r_pay.rep_period_id
                        || ', payment_id='||r_pay.payment_id 
                        || ' - error',
                 p_Src  => c_PkgName||'.'||v_prcName,
                 p_Level=> Pk01_Syslog.L_err );
              v_err := v_err + 1;
        END;  
        -- диагностика выполнения
        IF MOD((v_ok+v_err+v_zero), 500) = 0 THEN
            Pk01_Syslog.Write_msg('Processed: '||v_ok||'-ok, '||v_err||'-err, '||v_zero||'-empty', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        --
    END LOOP;
    
    Pk01_Syslog.Write_msg('Gather_Table_Stat', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        
    Gather_Table_Stat(l_Tab_Name => 'PAY_TRANSFER_T');
    --
    Pay_transfer_t_add_fk;
    --
    COMMIT;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ========================================================================== --
-- Операции над платежами
-- ========================================================================== --
-- ------------------------------------------------------------------------ --
-- Операция сторнирования платежа
-- ------------------------------------------------------------------------ --
-- Сторнировать платеж с Л/С клиента (сумма сразу учитывается в балансе Л/С)
--   - положительное - ID платежного документа (PAYMENT.PAYMENT_ID) в биллинге, 
--   - при ошибке выставляет исключение
--
FUNCTION OP_revers_payment (
               p_src_payment_id IN INTEGER,   -- ID сторнируемого платежа                        
               p_src_period_id  IN INTEGER,   -- ID отчетного периода, когда был зарегистрирован платеж
               p_dst_period_id  IN INTEGER,   -- ID отчетного периода, сторнирующего платежа
               p_manager        IN VARCHAR2,  -- менеджер проводивший операцию
               p_notes          IN VARCHAR2 DEFAULT NULL -- примечание к операции
           ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Revers_transfer';
    r_payment        PAYMENT_T%ROWTYPE;
    v_transfer_id    INTEGER;
    v_dst_payment_id INTEGER;
		v_dst_doc_id		 VARCHAR2(100);
    v_advance        NUMBER;
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
    -- формируем сторнирующий платеж в текущем периоде со статусом "закрыт"
    INSERT INTO PAYMENT_T (
        PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE,
        PAYMENT_DATE, ACCOUNT_ID, RECVD,
        ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED, REFUND,
        DATE_FROM, DATE_TO,
        PAYSYSTEM_ID, DOC_ID,
        STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED,
        CREATED_BY, NOTES
    )VALUES(
        v_dst_payment_id, p_dst_period_id, PK00_CONST.c_PAY_TYPE_REVERS, 
        r_payment.PAYMENT_DATE, r_payment.ACCOUNT_ID, -r_payment.RECVD,
        v_advance, SYSDATE, -r_payment.BALANCE, -r_payment.TRANSFERED, -r_payment.REFUND,
        r_payment.DATE_FROM, r_payment.DATE_TO,
        v_paysystem_correct, v_dst_doc_id,
        PK00_CONST.c_PAY_STATE_CLOSE, SYSDATE, SYSDATE, SYSDATE,
        p_manager, p_notes
    );
    --
    -- сторнируем операции разноски платежа в обратном порядке
    FOR r_trn IN (
       SELECT T.TRANSFER_ID, T.PAY_PERIOD_ID 
         FROM PAY_TRANSFER_T T
        WHERE T.PAYMENT_ID     = r_payment.PAYMENT_ID
           AND T.PAY_PERIOD_ID = r_payment.REP_PERIOD_ID
         ORDER BY T.TRANSFER_ID DESC  
        -- 
        -- честный способ при корректной связке между операциями разноски:
        -- SELECT TRANSFER_ID, PAY_PERIOD_ID 
        -- FROM (
        --   SELECT LEVEL LVL, T.TRANSFER_ID, T.PAY_PERIOD_ID 
        --     FROM PAY_TRANSFER_T T
        --    WHERE T.PAYMENT_ID    = r_payment.PAYMENT_ID
        --      AND T.PAY_PERIOD_ID = r_payment.REP_PERIOD_ID
        --   CONNECT BY PRIOR TRANSFER_ID = PREV_TRANSFER_ID 
        --   START WITH PREV_TRANSFER_ID IS NULL
        -- )ORDER BY 1 DESC
        --       
      )
    LOOP
        v_transfer_id := Revers_transfer(
               p_transfer_id   => r_trn.transfer_id,   -- ID сторнируемой операции разноски платежа
               p_pay_period_id => r_trn.pay_period_id, -- ID периода платежа
               p_notes         => NULL                 -- примечание
           ); 
    END LOOP;
    
    -- фиксируе выставляем возврат денег и проставляем статус "сторнирован"
    UPDATE PAYMENT_T SET REFUND = RECVD, STATUS = Pk00_Const.c_PAY_STATE_REVERS
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
FUNCTION OP_refund (
               p_src_payment_id IN INTEGER,   -- ID корректируемого платежа
               p_src_period_id  IN INTEGER,   -- ID отчетного периода, когда был зарегистрирован платеж
               p_dst_period_id  IN INTEGER,   -- ID отчетного периода, корректирующего платежа
               p_value          IN NUMBER,    -- заявленная сумма возврата
               p_date           IN DATE,      -- дата возврата платежа
               p_manager        IN VARCHAR2,  -- менеджер проводивший операцию
               p_notes          IN VARCHAR2 DEFAULT NULL -- примечание к операции
           ) RETURN INTEGER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'OP_refund';
    r_payment      PAYMENT_T%ROWTYPE;
    v_payment_id   INTEGER;
		v_doc_id			 VARCHAR2(100) := pk02_poid.Next_Payment_Doc_Id(v_paysystem_correct);
BEGIN
    -- читаем данные корректируемого платежа
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id;

    -- проверяем бостаточно ли средств   
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
    v_payment_id := Add_payment (
              p_account_id      => r_payment.Account_Id,        -- ID лицевого счета клиента
              p_rep_period_id   => p_dst_period_id,             -- ID отчетного периода куда распределен платеж
              p_payment_datе    => p_date,                      -- дата платежа
              p_payment_type    => Pk00_Const.c_PAY_TYPE_REFUND,-- тип платежа возврат денег
              p_recvd           => -p_value,                    -- сумма платежа
              p_paysystem_id    => v_paysystem_correct,         -- ID платежной системы
              p_doc_id          => v_doc_id,                    -- ID документа в платежной системе
              p_status          => Pk00_Const.c_PAY_STATE_OPEN, -- статус платежа
              p_manager    		  => p_manager,                   -- Ф.И.О. менеджера распределившего платеж на л/с
              p_notes           => p_notes                      -- примечание к платежу  
           );
           
    -- фиксируем операцию сторнирования платежа
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
FUNCTION OP_move_payment (
               p_src_payment_id IN INTEGER,  -- ID платежа источника
               p_src_period_id  IN INTEGER,  -- ID отчетного периода источника
               p_dst_account_id IN INTEGER,  -- ID платежа источника
               p_dst_period_id  IN INTEGER,  -- ID отчетного периода источника
               p_manager        IN VARCHAR2, -- менеджер проводивший операцию
               p_notes          IN VARCHAR2 DEFAULT NULL -- примечание к операции
           ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'OP_move_payment';
    r_payment        PAYMENT_T%ROWTYPE;
    v_rev_payment_id INTEGER;
    v_payment_id     INTEGER;
BEGIN
    -- читаем данные переносимого платежа
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id;

    -- сторнируем платеж источник
    v_rev_payment_id := OP_revers_payment (
               p_src_payment_id,  -- ID сторнируемого платежа                        
               p_src_period_id,   -- ID отчетного периода, когда был зарегистрирован платеж
               p_dst_period_id,   -- ID отчетного периода, сторнирующего платежа
               p_manager,         -- менеджер проводивший операцию
               p_notes            -- примечание к операции
           );
    -- добавляем платеж с переносом средств
    v_payment_id := Add_payment (
              p_account_id      => p_dst_account_id,           -- ID лицевого счета клиента
              p_rep_period_id   => p_dst_period_id,            -- ID отчетного периода куда распределен платеж
              p_payment_datе    => r_payment.Payment_Date,     -- дата платежа
              p_payment_type    => Pk00_Const.c_PAY_TYPE_MOVE, -- тип платежа
              p_recvd           => r_payment.Recvd,            -- сумма платежа
              p_paysystem_id    => r_payment.paysystem_id,     -- ID платежной системы
              p_doc_id          => r_payment.Doc_Id,           -- ID документа в платежной системе
              p_status          => Pk00_Const.c_PAY_STATE_OPEN,-- статус платежа
              p_manager    		  => p_manager,  -- Ф.И.О. менеджера распределившего платеж на л/с
              p_notes           => p_notes     -- примечание к платежу  
           );
           
    -- фиксируем операцию сторнирования платежа
    INSERT INTO PAYMENT_OPERATION_T O (
        O.OPER_ID, O.OPER_TYPE_ID, O.OPER_DATE, O.OPER_TOTAL, 
        O.SRC_PAYMENT_ID, O.SRC_REP_PERIOD_ID,
        O.DST_PAYMENT_ID, O.DST_REP_PERIOD_ID,
        O.CREATED_BY, O.NOTES )
    VALUES(
        Pk02_Poid.Next_transfer_id, PK00_CONST.c_PAY_OP_MOVE, SYSDATE, r_payment.RECVD,
        p_src_payment_id, p_src_period_id, v_payment_id, p_dst_period_id,
        p_manager, p_notes
    );
           
    RETURN v_payment_id;  
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.Payment_id='||p_src_payment_id, c_PkgName||'.'||v_prcName );
END;

--==========================================================================--
-- Фиксируем часть платежа как аванс
--   - размер аванса
--   - при ошибке выставляет исключение
FUNCTION Fix_advance(
               p_payment_id    IN INTEGER,   -- ID платежа
               p_pay_period_id IN INTEGER    -- ID периода платежа
           ) RETURN NUMBER
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Fix_advance';
    v_advance  NUMBER;
BEGIN
    UPDATE PAYMENT_T P
       SET ADVANCE = BALANCE,
           ADVANCE_DATE = SYSDATE
     WHERE P.PAYMENT_ID = p_payment_id
       AND P.REP_PERIOD_ID = p_pay_period_id
    RETURNING ADVANCE INTO v_advance; 
    RETURN v_advance;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--
-- пересчитать авансовые составляющие платежей для указанного периода
-- процедуру можно выполнять после закрытия биллингового периода и до закрытия финансового
-- ВНИМАНИЕ: пересчитывать балансы закрытых финансовых периодов КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО!!!
PROCEDURE Refresh_advance(
               p_pay_period_id IN INTEGER    -- ID периода платежа
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Refresh_advance';
BEGIN
    MERGE INTO PAYMENT_T P
    USING (
        SELECT REP_PERIOD_ID, PAYMENT_ID, SUM(TRANSFER_TOTAL) TRANSFER_TOTAL 
          FROM PAY_TRANSFER_T
         WHERE REP_PERIOD_ID <= PAY_PERIOD_ID
           AND PAY_PERIOD_ID = p_pay_period_id
        GROUP BY REP_PERIOD_ID, PAYMENT_ID
    ) PT
    ON (PT.REP_PERIOD_ID = P.REP_PERIOD_ID AND PT.PAYMENT_ID = P.PAYMENT_ID)
    WHEN MATCHED THEN 
      UPDATE SET P.ADVANCE = P.RECVD - PT.TRANSFER_TOTAL, 
                 P.ADVANCE_DATE = SYSDATE;  
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--==========================================================================--
-- поиск лицевого счета по номеру телефона (закрытые телефоны не исключаем, оплата может запоздать)
--   > 0 - ID лицевого счета в биллинге
--   NULL - значение не найдено 
--   - при ошибке выставляет исключение
FUNCTION Find_account_by_phone (
               p_phone         IN VARCHAR2,  -- номер телефона
               p_date          IN DATE       -- дата на которую ищем соответствие
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_account_by_phone';
    v_account_id INTEGER;
BEGIN
    BEGIN
        -- ищем в открытых телефонах
        SELECT O.ACCOUNT_ID INTO v_account_id
          FROM ORDER_T O, ORDER_PHONES_T R
         WHERE R.ORDER_ID = O.ORDER_ID
           AND R.PHONE_NUMBER = p_phone
           AND R.DATE_FROM <= p_date
           AND (R.DATE_TO IS NULL OR p_date < R.DATE_TO);
        RETURN v_account_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- если не нашли в открытых, ищем последний закрытый
            SELECT ACCOUNT_ID INTO v_account_id
            FROM (
                SELECT R.PHONE_NUMBER, O.ACCOUNT_ID, R.DATE_FROM, R.DATE_TO, 
                       MAX(R.DATE_TO) OVER (PARTITION BY R.PHONE_NUMBER) MAX_DATE_TO
                  FROM ORDER_T O, ORDER_PHONES_T R
                 WHERE R.ORDER_ID = O.ORDER_ID
                   AND R.PHONE_NUMBER = p_phone
            )
            WHERE DATE_TO = MAX_DATE_TO;  
            RETURN v_account_id;
    END;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
         RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --    
-- поиск ID лицевого счета по номеру выставленного счета
--   > 0  - положительное - ID лицевого счета в биллинге 
--   NULL - зачение не найдено
--   - при ошибке выставляет исключение
FUNCTION Find_account_by_billno (
               p_bill_no       IN VARCHAR2   -- номер выставленного счета
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_account_by_billno';
    v_account_id INTEGER;
BEGIN
    SELECT ACCOUNT_ID INTO v_account_id
      FROM BILL_T
     WHERE BILL_NO = p_bill_no;
    RETURN v_account_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- поиск ID счета по номеру выставленного счета
--   > 0  - положительное - ID счета в биллинге 
--   NULL - зачение не найдено
--   - при ошибке выставляет исключение
FUNCTION Find_id_by_billno (
               p_bill_no       IN VARCHAR2   -- номер выставленного счета
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_id_by_billno';
    v_bill_id    INTEGER;
BEGIN
    SELECT BILL_ID INTO v_bill_id
      FROM BILL_T
     WHERE BILL_NO = p_bill_no;
    RETURN v_bill_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- поиск ID лицевого счета по номеру лицевого счета
--   > 0  - ID лицевого счета в биллинге 
--   NULL - зачение не найдено
--   - при ошибке выставляет исключение
FUNCTION Find_id_by_accountno (
               p_account_no    IN VARCHAR2   -- номер телефона
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_id_by_accountno';
    v_account_id INTEGER;
BEGIN
    SELECT ACCOUNT_ID INTO v_account_id
      FROM ACCOUNT_T
     WHERE ACCOUNT_NO = p_account_no;
    RETURN v_account_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- Откат операции разноски на позицию c которой пришли деньги, возвращает
--   > 0  - ITEM_ID PAYMENT или TRANSFER в биллинге 
--   - при ошибке выставляет исключение
--FUNCTION Rollback_transfer() RETURN INTEGER;

-- ------------------------------------------------------------------------ --
-- Список платежей по лицевому счету
--   - при ошибке выставляет исключение
PROCEDURE Account_payment_list (
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID лицевого счета
               p_date_from  IN DATE,
               p_date_to    IN DATE 
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Account_payment_list';
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
          SELECT PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE, PAYMENT_DATE, 
                 ACCOUNT_ID, RECVD, ADVANCE, ADVANCE_DATE, 
                 BALANCE, TRANSFERED, DATE_FROM, DATE_TO, 
                 PS.PAYSYSTEM_ID, PS.PAYSYSTEM_NAME, DOC_ID,
                 STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED, 
								 P.CREATED_BY, P.MODIFIED_BY,
                 P.NOTES 
           FROM PAYMENT_T P, PAYSYSTEM_T PS
          WHERE ACCOUNT_ID = p_account_id
            AND REP_PERIOD_ID BETWEEN v_min_period_id AND v_max_period_id
            AND P.PAYSYSTEM_ID = PS.PAYSYSTEM_ID
          ORDER BY PAYMENT_ID DESC;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;  

-- ------------------------------------------------------------------------ --
-- Список платежей по лицевому счету
--   - при ошибке выставляет исключение
PROCEDURE Account_payment_list (
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID лицевого счета
               p_period_id  IN INTEGER
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Account_payment_list';
    v_retcode       INTEGER;
BEGIN   
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
          SELECT PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE, PAYMENT_DATE, 
                 ACCOUNT_ID, RECVD, ADVANCE, ADVANCE_DATE, 
                 BALANCE, TRANSFERED, DATE_FROM, DATE_TO, 
                 PS.PAYSYSTEM_ID, PS.PAYSYSTEM_NAME, DOC_ID,
                 STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED, 
								 P.CREATED_BY, P.MODIFIED_BY,
                 P.NOTES 
           FROM PAYMENT_T P, PAYSYSTEM_T PS
          WHERE ACCOUNT_ID = p_account_id
            AND REP_PERIOD_ID = p_period_id
            AND P.PAYSYSTEM_ID = PS.PAYSYSTEM_ID
          ORDER BY PAYMENT_ID DESC;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- Список оплат покрывающий счет
--   - положительное - кол-во выбранных записей
--   - при ошибке выставляет исключение
PROCEDURE Bill_pay_list (
               p_recordset    OUT t_refc, 
               p_bill_id       IN INTEGER,    -- ID платежа
               p_rep_period_id IN INTEGER     -- ID отчетного периода счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bill_pay_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
          SELECT B.BILL_ID, B.BILL_NO, B.REP_PERIOD_ID BILL_REP_PERIOD_ID, B.RECVD BILL_RECVD,
                 PT.TRANSFER_ID, PT.TRANSFER_TOTAL, PT.TRANSFER_DATE,
                 PT.OPEN_BALANCE, PT.CLOSE_BALANCE,
                 P.PAYMENT_ID, P.DOC_ID,P.RECVD,P.REP_PERIOD_ID,P.PAYMENT_TYPE,P.NOTES, P.PAYMENT_DATE, P.PAYSYSTEM_ID, PS.PAYSYSTEM_NAME
            FROM PAY_TRANSFER_T PT, PAYMENT_T P, PAYSYSTEM_T PS,BILL_T B
           WHERE B.BILL_ID       = p_bill_id
             AND B.REP_PERIOD_ID = p_rep_period_id
             AND B.BILL_ID       = PT.BILL_ID
             AND B.REP_PERIOD_ID = PT.REP_PERIOD_ID
             AND PT.PAYMENT_ID   = P.PAYMENT_ID
             AND PT.PAY_PERIOD_ID= P.REP_PERIOD_ID
             AND PS.PAYSYSTEM_ID = P.PAYSYSTEM_ID
          ORDER BY PT.TRANSFER_ID;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- Список оплат покрывающий счета за определенный период по определенному лицевому счету
--   - при ошибке выставляет исключение
PROCEDURE Bill_pay_list_by_account (
               p_recordset    OUT t_refc, 
               p_account_id   IN INTEGER,    -- ID лицевого счета
               p_rep_period_id IN INTEGER     -- ID отчетного периода счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bill_pay_list_by_account';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
          SELECT B.BILL_ID, B.BILL_NO,B.REP_PERIOD_ID BILL_REP_PERIOD_ID, B.RECVD,
                 PT.TRANSFER_ID, PT.TRANSFER_TOTAL, PT.TRANSFER_DATE,
                 PT.OPEN_BALANCE, PT.CLOSE_BALANCE,
                 P.PAYMENT_ID, P.DOC_ID,P.RECVD,P.REP_PERIOD_ID,P.PAYMENT_TYPE,P.NOTES, P.PAYMENT_DATE, P.PAYSYSTEM_ID, PS.PAYSYSTEM_NAME
            FROM PAY_TRANSFER_T PT, PAYMENT_T P, PAYSYSTEM_T PS,BILL_T B
           WHERE B.BILL_ID       = PT.BILL_ID
             AND B.REP_PERIOD_ID = PT.REP_PERIOD_ID
             AND PT.PAYMENT_ID   = P.PAYMENT_ID
             AND PT.PAY_PERIOD_ID= P.REP_PERIOD_ID
             AND PS.PAYSYSTEM_ID = P.PAYSYSTEM_ID
             AND B.ACCOUNT_ID    = p_account_id
             AND B.REP_PERIOD_ID = p_rep_period_id
          ORDER BY PT.TRANSFER_ID;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- Просмотр разноски платежа по счетам
--   - положительное - кол-во выбранных записей
--   - при ошибке выставляет исключение
PROCEDURE Transfer_list (
               p_recordset    OUT t_refc, 
               p_payment_id    IN INTEGER,   -- ID платежа
               p_pay_period_id IN INTEGER    -- ID отчетного периода счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Transfer_list';
    v_retcode    INTEGER;
BEGIN
    -- честный способ:
    --   SELECT LEVEL LVL, T.TRANSFER_ID, T.PAY_PERIOD_ID 
    --     FROM PAY_TRANSFER_T T
    --    WHERE T.PAYMENT_ID    = r_payment.PAYMENT_ID
    --      AND T.PAY_PERIOD_ID = r_payment.REP_PERIOD_ID
    --   CONNECT BY PRIOR TRANSFER_ID = PREV_TRANSFER_ID 
    --   START WITH PREV_TRANSFER_ID IS NULL

    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
          SELECT B.BILL_ID, B.BILL_NO, B.BILL_DATE, B.RECVD,
                 PT.TRANSFER_ID, PT.TRANSFER_TOTAL, PT.TRANSFER_DATE,
                 PT.OPEN_BALANCE, PT.CLOSE_BALANCE,
                 P.PAYMENT_DATE, P.PAYSYSTEM_ID, PS.PAYSYSTEM_NAME
            FROM BILL_T B, PAY_TRANSFER_T PT, PAYMENT_T P, PAYSYSTEM_T PS
           WHERE B.BILL_ID       = PT.BILL_ID
             AND B.REP_PERIOD_ID = PT.REP_PERIOD_ID
             AND PT.PAYMENT_ID   = P.PAYMENT_ID
             AND PT.PAY_PERIOD_ID= P.REP_PERIOD_ID
             AND P.PAYMENT_ID    = p_payment_id
             AND P.REP_PERIOD_ID = p_pay_period_id
             AND PS.PAYSYSTEM_ID = P.PAYSYSTEM_ID
          ORDER BY PT.TRANSFER_ID;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- Получить последнюю запись в цепочке разноски платежа
-- возвращает:
--  ID   - последней записи разноски
--  NULL - если записей нет
FUNCTION Get_transfer_tail (
               p_payment_id    IN INTEGER,   -- ID корректируемого платежа
               p_pay_period_id IN INTEGER    -- ID отчетного периода, когда был зарегистрирован платеж
           ) RETURN INTEGER
IS

    v_transfer_id INTEGER;
BEGIN
    SELECT PT.TRANSFER_ID INTO v_transfer_id
      FROM PAY_TRANSFER_T PT
     WHERE PT.PAYMENT_ID    = p_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND NOT EXISTS (
           SELECT *
            FROM PAY_TRANSFER_T T
           WHERE T.PAYMENT_ID       = PT.PAYMENT_ID
             AND T.PAY_PERIOD_ID    = PT.PAY_PERIOD_ID
             AND T.PREV_TRANSFER_ID = PT.TRANSFER_ID
       )
    ;
    RETURN v_transfer_id;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN NULL;
END;

-- ------------------------------------------------------------------------ --
-- Получить первую запись в цепочке разноски платежа
-- возвращает:
--  ID - последней записи разноски
--  NULL - если записей нет
FUNCTION Get_transfer_head (
               p_payment_id    IN INTEGER,   -- ID корректируемого платежа
               p_pay_period_id IN INTEGER    -- ID отчетного периода, когда был зарегистрирован платеж
           ) RETURN INTEGER
IS

    v_transfer_id INTEGER;
BEGIN
    SELECT PT.TRANSFER_ID INTO v_transfer_id
      FROM PAY_TRANSFER_T PT
     WHERE PT.PAYMENT_ID    = p_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND PT.PREV_TRANSFER_ID IS NULL;
    RETURN v_transfer_id;
    RETURN v_transfer_id;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN NULL;
END;

-- ------------------------------------------------------------------------ --
-- Рассчет границ времени которые покрывает платеж
--
PROCEDURE Payment_bound_time (
               p_payment_id    IN INTEGER,   -- ID корректируемого платежа
               p_pay_period_id IN INTEGER    -- ID отчетного периода, когда был зарегистрирован платеж
           )
IS
BEGIN
    MERGE INTO PAYMENT_T P
    USING (
      SELECT PT.PAYMENT_ID, PT.PAY_PERIOD_ID, 
             MIN(B.BILL_DATE) DATE_FROM , MAX(B.BILL_DATE) DATE_TO
        FROM PAY_TRANSFER_T PT, BILL_T B
       WHERE PT.PAYMENT_ID    = p_payment_id
         AND PT.PAY_PERIOD_ID = p_pay_period_id
         AND PT.BILL_ID       = B.BILL_ID
         AND PT.REP_PERIOD_ID = B.REP_PERIOD_ID
       GROUP BY PT.PAYMENT_ID, PT.PAY_PERIOD_ID
    ) T
    ON (P.PAYMENT_ID = T.PAYMENT_ID AND P.REP_PERIOD_ID = T.PAY_PERIOD_ID )
    WHEN MATCHED THEN UPDATE SET P.DATE_FROM = T.DATE_FROM, P.DATE_TO = T.DATE_TO;
END;

--=========================================================================
PROCEDURE xTTK_to_Saler
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'xTTK_to_Saler';
    v_profile_id    INTEGER;
    v_count         INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    FOR c IN (
      SELECT S.BRAND, LL.CONTRACTOR_ID, 
             LL.SELLER_ID, LL.SELLER_BANK_ID, LL.SELLER_REGION_ID,
             AP.PROFILE_ID, AP.ACCOUNT_ID, AP.DATE_FROM, AP.DATE_TO, 
             B.BILL_ID, B.BILL_NO
        FROM SVM_XTTK_TO_SALER_T S, CONTRACT_T C, ACCOUNT_PROFILE_T AP, BILL_T B,--, CONTRACTOR_T CR
             LL_CONTRACTOR_SELLER_MIGRATION LL
       WHERE S.CONTRACT_NO = C.CONTRACT_NO
         AND AP.CONTRACT_ID = C.CONTRACT_ID
         AND B.REP_PERIOD_ID(+) = 201503
         AND B.PROFILE_ID(+) = AP.PROFILE_ID
         AND B.ACCOUNT_ID(+) = AP.ACCOUNT_ID
         AND S.BRAND = LL.CONTRACTOR_NAME
         ORDER BY BRAND
    )
    LOOP
        -- закрываем старый профиль
        UPDATE ACCOUNT_PROFILE_T AP SET DATE_TO = TO_DATE('28.02.2015 23:59:59','dd.mm.yyyy hh24:mi:ss')
         WHERE AP.PROFILE_ID = c.Profile_Id
           AND (AP.DATE_TO IS NULL OR AP.DATE_TO >= TO_DATE('01.03.2015','dd.mm.yyyy'))
        ;
        -- создаем новый профиль
        v_profile_id := SQ_ACCOUNT_ID.NEXTVAL;
        
        INSERT INTO ACCOUNT_PROFILE_T (
           PROFILE_ID, ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, SUBSCRIBER_ID, 
           CONTRACTOR_ID, BRANCH_ID, AGENT_ID, CONTRACTOR_BANK_ID, VAT, 
           DATE_FROM, DATE_TO, CUSTOMER_PAYER_ID, BRAND_ID
        )
        SELECT v_profile_id, ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, SUBSCRIBER_ID, 
               c.SELLER_ID CONTRACTOR_ID, BRANCH_ID, AGENT_ID, 
               c.SELLER_BANK_ID CONTRACTOR_BANK_ID, VAT, 
               TO_DATE('01.03.2015','dd.mm.yyyy') DATE_FROM, NULL DATE_TO, 
               CUSTOMER_PAYER_ID, BRAND_ID 
          FROM ACCOUNT_PROFILE_T
          WHERE PROFILE_ID = c.profile_id
        ;

        -- изменяем поля счета  
        IF c.BILL_ID IS NOT NULL THEN
          
          UPDATE BILL_T B 
             SET B.CONTRACTOR_ID = c.SELLER_ID,
                 B.CONTRACTOR_BANK_ID = c.SELLER_BANK_ID,
                 B.PROFILE_ID = v_profile_id,
                 B.BILL_NO = LPAD(TO_CHAR(c.SELLER_REGION_ID), 4,'0')||'/'||B.BILL_NO
           WHERE B.REP_PERIOD_ID = 201503
             AND B.BILL_ID    = c.Bill_Id
             AND B.PROFILE_ID = c.profile_id;
             
        END IF;
        v_count := v_count + 1;
    
    END LOOP;

    Pk01_Syslog.Write_msg(v_count||' - accounts transfered', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- возвращаем ID операции разноски
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


END PKXX_SVM_TEST;
/
