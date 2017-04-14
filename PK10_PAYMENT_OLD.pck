CREATE OR REPLACE PACKAGE PK10_PAYMENT_OLD
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
                  p_prev_payment_id IN INTEGER DEFAULT NULL, 
                  p_prev_period_id  IN INTEGER DEFAULT NULL
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- Корректировать платеж с Л/С клиента (сумма сразу учитывается в балансе Л/С)
    --   - положительное - ID платежного документа (PAYMENT.PAYMENT_ID) в биллинге, 
    --   - при ошибке выставляет исключение
    --
    FUNCTION Adjust_payment (
                  p_src_payment_id IN INTEGER,    -- ID корректируемого платежа
                  p_src_period_id  IN INTEGER,    -- ID отчетного периода, когда был зарегистрирован платеж
                  p_dst_period_id  IN INTEGER,    -- ID отчетного периода, корректирующего платежа
                  p_value          IN NUMBER,     -- заявленная сумма корректировки
                  p_manager        IN VARCHAR2,   -- менеджер проводивший операцию
                  p_notes          IN VARCHAR2 DEFAULT NULL -- примечание к операции
               ) RETURN NUMBER;

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
                   p_value         IN OUT NUMBER, -- сумма которую хотим перенести, NULL - сколько нужно               
                   p_open_balance  OUT NUMBER,    -- сумма на платеже до проведения операции
                   p_close_balance OUT NUMBER,    -- сумма на платеже после проведения операции
                   p_bill_due      OUT NUMBER     -- оставшийся долг по счету после операции
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- Автоматическая разноска платежа методом FIFO на позиции (ITEM_T(P)) 
    -- выставленных ранее периодических счетов (item-ы не закрываем)
    -- для закрытия задолженности по периодам, возвращает:
    --   - остаток неразнесенных средств на платеже 
    --   - при ошибке выставляет исключение
    FUNCTION Transfer_to_account_fifo(
                   p_payment_id    IN INTEGER,  -- bill - источник
                   p_pay_period_id IN INTEGER,  -- ID отчетного периода куда распределен платеж
                   p_account_id    IN INTEGER   -- лицевой счет, счета которого погашаются
               ) RETURN NUMBER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- Разнести на сформированные счета все что осталось на платежах принятых 
    -- до указанного периода включительно.
    -- разноска внутри л/с по позициям баланс Л/С не изменяет
    -- разноска для 'Ф' автоматическая FIFO, 'Ю'-ручная через АРМ платежей
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Payment_processing_fifo( p_from_period_id IN INTEGER );

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
               ) RETURN NUMBER;

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
               ) RETURN NUMBER;

    -- ------------------------------------------------------------------------ --
    -- Откатить последнюю операцию разноски платежа, 
    -- при условии что период в котором поступил платеж еще не закрыт
    -- (если период закрыт, то уже сформирован аванс для отчетности)
    --   - остаток неразнесенных средств на платеже 
    --   - при ошибке выставляет исключение
    FUNCTION Rollback_transfer(
                   p_transfer_id   IN INTEGER,   -- ID платежа
                   p_pay_period_id IN INTEGER    -- ID периода платежа
               ) RETURN NUMBER;

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

    -- ------------------------------------------------------------------------ --
    -- Список платежей по лицевому счету
    --   - положительное - кол-во выбранных записей
    --   - при ошибке выставляет исключение
    FUNCTION Account_payment_list (
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,   -- ID лицевого счета
                   p_date_from  IN DATE,
                   p_date_to    IN DATE 
               ) RETURN INTEGER;

-- ------------------------------------------------------------------------ --
    -- Список платежей по лицевому счету
    --   - положительное - кол-во выбранных записей
    --   - при ошибке выставляет исключение
    FUNCTION Account_payment_list (
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID лицевого счета
               p_period_id  IN INTEGER
           ) RETURN INTEGER; 
-- ------------------------------------------------------------------------ --              
    -- Список оплат покрывающий счет
    --   - положительное - кол-во выбранных записей
    --   - при ошибке выставляет исключение
    FUNCTION Bill_pay_list (
                   p_recordset    OUT t_refc, 
                   p_bill_id       IN INTEGER,    -- ID платежа
                   p_rep_period_id IN INTEGER     -- ID отчетного периода счета
               ) RETURN INTEGER;

-- ------------------------------------------------------------------------ --
-- Список оплат покрывающий счета за определенный период по определенному лицевому счету
--   - положительное - кол-во выбранных записей
--   - при ошибке выставляет исключение
FUNCTION Bill_pay_list_by_account (
               p_recordset    OUT t_refc, 
               p_account_id   IN INTEGER,    -- ID лицевого счета
               p_rep_period_id IN INTEGER     -- ID отчетного периода счета
           ) RETURN INTEGER;  
            
    -- Просмотр разноски платежа по счетам
    --   - положительное - кол-во выбранных записей
    --   - при ошибке выставляет исключение
    FUNCTION Transfer_list (
                   p_recordset    OUT t_refc, 
                   p_payment_id    IN INTEGER,   -- ID платежа
                   p_pay_period_id IN INTEGER    -- ID отчетного периода счета
               ) RETURN INTEGER;


END PK10_PAYMENT_OLD;
/
CREATE OR REPLACE PACKAGE BODY PK10_PAYMENT_OLD
IS

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
              p_prev_payment_id IN INTEGER DEFAULT NULL,
              p_prev_period_id  IN INTEGER DEFAULT NULL
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
        ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED,
        DATE_FROM, DATE_TO,
        PAYSYSTEM_ID, DOC_ID,
        STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED,
        CREATED_BY, NOTES, 
        PREV_PAYMENT_ID, PREV_PERIOD_ID
    )VALUES(
        v_payment_id, p_rep_period_id, p_payment_type,
        p_payment_datе, p_account_id, p_recvd,
        p_recvd, p_payment_datе, p_recvd, 0,
        NULL, NULL,
        p_paysystem_id, p_doc_id,
        p_status, SYSDATE, SYSDATE, SYSDATE,
        p_manager, p_notes, 
        p_prev_payment_id, p_prev_period_id
    );
    -- Изменяем баланс лицевого счета на величину платежа
    UPDATE ACCOUNT_T
       SET BALANCE = BALANCE + p_recvd,
           BALANCE_DATE = SYSDATE  
     WHERE ACCOUNT_ID = p_account_id;
    --
    RETURN v_payment_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Корректировать платеж с Л/С клиента (сумма сразу учитывается в балансе Л/С)
--   - положительное - ID платежного документа (PAYMENT.PAYMENT_ID) в биллинге, 
--   - при ошибке выставляет исключение
--
FUNCTION Adjust_payment (
               p_src_payment_id IN INTEGER,   -- ID корректируемого платежа
               p_src_period_id  IN INTEGER,   -- ID отчетного периода, когда был зарегистрирован платеж
               p_dst_period_id  IN INTEGER,   -- ID отчетного периода, корректирующего платежа
               p_value          IN NUMBER,    -- заявленная сумма корректировки
               p_manager        IN VARCHAR2,  -- менеджер проводивший операцию
               p_notes          IN VARCHAR2 DEFAULT NULL -- примечание к операции
           ) RETURN NUMBER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Adjust_payment';
    r_payment      PAYMENT_T%ROWTYPE;
    v_payment_id   INTEGER;
BEGIN
    -- читаем данные корректируемого платежа
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id
       AND NEXT_PAYMENT_ID IS NULL;  -- два раза сторнировать нельзя
    -- добавляем корректирующий платеж
    v_payment_id := Add_payment (
              p_account_id      => r_payment.Account_Id,        -- ID лицевого счета клиента
              p_rep_period_id   => p_dst_period_id,             -- ID отчетного периода куда распределен платеж
              p_payment_datе    => r_payment.Payment_Date,      -- дата платежа
              p_payment_type    => Pk00_Const.c_PAY_TYPE_ADJUST,-- тип платежа
              p_recvd           => p_value,                     -- сумма платежа
              p_paysystem_id    => r_payment.paysystem_id,      -- ID платежной системы
              p_doc_id          => NULL,                        -- ID документа в платежной системе
              p_status          => Pk00_Const.c_PAY_STATE_OPEN, -- статус платежа
              p_manager    		  => p_manager,                   -- Ф.И.О. менеджера распределившего платеж на л/с
              p_notes           => p_notes,                     -- примечание к платежу  
              p_prev_payment_id => p_src_payment_id, 
              p_prev_period_id  => p_src_period_id
           );
    -- проставляем указатель на корректирующий платеж
    UPDATE PAYMENT_T 
       SET NEXT_PAYMENT_ID = v_payment_id,
           NEXT_PERIOD_ID  = p_dst_period_id
     WHERE PAYMENT_ID    = p_src_payment_id 
       AND REP_PERIOD_ID = p_src_period_id
       AND NEXT_PAYMENT_ID IS NULL;
    RETURN v_payment_id;  
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.Payment_id='||p_src_payment_id, c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Перенос части (или всей суммы) платежа на ITEM(P) оплаты укзаноого 
-- выставленного периодические счета,
-- если позиции нет - она создается,
-- разноска платежа по счетам, выставленным в период платежа или более ранние периоды 
-- будет уменьшать аванс,
-- возвращает:
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
               p_value         IN OUT NUMBER, -- сумма которую хотим перенести, NULL - сколько нужно               
               p_open_balance  OUT NUMBER,    -- сумма на платеже до проведения операции
               p_close_balance OUT NUMBER,    -- сумма на платеже после проведения операции
               p_bill_due      OUT NUMBER     -- оставшийся долг по счету после операции
           ) RETURN INTEGER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Transfer_to_bill';
    v_transfer_id  INTEGER := NULL;
    v_item_id      INTEGER;
    v_prev_id      INTEGER;
    v_date_from    DATE; 
    v_date_to      DATE;
    v_bill_date    DATE;
    v_payment_date DATE;
    v_advance      NUMBER;
BEGIN
    -- начальная установка переменных
    p_open_balance := 0;
    p_close_balance:= 0;
    p_bill_due := 0;
    
    -- получаем неразнесенный остаток на платеже (входящий остаток) и
    -- читаем текущую задолженность по ранее выставленному счету (до операции)
    SELECT P.BALANCE, P.DATE_FROM, P.DATE_TO, P.PAYMENT_DATE, P.ADVANCE, B.DUE, B.BILL_DATE
      INTO p_open_balance, v_date_from, v_date_to, v_payment_date, v_advance, p_bill_due, v_bill_date
      FROM PAYMENT_T P, BILL_T B
     WHERE P.ACCOUNT_ID = B.ACCOUNT_ID
       AND P.REP_PERIOD_ID = p_pay_period_id
       AND B.REP_PERIOD_ID = p_rep_period_id
       AND B.BILL_ID = p_bill_id
       AND P.PAYMENT_ID = p_payment_id;
    -- проверяем есть ли задолженность по счету или неразнесенные средства на платеже
    IF p_bill_due >= 0 OR p_open_balance <= 0 THEN
        p_close_balance := p_open_balance;
        p_value := 0;
        RETURN NULL; 
    END IF;
    -- рассчитываем сумму которую будем разносить
    IF p_value IS NULL THEN
        IF (p_open_balance + p_bill_due) >= 0 THEN
            -- платежных средств хватило на погашение задолженности по счету
            p_value := -p_bill_due;
        ELSE
            -- платежных средств НЕ хватило, задолженность погашена частично
            p_value := p_open_balance;
        END IF; 
    ELSE
        IF p_value <= 0 THEN
            Pk01_Syslog.Write_msg(p_Msg => 'Для разноски задана сумма меньше 0: '||p_value,
                                  p_Src => c_PkgName||'.'||v_prcName, 
                                  p_Level => Pk01_Syslog.L_warn);
            p_close_balance := p_open_balance;
            p_value := 0;
            RETURN NULL; -- отрицательных платежей быть не должно
        ELSIF (p_value + p_bill_due) > 0 THEN
            p_value := -p_bill_due;
        END IF;
    END IF;
    -- рассчитываем остатки
    p_close_balance := p_open_balance - p_value;  -- списываем с платежа
    p_bill_due := p_bill_due + p_value;           -- гасим задолженность

    -- изменяем задолженность по счету
    UPDATE BILL_T B 
       SET DUE   = p_bill_due, 
           RECVD = RECVD + p_value
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
     
    -- находим ID предыдущей операции разноски, если была
    IF v_date_from IS NOT NULL THEN
        --
        SELECT MAX(TRANSFER_ID) INTO v_prev_id
          FROM PAY_TRANSFER_T T
         WHERE T.PAYMENT_ID = p_payment_id
           AND T.PAY_PERIOD_ID = p_pay_period_id
         ;
        -- уточняем диапазон дат, начисления которого погашены платежом 
        IF v_bill_date < v_date_from THEN
            v_date_from := v_bill_date;
        ELSIF v_bill_date > v_date_to THEN
            v_date_to := v_bill_date;
        END IF;
    ELSE -- это первая операция разноски
        v_prev_id := NULL;
        -- выставляем диапазон дат, начисления которого погашены платежом
        v_date_from := v_bill_date;
        v_date_to   := v_bill_date;
    END IF;
    -- создаем операцию разноски платежа на счет
    v_transfer_id := Pk02_Poid.Next_transfer_id;
    --    
    INSERT INTO PAY_TRANSFER_T (
           TRANSFER_ID, 
           PAYMENT_ID, PAY_PERIOD_ID,
           BILL_ID, REP_PERIOD_ID, ITEM_ID,
           TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE,
           TRANSFER_DATE, PREV_TRANSFER_ID, NOTES
    )VALUES(
           v_transfer_id, 
           p_payment_id, p_pay_period_id,
           p_bill_id, p_rep_period_id, v_item_id, 
           p_value, p_open_balance, p_close_balance,
           SYSDATE, v_prev_id, p_notes
    );
    
    -- изменяем данные в описантии платежа
    UPDATE PAYMENT_T 
       SET BALANCE   = p_close_balance,
           TRANSFERED= TRANSFERED + p_value,
           DATE_FROM = v_date_from, 
           DATE_TO   = v_date_to
     WHERE PAYMENT_ID = p_payment_id
       AND REP_PERIOD_ID = p_pay_period_id;

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
    -- Получаем список (FIFO) выставленных, но неоплаченных счетов
    FOR c_bill IN ( 
        SELECT BILL_ID, REP_PERIOD_ID 
          FROM BILL_T
         WHERE ACCOUNT_ID = p_account_id
           AND DUE < 0
           AND BILL_STATUS IN (PK00_CONST.c_BILL_STATE_CLOSED, PK00_CONST.c_BILL_STATE_READY)
         ORDER BY BILL_DATE )
    LOOP
       -- для разноски доступем весь остаток на платеже 
       v_value := NULL;     
       -- разносим платеж на неоплаченные счета в порядке их выставления
       v_transfer_id := Transfer_to_bill(
               p_payment_id    => p_payment_id,   -- ID платежа - источника средств
               p_pay_period_id => p_pay_period_id,-- ID отчетного периода куда распределен платеж
               p_bill_id       => c_bill.bill_id, -- ID выставленного счета
               p_rep_period_id => c_bill.rep_period_id, -- ID отчетного периода счета
               p_notes         => NULL,           -- примечания к операции
               p_value         => v_value,        -- сумма которую хотим перенести, NULL - сколько нужно
               p_open_balance  => v_open_balance, -- сумма на платеже до проведения операции
               p_close_balance => v_close_balance,-- сумма на платеже после проведения операции
               p_bill_due      => v_bill_due      -- оставшийся долг по счету после операции
           );
       EXIT WHEN v_transfer_id IS NULL;
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
    v_transfer_id   INTEGER;
    v_value         NUMBER := 0; -- сумма которую хотим перенести, NULL - сколько нужно               
    v_open_balance  NUMBER := 0; -- сумма на платеже до проведения операции
    v_close_balance NUMBER := 0; -- сумма на платеже после проведения операции
    v_bill_due      NUMBER := 0; -- оставшийся долг по счету после операции
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, from period_id <= '||p_from_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_ok := 0;    
    v_err:= 0;
    --
    -- разносим авансы прошлых периодов на подготовленные в биллинговом периоде счета Физ. лиц
    FOR r_pay IN (
        SELECT P.ACCOUNT_ID, 
               P.PAYMENT_ID, P.REP_PERIOD_ID PAY_PERIOD_ID, 
               B.BILL_ID, B.REP_PERIOD_ID 
          FROM PAYMENT_T P, BILL_T B, ACCOUNT_T A
         WHERE A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_P
           AND P.ACCOUNT_ID = A.ACCOUNT_ID
           AND B.ACCOUNT_ID = A.ACCOUNT_ID
           AND P.BALANCE > 0
           AND B.DUE < 0
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_CLOSED, Pk00_Const.c_BILL_STATE_READY)
           AND P.REP_PERIOD_ID <= p_from_period_id  
        ORDER BY B.BILL_DATE, P.PAYMENT_DATE
      )
    LOOP
        SAVEPOINT X;  -- точка сохранения данных для лицевого счета
        BEGIN
            -- разносим остатки платежей по закрытым счетам 
            -- (для Физиков методом FIFO, для Юриков, только руками через АРМ)
            -- для разноски доступем весь остаток на платеже 
            v_value := NULL;
            -- разносим платеж на неоплаченные счета в порядке их выставления
            v_transfer_id := Transfer_to_bill(
                     p_payment_id    => r_pay.payment_id,   -- ID платежа - источника средств
                     p_pay_period_id => r_pay.pay_period_id,-- ID отчетного периода куда распределен платеж
                     p_bill_id       => r_pay.bill_id,      -- ID выставленного счета
                     p_rep_period_id => r_pay.rep_period_id,-- ID отчетного периода счета
                     p_notes         => NULL,           -- примечания к операции
                     p_value         => v_value,        -- сумма которую хотим перенести, NULL - сколько нужно
                     p_open_balance  => v_open_balance, -- сумма на платеже до проведения операции
                     p_close_balance => v_close_balance,-- сумма на платеже после проведения операции
                     p_bill_due      => v_bill_due      -- оставшийся долг по счету после операции
                 );
            v_ok := v_ok + 1;         -- инвойс создан успешно
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
        IF MOD((v_ok+v_err), 500) = 0 THEN
            Pk01_Syslog.Write_msg('Processed: '||v_ok||'-ok, '||v_err||'-err advances', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        --
    END LOOP;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Сторнирование платежа
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
           ) RETURN NUMBER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Revers_transfer';
    r_payment      PAYMENT_T%ROWTYPE;
    v_transfer_id  INTEGER;
    v_prev_trn_id  INTEGER;
    v_payment_id   INTEGER;
BEGIN
    v_payment_id := PK02_POID.Next_payment_id;
    -- читаем данные сторнируемого платежа
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id
       AND NEXT_PAYMENT_ID IS NULL;  -- два раза сторнировать нельзя
    -- формируем сторнирующий платеж в текущем периоде
    INSERT INTO PAYMENT_T (
        PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE,
        PAYMENT_DATE, ACCOUNT_ID, RECVD,
        ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED,
        DATE_FROM, DATE_TO,
        PAYSYSTEM_ID, DOC_ID,
        STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED,
        CREATED_BY, NOTES, PREV_PAYMENT_ID, PREV_PERIOD_ID
    )VALUES(
        v_payment_id, p_dst_period_id, PK00_CONST.c_PAY_TYPE_REVERS, 
        r_payment.PAYMENT_DATE, r_payment.ACCOUNT_ID, -r_payment.RECVD,
        -r_payment.ADVANCE, SYSDATE, -r_payment.BALANCE, -r_payment.TRANSFERED,
        r_payment.DATE_FROM, r_payment.DATE_TO,
        r_payment.PAYSYSTEM_ID, r_payment.DOC_ID,
        PK00_CONST.c_PAY_STATE_OPEN, SYSDATE, SYSDATE, SYSDATE,
        p_manager, p_notes, r_payment.PAYMENT_ID, r_payment.REP_PERIOD_ID
    );
    -- сохраняем указатель на сторнирующий платеж    
    UPDATE PAYMENT_T SET NEXT_PAYMENT_ID = v_payment_id
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id
       AND NEXT_PAYMENT_ID IS NULL;  -- два раза сторнировать нельзя
    -- откатываем изменение баланс лицевого счета на величину платежа
    UPDATE ACCOUNT_T
       SET BALANCE = BALANCE - r_payment.RECVD,
           BALANCE_DATE = SYSDATE  
     WHERE ACCOUNT_ID = r_payment.ACCOUNT_ID;
    --
    -- сторнируем операции разноски платежа   
    v_prev_trn_id := NULL;
    --
    FOR r_trn IN (
        SELECT 
           TRANSFER_ID, 
           PAYMENT_ID, PAY_PERIOD_ID,
           BILL_ID, REP_PERIOD_ID, ITEM_ID,
           TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE,
           TRANSFER_DATE, PREV_TRANSFER_ID, NOTES
          FROM PAY_TRANSFER_T
         WHERE PAYMENT_ID    = r_payment.PAYMENT_ID
           AND PAY_PERIOD_ID = r_payment.REP_PERIOD_ID
         ORDER BY TRANSFER_ID
      )
    LOOP
        -- формируем сторнирующие записи разноски платежа
        v_transfer_id := Pk02_Poid.Next_transfer_id;
        --
        INSERT INTO PAY_TRANSFER_T (
           TRANSFER_ID, 
           PAYMENT_ID, PAY_PERIOD_ID,
           BILL_ID, REP_PERIOD_ID, ITEM_ID,
           TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE,
           TRANSFER_DATE, PREV_TRANSFER_ID, NOTES
        )VALUES(
           v_transfer_id, 
           r_payment.PAYMENT_ID, r_payment.REP_PERIOD_ID,
           r_trn.BILL_ID, r_trn.REP_PERIOD_ID, r_trn.ITEM_ID,
           -r_trn.TRANSFER_TOTAL, -r_trn.OPEN_BALANCE, -r_trn.CLOSE_BALANCE,
           SYSDATE, v_prev_trn_id, NULL
        );
        --
        v_prev_trn_id := v_transfer_id;
        --
        -- узменяем баланс счета
        UPDATE BILL_T B 
           SET DUE   = DUE   - r_trn.TRANSFER_TOTAL,
               RECVD = RECVD - r_trn.TRANSFER_TOTAL
         WHERE BILL_ID       = r_trn.BILL_ID
           AND REP_PERIOD_ID = r_trn.REP_PERIOD_ID;
        --
    END LOOP;  
    --
    RETURN v_payment_id;
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
           ) RETURN NUMBER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Move_payment';
    r_payment        PAYMENT_T%ROWTYPE;
    v_rev_payment_id INTEGER;
    v_payment_id     INTEGER;
BEGIN
    -- читаем данные переносимого платежа
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id
       AND NEXT_PAYMENT_ID IS NULL;  -- два раза сторнировать нельзя
    -- сторнируем платеж источник
    v_rev_payment_id := Revers_payment (
               p_src_payment_id,  -- ID сторнируемого платежа                        
               p_src_period_id,   -- ID отчетного периода, когда был зарегистрирован платеж
               p_dst_period_id,   -- ID отчетного периода, сторнирующего платежа
               p_manager,         -- менеджер проводивший операцию
               p_notes            -- примечание к операции
           );
    -- добавляем корректирующий платеж
    v_payment_id := Add_payment (
              p_account_id      => p_dst_account_id,   -- ID лицевого счета клиента
              p_rep_period_id   => p_dst_period_id,   -- ID отчетного периода куда распределен платеж
              p_payment_datе    => r_payment.Payment_Date,        -- дата платежа
              p_payment_type    => Pk00_Const.c_PAY_TYPE_ADJUST,  -- тип платежа
              p_recvd           => r_payment.Recvd,    -- сумма платежа
              p_paysystem_id    => r_payment.paysystem_id,   -- ID платежной системы
              p_doc_id          => r_payment.Doc_Id,  -- ID документа в платежной системе
              p_status          => Pk00_Const.c_PAY_STATE_OPEN,  -- статус платежа
              p_manager    		  => p_manager,  -- Ф.И.О. менеджера распределившего платеж на л/с
              p_notes           => p_notes,  -- примечание к платежу  
              p_prev_payment_id => r_payment.Payment_Id, 
              p_prev_period_id  => r_payment.Rep_Period_Id
           );
    RETURN v_payment_id;  
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.Payment_id='||p_src_payment_id, c_PkgName||'.'||v_prcName );
END;


-- ------------------------------------------------------------------------ --
-- Проверка: возможен ли откат платежа?
-- Откат платежа возможен, если платеж пришел 
-- в открытый на текущий момент финансовый период
-- Возвращает: TRUE/FALSE
--
FUNCTION IF_rollback_enable(
               p_pay_period_id IN INTEGER    -- ID периода платежа
           ) RETURN BOOLEAN
IS
    v_fin_period   DATE;
BEGIN
    SELECT CLOSE_FIN_PERIOD INTO v_fin_period
      FROM PERIOD_T
     WHERE PERIOD_ID = p_pay_period_id;
    IF v_fin_period IS NULL THEN
        RETURN TRUE;  -- платеж принадлежит открытому фин. периоду
    ELSE
        RETURN FALSE; -- платеж принадлежит закрытому фин. периоду
    END IF;  
END;

-- ------------------------------------------------------------------------ --
-- Откатить последнюю операцию разноски платежа, 
-- при условии что период в котором поступил платеж еще не закрыт
-- (если период закрыт, то уже сформирован аванс для отчетности)
--   - остаток неразнесенных средств на платеже 
--   - при ошибке выставляет исключение
FUNCTION Rollback_transfer(
               p_transfer_id   IN INTEGER,   -- ID платежа
               p_pay_period_id IN INTEGER    -- ID периода платежа
           ) RETURN NUMBER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Rollback_transfer';
    v_payment_id     INTEGER;
    v_bill_id        INTEGER;
    v_rep_period_id  INTEGER;
    v_open_balance   NUMBER := 0; -- сумма на платеже до проведения операции
    v_transfer_total NUMBER := 0; -- сумма операции разноски платежа
    v_count          INTEGER:= 0;
    v_date_from      DATE;
    v_date_to        DATE;
BEGIN
    -- Нужно убедиться, что удаляемая операция последняя в цепочке
    SELECT COUNT(*)
      INTO v_count
      FROM PAY_TRANSFER_T
     WHERE PREV_TRANSFER_ID = p_transfer_id
       AND REP_PERIOD_ID = p_pay_period_id;
    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
               'Операция '||p_transfer_id||' - не последняя в списке');
    END IF;

    -- получаем данные операции разноски
    SELECT PT.BILL_ID, PT.REP_PERIOD_ID, PT.OPEN_BALANCE, PT.TRANSFER_TOTAL, PT.PAYMENT_ID
      INTO v_bill_id, v_rep_period_id, v_open_balance, v_transfer_total, v_payment_id
      FROM PAY_TRANSFER_T PT
     WHERE PT.TRANSFER_ID = p_transfer_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id;

    -- Платеж должен быть из открытого финансового периода 
    IF IF_rollback_enable(p_pay_period_id) = FALSE THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
               'Платеж '||v_payment_id||' - принадлежит закрытому финансовому периоду '
                        ||p_pay_period_id);
    END IF;
     
    -- изменяем задолженность по счету, тоже с полюсом
    UPDATE BILL_T B 
       SET DUE   = DUE + v_transfer_total,
           RECVD = RECVD - v_transfer_total
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
     WHERE PT.PAYMENT_ID = v_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND PT.BILL_ID = B.BILL_ID
       AND B.REP_PERIOD_ID = v_rep_period_id;
    
    -- возвращаем деньги на платеж
    UPDATE PAYMENT_T 
       SET BALANCE   = v_open_balance,
           TRANSFERED= TRANSFERED - v_transfer_total,
           DATE_FROM = v_date_from, 
           DATE_TO   = v_date_to,
           LAST_MODIFIED = SYSDATE
     WHERE PAYMENT_ID = v_payment_id
       AND REP_PERIOD_ID = p_pay_period_id;
 
    RETURN v_open_balance;
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
    v_value          NUMBER;
    v_paysystem_id   INTEGER;
    v_payment_date   DATE;
    v_doc_id         INTEGER;
    v_total          NUMBER;
    
BEGIN
    -- Платеж должен быть из открытого финансового периода 
    IF IF_rollback_enable(p_pay_period_id) = FALSE THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
               'Платеж '||p_payment_id||' - принадлежит закрытому финансовому периоду: '
                        ||p_pay_period_id);
    END IF;

    -- Удаляем все операции разноски платежа
    FOR c_transfer IN (
            SELECT TRANSFER_ID
              FROM PAY_TRANSFER_T
             WHERE PAYMENT_ID = p_payment_id
               AND PAY_PERIOD_ID = p_pay_period_id
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
    v_advance  NUMBER;
BEGIN
    MERGE INTO PAYMENT_T P
    USING (
        SELECT REP_PERIOD_ID, PAYMENT_ID, SUM(TRANSFER_TOTAL) TRANSFER_TOTAL 
          FROM PAY_TRANSFER_T
         WHERE REP_PERIOD_ID <= PAY_PERIOD_ID
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
--   - положительное - кол-во выбранных записей
--   - при ошибке выставляет исключение
FUNCTION Account_payment_list (
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID лицевого счета
               p_date_from  IN DATE,
               p_date_to    IN DATE 
           ) RETURN INTEGER
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
    
    -- вычисляем кол-во записей
    SELECT COUNT(*) INTO v_retcode 
     FROM PAYMENT_T P
    WHERE ACCOUNT_ID = p_account_id
      AND REP_PERIOD_ID BETWEEN v_min_period_id AND v_max_period_id;
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
    RETURN v_retcode;
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
--   - положительное - кол-во выбранных записей
--   - при ошибке выставляет исключение
FUNCTION Account_payment_list (
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID лицевого счета
               p_period_id  IN INTEGER
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Account_payment_list';
    v_retcode       INTEGER;
BEGIN   
    -- вычисляем кол-во записей
    SELECT COUNT(*) INTO v_retcode 
     FROM PAYMENT_T P
    WHERE ACCOUNT_ID = p_account_id
      AND REP_PERIOD_ID = p_period_id;

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
    RETURN v_retcode;
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
FUNCTION Bill_pay_list (
               p_recordset    OUT t_refc, 
               p_bill_id       IN INTEGER,    -- ID платежа
               p_rep_period_id IN INTEGER     -- ID отчетного периода счета
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bill_pay_list';
    v_retcode    INTEGER;
BEGIN
    -- вычисляем кол-во записей
    SELECT COUNT(*) INTO v_retcode
      FROM PAY_TRANSFER_T PT, PAYMENT_T P, PAYSYSTEM_T PS,BILL_T B
     WHERE B.BILL_ID       = p_bill_id
       AND B.REP_PERIOD_ID = p_rep_period_id
       AND B.BILL_ID       = PT.BILL_ID
       AND B.REP_PERIOD_ID = PT.REP_PERIOD_ID
       AND PT.PAYMENT_ID   = P.PAYMENT_ID
       AND PT.PAY_PERIOD_ID= P.REP_PERIOD_ID
       AND PS.PAYSYSTEM_ID = P.PAYSYSTEM_ID;
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
          SELECT B.BILL_ID, B.BILL_NO, B.REP_PERIOD_ID BILL_REP_PERIOD_ID, B.RECVD,
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
    RETURN v_retcode;
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
--   - положительное - кол-во выбранных записей
--   - при ошибке выставляет исключение
FUNCTION Bill_pay_list_by_account (
               p_recordset    OUT t_refc, 
               p_account_id   IN INTEGER,    -- ID лицевого счета
               p_rep_period_id IN INTEGER     -- ID отчетного периода счета
           ) RETURN INTEGER
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
    RETURN v_retcode;
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
FUNCTION Transfer_list (
               p_recordset    OUT t_refc, 
               p_payment_id    IN INTEGER,   -- ID платежа
               p_pay_period_id IN INTEGER    -- ID отчетного периода счета
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Transfer_list';
    v_retcode    INTEGER;
BEGIN
    -- вычисляем кол-во записей
    SELECT COUNT(*) INTO v_retcode
      FROM BILL_T B, PAY_TRANSFER_T PT, PAYMENT_T P, PAYSYSTEM_T PS
     WHERE B.BILL_ID       = PT.BILL_ID
       AND B.REP_PERIOD_ID = PT.REP_PERIOD_ID
       AND PT.PAYMENT_ID   = P.PAYMENT_ID
       AND PT.PAY_PERIOD_ID= P.REP_PERIOD_ID
       AND P.PAYMENT_ID    = p_payment_id
       AND P.REP_PERIOD_ID = p_pay_period_id
       AND PS.PAYSYSTEM_ID = P.PAYSYSTEM_ID;
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
          
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


END PK10_PAYMENT_OLD;
/
