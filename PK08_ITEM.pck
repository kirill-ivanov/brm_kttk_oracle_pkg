CREATE OR REPLACE PACKAGE PK08_ITEM
IS
    --
    -- Пакет для работы с объектом "ПОЗИЦИЯ СЧЕТА", таблицы:
    -- item_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK08_ITEM';
    -- ==============================================================================
    type t_refc is ref cursor;
    
    -- ------------------------------------------------------------------ --
    -- создание объекта новой биллинговой позиции (item) счета, возвращает:
    --   - ID позиции счета (item_id), 
    --   - при ошибке выставляет исключение
    FUNCTION New_bill_item (
                   p_bill_id        IN INTEGER,   -- ID счета
                   p_rep_period_id  IN INTEGER,   -- ID отчетного периода счета
                   p_order_id       IN INTEGER,   -- ID заказа
                   p_service_id     IN INTEGER,   -- ID усдуги
                   p_subservice_id  IN INTEGER,   -- ID компонента услуги
                   p_charge_type    IN VARCHAR2,  -- ID способа начисления (АП, трафик,...)
                   p_tax_incl       IN CHAR,      -- Начисления включают налог: "Y/N"
                   p_item_total     IN NUMBER DEFAULT 0, -- общая сумма на позиции счета
                   p_date_from      IN DATE DEFAULT NULL, -- дата первого события услуги
                   p_date_to        IN DATE DEFAULT NULL  -- дата последнего события услуги
               ) RETURN INTEGER;

      -- проведение начислениий на позицию (item) счета
      -- если позиции нет, то она создается
      --   - возвращает ITEM_ID
      --   - при ошибке выставляет исключение
      FUNCTION Put_bill_item(
                   p_bill_id        IN INTEGER,   -- ID счета
                   p_rep_period_id  IN INTEGER,   -- ID отчетного периода счета
                   p_order_id       IN INTEGER,   -- ID заказа
                   p_service_id     IN INTEGER,   -- ID усдуги
                   p_subservice_id  IN INTEGER,   -- ID компонента услуги
                   p_charge_type    IN VARCHAR2,  -- ID способа начисления (АП, трафик,...)
                   p_tax_incl       IN CHAR,      -- Начисления включают налог: "Y/N"
                   p_item_total     IN NUMBER DEFAULT 0, -- общая сумма на позиции счета
                   p_date_from      IN DATE DEFAULT NULL, -- дата первого события услуги
                   p_date_to        IN DATE DEFAULT NULL  -- дата последнего события услуги
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------ --
    -- создание объекта новой позиции корректировки (item) счета, возвращает:
    --   - ID позиции счета (item_id), 
    --   - при ошибке выставляет исключение
    FUNCTION New_adjust_item (
                   p_bill_id        IN INTEGER,   -- ID счета
                   p_rep_period_id  IN INTEGER,   -- ID отчетного периода счета
                   p_order_id       IN INTEGER,   -- ID заказа
                   p_service_id     IN INTEGER,   -- ID усдуги
                   p_subservice_id  IN INTEGER,   -- ID компонента услуги
                   p_charge_type    IN VARCHAR2,  -- ID способа начисления (АП, трафик,...)
                   p_tax_incl       IN CHAR,      -- Начисления включают налог: "Y/N"
                   p_adjusted       IN NUMBER DEFAULT 0,  -- сумма корретировки
                   p_date_from      IN DATE DEFAULT NULL, -- дата первого события услуги
                   p_date_to        IN DATE DEFAULT NULL, -- дата последнего события услуги
                   p_notes          IN VARCHAR2 DEFAULT NULL
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------ --
    -- Рассчитать задолженность для позиции счета
    --   - величину задолженности по позиции DUE=ITEM_TOTAL+ADJUSTED-TRANSFERED-RECVD 
    --   - при ошибке выставляет исключение
    FUNCTION Calculate_due(
                   p_bill_id       IN INTEGER,   -- ID счета
                   p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
                   p_item_id       IN INTEGER    -- ID позиции счета
               ) RETURN NUMBER; 

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Начисления
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- проверить возможно ли начисления и корректировки на ITEM
    -- начисления возможны, если ITEM еще не вошел в счет фактуру
    --   - при ошибке выставляет исключение
    FUNCTION Is_chargable (
                   p_bill_id       IN INTEGER,   -- ID счета
                   p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
                   p_item_id       IN INTEGER    -- ID позиции счета
               ) RETURN BOOLEAN;
    
    -- произвести начисление на позицию счета (item), возвращает:
    --   - величину задолженности по позиции DUE=ITEM_TOTAL+ADJUSTED-TRANSFERED-RECVD 
    --   - при ошибке выставляет исключение
    FUNCTION Charge_item_value (
                   p_bill_id       IN INTEGER,   -- ID счета
                   p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
                   p_item_id       IN INTEGER,   -- ID позиции счета
                   p_value         IN NUMBER,    -- сумма начислений на позицию счета
                   p_date_from     IN DATE,      -- временной диапазон оказанной услуги
                   p_date_to       IN DATE       -- из event_t
               ) RETURN NUMBER;

    -- произвести корректировку суммы позиции счета, возвращает:
    --   - величину задолженности по позиции DUE=ITEM_TOTAL+ADJUSTED-TRANSFERED-RECVD 
    --   - при ошибке выставляет исключение
    FUNCTION Adjust_item_value (
                   p_bill_id       IN INTEGER,   -- ID счета
                   p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
                   p_item_id       IN INTEGER,   -- ID позиции счета
                   p_value         IN NUMBER     -- сумма начислений на позицию счета
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Платежи
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- произвести прием спедств на позицию оплаты счета, возвращает:
    --   - величину задолженности по позиции DUE=ITEM_TOTAL+ADJUSTED+TRANSFERED+RECVD 
    --   - при ошибке выставляет исключение
    FUNCTION Recvd_item_value (
                   p_bill_id       IN INTEGER,   -- ID счета
                   p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
                   p_item_id       IN INTEGER,   -- ID позиции счета
                   p_value         IN NUMBER     -- сумма начислений на позицию счета
               ) RETURN INTEGER;
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
END PK08_ITEM;
/
CREATE OR REPLACE PACKAGE BODY PK08_ITEM
IS

--==================================================================================--
-- Проверяем статус счета, работа возможно только с открытым счетом, 
-- если статус != 'OPEN' - выставляется исключение
PROCEDURE Check_bill_status (
          p_bill_id  IN INTEGER, 
          p_rep_period_id IN INTEGER
       )
IS
    v_bill_status BILL_T.BILL_STATUS%TYPE;
BEGIN
    -- проверяем статус счета, работа возможно только с открытым счетом
    SELECT B.BILL_STATUS
      INTO v_bill_status 
      FROM BILL_T B
     WHERE B.BILL_ID = p_bill_id
       AND B.REP_PERIOD_ID = p_rep_period_id;
     IF v_bill_status != Pk00_Const.c_BILL_STATE_OPEN THEN
         RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
                      'BILL_ID='||p_bill_id||
                      ', BILL_STATUS='||v_bill_status||
                      ' - работа с позициями счета запрещена');         
     END IF;
END;

--==================================================================================--
-- создание объекта новой позиции (item) счета, возвращает:
--   - ID позиции счета (item_id), 
--   - при ошибке выставляет исключение
FUNCTION New_bill_item (
               p_bill_id        IN INTEGER,   -- ID счета
               p_rep_period_id  IN INTEGER,   -- ID отчетного периода счета
               p_order_id       IN INTEGER,   -- ID заказа
               p_service_id     IN INTEGER,   -- ID усдуги
               p_subservice_id  IN INTEGER,   -- ID компонента услуги
               p_charge_type    IN VARCHAR2,  -- ID способа начисления (АП, трафик,...)
               p_tax_incl       IN CHAR,      -- Начисления включают налог: "Y/N"
               p_item_total     IN NUMBER DEFAULT 0, -- общая сумма на позиции счета
               p_date_from      IN DATE DEFAULT NULL, -- дата первого события услуги
               p_date_to        IN DATE DEFAULT NULL  -- дата последнего события услуги
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'New_bill';
    v_item_id     INTEGER;
    v_item_type   ITEM_T.ITEM_TYPE%TYPE;
BEGIN
    -- работа возможна только с открытым счетом
    Check_bill_status (p_bill_id, p_rep_period_id);
    -- получаем ID позиции счета
    v_item_id := PK02_POID.Next_item_id;
    -- тип позиции счета
    v_item_type := Pk00_Const.c_ITEM_TYPE_BILL;
    -- создаем запись позиции счета
    INSERT INTO ITEM_T (
       BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, 
       ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE,
       ITEM_TOTAL, RECVD, 
       DATE_FROM, DATE_TO, LAST_MODIFIED,
       INV_ITEM_ID, ITEM_STATUS, TAX_INCL
    )VALUES(
       p_bill_id, p_rep_period_id, v_item_id, v_item_type,
       p_order_id, p_service_id, p_subservice_id, p_charge_type,
       p_item_total, 0,
       p_date_from, p_date_to, SYSDATE,
       NULL, Pk00_Const.c_ITEM_STATE_OPEN, p_tax_incl
    );
    -- возвращаем ID созданной позиции счета
    RETURN v_item_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================--
-- проведение начислениий на позицию (item) счета
-- если позиции нет, то она создается
--   - возвращает ITEM_ID
--   - при ошибке выставляет исключение
FUNCTION Put_bill_item(
               p_bill_id        IN INTEGER,   -- ID счета
               p_rep_period_id  IN INTEGER,   -- ID отчетного периода счета
               p_order_id       IN INTEGER,   -- ID заказа
               p_service_id     IN INTEGER,   -- ID усдуги
               p_subservice_id  IN INTEGER,   -- ID компонента услуги
               p_charge_type    IN VARCHAR2,  -- ID способа начисления (АП, трафик,...)
               p_tax_incl       IN CHAR,      -- Начисления включают налог: "Y/N"
               p_item_total     IN NUMBER DEFAULT 0, -- общая сумма на позиции счета
               p_date_from      IN DATE DEFAULT NULL, -- дата первого события услуги
               p_date_to        IN DATE DEFAULT NULL  -- дата последнего события услуги
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Put_bill_item';
    v_item_type  CONSTANT ITEM_T.ITEM_TYPE%TYPE := Pk00_Const.c_ITEM_TYPE_BILL;
    v_item_id    INTEGER;
    v_count      INTEGER := 0;
BEGIN
    -- работа возможна только с открытым счетом
    Check_bill_status (p_bill_id, p_rep_period_id);
    -- пытаемся выполнить изменение существующей позиции счета
    UPDATE ITEM_T
      SET ITEM_TOTAL= ITEM_TOTAL + p_item_total,
          DATE_FROM = CASE 
                        WHEN DATE_FROM IS NULL THEN p_date_from
                        WHEN p_date_from < DATE_FROM THEN p_date_from 
                      END,
          DATE_TO   = CASE 
                        WHEN DATE_TO IS NULL THEN p_date_to
                        WHEN DATE_TO < p_date_to THEN p_date_to 
                      END,
          LAST_MODIFIED = SYSDATE
     WHERE  BILL_ID       = p_bill_id
        AND REP_PERIOD_ID = p_rep_period_id
        AND ITEM_TYPE     = v_item_type
        AND ORDER_ID      = p_order_id 
        AND SERVICE_ID    = p_service_id
        AND SUBSERVICE_ID = p_subservice_id 
        AND CHARGE_TYPE   = p_charge_type 
        AND ( DATE_FROM IS NULL OR  -- принадлежат одному биллинговому периоду
              TRUNC(DATE_FROM,'mm') = TRUNC(p_date_from,'mm') 
            )
    RETURNING ITEM_ID INTO v_item_id;
    v_count := SQL%ROWCOUNT; 
    IF v_count = 0 THEN
        -- позиция платежа на счете еще не создана, создаем
        v_item_id := PK02_POID.Next_item_id;
        --
        INSERT INTO ITEM_T (
           BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, 
           ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE,
           ITEM_TOTAL, RECVD, 
           DATE_FROM, DATE_TO,
           INV_ITEM_ID, TAX_INCL
        )VALUES(
           p_bill_id, p_rep_period_id, v_item_id, v_item_type,
           p_order_id, p_service_id, p_subservice_id, p_charge_type,
           p_item_total, 0,
           p_date_from, p_date_to,
           NULL, p_tax_incl
        );
    ELSIF v_count > 1 THEN
        -- item платежа на счете не уникален - так быть не должно
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
                        'Не уникальный ITEM начислений на счете BILL_T.BILL_ID='||p_bill_id); 
    END IF;
    RETURN v_item_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================--
-- создание объекта новой позиции корректировки (item) счета, возвращает:
--   - ID позиции счета (item_id), 
--   - при ошибке выставляет исключение
FUNCTION New_adjust_item (
               p_bill_id        IN INTEGER,   -- ID счета
               p_rep_period_id  IN INTEGER,   -- ID отчетного периода счета
               p_order_id       IN INTEGER,   -- ID заказа
               p_service_id     IN INTEGER,   -- ID усдуги
               p_subservice_id  IN INTEGER,   -- ID компонента услуги
               p_charge_type    IN VARCHAR2,  -- ID способа начисления (АП, трафик,...)
               p_tax_incl       IN CHAR,      -- Начисления включают налог: "Y/N"
               p_adjusted       IN NUMBER DEFAULT 0,  -- сумма корретировки
               p_date_from      IN DATE DEFAULT NULL, -- дата первого события услуги
               p_date_to        IN DATE DEFAULT NULL, -- дата последнего события услуги
               p_notes          IN VARCHAR2 DEFAULT NULL
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Put_adjust_item';
    v_item_id    INTEGER;
    v_item_type  ITEM_T.ITEM_TYPE%TYPE;
BEGIN
    -- работа возможна только с открытым счетом
    Check_bill_status (p_bill_id, p_rep_period_id);
    -- получаем ID позиции счета
    v_item_id := PK02_POID.Next_item_id;
    -- выставляем тип позиции корректировки счета
    v_item_type := PK00_CONST.c_ITEM_TYPE_ADJUST;
    -- создаем запись позиции счета
    INSERT INTO ITEM_T (
       BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE,  
       ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE,
       ITEM_TOTAL, RECVD, ITEM_STATUS, 
       DATE_FROM, DATE_TO,
       INV_ITEM_ID, TAX_INCL, NOTES
    )VALUES(
       p_bill_id, p_rep_period_id, v_item_id, v_item_type,
       p_order_id, p_service_id, p_subservice_id, p_charge_type,
       p_adjusted, 0,'OPEN',
       p_date_from, p_date_to,
       NULL, p_tax_incl, p_notes
    );
    -- возвращаем ID созданной позиции счета
    RETURN v_item_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================--
-- Рассчитать задолженность для позиции счета
--   - величину задолженности по позиции DUE=ITEM_TOTAL+ADJUSTED+TRANSFERED+RECVD 
--   - при ошибке выставляет исключение
FUNCTION Calculate_due(
               p_bill_id       IN INTEGER,   -- ID счета
               p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
               p_item_id       IN INTEGER    -- ID позиции счета
           ) RETURN NUMBER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Calculate_due';
    v_due        NUMBER;
BEGIN
    SELECT RECVD-ITEM_TOTAL 
      INTO v_due
      FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND ITEM_ID = p_item_id;
     -- возвращаем сумму начислений и корректировок по счету (то что должно быть оплачено)
    RETURN v_due;
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- проверить возможно ли начисления и корректировки на ITEM
-- начисления возможны, если ITEM еще не вошел в счет фактуру
--   - при ошибке выставляет исключение
FUNCTION Is_chargable (
               p_bill_id       IN INTEGER,  -- ID счета
               p_rep_period_id IN INTEGER,  -- ID отчетного периода счета
               p_item_id       IN INTEGER   -- ID позиции счета
           ) RETURN BOOLEAN
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Is_chargable';
    v_inv_item_id INTEGER;
    v_retcode     BOOLEAN;
BEGIN
    SELECT INV_ITEM_ID INTO v_inv_item_id
      FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND ITEM_ID = p_item_id;
    IF v_inv_item_id IS NULL THEN
        v_retcode := TRUE;   -- ВОЗМОЖНЫ - ITEM НЕ вошел в счет/фактуру 
    ELSE
        v_retcode := FALSE;  -- НЕ возможны - ITEM УЖЕ вошел в счет/фактуру
    END IF;
    RETURN v_retcode;
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================--
-- произвести начисление на позицию счета (item), возвращает:
--   - величину задолженности по позиции DUE=ITEM_TOTAL+ADJUSTED-RECVD 
--   - при ошибке выставляет исключение
FUNCTION Charge_item_value (
               p_bill_id       IN INTEGER,   -- ID счета
               p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
               p_item_id       IN INTEGER,   -- ID позиции счета
               p_value         IN NUMBER,    -- сумма начислений на позицию счета
               p_date_from     IN DATE,      -- временной диапазон оказанной услуги
               p_date_to       IN DATE       -- из event_t
           ) RETURN NUMBER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Charge_item_value';
    v_value      NUMBER := NVL(p_value,0);
    v_count      INTEGER;
    v_due        NUMBER := 0;
BEGIN
    -- произвести начисление на позицию счета, если она еще не вошла в счет-фактуру
    UPDATE ITEM_T
       SET ITEM_TOTAL = ITEM_TOTAL + v_value,
           DATE_FROM = CASE 
                          WHEN DATE_FROM IS NULL THEN p_date_from
                          WHEN p_date_from < DATE_FROM THEN p_date_from 
                       END,
           DATE_TO   = CASE 
                          WHEN DATE_TO IS NULL THEN p_date_to
                          WHEN DATE_TO < p_date_to THEN p_date_to 
                       END,
           LAST_MODIFIED = SYSDATE
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND ITEM_ID = p_item_id
       AND ITEM_TYPE = PK00_CONST.c_ITEM_TYPE_BILL
       AND INV_ITEM_ID IS NULL   -- запись, еше не вошла в счет-фактуру
    RETURNING ITEM_TOTAL-RECVD INTO v_due;
    -- котроль результата выполнения начисления
    IF SQL%ROWCOUNT = 0 THEN
        SELECT COUNT(*) INTO v_count FROM ITEM_T WHERE ITEM_ID = p_item_id;
        IF v_count = 0 THEN
            RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'Отсутствует запись ITEM_T.ITEM_ID='||p_item_id);
        ELSE
            RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'Запись ITEM_T.ITEM_ID='||p_item_id
                                           ||' - уже вошла в счет-фактуру или неверный ITEM_TYPE');
        END IF;
    END IF; 
    -- возвращаем сумму расчитанной задолженнности      
    RETURN v_due;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );  
END;

-- произвести корректировку суммы позиции счета, возвращает:
--   - величину задолженности по позиции DUE=ITEM_TOTAL+ADJUSTED-RECVD
--   - при ошибке выставляет исключение
FUNCTION Adjust_item_value (
               p_bill_id       IN INTEGER,   -- ID счета
               p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
               p_item_id       IN INTEGER,   -- ID позиции счета
               p_value         IN NUMBER     -- новая сумма позиции счет
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Adjust_item_value';
    v_value      NUMBER := NVL(p_value,0);
    v_count      INTEGER;
    v_due        INTEGER := 0;
BEGIN
    -- произвести корректировку суммы позиции счета, если она еще не вошла в счет-фактуру
    UPDATE ITEM_T
       SET ITEM_TOTAL =  v_value,
           LAST_MODIFIED = SYSDATE
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND ITEM_ID = p_item_id
--       AND ITEM_TYPE = PK00_CONST.c_ITEM_TYPE_ADJUST
       AND INV_ITEM_ID IS NULL   -- запись, еше не вошла в счет-фактуру
    RETURNING ITEM_TOTAL-RECVD INTO v_due;
    -- котроль результата выполнения начисления
    IF SQL%ROWCOUNT = 0 THEN
        SELECT COUNT(*) INTO v_count FROM ITEM_T WHERE ITEM_ID = p_item_id;
        IF v_count = 0 THEN
            RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'Отсутствует запись ITEM_T.ITEM_ID='||p_item_id);
        ELSE
            RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'Запись ITEM_T.ITEM_ID='||p_item_id
                                           ||' - уже вошла в счет-фактуру или неверный ITEM_TYPE');
        END IF;
    END IF; 
    -- возвращаем сумму расчитанной задолженнности      
    RETURN v_due;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );  
END;

-- произвести прием средств на позицию счета, возвращает:
--   - величину общую сумму принятых на позизию платежей 
--   - при ошибке выставляет исключение
FUNCTION Recvd_item_value (
               p_bill_id       IN INTEGER,   -- ID счета
               p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
               p_item_id       IN INTEGER,   -- ID позиции счета
               p_value         IN NUMBER     -- сумма начислений на позицию счета
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Recvd_item_value';
    v_value      NUMBER := NVL(p_value,0);
    v_recvd      NUMBER := 0;
BEGIN
    -- произвести начисление на позицию счета, если она еще не вошла в счет-фактуру
    UPDATE ITEM_T
       SET RECVD = RECVD + v_value,
           LAST_MODIFIED = SYSDATE
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND ITEM_ID = p_item_id
     RETURNING RECVD INTO v_recvd; 
    -- котроль результата выполнения начисления
    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'Отсутствует запись ITEM_T(PAYMENT).ITEM_ID='||p_item_id);
    END IF; 
    -- возвращаем сумму расчитанной задолженнности      
    RETURN v_recvd;  
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================--
-- удалить позицию счета, для которой не сформирована позиция инвойса, 
-- т.е. она не вошла в закрытый счет
--   - при ошибке выставляет исключение
-- (когда все устоится добавлю ограничения на удаление в триггер)
PROCEDURE Delete_item(
               p_bill_id       IN INTEGER,   -- ID счета
               p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
               p_item_id       IN INTEGER    -- ID позиции счета
          )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_item';
    v_item_type   ITEM_T.ITEM_TYPE%TYPE;
    v_inv_item_id ITEM_T.INV_ITEM_ID%TYPE;
    v_item_status ITEM_T.ITEM_STATUS%TYPE;
BEGIN
    -- выполняем проверку возможности удаления
    SELECT I.ITEM_TYPE, I.INV_ITEM_ID, I.ITEM_STATUS
      INTO v_item_type, v_inv_item_id, v_item_status
      FROM ITEM_T I
     WHERE I.ITEM_ID = p_item_id
       AND BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- работа возможна только с открытым счетом
    Check_bill_status (p_bill_id, p_rep_period_id);
    --      
    IF v_inv_item_id IS NOT NULL THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
                     'Удаление невозможно, ITEM_ID='||p_item_id||
                     ' уже вошел в INV_TEM_ID='||v_inv_item_id);
    END IF;
    IF v_item_status != Pk00_Const.c_ITEM_STATE_OPEN THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
                     'Удаление невозможно, ITEM_ID='||p_item_id||
                     ' имеет ITEM_STATUS='||v_item_status);
    END IF;
    -- удаление позиции счета
    DELETE FROM ITEM_T 
     WHERE ITEM_ID = p_item_id
       AND BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );  
END;


END PK08_ITEM;
/
