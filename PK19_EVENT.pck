CREATE OR REPLACE PACKAGE PK19_EVENT
IS
    --
    -- Пакет для работы с объектом "СОБЫТИЕ", таблицы:
    -- event_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK19_EVENT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- создание объекта новой позиции (item) счета, возвращает:
    --   - ID события (event_id), 
    --   - при ошибке выставляет исключение
    FUNCTION New_event( 
                   p_order_id       IN INTEGER,   -- ID заказа на услугу
                   p_item_id        IN INTEGER,   -- ID позиции счета
                   p_event_type_id  IN INTEGER,   -- ID типа события
                   p_charge_type_id IN INTEGER,   -- ID типа начисления
                   p_date_from      IN DATE,      -- Дата первой записи об услуги
                   p_date_to        IN DATE,      -- Дата последней записи об услуги
                   p_quantity       IN NUMBER,    -- кол-во услуги в натуральных показателях
                   p_bill_amount    IN NUMBER,    -- сумма в валюте счета
                   p_tariff_amount  IN NUMBER     -- сумма в валюте тарифа
               ) RETURN INTEGER;
    
END PK19_EVENT;
/
CREATE OR REPLACE PACKAGE BODY PK19_EVENT
IS

-- создание объекта новой позиции (item) счета, возвращает:
--   - ID события (event_id), 
--   - при ошибке выставляет исключение
FUNCTION New_event( 
               p_order_id       IN INTEGER,   -- ID заказа на услугу
               p_item_id        IN INTEGER,   -- ID позиции счета
               p_event_type_id  IN INTEGER,   -- ID типа события
               p_charge_type_id IN INTEGER,   -- ID типа начисления
               p_date_from      IN DATE,      -- Дата первой записи об услуги
               p_date_to        IN DATE,      -- Дата последней записи об услуги
               p_quantity       IN NUMBER,    -- кол-во услуги в натуральных показателях
               p_bill_amount    IN NUMBER,    -- сумма в валюте счета
               p_tariff_amount  IN NUMBER     -- сумма в валюте тарифа
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'New_event';
    v_item_id    INTEGER;
    v_date_from  DATE;
    v_date_to    DATE;
BEGIN
    -- получаем ID позиции счета
    v_item_id := PK02_POID.Item_Event_id_nextval(p_item_id);
    -- создаем запись позиции счета
    INSERT INTO EVENT_T ( EVENT_ID, EVENT_TYPE_ID, ORDER_ID, ITEM_ID,
         DATE_FROM, DATE_TO, QUANTITY, BILL_AMOUNT, TARIFF_AMOUNT,
         CHARGE_TYPE_ID, SAVE_DATE
    )VALUES( 
         v_item_id, p_event_type_id, p_order_id, p_item_id,
         p_date_from, p_date_to, p_quantity, p_bill_amount, p_tariff_amount,
         p_charge_type_id, SYSDATE
    );
    -- возвращаем ID созданной позиции счета
    RETURN v_item_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


END PK19_EVENT;
/
