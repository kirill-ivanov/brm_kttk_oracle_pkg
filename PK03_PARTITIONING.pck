CREATE OR REPLACE PACKAGE PK03_PARTITIONING
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK03_PARTITIONING';
    -- ==============================================================================
    --
    -- Работа с секционированными таблицами
    -- Принципы секционирования:
    -- Данные всех секционированных таблиц, за исключением BILL_T 
    -- отправляются в архив по годам.
    -- Один год данных этих таблиц хранится в одном TABLESPACE, правила
    -- именования: TBS_BILL_YYYY, например TBS_BILL_2013, TBS_BILL_2014
    -- Данные BILL_T хранятся в отдельном TABLESPACE - TBS_BILL
    -- Ключевым полем для построения секций является PK табицы, 
    -- формат PK: YYMM.xxx.xxx.xxx, 
    -- где YYMM - год/месяц, xxx.xxx.xxx - зачение из последовательности SQ_BILL_ID
    -- последовательность зациклена от 000.000.000 до 999.999.999 
    -- и в начале месяца не обнуляется. 1 млрд. значений счетчика хватит на несколько месяцев.
    -- Пример PK: 1309.000.123.456 - 2013год, сентябрь, SQ_BILL_ID.NEXTVAL = 000123456 
    -- Секции таблиц строятся по годам (PYYYY), для таблиц 
    --        bill_t,
    --        item_t,
    --        item_transfer_t,
    --        payorder_t
    --        pay_transfer_t
    -- и по месяцам (PYYYYMM), для таблиц
    --        event_t
    --
    -- PS: Возможно позже правила секционирования таблиц уточним.
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- Построить месячные секции в таблице
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    PROCEDURE Make_month_partition(p_period IN DATE);
    
    -- ...
    
END PK03_PARTITIONING;
/
CREATE OR REPLACE PACKAGE BODY PK03_PARTITIONING
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Построить месячные секции в таблице
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
PROCEDURE Make_month_partition(p_period IN DATE)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_month_partition';
BEGIN
    NULL;    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error('ERROR', c_PkgName||'.'||v_prcName);
END;


END PK03_PARTITIONING;
/
