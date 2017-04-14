CREATE OR REPLACE PACKAGE PK09_INVOICE_NEW
IS
    --
    -- Пакет для работы с объектом "ПОЗИЦИЯ СЧЕТА-ФАКТУРЫ", таблицы:
    -- invoice_item_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK09_INVOICE';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Функции расчета/выделения налога
    -- 
    -- возвращает сумму налога на указанную сумму (без налога)
    FUNCTION Calc_tax(
                  p_taxfree_total IN NUMBER,  -- сумма без налога
                  p_tax_rate      IN NUMBER   -- ставка налога в процентах
               ) RETURN NUMBER DETERMINISTIC;
               
    -- возвращает сумму налога из указанной суммы (с налогом)
    FUNCTION Allocate_tax(
                  p_total      IN NUMBER,     -- сумма с налогом
                  p_tax_rate   IN NUMBER      -- ставка налога в процентах
               ) RETURN NUMBER DETERMINISTIC;
        
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Получить имя позиции счета-фактуры для услуги
    --   - при ошибке выставляет исключение
    FUNCTION Get_item_name (
                  p_service_id  IN INTEGER,
                  p_account_id  IN INTEGER,
                  p_contract_id IN INTEGER,
                  p_customer_id IN INTEGER
               ) RETURN VARCHAR2;
    
    -- Расчитать позицию счета-фактуры 
        --   - положительное - ID invoice_item, 
        --   - при ошибке выставляет исключение
    FUNCTION Calc_inv_item (
                   p_bill_id       IN INTEGER,   -- ID позиции счета
                   p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
                   p_service_id    IN INTEGER,   -- ID услуги
                   p_inv_item_no   IN INTEGER,   -- номер строки в счете фактуре
                   p_inv_item_name IN VARCHAR2,  -- имя строки в счете фактуре
                   p_vat           IN NUMBER,    -- ставка налога в процентах
                   p_date_from     IN DATE       -- дата начала оказания услуги, 
               ) RETURN INTEGER;                 -- в с-ф могут входить услуги оказанные 
                                                 -- в разных периодах (довыставление)
    
    -- Расчитать счет-фактуру
    --   - величину начислений по позиции, включая налог 
    --   - при ошибке выставляет исключение
    FUNCTION Calc_invoice (
                   p_bill_id       IN INTEGER,   -- ID позиции счета
                   p_rep_period_id IN INTEGER    -- ID отчетного периода счета
               ) RETURN NUMBER;
               
    -- найти все позиции указанного счета-фактуры
    --   - положительное - кол-во выбранных записей
    --   - при ошибке выставляет исключение
    FUNCTION Invoice_items_list( 
                   p_recordset OUT t_refc, 
                   p_bill_id       IN INTEGER,   -- ID счета
                   p_rep_period_id IN INTEGER    -- ID отчетного периода счета
               ) RETURN INTEGER;
    
    -- Удалить все позиции указанного счета-фактуры
    --   - положительное - кол-во удаленных записей
    --   - при ошибке выставляет исключение
    FUNCTION Delete_invoice_items (
                   p_bill_id       IN INTEGER,   -- ID счета
                   p_rep_period_id IN INTEGER    -- ID отчетного периода счета
               ) RETURN INTEGER;
    
END PK09_INVOICE_NEW;
/
CREATE OR REPLACE PACKAGE BODY PK09_INVOICE_NEW
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Получить имя позиции счета-фактуры для услуги
--   - при ошибке выставляет исключение
FUNCTION Get_item_name (
              p_service_id  IN INTEGER,
              p_account_id  IN INTEGER,
              p_contract_id IN INTEGER,
              p_customer_id IN INTEGER
           ) RETURN VARCHAR2
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Get_item_name';
    v_item_name     INVOICE_ITEM_T.INV_ITEM_NAME%TYPE;
BEGIN
    SELECT SRV_NAME
      INTO v_item_name
      FROM (
            SELECT NVL(SA.SRV_NAME, S.SERVICE) SRV_NAME,
                   CASE
                     WHEN SA.ACCOUNT_ID  = p_account_id  THEN 1
                     WHEN SA.CONTRACT_ID = p_contract_id THEN 2
                     WHEN SA.CUSTOMER_ID = p_customer_id THEN 3
                     ELSE 0
                   END 
              FROM SERVICE_T S, SERVICE_ALIAS_T SA
             WHERE S.SERVICE_ID   = p_service_id
               AND S.SERVICE_ID   = SA.SERVICE_ID(+)
               ORDER BY 2
           )
     WHERE ROWNUM = 1;
     RETURN v_item_name;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Функции расчета/выделения налога
-- 
-- возвращает сумму налога на указанную сумму (без налога)
FUNCTION Calc_tax(
              p_taxfree_total IN NUMBER,  -- сумма без налога
              p_tax_rate      IN NUMBER   -- ставка налога в процентах
           ) RETURN NUMBER DETERMINISTIC
IS
BEGIN    
     RETURN  ROUND(p_taxfree_total * p_tax_rate / 100, 2);
END;

-- возвращает сумму налога из указанной суммы (с налогом)
FUNCTION Allocate_tax(
              p_total      IN NUMBER,     -- сумма с налогом
              p_tax_rate   IN NUMBER      -- ставка налога в процентах
           ) RETURN NUMBER DETERMINISTIC
IS
BEGIN    
     RETURN  p_total - ROUND(p_total /(1 + p_tax_rate / 100),2);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расчитать позицию счета-фактуры 
    --   - положительное - ID invoice_item, 
    --   - при ошибке выставляет исключение
FUNCTION Calc_inv_item (
               p_bill_id       IN INTEGER,   -- ID позиции счета
               p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
               p_service_id    IN INTEGER,   -- ID услуги
               p_inv_item_no   IN INTEGER,   -- номер строки в счете фактуре
               p_inv_item_name IN VARCHAR2,  -- имя строки в счете фактуре
               p_vat           IN NUMBER,    -- ставка налога в процентах
               p_date_from     IN DATE       -- дата начала оказания услуги, 
           ) RETURN INTEGER                  -- в с-ф могут входить услуги оказанные 
IS                                           -- в разных периодах (довыставление)
    v_prcName       CONSTANT VARCHAR2(30) := 'Calc_inv_item';
    v_date_from     DATE;
    v_date_to       DATE;
    v_inv_item_id   INTEGER;
    v_count         INTEGER;
    --
BEGIN
    v_date_from := TRUNC(p_date_from,'mm');
    v_date_to   := ADD_MONTHS(v_date_from,1)-1/86400;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - --
    -- вычисляем id строки счета-фактуры от id строки счета
    v_inv_item_id := PK02_POID.Next_invoice_item_id;
        
    -- формируем строку счета-фактуры
    INSERT INTO INVOICE_ITEM_T (
       BILL_ID, REP_PERIOD_ID,
       INV_ITEM_ID, INV_ITEM_NO, SERVICE_ID, INV_ITEM_NAME, 
       VAT,         -- ставка НДС в процентах
       TOTAL,       -- сумма начислений с налогом
       GROSS,       -- сумма начислений без налога
       TAX,         -- сумма налога
       DATE_FROM, DATE_TO
    )
    SELECT 
         --
         p_bill_id, p_rep_period_id, v_inv_item_id, p_inv_item_no, p_service_id, 
         p_inv_item_name, p_vat,
         -- Полная сумма начислений с налогами -------------
         CASE
           WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN -- налог включен в начисленную сумму
             AMOUNT
           WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_NOT_INCL THEN -- налог не включен
             AMOUNT + PK09_INVOICE.Calc_tax(AMOUNT, p_vat)
           ELSE -- поле не определено
             NULL
         END TOTAL, 
         -- Сумма начислений без налогов -------------------
         CASE
           WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN -- налог включен в начисленную сумму
             AMOUNT - PK09_INVOICE.Allocate_tax(AMOUNT, p_vat)
           ELSE -- налог не включен
             AMOUNT
         END GROSS, 
         -- Полная сумма налога на проведенные начисления --
         CASE
           WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN -- налог включен в начисленную сумму
             PK09_INVOICE.Allocate_tax(AMOUNT, p_vat)
           WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_NOT_INCL THEN -- налог не включен
             PK09_INVOICE.Calc_tax(AMOUNT, p_vat)
           ELSE -- налог не определен
             NULL
         END TAX,
         -- минимальна и максимальные даты оказания услуги --
         DATE_FROM, DATE_TO
      FROM (
        SELECT TAX_INCL, 
           SUM(ITEM_TOTAL) AMOUNT, 
           MIN(
             CASE 
               WHEN v_date_from < O.DATE_FROM THEN TRUNC(O.DATE_FROM)
               ELSE v_date_from  
             END
           ) DATE_FROM,
           MAX(
             CASE 
               WHEN O.DATE_TO < v_date_to THEN TRUNC(O.DATE_TO)
               ELSE v_date_to  
             END
           ) DATE_TO
        FROM ITEM_T I, ORDER_T O
         WHERE I.BILL_ID       = p_bill_id
           AND I.REP_PERIOD_ID = p_rep_period_id
           AND I.SERVICE_ID    = p_service_id
           AND I.ORDER_ID      = O.ORDER_ID
        GROUP BY TAX_INCL
    );
    v_count := SQL%ROWCOUNT;
    IF v_count = 1 THEN
        -- проставляем признак вхождения в счет-фактуру, строкам счета (item)
        UPDATE ITEM_T 
           SET INV_ITEM_ID = v_inv_item_id
         WHERE BILL_ID = p_bill_id
           AND REP_PERIOD_ID = p_rep_period_id
           AND SERVICE_ID = p_service_id
           AND DATE_FROM BETWEEN v_date_from AND v_date_to;
    END IF;   
    -- возвращаем ID созданной позиции счета
    RETURN v_inv_item_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Bill_id='||p_bill_id
                                  ||', service_id='||p_service_id, c_PkgName||'.'||v_prcName );
END;

/*
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расчитать позицию счета-фактуры
    --   - положительное - номер следующей строки в счете-фактуры 
    --     для одной услуги, может быть сформировна более чем одна строка в с-ф,
    --     например когда в с-ф входят услуги из разных периодов
    --   - при ошибке выставляет исключение
FUNCTION Calc_inv_items (
               p_bill_id       IN INTEGER,   -- ID позиции счета
               p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
               p_service_id    IN INTEGER,   -- ID услуги
               p_inv_item_no   IN INTEGER,   -- номер строки в счете фактуре
               p_inv_item_name IN VARCHAR2,  -- имя строки в счете фактуре
               p_vat           IN NUMBER,    -- ставка налога в процентах
               p_account_type  IN ACCOUNT_T.ACCOUNT_TYPE%TYPE -- тип л/с
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Calc_inv_item';
    v_item_total    NUMBER;
    v_adjusted      NUMBER;
    v_date_from     DATE;
    v_date_to       DATE;    
    v_inv_item_id   INTEGER;
    v_gross         NUMBER;               -- сумма без налогов
    v_tax           NUMBER;               -- НДС
    v_total         NUMBER;               -- сумма с налогом
    --
BEGIN
  
    FOR r_item IN (
        SELECT TRUNC(DATE_FROM, 'mm') ITEM_MONTH, TAX_INCL, 
               SUM(ITEM_TOTAL+ADJUSTED) AMOUNT, 
               MIN(DATE_FROM) DATE_FROM, MAX(DATE_TO) DATE_TO
        FROM ITEM_T
         WHERE BILL_ID = p_bill_id
           AND REP_PERIOD_ID = p_rep_period_id
           AND SERVICE_ID = p_service_id
        GROUP BY TRUNC(DATE_FROM, 'mm'), TAX_INCL
    )
    LOOP
        -- Полная сумма начислений с налогами -------------
        IF r_item.tax_incl THEN
            v_total := r_item.amount; 
            v_tax   := Allocate_tax(v_total, p_vat);
            v_gross := v_total - v_tax;
        ELSE -- Сумма начислений без налогов --------------
            v_gross := r_item.amount;
            v_tax   := Calc_tax(v_total, p_vat);
            v_total := v_gross + v_tax;
        END IF;
        v_date_from := r_item.date_from; 
        v_date_to   := r_item.date_to;
        -- - - - - - - - - - - - - - - - - - - - - - - - --
        -- вычисляем id строки счета-фактуры от id строки счета
        v_inv_item_id := PK02_POID.Next_invoice_item_id;
        
        -- формируем строку счета-фактуры
        INSERT INTO INVOICE_ITEM_T (
           BILL_ID, REP_PERIOD_ID,
           INV_ITEM_ID, INV_ITEM_NO, SERVICE_ID,
           VAT,         -- ставка НДС в процентах
           TAX,         -- сумма налога
           GROSS,       -- сумма начислений без налога
           TOTAL,       -- сумма начислений с налогом
           INV_ITEM_NAME, DATE_FROM, DATE_TO
        )VALUES(
           p_bill_id, p_rep_period_id, v_inv_item_id, p_inv_item_no, p_service_id,
           p_vat, v_tax, v_gross, v_total, 
           p_inv_item_name, v_date_from, v_date_to
        );
    
        -- проставляем признак вхождения в счет-фактуру, строкам счета (item)
        UPDATE ITEM_T 
           SET INV_ITEM_ID = v_inv_item_id
         WHERE BILL_ID = p_bill_id
           AND REP_PERIOD_ID = p_rep_period_id
           AND SERVICE_ID = p_service_id;
        
        
    END LOOP;





    -- суммируем начисления по указанной услуге c учетом вхожднеия НДС
    SELECT 
           -- Полная сумма начислений с налогами -------------
           CASE
             WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN -- налог включен в начисленную сумму
               SUM(ITEM_TOTAL+ADJUSTED)
             ELSE -- налог не включен
               SUM(ITEM_TOTAL+ADJUSTED) + PK09_INVOICE.Calc_tax(SUM(ITEM_TOTAL+ADJUSTED), p_vat)
           END TOTAL, 
           -- Сумма начислений без налогов -------------------
           CASE
             WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN -- налог включен в начисленную сумму
               SUM(ITEM_TOTAL+ADJUSTED) - PK09_INVOICE.Allocate_tax(SUM(ITEM_TOTAL+ADJUSTED), p_vat)
             ELSE -- налог не включен
               SUM(ITEM_TOTAL+ADJUSTED)
           END GROSS, 
           -- Полная сумма налога на проведенные начисления --
           CASE
             WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN -- налог включен в начисленную сумму
               PK09_INVOICE.Allocate_tax(SUM(ITEM_TOTAL+ADJUSTED), p_vat)
             ELSE -- налог не включен
               PK09_INVOICE.Calc_tax(SUM(ITEM_TOTAL+ADJUSTED), p_vat);
           END TAX,
           -- минимальна и максимальные даты оказания услуги --
           MIN(DATE_FROM), MAX(DATE_TO)
      INTO v_total, v_gross, v_tax, v_date_from, v_date_to
    FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND SERVICE_ID = p_service_id;

    
    
    
    
    
    
    SUM(ITEM_TOTAL), SUM(ADJUSTED),
           MIN(DATE_FROM), MAX(DATE_TO)
      INTO v_item_total, v_adjusted, v_date_from, v_date_to
      FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND SERVICE_ID = p_service_id;
    
    --
    SELECT SUM(ITEM_TOTAL), SUM(ADJUSTED),
           MIN(DATE_FROM), MAX(DATE_TO)
      INTO v_item_total, v_adjusted, v_date_from, v_date_to
      FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND SERVICE_ID = p_service_id;
       
    -- ---------------------------------------------------------------------------------
    -- Как-то нужно понимать в ITEM_T для данного счета сумма указана с налогами или без
    -- рабочая версия:
    -- - все расчеты для ФИЗ. лиц - c налогами (account_t.business_type = 'P')
    -- - все расчеты для ЮР. лиц  - без налогов (account_t.business_type = 'J')
    -- ---------------------------------------------------------------------------------
    IF p_account_type = PK00_CONST.c_ACC_TYPE_P THEN
       -- в балансе сумма с налогами
       v_total := v_item_total + v_adjusted;
       v_tax   := Allocate_tax(v_total, p_vat);
       v_gross := v_total - v_tax;
    ELSE -- для Юридических лиц:
       -- в балансе - сумма без налогов
       v_gross := v_item_total + v_adjusted;
       v_tax   := Calc_tax(v_total, p_vat);
       v_total := v_gross + v_tax;
    END IF;
    
    -- вычисляем id строки счета-фактуры от id строки счета
    v_inv_item_id := PK02_POID.Next_invoice_item_id;
    
    -- формируем строку счета-фактуры
    INSERT INTO INVOICE_ITEM_T (
       BILL_ID, REP_PERIOD_ID,
       INV_ITEM_ID, INV_ITEM_NO, SERVICE_ID,
       VAT,         -- ставка НДС в процентах
       TAX,         -- сумма налога
       GROSS,       -- сумма начислений без налога
       TOTAL,       -- сумма начислений с налогом
       INV_ITEM_NAME, DATE_FROM, DATE_TO
    )VALUES(
       p_bill_id, p_rep_period_id, v_inv_item_id, p_inv_item_no, p_service_id,
       p_vat, v_tax, v_gross, v_total, 
       p_inv_item_name, v_date_from, v_date_to
    );
    
    -- проставляем признак вхождения в счет-фактуру, строкам счета (item)
    UPDATE ITEM_T 
       SET INV_ITEM_ID = v_inv_item_id
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND SERVICE_ID = p_service_id;
       
    -- возвращаем ID созданной позиции счета
    RETURN v_inv_item_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расчитать счет-фактуру
--   - положительное - кол-во строк в счете-фактуры
--   - при ошибке выставляет исключение
FUNCTION Calc_invoice (
               p_bill_id       IN INTEGER,   -- ID позиции счета
               p_rep_period_id IN INTEGER,   -- ID отчетного периода счета
               p_account_type  IN ACCOUNT_T.ACCOUNT_TYPE%TYPE
           ) RETURN NUMBER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Calc_invoice';
    v_inv_item_id   INTEGER;
    v_count         INTEGER := 0; -- в нулевой строке баланс за предыдущий период
    -- - - - - - - - - - -- 
    v_account_id    INTEGER;
    v_contract_id   INTEGER;
    v_customer_id   INTEGER;
    v_vat           NUMBER;
    v_inv_item_name INVOICE_ITEM_T.INV_ITEM_NAME%TYPE;
BEGIN
    -- получаем налоговую ставку л/с действующую в указанном биллинговом периоде
    SELECT AP.ACCOUNT_ID, AP.CONTRACT_ID, AP.CUSTOMER_ID, AP.VAT
      INTO v_account_id, v_contract_id, v_customer_id, v_vat
      FROM ACCOUNT_PROFILE_T AP, BILL_T B
     WHERE AP.ACCOUNT_ID   = B.ACCOUNT_ID
       AND B.BILL_ID       = p_bill_id
       AND B.REP_PERIOD_ID = p_rep_period_id
       AND AP.DATE_FROM   <= B.BILL_DATE
       AND (AP.DATE_TO IS NULL OR B.BILL_DATE <= AP.DATE_TO);
    
    -- формируем строки счета фактуры для всех видов услуг    
    FOR i IN (
        SELECT DISTINCT SERVICE_ID
          FROM ITEM_T
         WHERE BILL_ID = p_bill_id
           AND REP_PERIOD_ID = p_rep_period_id
           AND SERVICE_ID IS NOT NULL
      )
    LOOP
        -- нумерация с единицы
        v_count := v_count + 1;
        -- получаем имя позиции счета фактуры
        v_inv_item_name := Get_item_name (
                  p_service_id  => i.service_id,
                  p_account_id  => v_account_id,
                  p_contract_id => v_contract_id,
                  p_customer_id => v_customer_id
               );
        -- ID позиции счета фактуры
        v_inv_item_id := Calc_inv_item (
                  p_bill_id       => p_bill_id,      -- ID позиции счета
                  p_rep_period_id => p_rep_period_id,-- ID отчетного периода счета
                  p_service_id    => i.service_id,   -- ID услуги
                  p_inv_item_no   => v_count,        -- номер строки в счете фактуре
                  p_inv_item_name => v_inv_item_name,-- имя строки в счете фактуре
                  p_vat           => v_vat,          -- ставка налога в процентах
                  p_account_type  => p_account_type  -- тип л/с
               );
    END LOOP;
    RETURN v_count;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Расчитать счет-фактуру
--   - положительное - кол-во строк в счете-фактуры
--   - при ошибке выставляет исключение
FUNCTION Calc_invoice (
               p_bill_id       IN INTEGER,   -- ID позиции счета
               p_rep_period_id IN INTEGER    -- ID отчетного периода счета
           ) RETURN NUMBER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Calc_invoice';
    v_inv_item_id   INTEGER;
    v_count         INTEGER := 0; -- в нулевой строке баланс за предыдущий период
    v_period_from   DATE;
    v_period_to     DATE;
    -- - - - - - - - - - -- 
    v_account_id    INTEGER;
    v_contract_id   INTEGER;
    v_customer_id   INTEGER;
    v_vat           NUMBER;
    v_inv_item_name INVOICE_ITEM_T.INV_ITEM_NAME%TYPE;
BEGIN
    --
    v_period_from := Pk04_Period.Period_from(p_rep_period_id);
    v_period_to   := Pk04_Period.Period_to(p_rep_period_id);
    
    -- получаем налоговую ставку л/с действующую в указанном биллинговом периоде
    -- используем последнюю запись профиля в биллинговом периоде
    SELECT ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, VAT
      INTO v_account_id, v_contract_id, v_customer_id, v_vat
    FROM (
       SELECT AP.ACCOUNT_ID, AP.CONTRACT_ID, AP.CUSTOMER_ID, AP.VAT, AP.DATE_FROM,
              MAX(AP.DATE_FROM) OVER (PARTITION BY AP.ACCOUNT_ID) MAX_DATE_FROM
        FROM BILL_T B, ACCOUNT_PROFILE_T AP
       WHERE AP.ACCOUNT_ID   = B.ACCOUNT_ID
         AND B.BILL_ID       = p_bill_id
         AND B.REP_PERIOD_ID = p_rep_period_id
         AND AP.DATE_FROM   <= v_period_to
         AND (AP.DATE_TO IS NULL OR v_period_from <= AP.DATE_TO )
    ) WHERE DATE_FROM = MAX_DATE_FROM;
    
    -- формируем строки счета фактуры для всех видов услуг    
    FOR i IN (
        SELECT SERVICE_ID, v_period_from DATE_FROM
          FROM ITEM_T
         WHERE BILL_ID = p_bill_id
           AND REP_PERIOD_ID = p_rep_period_id
           AND SERVICE_ID IS NOT NULL
           GROUP BY SERVICE_ID
           ORDER BY 1
      )
    LOOP
        -- нумерация с единицы
        v_count := v_count + 1;
        -- получаем имя позиции счета фактуры
        v_inv_item_name := Get_item_name (
                  p_service_id  => i.service_id,
                  p_account_id  => v_account_id,
                  p_contract_id => v_contract_id,
                  p_customer_id => v_customer_id
               );
        -- ID позиции счета фактуры
        v_inv_item_id := Calc_inv_item (
                  p_bill_id       => p_bill_id,      -- ID позиции счета
                  p_rep_period_id => p_rep_period_id,-- ID отчетного периода счета
                  p_service_id    => i.service_id,   -- ID услуги
                  p_inv_item_no   => v_count,        -- номер строки в счете фактуре
                  p_inv_item_name => v_inv_item_name,-- имя строки в счете фактуре
                  p_vat           => v_vat,          -- ставка налога в процентах
                  p_date_from     => i.date_from     -- тип л/с
               );
    END LOOP;
    RETURN v_count;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Bill_id='||p_bill_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- найти все позиции указанного счета-фактуры
--   - положительное - кол-во выбранных записей
--   - при ошибке выставляет исключение
FUNCTION Invoice_items_list( 
               p_recordset OUT t_refc, 
               p_bill_id       IN INTEGER,   -- ID позиции счета
               p_rep_period_id IN INTEGER    -- ID отчетного периода счета
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Invoice_items_list';
    v_retcode    INTEGER;
BEGIN
    -- вычисляем кол-во записей
    SELECT COUNT(*) INTO v_retcode
      FROM INVOICE_ITEM_T
     WHERE BILL_ID = p_bill_id;
    -- возвращаем курсор
    OPEN p_recordset FOR
         SELECT BILL_ID, REP_PERIOD_ID,
                INV_ITEM_ID, INV_ITEM_NO, SERVICE_ID, 
                VAT, TAX, GROSS, TOTAL,
                INV_ITEM_NAME, DATE_FROM, DATE_TO
           FROM INVOICE_ITEM_T
          WHERE BILL_ID = p_bill_id
            AND REP_PERIOD_ID = p_rep_period_id
          ORDER BY INV_ITEM_NO;
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- Удалить все позиции указанного счета-фактуры
--   - положительное - кол-во удаленных записей
--   - при ошибке выставляет исключение
FUNCTION Delete_invoice_items (
               p_bill_id       IN INTEGER,   -- ID позиции счета
               p_rep_period_id IN INTEGER    -- ID отчетного периода счета
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_invoice_items';
BEGIN
    -- удаляем причнак вхождения позиций счета в счет-фактуру
    -- если есть записи в ITEM_T, то при удалении
    -- сработает constraint ITEM_T_INVOICE_ITEM_T_FK
    UPDATE ITEM_T SET INV_ITEM_ID = NULL
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- удаляем все позиции указанного счета-фактуры,
    DELETE 
      FROM INVOICE_ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- возвращает кол-во удаленных записей
    RETURN SQL%ROWCOUNT;
EXCEPTION
    WHEN OTHERS THEN
        RETURN(-Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName));
END;


END PK09_INVOICE_NEW;
/
