CREATE OR REPLACE PACKAGE PK112_PRINT
IS
    --
    -- Пакет для поддержки импорта данных из НБ
    -- event_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK112_PRINT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- ------------------------------------------------------------------------------- --
    -- получить данные для заполнения заголока документов:
    -- счета, счета-фактуры, акта передачи-приема, детализации
    --   - при ошибке выставляет исключение
    PROCEDURE Document_header( 
                   p_recordset    OUT t_refc, 
                   p_rep_period_id IN INTEGER,              -- ID периода счета
                   p_bill_id       IN INTEGER DEFAULT NULL, -- ID счета
                   p_delivery_id   IN INTEGER DEFAULT NULL  -- ID способа доставки счета
               );

    -- ------------------------------------------------------------------------------- --
    -- получить данные для печати строк счета-фактуры
    --   - при ошибке выставляет исключение
    PROCEDURE Invoice_items( 
                   p_recordset    OUT t_refc, 
                   p_rep_period_id IN INTEGER,   -- ID периода счета
                   p_bill_id       IN INTEGER    -- ID счета
               );

    -- ------------------------------------------------------------------------------- --
    -- Получение информации о позициях детализации счета 
    --
    PROCEDURE Bill_item_info (
                   p_recordset    OUT t_refc,
                   p_rep_period_id IN INTEGER,   -- ID периода счета
                   p_bill_id       IN INTEGER    -- ID счета
               );

    -- ------------------------------------------------------------------------------- --
    --           Деталировка к счету за услуги связи по указанной позиции
    -- Услуга: Услуги международной и междугородной телефонной связи
    --   - при ошибке выставляет исключение
    PROCEDURE Item_detail_MGMN( 
                   p_recordset    OUT t_refc, 
                   p_rep_period_id IN INTEGER,   -- ID периода счета
                   p_bill_id       IN INTEGER,   -- ID счета
                   p_item_id       IN INTEGER    -- ID позиции счета
               );

    -- ------------------------------------------------------------------------------- --
    --           Деталировка к счету за услуги связи
    -- Услуга: Услуги международной и междугородной телефонной связи
    --   - при ошибке выставляет исключение
    PROCEDURE Detail_MGMN( 
                   p_recordset    OUT t_refc, 
                   p_rep_period_id IN INTEGER,   -- ID периода счета
                   p_bill_id       IN INTEGER    -- ID счета
               );
    
    
END PK112_PRINT;
/
CREATE OR REPLACE PACKAGE BODY PK112_PRINT
IS

-- ------------------------------------------------------------------------------- --
-- получить данные для заполнения заголока документов:
-- счета, счета-фактуры, акта передачи-приема, детализации
--   - при ошибке выставляет исключение
PROCEDURE Document_header( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,              -- ID периода счета
               p_bill_id       IN INTEGER DEFAULT NULL, -- ID счета
               p_delivery_id   IN INTEGER DEFAULT NULL  -- ID способа доставки счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Document_header';
    v_retcode    INTEGER;
    v_period_to  DATE;
BEGIN
    -- получаем верхнюю границу периода
    v_period_to := Pk04_Period.Period_to(p_rep_period_id);
    -- возвращаем курсор
    OPEN p_recordset FOR
        WITH ADDR_GRP AS    -- адрес грузополучателя(для счет-фактуры) 
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS
                 FROM ACCOUNT_CONTACT_T ac 
                 WHERE AC.ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_GRP -- 'GRP'
                   AND DATE_FROM<= v_period_to AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ),    
             ADDR_JUR AS    -- юр. адрес (для счета)      
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS
                 FROM ACCOUNT_CONTACT_T  
                 WHERE ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_JUR    -- 'JUR'
                   AND DATE_FROM<= v_period_to AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ),
             ADDR_DLV AS    -- адрес доставки
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS
                 FROM ACCOUNT_CONTACT_T  
                 WHERE ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_DLV    -- 'DLV'
                   AND DATE_FROM<= v_period_to AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ),    
             ADDR_CTR AS    -- адрес получателя (исполнителя)
             (SELECT CONTRACTOR_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PHONE_ACCOUNT, PHONE_BILLING, FAX
                 FROM CONTRACTOR_ADDRESS_T  
                 WHERE ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_JUR    -- 'JUR'
                   AND DATE_FROM<= v_period_to AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ),
             SIGNER_R AS    -- подписант руководитель
             (SELECT NVL(S.SIGNER_ROLE, 'Руководитель организации') R_SIGNER_ROLE, 
                     S.SIGNER_NAME R_SIGNER_NAME, 
                     S.ATTORNEY_NO R_ATTORNEY_NO,
                     S.DATE_FROM R_DATE_FROM,
                     S.CONTRACTOR_ID
                FROM SIGNER_T S 
               WHERE S.SIGNER_ROLE_ID = Pk00_Const.c_SIGNER_HEAD    -- 6101 
                 AND S.DATE_FROM<= v_period_to AND (S.DATE_TO IS NULL OR v_period_to <= S.DATE_TO)
               ),
             SIGNER_B AS    -- подписант гл бухгалтер
             (SELECT NVL(S.SIGNER_ROLE, 'Главный бухгалтер') B_SIGNER_ROLE, 
                     S.SIGNER_NAME B_SIGNER_NAME, 
                     S.ATTORNEY_NO B_ATTORNEY_NO,
                     S.DATE_FROM B_DATE_FROM,  
                     S.CONTRACTOR_ID
                FROM SIGNER_T S 
               WHERE S.SIGNER_ROLE_ID = Pk00_Const.c_SIGNER_BOOKER  -- 6102 
                 AND S.DATE_FROM<= v_period_to AND (S.DATE_TO IS NULL OR v_period_to <= S.DATE_TO)
               )
        SELECT -- лицевой счет  - - - - - - - - - - - - - - - - - - - - - - - -
               A.ACCOUNT_ID,                -- ID лицевого счета
               A.ACCOUNT_NO,                -- номер лицевого счета
               -- договор       - - - - - - - - - - - - - - - - - - - - - - - -
               C.CONTRACT_NO,               -- номер договора
               C.DATE_FROM,                 -- дата договора
               -- информация по выставленному счету - - - - - - - - - - - - - -
               B.BILL_ID,                   -- ID счета
               B.BILL_NO,                   -- Номер выставленного счета
               B.BILL_DATE,                 -- Дата счета
               B.TOTAL,                     -- Сумма счета с НДС
               B.GROSS,                     -- Сумма счета без НДС
               B.TAX,                       -- Сумма налогов
               B.CURRENCY_ID,               -- ID валюты счета
               B.ACT_DATE_FROM,             -- диапазон оказания услуг
               B.ACT_DATE_TO,               -- для акта передачи приема
               -- ПОЛУЧАТЕЛЬ/ПРОДАВЕЦ (поставщик услуг) -----------------------
               -- координаты получателя (поставщика)  - - - - - - - - - - - - -
               CR.CONTRACTOR_ID,            -- ID получателя
               CR.CONTRACTOR CONTRACTOR_NAME,               -- поставщик услуг
               CR.SHORT_NAME CONTRACTOR_NAME_SHORT, -- краткое наименование 
               CR.INN CONTRACTOR_INN,
               CR.KPP CONTRACTOR_KPP,
               -- адрес получателя  - - - - - - - - - - - - - - - - - - - - - -
               ADDR_CTR.COUNTRY, 
               ADDR_CTR.ZIP,
               ADDR_CTR.STATE,              -- регион (область, край,...)
               ADDR_CTR.CITY,               -- город
               ADDR_CTR.ADDRESS,            -- строка адреса
               ADDR_CTR.PHONE_ACCOUNT,      -- телефон бухгалтерии 
               ADDR_CTR.PHONE_BILLING,      -- телефон отдела расчетов
               ADDR_CTR.FAX,                -- факс получателя
               -- подписант Руководитель    - - - - - - - - - - - - - - - - - -
               SIGNER_R.R_SIGNER_ROLE,      -- "Руководитель предприятия" или что-то индивидуальное 
               SIGNER_R.R_SIGNER_NAME, 
               SIGNER_R.R_ATTORNEY_NO,      -- номер доверенности руководителя
               SIGNER_R.R_DATE_FROM,        -- доверенность от
               -- подписант Гл.бухгалтер    - - - - - - - - - - - - - - - - - -
               SIGNER_B.B_SIGNER_ROLE,      -- "Главный бухгалтер" или что-то индивидуальное
               SIGNER_B.B_SIGNER_NAME, 
               SIGNER_B.B_ATTORNEY_NO,      -- номер доверенности
               SIGNER_B.B_DATE_FROM,        -- доверенность от 
               -- банк получателя
               CB.BANK_ID,                                            -- ID банка
               CB.BANK_NAME CONTRACTOR_BANK_NAME,                     -- банк получателя
               CB.BANK_CODE CONTRACTOR_BANK_CODE,                     -- БИК
               CB.BANK_CORR_ACCOUNT CONTRACTOR_BANK_CORR_ACCOUNT,     -- корр.счет
               CB.BANK_SETTLEMENT CONTRACTOR_BANK_SETTLEMENT,         -- расчетный счет покупателя
               --
               -- ПЛАТЕЛЬЩИК/ПОКУПАТЕЛЬ ---------------------------------------
               -- координаты покупателя - - - - - - - - - - - - - - - - - - - -
               CS.CUSTOMER_ID,
               CS.CUSTOMER CUSTOMER_NAME,                 -- название компании покупателя
               CS.SHORT_NAME CUSTOMER_NAME_SHORT,-- краткое название компании покупателя
               CS.INN CUSTOMER_INN,
               CS.KPP CUSTOMER_KPP,               
               -- координаты плательщика за покупателя
               PAYER.CUSTOMER_ID PAYER_CUSTOMER_ID,
               PAYER.CUSTOMER    PAYER_CUSTOMER,
               PAYER.SHORT_NAME  PAYER_SHORT,
               PAYER.INN         PAYER_INN,
               PAYER.KPP         PAYER_KPP,
               -- Юридический адрес плательщика
               ADDR_JUR.COUNTRY  JUR_COUNTRY, 
               ADDR_JUR.ZIP      JUR_ZIP, 
               ADDR_JUR.STATE    JUR_STATE, 
               ADDR_JUR.CITY     JUR_CITY, 
               ADDR_JUR.ADDRESS  JUR_ADDRESS,
               -- Адрес грузополучателя
               ADDR_GRP.COUNTRY  GRP_COUNTRY, 
               ADDR_GRP.ZIP      GRP_ZIP, 
               ADDR_GRP.STATE    GRP_STATE, 
               ADDR_GRP.CITY     GRP_CITY, 
               ADDR_GRP.ADDRESS  GRP_ADDRESS,
               -- Почтовый адрес плательщика
               ADDR_DLV.COUNTRY  DLV_COUNTRY, 
               ADDR_DLV.ZIP      DLV_ZIP, 
               ADDR_DLV.STATE    DLV_STATE, 
               ADDR_DLV.CITY     DLV_CITY, 
               ADDR_DLV.ADDRESS  DLV_ADDRESS
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, CONTRACTOR_T CR, 
               CUSTOMER_T CS, CUSTOMER_T PAYER, BILL_T B, CONTRACTOR_BANK_T CB,
               ADDR_GRP, ADDR_JUR, ADDR_DLV, ADDR_CTR,
               SIGNER_R, SIGNER_B
         WHERE A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_J -- 'J'
           AND A.ACCOUNT_ID = B.ACCOUNT_ID
           AND A.ACCOUNT_ID = ADDR_GRP.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = ADDR_JUR.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = ADDR_DLV.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND B.REP_PERIOD_ID  = p_rep_period_id       -- ПЕРИОД
           AND B.TOTAL > 0                              -- Пустые счета не интересуют
           AND B.BILL_TYPE = Pk00_Const.c_BILL_TYPE_REC -- 'B'- только периодические счета
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_READY, Pk00_Const.c_BILL_STATE_CLOSED)
           AND AP.DATE_FROM    <= B.BILL_DATE AND (AP.DATE_TO IS NULL OR B.BILL_DATE <= AP.DATE_TO)
           AND AP.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND CR.CONTRACTOR_ID = ADDR_CTR.CONTRACTOR_ID 
           AND AP.CONTRACT_ID   = C.CONTRACT_ID
           AND AP.CUSTOMER_ID   = CS.CUSTOMER_ID
           AND AP.CUSTOMER_PAYER_ID = PAYER.CUSTOMER_ID(+)
           AND AP.CONTRACTOR_BANK_ID = CB.BANK_ID
           AND CB.DATE_FROM <= SYSDATE AND (CB.DATE_TO IS NULL OR SYSDATE <= CB.DATE_TO)
           AND CR.CONTRACTOR_ID = SIGNER_R.CONTRACTOR_ID(+)
           AND CR.CONTRACTOR_ID = SIGNER_B.CONTRACTOR_ID(+)
           AND (p_bill_id IS NULL OR B.BILL_ID = p_bill_id)
        --   AND (p_delivery_id IS NULL OR A.DELIVERY_ID = p_delivery_id)
        ;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------------- --
-- Строки счета-фактуры
--   - при ошибке выставляет исключение
PROCEDURE Invoice_items( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Invoice_items';
    v_retcode    INTEGER;
BEGIN
    -- вычисляем границы сегмента, где хранятся данные Item для указанного счета
    -- возвращаем курсор
    OPEN p_recordset FOR
     SELECT II.BILL_ID, II.REP_PERIOD_ID, II.INV_ITEM_ID, II.INV_ITEM_NO, II.SERVICE_ID, 
            II.INV_ITEM_NAME NAME,                 -- Наименовние товара
            II.TOTAL         ITEM_TOTAL,           -- Стоимость с налогом
            II.GROSS         ITEM_NETTO,           -- Стоимость без налога
            II.TAX           ITEM_TAX,             -- Сумма налога
            II.VAT           TAX  ,                -- Налоговая ставка
            II.DATE_FROM     USAGE_START,          -- Диапазон дат 
            II.DATE_TO       USAGE_END   ,        -- оказания услуги
            SUM(II.TOTAL) OVER (PARTITION BY II.BILL_ID) , -- Всего к оплате: Стоимость с налогом
            SUM(II.GROSS) OVER (PARTITION BY II.BILL_ID), -- Всего к оплате: Стоимость без налога
            SUM(II.TAX)   OVER (PARTITION BY II.BILL_ID) ITEM_TAX  -- Всего к оплате: Сумма налога
       FROM INVOICE_ITEM_T II
      WHERE II.REP_PERIOD_ID = p_rep_period_id 
        AND II.BILL_ID       = p_bill_id
      ORDER BY II.INV_ITEM_NO 
     ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------------- --
--  Список услуг и их компонентов услуги к счету
--   - при ошибке выставляет исключение
PROCEDURE Service_component_list ( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Service_component_list';
    v_retcode    INTEGER;
BEGIN
    -- построить курсор
    OPEN p_recordset FOR
        WITH LST AS (
        SELECT BILL_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, COUNT(*) NROWS, 
               SUM(ITEM_TOTAL) ITEM_TOTAL, SUM(REP_GROSS) ITEM_GROSS, SUM(REP_TAX) ITEM_TAX 
          FROM ITEM_T I
         WHERE REP_PERIOD_ID = p_rep_period_id
           AND BILL_ID = p_bill_id
           AND ITEM_TOTAL > 0
         GROUP BY BILL_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID
         ORDER BY SERVICE_ID, SUBSERVICE_ID, ORDER_ID
        ) 
        SELECT L.BILL_ID, L.SERVICE_ID, L.SUBSERVICE_ID, L.NROWS,
               L.ITEM_TOTAL, L.ITEM_GROSS, L.ITEM_TAX,
               SUM(L.ITEM_TOTAL) OVER (PARTITION BY L.SERVICE_ID) SERVICE_TOTAL,
               SUM(L.ITEM_TOTAL) OVER (PARTITION BY L.SERVICE_ID, L.SUBSERVICE_ID) SUBSERVICE_TOTAL, 
               S.SERVICE_CODE, S.SERVICE, SS.SUBSERVICE_KEY, SS.SUBSERVICE, O.ORDER_NO 
          FROM LST L, SERVICE_T S, SUBSERVICE_T SS, ORDER_T O
         WHERE L.ORDER_ID      = O.ORDER_ID
           AND L.SERVICE_ID    = S.SERVICE_ID
           AND L.SUBSERVICE_ID = SS.SUBSERVICE_ID(+)
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------------- --
-- Получение информации о позициях детализации счета 
--
PROCEDURE Bill_item_info (
               p_recordset    OUT t_refc,
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bill_item_info';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT A.ACCOUNT_ID,           -- ID лицевого счета
               A.ACCOUNT_NO,           -- Номер лицевого счета
               O.ORDER_ID,             -- ID заказа
               O.ORDER_NO,             -- Номер заказа
               B.BILL_ID,              -- ID счета
               B.BILL_NO,              -- Номер счета
               B.BILL_DATE,            -- Дата счета
               I.ITEM_ID,              -- ID позиции детализации счета
               I.CHARGE_TYPE,          -- Способ начисления
               I.ITEM_TYPE,            -- Тип позиции счета
               S.SERVICE_ID,           -- ID услуги
               CASE
                 WHEN SA.ACCOUNT_ID = A.ACCOUNT_ID THEN SA.SRV_NAME
                 ELSE S.SERVICE 
               END SERVICE,            -- услуга
               SS.SUBSERVICE_ID,       -- ID компонента услуги
               SS.SUBSERVICE           -- rjvgjytyn eckeub
          FROM BILL_T B, ACCOUNT_T A, ORDER_T O, ITEM_T I, 
               SERVICE_T S, SUBSERVICE_T SS, SERVICE_ALIAS_T SA
         WHERE B.ACCOUNT_ID = A.ACCOUNT_ID 
           AND I.BILL_ID    = B.BILL_ID
           AND I.ORDER_ID   = O.ORDER_ID
           AND I.SERVICE_ID = S.SERVICE_ID
           AND I.SUBSERVICE_ID = SS.SUBSERVICE_ID
           AND S.SERVICE_ID    = SA.SERVICE_ID(+)
           AND B.REP_PERIOD_ID = p_rep_period_id
           AND B.BILL_ID       = p_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------------- --
--           Деталировка к счету за услуги связи по указанной позиции
-- Услуга: Услуги международной и междугородной телефонной связи
--   - при ошибке выставляет исключение
PROCEDURE Item_detail_MGMN( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER,   -- ID счета
               p_item_id       IN INTEGER    -- ID позиции счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Item_detail_MGMN';
    v_retcode    INTEGER;
    v_date_from  DATE;
    v_date_to    DATE;
BEGIN
    -- вычисляем границы сегмента
    v_date_from := Pk04_Period.Period_from(p_rep_period_id);
    v_date_to   := Pk04_Period.Period_to(p_rep_period_id);
    -- возвращаем курсор
    OPEN p_recordset FOR
        WITH BDR AS (
            SELECT v_date_to PERIOD_TO, 
                   B.BILL_ID, B.ITEM_ID, B.ORDER_ID, B.SERVICE_ID, B.SUBSERVICE_ID,
                   B.ABN_A, B.ABC_B, B.DIR_B_ID, 
                   COUNT(*) CALLS, SUM(B.BILL_MINUTES) MINUTES, SUM(B.AMOUNT) TOTAL   
              FROM E01_BDR_MMTS_T B
             WHERE B.REP_PERIOD BETWEEN v_date_from AND v_date_to
            GROUP BY TRUNC(B.REP_PERIOD), B.BILL_ID, B.ITEM_ID, B.ORDER_ID, 
                     B.SERVICE_ID, B.SUBSERVICE_ID, B.ABN_A, B.ABC_B, B.DIR_B_ID
        )
        SELECT BDR.ABN_A,                 -- Абонентский номер
               BDR.ABC_B,                 -- Код ызова
               DB.DIRECTION_NAME,         -- Направление
               BDR.CALLS,                 -- Кол-во звонков, шт.
               BDR.MINUTES,               -- Длительность, мин.
               BDR.TOTAL,                 -- Стоимость, руб.
               BDR.PERIOD_TO,             -- Месяц
               BDR.BILL_ID,               -- ID счета
               BDR.ITEM_ID,               -- ID позиции счета
               BDR.SERVICE_ID,            -- ID услуги
               BDR.SUBSERVICE_ID,         -- ID компонента услуги
               BDR.ORDER_ID,              -- ID заказа
               BDR.DIR_B_ID               -- ID напрввления
          FROM BDR, TARIFF_CB.TRF02_DIRECTION DB
         WHERE BDR.DIR_B_ID = DB.DIRECTION_ID(+)
           AND (DB.DIRECTION_ID IS NULL OR BDR.PERIOD_TO BETWEEN DB.DATE_FROM AND DB.DATE_TO)
           AND BDR.BILL_ID = p_bill_id
           AND BDR.ITEM_ID = p_item_id
        ORDER BY BDR.BILL_ID, BDR.ORDER_ID, BDR.ABN_A, BDR.ABC_B
     ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------------- --
--           Деталировка к счету за услуги связи
-- Услуга: Услуги международной и междугородной телефонной связи
--   - при ошибке выставляет исключение
PROCEDURE Detail_MGMN( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Detail_MGMN';
    v_retcode    INTEGER;
    v_date_from  DATE;
    v_date_to    DATE;
BEGIN
    -- вычисляем границы сегмента
    v_date_from := Pk04_Period.Period_from(p_rep_period_id);
    v_date_to   := Pk04_Period.Period_to(p_rep_period_id);
    -- возвращаем курсор
    OPEN p_recordset FOR
        WITH BDR AS (
            SELECT v_date_to PERIOD_TO, 
                   B.BILL_ID, B.ITEM_ID, B.ORDER_ID, B.SERVICE_ID, B.SUBSERVICE_ID,
                   B.ABN_A, B.ABC_B, B.DIR_B_ID, 
                   COUNT(*) CALLS, SUM(B.BILL_MINUTES) MINUTES, SUM(B.AMOUNT) TOTAL   
              FROM E01_BDR_MMTS_T B
             WHERE B.REP_PERIOD BETWEEN v_date_from AND v_date_to
            GROUP BY TRUNC(B.REP_PERIOD), B.BILL_ID, B.ITEM_ID, B.ORDER_ID, 
                     B.SERVICE_ID, B.SUBSERVICE_ID, B.ABN_A, B.ABC_B, B.DIR_B_ID
        )
        SELECT S.SERVICE_SHORT SERVICE,   -- наименование услуги
               SS.SHORTNAME SUBSERVICE,   -- наименование компонента услуги
               O.ORDER_NO,                -- Номер заказа
               BDR.ABN_A,                 -- Абонентский номер
               BDR.ABC_B,                 -- Код ызова
               DB.DIRECTION_NAME,         -- Направление
               BDR.CALLS,                 -- Кол-во звонков, шт.
               BDR.MINUTES,               -- Длительность, мин.
               BDR.TOTAL,                 -- Стоимость, руб.
               BDR.PERIOD_TO,             -- Месяц
               BDR.BILL_ID,               -- ID счета
               BDR.ITEM_ID,               -- ID позиции счета
               BDR.SERVICE_ID,            -- ID услуги
               BDR.SUBSERVICE_ID,         -- ID компонента услуги
               BDR.ORDER_ID,              -- ID заказа
               BDR.DIR_B_ID,              -- ID напрввления
               -- Итог по услуге
               SUM(BDR.TOTAL) OVER (PARTITION BY BDR.SERVICE_ID) SERVICE_TOTAL,
               -- Итог по компоненте услуги/услуге
               SUM(BDR.TOTAL) OVER (PARTITION BY BDR.SERVICE_ID, BDR.SUBSERVICE_ID) SUBSERVICE_TOTAL,
               -- Итог по заказу/компоненте услуги/услуге
               SUM(BDR.TOTAL) OVER (PARTITION BY BDR.SERVICE_ID, BDR.SUBSERVICE_ID, BDR.ORDER_ID) ORDER_TOTAL,
               -- Итог по телефону/заказу/компоненте услуги/услуге
               SUM(BDR.TOTAL) OVER (PARTITION BY BDR.SERVICE_ID, BDR.SUBSERVICE_ID, BDR.ORDER_ID, ABN_A) ABN_TOTAL
          FROM BDR, TARIFF_CB.TRF02_DIRECTION DB, SERVICE_T S, SUBSERVICE_T SS, ORDER_T O
         WHERE BDR.DIR_B_ID = DB.DIRECTION_ID(+)
           AND (DB.DIRECTION_ID IS NULL OR BDR.PERIOD_TO BETWEEN DB.DATE_FROM AND DB.DATE_TO)
           AND BDR.ORDER_ID      = O.ORDER_ID
           AND BDR.SERVICE_ID    = S.SERVICE_ID
           AND BDR.SUBSERVICE_ID = SS.SUBSERVICE_ID(+)
           AND BDR.BILL_ID = p_bill_id
        ORDER BY BDR.BILL_ID, BDR.ORDER_ID, BDR.ABN_A, BDR.ABC_B
     ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;


---------------------------------------------------------------------------------


END PK112_PRINT;
/
