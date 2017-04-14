CREATE OR REPLACE PACKAGE PK112_PRINT_V2
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
             p_bill_id       IN INTEGER DEFAULT NULL  -- ID счета
    );
    
--====================================================================================
-- Загрузка дополнительных параметров для счета
PROCEDURE Document_header_ext_param( 
             p_recordset     OUT t_refc,
             p_bill_id       IN INTEGER,      -- ID счета              
             p_period_id     IN INTEGER      -- ID периода счета
);    
   
-- ------------------------------------------------------------------------------- --
-- Получить подписантов по заданными ID
PROCEDURE Document_signer_info( 
           p_recordset          OUT t_refc,
           p_signer_header_id   IN  INTEGER,
           p_signer_booker_id   IN  INTEGER,
           p_stamp_id           IN  INTEGER
    );      

-- Получить подписантов по поставщику (BRANCH_ID у договора)
PROCEDURE Document_signer_by_contractor( 
           p_recordset          OUT t_refc,
           p_contractor_id      IN  NUMBER
);

---------------------------------------------------------------------------------------
-- Получить подписантов по поставщику (BRANCH_ID у договора)
PROCEDURE Document_signer_by_contractor( 
           p_recordset          OUT t_refc,
           p_contractor_id      IN  NUMBER,
           p_date_to            IN  DATE
);

 -- ------------------------------------------------------------------------------- --
    -- получить данные для заполнения заголока документов при:
    -- отправке адресов
    --   - при ошибке выставляет исключение

    PROCEDURE Document_header_adr( 
               p_recordset     OUT t_refc,
               p_rep_period_id IN INTEGER,              -- ID периода счета
               p_account_id    IN INTEGER DEFAULT NULL  -- ID счета              
           );           
  
/*
    PROCEDURE Document_header_adr( 
               p_recordset     OUT t_refc, 
               p_rep_period_id IN INTEGER,              -- ID периода счета
               p_account_id    IN INTEGER DEFAULT NULL  -- ID счета              
           );           
*/
    -- ------------------------------------------------------------------------------- --
    -- получить данные для печати строк счета-фактуры
    --   - при ошибке выставляет исключение
    PROCEDURE Invoice_items( 
                   p_recordset    OUT t_refc, 
                   p_rep_period_id IN INTEGER,   -- ID периода счета
                   p_bill_id       IN INTEGER    -- ID счета
               );

-- ------------------------------------------------------------------------------- --
-- Строки счета-фактуры для разового счета
--   - при ошибке выставляет исключение
PROCEDURE Invoice_items_for_manual( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           );
-- ------------------------------------------------------------------------------- --           
-- Строки корректирующей счета-фактуры
--   - при ошибке выставляет исключение
PROCEDURE Invoice_items_for_correct( 
               p_recordset     OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_new_bill_id   IN INTEGER,   -- ID дебетового счета
               p_old_bill_id   IN INTEGER    -- ID счета от которого строим корректировки. Если не задан, значит от предыдущего дебетового (должны быть связки в bill_t)
);  

-- ------------------------------------------------------------------------------- --
--  Список счетов, которые унаследованы от текущего дебетового
--   - при ошибке выставляет исключение
PROCEDURE BILL_HISTORY_FOR_DEBET ( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
);
                    
    -- ------------------------------------------------------------------------------- --
    --  Список услуг и их компонентов услуги к счету
    --   - при ошибке выставляет исключение

    PROCEDURE detail_item_list ( 
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
    
-----------------------------------------------------------------
-- Деталировка для АРМ массовой печати (временный костыль, пока массовая печать не переедет в новый АРМ)
-----------------------------------------------------------------
PROCEDURE MakeMGMN_Jur_DETAIL(p_recordset OUT t_refc, p_rep_period IN INTEGER, p_bill_id IN INTEGER);

--=========================================================================================--
-- ЗДЕСЬ ИДЕТ блок с запросами на уникальные запросы
-- ------------------------------------------------------------------------------- --
--  Позвонковка для Маяка (это у них такая детализация)
PROCEDURE pozvonkovka_MAYAK ( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           );

--=======================================================================
--Детализированные данны для услуги "Трафик IP (объемный)"
--=======================================================================           
PROCEDURE LOAD_BILL_DETAIL_TRAFFIC_IP(
          p_recordset     OUT t_refc,
          p_rep_period_id IN INTEGER,   -- ID периода счета
          p_bill_id       IN INTEGER,    -- ID счета
          p_item_id       IN INTEGER     -- ID позиции счета
);

--=======================================================================
--Детализированные данны для услуги "Трафик IP Burst"
--=======================================================================
procedure LOAD_BILL_DETAIL_TRAFFIC_BURST(
          p_recordset     OUT t_refc,
          p_rep_period_id IN INTEGER,   -- ID периода счета
          p_bill_id       IN INTEGER,    -- ID счета
          p_item_id       IN INTEGER     -- ID позиции счета
);

--=======================================================================
--Детализированные данны для услуги "Трафик IP VPN"
--=======================================================================
procedure LOAD_BILL_DETAIL_TRAFFIC_VPN(
          p_recordset     OUT t_refc,
          p_rep_period_id IN INTEGER,   -- ID периода счета
          p_bill_id       IN INTEGER,    -- ID счета
          p_item_id       IN INTEGER     -- ID позиции счета
);           
   
    -- ------------------------------------------------------------------------------- --
    --           Деталировка к счету за услуги связи по указанной позиции
    -- Услуга: Услуги международной и междугородной телефонной связи
    --   - при ошибке выставляет исключение
    PROCEDURE LOAD_BILL_DETAIL_TRAFFIC_MGMN( 
                   p_recordset    OUT t_refc, 
                   p_rep_period_id IN INTEGER,   -- ID периода счета
                   p_bill_id       IN INTEGER,   -- ID счета
                   p_item_id       IN INTEGER    -- ID позиции счета
               );

-- ------------------------------------------------------------------------------- --
--           Деталировка к счету за услуги связи по указанной позиции
-- Услуга: межоператорка
--   - при ошибке выставляет исключение
     PROCEDURE LOAD_BILL_DETAIL_TRAFFIC_OPER( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER,   -- ID счета
               p_item_id       IN INTEGER    -- ID позиции счета
           );

-- ------------------------------------------------------------------------------- --
--  Создание таблицы с данными по межоператорке по периоду
PROCEDURE CREATE_BILL_DETAIL_OPER_TMP( 
          p_rep_period_id IN INTEGER
);

--=======================================================================
-- Преобразование адреса в строку
--=======================================================================
function address_to_string(
         p_zip           IN VARCHAR2,
         p_state         IN  VARCHAR2,
         p_city          IN  VARCHAR2,
         p_address       IN  VARCHAR2
) return VARCHAR2 ;
 
END PK112_PRINT_V2;
/
CREATE OR REPLACE PACKAGE BODY PK112_PRINT_V2
IS

-- ------------------------------------------------------------------------------- --
-- получить данные для заполнения заголока документов:
-- счета, счета-фактуры, акта передачи-приема, детализации
--   - при ошибке выставляет исключение
PROCEDURE Document_header( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,              -- ID периода счета
               p_bill_id       IN INTEGER DEFAULT NULL  -- ID счета
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
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON
                 FROM ACCOUNT_CONTACT_T ac 
                 WHERE AC.ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_GRP -- 'GRP'
                   AND DATE_FROM<= v_period_to AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ),    
             ADDR_JUR AS    -- юр. адрес (для счета)      
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON
                 FROM ACCOUNT_CONTACT_T  
                 WHERE ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_JUR    -- 'JUR'
                   AND DATE_FROM<= v_period_to AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ),
             ADDR_DLV AS    -- адрес доставки
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON
                 FROM ACCOUNT_CONTACT_T  
                 WHERE ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_DLV    -- 'DLV'
                   AND DATE_FROM<= v_period_to 
                   AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ),    
             ADDR_CTR AS    -- адрес получателя (исполнителя)
             (SELECT CONTRACTOR_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PHONE_ACCOUNT, PHONE_BILLING, FAX
                 FROM CONTRACTOR_ADDRESS_T  
                 WHERE ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_JUR    -- 'JUR'
                   AND DATE_FROM<= v_period_to 
                   AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ),
             SIGNER_R AS    -- подписант руководитель
             (
               select * from (
                 SELECT NVL(S.SIGNER_ROLE, 'Руководитель') R_SIGNER_ROLE, 
                         S.SIGNER_NAME R_SIGNER_NAME, 
                         S.ATTORNEY_NO R_ATTORNEY_NO,
                         S.DATE_FROM R_DATE_FROM,  
                         S.CONTRACTOR_ID,
                         P.*,
                         ROW_NUMBER() OVER (PARTITION BY S.CONTRACTOR_ID ORDER BY S.PRIORITY) rn
                    FROM SIGNER_T S, MANAGER_T M, PICTURE_T P  
                   WHERE S.SIGNER_ROLE_ID = Pk00_Const.c_SIGNER_HEAD  -- 6101
                     AND S.DATE_FROM<= v_period_to 
                     AND (S.DATE_TO IS NULL OR v_period_to <= S.DATE_TO)
                     AND S.MANAGER_ID = M.MANAGER_ID
                     AND M.SIGN_PICTURE_ID = P.PICTURE_ID(+)
                     )where rn=1
               ),
             SIGNER_B AS    -- подписант гл бухгалтер
             (
               select * from (
                 SELECT NVL(S.SIGNER_ROLE, 'Главный бухгалтер') B_SIGNER_ROLE, 
                         S.SIGNER_NAME B_SIGNER_NAME, 
                         S.ATTORNEY_NO B_ATTORNEY_NO,
                         S.DATE_FROM B_DATE_FROM,  
                         S.CONTRACTOR_ID,
                         P.*,
                         ROW_NUMBER() OVER (PARTITION BY S.CONTRACTOR_ID ORDER BY S.PRIORITY) rn
                    FROM SIGNER_T S, MANAGER_T M, PICTURE_T P  
                   WHERE S.SIGNER_ROLE_ID = Pk00_Const.c_SIGNER_BOOKER  -- 6102 
                     AND S.DATE_FROM<= v_period_to 
                     AND (S.DATE_TO IS NULL OR v_period_to <= S.DATE_TO)
                     AND S.MANAGER_ID = M.MANAGER_ID
                     AND M.SIGN_PICTURE_ID = P.PICTURE_ID(+)
                     )where rn=1
               ),
               STAMP as (
                 SELECT * FROM ( 
                     SELECT 
                        s.contractor_id, 
                        p.*,
                        ROW_NUMBER() OVER (PARTITION BY S.CONTRACTOR_ID ORDER BY S.DATE_FROM DESC) rn
                      FROM stamp_t s, picture_t p
                     WHERE 
                        s.picture_Id = p.picture_Id
                        AND S.DATE_FROM<= v_period_to 
                        AND (S.DATE_TO IS NULL OR v_period_to <= S.DATE_TO)
                  )
                  WHERE RN = 1
               ),
               D_MARK_SEG AS (
                  SELECT KEY_ID, NAME FROM DICTIONARY_T
                  WHERE PARENT_ID = 63
               ),
               D_CL_TYPE AS (
                  SELECT KEY_ID, NAME FROM DICTIONARY_T
                  WHERE PARENT_ID = 64
               ),
               D_CONTR_TYPE AS (
                  SELECT KEY_ID, NOTES NAME, NOTES_TVOR NAME_TVOR FROM DICTIONARY_T
                  WHERE PARENT_ID = 71
               ),
                  PRINT_DOC_EXC AS (
                     SELECT *
                        FROM (SELECT b.account_id bill_account_id, exc.*,
                                     ROW_NUMBER ()
                                     OVER (
                                        ORDER BY
                                           CASE WHEN exc.ACCOUNT_ID IS NOT NULL THEN 1 ELSE 2 END)
                                        rn
                                FROM bill_t b, print_documents_exclude_t exc
                               WHERE     (   b.account_id = exc.account_Id
                                          OR b.contract_id = exc.contract_id)
                                     AND b.bill_id = p_bill_id)
                       WHERE rn = 1           
                  )
        SELECT -- лицевой счет  - - - - - - - - - - - - - - - - - - - - - - - -
               A.ACCOUNT_ID,                -- ID лицевого счета
               A.ACCOUNT_NO,                -- номер лицевого счета
               -- договор       - - - - - - - - - - - - - - - - - - - - - - - -
               C.CONTRACT_NO,               -- номер договора
               C.DATE_FROM,                 -- дата договора
               C.MARKET_SEGMENT_ID,
               D_MARK_SEG.NAME MARKET_SEGMENT_NAME,
               C.CLIENT_TYPE_ID,
               D_CL_TYPE.NAME CLIENT_TYPE_NAME,
               C.CONTRACT_TYPE_ID,
               D_CONTR_TYPE.NAME CONTRACT_TYPE_NAME,
               D_CONTR_TYPE.NAME_TVOR CONTRACT_TYPE_NAME_TVOR,
               -- информация по выставленному счету - - - - - - - - - - - - - -
               B.BILL_ID,                   -- ID счета
               B.REP_PERIOD_ID,             -- Период счета
               B.BILL_NO,                   -- Номер выставленного счета
               B.BILL_TYPE,                 -- Тип счета
               B.BILL_DATE,                 -- Дата счета
               B.TOTAL,                     -- Сумма счета с НДС
               B.GROSS,                     -- Сумма счета без НДС
               AP.VAT TAX,                  -- Процент налона
               --B.TAX,                       -- Сумма налогов
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
               -- - - - - - - - - - - - - - - - - - - - - - -
               AP.BRANCH_ID,                -- регион
               AP.AGENT_ID,                 -- агент
               -- адрес получателя  - - - - - - - - - - - - - - - - - - - - - -
               ADDR_CTR.COUNTRY, 
               ADDR_CTR.ZIP,
               ADDR_CTR.STATE,              -- регион (область, край,...)
               ADDR_CTR.CITY,               -- город
               ADDR_CTR.ADDRESS,            -- строка адреса
               ADDR_CTR.PHONE_ACCOUNT,      -- телефон бухгалтерии 
               ADDR_CTR.PHONE_BILLING,      -- телефон отдела расчетов
               ADDR_CTR.FAX,                -- факс получателя               
               NULL PERSON,                 -- загрушка (обязательно поле для получения адреса)...
               -- адрес получателя (головного офиса) - - - - - - - - - - - - - - - - - - - - - -
               ADDR_CTR_PARENT.COUNTRY PARENT_COUNTRY, 
               ADDR_CTR_PARENT.ZIP PARENT_ZIP,
               ADDR_CTR_PARENT.STATE PARENT_STATE,              -- регион (область, край,...)
               ADDR_CTR_PARENT.CITY PARENT_CITY,               -- город
               ADDR_CTR_PARENT.ADDRESS PARENT_ADDRESS,            -- строка адреса
               NULL PARENT_PERSON,                 -- загрушка (обязательно поле для получения адреса)...
               -- подписант Руководитель    - - - - - - - - - - - - - - - - - -
               SIGNER_R.R_SIGNER_ROLE,      -- "Руководитель предприятия" или что-то индивидуальное 
               SIGNER_R.R_SIGNER_NAME, 
               SIGNER_R.R_ATTORNEY_NO,      -- номер доверенности руководителя
               SIGNER_R.R_DATE_FROM,        -- доверенность от
               SIGNER_R.PICTURE_ID R_PICTURE_ID,
               SIGNER_R.PICTURE R_PICTURE,               
               -- подписант Гл.бухгалтер    - - - - - - - - - - - - - - - - - -
               SIGNER_B.B_SIGNER_ROLE,      -- "Главный бухгалтер" или что-то индивидуальное
               SIGNER_B.B_SIGNER_NAME, 
               SIGNER_B.B_ATTORNEY_NO,      -- номер доверенности
               SIGNER_B.B_DATE_FROM,        -- доверенность от 
               SIGNER_B.PICTURE_ID B_PICTURE_ID,
               SIGNER_B.PICTURE B_PICTURE,
               -- штамп
               STAMP.PICTURE_ID STAMP_PICTURE_ID,
               STAMP.PICTURE    STAMP_PICTURE,               
               -- банк получателя (может быть филиалом)
               CB.BANK_ID,                                            -- ID банка
               CB.BANK_NAME CONTRACTOR_BANK_NAME,                     -- банк получателя
               CB.BANK_CODE CONTRACTOR_BANK_CODE,                     -- БИК
               CB.BANK_CORR_ACCOUNT CONTRACTOR_BANK_CORR_ACCOUNT,     -- корр.счет
               CB.BANK_SETTLEMENT CONTRACTOR_BANK_SETTLEMENT,         -- расчетный счет покупателя
               -- Если получатель - филиал (то это инфа его головного офиса
               CR_PARENT.CONTRACTOR_ID CONTRACTOR_PARENT_ID,            -- ID получателя
               CR_PARENT.CONTRACTOR CONTRACTOR_PARENT_NAME,               -- поставщик услуг
               CR_PARENT.SHORT_NAME CONTRACTOR_PARENT_NAME_SHORT, -- краткое наименование 
               CR_PARENT.INN CONTRACTOR_PARENT_INN,
               CR_PARENT.KPP CONTRACTOR_PARENT_KPP,
               --
               -- ПЛАТЕЛЬЩИК/ПОКУПАТЕЛЬ ---------------------------------------
               -- координаты покупателя - - - - - - - - - - - - - - - - - - - -
               CS.CUSTOMER_ID,
               CM.COMPANY_NAME CUSTOMER_NAME,      -- название компании покупателя
               CM.SHORT_NAME CUSTOMER_NAME_SHORT,  -- краткое название компании покупателя
               CS.INN CUSTOMER_INN,
               CS.KPP CUSTOMER_KPP,               
               -- координаты плательщика за покупателя
               PAYER.CUSTOMER_PAYER_ID PAYER_CUSTOMER_ID,
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
               ADDR_JUR.PERSON   JUR_PERSON,
               -- Адрес грузополучателя
               ADDR_GRP.COUNTRY  GRP_COUNTRY, 
               ADDR_GRP.ZIP      GRP_ZIP, 
               ADDR_GRP.STATE    GRP_STATE, 
               ADDR_GRP.CITY     GRP_CITY, 
               ADDR_GRP.ADDRESS  GRP_ADDRESS,
               ADDR_GRP.PERSON   GRP_PERSON,
               -- Почтовый адрес плательщика
               ADDR_DLV.COUNTRY  DLV_COUNTRY, 
               ADDR_DLV.ZIP      DLV_ZIP, 
               ADDR_DLV.STATE    DLV_STATE, 
               ADDR_DLV.CITY     DLV_CITY, 
               ADDR_DLV.ADDRESS  DLV_ADDRESS,
               ADDR_DLV.PERSON   DLV_PERSON,
               A.BILLING_ID,
               PRINT_DOC_EXC.JASPER_BILL,
               PRINT_DOC_EXC.JASPER_AKT,
               PRINT_DOC_EXC.JASPER_FACTURA,
               PRINT_DOC_EXC.JASPER_DETAIL,
               PRINT_DOC_EXC.FUNCTION_BILL,
               PRINT_DOC_EXC.FUNCTION_AKT,
               PRINT_DOC_EXC.FUNCTION_FACTURA,
               PRINT_DOC_EXC.FUNCTION_DETAIL
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, CONTRACTOR_T CR, 
               CUSTOMER_T CS, 
               COMPANY_T  CM,
               CUSTOMER_PAYER_T PAYER, 
               BILL_T B, 
               CONTRACTOR_BANK_T CB, ADDR_GRP, ADDR_JUR, ADDR_DLV, ADDR_CTR, ADDR_CTR ADDR_CTR_PARENT,
               SIGNER_R, SIGNER_B,STAMP,
               CONTRACTOR_T CR_PARENT,
               D_CL_TYPE,
               D_MARK_SEG,
               D_CONTR_TYPE,               
               PRINT_DOC_EXC
         WHERE A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_J -- 'J'
           AND A.ACCOUNT_ID = B.ACCOUNT_ID           
           AND A.ACCOUNT_ID = ADDR_GRP.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = ADDR_JUR.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = ADDR_DLV.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID           
           AND B.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND B.CONTRACTOR_BANK_ID = CB.BANK_ID
           AND B.CONTRACT_ID = C.CONTRACT_ID
           AND AP.PROFILE_ID = B.PROFILE_ID
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_READY, Pk00_Const.c_BILL_STATE_CLOSED, Pk00_Const.c_BILL_STATE_OPEN, Pk00_Const.c_BILL_STATE_CHECK,Pk00_Const.c_BILL_STATE_PREPAID)
           AND NVL(CR.PARENT_ID, 1) = CR_PARENT.CONTRACTOR_ID            
           AND CR_PARENT.CONTRACTOR_ID = ADDR_CTR_PARENT.CONTRACTOR_ID
           AND CR.CONTRACTOR_ID = ADDR_CTR.CONTRACTOR_ID 
           AND AP.CONTRACT_ID   = CM.CONTRACT_ID
           AND CM.DATE_FROM     < SYSDATE
           AND (CM.DATE_TO IS NULL OR SYSDATE < CM.DATE_TO)
           AND AP.CUSTOMER_ID   = CS.CUSTOMER_ID
           AND AP.CUSTOMER_PAYER_ID = PAYER.CUSTOMER_PAYER_ID(+)           
           AND CB.DATE_FROM <= SYSDATE AND (CB.DATE_TO IS NULL OR SYSDATE <= CB.DATE_TO)
           AND AP.BRANCH_ID = SIGNER_R.CONTRACTOR_ID(+)
           AND AP.BRANCH_ID = SIGNER_B.CONTRACTOR_ID(+)
           AND AP.BRANCH_ID = STAMP.contractor_id(+)
           AND D_CL_TYPE.KEY_ID (+)= C.CLIENT_TYPE_ID
           AND D_MARK_SEG.KEY_ID (+)= C.MARKET_SEGMENT_ID 
           AND D_CONTR_TYPE.KEY_ID (+)=C.CONTRACT_TYPE_ID
           and a.account_Id = PRINT_DOC_EXC.bill_account_id (+)
           AND B.REP_PERIOD_ID  = p_rep_period_id       -- ПЕРИОД
           AND B.BILL_ID = p_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;

--====================================================================================
-- Загрузка дополнительных параметров для счета
PROCEDURE Document_header_ext_param( 
             p_recordset     OUT t_refc,
             p_bill_id       IN INTEGER,      -- ID счета              
             p_period_id     IN INTEGER       -- ID периода счета
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Document_header_ext_param';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR
         SELECT 
              PARAM_NAME,
              PARAM_VALUE,
              TYPE_DOC
            FROM (SELECT 
                         TYPE_DOC,
                         PARAM_NAME,
                         PARAM_VALUE,               
                         ROW_NUMBER ()
                            OVER (PARTITION BY BILL_ID, TYPE_DOC, PARAM_NAME ORDER BY WT)
                            rn
                    FROM (SELECT B.BILL_ID,
                                 EXC.TYPE_DOC,
                                 EXC.PARAM_NAME,
                                 PARAM_VALUE,
                                 CASE
                                    WHEN exc.BILL_ID IS NOT NULL THEN 1
                                    WHEN exc.ACCOUNT_ID IS NOT NULL THEN 2
                                    WHEN exc.CONTRACT_ID IS NOT NULL THEN 3
                                    WHEN exc.CONTRACTOR_ID IS NOT NULL THEN 4
                                    ELSE 999
                                 END
                                    WT
                            FROM bill_t b, PRINT_DOCUMENTS_EXT_PARAM exc
                           WHERE     B.BILL_ID = p_bill_id
                                 AND (   B.BILL_ID = exc.BILL_ID
                                      OR B.ACCOUNT_ID = exc.ACCOUNT_ID
                                      OR B.CONTRACT_ID = exc.CONTRACT_ID
                                      OR B.CONTRACTOR_ID = exc.CONTRACTOR_ID)))
           WHERE rn = 1;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;



-- ------------------------------------------------------------------------------- --
-- Получить подписантов по заданными ID
PROCEDURE Document_signer_info( 
           p_recordset          OUT t_refc,
           p_signer_header_id   IN  INTEGER,
           p_signer_booker_id   IN  INTEGER,
           p_stamp_id           IN  INTEGER
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Document_header_with_signer';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR    
          SELECT *
            FROM (       
                  SELECT NVL(S.SIGNER_ROLE, 'Руководитель организации') R_SIGNER_ROLE, 
                         S.SIGNER_NAME R_SIGNER_NAME, 
                         S.ATTORNEY_NO R_ATTORNEY_NO,
                         S.DATE_FROM R_DATE_FROM,
                         S.CONTRACTOR_ID,
                         P.PICTURE R_PICTURE
                    FROM SIGNER_T S, MANAGER_T M, PICTURE_T P 
                   WHERE S.MANAGER_ID = M.MANAGER_ID
                     AND M.SIGN_PICTURE_ID = P.PICTURE_ID(+)
                     AND S.SIGNER_ID = p_signer_header_id 
                 ) a FULL OUTER JOIN
                  (       
                  SELECT NVL(S.SIGNER_ROLE, 'Главный бухгалтер') B_SIGNER_ROLE, 
                         S.SIGNER_NAME B_SIGNER_NAME, 
                         S.ATTORNEY_NO B_ATTORNEY_NO,
                         S.DATE_FROM B_DATE_FROM,  
                         S.CONTRACTOR_ID,
                         P.PICTURE B_PICTURE
                    FROM SIGNER_T S, MANAGER_T M, PICTURE_T P  
                   WHERE S.MANAGER_ID = M.MANAGER_ID
                     AND M.SIGN_PICTURE_ID = P.PICTURE_ID(+)
                     AND S.SIGNER_ID = p_signer_booker_id
                 ) b ON (1=1) FULL OUTER JOIN
                 (       
                  SELECT 
                    s.contractor_id, 
                    p.PICTURE STAMP_PICTURE
                  FROM stamp_t s, picture_t p
                 WHERE 
                    s.picture_Id = p.picture_Id
                    and s.stamp_id = p_stamp_id
                 ) c           
                 ON 1=1;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;

---------------------------------------------------------------------------------------
-- Получить подписантов по поставщику (BRANCH_ID у договора)
PROCEDURE Document_signer_by_contractor( 
           p_recordset          OUT t_refc,
           p_contractor_id      IN  NUMBER
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Document_signer_by_contractor';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR    
          SELECT *
                FROM (
                SELECT NVL (SIGNER_ROLE,
                                  'Руководитель организации')
                                R_SIGNER_ROLE,
                             SIGNER_NAME R_SIGNER_NAME,
                             ATTORNEY_NO R_ATTORNEY_NO,
                             DATE_FROM R_DATE_FROM,               
                             PICTURE_ID R_PICTURE_ID,
                             PICTURE R_PICTURE
                        FROM (SELECT b.*,
                                     ROW_NUMBER ()
                                        OVER (ORDER BY PRIORITY)
                                        rn
                                FROM (SELECT S.SIGNER_ID,
                                             S.CONTRACTOR_ID,
                                             S.MANAGER_ID,
                                             S.SIGNER_NAME,
                                             S.ATTORNEY_NO,
                                             S.SIGNER_ROLE_ID,
                                             S.SIGNER_ROLE,
                                             S.DATE_FROM,
                                             S.DATE_TO,
                                             CASE
                                                WHEN S.CONTRACTOR_ID = 1 THEN 9999999
                                                ELSE S.PRIORITY
                                             END
                                                PRIORITY,
                                             M.SIGN_PICTURE_ID,
                                             P.*
                                        FROM SIGNER_T S,
                                             CONTRACTOR_T C,
                                             MANAGER_T M,
                                             PICTURE_T P
                                       WHERE     C.CONTRACTOR_ID = S.CONTRACTOR_ID
                                             AND M.MANAGER_ID = S.MANAGER_ID
                                             AND P.PICTURE_ID(+) = M.SIGN_PICTURE_ID
                                             AND S.SIGNER_ROLE_ID = 6101
                                             AND (   S.CONTRACTOR_ID = p_contractor_id
                                                  OR S.CONTRACTOR_ID = 1)) b)
                       WHERE rn = 1
                       ) a
                     FULL OUTER JOIN
                     (SELECT NVL (SIGNER_ROLE, 'Главный бухгалтер')
                                B_SIGNER_ROLE,
                             SIGNER_NAME B_SIGNER_NAME,
                             ATTORNEY_NO B_ATTORNEY_NO,
                             DATE_FROM B_DATE_FROM,
                             PICTURE_ID B_PICTURE_ID,
                             PICTURE B_PICTURE
                        FROM (SELECT b.*,
                                     ROW_NUMBER ()
                                        OVER (ORDER BY PRIORITY)
                                        rn
                                FROM (SELECT S.SIGNER_ID,
                                             S.CONTRACTOR_ID,
                                             S.MANAGER_ID,
                                             S.SIGNER_NAME,
                                             S.ATTORNEY_NO,
                                             S.SIGNER_ROLE_ID,
                                             S.SIGNER_ROLE,
                                             S.DATE_FROM,
                                             S.DATE_TO,
                                             CASE
                                                WHEN S.CONTRACTOR_ID = 1 THEN 9999999
                                                ELSE S.PRIORITY
                                             END
                                                PRIORITY,
                                             M.SIGN_PICTURE_ID,
                                             P.*
                                        FROM SIGNER_T S,
                                             CONTRACTOR_T C,
                                             MANAGER_T M,
                                             PICTURE_T P
                                       WHERE     C.CONTRACTOR_ID = S.CONTRACTOR_ID
                                             AND M.MANAGER_ID = S.MANAGER_ID
                                             AND P.PICTURE_ID(+) = M.SIGN_PICTURE_ID
                                             AND S.SIGNER_ROLE_ID = 6102
                                             AND (   S.CONTRACTOR_ID = p_contractor_id
                                                  OR S.CONTRACTOR_ID = 1)) b)
                       WHERE rn = 1) b
                        ON (1 = 1)
                     FULL OUTER JOIN
                     (SELECT *
                        FROM (SELECT 
                                  s.stamp_id,
                                  p.PICTURE_ID STAMP_PICTURE_ID,
                                  p.PICTURE STAMP_PICTURE,    
                                     ROW_NUMBER () OVER (ORDER BY CONTRACTOR_ID DESC) rn
                                FROM stamp_t s, picture_t p
                               WHERE     s.picture_Id = p.picture_Id
                                     AND SYSDATE BETWEEN S.DATE_FROM
                                                     AND NVL (
                                                            S.DATE_TO,
                                                            TO_DATE ('01.01.2050',
                                                                     'DD.MM.YYYY'))
                                     AND (   S.CONTRACTOR_ID = p_contractor_id
                                          OR S.CONTRACTOR_ID = 1))
                       WHERE rn = 1) c
                        ON 1 = 1;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;

---------------------------------------------------------------------------------------
-- Получить подписантов по поставщику (BRANCH_ID у договора)
PROCEDURE Document_signer_by_contractor( 
           p_recordset          OUT t_refc,
           p_contractor_id      IN  NUMBER,
           p_date_to            IN  DATE
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Document_signer_by_contractor';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR    
          SELECT *
                FROM (
                SELECT NVL (SIGNER_ROLE,
                                  'Руководитель организации')
                                R_SIGNER_ROLE,
                             SIGNER_NAME R_SIGNER_NAME,
                             ATTORNEY_NO R_ATTORNEY_NO,
                             DATE_FROM R_DATE_FROM,               
                             PICTURE_ID R_PICTURE_ID,
                             PICTURE R_PICTURE
                        FROM (SELECT b.*,
                                     ROW_NUMBER ()
                                        OVER (ORDER BY PRIORITY)
                                        rn
                                FROM (SELECT S.SIGNER_ID,
                                             S.CONTRACTOR_ID,
                                             S.MANAGER_ID,
                                             S.SIGNER_NAME,
                                             S.ATTORNEY_NO,
                                             S.SIGNER_ROLE_ID,
                                             S.SIGNER_ROLE,
                                             S.DATE_FROM,
                                             S.DATE_TO,
                                             CASE
                                                WHEN S.CONTRACTOR_ID = 1 THEN 9999999
                                                ELSE S.PRIORITY
                                             END
                                                PRIORITY,
                                             M.SIGN_PICTURE_ID,
                                             P.*
                                        FROM SIGNER_T S,
                                             CONTRACTOR_T C,
                                             MANAGER_T M,
                                             PICTURE_T P
                                       WHERE C.CONTRACTOR_ID = S.CONTRACTOR_ID
                                             AND M.MANAGER_ID = S.MANAGER_ID
                                             AND P.PICTURE_ID(+) = M.SIGN_PICTURE_ID
                                             AND NVL(p_date_to, SYSDATE) BETWEEN S.DATE_FROM AND NVL(S.DATE_TO,TO_DATE('01.01.2050','DD.MM.YYYY'))
                                             AND S.SIGNER_ROLE_ID = 6101
                                             AND (   S.CONTRACTOR_ID = p_contractor_id
                                                  OR S.CONTRACTOR_ID = 1)) b)
                       WHERE rn = 1
                       ) a
                     FULL OUTER JOIN
                     (SELECT NVL (SIGNER_ROLE, 'Главный бухгалтер')
                                B_SIGNER_ROLE,
                             SIGNER_NAME B_SIGNER_NAME,
                             ATTORNEY_NO B_ATTORNEY_NO,
                             DATE_FROM B_DATE_FROM,
                             PICTURE_ID B_PICTURE_ID,
                             PICTURE B_PICTURE
                        FROM (SELECT b.*,
                                     ROW_NUMBER ()
                                        OVER (ORDER BY PRIORITY)
                                        rn
                                FROM (SELECT S.SIGNER_ID,
                                             S.CONTRACTOR_ID,
                                             S.MANAGER_ID,
                                             S.SIGNER_NAME,
                                             S.ATTORNEY_NO,
                                             S.SIGNER_ROLE_ID,
                                             S.SIGNER_ROLE,
                                             S.DATE_FROM,
                                             S.DATE_TO,
                                             CASE
                                                WHEN S.CONTRACTOR_ID = 1 THEN 9999999
                                                ELSE S.PRIORITY
                                             END
                                                PRIORITY,
                                             M.SIGN_PICTURE_ID,
                                             P.*
                                        FROM SIGNER_T S,
                                             CONTRACTOR_T C,
                                             MANAGER_T M,
                                             PICTURE_T P
                                       WHERE     C.CONTRACTOR_ID = S.CONTRACTOR_ID
                                             AND M.MANAGER_ID = S.MANAGER_ID
                                             AND NVL(p_date_to, SYSDATE) BETWEEN S.DATE_FROM AND NVL(S.DATE_TO,TO_DATE('01.01.2050','DD.MM.YYYY'))
                                             AND P.PICTURE_ID(+) = M.SIGN_PICTURE_ID
                                             AND S.SIGNER_ROLE_ID = 6102
                                             AND (   S.CONTRACTOR_ID = p_contractor_id
                                                  OR S.CONTRACTOR_ID = 1)) b)
                       WHERE rn = 1) b
                        ON (1 = 1)
                     FULL OUTER JOIN
                     (SELECT *
                        FROM (SELECT 
                                  s.stamp_id,
                                  p.PICTURE_ID STAMP_PICTURE_ID,
                                  p.PICTURE STAMP_PICTURE,    
                                     ROW_NUMBER () OVER (ORDER BY CONTRACTOR_ID DESC) rn
                                FROM stamp_t s, picture_t p
                               WHERE s.picture_Id = p.picture_Id
                                     AND NVL(p_date_to, SYSDATE) BETWEEN S.DATE_FROM AND NVL(S.DATE_TO, TO_DATE ('01.01.2050', 'DD.MM.YYYY'))
                                     AND (S.CONTRACTOR_ID = p_contractor_id OR S.CONTRACTOR_ID = 1)
                             )
                       WHERE rn = 1) c
                        ON 1 = 1;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;


 -- ------------------------------------------------------------------------------- --
    -- получить данные для заполнения заголока документов при:
    -- отправке адресов
    --   - при ошибке выставляет исключение
 -- -------------------------------------------------------------------------------    
/*    
PROCEDURE Document_header_adr( 
               p_recordset     OUT t_refc, 
               p_rep_period_id IN INTEGER,              -- ID периода счета
               p_account_id    IN INTEGER DEFAULT NULL  -- ID счета              
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Document_header_adr';
    v_retcode    INTEGER;
    v_period_to  DATE;
BEGIN
    -- получаем верхнюю границу периода
    v_period_to := Pk04_Period.Period_to(p_rep_period_id);
    -- возвращаем курсор
    
    OPEN p_recordset FOR
    WITH ADDR_DLV AS    -- адрес доставки
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON
                 FROM ACCOUNT_CONTACT_T  
                 WHERE ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_DLV    -- 'DLV'
                   AND DATE_FROM<= v_period_to 
                   AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
              ) 
        SELECT A.ACCOUNT_ID,                -- ID лицевого счета
               A.ACCOUNT_NO,                -- номер лицевого счета
               -- договор       - - - - - - - - - - - - - - - - - - - - - - - -
               C.CONTRACT_NO,               -- номер договора
               C.DATE_FROM,                 -- дата договора
            
               -- ПЛАТЕЛЬЩИК/ПОКУПАТЕЛЬ ---------------------------------------
               -- координаты покупателя - - - - - - - - - - - - - - - - - - - -
               CS.CUSTOMER_ID,
               CS.CUSTOMER CUSTOMER_NAME,                 -- название компании покупателя
               CS.SHORT_NAME CUSTOMER_NAME_SHORT,-- краткое название компании покупателя
                          
               -- Почтовый адрес плательщика
               ADDR_DLV.COUNTRY  DLV_COUNTRY, 
               ADDR_DLV.ZIP      DLV_ZIP, 
               ADDR_DLV.STATE    DLV_STATE, 
               ADDR_DLV.CITY     DLV_CITY, 
               ADDR_DLV.ADDRESS  DLV_ADDRESS,
               ADDR_DLV.PERSON   DLV_PERSON               
          FROM ACCOUNT_T A, 
               ACCOUNT_PROFILE_T AP, 
               CONTRACT_T C,          
               CUSTOMER_T CS,           
               ADDR_DLV               
         WHERE A.ACCOUNT_TYPE = 'J'         
           AND A.ACCOUNT_ID = ADDR_DLV.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID           
        --   AND AP.DATE_FROM >= to_date(to_char(p_rep_period_id),'RRRRMM')          
           AND AP.CONTRACT_ID   = C.CONTRACT_ID
           AND AP.CUSTOMER_ID   = CS.CUSTOMER_ID 
           AND (p_account_id IS NULL OR A.ACCOUNT_ID = p_account_id)
   ;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;

*/

 -- ------------------------------------------------------------------------------- --
    -- получить данные для заполнения заголока документов при:
    -- отправке адресов
    --   - при ошибке выставляет исключение
    -- выбирается только по account_id, тк остальные условия 
    -- выполняются при выборе списка
 -- -------------------------------------------------------------------------------    
    
PROCEDURE Document_header_adr( 
               p_recordset     OUT t_refc,
               p_rep_period_id IN INTEGER,              -- ID периода счета
               p_account_id    IN INTEGER DEFAULT NULL  -- ID счета              
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Document_header_adr';
    v_retcode    INTEGER;
    v_period_to  DATE;
BEGIN
    v_period_to := Pk04_Period.Period_to(p_rep_period_id);
    -- возвращаем курсор
    OPEN p_recordset FOR
    WITH ADDR_DLV AS    -- адрес доставки
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON
                 FROM ACCOUNT_CONTACT_T  
                 WHERE ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_DLV    -- 'DLV'
              ) 
        SELECT A.ACCOUNT_ID,                -- ID лицевого счета
               A.ACCOUNT_NO,                -- номер лицевого счета
               -- договор       - - - - - - - - - - - - - - - - - - - - - - - -
               C.CONTRACT_NO,               -- номер договора
               C.DATE_FROM,                 -- дата договора
            
               -- ПЛАТЕЛЬЩИК/ПОКУПАТЕЛЬ ---------------------------------------
               -- координаты покупателя - - - - - - - - - - - - - - - - - - - -
               CS.CUSTOMER_ID,                   -- на всякий случай оставил, м.б. ИНН/КПП илил еще чего нужно будет
               CM.COMPANY_NAME CUSTOMER_NAME,    -- название компании покупателя
               CM.SHORT_NAME CUSTOMER_NAME_SHORT,-- краткое название компании покупателя
               -- Почтовый адрес плательщика
               ADDR_DLV.COUNTRY  DLV_COUNTRY, 
               ADDR_DLV.ZIP      DLV_ZIP, 
               ADDR_DLV.STATE    DLV_STATE, 
               ADDR_DLV.CITY     DLV_CITY, 
               ADDR_DLV.ADDRESS  DLV_ADDRESS,
               ADDR_DLV.PERSON   DLV_PERSON               
          FROM ACCOUNT_T A, 
               ACCOUNT_PROFILE_T AP, 
               CONTRACT_T C,          
               CUSTOMER_T CS,
               COMPANY_T CM,
               ADDR_DLV               
         WHERE A.ACCOUNT_TYPE = 'J'         
           AND A.ACCOUNT_ID = ADDR_DLV.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID           
      --     AND AP.DATE_FROM >= to_date(to_char(p_rep_period_id),'RRRRMM')          
           AND AP.CUSTOMER_ID   = CS.CUSTOMER_ID
           AND AP.CONTRACT_ID   = C.CONTRACT_ID
           AND CM.CONTRACT_ID   = C.CONTRACT_ID
           AND CM.DATE_FROM    <= v_period_to
           AND (CM.DATE_TO IS NULL OR v_period_to <= CM.DATE_TO)
           AND (p_account_id IS NULL OR A.ACCOUNT_ID = p_account_id)
   ;

--     AND AP.DATE_FROM >= to_date(to_char(p_rep_period_id),'RRRRMM')
-- закоментарено условие, тк возможен случай, когда выбран счет у кот.
-- прошло редактирование и это условие не выполнится

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;
-- ------------------------------------------------------------------------------- --

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
    -- возвращаем курсор
    OPEN p_recordset FOR
     SELECT II.BILL_ID, II.REP_PERIOD_ID, II.INV_ITEM_ID, II.INV_ITEM_NO, II.SERVICE_ID, 
            II.INV_ITEM_NAME NAME,                 -- Наименовние товара
            II.TOTAL         ITEM_TOTAL,           -- Стоимость с налогом
            II.GROSS         ITEM_NETTO,           -- Стоимость без налога
            II.TAX           ITEM_TAX,             -- Сумма налога
            II.VAT           TAX  ,                -- Налоговая ставка
            II.DATE_FROM     USAGE_START,          -- Диапазон дат 
            II.DATE_TO       USAGE_END ,          -- оказания услуги
            SUM(II.TOTAL) OVER (PARTITION BY II.BILL_ID) , -- Всего к оплате: Стоимость с налогом
            SUM(II.GROSS) OVER (PARTITION BY II.BILL_ID), -- Всего к оплате: Стоимость без налога
            SUM(II.TAX)   OVER (PARTITION BY II.BILL_ID) ITEM_TAX  -- Всего к оплате: Сумма налога
       FROM INVOICE_ITEM_T II--, ITEM_T I, ORDER_T O
      WHERE II.REP_PERIOD_ID = p_rep_period_id 
        AND II.BILL_ID       = p_bill_id
--        AND I.INV_ITEM_ID    = II.INV_ITEM_ID
--        AND I.BILL_ID        = II.BILL_ID
 --       AND I.ORDER_ID       = O.ORDER_ID
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
-- Строки счета-фактуры для разового счета
--   - при ошибке выставляет исключение
PROCEDURE Invoice_items_for_manual( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Invoice_items';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
     SELECT II.BILL_ID, II.REP_PERIOD_ID, II.INV_ITEM_ID, II.INV_ITEM_NO, II.SERVICE_ID, 
            II.INV_ITEM_NAME ||'. Заказ №' || 
            CASE
              WHEN INSTR (O.ORDER_NO, A.ACCOUNT_NO || '-') > 0
              THEN
                 SUBSTR (
                    O.ORDER_NO,
                    INSTR (O.ORDER_NO, A.ACCOUNT_NO) + LENGTH (A.ACCOUNT_NO || '-'))
              ELSE O.ORDER_NO END
            NAME,  -- Наименовние товара
            II.TOTAL         ITEM_TOTAL,           -- Стоимость с налогом
            II.GROSS         ITEM_NETTO,           -- Стоимость без налога
            II.TAX           ITEM_TAX,             -- Сумма налога
            II.VAT           TAX  ,                -- Налоговая ставка
            II.DATE_FROM     USAGE_START,          -- Диапазон дат 
            II.DATE_TO       USAGE_END,             -- оказания услуги
            CASE
              WHEN INSTR (O.ORDER_NO, A.ACCOUNT_NO || '-') > 0
              THEN
                 SUBSTR (
                    O.ORDER_NO,
                    INSTR (O.ORDER_NO, A.ACCOUNT_NO) + LENGTH (A.ACCOUNT_NO || '-'))
              ELSE O.ORDER_NO END
            ORDER_NO,
            II.INV_ITEM_NAME SERVICE_NAME
       FROM INVOICE_ITEM_T II, ITEM_T I, ORDER_T O, ACCOUNT_T A
      WHERE II.REP_PERIOD_ID = p_rep_period_id 
        AND II.BILL_ID       = p_bill_id
        AND I.INV_ITEM_ID    = II.INV_ITEM_ID
        AND I.BILL_ID        = II.BILL_ID
        AND I.ORDER_ID       = O.ORDER_ID
        AND O.ACCOUNT_ID     = A.ACCOUNT_ID
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
-- Строки корректирующей счета-фактуры
--   - при ошибке выставляет исключение
PROCEDURE Invoice_items_for_correct( 
               p_recordset     OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_new_bill_id   IN INTEGER,   -- ID дебетового счета
               p_old_bill_id   IN INTEGER    -- ID счета от которого строим корректировки. Если не задан, значит от предыдущего дебетового (должны быть связки в bill_t)
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Invoice_items_for_correct';
    v_retcode       INTEGER;
    v_old_bill_id   INTEGER;
    v_count         INTEGER;
BEGIN
    IF p_old_bill_id IS NULL THEN
       SELECT BILL_ID INTO v_old_bill_id
            FROM (SELECT ROW_NUMBER () OVER (ORDER BY LV) RN, t.*
                    FROM (    SELECT LEVEL LV, --SYS_CONNECT_BY_PATH (bill_id, '/') way,
                                              ac.*
                                FROM (SELECT *
                                        FROM bill_t b
                                       WHERE b.account_id = (SELECT account_id
                                                               FROM bill_t
                                                              WHERE bill_id = p_new_bill_id)) ac
                          START WITH ac.bill_id = p_new_bill_id
                          CONNECT BY PRIOR ac.bill_id = ac.next_bill_id) t
                   WHERE BILL_TYPE IN ('B', 'D', 'O') AND BILL_ID <> p_new_bill_id)
           WHERE rn = 1;
    ELSE
       v_old_bill_id := p_old_bill_id;
    END IF;
    
    -- Заглушка, пока ничего другого не придумали
    -- У Севера группировка строк идет не по услуге, а по компоненту, 
    -- поэтому в строчках с/ф может быть несколько записей с одной и той же услугой
    -- Поэтому сопоставляем счета по названию (DESCR), а именно по имени компоненты
    
    SELECT MAX(CNT) INTO v_count FROM (
      SELECT 
          SERVICE_ID, COUNT(*) CNT
        FROM INVOICE_ITEM_T
      WHERE BILL_ID = p_new_bill_id
      AND REP_PERIOD_ID = p_rep_period_id
    );    
    
    IF v_count = 1 THEN
        OPEN p_recordset FOR
           SELECT * FROM  (
             WITH B_OLD  AS (  
                       SELECT 
                              MIN(DATE_FROM) USAGE_START,
                              MAX(DATE_TO) USAGE_END, 
                              MAX (I.BILL_ID) BILL_ID,
                              SUM (I.TOTAL) ITEM_TOTAL,
                              SUM (I.GROSS) ITEM_GROSS,
                              SUM (I.TAX) ITEM_TAX,
                              S.SERVICE_ID
                         FROM INVOICE_ITEM_T i, service_t s
                        WHERE i.bill_id = v_old_bill_id 
                              AND I.SERVICE_ID = s.SERVICE_ID
                     GROUP BY S.SERVICE_ID,I.INV_ITEM_NAME,I.VAT
                     ORDER BY SERVICE_ID),
                 B_DEBT AS (                        
                       SELECT 
                              I.INV_ITEM_NAME,                  
                              MIN(DATE_FROM) USAGE_START,
                              MAX(DATE_TO) USAGE_END,         
                              MAX (I.BILL_ID) BILL_ID,
                              CASE WHEN B.BILL_TYPE = 'C' THEN 0 ELSE SUM (I.TOTAL) END ITEM_TOTAL,
                              CASE WHEN B.BILL_TYPE = 'C' THEN 0 ELSE SUM (I.GROSS) END ITEM_GROSS,
                              CASE WHEN B.BILL_TYPE = 'C' THEN 0 ELSE SUM (I.TAX) END ITEM_TAX,    
                              S.SERVICE_ID,
                              I.VAT TAX
                         FROM INVOICE_ITEM_T i, service_t s, bill_t b
                        WHERE b.bill_id = i.bill_id
                              AND I.SERVICE_ID = s.SERVICE_ID
                              AND i.bill_id = p_new_bill_id 
                     GROUP BY S.SERVICE_ID, B.BILL_TYPE, I.INV_ITEM_NAME, I.VAT
                     ORDER BY SERVICE_ID        
                     )
            SELECT B_OLD.ITEM_TOTAL A_TOTAL,
                   B_OLD.ITEM_GROSS A_GROSS,
                   B_OLD.ITEM_TAX A_TAX,
                   B_DEBT.ITEM_TOTAL B_TOTAL,
                   B_DEBT.ITEM_GROSS B_GROSS,
                   B_DEBT.ITEM_TAX B_TAX,
                   B_DEBT.USAGE_START B_USAGE_START,
                   B_DEBT.USAGE_END B_USAGE_END,
                   B_DEBT.TAX,
                   B_DEBT.INV_ITEM_NAME NAME,
                   CASE
                      WHEN (B_DEBT.ITEM_TOTAL - B_OLD.ITEM_TOTAL) < 0 THEN 0
                      ELSE (B_DEBT.ITEM_TOTAL - B_OLD.ITEM_TOTAL)
                   END
                      C_TOTAL,
                   CASE
                      WHEN (B_DEBT.ITEM_GROSS - B_OLD.ITEM_GROSS) < 0 THEN 0
                      ELSE (B_DEBT.ITEM_GROSS - B_OLD.ITEM_GROSS)
                   END
                      C_GROSS,
                   CASE
                      WHEN (B_DEBT.ITEM_TAX - B_OLD.ITEM_TAX) < 0 THEN 0
                      ELSE (B_DEBT.ITEM_TAX - B_OLD.ITEM_TAX)
                   END
                      C_TAX,                    
                   CASE
                      WHEN (B_OLD.ITEM_TOTAL - B_DEBT.ITEM_TOTAL) < 0 THEN 0
                      ELSE (B_OLD.ITEM_TOTAL - B_DEBT.ITEM_TOTAL)
                   END
                      D_TOTAL,    
                   CASE
                      WHEN (B_OLD.ITEM_GROSS - B_DEBT.ITEM_GROSS) < 0 THEN 0
                      ELSE(B_OLD.ITEM_GROSS - B_DEBT.ITEM_GROSS)
                   END
                      D_GROSS, 
                   CASE
                      WHEN (B_OLD.ITEM_TAX - B_DEBT.ITEM_TAX) < 0 THEN 0
                      ELSE (B_OLD.ITEM_TAX - B_DEBT.ITEM_TAX)
                   END
                      D_TAX                                     
              FROM B_OLD, B_DEBT
             WHERE B_OLD.SERVICE_ID = B_DEBT.SERVICE_ID
           )
           WHERE ROUND(A_TOTAL,2) <> ROUND(B_TOTAL,2);
       ELSE
         OPEN p_recordset FOR
           SELECT * FROM  (
             WITH B_OLD  AS (  
                       SELECT 
                              INV_ITEM_NAME,
                              MIN(DATE_FROM) USAGE_START,
                              MAX(DATE_TO) USAGE_END, 
                              MAX (I.BILL_ID) BILL_ID,
                              SUM (I.TOTAL) ITEM_TOTAL,
                              SUM (I.GROSS) ITEM_GROSS,
                              SUM (I.TAX) ITEM_TAX                              
                         FROM INVOICE_ITEM_T i
                        WHERE i.bill_id = v_old_bill_id 
                     GROUP BY I.INV_ITEM_NAME, I.VAT),
                 B_DEBT AS (                        
                       SELECT 
                              I.INV_ITEM_NAME,                  
                              MIN(DATE_FROM) USAGE_START,
                              MAX(DATE_TO) USAGE_END,         
                              MAX (I.BILL_ID) BILL_ID,
                              SUM (I.TOTAL) ITEM_TOTAL,
                              SUM (I.GROSS) ITEM_GROSS,
                              SUM (I.TAX) ITEM_TAX,
                              I.VAT TAX
                         FROM INVOICE_ITEM_T i
                        WHERE i.bill_id = p_new_bill_id 
                     GROUP BY I.INV_ITEM_NAME, I.VAT
                     )
            SELECT NVL(B_OLD.ITEM_TOTAL,0) A_TOTAL,
                   NVL(B_OLD.ITEM_GROSS,0) A_GROSS,
                   NVL(B_OLD.ITEM_TAX,0) A_TAX,
                   NVL(B_DEBT.ITEM_TOTAL,0) B_TOTAL,
                   NVL(B_DEBT.ITEM_GROSS,0) B_GROSS,
                   NVL(B_DEBT.ITEM_TAX,0) B_TAX,
                   NVL(B_DEBT.USAGE_START,B_OLD.USAGE_START) B_USAGE_START,
                   NVL(B_DEBT.USAGE_END,B_OLD.USAGE_END) B_USAGE_END,
                   NVL(B_DEBT.TAX,0) TAX,
                   NVL(B_DEBT.INV_ITEM_NAME,B_OLD.INV_ITEM_NAME) NAME,
                   CASE
                      WHEN (NVL(B_DEBT.ITEM_TOTAL,0) - NVL(B_OLD.ITEM_TOTAL,0)) < 0 THEN 0
                      ELSE (NVL(B_DEBT.ITEM_TOTAL,0) - NVL(B_OLD.ITEM_TOTAL,0))
                   END
                      C_TOTAL,
                   CASE
                      WHEN (NVL(B_DEBT.ITEM_GROSS,0) - NVL(B_OLD.ITEM_GROSS,0)) < 0 THEN 0
                      ELSE (NVL(B_DEBT.ITEM_GROSS,0) - NVL(B_OLD.ITEM_GROSS,0))
                   END
                      C_GROSS,
                   CASE
                      WHEN (NVL(B_DEBT.ITEM_TAX,0) - NVL(B_OLD.ITEM_TAX,0)) < 0 THEN 0
                      ELSE (NVL(B_DEBT.ITEM_TAX,0) - NVL(B_OLD.ITEM_TAX,0))
                   END
                      C_TAX,                    
                   CASE
                      WHEN (NVL(B_OLD.ITEM_TOTAL,0) - NVL(B_DEBT.ITEM_TOTAL,0)) < 0 THEN 0
                      ELSE (NVL(B_OLD.ITEM_TOTAL,0) - NVL(B_DEBT.ITEM_TOTAL,0))
                   END
                      D_TOTAL,    
                   CASE
                      WHEN (NVL(B_OLD.ITEM_GROSS,0) - NVL(B_DEBT.ITEM_GROSS,0)) < 0 THEN 0
                      ELSE(NVL(B_OLD.ITEM_GROSS,0) - NVL(B_DEBT.ITEM_GROSS,0))
                   END
                      D_GROSS, 
                   CASE
                      WHEN (NVL(B_OLD.ITEM_TAX,0) - NVL(B_DEBT.ITEM_TAX,0)) < 0 THEN 0
                      ELSE (NVL(B_OLD.ITEM_TAX,0) - NVL(B_DEBT.ITEM_TAX,0))
                   END
                      D_TAX                                       
              FROM B_OLD FULL JOIN B_DEBT
                 ON (B_OLD.INV_ITEM_NAME = B_DEBT.INV_ITEM_NAME)
           )
           WHERE ROUND(A_TOTAL,2) <> ROUND(B_TOTAL,2);
       END IF;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------------- --
--  Список счетов, которые унаследованы от текущего дебетового
--   - при ошибке выставляет исключение
PROCEDURE BILL_HISTORY_FOR_DEBET ( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BILL_HISTORY_FOR_DEBET';
    v_retcode    INTEGER;
BEGIN
    -- построить курсор
    OPEN p_recordset FOR
         SELECT *
            FROM (SELECT ROW_NUMBER () OVER (ORDER BY LV) RN, t.*
                    FROM (    SELECT LEVEL LV, --SYS_CONNECT_BY_PATH (bill_id, '/') way,
                                              ac.*
                                FROM (SELECT *
                                        FROM bill_t b
                                       WHERE b.account_id = (SELECT account_id
                                                               FROM bill_t
                                                              WHERE bill_id = p_bill_id)) ac
                          START WITH ac.bill_id = p_bill_id
                          CONNECT BY PRIOR ac.bill_id = ac.next_bill_id) t
                   WHERE BILL_TYPE IN ('B', 'D' , 'O') AND BILL_ID <> p_bill_id)  
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
--  Список услуг и их компонентов услуги к счету
--   - при ошибке выставляет исключение
PROCEDURE detail_item_list ( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'detail_item_list';
    v_retcode    INTEGER;
BEGIN
    -- построить курсор
    OPEN p_recordset FOR
      WITH LST AS (
          SELECT 
               I.BILL_ID, 
               I.ITEM_ID, 
               I.CHARGE_TYPE,
               I.ORDER_ID,
               I.ORDER_BODY_ID, 
               I.SERVICE_ID, 
               I.SUBSERVICE_ID, 
               I.DATE_FROM, 
               I.DATE_TO, 
               I.DESCR, 
               I.NOTES,
               B.CURRENCY_ID BILL_CURRENCY_ID, 
               I.ITEM_CURRENCY_ID,
--               COUNT(*) NROWS, 
               SUM(I.ITEM_TOTAL) ITEM_TOTAL, SUM(I.BILL_TOTAL) BILL_TOTAL, SUM(I.REP_GROSS) ITEM_GROSS, SUM(I.REP_TAX) ITEM_TAX
                    FROM ITEM_T I, ORDER_BODY_T OB, BILL_T B
                   WHERE B.REP_PERIOD_ID = p_rep_period_id
                     AND B.BILL_ID = p_bill_id
                     AND OB.ORDER_BODY_ID (+)= I.ORDER_BODY_ID
                     AND B.BILL_ID = I.BILL_ID  
                   GROUP BY I.ITEM_ID, I.CHARGE_TYPE, I.BILL_ID, I.ORDER_ID, I.ORDER_BODY_ID, I.SERVICE_ID, I.SUBSERVICE_ID, I.DATE_FROM, I.DATE_TO, I.DESCR, I.NOTES, B.CURRENCY_ID, I.ITEM_CURRENCY_ID
           ),
           ORD_MIN AS (
               SELECT 
                   ORDER_ID, ORDER_BODY_ID,
                   RATE_VALUE,
                   CURRENCY_ID RATE_VALUE_CURRENCY_ID
                 FROM ORDER_BODY_T
               WHERE CHARGE_TYPE = 'MIN' AND RATE_LEVEL_ID = 2302
           ),
           ORD_ABON AS (
               SELECT 
                   ORDER_ID, 
                   ORDER_BODY_ID,
                   RATE_VALUE,
                   CURRENCY_ID RATE_VALUE_CURRENCY_ID
                 FROM ORDER_BODY_T
               WHERE CHARGE_TYPE = 'REC'
           ),
           ORD_HAS_DTL AS (
               SELECT 
                     BILL_ID, ORDER_ID,'Y' HAS_DTL 
                   FROM ITEM_T I, SERVICE_SUBSERVICE_T SS
               WHERE 
                   I.SERVICE_ID = SS.SERVICE_ID (+)
                   AND I.SUBSERVICE_ID = SS.SUBSERVICE_ID (+)
                   AND I.REP_PERIOD_ID = p_rep_period_id
                   AND I.BILL_ID = p_bill_id
                   AND I.CHARGE_TYPE = 'USG'
                   --AND I.SERVICE_ID <> 7                 
                   AND (SS.DTL_KEY IS NULL OR SS.DTL_KEY <> 'TRAFFIC_IP_BURST' )  
               GROUP BY BILL_ID, ORDER_ID      
           )
        SELECT 
                  LST.BILL_ID,
                  LST.ITEM_ID,                  
                  LST.ORDER_ID,                   
                  CASE
                    WHEN INSTR (ORDER_NO, ACCOUNT_NO || '-') > 0
                    THEN
                       SUBSTR (
                          ORDER_NO,
                          INSTR (ORDER_NO, ACCOUNT_NO) + LENGTH (ACCOUNT_NO || '-'))
                    ELSE
                       ORDER_NO
                  END ORDER_NO,
                  LST.SERVICE_ID,
                  LST.SUBSERVICE_ID,
                  LST.DATE_FROM, 
                  LST.DATE_TO, 
                  LST.BILL_TOTAL,
                  LST.ITEM_TOTAL, 
                  LST.ITEM_GROSS, 
                  LST.ITEM_TAX, 
                  ORD_MIN.RATE_VALUE MIN_RATE_VALUE,
                  ORD_MIN.RATE_VALUE_CURRENCY_ID MIN_RATE_VALUE_CURRENCY_ID,
                  CASE WHEN
                     LST.CHARGE_TYPE = 'REC' THEN ORD_ABON.RATE_VALUE 
                     ELSE NULL END ABON_RATE_VALUE,
                  ORD_ABON.RATE_VALUE_CURRENCY_ID ABON_RATE_VALUE_CURRENCY_ID,                  
                  NVL(LST.BILL_CURRENCY_ID, 810) BILL_CURRENCY_ID,
                  NVL(LST.ITEM_CURRENCY_ID, 810) ITEM_CURRENCY_ID,
                  LST.DESCR ORDER_DESC,  
                  LST.NOTES,              
                  S.SERVICE,
                  S.SERVICE_CODE_PRINTFORM,
                  SS.SUBSERVICE,
                  LST.DESCR ORDER_DESCR,       
                  LST.CHARGE_TYPE ITEM_CHARGE_TYPE,
                  SDT.DTL_KEY ITEM_ALIAS,
                  NVL(ORD_HAS_DTL.HAS_DTL,'N') HAS_DTL
              FROM 
                  LST, ORD_MIN, ORD_ABON, ORD_HAS_DTL, ORDER_T O, SERVICE_T S, SUBSERVICE_T SS, SERVICE_SUBSERVICE_T SDT, ACCOUNT_T A
              WHERE LST.SERVICE_ID = S.SERVICE_ID
                  AND LST.SUBSERVICE_ID = SS.SUBSERVICE_ID
                  AND LST.SERVICE_ID = SDT.SERVICE_ID (+)
                  AND LST.SUBSERVICE_ID = SDT.SUBSERVICE_ID (+)  
                  AND LST.ORDER_ID = ORD_MIN.ORDER_ID (+)
                  AND LST.ORDER_BODY_ID = ORD_MIN.ORDER_BODY_ID (+)
                  AND LST.ORDER_ID = ORD_ABON.ORDER_ID (+)
                  AND LST.ORDER_BODY_ID = ORD_ABON.ORDER_BODY_ID (+)
                  AND LST.ORDER_ID = ORD_HAS_DTL.ORDER_ID (+)
                  AND LST.BILL_ID = ORD_HAS_DTL.BILL_ID (+)
                  AND O.ORDER_ID = LST.ORDER_ID
                  AND O.ACCOUNT_ID = A.ACCOUNT_ID                  
              ORDER BY S.service_id,  HAS_DTL, O.ORDER_NO,LST.DATE_FROM, DECODE(LST.CHARGE_TYPE,'REC',0,'USG',1,'MIN',3,'IDL',5,'ONT',10,'DIS',20),SS.subservice_id
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

-----------------------------------------------------------------
-- Деталировка для АРМ массовой печати (временный костыль, пока массовая печать не переедет в новый АРМ)
-----------------------------------------------------------------
PROCEDURE MakeMGMN_Jur_DETAIL(p_recordset OUT t_refc, p_rep_period IN INTEGER, p_bill_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'MakeMGMN_Jur_DETAIL';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
  WITH DETAIL 
  AS
  (  
  SELECT BDR.BILL_ID,
         COUNT (BDR.BILL_ID) CALL_COUNT,
         SUM (BDR.AMOUNT) COST,
         TRUNC (TO_DATE ('01.07.2014', 'dd.mm.yyyy')) DATE_T,
         ROUND (SUM (BDR.BILL_MINUTES)) DURATION,
         O.ORDER_NO ORDER_NUM,
         DECODE (S.SERVICE_ID, 1, BDR.ABN_A, BDR.ABN_F) PRIMARY_MSID,
         0 PRODUCT_OBJ_ID0,
         S.SERVICE SERVICE,
         BDR.PREFIX_B TERMINATE_CODE,
         BDR.TERM_Z_NAME ZONE_NAME,
            TO_CHAR (Pk04_Period.Period_from (p_rep_period), 'DD.MM.YYYY')
         || ' - '
         || TO_CHAR (Pk04_Period.Period_to (p_rep_period), 'DD.MM..YYYY')
            AS DD
    FROM BDR_VOICE_T BDR, ORDER_T o, SERVICE_T S
   WHERE     BDR.REP_PERIOD BETWEEN PK04_PERIOD.PERIOD_FROM (p_rep_period)
                                AND PK04_PERIOD.PERIOD_TO (p_rep_period)
         AND BDR.BILL_ID = p_bill_id
         AND BDR.ORDER_ID = O.ORDER_ID
         AND S.SERVICE_ID = BDR.SERVICE_ID
GROUP BY BDR.BILL_ID,
         S.SERVICE,
         DECODE (S.SERVICE_ID, 1, BDR.ABN_A, BDR.ABN_F),
         BDR.PREFIX_B,
         BDR.TERM_Z_NAME,
         TRUNC (TO_DATE ('01.07.2014', 'dd.mm.yyyy')),
         O.ORDER_NO
         
UNION
SELECT b.BILL_ID,
       0 CALL_COUNT,
       I.ITEM_TOTAL COST,
       TRUNC (TO_DATE ('01.07.2014', 'dd.mm.yyyy')) DATE_T,
       F.VALUE DURATION,
       O.ORDER_NO ORDER_NUM,
       '0' PRIMARY_MSID,
       0 PRODUCT_OBJ_ID0,
       SS.SUBSERVICE SERVICE,
       '999999999999' TERMINATE_CODE,
       '0' ZONE_NAME,
          TO_CHAR (I.DATE_FROM, 'DD.MM.YYYY')
       || ' - '
       || TO_CHAR (I.DATE_TO, 'DD.MM..YYYY')
          AS DD
  FROM item_t i,
       bill_t b,
       order_t o,
       SERVICE_T S,
       SUBSERVICE_T ss,
       fix_rate_t f
 WHERE     I.BILL_ID = p_bill_id
       AND i.BILL_ID = b.BILL_ID
       AND I.ORDER_ID = o.order_id
       AND I.SERVICE_ID = S.SERVICE_ID
       AND I.SUBSERVICE_ID = SS.SUBSERVICE_ID
       AND I.CHARGE_TYPE = 'MIN'
       and I.ORDER_ID = F.ORDER_ID)
       
       SELECT DETAIL.BILL_ID,
           DETAIL.CALL_COUNT,
           DETAIL.COST,
           DETAIL.DATE_T,
           DETAIL.DURATION,
           DETAIL.ORDER_NUM,
           DETAIL.PRIMARY_MSID,
           DETAIL.PRODUCT_OBJ_ID0,
           DETAIL.SERVICE,
           DETAIL.TERMINATE_CODE,
           DETAIL.ZONE_NAME,
           DETAIL.DD
       FROM DETAIL DETAIL
       order by DETAIL.TERMINATE_CODE, DETAIL.order_num
    
    
     /*SELECT BDR.BILL_ID,
            COUNT (BDR.BILL_ID) CALL_COUNT,
            SUM (BDR.AMOUNT) COST,
            --TRUNC (BDR.LOCAL_TIME) DATE_T,
            TRUNC (to_date('01.07.2014', 'dd.mm.yyyy')) DATE_T,
            round(SUM (BDR.BILL_MINUTES)) DURATION,
            O.ORDER_NO ORDER_NUM,
            --BDR.ABN_F PRIMARY_MSID,
            DECODE(S.SERVICE_ID, 1, BDR.ABN_A, BDR.ABN_F) PRIMARY_MSID,
            0 PRODUCT_OBJ_ID0,
            S.SERVICE SERVICE,
            BDR.PREFIX_B TERMINATE_CODE,
            BDR.TERM_Z_NAME ZONE_NAME,
            TO_CHAR(Pk04_Period.Period_from(p_rep_period),  'DD.MM.YYYY') || ' - ' || TO_CHAR(Pk04_Period.Period_to(p_rep_period),  'DD.MM..YYYY') AS DD
       FROM E04_BDR_MMTS_T BDR, ORDER_T o, SERVICE_T S
      WHERE BDR.REP_PERIOD BETWEEN PK04_PERIOD.PERIOD_FROM(p_rep_period) AND PK04_PERIOD.PERIOD_TO(p_rep_period)
            AND BDR.BILL_ID = p_bill_id
            AND BDR.ORDER_ID = O.ORDER_ID AND S.SERVICE_ID = BDR.SERVICE_ID
   GROUP BY BDR.BILL_ID,
            S.SERVICE,
            --BDR.ABN_F,
            DECODE(S.SERVICE_ID, 1, BDR.ABN_A, BDR.ABN_F),
            BDR.PREFIX_B,
            BDR.TERM_Z_NAME,
            --TRUNC (BDR.LOCAL_TIME),
            TRUNC (to_date('01.07.2014', 'dd.mm.yyyy')),
            O.ORDER_NO
   order by BDR.PREFIX_B --TRUNC (BDR.LOCAL_TIME) */
;
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

--=========================================================================================--
-- ЗДЕСЬ ИДЕТ блок с запросами на уникальные запросы
-- ------------------------------------------------------------------------------- --
--  Позвонковка для Маяка (это у них такая детализация)
PROCEDURE pozvonkovka_MAYAK ( 
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
        SELECT                 
               BDR.ABN_A,                 -- Абонентский номер
               TO_CHAR(BDR.START_TIME,'DD.MM.YYYY') DATE_START,
               TO_CHAR(BDR.START_TIME,'HH24:MI:SS') TIME_START,
               BDR.TERM_Z_NAME,               
               CASE WHEN SS.SUBSERVICE_ID = 1 THEN 'МГ связь (авт)'
                    WHEN SS.SUBSERVICE_ID = 2 THEN 'МН связь (авт)'
                      ELSE SS.SHORTNAME
               END SUBSERVICE_NAME,  
               BDR.ABN_B,
               BDR.DURATION,
               0 UVED,
               BDR.AMOUNT AMOUNT_MINUS_DISCOUNT,
               BDR.AMOUNT
          FROM BDR_VOICE_T BDR,
               SERVICE_T S, 
               SUBSERVICE_T SS
         WHERE BDR.SERVICE_ID = S.SERVICE_ID(+)
            AND BDR.SUBSERVICE_ID = SS.SUBSERVICE_ID(+)     
            AND rep_period BETWEEN TO_DATE (p_rep_period_id, 'YYYYMM') AND   LAST_DAY (TO_DATE (p_rep_period_id, 'YYYYMM')) + INTERVAL '00 23:59:59' DAY TO SECOND
            AND  BILL_ID = p_bill_id   
        ORDER BY BDR.ABN_A, BDR.START_TIME, BDR.PREFIX_B     
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
 
--=======================================================================
--Детализированные данные для услуги "Трафик IP (объемный)"
--=======================================================================
procedure LOAD_BILL_DETAIL_TRAFFIC_IP(
          p_recordset     OUT t_refc,
          p_rep_period_id IN INTEGER,   -- ID периода счета
          p_bill_id       IN INTEGER,    -- ID счета
          p_item_id       IN INTEGER     -- ID позиции счета
)
is
    v_prcName   constant varchar2(30) := 'LOAD_BILL_DETAIL_TRAFFIC_IP';
begin                                            
    open p_recordset for
       select 
           bdr.ITEM_ID,
           D2.NAME    QUANTITY,   
           BDR.PRICE  TARIFF_SUM,
           BDR.AMOUNT TOTAL_SUM,
           BDR.CURRENCY_ID
        from 
            BDR_CCAD_VPN_T bdr,
            dictionary_t d1,
            dictionary_t d2
        where 
            BDR.VOLUME_UNIT_ID = D1.KEY_id
            and BDR.QUALITY_ID = D2.KEY_id
            and bdr.bill_id = p_bill_Id
            and bdr.item_id = p_item_id;
exception
    when others then
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        if p_recordset%ISOPEN then 
            close p_recordset;
        end if;
end;

--=======================================================================
--Детализированные данны для услуги "Трафик IP Burst"
--=======================================================================
procedure LOAD_BILL_DETAIL_TRAFFIC_BURST(
          p_recordset     OUT t_refc,
          p_rep_period_id IN INTEGER,   -- ID периода счета
          p_bill_id       IN INTEGER,    -- ID счета
          p_item_id       IN INTEGER     -- ID позиции счета
)
is
    v_prcName   constant varchar2(30) := 'LOAD_BILL_DETAIL_TRAFFIC_BURST';
begin
    open p_recordset for
        SELECT 
             BDR.EXCESS_SPEED QUANTITY,
             BDR.EXCESS_SPEED_UNIT,
             d2.NOTES EXCESS_SPEED_UNIT_NAME,
             PRICE TARIFF_SUMM,
             AMOUNT TOTAL_SUMM,
             CURRENCY_ID             
        FROM BDR_CCAD_BURST_T bdr, dictionary_t d1, dictionary_t d2
       WHERE     BDR.PAID_SPEED_UNIT = D1.KEY_id
             AND BDR.EXCESS_SPEED_UNIT = D2.KEY_id
             AND RATE_RULE_ID = 2409
             AND bdr.BILL_ID = p_bill_id
             AND bdr.ITEM_ID = p_item_id
             AND BDR.REP_PERIOD_ID = p_rep_period_id;   
exception
    when others then
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        if p_recordset%ISOPEN then 
            close p_recordset;
        end if;
end;

--=======================================================================
--Детализированные данны для услуги "Трафик IP VPN"
--=======================================================================
procedure LOAD_BILL_DETAIL_TRAFFIC_VPN(
          p_recordset     OUT t_refc,
          p_rep_period_id IN INTEGER,   -- ID периода счета
          p_bill_id       IN INTEGER,    -- ID счета
          p_item_id       IN INTEGER     -- ID позиции счета
)
is
    v_prcName   constant varchar2(30) := 'LOAD_BILL_DETAIL_TRAFFIC_VPN';
begin
    open p_recordset for
          SELECT 
                  bdr.zone_IN RZONE,
                  bdr.ZONE_OUT ZONE,
                  D2.NAME QOS,
                  VOLUME QUANTITY,
                  PRICE TARIFF_SUMM,
                  AMOUNT TOTAL_SUMM ,
                  CURRENCY_ID      
            FROM 
                  BDR_CCAD_VPN_T bdr, 
                  dictionary_t d1, 
                  dictionary_t d2
           WHERE 
              BDR.VOLUME_UNIT_ID = D1.KEY_id 
              AND BDR.QUALITY_ID = D2.KEY_id  
              AND bdr.BILL_ID = p_bill_id
              AND BDR.ITEM_ID = p_item_id
              AND BDR.REP_PERIOD_ID = p_rep_period_id        
             ;                    
exception
    when others then
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        if p_recordset%ISOPEN then 
            close p_recordset;
        end if;
end;

-- ------------------------------------------------------------------------------- --
--           Деталировка к счету за услуги связи по указанной позиции
-- Услуга: Услуги международной и междугородной телефонной связи
--   - при ошибке выставляет исключение
PROCEDURE LOAD_BILL_DETAIL_TRAFFIC_MGMN( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER,   -- ID счета
               p_item_id       IN INTEGER    -- ID позиции счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'LOAD_BILL_DETAIL_TRAFIC_MGMN';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT 
             D.ORDER_NO,                      
             D.SERVICE_ID,                -- ID услуги
             D.SUBSERVICE_ID,             -- ID компонента услуги
             D.PREFIX_B,
             D.TERM_Z_NAME,               -- Направление
             D.TARIFF_AMOUNT,
             D.TARIFF_CURRENCY_ID,        
             SUM(D.CALLS) CALLS,          -- Кол-во звонков, шт.
             SUM(D.MINUTES) MINUTES,      -- Длительность, мин.
             SUM(D.TOTAL) TOTAL,          -- Стоимость, руб.
             D.BILL_ID,                   -- ID счета
             D.ITEM_ID,                   -- ID позиции счета
             D.ORDER_ID                   -- ID заказа
        FROM detail_mmts_t_jur d
       WHERE D.REP_PERIOD_ID = p_rep_period_id
             AND D.BILL_ID = p_bill_id
             AND (D.ITEM_ID = p_item_id OR p_item_id IS NULL)
      GROUP BY  D.BILL_ID, D.ITEM_ID, D.ORDER_ID, D.ORDER_NO, D.SERVICE_ID, D.SUBSERVICE_ID, D.PREFIX_B, D.TERM_Z_NAME, D.TARIFF_AMOUNT, D.TARIFF_CURRENCY_ID
        ORDER BY ORDER_NO, PREFIX_B;
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
-- Услуга: межоператорка
--   - при ошибке выставляет исключение
PROCEDURE LOAD_BILL_DETAIL_TRAFFIC_OPER( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER,   -- ID счета
               p_item_id       IN INTEGER    -- ID позиции счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'LOAD_BILL_DETAIL_TRAFIC_OPER';
    v_retcode    INTEGER;
    v_is_tmp     INTEGER := 1;
BEGIN
    IF v_is_tmp = 1 THEN
      OPEN p_recordset FOR 
       SELECT ITEM_ID,
                   CNT,
                   BILL_MINUTES,
                   TARIFF,
                   TOTAL_SUMM
              FROM DETAIL_OPER_T_JUR
             WHERE REP_PERIOD_ID = p_rep_period_id    
                   AND (item_id = p_item_id OR p_item_id is NULL)
                   AND bill_id = p_bill_id;
    ELSE
        OPEN p_recordset FOR
              SELECT ITEM_ID,
                   COUNT (*) CNT,
                   SUM (BILL_MINUTES) BILL_MINUTES,
                   PRICE TARIFF,
                   SUM (AMOUNT) TOTAL_SUMM
              FROM bdr_oper_t
             WHERE (item_id = p_item_id OR p_item_id is NULL)
                   AND bill_id = p_bill_id
                   AND rep_period >= TO_DATE (p_rep_period_id, 'YYYYMM')
                   AND start_time BETWEEN TO_DATE (p_rep_period_id, 'YYYYMM') AND   LAST_DAY (TO_DATE (p_rep_period_id, 'YYYYMM')) + INTERVAL '00 23:59:59' DAY TO SECOND
          GROUP BY ITEM_ID, PRICE;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------------- --
--  Создание таблицы с данными по межоператорке по периоду
PROCEDURE CREATE_BILL_DETAIL_OPER_TMP( 
          p_rep_period_id IN INTEGER
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'CREATE_BILL_DETAIL_OPER_TMP';
    v_retcode    INTEGER;
BEGIN
    DELETE FROM DETAIL_OPER_T_JUR WHERE REP_PERIOD_ID = p_rep_period_id;
    
    INSERT INTO DETAIL_OPER_T_JUR(REP_PERIOD_ID,BILL_ID,ITEM_ID, CNT, BILL_MINUTES, TARIFF, TOTAL_SUMM)
          SELECT 
                 p_rep_period_id PERIOD_ID,
                 BILL_ID,
                 ITEM_ID,
                 COUNT (*) CNT,
                 SUM (BILL_MINUTES) BILL_MINUTES,
                 PRICE TARIFF,
                 SUM (AMOUNT) TOTAL_SUMM
            FROM bdr_oper_t
           WHERE 1=1
                 AND rep_period >= TO_DATE (p_rep_period_id, 'YYYYMM')
                 AND start_time BETWEEN TO_DATE (p_rep_period_id, 'YYYYMM') AND   LAST_DAY (TO_DATE (p_rep_period_id, 'YYYYMM')) + INTERVAL '00 23:59:59' DAY TO SECOND
        GROUP BY BILL_ID,ITEM_ID, PRICE;
        
     COMMIT;          
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;
---------------------------------------------------------------------------------
--=======================================================================
-- Преобразование адреса в строку
--=======================================================================
function address_to_string(
         p_zip           IN VARCHAR2,
         p_state         IN  VARCHAR2,
         p_city          IN  VARCHAR2,
         p_address       IN  VARCHAR2
) return VARCHAR2 
is
    v_result VARCHAR2(1000) := '';
begin   
    IF p_zip IS NOT NULL THEN
       v_result := p_zip;
    END IF;    
    
    IF p_state IS NOT NULL THEN
       IF v_result IS NOT NULL THEN
          v_result := v_result || ', ';       
       END IF;    
       v_result := v_result || p_state;
    END IF;
    
    IF p_city IS NOT NULL THEN
       IF v_result IS NOT NULL THEN
          v_result := v_result || ', ';       
       END IF;    
       v_result := v_result || p_city;
    END IF;
    
    IF p_address IS NOT NULL THEN
       IF v_result IS NOT NULL THEN
          v_result := v_result || ', ';       
       END IF;    
       v_result := v_result || p_address;
    END IF;
    
    return v_result;
end address_to_string;

END PK112_PRINT_V2;
/
