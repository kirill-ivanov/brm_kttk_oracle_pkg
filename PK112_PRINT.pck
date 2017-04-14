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
    PROCEDURE BILL_HEADER( 
             p_recordset    OUT t_refc, 
             p_rep_period_id IN INTEGER,              -- ID периода счета
             p_bill_id       IN INTEGER DEFAULT NULL  -- ID счета
    );
    
--====================================================================================
-- Загрузка дополнительных параметров для счета
PROCEDURE BILL_HEADER_EXT_PARAM( 
             p_recordset     OUT t_refc,
             p_bill_id       IN INTEGER,      -- ID счета              
             p_period_id     IN INTEGER      -- ID периода счета
);  

-- Загрузка дополнительных параметров для счета
FUNCTION BILL_HEADER_EXT_PARAM_FUNC( 
             p_bill_id       IN INTEGER,       -- ID счета              
             p_period_id     IN INTEGER,       -- ID периода счета
             p_param_name    IN VARCHAR2,      -- наименование параметра
             p_type_doc      IN VARCHAR2       -- тип документа
) RETURN VARCHAR2; 
   
-- ------------------------------------------------------------------------------- --
-- Получить подписантов по заданными ID
PROCEDURE BILL_SIGNER_INFO( 
           p_recordset          OUT t_refc,
           p_signer_header_id   IN  INTEGER,
           p_signer_booker_id   IN  INTEGER,
           p_stamp_id           IN  INTEGER
    );      

---------------------------------------------------------------------------------------
-- Получить подписантов по поставщику (BRANCH_ID у договора)
PROCEDURE BILL_SIGNER_BY_CONTRACTOR( 
           p_recordset          OUT t_refc,
           p_contractor_id      IN  NUMBER,
           p_date_to            IN  DATE
);

-- ------------------------------------------------------------------------------- --
-- получить данные для заполнения заголока документов при:
-- отправке адресов
--   - при ошибке выставляет исключение

    PROCEDURE BILL_HEADER_ADR( 
               p_recordset     OUT t_refc,
               p_rep_period_id IN INTEGER,              -- ID периода счета
               p_account_id    IN INTEGER DEFAULT NULL  -- ID счета              
           );           
  
-- ------------------------------------------------------------------------------- --
-- получить данные для печати строк счета-фактуры
--   - при ошибке выставляет исключение
PROCEDURE BILL_INV_ITEMS( 
         p_recordset    OUT t_refc, 
         p_rep_period_id IN INTEGER,   -- ID периода счета
         p_bill_id       IN INTEGER    -- ID счета
     );

-- ------------------------------------------------------------------------------- --
-- Строки счета-фактуры для разового счета
--   - при ошибке выставляет исключение
PROCEDURE BILL_INV_ITEMS_FOR_MANUAL( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           );
-- ------------------------------------------------------------------------------- --           
-- Строки корректирующей счета-фактуры
--   - при ошибке выставляет исключение
PROCEDURE BILL_INV_ITEMS_FOR_CORRECT( 
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
--  Получить номер родительского счета (главного, первого)
FUNCTION BILL_HISTORY_FOR_DEBET_BASE ( 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           ) RETURN VARCHAR2;
               
-- ------------------------------------------------------------------------------- --
--  Список услуг и их компонентов услуги к счету
--   - при ошибке выставляет исключение

PROCEDURE DETAIL_ITEMS ( 
           p_recordset    OUT t_refc, 
           p_rep_period_id IN INTEGER,   -- ID периода счета
           p_bill_id       IN INTEGER    -- ID счета
       );
           
--=======================================================================
--Детализированные данны для услуги "Трафик IP (объемный)"
--=======================================================================           
PROCEDURE DETAIL_TRAFFIC_IP(
          p_recordset     OUT t_refc,
          p_rep_period_id IN INTEGER,   -- ID периода счета
          p_bill_id       IN INTEGER,    -- ID счета
          p_item_id       IN INTEGER     -- ID позиции счета
);

--=======================================================================
--Детализированные данны для услуги "Трафик IP Burst"
--=======================================================================
procedure DETAIL_TRAFFIC_BURST(
          p_recordset     OUT t_refc,
          p_rep_period_id IN INTEGER,   -- ID периода счета
          p_bill_id       IN INTEGER,    -- ID счета
          p_item_id       IN INTEGER     -- ID позиции счета
);

--=======================================================================
--Детализированные данны для услуги "Трафик IP Volume"
--=======================================================================
procedure DETAIL_TRAFFIC_VOLUME(
          p_recordset     OUT t_refc,
          p_rep_period_id IN INTEGER,   -- ID периода счета
          p_bill_id       IN INTEGER,    -- ID счета
          p_item_id       IN INTEGER     -- ID позиции счета
);

--=======================================================================
--Детализированные данны для услуги "Трафик IP VPN"
--=======================================================================
procedure DETAIL_TRAFFIC_VPN(
          p_recordset     OUT t_refc,
          p_rep_period_id IN INTEGER,   -- ID периода счета
          p_bill_id       IN INTEGER,    -- ID счета
          p_item_id       IN INTEGER     -- ID позиции счета
);           
   
    -- ------------------------------------------------------------------------------- --
    --           Деталировка к счету за услуги связи по указанной позиции
    -- Услуга: Услуги международной и междугородной телефонной связи
    --   - при ошибке выставляет исключение
    PROCEDURE DETAIL_TRAFFIC_MGMN( 
                   p_recordset    OUT t_refc, 
                   p_rep_period_id IN INTEGER,   -- ID периода счета
                   p_bill_id       IN INTEGER,   -- ID счета
                   p_item_id       IN INTEGER    -- ID позиции счета
               );
   -- ------------------------------------------------------------------------------- --
   -- Суммы минут по детализации МГМН/8800/операторка               
   PROCEDURE DETAIL_TRAFFIC_MGMN_SUM( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER   -- ID счета
           );
-- ------------------------------------------------------------------------------- --
--           Деталировка к счету за услуги связи по указанной позиции
-- Услуга: межоператорка
--   - при ошибке выставляет исключение
     PROCEDURE DETAIL_TRAFFIC_OPER( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER,   -- ID счета
               p_item_id       IN INTEGER    -- ID позиции счета
           );

-- ------------------------------------------------------------------------------- --
--  Деталировка к счету для ЦСС     
PROCEDURE DETAIL_TRAFFIC_MGMN_CSS( 
               p_recordset    OUT t_refc, 
               p_period_id    IN INTEGER,   
               p_bill_id      IN INTEGER,
               p_direction    IN VARCHAR2
);
--=======================================================================
-- Преобразование адреса в строку
--=======================================================================
FUNCTION ADDRESS_TO_STRING (
         p_zip           IN VARCHAR2,
         p_state         IN  VARCHAR2,
         p_city          IN  VARCHAR2,
         p_address       IN  VARCHAR2
) return VARCHAR2 ;

PROCEDURE BILL_HEADER_EN( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,              -- ID периода счета
               p_bill_id       IN INTEGER DEFAULT NULL  -- ID счета
           );

-- ------------------------------------------------------------------------------- --
-- получить данные для заполнения заголока документов (с/ф на аванс)
--   - при ошибке выставляет исключение
PROCEDURE BILL_HEADER_ADVANCE( 
               p_recordset        OUT t_refc, 
               p_rep_period_id    IN INTEGER,              -- ID периода счета
               p_advance_id       IN INTEGER DEFAULT NULL  -- ID счета
           );
           
-- ------------------------------------------------------------------------------- --
-- Строки счета-фактуры
--   - при ошибке выставляет исключение
PROCEDURE BILL_INV_ITEMS_EN( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           );           
 -- ------------------------------------------------------------------------------- --
--  Список услуг и их компонентов услуги к счету
--   - при ошибке выставляет исключение
PROCEDURE DETAIL_ITEMS_EN ( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           );
           
--=======================
-- Исключение для у.е. лицевых счетов с конвертацией на дату оплаты
PROCEDURE BILL_INV_ITEMS_CU( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           );

-- ------------------------------------------------------------------------------- --
--  Список услуг и их компонентов услуги к счету (Кастом для детализации для Альфа-Банка)
--   - при ошибке выставляет исключение
PROCEDURE DETAIL_ITEMS_ALFABANK ( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
);           
------------------------------------------------------------------------
-- Работа с нестандартными формами счета
------------------------------------------------------------------------
PROCEDURE GetPrintDocumentsExcl(p_recordset OUT t_refc, p_account_id IN INTEGER);
-----------
PROCEDURE SavePrintDocumentsExcl(
          p_account_id                                       IN INTEGER,
          p_contract_id                                      IN INTEGER,
          p_jasper_bill                                      IN VARCHAR2,
          p_jasper_akt                                       IN VARCHAR2, 
          p_jasper_factura                                   IN VARCHAR2, 
          p_jasper_detail                                    IN VARCHAR2, 
          p_function_bill                                    IN VARCHAR2, 
          p_function_akt                                     IN VARCHAR2, 
          p_function_factura                                 IN VARCHAR2, 
          p_function_detail                                  IN VARCHAR2, 
          p_header_akt                                       IN VARCHAR2, 
          p_function_bill_header                             IN VARCHAR2, 
          p_doc_lang                                         IN VARCHAR2);   
          
--------------------------------------------------------------------------------------
-- Работа с нестандартными параметрами счета
--------------------------------------------------------------------------------------    
PROCEDURE GetPrintDocumentsParams(p_recordset OUT t_refc, p_account_id IN INTEGER);
---------
FUNCTION CreateDocExtParam(    p_bill_id                    IN INTEGER,
                                p_account_id                 IN INTEGER,
                                p_contract_id                IN INTEGER,                                                                
                                p_param_name                 IN VARCHAR2,
                                p_param_value                IN VARCHAR2,
                                p_type_doc                   IN VARCHAR2,
                                p_notes                      IN VARCHAR2) RETURN INTEGER;      
--------------
PROCEDURE UpdateDocExtParam(    p_param_id                   IN INTEGER,
                                p_bill_id                    IN INTEGER,
                                p_account_id                 IN INTEGER,
                                p_contract_id                IN INTEGER,                                                                
                                p_param_name                 IN VARCHAR2,
                                p_param_value                IN VARCHAR2,
                                p_type_doc                   IN VARCHAR2,
                                p_notes                      IN VARCHAR2);         
-------
PROCEDURE GetExtParamById(p_recordset OUT t_refc, p_param_id IN INTEGER); 
----------------------------------------------------------------
-- Получить список всех нестандартных параметров
----------------------------------------------------------------
PROCEDURE GetExtParamList(p_recordset OUT t_refc);              
END PK112_PRINT;
/
CREATE OR REPLACE PACKAGE BODY PK112_PRINT
IS

-- ------------------------------------------------------------------------------- --
-- получить данные для заполнения заголока документов:
-- счета, счета-фактуры, акта передачи-приема, детализации
--   - при ошибке выставляет исключение
PROCEDURE BILL_HEADER( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,              -- ID периода счета
               p_bill_id       IN INTEGER DEFAULT NULL  -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BILL_HEADER';
    v_retcode    INTEGER;
    v_period_to  DATE;
BEGIN
    -- получаем верхнюю границу периода
    v_period_to := trunc(Pk04_Period.Period_to(p_rep_period_id));
    -- возвращаем курсор
    OPEN p_recordset FOR
        WITH ADDR_GRP AS    -- адрес грузополучателя(для счета) 
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON, CODE_REGION
                 FROM ACCOUNT_CONTACT_T ac 
                 WHERE AC.ADDRESS_TYPE =  'GRP'
                   AND DATE_FROM<= v_period_to AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ), 
             ADDR_GRP_ACT AS    -- адрес грузополучателя(для акта) 
             -- ПЕРЕДАЛ NULL В PERSON, т.к. печатать надо ТОЛЬКО на счете
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, null PERSON, CODE_REGION
                 FROM ACCOUNT_CONTACT_T ac 
                 WHERE AC.ADDRESS_TYPE =  'GRP'
                   AND DATE_FROM<= v_period_to AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ),    
             ADDR_JUR AS    -- юр. адрес 
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON, CODE_REGION
                 FROM ACCOUNT_CONTACT_T  
                 WHERE ADDRESS_TYPE = 'JUR'
                   AND DATE_FROM<= v_period_to AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ),
             ADDR_DLV AS    -- адрес доставки
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON, CODE_REGION
                 FROM ACCOUNT_CONTACT_T  
                 WHERE ADDRESS_TYPE =  'DLV'
                   AND DATE_FROM<= v_period_to 
                   AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ),    
             ADDR_CTR AS    -- адрес получателя (исполнителя)
             (SELECT CONTRACTOR_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PHONE_ACCOUNT, PHONE_BILLING, FAX, CODE_REGION
                 FROM CONTRACTOR_ADDRESS_T  
                 WHERE ADDRESS_TYPE = 'JUR'
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
                   WHERE S.SIGNER_ROLE_ID = 6101
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
                   WHERE S.SIGNER_ROLE_ID =  6102 
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
               B.TAX TAX_SUMM,
               AP.VAT TAX,                  -- Процент налона
               B.CURRENCY_ID,               -- ID валюты счета
               B.ACT_DATE_FROM,             -- диапазон оказания услуг
               B.ACT_DATE_TO,               -- для акта передачи приема
               -- ПОЛУЧАТЕЛЬ/ПРОДАВЕЦ (поставщик услуг) -----------------------
               -- координаты получателя (поставщика)  - - - - - - - - - - - - -
               CR.CONTRACTOR_ID,            -- ID получателя
               NVL(C_HIST.CONTRACTOR, CR.CONTRACTOR) CONTRACTOR_NAME,               -- поставщик услуг
               NVL(C_HIST.SHORT_NAME, CR.SHORT_NAME) CONTRACTOR_NAME_SHORT, -- краткое наименование 
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
               ADDR_CTR.CODE_REGION,
               -- адрес получателя (головного офиса) - - - - - - - - - - - - - - - - - - - - - -
               ADDR_CTR_PARENT.COUNTRY PARENT_COUNTRY, 
               ADDR_CTR_PARENT.ZIP PARENT_ZIP,
               ADDR_CTR_PARENT.STATE PARENT_STATE,              -- регион (область, край,...)
               ADDR_CTR_PARENT.CITY PARENT_CITY,               -- город
               ADDR_CTR_PARENT.ADDRESS PARENT_ADDRESS,            -- строка адреса
               NULL PARENT_PERSON,                 -- загрушка (обязательно поле для получения адреса)...
               ADDR_CTR_PARENT.CODE_REGION PARENT_CODE_REGION,
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
               NVL(C_HIST_PARENT.CONTRACTOR, CR_PARENT.CONTRACTOR) CONTRACTOR_PARENT_NAME,               -- поставщик услуг
               NVL(C_HIST_PARENT.SHORT_NAME, CR_PARENT.SHORT_NAME) CONTRACTOR_PARENT_NAME_SHORT, -- краткое наименование 
               CR_PARENT.INN CONTRACTOR_PARENT_INN,
               CR_PARENT.KPP CONTRACTOR_PARENT_KPP,
               --
               -- ПЛАТЕЛЬЩИК/ПОКУПАТЕЛЬ ---------------------------------------
               -- координаты покупателя - - - - - - - - - - - - - - - - - - - -
               CM.COMPANY_ID            COMPANY_ID,
               CM.COMPANY_NAME          COMPANY_NAME,      -- название компании покупателя
               CM.SHORT_NAME            COMPANY_NAME_SHORT,  -- краткое название компании покупателя
               CM.INN                   COMPANY_INN,
               AP.KPP                   COMPANY_KPP,
               CM.DATE_FROM             COMPANY_DATE_FROM,
               CM.DATE_TO               COMPANY_DATE_TO,
               PK112_PRINT.BILL_HEADER_EXT_PARAM_FUNC(p_bill_id, p_rep_period_id, 'p_customer','BILL')    COMPANY_NAME_BILL,
               PK112_PRINT.BILL_HEADER_EXT_PARAM_FUNC(p_bill_id, p_rep_period_id, 'p_customer','ACT')     COMPANY_NAME_ACT,
               PK112_PRINT.BILL_HEADER_EXT_PARAM_FUNC(p_bill_id, p_rep_period_id, 'p_customer','FACTURA') COMPANY_NAME_FACTURA,                     
               -- Юридический адрес плательщика
               ADDR_JUR.COUNTRY  JUR_COUNTRY, 
               ADDR_JUR.ZIP      JUR_ZIP, 
               ADDR_JUR.STATE    JUR_STATE, 
               ADDR_JUR.CITY     JUR_CITY, 
               ADDR_JUR.ADDRESS  JUR_ADDRESS,
               ADDR_JUR.PERSON   JUR_PERSON,
               ADDR_JUR.CODE_REGION JUR_CODE_REGION,
               -- Адрес грузополучателя
               ADDR_GRP.COUNTRY  GRP_COUNTRY, 
               ADDR_GRP.ZIP      GRP_ZIP, 
               ADDR_GRP.STATE    GRP_STATE, 
               ADDR_GRP.CITY     GRP_CITY, 
               ADDR_GRP.ADDRESS  GRP_ADDRESS,
               ADDR_GRP.PERSON   GRP_PERSON,
               ADDR_GRP.CODE_REGION  GRP_CODE_REGION,
               -- Адрес грузополучателя (акт)
               ADDR_GRP_ACT.PERSON GRP_ACT_PERSON,
               -- Почтовый адрес плательщика
               ADDR_DLV.COUNTRY  DLV_COUNTRY, 
               ADDR_DLV.ZIP      DLV_ZIP, 
               ADDR_DLV.STATE    DLV_STATE, 
               ADDR_DLV.CITY     DLV_CITY, 
               ADDR_DLV.ADDRESS  DLV_ADDRESS,
               ADDR_DLV.PERSON   DLV_PERSON,
               ADDR_DLV.CODE_REGION  DLV_CODE_REGION,
               A.BILLING_ID,
               PRINT_DOC_EXC.JASPER_BILL,
               PRINT_DOC_EXC.JASPER_AKT,
               PRINT_DOC_EXC.JASPER_FACTURA,
               PRINT_DOC_EXC.JASPER_DETAIL,
               PRINT_DOC_EXC.FUNCTION_BILL,
               PRINT_DOC_EXC.FUNCTION_AKT,
               PRINT_DOC_EXC.FUNCTION_FACTURA,
               PRINT_DOC_EXC.FUNCTION_DETAIL,
               PRINT_DOC_EXC.DOC_LANG,               
               NVL(AD.IS_STAMP, 0) IS_STAMP,
               AD.DELIVERY_METHOD_ID,
               AD.DELIVERY_METHOD_NAME,
               NULL PAYMENT_NO,
               NULL PAYMENT_DATE
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, CONTRACTOR_T CR,  
               COMPANY_T CM,
               BILL_T B, 
               CONTRACTOR_BANK_T CB, ADDR_GRP, ADDR_GRP_ACT, ADDR_JUR, ADDR_DLV, ADDR_CTR, ADDR_CTR ADDR_CTR_PARENT,
               SIGNER_R, SIGNER_B,STAMP,
               CONTRACTOR_T CR_PARENT,
               D_CL_TYPE,
               D_MARK_SEG,
               D_CONTR_TYPE,               
               PRINT_DOC_EXC,
               CONTRACTOR_HIST_T C_HIST,
               CONTRACTOR_HIST_T C_HIST_PARENT,
               (
                 SELECT account_id,
                         delivery_method_id,
                         d.name DELIVERY_METHOD_NAME,
                         CASE WHEN D.NAME LIKE '%штамп%' THEN 1 ELSE 0 END IS_STAMP
                    FROM account_documents_t ad, dictionary_t d
                   WHERE AD.DELIVERY_METHOD_ID = d.key_id AND doc_bill = 'Y'
               ) AD
         WHERE A.ACCOUNT_TYPE = 'J'
           AND A.ACCOUNT_ID = B.ACCOUNT_ID           
           AND A.ACCOUNT_ID = ADDR_GRP.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = ADDR_GRP_ACT.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = ADDR_JUR.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = ADDR_DLV.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID           
           AND B.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND B.CONTRACTOR_BANK_ID = CB.BANK_ID
           AND B.CONTRACT_ID = C.CONTRACT_ID
           AND C_HIST.CONTRACTOR_ID (+)= CR.CONTRACTOR_ID
           AND C_HIST_PARENT.CONTRACTOR_ID (+)= CR_PARENT.CONTRACTOR_ID
           AND B.BILL_DATE BETWEEN C_HIST.DATE_FROM AND NVL(C_HIST.DATE_TO, TO_DATE('01.01.2050','DD.MM.YYYY'))
           AND B.BILL_DATE BETWEEN C_HIST_PARENT.DATE_FROM AND NVL(C_HIST_PARENT.DATE_TO, TO_DATE('01.01.2050','DD.MM.YYYY'))
           AND AP.PROFILE_ID = B.PROFILE_ID
           AND B.BILL_STATUS IN ('READY','CLOSED','OPEN', 'CHECK','PREPAID')
           AND NVL(CR.PARENT_ID, 1) = CR_PARENT.CONTRACTOR_ID            
           AND CR_PARENT.CONTRACTOR_ID = ADDR_CTR_PARENT.CONTRACTOR_ID
           AND CR.CONTRACTOR_ID = ADDR_CTR.CONTRACTOR_ID 
           AND AP.CONTRACT_ID   = CM.CONTRACT_ID (+)
           AND TRUNC(B.BILL_DATE) BETWEEN CM.DATE_FROM AND NVL(CM.DATE_TO, TO_DATE('01.01.2050','DD.MM.YYYY'))
           AND CB.DATE_FROM <= B.BILL_DATE AND (CB.DATE_TO IS NULL OR B.BILL_DATE <= CB.DATE_TO)
           AND AP.BRANCH_ID = SIGNER_R.CONTRACTOR_ID(+)
           AND AP.BRANCH_ID = SIGNER_B.CONTRACTOR_ID(+)
           AND AP.BRANCH_ID = STAMP.contractor_id(+)
           AND D_CL_TYPE.KEY_ID (+)= C.CLIENT_TYPE_ID
           AND D_MARK_SEG.KEY_ID (+)= C.MARKET_SEGMENT_ID 
           AND D_CONTR_TYPE.KEY_ID (+)=C.CONTRACT_TYPE_ID
           and a.account_Id = PRINT_DOC_EXC.bill_account_id(+)
           AND a.account_id = ad.account_id(+)
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

-- ------------------------------------------------------------------------------- --
-- получить данные для заполнения заголока документов (с/ф на аванс)
--   - при ошибке выставляет исключение
PROCEDURE BILL_HEADER_ADVANCE( 
               p_recordset        OUT t_refc, 
               p_rep_period_id    IN INTEGER,              -- ID периода счета
               p_advance_id       IN INTEGER DEFAULT NULL  -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BILL_HEADER_ADVANCE';
    v_retcode    INTEGER;
    v_period_to  DATE;
BEGIN
    -- получаем верхнюю границу периода
    v_period_to := trunc(Pk04_Period.Period_to(p_rep_period_id));
    -- возвращаем курсор
    OPEN p_recordset FOR
        WITH ADDR_GRP AS    -- адрес грузополучателя(для счета) 
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON, CODE_REGION
                 FROM ACCOUNT_CONTACT_T ac 
                 WHERE AC.ADDRESS_TYPE =  'GRP'
                   AND DATE_FROM<=v_period_to AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ), 
             ADDR_GRP_ACT AS    -- адрес грузополучателя(для акта) 
             -- ПЕРЕДАЛ NULL В PERSON, т.к. печатать надо ТОЛЬКО на счете
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, null PERSON, CODE_REGION
                 FROM ACCOUNT_CONTACT_T ac 
                 WHERE AC.ADDRESS_TYPE =  'GRP'
                   AND DATE_FROM<= v_period_to AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ),    
             ADDR_JUR AS    -- юр. адрес 
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON, CODE_REGION
                 FROM ACCOUNT_CONTACT_T  
                 WHERE ADDRESS_TYPE = 'JUR'
                   AND DATE_FROM<= v_period_to AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ),
             ADDR_DLV AS    -- адрес доставки
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON, CODE_REGION
                 FROM ACCOUNT_CONTACT_T  
                 WHERE ADDRESS_TYPE =  'DLV'
                   AND DATE_FROM<= v_period_to 
                   AND (DATE_TO IS NULL OR v_period_to <= DATE_TO)
                 ),    
             ADDR_CTR AS    -- адрес получателя (исполнителя)
             (SELECT CONTRACTOR_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PHONE_ACCOUNT, PHONE_BILLING, FAX, CODE_REGION
                 FROM CONTRACTOR_ADDRESS_T  
                 WHERE ADDRESS_TYPE = 'JUR'
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
                   WHERE S.SIGNER_ROLE_ID = 6101
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
                   WHERE S.SIGNER_ROLE_ID =  6102 
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
               B.ADVANCE_ID BILL_ID,                   -- ID счета
               B.REP_PERIOD_ID,             -- Период счета
               B.ADVANCE_NO BILL_NO,                   -- Номер выставленного счета
               'B' BILL_TYPE,                 -- Тип счета
               B.ADVANCE_DATE BILL_DATE,    -- Дата счета
               B.TOTAL,                     -- Сумма счета с НДС
               B.GROSS,                     -- Сумма счета без НДС
               B.TAX TAX_SUMM,
               CASE WHEN AP.VAT = 18 THEN '18%/118' ELSE TO_CHAR(AP.VAT) END  TAX,  --Процент налога
               B.CURRENCY_ID,               -- ID валюты счета
               NULL ACT_DATE_FROM,             -- диапазон оказания услуг
               NULL ACT_DATE_TO,               -- для акта передачи приема
               -- ПОЛУЧАТЕЛЬ/ПРОДАВЕЦ (поставщик услуг) -----------------------
               -- координаты получателя (поставщика)  - - - - - - - - - - - - -
               CR.CONTRACTOR_ID,            -- ID получателя
               NVL(C_HIST.CONTRACTOR, CR.CONTRACTOR) CONTRACTOR_NAME,               -- поставщик услуг
               NVL(C_HIST.SHORT_NAME, CR.SHORT_NAME) CONTRACTOR_NAME_SHORT, -- краткое наименование 
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
               ADDR_CTR.CODE_REGION,
               -- адрес получателя (головного офиса) - - - - - - - - - - - - - - - - - - - - - -
               ADDR_CTR_PARENT.COUNTRY PARENT_COUNTRY, 
               ADDR_CTR_PARENT.ZIP PARENT_ZIP,
               ADDR_CTR_PARENT.STATE PARENT_STATE,              -- регион (область, край,...)
               ADDR_CTR_PARENT.CITY PARENT_CITY,               -- город
               ADDR_CTR_PARENT.ADDRESS PARENT_ADDRESS,            -- строка адреса
               NULL PARENT_PERSON,                 -- загрушка (обязательно поле для получения адреса)...
               ADDR_CTR_PARENT.CODE_REGION PARENT_CODE_REGION,
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
               NVL(C_HIST_PARENT.CONTRACTOR, CR_PARENT.CONTRACTOR) CONTRACTOR_PARENT_NAME,               -- поставщик услуг
               NVL(C_HIST_PARENT.SHORT_NAME, CR_PARENT.SHORT_NAME) CONTRACTOR_PARENT_NAME_SHORT, -- краткое наименование 
               CR_PARENT.INN CONTRACTOR_PARENT_INN,
               CR_PARENT.KPP CONTRACTOR_PARENT_KPP,
               --
               -- ПЛАТЕЛЬЩИК/ПОКУПАТЕЛЬ ---------------------------------------
               -- координаты покупателя - - - - - - - - - - - - - - - - - - - -
               CM.COMPANY_ID            COMPANY_ID,
               CM.COMPANY_NAME          COMPANY_NAME,      -- название компании покупателя
               CM.SHORT_NAME            COMPANY_NAME_SHORT,  -- краткое название компании покупателя
               CM.INN                   COMPANY_INN,
               AP.KPP                   COMPANY_KPP,
               CM.DATE_FROM             COMPANY_DATE_FROM,
               CM.DATE_TO               COMPANY_DATE_TO,
               NULL COMPANY_NAME_BILL,--PK112_PRINT.BILL_HEADER_EXT_PARAM_FUNC(:p_bill_id, :p_rep_period_id, 'p_customer','BILL')    COMPANY_NAME_BILL,
               NULL COMPANY_NAME_ACT,--PK112_PRINT.BILL_HEADER_EXT_PARAM_FUNC(:p_bill_id, :p_rep_period_id, 'p_customer','ACT')     COMPANY_NAME_ACT,
               NULL COMPANY_NAME_FACTURA,--PK112_PRINT.BILL_HEADER_EXT_PARAM_FUNC(:p_bill_id, :p_rep_period_id, 'p_customer','FACTURA') COMPANY_NAME_FACTURA,                     
               -- Юридический адрес плательщика
               ADDR_JUR.COUNTRY  JUR_COUNTRY, 
               ADDR_JUR.ZIP      JUR_ZIP, 
               ADDR_JUR.STATE    JUR_STATE, 
               ADDR_JUR.CITY     JUR_CITY, 
               ADDR_JUR.ADDRESS  JUR_ADDRESS,
               ADDR_JUR.PERSON   JUR_PERSON,
               ADDR_JUR.CODE_REGION JUR_CODE_REGION,
               -- Адрес грузополучателя
               ADDR_GRP.COUNTRY  GRP_COUNTRY, 
               ADDR_GRP.ZIP      GRP_ZIP, 
               ADDR_GRP.STATE    GRP_STATE, 
               ADDR_GRP.CITY     GRP_CITY, 
               ADDR_GRP.ADDRESS  GRP_ADDRESS,
               ADDR_GRP.PERSON   GRP_PERSON,
               ADDR_GRP.CODE_REGION  GRP_CODE_REGION,
               -- Адрес грузополучателя (акт)
               ADDR_GRP_ACT.PERSON GRP_ACT_PERSON,
               -- Почтовый адрес плательщика
               ADDR_DLV.COUNTRY  DLV_COUNTRY, 
               ADDR_DLV.ZIP      DLV_ZIP, 
               ADDR_DLV.STATE    DLV_STATE, 
               ADDR_DLV.CITY     DLV_CITY, 
               ADDR_DLV.ADDRESS  DLV_ADDRESS,
               ADDR_DLV.PERSON   DLV_PERSON,
               ADDR_DLV.CODE_REGION  DLV_CODE_REGION,
               A.BILLING_ID,
               NULL JASPER_BILL,
               NULL JASPER_AKT,
               NULL JASPER_FACTURA,
               NULL JASPER_DETAIL,
               NULL FUNCTION_BILL,
               NULL FUNCTION_AKT,
               NULL FUNCTION_FACTURA,
               NULL FUNCTION_DETAIL,
               NULL DOC_LANG,               
               NVL(AD.IS_STAMP, 0) IS_STAMP,
               AD.DELIVERY_METHOD_ID,
               AD.DELIVERY_METHOD_NAME,
               pa.doc_id payment_no,
               pa.payment_date
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, CONTRACTOR_T CR,  
               COMPANY_T CM,
               ADVANCE_T B, 
               ADVANCE_ITEM_T AI,
               CONTRACTOR_BANK_T CB, 
               ADDR_GRP, ADDR_GRP_ACT, 
               ADDR_JUR, ADDR_DLV, ADDR_CTR, ADDR_CTR ADDR_CTR_PARENT,
               SIGNER_R, SIGNER_B,STAMP,
               CONTRACTOR_T CR_PARENT,
               D_CL_TYPE,
               D_MARK_SEG,
               D_CONTR_TYPE,                              
               CONTRACTOR_HIST_T C_HIST,
               CONTRACTOR_HIST_T C_HIST_PARENT,
               (
                 SELECT account_id,
                         delivery_method_id,
                         d.name DELIVERY_METHOD_NAME,
                         CASE WHEN D.NAME LIKE '%штамп%' THEN 1 ELSE 0 END IS_STAMP
                    FROM account_documents_t ad, dictionary_t d
                   WHERE AD.DELIVERY_METHOD_ID = d.key_id AND doc_bill = 'Y'
               ) AD,
               PAYMENT_T PA
         WHERE A.ACCOUNT_TYPE = 'J'
           AND A.ACCOUNT_ID = B.ACCOUNT_ID           
           AND A.ACCOUNT_ID = ADDR_GRP.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = ADDR_GRP_ACT.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = ADDR_JUR.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = ADDR_DLV.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID           
           AND AP.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND AP.CONTRACTOR_BANK_ID = CB.BANK_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND C_HIST.CONTRACTOR_ID (+)= CR.CONTRACTOR_ID
           AND C_HIST_PARENT.CONTRACTOR_ID (+)= CR_PARENT.CONTRACTOR_ID
           AND B.ADVANCE_DATE BETWEEN C_HIST.DATE_FROM AND NVL(C_HIST.DATE_TO, TO_DATE('01.01.2050','DD.MM.YYYY'))
           AND B.ADVANCE_DATE BETWEEN C_HIST_PARENT.DATE_FROM AND NVL(C_HIST_PARENT.DATE_TO, TO_DATE('01.01.2050','DD.MM.YYYY'))           
           AND B.ADVANCE_DATE BETWEEN AP.DATE_FROM AND NVL(ap.date_to, TO_DATE('01.01.2050','DD.MM.YYYY'))
           AND B.ADVANCE_STATUS IN ('READY','CLOSED','OPEN', 'CHECK','PREPAID')
           AND NVL(CR.PARENT_ID, 1) = CR_PARENT.CONTRACTOR_ID            
           AND CR_PARENT.CONTRACTOR_ID = ADDR_CTR_PARENT.CONTRACTOR_ID
           AND CR.CONTRACTOR_ID = ADDR_CTR.CONTRACTOR_ID 
           AND AP.CONTRACT_ID   = CM.CONTRACT_ID (+)
           AND TRUNC(B.ADVANCE_DATE) BETWEEN CM.DATE_FROM AND NVL(CM.DATE_TO, TO_DATE('01.01.2050','DD.MM.YYYY'))
           AND CB.DATE_FROM <= B.ADVANCE_DATE AND (CB.DATE_TO IS NULL OR B.ADVANCE_DATE <= CB.DATE_TO)
           AND AP.BRANCH_ID = SIGNER_R.CONTRACTOR_ID(+)
           AND AP.BRANCH_ID = SIGNER_B.CONTRACTOR_ID(+)
           AND AP.BRANCH_ID = STAMP.contractor_id(+)
           AND D_CL_TYPE.KEY_ID (+)= C.CLIENT_TYPE_ID
           AND D_MARK_SEG.KEY_ID (+)= C.MARKET_SEGMENT_ID 
           AND D_CONTR_TYPE.KEY_ID (+)=C.CONTRACT_TYPE_ID           
           AND A.ACCOUNT_ID  = AD.ACCOUNT_ID(+)
           AND PA.PAYMENT_ID = AI.PAYMENT_ID(+)
           AND PA.REP_PERIOD_ID = AI.PAY_PERIOD_ID(+)
           AND AI.ADVANCE_ID  = B.ADVANCE_ID(+)
           AND AI.ADV_PERIOD_ID = B.REP_PERIOD_ID(+)
           AND B.REP_PERIOD_ID  = p_rep_period_id
           AND B.ADVANCE_ID = p_advance_id;
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
PROCEDURE BILL_HEADER_EXT_PARAM( 
             p_recordset     OUT t_refc,
             p_bill_id       IN INTEGER,      -- ID счета              
             p_period_id     IN INTEGER       -- ID периода счета
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BILL_HEADER_EXT_PARAM';
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

--====================================================================================
-- Загрузка дополнительных параметров для счета
FUNCTION BILL_HEADER_EXT_PARAM_FUNC( 
             p_bill_id       IN INTEGER,       -- ID счета              
             p_period_id     IN INTEGER,       -- ID периода счета
             p_param_name    IN VARCHAR2,      -- наименование параметра
             p_type_doc      IN VARCHAR2       -- тип документа
) RETURN VARCHAR2
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BILL_HEADER_EXT_PARAM_FUNC';
    v_retcode    INTEGER;
    v_ret        VARCHAR2(1000);
BEGIN        
   SELECT PARAM_VALUE INTO v_ret FROM (  
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
                           WHERE B.BILL_ID = p_bill_id
                                 AND B.REP_PERIOD_ID = p_period_id
                                 AND (   B.BILL_ID = exc.BILL_ID
                                      OR B.ACCOUNT_ID = exc.ACCOUNT_ID
                                      OR B.CONTRACT_ID = exc.CONTRACT_ID
                                      OR B.CONTRACTOR_ID = exc.CONTRACTOR_ID)))
           WHERE rn = 1
         )
       WHERE PARAM_NAME = p_param_name
             AND (TYPE_DOC = p_type_doc OR TYPE_DOC IS NULL);
    RETURN v_ret;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------------- --
-- Получить подписантов по заданными ID
PROCEDURE BILL_SIGNER_INFO( 
           p_recordset          OUT t_refc,
           p_signer_header_id   IN  INTEGER,
           p_signer_booker_id   IN  INTEGER,
           p_stamp_id           IN  INTEGER
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BILL_SIGNER_INFO';
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
PROCEDURE BILL_SIGNER_BY_CONTRACTOR( 
           p_recordset          OUT t_refc,
           p_contractor_id      IN  NUMBER,
           p_date_to            IN  DATE
)
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'BILL_SIGNER_BY_CONTRACTOR';
    v_retcode        INTEGER;
    v_contractor_id  INTEGER := p_contractor_id;
BEGIN
    -- Для ЦСС такие же подписанты как для КТТК
    IF v_contractor_Id = 200 THEN v_contractor_id := 11;
    END IF;

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
                                             AND (   S.CONTRACTOR_ID = v_contractor_id
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
                                             AND (   S.CONTRACTOR_ID = v_contractor_id
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
                                     AND (S.CONTRACTOR_ID = v_contractor_id OR S.CONTRACTOR_ID = 1)
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
    
PROCEDURE BILL_HEADER_ADR( 
               p_recordset     OUT t_refc,
               p_rep_period_id IN INTEGER,              -- ID периода счета
               p_account_id    IN INTEGER DEFAULT NULL  -- ID счета              
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BILL_HEADER_ADR';
    v_retcode    INTEGER;
    v_period_to  DATE;
BEGIN
    v_period_to := Pk04_Period.Period_to(p_rep_period_id);
    -- возвращаем курсор
    OPEN p_recordset FOR
    WITH ADDR_DLV AS    -- адрес доставки
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON,CODE_REGION
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
               CM.COMPANY_ID COMPANY_ID,                   -- на всякий случай оставил, м.б. ИНН/КПП илил еще чего нужно будет
               CM.COMPANY_NAME COMPANY_NAME,    -- название компании покупателя
               CM.SHORT_NAME COMPANY_NAME_SHORT,-- краткое название компании покупателя
               -- Почтовый адрес плательщика
               ADDR_DLV.COUNTRY  DLV_COUNTRY, 
               ADDR_DLV.ZIP      DLV_ZIP, 
               ADDR_DLV.STATE    DLV_STATE, 
               ADDR_DLV.CITY     DLV_CITY, 
               ADDR_DLV.ADDRESS  DLV_ADDRESS,
               ADDR_DLV.PERSON   DLV_PERSON,
               ADDR_DLV.CODE_REGION DLV_CODE_REGION
          FROM ACCOUNT_T A, 
               ACCOUNT_PROFILE_T AP, 
               CONTRACT_T C,          
               COMPANY_T CM,
               ADDR_DLV               
         WHERE A.ACCOUNT_TYPE = 'J'         
           AND A.ACCOUNT_ID = ADDR_DLV.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID           
      --     AND AP.DATE_FROM >= to_date(to_char(p_rep_period_id),'RRRRMM')          
           AND AP.CONTRACT_ID   = C.CONTRACT_ID
           AND CM.CONTRACT_ID   = C.CONTRACT_ID
           AND TRUNC(v_period_to) BETWEEN CM.DATE_FROM AND NVL(CM.DATE_TO, TO_DATE('01.01.2050','DD.MM.YYYY'))
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
PROCEDURE BILL_INV_ITEMS( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BILL_INV_ITEMS';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
     SELECT II.BILL_ID, II.REP_PERIOD_ID, II.INV_ITEM_ID, II.INV_ITEM_NO, II.SERVICE_ID, 
            NVL((SELECT SRV_NAME FROM SERVICE_ALIAS_T WHERE SERVICE_ID = II.SERVICE_ID AND ACCOUNT_ID = B.ACCOUNT_ID) , II.INV_ITEM_NAME) NAME, -- Наименование
            II.TOTAL         ITEM_TOTAL,           -- Стоимость с налогом
            II.GROSS         ITEM_NETTO,           -- Стоимость без налога
            II.TAX           ITEM_TAX,             -- Сумма налога
            II.VAT           TAX  ,                -- Налоговая ставка
            II.DATE_FROM     USAGE_START,          -- Диапазон дат 
            II.DATE_TO       USAGE_END
       FROM 
            INVOICE_ITEM_T II, 
            BILL_T B
      WHERE 
            II.BILL_ID           = B.BILL_ID
            AND II.REP_PERIOD_ID = p_rep_period_id 
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
-- Строки счета-фактуры для разового счета
--   - при ошибке выставляет исключение
PROCEDURE BILL_INV_ITEMS_FOR_MANUAL( 
       p_recordset    OUT t_refc, 
       p_rep_period_id IN INTEGER,   -- ID периода счета
       p_bill_id       IN INTEGER    -- ID счета
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BILL_INV_ITEMS_FOR_MANUAL';
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
PROCEDURE BILL_INV_ITEMS_FOR_CORRECT( 
               p_recordset     OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_new_bill_id   IN INTEGER,   -- ID дебетового счета
               p_old_bill_id   IN INTEGER    -- ID счета от которого строим корректировки. Если не задан, значит от предыдущего дебетового (должны быть связки в bill_t)
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'BILL_INV_ITEMS_FOR_CORRECT';
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
                   WHERE BILL_TYPE IN ('B', 'D', 'A', 'O') AND BILL_ID <> p_bill_id)  
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
--  Получить номер родительского счета (главного, первого)
FUNCTION BILL_HISTORY_FOR_DEBET_BASE ( 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           ) RETURN VARCHAR2
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BILL_HISTORY_FOR_DEBET_BASE';
    v_bill_no    VARCHAR2(255);
BEGIN
    -- построить курсор
         SELECT BILL_NO into v_bill_no
                FROM (SELECT ROW_NUMBER () OVER (ORDER BY LV DESC) RN, t.*
                        FROM (    SELECT LEVEL LV, --SYS_CONNECT_BY_PATH (bill_id, '/') way,
                                                  ac.*
                                    FROM (SELECT *
                                            FROM bill_t b
                                           WHERE b.account_id = (SELECT account_id
                                                                   FROM bill_t
                                                                  WHERE bill_id = p_bill_id)) ac
                              START WITH ac.bill_id = p_bill_id
                              CONNECT BY PRIOR ac.bill_id = ac.next_bill_id) t
                       WHERE BILL_TYPE IN ('B', 'D', 'A', 'O') AND BILL_ID <> p_bill_id)
               WHERE rn = 1;
               
     RETURN v_bill_no;
END;

-- ------------------------------------------------------------------------------- --
--  Список услуг и их компонентов услуги к счету
--   - при ошибке выставляет исключение
PROCEDURE DETAIL_ITEMS ( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DETAIL_ITEMS';
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
               OB.RATE_RULE_ID,
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
                   GROUP BY I.ITEM_ID, I.CHARGE_TYPE, I.BILL_ID, I.ORDER_ID, I.ORDER_BODY_ID, OB.RATE_RULE_ID, I.SERVICE_ID, I.SUBSERVICE_ID, I.DATE_FROM, I.DATE_TO, I.DESCR, I.NOTES, B.CURRENCY_ID, I.ITEM_CURRENCY_ID
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
                     I.BILL_ID, I.ORDER_ID,'Y' HAS_DTL 
                   FROM ITEM_T I, SERVICE_SUBSERVICE_T SS, ORDER_BODY_T OB, PRINT_DOCUMENTS_RATE_RULE PD
               WHERE 
                   I.SERVICE_ID = SS.SERVICE_ID (+)
                   AND I.ORDER_BODY_ID = OB.ORDER_BODY_ID
                   AND OB.RATE_RULE_ID = PD.RATE_RULE_ID (+)
                   AND I.SUBSERVICE_ID = SS.SUBSERVICE_ID (+)
                   AND I.REP_PERIOD_ID = p_rep_period_id
                   AND I.BILL_ID = p_bill_id
                   AND I.CHARGE_TYPE = 'USG'                                    
                   AND (NVL(PD.DTL_KEY, SS.DTL_KEY) IS NULL OR NVL(PD.DTL_KEY, SS.DTL_KEY) NOT IN ('TRAFFIC_IP_BURST'))  
               GROUP BY I.BILL_ID, I.ORDER_ID 
           )
        SELECT 
                  LST.BILL_ID,
                  LST.ITEM_ID,                  
                  LST.ORDER_ID,                   
                  CASE
                    WHEN OI.ORDER_NO_ALIAS IS NOT NULL THEN OI.ORDER_NO_ALIAS
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
                  LST.RATE_RULE_ID,
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
                  NVL(PDRR.DTL_KEY, SDT.DTL_KEY)  ITEM_ALIAS,
                  NVL(ORD_HAS_DTL.HAS_DTL,'N') HAS_DTL
              FROM 
                  LST, ORD_MIN, ORD_ABON, ORD_HAS_DTL, ORDER_T O, ORDER_INFO_T oi, SERVICE_T S, SUBSERVICE_T SS, SERVICE_SUBSERVICE_T SDT, ACCOUNT_T A, PRINT_DOCUMENTS_RATE_RULE PDRR
              WHERE LST.SERVICE_ID = S.SERVICE_ID
                  AND LST.SUBSERVICE_ID = SS.SUBSERVICE_ID
                  AND LST.SERVICE_ID = SDT.SERVICE_ID (+)
                  AND LST.SUBSERVICE_ID = SDT.SUBSERVICE_ID (+)  
                  AND LST.ORDER_ID = ORD_MIN.ORDER_ID (+)
--                  AND LST.ORDER_BODY_ID = ORD_MIN.ORDER_BODY_ID (+)
                  AND LST.ORDER_ID = ORD_ABON.ORDER_ID (+)
                  AND LST.ORDER_BODY_ID = ORD_ABON.ORDER_BODY_ID (+)
                  AND LST.ORDER_ID = ORD_HAS_DTL.ORDER_ID (+)
                  AND LST.BILL_ID = ORD_HAS_DTL.BILL_ID (+)
                  AND LST.RATE_RULE_ID = PDRR.RATE_RULE_ID (+)
                  AND O.ORDER_ID = LST.ORDER_ID
                  AND O.ORDER_ID = OI.ORDER_ID (+)
                  AND O.ACCOUNT_ID = A.ACCOUNT_ID                  
              ORDER BY S.service_id,  HAS_DTL, O.ORDER_NO,LST.DATE_FROM, DECODE(LST.CHARGE_TYPE,'REC',0,'MIN',1,'USG',3,'IDL',5,'ONT',10,'DIS',20),SS.subservice_id
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
procedure DETAIL_TRAFFIC_IP(
          p_recordset     OUT t_refc,
          p_rep_period_id IN INTEGER,   -- ID периода счета
          p_bill_id       IN INTEGER,    -- ID счета
          p_item_id       IN INTEGER     -- ID позиции счета
)
is
    v_prcName   constant varchar2(30) := 'DETAIL_TRAFFIC_IP';
begin                                            
    open p_recordset for
       select 
           bdr.ITEM_ID,
           D2.NAME    QUANTITY,   
           BDR.PRICE  TARIFF_SUM,
           BDR.AMOUNT TOTAL_SUM,
           BDR.CURRENCY_ID
        from 
            BDR_CCAD_T bdr,
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
procedure DETAIL_TRAFFIC_BURST(
          p_recordset     OUT t_refc,
          p_rep_period_id IN INTEGER,   -- ID периода счета
          p_bill_id       IN INTEGER,    -- ID счета
          p_item_id       IN INTEGER     -- ID позиции счета
)
is
    v_prcName   constant varchar2(30) := 'DETAIL_TRAFFIC_BURST';
begin
    open p_recordset for
        SELECT 
             BDR.EXCESS_SPEED QUANTITY,
             BDR.EXCESS_SPEED_UNIT,
             d2.NOTES EXCESS_SPEED_UNIT_NAME,
             PRICE TARIFF_SUMM,
             AMOUNT TOTAL_SUMM,
             CURRENCY_ID             
        FROM BDR_CCAD_T bdr, dictionary_t d1, dictionary_t d2
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
--Детализированные данны для услуги "Трафик IP Volume"
--=======================================================================
procedure DETAIL_TRAFFIC_VOLUME(
          p_recordset     OUT t_refc,
          p_rep_period_id IN INTEGER,   -- ID периода счета
          p_bill_id       IN INTEGER,    -- ID счета
          p_item_id       IN INTEGER     -- ID позиции счета
)
is
    v_prcName   constant varchar2(30) := 'DETAIL_TRAFFIC_VOLUME';
begin
    open p_recordset for
        SELECT 
             BDR.VOLUME,
             BDR.VOLUME_UNIT_ID,
             d1.NAME VOLUME_UNIT_NAME,
             PRICE TARIFF_SUMM,
             AMOUNT TOTAL_SUMM,
             CURRENCY_ID             
        FROM BDR_CCAD_T bdr, dictionary_t d1
       WHERE     BDR.VOLUME_UNIT_ID = D1.KEY_id
             AND RATE_RULE_ID = 2407
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
procedure DETAIL_TRAFFIC_VPN(
          p_recordset     OUT t_refc,
          p_rep_period_id IN INTEGER,   -- ID периода счета
          p_bill_id       IN INTEGER,    -- ID счета
          p_item_id       IN INTEGER     -- ID позиции счета
)
is
    v_prcName   constant varchar2(30) := 'DETAIL_TRAFFIC_VPN';
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
                  BDR_CCAD_T bdr, 
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
PROCEDURE DETAIL_TRAFFIC_MGMN( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER,   -- ID счета
               p_item_id       IN INTEGER    -- ID позиции счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DETAIL_TRAFIC_MGMN';
    v_retcode    INTEGER;
    v_billing_id INTEGER;
BEGIN
    --Определяем биллинг
      SELECT a.billing_id INTO v_billing_id
        FROM bill_t b, account_t a
       WHERE b.account_id = a.account_id
       and b.rep_period_id = p_rep_period_id
       and b.bill_id = p_bill_id;

    IF v_billing_id <> 2009 THEN     
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
     ELSE
       OPEN p_recordset FOR
           select  
                 D.ORDER_NO,                      
                 D.SERVICE_ID,                -- ID услуги
                 D.SUBSERVICE_ID,             -- ID компонента услуги
                 D.PREFIX PREFIX_B,
                 D.ZONE TERM_Z_NAME,               -- Направление
                 D.TARIFF_MIN TARIFF_AMOUNT,
                 CURRENCY_ID TARIFF_CURRENCY_ID,        
                 SUM(D.CALLS_NUM) CALLS,          -- Кол-во звонков, шт.
                 SUM(D.MINS) MINUTES,      -- Длительность, мин.
                 SUM(D.GROSS) TOTAL,          -- Стоимость, руб.
                 D.BILL_ID,                   -- ID счета
                 D.ITEM_ID,                   -- ID позиции счета
                 D.ORDER_ID  
        from DETAIL_BSRV_T d
          WHERE D.PERIOD_ID = p_rep_period_id
                   AND D.BILL_ID = p_bill_id
                   AND (D.ITEM_ID = p_item_id OR p_item_id IS NULL)
        GROUP BY  D.BILL_ID, D.ITEM_ID, D.ORDER_ID, D.ORDER_NO, D.SERVICE_ID, D.SUBSERVICE_ID, D.PREFIX, D.ZONE, D.TARIFF_MIN,CURRENCY_ID
        ORDER BY ORDER_NO, PREFIX;
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
-- Суммы минут по детализации МГМН/8800/операторка
PROCEDURE DETAIL_TRAFFIC_MGMN_SUM( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER   -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DETAIL_TRAFFIC_MGMN_SUM';
    v_retcode    INTEGER;
    v_billing_id INTEGER;
BEGIN
    --Определяем биллинг
      SELECT a.billing_id INTO v_billing_id
        FROM bill_t b, account_t a
       WHERE b.account_id = a.account_id
       and b.rep_period_id = p_rep_period_id
       and b.bill_id = p_bill_id;

    IF v_billing_id <> 2009 THEN     
        OPEN p_recordset FOR
              SELECT service_id, SUM (MINUTES) CALLS
                  FROM detail_mmts_t_jur
                 WHERE bill_id = p_bill_id 
                       AND rep_period_id = p_rep_period_id
              GROUP BY service_id;
     ELSE
       OPEN p_recordset FOR
           select                   
                 SERVICE_ID,                   -- ID услуги
                 SUM(MINS) CALLS          -- Кол-во звонков, шт.
        from DETAIL_BSRV_T
          WHERE PERIOD_ID = p_rep_period_id
                AND BILL_ID = p_bill_id                
        GROUP BY  SERVICE_ID;
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
--           Деталировка к счету за услуги связи по указанной позиции
-- Услуга: межоператорка
--   - при ошибке выставляет исключение
PROCEDURE DETAIL_TRAFFIC_OPER( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER,   -- ID счета
               p_item_id       IN INTEGER    -- ID позиции счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DETAIL_TRAFIC_OPER';
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
--  Деталировка к счету для ЦСС
PROCEDURE DETAIL_TRAFFIC_MGMN_CSS( 
               p_recordset    OUT t_refc, 
               p_period_id    IN INTEGER,
               p_bill_id      IN INTEGER,
               p_direction    IN VARCHAR2
)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'DETAIL_TRAFFIC_MGMN_CSS';
    v_retcode       INTEGER;
    v_date_from     DATE;
    v_date_to       DATE;
BEGIN
    v_date_from := TO_DATE (p_period_id, 'YYYYMM');
    v_date_to   := LAST_DAY (TO_DATE (p_period_id, 'YYYYMM')) + INTERVAL '00 23:59:59' DAY TO SECOND;

    IF p_direction IS NOT NULL THEN
      OPEN p_recordset FOR
            SELECT 
                b.bill_id,    
                b.service_id,
                S.SERVICE,
                B.ORDER_NO,
                B.ABN_A,
                B.ABN_B,    
                B.TERM_Z_NAME,
                TRUNC(b.local_time) local_date,
                v_date_from DATE_FROM,
                v_date_to DATE_TO,
                COUNT(*)            CALL_COUNT,
                SUM(BILL_MINUTES)   BILL_MINUTES,
                SUM(AMOUNT)         AMOUNT    
              FROM 
                bdr_voice_t b,
                service_t s,
                order_phones_t op
             WHERE 
                b.bill_id = p_bill_id
                AND b.service_id = s.service_id  
                AND b.bdr_status = 0
                AND b.rep_period between v_date_from AND v_date_to
                AND b.order_id = op.order_id
                AND b.abn_a = OP.PHONE_NUMBER
                and OP.DESCR = p_direction
            GROUP BY 
                b.bill_id,    
                b.service_id,
                S.SERVICE,
                B.ORDER_NO,
                B.ABN_A,
                B.ABN_B,    
                B.TERM_Z_NAME,
                TRUNC(b.local_time),
                v_date_from,
                v_date_to
            ORDER BY 
                B.SERVICE_ID,
                B.ORDER_NO,
                B.ABN_A,
                TRUNC(b.local_time) ; 
    ELSE
          OPEN p_recordset FOR
            SELECT 
                b.bill_id,    
                b.service_id,
                S.SERVICE,
                B.ORDER_NO,
                B.ABN_A,
                B.ABN_B,    
                B.TERM_Z_NAME,
                TRUNC(b.local_time) local_date,
                v_date_from DATE_FROM,
                v_date_to DATE_TO,
                COUNT(*)            CALL_COUNT,
                SUM(BILL_MINUTES)   BILL_MINUTES,
                SUM(AMOUNT)         AMOUNT    
              FROM 
                bdr_voice_t b,
                service_t s
             WHERE 
                b.bill_id = p_bill_id
                AND b.bdr_status = 0                
                AND b.service_id = s.service_id  
                AND b.rep_period between v_date_from AND v_date_to
            GROUP BY 
                b.bill_id,    
                b.service_id,
                S.SERVICE,
                B.ORDER_NO,
                B.ABN_A,
                B.ABN_B,    
                B.TERM_Z_NAME,
                TRUNC(b.local_time),
                v_date_from,
                v_date_to
            ORDER BY 
                B.SERVICE_ID,
                B.ORDER_NO,
                B.ABN_A,
                TRUNC(b.local_time) ;  
    END IF;    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        Pk01_Syslog.Raise_exception('msg_id='||v_retcode,c_PkgName||'.'||v_prcName);
END;

---------------------------------------------------------------------------------
--=======================================================================
-- Преобразование адреса в строку
--=======================================================================
function ADDRESS_TO_STRING(
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
       IF v_result IS NOT NULL AND LENGTH(TRIM(v_result)) > 0 THEN
          v_result := v_result || ', ';       
       END IF;    
       v_result := v_result || p_state;
    END IF;
    
    IF p_city IS NOT NULL THEN
       IF v_result IS NOT NULL AND LENGTH(TRIM(v_result)) > 0 THEN
          v_result := v_result || ', ';       
       END IF;    
       v_result := v_result || p_city;
    END IF;
    
    IF p_address IS NOT NULL THEN
       IF v_result IS NOT NULL AND LENGTH(TRIM(v_result)) > 0 THEN
          v_result := v_result || ', ';       
       END IF;    
       v_result := v_result || p_address;
    END IF;
    
    return v_result;
end address_to_string;

--====================================================================================================
-- ================================== MANUAL FUNCTION
-- ------------------------------------------------------------------------------- --
-- получить данные для заполнения заголока документов:
-- счета, счета-фактуры, акта передачи-приема, детализации
--   - при ошибке выставляет исключение
PROCEDURE BILL_HEADER_EN( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,              -- ID периода счета
               p_bill_id       IN INTEGER DEFAULT NULL  -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BILL_HEADER';
    v_retcode    INTEGER;
    v_period_to  DATE;
BEGIN
    -- получаем верхнюю границу периода
    v_period_to := Pk04_Period.Period_to(p_rep_period_id);
    -- возвращаем курсор
    OPEN p_recordset FOR
        WITH ADDR_GRP AS    -- адрес грузополучателя(для счета) 
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON, CODE_REGION
                 FROM ACCOUNT_CONTACT_T ac 
                 WHERE AC.ADDRESS_TYPE =  'GRP'
                   AND DATE_FROM<= v_period_to AND (DATE_TO IS NULL OR TRUNC(v_period_to) <= TRUNC(DATE_TO))
                 ), 
             ADDR_GRP_ACT AS    -- адрес грузополучателя(для акта) 
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON, CODE_REGION
                 FROM ACCOUNT_CONTACT_T ac 
                 WHERE AC.ADDRESS_TYPE =  'GRP'
                   AND DATE_FROM<= v_period_to AND (DATE_TO IS NULL OR TRUNC(v_period_to) <= TRUNC(DATE_TO))
                 ),    
             ADDR_JUR AS    -- юр. адрес 
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON, CODE_REGION
                 FROM ACCOUNT_CONTACT_T  
                 WHERE ADDRESS_TYPE = 'JUR'
                   AND DATE_FROM<= v_period_to AND (DATE_TO IS NULL OR TRUNC(v_period_to) <= TRUNC(DATE_TO))
                 ),
             ADDR_DLV AS    -- адрес доставки
             (SELECT CONTACT_ID, ACCOUNT_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON, CODE_REGION
                 FROM ACCOUNT_CONTACT_T  
                 WHERE ADDRESS_TYPE =  'DLV'
                   AND DATE_FROM<= v_period_to 
                   AND (DATE_TO IS NULL OR TRUNC(v_period_to) <= TRUNC(DATE_TO))
                 ),    
             ADDR_CTR AS    -- адрес получателя (исполнителя)
             (SELECT CONTRACTOR_ID, COUNTRY, ZIP, STATE, CITY, ADDRESS, PHONE_ACCOUNT, PHONE_BILLING, FAX,CODE_REGION
                 FROM CONTRACTOR_ADDRESS_T  
                 WHERE ADDRESS_TYPE = 'ENG'
                   AND DATE_FROM < = v_period_to 
                   AND (DATE_TO IS NULL OR TRUNC(v_period_to) <= TRUNC(DATE_TO))
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
                   WHERE S.SIGNER_ROLE_ID = 6101
                     AND S.DATE_FROM<= v_period_to 
                     AND (S.DATE_TO IS NULL OR v_period_to <= S.DATE_TO)
                     AND S.MANAGER_ID = M.MANAGER_ID
                     AND LANGUAGE = 'EN'
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
                   WHERE S.SIGNER_ROLE_ID =  6102 
                     AND S.DATE_FROM<= v_period_to 
                     AND (S.DATE_TO IS NULL OR v_period_to <= S.DATE_TO)
                     AND S.MANAGER_ID = M.MANAGER_ID
                     AND LANGUAGE = 'EN'
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
               B.CURRENCY_ID,               -- ID валюты счета
               B.ACT_DATE_FROM,             -- диапазон оказания услуг
               B.ACT_DATE_TO,               -- для акта передачи приема
               -- ПОЛУЧАТЕЛЬ/ПРОДАВЕЦ (поставщик услуг) -----------------------
               -- координаты получателя (поставщика)  - - - - - - - - - - - - -
               CR.CONTRACTOR_ID,            -- ID получателя
               NVL(C_HIST.CONTRACTOR_EN, CR.CONTRACTOR_EN) CONTRACTOR_NAME,               -- поставщик услуг
               NVL(C_HIST.SHORT_NAME_EN, CR.SHORT_NAME_EN) CONTRACTOR_NAME_SHORT, -- краткое наименование 
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
               ADDR_CTR.CODE_REGION,
               -- адрес получателя (головного офиса) - - - - - - - - - - - - - - - - - - - - - -
               ADDR_CTR_PARENT.COUNTRY PARENT_COUNTRY, 
               ADDR_CTR_PARENT.ZIP PARENT_ZIP,
               ADDR_CTR_PARENT.STATE PARENT_STATE,              -- регион (область, край,...)
               ADDR_CTR_PARENT.CITY PARENT_CITY,               -- город
               ADDR_CTR_PARENT.ADDRESS PARENT_ADDRESS,            -- строка адреса
               NULL PARENT_PERSON,                 -- загрушка (обязательно поле для получения адреса)...
               ADDR_CTR_PARENT.CODE_REGION PARENT_CODE_REGION,
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
               CB.BANK_SWIFT,
               CB.BANK_ADDRESS,
               -- банк получателя (может быть филиалом) КОРРЕСПОНДЕНТ
               CB_CORR.BANK_ID CORR_BANK_ID,                                            -- ID банка
               CB_CORR.BANK_NAME CORR_BANK_NAME,                     -- банк получателя
               CB_CORR.BANK_CODE CORR_BANK_CODE,                     -- БИК
               CB_CORR.BANK_CORR_ACCOUNT CORR_BANK_CORR_ACCOUNT,     -- корр.счет
               CB_CORR.BANK_SETTLEMENT CORR_BANK_SETTLEMENT,         -- расчетный счет покупателя
               CB_CORR.BANK_SWIFT CORR_BANK_SWIFT,
               CB_CORR.BANK_ADDRESS CORR_BANK_ADDRESS,
               -- Если получатель - филиал (то это инфа его головного офиса
               CR_PARENT.CONTRACTOR_ID CONTRACTOR_PARENT_ID,            -- ID получателя
               NVL(C_HIST_PARENT.CONTRACTOR, CR_PARENT.CONTRACTOR) CONTRACTOR_PARENT_NAME,               -- поставщик услуг
               NVL(C_HIST_PARENT.SHORT_NAME, CR_PARENT.SHORT_NAME) CONTRACTOR_PARENT_NAME_SHORT, -- краткое наименование 
               CR_PARENT.INN CONTRACTOR_PARENT_INN,
               CR_PARENT.KPP CONTRACTOR_PARENT_KPP,
               --
               -- ПЛАТЕЛЬЩИК/ПОКУПАТЕЛЬ ---------------------------------------
               -- координаты покупателя - - - - - - - - - - - - - - - - - - - -
               CM.COMPANY_ID            COMPANY_ID,
               CM.COMPANY_NAME          COMPANY_NAME,      -- название компании покупателя
               CM.SHORT_NAME            COMPANY_NAME_SHORT,  -- краткое название компании покупателя
               CM.INN                   COMPANY_INN,
               AP.KPP                   COMPANY_KPP,
               CM.DATE_FROM             COMPANY_DATE_FROM,
               CM.DATE_TO               COMPANY_DATE_TO,
               PK112_PRINT.BILL_HEADER_EXT_PARAM_FUNC(p_bill_id, p_rep_period_id, 'p_customer','BILL')    COMPANY_NAME_BILL,
               PK112_PRINT.BILL_HEADER_EXT_PARAM_FUNC(p_bill_id, p_rep_period_id, 'p_customer','ACT')     COMPANY_NAME_ACT,
               PK112_PRINT.BILL_HEADER_EXT_PARAM_FUNC(p_bill_id, p_rep_period_id, 'p_customer','FACTURA') COMPANY_NAME_FACTURA,                     
               -- Юридический адрес плательщика
               ADDR_JUR.COUNTRY  JUR_COUNTRY, 
               ADDR_JUR.ZIP      JUR_ZIP, 
               ADDR_JUR.STATE    JUR_STATE, 
               ADDR_JUR.CITY     JUR_CITY, 
               ADDR_JUR.ADDRESS  JUR_ADDRESS,
               ADDR_JUR.PERSON   JUR_PERSON,
               ADDR_JUR.CODE_REGION JUR_CODE_REGION,
               -- Адрес грузополучателя
               ADDR_GRP.COUNTRY  GRP_COUNTRY, 
               ADDR_GRP.ZIP      GRP_ZIP, 
               ADDR_GRP.STATE    GRP_STATE, 
               ADDR_GRP.CITY     GRP_CITY, 
               ADDR_GRP.ADDRESS  GRP_ADDRESS,
               ADDR_GRP.PERSON   GRP_PERSON,
               ADDR_GRP.CODE_REGION GRP_CODE_REGION,
               -- Адрес грузополучателя (акт)
               ADDR_GRP_ACT.PERSON GRP_ACT_PERSON,
               -- Почтовый адрес плательщика
               ADDR_DLV.COUNTRY  DLV_COUNTRY, 
               ADDR_DLV.ZIP      DLV_ZIP, 
               ADDR_DLV.STATE    DLV_STATE, 
               ADDR_DLV.CITY     DLV_CITY, 
               ADDR_DLV.ADDRESS  DLV_ADDRESS,
               ADDR_DLV.PERSON   DLV_PERSON,
               ADDR_DLV.CODE_REGION DLV_CODE_REGION,
               A.BILLING_ID,
               'bill_report_en.jasper' JASPER_BILL,
               'bill_act_report_en.jasper' JASPER_AKT,
               NULL JASPER_FACTURA,
               'bill_detail_report_v2_en.jasper' JASPER_DETAIL,
               'pk112_print.bill_inv_items_en' FUNCTION_BILL,
               'pk112_print.bill_inv_items_en' FUNCTION_AKT,
               'pk112_print.bill_inv_items_en' FUNCTION_FACTURA,
               'pk112_print.DETAIL_ITEMS_EN' FUNCTION_DETAIL,
               NVL(AD.IS_STAMP, 0) IS_STAMP,
               AD.DELIVERY_METHOD_ID,
               AD.DELIVERY_METHOD_NAME
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, CONTRACTOR_T CR,  
               COMPANY_T CM,
               BILL_T B, 
               CONTRACTOR_BANK_T CB, CONTRACTOR_BANK_T CB_CORR, 
               ADDR_GRP, ADDR_GRP_ACT, ADDR_JUR, ADDR_DLV, ADDR_CTR, ADDR_CTR ADDR_CTR_PARENT,
               SIGNER_R, SIGNER_B,STAMP,
               CONTRACTOR_T CR_PARENT,
               D_CL_TYPE,
               D_MARK_SEG,
               D_CONTR_TYPE,               
               PRINT_DOC_EXC,
               CONTRACTOR_HIST_T C_HIST,
               CONTRACTOR_HIST_T C_HIST_PARENT,
               (
                 SELECT account_id,
                         delivery_method_id,
                         d.name DELIVERY_METHOD_NAME,
                         CASE WHEN D.NAME LIKE '%штамп%' THEN 1 ELSE 0 END IS_STAMP
                    FROM account_documents_t ad, dictionary_t d
                   WHERE AD.DELIVERY_METHOD_ID = d.key_id AND doc_bill = 'Y'
               ) AD
         WHERE A.ACCOUNT_TYPE = 'J'
           AND A.ACCOUNT_ID = B.ACCOUNT_ID           
           AND A.ACCOUNT_ID = ADDR_GRP.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = ADDR_GRP_ACT.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = ADDR_JUR.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = ADDR_DLV.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID           
           AND B.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND B.CONTRACTOR_BANK_ID = CB.BANK_ID
           AND AP.CORRESPONDENT_BANK_ID = CB_CORR.BANK_ID     
           AND CB_CORR.DATE_FROM <= B.BILL_DATE AND (CB_CORR.DATE_TO IS NULL OR B.BILL_DATE <= CB_CORR.DATE_TO)      
           AND B.CONTRACT_ID = C.CONTRACT_ID
           AND C_HIST.CONTRACTOR_ID (+)= CR.CONTRACTOR_ID
           AND C_HIST_PARENT.CONTRACTOR_ID (+)= CR_PARENT.CONTRACTOR_ID
           AND B.BILL_DATE BETWEEN C_HIST.DATE_FROM AND NVL(C_HIST.DATE_TO, TO_DATE('01.01.2050','DD.MM.YYYY'))
           AND B.BILL_DATE BETWEEN C_HIST_PARENT.DATE_FROM AND NVL(C_HIST_PARENT.DATE_TO, TO_DATE('01.01.2050','DD.MM.YYYY'))
           AND AP.PROFILE_ID = B.PROFILE_ID
           AND B.BILL_STATUS IN ('READY','CLOSED','OPEN', 'CHECK','PREPAID')
           AND NVL(CR.PARENT_ID, 1) = CR_PARENT.CONTRACTOR_ID            
           AND CR_PARENT.CONTRACTOR_ID = ADDR_CTR_PARENT.CONTRACTOR_ID
           AND CR.CONTRACTOR_ID = ADDR_CTR.CONTRACTOR_ID 
           AND AP.CONTRACT_ID   = CM.CONTRACT_ID (+)
           AND TRUNC(B.BILL_DATE) BETWEEN CM.DATE_FROM AND NVL(CM.DATE_TO, TO_DATE('01.01.2050','DD.MM.YYYY'))
           AND CB.DATE_FROM <= B.BILL_DATE AND (CB.DATE_TO IS NULL OR B.BILL_DATE <= CB.DATE_TO)
           --AND AP.BRANCH_ID = SIGNER_R.CONTRACTOR_ID(+)
           --AND AP.BRANCH_ID = SIGNER_B.CONTRACTOR_ID(+)
           AND AP.BRANCH_ID = STAMP.contractor_id(+)
           AND D_CL_TYPE.KEY_ID (+)= C.CLIENT_TYPE_ID
           AND D_MARK_SEG.KEY_ID (+)= C.MARKET_SEGMENT_ID 
           AND D_CONTR_TYPE.KEY_ID (+)=C.CONTRACT_TYPE_ID
           and a.account_Id = PRINT_DOC_EXC.bill_account_id(+)
           AND a.account_id = ad.account_id(+)
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

-- ------------------------------------------------------------------------------- --
-- Строки счета-фактуры
--   - при ошибке выставляет исключение
PROCEDURE BILL_INV_ITEMS_EN( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BILL_INV_ITEMS';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
     SELECT II.BILL_ID, II.REP_PERIOD_ID, II.INV_ITEM_ID, II.INV_ITEM_NO, II.SERVICE_ID, 
            NVL(S.SERVICE_ENGLISH, II.INV_ITEM_NAME) NAME, -- Наименование
            II.TOTAL         ITEM_TOTAL,           -- Стоимость с налогом
            II.GROSS         ITEM_NETTO,           -- Стоимость без налога
            II.TAX           ITEM_TAX,             -- Сумма налога
            II.VAT           TAX  ,                -- Налоговая ставка
            II.DATE_FROM     USAGE_START,          -- Диапазон дат 
            II.DATE_TO       USAGE_END             -- оказания услуги
       FROM 
            INVOICE_ITEM_T II, 
            SERVICE_T S,
            BILL_T B
      WHERE 
            II.BILL_ID           = B.BILL_ID
            AND S.SERVICE_ID = II.SERVICE_ID
            AND II.REP_PERIOD_ID = p_rep_period_id 
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
PROCEDURE DETAIL_ITEMS_EN ( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DETAIL_ITEMS';
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
               CASE WHEN I.NOTES like '%час.%' OR I.NOTES like '%мин.%' THEN    
                    '(' || REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(I.NOTES,'мин',''),' ',''),'.',''),')',''),'(',''),'час',''),'Простои','')
                    ELSE I.NOTES 
                END ||
                CASE 
                    WHEN I.NOTES like '%час%' THEN ' Hour(s))' 
                    WHEN I.NOTES like '%мин%' THEN ' Minute(s))'            
               END NOTES,
               B.CURRENCY_ID BILL_CURRENCY_ID, 
               I.ITEM_CURRENCY_ID,
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
                  NVL(S.SERVICE_ENGLISH, S.SERVICE) SERVICE,
                  S.SERVICE_CODE_PRINTFORM,
                  NVL(SS.SUBSERVICE_ENGLISH,SS.SUBSERVICE) SUBSERVICE,
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
              ORDER BY S.service_id,  HAS_DTL, O.ORDER_NO,LST.DATE_FROM, DECODE(LST.CHARGE_TYPE,'REC',0,'MIN',1,'USG',3,'IDL',5,'ONT',10,'DIS',20),SS.subservice_id
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

--=======================
-- Исключение для у.е. лицевых счетов с конвертацией на дату оплаты
PROCEDURE BILL_INV_ITEMS_CU( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BILL_INV_ITEMS_CU';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
     SELECT II.BILL_ID, II.REP_PERIOD_ID, II.INV_ITEM_ID, II.INV_ITEM_NO, II.SERVICE_ID, 
            NVL((SELECT SRV_NAME FROM SERVICE_ALIAS_T WHERE SERVICE_ID = II.SERVICE_ID AND ACCOUNT_ID = B.ACCOUNT_ID) , II.INV_ITEM_NAME) NAME, -- Наименование
            NVL(IICU.TOTAL, II.TOTAL)         ITEM_TOTAL,           -- Стоимость с налогом
            NVL(IICU.GROSS, II.GROSS)         ITEM_NETTO,           -- Стоимость без налога
            NVL(IICU.TAX, II.TAX)           ITEM_TAX,             -- Сумма налога
            II.VAT           TAX  ,                -- Налоговая ставка
            II.DATE_FROM     USAGE_START,          -- Диапазон дат 
            II.DATE_TO       USAGE_END
       FROM 
            INVOICE_ITEM_T II, 
            (
              select * from 
                INVOICE_ITEM_CU_T
              WHERE CURRENCY_ID = 810
            ) IICU,
            BILL_T B
      WHERE 
            II.BILL_ID           = B.BILL_ID
            AND II.INV_ITEM_ID   = IICU.INV_ITEM_ID (+)      
            AND II.REP_PERIOD_ID = p_rep_period_id 
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

--====================================================================================

-- ------------------------------------------------------------------------------- --
--  Список услуг и их компонентов услуги к счету (Кастом для детализации для Альфа-Банка)
--   - при ошибке выставляет исключение
PROCEDURE DETAIL_ITEMS_ALFABANK ( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,   -- ID периода счета
               p_bill_id       IN INTEGER    -- ID счета
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DETAIL_ITEMS_ALFABANK';
    v_retcode    INTEGER;
BEGIN
    -- построить курсор
    OPEN p_recordset FOR
      WITH LST AS (
          SELECT 
               I.BILL_ID, 
               I.ITEM_ID, 
               I.ITEM_TYPE,
               I.CHARGE_TYPE,
               I.ORDER_ID,
               I.ORDER_BODY_ID, 
               OB.RATE_RULE_ID,
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
                   GROUP BY I.ITEM_ID, I.ITEM_TYPE, I.CHARGE_TYPE, I.BILL_ID, I.ORDER_ID, I.ORDER_BODY_ID, OB.RATE_RULE_ID, I.SERVICE_ID, I.SUBSERVICE_ID, I.DATE_FROM, I.DATE_TO, I.DESCR, I.NOTES, B.CURRENCY_ID, I.ITEM_CURRENCY_ID
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
                     I.BILL_ID, I.ORDER_ID,'Y' HAS_DTL 
                   FROM ITEM_T I, SERVICE_SUBSERVICE_T SS, ORDER_BODY_T OB, PRINT_DOCUMENTS_RATE_RULE PD
               WHERE 
                   I.SERVICE_ID = SS.SERVICE_ID (+)
                   AND I.ORDER_BODY_ID = OB.ORDER_BODY_ID
                   AND OB.RATE_RULE_ID = PD.RATE_RULE_ID (+)
                   AND I.SUBSERVICE_ID = SS.SUBSERVICE_ID (+)
                   AND I.REP_PERIOD_ID = p_rep_period_id
                   AND I.BILL_ID = p_bill_id
                   AND I.CHARGE_TYPE = 'USG'                                    
                   AND (NVL(PD.DTL_KEY, SS.DTL_KEY) IS NULL OR NVL(PD.DTL_KEY, SS.DTL_KEY) NOT IN ('TRAFFIC_IP_BURST'))  
               GROUP BY I.BILL_ID, I.ORDER_ID 
           ),
           ITEM_DIS AS (
               SELECT 
                     ORDER_ID, 
                     CASE WHEN NOTES IS NOT NULL THEN TRIM(REPLACE(NOTES,'%','')) 
                     ELSE NOTES END DIS_NOTES,
                     ABS(SUM(ITEM_TOTAL)) DIS_TOTAL 
                   FROM ITEM_T I
               WHERE 
                   I.BILL_ID = p_bill_id
                   AND I.CHARGE_TYPE = 'DIS'
                   AND ITEM_TYPE <> 'A'
               GROUP BY ORDER_ID, NOTES                                                                         
           )
        SELECT 
                  LST.BILL_ID,
                  LST.ITEM_ID,                  
                  LST.ORDER_ID,                   
                  CASE
                    WHEN OI.ORDER_NO_ALIAS IS NOT NULL THEN OI.ORDER_NO_ALIAS
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
                  LST.RATE_RULE_ID,
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
                  CASE 
                       WHEN LST.ITEM_TYPE = 'A' THEN SS.SUBSERVICE || ' ('|| LST.DESCR || ')' 
                       ELSE SS.SUBSERVICE 
                  END SUBSERVICE,
                  LST.DESCR ORDER_DESCR,       
                  LST.CHARGE_TYPE ITEM_CHARGE_TYPE,
                  NVL(PDRR.DTL_KEY, SDT.DTL_KEY)  ITEM_ALIAS,
                  NVL(ORD_HAS_DTL.HAS_DTL,'N') HAS_DTL,
                  OI.POINT_SRC,
                  OI.POINT_DST,                  
                  CASE  WHEN OI.SPEED_UNIT_ID = 6702 THEN SPEED_VALUE / 1024
                        WHEN OI.SPEED_UNIT_ID = 6700 THEN SPEED_VALUE * 1024
                  ELSE OI.SPEED_VALUE END SPEED_VALUE_MB,
                  ITEM_DIS.DIS_NOTES,
                  CASE 
                    WHEN LST.ITEM_TYPE = 'A' THEN NULL
                    WHEN ORD_HAS_DTL.HAS_DTL <> 'Y' OR ORD_HAS_DTL.HAS_DTL IS NULL THEN ITEM_DIS.DIS_TOTAL
                    WHEN ISNUMBER(ITEM_DIS.DIS_NOTES) <> -1 THEN LST.BILL_TOTAL * ISNUMBER(ITEM_DIS.DIS_NOTES)/100
                  END DIS_TOTAL 
              FROM 
                  LST, ORD_MIN, ORD_ABON, ORD_HAS_DTL, ORDER_T O, ORDER_INFO_T oi, SERVICE_T S, SUBSERVICE_T SS, SERVICE_SUBSERVICE_T SDT, ACCOUNT_T A, PRINT_DOCUMENTS_RATE_RULE PDRR, ITEM_DIS
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
                  AND LST.RATE_RULE_ID = PDRR.RATE_RULE_ID (+)
                  AND O.ORDER_ID = LST.ORDER_ID
                  AND O.ORDER_ID = OI.ORDER_ID (+)
                  AND O.ACCOUNT_ID = A.ACCOUNT_ID     
                  AND LST.ORDER_ID = ITEM_DIS.ORDER_ID (+)
                  AND (LST.CHARGE_TYPE <> 'DIS' OR (LST.CHARGE_TYPE = 'DIS' AND LST.ITEM_TYPE = 'A'))               
              ORDER BY S.service_id,  HAS_DTL, O.ORDER_NO,LST.DATE_FROM, DECODE(LST.CHARGE_TYPE,'REC',0,'MIN',1,'USG',3,'IDL',5,'ONT',10,'DIS',20),SS.subservice_id
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

------------------------------------------------------------------------
-- Работа с нестандартными формами счета
------------------------------------------------------------------------
PROCEDURE GetPrintDocumentsExcl(p_recordset OUT t_refc, p_account_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetPrintDocumentsExcl';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      SELECT DISTINCT ACCOUNT_ID, CONTRACT_ID, JASPER_BILL, JASPER_AKT, 
             JASPER_FACTURA, JASPER_DETAIL, FUNCTION_BILL, FUNCTION_AKT, 
             FUNCTION_FACTURA, FUNCTION_DETAIL, HEADER_AKT, FUNCTION_BILL_HEADER, 
             DOC_LANG
  FROM PRINT_DOCUMENTS_EXCLUDE_T
 WHERE    account_id = p_account_id
       OR (contract_id = (SELECT contract_id
                            FROM account_profile_t
                           WHERE account_id = p_account_id AND actual = 'Y'));
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------------------------------------------------------------------------
-- Работа с нестандартными формами счета
------------------------------------------------------------------------
PROCEDURE SavePrintDocumentsExcl(
          p_account_id                                       IN INTEGER,
          p_contract_id                                      IN INTEGER,
          p_jasper_bill                                      IN VARCHAR2,
          p_jasper_akt                                       IN VARCHAR2, 
          p_jasper_factura                                   IN VARCHAR2, 
          p_jasper_detail                                    IN VARCHAR2, 
          p_function_bill                                    IN VARCHAR2, 
          p_function_akt                                     IN VARCHAR2, 
          p_function_factura                                 IN VARCHAR2, 
          p_function_detail                                  IN VARCHAR2, 
          p_header_akt                                       IN VARCHAR2, 
          p_function_bill_header                             IN VARCHAR2, 
          p_doc_lang                                         IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'SavePrintDocumentsExcl';
    v_retcode       INTEGER;
    v_exists        INTEGER DEFAULT 0;   
BEGIN
    -- сначала все удалим
    IF p_account_id > 0 THEN
        DELETE FROM PRINT_DOCUMENTS_EXCLUDE_T
               WHERE ACCOUNT_ID = p_account_id
               OR CONTRACT_ID = (
                   select contract_id from account_profile_t
                      where account_id = p_account_id
                      and actual = 'Y'
               );
    ELSE
        DELETE FROM PRINT_DOCUMENTS_EXCLUDE_T
              WHERE CONTRACT_ID = p_contract_id
              OR ACCOUNT_ID IN (
                select account_id from account_profile_t
                  where contract_id = p_contract_id
                  and actual = 'Y'
              );
    END IF;
    -- если хоть что-то заполнено, тогда пишем
    IF  p_jasper_bill is not null 
      OR p_jasper_akt is not null 
      OR p_jasper_factura is not null 
      OR p_jasper_detail is not null 
      OR p_function_bill is not null 
      OR p_function_akt is not null 
      OR p_function_factura is not null 
      OR p_function_detail is not null 
      OR p_header_akt is not null 
      OR p_function_bill_header  is not null 
      OR p_doc_lang is not null THEN
    
        IF p_account_id > 0 THEN
            INSERT INTO PRINT_DOCUMENTS_EXCLUDE_T (ACCOUNT_ID, JASPER_BILL, JASPER_AKT, JASPER_FACTURA, JASPER_DETAIL, 
                                                   FUNCTION_BILL, FUNCTION_AKT, FUNCTION_FACTURA, FUNCTION_DETAIL, HEADER_AKT, 
                                                   FUNCTION_BILL_HEADER, DOC_LANG)
                               VALUES(p_account_id, p_jasper_bill, p_jasper_akt, p_jasper_factura, p_jasper_detail, p_function_bill, 
                                                    p_function_akt, p_function_factura, p_function_detail, p_header_akt, 
                                                    p_function_bill_header, p_doc_lang);
          
        ELSIF p_contract_id > 0 THEN
            INSERT INTO PRINT_DOCUMENTS_EXCLUDE_T (CONTRACT_ID, JASPER_BILL, JASPER_AKT, JASPER_FACTURA, JASPER_DETAIL, 
                                           FUNCTION_BILL, FUNCTION_AKT, FUNCTION_FACTURA, FUNCTION_DETAIL, HEADER_AKT, 
                                           FUNCTION_BILL_HEADER, DOC_LANG)
                       VALUES(p_contract_id, p_jasper_bill, p_jasper_akt, p_jasper_factura, p_jasper_detail, p_function_bill, 
                                             p_function_akt, p_function_factura, p_function_detail, p_header_akt, p_function_bill_header,
                                             p_doc_lang);
          END IF;       
     END IF;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

--------------------------------------------------------------------------------------
-- Работа с нестандартными параметрами счета
-------------------------------------------------------------------------------------- 
PROCEDURE GetPrintDocumentsParams(p_recordset OUT t_refc, p_account_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetPrintDocumentsParams';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
     SELECT P.BILL_ID,
       P.ACCOUNT_ID,
       P.CONTRACT_ID,
       P.CONTRACTOR_ID,
       P.PARAM_NAME,
       P.PARAM_VALUE,
       P.TYPE_DOC,
       P.NOTES, PARAM_ID,
       PD.PARAM_NOTES
  FROM PRINT_DOCUMENTS_EXT_PARAM p, PRINT_DOC_EXT_PARAM_DICT pd
 WHERE P.PARAM_NAME = PD.PARAM_NAME(+) 
       AND (P.account_id = p_account_id
       OR bill_id IN (SELECT bill_id
                        FROM bill_t
                       WHERE account_id = p_account_id)
       OR contract_id = (SELECT contract_id
                           FROM account_profile_t
                          WHERE account_id = p_account_id AND actual = 'Y'));
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

---------
FUNCTION CreateDocExtParam(     p_bill_id                    IN INTEGER,
                                p_account_id                 IN INTEGER,
                                p_contract_id                IN INTEGER,                                                                
                                p_param_name                 IN VARCHAR2,
                                p_param_value                IN VARCHAR2,
                                p_type_doc                   IN VARCHAR2,
                                p_notes                      IN VARCHAR2) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'CreateDocExtParams';
    v_retcode       INTEGER;   
    v_param_id      INTEGER;
BEGIN
    INSERT INTO PRINT_DOCUMENTS_EXT_PARAM(BILL_ID, ACCOUNT_ID, CONTRACT_ID, CONTRACTOR_ID, PARAM_NAME, PARAM_VALUE, TYPE_DOC, NOTES, PARAM_ID) 
                                   VALUES(p_bill_id, p_account_id, p_contract_id, NULL, p_param_name, p_param_value, p_type_doc, p_notes, SQ_PRINT_EXT_PARAM_ID.NEXTVAL)
                                   RETURNING PARAM_ID INTO v_param_id;
                                   RETURN v_param_id;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
-----------------------------------------
PROCEDURE UpdateDocExtParam(    p_param_id                   IN INTEGER,
                                p_bill_id                    IN INTEGER,
                                p_account_id                 IN INTEGER,
                                p_contract_id                IN INTEGER,                                                                
                                p_param_name                 IN VARCHAR2,
                                p_param_value                IN VARCHAR2,
                                p_type_doc                   IN VARCHAR2,
                                p_notes                      IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'UpdateDocExtParam';
    v_retcode       INTEGER;   
BEGIN
    UPDATE PRINT_DOCUMENTS_EXT_PARAM
      SET BILL_ID = p_bill_id, 
      ACCOUNT_ID = p_account_id, 
      CONTRACT_ID = p_contract_id, 
      CONTRACTOR_ID = NULL, 
      PARAM_NAME = p_param_name, 
      PARAM_VALUE = p_param_value, 
      TYPE_DOC = p_type_doc, 
      NOTES = p_notes 
    WHERE PARAM_ID = p_param_id;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
-------
PROCEDURE GetExtParamById(p_recordset OUT t_refc, p_param_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetExtParamById';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
     SELECT BILL_ID,
       ACCOUNT_ID,
       CONTRACT_ID,
       CONTRACTOR_ID,
       PARAM_NAME,
       PARAM_VALUE,
       TYPE_DOC,
       NOTES, PARAM_ID
  FROM PRINT_DOCUMENTS_EXT_PARAM
 WHERE    PARAM_ID = p_param_id;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

----------------------------------------------------------------
-- Получить список всех нестандартных параметров
----------------------------------------------------------------
PROCEDURE GetExtParamList(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetExtParamList';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
     SELECT
         PARAM_NAME,
         PARAM_NOTES
       FROM PRINT_DOC_EXT_PARAM_DICT
       ORDER BY PARAM_NAME;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK112_PRINT;
/
