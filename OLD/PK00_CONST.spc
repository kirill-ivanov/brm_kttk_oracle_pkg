CREATE OR REPLACE PACKAGE PK00_CONST
IS
    -- Пакет для описания констант
    -- ==============================================================================
    c_PkgName   CONSTANT varchar2(30) := 'PK00_CONST';
    -- ==============================================================================
    c_RET_OK    CONSTANT integer := 0;
    c_RET_ER    CONSTANT integer :=-1;

    --=============================================================================--
    -- Словарь DICTIONARY_T
    --=============================================================================--
    -- --------------------------------------------------------------------------- --
    -- Лицевые счета (ACCOUNT_T)
    -- --------------------------------------------------------------------------- --
    c_ACC_TYPE_P CONSTANT char(1) := 'P';          -- физические лица
    c_ACC_TYPE_J CONSTANT char(1) := 'J';          -- юридические лица

    c_ACC_STATUS_BILL CONSTANT varchar2(10):= 'B'; -- статус - биллингуемый (активен)
    c_ACC_STATUS_TEST CONSTANT varchar2(10):= 'T'; -- статус - тестовый (не биллингуемый)

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Типы счетов:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_BILL_TYPE_ONT CONSTANT char(1) := 'M';       -- разовый счет за услуги
    c_BILL_TYPE_REC CONSTANT char(1) := 'B';       -- ежемесячный счет за услуги
    c_BILL_TYPE_CRD CONSTANT char(1) := 'C';       -- кредит нота (сторнирующий счет)
    c_BILL_TYPE_DBT CONSTANT char(1) := 'D';       -- дебет нота (исправленный счет)

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- состояния счета:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- счет выставлет, период закрыт, изменения начислений невозможны, 
    -- корректировки только через формирование дебит/кредит нот
    -- разрешен, только прием платежей и работа с задолженностью
    c_BILL_STATE_CLOSED  CONSTANT varchar2(20) := 'CLOSED';
    c_ITEM_STATE_CLOSED  CONSTANT varchar2(20) := 'CLOSED';  
    -- счет готов, но отчетный период еще не закрыт, возможен пересчет начислений
    c_BILL_STATE_READY   CONSTANT varchar2(20) := 'READY';
    c_ITEM_STATE_REАDY   CONSTANT varchar2(20) := 'READY';
    -- счет пустой, но отчетный период еще не закрыт, возможен пересчет начислений
    c_BILL_STATE_EMPTY   CONSTANT varchar2(20) := 'EMPTY';
    c_ITEM_STATE_EMPTY   CONSTANT varchar2(20) := 'EMPTY';
    -- счет текущего - открыт для начислений и приема платежей 
    c_BILL_STATE_OPEN    CONSTANT varchar2(20) := 'OPEN';
    c_ITEM_STATE_OPEN    CONSTANT varchar2(20) := 'OPEN';
    -- ошибка при формировании счета
    c_BILL_STATE_ERROR   CONSTANT varchar2(20) := 'ERROR';
    c_ITEM_STATE_ERROR   CONSTANT varchar2(20) := 'ERROR';

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- cостояние платежа STATUS из таблицы PAYMENT_T:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_PAY_STATE_OPEN   CONSTANT varchar2(20) := 'OPEN';  -- не полностью распределен
    c_PAY_STATE_CLOSE  CONSTANT varchar2(20) := 'CLOSE'; -- платеж распределен
    c_PAY_STATE_ERROR  CONSTANT varchar2(20) := 'ERROR'; -- ошибка

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Типы позиций счета ITEM_TYPE из таблицы ITEM_T
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- позиции, которые не входят в выставленный счет, но нужны для работы с ним
    c_ITEM_TYPE_PAYMENT  CONSTANT char(1) := 'P';  -- платеж (PAYMENT)
    --c_ITEM_TYPE_TRANSFER CONSTANT char(1) := 'T';  -- позиция разноски платежа
    --c_ITEM_TYPE_ADVANCE  CONSTANT char(1) := 'A';  -- аванс от платежа предыдущего периода
    -- позиции, которые входят в выставленный счет
    c_ITEM_TYPE_BILL     CONSTANT char(1) := 'B';  -- позиция начислений за услугу
    c_ITEM_TYPE_ADJUST   CONSTANT char(1) := 'C';  -- позиция корретировки

    c_INV_ITEM_BALANCE   CONSTANT INTEGER := 0;    -- номер строки баланса в INV_ITEM_NO
    c_VAT                CONSTANT NUMBER  := 18;   -- ставка НДС 

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Типы начилений CHARGE_TYPE из таблиц ORDER_BODY_T, ITEM_T 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_CHARGE_TYPE_REC CONSTANT char(3) := 'REC';   -- предоставление услуги за абонплату
    c_CHARGE_TYPE_ONT CONSTANT char(3) := 'ONT';   -- разовое действие
    c_CHARGE_TYPE_USG CONSTANT char(3) := 'USG';   -- оплата трафика
    c_CHARGE_TYPE_MIN CONSTANT char(3) := 'MIN';   -- доплата до минимальной суммы
    c_CHARGE_TYPE_DIS CONSTANT char(3) := 'DIS';   -- скидки
    c_CHARGE_TYPE_IDL CONSTANT char(3) := 'IDL';   -- простои

    c_CHARGE_TYPE_IDL CONSTANT char(3) := 'PAY';   -- поступивший платеж
    c_CHARGE_TYPE_IDL CONSTANT char(3) := 'PAY';   -- поступивший платеж

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Типы адресов ADDRESS_TYPE из таблиц ACCOUNT_NAME_INFO_T, CUSTOMER_T, CONTRACTOR_T 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_ADDR_TYPE_JUR CONSTANT char(3) := 'JUR';     -- юридический адрес для юр. лиц
    c_ADDR_TYPE_REG CONSTANT char(3) := 'REG';     -- адрес регистрации для физ. лиц
    c_ADDR_TYPE_DLV CONSTANT char(3) := 'DLV';     -- адрес доставки
    c_ADDR_TYPE_GRP CONSTANT char(3) := 'GRP';     -- адрес грузополучателя
    c_ADDR_TYPE_SET CONSTANT char(3) := 'SET';     -- адрес установки оборудования

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Предбиллинги
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_RATESYS_MMTS_ID CONSTANT integer := 1201;    -- предбиллинг телефонии сети ММТС
    c_RATESYS_TOPS_ID CONSTANT integer := 1202;    -- предбиллинг местной операторской телефонии ТОПС 
    
    --=============================================================================--
    -- Типы контрагентов CONTRACTOR_T
    --=============================================================================--
    c_CTR_TYPE_KTTK  CONSTANT varchar(20) := 'КТТК';
    c_CTR_TYPE_XTTK  CONSTANT varchar(20) := 'XТТК';
    c_CTR_TYPE_ZTTK  CONSTANT varchar(20) := 'ZTTK';
    c_CTR_TYPE_BRAND CONSTANT varchar(20) := 'BRAND';
    c_CTR_TYPE_AGENT CONSTANT varchar(20) := 'AGENT';
    
    --=============================================================================--
    -- ID клиента физического лица CLIENT_T (клиент один на всех)
    --=============================================================================--
    c_CLIENT_PERSON_ID   CONSTANT integer := 1;
    
    --=============================================================================--
    -- ID покупателя для физического лица CUSTOMER_T (клиент один на всех)
    --=============================================================================--
    c_CUSTOMER_PERSON_ID CONSTANT integer := 1;    

    --=============================================================================--
    -- Справочник валют CURRENCY_T
    --=============================================================================--
    c_CURRENCY_RUB    CONSTANT integer := 810;  -- Российский рубль
    c_CURRENCY_USD    CONSTANT integer := 840;  -- Доллар США
    c_CURRENCY_EUR    CONSTANT integer := 978;  -- Евро
    c_CURRENCY_YE     CONSTANT integer := 36;   -- Условная единица Доллар
    c_CURRENCY_YEE    CONSTANT integer := 250;  -- Условная единица Евро
    c_CURRENCY_YE_FIX CONSTANT integer := 286;  -- Условная единица доллар по курсу 28,6

    --=============================================================================--
    -- Типы услуги из таблицы SERVICE_T
    --=============================================================================--
    -- ВНИМАНИЕ: ID услуг для тестов (реальные получим после заполнения продукт каталога)
    c_SERVICE_CALL_MGMN   CONSTANT integer := 1;   -- Услуги междугородной/международной телефонной связи
    c_SERVICE_CALL_FREE   CONSTANT integer := 2;   -- услуга вызов нв 8-800
    c_SERVICE_CALL_ZONE   CONSTANT integer := 3;   -- услуга зонового вызова
    c_SERVICE_CALL_LOCAL  CONSTANT integer := 4;   -- услуга местного вызова
    c_SERVICE_CALL_LZ     CONSTANT integer := 6;   -- услуга местная и зоновая связь
    c_SERVICE_OP_LOCAL    CONSTANT integer := 7;   -- Услуга присоединения на местном уровне
    
    --=============================================================================--
    -- Типы компонентов услуги из таблицы SUBSERVICE_T
    --=============================================================================--
    c_SUBSRV_MG  CONSTANT integer := 1;   -- Автоматическое междугородное телефонное соединение    
    c_SUBSRV_MN  CONSTANT integer := 2;   -- Автоматическое международное телефонное соединение
    c_SUBSRV_MIN CONSTANT integer := 3;   -- Доплата до мин. ежемесячной стоимости
    c_SUBSRV_DET CONSTANT integer := 4;   -- 'Позвонковая детализация'

    --=============================================================================--
    -- Типы событий из таблицы EVENT_TYPE_T
    --=============================================================================--
    c_EVENT_TYPE_CALL_LOCAL  CONSTANT integer := 1;  -- местный трафик
    c_EVENT_TYPE_CALL_ZONE   CONSTANT integer := 2;  -- зоновый трафик
    c_EVENT_TYPE_CALL_MG     CONSTANT integer := 3;  -- междугородний трафик
    c_EVENT_TYPE_CALL_MN     CONSTANT integer := 4;  -- международный трафик
    c_EVENT_TYPE_FREE_CALL   CONSTANT integer := 5;  -- вызов нв 8-800

    --=============================================================================--
    -- Ошибки привязки лицевых счетов
    --=============================================================================--
    c_ORDER_NOT_FOUND        CONSTANT integer := -1; -- не найден заказ
    c_ERR_SRV_NOT_FOUND      CONSTANT integer := -2; -- не найдена служба 
    c_NOT_ZONE_JOINT         CONSTANT integer := -3; -- стык не зоновый

    --=============================================================================--
    -- Типы BDR-ов
    --=============================================================================--
    c_BDR_Ph_A               CONSTANT integer := 1;
    
    --=============================================================================--
    -- Ошибки тарифкации
    --=============================================================================--
    c_ACC_NOT_FOUND          CONSTANT integer := -4; -- не л/счет по заказу
    c_NF_TAR_PLAN            CONSTANT integer := -5; -- не найден тарифный план 
    c_TARIFF_NOT_FOUND       CONSTANT integer := -6; -- не найден тарифный план в ЦБ 
    c_PRICE_NOT_FOUND        CONSTANT integer := -7; -- не найдены расценки для связки номеров А и Б
    

END PK00_CONST;
/
