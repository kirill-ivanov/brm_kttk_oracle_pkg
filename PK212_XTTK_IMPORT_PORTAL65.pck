CREATE OR REPLACE PACKAGE PK212_XTTK_IMPORT_PORTAL65
IS
    --
    -- Пакет для создания клиентов бренда xTTK «Санкт-Петербургский ТЕЛЕПОРТ»
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK212_XTTK_IMPORT_PORTAL65';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- =====================================================================
    -- биллинг в который загружаем клиентов, для проверки перед импортом
    c_BILLING_XTTK        CONSTANT INTEGER := 2008; -- Биллинг клиетнов блока Магистраль из ХТТК
    c_LOAD_CODE_START     CONSTANT INTEGER := 0;    -- загрузка
--    c_LOAD_CODE_PROGRESS  CONSTANT INTEGER := 1;    -- работа
--    c_LOAD_CODE_OK        CONSTANT INTEGER := 2;    -- ОК
    c_LOAD_CODE_ERR       CONSTANT INTEGER :=-1;    -- Ошибка
    c_LOAD_CODE_DBL       CONSTANT INTEGER :=-2;    -- Данные уже есть в BRM
    -- шаги выполнения программы
    c_LOAD_CODE_ACC       CONSTANT INTEGER := 1;    -- Создан л/с
    c_LOAD_CODE_CLN       CONSTANT INTEGER := 2;    -- Создан клиент
    c_LOAD_CODE_MGR       CONSTANT INTEGER := 3;    -- Создан sale-curator
    c_LOAD_CODE_CTR       CONSTANT INTEGER := 4;    -- Создан договор
    c_LOAD_CODE_CST       CONSTANT INTEGER := 5;    -- Создан контрагент-покупатель
    c_LOAD_CODE_APF       CONSTANT INTEGER := 6;    -- Создан профиль л/с
    c_LOAD_CODE_AJR       CONSTANT INTEGER := 7;    -- Создан адрес юридический
    c_LOAD_CODE_ADL       CONSTANT INTEGER := 8;    -- Создан адрес доставки
    c_LOAD_CODE_ORD       CONSTANT INTEGER := 9;    -- Создан заказ    
    c_LOAD_CODE_SAL       CONSTANT INTEGER :=10;    -- Создан service-alias
    c_LOAD_CODE_ABP       CONSTANT INTEGER :=11;    -- Создание компонена услуг - абонплата
    c_LOAD_CODE_USG       CONSTANT INTEGER :=12;    -- Создание компонена услуг - трафик
    c_LOAD_CODE_FIN       CONSTANT INTEGER :=13;    -- Финиш

    -- способ доставки АккордПост
    c_DLV_METHOD_AP CONSTANT INTEGER := 6512;   -- АккордПост
    
    -- описатель тарификатора
    с_RATESYS_ABP_ID CONSTANT INTEGER := 1207; -- стандартный расчет абонплаты, тариф в ORDER_BODY_T
    -- описатели тарифных планов
    c_RATEPLAN_NPL_RUR        CONSTANT INTEGER := 80043; -- 'NPL_RUR'
    c_RATEPLAN_NPL            CONSTANT INTEGER := 80045; -- 'NPL'
    c_RATEPLAN_RRW_RUR        CONSTANT INTEGER := 80046; -- 'RRW RUR'
    c_RATEPLAN_IP_ROUTING_RUR CONSTANT INTEGER := 80047; -- 'IP Routing RUR'
    
    c_LDSTAT_DBL_ORD CONSTANT INTEGER := -1;   -- заказ уже есть в BRM
    c_LDSTAT_NOT_SRV CONSTANT INTEGER := -2;   -- услуга не найдена
    
    c_MAX_DATE_TO    CONSTANT DATE := TO_DATE('01.01.2050','dd.mm.yyyy');

    --============================================================================================
    -- После полного тестирования переводим л/с из c_BILLING_NPL -> Pk00_Const.c_BILLING_OLD
    --============================================================================================
    PROCEDURE Change_billing_id;

    -- ----------------------------------------------------------------------------- --
    -- Импорт договоров старого биллинга, фильтр выставляем в теле процедуры
    PROCEDURE Import_contracts;
    
    -- ----------------------------------------------------------------------------- --
    -- Загрузка информации по заказам, для загруженных договоров
    PROCEDURE Import_orders;

    -- Импорт дополнительных данных из Portal 6.5
    PROCEDURE Import_add_data;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- проставляем CONTRACTOR_ID которые будем использовать для создания договора
    PROCEDURE Set_contractor_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- проставляем MANAGER_ID которые будем использовать для создания договора
    PROCEDURE Set_manager_id;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- проставляем BRM_CLIENT_ID которые будем использовать для создания договора
    PROCEDURE Set_client_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- проставляем BRM_CUSTOMER_ID которые будем использовать для создания договора
    PROCEDURE Set_customer_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- проставляем CONTRACT_ID - которые будем использовать для создания договора
    PROCEDURE Set_contract_id;

    -- проставляем дополнительную информацию по договору
    -- тип клиента, сегмент рынка, валюта
    PROCEDURE Set_contract_info;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- проставляем ACCOUNT_ID - которые будем использовать для создания договора
    PROCEDURE Set_account_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- проставляем ORDER_ID
    PROCEDURE Set_order_id;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- проставляем информацию для ORDER_INFO_T
    PROCEDURE Set_order_info;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Установить ID услуги
    PROCEDURE Set_service_id;
      
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Установить ID компонент услуги
    PROCEDURE Set_subservice_id;

    -- ----------------------------------------------------------------------------- --
    -- Экскорт данных из старого биллинга по бренду '.SPB TTK Brand'
    -- и услуге местного и зонового присоединения
    PROCEDURE Import_data;
    
    --============================================================================================
    -- Загрузка данных по лицевым счетам
    PROCEDURE Load_accounts;

    -- Загрузка данных по лицевым счетам
    PROCEDURE Load_orders;
        
    --============================================================================================
    -- Перенести загруженные данные в архив
    --============================================================================================
    PROCEDURE Move_to_archive;
    
    -- =========================================================== --
    -- Формирование счетов
    -- =========================================================== --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создание периодических счетов для клиентов имеющих
    -- абонплату или доплату до минимальной стоимости
    -- в биллинговом периоде p_period_id
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Create_new_bills(p_period_id IN INTEGER);
    
    -- ------------------------------------------------------------------------- --
    -- сформировать счета клиентов Юр.лиц регионов (РП блок Магистраль) 
    -- ------------------------------------------------------------------------- --
    PROCEDURE Make_bills( p_period_id IN INTEGER );
    
    -- ----------------------------------------------------------------------------- --
    -- описание строки лицевых счетов
    CURSOR c_CTR IS (
       SELECT 
          ROW_NUMBER() OVER (ORDER BY ACCOUNT_ID, CONTRACT_NO) RN,
          ACCOUNT_ID,
          CONTRACT_NO,
          ACCOUNT_NO,
          CLIENT_ID,
          KONTRAGENT,
          CLIENT,
          CUST_TYPE,
          CUSTDATE,
          JUR_ZIP,
          JUR_CITY,
          JUR_ADDRESS,
          PHIS_ZIP,
          PHIS_CITY,
          PHIS_ADDRESS,
          PHONE,
          FAX,
          EMAIL_ADDR,
          PHIS_NAME,
          INN,
          OKONH,
          OKPO,
          BANK,
          SETTLEMENT,
          CORR,
          BIC,
          KPP,
          COMPANY,
          CURRENCY,
          CURRENCY_SECONDARY,
          ORIGINAL,
          TAX_VAT,
          TAX_SALES,
          SALES_NAME,
          DIRECTORATE,
          MARKET_SEG,
          BILLING_CURATOR,
          GL_SEGMENT,
          IACCOUNT,
          AGENT_CODE,
          AGENT_NAME,
          PPTS_FLAG,
          DELIVERY,
          BRM_CONTRACT_NO,
          BRM_CONTRACT_ID,
          BRM_ACCOUNT_NO,
          BRM_ACCOUNT_ID,
          BRM_PROFILE_ID,
          BRM_CONTRACTOR_ID,
          BRM_CONTRACTOR_BANK_ID,
          BRM_BRANCH_ID,
          BRM_AGENT_ID,
          BRM_CLIENT_ID,
          BRM_CUSTOMER_ID,
          BRM_SALE_CURATOR_ID,
          BRM_BILLING_CURATOR_ID,
          BRM_DLV_ADDRESS_ID,
          BRM_JUR_ADDRESS_ID,
          BRM_BC_LASTNAME,
          BRM_BC_FIRSTNAME,
          BRM_BC_MIDDLENAME,
          BRM_SC_LASTNAME,
          BRM_SC_FIRSTNAME,
          BRM_SC_MIDDLENAME,
          BRM_MARKET_SEGMENT_ID,
          BRM_CLIENT_TYPE_ID,
          BRM_CURRENCY_ID,
          BRM_DELIVERY_METHOD_ID,
          IMPORT_DATE,
          LOAD_DATE,
          LOAD_CODE,
          LOAD_STATUS
         FROM PK212_PINDB_ALL_CONTRACTS_T 
        WHERE LOAD_CODE = 0
    )FOR UPDATE;
        
    -- описание строки заказов    
    CURSOR c_ORD IS (
       SELECT 
          ROW_NUMBER() OVER (ORDER BY ACCOUNT_POID, CONTRACT_NO) RN,
          --BRAND,
          ACCOUNT_POID,
          CONTRACT_NO,
          ACCOUNT_NO,
          --COMPANY,
          SERVICE_POID,
          ORDER_NO,
          STATUS,
          RATE_PLAN,
          PRODUCT_NAME,
          EVENT_TYPE,
          IP_USAGE_RATE_PLAN,
          CYCLE_FEE_AMT,
          CURRENCY,
          CURRENCY_SECONDARY,
          CYCLE_START_T,  -- order_t.date_from
          CYCLE_END_T,    -- order_t.date_to
          SMC_START_T,    -- должен быть текущий месяц, если раньше, то доначисления
          SMC_END_T,      -- всегда конец текущего месяца, 
          ORDER_DATE,     -- order_t.date_from - главная
          S_RGN,
          D_RGN,
          SERVICE_NAME,
          SPEED_STR,
          FREE_DOWNTIME,
          BRM_ACCOUNT_ID,
          BRM_ORDER_NO,
          BRM_ORDER_ID,
          BRM_SERVICE_ID,
          BRM_REC_OB_ID,
          BRM_REC_SUBSERVICE_ID,
          BRM_USG_OB_ID,
          BRM_USG_SUBSERVICE_ID,
          BRM_USG_RATERULE_ID,
          BRM_SPEED_VALUE,
          BRM_SPEED_UNIT_ID,
          BRM_CURRENCY_ID,
          IMPORT_DATE,
          LOAD_DATE,
          LOAD_CODE,
          LOAD_STATUS
         FROM PK212_PINDB_ORDERS_T 
        WHERE LOAD_CODE = 0
    )FOR UPDATE;
    
END PK212_XTTK_IMPORT_PORTAL65;
/
CREATE OR REPLACE PACKAGE BODY PK212_XTTK_IMPORT_PORTAL65
IS

--============================================================================================
-- После полного тестирования переводим л/с из c_BILLING_NPL -> Pk00_Const.c_BILLING_OLD
--============================================================================================
PROCEDURE Change_billing_id
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Check_data';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE ACCOUNT_T A
       SET A.BILLING_ID = Pk00_Const.c_BILLING_OLD
     WHERE A.BILLING_ID = c_BILLING_XTTK;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T.BILLING_ID: '||v_count||' rows c_BILLING_NPL -> c_BILLING_OLD', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Импорт договоров старого биллинга, фильтр выставляем в теле процедуры
--
PROCEDURE Import_contracts
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_contracts';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    DELETE FROM PK212_PINDB_ALL_CONTRACTS_T;
    --
    -- account_no - номер договора,
    -- custno - номер л/с
    -- остальное - интуитивно. если что, спрашивай.
    --
    INSERT INTO PK212_PINDB_ALL_CONTRACTS_T(
        ACCOUNT_ID, CONTRACT_NO, ACCOUNT_NO, 
        CLIENT_ID, KONTRAGENT, CLIENT, CUST_TYPE, CUSTDATE, 
        JUR_ZIP, JUR_CITY, JUR_ADDRESS, 
        PHIS_ZIP, PHIS_CITY, PHIS_ADDRESS, 
        PHONE, FAX, EMAIL_ADDR, PHIS_NAME, 
        INN, OKONH, OKPO, BANK, 
        SETTLEMENT, CORR, BIC, KPP, 
        COMPANY, CURRENCY, CURRENCY_SECONDARY, ORIGINAL, TAX_VAT, TAX_SALES, 
        SALES_NAME, DIRECTORATE, MARKET_SEG, BILLING_CURATOR, 
        GL_SEGMENT, IACCOUNT, AGENT_CODE, AGENT_NAME, PPTS_FLAG, DELIVERY
    )
    SELECT 
        ACCOUNT_ID, ACCOUNT_NO, CUSTNO, 
        CLIENT_ID, KONTRAGENT, CLIENT, CUST_TYPE, CUSTDATE, 
        JUR_ZIP, JUR_CITY, JUR_ADDRESS, 
        PHIS_ZIP, PHIS_CITY, PHIS_ADDRESS, PHONE, FAX, EMAIL_ADDR, PHIS_NAME, 
        INN, OKONH, OKPO, BANK, SETTLEMENT, CORR, BIC, KPP, 
        COMPANY, CURRENCY, CURRENCY_SECONDARY, ORIGINAL, TAX_VAT, TAX_SALES, 
        SALES_NAME, DIRECTORATE, MARKET_SEG, BILLING_CURATOR, 
        GL_SEGMENT, IACCOUNT, AGENT_CODE, AGENT_NAME, PPTS_FLAG, DELIVERY 
      FROM PIN.V_ALL_CONTRACTS@PINDB.WORLD c 
     WHERE CLIENT_ID != 1
       AND GL_SEGMENT LIKE '%RP%'
       AND GL_SEGMENT NOT LIKE '.Sakhalin TTK RP Brand' -- загружен из *.csv
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PINDB_ALL_CONTRACTS_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PINDB_ALL_CONTRACTS_T');
    COMMIT;

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;  

--============================================================================================
-- Загрузка информации по заказам, для загруженных договоров
--============================================================================================
PROCEDURE Import_orders
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_orders';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    --
    DELETE FROM PK212_PINDB_ORDERS_T;
    --
    -- заказы получаем по запросу А.Ю.Гурова
    --
    INSERT INTO PK212_PINDB_ORDERS_T (
        BRAND, ACCOUNT_POID, CONTRACT_NO, 
        ACCOUNT_NO, COMPANY, SERVICE_POID, ORDER_NO, 
        STATUS, RATE_PLAN, PRODUCT_NAME, EVENT_TYPE, IP_USAGE_RATE_PLAN,
        CYCLE_FEE_AMT, CURRENCY, CURRENCY_SECONDARY, 
        CYCLE_START_T, CYCLE_END_T,
        SMC_START_T, SMC_END_T, 
        ORDER_DATE, S_RGN, D_RGN, SERVICE_NAME, 
        SPEED_STR, FREE_DOWNTIME
    )
     select  a.gl_segment brand, a.poid_id0 account_poid, ci.auto_no CONTRACT_NO, 
             a.account_no ACCOUNT_NO, an.company, s.poid_id0 service_poid, s.login order_no, 
             ap.status, p.name rate_plan, d.name product_name, r.event_type, ap.descr ip_usage_rate_plan,
             ap.cycle_fee_amt, A.CURRENCY, A.CURRENCY_SECONDARY, 
             i2d@PINDB.WORLD(ap.cycle_start_t) cycle_start_t, i2d@PINDB.WORLD(ap.cycle_end_t) cycle_end_t,
             i2d@PINDB.WORLD(ap.smc_start_t) smc_start_t, i2d@PINDB.WORLD(ap.smc_end_t) smc_end_t, 
             VS.ORDER_DATE, VS.S_RGN, VS.D_RGN, VS.SERVICE_NAME, VS.SPEED_STR, VS.FREE_DOWNTIME
        from account_t@PINDB.WORLD a 
             inner join account_products_t@PINDB.WORLD ap on a.poid_id0 = ap.obj_id0
             inner join account_nameinfo_t@PINDB.WORLD an on a.poid_id0 = an.obj_id0 and an.rec_id = 1
             inner join plan_t@PINDB.WORLD p on ap.plan_obj_id0 = p.poid_id0
             inner join product_t@PINDB.WORLD d on ap.product_obj_id0 = d.poid_id0
             inner join rate_plan_t@PINDB.WORLD r on ap.product_obj_id0 = r.product_obj_id0
             inner join service_t@PINDB.WORLD s on ap.service_obj_id0 = s.poid_id0
             inner join profile_t@PINDB.WORLD pr on a.poid_id0 = pr.account_obj_id0
             inner join contract_info_t@PINDB.WORLD ci on pr.poid_id0 = ci.obj_id0
             inner join v_all_data_serv_plus@PINDB.WORLD vs on S.POID_ID0 = VS.POID_ID0
        where a.poid_id0 <> a.brand_obj_id0
          and exists (
              SELECT * FROM PK212_PINDB_ALL_CONTRACTS_T PC
               WHERE A.POID_ID0 = PC.ACCOUNT_ID
          ) 
        order by a.gl_segment, ci.auto_no, s.login;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK212_PINDB_ORDERS_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- оставляем только строки связанные с л/с из списка
    --
    Gather_Table_Stat(l_Tab_Name => 'PK212_PINDB_ORDERS_T');
    COMMIT;

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Импорт дополнительных данных из Portal 6.5
--
PROCEDURE Import_add_data
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_add_data';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- импортируем банки поставщика
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK212_PINDB_CONTRACTOR_BANK_T DROP STORAGE';

    INSERT INTO PK212_PINDB_CONTRACTOR_BANK_T( 
        ACCOUNT_POID, ACCOUNT_NO, CONTRACTOR_POID, CONTRACTOR, BANK, SETTLEMENT
    )
    select A.POID_ID0 account_poid, A.ACCOUNT_NO, C.IACC_OBJ_ID0 contractor_poid, 
           PN.COMPANY CONTRACTOR, PC.BANK, PC.SETTLEMENT
    from account_t@PINDB.WORLD a 
        inner join profile_t@PINDB.WORLD p on A.POID_ID0 = P.ACCOUNT_OBJ_ID0
        inner join contract_info_t@PINDB.WORLD c on P.POID_ID0 = C.OBJ_ID0
        inner join account_nameinfo_t@PINDB.WORLD pn on C.IACC_OBJ_ID0 = PN.OBJ_ID0 and PN.REC_ID = 1
        inner join profile_t@PINDB.WORLD pr on C.IACC_OBJ_ID0 = PR.ACCOUNT_OBJ_ID0
        inner join contract_info_t@PINDB.WORLD pc on PR.POID_ID0 = PC.OBJ_ID0
    WHERE EXISTS (
        SELECT * FROM PINDB_ALL_CONTRACTS_T PC
         WHERE PC.ACCOUNT_NO = A.ACCOUNT_NO
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK212_PINDB_CONTRACTOR_BANK_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- импортируем типы договоров
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK212_PINDB_CONTRACT_TYPE_T DROP STORAGE';
   
    INSERT INTO PK212_PINDB_CONTRACT_TYPE_T ( CONTRACT_NO, CONTRACT_TYPE_ID )
    SELECT DISTINCT b.auto_no CONTRACT_NO, b.client_cat_id CONTRACT_TYPE_ID
      from contract_info_t@PINDB.WORLD b --where b.client_cat_id = 4
     WHERE CLIENT_CAT_ID > 0
       AND EXISTS (
          SELECT * 
            FROM PK212_PINDB_ALL_CONTRACTS_T PC
           WHERE PC.ACCOUNT_NO = b.AUTO_NO
       )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK212_PINDB_CONTRACT_TYPE_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- проставляем CONTRACTOR_ID которые будем использовать для создания договора
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_contractor_id
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Set_contractor_id';
    v_count     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    UPDATE PK212_PINDB_ALL_CONTRACTS_T X
       SET X.BRM_CONTRACTOR_ID = 
            CASE 
                WHEN GL_SEGMENT = '.Sever TTK RP Brand'    THEN 1524380
                WHEN GL_SEGMENT = '.Sever TTK RP Access'   THEN 1524380
                WHEN GL_SEGMENT = '.Kaliningrad TTK RP Brand' THEN 1524391
                WHEN GL_SEGMENT = '.Centre TTK RP Brand'   THEN 1524378
                WHEN GL_SEGMENT = '.Samara TTK RP Brand'   THEN 1524376
                WHEN GL_SEGMENT = '.Chita TTK RP Brand'    THEN 1524397
                WHEN GL_SEGMENT = '.Sakhalin TTK RP Brand' THEN 1524393
            END,
           X.BRM_CONTRACTOR_BANK_ID =
            CASE 
                WHEN GL_SEGMENT = '.Sever TTK RP Brand'    THEN 1524381
                WHEN GL_SEGMENT = '.Sever TTK RP Access'   THEN 1524381
                WHEN GL_SEGMENT = '.Kaliningrad TTK RP Brand' THEN 1524392
                WHEN GL_SEGMENT = '.Centre TTK RP Brand'   THEN 1524379
                WHEN GL_SEGMENT = '.Samara TTK RP Brand'   THEN 1524377
                WHEN GL_SEGMENT = '.Chita TTK RP Brand'    THEN 1524398
                WHEN GL_SEGMENT = '.Sakhalin TTK RP Brand' THEN 1524394
            END,
           X.BRM_BRANCH_ID =
            CASE 
                WHEN GL_SEGMENT = '.Sever TTK RP Brand'    THEN 12
                WHEN GL_SEGMENT = '.Sever TTK RP Access'   THEN 12
                WHEN GL_SEGMENT = '.Kaliningrad TTK RP Brand' THEN 7
                WHEN GL_SEGMENT = '.Centre TTK RP Brand'   THEN 21
                WHEN GL_SEGMENT = '.Samara TTK RP Brand'   THEN 4
                WHEN GL_SEGMENT = '.Chita TTK RP Brand'    THEN 6
                WHEN GL_SEGMENT = '.Sakhalin TTK RP Brand' THEN 18
            END, 
           X.BRM_AGENT_ID = 
            CASE 
                WHEN GL_SEGMENT = '.Sever TTK RP Brand' THEN 304
                WHEN GL_SEGMENT = '.Sever TTK RP Access' THEN 312
                WHEN GL_SEGMENT = '.Kaliningrad TTK RP Brand' THEN 301
                WHEN GL_SEGMENT = '.Centre TTK RP Brand' THEN 308
                WHEN GL_SEGMENT = '.Samara TTK RP Brand' THEN 306
                WHEN GL_SEGMENT = '.Chita TTK RP Brand' THEN 309
                WHEN GL_SEGMENT = '.Sakhalin TTK RP Brand' THEN 303
            END;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK212_PINDB_ALL_CONTRACTS_T.BRM_CONTRACTOR_ID '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- проставляем MANAGER_ID которые будем использовать для создания договора
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_manager_id
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Set_manager_id';
    --v_client_id   INTEGER;
    v_count       INTEGER := 0;
    v_last_name   VARCHAR2(100);
    v_first_name  VARCHAR2(100);
    v_middle_name VARCHAR2(100);
    v_pos         INTEGER;
    v_len         INTEGER;
    v_manager_id  INTEGER;
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- биллиг-кураторы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FOR r_mgr IN (
      SELECT DISTINCT X.BRM_CONTRACTOR_ID, TRIM( X.BILLING_CURATOR ) MANAGER
        FROM PK212_PINDB_ALL_CONTRACTS_T X
       WHERE LOAD_CODE IS NULL
         AND BRM_BILLING_CURATOR_ID IS NULL
         AND TRIM( X.BILLING_CURATOR ) IS NOT NULL
    ) LOOP
      -- Варианты написанния менеджеров:
      -- 'Макеев Сергей Валентинович'
      -- 'Макеев С.В.'
      -- 'Макеев С.'
      -- 'Макеев С'
      -- 'Макеев'
      --
      -- фамилия --------------------------------------------------------
      v_len := INSTR(r_mgr.manager,' ',1);
      IF v_len > 0 THEN
        v_last_name := LTRIM(SUBSTR(r_mgr.manager, 1, v_len-1));
      ELSE
        v_last_name := TRIM(r_mgr.manager);
      END IF;
      
      -- имя ------------------------------------------------------------
      v_first_name := SUBSTR(LTRIM(SUBSTR(r_mgr.manager, v_len)),1,1);
      IF v_first_name IS NOT NULL THEN 
        v_first_name := v_first_name||'.';
        
        -- отчество -----------------------------------------------------
        v_pos := INSTR(r_mgr.manager,' ',1,2);
        IF v_pos > 0 THEN
          v_middle_name := SUBSTR(LTRIM(SUBSTR(r_mgr.manager, v_pos+1)),1,1);
        ELSE
          v_pos := INSTR(r_mgr.manager,'.',1,1);
          IF v_pos > 0 THEN 
            v_middle_name := SUBSTR(LTRIM(SUBSTR(r_mgr.manager, v_pos+1)),1,1);
          END IF;
        END IF;
        IF v_middle_name IS NOT NULL THEN
          v_middle_name := v_middle_name||'.';
        END IF;
        
      END IF;

      -- поиск подходящего менеджера в БД
      SELECT MIN(MANAGER_ID) INTO v_manager_id
       FROM MANAGER_T M
      WHERE M.LAST_NAME     = v_last_name
        AND M.FIRST_NAME    = v_first_name
        AND M.MIDDLE_NAME   = v_middle_name
        AND M.CONTRACTOR_ID = r_mgr.brm_contractor_id;

      -- назначение ID менеджеру
      IF v_manager_id IS NULL THEN
        v_manager_id := Pk02_Poid.Next_manager_id;
      END IF;

      -- проставляем Ф.И.О - BRM-менеджера в формате: 'Макеев С.В.'
      UPDATE PK212_PINDB_ALL_CONTRACTS_T X
         SET BRM_BILLING_CURATOR_ID = v_manager_id,
             BRM_BC_LASTNAME   = v_last_name,
             BRM_BC_FIRSTNAME  = v_first_name,
             BRM_BC_MIDDLENAME = v_middle_name
       WHERE LOAD_CODE IS NULL
         AND BRM_BILLING_CURATOR_ID IS NULL
         AND BRM_BC_LASTNAME IS NULL
         AND BILLING_CURATOR = r_mgr.manager;

      -- увеличиваем счетчик статистики
      v_count := v_count + 1; 

    END LOOP;  

    Pk01_Syslog.Write_msg(v_count||' - distinct brm_billing_curator_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- продавцы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FOR r_mgr IN (
      SELECT DISTINCT X.BRM_CONTRACTOR_ID, TRIM( X.SALES_NAME ) MANAGER
        FROM PK212_PINDB_ALL_CONTRACTS_T X
       WHERE LOAD_CODE IS NULL
         AND BRM_SALE_CURATOR_ID IS NULL
         AND TRIM( X.SALES_NAME ) IS NOT NULL
    ) LOOP
      -- Варианты написанния менеджеров:
      -- 'Макеев Сергей Валентинович'
      -- 'Макеев С.В.'
      -- 'Макеев С.'
      -- 'Макеев С'
      -- 'Макеев'
      --
      -- фамилия --------------------------------------------------------
      v_len := INSTR(r_mgr.manager,' ',1);
      IF v_len > 0 THEN
        v_last_name := LTRIM(SUBSTR(r_mgr.manager, 1, v_len-1));
      ELSE
        v_last_name := TRIM(r_mgr.manager);
      END IF;
      
      -- имя ------------------------------------------------------------
      v_first_name := SUBSTR(LTRIM(SUBSTR(r_mgr.manager, v_len)),1,1);
      IF v_first_name IS NOT NULL THEN 
        v_first_name := v_first_name||'.';
        
        -- отчество -----------------------------------------------------
        v_pos := INSTR(r_mgr.manager,' ',1,2);
        IF v_pos > 0 THEN
          v_middle_name := SUBSTR(LTRIM(SUBSTR(r_mgr.manager, v_pos+1)),1,1);
        ELSE
          v_pos := INSTR(r_mgr.manager,'.',1,1);
          IF v_pos > 0 THEN 
            v_middle_name := SUBSTR(LTRIM(SUBSTR(r_mgr.manager, v_pos+1)),1,1);
          END IF;
        END IF;
        IF v_middle_name IS NOT NULL THEN
          v_middle_name := v_middle_name||'.';
        END IF;
        
      END IF;

      -- поиск подходящего менеджера в БД
      SELECT MIN(MANAGER_ID) INTO v_manager_id
       FROM MANAGER_T M
      WHERE M.LAST_NAME     = v_last_name
        AND M.FIRST_NAME    = v_first_name
        AND M.MIDDLE_NAME   = v_middle_name
        AND M.CONTRACTOR_ID = r_mgr.brm_contractor_id;

      -- назначение ID менеджеру
      IF v_manager_id IS NULL THEN
        v_manager_id := Pk02_Poid.Next_manager_id;
      END IF;

      -- проставляем Ф.И.О - BRM-менеджера в формате: 'Макеев С.В.'
      UPDATE PK212_PINDB_ALL_CONTRACTS_T X
         SET BRM_SALE_CURATOR_ID = v_manager_id,
             BRM_SC_LASTNAME   = v_last_name,
             BRM_SC_FIRSTNAME  = v_first_name,
             BRM_SC_MIDDLENAME = v_middle_name
       WHERE LOAD_CODE IS NULL
         AND BRM_SALE_CURATOR_ID IS NULL
         AND BRM_SC_LASTNAME IS NULL
         AND SALES_NAME = r_mgr.manager;

      -- увеличиваем счетчик статистики
      v_count := v_count + 1;

    END LOOP;  

    Pk01_Syslog.Write_msg(v_count||' - distinct brm_sale_curator_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- проставляем BRM_CLIENT_ID которые будем использовать для создания договора
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_client_id
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Set_client_id';
    v_client_id INTEGER;
    v_count     INTEGER := 0;
    v_count_new INTEGER := 0;
BEGIN
    FOR r_cln IN (
      SELECT DISTINCT CLIENT CLIENT  
        FROM PK212_PINDB_ALL_CONTRACTS_T X
       WHERE LOAD_CODE IS NULL
         AND BRM_CLIENT_ID IS NULL
         AND CLIENT IS NOT NULL  
    ) LOOP
      --
      -- ищем среди существующих покупателей
      SELECT MIN(CL.CLIENT_ID) 
        INTO v_client_id  
        FROM CLIENT_T CL
       WHERE LOWER(CL.CLIENT_NAME) = LOWER(r_cln.CLIENT);
        
      IF v_client_id IS NULL THEN 
         v_client_id := PK02_POID.NEXT_CLIENT_ID;
         v_count_new := v_count_new + 1;
      END IF;
      --
      UPDATE PK212_PINDB_ALL_CONTRACTS_T X
         SET X.BRM_CLIENT_ID = v_client_id
       WHERE X.CLIENT        = r_cln.CLIENT
         AND X.LOAD_CODE     IS NULL
         AND X.BRM_CLIENT_ID IS NULL;  
      --
      v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg(v_count||' - distinct client_id, '||v_count_new||' - new', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- проставляем BRM_CUSTOMER_ID которые будем использовать для создания договора
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_customer_id
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Set_customer_id';
    v_customer_id INTEGER;
    v_count       INTEGER := 0;
    v_count_new   INTEGER := 0;
BEGIN
    FOR r_cst IN (
      SELECT KONTRAGENT ERP_CODE, COMPANY, INN, KPP  
        FROM PK212_PINDB_ALL_CONTRACTS_T X
       WHERE LOAD_CODE IS NULL
         AND BRM_CUSTOMER_ID IS NULL
         AND COMPANY IS NOT NULL
       GROUP BY KONTRAGENT, COMPANY, INN, KPP
    ) LOOP
      --
      -- ищем среди существующих покупателей
      SELECT MIN(CS.CUSTOMER_ID) 
        INTO v_customer_id  
        FROM CUSTOMER_T CS
       WHERE CS.ERP_CODE = r_cst.ERP_CODE  
         AND NVL(CS.INN,'0') = NVL(r_cst.INN,'0')
         AND NVL(CS.KPP,'0') = NVL(r_cst.KPP,'0')
         AND LOWER(CS.CUSTOMER) = LOWER(r_cst.COMPANY);
        
      IF v_customer_id IS NULL THEN 
         v_customer_id := PK02_POID.NEXT_CUSTOMER_ID;
         v_count_new := v_count_new + 1;
      END IF;
      --
      UPDATE PK212_PINDB_ALL_CONTRACTS_T X
         SET X.BRM_CUSTOMER_ID = v_customer_id
       WHERE X.KONTRAGENT   = r_cst.ERP_CODE  
         AND NVL(X.INN,'0') = NVL(r_cst.INN,'0')
         AND NVL(X.KPP,'0') = NVL(r_cst.KPP,'0')
         AND X.COMPANY      = r_cst.COMPANY
         AND X.LOAD_CODE IS NULL
         AND X.BRM_CUSTOMER_ID IS NULL;  
      --
      v_count := v_count + 1;
    END LOOP;
    --
    Pk01_Syslog.Write_msg(v_count||' - distinct customer_id, '||v_count_new||' - new', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- проставляем CONTRACT_ID - которые будем использовать для создания договора
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_contract_id
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Set_contract_id';
    v_contract_id INTEGER;
    v_count       INTEGER := 0;
BEGIN
    -- проверяем что номеров договоров нет в BRM
    UPDATE PK212_PINDB_ALL_CONTRACTS_T X
       SET LOAD_CODE = -1,
           LOAD_STATUS = 'CONTRACT_NO - уже существует в BRM'
     WHERE CONTRACT_NO     IS NOT NULL
       AND EXISTS (
        SELECT * FROM CONTRACT_T C
         WHERE C.CONTRACT_NO = X.CONTRACT_NO
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - contract_no already exists in BRM', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );

    -- проставляем contract_id
    FOR r_ctr IN (
      SELECT DISTINCT CONTRACT_NO  
        FROM PK212_PINDB_ALL_CONTRACTS_T X
       WHERE LOAD_CODE       IS NULL
         AND BRM_CONTRACT_ID IS NULL
         AND CONTRACT_NO     IS NOT NULL  
    ) LOOP
      --
      v_contract_id := PK02_POID.NEXT_CONTRACT_ID;
      -- проставляем ID
      UPDATE PK212_PINDB_ALL_CONTRACTS_T X
         SET X.BRM_CONTRACT_ID = v_contract_id,
             X.BRM_CONTRACT_NO = r_ctr.contract_no
       WHERE X.CONTRACT_NO = r_ctr.contract_no
         AND X.LOAD_CODE IS NULL
         AND X.BRM_CONTRACT_ID IS NULL;  
      --
      v_count := v_count + 1;
      --
    END LOOP;
    --
    Pk01_Syslog.Write_msg(v_count||' - distinct contract_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- проставляем дополнительную информацию по договору
-- тип клиента, сегмент рынка, валюта
PROCEDURE Set_contract_info
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Set_contract_info';
    v_count       INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- сегмент рынка (V_PINDB_ALL_CONTRACTS_T.CUST_TYPE - перепутано с типом клиента)
    MERGE INTO PK212_PINDB_ALL_CONTRACTS_T X
    USING (
    SELECT DM.NAME, DM.KEY_ID 
      FROM DICTIONARY_T DM
     WHERE DM.PARENT_ID = 63 -- MARKET_SEGMENT
    ) DM
    ON (
        DM.NAME = X.CUST_TYPE
    )
    WHEN MATCHED THEN UPDATE SET X.BRM_MARKET_SEGMENT_ID = DM.KEY_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_MARKET_SEGMENT_ID '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- тип клиента (V_PINDB_ALL_CONTRACTS_T.MARKET_SEGMENT - перепутано с сегментом рынка)
    MERGE INTO PK212_PINDB_ALL_CONTRACTS_T X
    USING (
    SELECT DM.NAME, DM.KEY_ID 
      FROM DICTIONARY_T DM
     WHERE DM.PARENT_ID = 64 -- CLIENT_TYPE
    ) DM
    ON (
        DM.NAME = X.MARKET_SEG
    )
    WHEN MATCHED THEN UPDATE SET X.BRM_CLIENT_TYPE_ID = DM.KEY_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_CLIENT_TYPE_ID '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- устанавливаем валюту договора
    UPDATE PK212_PINDB_ALL_CONTRACTS_T
       SET BRM_CURRENCY_ID = 
         CASE
            WHEN CURRENCY = 810 AND CURRENCY_SECONDARY = 0 THEN 810
            WHEN CURRENCY_SECONDARY = 286 THEN 286
            ELSE NULL 
         END;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_CURRENCY_ID '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- проставляем метод доставки счета
    MERGE INTO PK212_PINDB_ALL_CONTRACTS_T X
    USING (
        SELECT ACCOUNT_ID, KEY_ID
          FROM (
            SELECT X.ACCOUNT_ID, D.KEY_ID, 
                   ROW_NUMBER() OVER (PARTITION BY X.ACCOUNT_ID ORDER BY CONTRACT_NO) RN 
              FROM PK212_PINDB_ALL_CONTRACTS_T X, DICTIONARY_T D
             WHERE D.PARENT_ID = 65
               AND (X.DELIVERY = D.NAME OR X.DELIVERY = D.NOTES)
         )
         WHERE RN = 1
    ) D
    ON (
       X.ACCOUNT_ID =  D.ACCOUNT_ID
    )
    WHEN MATCHED THEN UPDATE SET X.BRM_DELIVERY_METHOD_ID = D.KEY_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DELIVERY_METHOD_ID '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- проставляем ACCOUNT_ID - которые будем использовать для создания договора
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_account_id
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Set_account_id';
    v_account_id     INTEGER;
    v_profile_id     INTEGER;
    v_jur_address_id INTEGER;
    v_dlv_address_id INTEGER;
    v_count          INTEGER := 0;
BEGIN
    -- проверяем что номера л/с уникальны в выгрузке
    UPDATE PK212_PINDB_ALL_CONTRACTS_T X
       SET LOAD_CODE = -1,
           LOAD_STATUS = 'ACCOUNT_NO - задублирован в Portal 6.5'
     WHERE EXISTS (
        SELECT * FROM PK212_PINDB_ALL_CONTRACTS_T X1
         WHERE X1.ACCOUNT_ID != X.ACCOUNT_ID
           AND X1.ACCOUNT_NO = X.ACCOUNT_NO
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - account_no duplicated in Portal 6.5', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );

    -- проверяем что номеров л/с нет в BRM
    UPDATE PK212_PINDB_ALL_CONTRACTS_T X
       SET LOAD_CODE = -1,
           LOAD_STATUS = 'ACCOUNT_NO - уже существует в BRM'
     WHERE EXISTS (
        SELECT * FROM ACCOUNT_T A
         WHERE A.ACCOUNT_NO = X.ACCOUNT_NO
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - account_no already exists in BRM', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );

    -- проставляем ACCOUNT_ID - которые будем использовать для создания договора
    FOR r_acc IN (
      SELECT DISTINCT ACCOUNT_NO  
        FROM PK212_PINDB_ALL_CONTRACTS_T X
       WHERE LOAD_CODE  IS NULL
         AND BRM_ACCOUNT_ID IS NULL
         AND ACCOUNT_NO IS NOT NULL
    ) LOOP
      --
      v_account_id     := PK02_POID.NEXT_ACCOUNT_ID;
      v_profile_id     := PK02_POID.NEXT_ACCOUNT_PROFILE_ID;
      v_jur_address_id := PK02_POID.NEXT_ADDRESS_ID;
      v_dlv_address_id := PK02_POID.NEXT_ADDRESS_ID;
      --
      UPDATE PK212_PINDB_ALL_CONTRACTS_T X
         SET X.BRM_ACCOUNT_NO     = r_acc.account_no,
             X.BRM_ACCOUNT_ID     = v_account_id,
             X.BRM_PROFILE_ID     = v_profile_id,
             X.BRM_JUR_ADDRESS_ID = v_jur_address_id,
             X.BRM_DLV_ADDRESS_ID = v_dlv_address_id
       WHERE X.ACCOUNT_NO         = r_acc.account_no
         AND X.LOAD_CODE      IS NULL
         AND X.BRM_ACCOUNT_ID IS NULL;  
      --
      v_count := v_count + 1;
      --
    END LOOP;
    Pk01_Syslog.Write_msg(v_count||' - distinct account_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- проставляем ORDER_ID
PROCEDURE Set_order_id
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Set_order_id';
    v_count          INTEGER := 0;
    v_order_id       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    -- эти события для данной задачи не нужны, по словам А.Ю.Гурова
    DELETE FROM PK212_PINDB_ORDERS_T XO
     WHERE EVENT_TYPE != '/event/billing/product/fee/cycle/cycle_arrear';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - rows out', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- проставляем ошибку, если заказ уже есть в BRM
    UPDATE PK212_PINDB_ORDERS_T XO
       SET XO.LOAD_CODE = -1,
           XO.LOAD_STATUS = 'Номер заказа уже есть в BRM'
     WHERE EXISTS (
           SELECT * 
             FROM ORDER_T O
            WHERE O.ORDER_NO = XO.ORDER_NO
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - ORDER_NO, exists in BRM', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );

    -- проставляем ошибку, для строк открытых и закрытых в один день
    UPDATE PK212_PINDB_ORDERS_T XO
       SET XO.LOAD_CODE = -1,
           XO.LOAD_STATUS = 'Совпадают даты открытия и закрытия заказа'
     WHERE CYCLE_START_T >= CYCLE_END_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - CYCLE_START_T >= CYCLE_END_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );

    -- проставляем ошибку, для строк открытых и закрытых в один день
    UPDATE PK212_PINDB_ORDERS_T XO
       SET XO.LOAD_CODE = -1,
           XO.LOAD_STATUS = 'Отсутствует дата начала действия заказа'
     WHERE CYCLE_START_T IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - CYCLE_START_T >= CYCLE_END_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );


    -- проставляем ID лицевого счета
    MERGE INTO PK212_PINDB_ORDERS_T XO
    USING (
        SELECT DISTINCT X.ACCOUNT_ID, X.BRM_ACCOUNT_ID
          FROM PK212_PINDB_ALL_CONTRACTS_T X
    ) X
    ON (
       XO.ACCOUNT_POID = X.ACCOUNT_ID
    )
    WHEN MATCHED THEN UPDATE SET XO.BRM_ACCOUNT_ID = X.BRM_ACCOUNT_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_ACCOUNT_ID '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- проставляем ошибку, если есть ошибка по л/с
    UPDATE PK212_PINDB_ORDERS_T XO
       SET XO.LOAD_CODE = -1,
           XO.LOAD_STATUS = 'Не найден ID л/с в BRM'
     WHERE XO.BRM_ACCOUNT_ID IS NULL;

    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' - ACCOUNT_ID - have status error in BRM', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );

    -- проставляем order_id
    FOR r_ord IN (
      SELECT DISTINCT XO.ORDER_NO  
        FROM PK212_PINDB_ORDERS_T XO
       WHERE XO.LOAD_CODE IS NULL
    )
    LOOP
      v_order_id := PK02_POID.Next_order_id;
      UPDATE PK212_PINDB_ORDERS_T XO 
         SET XO.BRM_ORDER_ID = v_order_id,
             XO.BRM_ORDER_NO = SUBSTR(XO.ORDER_NO,1,100)
       WHERE XO.ORDER_NO = r_ord.order_no;
      v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg(v_count||' - distinct order_id', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- проставляем информацию для ORDER_INFO_T
PROCEDURE Set_order_info
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Set_order_info';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    MERGE INTO PK212_PINDB_ORDERS_T XO
    USING (
        SELECT DISTINCT PO.SPEED_STR, 
               TO_NUMBER( NVL(TRIM(SUBSTR(LTRIM( REPLACE(PO.SPEED_STR,',','.') ), 1, INSTR(LTRIM(PO.SPEED_STR),' '))), 0) ) SPEED_VALUE,
               D.KEY_ID SPEED_UNIT_ID
          FROM PK212_PINDB_ORDERS_T PO, DICTIONARY_T D 
         WHERE D.PARENT_ID(+) = 67
           AND D.NAME = TRIM(SUBSTR(LTRIM(PO.SPEED_STR), INSTR(LTRIM(PO.SPEED_STR),' ')))
           AND PO.LOAD_STATUS IS NULL
           AND (PO.RATE_PLAN LIKE 'IP Routing%' OR PO.RATE_PLAN LIKE 'IP Burst%')
           AND PO.IP_USAGE_RATE_PLAN = 'IP 0'
           AND PO.SPEED_STR IS NOT NULL
    ) XD
    ON (
        XO.SPEED_STR = XD.SPEED_STR
    )
    WHEN MATCHED THEN UPDATE SET XO.BRM_SPEED_VALUE = XD.SPEED_VALUE, 
                                 XO.BRM_SPEED_UNIT_ID = XD.SPEED_UNIT_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_INFO: '||v_count||' - rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );

    -- устанавливаем валюту заказа
    UPDATE PK212_PINDB_ORDERS_T XO
       SET BRM_CURRENCY_ID = 
         CASE
            WHEN CURRENCY = 810 AND CURRENCY_SECONDARY = 0 THEN 810
            WHEN CURRENCY_SECONDARY = 286 THEN 286
            ELSE NULL 
         END;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_CURRENCY_ID '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Установить ID услуги
PROCEDURE Set_service_id
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Set_service_id';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    -- проставляем service_id
    MERGE INTO PK212_PINDB_ORDERS_T XO
    USING (
        SELECT DISTINCT S.SERVICE, S.SERVICE_ID 
          FROM PK212_PINDB_ORDERS_T XO, SERVICE_T S -- 2.970
         WHERE XO.SERVICE_NAME = S.SERVICE
           AND S.SERVICE_ID NOT IN (152, 61)
    ) S
    ON (
        XO.SERVICE_NAME = S.SERVICE
    )
    WHEN MATCHED THEN UPDATE SET XO.BRM_SERVICE_ID = S.SERVICE_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SERVICE_ID '||v_count||' - rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- проставляем ошибки, где service_id неопределен
    UPDATE PK212_PINDB_ORDERS_T XO
       SET LOAD_CODE = -1, 
           LOAD_STATUS = 'Нет услуги в SERVICE_T'
     WHERE BRM_SERVICE_ID IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SERVICE_ID '||v_count||' - rows unknown', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Установить ID компонент услуги
PROCEDURE Set_subservice_id
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Set_subservice_id';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    -- проставляем компоненту услуги абонплата
    UPDATE PK212_PINDB_ORDERS_T XO
       SET XO.BRM_REC_SUBSERVICE_ID = Pk00_Const.c_SUBSRV_REC, -- 41
           XO.BRM_REC_OB_ID         = PK02_POID.Next_order_body_id
     WHERE BRM_SERVICE_ID IS NOT NULL
       AND XO.BRM_REC_SUBSERVICE_ID IS NULL
       AND XO.CYCLE_FEE_AMT IS NOT NULL
       AND XO.CYCLE_FEE_AMT != 0;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_REC_SUBSERVICE_ID '||v_count||' - rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- проставляем компоненты услуги с трафиком
    UPDATE PK212_PINDB_ORDERS_T XO
       SET XO.BRM_USG_SUBSERVICE_ID = CASE
                                        WHEN XO.RATE_PLAN = 'IP Routing RUR' THEN Pk00_Const.c_SUBSRV_VOLUME
                                        WHEN XO.RATE_PLAN = 'IP Burst RUR'   THEN Pk00_Const.c_SUBSRV_BURST
                                        ELSE NULL
                                      END,
           XO.BRM_USG_RATERULE_ID   = CASE
                                        WHEN XO.RATE_PLAN = 'IP Routing RUR' THEN 2407
                                        WHEN XO.RATE_PLAN = 'IP Burst RUR'   THEN 2409
                                        ELSE NULL
                                      END,
           XO.BRM_USG_OB_ID         = PK02_POID.Next_order_body_id
     WHERE BRM_SERVICE_ID IS NOT NULL
       AND XO.BRM_USG_SUBSERVICE_ID IS NULL
       AND IP_USAGE_RATE_PLAN IS NOT NULL
       AND IP_USAGE_RATE_PLAN != 'IP 0';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_USG_SUBSERVICE_ID '||v_count||' - rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- проставляем ошибки, где оба поля subservice_id неопределены
    UPDATE PK212_PINDB_ORDERS_T XO
       SET LOAD_CODE = -1, 
           LOAD_STATUS = 'Не проставлена компонента услуги'
     WHERE BRM_REC_SUBSERVICE_ID IS NULL
       AND BRM_USG_SUBSERVICE_ID IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SUBSERVICE_ID '||v_count||' - rows unknown', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Импорт данных из старого биллинга 
--
PROCEDURE Import_data
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_data';
BEGIN

    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    DELETE FROM PK212_PINDB_ALL_CONTRACTS_T;
    DELETE FROM PK212_PINDB_ORDERS_T;

    Import_contracts;
    Import_orders;
    Import_add_data;
    
    Set_contractor_id;
    Set_manager_id;
    Set_client_id;
    Set_customer_id;
    Set_contract_id;
    Set_contract_info;
    Set_account_id;
    UPDATE PK212_PINDB_ALL_CONTRACTS_T SET IMPORT_DATE = SYSDATE;
    UPDATE PK212_PINDB_ALL_CONTRACTS_T SET LOAD_CODE = 0 WHERE LOAD_CODE IS NULL;
    
    Set_order_id;
    Set_order_info;    
    Set_service_id;
    Set_subservice_id;
    UPDATE PK212_PINDB_ORDERS_T SET IMPORT_DATE = SYSDATE;
    UPDATE PK212_PINDB_ORDERS_T SET LOAD_CODE = 0 WHERE LOAD_CODE IS NULL;

    Gather_Table_Stat(l_Tab_Name => 'PK212_PINDB_ALL_CONTRACTS_T');
    Gather_Table_Stat(l_Tab_Name => 'PK212_PINDB_ORDERS_T');

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Загрузка данных
--============================================================================================
-- расшифровка шагов выполнения импорта
FUNCTION view_step (p_step IN INTEGER) RETURN VARCHAR2
IS 
BEGIN
  RETURN CASE
           WHEN p_step = c_LOAD_CODE_ACC THEN 'Создание л/с'
           WHEN p_step = c_LOAD_CODE_CLN THEN 'Создание клиента'
           WHEN p_step = c_LOAD_CODE_MGR THEN 'Создание sale-curator'
           WHEN p_step = c_LOAD_CODE_CTR THEN 'Создание договора'
           WHEN p_step = c_LOAD_CODE_CST THEN 'Создание контрагента-покупателя'
           WHEN p_step = c_LOAD_CODE_APF THEN 'Создание профиля л/с'
           WHEN p_step = c_LOAD_CODE_AJR THEN 'Создание адреса юридического'
           WHEN p_step = c_LOAD_CODE_ADL THEN 'Создание адреса доставки'
           WHEN p_step = c_LOAD_CODE_ORD THEN 'Создание заказа'    
           WHEN p_step = c_LOAD_CODE_SAL THEN 'Создание service-alias'
           WHEN p_step = c_LOAD_CODE_ABP THEN 'Создание компонена услуги - абонплата'
           WHEN p_step = c_LOAD_CODE_USG THEN 'Создание компонена услуги - трафик'
           WHEN p_step = c_LOAD_CODE_FIN THEN 'Финиш'
           ELSE 'Неизвестный шаг'
         END;
END;

--============================================================================================
-- Загрузка данных по лицевым счетам
--============================================================================================
PROCEDURE Load_accounts
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Load_accounts';
    v_count           INTEGER := 0;
    v_step            INTEGER := 1;
    v_ok              INTEGER := 0;
    v_error           INTEGER := 0;
    v_load_status     VARCHAR2(1000);
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR r_abn IN c_CTR LOOP
      
      BEGIN
        -- -------------------------------------------------------- --
        -- создаем Л/С
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_ACC;        
        SELECT COUNT(*) INTO v_count
          FROM ACCOUNT_T A
         WHERE A.ACCOUNT_ID = r_abn.brm_account_id;
        IF v_count = 0 THEN
            -- создаем запись лицевого счета
            INSERT INTO ACCOUNT_T A(
               ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, CURRENCY_ID, 
               STATUS, PARENT_ID, NOTES,
               BALANCE, BALANCE_DATE, CREATE_DATE, BILLING_ID,
               EXTERNAL_ID, EXTERNAL_NO
            )VALUES(
               r_abn.brm_account_id, r_abn.brm_account_no, 
               Pk00_Const.c_ACC_TYPE_J, r_abn.brm_currency_id, 
               'NEW', NULL, 
               'ACCOUNT_NO='||r_abn.ACCOUNT_NO||' импортировано из Portal65-XTTK'|| TO_CHAR(SYSDATE,'dd.mm.yyyy'), 
               0, SYSDATE, SYSDATE, c_BILLING_XTTK, 
               r_abn.account_id, r_abn.account_no
            );
            -- создаем описатель счетов и способ доставки счета
            Pk07_Bill.New_billinfo (
                         p_account_id    => r_abn.brm_account_id,        -- ID лицевого счета
                         p_currency_id   => Pk00_Const.c_CURRENCY_RUB,   -- ID валюты счета
                         p_delivery_id   => r_abn.brm_delivery_method_id,-- ID способа доставки счета
                         p_days_for_payment => 30           -- кол-во дней на оплату счета
                     );
        END IF;
    
        -- -------------------------------------------------------- --
        -- создаем клиента договора
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_CLN;
        SELECT COUNT(*) INTO v_count
          FROM CLIENT_T CL
         WHERE CLIENT_ID = r_abn.brm_client_id
        ;
        IF v_count = 0 THEN
            INSERT INTO CLIENT_T (CLIENT_ID, CLIENT_NAME)
            VALUES(r_abn.brm_client_id, r_abn.client);
        END IF;
       
        -- -------------------------------------------------------- --
        -- создаем sale-куратора договора
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_MGR;
        SELECT COUNT(*) INTO v_count
          FROM MANAGER_T M
         WHERE M.MANAGER_ID = r_abn.brm_sale_curator_id
        ;
        IF v_count = 0 AND r_abn.brm_sale_curator_id IS NOT NULL THEN
            INSERT INTO MANAGER_T (
                MANAGER_ID, CONTRACTOR_ID, 
                LAST_NAME, FIRST_NAME, MIDDLE_NAME, 
                DATE_FROM 
            )VALUES(
                r_abn.brm_sale_curator_id, r_abn.brm_contractor_id, 
                r_abn.brm_sc_lastname, r_abn.brm_sc_firstname, r_abn.brm_sc_middlename, 
                TO_DATE('01.01.2000','dd.mm.yyyy')
            );
        END IF;

        -- -------------------------------------------------------- --
        -- создаем billing-куратора договора
        -- -------------------------------------------------------- --
        SELECT COUNT(*) INTO v_count
          FROM MANAGER_T M
         WHERE M.MANAGER_ID = r_abn.brm_billing_curator_id
        ;
        IF v_count = 0 AND r_abn.brm_billing_curator_id IS NOT NULL THEN
            INSERT INTO MANAGER_T (
                MANAGER_ID, CONTRACTOR_ID, 
                LAST_NAME, FIRST_NAME, MIDDLE_NAME, 
                DATE_FROM 
            )VALUES(
                r_abn.brm_billing_curator_id, r_abn.brm_contractor_id, 
                r_abn.brm_bc_lastname, r_abn.brm_bc_firstname, r_abn.brm_bc_middlename, 
                TO_DATE('01.01.2000','dd.mm.yyyy')
            );
        END IF;

        -- -------------------------------------------------------- --
        -- создаем договор
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_CTR;
        SELECT COUNT(*) INTO v_count
          FROM CONTRACT_T C
         WHERE C.CONTRACT_ID = r_abn.brm_contract_id
        ;
        IF v_count = 0 THEN
            INSERT INTO CONTRACT_T C (
              CONTRACT_ID, CONTRACT_NO, 
              MARKET_SEGMENT_ID, CLIENT_TYPE_ID, 
              DATE_FROM, DATE_TO, 
              CLIENT_ID, 
              NOTES
            )VALUES(
              r_abn.brm_contract_id, r_abn.brm_contract_no, 
              r_abn.brm_market_segment_id, r_abn.brm_client_type_id,
              r_abn.custdate, Pk00_Const.c_DATE_MAX, 
              r_abn.brm_client_id, 
              'импортировано из Portal65-XTTK'|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );
            -- привязываем менеджера к договору
            IF r_abn.brm_sale_curator_id IS NOT NULL THEN 
                INSERT INTO SALE_CURATOR_T (MANAGER_ID, CONTRACT_ID, DATE_FROM, DATE_TO)
                VALUES(r_abn.brm_sale_curator_id, r_abn.brm_contract_id, 
                       r_abn.custdate, Pk00_Const.c_DATE_MAX)
                ;
            END IF;
            -- привязываем сотрудника ДРУ к договору
            IF r_abn.brm_billing_curator_id IS NOT NULL THEN 
                INSERT INTO BILLING_CURATOR_T (MANAGER_ID, CONTRACT_ID)
                VALUES(r_abn.brm_billing_curator_id, r_abn.brm_contract_id)
                ;
            END IF;
            
        END IF;
    
        -- -------------------------------------------------------- --
        -- создаем покупателя
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_CST;
        SELECT COUNT(*) INTO v_count
          FROM CUSTOMER_T CS
         WHERE CUSTOMER_ID = r_abn.brm_customer_id
        ;
        IF v_count = 0 THEN
            INSERT INTO CUSTOMER_T (
                   CUSTOMER_ID, ERP_CODE, INN, KPP, 
                   CUSTOMER, SHORT_NAME, 
                   NOTES
                   )
            VALUES(r_abn.brm_customer_id, r_abn.kontragent, r_abn.inn, r_abn.kpp, 
                   r_abn.company, r_abn.company, 
                   'импортировано из Portal65-XTTK '||TO_CHAR(SYSDATE,'dd.mm.yyyy')
                   )  
            ;
        END IF;
    
        -- -------------------------------------------------------- --
        -- Создаем профиль л/с
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_APF;
        SELECT COUNT(*) INTO v_count
          FROM ACCOUNT_PROFILE_T AP
         WHERE AP.PROFILE_ID = r_abn.brm_profile_id;
        IF v_count = 0 THEN
            INSERT INTO ACCOUNT_PROFILE_T (
               PROFILE_ID, ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID,
               CONTRACTOR_ID, BRANCH_ID, AGENT_ID,  
               CONTRACTOR_BANK_ID, VAT, DATE_FROM, DATE_TO, KPP, ERP_CODE)
            VALUES
               (r_abn.brm_profile_id, 
                r_abn.brm_account_id, r_abn.brm_contract_id, r_abn.brm_customer_id, 
                r_abn.brm_contractor_id, r_abn.brm_branch_id, r_abn.brm_agent_id, 
                r_abn.brm_contractor_bank_id, Pk00_Const.c_VAT, 
                r_abn.custdate, NULL,
                r_abn.kpp, r_abn.kontragent 
                )
            ;
        END IF;
   
        -- -------------------------------------------------------- --
        -- Создаем юридический адрес
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_AJR;
        SELECT COUNT(*) INTO v_count
          FROM ACCOUNT_CONTACT_T AC
         WHERE AC.CONTACT_ID = r_abn.brm_jur_address_id
        ;
        IF v_count = 0 THEN
            INSERT INTO ACCOUNT_CONTACT_T (   
                CONTACT_ID,ADDRESS_TYPE,ACCOUNT_ID,
                COUNTRY,ZIP,STATE,CITY,ADDRESS,DATE_FROM,
                NOTES
            )VALUES(
                r_abn.brm_jur_address_id, PK00_CONST.c_ADDR_TYPE_JUR, r_abn.brm_account_id,
                'РФ', r_abn.jur_zip, NULL, 
                r_abn.jur_city, r_abn.jur_address, r_abn.custdate, 
                'импортировано из Portal65-XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );  
        END IF;
        
        -- -------------------------------------------------------- --
        -- Создаем адрес доставки
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_ADL;
        SELECT COUNT(*) INTO v_count
          FROM ACCOUNT_CONTACT_T AC
         WHERE AC.CONTACT_ID = r_abn.brm_dlv_address_id
        ;
        IF v_count = 0 THEN
            INSERT INTO ACCOUNT_CONTACT_T (   
                CONTACT_ID,ADDRESS_TYPE,ACCOUNT_ID,
                COUNTRY,ZIP,STATE,CITY,ADDRESS, EMAIL, DATE_FROM,
                NOTES
            )VALUES(
                r_abn.brm_dlv_address_id, PK00_CONST.c_ADDR_TYPE_DLV, r_abn.brm_account_id,
                'РФ', r_abn.phis_zip, NULL, 
                r_abn.phis_city, r_abn.phis_address, r_abn.email_addr, r_abn.custdate, 
                'импортировано из Portal65-XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );  
        END IF;
    
        -- -------------------------------------------------------- --
        -- счетчик успешно загруженных записей
        v_ok := v_ok + 1;
        
        UPDATE PK212_PINDB_ALL_CONTRACTS_T
           SET LOAD_CODE   = v_step,
               LOAD_STATUS = 'OK'
         WHERE CURRENT OF c_CTR;
        
        IF MOD(v_ok, 100) = 0 THEN
            Pk01_Syslog.Write_msg(v_ok||' - ок, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
      EXCEPTION
         -- -------------------------------------------------------- --
         -- обработка ошибки загрузки записи 
         WHEN OTHERS THEN
            v_load_status := 'ERROR, шаг => '||view_step(v_step)||'. '
                            ||Pk01_Syslog.get_OraErrTxt(c_PkgName||'.'||v_prcName);
            UPDATE PK212_PINDB_ALL_CONTRACTS_T
               SET LOAD_STATUS = v_load_status,
                   LOAD_CODE   = -v_step
             WHERE CURRENT OF c_CTR;

            Pk01_Syslog.Write_msg('contract_no='||r_abn.CONTRACT_NO||
                                ', account_no=' ||r_abn.ACCOUNT_NO||
                                ' => '||v_load_status
                                , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );            

            v_error := v_error + 1;
      END;
    END LOOP;

    Pk01_Syslog.Write_msg('Report: '||v_ok||' - ок, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE PK212_PINDB_ALL_CONTRACTS_T SET LOAD_DATE = SYSDATE;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Загрузка данных по лицевым счетам
--============================================================================================
PROCEDURE Load_orders
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Load_orders';
    v_count           INTEGER := 0;
    v_step            INTEGER := 1;
    v_ok              INTEGER := 0;
    v_error           INTEGER := 0;
    v_load_status     VARCHAR2(1000);
   
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR r_ord IN c_ORD LOOP
      
      BEGIN
        -- -------------------------------------------------------- --
        -- Создаем заказ, услуга определена ранее
        -- -------------------------------------------------------- --
        v_step := c_LOAD_CODE_ORD;
        SELECT COUNT(*) INTO v_count
          FROM ORDER_T O
         WHERE O.ORDER_ID = r_ord.brm_order_id
        ;
        IF v_count = 0 THEN
            -- создаем запись заказа
            INSERT INTO ORDER_T (
               ORDER_ID, ORDER_NO, ACCOUNT_ID, SERVICE_ID, RATEPLAN_ID, 
               DATE_FROM, DATE_TO, 
               CREATE_DATE, MODIFY_DATE, 
               TIME_ZONE, NOTES
            )VALUES(
               r_ord.brm_order_id, r_ord.brm_order_no, r_ord.brm_account_id, 
               r_ord.brm_service_id, NULL,
               NVL(r_ord.order_date, r_ord.cycle_start_t),
               CASE 
                 WHEN r_ord.cycle_end_t IS NULL THEN TO_DATE('01.01.2050','dd.mm.yyyy')
                 ELSE r_ord.cycle_end_t-1/86400
               END,
               SYSDATE, SYSDATE, NULL, 
               'импортировано из Portal65-XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
            );
            -- Добавляем информацию о точках подключения
            INSERT INTO ORDER_INFO_T( 
                   ORDER_ID, POINT_SRC, POINT_DST, 
                   SPEED_STR, SPEED_VALUE, SPEED_UNIT_ID, 
                   DOWNTIME_FREE )
            VALUES( 
                   r_ord.brm_order_id, r_ord.s_rgn, r_ord.d_rgn, 
                   r_ord.speed_str, r_ord.brm_speed_value, r_ord.brm_speed_unit_id,
                   r_ord.free_downtime );
        END IF;

        -- -------------------------------------------------------- --
        -- сохраняем наименование услуги, как она написана в заказе
        -- -------------------------------------------------------- --
        /*
        v_step := c_LOAD_CODE_SAL;
        IF r_abn.SERVICE_ALIAS IS NOT NULL AND r_abn.SERVICE != r_abn.SERVICE_ALIAS THEN
            SELECT COUNT(*) INTO v_count
              FROM SERVICE_ALIAS_T SA
             WHERE SA.ACCOUNT_ID = r_abn.account_id
               AND SA.SERVICE_ID = r_abn.service_id
            ;
            IF v_count = 0 THEN 
                INSERT INTO SERVICE_ALIAS_T (SERVICE_ID, ACCOUNT_ID, SRV_NAME)
                VALUES(r_abn.service_id, r_abn.account_id, r_abn.SERVICE_ALIAS);
            END IF;
        END IF;
        */
        -- -------------------------------------------------------- --
        -- Добавляем компоненты услуги
        -- -------------------------------------------------------- --
        -- абонплата
        IF r_ord.brm_rec_ob_id IS NOT NULL THEN
            -- создаем компоненту услуги абонплата
            v_step := c_LOAD_CODE_ABP;
            INSERT INTO ORDER_BODY_T(
                ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
                DATE_FROM, DATE_TO, 
                RATE_VALUE, RATE_LEVEL_ID, TAX_INCL, CURRENCY_ID,
                QUANTITY, RATE_RULE_ID
            ) VALUES (
                r_ord.brm_rec_ob_id, r_ord.brm_order_id, 
                r_ord.brm_rec_subservice_id, Pk00_Const.c_CHARGE_TYPE_REC,
                r_ord.cycle_start_t, 
                CASE
                  WHEN r_ord.cycle_end_t IS NULL THEN Pk00_Const.c_DATE_MAX
                  ELSE r_ord.cycle_end_t-1/86400
                END, 
                r_ord.cycle_fee_amt, Pk00_Const.c_RATE_LEVEL_ORDER, 
                'N', r_ord.brm_currency_id, 
                1, Pk00_Const.c_RATE_RULE_ABP_STD
            );
        END IF;
        -- трафик 
        IF r_ord.brm_usg_ob_id IS NOT NULL THEN
            -- создаем компоненту услуги трафик
            v_step := c_LOAD_CODE_USG;
            INSERT INTO ORDER_BODY_T(
                ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
                DATE_FROM, DATE_TO, 
                TAX_INCL, CURRENCY_ID,
                QUANTITY, RATE_RULE_ID
            ) VALUES (
                r_ord.brm_usg_ob_id, r_ord.brm_order_id, 
                r_ord.brm_usg_subservice_id, Pk00_Const.c_CHARGE_TYPE_USG,
                r_ord.cycle_start_t, 
                CASE
                  WHEN r_ord.cycle_end_t IS NULL THEN Pk00_Const.c_DATE_MAX
                  ELSE r_ord.cycle_end_t-1/86400
                END,
                'N', r_ord.brm_currency_id, 
                1, r_ord.brm_usg_raterule_id
            );
        END IF;

        -- -------------------------------------------------------- --
        -- счетчик успешно загруженных записей
        v_ok := v_ok + 1;
        
        UPDATE PK212_PINDB_ORDERS_T
           SET LOAD_CODE   = v_step,
               LOAD_STATUS = 'OK'
         WHERE CURRENT OF c_ORD;
        
        IF MOD(v_ok, 100) = 0 THEN
            Pk01_Syslog.Write_msg(v_ok||' - ок, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
      EXCEPTION
         -- -------------------------------------------------------- --
         -- обработка ошибки загрузки записи 
         WHEN OTHERS THEN
            v_load_status := 'ERROR, шаг => '||view_step(v_step)||'. '
                            ||Pk01_Syslog.get_OraErrTxt(c_PkgName||'.'||v_prcName);
            UPDATE PK212_PINDB_ORDERS_T
               SET LOAD_STATUS = v_load_status,
                   LOAD_CODE   = -v_step
             WHERE CURRENT OF c_ORD;

            Pk01_Syslog.Write_msg('contract_no='||r_ord.CONTRACT_NO||
                                ', account_no=' ||r_ord.ACCOUNT_NO||
                                ', order_no='   ||r_ord.ORDER_NO||
                                ' => '||v_load_status
                                , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );            

            v_error := v_error + 1;
      END;
    END LOOP;

    Pk01_Syslog.Write_msg('Report: '||v_ok||' - ок, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    UPDATE PK212_PINDB_ORDERS_T SET LOAD_DATE = SYSDATE;
    
    -- - - - - - - - - - - - - - - -- - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Перенести загруженные данные в архив
--============================================================================================
PROCEDURE Move_to_archive
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Move_to_archive';
    v_count           INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - -- - - - - - - - - - - - - - - - - - - - - - - - --
    INSERT INTO PK212_PINDB_ALL_CONTRACTS_ARX
    SELECT * FROM PK212_PINDB_ALL_CONTRACTS_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK212_PINDB_ALL_CONTRACTS_ARX '||v_count||' - rows insert', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    
    INSERT INTO PK212_PINDB_ORDERS_ARX
    SELECT * FROM PK212_PINDB_ORDERS_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK212_PINDB_ORDERS_ARX '||v_count||' - rows insert', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - -- - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

-- =========================================================== --
-- Формирование счетов
-- =========================================================== --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Создание периодических счетов для клиентов имеющих
-- абонплату или доплату до минимальной стоимости
-- в биллинговом периоде p_period_id
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Create_new_bills(p_period_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Create_new_bills';
    v_bill_id       INTEGER;
    v_count         INTEGER;
    v_error         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- создаем описатели счетов где их нет
    v_count := 0;
    FOR bi IN (
      SELECT DISTINCT X.BRM_ACCOUNT_ID  
        FROM PK212_PINDB_ALL_CONTRACTS_T X
       WHERE X.LOAD_CODE > 0
         AND NOT EXISTS (
            SELECT * FROM BILLINFO_T BI
             WHERE BI.ACCOUNT_ID = X.BRM_ACCOUNT_ID
         )
    )
    LOOP
       -- создаем описатель счетов и способ доставки счета
       Pk07_Bill.New_billinfo (
                   p_account_id    => bi.brm_account_id,   -- ID лицевого счета
                   p_currency_id   => Pk00_Const.c_CURRENCY_RUB,  -- ID валюты счета
                   p_delivery_id   => c_DLV_METHOD_AP,-- ID способа доставки счета
                   p_days_for_payment => 30           -- кол-во дней на оплату счета
               );  
    
       v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('Billinfo_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создаем периодические счета в биллинговом периоде для л/с где их нет
    v_count := 0;
    v_error := 0;
    --
    FOR rb IN (
      SELECT DISTINCT X.BRM_ACCOUNT_ID  
        FROM PK212_PINDB_ALL_CONTRACTS_T X
       WHERE X.LOAD_CODE > 0
         AND NOT EXISTS (
            SELECT * FROM BILL_T B
             WHERE B.ACCOUNT_ID    = X.BRM_ACCOUNT_ID
               AND B.REP_PERIOD_ID = p_period_id
               AND B.BILL_TYPE     = PK00_CONST.c_BILL_TYPE_REC
         )
    )LOOP
      BEGIN
      v_bill_id := Pk07_BIll.Next_recuring_bill (
               p_account_id    => rb.brm_account_id, -- ID лицевого счета
               p_rep_period_id => p_period_id    -- ID расчетного периода YYYYMM
           );
      EXCEPTION WHEN OTHERS THEN
          Pk01_Syslog.Write_error( 'ERROR', c_PkgName||'.'||v_prcName );
          v_error := v_error + 1;
      END;
      v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('Bill_t: '||v_count||' rows created, '||v_error||' - error', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- сформировать счета клиентов Юр.лиц регионов (РП блок Магистраль) 
-- ------------------------------------------------------------------------- --
PROCEDURE Make_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_bills';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- корректируем тип л/с
    UPDATE ACCOUNT_T A SET STATUS = 'B'
     WHERE A.BILLING_ID = 2008
       AND A.STATUS != 'B';
    
    Pk30_Billing.Make_Region_bills(p_period_id);
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


END PK212_XTTK_IMPORT_PORTAL65;
/
