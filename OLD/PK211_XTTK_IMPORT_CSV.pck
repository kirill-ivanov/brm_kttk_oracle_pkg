CREATE OR REPLACE PACKAGE PK211_XTTK_IMPORT_CSV
IS
    --
    -- Пакет для создания клиентов блока Магистраль
    -- импортированных в виде файла *.csv из ХТТК
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK211_XTTK_IMPORT_CSV';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- =====================================================================
    -- биллинг в который загружаем клиентов, для проверки перед импортом
    c_BILLING_XTTK        CONSTANT INTEGER := 2008; -- Биллинг клиетнов блока Магистраль из ХТТК
    c_LOAD_CODE_START     CONSTANT INTEGER := 0;    -- загрузка
    c_LOAD_CODE_PROGRESS  CONSTANT INTEGER := 1;    -- работа
    c_LOAD_CODE_OK        CONSTANT INTEGER := 2;    -- ОК
    c_LOAD_CODE_ERR       CONSTANT INTEGER :=-1;    -- Ошибка
    c_LOAD_CODE_DBL       CONSTANT INTEGER :=-2;    -- Данные уже есть в BRM

    -- способ доставки АккордПост
    c_DLV_METHOD_AP CONSTANT INTEGER := 6512;   -- АккордПост

    c_CONTRACTOR_ID       CONSTANT INTEGER := 1;

    -- Макрорегионы (xttk_id)
    c_mbl CONSTANT INTEGER := 1524374; -- «Макрорегион Байкал»
    c_mvv CONSTANT INTEGER := 1524395; -- «Макрорегион Верхневолжский»
    c_mdv CONSTANT INTEGER := 1524387; -- «Макрорегион Дальний Восток»
    c_mkz CONSTANT INTEGER := 1524389; -- «Макрорегион Кавказ»
    c_mkl CONSTANT INTEGER := 1524391; -- «Макрорегион Калининград»
    c_msp CONSTANT INTEGER := 1524399; -- «Макрорегион СПАРК»
    c_msh CONSTANT INTEGER := 1524393; -- «Макрорегион Сахалин»
    c_msv CONSTANT INTEGER := 1524380; -- «Макрорегион Север»
    c_msz CONSTANT INTEGER := 1520993; -- «Макрорегион Северо-Запад»
    c_msr CONSTANT INTEGER := 1524376; -- «Макрорегион Средневолжский»
    c_mur CONSTANT INTEGER := 1524365; -- «Макрорегион Урал»
    c_mct CONSTANT INTEGER := 1524378; -- «Макрорегион Центр»
    c_mch CONSTANT INTEGER := 1524397; -- «Макрорегион Чита»
    c_muv CONSTANT INTEGER := 1524383; -- «Макрорегион Юго-Восток»
    c_muu CONSTANT INTEGER := 1524385; -- «Макрорегион Южный Урал»

    -- Агенты региональных продуктов (agent_id)
    c_ag_mbl CONSTANT INTEGER := 297; -- «Макрорегион Байкал» (РП)
    c_ag_mvv CONSTANT INTEGER := 298; -- «Макрорегион Верхневолжский» (РП)
    c_ag_mdv CONSTANT INTEGER := 299; -- «Макрорегион Дальний Восток» (РП)
    c_ag_mkz CONSTANT INTEGER := 300; -- «Макрорегион Кавказ» (РП)
    c_ag_mkl CONSTANT INTEGER := 301; -- «Макрорегион Калининград» (РП)
    c_ag_msp CONSTANT INTEGER := 302; -- «Макрорегион СПАРК» (РП)
    c_ag_msh CONSTANT INTEGER := 303; -- «Макрорегион Сахалин» (РП)
    c_ag_msv CONSTANT INTEGER := 304; -- «Макрорегион Север» (РП)
    c_ag_msz CONSTANT INTEGER := 305; -- «Макрорегион Северо-Запад» (РП)
    c_ag_msr CONSTANT INTEGER := 306; -- «Макрорегион Средневолжский» (РП)
    c_ag_mur CONSTANT INTEGER := 307; -- «Макрорегион Урал» (РП)
    c_ag_mct CONSTANT INTEGER := 308; -- «Макрорегион Центр» (РП)
    c_ag_mch CONSTANT INTEGER := 309; -- «Макрорегион Чита» (РП)
    c_ag_muv CONSTANT INTEGER := 310; -- «Макрорегион Юго-Восток» (РП)
    c_ag_muu CONSTANT INTEGER := 311; -- «Макрорегион Южный Урал» (РП)

    -- Банки филиалов (bank_id)
    c_bank_mbl CONSTANT INTEGER := 1524375; -- «Макрорегион Байкал»
    c_bank_mvv CONSTANT INTEGER := 1524396; -- «Макрорегион Верхневолжский»
    c_bank_mdv CONSTANT INTEGER := 1524388; -- «Макрорегион Дальний Восток»
    c_bank_mkz CONSTANT INTEGER := 1524390; -- «Макрорегион Кавказ»
    c_bank_mkl CONSTANT INTEGER := 1524392; -- «Макрорегион Калининград»
    c_bank_msp CONSTANT INTEGER := 1524400; -- «Макрорегион СПАРК»
    c_bank_msh CONSTANT INTEGER := 1524394; -- «Макрорегион Сахалин»
    c_bank_msv CONSTANT INTEGER := 1524381; -- «Макрорегион Север»
    c_bank_msz CONSTANT INTEGER := 10;      -- «Макрорегион Северо-Запад»
    c_bank_msr CONSTANT INTEGER := 1524377; -- «Макрорегион Средневолжский»
    c_bank_mur CONSTANT INTEGER := 1524367; -- «Макрорегион Урал»
    c_bank_mct CONSTANT INTEGER := 1524379; -- «Макрорегион Центр»
    c_bank_mch CONSTANT INTEGER := 1524398; -- «Макрорегион Чита»
    c_bank_muv CONSTANT INTEGER := 1524384; -- «Макрорегион Юго-Восток»
    c_bank_muu CONSTANT INTEGER := 1524386; -- «Макрорегион Южный Урал»

    -- описание строки импортированной информации    
    CURSOR c_FILE_CSV IS (
       SELECT 
          ERP_CODE,         -- Код ЕИСУП    
          CLIENT,           -- Клиент (юр. название)    
          CONTRACT_NO,      -- Договор    
          CONTRACT_DATE,    -- Дата договора    
          ACCOUNT_NO,       -- номер Л/С    
          INN,              -- инн    
          KPP,              -- кпп    
          -- +юр. адрес
          JUR_ZIP,          -- индекс    
          JUR_REGION,       -- область/регион    
          JUR_CITY,         -- город    
          JUR_ADDRESS,      -- адрес                
          -- +адрес для доставки счетов                                                            
          DLV_ZIP,          -- индекс    
          DLV_REGION,       -- область/регион    
          DLV_CITY,         -- город    
          DLV_ADDRESS,      -- адрес    
          ORDER_NO,         -- Номер заказа    
          ORDER_DATE,       -- дата заказа    
          SERVICE,          -- Услуга из продукт-каталога ТТК (из перечня услуг ОМ)    
          SERVICE_ALIAS,    -- Название услуги по договору    
          POINT_SRC,        -- точка подключения 1    
          POINT_DST,        -- точка подключения 2    
          SPEED,            -- скорость     
          ABP_VALUE,        -- абон. плата     
          QUANTITY,         -- Кол-во    
          MANAGER,          -- Менеджер    
          NOTES,            -- Комментарий    
          ACCOUNT_ID, 
          PROFILE_ID, 
          CONTRACT_ID, 
          CLIENT_ID,
          CUSTOMER_ID,
          CONTRACTOR_ID,
          CONTRACTOR_BANK_ID,
          XTTK_ID,
          AGENT_ID,
          JUR_ADDRESS_ID,
          DLV_ADDRESS_ID,
          ORDER_ID, 
          SERVICE_ID,
          ORDER_BODY_ID,
          ORDER_BODY_2_ID,
          MANAGER_ID,
          ABP_NUMBER,
          REGION,
          LOAD_STATUS,
          LOAD_CODE
         FROM PK211_XTTK_IMPORT_T 
        WHERE LOAD_CODE = c_LOAD_CODE_PROGRESS
    )FOR UPDATE;
    
    --============================================================================================
    -- После полного тестирования переводим л/с из c_BILLING_NPL -> Pk00_Const.c_BILLING_OLD
    --============================================================================================
    PROCEDURE Change_billing_id;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- загрузка данных из временной таблицы PK211_XTTK_IMPORT_TMP
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Import_data(p_seller_id IN INTEGER);

    -- Загрузка информации о лицевых счетах клиентов
    PROCEDURE Load_data;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Импорт данных по регионам
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- «Макрорегион Верхневолжский» XTTK-НН 
    PROCEDURE Import_mvv;
    
END PK211_XTTK_IMPORT_CSV;
/
CREATE OR REPLACE PACKAGE BODY PK211_XTTK_IMPORT_CSV
IS

--============================================================================================
-- После полного тестирования переводим л/с из 2007 -> ...
--============================================================================================
PROCEDURE Change_billing_id
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Check_data';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE ACCOUNT_T A
       SET A.BILLING_ID = 2003
     WHERE A.BILLING_ID = 2007;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T.BILLING_ID: '||v_count||' rows c_BILLING_NPL -> c_BILLING_OLD', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- загрузка данных из временной таблицы PK211_XTTK_IMPORT_TMP
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Import_data(p_seller_id IN INTEGER)
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_data';
    v_count          INTEGER := 0;
    v_contractor_id  INTEGER;
    v_xttk_id        INTEGER;
    v_agent_id       INTEGER;
    v_bank_id        INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, seller_id=' ||p_seller_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  
                               
    -- ==========================================================================--
    -- связь продавцов с регионом
    SELECT CONTRACTOR_ID, XTTK_ID, AGENT_ID, BANK_ID --, ERP_CODE, CONTRACTOR
      INTO v_contractor_id, v_xttk_id, v_agent_id, v_bank_id
      FROM (
        SELECT CT.CONTRACTOR_ID, CT.XTTK_ID, CA.CONTRACTOR_ID AGENT_ID, CB.BANK_ID, CT.ERP_CODE, CT.CONTRACTOR,
               ROW_NUMBER() OVER (PARTITION BY CT.CONTRACTOR_ID ORDER BY CB.BANK_ID) RN  
          FROM CONTRACTOR_T CT, CONTRACTOR_BANK_T CB, CONTRACTOR_T CA 
         WHERE CT.CONTRACTOR_TYPE  = 'SELLER'
           AND CT.CONTRACTOR_ID = CB.CONTRACTOR_ID
           AND CA.PARENT_ID = CT.CONTRACTOR_ID
           AND CA.CONTRACTOR LIKE '%(РП)'
           AND CT.CONTRACTOR_ID = p_seller_id
    )
    WHERE RN = 1
    ORDER BY CONTRACTOR;
    
    Pk01_Syslog.Write_msg('contractor_id=' ||v_contractor_id||
                          ', xttk_id='  ||v_xttk_id||
                          ', agent_id=' ||v_agent_id||
                          ', bank_id='  ||v_bank_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
                               
/*
    -- переносим данные из временной таблицы
    INSERT INTO PK211_XTTK_IMPORT_T (
        ERP_CODE,CLIENT,    
        CONTRACT_NO, CONTRACT_DATE,    
        ACCOUNT_NO,    
        INN,KPP,    
        JUR_ZIP, JUR_REGION, JUR_CITY, JUR_ADDRESS,                
        DLV_ZIP, DLV_REGION, DLV_CITY, DLV_ADDRESS,    
        ORDER_NO, ORDER_DATE,SERVICE,SERVICE_ALIAS,
        POINT_SRC,POINT_DST,SPEED, ABP_VALUE,QUANTITY,    
        MANAGER, NOTES, 
        CONTRACTOR_ID, XTTK_ID, AGENT_ID, CONTRACTOR_BANK_ID, LOAD_CODE
    )
    SELECT 
        ERP_CODE,CLIENT,    
        CONTRACT_NO, TO_DATE(CONTRACT_DATE,'dd.mm.yyyy') CONTRACT_DATE,    
        ACCOUNT_NO,    
        INN,KPP,    
        JUR_ZIP, JUR_REGION, JUR_CITY, JUR_ADDRESS,
        DLV_ZIP, DLV_REGION, DLV_CITY, DLV_ADDRESS,
        ORDER_NO, ORDER_DATE,SERVICE,SERVICE_ALIAS,
        POINT_SRC,POINT_DST,SPEED, 
        ABP_VALUE,
        QUANTITY,
        MANAGER, NOTES,
        v_contractor_id, v_xttk_id, v_agent_id, v_bank_id, c_LOAD_CODE_START
    FROM PK211_XTTK_IMPORT_TMP;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
*/

    UPDATE PK211_XTTK_IMPORT_T X
       SET X.CONTRACTOR_ID      = v_contractor_id,
           X.CONTRACTOR_BANK_ID = v_bank_id,
           X.XTTK_ID            = v_xttk_id, 
           X.AGENT_ID           = v_agent_id, 
           X.LOAD_CODE          = c_LOAD_CODE_START
     WHERE X.LOAD_CODE IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- переводим абонплату в число, в файле может быть что угодно
    UPDATE PK211_XTTK_IMPORT_T 
       SET ABP_NUMBER = TO_NUMBER(REPLACE(REPLACE(RTRIM(ABP_VALUE,'р. '),',','.'),' ',''))
     WHERE LTRIM(RTRIM(ABP_VALUE,'р. '),'1234567890., ') IS NULL 
       AND INSTR(ABP_VALUE,',',1,2) = 0
       AND ABP_NUMBER IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T.ABP_NUMBER '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- фиксируем ошибки
    UPDATE PK211_XTTK_IMPORT_T X
       SET X.LOAD_CODE = c_LOAD_CODE_ERR, 
           X.LOAD_STATUS = 'Не числовое значение в поле ABP_VALUE'
     WHERE ABP_NUMBER IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T.ABP_NUMBER '||v_count||' rows error', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- проверяем пересечение по CONTRACT_NO
    MERGE INTO PK211_XTTK_IMPORT_T X
    USING (
        SELECT C.CONTRACT_ID, C.CONTRACT_NO 
          FROM CONTRACT_T C, PK211_XTTK_IMPORT_T X
         WHERE X.CONTRACT_NO = C.CONTRACT_NO 
           AND X.LOAD_CODE   = c_LOAD_CODE_START 
           AND X.CONTRACTOR_ID = v_contractor_id
    ) C
    ON(
       X.CONTRACT_NO = C.CONTRACT_NO
    )
    WHEN MATCHED THEN UPDATE SET X.CONTRACT_ID = C.CONTRACT_ID, 
                                 X.LOAD_CODE   = c_LOAD_CODE_DBL,
                                 X.LOAD_STATUS = 'Задвоение номеров договоров';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T.CONTRACT_NO '||v_count||' rows exists', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- проверяем пересечение по ACCOUNT_NO
    MERGE INTO PK211_XTTK_IMPORT_T X
    USING (
        SELECT A.ACCOUNT_ID, A.ACCOUNT_NO 
          FROM ACCOUNT_T A, PK211_XTTK_IMPORT_T X
         WHERE X.ACCOUNT_NO  = A.ACCOUNT_NO 
           AND X.LOAD_CODE   = c_LOAD_CODE_START
           AND X.CONTRACTOR_ID = v_contractor_id 
    ) A
    ON(
        X.ACCOUNT_NO  = A.ACCOUNT_NO
    )
    WHEN MATCHED THEN UPDATE SET X.ACCOUNT_ID  = A.ACCOUNT_ID, 
                                 X.LOAD_CODE   = c_LOAD_CODE_DBL,
                                 X.LOAD_STATUS = 'Задвоение номеров л/с';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T.ACCOUNT_NO '||v_count||' rows exists', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- проверяем пересечение по ORDER_NO
    MERGE INTO PK211_XTTK_IMPORT_T X
    USING (
        SELECT O.ORDER_ID, O.ORDER_NO 
          FROM ORDER_T O, PK211_XTTK_IMPORT_T X
         WHERE X.ORDER_NO  = O.ORDER_NO
           AND X.LOAD_CODE = c_LOAD_CODE_START
           AND X.CONTRACTOR_ID = v_contractor_id
    ) O
    ON(
        X.ORDER_NO  = O.ORDER_NO
    )
    WHEN MATCHED THEN UPDATE SET X.ORDER_ID = O.ORDER_ID, 
                                 X.LOAD_CODE   = c_LOAD_CODE_DBL,
                                 X.LOAD_STATUS = 'Задвоение номеров заказов';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK211_XTTK_IMPORT_T.ORDER_NO '||v_count||' rows exists', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE PK211_XTTK_IMPORT_T X SET X.LOAD_CODE = c_LOAD_CODE_PROGRESS
     WHERE X.LOAD_CODE = c_LOAD_CODE_START
       AND X.CONTRACTOR_ID = v_contractor_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. PK211_XTTK_IMPORT_T '||v_count||' rows ok', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Импорт данных по регионам
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- «Макрорегион Верхневолжский» XTTK-НН 
PROCEDURE Import_mvv IS
BEGIN
    Import_data(p_seller_id => c_mvv);
END;



--============================================================================================
-- Загрузка данных
--============================================================================================
PROCEDURE Load_data
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Load_data';
    v_count           INTEGER := 0;
    v_error           INTEGER := 0;
    v_contract_id     INTEGER;
    v_client_id       INTEGER;
    v_customer_id     INTEGER;
    v_profile_id      INTEGER;
    v_account_id      INTEGER;
    v_account_no      ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_jur_address_id  INTEGER;
    v_dlv_address_id  INTEGER;
    v_order_id        INTEGER;
    v_rec_ob_id       INTEGER;
    v_service_id      INTEGER;
    v_manager_id      INTEGER;    
    v_m_last_name     MANAGER_T.LAST_NAME%TYPE;
    v_m_first_name    MANAGER_T.FIRST_NAME%TYPE;
    v_m_middle_name   MANAGER_T.MIDDLE_NAME%TYPE;
    v_load_status     VARCHAR2(1000);
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR r_abn IN c_FILE_CSV LOOP
      
      BEGIN
        -- -------------------------------------------------------- --
        -- создаем Л/С
        -- -------------------------------------------------------- --
        SELECT MIN(ACCOUNT_ID), MIN(ACCOUNT_NO), COUNT(*) 
          INTO v_account_id, v_account_no,  v_count
          FROM ACCOUNT_T
         WHERE EXTERNAL_NO = r_abn.ACCOUNT_NO
        ;
        
        IF v_account_id IS NULL THEN
            -- номер л/с создаем в стандартном формате
            v_account_no := 'XJ'||LPAD(SQ_ACCOUNT_NO.NEXTVAL,6,'0');
            
            -- создаем л/с
            v_account_id := Pk05_Account.New_account(
                         p_account_no    => v_account_no,
                         p_account_type  => Pk00_Const.c_ACC_TYPE_J,
                         p_currency_id   => Pk00_Const.c_CURRENCY_RUB,
                         p_status        => 'NEW', --Pk00_Const.c_ACC_STATUS_BILL,
                         p_parent_id     => NULL,
                         p_notes         => 'ACCOUNT_NO='||r_abn.ACCOUNT_NO||' импортировано из XTTK'|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
                     );
             
            -- проставляем внешний идентификатор л/с (когда цифровой)
            UPDATE ACCOUNT_T
               SET EXTERNAL_NO = r_abn.ACCOUNT_NO
             WHERE ACCOUNT_ID  = v_account_id
               AND LTRIM(RTRIM(r_abn.ACCOUNT_NO,'р. '),'1234567890., ') IS NULL;

            -- проставляем принадлежность биллингу XTTK
            Pk05_Account.Set_billing(
                         p_account_id => v_account_id,
                         p_billing_id => c_BILLING_XTTK
                     );
                 
            -- создаем описатель счетов и способ доставки счета
            Pk07_Bill.New_billinfo (
                         p_account_id    => v_account_id,   -- ID лицевого счета
                         p_currency_id   => Pk00_Const.c_CURRENCY_RUB,  -- ID валюты счета
                         p_delivery_id   => c_DLV_METHOD_AP,-- ID способа доставки счета
                         p_days_for_payment => 30           -- кол-во дней на оплату счета
                     );

        END IF;
        -- сохраняем ID л/с
        UPDATE PK211_XTTK_IMPORT_T
           SET ACCOUNT_ID = v_account_id
         WHERE CURRENT OF c_FILE_CSV;
    
        -- -------------------------------------------------------- --
        -- создаем клиента договора
        -- -------------------------------------------------------- --
        SELECT MIN(CLIENT_ID), COUNT(*) 
          INTO v_client_id, v_count
          FROM CLIENT_T CL
         WHERE CL.CLIENT_NAME = r_abn.CLIENT;
        IF v_client_id IS NULL THEN
           v_client_id := PK11_CLIENT.New_client(r_abn.CLIENT);
        END IF;
        -- сохраняем ID
        UPDATE PK211_XTTK_IMPORT_T
           SET CLIENT_ID = v_client_id
         WHERE CURRENT OF c_FILE_CSV;
        
        -- -------------------------------------------------------- --
        -- создаем sale-куратора договора
        -- -------------------------------------------------------- --
        v_m_last_name   := SUBSTR(r_abn.MANAGER, 1, INSTR(r_abn.MANAGER,' ',1)-1);
        v_m_first_name  := SUBSTR(r_abn.MANAGER, INSTR(r_abn.MANAGER,' ',1)+1, 1)||'.';
        v_m_middle_name := SUBSTR(r_abn.MANAGER, INSTR(r_abn.MANAGER,'.',1)+1, 1)||'.';

        SELECT MIN(MANAGER_ID), COUNT(*) 
          INTO v_manager_id, v_count
          FROM MANAGER_T M
         WHERE M.CONTRACTOR_ID = r_abn.XTTK_ID
           AND M.LAST_NAME   = v_m_last_name
           AND M.FIRST_NAME  = v_m_last_name
           AND M.MIDDLE_NAME = v_m_middle_name
        ;

        IF v_manager_id IS NULL THEN
           v_manager_id := PK15_MANAGER.New_manager(
               p_contractor_id    => r_abn.XTTK_ID,
               p_department       => NULL,
               p_position         => NULL, 
               p_last_name        => v_m_last_name,   -- фамилия
               p_first_name       => v_m_first_name,  -- имя 
               p_middle_name      => v_m_middle_name, -- отчество
               p_phones           => NULL,
               p_email            => NULL,
               p_date_from        => TO_DATE('01.01.2000','dd.mm.yyyy'),
               p_date_to          => NULL
           );
        END IF;
        -- сохраняем ID
        UPDATE PK211_XTTK_IMPORT_T
           SET MANAGER_ID = v_manager_id
         WHERE CURRENT OF c_FILE_CSV;

        -- -------------------------------------------------------- --
        -- создаем договор
        -- -------------------------------------------------------- --
        SELECT MIN(CONTRACT_ID), COUNT(*) 
          INTO v_contract_id, v_count
          FROM CONTRACT_T C
         WHERE C.CONTRACT_NO = r_abn.CONTRACT_NO
        ;
        IF v_contract_id IS NULL THEN
            v_contract_id := Pk12_Contract.Open_contract(
               p_contract_no => r_abn.CONTRACT_NO, 
               p_date_from   => r_abn.CONTRACT_DATE,
               p_date_to     => Pk00_Const.c_DATE_MAX,
               p_client_id   => v_client_id,
               p_manager_id  => v_manager_id
            );
        END IF;
        -- сохраняем ID
        UPDATE PK211_XTTK_IMPORT_T
           SET CONTRACT_ID = v_contract_id
         WHERE CURRENT OF c_FILE_CSV;
    
        -- -------------------------------------------------------- --
        -- создаем покупателя
        -- -------------------------------------------------------- --
        SELECT MIN(CUSTOMER_ID), COUNT(*) 
          INTO v_customer_id, v_count
          FROM CUSTOMER_T CS
         WHERE CS.ERP_CODE = r_abn.ERP_CODE
           AND CS.INN      = r_abn.INN
           AND CS.KPP      = r_abn.KPP
        ;
        IF v_customer_id IS NULL THEN
           v_customer_id := Pk13_Customer.New_customer(
               p_erp_code    => r_abn.ERP_CODE,
               p_inn         => r_abn.INN,
               p_kpp         => r_abn.KPP, 
               p_name        => r_abn.CLIENT,
               p_short_name  => r_abn.CLIENT,
               p_notes       => 'импортировано из XTTK '||TO_CHAR(SYSDATE,'dd.mm.yyyy')
           );
        END IF;
        -- сохраняем ID
        UPDATE PK211_XTTK_IMPORT_T
           SET CUSTOMER_ID = v_customer_id
         WHERE CURRENT OF c_FILE_CSV;
    
        -- -------------------------------------------------------- --
        -- Создаем профиль л/с
        -- -------------------------------------------------------- --
        IF r_abn.PROFILE_ID IS NULL THEN
            v_profile_id := Pk05_Account.Set_profile(
                 p_account_id         => v_account_id,
                 p_brand_id           => NULL,
                 p_contract_id        => v_contract_id,
                 p_customer_id        => v_customer_id,
                 p_subscriber_id      => NULL,
                 p_contractor_id      => r_abn.CONTRACTOR_ID,
                 p_branch_id          => r_abn.XTTK_ID,
                 p_agent_id           => r_abn.AGENT_ID,
                 p_contractor_bank_id => r_abn.CONTRACTOR_BANK_ID,
                 p_vat                => Pk00_Const.c_VAT,
                 p_date_from          => r_abn.CONTRACT_DATE,
                 p_date_to            => NULL
             );
            -- сохраняем ID
            UPDATE PK211_XTTK_IMPORT_T
               SET PROFILE_ID = v_profile_id
             WHERE CURRENT OF c_FILE_CSV;    
        ELSE
            v_profile_id := r_abn.PROFILE_ID;
        END IF;
    
        -- -------------------------------------------------------- --
        -- Создаем юридический адрес
        -- -------------------------------------------------------- --
        IF r_abn.JUR_ADDRESS_ID IS NULL THEN
            SELECT MIN(AC.CONTACT_ID), COUNT(*) 
              INTO v_jur_address_id, v_count
              FROM ACCOUNT_CONTACT_T AC
             WHERE AC.ACCOUNT_ID   = v_account_id
               AND AC.ADDRESS_TYPE = PK00_CONST.c_ADDR_TYPE_JUR
            ;
            IF v_jur_address_id IS NULL THEN
                v_jur_address_id := PK05_ACCOUNT.Add_address(
                            p_account_id    => v_account_id,
                            p_address_type  => PK00_CONST.c_ADDR_TYPE_JUR,
                            p_country       => 'РФ',
                            p_zip           => r_abn.JUR_ZIP,
                            p_state         => r_abn.JUR_REGION,
                            p_city          => r_abn.JUR_CITY,
                            p_address       => r_abn.JUR_ADDRESS,
                            p_person        => NULL,
                            p_phones        => NULL,
                            p_fax           => NULL,
                            p_email         => NULL,
                            p_date_from     => r_abn.CONTRACT_DATE,
                            p_date_to       => NULL,
                            p_notes         => 'импортировано из XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
                       );
            END IF;
            -- сохраняем ID
            UPDATE PK211_XTTK_IMPORT_T
               SET JUR_ADDRESS_ID = v_jur_address_id
             WHERE CURRENT OF c_FILE_CSV;
        ELSE
            v_jur_address_id := r_abn.JUR_ADDRESS_ID;
        END IF;
    
        -- -------------------------------------------------------- --
        -- Создаем адрес доставки
        -- -------------------------------------------------------- --
        IF r_abn.DLV_ADDRESS_ID IS NULL THEN
            SELECT MIN(AC.CONTACT_ID), COUNT(*) 
              INTO v_dlv_address_id, v_count
              FROM ACCOUNT_CONTACT_T AC
             WHERE AC.ACCOUNT_ID   = v_account_id
               AND AC.ADDRESS_TYPE = PK00_CONST.c_ADDR_TYPE_JUR
            ;
            IF v_dlv_address_id IS NULL THEN  
                v_dlv_address_id := PK05_ACCOUNT.Add_address(
                            p_account_id    => v_account_id,
                            p_address_type  => PK00_CONST.c_ADDR_TYPE_DLV,
                            p_country       => 'РФ',
                            p_zip           => r_abn.DLV_ZIP,
                            p_state         => r_abn.DLV_REGION,
                            p_city          => r_abn.DLV_CITY,
                            p_address       => r_abn.DLV_ADDRESS,
                            p_person        => NULL,
                            p_phones        => NULL,
                            p_fax           => NULL,
                            p_email         => NULL,
                            p_date_from     => r_abn.CONTRACT_DATE,
                            p_date_to       => NULL,
                            p_notes         => 'импортировано из XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
                       );
            END IF;
            -- сохраняем ID
            UPDATE PK211_XTTK_IMPORT_T
               SET DLV_ADDRESS_ID = v_dlv_address_id
             WHERE CURRENT OF c_FILE_CSV;
        ELSE
            v_dlv_address_id := r_abn.DLV_ADDRESS_ID;
        END IF;
    
        -- -------------------------------------------------------- --
        -- определяем услугу (нет - исключение)
        -- -------------------------------------------------------- --
        SELECT S.SERVICE_ID INTO v_service_id 
          FROM SERVICE_T S
         WHERE S.SERVICE = r_abn.SERVICE;
        
        UPDATE PK211_XTTK_IMPORT_T
           SET SERVICE_ID = v_service_id
         WHERE CURRENT OF c_FILE_CSV;
        
        -- -------------------------------------------------------- --
        -- Создаем заказ
        -- -------------------------------------------------------- --
        IF r_abn.ORDER_ID IS NULL THEN
            v_order_id := Pk06_Order.New_order(
               p_account_id   => v_account_id,       -- ID лицевого счета
               p_order_no     => r_abn.ORDER_NO,     -- Номер заказа, как на бумаге
               p_service_id   => v_service_id,       -- ID услуги из таблицы SERVICE_T
               p_rateplan_id  => NULL,               -- ID тарифного плана из RATEPLAN_T
               p_time_zone    => NULL,               -- GMT               
               p_date_from    => NVL(r_abn.ORDER_DATE, r_abn.CONTRACT_DATE), -- дата начала действия заказа
               p_date_to      => Pk00_Const.c_DATE_MAX,
               p_create_date  => SYSDATE,
               p_note         => 'импортировано из XTTK '|| TO_CHAR(SYSDATE,'dd.mm.yyyy') 
            );
        ELSE
            v_order_id := r_abn.ORDER_ID;
        END IF;

        UPDATE PK211_XTTK_IMPORT_T
           SET ORDER_ID = v_order_id
         WHERE CURRENT OF c_FILE_CSV;

        -- сохраняем наименование услуги, как она написана в заказе
        IF r_abn.SERVICE_ALIAS IS NOT NULL AND r_abn.SERVICE != r_abn.SERVICE_ALIAS THEN
            SELECT COUNT(*) INTO v_count
              FROM SERVICE_ALIAS_T SA
             WHERE SA.ACCOUNT_ID = v_account_id
               AND SA.SERVICE_ID = v_service_id
            ;
            IF v_count = 0 THEN 
                INSERT INTO SERVICE_ALIAS_T (SERVICE_ID, ACCOUNT_ID, SRV_NAME)
                VALUES(v_service_id, v_account_id, r_abn.SERVICE_ALIAS);
            END IF;
        END IF;

        -- -------------------------------------------------------- --
        -- Добавляем информацию о точках подключения
        -- -------------------------------------------------------- --
        INSERT INTO ORDER_INFO_T( ORDER_ID, POINT_SRC, POINT_DST, SPEED_STR )
        VALUES( v_order_id, r_abn.POINT_SRC, r_abn.POINT_DST, r_abn.SPEED);
    
        -- -------------------------------------------------------- --
        -- Добавляем компоненты услуги
        -- -------------------------------------------------------- --
        -- абонплата
        IF r_abn.ABP_NUMBER IS NOT NULL THEN
            IF r_abn.ORDER_BODY_ID IS NULL THEN
                v_rec_ob_id := Pk06_order.Add_subs_abon (
                     p_order_id      => v_order_id,               -- ID заказа - услуги
                     p_subservice_id => Pk00_Const.c_SUBSRV_REC,  -- ID компонента услуги
                     p_value         => r_abn.ABP_NUMBER,         -- сумма абонплаты
                     p_tax_incl      => 'N',                      -- включен ли налог в сумму абонплаты
                     p_currency_id   => Pk00_Const.c_CURRENCY_RUB,-- валюта
                     p_quantity      => r_abn.QUANTITY,           -- кол-во услуги в натуральном измерении
                     p_date_from     => NVL(r_abn.ORDER_DATE, r_abn.CONTRACT_DATE),
                     p_date_to       => Pk00_Const.c_DATE_MAX
                );
                UPDATE PK211_XTTK_IMPORT_T
                   SET ORDER_BODY_ID = v_rec_ob_id
                 WHERE CURRENT OF c_FILE_CSV;

                Pk06_order.Add_subs_downtime (
                     p_order_id      => v_order_id,               -- ID заказа - услуги
                     p_charge_type   => Pk00_Const.c_CHARGE_TYPE_IDL,
                     p_free_value    => 43,  -- кол-во некомпенсируемых минут простоев
                     p_descr         => NULL,
                     p_date_from     => NVL(r_abn.ORDER_DATE, r_abn.CONTRACT_DATE)
                 );  

            END IF;

        END IF;
    
        -- -------------------------------------------------------- --
        -- счетчик успешно загруженных записей
        v_count := v_count + 1;
        
        IF MOD(v_count, 100) = 0 THEN
            Pk01_Syslog.Write_msg(v_count||' - ок, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
      EXCEPTION
         -- -------------------------------------------------------- --
         -- обработка ошибки загрузки записи 
         -- -------------------------------------------------------- --
         WHEN OTHERS THEN
            v_load_status := Pk01_Syslog.get_OraErrTxt(c_PkgName||'.'||v_prcName);
            UPDATE PK211_XTTK_IMPORT_T
               SET LOAD_STATUS = v_load_status
             WHERE CURRENT OF c_FILE_CSV;

            v_error := v_error + 1;
      END;
    END LOOP;

    Pk01_Syslog.Write_msg('Report: '||v_count||' - ок, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;



END PK211_XTTK_IMPORT_CSV;
/
