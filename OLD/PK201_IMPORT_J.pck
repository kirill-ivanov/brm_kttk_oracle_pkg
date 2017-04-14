CREATE OR REPLACE PACKAGE PK201_IMPORT_J
IS
    --
    -- Пакет для поддержки импорта юридических лиц из "Нового Биллинга" 
    --
    -- ==============================================================================
    c_PkgName   CONSTANT varchar2(30) := 'PK201_IMPORT_J';
    -- ==============================================================================
    c_RET_OK    CONSTANT integer := 0;
    c_RET_ER		CONSTANT integer :=-1;
    
    TYPE t_refc IS REF CURSOR;

    --===========================================================================
    -- Синхронизация с "НОВЫМ БИЛЛИНГОМ"
    PROCEDURE Sync_customer;

    --===========================================================================
    -- Дозагрузка брендов
    PROCEDURE Load_brands;
    
    --=============================================================================--
    -- загрузка данных для импорта из НБ
    PROCEDURE Load_data;

    -- Импорт физиков
    PROCEDURE Import_Customers;

    -- ----------------------------------------------------------------------- --    
    -- Создать счета для всех Л/С для биллинговых периодов из PERIOD_T
    PROCEDURE Make_Bill_For_Periods;
    
    -- Создать описатель периода лицевого счета
    PROCEDURE New_billinfo (
                   p_account_id    IN INTEGER
               );    
    
    --=========================================================================--
    -- Откат абонентов физ.лиц
    PROCEDURE Rollback_customers;
    
    
END PK201_IMPORT_J;
/
CREATE OR REPLACE PACKAGE BODY PK201_IMPORT_J
IS

--===========================================================================
-- Синхронизация с "НОВЫМ БИЛЛИНГОМ"
--===========================================================================
PROCEDURE Sync_customer
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Sync_customer';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName);
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- дозагрузка брендов
    Load_brands;
    -- загрузка данных во временные таблицы
    Load_data;
    -- дозаргузка юриков
    Import_Customers;
    -- создание счетов для дозагруженных данных
    Make_Bill_For_Periods;
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    COMMIT;
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


--===========================================================================
-- Дозагрузка брендов
--===========================================================================
--
PROCEDURE Load_brands 
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Load_brands';
    v_count         INTEGER := 0;
    v_new_xttk      INTEGER := 0;
    v_new_agent     INTEGER := 0;
    v_kttk_id       INTEGER;
    v_xttk_id       INTEGER;
    v_agent_id      INTEGER;
    v_kttk_name     CONTRACTOR_T.SHORT_NAME%TYPE;
    v_xttk_name     CONTRACTOR_T.SHORT_NAME%TYPE;
    v_agent_name    CONTRACTOR_T.SHORT_NAME%TYPE;
    v_address_id    INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Старт', c_PkgName||'.'||v_prcName);
    -- Заполняем таблицу брендов
    DELETE FROM PK201_BRANDS;
    --
    INSERT INTO PK201_BRANDS
    SELECT a.poid_id0, a.NAME,
          (SELECT p.NAME 
             FROM group_t@mmtdb g, group_billing_members_t@mmtdb gbm, account_t@mmtdb p 
            WHERE gbm.object_id0 = a.poid_id0
              AND g.poid_id0 = gbm.obj_id0
              AND g.account_obj_id0 = p.poid_id0
          ) parent_name,
          ni.company, ni.country, ni.zip, ni.state, ni.city, ni.address
    FROM account_t@mmtdb a, account_nameinfo_t@mmtdb ni
    WHERE a.account_type = 2
      AND ni.obj_id0 = a.poid_id0
      AND a.NAME != 'Brand Host';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK201_BRANDS insert '||v_count||' rows', c_PkgName||'.'||v_prcName);
    Gather_Table_Stat(l_Tab_Name => 'PK201_BRANDS');
    --
    -- Просмотр и добавление брендов
    FOR r_brand IN (
        SELECT 
            LEVEL, NAME, PARENT_NAME, SYS_CONNECT_BY_PATH(NAME, '/') CONTRACTOR_PATH,
            COMPANY, COUNTRY, ZIP, STATE, CITY, ADDRESS
          FROM PK201_BRANDS PK201 
         WHERE NOT EXISTS (
            SELECT * 
              FROM CONTRACTOR_T C
             WHERE C.CONTRACTOR = PK201.NAME
            )
           AND PK201.NAME != 'КТТК'
        CONNECT BY PRIOR NAME = PARENT_NAME 
        START WITH PARENT_NAME IS NULL
        ORDER SIBLINGS BY NAME  
    )
    LOOP
        -- 
        IF r_brand.LEVEL = 1 THEN
            -- верхний уровень КТТК должен быть введен руками
            v_kttk_name := r_brand.NAME;
            v_xttk_name := NULL;
            v_agent_name:= NULL;
            v_xttk_id  := NULL;
            v_agent_id := NULL;
            --
        ELSIF r_brand.LEVEL = 2 THEN
            v_xttk_name := r_brand.NAME;
            v_agent_name:= NULL;
            --
            BEGIN
              SELECT C.CONTRACTOR_ID INTO v_xttk_id
                FROM CONTRACTOR_T C
               WHERE C.SHORT_NAME = r_brand.NAME;
            EXCEPTION WHEN NO_DATA_FOUND THEN
              -- создаем бренд xTTK
              v_xttk_id := Pk14_Contractor.New_contractor(
                     p_type        => 'XTTK',
                     p_erp_code    => NULL,
                     p_inn         => NULL,
                     p_kpp         => NULL, 
                     p_name        => v_xttk_name,
                     p_short_name  => v_xttk_name,
                     p_parent_id   => v_kttk_id,
                     p_notes       => NULL
                 );
              IF v_xttk_id > 0 THEN
                  v_address_id := Pk14_Contractor.Set_address(
                     p_contractor_id => v_xttk_id,
                     p_address_type  => Pk00_Const.c_ADDR_TYPE_JUR,
                     p_country       => 'РФ', 
                     p_zip           => r_brand.zip,
                     p_state         => r_brand.state,
                     p_city          => r_brand.city, 
                     p_address       => r_brand.address,
                     p_phone_account => NULL,
                     p_phone_billing => NULL,
                     p_fax           => NULL,
                     p_email         => NULL,
                     p_date_from     => TO_DATE('01.01.2013','dd.mm.yyyy'),
                     p_date_to       => NULL
                 );  
              END IF;
              Pk01_Syslog.Write_msg('Create XTTK brand '||v_xttk_name, c_PkgName||'.'||v_prcName);
              v_new_xttk := v_new_xttk + 1;
            END;
            --
        ELSIF r_brand.LEVEL = 3 THEN  
            v_agent_name:= r_brand.NAME;
            --
            BEGIN
              SELECT C.CONTRACTOR_ID INTO v_agent_id
                FROM CONTRACTOR_T C
               WHERE C.CONTRACTOR_TYPE = Pk00_Const.c_CTR_TYPE_AGENT
                 AND C.SHORT_NAME = r_brand.NAME;
            EXCEPTION WHEN NO_DATA_FOUND THEN
              -- создаем бренд АГЕНТА
              v_agent_id := Pk14_Contractor.New_contractor(
                     p_type        => 'AGENT',
                     p_erp_code    => NULL,
                     p_inn         => NULL,
                     p_kpp         => NULL, 
                     p_name        => v_agent_name,
                     p_short_name  => v_agent_name,
                     p_parent_id   => v_kttk_id,
                     p_notes       => NULL
                 );
              IF v_agent_id > 0 THEN
                  v_address_id := Pk14_Contractor.Set_address(
                     p_contractor_id => v_agent_id,
                     p_address_type  => Pk00_Const.c_ADDR_TYPE_JUR,
                     p_country       => 'РФ', 
                     p_zip           => r_brand.zip,
                     p_state         => r_brand.state,
                     p_city          => r_brand.city, 
                     p_address       => r_brand.address,
                     p_phone_account => NULL,
                     p_phone_billing => NULL,
                     p_fax           => NULL,
                     p_email         => NULL,
                     p_date_from     => TO_DATE('01.01.2013','dd.mm.yyyy'),
                     p_date_to       => NULL
                 );  
              END IF;
              Pk01_Syslog.Write_msg('Create XTTK.AGENT brand '||v_xttk_name||'.'||v_agent_name, 
                                                                c_PkgName||'.'||v_prcName);
              v_new_agent := v_new_agent + 1;
            END; 
            --
        END IF;
        --
        v_count := v_count + 1;
        -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
        -- выводим диагностику
        IF MOD(v_count, 10) = 0 THEN
            Pk01_Syslog.Write_msg('обработано '||v_count||' строк', c_PkgName||'.'||v_prcName );
        END IF;
        --
    END LOOP;
    --
    Pk01_Syslog.Write_msg('Просмотрено '||v_count||' записей, добавлено xTTK-'||v_new_xttk||
                          ', Agent-'||v_new_agent, c_PkgName||'.'||v_prcName);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


--=========================================================================--
-- Загрузка данных из НБ Микротест
--=========================================================================--
--  
PROCEDURE Load_data 
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Load_data';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Старт', c_PkgName||'.'||v_prcName);
    --
    -- Заполняем таблицу клиентов - Юридических лиц
    DELETE FROM PK201_CUSTOMER_J;
    --
    INSERT INTO PK201_CUSTOMER_J
    SELECT  
        ACCOUNT_POID_ID0,    -- POID_ID0 в новом биллинге
        ACCOUNT_NO,          -- № лицевого счета
        COMPANY,             -- наименование компании
        CONTRACT_NO,         -- № договора
        CONTRACT_DATE,       -- дата договора
        BRAND_NAME,          -- наименование бренда
        SERVICE_PROVIDER,    -- ACCOUNT_T.POID_ID0 КТТК в "Новом Биллинге"
        -- юридический адрес
        J_ZIP,
        J_REG,
        J_CITY,
        J_ADDR,
        -- фактический адрес
        B_ZIP,
        B_REG,
        B_CITY,
        SET_ADDR,
        -- адрес доставки
        D_ZIP,
        D_REG,
        D_CITY,
        D_ADDR,
        -- координаты компании, которая платит за клиента заключившего договор
        SF_COMPANY,
        SF_ADDRESS,
        SF_INN,
        SF_KPP,
        -- ИНН, КПП клиента
        S_INN,
        S_KPP,
        -- Банк клиента
        B_SETTLEMENT,        -- расчетный счет клиента
        B_BANK,              -- наименование банка клиента
        B_CORR,              -- корр.счет
        B_BIC,               -- БИК
        --
        CONTACT_PHONE,       -- контасктый телефон клиента
        STATUS               -- статус лицевого счета клиента
    FROM V_UR_S_EXPORT_UR@MMTDB;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK201_CUSTOMER_J insert '||v_count||' rows', c_PkgName||'.'||v_prcName);
    Gather_Table_Stat(l_Tab_Name => 'PK201_CUSTOMER_J');
    --
    -- Заполняем таблицу телефонов на заказах юр.лиц 
    DELETE FROM PK201_CUSTOMER_J_PHONES;
    INSERT INTO PK201_CUSTOMER_J_PHONES
    SELECT * FROM V_UR_S_EXPORT_UR_PHONES@MMTDB;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK201_CUSTOMER_J_PHONES insert '||v_count||' rows', c_PkgName||'.'||v_prcName);
    Gather_Table_Stat(l_Tab_Name => 'PK201_CUSTOMER_J_PHONES');
    --
    -- Заполняем таблицу телефонов на заказах юр.лиц 
    DELETE FROM PK201_CUSTOMER_J_MIN;
    INSERT INTO PK201_CUSTOMER_J_MIN
    SELECT * FROM V_UR_S_EXPORT_UR_MIN@MMTDB;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK201_CUSTOMER_J_MIN insert '||v_count||' rows', c_PkgName||'.'||v_prcName);
    Gather_Table_Stat(l_Tab_Name => 'PK201_CUSTOMER_J_MIN');
    --
    Pk01_Syslog.Write_msg('Стоп', c_PkgName||'.'||v_prcName);
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--=========================================================================--
-- Импорт юридических лиц
-- временные таблицы заполняются PK02_EXPORT_P.Exp_subs_info@MMTDB
--=========================================================================--
PROCEDURE Import_Customers
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Import_Customers';
    v_brand_id        INTEGER;
    v_branch_id       INTEGER;
    v_agent_id        INTEGER;
    v_client_id       INTEGER;
    v_contract_id     INTEGER;
    v_account_id      INTEGER;
    v_profile_id      INTEGER;
    v_customer_id     INTEGER;
    v_sf_customer_id  INTEGER;
    v_address_id      INTEGER;
    v_acc_status      ACCOUNT_T.STATUS%TYPE;
    v_contractor_type CONTRACTOR_T.CONTRACTOR_TYPE%TYPE;
    v_contractor_name CONTRACTOR_T.SHORT_NAME%TYPE;
    v_contractor_id   CONTRACTOR_T.CONTRACTOR_ID%TYPE;
    v_p_contractor_id CONTRACTOR_T.PARENT_ID%TYPE;
    v_all             INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName);
    --
    FOR r_cust IN (
        SELECT  
            ACCOUNT_POID_ID0,    -- POID_ID0 в новом биллинге
            ACCOUNT_NO,          -- № лицевого счета
            COMPANY,             -- наименование компании
            CONTRACT_NO,         -- № договора
            CONTRACT_DATE,       -- дата договора
            BRAND_NAME,          -- наименование бренда
            SERVICE_PROVIDER,    -- ACCOUNT_T.POID_ID0 КТТК в "Новом Биллинге"
            -- юридический адрес
            J_ZIP, J_REG, J_CITY, J_ADDR,
            -- фактический адрес
            B_ZIP, B_REG, B_CITY, SET_ADDR,
            -- адрес доставки
            D_ZIP, D_REG, D_CITY, D_ADDR,
            -- координаты компании, которая платит за клиента заключившего договор
            SF_COMPANY, SF_ADDRESS, SF_INN, SF_KPP,
            -- ИНН, КПП клиента
            S_INN, S_KPP,
            -- Банк клиента
            B_SETTLEMENT,        -- расчетный счет клиента
            B_BANK,              -- наименование банка клиента
            B_CORR,              -- корр.счет
            B_BIC,               -- БИК
            --
            CONTACT_PHONE,       -- контасктый телефон клиента
            STATUS               -- статус лицевого счета клиента
        FROM PK201_CUSTOMER_J CJ
       WHERE NOT EXISTS (
           SELECT * FROM ACCOUNT_T A
            WHERE A.ACCOUNT_NO = CJ.ACCOUNT_NO
        )
    )
    LOOP
        v_all := v_all + 1;
        
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- CLIENT_T - создать клиента
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        BEGIN
            -- получить ID договора
            SELECT CLIENT_ID INTO v_client_id
              FROM CLIENT_T
             WHERE CLIENT_NAME = r_cust.COMPANY;
        EXCEPTION WHEN NO_DATA_FOUND THEN
            v_client_id := PK11_CLIENT.New_client(p_name => r_cust.COMPANY);
        END;
        
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- CONTRACT_T - создать договор
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        BEGIN
            -- получить ID договора
            SELECT CONTRACT_ID INTO v_contract_id
              FROM CONTRACT_T
             WHERE CONTRACT_NO = r_cust.CONTRACT_NO;
        EXCEPTION WHEN NO_DATA_FOUND THEN
            v_contract_id := PK12_CONTRACT.Open_contract(
                   p_contract_no=> r_cust.CONTRACT_NO,
                   p_date_from  => r_cust.CONTRACT_DATE,
                   p_date_to    => NULL,
                   p_client_id  => v_client_id,
                   p_manager_id => Pk00_Const.c_MANAGER_SIEBEL_ID
                 );
        END;

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- Создаем клиента Юр.лицо
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        v_customer_id := PK13_CUSTOMER.New_customer(
               p_erp_code   => NULL,
               p_inn        => r_cust.S_INN,
               p_kpp        => r_cust.S_KPP,
               p_name       => r_cust.COMPANY,
               p_short_name => r_cust.COMPANY,
               p_notes      => NULL
           );

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- Создаем создаем плательщика за клиента Юр.лицо, если нужно
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        IF r_cust.SF_COMPANY IS NOT NULL THEN
            BEGIN
                SELECT C.CUSTOMER_ID INTO v_sf_customer_id
                  FROM CUSTOMER_T C
                 WHERE C.INN = r_cust.SF_INN 
                   AND C.KPP = r_cust.SF_KPP;
            EXCEPTION WHEN NO_DATA_FOUND THEN
                v_sf_customer_id := PK13_CUSTOMER.New_customer(
                     p_erp_code   => NULL,
                     p_inn        => r_cust.SF_INN,
                     p_kpp        => r_cust.SF_KPP,
                     p_name       => r_cust.SF_COMPANY,
                     p_short_name => r_cust.SF_COMPANY,
                     p_notes      => NULL
                   );
            END;
        END IF;

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- Добавить банк Юр.лица
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        PK13_CUSTOMER.Set_bank(
                     p_customer_id      => v_customer_id,
                     p_bank_name        => r_cust.B_BANK,
                     p_bank_code        => r_cust.B_BIC,
                     p_bank_corr_account=> r_cust.B_CORR,
                     p_bank_settlement  => r_cust.B_SETTLEMENT
                   );

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- ACCOUNT_T - создать лицевой счет
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        IF r_cust.STATUS = 10100 THEN    -- статус билингуемый
            v_acc_status := PK00_CONST.c_ACC_STATUS_BILL;
        ELSIF r_cust.STATUS = 10103 THEN -- статус архивный
            v_acc_status := PK00_CONST.c_ACC_STATUS_CLOSED;
        ELSE                             -- статус в работе
            v_acc_status := PK00_CONST.c_ACC_STATUS_TEST;
        END IF;
        
        v_account_id := PK05_ACCOUNT.New_account(
               p_account_no   => r_cust.ACCOUNT_NO,
               p_account_type => PK00_CONST.c_ACC_TYPE_J,
               p_currency_id  => PK00_CONST.c_CURRENCY_RUB,
               p_status       => v_acc_status,
               p_parent_id    => NULL
           );

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- Организационная привязка
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        BEGIN
            SELECT C.CONTRACTOR_TYPE, C.SHORT_NAME, 
                   C.CONTRACTOR_ID, C.PARENT_ID 
              INTO v_contractor_type, v_contractor_name,
                   v_contractor_id, v_p_contractor_id
              FROM CONTRACTOR_T C
             WHERE C.SHORT_NAME = r_cust.BRAND_NAME;
            -- 
            IF v_contractor_type = PK00_CONST.c_CTR_TYPE_BRAND THEN
                -- привязка на уровне xTTK
                v_branch_id := v_contractor_id;
                v_agent_id  := NULL;
            ELSIF v_contractor_type = PK00_CONST.c_CTR_TYPE_AGENT THEN
                -- привязка на уровне агента
                v_branch_id := v_p_contractor_id;
                v_agent_id  := v_contractor_id;
            ELSE
                -- привязка не найдена
                v_branch_id := NULL;
                v_agent_id  := NULL;
            END IF;
            --
        EXCEPTION WHEN NO_DATA_FOUND THEN
            -- добавляем БРЕНД, как агента на BRAND_ID = 11 (KTTK)
            Pk01_Syslog.Write_msg('ACCOUNT_ID='||v_account_id||'Не найден CONTRACTOR_T для бренда '||r_cust.BRAND_NAME, 
                                  c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err);
        END;

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- получить бренд
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        BEGIN
            -- получить ID договора
            SELECT BRAND_ID INTO v_brand_id
              FROM BRAND_T
             WHERE BRAND = r_cust.BRAND_NAME;
        EXCEPTION WHEN NO_DATA_FOUND THEN
            v_brand_id := NULL;
        END;

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- ACCOUNT_PROFILE_T - создать профиль лицевого счета
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- создать профиль лицевого счета
        v_profile_id := PK05_ACCOUNT.Set_profile(
               p_account_id    => v_account_id,
               p_brand_id      => v_brand_id,
               p_contract_id   => v_contract_id,
               p_customer_id   => v_customer_id,
               p_subscriber_id => NULL,
               p_contractor_id => Pk00_Const.c_CONTRACTOR_KTTK_ID,
               p_branch_id     => v_branch_id,
               p_agent_id      => v_agent_id,
               p_contractor_bank_id => Pk00_Const.c_KTTK_J_BANK_ID,
               p_vat           => Pk00_Const.c_VAT,
               p_date_from     => r_cust.CONTRACT_DATE,
               p_date_to       => NULL
            );

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- ACCOUNT_CONTACT_T - добавить адреса на л/с
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- добавить юридический адрес
        v_address_id := PK05_ACCOUNT.Add_address(
               p_account_id   => v_account_id,
               p_address_type => PK00_CONST.c_ADDR_TYPE_JUR,
               p_country      => 'РФ',
               p_zip          => r_cust.J_ZIP,
               p_state        => NULL,
               p_city         => r_cust.J_CITY,
               p_address      => r_cust.J_ADDR,
               p_person       => NULL,
               p_phones       => r_cust.CONTACT_PHONE,
               p_fax          => NULL,
               p_email        => NULL,
               p_date_from    => r_cust.CONTRACT_DATE,
               p_date_to      => NULL
            );

        -- добавить адрес доставки счета
        v_address_id := PK05_ACCOUNT.Add_address(
               p_account_id   => v_account_id,
               p_address_type => PK00_CONST.c_ADDR_TYPE_DLV,
               p_country      => 'РФ',
               p_zip          => r_cust.D_ZIP,
               p_state        => NULL,
               p_city         => r_cust.D_CITY,
               p_address      => r_cust.D_ADDR,
               p_person       => NULL,
               p_phones       => r_cust.CONTACT_PHONE,
               p_fax          => NULL,
               p_email        => NULL,
               p_date_from    => r_cust.CONTRACT_DATE,
               p_date_to      => NULL
            );
             
        -- добавить адрес грузополучателя, того кто платит за клиента (если есть)
        -- адрес разобъем руками
        IF r_cust.SF_ADDRESS IS NOT NULL THEN
            v_address_id := PK05_ACCOUNT.Add_address(
               p_account_id   => v_account_id,
               p_address_type => PK00_CONST.c_ADDR_TYPE_GRP,
               p_country      => 'РФ',
               p_zip          => NULL,
               p_state        => NULL,
               p_city         => NULL,
               p_address      => r_cust.SF_ADDRESS,
               p_person       => NULL,
               p_phones       => NULL,
               p_fax          => NULL,
               p_email        => NULL,
               p_date_from    => r_cust.CONTRACT_DATE,
               p_date_to      => NULL
            );
        END IF;    
        
        -- добавить фактический адрес
        v_address_id := PK05_ACCOUNT.Add_address(
               p_account_id   => v_account_id,
               p_address_type => PK00_CONST.c_ADDR_TYPE_LOC,
               p_country      => 'РФ',
               p_zip          => r_cust.B_ZIP,
               p_state        => NULL,
               p_city         => r_cust.B_CITY,
               p_address      => r_cust.SET_ADDR,
               p_person       => NULL,
               p_phones       => r_cust.CONTACT_PHONE,
               p_fax          => NULL,
               p_email        => NULL,
               p_date_from    => r_cust.CONTRACT_DATE,
               p_date_to      => NULL
            );
               
        -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
        -- выводим диагностику
        IF MOD(v_all, 1000) = 0 THEN
            Pk01_Syslog.Write_msg('обработано '||v_all||' строк', c_PkgName||'.'||v_prcName );
        END IF;
        --
    END LOOP;    
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.Write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- Создать описатель периода лицевого счета
PROCEDURE New_billinfo (
               p_account_id    IN INTEGER
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'New_billinfo';
    v_period_id      INTEGER;
    v_last_period_id INTEGER;
    v_next_period_id INTEGER;
    v_period_from    DATE;
    v_period_to      DATE;
    v_period_length  INTEGER := 1;
    v_bill_id        INTEGER;
    v_next_bill_id   INTEGER;
    v_last_bill_id   INTEGER;
    v_bill_no        BILL_T.BILL_NO%TYPE := NULL;
    v_prev_bill_id   INTEGER;
    v_currency_id    INTEGER := PK00_CONST.c_CURRENCY_RUB;
BEGIN
    -- ---------------------------------------------------------- --
    -- период перед текущим: LAST или BILL 
    -- ---------------------------------------------------------- --
    -- получаем ID биллингового периода
    BEGIN
        -- ID счета предыдущего периода
        v_prev_bill_id := NULL;
        -- получаем описатель периода
        SELECT PERIOD_ID, PERIOD_FROM, PERIOD_TO
          INTO v_last_period_id, v_period_from, v_period_to
          FROM PERIOD_T
         WHERE POSITION = PK00_CONST.c_PERIOD_BILL;  
        -- получить номер счета для периода
        v_bill_no := Pk07_Bill.Make_bill_no( p_account_id, v_last_period_id);
        -- Создаем счет для периода
        v_last_bill_id := Pk07_Bill.Open_recuring_bill (
                   p_account_id,    -- ID лицевого счета
                   v_last_period_id,-- ID расчетного периода YYYYMM
                   v_bill_no,       -- Номер счета
                   v_currency_id,   -- ID валюты счета
                   v_period_to      -- Дата счета (биллингового периода)
               );
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- получаем описатель периода
            SELECT PERIOD_ID, PERIOD_FROM, PERIOD_TO
              INTO v_last_period_id, v_period_from, v_period_to
              FROM PERIOD_T
             WHERE POSITION = PK00_CONST.c_PERIOD_LAST;  
            -- получить номер счета для периода
            v_bill_no := Pk07_Bill.Make_bill_no( p_account_id, v_last_period_id);
            -- Создаем счет для периода
            v_last_bill_id := Pk07_Bill.Open_recuring_bill (
                       p_account_id,    -- ID лицевого счета
                       v_last_period_id,-- ID расчетного периода YYYYMM
                       v_bill_no,       -- Номер счета
                       v_currency_id,   -- ID валюты счета
                       v_period_to      -- Дата счета (биллингового периода)
                   );
    END;
    -- ---------------------------------------------------------- --
    -- текущий период
    -- ---------------------------------------------------------- --
    -- ID счета предыдущего периода
    v_prev_bill_id := v_last_bill_id;
    -- получаем описатель периода
    SELECT PERIOD_ID, PERIOD_FROM, PERIOD_TO
      INTO v_period_id, v_period_from, v_period_to
      FROM PERIOD_T
     WHERE POSITION = PK00_CONST.c_PERIOD_OPEN;  
    -- получить номер счета для периода
    v_bill_no := Pk07_Bill.Make_bill_no( p_account_id, v_period_id);
    -- Создаем счет для периода
    v_bill_id := Pk07_Bill.Open_recuring_bill (
               p_account_id,   -- ID лицевого счета
               v_period_id,    -- ID расчетного периода YYYYMM
               v_bill_no,      -- Номер счета
               v_currency_id,  -- ID валюты счета
               v_period_to     -- Дата счета (биллингового периода)
           );
           
    -- ---------------------------------------------------------- --
    -- следующий период
    -- ---------------------------------------------------------- --
    -- обнуляем ID предыдущего расчетного периода
    v_prev_bill_id := v_bill_id;
    -- получаем ID расчетного периода счета
    SELECT PERIOD_ID, PERIOD_FROM, PERIOD_TO
      INTO v_next_period_id, v_period_from, v_period_to
      FROM PERIOD_T
     WHERE POSITION = PK00_CONST.c_PERIOD_NEXT;
    -- получить номер счета текущего периода
    v_bill_no := Pk07_Bill.Make_bill_no( p_account_id, v_next_period_id);
    -- Создаем счет для текущего периода
    v_next_bill_id := Pk07_Bill.Open_recuring_bill (
               p_account_id,    -- ID лицевого счета
               v_next_period_id,-- ID расчетного периода YYYYMM
               v_bill_no,       -- Номер счета
               v_currency_id,   -- ID валюты счета
               v_period_to      -- Дата счета (биллингового периода)
           ); 
    -- - - - - - - - - --
    -- создаем информационную запись о счетах для вновь созданного Л/С
    INSERT INTO BILLINFO_T ( 
        ACCOUNT_ID, 
        LAST_PERIOD_ID, PERIOD_ID, NEXT_PERIOD_ID, 
        LAST_BILL_ID, BILL_ID, NEXT_BILL_ID, 
        PERIOD_LENGTH, CURRENCY_ID 
    ) VALUES (
        p_account_id, 
        v_last_period_id, v_period_id, v_next_period_id,
        v_last_bill_id, v_bill_id, v_next_bill_id, 
        v_period_length, v_currency_id
    );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,account_id='||p_account_id, c_PkgName||'.'||v_prcName );
END;

--=========================================================================--
-- Создать счета для всех Л/С для биллинговых периодов из PERIOD_T
--=========================================================================--
PROCEDURE Make_Bill_For_Periods 
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Make_Bill_For_Periods';
    v_bill_id  INTEGER;
    v_all      INTEGER := 0;
    v_ok       INTEGER := 0;
    v_err      INTEGER := 0;
    --
    v_date_from       DATE;
    --
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    FOR l_cur IN ( 
      SELECT A.ACCOUNT_ID, A.CURRENCY_ID 
        FROM ACCOUNT_T A
       WHERE A.STATUS = Pk00_Const.c_ACC_STATUS_BILL 
         AND A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_J 
         AND NOT EXISTS (
           SELECT * FROM BILLINFO_T BI
            WHERE BI.ACCOUNT_ID = A.ACCOUNT_ID
         )
    )
    LOOP
        v_all := v_all + 1;
        BEGIN
            -- создание описателя счетов и самих счетов для нового Л/С
            New_billinfo ( p_account_id    => l_cur.account_id ); -- ID лицевого счета
            v_ok := v_ok + 1;
        EXCEPTION
            WHEN OTHERS THEN
              Pk01_Syslog.Write_msg(
                 p_Msg  => l_cur.account_id || ' - error',
                 p_Src  => c_PkgName||'.'||v_prcName,
                 p_Level=> Pk01_Syslog.L_err );
              v_err := v_err + 1;
        END;
        -- выводим диагностику
        IF MOD(v_all, 1000) = 0 THEN
            Pk01_Syslog.Write_msg('Processed: '||v_ok||'-ок, '||v_err||'-err from '||v_all, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        --
    END LOOP;
    --
    Pk01_Syslog.Write_msg('Processed: '||v_ok||'-ок, '||v_err||'-err from '||v_all, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================
-- Откат абонентов физ.лиц
--==================================================================================
PROCEDURE Rollback_customers
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_customers';
    v_rateplan_id   INTEGER; 
    v_rateplan_name RATEPLAN_T.RATEPLAN_NAME%TYPE;
    v_count         INTEGER;
    -- служебные процедуры - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Bill_t_drop_fk
    IS
        v_prcName       CONSTANT VARCHAR2(30) := 'Bill_t_drop_fk';
    BEGIN 
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ITEM_T DROP CONSTRAINT ITEM_T_BILL_T_FK';
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.INVOICE_ITEM_T DROP CONSTRAINT INVOICE_ITEM_T_BILL_T_FK';
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.BILLINFO_T DROP CONSTRAINT BILLINFO_T_BILL_T_FK';
        COMMIT;
    EXCEPTION WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
    END;

    PROCEDURE Bill_t_add_fk
    IS
        v_prcName       CONSTANT VARCHAR2(30) := 'Bill_t_add_fk';
    BEGIN 
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ITEM_T ADD (
          CONSTRAINT ITEM_T_BILL_T_FK 
          FOREIGN KEY (BILL_ID, REP_PERIOD_ID) 
          REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
          ENABLE VALIDATE )';
          
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.INVOICE_ITEM_T ADD (
          CONSTRAINT INVOICE_ITEM_T_BILL_T_FK 
          FOREIGN KEY (BILL_ID, REP_PERIOD_ID) 
          REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
          ENABLE VALIDATE )';
          
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.BILLINFO_T ADD (
          CONSTRAINT BILLINFO_T_BILL_T_FK 
          FOREIGN KEY (BILL_ID, PERIOD_ID) 
          REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
          ENABLE VALIDATE)';
    EXCEPTION WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
    END;
    
    PROCEDURE Account_t_drop_fk
    IS
        v_prcName       CONSTANT VARCHAR2(30) := 'Account_t_drop_fk';
    BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.REP_PERIOD_INFO_T DROP CONSTRAINT REP_PERIOD_INFO_T_ACC_T_FK';
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ACCOUNT_T DROP CONSTRAINT ACCOUNT_T_ACCOUNT_T_FK';
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ORDER_T DROP  CONSTRAINT ORDER_T_ACCOUNT_T_FK';
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.BILL_T DROP CONSTRAINT BILL_T_ACCOUNT_T_FK';
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ACCOUNT_PROFILE_T DROP CONSTRAINT ACCOUNT_PROFILE_ACCOUNT_T_FK';
    EXCEPTION WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
    END;

    PROCEDURE Account_t_add_fk
    IS
        v_prcName       CONSTANT VARCHAR2(30) := 'Account_t_add_fk';
    BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.REP_PERIOD_INFO_T ADD (
            CONSTRAINT REP_PERIOD_INFO_T_ACC_T_FK 
            FOREIGN KEY (ACCOUNT_ID) 
            REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
            ENABLE VALIDATE)';
  
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ACCOUNT_T ADD (
            CONSTRAINT ACCOUNT_T_ACCOUNT_T_FK 
            FOREIGN KEY (PARENT_ID) 
            REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
            ENABLE VALIDATE)';
  
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ORDER_T ADD (
            CONSTRAINT ORDER_T_ACCOUNT_T_FK 
            FOREIGN KEY (ACCOUNT_ID) 
            REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
            ENABLE VALIDATE)';
  
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.BILL_T ADD (
            CONSTRAINT BILL_T_ACCOUNT_T_FK 
            FOREIGN KEY (ACCOUNT_ID) 
            REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
            ENABLE VALIDATE)';
  
        EXECUTE IMMEDIATE 'ALTER TABLE PIN.ACCOUNT_PROFILE_T ADD (
            CONSTRAINT ACCOUNT_PROFILE_ACCOUNT_T_FK 
            FOREIGN KEY (ACCOUNT_ID) 
            REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
            ENABLE VALIDATE)';

    EXCEPTION WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
    END;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- заполняем таблицу счетов Юр.лиц (физиков не трогаем)
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK201_ACC_BILL_J DROP STORAGE';
    INSERT INTO PK201_ACC_BILL_J
    SELECT A.ACCOUNT_ID, AP.CONTRACT_ID, B.BILL_ID, B.REP_PERIOD_ID 
      FROM ACCOUNT_T A, BILL_T B, ACCOUNT_PROFILE_T AP
     WHERE A.ACCOUNT_TYPE = PK00_CONST.c_ACC_TYPE_J
       AND A.ACCOUNT_ID = B.ACCOUNT_ID
       AND A.ACCOUNT_ID = AP.ACCOUNT_ID
       AND A.ACCOUNT_ID NOT IN (993556, 993560, 1926034, 1926458, 1926460);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK201_ACC_BILL_J: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PK201_ACC_BILL_J'); 
    COMMIT;   
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- откатываем начисления
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- откатываем детализацию начислений по счету
    DELETE FROM DETAIL_MMTS_T D
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J  WHERE J.BILL_ID = D.BILL_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('DETAIL_MMTS_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'DETAIL_MMTS_T');
    COMMIT;
    
    -- откатываем операции разноски платежей
    DELETE FROM PAY_TRANSFER_T PT
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J  WHERE J.BILL_ID = PT.BILL_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAY_TRANSFER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PAY_TRANSFER_T');
    COMMIT;
    
    -- откатываем платежи
    DELETE FROM PAYMENT_T P
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J  WHERE J.ACCOUNT_ID = P.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAYMENT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info ); 
    Gather_Table_Stat(l_Tab_Name => 'PAYMENT_T');
    COMMIT;
    
    -- откатываем строки счетов-фактур
    UPDATE ITEM_T I SET INV_ITEM_ID = NULL
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J  WHERE J.BILL_ID = I.BILL_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    
    --
    DELETE FROM INVOICE_ITEM_T II
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J  WHERE J.BILL_ID = II.BILL_ID
    ); 
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'INVOICE_ITEM_T');
    COMMIT;
    
    -- откатываем строки насислений по счетам
    DELETE FROM ITEM_T I
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J  WHERE J.BILL_ID = I.BILL_ID
    ); 
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ITEM_T');
    COMMIT;
    
    -- удаляем описатели счетов
    DELETE FROM BILLINFO_T BI
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J  WHERE J.BILL_ID = BI.BILL_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLINFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILLINFO_T');
    COMMIT;
    
    -- удаляем счета
    Bill_t_drop_fk;
    DELETE FROM BILL_T B
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J  WHERE J.BILL_ID = B.BILL_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILL_T');
    Bill_t_add_fk;
    COMMIT;
    
    -- удаляем обороты по периодам
    DELETE FROM REP_PERIOD_INFO_T BI
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J  WHERE J.ACCOUNT_ID = BI.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('REP_PERIOD_INFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'REP_PERIOD_INFO_T');
    COMMIT;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- удаляем телефоны на заказе
    DELETE FROM ORDER_PHONES_T OP
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J, ORDER_T O  
         WHERE OP.ORDER_ID = O.ORDER_ID
           AND J.ACCOUNT_ID= O.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_PHONES_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ORDER_PHONES_T');
    COMMIT;
    
    -- удаляем блокировки заказов
    DELETE FROM ORDER_LOCK_T OL
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J, ORDER_T O  
         WHERE OL.ORDER_ID = O.ORDER_ID
           AND J.ACCOUNT_ID= O.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_LOCK_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ORDER_LOCK_T');
    COMMIT;
    
    -- удаляем описатели заказов
    DELETE FROM ORDER_BODY_T OB
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J, ORDER_T O  
         WHERE OB.ORDER_ID = O.ORDER_ID
           AND J.ACCOUNT_ID= O.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ORDER_BODY_T');
    COMMIT;
    
    -- удаляем описатели заказов
    DELETE FROM ORDER_T O
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J  WHERE J.ACCOUNT_ID = O.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ORDER_T');
    COMMIT;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- удаляем профили клиента
    DELETE FROM ACCOUNT_PROFILE_T AP
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J  WHERE J.ACCOUNT_ID = AP.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_PROFILE_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    Gather_Table_Stat(l_Tab_Name => 'ACCOUNT_PROFILE_T');
    COMMIT;
    
    -- удаляем описатели клиента
    DELETE FROM CUSTOMER_BANK_T
     WHERE CUSTOMER_ID NOT IN (1, 1420564, 492225, 492217, 1420985, 1420988);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CUSTOMER_BANK_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'CUSTOMER_BANK_T');
    COMMIT;
    
    -- удаляем клиента
    DELETE FROM CUSTOMER_T
     WHERE CUSTOMER_ID NOT IN (1, 1420564, 492225, 492217, 1420985, 1420988);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CUSTOMER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'CUSTOMER_T');
    COMMIT;
    
    -- удаляем договора клиента
    DELETE FROM CONTRACT_T C
     WHERE NOT EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J  WHERE J.CONTRACT_ID =  C.CONTRACT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CONTRACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    Gather_Table_Stat(l_Tab_Name => 'CONTRACT_T');
    COMMIT;
    
    -- удаляем адреса лицевого счета
    DELETE FROM ACCOUNT_CONTACT_T AC
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J  WHERE J.ACCOUNT_ID = AC.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_CONTACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ACCOUNT_CONTACT_T');
    COMMIT;
    
    -- удаляем лицевые счета
    Account_t_drop_fk;
    DELETE FROM ACCOUNT_T A
     WHERE EXISTS (
        SELECT * FROM PK201_ACC_BILL_J J  WHERE J.ACCOUNT_ID = A.ACCOUNT_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ACCOUNT_T');
    Account_t_add_fk;
    COMMIT;
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


END PK201_IMPORT_J;
/
