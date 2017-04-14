CREATE OR REPLACE PACKAGE PK203_SPB_OPERATOR
IS
    --
    -- Пакет для создания клиентов бренда xTTK «Санкт-Петербургский ТЕЛЕПОРТ»
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK203_SPB_OPERATOR';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    -- ID КТТК
    c_CONTRACTOR_KTTK_ID         constant integer := 1;
    -- Поставщик для "СПБ ТЕЛЕПОРТ" 
    c_CONTRACTOR_SPB_TELEPORT_ID constant integer := 19;
    -- Бренд для "СПБ ТЕЛЕПОРТ"
    c_SPB_TTK_Root               constant integer := 21;
    
    -- Услуги присоединения и пропуска трафика на местном и/или зоновом уровне
    c_SERVICE_OPLOCL CONSTANT INTEGER := 7;
    
    -- =========================================================================
    -- Импортируем клиентов СПб-Телепорт для межоператорских расчетов
    -- ========================================================================= 
    PROCEDURE Import_Clients;
    PROCEDURE Import_orders;
    
    -- ----------------------------------------------------------------------------- --
    -- Создание клиента для бренда xTTK «Санкт-Петербургский ТЕЛЕПОРТ»  
    -- имеющего договор присоединения на местном уровне
    PROCEDURE Add_client(
                  p_client_id     OUT INTEGER,  -- ID клиента
                  p_customer_id   OUT INTEGER,  -- ID покупателя
                  p_contract_id   OUT INTEGER,  -- ID договора
                  p_account_id    OUT INTEGER,  -- ID лицевого счета
                  p_order_id      OUT INTEGER,  -- ID заказа
                  p_customer       IN VARCHAR2, -- наименование клиента/покупателя
                  p_customer_short IN VARCHAR2, -- краткое наименование клиента/покупателя
                  p_customer_inn   IN VARCHAR2, -- ИНН
                  p_customer_kpp   IN VARCHAR2, -- КПП
                  p_contract_date  IN DATE,     -- дата договора
                  p_contract_no    IN VARCHAR2, -- номер договора
                  p_account_no     IN VARCHAR2, -- номер лицевого счета
                  p_order_no       IN VARCHAR2, -- номер заказа (первого, позже можно добавить еще)
                  p_rateplan_id    IN INTEGER DEFAULT NULL -- ID тарифного плана, если знаем
              );
    
    -- Добавить заказ на "Услуги присоединения и пропуска трафика на местном и/или зоновом уровне"
    PROCEDURE Add_order(
                  p_order_id      OUT INTEGER,  -- ID заказа
                  p_account_no    IN VARCHAR2,  -- номер лицевого счета
                  p_order_no      IN VARCHAR2,  -- номер заказа
                  p_date_from     IN DATE,      -- дата начала действия заказа
                  p_rateplan_id   IN INTEGER DEFAULT NULL -- ID тарифного плана, если есть
              );
              
    -- Назначить заказу тарифный план          
    PROCEDURE Bind_rateplan (
                  p_order_no      IN VARCHAR2,  -- номер заказа
                  p_rateplan_id   IN INTEGER    -- ID заказа
              );
              
    -- Зарегистрировать в БИЛЛИНГЕ тарифный план на
    -- "Услуги присоединения и пропуска трафика на местном и/или зоновом уровне" 
    PROCEDURE Register_rateplan(
                  p_rateplan_name IN VARCHAR2,  -- имя карифного плана
                  p_rateplan_code IN VARCHAR2,  -- код - краткое наименование тарифного плана
                  p_rateplan_id   OUT INTEGER   -- ID заказа
              );

    -- ==============================================================================
    -- Д О П О Л Н И Т Е Л Ь Н Ы Е    Ф У Н К Ц И И
    -- ==============================================================================
    -- Заполнить юридический адрес покупателя
    FUNCTION Set_customer_address(
                   p_customer_id  IN VARCHAR2,
                   p_address_type IN VARCHAR2,
                   p_country      IN VARCHAR2, 
                   p_zip          IN VARCHAR2,
                   p_state        IN VARCHAR2,
                   p_city         IN VARCHAR2, 
                   p_address      IN VARCHAR2,
                   p_date_from    IN DATE,
                   p_date_to      IN DATE
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- Добавить юридический адрес к лицевому счету, возвращает значения
    --   - положительное - id контакта 
    --   - при ошибке выставляет исключение
    --
    FUNCTION Add_jur_address(
                   p_account_id    IN INTEGER,
                   p_country       IN VARCHAR2,
                   p_zip           IN VARCHAR2,
                   p_state         IN VARCHAR2,
                   p_city          IN VARCHAR2,
                   p_address       IN VARCHAR2,
                   p_person        IN VARCHAR2,
                   p_phones        IN VARCHAR2,
                   p_fax           IN VARCHAR2,
                   p_email         IN VARCHAR2,
                   p_date_from     IN DATE,
                   p_date_to       IN DATE DEFAULT NULL ,
                   p_notes         IN VARCHAR2 DEFAULT NULL
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- Добавить адрес доставки к лицевому счету, возвращает значения
    --   - положительное - id контакта 
    --   - при ошибке выставляет исключение
    --
    FUNCTION Add_dlv_address(
                   p_account_id    IN INTEGER,
                   p_country       IN VARCHAR2,
                   p_zip           IN VARCHAR2,
                   p_state         IN VARCHAR2,
                   p_city          IN VARCHAR2,
                   p_address       IN VARCHAR2,
                   p_person        IN VARCHAR2,
                   p_phones        IN VARCHAR2,
                   p_fax           IN VARCHAR2,
                   p_email         IN VARCHAR2,
                   p_date_from     IN DATE,
                   p_date_to       IN DATE DEFAULT NULL ,
                   p_notes         IN VARCHAR2 DEFAULT NULL
               ) RETURN INTEGER;

    -- создать нового менеджера, возвращает значения
    --   - положительное - ID менеджера, 
    --   - при ошибке выставляет исключение
    FUNCTION New_manager(
                   p_contractor_id    IN INTEGER,
                   p_department       IN VARCHAR2,
                   p_position         IN VARCHAR2, 
                   p_last_name        IN VARCHAR2, -- фамилия
                   p_first_name       IN VARCHAR2, -- имя 
                   p_middle_name      IN VARCHAR2, -- отчество
                   p_phones           IN VARCHAR2,
                   p_email            IN VARCHAR2,
                   p_date_from        IN DATE,
                   p_date_to          IN DATE DEFAULT NULL
               ) RETURN INTEGER;

    -- Назначить менеджера на договор/лицевой счет/заказ
    PROCEDURE Set_manager( 
                   p_manager_id  IN INTEGER,
                   p_contract_id IN INTEGER,
                   p_account_id  IN INTEGER,
                   p_order_id    IN INTEGER,
                   p_date_from   IN DATE,
                   p_date_to     IN DATE DEFAULT NULL
                 );

    -- ============================================================================== --
    -- Удалить клиента (все что создано процедурой New_client)
    -- ============================================================================== --
    PROCEDURE Remove_client(
                      p_account_id IN INTEGER
                   );
    
END PK203_SPB_OPERATOR;
/
CREATE OR REPLACE PACKAGE BODY PK203_SPB_OPERATOR
IS

    
-- =========================================================================
-- Импортируем клиентов СПб-Телепорт для межоператорских расчетов
-- ========================================================================= 
PROCEDURE Import_Clients 
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_Clients';
    v_account_id     INTEGER;
    v_profile_id     INTEGER;
    v_brand_id       INTEGER;
    v_client_id      INTEGER;
    v_customer_id    INTEGER;
    v_contract_id    INTEGER;
    v_address_id     INTEGER;
    v_client_type    INTEGER;
    v_market_segment INTEGER;
    v_manager_id     INTEGER;
    v_abn_count      INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName);
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    FOR abn IN (
      SELECT ACCOUNT_NO, 
             COMPANY,     
             CUSTDATE,    -- дата договора 
             CURRENCY, 
             TAX_VAT, 
             CUSTNO,      -- номер договора 
             KONTRAGENT,  -- ERP_CODE в 1С 
             CLIENT,      -- наименование клиента 
             INN, 
             KPP, 
             EMAIL_ADDR,  -- адрес для  
             DELIVERY,    -- способ доставки 
             --
             JUR_ZIP, 
             JUR_CITY, 
             JUR_ADDRESS,
             -- 
             PHIS_ZIP, 
             PHIS_CITY, 
             PHIS_ADDRESS, 
             PHIS_NAME, 
             PHONE, 
             FAX,
             -- 
             SALES_NAME,
             -- 
             MARKET_SEG, 
             CUST_TYPE, 
             IACCOUNT 
        FROM PIN.AG04_PITER_AGREEMENTS AG
        WHERE NOT EXISTS (
            SELECT A.ACCOUNT_NO, CT.CONTRACT_NO, CS.ERP_CODE, CS.INN, CS.KPP, CS.CUSTOMER
              FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T CT, CUSTOMER_T CS
             WHERE A.ACCOUNT_ID = AP.ACCOUNT_ID
               AND AP.CONTRACT_ID = CT.CONTRACT_ID
               AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
               --
               AND A.ACCOUNT_NO = AG.CUSTNO
               --AND CT.CONTRACT_NO = AG.ACCOUNT_NO
               --AND CS.ERP_CODE = AG.KONTRAGENT
               AND CS.INN = AG.INN
               AND CS.KPP = AG.KPP
        )
    )
    LOOP
      Pk01_Syslog.Write_msg(abn.CLIENT||
                            '; contract_no='||abn.account_no||
                            ', account_no='||abn.custno, 
                            c_PkgName||'.'||v_prcName);
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- создаем клиента
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      BEGIN
        SELECT CLIENT_ID INTO v_client_id
          FROM CLIENT_T CL
         WHERE CL.CLIENT_NAME = abn.CLIENT;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          v_client_id := Pk11_Client.New_client(abn.CLIENT);
      END;
      Pk01_Syslog.Write_msg('p_client_id='||v_client_id, c_PkgName||'.'||v_prcName);
            
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- Создать покупателя
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      BEGIN
        SELECT CS.CUSTOMER_ID 
          INTO v_customer_id 
          FROM CUSTOMER_T CS
        WHERE CS.CUSTOMER = abn.company
          AND CS.INN = abn.inn
          AND CS.KPP = abn.kpp
          AND CS.ERP_CODE = abn.KONTRAGENT;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_customer_id := Pk13_Customer.New_customer(
                      p_erp_code    => abn.KONTRAGENT,
                      p_inn         => abn.inn,
                      p_kpp         => abn.kpp, 
                      p_name        => abn.company,
                      p_short_name  => abn.company
                   );
      END;
      Pk01_Syslog.Write_msg('v_customer_id='||v_customer_id, c_PkgName||'.'||v_prcName);
    
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- получить ID менеджера
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
      SELECT M.MANAGER_ID 
        INTO v_manager_id 
        FROM MANAGER_T M
       WHERE M.LAST_NAME||' '||M.FIRST_NAME||' '||M.MIDDLE_NAME = abn.Sales_Name;

      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- получить ID сегмента рынка
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      v_client_type    := 6405;  -- 'Оператор связи / Большая 4-ка'
      v_market_segment := 6302;  -- 'Общероссийские операторы'
    
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- Создать договор
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      BEGIN
        SELECT C.CONTRACT_ID
          INTO v_contract_id
          FROM CONTRACT_T C
         WHERE C.CONTRACT_NO = abn.account_no;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          INSERT INTO CONTRACT_T (
            CONTRACT_ID, CONTRACT_NO, DATE_FROM, DATE_TO, 
            CLIENT_ID, CLIENT_TYPE_ID, MARKET_SEGMENT_ID
          )VALUES(
            SQ_CLIENT_ID.NEXTVAL, abn.account_no, abn.Custdate, NULL, 
            v_client_id, v_client_type, v_market_segment
          )
          RETURNING CONTRACT_ID INTO v_contract_id;
          -- привязываем менеджера к договору
          INSERT INTO SALE_CURATOR_T (MANAGER_ID, CONTRACT_ID, DATE_FROM, DATE_TO)
          VALUES(v_manager_id, v_contract_id, abn.Custdate, NULL);
      END;
      Pk01_Syslog.Write_msg('v_contract_id='||v_contract_id, c_PkgName||'.'||v_prcName);

      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- Создать лицевой счет покупателя
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      v_account_id := PK05_ACCOUNT.New_account(
                p_account_no   => abn.custno,
                p_account_type => PK00_CONST.c_ACC_TYPE_J,
                p_currency_id  => PK00_CONST.c_CURRENCY_RUB,
                p_status       => PK00_CONST.c_ACC_STATUS_BILL,
                p_parent_id    => NULL,
                p_notes        => 'Импортировано из Барсум'
             );

      Pk01_Syslog.Write_msg('p_account_id='||v_account_id, c_PkgName||'.'||v_prcName);
    
      -- создать профиль лицевого счета
      v_profile_id := PK05_ACCOUNT.Set_profile(
                 p_account_id    => v_account_id,
                 p_brand_id      => c_SPB_TTK_Root,
                 p_contract_id   => v_contract_id,
                 p_customer_id   => v_customer_id,
                 p_subscriber_id => NULL,
                 p_contractor_id => c_CONTRACTOR_KTTK_ID,
                 p_branch_id     => c_CONTRACTOR_SPB_TELEPORT_ID,
                 p_agent_id      => NULL,
                 p_contractor_bank_id => NULL,
                 p_vat           => Pk00_Const.c_VAT,
                 p_date_from     => abn.custdate,
                 p_date_to       => NULL
             );
      Pk01_Syslog.Write_msg('v_profile_id='||v_profile_id, c_PkgName||'.'||v_prcName);

      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- создание описателя счетов и самих счетов для нового Л/С
      Pk07_Bill.New_billinfo (
                     p_account_id    => v_account_id,   -- ID лицевого счета
                     p_currency_id   => Pk00_Const.c_CURRENCY_RUB, -- ID валюты счета
                     p_delivery_id   => NULL,     -- ID способа доставки счета
                     p_days_for_payment => 30
                 );

      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- добавить юридический адрес
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      v_address_id := PK05_ACCOUNT.Add_address(
               p_account_id   => v_account_id,
               p_address_type => PK00_CONST.c_ADDR_TYPE_JUR,
               p_country      => 'РФ',
               p_zip          => abn.jur_zip,
               p_state        => NULL,
               p_city         => abn.jur_city,
               p_address      => abn.jur_address,
               p_person       => NULL,
               p_phones       => abn.phone,
               p_fax          => abn.fax,
               p_email        => abn.email_addr,
               p_date_from    => abn.custdate,
               p_date_to      => NULL ,
               p_notes        => NULL
           );
      
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- добавить адрес доставки
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      v_address_id := PK05_ACCOUNT.Add_address(
               p_account_id   => v_account_id,
               p_address_type => PK00_CONST.c_ADDR_TYPE_DLV,
               p_country      => 'РФ',
               p_zip          => abn.phis_zip,
               p_state        => NULL,
               p_city         => abn.phis_city,
               p_address      => abn.phis_address,
               p_person       => abn.phis_name,
               p_phones       => abn.phone,
               p_fax          => abn.fax,
               p_email        => abn.email_addr,
               p_date_from    => abn.custdate,
               p_date_to      => NULL ,
               p_notes        => NULL
           );
      
      
      v_abn_count := v_abn_count + 1;
      
    END LOOP;
    Pk01_Syslog.Write_msg(v_abn_count||' abonents imported', c_PkgName||'.'||v_prcName);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('End', c_PkgName||'.'||v_prcName);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--=========================================================================--
-- Создание клиента для бренда xTTK «Санкт-Петербургский ТЕЛЕПОРТ»  
-- имеющего договор присоединения на местном уровне 
--=========================================================================--
PROCEDURE Import_orders
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_orders';
    v_count          INTEGER;
    v_order_id       INTEGER;
    v_order_body_id  INTEGER;
    v_fix_reate_id   INTEGER;
    v_all            INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName);
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FOR ro IN (
      SELECT AO.ACCOUNT_ID, AO.ORDER_NUM, AO.ORDER_DATE, 
             AO.SERVICE_ID, AO.SUBSERVICE_ID,
             AO.QUANTITY, AO.PRICE_FIX 
        FROM AG04_PITER_ORDERS AO
    )
    LOOP
      -- проверяем есть ли заказ в системе
      SELECT COUNT(*) INTO v_count
        FROM ORDER_T O
       WHERE O.ACCOUNT_ID = ro.account_id
         AND O.ORDER_NO = ro.order_num;
      -- 
      IF v_count = 0 THEN
        -- создаем заказ
        v_order_id := PK06_ORDER.New_order(
                   p_account_id    => ro. account_id,
                   p_order_no      => ro.order_num,
                   p_service_id    => ro.service_id,
                   p_rateplan_id   => NULL,
                   p_time_zone     => NULL,
                   p_date_from     => ro.order_date,
                   p_date_to       => Pk00_Const.c_DATE_MAX,
                   p_create_date   => SYSDATE
               );
      END IF;
      -- создаем тело заказа для компонента услуги
      IF ro.subservice_id IS NOT NULL THEN
        v_order_body_id := PK06_ORDER.Add_subservice (
                 p_order_id      => v_order_id,
                 p_subservice_id => ro.subservice_id,
                 p_charge_type   => Pk00_Const.c_CHARGE_TYPE_REC,
                 p_rateplan_id   => NULL,
                 p_date_from     => ro.order_date,
                 p_date_to       => Pk00_Const.c_DATE_MAX
             );
        -- создаем тарифы абонплаты
        v_fix_reate_id := PK06_ORDER.Set_abon_value (
                 p_order_id      => v_order_id,      -- ID заказа - услуги
                 p_order_body_id => v_order_body_id, -- ID тела заказа - компонента услуги
                 p_value         => ro.price_fix,    -- сумма абонплаты
                 p_quantity      => ro.quantity,     -- кол-во услуги в натуральном измерении
                 p_descr         => 'Импортировано из AG04_PITER_ORDERS'  -- описание
             );
      END IF;
      --
      v_all := v_all + 1;
      --
    END LOOP;
    Pk01_Syslog.Write_msg('Imported '||v_all||' orders', c_PkgName||'.'||v_prcName);
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('End', c_PkgName||'.'||v_prcName);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--=========================================================================--
-- Создание клиента для бренда xTTK «Санкт-Петербургский ТЕЛЕПОРТ»  
-- имеющего договор присоединения на местном уровне 
--=========================================================================--
PROCEDURE Add_client(
              p_client_id     OUT INTEGER,  -- ID клиента
              p_customer_id   OUT INTEGER,  -- ID покупателя
              p_contract_id   OUT INTEGER,  -- ID договора
              p_account_id    OUT INTEGER,  -- ID лицевого счета
              p_order_id      OUT INTEGER,  -- ID заказа
              p_customer       IN VARCHAR2, -- наименование клиента/покупателя
              p_customer_short IN VARCHAR2, -- краткое наименование клиента/покупателя
              p_customer_inn   IN VARCHAR2, -- ИНН
              p_customer_kpp   IN VARCHAR2, -- КПП
              p_contract_date  IN DATE,     -- дата договора
              p_contract_no    IN VARCHAR2, -- номер договора
              p_account_no     IN VARCHAR2, -- номер лицевого счета
              p_order_no       IN VARCHAR2, -- номер заказа (первого, позже можно добавить еще)
              p_rateplan_id    IN INTEGER DEFAULT NULL -- ID тарифного плана, если знаем
          )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Add_client';
    v_profile_id     INTEGER;
    v_bill_id        INTEGER;
    v_brand_id       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start. Customer='||p_customer, c_PkgName||'.'||v_prcName);
    
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создать клиента
    p_client_id := Pk11_Client.New_client(
              p_name => p_customer
           );    
           
    Pk01_Syslog.Write_msg('p_client_id='||p_client_id, c_PkgName||'.'||v_prcName);
    
    -- Создать покупателя
    p_customer_id := Pk13_Customer.New_customer(
              p_erp_code    => NULL,
              p_inn         => p_customer_inn,
              p_kpp         => p_customer_kpp, 
              p_name        => p_customer,
              p_short_name  => p_customer_short
           );

    Pk01_Syslog.Write_msg('p_customer_id='||p_customer_id, c_PkgName||'.'||v_prcName);
    
    -- Создать договор
    p_contract_id := PK12_CONTRACT.Open_contract(
              p_contract_no=> p_contract_no,
              p_date_from  => p_contract_date,
              p_date_to    => NULL,
              p_client_id  => p_client_id,
              p_manager_id => NULL            -- добавим позже
           );

    Pk01_Syslog.Write_msg('p_contract_id='||p_contract_id, c_PkgName||'.'||v_prcName);

    -- Создать лицевой счет покупателя
    p_account_id := PK05_ACCOUNT.New_account(
              p_account_no   => p_account_no,
              p_account_type => PK00_CONST.c_ACC_TYPE_J,
              p_currency_id  => PK00_CONST.c_CURRENCY_RUB,
              p_status       => PK00_CONST.c_ACC_STATUS_BILL,
              p_parent_id    => NULL
           );

    Pk01_Syslog.Write_msg('p_account_id='||p_account_id, c_PkgName||'.'||v_prcName);
    
    -- создать профиль лицевого счета
    v_profile_id := PK05_ACCOUNT.Set_profile(
               p_account_id    => p_account_id,
               p_brand_id      => v_brand_id,
               p_contract_id   => p_contract_id,
               p_customer_id   => p_customer_id,
               p_subscriber_id => NULL,
               p_contractor_id => c_CONTRACTOR_KTTK_ID,
               p_branch_id     => c_CONTRACTOR_SPB_TELEPORT_ID,
               p_agent_id      => NULL,
               p_contractor_bank_id => NULL,
               p_vat           => Pk00_Const.c_VAT,
               p_date_from     => p_contract_date,
               p_date_to       => NULL
           );
    Pk01_Syslog.Write_msg('v_profile_id='||v_profile_id, c_PkgName||'.'||v_prcName);
    
    -- создаем заказ на услуги МГ/МН связи
    p_order_id := PK06_ORDER.New_order(
              p_account_id => p_account_id,
              p_order_no   => p_order_no,
              p_service_id => PK00_CONST.c_SERVICE_OP_LOCAL,
              p_rateplan_id=> p_rateplan_id,
              p_time_zone  => NULL,
              p_date_from  => p_contract_date,
              p_date_to    => NULL
           );
           
    Pk01_Syslog.Write_msg('p_order_id='||p_order_id, c_PkgName||'.'||v_prcName);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- создание описателя счетов и самих счетов для нового Л/С
    Pk07_Bill.New_billinfo (
                   p_account_id    => p_account_id,   -- ID лицевого счета
                   p_currency_id   => Pk00_Const.c_CURRENCY_RUB,  -- ID валюты счета
                   p_delivery_id   => NULL,     -- ID способа доставки счета
                   p_days_for_payment => 30
               );

    Pk01_Syslog.Write_msg('v_bill_id='||v_bill_id, c_PkgName||'.'||v_prcName);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('End', c_PkgName||'.'||v_prcName);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;    
    
--============================================================================================
-- Добавить заказ на "Услуги присоединения и пропуска трафика на местном и/или зоновом уровне"
--
PROCEDURE Add_order(
              p_order_id      OUT INTEGER,  -- ID заказа
              p_account_no    IN VARCHAR2,  -- номер лицевого счета
              p_order_no      IN VARCHAR2,  -- номер заказа
              p_date_from     IN DATE,      -- дата начала действия заказа
              p_rateplan_id   IN INTEGER DEFAULT NULL -- ID тарифного плана, если есть
          )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Add_order';
    v_account_id     INTEGER;
BEGIN
    -- получаем ID лицевого счета
    SELECT ACCOUNT_ID INTO v_account_id
      FROM ACCOUNT_T
     WHERE ACCOUNT_NO = p_account_no;
    -- создаем заказ
    p_order_id := PK06_ORDER.New_order(
                   p_account_id    => v_account_id,
                   p_order_no      => p_order_no,
                   p_service_id    => PK00_CONST.c_SERVICE_OP_LOCAL,
                   p_rateplan_id   => p_rateplan_id,
                   p_time_zone     => NULL,
                   p_date_from     => p_date_from,
                   p_date_to       => Pk00_Const.c_DATE_MAX,
                   p_create_date   => SYSDATE
               );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================              
-- Назначить заказу тарифный план
--
PROCEDURE Bind_rateplan (
              p_order_no      IN VARCHAR2,  -- номер заказа
              p_rateplan_id   IN INTEGER    -- ID заказа
          )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Bind_rateplan';
    v_order_id INTEGER;
BEGIN
    -- получаем ID заказа
    SELECT ORDER_ID INTO v_order_id
      FROM ORDER_T
     WHERE ORDER_NO = p_order_no;
    -- привязываем ТП к заказу    
    PK06_ORDER.Bind_rateplan (
               p_rateplan_id   => p_rateplan_id, -- ID тарифного плана
               p_order_id      => v_order_id     -- ID заказа - услуги
           );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Зарегистрировать в БИЛЛИНГЕ тарифный план на
-- "Услуги присоединения и пропуска трафика на местном и/или зоновом уровне" 
--
PROCEDURE Register_rateplan(
              p_rateplan_name IN VARCHAR2,  -- имя карифного плана
              p_rateplan_code IN VARCHAR2,  -- код - краткое наименование тарифного плана
              p_rateplan_id   OUT INTEGER   -- ID заказа
          )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Register_rateplan';
BEGIN
    -- Создать описатель тарифного плана для СЗТ
    p_rateplan_id := PK17_RATEPLANE.Add_rateplan(
              p_rateplan_id   => SQ_RATEPLAN_ID.NEXTVAL,
              p_tax_incl      => PK00_CONST.c_RATEPLAN_TAX_INCL,
              p_rateplan_name => p_rateplan_name,
              p_ratesystem_id => PK00_CONST.c_RATESYS_TOPS_ID,
              p_rateplan_code => p_rateplan_code,
              p_service_id    => PK00_CONST.c_SERVICE_OP_LOCAL
           );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Д О П О Л Н И Т Е Л Ь Н Ы Е    Ф У Н К Ц И И
--============================================================================================
-- Заполнить юридический адрес покупателя
FUNCTION Set_customer_address(
               p_customer_id  IN VARCHAR2,
               p_address_type IN VARCHAR2,
               p_country      IN VARCHAR2, 
               p_zip          IN VARCHAR2,
               p_state        IN VARCHAR2,
               p_city         IN VARCHAR2, 
               p_address      IN VARCHAR2,
               p_date_from    IN DATE,
               p_date_to      IN DATE
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_customer_address';
BEGIN
    RETURN PK13_CUSTOMER.Set_address(
               p_customer_id,
               p_address_type,
               p_country, 
               p_zip,
               p_state,
               p_city, 
               p_address,
               p_date_from,
               p_date_to
           );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Добавить юридический адрес к лицевому счету, возвращает значения
--   - положительное - id контакта 
--   - при ошибке выставляет исключение
--
FUNCTION Add_jur_address(
               p_account_id    IN INTEGER,
               p_country       IN VARCHAR2,
               p_zip           IN VARCHAR2,
               p_state         IN VARCHAR2,
               p_city          IN VARCHAR2,
               p_address       IN VARCHAR2,
               p_person        IN VARCHAR2,
               p_phones        IN VARCHAR2,
               p_fax           IN VARCHAR2,
               p_email         IN VARCHAR2,
               p_date_from     IN DATE,
               p_date_to       IN DATE DEFAULT NULL ,
               p_notes         IN VARCHAR2 DEFAULT NULL
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Add_jur_address';
BEGIN
    RETURN PK05_ACCOUNT.Add_address(
               p_account_id,
               PK00_CONST.c_ADDR_TYPE_JUR,
               p_country,
               p_zip,
               p_state,
               p_city,
               p_address,
               p_person,
               p_phones,
               p_fax,
               p_email,
               p_date_from,
               p_date_to,
               p_notes
           );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Добавить адрес доставки к лицевому счету, возвращает значения
--   - положительное - id контакта 
--   - при ошибке выставляет исключение
--
FUNCTION Add_dlv_address(
               p_account_id    IN INTEGER,
               p_country       IN VARCHAR2,
               p_zip           IN VARCHAR2,
               p_state         IN VARCHAR2,
               p_city          IN VARCHAR2,
               p_address       IN VARCHAR2,
               p_person        IN VARCHAR2,
               p_phones        IN VARCHAR2,
               p_fax           IN VARCHAR2,
               p_email         IN VARCHAR2,
               p_date_from     IN DATE,
               p_date_to       IN DATE DEFAULT NULL ,
               p_notes         IN VARCHAR2 DEFAULT NULL
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_dlv_address';
BEGIN
    RETURN PK05_ACCOUNT.Add_address(
               p_account_id,
               PK00_CONST.c_ADDR_TYPE_DLV,
               p_country,
               p_zip,
               p_state,
               p_city,
               p_address,
               p_person,
               p_phones,
               p_fax,
               p_email,
               p_date_from,
               p_date_to,
               p_notes
           );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- создать нового менеджера, возвращает значения
--   - положительное - ID менеджера, 
--   - при ошибке выставляет исключение
FUNCTION New_manager(
               p_contractor_id    IN INTEGER,
               p_department       IN VARCHAR2,
               p_position         IN VARCHAR2, 
               p_last_name        IN VARCHAR2, -- фамилия
               p_first_name       IN VARCHAR2, -- имя 
               p_middle_name      IN VARCHAR2, -- отчество
               p_phones           IN VARCHAR2,
               p_email            IN VARCHAR2,
               p_date_from        IN DATE,
               p_date_to          IN DATE DEFAULT NULL
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'New_manager';
BEGIN
    RETURN PK15_MANAGER.New_manager(
               p_contractor_id,
               p_department,
               p_position, 
               p_last_name,
               p_first_name, 
               p_middle_name,
               p_phones,
               p_email,
               p_date_from,
               p_date_to
           );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- Назначить менеджера на договор/лицевой счет/заказ
PROCEDURE Set_manager( 
               p_manager_id  IN INTEGER,
               p_contract_id IN INTEGER,
               p_account_id  IN INTEGER,
               p_order_id    IN INTEGER,
               p_date_from   IN DATE,
               p_date_to     IN DATE DEFAULT NULL
             )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_manager';
BEGIN
    PK15_MANAGER.Set_manager_info( 
               p_manager_id,
               p_contract_id,
               p_account_id,
               p_order_id,
               p_date_from,
               p_date_to
             );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ============================================================================== --
-- Удалить клиента (все что создано процедурой New_client)
-- ============================================================================== --
PROCEDURE Remove_client(
                  p_account_id IN INTEGER
               )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'New_client';
    --
    v_acc_type       ACCOUNT_T.ACCOUNT_TYPE%TYPE;
    v_order_id       INTEGER;
    v_rateplan_id    INTEGER;
    v_customer_id    INTEGER;
    v_contract_id    INTEGER;
    v_client_id      INTEGER;
    v_profile_id     INTEGER;
    v_manager_id     INTEGER;
    --
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName);
    -- убеждаемся что л/с существует и принадлежит "Ю"рику
    SELECT ACCOUNT_TYPE INTO v_acc_type
    FROM ACCOUNT_T
    WHERE ACCOUNT_ID = p_account_id;
    IF v_acc_type != 'J' THEN
        Pk01_Syslog.Write_msg('Account_id='||p_account_id||', has a wrong type "'||v_acc_type||'"', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'Account_id='||p_account_id||', has a wrong type "'||v_acc_type||'"');
    END IF;
    -- удаляем INVOICE_ITEM
    DELETE FROM INVOICE_ITEM_T II
    WHERE II.BILL_ID IN (
        SELECT B.BILL_ID FROM BILL_T B
        WHERE B.ACCOUNT_ID = p_account_id
    );
    -- удаляем ITEM
    DELETE FROM ITEM_T I
    WHERE EXISTS (
        SELECT * FROM BILL_T B
         WHERE B.ACCOUNT_ID = p_account_id
           AND I.BILL_ID = B.BILL_ID
           AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
    );
    -- удаляем BILL
    DELETE FROM BILL_T B
    WHERE B.ACCOUNT_ID = p_account_id;
    -- удаляем BILLINFO_T
    DELETE FROM BILLINFO_T
    WHERE ACCOUNT_ID = p_account_id;
    -- удаляем BILL_T
    DELETE FROM BILL_T
    WHERE ACCOUNT_ID = p_account_id;
    -- удаляем данные заказа (заказ создавали один)
    SELECT ORDER_ID, RATEPLAN_ID INTO v_order_id, v_rateplan_id
    FROM ORDER_T
    WHERE ACCOUNT_ID = p_account_id;
    -- удаляем телефонные номера на заказе
    DELETE FROM ORDER_PHONES_T 
    WHERE ORDER_ID = v_order_id;
    -- удаляем привязку менеджера к клиенту
    SELECT MANAGER_ID INTO v_manager_id
    FROM SALE_CURATOR_T
    WHERE ACCOUNT_ID = p_account_id OR ORDER_ID = v_order_id;
    DELETE FROM SALE_CURATOR_T
    WHERE MANAGER_ID = v_manager_id;
    -- удаляем ресурсы
    DELETE FROM ORDER_SWTG_T
    WHERE ORDER_ID = v_order_id;
    -- удаляем строки заказа
    DELETE FROM ORDER_BODY_T
    WHERE ORDER_ID = v_order_id;
    -- удаляем заказ
    DELETE FROM ORDER_T 
    WHERE ORDER_ID = v_order_id;
    -- удалем тарифный план (при новом импорте воссоздадим)
    DELETE FROM RATEPLAN_T
    WHERE RATEPLAN_ID = v_rateplan_id;
    -- удаляем адреса
    DELETE FROM ACCOUNT_CONTACT_T
    WHERE ACCOUNT_ID = p_account_id;
    --    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    -- Удаляем окружение
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    SELECT AP.PROFILE_ID, AP.CONTRACT_ID, AP.CUSTOMER_ID, C.CLIENT_ID
    INTO v_profile_id, v_contract_id, v_customer_id, v_client_id
    FROM ACCOUNT_PROFILE_T AP, CONTRACT_T C
    WHERE AP.ACCOUNT_ID = p_account_id
      AND AP.CONTRACT_ID = C.CONTRACT_ID;
    -- удаляем ACCOUNT_PROFILE_T
    DELETE FROM ACCOUNT_PROFILE_T
    WHERE PROFILE_ID = v_profile_id;
    -- удаляем лицевой счет
    DELETE FROM ACCOUNT_T
    WHERE ACCOUNT_ID = p_account_id;
    -- удаляем договор
    DELETE FROM CONTRACT_T
    WHERE CONTRACT_ID = v_contract_id;
    -- удаляем клиента
    DELETE FROM CLIENT_T
    WHERE CLIENT_ID = v_client_id;
    -- удаляем юр.адрес покупателя
    DELETE FROM CUSTOMER_ADDRESS_T
    WHERE CUSTOMER_ID = v_customer_id;
    -- удаляем покупателя
    DELETE FROM CUSTOMER_T
    WHERE CUSTOMER_ID = v_customer_id;
    -- удаляем менеджера
    DELETE FROM MANAGER_T 
    WHERE MANAGER_ID = v_manager_id ;
    --
    Pk01_Syslog.Write_msg('The end.', c_PkgName||'.'||v_prcName);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

END PK203_SPB_OPERATOR;
/
