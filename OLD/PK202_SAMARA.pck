CREATE OR REPLACE PACKAGE PK202_SAMARA
IS
    --
    -- Пакет для поддержки импорта данных для клиента
    -- 'Самарский государственный университет путей сообщения'
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK202_SAMARA';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    -- ID продавца для физического лица CONTRACTOR_T (продавец один на всех)
    c_CONTRACTOR_KTTK_ID constant integer := 1;
    -- ID лицевого счета клиента 'СамГУПС'
    c_SAMARA_ACCOUNT_ID  constant integer := 993556;
    -- ID тарифного плана клиента 'СамГУПС'
    c_SAMARA_TARIFF_ID   constant integer := 12076;
    -- имя и ID коммутатора
    c_SW_NAME constant varchar2(13) := 'samara_si3000';
    c_SW_ID   constant integer      := 3000;
    
    TYPE phones_t IS TABLE OF VARCHAR2(7) INDEX BY BINARY_INTEGER;
    g_phones phones_t;
    
    -- Инициализация списка телефонов
    PROCEDURE Init_phones_list;
    
    -- Создание тестового клиента для Самары
    PROCEDURE New_client;
    
    --===========================================================================--
    --                    К   О   Н   Т   Р   О   Л   Ь                          --
    -- ==========================================================================--
    -- Контроль привязки CDR коммутатора Самары
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE CDR_bind_report( 
                   p_recordset  OUT t_refc,
                   p_date_from  IN DATE,
                   p_date_to    IN DATE
               );
           
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Контроль тарификации трафика Самары
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE BDR_report( 
                   p_recordset      OUT t_refc,
                   p_period_id_from IN INTEGER, -- YYYYMM
                   p_period_id_to   IN INTEGER  -- YYYYMM
               );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Просмотр CDR xTTK
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE xTTK_CDR( 
                   p_recordset  OUT t_refc,
                   p_date_from   IN DATE,
                   p_date_to     IN DATE, 
                   p_order_id    IN INTEGER DEFAULT NULL
               );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Просмотр CDR  клиента 'СамГУПС'
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE View_CDR( 
                   p_recordset  OUT t_refc,
                   p_date_from   IN DATE,
                   p_date_to     IN DATE
               );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Просмотр BDR клиента 'СамГУПС'
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE View_BDR( 
                   p_recordset  OUT t_refc,
                   p_date_from   IN DATE,
                   p_date_to     IN DATE
               );
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Поиск тарифа на указанную дату, для номера В
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Find_tariff( 
                   p_recordset  OUT t_refc,
                   p_date       IN DATE DEFAULT SYSDATE, -- дата на которую просматриваем тариф
                   p_phone_b      IN VARCHAR2 DEFAULT NULL -- номер телефона (B), для которого ищем информацию
               );
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Список телефонов на заказах 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Phone_list( 
                   p_recordset  OUT t_refc
               );
               
    --
    -- список диапазонов телефонов на заказах 
    --
    PROCEDURE Phone_range_list( 
                   p_recordset  OUT t_refc
               );
----------------------------------------------
-- Поиск ошибочных BDR в опр. периоде
----------------------------------------------
PROCEDURE GetErrBDRbyPeriod( 
               p_recordset  OUT t_refc,
               p_period   IN INTEGER,
               p_error    IN INTEGER
           );           
---------------------------------------------------------
-- Просмотр логов последнего закрытия
---------------------------------------------------------
PROCEDURE GetLastCloseLog( 
               p_recordset  OUT t_refc
           );  
--------------------------------------------------
-- получить тарификационные записи о вызовах (BDR)
--   - при ошибке выставляет исключение
--------------------------------------------------
PROCEDURE View_BDR( 
               p_recordset      OUT t_refc, 
               p_period_id       IN INTEGER,              -- id отчетного периода    
               p_date_from       IN DATE,                 -- диапазон дат
               p_date_to         IN DATE,                 --   начала вызовов
               p_list_order_no   IN VARCHAR2 DEFAULT NULL,-- список номеров заказов
               p_list_abn_a      IN VARCHAR2 DEFAULT NULL,-- список вызывающих абонентов
               p_abc             IN VARCHAR2 DEFAULT NULL,-- префикс вызываемого номера
               p_direction       IN VARCHAR2 DEFAULT NULL,-- направление вызова
               p_min_duration    IN NUMBER   DEFAULT NULL,-- минимальная длительность вызова
               p_max_duration    IN NUMBER   DEFAULT NULL,-- минимальная длительность вызова
               p_min_amount      IN NUMBER   DEFAULT NULL,-- минимальная сумма за вызов
               p_max_amount      IN NUMBER   DEFAULT NULL -- минимальная сумма за вызов
           );
    -- разбить строку на массив подстрок
    PROCEDURE Split_string (
            p_table  OUT vc100_table_t,
            p_list   VARCHAR2,
            p_delim  VARCHAR2 DEFAULT ','
        );                      
    
END PK202_SAMARA;
/
CREATE OR REPLACE PACKAGE BODY PK202_SAMARA
IS

--=========================================================================--
-- Инициализация списка номеров телефонов
--=========================================================================--
PROCEDURE Init_phones_list 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Init_phones_list';
BEGIN
    /*
    2556875,2556876,
    2556892,2556893,2556894,2556895,2556896
    2556861,2556862,2556863,2556864,2556865,2556867,
    2556836,2556837,2556838,2556839,2556840,2556841,2556842,2556843,2556845,2556846,2556847,2556848,2556849,2556851,2556852,2556853,2556854,2556856,2556857,2556858,2556859,
    2056954,
    2556881,2556882,2556883 
    2556948,2556949,2556950,2556951,2556952,2556953,2556954
    2556752,2556753,2556754,2556755,2556756,2556757, 
    2556762, 
    2556764, 2556765, 2556766, 
    2556732 - 2556749
    2556700 - 2556731
    */
    g_phones(2056954) := '2056954';
    FOR n IN 2556700..2556749 LOOP g_phones(n) := TO_CHAR(n); END LOOP;
    FOR n IN 2556752..2556757 LOOP g_phones(n) := TO_CHAR(n); END LOOP;
    g_phones(2556762) := '2556762';
    FOR n IN 2556764..2556766 LOOP g_phones(n) := TO_CHAR(n); END LOOP;
    FOR n IN 2556836..2556859 LOOP g_phones(n) := TO_CHAR(n); END LOOP;    
    FOR n IN 2556861..2556867 LOOP g_phones(n) := TO_CHAR(n); END LOOP;
    FOR n IN 2556875..2556876 LOOP g_phones(n) := TO_CHAR(n); END LOOP;
    FOR n IN 2556881..2556883 LOOP g_phones(n) := TO_CHAR(n); END LOOP;
    FOR n IN 2556892..2556896 LOOP g_phones(n) := TO_CHAR(n); END LOOP;
    FOR n IN 2556948..2556954 LOOP g_phones(n) := TO_CHAR(n); END LOOP;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;    
    

--=========================================================================--
-- Создание тестового клиента для Самары
--=========================================================================--
PROCEDURE New_client
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'New_client';
    v_date_from      DATE := TO_DATE('01.10.2013','dd.mm.yyyy');
    v_brand_id       INTEGER;
    v_manager_id     INTEGER;
    v_client_id      INTEGER;
    v_rateplan_id    INTEGER;
    v_customer_id    INTEGER;
    v_cust_addr_id   INTEGER;
    v_contract_id    INTEGER;
    v_account_id     INTEGER;
    v_profile_id     INTEGER;
    v_jur_address_id INTEGER;
    v_dlv_address_id INTEGER;
    v_order_id       INTEGER;
    v_order_mg_id    INTEGER;
    v_order_mn_id    INTEGER;
    v_bill_id        INTEGER;
    idx              BINARY_INTEGER;
    v_rowid          VARCHAR2(200);
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName);
    -- Создать менеджера
    v_manager_id := Pk15_Manager.New_manager(
              p_contractor_id => c_CONTRACTOR_KTTK_ID,
              p_department => 'ДРУ',
              p_position => 'менеджер',
              p_last_name => 'Самарский',
              p_first_name => 'Иван',
              p_middle_name => 'Иванович',
              p_phones => NULL,
              p_email => NULL,
              p_date_from => v_date_from
           );

    Pk01_Syslog.Write_msg('v_manager_id='||v_manager_id, c_PkgName||'.'||v_prcName);

    -- Создать описатель тарифного плана для Самары
    v_rateplan_id := PK17_RATEPLANE.Add_rateplan(
               p_rateplan_id    => SQ_RATEPLAN_ID.NEXTVAL,
               p_tax_incl       => PK00_CONST.c_RATEPLAN_TAX_NOT_INCL,
               p_rateplan_name  => 'СамГУПС',
               p_ratesystem_id  => PK00_CONST.c_RATESYS_MMTS_ID,
               p_service_id     => PK00_CONST.c_SERVICE_CALL_LZ,
               p_subservice_id  => NULL,
               p_rateplan_code  => NULL
           );

    Pk01_Syslog.Write_msg('v_rateplan_id='||v_rateplan_id, c_PkgName||'.'||v_prcName);

    -- Создать клиента
    v_client_id := Pk11_Client.New_client(
              p_name => 'Самарский государственный университет путей сообщения'
           );    
           
    Pk01_Syslog.Write_msg('v_client_id='||v_client_id, c_PkgName||'.'||v_prcName);

    -- Создать покупателя
    v_customer_id := Pk13_Customer.New_customer(
              p_erp_code    => NULL,
              p_inn         => '6318100463',
              p_kpp         => '631801001', 
              p_name        => 'Самарский государственный университет путей сообщения',
              p_short_name  => 'СамГУПС'
           );

    Pk01_Syslog.Write_msg('v_customer_id='||v_customer_id, c_PkgName||'.'||v_prcName);

    -- Установить юридический адрес покупателя, возвращает значения
    v_cust_addr_id := Pk13_Customer.Set_address(
              p_customer_id  => v_customer_id,
              p_address_type => PK00_CONST.c_ADDR_TYPE_JUR,
              p_country      => 'РФ', 
              p_zip          => '443066',
              p_state        => 'Самарская обл.',
              p_city         => 'Самара', 
              p_address      => '1-ый Безымянный пер.18',
              p_date_from    => v_date_from,
              p_date_to      => NULL
           );

    Pk01_Syslog.Write_msg('v_cust_addr_id='||v_cust_addr_id, c_PkgName||'.'||v_prcName);

    -- Создать договор
    v_contract_id := PK12_CONTRACT.Open_contract(
              p_contract_no=> 'SM01',
              p_date_from  => v_date_from,
              p_date_to    => NULL,
              p_client_id  => v_client_id,
              p_manager_id => v_manager_id
           );

    Pk01_Syslog.Write_msg('v_contract_id='||v_contract_id, c_PkgName||'.'||v_prcName);

    -- Создать лицевой счет покупателя
    v_account_id := PK05_ACCOUNT.New_account(
              p_account_no   => 'SM01',
              p_account_type => PK00_CONST.c_ACC_TYPE_J,
              p_currency_id  => PK00_CONST.c_CURRENCY_RUB,
              p_status       => PK00_CONST.c_ACC_STATUS_BILL,
              p_parent_id    => NULL
           );

    Pk01_Syslog.Write_msg('v_account_id='||v_account_id, c_PkgName||'.'||v_prcName);

    -- Назначить менеджера на договор/лицевой счет/заказ
    Pk15_Manager.Set_manager_info (
              p_manager_id  => v_manager_id,
              p_contract_id => v_contract_id,
              p_account_id  => v_account_id,
              p_order_id    => NULL,
              p_date_from   => v_date_from,
              p_date_to     => NULL
           );

    -- создать профиль лицевого счета
    v_profile_id := PK05_ACCOUNT.Set_profile(
               p_account_id    => v_account_id,
               p_brand_id      => v_brand_id,
               p_contract_id   => v_contract_id,
               p_customer_id   => v_customer_id,
               p_subscriber_id => NULL,
               p_contractor_id => c_CONTRACTOR_KTTK_ID,
               p_branch_id     => NULL,
               p_agent_id      => NULL,
               p_contractor_bank_id => NULL,
               p_vat           => Pk00_Const.c_VAT,
               p_date_from     => v_date_from,
               p_date_to       => NULL
           );

    Pk01_Syslog.Write_msg('v_profile_id='||v_profile_id, c_PkgName||'.'||v_prcName);
           
    -- добавить юридический адрес
    v_jur_address_id := PK05_ACCOUNT.Add_address(
               p_account_id    => v_account_id,
               p_address_type  => PK00_CONST.c_ADDR_TYPE_JUR,
               p_country       => 'РФ',
               p_zip           => '443066',
               p_state         => 'Самарская обл.',
               p_city          => 'Самара',
               p_address       => '1-ый Безымянный пер.18',
               p_person        => 'Андрончев И.К.',
               p_phones        => NULL,
               p_fax           => NULL,
               p_email         => NULL,
               p_date_from     => v_date_from,
               p_date_to       => NULL,
               p_notes         => NULL
           );
                                  
    Pk01_Syslog.Write_msg('v_jur_address_id='||v_jur_address_id, c_PkgName||'.'||v_prcName);
           
    -- добавить адрес доставки счета
    v_dlv_address_id := PK05_ACCOUNT.Add_address(
              p_account_id   => v_account_id,
              p_address_type => PK00_CONST.c_ADDR_TYPE_DLV,
              p_country      => 'РФ', 
              p_zip          => '443066',
              p_state        => 'Самарская обл.',
              p_city         => 'Самара', 
              p_address      => '1-ый Безымянный пер.18',
              p_person       => 'Андрончев И.К.',
              p_phones       => NULL,
              p_fax          => NULL,
              p_email        => NULL,
              p_date_from    => v_date_from,
              p_date_to      => NULL,
              p_notes         => NULL
           );
           
    Pk01_Syslog.Write_msg('v_dlv_address_id='||v_dlv_address_id, c_PkgName||'.'||v_prcName);

    -- создаем заказ на услуги МГ/МН связи
    v_order_id := PK06_ORDER.New_order(
              p_account_id => v_account_id,
              p_order_no   => 'SM01',
              p_service_id => PK00_CONST.c_SERVICE_CALL_MGMN,
              p_rateplan_id=> v_rateplan_id,
              p_time_zone=> NULL,
              p_date_from  => v_date_from,
              p_date_to    => NULL
           );
           
    Pk01_Syslog.Write_msg('v_order_id='||v_order_id, c_PkgName||'.'||v_prcName);
           
    -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    -- создаем строку заказа для МГ
    v_order_mg_id := PK06_ORDER.Add_subservice(
              p_order_id      => v_order_id,
              p_subservice_id => PK00_CONST.c_SUBSRV_MG,
              p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
              p_date_from     => v_date_from,
              p_date_to       => NULL
           );
           
    Pk01_Syslog.Write_msg('v_order_mg_id='||v_order_mg_id, c_PkgName||'.'||v_prcName);
           
    -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    -- создаем строку заказа для МН
    v_order_mn_id := PK06_ORDER.Add_subservice(
              p_order_id      => v_order_id,
              p_subservice_id => PK00_CONST.c_SUBSRV_MN,
              p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
              p_date_from     => v_date_from,
              p_date_to       => NULL
           );
           
    Pk01_Syslog.Write_msg('v_order_mn_id='||v_order_mn_id, c_PkgName||'.'||v_prcName);
           
    -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    -- инициализируем список телефонов на заказе
    Init_phones_list;
    
    -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    -- переносим телефоны на заказ
    --Pk01_Syslog.Write_msg('Count='||g_phones.COUNT, c_PkgName||'.'||v_prcName);    
    idx := g_phones.FIRST;
    LOOP
        -- выводим текущий элемент
        --Pk01_Syslog.Write_msg('Phone='||g_phones(idx), c_PkgName||'.'||v_prcName);
        v_rowid := PK18_RESOURCE.Add_phone(
                       p_order_id  => v_order_id,
                       p_phone     => '7846'||g_phones(idx),
                       p_date_from => v_date_from,
                       p_date_to   => NULL
                   );
        -- условие выхода - равенство счетчика индексу последнего элемента
        EXIT WHEN idx = g_phones.LAST;
        -- счетчик устанавливаем на индекс следующего элемента
        idx := g_phones.NEXT(idx);
    END LOOP;  
    --Pk01_Syslog.Write_msg('The end.', c_PkgName||'.'||v_prcName);
    -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    
    -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    -- Формируем счета
    -- читаем глобальный описатеь периода
    SELECT PERIOD_FROM INTO v_date_from FROM PERIOD_T
    WHERE POSITION = 'OPEN';
    
    -- создание описателя счетов и самих счетов для нового Л/С
    v_bill_id := Pk07_Bill.New_billinfo (
                   p_account_id    => v_account_id,   -- ID лицевого счета
                   p_currency_id   => Pk00_Const.c_CURRENCY_RUB  -- ID валюты счета
               );

    Pk01_Syslog.Write_msg('v_bill_id='||v_bill_id, c_PkgName||'.'||v_prcName);
    --
    Pk01_Syslog.Write_msg('End. Добавлено '||g_phones.COUNT||' номеров.', c_PkgName||'.'||v_prcName);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------------ --
-- Удалить клиента (все что создано процедурой New_client)
-- ------------------------------------------------------------------------------ --
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

--===========================================================================--
--                    К   О   Н   Т   Р   О   Л   Ь                          --
-- ==========================================================================--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Контроль привязки CDR коммутатора Самары
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE CDR_bind_report( 
               p_recordset  OUT t_refc,
               p_date_from  IN DATE,
               p_date_to    IN DATE
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'CDR_bind_report';
    v_retcode     INTEGER;
BEGIN
    OPEN p_recordset FOR
      SELECT --/*+ PARALLEL(MDV.X03_XTTK_CDR 10) */
             TRUNC(X.ANS_TIME) CDR_DATE, 
             COUNT(*) RECORDS,
             SUM(CASE WHEN ORDER_ID_A >=0 THEN 1 ELSE 0 END) BIND,
             SUM(CASE WHEN ORDER_ID_A < 0 THEN 1 ELSE 0 END) NOT_BIND,
             SUM(CASE WHEN ORDER_ID_A IS NULL THEN 1 ELSE 0 END) UNKNOWN
        FROM MDV.X03_XTTK_CDR X
       WHERE SW_NAME = c_SW_NAME
         AND ANS_TIME BETWEEN p_date_from AND p_date_to
      GROUP BY TRUNC(X.ANS_TIME)
      ORDER BY 1
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);  
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Контроль тарификации трафика Самары
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE BDR_report( 
               p_recordset      OUT t_refc,
               p_period_id_from IN INTEGER, -- YYYYMM
               p_period_id_to   IN INTEGER  -- YYYYMM
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'BDR_report';
    v_retcode     INTEGER;
    v_period_from DATE;
    v_period_to   DATE;
BEGIN
    --
    v_period_from := PK04_PERIOD.Period_from(p_period_id_from);
    v_period_to   := PK04_PERIOD.Period_to(p_period_id_to);
    -- отчет 
    OPEN p_recordset FOR
      SELECT TO_CHAR(B.REP_PERIOD,'yyyymm') REP_PERIOD_ID, COUNT(*) RECORDS,
             SUM(DECODE(B.TRF_TYPE,0,1,0)) LOCAL,
             SUM(DECODE(B.TRF_TYPE,9,1,0)) ZONE,
             SUM(DECODE(B.TRF_TYPE,7,1,0)) RUS,
             SUM(DECODE(B.TRF_TYPE,8,1,0)) INTER,
             SUM(DECODE(B.BDR_STATUS,-6,1,0)) E_TARIFF_NOT_FOUND,
             SUM(DECODE(B.BDR_STATUS,-7,1,0)) E_PRICE_NOT_FOUND,
             SUM(CASE WHEN B.BDR_STATUS < 0 
                      AND B.BDR_STATUS NOT IN (PK00_CONST.c_TARIFF_NOT_FOUND, PK00_CONST.c_PRICE_NOT_FOUND) 
                      THEN 1 ELSE 0 
                 END) E_OTHERS, p.position
        FROM E02_BDR_XTTK B, PERIOD_T P
       WHERE B.REP_PERIOD BETWEEN v_period_from AND v_period_to
         AND B.ACCOUNT_ID = c_SAMARA_ACCOUNT_ID
         AND p.period_id = TO_CHAR(b.rep_period, 'yyyymm')
       GROUP BY TO_CHAR(B.REP_PERIOD,'yyyymm'),p.position
       ORDER BY TO_CHAR(B.REP_PERIOD,'yyyymm')
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);  
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Просмотр CDR 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE xTTK_CDR( 
               p_recordset  OUT t_refc,
               p_date_from   IN DATE,
               p_date_to     IN DATE, 
               p_order_id    IN INTEGER DEFAULT NULL
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'xTTK_CDR';
    v_retcode    INTEGER;
BEGIN
    -- список CDR для 
    OPEN p_recordset FOR
        SELECT O.ORDER_NO, X.ANS_TIME, X.CONVERSATION_TIME SECONDS, 
               X.CALLER_NUMBER, X.CALLED_NUMBER, 
               X.TRUNK_GROUP_IN, X.TRUNK_GROUP_OUT, X.SW_NAME  
          FROM MDV.X03_XTTK_CDR X, ORDER_T O
         WHERE SW_NAME = c_SW_NAME
           AND ANS_TIME BETWEEN p_date_from AND p_date_to
           AND X.ORDER_ID_A = O.ORDER_ID(+)
           AND (p_order_id IS NULL OR p_order_id = X.ORDER_ID_A)
        ;   
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);  
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Просмотр CDR  клиента 'СамГУПС'
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE View_CDR( 
               p_recordset  OUT t_refc,
               p_date_from   IN DATE,
               p_date_to     IN DATE
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'View_CDR';
    v_retcode    INTEGER;
BEGIN
    -- список CDR для 
    OPEN p_recordset FOR
        SELECT O.ORDER_NO, X.ANS_TIME, X.CONVERSATION_TIME SECONDS, 
               X.CALLER_NUMBER, X.CALLED_NUMBER, 
               X.TRUNK_GROUP_IN, X.TRUNK_GROUP_OUT, X.SW_NAME  
          FROM MDV.X03_XTTK_CDR X, ORDER_T O
         WHERE SW_NAME = c_SW_NAME
           AND ANS_TIME BETWEEN p_date_from AND p_date_to
           AND X.ORDER_ID_A   = O.ORDER_ID
           AND O.ACCOUNT_ID = c_SAMARA_ACCOUNT_ID
        ;   
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);  
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Просмотр BDR клиента 'СамГУПС'
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE View_BDR( 
               p_recordset  OUT t_refc,
               p_date_from   IN DATE,
               p_date_to     IN DATE
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'View_BDR';
    v_retcode    INTEGER;
BEGIN
    -- список CDR для 
    OPEN p_recordset FOR
      SELECT A.ACCOUNT_NO, O.ORDER_NO, 
             TO_CHAR(B.REP_PERIOD,'yyyymm') REP_PERIOD_ID,
             B.START_TIME, B.ABN_A, B.ABN_B, B.AMOUNT, B.TARIFF, B.BILL_MINUTES, 
             B.ABC_B, TD.DIRECTION_NAME, 
             DECODE(B.TRF_TYPE, 0, 'МЕСТ', 7, 'МГ', 8, 'МН', 9, 'ЗОНА', TO_CHAR(B.TRF_TYPE)) TRF_TYPE,
             B.BDR_STATUS,
             B.ACCOUNT_ID, B.ORDER_ID, B.TARIFF_ID, B.DIR_B_ID 
        FROM E02_BDR_XTTK B, 
             ACCOUNT_T A, ORDER_T O,
             TARIFF_CB.TRF02_DIRECTION TD
       WHERE B.START_TIME BETWEEN p_date_from AND p_date_to
         AND B.ACCOUNT_ID = A.ACCOUNT_ID(+)
         AND B.ORDER_ID   = O.ORDER_ID(+)
         AND B.TARIFF_ID  = TD.TARIFF_ID(+)
         AND B.DIR_B_ID   = TD.DIRECTION_ID(+)
         AND (TD.DATE_FROM IS NULL OR TD.DATE_FROM <= B.START_TIME)
         AND (TD.DATE_TO   IS NULL OR B.START_TIME <= TD.DATE_TO)
         AND B.ACCOUNT_ID = c_SAMARA_ACCOUNT_ID
      ;   
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);  
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Поиск тарифа на указанную дату, для номера В
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Find_tariff( 
               p_recordset  OUT t_refc,
               p_date       IN DATE DEFAULT SYSDATE, -- дата на которую просматриваем тариф
               p_phone_b      IN VARCHAR2 DEFAULT NULL -- номер телефона (B), для которого ищем информацию
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_tariff';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR
      SELECT T1.TARIFF_NAME, T2.DIRECTION_NAME, T3.DN, T4.PRICE,  
             T1.TARIFF_ID, T2.DIRECTION_ID, T4.PRICE_ID 
        FROM TARIFF_CB.TRF01_TARIFF T1,
             TARIFF_CB.TRF02_DIRECTION T2,
             TARIFF_CB.TRF03_DN_CODE T3, 
             TARIFF_CB.TRF04_PRICE T4
       WHERE T1.TARIFF_ID = c_SAMARA_TARIFF_ID
         AND T1.TARIFF_ID = T2.TARIFF_ID
         AND T2.DIRECTION_ID = T3.DIRECTION_ID
         AND T2.SIDE = 'B'
         AND p_date BETWEEN T2.DATE_FROM AND T2.DATE_TO
         AND p_date BETWEEN T3.DATE_FROM AND T3.DATE_TO
         AND T4.TERM_DIR_ID = T2.DIRECTION_ID
         AND T4.TARIFF_ID = T1.TARIFF_ID
         AND (p_phone_b IS NULL OR p_phone_b LIKE T3.DN||'%')
      ;   
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);  
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Список телефонов на заказах 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Phone_list( 
               p_recordset  OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Phone_list';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR
      SELECT O.ORDER_NO, OP.PHONE_NUMBER, OP.DATE_FROM, OP.DATE_TO 
        FROM ORDER_PHONES_T OP, ORDER_T O
       WHERE O.ACCOUNT_ID = c_SAMARA_ACCOUNT_ID
         AND O.ORDER_ID = OP.ORDER_ID 
       ORDER BY O.ORDER_NO, OP.PHONE_NUMBER, OP.DATE_FROM
      ;   
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);  
END;
--
-- список диапазонов телефонов на заказах 
--
PROCEDURE Phone_range_list( 
               p_recordset  OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Phone_range_list';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR
      SELECT ORDER_NO, PHONE_FROM, DECODE(PHONE_TO, PHONE_FROM, NULL, PHONE_TO) PHONE_TO,
             DATE_FROM, DATE_TO
          FROM (
          SELECT O.ORDER_NO, MIN(OP.PHONE_NUMBER) PHONE_FROM, MAX(OP.PHONE_NUMBER) PHONE_TO,
                 MAX(OP.DATE_FROM) DATE_FROM, MIN(OP.DATE_TO) DATE_TO 
            FROM ORDER_PHONES_T OP, ORDER_T O
           WHERE SYSDATE BETWEEN OP.DATE_FROM AND OP.DATE_TO
             AND O.ACCOUNT_ID = c_SAMARA_ACCOUNT_ID
             AND O.ORDER_ID = OP.ORDER_ID
          GROUP BY O.ORDER_NO, (OP.PHONE_NUMBER - ROWNUM + 1)
          ) ORDER BY ORDER_NO, PHONE_TO
      ;   
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);  
END;

----------------------------------------------
-- Поиск ошибочных BDR в опр. периоде
----------------------------------------------
PROCEDURE GetErrBDRbyPeriod( 
               p_recordset  OUT t_refc,
               p_period   IN INTEGER, -- ID периода
               p_error    IN INTEGER -- ID Ошибки
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'GetErrBDRbyPeriod';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR
      SELECT  b.LOCAL_TIME CALL_DATE, b.ABN_A, b.ABN_B, b.DURATION,
                 o.ORDER_NO, d.name as error_name
        FROM E02_BDR_XTTK B,
             TARIFF_CB.TRF02_DIRECTION a,
             TARIFF_CB.TRF02_DIRECTION bd,
             ORDER_T o,
             DICTIONARY_T d
       WHERE B.REP_PERIOD BETWEEN PK04_PERIOD.Period_from(p_period) and PK04_PERIOD.Period_to(p_period)
         AND B.ACCOUNT_ID =  c_SAMARA_ACCOUNT_ID
         and B.BDR_STATUS = p_error
         AND d.key = to_char(b.Bdr_Status)
         AND b.DIR_A_ID = a.DIRECTION_ID(+)
         AND b.DIR_B_ID = bd.DIRECTION_ID(+)
         AND b.order_id = o.order_id
         ORDER BY b.LOCAL_TIME
      ;   
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);  
END;

---------------------------------------------------------
-- Просмотр логов последнего закрытия
---------------------------------------------------------
PROCEDURE GetLastCloseLog( 
               p_recordset  OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'GetLastCloseLog';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR
      select * from PIN.L01_MESSAGES
      where SSID = (
            select SSID from PIN.L01_MESSAGES where L01_ID = (
                   select max(L01_ID) from PIN.L01_MESSAGES
                          where LOWER(MSG_SRC) = 'pk110_tariffing.trf_samara_cl_a')
                          )
      order by L01_ID;   
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);  
END;
--------------------------------------------------
-- получить тарификационные записи о вызовах (BDR)
--   - при ошибке выставляет исключение
--------------------------------------------------
PROCEDURE View_BDR( 
               p_recordset      OUT t_refc, 
               p_period_id       IN INTEGER,              -- id отчетного периода    
               p_date_from       IN DATE,                 -- диапазон дат
               p_date_to         IN DATE,                 --   начала вызовов
               p_list_order_no   IN VARCHAR2 DEFAULT NULL,-- список номеров заказов
               p_list_abn_a      IN VARCHAR2 DEFAULT NULL,-- список вызывающих абонентов
               p_abc             IN VARCHAR2 DEFAULT NULL,-- префикс вызываемого номера
               p_direction       IN VARCHAR2 DEFAULT NULL,-- направление вызова
               p_min_duration    IN NUMBER   DEFAULT NULL,-- минимальная длительность вызова
               p_max_duration    IN NUMBER   DEFAULT NULL,-- минимальная длительность вызова
               p_min_amount      IN NUMBER   DEFAULT NULL,-- минимальная сумма за вызов
               p_max_amount      IN NUMBER   DEFAULT NULL -- минимальная сумма за вызов
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'View_BDR';
    v_retcode       INTEGER;
    v_period_from   DATE ;
    v_period_to     DATE;
    v_min_duration  NUMBER := NVL(p_min_duration,-1000000);
    v_max_duration  NUMBER := NVL(p_max_duration, 1000000);
    v_min_amount    NUMBER := NVL(p_min_amount,  -1000000);
    v_max_amount    NUMBER := NVL(p_max_amount,   1000000);
    v_abn_table     vc100_table_t := vc100_table_t ();
    v_order_table   vc100_table_t := vc100_table_t ();
BEGIN
    v_period_from := TO_DATE(p_period_id,'YYYYMM');
    v_period_to   := LAST_DAY(v_period_from) + INTERVAL '00 23:59:59' DAY TO SECOND;
    --
    Split_string ( v_abn_table, p_list_abn_a, ',');
    Split_string ( v_order_table, p_list_order_no, ',');
    --    
    OPEN p_recordset FOR
          WITH list_abn_b AS (SELECT /*+ materialize */ * FROM TABLE(v_abn_table) ),
               list_order AS (SELECT /*+ materialize */ * FROM TABLE(v_order_table) )
          SELECT b.LOCAL_TIME CALL_DATE, b.ABN_A, b.ABN_B, b.DURATION,
                 b.ABC_A, a.DIRECTION_NAME DIR_FROM, b.ABC_B, bd.DIRECTION_NAME DIR_TO,
                 b.BILL_MINUTES, b.TARIFF, b.AMOUNT, o.ORDER_NO
            FROM PIN.E02_BDR_XTTK b,
                 TARIFF_CB.TRF02_DIRECTION a,
                 TARIFF_CB.TRF02_DIRECTION bd,
                 ORDER_T o
           WHERE b.DIR_A_ID = a.DIRECTION_ID(+)
             AND b.DIR_B_ID = bd.DIRECTION_ID(+)
             AND b.order_id = o.order_id
             AND b.REP_PERIOD BETWEEN v_period_from AND v_period_to
             AND b.LOCAL_TIME BETWEEN p_date_from AND p_date_to
             AND (p_list_order_no IS NULL OR o.ORDER_NO IN (SELECT * FROM list_order))
             AND (p_list_abn_a IS NULL OR b.ABN_A IN (SELECT * FROM list_abn_b))
             AND (p_abc IS NULL OR b.ABN_B LIKE p_abc||'%')
             AND (p_direction IS NULL OR LOWER(bd.DIRECTION_NAME) LIKE LOWER(p_direction||'%'))
             AND b.BILL_MINUTES BETWEEN v_min_duration AND v_max_duration
             AND b.AMOUNT BETWEEN v_min_amount AND v_max_amount
          ORDER BY b.LOCAL_TIME
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

------------------------------------------
-- Разбить строку с разделителями на части
------------------------------------------
FUNCTION Get_token(
        p_list   VARCHAR2,
        p_index  NUMBER,
        p_delim  VARCHAR2 DEFAULT ','
    ) RETURN VARCHAR2
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Get_token';
    v_start_pos NUMBER;
    v_end_pos   NUMBER;
BEGIN
    IF p_index = 1 THEN
        v_start_pos := 1;
    ELSE
        v_start_pos := INSTR(p_list, p_delim, 1, p_index - 1);
        IF v_start_pos > 0 THEN
            v_start_pos := v_start_pos + LENGTH(p_delim);            
        ELSE
            RETURN NULL;
        END IF;
    END IF;
    v_end_pos := INSTR(p_list, p_delim, v_start_pos, 1);
    IF v_end_pos > 0 THEN
        RETURN SUBSTR(p_list, v_start_pos, v_end_pos - v_start_pos);
    ELSE
        RETURN SUBSTR(p_list, v_start_pos);        
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.', c_PkgName||'.'||v_prcName );
END;
------------------------------------
-- разбить строку на массив подстрок
------------------------------------
PROCEDURE Split_string (
        p_table  OUT vc100_table_t,
        p_list   VARCHAR2,
        p_delim  VARCHAR2 DEFAULT ','
    )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Split_string';
    v_token     VARCHAR2(40);
    i           PLS_INTEGER := 1 ;
BEGIN
    -- на всякий случай чистим таблицу
    p_table := vc100_table_t();
    IF p_list IS NOT NULL THEN
        LOOP
            v_token := get_token( p_list, i , ',') ;
            EXIT WHEN v_token IS NULL ;
            p_table.EXTEND;
            p_table(i) := v_token;
            i := i + 1 ;
        END LOOP ;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_list='||p_list, c_PkgName||'.'||v_prcName );
END;


END PK202_SAMARA;
/
