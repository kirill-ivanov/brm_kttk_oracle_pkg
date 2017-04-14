CREATE OR REPLACE PACKAGE PK204_SPB_IMPORT
IS
    --
    -- Пакет для создания клиентов бренда xTTK «Санкт-Петербургский ТЕЛЕПОРТ»
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK204_SPB_IMPORT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    -- ID КТТК
    c_CONTRACTOR_KTTK_ID     CONSTANT INTEGER := 1;
    -- Поставщик для 'СПб филиал ТТК' 
    c_CONTRACTOR_SPB_XTTK_ID CONSTANT INTEGER := 8;
    -- Услуги присоединения и пропуска трафика на местном и/или зоновом уровне
    c_SERVICE_OPLOCL         CONSTANT INTEGER := 7;
    
    
    -- ----------------------------------------------------------------------------- --
    -- Экскорт данных из старого биллинга по бренду '.SPB TTK Brand'
    -- и услуге местного и зонового присоединения
    --
    PROCEDURE Import_data;
    
    -- ----------------------------------------------------------------------------- --
    -- Загрузка данных в биллинг
    --
    PROCEDURE Load_customers;
    
    -- ============================================================================== --
    -- Удалить клиента (все что создано процедурой New_client)
    -- ============================================================================== --
    PROCEDURE Remove_client(
                      p_account_id IN INTEGER
                   );
    
END PK204_SPB_IMPORT;
/
CREATE OR REPLACE PACKAGE BODY PK204_SPB_IMPORT
IS


--============================================================================================
-- Экскорт данных из старого биллинга по бренду '.SPB TTK Brand'
-- и услуге местного и зонового присоединения
--
PROCEDURE Import_data
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_data';
    v_count          INTEGER := 0;
BEGIN
    -- удаляем данные из временной таблицы
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK204_SPB_IMPORT_T DROP STORAGE';
    --
    -- загружаем данные во временную таблицу
    INSERT INTO PK204_SPB_IMPORT_T
    select 
        a.gl_segment BRAND, 
        CL.NAME_RU   CLIENT_NAME, 
        CI.AUTO_NO   CONTRACT_NO,
        AN.COMPANY   CUSTOMER, 
        CI.INN, CI.KPP, 
        A.ACCOUNT_NO, 
        A.CURRENCY   CURRENCY_ID,  
        S.LOGIN      ORDER_NO, 
        VS.ORDER_DATE, 
        VS.SERVICE_NAME,
        i2d@PINDB.WORLD(min(AP.CYCLE_START_T)) DATE_FROM, 
        i2d@PINDB.WORLD(max(AP.CYCLE_END_T))   DATE_TO,
        an.ZIP JUR_ZIP, an.STATE JUR_REGION, an.CITY JUR_CITY, an.ADDRESS JUR_ADDRESS,
        pii.ZIP DLV_ZIP, pii.STATE DLV_REGION, pii.CITY DLV_CITY, pii.ADDRESS DLV_ADDRESS,
        SYSDATE SAVE_DATE, NULL STATUS     
    from account_t@PINDB.WORLD a 
         inner join account_products_t@PINDB.WORLD ap on a.poid_id0 = ap.obj_id0
         inner join account_nameinfo_t@PINDB.WORLD an on a.poid_id0 = an.obj_id0 and an.rec_id = 1
         inner join plan_t@PINDB.WORLD p on ap.plan_obj_id0 = p.poid_id0
         inner join service_t@PINDB.WORLD s on ap.service_obj_id0 = s.poid_id0
         inner join profile_t@PINDB.WORLD pr on a.poid_id0 = pr.account_obj_id0
         inner join contract_info_t@PINDB.WORLD ci on pr.poid_id0 = ci.obj_id0
         inner join v_all_data_serv_plus@PINDB.WORLD vs on S.POID_ID0 = VS.POID_ID0
         inner join clients_t@PINDB.WORLD cl on CI.CLIENT_ID = CL.REC_ID
         inner join payinfo_t@PINDB.WORLD pi on a.poid_id0 = pi.account_obj_id0
         inner join payinfo_inv_t@PINDB.WORLD pii on pi.poid_id0 = pii.obj_id0
    where a.gl_segment = '.SPB TTK Brand' and VS.SERV_ID = 42
    group by a.gl_segment, CI.AUTO_NO, AN.COMPANY, A.ACCOUNT_NO, A.CURRENCY, CI.INN, CI.KPP, 
        CL.NAME_RU, S.LOGIN, VS.ORDER_DATE, VS.SERVICE_NAME, an.ZIP, an.STATE, an.CITY, an.ADDRESS,
        pii.ZIP, pii.STATE, pii.CITY, pii.ADDRESS;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK204_SPB_IMPORT_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PK204_SPB_IMPORT_T');
    --
    -- Находим строки, которые уже были проимпортированы ранее
    --
    UPDATE PK204_SPB_IMPORT_T P
    SET P.STATUS = 1
    WHERE EXISTS (
        SELECT COUNT(*) FROM (
            SELECT CL.CLIENT_NAME, C.CONTRACT_NO, CS.CUSTOMER, CS.INN, CS.KPP, 
                   A.ACCOUNT_NO, A.CURRENCY_ID, 
                   O.ORDER_NO, O.CREATE_DATE ORDER_DATE, 
                   O.DATE_FROM, O.DATE_TO,
                   AJ.ZIP JUR_ZIP, AJ.STATE JUR_STATE, AJ.CITY JUR_CITY, AJ.ADDRESS JUR_ADDRESS,
                   AD.ZIP DLV_ZIP, AD.STATE DLV_STATE, AD.CITY DLV_CITY, AD.ADDRESS DLV_ADDRESS
              FROM ACCOUNT_T A, ORDER_T O, ACCOUNT_PROFILE_T AP, CONTRACTOR_T CT, 
                   CONTRACT_T C, CLIENT_T CL, CUSTOMER_T CS, SERVICE_T S,
                   ACCOUNT_CONTACT_T AJ, ACCOUNT_CONTACT_T AD  
            WHERE A.ACCOUNT_ID   = O.ACCOUNT_ID
              AND A.ACCOUNT_ID   = AP.ACCOUNT_ID
              AND AP.DATE_FROM  <= SYSDATE
              AND (AP.DATE_TO IS NULL OR AP.DATE_TO < SYSDATE)
              AND AP.BRANCH_ID   = CT.CONTRACTOR_ID
              AND AP.CONTRACT_ID = C.CONTRACT_ID
              AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
              AND C.CLIENT_ID    = CL.CLIENT_ID
              AND O.SERVICE_ID   = S.SERVICE_ID
              AND S.SERVICE_ID   = 7
              AND A.ACCOUNT_TYPE = 'J'
              AND CT.SHORT_NAME  = 'СПб филиал ТТК'
              AND CT.CONTRACTOR_TYPE = 'BRAND'
              AND AJ.ACCOUNT_ID(+)  = A.ACCOUNT_ID
              AND AJ.ADDRESS_TYPE(+) = 'JUR'
              AND AJ.DATE_TO(+) IS NULL
              AND AD.ACCOUNT_ID(+)  = A.ACCOUNT_ID
              AND AD.ADDRESS_TYPE(+) = 'DLV'
              AND AD.DATE_TO(+) IS NULL
        ) L
    WHERE  P.CLIENT_NAME = L.CLIENT_NAME AND
            P.CLIENT_NAME = L.CLIENT_NAME AND 
            P.CONTRACT_NO = L.CONTRACT_NO AND 
            P.CUSTOMER    = L.CUSTOMER    AND 
            P.INN         = L.INN         AND 
            P.KPP         = L.KPP         AND 
            P.ACCOUNT_NO  = L.ACCOUNT_NO  AND 
            P.CURRENCY_ID = L.CURRENCY_ID AND 
            P.ORDER_NO    = L.ORDER_NO    AND 
            P.ORDER_DATE  = L.ORDER_DATE  AND 
            P.DATE_FROM   = L.DATE_FROM   AND    
            (P.DATE_TO = L.DATE_TO OR (P.DATE_TO IS NULL AND L.DATE_TO IS NULL))    --AND
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK204_SPB_IMPORT_T: '||v_count||' rows exists', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Загрузка данных в биллинг
--
PROCEDURE Load_customers
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Load_customers';
    v_count          INTEGER := 0;
    v_client_id      INTEGER;
    v_customer_id    INTEGER;
    v_contract_id    INTEGER;
    v_account_id     INTEGER;
    v_address_id     INTEGER; 
    v_profile_id     INTEGER;
    v_bill_id        INTEGER;
    v_order_id       INTEGER;
    v_order_to       DATE;
BEGIN
    --
    FOR r_cust IN (
        SELECT BRAND, CLIENT_NAME, CONTRACT_NO, CUSTOMER, INN, KPP, 
               ACCOUNT_NO, CURRENCY_ID, ORDER_NO, ORDER_DATE, SERVICE_NAME, 
               DATE_FROM, NVL(DATE_TO, Pk00_Const.c_DATE_MAX) DATE_TO, 
               JUR_ZIP, JUR_REGION, JUR_CITY, JUR_ADDRESS, 
               DLV_ZIP, DLV_REGION, DLV_CITY, DLV_ADDRESS, 
               SAVE_DATE, STATUS 
        FROM PK204_SPB_IMPORT_T
        WHERE STATUS IS NULL
        ORDER BY CLIENT_NAME, CUSTOMER, CONTRACT_NO, ACCOUNT_NO, ORDER_DATE, ORDER_NO
      )
    LOOP  
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- находим клиента клиента
        BEGIN
            SELECT CL.CLIENT_ID
              INTO v_client_id
              FROM CLIENT_T CL
             WHERE CL.CLIENT_NAME = r_cust.client_name;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            -- Создать клиента
            v_client_id := Pk11_Client.New_client(
                      p_name => r_cust.client_name
                   );    
            Pk01_Syslog.Write_msg('new client_id='||v_client_id, c_PkgName||'.'||v_prcName);
        END;
        
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- находим компанию-заказчика
        BEGIN
            SELECT CS.CUSTOMER_ID
              INTO v_customer_id
              FROM CUSTOMER_T CS
             WHERE CS.CUSTOMER = r_cust.customer
               AND CS.INN      = r_cust.inn
               AND CS.KPP      = r_cust.kpp;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            -- Создать покупателя
            v_customer_id := Pk13_Customer.New_customer(
                      p_erp_code    => NULL,
                      p_inn         => r_cust.inn,
                      p_kpp         => r_cust.kpp, 
                      p_name        => r_cust.customer,
                      p_short_name  => r_cust.customer
                   );
            Pk01_Syslog.Write_msg('new customer_id='||v_customer_id, c_PkgName||'.'||v_prcName);
        END;
        
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- находим договор
        BEGIN
            SELECT C.CONTRACT_ID
              INTO v_contract_id
              FROM CONTRACT_T C
             WHERE C.CONTRACT_NO = r_cust.contract_no;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            NULL;
            -- Создать договор
            v_contract_id := PK12_CONTRACT.Open_contract(
                      p_contract_no=> r_cust.contract_no,
                      p_date_from  => r_cust.order_date,
                      p_date_to    => NULL,
                      p_client_id  => v_client_id,
                      p_manager_id => NULL
                   );
            Pk01_Syslog.Write_msg('new contract_id='||v_contract_id, c_PkgName||'.'||v_prcName);
        END;
        
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- находим лицевой счет
        BEGIN
            SELECT A.ACCOUNT_ID
              INTO v_account_id
              FROM ACCOUNT_T A
             WHERE A.ACCOUNT_NO = r_cust.account_no;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            NULL;
            -- Создать лицевой счет покупателя
            v_account_id := PK05_ACCOUNT.New_account(
                      p_account_no   => r_cust.account_no,
                      p_account_type => PK00_CONST.c_ACC_TYPE_J,
                      p_currency_id  => PK00_CONST.c_CURRENCY_RUB,
                      p_status       => PK00_CONST.c_ACC_STATUS_BILL,
                      p_parent_id    => NULL
                   );
            Pk01_Syslog.Write_msg('new account_id='||v_account_id, c_PkgName||'.'||v_prcName);
            --
            -- Добавить Юридический адрес на л/с
            v_address_id := PK05_ACCOUNT.Add_address(
                      p_account_id    => v_account_id,
                      p_address_type  => PK00_CONST.c_ADDR_TYPE_JUR,
                      p_country       => 'РФ',
                      p_zip           => r_cust.jur_zip,
                      p_state         => r_cust.jur_region,
                      p_city          => r_cust.jur_city,
                      p_address       => r_cust.jur_address,
                      p_person        => NULL,
                      p_phones        => NULL,
                      p_fax           => NULL,
                      p_email         => NULL,
                      p_date_from     => r_cust.order_date,
                      p_date_to       => NULL,
                      p_notes         => NULL
                   );
            Pk01_Syslog.Write_msg('new jur_address_id='||v_address_id, c_PkgName||'.'||v_prcName);
            --
            -- Добавить адрес доставки на л/с
            v_address_id := PK05_ACCOUNT.Add_address(
                      p_account_id    => v_account_id,
                      p_address_type  => PK00_CONST.c_ADDR_TYPE_DLV,
                      p_country       => 'РФ',
                      p_zip           => r_cust.dlv_zip,
                      p_state         => r_cust.dlv_region,
                      p_city          => r_cust.dlv_city,
                      p_address       => r_cust.dlv_address,
                      p_person        => NULL,
                      p_phones        => NULL,
                      p_fax           => NULL,
                      p_email         => NULL,
                      p_date_from     => r_cust.order_date,
                      p_date_to       => NULL,
                      p_notes         => NULL
                   );
            Pk01_Syslog.Write_msg('new dlv_address_id='||v_address_id, c_PkgName||'.'||v_prcName);
            --
            -- создать описатель счетов и самих счетов для нового Л/С
            v_bill_id := Pk07_Bill.New_billinfo (
                           p_account_id    => v_account_id,   -- ID лицевого счета
                           p_currency_id   => Pk00_Const.c_CURRENCY_RUB  -- ID валюты счета
                       );

            Pk01_Syslog.Write_msg('bill_id='||v_bill_id, c_PkgName||'.'||v_prcName);
            --
        END;
        
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- находим профиль лицевого счета
        BEGIN
            SELECT AP.PROFILE_ID
              INTO v_profile_id
              FROM ACCOUNT_PROFILE_T AP
             WHERE AP.ACCOUNT_ID  = v_account_id
               AND AP.CONTRACT_ID = v_contract_id
               AND AP.CUSTOMER_ID = v_customer_id
               AND AP.DATE_TO IS NULL;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            -- закрыть текущий профильл/с
            UPDATE ACCOUNT_PROFILE_T AP
               SET AP.DATE_TO = r_cust.order_date-1/86400
             WHERE AP.ACCOUNT_ID  = v_account_id
               AND AP.DATE_TO IS NULL;
            -- создать профиль лицевого счета
            v_profile_id := PK05_ACCOUNT.Set_profile(
                       p_account_id    => v_account_id,
                       p_contract_id   => v_contract_id,
                       p_customer_id   => v_customer_id,
                       p_subscriber_id => NULL,
                       p_contractor_id => c_CONTRACTOR_KTTK_ID,
                       p_branch_id     => c_CONTRACTOR_SPB_XTTK_ID,
                       p_agent_id      => NULL,
                       p_contractor_bank_id => NULL,
                       p_vat           => Pk00_Const.c_VAT,
                       p_date_from     => r_cust.order_date,
                       p_date_to       => NULL
                   );
            Pk01_Syslog.Write_msg('v_profile_id='||v_profile_id, c_PkgName||'.'||v_prcName);
        END;

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- находим заказ на лицевом счете
        BEGIN
            SELECT O.ORDER_ID, O.DATE_TO
              INTO v_order_id, v_order_to
              FROM ORDER_T O
             WHERE O.ORDER_NO = r_cust.order_no;
            --
            -- закрываем заказ если нужно 
            IF r_cust.date_to < v_order_to THEN
                PK06_ORDER.Close_order(
                           p_order_id => v_order_id, 
                           p_date_to  => r_cust.date_to
                       );
            END IF;  
            --
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            NULL;
            -- создаем заказ
            v_order_id := PK06_ORDER.New_order(
                           p_account_id    => v_account_id,
                           p_order_no      => r_cust.order_no,
                           p_service_id    => PK00_CONST.c_SERVICE_OP_LOCAL,
                           p_rateplan_id   => NULL,
                           p_time_zone     => NULL,
                           p_date_from     => r_cust.date_from,
                           p_date_to       => r_cust.date_to,
                           p_create_date   => SYSDATE
                       );
        END;
        --
        
        --
        v_count := v_count + 1;
        --
    END LOOP;

    Pk01_Syslog.Write_msg('processed = '||v_count||' rows', c_PkgName||'.'||v_prcName);

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
    v_prcName        CONSTANT VARCHAR2(30) := 'Remove_client';
    --
    v_acc_type       ACCOUNT_T.ACCOUNT_TYPE%TYPE;
    v_order_id       INTEGER;
    v_rateplan_id    INTEGER;
    v_customer_id    INTEGER;
    v_contract_id    INTEGER;
    v_client_id      INTEGER;
    v_profile_id     INTEGER;
    v_count          INTEGER;
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

    -- удаляем обороты за месяц
    DELETE FROM REP_PERIOD_INFO_T RP
    WHERE RP.ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('REP_PERIOD_INFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем INVOICE_ITEM
    DELETE FROM INVOICE_ITEM_T II
    WHERE II.BILL_ID IN (
        SELECT B.BILL_ID FROM BILL_T B
        WHERE B.ACCOUNT_ID = p_account_id
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- удаляем ITEM
    DELETE FROM ITEM_T I
    WHERE EXISTS (
        SELECT * FROM BILL_T B
         WHERE B.ACCOUNT_ID = p_account_id
           AND I.BILL_ID = B.BILL_ID
           AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- удаляем BILLINFO_T
    DELETE FROM BILLINFO_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLINFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- удаляем BILL_T
    DELETE FROM BILL_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- удаляем данные заказа (заказ создавали один)
    SELECT ORDER_ID, RATEPLAN_ID INTO v_order_id, v_rateplan_id
    FROM ORDER_T
    WHERE ACCOUNT_ID = p_account_id;
    
    -- удаляем привязку менеджера к клиенту
    DELETE FROM SALE_CURATOR_T
    WHERE ACCOUNT_ID = p_account_id OR ORDER_ID = v_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SALE_CURATOR_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
   
    -- удаляем строки заказа
    DELETE FROM ORDER_BODY_T
    WHERE ORDER_ID = v_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- удаляем заказ
    DELETE FROM ORDER_T 
    WHERE ORDER_ID = v_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- удалем тарифный план (при новом импорте воссоздадим)
    --DELETE FROM RATEPLAN_T
    --WHERE RATEPLAN_ID = v_rateplan_id;
    -- удаляем адреса
    DELETE FROM ACCOUNT_CONTACT_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_CONTACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
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
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_PROFILE_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- удаляем лицевой счет
    DELETE FROM ACCOUNT_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- удаляем договор
--    DELETE FROM CONTRACT_T
--    WHERE CONTRACT_ID = v_contract_id;
    -- удаляем клиента
--    DELETE FROM CLIENT_T
--    WHERE CLIENT_ID = v_client_id;
    -- удаляем юр.адрес покупателя
    DELETE FROM CUSTOMER_ADDRESS_T
    WHERE CUSTOMER_ID = v_customer_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CUSTOMER_ADDRESS_T.JUR: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- удаляем покупателя
    DELETE FROM CUSTOMER_T
    WHERE CUSTOMER_ID = v_customer_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CUSTOMER_ADDRESS_T.DLV: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('The end.', c_PkgName||'.'||v_prcName);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

END PK204_SPB_IMPORT;
/
