CREATE OR REPLACE PACKAGE PK210_SPBF_IMPORT
IS
    --
    -- Пакет для создания клиентов бренда xTTK «Санкт-Петербургский ТЕЛЕПОРТ»
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK210_SPBF_IMPORT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;


    -- биллинг в который загружаем клиентов, для проверки перед импортом
    c_BILLING_SPB_AF      CONSTANT INTEGER := 2007; -- Биллинг клиетнов ФЛ СПб блока "Доступ"
    c_REGION_ID           CONSTANT INTEGER := 7801;   -- код региона
    c_IN_PERIOD_ID        CONSTANT INTEGER := 201505; -- ID - периода для входящих счетов/платежей 
    c_IN_PERIOD_FROM      CONSTANT DATE    := TO_DATE('01.05.2015','dd.mm.yyyy'); -- ID - периода для входящих счетов/платежей
    c_IN_PERIOD_TO        CONSTANT DATE    := TO_DATE('31.05.2015 23:59:59','dd.mm.yyyy hh24:mi:ss'); -- ID - периода для входящих счетов/платежей
    c_GMT_SPB             CONSTANT INTEGER := 3;
    
    -- Переменные, которые после массовой загрузки следует уточнить
    c_CLIENT_ID           CONSTANT INTEGER := 1; 
    c_CONTRACTOR_ID       CONSTANT INTEGER := 1;
    c_BRANCH_ID           CONSTANT INTEGER := 8;
    c_AGENT_ID            CONSTANT INTEGER := 295;
    c_BANK_ID             CONSTANT INTEGER := 2;
    c_SALE_CURATOR_ID     CONSTANT INTEGER := 1520454; -- временно Лапин А.Л.
    c_BILLING_CURATOR_ID  CONSTANT INTEGER := 1520454; -- временно Лапин А.Л.
    c_VAT                 CONSTANT NUMBER  := 18;
    
    -- Тариф МГМН:   	SPB_F_01.05.2015			'МР Северо-Запад МГМН ФЛ с 01.05.2015'
    c_RATEPLAN_LD_ID      CONSTANT INTEGER := 84114;   -- 'NPL_RUR'
    c_CURRENCY_ID         CONSTANT INTEGER := 810;     -- RUR
    c_TAX_INCL            CONSTANT CHAR(1) := 'Y';     -- Налог включен в тариф
    
    -- SERVICE_T  -  типы услуг    
    c_SERVICE_CALL_MGMN   CONSTANT INTEGER := 1;   -- Услуги междугородной/международной телефонной связи
    c_SERVICE_CALL_ZONE   CONSTANT INTEGER := 140; -- услуга зонового вызова
    c_SERVICE_CALL_LOCAL  CONSTANT INTEGER := 125; -- услуга местного вызова    
    
    -- SUBSERVICE_T - типы компонентов услуг
    c_SUBSRV_MG     CONSTANT INTEGER := 1;      -- Автоматическое междугородное телефонное соединение    
    c_SUBSRV_MN     CONSTANT INTEGER := 2;      -- Автоматическое международное телефонное соединение
    c_SUBSRV_MIN    CONSTANT INTEGER := 3;      -- Доплата до мин. ежемесячной стоимости
    c_SUBSRV_LOCAL  CONSTANT INTEGER := 5;      -- Автоматическое местное телефонное соединение
    c_SUBSRV_ZONE   CONSTANT INTEGER := 6;      -- Автоматическое зоновое телефонное соединение
    c_SUBSRV_REC    CONSTANT INTEGER := 41;     -- Ежемесячный платеж

    -- способ доставки АккордПост
    c_DLV_METHOD_AP CONSTANT INTEGER := 6512;   -- АккордПост 
    
    c_PAYSYSTEM_ID  CONSTANT INTEGER := 16;     -- Унаследованные платежи из БАРСУМ

    -- телефонные сети
    c_MMTS_NETWORK_ID CONSTANT INTEGER := 1;
    c_SPb_NETWORK_ID  CONSTANT INTEGER := 3;
    
    c_MAX_DATE_TO   CONSTANT DATE := TO_DATE('01.01.2050','dd.mm.yyyy');
    
    c_TASK_ID       CONSTANT INTEGER := 1;

    -- описание строки импортированной информации    
    CURSOR c_FILE_CSV IS (
       SELECT 
          CONTRACT_NO,
          SUBSTR(SUBSCRIBER,1,INSTR(SUBSCRIBER,' ',1,1)-1) LAST_NAME,
          SUBSTR(SUBSCRIBER,INSTR(SUBSCRIBER,' ',1,1)+1,INSTR(SUBSCRIBER,' ',1,2)-INSTR(SUBSCRIBER,' ',1,1)-1) FIRST_NAME,
          SUBSTR(SUBSCRIBER,INSTR(SUBSCRIBER,' ',1,2)+1) MIDDLE_NAME,
          STATUS,
          TO_DATE(CONTRACT_DATE,'dd.mm.yyyy') DATE_FROM,
          TO_NUMBER(REPLACE(TOTAL,',','.')) TOTAL,
          '7812'||PHONE PHONE,
          RATEPLAN,
          ZIP,
          CITY,
          ADDRESS,
          ACCOUNT_ID,
          PROFILE_ID,
          CONTRACT_ID,
          SUBSCRIBER_ID,
          RATEPLAN_PCK_ID,
          LD_ORDER_ID,
          Z_ORDER_ID,
          L_ORDER_ID,
          ADDRESS_ID,
          IN_BILL_ID,
          IN_PAYMENT_ID
         FROM PK210_SPBF_IMPORT_T 
        WHERE PHONE != '-'
    )FOR UPDATE;
    
    -- =====================================================================
    -- СТАРЫЙ ТЕКСТ
    -- =====================================================================
    
    -- описатель тарификатора
    с_RATESYS_ABP_ID CONSTANT INTEGER := 1207; -- стандартный расчет абонплаты, тариф в ORDER_BODY_T
    -- описатели тарифных планов
    c_RATEPLAN_NPL_RUR        CONSTANT INTEGER := 80043; -- 'NPL_RUR'
    c_RATEPLAN_NPL            CONSTANT INTEGER := 80045; -- 'NPL'
    c_RATEPLAN_RRW_RUR        CONSTANT INTEGER := 80046; -- 'RRW RUR'
    c_RATEPLAN_IP_ROUTING_RUR CONSTANT INTEGER := 80047; -- 'IP Routing RUR'
    
    c_LDSTAT_DBL_ORD CONSTANT INTEGER := -1;   -- заказ уже есть в BRM
    c_LDSTAT_NOT_SRV CONSTANT INTEGER := -2;   -- услуга не найдена

    --============================================================================================
    -- После полного тестирования переводим л/с из c_BILLING_NPL -> Pk00_Const.c_BILLING_OLD
    --============================================================================================
    PROCEDURE Change_billing_id;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Проверка данных, проставление существующих ID договоров, л/с, пакетов ТП
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Check_data;

    -- Загрузка информации о лицевых счетах клиентов
    PROCEDURE Load_accounts;
    
    -- загрузка платежей из Барсум
    PROCEDURE Load_payments;
    
    --============================================================================================
    -- Догрузка информации о лицевых счетах клиентов
    --============================================================================================
    PROCEDURE Load_abp_min;
    
    --============================================================================================
    -- Биллинг - тестовый
    --============================================================================================
    PROCEDURE Billing;
    
    
END PK210_SPBF_IMPORT;
/
CREATE OR REPLACE PACKAGE BODY PK210_SPBF_IMPORT
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
-- Проверка данных, проставление существующих ID договоров, л/с, пакетов ТП
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_data
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Check_data';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    -- Проставляем ID существующих лицевых счетов (внешний ID совпадает с номером договора)
    UPDATE PK210_SPBF_IMPORT_T I
      SET ACCOUNT_ID = (
                   SELECT A.ACCOUNT_ID 
                     FROM ACCOUNT_T A
                    WHERE A.EXTERNAL_ID = I.CONTRACT_NO
                      AND A.BILLING_ID  = c_BILLING_SPB_AF
               ),
           -- Проставляем ID существующих пакетов тарифных планов
           RATEPLAN_PCK_ID = (
               SELECT R.RATEPLAN_ID 
                 FROM RATEPLAN_T R
                WHERE R.RATEPLAN_NAME = I.RATEPLAN
                  AND R.RATEPLAN_ID IN ( -- должны быть пакеты
                      SELECT RATEPLAN_ID
                        FROM TARIFF_PH.D41_TRF_HEADER H
                       WHERE H.IS_PCK = 'Y'
                  )
           )
    ;
    Pk01_Syslog.Write_msg('PK210_SPBF_IMPORT_T.ACCOUNT_ID '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- Проставляем остальные идентификаторы, зависимые от л/с:
    UPDATE PK210_SPBF_IMPORT_T I
       SET
           -- Проставляем ID существующих договоров
           CONTRACT_ID = (
               SELECT C.CONTRACT_ID 
                 FROM CONTRACT_T C
                WHERE C.CONTRACT_NO = I.CONTRACT_NO
           ),
           -- Проставляем клиента-плательщика               
           SUBSCRIBER_ID = (
               SELECT MIN(S.SUBSCRIBER_ID)
                 FROM SUBSCRIBER_T S 
                WHERE S.LAST_NAME   = SUBSTR(I.SUBSCRIBER,1,INSTR(I.SUBSCRIBER,' ',1,1)-1)
                  AND S.FIRST_NAME  = SUBSTR(I.SUBSCRIBER,INSTR(I.SUBSCRIBER,' ',1,1)+1,INSTR(I.SUBSCRIBER,' ',1,2)-INSTR(I.SUBSCRIBER,' ',1,1)-1)
                  AND S.MIDDLE_NAME = SUBSTR(I.SUBSCRIBER,INSTR(I.SUBSCRIBER,' ',1,2)+1)
           ),
           -- Проставляем ID профиля л/с
           PROFILE_ID = (
               SELECT AP.PROFILE_ID 
                 FROM ACCOUNT_PROFILE_T AP
                WHERE AP.ACCOUNT_ID = I.ACCOUNT_ID
           ),
           -- Проставляем ID адреса доставки
           ADDRESS_ID = (
               SELECT AC.CONTACT_ID
                 FROM ACCOUNT_CONTACT_T AC
                WHERE AC.ACCOUNT_ID   = I.ACCOUNT_ID
                  AND AC.ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_DLV
           ),
           -- Проставляем ID заказа на МГ/МН связь
           LD_ORDER_ID = (
               SELECT O.ORDER_ID
                 FROM ORDER_T O
                WHERE O.ACCOUNT_ID = I.ACCOUNT_ID
                  AND O.SERVICE_ID = Pk00_Const.c_SERVICE_CALL_MGMN
           ),
           -- Проставляем ID заказа на зоновую связь
           Z_ORDER_ID = (
               SELECT O.ORDER_ID
                 FROM ORDER_T O
                WHERE O.ACCOUNT_ID = I.ACCOUNT_ID
                  AND O.SERVICE_ID = Pk00_Const.c_SERVICE_CALL_ZONE
           ),
           -- Проставляем ID заказа на местную связь
           L_ORDER_ID = (
               SELECT O.ORDER_ID
                 FROM ORDER_T O
                WHERE O.ACCOUNT_ID = I.ACCOUNT_ID
                  AND O.SERVICE_ID = Pk00_Const.c_SERVICE_CALL_LOCAL
           ),
           /*
           -- Проставляем ID существующих пакетов тарифных планов
           RATEPLAN_PCK_ID = (
               SELECT R.RATEPLAN_ID 
                 FROM RATEPLAN_T R
                WHERE R.RATEPLAN_NAME = I.RATEPLAN
                  AND R.RATEPLAN_ID IN ( -- должны быть пакеты
                      SELECT RATEPLAN_ID
                        FROM TARIFF_PH.D41_TRF_HEADER H
                       WHERE H.IS_PCK = 'Y'
                  )
           ),
           */
           -- Проставляем ID фиктивного сечта с входящими долгами
          IN_BILL_ID = (
               SELECT B.BILL_ID
                 FROM BILL_T B
                WHERE B.ACCOUNT_ID    = I.ACCOUNT_ID
                  AND B.REP_PERIOD_ID = c_IN_PERIOD_ID -- входящий период май 2015
                  AND B.TOTAL         = I.TOTAL
           ),
           -- Проставляем ID фиктивного платежа с входящим остатком денег
          IN_PAYMENT_ID = (
               SELECT P.PAYMENT_ID
                 FROM PAYMENT_T P
                WHERE P.ACCOUNT_ID    = I.ACCOUNT_ID
                  AND P.REP_PERIOD_ID = c_IN_PERIOD_ID
                  AND P.PAYSYSTEM_ID  = Pk00_Const.c_PAYSYSTEM_CORRECT_ID -- 12;
           )
    WHERE I.ACCOUNT_ID IS NOT NULL;
    Pk01_Syslog.Write_msg('PK210_SPBF_IMPORT_T '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    /*
    -- проставляем остальные услуги
    FOR r_abn IN c_FILE_CSV LOOP
        UPDATE PK210_SPBF_IMPORT_T I
               -- Проставляем ID существующих договоров
           SET CONTRACT_ID = (
                   SELECT C.CONTRACT_ID 
                     FROM CONTRACT_T C
                    WHERE C.CONTRACT_NO = I.CONTRACT_NO
               ),
               -- Проставляем клиента-плательщика               
               SUBSCRIBER_ID = (
                   SELECT S.SUBSCRIBER_ID 
                     FROM SUBSCRIBER_T S 
                    WHERE S.LAST_NAME   = r_abn.LAST_NAME
                      AND S.FIRST_NAME  = r_abn.FIRST_NAME
                      AND S.MIDDLE_NAME = r_abn.MIDDLE_NAME
               ),
               -- проставляем заранее
               -- Проставляем ID существующих лицевых счетов (внешний ID совпадает с номером договора)
               --ACCOUNT_ID = (
               --  SELECT A.ACCOUNT_ID 
               --    FROM ACCOUNT_T A
               --   WHERE A.EXTERNAL_ID = I.CONTRACT_NO
               --     AND A.BILLING_ID  = c_BILLING_SPB_AF
               --),
               -- Проставляем ID профиля л/с
               PROFILE_ID = (
                   SELECT AP.PROFILE_ID 
                     FROM ACCOUNT_PROFILE_T AP
                    WHERE AP.ACCOUNT_ID = I.ACCOUNT_ID
               ),
               -- Проставляем ID адреса доставки
               ADDRESS_ID = (
                   SELECT AC.CONTACT_ID
                     FROM ACCOUNT_CONTACT_T AC
                    WHERE AC.ACCOUNT_ID   = I.ACCOUNT_ID
                      AND AC.ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_DLV
               ),
               -- Проставляем ID заказа на МГ/МН связь
               LD_ORDER_ID = (
                   SELECT O.ORDER_ID
                     FROM ORDER_T O
                    WHERE O.ACCOUNT_ID = I.ACCOUNT_ID
                      AND O.SERVICE_ID = Pk00_Const.c_SERVICE_CALL_MGMN
               ),
               -- Проставляем ID заказа на зоновую связь
               Z_ORDER_ID = (
                   SELECT O.ORDER_ID
                     FROM ORDER_T O
                    WHERE O.ACCOUNT_ID = I.ACCOUNT_ID
                      AND O.SERVICE_ID = Pk00_Const.c_SERVICE_CALL_ZONE
               ),
               -- Проставляем ID заказа на местную связь
               L_ORDER_ID = (
                   SELECT O.ORDER_ID
                     FROM ORDER_T O
                    WHERE O.ACCOUNT_ID = I.ACCOUNT_ID
                      AND O.SERVICE_ID = Pk00_Const.c_SERVICE_CALL_LOCAL
               ),
               -- Проставляем ID существующих пакетов тарифных планов
               RATEPLAN_PCK_ID = (
                   SELECT R.RATEPLAN_ID 
                     FROM RATEPLAN_T R
                    WHERE R.RATEPLAN_NAME = I.RATEPLAN
                      AND R.RATEPLAN_ID IN ( -- должны быть пакеты
                          SELECT RATEPLAN_ID
                            FROM TARIFF_PH.D41_TRF_HEADER H
                           WHERE H.IS_PCK = 'Y'
                      )
               ),
               -- Проставляем ID фиктивного сечта с входящими долгами
              IN_BILL_ID = (
                   SELECT B.BILL_ID
                     FROM BILL_T B
                    WHERE B.ACCOUNT_ID    = I.ACCOUNT_ID
                      AND B.REP_PERIOD_ID = c_IN_PERIOD_ID -- входящий период май 2015
                      AND B.TOTAL         = I.TOTAL
               ),
               -- Проставляем ID фиктивного платежа с входящим остатком денег
              IN_PAYMENT_ID = (
                   SELECT P.PAYMENT_ID
                     FROM PAYMENT_T P
                    WHERE P.ACCOUNT_ID    = I.ACCOUNT_ID
                      AND P.REP_PERIOD_ID = c_IN_PERIOD_ID
                      AND P.PAYSYSTEM_ID  = Pk00_Const.c_PAYSYSTEM_CORRECT_ID -- 12;
               )
        WHERE CURRENT OF c_FILE_CSV;
        v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg( v_count||' rows processed', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    */
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Загрузка информации о лицевых счетах клиентов
--============================================================================================
PROCEDURE Load_accounts
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Load_accounts';
    v_count           INTEGER := 0;
    v_account_id      INTEGER;
    v_address_id      INTEGER;
    v_profile_id      INTEGER;
    v_contract_id     INTEGER;
    v_subscriber_id   INTEGER;
    v_ld_order_id     INTEGER;
    v_z_order_id      INTEGER;
    v_l_order_id      INTEGER;
    v_z_rateplan_id   INTEGER;
    v_l_rateplan_id   INTEGER;
    v_in_bill_id      INTEGER;
    v_in_payment_id   INTEGER;
    v_mg_ob_id        INTEGER;
    v_mn_ob_id        INTEGER;
    v_zn_ob_id        INTEGER;
    v_lc_ob_id        INTEGER;
    v_rec_ob_id       INTEGER;
    v_min_ob_id       INTEGER;
    v_item_id         INTEGER;
    v_subservice_id   INTEGER;
    v_account_no      ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_price           NUMBER;
    v_free_value      NUMBER;
    v_free_value_unit RATEPLAN_FIXRATE_T.FREE_VALUE_UNIT%TYPE;
    v_charge_type     SUBSERVICE_T.CHARGE_TYPE%TYPE;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    FOR r_abn IN c_FILE_CSV LOOP
    	
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- создаем л/с
        IF r_abn.ACCOUNT_ID IS NULL THEN
          
            -- номер л/с создаем в стандартном формате
            v_account_no := 'XP'||LPAD(SQ_ACCOUNT_NO.NEXTVAL,6,'0');
        
            -- создаем л/с
            v_account_id := Pk05_Account.New_account(
                         p_account_no    => v_account_no,
                         p_account_type  => Pk00_Const.c_ACC_TYPE_P,
                         p_currency_id   => c_CURRENCY_ID,
                         p_status        => 'NEW', --Pk00_Const.c_ACC_STATUS_BILL,
                         p_parent_id     => NULL,
                         p_notes         => 'импортировано из БАРСУМ '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
                     );
            -- сохраняем ID л/с
            UPDATE PK210_SPBF_IMPORT_T
               SET ACCOUNT_ID = v_account_id
             WHERE CURRENT OF c_FILE_CSV;
             
            -- проставляем внешний идентификатор л/с = № договора
            UPDATE ACCOUNT_T
               SET EXTERNAL_ID = r_abn.CONTRACT_NO
             WHERE ACCOUNT_ID  = v_account_id;

            -- проставляем принадлежность биллингу БАРСУМ
            Pk05_Account.Set_billing(
                         p_account_id => v_account_id,
                         p_billing_id => c_BILLING_SPB_AF
                     );
             
            -- создаем описатель счетов и способ доставки счета
            Pk07_Bill.New_billinfo (
                       p_account_id    => v_account_id,   -- ID лицевого счета
                       p_currency_id   => c_CURRENCY_ID,  -- ID валюты счета
                       p_delivery_id   => c_DLV_METHOD_AP,-- ID способа доставки счета
                       p_days_for_payment => 30           -- кол-во дней на оплату счета
                   );
        ELSE
            v_account_id := r_abn.ACCOUNT_ID;
        END IF;

        -- Создаем договор
        IF r_abn.CONTRACT_ID IS NULL THEN
            v_contract_id := Pk12_Contract.Open_contract(
               p_contract_no => r_abn.CONTRACT_NO, 
               p_date_from   => r_abn.DATE_FROM,
               p_date_to     => c_MAX_DATE_TO,
               p_client_id   => c_CLIENT_ID,
               p_manager_id  => c_SALE_CURATOR_ID
            );
            -- сохраняем ID
            UPDATE PK210_SPBF_IMPORT_T
               SET CONTRACT_ID = v_contract_id
             WHERE CURRENT OF c_FILE_CSV;
        ELSE
            v_contract_id := r_abn.CONTRACT_ID;
        END IF;

        -- Создаем клиента-плательщика
        IF r_abn.SUBSCRIBER_ID IS NULL THEN
            v_subscriber_id := Pk21_Subscriber.New_subscriber(
               p_last_name   => r_abn.LAST_NAME,   -- фамилия
               p_first_name  => r_abn.FIRST_NAME,  -- имя 
               p_middle_name => r_abn.MIDDLE_NAME, -- отчество
               p_category    => Pk00_Const.c_SUBS_RESIDENT  -- категория 1/2 = резидент/нерезидент
            );
            -- сохраняем ID
            UPDATE PK210_SPBF_IMPORT_T
               SET SUBSCRIBER_ID = v_subscriber_id
             WHERE CURRENT OF c_FILE_CSV;
        ELSE 
            v_subscriber_id := r_abn.SUBSCRIBER_ID;
        END IF;
        
        -- Создаем профиль лицевого счета
        IF r_abn.PROFILE_ID IS NULL THEN
            v_profile_id := Pk05_Account.Set_profile(
                 p_account_id         => v_account_id,
                 p_brand_id           => NULL,
                 p_contract_id        => v_contract_id,
                 p_customer_id        => NULL,
                 p_subscriber_id      => v_subscriber_id,
                 p_contractor_id      => c_CONTRACTOR_ID,
                 p_branch_id          => c_BRANCH_ID,
                 p_agent_id           => c_AGENT_ID,
                 p_contractor_bank_id => c_BANK_ID,
                 p_vat                => Pk00_Const.c_VAT,
                 p_date_from          => r_abn.DATE_FROM,
                 p_date_to            => NULL
             );
            -- сохраняем ID
            UPDATE PK210_SPBF_IMPORT_T
               SET PROFILE_ID = v_profile_id
             WHERE CURRENT OF c_FILE_CSV;
        ELSE
            v_profile_id := r_abn.PROFILE_ID;
        END IF;
        
        -- Создаем адрес доставки
        IF r_abn.ADDRESS_ID IS NULL THEN
            v_address_id := PK05_ACCOUNT.Add_address(
                        p_account_id    => v_account_id,
                        p_address_type  => PK00_CONST.c_ADDR_TYPE_DLV,
                        p_country       => 'РФ',
                        p_zip           => r_abn.ZIP,
                        p_state         => NULL,
                        p_city          => r_abn.CITY,
                        p_address       => r_abn.ADDRESS,
                        p_person        => r_abn.LAST_NAME||' '||r_abn.FIRST_NAME||' '||r_abn.MIDDLE_NAME,
                        p_phones        => r_abn.PHONE,
                        p_fax           => NULL,
                        p_email         => NULL,
                        p_date_from     => r_abn.DATE_FROM,
                        p_date_to       => NULL,
                        p_notes         => 'импортировано из БАРСУМ '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')
                   );
            -- сохраняем ID
            UPDATE PK210_SPBF_IMPORT_T
               SET ADDRESS_ID = v_address_id
             WHERE CURRENT OF c_FILE_CSV;  
        ELSE
            v_address_id := r_abn.ADDRESS_ID;
        END IF;             
        
        -- Создаем заказ на МГ/МН связь
        IF r_abn.LD_ORDER_ID IS NULL THEN
            v_ld_order_id := Pk06_Order.New_order(
               p_account_id   => v_account_id,       -- ID лицевого счета
               p_order_no     => r_abn.CONTRACT_NO||'-LD', -- Номер заказа, как на бумаге
               p_service_id   => c_SERVICE_CALL_MGMN,-- ID услуги из таблицы SERVICE_T
               p_rateplan_id  => c_RATEPLAN_LD_ID,   -- ID тарифного плана из RATEPLAN_T
               p_time_zone    => c_GMT_SPB,          -- GMT               
               p_date_from    => r_abn.DATE_FROM,    -- дата начала действия заказа
               p_date_to      => Pk00_Const.c_DATE_MAX,
               p_create_date  => SYSDATE,
               p_note         => 'импортировано из БАРСУМ '|| TO_CHAR(SYSDATE,'dd.mm.yyyy') 
            );
            -- сохраняем ID
            UPDATE PK210_SPBF_IMPORT_T
               SET LD_ORDER_ID = v_ld_order_id
             WHERE CURRENT OF c_FILE_CSV;
            
            -- привязываем заказ к телефонной сети
            MERGE INTO ORDER_INFO_T I
            USING (
               SELECT v_ld_order_id ORDER_ID, c_MMTS_NETWORK_ID NETWORK_ID FROM DUAL
            ) D
            ON (
                I.ORDER_ID = D.ORDER_ID
            )
            WHEN MATCHED THEN UPDATE SET I.NETWORK_ID = D.NETWORK_ID
            WHEN NOT MATCHED THEN INSERT (I.ORDER_ID, I.NETWORK_ID) VALUES (D.ORDER_ID, D.NETWORK_ID);
            
            -- привязываем телефон к заказу
            Pk06_Order.Add_phone (
               p_order_id      => v_ld_order_id,
               p_phone         => r_abn.PHONE,
               p_date_from     => r_abn.DATE_FROM,
               p_date_to       => Pk00_Const.c_DATE_MAX
            );
             
            -- создаем компоненты услуги МГ-связь
            v_mg_ob_id := Pk06_Order.Add_subservice (
                 p_order_id      => v_ld_order_id,
                 p_subservice_id => c_SUBSRV_MG,
                 p_charge_type   => Pk00_Const.c_CHARGE_TYPE_USG,
                 p_rateplan_id   => c_RATEPLAN_LD_ID,
                 p_date_from     => r_abn.DATE_FROM,
                 p_date_to       => Pk00_Const.c_DATE_MAX,
                 p_notes         => 'импортировано из БАРСУМ '|| TO_CHAR(SYSDATE,'dd.mm.yyyy'),
                 p_currency_id   => c_CURRENCY_ID
             );
             
            -- создаем компоненты услуги МН-связь
            v_mn_ob_id := Pk06_Order.Add_subservice (
                 p_order_id      => v_ld_order_id,
                 p_subservice_id => c_SUBSRV_MN,
                 p_charge_type   => Pk00_Const.c_CHARGE_TYPE_USG,
                 p_rateplan_id   => c_RATEPLAN_LD_ID,
                 p_date_from     => r_abn.DATE_FROM,
                 p_date_to       => Pk00_Const.c_DATE_MAX,
                 p_notes         => 'импортировано из БАРСУМ '|| TO_CHAR(SYSDATE,'dd.mm.yyyy'),
                 p_currency_id   => c_CURRENCY_ID
             );

        ELSE
            v_ld_order_id := r_abn.LD_ORDER_ID;
        END IF;
        
        -- Проставляем ID заказа на зоновую связь
        IF r_abn.Z_ORDER_ID IS NULL THEN
            BEGIN
                -- проверяем - заказана ли услуга, если нет выход через NO_DATA_FOUND
                SELECT R.RATEPLAN_ID
                  INTO v_z_rateplan_id
                  FROM RATEPLAN_PCK_T PR, RATEPLAN_T R
                 WHERE PARENT_RATEPLAN_ID = r_abn.RATEPLAN_PCK_ID
                   AND PR.RATEPLAN_ID = R.RATEPLAN_ID
                   AND R.SERVICE_ID   = c_SERVICE_CALL_ZONE
                ;
                -- создаем заказ
                v_z_order_id := Pk06_Order.New_order(
                   p_account_id   => v_account_id,            -- ID лицевого счета
                   p_order_no     => r_abn.CONTRACT_NO||'-Z', -- Номер заказа, как на бумаге
                   p_service_id   => c_SERVICE_CALL_ZONE,     -- ID услуги из таблицы SERVICE_T
                   p_rateplan_id  => r_abn.RATEPLAN_PCK_ID,   -- ID тарифного плана пакета
                   p_time_zone    => c_GMT_SPB,               -- GMT               
                   p_date_from    => r_abn.DATE_FROM,         -- дата начала действия заказа
                   p_date_to      => Pk00_Const.c_DATE_MAX,
                   p_create_date  => SYSDATE,
                   p_note         => 'импортировано из БАРСУМ '|| TO_CHAR(SYSDATE,'dd.mm.yyyy') 
                );
                -- сохраняем ID
                UPDATE PK210_SPBF_IMPORT_T
                   SET Z_ORDER_ID = v_z_order_id
                 WHERE CURRENT OF c_FILE_CSV;
                 
                -- привязываем заказ к телефонной сети
                MERGE INTO ORDER_INFO_T I
                USING (
                   SELECT v_z_order_id ORDER_ID, c_SPb_NETWORK_ID NETWORK_ID FROM DUAL
                ) D
                ON (
                    I.ORDER_ID = D.ORDER_ID
                )
                WHEN MATCHED THEN UPDATE SET I.NETWORK_ID = D.NETWORK_ID
                WHEN NOT MATCHED THEN INSERT (I.ORDER_ID, I.NETWORK_ID) VALUES (D.ORDER_ID, D.NETWORK_ID);

                -- привязываем телефон к заказу
                Pk06_Order.Add_phone (
                   p_order_id      => v_z_order_id,
                   p_phone         => r_abn.PHONE,
                   p_date_from     => r_abn.DATE_FROM,
                   p_date_to       => Pk00_Const.c_DATE_MAX
                );
                 
                -- создаем компоненту услуги зоновую-связь
                v_zn_ob_id := Pk06_Order.Add_subservice (
                     p_order_id      => v_z_order_id,
                     p_subservice_id => c_SUBSRV_ZONE,
                     p_charge_type   => Pk00_Const.c_CHARGE_TYPE_USG,
                     p_rateplan_id   => v_z_rateplan_id,
                     p_date_from     => r_abn.DATE_FROM,
                     p_date_to       => Pk00_Const.c_DATE_MAX,
                     p_notes         => 'импортировано из БАРСУМ '|| TO_CHAR(SYSDATE,'dd.mm.yyyy'),
                     p_currency_id   => c_CURRENCY_ID
                 );

                -- добавляем информацию о пакете тарифных планов
                UPDATE ORDER_BODY_T OB SET OB.RATEPLAN_PCK_ID = r_abn.RATEPLAN_PCK_ID
                 WHERE OB.ORDER_BODY_ID = v_zn_ob_id;
               
                -- добавляем абонки-минималки, если нет выход через NO_DATA_FOUND   
                SELECT RF.SUBSERVICE_ID, RF.PRICE, SS.CHARGE_TYPE,-- SS.SUBSERVICE  
                       RF.FREE_VALUE, RF.FREE_VALUE_UNIT
                  INTO v_subservice_id, v_price, v_charge_type, v_free_value, v_free_value_unit
                  FROM RATEPLAN_T R, RATEPLAN_FIXRATE_T RF, SUBSERVICE_T SS
                 WHERE R.RATEPLAN_ID    = RF.RATEPLAN_ID
                   AND RF.SUBSERVICE_ID = SS.SUBSERVICE_ID
                   AND RF.RATEPLAN_ID   = r_abn.RATEPLAN_PCK_ID
                   AND RF.SERVICE_ID    = c_SERVICE_CALL_ZONE
                ;
                IF v_subservice_id = Pk00_Const.c_SUBSRV_REC THEN
                    IF v_free_value IS NOT NULL THEN
                        -- Добавляем компонент - абонплата с включенным трафиком
                        v_rec_ob_id := Pk06_Order.Add_subs_abon_voice (
                           p_order_id      => v_z_order_id,    -- ID заказа - услуги
                           p_subservice_id => v_subservice_id, -- ID компонента услуги
                           p_value         => v_price,         -- сумма абонплаты
                           p_tax_incl      => c_TAX_INCL,      -- включен ли налог в сумму абонплаты
                           p_currency_id   => c_CURRENCY_ID,   -- валюта
                           p_free_traffic  => v_free_value,    -- кол-во услуги в натуральном измерении
                           p_date_from     => r_abn.DATE_FROM
                        );
                        -- добавляем информацию о пакете тарифных планов
                        UPDATE ORDER_BODY_T OB 
                           SET OB.RATEPLAN_PCK_ID = r_abn.RATEPLAN_PCK_ID
                         WHERE OB.ORDER_BODY_ID = v_rec_ob_id;
                   ELSE
                        -- Добавляем компонент - абонплата
                        v_rec_ob_id := Pk06_Order.Add_subs_abon (
                           p_order_id      => v_z_order_id,    -- ID заказа - услуги
                           p_subservice_id => v_subservice_id, -- ID компонента услуги
                           p_value         => v_price,         -- сумма абонплаты
                           p_tax_incl      => c_TAX_INCL,      -- включен ли налог в сумму абонплаты
                           p_currency_id   => c_CURRENCY_ID,   -- валюта
                           p_quantity      => 1,                -- кол-во услуги в натуральном измерении
                           p_date_from     => r_abn.DATE_FROM,
                           p_date_to       => Pk00_Const.c_DATE_MAX
                        );
                        -- добавляем информацию о пакете тарифных планов
                        UPDATE ORDER_BODY_T OB 
                           SET OB.RATEPLAN_PCK_ID = r_abn.RATEPLAN_PCK_ID
                         WHERE OB.ORDER_BODY_ID = v_rec_ob_id;
                   END IF;
               ELSIF v_subservice_id = Pk00_Const.c_SUBSRV_MIN THEN
                   -- Добавляем компонент - минималка
                   v_min_ob_id := Pk06_Order.Add_subs_min (
                       p_order_id      => v_z_order_id,    -- ID заказа - услуги
                       p_subservice_id => v_subservice_id, -- ID компонента услуги
                       p_value         => v_price,         -- сумма абонплаты
                       p_tax_incl      => c_TAX_INCL,      -- включен ли налог в сумму абонплаты
                       p_currency_id   => c_CURRENCY_ID,   -- валюта
                       p_rate_level_id => Pk00_Const.c_RATE_LEVEL_ORDER, -- уровень действия: компонент/заказ/лицевой счет
                       p_date_from     => r_abn.DATE_FROM
                   );
                   -- добавляем информацию о пакете тарифных планов
                   UPDATE ORDER_BODY_T OB 
                      SET OB.RATEPLAN_PCK_ID = r_abn.RATEPLAN_PCK_ID
                    WHERE OB.ORDER_BODY_ID = v_min_ob_id;
               END IF;
            EXCEPTION WHEN NO_DATA_FOUND THEN
              NULL;
            END;
        ELSE
            v_z_order_id := r_abn.Z_ORDER_ID;
        END IF;
        
        -- Проставляем ID заказа на местную связь
        IF r_abn.L_ORDER_ID IS NULL THEN
            BEGIN
                -- проверяем - заказана ли услуга, если нет выход через NO_DATA_FOUND
                SELECT R.RATEPLAN_ID
                  INTO v_l_rateplan_id
                  FROM RATEPLAN_PCK_T PR, RATEPLAN_T R
                 WHERE PARENT_RATEPLAN_ID = r_abn.RATEPLAN_PCK_ID
                   AND PR.RATEPLAN_ID = R.RATEPLAN_ID
                   AND R.SERVICE_ID   = c_SERVICE_CALL_LOCAL;

                v_l_order_id := Pk06_Order.New_order(
                   p_account_id   => v_account_id,       -- ID лицевого счета
                   p_order_no     => r_abn.CONTRACT_NO||'-L', -- Номер заказа, как на бумаге
                   p_service_id   => c_SERVICE_CALL_LOCAL,-- ID услуги из таблицы SERVICE_T
                   p_rateplan_id  => r_abn.RATEPLAN_PCK_ID,-- ID тарифного плана пакета
                   p_time_zone    => c_GMT_SPB,          -- GMT               
                   p_date_from    => r_abn.DATE_FROM,    -- дата начала действия заказа
                   p_date_to      => Pk00_Const.c_DATE_MAX,
                   p_create_date  => SYSDATE,
                   p_note         => 'импортировано из БАРСУМ '|| TO_CHAR(SYSDATE,'dd.mm.yyyy') 
                );

                -- сохраняем ID
                UPDATE PK210_SPBF_IMPORT_T
                   SET L_ORDER_ID = v_l_order_id
                 WHERE CURRENT OF c_FILE_CSV;
                 
                -- привязываем заказ к телефонной сети
                MERGE INTO ORDER_INFO_T I
                USING (
                   SELECT v_l_order_id ORDER_ID, c_SPb_NETWORK_ID NETWORK_ID FROM DUAL
                ) D
                ON (
                    I.ORDER_ID = D.ORDER_ID
                )
                WHEN MATCHED THEN UPDATE SET I.NETWORK_ID = D.NETWORK_ID
                WHEN NOT MATCHED THEN INSERT (I.ORDER_ID, I.NETWORK_ID) VALUES (D.ORDER_ID, D.NETWORK_ID);

                -- привязываем телефон к заказу
                Pk06_Order.Add_phone (
                   p_order_id      => v_l_order_id,
                   p_phone         => r_abn.PHONE,
                   p_date_from     => r_abn.DATE_FROM,
                   p_date_to       => Pk00_Const.c_DATE_MAX
                );

                -- создаем компоненты услуги местную-связь
                v_lc_ob_id := Pk06_Order.Add_subservice (
                     p_order_id      => v_l_order_id,
                     p_subservice_id => c_SUBSRV_LOCAL,
                     p_charge_type   => Pk00_Const.c_CHARGE_TYPE_USG,
                     p_rateplan_id   => v_l_rateplan_id,
                     p_date_from     => r_abn.DATE_FROM,
                     p_date_to       => Pk00_Const.c_DATE_MAX,
                     p_notes         => 'импортировано из БАРСУМ '|| TO_CHAR(SYSDATE,'dd.mm.yyyy'),
                     p_currency_id   => c_CURRENCY_ID
                 );
                 -- добавляем информацию о пакете тарифных планов
                 UPDATE ORDER_BODY_T OB 
                    SET OB.RATEPLAN_PCK_ID = r_abn.RATEPLAN_PCK_ID
                  WHERE OB.ORDER_BODY_ID = v_lc_ob_id;
                 
                -- добавляем абонки-минималки, если нет выход через NO_DATA_FOUND   
                SELECT RF.SUBSERVICE_ID, RF.PRICE, SS.CHARGE_TYPE,-- SS.SUBSERVICE  
                       RF.FREE_VALUE, RF.FREE_VALUE_UNIT
                  INTO v_subservice_id, v_price, v_charge_type, v_free_value, v_free_value_unit
                  FROM RATEPLAN_T R, RATEPLAN_FIXRATE_T RF, SUBSERVICE_T SS
                 WHERE R.RATEPLAN_ID    = RF.RATEPLAN_ID
                   AND RF.SUBSERVICE_ID = SS.SUBSERVICE_ID
                   AND RF.RATEPLAN_ID   = r_abn.RATEPLAN_PCK_ID
                   AND RF.SERVICE_ID    = c_SERVICE_CALL_LOCAL
                ;
                IF v_subservice_id = Pk00_Const.c_SUBSRV_REC THEN
                   IF v_free_value IS NOT NULL THEN
                       -- Добавляем компонент - абонплата с включенным трафиком
                       v_rec_ob_id := Pk06_Order.Add_subs_abon_voice (
                           p_order_id      => v_l_order_id,    -- ID заказа - услуги
                           p_subservice_id => v_subservice_id, -- ID компонента услуги
                           p_value         => v_price,         -- сумма абонплаты
                           p_tax_incl      => c_TAX_INCL,      -- включен ли налог в сумму абонплаты
                           p_currency_id   => c_CURRENCY_ID,   -- валюта
                           p_free_traffic  => v_free_value,    -- кол-во услуги в натуральном измерении
                           p_date_from     => r_abn.DATE_FROM
                       );
                       -- добавляем информацию о пакете тарифных планов
                       UPDATE ORDER_BODY_T OB 
                          SET OB.RATEPLAN_PCK_ID = r_abn.RATEPLAN_PCK_ID
                        WHERE OB.ORDER_BODY_ID = v_rec_ob_id;
                   ELSE
                       -- Добавляем компонент - абонплата 
                       v_rec_ob_id := Pk06_Order.Add_subs_abon (
                           p_order_id      => v_l_order_id,    -- ID заказа - услуги
                           p_subservice_id => v_subservice_id, -- ID компонента услуги
                           p_value         => v_price,         -- сумма абонплаты
                           p_tax_incl      => c_TAX_INCL,      -- включен ли налог в сумму абонплаты
                           p_currency_id   => c_CURRENCY_ID,   -- валюта
                           p_quantity      => 1,                -- кол-во услуги в натуральном измерении
                           p_date_from     => r_abn.DATE_FROM,
                           p_date_to       => Pk00_Const.c_DATE_MAX
                        );
                       -- добавляем информацию о пакете тарифных планов
                       UPDATE ORDER_BODY_T OB 
                          SET OB.RATEPLAN_PCK_ID = r_abn.RATEPLAN_PCK_ID
                        WHERE OB.ORDER_BODY_ID = v_rec_ob_id;
                   END IF;
               ELSIF v_subservice_id = Pk00_Const.c_SUBSRV_MIN THEN
                   -- Добавляем компонент - минималка
                   v_min_ob_id := Pk06_Order.Add_subs_min (
                       p_order_id      => v_l_order_id,    -- ID заказа - услуги
                       p_subservice_id => v_subservice_id, -- ID компонента услуги
                       p_value         => v_price,         -- сумма абонплаты
                       p_tax_incl      => c_TAX_INCL,      -- включен ли налог в сумму абонплаты
                       p_currency_id   => c_CURRENCY_ID,   -- валюта
                       p_rate_level_id => Pk00_Const.c_RATE_LEVEL_ORDER, -- уровень действия: компонент/заказ/лицевой счет
                       p_date_from     => r_abn.DATE_FROM
                   );
                   -- добавляем информацию о пакете тарифных планов
                   UPDATE ORDER_BODY_T OB 
                      SET OB.RATEPLAN_PCK_ID = r_abn.RATEPLAN_PCK_ID
                    WHERE OB.ORDER_BODY_ID = v_min_ob_id;
               END IF;

            EXCEPTION WHEN NO_DATA_FOUND THEN
              NULL;
            END;
        ELSE
            v_l_order_id := r_abn.L_ORDER_ID;
        END IF;

        -- Cоздаем корректировочный счет для последующего заполнения
        IF r_abn.IN_BILL_ID IS NULL AND r_abn.TOTAL < 0 THEN
            v_in_bill_id := Pk02_POID.Next_bill_id;
            --
            INSERT INTO BILL_T (
                CONTRACT_ID,     -- ID договора
                ACCOUNT_ID,      -- ID лицевого счета
                BILL_ID,         -- ID лицевого счета
                REP_PERIOD_ID,   -- ID расчетного периода
                BILL_TYPE,       -- Тип счета
                BILL_NO,         -- Номер счета
                CURRENCY_ID,     -- ID валюты счета
                BILL_DATE,       -- Дата счета (биллингового периода)
                BILL_STATUS,     -- Состояние счета - ОТКРЫТ
                VAT,             -- ставка НДС 
                PROFILE_ID,      -- ID профиля л/с
                CONTRACTOR_ID,   -- ID продавца
                CONTRACTOR_BANK_ID, -- ID банка продавца
                NOTES
            )VALUES(
                v_contract_id,
                v_account_id,
                v_in_bill_id,
                c_IN_PERIOD_ID,
                PK00_CONST.c_BILL_TYPE_ADS, -- 'A'
                c_REGION_ID||'/'||c_IN_PERIOD_ID||'XP'||r_abn.CONTRACT_NO,
                c_CURRENCY_ID,
                c_IN_PERIOD_TO,
                PK00_CONST.c_BILL_STATE_OPEN,
                c_VAT,
                v_profile_id,
                c_CONTRACTOR_ID,
                c_BANK_ID,
                'импортировано из БАРСУМ '|| TO_CHAR(SYSDATE,'dd.mm.yyyy')||'. Входящая задолженность на 01.06.2015'
            );  
            -- сохраняем ID
            UPDATE PK210_SPBF_IMPORT_T
               SET IN_BILL_ID = v_in_bill_id
             WHERE CURRENT OF c_FILE_CSV;
             
            -- cоздаем ITEM начислений за МГ/МН связь
            v_item_id := PK02_POID.Next_item_id;
            -- создаем запись позиции счета
            INSERT INTO ITEM_T (
               BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, 
               ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, CHARGE_TYPE,
               ITEM_TOTAL, RECVD, 
               DATE_FROM, DATE_TO, LAST_MODIFIED,
               INV_ITEM_ID, ITEM_STATUS, TAX_INCL, NOTES
            )
            SELECT BILL_ID, REP_PERIOD_ID, 
                   ITEM_ID, ITEM_TYPE, 
                   ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, CHARGE_TYPE,
                   ITEM_TOTAL, RECVD, 
                   DATE_FROM, DATE_TO, LAST_MODIFIED,
                   INV_ITEM_ID, ITEM_STATUS, TAX_INCL, NOTES
              FROM ( 
                SELECT ROW_NUMBER() OVER (PARTITION BY O.ACCOUNT_ID ORDER BY O.ORDER_ID DESC) RN,
                       v_in_bill_id BILL_ID, c_IN_PERIOD_ID REP_PERIOD_ID, 
                       v_item_id ITEM_ID, 
                       Pk00_Const.c_ITEM_TYPE_ADJUST ITEM_TYPE,
                       O.ORDER_ID, O.SERVICE_ID, OB.ORDER_BODY_ID, OB.SUBSERVICE_ID, 
                       Pk00_Const.c_CHARGE_TYPE_ONT CHARGE_TYPE,
                       -r_abn.TOTAL ITEM_TOTAL, 0 RECVD,
                       OB.DATE_FROM, OB.DATE_TO, SYSDATE LAST_MODIFIED,
                       NULL INV_ITEM_ID, 
                       Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS, 
                       c_TAX_INCL TAX_INCL, 
                       'Входящая задолженность на '||TO_CHAR(c_IN_PERIOD_TO+1/86400,'dd.mm.yyyy') NOTES
                  FROM ORDER_T O, ORDER_BODY_T OB
                 WHERE O.ACCOUNT_ID = v_account_id
                   AND O.ORDER_ID = OB.ORDER_ID
            )
            WHERE RN = 1;
            --
            -- добавляем счет в очередь на пересчет
            INSERT INTO BILLING_QUEUE_T Q (TASK_ID, BILL_ID, ACCOUNT_ID, PROFILE_ID, REP_PERIOD_ID, DATA_PERIOD_ID)
            VALUES(c_TASK_ID, v_in_bill_id, v_account_id, v_profile_id, c_IN_PERIOD_ID, c_IN_PERIOD_ID);
            
        ELSE
            v_in_bill_id := r_abn.IN_BILL_ID;
        END IF;

        -- Проставляем ID фиктивного платежа с входящим остатком денег
        IF r_abn.IN_PAYMENT_ID IS NULL  AND r_abn.TOTAL > 0 THEN

            v_in_payment_id := PK02_POID.Next_payment_id;
            
            -- используем INSERT, т.к. Pk10_Payment.Add_payment работает только для 2003 биллинга
            -- cохраняем информацию о платеже
            INSERT INTO PAYMENT_T (
                PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE,
                PAYMENT_DATE, ACCOUNT_ID, RECVD,
                ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED, REFUND,
                DATE_FROM, DATE_TO,
                PAYSYSTEM_ID, DOC_ID,
                STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED,
                CREATED_BY, NOTES, PAY_DESCR
            )VALUES(
                v_in_payment_id, c_IN_PERIOD_ID, Pk00_Const.c_PAY_TYPE_ADJUST,
                c_IN_PERIOD_TO, v_account_id, r_abn.TOTAL,
                r_abn.TOTAL, c_IN_PERIOD_TO, r_abn.TOTAL, 0, 0,
                NULL, NULL,
                c_PAYSYSTEM_ID, r_abn.CONTRACT_NO,
                Pk00_Const.c_PAY_STATE_OPEN, SYSDATE, SYSDATE, SYSDATE,
                'Макеев С.В.', 
                'импортировано из БАРСУМ '|| TO_CHAR(SYSDATE,'dd.mm.yyyy'), 
                'Входящая задолженность на 01.06.2015'
            );
            -- сохраняем ID
            UPDATE PK210_SPBF_IMPORT_T
               SET IN_PAYMENT_ID = v_in_payment_id
             WHERE CURRENT OF c_FILE_CSV;
             
        ELSE
            v_in_payment_id := r_abn.IN_PAYMENT_ID;
        END IF;
  
        v_count := v_count + 1;
        IF MOD(v_count, 100) = 0 THEN
            Pk01_Syslog.Write_msg(v_count||' rows.csv processed', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;

    END LOOP;	
    -- ИТОГО:    
    Pk01_Syslog.Write_msg(v_count||' rows.csv processed', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- В ФИНАЛЕ:
    -- пересчет счетов:
    -- расформировать счет, не трогая позиции счета
    Pk30_Billing_Queue.Rollback_bills( c_TASK_ID );
    -- формируем счет 
    Pk30_Billing_Queue.Close_bills( c_TASK_ID );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- пересчет входящих остатков, процедурой PK30_BILLING_BASE.Period_infoпользоваться нельзя 
    -- т.к. без счетов не сформирует обороты для платежей
    --
    DELETE FROM REP_PERIOD_INFO_T RP
    WHERE RP.ACCOUNT_ID IN (
        SELECT ACCOUNT_ID  
          FROM PK210_SPBF_IMPORT_T I
         WHERE I.ACCOUNT_ID IS NOT NULL
    );
    
    INSERT INTO REP_PERIOD_INFO_T (
        REP_PERIOD_ID, ACCOUNT_ID, OPEN_BALANCE, CLOSE_BALANCE,
        TOTAL, GROSS, RECVD, ADVANCE, LAST_MODIFIED
    )
    WITH MP AS ( 
        -- получаем сумму счетов за указанный период
        SELECT REP_PERIOD_ID, ACCOUNT_ID,
               SUM(BILL_TOTAL) BILL_TOTAL, SUM(GROSS) GROSS,
               SUM(RECVD) RECVD, SUM(ADVANCE) ADVANCE    
         FROM (
            SELECT B.REP_PERIOD_ID, B.ACCOUNT_ID,  
                   B.TOTAL BILL_TOTAL, B.GROSS,  
                   0 RECVD, 0 ADVANCE 
              FROM BILL_T B             
             WHERE B.REP_PERIOD_ID >= c_IN_PERIOD_ID
               AND B.ACCOUNT_ID IN (
                   SELECT ACCOUNT_ID  
                     FROM PK210_SPBF_IMPORT_T I
                    WHERE I.ACCOUNT_ID IS NOT NULL
               )
            --
            UNION ALL
            -- получаем сумму поступивших за период платежей
            SELECT P.REP_PERIOD_ID, P.ACCOUNT_ID, 
                   0 BILL_TOTAL, 0 GROSS,
                   P.RECVD, P.ADVANCE  
              FROM PAYMENT_T P
             WHERE P.REP_PERIOD_ID >= c_IN_PERIOD_ID
               AND P.ACCOUNT_ID IN (
                   SELECT ACCOUNT_ID  
                     FROM PK210_SPBF_IMPORT_T I
                    WHERE I.ACCOUNT_ID IS NOT NULL
               )
        )
        GROUP BY REP_PERIOD_ID, ACCOUNT_ID
    )
    SELECT REP_PERIOD_ID, ACCOUNT_ID, 0 OPEN_BALANCE, 
           MP.RECVD-MP.BILL_TOTAL CLOSE_BALANCE,
           MP.BILL_TOTAL, 
           MP.GROSS,
           MP.RECVD,
           MP.ADVANCE,
           SYSDATE LAST_MODIFIED 
      FROM MP;

    -- пересчет балансов л/с    
    MERGE INTO ACCOUNT_T A
    USING (
        WITH D AS (
            -- получаем строки задолженности по выставленным счетам
            SELECT B.ACCOUNT_ID, B.TOTAL BILL_TOTAL, B.ADJUSTED, 0 RECVD, 0 OPEN_BALANCE, BILL_DATE BALANCE_DATE
              FROM BILL_T B
             WHERE B.REP_PERIOD_ID >=  c_IN_PERIOD_ID
               AND B.ACCOUNT_ID IN (
                   SELECT ACCOUNT_ID  
                     FROM PK210_SPBF_IMPORT_T I
                    WHERE I.ACCOUNT_ID IS NOT NULL
               )
            --
            UNION ALL
            -- получаем строки поступивших за период платежей
            SELECT P.ACCOUNT_ID, 0 BILL_TOTAL, 0 ADJUSTED, P.RECVD, 0 OPEN_BALANCE, P.PAYMENT_DATE BALANCE_DATE  
            FROM PAYMENT_T P
            WHERE P.REP_PERIOD_ID >=  c_IN_PERIOD_ID
               AND P.ACCOUNT_ID IN (
                   SELECT ACCOUNT_ID  
                     FROM PK210_SPBF_IMPORT_T I
                    WHERE I.ACCOUNT_ID IS NOT NULL
               )
        ) 
        SELECT ACCOUNT_ID, 
               SUM(OPEN_BALANCE+ADJUSTED+RECVD-BILL_TOTAL ) BALANCE, 
               MAX(BALANCE_DATE) BALANCE_DATE
          FROM D
        GROUP BY ACCOUNT_ID
    )BAL
    ON( BAL.ACCOUNT_ID = A.ACCOUNT_ID )
    WHEN MATCHED THEN UPDATE 
      SET A.BALANCE = BAL.BALANCE, A.BALANCE_DATE = BAL.BALANCE_DATE;
            
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- загрузка платежей из Барсум (из CSV - файла)
--============================================================================================
PROCEDURE Load_payments
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Load_abp_min';
    v_count          INTEGER := 0;
    v_period_id      INTEGER;
    v_payment_id     INTEGER;
    CURSOR c_pay IS (
      SELECT CONTRACT_NO, 
             TO_DATE(PAYMENT_DATE,'dd.mm.yyyy hh24:mi') PAYMENT_DATE, 
             TO_NUMBER(REPLACE(VALUE,',','.')) VALUE, 
             BARSUM_ID, PAYMENT_ID, A.ACCOUNT_ID 
        FROM PK210_SPBF_PAYMENT_T PP, ACCOUNT_T A
       WHERE PP.CONTRACT_NO = A.EXTERNAL_ID
    )FOR UPDATE;
    
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- получаем ID текущего периода
    v_period_id := Pk04_Period.Period_id(SYSDATE);
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR r_pay IN c_pay LOOP
      
        v_payment_id := PK02_POID.Next_payment_id;
            
        -- используем INSERT, т.к. Pk10_Payment.Add_payment работает только для 2003 биллинга
        -- cохраняем информацию о платеже
        INSERT INTO PAYMENT_T (
            PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE,
            PAYMENT_DATE, ACCOUNT_ID, RECVD,
            ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED, REFUND,
            DATE_FROM, DATE_TO,
            PAYSYSTEM_ID, DOC_ID,
            STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED,
            CREATED_BY, NOTES, PAY_DESCR
        )VALUES(
            v_payment_id, v_period_id, Pk00_Const.c_PAY_TYPE_PAYMENT,
            r_pay.PAYMENT_DATE, r_pay.ACCOUNT_ID, r_pay.VALUE,
            r_pay.VALUE, r_pay.PAYMENT_DATE, r_pay.VALUE, 0, 0,
            NULL, NULL,
            c_PAYSYSTEM_ID, r_pay.BARSUM_ID,
            Pk00_Const.c_PAY_STATE_OPEN, SYSDATE, SYSDATE, SYSDATE,
            'Макеев С.В.', 
            'импортировано из БАРСУМ '|| TO_CHAR(SYSDATE,'dd.mm.yyyy'), 
            NULL
        );
        -- сохраняем ID
        UPDATE PK210_SPBF_PAYMENT_T
           SET PAYMENT_ID = v_payment_id
         WHERE CURRENT OF c_FILE_CSV;
         
    END LOOP;
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;
    
--============================================================================================
-- Догрузка информации о лицевых счетах клиентов
--============================================================================================
PROCEDURE Load_abp_min
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Load_abp_min';
    v_count          INTEGER := 0;
    v_rec_ob_id      INTEGER;
    v_min_ob_id      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    FOR rd IN (
        SELECT O.ORDER_ID, O.SERVICE_ID, RF.SUBSERVICE_ID, RF.PRICE, SS.CHARGE_TYPE, SS.SUBSERVICE,  
               RF.FREE_VALUE, RF.FREE_VALUE_UNIT, RF.RATEPLAN_ID, O.DATE_FROM
          FROM RATEPLAN_T R, RATEPLAN_FIXRATE_T RF, SUBSERVICE_T SS,
               ORDER_T O
         WHERE R.RATEPLAN_ID    = RF.RATEPLAN_ID
           AND RF.SUBSERVICE_ID = SS.SUBSERVICE_ID
           AND RF.RATEPLAN_ID   = O.RATEPLAN_ID
           AND RF.SERVICE_ID    = O.SERVICE_ID
    ) LOOP
    
        IF rd.Subservice_Id = Pk00_Const.c_SUBSRV_REC THEN
            IF rd.free_value IS NOT NULL THEN
                -- Добавляем компонент - абонплата c включенным трафиком
                v_rec_ob_id := Pk06_Order.Add_subs_abon_voice (
                   p_order_id      => rd.Order_Id,      -- ID заказа - услуги
                   p_subservice_id => rd.Subservice_Id, -- ID компонента услуги
                   p_value         => rd.Price,         -- сумма абонплаты
                   p_tax_incl      => c_TAX_INCL,       -- включен ли налог в сумму абонплаты
                   p_currency_id   => c_CURRENCY_ID,    -- валюта
                   p_free_traffic  => rd.free_value,    -- кол-во услуги в натуральном измерении
                   p_date_from     => rd.DATE_FROM
               );
            ELSE
                v_rec_ob_id := Pk06_Order.Add_subs_abon (
                   p_order_id      => rd.Order_Id,      -- ID заказа - услуги
                   p_subservice_id => rd.Subservice_Id, -- ID компонента услуги
                   p_value         => rd.Price,         -- сумма абонплаты
                   p_tax_incl      => c_TAX_INCL,       -- включен ли налог в сумму абонплаты
                   p_currency_id   => c_CURRENCY_ID,    -- валюта
                   p_quantity      => 1,                -- кол-во услуги в натуральном измерении
                   p_date_from     => rd.DATE_FROM,
                   p_date_to       => Pk00_Const.c_DATE_MAX
               );  
            END IF;
        ELSIF rd.Subservice_Id = Pk00_Const.c_SUBSRV_MIN THEN
           -- Добавляем компонент - минималка
           v_min_ob_id := Pk06_Order.Add_subs_min (
               p_order_id      => rd.Order_Id,     -- ID заказа - услуги
               p_subservice_id => rd.Subservice_Id,-- ID компонента услуги
               p_value         => rd.Price,        -- сумма абонплаты
               p_tax_incl      => c_TAX_INCL,      -- включен ли налог в сумму абонплаты
               p_currency_id   => c_CURRENCY_ID,   -- валюта
               p_rate_level_id => Pk00_Const.c_RATE_LEVEL_ORDER, -- уровень действия: компонент/заказ/лицевой счет
               p_date_from     => rd.DATE_FROM
           );
        END IF;

        v_count := v_count + 1;
        
    END LOOP;
    Pk01_Syslog.Write_msg('v_count = '||v_count, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Биллинг - тестовый
--============================================================================================
PROCEDURE Billing
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Billing';
    v_count          INTEGER := 0;
    v_task_id        INTEGER := 2007;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    DELETE FROM BILLING_QUEUE_T Q WHERE Q.TASK_ID = 2007;

    INSERT INTO BILLING_QUEUE_T Q (BILL_ID, ACCOUNT_ID, PROFILE_ID, TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID)
    SELECT B.BILL_ID, B.ACCOUNT_ID, B.PROFILE_ID, 2007, B.REP_PERIOD_ID, B.REP_PERIOD_ID 
    FROM BILL_T B, ACCOUNT_T A
    WHERE B.REP_PERIOD_ID = 201506
      AND B.ACCOUNT_ID = A.ACCOUNT_ID
      AND A.BILLING_ID = v_task_id;

    COMMIT;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Начисление абонентской платы и доплаты до минимальной суммы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Charge_fixrates( v_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Формирование счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Make_bills( v_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

END PK210_SPBF_IMPORT;
/
