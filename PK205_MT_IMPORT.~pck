CREATE OR REPLACE PACKAGE PK205_MT_IMPORT
IS
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- 
    -- Пакет для импорта начислений и платежей из биллинга ММТС "Микротест"
    -- по данным А.Ю.Гурова
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK205_MT_IMPORT';
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
    -- создание технологических лицевых счетов, по данным А.Ю.Гурова
    --
    PROCEDURE Create_tech_accounts;
    
    -- ----------------------------------------------------------------------------- --
    -- процедура выверки абонентов ММТС, по данным А.Ю.Гурова
    --
    FUNCTION get_order_no(p_order_no IN VARCHAR2) RETURN VARCHAR2;
    PROCEDURE Check_abonents(p_period_id IN INTEGER);
    
    -- ----------------------------------------------------------------------------- --
    -- Информация по адресам, как их должен видеть А.Ю.Гуров
    PROCEDURE Check_delivery_address;
    
    -- ----------------------------------------------------------------------------- --
    -- процедура заполнения таблицы PERIOD_T от рождения биллинга МИКРОТЕСТ
    PROCEDURE Insert_period;
    
    -- ----------------------------------------------------------------------------- --
    -- сдвинуть BILL_ID на заданную величину, для вставки исторических счетов
    --
    PROCEDURE Shift_bill_id (p_delta_id IN INTEGER);

    -- ----------------------------------------------------------------------------- --
    -- удалить ранее загруженную историю, включая указанный период 
    --
    PROCEDURE Rollback_billing(p_period_id IN INTEGER);
    
    -- ----------------------------------------------------------------------------- --
    -- загрузка данных по счетам (начисления) 
    -- процедура заполнения таблиц BILL_T
    -- за указанный месяц
    PROCEDURE Import_bill(p_period IN DATE);
    
    -- ----------------------------------------------------------------------------- --
    -- загрузка данных по позициям начислений 
    -- процедура заполнения таблиц ITEM_T
    -- за указанный месяц
    PROCEDURE Import_item(p_period IN DATE);
    
    -- ----------------------------------------------------------------------------- --
    -- загрузка данных по позициям счета-фактуры
    -- процедура заполнения таблиц ITEM_T
    -- до указанной даты
    PROCEDURE Import_invoice_item(p_period_id IN INTEGER);
    
    -- ----------------------------------------------------------------------------- --
    -- Миграция из временных таблиц в боевые
    PROCEDURE Migrate_table;
    
    -- ----------------------------------------------------------------------------- --
    -- загрузка данных по позициям начислений 
    -- процедура заполнения таблиц PAYMENT_T
    -- за указанное число периодов, нациная с заданного месяца
    PROCEDURE Import_payment(p_month IN DATE, p_periods IN INTEGER);
    
    -- ----------------------------------------------------------------------------- --
    -- Пересчитать баланс по всем выставленным счетам и оплатам 
    --   - при ошибке выставляем исключение
    PROCEDURE Refresh_all_balance;
    
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
    
    --============================================================================================
    --                  С Л У Ж Е Б Н Ы Е     П Р О Ц Е Д У Р Ы 
    --============================================================================================
    --  BILL_INFO_T
    PROCEDURE Bill_info_t_drop_fk;
    PROCEDURE Bill_info_t_add_fk;  
    --  BILL_T
    PROCEDURE Bill_t_drop_fk;
    PROCEDURE Bill_t_add_fk;
    --  ITEM_T
    PROCEDURE Item_t_drop_fk;
    PROCEDURE Item_t_add_fk;
    --  INVOICE_ITEM_T
    PROCEDURE Invoice_item_t_drop_fk;
    PROCEDURE Invoice_item_t_add_fk;
    --  PAY_TRANSFER_T
    PROCEDURE Transfer_t_drop_fk;
    PROCEDURE Transfer_t_add_fk;
    
END PK205_MT_IMPORT;
/
CREATE OR REPLACE PACKAGE BODY PK205_MT_IMPORT
IS

--============================================================================================
-- Собрать статистику по таблице
--
PROCEDURE Gather_Table_Stat(l_Tab_Name varchar2)
IS
    PRAGMA AUTONOMOUS_TRANSACTION; 
BEGIN 
    DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'PIN',
                                  TABNAME => l_Tab_Name,
                                  DEGREE  => 5,
                                  CASCADE => TRUE,
                                  NO_INVALIDATE => FALSE
                                 ); 
END;

--============================================================================================
-- Экскорт данных из старого биллинга по бренду '.SPB TTK Brand'
-- и услуге местного и зонового присоединения
--
PROCEDURE Import_data
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_data';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- удаляем данные из временной таблицы
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK205_INVOICES_T DROP STORAGE';
    -- заполняем временную таблицу, данными предоставленными А.Ю.Гуровым
    INSERT INTO PK205_INVOICES_T
    SELECT 
        ITEM_POID,        -- позиции строки начислений
        BUSINESS_TYPE,    -- тип клиента 1-физик, 2-юрик
        ACCOUNT_NO,       -- номер лицевого счета
        BILL_NO,          -- номер счета
        CONTRACT_NUM,     -- номер договора
        TRIM(COMPANY),    -- компания
        ORDER_NUM,        -- номер заказа
        SVC_ID,           -- 
        SERVICE,          -- услуга
        CURRENCY,         -- валюта начислений
        GROSS,            -- начисления без налогов
        DUE,              -- начисления с НДС
        USAGE_TYPE,       -- тип начислений
        BILL_DATE,        -- дата выставления счета - последний день месяца
        TRIM(XTTK_NAME),  -- имя хТТК
        TRIM(AGENT_NAME), -- имя агента
        BRAND_POID, 
        TRIM(BRAND_NAME), -- имя брэнда
        REP_DATE,         -- последний день рачсетного периода
        PARENT_ACC_NO,    -- номер корневого лицевого счета
        PARENT_ACC_ID,    -- id корневого лицевого счета
        TO_NUMBER(NULL) UB_ACCOUNT_NO,
        TO_CHAR(NULL)   UB_BILL_NO,        
        TO_NUMBER(TO_CHAR(TRUNC(REP_DATE,'mm'),'yyyymm')) UB_REP_PERIOD_ID,
        ADD_MONTHS(TRUNC(REP_DATE,'mm'),1)-1/86400 UB_BILL_DATE
      FROM MDV_ADM.G_INVOICES_T@MMTDB;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK205_INVOICES_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PK205_INVOICES_T');
    --
    -- удаляем данные из временной таблицы
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK205_PAYMENTS_T DROP STORAGE';
    INSERT INTO PK205_PAYMENTS_T
    SELECT 
        ACCOUNT_NO,     -- номер лицевого счета
        BANK_CODE,      -- код банка
        DOC_ID,         -- номер транзакции
        CREATED_T,      -- дата создания документа
        ACCOUNT_POID,   -- ID лицевого счета в биллинге Микротест
        BUSINESS_TYPE,  -- тип клиента 1-физик, 2-юрик
        PAY_DATE,       -- дата платежа
        SUB_BANK_CODE,  -- корреспондентский счет
        BILL_NO,        -- номер счета
        AMOUNT,         -- сумма оплаты
        MOD_T,          -- дата изменения строки оплаты
        DESCR,          -- описание платежа
        PARENT_ACC_NO,  -- номер корневого лицевого счета
        PARENT_ACC_ID,  -- id корневого лицевого счета
        TO_CHAR(NULL) UB_ACCOUNT_NO,
        TO_NUMBER(NULL) UB_PAYSYSTEM_ID
      FROM MDV_ADM.G_PAYMENTS_T@MMTDB;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK205_PAYMENTS_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PK205_PAYMENTS_T');
    --    
    INSERT INTO PK205_DELIVERY_T
    SELECT 
       SREFERENCE, 
       CCODECHAR, 
       CCOMPANYNAME, 
       CCOUNTRY, 
       CREGION, 
       CCITY, 
       CADDRESS, 
       CPOSTCODE, 
       CCONTACTNAME, 
       CPHONE 
      FROM MDV_ADM.G_DELIVERY_T@MMTDB;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK205_DELIVERY_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PK205_DELIVERY_T');
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- создание технологических лицевых счетов, по данным А.Ю.Гурова
--
PROCEDURE Create_tech_accounts
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Create_tech_accounts';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- создаем технологические лицевые счета
    -- ACC000000002 лицевой счет payment suspense
    INSERT INTO ACCOUNT_T (
       ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, CURRENCY_ID, 
       STATUS, PARENT_ID, BALANCE, BALANCE_DATE, CREATE_DATE, NOTES 
    )VALUES(
       Pk02_Poid.Next_account_id, 'ACC000000002', 'T', Pk00_Const.c_CURRENCY_RUB, 
       Pk00_Const.c_ACC_STATUS_TEST, NULL, 0, SYSDATE, SYSDATE,
       'payment suspense'
    );
    --
    -- ACC000000005 лицевой счет Александр Гуров
    INSERT INTO ACCOUNT_T (
       ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, CURRENCY_ID, 
       STATUS, PARENT_ID, BALANCE, BALANCE_DATE, CREATE_DATE, NOTES 
    )VALUES(
       Pk02_Poid.Next_account_id, 'ACC000000005', 'T', Pk00_Const.c_CURRENCY_RUB, 
       Pk00_Const.c_ACC_STATUS_TEST, NULL, 0, SYSDATE, SYSDATE,
       'Александр Гуров'
    );
    -- ACC000000008 лицевой счет Любовь Привезенцева
    INSERT INTO ACCOUNT_T (
       ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, CURRENCY_ID, 
       STATUS, PARENT_ID, BALANCE, BALANCE_DATE, CREATE_DATE, NOTES 
    )VALUES(
       Pk02_Poid.Next_account_id, 'ACC000000008', 'T', Pk00_Const.c_CURRENCY_RUB, 
       Pk00_Const.c_ACC_STATUS_TEST, NULL, 0, SYSDATE, SYSDATE,
       'Любовь Привезенцева'
    );
    --
    -- ACC000014887 лицевой счет payment trash
    INSERT INTO ACCOUNT_T (
       ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, CURRENCY_ID, 
       STATUS, PARENT_ID, BALANCE, BALANCE_DATE, CREATE_DATE, NOTES 
    )VALUES(
       Pk02_Poid.Next_account_id, 'ACC000014887', 'T', Pk00_Const.c_CURRENCY_RUB, 
       Pk00_Const.c_ACC_STATUS_TEST, NULL, 0, SYSDATE, SYSDATE,
       'payment trash'
    );
    --
    -- ACC000035683 лицевой счет payment back
    INSERT INTO ACCOUNT_T (
       ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, CURRENCY_ID, 
       STATUS, PARENT_ID, BALANCE, BALANCE_DATE, CREATE_DATE, NOTES 
    )VALUES(
       Pk02_Poid.Next_account_id, 'ACC000035683', 'T', Pk00_Const.c_CURRENCY_RUB, 
       Pk00_Const.c_ACC_STATUS_TEST, NULL, 0, SYSDATE, SYSDATE,
       'payment back'
    );
    --
    -- ACC000038685 лицевой счет payment garbage
    INSERT INTO ACCOUNT_T (
       ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, CURRENCY_ID, 
       STATUS, PARENT_ID, BALANCE, BALANCE_DATE, CREATE_DATE, NOTES 
    )VALUES(
       Pk02_Poid.Next_account_id, 'ACC000038685', 'T', Pk00_Const.c_CURRENCY_RUB, 
       Pk00_Const.c_ACC_STATUS_TEST, NULL, 0, SYSDATE, SYSDATE,
       'payment garbage'
    );
    --
    -- переставляем указатели на корневые счета, там где они ошибочно указывают на дочерние
    -- ACC000350754 ACC000343187 OOO "Интеллин" Михаил Воронцов
    UPDATE PK205_PAYMENTS_T 
       SET ACCOUNT_NO = 'ACC000343187'
     WHERE ACCOUNT_NO = 'ACC000350754';  
    --
    -- ACC000367809 ACC000348618 OOO "Байтек Машинери"
    UPDATE PK205_PAYMENTS_T 
       SET ACCOUNT_NO = 'ACC000348618'
     WHERE ACCOUNT_NO = 'ACC000367809';
    --
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- процедура выверки абонентов ММТС, по данным А.Ю.Гурова
--
FUNCTION get_order_no(p_order_no IN VARCHAR2) RETURN VARCHAR2 IS
    v_order_no   VARCHAR2(100);
    v_order_next INTEGER;
BEGIN
    v_order_no := p_order_no;
    IF v_order_no = 'б/н' THEN 
        v_order_next := SQ_ORDER_NO_NUM.NEXTVAL;  
        v_order_no := 'б/н-'||LPAD(v_order_next,3,'0');
    ELSE
        v_order_no := p_order_no;
    END IF;
    RETURN v_order_no; 
END;

PROCEDURE Check_abonents(p_period_id IN INTEGER)
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Check_abonents';
    v_count          INTEGER := 0;
    v_ok             INTEGER := 0;
    v_err            INTEGER := 0;
    v_msg_id         INTEGER;
    v_order_id       INTEGER;
    -- курсор для получения списка заказов, которые необходимо создать
    CURSOR c_abn IS 
      SELECT 
          ACCOUNT_ID, 
          ACCOUNT_NO, 
          BUSINESS_TYPE, 
          CONTRACT_NO, 
          ORDER_NO, 
          SERVICE_ID, 
          DATE_FROM, 
          MSG_ID
        FROM PK205_MT_NEW_ORDER_T
        FOR UPDATE OF MSG_ID;
    --
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- заполняем временную таблицу, с абонентами биллинга ММТС, по данным А.Ю.Гурова
    -- для указанного периода
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK205_MT_ABONENTS_T DROP STORAGE';
    INSERT INTO PK205_MT_ABONENTS_T 
    SELECT 
         PI.BUSINESS_TYPE, 
         TRIM(PI.CONTRACT_NUM)    CONTRACT_NUM, 
         TRIM(PI.ACCOUNT_NO)      ACCOUNT_NUM,
         TRIM(PI.PARENT_ACC_NO)   PARENT_ACC_NO,
         PI.PARENT_ACC_ID, 
         TRIM(PI.ORDER_NUM)       ORDER_NUM, 
         SUBSTR(PI.ORDER_NUM,3,2) SERVICE_TYPE, -- '00','FP' : FREECALL, 'LD' : МГ/МН, 'н' - UNKNOWN
         TRIM(PI.COMPANY)         COMPANY, 
         TRIM(PI.XTTK_NAME)       XTTK_NAME, 
         TRIM(PI.AGENT_NAME)      AGENT_NAME, 
         TRIM(PI.BRAND_NAME)      BRAND_NAME,
         MIN(PI.REP_DATE)         DATE_FROM,
         COUNT(*)                 CNT,
         TO_NUMBER(NULL,'9999999999999999') MSG_ID,
         TO_CHAR(NULL) UB_ACCOUNT_NO 
      FROM PK205_INVOICES_T PI
     WHERE PI.UB_REP_PERIOD_ID = p_period_id
    GROUP BY 
         PI.BUSINESS_TYPE, 
         TRIM(PI.CONTRACT_NUM), 
         TRIM(PI.ACCOUNT_NO),
         TRIM(PI.PARENT_ACC_NO),
         PI.PARENT_ACC_ID, 
         TRIM(PI.ORDER_NUM), 
         SUBSTR(PI.ORDER_NUM,3,2),
         TRIM(PI.COMPANY), 
         TRIM(PI.XTTK_NAME), 
         TRIM(PI.AGENT_NAME), 
         TRIM(PI.BRAND_NAME);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK205_MT_ABONENTS_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PK205_MT_ABONENTS_T');
    --
    -- заполняем временную таблицу с заказами которые необходимо досоздать в UNIBILL
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK205_MT_NEW_ORDER_T DROP STORAGE';
    INSERT INTO PK205_MT_NEW_ORDER_T
    WITH OG AS (
        SELECT  
            BUSINESS_TYPE, 
            CONTRACT_NUM,
            get_order_no(ORDER_NUM) ORDER_NUM,
            CASE
              WHEN SERVICE_TYPE = 'LD'         THEN 1
              WHEN SERVICE_TYPE IN ('00','FP') THEN 2
              ELSE -1
            END SERVICE_ID,
            MIN(DATE_FROM) DATE_FROM
        FROM PK205_MT_ABONENTS_T
        GROUP BY BUSINESS_TYPE, CONTRACT_NUM, ORDER_NUM,
                 get_order_no(ORDER_NUM),
                 CASE
                    WHEN SERVICE_TYPE = 'LD'         THEN 1
                    WHEN SERVICE_TYPE IN ('00','FP') THEN 2
                    ELSE -1
                 END
    ),
    AP AS (
        SELECT AP.CONTRACT_ID, MAX(ACCOUNT_ID) ACCOUNT_ID 
          FROM ACCOUNT_PROFILE_T AP
        GROUP BY AP.CONTRACT_ID
    )
    SELECT A.ACCOUNT_ID, A.ACCOUNT_NO, OG.BUSINESS_TYPE, C.CONTRACT_NO, 
           OG.ORDER_NUM ORDER_NO, 
           OG.SERVICE_ID, OG.DATE_FROM,
           TO_NUMBER(NULL,'9999999999999999') MSG_ID 
      FROM OG, AP, CONTRACT_T C, ACCOUNT_T A
     WHERE NOT EXISTS ( -- 1322
        SELECT * FROM ORDER_T O
         WHERE O.ORDER_NO = OG.ORDER_NUM 
     )
     AND OG.CONTRACT_NUM = C.CONTRACT_NO
     AND AP.CONTRACT_ID  = C.CONTRACT_ID
     AND AP.ACCOUNT_ID   = A.ACCOUNT_ID
    ORDER BY OG.CONTRACT_NUM;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK205_MT_NEW_ORDER_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PK205_MT_NEW_ORDER_T');
    COMMIT;
    
    Pk01_Syslog.Write_msg('Start new order creation.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- номера заказов в выгрузке А.Ю Гурова, которых нет в UNIBILL
    FOR r_abn IN c_abn
    LOOP
        SAVEPOINT X;  -- точка сохранения данных для лицевого счета
        BEGIN
            -- создаем заказ на лицевом счете
            v_order_id := Pk06_Order.New_order(
               p_account_id    => r_abn.account_id, -- ID лицевого счета
               p_order_no      => r_abn.order_no,   -- Номер заказа, как на бумаге
               p_service_id    => r_abn.service_id, -- ID услуги из таблицы SERVICE_T
               p_rateplan_id   => NULL,             -- ID тарифного плана из RATEPLAN_T
               p_time_zone     => NULL,             -- GMT               
               p_date_from     => r_abn.date_from,  -- дата начала действия заказа
               p_date_to       => Pk00_Const.c_DATE_MAX,
               p_create_date   => SYSDATE,
               p_note          => 'export from PK205_INVOICES_T/PK205_MT_ABONENTS_T/PK205_MT_NEW_ORDER_T' 
            );
            --            
            v_ok := v_ok + 1;
            --
            UPDATE PK205_MT_NEW_ORDER_T
               SET msg_id = 0
             WHERE CURRENT OF c_abn;
            --
        EXCEPTION
            WHEN OTHERS THEN
              -- откат изменений для лицевого счета
              ROLLBACK TO X;
              -- фиксируем ошибку в системе логирования
              v_msg_id := Pk01_Syslog.Fn_write_error(
                 p_Msg  => 'Account_no  =' || r_abn.account_no  ||', '
                        || 'Contract_no =' || r_abn.contract_no ||', '
                        || 'Order_no    =' || r_abn.order_no 
                        || ' - error',
                 p_Src  => c_PkgName||'.'||v_prcName);
              -- 
              UPDATE PK205_MT_NEW_ORDER_T
                 SET MSG_ID = v_msg_id
               WHERE CURRENT OF c_abn;
              --
              v_err := v_err + 1;
        END;
        v_count := v_ok + v_err;
        IF MOD(v_count, 100) = 0 THEN
            Pk01_Syslog.Write_msg('Processed: '||v_count||'-rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
    END LOOP;
    Pk01_Syslog.Write_msg('Processed: '||v_count||'-rows, '||v_ok||'-ok, '||v_err||'-err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    
--    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Информация по адресам, как их должен видеть А.Ю.Гуров
-- в таком виде не работает, нужна более тщательная выгрузка адресов
PROCEDURE Check_delivery_address
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Check_delivery_address';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- Исправить адреса доставки компани, по результатам прошедшего периода 
    MERGE INTO ACCOUNT_CONTACT_T AC
       USING (
            SELECT 
               AP.ACCOUNT_ID,
               GD.SREFERENCE, 
               GD.CCODECHAR    CONTRACT_NO, 
               GD.CCOMPANYNAME CUSTOMER, 
               GD.CCOUNTRY     COUNTRY, 
               GD.CREGION      STATE, 
               GD.CCITY        CITY, 
               GD.CADDRESS     ADDRESS, 
               GD.CPOSTCODE    ZIP, 
               GD.CCONTACTNAME PERSON, 
               GD.CPHONE       PHONES
              FROM PK205_DELIVERY_T GD, CONTRACT_T C, ACCOUNT_PROFILE_T AP
            WHERE GD.CCODECHAR   = C.CONTRACT_NO
              AND AP.CONTRACT_ID = C.CONTRACT_ID
       ) GD
       ON (GD.ACCOUNT_ID = AC.ACCOUNT_ID AND AC.ADDRESS_TYPE = 'DLV')
       WHEN MATCHED THEN UPDATE 
            SET AC.COUNTRY = GD.COUNTRY, AC.STATE = GD.STATE, AC.CITY = GD.CITY,
                AC.ADDRESS = GD.ADDRESS, AC.ZIP = GD.ZIP, AC.PERSON = GD.PERSON, 
                AC.PHONES  = GD.PHONES;

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- процедура заполнения таблицы PERIOD_T от рождения биллинга МИКРОТЕСТ
PROCEDURE Insert_period
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Insert_period';
    v_count          INTEGER := 0;
    v_date_min       DATE := TO_DATE('01.01.2008','dd.mm.yyyy');
    v_date_max       DATE;
    v_date_from      DATE;
    v_date_to        DATE;
    v_period_id      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    SELECT MIN(PERIOD_FROM) INTO v_date_max FROM PERIOD_T;
    --
    v_date_from := v_date_min;
    v_date_to   := ADD_MONTHS(v_date_min, 1);
    v_period_id := Pk04_Period.Period_id(v_date_from);
    --
    LOOP
        -- сохраняем период
        INSERT INTO PERIOD_T (
          PERIOD_ID, PERIOD_FROM, PERIOD_TO, CLOSE_REP_PERIOD, CLOSE_FIN_PERIOD, POSITION
        )VALUES(
          v_period_id, v_date_from, v_date_to, v_date_to, v_date_to, NULL
        );
        v_count := v_count + 1;
        -- переход к следующему периоду
        v_date_from := ADD_MONTHS(v_date_from, 1);
        v_date_to   := ADD_MONTHS(v_date_to, 1);
        v_period_id := Pk04_Period.Period_id(v_date_from);
        EXIT WHEN v_date_from >= v_date_max;
    END LOOP;
    --
    Gather_Table_Stat(l_Tab_Name => 'PERIOD_T');
    Pk01_Syslog.Write_msg('Stop: '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --   
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

    -- ----------------------------------------------------------------------------- --
-- сдвинуть BILL_ID на заданную величину, для вставки исторических счетов
--
PROCEDURE Shift_bill_id (p_delta_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Shift_bill_id';
    v_count         INTEGER;
    
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  DROP CONSTRAINT
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Bill_info_t_drop_fk;
    Bill_t_drop_fk;
    Item_t_drop_fk;
    Invoice_item_t_drop_fk;
    Transfer_t_drop_fk;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  DATA PROCESSING
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE BILL_T SET BILL_ID = BILL_ID + p_delta_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    UPDATE BILLINFO_T SET BILL_ID = BILL_ID + p_delta_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLINFO_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    UPDATE ITEM_T SET BILL_ID = BILL_ID + p_delta_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    UPDATE INVOICE_ITEM_T SET BILL_ID = BILL_ID + p_delta_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    UPDATE PAY_TRANSFER_T SET BILL_ID = BILL_ID + p_delta_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAY_TRANSFER_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  ADD CONSTRAINT
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Bill_info_t_add_fk;  
    Bill_t_add_fk;
    Item_t_add_fk;
    Invoice_item_t_add_fk;
    Transfer_t_add_fk;
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --   
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- удалить ранее загруженную историю, за указанный период
--
PROCEDURE Rollback_billing(p_period_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_billing';
    v_count         INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  DROP CONSTRAINT
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Bill_info_t_drop_fk;
    Bill_t_drop_fk;
    Item_t_drop_fk;
    Invoice_item_t_drop_fk;
    Transfer_t_drop_fk;

    -- откат всех входящих балансов 
    DELETE FROM REP_PERIOD_INFO_T WHERE REP_PERIOD_ID = p_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('deleted: '||v_count||' rows from rep_period_info_t', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- откат данных о разноске платежей
    DELETE FROM PAY_TRANSFER_T WHERE PAY_PERIOD_ID = p_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('deleted: '||v_count||' rows from pay_transfer_t', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- откат invoice_item_t
    DELETE FROM INVOICE_ITEM_T WHERE REP_PERIOD_ID = p_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('deleted: '||v_count||' rows from invoice_item_t', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- откат item_t
    DELETE FROM ITEM_T WHERE REP_PERIOD_ID = p_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('deleted: '||v_count||' rows from item_t', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- откат payment_t
    DELETE FROM PAYMENT_T WHERE REP_PERIOD_ID = p_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('deleted: '||v_count||' rows from payment_t', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- откат счетов bill_t
    DELETE FROM BILL_T WHERE REP_PERIOD_ID = p_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('deleted: '||v_count||' rows from bill_t', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- подтверждаем изменения
    COMMIT;
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  ADD CONSTRAINT
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--    Bill_info_t_add_fk;  
    Bill_t_add_fk;
    Item_t_add_fk;
    Invoice_item_t_add_fk;
    Transfer_t_add_fk;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- загрузка данных по счетам (начисления) 
-- процедура заполнения таблиц BILL_T
-- за указанный месяц
PROCEDURE Import_bill(p_period IN DATE)
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_bill';
    v_count          INTEGER := 0;
    v_date_from      DATE;
    v_date_to        DATE;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_date_from := TRUNC(p_period);
    v_date_to   := ADD_MONTHS(v_date_from, 1)-1/86400;  -- дата следующего периода
    --
    -- ------------------------------------------------------------------------- --
    -- Формирование записей BILL_T
    -- ------------------------------------------------------------------------- --
    EXECUTE IMMEDIATE 'TRUNCATE TABLE SVM_BILL_T DROP STORAGE';
    
    INSERT INTO SVM_BILL_T (
        BILL_ID, REP_PERIOD_ID, ACCOUNT_ID, BILL_NO, BILL_DATE, BILL_TYPE, 
        BILL_STATUS, CURRENCY_ID, TOTAL, GROSS, TAX, RECVD, DUE, DUE_DATE, 
        PAID_TO, PREV_BILL_ID, PREV_BILL_PERIOD_ID, 
        NEXT_BILL_ID, NEXT_BILL_PERIOD_ID, 
        CALC_DATE, ACT_DATE_FROM, ACT_DATE_TO, NOTES
    )
    WITH GB AS (
        SELECT UB_REP_PERIOD_ID,
               UB_BILL_DATE,
               UB_BILL_NO,     
               UB_ACCOUNT_NO,
               CURRENCY CURRENCY_ID,
               SUM(GROSS) GROSS,
               SUM(DUE)   TOTAL,
               MIN(BILL_DATE) ACT_DATE_FROM,
               MAX(BILL_DATE) ACT_DATE_TO
          FROM PK205_INVOICES_T
         WHERE REP_DATE BETWEEN v_date_from AND v_date_to
        GROUP BY 
               UB_REP_PERIOD_ID,
               UB_BILL_DATE,
               UB_BILL_NO, 
               UB_ACCOUNT_NO, 
               CURRENCY
    )
    SELECT --TO_NUMBER(NULL,'99999999999999999') BILL_ID,
           ROW_NUMBER() OVER (ORDER BY GB.UB_REP_PERIOD_ID, UB_BILL_NO) BILL_ID,
           GB.UB_REP_PERIOD_ID,
           A.ACCOUNT_ID,
           GB.UB_BILL_NO,
           GB.UB_BILL_DATE,
           'B' BILL_TYPE, 'CLOSED' BILL_STATUS, GB.CURRENCY_ID,
           GB.TOTAL, GB.GROSS, GB.TOTAL-GB.GROSS TAX, 0 RECVD,
           -GB.TOTAL DUE, GB.UB_BILL_DATE DUE_DATE,
           ADD_MONTHS(GB.UB_BILL_DATE,1) PAID_TO,
           NULL PREV_BILL_ID, NULL PREV_BILL_PERIOD_ID, 
           NULL NEXT_BILL_ID, NULL NEXT_BILL_PERIOD_ID, 
           GB.UB_BILL_DATE CALC_DATE, 
           GB.ACT_DATE_FROM, 
           GB.ACT_DATE_TO, 
           'imported from billing MMTS (A.Y.Gurov)' NOTES
      FROM GB, ACCOUNT_T A
     WHERE GB.UB_ACCOUNT_NO = A.ACCOUNT_NO
    ORDER BY UB_REP_PERIOD_ID, UB_BILL_NO;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'SVM_BILL_T');
    --
    UPDATE SVM_BILL_T SET BILL_ID = SQ_BILL_ID.NEXTVAL;
    --
    COMMIT;
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --   
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- загрузка данных по позициям начислений 
-- процедура заполнения таблиц ITEM_T
-- за указанный месяц
PROCEDURE Import_item(p_period IN DATE)
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_item';
    v_count          INTEGER := 0;
    v_date_from      DATE;
    v_date_to        DATE;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_date_from := TRUNC(p_period);
    v_date_to   := ADD_MONTHS(v_date_from, 1)-1/86400;  -- дата следующего периода

    EXECUTE IMMEDIATE 'TRUNCATE TABLE SVM_ITEM_T DROP STORAGE';
    
    -- ------------------------------------------------------------------------- --
    -- Формирование записей ITEM_T
    -- ------------------------------------------------------------------------- --
    INSERT INTO SVM_ITEM_T (
        BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
        CHARGE_TYPE, ITEM_TOTAL, ADJUSTED, RECVD, DATE_FROM, DATE_TO,
        CREATE_DATE, LAST_MODIFIED, REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID )
    SELECT B.BILL_ID,
           B.REP_PERIOD_ID,
           SQ_ITEM_ID.NEXTVAL ITEM_ID,
           --ROW_NUMBER() OVER (ORDER BY GI.REP_DATE, GI.UB_BILL_NO) ITEM_ID, -- имитируем последовательность
           CASE 
              WHEN GI.SERVICE = 'Корректировка'  THEN 'A'
              ELSE 'B'
           END ITEM_TYPE,
           O.ORDER_ID,
           CASE
              WHEN SUBSTR(GI.ORDER_NUM,3,2) = 'LD' THEN 1
              WHEN SUBSTR(GI.ORDER_NUM,3,2) IN ('00','FP') THEN 2
              ELSE -1
           END SERVICE_ID,
           CASE
              WHEN GI.SERVICE = 'Автоматическое междугородное телефонное соединения' THEN 1
              WHEN GI.SERVICE = 'Автоматическое международное телефонное соединение' THEN 2
              WHEN GI.SERVICE = 'Бесплатный вызов' THEN NULL
              WHEN GI.SERVICE = 'Корректировка' THEN NULL
              WHEN GI.SERVICE = 'Внутризоновая телефонная связь' THEN 6
              WHEN GI.SERVICE = 'Подключение' THEN 7
              WHEN GI.SERVICE = 'Бесплатный вызов\Плата за подключение' THEN 7 
              WHEN GI.SERVICE = 'Доплата до минимальной суммы за трафик' THEN 3
              WHEN GI.SERVICE = 'Разовая детализация' THEN 4
           END SUBSERVICE_ID,
           UPPER(GI.USAGE_TYPE) CHARGE_TYPE,
           CASE 
              WHEN GI.SERVICE != 'Корректировка' AND GI.BUSINESS_TYPE = 1 THEN GI.DUE
              WHEN GI.SERVICE != 'Корректировка' AND GI.BUSINESS_TYPE!= 1 THEN GI.GROSS
              ELSE 0
           END ITEM_TOTAL,
           CASE 
              WHEN GI.SERVICE = 'Корректировка' AND GI.BUSINESS_TYPE = 1 THEN GI.DUE
              WHEN GI.SERVICE = 'Корректировка' AND GI.BUSINESS_TYPE!= 1 THEN GI.GROSS 
              ELSE 0
           END ADJUSTED, 
           0 RECVD,
           TRUNC(GI.BILL_DATE,'mm') DATE_FROM, 
           ADD_MONTHS(TRUNC(GI.BILL_DATE,'mm'),1)-1/86400 DATE_TO,  
           GI.UB_BILL_DATE CREATE_DATE, 
           GI.UB_BILL_DATE LAST_MODIFIED,  
           GI.GROSS REP_GROSS, GI.DUE-GI.GROSS REP_TAX, 
           DECODE(GI.BUSINESS_TYPE,1,'Y', 'N') TAX_INCL, 
           GI.ITEM_POID EXTERNAL_ID
      FROM PK205_INVOICES_T GI, ORDER_T O, SVM_BILL_T B
    WHERE GI.ORDER_NUM = O.ORDER_NO
      AND GI.UB_BILL_NO= B.BILL_NO
      AND GI.UB_REP_PERIOD_ID = B.REP_PERIOD_ID
      AND GI.UB_BILL_DATE BETWEEN v_date_from AND v_date_to;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'SVM_ITEM_T');
    --
    COMMIT;
    --
    -- ------------------------------------------------------------------------- --
    -- Формирование записей INVOICE_ITEM_T
    -- ------------------------------------------------------------------------- --
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --   
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- загрузка данных по позициям счета-фактуры
-- процедура заполнения таблиц ITEM_T
-- до указанной даты
PROCEDURE Import_invoice_item(p_period_id IN INTEGER)
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Import_invoice_item';
    v_count          INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
   
    EXECUTE IMMEDIATE 'TRUNCATE TABLE SVM_INVOICE_ITEM_T DROP STORAGE';

    -- ------------------------------------------------------------------------- --
    -- Формирование записей INVOICE_ITEM_T
    -- ------------------------------------------------------------------------- --
    INSERT INTO SVM_INVOICE_ITEM_T (
        BILL_ID, REP_PERIOD_ID, 
        INV_ITEM_ID, INV_ITEM_NO, 
        SERVICE_ID, 
        TOTAL, GROSS, TAX, VAT, 
        INV_ITEM_NAME, DATE_FROM, DATE_TO
    )
    WITH INV AS (
        SELECT I.BILL_ID, I.REP_PERIOD_ID, I.SERVICE_ID,
               SUM(I.REP_GROSS+I.REP_TAX) TOTAL, SUM(I.REP_GROSS) GROSS, SUM(I.REP_TAX) TAX,
               MIN(I.DATE_FROM) DATE_FROM, MAX(I.DATE_TO) DATE_TO   
          FROM SVM_ITEM_T I
        GROUP BY BILL_ID, REP_PERIOD_ID, SERVICE_ID
    )
    SELECT INV.BILL_ID, INV.REP_PERIOD_ID,
           --ROWNUM INV_ITEM_ID,
           SQ_INVOICE_ITEM_ID.NEXTVAL INV_ITEM_ID,
           ROW_NUMBER() OVER (PARTITION BY INV.BILL_ID, INV.REP_PERIOD_ID ORDER BY INV.SERVICE_ID) INV_ITEM_NO,
           INV.SERVICE_ID, 
           INV.TOTAL, INV.GROSS, INV.TAX, 18 VAT,
           S.SERVICE INV_ITEM_NAME, 
           INV.DATE_FROM, INV.DATE_TO
      FROM INV, SERVICE_T S
    WHERE INV.SERVICE_ID = S.SERVICE_ID
      AND INV.REP_PERIOD_ID = p_period_id;
    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'SVM_INVOICE_ITEM_T');

    -- ------------------------------------------------------------------------- --
    -- В INVOICE_T простаить указатели на строки INVOICE_ITEM_T
    -- ------------------------------------------------------------------------- --
    MERGE INTO SVM_ITEM_T SI
       USING (
          SELECT REP_PERIOD_ID, BILL_ID, SERVICE_ID, INV_ITEM_ID
            FROM SVM_INVOICE_ITEM_T
       ) SV
       ON (  SI.REP_PERIOD_ID = SV.REP_PERIOD_ID
         AND SI.BILL_ID = SV.BILL_ID
         AND SI.SERVICE_ID = SV.SERVICE_ID
       )
       WHEN MATCHED THEN UPDATE SET SI.INV_ITEM_ID = SV.INV_ITEM_ID;
    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SVM_ITEM_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'SVM_ITEM_T');
    
    COMMIT;
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --   
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Миграция из временных таблиц в боевые
--============================================================================================
PROCEDURE Migrate_table
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Migrate_table';
    v_count          INTEGER := 0;
BEGIN
    --    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ------------------------------------------------------------------------- --
    -- Сбрасываем ограничения
    -- ------------------------------------------------------------------------- --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  DROP CONSTRAINT
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Bill_info_t_drop_fk;
    Bill_t_drop_fk;
    Item_t_drop_fk;
    Invoice_item_t_drop_fk;
    Transfer_t_drop_fk;

    -- ------------------------------------------------------------------------- --
    -- Миграция записей BILL_T
    -- ------------------------------------------------------------------------- --
    INSERT INTO BILL_T(
       BILL_ID, REP_PERIOD_ID, ACCOUNT_ID, BILL_NO, BILL_DATE, BILL_TYPE, 
       BILL_STATUS, CURRENCY_ID, TOTAL, GROSS, TAX, RECVD, DUE, DUE_DATE, 
       PAID_TO, PREV_BILL_ID, PREV_BILL_PERIOD_ID, 
       NEXT_BILL_ID, NEXT_BILL_PERIOD_ID, 
       CALC_DATE, ACT_DATE_FROM, ACT_DATE_TO, NOTES
    )
    SELECT
       BILL_ID, REP_PERIOD_ID, ACCOUNT_ID, BILL_NO, BILL_DATE, BILL_TYPE, 
       BILL_STATUS, CURRENCY_ID, TOTAL, GROSS, TAX, RECVD, DUE, DUE_DATE, 
       PAID_TO, PREV_BILL_ID, PREV_BILL_PERIOD_ID, 
       NEXT_BILL_ID, NEXT_BILL_PERIOD_ID, 
       CALC_DATE, ACT_DATE_FROM, ACT_DATE_TO, NOTES
    FROM SVM_BILL_T;
    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILL_T');
    
    COMMIT;
    -- ------------------------------------------------------------------------- --
    -- Миграция записей ITEM_T
    -- ------------------------------------------------------------------------- --
    INSERT INTO ITEM_T (
       BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
       ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
       CHARGE_TYPE, ITEM_TOTAL, ADJUSTED, RECVD, DATE_FROM, DATE_TO,
       CREATE_DATE, LAST_MODIFIED, REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID
    )
    SELECT
       BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID,  
       ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
       CHARGE_TYPE, ITEM_TOTAL, ADJUSTED, RECVD, DATE_FROM, DATE_TO,
       CREATE_DATE, LAST_MODIFIED, REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID
    FROM SVM_ITEM_T;
    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'ITEM_T');
    
    COMMIT;
    
    -- ------------------------------------------------------------------------- --
    -- Миграция записей INVOICE_ITEM_T
    -- ------------------------------------------------------------------------- --
    INSERT INTO INVOICE_ITEM_T(
       BILL_ID, REP_PERIOD_ID,
       INV_ITEM_ID, INV_ITEM_NO, SERVICE_ID, INV_ITEM_NAME, 
       VAT,         -- ставка НДС в процентах
       TOTAL,       -- сумма начислений с налогом
       GROSS,       -- сумма начислений без налога
       TAX,         -- сумма налога
       DATE_FROM, DATE_TO
    )
    SELECT 
       BILL_ID, REP_PERIOD_ID,
       INV_ITEM_ID, INV_ITEM_NO, SERVICE_ID, INV_ITEM_NAME, 
       VAT,         -- ставка НДС в процентах
       TOTAL,       -- сумма начислений с налогом
       GROSS,       -- сумма начислений без налога
       TAX,         -- сумма налога
       DATE_FROM, DATE_TO
    FROM SVM_INVOICE_ITEM_T;
    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'INVOICE_ITEM_T');
    
    COMMIT;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  ADD CONSTRAINT
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--    Bill_info_t_add_fk;  
    Bill_t_add_fk;
    Item_t_add_fk;
    Invoice_item_t_add_fk;
    Transfer_t_add_fk;
    --
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --   
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- загрузка данных по позициям начислений 
-- процедура заполнения таблиц PAYMENT_T
-- за указанное число периодов, нациная с заданного месяца
PROCEDURE Import_payment(p_month IN DATE, p_periods IN INTEGER)
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Import_payment';
    v_count     INTEGER := 0;
    v_date_from DATE;
    v_date_to   DATE;
    v_period_from INTEGER;
    v_period_to INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_date_from := TRUNC(p_month);
    v_date_to   := ADD_MONTHS(v_date_from, p_periods)-1/86400;  -- дата следующего периода
    v_period_from := Pk04_Period.Period_id(v_date_from);
    v_period_to   := Pk04_Period.Period_id(v_date_to);
        
    -- ------------------------------------------------------------------------- --
    -- Дополняем данные А.Г. информацией о ID платежной системы
    -- ------------------------------------------------------------------------- --
    MERGE INTO PK205_PAYMENTS_T GP
    USING (
        SELECT PS.PAYSYSTEM_CODE, PS.PAYSYSTEM_ID FROM PAYSYSTEM_T PS
    ) PS
    ON (GP.BANK_CODE = PS.PAYSYSTEM_CODE 
    AND GP.CREATED_T BETWEEN v_date_from AND v_date_to
    )
    WHEN MATCHED THEN UPDATE SET GP.UB_PAYSYSTEM_ID = PS.PAYSYSTEM_ID;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK205_PAYMENTS_T: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ------------------------------------------------------------------------- --
    -- Удаляем внешние ключи
    -- ------------------------------------------------------------------------- --
    Transfer_t_drop_fk;
    
    -- ------------------------------------------------------------------------- --
    -- Удаляем данные о разноске платежей
    -- ------------------------------------------------------------------------- --
    DELETE FROM PAY_TRANSFER_T
    WHERE PAY_PERIOD_ID BETWEEN v_period_from AND v_period_to;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAY_TRANSFER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- ------------------------------------------------------------------------- --
    -- Удаляем данные о платежах
    -- ------------------------------------------------------------------------- --
    DELETE FROM PAYMENT_T
    WHERE REP_PERIOD_ID BETWEEN v_period_from AND v_period_to;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAYMENT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ------------------------------------------------------------------------- --
    -- Формирование записей PAYMENT_T
    -- ------------------------------------------------------------------------- --
    INSERT INTO PAYMENT_T (
           PAYMENT_ID, REP_PERIOD_ID, ACCOUNT_ID, 
           RECVD, ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED, 
           DATE_FROM, DATE_TO, PAYMENT_DATE, PAYMENT_TYPE, 
           PAYSYSTEM_ID, DOC_ID, STATUS, STATUS_DATE, 
           CREATE_DATE, CREATED_BY, LAST_MODIFIED, 
           PAYSYSTEM_CODE, PAY_DESCR, PREV_PAYMENT_ID, PREV_PERIOD_ID, 
           REFUND, MODIFIED_BY, NOTES, EXTERNAL_ID
    )
    WITH GP AS (
        SELECT ACCOUNT_NO, UB_ACCOUNT_NO, BANK_CODE, DOC_ID, CREATED_T, ACCOUNT_POID, 
               BUSINESS_TYPE, PAY_DATE, SUB_BANK_CODE, BILL_NO, AMOUNT, MOD_T, DESCR,
               UB_PAYSYSTEM_ID,
               ROW_NUMBER() OVER (PARTITION BY DOC_ID ORDER BY BANK_CODE, ACCOUNT_NO) DOC_SUBNUM
          FROM PK205_PAYMENTS_T
    )
    SELECT (ROWNUM+1500000) PAYMENT_ID,   -- TO_NUMBER(NULL,'99999999999999999')
           TO_NUMBER(TO_CHAR(TRUNC(CREATED_T,'mm'),'yyyymm')) REP_PERIOD_ID,
           ACCOUNT_ID,
           AMOUNT RECVD, 0 ADVANCE, ADD_MONTHS(TRUNC(CREATED_T,'mm'),1)-1/86400 ADVANCE_DATE,
           AMOUNT BALANCE, 0 TRANSFERED, TO_DATE(NULL) DATE_FROM, TO_DATE(NULL) DATE_TO,
           PAY_DATE PAYMENT_DATE, 'IMP' PAYMENT_TYPE,
           UB_PAYSYSTEM_ID PAYSYSTEM_ID,
           CASE 
             WHEN DOC_SUBNUM > 1 THEN DOC_ID||'-x'||DOC_SUBNUM
             ELSE DOC_ID
           END DOC_ID, 'IMPORT' STATUS, SYSDATE STATUS_DATE,
           CREATED_T CREATE_DATE, 'Gurov A.Y.' CREATE_BY, MOD_T LAST_MODIFIED,
           BANK_CODE||DECODE(SUB_BANK_CODE, NULL, '', '/'||SUB_BANK_CODE) PAYSYSTEM_CODE, 
           DESCR PAY_DESCR,  
           NULL PREV_PAYMENT_ID, NULL PREV_PERIOD_ID,
           0 REFUND, NULL MODIFIED_BY, BILL_NO, ACCOUNT_POID EXTERNAL_ID
     FROM GP, ACCOUNT_T A
    WHERE CREATED_T BETWEEN v_date_from AND v_date_to
      AND A.ACCOUNT_NO = GP.ACCOUNT_NO;
--      AND A.ACCOUNT_NO = GP.UB_ACCOUNT_NO;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAYMENT_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'PAYMENT_T');
    --
    COMMIT;
    --
    -- ------------------------------------------------------------------------- --
    -- Восстанавливаем внешние ключи
    -- ------------------------------------------------------------------------- --
    Transfer_t_add_fk;
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --   
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- Пересчитать баланс по всем выставленным счетам и оплатам 
--   - при ошибке выставляем исключение
PROCEDURE Refresh_all_balance
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Refresh_all_balance';
    v_count      INTEGER;
BEGIN
  
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    MERGE INTO ACCOUNT_T A
    USING   
       (SELECT ACCOUNT_ID, SUM(RECVD-BILL_TOTAL) BALANCE,
               CASE
                   WHEN MAX(BILL_DATE) > MAX(PAYMENT_DATE) THEN MAX(BILL_DATE)
                   ELSE MAX(PAYMENT_DATE)
               END BALANCE_DATE 
        FROM (
            -- получаем полную задолженность по выставленным счетам
            SELECT B.ACCOUNT_ID, 
                   (B.GROSS+B.TAX) BILL_TOTAL, BILL_DATE, 
                   0 RECVD, TO_DATE('01.01.2000','dd.mm.yyyy') PAYMENT_DATE 
              FROM BILL_T B
             WHERE B.TOTAL > 0 -- отсекаем секцию с пустыми счетами
            UNION ALL
            -- получаем сумму поступивших за период платежей
            SELECT P.ACCOUNT_ID, 
                   0 BILL_TOTAL, TO_DATE('01.01.2000','dd.mm.yyyy') BILL_DATE,
                   P.RECVD, P.PAYMENT_DATE  
              FROM PAYMENT_T P
        )
        GROUP BY ACCOUNT_ID) T
    ON (A.ACCOUNT_ID = T.ACCOUNT_ID)
    WHEN MATCHED THEN UPDATE SET A.BALANCE_DATE = T.BALANCE_DATE, A.BALANCE = T.BALANCE;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T: '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    COMMIT;

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION   -- при ошибке выставляем исключение
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
    v_brand_id       INTEGER;
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
               
            -- получить бренд
            BEGIN
                -- получить ID бренда
                SELECT BRAND_ID INTO v_brand_id
                  FROM BRAND_T
                 WHERE BRAND = r_cust.BRAND;
            EXCEPTION WHEN NO_DATA_FOUND THEN
                v_brand_id := NULL;
            END;
               
            -- создать профиль лицевого счета
            v_profile_id := PK05_ACCOUNT.Set_profile(
                       p_account_id    => v_account_id,
                       p_brand_id      => v_brand_id,
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

--============================================================================================
--                  С Л У Ж Е Б Н Ы Е     П Р О Ц Е Д У Р Ы 
--============================================================================================
PROCEDURE Run_DDL(p_ddl IN VARCHAR2) IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Run_DDL';
BEGIN
    EXECUTE IMMEDIATE p_ddl;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.Write_error('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  BILL_INFO_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Bill_info_t_drop_fk
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Bill_info_t_drop_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Run_DDL('ALTER TABLE PIN.BILLINFO_T DROP CONSTRAINT BILLINFO_T_ACCOUNT_T_FK');
    Run_DDL('ALTER TABLE PIN.BILLINFO_T DROP CONSTRAINT BILLINFO_T_BILL_T_FK');
    Run_DDL('ALTER TABLE PIN.BILLINFO_T DROP CONSTRAINT BILLINFO_T_CURRENCY_T_FK');
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
PROCEDURE Bill_info_t_add_fk
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Bill_info_t_add_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    EXECUTE IMMEDIATE 'ALTER TABLE PIN.BILLINFO_T ADD (
      CONSTRAINT BILLINFO_T_ACCOUNT_T_FK 
      FOREIGN KEY (ACCOUNT_ID) 
      REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
      ENABLE VALIDATE,
      CONSTRAINT BILLINFO_T_BILL_T_FK 
      FOREIGN KEY (BILL_ID, PERIOD_ID) 
      REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
      ENABLE VALIDATE,
      CONSTRAINT BILLINFO_T_CURRENCY_T_FK 
      FOREIGN KEY (CURRENCY_ID) 
      REFERENCES PIN.CURRENCY_T (CURRENCY_ID)
      ENABLE VALIDATE)';
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;  

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  BILL_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Bill_t_drop_fk
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Bill_t_drop_fk';
BEGIN 
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Run_DDL('ALTER TABLE PIN.BILL_T DROP CONSTRAINT BILL_T_ACCOUNT_T_FK');
    Run_DDL('ALTER TABLE PIN.BILL_T DROP CONSTRAINT BILL_T_PERIOD_T_FK');
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE Bill_t_add_fk
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Bill_t_add_fk';
BEGIN 
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    EXECUTE IMMEDIATE 'ALTER TABLE PIN.BILL_T ADD (
      CONSTRAINT BILL_T_ACCOUNT_T_FK 
      FOREIGN KEY (ACCOUNT_ID) 
      REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
      ENABLE VALIDATE,
      CONSTRAINT BILL_T_PERIOD_T_FK 
      FOREIGN KEY (REP_PERIOD_ID) 
      REFERENCES PIN.PERIOD_T (PERIOD_ID)
      ENABLE VALIDATE)';
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  ITEM_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Item_t_drop_fk
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Item_t_drop_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Run_DDL('ALTER TABLE PIN.ITEM_T DROP CONSTRAINT ITEM_T_BILL_T_FK'); 
    Run_DDL('ALTER TABLE PIN.ITEM_T DROP CONSTRAINT ITEM_T_INVOICE_ITEM_T_FK');
    Run_DDL('ALTER TABLE PIN.ITEM_T DROP CONSTRAINT ITEM_T_ORDER_T_FK');
    Run_DDL('ALTER TABLE PIN.ITEM_T DROP CONSTRAINT ITEM_T_SERVICE_T_FK');
    Run_DDL('ALTER TABLE PIN.ITEM_T DROP CONSTRAINT ITEM_T_SUBSERVICE_T_FK');  
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE Item_t_add_fk
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Item_t_add_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    EXECUTE IMMEDIATE 'ALTER TABLE PIN.ITEM_T ADD (
      CONSTRAINT ITEM_T_BILL_T_FK 
      FOREIGN KEY (BILL_ID, REP_PERIOD_ID) 
      REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
      ENABLE VALIDATE,
      CONSTRAINT ITEM_T_INVOICE_ITEM_T_FK 
      FOREIGN KEY (BILL_ID, REP_PERIOD_ID, INV_ITEM_ID) 
      REFERENCES PIN.INVOICE_ITEM_T (BILL_ID,REP_PERIOD_ID,INV_ITEM_ID)
      ENABLE VALIDATE,
      CONSTRAINT ITEM_T_ORDER_T_FK 
      FOREIGN KEY (ORDER_ID) 
      REFERENCES PIN.ORDER_T (ORDER_ID)
      ENABLE VALIDATE,
      CONSTRAINT ITEM_T_SERVICE_T_FK 
      FOREIGN KEY (SERVICE_ID) 
      REFERENCES PIN.SERVICE_T (SERVICE_ID)
      ENABLE VALIDATE,
      CONSTRAINT ITEM_T_SUBSERVICE_T_FK 
      FOREIGN KEY (SUBSERVICE_ID) 
      REFERENCES PIN.SUBSERVICE_T (SUBSERVICE_ID)
      ENABLE VALIDATE)';
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  INVOICE_ITEM_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Invoice_item_t_drop_fk
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Invoice_item_t_drop_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Run_DDL('ALTER TABLE PIN.INVOICE_ITEM_T DROP CONSTRAINT INVOICE_ITEM_T_BILL_T_FK'); 
    Run_DDL('ALTER TABLE PIN.INVOICE_ITEM_T DROP CONSTRAINT INVOICE_ITEM_T_SERVICE_T_FK');
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE Invoice_item_t_add_fk
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Invoice_item_t_add_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    EXECUTE IMMEDIATE 'ALTER TABLE PIN.INVOICE_ITEM_T ADD (
      CONSTRAINT INVOICE_ITEM_T_BILL_T_FK 
      FOREIGN KEY (BILL_ID, REP_PERIOD_ID) 
      REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
      ENABLE VALIDATE,
      CONSTRAINT INVOICE_ITEM_T_SERVICE_T_FK 
      FOREIGN KEY (SERVICE_ID) 
      REFERENCES PIN.SERVICE_T (SERVICE_ID)
      ENABLE VALIDATE)';
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  PAY_TRANSFER_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Transfer_t_drop_fk
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Transfer_t_drop_fk';
BEGIN 
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Run_DDL('ALTER TABLE PIN.PAY_TRANSFER_T DROP CONSTRAINT PAY_TRANSFER_ID_BILL_T_FK'); 
    Run_DDL('ALTER TABLE PIN.PAY_TRANSFER_T DROP CONSTRAINT PAY_TRANSFER_ID_PAYMENT_T_FK'); 
    Run_DDL('ALTER TABLE PIN.PAY_TRANSFER_T DROP CONSTRAINT PAY_TRANSFER_T_FK'); 
    --    
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
PROCEDURE Transfer_t_add_fk
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Transfer_t_add_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
   
    Run_DDL('ALTER TABLE PIN.PAY_TRANSFER_T ADD CONSTRAINT 
      PAY_TRANSFER_ID_BILL_T_FK
      FOREIGN KEY (BILL_ID, REP_PERIOD_ID) 
      REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
      ENABLE VALIDATE');
    
    Run_DDL('ALTER TABLE PIN.PAY_TRANSFER_T ADD CONSTRAINT 
      PAY_TRANSFER_ID_PAYMENT_T_FK
      FOREIGN KEY (PAYMENT_ID, PAY_PERIOD_ID) 
      REFERENCES PIN.PAYMENT_T (PAYMENT_ID,REP_PERIOD_ID)
      ENABLE VALIDATE');
    
    Run_DDL('ALTER TABLE PIN.PAY_TRANSFER_T ADD CONSTRAINT 
      PAY_TRANSFER_T_FK
      FOREIGN KEY (PREV_TRANSFER_ID, PAY_PERIOD_ID) 
      REFERENCES PIN.PAY_TRANSFER_T (TRANSFER_ID,PAY_PERIOD_ID)
      ENABLE VALIDATE');

    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


END PK205_MT_IMPORT;
/
