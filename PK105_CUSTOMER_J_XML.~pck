CREATE OR REPLACE PACKAGE PK105_CUSTOMER_J_XML
IS
    --
    -- Пакет для печати КВИТАНЦИЙ на оплату для ФИЗИЧЕСКИХ ЛИЦ
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK105_CUSTOMER_J_XML';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- Максимальное значение, которое подставляется в поле DATE_TO вместо NULL
    c_MAX_DATE_TO constant date := TO_DATE('01.01.2050','dd.mm.yyyy');
    -- ID продавца для физического лица CONTRACTOR_T (продавец один на всех)
    c_CONTRACTOR_KTTK_ID constant integer := 1;
    -- ID клиента для физ лиц (один на всех)
    c_CLIENT_ID constant integer := 1;
    -- ID менеджера для физического лица MANAGER_T (пока один на всех)
    c_MANAGER_SIEBEL_ID  constant integer := 1;
    -- ID менеджера для физического лица MANAGER_T ЦСС
    c_MANAGER_CSS_ID  constant integer := 2;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- Регистрируем XML-файл в системе загрузки
    -- возвращает XML_SUBS_FILES_T.XML_FILE_ID
    FUNCTION Load_start(
                 p_filename IN VARCHAR2
             ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- Сохраняем отчет о загрузке XML-файла
    -- - при ошибке выставляет исключение
    PROCEDURE Load_stop(
                 p_xml_file_id IN INTEGER,
                 p_state       IN XML_SUBS_FILES_T.STATE%TYPE,
                 p_records     IN INTEGER,
                 p_ok          IN INTEGER,
                 p_err         IN INTEGER,
                 p_notes       IN XML_SUBS_FILES_T.NOTES%TYPE
             );

		-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
		-- добавить банковские реквизиты
		--   - положительное - ID бренда 
		--   - NULL - не наден, сообщить администратору
		--   - при ошибке выставляет исключение
		procedure Add_Bank_Data(
			p_customer_id			in number,
			p_bank_name				in varchar2,
			p_bank_code				in varchar2,
			p_corr_account		in varchar2,
			p_settl_account		in varchar2
			);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- найти ID бренда
    --   - положительное - ID бренда 
    --   - NULL - не наден, сообщить администратору
    --   - при ошибке выставляет исключение
    FUNCTION Get_brand_id( 
                   p_brand IN VARCHAR2        -- Имя контрагента 
               ) RETURN INTEGER;              -- ID контрагента

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- найти куратора, если не нашли - создать
    --   - положительное - ID куратора 
    --   - при ошибке выставляет исключение
    FUNCTION Get_curator_id( 
                   p_brand_id   IN INTEGER,   -- ID бренда
                   p_first_name IN VARCHAR2,  -- имя
                   p_last_name  IN VARCHAR2   -- фамилия
               ) RETURN INTEGER;              -- ID менеджера

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- найти тарифный план
    --   - положительное - ID тарифного плана 
    --   - NULL - не надйен, сообщить администратору
    --   - при ошибке выставляет исключение
    FUNCTION Get_rateplan_id_by_name( 
                   p_rateplan_name IN VARCHAR2 -- имя тарифного плана 
               ) RETURN INTEGER;               -- ID тарифного плана

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- создать нового покупателя-физ.лицо, возвращает значения
    --   - положительное - ID покупателя в биллинге, 
    --   - NULL - не надйен, создаем нового
    --   - при ошибке выставляет исключение
    FUNCTION Find_subscriber_by_xmlid(
                   p_xml_subscr_id  IN INTEGER
               ) RETURN INTEGER;                          

    -- найти покупателя-Юр.лицо по ИНН, КПП, возвращает значения
    --   - положительное - ID покупателя в биллинге, 
    --   - NULL - не надйен, создаем нового
    --   - при ошибке выставляет исключение
    FUNCTION Find_customer (
                   p_inn        IN VARCHAR2,   -- ИНН
                   p_kpp        IN VARCHAR2    -- КПП 
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- создать нового покупателя-Юр.лицо, возвращает значения
    --   - положительное - ID покупателя, 
    --   - при ошибке выставляет исключение
    FUNCTION New_customer(
                   p_erp_code    IN VARCHAR2,
                   p_inn         IN VARCHAR2,
                   p_kpp         IN VARCHAR2, 
                   p_name        IN VARCHAR2,
                   p_short_name  IN VARCHAR2
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- создать лицевой счет клиента
    --   - положительное - ID лицевого счета 
    --   - при ошибке выставляет исключение
    FUNCTION New_account(
                   p_xml_subscr_id  IN INTEGER,  -- ID клиента (XML) в удаленной системе
                   p_contractor_id  IN INTEGER,  -- ID поставщика услуги, по умолчанию КТТК
                   p_customer_id    IN INTEGER,  -- ID клиента в биллинге
                   p_brand_id       IN INTEGER,  -- бренд (продавец/агент)
                   p_curator_id     IN INTEGER,  -- куратор (менеджер)
                   p_rateplan_id    IN INTEGER,  -- тарифный план
                   p_date           IN DATE,     -- дата начала действия договора
                   p_contract_no   OUT VARCHAR2, -- номер договора
                   p_account_no    OUT VARCHAR2, -- номер лицевого счета
                   p_order_no      OUT VARCHAR2  -- номер заказа
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- добавить адрес на л/с:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Add_address(
                   p_account_id    IN INTEGER,   -- ID Л/С
                   p_address_type  IN VARCHAR2,  -- тип аддреса (см. pk00_const)
                   p_country       IN VARCHAR2,  -- 'РФ' - страна, 99.9999999999% случаях РФ
                   p_zip           IN VARCHAR2,  -- почтовый индекс
                   p_state         IN VARCHAR2,  -- регион (область )
                   p_city          IN VARCHAR2,  -- город
                   p_address       IN VARCHAR2,  -- адрес в одну строку
                   p_person        IN VARCHAR2,  -- ФИО
                   p_phones        IN VARCHAR2,  -- контактный телефон, может отличаться от купленного
                   p_email         IN VARCHAR2,  
                   p_date_from     IN DATE,
                   p_date_to       IN DATE DEFAULT NULL
               ) RETURN INTEGER;                 -- ID строки адреса

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- добавить телефон на лицевой счет клиента
    --   - положительное - ID лицевого счета 
    --   - при ошибке выставляет исключение
    PROCEDURE Add_phone(
                   p_account_id   IN INTEGER,    -- ID лицевого счета
                   p_phone        IN VARCHAR2,   -- номер телефона
                   p_date_from    IN DATE,       -- дата начала действия
                   p_date_to      IN DATE DEFAULT c_MAX_DATE_TO
               );

               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- удалить все данные для покупателя-физ.лицо по внешнему XML.SUBSCRIBER_ID
    PROCEDURE Delete_xml_subscriber(
                   p_xml_subscr_id IN INTEGER
               );
		
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Просмотр сообщений для указанной функции в системе логирования 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Msg_list(
                  p_recordset OUT t_refc,
                  p_function   IN VARCHAR2,                   -- имя функции
                  p_date_from  IN DATE DEFAULT (SYSDATE-30)   -- время старта функции (ориентировочное)
               );
    
    
END PK105_CUSTOMER_J_XML;
/
CREATE OR REPLACE PACKAGE BODY PK105_CUSTOMER_J_XML
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- Регистрируем XML-файл в системе загрузки
-- возвращает XML_SUBS_FILES_T.XML_FILE_ID
FUNCTION Load_start(
             p_filename IN VARCHAR2
         ) RETURN INTEGER
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Load_start';
    v_file_id XML_SUBS_FILES_T.XML_FILE_ID%TYPE;
BEGIN
    --
    INSERT INTO XML_SUBS_FILES_T(XML_FILE_ID, FILE_NAME, LOAD_START)
    VALUES(SQ_XML_FILE_ID.NEXTVAL, p_filename, SYSDATE)
    RETURNING XML_FILE_ID INTO v_file_id;
    --
    Pk01_Syslog.Write_msg(p_Msg   => 'Start. File.xml='||p_filename,
                          p_Src   => c_PkgName||'.'||v_prcName ,
                          p_Level => Pk01_Syslog.L_info );
    --
    RETURN v_file_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- Сохраняем отчет о загрузке XML-файла
-- - при ошибке выставляет исключение
PROCEDURE Load_stop(
             p_xml_file_id IN INTEGER,
             p_state       IN XML_SUBS_FILES_T.STATE%TYPE,
             p_records     IN INTEGER,
             p_ok          IN INTEGER,
             p_err         IN INTEGER,
             p_notes       IN XML_SUBS_FILES_T.NOTES%TYPE
         )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Load_start';
BEGIN
    --
    UPDATE XML_SUBS_FILES_T SET 
           LOAD_STOP = SYSDATE,
           STATE     = p_state,
           RECORDS   = p_records,
           LOAD_OK   = p_ok,
           LOAD_ER   = p_err,
           NOTES     = p_notes
     WHERE XML_FILE_ID = p_xml_file_id;
    --
    Pk01_Syslog.Write_msg(p_Msg   => 'Stop. File.xml.id='||p_xml_file_id||', status='||p_state,
                          p_Src   => c_PkgName||'.'||v_prcName ,
                          p_Level => Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;




-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- добавить банковские реквизиты
--   - положительное - ID бренда 
--   - NULL - не наден, сообщить администратору
--   - при ошибке выставляет исключение
procedure Add_Bank_Data(
	p_customer_id			in number,
	p_bank_name				in varchar2,
	p_bank_code				in varchar2,
	p_corr_account		in varchar2,
	p_settl_account		in varchar2
	) is
	v_cnt		number;
	v_prcName       CONSTANT VARCHAR2(30) := 'Add_Bank_Data';
begin
	-- проверить наличие реквизитов
	begin
		select customer_id into v_cnt
		from customer_bank_t
		where customer_id = p_customer_id;
		-- обновить
		update customer_bank_t
		set bank_name = p_bank_name, 
				bank_code = p_bank_code, 
				bank_corr_account = p_corr_account, 
				bank_settlement = p_settl_account
		where customer_id = p_customer_id;
	exception when no_data_found then
		-- нет данных
			insert into customer_bank_t
				(customer_id, bank_name, bank_code, bank_corr_account, bank_settlement)
			values(p_customer_id, p_bank_name, p_bank_code, p_corr_account, p_settl_account);
	end;
exception when others then
	Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
end;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- найти ID бренда
--   - положительное - ID бренда 
--   - NULL - не наден, сообщить администратору
--   - при ошибке выставляет исключение
FUNCTION Get_brand_id( 
               p_brand IN VARCHAR2   -- Имя контрагента 
           ) RETURN INTEGER          -- ID контрагента
IS
    v_brand_id INTEGER;
BEGIN
    -- поиск только по уровню БРЕНД, если не нашли, должен завести АДМИН
    SELECT CONTRACTOR_ID INTO v_brand_id
      FROM CONTRACTOR_T
     WHERE CONTRACTOR_TYPE = 'CSS'--Pk00_Const.c_CTR_TYPE_BRAND
       AND CONTRACTOR = p_brand
    ;
    RETURN v_brand_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- найти куратора, если не нашли - создать
--   - положительное - ID куратора 
--   - при ошибке выставляет исключение
FUNCTION Get_curator_id( 
               p_brand_id   IN INTEGER,     -- ID бренда
               p_first_name IN VARCHAR2,    -- имя
               p_last_name  IN VARCHAR2     -- фамилия
           ) RETURN INTEGER                 -- ID менеджера
IS
    v_manager_id INTEGER;
BEGIN
    -- находим менеджера из списка
    SELECT MANAGER_ID
      INTO v_manager_id 
      FROM MANAGER_T
     WHERE CONTRACTOR_ID = p_brand_id
       AND LAST_NAME  = p_last_name
       AND FIRST_NAME = p_first_name
    ;
    RETURN v_manager_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- добавляем менеджера
        v_manager_id := PK02_POID.Next_manager_id;
        INSERT INTO MANAGER_T(
            MANAGER_ID,CONTRACTOR_ID,LAST_NAME,FIRST_NAME,DATE_FROM
        )VALUES(
            v_manager_id, p_brand_id, p_last_name, p_first_name, SYSDATE
        );
        RETURN v_manager_id;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- найти тарифный план
--   - положительное - ID тарифного плана 
--   - NULL - не найден, сообщить администратору
--   - при ошибке выставляет исключение
FUNCTION Get_rateplan_id_by_name( 
               p_rateplan_name IN VARCHAR2 -- имя тарифного плана 
           ) RETURN INTEGER                -- ID тарифного плана
IS
    v_rateplan_id INTEGER;
BEGIN
    SELECT RATEPLAN_ID INTO v_rateplan_id
      FROM RATEPLAN_T
     WHERE RATEPLAN_NAME = p_rateplan_name
       AND RATESYSTEM_ID = Pk00_Const.c_RATESYS_MMTS_ID;
    RETURN v_rateplan_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- создать номер договора клиента, формат: PR xxx xxx,
-- где  PR - префикс, какой договоримся позже
FUNCTION Make_contract_No(p_prefix IN VARCHAR2) RETURN VARCHAR2
IS
BEGIN
    RETURN p_prefix||LPAD(SQ_CONTRACT_NO.NEXTVAL,6,'0');
END;
           
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- создать номер лицевого счета клиента: формат: ACC xxx xxx xxx 
-- вариант вренменный, позже переделаю
FUNCTION Make_account_No RETURN VARCHAR2
IS
BEGIN
    RETURN 'ACC'||TRIM(LPAD(SQ_ACCOUNT_NO.NEXTVAL,9,'0'));
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- Заказы, которые приходят из ОМ, уже имеют присвоенный номер,
-- номер для заказов пришедших из XML, формируем так как предложил А.Ю.Гуров:
-- ACC xxx xxx xxx - nn
-- (формат Нового Биллинга:: YY LD x xxx xxx - не используем)
FUNCTION Make_order_No (p_account_no IN VARCHAR2) RETURN VARCHAR2
IS
    --v_order_id INTEGER;
    v_count      INTEGER;
    v_nn         INTEGER;
    v_order_no   ORDER_T.ORDER_NO%TYPE;
BEGIN
    --v_order_id := PK02_POID.Next_order_id;
    --RETURN 'KH_'||TO_CHAR(SYSDATE,'YY')||'LD'||TRIM(LPAD(v_order_id,7,'0'));
    SELECT COUNT(*) INTO v_nn
      FROM ACCOUNT_T A, ORDER_T O
     WHERE A.ACCOUNT_ID = O.ACCOUNT_ID
       AND A.ACCOUNT_NO = p_account_no;
    LOOP
       v_nn := v_nn + 1;
       -- формируем номер заказа по предложению А.Ю.Гурова
       v_order_no := p_account_no||'-'||TRIM(LPAD(v_nn,2,'0'));
       -- проверяем на уникальность 
       SELECT COUNT(*) INTO v_count 
         FROM ORDER_T O
        WHERE O.ORDER_NO = v_order_no;
       EXIT WHEN v_count = 0;
    END LOOP;
    RETURN v_order_no;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- найти покупателя-юр.лицо по внешнему XML.SUBSCRIBER_ID, возвращает значения
--   - положительное - ID покупателя в биллинге, 
--   - NULL - не надйен, создаем нового
--   - при ошибке выставляет исключение
FUNCTION Find_subscriber_by_xmlid(
               p_xml_subscr_id  IN INTEGER
           ) RETURN INTEGER
IS
    v_customer_id INTEGER;
BEGIN
    SELECT AP.CUSTOMER_ID INTO v_customer_id
      FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP
     WHERE A.EXTERNAL_ID  = p_xml_subscr_id
       AND A.ACCOUNT_TYPE = 'J'--Pk00_Const.c_ACC_TYPE_J
       AND A.ACCOUNT_ID   = AP.ACCOUNT_ID;
    --       
    RETURN v_customer_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;

-- найти покупателя-Юр.лицо по ИНН, КПП, возвращает значения
--   - положительное - ID покупателя в биллинге, 
--   - NULL - не надйен, создаем нового
--   - при ошибке выставляет исключение
FUNCTION Find_customer (
               p_inn        IN VARCHAR2,   -- ИНН
               p_kpp        IN VARCHAR2    -- КПП 
           ) RETURN INTEGER
IS
    v_customer_id INTEGER;
BEGIN
    SELECT C.CUSTOMER_ID INTO v_customer_id
      FROM CUSTOMER_T C  
     WHERE C.INN = p_inn
       AND C.KPP = p_kpp;
    RETURN v_customer_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;
        
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- создать нового покупателя-Юр.лицо, возвращает значения
--   - положительное - ID покупателя, 
--   - при ошибке выставляет исключение
FUNCTION New_customer(
               p_erp_code    IN VARCHAR2,
               p_inn         IN VARCHAR2,
               p_kpp         IN VARCHAR2, 
               p_name        IN VARCHAR2,
               p_short_name  IN VARCHAR2
           ) RETURN INTEGER
IS
BEGIN
    RETURN PK13_CUSTOMER.New_customer(
               p_erp_code,
               p_inn,
               p_kpp, 
               p_name,
               p_short_name,
               NULL
           );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Установить юридический адрес покупателя из ERP, возможно не из XML
-- возвращает значения
--   - положительное - id строки адреса 
--   - при ошибке выставляет исключение
--
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
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- добавить описание клиента для аналитики (возможно не из XML)
--   - положительное - ID клиета, 
--   - при ошибке выставляет исключение
FUNCTION Add_client(
               p_client_name   IN VARCHAR2
           ) RETURN INTEGER
IS
BEGIN
    RETURN PK11_CLIENT.New_client(p_client_name);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- создать лицевой счет клиента
--   - положительное - ID лицевого счета 
--   - при ошибке выставляет исключение
FUNCTION New_account(
               p_xml_subscr_id  IN INTEGER,  -- ID клиента (XML) в удаленной системе
               p_contractor_id  IN INTEGER,  -- ID поставщика услуги, по умолчанию КТТК
               p_customer_id    IN INTEGER,  -- ID клиента в биллинге
               p_brand_id       IN INTEGER,  -- бренд (продавец/агент)
               p_curator_id     IN INTEGER,  -- куратор (менеджер)
               p_rateplan_id    IN INTEGER,  -- тарифный план
               p_date           IN DATE,     -- дата начала действия договора
               p_contract_no   OUT VARCHAR2, -- номер договора
               p_account_no    OUT VARCHAR2, -- номер лицевого счета
               p_order_no      OUT VARCHAR2  -- номер заказа
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'New_account';
    v_account_id    INTEGER;
    v_contract_id   INTEGER;
    v_profile_id    INTEGER;
    v_order_id      INTEGER;
    v_order_body_id INTEGER;
    v_bill_id       INTEGER;
    v_count         INTEGER;
    v_brand_id      INTEGER;
    v_branch_id     INTEGER;
    v_agent_id      INTEGER;
    v_date          DATE := TRUNC(p_date);
BEGIN
    -- определяем бренд и продавца c учетом филиальной структуры ЦСС
    -- переходим от нумерации филиалов ЦСС к внутренним брендам
    v_brand_id := p_brand_id + 33000;
    --
    SELECT C.CONTRACTOR_ID, C.PARENT_ID 
      INTO v_agent_id, v_branch_id
      FROM CONTRACTOR_BRAND_T CB, CONTRACTOR_T C
    WHERE CB.CONTRACTOR_ID = C.CONTRACTOR_ID 
      AND CB.BRAND_ID = v_brand_id;

    -- проверяем на новизну номер лицевого счета
    p_account_no := Make_account_No;
    SELECT COUNT(*) INTO v_count
      FROM ACCOUNT_T A
     WHERE A.ACCOUNT_NO   = p_account_no
       AND A.ACCOUNT_TYPE = PK00_CONST.c_ACC_TYPE_J;
    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
               'Для XML.SUBSCRIBER_ID='||p_xml_subscr_id||
               ', в таблице ACCOUNT_T уже существует запись с ACCOUNT_NO='||p_account_no);
    END IF;
    -- создаем лицевой счет
    v_account_id := Pk05_Account.New_account(
               p_account_no    => p_account_no,
               p_account_type  => PK00_CONST.c_ACC_TYPE_J,
               p_currency_id   => PK00_CONST.c_CURRENCY_RUB,
               p_status        => PK00_CONST.c_ACC_STATUS_BILL,
               p_parent_id     => NULL
           );
    -- создаем договор
    p_contract_no := Make_contract_No(p_xml_subscr_id);
    v_contract_id := PK12_CONTRACT.Open_contract(
               p_contract_no => p_contract_no,
               p_date_from   => v_date,
               p_date_to     => NULL,
               p_client_id   => c_CLIENT_ID,
               p_manager_id  => p_curator_id
           );
    -- создаем профиль лицевого счета
    v_profile_id := PK05_ACCOUNT.Set_profile(
               p_account_id    => v_account_id,
               p_brand_id      => v_brand_id,
               p_contract_id   => v_contract_id,
               p_customer_id   => p_customer_id,
               p_subscriber_id => NULL,
               p_contractor_id => NVL(p_contractor_id, c_CONTRACTOR_KTTK_ID),
               p_branch_id     => v_branch_id,
               p_agent_id      => v_agent_id,
               p_contractor_bank_id => Pk00_Const.c_KTTK_J_BANK_ID,
               p_vat           => Pk00_Const.c_VAT,
               p_date_from     => v_date,
               p_date_to       => NULL
           );
    -- создаем заказ на МГ/МН связь
    p_order_no := pk06_order.Make_order_No(p_account_no => p_account_no);
    v_order_id := PK06_ORDER.New_order(
               p_account_id  => v_account_id,
               p_order_no    => p_order_no,
               p_service_id  => PK00_CONST.c_SERVICE_CALL_MGMN,
               p_rateplan_id => p_rateplan_id,
               p_time_zone   => 4,
               p_date_from   => v_date,
               p_date_to     => NULL
           );
             
    -- создаем строку заказа для МГ
    v_order_body_id := PK06_ORDER.Add_subservice(
               p_order_id      => v_order_id,
               p_subservice_id => PK00_CONST.c_SUBSRV_MG,
               p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
               p_date_from     => v_date,
               p_date_to       => NULL
           );
           
    -- создаем строку заказа для МН
    v_order_body_id := PK06_ORDER.Add_subservice(
               p_order_id      => v_order_id,
               p_subservice_id => PK00_CONST.c_SUBSRV_MN,
               p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
               p_date_from     => v_date,
               p_date_to       => NULL
           );
		-- создаем строку для внутризововой связи
		v_order_body_id := PK06_ORDER.Add_subservice(
               p_order_id      => v_order_id,
               p_subservice_id => PK00_CONST.c_SUBSRV_ZONE,
               p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
							 p_rateplan_id	 => p_rateplan_id,
               p_date_from     => v_date,
               p_date_to       => NULL
           );
    -- создание описателя счетов и самих счетов для нового Л/С
    -- данные периода в таблице PERIOD_T
    PK07_BILL.New_billinfo (
               p_account_id       => v_account_id, -- ID лицевого счета
               p_currency_id      => PK00_CONST.c_CURRENCY_RUB, -- ID валюты счета
               p_delivery_id      => NULL,
               p_days_for_payment => NULL          -- кол-во дней на оплату счета
           );

    RETURN v_account_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- добавить адрес на л/с:
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Add_address(
               p_account_id    IN INTEGER,   -- ID Л/С
               p_address_type  IN VARCHAR2,  -- тип аддреса (см. pk00_const)
               p_country       IN VARCHAR2,  -- 'РФ' - страна, 99.9999999999% случаях РФ
               p_zip           IN VARCHAR2,  -- почтовый индекс
               p_state         IN VARCHAR2,  -- регион (область )
               p_city          IN VARCHAR2,  -- город
               p_address       IN VARCHAR2,  -- адрес в одну строку
               p_person        IN VARCHAR2,  -- ФИО
               p_phones        IN VARCHAR2,  -- контактный телефон, может отличаться от купленного
               p_email         IN VARCHAR2,  
               p_date_from     IN DATE,
               p_date_to       IN DATE DEFAULT NULL
           ) RETURN INTEGER                  -- ID строки адреса
IS
BEGIN
    RETURN PK05_ACCOUNT.Add_address(
                             p_account_id,
                             p_address_type,
                             p_country,
                             p_zip,
                             p_state,
                             p_city,
                             p_address,
                             p_person,
                             p_phones,
                             NULL,             -- факса думаю не будет
                             p_email,
                             p_date_from,
                             p_date_to,
                             NULL
                          );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- добавить телефон на лицевой счет клиента
--   - положительное - ID лицевого счета 
--   - при ошибке выставляет исключение
PROCEDURE Add_phone(
               p_account_id   IN INTEGER,    -- ID лицевого счета
               p_phone        IN VARCHAR2,   -- номер телефона
               p_date_from    IN DATE,       -- дата начала действия
               p_date_to      IN DATE DEFAULT c_MAX_DATE_TO
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_phone';
    v_order_id    INTEGER;
    v_rowid       VARCHAR2(100);
BEGIN
    -- получаем номер заказа, для физиков есть правило: один л/с - один заказ
    SELECT ORDER_ID INTO v_order_id
      FROM ORDER_T
     WHERE ACCOUNT_ID = p_account_id
       AND DATE_TO IS NULL;
    -- добавляем телефон на заказ
    v_rowid := PK18_RESOURCE.Add_phone(
                   p_order_id => v_order_id,
                   p_phone    => p_phone,
                   p_date_from=> p_date_from,
                   p_date_to  => p_date_to
               );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- удалить все данные для покупателя-физ.лицо по внешнему XML.SUBSCRIBER_ID
--
PROCEDURE Delete_xml_subscriber(
               p_xml_subscr_id IN INTEGER
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_xml_subscriber';
    v_account_id  INTEGER;
BEGIN
    SELECT ACCOUNT_ID INTO v_account_id 
      FROM ACCOUNT_T
     WHERE EXTERNAL_ID = p_xml_subscr_id;
    
    FOR r_account_profile IN (
        SELECT AP.ACCOUNT_ID, AP.PROFILE_ID, 
               AP.SUBSCRIBER_ID, AP.CONTRACT_ID, AP.BRANCH_ID
          FROM ACCOUNT_PROFILE_T AP
         WHERE AP.ACCOUNT_ID = v_account_id
      )
    LOOP
        -- удаляем заказы
        FOR r_order IN (
            SELECT O.ORDER_ID 
              FROM ORDER_T O
             WHERE O.ACCOUNT_ID = r_account_profile.account_id
          )
        LOOP
            -- удаляем адреса установки телефонов
            DELETE PHONE_ADDRESS_T WHERE ADDRESS_ID IN (
                SELECT ADDRESS_ID FROM ORDER_PHONES_T WHERE ORDER_ID = r_order.order_id
            );
            -- удаляем телефоны
            DELETE ORDER_PHONES_T WHERE ORDER_ID = r_order.order_id;
            -- удаляем тело заказа
            DELETE FROM ORDER_BODY_T WHERE ORDER_ID = r_order.order_id;
        END LOOP;
        -- удаляем заказы
        DELETE FROM ORDER_T WHERE ACCOUNT_ID = r_account_profile.account_id;
        -- удаляем договор
        DELETE FROM CONTRACT_T WHERE CONTRACT_ID = r_account_profile.contract_id;
        -- удаляем персональные данные абонента
        DELETE FROM SUBSCRIBER_DOC_T WHERE SUBSCRIBER_ID = r_account_profile.subscriber_id;
        DELETE FROM SUBSCRIBER_T WHERE SUBSCRIBER_ID = r_account_profile.subscriber_id;
    END LOOP;
    -- удаляем профили лицевого счета
    DELETE FROM ACCOUNT_PROFILE_T AP WHERE ACCOUNT_ID = v_account_id;
    -- удаляем лицевой счет
    DELETE FROM ACCOUNT_T A WHERE ACCOUNT_ID = v_account_id;
    -- фиксируем удаление
    Pk01_Syslog.Write_msg(p_Msg   => 'Удален ACCOUNT_T.EXTERNAL_ID='||p_xml_subscr_id,
                          p_Src   => c_PkgName||'.'||v_prcName ,
                          p_Level => Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Просмотр сообщений для указанной функции в системе логирования 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Msg_list(
              p_recordset OUT t_refc,
              p_function   IN VARCHAR2,                   -- имя функции
              p_date_from  IN DATE DEFAULT (SYSDATE-30)   -- время старта функции (ориентировочное)
           )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Msg_list';
    v_ssid    INTEGER; 
    v_id_from INTEGER; 
    v_id_to   INTEGER;
    v_retcode INTEGER;
BEGIN
    -- подготовка данных
    SELECT MAX(SSID), MIN(L01_ID), MAX(L01_ID) 
      INTO v_ssid, v_id_from, v_id_to
      FROM L01_MESSAGES
     WHERE MSG_SRC LIKE c_PkgName||'.'||p_function||'%' 
       AND p_date_from < MSG_DATE 
       AND (MESSAGE LIKE 'Start%' OR MESSAGE LIKE 'Stop%');  
    -- отображение
    IF v_id_from IS NULL THEN
        OPEN p_recordset FOR
            SELECT 0, 'E', SYSDATE, 'Не найдена строка: "Start" ', TO_CHAR(NULL) 
              FROM DUAL;
    ELSIF v_id_from = v_id_to THEN -- Не найдена строка стоп (процесс еще продолжается)
        -- возвращаем курсор на данные сеанса, начиная от момента старта функции 
        OPEN p_recordset FOR
            SELECT L01_ID, MSG_LEVEL, MSG_DATE, MESSAGE, APP_USER 
              FROM L01_MESSAGES
             WHERE SSID = v_ssid
               AND L01_ID >= v_id_from
             ORDER BY L01_ID;
    ELSE
        -- возвращаем курсор на данные диапазона времени, когда работала ф-ия 
        OPEN p_recordset FOR
            SELECT L01_ID, MSG_LEVEL, MSG_DATE, MESSAGE, APP_USER 
              FROM L01_MESSAGES
             WHERE SSID = v_ssid
               AND L01_ID BETWEEN v_id_from AND v_id_to
             ORDER BY L01_ID;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


END PK105_CUSTOMER_J_XML;
/
