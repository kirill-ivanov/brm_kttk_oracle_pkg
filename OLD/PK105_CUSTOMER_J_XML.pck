CREATE OR REPLACE PACKAGE PK105_CUSTOMER_J_XML
IS
    --
    -- ����� ��� ������ ��������� �� ������ ��� ���������� ���
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK105_CUSTOMER_J_XML';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- ������������ ��������, ������� ������������� � ���� DATE_TO ������ NULL
    c_MAX_DATE_TO constant date := TO_DATE('01.01.2050','dd.mm.yyyy');
    -- ID �������� ��� ����������� ���� CONTRACTOR_T (�������� ���� �� ����)
    c_CONTRACTOR_KTTK_ID constant integer := 1;
    -- ID ������� ��� ��� ��� (���� �� ����)
    c_CLIENT_ID constant integer := 1;
    -- ID ��������� ��� ����������� ���� MANAGER_T (���� ���� �� ����)
    c_MANAGER_SIEBEL_ID  constant integer := 1;
    -- ID ��������� ��� ����������� ���� MANAGER_T ���
    c_MANAGER_CSS_ID  constant integer := 2;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- ������������ XML-���� � ������� ��������
    -- ���������� XML_SUBS_FILES_T.XML_FILE_ID
    FUNCTION Load_start(
                 p_filename IN VARCHAR2
             ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- ��������� ����� � �������� XML-�����
    -- - ��� ������ ���������� ����������
    PROCEDURE Load_stop(
                 p_xml_file_id IN INTEGER,
                 p_state       IN XML_SUBS_FILES_T.STATE%TYPE,
                 p_records     IN INTEGER,
                 p_ok          IN INTEGER,
                 p_err         IN INTEGER,
                 p_notes       IN XML_SUBS_FILES_T.NOTES%TYPE
             );

		-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
		-- �������� ���������� ���������
		--   - ������������� - ID ������ 
		--   - NULL - �� �����, �������� ��������������
		--   - ��� ������ ���������� ����������
		procedure Add_Bank_Data(
			p_customer_id			in number,
			p_bank_name				in varchar2,
			p_bank_code				in varchar2,
			p_corr_account		in varchar2,
			p_settl_account		in varchar2
			);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- ����� ID ������
    --   - ������������� - ID ������ 
    --   - NULL - �� �����, �������� ��������������
    --   - ��� ������ ���������� ����������
    FUNCTION Get_brand_id( 
                   p_brand IN VARCHAR2        -- ��� ����������� 
               ) RETURN INTEGER;              -- ID �����������

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- ����� ��������, ���� �� ����� - �������
    --   - ������������� - ID �������� 
    --   - ��� ������ ���������� ����������
    FUNCTION Get_curator_id( 
                   p_brand_id   IN INTEGER,   -- ID ������
                   p_first_name IN VARCHAR2,  -- ���
                   p_last_name  IN VARCHAR2   -- �������
               ) RETURN INTEGER;              -- ID ���������

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- ����� �������� ����
    --   - ������������� - ID ��������� ����� 
    --   - NULL - �� ������, �������� ��������������
    --   - ��� ������ ���������� ����������
    FUNCTION Get_rateplan_id_by_name( 
                   p_rateplan_name IN VARCHAR2 -- ��� ��������� ����� 
               ) RETURN INTEGER;               -- ID ��������� �����

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- ������� ������ ����������-���.����, ���������� ��������
    --   - ������������� - ID ���������� � ��������, 
    --   - NULL - �� ������, ������� ������
    --   - ��� ������ ���������� ����������
    FUNCTION Find_subscriber_by_xmlid(
                   p_xml_subscr_id  IN INTEGER
               ) RETURN INTEGER;                          

    -- ����� ����������-��.���� �� ���, ���, ���������� ��������
    --   - ������������� - ID ���������� � ��������, 
    --   - NULL - �� ������, ������� ������
    --   - ��� ������ ���������� ����������
    FUNCTION Find_customer (
                   p_inn        IN VARCHAR2,   -- ���
                   p_kpp        IN VARCHAR2    -- ��� 
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- ������� ������ ����������-��.����, ���������� ��������
    --   - ������������� - ID ����������, 
    --   - ��� ������ ���������� ����������
    FUNCTION New_customer(
                   p_erp_code    IN VARCHAR2,
                   p_inn         IN VARCHAR2,
                   p_kpp         IN VARCHAR2, 
                   p_name        IN VARCHAR2,
                   p_short_name  IN VARCHAR2
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- ������� ������� ���� �������
    --   - ������������� - ID �������� ����� 
    --   - ��� ������ ���������� ����������
    FUNCTION New_account(
                   p_xml_subscr_id  IN INTEGER,  -- ID ������� (XML) � ��������� �������
                   p_contractor_id  IN INTEGER,  -- ID ���������� ������, �� ��������� ����
                   p_customer_id    IN INTEGER,  -- ID ������� � ��������
                   p_brand_id       IN INTEGER,  -- ����� (��������/�����)
                   p_curator_id     IN INTEGER,  -- ������� (��������)
                   p_rateplan_id    IN INTEGER,  -- �������� ����
                   p_date           IN DATE,     -- ���� ������ �������� ��������
                   p_contract_no   OUT VARCHAR2, -- ����� ��������
                   p_account_no    OUT VARCHAR2, -- ����� �������� �����
                   p_order_no      OUT VARCHAR2  -- ����� ������
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ����� �� �/�:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Add_address(
                   p_account_id    IN INTEGER,   -- ID �/�
                   p_address_type  IN VARCHAR2,  -- ��� ������� (��. pk00_const)
                   p_country       IN VARCHAR2,  -- '��' - ������, 99.9999999999% ������� ��
                   p_zip           IN VARCHAR2,  -- �������� ������
                   p_state         IN VARCHAR2,  -- ������ (������� )
                   p_city          IN VARCHAR2,  -- �����
                   p_address       IN VARCHAR2,  -- ����� � ���� ������
                   p_person        IN VARCHAR2,  -- ���
                   p_phones        IN VARCHAR2,  -- ���������� �������, ����� ���������� �� ����������
                   p_email         IN VARCHAR2,  
                   p_date_from     IN DATE,
                   p_date_to       IN DATE DEFAULT NULL
               ) RETURN INTEGER;                 -- ID ������ ������

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- �������� ������� �� ������� ���� �������
    --   - ������������� - ID �������� ����� 
    --   - ��� ������ ���������� ����������
    PROCEDURE Add_phone(
                   p_account_id   IN INTEGER,    -- ID �������� �����
                   p_phone        IN VARCHAR2,   -- ����� ��������
                   p_date_from    IN DATE,       -- ���� ������ ��������
                   p_date_to      IN DATE DEFAULT c_MAX_DATE_TO
               );

               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ��� ������ ��� ����������-���.���� �� �������� XML.SUBSCRIBER_ID
    PROCEDURE Delete_xml_subscriber(
                   p_xml_subscr_id IN INTEGER
               );
		
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ��������� ��� ��������� ������� � ������� ����������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Msg_list(
                  p_recordset OUT t_refc,
                  p_function   IN VARCHAR2,                   -- ��� �������
                  p_date_from  IN DATE DEFAULT (SYSDATE-30)   -- ����� ������ ������� (���������������)
               );
    
    
END PK105_CUSTOMER_J_XML;
/
CREATE OR REPLACE PACKAGE BODY PK105_CUSTOMER_J_XML
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- ������������ XML-���� � ������� ��������
-- ���������� XML_SUBS_FILES_T.XML_FILE_ID
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
-- ��������� ����� � �������� XML-�����
-- - ��� ������ ���������� ����������
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
-- �������� ���������� ���������
--   - ������������� - ID ������ 
--   - NULL - �� �����, �������� ��������������
--   - ��� ������ ���������� ����������
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
	-- ��������� ������� ����������
	begin
		select customer_id into v_cnt
		from customer_bank_t
		where customer_id = p_customer_id;
		-- ��������
		update customer_bank_t
		set bank_name = p_bank_name, 
				bank_code = p_bank_code, 
				bank_corr_account = p_corr_account, 
				bank_settlement = p_settl_account
		where customer_id = p_customer_id;
	exception when no_data_found then
		-- ��� ������
			insert into customer_bank_t
				(customer_id, bank_name, bank_code, bank_corr_account, bank_settlement)
			values(p_customer_id, p_bank_name, p_bank_code, p_corr_account, p_settl_account);
	end;
exception when others then
	Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
end;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- ����� ID ������
--   - ������������� - ID ������ 
--   - NULL - �� �����, �������� ��������������
--   - ��� ������ ���������� ����������
FUNCTION Get_brand_id( 
               p_brand IN VARCHAR2   -- ��� ����������� 
           ) RETURN INTEGER          -- ID �����������
IS
    v_brand_id INTEGER;
BEGIN
    -- ����� ������ �� ������ �����, ���� �� �����, ������ ������� �����
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
-- ����� ��������, ���� �� ����� - �������
--   - ������������� - ID �������� 
--   - ��� ������ ���������� ����������
FUNCTION Get_curator_id( 
               p_brand_id   IN INTEGER,     -- ID ������
               p_first_name IN VARCHAR2,    -- ���
               p_last_name  IN VARCHAR2     -- �������
           ) RETURN INTEGER                 -- ID ���������
IS
    v_manager_id INTEGER;
BEGIN
    -- ������� ��������� �� ������
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
        -- ��������� ���������
        v_manager_id := PK02_POID.Next_manager_id;
        INSERT INTO MANAGER_T(
            MANAGER_ID,CONTRACTOR_ID,LAST_NAME,FIRST_NAME,DATE_FROM
        )VALUES(
            v_manager_id, p_brand_id, p_last_name, p_first_name, SYSDATE
        );
        RETURN v_manager_id;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- ����� �������� ����
--   - ������������� - ID ��������� ����� 
--   - NULL - �� ������, �������� ��������������
--   - ��� ������ ���������� ����������
FUNCTION Get_rateplan_id_by_name( 
               p_rateplan_name IN VARCHAR2 -- ��� ��������� ����� 
           ) RETURN INTEGER                -- ID ��������� �����
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
-- ������� ����� �������� �������, ������: PR xxx xxx,
-- ���  PR - �������, ����� ����������� �����
FUNCTION Make_contract_No(p_prefix IN VARCHAR2) RETURN VARCHAR2
IS
BEGIN
    RETURN p_prefix||LPAD(SQ_CONTRACT_NO.NEXTVAL,6,'0');
END;
           
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- ������� ����� �������� ����� �������: ������: ACC xxx xxx xxx 
-- ������� ����������, ����� ���������
FUNCTION Make_account_No RETURN VARCHAR2
IS
BEGIN
    RETURN 'ACC'||TRIM(LPAD(SQ_ACCOUNT_NO.NEXTVAL,9,'0'));
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- ������, ������� �������� �� ��, ��� ����� ����������� �����,
-- ����� ��� ������� ��������� �� XML, ��������� ��� ��� ��������� �.�.�����:
-- ACC xxx xxx xxx - nn
-- (������ ������ ��������:: YY LD x xxx xxx - �� ����������)
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
       -- ��������� ����� ������ �� ����������� �.�.������
       v_order_no := p_account_no||'-'||TRIM(LPAD(v_nn,2,'0'));
       -- ��������� �� ������������ 
       SELECT COUNT(*) INTO v_count 
         FROM ORDER_T O
        WHERE O.ORDER_NO = v_order_no;
       EXIT WHEN v_count = 0;
    END LOOP;
    RETURN v_order_no;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- ����� ����������-��.���� �� �������� XML.SUBSCRIBER_ID, ���������� ��������
--   - ������������� - ID ���������� � ��������, 
--   - NULL - �� ������, ������� ������
--   - ��� ������ ���������� ����������
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

-- ����� ����������-��.���� �� ���, ���, ���������� ��������
--   - ������������� - ID ���������� � ��������, 
--   - NULL - �� ������, ������� ������
--   - ��� ������ ���������� ����������
FUNCTION Find_customer (
               p_inn        IN VARCHAR2,   -- ���
               p_kpp        IN VARCHAR2    -- ��� 
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
-- ������� ������ ����������-��.����, ���������� ��������
--   - ������������� - ID ����������, 
--   - ��� ������ ���������� ����������
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
-- ���������� ����������� ����� ���������� �� ERP, �������� �� �� XML
-- ���������� ��������
--   - ������������� - id ������ ������ 
--   - ��� ������ ���������� ����������
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
-- �������� �������� ������� ��� ��������� (�������� �� �� XML)
--   - ������������� - ID ������, 
--   - ��� ������ ���������� ����������
FUNCTION Add_client(
               p_client_name   IN VARCHAR2
           ) RETURN INTEGER
IS
BEGIN
    RETURN PK11_CLIENT.New_client(p_client_name);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- ������� ������� ���� �������
--   - ������������� - ID �������� ����� 
--   - ��� ������ ���������� ����������
FUNCTION New_account(
               p_xml_subscr_id  IN INTEGER,  -- ID ������� (XML) � ��������� �������
               p_contractor_id  IN INTEGER,  -- ID ���������� ������, �� ��������� ����
               p_customer_id    IN INTEGER,  -- ID ������� � ��������
               p_brand_id       IN INTEGER,  -- ����� (��������/�����)
               p_curator_id     IN INTEGER,  -- ������� (��������)
               p_rateplan_id    IN INTEGER,  -- �������� ����
               p_date           IN DATE,     -- ���� ������ �������� ��������
               p_contract_no   OUT VARCHAR2, -- ����� ��������
               p_account_no    OUT VARCHAR2, -- ����� �������� �����
               p_order_no      OUT VARCHAR2  -- ����� ������
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
    -- ���������� ����� � �������� c ������ ���������� ��������� ���
    -- ��������� �� ��������� �������� ��� � ���������� �������
    v_brand_id := p_brand_id + 33000;
    --
    SELECT C.CONTRACTOR_ID, C.PARENT_ID 
      INTO v_agent_id, v_branch_id
      FROM CONTRACTOR_BRAND_T CB, CONTRACTOR_T C
    WHERE CB.CONTRACTOR_ID = C.CONTRACTOR_ID 
      AND CB.BRAND_ID = v_brand_id;

    -- ��������� �� ������� ����� �������� �����
    p_account_no := Make_account_No;
    SELECT COUNT(*) INTO v_count
      FROM ACCOUNT_T A
     WHERE A.ACCOUNT_NO   = p_account_no
       AND A.ACCOUNT_TYPE = PK00_CONST.c_ACC_TYPE_J;
    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
               '��� XML.SUBSCRIBER_ID='||p_xml_subscr_id||
               ', � ������� ACCOUNT_T ��� ���������� ������ � ACCOUNT_NO='||p_account_no);
    END IF;
    -- ������� ������� ����
    v_account_id := Pk05_Account.New_account(
               p_account_no    => p_account_no,
               p_account_type  => PK00_CONST.c_ACC_TYPE_J,
               p_currency_id   => PK00_CONST.c_CURRENCY_RUB,
               p_status        => PK00_CONST.c_ACC_STATUS_BILL,
               p_parent_id     => NULL
           );
    -- ������� �������
    p_contract_no := Make_contract_No(p_xml_subscr_id);
    v_contract_id := PK12_CONTRACT.Open_contract(
               p_contract_no => p_contract_no,
               p_date_from   => v_date,
               p_date_to     => NULL,
               p_client_id   => c_CLIENT_ID,
               p_manager_id  => p_curator_id
           );
    -- ������� ������� �������� �����
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
    -- ������� ����� �� ��/�� �����
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
             
    -- ������� ������ ������ ��� ��
    v_order_body_id := PK06_ORDER.Add_subservice(
               p_order_id      => v_order_id,
               p_subservice_id => PK00_CONST.c_SUBSRV_MG,
               p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
               p_date_from     => v_date,
               p_date_to       => NULL
           );
           
    -- ������� ������ ������ ��� ��
    v_order_body_id := PK06_ORDER.Add_subservice(
               p_order_id      => v_order_id,
               p_subservice_id => PK00_CONST.c_SUBSRV_MN,
               p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
               p_date_from     => v_date,
               p_date_to       => NULL
           );
		-- ������� ������ ��� ������������� �����
		v_order_body_id := PK06_ORDER.Add_subservice(
               p_order_id      => v_order_id,
               p_subservice_id => PK00_CONST.c_SUBSRV_ZONE,
               p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
							 p_rateplan_id	 => p_rateplan_id,
               p_date_from     => v_date,
               p_date_to       => NULL
           );
    -- �������� ��������� ������ � ����� ������ ��� ������ �/�
    -- ������ ������� � ������� PERIOD_T
    PK07_BILL.New_billinfo (
               p_account_id       => v_account_id, -- ID �������� �����
               p_currency_id      => PK00_CONST.c_CURRENCY_RUB, -- ID ������ �����
               p_delivery_id      => NULL,
               p_days_for_payment => NULL          -- ���-�� ���� �� ������ �����
           );

    RETURN v_account_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ����� �� �/�:
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Add_address(
               p_account_id    IN INTEGER,   -- ID �/�
               p_address_type  IN VARCHAR2,  -- ��� ������� (��. pk00_const)
               p_country       IN VARCHAR2,  -- '��' - ������, 99.9999999999% ������� ��
               p_zip           IN VARCHAR2,  -- �������� ������
               p_state         IN VARCHAR2,  -- ������ (������� )
               p_city          IN VARCHAR2,  -- �����
               p_address       IN VARCHAR2,  -- ����� � ���� ������
               p_person        IN VARCHAR2,  -- ���
               p_phones        IN VARCHAR2,  -- ���������� �������, ����� ���������� �� ����������
               p_email         IN VARCHAR2,  
               p_date_from     IN DATE,
               p_date_to       IN DATE DEFAULT NULL
           ) RETURN INTEGER                  -- ID ������ ������
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
                             NULL,             -- ����� ����� �� �����
                             p_email,
                             p_date_from,
                             p_date_to,
                             NULL
                          );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- �������� ������� �� ������� ���� �������
--   - ������������� - ID �������� ����� 
--   - ��� ������ ���������� ����������
PROCEDURE Add_phone(
               p_account_id   IN INTEGER,    -- ID �������� �����
               p_phone        IN VARCHAR2,   -- ����� ��������
               p_date_from    IN DATE,       -- ���� ������ ��������
               p_date_to      IN DATE DEFAULT c_MAX_DATE_TO
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_phone';
    v_order_id    INTEGER;
    v_rowid       VARCHAR2(100);
BEGIN
    -- �������� ����� ������, ��� ������� ���� �������: ���� �/� - ���� �����
    SELECT ORDER_ID INTO v_order_id
      FROM ORDER_T
     WHERE ACCOUNT_ID = p_account_id
       AND DATE_TO IS NULL;
    -- ��������� ������� �� �����
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
-- ������� ��� ������ ��� ����������-���.���� �� �������� XML.SUBSCRIBER_ID
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
        -- ������� ������
        FOR r_order IN (
            SELECT O.ORDER_ID 
              FROM ORDER_T O
             WHERE O.ACCOUNT_ID = r_account_profile.account_id
          )
        LOOP
            -- ������� ������ ��������� ���������
            DELETE PHONE_ADDRESS_T WHERE ADDRESS_ID IN (
                SELECT ADDRESS_ID FROM ORDER_PHONES_T WHERE ORDER_ID = r_order.order_id
            );
            -- ������� ��������
            DELETE ORDER_PHONES_T WHERE ORDER_ID = r_order.order_id;
            -- ������� ���� ������
            DELETE FROM ORDER_BODY_T WHERE ORDER_ID = r_order.order_id;
        END LOOP;
        -- ������� ������
        DELETE FROM ORDER_T WHERE ACCOUNT_ID = r_account_profile.account_id;
        -- ������� �������
        DELETE FROM CONTRACT_T WHERE CONTRACT_ID = r_account_profile.contract_id;
        -- ������� ������������ ������ ��������
        DELETE FROM SUBSCRIBER_DOC_T WHERE SUBSCRIBER_ID = r_account_profile.subscriber_id;
        DELETE FROM SUBSCRIBER_T WHERE SUBSCRIBER_ID = r_account_profile.subscriber_id;
    END LOOP;
    -- ������� ������� �������� �����
    DELETE FROM ACCOUNT_PROFILE_T AP WHERE ACCOUNT_ID = v_account_id;
    -- ������� ������� ����
    DELETE FROM ACCOUNT_T A WHERE ACCOUNT_ID = v_account_id;
    -- ��������� ��������
    Pk01_Syslog.Write_msg(p_Msg   => '������ ACCOUNT_T.EXTERNAL_ID='||p_xml_subscr_id,
                          p_Src   => c_PkgName||'.'||v_prcName ,
                          p_Level => Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ��������� ��� ��������� ������� � ������� ����������� 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Msg_list(
              p_recordset OUT t_refc,
              p_function   IN VARCHAR2,                   -- ��� �������
              p_date_from  IN DATE DEFAULT (SYSDATE-30)   -- ����� ������ ������� (���������������)
           )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Msg_list';
    v_ssid    INTEGER; 
    v_id_from INTEGER; 
    v_id_to   INTEGER;
    v_retcode INTEGER;
BEGIN
    -- ���������� ������
    SELECT MAX(SSID), MIN(L01_ID), MAX(L01_ID) 
      INTO v_ssid, v_id_from, v_id_to
      FROM L01_MESSAGES
     WHERE MSG_SRC LIKE c_PkgName||'.'||p_function||'%' 
       AND p_date_from < MSG_DATE 
       AND (MESSAGE LIKE 'Start%' OR MESSAGE LIKE 'Stop%');  
    -- �����������
    IF v_id_from IS NULL THEN
        OPEN p_recordset FOR
            SELECT 0, 'E', SYSDATE, '�� ������� ������: "Start" ', TO_CHAR(NULL) 
              FROM DUAL;
    ELSIF v_id_from = v_id_to THEN -- �� ������� ������ ���� (������� ��� ������������)
        -- ���������� ������ �� ������ ������, ������� �� ������� ������ ������� 
        OPEN p_recordset FOR
            SELECT L01_ID, MSG_LEVEL, MSG_DATE, MESSAGE, APP_USER 
              FROM L01_MESSAGES
             WHERE SSID = v_ssid
               AND L01_ID >= v_id_from
             ORDER BY L01_ID;
    ELSE
        -- ���������� ������ �� ������ ��������� �������, ����� �������� �-�� 
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
