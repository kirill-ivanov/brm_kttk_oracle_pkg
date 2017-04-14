CREATE OR REPLACE PACKAGE PK103_PERSON_XML
IS
    --
    -- ����� ��� ������ ��������� �� ������ ��� ���������� ���
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK103_PERSON_XML';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- ID �������� ��� ����������� ���� CONTRACTOR_T (�������� ���� �� ����)
    c_CONTRACTOR_KTTK_ID constant integer := 1;
    -- ID ������� ��� ��� ��� (���� �� ����)
    c_CLIENT_ID constant integer := 1;
    -- ID ��������� ��� ����������� ���� MANAGER_T (���� ���� �� ����)
    c_MANAGER_SIEBEL_ID  constant integer := 1;
    -- ID ��������� ��� ����������� ���� MANAGER_T ���
    c_MANAGER_CSS_ID  constant integer := 2;
	
	-- -------------------------------------------------------------------
	-- ���������� �������
	procedure Update_Address(
		p_client_id    	IN INTEGER,   -- ID �/�
	   	p_client_type	in number,
		p_address_type  IN VARCHAR2,  -- ��� ������� (��. pk00_const)
		p_country       IN VARCHAR2,  -- '��' - ������, 99.9999999999% ������� ��
		p_zip           IN VARCHAR2,  -- �������� ������
		p_state         IN VARCHAR2,  -- ������ (������� )
		p_city          IN VARCHAR2,  -- �����
		p_address       IN VARCHAR2,  -- ����� � ���� ������
		p_person        IN VARCHAR2,  -- ���
		p_phones        IN VARCHAR2,  -- ���������� �������, ����� ���������� �� ����������
		p_email         IN VARCHAR2
		);	
	-- -------------------------------------------------------------------
	-- ���������� ��������
	procedure Update_Curator(
		p_subscriber_id		in number,
		p_region_id			in number,
		p_fst_name			in varchar2,
		p_lst_name			in varchar2
		);

	-- -------------------------------------------------------------------
	-- ���������� ���������� ������ ��������
	procedure Update_Order_Passport(
		p_subscriber_id		in number,
		p_serial			in number,
		p_no				in number,
		p_issuer			in varchar2,
		p_issue_date		in date
		) ;

	-- -------------------------------------------------------------------
	-- ���������� ������ ��������
	procedure Update_Order_Personal(
		p_subscriber_id		in number,
		p_phone				in varchar2,
		p_last_name			in varchar2,
		p_first_name		in varchar2,
		p_middle_name		in varchar2
		) ;
	-- -------------------------------------------------------------------
	-- ��� ������� update �������� subscriber_id/customer_id
	function Update_Get_Client_ID(
		p_xml_subsriber_id	in number,
		p_cl_type			in number,
		p_phone				in varchar2
		) return number;
	
	-- -------------------------------------------------------------------
	-- ��� ������� delete �������� ������ ������
	function Delete_Get_Order_ID(
		p_xml_subsriber_id	in number,
		p_phone				in varchar2,
		p_res				out varchar2
		) return number;

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
               p_region_id   IN INTEGER,     -- ID ������
               p_first_name IN VARCHAR2,    -- ���
               p_last_name  IN VARCHAR2     -- �������
           ) RETURN INTEGER;                 -- ID ���������

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

    -- ����� ����������-���.���� �� �������� XML.SUBSCRIBER_ID, ���������� ��������
    --   - ������������� - ID ���������� � ��������, 
    --   - NULL - �� ������, ������� ������
    --   - ��� ������ ���������� ����������
    FUNCTION Find_subscriber_by_fio (
                   p_last_name   IN VARCHAR2,   -- �������
                   p_first_name  IN VARCHAR2,   -- ��� 
                   p_middle_name IN VARCHAR2,   -- ��������
                   p_doc_serial  IN VARCHAR2,   -- ����� ���������
                   p_doc_no      IN VARCHAR2    -- ����� ���������
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- ������� ������ ����������-���.����, ���������� ��������
    --   - ������������� - ID ����������, 
    --   - ��� ������ ���������� ����������
    FUNCTION New_subscriber(
                   p_last_name   IN VARCHAR2,   -- �������
                   p_first_name  IN VARCHAR2,   -- ��� 
                   p_middle_name IN VARCHAR2,   -- ��������
                   p_category    IN INTEGER DEFAULT Pk00_Const.c_SUBS_RESIDENT  -- ��������� 1/2 = ��������/����������
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� �������� �������
    --   - ��� ������ ���������� ����������
    PROCEDURE Add_document(
                   p_subscriber_id  IN INTEGER,
                   p_doc_type       IN VARCHAR2, -- ��� ���������
                   p_doc_serial     IN VARCHAR2, -- ����� ���������
                   p_doc_no         IN VARCHAR2, -- ����� ���������
                   p_doc_issuer     IN VARCHAR2, -- ��� ����� ��������
                   p_doc_issue_date IN DATE      -- ���� ������ ���������
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- ������� ������� ���� �������
    --   - ������������� - ID �������� ����� 
    --   - ��� ������ ���������� ����������
    FUNCTION New_account(
                   p_xml_subscr_id  IN INTEGER,  -- ID ������� (XML) � ��������� �������
                   p_subscriber_id  IN INTEGER,  -- ID ������� � ��������
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
    FUNCTION Add_phone(
               p_account_id   IN INTEGER,    -- ID �������� �����
               p_phone        IN VARCHAR2,   -- ����� ��������
               p_date_from    IN DATE        -- ���� ������ ��������
           ) return varchar2;

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
    
    
END PK103_PERSON_XML;
/
CREATE OR REPLACE PACKAGE BODY PK103_PERSON_XML
IS

	c_CSS_BRAND		constant number := 33;

-- ------------------------------------------------------------------
-- �������� accountId
function Get_AccountId(
	p_client_id		in number,
	p_client_type	in number
	) return number is
	ret		number;
begin
	if p_client_type = 0 then
		select a.account_id
			into ret
		from account_profile_t a
		where a.subscriber_id = p_client_id;
	else
		select a.account_id
			into ret
		from account_profile_t a
		where a.customer_id = p_client_id;
	end if;
	return ret;
end;
-- -------------------------------------------------------------------
-- ���������� �������
procedure Update_Address(
	   p_client_id    	IN INTEGER,   -- ID �/�
	   p_client_type	in number,
	   p_address_type  IN VARCHAR2,  -- ��� ������� (��. pk00_const)
	   p_country       IN VARCHAR2,  -- '��' - ������, 99.9999999999% ������� ��
	   p_zip           IN VARCHAR2,  -- �������� ������
	   p_state         IN VARCHAR2,  -- ������ (������� )
	   p_city          IN VARCHAR2,  -- �����
	   p_address       IN VARCHAR2,  -- ����� � ���� ������
	   p_person        IN VARCHAR2,  -- ���
	   p_phones        IN VARCHAR2,  -- ���������� �������, ����� ���������� �� ����������
	   p_email         IN VARCHAR2
	) is
	v_prcName CONSTANT VARCHAR2(40) := 'Update_Address';
	v_account_id 	number := Get_AccountId(p_client_id, p_client_type);
begin
 -- ��������� ����� ������
    update ACCOUNT_CONTACT_T 
	set
		COUNTRY = p_country,
		ZIP = p_zip,
		STATE = p_state,
		CITY = p_city,
		ADDRESS = p_address,
		PERSON = p_person,
        PHONES = p_phones,
		EMAIL = p_email
	where account_id = v_account_id
		and address_type = p_address_type
		and nvl(DATE_TO, sysdate+1) > sysdate;
exception when others then
	Pk01_Syslog.raise_Exception(
		'������ ���������� ������.' || sqlerrm, c_PkgName||'.'||v_prcName );
end;
-- -------------------------------------------------------------------
-- ���������� ��������
procedure Update_Curator(
	p_subscriber_id		in number,
	p_region_id			in number,
	p_fst_name			in varchar2,
	p_lst_name			in varchar2
	) is
	v_man_id	number;
	v_prcName CONSTANT VARCHAR2(30) := 'Update_Curator';
begin
	-- ���������� ���� �� ����� ��������
	v_man_id := Get_curator_id(p_region_id, p_fst_name, p_lst_name);
	update sale_curator_t s
	set s.manager_id = v_man_id
	where exists(
		select 1 from account_profile_t a 
		where subscriber_id = p_subscriber_id
			and s.contract_id = a.contract_id
		);
exception when others then
	Pk01_Syslog.raise_Exception(
		'������ ���������� ��������.' || sqlerrm, c_PkgName||'.'||v_prcName );
end;
-- -------------------------------------------------------------------
-- ���������� ���������� ������ ��������
procedure Update_Order_Passport(
	p_subscriber_id		in number,
	p_serial			in number,
	p_no				in number,
	p_issuer			in varchar2,
	p_issue_date		in date
	) is
	v_order_id			number;
	v_subscriber_id		number;
	v_prcName CONSTANT VARCHAR2(30) := 'Update_Order_Passport';
begin
	--
	update subscriber_doc_t s
	set 
		DOC_SERIAL = nvl(p_serial, doc_serial),
		DOC_NO = nvl(p_no, s.doc_no),
		DOC_ISSUER = nvl(p_issuer, s.doc_issuer),
		DOC_ISSUE_DATE = nvl(p_issue_date, s.doc_issue_date)
	where s.subscriber_id = p_subscriber_id;
	--
exception when others then
	Pk01_Syslog.raise_Exception('������ ���������� ���������� ������. ' || sqlerrm, c_PkgName||'.'||v_prcName );
end;

-- -------------------------------------------------------------------
-- ���������� ������ ��������
procedure Update_Order_Personal(
	p_subscriber_id		in number,
	p_phone				in varchar2,
	p_last_name			in varchar2,
	p_first_name		in varchar2,
	p_middle_name		in varchar2
	)  is
	v_prcName CONSTANT VARCHAR2(40) := 'Update_Order_Personal';
begin
	update subscriber_t s
	set s.last_name = nvl(p_last_name, s.last_name), 
		s.first_name = nvl(p_first_name, s.first_name),
		s.middle_name = nvl(p_middle_name, s.middle_name)
	where s.subscriber_id = p_subscriber_id;
exception when others then
	Pk01_Syslog.raise_Exception('������ ���������� ������ ��������. ' || sqlerrm, c_PkgName||'.'||v_prcName );
end;

-- -------------------------------------------------------------------
-- ��� ������� update �������� subscriber_id/customer_id
function Update_Get_Client_ID(
	p_xml_subsriber_id	in number,
	p_cl_type			in number,
	p_phone				in varchar2
	) return number is
	v_cl_id		number;
	v_order_id			number;
	v_prcName CONSTANT VARCHAR2(40) := 'Update_Get_Subsriber_ID';
	p_res		varchar2(1024);
begin
	--
	v_order_id := Delete_Get_Order_ID(p_xml_subsriber_id, p_phone, p_res);
	if p_res != 'OK' then
		Pk01_Syslog.raise_Exception('������ ��������� OrderId. ' || p_res, c_PkgName||'.'||v_prcName );
	end if;
	--
	if p_cl_type = 0 then
		select subscriber_id 
			into v_cl_id
		from account_profile_t a, order_t o
		where o.order_id = v_order_id
			and a.account_id = o.account_id;
	else
		select a.customer_id 
			into v_cl_id
		from account_profile_t a, order_t o
		where o.order_id = v_order_id
			and a.account_id = o.account_id;
	end if;
	return v_cl_id;
exception when others then
	Pk01_Syslog.raise_Exception('������ ��������� SubscriberId. ' || sqlerrm, c_PkgName||'.'||v_prcName );
end;

-- -------------------------------------------------------------------
-- ��� ������� delete �������� ������ ������
function Delete_Get_Order_ID(
	p_xml_subsriber_id	in number,
	p_phone				in varchar2,
	p_res				out varchar2
	) return number is
	p_account_id	number;
	p_order_id		number;
begin
	p_res := 'OK';
	--
	begin
		select account_id 
			into p_account_id
		from account_t a
		where a.external_id = p_xml_subsriber_id;
	exception 
		when no_data_found then
			if p_phone is not null then
				-- ����������� ����� �� ��������
				begin
					select order_id
						into p_order_id
					from order_phones_t p
					where p.phone_number=p_phone
						and sysdate between p.date_from and nvl(p.date_to, sysdate+1);
					return p_order_id;
				exception when no_data_found then
					p_res := '������� �� ������';
					return null;
				end;
			else
				p_res := '������� �� ������';
				return null;
			end if;
		when too_many_rows then
			p_res := '���������� ��������� ������� ������ ��� Subscriber_id=' || p_xml_subsriber_id;
			return null;
	end;
	-- �������� ����� ������
	begin
		select order_id 
			into p_order_id 
		from order_t o
		where o.account_id = p_account_id;
		--
		return p_order_id;
	exception when too_many_rows then
		-- �� �/� ����� �������
		if p_phone is null then
			p_res := '�� ������� ����� ������ ������ ������';
			return null;
		else
			-- ����� ����� ��� ����� �������� 
			begin
				select order_id
					into p_order_id
				from order_phones_t p
				where p.phone_number=p_phone
					and sysdate between p.date_from and nvl(p.date_to, sysdate+1);
				return p_order_id;
			exception when no_data_found then
				p_res := '�� ������ �������� ����� ' || p_phone;
				return null;
			end;
		end if;
	end;
	return null;
exception when others then
	p_res := sqlerrm;
	return null;
end;
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
     WHERE CONTRACTOR_TYPE = 'CSS'--Pk00_Const.c_CTR_TYPE_CSS
       AND CONTRACTOR = p_brand
    ;
    RETURN v_brand_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
		when others then
				dbms_output.put_line(sqlerrm);
				return null;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- ����� ��������, ���� �� ����� - �������
--   - ������������� - ID �������� 
--   - ��� ������ ���������� ����������
FUNCTION Get_curator_id( 
               p_region_id   IN INTEGER,     -- ID ������
               p_first_name IN VARCHAR2,    -- ���
               p_last_name  IN VARCHAR2     -- �������
           ) RETURN INTEGER                 -- ID ���������
IS
    v_manager_id INTEGER;
	v_contractor_id integer;
	v_prcName CONSTANT VARCHAR2(30) := 'Get_curator_id';
BEGIN
	-- ������� �����������
	begin
		select contractor_id 
			into v_contractor_id
		from contractor_t c where c.external_id=p_region_id;
	exception when others then
		Pk01_Syslog.raise_Exception('Can not find contractor with region_id=' || p_region_id, 
			c_PkgName||'.'||v_prcName );
	end;
    -- ������� ��������� �� ������
    SELECT MANAGER_ID
      INTO v_manager_id 
      FROM MANAGER_T m
     WHERE m.CONTRACTOR_ID = v_contractor_id
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
            v_manager_id, v_contractor_id, p_last_name, p_first_name, SYSDATE
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
-- ������� ����� �������� �������, ������: NPxxxxxxxxx
FUNCTION Make_contract_No RETURN VARCHAR2
IS
BEGIN
    RETURN 'NP'||TRIM(LPAD(SQ_CONTRACT_NO.NEXTVAL,9,'0'));
END;
           
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- ������� ����� �������� ����� �������: ������: ACCxxxxxxxxx 
FUNCTION Make_account_No RETURN VARCHAR2
IS
BEGIN
    RETURN 'ACC'||TRIM(LPAD(SQ_ACCOUNT_NO.NEXTVAL,9,'0'));
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- ������� ����� ������ ����� �������, ������: YYLDxxxxxxx
FUNCTION Make_order_No RETURN VARCHAR2
IS
BEGIN
    RETURN TO_CHAR(SYSDATE,'YY')||'LD'||TRIM(LPAD(SQ_ORDER_NO.NEXTVAL,7,'0'));
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- ����� ����������-���.���� �� �������� XML.SUBSCRIBER_ID, ���������� ��������
--   - ������������� - ID ���������� � ��������, 
--   - NULL - �� ������, ������� ������
--   - ��� ������ ���������� ����������
FUNCTION Find_subscriber_by_xmlid(
               p_xml_subscr_id  IN INTEGER
           ) RETURN INTEGER
IS
    v_subscriber_id INTEGER;
BEGIN
    SELECT AP.SUBSCRIBER_ID INTO v_subscriber_id
      FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP
     WHERE A.EXTERNAL_ID  = p_xml_subscr_id
       AND A.ACCOUNT_TYPE = 'P' --Pk00_Const.c_ACC_TYPE_P
       AND A.ACCOUNT_ID = AP.ACCOUNT_ID;
    RETURN v_subscriber_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;

-- ����� ����������-���.���� �� �������� XML.SUBSCRIBER_ID, ���������� ��������
--   - ������������� - ID ���������� � ��������, 
--   - NULL - �� ������, ������� ������
--   - ��� ������ ���������� ����������
FUNCTION Find_subscriber_by_fio (
               p_last_name   IN VARCHAR2,   -- �������
               p_first_name  IN VARCHAR2,   -- ��� 
               p_middle_name IN VARCHAR2,   -- ��������
               p_doc_serial  IN VARCHAR2,   -- ����� ���������
               p_doc_no      IN VARCHAR2    -- ����� ���������
           ) RETURN INTEGER
IS
    v_subscriber_id INTEGER;
BEGIN
    SELECT S.SUBSCRIBER_ID 
      INTO v_subscriber_id
      FROM SUBSCRIBER_T S, SUBSCRIBER_DOC_T SD
     WHERE UPPER(S.LAST_NAME)   = UPPER(p_last_name)
       AND UPPER(S.FIRST_NAME)  = UPPER(p_first_name)
       AND UPPER(S.MIDDLE_NAME) = UPPER(p_middle_name)
       AND SD.SUBSCRIBER_ID     = S.SUBSCRIBER_ID
       AND UPPER(SD.DOC_SERIAL) = UPPER(p_doc_serial)
       AND UPPER(SD.DOC_NO)     = UPPER(p_doc_no);
    RETURN v_subscriber_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;
        
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- ������� ������ ����������-���.����, ���������� ��������
--   - ������������� - ID ����������, 
--   - ��� ������ ���������� ����������
FUNCTION New_subscriber(
               p_last_name   IN VARCHAR2,   -- �������
               p_first_name  IN VARCHAR2,   -- ��� 
               p_middle_name IN VARCHAR2,   -- ��������
               p_category    IN INTEGER DEFAULT Pk00_Const.c_SUBS_RESIDENT  -- ��������� 1/2 = ��������/����������
           ) RETURN INTEGER
IS
BEGIN
    RETURN PK21_SUBSCRIBER.New_subscriber(
               p_last_name,
               p_first_name, 
               p_middle_name,
               p_category
           );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� �������� �������
--   - ������������� - ID ����������, 
--   - ��� ������ ���������� ����������
PROCEDURE Add_document(
               p_subscriber_id  IN INTEGER,
               p_doc_type       IN VARCHAR2, -- ��� ���������
               p_doc_serial     IN VARCHAR2, -- ����� ���������
               p_doc_no         IN VARCHAR2, -- ����� ���������
               p_doc_issuer     IN VARCHAR2, -- ��� ����� ��������
               p_doc_issue_date IN DATE      -- ���� ������ ���������
           )
IS
BEGIN
    PK21_SUBSCRIBER.Add_document(
               p_subscriber_id,
               p_doc_type,
               p_doc_serial,
               p_doc_no,
               p_doc_issuer,
               p_doc_issue_date
           );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- ������� ������� ���� �������
--   - ������������� - ID �������� ����� 
--   - ��� ������ ���������� ����������
FUNCTION New_account(
               p_xml_subscr_id  IN INTEGER,  -- ID ������� (XML) � ��������� �������
               p_subscriber_id  IN INTEGER,  -- ID ������� � ��������
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
    v_branch_id     INTEGER;
    v_agent_id      INTEGER;
		v_ret						INTEGER;
BEGIN
    -- ���������� ����� � �������� c ������ ���������� ��������� ���
    -- ��������� �� ��������� �������� ��� � ���������� �������
    SELECT C.CONTRACTOR_ID, C.PARENT_ID
     INTO v_agent_id, v_branch_id
     FROM CONTRACTOR_T C
    WHERE PARENT_ID = 200
      AND EXTERNAL_ID = p_brand_id;
    
    -- ��������� �� ������� ����� �������� �����
    p_account_no := Make_account_No;
    SELECT COUNT(*) INTO v_count
      FROM ACCOUNT_T A
     WHERE A.ACCOUNT_NO   = p_account_no
       AND A.ACCOUNT_TYPE = PK00_CONST.c_ACC_TYPE_P;
    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
               '��� XML.SUBSCRIBER_ID='||p_xml_subscr_id||
               ', � ������� ACCOUNT_T ��� ���������� ������ � ACCOUNT_NO='||p_account_no);
    END IF;
    -- ������� ������� ����
    v_account_id := Pk05_Account.New_account(
               p_account_no    => p_account_no,
               p_account_type  => PK00_CONST.c_ACC_TYPE_P,
               p_currency_id   => PK00_CONST.c_CURRENCY_RUB,
               p_status        => PK00_CONST.c_ACC_STATUS_BILL,
               p_parent_id     => NULL
           );
    UPDATE ACCOUNT_T SET EXTERNAL_ID = p_xml_subscr_id
     WHERE ACCOUNT_ID = v_account_id;
    -- ������� �������
    p_contract_no := Make_contract_No;
    v_contract_id := PK12_CONTRACT.Open_contract(
               p_contract_no => p_contract_no,
               p_date_from   => p_date,
               p_date_to     => NULL,
               p_client_id   => c_CLIENT_ID,
               p_manager_id  => p_curator_id
           );
    -- ������� ������� �������� �����
    v_profile_id := PK05_ACCOUNT.Set_profile(
               p_account_id    => v_account_id,
							 p_brand_id      => NULL,
               p_contract_id   => v_contract_id,
               p_customer_id   => PK00_CONST.c_CUSTOMER_PERSON_ID,
               p_subscriber_id => p_subscriber_id,
               p_contractor_id => c_CONTRACTOR_KTTK_ID,
               p_branch_id     => v_branch_id,
               p_agent_id      => v_agent_id,
               p_contractor_bank_id => Pk00_Const.c_KTTK_P_BANK_ID,
               p_vat           => Pk00_Const.c_VAT,
               p_date_from     => p_date,
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
               p_date_from   => p_date,
               p_date_to     => NULL
           );
    --���������� ��������
	v_ret := pk06_order.Lock_order (
			p_order_id      => v_order_id,
            p_lock_type_id  => pk00_const.c_ORDER_LOCK_NEW,
            p_manager_login => 'CSS',
            p_date_from     => SYSDATE,
            p_notes         => null
           );
    -- ������� ������ ������ ��� ��
    v_order_body_id := PK06_ORDER.Add_subservice(
               p_order_id      => v_order_id,
               p_subservice_id => PK00_CONST.c_SUBSRV_MG,
               p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
               p_date_from     => p_date
           );
           
    -- ������� ������ ������ ��� ��
    v_order_body_id := PK06_ORDER.Add_subservice(
               p_order_id      => v_order_id,
               p_subservice_id => PK00_CONST.c_SUBSRV_MN,
               p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
               p_date_from     => p_date,
               p_date_to       => NULL
           );
				-- ������� ������ ��� ������������� �����
		v_order_body_id := PK06_ORDER.Add_subservice(
               p_order_id      => v_order_id,
               p_subservice_id => PK00_CONST.c_SUBSRV_ZONE,
               p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
							 p_rateplan_id	 => p_rateplan_id,
               p_date_from     => p_date,
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
FUNCTION Add_phone(
               p_account_id   IN INTEGER,    -- ID �������� �����
               p_phone        IN VARCHAR2,   -- ����� ��������
               p_date_from    IN DATE        -- ���� ������ ��������
           ) return varchar2
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_phone';
    v_order_id    INTEGER;
		v_phone_rowid	varchar2(100);
BEGIN
    -- �������� ����� ������, ��� ������� ���� �������: ���� �/� - ���� �����
		begin
			SELECT ORDER_ID INTO v_order_id
				FROM ORDER_T t
			 WHERE ACCOUNT_ID = p_account_id
				 AND p_date_from between t.date_from and DATE_TO;
		exception when no_data_found then
			Pk01_Syslog.raise_Exception('Can''t find order_id for account_id=' || p_account_id, c_PkgName||'.'||v_prcName );
      RAISE;
		end;
    -- ��������� ������� �� �����
    v_phone_rowid := PK18_RESOURCE.Add_phone(
                   p_order_id => v_order_id,
                   p_phone    => p_phone,
                   p_date_from=> p_date_from,
                   p_date_to  => PK00_CONST.c_DATE_MAX
               );
		return v_phone_rowid;
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
--    v_account_no  ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_account_id  INTEGER;
BEGIN
    --v_account_no := Make_account_No;
    SELECT ACCOUNT_ID INTO v_account_id 
      FROM ACCOUNT_T A
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
    Pk01_Syslog.Write_msg(p_Msg   => '������ ACCOUNT.EXTERNAL_ID='||p_xml_subscr_id,
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


END PK103_PERSON_XML;
/
