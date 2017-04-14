create or replace package pk51_w_account_edit is

  -- Author  : KHROMYKH
  -- Created : 17.11.2014 13.24.19
  -- Purpose : 
  
	-- ==============================================================================
	c_PkgName   constant varchar2(30) := 'PK51_W_ACCOUNT_EDIT';
	-- ==============================================================================
	type forcursor is ref cursor;
	/* ************************************************************
	** �������������� ���������� � �/�
	
	procedure Edit_Account_Info(
		p_account_id		in number, 
		p_account_no		in varchar2,
		p_date_from			in date, 
		p_date_to			in date, 
		p_currency_id		in number, 	
		p_tax				in number, 
		p_billing_type		in number
		);*/
  procedure Edit_Account_Info(
    p_account_id          in number, 
    p_account_no		      in varchar2,
    p_currency_id		      in number, 	
    p_billing_id	        in number,
    p_account_status      in varchar2,
    p_account_commentary  in varchar2,
    p_account_notes       in varchar2
	);
	/* ***************************************************************
	** �������������� ��������
	*/
	procedure Edit_Account_Profile_Info(
		p_account_id			in number, 
		p_branch_id				in number, 
		p_agent_id				in number,
		p_brand_id				in number, 
		p_contractor_id			in number, 
		p_contractor_bank_id	in number
		);
		
	/* **********************************************************************
	** �������������� ���������� � ���������
	*/
PROCEDURE Edit_Contract_Info( 
               p_account_id           IN INTEGER,
               p_contract_id          IN INTEGER,
               p_contract_no          IN VARCHAR2,
               p_date_from            IN DATE,
               p_date_to              IN DATE,
               p_client_type_id       IN NUMBER,
               p_market_segment_id    IN NUMBER,
               p_contract_type_id     IN NUMBER
);

	/* *************************************************************************
	** �������������� ������ ��������
	 �������������� �������� �������� (������� ACCOUNT_DOCUMENTS_T)
		 * ���������� ��������/��������/������� ������ ��������, �������� ������ � �������
		 * ����� ������, ��� ��������� ������ ����� ��������� ����� ��������� � ���� ����� ��������
		 * �.�. ��������, ������ �������� Email ��� ��������� ���������� (DOC_BILL) � ����������� (DOC_CALLS) ����� ���� �������� 
			��� ��� �������, ��� � � ����� (� ����� ������ �������� - �� �������������)
		 * ��� DOC_BILL ����� � ���� 'Y', ��� DOC_DETAIL ����� ������ �� labelType ������� DeliveryMethod (��� �����������), 
			��� DOC_CALLS ����� ����� �� labelType ������� DeliveryMethod
		
	 �������� p_delivery_method - xml ���������
		<delivery>
			<delivery_method>
				<doc_type></doc_type>
				<delivery_type></<delivery_type>
				<delivery_mid></delivery_mid>
			</delivery_method>
		<delivery>
	*/
	procedure Edit_Account_Delivery_Method(
		p_account_id		in number,
		p_delivery_method	in varchar2
		);

-- �������� ������ ��������� �� �������� �����
PROCEDURE List_profile_by_account( 
         p_recordset     OUT forcursor,
         p_account_id    IN  NUMBER
);
-- �������� ������ ������, ����������� � �������
PROCEDURE list_profile_bills( 
         p_recordset     OUT forcursor,
         p_profile_id    IN  NUMBER
    );
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ������ �������
--   - ������������� - id ������� 
--   - ��� ������ ���������� ����������
FUNCTION New_profile(
           p_account_id         IN INTEGER,
           p_brand_id           IN INTEGER,
           p_contract_id        IN INTEGER,
           p_customer_id        IN INTEGER,
           p_subscriber_id      IN INTEGER,
           p_contractor_id      IN INTEGER,
           p_branch_id          IN INTEGER,
           p_agent_id           IN INTEGER,
           p_contractor_bank_id IN INTEGER,
           p_vat                IN NUMBER,
           p_date_from          IN DATE,
           p_date_to            IN DATE,
           close_prev_profile   IN INTEGER DEFAULT 0
       ) RETURN INTEGER;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������������� ������� �������� �����, ���������� ��������
--   - ������������� - id ������� 
--   - ��� ������ ���������� ����������
PROCEDURE Edit_profile(
               p_profile_id         IN INTEGER,
               p_account_id         IN INTEGER,
               p_brand_id           IN INTEGER,
               p_contract_id        IN INTEGER,
               p_customer_id        IN INTEGER,
               p_subscriber_id      IN INTEGER,
               p_contractor_id      IN INTEGER,
               p_branch_id          IN INTEGER,
               p_agent_id           IN INTEGER,
               p_contractor_bank_id IN INTEGER,
               p_vat                IN NUMBER,
               p_date_from          IN DATE,
               p_date_to            IN DATE
           );
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������������� ID ������� � �����
-- ------------------------------------------------------------------------
PROCEDURE Edit_bill_profile(
               p_profile_id         IN INTEGER,
               p_bill_id         	  IN INTEGER
           );
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ������� �������� �����
--   - ������������� - id ������� 
--   - ��� ������ ���������� ����������
PROCEDURE Delete_profile(
          p_profile_id         IN INTEGER,
          p_account_id         IN INTEGER
);


--=========================================================================================================================
-- �������� ������ ALIAS ��� ����� �� �������� �����
PROCEDURE LIST_SERVICE_ALIAS( 
         p_recordset     OUT forcursor,
         p_account_id    IN  NUMBER
);
    
-- ��������/�������� ������������ ������
--   - ��� ������ ���������� ����������
PROCEDURE UPSERT_SERVICE_ALIAS(
           p_account_id         IN INTEGER,
           p_service_id         IN INTEGER,
           p_alias_name         IN VARCHAR2
);

PROCEDURE DELETE_SERVICE_ALIAS(
           p_account_id         IN INTEGER,
           p_service_id         IN INTEGER                    
           );

end pk51_w_account_edit;
/
create or replace package body pk51_w_account_edit is

/* ************************************************************
** �������� id �������� 
*/
function Get_Profile_Id(
	p_account_id		in number
	) return number is
	v_profile_id		number;
begin
	select profile_id into v_profile_id
	from account_profile_t p
	where account_id = p_account_id
		and p.date_from = (select max(date_from) from account_profile_t p where account_id = p_account_id);
	return v_profile_id;
end;

/* ************************************************************
** �������������� ���������� � �/�

procedure Edit_Account_Info(
    p_account_id		in number, 
    p_account_no		in varchar2,
    p_date_from			in date, 
    p_date_to			in date, 
    p_currency_id		in number, 	
    p_tax				in number, 
    p_billing_type		in number
	) is
	v_profile_id		number;
	v_prcName			varchar2(1024) := c_PkgName || '.EDIT_ACCOUNT_INFO';
begin
	-- ��������  ������ ��������
	begin
		v_profile_id := Get_Profile_Id(p_account_id);
	exception when no_data_found then
		Pk01_Syslog.raise_Exception('������ � account_profile_t �� �������!', v_prcName);
	end;
	-- �������� ������ � ��������
	update account_profile_t p
	set p.vat = p_tax,
		p.date_from = p_date_from,
		p.date_to = p_date_to
	where profile_id = v_profile_id;
	-- �������� ������ � �/�
	update account_t a
	set a.account_no = upper(p_account_no),
		a.currency_id = p_currency_id,
		a.billing_id = p_billing_type
	where account_id = p_account_id;
exception when others then
	Pk01_Syslog.raise_Exception(sqlerrm, v_prcName);
end;*/
procedure Edit_Account_Info(
    p_account_id          in number, 
    p_account_no		      in varchar2,
    p_currency_id		      in number, 	
    p_billing_id	        in number,
    p_account_status      in varchar2,
    p_account_commentary  in varchar2,
    p_account_notes       in varchar2
	) is
	v_prcName			varchar2(1024) := c_PkgName || '.EDIT_ACCOUNT_INFO';
begin
	-- �������� ������ � �/�
	update account_t a
	set a.account_no = upper(p_account_no),
		  a.currency_id = p_currency_id,
      a.billing_id = p_billing_id,
		  a.status = p_account_status,
      a.commentary = p_account_commentary,
      a.notes = p_account_notes
	where account_id = p_account_id;
exception when others then
	Pk01_Syslog.raise_Exception(sqlerrm, v_prcName);
end;
/* ***************************************************************
** �������������� ��������
*/
procedure Edit_Account_Profile_Info(
	p_account_id			in number, 
	p_branch_id				in number, 
	p_agent_id				in number,
	p_brand_id				in number, 
	p_contractor_id			in number, 
	p_contractor_bank_id	in number
	) is
	v_prcName			varchar2(1024) := c_PkgName || '.EDIT_ACCOUNT_PROFILE_INFO';
	v_profile_id		number;
begin
	v_profile_id := Get_Profile_Id(p_account_id);
	--
	update account_profile_t p
	set p.branch_id = p_branch_id,
		p.agent_id = p_agent_id,
 		p.brand_id = NVL(p_brand_id,p.brand_id),
		p.contractor_id = NVL(p_contractor_id,p.contractor_id),
		p.contractor_bank_id = NVL(p_contractor_bank_id,p.contractor_bank_id)
	where p.profile_id = v_profile_id;
exception 
	when no_data_found then
		Pk01_Syslog.raise_Exception('������� �� ������', v_prcName);
	when others then
		Pk01_Syslog.raise_Exception(sqlerrm, v_prcName);
end;

/* **********************************************************************
** �������������� ���������� � ���������
*/
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ���������� �� ��������
PROCEDURE Edit_Contract_Info( 
               p_account_id           IN INTEGER,
               p_contract_id          IN INTEGER,
               p_contract_no          IN VARCHAR2,
               p_date_from            IN DATE,
               p_date_to              IN DATE,
               p_client_type_id       IN NUMBER,
               p_market_segment_id    IN NUMBER,
               p_contract_type_id     IN NUMBER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Edit_contact_datecreate';
BEGIN                          
    -- ��������� �������
    UPDATE contract_t
       SET contract_no =        p_contract_no,
           date_from =          p_date_from,
           date_to   =          p_date_to,
           CONTRACT_TYPE_ID =   p_contract_type_id,
           MARKET_SEGMENT_ID =  NVL(p_market_segment_id, MARKET_SEGMENT_ID),
           CLIENT_TYPE_ID =     NVL(p_client_type_id, CLIENT_TYPE_ID)
     WHERE contract_id = p_contract_id;
     
     EXCEPTION
    WHEN OTHERS THEN        
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION,   c_PkgName||'.'||v_prcName);
END;

/* *************************************************************************
** �������������� ������ ��������
 �������������� �������� �������� (������� ACCOUNT_DOCUMENTS_T)
     * ���������� ��������/��������/������� ������ ��������, �������� ������ � �������
     * ����� ������, ��� ��������� ������ ����� ��������� ����� ��������� � ���� ����� ��������
     * �.�. ��������, ������ �������� Email ��� ��������� ���������� (DOC_BILL) � ����������� (DOC_CALLS) ����� ���� �������� 
	 	��� ��� �������, ��� � � ����� (� ����� ������ �������� - �� �������������)
     * ��� DOC_BILL ����� � ���� 'Y', ��� DOC_DETAIL ����� ������ �� labelType ������� DeliveryMethod (��� �����������), 
	 	��� DOC_CALLS ����� ����� �� labelType ������� DeliveryMethod
	
 �������� p_delivery_method - xml ���������
	<delivery>
		<delivery_method>
 			<doc_type></doc_type>
			<delivery_type></<delivery_type>
			<delivery_mid></delivery_mid>
		</delivery_method>
	<delivery>
*/
procedure Edit_Account_Delivery_Method(
	p_account_id		in number,
	p_delivery_method	in varchar2
	) is
	v_prcName			varchar2(1024) := c_PkgName || '.EDIT_ACCOUNT_DELIVERY_METHOD';
	xml					xmltype;
	TYPE dlv_type IS RECORD (
		doc_type		varchar2(50),
		dlv_type		varchar2(50),
		dlv_mid			number
	);
	type dlv_tab is table of dlv_type index by binary_integer;
	v_dlv_tab		dlv_tab;
	v_cnt			number := 0;
	v_cur			forcursor;
begin
	if p_delivery_method is null then
		-- �������� �� �������� - �������
		delete from account_documents_t
		where account_id = p_account_id;
		return;
	end if;
	xml := xmltype(p_delivery_method);
	open v_cur for 
		select 
			extractValue(value(t),'delivery_method/doc_type'),
			extractValue(value(t),'delivery_method/delivery_type'),
			extractValue(value(t),'delivery_method/delivery_mid')
		from table(XMLSequence(xml.extract('delivery/delivery_method'))) t;
		loop
			fetch v_cur into
				v_dlv_tab(v_cnt+1).doc_type,
				v_dlv_tab(v_cnt+1).dlv_type,
				v_dlv_tab(v_cnt+1).dlv_mid;
			exit when v_cur%notfound;
			v_cnt := v_cnt + 1;
		end loop;
		-- �������� ������� ������ ��������
		delete from account_documents_t
		where account_id = p_account_id;
/*
		if v_cnt = 0 then
			Pk01_Syslog.raise_Exception('����������� ������ ��������', v_prcName);
		end if;
*/
		-- �������� ������� ������ ��������
		delete from account_documents_t
		where account_id = p_account_id;
		if v_cnt > 0 then
			-- ������� �����
			for v_i in 1..v_cnt loop
				insert into account_documents_t(
					account_id, doc_bill, doc_detail, doc_calls, delivery_method_id)
				values(
					p_account_id, 
						decode(v_dlv_tab(v_i).doc_type, 'DOC_BILL', v_dlv_tab(v_i).dlv_type, null),
						decode(v_dlv_tab(v_i).doc_type, 'DOC_DETAIL', v_dlv_tab(v_i).dlv_type, null),
						decode(v_dlv_tab(v_i).doc_type, 'DOC_CALLS', v_dlv_tab(v_i).dlv_type, null),
						v_dlv_tab(v_i).dlv_mid
				);
			end loop;
		end if;
exception 
	when others then
		Pk01_Syslog.raise_Exception(sqlerrm, v_prcName);
end;

/** ====================================================================== */
--=====================================================================================================================
-- �������� ������ ��������� �� �������� �����
PROCEDURE List_profile_by_account( 
         p_recordset     OUT forcursor,
         p_account_id    IN  NUMBER
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'List_profile_by_account';
    v_retcode            INTEGER;
BEGIN
    open p_recordset for
       WITH BILL_LINK AS (
          SELECT PROFILE_ID, COUNT(*) CNT FROM BILL_T
          GROUP BY PROFILE_ID
       )       
       SELECT 
           ap.PROFILE_ID,
           ap.ACCOUNT_ID,
           AP.CONTRACT_ID,
           C.CONTRACT_NO,
           AP.CUSTOMER_ID,       
           CUS.CUSTOMER CUSTOMER_NAME,
           CUS.SHORT_NAME CUSTOMER_NAME_SHORT,
           CUS.INN CUSTOMER_INN,
           CUS.KPP CUSTOMER_KPP,
           CUS.ERP_CODE CUSTOMER_ERP_CODE,
           AP.SUBSCRIBER_ID CLIENT_F_ID,
           SUB.LAST_NAME CLIENT_F_LAST_NAME,
           SUB.FIRST_NAME CLIENT_F_FIRST_NAME,
           SUB.MIDDLE_NAME CLIENT_F_MIDDLE_NAME,
           AP.CONTRACTOR_ID,
           PROV.SHORT_NAME CONTRACTOR_NAME,
           AP.BRANCH_ID,
           branch.SHORT_NAME BRANCH_NAME,
           AP.AGENT_ID,
           agent.SHORT_NAME AGENT_NAME,
           AP.CONTRACTOR_BANK_ID,
           BANK.BANK_NAME,
           BANK.BANK_CODE,
           BANK.BANK_CORR_ACCOUNT,
           BANK.BANK_SETTLEMENT,
           AP.VAT,
           AP.DATE_FROM,
           AP.DATE_TO,
           CASE WHEN SYSDATE BETWEEN ap.DATE_FROM AND NVL(ap.DATE_TO,TO_DATE('01.01.2050','DD.MM.YYYY')) THEN 1 ELSE 0 END IS_CURRENT,
           NVL(BILL_LINK.CNT,0) BILL_LINK_COUNT
     FROM 
            account_profile_t ap,
            contract_t c,
            contractor_t prov,
            contractor_t branch,
            contractor_t agent,
            contractor_bank_t bank,
            customer_t cus,
            subscriber_t sub,
            BILL_LINK
    WHERE 
        ap.contract_id = c.contract_id
        and AP.CUSTOMER_ID = cus.CUSTOMER_ID(+)
        AND AP.SUBSCRIBER_ID = SUB.SUBSCRIBER_ID (+)
        and AP.CONTRACTOR_ID = prov.CONTRACTOR_ID
        and AP.BRANCH_ID = branch.CONTRACTOR_ID
        and ap.AGENT_ID = agent.CONTRACTOR_ID (+)
        and AP.CONTRACTOR_BANK_ID = BANK.BANK_ID
        AND AP.PROFILE_ID = BILL_LINK.PROFILE_ID (+)
        and AP.ACCOUNT_ID = p_account_id
    ORDER BY AP.DATE_FROM, AP.PROFILE_ID;   
END;
--=====================================================================================================================
-- �������� ������ ������, ����������� � �������
--=====================================================================================================================
PROCEDURE list_profile_bills( 
         p_recordset     OUT forcursor,
         p_profile_id    IN  NUMBER
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'list_profile_bills';
    v_retcode            INTEGER;
BEGIN
    open p_recordset for      
SELECT B.BILL_ID,
       b.bill_no,
       b.bill_type,
       B.REP_PERIOD_ID,
       b.bill_date,
       B.PROFILE_ID,
       A.ACCOUNT_ID,
       C.CONTRACT_NO ACCOUNT_NO,               --TODO ���������� �� ���������� ��������
       CT.CONTRACTOR_ID,
       CT.CONTRACTOR,
       CB.BANK_ID,
       CB.BANK_NAME,
       CB.NOTES
  FROM bill_t b,
       account_t a,
       account_profile_t ap,
       contractor_t ct,
       contractor_bank_t cb,
       contract_t c
 WHERE     A.ACCOUNT_ID = B.ACCOUNT_ID
       AND ap.profile_id = b.profile_id
       AND AP.CONTRACTOR_ID = CT.CONTRACTOR_ID
       AND CB.BANK_ID = AP.CONTRACTOR_BANK_ID
       AND ap.profile_id  = p_profile_id
       AND c.contract_id = b.contract_id;
END;
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ������ �������
--   - ������������� - id ������� 
--   - ��� ������ ���������� ����������
FUNCTION New_profile(
           p_account_id         IN INTEGER,
           p_brand_id           IN INTEGER,
           p_contract_id        IN INTEGER,
           p_customer_id        IN INTEGER,
           p_subscriber_id      IN INTEGER,
           p_contractor_id      IN INTEGER,
           p_branch_id          IN INTEGER,
           p_agent_id           IN INTEGER,
           p_contractor_bank_id IN INTEGER,
           p_vat                IN NUMBER,
           p_date_from          IN DATE,
           p_date_to            IN DATE,
           close_prev_profile   IN INTEGER DEFAULT 0
       ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'New_profile';
    v_profile_id INTEGER;
    v_date_from  DATE;
    v_billing_id INTEGER;
    v_count      INTEGER;
BEGIN
    IF close_prev_profile = 1 THEN   
        -- ��������� ������� ������, ���� ��� ����
        UPDATE ACCOUNT_PROFILE_T
           SET DATE_TO = p_date_from - 1/86400
         WHERE ACCOUNT_ID = p_account_id
           AND ( DATE_TO IS NULL OR p_date_from <= DATE_TO);
    END IF;
    
    -- �������� �� ���������� ���������
    SELECT BILLING_ID INTO v_billing_id
      FROM ACCOUNT_T
     WHERE ACCOUNT_ID = p_account_id;

    IF v_billing_id = Pk00_Const.c_BILLING_MMTS AND p_contractor_bank_id NOT IN (1,2) THEN
       Pk01_Syslog.Raise_user_exception('��� �������� ���� [2003] ����������� ����� � ID 1 � 2', c_PkgName||'.'||v_prcName);
    END IF;

    -- ��������� ������� ������ � COMPANY_T ��� ��������
    SELECT COUNT(*) INTO v_count
      FROM COMPANY_T CM
     WHERE CM.CONTRACT_ID = p_contract_id;
    IF v_count = 0 THEN
        INSERT INTO COMPANY_T (CONTRACT_ID, COMPANY_NAME, SHORT_NAME, DATE_FROM, DATE_TO)
        SELECT p_contract_id, CS.CUSTOMER, CS.SHORT_NAME, p_date_from, NULL
          FROM CUSTOMER_T CS
         WHERE CS.CUSTOMER_ID = p_customer_id;
    END IF;
    
    -- ������������� ����� �����:
    INSERT INTO ACCOUNT_PROFILE_T (
       PROFILE_ID, ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, SUBSCRIBER_ID,
       CONTRACTOR_ID, BRANCH_ID, AGENT_ID,  
       CONTRACTOR_BANK_ID, VAT, BRAND_ID, DATE_FROM, DATE_TO)
    VALUES
       (Pk02_Poid.Next_account_profile_id, 
        p_account_id, p_contract_id, p_customer_id, p_subscriber_id,
        p_contractor_id, p_branch_id, p_agent_id, 
        p_contractor_bank_id, p_vat, p_brand_id, p_date_from, p_date_to)
    RETURNING PROFILE_ID INTO v_profile_id;
    RETURN v_profile_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������������� ������� �������� �����, ���������� ��������
--   - ������������� - id ������� 
--   - ��� ������ ���������� ����������
PROCEDURE Edit_profile(
               p_profile_id         IN INTEGER,
               p_account_id         IN INTEGER,
               p_brand_id           IN INTEGER,
               p_contract_id        IN INTEGER,
               p_customer_id        IN INTEGER,
               p_subscriber_id      IN INTEGER,
               p_contractor_id      IN INTEGER,
               p_branch_id          IN INTEGER,
               p_agent_id           IN INTEGER,
               p_contractor_bank_id IN INTEGER,
               p_vat                IN NUMBER,
               p_date_from          IN DATE,
               p_date_to            IN DATE                            
           ) 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Edit_profile';
    v_profile_id INTEGER;
    v_date_from  DATE;
    v_billing_id INTEGER;
BEGIN    
  
    -- �������� �� ���������� ���������
    SELECT BILLING_ID INTO v_billing_id
      FROM ACCOUNT_T
     WHERE ACCOUNT_ID = p_account_id;
    IF v_billing_id = Pk00_Const.c_BILLING_MMTS AND p_contractor_bank_id NOT IN (1,2) THEN
       Pk01_Syslog.Raise_user_exception('��� �������� ���� [2003] ����������� ����� � ID 1 � 2', c_PkgName||'.'||v_prcName);
    END IF;
    
    UPDATE ACCOUNT_PROFILE_T 
      SET   
           CONTRACT_ID    = p_contract_id, 
           CUSTOMER_ID    = p_customer_id, 
           SUBSCRIBER_ID  = p_subscriber_id,
           CONTRACTOR_ID  = p_contractor_id, 
           BRANCH_ID      = p_branch_id, 
           AGENT_ID       = p_agent_id,  
           CONTRACTOR_BANK_ID = p_contractor_bank_id, 
           VAT            = p_vat, 
           BRAND_ID       = p_brand_id, 
           DATE_FROM      = p_date_from, 
           DATE_TO        = p_date_to
   WHERE PROFILE_ID = p_profile_id
         AND ACCOUNT_ID = p_account_id;
         
   IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '� ������� ACCOUNT_PROFILE_T ��� ������ � ACCOUNT_ID='||p_account_id || ', PROFILE_ID = ' || p_profile_id);
  END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������������� ID ������� � �����
-- ------------------------------------------------------------------------
PROCEDURE Edit_bill_profile(
               p_profile_id         IN INTEGER,
               p_bill_id         	  IN INTEGER
           ) 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Edit_bill_profile';
BEGIN    
     MERGE INTO BILL_T  b
     USING (
         SELECT * FROM ACCOUNT_PROFILE_T ap
     ) t
     ON (b.BILL_ID = p_bill_id AND t.PROFILE_ID = p_profile_id)
     WHEN MATCHED THEN
          UPDATE SET
             b.PROFILE_ID         = t.profile_id,
             b.CONTRACT_ID        = t.contract_id,
             b.CONTRACTOR_ID      = t.contractor_id,
             b.CONTRACTOR_BANK_ID = t.contractor_bank_id;
     
   IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '� ������� BILL_T ��� ������ � BILL_ID='||p_bill_id);
  END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE Delete_profile(
          p_profile_id         IN INTEGER,
          p_account_id         IN INTEGER
)IS
BEGIN
   DELETE FROM ACCOUNT_PROFILE_T WHERE ACCOUNT_ID = p_account_id AND PROFILE_ID = p_profile_id;
    IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '� ������� ACCOUNT_PROFILE_T ��� ������ � ACCOUNT_ID='||p_account_id || ', PROFILE_ID = ' || p_profile_id);
    END IF;       
END;

--=========================================================================================================================
-- �������� ������ ALIAS ��� ����� �� �������� �����
PROCEDURE LIST_SERVICE_ALIAS( 
         p_recordset     OUT forcursor,
         p_account_id    IN  NUMBER
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'LIST_SERVICE_ALIAS';
    v_retcode            INTEGER;
BEGIN
    open p_recordset for
       SELECT sa.ACCOUNT_ID,
             SA.SERVICE_ID,
             S.SERVICE SERVICE_NAME,
             SA.SRV_NAME SERVICE_ALIAS_NAME
        FROM service_alias_t sa, service_t s
       WHERE s.service_id = sa.service_id 
             AND sa.account_id = p_account_id;   
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ��������/�������� ������������ ������
--   - ��� ������ ���������� ����������
PROCEDURE UPSERT_SERVICE_ALIAS(
           p_account_id         IN INTEGER,
           p_service_id         IN INTEGER,
           p_alias_name         IN VARCHAR2
       )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'UPSERT_SERVICE_ALIAS';
BEGIN   
    MERGE INTO SERVICE_ALIAS_T
             USING DUAL
                ON (service_id = p_service_id and account_id = p_account_id)
        WHEN MATCHED
        THEN
           UPDATE SET srv_name = p_alias_name
        WHEN NOT MATCHED
        THEN
           INSERT (service_id, account_id, srv_name)
               VALUES (p_service_id, p_account_id, p_alias_name)
;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
PROCEDURE DELETE_SERVICE_ALIAS(
           p_account_id         IN INTEGER,
           p_service_id         IN INTEGER                    
           ) 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DELETE_SERVICE_ALIAS';
    v_profile_id INTEGER;
    v_date_from  DATE;
BEGIN    
    delete from SERVICE_ALIAS_T
    WHERE 
           SERVICE_ID = p_service_id 
           AND ACCOUNT_ID = p_account_id;          
         
   IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '� ������� SERVICE_ALIAS_T ��� ������ � ACCOUNT_ID='||p_account_id || ', SERVICE_ID = ' || p_service_id);
  END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

end pk51_w_account_edit;
/
