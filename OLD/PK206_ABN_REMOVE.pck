CREATE OR REPLACE PACKAGE PK206_ABN_REMOVE
IS
    --
    -- ����� ��� �������� �������� ������ xTTK ������-������������� �������һ
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK206_ABN_REMOVE';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    -- ID ����
    c_CONTRACTOR_KTTK_ID     CONSTANT INTEGER := 1;
    -- ��������� ��� '��� ������ ���' 
    c_CONTRACTOR_SPB_XTTK_ID CONSTANT INTEGER := 8;
    -- ������ ������������� � �������� ������� �� ������� �/��� ������� ������
    c_SERVICE_OPLOCL         CONSTANT INTEGER := 7;

		/* **********************************************
		* ������� ������ �� ������
		*/
		procedure Remove_Subs_By_phone(
			p_phone			in varchar2
			);
		/* **********************************************
		* ������� ����� �� ������
		*/
		procedure Remove_Cust_By_phone(
			p_phone			in varchar2
			);


    
    -- ============================================================================== --
    -- ������� ������� "������"
    -- ============================================================================== --
    PROCEDURE Remove_subscriber( p_account_id IN INTEGER );
    
    -- ============================================================================== --
    -- ������� ������� "�����"
    -- ============================================================================== --
    PROCEDURE Remove_customer( p_account_id IN INTEGER );
    
END PK206_ABN_REMOVE;
/
CREATE OR REPLACE PACKAGE BODY PK206_ABN_REMOVE
IS

/* **********************************************
* ������� ������ �� ������
*/
procedure Remove_Subs_By_phone(
	p_phone			in varchar2
	) is
	p_account_id		number;
begin
	select o.account_id 
		into p_account_id
	from ORDER_PHONES_T p, order_t o
	where phone_number = p_phone and p.order_id=o.order_id;
	--
	Remove_subscriber(p_account_id);
end;

/* **********************************************
* ������� ����� �� ������
*/
procedure Remove_Cust_By_phone(
	p_phone			in varchar2
	) is
	p_account_id		number;
begin
	select o.account_id 
		into p_account_id
	from ORDER_PHONES_T p, order_t o
	where phone_number = p_phone and p.order_id=o.order_id;
	--
	Remove_customer(p_account_id);
end;

-- ============================================================================== --
-- ������� ������� "������"
-- ============================================================================== --
PROCEDURE Remove_subscriber( p_account_id IN INTEGER )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Remove_customer';
    --
    v_acc_type       ACCOUNT_T.ACCOUNT_TYPE%TYPE;
    v_order_id       INTEGER;
    v_rateplan_id    INTEGER;
    v_subscriber_id  INTEGER;
    v_contract_id    INTEGER;
    v_profile_id     INTEGER;
    v_count          INTEGER;
    v_count_contract INTEGER;
    v_count_subs     INTEGER;
    --
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName);
    -- ���������� ��� �/� ���������� � ����������� "�"�����
    SELECT ACCOUNT_TYPE INTO v_acc_type
    FROM ACCOUNT_T
    WHERE ACCOUNT_ID = p_account_id;
    IF v_acc_type != 'P' THEN
        Pk01_Syslog.Write_msg('Account_id='||p_account_id||', has a wrong type "'||v_acc_type||'"', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'Account_id='||p_account_id||', has a wrong type "'||v_acc_type||'"');
    END IF;

    -- ������� ������� �� �����
    DELETE FROM REP_PERIOD_INFO_T RP
    WHERE RP.ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('REP_PERIOD_INFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� INVOICE_ITEM
    DELETE FROM INVOICE_ITEM_T II
    WHERE EXISTS (
       SELECT * FROM BILL_T B
        WHERE B.ACCOUNT_ID = p_account_id
          AND B.BILL_ID = II.BILL_ID
          AND B.REP_PERIOD_ID = II.REP_PERIOD_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� ITEM
    DELETE FROM ITEM_T I
    WHERE EXISTS (
        SELECT * FROM BILL_T B
         WHERE B.ACCOUNT_ID = p_account_id
           AND I.BILL_ID = B.BILL_ID
           AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� BILLINFO_T
    DELETE FROM BILLINFO_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLINFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� BILL_T
    DELETE FROM BILL_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� ������ ������ (����� ��������� ����)
    BEGIN
        SELECT ORDER_ID, RATEPLAN_ID INTO v_order_id, v_rateplan_id
        FROM ORDER_T
        WHERE ACCOUNT_ID = p_account_id;
        
        -- ������� �������� ��������� � �������
        DELETE FROM SALE_CURATOR_T
        WHERE ACCOUNT_ID = p_account_id OR ORDER_ID = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('SALE_CURATOR_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
       
        -- ������� ������ ������
        DELETE FROM ORDER_BODY_T
        WHERE ORDER_ID = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('ORDER_BODY_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        
        -- ������� ������ ���������
        DELETE FROM PHONE_ADDRESS_T PA
        WHERE EXISTS (
            SELECT *
              FROM ORDER_PHONES_T OPH
             WHERE OPH.ORDER_ID = v_order_id
               AND OPH.ADDRESS_ID = PA.ADDRESS_ID
        );
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('PHONE_ADDRESS_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info ); 
        
        -- ������� ���������� ������
				DELETE FROM ORDER_LOCK_T
				WHERE  ORDER_ID = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('ORDER_LOCK_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        
        -- ������� �������� �� ������
        DELETE FROM ORDER_PHONES_T
         WHERE ORDER_ID = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('ORDER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

        -- ������� �����
        DELETE FROM ORDER_T 
        WHERE ORDER_ID = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('ORDER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    EXCEPTION
      WHEN NO_DATA_FOUND THEN NULL;
      WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
    END;
    
    -- ������ �������� ���� (��� ����� ������� �����������)
    --DELETE FROM RATEPLAN_T
    --WHERE RATEPLAN_ID = v_rateplan_id;

    -- ������� ������ �� ������� �����
    DELETE FROM ACCOUNT_CONTACT_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_CONTACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    -- ������� ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ������ �� ������� �������� ����� (������������ ��� �� ����)
    SELECT AP.PROFILE_ID, AP.CONTRACT_ID, AP.SUBSCRIBER_ID
    INTO v_profile_id, v_contract_id, v_subscriber_id
    FROM ACCOUNT_PROFILE_T AP, CONTRACT_T C
    WHERE AP.ACCOUNT_ID = p_account_id
      AND AP.CONTRACT_ID = C.CONTRACT_ID;

    -- ������� ACCOUNT_PROFILE_T
    DELETE FROM ACCOUNT_PROFILE_T
    WHERE PROFILE_ID = v_profile_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_PROFILE_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� ������� ����
    DELETE FROM ACCOUNT_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      
    -- ������� �������, ���� �� ��� ��� ������ ������� ������
    SELECT COUNT(*) INTO v_count
    FROM ACCOUNT_PROFILE_T
    WHERE CONTRACT_ID = v_contract_id;
    IF v_count = 0 THEN
      --
      DELETE FROM SALE_CURATOR_T
      WHERE CONTRACT_ID = v_contract_id;
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('SALE_CURATOR_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      --
      DELETE FROM CONTRACT_T
      WHERE CONTRACT_ID = v_contract_id;
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('CONTRACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      --
    END IF;
    Pk01_Syslog.Write_msg('CONTRACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info ); 

    -- ������� ������ �������� (�������), ���� � ��� ��� ������ ������� ������
    SELECT COUNT(*) INTO v_count
    FROM ACCOUNT_PROFILE_T
    WHERE SUBSCRIBER_ID = v_subscriber_id;
    IF v_count = 0 THEN
      -- ������� ���������� ������
      DELETE FROM SUBSCRIBER_DOC_T
      WHERE SUBSCRIBER_ID = v_subscriber_id;
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('SUBSCRIBER_DOC_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      -- ������� ��������
      DELETE FROM SUBSCRIBER_T
      WHERE SUBSCRIBER_ID = v_subscriber_id;
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('SUBSCRIBER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    END IF;
    --
    Pk01_Syslog.Write_msg('The end.', c_PkgName||'.'||v_prcName);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ============================================================================== --
-- ������� ������� "�����"
-- ============================================================================== --
PROCEDURE Remove_customer( p_account_id IN INTEGER )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Remove_customer';
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
    -- ���������� ��� �/� ���������� � ����������� "�"����
    SELECT ACCOUNT_TYPE INTO v_acc_type
    FROM ACCOUNT_T
    WHERE ACCOUNT_ID = p_account_id;
    IF v_acc_type != 'J' THEN
        Pk01_Syslog.Write_msg('Account_id='||p_account_id||', has a wrong type "'||v_acc_type||'"', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'Account_id='||p_account_id||', has a wrong type "'||v_acc_type||'"');
    END IF;

    -- ������� ������� �� �����
    DELETE FROM REP_PERIOD_INFO_T RP
    WHERE RP.ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('REP_PERIOD_INFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� INVOICE_ITEM
    DELETE FROM INVOICE_ITEM_T II
    WHERE EXISTS (
       SELECT * FROM BILL_T B
        WHERE B.ACCOUNT_ID = p_account_id
          AND B.BILL_ID = II.BILL_ID
          AND B.REP_PERIOD_ID = II.REP_PERIOD_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� ITEM
    DELETE FROM ITEM_T I
    WHERE EXISTS (
        SELECT * FROM BILL_T B
         WHERE B.ACCOUNT_ID = p_account_id
           AND I.BILL_ID = B.BILL_ID
           AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� BILLINFO_T
    DELETE FROM BILLINFO_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLINFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� BILL_T
    DELETE FROM BILL_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� ������ ������ (����� ��������� ����)
    BEGIN
        SELECT ORDER_ID, RATEPLAN_ID INTO v_order_id, v_rateplan_id
        FROM ORDER_T
        WHERE ACCOUNT_ID = p_account_id;
        
        -- ������� �������� ��������� � �������
        DELETE FROM SALE_CURATOR_T
        WHERE ACCOUNT_ID = p_account_id OR ORDER_ID = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('SALE_CURATOR_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
       
        -- ������� ������ ������
        DELETE FROM ORDER_BODY_T
        WHERE ORDER_ID = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('ORDER_BODY_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        -- ������� ������ �� �����
				begin 
					select address_id into v_count
					from order_phones_t
					where ORDER_ID = v_order_id;
					-- 		������ ������ �� �����
					update order_phones_t
					set address_id = null
					where ORDER_ID = v_order_id;
					-- ������� ������ ���������
					DELETE FROM PHONE_ADDRESS_T PA
					WHERE address_id = v_count;/*EXISTS (
							SELECT *
								FROM ORDER_PHONES_T OPH
							 WHERE OPH.ORDER_ID = v_order_id
								 AND OPH.ADDRESS_ID = PA.ADDRESS_ID
					);*/
					v_count := SQL%ROWCOUNT;
					Pk01_Syslog.Write_msg('PHONE_ADDRESS_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
				exception when no_data_found then
					null;
				end;			
        -- ������� �������� �� ������
        DELETE FROM ORDER_PHONES_T
         WHERE ORDER_ID = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('ORDER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

        -- ������� SALE-CURATOR ������
        DELETE FROM SALE_CURATOR_T
        WHERE ORDER_ID    = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('SALE_CURATOR_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        -- ������� ���������� ������
				delete from order_lock_t t
				where t.order_id = v_order_id;
        -- ������� �����
        DELETE FROM ORDER_T 
        WHERE ORDER_ID = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('ORDER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    EXCEPTION
      WHEN NO_DATA_FOUND THEN NULL;
      WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
    END;
 
    -- ������ �������� ���� (��� ����� ������� �����������)
    --DELETE FROM RATEPLAN_T
    --WHERE RATEPLAN_ID = v_rateplan_id;
    -- ������� ������
    DELETE FROM ACCOUNT_CONTACT_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_CONTACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    -- ������� ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    SELECT AP.PROFILE_ID, AP.CONTRACT_ID, AP.CUSTOMER_ID, C.CLIENT_ID
    INTO v_profile_id, v_contract_id, v_customer_id, v_client_id
    FROM ACCOUNT_PROFILE_T AP, CONTRACT_T C
    WHERE AP.ACCOUNT_ID = p_account_id
      AND AP.CONTRACT_ID = C.CONTRACT_ID;
      
    -- ������� ACCOUNT_PROFILE_T
    DELETE FROM ACCOUNT_PROFILE_T
    WHERE PROFILE_ID = v_profile_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_PROFILE_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� SALE-CURATOR ��� �������� �����
    DELETE FROM SALE_CURATOR_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SALE_CURATOR_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� ������� ����
    DELETE FROM ACCOUNT_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      
    -- ������� �������
    SELECT COUNT(*) INTO v_count
    FROM ACCOUNT_PROFILE_T
    WHERE CONTRACT_ID = v_contract_id;
    IF v_count = 0 THEN
      -- ������� SALE-CURATOR ��� �������� �����
      DELETE FROM SALE_CURATOR_T
      WHERE CONTRACT_ID = v_contract_id;
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('SALE_CURATOR_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      --
      DELETE FROM CONTRACT_T
      WHERE CONTRACT_ID = v_contract_id;
      v_count := SQL%ROWCOUNT;
    END IF;
    Pk01_Syslog.Write_msg('CONTRACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info ); 
   
    -- ������� �������
    SELECT COUNT(*) INTO v_count
    FROM CONTRACT_T
    WHERE CLIENT_ID = v_client_id;
    IF v_count = 0 THEN
      --
      DELETE FROM CLIENT_T
      WHERE CLIENT_ID = v_client_id;
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('CLIENT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    END IF;
		-- ������� ���������� ���������
		delete from customer_bank_t
		where customer_id = v_customer_id;
    -- ������� ������ ����������
    SELECT COUNT(*) INTO v_count
    FROM ACCOUNT_PROFILE_T
    WHERE CUSTOMER_ID = v_customer_id;
    IF v_count = 0 THEN
      -- ������� �������� ������
      DELETE FROM CUSTOMER_ADDRESS_T
      WHERE CUSTOMER_ID = v_customer_id;
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('CUSTOMER_ADDRESS_T.JUR: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      -- ������� ����������
      DELETE FROM CUSTOMER_T
      WHERE CUSTOMER_ID = v_customer_id;
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('CUSTOMER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    END IF;
    --
    Pk01_Syslog.Write_msg('The end.', c_PkgName||'.'||v_prcName);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

END PK206_ABN_REMOVE;
/
