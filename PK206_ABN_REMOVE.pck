CREATE OR REPLACE PACKAGE PK206_ABN_REMOVE
IS
    --
    -- Пакет для создания клиентов бренда xTTK «Санкт-Петербургский ТЕЛЕПОРТ»
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK206_ABN_REMOVE';
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

		/* **********************************************
		* удалить физика по номеру
		*/
		procedure Remove_Subs_By_phone(
			p_phone			in varchar2
			);
		/* **********************************************
		* удалить юрика по номеру
		*/
		procedure Remove_Cust_By_phone(
			p_phone			in varchar2
			);


    
    -- ============================================================================== --
    -- Удалить клиента "Физика"
    -- ============================================================================== --
    PROCEDURE Remove_subscriber( p_account_id IN INTEGER );
    
    -- ============================================================================== --
    -- Удалить клиента "Юрика"
    -- ============================================================================== --
    PROCEDURE Remove_customer( p_account_id IN INTEGER );
    
END PK206_ABN_REMOVE;
/
CREATE OR REPLACE PACKAGE BODY PK206_ABN_REMOVE
IS

/* **********************************************
* удалить физика по номеру
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
* удалить юрика по номеру
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
-- Удалить клиента "Физика"
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
    -- убеждаемся что л/с существует и принадлежит "Ф"изику
    SELECT ACCOUNT_TYPE INTO v_acc_type
    FROM ACCOUNT_T
    WHERE ACCOUNT_ID = p_account_id;
    IF v_acc_type != 'P' THEN
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
    WHERE EXISTS (
       SELECT * FROM BILL_T B
        WHERE B.ACCOUNT_ID = p_account_id
          AND B.BILL_ID = II.BILL_ID
          AND B.REP_PERIOD_ID = II.REP_PERIOD_ID
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
    BEGIN
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
        
        -- удаляем адреса телефонов
        DELETE FROM PHONE_ADDRESS_T PA
        WHERE EXISTS (
            SELECT *
              FROM ORDER_PHONES_T OPH
             WHERE OPH.ORDER_ID = v_order_id
               AND OPH.ADDRESS_ID = PA.ADDRESS_ID
        );
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('PHONE_ADDRESS_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info ); 
        
        -- удалить блокировку номера
				DELETE FROM ORDER_LOCK_T
				WHERE  ORDER_ID = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('ORDER_LOCK_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        
        -- удаляем телефоны на заказе
        DELETE FROM ORDER_PHONES_T
         WHERE ORDER_ID = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('ORDER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

        -- удаляем заказ
        DELETE FROM ORDER_T 
        WHERE ORDER_ID = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('ORDER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    EXCEPTION
      WHEN NO_DATA_FOUND THEN NULL;
      WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
    END;
    
    -- удалем тарифный план (при новом импорте воссоздадим)
    --DELETE FROM RATEPLAN_T
    --WHERE RATEPLAN_ID = v_rateplan_id;

    -- удаляем адреса на лицевом счете
    DELETE FROM ACCOUNT_CONTACT_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_CONTACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    -- Удаляем окружение
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- получаем данные из профиля лицевого счета (предполагаем что он один)
    SELECT AP.PROFILE_ID, AP.CONTRACT_ID, AP.SUBSCRIBER_ID
    INTO v_profile_id, v_contract_id, v_subscriber_id
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
      
    -- удаляем договор, если на нем нет других лицевых счетов
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

    -- удаляем данные абонента (клиента), если у нег нет других лицевых счетов
    SELECT COUNT(*) INTO v_count
    FROM ACCOUNT_PROFILE_T
    WHERE SUBSCRIBER_ID = v_subscriber_id;
    IF v_count = 0 THEN
      -- удаляем паспортные данные
      DELETE FROM SUBSCRIBER_DOC_T
      WHERE SUBSCRIBER_ID = v_subscriber_id;
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('SUBSCRIBER_DOC_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      -- удаляем абонента
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
-- Удалить клиента "Юрика"
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
    WHERE EXISTS (
       SELECT * FROM BILL_T B
        WHERE B.ACCOUNT_ID = p_account_id
          AND B.BILL_ID = II.BILL_ID
          AND B.REP_PERIOD_ID = II.REP_PERIOD_ID
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
    BEGIN
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
        -- удаляем ссылку на адрес
				begin 
					select address_id into v_count
					from order_phones_t
					where ORDER_ID = v_order_id;
					-- 		убрать ссылку на адрес
					update order_phones_t
					set address_id = null
					where ORDER_ID = v_order_id;
					-- удаляем адреса телефонов
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
        -- удаляем телефоны на заказе
        DELETE FROM ORDER_PHONES_T
         WHERE ORDER_ID = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('ORDER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

        -- удаляем SALE-CURATOR заказа
        DELETE FROM SALE_CURATOR_T
        WHERE ORDER_ID    = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('SALE_CURATOR_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        -- удалить блокировку номера
				delete from order_lock_t t
				where t.order_id = v_order_id;
        -- удаляем заказ
        DELETE FROM ORDER_T 
        WHERE ORDER_ID = v_order_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('ORDER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    EXCEPTION
      WHEN NO_DATA_FOUND THEN NULL;
      WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
    END;
 
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

    -- удаляем SALE-CURATOR для лицевого счета
    DELETE FROM SALE_CURATOR_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SALE_CURATOR_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- удаляем лицевой счет
    DELETE FROM ACCOUNT_T
    WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      
    -- удаляем договор
    SELECT COUNT(*) INTO v_count
    FROM ACCOUNT_PROFILE_T
    WHERE CONTRACT_ID = v_contract_id;
    IF v_count = 0 THEN
      -- удаляем SALE-CURATOR для лицевого счета
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
   
    -- удаляем клиента
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
		-- удалить банковские реквизиты
		delete from customer_bank_t
		where customer_id = v_customer_id;
    -- удаляем данные покупателя
    SELECT COUNT(*) INTO v_count
    FROM ACCOUNT_PROFILE_T
    WHERE CUSTOMER_ID = v_customer_id;
    IF v_count = 0 THEN
      -- удаляем адресные данные
      DELETE FROM CUSTOMER_ADDRESS_T
      WHERE CUSTOMER_ID = v_customer_id;
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('CUSTOMER_ADDRESS_T.JUR: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      -- удаляем покупателя
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
