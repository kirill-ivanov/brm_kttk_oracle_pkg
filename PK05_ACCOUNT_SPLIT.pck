CREATE OR REPLACE PACKAGE PK05_ACCOUNT_SPLIT
IS
    --
    -- ���������� �/� �� �������
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK05_ACCOUNT_SPLIT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    -- ---------------------------------------------------------------------------- --
    -- ������� ����� ���������� �/�
    -- ���� p_dst_account_no �� ����� ����������� �����������
    -- ---------------------------------------------------------------------------- --
    FUNCTION Account_create_like (
                 p_src_account_id IN INTEGER,
                 p_dst_account_no IN VARCHAR2 DEFAULT NULL
              ) RETURN INTEGER;    
    
    -- ---------------------------------------------------------------------------- --
    -- ������� ����� ���������� �/�
    -- ���� p_dst_account_no �� ����� ����������� �����������
    -- ---------------------------------------------------------------------------- --
    FUNCTION Move_Order (
                 p_src_order_id   IN INTEGER,
                 p_dst_account_id IN INTEGER,
                 p_date_from      IN DATE
              ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ���������� �� ������ � ������ �� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Move_charge_usg (
                 p_src_order_id   IN INTEGER,
                 p_dst_order_id   IN INTEGER,
                 p_period_id      IN INTEGER
              );
    
END PK05_ACCOUNT_SPLIT;
/
CREATE OR REPLACE PACKAGE BODY PK05_ACCOUNT_SPLIT
IS

-- ---------------------------------------------------------------------------- --
-- ������� ����� ���������� �/�
-- ���� p_dst_account_no �� ����� ����������� �����������
-- ---------------------------------------------------------------------------- --
FUNCTION Account_create_like (
             p_src_account_id IN INTEGER,
             p_dst_account_no IN VARCHAR2 DEFAULT NULL
          ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Account_create_like';
    v_dst_account_no ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_account_type   ACCOUNT_T.ACCOUNT_TYPE%TYPE;
    v_account_id     ACCOUNT_T.ACCOUNT_ID%TYPE;
    v_profile_id     ACCOUNT_PROFILE_T.PROFILE_ID%TYPE;
BEGIN
    Pk01_Syslog.Write_msg('Start, p_src_account_id = '||p_src_account_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ������ ���.����������
    SELECT A.ACCOUNT_TYPE 
      INTO v_account_type
      FROM ACCOUNT_T A
     WHERE A.ACCOUNT_ID = p_src_account_id;
    --
    -- ��������� ����� ����� ����������
    IF p_dst_account_no IS NOT NULL THEN
        v_dst_account_no := p_dst_account_no;
    ELSE
        v_dst_account_no := Pk05_Account.New_std_account_no(v_account_type);
    END IF;
    --
    -- ������� ����� �/�
    v_account_id := Pk02_Poid.Next_account_id;
    --
    INSERT INTO ACCOUNT_T (
        ACCOUNT_ID, PARENT_ID, ACCOUNT_NO, 
        ACCOUNT_TYPE, CURRENCY_ID, CREATE_DATE, 
        STATUS, BALANCE, BALANCE_DATE, EXTERNAL_ID, 
        SOURCE_ID, NOTES, BILLING_ID, CURRENCY_CONVERSION_ID, 
        COMMENTARY, EXTERNAL_NO, IDL_ENB
    )
    SELECT 
        v_account_id, PARENT_ID, v_dst_account_no, 
        ACCOUNT_TYPE, CURRENCY_ID, SYSDATE, STATUS, BALANCE, BALANCE_DATE, EXTERNAL_ID, 
        SOURCE_ID, 'Duplicate', BILLING_ID, CURRENCY_CONVERSION_ID, 
        COMMENTARY, EXTERNAL_NO, IDL_ENB
      FROM ACCOUNT_T A
     WHERE A.ACCOUNT_ID = p_src_account_id
    ;
    -- ������� ����� �������    
    v_profile_id := Pk02_Poid.Next_account_profile_id;
    --
    INSERT INTO ACCOUNT_PROFILE_T AP(
        PROFILE_ID, ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, 
        SUBSCRIBER_ID, CONTRACTOR_ID, BRANCH_ID, AGENT_ID, 
        CONTRACTOR_BANK_ID, VAT, DATE_FROM, DATE_TO, 
        CUSTOMER_PAYER_ID, BRAND_ID
    ) 
    SELECT 
        v_profile_id, v_account_id, CONTRACT_ID, CUSTOMER_ID, 
        SUBSCRIBER_ID, CONTRACTOR_ID, BRANCH_ID, AGENT_ID, 
        CONTRACTOR_BANK_ID, VAT, DATE_FROM, DATE_TO, 
        CUSTOMER_PAYER_ID, BRAND_ID
      FROM ACCOUNT_PROFILE_T AP
     WHERE AP.ACCOUNT_ID = p_src_account_id
       AND AP.DATE_FROM <= SYSDATE
       AND (AP.DATE_TO IS NULL OR SYSDATE <= AP.DATE_TO )
    ;
    -- ������� ����� �������
    INSERT INTO ACCOUNT_CONTACT_T (
           CONTACT_ID, ACCOUNT_ID, ADDRESS_TYPE, 
           COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON, PHONES, FAX, EMAIL, 
           DATE_FROM, DATE_TO, NOTES, MODIFY_DATE
    )
    SELECT SQ_ADDRESS_ID.NEXTVAL, v_account_id ACCOUNT_ID, ADDRESS_TYPE, 
           COUNTRY, ZIP, STATE, CITY, ADDRESS, PERSON, PHONES, FAX, EMAIL, 
           DATE_FROM, DATE_TO, NOTES, MODIFY_DATE
      FROM ACCOUNT_CONTACT_T AC
     WHERE AC.ACCOUNT_ID = p_src_account_id;
     
    -- ����������� ���������
    INSERT INTO ACCOUNT_DOCUMENTS_T(
           ACCOUNT_ID, DOC_BILL, DOC_DETAIL, DOC_CALLS, 
           DELIVERY_METHOD_ID, ORDER_ID, CONTRACTOR_ID, EMAIL
    )
    SELECT v_account_id, DOC_BILL, DOC_DETAIL, DOC_CALLS, 
           DELIVERY_METHOD_ID, ORDER_ID, CONTRACTOR_ID, EMAIL 
     FROM ACCOUNT_DOCUMENTS_T AC
     WHERE AC.ACCOUNT_ID = p_src_account_id;

    -- ��������� ��� �������� � ������� �����������
    Pk01_Syslog.Write_msg('Stop, account_id = '||v_account_id
                        ||', account_no = '||v_dst_account_no, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ���������� ���������
    RETURN v_account_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------------- --
-- ������� ����� ���������� �/�
-- ���� p_dst_account_no �� ����� ����������� �����������
-- ---------------------------------------------------------------------------- --
FUNCTION Move_Order (
             p_src_order_id   IN INTEGER,
             p_dst_account_id IN INTEGER,
             p_date_from      IN DATE
          ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Move_Order';
    v_order_id       INTEGER;    
    v_order_no_old   ORDER_T.ORDER_NO%TYPE;
    v_order_no_new   ORDER_T.ORDER_NO%TYPE;
    v_account_no_old ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_account_no_new ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_date_from      DATE;
BEGIN
    Pk01_Syslog.Write_msg('Start, from src_order_id = '||p_src_order_id
                         ||', to dst_account_id = p_dst_account_id'
                          , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ����, �� ������ ������
    v_date_from := TRUNC(p_date_from);

    -- �������� ����� ������ � ����� �/�
    SELECT O.ORDER_NO, A.ACCOUNT_NO INTO v_order_no_old, v_account_no_old
      FROM ORDER_T O, ACCOUNT_T A
    WHERE O.ORDER_ID   = p_src_order_id
      AND O.ACCOUNT_ID = A.ACCOUNT_ID;
      
    -- �������� ����� ������ �/�
    SELECT A.ACCOUNT_NO INTO v_account_no_new
      FROM ACCOUNT_T A
     WHERE A.ACCOUNT_ID = p_dst_account_id;

    -- ��������� ������ ������ ������ �� ������: "ACCOUNT_NO-ORDER_NO"
    IF v_order_no_old LIKE v_account_no_old||'-%' THEN
        -- �������� ���������� ����� ������
        v_order_no_new := SUBSTR(v_order_no_old, LENGTH(v_account_no_old)+2);
        -- ��������� ��������� ����� ������
        v_order_no_new := v_account_no_new||'-'||v_order_no_new;
    ELSE
        -- ��������� ���������� ����� ������
        v_order_no_new := v_order_no_old;
        -- ��������� ��������� ����� ��� ������� ������
        v_order_no_old := v_account_no_old||'-'||v_order_no_old;
    END IF;

    -- ��������� ������ ����� ������
    UPDATE ORDER_T
      SET ORDER_NO = v_order_no_old
    WHERE ORDER_ID = p_src_order_id;
    
    -- �������� �����
    v_order_id := Pk02_Poid.Next_order_id;               
    INSERT INTO ORDER_T (
        ORDER_ID, ORDER_NO, ACCOUNT_ID, SERVICE_ID, RATEPLAN_ID, 
        DATE_FROM, DATE_TO, CREATE_DATE, MODIFY_DATE, TIME_ZONE, 
        STATUS, NOTES, AGENT_RATEPLAN_ID, EXTERNAL_ID, 
        PARENT_ID, EXT_ORDER_NO, SERVICE_ALIAS
    )
    SELECT 
        v_order_id, v_order_no_new, p_dst_account_id, SERVICE_ID, RATEPLAN_ID, 
        v_date_from, DATE_TO, CREATE_DATE, MODIFY_DATE, TIME_ZONE, 
        STATUS, NOTES, AGENT_RATEPLAN_ID, EXTERNAL_ID, 
        PARENT_ID, EXT_ORDER_NO, SERVICE_ALIAS
      FROM ORDER_T
     WHERE ORDER_ID = p_src_order_id
       AND (DATE_TO IS NULL OR v_date_from < DATE_TO );
     
    -- �������� ���������� ������
    INSERT INTO ORDER_BODY_T (
        ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
        DATE_FROM, DATE_TO, 
        RATEPLAN_ID, RATE_VALUE, FREE_VALUE, RATE_RULE_ID, RATE_LEVEL_ID, TAX_INCL, 
        QUANTITY, CREATE_DATE, MODIFY_DATE, CURRENCY_ID, NOTES, RATEPLAN_PCK_ID
    )
    SELECT 
        Pk02_Poid.Next_order_body_id, v_order_id, SUBSERVICE_ID, CHARGE_TYPE, 
        v_date_from, DATE_TO, 
        RATEPLAN_ID, RATE_VALUE, FREE_VALUE, RATE_RULE_ID, RATE_LEVEL_ID, TAX_INCL, 
        QUANTITY, CREATE_DATE, MODIFY_DATE, CURRENCY_ID, NOTES, RATEPLAN_PCK_ID 
      FROM ORDER_BODY_T
     WHERE ORDER_ID = p_src_order_id
       AND (DATE_TO IS NULL OR v_date_from < DATE_TO ); 

    -- �������� ���������� � ������
    INSERT INTO ORDER_INFO_T I (
        ORDER_ID, NETWORK_ID, ADD_FLR, POINT_SRC, POINT_DST, SPEED_STR, 
        ROUTER_ZONE, SPEED_VALUE, SPEED_UNIT_ID, DOWNTIME_FREE, IP_ADDRESS, SWITCH_ID
    )
    SELECT 
        v_order_id, NETWORK_ID, ADD_FLR, POINT_SRC, POINT_DST, SPEED_STR, 
        ROUTER_ZONE, SPEED_VALUE, SPEED_UNIT_ID, DOWNTIME_FREE, IP_ADDRESS, SWITCH_ID
      FROM ORDER_INFO_T
     WHERE ORDER_ID = p_src_order_id;

    -- �������� ����������
    INSERT INTO ORDER_LOCK_T (
        ORDER_LOCK_ID, ORDER_ID, LOCK_TYPE_ID, DATE_FROM, DATE_TO, 
        CREATE_DATE, LOCKED_BY, UNLOCKED_BY, LOCK_REASON, UNLOCK_REASON
    )
    SELECT SQ_ORDER_ID.NEXTVAL, v_order_id, LOCK_TYPE_ID, DATE_FROM, DATE_TO, 
           CREATE_DATE, LOCKED_BY, UNLOCKED_BY, LOCK_REASON, UNLOCK_REASON
      FROM ORDER_LOCK_T L
     WHERE ORDER_ID = p_src_order_id
       AND (DATE_TO IS NULL OR v_date_from < DATE_TO );
    
    -- �������� ��������
    INSERT INTO ORDER_PHONES_T  (
        ORDER_ID, PHONE_NUMBER, DATE_FROM, DATE_TO, ADDRESS_ID, DESCR
    )
    SELECT v_order_id, PHONE_NUMBER, DATE_FROM, DATE_TO, ADDRESS_ID, DESCR
      FROM ORDER_PHONES_T 
     WHERE ORDER_ID = p_src_order_id
       AND (DATE_TO IS NULL OR v_date_from < DATE_TO );
     
    -- ����������� ��������� ������
    INSERT INTO ORDER_SWTG_T (
        ORDER_ID, SWITCH_ID, TRUNKGROUP, TRUNKGROUP_NO, 
        DATE_FROM, DATE_TO, ORDER_SWTG_ID
    )
    SELECT 
        v_order_id, SWITCH_ID, TRUNKGROUP, TRUNKGROUP_NO, 
        DATE_FROM, DATE_TO, ORDER_SWTG_ID
      FROM ORDER_SWTG_T
     WHERE ORDER_ID = p_src_order_id
       AND (DATE_TO IS NULL OR v_date_from < DATE_TO );

    -- ��������� ������ �����
    UPDATE ORDER_T
      SET DATE_TO = v_date_from - 1/86400, STATUS = 'CLOSED'
    WHERE ORDER_ID = p_src_order_id;

    UPDATE ORDER_BODY_T
      SET DATE_TO = v_date_from - 1/86400
    WHERE ORDER_ID = p_src_order_id;

    UPDATE ORDER_PHONES_T
      SET DATE_TO = v_date_from - 1/86400
    WHERE ORDER_ID = p_src_order_id;
    
    UPDATE ORDER_SWTG_T
      SET DATE_TO = v_date_from - 1/86400
    WHERE ORDER_ID = p_src_order_id;

    Pk01_Syslog.Write_msg('Stop, dst_order_id = '||v_order_id
                          , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ���������� ���������
    RETURN v_order_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ���������� �� ������ � ������ �� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Move_charge_usg (
             p_src_order_id   IN INTEGER,
             p_dst_order_id   IN INTEGER,
             p_period_id      IN INTEGER
          )
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Move_charge_usg';
    v_src_account_id  INTEGER;
    v_src_bill_id     INTEGER;
    v_src_bill_status BILL_T.BILL_STATUS%TYPE;
    v_dst_account_id  INTEGER;
    v_dst_bill_id     INTEGER;
    v_dst_bill_no     BILL_T.BILL_NO%TYPE;
    v_dst_date_from   DATE;
    v_task_id         INTEGER;
BEGIN
    --
    SELECT O.ACCOUNT_ID
      INTO v_src_account_id
      FROM ORDER_T O
     WHERE O.ORDER_ID = p_src_order_id;  
    --
    SELECT O.ACCOUNT_ID, O.DATE_FROM
      INTO v_dst_account_id, v_dst_date_from
      FROM ORDER_T O
     WHERE O.ORDER_ID = p_dst_order_id;

    -- ��������� ���� �� ���������� �� ������� �� ��������� ������
    SELECT DISTINCT B.BILL_ID, B.BILL_STATUS 
      INTO v_src_bill_id, v_src_bill_status
      FROM ITEM_T I, BILL_T B
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.ORDER_ID      = p_src_order_id
       AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND I.BILL_ID       = B.BILL_ID
       AND B.BILL_TYPE     = 'B';
    
    BEGIN
        SELECT BILL_ID, BILL_NO INTO v_dst_bill_id, v_dst_bill_no
          FROM BILL_T
        WHERE ACCOUNT_ID = v_dst_account_id
           AND REP_PERIOD_ID = p_period_id
           AND BILL_TYPE = 'B';
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
              -- ������� ���������� ����
              v_dst_bill_id := Pk02_Poid.Next_bill_id;
              v_dst_bill_no := Pk07_Bill.Next_rec_bill_no( v_dst_account_id, p_period_id );
              --
              INSERT INTO BILL_T B (
                  BILL_ID, REP_PERIOD_ID, ACCOUNT_ID, BILL_NO, BILL_DATE, BILL_TYPE, 
                  BILL_STATUS, CURRENCY_ID, TOTAL, GROSS, TAX, RECVD, DUE, DUE_DATE, 
                  PAID_TO, PREV_BILL_ID, PREV_BILL_PERIOD_ID, 
                  NEXT_BILL_ID, NEXT_BILL_PERIOD_ID, CALC_DATE, 
                  ACT_DATE_FROM, ACT_DATE_TO, NOTES, DELIVERY_DATE, 
                  ADJUSTED, CONTRACT_ID, VAT, CREATE_DATE, 
                  PROFILE_ID, CONTRACTOR_ID, CONTRACTOR_BANK_ID, 
                  PONY_EXPRESS_KONVERT_ID, INVOICE_RULE_ID, ERR_CODE_ID
              )
              SELECT 
                  v_dst_bill_id, REP_PERIOD_ID, v_dst_account_id, 
                  v_dst_bill_no, BILL_DATE, BILL_TYPE, 
                  BILL_STATUS, CURRENCY_ID, TOTAL, GROSS, TAX, RECVD, DUE, DUE_DATE, 
                  PAID_TO, PREV_BILL_ID, PREV_BILL_PERIOD_ID, 
                  NEXT_BILL_ID, NEXT_BILL_PERIOD_ID, SYSDATE, 
                  ACT_DATE_FROM, ACT_DATE_TO, NOTES, DELIVERY_DATE, 
                  ADJUSTED, CONTRACT_ID, VAT, CREATE_DATE, 
                  PROFILE_ID, CONTRACTOR_ID, CONTRACTOR_BANK_ID, 
                  PONY_EXPRESS_KONVERT_ID, INVOICE_RULE_ID, ERR_CODE_ID 
                FROM BILL_T B
               WHERE B.REP_PERIOD_ID = p_period_id
                 AND B.ACCOUNT_ID = v_src_account_id
                 AND B.BILL_TYPE  = 'B'
              ;
    END;       

    -- ��������� item-�
    INSERT INTO ITEM_T I (
        BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
        ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, ITEM_TOTAL, 
        RECVD, DATE_FROM, DATE_TO, ITEM_STATUS, 
        CREATE_DATE, LAST_MODIFIED, REP_GROSS, REP_TAX, TAX_INCL, 
        EXTERNAL_ID, NOTES, ORDER_BODY_ID, DESCR, 
        QUANTITY, ITEM_CURRENCY_ID, BILL_TOTAL
    )
    SELECT 
        v_dst_bill_id, REP_PERIOD_ID, Pk02_Poid.Next_item_id, ITEM_TYPE, NULL, 
        p_dst_order_id, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, ITEM_TOTAL, 
        RECVD, DATE_FROM, DATE_TO, 'OPEN', 
        CREATE_DATE, LAST_MODIFIED, REP_GROSS, REP_TAX, TAX_INCL, 
        EXTERNAL_ID, NOTES, ORDER_BODY_ID, DESCR, 
        QUANTITY, ITEM_CURRENCY_ID, BILL_TOTAL 
      FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.BILL_ID       = v_src_bill_id
       AND I.ORDER_ID      = p_src_order_id
       AND I.DATE_FROM    >= v_dst_date_from
    ;
 
    -- ������� item-s �� ����� ���������
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.BILL_ID       = v_src_bill_id
       AND I.ORDER_ID      = p_src_order_id
       AND I.DATE_FROM    >= v_dst_date_from;

    IF v_src_bill_status = 'READY' THEN
        -- ������ ����� � ������� �� ���������
        v_task_id := PK30_BILLING_QUEUE.Open_task;
        -- 
        INSERT INTO BILLING_QUEUE_T(BILL_ID, ACCOUNT_ID, ORDER_ID, TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID)
        SELECT B.BILL_ID, B.ACCOUNT_ID, NULL ORDER_ID, v_task_id TASK_ID, B.REP_PERIOD_ID, B.REP_PERIOD_ID
          FROM BILL_T B
         WHERE B.REP_PERIOD_ID = p_period_id
           AND B.BILL_ID IN ( v_src_bill_id, v_dst_bill_id );

        -- �������������� ����� � ����������
        Pk30_Billing.Rollback_billing(v_task_id);
        
        -- ��������������� ����� � ����������
        Pk30_Billing.Remake_billing(v_task_id);
        
        -- ��������� �������        
        PK30_BILLING_QUEUE.Close_task(v_task_id);
      
    END IF;
    
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


END PK05_ACCOUNT_SPLIT;
/
