CREATE OR REPLACE PACKAGE PK03_BCR_REPORT IS
  
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK03_BCR_REPORT';
    -- ==============================================================================
    -- Интерфейс для выгрузки данных в INFRANET
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- IP: 10.110.32.160
    -- protocol: FTP
    -- user: billsrv
    -- passwd: portal6.5
    -- /home/billsrv/data   -- (BS_DATA_DIR)
    -- /home/billsrv/bcr    -- (BCR_DIR) данные для BCR
    -- /home/billsrv/agent  -- (AGENT_DIR) экспорт агентских BDR
    -- ==============================================================================

    -- ITEMS: /home/billsrv/bcr/items.csv
    PROCEDURE Brm_items_to_file(p_period_id IN INTEGER);

    -- BILLS: /home/billsrv/bcr/bills.csv
    PROCEDURE Brm_bills_to_file(p_period_id IN INTEGER);
      
    -- ORDERS: /home/billsrv/bcr/orders.csv
    PROCEDURE Brm_orders_to_file;

    -- ACCOUNTS: /home/billsrv/bcr/accounts.csv
    PROCEDURE Brm_accounts_to_file;

    -- CLIENTS: /home/billsrv/bcr/clients.csv
    PROCEDURE Brm_clients_to_file;

    -- PHONES: /home/billsrv/bcr/phones.csv
    PROCEDURE Brm_phones_to_file;

    -- DELIVERY: /home/billsrv/bcr/delivery.csv
    PROCEDURE Brm_delivery_to_file;


END PK03_BCR_REPORT;
/
CREATE OR REPLACE PACKAGE BODY PK03_BCR_REPORT IS


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ITEMS
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Brm_items_to_file(p_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Brm_items_to_file';
BEGIN
    PIN.PK402_BCR_FILE.Brm_items_to_file(p_period_id);
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- BILLS
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Brm_bills_to_file(p_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Brm_bills_to_file';
BEGIN
    PIN.PK402_BCR_FILE.Brm_bills_to_file(p_period_id);
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;
      
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ORDERS
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Brm_orders_to_file
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Brm_orders_to_file';
BEGIN
    PIN.PK402_BCR_FILE.Brm_orders_to_file;
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ACCOUNTS
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Brm_accounts_to_file
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Brm_accounts_to_file';
BEGIN
    PIN.PK402_BCR_FILE.Brm_accounts_to_file;
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- CLIENTS: /home/billsrv/bcr/clients.csv
PROCEDURE Brm_clients_to_file
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Brm_clients_to_file';
BEGIN
    PIN.PK402_BCR_FILE.Brm_clients_to_file;
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- PHONES
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Brm_phones_to_file
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Brm_phones_to_file';
BEGIN
    PIN.PK402_BCR_FILE.Brm_phones_to_file;
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- DELIVERY
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Brm_delivery_to_file
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Brm_delivery_to_file';
BEGIN
    PIN.PK402_BCR_FILE.Brm_delivery_to_file;
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


END PK03_BCR_REPORT;
/
