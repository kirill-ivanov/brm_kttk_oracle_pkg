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
    -- PHONES: /home/billsrv/bcr/phones.csv
    PROCEDURE Brm_phones_to_file;

    -- ==============================================================================
    -- Заполнение промежуточных таблиц в схеме INFRANET 
    -- ------------------------------------------------------------------------------
    -- заполнение всех таблиц оптом:
    PROCEDURE Load_bcr_tables(p_period_id IN INTEGER);

    -- заполнение каждой таблицы отдельно:
    
    -- INFRANET.BRM_ITEM_T;
    PROCEDURE Brm_items_to_table(p_period_id IN INTEGER);

    -- INFRANET.BRM_BILL_T;
    PROCEDURE Brm_bills_to_table(p_period_id IN INTEGER);

    -- INFRANET.BRM_ORDER_T;
    PROCEDURE Brm_orders_to_table;
    
    -- INFRANET.BRM_ACCOUNT_T;
    PROCEDURE Brm_accounts_to_table;

    -- INFRANET.BRM_CLIENT_T;
    PROCEDURE Brm_clients_to_table;
    
    -- INFRANET.BRM_DELIVERY_T;
    PROCEDURE Brm_delivery_to_table;
    

END PK03_BCR_REPORT;
/
CREATE OR REPLACE PACKAGE BODY PK03_BCR_REPORT IS


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- PHONES
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Brm_phones_to_file
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Brm_phones_to_file';
BEGIN
    PIN.PK402_BCR_FILE.Brm_phones_to_file;
    NULL;
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ==============================================================================
-- Заполнение промежуточных таблиц в схеме INFRANET 
-- ------------------------------------------------------------------------------
-- заполнение всех таблиц оптом:
PROCEDURE Load_bcr_tables(p_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Load_bcr_tables';
BEGIN
    PIN.PK402_BCR_FILE.Load_bcr_tables(p_period_id);
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- заполнение каждой таблицы отдельно:
    
-- INFRANET.BRM_ITEM_T;
PROCEDURE Brm_items_to_table(p_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Brm_items_to_table';
BEGIN
    PIN.PK402_BCR_FILE.Brm_items_to_table(p_period_id);
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- INFRANET.BRM_BILL_T;
PROCEDURE Brm_bills_to_table(p_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Brm_bills_to_table';
BEGIN
    PIN.PK402_BCR_FILE.Brm_bills_to_table(p_period_id);
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- INFRANET.BRM_ORDER_T;
PROCEDURE Brm_orders_to_table
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Brm_orders_to_table';
BEGIN
    PIN.PK402_BCR_FILE.Brm_orders_to_table;
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- INFRANET.BRM_ACCOUNT_T;
PROCEDURE Brm_accounts_to_table
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Brm_accounts_to_table';
BEGIN
    PIN.PK402_BCR_FILE.Brm_accounts_to_table;
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- INFRANET.BRM_CLIENT_T;
PROCEDURE Brm_clients_to_table
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Brm_clients_to_table';
BEGIN
    PIN.PK402_BCR_FILE.Brm_clients_to_table;
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- INFRANET.BRM_DELIVERY_T;
PROCEDURE Brm_delivery_to_table
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Brm_delivery_to_table';
BEGIN
    PIN.PK402_BCR_FILE.Brm_delivery_to_table;
EXCEPTION
    WHEN OTHERS THEN
        PIN.Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

END PK03_BCR_REPORT;
/
