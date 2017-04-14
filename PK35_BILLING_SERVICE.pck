CREATE OR REPLACE PACKAGE PK35_BILLING_SERVICE
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK35_BILLING_SERVICE';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    --===============================================================================
    --                  С Л У Ж Е Б Н Ы Е     П Р О Ц Е Д У Р Ы 
    --===============================================================================
    -- Собрать статистику по таблице
    --
    PROCEDURE Gather_Table_Stat(l_Tab_Name varchar2);
    PROCEDURE Drop_constraint(p_table IN VARCHAR2, p_constraint IN VARCHAR2);
    PROCEDURE Add_constraint(p_ddl IN VARCHAR2);
    PROCEDURE Run_DDL(p_ddl IN VARCHAR2);
    --
    -- Ограничения целостности ( CONSTRAINTS )
    --
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
    -- REP_PERIOD_INFO_T
    PROCEDURE Rep_period_info_t_drop_fk;
    PROCEDURE Rep_period_info_t_add_fk;
    
END PK35_BILLING_SERVICE;
/
CREATE OR REPLACE PACKAGE BODY PK35_BILLING_SERVICE
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
--                  С Л У Ж Е Б Н Ы Е     П Р О Ц Е Д У Р Ы 
--============================================================================================
PROCEDURE Run_DDL(p_ddl IN VARCHAR2) IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Run_DDL';
BEGIN
    EXECUTE IMMEDIATE p_ddl;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.Write_error('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE Drop_constraint(p_table IN VARCHAR2, p_constraint IN VARCHAR2) IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Drop_constraint';
BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE '||p_table||' DROP CONSTRAINT '||p_constraint;
    Pk01_Syslog.Write_msg(p_constraint||' - deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.Write_error('ERROR.'||p_constraint, c_PkgName||'.'||v_prcName );
END;

PROCEDURE Add_constraint(p_ddl IN VARCHAR2) IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Add_constraint';
    v_pos_from INTEGER;
    v_pos_to   INTEGER;
    v_cname    VARCHAR2(200);
BEGIN
    v_pos_from := INSTR(p_ddl, 'CONSTRAINT ');
    v_pos_to   := INSTR(p_ddl, 'FOREIGN KEY', v_pos_from);
    v_cname    := TRIM(SUBSTR(p_ddl, v_pos_from + 11, v_pos_to-(v_pos_from + 11)));
    v_cname    := TRIM(TRAILING CHR(13) FROM v_cname);
    v_cname    := TRIM(TRAILING CHR(10) FROM v_cname);
    EXECUTE IMMEDIATE p_ddl;
    Pk01_Syslog.Write_msg(v_cname||' - add', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.Write_error('ERROR.'||v_cname, c_PkgName||'.'||v_prcName );
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
    Drop_constraint('BILLINFO_T','BILLINFO_T_ACCOUNT_T_FK');
    Drop_constraint('BILLINFO_T','BILLINFO_T_BILL_T_FK');
    Drop_constraint('BILLINFO_T','BILLINFO_T_CURRENCY_T_FK');
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
    
    Add_constraint('ALTER TABLE PIN.BILLINFO_T ADD (
      CONSTRAINT BILLINFO_T_ACCOUNT_T_FK 
      FOREIGN KEY (ACCOUNT_ID) 
      REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
      ENABLE VALIDATE)');
      
    Add_constraint('ALTER TABLE PIN.BILLINFO_T ADD (
      CONSTRAINT BILLINFO_T_BILL_T_FK 
      FOREIGN KEY (BILL_ID, PERIOD_ID) 
      REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
      ENABLE VALIDATE)');
      
    Add_constraint('ALTER TABLE PIN.BILLINFO_T ADD (
      CONSTRAINT BILLINFO_T_CURRENCY_T_FK 
      FOREIGN KEY (CURRENCY_ID) 
      REFERENCES PIN.CURRENCY_T (CURRENCY_ID)
      ENABLE VALIDATE)');
      
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
    Drop_constraint('BILL_T','BILL_T_ACCOUNT_T_FK');
    Drop_constraint('BILL_T','BILL_T_PERIOD_T_FK');
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
  
    Add_constraint( 'ALTER TABLE PIN.BILL_T ADD (
      CONSTRAINT BILL_T_ACCOUNT_T_FK 
      FOREIGN KEY (ACCOUNT_ID) 
      REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
      ENABLE VALIDATE)');
      
    Add_constraint( 'ALTER TABLE PIN.BILL_T ADD (
      CONSTRAINT BILL_T_PERIOD_T_FK 
      FOREIGN KEY (REP_PERIOD_ID) 
      REFERENCES PIN.PERIOD_T (PERIOD_ID)
      ENABLE VALIDATE)');
      
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
    Drop_constraint('ITEM_T','ITEM_T_BILL_T_FK');
    Drop_constraint('ITEM_T','ITEM_T_INVOICE_ITEM_T_FK');
    Drop_constraint('ITEM_T','ITEM_T_ORDER_T_FK');
    Drop_constraint('ITEM_T','ITEM_T_SERVICE_T_FK');
    Drop_constraint('ITEM_T','ITEM_T_SUBSERVICE_T_FK');
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

    Add_constraint('ALTER TABLE PIN.ITEM_T ADD (
      CONSTRAINT ITEM_T_BILL_T_FK 
      FOREIGN KEY (BILL_ID, REP_PERIOD_ID) 
      REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
      ENABLE VALIDATE)');
      
    Add_constraint('ALTER TABLE PIN.ITEM_T ADD (
      CONSTRAINT ITEM_T_INVOICE_ITEM_T_FK 
      FOREIGN KEY (BILL_ID, REP_PERIOD_ID, INV_ITEM_ID) 
      REFERENCES PIN.INVOICE_ITEM_T (BILL_ID,REP_PERIOD_ID,INV_ITEM_ID)
      ENABLE VALIDATE)');
      
    Add_constraint('ALTER TABLE PIN.ITEM_T ADD (
      CONSTRAINT ITEM_T_ORDER_T_FK 
      FOREIGN KEY (ORDER_ID) 
      REFERENCES PIN.ORDER_T (ORDER_ID)
      ENABLE VALIDATE)');
    
    Add_constraint('ALTER TABLE PIN.ITEM_T ADD (
      CONSTRAINT ITEM_T_SERVICE_T_FK 
      FOREIGN KEY (SERVICE_ID) 
      REFERENCES PIN.SERVICE_T (SERVICE_ID)
      ENABLE VALIDATE)');
      
    Add_constraint('ALTER TABLE PIN.ITEM_T ADD (
      CONSTRAINT ITEM_T_SUBSERVICE_T_FK 
      FOREIGN KEY (SUBSERVICE_ID) 
      REFERENCES PIN.SUBSERVICE_T (SUBSERVICE_ID)
      ENABLE VALIDATE)');

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
    Drop_constraint('INVOICE_ITEM_T','INVOICE_ITEM_T_BILL_T_FK');
    Drop_constraint('INVOICE_ITEM_T','INVOICE_ITEM_T_SERVICE_T_FK');
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

    Add_constraint('ALTER TABLE PIN.INVOICE_ITEM_T ADD (
      CONSTRAINT INVOICE_ITEM_T_BILL_T_FK 
      FOREIGN KEY (BILL_ID, REP_PERIOD_ID) 
      REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
      ENABLE VALIDATE)');

    Add_constraint('ALTER TABLE PIN.INVOICE_ITEM_T ADD (
      CONSTRAINT INVOICE_ITEM_T_SERVICE_T_FK 
      FOREIGN KEY (SERVICE_ID) 
      REFERENCES PIN.SERVICE_T (SERVICE_ID)
      ENABLE VALIDATE)');
      
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
    Drop_constraint('PAY_TRANSFER_T','PAY_TRANSFER_ID_BILL_T_FK');
    Drop_constraint('PAY_TRANSFER_T','PAY_TRANSFER_ID_PAYMENT_T_FK');
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
    
    Add_constraint('ALTER TABLE PIN.PAY_TRANSFER_T ADD (
      CONSTRAINT PAY_TRANSFER_ID_BILL_T_FK 
      FOREIGN KEY (BILL_ID, REP_PERIOD_ID) 
      REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
      ENABLE VALIDATE)');
  
    Add_constraint('ALTER TABLE PIN.PAY_TRANSFER_T ADD (
      CONSTRAINT PAY_TRANSFER_ID_PAYMENT_T_FK 
      FOREIGN KEY (PAYMENT_ID, PAY_PERIOD_ID) 
      REFERENCES PIN.PAYMENT_T (PAYMENT_ID,REP_PERIOD_ID)
      ENABLE VALIDATE)');

    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  REP_PERIOD_INFO_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rep_period_info_t_drop_fk
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Rep_period_info_t_drop_fk';
BEGIN 
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Drop_constraint('REP_PERIOD_INFO_T','REP_PREIOD_INFO_ACC_T_FK');
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
PROCEDURE Rep_period_info_t_add_fk
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Billinfo_t_add_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
   
    Add_constraint('ALTER TABLE PIN.REP_PERIOD_INFO_T ADD (
      CONSTRAINT REP_PREIOD_INFO_ACC_T_FK 
      FOREIGN KEY (ACCOUNT_ID) 
      REFERENCES PIN.ACCOUNT_T (ACCOUNT_ID)
      ENABLE VALIDATE)');

    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;





END PK35_BILLING_SERVICE;
/
