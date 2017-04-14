CREATE OR REPLACE PACKAGE PK33_BILLING_ACCOUNT
IS
    --
    -- ����� ��� ��������� �������� ������������/���������������� ������
    -- ��� ���������� �������� �����
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK33_BILLING_ACCOUNT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- ===================================================================================
    -- ��������� ���� � ������� �� ���������
    -- ===================================================================================
    FUNCTION  push_Bill( 
                 p_bill_id        IN INTEGER,
                 p_bill_period_id IN INTEGER,
                 p_data_period_id IN INTEGER DEFAULT NULL
              ) RETURN INTEGER;

    -- ===================================================================================
    -- ��������������� ���������� �����, ������ � ������ ������������ �������
    -- ===================================================================================
    -- ������������� ���� ������������ �������
    --
    PROCEDURE Rollback_Bill(
                p_bill_id IN INTEGER,
                p_period_id IN INTEGER
              );

    -- ==================================================================================== --
    -- ������������ ���������� �����
    -- ==================================================================================== --
    PROCEDURE Make_Bill(
                p_bill_id   IN INTEGER,
                p_period_id IN INTEGER
              );

    -- ===================================================================== --
    -- ������ � �������������� ������������ ������ + ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������������� ������������� ����������,
    -- �� ����������� ��� ��� ����������� ����������� �������
    --
    PROCEDURE Rollback_fixrates(
                p_bill_id   IN INTEGER,
                p_period_id IN INTEGER
              );
        
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ����������� ����� � ������� �� ����������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Charge_fixrates(
                p_bill_id        IN INTEGER,
                p_rep_period_id  IN INTEGER,
                p_data_period_id IN INTEGER
              );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� �������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Account_period_info (
                p_account_id     IN INTEGER,
                p_period_id      IN INTEGER
              );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� �/�
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Refresh_balance (
              p_account_id     IN INTEGER
        );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ����������� �� ���� ��������, ������� �� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Delete_paytransfer_from_bill (
                p_bill_id    IN INTEGER,
                p_period_id  IN INTEGER
              );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ��������������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Recalc_bill (
                p_bill_id         IN INTEGER, -- ID �����-����
                p_bill_period_id  IN INTEGER, -- ID ���������� ������� �����-���� YYYYMM
                p_data_period_id  IN INTEGER  -- ID ���������� ������� ������-���� YYYYMM
              );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ������ ������������ ����� �����-������, ��� ���������� �����
    PROCEDURE Invoice_rules_list( 
                p_recordset IN OUT SYS_REFCURSOR,
                p_bill_id   IN INTEGER DEFAULT NULL -- ���� ���� �� ����� �������� ������ ������
              );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ��� ����� ������� ������������ ����� �����-������
    PROCEDURE Set_bill_invoice_rule( 
                p_bill_id         IN INTEGER,
                p_bill_period_id  IN INTEGER,
                p_invoice_rule_id IN INTEGER  -- DICTIONARY_T(77)
              );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ��� �/����� ������� ������������ ����� �����-������ (������������)
    PROCEDURE Set_account_invoice_rule( 
                p_account_id      IN INTEGER,
                p_invoice_rule_id IN INTEGER  -- DICTIONARY_T(77)
              );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ����, ��������� ������� � ����������
    -- �� ��� ��� ���� �������� ����� ������� - ��� ���������
    --
    PROCEDURE Correct_tax_incl( 
                p_bill_id         IN INTEGER,
                p_bill_period_id  IN INTEGER
              );
    
    
END PK33_BILLING_ACCOUNT;
/
CREATE OR REPLACE PACKAGE BODY PK33_BILLING_ACCOUNT
IS

-- ===================================================================================
-- ��������� ���� � ������� �� ���������
-- ===================================================================================
FUNCTION  push_Bill( 
             p_bill_id        IN INTEGER,
             p_bill_period_id IN INTEGER,
             p_data_period_id IN INTEGER DEFAULT NULL
          ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'push_Bill';
    v_task_id    INTEGER;
BEGIN
    --
    v_task_id := PK30_BILLING_QUEUE.Open_task;
    --    
    INSERT INTO BILLING_QUEUE_T (
           BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, 
           TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID
       )
    SELECT B.BILL_ID, B.ACCOUNT_ID, A.BILLING_ID, AP.PROFILE_ID, 
           v_task_id, B.REP_PERIOD_ID, NVL(p_data_period_id, p_bill_period_id)
      FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
     WHERE B.REP_PERIOD_ID = p_bill_period_id
       AND B.BILL_ID       = p_bill_id
       AND B.ACCOUNT_ID    = A.ACCOUNT_ID
       AND B.ACCOUNT_ID    = AP.ACCOUNT_ID
       AND AP.DATE_FROM   <= B.BILL_DATE
       AND (AP.DATE_TO IS NULL OR B.BILL_DATE <= AP.DATE_TO)
    ;
    RETURN v_task_id;
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ===================================================================================
-- ��������� �/���� � ������� �� ���������
-- ===================================================================================
FUNCTION  push_Account( 
             p_account_id IN INTEGER,
             p_period_id  IN INTEGER
          ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'push_Account';
    v_task_id    INTEGER;
BEGIN
    --
    v_task_id := PK30_BILLING_QUEUE.Open_task;
    --    
    INSERT INTO BILLING_QUEUE_T(BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, TASK_ID, REP_PERIOD_ID)
    SELECT B.BILL_ID, B.ACCOUNT_ID, A.BILLING_ID, B.PROFILE_ID, v_task_id, B.REP_PERIOD_ID 
      FROM BILL_T B, ACCOUNT_T A
     WHERE B.REP_PERIOD_ID = p_period_id
       AND A.ACCOUNT_ID    = p_account_id
       AND B.ACCOUNT_ID    = A.ACCOUNT_ID
    ;
    RETURN v_task_id;
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


-- ===================================================================================
-- ��������������� ���������� �����, ������ � ������ ������������ �������
-- ===================================================================================
-- ������������� ���� ������������ �������
--
PROCEDURE Rollback_Bill(
             p_bill_id IN INTEGER,
             p_period_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_Bill';
    v_task_id    INTEGER;
    v_msg_id     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start bill_id = '||p_bill_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������ ���� � ������� �� ���������
    v_task_id := push_Bill(p_bill_id, p_period_id);
    
    -- �������� ����
    Pk30_Billing_Queue.Rollback_bills(v_task_id );
    
    -- ����������� �������
    Pk30_Billing_Queue.Close_task(v_task_id);
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        v_msg_id := Pk01_Syslog.Fn_write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
        Pk30_Billing_Queue.Close_task(v_task_id);
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'msg_id='||v_msg_id||':'||c_PkgName||'.'||v_prcName);
END;

-- ==================================================================================== --
-- ������������ ���������� �����
-- ==================================================================================== --
PROCEDURE Make_Bill(
            p_bill_id   IN INTEGER,
            p_period_id IN INTEGER
          )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Make_Bill';
    v_task_id     INTEGER;
    v_account_id  INTEGER;
    v_msg_id      INTEGER;
    v_balance     NUMBER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start bill_id = '||p_bill_id||', period_id = '||p_period_id, 
                                     c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������������ ��������� ������ � ����� ������ (�� ������ ������)
    Correct_tax_incl(p_bill_id, p_period_id);

    -- ������ ���� � ������� �� ���������
    v_task_id := push_Bill(p_bill_id, p_period_id);

    --  ��������� ���� ����������� ��� ��������� ����������� �� ���������
    Pk36_Billing_Fixrate.Put_ABP_detail( v_task_id );

    -- ��������� ����
    Pk30_Billing_Queue.Close_bills(v_task_id );

    -- ������������ ������ �������� �����
    SELECT B.ACCOUNT_ID
      INTO v_account_id
      FROM BILL_T B
     WHERE B.BILL_ID = p_bill_id
       AND B.REP_PERIOD_ID = p_period_id;
    
    v_balance := PK05_ACCOUNT_BALANCE.Refresh_balance(v_account_id);
    
    -- ����������� �������
    Pk30_Billing_Queue.Close_task(v_task_id);

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        v_msg_id := Pk01_Syslog.Fn_write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
        Pk30_Billing_Queue.Close_task(v_task_id);
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'msg_id='||v_msg_id||':'||c_PkgName||'.'||v_prcName);
END;

-- ===================================================================== --
-- ������ � �������������� ������������ ������ + ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������������� ������������� ����������,
-- �� ����������� ��� ��� ����������� ����������� �������
--
PROCEDURE Rollback_fixrates(
            p_bill_id    IN INTEGER,
            p_period_id  IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_fixrates';
    v_task_id    INTEGER;
    v_msg_id     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������ ���� � ������� �� ���������
    v_task_id := push_Bill(p_bill_id, p_period_id);
    
    Pk30_Billing_Queue.Rollback_fixrates(v_task_id );
    --
    -- ����������� �������
    Pk30_Billing_Queue.Close_task(v_task_id);
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        v_msg_id := Pk01_Syslog.Fn_write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
        Pk30_Billing_Queue.Close_task(v_task_id);
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'msg_id='||v_msg_id||':'||c_PkgName||'.'||v_prcName);
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ����������� ����� � ������� �� ����������� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_fixrates(
            p_bill_id        IN INTEGER,
            p_rep_period_id  IN INTEGER,
            p_data_period_id IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Charge_fixrates';
    v_task_id    INTEGER;
    v_msg_id     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������ ���� � ������� �� ���������
    v_task_id := push_Bill(p_bill_id, p_rep_period_id, p_data_period_id);
    
    Pk36_Billing_Fixrate.Charge_fixrates( v_task_id );

    -- ����������� �������
    Pk30_Billing_Queue.Close_task(v_task_id);
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        v_msg_id := Pk01_Syslog.Fn_write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
        Pk30_Billing_Queue.Close_task(v_task_id);
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'msg_id='||v_msg_id||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ������ �� ������ ��� �������� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Account_period_info (
          p_account_id     IN INTEGER,
          p_period_id      IN INTEGER
    )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Account_period_info';
    v_task_id    INTEGER;
    v_msg_id     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������ ���� � ������� �� ���������
    v_task_id := push_Account(p_account_id, p_period_id);

    -- ���������� ������ �� ������ ��� ���� ������� ������, ��� ���� ������� �� �����
    PK30_BILLING_BASE.Period_info( v_task_id );
    
    -- ����������� �������
    Pk30_Billing_Queue.Close_task(v_task_id);
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        v_msg_id := Pk01_Syslog.Fn_write_error('Stop.ERROR', c_PkgName||'.'||v_prcName );
        Pk30_Billing_Queue.Close_task(v_task_id);
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'msg_id='||v_msg_id||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������� �/�
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Refresh_balance (
          p_account_id     IN INTEGER
    )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Refresh_balance';
    v_balance    NUMBER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_balance := Pk05_Account_Balance.Refresh_balance ( p_account_id );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� ����������� �� ���� ��������, ������� �� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Delete_paytransfer_from_bill (
               p_bill_id    IN INTEGER,
               p_period_id  IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_paytransfer_from_bill';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    PK10_PAYMENTS_TRANSFER.Delete_transfer_bill(p_period_id, p_bill_id);
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ��������������� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Recalc_bill (
               p_bill_id         IN INTEGER, -- ID �����-����
               p_bill_period_id  IN INTEGER, -- ID ���������� ������� �����-���� YYYYMM
               p_data_period_id  IN INTEGER  -- ID ���������� ������� ������-���� YYYYMM
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Recalc_bill';
    v_task_id  INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start dbt_bill_id = '||p_bill_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������������ ��������� ������ � ����� ������ (�� ������ ������)
    Correct_tax_incl(p_bill_id, p_bill_period_id);

    -- ������ ���� � ������� �� ���������
    v_task_id := push_Bill(
                      p_bill_id        => p_bill_id,
                      p_bill_period_id => p_bill_period_id,
                      p_data_period_id => p_data_period_id );

    -- ������� ����������� �� ���� ��������, ������� �� �������
    Pk10_Payments_Transfer.Delete_transfer_bill(
                   p_period_id  => p_bill_period_id,
                   p_bill_id    => p_bill_id );
    
    -- �������������� ����, �� ������ ������� �����
    Pk30_Billing_Queue.Rollback_bills(v_task_id );

    -- ��������������� ������������� ����������,
    -- �� ����������� ��� ��� ����������� ����������� �������
    Pk30_Billing_Queue.Rollback_fixrates(v_task_id );

    -- ���������� ����������� ����� � ������� �� ����������� �����
    Pk36_Billing_Fixrate.Charge_fixrates(v_task_id);

    -- ��������� ���� 
    Pk30_Billing_Queue.Close_bills(v_task_id );

    -- ����������� ������ �������� �����
--    Pk30_Billing_Base.Refresh_balance(v_task_id);

    -- ����������� �������
    Pk30_Billing_Queue.Close_task(v_task_id);
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������ ������ ������������ ����� �����-������, ��� ���������� �����
--
PROCEDURE Invoice_rules_list( 
               p_recordset IN OUT SYS_REFCURSOR,
               p_bill_id   IN INTEGER DEFAULT NULL -- ���� ���� �� ����� �������� ������ ������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Invoice_rules_list';
    v_retcode    INTEGER;
    v_bill_type  VARCHAR2(1);
BEGIN
    -- ��������� ��� ���� �� ������� �������������
    SELECT MIN(B.BILL_TYPE), COUNT(*) 
      INTO v_bill_type, v_retcode
      FROM BILL_T B, ORDER_T O
     WHERE B.BILL_ID = p_bill_id
       AND B.ACCOUNT_ID = O.ACCOUNT_ID
       AND O.SERVICE_ID = Pk00_Const.c_SERVICE_OP_LOCAL;
    -- ��� �� ������������� ������ ���������� ���
    IF v_bill_type != Pk00_Const.c_BILL_TYPE_REC THEN
        PK09_INVOICE.Invoice_rules_list(p_recordset);
    ELSIF v_retcode > 0 THEN
        -- ������� �������������
        OPEN p_recordset FOR
            SELECT KEY_ID INVOICE_RULE_ID, KEY INVOICE_RULE_KEY, NAME INVOICE_RULE, NOTES 
              FROM DICTIONARY_T
             WHERE PARENT_ID = Pk00_Const.k_DICT_INV_RULE
               AND KEY_ID IN (
                  Pk00_Const.c_INVOICE_RULE_SUB_STD,
                  Pk00_Const.c_INVOICE_RULE_SUB_BIL,
                  Pk00_Const.c_INVOICE_RULE_SUB_EXT
               )
             ORDER BY 1;
    ELSE
        -- ��� ���������
        OPEN p_recordset FOR
            SELECT KEY_ID INVOICE_RULE_ID, KEY INVOICE_RULE_KEY, NAME INVOICE_RULE, NOTES 
              FROM DICTIONARY_T
             WHERE PARENT_ID = Pk00_Const.k_DICT_INV_RULE
               AND KEY_ID NOT IN (
                  Pk00_Const.c_INVOICE_RULE_SUB_STD,
                  Pk00_Const.c_INVOICE_RULE_SUB_BIL,
                  Pk00_Const.c_INVOICE_RULE_SUB_EXT
               )
             ORDER BY 1;
    END IF;
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := PIN.Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(PIN.Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������ ��� ����� ������� ������������ ����� �����-������
PROCEDURE Set_bill_invoice_rule( 
            p_bill_id         IN INTEGER,
            p_bill_period_id  IN INTEGER,
            p_invoice_rule_id IN INTEGER  -- DICTIONARY_T(77)
          )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Set_bill_invoice_rule';
BEGIN
    UPDATE BILL_T B
       SET B.INVOICE_RULE_ID = p_invoice_rule_id
     WHERE B.BILL_ID         = p_bill_id
       AND B.REP_PERIOD_ID   = p_bill_period_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������ ��� �/����� ������� ������������ ����� �����-������ (������������)
PROCEDURE Set_account_invoice_rule( 
            p_account_id      IN INTEGER,
            p_invoice_rule_id IN INTEGER  -- DICTIONARY_T(77)
          )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Set_account_invoice_rule';
BEGIN
    UPDATE BILLINFO_T BI
       SET BI.INVOICE_RULE_ID = p_invoice_rule_id
     WHERE BI.ACCOUNT_ID      = p_account_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ����, ��������� ������� � ����������
-- �� ��� ��� ���� �������� ����� ������� - ��� ���������
--
PROCEDURE Correct_tax_incl( 
            p_bill_id         IN INTEGER,
            p_bill_period_id  IN INTEGER
          )
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Correct_tax_incl';
BEGIN
    -- ����� ��� � - �������, ��� � - �� �������
    MERGE INTO ITEM_T I
    USING (
      SELECT B.BILL_ID, B.REP_PERIOD_ID, 
             DECODE(A.ACCOUNT_TYPE,'P','Y','N') TAX_INCL
        FROM ACCOUNT_T A, BILL_T B
       WHERE A.ACCOUNT_ID = B.ACCOUNT_ID
         AND B.BILL_ID    = p_bill_id
         AND B.REP_PERIOD_ID = p_bill_period_id
    ) B
    ON (
       I.REP_PERIOD_ID = B.REP_PERIOD_ID AND
       I.BILL_ID = B.BILL_ID
    )   
    WHEN MATCHED THEN UPDATE SET I.TAX_INCL = B.TAX_INCL;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


END PK33_BILLING_ACCOUNT;
/
