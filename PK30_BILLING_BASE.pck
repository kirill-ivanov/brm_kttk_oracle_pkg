CREATE OR REPLACE PACKAGE PK30_BILLING_BASE
IS
    --
    -- ����� �������� ������� ������ ��� ��������� �������� ����������� 
    -- ������ � �������� �������
    -- ��������: ����������� ������ ����� ������� � �������: BILLING_QUEUE_T !!!
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK30_BILLING_BASE';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �������� �� ��������� ����������� ������ (������� ����� �����)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Next_Bill_Period;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ����������� ������ (����� ���������� ���������� ������) 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Period_for_close RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ���������������� ���������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Lock_resource;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����� ���������������� ���������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Unlock_resource;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� � ������� �� ����������� ������ 
    -- �������� ���������� �������� ��.��� ��� ��� ���
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Mark_bills( p_period_id    IN INTEGER, 
                          p_billing_id   IN INTEGER, 
                          p_task_id      IN INTEGER DEFAULT NULL,
                          p_account_type IN VARCHAR2 DEFAULT PK00_CONST.c_ACC_TYPE_J);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������� ����� � �������������
    -- ��������� ���� � ��������� READY
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Prep_items( p_task_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ����� ������-������ �� �������������� ������ 
    -- �� ����������� ������ (last_period)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Invoicing( p_task_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� - ��������� ���� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Billing( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ����� � ������ �������� (CHECK) - ����������� ����������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Set_billstatus_check( p_task_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ����� � ������ ����� � ������ (READY) �� ������� �������� (CHECK)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Set_billstatus_ready( p_task_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- �������� ��� ��� �������� �� �������� �� ����������� ������ �� �������������� �����
    -- �������� ������ �/� �� �������� ������ �/� �� ��������
    -- �������� ��� '�' �������������� FIFO, '�'-������ ����� ��� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Payment_processing( p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ �� �������� �� ������ ������
    PROCEDURE Calc_advance( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� ���� ������� ������
    PROCEDURE Period_info( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� �������, ���� ������� ������
    -- � ������ ������ �������� ������ + ������������ ����� + ��� ������� �������� �������
    PROCEDURE Refresh_balance( p_task_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� �� ������,����� ��� ��������������� ���� �����
    -- ������� ����� �������� � ����� �������� �����
    --
    PROCEDURE Check_profile( p_task_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ����������� ������ ������ � ������ �����
    -- ���������� ���������� ��� �������� ������ ������ � ������ �����
    -- ValueRUR := Currency_rate(UE_ID, p_date) + ValueUE
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Currency_rate(
             p_item_currency_id IN INTEGER,  -- ������ ������
             p_bill_currency_id IN INTEGER,  -- ������ �����
             p_date_rate   IN DATE           -- ���� �����������
      ) RETURN NUMBER DETERMINISTIC;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ���������-������������� ����������
    -- ��� ������ ������������ � �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Charge_fixrates( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ���� � �������
    PROCEDURE Put_bill2queue( p_bill_no IN VARCHAR2, p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ������ ������������ �������
    -- ��� ������ ������������ � �������
    PROCEDURE Make_bills( p_task_id IN INTEGER, 
                          p_force   IN BOOLEAN DEFAULT FALSE );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������������� ������ ������� � �������
    --
    PROCEDURE Rollback_bills(p_task_id IN INTEGER, 
                             p_force   IN BOOLEAN DEFAULT FALSE );
  
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������������ �������
    -- ��� ������ ������������ � �������
    PROCEDURE Close_period( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ����������� �������
    -- ������� ������� ����� �������� ������� (�������� ������ �� CLOSED - ��������� � ��� ��� ���������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Close_Financial_Period;

    -- ----------------------------------------------------------------------- --
    -- � � � � � � � � � � � � � � �   � � � � � � �
    -- ----------------------------------------------------------------------- --
    -- ������������� �������� ��������� ��� � ����
    PROCEDURE Correct_tax_incl( p_period_id IN INTEGER );
    
    -- ���������/���������� ���� �������� � ������
    PROCEDURE Correct_region_bill( p_task_id IN INTEGER );
    
    -- ������� �������/��������� ������
    -- ���������� ����� ������ �� ��������� ����� (��� ������)
    FUNCTION Calc_tax(
                  p_taxfree_total IN NUMBER,  -- ����� ��� ������
                  p_tax_rate      IN NUMBER   -- ������ ������ � ���������
               ) RETURN NUMBER DETERMINISTIC;

    -- ���������� ����� ������ �� ��������� ����� (� �������)
    FUNCTION Allocate_tax(
                  p_total      IN NUMBER,     -- ����� � �������
                  p_tax_rate   IN NUMBER      -- ������ ������ � ���������
               ) RETURN NUMBER DETERMINISTIC;
    
END PK30_BILLING_BASE;
/
CREATE OR REPLACE PACKAGE BODY PK30_BILLING_BASE
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ����������� ������ (����� ���������� ���������� ������) 
--
FUNCTION Period_for_close RETURN INTEGER
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Period_for_close';
    v_period_id INTEGER;
BEGIN
    SELECT PERIOD_ID
      INTO v_period_id
      FROM PERIOD_T
     WHERE CLOSE_FIN_PERIOD IS NULL
       AND POSITION = PK00_CONST.c_PERIOD_BILL;
    RETURN v_period_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� �������� �� ��������� ������
-- ����� ��������� �� �������� ��������, ������� 
-- UPDATE BILLINFO_T �� ��������� ������ last <- bill, bill <- next, next <- null
-- �����, ����������, ������� ����� ��� next
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 
PROCEDURE Next_Bill_Period 
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Next_Bill_Period';
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������� �� ��������� ����������� ������
    Pk04_Period.Next_bill_period;
    --
    COMMIT;
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ���������������� ���������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Lock_resource
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    
    v_prcName    CONSTANT VARCHAR2(30) := 'Lock_resource';
    v_result     NUMBER;
    v_handle     VARCHAR2(128);
    v_lock_name  VARCHAR2(16) := Pk00_Const.c_Bill_Lock;
BEGIN
    -- �������� ���������� � �������� ������
    DBMS_LOCK.ALLOCATE_UNIQUE(v_lock_name, v_handle, 86400);
    -- ������������� ����������
    v_result := DBMS_LOCK.REQUEST (
                    lockhandle        => v_handle,
                    lockmode          => DBMS_LOCK.S_MODE,
                    timeout           => 10,   -- ������ � ���., � ������� �������� �������� �������� ����������
                    release_on_commit => FALSE
                );
    -- ������������ ���������
    CASE v_result
      WHEN 0 THEN                       
          Pk01_Syslog.Write_Msg(p_Msg => 'The lock has been got successfully (' || v_lock_name || '). ',
                                p_Src => c_PkgName||v_prcName );
      WHEN 1 THEN      
          Pk01_Syslog.Write_Msg(p_Msg => 'The lock is busy (' || v_lock_name || ').',
                                p_Src => c_PkgName||v_prcName );
      WHEN 2 THEN  -- ����������� ������
          Pk01_Syslog.Raise_user_Exception('Deadlock detected (' || v_lock_name || ').', c_PkgName||'.'||v_prcName );
      WHEN 3 THEN         
          Pk01_Syslog.Write_Msg(p_Msg => 'Parameter error.',	
                                p_Src => c_PkgName||v_prcName );
      WHEN 4 THEN
          Pk01_Syslog.Write_Msg(p_Msg => 'Already own lock specified by lockhandle. (' ||v_lock_name|| ')',
                                p_Src => c_PkgName||v_prcName );
      WHEN 5 THEN
          Pk01_Syslog.Raise_user_Exception('Illegal lock handle (' || v_lock_name || ').', c_PkgName||'.'||v_prcName );
    END CASE;
    -- ������������ ���������
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ���������������� ���������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Unlock_resource
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    
    v_prcName    CONSTANT VARCHAR2(30) := 'Unlock_resource';
    v_result     NUMBER;
    v_handle     VARCHAR2(128);
    v_lock_name  VARCHAR2(16) := Pk00_Const.c_Bill_Lock;
BEGIN
  
    -- �������� ���������� � �������� ������
    DBMS_LOCK.ALLOCATE_UNIQUE(v_lock_name, v_handle, 86400);

    v_result := DBMS_LOCK.RELEASE(lockhandle => v_handle);
    CASE v_result
        WHEN 0 THEN     
            Pk01_Syslog.Write_Msg(p_Msg => 'The lock has been released successfully (' || v_lock_name || ').',
                                  p_Src => c_PkgName||v_prcName );
        WHEN 3 THEN         
            Pk01_Syslog.Write_Msg(p_Msg => 'Parameter error.',    
                                  p_Src => c_PkgName||v_prcName );
        WHEN 4 THEN
            Pk01_Syslog.Write_Msg(p_Msg => 'Do not own lock specified by lockhandle (' || v_lock_name || ').', 
                                  p_Src => c_PkgName||v_prcName );
        WHEN 5 THEN
            Pk01_Syslog.Write_Msg(p_Msg => 'Illegal lock handle (' || v_lock_name || ').',    
                                  p_Src => c_PkgName||v_prcName );        
    END CASE;
    -- ������������ ���������
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- ��������� � ������� �� ����������� ������ 
-- �������� ���������� �������� ��.��� ��� ��� ���
-- ------------------------------------------------------------------------- --
PROCEDURE Mark_bills( p_period_id    IN INTEGER,
                      p_billing_id   IN INTEGER, 
                      p_task_id      IN INTEGER DEFAULT NULL,
                      p_account_type IN VARCHAR2 DEFAULT PK00_CONST.c_ACC_TYPE_J)
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Mark_bills';
    v_count       INTEGER;
    v_task_id     INTEGER;
BEGIN
    -- ���� id ������ �� ������, ������� ��� ������ id ��������  
    IF p_task_id IS NOT NULL THEN
        v_task_id := p_task_id;
    ELSE
        v_task_id := p_billing_id;
    END IF;
    -- �� ������ ������ ������ ������� �� ��������� �������
    DELETE FROM BILLING_QUEUE_T Q 
     WHERE Q.TASK_ID    = v_task_id;
    --
    IF p_account_type = PK00_CONST.c_ACC_TYPE_J THEN
        INSERT INTO BILLING_QUEUE_T (
            BILL_ID, ACCOUNT_ID, TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID
        )
        SELECT B.BILL_ID, B.ACCOUNT_ID, 
               v_task_id, p_period_id, p_period_id
          FROM BILL_T B, ACCOUNT_T A
         WHERE B.REP_PERIOD_ID = p_period_id
           AND A.ACCOUNT_ID    = B.ACCOUNT_ID
           AND A.BILLING_ID    = p_billing_id
           AND A.ACCOUNT_TYPE  = p_account_type
           AND A.STATUS        = Pk00_Const.c_ACC_STATUS_BILL
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_OPEN)
        ;
    ELSE
        INSERT INTO BILLING_QUEUE_T (
            BILL_ID, ACCOUNT_ID, TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID
        )
        SELECT B.BILL_ID, B.ACCOUNT_ID, 
               v_task_id, p_period_id, p_period_id
          FROM BILL_T B, ACCOUNT_T A
         WHERE B.REP_PERIOD_ID = p_period_id
           AND A.ACCOUNT_ID    = B.ACCOUNT_ID
           AND A.BILLING_ID    = p_billing_id
           AND A.ACCOUNT_TYPE  = p_account_type
           AND A.STATUS      IN (Pk00_Const.c_ACC_STATUS_BILL, 
                                 Pk00_Const.c_ACC_STATUS_CLOSED)
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_OPEN)
        ;
    END IF;
    v_count := SQL%ROWCOUNT;
    COMMIT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILLING_QUEUE_T');
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ������� ����� � �������������
-- ��������� ���� � ��������� READY
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Prep_items( p_task_id IN INTEGER )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Prep_items';
    v_count       INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ���������� ����������� ���������� � ������ ������ �
    -- ���������� � ������ �����, ��� ���� ����� ������ ����� �����/������
    -- ��� ����� - ����������� ��������� ���������
    -- ������ ������� ������ ������������
    MERGE INTO ITEM_T I
    USING (
      --------------------------------------------------------------------------
      SELECT ITEM_ID, REP_PERIOD_ID, ITEM_CURRENCY_RATE,
             (ITEM_TOTAL * ITEM_CURRENCY_RATE) BILL_TOTAL
        FROM (
          SELECT I.ITEM_ID, I.REP_PERIOD_ID, I.ITEM_TOTAL, I.CHARGE_TYPE,
                 -- ����������� �� ���� �����
                 Currency_rate(
                      p_item_currency_id => I.ITEM_CURRENCY_ID,
                      p_bill_currency_id => B.CURRENCY_ID,
                      p_date_rate        => B.BILL_DATE) ITEM_CURRENCY_RATE
            FROM ITEM_T I, BILL_T B, BILLING_QUEUE_T Q
           WHERE I.BILL_ID       = B.BILL_ID
             AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
             AND Q.BILL_ID       = B.BILL_ID
             AND Q.REP_PERIOD_ID = B.REP_PERIOD_ID
             AND I.ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, 
                                 Pk00_Const.c_ITEM_TYPE_ADJUST)
             AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
             AND B.BILL_TYPE IN (Pk00_Const.c_BILL_TYPE_REC,
                                 Pk00_Const.c_BILL_TYPE_OLD,
                                 Pk00_Const.c_BILL_TYPE_ONT,
                                 Pk00_Const.c_BILL_TYPE_PRE
                                 --
                                 --Pk00_Const.c_BILL_TYPE_CRD, - ������ �����������
                                 --Pk00_Const.c_BILL_TYPE_DBT,
                                 --Pk00_Const.c_BILL_TYPE_ADS
                                )
        )
    ) IQ
    ON (
       I.ITEM_ID = IQ.ITEM_ID AND
       I.REP_PERIOD_ID = IQ.REP_PERIOD_ID
    )
    WHEN MATCHED THEN UPDATE 
                        SET I.BILL_TOTAL = CASE
                                           WHEN I.CHARGE_TYPE = 'USG' THEN ROUND(IQ.BILL_TOTAL, 4) -- ������� 02.10.2016 (������ �.�. 20160726)
                                           ELSE ROUND(IQ.BILL_TOTAL, 2) -- ���������� �� 2 ������ ������ � 7754550 �� 26.10.2016 �.������� 
                                           END,
                            I.ITEM_CURRENCY_RATE = IQ.ITEM_CURRENCY_RATE;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Calc bill_total for '||v_count||' - items to '
                          || Pk00_Const.c_ITEM_STATE_RE�DY 
                          || ', bill_type = B/O/M/P', 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );


    -- ���������� ����������� ���������� � ������ ������ �
    -- ���������� � ������ �����, ��� �����-����������� � �����-�������������, 
    -- ��� ��� ����������� ��������� ���������
    MERGE INTO ITEM_T I
    USING (
      SELECT  ITEM_ID, REP_PERIOD_ID, ITEM_CURRENCY_RATE,
             (ITEM_TOTAL * ITEM_CURRENCY_RATE) BILL_TOTAL
        FROM (
          SELECT I.ITEM_ID, I.REP_PERIOD_ID, I.ITEM_TOTAL, 
                 -- ����������� �� ���� ��������������� �����
                 Currency_rate(
                            p_item_currency_id => I.ITEM_CURRENCY_ID,
                            p_bill_currency_id => B.CURRENCY_ID,
                            p_date_rate        => B.FIRST_DATE 
                        ) ITEM_CURRENCY_RATE 
            FROM (
                  SELECT --SYS_CONNECT_BY_PATH(B.BILL_NO, '|') PATH,
                         --LEVEL LV,
                         CONNECT_BY_ISLEAF LF,
                         --CONNECT_BY_ROOT(B.BILL_NO) BILL_NO,
                         CONNECT_BY_ROOT(B.BILL_ID) BILL_ID,
                         CONNECT_BY_ROOT(B.REP_PERIOD_ID) REP_PERIOD_ID,
                         --B.REP_PERIOD_ID FIRST_PERIOD_ID,
                         --B.BILL_ID       FIRST_BILL_ID,
                         --B.BILL_NO       FIRST_BILL_NO,
                         B.BILL_DATE     FIRST_DATE,
                         B.CURRENCY_ID
                    FROM BILL_T B
                  CONNECT BY NOCYCLE PRIOR B.PREV_BILL_ID = B.BILL_ID 
                                 AND PRIOR B.PREV_BILL_PERIOD_ID = B.REP_PERIOD_ID
                  START WITH (B.REP_PERIOD_ID, B.BILL_ID) IN (
                     SELECT Q.REP_PERIOD_ID, Q.BILL_ID 
                       FROM BILLING_QUEUE_T Q, BILL_T B
                      WHERE Q.TASK_ID       = p_task_id
                        AND Q.REP_PERIOD_ID = B.REP_PERIOD_ID 
                        AND Q.BILL_ID       = B.BILL_ID
                        AND B.BILL_TYPE IN ( --Pk00_Const.c_BILL_TYPE_CRD, - ������ �����������
                                             Pk00_Const.c_BILL_TYPE_DBT,
                                             Pk00_Const.c_BILL_TYPE_ADS)
                  )
              ) B, ITEM_T I
           WHERE B.LF = 1
             AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
             AND I.BILL_ID = B.BILL_ID
             AND I.ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, 
                                 Pk00_Const.c_ITEM_TYPE_ADJUST)
             AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
         )
    )IQ
    ON (
       I.ITEM_ID = IQ.ITEM_ID AND
       I.REP_PERIOD_ID = IQ.REP_PERIOD_ID
    )
    WHEN MATCHED THEN UPDATE SET I.BILL_TOTAL = ROUND(IQ.BILL_TOTAL, 4), -- ���������� �� 4 ������ (������ �.�. 20160726)
                                 I.ITEM_CURRENCY_RATE = IQ.ITEM_CURRENCY_RATE;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Calc bill_total for '||v_count||' - items to '
                          || Pk00_Const.c_ITEM_STATE_RE�DY 
                          || ', bill_type = D/A', 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ��������� ������� ���������� � ������������� � ��������� READY
    -- ��������� ���� ������� ������� ���������� � ������������� ��� ������� �������
    UPDATE ITEM_T I 
       SET (I.REP_GROSS, I.REP_TAX, I.ITEM_STATUS) = (
            SELECT 
                   CASE
                      WHEN I.TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN
                        (I.BILL_TOTAL - PK30_BILLING_BASE.ALLOCATE_TAX(I.BILL_TOTAL, AP.VAT))
                      WHEN I.TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_NOT_INCL THEN
                         I.BILL_TOTAL
                      ELSE 
                        NULL
                   END REP_GROSS,
                   CASE
                      WHEN I.TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN
                         PK30_BILLING_BASE.ALLOCATE_TAX(I.BILL_TOTAL, AP.VAT)
                      WHEN I.TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_NOT_INCL THEN
                         PK30_BILLING_BASE.CALC_TAX(I.BILL_TOTAL, AP.VAT)
                      ELSE 
                        NULL
                   END REP_TAX,
                   Pk00_Const.c_ITEM_STATE_RE�DY                    
              FROM BILL_T B, ACCOUNT_PROFILE_T AP
             WHERE B.REP_PERIOD_ID = I.REP_PERIOD_ID
               AND B.BILL_ID       = I.BILL_ID
               AND B.PROFILE_ID    = AP.PROFILE_ID
       ) 
    WHERE I.ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, 
                          Pk00_Const.c_ITEM_TYPE_ADJUST)
      AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
      AND EXISTS (
          SELECT * FROM BILLING_QUEUE_T Q
           WHERE Q.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND Q.BILL_ID       = I.BILL_ID
             AND Q.TASK_ID       = p_task_id
      );
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Move status for '||v_count||' - items to '
                          || Pk00_Const.c_ITEM_STATE_RE�DY, 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������������ ����� ������-������ �� ������ (last_period)
-- �� ������� ������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Invoicing( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Invoicing';
    v_ok         INTEGER := 0;     -- ���-�� �������������� �������� (�� ������)
    v_err        INTEGER := 0;     -- ���-�� ������ ��� ������������ ��������
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ��������� ������� ��� ���� �������� ������������� ������ ������������ �������
    FOR r_bill IN (
        SELECT B.BILL_ID, B.REP_PERIOD_ID
          FROM BILL_T B
         WHERE B.BILL_STATUS  = Pk00_Const.c_BILL_STATE_OPEN
           AND EXISTS (
               SELECT * FROM BILLING_QUEUE_T Q
                WHERE Q.REP_PERIOD_ID = B.REP_PERIOD_ID
                  AND Q.BILL_ID       = B.BILL_ID
                  AND Q.TASK_ID       = p_task_id
           )
      )
    LOOP
        SAVEPOINT X;  -- ����� ���������� ������ ��� �������� �����
        BEGIN
            v_count := Pk09_Invoice.Calc_invoice(
                             p_bill_id       => r_bill.bill_id,
                             p_rep_period_id => r_bill.rep_period_id
                          );
            v_ok := v_ok + 1;         -- ������ ������ �������
        EXCEPTION
            WHEN OTHERS THEN
              -- ����� ��������� ��� �������� �����
              ROLLBACK TO X;
              -- ��������� ������ � ������� �����������
              Pk01_Syslog.Write_msg(
                 p_Msg  => 'bill_id = ' || r_bill.bill_id || ' - error',
                 p_Src  => c_PkgName||'.'||v_prcName,
                 p_Level=> Pk01_Syslog.L_err );
              v_err := v_err + 1;
            -- �������� ������ ����� �� ������:
            UPDATE BILL_T 
               SET BILL_STATUS   = Pk00_Const.c_BILL_STATE_ERROR
             WHERE BILL_ID       = r_bill.bill_id
               AND REP_PERIOD_ID = r_bill.rep_period_id;
        END;
        -- ��������� ����������
        v_count := v_ok + v_err;
        IF MOD(v_count, 10000) = 0 THEN
            Pk01_Syslog.Write_msg('count='||v_count, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
    END LOOP;    
    --
    Pk01_Syslog.Write_msg('Processed: '||v_ok||'-��, '||v_err||'-err from '||v_count, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� - ��������� ���� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Billing( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing';
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ------------------------------------------------------------------------- --
    -- ��������� ���� �����, ��� �������� ������ 
    -- � ������������� ������������ ���� ������ �����, �� ��������� ���� ����������� �����
    UPDATE BILL_T B 
       SET (B.TOTAL, B.GROSS, B.TAX, B.DUE, B.ACT_DATE_FROM, B.ACT_DATE_TO) = (
          SELECT NVL(SUM(II.TOTAL),0), NVL(SUM(II.GROSS),0), 
                 NVL(SUM(II.TAX),0),  -NVL(SUM(II.TOTAL),0),
                 MIN(II.DATE_FROM), MAX(II.DATE_TO)
            FROM INVOICE_ITEM_T II
           WHERE II.BILL_ID = B.BILL_ID
             AND II.REP_PERIOD_ID = B.REP_PERIOD_ID
        ),
        (B.PAID_TO) = (
          SELECT CASE 
                   WHEN BI.DAYS_FOR_PAYMENT IS NULL THEN -- "�����" - �������� �� ���������
                     ADD_MONTHS(B.BILL_DATE, 1)
                   ELSE
                     B.BILL_DATE + BI.DAYS_FOR_PAYMENT
                 END PAID_TO
            FROM BILLINFO_T BI
           WHERE BI.ACCOUNT_ID = B.ACCOUNT_ID
        ),
        -- ���� �� ������, ������� ��� ��� ��������� ���������
--        (B.PROFILE_ID, B.CONTRACT_ID, B.CONTRACTOR_ID, B.CONTRACTOR_BANK_ID) = (
--          SELECT AP.PROFILE_ID, AP.CONTRACT_ID, AP.CONTRACTOR_ID, AP.CONTRACTOR_BANK_ID
--            FROM ACCOUNT_PROFILE_T AP
--           WHERE AP.PROFILE_ID = B.PROFILE_ID
--        ),
        B.DUE_DATE      = B.BILL_DATE,
        B.CALC_DATE     = SYSDATE,
        B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_READY
    WHERE B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN
      AND EXISTS (
          SELECT * FROM BILLING_QUEUE_T Q
           WHERE Q.REP_PERIOD_ID = B.REP_PERIOD_ID
             AND Q.BILL_ID       = B.BILL_ID
             AND Q.TASK_ID       = p_task_id
      )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||' - bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- ������������ �������� �������������, ��� ������ ��� ����������� ���� �������������
    UPDATE BILL_T B SET B.DUE = (B.RECVD - (B.TOTAL - B.ADJUSTED))
    WHERE B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_READY
      AND B.ADJUSTED     != 0
      AND EXISTS (
          SELECT * FROM BILLING_QUEUE_T Q
           WHERE Q.REP_PERIOD_ID = B.REP_PERIOD_ID
             AND Q.BILL_ID = B.BILL_ID
             AND Q.TASK_ID = p_task_id
      );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Correct due for: '||v_count||' - bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ��� ��������� ������ ������ ������ �� 'PREPAID'
    UPDATE BILL_T B SET B.BILL_STATUS = Pk00_Const.c_BILL_STATE_PREPAID
    WHERE B.BILL_STATUS = Pk00_Const.c_BILL_STATE_READY
      AND B.BILL_TYPE   = Pk00_Const.c_BILL_TYPE_PRE 
      AND EXISTS (
          SELECT * FROM BILLING_QUEUE_T Q
           WHERE Q.REP_PERIOD_ID = B.REP_PERIOD_ID
             AND Q.BILL_ID = B.BILL_ID
             AND Q.TASK_ID = p_task_id
      );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Correct status for: '||v_count||' - prepaid bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ����� � ������ �������� (CHECK) - ����������� ����������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_billstatus_check( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_billstate_check';
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ��������� �������������� ����� � ������ CHECK
    UPDATE BILL_T B  
       SET B.BILL_STATUS = Pk00_Const.c_BILL_STATE_CHECK 
    WHERE B.BILL_STATUS  = Pk00_Const.c_BILL_STATE_READY
      AND EXISTS (
          SELECT * FROM BILLING_QUEUE_T Q
           WHERE Q.REP_PERIOD_ID = B.REP_PERIOD_ID
             AND Q.BILL_ID       = B.BILL_ID
             AND Q.TASK_ID       = p_task_id
      )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||' - bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ����� � ������ ����� � ������ (READY) �� ������� �������� (CHECK)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_billstatus_ready( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_billstate_ready';
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ��������� �������������� � ����� � ������ CHECK
    UPDATE BILL_T B  
       SET B.BILL_STATUS = Pk00_Const.c_BILL_STATE_READY 
    WHERE B.BILL_STATUS  = Pk00_Const.c_BILL_STATE_CHECK
      AND EXISTS (
          SELECT * FROM BILLING_QUEUE_T Q
           WHERE Q.REP_PERIOD_ID = B.REP_PERIOD_ID
             AND Q.BILL_ID       = B.BILL_ID
             AND Q.TASK_ID       = p_task_id
      )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||' - bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
-- �������� ��� ��� �������� �� �������� �� ��������������� � ����������� ������� 
-- �� �������������� �����
-- �������� ������ �/� �� �������� ������ �/� �� ��������
-- �������� ��� '�' �������������� FIFO, '�'-������ ����� ��� ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Payment_processing( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Payment_processing';
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id <= '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- �������� ��� ������� ������� FIFO (������� ������)
    PK10_PAYMENTS_TRANSFER.Method_fifo;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������ �� �������� �� ������ ���������� ������
-- �� ������� ������ ������� ��������� ����� �������, ������� �� ����� 
-- �� �������� ������ ������������, � ������ ������� ��� ����� ������ ������� 
--
PROCEDURE Calc_advance( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Calc_advance';
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ���������� ��� ��������� ������
    UPDATE PAYMENT_T P SET P.ADVANCE = 0, P.ADVANCE_DATE = TO_DATE(P.REP_PERIOD_ID,'yyyymm')
     WHERE EXISTS (
           SELECT * FROM BILLING_QUEUE_T Q
            WHERE Q.REP_PERIOD_ID = P.REP_PERIOD_ID
              AND Q.ACCOUNT_ID    = P.ACCOUNT_ID
              AND Q.TASK_ID       = p_task_id
       )
    ;
    MERGE INTO PAYMENT_T P
    USING (
        SELECT PAYMENT_ID, PAY_PERIOD_ID, SUM(TRANSFER_TOTAL) FOR_SERVICE 
          FROM PAY_TRANSFER_T T
        WHERE PAY_PERIOD_ID >= REP_PERIOD_ID     -- �� ��������� ������
          AND EXISTS (
               SELECT * FROM BILLING_QUEUE_T Q
                WHERE Q.REP_PERIOD_ID = T.PAY_PERIOD_ID
                  AND Q.TASK_ID       = p_task_id
            )
        GROUP BY PAYMENT_ID, PAY_PERIOD_ID
    ) T
    ON (P.PAYMENT_ID = T.PAYMENT_ID
        AND EXISTS (
             SELECT * FROM BILLING_QUEUE_T Q
              WHERE Q.REP_PERIOD_ID = P.REP_PERIOD_ID
                AND Q.ACCOUNT_ID    = P.ACCOUNT_ID
                AND Q.TASK_ID       = p_task_id
          )
       )
    WHEN MATCHED THEN UPDATE SET P.ADVANCE = P.RECVD-T.FOR_SERVICE, 
         P.ADVANCE_DATE = ADD_MONTHS(TRUNC(P.PAYMENT_DATE,'mm'),1)-1/86400;
    v_count := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg('Stop, processed: '||v_count||' payments', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ������ �� ������ ��� ���� ������� ������ ���.���
-- ��� ��� ��� ��������, �.�. �������� ���������
-- �������� ������� ��� �������� 2003, ����� ������ ������ ������ �� �����, 
-- �� ������ �� �������� - ����� ���, ������� �� �����������, � ������ ���������
-- ��������:
-- ������ ������� ������ � ������ ������ �������, ������ ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Period_info(p_task_id IN INTEGER)
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Period_info_p';
    v_count          INTEGER;
    v_period_id      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- �������� ����������, ������ �������� ���������� ������� �������������� ������� � ��������
    SELECT DISTINCT Q.REP_PERIOD_ID
      INTO v_period_id
      FROM BILLING_QUEUE_T Q
     WHERE Q.TASK_ID = p_task_id;
    -- 
    -- ������������� ������� �� ���� �/� ������� ������ 'BILL'
    MERGE INTO REP_PERIOD_INFO_T RP
    USING (
        WITH MONTH_PERIOD AS ( 
            -- �������� ����� ������ �� ��������� ������
            SELECT REP_PERIOD_ID, ACCOUNT_ID,
                   SUM(BILL_TOTAL) BILL_TOTAL, SUM(GROSS) GROSS,
                   SUM(RECVD) RECVD, SUM(ADVANCE) ADVANCE    
             FROM (
                SELECT B.REP_PERIOD_ID, B.ACCOUNT_ID,  
                       B.TOTAL BILL_TOTAL, B.GROSS,  
                       0 RECVD, 0 ADVANCE 
                  FROM BILL_T B, BILLING_QUEUE_T Q
                 WHERE Q.REP_PERIOD_ID = B.REP_PERIOD_ID
                   AND Q.BILL_ID       = B.BILL_ID
                   AND Q.ACCOUNT_ID    = B.ACCOUNT_ID
                   AND Q.TASK_ID       = p_task_id
                --
                UNION ALL
                -- �������� ����� ����������� �� ������ ��������
                SELECT P.REP_PERIOD_ID, P.ACCOUNT_ID, 
                       0 BILL_TOTAL, 0 GROSS,
                       P.RECVD, P.ADVANCE  
                  FROM PAYMENT_T P
                 WHERE P.REP_PERIOD_ID = v_period_id
            )
            GROUP BY REP_PERIOD_ID, ACCOUNT_ID
        ), 
        PREV_PERIOD AS (
            SELECT ACCOUNT_ID, REP_PERIOD_ID, CLOSE_BALANCE 
            FROM (
                SELECT ACCOUNT_ID, 
                       MAX(REP_PERIOD_ID) OVER (PARTITION BY ACCOUNT_ID) MAX_PERIOD_ID,
                       REP_PERIOD_ID, CLOSE_BALANCE
                  FROM REP_PERIOD_INFO_T R
            ) WHERE REP_PERIOD_ID = MAX_PERIOD_ID
        )
        SELECT MP.REP_PERIOD_ID, MP.ACCOUNT_ID, 
               NVL(PP.CLOSE_BALANCE,0) OPEN_BALANCE,
               NVL(PP.CLOSE_BALANCE,0)+MP.RECVD-MP.BILL_TOTAL CLOSE_BALANCE,
               MP.BILL_TOTAL, MP.GROSS, MP.RECVD, MP.ADVANCE,
               SYSDATE LAST_MODIFIED
          FROM MONTH_PERIOD MP, PREV_PERIOD PP 
         WHERE MP.ACCOUNT_ID = PP.ACCOUNT_ID(+)
    ) PI
    ON (
        RP.REP_PERIOD_ID = PI.REP_PERIOD_ID  AND
        RP.ACCOUNT_ID    = PI.ACCOUNT_ID
    )
    WHEN MATCHED THEN UPDATE SET RP.TOTAL         = PI.BILL_TOTAL,
                                 RP.GROSS         = PI.GROSS,
                                 RP.RECVD         = PI.RECVD,
                                 RP.ADVANCE       = PI.ADVANCE,
                                 RP.OPEN_BALANCE  = PI.OPEN_BALANCE, 
                                 RP.CLOSE_BALANCE = PI.CLOSE_BALANCE
                                 
    WHEN NOT MATCHED THEN INSERT ( 
                RP.REP_PERIOD_ID, RP.ACCOUNT_ID, RP.OPEN_BALANCE, RP.CLOSE_BALANCE, 
                RP.TOTAL, RP.GROSS, RP.RECVD, RP.ADVANCE, RP.LAST_MODIFIED
            )VALUES(
                PI.REP_PERIOD_ID, PI.ACCOUNT_ID, PI.OPEN_BALANCE, PI.CLOSE_BALANCE, 
                PI.BILL_TOTAL, PI.GROSS, PI.RECVD, PI.ADVANCE, PI.LAST_MODIFIED
            )
    ;
    --    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('������������ ������� �� '||v_count||' �/�', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� �������, ���� ������� ������ ������
-- � ������ ������ �������� ������ + ������������ ����� + ��� ������� �������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Refresh_balance(p_task_id IN INTEGER)
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Refresh_balance';
    v_count          INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    MERGE INTO ACCOUNT_T A
    USING   
       (SELECT ACCOUNT_ID, SUM(RECVD-BILL_TOTAL) BALANCE,
               GREATEST(MAX(BILL_DATE), MAX(PAYMENT_DATE)) BALANCE_DATE
        FROM (
            -- �������� ������ ������������� �� ������������ ������
            SELECT B.ACCOUNT_ID, 
                   B.TOTAL BILL_TOTAL, BILL_DATE, 
                   0 RECVD, TO_DATE('01.01.2000','dd.mm.yyyy') PAYMENT_DATE 
              FROM BILL_T B
             WHERE EXISTS (
                SELECT * FROM BILLING_QUEUE_T Q
                 WHERE B.ACCOUNT_ID = Q.ACCOUNT_ID
                   AND Q.TASK_ID = p_task_id
             )
            UNION ALL
            -- �������� ����� ����������� �� ������ ��������
            SELECT P.ACCOUNT_ID, 
                   0 BILL_TOTAL, TO_DATE('01.01.2000','dd.mm.yyyy') BILL_DATE,
                   P.RECVD, P.PAYMENT_DATE  
              FROM PAYMENT_T P
             WHERE EXISTS (
                SELECT * FROM BILLING_QUEUE_T Q
                 WHERE P.ACCOUNT_ID = Q.ACCOUNT_ID
                   AND Q.TASK_ID = p_task_id
             )
        )
        GROUP BY ACCOUNT_ID) T
    ON (A.ACCOUNT_ID = T.ACCOUNT_ID)
    WHEN MATCHED THEN UPDATE SET A.BALANCE_DATE = T.BALANCE_DATE, A.BALANCE = T.BALANCE;
    --
    v_count := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg('����������� ������� '||v_count||' �/�', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������� �� ������,����� ��� ��������������� ���� �����
-- ������� ����� �������� � ����� �������� �����
--
PROCEDURE Check_profile( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_profile';
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    MERGE INTO BILL_T B
    USING (
        SELECT RN, ACCOUNT_ID, PROFILE_ID, CONTRACT_ID, CONTRACTOR_ID, CONTRACTOR_BANK_ID, VAT,
               BILL_ID, REP_PERIOD_ID, 
               BILL_PROFILE_ID, BILL_CONTRACT_ID, BILL_CONTRACTOR_ID, BILL_CONTRACTOR_BANK_ID,
               DATE_FROM, DATE_TO 
          FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY AP.ACCOUNT_ID, B.BILL_ID ORDER BY AP.DATE_FROM DESC) RN, 
                   AP.ACCOUNT_ID, AP.PROFILE_ID, AP.CONTRACT_ID, AP.CONTRACTOR_ID, AP.CONTRACTOR_BANK_ID, AP.VAT,
                   B.BILL_ID, 
                   B.REP_PERIOD_ID, 
                   B.PROFILE_ID         BILL_PROFILE_ID, 
                   B.CONTRACT_ID        BILL_CONTRACT_ID,
                   B.CONTRACTOR_ID      BILL_CONTRACTOR_ID,
                   B.CONTRACTOR_BANK_ID BILL_CONTRACTOR_BANK_ID,
                   AP.DATE_FROM, AP.DATE_TO 
              FROM ACCOUNT_PROFILE_T AP, BILL_T B, BILLING_QUEUE_T Q
             WHERE Q.TASK_ID = p_task_id
               AND Q.REP_PERIOD_ID = B.REP_PERIOD_ID
               AND Q.BILL_ID       = B.BILL_ID
               AND B.BILL_DATE     > AP.DATE_FROM
               AND (B.BILL_DATE   <= AP.DATE_TO OR AP.DATE_TO IS NULL)
               AND B.ACCOUNT_ID    = AP.ACCOUNT_ID
        )WHERE RN = 1
           AND ( PROFILE_ID != BILL_PROFILE_ID
              OR CONTRACT_ID != BILL_CONTRACT_ID
              OR CONTRACTOR_ID != BILL_CONTRACTOR_ID
              OR CONTRACTOR_BANK_ID != BILL_CONTRACTOR_BANK_ID )
    ) P
    ON (
        B.BILL_ID = P.BILL_ID
        AND B.REP_PERIOD_ID = P.REP_PERIOD_ID
    )
    WHEN MATCHED THEN UPDATE SET B.PROFILE_ID         = P.PROFILE_ID, 
                                 B.CONTRACT_ID        = P.CONTRACT_ID, 
                                 B.CONTRACTOR_ID      = P.CONTRACTOR_ID,
                                 B.CONTRACTOR_BANK_ID = P.CONTRACTOR_BANK_ID,
                                 B.VAT = P.VAT
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T.PROFILE_ID '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- �������� �� �������������� ������ ������� � ����� �������
    MERGE INTO BILL_T B
    USING (
        SELECT B.BILL_ID, 
               B.REP_PERIOD_ID,
               P.CONTRACT_ID, 
               P.CONTRACTOR_ID,
               P.CONTRACTOR_BANK_ID,
               P.VAT 
          FROM BILL_T B, ACCOUNT_PROFILE_T P, BILLING_QUEUE_T Q
         WHERE Q.TASK_ID       = p_task_id
           AND Q.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND Q.BILL_ID       = B.BILL_ID
           AND B.PROFILE_ID    = P.PROFILE_ID
           AND (B.CONTRACT_ID        != P.CONTRACT_ID 
             OR B.CONTRACTOR_ID      != P.CONTRACTOR_ID
             OR B.CONTRACTOR_BANK_ID != P.CONTRACTOR_BANK_ID
             OR B.VAT                != P.VAT
           )
    ) P   
    ON (
        B.BILL_ID = P.BILL_ID AND
        B.REP_PERIOD_ID = P.REP_PERIOD_ID
    )
    WHEN MATCHED THEN UPDATE SET
            B.CONTRACT_ID        = P.CONTRACT_ID, 
            B.CONTRACTOR_ID      = P.CONTRACTOR_ID,
            B.CONTRACTOR_BANK_ID = P.CONTRACTOR_BANK_ID,
            B.VAT                = P.VAT
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T.PROFILE DATA '||v_count||' rows corrected', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� ����������� ������ ������ � ������ �����
-- ���������� ���������� ��� �������� ������ ������ � ������ �����
-- ValueRUR := Currency_rate(UE_ID, p_date) + ValueUE
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Currency_rate(
         p_item_currency_id IN INTEGER,  -- ������ ������
         p_bill_currency_id IN INTEGER,  -- ������ �����
         p_date_rate        IN DATE      -- ���� �����������
  ) RETURN NUMBER DETERMINISTIC
IS 
    v_prcName     CONSTANT VARCHAR2(30) := 'Currency_rate';
    v_rate        NUMBER;
    v_currency_id INTEGER;
BEGIN
    IF p_item_currency_id = p_bill_currency_id THEN
        v_rate := 1;
    ELSIF p_item_currency_id = Pk00_Const.c_CURRENCY_RUB THEN
        IF p_bill_currency_id IN (
                Pk00_Const.c_CURRENCY_USD,
                Pk00_Const.c_CURRENCY_EUR,
                960 -- XDR
              ) 
        THEN
            -- ����������� �� ����� � ������, �� ��������� ����
            SELECT 1/RATE_VALUE
              INTO v_rate
              FROM (
                SELECT ROW_NUMBER() OVER (ORDER BY DATE_RATE DESC) RN, RATE_VALUE 
                  FROM CURRENCY_RATE_T
                 WHERE CURRENCY_ID = p_bill_currency_id
                   AND DATE_RATE  <= TRUNC(p_date_rate)
               )
              WHERE RN = 1;
        ELSIF p_bill_currency_id = Pk00_Const.c_CURRENCY_YE_FIX THEN -- 286
            -- ����������� � ����� 01.02.2017
            v_rate := 1/28.6;
        ELSE -- ���� ���-�� ���� - ������ �� �������
           Pk01_Syslog.Write_msg(p_Msg   => 'p_bill_currency_id = '||p_bill_currency_id,
                                 p_Src   => c_PkgName||'.'||v_prcName,
                                 p_Level => Pk01_Syslog.L_err );
           RAISE NO_DATA_FOUND;
        END IF;  
    ELSIF p_item_currency_id = Pk00_Const.c_CURRENCY_USD 
      AND p_bill_currency_id = Pk00_Const.c_CURRENCY_YE THEN
        -- � �� � ������ USD, ������� � ����� �����-�������
        v_rate := 1;
    ELSIF p_item_currency_id = Pk00_Const.c_CURRENCY_YE_FIX THEN 
        -- ������������� ���� 28.6 ��� (������ � �� ������� �� ������)
        v_rate := 28.6;
    ELSE -- ����������� �� ���� ����������� �����
        IF p_item_currency_id IN (  Pk00_Const.c_CURRENCY_YE, -- 36
                                    Pk00_Const.c_CURRENCY_USD )     
           AND p_bill_currency_id = Pk00_Const.c_CURRENCY_RUB -- 810  
        THEN -- ����������� USD �� ���� �����
            v_currency_id := Pk00_Const.c_CURRENCY_USD;       -- 840;
        ELSIF p_item_currency_id IN ( 124, --Pk00_Const.c_CURRENCY_YEE -- 124
                                      Pk00_Const.c_CURRENCY_EUR ) -- 978
           AND p_bill_currency_id =   Pk00_Const.c_CURRENCY_RUB   -- 810
        THEN -- ����������� EURO �� ���� �����
            v_currency_id := Pk00_Const.c_CURRENCY_EUR; -- 978;
        ELSE  -- ������������ ������ ������ � �����, �� ���� �����
            v_currency_id := p_item_currency_id;
        END IF;
        -- ������ ���, ���� - ����������� �� ��������� ����
        SELECT RATE_VALUE 
          INTO v_rate
          FROM (
            SELECT ROW_NUMBER() OVER (ORDER BY DATE_RATE DESC) RN, RATE_VALUE 
              FROM CURRENCY_RATE_T
             WHERE CURRENCY_ID = v_currency_id
               AND DATE_RATE  <= TRUNC(p_date_rate)
           )
          WHERE RN = 1;
        --
    END IF;
    RETURN v_rate;
EXCEPTION 
    WHEN NO_DATA_FOUND THEN
        Pk01_Syslog.raise_Exception('item_currency_id='||p_item_currency_id||
                                  ', date='||TO_CHAR(p_date_rate,'dd.mm.yyyy')||
                                  ', bill_currency_id='||p_bill_currency_id
                                   , c_PkgName||'.'||v_prcName );
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

/*
FUNCTION Currency_rate(
         p_item_currency_id IN INTEGER,  -- ������ ������
         p_bill_currency_id IN INTEGER,  -- ������ �����
         p_date_rate        IN DATE      -- ���� �����������
  ) RETURN NUMBER DETERMINISTIC
IS 
    v_prcName     CONSTANT VARCHAR2(30) := 'Currency_rate';
    v_rate        NUMBER;
    v_currency_id INTEGER;
BEGIN
    IF p_item_currency_id = Pk00_Const.c_CURRENCY_RUB THEN
        IF p_bill_currency_id = Pk00_Const.c_CURRENCY_RUB THEN
           v_rate := 1;
        ELSIF p_bill_currency_id IN (
              Pk00_Const.c_CURRENCY_USD,
              Pk00_Const.c_CURRENCY_EUR
            ) 
        THEN
            -- ����������� �� ����� � ������, �� ��������� ����
            SELECT 1/RATE_VALUE
              INTO v_rate
              FROM (
                SELECT ROW_NUMBER() OVER (ORDER BY DATE_RATE DESC) RN, RATE_VALUE 
                  FROM CURRENCY_RATE_T
                 WHERE CURRENCY_ID = p_bill_currency_id
                   AND DATE_RATE  <= TRUNC(p_date_rate)
               )
              WHERE RN = 1;
        ELSE -- ���� ���-�� ���� - ������ �� �������
           Pk01_Syslog.Write_msg(p_Msg   => 'p_bill_currency_id = '||p_bill_currency_id,
                                 p_Src   => c_PkgName||'.'||v_prcName,
                                 p_Level => Pk01_Syslog.L_err );
           RAISE NO_DATA_FOUND;
        END IF;
    ELSIF p_bill_currency_id IN (
            Pk00_Const.c_CURRENCY_USD, -- ������ ����� USD (������ � �� ������� �� ������)
            Pk00_Const.c_CURRENCY_YE,  -- ������ ����� USD �� ���� ������ (������ � �� ������� �� ������)
            Pk00_Const.c_CURRENCY_EUR, -- ������ ����� EURO (������ � �� ������� �� ������)
            124,  --Pk00_Const.c_CURRENCY_YEE -- ������ ����� EURO �� ���� ������(������ � �� ������� �� ������)
            960   -- XDR            
            ) 
    THEN
        v_rate := 1;
    ELSIF p_item_currency_id = Pk00_Const.c_CURRENCY_YE_FIX THEN 
        -- ������������� ���� 28.6 ��� (������ � �� ������� �� ������)
        v_rate := 28.6;
    ELSE -- ����������� �� ���� ����������� �����
        IF p_item_currency_id IN (  Pk00_Const.c_CURRENCY_YE, -- 36
                                    Pk00_Const.c_CURRENCY_USD )     
           AND p_bill_currency_id = Pk00_Const.c_CURRENCY_RUB -- 810  
        THEN -- ����������� USD �� ���� �����
            v_currency_id := Pk00_Const.c_CURRENCY_USD;       -- 840;
        ELSIF p_item_currency_id IN ( 124, --Pk00_Const.c_CURRENCY_YEE -- 124
                                      Pk00_Const.c_CURRENCY_EUR ) -- 978
           AND p_bill_currency_id =   Pk00_Const.c_CURRENCY_RUB   -- 810
        THEN -- ����������� EURO �� ���� �����
            v_currency_id := Pk00_Const.c_CURRENCY_EUR; -- 978;
        ELSE  -- ������������ ������ ������ � �����, �� ���� �����
            v_currency_id := p_item_currency_id;
        END IF;
        -- ������ ���, ���� - ����������� �� ��������� ����
        SELECT RATE_VALUE 
          INTO v_rate
          FROM (
            SELECT ROW_NUMBER() OVER (ORDER BY DATE_RATE DESC) RN, RATE_VALUE 
              FROM CURRENCY_RATE_T
             WHERE CURRENCY_ID = v_currency_id
               AND DATE_RATE  <= TRUNC(p_date_rate)
           )
          WHERE RN = 1;
        --
    END IF;
    RETURN v_rate;
EXCEPTION 
    WHEN NO_DATA_FOUND THEN
        Pk01_Syslog.raise_Exception('item_currency_id='||p_item_currency_id||
                                  ', date='||TO_CHAR(p_date_rate,'dd.mm.yyyy')
                                   , c_PkgName||'.'||v_prcName );
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ���������-������������� ����������, ��� ������ 
-- ������������ � �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_fixrates(p_task_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Charge_fixrates';
BEGIN

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������������� ������ ��� �������� �������
    -- ��������� ��� ������� �� ����������� ���������
    -- � ����������� ������� p_period_id
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --PK36_BILLING_FIXRATE.Make_bills_for_fixrates( p_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ����������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK36_BILLING_FIXRATE.Charge_fixrates(
          p_task_id       => p_task_id  
       );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ���� � �������
PROCEDURE Put_bill2queue( p_bill_no IN VARCHAR2, p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Put_bill2queue';
BEGIN
    INSERT INTO BILLING_QUEUE_T Q(
        BILL_ID, ACCOUNT_ID, TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID
    )
    SELECT BILL_ID, ACCOUNT_ID, p_task_id, REP_PERIOD_ID, REP_PERIOD_ID
      FROM BILL_T B
     WHERE B.BILL_NO = p_bill_no
    ;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������������ ������ ������� � �������
--
PROCEDURE Make_bills(p_task_id IN INTEGER,
                     p_force   IN BOOLEAN DEFAULT FALSE )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_bills';
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ������������� ������-���� � ����� �� �������� ��������
    IF p_force = FALSE THEN
       Pk30_Billing_Queue.Check_queue_bills(p_task_id);
    END IF;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� �� ������,����� ��� ��������������� ���� �����    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Check_profile( p_task_id );
    
    -- ���������/���������� ���� �������� � ������
    Correct_region_bill( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������� ����� � �������������, ��������� ���� � ��������� READY
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Prep_items( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ����� ������-������ �� ������ (last_period) �� ������� ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Invoicing( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� - �������� � ���� ��������� ITEMs, ��������� ���� � ��������� READY
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Billing( p_task_id );

    --     
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������������� ������ ������� � �������
--
PROCEDURE Rollback_bills(p_task_id IN INTEGER, 
                         p_force   IN BOOLEAN DEFAULT FALSE)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_bills';
    v_count      INTEGER;
    v_last_period_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ���������������� ������-���� � ����� �� �������� ��������
    IF p_force = FALSE THEN
       Pk30_Billing_Queue.Check_queue_bills(p_task_id);
    END IF;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ���� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� � �������� ��������� ������ �����        
    UPDATE BILL_T B
       SET B.TOTAL = 0,
           B.GROSS = 0,
           B.TAX   = 0,
           B.RECVD = 0,
           B.DUE   = 0,
           B.DUE_DATE = NULL,
           B.PAID_TO  = NULL,
           B.CALC_DATE = NULL,
           B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN
     WHERE EXISTS (
         SELECT * FROM BILLING_QUEUE_T Q
          WHERE Q.REP_PERIOD_ID = B.REP_PERIOD_ID
            AND Q.BILL_ID = B.BILL_ID
            AND Q.TASK_ID = p_task_id
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('update bill_t '||v_count||' rows - �������� � �������� ��������� ������ �����', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� ������ ������ �� ������� ����� � �������� � �������� ���������
    UPDATE ITEM_T I
       SET I.INV_ITEM_ID = NULL,
           I.REP_GROSS   = NULL,
           I.REP_TAX     = NULL,
           I.BILL_TOTAL  = NULL,
           I.ITEM_CURRENCY_RATE = NULL,
           I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
     WHERE I.ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, Pk00_Const.c_ITEM_TYPE_ADJUST)
       AND EXISTS (
           SELECT * FROM BILLING_QUEUE_T Q
            WHERE Q.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND Q.BILL_ID       = I.BILL_ID
              AND Q.TASK_ID       = p_task_id
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('update item_t '||v_count||' rows - �������� ������� ������ ������ �� ������� �����', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
       
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ������ ������
    DELETE FROM INVOICE_ITEM_T II
     WHERE EXISTS (
           SELECT * FROM BILLING_QUEUE_T Q
            WHERE Q.REP_PERIOD_ID = II.REP_PERIOD_ID
              AND Q.BILL_ID       = II.BILL_ID
              AND Q.TASK_ID       = p_task_id
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('delete invoice_item_t '||v_count||' rows - ������� ������� ������ ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� �������, ���� ������� ������ (����� READY - ������ � ������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ID ���������� ��������� �������
    v_last_period_id := PK04_PERIOD.Last_period_id;
    -- �������� �������� ������� �� ���������� ��������� �������
    UPDATE ACCOUNT_T A
       SET (A.BALANCE, A.BALANCE_DATE) = (
         SELECT 
         (
            -- �������� ��������� ������ �� ���������� ��������� �������
            SELECT NVL(SUM(R.CLOSE_BALANCE),0)
              FROM REP_PERIOD_INFO_T R
             WHERE R.ACCOUNT_ID = A.ACCOUNT_ID
               AND R.REP_PERIOD_ID = v_last_period_id
         )-(
            -- �������� ������ ������������� �� ������������ ������
            SELECT NVL(SUM(B.TOTAL),0)
              FROM BILL_T B
             WHERE B.ACCOUNT_ID = A.ACCOUNT_ID
               AND B.BILL_STATUS IN (PK00_CONST.c_BILL_STATE_CLOSED, PK00_CONST.c_BILL_STATE_READY)
               AND B.REP_PERIOD_ID > v_last_period_id
         )+(
            -- �������� ����� ����������� �� ������ ��������
            SELECT NVL(SUM(P.RECVD),0)
              FROM PAYMENT_T P
             WHERE P.ACCOUNT_ID = A.ACCOUNT_ID
               AND P.REP_PERIOD_ID > v_last_period_id
         ) BALANCE, SYSDATE
         FROM DUAL
       )
    WHERE EXISTS (
        SELECT * FROM BILLING_QUEUE_T Q
         WHERE Q.ACCOUNT_ID = A.ACCOUNT_ID
           AND Q.TASK_ID    = p_task_id
    );
    v_count := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg('����������� ������� '||v_count||' �/�', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --   
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ��������� �������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Close_period( p_task_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_period';
BEGIN

    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ �� �������� �� ������ ���������� ������
    -- �� ������� ������ ������� ��������� ����� �������, ������� �� ����� 
    -- �� �������� ������ ������������, � ������ ������� ��� ����� ������ ������� 
    --
    Calc_advance( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� ���� ������� ������
    -- (����� ��� ������ ��������� ���.�����)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Period_info( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� �������, ���� ������� ������ (����� READY - ������ � ������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Refresh_balance( p_task_id );
    --
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ����������� �������
-- ������� ������� ����� �������� ������� (�������� ������ �� CLOSED - ��������� � ��� ��� ���������)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Close_Financial_Period
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_Financial_Period';
    v_period_id  INTEGER;
    v_count      INTEGER;
BEGIN
    -- �������� ���� ������� (����� ���������� ���������� ������)
    v_period_id := Period_for_close;
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||v_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk04_Period.Close_fin_period;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �������� (��������������) ������ (���� ��������� � ����������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk04_Period.Close_rep_period;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ������� ����� �������� ������� (��������� � ��� ��� ���������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE ITEM_T
       SET ITEM_STATUS = Pk00_Const.c_ITEM_STATE_CLOSED
     WHERE REP_PERIOD_ID = v_period_id
       AND ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, Pk00_Const.c_ITEM_TYPE_ADJUST);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('������� '||v_count||' ������� ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ����� �������� ������� (��������� � ��� ��� ���������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE BILL_T 
       SET BILL_STATUS = Pk00_Const.c_BILL_STATE_CLOSED
     WHERE REP_PERIOD_ID = v_period_id
       AND BILL_STATUS IN ( Pk00_Const.c_BILL_STATE_READY);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('������� '||v_count||' ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ �� �������� �� ������ ���������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --Calc_advance( v_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� ���� ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --Period_info( v_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� �������, ���� ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --Refresh_balance;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------------- --
-- � � � � � � � � � � � � � � �   � � � � � � �
-- ----------------------------------------------------------------------- --

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ����, ��������� ������� � ����������
-- �� ��� ��� ���� �������� ����� ������� - ��� ���������
--
PROCEDURE Correct_tax_incl( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Correct_tax_incl';
    v_count      INTEGER;
BEGIN
    -- ��� ���.��� ��� ���������� ������ ������� ������
    -- ���������� (������ �.������� �� 25.09.2015):
    -- 1.��
    -- ������� ���� ����������    SP149399
    -- ����� 14FPH016268 - ������ �������� ��� ���, ������� ��� ������ �����������  � �����, � �� ���������� �� ���
    -- ORDER_ID = 2645246, 
    -- ORDER_BODY_ID = 2659136 - MIN
    -- ORDER_BODY_ID = 2645248 - USG
    --    
    UPDATE ITEM_T I
       SET I.TAX_INCL = 'Y'
     WHERE (I.TAX_INCL = 'N' OR I.TAX_INCL IS NULL)
       AND I.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_USG
       AND EXISTS (
           SELECT * FROM ACCOUNT_T A, BILL_T B
            WHERE A.ACCOUNT_TYPE = 'P'
              AND A.ACCOUNT_ID = B.ACCOUNT_ID
              AND I.BILL_ID = B.BILL_ID
              AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
         )  
       AND I.REP_PERIOD_ID = p_period_id
       AND NOT EXISTS (
           SELECT * -- ���������� �� ������ �������� � �������
              FROM PK30_CORRECT_TAX_INCL_T PK
             WHERE PK.ORDER_BODY_ID = I.ORDER_BODY_ID
               AND PK.ACCOUNT_TYPE  = 'P'
               AND PK.TAX_INCL      = 'N'
       )
       --AND I.ORDER_BODY_ID NOT IN (2659136,2645246,2933630,2933631)
       ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('���������� '||v_count||' item ��� ���.���', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��� ��.��� ���������� �� ������� ������
    -- ���������� (������ �.������� �� 25.09.2015):
    -- 2. ��
    -- �/� MS006806  ����� 14K008164
    -- ����� ������ ���� �������  ��� � ������ ��� � ������� 20048,63 �
    -- ORDER_ID = 2919062, ORDER_BODY_ID = 2920042
    --
    UPDATE ITEM_T I
       SET I.TAX_INCL = 'N'
     WHERE (I.TAX_INCL = 'Y' OR I.TAX_INCL IS NULL)
       AND EXISTS (
           SELECT * FROM ACCOUNT_T A, BILL_T B
            WHERE A.ACCOUNT_TYPE = 'J'
              AND A.ACCOUNT_ID = B.ACCOUNT_ID
              AND I.BILL_ID = B.BILL_ID
              AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
              AND A.ACCOUNT_ID NOT IN (2346760, 2346970)
         )  
       AND I.REP_PERIOD_ID = p_period_id
       AND I.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_USG
       --AND I.ORDER_BODY_ID NOT IN (2920042,2817286,2929701,2929702,2929703,2929704,2929705,2935994)
       AND NOT EXISTS (
           SELECT * -- ���������� �� ������ �������� � �������
              FROM PK30_CORRECT_TAX_INCL_T PK
             WHERE PK.ORDER_BODY_ID = I.ORDER_BODY_ID
               AND PK.ACCOUNT_TYPE  = 'J'
               AND PK.TAX_INCL      = 'Y'
       )       
       ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('���������� '||v_count||' item ��� ��.���', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���� ���������� (������ �.������� �� 25.09.2015):
    -- 1.��
    -- ������� ���� ����������	SP149399
    -- ����� 14FPH016268 - ������ �������� ��� ���, ������� ��� ������ �����������  � �����, � �� ���������� �� ���

    -- 2. ��
    -- �/� MS006806  ����� 14K008164
    -- ����� ������ ���� �������  ��� � ������ ��� � ������� 20048,63 �

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������/���������� ���� �������� � ������
PROCEDURE Correct_region_bill( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Correct_region_bill';
    v_count      INTEGER;
BEGIN
    -- ============================================================================       
    -- ��������� ���������, ��� ������ � ������������� ������������ ���������  
    MERGE INTO BILL_T B
    USING (       
        SELECT B.ACCOUNT_ID, CR.REGION_ID, B.BILL_ID, B.REP_PERIOD_ID, B.BILL_NO 
          FROM CONTRACTOR_T CR, BILL_T B, BILLING_QUEUE_T Q
         WHERE Q.TASK_ID = p_task_id
           AND Q.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND Q.BILL_ID       = B.BILL_ID
           AND B.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND CR.REGION_ID IS NOT NULL
           AND CR.REGION_ID != SUBSTR(B.BILL_NO,1,4)
           AND SUBSTR(B.BILL_NO,5,1) = '/'
    ) P
    ON (
        B.REP_PERIOD_ID = P.REP_PERIOD_ID
        AND B.BILL_ID   = P.BILL_ID
    )
    WHEN MATCHED THEN UPDATE SET B.BILL_NO = LPAD(TO_CHAR(P.REGION_ID), 4,'0')||SUBSTR(B.BILL_NO,5);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('���� ������� ����������� '||v_count||' ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ============================================================================       
    -- ��������� ���������, ��� ������ � ��������������� ���������
    MERGE INTO BILL_T B
    USING (       
        SELECT B.ACCOUNT_ID, CR.REGION_ID, B.BILL_ID, B.REP_PERIOD_ID, B.BILL_NO 
          FROM CONTRACTOR_T CR, BILL_T B, BILLING_QUEUE_T Q
         WHERE Q.TASK_ID = p_task_id
           AND Q.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND Q.BILL_ID       = B.BILL_ID
           AND B.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND CR.REGION_ID IS NOT NULL
           AND SUBSTR(B.BILL_NO,5,1) != '/' -- ������� �� ����������
    ) P
    ON (
        B.REP_PERIOD_ID = P.REP_PERIOD_ID
        AND B.BILL_ID   = P.BILL_ID
    )
    WHEN MATCHED THEN UPDATE SET B.BILL_NO = LPAD(TO_CHAR(P.REGION_ID), 4,'0')||'/'||B.BILL_NO;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('���� ������� ����������� ��� '||v_count||' ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ============================================================================       
    -- ����� ��������, ��� ��� ���� ����������� ��������
    MERGE INTO BILL_T B
    USING (       
        SELECT B.ACCOUNT_ID, CR.REGION_ID, B.BILL_ID, B.REP_PERIOD_ID, B.BILL_NO 
          FROM CONTRACTOR_T CR, BILL_T B, BILLING_QUEUE_T Q
         WHERE Q.TASK_ID = p_task_id
           AND Q.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND Q.BILL_ID       = B.BILL_ID
           AND B.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND CR.REGION_ID IS NULL          -- ������ �� ����������
           AND SUBSTR(B.BILL_NO,5,1) = '/'   -- ������� ����������
    ) P
    ON (
        B.REP_PERIOD_ID = P.REP_PERIOD_ID
        AND B.BILL_ID   = P.BILL_ID
    )
    WHEN MATCHED THEN UPDATE SET B.BILL_NO = SUBSTR(B.BILL_NO, 6);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('���� ������� ������� ��� '||v_count||' ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� �������/��������� ������
-- 
-- ���������� ����� ������ �� ��������� ����� (��� ������)
FUNCTION Calc_tax(
              p_taxfree_total IN NUMBER,  -- ����� ��� ������
              p_tax_rate      IN NUMBER   -- ������ ������ � ���������
           ) RETURN NUMBER DETERMINISTIC
IS
BEGIN    
     RETURN  ROUND(p_taxfree_total * p_tax_rate / 100, 4);
END;

-- ���������� ����� ������ �� ��������� ����� (� �������)
FUNCTION Allocate_tax(
              p_total      IN NUMBER,     -- ����� � �������
              p_tax_rate   IN NUMBER      -- ������ ������ � ���������
           ) RETURN NUMBER DETERMINISTIC
IS
BEGIN    
     RETURN  p_total - ROUND(p_total /(1 + p_tax_rate / 100), 4);
END;

END PK30_BILLING_BASE;
/
