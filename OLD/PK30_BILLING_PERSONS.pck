CREATE OR REPLACE PACKAGE PK30_BILLING_PERSONS
IS
    --
    -- ����� ��� ��������� �������� ����������� ������ � �������� �������
    -- �������� BRM-KTTK ( � ��������������� ������ �� "���������" )
    -- ��� �������� ���.���
    -- ����������� ������ ����� ������� � �������: BILLING_QUEUE_T
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK30_BILLING_PERSONS';
    -- ==============================================================================
    c_TASK_ID   constant integer := Pk00_Const.c_BILLING_MMTS;
    
    type t_refc is ref cursor;

    -- ------------------------------------------------------------------------- --
    -- � � � � �   � � � � � � � �
    -- ------------------------------------------------------------------------- --
    -- ��������� ������� �� ����������� ������
    PROCEDURE Mark_bills( p_period_id IN INTEGER );
   
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- �������� ��� ��� �������� �� �������� �� ����������� ������ �� �������������� �����
    -- �������� ������ �/� �� �������� ������ �/� �� ��������
    -- �������� ��� '�' �������������� FIFO, '�'-������ ����� ��� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Payment_processing( p_bill_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������������ �������
    PROCEDURE Close_period( p_period_id IN INTEGER );

    
END PK30_BILLING_PERSONS;
/
CREATE OR REPLACE PACKAGE BODY PK30_BILLING_PERSONS
IS

-- ------------------------------------------------------------------------- --
-- ��������� � ������� �� ����������� ������ �������� ���.���
-- ����������:
-- ��� �� �� ���� ��������, ���� ���.��� ����� � �������� ���� "���������" (2003),
-- �.�. �� ������������ � ��� BRM_KTTK (2001) �������� �� �������� 
-- "�������" �������� (2002), �.�. ��� ����� �������� � ������� ��������
-- ------------------------------------------------------------------------- --
PROCEDURE Mark_bills( p_period_id IN INTEGER )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Mark_bills';
    v_count     INTEGER;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
    -- ����� ������� � BRM-KTTK (�� ������ ������, ����� �����)
    INSERT INTO BILLING_QUEUE_T (BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, TASK_ID)
    SELECT BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, c_TASK_ID
      FROM (
        SELECT B.BILL_ID, B.ACCOUNT_ID, A.BILLING_ID, AP.PROFILE_ID, 
               AP.DATE_FROM, MAX(AP.DATE_FROM) OVER (PARTITION BY AP.ACCOUNT_ID) MAX_DATE_FROM
          FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE B.REP_PERIOD_ID = p_period_id
           AND A.ACCOUNT_ID    = B.ACCOUNT_ID
           AND A.BILLING_ID    = Pk00_Const.c_BILLING_MMTS
           AND A.ACCOUNT_TYPE  = Pk00_Const.c_ACC_TYPE_P
           AND A.STATUS        = Pk00_Const.c_ACC_STATUS_BILL
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_OPEN, Pk00_Const.c_BILL_STATE_EMPTY)
           AND AP.ACCOUNT_ID   = A.ACCOUNT_ID
           AND AP.DATE_FROM   <= B.BILL_DATE
           AND (AP.DATE_TO IS NULL OR B.BILL_DATE <= AP.DATE_TO )
    )
    WHERE DATE_FROM = MAX_DATE_FROM;
    v_count := SQL%ROWCOUNT;
    COMMIT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILLING_QUEUE_T');
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ��� ��� �������� �� �������� �� ��������������� � ����������� ������� 
-- �� �������������� �����
-- �������� ������ �/� �� �������� ������ �/� �� ��������
-- �������� ��� '�' �������������� FIFO, '�'-������ ����� ��� ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Payment_processing( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Payment_processing';
    v_pay_due    NUMBER;     -- ������� �� �������� ����� ��������
    v_ok         INTEGER;    
    v_err        INTEGER;
    v_prev_period_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id <= '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- �������� ��� ������� ������� FIFO (������� ������)
    PK10_PAYMENTS_TRANSFER.Method_fifo;

    -- �������� ������� FIFO ������� ���������� ��� (��������� ������, �������)
    --Pk10_Payment.Payment_processing_fifo(p_from_period_id => p_bill_period_id);
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ �������
--
PROCEDURE Close_period(p_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_period';
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������� ���������������� ����������:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� � ������� �� ����������� ������ �������� ���.���
    -- BRM_KTTK, ������� �������� ��������������� �� �������� ���� "���������"
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Mark_bills( p_period_id );
     
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ����������� ����� � ������� �� ����������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Charge_fixrates( c_TASK_ID, p_period_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Make_bills(c_TASK_ID, p_period_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- �������� ��� ��� �������� �� �������� �� ����������� ������ �� �������������� �����
    -- �������� ������ �/� �� �������� ������ �/� �� ��������
    -- �������� ��� '�' �������������� FIFO, '�'-������ ����� ��� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Payment_processing( p_period_id );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� ���� ������� ������
    -- (����� ��� ������ ��������� ���.�����)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Period_info( c_TASK_ID, p_period_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� �������, ���� ������� ������ (����� READY - ������ � ������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Refresh_balance( c_TASK_ID );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���������������� ����������:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        -- ������� ���������������� ����������:
        -- ...
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


END PK30_BILLING_PERSONS;
/
