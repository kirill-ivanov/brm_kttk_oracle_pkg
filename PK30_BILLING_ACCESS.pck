CREATE OR REPLACE PACKAGE PK30_BILLING_ACCESS
IS
    --
    -- ����� ��� ��������� �������� ����������� ������ � �������� �������
    -- �������� BRM-KTTK ( � ��������������� ������ �� "���������" )
    -- ��� �������� ��.���
    -- ����������� ������ ����� ������� � �������: BILLING_QUEUE_T
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK30_BILLING_ACCESS';
    -- ==============================================================================
    -- � � � � �   � � � � � � � �
    -- ------------------------------------------------------------------------- --
    c_TASK_ID   constant integer := Pk00_Const.c_BILLING_ACCESS;
    
    -- ��������� ������� �� ����������� ������
    PROCEDURE Mark_bills( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������������ �������
    PROCEDURE Close_period(p_period_id IN INTEGER);

    
END PK30_BILLING_ACCESS;
/
CREATE OR REPLACE PACKAGE BODY PK30_BILLING_ACCESS
IS

-- ------------------------------------------------------------------------- --
-- ��������� � ������� �� ����������� ������ �������� ��.���
-- BRM_KTTK, ������� �������� ��������������� �� �������� ���� "���������"
-- ------------------------------------------------------------------------- --
PROCEDURE Mark_bills( p_period_id IN INTEGER )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Mark_bills';
    v_count       INTEGER;
    v_period_from DATE;
    v_period_to   DATE;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
    --
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);
    --
    -- ����� ������� � BRM-KTTK
    INSERT INTO BILLING_QUEUE_T (BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, TASK_ID)
    SELECT BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, c_TASK_ID
      FROM (
        SELECT B.BILL_ID, B.ACCOUNT_ID, A.BILLING_ID, AP.PROFILE_ID, 
               AP.DATE_FROM, MAX(AP.DATE_FROM) OVER (PARTITION BY AP.ACCOUNT_ID) MAX_DATE_FROM
          FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE B.REP_PERIOD_ID = p_period_id
           AND A.ACCOUNT_ID    = B.ACCOUNT_ID
           AND A.BILLING_ID    = Pk00_Const.c_BILLING_ACCESS
           AND A.ACCOUNT_TYPE  = Pk00_Const.c_ACC_TYPE_J
           AND A.STATUS        = Pk00_Const.c_ACC_STATUS_BILL
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_OPEN, Pk00_Const.c_BILL_STATE_EMPTY)
           AND AP.ACCOUNT_ID   = A.ACCOUNT_ID
           AND AP.DATE_FROM   <= v_period_to
           AND (AP.DATE_TO IS NULL OR v_period_from <= AP.DATE_TO )
    )
    WHERE DATE_FROM = MAX_DATE_FROM;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILLING_QUEUE_T');
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- ------------------------------------------------------------------------- --
-- ��������� � ������� �� ����������� ������ �������� ��.��� "�������" ��������
-- �� ������� �� ��������� �� ������� �� ������������ �������
-- ------------------------------------------------------------------------- --
PROCEDURE Mark_bills_simple( p_period_id IN INTEGER )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Mark_bills_simple';
    v_count       INTEGER;
    v_period_from DATE;
    v_period_to   DATE;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
    --
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);
    -- 
    INSERT INTO BILLING_QUEUE_T (BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, TASK_ID)
    SELECT BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, c_TASK_ID
      FROM (
        SELECT B.BILL_ID, B.ACCOUNT_ID, A.BILLING_ID, AP.PROFILE_ID, 
               AP.DATE_FROM, MAX(AP.DATE_FROM) OVER (PARTITION BY AP.ACCOUNT_ID) MAX_DATE_FROM
          FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE B.REP_PERIOD_ID = p_period_id
           AND A.ACCOUNT_ID    = B.ACCOUNT_ID
           AND A.BILLING_ID    = Pk00_Const.c_BILLING_ACCESS
           AND A.ACCOUNT_TYPE  = Pk00_Const.c_ACC_TYPE_J
           AND A.STATUS        = Pk00_Const.c_ACC_STATUS_BILL
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_OPEN, Pk00_Const.c_BILL_STATE_EMPTY)
           AND AP.ACCOUNT_ID   = A.ACCOUNT_ID
           AND AP.DATE_FROM   <= v_period_to
           AND (AP.DATE_TO IS NULL OR v_period_from <= AP.DATE_TO )
    ) BQ
    WHERE DATE_FROM = MAX_DATE_FROM
      AND NOT EXISTS (
        SELECT * FROM FIX_RATE_T FR, ORDER_T O
         WHERE FR.ORDER_ID = O.ORDER_ID
           AND O.DATE_FROM <= v_period_to 
           AND (O.DATE_TO IS NULL OR v_period_from <= O.DATE_TO)
           AND O.ACCOUNT_ID = BQ.ACCOUNT_ID
     )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILLING_QUEUE_T');
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- ------------------------------------------------------------------------- --
-- ��������� � ������� �� ����������� ������ �������� ��.��� "�������" ��������
-- ������� ������� �� ������������ �������
-- ------------------------------------------------------------------------- --
PROCEDURE Mark_bills_min( p_period_id IN INTEGER )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Mark_bills_min';
    v_count       INTEGER;
    v_period_from DATE;
    v_period_to   DATE;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
    --
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);
    -- 
    INSERT INTO BILLING_QUEUE_T (BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, TASK_ID)
    SELECT BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, c_TASK_ID
      FROM (
        SELECT B.BILL_ID, B.ACCOUNT_ID, A.BILLING_ID, AP.PROFILE_ID, 
               AP.DATE_FROM, MAX(AP.DATE_FROM) OVER (PARTITION BY AP.ACCOUNT_ID) MAX_DATE_FROM
          FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE B.REP_PERIOD_ID = p_period_id
           AND A.ACCOUNT_ID    = B.ACCOUNT_ID
           AND A.BILLING_ID    = Pk00_Const.c_BILLING_ACCESS
           AND A.ACCOUNT_TYPE  = Pk00_Const.c_ACC_TYPE_J
           AND A.STATUS        = Pk00_Const.c_ACC_STATUS_BILL
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_OPEN, Pk00_Const.c_BILL_STATE_EMPTY)
           AND AP.ACCOUNT_ID   = A.ACCOUNT_ID
           AND AP.DATE_FROM   <= v_period_to
           AND (AP.DATE_TO IS NULL OR v_period_from <= AP.DATE_TO )
           AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_REC
    ) BQ
    WHERE DATE_FROM = MAX_DATE_FROM
      AND EXISTS (
          SELECT * FROM FIX_RATE_T FR, ORDER_T O
           WHERE FR.ORDER_ID = O.ORDER_ID
             AND O.DATE_FROM <= v_period_to 
             AND (O.DATE_TO IS NULL OR v_period_from <= O.DATE_TO)
             AND O.ACCOUNT_ID = BQ.ACCOUNT_ID
             AND FR.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_MIN
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILLING_QUEUE_T');
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- ------------------------------------------------------------------------- --
-- ��������� � ������� �� ����������� ������ �������� ��.��� "�������" ��������
-- ������� ����������� �����
-- ------------------------------------------------------------------------- --
PROCEDURE Mark_bills_rec( p_period_id IN INTEGER )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Mark_bills_rec';
    v_count       INTEGER;
    v_period_from DATE;
    v_period_to   DATE;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
    --
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);
    -- 
    INSERT INTO BILLING_QUEUE_T (BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID)
    SELECT BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID
      FROM (
        SELECT B.BILL_ID, B.ACCOUNT_ID, A.BILLING_ID, AP.PROFILE_ID, 
               AP.DATE_FROM, MAX(AP.DATE_FROM) OVER (PARTITION BY AP.ACCOUNT_ID) MAX_DATE_FROM
          FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE B.REP_PERIOD_ID = p_period_id
           AND A.ACCOUNT_ID    = B.ACCOUNT_ID
           AND A.BILLING_ID    = Pk00_Const.c_BILLING_ACCESS
           AND A.ACCOUNT_TYPE  = Pk00_Const.c_ACC_TYPE_J
           AND A.STATUS        = Pk00_Const.c_ACC_STATUS_BILL
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_OPEN, Pk00_Const.c_BILL_STATE_EMPTY)
           AND AP.ACCOUNT_ID   = A.ACCOUNT_ID
           AND AP.DATE_FROM   <= v_period_to
           AND (AP.DATE_TO IS NULL OR v_period_from <= AP.DATE_TO )
           AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_REC
    ) BQ
    WHERE DATE_FROM = MAX_DATE_FROM
      AND EXISTS (
          SELECT * FROM FIX_RATE_T FR, ORDER_T O
           WHERE FR.ORDER_ID = O.ORDER_ID
             AND O.DATE_FROM <= v_period_to 
             AND (O.DATE_TO IS NULL OR v_period_from <= O.DATE_TO)
             AND O.ACCOUNT_ID = BQ.ACCOUNT_ID
             AND FR.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_REC
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILLING_QUEUE_T');
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ �������
--
PROCEDURE Close_period(p_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_period';
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� � ������� �� ����������� ������ �������� ���.���
    -- BRM_KTTK, ������� �������� ��������������� �� �������� ���� "���������"
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Mark_bills( p_period_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ����������� ����� � ������� �� ����������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Charge_fixrates( p_period_id, c_TASK_ID );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Make_bills(p_period_id, c_TASK_ID);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� ���� ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Period_info( p_period_id, c_TASK_ID );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


END PK30_BILLING_ACCESS;
/
