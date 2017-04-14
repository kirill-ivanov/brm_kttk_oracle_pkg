CREATE OR REPLACE PACKAGE PK30_BILLING_QUEUE_NEW
IS
    --
    -- ����� ��� ��������� �������� ����������� ������������/����������������
    -- ������� ������
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK30_BILLING_QUEUE_NEW';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����� ������������� �-� ������, ���������� ��������� �������:
    -- TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE
    -- INSERT INTO BILLING_QUEUE_T (BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID) ...
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ����� ������ ��� ���������� � �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Open_task RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ - ������� ������ �� ������� (��������� �����������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Close_task(p_task_id IN INTEGER);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ����������� ������� ������� � ������, � �������
    -- ��������� �������� ITEM-�, �������� � ���������� ���������
    --
    Procedure Fill_queue_sample(p_task_id IN INTEGER, p_period_id IN INTEGER);
    
    /*
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������������� ������ ������� � �������
    --
    Procedure Rollback_queue(p_period_id IN INTEGER, p_task_id IN INTEGER);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������������ ������� ��� ������ ������������ � �������
    --
    PROCEDURE Close_period_queue(p_period_id IN INTEGER, p_task_id IN INTEGER);
    */
    -- ===================================================================== --
    -- ������ �� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������������� ������ ������� � �������
    --
    Procedure Rollback_bills(p_task_id IN INTEGER, p_period_id IN INTEGER);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ������ ������������ � �������
    --
    PROCEDURE Close_bills(p_task_id IN INTEGER, p_period_id IN INTEGER);
    
    -- ===================================================================== --
    -- ������ � �������������� ������������ ������ + ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������������� ������������� ����������
    --
    Procedure Rollback_fixrates(p_task_id IN INTEGER, p_period_id IN INTEGER);
    
END PK30_BILLING_QUEUE_NEW;
/
CREATE OR REPLACE PACKAGE BODY PK30_BILLING_QUEUE_NEW
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ����� ������ ��� ���������� � �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Open_task RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Open_task';
    v_task_id     INTEGER;
BEGIN
    -- �������� ����� ������
    SELECT SQ_BILLING_QUEUE_T.NEXTVAL INTO v_task_id FROM DUAL;
    -- ��������� ��� �������� � ������� �����������
    Pk01_Syslog.Write_msg('task_id = '||v_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ���������� ���������
    RETURN v_task_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������ - ������� ������ �� ������� (��������� �����������)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Close_task(p_task_id IN INTEGER)
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    v_prcName     CONSTANT VARCHAR2(30) := 'Close_task';
    v_count       INTEGER := 0;
BEGIN
    -- ������� ������ �� �������
    DELETE FROM BILLING_QUEUE_T Q
     WHERE Q.TASK_ID = p_task_id;
    v_count := SQL%ROWCOUNT;
    COMMIT;
    -- ��������� ��� �������� � ������� �����������
    Pk01_Syslog.Write_msg('task_id = '||p_task_id||', deleted '||v_count||' rows', 
                                   c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Task_id = '||p_task_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ������������� �-� ������, ���������� ��������� �������:
-- TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE
-- INSERT INTO BILLING_QUEUE_T (BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID) ...
--
-- ������ ����������� ������� ������� � ������, � �������
-- ��������� �������� ITEM-�, �������� � ���������� ���������
Procedure Fill_queue_sample(p_task_id IN INTEGER, p_period_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Fill_queue_sample';
    v_period_from DATE;
    v_period_to   DATE;
    v_count       INTEGER;
    v_task_id     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);    
    --
    INSERT INTO BILLING_QUEUE_T(BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, TASK_ID, REP_PERIOD_ID)
    SELECT BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, p_task_id, REP_PERIOD_ID
      FROM (
        SELECT B.BILL_ID, B.REP_PERIOD_ID, B.ACCOUNT_ID, A.BILLING_ID, AP.PROFILE_ID,
               AP.DATE_FROM, 
               MAX(AP.DATE_FROM) OVER (PARTITION BY AP.ACCOUNT_ID) MAX_DATE_FROM
          FROM ITEM_T I, BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
         WHERE I.REP_PERIOD_ID = p_period_id
           AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND I.BILL_ID       = B.BILL_ID
           AND I.ITEM_STATUS   = Pk00_Const.c_ITEM_STATE_OPEN
           AND A.ACCOUNT_ID    = B.ACCOUNT_ID
           AND AP.ACCOUNT_ID   = A.ACCOUNT_ID
           AND AP.DATE_FROM   <= B.BILL_DATE
           AND (AP.DATE_TO IS NULL OR B.BILL_DATE <= AP.DATE_TO )
      )
      WHERE DATE_FROM = MAX_DATE_FROM -- ���������� ��������� �������� � ������� �������
    GROUP BY BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID;
    v_count := SQL%ROWCOUNT;
    
    Pk01_Syslog.Write_msg('Stop, '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

/*
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������������� ������ ������� � �������
--
Procedure Rollback_queue(p_period_id IN INTEGER, p_task_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_queue';
    v_period_id  INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk32_Billing_rollback.jur_rollback_billing_queue(p_period_id, p_task_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ������������� ����������, �� ����������� ��� ��� 
    -- ����������� ����������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    DELETE FROM ITEM_T I
    WHERE I.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_MIN, 
                            Pk00_Const.c_CHARGE_TYPE_REC)
    AND I.REP_PERIOD_ID = p_period_id
    AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
    AND I.EXTERNAL_ID IS NULL
    AND EXISTS (
        SELECT * FROM BILLING_QUEUE_T Q
         WHERE Q.BILL_ID = I.BILL_ID
    );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ ������� ��� ������ ������������ � �������
--
PROCEDURE Close_period_queue(p_period_id IN INTEGER, p_task_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_period';
    v_period_id  INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ����������� ����� � ������� �� ����������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Charge_fixrates( p_period_id, p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Make_bills( p_period_id, p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� ���� ������� ������,
    -- ��� ���� ������� �� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Period_info( p_period_id, p_task_id );
    
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;
*/
-- ===================================================================== --
-- ������ �� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������������� ������ ������� � �������
--
PROCEDURE Rollback_bills(p_task_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_bills';
    v_count      INTEGER;
    v_last_period_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, bill_period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� ������ ������ �� ������� ����� � �������� � �������� ���������
    MERGE INTO ITEM_T I
    USING (
       SELECT Q.BILL_ID, Q.REP_PERIOD_ID 
         FROM BILLING_QUEUE_T Q
        WHERE Q.TASK_ID = p_task_id
    ) Q
    ON (
       I.BILL_ID = Q.BILL_ID AND
       I.REP_PERIOD_ID = Q.REP_PERIOD_ID
    )
    WHEN MATCHED THEN UPDATE 
       SET I.INV_ITEM_ID = NULL,
           I.REP_GROSS   = NULL,
           I.REP_TAX     = NULL,
           I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('update item_t '||v_count||' rows - �������� ������� ������ ������ �� ������� �����', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
       
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ������ ������
    DELETE FROM INVOICE_ITEM_T II
     WHERE EXISTS (
           SELECT * FROM BILLING_QUEUE_T Q
            WHERE Q.TASK_ID       = p_task_id
              AND Q.REP_PERIOD_ID = II.REP_PERIOD_ID
              AND Q.BILL_ID       = II.BILL_ID

       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('delete invoice_item_t '||v_count||' rows - ������� ������� ������ ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� 
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
          WHERE Q.TASK_ID = p_task_id
            AND Q.BILL_ID = B.BILL_ID
            AND Q.REP_PERIOD_ID = B.REP_PERIOD_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('update bill_t '||v_count||' rows - �������� � �������� ��������� ������ �����', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

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
        SELECT * FROM BILLING_QUEUE_T BQ
         WHERE BQ.ACCOUNT_ID = A.ACCOUNT_ID
           AND BQ.TASK_ID    = p_task_id
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
-- ������������ ������ ������������ � �������
--
PROCEDURE Close_bills(p_task_id IN INTEGER, p_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_bills';
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Make_bills( p_task_id => p_task_id, 
                                  p_period_id => p_period_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� ���� ������� ������, ��� ���� ������� �� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Period_info( p_task_id => p_task_id, 
                                   p_period_id => p_period_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ===================================================================== --
-- ������ � �������������� ������������ ������ + ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������������� ������������� ����������,
-- �� ����������� ��� ��� ����������� ����������� �������
--
Procedure Rollback_fixrates(p_task_id IN INTEGER, p_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_fixrates';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    PK36_BILLING_FIXRATE.Rollback_fixrates(p_task_id   => p_task_id,
                                           p_period_id => p_period_id);
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

END PK30_BILLING_QUEUE_NEW;
/
