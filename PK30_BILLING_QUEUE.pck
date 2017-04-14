CREATE OR REPLACE PACKAGE PK30_BILLING_QUEUE
IS
    --
    -- ����� ��� ��������� �������� ����������� ������������/����������������
    -- ������� ������
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK30_BILLING_QUEUE';
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
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� �� ������������ �������� ������, ������� � �������
    -- � ������� �� ������ ���� ������-��� � ������ �� �������� ��������
    PROCEDURE Check_queue_bills(p_task_id IN INTEGER);
    
    /*
    -- ===================================================================== --
    -- ������ �� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������������� ������ ������� � �������
    --
    Procedure Rollback_bills(p_task_id IN INTEGER);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ������ ������������ � �������
    --
    PROCEDURE Make_bills(p_task_id IN INTEGER);
        
    -- ===================================================================== --
    -- ������ � �������������� ������������ ������ + ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������������� ������������� ����������
    --
    Procedure Rollback_fixrates(p_task_id IN INTEGER);
    */
    
END PK30_BILLING_QUEUE;
/
CREATE OR REPLACE PACKAGE BODY PK30_BILLING_QUEUE
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
--    PRAGMA AUTONOMOUS_TRANSACTION;
    v_prcName     CONSTANT VARCHAR2(30) := 'Close_task';
    v_count       INTEGER := 0;
BEGIN
    -- ������� ������ �� �������
    DELETE FROM BILLING_QUEUE_T Q
     WHERE Q.TASK_ID = p_task_id;
    v_count := SQL%ROWCOUNT;
--    COMMIT;
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
    v_count       INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    INSERT INTO BILLING_QUEUE_T(BILL_ID, ACCOUNT_ID, 
                TASK_ID, REP_PERIOD_ID)
    SELECT B.BILL_ID, B.ACCOUNT_ID, p_task_id, B.REP_PERIOD_ID
      FROM ITEM_T I, BILL_T B
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND I.BILL_ID       = B.BILL_ID
       AND I.ITEM_STATUS   = Pk00_Const.c_ITEM_STATE_OPEN
    ;
    v_count := SQL%ROWCOUNT;
    
    Pk01_Syslog.Write_msg('Stop, '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ===================================================================== --
-- ������ �� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� �� ������������ �������� ������, ������� � �������
-- � ������� �� ������ ���� ������-��� � ������ �� �������� ��������
--
PROCEDURE Check_queue_bills(p_task_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_queue_bills';
    v_count      INTEGER;
    v_qcount     INTEGER;
    v_last_period_id INTEGER;
BEGIN
    MERGE INTO BILLING_QUEUE_T Q
    USING (
        SELECT Q.BILL_ID,
               CASE
                 WHEN P.POSITION IS NULL OR P.POSITION  = 'LAST' 
                   THEN 'Bill period closed'
                 WHEN B.BILL_TYPE = Pk00_Const.c_BILL_TYPE_CRD 
                   THEN 'Bill is credit note'
               END MSG
          FROM BILLING_QUEUE_T Q, BILL_T B, PERIOD_T P, ACCOUNT_T A
         WHERE Q.TASK_ID       = p_task_id
           AND Q.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND Q.BILL_ID       = B.BILL_ID
           AND Q.REP_PERIOD_ID = P.PERIOD_ID
           AND Q.ACCOUNT_ID    = A.ACCOUNT_ID
           AND A.BILLING_ID   != Pk00_Const.c_BILLING_RP -- 2008 - � �������� ����� ���
           AND (
               P.POSITION IS NULL  OR 
               P.POSITION  = 'LAST' OR
               B.BILL_TYPE = Pk00_Const.c_BILL_TYPE_CRD
           )
    ) E
    ON (
       Q.BILL_ID = E.BILL_ID
    )
    WHEN MATCHED THEN UPDATE SET Q.ERR = -1, Q.MSG = E.MSG;
    v_count := SQL%ROWCOUNT;
    -- ��������� ������ � ������� �����������
    IF v_count > 0 THEN
      -- ���-�� ������� � �������, ���� ����, �� �������� ����������
      SELECT COUNT(*) 
        INTO v_qcount
        FROM BILLING_QUEUE_T Q
       WHERE Q.TASK_ID = p_task_id;  
      --    
      FOR re IN (
        SELECT * FROM BILLING_QUEUE_T Q
         WHERE Q.ERR IS NOT NULL 
      )
      LOOP
          -- ��������� ������ � ������� �����������
          Pk01_Syslog.Write_msg('Bill_id='||re.bill_id
                              ||', period='||re.rep_period_id
                              ||' error: '||re.msg
                               ,c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );
      END LOOP;
      
      -- ������� ��������� ������ �� �������
      DELETE FROM BILLING_QUEUE_T Q 
       WHERE Q.TASK_ID = p_task_id
         AND Q.ERR IS NOT NULL;
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('BILLING_QUEUE_T: '||v_count||' error rows deleted',
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );
                               
      -- ���� ������� ������ ���������� ����������
      IF v_qcount = v_count THEN
            Pk01_Syslog.Raise_user_exception('Billin_queue_error is empty',
                                             c_PkgName||'.'||v_prcName);
      END IF;
    END IF;
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

/*
-- ===================================================================== --
-- ������ �� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������������� ������ ������� � �������
--
PROCEDURE Rollback_bills(p_task_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_bills';
    v_count      INTEGER;
    v_qcount     INTEGER;
    v_last_period_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ���������������� ������-���� � ����� �� �������� ��������
    Check_queue_bills(p_task_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� ������ ������ �� ������� ����� � �������� � �������� ���������
    UPDATE ITEM_T I
       SET I.INV_ITEM_ID = NULL,
           I.REP_GROSS   = NULL,
           I.REP_TAX     = NULL,
           I.BILL_TOTAL  = NULL,
           I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
     WHERE I.ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, Pk00_Const.c_ITEM_TYPE_ADJUST)
       AND EXISTS (
           SELECT * FROM BILLING_QUEUE_T Q
            WHERE Q.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND Q.BILL_ID       = I.BILL_ID
              AND Q.TASK_ID       = p_task_id
              AND Q.ERR IS NULL
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
              AND Q.ERR IS NULL
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
          WHERE Q.REP_PERIOD_ID = B.REP_PERIOD_ID
            AND Q.BILL_ID = B.BILL_ID
            AND Q.TASK_ID = p_task_id
            AND Q.ERR IS NULL
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
        SELECT * FROM BILLING_QUEUE_T Q
         WHERE Q.ACCOUNT_ID = A.ACCOUNT_ID
           AND Q.TASK_ID    = p_task_id
           AND Q.ERR IS NULL
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
PROCEDURE Make_bills(p_task_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_bills';
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ������������� ������-���� � ����� �� �������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Check_queue_bills( p_task_id => p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Make_bills( p_task_id => p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� ���� ������� ������, ��� ���� ������� �� �����
    -- �������� �����!!! ���������� ������ ���������� � ����� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --PK30_BILLING_BASE.Period_info( p_task_id => p_task_id );
    
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
Procedure Rollback_fixrates( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_fixrates';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    PK36_BILLING_FIXRATE.Rollback_fixrates( p_task_id   => p_task_id );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;
*/

END PK30_BILLING_QUEUE;
/
