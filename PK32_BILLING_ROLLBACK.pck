CREATE OR REPLACE PACKAGE PK32_BILLING_ROLLBACK
IS
    --
    -- ����� ��� ��������� �������� ������ ����������� ������ � �������� �������
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK32_BILLING_ROLLBACK';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- �������� ������������ ������
    PROCEDURE Rollback_Billing;

    -- �������� �������� ����������� �������
    PROCEDURE Rollback_close_fin_period;
    
   
END PK32_BILLING_ROLLBACK;
/
CREATE OR REPLACE PACKAGE BODY PK32_BILLING_ROLLBACK
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
-- ����������� �������, ���� ������� ������
-- � ������ ������ �������� ������ + ������������ ����� + ��� ������� �������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Refresh_balance
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Refresh_balance';
    v_count          INTEGER;
    v_last_period_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
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

-- ======================================================================= --
-- ����� �������� �������� �������
-- ======================================================================= --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� �������� ����������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_close_fin_period
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_close_fin_period';
    v_last_period_id INTEGER;
    v_bill_period_id INTEGER; 
    v_count          INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������� �������� � ���������� �������, ������� ����������� ������
    UPDATE PERIOD_T P
       SET POSITION = Pk00_Const.c_PERIOD_BILL,
           P.CLOSE_REP_PERIOD = NULL,
           P.CLOSE_FIN_PERIOD = NULL
     WHERE POSITION = Pk00_Const.c_PERIOD_LAST
     RETURNING PERIOD_ID INTO v_bill_period_id;
    --
    v_last_period_id := PK04_PERIOD.Make_prev_id(v_bill_period_id);
    --
    UPDATE PERIOD_T
       SET POSITION  = Pk00_Const.c_PERIOD_LAST
     WHERE PERIOD_ID = v_last_period_id;
    -- 
    Pk01_Syslog.Write_msg('bill_period_id='||v_bill_period_id||
                          'last_period_id='||v_last_period_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ����� �������� ������� (��������� � � ��� ���������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE BILL_T 
       SET BILL_STATUS = Pk00_Const.c_BILL_STATE_READY
     WHERE REP_PERIOD_ID = v_bill_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('����������� '||v_count||' ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ������� ����� �������� ������� (��������� � � ��� ���������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE ITEM_T 
       SET ITEM_STATUS = Pk00_Const.c_ITEM_STATE_RE�DY
     WHERE REP_PERIOD_ID = v_bill_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('����������� '||v_count||' ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������ �� �������� �� ������ ���������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE PAYMENT_T P SET P.ADVANCE = 0, P.ADVANCE_DATE = NULL
     WHERE P.REP_PERIOD_ID = v_bill_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('������� ������ ��� '||v_count||' ��������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������ �� ������ ��� ���� ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    DELETE FROM REP_PERIOD_INFO_T
    WHERE REP_PERIOD_ID = v_bill_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('������� '||v_count||' ������� ��������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �� ������� ���� �������, ���� ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ ������, ��� ������������ �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_Billing
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_Billing';
    v_period_id  INTEGER;
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ID ������������ �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_period_id := Period_for_close;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('bill_period_id = '||v_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- �������� �������� ������ �������� � ������������ �������
    -- �������� ������ �/� �� �������� ������ �/� �� ��������
    -- �������� ��� '�' �������������� FIFO, '�'-������ ����� ��� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� �������� �� �������, ����������� �� ����� ������������ �������
    MERGE INTO PAYMENT_T P
    USING 
    (
        SELECT TRANSFER_ID, PAYMENT_ID, PAY_PERIOD_ID, BILL_ID, REP_PERIOD_ID,
               TRANSFER_TOTAL, TRANSFER_DATE, NOTES
          FROM PAY_TRANSFER_T
         WHERE REP_PERIOD_ID = v_period_id
        ORDER BY TRANSFER_DATE DESC, TRANSFER_ID DESC
    ) PT ON (P.PAYMENT_ID = PT.PAYMENT_ID AND P.REP_PERIOD_ID = PT.PAY_PERIOD_ID)
    WHEN MATCHED THEN UPDATE 
        SET P.TRANSFERED = P.TRANSFERED - PT.TRANSFER_TOTAL, 
            P.BALANCE = P.BALANCE + PT.TRANSFER_TOTAL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('update payment_t '||v_count||' rows - ���������� �������� �� ������� ������������� ��������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ���������������� ������ �������� ��������
    DELETE FROM PAY_TRANSFER_T PT
     WHERE REP_PERIOD_ID = v_period_id; 
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('delete from pay_transfer_t '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info ); 

    -- �������� ������� ������������ ������� � �������� ��������� (�� ������ ������)
    UPDATE PAYMENT_T P
       SET P.BALANCE = P.RECVD,
           P.ADVANCE = 0,
           p.ADVANCE_DATE = p.PAYMENT_DATE,
           P.TRANSFERED = 0,
           P.DATE_FROM  = NULL,
           P.DATE_TO    = NULL
     WHERE P.REP_PERIOD_ID = v_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('update payment_t '||v_count||' rows - �������� ������� ������������ ������� � �������� ���������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� ������ ������ �� ������� ����� � �������� � �������� ���������
    UPDATE ITEM_T I
       SET I.INV_ITEM_ID = NULL,
           I.REP_GROSS   = NULL,
           I.REP_TAX     = NULL,
           I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
     WHERE I.REP_PERIOD_ID = v_period_id
       AND I.ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, Pk00_Const.c_ITEM_TYPE_ADJUST);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('update item_t '||v_count||' rows - �������� ������� ������ ������ �� ������� �����', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
       
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ������ ������
    DELETE FROM INVOICE_ITEM_T II
     WHERE II.REP_PERIOD_ID = v_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('update item_t '||v_count||' rows - ������� ������� ������ ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

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
     WHERE B.REP_PERIOD_ID = v_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('update bill_t '||v_count||' rows - �������� � �������� ��������� ������ �����', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� �������, ���� ������� ������ (����� READY - ������ � ������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --Refresh_balance;
    --
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

/*
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ ������ ��� ��. ��� ������������ � �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE JUR_Rollback_Billing_queue(p_period_id IN INTEGER, p_task_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'JUR_Rollback_Billing_queue';
    v_count      INTEGER;
    v_period_id  INTEGER;
    v_last_period_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ID ������������ �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_period_id := p_period_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('bill_period_id = '||v_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� ������ ������ �� ������� ����� � �������� � �������� ���������
    UPDATE ITEM_T I
       SET I.INV_ITEM_ID = NULL,
           I.REP_GROSS   = NULL,
           I.REP_TAX     = NULL,
           I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
     WHERE I.REP_PERIOD_ID = v_period_id
       AND I.ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, Pk00_Const.c_ITEM_TYPE_ADJUST)
       AND EXISTS (
           SELECT * FROM BILLING_QUEUE_T BQ
            WHERE BQ.BILL_ID = I.BILL_ID
              AND BQ.TASK_ID = p_task_id
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('update item_t '||v_count||' rows - �������� ������� ������ ������ �� ������� �����', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
       
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ������ ������
    DELETE FROM INVOICE_ITEM_T II
     WHERE II.REP_PERIOD_ID = v_period_id
       AND EXISTS (
           SELECT * FROM BILLING_QUEUE_T BQ
            WHERE BQ.BILL_ID = II.BILL_ID
              AND BQ.TASK_ID = p_task_id
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('update item_t '||v_count||' rows - ������� ������� ������ ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

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
     WHERE B.REP_PERIOD_ID = v_period_id
       AND EXISTS (
         SELECT * FROM BILLING_QUEUE_T BQ
          WHERE BQ.BILL_ID = B.BILL_ID
            AND BQ.TASK_ID = p_task_id
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
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;
*/


END PK32_BILLING_ROLLBACK;
/
