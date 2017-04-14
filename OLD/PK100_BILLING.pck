CREATE OR REPLACE PACKAGE PK100_BILLING
IS
    --
    -- ����� ��� ��������� �������� ����������� ������ � �������� �������
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK100_BILLING';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���������� �������� �������:
    -- 1) Next_Bill_Period - ����������� � 00:01 UTS - ������� ������� ���� ������
    -- 2) Billing - ����������� � 02:00 UTS (�� ��������� �������� � ����������� �������� ������� �� ���������� �����)
    -- 3) Invoicing - �� ��������� ��������
    -- 4) Payment_processing - �������������� �������� �������� '�' �� �������������� �����
    -- 5) Refresh_balance - ����������� �������, ���� ������� ������ (����� READY - ������ � ������)
    -- 6) ����� � ��������� 'READY' - ����������, ����������, ������������ ��������
    -- 7) Close_Financial_Period - ���-�� 6-7 ����� ����������� ���. ������, ��� ����:
    -- 7.1) Pk04_Period.Close_fin_period - ������� ���������� ������: FIN_PERIOD = BILL_PERIOD_LAST    
    -- 7.2) ������� ������� ����� �������� ������� (��������� � ��� ��� ���������)
    -- 7.3) Calc_advance - ��������� ������ �� �������� �� ������ ���������� ������
    -- 7.4) Period_info - ���������� ������ �� ������ ��� ���� ������� ������
    -- 7.5) Refresh_balance - ����������� �������, ���� ������� ������
    -- � �������� ��������, 
    --   * ���������� ��������� ������, 
    --   * ������� ������, 
    --   * �������� �������� �������� �������� 
    --   * ��������� ��������� ������������ ����� 
    -- ����������!!!
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �������� �� ��������� ����������� ������ (������� ����� �����)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Next_Bill_Period;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������� ����� � �������������
    -- ��������� ���� � ��������� READY
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Prep_items( p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ����� ������-������ �� �������������� ������ 
    -- �� ����������� ������ (last_period)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Invoicing( p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� - ��������� ���� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Billing( p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- �������� ��� ��� �������� �� �������� �� ����������� ������ �� �������������� �����
    -- �������� ������ �/� �� �������� ������ �/� �� ��������
    -- �������� ��� '�' �������������� FIFO, '�'-������ ����� ��� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Payment_processing( p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ �� �������� �� ������ ������
    PROCEDURE Calc_advance( p_bill_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� �������� �����
    PROCEDURE Account_period_info (
              p_account_id     IN INTEGER,
              p_period_id      IN INTEGER
        );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� ���� ������� ������
    PROCEDURE Period_info( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� �������, ���� ������� ������
    -- � ������ ������ �������� ������ + ������������ ����� + ��� ������� �������� �������
    PROCEDURE Refresh_balance;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������������ �������
    PROCEDURE Close_period;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ����������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Close_Financial_Period;
    
    -- ======================================================================= --
    -- �������� �������� �� ������� �����������
    -- ======================================================================= --
    -- ���������� � �������� �������
    --   - ������������� - ���-�� ��������� �������
    --   - ��� ������ ���������� ����������
    PROCEDURE Period_list( 
                   p_recordset OUT t_refc
               );

    -- �������� ��������� � ������� ����������� ��� ��������� �������
    PROCEDURE Msg_list(
                  p_recordset OUT t_refc,
                  p_function   IN VARCHAR2,                   -- ��� �������
                  p_date_from  IN DATE DEFAULT (SYSDATE-30)   -- ����� ������ ������� (���������������)
               );
               
    -- �������� ������� �������� �������������
    PROCEDURE Billing_history( 
                   p_recordset OUT t_refc, 
                   p_months    IN INTEGER    -- ���-�� ������� �����
               );
    
    -- �������� �������� �� ������������ ������ 
    -- �������� ����� ��������� ������ ��� ������ ������������� � �������� �����
    PROCEDURE Check_period_info( 
                   p_recordset OUT t_refc, 
                   p_months    IN INTEGER    -- ���-�� ������� �����
               );
    
    -- �������� ������ ������, ���������� � ������ ������ 
    PROCEDURE Err_bill_list( 
                   p_recordset OUT t_refc, 
                   p_period_id  IN INTEGER    -- ID ��������� �������
               );
    
    -- ========================================================================= --
    --                               � � � � � � �
    -- ========================================================================= --
    -- ������ �������� �������� 
    -- ��� ��������������� ��������, ����������� ������ � ������ �������
    PROCEDURE Recalc_period_info ( p_period_id IN INTEGER );

    -- ������ �������� �������
    -- ��� ��������������� ��������, ����������� ������ � ������ �������
    PROCEDURE Recalc_all_balances;
    
END PK100_BILLING;
/
CREATE OR REPLACE PACKAGE BODY PK100_BILLING
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
    v_prcName  CONSTANT VARCHAR2(30) := 'Next_Bill_Period';
    v_bill_id  INTEGER;
    v_ok       INTEGER := 0;
    v_err      INTEGER := 0;
    v_count    INTEGER := 0;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ��������� ������� �� ��������� ����������� ������
    Pk04_Period.Next_bill_period;
    
    -- ������� �������� ������� ���� ������� ������ �� ��������� ������ 
    -- ( ���� ������ �����-������ ����� ������ ������ ������ - ������� ���.�������)
    UPDATE BILLINFO_T
       SET LAST_PERIOD_ID = PERIOD_ID, 
           PERIOD_ID      = NEXT_PERIOD_ID, 
           NEXT_PERIOD_ID = NULL,    -- ������ ��������� ������������� ��� ������� ����� 
           LAST_BILL_ID   = BILL_ID, 
           BILL_ID        = NEXT_BILL_ID, 
           NEXT_BILL_ID   = NULL
     WHERE BILL_ID IS NOT NULL;
    v_count := SQL%ROWCOUNT;
    COMMIT;
    Pk01_Syslog.Write_msg('Next period is set for the '||v_count||' accounts', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������� ����� � ��������� �������, ��� �������� �/�
    FOR l_cur IN ( 
        SELECT ACCOUNT_ID 
          FROM ACCOUNT_T 
         WHERE STATUS = Pk00_Const.c_ACC_STATUS_BILL 
      )
    LOOP
        SAVEPOINT X;  -- ����� ���������� ������ ��� �������� �����
        BEGIN
            v_bill_id := Pk07_Bill.Bill_for_next_period(l_cur.account_id);
            v_ok := v_ok + 1;
        EXCEPTION
            WHEN OTHERS THEN
              -- ����� ��������� ��� �������� �����
              ROLLBACK TO X;
              -- ��������� ������ � ������� �����������
              Pk01_Syslog.Write_msg(
                 p_Msg  => 'Account_id =' || l_cur.account_id || ' - error',
                 p_Src  => c_PkgName||'.'||v_prcName,
                 p_Level=> Pk01_Syslog.L_err );
              v_err := v_err + 1;
        END;
        v_count := v_ok + v_err;
        IF MOD(v_count, 10000) = 0 THEN
            Pk01_Syslog.Write_msg('Processed: '||v_count||'-rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
    END LOOP;
    --
    COMMIT;
    --
    Pk01_Syslog.Write_msg('Processed: '||v_ok||'-��, '||v_err||'-err from '||v_count, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ������� ����� � �������������
-- ��������� ���� � ��������� READY
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Prep_items( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Prep_items';
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ��������� ������� ���������� � ������������� � ��������� READY
    -- ��������� ���� ������� ������� ���������� � ������������� ��� ������� �������
    UPDATE ITEM_T I 
       SET (I.REP_GROSS, I.REP_TAX, I.ITEM_STATUS) = (
            SELECT 
                   CASE
                      WHEN I.TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN
                        (I.ITEM_TOTAL - PK09_INVOICE.ALLOCATE_TAX(I.ITEM_TOTAL, AP.VAT))
                      ELSE 
                        I.ITEM_TOTAL
                   END REP_GROSS,
                   CASE
                      WHEN I.TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN
                        (PK09_INVOICE.ALLOCATE_TAX(I.ITEM_TOTAL, AP.VAT))
                      ELSE 
                        PK09_INVOICE.CALC_TAX(I.ITEM_TOTAL, AP.VAT)
                   END REP_TAX,
                   Pk00_Const.c_ITEM_STATE_RE�DY                    
              FROM ACCOUNT_PROFILE_T AP, BILL_T B
             WHERE B.REP_PERIOD_ID = I.REP_PERIOD_ID
               AND B.BILL_ID       = I.BILL_ID
               AND B.ACCOUNT_ID    = AP.ACCOUNT_ID
               AND AP.DATE_FROM    < B.BILL_DATE
               AND (AP.DATE_TO IS NULL OR B.BILL_DATE <= AP.DATE_TO)
       ) 
    WHERE I.REP_PERIOD_ID = p_bill_period_id
      AND I.ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, 
                          Pk00_Const.c_ITEM_TYPE_ADJUST)
      AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN;
                          
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Move status for '||v_count||' - items to '
                          || Pk00_Const.c_ITEM_STATE_RE�DY, 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ����������� ������� READY � EMPTY, ������ ����� � ���������� �������� �� ���������
    -- (��������� � ����� �����, � �� ���������� �����)
    UPDATE BILL_T B
       SET (B.BILL_STATUS, B.TOTAL, B.GROSS, B.TAX, B.DUE, B.DUE_DATE, B.CALC_DATE) = (
           SELECT
               CASE 
                   WHEN EXISTS (
                       SELECT * FROM ITEM_T I 
                        WHERE I.BILL_ID = B.BILL_ID
                          AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
                          AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_RE�DY
                   )
                   THEN Pk00_Const.c_BILL_STATE_READY
                   ELSE Pk00_Const.c_BILL_STATE_EMPTY
               END, 0, 0, 0, 0, SYSDATE, SYSDATE
           FROM DUAL 
       )
    WHERE B.REP_PERIOD_ID = p_bill_period_id
      AND B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN;
    --
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
-- ������������ ����� ������-������ �� ������ (last_period)
-- �� ������� ������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Invoicing( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Invoicing';
    v_ok         INTEGER := 0;     -- ���-�� �������������� �������� (�� ������)
    v_err        INTEGER := 0;     -- ���-�� ������ ��� ������������ ��������
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ��������� ������� ��� ���� �������� ������������� ������ ������������ �������
    FOR r_bill IN (
        SELECT B.BILL_ID
          FROM BILL_T B
         WHERE B.REP_PERIOD_ID= p_bill_period_id 
           AND B.BILL_STATUS  = Pk00_Const.c_BILL_STATE_READY
      )
    LOOP
        SAVEPOINT X;  -- ����� ���������� ������ ��� �������� �����
        BEGIN
            v_count := Pk09_Invoice.Calc_invoice(
                             p_bill_id       => r_bill.bill_id,
                             p_rep_period_id => p_bill_period_id
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
            UPDATE BILL_T SET BILL_STATUS = Pk00_Const.c_BILL_STATE_ERROR
             WHERE BILL_ID       = r_bill.bill_id
               AND REP_PERIOD_ID = p_bill_period_id;
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
PROCEDURE Billing( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing';
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
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
        B.DUE_DATE  = B.BILL_DATE,
        B.CALC_DATE = SYSDATE
    WHERE B.REP_PERIOD_ID = p_bill_period_id
      AND B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_READY;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||' - bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- ��������� ���� �����, ��� ������ ������
    UPDATE BILL_T B
       SET B.PAID_TO   = NULL, 
           B.DUE_DATE  = SYSDATE, 
           B.CALC_DATE = SYSDATE
     WHERE B.REP_PERIOD_ID = p_bill_period_id
       AND B.BILL_STATUS = Pk00_Const.c_BILL_STATE_EMPTY;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||' - empty bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
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
    v_pay_due    NUMBER;     -- ������� �� �������� ����� ��������
    v_ok         INTEGER;    
    v_err        INTEGER;
    v_prev_period_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id <= '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- �������� ������� FIFO ������� ���������� ���
    Pk10_Payment.Payment_processing_fifo(p_from_period_id => p_bill_period_id);

    /*
    v_ok := 0;    
    v_err:= 0;
    --
    -- �������� ������ ������� �������� �� �������������� � ����������� ������� ����� ���. ���
    FOR r_adv IN (
        SELECT P.PAYMENT_ID, P.ACCOUNT_ID, P.REP_PERIOD_ID
          FROM PAYMENT_T P, ACCOUNT_T A, BILL_T B
         WHERE P.REP_PERIOD_ID <= p_bill_period_id
           AND P.BALANCE > 0
           AND P.ACCOUNT_ID = A.ACCOUNT_ID
           AND A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_P
           AND A.ACCOUNT_ID = B.ACCOUNT_ID
           AND B.REP_PERIOD_ID = p_bill_period_id
           AND B.DUE < 0
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_READY, Pk00_Const.c_BILL_STATE_CLOSED)
        ORDER BY P.REP_PERIOD_ID, P.ACCOUNT_ID, P.PAYMENT_DATE, P.PAYMENT_ID
      )
    LOOP
        SAVEPOINT X1;  -- ����� ���������� ������ ��� �������� �����
        BEGIN
            -- �������� ������� �������� �� �������� ������ 
            -- (��� ������� ������� FIFO, ��� ������, ������ ������ ����� ���)
            v_pay_due := Pk10_Payment.Transfer_to_account_fifo(
                   p_payment_id   => r_adv.payment_id,   -- ������
                   p_pay_period_id=> r_adv.rep_period_id,-- ������ �������
                   p_account_id   => r_adv.account_id    -- ������� ����, ����� �������� ����������
               );
            v_ok := v_ok + 1;         -- ������ ������ �������
        EXCEPTION
            WHEN OTHERS THEN
              -- ����� ��������� ��� �������� �����
              ROLLBACK TO X1;
              -- ��������� ������ � ������� �����������
              Pk01_Syslog.Write_msg(
                 p_Msg  => 'account_id='  ||r_adv.account_id
                        || ', period_id=' ||r_adv.rep_period_id
                        || ', payment_id='||r_adv.payment_id 
                        || ' - error',
                 p_Src  => c_PkgName||'.'||v_prcName,
                 p_Level=> Pk01_Syslog.L_err );
              v_err := v_err + 1;
        END;  
        -- ����������� ����������
        IF MOD((v_ok+v_err), 5000) = 0 THEN
            Pk01_Syslog.Write_msg('Processed: '||v_ok||'-ok, '||v_err||'-err advances', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        --
    END LOOP;
    --
    v_ok := 0;    
    v_err:= 0;
    --
    -- �������� ������� �������� � ����������� ������� �� ����� ���. ���,
    -- ������������ � ��������� � ����������� ��������
    v_prev_period_id := PK04_PERIOD.Make_prev_id(p_bill_period_id);
    --
    FOR r_pay IN (
        SELECT P.PAYMENT_ID, P.ACCOUNT_ID, P.REP_PERIOD_ID  
          FROM PAYMENT_T P, ACCOUNT_T A, REP_PERIOD_INFO_T PI  
         WHERE A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_P
           AND P.ACCOUNT_ID = A.ACCOUNT_ID
           AND P.REP_PERIOD_ID = p_bill_period_id
           AND P.BALANCE > 0 
           AND PI.ACCOUNT_ID = A.ACCOUNT_ID
           AND PI.REP_PERIOD_ID = v_prev_period_id
           AND PI.CLOSE_BALANCE < 0
        ORDER BY P.REP_PERIOD_ID, P.ACCOUNT_ID, P.PAYMENT_DATE, P.PAYMENT_ID
      )
    LOOP
        SAVEPOINT X2;  -- ����� ���������� ������ ��� �������� �����
        BEGIN
            -- �������� ������� �������� �� �������� ������ 
            -- (��� ������� ������� FIFO, ��� ������, ������ ������ ����� ���)
            v_pay_due := Pk10_Payment.Transfer_to_account_fifo(
                   p_payment_id   => r_pay.payment_id,   -- ������
                   p_pay_period_id=> r_pay.rep_period_id,-- ������ �������
                   p_account_id   => r_pay.account_id    -- ������� ����, ����� �������� ����������
               );
            v_ok := v_ok + 1;         -- ������ ������ �������
        EXCEPTION
            WHEN OTHERS THEN
              -- ����� ��������� ��� �������� �����
              ROLLBACK TO X2;
              -- ��������� ������ � ������� �����������
              Pk01_Syslog.Write_msg(
                 p_Msg  => 'account_id='  ||r_pay.account_id
                        || ', period_id=' ||r_pay.rep_period_id
                        || ', payment_id='||r_pay.payment_id 
                        || ' - error',
                 p_Src  => c_PkgName||'.'||v_prcName,
                 p_Level=> Pk01_Syslog.L_err );
              v_err := v_err + 1;
        END;  
        -- ����������� ����������
        IF MOD((v_ok+v_err), 5000) = 0 THEN
            Pk01_Syslog.Write_msg('Processed: '||v_ok||'-ok, '||v_err||'-err payments', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        --
    END LOOP;
    --
    Pk01_Syslog.Write_msg('Processed: ok='||v_ok||', err='||v_err||' -payments', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    */
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
PROCEDURE Calc_advance( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Calc_advance';
    v_count      INTEGER;
    v_period     DATE;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    v_period := PK04_PERIOD.Period_from(p_bill_period_id);
    --
    UPDATE PAYMENT_T P SET P.ADVANCE = P.BALANCE, P.ADVANCE_DATE = v_period
     WHERE P.REP_PERIOD_ID= p_bill_period_id
       AND P.BALANCE > 0;
    v_count := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg('Stop, processed: '||v_count||' payments', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ������ �� ������ ��� �������� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Account_period_info (
          p_account_id     IN INTEGER,
          p_period_id      IN INTEGER
    )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Account_period_info';
    v_open_balance   NUMBER;
    v_close_balance  NUMBER;
    v_total          NUMBER;
    v_gross          NUMBER;
    v_tax            NUMBER; 
    v_recvd          NUMBER;
    v_advance        NUMBER;
    v_last_modified  DATE:= NULL;
    v_prev_period_id INTEGER;
BEGIN
    -- ��������� ID �����������, �� �����������, �������
    v_prev_period_id := PK04_PERIOD.Make_prev_id(p_period_id);
    -- �������� ��������� ������ ����������� ������� - �������� ��������
    SELECT NVL(SUM(CLOSE_BALANCE),0)
      INTO v_open_balance
      FROM REP_PERIOD_INFO_T
     WHERE ACCOUNT_ID = p_account_id
       AND REP_PERIOD_ID = v_prev_period_id;
    -- �������� ������ � ����������� �� ������ �� ������ ������
    SELECT NVL(SUM(B.TOTAL),0), NVL(SUM(B.GROSS),0), NVL(SUM(B.TAX),0)
      INTO v_total, v_gross, v_tax  
      FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_period_id
       AND B.ACCOUNT_ID = p_account_id;
    -- �������� ����� �������� � ������� �� ��������� ������
    SELECT NVL(SUM(P.RECVD),0), NVL(SUM(P.ADVANCE),0)
      INTO v_recvd, v_advance
      FROM PAYMENT_T P
     WHERE P.ACCOUNT_ID = p_account_id
       AND P.REP_PERIOD_ID = p_period_id;
    -- ������ �� ����� �������
    v_close_balance := v_open_balance - v_total + v_recvd;
    -- ��������� ���������
    INSERT INTO REP_PERIOD_INFO_T(
        REP_PERIOD_ID, ACCOUNT_ID, OPEN_BALANCE, CLOSE_BALANCE, 
        TOTAL, GROSS, RECVD, ADVANCE, LAST_MODIFIED
    )
    VALUES(
        p_period_id, p_account_id, v_open_balance, v_close_balance,
        v_total, v_gross, v_recvd, v_advance, v_last_modified
    );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR account_id='||p_account_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ������ �� ������ ��� ���� ������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Period_info( p_period_id IN INTEGER )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Period_info';
    v_count          INTEGER;
    v_prev_period_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ��������� ID �����������, �� �����������, �������
    v_prev_period_id := PK04_PERIOD.Make_prev_id(p_period_id);
    -- ������������� ������� �� ���� �/� ������� ������ 'BILL'
    INSERT INTO REP_PERIOD_INFO_T(
        REP_PERIOD_ID, ACCOUNT_ID, OPEN_BALANCE, CLOSE_BALANCE, 
        TOTAL, GROSS, RECVD, ADVANCE, LAST_MODIFIED
    )
    SELECT  p_period_id, ACCOUNT_ID, OPEN_BALANCE, 
            (OPEN_BALANCE-TOTAL+RECVD) CLOSE_BALANCE, 
            TOTAL, GROSS, RECVD, ADVANCE, SYSDATE LAST_MODIFIED 
    FROM ( 
        SELECT B.ACCOUNT_ID, NVL(SUM(R.CLOSE_BALANCE),0) OPEN_BALANCE,
               NVL(SUM(B.TOTAL),0) TOTAL, NVL(SUM(B.GROSS),0) GROSS,
               NVL(SUM(P.RECVD),0) RECVD, NVL(SUM(P.ADVANCE),0) ADVANCE
          FROM BILL_T B, PAYMENT_T P, REP_PERIOD_INFO_T R
         WHERE B.REP_PERIOD_ID     = p_period_id
           AND P.REP_PERIOD_ID(+)  = p_period_id
           AND R.REP_PERIOD_ID(+)  = v_prev_period_id
           AND P.ACCOUNT_ID(+)     = B.ACCOUNT_ID
           AND R.ACCOUNT_ID(+)     = B.ACCOUNT_ID
        GROUP BY B.ACCOUNT_ID
    );
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
-- ����������� �������, ���� ������� ������
-- � ������ ������ �������� ������ + ������������ ����� + ��� ������� �������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Refresh_balance
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Refresh_balance';
    c_min_date       CONSTANT DATE := TO_DATE('01.01.2000','dd.mm.yyyy');
    v_count          INTEGER;
    v_last_period_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ID ���������� ��������� �������
    v_last_period_id := PK04_PERIOD.Last_period_id; 
    -- �������� �������� ������� �� ���������� ��������� ������� + ���������� � �������
    MERGE INTO ACCOUNT_T A
    USING (
      SELECT R.ACCOUNT_ID, (R.CLOSE_BALANCE + BALANCE) BALANCE, BALANCE_DATE
        FROM REP_PERIOD_INFO_T R,
             (SELECT ACCOUNT_ID, SUM(RECVD-BILL_TOTAL) BALANCE,
                     CASE
                         WHEN MAX(BILL_DATE) > MAX(PAYMENT_DATE) THEN MAX(BILL_DATE)
                         ELSE MAX(PAYMENT_DATE)
                     END BALANCE_DATE 
              FROM (
                  -- �������� ������ ������������� �� ������������ ������
                  SELECT B.ACCOUNT_ID, 
                         (B.GROSS+B.TAX) BILL_TOTAL, BILL_DATE, 
                         0 RECVD, TO_DATE('01.01.2000','dd.mm.yyyy') PAYMENT_DATE 
                    FROM BILL_T B
                   WHERE B.REP_PERIOD_ID > v_last_period_id
                     AND B.TOTAL > 0 -- �������� ������ � ������� �������
                  UNION ALL
                  -- �������� ����� ����������� �� ������ ��������
                  SELECT P.ACCOUNT_ID, 
                         0 BILL_TOTAL, TO_DATE('01.01.2000','dd.mm.yyyy') BILL_DATE,
                         P.RECVD, P.PAYMENT_DATE  
                    FROM PAYMENT_T P
                   WHERE P.REP_PERIOD_ID > v_last_period_id
              )
              GROUP BY ACCOUNT_ID) T
      WHERE R.ACCOUNT_ID = T.ACCOUNT_ID
        AND R.REP_PERIOD_ID = v_last_period_id
    ) D
    ON (A.ACCOUNT_ID = D.ACCOUNT_ID)
    WHEN MATCHED THEN UPDATE SET A.BALANCE_DATE = D.BALANCE_DATE, A.BALANCE = D.BALANCE;
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
-- ���������� ����, ��������� ������� � ����������
-- �� ��� ��� ���� �������� ����� ������� - ��� ���������
--
PROCEDURE Correct_tax_incl( p_period IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Correct_tax_incl';
    v_count      INTEGER;
BEGIN
    -- ��� ���.��� ��� ���������� ������ ������� ������
    UPDATE ITEM_T I
       SET I.TAX_INCL = 'Y'
     WHERE I.TAX_INCL = 'N'
       AND EXISTS (
           SELECT * FROM ACCOUNT_T A
            WHERE A.ACCOUNT_TYPE = 'P'
         )  
       AND I.REP_PERIOD_ID = p_period;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('���������� '||v_count||' item ��� ���.���', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��� ��.��� ���������� �� ������� ������
    UPDATE ITEM_T I
       SET I.TAX_INCL = 'N'
     WHERE I.TAX_INCL = 'Y'
       AND EXISTS (
           SELECT * FROM ACCOUNT_T A
            WHERE A.ACCOUNT_TYPE = 'J'
         )  
       AND I.REP_PERIOD_ID = p_period;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('���������� '||v_count||' item ��� ��.���', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ �������
--
PROCEDURE Close_period
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_period';
    v_period_id  INTEGER;
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ID ������������ �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_period_id := Period_for_close;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Start, period_id = '||v_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ����, ��������� ������� � ����������
    -- �� ��� ��� ���� �������� ����� ������� - ��� ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Correct_tax_incl( v_period_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������� ����� � �������������, ��������� ���� � ��������� READY
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Prep_items( v_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ����� ������-������ �� ������ (last_period) �� ������� ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Invoicing( v_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� - �������� � ���� ��������� ITEMs, ��������� ���� � ��������� READY
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Billing( v_period_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- �������� ��� ��� �������� �� �������� �� ����������� ������ �� �������������� �����
    -- �������� ������ �/� �� �������� ������ �/� �� ��������
    -- �������� ��� '�' �������������� FIFO, '�'-������ ����� ��� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Payment_processing( v_period_id );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� �������, ���� ������� ������ (����� READY - ������ � ������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Refresh_balance;
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
    Pk01_Syslog.Write_msg('������� '||v_count||' ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ����� �������� ������� (��������� � ��� ��� ���������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE BILL_T 
       SET BILL_STATUS = Pk00_Const.c_BILL_STATE_CLOSED
     WHERE REP_PERIOD_ID = v_period_id
       AND BILL_STATUS IN ( Pk00_Const.c_BILL_STATE_READY, PK00_CONST.c_BILL_STATE_EMPTY);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('������� '||v_count||' ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ �� �������� �� ������ ���������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Calc_advance( v_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� ���� ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Period_info( v_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� �������, ���� ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Refresh_balance;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--=======================================================================================
-- �������� �������� �� ������� �����������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� � �������� �������
--   - ������������� - ���-�� ��������� �������
--   - ��� ������ ���������� ����������
PROCEDURE Period_list( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Period_list';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
        SELECT P.POSITION, P.PERIOD_ID, P.PERIOD_FROM, P.PERIOD_TO, P.CLOSE_REP_PERIOD
          FROM PERIOD_T P
         WHERE POSITION IS NOT NULL
        ORDER BY PERIOD_ID;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ��������� ��� ��������� ������� � ������� ����������� 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Msg_list(
              p_recordset OUT t_refc,
              p_function   IN VARCHAR2,                   -- ��� �������
              p_date_from  IN DATE DEFAULT (SYSDATE-30)   -- ����� ������ ������� (���������������)
           )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Msg_list';
    v_ssid    INTEGER; 
    v_id_from INTEGER; 
    v_id_to   INTEGER;
    v_retcode INTEGER;
BEGIN
    -- ���������� ������
    SELECT MAX(SSID), MIN(L01_ID), MAX(L01_ID) 
      INTO v_ssid, v_id_from, v_id_to
      FROM L01_MESSAGES
     WHERE MSG_SRC LIKE c_PkgName||'.'||p_function||'%'
       AND p_date_from < MSG_DATE 
       AND (MESSAGE LIKE 'Start%' OR MESSAGE LIKE 'Stop%');  
    -- �����������
    IF v_id_from IS NULL THEN
        OPEN p_recordset FOR
            SELECT 0, 'E', SYSDATE, '�� ������� ������: "Start" ', c_PkgName||'.'||p_function, TO_CHAR(NULL) 
              FROM DUAL;
    ELSIF v_id_from = v_id_to THEN -- �� ������� ������ ���� (������� ��� ������������)
        -- ���������� ������ �� ������ ������, ������� �� ������� ������ ������� 
        OPEN p_recordset FOR
            SELECT L01_ID, MSG_LEVEL, MSG_DATE, MESSAGE, MSG_SRC, APP_USER 
              FROM L01_MESSAGES L
             WHERE SSID = v_ssid
               AND L01_ID >= v_id_from
             ORDER BY L01_ID;
    ELSE
        -- ���������� ������ �� ������ ��������� �������, ����� �������� �-�� 
        OPEN p_recordset FOR
            SELECT L01_ID, MSG_LEVEL, MSG_DATE, MESSAGE, MSG_SRC, APP_USER 
              FROM L01_MESSAGES
             WHERE SSID = v_ssid
               AND L01_ID BETWEEN v_id_from AND v_id_to
             ORDER BY L01_ID;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������� �������� ������������� 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Billing_history( 
               p_recordset OUT t_refc, 
               p_months    IN INTEGER    -- ���-�� ������� �����
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing_history';
    v_retcode    INTEGER;
    v_date_from  DATE;
BEGIN
    v_date_from := ADD_MONTHS(TRUNC(SYSDATE,'mm'), -p_months);
    -- ���������� ������
    OPEN p_recordset FOR 
        SELECT B.REP_PERIOD_ID,       -- �������� ������
               A.ACCOUNT_TYPE,        -- ��� �������� �����
               B.BILL_STATUS,         -- ������ �����
               SUM(B.TOTAL) TOTAL,    -- ����� ����� ���������� �� ������ � ��������
               SUM(B.GROSS) GROSS,    -- ����� ����� ���������� �� ������ ��� �������
               SUM(B.TAX) TAX,        -- ����� ����� �������
               SUM(B.RECVD) RECVD,    -- ����� ����� ������ �� ������
               COUNT(*) NUM           -- ���-�� ������������ ������
          FROM BILL_T B, ACCOUNT_T A, PERIOD_T P
         WHERE A.ACCOUNT_ID = B.ACCOUNT_ID 
           AND B.REP_PERIOD_ID = P.PERIOD_ID
           AND P.PERIOD_FROM >= v_date_from
           AND P.POSITION NOT IN ('OPEN','NEXT')
        GROUP BY B.REP_PERIOD_ID, A.ACCOUNT_TYPE, B.BILL_STATUS
         ORDER BY A.ACCOUNT_TYPE, B.REP_PERIOD_ID, B.BILL_STATUS;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� �������� �� ������������ ������ 
-- �������� ����� ��������� ������ ��� ������ ������������� � �������� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_period_info( 
               p_recordset OUT t_refc, 
               p_months    IN INTEGER    -- ���-�� ������� �����
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing_history';
    v_retcode    INTEGER;
    v_date_from  DATE;
    v_period_id  INTEGER;
BEGIN
    v_date_from := ADD_MONTHS(TRUNC(SYSDATE,'mm'), -p_months);
    v_period_id := PK04_PERIOD.Period_id(v_date_from);
    -- ���������� ������
    OPEN p_recordset FOR 
        SELECT B.REP_PERIOD_ID, B.ACCOUNT_ID, A.TOTAL PERIOD_INFO_TOTAL, B.TOTAL BILLS_TOTAL  
          FROM (
            SELECT REP_PERIOD_ID, ACCOUNT_ID, SUM(TOTAL) TOTAL 
              FROM REP_PERIOD_INFO_T
             WHERE REP_PERIOD_ID > v_period_id
             GROUP BY REP_PERIOD_ID, ACCOUNT_ID ) A,
          ( SELECT REP_PERIOD_ID, ACCOUNT_ID, SUM(TOTAL) TOTAL 
              FROM BILL_T
             WHERE REP_PERIOD_ID > v_period_id
            GROUP BY REP_PERIOD_ID, ACCOUNT_ID ) B
        WHERE A.REP_PERIOD_ID(+) = B.REP_PERIOD_ID
          AND A.ACCOUNT_ID(+) = B.ACCOUNT_ID
          AND (A.TOTAL IS NULL OR B.TOTAL IS NULL OR A.TOTAL != B.TOTAL);
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������ ������, ���������� � ������ ������ 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Err_bill_list( 
               p_recordset OUT t_refc, 
               p_period_id  IN INTEGER    -- ID ��������� �������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Err_bill_list';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
        SELECT A.ACCOUNT_NO, A.ACCOUNT_TYPE, B.REP_PERIOD_ID, B.BILL_NO, B.BILL_ID, B.BILL_STATUS, B.CALC_DATE  
        FROM BILL_T B, ACCOUNT_T A 
        WHERE B.BILL_STATUS = PK00_CONST.c_BILL_STATE_ERROR
          AND A.ACCOUNT_ID = B.ACCOUNT_ID
          AND B.REP_PERIOD_ID = p_period_id;
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ========================================================================= --
--                               � � � � � � �
-- ========================================================================= --
-- ������ �������� �������� 
-- ��� ��������������� ��������, ����������� ������ � ������ �������
-- ========================================================================= --
PROCEDURE Recalc_period_info ( p_period_id IN INTEGER )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Recalc_period_info';
    v_count     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- �������� �������
    DELETE FROM REP_PERIOD_INFO_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Deleted '||v_count||' rows from REP_PERIOD_INFO_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- �������� ������������� ������
    INSERT INTO REP_PERIOD_INFO_T (
        REP_PERIOD_ID, ACCOUNT_ID, OPEN_BALANCE, CLOSE_BALANCE,
        TOTAL, GROSS, RECVD, ADVANCE, LAST_MODIFIED
    )
    SELECT REP_PERIOD_ID, ACCOUNT_ID, 
           ((RECVD_ALL-TOTAL_ALL) - (RECVD-TOTAL)) OPEN_BALANCE,
           (RECVD_ALL-TOTAL_ALL) CLOSE_BALANCE,
           TOTAL, GROSS, RECVD, ADVANCE, SYSDATE LAST_MODIFIED
    FROM (
        SELECT ACCOUNT_ID, REP_PERIOD_ID, TOTAL, GROSS, RECVD, ADVANCE,
               SUM(TOTAL) OVER (PARTITION BY ACCOUNT_ID ORDER BY REP_PERIOD_ID) TOTAL_ALL,
               SUM(RECVD) OVER (PARTITION BY ACCOUNT_ID ORDER BY REP_PERIOD_ID) RECVD_ALL
          FROM (
            SELECT ACCOUNT_ID, REP_PERIOD_ID, 
                   SUM(TOTAL) TOTAL, SUM(GROSS) GROSS, 
                   SUM(RECVD) RECVD, SUM(ADVANCE) ADVANCE
            FROM (
                SELECT B.ACCOUNT_ID, B.REP_PERIOD_ID, B.TOTAL, B.GROSS, 0 RECVD, 0 ADVANCE
                  FROM BILL_T B
                UNION ALL
                SELECT P.ACCOUNT_ID, P.REP_PERIOD_ID, 0 TOTAL, 0 GROSS, P.RECVD, P.ADVANCE
                  FROM PAYMENT_T P
            )GROUP BY ACCOUNT_ID, REP_PERIOD_ID
        )
    )
    WHERE REP_PERIOD_ID <= p_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Inserted '||v_count||' rows from REP_PERIOD_INFO_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- �������� ���������� �� �������
    Gather_Table_Stat(l_Tab_Name => 'REP_PERIOD_INFO_T');
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- ------------------------------------------------------------------------- --
-- ������ �������� �������
-- ��� ��������������� ��������, ����������� ������ � ������ �������
-- ------------------------------------------------------------------------- --
PROCEDURE Recalc_all_balances
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Recalc_all_balances';
    v_count     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    MERGE INTO ACCOUNT_T A
    USING   
       (SELECT ACCOUNT_ID, SUM(RECVD-BILL_TOTAL) BALANCE,
               CASE
                   WHEN MAX(BILL_DATE) > MAX(PAYMENT_DATE) THEN MAX(BILL_DATE)
                   ELSE MAX(PAYMENT_DATE)
               END BALANCE_DATE 
        FROM (
            -- �������� ������ ������������� �� ������������ ������
            SELECT B.ACCOUNT_ID, 
                   (B.GROSS+B.TAX) BILL_TOTAL, BILL_DATE, 
                   0 RECVD, TO_DATE('01.01.2000','dd.mm.yyyy') PAYMENT_DATE 
              FROM BILL_T B
             WHERE B.TOTAL > 0 -- �������� ������ � ������� �������
            UNION ALL
            -- �������� ����� ����������� �� ������ ��������
            SELECT P.ACCOUNT_ID, 
                   0 BILL_TOTAL, TO_DATE('01.01.2000','dd.mm.yyyy') BILL_DATE,
                   P.RECVD, P.PAYMENT_DATE  
              FROM PAYMENT_T P
        )
        GROUP BY ACCOUNT_ID) T
    ON (A.ACCOUNT_ID = T.ACCOUNT_ID)
    WHEN MATCHED THEN UPDATE SET A.BALANCE_DATE = T.BALANCE_DATE, A.BALANCE = T.BALANCE;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Updated '||v_count||' rows in ACCOUNT_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    
    -- �������� ���������� �� �������
    Gather_Table_Stat(l_Tab_Name => 'ACCOUNT_T');
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

END PK100_BILLING;
/
