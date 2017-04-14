CREATE OR REPLACE PACKAGE PK99_ACCOUNT_BILLING
IS
    --
    -- ����� ��� ��������� �������� ����������� ������
    -- ��� ���������� �������� �����
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK99_ACCOUNT_BILLING';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- ==============================================================================
    -- ��������������� ���������� �����, ������ � ������ ������������ �������
    -- ==============================================================================
    -- ������������� ���� ������������ �������
    --
    PROCEDURE Rollback_Bill(p_bill_id IN INTEGER);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� ��� ���������� �������� ����� (��� ������� ���������������� �����)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Account_Billing(p_account_id IN INTEGER);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- �������� ��� ��� �������� �� �������� ���������� �/�
    -- �������� ������ �/� �� �������� ������ �/� �� ��������
    -- �������� ��� '�' �������������� FIFO, '�'-������ ����� ��� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Payment_FIFO( p_period_id IN INTEGER, p_account_id IN INTEGER );
    
END PK99_ACCOUNT_BILLING;
/
CREATE OR REPLACE PACKAGE BODY PK99_ACCOUNT_BILLING
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

-- ===================================================================================
-- ��������������� ���������� �����, ������ � ������ ������������ �������
-- ===================================================================================
-- ������������� ���� ������������ �������
--
PROCEDURE Rollback_Bill(p_bill_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_Bill';
    v_period_id  INTEGER;
    v_account_id INTEGER;
    v_count      INTEGER;
    v_balance    NUMBER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start bill_id = '||p_bill_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������, ��� ���� ����������� ������������ �������
    --
    SELECT PR.PERIOD_ID, B.ACCOUNT_ID
      INTO v_period_id, v_account_id
      FROM PERIOD_T PR, BILL_T B
     WHERE PR.CLOSE_FIN_PERIOD IS NULL
       AND PR.POSITION = PK00_CONST.c_PERIOD_BILL
       AND PR.PERIOD_ID= B.REP_PERIOD_ID
       AND B.BILL_ID = p_bill_id;
    
    Pk01_Syslog.Write_msg('bill_period_id = '||v_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������� �� �����
    --    
    -- ���������� �������� �� ������� ����������� �� ����� ������������ �������
    MERGE INTO PAYMENT_T P
    USING 
    (
        SELECT TRANSFER_ID, PAYMENT_ID, PAY_PERIOD_ID, BILL_ID, REP_PERIOD_ID, ITEM_ID, 
               TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE, 
               TRANSFER_DATE, PREV_TRANSFER_ID, NOTES
          FROM PAY_TRANSFER_T PT
         WHERE BILL_ID = p_bill_id
           AND REP_PERIOD_ID = v_period_id
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
    Pk01_Syslog.Write_msg('delete from pay_transfer_t '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
   
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� ������ ������ �� ������� ����� � �������� � �������� ���������
    UPDATE ITEM_T I
       SET I.INV_ITEM_ID = NULL,
           I.REP_GROSS   = NULL,
           I.REP_TAX     = NULL,
           I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
     WHERE I.REP_PERIOD_ID = v_period_id
       AND I.BILL_ID = p_bill_id
       AND I.ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, Pk00_Const.c_ITEM_TYPE_ADJUST);
       
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ������ ������
    DELETE FROM INVOICE_ITEM_T II
     WHERE II.REP_PERIOD_ID = v_period_id
       AND II.BILL_ID = p_bill_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ���� 
    UPDATE BILL_T B
       SET B.TOTAL = 0,
           B.GROSS = 0,
           B.TAX   = 0,
           B.RECVD = 0,
           B.DUE   = 0,
           B.DUE_DATE = NULL,
           B.PAID_TO  = NULL,
           B.CALC_DATE= NULL,
           B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN
     WHERE B.REP_PERIOD_ID = v_period_id
       AND B.BILL_ID = p_bill_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ������ �������� �����
    v_balance := PK05_ACCOUNT.Refresh_balance(v_account_id);
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ==================================================================================== --
-- �������� ������� ��� ���������� �������� ����� (��� ������� ���������������� �����)
-- ==================================================================================== --
PROCEDURE Account_Billing(p_account_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Account_Billing';
    v_period_id   INTEGER;
    v_count       INTEGER;
    v_inv_item_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start account_id = '||p_account_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ID ������������ �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_period_id := Period_for_close;
    Pk01_Syslog.Write_msg('bill_period_id = '||v_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������� ���������� � ������������� �������� ������ � ��������� READY,
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
    WHERE I.REP_PERIOD_ID = v_period_id
      AND I.ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, Pk00_Const.c_ITEM_TYPE_ADJUST)
      AND EXISTS (
          SELECT * FROM BILL_T B
           WHERE B.BILL_ID = I.BILL_ID
             AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND B.ACCOUNT_ID = p_account_id
             AND B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN
      )
    ;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Move status for '||v_count||' - items to '
                          || Pk00_Const.c_ITEM_STATE_RE�DY, 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
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
    WHERE B.REP_PERIOD_ID = v_period_id
      AND B.ACCOUNT_ID = p_account_id
      AND B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||' - bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������� ��� ���� �������� ������������� ������ ������������ �������
    --
    FOR r_bill IN (
        SELECT B.BILL_ID
          FROM BILL_T B
         WHERE B.REP_PERIOD_ID= v_period_id 
           AND B.ACCOUNT_ID   = p_account_id
           AND B.BILL_STATUS  = Pk00_Const.c_BILL_STATE_READY
      )
    LOOP
        v_inv_item_id := Pk09_Invoice.Calc_invoice(
                         p_bill_id       => r_bill.bill_id,
                         p_rep_period_id => v_period_id
                      );
        v_count := v_count + 1;         -- ������ ������ �������
    END LOOP;    
    --
    Pk01_Syslog.Write_msg('Make invoice_item: '||v_count||' rows created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ���� �����, ��� �������� ������ �/� ������������ �������
    -- � ������������� ������������ ���� ������ �����, �� ��������� ���� ����������� �����
    UPDATE BILL_T B 
       SET (B.TOTAL, B.GROSS, B.TAX, B.DUE) = (
          SELECT NVL(SUM(II.TOTAL),0), NVL(SUM(II.GROSS),0), 
                 NVL(SUM(II.TAX),0),  -NVL(SUM(II.TOTAL),0)
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
    WHERE B.REP_PERIOD_ID = v_period_id
      AND B.ACCOUNT_ID    = p_account_id
      AND B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_READY;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||' - bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- ��������� ���� �����, ��� ������ ������ �/� ������������ �������
    UPDATE BILL_T B
       SET B.PAID_TO   = NULL, 
           B.DUE_DATE  = SYSDATE, 
           B.CALC_DATE = SYSDATE
     WHERE B.REP_PERIOD_ID = v_period_id
       AND B.ACCOUNT_ID    = p_account_id
       AND B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_EMPTY;
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
-- �������� ��� ��� �������� �� �������� ���������� �/�
-- �������� ������ �/� �� �������� ������ �/� �� ��������
-- �������� ��� '�' �������������� FIFO, '�'-������ ����� ��� ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Payment_FIFO( p_period_id IN INTEGER, p_account_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Payment_FIFO';
    v_ok            INTEGER;    
    v_err           INTEGER;
    v_transfer_id   INTEGER;
    v_value         NUMBER := 0; -- ����� ������� ����� ���������, NULL - ������� �����               
    v_open_balance  NUMBER := 0; -- ����� �� ������� �� ���������� ��������
    v_close_balance NUMBER := 0; -- ����� �� ������� ����� ���������� ��������
    v_bill_due      NUMBER := 0; -- ���������� ���� �� ����� ����� ��������
    
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id <= '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_ok := 0;    
    v_err:= 0;
    -- �������� ������� �������� �������� �� �/�
    FOR r_pay IN (
        SELECT B.ACCOUNT_ID, 
               P.PAYMENT_ID, P.REP_PERIOD_ID PAY_PERIOD_ID, 
               B.BILL_ID, B.REP_PERIOD_ID, 
               B.DUE, P.BALANCE,
               B.BILL_DATE, P.PAYMENT_DATE
          FROM PAYMENT_T P, BILL_T B
         WHERE B.ACCOUNT_ID = p_account_id
           AND P.ACCOUNT_ID = B.ACCOUNT_ID
           AND P.BALANCE > 0
           AND B.DUE < 0
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_CLOSED, Pk00_Const.c_BILL_STATE_READY)
           AND P.REP_PERIOD_ID <= p_period_id  
        ORDER BY B.BILL_DATE, P.PAYMENT_DATE  
      )
    LOOP
        SAVEPOINT X;  -- ����� ���������� ������ ��� �������� �����
        BEGIN
            -- �������� ������� �������� �� �������� ������ 
            -- (��� ������� ������� FIFO, ��� ������, ������ ������ ����� ���)
            -- ��� �������� �������� ���� ������� �� ������� 
            v_value := NULL;
            -- �������� ������ �� ������������ ����� � ������� �� �����������
            v_transfer_id := Pk10_Payment.Transfer_to_bill(
                     p_payment_id    => r_pay.payment_id,   -- ID ������� - ��������� �������
                     p_pay_period_id => r_pay.pay_period_id,-- ID ��������� ������� ���� ����������� ������
                     p_bill_id       => r_pay.bill_id,      -- ID ������������� �����
                     p_rep_period_id => r_pay.rep_period_id,-- ID ��������� ������� �����
                     p_notes         => NULL,           -- ���������� � ��������
                     p_value         => v_value,        -- ����� ������� ����� ���������, NULL - ������� �����
                     p_open_balance  => v_open_balance, -- ����� �� ������� �� ���������� ��������
                     p_close_balance => v_close_balance,-- ����� �� ������� ����� ���������� ��������
                     p_bill_due      => v_bill_due      -- ���������� ���� �� ����� ����� ��������
                 );
            v_ok := v_ok + 1;         -- ������ ������ �������
        EXCEPTION
            WHEN OTHERS THEN
              -- ����� ��������� ��� �������� �����
              ROLLBACK TO X;
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
            Pk01_Syslog.Write_msg('Processed: '||v_ok||'-ok, '||v_err||'-err advances', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        --
    END LOOP;
    --
    Pk01_Syslog.Write_msg('Processed: ok='||v_ok||', err='||v_err||' -payments', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;



END PK99_ACCOUNT_BILLING;
/
