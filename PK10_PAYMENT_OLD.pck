CREATE OR REPLACE PACKAGE PK10_PAYMENT_OLD
IS
    --
    -- ����� ��� ������ � �������� "������", �������:
    -- payment_t, pay_transfer_t
    -- --------------------------------------------------------------------------- --
    -- ����� �������� ������ :
    -- �� ������ ����� ��-���� ������� �� �� ���������, ����� ������ �� ������� ��������
    -- 1) ��� ��������� �������, ������������ ACCOUNT_T.ACCOUNT_ID 
    --    ������������� �������� �� �����, ������������ �-�� ��������� "Find_..."
    -- 2) ������ �������������� �� ��������� ACCOUNT_ID � ��� ����� ����� ������ 
    --    � ������ �/� ACCOUNT_T.BALANCE: �-�� "Add_payment(...)"
    -- 3) ������ ��� ���� ������� ����� ��������� ������������ ���� (BILL_T.STATUS = 'CLOSED')
    --    ��������� ��� ��������: �-�� Transfer_to_bill()
    -- 4) ������ ���������� �� ������������ ������ (BILL_T.STATUS = 'CLOSED') �������
    --    FIFO: �-�� "Transfer_to_account_fifo(...)"
    -- 5) ���� ����� �������� �������, � ������� �������� ������, �� ��� ��������
    --    �������� - ��� ����������� � ���� PAYMENT_T.ADVANCE 
    --    � ��������� ���� PAYMENT_T.ADVANCE_DATE
    --
    --    ������� :
    -- PAYMENT_T - �������� ������ � ��������� � �������� ����� �� ���� ����������� �������
    -- PAY_TRANSFER_T - �������� �������� �� �������� ������� PAYMENT_T �� 
    --                  ��������� ������� ������������ ������ (BILL_T.STATUS = 'CLOSED')
    -- ITEM_T - �������� ������� �������� ITEM(P) ������������ ������, 
    --          �� ������ ���� - ���� ��������� �������
    -- --------------------------------------------------------------------------- --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_PAYMENT';
    -- ==============================================================================
   
    type t_refc is ref cursor;
    
    -- ------------------------------------------------------------------------ --
    -- �������� ������ �� �/� ������� (����� ����� ����������� � ������� �/�)
    --   - ������������� - ID ���������� ��������� (PAYMENT.PAYMENT_ID) � ��������, 
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Add_payment (
                  p_account_id      IN INTEGER,   -- ID �������� ����� �������
                  p_rep_period_id   IN INTEGER,   -- ID ��������� ������� ���� ����������� ������
                  p_payment_dat�    IN DATE,      -- ���� �������
                  p_payment_type    IN VARCHAR2,  -- ��� �������
                  p_recvd           IN NUMBER,    -- ����� �������
                  p_paysystem_id    IN INTEGER,   -- ID ��������� �������
                  p_doc_id          IN VARCHAR2,  -- ID ��������� � ��������� �������
                  p_status          IN VARCHAR2,  -- ������ �������
                  p_manager    		  IN VARCHAR2,  -- �.�.�. ��������� ��������������� ������ �� �/�
                  p_notes           IN VARCHAR2,  -- ���������� � �������  
                  p_prev_payment_id IN INTEGER DEFAULT NULL, 
                  p_prev_period_id  IN INTEGER DEFAULT NULL
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- �������������� ������ � �/� ������� (����� ����� ����������� � ������� �/�)
    --   - ������������� - ID ���������� ��������� (PAYMENT.PAYMENT_ID) � ��������, 
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Adjust_payment (
                  p_src_payment_id IN INTEGER,    -- ID ��������������� �������
                  p_src_period_id  IN INTEGER,    -- ID ��������� �������, ����� ��� ��������������� ������
                  p_dst_period_id  IN INTEGER,    -- ID ��������� �������, ��������������� �������
                  p_value          IN NUMBER,     -- ���������� ����� �������������
                  p_manager        IN VARCHAR2,   -- �������� ����������� ��������
                  p_notes          IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
               ) RETURN NUMBER;

    -- ------------------------------------------------------------------------ --
    -- ������� ����� (��� ���� �����) ������� �� ITEM(P) ������ ��������� 
    -- ������������� ������������� �����,
    -- ���� ������� ��� - ��� ���������
    -- ��� �������� ������������� �� ��������, ����������:
    --   > 0  - PAY_TRANSFER.TRANSFER_ID ��������� �������� �������� 
    --   NULL - ������� ���������� �������: 
    --        * �� ����� ���������� ����
    --        * ��� ������� �� �������: p_open_balance = 0
    --        * p_total < 0 - ��� �� ������ ����
    --   - ��� ������ ���������� ����������
    FUNCTION Transfer_to_bill(
                   p_payment_id    IN INTEGER,    -- ID ������� - ��������� �������
                   p_pay_period_id IN INTEGER,    -- ID ��������� ������� ���� ����������� ������
                   p_bill_id       IN INTEGER,    -- ID ������������� �����
                   p_rep_period_id IN INTEGER,    -- ID ��������� ������� �����               
                   p_notes         IN VARCHAR2,   -- ���������� � ��������
                   p_value         IN OUT NUMBER, -- ����� ������� ����� ���������, NULL - ������� �����               
                   p_open_balance  OUT NUMBER,    -- ����� �� ������� �� ���������� ��������
                   p_close_balance OUT NUMBER,    -- ����� �� ������� ����� ���������� ��������
                   p_bill_due      OUT NUMBER     -- ���������� ���� �� ����� ����� ��������
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- �������������� �������� ������� ������� FIFO �� ������� (ITEM_T(P)) 
    -- ������������ ����� ������������� ������ (item-� �� ���������)
    -- ��� �������� ������������� �� ��������, ����������:
    --   - ������� ������������� ������� �� ������� 
    --   - ��� ������ ���������� ����������
    FUNCTION Transfer_to_account_fifo(
                   p_payment_id    IN INTEGER,  -- bill - ��������
                   p_pay_period_id IN INTEGER,  -- ID ��������� ������� ���� ����������� ������
                   p_account_id    IN INTEGER   -- ������� ����, ����� �������� ����������
               ) RETURN NUMBER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- �������� �� �������������� ����� ��� ��� �������� �� �������� �������� 
    -- �� ���������� ������� ������������.
    -- �������� ������ �/� �� �������� ������ �/� �� ��������
    -- �������� ��� '�' �������������� FIFO, '�'-������ ����� ��� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Payment_processing_fifo( p_from_period_id IN INTEGER );

    -- ------------------------------------------------------------------------ --
    -- ������������ ������ � �/� ������� (����� ����� ����������� � ������� �/�)
    --   - ������������� - ID ���������� ��������� (PAYMENT.PAYMENT_ID) � ��������, 
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Revers_payment (
                   p_src_payment_id IN INTEGER,   -- ID ������������� �������                        
                   p_src_period_id  IN INTEGER,   -- ID ��������� �������, ����� ��� ��������������� ������
                   p_dst_period_id  IN INTEGER,   -- ID ��������� �������, ������������� �������
                   p_manager        IN VARCHAR2,  -- �������� ����������� ��������
                   p_notes          IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
               ) RETURN NUMBER;

    -- ------------------------------------------------------------------------ --
    -- ��������� ������ � ������ �������� ����� �� ������
    --   - ������������� - ID ���������� ��������� (PAYMENT.PAYMENT_ID) � ��������, 
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Move_payment (
                   p_src_payment_id IN INTEGER,  -- ID ������� ���������
                   p_src_period_id  IN INTEGER,  -- ID ��������� ������� ���������
                   p_dst_account_id IN INTEGER,  -- ID ������� ���������
                   p_dst_period_id  IN INTEGER,  -- ID ��������� ������� ���������
                   p_manager        IN VARCHAR2, -- �������� ����������� ��������
                   p_notes          IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
               ) RETURN NUMBER;

    -- ------------------------------------------------------------------------ --
    -- �������� ��������� �������� �������� �������, 
    -- ��� ������� ��� ������ � ������� �������� ������ ��� �� ������
    -- (���� ������ ������, �� ��� ����������� ����� ��� ����������)
    --   - ������� ������������� ������� �� ������� 
    --   - ��� ������ ���������� ����������
    FUNCTION Rollback_transfer(
                   p_transfer_id   IN INTEGER,   -- ID �������
                   p_pay_period_id IN INTEGER    -- ID ������� �������
               ) RETURN NUMBER;

    -- ------------------------------------------------------------------------ --
    -- �������� ������, 
    -- ��� ������� ��� ������ � ������� �������� ������ ��� �� ������
    -- (���� ������ ������, �� ��� ����������� ����� ��� ����������)
    --   - ������� ������������� ������� �� ������� 
    --   - ��� ������ ���������� ����������
    PROCEDURE Rollback_payment(
                   p_payment_id    IN INTEGER,   -- ������
                   p_pay_period_id IN INTEGER,   -- ID ������� �������
                   p_app_user      IN VARCHAR2   -- ������������ ����������
               );
               
    -- ------------------------------------------------------------------------ --
    -- ��������� ����� ������� ��� �����
    --   - ������ ������
    --   - ��� ������ ���������� ����������
    FUNCTION Fix_advance(
                   p_payment_id    IN INTEGER,   -- ID �������
                   p_pay_period_id IN INTEGER    -- ID ������� �������
               ) RETURN NUMBER;

    -- ------------------------------------------------------------------------ --
    -- ����������� ��������� ������������ �������� ��� ���������� �������
    -- ��������� ����� ��������� ����� �������� ������������ ������� � �� �������� �����������
    -- ��������: ������������� ������� �������� ���������� �������� ������������� ���������!!!
    PROCEDURE Refresh_advance(
                   p_pay_period_id IN INTEGER    -- ID ������� �������
               );

    -- ------------------------------------------------------------------------ --
    -- ����� �������� ����� �� ������ ��������, ����������:
    --   > 0 - ID �������� ����� � ��������
    --   NULL - �������� �� ������� 
    --   - ��� ������ ���������� ����������
    FUNCTION Find_account_by_phone (
                   p_phone         IN VARCHAR2,  -- ����� ��������
                   p_date          IN DATE       -- ���� �� ������� ���� ������������
               ) RETURN INTEGER;
               
    -- ����� ID �������� ����� �� ������ ������������� �����
    --   > 0 - ������������� - ID �������� ����� � �������� 
    --   NULL - ������� �� �������
    --   - ��� ������ ���������� ����������
    FUNCTION Find_account_by_billno (
                   p_bill_no       IN VARCHAR2   -- ����� ������������� �����
               ) RETURN INTEGER;
               
    -- ����� ID ����� �� ������ �����, ����������:
    --   > 0 - ������������� - ID ����� � �������� 
    --   NULL - ������� �� �������
    --   - ��� ������ ���������� ����������
    FUNCTION Find_id_by_billno (
                   p_bill_no       IN VARCHAR2   -- ����� �����
               ) RETURN INTEGER;

    -- ����� ID �������� ����� �� ������ �������� �����, ����������:
    --   > 0 - ID �������� ����� � �������� 
    --   NULL - ������� �� �������
    --   - ��� ������ ���������� ����������
    FUNCTION Find_id_by_accountno (
                   p_account_no    IN VARCHAR2   -- ����� ��������
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- ������ �������� �� �������� �����
    --   - ������������� - ���-�� ��������� �������
    --   - ��� ������ ���������� ����������
    FUNCTION Account_payment_list (
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,   -- ID �������� �����
                   p_date_from  IN DATE,
                   p_date_to    IN DATE 
               ) RETURN INTEGER;

-- ------------------------------------------------------------------------ --
    -- ������ �������� �� �������� �����
    --   - ������������� - ���-�� ��������� �������
    --   - ��� ������ ���������� ����������
    FUNCTION Account_payment_list (
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID �������� �����
               p_period_id  IN INTEGER
           ) RETURN INTEGER; 
-- ------------------------------------------------------------------------ --              
    -- ������ ����� ����������� ����
    --   - ������������� - ���-�� ��������� �������
    --   - ��� ������ ���������� ����������
    FUNCTION Bill_pay_list (
                   p_recordset    OUT t_refc, 
                   p_bill_id       IN INTEGER,    -- ID �������
                   p_rep_period_id IN INTEGER     -- ID ��������� ������� �����
               ) RETURN INTEGER;

-- ------------------------------------------------------------------------ --
-- ������ ����� ����������� ����� �� ������������ ������ �� ������������� �������� �����
--   - ������������� - ���-�� ��������� �������
--   - ��� ������ ���������� ����������
FUNCTION Bill_pay_list_by_account (
               p_recordset    OUT t_refc, 
               p_account_id   IN INTEGER,    -- ID �������� �����
               p_rep_period_id IN INTEGER     -- ID ��������� ������� �����
           ) RETURN INTEGER;  
            
    -- �������� �������� ������� �� ������
    --   - ������������� - ���-�� ��������� �������
    --   - ��� ������ ���������� ����������
    FUNCTION Transfer_list (
                   p_recordset    OUT t_refc, 
                   p_payment_id    IN INTEGER,   -- ID �������
                   p_pay_period_id IN INTEGER    -- ID ��������� ������� �����
               ) RETURN INTEGER;


END PK10_PAYMENT_OLD;
/
CREATE OR REPLACE PACKAGE BODY PK10_PAYMENT_OLD
IS

-- ------------------------------------------------------------------------ --
-- �������� ������ �� �/� ������� 
-- ����� ����� ����������� � ������� �/� � ����������� � �����, 
-- �������� ������� �� ������, ������������ � ������ ������� ��� ����� ������ ������� 
-- ����� ��������� �����.
--   - ������������� - ID ���������� ��������� (PAYMENT.PAYMENT_ID) � ��������, 
--   - ��� ������ ���������� ����������
--
FUNCTION Add_payment (
              p_account_id      IN INTEGER,   -- ID �������� ����� �������
              p_rep_period_id   IN INTEGER,   -- ID ��������� ������� ���� ����������� ������
              p_payment_dat�    IN DATE,      -- ���� �������
              p_payment_type    IN VARCHAR2,  -- ��� �������
              p_recvd           IN NUMBER,    -- ����� �������
              p_paysystem_id    IN INTEGER,   -- ID ��������� �������
              p_doc_id          IN VARCHAR2,  -- ID ��������� � ��������� �������
              p_status          IN VARCHAR2,  -- ������ �������
              p_manager    		  IN VARCHAR2,  -- �.�.�. ��������� ��������������� ������ �� �/�
              p_notes           IN VARCHAR2,  -- ���������� � �������  
              p_prev_payment_id IN INTEGER DEFAULT NULL,
              p_prev_period_id  IN INTEGER DEFAULT NULL
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_payment';
    v_payment_id  INTEGER;
BEGIN
    v_payment_id := PK02_POID.Next_payment_id;
    -- c�������� ���������� � �������
    INSERT INTO PAYMENT_T (
        PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE,
        PAYMENT_DATE, ACCOUNT_ID, RECVD,
        ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED,
        DATE_FROM, DATE_TO,
        PAYSYSTEM_ID, DOC_ID,
        STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED,
        CREATED_BY, NOTES, 
        PREV_PAYMENT_ID, PREV_PERIOD_ID
    )VALUES(
        v_payment_id, p_rep_period_id, p_payment_type,
        p_payment_dat�, p_account_id, p_recvd,
        p_recvd, p_payment_dat�, p_recvd, 0,
        NULL, NULL,
        p_paysystem_id, p_doc_id,
        p_status, SYSDATE, SYSDATE, SYSDATE,
        p_manager, p_notes, 
        p_prev_payment_id, p_prev_period_id
    );
    -- �������� ������ �������� ����� �� �������� �������
    UPDATE ACCOUNT_T
       SET BALANCE = BALANCE + p_recvd,
           BALANCE_DATE = SYSDATE  
     WHERE ACCOUNT_ID = p_account_id;
    --
    RETURN v_payment_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- �������������� ������ � �/� ������� (����� ����� ����������� � ������� �/�)
--   - ������������� - ID ���������� ��������� (PAYMENT.PAYMENT_ID) � ��������, 
--   - ��� ������ ���������� ����������
--
FUNCTION Adjust_payment (
               p_src_payment_id IN INTEGER,   -- ID ��������������� �������
               p_src_period_id  IN INTEGER,   -- ID ��������� �������, ����� ��� ��������������� ������
               p_dst_period_id  IN INTEGER,   -- ID ��������� �������, ��������������� �������
               p_value          IN NUMBER,    -- ���������� ����� �������������
               p_manager        IN VARCHAR2,  -- �������� ����������� ��������
               p_notes          IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
           ) RETURN NUMBER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Adjust_payment';
    r_payment      PAYMENT_T%ROWTYPE;
    v_payment_id   INTEGER;
BEGIN
    -- ������ ������ ��������������� �������
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id
       AND NEXT_PAYMENT_ID IS NULL;  -- ��� ���� ������������ ������
    -- ��������� �������������� ������
    v_payment_id := Add_payment (
              p_account_id      => r_payment.Account_Id,        -- ID �������� ����� �������
              p_rep_period_id   => p_dst_period_id,             -- ID ��������� ������� ���� ����������� ������
              p_payment_dat�    => r_payment.Payment_Date,      -- ���� �������
              p_payment_type    => Pk00_Const.c_PAY_TYPE_ADJUST,-- ��� �������
              p_recvd           => p_value,                     -- ����� �������
              p_paysystem_id    => r_payment.paysystem_id,      -- ID ��������� �������
              p_doc_id          => NULL,                        -- ID ��������� � ��������� �������
              p_status          => Pk00_Const.c_PAY_STATE_OPEN, -- ������ �������
              p_manager    		  => p_manager,                   -- �.�.�. ��������� ��������������� ������ �� �/�
              p_notes           => p_notes,                     -- ���������� � �������  
              p_prev_payment_id => p_src_payment_id, 
              p_prev_period_id  => p_src_period_id
           );
    -- ����������� ��������� �� �������������� ������
    UPDATE PAYMENT_T 
       SET NEXT_PAYMENT_ID = v_payment_id,
           NEXT_PERIOD_ID  = p_dst_period_id
     WHERE PAYMENT_ID    = p_src_payment_id 
       AND REP_PERIOD_ID = p_src_period_id
       AND NEXT_PAYMENT_ID IS NULL;
    RETURN v_payment_id;  
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.Payment_id='||p_src_payment_id, c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- ������� ����� (��� ���� �����) ������� �� ITEM(P) ������ ��������� 
-- ������������� ������������� �����,
-- ���� ������� ��� - ��� ���������,
-- �������� ������� �� ������, ������������ � ������ ������� ��� ����� ������ ������� 
-- ����� ��������� �����,
-- ����������:
--   > 0  - PAY_TRANSFER.TRANSFER_ID ��������� �������� �������� 
--   NULL - ������� ���������� �������: 
--        * �� ����� ���������� ����
--        * ��� ������� �� �������: p_open_balance = 0
--        * p_total < 0 - ��� �� ������ ����
--   - ��� ������ ���������� ����������
FUNCTION Transfer_to_bill(
               p_payment_id    IN INTEGER,    -- ID ������� - ��������� �������
               p_pay_period_id IN INTEGER,    -- ID ��������� ������� ���� ����������� ������
               p_bill_id       IN INTEGER,    -- ID ������������� �����
               p_rep_period_id IN INTEGER,    -- ID ��������� ������� �����               
               p_notes         IN VARCHAR2,   -- ���������� � ��������
               p_value         IN OUT NUMBER, -- ����� ������� ����� ���������, NULL - ������� �����               
               p_open_balance  OUT NUMBER,    -- ����� �� ������� �� ���������� ��������
               p_close_balance OUT NUMBER,    -- ����� �� ������� ����� ���������� ��������
               p_bill_due      OUT NUMBER     -- ���������� ���� �� ����� ����� ��������
           ) RETURN INTEGER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Transfer_to_bill';
    v_transfer_id  INTEGER := NULL;
    v_item_id      INTEGER;
    v_prev_id      INTEGER;
    v_date_from    DATE; 
    v_date_to      DATE;
    v_bill_date    DATE;
    v_payment_date DATE;
    v_advance      NUMBER;
BEGIN
    -- ��������� ��������� ����������
    p_open_balance := 0;
    p_close_balance:= 0;
    p_bill_due := 0;
    
    -- �������� ������������� ������� �� ������� (�������� �������) �
    -- ������ ������� ������������� �� ����� ������������� ����� (�� ��������)
    SELECT P.BALANCE, P.DATE_FROM, P.DATE_TO, P.PAYMENT_DATE, P.ADVANCE, B.DUE, B.BILL_DATE
      INTO p_open_balance, v_date_from, v_date_to, v_payment_date, v_advance, p_bill_due, v_bill_date
      FROM PAYMENT_T P, BILL_T B
     WHERE P.ACCOUNT_ID = B.ACCOUNT_ID
       AND P.REP_PERIOD_ID = p_pay_period_id
       AND B.REP_PERIOD_ID = p_rep_period_id
       AND B.BILL_ID = p_bill_id
       AND P.PAYMENT_ID = p_payment_id;
    -- ��������� ���� �� ������������� �� ����� ��� ������������� �������� �� �������
    IF p_bill_due >= 0 OR p_open_balance <= 0 THEN
        p_close_balance := p_open_balance;
        p_value := 0;
        RETURN NULL; 
    END IF;
    -- ������������ ����� ������� ����� ���������
    IF p_value IS NULL THEN
        IF (p_open_balance + p_bill_due) >= 0 THEN
            -- ��������� ������� ������� �� ��������� ������������� �� �����
            p_value := -p_bill_due;
        ELSE
            -- ��������� ������� �� �������, ������������� �������� ��������
            p_value := p_open_balance;
        END IF; 
    ELSE
        IF p_value <= 0 THEN
            Pk01_Syslog.Write_msg(p_Msg => '��� �������� ������ ����� ������ 0: '||p_value,
                                  p_Src => c_PkgName||'.'||v_prcName, 
                                  p_Level => Pk01_Syslog.L_warn);
            p_close_balance := p_open_balance;
            p_value := 0;
            RETURN NULL; -- ������������� �������� ���� �� ������
        ELSIF (p_value + p_bill_due) > 0 THEN
            p_value := -p_bill_due;
        END IF;
    END IF;
    -- ������������ �������
    p_close_balance := p_open_balance - p_value;  -- ��������� � �������
    p_bill_due := p_bill_due + p_value;           -- ����� �������������

    -- �������� ������������� �� �����
    UPDATE BILL_T B 
       SET DUE   = p_bill_due, 
           RECVD = RECVD + p_value
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
     
    -- ������� ID ���������� �������� ��������, ���� ����
    IF v_date_from IS NOT NULL THEN
        --
        SELECT MAX(TRANSFER_ID) INTO v_prev_id
          FROM PAY_TRANSFER_T T
         WHERE T.PAYMENT_ID = p_payment_id
           AND T.PAY_PERIOD_ID = p_pay_period_id
         ;
        -- �������� �������� ���, ���������� �������� �������� �������� 
        IF v_bill_date < v_date_from THEN
            v_date_from := v_bill_date;
        ELSIF v_bill_date > v_date_to THEN
            v_date_to := v_bill_date;
        END IF;
    ELSE -- ��� ������ �������� ��������
        v_prev_id := NULL;
        -- ���������� �������� ���, ���������� �������� �������� ��������
        v_date_from := v_bill_date;
        v_date_to   := v_bill_date;
    END IF;
    -- ������� �������� �������� ������� �� ����
    v_transfer_id := Pk02_Poid.Next_transfer_id;
    --    
    INSERT INTO PAY_TRANSFER_T (
           TRANSFER_ID, 
           PAYMENT_ID, PAY_PERIOD_ID,
           BILL_ID, REP_PERIOD_ID, ITEM_ID,
           TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE,
           TRANSFER_DATE, PREV_TRANSFER_ID, NOTES
    )VALUES(
           v_transfer_id, 
           p_payment_id, p_pay_period_id,
           p_bill_id, p_rep_period_id, v_item_id, 
           p_value, p_open_balance, p_close_balance,
           SYSDATE, v_prev_id, p_notes
    );
    
    -- �������� ������ � ��������� �������
    UPDATE PAYMENT_T 
       SET BALANCE   = p_close_balance,
           TRANSFERED= TRANSFERED + p_value,
           DATE_FROM = v_date_from, 
           DATE_TO   = v_date_to
     WHERE PAYMENT_ID = p_payment_id
       AND REP_PERIOD_ID = p_pay_period_id;

    RETURN v_transfer_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- �������������� �������� ������� ������� FIFO �� ������� (payment item) 
-- ������������ ����� ������������� ������ (item-� �� ���������)
-- ��� �������� ������������� �� ��������, ����������:
--   - ������� ������������� ������� �� ������� 
--   - ��� ������ ���������� ����������
FUNCTION Transfer_to_account_fifo(
               p_payment_id    IN INTEGER,  -- ������
               p_pay_period_id IN INTEGER,  -- ID ��������� ������� �����
               p_account_id    IN INTEGER   -- ������� ����, ����� �������� ����������
           ) RETURN NUMBER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Transfer_to_account_fifo';
    v_transfer_id   INTEGER;
    v_value         NUMBER := 0; -- ����� ������� ����� ���������, NULL - ������� �����               
    v_open_balance  NUMBER := 0; -- ����� �� ������� �� ���������� ��������
    v_close_balance NUMBER := 0; -- ����� �� ������� ����� ���������� ��������
    v_bill_due      NUMBER := 0; -- ���������� ���� �� ����� ����� ��������
BEGIN
    -- �������� ������ (FIFO) ������������, �� ������������ ������
    FOR c_bill IN ( 
        SELECT BILL_ID, REP_PERIOD_ID 
          FROM BILL_T
         WHERE ACCOUNT_ID = p_account_id
           AND DUE < 0
           AND BILL_STATUS IN (PK00_CONST.c_BILL_STATE_CLOSED, PK00_CONST.c_BILL_STATE_READY)
         ORDER BY BILL_DATE )
    LOOP
       -- ��� �������� �������� ���� ������� �� ������� 
       v_value := NULL;     
       -- �������� ������ �� ������������ ����� � ������� �� �����������
       v_transfer_id := Transfer_to_bill(
               p_payment_id    => p_payment_id,   -- ID ������� - ��������� �������
               p_pay_period_id => p_pay_period_id,-- ID ��������� ������� ���� ����������� ������
               p_bill_id       => c_bill.bill_id, -- ID ������������� �����
               p_rep_period_id => c_bill.rep_period_id, -- ID ��������� ������� �����
               p_notes         => NULL,           -- ���������� � ��������
               p_value         => v_value,        -- ����� ������� ����� ���������, NULL - ������� �����
               p_open_balance  => v_open_balance, -- ����� �� ������� �� ���������� ��������
               p_close_balance => v_close_balance,-- ����� �� ������� ����� ���������� ��������
               p_bill_due      => v_bill_due      -- ���������� ���� �� ����� ����� ��������
           );
       EXIT WHEN v_transfer_id IS NULL;
    END LOOP; 
    RETURN v_close_balance;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
-- �������� �� �������������� ����� ��� ��� �������� �� �������� �������� 
-- �� ���������� ������� ������������.
-- �������� ������ �/� �� �������� ������ �/� �� ��������
-- �������� ��� '�' �������������� FIFO, '�'-������ ����� ��� ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Payment_processing_fifo( p_from_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Payment_processing_fifo';
    v_ok            INTEGER;    
    v_err           INTEGER;
    v_transfer_id   INTEGER;
    v_value         NUMBER := 0; -- ����� ������� ����� ���������, NULL - ������� �����               
    v_open_balance  NUMBER := 0; -- ����� �� ������� �� ���������� ��������
    v_close_balance NUMBER := 0; -- ����� �� ������� ����� ���������� ��������
    v_bill_due      NUMBER := 0; -- ���������� ���� �� ����� ����� ��������
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, from period_id <= '||p_from_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_ok := 0;    
    v_err:= 0;
    --
    -- �������� ������ ������� �������� �� �������������� � ����������� ������� ����� ���. ���
    FOR r_pay IN (
        SELECT P.ACCOUNT_ID, 
               P.PAYMENT_ID, P.REP_PERIOD_ID PAY_PERIOD_ID, 
               B.BILL_ID, B.REP_PERIOD_ID 
          FROM PAYMENT_T P, BILL_T B, ACCOUNT_T A
         WHERE A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_P
           AND P.ACCOUNT_ID = A.ACCOUNT_ID
           AND B.ACCOUNT_ID = A.ACCOUNT_ID
           AND P.BALANCE > 0
           AND B.DUE < 0
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_CLOSED, Pk00_Const.c_BILL_STATE_READY)
           AND P.REP_PERIOD_ID <= p_from_period_id  
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
            v_transfer_id := Transfer_to_bill(
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
        IF MOD((v_ok+v_err), 500) = 0 THEN
            Pk01_Syslog.Write_msg('Processed: '||v_ok||'-ok, '||v_err||'-err advances', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        --
    END LOOP;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- ������������� �������
-- ------------------------------------------------------------------------ --
-- ������������ ������ � �/� ������� (����� ����� ����������� � ������� �/�)
--   - ������������� - ID ���������� ��������� (PAYMENT.PAYMENT_ID) � ��������, 
--   - ��� ������ ���������� ����������
--
FUNCTION Revers_payment (
               p_src_payment_id IN INTEGER,   -- ID ������������� �������                        
               p_src_period_id  IN INTEGER,   -- ID ��������� �������, ����� ��� ��������������� ������
               p_dst_period_id  IN INTEGER,   -- ID ��������� �������, ������������� �������
               p_manager        IN VARCHAR2,  -- �������� ����������� ��������
               p_notes          IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
           ) RETURN NUMBER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Revers_transfer';
    r_payment      PAYMENT_T%ROWTYPE;
    v_transfer_id  INTEGER;
    v_prev_trn_id  INTEGER;
    v_payment_id   INTEGER;
BEGIN
    v_payment_id := PK02_POID.Next_payment_id;
    -- ������ ������ ������������� �������
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id
       AND NEXT_PAYMENT_ID IS NULL;  -- ��� ���� ������������ ������
    -- ��������� ������������ ������ � ������� �������
    INSERT INTO PAYMENT_T (
        PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE,
        PAYMENT_DATE, ACCOUNT_ID, RECVD,
        ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED,
        DATE_FROM, DATE_TO,
        PAYSYSTEM_ID, DOC_ID,
        STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED,
        CREATED_BY, NOTES, PREV_PAYMENT_ID, PREV_PERIOD_ID
    )VALUES(
        v_payment_id, p_dst_period_id, PK00_CONST.c_PAY_TYPE_REVERS, 
        r_payment.PAYMENT_DATE, r_payment.ACCOUNT_ID, -r_payment.RECVD,
        -r_payment.ADVANCE, SYSDATE, -r_payment.BALANCE, -r_payment.TRANSFERED,
        r_payment.DATE_FROM, r_payment.DATE_TO,
        r_payment.PAYSYSTEM_ID, r_payment.DOC_ID,
        PK00_CONST.c_PAY_STATE_OPEN, SYSDATE, SYSDATE, SYSDATE,
        p_manager, p_notes, r_payment.PAYMENT_ID, r_payment.REP_PERIOD_ID
    );
    -- ��������� ��������� �� ������������ ������    
    UPDATE PAYMENT_T SET NEXT_PAYMENT_ID = v_payment_id
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id
       AND NEXT_PAYMENT_ID IS NULL;  -- ��� ���� ������������ ������
    -- ���������� ��������� ������ �������� ����� �� �������� �������
    UPDATE ACCOUNT_T
       SET BALANCE = BALANCE - r_payment.RECVD,
           BALANCE_DATE = SYSDATE  
     WHERE ACCOUNT_ID = r_payment.ACCOUNT_ID;
    --
    -- ���������� �������� �������� �������   
    v_prev_trn_id := NULL;
    --
    FOR r_trn IN (
        SELECT 
           TRANSFER_ID, 
           PAYMENT_ID, PAY_PERIOD_ID,
           BILL_ID, REP_PERIOD_ID, ITEM_ID,
           TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE,
           TRANSFER_DATE, PREV_TRANSFER_ID, NOTES
          FROM PAY_TRANSFER_T
         WHERE PAYMENT_ID    = r_payment.PAYMENT_ID
           AND PAY_PERIOD_ID = r_payment.REP_PERIOD_ID
         ORDER BY TRANSFER_ID
      )
    LOOP
        -- ��������� ������������ ������ �������� �������
        v_transfer_id := Pk02_Poid.Next_transfer_id;
        --
        INSERT INTO PAY_TRANSFER_T (
           TRANSFER_ID, 
           PAYMENT_ID, PAY_PERIOD_ID,
           BILL_ID, REP_PERIOD_ID, ITEM_ID,
           TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE,
           TRANSFER_DATE, PREV_TRANSFER_ID, NOTES
        )VALUES(
           v_transfer_id, 
           r_payment.PAYMENT_ID, r_payment.REP_PERIOD_ID,
           r_trn.BILL_ID, r_trn.REP_PERIOD_ID, r_trn.ITEM_ID,
           -r_trn.TRANSFER_TOTAL, -r_trn.OPEN_BALANCE, -r_trn.CLOSE_BALANCE,
           SYSDATE, v_prev_trn_id, NULL
        );
        --
        v_prev_trn_id := v_transfer_id;
        --
        -- �������� ������ �����
        UPDATE BILL_T B 
           SET DUE   = DUE   - r_trn.TRANSFER_TOTAL,
               RECVD = RECVD - r_trn.TRANSFER_TOTAL
         WHERE BILL_ID       = r_trn.BILL_ID
           AND REP_PERIOD_ID = r_trn.REP_PERIOD_ID;
        --
    END LOOP;  
    --
    RETURN v_payment_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.Payment_id='||p_src_payment_id, c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- ��������� ������ � ������ �������� ����� �� ������
--   - ������������� - ID ���������� ��������� (PAYMENT.PAYMENT_ID) � ��������, 
--   - ��� ������ ���������� ����������
--
FUNCTION Move_payment (
               p_src_payment_id IN INTEGER,  -- ID ������� ���������
               p_src_period_id  IN INTEGER,  -- ID ��������� ������� ���������
               p_dst_account_id IN INTEGER,  -- ID ������� ���������
               p_dst_period_id  IN INTEGER,  -- ID ��������� ������� ���������
               p_manager        IN VARCHAR2, -- �������� ����������� ��������
               p_notes          IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
           ) RETURN NUMBER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Move_payment';
    r_payment        PAYMENT_T%ROWTYPE;
    v_rev_payment_id INTEGER;
    v_payment_id     INTEGER;
BEGIN
    -- ������ ������ ������������ �������
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id
       AND NEXT_PAYMENT_ID IS NULL;  -- ��� ���� ������������ ������
    -- ���������� ������ ��������
    v_rev_payment_id := Revers_payment (
               p_src_payment_id,  -- ID ������������� �������                        
               p_src_period_id,   -- ID ��������� �������, ����� ��� ��������������� ������
               p_dst_period_id,   -- ID ��������� �������, ������������� �������
               p_manager,         -- �������� ����������� ��������
               p_notes            -- ���������� � ��������
           );
    -- ��������� �������������� ������
    v_payment_id := Add_payment (
              p_account_id      => p_dst_account_id,   -- ID �������� ����� �������
              p_rep_period_id   => p_dst_period_id,   -- ID ��������� ������� ���� ����������� ������
              p_payment_dat�    => r_payment.Payment_Date,        -- ���� �������
              p_payment_type    => Pk00_Const.c_PAY_TYPE_ADJUST,  -- ��� �������
              p_recvd           => r_payment.Recvd,    -- ����� �������
              p_paysystem_id    => r_payment.paysystem_id,   -- ID ��������� �������
              p_doc_id          => r_payment.Doc_Id,  -- ID ��������� � ��������� �������
              p_status          => Pk00_Const.c_PAY_STATE_OPEN,  -- ������ �������
              p_manager    		  => p_manager,  -- �.�.�. ��������� ��������������� ������ �� �/�
              p_notes           => p_notes,  -- ���������� � �������  
              p_prev_payment_id => r_payment.Payment_Id, 
              p_prev_period_id  => r_payment.Rep_Period_Id
           );
    RETURN v_payment_id;  
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.Payment_id='||p_src_payment_id, c_PkgName||'.'||v_prcName );
END;


-- ------------------------------------------------------------------------ --
-- ��������: �������� �� ����� �������?
-- ����� ������� ��������, ���� ������ ������ 
-- � �������� �� ������� ������ ���������� ������
-- ����������: TRUE/FALSE
--
FUNCTION IF_rollback_enable(
               p_pay_period_id IN INTEGER    -- ID ������� �������
           ) RETURN BOOLEAN
IS
    v_fin_period   DATE;
BEGIN
    SELECT CLOSE_FIN_PERIOD INTO v_fin_period
      FROM PERIOD_T
     WHERE PERIOD_ID = p_pay_period_id;
    IF v_fin_period IS NULL THEN
        RETURN TRUE;  -- ������ ����������� ��������� ���. �������
    ELSE
        RETURN FALSE; -- ������ ����������� ��������� ���. �������
    END IF;  
END;

-- ------------------------------------------------------------------------ --
-- �������� ��������� �������� �������� �������, 
-- ��� ������� ��� ������ � ������� �������� ������ ��� �� ������
-- (���� ������ ������, �� ��� ����������� ����� ��� ����������)
--   - ������� ������������� ������� �� ������� 
--   - ��� ������ ���������� ����������
FUNCTION Rollback_transfer(
               p_transfer_id   IN INTEGER,   -- ID �������
               p_pay_period_id IN INTEGER    -- ID ������� �������
           ) RETURN NUMBER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Rollback_transfer';
    v_payment_id     INTEGER;
    v_bill_id        INTEGER;
    v_rep_period_id  INTEGER;
    v_open_balance   NUMBER := 0; -- ����� �� ������� �� ���������� ��������
    v_transfer_total NUMBER := 0; -- ����� �������� �������� �������
    v_count          INTEGER:= 0;
    v_date_from      DATE;
    v_date_to        DATE;
BEGIN
    -- ����� ���������, ��� ��������� �������� ��������� � �������
    SELECT COUNT(*)
      INTO v_count
      FROM PAY_TRANSFER_T
     WHERE PREV_TRANSFER_ID = p_transfer_id
       AND REP_PERIOD_ID = p_pay_period_id;
    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
               '�������� '||p_transfer_id||' - �� ��������� � ������');
    END IF;

    -- �������� ������ �������� ��������
    SELECT PT.BILL_ID, PT.REP_PERIOD_ID, PT.OPEN_BALANCE, PT.TRANSFER_TOTAL, PT.PAYMENT_ID
      INTO v_bill_id, v_rep_period_id, v_open_balance, v_transfer_total, v_payment_id
      FROM PAY_TRANSFER_T PT
     WHERE PT.TRANSFER_ID = p_transfer_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id;

    -- ������ ������ ���� �� ��������� ����������� ������� 
    IF IF_rollback_enable(p_pay_period_id) = FALSE THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
               '������ '||v_payment_id||' - ����������� ��������� ����������� ������� '
                        ||p_pay_period_id);
    END IF;
     
    -- �������� ������������� �� �����, ���� � �������
    UPDATE BILL_T B 
       SET DUE   = DUE + v_transfer_total,
           RECVD = RECVD - v_transfer_total
     WHERE BILL_ID = v_bill_id
       AND REP_PERIOD_ID = v_rep_period_id;

    -- ������� �������� ��������
    DELETE FROM PAY_TRANSFER_T
     WHERE TRANSFER_ID = p_transfer_id
       AND PAY_PERIOD_ID = p_pay_period_id;
     
    -- �������� ��������� ������� ��������
    SELECT MIN(B.BILL_DATE), MAX(B.BILL_DATE)
      INTO v_date_from, v_date_to
      FROM PAY_TRANSFER_T PT, BILL_T B
     WHERE PT.PAYMENT_ID = v_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND PT.BILL_ID = B.BILL_ID
       AND B.REP_PERIOD_ID = v_rep_period_id;
    
    -- ���������� ������ �� ������
    UPDATE PAYMENT_T 
       SET BALANCE   = v_open_balance,
           TRANSFERED= TRANSFERED - v_transfer_total,
           DATE_FROM = v_date_from, 
           DATE_TO   = v_date_to,
           LAST_MODIFIED = SYSDATE
     WHERE PAYMENT_ID = v_payment_id
       AND REP_PERIOD_ID = p_pay_period_id;
 
    RETURN v_open_balance;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- �������� ������, 
-- ��� ������� ��� ������ � ������� �������� ������ ��� �� ������
-- (���� ������ ������, �� ��� ����������� ����� ��� ����������)
--   - ������� ������������� ������� �� ������� 
--   - ��� ������ ���������� ����������
PROCEDURE Rollback_payment(
               p_payment_id    IN INTEGER,   -- ������
               p_pay_period_id IN INTEGER,   -- ID ������� �������
               p_app_user      IN VARCHAR2   -- ������������ ����������
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Rollback_payment';
    v_value          NUMBER;
    v_paysystem_id   INTEGER;
    v_payment_date   DATE;
    v_doc_id         INTEGER;
    v_total          NUMBER;
    
BEGIN
    -- ������ ������ ���� �� ��������� ����������� ������� 
    IF IF_rollback_enable(p_pay_period_id) = FALSE THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
               '������ '||p_payment_id||' - ����������� ��������� ����������� �������: '
                        ||p_pay_period_id);
    END IF;

    -- ������� ��� �������� �������� �������
    FOR c_transfer IN (
            SELECT TRANSFER_ID
              FROM PAY_TRANSFER_T
             WHERE PAYMENT_ID = p_payment_id
               AND PAY_PERIOD_ID = p_pay_period_id
        )
    LOOP
        v_value := Rollback_transfer( c_transfer.transfer_id, p_pay_period_id );
    END LOOP;

    -- �������� ������ �� ��������� �������    
    SELECT P.PAYSYSTEM_ID, PAYMENT_DATE, DOC_ID, RECVD
      INTO v_paysystem_id, v_payment_date, v_doc_id, v_total
      FROM PAYMENT_T P
     WHERE PAYMENT_ID = p_payment_id
       AND REP_PERIOD_ID = p_pay_period_id;
    
    -- ������� ������
    DELETE FROM PAYMENT_T 
     WHERE PAYMENT_ID = p_payment_id
       AND REP_PERIOD_ID = p_pay_period_id;
    
    -- ��������� ���� �������� � ������� �����������
    Pk01_Syslog.Write_msg(p_Msg => '������ ������ PAYSYSTEM_ID='||v_paysystem_id||
                                ', DOC_ID='||v_doc_id||
                                ', DATE='||TO_DATE(v_payment_date,'dd.mm.yyyy')||
                                ', TOTAL='||v_total,
                                p_Src => c_PkgName||'.'||v_prcName,
                                p_Level => Pk01_Syslog.L_info, p_AppUsr => p_app_user );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
--==========================================================================--
-- ��������� ����� ������� ��� �����
--   - ������ ������
--   - ��� ������ ���������� ����������
FUNCTION Fix_advance(
               p_payment_id    IN INTEGER,   -- ID �������
               p_pay_period_id IN INTEGER    -- ID ������� �������
           ) RETURN NUMBER
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Fix_advance';
    v_advance  NUMBER;
BEGIN
    UPDATE PAYMENT_T P
       SET ADVANCE = BALANCE,
           ADVANCE_DATE = SYSDATE
     WHERE P.PAYMENT_ID = p_payment_id
       AND P.REP_PERIOD_ID = p_pay_period_id
    RETURNING ADVANCE INTO v_advance; 
    RETURN v_advance;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--
-- ����������� ��������� ������������ �������� ��� ���������� �������
-- ��������� ����� ��������� ����� �������� ������������ ������� � �� �������� �����������
-- ��������: ������������� ������� �������� ���������� �������� ������������� ���������!!!
PROCEDURE Refresh_advance(
               p_pay_period_id IN INTEGER    -- ID ������� �������
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Refresh_advance';
    v_advance  NUMBER;
BEGIN
    MERGE INTO PAYMENT_T P
    USING (
        SELECT REP_PERIOD_ID, PAYMENT_ID, SUM(TRANSFER_TOTAL) TRANSFER_TOTAL 
          FROM PAY_TRANSFER_T
         WHERE REP_PERIOD_ID <= PAY_PERIOD_ID
        GROUP BY REP_PERIOD_ID, PAYMENT_ID
    ) PT
    ON (PT.REP_PERIOD_ID = P.REP_PERIOD_ID AND PT.PAYMENT_ID = P.PAYMENT_ID)
    WHEN MATCHED THEN 
      UPDATE SET P.ADVANCE = P.RECVD - PT.TRANSFER_TOTAL, 
                 P.ADVANCE_DATE = SYSDATE;  
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--==========================================================================--
-- ����� �������� ����� �� ������ �������� (�������� �������� �� ���������, ������ ����� ���������)
--   > 0 - ID �������� ����� � ��������
--   NULL - �������� �� ������� 
--   - ��� ������ ���������� ����������
FUNCTION Find_account_by_phone (
               p_phone         IN VARCHAR2,  -- ����� ��������
               p_date          IN DATE       -- ���� �� ������� ���� ������������
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_account_by_phone';
    v_account_id INTEGER;
BEGIN
    BEGIN
        -- ���� � �������� ���������
        SELECT O.ACCOUNT_ID INTO v_account_id
          FROM ORDER_T O, ORDER_PHONES_T R
         WHERE R.ORDER_ID = O.ORDER_ID
           AND R.PHONE_NUMBER = p_phone
           AND R.DATE_FROM <= p_date
           AND (R.DATE_TO IS NULL OR p_date < R.DATE_TO);
        RETURN v_account_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- ���� �� ����� � ��������, ���� ��������� ��������
            SELECT ACCOUNT_ID INTO v_account_id
            FROM (
                SELECT R.PHONE_NUMBER, O.ACCOUNT_ID, R.DATE_FROM, R.DATE_TO, 
                       MAX(R.DATE_TO) OVER (PARTITION BY R.PHONE_NUMBER) MAX_DATE_TO
                  FROM ORDER_T O, ORDER_PHONES_T R
                 WHERE R.ORDER_ID = O.ORDER_ID
                   AND R.PHONE_NUMBER = p_phone
            )
            WHERE DATE_TO = MAX_DATE_TO;  
            RETURN v_account_id;
    END;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
         RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --    
-- ����� ID �������� ����� �� ������ ������������� �����
--   > 0  - ������������� - ID �������� ����� � �������� 
--   NULL - ������� �� �������
--   - ��� ������ ���������� ����������
FUNCTION Find_account_by_billno (
               p_bill_no       IN VARCHAR2   -- ����� ������������� �����
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_account_by_billno';
    v_account_id INTEGER;
BEGIN
    SELECT ACCOUNT_ID INTO v_account_id
      FROM BILL_T
     WHERE BILL_NO = p_bill_no;
    RETURN v_account_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- ����� ID ����� �� ������ ������������� �����
--   > 0  - ������������� - ID ����� � �������� 
--   NULL - ������� �� �������
--   - ��� ������ ���������� ����������
FUNCTION Find_id_by_billno (
               p_bill_no       IN VARCHAR2   -- ����� ������������� �����
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_id_by_billno';
    v_bill_id    INTEGER;
BEGIN
    SELECT BILL_ID INTO v_bill_id
      FROM BILL_T
     WHERE BILL_NO = p_bill_no;
    RETURN v_bill_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- ����� ID �������� ����� �� ������ �������� �����
--   > 0  - ID �������� ����� � �������� 
--   NULL - ������� �� �������
--   - ��� ������ ���������� ����������
FUNCTION Find_id_by_accountno (
               p_account_no    IN VARCHAR2   -- ����� ��������
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_id_by_accountno';
    v_account_id INTEGER;
BEGIN
    SELECT ACCOUNT_ID INTO v_account_id
      FROM ACCOUNT_T
     WHERE ACCOUNT_NO = p_account_no;
    RETURN v_account_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ����� �������� �������� �� ������� c ������� ������ ������, ����������
--   > 0  - ITEM_ID PAYMENT ��� TRANSFER � �������� 
--   - ��� ������ ���������� ����������
--FUNCTION Rollback_transfer() RETURN INTEGER;

-- ------------------------------------------------------------------------ --
-- ������ �������� �� �������� �����
--   - ������������� - ���-�� ��������� �������
--   - ��� ������ ���������� ����������
FUNCTION Account_payment_list (
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID �������� �����
               p_date_from  IN DATE,
               p_date_to    IN DATE 
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Account_payment_list';
    v_retcode       INTEGER;
    v_min_period_id INTEGER;
    v_max_period_id INTEGER;
    v_date_from     DATE;
    v_date_to       DATE;
BEGIN
    -- ���������� ������� ���������
    IF p_date_from IS NOT NULL THEN
        v_date_from := p_date_from;
    ELSE
        v_date_from := TO_DATE('01.01.2000','dd.mm.yyyy');
    END IF;
    --
    IF p_date_to IS NOT NULL THEN
        v_date_to := p_date_to;
    ELSE
        v_date_to := SYSDATE+1;
    END IF;
    -- ��������� ������� ��������, ��� �������� ������ ������� ��� ���������� �����
    v_min_period_id := Pk04_Period.Period_id(v_date_from);
    v_max_period_id := Pk04_Period.Period_id(v_date_to);
    
    -- ��������� ���-�� �������
    SELECT COUNT(*) INTO v_retcode 
     FROM PAYMENT_T P
    WHERE ACCOUNT_ID = p_account_id
      AND REP_PERIOD_ID BETWEEN v_min_period_id AND v_max_period_id;
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
          SELECT PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE, PAYMENT_DATE, 
                 ACCOUNT_ID, RECVD, ADVANCE, ADVANCE_DATE, 
                 BALANCE, TRANSFERED, DATE_FROM, DATE_TO, 
                 PS.PAYSYSTEM_ID, PS.PAYSYSTEM_NAME, DOC_ID,
                 STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED, 
								 P.CREATED_BY, P.MODIFIED_BY,
                 P.NOTES 
           FROM PAYMENT_T P, PAYSYSTEM_T PS
          WHERE ACCOUNT_ID = p_account_id
            AND REP_PERIOD_ID BETWEEN v_min_period_id AND v_max_period_id
            AND P.PAYSYSTEM_ID = PS.PAYSYSTEM_ID
          ORDER BY PAYMENT_ID DESC;
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;  

-- ------------------------------------------------------------------------ --
-- ������ �������� �� �������� �����
--   - ������������� - ���-�� ��������� �������
--   - ��� ������ ���������� ����������
FUNCTION Account_payment_list (
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID �������� �����
               p_period_id  IN INTEGER
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Account_payment_list';
    v_retcode       INTEGER;
BEGIN   
    -- ��������� ���-�� �������
    SELECT COUNT(*) INTO v_retcode 
     FROM PAYMENT_T P
    WHERE ACCOUNT_ID = p_account_id
      AND REP_PERIOD_ID = p_period_id;

    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
          SELECT PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE, PAYMENT_DATE, 
                 ACCOUNT_ID, RECVD, ADVANCE, ADVANCE_DATE, 
                 BALANCE, TRANSFERED, DATE_FROM, DATE_TO, 
                 PS.PAYSYSTEM_ID, PS.PAYSYSTEM_NAME, DOC_ID,
                 STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED, 
								 P.CREATED_BY, P.MODIFIED_BY,
                 P.NOTES 
           FROM PAYMENT_T P, PAYSYSTEM_T PS
          WHERE ACCOUNT_ID = p_account_id
            AND REP_PERIOD_ID = p_period_id
            AND P.PAYSYSTEM_ID = PS.PAYSYSTEM_ID
          ORDER BY PAYMENT_ID DESC;
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- ������ ����� ����������� ����
--   - ������������� - ���-�� ��������� �������
--   - ��� ������ ���������� ����������
FUNCTION Bill_pay_list (
               p_recordset    OUT t_refc, 
               p_bill_id       IN INTEGER,    -- ID �������
               p_rep_period_id IN INTEGER     -- ID ��������� ������� �����
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bill_pay_list';
    v_retcode    INTEGER;
BEGIN
    -- ��������� ���-�� �������
    SELECT COUNT(*) INTO v_retcode
      FROM PAY_TRANSFER_T PT, PAYMENT_T P, PAYSYSTEM_T PS,BILL_T B
     WHERE B.BILL_ID       = p_bill_id
       AND B.REP_PERIOD_ID = p_rep_period_id
       AND B.BILL_ID       = PT.BILL_ID
       AND B.REP_PERIOD_ID = PT.REP_PERIOD_ID
       AND PT.PAYMENT_ID   = P.PAYMENT_ID
       AND PT.PAY_PERIOD_ID= P.REP_PERIOD_ID
       AND PS.PAYSYSTEM_ID = P.PAYSYSTEM_ID;
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
          SELECT B.BILL_ID, B.BILL_NO, B.REP_PERIOD_ID BILL_REP_PERIOD_ID, B.RECVD,
                 PT.TRANSFER_ID, PT.TRANSFER_TOTAL, PT.TRANSFER_DATE,
                 PT.OPEN_BALANCE, PT.CLOSE_BALANCE,
                 P.PAYMENT_ID, P.DOC_ID,P.RECVD,P.REP_PERIOD_ID,P.PAYMENT_TYPE,P.NOTES, P.PAYMENT_DATE, P.PAYSYSTEM_ID, PS.PAYSYSTEM_NAME
            FROM PAY_TRANSFER_T PT, PAYMENT_T P, PAYSYSTEM_T PS,BILL_T B
           WHERE B.BILL_ID       = p_bill_id
             AND B.REP_PERIOD_ID = p_rep_period_id
             AND B.BILL_ID       = PT.BILL_ID
             AND B.REP_PERIOD_ID = PT.REP_PERIOD_ID
             AND PT.PAYMENT_ID   = P.PAYMENT_ID
             AND PT.PAY_PERIOD_ID= P.REP_PERIOD_ID
             AND PS.PAYSYSTEM_ID = P.PAYSYSTEM_ID
          ORDER BY PT.TRANSFER_ID;
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- ������ ����� ����������� ����� �� ������������ ������ �� ������������� �������� �����
--   - ������������� - ���-�� ��������� �������
--   - ��� ������ ���������� ����������
FUNCTION Bill_pay_list_by_account (
               p_recordset    OUT t_refc, 
               p_account_id   IN INTEGER,    -- ID �������� �����
               p_rep_period_id IN INTEGER     -- ID ��������� ������� �����
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bill_pay_list_by_account';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
          SELECT B.BILL_ID, B.BILL_NO,B.REP_PERIOD_ID BILL_REP_PERIOD_ID, B.RECVD,
                 PT.TRANSFER_ID, PT.TRANSFER_TOTAL, PT.TRANSFER_DATE,
                 PT.OPEN_BALANCE, PT.CLOSE_BALANCE,
                 P.PAYMENT_ID, P.DOC_ID,P.RECVD,P.REP_PERIOD_ID,P.PAYMENT_TYPE,P.NOTES, P.PAYMENT_DATE, P.PAYSYSTEM_ID, PS.PAYSYSTEM_NAME
            FROM PAY_TRANSFER_T PT, PAYMENT_T P, PAYSYSTEM_T PS,BILL_T B
           WHERE B.BILL_ID       = PT.BILL_ID
             AND B.REP_PERIOD_ID = PT.REP_PERIOD_ID
             AND PT.PAYMENT_ID   = P.PAYMENT_ID
             AND PT.PAY_PERIOD_ID= P.REP_PERIOD_ID
             AND PS.PAYSYSTEM_ID = P.PAYSYSTEM_ID
             AND B.ACCOUNT_ID    = p_account_id
             AND B.REP_PERIOD_ID = p_rep_period_id
          ORDER BY PT.TRANSFER_ID;
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- �������� �������� ������� �� ������
--   - ������������� - ���-�� ��������� �������
--   - ��� ������ ���������� ����������
FUNCTION Transfer_list (
               p_recordset    OUT t_refc, 
               p_payment_id    IN INTEGER,   -- ID �������
               p_pay_period_id IN INTEGER    -- ID ��������� ������� �����
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Transfer_list';
    v_retcode    INTEGER;
BEGIN
    -- ��������� ���-�� �������
    SELECT COUNT(*) INTO v_retcode
      FROM BILL_T B, PAY_TRANSFER_T PT, PAYMENT_T P, PAYSYSTEM_T PS
     WHERE B.BILL_ID       = PT.BILL_ID
       AND B.REP_PERIOD_ID = PT.REP_PERIOD_ID
       AND PT.PAYMENT_ID   = P.PAYMENT_ID
       AND PT.PAY_PERIOD_ID= P.REP_PERIOD_ID
       AND P.PAYMENT_ID    = p_payment_id
       AND P.REP_PERIOD_ID = p_pay_period_id
       AND PS.PAYSYSTEM_ID = P.PAYSYSTEM_ID;
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
          SELECT B.BILL_ID, B.BILL_NO, B.BILL_DATE, B.RECVD,
                 PT.TRANSFER_ID, PT.TRANSFER_TOTAL, PT.TRANSFER_DATE,
                 PT.OPEN_BALANCE, PT.CLOSE_BALANCE,
                 P.PAYMENT_DATE, P.PAYSYSTEM_ID, PS.PAYSYSTEM_NAME
            FROM BILL_T B, PAY_TRANSFER_T PT, PAYMENT_T P, PAYSYSTEM_T PS
           WHERE B.BILL_ID       = PT.BILL_ID
             AND B.REP_PERIOD_ID = PT.REP_PERIOD_ID
             AND PT.PAYMENT_ID   = P.PAYMENT_ID
             AND PT.PAY_PERIOD_ID= P.REP_PERIOD_ID
             AND P.PAYMENT_ID    = p_payment_id
             AND P.REP_PERIOD_ID = p_pay_period_id
             AND PS.PAYSYSTEM_ID = P.PAYSYSTEM_ID
          ORDER BY PT.TRANSFER_ID;
          
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


END PK10_PAYMENT_OLD;
/
