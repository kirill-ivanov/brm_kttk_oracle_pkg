CREATE OR REPLACE PACKAGE PK10_PAYMENT
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
    -- 6) �������������
    -- 7) �������������
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
									p_descr						IN VARCHAR2		DEFAULT NULL -- �������� �������
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- ���������� ������ ������� 
    --
    PROCEDURE Set_currency (
                  p_rep_period_id   IN INTEGER,   -- ID ��������� ������� ���� ����������� ������
                  p_payment_id      IN INTEGER,   -- ID �������
                  p_currency_id     IN INTEGER    -- ��� ������
               );

    -- ------------------------------------------------------------------------ --
    -- �������� �������������� ������ �� �/� ������� 
    --
    FUNCTION Add_adjust_payment (
                  p_account_id      IN INTEGER,   -- ID �������� ����� �������
                  p_rep_period_id   IN INTEGER,   -- ID ��������� ������� ���� ����������� ������
                  p_payment_dat�    IN DATE,      -- ���� �������
                  p_amount          IN NUMBER,    -- ����� �������
                  p_doc_id          IN VARCHAR2,  -- ID ��������� � ��������� �������
                  p_manager         IN VARCHAR2,  -- �.�.�. ��������� ��������������� ������ �� �/�
                  p_notes           IN VARCHAR2   -- ���������� � �������  
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- ������� ������
    --
    FUNCTION Remove_Payment(
                  p_payment_id		IN INTEGER,
                  p_period_id			IN INTEGER
              ) RETURN VARCHAR2;

    -- ------------------------------------------------------------------------ --
    -- ��������� ������ �������� �� �������� �� ������ � �����
    --
    PROCEDURE Align_Payments_Period(
                  p_period	IN DATE DEFAULT TRUNC(SYSDATE,'mm')
              );

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
                   p_value         IN NUMBER,     -- ����� ������� ����� ���������, NULL - ������� �����               
                   p_open_balance  OUT NUMBER,    -- ����� �� ������� �� ���������� ��������
                   p_close_balance OUT NUMBER,    -- ����� �� ������� ����� ���������� ��������
                   p_bill_due      OUT NUMBER     -- ���������� ���� �� ����� ����� ��������
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- �������� �������� �������� �������, 
    -- ��� ������� ��� ������ � ������� �������� ������ ��� �� ������
    -- (���� ������ ������, �� ��� ����������� ����� ��� ����������)
    --   - ������� ������������� ������� �� ������� 
    --   - ��� ������ ���������� ����������
    PROCEDURE Rollback_transfer(
                   p_pay_period_id IN INTEGER,
                   p_payment_id    IN INTEGER,
                   p_transfer_id   IN INTEGER
               );
/*
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
*/
    -- ------------------------------------------------------------------------ --
    -- ������������ �������� �������� �������, ����� �� �����-�� �������� �������� ������
    -- ��� ������� ��� ������ � ������� �������� ������ ��� �� ������
    -- ����������:
    --   - ID ������������ ������ 
    --   - ��� ������ ���������� ����������
    FUNCTION Revers_transfer(
                   p_transfer_id   IN INTEGER,   -- ID ������������ �������� �������� �������
                   p_pay_period_id IN INTEGER,   -- ID ������� �������
                   p_notes         IN VARCHAR2   -- ����������
               ) RETURN INTEGER;
            
    -- ------------------------------------------------------------------------ --
    -- �������������� �������� ������� ������� FIFO �� ������� (payment item) 
    -- ������������ ����� ������������� ������ (item-� �� ���������)
    -- ��� �������� ������������� �� ��������, ����������:
    --   - ������� ������������� ������� �� ������� 
    --   - ��� ������ ���������� ����������
    FUNCTION Transfer_to_account_fifo(
                   p_payment_id    IN INTEGER,  -- ������
                   p_pay_period_id IN INTEGER,  -- ID ��������� ������� �������
                   p_account_id    IN INTEGER DEFAULT NULL   -- ������� ����, ����� �������� ����������
               ) RETURN NUMBER;

    -- ========================================================================== --
    -- �����
    -- ========================================================================== --
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

    -- ========================================================================== --
    -- �-�� ������
    -- ========================================================================== --
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

    -- ========================================================================== --
    -- �-�� �����������
    -- ========================================================================== --
    -- ------------------------------------------------------------------------ --
    -- ������ �������� �� �������� �����
    --   - ��� ������ ���������� ����������
    PROCEDURE Account_payment_list (
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,   -- ID �������� �����
                   p_date_from  IN DATE,
                   p_date_to    IN DATE 
               );

    -- ------------------------------------------------------------------------ --
    -- ������ �������� �� �������� �����
    --   - ��� ������ ���������� ����������
    PROCEDURE Account_payment_list (
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID �������� �����
               p_period_id  IN INTEGER
           ); 

    -- ------------------------------------------------------------------------ --              
    -- ������ ����� ����������� ����
    --   - ��� ������ ���������� ����������
    PROCEDURE Bill_pay_list (
                   p_recordset    OUT t_refc, 
                   p_bill_id       IN INTEGER,    -- ID �������
                   p_rep_period_id IN INTEGER     -- ID ��������� ������� �����
               );

    -- ------------------------------------------------------------------------ --
    -- ������ ����� ����������� ����� �� ������������ ������ �� ������������� �������� �����
    --   - ��� ������ ���������� ����������
    PROCEDURE Bill_pay_list_by_account (
                   p_recordset    OUT t_refc, 
                   p_account_id   IN INTEGER,    -- ID �������� �����
                   p_rep_period_id IN INTEGER     -- ID ��������� ������� �����
               );  
            
    -- �������� �������� ������� �� ������
    --   - ��� ������ ���������� ����������
    PROCEDURE Transfer_list (
                   p_recordset    OUT t_refc, 
                   p_payment_id    IN INTEGER,   -- ID �������
                   p_pay_period_id IN INTEGER    -- ID ��������� ������� �����
               );

    -- ------------------------------------------------------------------------ --
    -- �������� ��������� ������ � ������� �������� �������
    -- ����������:
    --  ID   - ��������� ������ ��������
    --  NULL - ���� ������� ���
    FUNCTION Get_transfer_tail (
                   p_payment_id    IN INTEGER,   -- ID ��������������� �������
                   p_pay_period_id IN INTEGER    -- ID ��������� �������, ����� ��� ��������������� ������
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- �������� ������ ������ � ������� �������� �������
    -- ����������:
    --  ID - ��������� ������ ��������
    --  NULL - ���� ������� ���
    FUNCTION Get_transfer_head (
                   p_payment_id    IN INTEGER,   -- ID ��������������� �������
                   p_pay_period_id IN INTEGER    -- ID ��������� �������, ����� ��� ��������������� ������
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- ������� ������ ������� ������� ��������� ������
    --
    PROCEDURE Payment_bound_time (
                   p_payment_id    IN INTEGER,   -- ID ��������������� �������
                   p_pay_period_id IN INTEGER    -- ID ��������� �������, ����� ��� ��������������� ������
               );

END PK10_PAYMENT;
/
CREATE OR REPLACE PACKAGE BODY PK10_PAYMENT
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
              p_manager         IN VARCHAR2,  -- �.�.�. ��������� ��������������� ������ �� �/�
              p_notes           IN VARCHAR2,  -- ���������� � �������  
              p_descr           IN VARCHAR2  DEFAULT NULL  -- �������� �������
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_payment';
    v_payment_id  INTEGER;
    v_billing_id  INTEGER;
BEGIN
    -- ��������� ������� �������� �����
    SELECT a.billing_id
      INTO v_billing_id
      FROM ACCOUNT_T a
     WHERE a.account_id = p_account_id;
     
--    IF v_billing_id NOT IN (2003, 2007) THEN
--      Pk01_Syslog.raise_Exception('ERROR billing_id = '||v_bill_id, c_PkgName||'.'||v_prcName );
--    END IF;
    
    v_payment_id := PK02_POID.Next_payment_id;
    -- c�������� ���������� � �������
    INSERT INTO PAYMENT_T (
        PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE,
        PAYMENT_DATE, ACCOUNT_ID, RECVD,
        ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED, REFUND,
        DATE_FROM, DATE_TO,
        PAYSYSTEM_ID, DOC_ID,
        STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED,
        CREATED_BY, NOTES, Pay_Descr
    )VALUES(
        v_payment_id, p_rep_period_id, p_payment_type,
        p_payment_dat�, p_account_id, p_recvd,
        p_recvd, p_payment_dat�, p_recvd, 0, 0,
        NULL, NULL,
        p_paysystem_id, p_doc_id,
        p_status, SYSDATE, SYSDATE, SYSDATE,
        p_manager, p_notes, p_descr
    );
    -- �������� ������ �������� ����� �� �������� �������
    UPDATE ACCOUNT_T
       SET BALANCE = BALANCE + p_recvd,
           BALANCE_DATE = p_payment_dat�  
     WHERE ACCOUNT_ID = p_account_id;
    --
    RETURN v_payment_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- ���������� ������ ������� 
--
PROCEDURE Set_currency (
              p_rep_period_id   IN INTEGER,   -- ID ��������� ������� ���� ����������� ������
              p_payment_id      IN INTEGER,   -- ID �������
              p_currency_id     IN INTEGER    -- ��� ������
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_adjust_payment';
  	v_payment_id	NUMBER;
BEGIN
    --
    UPDATE PAYMENT_T P
       SET P.CURRENCY_ID   = p_currency_id
     WHERE P.REP_PERIOD_ID = p_rep_period_id
       AND P.PAYMENT_ID    = p_payment_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- �������� �������������� ������ �� �/� ������� 
--
FUNCTION Add_adjust_payment (
              p_account_id      IN INTEGER,   -- ID �������� ����� �������
              p_rep_period_id   IN INTEGER,   -- ID ��������� ������� ���� ����������� ������
              p_payment_dat�    IN DATE,      -- ���� �������
              p_amount          IN NUMBER,    -- ����� �������
              p_doc_id          IN VARCHAR2,  -- ID ��������� � ��������� �������
              p_manager         IN VARCHAR2,  -- �.�.�. ��������� ��������������� ������ �� �/�
              p_notes           IN VARCHAR2   -- ���������� � �������  
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_adjust_payment';
  	v_payment_id	NUMBER;
BEGIN
    v_payment_id := PK10_PAYMENT.Add_payment(
                      p_account_id, 
                      p_rep_period_id, 
                      p_payment_dat�, 
                      Pk00_Const.c_PAY_TYPE_ADJUST_BALANCE,
                      -p_amount, 
                      Pk00_Const.c_PAYSYSTEM_CORRECT_ID,
                      p_doc_id, 
                      pk00_const.c_PAY_STATE_OPEN, 
                      p_manager, 
                      p_notes
                   );
	  -- ��������� �������� �������������
    INSERT INTO PAYMENT_OPERATION_T O (
        O.OPER_ID, O.OPER_TYPE_ID, O.OPER_DATE, O.OPER_TOTAL, 
        O.SRC_PAYMENT_ID, O.SRC_REP_PERIOD_ID,
        O.DST_PAYMENT_ID, O.DST_REP_PERIOD_ID,
        O.CREATED_BY, O.NOTES )
    VALUES(
        Pk02_Poid.Next_transfer_id, Pk00_Const.c_PAY_OP_ADJUST_BALANCE, SYSDATE+get_tz_offset, -p_amount,
        v_payment_id, p_rep_period_id, v_payment_id, p_rep_period_id,
        p_manager, p_notes
    );
	  RETURN v_payment_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- ������� ������
--
FUNCTION Remove_Payment(
      p_payment_id		IN INTEGER,
      p_period_id			IN INTEGER
	) RETURN VARCHAR2 
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_payment';
    v_cnt				  INTEGER;
    v_account_id	INTEGER;
BEGIN
    -- ���. ����
    SELECT account_id INTO v_account_id
      FROM PAYMENT_T 
     WHERE payment_id = p_payment_id 
       AND rep_period_id = p_period_id;
       
    -- ��������� ������������� �� �����
    SELECT COUNT(1) INTO v_cnt
      FROM PAY_TRANSFER_T p 
     WHERE p.payment_id   = p_payment_id 
       AND p.pay_period_id= p_period_id;
    IF v_cnt > 0 THEN
        RETURN '������ ����������� �� �����';
    END IF;
    
    -- ��������� ������
    SELECT COUNT(1) INTO v_cnt 
      FROM PERIOD_T p
     WHERE p.period_id = p_period_id 
       AND p.position IN(
           pk00_const.c_PERIOD_OPEN, 
           pk00_const.c_PERIOD_NEXT, 
           pk00_const.c_PERIOD_BILL);
    IF v_cnt =  0 THEN
        RETURN '������ ������� �� ������';
    END IF;             

    -- ������� ������
    DELETE FROM payment_t 
     WHERE payment_id = p_payment_id 
       AND rep_period_id = p_period_id;
       
    -- ����������� ������ �������� �����
    v_cnt := PK05_ACCOUNT_BALANCE.Refresh_balance(v_account_id);
    
    return 'OK';
EXCEPTION
    WHEN OTHERS THEN
      Pk01_Syslog.Write_error('Error', c_PkgName||'.'||v_prcName );
      return '������ ��� �������� �������';
END;

-- ------------------------------------------------------------------------ --
-- ��������� ������ �������� �� �������� �� ������ � �����
--
PROCEDURE Align_Payments_Period(
       p_period	IN DATE DEFAULT TRUNC(SYSDATE,'mm')
  	) 
IS
    v_prcName			   VARCHAR2(30) := 'Align_Payments_Period';
  	v_period				 DATE    := trunc(p_period, 'mm');
	  v_nb_period		   INTEGER := Pk04_Period.Period_id(p_period);
	  v_nb_last_period INTEGER;
	  
BEGIN
    -- ��������� ��� ��� �� �������� ������
    SELECT period_id 
      INTO v_nb_last_period
      FROM PERIOD_T
     WHERE position = 'LAST';
    -- ���������� ���������� ������ � ��������
    UPDATE PAYMENT_T
       SET rep_period_id = v_nb_period
     WHERE 1=1 -- rep_period_id > v_nb_last_period
       AND rep_period_id != v_nb_period
       AND EXISTS(
        SELECT 1
          FROM payment_gate.ps_registry_payments@mmtdb.world p
         WHERE calc_date = v_period
           AND p.receipt = doc_id 
           AND p.ps_id   = paysystem_id
           AND reg_id    = 0
           AND p.b_status = 1);
    Pk01_Syslog.Write_msg(p_Msg => 'eps:' || SQL%ROWCOUNT, p_src=>c_PkgName||'.'||v_prcName);
    --
    UPDATE PAYMENT_T u
       SET rep_period_id = v_nb_period
     WHERE 1=1 --rep_period_id > v_nb_last_period
       AND rep_period_id != v_nb_period
       AND exists(
        SELECT 1
          FROM payment_gate.ps_sbrf_registry@mmtdb.world p
         WHERE calc_date    = v_period
           AND p.payment_id = u.doc_id 
           AND 11 = u.paysystem_id
       );
    Pk01_Syslog.Write_msg(p_Msg => 'sbrf:' || SQL%ROWCOUNT, p_src=>c_PkgName||'.'||v_prcName);
    COMMIT;
    
EXCEPTION 
  WHEN OTHERS THEN
  	ROLLBACK;
   	Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName);
END;



-- ------------------------------------------------------------------------ --
-- ������� ����� (��� ���� �����) ������� �� ������� ������������ ����
-- PS: ����� ��������, ������� ������� - ������� � ��������� ������ � ������ ��� ��� ���������� ��������
-- �������� ������� �� ������, ������������ � ������ ������� ��� ����� ������ ������� 
-- ����� ��������� �����,
-- ����������:
--   - PAY_TRANSFER.TRANSFER_ID ��������� �������� �������� 
--   - NULL - ���� ����� �������� ����� 0
--   - ��� ������ ���������� ����������
FUNCTION Transfer_to_bill(
               p_payment_id    IN INTEGER,    -- ID ������� - ��������� �������
               p_pay_period_id IN INTEGER,    -- ID ��������� ������� ���� ����������� ������
               p_bill_id       IN INTEGER,    -- ID ������������� �����
               p_rep_period_id IN INTEGER,    -- ID ��������� ������� �����               
               p_notes         IN VARCHAR2,   -- ���������� � ��������
               p_value         IN NUMBER,     -- ����� ������� ����� ���������               
               p_open_balance  OUT NUMBER,    -- ����� �� ������� �� ���������� ��������
               p_close_balance OUT NUMBER,    -- ����� �� ������� ����� ���������� ��������
               p_bill_due      OUT NUMBER     -- ���������� ���� �� ����� ����� ��������
           ) RETURN INTEGER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Transfer_to_bill';
    v_transfer_id  INTEGER := NULL;
    v_bill_date    DATE;
    v_date_from    DATE; 
    v_date_to      DATE;
    v_advance      NUMBER;
    v_balance      NUMBER;
    v_transfered   NUMBER;
    v_payment_type PAYMENT_T.PAYMENT_TYPE%TYPE;
BEGIN
    -- ��������� ��������� ����������
    p_open_balance := 0;
    p_close_balance:= 0;
    p_bill_due := 0;
    
    -- ���� ����� ������ ���� �������������, �������
    IF p_value < 0 THEN
        Pk01_Syslog.Raise_user_exception('������. ����� ������� ������ 0 ��� ('||p_value||')' , c_PkgName||'.'||v_prcName);
    END IF;
    
    -- ���� ����� ����� ����, �������
    IF p_value = 0 THEN
        RETURN NULL;
    END IF;
    
    -- �������� ������������� ������� �� ������� (�������� �������)
    -- �������� ����� FOR_UPDATE ��� ���������� ������ - ����� �����, �� ������ �� ����������
    -- ��� �������� �������� � commit � �����
    SELECT P.BALANCE, P.BALANCE, P.TRANSFERED, P.ADVANCE, 
           P.PAYMENT_TYPE, P.DATE_FROM, P.DATE_TO
      INTO p_open_balance, v_balance, v_transfered, v_advance, 
           v_payment_type, v_date_from, v_date_to
      FROM PAYMENT_T P
     WHERE P.REP_PERIOD_ID = p_pay_period_id
       AND P.PAYMENT_ID = p_payment_id;

    -- ������ �� ������� �� ����� ����� ������������� (������� ����� �� ������)
    IF v_balance < p_value THEN
      PK01_SYSLOG.Raise_user_exception('payment_id='||p_payment_id||
       ', bill_id='||p_bill_id||', pay_value='||p_value||
       ' - ������������� ����� ��������� ������� �� �������' , 
       c_PkgName||'.'||v_prcName );
    END IF;

    -- �������� ����� �� ������������ ���� 
    UPDATE BILL_T B 
       SET DUE   = DUE   + p_value, 
           RECVD = RECVD + p_value
     WHERE BILL_ID       = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
    RETURNING DUE, BILL_DATE INTO p_bill_due, v_bill_date;
    
    -- ������� �������� �������� ������� �� ����
    v_transfer_id := Pk02_Poid.Next_transfer_id;
    --    
    p_close_balance := p_open_balance - p_value;
    --
    INSERT INTO PAY_TRANSFER_T (
           TRANSFER_ID, 
           PAYMENT_ID, PAY_PERIOD_ID,
           BILL_ID, REP_PERIOD_ID,
           TRANSFER_TOTAL,
           TRANSFER_DATE, NOTES
    )
    VALUES(
           v_transfer_id, 
           p_payment_id, p_pay_period_id,
           p_bill_id, p_rep_period_id, 
           p_value,
           SYSDATE, p_notes
    );

    -- �������� ��������� ������� ��������
    IF v_bill_date < v_date_from THEN
        v_date_from := v_bill_date;
    ELSIF v_date_to < v_bill_date THEN
        v_date_to := v_bill_date;
    END IF;

    -- ��������� ��������� �� �������
    v_balance    := v_balance - p_value;
    v_transfered := v_transfered + p_value;
    
    -- ����� ������� ������ ��� �������� �� ����� � ������� � ���������� ��������
    -- ��� �������� ���������� �������� 
    IF v_payment_type IN (PK00_CONST.c_PAY_TYPE_ADJUST, PK00_CONST.c_PAY_TYPE_REVERS) THEN
        v_advance := 0;
    ELSIF p_pay_period_id >= p_rep_period_id AND Pk04_Period.Is_closed(p_pay_period_id) = FALSE THEN
        v_advance:= v_advance - p_value;
    END IF;
    -- �������� ������ � �������� �������
    UPDATE PAYMENT_T 
       SET BALANCE   = v_balance,
           TRANSFERED= v_transfered,
           ADVANCE   = v_advance,
           DATE_FROM = v_date_from, 
           DATE_TO   = v_date_to
     WHERE PAYMENT_ID= p_payment_id
       AND REP_PERIOD_ID = p_pay_period_id;

    -- ���������� ID �������� ��������
    RETURN v_transfer_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- �������� �������� �������� �������, 
-- ��� ������� ��� ������ � ������� �������� ������ ��� �� ������
-- (���� ������ ������, �� ��� ����������� ����� ��� ����������)
--   - ID ���������� �������� �������� 
--   - ��� ������ ���������� ����������
PROCEDURE Rollback_transfer(
               p_pay_period_id IN INTEGER,
               p_payment_id    IN INTEGER,
               p_transfer_id   IN INTEGER
           )
IS
    v_prcName          CONSTANT VARCHAR2(30) := 'Rollback_transfer';
BEGIN
    -- ���������� ������ �� ������ ���� ������  
    IF Pk04_Period.Is_closed(p_pay_period_id) = TRUE THEN
        Pk01_Syslog.raise_user_Exception( '������ ����������� ��������� ����������� �������: '||
                                      p_pay_period_id, c_PkgName||'.'||v_prcName );
    END IF;
    -- ������� �������� ��������
    PK10_PAYMENTS_TRANSFER.Delete_from_chain (
               p_pay_period_id,
               p_payment_id,
               p_transfer_id
           );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- ������������ �������� �������� �������, ����� �� �����-�� �������� �������� ������
-- ��� ������� ��� ������ � ������� �������� ������ ��� �� ������
-- ����������:
--   - ID ������������ ������ 
--   - ��� ������ ���������� ����������
FUNCTION Revers_transfer(
               p_transfer_id   IN INTEGER,   -- ID ������������ �������� �������� �������
               p_pay_period_id IN INTEGER,   -- ID ������� �������
               p_notes         IN VARCHAR2   -- ����������
           ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Rollback_transfer';
    v_payment_id     INTEGER;
    v_bill_id        INTEGER;
    v_rep_period_id  INTEGER;
    v_transfer_total NUMBER := 0; -- ����� �������� �������� �������
    v_open_balance   NUMBER := 0; -- ����� �� ������� �� ���������� ��������
    v_close_balance  NUMBER := 0;
    v_bill_due       NUMBER := 0;
    v_transfer_id    INTEGER;
BEGIN
    -- ���������� ������ �� ������ ���� ������  
    IF Pk04_Period.Is_closed(p_pay_period_id) = TRUE THEN
        Pk01_Syslog.raise_user_Exception( '������ ����������� ��������� ����������� �������: '||
                                      p_pay_period_id, c_PkgName||'.'||v_prcName );
    END IF;  

    -- �������� ������ �������� ��������, ������� ����� ������������
    SELECT PT.BILL_ID, PT.REP_PERIOD_ID, PT.TRANSFER_TOTAL, PT.PAYMENT_ID
      INTO v_bill_id, v_rep_period_id, v_transfer_total, v_payment_id
      FROM PAY_TRANSFER_T PT
     WHERE PT.TRANSFER_ID   = p_transfer_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id;
       
    v_transfer_id := Transfer_to_bill(
               p_payment_id    => v_payment_id,    -- ID ������� - ��������� �������
               p_pay_period_id => p_pay_period_id, -- ID ��������� ������� ���� ����������� ������
               p_bill_id       => v_bill_id,       -- ID ������������� �����
               p_rep_period_id => v_rep_period_id, -- ID ��������� ������� �����               
               p_notes         => p_notes,         -- ���������� � ��������
               p_value         => -v_transfer_total,-- ����� ������� ����� ������������
               p_open_balance  => v_open_balance,  -- ����� �� ������� �� ���������� ��������
               p_close_balance => v_close_balance, -- ����� �� ������� ����� ���������� ��������
               p_bill_due      => v_bill_due       -- ���������� ���� �� ����� ����� ��������
           );

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
               p_pay_period_id IN INTEGER,  -- ID ��������� ������� �������
               p_account_id    IN INTEGER DEFAULT NULL  -- ������� ����, ����� �������� ����������
           ) RETURN NUMBER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Transfer_to_account_fifo';
    v_transfer_id   INTEGER;
    v_value         NUMBER := 0; -- ����� ������� ����� ���������, NULL - ������� �����               
    v_open_balance  NUMBER := 0; -- ����� �� ������� �� ���������� ��������
    v_close_balance NUMBER := 0; -- ����� �� ������� ����� ���������� ��������
    v_bill_due      NUMBER := 0; -- ���������� ���� �� ����� ����� ��������
    v_account_id    INTEGER;
BEGIN
    -- �������� ���������� � ������� �� ������ ��������
    SELECT P.BALANCE, P.ACCOUNT_ID 
      INTO v_value, v_account_id
      FROM PAYMENT_T P
     WHERE P.PAYMENT_ID    = p_payment_id
       AND P.REP_PERIOD_ID = p_pay_period_id;

    -- �������� ������ (FIFO) ������������, �� ������������ ������
    FOR r_bill IN ( 
        SELECT BILL_ID, REP_PERIOD_ID, DUE 
          FROM BILL_T
         WHERE ACCOUNT_ID = v_account_id
           AND TOTAL > 0         -- �������� ������ � ������� �������
           AND DUE   < 0         -- ���� ������������ �������������
           AND BILL_STATUS IN (PK00_CONST.c_BILL_STATE_CLOSED, PK00_CONST.c_BILL_STATE_READY)
         ORDER BY BILL_DATE )
    LOOP
       -- ����������� ����� ��������
       IF (v_value + r_bill.due) > 0 THEN  -- ������� ������� �� �������� ����� �� �����
           v_value := -r_bill.due;
       END IF; 
       -- �������� ������ �� ������������ ����� � ������� �� �����������
       v_transfer_id := Transfer_to_bill(
               p_payment_id    => p_payment_id,   -- ID ������� - ��������� �������
               p_pay_period_id => p_pay_period_id,-- ID ��������� ������� ���� ����������� ������
               p_bill_id       => r_bill.bill_id, -- ID ������������� �����
               p_rep_period_id => r_bill.rep_period_id, -- ID ��������� ������� �����
               p_notes         => NULL,           -- ���������� � ��������
               p_value         => v_value,        -- ����� ������� ����� ���������, NULL - ������� �����
               p_open_balance  => v_open_balance, -- ����� �� ������� �� ���������� ��������
               p_close_balance => v_close_balance,-- ����� �� ������� ����� ���������� ��������
               p_bill_due      => v_bill_due      -- ���������� ���� �� ����� ����� ��������
           );
       EXIT WHEN v_close_balance <= 0;            -- ������ ��� ���������
       -- ��������� � ���������� �����
       v_value := v_close_balance;
       --
    END LOOP; 
    RETURN v_close_balance;
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
BEGIN
    MERGE INTO PAYMENT_T P
    USING (
        SELECT REP_PERIOD_ID, PAYMENT_ID, SUM(TRANSFER_TOTAL) TRANSFER_TOTAL 
          FROM PAY_TRANSFER_T
         WHERE REP_PERIOD_ID <= PAY_PERIOD_ID
           AND PAY_PERIOD_ID = p_pay_period_id
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
          FROM ORDER_T O, ORDER_PHONES_T R, account_t a
         WHERE R.ORDER_ID = O.ORDER_ID
           AND R.PHONE_NUMBER = p_phone
           AND R.DATE_FROM <= p_date
           AND (R.DATE_TO IS NULL OR p_date < R.DATE_TO)
    		   AND A.ACCOUNT_ID   = O.ACCOUNT_ID 
    		   AND A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_P -- 'P'
    		   AND A.BILLING_ID   = pk00_const.c_BILLING_MMTS;
        RETURN v_account_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- ���� �� ����� � ��������, ���� ��������� ��������
            SELECT ACCOUNT_ID INTO v_account_id
            FROM (
                SELECT R.PHONE_NUMBER, O.ACCOUNT_ID, R.DATE_FROM, R.DATE_TO, 
                       MAX(R.DATE_TO) OVER (PARTITION BY R.PHONE_NUMBER) MAX_DATE_TO
                  FROM ORDER_T O, ORDER_PHONES_T R, ACCOUNT_T a
                WHERE R.ORDER_ID = O.ORDER_ID
                	AND R.PHONE_NUMBER = p_phone
                  AND A.ACCOUNT_ID   = O.ACCOUNT_ID 
                  AND A.Account_Type = Pk00_Const.c_ACC_TYPE_P
                  AND A.BILLING_ID   = Pk00_Const.c_BILLING_MMTS
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
    SELECT b.ACCOUNT_ID INTO v_account_id
      FROM BILL_T b, account_t a
     WHERE BILL_NO      = p_bill_no
   	 	 AND A.ACCOUNT_ID = B.ACCOUNT_ID
       AND A.BILLING_ID = Pk00_Const.c_BILLING_MMTS;
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
--   - ��� ������ ���������� ����������
PROCEDURE Account_payment_list (
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID �������� �����
               p_date_from  IN DATE,
               p_date_to    IN DATE 
           )
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
--   - ��� ������ ���������� ����������
PROCEDURE Account_payment_list (
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID �������� �����
               p_period_id  IN INTEGER
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Account_payment_list';
    v_retcode       INTEGER;
BEGIN   
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
PROCEDURE Bill_pay_list (
               p_recordset    OUT t_refc, 
               p_bill_id       IN INTEGER,    -- ID �������
               p_rep_period_id IN INTEGER     -- ID ��������� ������� �����
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bill_pay_list';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
          SELECT B.BILL_ID, B.BILL_NO, B.REP_PERIOD_ID BILL_REP_PERIOD_ID, B.RECVD BILL_RECVD,
                 PT.TRANSFER_ID, PT.TRANSFER_TOTAL, PT.TRANSFER_DATE,
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
--   - ��� ������ ���������� ����������
PROCEDURE Bill_pay_list_by_account (
               p_recordset    OUT t_refc, 
               p_account_id   IN INTEGER,    -- ID �������� �����
               p_rep_period_id IN INTEGER     -- ID ��������� ������� �����
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bill_pay_list_by_account';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
          SELECT B.BILL_ID, B.BILL_NO,B.REP_PERIOD_ID BILL_REP_PERIOD_ID, B.RECVD,
                 PT.TRANSFER_ID, PT.TRANSFER_TOTAL, PT.TRANSFER_DATE,
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
PROCEDURE Transfer_list (
               p_recordset    OUT t_refc, 
               p_payment_id    IN INTEGER,   -- ID �������
               p_pay_period_id IN INTEGER    -- ID ��������� ������� �����
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Transfer_list';
    v_retcode    INTEGER;
BEGIN
    -- ������� ������:
    --   SELECT LEVEL LVL, T.TRANSFER_ID, T.PAY_PERIOD_ID 
    --     FROM PAY_TRANSFER_T T
    --    WHERE T.PAYMENT_ID    = r_payment.PAYMENT_ID
    --      AND T.PAY_PERIOD_ID = r_payment.REP_PERIOD_ID
    --   CONNECT BY PRIOR TRANSFER_ID = PREV_TRANSFER_ID 
    --   START WITH PREV_TRANSFER_ID IS NULL

    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
          SELECT B.BILL_ID, B.BILL_NO, B.BILL_DATE, B.RECVD,
                 PT.TRANSFER_ID, PT.TRANSFER_TOTAL, PT.TRANSFER_DATE,
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
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- �������� ������ ������ � ������� �������� �������
-- ����������:
--  ID - ��������� ������ ��������
--  NULL - ���� ������� ���
FUNCTION Get_transfer_head (
               p_payment_id    IN INTEGER,   -- ID ��������������� �������
               p_pay_period_id IN INTEGER    -- ID ��������� �������, ����� ��� ��������������� ������
           ) RETURN INTEGER
IS
    v_transfer_id INTEGER;
BEGIN
    /*
    SELECT PT.TRANSFER_ID INTO v_transfer_id
      FROM PAY_TRANSFER_T PT
     WHERE PT.PAYMENT_ID    = p_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND PT.PREV_TRANSFER_ID IS NULL;
    */
    RETURN v_transfer_id;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN NULL;
END;

-- ------------------------------------------------------------------------ --
-- �������� ��������� ������ � ������� �������� �������
-- ����������:
--  ID   - ��������� ������ ��������
--  NULL - ���� ������� ���
FUNCTION Get_transfer_tail (
               p_payment_id    IN INTEGER,   -- ID ��������������� �������
               p_pay_period_id IN INTEGER    -- ID ��������� �������, ����� ��� ��������������� ������
           ) RETURN INTEGER
IS
    v_transfer_id INTEGER;
BEGIN
    /*
    SELECT PT.TRANSFER_ID INTO v_transfer_id
      FROM PAY_TRANSFER_T PT
     WHERE PT.PAYMENT_ID    = p_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND PT.PREV_TRANSFER_ID IS NULL;
    */
    RETURN v_transfer_id;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN NULL;
END;

-- ------------------------------------------------------------------------ --
-- ������� ������ ������� ������� ��������� ������
--
PROCEDURE Payment_bound_time (
               p_payment_id    IN INTEGER,   -- ID ��������������� �������
               p_pay_period_id IN INTEGER    -- ID ��������� �������, ����� ��� ��������������� ������
           )
IS
BEGIN
    MERGE INTO PAYMENT_T P
    USING (
      SELECT PT.PAYMENT_ID, PT.PAY_PERIOD_ID, 
             MIN(B.BILL_DATE) DATE_FROM , MAX(B.BILL_DATE) DATE_TO
        FROM PAY_TRANSFER_T PT, BILL_T B
       WHERE PT.PAYMENT_ID    = p_payment_id
         AND PT.PAY_PERIOD_ID = p_pay_period_id
         AND PT.BILL_ID       = B.BILL_ID
         AND PT.REP_PERIOD_ID = B.REP_PERIOD_ID
       GROUP BY PT.PAYMENT_ID, PT.PAY_PERIOD_ID
    ) T
    ON (P.PAYMENT_ID = T.PAYMENT_ID AND P.REP_PERIOD_ID = T.PAY_PERIOD_ID )
    WHEN MATCHED THEN UPDATE SET P.DATE_FROM = T.DATE_FROM, P.DATE_TO = T.DATE_TO;
END;


END PK10_PAYMENT;
/
