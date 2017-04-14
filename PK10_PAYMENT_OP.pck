CREATE OR REPLACE PACKAGE PK10_PAYMENT_OP
IS
    --
    -- ����� ��� ������ � �������� "������", �������:
    -- payment_t, pay_transfer_t
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_PAYMENT_OP';
    -- ==============================================================================
   
    type t_refc is ref cursor;

    -- ---------------------------------------------------------------------
    -- id �� (��������� �������) ��� ���������� �������������
    c_PAYSYSTEM_CORRECT_ID INTEGER := PK00_CONST.c_PAYSYSTEM_CORRECT_ID; -- 12;
    c_PAYSYSTEM_CORRECT_CODE VARCHAR2(30) := PK00_CONST.c_PAYSYSTEM_CORRECT_CODE; -- '�������������';

    -- ========================================================================== --
    -- �������� ��� ���������
    -- ========================================================================== --
    -- ------------------------------------------------------------------------ --
    -- ������� ������
    -- ------------------------------------------------------------------------ --
    -- ��� ������� ��� ������ � ������� �������� ������ ��� �� ������
    -- (���� ������ ������, �� ��� ����������� ����� ��� ����������)
    --   - ��� ������ ���������� ����������
    PROCEDURE Delete_payment(
                   p_payment_id    IN INTEGER,   -- ������
                   p_pay_period_id IN INTEGER,   -- ID ������� �������
                   p_app_user      IN VARCHAR2   -- ������������ ����������
               );

    
	-- ------------------------------------------------------------------------ --
	-- �������� ������������� ������� ��� ������������� �������
	-- ------------------------------------------------------------------------ --
	-- ������������ ������ �� �/� ������� ��� ������������� �������(����� ����� ����������� � ������� �/�)
	--   - ������������� - ID ���������� ��������� (PAYMENT.PAYMENT_ID) � ��������, 
	--   - ��� ������ ���������� ����������
	--
	FUNCTION Revers_payment_Ajust_Balance (
				   p_src_payment_id IN INTEGER,   -- ID ������������� �������                        
				   p_src_period_id  IN INTEGER,   -- ID ��������� �������, ����� ��� ��������������� ������
				   p_dst_period_id  IN INTEGER,   -- ID ��������� �������, ������������� �������
				   p_manager        IN VARCHAR2,  -- �������� ����������� ��������
				   p_notes          IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
			   ) RETURN INTEGER;

	-- ------------------------------------------------------------------------ --
    -- �������� ������������� �������
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
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- �������� �������� ����� � �������
    -- ------------------------------------------------------------------------ --
    --   - ������������� - ID ���������� ��������� (PAYMENT.PAYMENT_ID) � ��������, 
    --   - ��� ������ ���������� ����������
    FUNCTION Refund (
                   p_src_payment_id IN INTEGER,   -- ID ��������������� �������
                   p_src_period_id  IN INTEGER,   -- ID ��������� �������, ����� ��� ��������������� ������
                   p_dst_period_id  IN INTEGER,   -- ID ��������� �������, ��������������� �������
                   p_value          IN NUMBER,    -- ���������� ����� ��������
                   p_date           IN DATE,      -- ���� �������� �������
                   p_manager        IN VARCHAR2,  -- �������� ����������� ��������
                   p_notes          IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
               ) RETURN INTEGER;

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
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- ������ �������� �� �������� �����
    PROCEDURE Operation_list (
                   p_recordset OUT t_refc, 
                   p_date_from  IN DATE,
                   p_date_to    IN DATE 
               );


END PK10_PAYMENT_OP;
/
CREATE OR REPLACE PACKAGE BODY PK10_PAYMENT_OP
IS

-- ========================================================================== --
-- �������� ��� ���������
-- ========================================================================== --

-- ------------------------------------------------------------------------ --
-- ������� ������, 
-- ��� ������� ��� ������ � ������� �������� ������ ��� �� ������
-- (���� ������ ������, �� ��� ����������� ����� ��� ����������)
--   - ��� ������ ���������� ����������
PROCEDURE Delete_payment(
               p_payment_id    IN INTEGER,   -- ������
               p_pay_period_id IN INTEGER,   -- ID ������� �������
               p_app_user      IN VARCHAR2   -- ������������ ����������
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Delete_payment';
    v_paysystem_id   INTEGER;
    v_payment_date   DATE;
    v_doc_id         VARCHAR2(100);
    v_total          NUMBER;
    v_account_id	   INTEGER;
    v_bal			       NUMBER;
    v_bill_id		     INTEGER;
BEGIN
    -- �������� billing_id
    SELECT billing_id 
      INTO v_bill_id
      FROM payment_t p, account_t a
     WHERE p.account_id    = a.account_id 
       AND p.rep_period_id = p_pay_period_id 
       AND p.payment_id    = p_payment_id;
  
    -- ������ ������ ���� �� ��������� ����������� ������� 
    IF Pk04_Period.Is_closed(p_pay_period_id) = TRUE and v_bill_id = 2003 THEN
        Pk01_Syslog.raise_Exception( '������ '||p_payment_id||' - ����������� ��������� ����������� �������: '||
                                      p_pay_period_id, c_PkgName||'.'||v_prcName );  
    END IF;

    -- ������� ��� �������� �������� �������
    PK10_PAYMENTS_TRANSFER.Delete_transfer_chain(p_pay_period_id ,p_payment_id);

    -- �������� ������ �� ��������� ������� ��� ������   
    SELECT P.PAYSYSTEM_ID, PAYMENT_DATE, DOC_ID, RECVD, p.account_id
      INTO v_paysystem_id, v_payment_date, v_doc_id, v_total, v_account_id
      FROM PAYMENT_T P
     WHERE PAYMENT_ID = p_payment_id
       AND REP_PERIOD_ID = p_pay_period_id;
    
    -- ������� �������� � ��������� (������ �.�.)
    DELETE FROM PAYMENT_OPERATION_T P
     WHERE p_payment_id in ( P.SRC_PAYMENT_ID, P.DST_PAYMENT_ID );
  
    -- ������� ������
    DELETE FROM PAYMENT_T 
     WHERE PAYMENT_ID = p_payment_id
       AND REP_PERIOD_ID = p_pay_period_id;
	 
    -- ����������� ������
    v_bal := pk05_account_balance.Refresh_balance(p_account_id => v_account_id);
    
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

-- ------------------------------------------------------------------------ --
-- �������� ������������� ������� ��� ������������� �������
-- ------------------------------------------------------------------------ --
-- ������������ ������ �� �/� ������� ��� ������������� �������(����� ����� ����������� � ������� �/�)
--   - ������������� - ID ���������� ��������� (PAYMENT.PAYMENT_ID) � ��������, 
--   - ��� ������ ���������� ����������
--
FUNCTION Revers_payment_Ajust_Balance (
               p_src_payment_id IN INTEGER,   -- ID ������������� �������                        
               p_src_period_id  IN INTEGER,   -- ID ��������� �������, ����� ��� ��������������� ������
               p_dst_period_id  IN INTEGER,   -- ID ��������� �������, ������������� �������
               p_manager        IN VARCHAR2,  -- �������� ����������� ��������
               p_notes          IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
           ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Revers_payment_Ajust_Balance';
    r_payment        PAYMENT_T%ROWTYPE;
    v_dst_payment_id INTEGER;
		v_dst_doc_id		 VARCHAR2(100);
    v_advance        NUMBER := 0; -- payment_t.advance - �������
    v_payment_id     INTEGER;
BEGIN
    v_dst_payment_id := PK02_POID.Next_payment_id;
    -- ������ ������ ������������� �������
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id
       AND REFUND = 0  -- ��� ���� ������������ ������
       AND STATUS != Pk00_Const.c_PAY_STATE_REVERS;
    --
    -- �������� ����� ���������� ��� ������������� �������
		v_dst_doc_id := pk02_poid.Next_Payment_Doc_Id();
    --    
    -- ��������� ������������ ������ � ������� ������� �� �������� "������"
    INSERT INTO PAYMENT_T (
        PAYMENT_ID, REP_PERIOD_ID, ACCOUNT_ID, 
        RECVD, ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED, 
        DATE_FROM, DATE_TO, PAYMENT_DATE, 
        PAYMENT_TYPE, PAYSYSTEM_ID, 
        DOC_ID, STATUS, STATUS_DATE, 
        CREATE_DATE, CREATED_BY, LAST_MODIFIED, 
        PAYSYSTEM_CODE, PAY_DESCR, 
        PREV_PAYMENT_ID, PREV_PERIOD_ID, 
        REFUND, MODIFIED_BY, NOTES, EXTERNAL_ID
    )VALUES(
        v_dst_payment_id, p_dst_period_id, r_payment.ACCOUNT_ID,
        -r_payment.RECVD, 0, SYSDATE, 0, 0,
        r_payment.DATE_FROM, r_payment.DATE_TO, r_payment.PAYMENT_DATE,
        PK00_CONST.c_PAY_TYPE_ADJUST_BALANCE, c_PAYSYSTEM_CORRECT_ID,
        v_dst_doc_id, PK00_CONST.c_PAY_STATE_CLOSE, SYSDATE,
        SYSDATE, p_manager, SYSDATE, 
        c_PAYSYSTEM_CORRECT_CODE, r_payment.PAY_DESCR,
        p_src_payment_id, p_src_period_id, 
        -r_payment.RECVD, NULL, nvl(p_notes, r_payment.NOTES), r_payment.EXTERNAL_ID
    );

    -- ���������� ������� �������� �������
    PK10_PAYMENTS_TRANSFER.Revers_transfer_chain (
             p_period_id  => p_src_period_id,
             p_payment_id => p_src_payment_id
         );
      
    -- �������� ��������� ������������� �������
    UPDATE PAYMENT_T
       SET BALANCE = 0,
           REFUND  = RECVD,
           STATUS  = Pk00_Const.c_PAY_STATE_REVERS
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id;    

    -- ���������� ��������� ������� �������� ����� �� �������� �������
    UPDATE ACCOUNT_T
       SET BALANCE = BALANCE - r_payment.RECVD,
           BALANCE_DATE = SYSDATE  
     WHERE ACCOUNT_ID = r_payment.ACCOUNT_ID;

    -- ��������� �������� ������������� �������
    INSERT INTO PAYMENT_OPERATION_T O (
        O.OPER_ID, O.OPER_TYPE_ID, O.OPER_DATE, O.OPER_TOTAL, 
        O.SRC_PAYMENT_ID, O.SRC_REP_PERIOD_ID,
        O.DST_PAYMENT_ID, O.DST_REP_PERIOD_ID,
        O.CREATED_BY, O.NOTES )
    VALUES(
        Pk02_Poid.Next_transfer_id, Pk00_Const.c_PAY_OP_REVERS, SYSDATE, r_payment.RECVD,
        p_src_payment_id, p_src_period_id, v_dst_payment_id, p_dst_period_id,
        p_manager, p_notes
    );
    --
    RETURN v_dst_payment_id;
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.Payment_id='||p_src_payment_id, c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- �������� ������������� �������
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
           ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Revers_transfer';
    r_payment        PAYMENT_T%ROWTYPE;
    v_dst_payment_id INTEGER;
		v_dst_doc_id		 VARCHAR2(100);
    v_advance        NUMBER := 0; -- payment_t.advance - �������
    v_payment_id     INTEGER;
BEGIN
    v_dst_payment_id := PK02_POID.Next_payment_id;
    -- ������ ������ ������������� �������
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id
       AND REFUND = 0  -- ��� ���� ������������ ������
       AND STATUS != Pk00_Const.c_PAY_STATE_REVERS;
    -- �������� ����� ���������� ��� ������������� �������
		v_dst_doc_id := pk02_poid.Next_Payment_Doc_Id();
    --    
    -- �������� ����� ���������� ��� ������������� �������
    v_dst_doc_id := pk02_poid.Next_Payment_Doc_Id();
    --v_payment_id := PK02_POID.Next_payment_id;
    --
    -- ��������� ������������ ������ � ������� ������� �� �������� "������"
    INSERT INTO PAYMENT_T (
        PAYMENT_ID, REP_PERIOD_ID, ACCOUNT_ID, 
        RECVD, ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED, 
        DATE_FROM, DATE_TO, PAYMENT_DATE, 
        PAYMENT_TYPE, PAYSYSTEM_ID, 
        DOC_ID, STATUS, STATUS_DATE, 
        CREATE_DATE, CREATED_BY, LAST_MODIFIED, 
        PAYSYSTEM_CODE, PAY_DESCR, 
        PREV_PAYMENT_ID, PREV_PERIOD_ID, 
        REFUND, MODIFIED_BY, NOTES, EXTERNAL_ID
    )VALUES(
        v_dst_payment_id/*v_payment_id*/, p_dst_period_id, r_payment.ACCOUNT_ID,
        -r_payment.RECVD, 0, SYSDATE, 0, 0,
        r_payment.DATE_FROM, r_payment.DATE_TO, r_payment.PAYMENT_DATE,
        PK00_CONST.c_PAY_TYPE_REVERS, c_PAYSYSTEM_CORRECT_ID,
        v_dst_doc_id, PK00_CONST.c_PAY_STATE_CLOSE, SYSDATE,
        SYSDATE, p_manager, SYSDATE, 
        c_PAYSYSTEM_CORRECT_CODE, r_payment.PAY_DESCR,
        p_src_payment_id, p_src_period_id, 
        -r_payment.RECVD, NULL, nvl(p_notes, r_payment.NOTES), r_payment.EXTERNAL_ID
    );

    -- ���������� ������� �������� �������
    PK10_PAYMENTS_TRANSFER.Revers_transfer_chain (
             p_period_id  => p_src_period_id,
             p_payment_id => p_src_payment_id
         );
      
    -- �������� ��������� ������������� �������
    UPDATE PAYMENT_T
       SET BALANCE = 0,
           REFUND  = RECVD,
           STATUS  = Pk00_Const.c_PAY_STATE_REVERS
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id;    

    -- ���������� ��������� ������� �������� ����� �� �������� �������
    UPDATE ACCOUNT_T
       SET BALANCE = BALANCE - r_payment.RECVD,
           BALANCE_DATE = SYSDATE  
     WHERE ACCOUNT_ID = r_payment.ACCOUNT_ID;

    -- ��������� �������� ������������� �������
    INSERT INTO PAYMENT_OPERATION_T O (
        O.OPER_ID, O.OPER_TYPE_ID, O.OPER_DATE, O.OPER_TOTAL, 
        O.SRC_PAYMENT_ID, O.SRC_REP_PERIOD_ID,
        O.DST_PAYMENT_ID, O.DST_REP_PERIOD_ID,
        O.CREATED_BY, O.NOTES )
    VALUES(
        Pk02_Poid.Next_transfer_id, Pk00_Const.c_PAY_OP_REVERS, SYSDATE, r_payment.RECVD,
        p_src_payment_id, p_src_period_id, v_dst_payment_id, p_dst_period_id,
        p_manager, p_notes
    );
    --
    RETURN v_dst_payment_id;
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.Payment_id='||p_src_payment_id, c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- �������� �������� ����� � �������
-- ------------------------------------------------------------------------ --
--   - ������������� - ID ���������� ��������� (PAYMENT.PAYMENT_ID) � ��������, 
--   - ��� ������ ���������� ����������
FUNCTION Refund (
               p_src_payment_id IN INTEGER,   -- ID ��������������� �������
               p_src_period_id  IN INTEGER,   -- ID ��������� �������, ����� ��� ��������������� ������
               p_dst_period_id  IN INTEGER,   -- ID ��������� �������, ��������������� �������
               p_value          IN NUMBER,    -- ���������� ����� ��������
               p_date           IN DATE,      -- ���� �������� �������
               p_manager        IN VARCHAR2,  -- �������� ����������� ��������
               p_notes          IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
           ) RETURN INTEGER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Refund';
    r_payment      PAYMENT_T%ROWTYPE;
    v_payment_id   INTEGER;
		v_doc_id			 PAYMENT_T.DOC_ID%TYPE;
BEGIN
 		v_doc_id := PK02_POID.Next_Payment_Doc_Id(c_PAYSYSTEM_CORRECT_ID);  

    -- ������ ������ ��������������� �������
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id;

    -- ��������� ���������� �� �������   
    IF r_payment.Balance < p_value THEN
        Pk01_Syslog.raise_Exception('�� ���������� ������� ��� ��������. '||
                                    '������� �� ������� '||r_payment.Balance||' ���, '||
                                    '������ �� ������� '||p_value||' ���, '||
                                    'PAYMENT_ID='||p_src_payment_id
                                    , c_PkgName||'.'||v_prcName );
    END IF;
    
    -- ��������� ������� ����� � ������� ���������
    UPDATE PAYMENT_T P
       SET P.REFUND       = p_value,
           P.BALANCE      = P.BALANCE - p_value
     WHERE P.PAYMENT_ID   = p_src_payment_id 
       AND P.REP_PERIOD_ID= p_src_period_id;
    
    -- ��������� ������������� ������ �������� 
    v_payment_id := PK02_POID.Next_payment_id;
    
    -- ��������� �������� ������������ ������ � ������� ������� �� �������� "������"
    INSERT INTO PAYMENT_T (
        PAYMENT_ID, REP_PERIOD_ID, ACCOUNT_ID, 
        RECVD, ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED, 
        DATE_FROM, DATE_TO, PAYMENT_DATE, 
        PAYMENT_TYPE, PAYSYSTEM_ID, 
        DOC_ID, STATUS, STATUS_DATE, 
        CREATE_DATE, CREATED_BY, LAST_MODIFIED, 
        PAYSYSTEM_CODE, PAY_DESCR, 
        PREV_PAYMENT_ID, PREV_PERIOD_ID, 
        REFUND, MODIFIED_BY, NOTES, EXTERNAL_ID
    )VALUES(
        v_payment_id, p_dst_period_id, r_payment.ACCOUNT_ID,
        -p_value, 0, SYSDATE, 0, 0,
        r_payment.DATE_FROM, r_payment.DATE_TO, r_payment.PAYMENT_DATE,
        Pk00_Const.c_PAY_TYPE_REFUND, c_PAYSYSTEM_CORRECT_ID,
        v_doc_id, PK00_CONST.c_PAY_STATE_CLOSE, SYSDATE,
        SYSDATE, p_manager, SYSDATE, 
        c_PAYSYSTEM_CORRECT_CODE, r_payment.PAY_DESCR,
        p_src_payment_id, p_src_period_id, 
        -p_value, NULL, nvl(p_notes, r_payment.NOTES), r_payment.EXTERNAL_ID
    );
    /*
    v_payment_id := PK10_PAYMENT.Add_payment (
              p_account_id      => r_payment.Account_Id,        -- ID �������� ����� �������
              p_rep_period_id   => p_dst_period_id,             -- ID ��������� ������� ���� ����������� ������
              p_payment_dat�    => p_date,                      -- ���� �������
              p_payment_type    => Pk00_Const.c_PAY_TYPE_REFUND,-- ��� ������� ������� �����
              p_recvd           => -p_value,                    -- ����� �������
              p_paysystem_id    => c_PAYSYSTEM_CORRECT_ID,      -- ID ��������� �������
              p_doc_id          => v_doc_id,                    -- ID ��������� � ��������� �������
              p_status          => Pk00_Const.c_PAY_STATE_OPEN, -- ������ �������
              p_manager    		  => p_manager,                   -- �.�.�. ��������� ��������������� ������ �� �/�
              p_notes           => p_notes                      -- ���������� � �������  
           );
    */           
    -- ��������� �������� �������� ������� � �������
    INSERT INTO PAYMENT_OPERATION_T O (
        O.OPER_ID, O.OPER_TYPE_ID, O.OPER_DATE, O.OPER_TOTAL, 
        O.SRC_PAYMENT_ID, O.SRC_REP_PERIOD_ID,
        O.DST_PAYMENT_ID, O.DST_REP_PERIOD_ID,
        O.CREATED_BY, O.NOTES )
    VALUES(
        Pk02_Poid.Next_transfer_id, Pk00_Const.c_PAY_OP_REFUND, SYSDATE, r_payment.RECVD,
        p_src_payment_id, p_src_period_id, v_payment_id, p_dst_period_id,
        p_manager, p_notes
    );
    --
    RETURN v_payment_id;  
    --
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
           ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Move_payment';
    r_payment        PAYMENT_T%ROWTYPE;
    v_rev_payment_id INTEGER;
    v_dst_payment_id INTEGER;
BEGIN
    -- ������ ������ ������������ �������
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id;

    -- ���������� ������ ��������
    v_rev_payment_id := Revers_payment (
               p_src_payment_id,  -- ID ������������� �������                        
               p_src_period_id,   -- ID ��������� �������, ����� ��� ��������������� ������
               p_dst_period_id,   -- ID ��������� �������, ������������� �������
               p_manager,         -- �������� ����������� ��������
               p_notes            -- ���������� � ��������
           );
           
    -- ��������� ������ � ��������� �������
    v_dst_payment_id := PK02_POID.Next_payment_id;
    
    -- ��������� �������� ������������ ������ � ������� ������� �� �������� "������"
    INSERT INTO PAYMENT_T (
        PAYMENT_ID, REP_PERIOD_ID, ACCOUNT_ID, 
        RECVD, ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED, 
        DATE_FROM, DATE_TO, PAYMENT_DATE, 
        PAYMENT_TYPE, PAYSYSTEM_ID, 
        DOC_ID, STATUS, STATUS_DATE, 
        CREATE_DATE, CREATED_BY, LAST_MODIFIED, 
        PAYSYSTEM_CODE, PAY_DESCR, 
        PREV_PAYMENT_ID, PREV_PERIOD_ID, 
        REFUND, MODIFIED_BY, NOTES, EXTERNAL_ID
    )VALUES(
        v_dst_payment_id, p_dst_period_id, p_dst_account_id,
        r_payment.Recvd, 0, SYSDATE, r_payment.Recvd, 0,
        NULL, NULL, r_payment.PAYMENT_DATE,
        Pk00_Const.c_PAY_TYPE_MOVE, c_PAYSYSTEM_CORRECT_ID,
        r_payment.Doc_Id, PK00_CONST.c_PAY_STATE_OPEN, SYSDATE,
        SYSDATE, p_manager, SYSDATE, 
        c_PAYSYSTEM_CORRECT_CODE, r_payment.PAY_DESCR,
        v_rev_payment_id, p_src_period_id, 
        0, NULL, nvl(p_notes, r_payment.NOTES), r_payment.EXTERNAL_ID
    );
    /*
    v_dst_payment_id := PK10_PAYMENT.Add_payment (
              p_account_id      => p_dst_account_id,           -- ID �������� ����� �������
              p_rep_period_id   => p_dst_period_id,            -- ID ��������� ������� ���� ����������� ������
              p_payment_dat�    => r_payment.Payment_Date,     -- ���� �������
              p_payment_type    => Pk00_Const.c_PAY_TYPE_MOVE, -- ��� �������
              p_recvd           => r_payment.Recvd,            -- ����� �������
              p_paysystem_id    => c_PAYSYSTEM_CORRECT_ID,     -- ID ��������� �������
              p_doc_id          => r_payment.Doc_Id,           -- ID ��������� � ��������� �������
              p_status          => Pk00_Const.c_PAY_STATE_OPEN,-- ������ �������
              p_manager    		  => p_manager,  -- �.�.�. ��������� ��������������� ������ �� �/�
              p_notes           => p_notes,      -- ���������� � �������  
              p_descr 			=> r_payment.pay_descr			
           );
    */           
    UPDATE PAYMENT_T P
       SET P.PREV_PAYMENT_ID = p_src_payment_id,
           P.PREV_PERIOD_ID  = p_src_period_id
     WHERE PAYMENT_ID   = v_dst_payment_id 
       AND REP_PERIOD_ID= p_dst_period_id;
           
    -- ��������� �������� ������������� �������
    INSERT INTO PAYMENT_OPERATION_T O (
        O.OPER_ID, O.OPER_TYPE_ID, O.OPER_DATE, O.OPER_TOTAL, 
        O.SRC_PAYMENT_ID, O.SRC_REP_PERIOD_ID,
        O.DST_PAYMENT_ID, O.DST_REP_PERIOD_ID,
        O.CREATED_BY, O.NOTES )
    VALUES(
        Pk02_Poid.Next_transfer_id, PK00_CONST.c_PAY_OP_MOVE, SYSDATE, r_payment.RECVD,
        p_src_payment_id, p_src_period_id, v_dst_payment_id, p_dst_period_id,
        p_manager, p_notes
    );
           
    RETURN v_dst_payment_id;  
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.Payment_id='||p_src_payment_id, c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- ������ �������� �� �������� �����
PROCEDURE Operation_list (
               p_recordset OUT t_refc, 
               p_date_from  IN DATE,
               p_date_to    IN DATE 
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Operation_list';
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
        SELECT O.OPER_ID, O.OPER_TYPE_ID, O.OPER_DATE, O.OPER_TOTAL, 
               O.SRC_PAYMENT_ID, O.SRC_REP_PERIOD_ID,
               O.DST_PAYMENT_ID, O.DST_REP_PERIOD_ID,
               O.CREATED_BY, O.NOTES
          FROM PAYMENT_OPERATION_T O    
         WHERE O.OPER_DATE BETWEEN v_date_from AND v_date_to
         ORDER BY O.OPER_ID DESC;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;  

END PK10_PAYMENT_OP;
/
