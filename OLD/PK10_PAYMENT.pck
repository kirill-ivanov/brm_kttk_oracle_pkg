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
    
	/* *******************************************************************
	* ������� ������
	*/
	function Remove_Payment(
		p_payment_id		in number,
		p_period_id			in number
		) return varchar2;

	/* ******************************************************************
	* ��������� ������ �������� �� �������� �� ������ � �����
	*/
	procedure Align_Payments_Period(
		p_period		in date default trunc(sysdate,'mm')
		);

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
			-- ---------------------------------------------------------------------
			-- id �� ��� �������������
			v_paysystem_correct NUMBER := 12;


/* *******************************************************************
* ������� ������
*/
function Remove_Payment(
	p_payment_id		in number,
	p_period_id			in number
	) return varchar2 is
	v_cnt				number;
	v_account_id		number;
begin
	-- ���. ����
	select account_id into v_account_id
	from payment_t 
	where payment_id = p_payment_id and rep_period_id = p_period_id;
	-- ��������� ������������� �� �����
	select count(1) into v_cnt
	from pay_transfer_t p 
	where p.payment_id = p_payment_id and p.pay_period_id=p_period_id;
	if v_cnt > 0 then
		return '������ ����������� �� �����';
	end if;
	-- ��������� ������
	begin
		select 1 into v_cnt 
		from period_t p
		where p.period_id = p_period_id and 
			p.position in(pk00_const.c_PERIOD_OPEN, pk00_const.c_PERIOD_NEXT, pk00_const.c_PERIOD_BILL);
	exception when no_data_found then
		return '������ ������� �� ������';
	end;
	-- �������
	delete from payment_t where payment_id = p_payment_id and rep_period_id = p_period_id;
	-- ����������� ������ �������� �����
	v_cnt := PK05_ACCOUNT_BALANCE.Refresh_balance(v_account_id);
	return 'OK';
exception when others then
	return sqlerrm;
end;

/* ******************************************************************
* ��������� ������ �������� �� �������� �� ������ � �����
*/
procedure Align_Payments_Period(
	p_period		in date default trunc(sysdate,'mm')
	) is
	v_period				date := trunc(p_period, 'mm');
	v_cnt					number;
	v_nb_period				number := to_number(to_char(v_period, 'yyyymm'));
	v_nb_last_period		number;
	v_prcName				varchar2(100) := 'PK_NB_PAYMENT.ALIGN_NB_PAYMENTS_PERIOD';
begin
	-- ��������� ��� ��� �� �������� ������
	select period_id into v_nb_last_period
	from period_t
	where position = 'LAST';
	-- ���������� ���������� ������ � ��������
	update payment_t
	set rep_period_id = v_nb_period
	where 1=1 -- rep_period_id > v_nb_last_period
		and rep_period_id != v_nb_period
		and exists(
			select 1
			from payment_gate.ps_registry_payments@mmtdb p
			where calc_date = v_period
				and p.receipt = doc_id and p.ps_id = paysystem_id
				and reg_id = 0
				and p.b_status = 1);

	pk01_syslog.Write_msg(p_Msg => 'eps:' || sql%rowcount, p_src=>v_prcName);

	update payment_t u
	set rep_period_id = v_nb_period
	where 1=1 --rep_period_id > v_nb_last_period
		and rep_period_id != v_nb_period
		and exists(
		select 1
			from payment_gate.ps_sbrf_registry@mmtdb p
			where calc_date = v_period
				and p.payment_id = u.doc_id and 11 = u.paysystem_id
				);
	pk01_syslog.Write_msg(p_Msg => 'sbrf:' || sql%rowcount, p_src=>v_prcName);
	commit;
exception when others then
	rollback;
	dbms_output.put_line('Error. ' || sqlerrm);
	Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName);
	rollback;
end;


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
							p_descr						IN VARCHAR2	DEFAULT NULL	-- �������� �������
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_payment';
    v_payment_id  INTEGER;
	  v_bill_id		  INTEGER;
BEGIN
	  -- ��������� ������� �������� �����
    SELECT a.billing_id
      INTO v_bill_id
      FROM ACCOUNT_T a
     WHERE a.account_id = p_account_id;
     
    IF v_bill_id != 2003 THEN
      Pk01_Syslog.raise_Exception('ERROR billing code', c_PkgName||'.'||v_prcName );
    END IF;
    
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
    
    -- ������� �������� �������� ������� �� ���� � ��������� �� � ����� ������� ��������
    v_transfer_id := Pk02_Poid.Next_transfer_id;
    --    
    p_close_balance := p_open_balance - p_value;
    --
    INSERT INTO PAY_TRANSFER_T (
           TRANSFER_ID, 
           PAYMENT_ID, PAY_PERIOD_ID,
           BILL_ID, REP_PERIOD_ID,
           TRANSFER_TOTAL, OPEN_BALANCE, CLOSE_BALANCE,
           TRANSFER_DATE, NOTES, PREV_TRANSFER_ID
    )
    SELECT v_transfer_id, 
           p_payment_id, p_pay_period_id,
           p_bill_id, p_rep_period_id, 
           p_value, p_open_balance, p_close_balance,
           SYSDATE, p_notes, 
           MAX(TRANSFER_ID) -- ����� ���������� NO_DATA_FOUND �� ���������
      FROM PAY_TRANSFER_T PT
     WHERE PT.PAYMENT_ID    = p_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND NOT EXISTS (
           SELECT *
            FROM PAY_TRANSFER_T T
           WHERE T.PAYMENT_ID       = PT.PAYMENT_ID
             AND T.PAY_PERIOD_ID    = PT.PAY_PERIOD_ID
             AND T.PREV_TRANSFER_ID = PT.TRANSFER_ID
       )
    ;
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
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Rollback_payment';
    v_paysystem_id   INTEGER;
    v_payment_date   DATE;
    v_doc_id         INTEGER;
    v_total          NUMBER;
    
BEGIN
    -- ������ ������ ���� �� ��������� ����������� ������� 
    IF Pk04_Period.Is_closed(p_pay_period_id) = TRUE THEN
        Pk01_Syslog.raise_Exception( '������ '||p_payment_id||' - ����������� ��������� ����������� �������: '||
                                      p_pay_period_id, c_PkgName||'.'||v_prcName );  
    END IF;

    -- ������� ��� �������� �������� �������
    PK10_PAYMENTS_TRANSFER.Delete_transfer_chain(p_pay_period_id ,p_payment_id);

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
    SELECT PT.BILL_ID, PT.REP_PERIOD_ID, PT.OPEN_BALANCE, PT.TRANSFER_TOTAL, PT.PAYMENT_ID
      INTO v_bill_id, v_rep_period_id, v_open_balance, v_transfer_total, v_payment_id
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
    SELECT P.BALANCE, P.ACCOUNT_ID INTO v_value, v_account_id
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
		   and a.account_id=o.account_id 
		   and a.account_type = 'P'
		   and a.billing_id = pk00_const.c_BILLING_MMTS;
        RETURN v_account_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- ���� �� ����� � ��������, ���� ��������� ��������
            SELECT ACCOUNT_ID INTO v_account_id
            FROM (
                SELECT R.PHONE_NUMBER, O.ACCOUNT_ID, R.DATE_FROM, R.DATE_TO, 
                       MAX(R.DATE_TO) OVER (PARTITION BY R.PHONE_NUMBER) MAX_DATE_TO
                  FROM ORDER_T O, ORDER_PHONES_T R, account_t a
                WHERE R.ORDER_ID = O.ORDER_ID
                	AND R.PHONE_NUMBER = p_phone
				   	and a.account_id=o.account_id 
		   			and a.account_type = 'P'
					and a.billing_id = pk00_const.c_BILLING_MMTS
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
     WHERE BILL_NO = p_bill_no
	 	and a.account_id=b.account_id
		and a.billing_id = pk00_const.c_BILLING_MMTS;
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
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
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
    SELECT PT.TRANSFER_ID INTO v_transfer_id
      FROM PAY_TRANSFER_T PT
     WHERE PT.PAYMENT_ID    = p_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND NOT EXISTS (
           SELECT *
            FROM PAY_TRANSFER_T T
           WHERE T.PAYMENT_ID       = PT.PAYMENT_ID
             AND T.PAY_PERIOD_ID    = PT.PAY_PERIOD_ID
             AND T.PREV_TRANSFER_ID = PT.TRANSFER_ID
       )
    ;
    RETURN v_transfer_id;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN NULL;
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
    SELECT PT.TRANSFER_ID INTO v_transfer_id
      FROM PAY_TRANSFER_T PT
     WHERE PT.PAYMENT_ID    = p_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND PT.PREV_TRANSFER_ID IS NULL;
    RETURN v_transfer_id;
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
