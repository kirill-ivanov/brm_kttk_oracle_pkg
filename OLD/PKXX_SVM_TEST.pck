CREATE OR REPLACE PACKAGE PKXX_SVM_TEST
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
    -- �������� ��������� �������� �������� �������, 
    -- ��� ������� ��� ������ � ������� �������� ������ ��� �� ������
    -- (���� ������ ������, �� ��� ����������� ����� ��� ����������)
    --   - ������� ������������� ������� �� ������� 
    --   - ��� ������ ���������� ����������
    FUNCTION Rollback_transfer(
                   p_transfer_id   IN INTEGER,   -- ID �������
                   p_pay_period_id IN INTEGER    -- ID ������� �������
               ) RETURN INTEGER;

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
                   p_pay_period_id IN INTEGER,  -- ID ��������� ������� �����
                   p_account_id    IN INTEGER   -- ������� ����, ����� �������� ����������
               ) RETURN NUMBER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- �������� �� �������������� ����� ��� ��� �������� �� �������� �������� 
    -- �� ���������� ������� ������������.
    -- �������� ������ �/� �� �������� ������ �/� �� ��������
    -- �������� ��� '�' �������������� FIFO, '�'-������ ����� ��� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Payment_processing_fifo( p_from_period_id IN INTEGER );

    -- ========================================================================== --
    -- �������� ��� ���������
    -- ========================================================================== --
    -- ------------------------------------------------------------------------ --
    -- �������� ������������� �������
    -- ------------------------------------------------------------------------ --
    -- ������������ ������ � �/� ������� (����� ����� ����������� � ������� �/�)
    --   - ������������� - ID ���������� ��������� (PAYMENT.PAYMENT_ID) � ��������, 
    --   - ��� ������ ���������� ����������
    --
    FUNCTION OP_revers_payment (
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
    FUNCTION OP_refund (
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
    FUNCTION OP_move_payment (
                   p_src_payment_id IN INTEGER,  -- ID ������� ���������
                   p_src_period_id  IN INTEGER,  -- ID ��������� ������� ���������
                   p_dst_account_id IN INTEGER,  -- ID ������� ���������
                   p_dst_period_id  IN INTEGER,  -- ID ��������� ������� ���������
                   p_manager        IN VARCHAR2, -- �������� ����������� ��������
                   p_notes          IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
               ) RETURN INTEGER;
               
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

    --=========================================================================
    PROCEDURE xTTK_to_Saler;

END PKXX_SVM_TEST;
/
CREATE OR REPLACE PACKAGE BODY PKXX_SVM_TEST
IS

-- ---------------------------------------------------------------------
-- id �� ��� �������������
v_paysystem_correct NUMBER := 12;

--============================================================================================
--                  � � � � � � � � �     � � � � � � � � � 
--============================================================================================
-- ������� ���������� �� �������
--
PROCEDURE Gather_Table_Stat(l_Tab_Name varchar2)
IS
    PRAGMA AUTONOMOUS_TRANSACTION; 
BEGIN 
    DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'PIN',
                                  TABNAME => l_Tab_Name,
                                  DEGREE  => 5,
                                  CASCADE => TRUE,
                                  NO_INVALIDATE => FALSE
                                 ); 
END;

--============================================================================================
PROCEDURE Run_DDL(p_ddl IN VARCHAR2) IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Run_DDL';

BEGIN
    EXECUTE IMMEDIATE p_ddl;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.Write_error('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  PAY_TRANSFER_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Pay_transfer_t_drop_fk
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Pay_transfer_t_drop_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Run_DDL('ALTER TABLE PIN.PAY_TRANSFER_T DROP CONSTRAINT PAY_TRANSFER_ID_BILL_T_FK');
    Run_DDL('ALTER TABLE PIN.PAY_TRANSFER_T DROP CONSTRAINT PAY_TRANSFER_ID_PAYMENT_T_FK');
    Run_DDL('ALTER TABLE PIN.PAY_TRANSFER_T DROP CONSTRAINT PAY_TRANSFER_T_FK');
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
PROCEDURE Pay_transfer_t_add_fk
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Pay_transfer_t_add_fk';
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    EXECUTE IMMEDIATE 'ALTER TABLE PIN.PAY_TRANSFER_T ADD (
      CONSTRAINT PAY_TRANSFER_ID_BILL_T_FK 
      FOREIGN KEY (BILL_ID, REP_PERIOD_ID) 
      REFERENCES PIN.BILL_T (BILL_ID,REP_PERIOD_ID)
      ENABLE VALIDATE,
      CONSTRAINT PAY_TRANSFER_ID_PAYMENT_T_FK 
      FOREIGN KEY (PAYMENT_ID, PAY_PERIOD_ID) 
      REFERENCES PIN.PAYMENT_T (PAYMENT_ID,REP_PERIOD_ID)
      ENABLE VALIDATE,
      CONSTRAINT PAY_TRANSFER_T_FK 
      FOREIGN KEY (PREV_TRANSFER_ID, PAY_PERIOD_ID) 
      REFERENCES PIN.PAY_TRANSFER_T (TRANSFER_ID,PAY_PERIOD_ID)
      ENABLE VALIDATE)';
    --  
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;



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
BEGIN
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
    IF p_value > 0 THEN   -- ��� ������������� ������ �������� ��� ��� ���������
        IF v_bill_date < v_date_from THEN
            v_date_from := v_bill_date;
        ELSIF v_date_to < v_bill_date THEN
            v_date_to := v_bill_date;
        END IF;
    END IF;
    /*
    -- ������ �������, �� ������� ��� �������� ��������    
    SELECT MIN(B.BILL_DATE), MAX(B.BILL_DATE)
      INTO v_date_from, v_date_to
      FROM PAY_TRANSFER_T PT, BILL_T B
     WHERE PT.PAYMENT_ID    = p_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND PT.BILL_ID       = B.BILL_ID
       AND PT.REP_PERIOD_ID = B.REP_PERIOD_ID;
    */
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
-- �������� ��������� �������� �������� �������, 
-- ��� ������� ��� ������ � ������� �������� ������ ��� �� ������
-- (���� ������ ������, �� ��� ����������� ����� ��� ����������)
--   - ID ���������� �������� �������� 
--   - ��� ������ ���������� ����������
FUNCTION Rollback_transfer(
               p_transfer_id   IN INTEGER,   -- ID �������
               p_pay_period_id IN INTEGER    -- ID ������� �������
           ) RETURN INTEGER
IS
    v_prcName          CONSTANT VARCHAR2(30) := 'Rollback_transfer';
    v_payment_id       INTEGER;
    v_bill_id          INTEGER;
    v_rep_period_id    INTEGER;
    v_open_balance     NUMBER := 0; -- ����� �� ������� �� ���������� ��������
    v_transfer_total   NUMBER := 0; -- ����� �������� �������� �������
    v_advance_back     NUMBER := 0; -- ������� �� ����� ��� ������ �������� �������� 
    v_count            INTEGER:= 0;
    v_date_from        DATE;
    v_date_to          DATE;
    v_prev_transfer_id INTEGER;
BEGIN
    -- ���������� ������ �� ������ ���� ������  
    IF Pk04_Period.Is_closed(p_pay_period_id) = TRUE THEN
        Pk01_Syslog.raise_Exception( '������ ����������� ��������� ����������� �������: '||
                                      p_pay_period_id, c_PkgName||'.'||v_prcName );
    END IF;
    
    -- ��������� �������� ������ ���� ��������� � ������� ��������
    SELECT COUNT(*)
      INTO v_count
      FROM PAY_TRANSFER_T
     WHERE PREV_TRANSFER_ID = p_transfer_id
       AND REP_PERIOD_ID = p_pay_period_id;
    IF v_count > 0 THEN
        Pk01_Syslog.raise_Exception( '�������� '||p_transfer_id||' - �� ��������� � ������� �������� �������'||
                                      p_pay_period_id, c_PkgName||'.'||v_prcName );
    END IF;
 
    -- �������� ������ �������� ��������
    SELECT PT.BILL_ID, PT.REP_PERIOD_ID, PT.PREV_TRANSFER_ID,
           PT.OPEN_BALANCE, PT.TRANSFER_TOTAL, PT.PAYMENT_ID
      INTO v_bill_id, v_rep_period_id, v_prev_transfer_id,
           v_open_balance, v_transfer_total, v_payment_id
      FROM PAY_TRANSFER_T PT
     WHERE PT.TRANSFER_ID = p_transfer_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id;

    -- �������� ������������� �� �����, ���� � �������
    UPDATE BILL_T B 
       SET DUE     = DUE     + v_transfer_total,
           RECVD   = RECVD   - v_transfer_total
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
     WHERE PT.PAYMENT_ID    = v_payment_id
       AND PT.PAY_PERIOD_ID = p_pay_period_id
       AND PT.BILL_ID       = B.BILL_ID
       AND PT.REP_PERIOD_ID = B.REP_PERIOD_ID;

    -- ����� ���������� ������ ��� �������� �������� �� ����� � ������� � ���������� ��������
    -- ��� �������� ���������� �������� 
    IF p_pay_period_id >= v_rep_period_id AND Pk04_Period.Is_closed(p_pay_period_id) = FALSE THEN
        v_advance_back := v_transfer_total;
    ELSE
        v_advance_back := 0;
    END IF;

    -- ���������� ������ �� ������
    UPDATE PAYMENT_T 
       SET BALANCE   = v_open_balance,
           TRANSFERED= TRANSFERED - v_transfer_total,
           ADVANCE   = ADVANCE + v_advance_back,
           DATE_FROM = v_date_from, 
           DATE_TO   = v_date_to,
           LAST_MODIFIED = SYSDATE
     WHERE PAYMENT_ID = v_payment_id
       AND REP_PERIOD_ID = p_pay_period_id;

    RETURN v_prev_transfer_id;
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
    v_value          INTEGER;
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
    FOR c_transfer IN (
            SELECT TRANSFER_ID
              FROM PAY_TRANSFER_T
             WHERE PAYMENT_ID = p_payment_id
               AND PAY_PERIOD_ID = p_pay_period_id
            ORDER BY TRANSFER_ID DESC 
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
        Pk01_Syslog.raise_Exception( '������ ����������� ��������� ����������� �������: '||
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
    -- �������� ���������� � ������� �� ������ ��������
    SELECT P.BALANCE INTO v_value
      FROM PAYMENT_T P
     WHERE P.PAYMENT_ID    = p_payment_id
       AND P.REP_PERIOD_ID = p_pay_period_id;

    -- �������� ������ (FIFO) ������������, �� ������������ ������
    FOR r_bill IN ( 
        SELECT BILL_ID, REP_PERIOD_ID, DUE 
          FROM BILL_T
         WHERE ACCOUNT_ID = p_account_id
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
    v_zero          INTEGER;
    v_count         INTEGER;
    v_transfer_id   INTEGER;
    v_value         NUMBER := 0; -- ����� ������� ����� ���������, NULL - ������� �����               
    v_open_balance  NUMBER := 0; -- ����� �� ������� �� ���������� ��������
    v_close_balance NUMBER := 0; -- ����� �� ������� ����� ���������� ��������
    v_bill_due      NUMBER := 0; -- ���������� ���� �� ����� ����� ��������
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, from period_id <= '||p_from_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_ok  := 0;    
    v_err := 0;
    v_zero:= 0;
    
    -- ����������� ������
    UPDATE PAYMENT_T P SET REP_PERIOD_ID = SUBSTR(REP_PERIOD_ID,1,6);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAYMENT_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    
    Gather_Table_Stat(l_Tab_Name => 'BILL_T');
    Gather_Table_Stat(l_Tab_Name => 'PAYMENT_T');
    Gather_Table_Stat(l_Tab_Name => 'PAY_TRANSFER_T');
    --
    Pay_transfer_t_drop_fk;
    --
    -- �������� ������ ������� �������� �� �������������� � ����������� ������� ����� ���. ���
    FOR r_pay IN (
        SELECT P.ACCOUNT_ID, 
               P.PAYMENT_ID, P.REP_PERIOD_ID PAY_PERIOD_ID, 
               B.BILL_ID, B.REP_PERIOD_ID  
          FROM PAYMENT_T P, BILL_T B, ACCOUNT_T A
         WHERE 1=1 --A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_P
           AND P.ACCOUNT_ID   = A.ACCOUNT_ID
           AND B.ACCOUNT_ID   = A.ACCOUNT_ID
           AND P.BALANCE > 0
           AND B.TOTAL   > 0  -- �������� ������ � ������� �������
           AND B.DUE     < 0  -- ���� ������������ �������������
           AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_CLOSED, Pk00_Const.c_BILL_STATE_READY)
           AND P.REP_PERIOD_ID <= p_from_period_id 
           AND B.REP_PERIOD_ID <= p_from_period_id
        ORDER BY B.BILL_DATE, P.PAYMENT_DATE
      )
    LOOP
        SAVEPOINT X;  -- ����� ���������� ������ ��� �������� �����
        BEGIN
            -- �������� ���������� � ������� �� ������ ��������
            SELECT P.BALANCE INTO v_value
              FROM PAYMENT_T P
             WHERE P.PAYMENT_ID    = r_pay.payment_id
               AND P.REP_PERIOD_ID = r_pay.pay_period_id;
            
            -- ����� ��� ��� �������� � ������� ��������
            IF v_value > 0 THEN
                -- �������� ���������� � ����� �� ������ ��������
                SELECT B.DUE INTO v_bill_due
                  FROM BILL_T B
                 WHERE B.BILL_ID       = r_pay.bill_id
                   AND B.REP_PERIOD_ID = r_pay.rep_period_id;
                -- ���� ��� ����� ������� ���������
                IF v_bill_due < 0 THEN
                    -- ����������� ����� ��������
                    IF (v_value + v_bill_due) > 0 THEN  -- ������� ������� �� �������� ����� �� �����
                        v_value := -v_bill_due;         -- � ��� ���������
                    END IF;
                    -- �������� ������ (��� �����) �� ������������ ����
                    v_transfer_id := Transfer_to_bill(
                             p_payment_id    => r_pay.payment_id,   -- ID ������� - ��������� �������
                             p_pay_period_id => r_pay.pay_period_id,-- ID ��������� ������� ���� ����������� ������
                             p_bill_id       => r_pay.bill_id,      -- ID ������������� �����
                             p_rep_period_id => r_pay.rep_period_id,-- ID ��������� ������� �����
                             p_notes         => NULL,               -- ���������� � ��������
                             p_value         => v_value,        -- ����� ������� ����� ���������, NULL - ������� �����
                             p_open_balance  => v_open_balance, -- ����� �� ������� �� ���������� ��������
                             p_close_balance => v_close_balance,-- ����� �� ������� ����� ���������� ��������
                             p_bill_due      => v_bill_due      -- ���������� ���� �� ����� ����� ��������
                         );
                    v_ok := v_ok + 1;         -- �������� ����������� �������
                END IF;   
            ELSE
                -- ������ ��� ��������� ��������
                v_zero := v_zero + 1;
                /*
                Pk01_Syslog.Write_msg(
                   p_Msg  => 'account_id='  ||r_pay.account_id
                          || ', period_id=' ||r_pay.rep_period_id
                          || ', payment_id='||r_pay.payment_id
                          || ', bill_id='   ||r_pay.bill_id 
                          || ' - payment already transfered',
                   p_Src  => c_PkgName||'.'||v_prcName,
                   p_Level=> Pk01_Syslog.L_err );
                */
            END IF;
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
        IF MOD((v_ok+v_err+v_zero), 500) = 0 THEN
            Pk01_Syslog.Write_msg('Processed: '||v_ok||'-ok, '||v_err||'-err, '||v_zero||'-empty', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        --
    END LOOP;
    
    Pk01_Syslog.Write_msg('Gather_Table_Stat', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        
    Gather_Table_Stat(l_Tab_Name => 'PAY_TRANSFER_T');
    --
    Pay_transfer_t_add_fk;
    --
    COMMIT;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ========================================================================== --
-- �������� ��� ���������
-- ========================================================================== --
-- ------------------------------------------------------------------------ --
-- �������� ������������� �������
-- ------------------------------------------------------------------------ --
-- ������������ ������ � �/� ������� (����� ����� ����������� � ������� �/�)
--   - ������������� - ID ���������� ��������� (PAYMENT.PAYMENT_ID) � ��������, 
--   - ��� ������ ���������� ����������
--
FUNCTION OP_revers_payment (
               p_src_payment_id IN INTEGER,   -- ID ������������� �������                        
               p_src_period_id  IN INTEGER,   -- ID ��������� �������, ����� ��� ��������������� ������
               p_dst_period_id  IN INTEGER,   -- ID ��������� �������, ������������� �������
               p_manager        IN VARCHAR2,  -- �������� ����������� ��������
               p_notes          IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
           ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Revers_transfer';
    r_payment        PAYMENT_T%ROWTYPE;
    v_transfer_id    INTEGER;
    v_dst_payment_id INTEGER;
		v_dst_doc_id		 VARCHAR2(100);
    v_advance        NUMBER;
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
    -- ������ ������, ��� �������� ������� �����������, ��� ���������� - ��� ���������
    IF p_src_period_id = p_dst_period_id THEN
        v_advance := -r_payment.ADVANCE;
    ELSE
        v_advance := 0;
    END IF;
    --
    -- ��������� ������������ ������ � ������� ������� �� �������� "������"
    INSERT INTO PAYMENT_T (
        PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE,
        PAYMENT_DATE, ACCOUNT_ID, RECVD,
        ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED, REFUND,
        DATE_FROM, DATE_TO,
        PAYSYSTEM_ID, DOC_ID,
        STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED,
        CREATED_BY, NOTES
    )VALUES(
        v_dst_payment_id, p_dst_period_id, PK00_CONST.c_PAY_TYPE_REVERS, 
        r_payment.PAYMENT_DATE, r_payment.ACCOUNT_ID, -r_payment.RECVD,
        v_advance, SYSDATE, -r_payment.BALANCE, -r_payment.TRANSFERED, -r_payment.REFUND,
        r_payment.DATE_FROM, r_payment.DATE_TO,
        v_paysystem_correct, v_dst_doc_id,
        PK00_CONST.c_PAY_STATE_CLOSE, SYSDATE, SYSDATE, SYSDATE,
        p_manager, p_notes
    );
    --
    -- ���������� �������� �������� ������� � �������� �������
    FOR r_trn IN (
       SELECT T.TRANSFER_ID, T.PAY_PERIOD_ID 
         FROM PAY_TRANSFER_T T
        WHERE T.PAYMENT_ID     = r_payment.PAYMENT_ID
           AND T.PAY_PERIOD_ID = r_payment.REP_PERIOD_ID
         ORDER BY T.TRANSFER_ID DESC  
        -- 
        -- ������� ������ ��� ���������� ������ ����� ���������� ��������:
        -- SELECT TRANSFER_ID, PAY_PERIOD_ID 
        -- FROM (
        --   SELECT LEVEL LVL, T.TRANSFER_ID, T.PAY_PERIOD_ID 
        --     FROM PAY_TRANSFER_T T
        --    WHERE T.PAYMENT_ID    = r_payment.PAYMENT_ID
        --      AND T.PAY_PERIOD_ID = r_payment.REP_PERIOD_ID
        --   CONNECT BY PRIOR TRANSFER_ID = PREV_TRANSFER_ID 
        --   START WITH PREV_TRANSFER_ID IS NULL
        -- )ORDER BY 1 DESC
        --       
      )
    LOOP
        v_transfer_id := Revers_transfer(
               p_transfer_id   => r_trn.transfer_id,   -- ID ������������ �������� �������� �������
               p_pay_period_id => r_trn.pay_period_id, -- ID ������� �������
               p_notes         => NULL                 -- ����������
           ); 
    END LOOP;
    
    -- �������� ���������� ������� ����� � ����������� ������ "�����������"
    UPDATE PAYMENT_T SET REFUND = RECVD, STATUS = Pk00_Const.c_PAY_STATE_REVERS
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
FUNCTION OP_refund (
               p_src_payment_id IN INTEGER,   -- ID ��������������� �������
               p_src_period_id  IN INTEGER,   -- ID ��������� �������, ����� ��� ��������������� ������
               p_dst_period_id  IN INTEGER,   -- ID ��������� �������, ��������������� �������
               p_value          IN NUMBER,    -- ���������� ����� ��������
               p_date           IN DATE,      -- ���� �������� �������
               p_manager        IN VARCHAR2,  -- �������� ����������� ��������
               p_notes          IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
           ) RETURN INTEGER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'OP_refund';
    r_payment      PAYMENT_T%ROWTYPE;
    v_payment_id   INTEGER;
		v_doc_id			 VARCHAR2(100) := pk02_poid.Next_Payment_Doc_Id(v_paysystem_correct);
BEGIN
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
    v_payment_id := Add_payment (
              p_account_id      => r_payment.Account_Id,        -- ID �������� ����� �������
              p_rep_period_id   => p_dst_period_id,             -- ID ��������� ������� ���� ����������� ������
              p_payment_dat�    => p_date,                      -- ���� �������
              p_payment_type    => Pk00_Const.c_PAY_TYPE_REFUND,-- ��� ������� ������� �����
              p_recvd           => -p_value,                    -- ����� �������
              p_paysystem_id    => v_paysystem_correct,         -- ID ��������� �������
              p_doc_id          => v_doc_id,                    -- ID ��������� � ��������� �������
              p_status          => Pk00_Const.c_PAY_STATE_OPEN, -- ������ �������
              p_manager    		  => p_manager,                   -- �.�.�. ��������� ��������������� ������ �� �/�
              p_notes           => p_notes                      -- ���������� � �������  
           );
           
    -- ��������� �������� ������������� �������
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
FUNCTION OP_move_payment (
               p_src_payment_id IN INTEGER,  -- ID ������� ���������
               p_src_period_id  IN INTEGER,  -- ID ��������� ������� ���������
               p_dst_account_id IN INTEGER,  -- ID ������� ���������
               p_dst_period_id  IN INTEGER,  -- ID ��������� ������� ���������
               p_manager        IN VARCHAR2, -- �������� ����������� ��������
               p_notes          IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
           ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'OP_move_payment';
    r_payment        PAYMENT_T%ROWTYPE;
    v_rev_payment_id INTEGER;
    v_payment_id     INTEGER;
BEGIN
    -- ������ ������ ������������ �������
    SELECT * INTO r_payment
      FROM PAYMENT_T 
     WHERE PAYMENT_ID   = p_src_payment_id 
       AND REP_PERIOD_ID= p_src_period_id;

    -- ���������� ������ ��������
    v_rev_payment_id := OP_revers_payment (
               p_src_payment_id,  -- ID ������������� �������                        
               p_src_period_id,   -- ID ��������� �������, ����� ��� ��������������� ������
               p_dst_period_id,   -- ID ��������� �������, ������������� �������
               p_manager,         -- �������� ����������� ��������
               p_notes            -- ���������� � ��������
           );
    -- ��������� ������ � ��������� �������
    v_payment_id := Add_payment (
              p_account_id      => p_dst_account_id,           -- ID �������� ����� �������
              p_rep_period_id   => p_dst_period_id,            -- ID ��������� ������� ���� ����������� ������
              p_payment_dat�    => r_payment.Payment_Date,     -- ���� �������
              p_payment_type    => Pk00_Const.c_PAY_TYPE_MOVE, -- ��� �������
              p_recvd           => r_payment.Recvd,            -- ����� �������
              p_paysystem_id    => r_payment.paysystem_id,     -- ID ��������� �������
              p_doc_id          => r_payment.Doc_Id,           -- ID ��������� � ��������� �������
              p_status          => Pk00_Const.c_PAY_STATE_OPEN,-- ������ �������
              p_manager    		  => p_manager,  -- �.�.�. ��������� ��������������� ������ �� �/�
              p_notes           => p_notes     -- ���������� � �������  
           );
           
    -- ��������� �������� ������������� �������
    INSERT INTO PAYMENT_OPERATION_T O (
        O.OPER_ID, O.OPER_TYPE_ID, O.OPER_DATE, O.OPER_TOTAL, 
        O.SRC_PAYMENT_ID, O.SRC_REP_PERIOD_ID,
        O.DST_PAYMENT_ID, O.DST_REP_PERIOD_ID,
        O.CREATED_BY, O.NOTES )
    VALUES(
        Pk02_Poid.Next_transfer_id, PK00_CONST.c_PAY_OP_MOVE, SYSDATE, r_payment.RECVD,
        p_src_payment_id, p_src_period_id, v_payment_id, p_dst_period_id,
        p_manager, p_notes
    );
           
    RETURN v_payment_id;  
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.Payment_id='||p_src_payment_id, c_PkgName||'.'||v_prcName );
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

--=========================================================================
PROCEDURE xTTK_to_Saler
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'xTTK_to_Saler';
    v_profile_id    INTEGER;
    v_count         INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    FOR c IN (
      SELECT S.BRAND, LL.CONTRACTOR_ID, 
             LL.SELLER_ID, LL.SELLER_BANK_ID, LL.SELLER_REGION_ID,
             AP.PROFILE_ID, AP.ACCOUNT_ID, AP.DATE_FROM, AP.DATE_TO, 
             B.BILL_ID, B.BILL_NO
        FROM SVM_XTTK_TO_SALER_T S, CONTRACT_T C, ACCOUNT_PROFILE_T AP, BILL_T B,--, CONTRACTOR_T CR
             LL_CONTRACTOR_SELLER_MIGRATION LL
       WHERE S.CONTRACT_NO = C.CONTRACT_NO
         AND AP.CONTRACT_ID = C.CONTRACT_ID
         AND B.REP_PERIOD_ID(+) = 201503
         AND B.PROFILE_ID(+) = AP.PROFILE_ID
         AND B.ACCOUNT_ID(+) = AP.ACCOUNT_ID
         AND S.BRAND = LL.CONTRACTOR_NAME
         ORDER BY BRAND
    )
    LOOP
        -- ��������� ������ �������
        UPDATE ACCOUNT_PROFILE_T AP SET DATE_TO = TO_DATE('28.02.2015 23:59:59','dd.mm.yyyy hh24:mi:ss')
         WHERE AP.PROFILE_ID = c.Profile_Id
           AND (AP.DATE_TO IS NULL OR AP.DATE_TO >= TO_DATE('01.03.2015','dd.mm.yyyy'))
        ;
        -- ������� ����� �������
        v_profile_id := SQ_ACCOUNT_ID.NEXTVAL;
        
        INSERT INTO ACCOUNT_PROFILE_T (
           PROFILE_ID, ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, SUBSCRIBER_ID, 
           CONTRACTOR_ID, BRANCH_ID, AGENT_ID, CONTRACTOR_BANK_ID, VAT, 
           DATE_FROM, DATE_TO, CUSTOMER_PAYER_ID, BRAND_ID
        )
        SELECT v_profile_id, ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, SUBSCRIBER_ID, 
               c.SELLER_ID CONTRACTOR_ID, BRANCH_ID, AGENT_ID, 
               c.SELLER_BANK_ID CONTRACTOR_BANK_ID, VAT, 
               TO_DATE('01.03.2015','dd.mm.yyyy') DATE_FROM, NULL DATE_TO, 
               CUSTOMER_PAYER_ID, BRAND_ID 
          FROM ACCOUNT_PROFILE_T
          WHERE PROFILE_ID = c.profile_id
        ;

        -- �������� ���� �����  
        IF c.BILL_ID IS NOT NULL THEN
          
          UPDATE BILL_T B 
             SET B.CONTRACTOR_ID = c.SELLER_ID,
                 B.CONTRACTOR_BANK_ID = c.SELLER_BANK_ID,
                 B.PROFILE_ID = v_profile_id,
                 B.BILL_NO = LPAD(TO_CHAR(c.SELLER_REGION_ID), 4,'0')||'/'||B.BILL_NO
           WHERE B.REP_PERIOD_ID = 201503
             AND B.BILL_ID    = c.Bill_Id
             AND B.PROFILE_ID = c.profile_id;
             
        END IF;
        v_count := v_count + 1;
    
    END LOOP;

    Pk01_Syslog.Write_msg(v_count||' - accounts transfered', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ���������� ID �������� ��������
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


END PKXX_SVM_TEST;
/
