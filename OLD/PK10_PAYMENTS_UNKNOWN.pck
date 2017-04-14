CREATE OR REPLACE PACKAGE PK10_PAYMENTS_UNKNOWN
IS
    --
    -- ������������ �������� �������� �������� ��� ����������� ������
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_PAYMENTS_UNKNOWN';
    -- ==============================================================================
   
    type t_refc is ref cursor;

    -- ��������� �� �/� �������������� ��������    
    c_UNKNOWN_ACCOUNT_ID CONSTANT INTEGER      := 2; ---1947741; -- ��������� �� 2
    c_UNKNOWN_ACCOUNT_NO CONSTANT VARCHAR2(12) := 'ACC000000002';
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������ ������� � ��������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Push_payment (
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
      
   
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���������� ������� �� ��������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Pop_payment (
                  p_payment_id      IN INTEGER,   -- ID ��������������� �������
                  p_pay_period_id   IN INTEGER,   -- ID ��������� ������� ���� ������ ������
                  p_account_id      IN INTEGER,   -- ID �������� ����� �������
                  p_rep_period_id   IN INTEGER,   -- ID ��������� ������� ���� ����������� ������
                  p_manager         IN VARCHAR2,  -- �������� ����������� ��������
                  p_notes           IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ �������������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Payment_list (
                   p_recordset OUT t_refc, 
                   p_date_from  IN DATE DEFAULT TO_DATE('01.01.2008','dd.mm.yyyy'),
                   p_date_to    IN DATE DEFAULT NULL
               );


END PK10_PAYMENTS_UNKNOWN;
/
CREATE OR REPLACE PACKAGE BODY PK10_PAYMENTS_UNKNOWN
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� ������ ������� � ��������������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Push_payment (
              p_payment_dat�    IN DATE,      -- ���� �������
              p_payment_type    IN VARCHAR2,  -- ��� �������
              p_recvd           IN NUMBER,    -- ����� �������
              p_paysystem_id    IN INTEGER,   -- ID ��������� �������
              p_doc_id          IN VARCHAR2,  -- ID ��������� � ��������� �������
              p_status          IN VARCHAR2,  -- ������ �������
              p_manager         IN VARCHAR2,  -- �.�.�. ��������� ��������������� ������ �� �/�
              p_notes           IN VARCHAR2,  -- ���������� � �������  
              p_descr           IN VARCHAR2    DEFAULT NULL -- �������� �������
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Push_payment';
    v_payment_id    INTEGER;
    v_pay_period_id INTEGER;
BEGIN
    v_pay_period_id := Pk04_Period.Period_id(p_payment_dat�);
    --    
    v_payment_id := PK10_PAYMENT.Add_payment (
        p_account_id    => c_UNKNOWN_ACCOUNT_ID, -- ID �������� ����� �������
        p_rep_period_id => v_pay_period_id, -- ID ��������� ������� ���� ����������� ������
        p_payment_dat�  => p_payment_dat�,  -- ���� �������
        p_payment_type  => p_payment_type,  -- ��� �������
        p_recvd         => p_recvd,         -- ����� �������
        p_paysystem_id  => p_paysystem_id,  -- ID ��������� �������
        p_doc_id        => p_doc_id,        -- ID ��������� � ��������� �������
        p_status        => p_status,        -- ������ �������
        p_manager       => p_manager,       -- �.�.�. ��������� ��������������� ������ �� �/�
        p_notes         => p_notes,         -- ���������� � �������  
        p_descr         => p_descr          -- �������� �������
     );
    --
    RETURN v_payment_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;
   
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� ���������� ������� �� ��������������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Pop_payment (
              p_payment_id      IN INTEGER,   -- ID ��������������� �������
              p_pay_period_id   IN INTEGER,   -- ID ��������� ������� ���� ������ ������
              p_account_id      IN INTEGER,   -- ID �������� ����� �������
              p_rep_period_id   IN INTEGER,   -- ID ��������� ������� ���� ����������� ������
              p_manager         IN VARCHAR2,  -- �������� ����������� ��������
              p_notes           IN VARCHAR2 DEFAULT NULL -- ���������� � ��������
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Pop_payment';
    v_payment_id INTEGER;
    v_recvd      NUMBER;
		v_dst_doc_id VARCHAR2(100);
    r_payment    PAYMENT_T%ROWTYPE;
   
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ��������� ������ �� ������ �������� ����������� �������������� ������
    IF PK04_PERIOD.Is_closed(p_pay_period_id) THEN
      -- ���� ������ ������, �� ���������� ������ � ������� ������� � ������� �����
      --
      -- ������ ������ ������� �� ��������������
      SELECT * INTO r_payment
        FROM PAYMENT_T 
       WHERE PAYMENT_ID   = p_payment_id 
         AND REP_PERIOD_ID= p_pay_period_id
         AND ACCOUNT_ID   = c_UNKNOWN_ACCOUNT_ID
         AND BALANCE > 0       -- �� �������������� ������� ������ = 0
         AND ADVANCE = RECVD   -- �������� � ��������������� ���������
         AND TRANSFERED = 0
         AND REFUND = 0        -- ������� � ��������������� ��������
         AND STATUS != Pk00_Const.c_PAY_STATE_REVERS;  -- ��� �� �����������
      --
      -- �������� ����� ���������� ��� ������������� �������
      v_dst_doc_id := pk02_poid.Next_Payment_Doc_Id();
      v_payment_id := PK02_POID.Next_payment_id;
      --
      -- ��������� ������������ ������ � ������� ������� �� �������� "������"
      -- � ������� ������� � ��������� � ��������������� �������
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
          v_payment_id, p_rep_period_id, r_payment.ACCOUNT_ID,
          -r_payment.RECVD, 0, SYSDATE, 0, r_payment.TRANSFERED,
          r_payment.DATE_FROM, r_payment.DATE_TO, r_payment.PAYMENT_DATE,
          PK00_CONST.c_PAY_TYPE_REVERS, r_payment.PAYSYSTEM_ID,
          v_dst_doc_id, PK00_CONST.c_PAY_STATE_CLOSE, SYSDATE,
          SYSDATE, p_manager, SYSDATE, 
          r_payment.PAYSYSTEM_CODE, r_payment.PAY_DESCR,
          p_payment_id, p_pay_period_id, 
          r_payment.REFUND, NULL, r_payment.NOTES, r_payment.EXTERNAL_ID
      );
      
      -- ��������� �������� ������������� �������
      INSERT INTO PAYMENT_OPERATION_T O (
          O.OPER_ID, O.OPER_TYPE_ID, O.OPER_DATE, O.OPER_TOTAL, 
          O.SRC_PAYMENT_ID, O.SRC_REP_PERIOD_ID,
          O.DST_PAYMENT_ID, O.DST_REP_PERIOD_ID,
          O.CREATED_BY, O.NOTES )
      VALUES(
          Pk02_Poid.Next_transfer_id, Pk00_Const.c_PAY_OP_REVERS, SYSDATE, r_payment.RECVD,
          p_payment_id, p_pay_period_id, v_payment_id, p_rep_period_id,
          p_manager, p_notes
      );
      
      --
      -- �������� ����� ���������� ��� ������������� �������
      v_dst_doc_id := pk02_poid.Next_Payment_Doc_Id();
      v_payment_id := PK02_POID.Next_payment_id;
      --
      -- ��������� ������ �� ��������� �/�
      -- ��������� �������� � ��������������� �������
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
          v_payment_id, p_rep_period_id, p_account_id,
          r_payment.RECVD, r_payment.ADVANCE, SYSDATE, r_payment.BALANCE, r_payment.TRANSFERED,
          r_payment.DATE_FROM, r_payment.DATE_TO, r_payment.PAYMENT_DATE,
          PK00_CONST.c_PAY_TYPE_MOVE, r_payment.PAYSYSTEM_ID,
          v_dst_doc_id, PK00_CONST.c_PAY_STATE_OPEN, SYSDATE,
          SYSDATE, p_manager, SYSDATE, 
          r_payment.PAYSYSTEM_CODE, r_payment.PAY_DESCR,
          p_payment_id, p_pay_period_id, 
          r_payment.REFUND, NULL, p_notes||' - '||r_payment.NOTES, r_payment.EXTERNAL_ID
      );
      --
      -- �������� ������ � ������ ��������������� �������
      UPDATE PAYMENT_T P
         SET BALANCE = 0,
             STATUS  = Pk00_Const.c_PAY_STATE_REVERS
       WHERE PAYMENT_ID   = p_payment_id 
         AND REP_PERIOD_ID= p_pay_period_id
         AND ACCOUNT_ID   = c_UNKNOWN_ACCOUNT_ID;
      --
      -- ����� ������� ��������� �� �/�
      v_recvd := r_payment.RECVD;
      --
    ELSE
      -- ���� ������ ������, �� ������ ��������� �������� ��������� �/�
      UPDATE PAYMENT_T P
         SET P.REP_PERIOD_ID = p_rep_period_id,
             P.ACCOUNT_ID    = p_account_id
       WHERE P.REP_PERIOD_ID = p_pay_period_id
         AND P.PAYMENT_ID    = p_payment_id
         AND P.ACCOUNT_ID    = c_UNKNOWN_ACCOUNT_ID
      RETURNING P.RECVD INTO v_recvd;
      v_payment_id := p_payment_id;
      --
    END IF; 
    --
    -- �������� ������ �/� �������������� ��������
    UPDATE ACCOUNT_T A
       SET A.BALANCE = A.BALANCE - v_recvd,
           A.BALANCE_DATE = SYSDATE
     WHERE A.ACCOUNT_ID = c_UNKNOWN_ACCOUNT_ID;
    --
    -- �������� ������ ����� ��������� �������
    UPDATE ACCOUNT_T A
       SET A.BALANCE = A.BALANCE + v_recvd,
           A.BALANCE_DATE = SYSDATE
     WHERE A.ACCOUNT_ID = p_account_id;
    --
    RETURN v_payment_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������ �������������� ��������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Payment_list (
               p_recordset OUT t_refc, 
               p_date_from  IN DATE DEFAULT TO_DATE('01.01.2008','dd.mm.yyyy'),
               p_date_to    IN DATE DEFAULT NULL
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Payment_list';
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
        v_date_from := TO_DATE('01.01.2008','dd.mm.yyyy');
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
          WHERE ACCOUNT_ID = c_UNKNOWN_ACCOUNT_ID
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

END PK10_PAYMENTS_UNKNOWN;
/
