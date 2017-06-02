CREATE OR REPLACE PACKAGE PK10_PAYMENT_ADVANCE
IS
    --
    -- ����� ��� ������ � ���������� ���������
    -- --------------------------------------------------------------------------- --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_PAYMENT_ADVANCE';
    -- ==============================================================================
   
    type t_refc is ref cursor;
    
    -- ======================================================================== --
    --                     � � � � � � � � �   � � � � � � �
    -- ======================================================================== --
    -- ������ ����� ������ (�������� id ������ ��� ���������� � �������)
    FUNCTION Open_task RETURN INTEGER;

    -- ��������� ������ - ������� ������ �� ������� (��������� �����������)
    PROCEDURE Close_task(p_task_id IN INTEGER);

    -- �������� ������� ������ �� ���������� �������
    FUNCTION Check_queue(p_task_id IN INTEGER) RETURN BOOLEAN;

    -- ��������� � ������� ������, 
    -- ���� ������ �� �����, �� ��� ������� �� ������, �� ������� ���� �����
    PROCEDURE Add_payments_to_queue (
            p_task_id      IN INTEGER,
            p_period_id    IN INTEGER,
            p_payment_id   IN INTEGER DEFAULT NULL -- �� ��������� - ��� ������� �������
        );

    -- ��������� � ������� ���� ������
    PROCEDURE Add_payment_to_queue (
            p_task_id      IN INTEGER,
            p_period_id    IN INTEGER,
            p_payment_id   IN INTEGER,
            p_advance_date IN DATE DEFAULT NULL,
            p_advance_no   IN VARCHAR2 DEFAULT NULL
        );
        
   
    -- ------------------------------------------------------------------------ --
    -- �������� ��������� ����� �����-������� �� ����� � BRM
    -- ������ ������ ����������� �� ������� �������: A_REGIONID/YYMM(� �/�)/[1-9]
    -- �������� ���, ��������!!!
    --
    FUNCTION Next_advance_no(
         p_payment_id IN INTEGER,
         p_period_id  IN INTEGER
     ) RETURN VARCHAR2;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ��������� �����-������� ������� � �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_advance(p_task_id IN INTEGER);
   
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ��������� �����-������� ������� � �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Delete_advance(p_task_id IN INTEGER);

    -- ======================================================================== --
    -- ������������ ������-������ �� �����
    -- ======================================================================== --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ����� ��� ���������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Make_payment_advance(
                 p_payment_id IN INTEGER,
                 p_period_id  IN INTEGER
              ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ��������� �/� �� ����� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Make_reverse_advance(
                 p_advance_id IN INTEGER,  -- ������������ �����
                 p_period_id  IN INTEGER,  -- ������ ������������� ������
                 p_dst_date   IN DATE      -- ���� ������������ ������
              ) RETURN INTEGER;
             
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������������ �/� �� ����� ����� �� ������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Make_adjust_advance(
                 p_rev_advance_id IN INTEGER,  -- id ������������ �/�
                 p_rev_period_id  IN INTEGER,  -- ������ ������
                 p_dst_date       IN DATE,     -- ���� ������������ ������
                 p_amount         IN NUMBER    -- ����� ������������ �/�
              ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ������� ������, ��� ��������� ������-����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Return_advance_for_credit_note(
                 p_crd_bill_id    IN INTEGER,
                 p_crd_period_id  IN INTEGER
              );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ����� ��� ���� �������� ���������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_period_advance(
                 p_period_id  IN INTEGER
              );


END PK10_PAYMENT_ADVANCE;
/
CREATE OR REPLACE PACKAGE BODY PK10_PAYMENT_ADVANCE
IS

-- ======================================================================== --
--                     � � � � � � � � �   � � � � � � �
-- ======================================================================== --
-- ������ ����� ������ (�������� id ������ ��� ���������� � �������)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Open_task RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Open_task';
    v_task_id     INTEGER;
BEGIN
    -- �������� ����� ������
    SELECT SQ_BILLING_QUEUE_T.NEXTVAL INTO v_task_id FROM DUAL;
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
BEGIN
    -- ������� ������ �� �������
    DELETE FROM ADVANCE_QUEUE_T Q
     WHERE Q.TASK_ID = p_task_id;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Task_id = '||p_task_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������� ������ �� ���������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Check_queue(p_task_id IN INTEGER) RETURN BOOLEAN
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Check_queue';
    v_count   INTEGER;
    v_retcode BOOLEAN;
BEGIN
    -- 
    SELECT COUNT(*) INTO v_count
      FROM ADVANCE_QUEUE_T Q
     WHERE Q.TASK_ID = p_task_id
       AND EXISTS (
           SELECT * FROM PERIOD_T P
            WHERE P.PERIOD_ID = Q.ADV_PERIOD_ID
              AND P.POSITION NOT IN ('OPEN','BILL')
       );
    IF v_count > 0 THEN
      v_retcode := FALSE;
    ELSE
      v_retcode := TRUE;
    END IF;
    
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Task_id = '||p_task_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� � ������� ������, 
-- ���� ������ �� �����, �� ��� ������� �� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Add_payments_to_queue (
        p_task_id      IN INTEGER,
        p_period_id    IN INTEGER,
        p_payment_id   IN INTEGER DEFAULT NULL -- �� ��������� - ��� ������� �������
    ) 
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_payments_to_queue';
    v_count       INTEGER;
BEGIN
    -- ��������� ������ � �������, ������� ��������� �� ������������ � ��������� �����
    INSERT INTO ADVANCE_QUEUE_T (
           TASK_ID, PAY_PERIOD_ID, PAYMENT_ID, ADVANCE_ID, ADVANCE_TOTAL
    )
    SELECT p_task_id TASK_ID, P.REP_PERIOD_ID, P.PAYMENT_ID, 
           Pk02_Poid.Next_bill_id,
           P.RECVD - SUM(NVL(T.TRANSFER_TOTAL, 0)) ADVANCE_TOTAL 
      FROM PAYMENT_T P, PAY_TRANSFER_T T, ACCOUNT_T A
     WHERE P.REP_PERIOD_ID = p_period_id
       AND P.REP_PERIOD_ID = T.PAY_PERIOD_ID(+) -- �������� ����� � �� ����
       AND P.PAYMENT_ID    = T.PAYMENT_ID(+)
       AND T.REP_PERIOD_ID(+)<= T.PAY_PERIOD_ID(+)
       AND P.ACCOUNT_ID    > 5   -- ��������� ��������� �/�
       AND P.ACCOUNT_ID    = A.ACCOUNT_ID
       AND A.ACCOUNT_TYPE  = 'J' -- ������ ��.����
       AND P.PAYSYSTEM_ID  < 50  -- ��� ������������� �������� �/� �� ����������
       AND ( p_payment_id IS NULL OR p_payment_id = P.PAYMENT_ID )
       AND NOT EXISTS (
         SELECT * FROM ADVANCE_T AD
          WHERE P.REP_PERIOD_ID = AD.PAY_PERIOD_ID
            AND P.PAYMENT_ID    = AD.PAYMENT_ID
       )
     GROUP BY P.ACCOUNT_ID, P.REP_PERIOD_ID, P.PAYMENT_ID, P.RECVD
     HAVING P.RECVD > NVL(SUM(T.TRANSFER_TOTAL),0);
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ADVANCE_QUEUE_T - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� � ������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Add_payment_to_queue (
        p_task_id      IN INTEGER,
        p_period_id    IN INTEGER,
        p_payment_id   IN INTEGER,
        p_advance_date IN DATE DEFAULT NULL,
        p_advance_no   IN VARCHAR2 DEFAULT NULL
    ) 
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_payment_to_queue';
BEGIN
    -- ������ ������ � �������
    Add_payments_to_queue ( p_task_id, p_period_id, p_payment_id );
    -- ��������� �������������� ����������
    UPDATE ADVANCE_QUEUE_T Q
       SET Q.ADVANCE_DATE= p_advance_date,
           Q.ADVANCE_NO  = p_advance_no
     WHERE Q.TASK_ID     = p_task_id
       AND Q.PAYMENT_ID  = p_payment_id;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- �������� ��������� ����� �����-������� �� ����� � BRM
-- ������ ������ ����������� �� ������� �������: A_REGIONID/YYMM(� �/�)/[1-9]
-- �������� ���, ��������!!!
--
FUNCTION Next_advance_no(
               p_payment_id IN INTEGER,
               p_period_id  IN INTEGER
           ) RETURN VARCHAR2
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Next_advance_no';
    v_billing_id   INTEGER;
    v_region_id    INTEGER;
    v_advance_no   ADVANCE_T.ADVANCE_NO%TYPE:=NULL;
    v_account_no   ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_count        INTEGER;
    v_next         INTEGER;
BEGIN
    -- �������� ��������������� ����������
    SELECT A.ACCOUNT_NO, A.BILLING_ID, CR.REGION_ID
      INTO v_account_no, v_billing_id, v_region_id
      FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACTOR_T CR, PAYMENT_T P
     WHERE A.ACCOUNT_ID     = P.ACCOUNT_ID
       AND A.ACCOUNT_ID     = AP.ACCOUNT_ID
       AND AP.ACTUAL        = 'Y'
       AND AP.CONTRACTOR_ID = CR.CONTRACTOR_ID
       AND P.PAYMENT_ID     = p_payment_id
       AND P.REP_PERIOD_ID  = p_period_id;

    -- ��������� ����� �����
    v_advance_no := SUBSTR(TO_CHAR(p_period_id),3,4)||v_account_no;
    -- ��������� �� ������������
    v_next := 1;
    LOOP
        -- ��������� ���������� �� �����
        SELECT COUNT(*) INTO v_count
          FROM ADVANCE_T B
         WHERE B.ADVANCE_NO = v_advance_no;  
        EXIT WHEN v_count = 0;  -- ��� ���������, ������� �� �����
        --
        -- ��������� ��������� �� ������� ����    
        v_advance_no := SUBSTR(TO_CHAR(p_period_id),3,4)||v_account_no||'/'||v_next;
        --
        v_next := v_next + 1;
    END LOOP;
    
    -- � ��������� �� ���������� ���������, ��������� ����� �������
    IF v_region_id IS NOT NULL THEN
        v_advance_no := 'A'||v_region_id||'/'||v_advance_no;
    ELSE
        v_advance_no := 'A'||v_advance_no;
    END IF;
    
    RETURN v_advance_no;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR(payment_id=' ||p_payment_id||
                                        ', period_id='  ||p_period_id||')'
                                    , c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������������ ��������� �����-������� ������� � �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Make_advance(p_task_id IN INTEGER) 
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Make_advance';
    v_count       INTEGER;
BEGIN
    -- ��������� ���� �����-�������
    INSERT INTO ADVANCE_T AD (
       ADVANCE_ID, REP_PERIOD_ID, PAYMENT_ID, PAY_PERIOD_ID, 
       ADVANCE_NO, ADVANCE_DATE, ADVANCE_TYPE, ADVANCE_STATUS, 
       CURRENCY_ID, TOTAL, GROSS, TAX, VAT, 
       ACCOUNT_ID, PROFILE_ID, CREATE_DATE
    )
    SELECT 
       Q.ADVANCE_ID, 
       NVL(Q.ADV_PERIOD_ID, Q.PAY_PERIOD_ID) ADV_PERIOD_ID,
       Q.PAYMENT_ID, Q.PAY_PERIOD_ID, 
       NVL(Q.ADVANCE_NO, Pk10_Payment_Advance.Next_advance_no(Q.PAYMENT_ID, Q.PAY_PERIOD_ID)) ADVANCE_NO,
       NVL(Q.ADVANCE_DATE, Pk04_Period.Period_to(Q.PAY_PERIOD_ID)) ADVANCE_DATE,
       'P' ADVANCE_TYPE, 'READY' ADVANCE_STATUS,
       A.CURRENCY_ID,
       Q.ADVANCE_TOTAL,
       PK09_INVOICE.CALC_GROSS(Q.ADVANCE_TOTAL, 'Y', VAT) GROSS, 
       PK09_INVOICE.CALC_TAX(Q.ADVANCE_TOTAL, 'Y', VAT) TAX,
       AP.VAT, AP.ACCOUNT_ID, AP.PROFILE_ID,
       SYSDATE CREATE_DATE 
      FROM ADVANCE_QUEUE_T Q, PAYMENT_T P, ACCOUNT_T A, ACCOUNT_PROFILE_T AP 
     WHERE Q.TASK_ID       = p_task_id
       AND P.REP_PERIOD_ID = Q.PAY_PERIOD_ID
       AND P.PAYMENT_ID    = Q.PAYMENT_ID
       AND P.ACCOUNT_ID    = A.ACCOUNT_ID
       AND P.ACCOUNT_ID    = AP.ACCOUNT_ID
       AND AP.ACTUAL       = 'Y';
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ADVANCE_T - '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� ��������� �����-������� ������� � �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Delete_advance(p_task_id IN INTEGER) 
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_advance';
    v_count       INTEGER;
BEGIN

    -- ������� �����-������� �� �����
    DELETE FROM ADVANCE_T AD
     WHERE EXISTS (
       SELECT * FROM ADVANCE_QUEUE_T Q
        WHERE Q.ADV_PERIOD_ID = AD.REP_PERIOD_ID
          AND Q.ADVANCE_ID    = AD.ADVANCE_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ADVANCE_T - '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ======================================================================== --
-- ������������ ������-������ �� �����
-- ======================================================================== --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������������ ����� ��� ���������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Make_payment_advance(
             p_payment_id IN INTEGER,
             p_period_id  IN INTEGER
          ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Make_payment_advance';
    v_task_id     INTEGER;
    v_advance_id  INTEGER;
BEGIN
    -- �������� id ������
    v_task_id := Open_task;
    
    -- ��������� � ������� ������
    Add_payments_to_queue (
            p_task_id      => v_task_id,
            p_period_id    => p_period_id,
            p_payment_id   => p_payment_id
        ); 

    -- ������������ ��������� ����-�������
    Make_advance( v_task_id );

    -- �������� id ��������������� ������
    SELECT Q.ADVANCE_ID INTO v_advance_id 
      FROM ADVANCE_QUEUE_T Q
     WHERE Q.TASK_ID = v_task_id;

    -- �������� ��������� �������
    Close_task( v_task_id );

    RETURN v_advance_id;
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Payment_id = '||p_payment_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������������ ��������� �/� �� ����� 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Make_reverse_advance(
             p_advance_id IN INTEGER,  -- ������������ �����
             p_period_id  IN INTEGER,  -- ������ ������������� ������
             p_dst_date   IN DATE      -- ���� ������������ ������
          ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Make_revers_advance';
    v_dst_advance_id INTEGER; 
    v_dst_period_id  INTEGER;
    v_dst_level      INTEGER; -- ����� ������������� �/� � ������������������
BEGIN
    -- �������� id ������������� �����-������� �� �����
    v_dst_advance_id:= Pk02_Poid.Next_bill_id;
    v_dst_period_id := TO_CHAR(p_dst_date, 'yyyymm');
    
    -- ��������� ����� ������������ ������
    SELECT TRUNC((MAX(LEVEL)+1)/2)
      INTO v_dst_level
      FROM ADVANCE_T AD
     CONNECT BY PRIOR AD.PREV_ADVANCE_ID = AD.ADVANCE_ID  
     START WITH AD.ADVANCE_ID = p_advance_id;
    
    -- �������� ������������ ����/������� �� �����
    INSERT INTO ADVANCE_T (
        ADVANCE_ID, REP_PERIOD_ID, PAYMENT_ID, PAY_PERIOD_ID,
        ACCOUNT_ID, ADVANCE_NO, ADVANCE_DATE, 
        ADVANCE_TYPE, ADVANCE_STATUS, CURRENCY_ID, TOTAL, GROSS, TAX, VAT, 
        PREV_ADVANCE_ID, PREV_PERIOD_ID, NEXT_ADVANCE_ID, NEXT_PERIOD_ID, 
        CREATE_DATE, MODIFY_DATE, NOTES, PROFILE_ID
    )
    SELECT v_dst_advance_id ADVANCE_ID, v_dst_period_id REP_PERIOD_ID, 
           PAYMENT_ID, PAY_PERIOD_ID,
           ACCOUNT_ID, ADVANCE_NO||'-R'||v_dst_level, TRUNC(p_dst_date) ADVANCE_DATE, 
           'R', ADVANCE_STATUS, CURRENCY_ID, -TOTAL, -GROSS, -TAX, VAT, 
           ADVANCE_ID PREV_ADVANCE_ID, REP_PERIOD_ID PREV_PERIOD_ID, 
           NULL NEXT_ADVANCE_ID, NULL NEXT_PERIOD_ID, 
           SYSDATE CREATE_DATE, SYSDATE MODIFY_DATE, NOTES, PROFILE_ID 
      FROM ADVANCE_T AD
     WHERE AD.ADVANCE_ID    = p_advance_id
       AND AD.REP_PERIOD_ID = p_period_id;

    -- ���������� id ������������ ����-�������
    RETURN v_dst_advance_id;
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Advance_id = '||p_advance_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� ������������ �/� �� ����� ����� �� ������������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Make_adjust_advance(
             p_rev_advance_id IN INTEGER,  -- id ������������ �/�
             p_rev_period_id  IN INTEGER,  -- ������ ������
             p_dst_date       IN DATE,     -- ���� ������������ ������
             p_amount         IN NUMBER    -- ����� ������������ �/�
          ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Make_adjust_advance';
    v_task_id        INTEGER;
    v_dst_advance_id INTEGER; 
    v_dst_period_id  INTEGER;
    v_dst_level      INTEGER; -- ����� �������������� �/� � ������������������
BEGIN
    -- �������� id �������������� ����-������� �� �����
    v_dst_advance_id:= Pk02_Poid.Next_bill_id;
    v_dst_period_id := TO_CHAR(p_dst_date, 'yyyymm');
    
    -- ��������� ����� ������������ ������
    SELECT TRUNC(MAX(LEVEL)/2)
      INTO v_dst_level
      FROM ADVANCE_T AD
     CONNECT BY PRIOR AD.PREV_ADVANCE_ID = AD.ADVANCE_ID  
     START WITH AD.ADVANCE_ID = p_rev_advance_id;
    
    -- �������� ������������ ����/������� �� �����
    INSERT INTO ADVANCE_T (
        ADVANCE_ID, REP_PERIOD_ID, 
        PAYMENT_ID, PAY_PERIOD_ID, ACCOUNT_ID, 
        ADVANCE_NO, ADVANCE_DATE, 
        ADVANCE_TYPE, ADVANCE_STATUS, CURRENCY_ID, TOTAL, GROSS, TAX, VAT, 
        PREV_ADVANCE_ID, PREV_PERIOD_ID, NEXT_ADVANCE_ID, NEXT_PERIOD_ID, 
        CREATE_DATE, MODIFY_DATE, NOTES, PROFILE_ID
    )
    SELECT v_dst_advance_id ADVANCE_ID, v_dst_period_id REP_PERIOD_ID, 
           PAYMENT_ID, PAY_PERIOD_ID, ACCOUNT_ID, 
           SUBSTR(ADVANCE_NO, 1, INSTR(ADVANCE_NO,'-',-1,1)-1)||'-A'||v_dst_level,
           TRUNC(p_dst_date) ADVANCE_DATE, 
           'A', ADVANCE_STATUS, CURRENCY_ID, 
           p_amount, 
           PK09_INVOICE.CALC_GROSS(p_amount, 'Y', AD.VAT) GROSS, 
           PK09_INVOICE.CALC_TAX(p_amount, 'Y', AD.VAT) TAX,
           VAT, 
           p_rev_advance_id PREV_ADVANCE_ID, p_rev_period_id PREV_PERIOD_ID, 
           NULL NEXT_ADVANCE_ID, NULL NEXT_PERIOD_ID, 
           SYSDATE CREATE_DATE, SYSDATE MODIFY_DATE, NOTES, PROFILE_ID 
      FROM ADVANCE_T AD
     WHERE AD.ADVANCE_ID    = p_rev_advance_id
       AND AD.REP_PERIOD_ID = p_rev_period_id;
    
    -- ���������� ������������ ���� � ������������
    UPDATE ADVANCE_T AR
       SET AR.NEXT_ADVANCE_ID = v_dst_advance_id, 
           AR.NEXT_PERIOD_ID  = v_dst_period_id
     WHERE AR.ADVANCE_ID      = p_rev_advance_id
       AND AR.REP_PERIOD_ID   = p_rev_period_id;

    -- ���������� id �������������� ����-�������
    RETURN v_dst_advance_id;
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Rev_advance_id = '||p_rev_advance_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������������ ������� ������, ��� ��������� ������-����
-- ����� ������������ � �������� ������ ������-����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Return_advance_for_credit_note(
             p_crd_bill_id    IN INTEGER,
             p_crd_period_id  IN INTEGER
          )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Return_advance_for_credit_note';
    v_advance_id    INTEGER;
    v_adv_period_id INTEGER; 
    v_advance_type  VARCHAR2(1);
    v_adv_total     NUMBER;
    v_count         INTEGER;
BEGIN
    -- �������� ������ ��������, ������� �������� �������������
    FOR rp IN (
        SELECT T.PAY_PERIOD_ID, T.PAYMENT_ID, B.BILL_DATE,
               SUM(T.TRANSFER_TOTAL) TRANSFER_TOTAL  
          FROM PAY_TRANSFER_T T, BILL_T B--, ADVANCE_T AD
         WHERE T.TRANSFER_TOTAL < 0
           AND T.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND T.BILL_ID       = B.BILL_ID
           AND B.BILL_TYPE     = 'C'
           AND B.REP_PERIOD_ID = p_crd_period_id
           AND B.BILL_ID       = p_crd_bill_id
         GROUP BY T.PAY_PERIOD_ID, T.PAYMENT_ID, B.BILL_DATE
      )
    LOOP
        -- ��������� ���� �� ������������ ��� ������� �/� �� �����
        SELECT COUNT(*) INTO v_count
          FROM ADVANCE_T AD
         WHERE AD.PAYMENT_ID    = rp.payment_id  
           AND AD.PAY_PERIOD_ID = rp.pay_period_id
           AND AD.REP_PERIOD_ID < p_crd_period_id;
        -- ���� ���� ������������, �� ���������� ��
        IF v_count > 0 THEN
            -- �������� �������������� ����������
            SELECT REP_PERIOD_ID, ADVANCE_ID, TOTAL, ADVANCE_TYPE 
              INTO v_adv_period_id, v_advance_id, v_adv_total, v_advance_type
              FROM (
                SELECT ROW_NUMBER() OVER (PARTITION BY AD.PAYMENT_ID 
                                          ORDER BY AD.REP_PERIOD_ID DESC, 
                                                   DECODE(ADVANCE_TYPE,'P',1,'A',2,'R',3,10)) RN,
                       AD.REP_PERIOD_ID, AD.ADVANCE_ID, AD.TOTAL, AD.ADVANCE_TYPE 
                  FROM ADVANCE_T AD
                 WHERE AD.PAYMENT_ID    = rp.payment_id  
                   AND AD.PAY_PERIOD_ID = rp.pay_period_id
                   AND AD.REP_PERIOD_ID < p_crd_period_id
            )
            WHERE RN = 1;
            -- ���������� ��������� �/� �� ����� 
            v_advance_id := Make_reverse_advance(
                         p_advance_id => v_advance_id,    -- ������������ �����
                         p_period_id  => v_adv_period_id, -- ������ ������������� ������
                         p_dst_date   => rp.bill_date     -- ���� ������������ ������
                      );
            -- ������� ������������ �/� �� ����� ����� �� ������������
            v_advance_id := Make_adjust_advance(
                       p_rev_advance_id => v_advance_id,     -- id ������������ �/�
                       p_rev_period_id  => v_adv_period_id,  -- ������ ������
                       p_dst_date       => rp.bill_date,     -- ���� ������������ ������
                       p_amount         => rp.transfer_total - v_adv_total -- ����� ������������ �/�
                    );
          
        ELSE
            -- ��������� ���� ������� �� �������������� ����� 
            v_advance_id := Make_payment_advance(
                       p_payment_id => rp.payment_id,
                       p_period_id  => rp.pay_period_id
                    );
        END IF;
        
    END LOOP;
    
    /*
    -- �������� ������ �/� �� ����� ������� �������� ������������� �����
    FOR ra IN (
      SELECT ADV_PERIOD_ID, ADVANCE_ID, BILL_DATE, REP_PERIOD_ID, 
             ADVANCE_TOTAL, TRANSFER_TOTAL
        FROM ( 
            SELECT ROW_NUMBER() OVER (
                     PARTITION BY AI.PAY_PERIOD_ID, AI.PAYMENT_ID 
                         ORDER BY AI.ADV_PERIOD_ID DESC, 
                                  AI.ADVANCE_ID DESC
                   ) RN,
                   AI.ADV_PERIOD_ID, AI.ADVANCE_ID, BC.BILL_DATE,
                   BC.REP_PERIOD_ID, T.PAY_PERIOD_ID, T.PAYMENT_ID, 
                   SUM(T.TRANSFER_TOTAL) OVER (
                     PARTITION BY T.C_REP_PERIOD_ID, T.C_BILL_ID
                   ) TRANSFER_TOTAL,
                   AD.TOTAL ADVANCE_TOTAL
              FROM BILL_T BC, PAY_TRANSFER_T T, ADVANCE_ITEM_T AI, ADVANCE_T AD
             WHERE BC.REP_PERIOD_ID = p_crd_period_id
               AND BC.BILL_ID       = p_crd_bill_id
               AND BC.BILL_TYPE     = 'C'
               AND T.C_BILL_ID      = BC.BILL_ID 
               AND T.C_REP_PERIOD_ID= BC.REP_PERIOD_ID
               AND AI.PAY_PERIOD_ID = T.PAY_PERIOD_ID
               AND AI.PAYMENT_ID    = T.PAYMENT_ID
               AND AI.ADV_PERIOD_ID < BC.REP_PERIOD_ID
               AND AI.ADV_PERIOD_ID = AD.REP_PERIOD_ID
               AND AI.ADVANCE_ID    = AD.ADVANCE_ID
             )
       WHERE RN = 1
    )
    LOOP
      -- ������������ ��������� �/� �� ����� 
      v_advance_id := Make_reverse_advance(
                   p_advance_id => ra.advance_id,    -- ������������ �����
                   p_period_id  => ra.adv_period_id, -- ������ ������������� ������
                   p_dst_date   => ra.bill_date      -- ���� ������������ ������
                );
      -- ������� ������������ �/� �� ����� ����� �� ������������
      v_advance_id :=  Make_adjust_advance(
                   p_rev_advance_id => v_advance_id, -- id ������������ �/�
                   p_rev_period_id  => ra.rep_period_id,  -- ������ ������
                   p_dst_date       => ra.bill_date, -- ���� ������������ ������
                   p_amount         => ra.advance_total - ra.transfer_total -- ����� ������������ �/�
                );
      --
    END LOOP;
    */
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Credit_bill_id = '||p_crd_bill_id, c_PkgName||'.'||v_prcName );
END;


-- ======================================================================== --
--                           �  �  �  �  �  �
-- ======================================================================== --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������������ ����� ��� ���� �������� ���������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Make_period_advance(
             p_period_id  IN INTEGER
          )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Make_period_advance';
    v_task_id INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- �������� id ������
    v_task_id := Open_task;
    
    -- ��������� � ������� ������
    Add_payments_to_queue (
        p_task_id   => v_task_id,
        p_period_id => p_period_id
    );

    -- ������������ ��������� �����-������� ��� �������
    Make_advance( v_task_id );

    -- �������� ��������� �������
    Close_task( v_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Period_id = '||v_task_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� �������� ������� �� ����� � �/� �� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Bind_period_advance_transfer(
             p_period_id  IN INTEGER
          )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Bind_period_advance_transfer';
    v_count   INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    MERGE INTO PAY_TRANSFER_T T
    USING (
        SELECT DISTINCT T.PAY_PERIOD_ID, T.PAYMENT_ID, 
               AD.REP_PERIOD_ID ADV_PERIOD_ID, AD.ADVANCE_ID
          FROM PAY_TRANSFER_T T, ADVANCE_T AD, BILL_T B
         WHERE T.PAY_PERIOD_ID < T.REP_PERIOD_ID -- ������ ����������
           AND T.REP_PERIOD_ID = 201605
           AND AD.PAY_PERIOD_ID= T.PAY_PERIOD_ID
           AND AD.PAYMENT_ID   = T.PAYMENT_ID 
           AND T.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND T.BILL_ID       = B.BILL_ID
           AND AD.ADVANCE_TYPE = 'P'
           AND B.BILL_TYPE    != 'C'  -- ������������� ��������
    ) TA
    ON (
        T.PAY_PERIOD_ID = TA.PAY_PERIOD_ID AND T.PAYMENT_ID = TA.PAYMENT_ID 
    )
    WHEN MATCHED THEN UPDATE 
                         SET T.ADVANCE_ID = TA.ADVANCE_ID, 
                             T.ADV_PERIOD_ID = TA.ADV_PERIOD_ID;
    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. PAY_TRANSFER_T: '||v_count||' rows meged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Period_id = '||p_period_id, c_PkgName||'.'||v_prcName );
END;


END PK10_PAYMENT_ADVANCE;
/
