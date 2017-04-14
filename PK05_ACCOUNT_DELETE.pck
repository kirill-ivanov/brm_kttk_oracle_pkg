CREATE OR REPLACE PACKAGE PK05_ACCOUNT_DELETE
IS
    --
    -- ����� ��� ������ � �������� "������� ����", �������:
    -- account_t, account_profile_t, billinfo_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK05_ACCOUNT_DELETE';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ����� ������ ��� ���������� � �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Open_task RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ - ������� ������ �� ������� (��������� �����������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Close_task(p_task_id IN INTEGER);

    -- ---------------------------------------------------------------------------- --
    -- ��������� ���������� ������� (ROLLBACK_QUEUE_T) �� �������� ������ 
    -- ---------------------------------------------------------------------------- --
    PROCEDURE Put_order_to_queue (
                 p_task_id  IN INTEGER,
                 p_order_id IN INTEGER
              );

    -- ---------------------------------------------------------------------------- --
    -- ��������� ���������� ������� (ROLLBACK_QUEUE_T) �� �������� �/�
    -- ---------------------------------------------------------------------------- --
    PROCEDURE Put_account_to_queue (
                 p_task_id    IN INTEGER,
                 p_account_id IN INTEGER
              );

    -- ---------------------------------------------------------------------------- --
    -- ��������� ���������� ������� (ROLLBACK_QUEUE_T) �� �������� ��������
    -- ---------------------------------------------------------------------------- --
    PROCEDURE Put_contract_to_queue (
                 p_task_id     IN INTEGER,
                 p_contract_id IN INTEGER
              );

    -- ---------------------------------------------------------------------------- --
    -- �������� ������ �� ��������
    -- ---------------------------------------------------------------------------- --
    PROCEDURE Delete_bills (
                 p_task_id     IN INTEGER,
                 p_period_from IN INTEGER
              );

    -- ---------------------------------------------------------------------------- --
    -- �������� ������� �� ��������
    -- ---------------------------------------------------------------------------- --
    PROCEDURE Delete_orders (
                 p_task_id     IN INTEGER
              );

    -- ---------------------------------------------------------------------------- --
    -- �������� �/� �� ��������
    -- ---------------------------------------------------------------------------- --
    PROCEDURE Delete_accounts (
                 p_task_id     IN INTEGER
              );

    -- ---------------------------------------------------------------------------- --
    -- �������� ��������� �� ��������
    -- ---------------------------------------------------------------------------- --
    PROCEDURE Delete_contract (
                 p_task_id     IN INTEGER
              );

    -- ---------------------------------------------------------------------------- --
    -- �������� ������ �� ��������
    -- ---------------------------------------------------------------------------- --
    PROCEDURE Delete_data (
                 p_task_id     IN INTEGER
              );
    
END PK05_ACCOUNT_DELETE;
/
CREATE OR REPLACE PACKAGE BODY PK05_ACCOUNT_DELETE
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ����� ������ ��� ���������� � �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Open_task RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Open_task';
    v_task_id     INTEGER;
BEGIN
    -- �������� ����� ������
    SELECT SQ_BILLING_QUEUE_T.NEXTVAL INTO v_task_id FROM DUAL;
    -- ��������� ��� �������� � ������� �����������
    Pk01_Syslog.Write_msg('task_id = '||v_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
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
    v_count       INTEGER := 0;
BEGIN
    -- ������� ������ �� �������
    DELETE FROM PK05_DELETE_QUEUE_T Q
     WHERE Q.TASK_ID = p_task_id;
    v_count := SQL%ROWCOUNT;
--    COMMIT;
    -- ��������� ��� �������� � ������� �����������
    Pk01_Syslog.Write_msg('task_id = '||p_task_id||', deleted '||v_count||' rows', 
                                   c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Task_id = '||p_task_id, c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------------- --
-- ��������� ���������� ������� (ROLLBACK_QUEUE_T) �� �������� ������ 
-- ---------------------------------------------------------------------------- --
PROCEDURE Put_order_to_queue (
             p_task_id  IN INTEGER,
             p_order_id IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Put_order_to_queue';
    v_count      INTEGER;
BEGIN
    --
    INSERT INTO PK05_DELETE_QUEUE_T(TASK_ID, CONTRACT_ID, ACCOUNT_ID, ORDER_ID)
    SELECT DISTINCT p_task_id, NULL, O.ACCOUNT_ID, O.ORDER_ID 
      FROM ORDER_T O
     WHERE O.ORDER_ID   = p_order_id
    ;     
    v_count := SQL%ROWCOUNT;
    IF v_count = 1 THEN
        Pk01_Syslog.Write_msg('order_id='||p_order_id||' add to task '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    ELSE 
        Pk01_Syslog.Write_msg('Error: can not add order_id='||p_order_id||' to task '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    END IF;
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------------- --
-- ��������� ���������� ������� (ROLLBACK_QUEUE_T) �� �������� �/�
-- ---------------------------------------------------------------------------- --
PROCEDURE Put_account_to_queue (
             p_task_id    IN INTEGER,
             p_account_id IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Put_account_to_queue';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id='||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������ ������ � ������� �� ��������
    INSERT INTO PK05_DELETE_QUEUE_T(TASK_ID, CONTRACT_ID, ACCOUNT_ID, ORDER_ID)
    SELECT DISTINCT p_task_id, NULL, O.ACCOUNT_ID, O.ORDER_ID 
      FROM ORDER_T O
     WHERE O.ACCOUNT_ID = p_account_id
    ;     
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' orders add to task '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ���� �� ��������, ������ ���� �/� ������ �� �������� �/� � �������
    SELECT COUNT(*) INTO v_count
      FROM ACCOUNT_PROFILE_T AP, ACCOUNT_PROFILE_T AP1
     WHERE AP.ACCOUNT_ID = p_account_id
       AND AP.ACCOUNT_ID != AP1.ACCOUNT_ID
       AND AP.CONTRACT_ID = AP1.CONTRACT_ID;
    IF v_count = 0 THEN
        -- ������� � ������� � ������� ����
        INSERT INTO PK05_DELETE_QUEUE_T(TASK_ID, CONTRACT_ID, ACCOUNT_ID, ORDER_ID)
        SELECT p_task_id, AP.CONTRACT_ID, AP.ACCOUNT_ID, NULL 
          FROM ACCOUNT_PROFILE_T AP
        WHERE AP.ACCOUNT_ID = p_account_id;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg(v_count||' account and contract add to task '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        -- ��������, ��� � ������� ���
        IF v_count = 0 THEN
            -- ������� ������ ������� ����
            INSERT INTO PK05_DELETE_QUEUE_T(TASK_ID, CONTRACT_ID, ACCOUNT_ID, ORDER_ID)
            VALUES(p_task_id, NULL, p_account_id, NULL ); 
            v_count := SQL%ROWCOUNT;
            Pk01_Syslog.Write_msg(v_count||' accounts add to task '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
    ELSE
        -- ������� ������ ������� ����
        INSERT INTO PK05_DELETE_QUEUE_T(TASK_ID, CONTRACT_ID, ACCOUNT_ID, ORDER_ID)
        VALUES(p_task_id, NULL, p_account_id, NULL ); 
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg(v_count||' accounts add to task '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    END IF;

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------------- --
-- ��������� ���������� ������� (ROLLBACK_QUEUE_T) �� �������� ��������
-- ---------------------------------------------------------------------------- --
PROCEDURE Put_contract_to_queue (
             p_task_id     IN INTEGER,
             p_contract_id IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Put_contract_to_queue';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id='||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������ ������ � ������� �� ��������
    INSERT INTO PK05_DELETE_QUEUE_T(TASK_ID, CONTRACT_ID, ACCOUNT_ID, ORDER_ID)
    SELECT DISTINCT p_task_id, NULL, O.ACCOUNT_ID, O.ORDER_ID 
      FROM ORDER_T O, ACCOUNT_PROFILE_T AP
     WHERE AP.ACCOUNT_ID  = O.ACCOUNT_ID
       AND AP.CONTRACT_ID = p_contract_id
    ;     
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' orders add to task '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������ �� �������� ������� �����
    INSERT INTO PK05_DELETE_QUEUE_T(TASK_ID, CONTRACT_ID, ACCOUNT_ID, ORDER_ID)
    SELECT DISTINCT p_task_id, NULL, AP.ACCOUNT_ID, NULL 
      FROM ACCOUNT_PROFILE_T AP
     WHERE AP.CONTRACT_ID = p_contract_id
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' accounts add to task '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������ �� �������� �������
    INSERT INTO PK05_DELETE_QUEUE_T(TASK_ID, CONTRACT_ID, ACCOUNT_ID, ORDER_ID)
    VALUES(p_task_id, p_contract_id, NULL, NULL ); 
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_count||' contracts add to task '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- ---------------------------------------------------------------------------- --
-- �������� �������� �� ��������
-- ---------------------------------------------------------------------------- --
PROCEDURE Delete_payments (
             p_task_id     IN INTEGER,
             p_period_from IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_payments';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id='||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� �������� ��������
    DELETE FROM PAY_TRANSFER_T T
    WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q, PAYMENT_T P
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = P.ACCOUNT_ID
           AND T.PAYMENT_ID = P.PAYMENT_ID
           AND P.REP_PERIOD_ID > p_period_from
    )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAY_TRANSFER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� �������� ��������
    DELETE FROM PAY_TRANSFER_T T
    WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q, BILL_T B
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = B.ACCOUNT_ID
           AND T.BILL_ID    = B.BILL_ID
           AND B.REP_PERIOD_ID > p_period_from
    )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAY_TRANSFER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� �������
    DELETE FROM PAYMENT_T P
    WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = P.ACCOUNT_ID
      )
      AND P.REP_PERIOD_ID > p_period_from
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAYMENT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------------- --
-- �������� ������ �� ��������
-- ---------------------------------------------------------------------------- --
PROCEDURE Delete_bills (
             p_task_id     IN INTEGER,
             p_period_from IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_bills';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id='||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� ITEMs
    DELETE FROM ITEM_T I
    WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q, BILL_T B
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = B.ACCOUNT_ID
           AND I.BILL_ID    = B.BILL_ID
           AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND B.REP_PERIOD_ID > p_period_from
    )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� INVOICE_ITEMs
    DELETE FROM INVOICE_ITEM_T V
    WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q, BILL_T B
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = B.ACCOUNT_ID
           AND V.BILL_ID    = B.BILL_ID
           AND V.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND B.REP_PERIOD_ID > p_period_from
    )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� �����
    DELETE FROM BILL_T B
    WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = B.ACCOUNT_ID
      )
      AND B.REP_PERIOD_ID > p_period_from
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------------- --
-- �������� ������� �� ��������
-- ---------------------------------------------------------------------------- --
PROCEDURE Delete_orders (
             p_task_id     IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_orders';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id='||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������� �������������� ������������ ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM SERVICE_ALIAS_T SA
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q, ORDER_T O
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ORDER_ID   = O.ORDER_ID
           AND O.SERVICE_ID = SA.SERVICE_ID
           AND Q.ACCOUNT_ID = SA.ACCOUNT_ID     
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SERVICE_ALIAS_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ���������� � �������� SLA 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    /*
    DELETE FROM SLA_PERCENT_T SP
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q, ORDER_BODY_T OB
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ORDER_ID   = OB.ORDER_ID
           AND SP.ORDER_BODY_ID = OB.ORDER_BODY_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SLA_PERCENT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    */
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ���������� � ����������� �������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM DOWNTIME_T DT
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ORDER_ID   = DT.ORDER_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('DOWNTIME_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ���������� ����� ��� ��������� � ����������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM ORDER_BODY_T OB
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ORDER_ID   = OB.ORDER_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� �������������� ���������� ������ (�� ������ ������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM ORDER_INFO_T OI
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ORDER_ID   = OI.ORDER_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_INFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ���������� ������ (�� ������ ������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM ORDER_LOCK_T OL
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ORDER_ID   = OL.ORDER_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_LOCK_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ���������� ������ (�� ������ ������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM ORDER_PHONES_T OP
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ORDER_ID   = OP.ORDER_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_PHONES_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DELETE FROM ORDER_T O
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ORDER_ID   = O.ORDER_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ------------------------------------------------------------ --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------------- --
-- �������� �/� �� ��������
-- ---------------------------------------------------------------------------- --
PROCEDURE Delete_accounts (
             p_task_id     IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_accounts';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id='||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ------------------------------------------------------------ --

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ��������� ������� ���������� �/�
    DELETE FROM ACCOUNT_DOCUMENTS_T AD
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = AD.ACCOUNT_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_DOCUMENTS_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������ �/�
    DELETE FROM ACCOUNT_CONTACT_T AC
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = AC.ACCOUNT_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_CONTACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ��������� ������
    DELETE FROM BILLINFO_T BI
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = BI.ACCOUNT_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLINFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������� �� �/�
    DELETE FROM REP_PERIOD_INFO_T RP
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = RP.ACCOUNT_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('REP_PERIOD_INFO_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

   -- ��������� ������� �/� �� ��������� �������
   INSERT INTO PK05_ACCOUNT_PROFILE_TMP
   SELECT * FROM ACCOUNT_PROFILE_T AP
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = AP.ACCOUNT_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK05_ACCOUNT_PROFILE_TMP '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������� �������� �����
    DELETE FROM ACCOUNT_PROFILE_T AP
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = AP.ACCOUNT_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_PROFILE_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������� �����
    DELETE FROM ACCOUNT_T A
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = A.ACCOUNT_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �����������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ����� �����������
    DELETE
      FROM CUSTOMER_BANK_T CB
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q, PK05_ACCOUNT_PROFILE_TMP AP
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = AP.ACCOUNT_ID
           AND NOT EXISTS (
              SELECT * FROM ACCOUNT_PROFILE_T AP1
               WHERE AP.ACCOUNT_ID != AP1.ACCOUNT_ID
                 AND AP.CUSTOMER_ID = AP1.CUSTOMER_ID 
           )
           AND CB.CUSTOMER_ID = AP.CUSTOMER_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CUSTOMER_BANK_T '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� �����������
    DELETE
      FROM CUSTOMER_T CS
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q, PK05_ACCOUNT_PROFILE_TMP AP
         WHERE Q.TASK_ID    = p_task_id
           AND Q.ACCOUNT_ID = AP.ACCOUNT_ID
           AND NOT EXISTS (
              SELECT * FROM ACCOUNT_PROFILE_T AP1
               WHERE AP.ACCOUNT_ID != AP1.ACCOUNT_ID
                 AND AP.CUSTOMER_ID = AP1.CUSTOMER_ID 
           )
           AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CUSTOMER_T '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ------------------------------------------------------------ --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------------- --
-- �������� ��������� �� ��������
-- ---------------------------------------------------------------------------- --
PROCEDURE Delete_contract (
             p_task_id     IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_contract';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ------------------------------------------------------------ --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �������� �������� � ��������
    DELETE 
      FROM SALE_CURATOR_T SC
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID     = p_task_id
           AND Q.CONTRACT_ID = SC.CONTRACT_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('SALE_CURATOR_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� �������� �������� � ������� ��������
    DELETE 
      FROM BILLING_CURATOR_T BC
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID     = p_task_id
           AND Q.CONTRACT_ID = BC.CONTRACT_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_CURATOR_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ��������
    DELETE FROM COMPANY_T C
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID     = p_task_id
           AND Q.CONTRACT_ID = C.CONTRACT_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('COMPANY_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� �������
    DELETE FROM CONTRACT_T C
     WHERE EXISTS (
        SELECT * FROM PK05_DELETE_QUEUE_T Q
         WHERE Q.TASK_ID     = p_task_id
           AND Q.CONTRACT_ID = C.CONTRACT_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('CONTRACT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ------------------------------------------------------------ --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------------- --
-- �������� ������ �� ��������
-- ---------------------------------------------------------------------------- --
PROCEDURE Delete_data (
             p_task_id     IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_data';
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id='||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- �������� �������� �� ��������
    Delete_payments ( p_task_id, 201504 );

    -- �������� ������ �� ��������
    Delete_bills ( p_task_id, 201504 );

    -- �������� ������� �� ��������
    Delete_orders ( p_task_id );

    -- �������� �/� �� ��������
    Delete_accounts ( p_task_id );

    -- �������� ��������� �� ��������
    Delete_contract ( p_task_id );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


END PK05_ACCOUNT_DELETE;
/
