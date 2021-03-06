CREATE OR REPLACE PACKAGE PK38_BILLING_SLA_K
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK38_BILLING_SLA_K';
    -- ==============================================================================
    type t_refc is ref cursor;
    -- 
    -- ������ ����� ����������� �������� ������ �� ������� BRM_SLA_K_T
    -- �������� Coca Cola
    -- �/� 'MS002234'
    -- ��������!!! ������� ������� ����� ���������� ���������
    
    -- ������� ������ ����������
    c_ST_NOT_PROC     CONSTANT INTEGER := 0;  -- ����������� �� ������������� ��������� ��������
    c_ST_ACC_BIND     CONSTANT INTEGER := 1;  -- �/� �������� � ������ � ��������
    c_ST_OB_BIND      CONSTANT INTEGER := 2;  -- ���������� ������ ��������� � ������ � ��������
    c_ST_CALK_PRC     CONSTANT INTEGER := 3;  -- ������ ������� ������ ��� ������ ������    
    c_ST_BILL_BIND    CONSTANT INTEGER := 4;  -- ����������� �������� �������� � ������
    c_ST_ITEM_READY   CONSTANT INTEGER := 5;  -- ������� �������� � ������ ������������

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� � ����� ������� ����������� �� ������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Period_processing( p_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� � ����� ����������� �� ������� ��� ��������� ������
    PROCEDURE Task_processing( p_task_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������� �������� �� ��������� �������
    -- ���������� ����� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Load_SLA_K(p_period_id IN INTEGER, p_task_id IN INTEGER);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� id ������ � �/� � ���������� ������ ��� BRM_SLA_K_T
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Bind_orders( p_task_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ �������� SLA, �������� ������������� ����������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Calc_SLA_K( p_task_id   IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �������, ��� ��������� ��������� ������ 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_period_queue( 
                 p_task_id   IN INTEGER, 
                 p_bill_type IN VARCHAR2 DEFAULT 'B' );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���������� �� ������� � ������ 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_items( p_task_id   IN INTEGER );

    -- --------------------------------------------------------------------- --
    --              � � � � � �    � � �    � � � � � � � �                  --
    -- --------------------------------------------------------------------- --
    -- ��������������� ����������� �� �������
    PROCEDURE Rollback_downtimes( p_task_id IN INTEGER );
    --
    -- ��������������� ����������� �� �������
    PROCEDURE Recharge_downtimes( p_task_id IN INTEGER );
    
END PK38_BILLING_SLA_K;
/
CREATE OR REPLACE PACKAGE BODY PK38_BILLING_SLA_K
IS

-- ---------------------------------------------------------------------- --
-- ��������� � ����� ������� ����������� �� ������� 
-- ---------------------------------------------------------------------- --
PROCEDURE Period_processing( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Downtime_processing';
    v_task_id    INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_task_id := Pk30_Billing_Queue.Open_task;
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1) ���������� ������� �������� �� ��������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Load_SLA_K( p_period_id, v_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2) ���������� ������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Task_processing( v_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3) ������ ������� ������
    Pk30_Billing_Queue.Close_task( v_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------- --
-- ��������� � ����� ����������� �� ������� 
-- ---------------------------------------------------------------------- --
PROCEDURE Task_processing( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Task_processing';
BEGIN

    Pk01_Syslog.Write_msg('Start, task_id='||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1) ����������� id ������ � �/� � ���������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Bind_orders( p_task_id );

    -- ---------------------------------------------------------------------- --
    -- 2) ������ �������� SLA, �������� ������������� ����������� 
    -- ---------------------------------------------------------------------- --
    Calc_SLA_K( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3) ������� �������, ��� ��������� ���������� ������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Make_period_queue( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 4) ���������������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk30_Billing_Base.Rollback_bills(p_task_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 5) ������� ���������� �� ������� � ������ 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Make_items( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 6) ��������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk30_Billing_Base.Make_bills( p_task_id );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ������� �������� �� ��������� �������
-- ���������� ����� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Load_SLA_K(p_period_id IN INTEGER, p_task_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Load_SLA_K';
    v_period_id     INTEGER := NULL; -- ��������� �� ����� ��������
    v_count         INTEGER;
    v_year          INTEGER;
    v_month         INTEGER;
    v_task_id       INTEGER;
BEGIN
    -- ���������� ������� �������� �� ��������� �������
    INSERT INTO BRM_SLA_K_T (VPN_NAME, ORDER_NO, CLASS, KD, YEAR, MONTH)
    SELECT AGENT_NAME, ORDER_NO, 
           CASE
             WHEN CLASS_ID IS NOT NULL THEN CLASS_ID
             WHEN CLASS_ID IS NULL AND CLASS = 'Standard'  THEN 1
             WHEN CLASS_ID IS NULL AND CLASS = 'Premium-3' THEN 3
             WHEN CLASS_ID IS NULL AND CLASS = 'Realtime'  THEN 5
           END CLASS_ID, 
           KD, 
           SUBSTR(p_period_id,1,4), SUBSTR(p_period_id,5)
      FROM BRM_SLA_K_TMP;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SLA_K_T(task_id = '||v_task_id||' ) '||v_count||' rows inserted' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������ ��������� �������
    DELETE FROM BRM_SLA_K_TMP;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� id ��� ����� ����������� �����
    UPDATE BRM_SLA_K_T S 
       SET S.SLA_ID    = SQ_SLA_K_ID.NEXTVAL,
           S.KD_NUM    = TO_NUMBER(S.KD),
           S.DATE_FROM = TO_DATE(MONTH||'.'||YEAR,'mm.yyyy'),
           S.DATE_TO   = ADD_MONTHS(TO_DATE(MONTH||'.'||YEAR,'mm.yyyy'),1)-1/86400,
           S.TASK_ID   = p_task_id,
           S.REP_PERIOD_ID = p_period_id
     WHERE S.SLA_ID IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SLA_K_T(task_id = '||v_task_id||' ) '||v_count||' rows mark' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� id ������ � �/� � ���������� ������ ��� BRM_SLA_K_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Bind_orders( p_task_id IN INTEGER )
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Bind_orders';
    v_rate_rule_id CONSTANT INTEGER      := 2414;
    v_account_id   CONSTANT INTEGER      := 2403066;
    v_account_no   CONSTANT VARCHAR2(20) := 'MS002234';
    v_count        INTEGER;
BEGIN
    -- --------------------------------------------------------------------- --
    -- ������������ ������ ��� ������������� ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Coca-Cola (�/� MS002234), �� ������� ���� �� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE ORDER_BODY_T OB 
       SET OB.RATE_RULE_ID = v_rate_rule_id
     WHERE OB.ORDER_ID IN (
        SELECT O.ORDER_ID 
          FROM ORDER_T O
         WHERE O.ACCOUNT_ID IN (
            SELECT A.ACCOUNT_ID FROM ACCOUNT_T A
             WHERE A.ACCOUNT_NO = v_account_no
         )
     )
     AND OB.CHARGE_TYPE   = 'SLA'
     AND (OB.RATE_RULE_ID IS NULL OR OB.RATE_RULE_ID != v_rate_rule_id);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T = '||v_count||' rows CocaCola', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ����� � �/�
    MERGE INTO BRM_SLA_K_T S
    USING (
        SELECT S.SLA_ID, O.ORDER_NO, O.ACCOUNT_ID, O.ORDER_ID 
          FROM ORDER_T O, BRM_SLA_K_T S
         WHERE O.ACCOUNT_ID = v_account_id
           AND O.ORDER_NO   = S.ORDER_NO 
           AND S.TASK_ID    = p_task_id 
           AND S.STATUS  IS NULL
    ) O
    ON (
       O.SLA_ID = S.SLA_ID 
    )
    WHEN MATCHED THEN UPDATE  
                         SET S.ORDER_ID      = O.ORDER_ID, 
                             S.ACCOUNT_ID    = O.ACCOUNT_ID,
                             S.STATUS        = c_ST_ACC_BIND,
                             S.STATUS_DATE   = SYSDATE;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SLA_K_T(task_id = '||p_task_id||' ) '||v_count||' rows bind' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ������ �������� �������
    UPDATE BRM_SLA_K_T S
       SET S.STATUS  = -c_ST_ACC_BIND, 
           S.NOTES = '����� �� ������ ��� �� ����������� MS002234 (CocaCola)'
     WHERE S.TASK_ID = p_task_id
       AND ( S.ORDER_ID IS NULL OR S.ACCOUNT_ID IS NULL );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SLA_K_T(task_id = '||p_task_id||' ) '||v_count||' rows not bind' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ���������� ������ SLA
    MERGE INTO BRM_SLA_K_T S
    USING (
            SELECT ROW_NUMBER() OVER (PARTITION BY S.SLA_ID ORDER BY OB.DATE_FROM DESC) RN, 
                   S.SLA_ID, OB.ORDER_BODY_ID, 
                   OB.RATE_RULE_ID, OB.RATEPLAN_ID, OB.CHARGE_TYPE 
              FROM BRM_SLA_K_T S, ORDER_BODY_T OB
             WHERE S.ORDER_ID      = OB.ORDER_ID
               AND OB.CHARGE_TYPE  = 'SLA'
               AND OB.RATE_RULE_ID = v_rate_rule_id
               AND OB.DATE_FROM   <= S.DATE_TO
               AND (OB.DATE_TO IS NULL OR S.DATE_FROM <= OB.DATE_TO)
               AND S.TASK_ID       = p_task_id
               AND S.STATUS        = c_ST_ACC_BIND
    ) OB
    ON (
        OB.SLA_ID = S.SLA_ID
    )
    WHEN MATCHED THEN UPDATE SET S.ORDER_BODY_ID   = OB.ORDER_BODY_ID,
                                 S.SLA_RATEPLAN_ID = OB.RATEPLAN_ID,
                                 S.RATE_RULE_ID    = OB.RATE_RULE_ID, 
                                 S.STATUS          = c_ST_OB_BIND,
                                 S.STATUS_DATE     = SYSDATE
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SLA_K_T(task_id = '||p_task_id||' ) '||v_count||' rows SLA_K set' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� �� ������� ������ ��� ���������
    UPDATE BRM_SLA_K_T S 
       SET S.NOTES   = '� ������ ���������� ��� ������� ���������� ������ REC/MIN',
           S.STATUS  = -c_ST_OB_BIND 
     WHERE S.STATUS  = c_ST_ACC_BIND 
       AND S.TASK_ID = p_task_id
       AND NOT EXISTS (
           SELECT * 
             FROM ORDER_BODY_T OB
            WHERE OB.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_MIN,
                                     Pk00_Const.c_CHARGE_TYPE_REC)
              AND OB.ORDER_ID   = S.ORDER_ID
              AND OB.DATE_FROM <= S.DATE_TO
              AND (OB.DATE_TO IS NULL OR S.DATE_FROM <= OB.DATE_TO)
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SLA_K_T.OB_ID = '||v_count||' rows OB.MIN/REC not found', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- ����������� ������ �������� ����������� �����
    UPDATE BRM_SLA_K_T S
       SET S.STATUS  = -c_ST_OB_BIND, 
           S.NOTES   = '���������� ������ SLA.RATERULE_ID = 2414 �� ������� � ��������� ���������� �������',
           S.STATUS_DATE = SYSDATE
     WHERE S.TASK_ID = p_task_id
       AND S.ORDER_BODY_ID IS NULL
       AND S.STATUS > 0;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SLA_K_T(task_id = '||p_task_id||' ) '||v_count||' rows IDL not found' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��� ��������� �/�, ����������� �� ������������� ��������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE BRM_SLA_K_T S 
       SET S.NOTES  = '����������� �������� ��������� ��������� ��������',
           S.STATUS = c_ST_NOT_PROC,
           S.STATUS_DATE = SYSDATE
     WHERE NOT EXISTS (
           SELECT * 
             FROM ACCOUNT_T A
            WHERE A.ACCOUNT_ID = S.ACCOUNT_ID
              AND A.IDL_ENB = 'Y' 
       )
       AND S.ACCOUNT_ID IS NOT NULL
       AND S.TASK_ID = p_task_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SLA_K_T = '||v_count||' rows ACCOUNT_T.IDL_ENB=N', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------- --
-- ������ �������� SLA, �������� ������������� ����������� 
-- ---------------------------------------------------------------------- --
PROCEDURE Calc_SLA_K( p_task_id   IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Calc_SLA_K';
    v_charge_type  CONSTANT VARCHAR2(3)  := 'SLA';
    v_rate_rule_id CONSTANT INTEGER      := 2414;
    v_count      INTEGER;
BEGIN
    -- ������ �������� ����������� �� �������
    MERGE INTO BRM_SLA_K_T S
    USING (
        SELECT S.SLA_ID, 
               SP.SLA_PERCENT
          FROM SLA_PERCENT_T SP, BRM_SLA_K_T S 
         WHERE S.SLA_RATEPLAN_ID = SP.RATEPLAN_ID
           AND SP.K_MIN         <= S.KD_NUM * 100
           AND SP.K_MAX         >  S.KD_NUM * 100
           AND S.TASK_ID         = p_task_id
           AND S.RATE_RULE_ID    = v_rate_rule_id
           AND S.STATUS          = c_ST_OB_BIND
    ) DP
    ON (
        S.SLA_ID = DP.SLA_ID
    )
    WHEN MATCHED THEN UPDATE 
                         SET S.SLA_PERCENT = DP.SLA_PERCENT, 
                             S.STATUS_DATE = SYSDATE,
                             S.STATUS      = c_ST_CALK_PRC
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SLA_K_T(task_id = '||p_task_id||'): '||v_count||' rows SLA_K - ok', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������ ������� ����������� �� �������
    UPDATE BRM_SLA_K_T S 
       SET S.STATUS = -c_ST_CALK_PRC, 
           S.STATUS_DATE = SYSDATE, S.NOTES = '�� ������ SLA_PERCENT'
     WHERE S.TASK_ID      = p_task_id
       AND S.RATE_RULE_ID = v_rate_rule_id
       AND S.SLA_PERCENT IS NULL
       AND S.STATUS IN ( c_ST_OB_BIND, c_ST_CALK_PRC );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||'): '||v_count||' rows SLA_K - error', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------- --
-- ������� �������, ��� ��������� ���������� ������� 
-- ---------------------------------------------------------------------- --
PROCEDURE Make_period_queue( 
             p_task_id   IN INTEGER, 
             p_bill_type IN VARCHAR2 DEFAULT 'B' )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_period_queue';
    v_count      INTEGER;
BEGIN
    -- ��������� ������� � ��������
    INSERT INTO BILLING_QUEUE_T( 
           BILL_ID, ACCOUNT_ID, TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID 
    )
    SELECT DISTINCT 
           B.BILL_ID, B.ACCOUNT_ID, p_task_id TASK_ID, B.REP_PERIOD_ID, B.REP_PERIOD_ID
      FROM BILL_T B, BRM_SLA_K_T S
     WHERE B.REP_PERIOD_ID = S.REP_PERIOD_ID
       AND B.ACCOUNT_ID    = S.ACCOUNT_ID
       AND S.TASK_ID       = p_task_id
       AND S.STATUS        = c_ST_BILL_BIND -1
       AND B.BILL_TYPE     = p_bill_type;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T(task_id = '||p_task_id||'): '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ����������� id ������
    MERGE INTO BRM_SLA_K_T S
    USING (
    SELECT S.SLA_ID, Q.BILL_ID 
      FROM BRM_SLA_K_T S, BILLING_QUEUE_T Q
     WHERE S.ACCOUNT_ID = Q.ACCOUNT_ID
       AND S.TASK_ID       = p_task_id
       AND S.STATUS        = c_ST_BILL_BIND -1
    ) Q
    ON (
      S.SLA_ID = Q.SLA_ID
    )
    WHEN MATCHED THEN UPDATE SET S.BILL_ID = Q.BILL_ID,
                                 S.STATUS  = c_ST_BILL_BIND,
                                 S.STATUS_DATE = SYSDATE;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SLA_K_T(task_id = '||p_task_id||'): '||v_count||' bills exists', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������� �������� � ������
    UPDATE BRM_SLA_K_T S 
       SET S.STATUS = -c_ST_BILL_BIND, 
           S.NOTES = DECODE(S.NOTES, NULL, NULL, S.NOTES||', ') || '�� ������ ����',
           S.STATUS_DATE = SYSDATE
     WHERE S.BILL_ID IS  NULL
       AND S.TASK_ID = p_task_id
       AND S.STATUS  = c_ST_BILL_BIND -1;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SLA_K_T(task_id = '||p_task_id||'): '||v_count||' bills not found', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
     
EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------- --
-- ������� ���������� �� ������� � ������������ ����������� 
-- ---------------------------------------------------------------------- --
PROCEDURE Make_items( p_task_id   IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_items';
    v_count      INTEGER;
    v_rate_rule_id CONSTANT INTEGER := 2414;
    v_item_id    INTEGER;    
BEGIN
    -- ����������� ������ � ����������� ����� REC/MIN -> SLA
    MERGE INTO ORDER_BODY_T OB
    USING (
        SELECT OB.ORDER_BODY_ID, OBR.CURRENCY_ID, OBR.TAX_INCL 
          FROM BRM_SLA_K_T S, ORDER_BODY_T OBR, ORDER_BODY_T OB
         WHERE S.TASK_ID  = p_task_id
           AND S.ORDER_ID = OBR.ORDER_ID
           AND S.ORDER_ID = OB.ORDER_ID
           AND (
             OBR.CURRENCY_ID != OB.CURRENCY_ID OR OB.CURRENCY_ID IS NULL OR
             OBR.TAX_INCL    != OB.TAX_INCL    OR OB.TAX_INCL    IS NULL
           ) 
           AND OBR.CHARGE_TYPE IN ('REC','MIN')
           AND OB.CHARGE_TYPE  IN ('SLA')
           AND OB.DATE_FROM   <= S.DATE_TO
           AND (OB.DATE_TO IS NULL OR S.DATE_FROM <= OB.DATE_TO)
           AND OBR.DATE_FROM  <= S.DATE_TO
           AND (OBR.DATE_TO IS NULL OR S.DATE_FROM <= OBR.DATE_TO)
           AND S.STATUS > 0
         GROUP BY OB.ORDER_BODY_ID, OBR.CURRENCY_ID, OBR.TAX_INCL
    ) OBR
    ON (
        OB.ORDER_BODY_ID = OBR.ORDER_BODY_ID 
    )
    WHEN MATCHED THEN UPDATE SET OB.CURRENCY_ID = OBR.CURRENCY_ID,
                                 OB.TAX_INCL    = OBR.TAX_INCL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Set currency_id REC/MIN -> SLA for '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ------------------------------------------------------------------------ --
    -- ��������� item-� ��� ����������� SLA
    v_count := 0;
    --
    FOR rd IN (
      SELECT S.BILL_ID, S.REP_PERIOD_ID,  
             S.ORDER_ID, S.ORDER_BODY_ID, 
             SUM(SP.SLA_PERCENT) PERCENT,
             MIN(S.DATE_FROM) DATE_FROM,
             MAX(S.DATE_TO) DATE_TO
        FROM SLA_PERCENT_T SP, BRM_SLA_K_T S 
       WHERE S.SLA_RATEPLAN_ID = SP.RATEPLAN_ID
         AND SP.K_MIN         <= S.KD_NUM * 100
         AND SP.K_MAX         >  S.KD_NUM * 100
         AND S.TASK_ID         = p_task_id
         AND S.RATE_RULE_ID    = v_rate_rule_id
         AND S.STATUS          = c_ST_ITEM_READY-1
       GROUP BY S.BILL_ID, S.REP_PERIOD_ID, 
                S.ORDER_ID, S.ORDER_BODY_ID
    ) LOOP
        v_item_id := Pk02_Poid.Next_item_id;
        -- ��������� item-� ��� ����������� SLA
        INSERT INTO ITEM_T (
            BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
            ORDER_ID, ORDER_BODY_ID,
            SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
            ITEM_TOTAL, ITEM_CURRENCY_ID, RECVD, DATE_FROM, DATE_TO, 
            ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
            REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, DESCR
        )
        SELECT rd.bill_id, rd.rep_period_id, v_item_id, 'B', NULL,
               O.ORDER_ID, OB.ORDER_BODY_ID,
               O.SERVICE_ID, OB.SUBSERVICE_ID, OB.CHARGE_TYPE,
               -I.ITEM_TOTAL * rd.percent / 100, 
               OB.CURRENCY_ID, 0, rd.date_from, rd.date_to,
               'OPEN', SYSDATE, SYSDATE,
               0, 0, OB.TAX_INCL, NULL, NULL, NULL 
          FROM ORDER_BODY_T OB, ORDER_T O, ITEM_T I
         WHERE O.ORDER_ID       = rd.order_id
           AND OB.ORDER_ID      = O.ORDER_ID
           AND OB.ORDER_BODY_ID = rd.order_body_id
           AND I.BILL_ID        = rd.bill_id
           AND I.REP_PERIOD_ID  = rd.rep_period_id
           AND I.ORDER_ID       = O.ORDER_ID
           --AND I.ORDER_BODY_ID  = OB.ORDER_BODY_ID
           AND I.CHARGE_TYPE    = 'REC'
           AND ( rd.date_from BETWEEN I.DATE_FROM AND I.DATE_TO OR
                 I.DATE_FROM  BETWEEN rd.date_from AND rd.date_to )
        ;
        v_count := v_count + SQL%ROWCOUNT;
        -- ����������� ������� �������� �������
        UPDATE BRM_SLA_K_T S 
           SET S.STATUS = DECODE(v_count, 1, c_ST_ITEM_READY, -c_ST_ITEM_READY),
               S.NOTES = CASE
                         WHEN v_count = 1 THEN 'OK'
                         WHEN v_count = 0 THEN '������ �������� item-�' 
                         END 
         WHERE S.TASK_ID  = 1
           AND S.STATUS   = c_ST_ITEM_READY-1
           AND S.ORDER_ID = rd.order_id
           AND S.ORDER_BODY_ID = rd.order_body_id;

    END LOOP;
    Pk01_Syslog.Write_msg('ITEM_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
--              � � � � � �    � � �    � � � � � � � �                  --
-- --------------------------------------------------------------------- --
-- ��������������� ����������� �� �������
--
PROCEDURE Rollback_downtimes( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_downtimes';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������� Item-� ����������� �� �������
    DELETE FROM ITEM_T I
     WHERE I.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_SLA)
       AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN 
       AND I.ITEM_TYPE   = Pk00_Const.c_ITEM_TYPE_BILL  -- ������������� �� �������
       AND EXISTS (
          SELECT * FROM BRM_SLA_K_T S
           WHERE S.TASK_ID = p_task_id
             AND S.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND S.ITEM_ID = I.ITEM_ID
             AND S.ITEM_ID IS NOT NULL
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ���������� ������� �������� �� ������� ����������
    UPDATE BRM_SLA_K_T S
       SET ACCOUNT_ID     = NULL, 
           ORDER_ID       = NULL, 
           ORDER_BODY_ID  = NULL, 
           SLA_RATEPLAN_ID= NULL, 
           RATE_RULE_ID   = NULL, 
           BILL_ID        = NULL, 
           ITEM_ID        = NULL, 
           STATUS         = NULL, 
           STATUS_DATE    = SYSDATE, 
           NOTES          = NULL
     WHERE S.TASK_ID = p_task_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_SLA_K_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop. '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--
-- ��������������� ����������� �� �������
--
PROCEDURE Recharge_downtimes( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Recharge_downtimes';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --   
    Pk01_Syslog.Write_msg('Start, task_id='||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1) ������� �������, ��� ��������� ���������� ������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Make_period_queue( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2) ���������������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk30_Billing_Base.Rollback_bills(p_task_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3) ������� ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Rollback_downtimes( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 4) ��������� ������ ������ �������� (������ biling_quque_t)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk30_Billing_Queue.Close_task( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 4) ��������� � ����� ����������� �� ������� (����� ������ �� ������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Task_processing( p_task_id );

    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


END PK38_BILLING_SLA_K;
/
