CREATE OR REPLACE PACKAGE PK38_BILLING_DOWNTIME
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK38_BILLING_DOWNTIME';
    -- ==============================================================================
    type t_refc is ref cursor;
    -- 
    -- ������ ��� ���������� ����������� �� �������
    c_TASK_DOWNTIME_ID CONSTANT INTEGER := 0;
    --
    -- ������ ����� ����������� �������� ������
    -- ������� : DOWNTIME_T, SLA_PERCENT_T
    -- ��������!!! ������� ������� ����� ���������� ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_FLAG_H  CONSTANT INTEGER := 0;  -- ������� ������ � �����
    c_FLAG_K  CONSTANT INTEGER := 1;  -- ����� ����������� ����������� ������

    c_DEFAULT_FREE_DOWNTIME CONSTANT INTEGER := 43; -- ����������� �������� ����������������� �������
    
    -- ������� ������ ����������
    c_DS_DISABLE         CONSTANT INTEGER := -5;  -- ����������� �������� ��� �/� ���������
    c_DS_NULL            CONSTANT INTEGER := -4;  -- ������� �������
    c_DS_ERROR           CONSTANT INTEGER := -3;  -- ������ ������
    c_DS_BIND_OB_ER      CONSTANT INTEGER := -2;  -- ������ �������� � ���������� ������ ������
    c_DS_BIND_O_ER       CONSTANT INTEGER := -1;  -- ������ �������� � ������
    c_DS_BIND_ORDER      CONSTANT INTEGER :=  0;  -- ������ � ������� ��������� � ������
    c_DS_BIND_ORDER_BODY CONSTANT INTEGER :=  1;  -- ������ � ������� ��������� � ���������� ������ ������
    c_DS_OK              CONSTANT INTEGER :=  2;  -- ���������� ���������

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� � ����� ������� ����������� �� ������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Period_processing( p_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� � �������, �/�, �������� ������������ ������ 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Bind_processing( p_task_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� � ����� ����������� �� ������� ��� ��������� ������
    PROCEDURE Task_processing( p_task_id   IN INTEGER );
                 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������� �������� �� ��������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION  Load_Downtime ( p_period_id IN INTEGER ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� id ������ � �/� � ���������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Bind_orders( p_task_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� Item-� ��� ������� ��������� ������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Bind_items( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ������ �������� � Item-�
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Bills_items_error ( 
                   p_recordset OUT t_refc, 
                   p_task_id   IN INTEGER 
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ������� �������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Calc_IDL( p_task_id   IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ �������� � �������, ��� ������ ����������������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Calc_IDL_MIN( p_task_id   IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ �������� SLA, �������� � ����� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Calc_SLA_H( p_task_id   IN INTEGER );

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

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ����� � ����� INFRANET 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_report( p_period_id   IN INTEGER );

    -- --------------------------------------------------------------------- --
    --              � � � � � �    � � �    � � � � � � � �                  --
    -- --------------------------------------------------------------------- --
    -- ��������������� ����������� �� �������
    PROCEDURE Rollback_downtimes( p_task_id IN INTEGER );
    --
    --
    -- ��������������� ����������� �� �������
    --
    PROCEDURE Recharge_downtimes( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ � ��������� ����������� �� ������� ��� �����-����
    -- �����-���� ������ ���� � ���������: 'OPEN'
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Recharge_downtime_for_debet( 
                 p_dbt_bill_id    IN INTEGER,  -- id - ���������� �����, � ������� ������� ������
                 p_dbt_period_id  IN INTEGER,  -- ������ ���������� �����
                 p_crd_period_id  IN INTEGER   -- ������ ����������� �����, ��� �������� ������������� ������
              );
    
    -- ������������ ������ � ��������, ������ � �������� �����������, � ����������� ���������
    -- ��� ����� ��� ���������� �������� � ���������/���������������� ������ 
    PROCEDURE Dup_downtimes( 
                 p_src_task_id IN INTEGER,  -- �������� ������ ��� ������������ �������
                 p_dst_task_id IN INTEGER   -- �������� ����� ��������� ������ �� ������������������
              );
    
END PK38_BILLING_DOWNTIME;
/
CREATE OR REPLACE PACKAGE BODY PK38_BILLING_DOWNTIME
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
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1) ���������� ������� �������� �� ��������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_task_id := Load_Downtime( p_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2) �������� � �������, �/�, �������� ������������ ������ 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Bind_processing( v_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3) ���������� ������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Task_processing( v_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3) ������ ������� ������
    Pk30_Billing_Queue.Close_task( v_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- 4. ��������� ����� � ����� INFRANET 
    Make_report( p_period_id );
    
EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------- --
-- �������� � �������, �/�, �������� ������������ ������ 
-- ---------------------------------------------------------------------- --
PROCEDURE Bind_processing( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bind_processing';
BEGIN

    Pk01_Syslog.Write_msg('Start, task_id='||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1) ����������� id ������ � �/� � ���������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Bind_orders( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2) ����������� Item-� ��� ������� ��������� ������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Bind_items( p_task_id );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- ---------------------------------------------------------------------- --
-- ��������� � ����� ����������� �� ������� 
-- ---------------------------------------------------------------------- --
PROCEDURE Task_processing( p_task_id   IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Task_processing';
BEGIN

    Pk01_Syslog.Write_msg('Start, task_id='||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3) ������ ������� �������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Calc_IDL( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 4) ������ �������� � �������, ��� ������ ����������������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Calc_IDL_MIN( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 5) ������ �������� SLA, �������� � ����� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Calc_SLA_H( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 7) ������� �������, ��� ��������� ���������� ������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Make_period_queue( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 8) ���������������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk30_Billing_Base.Rollback_bills(p_task_id);

    UPDATE BRM_DOWNTIME_T D 
       SET D.STATUS      = 6,
           D.STATUS_DATE = SYSDATE
     WHERE D.TASK_ID     = p_task_id
       AND D.STATUS      = 5;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 9) ������� ���������� �� ������� � ������ 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Make_items( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 10) ��������� �����
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
FUNCTION Load_Downtime(p_period_id IN INTEGER) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Load_Downtime';
    v_period_id     INTEGER := NULL; -- ��������� �� ����� ��������
    v_count         INTEGER;
    v_task_id       INTEGER;
BEGIN
    -- �������� ����� ������
    v_task_id := Pk30_Billing_Queue.Open_task; 
    -- ����������� ����� ������ � �������� �������
    UPDATE INFRANET.BRM_DOWNTIME_TMP SET TASK_ID = v_task_id
     WHERE TASK_ID IS NULL
       AND PERIOD_ID = p_period_id;
    -- ��������� ������ � ������� ��������
    INSERT INTO BRM_DOWNTIME_T (
        TASK_ID, DOWNTIME_ID, PERIOD_ID, 
        ORDER_NO, DATE_FROM, DATE_TO, MINUTES, K_SLA
    )
    SELECT 
        TASK_ID, DOWNTIME_ID, PERIOD_ID, 
        TRIM(ORDER_NO), DATE_FROM, DATE_TO, MINUTES, K_SLA
      FROM INFRANET.BRM_DOWNTIME_TMP
     WHERE TASK_ID = v_task_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||v_task_id||' ) '||v_count||' rows inserted' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    return v_task_id;
EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� id ������ � �/� � ���������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Bind_orders( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bind_orders';
    v_count      INTEGER;
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ����� � �/�
    MERGE INTO BRM_DOWNTIME_T D
    USING (
       SELECT D.DOWNTIME_ID, D.ORDER_NO, O.ORDER_ID, O.ACCOUNT_ID, OI.DOWNTIME_FREE 
         FROM BRM_DOWNTIME_T D, ORDER_T O, ORDER_INFO_T OI
        WHERE D.ORDER_NO = O.ORDER_NO
          AND O.ORDER_ID = OI.ORDER_ID(+)
          AND D.TASK_ID  = p_task_id
    ) O
    ON (
        D.DOWNTIME_ID = O.DOWNTIME_ID
    )
    WHEN MATCHED THEN UPDATE 
                         SET D.ORDER_ID      = O.ORDER_ID, 
                             D.ACCOUNT_ID    = O.ACCOUNT_ID,
                             D.DOWNTIME_FREE = O.DOWNTIME_FREE,
                             D.STATUS        = 1,
                             D.STATUS_DATE   = SYSDATE;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||' ) '||v_count||' rows bind' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ������ �������� �������
    UPDATE BRM_DOWNTIME_T D
       SET D.STATUS  = -1, D.NOTES = '����� �� ������'
     WHERE D.TASK_ID = p_task_id
       AND ( D.ORDER_ID IS NULL OR D.ACCOUNT_ID IS NULL );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||' ) '||v_count||' rows not bind' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ���������� ������ SLA
    MERGE INTO BRM_DOWNTIME_T D
    USING (
        SELECT DOWNTIME_ID, ORDER_BODY_ID, RATE_RULE_ID, RATEPLAN_ID, CHARGE_TYPE
          FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY D.DOWNTIME_ID ORDER BY OB.DATE_FROM DESC) RN, 
                   D.DOWNTIME_ID, OB.ORDER_BODY_ID, 
                   OB.RATE_RULE_ID, OB.RATEPLAN_ID, OB.CHARGE_TYPE 
              FROM BRM_DOWNTIME_T D, ORDER_BODY_T OB
             WHERE D.ORDER_ID = OB.ORDER_ID
               AND OB.CHARGE_TYPE = 'SLA'
               AND OB.DATE_FROM <= D.DATE_TO
               AND (OB.DATE_TO IS NULL OR D.DATE_FROM <= OB.DATE_TO)
               AND D.TASK_ID  = p_task_id
               AND D.STATUS   = 1
        )
        WHERE RN = 1
    ) OB
    ON (
        OB.DOWNTIME_ID = D.DOWNTIME_ID
    )
    WHEN MATCHED THEN UPDATE SET D.ORDER_BODY_ID   = OB.ORDER_BODY_ID, 
                                 D.SLA_RATEPLAN_ID = OB.RATEPLAN_ID,
                                 D.RATE_RULE_ID    = OB.RATE_RULE_ID, 
                                 D.CHARGE_TYPE     = OB.CHARGE_TYPE,
                                 D.STATUS          = 2,
                                 D.STATUS_DATE     = SYSDATE
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||' ) '||v_count||' rows SLA set' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���������� ������ IDL
    --
    -- ����������� ���� �������� IDL, �� ������ ������
    UPDATE ORDER_BODY_T OB
       SET OB.DATE_TO = (
           SELECT O.DATE_TO
             FROM ORDER_T O
            WHERE O.ORDER_ID = OB.ORDER_ID
       )
     WHERE OB.CHARGE_TYPE = 'IDL'
       AND OB.DATE_TO != TO_DATE('01.01.2050','dd.mm.yyyy')
       AND EXISTS (
           SELECT * 
             FROM ORDER_T O
            WHERE O.ORDER_ID = OB.ORDER_ID
              AND O.DATE_TO != OB.DATE_TO 
       )
       AND NOT EXISTS ( -- ���� ���� �������� SLA, �� ������ �� �������
           SELECT * 
             FROM ORDER_BODY_T OBS
            WHERE OB.ORDER_ID = OBS.ORDER_ID
              AND OBS.CHARGE_TYPE = 'SLA'
              AND (OBS.DATE_TO = TO_DATE('01.01.2050','dd.mm.yyyy') OR OBS.DATE_TO IS NULL)
       )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T.DATE_TO: '||v_count||' rows corrected', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� �� ������� ������ ��� ���������
    UPDATE BRM_DOWNTIME_T D 
       SET D.NOTES  = '� ������ ���������� ��� ������� ���������� ������ REC/MIN',
           D.STATUS  = -2
     WHERE D.STATUS  = 1
       AND D.TASK_ID = p_task_id
       AND NOT EXISTS (
           SELECT * 
             FROM ORDER_BODY_T OB
            WHERE OB.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_MIN,
                                     Pk00_Const.c_CHARGE_TYPE_REC)
              AND OB.ORDER_ID = D.ORDER_ID
              AND OB.DATE_FROM <= D.DATE_TO
              AND (OB.DATE_TO IS NULL OR D.DATE_FROM <= OB.DATE_TO)
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T.OB_ID = '||v_count||' rows OB.MIN/REC not found', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- ������� ���������� ����� ��� ����������� �������� IDL
    INSERT INTO ORDER_BODY_T (
        ORDER_BODY_ID,
        ORDER_ID,
        SUBSERVICE_ID,
        CHARGE_TYPE,
        DATE_FROM,
        DATE_TO,
        FREE_VALUE,
        RATE_RULE_ID, 
        RATE_LEVEL_ID,
        TAX_INCL,
        QUANTITY,
        CREATE_DATE,
        MODIFY_DATE,
        CURRENCY_ID,
        NOTES
    )
    SELECT 
           SQ_ORDER_ID.NEXTVAL ORDER_BODY_ID, 
           ORDER_ID,
           Pk00_Const.c_SUBSRV_IDL SUBSERVICE_ID,       -- c_SUBSRV_IDL    CONSTANT integer := 36;  -- ����������� ��������
           Pk00_Const.c_CHARGE_TYPE_IDL CHARGE_TYPE,    -- c_CHARGE_TYPE_IDL := 'IDL'
           DATE_FROM, 
           DATE_TO,
           c_DEFAULT_FREE_DOWNTIME FREE_VALUE,          -- 43
           Pk00_Const.c_RATE_RULE_IDL_STD RATE_RULE_ID, -- c_RATE_RULE_IDL_STD    CONSTANT INTEGER := 2404; -- ����������� ��������, ����������� �����  
           Pk00_Const.c_RATE_LEVEL_ORDER RATE_LAVEL_ID, -- c_RATE_LEVEL_ORDER     CONSTANT INTEGER := 2302; -- ����� ������ �� �����
           TAX_INCL,
           1 QUANTITY,
           SYSDATE CREATE_DATE,
           SYSDATE MODIFY_DATE,
           CURRENCY_ID,
           '������� ������������� ��� ������� ����������� �� �������' NOTES
      FROM (
          SELECT ROW_NUMBER() OVER (PARTITION BY OB.ORDER_ID ORDER BY OB.DATE_FROM) RN,
                 OB.ORDER_ID,
                 OB.DATE_FROM, 
                 Pk00_Const.c_DATE_MAX DATE_TO,
                 OB.TAX_INCL,
                 OB.CURRENCY_ID
            FROM ORDER_BODY_T OB, 
                 BRM_DOWNTIME_T D 
           WHERE D.TASK_ID  = p_task_id
             AND D.STATUS   = 1
             AND D.ORDER_ID = OB.ORDER_ID
             AND OB.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_REC,
                                    Pk00_Const.c_CHARGE_TYPE_MIN)
             AND OB.DATE_FROM <= D.DATE_TO
             AND (OB.DATE_TO IS NULL OR D.DATE_FROM <= OB.DATE_TO)
             AND NOT EXISTS (
                SELECT * FROM ORDER_BODY_T IDL
                 WHERE IDL.ORDER_ID = OB.ORDER_ID
                   AND IDL.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_IDL
             )
       )
     WHERE RN = 1
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T: '||v_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ���������� ������ IDL
    --
    MERGE INTO BRM_DOWNTIME_T D
    USING (
        SELECT DOWNTIME_ID, ORDER_BODY_ID, RATE_RULE_ID, CHARGE_TYPE
          FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY D.DOWNTIME_ID ORDER BY OB.DATE_FROM DESC) RN, 
                   D.DOWNTIME_ID, OB.ORDER_BODY_ID, 
                   OB.RATE_RULE_ID, OB.CHARGE_TYPE 
              FROM BRM_DOWNTIME_T D, ORDER_BODY_T OB
             WHERE D.ORDER_ID = OB.ORDER_ID
               AND D.TASK_ID  = p_task_id
               AND D.STATUS   = 1
               AND OB.CHARGE_TYPE = 'IDL'
               AND OB.DATE_FROM <= D.DATE_TO
               AND (OB.DATE_TO IS NULL OR D.DATE_FROM <= OB.DATE_TO)
        )
        WHERE RN = 1
    ) OB
    ON (
        OB.DOWNTIME_ID = D.DOWNTIME_ID
    )
    WHEN MATCHED THEN UPDATE SET D.ORDER_BODY_ID   = OB.ORDER_BODY_ID, 
                                 D.RATE_RULE_ID    = OB.RATE_RULE_ID,
                                 D.CHARGE_TYPE     = OB.CHARGE_TYPE,
                                 D.STATUS          = 2,
                                 D.STATUS_DATE     = SYSDATE
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||' ) '||v_count||' rows IDL set' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ���������� ����� � ���������� ��� ����������, ��� ������� ������������ �������
    --
    MERGE INTO BRM_DOWNTIME_T D
    USING (
        SELECT DOWNTIME_ID, ORDER_BODY_ID, RATE_RULE_ID, CHARGE_TYPE, RATE_VALUE
          FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY D.DOWNTIME_ID ORDER BY OB.DATE_FROM DESC) RN, 
                   D.DOWNTIME_ID, OB.ORDER_BODY_ID, 
                   OB.RATE_RULE_ID, OB.CHARGE_TYPE,
                   OB.RATE_VALUE 
              FROM BRM_DOWNTIME_T D, ORDER_BODY_T OB
             WHERE D.ORDER_ID = OB.ORDER_ID
               AND D.TASK_ID  = p_task_id
               AND D.STATUS   > 0
               AND OB.CHARGE_TYPE IN ('MIN', 'REC')
               AND OB.DATE_FROM <= D.DATE_TO
               AND (OB.DATE_TO IS NULL OR D.DATE_FROM <= OB.DATE_TO)
        )
        WHERE RN = 1
    ) OB
    ON (
        OB.DOWNTIME_ID = D.DOWNTIME_ID
    )
    WHEN MATCHED THEN UPDATE SET D.REC_OB_ID         = OB.ORDER_BODY_ID, 
                                 D.REC_OB_CHARGE_TYPE= OB.CHARGE_TYPE,
                                 D.REC_OB_RATE_VALUE = OB.RATE_VALUE
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||' ) '||v_count||' rows REC_OB set' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������ �������� ����������� �����
    UPDATE BRM_DOWNTIME_T D
       SET D.STATUS  = -2, 
           D.NOTES   = '���������� ������ IDL �� ������� � ��������� ���������� �������',
           D.STATUS_DATE = SYSDATE
     WHERE D.TASK_ID = p_task_id
       AND D.ORDER_BODY_ID IS NULL
       AND D.STATUS > 0
       AND NOT EXISTS (
           SELECT * FROM ORDER_BODY_T OB
            WHERE OB.ORDER_ID = D.ORDER_ID
               AND OB.CHARGE_TYPE IN ('IDL')
               AND OB.DATE_FROM <= D.DATE_TO
               AND (OB.DATE_TO IS NULL OR D.DATE_FROM <= OB.DATE_TO)
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||' ) '||v_count||' rows IDL not found' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    UPDATE BRM_DOWNTIME_T D
       SET D.STATUS  = -2, 
           D.NOTES   = '���������� ������ REC/MIN �� ������� � ��������� ���������� �������',
           D.STATUS_DATE = SYSDATE
     WHERE D.TASK_ID = p_task_id
       AND D.REC_OB_ID IS NULL
       AND D.STATUS > 0
       AND NOT EXISTS (
           SELECT * FROM ORDER_BODY_T OB
            WHERE OB.ORDER_ID = D.ORDER_ID
               AND OB.CHARGE_TYPE IN ('REC','MIN')
               AND OB.DATE_FROM <= D.DATE_TO
               AND (OB.DATE_TO IS NULL OR D.DATE_FROM <= OB.DATE_TO)
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||' ) '||v_count||' rows not bind' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������ �������� ����������� �����
    UPDATE BRM_DOWNTIME_T D
       SET D.STATUS  = -2, 
           D.NOTES   = '���������� ������ �� �������',
           D.STATUS_DATE = SYSDATE
     WHERE D.TASK_ID = p_task_id
       AND (D.ORDER_BODY_ID IS NULL OR D.REC_OB_ID IS NULL)
       AND D.STATUS > 0;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||' ) '||v_count||' rows not bind' , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��� ��������� �/�, ����������� �� ������������� ��������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE BRM_DOWNTIME_T D 
       SET D.NOTES  = '����������� �������� ��������� ��������� ��������',
           D.STATUS = 0,
           D.STATUS_DATE = SYSDATE
     WHERE NOT EXISTS (
           SELECT * 
             FROM ACCOUNT_T A
            WHERE A.ACCOUNT_ID = D.ACCOUNT_ID
              AND A.IDL_ENB = 'Y' 
       )
       AND D.ACCOUNT_ID IS NOT NULL
       AND D.TASK_ID = p_task_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T = '||v_count||' rows ACCOUNT_T.IDL_ENB=N', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- --------------------------------------------------------------------- --
    -- ������������ ������ ��� ������������� ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Coca-Cola (�/� MS002234)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ����������� �������� ���������������
    -- ������������ ���������� ������
    UPDATE ORDER_BODY_T OB SET OB.RATE_RULE_ID = 2414
     WHERE OB.ORDER_ID IN (
        SELECT O.ORDER_ID 
          FROM ORDER_T O
         WHERE O.ACCOUNT_ID IN (
            SELECT A.ACCOUNT_ID FROM ACCOUNT_T A
             WHERE A.ACCOUNT_NO = 'MS002234'
         )
     )
     AND OB.CHARGE_TYPE = 'SLA';
    -- ������������ �������� ������
     UPDATE BRM_DOWNTIME_T D
        SET D.RATE_RULE_ID = 2414
      WHERE D.ORDER_NO IN (
        SELECT O.ORDER_NO 
          FROM ORDER_T O
         WHERE O.ACCOUNT_ID IN (
            SELECT A.ACCOUNT_ID FROM ACCOUNT_T A
             WHERE A.ACCOUNT_NO = 'MS002234'
         )
      );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T = '||v_count||' rows Coca Cola', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �� �� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� �������� ������� � �������, ��� ������ ����������������� �������
    -- ������������ ���������� ������
    UPDATE ORDER_BODY_T OB SET OB.RATE_RULE_ID = 2422
     WHERE OB.ORDER_ID IN (
        SELECT O.ORDER_ID 
          FROM ORDER_T O
         WHERE O.ACCOUNT_ID IN (
            SELECT A.ACCOUNT_ID FROM ACCOUNT_T A
             WHERE A.ACCOUNT_NO = 'MS000145'
         )
     )
     AND OB.CHARGE_TYPE = 'IDL';
    -- ������������ �������� ������
     UPDATE BRM_DOWNTIME_T D
        SET D.RATE_RULE_ID = 2422
      WHERE D.ORDER_NO IN (
        SELECT O.ORDER_NO 
          FROM ORDER_T O
         WHERE O.ACCOUNT_ID IN (
            SELECT A.ACCOUNT_ID FROM ACCOUNT_T A
             WHERE A.ACCOUNT_NO = 'MS000145'
         )
      );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T = '||v_count||' rows CBRF', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� � ��� �/� MS007316  (account_id = 2447046)
    -- � ������� ������� �������� ����� ��������� 1/360, � �� 1/720 (�����������) 
    -- �� �������� ������� �����������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE ORDER_BODY_T OB SET OB.RATE_RULE_ID = 2432
     WHERE OB.ORDER_ID IN (
        SELECT O.ORDER_ID 
          FROM ORDER_T O
         WHERE O.ACCOUNT_ID IN (
            SELECT A.ACCOUNT_ID FROM ACCOUNT_T A
             WHERE A.ACCOUNT_NO = 'MS007316'
         )
     )
     AND OB.CHARGE_TYPE = 'IDL';
    -- ������������ �������� ������
     UPDATE BRM_DOWNTIME_T D
        SET D.RATE_RULE_ID = 2432
      WHERE D.ORDER_NO IN (
        SELECT O.ORDER_NO 
          FROM ORDER_T O
         WHERE O.ACCOUNT_ID IN (
            SELECT A.ACCOUNT_ID FROM ACCOUNT_T A
             WHERE A.ACCOUNT_NO = 'MS007316'
         )
      );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T = '||v_count||' rows RZD', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------- --
-- ����������� Item-� ��� ������� ��������� ������� 
-- ---------------------------------------------------------------------- --
PROCEDURE Bind_items( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bind_items';
    v_count      INTEGER;
BEGIN
    -- ����������� item-� ��� ������� ��������� �������
    MERGE INTO BRM_DOWNTIME_T D
    USING (
        SELECT DOWNTIME_ID, ITEM_ID, REP_PERIOD_ID, ITEM_TOTAL
          FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY DOWNTIME_ID ORDER BY I.ITEM_TOTAL DESC) RN, 
                   D.DOWNTIME_ID, I.ITEM_ID, I.REP_PERIOD_ID, I.ITEM_TOTAL
              FROM BRM_DOWNTIME_T D, ITEM_T I, BILL_T B
             WHERE D.TASK_ID = p_task_id
               AND D.STATUS  = 2
               AND I.REP_PERIOD_ID = TO_NUMBER(TO_CHAR(D.DATE_FROM, 'yyyymm'))
               AND I.ORDER_ID = D.ORDER_ID
               AND I.CHARGE_TYPE IN ('REC','MIN')
               AND I.BILL_ID = B.BILL_ID
               AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
               AND B.BILL_TYPE = 'B'
               AND I.ITEM_TYPE = 'B'
           )
           WHERE RN = 1
    ) DI
    ON (
        D.DOWNTIME_ID = DI.DOWNTIME_ID
    )
    WHEN MATCHED THEN UPDATE SET 
        D.REC_ITEM_ID        = DI.ITEM_ID,
        D.REC_ITEM_PERIOD_ID = DI.REP_PERIOD_ID,
        D.REC_ITEM_VALUE     = DI.ITEM_TOTAL,
        D.STATUS             = 3
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T.REC_ITEM_ID = '||v_count||' rows set', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- ����������� ��������������, ��� �� ������
    UPDATE BRM_DOWNTIME_T D
       SET D.STATUS = 3,
           D.NOTES  = '��������, �� ������ item_t ���� REC ��� MIN'
     WHERE D.TASK_ID = p_task_id
       AND D.STATUS  = 2
       AND D.REC_ITEM_ID IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T.REC_ITEM_ID = '||v_count||' rows not found', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� ������ �������� � Item-�
--
PROCEDURE Bills_items_error ( 
               p_recordset OUT t_refc, 
               p_task_id   IN INTEGER 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bills_items_error';
    v_retcode    INTEGER;
BEGIN
    -- ��������� ������
    OPEN p_recordset FOR
      SELECT * FROM (    
          WITH OD AS (
              SELECT D.TASK_ID, D.DOWNTIME_ID, D.ORDER_NO, D.ORDER_ID, 
                     D.DATE_FROM D_DATE_FROM, D.DATE_TO D_DATE_TO,
                     OB.DATE_FROM OB_DATE_FROM, OB.DATE_TO OB_DATE_TO,
                     OB.CHARGE_TYPE, OB.CREATE_DATE, TO_NUMBER(TO_CHAR(D.DATE_FROM, 'yyyymm')) D_PERIOD_ID 
                FROM BRM_DOWNTIME_T D, ORDER_BODY_T OB
               WHERE D.TASK_ID = 23897
                 AND D.STATUS = -3
                 AND D.ORDER_ID = OB.ORDER_ID
                 AND OB.CHARGE_TYPE IN ('MIN', 'REC')
               ORDER BY ORDER_NO, OB.DATE_TO
          )
          SELECT OD.TASK_ID, OD.DOWNTIME_ID, OD.ORDER_NO, OD.ORDER_ID, 
                 OD.D_DATE_FROM, OD.D_DATE_TO,
                 OD.CHARGE_TYPE D_CHARGE_TYPE, OD.CREATE_DATE, OD.D_PERIOD_ID, 
                 I.ITEM_ID, I.CHARGE_TYPE, I.SERVICE_ID, I.SUBSERVICE_ID, I.ITEM_TOTAL 
            FROM OD, ITEM_T I
           WHERE OD.ORDER_ID    = I.ORDER_ID(+)
             AND OD.CHARGE_TYPE = I.CHARGE_TYPE(+)
             AND OD.D_PERIOD_ID = I.REP_PERIOD_ID(+)
      );
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- ---------------------------------------------------------------------- --
-- ������ ������� �������� 
-- ---------------------------------------------------------------------- --
PROCEDURE Calc_IDL( p_task_id IN INTEGER )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Calc_IDL';
    v_charge_type CONSTANT VARCHAR2(3)  := 'IDL';
    v_count       INTEGER;
BEGIN
    -- ������ �������� ����������� �� �������
    UPDATE BRM_DOWNTIME_T D 
       SET IDL_NOTES = '(������� '||
               CASE 
               WHEN D.RATE_RULE_ID = 2404 THEN TO_CHAR(ROUND((D.MINUTES - D.DOWNTIME_FREE)/60))
               WHEN D.RATE_RULE_ID = 2421 THEN TO_CHAR(CEIL((D.MINUTES - D.DOWNTIME_FREE)/60))  -- ��������� � ������� ������� �� ����
               END||' ���.)',
           IDL_VALUE = -1 * (
               CASE
               WHEN D.RATE_RULE_ID = 2404 THEN REC_OB_RATE_VALUE * ROUND((D.MINUTES - D.DOWNTIME_FREE)/60) / 720 
               WHEN D.RATE_RULE_ID = 2421 THEN REC_OB_RATE_VALUE * CEIL((D.MINUTES - D.DOWNTIME_FREE)/60) / 720  -- ��������� � ������� ������� �� ����
               WHEN D.RATE_RULE_ID = 2432 THEN REC_OB_RATE_VALUE * ROUND((D.MINUTES - D.DOWNTIME_FREE)/60) / 320  -- ��� ���, �/� MS007316 - �� �������� ������� �����������
               END
               ),
           D.STATUS      = 4,
           D.STATUS_DATE = SYSDATE
     WHERE D.TASK_ID     = p_task_id
       AND D.STATUS      = 3
       AND D.CHARGE_TYPE = v_charge_type 
       AND ROUND((D.MINUTES - D.DOWNTIME_FREE)/60) >= 1
       AND D.RATE_RULE_ID != 2422;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||'): '||v_count||' rows idl - ok', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������ ������� ����������� 
    UPDATE BRM_DOWNTIME_T D 
       SET D.STATUS = 0, 
           D.NOTES  = '�������� ����������� 0',
           D.STATUS_DATE = SYSDATE
     WHERE D.TASK_ID = p_task_id
       AND D.STATUS  = 4
       AND D.IDL_VALUE = 0
       AND D.CHARGE_TYPE = v_charge_type;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||'): '||v_count||' rows idl - null', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
     
    -- ������ ������ ������ ����������� ������� 
    UPDATE BRM_DOWNTIME_T D 
       SET D.STATUS = -4, 
           D.NOTES  = '���� ����������������� ������',
           D.STATUS_DATE = SYSDATE
     WHERE D.TASK_ID = p_task_id
       AND D.STATUS  = 3
       AND D.CHARGE_TYPE = v_charge_type 
       AND CASE 
           WHEN D.RATE_RULE_ID = 2404 THEN TO_CHAR(ROUND((D.MINUTES - D.DOWNTIME_FREE)/60))
           WHEN D.RATE_RULE_ID = 2421 THEN TO_CHAR(CEIL((D.MINUTES - D.DOWNTIME_FREE)/60))  -- ��������� � ������� ������� �� ����
           END < 1;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||'): '||v_count||' rows idl - error', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------- --
-- ������ �������� � �������, ��� ������ ����������������� �������
-- ---------------------------------------------------------------------- --
PROCEDURE Calc_IDL_MIN( p_task_id   IN INTEGER )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Calc_IDL_MIN';
    v_charge_type CONSTANT VARCHAR2(3)  := 'IDL';
    v_rate_rule_id CONSTANT INTEGER     := 2422;
    v_count       INTEGER;
BEGIN
    -- ������ �������� ����������� �� �������
    UPDATE BRM_DOWNTIME_T D 
       SET IDL_NOTES = '(������� '||D.MINUTES||' ���.)',
           IDL_VALUE = -1 * ( D.MINUTES * (D.REC_OB_RATE_VALUE / 43200)),
           D.STATUS  = 4,
           D.STATUS_DATE  = SYSDATE
     WHERE D.TASK_ID      = p_task_id
       AND D.STATUS       = 3
       AND D.CHARGE_TYPE  = v_charge_type 
       AND D.RATE_RULE_ID = v_rate_rule_id
       AND D.MINUTES     >= D.DOWNTIME_FREE;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||'): '||v_count||' rows IDL_MIN - ok', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------- --
-- ������ �������� SLA, �������� � ����� 
-- ---------------------------------------------------------------------- --
PROCEDURE Calc_SLA_H( p_task_id   IN INTEGER )
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Calc_SLA_H';
    v_charge_type  CONSTANT VARCHAR2(3)  := 'SLA';
    v_rate_rule_id CONSTANT INTEGER      := 2413;
    v_count        INTEGER;
BEGIN
    -- ������ �������� ����������� �� �������
    MERGE INTO BRM_DOWNTIME_T D
    USING (
        SELECT D.DOWNTIME_ID, 
               SP.SLA_PERCENT,
               -(D.REC_OB_RATE_VALUE * SP.SLA_PERCENT/100) IDL_VALUE,
               IDL_NOTES 
          FROM SLA_PERCENT_T SP, (
            SELECT D.DOWNTIME_ID, SLA_RATEPLAN_ID, '('||ROUND(MINUTES/60)||' ���.)' IDL_NOTES, 
                   CASE
                      WHEN (100 * (720 - (D.MINUTES/60))/720) < 0 THEN 0
                      ELSE (100 * (720 - (D.MINUTES/60))/720)
                   END K_SLA,
                   D.REC_OB_RATE_VALUE 
              FROM BRM_DOWNTIME_T D
             WHERE D.TASK_ID      = p_task_id
               AND D.CHARGE_TYPE  = v_charge_type
               AND D.RATE_RULE_ID = v_rate_rule_id
               AND D.STATUS = 3
            ) D
         WHERE D.SLA_RATEPLAN_ID  = SP.RATEPLAN_ID
           AND SP.K_MIN <= K_SLA 
           AND SP.K_MAX >  K_SLA
        /*    
        SELECT D.DOWNTIME_ID,
               SP.SLA_PERCENT,
               -(D.REC_OB_RATE_VALUE * SP.SLA_PERCENT/100) IDL_VALUE,
               '('||ROUND(MINUTES/60)||' ���.)' IDL_NOTES 
          FROM BRM_DOWNTIME_T D, SLA_PERCENT_T SP
         WHERE D.TASK_ID      = p_task_id
           AND D.CHARGE_TYPE  = v_charge_type
           AND D.RATE_RULE_ID = v_rate_rule_id 
           AND D.SLA_RATEPLAN_ID  = SP.RATEPLAN_ID
           --AND SP.K_MIN <= (100 * (1 - ((D.MINUTES/60)/(24 * (D.DATE_TO-D.DATE_FROM+1/86400) )))) 
           --AND SP.K_MAX >  (100 * (1 - ((D.MINUTES/60)/(24 * (D.DATE_TO-D.DATE_FROM+1/86400) ))))
           AND SP.K_MIN <= (100 * (720 - (D.MINUTES/60))/720) 
           AND SP.K_MAX >  (100 * (720 - (D.MINUTES/60))/720) 
           AND D.STATUS = 3
        */
    ) DP
    ON (
        D.DOWNTIME_ID = DP.DOWNTIME_ID
    )
    WHEN MATCHED THEN UPDATE 
                         SET D.K_SLA       = DP.SLA_PERCENT, 
                             D.IDL_VALUE   = DP.IDL_VALUE, 
                             D.IDL_NOTES   = DP.IDL_NOTES,
                             D.STATUS_DATE = SYSDATE,
                             D.STATUS      = 4  
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||'): '||v_count||' rows SLA_H - ok', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������ ��� ������������� � �������
    UPDATE BRM_DOWNTIME_T D 
       SET D.STATUS = -4, 
           D.STATUS_DATE = SYSDATE, 
           D.NOTES = '�� ������� ������ � ������� SLA_PERCENT_T'
     WHERE D.TASK_ID      = p_task_id
       AND D.CHARGE_TYPE  = v_charge_type
       AND D.RATE_RULE_ID = v_rate_rule_id
       AND D.IDL_VALUE IS NULL
       AND D.STATUS IN ( 3, 4 );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||'): '||v_count||' rows SLA_H - error', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

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
    v_status     CONSTANT INTEGER := 5;
BEGIN
    -- ��������� ������� � ��������
    INSERT INTO BILLING_QUEUE_T( 
           BILL_ID, ACCOUNT_ID, TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID 
    )
    SELECT DISTINCT 
           B.BILL_ID, B.ACCOUNT_ID, p_task_id TASK_ID, B.REP_PERIOD_ID, B.REP_PERIOD_ID
      FROM BILL_T B, BRM_DOWNTIME_T D
     WHERE B.REP_PERIOD_ID = D.PERIOD_ID
       AND B.ACCOUNT_ID    = D.ACCOUNT_ID
       AND D.TASK_ID       = p_task_id
       AND D.STATUS        = v_status -1
       AND B.BILL_TYPE     = p_bill_type
       AND (
           B.CALC_DATE IS NULL
           OR
           PK04_PERIOD.PERIOD_TO(B.REP_PERIOD_ID) < B.CALC_DATE 
           )-- 25.11.2016 �.������, �� ������ �.�������: "������� �� ����������� ��� �������� �� ������ ������������ ������� ������"
       ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T(task_id = '||p_task_id||'): '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ����������� id ������
    MERGE INTO BRM_DOWNTIME_T D
    USING (
    SELECT D.DOWNTIME_ID, Q.BILL_ID 
      FROM BRM_DOWNTIME_T D, BILLING_QUEUE_T Q
     WHERE D.ACCOUNT_ID    = Q.ACCOUNT_ID
       AND D.TASK_ID       = p_task_id
       AND D.STATUS        = v_status -1
    ) Q
    ON (
      D.DOWNTIME_ID = Q.DOWNTIME_ID
    )
    WHEN MATCHED THEN UPDATE SET D.BILL_ID = Q.BILL_ID,
                                 D.STATUS  = v_status,
                                 D.STATUS_DATE = SYSDATE;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||'): '||v_count||' bills exists', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������� �������� � ������
    UPDATE BRM_DOWNTIME_T D 
       SET D.STATUS = -v_status, 
           D.NOTES = DECODE(D.NOTES, NULL, NULL, D.NOTES||', ') || '�� ������ ����',
           D.STATUS_DATE = SYSDATE
     WHERE D.BILL_ID IS  NULL
       AND D.TASK_ID = p_task_id
       AND D.STATUS  = v_status -1;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BRM_DOWNTIME_T(task_id = '||p_task_id||'): '||v_count||' bills not found', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
     
EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------- --
-- ������� ���������� �� ������� � ������ 
-- ---------------------------------------------------------------------- --
PROCEDURE Make_items( p_task_id   IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_items';
    v_count      INTEGER;
    v_status     CONSTANT INTEGER := 7;
BEGIN
    -- ����������� ������ � ����������� ����� REC/MIN -> IDL/SLA
    MERGE INTO ORDER_BODY_T OB
    USING (
        SELECT OB.ORDER_BODY_ID, OBR.CURRENCY_ID, OBR.TAX_INCL 
          FROM BRM_DOWNTIME_T D, ORDER_BODY_T OBR, ORDER_BODY_T OB
         WHERE D.TASK_ID = p_task_id
           AND D.REC_OB_ID = OBR.ORDER_BODY_ID
           AND D.ORDER_BODY_ID = OB.ORDER_BODY_ID
           AND (
             OBR.CURRENCY_ID != OB.CURRENCY_ID OR OB.CURRENCY_ID IS NULL OR
             OBR.TAX_INCL    != OB.TAX_INCL    OR OB.TAX_INCL    IS NULL
           ) 
           AND OBR.CHARGE_TYPE IN ('REC','MIN')
           AND OB.CHARGE_TYPE  IN ('IDL','SLA')
           AND D.STATUS > 0
         GROUP BY OB.ORDER_BODY_ID, OBR.CURRENCY_ID, OBR.TAX_INCL
    ) OBR
    ON (
        OB.ORDER_BODY_ID = OBR.ORDER_BODY_ID 
    )
    WHEN MATCHED THEN UPDATE SET OB.CURRENCY_ID = OBR.CURRENCY_ID,
                                 OB.TAX_INCL    = OBR.TAX_INCL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Set currency_id REC/MIN -> IDL/SLA for '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� item_id � ������������� �������
    UPDATE BRM_DOWNTIME_T D 
       SET D.IDL_ITEM_ID = Pk02_Poid.Next_item_id,
           D.STATUS  = v_status,
           D.STATUS_DATE = SYSDATE
     WHERE D.TASK_ID = p_task_id
       AND D.STATUS  = v_status -1;
    
    -- ��������� item-� ��� ����������� �������� � SLA
    INSERT INTO ITEM_T (
        BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
        ORDER_ID, ORDER_BODY_ID,
        SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
        ITEM_TOTAL, ITEM_CURRENCY_ID, RECVD, DATE_FROM, DATE_TO, 
        ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
        REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, DESCR
    )
    SELECT D.BILL_ID, D.PERIOD_ID, D.IDL_ITEM_ID, 'B', NULL,
           D.ORDER_ID, D.ORDER_BODY_ID,
           O.SERVICE_ID, OB.SUBSERVICE_ID, D.CHARGE_TYPE,
           D.IDL_VALUE, OB.CURRENCY_ID, 0, D.DATE_FROM, D.DATE_TO,
           'OPEN', SYSDATE, SYSDATE,
           0, 0, OB.TAX_INCL, NULL, D.IDL_NOTES, D.IDL_NOTES 
      FROM BRM_DOWNTIME_T D,  
           ORDER_BODY_T OB, ORDER_T O
     WHERE D.TASK_ID            = p_task_id
       AND D.STATUS             = v_status
       AND D.ORDER_BODY_ID      = OB.ORDER_BODY_ID
       AND D.ORDER_ID           = O.ORDER_ID
       AND D.ORDER_ID           = OB.ORDER_ID
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������
    UPDATE BRM_DOWNTIME_T D
       SET D.STATUS      = -v_status,
           D.STATUS_DATE = SYSDATE,
           D.NOTES       = '������ �������� item-�'
     WHERE D.TASK_ID     = p_task_id
       AND D.STATUS      = v_status
       AND NOT EXISTS (
           SELECT * FROM ITEM_T I
            WHERE I.REP_PERIOD_ID = D.PERIOD_ID
              AND I.ITEM_ID       = D.IDL_ITEM_ID
       )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T '||v_count||' rows error', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ����� � ����� INFRANET 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Make_report( p_period_id   IN INTEGER )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Make_report';
    v_count       INTEGER;

BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ���� ������ � �������� �������
    MERGE INTO INFRANET.BRM_DOWNTIME_TMP T
    USING (
        SELECT D.* 
          FROM BRM_DOWNTIME_T D
         WHERE D.PERIOD_ID = 201609
    ) D
    ON (
       T.DOWNTIME_ID = D.DOWNTIME_ID
    )
    WHEN MATCHED THEN UPDATE 
                         SET T.ACCOUNT_ID  = D.ACCOUNT_ID, 
                             T.ORDER_ID    = D.ORDER_ID, 
                             T.BILL_ID     = D.BILL_ID, 
                             T.ITEM_ID     = D.IDL_ITEM_ID, 
                             T.STATUS      = D.STATUS, 
                             T.STATUS_DATE = D.STATUS_DATE, 
                             T.NOTES       = D.NOTES
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.BRM_DOWNTIME_TMP T '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION 
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;



-- --------------------------------------------------------------------- --
--              � � � � � �    � � �    � � � � � � � �                  --
-- --------------------------------------------------------------------- --
-- ��������������� ����������� �� �������
-- �������� - �� �� ��� ���� READY/CLOSED �������� �� �������, 
-- ����� ������� ��������� Recharge_downtimes
--
PROCEDURE Rollback_downtimes( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_downtimes';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ������� Item-� ����������� �� �������
    DELETE FROM ITEM_T I
     WHERE I.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_IDL, 
                             Pk00_Const.c_CHARGE_TYPE_SLA)
       --AND I.ITEM_STATUS   = Pk00_Const.c_ITEM_STATE_OPEN   -- ���-���� ������ ���� ������
       --AND I.ITEM_TYPE   = Pk00_Const.c_ITEM_TYPE_BILL  -- ������������� �� �������
       --AND I.EXTERNAL_ID IS NULL -- �� ������ ������
       AND EXISTS (
          SELECT * FROM BRM_DOWNTIME_T D
           WHERE D.TASK_ID     = p_task_id
             AND D.PERIOD_ID   = I.REP_PERIOD_ID
             AND D.IDL_ITEM_ID = I.ITEM_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T.IDL/SLA '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ���������� ������� �������� �� ������� ����������
    UPDATE BRM_DOWNTIME_T D
       SET D.STATUS = 3, D.STATUS_DATE = SYSDATE, D.NOTES = NULL, 
           D.IDL_ITEM_ID = NULL, D.IDL_VALUE = NULL, D.IDL_NOTES = NULL
     WHERE D.TASK_ID = p_task_id
       AND D.STATUS > 3      -- �������, ��� ���� ������ ������ ���������
       AND NOT EXISTS (
           SELECT * FROM ITEM_T I
           WHERE D.PERIOD_ID   = I.REP_PERIOD_ID
             AND D.IDL_ITEM_ID = I.ITEM_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. BRM_DOWNTIME_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
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

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Rollback_downtimes( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� � ����� ����������� �� ������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Task_processing( p_task_id );

    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������������ � ��������� ����������� �� ������� ��� �����-����
-- �����-���� ������ ���� � ���������: 'OPEN'
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Recharge_downtime_for_debet( 
             p_dbt_bill_id    IN INTEGER,  -- id - ���������� �����, � ������� ������� ������
             p_dbt_period_id  IN INTEGER,  -- ������ ���������� �����
             p_crd_period_id  IN INTEGER   -- ������ ����������� �����, ��� �������� ������������� ������
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Recharge_downtime_for_debet';
    v_task_id    INTEGER;
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --   
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1) ������� �������, ��� ��������� ���������� ������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_task_id := PK30_BILLING_QUEUE.Open_task;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2) ��������� ������� � ��������, ��� ����������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    INSERT INTO BILLING_QUEUE_T( BILL_ID, ACCOUNT_ID, ORDER_ID, TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID )
    WITH D AS (
        SELECT DISTINCT O.ACCOUNT_ID 
          FROM DOWNTIME_T D, ORDER_T O, PERIOD_T P
         WHERE P.PERIOD_ID    = p_crd_period_id
           AND D.DATE_FROM   <= P.PERIOD_TO
           AND P.PERIOD_FROM <= D.DATE_TO 
           AND O.ORDER_NO     = D.ORDER_NO
    )
    SELECT B.BILL_ID, B.ACCOUNT_ID, NULL ORDER_ID,  
           v_task_id TASK_ID, B.REP_PERIOD_ID, p_crd_period_id 
      FROM D, BILL_T B 
     WHERE B.REP_PERIOD_ID = p_dbt_period_id
       AND B.BILL_ID       = p_dbt_bill_id
       AND D.ACCOUNT_ID    = B.ACCOUNT_ID
       AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_DBT
       AND B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_OPEN;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    IF v_count > 0 THEN
        -- 3) ��������������� ������� ����������� �� �������
        Rollback_downtimes( v_task_id );
        -- 4) ������������ ������ ������������ ����������� �� �������
        Task_processing( v_task_id );

    END IF;
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 5) ������ ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk30_Billing_Queue.Close_task( v_task_id ); 
   
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;



-- ������������ ������ � ��������, ������ � �������� �����������, � ����������� ���������
-- ��� ����� ��� ���������� �������� � ���������/���������������� ������ 
PROCEDURE Dup_downtimes( 
             p_src_task_id IN INTEGER,  -- �������� ������ ��� ������������ �������
             p_dst_task_id IN INTEGER   -- �������� ����� ��������� ������ �� ������������������
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Dup_downtimes';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
          
    INSERT INTO BRM_DOWNTIME_T D(
        TASK_ID, DOWNTIME_ID, PERIOD_ID, ORDER_NO, DATE_FROM, DATE_TO, 
        MINUTES, K_SLA, ACCOUNT_ID, ORDER_ID, ORDER_BODY_ID, CHARGE_TYPE, 
        RATE_RULE_ID, SLA_RATEPLAN_ID, DOWNTIME_FREE, REC_OB_ID, 
        REC_OB_CHARGE_TYPE, REC_OB_RATE_VALUE, BILL_ID, REC_ITEM_ID, 
        REC_ITEM_PERIOD_ID, REC_ITEM_VALUE, 
        IDL_ITEM_ID, IDL_VALUE, IDL_NOTES, 
        STATUS, STATUS_DATE, NOTES
    )
    SELECT 
        p_dst_task_id, DOWNTIME_ID, PERIOD_ID, ORDER_NO, DATE_FROM, DATE_TO, 
        MINUTES, K_SLA, ACCOUNT_ID, ORDER_ID, ORDER_BODY_ID, CHARGE_TYPE, 
        RATE_RULE_ID, SLA_RATEPLAN_ID, DOWNTIME_FREE, REC_OB_ID, 
        REC_OB_CHARGE_TYPE, REC_OB_RATE_VALUE, BILL_ID, REC_ITEM_ID, 
        REC_ITEM_PERIOD_ID, REC_ITEM_VALUE, 
        NULL IDL_ITEM_ID, NULL IDL_VALUE, NULL IDL_NOTES, 
        3 STATUS, SYSDATE STATUS_DATE, NULL NOTES
      FROM BRM_DOWNTIME_T D
     WHERE D.TASK_ID = p_src_task_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. BRM_DOWNTIME_T: '||v_count||' rows duplicated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


/************************************************************************************
1. �������.

- ��� ������� � ��� ���������� � Access ���� � �� ��� ����������� � ���.
��� ������ ���� ����

����� ������� � ���� ��� ������� ������������ �� ������� �� �����.
� �� ����

ID (����� 2 ���� �� �������, ����������� � ��� ��� �������� ������)
����� ������
���� ������ �������
���� ��������� �������
������� � ����� (� �������� � ������, � �� � ��������)
flags    - ���������� � ��� ������, � ����� ��� �.�����������. � ��� ���� ...

- ������� �� �� ������� �������� � ���. ���� ���� ��� ��� ����� ��������.
	� ������ :
- �� ������ ������ ������������, ��� �� ����� ������ � �������� SLA. ���� ���, �� ��� ������� �������.
�����: 

- �� ������ ������ ��������� ��� ��������� (��� �������) ���
	��������� (��� VPN � ��������).
- ������������ ������� ����������� �� ����� ����� (������ ����� ������� ����������� � ������� �������)
- ��������� (��� ���������) ������� �� 720 (������� ����� ����� � ������) � ���������� �� ���� ������� (�����).
	�������� ����� �� �������.


����� �� ������ ����������� � ��� �� ������ �����, ��������� ������ � ���. �����������. ��������:
cycle_part		Downtime (��� ����������)      Percent		Flag
17.080555555555555	0.699999999999999		0		NULL
��� ���������� ��� �����������.

��� ������ SLA Percent � Flag ����� ������ ��������.

���� ������� ���������� (�� ����������) � �������� ������������ ������� �� �����������.
� ��������� ������� ����������� �� ������������� ������������.

2. SLA.
���� ����� ������� ������ � ������ ������� SLA. �� ������������ ����� ��������� ��� ��������� ����������� ��-�������.
� ���� ������ �� ������������ ������� ����������� ����������� �����������.
����. ����������� ����������� �� �������
(100 * (1 - (���� ������� / (24 * ���� � ������)))
���������� ��������� ��������� � �������� �� 0 �� 1.
� ������� SLA ��� ������� ������ ����������� (��������� ������� ���������) ������ ������ � ���������
�� ������������ ����������� �����������. ������ :

������� ������	���� �			��� �
			
1 			99.69                                    	99.65
3 			99.64                                    	99.6 
5 			99.59                                    	99.55
7 			99.54                                    	99.5 
10			99.489			0   .

��� ���������� ���������, � ������ ����� � ����������� ���������� �������.
- ��������� ��������� ��� ��������� � ������ ����� �� �������� ����������� �������� �� ���.

� ���� ������ � ��� �������� � ���� Percent - �������� ������������ �������� ������.

���� ������ ����� � �� �ccess ������� ��� �.�����������. � � ���� flags ����� 1.
����� � ����������� �� ���������, ��� � ���������� ������, � �����
���� �������� ���� ����������� �������� ������.


SLA �������� ������ �� �����. �.�. �� ��� ������ � �������� ����� ����� ���������� � �������� �� SLA.
��. ����� ����� �������� �� ������ ������� � �.�. �������� ������� � ����������� ���� ���� (������, ������ � �.�.)
����� �� ��������� ��������. ����� � �� ���, ��� ������� ������.
************************************************************************************/


/***********************************************************************************
������� ������� �� ������� ��������

1. ��� ������ �� ������ �������� SLA

SELECT a.obj_id0, s.login
  FROM sla_services_t a, service_t s 
  where s.poid_id0 = a.service_obj_id0

����� a.obj_id0 - id ������ �� ������� � ��������
���������� �� ������� ������.
s.login - � ������.

- ����� ����� ���� ������ �� ������, �.�. �� ����� ������� ����� ����
��������� �������.


1. ������-��������

SELECT a.obj_id0, a.rec_id, a.percent, round(a.step_max,3), round(a.step_min,3)
  FROM percents_t a 

����� a.obj_id0 - id ������ �� ������� � �������� �������
a.rec_id  - id �������
a.percent - % ������
round(a.step_max,3) - ������� �������� ���������
round(a.step_min,3) - ������ �������� ���������

� ���������, �.�������
************************************************************************************/

END PK38_BILLING_DOWNTIME;
/
