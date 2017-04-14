CREATE OR REPLACE PACKAGE PK39_BILLING_DISCOUNT
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK39_BILLING_DISCOUNT';
    -- ==============================================================================
    -- ������ ��������� ������, ������ ������� ����� ����������� ������:
    -- ������ ��� �������� � ��������:
    -- discount_group_t, dg_account_t, dg_percent_t
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ���� ��������� ������ ��� ���������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Apply_discounts( p_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ������ ��� ��������� ������ � �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Apply_group_discount( p_group_id IN INTEGER, p_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������� � ������ ������ �������
    --
    PROCEDURE Apply_discount_percent( 
                 p_group_id  IN INTEGER, 
                 p_period_id IN INTEGER,
                 p_percent   IN NUMBER 
              );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ������ ��������� ������
    -- DG_RULE_ID = 2501   - ����������� ������ ������ �� ������� DG_PERCENT_T
    -- RATE_RULE_ID = 2412 - ���������� ��������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������� ��� ����������� ������
    -- ��������: ����� ������ ���� ������������
    FUNCTION Get_std_discount_percent( 
               p_group_id  IN INTEGER, 
               p_period_id IN INTEGER 
             ) RETURN NUMBER;
             
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ������, ��������� �� ���-�� ����� �����������
    -- ����� ����������� ��� ����������� ����� �� ������ VPN
    -- DG_RULE_ID = 2506
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������: ����� ������ ���� ������������
    FUNCTION Get_point_discount_percent( 
               p_group_id  IN INTEGER, 
               p_period_id IN INTEGER 
             ) RETURN NUMBER;
             
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������� ��� �������� ������ �� ������ ������� (���)
    -- ��������: ����� ������ ���� ������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Get_minutes_discount_percent( 
               p_group_id  IN INTEGER, 
               p_period_id IN INTEGER 
             ) RETURN NUMBER;
             
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������ ����������� ������ ��� ������ �� �������
    -- ��������: ���� ������ ���� ������ - bill_status = 'OPEN'
    PROCEDURE Make_item_std_discount( p_task_id IN INTEGER, p_percent IN NUMBER );

    -- ��������� � ��������� ������� ������
    PROCEDURE Apply_std_discount( p_group_id IN INTEGER, p_period_id IN INTEGER );
    
    -- ��������� � ��������� ������, ��������� �� ���-�� ����� �����������
    PROCEDURE Apply_point_discount( p_group_id IN INTEGER, p_period_id IN INTEGER );
    
    -- ��������� � ��������� ��������� ������, ��������� �� ���-�� ����� �������
    PROCEDURE Apply_minutes_discount( p_group_id IN INTEGER, p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������, ��� ��� ������ � ������� ������� ��������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Check_dg_volume_orders( p_group_id IN INTEGER, p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����� ������ ���������� � ����� �� �������
    -- ��������!
    -- 1) ���� ������� ���� �������������
    -- 2) ������, ��� ��������� �����, ����� �������� � ��������� ������� 
    -- �� ������� � ������, �� ������ �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Rollback_bill_discount( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����� ���� ��������� ������, ����������� �� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Rollback_discounts( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����� ������ �� ������, ����������� �� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Rollback_group_discount( p_group_id IN INTEGER, p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ � ��������� ������ ��� ������������ �����
    -- ���� ������ ���� �������������: BILL_STATUS = 'OPEN'
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Recalc_discount_for_bill( 
                 p_bill_id    IN INTEGER,  -- id - ���������� �����, � ������� ������� ������
                 p_period_id  IN INTEGER   -- ������ �����
              );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ � ��������� ������ ��� �����-����
    -- ��������� ���� ������ ���� �������������: BILL_STATUS = 'OPEN'
    -- � ���������� � ������� 'CLOSED'
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Recalc_discount_for_debet( 
                 p_dbt_bill_id    IN INTEGER,  -- id - ���������� �����, � ������� ������� ������
                 p_dbt_period_id  IN INTEGER,  -- ������ ���������� �����
                 p_crd_period_id  IN INTEGER   -- ������ ����������� �����, ��� �������� ������������� ������
              );    
    
END PK39_BILLING_DISCOUNT;
/
CREATE OR REPLACE PACKAGE BODY PK39_BILLING_DISCOUNT
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������ ���� ��������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Apply_discounts( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Apply_discounts';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ������ ��������� ������
    -- DG_RULE_ID   = 2501, 2505 - ����������� ������ ������ �� ������� DG_PERCENT_T
    -- RATE_RULE_ID = 2412 - ���������� ��������� ������
    --
    v_count := 0;
    FOR dg IN (
      SELECT DG_ID
        FROM DISCOUNT_GROUP_T DG, PERIOD_T P
       WHERE P.PERIOD_ID = p_period_id
         AND DG.DATE_FROM < P.PERIOD_TO
         AND (DG.DATE_TO IS NULL OR P.PERIOD_FROM < DG.DATE_TO ) 
         AND DG_RULE_ID NOT IN ( 2502, 2503, 2507, 2508 ) -- ��������� � ������������� ������� ��������
    )
    LOOP
        -- ��������� ������ ��� ������
        Apply_group_discount( dg.Dg_Id, p_period_id );
        v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg(v_count||' - std_discounts', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������ ������ ��� ��������� ������ � �������
--
PROCEDURE Apply_group_discount( p_group_id IN INTEGER, p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Apply_group_discount';
    v_rule_id       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id||', group_id = '||p_group_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    SELECT DG.DG_RULE_ID INTO v_rule_id
      FROM DISCOUNT_GROUP_T DG, PERIOD_T P
     WHERE DG.DG_ID    = p_group_id
       AND P.PERIOD_ID = p_period_id
       AND DG.DATE_FROM < P.PERIOD_TO
       AND (DG.DATE_TO IS NULL OR P.PERIOD_FROM < DG.DATE_TO )
       AND DG.DG_RULE_ID NOT IN (2502,2503); -- ������������� � ��������� ������� ��������
    -- ��� ������ �� ������, ��������� ��� ��� ������ �������������� 
    IF v_rule_id IN (2504, 2505) THEN
       Check_dg_volume_orders( p_group_id => p_group_id, p_period_id => p_period_id );
    END IF;
    IF v_rule_id = 2506 THEN
       -- ��������� � ��������� ������, ��������� �� ���-�� ����� �����������
       Apply_point_discount( p_group_id => p_group_id, p_period_id => p_period_id );
    ELSIF v_rule_id = 2504 THEN
       -- ��������� � ��������� ������, ��������� �� ���-�� ����� �������
       Apply_minutes_discount( p_group_id => p_group_id, p_period_id => p_period_id );
    ELSE
       -- ��������� �����������
       Apply_std_discount( p_group_id => p_group_id, p_period_id => p_period_id );
    END IF;
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ������������� ����� ������ ���, ���������, ...
    
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR. Grup_id='||p_group_id||', period_id='||p_period_id        , c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������� � ������ ������ �������
--
PROCEDURE Apply_discount_percent( 
             p_group_id  IN INTEGER, 
             p_period_id IN INTEGER,
             p_percent   IN NUMBER 
          )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Apply_discount_percent';
    v_count         INTEGER;
    v_percent       NUMBER  := p_percent;
    v_task_id       INTEGER := NULL;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id||', group_id = '||p_group_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    IF v_percent > 0 THEN 
        -- 1) �������� ��������� ������������� ������
        v_task_id := PK30_BILLING_QUEUE.Open_task;
        
        -- 2) ��������� ������� � �������� ����������
        INSERT INTO BILLING_QUEUE_T(BILL_ID, ACCOUNT_ID, 
                                    TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID)
        SELECT B.BILL_ID, GA.ACCOUNT_ID,  
               v_task_id, p_period_id, p_period_id
          FROM DISCOUNT_GROUP_T G, DG_ACCOUNT_T GA, BILL_T B, PERIOD_T P
         WHERE P.PERIOD_ID     = p_period_id
           AND G.DG_ID         = p_group_id
           AND G.DATE_FROM     < P.PERIOD_TO
           AND (G.DATE_TO IS NULL OR P.PERIOD_FROM < G.DATE_TO)
           AND GA.DATE_FROM    < P.PERIOD_TO
           AND (GA.DATE_TO IS NULL OR P.PERIOD_FROM < GA.DATE_TO)
           AND G.DG_ID         = GA.DG_ID
           AND B.ACCOUNT_ID    = GA.ACCOUNT_ID
           AND B.BILL_TYPE     = PK00_CONST.c_BILL_TYPE_REC
           AND B.REP_PERIOD_ID = P.PERIOD_ID;
        --
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('BILLING_QUEUE_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
 
        -- 3) ��������� ������ ������ - ������ ���� 'READY'
        SELECT COUNT(*) INTO v_count
          FROM BILL_T B, BILLING_QUEUE_T Q
         WHERE B.REP_PERIOD_ID = p_period_id
           AND B.BILL_ID = Q.BILL_ID
           AND Q.TASK_ID = v_task_id
           AND B.BILL_STATUS NOT IN (Pk00_Const.c_BILL_STATE_READY);
        IF v_count != 0 THEN
           Pk01_Syslog.Raise_user_exception(p_Msg => v_count||' ������ �� ����� ������ "READY"',
                                            p_Src => c_PkgName||'.'||v_prcName);
        END IF;

        -- 4) ���������������� �����
        Pk30_Billing_Base.Rollback_bills(v_task_id, true);

        -- 5) ������� ����� ������������� ������, ���� ����
        Rollback_bill_discount( v_task_id );

        -- 6) ��������� � ���� ������� ������ � ���� �� ������ ��������� ������
        Make_item_std_discount( v_task_id, v_percent );
        
        -- 7) ��������� �����
        Pk30_Billing_Base.Make_bills(v_task_id, true);

        -- 8) ����������� ������
        PK30_BILLING_QUEUE.Close_task(p_task_id => v_task_id);
    
    END IF;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR, group_id = '||p_group_id, c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� ������ ��������� ������
-- DG_RULE_ID = 2501, 2505   - ����������� ������ ������ �� ������� DG_PERCENT_T
-- RATE_RULE_ID = 2412 - ���������� ��������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������� ��� ����������� ������
-- ��������: ����� ������ ���� ������������
FUNCTION Get_std_discount_percent( 
           p_group_id IN INTEGER, 
           p_period_id IN INTEGER 
         ) RETURN NUMBER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Get_std_discount_percent';
    v_percent       NUMBER;
BEGIN
    -- �������� ������� ������
    WITH Q AS ( 
      SELECT G.DG_ID, B.BILL_ID, GA.ACCOUNT_ID, B.PROFILE_ID, 
             P.PERIOD_FROM, P.PERIOD_TO, P.PERIOD_ID 
        FROM DISCOUNT_GROUP_T G, DG_ACCOUNT_T GA,
             PERIOD_T P, BILL_T B
       WHERE G.DG_ID         = p_group_id
         AND G.DG_ID         = GA.DG_ID
         AND P.PERIOD_ID     = p_period_id
         AND G.DATE_FROM     < P.PERIOD_TO
         AND (G.DATE_TO IS NULL OR P.PERIOD_FROM < G.DATE_TO)
         AND GA.DATE_FROM    < P.PERIOD_TO
         AND (GA.DATE_TO IS NULL OR P.PERIOD_FROM < GA.DATE_TO)
         AND B.REP_PERIOD_ID = P.PERIOD_ID
         AND B.ACCOUNT_ID    = GA.ACCOUNT_ID
         AND B.BILL_TYPE     IN ( Pk00_Const.c_BILL_TYPE_REC, -- 'B'
                                  Pk00_Const.c_BILL_TYPE_DBT, -- 'D'    -- LL. 2016/12/27. �������� ������ ��� ���������� �����
                                  Pk00_Const.c_BILL_TYPE_ADS  -- 'A'
                                 )                            
         AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_READY, 
                               Pk00_Const.c_BILL_STATE_CLOSED)
    ), BI AS (
      SELECT Q.DG_ID, SUM(I.REP_GROSS) GROSS --INTO v_gross 
        FROM ITEM_T I, Q
       WHERE Q.PERIOD_ID     = I.REP_PERIOD_ID
         AND Q.BILL_ID       = I.BILL_ID
         AND I.ITEM_TYPE     = Pk00_Const.c_ITEM_TYPE_BILL -- ��������� �������������
         AND I.CHARGE_TYPE NOT IN ( 
                               Pk00_Const.c_CHARGE_TYPE_DIS, -- ������
                               Pk00_Const.c_CHARGE_TYPE_IDL, -- ����������� ������� �� ��������
                               Pk00_Const.c_CHARGE_TYPE_SLA )
         AND I.DATE_FROM    <= Q.PERIOD_TO
         AND I.DATE_TO      >= Q.PERIOD_FROM
         AND EXISTS (
             SELECT * FROM ORDER_BODY_T OB
              WHERE OB.ORDER_ID     = I.ORDER_ID
                AND OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_DIS
                AND OB.RATE_RULE_ID = Pk00_Const.c_RATE_RULE_DIS_STD
                AND OB.DATE_FROM    < Q.PERIOD_TO
                AND (OB.DATE_TO IS NULL OR Q.PERIOD_FROM < OB.DATE_TO)
         )
       GROUP BY Q.DG_ID
    )
    SELECT DISCOUNT_PRC 
      INTO v_percent
      FROM BI, DG_PERCENT_T GP
     WHERE GP.DG_ID = BI.DG_ID
       AND GP.VALUE_MIN <= BI.GROSS
       AND BI.GROSS < GP.VALUE_MAX;

    -- ���������� ������� ������
    RETURN v_percent;
    --
EXCEPTION 
  WHEN NO_DATA_FOUND THEN
    RETURN 0;
  WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR, group_id = '||p_group_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� ������ ����������� ������ ��� ������ �� �������
-- ��������: ���� ������ ���� ������ - bill_status = 'OPEN'
PROCEDURE Make_item_std_discount( p_task_id IN INTEGER, p_percent IN NUMBER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Make_item_std_discount';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    INSERT INTO ITEM_T I(
      BILL_ID, REP_PERIOD_ID, 
      ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
      ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
      CHARGE_TYPE, 
      ITEM_TOTAL, 
      ITEM_CURRENCY_ID, 
      RECVD, 
      DATE_FROM, DATE_TO, ITEM_STATUS, 
      CREATE_DATE, LAST_MODIFIED, 
      REP_GROSS, REP_TAX, TAX_INCL, 
      EXTERNAL_ID, NOTES, ORDER_BODY_ID, DESCR
    )
    WITH I AS (   
    SELECT  
        Q.ACCOUNT_ID, I.BILL_ID, I.REP_PERIOD_ID, 
        I.ITEM_TYPE, I.ORDER_ID, I.SERVICE_ID, 
        SUM(I.ITEM_TOTAL) ITEM_TOTAL,
        I.ITEM_CURRENCY_ID,
        MIN(I.DATE_FROM) DATE_FROM, 
        MAX(I.DATE_TO) DATE_TO, 
        I.ITEM_STATUS, 
        I.TAX_INCL, 
        I.EXTERNAL_ID,
        Q.DATA_PERIOD_ID 
      FROM ITEM_T I, BILLING_QUEUE_T Q 
     WHERE Q.TASK_ID       = p_task_id
       AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID
       AND Q.BILL_ID       = I.BILL_ID
       AND I.CHARGE_TYPE  != Pk00_Const.c_CHARGE_TYPE_DIS -- ��������� ������
       AND I.ITEM_TYPE     = Pk00_Const.c_ITEM_TYPE_BILL  -- ��������� �������������
    GROUP BY 
        Q.ACCOUNT_ID,I.BILL_ID, I.REP_PERIOD_ID, 
        I.ITEM_TYPE, I.ORDER_ID, I.SERVICE_ID, 
        I.ITEM_CURRENCY_ID,
        I.ITEM_STATUS, 
        I.TAX_INCL, 
        I.EXTERNAL_ID,
        Q.DATA_PERIOD_ID
    )
    SELECT  
        I.BILL_ID, I.REP_PERIOD_ID, 
        SQ_ITEM_ID.NEXTVAL ITEM_ID, I.ITEM_TYPE, NULL INV_ITEM_ID, 
        I.ORDER_ID, I.SERVICE_ID, Pk00_Const.c_SUBSRV_DISC SUBSERVICE_ID, 
        Pk00_Const.c_CHARGE_TYPE_DIS CHARGE_TYPE, 
        -(I.ITEM_TOTAL * p_percent) / 100 ITEM_TOTAL,
        I.ITEM_CURRENCY_ID,
        0 RECVD, 
        I.DATE_FROM, I.DATE_TO, I.ITEM_STATUS, 
        SYSDATE CREATE_DATE, SYSDATE LAST_MODIFIED, 
        0 REP_GROSS, 0 REP_TAX, I.TAX_INCL, 
        I.EXTERNAL_ID, 
        p_percent||' %',
        OB.ORDER_BODY_ID, 
        NULL
      FROM I, 
           BILL_T B,
           PERIOD_T P,
           ORDER_T O, 
           ORDER_BODY_T OB
     WHERE I.ACCOUNT_ID    = O.ACCOUNT_ID
       AND I.ORDER_ID      = O.ORDER_ID
       AND OB.ORDER_ID     = O.ORDER_ID
       AND OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_DIS
       AND OB.RATE_RULE_ID = Pk00_Const.c_RATE_RULE_DIS_STD
       AND P.PERIOD_ID     = I.DATA_PERIOD_ID
       AND OB.DATE_FROM   <= P.PERIOD_TO
       AND (OB.DATE_TO IS NULL OR P.PERIOD_FROM <= OB.DATE_TO)
       AND I.DATE_FROM    <= P.PERIOD_TO
       AND I.DATE_TO      >= P.PERIOD_FROM
       AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND I.BILL_ID       = B.BILL_ID
       AND B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_OPEN -- ���� ������ ���� � ������� 'OPEN'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION 
  WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR, task_id = '||p_task_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� � ��������� ������� � ������ ������ ������� ��� ����������� ������
--
PROCEDURE Apply_std_discount( p_group_id IN INTEGER, p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Apply_std_discount';
    v_percent       NUMBER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id||', group_id = '||p_group_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ��������� ������� ������
    v_percent := Get_std_discount_percent( p_group_id, p_period_id );
    --
    -- ��������� ������� � ������ ������ �������
    Apply_discount_percent( 
                 p_group_id  => p_group_id, 
                 p_period_id => p_period_id,
                 p_percent   => v_percent 
              );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR, group_id = '||p_group_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������ ������, ��������� �� ���-�� ����� �����������
-- ����� ����������� ��� ����������� ����� �� ������ VPN
-- DG_RULE_ID = 2506
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������: ����� ������ ���� ������������
FUNCTION Get_point_discount_percent( 
           p_group_id IN INTEGER, 
           p_period_id IN INTEGER 
         ) RETURN NUMBER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Get_point_discount_percent';
    v_percent       NUMBER;
BEGIN
    -- �������� ������� ������
    WITH PNT AS ( 
      SELECT G.DG_ID, COUNT(*) VAL
        FROM DISCOUNT_GROUP_T G, DG_ACCOUNT_T GA,
             PERIOD_T P, ORDER_T O
       WHERE G.DG_ID         = p_group_id
         AND G.DG_ID         = GA.DG_ID
         AND G.DG_RULE_ID    = 2506 -- ��������� �� ������ ������
         AND P.PERIOD_ID     = p_period_id
         AND G.DATE_FROM     < P.PERIOD_TO
         AND (G.DATE_TO IS NULL OR P.PERIOD_FROM < G.DATE_TO)
         AND GA.DATE_FROM    < P.PERIOD_TO
         AND (GA.DATE_TO IS NULL OR P.PERIOD_FROM < GA.DATE_TO)
         AND GA.ACCOUNT_ID   = O.ACCOUNT_ID
         AND O.SERVICE_ID    = 106 -- ����������� ������� ���� (���/IP VPN)
         AND O.DATE_FROM     < P.PERIOD_TO
         AND (O.DATE_TO IS NULL OR P.PERIOD_FROM < O.DATE_TO)
       GROUP BY G.DG_ID
    )
    SELECT DISCOUNT_PRC 
      INTO v_percent
      FROM PNT, DG_PERCENT_T GP
     WHERE GP.DG_ID = PNT.DG_ID
       AND GP.VALUE_MIN <= PNT.VAL
       AND PNT.VAL < GP.VALUE_MAX;
    -- ���������� ������� ������
    RETURN v_percent;
    --
EXCEPTION 
  WHEN NO_DATA_FOUND THEN
    RETURN 0;
  WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR, group_id = '||p_group_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� � ��������� ������, ��������� �� ���-�� ����� �����������
-- DG_RULE_ID = 2506
--
PROCEDURE Apply_point_discount( p_group_id IN INTEGER, p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Apply_std_discount';
    v_percent       NUMBER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id||', group_id = '||p_group_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ��������� ������� ������
    v_percent := Get_point_discount_percent( p_group_id, p_period_id );
    --
    -- ��������� ������� � ������ ������ �������
    Apply_discount_percent( 
                 p_group_id  => p_group_id, 
                 p_period_id => p_period_id,
                 p_percent   => v_percent 
              );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR, group_id = '||p_group_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������� ��� �������� ������ �� ������ ������� (���)
-- ��������: ����� ������ ���� ������������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Get_minutes_discount_percent( 
           p_group_id IN INTEGER, 
           p_period_id IN INTEGER 
         ) RETURN NUMBER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Get_minutes_discount_percent';
    v_percent       NUMBER;
BEGIN
    -- �������� ������� ������
    WITH Q AS ( 
      SELECT G.DG_ID, B.BILL_ID, GA.ACCOUNT_ID, B.PROFILE_ID, 
             P.PERIOD_FROM, P.PERIOD_TO, P.PERIOD_ID 
        FROM DISCOUNT_GROUP_T G, DG_ACCOUNT_T GA,
             PERIOD_T P, BILL_T B
       WHERE G.DG_ID         = p_group_id
         AND G.DG_ID         = GA.DG_ID
         AND P.PERIOD_ID     = p_period_id
         AND G.DATE_FROM     < P.PERIOD_TO
         AND (G.DATE_TO IS NULL OR P.PERIOD_FROM < G.DATE_TO)
         AND GA.DATE_FROM    < P.PERIOD_TO
         AND (GA.DATE_TO IS NULL OR P.PERIOD_FROM < GA.DATE_TO)
         AND B.REP_PERIOD_ID = P.PERIOD_ID
         AND B.ACCOUNT_ID    = GA.ACCOUNT_ID
         AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_REC -- 'B'
         AND B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_READY, 
                               Pk00_Const.c_BILL_STATE_CLOSED)
    ), M AS (
      SELECT Q.DG_ID, SUM(DT.MINUTES) MINUTES
        FROM PIN.DETAIL_MMTS_T_JUR DT, Q
       WHERE DT.BILL_ID       = Q.BILL_ID
         AND DT.REP_PERIOD_ID = Q.PERIOD_ID
       GROUP BY Q.DG_ID
    )
    SELECT GP.DISCOUNT_PRC
      INTO v_percent
      FROM M, DG_PERCENT_T GP
     WHERE GP.DG_ID = M.DG_ID
       AND GP.VALUE_MIN <= M.MINUTES
       AND M.MINUTES    <  GP.VALUE_MAX
    ;
    -- ���������� ������� ������
    RETURN v_percent;
    --
EXCEPTION 
  WHEN NO_DATA_FOUND THEN
    RETURN 0;
  WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR, group_id = '||p_group_id, c_PkgName||'.'||v_prcName );
END;

-- ��������� � ��������� ��������� ������, ��������� �� ���-�� ����� �������
PROCEDURE Apply_minutes_discount( p_group_id IN INTEGER, p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Apply_minutes_discount';
    v_percent       NUMBER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id||', group_id = '||p_group_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ��������� ������� ������
    v_percent := Get_minutes_discount_percent( p_group_id, p_period_id );
    --
    -- ��������� ������� � ������ ������ �������
    Apply_discount_percent( 
                 p_group_id  => p_group_id, 
                 p_period_id => p_period_id,
                 p_percent   => v_percent 
              );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR, group_id = '||p_group_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������, ��� ��� ������ � ������� ������� ��������������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_dg_volume_orders( p_group_id IN INTEGER, p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Check_dg_volume_orders';
    v_period_from   DATE;
    v_period_to     DATE;
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, group_id = '||p_group_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);

    INSERT INTO ORDER_BODY_T OB (
           ORDER_ID, ORDER_BODY_ID, CHARGE_TYPE, 
           SUBSERVICE_ID, DATE_FROM, DATE_TO, RATE_RULE_ID 
    )
    SELECT O.ORDER_ID, SQ_ORDER_ID.NEXTVAL ORDER_BODY_ID, 
           Pk00_Const.c_CHARGE_TYPE_DIS CHARGE_TYPE, 
           Pk00_Const.c_SUBSRV_DISC SUBSERVICE_ID, 
           v_period_from DATE_FROM, O.DATE_TO, 
           Pk00_Const.c_RATE_RULE_DIS_STD RATE_RULE_ID 
      FROM ORDER_T O
     WHERE O.ACCOUNT_ID IN (
            SELECT DA.ACCOUNT_ID 
              FROM DISCOUNT_GROUP_T DG, DG_ACCOUNT_T DA
             WHERE DG.DG_RULE_ID = 2505
               AND DG.DATE_FROM < v_period_to
               AND (DG.DATE_TO IS NULL OR v_period_from < DG.DATE_TO)
               AND DG.DG_ID = DA.DG_ID
       )
       AND O.DATE_FROM < v_period_to
       AND (O.DATE_TO IS NULL OR v_period_from < O.DATE_TO)
       AND NOT EXISTS (
          SELECT * FROM ORDER_BODY_T OB
           WHERE OB.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_DIS
             AND OB.ORDER_ID = O.ORDER_ID
       )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR, group_id = '||p_group_id, c_PkgName||'.'||v_prcName );
END;



-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ������ ���������� � ����� �� �������
-- ��������!
-- 1) ���� ������� ���� �������������: BILL_STATUS = 'OPEN'
-- 2) ������, ��� ��������� �����, ����� �������� � ��������� ������� 
-- �� ������� � ������, �� ������ �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_bill_discount( p_task_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_bill_discount';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- 1) ������� item-� ������, ����������� ��� ������ �� �������
    DELETE FROM ITEM_T I
     WHERE I.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_DIS
       AND I.ITEM_TYPE   = Pk00_Const.c_ITEM_TYPE_BILL -- ������������� �� �������
       AND EXISTS (
           SELECT * 
             FROM BILLING_QUEUE_T Q, PERIOD_T P
            WHERE I.REP_PERIOD_ID = Q.REP_PERIOD_ID
              AND I.BILL_ID       = Q.BILL_ID
              AND P.PERIOD_ID     = Q.DATA_PERIOD_ID
              AND I.DATE_FROM    <= P.PERIOD_TO
              AND I.DATE_TO      >= P.PERIOD_FROM
              AND Q.TASK_ID       = p_task_id
       )
       AND NOT EXISTS (  -- ���� ������ ���� � ������� 'OPEN'
           SELECT *
             FROM BILL_T B, BILLING_QUEUE_T Q
            WHERE B.REP_PERIOD_ID = Q.REP_PERIOD_ID
              AND B.BILL_ID       = Q.BILL_ID
              AND B.BILL_STATUS  != Pk00_Const.c_BILL_STATE_OPEN
              AND Q.TASK_ID       = p_task_id
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows for DIS deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ������ �� ������, ����������� �� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_group_discount( p_group_id IN INTEGER, p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_group_discount';
    v_period_from   DATE;
    v_period_to     DATE;
    v_count         INTEGER;
    v_task_id       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);
    --
    -- 1) �������� ��������� ������������� ������
    v_task_id := PK30_BILLING_QUEUE.Open_task;
    
    -- 2) ��������� ������� �� �������� ������, ����������� � ������� ������ �� ������
    INSERT INTO BILLING_QUEUE_T Q (BILL_ID, ACCOUNT_ID, TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID)
    SELECT DISTINCT BILL_ID, ACCOUNT_ID, v_task_id TASK_ID, REP_PERIOD_ID, REP_PERIOD_ID 
      FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_period_id
       AND B.BILL_TYPE = Pk00_Const.c_BILL_TYPE_REC -- ������ ������������� �����
       AND B.ACCOUNT_ID IN (
      SELECT DA.ACCOUNT_ID 
        FROM DISCOUNT_GROUP_T DG, DG_ACCOUNT_T DA 
       WHERE DG.DG_ID = DA.DG_ID
         AND DA.DATE_FROM < v_period_to
         AND (DA.DATE_TO IS NULL OR v_period_from < DA.DATE_TO)
         AND DG.DG_ID = p_group_id
         );
    
    -- 3) ���������������� �����
    Pk30_Billing_Base.Rollback_bills(p_task_id => v_task_id);
    
    -- 4) ������� item-� ������, ����������� ��� ���������� �������
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_DIS
       AND I.ITEM_TYPE   = Pk00_Const.c_ITEM_TYPE_BILL -- ������������� �� �������
       AND EXISTS (
           SELECT * 
             FROM BILLING_QUEUE_T Q, PERIOD_T P
            WHERE I.REP_PERIOD_ID = Q.REP_PERIOD_ID
              AND I.BILL_ID       = Q.BILL_ID
              AND P.PERIOD_ID     = Q.DATA_PERIOD_ID
              AND I.DATE_FROM    <= P.PERIOD_TO
              AND I.DATE_TO      >= P.PERIOD_FROM
              AND Q.TASK_ID       = v_task_id
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows for DIS deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
           
    -- 5) ��������� �����
    PK30_BILLING_BASE.Make_bills( p_task_id => v_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ���� ��������� ������, ����������� �� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_discounts( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_discounts';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_count := 0;
    FOR dg IN (
      SELECT DG_ID
        FROM DISCOUNT_GROUP_T DG, PERIOD_T P
       WHERE P.PERIOD_ID = p_period_id
         AND DG.DATE_FROM < P.PERIOD_TO
         AND (DG.DATE_TO IS NULL OR P.PERIOD_FROM < DG.DATE_TO )
         AND DG_RULE_ID NOT IN ( 2502, 2503, 2507, 2508 ) -- ��������� � ������������� ������� ��������
    )
    LOOP
        -- ��������� ������ ��� ������
        Rollback_group_discount( dg.Dg_Id, p_period_id );
        v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('Rollback '||v_count||' - std_discounts', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������������ � ��������� ������ ��� ������������ �����
-- ���� ������ ���� �������������: BILL_STATUS = 'READY'
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Recalc_discount_for_bill( 
             p_bill_id    IN INTEGER,  -- id - ���������� �����, � ������� ������� ������
             p_period_id  IN INTEGER   -- ������ �����
          )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Recalc_discount_for_debet';
    v_group_id      INTEGER;  -- ��������� ������ ������� ����������� ����
    v_account_id    INTEGER;
    v_profile_id    INTEGER;
    v_percent       NUMBER;
    v_task_id       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id||', bill_id = '||p_bill_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- �������� ������ ������ ��� �����
    SELECT GA.ACCOUNT_ID, B.PROFILE_ID, G.DG_ID
      INTO v_account_id, v_profile_id, v_group_id
      FROM DISCOUNT_GROUP_T G, DG_ACCOUNT_T GA, BILL_T B, PERIOD_T P
     WHERE P.PERIOD_ID     = p_period_id
       AND G.DATE_FROM     < P.PERIOD_TO
       AND (G.DATE_TO IS NULL OR P.PERIOD_FROM < G.DATE_TO)
       AND GA.DATE_FROM    < P.PERIOD_TO
       AND (GA.DATE_TO IS NULL OR P.PERIOD_FROM < GA.DATE_TO)
       AND G.DG_ID         = GA.DG_ID
       AND B.ACCOUNT_ID    = GA.ACCOUNT_ID
       AND B.REP_PERIOD_ID = P.PERIOD_ID
       AND B.BILL_ID       = p_bill_id
       AND B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_READY;
    
    -- ��������� ������� ������
    v_percent := Get_std_discount_percent( v_group_id, p_period_id );
    --
    IF v_percent > 0 THEN 
      
        -- 1) ���������, ��� ��� ������ � ������� ������� ��������������
        Check_dg_volume_orders( v_group_id, p_period_id );  
    
        -- 2) �������� ��������� ������������� ������
        v_task_id := PK30_BILLING_QUEUE.Open_task;
        
        -- 3) ��������� ������� 
        INSERT INTO BILLING_QUEUE_T(BILL_ID, ACCOUNT_ID, 
                                    TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID)
        VALUES(p_bill_id, v_account_id, 
               v_task_id, p_period_id, p_period_id
        );
        
        -- 4) ���������������� ����
        Pk30_Billing.Rollback_bills( v_task_id );
        
        -- 5) ������� ����� ����������� ������, ���� ����
        Rollback_bill_discount( v_task_id );
       
        -- 6) ��������� ������� ������ � ���� �� ������ ��������� ������
        Make_item_std_discount( v_task_id, v_percent );
        
        -- 7) ��������� ����
        Pk30_Billing.Remake_bills( v_task_id );
        
    END IF;

    -- 8) ����������� ������
    PK30_BILLING_QUEUE.Close_task(p_task_id => v_task_id);    

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION 
  WHEN NO_DATA_FOUND THEN
    Pk01_Syslog.Write_error('ERROR, bill_id = '||p_bill_id, c_PkgName||'.'||v_prcName );
  WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR, bill_id = '||p_bill_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������������ � ��������� ������ ��� �����-����
-- ��������� ���� ������ ���� �������������: BILL_STATUS = 'OPEN'
-- � ���������� � ������� 'CLOSED'
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Recalc_discount_for_debet( 
             p_dbt_bill_id    IN INTEGER,  -- id - ���������� �����, � ������� ������� ������
             p_dbt_period_id  IN INTEGER,  -- ������ ���������� �����
             p_crd_period_id  IN INTEGER   -- ������ ����������� �����, ��� �������� ������������� ������
          )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Recalc_discount_for_debet';
    v_group_id      INTEGER;  -- ��������� ������ ������� ����������� ����
    v_account_id    INTEGER;
    v_profile_id    INTEGER;
    v_percent       NUMBER;
    v_task_id       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_dbt_period_id||', bill_id = '||p_dbt_bill_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- �������� ������ ������ ��� �����
    SELECT GA.ACCOUNT_ID, B.PROFILE_ID, G.DG_ID
      INTO v_account_id, v_profile_id, v_group_id
      FROM DISCOUNT_GROUP_T G, DG_ACCOUNT_T GA, BILL_T B, PERIOD_T P
     WHERE P.PERIOD_ID     = p_crd_period_id
       AND G.DATE_FROM     < P.PERIOD_TO
       AND (G.DATE_TO IS NULL OR P.PERIOD_FROM < G.DATE_TO)
       AND GA.DATE_FROM    < P.PERIOD_TO
       AND (GA.DATE_TO IS NULL OR P.PERIOD_FROM < GA.DATE_TO)
       AND G.DG_ID         = GA.DG_ID
       AND B.ACCOUNT_ID    = GA.ACCOUNT_ID
       AND B.REP_PERIOD_ID = p_dbt_period_id
       AND B.BILL_ID       = p_dbt_bill_id
       AND B.BILL_TYPE     IN (Pk00_Const.c_BILL_TYPE_DBT, Pk00_Const.c_BILL_TYPE_ADS)
       AND B.BILL_STATUS   IN (Pk00_Const.c_BILL_STATE_READY, 
                               Pk00_Const.c_BILL_STATE_CLOSED);
    
    -- ��������� ������� ������
    v_percent := Get_std_discount_percent( v_group_id, p_crd_period_id );
    --
    IF v_percent > 0 THEN 
        -- 1) �������� ��������� ������������� ������
        v_task_id := PK30_BILLING_QUEUE.Open_task;
        
        -- 2) ��������� ������� 
        INSERT INTO BILLING_QUEUE_T(BILL_ID, ACCOUNT_ID, 
                                    TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID)
        VALUES(p_dbt_bill_id, v_account_id, 
               v_task_id, p_dbt_period_id, p_crd_period_id
        );
        --
        -- LL 2016/12/27. ���������������� ����
        pk30_billing.rollback_bills(v_task_id);
        
        -- 4) ������� ����� ����������� ������, ���� ����
        Rollback_bill_discount( v_task_id );
        
        -- 5) ��������� ������� ������ � ���� �� ������ ��������� ������
        Make_item_std_discount( v_task_id, v_percent );
        
        -- LL 2016/12/27. ������������ ����
        pk30_billing.Remake_bills(v_task_id);
        
    END IF;

    -- 8) ����������� ������
    PK30_BILLING_QUEUE.Close_task(p_task_id => v_task_id);    

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION 
  WHEN NO_DATA_FOUND THEN
    Pk01_Syslog.Write_error('ERROR, bill_id = '||p_dbt_bill_id, c_PkgName||'.'||v_prcName );
  WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR, bill_id = '||p_dbt_bill_id, c_PkgName||'.'||v_prcName );
END;

END PK39_BILLING_DISCOUNT;
/
