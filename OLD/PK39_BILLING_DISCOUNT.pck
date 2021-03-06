CREATE OR REPLACE PACKAGE PK39_BILLING_DISCOUNT
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK39_BILLING_DISCOUNT';
    -- ==============================================================================
    -- ������ ��������� ������, ������ ������� ����� ����������� ������:
    -- ������ ��� �������� � ��������:
    -- discount_group_t, dg_account_t, dg_percent_t
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ���� ��������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Apply_discounts( p_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ������ ��������� ������
    -- DG_RULE_ID = 2501   - ����������� ������ ������ �� ������� DG_PERCENT_T
    -- RATE_RULE_ID = 2412 - ���������� ��������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Apply_std_discount( p_group_id IN INTEGER, p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������, ��� ��� ������ � ������� ������� ��������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Check_dg_volume_orders( p_group_id IN INTEGER, p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����� ���� ��������� ������, ����������� �� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Rollback_discounts( p_period_id IN INTEGER );
    
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
    v_period_from   DATE;
    v_period_to     DATE;
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ������ ��������� ������
    -- DG_RULE_ID   = 2501 - ����������� ������ ������ �� ������� DG_PERCENT_T
    -- RATE_RULE_ID = 2412 - ���������� ��������� ������
    --
    v_count := 0;
    FOR dg IN (
      SELECT DG_ID, DG_RULE_ID 
        FROM DISCOUNT_GROUP_T
       WHERE DG_RULE_ID = 2501    -- 
        AND DATE_FROM   < v_period_to
        AND (DATE_TO IS NULL OR v_period_from < DATE_TO)
    )
    LOOP
        -- ��� ������ �� ������, ��������� ��� ��� ������ �������������� 
        IF dg.dg_rule_id = 2505 THEN
           Check_dg_volume_orders( p_group_id => dg.dg_id, p_period_id => p_period_id );
        END IF;
        -- ��������� ������
        Apply_std_discount( p_group_id => dg.dg_id, p_period_id => p_period_id );
        v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg(v_count||' - std_discounts', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ������������� ����� ������ ���, ���������, ...


    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����������� ������ ��������� ������
-- DG_RULE_ID = 2501   - ����������� ������ ������ �� ������� DG_PERCENT_T
-- RATE_RULE_ID = 2412 - ���������� ��������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Apply_std_discount( p_group_id IN INTEGER, p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Apply_std_discount';
    v_period_from   DATE;
    v_period_to     DATE;
    v_count         INTEGER;
    v_gross         NUMBER;
    v_percent       NUMBER;
    v_task_id       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id||', group_id = '||p_group_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);
    --
    -- 1) �������� ��������� ������������� ������
    v_task_id := PK30_BILLING_QUEUE.Open_task;
    
    -- 2) ��������� ������� � �������� ����������
    INSERT INTO BILLING_QUEUE_T(BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, 
                                TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID)
    SELECT B.BILL_ID, GA.ACCOUNT_ID, NULL BILLING_ID, B.PROFILE_ID, 
           v_task_id, p_period_id, p_period_id
      FROM DISCOUNT_GROUP_T G, DG_ACCOUNT_T GA, BILL_T B
     WHERE G.DG_ID         = p_group_id
       AND G.DATE_FROM     < v_period_to
       AND (G.DATE_TO IS NULL OR v_period_from < G.DATE_TO)
       AND GA.DATE_FROM    < v_period_to
       AND (GA.DATE_TO IS NULL OR v_period_from < GA.DATE_TO)
       AND G.DG_ID         = GA.DG_ID
       AND B.ACCOUNT_ID    = GA.ACCOUNT_ID
       AND B.BILL_TYPE     = PK00_CONST.c_BILL_TYPE_REC
       AND B.REP_PERIOD_ID = p_period_id;
    --
    v_count := SQL%ROWCOUNT;
    COMMIT;
    Pk01_Syslog.Write_msg('BILLING_QUEUE_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Gather_Table_Stat(l_Tab_Name => 'BILLING_QUEUE_T');
   
    -- ����� ���� ������ ���
    IF v_count > 0 THEN
      
      -- 3) ��������� ������ ������ - ������ ���� 'READY' ��� 'CHECK'
      SELECT COUNT(*) INTO v_count
        FROM BILL_T B, BILLING_QUEUE_T Q
       WHERE B.REP_PERIOD_ID = p_period_id
         AND B.BILL_ID = Q.BILL_ID
         AND B.BILL_STATUS NOT IN (Pk00_Const.c_BILL_STATE_READY, 
                                   Pk00_Const.c_BILL_STATE_CHECK);
      IF v_count != 0 THEN
         Pk01_Syslog.Raise_user_exception(p_Msg => v_count||' ������ �� ����� ������ "READY"',
                                          p_Src => c_PkgName||'.'||v_prcName);
      END IF;

      -- 4) ������� ����� ����������� ������, ���� ����
      DELETE FROM ITEM_T I
       WHERE I.ITEM_TYPE = Pk00_Const.c_ITEM_TYPE_BILL
         AND I.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_DIS
         AND EXISTS ( 
         SELECT * 
           FROM BILLING_QUEUE_T Q
          WHERE Q.TASK_ID = v_task_id
            AND Q.BILL_ID = I.BILL_ID
            AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID
       );
      v_count := SQL%ROWCOUNT;
      Pk01_Syslog.Write_msg('ITEM_T '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );


      -- 5) �������� ����� ���������� (��� �������) �� ��������� ������� ������ ������
      SELECT SUM(I.REP_GROSS) INTO v_gross 
        FROM ITEM_T I, BILL_T B,
             BILLING_QUEUE_T Q
       WHERE Q.TASK_ID       = v_task_id
         AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID
         AND Q.BILL_ID       = I.BILL_ID
         AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
         AND I.BILL_ID       = B.BILL_ID
         AND I.ITEM_TYPE     = Pk00_Const.c_ITEM_TYPE_BILL -- ��������� �������������
         AND I.CHARGE_TYPE NOT IN ( 
                               Pk00_Const.c_CHARGE_TYPE_IDL, -- ����������� ������� �� ��������
                               Pk00_Const.c_CHARGE_TYPE_SLA )
         AND EXISTS (
             SELECT * FROM ORDER_T O, ORDER_BODY_T OB
              WHERE B.ACCOUNT_ID    = O.ACCOUNT_ID
                AND I.ORDER_ID      = O.ORDER_ID
                AND OB.ORDER_ID     = O.ORDER_ID
                AND OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_DIS
                AND OB.RATE_RULE_ID = Pk00_Const.c_RATE_RULE_DIS_STD
                AND OB.DATE_FROM    < v_period_to
                AND (OB.DATE_TO IS NULL OR v_period_from < OB.DATE_TO)
         );
      Pk01_Syslog.Write_msg('ITEM_T.GROSS = '||v_gross, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

      -- 5) �������� ������� ������
      SELECT GP.DISCOUNT_PRC INTO v_percent 
        FROM DG_PERCENT_T GP
       WHERE GP.DG_ID = p_group_id
         AND v_gross BETWEEN GP.VALUE_MIN AND GP.VALUE_MAX;
         
      Pk01_Syslog.Write_msg('PERCENT_T '||v_percent||' %', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
         
      --
      IF v_percent > 0 THEN
        --
        -- 5) ���������������� �����
        Pk30_Billing_Queue.Rollback_bills(v_task_id);
        
        -- 6) ��������� ������� ������ � ���� �� ������ ��������� ������
        -- � �������� ������ TAX_INCL !!!! (���� ������, ��� TAX_INCL = 'N')
        INSERT INTO ITEM_T I(
          BILL_ID, REP_PERIOD_ID, 
          ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
          ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
          CHARGE_TYPE, 
          ITEM_TOTAL, 
          RECVD, 
          DATE_FROM, DATE_TO, ITEM_STATUS, 
          CREATE_DATE, LAST_MODIFIED, 
          REP_GROSS, REP_TAX, TAX_INCL, 
          EXTERNAL_ID, NOTES, ORDER_BODY_ID, DESCR
        )
        SELECT  
            I.BILL_ID, I.REP_PERIOD_ID, 
            SQ_ITEM_ID.NEXTVAL ITEM_ID, I.ITEM_TYPE, NULL INV_ITEM_ID, 
            I.ORDER_ID, I.SERVICE_ID, Pk00_Const.c_SUBSRV_DISC SUBSERVICE_ID, 
            Pk00_Const.c_CHARGE_TYPE_DIS CHARGE_TYPE, 
            ROUND(-(I.ITEM_TOTAL * v_percent) / 100,2) ITEM_TOTAL,
            I.RECVD, 
            I.DATE_FROM, I.DATE_TO, I.ITEM_STATUS, 
            SYSDATE CREATE_DATE, SYSDATE LAST_MODIFIED, 
            I.REP_GROSS, I.REP_TAX, I.TAX_INCL, 
            I.EXTERNAL_ID, I.NOTES, OB.ORDER_BODY_ID, v_percent||' ���������'
          FROM ITEM_T I, BILL_T B,
               BILLING_QUEUE_T Q,
               ORDER_T O, 
               ORDER_BODY_T OB
         WHERE Q.TASK_ID       = v_task_id
           AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID
           AND Q.BILL_ID       = I.BILL_ID
           AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
           AND B.BILL_ID       = I.BILL_ID
           AND B.ACCOUNT_ID    = O.ACCOUNT_ID
           AND I.ORDER_ID      = O.ORDER_ID
           AND I.ITEM_TYPE     = Pk00_Const.c_ITEM_TYPE_BILL -- ��������� �������������
           AND I.CHARGE_TYPE NOT IN ( 
                                 Pk00_Const.c_CHARGE_TYPE_IDL, -- ����������� ������� �� ��������
                                 Pk00_Const.c_CHARGE_TYPE_SLA)
           AND OB.ORDER_ID     = O.ORDER_ID
           AND OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_DIS
           AND OB.RATE_RULE_ID = Pk00_Const.c_RATE_RULE_DIS_STD
           AND OB.DATE_FROM     < v_period_to
           AND (OB.DATE_TO IS NULL OR v_period_from < OB.DATE_TO)
        ;
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('ITEM_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        
        -- 7) ��������� �����
        Pk30_Billing_Queue.Close_bills(v_task_id);

      END IF;
      --
    END IF;

    -- N) ����������� ������
    PK30_BILLING_QUEUE.Close_task(p_task_id => v_task_id);    

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
-- ����� ���� ��������� ������, ����������� �� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_discounts( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_discounts';
    v_period_from   DATE;
    v_period_to     DATE;
    v_count         INTEGER;
    v_task_id       INTEGER;
    v_current_month DATE := TRUNC(SYSDATE, 'mm');
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);
    --
    -- 1) �������� ��������� ������������� ������
    v_task_id := PK30_BILLING_QUEUE.Open_task;
    
    -- 2) ��������� ������� �� �������� ������, ����������� � ������� ������
    INSERT INTO BILLING_QUEUE_T Q (BILL_ID, ACCOUNT_ID, PROFILE_ID, TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID)
    SELECT BILL_ID, ACCOUNT_ID, PROFILE_ID, v_task_id TASK_ID, REP_PERIOD_ID, REP_PERIOD_ID 
      FROM BILL_T B
     WHERE B.BILL_ID IN (
        SELECT I.BILL_ID FROM ITEM_T I
         WHERE I.REP_PERIOD_ID = p_period_id
           AND I.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_DIS
           AND I.CREATE_DATE > v_current_month
     );
    
    -- 3) ���������������� �����
    Pk30_Billing_Queue.Rollback_bills(p_task_id => v_task_id);
    
    -- 4) ������� item-� ������, ����������� � ������� ������
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_DIS
       AND I.CREATE_DATE > v_current_month;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows for DIS deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
           
    -- 5) ��������� �����
    PK30_BILLING_BASE.Make_bills( p_task_id => v_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

END PK39_BILLING_DISCOUNT;
/
