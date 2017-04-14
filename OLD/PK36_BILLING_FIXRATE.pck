CREATE OR REPLACE PACKAGE PK36_BILLING_FIXRATE
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK36_BILLING_FIXRATE';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    --=============================================================================--
    --                ������ ���������� �� ������� �������                         --
    --=============================================================================--
    -- �������� ������������� ������ ��� �������� �������
    -- ��������� ��� ������� �� ����������� ���������
    -- � ����������� ������� p_period_id
    PROCEDURE Make_bills_for_fixrates(p_period_id IN INTEGER);

    --  ������ ��������� (subscriber fee)
    PROCEDURE Charge_ABP( p_task_id IN INTEGER );

    --  ������ ��������� (subscriber fee) �� ������� �������� 
    -- ������������� ��� ������� ������ (MONTH_TARIFF_T),
    -- ����� ����� ������ ������� ��������� �����������, 
    -- ����������� ������� �� ���� ���, ����� ������ � ����� �������
    -- ��� ������������ ������� p_period_id
    PROCEDURE Charge_ABP_by_month_tariff( p_task_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  ��������� ���� ����������� ��� ��������� ����������� �� ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Put_ABP_detail( p_task_id IN INTEGER );

    --  ������ ������� �� ������������ ����� ���������� ������ ������
    PROCEDURE Subservice_charge_MIN( p_task_id IN INTEGER );

    --  ������ ������� �� ������������ ����� ������
    PROCEDURE Order_charge_MIN( p_task_id IN INTEGER );
    
    --  ������ ������� �� ������������ ����� �������� �����
    PROCEDURE Account_charge_MIN( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������������� ������������� ����������,
    -- �� ����������� ��� ��� ����������� ����������� �������
    PROCEDURE Rollback_fixrates( p_task_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ����������� ����� � ������� �� ����������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Charge_fixrates( p_task_id IN INTEGER );
       
    
END PK36_BILLING_FIXRATE;
/
CREATE OR REPLACE PACKAGE BODY PK36_BILLING_FIXRATE
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������� ������ ��� �������� �������
-- ��������� ��� ������� �� ����������� ���������
-- � ����������� ������� p_period_id
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Make_bills_for_fixrates(p_period_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Make_bills_for_fixrates';
    v_period_from   DATE;
    v_period_to     DATE;
    v_bill_id       INTEGER;
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������������� ����� � ����������� ������� ��� �/� ��� �� ���
    v_count := 0;
    --
    FOR rb IN (
      SELECT DISTINCT A.ACCOUNT_ID 
        FROM ORDER_BODY_T OB, ORDER_T O, ACCOUNT_T A
       WHERE OB.CHARGE_TYPE IN (PK00_CONST.c_CHARGE_TYPE_MIN, PK00_CONST.c_CHARGE_TYPE_REC)
         AND OB.ORDER_ID = O.ORDER_ID 
         AND A.ACCOUNT_ID = O.ACCOUNT_ID
         AND A.STATUS = Pk00_Const.c_ACC_STATUS_BILL
         AND O.DATE_FROM <= v_period_to
         AND ( O.DATE_TO IS NULL OR v_period_from <= O.DATE_TO) 
         AND OB.DATE_FROM <= v_period_to
         AND ( OB.DATE_TO IS NULL OR v_period_from <=  OB.DATE_TO)
         AND NOT EXISTS (
            SELECT * FROM BILL_T B
             WHERE B.REP_PERIOD_ID = p_period_id
               AND B.ACCOUNT_ID    = A.ACCOUNT_ID
               AND B.BILL_TYPE     = PK00_CONST.c_BILL_TYPE_REC
         )
    )LOOP
      v_bill_id := Pk07_BIll.Next_recuring_bill (
               p_account_id    => rb.account_id, -- ID �������� �����
               p_rep_period_id => p_period_id    -- ID ���������� ������� YYYYMM
           );
      v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('Bill_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  ������ ��������� (subscriber fee)
-- ��� ������������ ������� p_period_id
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_ABP( p_task_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Charge_ABP';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ���������� �� ��������� ��� ������ ������������ �������
    --
    INSERT INTO ITEM_T I(
       BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
       CHARGE_TYPE, ITEM_TOTAL, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
       ITEM_STATUS, ORDER_BODY_ID
    )
    WITH ORD AS (   -- ������, ������ ���������� ������ - ���������
        SELECT  ACCOUNT_ID, BILL_ID, REP_PERIOD_ID,
                ORDER_ID, ORDER_BODY_ID, QUANTITY, VALUE, TAX_INCL, 
                CHARGE_TYPE, SERVICE_ID, SUBSERVICE_ID,
                DATE_FROM, DATE_TO,
                ROUND(DATE_TO-DATE_FROM) ORD_DAYS,
                MON_DAYS
          FROM (
            -- -------------------------------------------------------------------- --
            SELECT Q.ACCOUNT_ID, Q.BILL_ID, Q.REP_PERIOD_ID,
                   OB.ORDER_ID, OB.ORDER_BODY_ID, 
                   OB.QUANTITY, 
                   CASE
                     WHEN OB.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN (OB.RATE_VALUE * 28.6)
                     ELSE OB.RATE_VALUE
                   END VALUE,
                   OB.TAX_INCL, 
                   OB.CHARGE_TYPE, O.SERVICE_ID, OB.SUBSERVICE_ID,
                   CASE
                    WHEN GREATEST(O.DATE_FROM, OB.DATE_FROM) <= P.PERIOD_FROM  
                    THEN P.PERIOD_FROM ELSE GREATEST(O.DATE_FROM, OB.DATE_FROM) 
                   END DATE_FROM,
                   CASE
                    WHEN LEAST(NVL(O.DATE_TO,P.PERIOD_TO), NVL(OB.DATE_TO,P.PERIOD_TO)) >= P.PERIOD_TO  
                    THEN P.PERIOD_TO  ELSE LEAST(NVL(O.DATE_TO,P.PERIOD_TO), NVL(OB.DATE_TO,P.PERIOD_TO))
                   END DATE_TO,
                   ROUND(P.PERIOD_TO - P.PERIOD_FROM) MON_DAYS,
                   Q.TASK_ID
              FROM ORDER_BODY_T OB, ORDER_T O, BILLING_QUEUE_T Q, PERIOD_T P
             WHERE OB.CHARGE_TYPE       = Pk00_Const.c_CHARGE_TYPE_REC
               AND OB.RATE_RULE_ID IN  (  Pk00_Const.c_RATE_RULE_ABP_STD,  -- 2402
                                          Pk00_Const.c_RATE_RULE_ABP_FREE_MIN ) -- 2403
               AND O.ORDER_ID           = OB.ORDER_ID
               AND P.PERIOD_ID          = NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID) 
               AND Q.ACCOUNT_ID         = O.ACCOUNT_ID               
               AND OB.DATE_FROM        <= P.PERIOD_TO
               AND (P.PERIOD_FROM      <= OB.DATE_TO OR OB.DATE_TO IS NULL)
               AND O.DATE_FROM         <= P.PERIOD_TO
               AND (P.PERIOD_FROM      <= O.DATE_TO OR O.DATE_TO IS NULL)
               AND Q.TASK_ID            = p_task_id
               AND OB.RATE_VALUE IS NOT NULL
               AND OB.QUANTITY   IS NOT NULL
            -- -------------------------------------------------------------------- --
            )
    ), LCK AS (    -- ����������, ������������ �������
        SELECT BILL_ID, REP_PERIOD_ID,
               ORDER_ID, ROUND(SUM(LOCK_DAYS)) LCK_DAYS
          FROM (
            -- -------------------------------------------------------------------- --
            SELECT Q.BILL_ID, Q.REP_PERIOD_ID, L.ORDER_ID, 
                   CASE 
                    -- ������������ ���� �����:  DATE_FROM---[-------]---DATE_TO
                    WHEN L.DATE_FROM <= P.PERIOD_FROM 
                     AND (P.PERIOD_TO <= L.DATE_TO OR L.DATE_TO IS NULL) 
                    THEN P.PERIOD_TO - P.PERIOD_FROM
                    -- ������������ ������ ������: [--DATE_FROM-----DATE_TO--]
                    WHEN P.PERIOD_FROM < L.DATE_FROM 
                     AND L.DATE_TO < P.PERIOD_TO 
                    THEN L.DATE_TO - L.DATE_FROM
                    -- ������������ � ���������� �������, ������ � �������: ---DATE_FROM-- [---DATE_TO---]
                    WHEN L.DATE_FROM <= P.PERIOD_FROM 
                     AND L.DATE_TO < P.PERIOD_TO 
                    THEN L.DATE_TO - P.PERIOD_FROM
                    -- ������������ � ������� �������, � �������� �� ����� �������:---[--DATE_FROM--]---DATE_TO---  
                    WHEN P.PERIOD_FROM < L.DATE_FROM  
                     AND (P.PERIOD_TO <= L.DATE_TO OR L.DATE_TO IS NULL)
                    THEN P.PERIOD_TO - L.DATE_FROM
                    -- �������� ������������ ����
                    ELSE 0
                   END LOCK_DAYS
              FROM ORDER_LOCK_T L, ORDER_T O, BILLING_QUEUE_T Q, PERIOD_T P
            WHERE L.DATE_FROM    <= P.PERIOD_TO                     -- ������ ���������� 
              AND (L.DATE_TO IS NULL OR P.PERIOD_FROM <= L.DATE_TO) -- ������������� � �������
              AND O.ORDER_ID      = L.ORDER_ID
              AND Q.ACCOUNT_ID    = O.ACCOUNT_ID
              AND P.PERIOD_ID     = NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID)
              AND Q.TASK_ID       = p_task_id
            -- -------------------------------------------------------------------- --
        )
        GROUP BY BILL_ID, REP_PERIOD_ID, ORDER_ID
    ), ABP AS (
        SELECT ORD.ACCOUNT_ID, ORD.BILL_ID, ORD.REP_PERIOD_ID,
               ORD.ORDER_ID, ORD.ORDER_BODY_ID, 
               ORD.QUANTITY, ORD.VALUE, ORD.TAX_INCL, 
               ORD.CHARGE_TYPE, ORD.SERVICE_ID, ORD.SUBSERVICE_ID, 
               ORD.DATE_FROM, ORD.DATE_TO,
               ORD.ORD_DAYS, ORD.MON_DAYS, NVL(LCK.LCK_DAYS,0) LCK_DAYS,
               CASE
                WHEN ORD.ORD_DAYS < NVL(LCK.LCK_DAYS,0) THEN 1
                ELSE ((ORD.ORD_DAYS - NVL(LCK.LCK_DAYS,0))/MON_DAYS)
               END K_DAYS
          FROM ORD, LCK
         WHERE ORD.ORDER_ID      = LCK.ORDER_ID(+)
           AND ORD.BILL_ID       = LCK.BILL_ID(+) 
           AND ORD.REP_PERIOD_ID = LCK.REP_PERIOD_ID(+)
    )
    SELECT B.BILL_ID, B.REP_PERIOD_ID, SQ_ITEM_ID.NEXTVAL ITEM_ID,
           ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE,  
           ROUND((K_DAYS * QUANTITY * VALUE),2) ITEM_TOTAL,
           DATE_FROM, DATE_TO, TAX_INCL, 
           PK00_CONST.c_ITEM_TYPE_BILL,
           Pk00_Const.c_ITEM_STATE_OPEN,
           ABP.ORDER_BODY_ID
      FROM ABP, BILL_T B
     WHERE ABP.ACCOUNT_ID    = B.ACCOUNT_ID
       AND ABP.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND ABP.BILL_ID       = B.BILL_ID
       AND B.BILL_TYPE     IN (Pk00_Const.c_BILL_TYPE_REC, 
                               Pk00_Const.c_BILL_TYPE_DBT, 
                               Pk00_Const.c_BILL_TYPE_OLD)
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  ������ ��������� (subscriber fee) �� ������� �������� 
-- ������������� ��� ������� ������ (MONTH_TARIFF_T),
-- ����� ����� ������ ������� ��������� �����������, 
-- ����������� ������� �� ���� ���, ����� ������ � ����� �������
-- ��� ������������ ������� p_period_id
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_ABP_by_month_tariff(p_task_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Charge_ABP_by_month_tariff';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ���������� �� ��������� ��� ������ ������������ �������
    --
    INSERT INTO ITEM_T I(
       BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
       CHARGE_TYPE, ITEM_TOTAL, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
       ITEM_STATUS, ORDER_BODY_ID
    )
    WITH ORD AS (   -- ������, ������ ���������� ������ - ���������
        SELECT  ACCOUNT_ID, BILL_ID, REP_PERIOD_ID,
                ORDER_ID, ORDER_BODY_ID, QUANTITY, VALUE, TAX_INCL, 
                CHARGE_TYPE, SERVICE_ID, SUBSERVICE_ID,
                DATE_FROM, DATE_TO,
                ROUND(DATE_TO-DATE_FROM) ORD_DAYS,
                MON_DAYS
          FROM (      
            SELECT Q.ACCOUNT_ID, Q.BILL_ID, Q.REP_PERIOD_ID,
                   OB.ORDER_ID, OB.ORDER_BODY_ID, 
                   OB.QUANTITY, 
                   CASE
                     WHEN R.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN (T.PRICE * 28.6)
                     ELSE T.PRICE
                   END VALUE,
                   R.TAX_INCL, 
                   OB.CHARGE_TYPE, O.SERVICE_ID, OB.SUBSERVICE_ID,
                   CASE
                    WHEN GREATEST(O.DATE_FROM, OB.DATE_FROM) <= P.PERIOD_FROM  
                    THEN P.PERIOD_FROM ELSE GREATEST(O.DATE_FROM, OB.DATE_FROM) 
                   END DATE_FROM,
                   CASE
                    WHEN LEAST(NVL(O.DATE_TO,P.PERIOD_TO), NVL(OB.DATE_TO,P.PERIOD_TO)) >= P.PERIOD_TO  
                    THEN P.PERIOD_TO  ELSE LEAST(NVL(O.DATE_TO,P.PERIOD_TO), NVL(OB.DATE_TO,P.PERIOD_TO))
                   END DATE_TO,
                   ROUND(P.PERIOD_TO - P.PERIOD_FROM) MON_DAYS
              FROM ORDER_BODY_T OB, ORDER_T O, 
                   BILLING_QUEUE_T Q, PERIOD_T P,
                   RATEPLAN_T R, MONTH_TARIFF_T T
             WHERE OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_REC
               AND OB.RATE_RULE_ID = Pk00_Const.c_RATE_RULE_ABP_MON
               AND OB.RATEPLAN_ID  = R.RATEPLAN_ID
               AND R.RATESYSTEM_ID = Pk00_Const.�_RATESYS_MON_TRF_ID
               AND R.RATEPLAN_ID   = T.RATEPLAN_ID
               AND T.PERIOD_ID     = NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID)
               AND O.ORDER_ID      = OB.ORDER_ID
               AND P.PERIOD_ID     = NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID)
               AND Q.ACCOUNT_ID    = O.ACCOUNT_ID               
               AND OB.DATE_FROM   <= P.PERIOD_TO
               AND (P.PERIOD_FROM <= OB.DATE_TO OR OB.DATE_TO IS NULL)
               AND O.DATE_FROM    <= P.PERIOD_TO
               AND (P.PERIOD_FROM <= O.DATE_TO OR O.DATE_TO IS NULL)
               AND Q.TASK_ID       = p_task_id
            )
    ), LCK AS (    -- ����������, ������������ �������
        SELECT BILL_ID, REP_PERIOD_ID,
               ORDER_ID, ROUND(SUM(LOCK_DAYS)) LCK_DAYS
          FROM (
            -- -------------------------------------------------------------------- --
            SELECT Q.BILL_ID, Q.REP_PERIOD_ID, L.ORDER_ID, 
                   CASE 
                    -- ������������ ���� �����:  DATE_FROM---[-------]---DATE_TO
                    WHEN L.DATE_FROM <= P.PERIOD_FROM 
                     AND (P.PERIOD_TO <= L.DATE_TO OR L.DATE_TO IS NULL) 
                    THEN P.PERIOD_TO - P.PERIOD_FROM
                    -- ������������ ������ ������: [--DATE_FROM-----DATE_TO--]
                    WHEN P.PERIOD_FROM < L.DATE_FROM 
                     AND L.DATE_TO < P.PERIOD_TO 
                    THEN L.DATE_TO - L.DATE_FROM
                    -- ������������ � ���������� �������, ������ � �������: ---DATE_FROM-- [---DATE_TO---]
                    WHEN L.DATE_FROM <= P.PERIOD_FROM 
                     AND L.DATE_TO < P.PERIOD_TO 
                    THEN L.DATE_TO - P.PERIOD_FROM
                    -- ������������ � ������� �������, � �������� �� ����� �������:---[--DATE_FROM--]---DATE_TO---  
                    WHEN P.PERIOD_FROM < L.DATE_FROM  
                     AND (P.PERIOD_TO <= L.DATE_TO OR L.DATE_TO IS NULL)
                    THEN P.PERIOD_TO - L.DATE_FROM
                    -- �������� ������������ ����
                    ELSE 0
                   END LOCK_DAYS
              FROM ORDER_LOCK_T L, ORDER_T O, BILLING_QUEUE_T Q, PERIOD_T P
            WHERE L.DATE_FROM    <= P.PERIOD_TO                     -- ������ ���������� 
              AND (L.DATE_TO IS NULL OR P.PERIOD_FROM <= L.DATE_TO) -- ������������� � �������
              AND O.ORDER_ID      = L.ORDER_ID
              AND Q.ACCOUNT_ID    = O.ACCOUNT_ID
              AND P.PERIOD_ID     = NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID)
              AND Q.TASK_ID       = p_task_id
            -- -------------------------------------------------------------------- --
        )
        GROUP BY BILL_ID, REP_PERIOD_ID, ORDER_ID
    ), ABP AS (
        SELECT ORD.ACCOUNT_ID, ORD.BILL_ID, ORD.REP_PERIOD_ID,
               ORD.ORDER_ID, ORD.ORDER_BODY_ID, 
               ORD.QUANTITY, ORD.VALUE, ORD.TAX_INCL, 
               ORD.CHARGE_TYPE, ORD.SERVICE_ID, ORD.SUBSERVICE_ID, 
               ORD.DATE_FROM, ORD.DATE_TO,
               ORD.ORD_DAYS, ORD.MON_DAYS, NVL(LCK.LCK_DAYS,0) LCK_DAYS,
               CASE
                WHEN ORD.ORD_DAYS < NVL(LCK.LCK_DAYS,0) THEN 1
                ELSE ((ORD.ORD_DAYS - NVL(LCK.LCK_DAYS,0))/MON_DAYS)
               END K_DAYS
          FROM ORD, LCK
         WHERE ORD.ORDER_ID      = LCK.ORDER_ID(+)
           AND ORD.BILL_ID       = LCK.BILL_ID(+) 
           AND ORD.REP_PERIOD_ID = LCK.REP_PERIOD_ID(+)
    )
    SELECT B.BILL_ID, B.REP_PERIOD_ID, SQ_ITEM_ID.NEXTVAL ITEM_ID,
           ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE,  
           -- �� ������� ������ ITEM ��� � ������ ��������� ��� �������
           -- �������� ����� ��������
           CASE 
             WHEN TAX_INCL = Pk00_Const.c_RATEPLAN_TAX_INCL THEN 
               ROUND((K_DAYS * QUANTITY * VALUE/1.18),2)
             ELSE
               ROUND((K_DAYS * QUANTITY * VALUE),2)
           END ITEM_TOTAL,
           DATE_FROM, DATE_TO, 
           Pk00_Const.c_RATEPLAN_TAX_NOT_INCL, -- ���������� ��� � �� ������������� ������
           PK00_CONST.c_ITEM_TYPE_BILL,
           Pk00_Const.c_ITEM_STATE_OPEN,
           ABP.ORDER_BODY_ID
      FROM ABP, BILL_T B
     WHERE B.ACCOUNT_ID    = ABP.ACCOUNT_ID 
       AND B.REP_PERIOD_ID = ABP.REP_PERIOD_ID
       AND B.ACCOUNT_ID    = ABP.ACCOUNT_ID
       AND B.BILL_TYPE   IN (Pk00_Const.c_BILL_TYPE_REC, 
                             Pk00_Const.c_BILL_TYPE_DBT, 
                             Pk00_Const.c_BILL_TYPE_OLD)
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  ��������� ���� ����������� ��� ��������� ����������� �� ���������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Put_ABP_detail( p_task_id IN INTEGER )    
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Put_ABP_detail';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ���� �������� ���������� ��������� ��� ����������� �����������
    -- 1) ������ NPL
    -- 2) ������ IP_ACCESS 
    -- 3) ������ EPL
    -- 4) ������ DPL
    -- 5) ������ LM
    MERGE INTO ITEM_T I
    USING (
        SELECT I.ITEM_ID,
               CASE 
                    WHEN I.SERVICE_ID = Pk00_Const.c_SERVICE_IP_ACCESS -- 104 
                      THEN 'IP port, '||INF.POINT_SRC||', '||INF.SPEED_STR
                    WHEN I.SERVICE_ID = Pk00_Const.c_SERVICE_LM -- 108 
                      THEN INF.POINT_SRC||', '||INF.SPEED_STR
                    ELSE INF.POINT_SRC||DECODE(INF.POINT_DST, NULL, NULL,' - '||INF.POINT_DST)||', '||INF.SPEED_STR
               END STR
          FROM ORDER_INFO_T INF, ITEM_T I, BILLING_QUEUE_T Q
        WHERE I.CHARGE_TYPE   IN ( Pk00_Const.c_CHARGE_TYPE_REC )
          AND INF.ORDER_ID    = I.ORDER_ID
          AND Q.BILL_ID       = I.BILL_ID
          AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID
          AND Q.TASK_ID       = p_task_id
          AND INF.POINT_SRC   IS NOT NULL
    ) INF
    ON (
        I.ITEM_ID = INF.ITEM_ID 
    )
    WHEN MATCHED THEN UPDATE SET I.DESCR = INF.STR;    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows, set desc ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  ������ ������� �� ������������ ����� ���������� ������ ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Subservice_charge_MIN( p_task_id IN INTEGER )    
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Subservice_charge_MIN';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ���������� ������� �� ������������ ����� ������
    --
    INSERT INTO ITEM_T(
        BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
        CHARGE_TYPE, ITEM_TOTAL, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
        ITEM_STATUS, ORDER_BODY_ID
    )
    WITH LCK AS (
        SELECT BILL_ID, REP_PERIOD_ID,
               ORDER_ID, ROUND(SUM(LOCK_DAYS)) LCK_DAYS
          FROM (
            -- -------------------------------------------------------------------- --
            SELECT Q.BILL_ID, Q.REP_PERIOD_ID, L.ORDER_ID, 
                   CASE 
                    -- ������������ ���� �����:  DATE_FROM---[-------]---DATE_TO
                    WHEN L.DATE_FROM <= P.PERIOD_FROM 
                     AND (P.PERIOD_TO <= L.DATE_TO OR L.DATE_TO IS NULL) 
                    THEN P.PERIOD_TO - P.PERIOD_FROM
                    -- ������������ ������ ������: [--DATE_FROM-----DATE_TO--]
                    WHEN P.PERIOD_FROM < L.DATE_FROM 
                     AND L.DATE_TO < P.PERIOD_TO 
                    THEN L.DATE_TO - L.DATE_FROM
                    -- ������������ � ���������� �������, ������ � �������: ---DATE_FROM-- [---DATE_TO---]
                    WHEN L.DATE_FROM <= P.PERIOD_FROM 
                     AND L.DATE_TO < P.PERIOD_TO 
                    THEN L.DATE_TO - P.PERIOD_FROM
                    -- ������������ � ������� �������, � �������� �� ����� �������:---[--DATE_FROM--]---DATE_TO---  
                    WHEN P.PERIOD_FROM < L.DATE_FROM  
                     AND (P.PERIOD_TO <= L.DATE_TO OR L.DATE_TO IS NULL)
                    THEN P.PERIOD_TO - L.DATE_FROM
                    -- �������� ������������ ����
                    ELSE 0
                   END LOCK_DAYS
              FROM ORDER_LOCK_T L, ORDER_T O, BILLING_QUEUE_T Q, PERIOD_T P
            WHERE L.DATE_FROM    <= P.PERIOD_TO                     -- ������ ���������� 
              AND (L.DATE_TO IS NULL OR P.PERIOD_FROM <= L.DATE_TO) -- ������������� � �������
              AND O.ORDER_ID      = L.ORDER_ID
              AND Q.ACCOUNT_ID    = O.ACCOUNT_ID
              AND P.PERIOD_ID     = NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID)
              AND Q.TASK_ID       = p_task_id
            -- -------------------------------------------------------------------- --
        )
        GROUP BY BILL_ID, REP_PERIOD_ID, ORDER_ID
    ), MP AS (
        SELECT BILL_ID, REP_PERIOD_ID, ACCOUNT_ID, ORDER_ID, ORDER_BODY_ID,
               SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
               DATE_FROM, DATE_TO, TAX_INCL, MIN_VALUE, VAT,
               ROUND(DATE_TO - DATE_FROM) ORD_DAYS,
               MON_DAYS
          FROM ( 
            SELECT B.BILL_ID, B.REP_PERIOD_ID, O.ACCOUNT_ID, 
                   O.ORDER_ID, O.SERVICE_ID, 
                   OB.ORDER_BODY_ID, OB.SUBSERVICE_ID, OB.CHARGE_TYPE,
                   GREATEST(P.PERIOD_FROM, O.DATE_FROM) DATE_FROM, 
                   LEAST(NVL(O.DATE_TO, P.PERIOD_TO),P.PERIOD_TO) DATE_TO, 
                   OB.TAX_INCL, OB.RATE_VALUE MIN_VALUE, AP.VAT,
                   ROUND(P.PERIOD_TO - P.PERIOD_FROM) MON_DAYS
              FROM ORDER_T O, ORDER_BODY_T OB, 
                   BILL_T B, ACCOUNT_PROFILE_T AP, 
                   BILLING_QUEUE_T BQ, PERIOD_T P
             WHERE (P.PERIOD_FROM <= O.DATE_TO OR O.DATE_TO IS NULL)
               AND O.DATE_FROM    <= P.PERIOD_TO
               AND (P.PERIOD_FROM <= OB.DATE_TO OR OB.DATE_TO IS NULL)
               AND OB.DATE_FROM   <= P.PERIOD_TO
               AND OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_MIN -- 'MIN'
               AND OB.RATE_LEVEL_ID= Pk00_Const.c_RATE_LEVEL_SUBSRV
               AND OB.ORDER_ID     = O.ORDER_ID
               AND B.ACCOUNT_ID    = O.ACCOUNT_ID
               AND B.ACCOUNT_ID    = AP.ACCOUNT_ID
               AND B.PROFILE_ID    = AP.PROFILE_ID
               AND B.REP_PERIOD_ID = BQ.REP_PERIOD_ID
               AND B.ACCOUNT_ID    = BQ.ACCOUNT_ID
               AND B.BILL_ID       = BQ.BILL_ID
               AND B.BILL_TYPE     IN (Pk00_Const.c_BILL_TYPE_REC, 
                                       Pk00_Const.c_BILL_TYPE_DBT, 
                                       Pk00_Const.c_BILL_TYPE_OLD)
               AND P.PERIOD_ID     = NVL(BQ.DATA_PERIOD_ID, BQ.REP_PERIOD_ID)
               AND BQ.TASK_ID      = p_task_id
        )
    ), IT AS (
        SELECT I.BILL_ID, I.REP_PERIOD_ID, 
               I.ORDER_ID, I.SERVICE_ID, I.SUBSERVICE_ID,
               I.TAX_INCL, SUM(I.ITEM_TOTAL) SUM_ITEM_TOTAL
          FROM ITEM_T I, ORDER_BODY_T OB, 
               BILLING_QUEUE_T BQ, PERIOD_T P
         WHERE OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_MIN -- 'MIN'
           AND OB.RATE_LEVEL_ID= Pk00_Const.c_RATE_LEVEL_SUBSRV
           AND OB.DATE_FROM   <= P.PERIOD_TO
           AND (P.PERIOD_FROM <= OB.DATE_TO OR OB.DATE_TO IS NULL)
           AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG -- 'USG'    -- ������ ������ 
           AND I.REP_PERIOD_ID = BQ.REP_PERIOD_ID
           AND I.DATE_FROM    <= P.PERIOD_TO        -- ������������� ������ item-s
           AND I.DATE_TO      >= P.PERIOD_FROM      -- ���������� �������
           AND I.SUBSERVICE_ID = OB.SUBSERVICE_ID
           AND I.BILL_ID       = BQ.BILL_ID
           AND P.PERIOD_ID     = NVL(BQ.DATA_PERIOD_ID, BQ.REP_PERIOD_ID)
           AND BQ.TASK_ID      = p_task_id
         GROUP BY I.BILL_ID, I.REP_PERIOD_ID, 
                  I.ORDER_ID, I.SERVICE_ID, I.SUBSERVICE_ID, I.TAX_INCL
    ), ITM AS ( 
      SELECT 
           MP.ACCOUNT_ID, MP.ORDER_ID, MP.ORDER_BODY_ID,
           MP.SERVICE_ID, MP.SUBSERVICE_ID,
           MP.DATE_FROM, MP.DATE_TO, MP.CHARGE_TYPE, 
           MP.BILL_ID, MP.REP_PERIOD_ID, 
           NVL(IT.SUM_ITEM_TOTAL,0) SUM_ITEM_TOTAL, 
           NVL(IT.TAX_INCL, MP.TAX_INCL) IT_TAX_INCL,
           CASE 
             WHEN IT.TAX_INCL = 'N' AND MP.TAX_INCL = 'Y' THEN ROUND(MP.MIN_VALUE /(1 + MP.VAT / 100),2) 
             WHEN IT.TAX_INCL = 'Y' AND MP.TAX_INCL = 'N' THEN MP.MIN_VALUE + ROUND(MP.MIN_VALUE * MP.VAT / 100, 2)
             ELSE MP.MIN_VALUE 
           END MIN_VALUE,
           NVL(LCK.LCK_DAYS,0) LCK_DAYS,
           ((MP.ORD_DAYS - NVL(LCK.LCK_DAYS,0))/MON_DAYS) K_DAYS
        FROM MP, IT, LCK
       WHERE MP.BILL_ID       = IT.BILL_ID(+)
         AND MP.REP_PERIOD_ID = IT.REP_PERIOD_ID(+)
         AND MP.ORDER_ID      = IT.ORDER_ID(+)
         AND MP.SERVICE_ID    = IT.SERVICE_ID(+)
         AND MP.SUBSERVICE_ID = IT.SUBSERVICE_ID(+)
         AND MP.ORDER_ID      = LCK.ORDER_ID(+)
         AND MP.BILL_ID       = LCK.BILL_ID(+) 
         AND MP.REP_PERIOD_ID = LCK.REP_PERIOD_ID(+)
    )
    SELECT BILL_ID, REP_PERIOD_ID, SQ_ITEM_ID.NEXTVAL ITEM_ID, ORDER_ID, 
           SERVICE_ID, 
           --Pk00_Const.c_SUBSRV_MIN SUBSERVICE_ID, -- 
           SUBSERVICE_ID, --(������� �.�.������)
           CHARGE_TYPE,
           ROUND((K_DAYS * MIN_VALUE - SUM_ITEM_TOTAL),2) ITEM_TOTAL,
           DATE_FROM, DATE_TO, IT_TAX_INCL, 
           PK00_CONST.c_ITEM_TYPE_BILL,
           Pk00_Const.c_ITEM_STATE_OPEN,
           ORDER_BODY_ID 
      FROM ITM
     WHERE K_DAYS > 0
       AND ROUND((K_DAYS * MIN_VALUE - SUM_ITEM_TOTAL),2) > 0
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
--  ������ ������� �� ������������ ����� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Order_charge_MIN( p_task_id IN INTEGER )    
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Order_charge_MIN';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ���������� ������� �� ������������ ����� ������
    --
    INSERT INTO ITEM_T(
        BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
        CHARGE_TYPE, ITEM_TOTAL, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
        ITEM_STATUS, ORDER_BODY_ID
    )
    WITH LCK AS (
        SELECT BILL_ID, REP_PERIOD_ID,
               ORDER_ID, ROUND(SUM(LOCK_DAYS)) LCK_DAYS
          FROM (
            -- -------------------------------------------------------------------- --
            SELECT Q.BILL_ID, Q.REP_PERIOD_ID, L.ORDER_ID, 
                   CASE 
                    -- ������������ ���� �����:  DATE_FROM---[-------]---DATE_TO
                    WHEN L.DATE_FROM <= P.PERIOD_FROM 
                     AND (P.PERIOD_TO <= L.DATE_TO OR L.DATE_TO IS NULL) 
                    THEN P.PERIOD_TO - P.PERIOD_FROM
                    -- ������������ ������ ������: [--DATE_FROM-----DATE_TO--]
                    WHEN P.PERIOD_FROM < L.DATE_FROM 
                     AND L.DATE_TO < P.PERIOD_TO 
                    THEN L.DATE_TO - L.DATE_FROM
                    -- ������������ � ���������� �������, ������ � �������: ---DATE_FROM-- [---DATE_TO---]
                    WHEN L.DATE_FROM <= P.PERIOD_FROM 
                     AND L.DATE_TO < P.PERIOD_TO 
                    THEN L.DATE_TO - P.PERIOD_FROM
                    -- ������������ � ������� �������, � �������� �� ����� �������:---[--DATE_FROM--]---DATE_TO---  
                    WHEN P.PERIOD_FROM < L.DATE_FROM  
                     AND (P.PERIOD_TO <= L.DATE_TO OR L.DATE_TO IS NULL)
                    THEN P.PERIOD_TO - L.DATE_FROM
                    -- �������� ������������ ����
                    ELSE 0
                   END LOCK_DAYS
              FROM ORDER_LOCK_T L, ORDER_T O, BILLING_QUEUE_T Q, PERIOD_T P
            WHERE L.DATE_FROM    <= P.PERIOD_TO                     -- ������ ���������� 
              AND (L.DATE_TO IS NULL OR P.PERIOD_FROM <= L.DATE_TO) -- ������������� � �������
              AND O.ORDER_ID      = L.ORDER_ID
              AND Q.ACCOUNT_ID    = O.ACCOUNT_ID
              AND P.PERIOD_ID     = NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID)
              AND Q.TASK_ID       = p_task_id
            -- -------------------------------------------------------------------- --
        )
        GROUP BY BILL_ID, REP_PERIOD_ID, ORDER_ID
    ), MP AS (
        SELECT BILL_ID, REP_PERIOD_ID, 
               ACCOUNT_ID, ORDER_ID, ORDER_BODY_ID,
               SERVICE_ID, CHARGE_TYPE, 
               DATE_FROM, DATE_TO, TAX_INCL, MIN_VALUE, VAT,
               ROUND(DATE_TO - DATE_FROM) ORD_DAYS,
               MON_DAYS
          FROM ( 
            SELECT B.BILL_ID, B.REP_PERIOD_ID, 
                   O.ACCOUNT_ID, O.ORDER_ID, OB.ORDER_BODY_ID,
                   O.SERVICE_ID, OB.CHARGE_TYPE,
                   GREATEST(P.PERIOD_FROM, O.DATE_FROM) DATE_FROM, 
                   LEAST( NVL(O.DATE_TO, P.PERIOD_TO), P.PERIOD_TO) DATE_TO, 
                   OB.TAX_INCL, OB.RATE_VALUE MIN_VALUE, AP.VAT,
                   ROUND(P.PERIOD_TO - P.PERIOD_FROM) MON_DAYS
              FROM ORDER_T O, ORDER_BODY_T OB, 
                   BILL_T B, ACCOUNT_PROFILE_T AP, 
                   BILLING_QUEUE_T BQ, PERIOD_T P
             WHERE OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_MIN -- 'MIN'
               AND OB.RATE_LEVEL_ID= Pk00_Const.c_RATE_LEVEL_ORDER
               AND OB.DATE_FROM   <= P.PERIOD_TO
               AND (P.PERIOD_FROM <= OB.DATE_TO OR OB.DATE_TO IS NULL)
               AND OB.ORDER_ID     = O.ORDER_ID
               AND O.DATE_FROM    <= P.PERIOD_TO             
               AND (P.PERIOD_FROM <= O.DATE_TO OR O.DATE_TO IS NULL)
               AND B.ACCOUNT_ID    = O.ACCOUNT_ID
               AND B.ACCOUNT_ID    = AP.ACCOUNT_ID
               AND B.PROFILE_ID   = AP.PROFILE_ID
               AND B.REP_PERIOD_ID = BQ.REP_PERIOD_ID
               AND B.ACCOUNT_ID    = BQ.ACCOUNT_ID
               AND B.BILL_ID       = BQ.BILL_ID
               AND B.BILL_TYPE     IN (Pk00_Const.c_BILL_TYPE_REC, 
                                       Pk00_Const.c_BILL_TYPE_DBT, 
                                       Pk00_Const.c_BILL_TYPE_OLD)
               AND P.PERIOD_ID     = NVL(BQ.DATA_PERIOD_ID, BQ.REP_PERIOD_ID)
               AND BQ.TASK_ID      = p_task_id
        )
    ), IT AS (
        SELECT I.BILL_ID, I.REP_PERIOD_ID, I.ORDER_ID, I.SERVICE_ID, I.TAX_INCL, SUM(I.ITEM_TOTAL) SUM_ITEM_TOTAL
          FROM ITEM_T I, ORDER_BODY_T OB, BILLING_QUEUE_T BQ, PERIOD_T P
         WHERE OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_MIN -- 'MIN'
           AND OB.RATE_LEVEL_ID= Pk00_Const.c_RATE_LEVEL_ORDER
           AND OB.DATE_FROM   <= P.PERIOD_TO
           AND (P.PERIOD_FROM <= OB.DATE_TO OR OB.DATE_TO IS NULL)
           AND OB.ORDER_ID     = I.ORDER_ID
           AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG -- 'USG'    -- ������ ������ 
           AND I.REP_PERIOD_ID = BQ.REP_PERIOD_ID
           AND I.DATE_FROM    <= P.PERIOD_TO        -- ������������� ������ item-s
           AND I.DATE_TO      >= P.PERIOD_FROM      -- ���������� �������
           AND I.BILL_ID       = BQ.BILL_ID
           AND P.PERIOD_ID     = NVL(BQ.DATA_PERIOD_ID, BQ.REP_PERIOD_ID)
           AND BQ.TASK_ID      = p_task_id
         GROUP BY I.BILL_ID, I.REP_PERIOD_ID, I.ORDER_ID, I.SERVICE_ID, I.TAX_INCL
    ), ITM AS ( 
      SELECT 
           MP.ACCOUNT_ID, MP.ORDER_ID, MP.ORDER_BODY_ID, MP.SERVICE_ID, 
           MP.DATE_FROM, MP.DATE_TO, MP.CHARGE_TYPE, 
           MP.BILL_ID, MP.REP_PERIOD_ID, 
           NVL(IT.SUM_ITEM_TOTAL,0) SUM_ITEM_TOTAL, 
           NVL(IT.TAX_INCL, MP.TAX_INCL) IT_TAX_INCL,
           CASE 
             WHEN IT.TAX_INCL = 'N' AND MP.TAX_INCL = 'Y' THEN ROUND(MP.MIN_VALUE /(1 + MP.VAT / 100),2) 
             WHEN IT.TAX_INCL = 'Y' AND MP.TAX_INCL = 'N' THEN MP.MIN_VALUE + ROUND(MP.MIN_VALUE * MP.VAT / 100, 2)
             ELSE MP.MIN_VALUE 
           END MIN_VALUE,
           NVL(LCK.LCK_DAYS,0) LCK_DAYS,
           ((MP.ORD_DAYS - NVL(LCK.LCK_DAYS,0))/MON_DAYS) K_DAYS
        FROM MP, IT, LCK
       WHERE MP.BILL_ID       = IT.BILL_ID(+)
         AND MP.REP_PERIOD_ID = IT.REP_PERIOD_ID(+)
         AND MP.ORDER_ID      = IT.ORDER_ID(+)
         AND MP.SERVICE_ID    = IT.SERVICE_ID(+)
         AND MP.ORDER_ID      = LCK.ORDER_ID(+)
         AND MP.BILL_ID       = LCK.BILL_ID(+) 
         AND MP.REP_PERIOD_ID = LCK.REP_PERIOD_ID(+)
    )
    SELECT BILL_ID, REP_PERIOD_ID, SQ_ITEM_ID.NEXTVAL ITEM_ID, ORDER_ID, 
           SERVICE_ID, Pk00_Const.c_SUBSRV_MIN SUBSERVICE_ID, CHARGE_TYPE,
           ROUND((K_DAYS * MIN_VALUE - SUM_ITEM_TOTAL),2) ITEM_TOTAL,
           DATE_FROM, DATE_TO, IT_TAX_INCL, 
           PK00_CONST.c_ITEM_TYPE_BILL,
           Pk00_Const.c_ITEM_STATE_OPEN,
           ORDER_BODY_ID 
      FROM ITM
     WHERE K_DAYS > 0
       AND ROUND((K_DAYS * MIN_VALUE - SUM_ITEM_TOTAL),2) > 0
       
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------- --
--  ������ ������� �� ������������ ����� �������� �����
-- ----------------------------------------------------------------- --
PROCEDURE Account_charge_MIN( p_task_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Account_charge_MIN';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ���������� ������� �� ������������ ����� �������������� ����� �� �/�
    --
    INSERT INTO ITEM_T(
        BILL_ID, REP_PERIOD_ID, ITEM_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, 
        CHARGE_TYPE, ITEM_TOTAL, DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
        ITEM_STATUS, ORDER_BODY_ID
    )
    WITH LCK AS (
        SELECT BILL_ID, REP_PERIOD_ID,
               ORDER_ID, ROUND(SUM(LOCK_DAYS)) LCK_DAYS
          FROM (
            -- -------------------------------------------------------------------- --
            SELECT Q.BILL_ID, Q.REP_PERIOD_ID, L.ORDER_ID, 
                   CASE 
                    -- ������������ ���� �����:  DATE_FROM---[-------]---DATE_TO
                    WHEN L.DATE_FROM <= P.PERIOD_FROM 
                     AND (P.PERIOD_TO <= L.DATE_TO OR L.DATE_TO IS NULL) 
                    THEN P.PERIOD_TO - P.PERIOD_FROM
                    -- ������������ ������ ������: [--DATE_FROM-----DATE_TO--]
                    WHEN P.PERIOD_FROM < L.DATE_FROM 
                     AND L.DATE_TO < P.PERIOD_TO 
                    THEN L.DATE_TO - L.DATE_FROM
                    -- ������������ � ���������� �������, ������ � �������: ---DATE_FROM-- [---DATE_TO---]
                    WHEN L.DATE_FROM <= P.PERIOD_FROM 
                     AND L.DATE_TO < P.PERIOD_TO 
                    THEN L.DATE_TO - P.PERIOD_FROM
                    -- ������������ � ������� �������, � �������� �� ����� �������:---[--DATE_FROM--]---DATE_TO---  
                    WHEN P.PERIOD_FROM < L.DATE_FROM  
                     AND (P.PERIOD_TO <= L.DATE_TO OR L.DATE_TO IS NULL)
                    THEN P.PERIOD_TO - L.DATE_FROM
                    -- �������� ������������ ����
                    ELSE 0
                   END LOCK_DAYS
              FROM ORDER_LOCK_T L, ORDER_T O, BILLING_QUEUE_T Q, PERIOD_T P
            WHERE L.DATE_FROM    <= P.PERIOD_TO                     -- ������ ���������� 
              AND (L.DATE_TO IS NULL OR P.PERIOD_FROM <= L.DATE_TO) -- ������������� � �������
              AND O.ORDER_ID      = L.ORDER_ID
              AND Q.ACCOUNT_ID    = O.ACCOUNT_ID
              AND P.PERIOD_ID     = NVL(Q.DATA_PERIOD_ID, Q.REP_PERIOD_ID)
              AND Q.TASK_ID       = p_task_id
            -- -------------------------------------------------------------------- --
        )
        GROUP BY BILL_ID, REP_PERIOD_ID, ORDER_ID
    ), MP AS (
        SELECT BILL_ID, REP_PERIOD_ID, 
               ACCOUNT_ID, ORDER_ID, ORDER_BODY_ID,
               SERVICE_ID, CHARGE_TYPE, 
               DATE_FROM, DATE_TO, TAX_INCL, MIN_VALUE, VAT,
               ROUND(DATE_TO - DATE_FROM) ORD_DAYS,
               MON_DAYS
          FROM ( 
            SELECT B.BILL_ID, B.REP_PERIOD_ID, 
                   O.ACCOUNT_ID, O.ORDER_ID, OB.ORDER_BODY_ID,
                   O.SERVICE_ID, OB.CHARGE_TYPE,
                   GREATEST(P.PERIOD_FROM, AP.DATE_FROM) DATE_FROM, 
                   LEAST(NVL(AP.DATE_TO, P.PERIOD_TO), P.PERIOD_TO) DATE_TO, 
                   OB.TAX_INCL, OB.RATE_VALUE MIN_VALUE, AP.VAT,
                   ROUND(P.PERIOD_TO - P.PERIOD_FROM) MON_DAYS
              FROM ORDER_T O, ORDER_BODY_T OB, 
                   BILL_T B, ACCOUNT_PROFILE_T AP, 
                   BILLING_QUEUE_T BQ, PERIOD_T P
             WHERE OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_MIN -- 'MIN'
               AND OB.RATE_LEVEL_ID= Pk00_Const.c_RATE_LEVEL_ACCOUNT
               AND OB.DATE_FROM   <= P.PERIOD_TO
               AND (P.PERIOD_FROM <= OB.DATE_TO OR OB.DATE_TO IS NULL)
               AND OB.ORDER_ID     = O.ORDER_ID
               AND B.ACCOUNT_ID    = O.ACCOUNT_ID
               AND B.ACCOUNT_ID    = AP.ACCOUNT_ID
               AND B.ACCOUNT_ID    = AP.ACCOUNT_ID
               AND B.PROFILE_ID    = AP.PROFILE_ID
               AND B.REP_PERIOD_ID = BQ.REP_PERIOD_ID
               AND B.ACCOUNT_ID    = BQ.ACCOUNT_ID
               AND B.BILL_ID       = BQ.BILL_ID
               AND B.BILL_TYPE     IN (Pk00_Const.c_BILL_TYPE_REC, 
                                       Pk00_Const.c_BILL_TYPE_DBT, 
                                       Pk00_Const.c_BILL_TYPE_OLD)
               AND P.PERIOD_ID     = NVL(BQ.DATA_PERIOD_ID, BQ.REP_PERIOD_ID)
               AND BQ.TASK_ID      = p_task_id
        )
    ), IT AS (
        SELECT I.BILL_ID, I.REP_PERIOD_ID, I.TAX_INCL, 
               SUM(I.ITEM_TOTAL) SUM_ITEM_TOTAL
          FROM ITEM_T I, ORDER_BODY_T OB, ORDER_T O, BILLING_QUEUE_T BQ, PERIOD_T P
         WHERE OB.CHARGE_TYPE  = Pk00_Const.c_CHARGE_TYPE_MIN -- 'MIN'
           AND OB.RATE_LEVEL_ID= Pk00_Const.c_RATE_LEVEL_ACCOUNT
           AND OB.DATE_FROM   <= P.PERIOD_TO
           AND (P.PERIOD_FROM <= OB.DATE_TO OR OB.DATE_TO IS NULL)
           AND OB.ORDER_ID     = O.ORDER_ID 
           AND O.ACCOUNT_ID    = BQ.ACCOUNT_ID
           AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG -- 'USG'    -- ������ ������
           AND I.REP_PERIOD_ID = BQ.REP_PERIOD_ID
           AND I.DATE_FROM    <= P.PERIOD_TO        -- ������������� ������ item-s
           AND I.DATE_TO      >= P.PERIOD_FROM      -- ���������� �������
           AND I.BILL_ID       = BQ.BILL_ID
           AND P.PERIOD_ID     = NVL(BQ.DATA_PERIOD_ID, BQ.REP_PERIOD_ID)
           AND BQ.TASK_ID      = p_task_id
         GROUP BY I.BILL_ID, I.REP_PERIOD_ID, I.TAX_INCL
    ), ITM AS ( 
      SELECT 
           MP.ACCOUNT_ID, MP.ORDER_ID, MP.ORDER_BODY_ID, MP.SERVICE_ID, 
           MP.DATE_FROM, MP.DATE_TO, MP.CHARGE_TYPE, 
           MP.BILL_ID, MP.REP_PERIOD_ID, 
           NVL(IT.SUM_ITEM_TOTAL,0) SUM_ITEM_TOTAL, 
           NVL(IT.TAX_INCL, MP.TAX_INCL) IT_TAX_INCL,
           CASE 
             WHEN IT.TAX_INCL = 'N' AND MP.TAX_INCL = 'Y' THEN ROUND(MP.MIN_VALUE /(1 + MP.VAT / 100),2) 
             WHEN IT.TAX_INCL = 'Y' AND MP.TAX_INCL = 'N' THEN MP.MIN_VALUE + ROUND(MP.MIN_VALUE * MP.VAT / 100, 2)
             ELSE MP.MIN_VALUE 
           END MIN_VALUE,
           NVL(LCK.LCK_DAYS,0) LCK_DAYS,
           ((MP.ORD_DAYS - NVL(LCK.LCK_DAYS,0))/MON_DAYS) K_DAYS
        FROM MP, IT, LCK
       WHERE MP.BILL_ID       = IT.BILL_ID(+)
         AND MP.REP_PERIOD_ID = IT.REP_PERIOD_ID(+)
         AND MP.ORDER_ID      = LCK.ORDER_ID(+)
         AND MP.BILL_ID       = LCK.BILL_ID(+) 
         AND MP.REP_PERIOD_ID = LCK.REP_PERIOD_ID(+)
    )
    SELECT BILL_ID, REP_PERIOD_ID, SQ_ITEM_ID.NEXTVAL ITEM_ID, ORDER_ID, 
           SERVICE_ID, Pk00_Const.c_SUBSRV_MIN SUBSERVICE_ID, CHARGE_TYPE,
           ROUND((K_DAYS * MIN_VALUE - SUM_ITEM_TOTAL),2) ITEM_TOTAL,
           DATE_FROM, DATE_TO, IT_TAX_INCL, 
           PK00_CONST.c_ITEM_TYPE_BILL,
           Pk00_Const.c_ITEM_STATE_OPEN,
           ORDER_BODY_ID 
      FROM ITM
     WHERE K_DAYS > 0
       AND ROUND((K_DAYS * MIN_VALUE - SUM_ITEM_TOTAL),2) > 0
    ;

    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows created ', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������������� ������������� ����������,
-- �� ����������� ��� ��� ����������� ����������� �������
--
PROCEDURE Rollback_fixrates( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_fixrates';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    DELETE FROM ITEM_T I
     WHERE I.CHARGE_TYPE IN (
                           Pk00_Const.c_CHARGE_TYPE_MIN, 
                           Pk00_Const.c_CHARGE_TYPE_REC)
       AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_OPEN
       AND I.ITEM_TYPE   = PK00_CONST.c_ITEM_TYPE_BILL
       AND I.EXTERNAL_ID IS NULL -- ��� �����, ����� �� �������� ����������, �������������� �������������
       AND EXISTS (
        SELECT * FROM BILLING_QUEUE_T Q
         WHERE Q.BILL_ID       = I.BILL_ID
           AND Q.REP_PERIOD_ID = I.REP_PERIOD_ID
           AND Q.TASK_ID       = p_task_id
    );
    v_count := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg('Stop. '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ����������� ����� � ������� �� ����������� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Charge_fixrates( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Charge_fixrates';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ����������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Charge_ABP( p_task_id => p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  ������ ��������� (subscriber fee) �� ������� �������� 
    -- ������������� ��� ������� ������ (MONTH_TARIFF_T),
    -- ����� ����� ������ ������� ��������� �����������, 
    -- ����������� ������� �� ���� ���, ����� ������ � ����� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Charge_ABP_by_month_tariff( p_task_id => p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������� �� ����������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --  ������ ������� �� ������������ ����� ���������� ������ ������
    Subservice_charge_MIN( p_task_id => p_task_id );

    --  ������ ������� �� ������������ ����� ������
    Order_charge_MIN( p_task_id => p_task_id );
    
    --  ������ ������� �� ������������ ����� �������� �����
    Account_charge_MIN( p_task_id => p_task_id );

    --  ��������� ���� ����������� ��� ��������� ����������� �� ���������
    Put_abp_detail( p_task_id => p_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

END PK36_BILLING_FIXRATE;
/
