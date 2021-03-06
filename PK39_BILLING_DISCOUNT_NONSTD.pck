CREATE OR REPLACE PACKAGE PK39_BILLING_DISCOUNT_NONSTD
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK39_BILLING_DISCOUNT_NONSTD';
    -- ==============================================================================

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1. ������ ��� �������� MS002088
    --    ������ �� ������� ��� (IP VPN) - 20% � (IP, LM) - 10%
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE DG_MS002088( p_period_id IN INTEGER );
    -- ����� ������
    PROCEDURE Rollback_DG_MS002088( p_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2. ������ ���
    --  1. ����������� ����� ����� �� �������  ����� NPL + EPL �� ������������� ������  �/�, ������������� � ������ ���
    --  2. �� ���������� ����� ������������ ������ ������ 
    --  3. ������ ����������� � �������, ���������� � ���������� ������ � ������ ��� (�������� ������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE DG_MTC( p_period_id IN INTEGER );
    -- ����� ������
    PROCEDURE Rollback_DG_MTC( p_period_id IN INTEGER );

    -- 3. �������� ������  MS001221 (�����-����)
    --  1, ���������� ����� ����� �� ������ IP VPN
    --  2. ���������� ������ ������ �� �������� ���������� ������
    --  3. ��������� � ������� � �������� �� �������
    PROCEDURE DG_MS001221_ALPHA_BANK( p_period_id IN INTEGER );
    -- ����� ������
    PROCEDURE Rollback_DG_MS001221( p_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 4. �������� ������  MS107643_��� ( ��� ��� )
    --  1, ������������� ����� ������ 20% �� ���������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE DG_MS107643_RZD( p_period_id IN INTEGER );
    -- ����� ������
    PROCEDURE Rollback_DG_MS107643_RZD( p_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 5. �������� ������  DG_RNDZT0128_KRDZT0138 ( �� ������(��) )
    --  1. ������������ ����������� ������ ������ �� item-��
    --  2. ������������ ������ ������ �� BDR 
    --  3. ���������� ������� ����������� � ������� ������ � ������������ ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE DG_RNDZT0128_KRDZT0138( p_period_id IN INTEGER );
    -- ����� ������
    PROCEDURE Rollback_DG_RNDZT0128( p_period_id IN INTEGER );

    -- ==============================================================================

    -- ������ ���� ������������� ��������� ������ ��� ���������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Apply_discounts( p_period_id IN INTEGER );
    
END PK39_BILLING_DISCOUNT_NONSTD;
/
CREATE OR REPLACE PACKAGE BODY PK39_BILLING_DISCOUNT_NONSTD
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������ � ��������� ������� ����������� � ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Open_task( p_period_id IN INTEGER, p_dg_id IN INTEGER ) RETURN INTEGER
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Open_task';
    v_task_id INTEGER;
    v_count   INTEGER;
BEGIN
    -- �������� ������������� ������
    v_task_id := PK30_BILLING_QUEUE.Open_task;
        
    -- ��������� �������
    INSERT INTO BILLING_QUEUE_T(BILL_ID, ACCOUNT_ID, 
                                TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID)
    SELECT DISTINCT B.BILL_ID, GA.ACCOUNT_ID,  
           v_task_id, p_period_id, p_period_id
      FROM DISCOUNT_GROUP_T G, DG_ACCOUNT_T GA, BILL_T B, PERIOD_T P
     WHERE G.DG_ID         = p_dg_id
       AND P.PERIOD_ID     = p_period_id
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

    RETURN v_task_id;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 1. ������ �� �������� MS002088 (�� "��������")
--    ������ �� ������� ��� (IP VPN) - 20% � (IP, LM) - 10%
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE DG_MS002088( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'DG_MS002088';
    v_dg_id         CONSTANT INTEGER := 1346;  -- ID ������
    v_dg_rule_id    CONSTANT INTEGER := 2502;  -- ������������� ������
    v_srv_ipvpn     CONSTANT INTEGER := 106;   -- IP VPN
    v_srv_iplm      CONSTANT INTEGER := 108;   -- IP LM
    v_prc_ipvpn     CONSTANT NUMBER  := 20;    -- (IP VPN) - 20%
    v_prc_iplm      CONSTANT NUMBER  := 10;    -- (IP, LM) - 10%
    v_count         INTEGER;
    v_period_from   DATE;
    v_period_to     DATE;
    v_task_id       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���������� ������ ��� �� ���
    v_count := 0;
    --
    FOR ro IN (
      WITH DA AS (
          SELECT DA.ACCOUNT_ID 
            FROM DISCOUNT_GROUP_T DG, DG_ACCOUNT_T DA
           WHERE DG.DG_RULE_ID = v_dg_rule_id 
             AND DG.DG_ID      = v_dg_id
             AND DG.DG_ID      = DA.DG_ID
             AND DA.DATE_FROM  < v_period_to
             AND ( DA.DATE_TO IS NULL OR v_period_from < DA.DATE_TO )
      )
      SELECT O.ORDER_ID, MIN(OB.CURRENCY_ID) CURRENCY_ID, MIN(OB.DATE_FROM) DATE_FROM 
        FROM DA, ORDER_T O, ORDER_BODY_T OB
       WHERE DA.ACCOUNT_ID = O.ACCOUNT_ID
         AND O.SERVICE_ID IN ( v_srv_ipvpn, v_srv_iplm )
         AND v_period_from < O.DATE_TO
         AND O.DATE_FROM < v_period_to
         AND OB.ORDER_ID = O.ORDER_ID
         AND OB.CHARGE_TYPE IN ('REC','USG','MIN')
         AND v_period_from < OB.DATE_TO
         AND OB.DATE_FROM < v_period_to
         AND NOT EXISTS (
           SELECT * FROM ORDER_BODY_T OBD
            WHERE OBD.ORDER_ID = O.ORDER_ID
              AND OBD.CHARGE_TYPE = 'DIS'
              AND v_period_from < OBD.DATE_TO
              AND OBD.DATE_FROM < v_period_to
         )
       GROUP BY O.ORDER_ID
    )
    LOOP
       -- c������ ������� ������
       Pk06_ORDER.Add_subs_discount (
                   p_order_id      => ro.order_id,    -- ID ������ - ������
                   p_currency_id   => ro.currency_id, -- ID ������ �������
                   p_date_from     => ro.date_from    -- ���� ������ ��������
               );
       v_count := v_count + 1;
    END LOOP;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Order_body_t(DIS): '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ������ � ��������� ������� ����������� � ������
    v_task_id := Open_task( p_period_id, v_dg_id );

    -- ���������������� �����
    Pk30_Billing_Base.Rollback_bills(v_task_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ ������
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
    WITH OD AS (   -- ������ ��� ������� ����������� ������ 
        SELECT DA.ACCOUNT_ID, O.ORDER_ID, O.SERVICE_ID 
          FROM DISCOUNT_GROUP_T DG, DG_ACCOUNT_T DA, ORDER_T O
         WHERE DG.DG_RULE_ID = v_dg_rule_id
           AND DG.DG_ID      = v_dg_id
           AND DG.DG_ID      = DA.DG_ID
           AND DA.ACCOUNT_ID = O.ACCOUNT_ID
           AND O.SERVICE_ID IN ( v_srv_ipvpn, v_srv_iplm )
           AND v_period_from < O.DATE_TO
           AND O.DATE_FROM   < v_period_to
           AND DA.DATE_FROM  < v_period_to
           AND ( DA.DATE_TO IS NULL OR v_period_from < DA.DATE_TO )
    ), BI AS ( -- ��������� ������ ��� �������
       SELECT 
            B.BILL_ID, B.REP_PERIOD_ID, 
            OD.ORDER_ID, OD.SERVICE_ID, 
            SUM(I.ITEM_TOTAL) ITEM_TOTAL, 
            I.ITEM_CURRENCY_ID, 
            MIN(I.DATE_FROM) DATE_FROM, MAX(I.DATE_TO) DATE_TO,
            I.TAX_INCL 
         FROM ITEM_T I, BILL_T B, OD
        WHERE B.REP_PERIOD_ID = p_period_id
          AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
          AND B.BILL_ID       = I.BILL_ID
          AND B.BILL_TYPE     = 'B'
          AND I.ORDER_ID      = OD.ORDER_ID
          AND I.ITEM_TYPE     = Pk00_Const.c_ITEM_TYPE_BILL  -- ��������� �������������
        GROUP BY 
            B.BILL_ID, B.REP_PERIOD_ID, 
            OD.ORDER_ID, OD.SERVICE_ID, 
            I.ITEM_CURRENCY_ID, 
            I.TAX_INCL
    ) -- ��������� ������� ����� items
    SELECT 
          BI.BILL_ID, BI.REP_PERIOD_ID, 
          SQ_ITEM_ID.NEXTVAL ITEM_ID, 
          'B'  ITEM_TYPE, 
          NULL INV_ITEM_ID, 
          BI.ORDER_ID, BI.SERVICE_ID, 
          OB.SUBSERVICE_ID, 
          OB.CHARGE_TYPE, 
          -(BI.ITEM_TOTAL* DECODE(BI.SERVICE_ID, v_srv_ipvpn, v_prc_ipvpn, v_srv_iplm, v_prc_iplm, 0)) / 100 ITEM_TOTAL,
          BI.ITEM_CURRENCY_ID,
          0 RECVD, 
          BI.DATE_FROM, BI.DATE_TO, 
          Pk00_Const.c_ITEM_STATE_OPEN,
          SYSDATE CREATE_DATE, SYSDATE LAST_MODIFIED, 
          0 REP_GROSS, 0 REP_TAX, BI.TAX_INCL, 
          NULL EXTERNAL_ID, 
          DECODE(BI.SERVICE_ID, v_srv_ipvpn, v_prc_ipvpn, v_srv_iplm, v_prc_iplm, '0')||' %'  NOTES,
          OB.ORDER_BODY_ID, 
          NULL
      FROM BI, ORDER_BODY_T OB
     WHERE BI.ORDER_ID = OB.ORDER_ID
       AND OB.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_DIS
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� �����
    Pk30_Billing_Base.Make_bills(v_task_id);
    -- ����������� ������
    PK30_BILLING_QUEUE.Close_task(v_task_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_DG_MS002088( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_DG_MS002088';
    v_dg_id         CONSTANT INTEGER := 1346;  -- ID ������
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    PK39_BILLING_DISCOUNT.Rollback_group_discount( v_dg_id, p_period_id );

    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 2. ������ dg_id = 1341 - ���
--  1. ����������� ����� ����� �� �������  ����� NPL + EPL �� ������������� ������  �/�, ������������� � ������ ���
--  2. �� ���������� ����� ������������ ������ ������ 
--  3. ������ ����������� � �������, ���������� � ���������� ������ � ������ ��� (�������� ������)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE DG_MTC( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'DG_MTC';
    v_dg_id         CONSTANT INTEGER := 1341;  -- ������
    --v_dg_rule_id    CONSTANT INTEGER := 2502;  -- ������� ���������� ������
    v_srv_npl       CONSTANT INTEGER := 101;   -- NPL: ���010101 ������������� �������� ����� ����� (NPL)
    v_srv_epl       CONSTANT INTEGER := 133;   -- EPL: ���0205 ����������� ����� Ethernet (EPL)
    v_total         NUMBER;
    v_percent       NUMBER;
    v_count         INTEGER;
    v_period_from   DATE;
    v_period_to     DATE;
    v_task_id       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������ � ��� ���������� ������������ ������ 'DIS', ����� �� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ � ��������� ������� ����������� � ������
    v_task_id := Open_task( p_period_id, v_dg_id );

    -- ���������������� �����
    Pk30_Billing_Base.Rollback_bills(v_task_id);

    -- �������� ����� ����� �� �������  ����� NPL + EPL �� ������������� ������  �/�
    SELECT SUM(I.ITEM_TOTAL) 
      INTO v_total
      FROM ITEM_T I, BILLING_QUEUE_T Q
     WHERE Q.TASK_ID       = v_task_id
       AND I.REP_PERIOD_ID = Q.REP_PERIOD_ID
       AND I.BILL_ID       = Q.BILL_ID
       AND I.SERVICE_ID IN ( v_srv_npl, v_srv_epl );

    -- �������� ������� ������
    SELECT DISCOUNT_PRC 
      INTO v_percent
      FROM DG_PERCENT_T GP
     WHERE GP.DG_ID = v_dg_id
       AND GP.VALUE_MIN <= v_total
       AND v_total < GP.VALUE_MAX;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ ������
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
    WITH OD AS (   -- ������ ��� ������� ����������� ������ 
        SELECT DA.ACCOUNT_ID, O.ORDER_ID, O.SERVICE_ID
          FROM DG_ACCOUNT_T DA, ORDER_T O
         WHERE DA.DG_ID      = v_dg_id
           AND DA.ACCOUNT_ID = O.ACCOUNT_ID
           AND v_period_from < O.DATE_TO
           AND O.DATE_FROM   < v_period_to
           AND DA.DATE_FROM  < v_period_to
           AND (DA.DATE_TO IS NULL OR v_period_from < DA.DATE_TO)
    ), BI AS (     -- ��������� ������ ��� �������
       SELECT 
            B.BILL_ID, B.REP_PERIOD_ID, 
            OD.ORDER_ID, OD.SERVICE_ID, 
            -(SUM(I.ITEM_TOTAL) * v_percent / 100) ITEM_TOTAL, 
            I.ITEM_CURRENCY_ID, 
            MIN(I.DATE_FROM) DATE_FROM, MAX(I.DATE_TO) DATE_TO,
            I.TAX_INCL 
         FROM ITEM_T I, BILL_T B, OD
        WHERE B.REP_PERIOD_ID = p_period_id
          AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
          AND B.BILL_ID       = I.BILL_ID
          AND B.BILL_TYPE     = 'B'
          AND I.ORDER_ID      = OD.ORDER_ID
          AND I.ITEM_TYPE     = Pk00_Const.c_ITEM_TYPE_BILL  -- ��������� �������������
        GROUP BY 
            B.BILL_ID, B.REP_PERIOD_ID, 
            OD.ORDER_ID, OD.SERVICE_ID, 
            I.ITEM_CURRENCY_ID, 
            I.TAX_INCL
    ) -- ��������� ������� ����� items
    SELECT 
          BI.BILL_ID, BI.REP_PERIOD_ID, 
          SQ_ITEM_ID.NEXTVAL ITEM_ID, 
          'B'  ITEM_TYPE, 
          NULL INV_ITEM_ID, 
          BI.ORDER_ID, BI.SERVICE_ID, 
          OB.SUBSERVICE_ID, 
          OB.CHARGE_TYPE, 
          BI.ITEM_TOTAL,
          BI.ITEM_CURRENCY_ID,
          0 RECVD, 
          BI.DATE_FROM, BI.DATE_TO, 
          Pk00_Const.c_ITEM_STATE_OPEN,
          SYSDATE CREATE_DATE, SYSDATE LAST_MODIFIED, 
          0 REP_GROSS, 0 REP_TAX, BI.TAX_INCL, 
          NULL EXTERNAL_ID, 
          v_percent||' %'  NOTES,
          OB.ORDER_BODY_ID, 
          NULL
      FROM BI, ORDER_BODY_T OB
     WHERE BI.ORDER_ID = OB.ORDER_ID
       AND OB.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_DIS
       AND v_period_from <= OB.DATE_TO
       AND OB.DATE_FROM  <= v_period_to
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� �����
    Pk30_Billing_Base.Make_bills(v_task_id);
    -- ����������� ������
    PK30_BILLING_QUEUE.Close_task(v_task_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ������ ��� ���
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_DG_MTC( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_DG_MTC';
    v_dg_id         CONSTANT INTEGER := 1341;  -- ������� ���������� ������
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    PK39_BILLING_DISCOUNT.Rollback_group_discount( v_dg_id, p_period_id );
    
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ========================================================================== --
-- 3. �������� ������  MS001221 (�����-����)
--  1, ���������� ����� ����� �� ������ IP VPN
--  2. ���������� ������ ������ �� �������� ���������� ������
--  3. ��������� � ������� � �������� �� �������
-- ========================================================================== --
PROCEDURE DG_MS001221_ALPHA_BANK( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'DG_MS001221_ALPHA_BANK';
    v_dg_id         CONSTANT INTEGER := 1339;  -- ������� ���������� ������
    --v_dg_rule_id    CONSTANT INTEGER := 2502;  -- ������� ���������� ������
    v_srv_ipvpn     CONSTANT INTEGER := 106;   -- IP VPN
    v_total         NUMBER;
    v_percent       NUMBER;
    v_count         INTEGER;
    v_period_from   DATE;
    v_period_to     DATE;
    v_task_id       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������ � ��� ���������� ������������ ������ 'DIS', ����� �� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ � ��������� ������� ����������� � ������
    v_task_id := Open_task( p_period_id, v_dg_id );

    -- ���������������� �����
    Pk30_Billing_Base.Rollback_bills(v_task_id);

    -- �������� ����� ����� �� ������� �� ������ IP VPN �� ������������� ������  �/�
    SELECT SUM(I.ITEM_TOTAL) 
      INTO v_total
      FROM ITEM_T I, BILLING_QUEUE_T Q
     WHERE Q.TASK_ID       = v_task_id
       AND I.REP_PERIOD_ID = Q.REP_PERIOD_ID
       AND I.BILL_ID       = Q.BILL_ID
       AND I.SERVICE_ID    = v_srv_ipvpn;

    -- �������� ������� ������
    SELECT DISCOUNT_PRC 
      INTO v_percent
      FROM DG_PERCENT_T GP
     WHERE GP.DG_ID = v_dg_id
       AND GP.VALUE_MIN <= v_total
       AND v_total < GP.VALUE_MAX;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ ������
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
    WITH OD AS (   -- ������ ��� ������� ����������� ������ 
        SELECT DA.ACCOUNT_ID, O.ORDER_ID, O.SERVICE_ID
          FROM DG_ACCOUNT_T DA, ORDER_T O
         WHERE DA.DG_ID      = v_dg_id
           AND DA.ACCOUNT_ID = O.ACCOUNT_ID
           AND v_period_from < O.DATE_TO
           AND O.DATE_FROM   < v_period_to
           AND DA.DATE_FROM  < v_period_to
           AND (DA.DATE_TO IS NULL OR v_period_from < DA.DATE_TO)
    ), BI AS (     -- ��������� ������ ��� �������
       SELECT 
            B.BILL_ID, B.REP_PERIOD_ID, 
            OD.ORDER_ID, OD.SERVICE_ID, 
            SUM(I.ITEM_TOTAL) ITEM_TOTAL, 
            I.ITEM_CURRENCY_ID, 
            MIN(I.DATE_FROM) DATE_FROM, MAX(I.DATE_TO) DATE_TO,
            I.TAX_INCL 
         FROM ITEM_T I, BILL_T B, OD
        WHERE B.REP_PERIOD_ID = p_period_id
          AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
          AND B.BILL_ID       = I.BILL_ID
          AND B.BILL_TYPE     = 'B'
          AND I.ORDER_ID      = OD.ORDER_ID
          AND I.ITEM_TYPE     = Pk00_Const.c_ITEM_TYPE_BILL  -- ��������� �������������
        GROUP BY 
            B.BILL_ID, B.REP_PERIOD_ID, 
            OD.ORDER_ID, OD.SERVICE_ID, 
            I.ITEM_CURRENCY_ID, 
            I.TAX_INCL
    ) -- ��������� ������� ����� items
    SELECT 
          BI.BILL_ID, BI.REP_PERIOD_ID, 
          SQ_ITEM_ID.NEXTVAL ITEM_ID, 
          'B'  ITEM_TYPE, 
          NULL INV_ITEM_ID, 
          BI.ORDER_ID, BI.SERVICE_ID, 
          OB.SUBSERVICE_ID, 
          OB.CHARGE_TYPE, 
          -(BI.ITEM_TOTAL * v_percent / 100) ITEM_TOTAL,
          BI.ITEM_CURRENCY_ID,
          0 RECVD, 
          BI.DATE_FROM, BI.DATE_TO, 
          Pk00_Const.c_ITEM_STATE_OPEN,
          SYSDATE CREATE_DATE, SYSDATE LAST_MODIFIED, 
          0 REP_GROSS, 0 REP_TAX, BI.TAX_INCL, 
          NULL EXTERNAL_ID, 
          v_percent||' %'  NOTES,
          OB.ORDER_BODY_ID, 
          NULL
      FROM BI, ORDER_BODY_T OB
     WHERE BI.ORDER_ID = OB.ORDER_ID
       AND OB.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_DIS
       AND v_period_from <= OB.DATE_TO
       AND OB.DATE_FROM  <= v_period_to
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� �����
    Pk30_Billing_Base.Make_bills(v_task_id);
    -- ����������� ������
    PK30_BILLING_QUEUE.Close_task(v_task_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_DG_MS001221( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_DG_MS001221';
    v_dg_id         CONSTANT INTEGER := 1339;  -- ������� ���������� ������
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    PK39_BILLING_DISCOUNT.Rollback_group_discount( v_dg_id, p_period_id );
    
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ========================================================================== --
-- 4. �������� ������  MS107643_��� ( ��� ��� )
--  1, ������������� ����� ������ 20% �� ���������� ������
-- ========================================================================== --
PROCEDURE DG_MS107643_RZD( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'DG_MS107643_RZD';
    v_dg_id         CONSTANT INTEGER := 1412;  -- ������ ������
    --v_dg_rule_id    CONSTANT INTEGER := 2502;  -- ������� ���������� ������
    v_percent       NUMBER := 20;
    v_count         INTEGER;
    --v_period_from   DATE;
    --v_period_to     DATE;
    v_task_id       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    --v_period_from := Pk04_Period.Period_from(p_period_id);
    --v_period_to   := Pk04_Period.Period_to(p_period_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������ � ��� ���������� ������������ ������ 'DIS', ����� �� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ � ��������� ������� ����������� � ������
    v_task_id := Open_task( p_period_id, v_dg_id );

    -- ���������������� �����
    Pk30_Billing_Base.Rollback_bills(v_task_id, true);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ ������
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
    SELECT 
          BI.BILL_ID, BI.REP_PERIOD_ID, 
          SQ_ITEM_ID.NEXTVAL ITEM_ID, 
          'B'  ITEM_TYPE, 
          NULL INV_ITEM_ID, 
          BI.ORDER_ID, BI.SERVICE_ID, 
          BI.SUBSERVICE_ID, 
          'DIS', 
          -(BI.ITEM_TOTAL * v_percent / 100) ITEM_TOTAL,
          BI.ITEM_CURRENCY_ID,
          0 RECVD, 
          BI.DATE_FROM, BI.DATE_TO, 
          'OPEN', --Pk00_Const.c_ITEM_STATE_OPEN,
          SYSDATE CREATE_DATE, SYSDATE LAST_MODIFIED, 
          0 REP_GROSS, 0 REP_TAX, BI.TAX_INCL, 
          NULL EXTERNAL_ID, 
          v_percent||' %'  NOTES,
          BI.ORDER_BODY_ID, 
          NULL
      FROM (
        SELECT 
            B.BILL_ID, B.REP_PERIOD_ID, 
            I.ORDER_ID, I.SERVICE_ID, 
            I.ORDER_BODY_ID, I.SUBSERVICE_ID, 
            SUM(I.ITEM_TOTAL) ITEM_TOTAL, 
            I.ITEM_CURRENCY_ID, 
            MIN(I.DATE_FROM) DATE_FROM, MAX(I.DATE_TO) DATE_TO,
            I.TAX_INCL 
         FROM ITEM_T I, BILL_T B, DG_ACCOUNT_T DA, PERIOD_T P
         WHERE DA.DG_ID        = 1412
           AND DA.ACCOUNT_ID   = 1937070
           AND P.PERIOD_ID     = p_period_id
           AND B.ACCOUNT_ID    = DA.ACCOUNT_ID 
           AND B.REP_PERIOD_ID = P.PERIOD_ID 
           AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
           AND B.BILL_ID       = I.BILL_ID
           AND B.BILL_TYPE     = 'B'
           AND I.ITEM_TYPE     = Pk00_Const.c_ITEM_TYPE_BILL  -- ��������� �������������
         GROUP BY 
            B.BILL_ID, B.REP_PERIOD_ID, 
            I.ORDER_ID, I.SERVICE_ID, 
            I.ORDER_BODY_ID, I.SUBSERVICE_ID, 
            I.ITEM_CURRENCY_ID, 
            I.TAX_INCL 
      ) BI
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Item_t: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� �����
    Pk30_Billing_Base.Make_bills(v_task_id, true);
    -- ����������� ������
    PK30_BILLING_QUEUE.Close_task(v_task_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_DG_MS107643_RZD( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_DG_MS107643_RZD';
    v_dg_id         CONSTANT INTEGER := 1412;  
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    PK39_BILLING_DISCOUNT.Rollback_group_discount( v_dg_id, p_period_id );
    
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- 
-- ========================================================================== --
-- 5. �������� ������  DG_RNDZT0128_KRDZT0138 ( �� ������(��) )
--  1. ������������ ����������� ������ ������ �� item-��
--  2. ������������ ������ ������ �� BDR 
--  3. ���������� ������� ����������� � ������� ������ � ������������ ���������
-- ========================================================================== --
PROCEDURE DG_RNDZT0128_KRDZT0138( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'DG_RNDZT0128_KRDZT0138';
    v_dg_id         CONSTANT INTEGER := 1317;  -- ������ ������
    --v_dg_rule_id    CONSTANT INTEGER := 2502;  -- ������� ���������� ������
    v_percent       NUMBER;
    v_count         INTEGER;
    v_bdr_amount    NUMBER;
    v_bdr_discount  NUMBER;
    v_bdr_total     NUMBER;
    v_item_discount NUMBER;
    v_item_total    NUMBER;
    v_period_from   DATE;
    v_period_to     DATE;
    v_task_id       INTEGER;
    v_item_id       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������ � ��� ���������� ������������ ������ 'DIS', ����� �� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ � ��������� ������� ����������� � ������
    v_task_id := Open_task( p_period_id, v_dg_id );

    -- ���������������� �����
    Pk30_Billing_Base.Rollback_bills(v_task_id, true);

    -- ���������, ��� ��� ������ �� �/� ��������������
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
            SELECT Q.ACCOUNT_ID FROM BILLING_QUEUE_T Q
             WHERE Q.TASK_ID = v_task_id
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

    -- �������� ������� ������, �� �������������:
    SELECT DISCOUNT_PRC INTO v_percent
      FROM DG_PERCENT_T DP
     WHERE DP.DG_ID = v_dg_id;

    -- ��������� ����������� ������ ������ � item-��
    Pk39_Billing_Discount.Make_item_std_discount(v_task_id, v_percent);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������ ������ ��� ������� ������������� �����
    --
    FOR Q IN (
        SELECT * 
          FROM BILLING_QUEUE_T Q
         WHERE Q.TASK_ID = v_task_id
    )
    LOOP
      -- �������� ����� ���������� �� ������ BDR
      SELECT SUM (AMOUNT) INTO v_bdr_amount
        FROM BDR_VOICE_T BDR
       WHERE BDR.BILL_ID    = Q.BILL_ID 
         AND BDR.REP_PERIOD BETWEEN v_period_from AND v_period_to
         AND BDR.BDR_STATUS = 0
         AND BDR.ITEM_ID IS NOT NULL;
      -- ������ �� ������ BDR
      v_bdr_discount := - v_bdr_amount * v_percent / 100;
      v_bdr_total    := v_bdr_amount + v_bdr_discount;
      
      -- �������� ������ �� ������ ITEM_T
      SELECT SUM(ROUND(I.ITEM_TOTAL,2)) INTO v_item_total
        FROM ITEM_T I
       WHERE I.REP_PERIOD_ID = p_period_id
         AND I.BILL_ID       = Q.BILL_ID
         AND I.ITEM_TYPE     = 'B'
      ;

      -- �������� item_id � ������������ ������ ������
      SELECT ITEM_ID INTO v_item_id
        FROM (
          SELECT I.ITEM_ID 
            FROM ITEM_T I
          WHERE I.REP_PERIOD_ID = p_period_id
            AND I.BILL_ID       = Q.BILL_ID
            AND I.CHARGE_TYPE   = 'DIS'
          ORDER BY I.ITEM_TOTAL
      )
      WHERE ROWNUM = 1;
      
      IF (v_item_total - v_bdr_total) != 0 THEN
          -- ���������� ����������� � ����������� ������
          UPDATE ITEM_T I 
             SET I.ITEM_TOTAL = ROUND(I.ITEM_TOTAL - (v_item_total - v_bdr_total), 4)
           WHERE I.REP_PERIOD_ID = p_period_id
             AND I.ITEM_ID = v_item_id;
      END IF;
      Pk01_Syslog.Write_msg('Item_id='||v_item_id||' discount_delta='||(v_bdr_discount-v_item_discount), c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      
    END LOOP;

    -- ��������� �����
    Pk30_Billing_Base.Make_bills(v_task_id, true);
    -- ����������� ������
    PK30_BILLING_QUEUE.Close_task(v_task_id);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_DG_RNDZT0128( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Rollback_DG_RNDZT0128';
    v_dg_id         CONSTANT INTEGER := 1317;  
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    PK39_BILLING_DISCOUNT.Rollback_group_discount( v_dg_id, p_period_id );
    
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- ========================================================================== --
-- ������ ���� ��������� ������
-- ========================================================================== --
PROCEDURE Apply_discounts( p_period_id IN INTEGER )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Apply_discounts';
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1. ������ ��� �������� MS002088
    --    ������ �� ������� ��� (IP VPN) - 20% � (IP, LM) - 10%
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    DG_MS002088( p_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2. ������ ���
    --  1. ����������� ����� ����� �� �������  ����� NPL + EPL �� ������������� ������  �/�, ������������� � ������ ���
    --  2. �� ���������� ����� ������������ ������ ������ 
    --  3. ������ ����������� � �������, ���������� � ���������� ������ � ������ ��� (�������� ������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    DG_MTC( p_period_id );

    -- 3. �������� ������  MS001221 (�����-����)
    --  1, ���������� ����� ����� �� ������ IP VPN
    --  2. ���������� ������ ������ �� �������� ���������� ������
    --  3. ��������� � ������� � �������� �� �������
    DG_MS001221_ALPHA_BANK( p_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 5. �������� ������  DG_RNDZT0128_KRDZT0138 ( �� ������(��) )
    --  1. ������������ ����������� ������ ������ �� item-��
    --  2. ������������ ������ ������ �� BDR 
    --  3. ���������� ������� ����������� � ������� ������ � ������������ ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    DG_RNDZT0128_KRDZT0138( p_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


END PK39_BILLING_DISCOUNT_NONSTD;
/
