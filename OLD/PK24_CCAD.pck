CREATE OR REPLACE PACKAGE PK24_CCAD
IS
    --
    -- ����� ��� ������ � ����
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK24_CCAD';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- ������ ����������� ����
    c_BERR_OK          			  CONSTANT integer :=  0;   -- OK
    c_BERR_RATEPLAN_NotFound  CONSTANT integer := -1;   -- �� ������ rateplan
    c_BERR_VPN_PRICE_NotFound CONSTANT integer := -2;   -- �� ������� ���� ��� qos,zone,rzone
    c_BERR_VOL_PRICE_NotFound CONSTANT integer := -3;   -- �� ������� ���� ��� ��������� ������ 
    c_BERR_NotDefined         CONSTANT integer := -100; -- ������������ ������

    -- --------------------------------------------------------------------- --
    --  ������������ ������� ������ �� BDR CCAD
    -- --------------------------------------------------------------------- --
    PROCEDURE Load_BDRs(
                   p_period_id IN INTEGER
               );
           
    -- --------------------------------------------------------------------- --
    -- BURST (service_id = 104, subservice_id = 40)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Bdr2Item_BURST(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER DEFAULT NULL
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- BURST - ������� ���������� �� ��������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Del_Items_BURST(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER
               );

    -- --------------------------------------------------------------------- --
    -- IP_VOLUME  - ����������� �� ������ (service_id = 104, subservice_id = 39)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    PROCEDURE Bdr2Item_IP_VOLUME(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER DEFAULT NULL
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- IP_VOLUME - ������� ���������� �� ��������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Del_Items_IP_VOLUME(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER
               );

    -- --------------------------------------------------------------------- --
    -- RT - ���������� ����������� (service_id = 104, subservice_id = 31)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    PROCEDURE Bdr2Item_IP_RT(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER DEFAULT NULL
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- RT - ������� ���������� �� ��������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Del_Items_IP_RT(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER
               );

    -- --------------------------------------------------------------------- --
    -- VPN (service_id = 106, subservice_id = 39)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    PROCEDURE Bdr2Item_VPN(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER DEFAULT NULL
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- VPN - ������� ���������� �� ��������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Del_Items_VPN(
                   p_period_id IN INTEGER,
                   p_order_id  IN INTEGER
               );

    -- --------------------------------------------------------------------- --
    --  ������ ��� ���������� ������� �� ����������� ������
    -- --------------------------------------------------------------------- --
    PROCEDURE Billing_queue_list( 
                   p_recordset OUT t_refc, 
                   p_period_id IN INTEGER    -- ID ��������� ������� �����
               );

    -- --------------------------------------------------------------------- --
    --  ����������� ���-�� ������� �������� ������ � ������� ������    
    FUNCTION Correction_factor(
                p_order_id  IN INTEGER,
                p_month     IN DATE   -- ��� ����������� ������, ��� �������� ������������ �����������
            ) RETURN NUMBER;  -- retun correction_factor

    
END PK24_CCAD;
/
CREATE OR REPLACE PACKAGE BODY PK24_CCAD
IS

-- --------------------------------------------------------------------- --
--  ������������ ������� ������ �� BDR CCAD
-- --------------------------------------------------------------------- --
PROCEDURE Load_BDRs(
               p_period_id IN INTEGER
           ) 
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Load_BDRs';
    v_count    INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- BURST (service_id = 104,133, subservice_id = 40)
    Bdr2Item_BURST( p_period_id );

    -- IP_VOLUME  - ����������� �� ������ (service_id = 104, subservice_id = 39)
    Bdr2Item_IP_VOLUME( p_period_id );
    
    -- RT - ���������� ����������� (service_id = 104, subservice_id = 31)
    Bdr2Item_IP_RT( p_period_id );
    
    -- VPN (service_id = 106, subservice_id = 39)
    Bdr2Item_VPN( p_period_id );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

/*
�������� ����� BDR_CCAD_BURST_T:
PAID_SPEED 		    � �������������� ��������
PAID_SPEED_UNIT 	� ������� ��������� �������������� ��������
EXCESS_SPEED 		  � ���������� �������� 
EXCESS_SPEED_UNIT - ������� ��������� ���������� ��������
PRICE 			      � ��������� 1 ������� ��������� ���������� ��������
CF 			          � correction factor
AMOUNT 		        � ����� ������� (EXCESS_SPEED* PRICE * CF)
INFO_WHEN 		    � ����� ��������� ������� ���������� ��������
INFO_ROUTER_IP 	  � �� ����� �������������� ��������� ������� ���������� ��������
INFO_DIRECTION 	  � �� ����� ����������� 0-�������� �� ������� 1-��������� �� �������
CURRENCY_ID 		  � ������ ������
TAX_INCL 		      � 'Y� = ����� �������
*/
-- -------------------------------------------------------------------- --
-- IP_BURST (service_id = 104, subservice_id = 40)
-- -------------------------------------------------------------------- --
PROCEDURE Bdr2Item_BURST(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER DEFAULT NULL
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Bdr2Item_BURST';
    v_count    INTEGER := 0;
    v_bill_id  INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ������� ����������� �����, ���� ����������
    FOR rb IN (
      SELECT DISTINCT CB.REP_PERIOD_ID, O.ACCOUNT_ID 
        FROM BDR_CCAD_BURST_T CB, ORDER_T O
       WHERE CB.ORDER_ID      = O.ORDER_ID
         AND CB.REP_PERIOD_ID = p_period_id
         AND CB.BDR_STATUS_ID = c_BERR_OK
         AND CB.ITEM_ID IS NULL
         AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
         AND NOT EXISTS (
              SELECT * FROM BILL_T B
               WHERE B.REP_PERIOD_ID = CB.REP_PERIOD_ID
                 AND B.ACCOUNT_ID    = O.ACCOUNT_ID
                 AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_REC -- 'B'
         )
    )LOOP
         v_bill_id := PK07_BILL.Next_recuring_bill (
               p_account_id    => rb.account_id,   -- ID �������� �����
               p_rep_period_id => rb.rep_period_id -- ID ���������� ������� YYYYMM
           );
         v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- ��������� item-�
    INSERT INTO ITEM_T I (
        I.REP_PERIOD_ID,
        I.BILL_ID,
        I.ITEM_ID,
        I.ITEM_TYPE,
        I.INV_ITEM_ID,
        I.ORDER_ID, 
        I.SERVICE_ID,
        I.ORDER_BODY_ID,
        I.SUBSERVICE_ID,
        I.CHARGE_TYPE,
        I.ITEM_TOTAL,
        I.DATE_FROM,
        I.DATE_TO,
        I.ITEM_STATUS,
        I.TAX_INCL,
        I.CREATE_DATE
    )
    WITH BDR AS (
        SELECT CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_BURST_T CB
         WHERE CB.REP_PERIOD_ID = p_period_id
           AND CB.BDR_STATUS_ID = c_BERR_OK
           --AND CB.SERVICE_ID    = Pk00_Const.c_SERVICE_IP_ACCESS
           --AND CB.SUBSERVICE_ID = Pk00_Const.c_SUBSRV_BURST
           AND CB.ITEM_ID IS NULL
           AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        B.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,
        CASE
          WHEN OB.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
          ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE
      FROM BDR, ORDER_T O, ORDER_BODY_T OB, BILL_T B
     WHERE BDR.ORDER_ID      = O.ORDER_ID
       AND BDR.ORDER_BODY_ID = OB.ORDER_BODY_ID
       AND BDR.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND O.ACCOUNT_ID      = B.ACCOUNT_ID
       AND B.BILL_TYPE       = PK00_CONST.c_BILL_TYPE_REC   -- 'B'
       AND B.BILL_STATUS     = PK00_CONST.c_BILL_STATE_OPEN -- 'OPEN'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ����������� ��������� �� item-�
    MERGE INTO BDR_CCAD_BURST_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.BILL_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_BURST_T CB
           WHERE I.REP_PERIOD_ID  = p_period_id
             AND I.CHARGE_TYPE    = Pk00_Const.c_CHARGE_TYPE_USG
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
             AND CB.BILL_ID IS NULL
             AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID, CB.BILL_ID = I.BILL_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- IP_BURST - ������� ���������� �� ��������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Del_Items_BURST(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Del_Items_IP_BURST';
    v_count    INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id
                          ||', order_id = '||p_order_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ITEM-� 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.ORDER_ID      = p_order_id
       AND EXISTS (
           SELECT * FROM BDR_CCAD_BURST_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.ITEM_ID  = I.ITEM_ID
              AND CB.ORDER_ID = I.ORDER_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������ �� ITEM-� � BDR
    UPDATE BDR_CCAD_BURST_T CB SET CB.ITEM_ID = NULL
    WHERE CB.REP_PERIOD_ID = p_period_id
      AND CB.ORDER_ID      = p_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. BDR_CCAD_BURST_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
-- IP_VOLUME  - ����������� �� ������ (service_id = 104, subservice_id = 39)
-- --------------------------------------------------------------------- --
PROCEDURE Bdr2Item_IP_VOLUME(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER DEFAULT NULL
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Bdr2Item_IP_VOLUME';
    v_count    INTEGER := 0;
    v_bill_id  INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������� ����������� �����, ���� ����������
    FOR rb IN (
      SELECT DISTINCT CB.REP_PERIOD_ID, O.ACCOUNT_ID 
        FROM BDR_CCAD_VOL_T CB, ORDER_T O
       WHERE CB.ORDER_ID      = O.ORDER_ID
         AND CB.REP_PERIOD_ID = p_period_id
         AND CB.BDR_STATUS_ID = c_BERR_OK
         AND CB.ITEM_ID IS NULL
         AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
         AND NOT EXISTS (
              SELECT * FROM BILL_T B
               WHERE B.REP_PERIOD_ID = CB.REP_PERIOD_ID
                 AND B.ACCOUNT_ID    = O.ACCOUNT_ID
                 AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_REC -- 'B'
         )
    )LOOP
         v_bill_id := PK07_BILL.Next_recuring_bill (
               p_account_id    => rb.account_id,   -- ID �������� �����
               p_rep_period_id => rb.rep_period_id -- ID ���������� ������� YYYYMM
           );
         v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- ��������� item-�
    INSERT INTO ITEM_T I (
        I.REP_PERIOD_ID,
        I.BILL_ID,
        I.ITEM_ID,
        I.ITEM_TYPE,
        I.INV_ITEM_ID,
        I.ORDER_ID, 
        I.SERVICE_ID,
        I.ORDER_BODY_ID,
        I.SUBSERVICE_ID,
        I.CHARGE_TYPE,
        I.ITEM_TOTAL,
        I.DATE_FROM,
        I.DATE_TO,
        I.ITEM_STATUS,
        I.TAX_INCL,
        I.CREATE_DATE,
        I.EXTERNAL_ID
    )
    WITH BDR AS (
        SELECT CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_VOL_T CB
         WHERE CB.REP_PERIOD_ID = p_period_id
           AND (CB.BDR_STATUS_ID = c_BERR_OK OR CB.BDR_STATUS_ID IS NULL)
           --AND CB.SERVICE_ID    = Pk00_Const.c_SERVICE_IP_ACCESS
           --AND CB.SUBSERVICE_ID = Pk00_Const.c_SUBSRV_VOLUME
           AND CB.ITEM_ID IS NULL
           AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        B.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_REC CHARGE_REC,  -- ���������, ��������� �� �������
        CASE
           WHEN OB.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
           ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE,
        Pk00_Const.c_RATESYS_CCAD_ID    -- ����������� ���� 
      FROM BDR, ORDER_T O, ORDER_BODY_T OB, BILL_T B
     WHERE BDR.ORDER_ID      = O.ORDER_ID
       AND BDR.ORDER_BODY_ID = OB.ORDER_BODY_ID
       AND BDR.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND O.ACCOUNT_ID      = B.ACCOUNT_ID
       AND B.BILL_TYPE       = PK00_CONST.c_BILL_TYPE_REC   -- 'B'
       AND B.BILL_STATUS     = PK00_CONST.c_BILL_STATE_OPEN -- 'OPEN'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ����������� ��������� �� item-�
    MERGE INTO BDR_CCAD_VOL_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.BILL_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_VOL_T CB
           WHERE I.REP_PERIOD_ID  = p_period_id
             AND I.CHARGE_TYPE  IN (Pk00_Const.c_CHARGE_TYPE_USG, Pk00_Const.c_CHARGE_TYPE_REC)
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
             AND CB.BILL_ID IS NULL
             AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID, CB.BILL_ID = I.BILL_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
/*
�������� ����� BDR_CCAD_VOL_T:
CF 			        � correction factor
BYTES			      - ����� ������������� ������� � ������
VOLUME			    - ����� ������������� ������� � VOLUME_UNIT_ID (� �������� ������)
VOLUME_UNIT_ID	- ������� ��������� ������� VOLUME
PRICE			      - ���� �� 1 VOLUME_UNIT_ID
AMOUNT			    - ����� � ������ = bdr_rec.VOLUME * bdr_rec.price
[STEP_MIN_BYTES - STEP_MAX_BYTES] � �������� ������ � ������ � ������ CF
CURRENCY_ID 		� ������ ������
TAX_INCL 		    ��Y� = ����� �������
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- IP_VOLUME - ������� ���������� �� ��������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Del_Items_IP_VOLUME(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Del_Items_IP_VOLUME';
    v_count    INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id
                          ||', order_id = '||p_order_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ITEM-� 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.ORDER_ID      = p_order_id
       AND EXISTS (
           SELECT * FROM BDR_CCAD_VOL_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.ITEM_ID  = I.ITEM_ID
              AND CB.ORDER_ID = I.ORDER_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������ �� ITEM-� � BDR
    UPDATE BDR_CCAD_VOL_T CB SET CB.ITEM_ID = NULL
    WHERE CB.REP_PERIOD_ID = p_period_id
      AND CB.ORDER_ID      = p_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. BDR_CCAD_VOL_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
-- RT - ���������� ����������� (service_id = 104, subservice_id = 31)
-- --------------------------------------------------------------------- --
PROCEDURE Bdr2Item_IP_RT(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER DEFAULT NULL
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Bdr2Item_IP_RT';
    v_count    INTEGER := 0;
    v_bill_id  INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������� ����������� �����
    FOR rb IN (
      SELECT DISTINCT CB.REP_PERIOD_ID, O.ACCOUNT_ID 
        FROM BDR_CCAD_RT_T CB, ORDER_T O
       WHERE CB.ORDER_ID      = O.ORDER_ID
         AND CB.REP_PERIOD_ID = p_period_id
         AND CB.BDR_STATUS_ID = c_BERR_OK
         AND CB.ITEM_ID IS NULL
         AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
         AND NOT EXISTS (
              SELECT * FROM BILL_T B
               WHERE B.REP_PERIOD_ID = CB.REP_PERIOD_ID
                 AND B.ACCOUNT_ID    = O.ACCOUNT_ID
                 AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_REC -- 'B'
         )
    )LOOP
         v_bill_id := PK07_BILL.Next_recuring_bill (
               p_account_id    => rb.account_id,   -- ID �������� �����
               p_rep_period_id => rb.rep_period_id -- ID ���������� ������� YYYYMM
           );
         v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- ��������� ������� ������ ITEM-s
    INSERT INTO ITEM_T I (
        I.REP_PERIOD_ID,
        I.BILL_ID,
        I.ITEM_ID,
        I.ITEM_TYPE,
        I.INV_ITEM_ID,
        I.ORDER_ID, 
        I.SERVICE_ID,
        I.ORDER_BODY_ID,
        I.SUBSERVICE_ID,
        I.CHARGE_TYPE,
        I.ITEM_TOTAL,
        I.DATE_FROM,
        I.DATE_TO,
        I.ITEM_STATUS,
        I.TAX_INCL,
        I.CREATE_DATE
    )
    WITH BDR AS (
        SELECT CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_RT_T CB
         WHERE CB.REP_PERIOD_ID = p_period_id
           AND CB.BDR_STATUS_ID = c_BERR_OK
           --AND CB.SERVICE_ID    = 149 --Pk00_Const.c_SERVICE_IP_ACCESS_IC
           --AND CB.SUBSERVICE_ID = Pk00_Const.c_SUBSRV_IRT
           AND CB.ITEM_ID IS NULL
           AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        B.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,
        CASE
           WHEN OB.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
           ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE
      FROM BDR, ORDER_T O, ORDER_BODY_T OB, BILL_T B
     WHERE BDR.ORDER_ID      = O.ORDER_ID
       AND BDR.ORDER_BODY_ID = OB.ORDER_BODY_ID
       AND BDR.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND O.ACCOUNT_ID      = B.ACCOUNT_ID
       AND B.BILL_TYPE       = PK00_CONST.c_BILL_TYPE_REC   -- 'B'
       AND B.BILL_STATUS     = PK00_CONST.c_BILL_STATE_OPEN -- 'OPEN'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ����������� ��������� �� item-�
    MERGE INTO BDR_CCAD_RT_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.BILL_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_RT_T CB
           WHERE I.REP_PERIOD_ID  = p_period_id
             AND I.CHARGE_TYPE    = Pk00_Const.c_CHARGE_TYPE_USG
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
             AND CB.BILL_ID IS NULL
             AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID, CB.BILL_ID = I.BILL_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
/*
�������� ����� BDR_CCAD_RT_T:
GR_1_BYTES,  		-  ����� ���������� ������� (� ������)
GR_2_BYTES, 		- ����� ����������� ������� (� ������)
GR_31_BYTES, 	  - ����� ������� �������-�������� ������ 1
GR_32_BYTES,    - ����� ������� �������-�������� ������ 2
GR_33_BYTES, 		- ����� ������� �������-�������� ������ 3
GR_34_BYTES, 		- ����� ������� �������-�������� ������ 4
GR_1_PRICE, 		- ���� ��� ����������
GR_2_PRICE, 		- ���� ��� �����������
GR_31_PRICE, 		- ���� ��� �������-�������� ������ 1
GR_32_PRICE, 		- ���� ��� �������-�������� ������ 2
GR_33_PRICE, 		- ���� ��� �������-�������� ������ 3
GR_34_PRICE, 		- ���� ��� �������-�������� ������ 4

SPEED			      - �������� (������������ ��� ������������� � ������)
SPEED_UNIT		  - ������� ��������� ��������
AMOUNT			    - ����� � ������.
CF			        - correction factor
CURRENCY_ID 		� ������ ������
TAX_INCL 		    � �Y� = ����� �������
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- RT - ������� ���������� �� ��������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Del_Items_IP_RT(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Del_Items_IP_RT';
    v_count    INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id
                          ||', order_id = '||p_order_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ITEM-� 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.ORDER_ID      = p_order_id
       AND EXISTS (
           SELECT * FROM BDR_CCAD_RT_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.ITEM_ID  = I.ITEM_ID
              AND CB.ORDER_ID = I.ORDER_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������ �� ITEM-� � BDR
    UPDATE BDR_CCAD_VOL_T CB SET CB.ITEM_ID = NULL
    WHERE CB.REP_PERIOD_ID = p_period_id
      AND CB.ORDER_ID      = p_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. BDR_CCAD_VOL_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
  
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
-- VPN (service_id = 106, subservice_id = 39)
-- --------------------------------------------------------------------- --
PROCEDURE Bdr2Item_VPN(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER DEFAULT NULL
           ) 
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Bdr2Item_VPN';
    v_count    INTEGER := 0;
    v_bill_id  INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������� ����������� �����, ���� ����������
    FOR rb IN (
      SELECT DISTINCT CB.REP_PERIOD_ID, O.ACCOUNT_ID 
        FROM BDR_CCAD_VPN_T CB, ORDER_T O
       WHERE CB.ORDER_ID      = O.ORDER_ID
         AND CB.REP_PERIOD_ID = p_period_id
         AND CB.BDR_STATUS_ID = c_BERR_OK
         AND CB.ITEM_ID IS NULL
         AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
         AND NOT EXISTS (
              SELECT * FROM BILL_T B
               WHERE B.REP_PERIOD_ID = CB.REP_PERIOD_ID
                 AND B.ACCOUNT_ID    = O.ACCOUNT_ID
                 AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_REC -- 'B'
         )
    )LOOP
         v_bill_id := PK07_BILL.Next_recuring_bill (
               p_account_id    => rb.account_id,   -- ID �������� �����
               p_rep_period_id => rb.rep_period_id -- ID ���������� ������� YYYYMM
           );
         v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- ��������� item-�
    INSERT INTO ITEM_T I (
        I.REP_PERIOD_ID,
        I.BILL_ID,
        I.ITEM_ID,
        I.ITEM_TYPE,
        I.INV_ITEM_ID,
        I.ORDER_ID, 
        I.SERVICE_ID,
        I.ORDER_BODY_ID,
        I.SUBSERVICE_ID,
        I.CHARGE_TYPE,
        I.ITEM_TOTAL,
        I.DATE_FROM,
        I.DATE_TO,
        I.ITEM_STATUS,
        I.TAX_INCL,
        I.CREATE_DATE
    )
    WITH BDR AS (
        SELECT CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_VPN_T CB
         WHERE CB.REP_PERIOD_ID = p_period_id
           AND CB.BDR_STATUS_ID = c_BERR_OK
           --AND CB.SERVICE_ID    = Pk00_Const.c_SERVICE_VPN
           --AND CB.SUBSERVICE_ID = Pk00_Const.c_SUBSRV_VOLUME
           AND CB.ITEM_ID IS NULL
           AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        B.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,
        CASE
           WHEN OB.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
           ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE
      FROM BDR, ORDER_T O, ORDER_BODY_T OB, BILL_T B
     WHERE BDR.ORDER_ID      = O.ORDER_ID
       AND BDR.ORDER_BODY_ID = OB.ORDER_BODY_ID
       AND BDR.REP_PERIOD_ID = B.REP_PERIOD_ID
       AND O.ACCOUNT_ID      = B.ACCOUNT_ID
       AND B.BILL_TYPE       = PK00_CONST.c_BILL_TYPE_REC   -- 'B'
       AND B.BILL_STATUS     = PK00_CONST.c_BILL_STATE_OPEN -- 'OPEN'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 

    -- ����������� ��������� �� item-�    
    MERGE INTO BDR_CCAD_VPN_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.BILL_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_VPN_T CB
           WHERE I.REP_PERIOD_ID  = p_period_id
             AND I.CHARGE_TYPE    = Pk00_Const.c_CHARGE_TYPE_USG
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
             AND CB.BILL_ID IS NULL
             AND (p_order_id IS NULL OR CB.ORDER_ID = p_order_id)
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID, CB.BILL_ID = I.BILL_ID;
    v_count := SQL%ROWCOUNT;

    Pk01_Syslog.Write_msg('Stop. '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
/*
�������� ����� BDR_CCAD_VPN_T:
QUALITY_ID	   - qos
ZONE_OUT		   - �� ����� ����
ZONE_IN		     - � ����� ����

VOLUME			   - ����� ������� � �������� VOLUME_UNIT_ID
VOLUME_UNIT_ID - ������� ��������� ������ �������
PRICE			     - ���� 1 VOLUME_UNIT_ID
AMOUNT			   - �����

BYTES			     - ����� � ������

CF			       - correction factor
CURRENCY_ID 	 � ������ ������
TAX_INCL 		   � �Y� = ����� �������
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- VPN - ������� ���������� �� ��������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Del_Items_VPN(
               p_period_id IN INTEGER,
               p_order_id  IN INTEGER
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Del_Items_VPN';
    v_count    INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id
                          ||', order_id = '||p_order_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ITEM-� 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.ORDER_ID      = p_order_id
       AND EXISTS (
           SELECT * FROM BDR_CCAD_VPN_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.ITEM_ID  = I.ITEM_ID
              AND CB.ORDER_ID = I.ORDER_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������ �� ITEM-� � BDR
    UPDATE BDR_CCAD_VOL_T CB SET CB.ITEM_ID = NULL
    WHERE CB.REP_PERIOD_ID = p_period_id
      AND CB.ORDER_ID      = p_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop. BDR_CCAD_VOL_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
 
-- --------------------------------------------------------------------- --
--  ������ ��� ���������� ������� �� ����������� ������
-- --------------------------------------------------------------------- --
PROCEDURE Billing_queue_list( 
               p_recordset OUT t_refc, 
               p_period_id IN INTEGER    -- ID ��������� ������� �����
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing_queue_list';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        WITH BC AS (
            SELECT B.BILL_ID, B.REP_PERIOD_ID, B.BILL_NO, B.PROFILE_ID, 
                   A.ACCOUNT_ID, A.ACCOUNT_NO, A.BILLING_ID, O.ORDER_ID, O.ORDER_NO, O.SERVICE_ID, S.SERVICE_CODE,  
                   OB.ORDER_BODY_ID, OB.SUBSERVICE_ID, OB.CHARGE_TYPE, OB.RATE_RULE_ID, SS.SUBSERVICE, SS.SUBSERVICE_KEY  
              FROM ACCOUNT_T A, ORDER_T O, ORDER_BODY_T OB, SERVICE_T S, SUBSERVICE_T SS, BILL_T B
             WHERE A.ACCOUNT_ID = O.ACCOUNT_ID
               AND O.ORDER_ID   = OB.ORDER_ID
               AND O.SERVICE_ID = S.SERVICE_ID
               AND O.SERVICE_ID IN (101,103, 104, 106, 108, 133, 149)
               AND SS.SUBSERVICE_ID = OB.SUBSERVICE_ID
               AND OB.RATE_RULE_ID IN (
                   2406,    -- IP, IP AccessIC - ���������, ��������� �� ����������� � ������ �������
                   2407,    -- �� ������ ������� 
                   2408,    -- IP VPN ����������� �� ������
                   2409,    -- IP, IP AccessIC - ����������� �� ������ BURST
                   2410,    -- ���������� �����������
                   2411     -- EPL BURST ����������� �� ������ 
               ) 
               AND A.ACCOUNT_ID = B.ACCOUNT_ID(+)
               AND B.REP_PERIOD_ID(+) = p_period_id
            ORDER BY A.ACCOUNT_NO, O.ORDER_NO, OB.CHARGE_TYPE
        )
        SELECT DISTINCT BILL_ID, ACCOUNT_ID, BILLING_ID, PROFILE_ID, 
               TO_NUMBER(NULL) TASK_ID, 
               REP_PERIOD_ID, REP_PERIOD_ID DATA_PERIOD_ID 
          FROM BC;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- --------------------------------------------------------------------- --
--  ����������� ���-�� ������� �������� ������ � ������� ������
-- --------------------------------------------------------------------- --
FUNCTION Correction_factor(
            p_order_id  IN INTEGER,
            p_month     IN DATE   -- ��� ����������� ������, ��� �������� ������������ �����������
        ) RETURN NUMBER  -- retun correction_factor
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Correction_factor';
    v_start_d   DATE := trunc(p_month);
    v_end_d     DATE := trunc(LAST_DAY(p_month))+1-1/(24*60*60);
    v_days_lock NUMBER; -- ���� ���������� � �������� �������

    v_date_from     date; -- ������ �������� ������ � �������� �������
    v_date_to       date; -- ��������� �������� ������ � �������� �������
    
    v_m             number; -- ���-�� ���� � ������
    v_p             number; -- ���-�� ���� � ������� p_dt_from-p_dt_to

    v_cf            number; -- correction factor
BEGIN
    select GREATEST(trunc(DATE_FROM),v_start_d), LEAST(trunc(DATE_TO)+1-1/(24*60*60),v_end_d)
    into v_date_from,v_date_to
    from ORDER_T
    where ORDER_ID = p_order_id;
    
    -- ������� ����� ���������� 
    select sum(least(trunc(ol.DATE_TO), v_date_to)-greatest((trunc( ol.DATE_FROM )+1),v_date_from)) -- days_lock  
    into v_days_lock
    from ORDER_LOCK_T ol
    where 
        ORDER_ID = p_order_id
        and
        least(trunc(ol.DATE_TO), v_date_to)-greatest((trunc( ol.DATE_FROM )+1),v_date_from)>0
        and
        (
          ol.DATE_FROM between  v_start_d and  v_end_d
          or 
          ol.DATE_TO between  v_start_d and  v_end_d
          or 
          v_start_d between ol.DATE_FROM and ol.DATE_TO
          or 
          v_end_d between ol.DATE_FROM and ol.DATE_TO
        );

     if v_days_lock is null then
        v_days_lock := 0;
     end if;
    
      v_m := add_months(trunc(p_month,'mm'),1)-trunc(p_month,'mm');
      v_p := trunc(v_date_to)-trunc(v_date_from)+1 - v_days_lock;

      if (v_p>0) then
        v_cf := v_p/v_m;
        return v_cf;
      else
        return 0;
      end if; 

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

END PK24_CCAD;
/
