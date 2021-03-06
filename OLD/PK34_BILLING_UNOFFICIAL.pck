CREATE OR REPLACE PACKAGE PK34_BILLING_UNOFFICIAL
IS
    --
    -- ����� ��� ��������� �������� ����������� ������
    -- ��� ���������� �������� �����
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK34_BILLING_UNOFFICIAL';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- ========================================================================= --
    --                               � � � � � � �
    -- ========================================================================= --
    -- ������ �������� �������� 
    -- ��� ��������������� ��������, ����������� ������ � ������ �������
    PROCEDURE Recalc_all_period_info ( p_period_id IN INTEGER );

    -- �������� �������� ������ �� ������������� �������� �����
    -- ��� ��������������� ��������, ����������� ������ � ������ �������
    PROCEDURE Recalc_period_info_by_account( p_period_id IN INTEGER, p_account_id IN INTEGER);

    -- ������ �������� �������
    -- ��� ��������������� ��������, ����������� ������ � ������ �������
    PROCEDURE Recalc_all_balances;

    -- ���������������� ������ �����, � ��� ����� � ���������
    -- ���� �� ������ ���� �������, ���� �������� - �� ���� ������ ����������� ��������
    PROCEDURE Rebuild_bill(
                        p_rep_period_id IN INTEGER,  -- ID ������������ �������
                        p_bill_id       IN INTEGER   -- ID �����
                      );

    -- �������� ����� BILL_T (� ����� �� � ��� �������)
    -- ���� ���� � ������� READY ��� CLOSED - ���������� ����������� REP_PERIOD_INFO_T �
    -- ������� ������� ������
    PROCEDURE Delete_bill(
                        p_rep_period_id IN INTEGER,  -- ID ������������ �������
                        p_bill_id       IN INTEGER   -- ID �����
                      );

    -- ������ ����������� BILLINFO_T.LAST_BILL_ID
    -- �� ���������� ������� ������������
    PROCEDURE Repair_billinfo(p_period_id IN INTEGER);

    -- ������������ �������� �� ��������� ������
    PROCEDURE Retransfer_payments(p_period_id IN INTEGER);

    -- ������������ �������� �� ��� ����������� ������� (������ ��� �������)
    -- �� p_period_id �� ������������
    PROCEDURE Retransfer_all_payments(p_period_id IN INTEGER);

END PK34_BILLING_UNOFFICIAL;
/
CREATE OR REPLACE PACKAGE BODY PK34_BILLING_UNOFFICIAL
IS

-- ========================================================================= --
--                               � � � � � � �
-- ========================================================================= --
-- ������ �������� ��������, �� ���������� ������� ������������ 
-- ��� ��������������� ��������, ����������� ������ � ������ �������
-- ========================================================================= --
PROCEDURE Recalc_all_period_info ( p_period_id IN INTEGER )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Recalc_all_period_info';
    v_count     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ����������� �����������    
    PK35_BILLING_SERVICE.Rep_period_info_t_drop_fk;
        
    -- �������� �������
    DELETE FROM REP_PERIOD_INFO_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Deleted '||v_count||' rows from REP_PERIOD_INFO_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- �������� ������������� ������
    INSERT INTO REP_PERIOD_INFO_T (
        REP_PERIOD_ID, ACCOUNT_ID, OPEN_BALANCE, CLOSE_BALANCE,
        TOTAL, GROSS, RECVD, ADVANCE, LAST_MODIFIED
    )
    SELECT REP_PERIOD_ID, ACCOUNT_ID, 
           ((RECVD_ALL-TOTAL_ALL) - (RECVD-TOTAL)) OPEN_BALANCE,
           (RECVD_ALL-TOTAL_ALL) CLOSE_BALANCE,
           TOTAL, GROSS, RECVD, ADVANCE, SYSDATE LAST_MODIFIED
    FROM (
        SELECT ACCOUNT_ID, REP_PERIOD_ID, TOTAL, GROSS, RECVD, ADVANCE,
               SUM(TOTAL) OVER (PARTITION BY ACCOUNT_ID ORDER BY REP_PERIOD_ID) TOTAL_ALL,
               SUM(RECVD) OVER (PARTITION BY ACCOUNT_ID ORDER BY REP_PERIOD_ID) RECVD_ALL
          FROM (
            SELECT ACCOUNT_ID, REP_PERIOD_ID, 
                   SUM(TOTAL) TOTAL, SUM(GROSS) GROSS, 
                   SUM(RECVD) RECVD, SUM(ADVANCE) ADVANCE
            FROM (
                SELECT B.ACCOUNT_ID, B.REP_PERIOD_ID, B.TOTAL, B.GROSS, 0 RECVD, 0 ADVANCE
                  FROM BILL_T B
                 WHERE REP_PERIOD_ID <= p_period_id
                   AND B.TOTAL <> 0 -- �������� ������ � ������� �������
                UNION ALL
                SELECT P.ACCOUNT_ID, P.REP_PERIOD_ID, 0 TOTAL, 0 GROSS, P.RECVD, P.ADVANCE
                  FROM PAYMENT_T P
                 WHERE REP_PERIOD_ID <= p_period_id
            )GROUP BY ACCOUNT_ID, REP_PERIOD_ID
        )
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Inserted '||v_count||' rows from REP_PERIOD_INFO_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ��������������� ����������� �����������
    PK35_BILLING_SERVICE.Rep_period_info_t_add_fk;
    
    -- ������������ ���������
    COMMIT;
    
    -- �������� ���������� �� �������
    Gather_Table_Stat(l_Tab_Name => 'REP_PERIOD_INFO_T');
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

PROCEDURE Recalc_period_info_by_account( p_period_id IN INTEGER, p_account_id IN INTEGER)
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Recalc_period_info_by_account';
    v_count     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- �������� �������
    DELETE FROM REP_PERIOD_INFO_T WHERE ACCOUNT_ID = p_account_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Deleted '||v_count||' rows from REP_PERIOD_INFO_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- �������� ������������� ������
    INSERT INTO REP_PERIOD_INFO_T (
        REP_PERIOD_ID, ACCOUNT_ID, OPEN_BALANCE, CLOSE_BALANCE,
        TOTAL, GROSS, RECVD, ADVANCE, LAST_MODIFIED
    )
    SELECT REP_PERIOD_ID, ACCOUNT_ID, 
           ((RECVD_ALL-TOTAL_ALL) - (RECVD-TOTAL)) OPEN_BALANCE,
           (RECVD_ALL-TOTAL_ALL) CLOSE_BALANCE,
           TOTAL, GROSS, RECVD, ADVANCE, SYSDATE LAST_MODIFIED
    FROM (
        SELECT ACCOUNT_ID, REP_PERIOD_ID, TOTAL, GROSS, RECVD, ADVANCE,
               SUM(TOTAL) OVER (PARTITION BY ACCOUNT_ID ORDER BY REP_PERIOD_ID) TOTAL_ALL,
               SUM(RECVD) OVER (PARTITION BY ACCOUNT_ID ORDER BY REP_PERIOD_ID) RECVD_ALL
          FROM (
            SELECT ACCOUNT_ID, REP_PERIOD_ID, 
                   SUM(TOTAL) TOTAL, SUM(GROSS) GROSS, 
                   SUM(RECVD) RECVD, SUM(ADVANCE) ADVANCE
            FROM (
                SELECT B.ACCOUNT_ID, B.REP_PERIOD_ID, B.TOTAL, B.GROSS, 0 RECVD, 0 ADVANCE
                  FROM BILL_T B
                 WHERE REP_PERIOD_ID <= p_period_id
                   --AND B.TOTAL > 0 -- �������� ������ � ������� �������
                   AND B.ACCOUNT_ID = p_account_id
                UNION ALL
                SELECT P.ACCOUNT_ID, P.REP_PERIOD_ID, 0 TOTAL, 0 GROSS, P.RECVD, P.ADVANCE
                  FROM PAYMENT_T P
                 WHERE REP_PERIOD_ID <= p_period_id
                       AND P.ACCOUNT_ID = p_account_id
            )GROUP BY ACCOUNT_ID, REP_PERIOD_ID
        )
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Inserted '||v_count||' rows from REP_PERIOD_INFO_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������������ ���������
    COMMIT;
    
    -- �������� ���������� �� �������
    Gather_Table_Stat(l_Tab_Name => 'REP_PERIOD_INFO_T');
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- ------------------------------------------------------------------------- --
-- ������ �������� �������
-- ��� ��������������� ��������, ����������� ������ � ������ �������
-- ------------------------------------------------------------------------- --
PROCEDURE Recalc_all_balances
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Recalc_all_balances';
    v_count     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    MERGE INTO ACCOUNT_T A
    USING   
       (SELECT ACCOUNT_ID, SUM(RECVD-BILL_TOTAL) BALANCE,
               CASE
                   WHEN MAX(BILL_DATE) > MAX(PAYMENT_DATE) THEN MAX(BILL_DATE)
                   ELSE MAX(PAYMENT_DATE)
               END BALANCE_DATE 
        FROM (
            -- �������� ������ ������������� �� ������������ ������
            SELECT B.ACCOUNT_ID, 
                   B.TOTAL BILL_TOTAL, BILL_DATE, 
                   0 RECVD, TO_DATE('01.01.2000','dd.mm.yyyy') PAYMENT_DATE 
              FROM BILL_T B
             WHERE B.TOTAL <> 0 -- (����� ���� �������������� � �������) �������� ������ � ������� �������
            UNION ALL
            -- �������� ����� ����������� �� ������ ��������
            SELECT P.ACCOUNT_ID, 
                   0 BILL_TOTAL, TO_DATE('01.01.2000','dd.mm.yyyy') BILL_DATE,
                   P.RECVD, P.PAYMENT_DATE  
              FROM PAYMENT_T P
        )
        GROUP BY ACCOUNT_ID) T
    ON (A.ACCOUNT_ID = T.ACCOUNT_ID)
    WHEN MATCHED THEN UPDATE SET A.BALANCE_DATE = T.BALANCE_DATE, A.BALANCE = T.BALANCE;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Updated '||v_count||' rows in ACCOUNT_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    
    -- ������������ ���������
    COMMIT;
    
    -- �������� ���������� �� �������
    Gather_Table_Stat(l_Tab_Name => 'ACCOUNT_T');
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- ==================================================================================== --
-- ���������������� ������ �����, � ��� ����� � ���������
-- ���� �� ������ ���� �������, ���� �������� - �� ���� ������ ����������� ��������
-- ==================================================================================== --
PROCEDURE Rebuild_bill(
                    p_rep_period_id IN INTEGER,  -- ID ������������ �������
                    p_bill_id       IN INTEGER   -- ID �����
                  )
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Rebuild_bill';
    v_count        INTEGER;
    v_bill_recvd   NUMBER;
    v_account_id   INTEGER;
    v_account_type ACCOUNT_T.ACCOUNT_TYPE%TYPE;
    v_vat          NUMBER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start bill_id='||p_bill_id||', rep_period_id='||p_rep_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ���������� � �������
    -- ���� ���� ������ �� �����, �� �������
    SELECT B.RECVD, B.ACCOUNT_ID, A.ACCOUNT_TYPE, AP.VAT
      INTO v_bill_recvd, v_account_id, v_account_type, v_vat
      FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP
     WHERE B.BILL_ID = p_bill_id
       AND B.REP_PERIOD_ID = p_rep_period_id
       AND A.ACCOUNT_ID = B.ACCOUNT_ID
       AND A.ACCOUNT_ID = AP.ACCOUNT_ID
       AND AP.DATE_FROM <= B.BILL_DATE
       AND (AP.DATE_TO IS NULL OR B.BILL_DATE <= AP.DATE_TO);
    IF v_bill_recvd != 0 THEN
        Pk01_Syslog.raise_Exception('������, �� ���� (��������))�������, ��� �����������, ����� ����� ������ ', c_PkgName||'.'||v_prcName );
    END IF;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������� ���������� � ������������� �������� ������ � ��������� READY,
    -- ��������� ���� ������� ������� ���������� � ������������� ��� ������� �������
    UPDATE ITEM_T I 
       SET I.REP_GROSS = 
           CASE
             WHEN I.TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN
                 (I.ITEM_TOTAL - PK09_INVOICE.ALLOCATE_TAX(I.ITEM_TOTAL, v_vat))
             WHEN I.TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_NOT_INCL THEN
                  I.ITEM_TOTAL
             ELSE 
                  NULL
             END, 
           I.REP_TAX =
           CASE
             WHEN I.TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN
                 (PK09_INVOICE.ALLOCATE_TAX(I.ITEM_TOTAL, v_vat))
             WHEN I.TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_NOT_INCL THEN
                  PK09_INVOICE.CALC_TAX(I.ITEM_TOTAL, v_vat)
             ELSE 
                  NULL
           END,
           I.INV_ITEM_ID = NULL,
           I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_RE�DY
    WHERE I.ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, Pk00_Const.c_ITEM_TYPE_ADJUST)
      AND I.REP_PERIOD_ID = p_rep_period_id
      AND I.BILL_ID = p_bill_id
    ;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Move status for '||v_count||' - items to '
                          || Pk00_Const.c_ITEM_STATE_RE�DY, 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ������� READY � EMPTY, ������ ����� � ���������� �������� �� ���������
    -- (��������� � ����� �����, � �� ���������� �����)
    UPDATE BILL_T B
       SET (B.BILL_STATUS, B.TOTAL, B.GROSS, B.TAX, B.DUE, B.DUE_DATE, B.CALC_DATE) = (
           SELECT
               CASE 
                   WHEN EXISTS (
                       SELECT * FROM ITEM_T I 
                        WHERE I.BILL_ID = B.BILL_ID
                          AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
                          AND I.ITEM_STATUS = Pk00_Const.c_ITEM_STATE_RE�DY
                   )
                   THEN Pk00_Const.c_BILL_STATE_READY
                   ELSE Pk00_Const.c_BILL_STATE_EMPTY
               END, 0, 0, 0, 0, SYSDATE, SYSDATE
           FROM DUAL 
       )
    WHERE B.REP_PERIOD_ID = p_rep_period_id
      AND B.BILL_ID = p_bill_id;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||' - bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �������������� ����� ������ ������
    DELETE FROM INVOICE_ITEM_T II
    WHERE II.REP_PERIOD_ID = p_rep_period_id
      AND II.BILL_ID = p_bill_id;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Deleted: '||v_count||' - invoice_items', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������� ��� ���� �������� ������������� ������ ������������ �������
    --
    v_count := Pk09_Invoice.Calc_invoice(
                     p_bill_id       => p_bill_id,
                     p_rep_period_id => p_rep_period_id
                  );
    Pk01_Syslog.Write_msg('Created: '||v_count||' - invoice_items', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
   
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ���� �����, ��� �������� ������ �/� ������������ �������
    -- � ������������� ������������ ���� ������ �����, �� ��������� ���� ����������� �����
    UPDATE BILL_T B 
       SET (B.TOTAL, B.GROSS, B.TAX, B.DUE) = (
          SELECT NVL(SUM(II.TOTAL),0), NVL(SUM(II.GROSS),0), 
                 NVL(SUM(II.TAX),0),  -NVL(SUM(II.TOTAL),0)
            FROM INVOICE_ITEM_T II
           WHERE II.BILL_ID = B.BILL_ID
             AND II.REP_PERIOD_ID = B.REP_PERIOD_ID
        ),
        (B.PAID_TO) = (
          SELECT CASE 
                   WHEN BI.DAYS_FOR_PAYMENT IS NULL THEN -- "�����" - �������� �� ���������
                     ADD_MONTHS(B.BILL_DATE, 1)
                   ELSE
                     B.BILL_DATE + BI.DAYS_FOR_PAYMENT
                 END PAID_TO
            FROM BILLINFO_T BI
           WHERE BI.ACCOUNT_ID = B.ACCOUNT_ID
        ),
        B.DUE_DATE  = B.BILL_DATE,
        B.CALC_DATE = SYSDATE
    WHERE B.REP_PERIOD_ID = p_rep_period_id
      AND B.BILL_ID       = p_bill_id;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ==================================================================================== --
-- �������� ����� BILL_T (� ����� �� � ��� �������)
-- ���� ���� � ������� READY ��� CLOSED - ���������� ����������� REP_PERIOD_INFO_T �
-- ������� ������� ������
-- ==================================================================================== --
PROCEDURE Delete_bill(
                    p_rep_period_id IN INTEGER,  -- ID ������������ �������
                    p_bill_id       IN INTEGER   -- ID �����
                  )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_bill';
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ������� ������� �����
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_rep_period_id
       AND I.BILL_ID = p_bill_id;
       
    -- ������� ������� �����-�������
    DELETE FROM INVOICE_ITEM_T II
     WHERE II.REP_PERIOD_ID = p_rep_period_id
       AND II.BILL_ID = p_bill_id;
        
    -- ������� ����
    DELETE FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_rep_period_id
       AND B.BILL_ID = p_bill_id;
    
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ==================================================================================== --
-- ������ ����������� BILLINFO_T.LAST_BILL_ID
-- �� ���������� ������� ������������
-- ==================================================================================== --
PROCEDURE Repair_billinfo(p_period_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Repair_billinfo';
    v_count       INTEGER;
    v_error       INTEGER;
    v_period_id   INTEGER;
    v_bill_id     INTEGER;
    v_bill_no     BILL_T.BILL_NO%TYPE;
    v_bill_date   DATE;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- -------------------------------------------------------------------------- --
    -- �������� � �������� ���������
    -- -------------------------------------------------------------------------- --
    UPDATE BILLINFO_T BI 
       SET  BI.LAST_BILL_ID   = NULL, 
            BI.LAST_PERIOD_ID = NULL,
            BI.SQ_BILL_NO     = NULL,
            BI.BILL_NAME      = NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Reset: '||v_count||' - rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- -------------------------------------------------------------------------- --
    -- ��������������� ��� ����� � ����� �� ������������ ������
    -- -------------------------------------------------------------------------- --
    MERGE INTO BILLINFO_T BI
    USING (
        WITH LAST_BILL AS (
            SELECT ACCOUNT_ID, REP_PERIOD_ID, BILL_ID, BILL_NO, BILL_NUM, MAX_BILL_NUM
              FROM (
                SELECT MAX(REP_PERIOD_ID) OVER (PARTITION BY ACCOUNT_ID) MAX_PERIOD_ID,
                       SUBSTR(BILL_NO, INSTR(BILL_NO, '-', -1)+1, 4) BILL_NUM,
                       MAX(SUBSTR(BILL_NO, INSTR(BILL_NO, '-', -1)+1, 4)) OVER (PARTITION BY ACCOUNT_ID, REP_PERIOD_ID) MAX_BILL_NUM,
                       REP_PERIOD_ID, BILL_ID, BILL_NO, ACCOUNT_ID 
                  FROM (
                    SELECT ACCOUNT_ID, REP_PERIOD_ID, BILL_ID, BILL_NO FROM BILL_T B
                     WHERE REP_PERIOD_ID < p_period_id
                       --AND BILL_NO NOT LIKE '%-CORRECT'
                       AND BILL_NO NOT LIKE '%-[_]'
                       AND BILL_TYPE = Pk00_Const.c_BILL_TYPE_REC -- 'B'
                    UNION ALL
                    SELECT ACCOUNT_ID, REP_PERIOD_ID, BILL_ID, BILL_NO FROM BILL_T B
                     WHERE REP_PERIOD_ID = p_period_id
                       AND BILL_TYPE = Pk00_Const.c_BILL_TYPE_REC -- 'B'
                       AND EXISTS (
                           SELECT * FROM ITEM_T I
                            WHERE I.REP_PERIOD_ID = B.REP_PERIOD_ID
                              AND I.BILL_ID = B.BILL_ID
                       )
                )WHERE RTRIM(SUBSTR(BILL_NO, INSTR(BILL_NO, '-', -1)+1, 4),'0123456789') IS NULL
            )
            WHERE MAX_PERIOD_ID = REP_PERIOD_ID
              AND MAX_BILL_NUM   = BILL_NUM
        )
        SELECT ACCOUNT_ID, REP_PERIOD_ID, BILL_ID, BILL_NO, BILL_NUM, MAX_BILL_NUM  
          FROM LAST_BILL
         WHERE RTRIM(SUBSTR(BILL_NO, INSTR(BILL_NO, '-', -1)+1, 4),'0123456789') IS NULL
        ORDER BY REP_PERIOD_ID DESC
    ) BILL
    ON (BI.ACCOUNT_ID = BILL.ACCOUNT_ID)
    WHEN MATCHED THEN UPDATE SET BI.LAST_BILL_ID   = BILL.BILL_ID, 
                                 BI.LAST_PERIOD_ID = BILL.REP_PERIOD_ID,
                                 BI.SQ_BILL_NO     = TO_NUMBER(SUBSTR(BILL.BILL_NO, INSTR(BILL.BILL_NO, '-', -1)+1, 4)),
                                 BI.BILL_NAME      = TRIM(SUBSTR(BILL.BILL_NO, 1, INSTR(BILL.BILL_NO, '-', -1)-1))
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Repair: '||v_count||' - rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
 
    -- -------------------------------------------------------------------------- --
    -- ��� �/� �� ������� �� ������������ ����� ��� ����� = � ��������
    -- -------------------------------------------------------------------------- --
    UPDATE BILLINFO_T BI
    SET BI.BILL_NAME = (
          SELECT C.CONTRACT_NO  
            FROM ACCOUNT_PROFILE_T AP, CONTRACT_T C
           WHERE AP.CONTRACT_ID = C.CONTRACT_ID
             AND AP.ACCOUNT_ID  = BI.ACCOUNT_ID 
        ),
        SQ_BILL_NO = 0
    WHERE BI.BILL_NAME IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Set bill_name for: '||v_count||' - empty accounts', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

        
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ==================================================================================== --
-- ������������ �������� �� ��������� ������
-- ==================================================================================== --
PROCEDURE Retransfer_payments(p_period_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Retransfer_payments';
    v_count       INTEGER := 0;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������� �� �������� ���������� � ��������
    PK35_BILLING_SERVICE.Transfer_t_drop_fk;
    FOR rp IN (
      SELECT PAYMENT_ID, REP_PERIOD_ID 
        FROM PAYMENT_T
       WHERE REP_PERIOD_ID = p_period_id
    )
    LOOP
      PK10_PAYMENTS_TRANSFER.Delete_transfer_chain (
               p_pay_period_id => rp.rep_period_id,
               p_payment_id    => rp.payment_id
           );
      v_count := v_count + 1;
    END LOOP;
    PK35_BILLING_SERVICE.Transfer_t_add_fk;
    Pk01_Syslog.Write_msg('deleted transer chain from '||v_count||' payments', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    -- ����������� �������
    PK35_BILLING_SERVICE.Gather_Table_Stat('PAY_TRANSFER_T');    
    -- ���������� �������� ������
    PK10_PAYMENTS_TRANSFER.Method_fifo;
    COMMIT;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


-- ==================================================================================== --
-- ������������ �������� �� ��� ����������� ������� (������ ��� �������)
-- �� p_period_id �� ������������
-- ==================================================================================== --
PROCEDURE Retransfer_all_payments(p_period_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Retransfer_all_payments';
    v_count       INTEGER := 0;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ������� ���������� � �������� �� ������
    UPDATE BILL_T B
       SET B.RECVD = 0, B.DUE = -B.TOTAL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� ���������� � �������� �� ��������
    UPDATE PAYMENT_T P
       SET P.TRANSFERED = 0, 
           P.BALANCE = P.RECVD - P.REFUND, 
           P.ADVANCE = P.RECVD - P.REFUND,
           P.DATE_FROM = NULL,
           P.DATE_TO   = NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PAYMENT_T '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    COMMIT;
    
    -- ������� ��� �������� ��������
    PK35_BILLING_SERVICE.Transfer_t_drop_fk;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PAY_TRANSFER_T DROP STORAGE';
    PK35_BILLING_SERVICE.Transfer_t_add_fk;

    -- ����������� �������
    PK35_BILLING_SERVICE.Gather_Table_Stat('PAY_TRANSFER_T');
    
    -- ���������� ���������� ������������
    PK10_PAYMENTS_TRANSFER.Method_fifo;
    COMMIT;
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;




END PK34_BILLING_UNOFFICIAL;
/
