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
    -- ������ �������� ��������, �� ������� 'LAST' ������������
    -- ��� ��������������� ��������, ����������� ������ � ������ �������
    PROCEDURE Recalc_all_period_info;

    -- �������� �������� ������ �� ������������� �������� �����
    -- ��� ��������������� ��������, ����������� ������ � ������ �������
    PROCEDURE Recalc_period_info_by_account( 
                   p_account_id IN INTEGER
               );

    -- �������� �������� �� ��������� ������
    PROCEDURE View_period_info (
                   p_recordset    OUT t_refc,
                   p_period_id    IN  NUMBER,
                   p_account_type IN  VARCHAR2 DEFAULT 'J'
               );

    -- �������� ������������ ������� ��������
    -- � ������� ������ � ��������, ������� ��������� �������� ������
    PROCEDURE Check_period_info (
                   p_recordset    OUT t_refc,
                   p_period_id    IN INTEGER 
               );

    -- ������ �������� �������
    -- ��� ��������������� ��������, ����������� ������ � ������ �������
    PROCEDURE Recalc_all_balances;

    -- ������ �������� ��������, ���� ����������� ������
    PROCEDURE Recalc_due_for_all_bills;

    /*
    -- �������� ����� BILL_T (� ����� �� � ��� �������)
    -- ���� ���� � ������� READY ��� CLOSED - ���������� ����������� REP_PERIOD_INFO_T �
    -- ������� ������� ������
    PROCEDURE Delete_bill(
                        p_rep_period_id IN INTEGER,  -- ID ������������ �������
                        p_bill_id       IN INTEGER   -- ID �����
                      );
    */
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
--// ������ �������� ��������, �� ���������� ������� ������������
-- ������ �������� ��������, �� ������� 'LAST' ������������ 
-- ��� ��������������� ��������, ����������� ������ � ������ �������
-- ========================================================================= --
PROCEDURE Recalc_all_period_info
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Recalc_all_period_info';
    v_period_id INTEGER;
    v_count     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ��������� �� ���������� ��������� ������� ������������
    SELECT PERIOD_ID INTO v_period_id
      FROM (
        SELECT PERIOD_ID 
          FROM PERIOD_T
         WHERE POSITION IN ('LAST', 'BILL')
         ORDER BY PERIOD_ID DESC
     )
     WHERE ROWNUM = 1;

    -- ������� ����������� �����������    
    PK35_BILLING_SERVICE.Rep_period_info_t_drop_fk;
        
    -- �������� �������
    EXECUTE IMMEDIATE 'TRUNCATE TABLE REP_PERIOD_INFO_T DROP STORAGE';
    --DELETE FROM REP_PERIOD_INFO_T;
    --v_count := SQL%ROWCOUNT;
    --Pk01_Syslog.Write_msg('Deleted '||v_count||' rows from REP_PERIOD_INFO_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- �������� ������������� ������
    INSERT INTO REP_PERIOD_INFO_T (
        REP_PERIOD_ID, ACCOUNT_ID, OPEN_BALANCE, CLOSE_BALANCE,
        TOTAL, GROSS, RECVD, ADVANCE, LAST_MODIFIED
    )
    SELECT REP_PERIOD_ID, ACCOUNT_ID, 
           CASE
            WHEN INBAL != 0 THEN INBAL
            ELSE (CLOSE_BALANCE - (RECVD-TOTAL)) 
           END OPEN_BALANCE,
           CLOSE_BALANCE,
           TOTAL, GROSS, RECVD, ADVANCE, -- INBAL, 
           SYSDATE LAST_MODIFIED
      FROM (
        SELECT ACCOUNT_ID, REP_PERIOD_ID, TOTAL, GROSS, RECVD, ADVANCE, INBAL,
               SUM(INBAL + RECVD - TOTAL) OVER (PARTITION BY ACCOUNT_ID ORDER BY REP_PERIOD_ID) CLOSE_BALANCE
          FROM (
            SELECT ACCOUNT_ID, REP_PERIOD_ID, 
                   SUM(INBAL) INBAL,
                   SUM(TOTAL) TOTAL, SUM(GROSS) GROSS, 
                   SUM(RECVD) RECVD, SUM(ADVANCE) ADVANCE
              FROM (
                SELECT ACCOUNT_ID, REP_PERIOD_ID, TOTAL, GROSS, RECVD, ADVANCE, INBAL
                  FROM (
                    SELECT B.ACCOUNT_ID, B.REP_PERIOD_ID,
                           CASE
                            WHEN B.BILL_TYPE != 'I' THEN B.TOTAL
                            ELSE 0
                           END TOTAL, 
                           CASE
                            WHEN B.BILL_TYPE != 'I' THEN B.GROSS
                            ELSE 0
                           END GROSS,
                           0 RECVD, 0 ADVANCE,
                           CASE
                            WHEN B.BILL_TYPE  = 'I' THEN -B.TOTAL
                            ELSE 0 
                           END INBAL
                      FROM BILL_T B
                     WHERE REP_PERIOD_ID <= v_period_id
                    UNION ALL
                    SELECT P.ACCOUNT_ID, P.REP_PERIOD_ID, 
                           0 TOTAL, 0 GROSS, 
                           CASE
                            WHEN P.PAYMENT_TYPE != 'INBAL' THEN P.RECVD
                            ELSE 0 
                           END RECVD,
                           CASE
                            WHEN P.PAYMENT_TYPE != 'INBAL' THEN P.ADVANCE
                            ELSE 0 
                           END ADVANCE,
                           CASE
                            WHEN P.PAYMENT_TYPE  = 'INBAL' THEN P.RECVD
                            ELSE 0 
                           END INBAL
                      FROM PAYMENT_T P
                     WHERE REP_PERIOD_ID <= v_period_id
                ) BP
                WHERE NOT EXISTS (
                    SELECT * 
                      FROM INCOMING_BALANCE_T IB
                     WHERE IB.ACCOUNT_ID    = BP.ACCOUNT_ID
                       AND IB.REP_PERIOD_ID > BP.REP_PERIOD_ID 
                )
              )
              GROUP BY ACCOUNT_ID, REP_PERIOD_ID
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

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� �������� ������ �� ������������� �������� �����
-- ��������� ��������� ���������� ���������
-- ��� ��������������� ��������, ����������� ������ � ������ �������
PROCEDURE Recalc_period_info_by_account( 
               p_account_id IN INTEGER
           )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Recalc_period_info_by_account';
    v_period_id INTEGER;
    v_count     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� ��������� �� ���������� ��������� ������� ������������
    SELECT PERIOD_ID INTO v_period_id 
      FROM PERIOD_T
     WHERE POSITION = 'LAST';
    
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
           CASE
            WHEN INBAL != 0 THEN INBAL
            ELSE (CLOSE_BALANCE - (RECVD-TOTAL)) 
           END OPEN_BALANCE,
           CLOSE_BALANCE,
           TOTAL, GROSS, RECVD, ADVANCE, -- INBAL, 
           SYSDATE LAST_MODIFIED
      FROM (
        SELECT ACCOUNT_ID, REP_PERIOD_ID, TOTAL, GROSS, RECVD, ADVANCE, INBAL,
               SUM(INBAL + RECVD - TOTAL) OVER (PARTITION BY ACCOUNT_ID ORDER BY REP_PERIOD_ID) CLOSE_BALANCE
          FROM (
            SELECT ACCOUNT_ID, REP_PERIOD_ID, 
                   SUM(INBAL) INBAL,
                   SUM(TOTAL) TOTAL, SUM(GROSS) GROSS, 
                   SUM(RECVD) RECVD, SUM(ADVANCE) ADVANCE
              FROM (
                SELECT ACCOUNT_ID, REP_PERIOD_ID, TOTAL, GROSS, RECVD, ADVANCE, INBAL
                  FROM (
                    SELECT B.ACCOUNT_ID, B.REP_PERIOD_ID,
                           CASE
                            WHEN B.BILL_TYPE != 'I' THEN B.TOTAL
                            ELSE 0
                           END TOTAL, 
                           CASE
                            WHEN B.BILL_TYPE != 'I' THEN B.GROSS
                            ELSE 0
                           END GROSS,
                           0 RECVD, 0 ADVANCE,
                           CASE
                            WHEN B.BILL_TYPE  = 'I' THEN -B.TOTAL
                            ELSE 0 
                           END INBAL
                      FROM BILL_T B
                     WHERE REP_PERIOD_ID <= v_period_id
                       AND B.ACCOUNT_ID = p_account_id
                    UNION ALL
                    SELECT P.ACCOUNT_ID, P.REP_PERIOD_ID, 
                           0 TOTAL, 0 GROSS, 
                           CASE
                            WHEN P.PAYMENT_TYPE != 'INBAL' THEN P.RECVD
                            ELSE 0 
                           END RECVD,
                           CASE
                            WHEN P.PAYMENT_TYPE != 'INBAL' THEN P.ADVANCE
                            ELSE 0 
                           END ADVANCE,
                           CASE
                            WHEN P.PAYMENT_TYPE  = 'INBAL' THEN P.RECVD
                            ELSE 0 
                           END INBAL
                      FROM PAYMENT_T P
                     WHERE REP_PERIOD_ID <= v_period_id
                       AND P.ACCOUNT_ID = p_account_id
                ) BP
                WHERE NOT EXISTS (
                    SELECT * 
                      FROM INCOMING_BALANCE_T IB
                     WHERE IB.ACCOUNT_ID    = BP.ACCOUNT_ID
                       AND IB.REP_PERIOD_ID > BP.REP_PERIOD_ID 
                )
              )
              GROUP BY ACCOUNT_ID, REP_PERIOD_ID
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
-- �������� �������� �� ��������� ������
-- ------------------------------------------------------------------------- --
PROCEDURE View_period_info (
               p_recordset    OUT t_refc,
               p_period_id    IN  NUMBER,
               p_account_type IN  VARCHAR2 DEFAULT 'J'
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'View_period_info';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
      WITH PI AS (
          SELECT ACCOUNT_NO, ACCOUNT_ID, 
                 NVL(REP_PERIOD_ID, p_period_id) REP_PERIOD_ID, 
                 NVL(OPEN_BALANCE, 0)  OPEN_BALANCE, 
                 NVL(CLOSE_BALANCE, 0) CLOSE_BALANCE, 
                 NVL(TOTAL, 0) TOTAL,
                 NVL(GROSS, 0) GROSS,
                 NVL(RECVD, 0) RECVD
            FROM (
              SELECT A.ACCOUNT_NO, A.ACCOUNT_ID, 
                     PI.REP_PERIOD_ID, PI.OPEN_BALANCE, PI.CLOSE_BALANCE, PI.TOTAL, PI.GROSS, PI.RECVD,
                     MAX(PI.REP_PERIOD_ID) OVER (PARTITION BY PI.ACCOUNT_ID) MAX_PERIOD_ID 
                FROM ACCOUNT_T A, REP_PERIOD_INFO_T PI
               WHERE A.ACCOUNT_ID   = PI.ACCOUNT_ID(+)
                 AND PI.REP_PERIOD_ID(+) <= p_period_id
                 AND A.ACCOUNT_TYPE = p_account_type
           )
           WHERE (REP_PERIOD_ID = MAX_PERIOD_ID OR REP_PERIOD_ID IS NULL)
      )
      SELECT --PI.REP_PERIOD_ID,
             PI.ACCOUNT_ID,
             PI.ACCOUNT_NO,
             CASE
                 WHEN PI.REP_PERIOD_ID < p_period_id THEN PI.CLOSE_BALANCE
                 ELSE PI.OPEN_BALANCE
             END IN_BALANCE,
             CASE
                 WHEN PI.REP_PERIOD_ID < p_period_id THEN 0
                 ELSE PI.TOTAL
             END TOTAL,
             CASE
                 WHEN PI.REP_PERIOD_ID < p_period_id THEN 0
                 ELSE PI.RECVD
             END RECVD,
             CASE
                 WHEN PI.REP_PERIOD_ID < p_period_id THEN PI.CLOSE_BALANCE
                 ELSE PI.CLOSE_BALANCE
             END OUT_BALANCE
        FROM PI
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- ------------------------------------------------------------------------- --
-- �������� ������������ ������� ��������
-- � ������� ������ � ��������, ������� ��������� �������� ������
-- ------------------------------------------------------------------------- --
PROCEDURE Check_period_info (
               p_recordset    OUT t_refc,
               p_period_id    IN INTEGER 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_period_info';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
        WITH RA AS (
            SELECT C.CONTRACT_ID, C.CONTRACT_NO, AP.ACCOUNT_ID, A.ACCOUNT_NO, 
                   RP.REP_PERIOD_ID, RP.OPEN_BALANCE, RP.TOTAL, RP.RECVD, RP.CLOSE_BALANCE 
              FROM REP_PERIOD_INFO_T RP, ACCOUNT_PROFILE_T AP, CONTRACT_T C, ACCOUNT_T A
             WHERE RP.ACCOUNT_ID = AP.ACCOUNT_ID
               AND TO_DATE(RP.REP_PERIOD_ID, 'yyyymm') > AP.DATE_FROM
               AND (AP.DATE_TO IS NULL OR TO_DATE(RP.REP_PERIOD_ID, 'yyyymm') < AP.DATE_TO)
               AND AP.CONTRACT_ID = C.CONTRACT_ID
               AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
               AND A.ACCOUNT_TYPE = 'J'
               AND A.BILLING_ID   != 2003
               AND RP.REP_PERIOD_ID = p_period_id
        ), RC AS (
            SELECT CONTRACT_ID, CONTRACT_NO, REP_PERIOD_ID, 
                   SUM(OPEN_BALANCE) OPEN_BALANCE, 
                   SUM(TOTAL) TOTAL, 
                   SUM(RECVD) RECVD, 
                   SUM(CLOSE_BALANCE) CLOSE_BALANCE
              FROM RA
             GROUP BY CONTRACT_ID, CONTRACT_NO, REP_PERIOD_ID
        ), RAE AS ( 
            SELECT RA.*, B.BILL_NO, B.TOTAL BILL_TOTAL, P.RECVD IN_RECVD 
              FROM RA, BILL_T B, PAYMENT_T P
             WHERE RA.ACCOUNT_ID    = B.ACCOUNT_ID(+)
               AND RA.REP_PERIOD_ID = B.REP_PERIOD_ID(+)
               AND B.BILL_TYPE(+)   = 'I'
               AND RA.ACCOUNT_ID    = P.ACCOUNT_ID(+)
               AND RA.REP_PERIOD_ID = P.REP_PERIOD_ID(+)
               AND P.PAYMENT_TYPE(+)= 'INBAL'
        )
        SELECT * FROM RAE 
       ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- ------------------------------------------------------------------------- --
-- ������ �������� �������
-- ��������� �������� ������� 
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
    USING (
       /*
       SELECT BP.ACCOUNT_ID, 
              MIN(NVL(IB.BALANCE, 0))+SUM(BP.RECVD-BP.BILL_TOTAL) BALANCE, 
              GREATEST( NVL(MAX(IB.BALANCE_DATE), TO_DATE('01.01.2000','dd.mm.yyyy')), 
                            MAX(BP.BILL_DATE), 
                            MAX(BP.PAYMENT_DATE)) BALANCE_DATE 
        FROM (
            -- �������� ������ ������������� �� ������������ ������
            SELECT B.ACCOUNT_ID, 
                   B.REP_PERIOD_ID,
                   B.TOTAL BILL_TOTAL, 
                   BILL_DATE, 
                   0 RECVD, TO_DATE('01.01.2000','dd.mm.yyyy') PAYMENT_DATE 
              FROM BILL_T B
             WHERE B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_READY, 
                                     Pk00_Const.c_BILL_STATE_CLOSED)
            UNION ALL
            -- �������� ����� ����������� �� ������ ��������
            SELECT P.ACCOUNT_ID, 
                   P.REP_PERIOD_ID,
                   0 BILL_TOTAL, TO_DATE('01.01.2000','dd.mm.yyyy') BILL_DATE,
                   P.RECVD, 
                   P.PAYMENT_DATE  
              FROM PAYMENT_T P
        ) BP, INCOMING_BALANCE_T IB 
       WHERE BP.ACCOUNT_ID = IB.ACCOUNT_ID(+)
       AND CASE
            WHEN IB.ACCOUNT_ID IS NULL THEN 1
            WHEN IB.ACCOUNT_ID IS NOT NULL AND BP.BILL_DATE     > IB.BALANCE_DATE  THEN 1
            WHEN IB.ACCOUNT_ID IS NOT NULL AND BP.REP_PERIOD_ID > IB.REP_PERIOD_ID THEN 1
            ELSE 0
           END = 1
        GROUP BY BP.ACCOUNT_ID
        */
        /*
        WITH BP AS (      
          SELECT *
            FROM (
                -- �������� ������ ������������� �� ������������ ������
                SELECT B.ACCOUNT_ID, 
                       B.REP_PERIOD_ID,
                       B.BILL_DATE BP_DATE,
                       B.TOTAL BILL_TOTAL, 
                       0 RECVD
                  FROM BILL_T B
                 WHERE B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_READY, 
                                         Pk00_Const.c_BILL_STATE_CLOSED)
                UNION ALL
                -- �������� ����� ����������� �� ������ ��������
                SELECT P.ACCOUNT_ID, 
                       P.REP_PERIOD_ID,
                       P.PAYMENT_DATE BP_DATE,
                       0 BILL_TOTAL,
                       P.RECVD 
                  FROM PAYMENT_T P
             ) BP
           WHERE NOT EXISTS (
            SELECT * FROM INCOMING_BALANCE_T IB
             WHERE BP.ACCOUNT_ID = IB.ACCOUNT_ID
               AND BP.BP_DATE   <= IB.BALANCE_DATE
          )  
        ), BPI AS (
            SELECT ACCOUNT_ID, 
                   REP_PERIOD_ID,
                   BP_DATE,
                   BILL_TOTAL,
                   RECVD,
                   0 INBAL
              FROM BP
            UNION ALL  
            SELECT ACCOUNT_ID, 
                   TO_NUMBER(TO_CHAR(TRUNC(IB.BALANCE_DATE,'mm'),'yyyymm')) REP_PERIOD_ID,
                   IB.BALANCE_DATE BP_DATE,
                   0 BILL_TOTAL,
                   0 RECVD,
                   IB.BALANCE
              FROM INCOMING_BALANCE_T IB   
        )
        SELECT ACCOUNT_ID, SUM(RECVD) - SUM(BILL_TOTAL) + SUM(INBAL) BALANCE, MAX(BP_DATE) BALANCE_DATE 
          FROM BPI
         GROUP BY ACCOUNT_ID
        */          
        WITH BP AS (      
          SELECT *
            FROM (
                -- �������� ������ ������������� �� ������������ ������
                SELECT B.ACCOUNT_ID, 
                       B.REP_PERIOD_ID,
                       B.BILL_DATE BP_DATE,
                       B.TOTAL BILL_TOTAL, 
                       0 RECVD
                  FROM BILL_T B
                 WHERE B.BILL_STATUS IN (Pk00_Const.c_BILL_STATE_READY, 
                                         Pk00_Const.c_BILL_STATE_CLOSED)
                UNION ALL
                -- �������� ����� ����������� �� ������ ��������
                SELECT P.ACCOUNT_ID, 
                       P.REP_PERIOD_ID,
                       P.PAYMENT_DATE BP_DATE,
                       0 BILL_TOTAL,
                       P.RECVD 
                  FROM PAYMENT_T P
             ) BP
           WHERE NOT EXISTS (
            SELECT * FROM INCOMING_BALANCE_T IB
             WHERE BP.ACCOUNT_ID = IB.ACCOUNT_ID
               AND BP.REP_PERIOD_ID < IB.REP_PERIOD_ID
          )  
        )
        SELECT ACCOUNT_ID, SUM(RECVD) - SUM(BILL_TOTAL) BALANCE, MAX(BP_DATE) BALANCE_DATE 
          FROM BP
         GROUP BY ACCOUNT_ID
      ) T
    ON (A.ACCOUNT_ID = T.ACCOUNT_ID)
    WHEN MATCHED THEN UPDATE 
                         SET A.BALANCE_DATE = T.BALANCE_DATE, 
                             A.BALANCE = T.BALANCE;
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

-- ------------------------------------------------------------------------- --
-- ������ �������� ��������, ���� ����������� ������
-- ------------------------------------------------------------------------- --
PROCEDURE Recalc_due_for_all_bills
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Recalc_due_for_all_bills';
    v_count     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ��������� ���� ������ ����������� � ��������
    MERGE INTO BILL_T B
    USING (    
        SELECT PT.BILL_ID, PT.REP_PERIOD_ID, SUM(PT.TRANSFER_TOTAL) RECVD, MAX(PT.TRANSFER_DATE) DUE_DATE
          FROM PAY_TRANSFER_T PT
         GROUP BY PT.BILL_ID, PT.REP_PERIOD_ID
    ) P
    ON (
        B.BILL_ID = P.BILL_ID AND
        B.REP_PERIOD_ID = P.REP_PERIOD_ID
    )
    WHEN MATCHED THEN UPDATE SET B.RECVD = P.RECVD, B.DUE_DATE = P.DUE_DATE;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Bill_t - set recvd for '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- �� ������ ����� ����� ������ �� ������
    UPDATE BILL_T B SET B.DUE = 0, B.ADJUSTED = -B.TOTAL
     WHERE B.BILL_TYPE = 'C';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Bill_t - set due for '||v_count||' credit-note', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- �� ����������������� ������ ���� ������������� ��� (�� ����� ����� �������� ��������)
    UPDATE BILL_T B SET B.DUE = 0, B.ADJUSTED = -B.TOTAL
     WHERE B.NEXT_BILL_ID IS NOT NULL
       AND B.RECVD = 0
       AND B.BILL_TYPE != 'C';
    Pk01_Syslog.Write_msg('Bill_t - set due for '||v_count||' adjusted bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ���������� �����, ��� ��� ��� �������������
    UPDATE BILL_T B SET B.ADJUSTED = 0, B.DUE = B.RECVD - B.TOTAL
     WHERE B.BILL_TYPE = 'B'
       AND B.NEXT_BILL_ID IS NULL;
    Pk01_Syslog.Write_msg('Bill_t - set due for '||v_count||' regular bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

/*
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
*/

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
