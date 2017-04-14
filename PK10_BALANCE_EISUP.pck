CREATE OR REPLACE PACKAGE PK10_BALANCE_EISUP
IS
    --
    -- ����� ��� ��������� ������� �� �����
    -- eisup_payment_t, eisup_pay_transfer_t
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_BALANCE_EISUP';
    -- ==============================================================================
   
    type t_refc is ref cursor;
    
    -- ��������� ������� ��� �������� ��������������� �� �����:
    c_PAYSYSTEM_EISUP_ID CONSTANT INTEGER := 19;
    
    -- ���� ��������
    c_CODE_LOAD_ERR CONSTANT INTEGER := -2;    
    c_CODE_BIND_ERR CONSTANT INTEGER := -1;
    c_CODE_NULL     CONSTANT INTEGER :=  NULL;
    c_CODE_BIND_OK  CONSTANT INTEGER :=  1;
    c_CODE_LOAD_OK  CONSTANT INTEGER :=  2;
    
    -- ������� ������� �������
    c_J_STATUS_OK    CONSTANT VARCHAR2(10) := 'OK';   -- ������� �������� � �������
    c_J_STATUS_NEW   CONSTANT VARCHAR2(10) := 'NEW';  -- ����� � ��������
    c_J_STATUS_SKIP  CONSTANT VARCHAR2(10) := 'SKIP'; -- ��������
    c_J_STATUS_ERR   CONSTANT VARCHAR2(10) := 'ERR';  -- ������ ��� ��������
    c_J_STATUS_OLD   CONSTANT VARCHAR2(10) := 'OLD';  -- ������� �� �������� � �������
    c_J_STATUS_DEL   CONSTANT VARCHAR2(10) := 'DEL';  -- ������ �� ��������


    -- ------------------------------------------------------------------------ --
    -- ������������ ������ ��������� BRM ��� ������������� � �����
    -- ------------------------------------------------------------------------ --
    PROCEDURE Make_brm_contracts_list;
    
    -- ======================================================================== --
    -- �������� ����-������ (������)
    --   - ��� ������ ���������� ����������
    -- ------------------------------------------------------------------------ --
    PROCEDURE BRM_contracts_dup_list (
                   p_recordset    OUT t_refc
               );
    
    -- ======================================================================== --
    -- ������ � ������� ��������
    -- ------------------------------------------------------------------------ --
    PROCEDURE Load_in_balance;
    
    -- ------------------------------------------------------------------------ --
    -- �������� �������� �������� � ������ ��������
    -- ------------------------------------------------------------------------ --
    PROCEDURE Bind_in_balance;
    
    
    -- ------------------------------------------------------------------------ --
    -- ����������� ������� ������ ��� ������� ���������� �������� ������
    PROCEDURE Recalc_for_inBalances;
    
    -- ------------------------------------------------------------------------ --
    -- ��������� �������� ������ � ��������
    -- ��� ��� �����
    -- ------------------------------------------------------------------------ --
    PROCEDURE Set_in_balance(p_report_id IN INTEGER);
    
    -- ------------------------------------------------------------------------ --
    -- ��������� �������� ������ � ��������
    PROCEDURE Set_in_balance;
    
    -- ------------------------------------------------------------------------ --
    -- ������� �������� ������ �� ��������
    PROCEDURE Delete_in_balance;

    -- ------------------------------------------------------------------------ --
    -- ������� ������ � �����
    -- ------------------------------------------------------------------------ --
    PROCEDURE Move_to_archive;

    -- ======================================================================== --
    -- �������� ����-������ (������)
    -- ------------------------------------------------------------------------ --
    -- ������ ����� � �������� ��������� BRM � �������� �������� �� �����
    -- ------------------------------------------------------------------------ --
    PROCEDURE Bind_report (
                   p_recordset    OUT t_refc 
               );

    -- ------------------------------------------------------------------------- --   
    -- ������� ����� � ����������� �������� ���������� ����������� �� �����
    -- ------------------------------------------------------------------------- --
    PROCEDURE Bind_short_summary (
                   p_recordset    OUT t_refc 
               );
    -- ------------------------------------------------------------------------- --
    -- ������� ����� � ��������, ������� ���� � BRM, �� �� ������ � �������� ����� 
    -- ------------------------------------------------------------------------- --
    PROCEDURE Not_found_in_EISUP_summary (
                   p_recordset    OUT t_refc 
               );
    -- ------------------------------------------------------------------------- --
    -- ��������� ����� � ��������, ������� ���� � BRM, �� �� ������ � �������� �����
    -- ------------------------------------------------------------------------- --
    PROCEDURE Not_found_in_EISUP_list (
                   p_recordset    OUT t_refc 
               );

    -- ------------------------------------------------------------------------ --
    -- ������� ����� � ����������� �������� �������� �� �����
    -- ------------------------------------------------------------------------ --
    PROCEDURE Bind_report_summary (
                   p_recordset    OUT t_refc 
               );

    -- ======================================================================== --
    -- ����� � ���������� ������� � ������ �����, 
    -- ������ ��������� ��� ������� ���� � BRM
    -- ------------------------------------------------------------------------ --
    PROCEDURE Dup_report (
                   p_recordset    OUT t_refc 
               );
               
    -- --------------------------------------------------------------------------------------
    -- ������ ��������� � BRM ��� ������� ���� ������������ ����� � 201601 
    -- ------------------------------------------------------------------------ --
    PROCEDURE BRM_contract_not_found (
                   p_recordset    OUT t_refc 
               );

    -- --------------------------------------------------------------------------------------
    -- ������ ��������� � BRM ��� ������� ���� ������������ ����� � 201601 
    -- ------------------------------------------------------------------------ --
    PROCEDURE Other_error_report (
                   p_recordset    OUT t_refc 
               );

    -- ------------------------------------------------------------------------ --
    -- �������� �������
    -- ------------------------------------------------------------------------ --
    CURSOR c_BAL IS (
      SELECT EB.BRM_ACCOUNT_ID, 
             EB.BALANCE, 
             TRUNC(ADD_MONTHS(EB.BALANCE_PERIOD,1),'mm') BALANCE_DATE, 
             EB.BRM_LOAD_STATUS, 
             EB.BRM_LOAD_NOTES, 
             EB.BRM_LOAD_DATE 
        FROM EISUP_BALANCE_T EB
       WHERE EB.BRM_LOAD_STATUS = 2
    )FOR UPDATE;

    -- ========================================================================= --
    -- �������������� ������
    -- ========================================================================= --
    -- ------------------------------------------------------------------------- --
    -- �������� ��������� �� ������� ���� �/�. ������� �� ��������. 
    -- ------------------------------------------------------------------------- --
    PROCEDURE View_simple_contracts (
                   p_recordset    OUT t_refc
               );
           
    -- ------------------------------------------------------------------------- --
    -- �������� �������� �� ��������� ������ 
    -- ------------------------------------------------------------------------- --
    PROCEDURE View_period_info (
                   p_recordset    OUT t_refc,
                   p_period_id    IN  NUMBER,
                   p_account_type IN  VARCHAR2 DEFAULT 'J'
               );
               
    -- ------------------------------------------------------------------------- --
    -- �������� �������� �� �������� �� ��������� ������ 
    -- ------------------------------------------------------------------------- --
    PROCEDURE View_contract_period_info (
                   p_recordset    OUT t_refc,
                   p_period_id    IN  NUMBER,
                   p_contract_id  IN  INTEGER
               );
               
    -- ------------------------------------------------------------------------- --
    -- �������� �������� ��� ���������� �������� �� ������
    -- ------------------------------------------------------------------------- --
    PROCEDURE View_contract_payments (
                   p_recordset    OUT t_refc,
                   p_period_id    IN  INTEGER,
                   p_contract_id  IN  INTEGER
               );

    -- ------------------------------------------------------------------------- --
    -- �������� ������ ��� ���������� �������� �� ������ 
    -- ------------------------------------------------------------------------- --
    PROCEDURE View_contract_bills (
                   p_recordset    OUT t_refc,
                   p_period_id    IN  INTEGER,
                   p_contract_id  IN  INTEGER
               );

    -- ------------------------------------------------------------------------- --
    -- �������� ���������� �� ������ ����������� � TPI �� ������
    -- ------------------------------------------------------------------------- --
    PROCEDURE View_tpi_bills (
                   p_recordset    OUT t_refc,
                   p_period_id    IN  INTEGER
               );

    -- ------------------------------------------------------------------------- --
    -- ������� ��� ������ �������� 
    -- ------------------------------------------------------------------------- --
    PROCEDURE SQL_for_check_payments (
                   p_recordset    OUT t_refc,
                   p_period_id    IN  INTEGER
               );

END PK10_BALANCE_EISUP;
/
CREATE OR REPLACE PACKAGE BODY PK10_BALANCE_EISUP
IS

-- ------------------------------------------------------------------------ --
-- ������������ ������ ��������� BRM ��� ������������� � �����
-- ------------------------------------------------------------------------ --
PROCEDURE Make_brm_contracts_list
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Make_brm_contracts_list';
    v_count       INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg( 'Start', c_PkgName||'.'||v_prcName);
    
    DELETE FROM EISUP_CONTRACTS_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_CONTRACTS: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName);
    
    INSERT INTO EISUP_CONTRACTS_T(
        CONTRACT_NO, CONTRACT_DATE, CONTRACT_ID, 
        ERP_CONTRACT_ID, ERP_CONTRACT_DATE, ERP_CODE, COMPANY_NAME, 
        REP_PERIOD_ID, BILL_NO, BILL_DATE,
        RN
    )
    WITH C AS (
        SELECT C.CONTRACT_NO,
               C.DATE_FROM CONTRACT_DATE,
               CM.CONTRACT_ID, 
               CM.ERP_CONTRACT_ID, 
               CM.DATE_FROM ERP_CONTRACT_DATE, 
               CM.ERP_CODE, CM.COMPANY_NAME  
          FROM COMPANY_T CM, CONTRACT_T C
         WHERE CM.ACTUAL = 'Y'
           AND CM.CONTRACT_ID = C.CONTRACT_ID
           AND EXISTS (
               SELECT * FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A
                WHERE AP.ACCOUNT_ID = A.ACCOUNT_ID
                  AND AP.CONTRACT_ID = C.CONTRACT_ID
                  AND A.BILLING_ID IN (2001,2002)  
                  AND A.ACCOUNT_TYPE = 'J'
                  AND A.STATUS != 'T'
           )
    )
    SELECT CONTRACT_NO, CONTRACT_DATE, CONTRACT_ID, 
           ERP_CONTRACT_ID, ERP_CONTRACT_DATE, ERP_CODE, COMPANY_NAME, 
           REP_PERIOD_ID, BILL_NO, BILL_DATE,
           ROW_NUMBER() OVER (PARTITION BY CONTRACT_NO ORDER BY BILL_DATE DESC ) RN 
      FROM (
        SELECT * 
          FROM (
            SELECT C.CONTRACT_NO, C.CONTRACT_DATE, C.CONTRACT_ID, 
                   C.ERP_CONTRACT_ID, C.ERP_CONTRACT_DATE, C.ERP_CODE, C.COMPANY_NAME, 
                   B.REP_PERIOD_ID, B.BILL_NO, B.BILL_DATE,
                   ROW_NUMBER() OVER (PARTITION BY C.CONTRACT_ID ORDER BY B.BILL_DATE DESC ) RN
              FROM C, BILL_T B
             WHERE C.CONTRACT_ID = B.CONTRACT_ID(+)
          )
         WHERE RN = 1
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_CONTRACTS: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ======================================================================== --
-- �������� ����-������ (������)
--   - ��� ������ ���������� ����������
-- ------------------------------------------------------------------------ --
PROCEDURE BRM_contracts_dup_list (
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BRM_contracts_dup_list';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
      SELECT C.CONTRACT_ID, C.CONTRACT_NO, CM.ERP_CODE, CM.COMPANY_NAME  
        FROM CONTRACT_T C, COMPANY_T CM
       WHERE C.CONTRACT_NO IN (
          SELECT C.CONTRACT_NO 
            FROM CONTRACT_T C
           WHERE EXISTS (
              SELECT * FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A
               WHERE AP.CONTRACT_ID = C.CONTRACT_ID
                 AND AP.ACCOUNT_ID = A.ACCOUNT_ID
                 AND A.ACCOUNT_TYPE = 'J'
                 AND A.BILLING_ID IN (2001,2002)
           )
           GROUP BY C.CONTRACT_NO
           HAVING COUNT(*) > 1
       )
       AND C.CONTRACT_ID = CM.CONTRACT_ID
       AND CM.ACTUAL = 'Y'
      ORDER BY CONTRACT_NO;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ========================================================================== --
-- ������ � �������� ��������
-- ========================================================================== --
PROCEDURE Load_in_balance
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Load_in_balance';
    v_count       INTEGER := 0;
    v_task        INTEGER;
BEGIN
    -- ������ ������
    DELETE FROM EISUP_BALANCE_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName);

    v_task := PK30_BILLING_QUEUE.Open_task;

    -- ����������� ������ �� ��������� �������
    INSERT INTO EISUP_BALANCE_T (
        CONTRACT_ID, CONTRACT_NO, ACCOUNT_NO, ERP_CODE, 
        FACTURE_NUM, EISUP_CODE,
        BALANCE, BALANCE_PERIOD, CREATE_DATE,
        BRM_PERIOD_ID, TASK_ID
    )
    SELECT 
        CONTRACT_ID, TRIM(CONTRACT_NO), TRIM(ACCOUNT_NO), TRIM(ERP_CODE), 
        TRIM(FACTURE_NUM), TRIM(EISUP_CODE),
        BALANCE, BALANCE_PERIOD, CREATE_DATE,
        TO_CHAR(BALANCE_PERIOD+1/86400,'yyyymm'),
        v_task
      FROM EISUP_BALANCE_TMP;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- �������� �������� �������� � ������ ��������
-- ------------------------------------------------------------------------ --
PROCEDURE Bind_in_balance
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Bind_in_balance';
    v_count       INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg( 'START', c_PkgName||'.'||v_prcName);
    --
    -- ����� �������� � BRM �� contract_id + contract_no
    MERGE INTO EISUP_BALANCE_T E
    USING (
      SELECT RID, ERP_CONTRACT_ID, CONTRACT_ID
        FROM (
          SELECT E.ROWID RID, CM.ERP_CONTRACT_ID, C.CONTRACT_ID,
                 ROW_NUMBER() OVER (PARTITION BY CM.ERP_CONTRACT_ID, C.CONTRACT_ID 
                                        ORDER BY CM.DATE_FROM DESC) RN
            FROM CONTRACT_T C, COMPANY_T CM, EISUP_BALANCE_T E  
           WHERE C.CONTRACT_ID  = CM.CONTRACT_ID
             AND E.CONTRACT_ID  = CM.ERP_CONTRACT_ID
             AND E.CONTRACT_NO  = C.CONTRACT_NO
             AND E.BRM_LOAD_STATUS IS NULL
         )
        WHERE RN = 1
    ) EC
    ON (
        E.ROWID = EC.RID
    )
    WHEN MATCHED THEN UPDATE SET E.ERP_CONTRACT_ID = EC.ERP_CONTRACT_ID,
                                 E.BRM_CONTRACT_ID = EC.CONTRACT_ID,
                                 E.BRM_LOAD_STATUS = 1,
                                 E.BRM_LOAD_DATE   = SYSDATE,
                                 E.BRM_LOAD_NOTES  = NVL2(E.BRM_LOAD_NOTES,E.BRM_LOAD_NOTES||' | ', NULL)||'contract_id + contract_no = ok'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.BRM_CONTRACT_NO: '||v_count||' rows find by ID+NO', c_PkgName||'.'||v_prcName);
    --
    -- ����� �������� � BRM �� contract_id 
    MERGE INTO EISUP_BALANCE_T E
    USING (
      SELECT RID, ERP_CONTRACT_ID, CONTRACT_ID
        FROM (
          SELECT E.ROWID RID, CM.ERP_CONTRACT_ID, C.CONTRACT_ID,
                 ROW_NUMBER() OVER (PARTITION BY CM.ERP_CONTRACT_ID 
                                        ORDER BY CM.DATE_FROM DESC) RN
            FROM CONTRACT_T C, COMPANY_T CM, EISUP_BALANCE_T E  
           WHERE C.CONTRACT_ID  = CM.CONTRACT_ID
             AND E.CONTRACT_ID  = CM.ERP_CONTRACT_ID
             AND E.BRM_LOAD_STATUS IS NULL
         )
        WHERE RN = 1
    ) EC
    ON (
        E.ROWID = EC.RID
    )
    WHEN MATCHED THEN UPDATE SET E.ERP_CONTRACT_ID = EC.ERP_CONTRACT_ID,
                                 E.BRM_CONTRACT_ID = EC.CONTRACT_ID,
                                 E.BRM_LOAD_STATUS = 1,
                                 E.BRM_LOAD_DATE   = SYSDATE,
                                 E.BRM_LOAD_NOTES  = NVL2(E.BRM_LOAD_NOTES,E.BRM_LOAD_NOTES||' | ', NULL)||'contract_id = ok'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.BRM_CONTRACT_NO: '||v_count||' rows find by ID', c_PkgName||'.'||v_prcName);
    --
    -- ����� �� ������ ��������
    MERGE INTO EISUP_BALANCE_T E
    USING (
      SELECT RID, ERP_CONTRACT_ID, CONTRACT_ID
        FROM (
          SELECT E.ROWID RID, CM.ERP_CONTRACT_ID, C.CONTRACT_ID,
                 ROW_NUMBER() OVER (PARTITION BY C.CONTRACT_NO 
                                        ORDER BY CM.DATE_FROM DESC) RN
            FROM CONTRACT_T C, COMPANY_T CM, EISUP_BALANCE_T E  
           WHERE C.CONTRACT_ID  = CM.CONTRACT_ID
             AND E.CONTRACT_NO  = C.CONTRACT_NO
             AND E.BRM_LOAD_STATUS IS NULL
         )
        WHERE RN = 1
    ) EC
    ON (
        E.ROWID = EC.RID
    )
    WHEN MATCHED THEN UPDATE SET E.ERP_CONTRACT_ID = EC.ERP_CONTRACT_ID,
                                 E.BRM_CONTRACT_ID = EC.CONTRACT_ID,
                                 E.BRM_LOAD_STATUS = 1,
                                 E.BRM_LOAD_DATE   = SYSDATE,
                                 E.BRM_LOAD_NOTES  = NVL2(E.BRM_LOAD_NOTES,E.BRM_LOAD_NOTES||' | ', NULL)||'contract_id + contract_no = ok'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.BRM_CONTRACT_NO: '||v_count||' rows find by NO', c_PkgName||'.'||v_prcName);
    --
    -- ����������� ����� �������� �� ERP_CODE, ��� ���� �� ����� �� ������ ��������
    MERGE INTO EISUP_BALANCE_T E
    USING (
        WITH EC AS (
            SELECT E.ERP_CODE 
              FROM EISUP_BALANCE_T E
             GROUP BY E.ERP_CODE
             HAVING COUNT(*) = 1
        )
        SELECT E.ROWID RID, E.ERP_CODE, CM.CONTRACT_ID, CM.ERP_CONTRACT_ID 
          FROM EISUP_BALANCE_T E, COMPANY_T CM
         WHERE E.BRM_LOAD_STATUS IS NULL
           AND E.BRM_CONTRACT_ID IS NULL
           AND E.ERP_CODE    = CM.ERP_CODE
           AND CM.DATE_FROM <= E.BALANCE_PERIOD
           AND (CM.DATE_TO IS NULL OR E.BALANCE_PERIOD <= CM.DATE_TO )
           AND EXISTS ( 
              SELECT ERP_CODE FROM EC
               WHERE EC.ERP_CODE = E.ERP_CODE  
           )
           AND NOT EXISTS (
               SELECT * FROM COMPANY_T CMM
                WHERE CMM.CONTRACT_ID != CM.CONTRACT_ID
                  AND E.ERP_CODE    = CMM.ERP_CODE
                  AND CMM.DATE_FROM <= E.BALANCE_PERIOD
                  AND (CMM.DATE_TO IS NULL OR E.BALANCE_PERIOD <= CM.DATE_TO )
           )
           AND NOT EXISTS (
                SELECT *  
                  FROM EISUP_BALANCE_T EB
                 WHERE EB.BRM_CONTRACT_ID = CM.CONTRACT_ID
           )
    ) EE
    ON (
        E.ROWID = EE.RID
    )
    WHEN MATCHED THEN UPDATE SET E.ERP_CONTRACT_ID = EE.ERP_CONTRACT_ID,
                                 E.BRM_CONTRACT_ID = EE.CONTRACT_ID,
                                 E.BRM_LOAD_STATUS = 1,
                                 E.BRM_LOAD_DATE   = SYSDATE,
                                 E.BRM_LOAD_NOTES  = NVL2(E.BRM_LOAD_NOTES,E.BRM_LOAD_NOTES||' | ', NULL)||'erp_code  = ok'
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.BRM_CONTRACT_NO: '||v_count||' rows find by ERP_CODE', c_PkgName||'.'||v_prcName);
    --
    -- ����������� �������� ��������� ID ��������� � �����
    UPDATE EISUP_BALANCE_T E 
       SET E.BRM_LOAD_STATUS = -1,
           E.BRM_LOAD_NOTES  = NVL2(E.BRM_LOAD_NOTES,E.BRM_LOAD_NOTES||' | ', NULL)||'��������� contract_id � �����',
           E.BRM_LOAD_DATE   = SYSDATE
     WHERE E.CONTRACT_ID IN (
        SELECT EE.CONTRACT_ID 
          FROM EISUP_BALANCE_T EE
         GROUP BY EE.CONTRACT_ID
         HAVING COUNT(*) > 1
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.CONTRACT_ID: '||v_count||' rows duplicated', c_PkgName||'.'||v_prcName); 
    --
    -- ����������� �������� ��������� ������� ��������� � �����
    UPDATE EISUP_BALANCE_T E 
       SET E.BRM_LOAD_STATUS = -1,
           E.BRM_LOAD_NOTES  = NVL2(E.BRM_LOAD_NOTES,E.BRM_LOAD_NOTES||' | ', NULL)||'��������� contract_no � �����',
           E.BRM_LOAD_DATE   = SYSDATE
     WHERE E.CONTRACT_NO IN (
        SELECT EE.CONTRACT_NO 
          FROM EISUP_BALANCE_T EE
         GROUP BY EE.CONTRACT_NO
         HAVING COUNT(*) > 1
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.CONTRACT_NO: '||v_count||' rows duplicated in EISUP', c_PkgName||'.'||v_prcName); 
    --
    -- ������ �������, ������ ��������� ��������� ������� ���� � BRM
    UPDATE EISUP_BALANCE_T E 
       SET E.BRM_LOAD_NOTES  = NVL2(E.BRM_LOAD_NOTES,E.BRM_LOAD_NOTES||' | ', NULL)||'contract_no ���� � BRM'
     WHERE E.BRM_LOAD_STATUS = -1
       AND EXISTS (
           SELECT * FROM CONTRACT_T C
            WHERE E.CONTRACT_NO = C.CONTRACT_NO
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.CONTRACT_NO: '||v_count||' error rows exists in BRM', c_PkgName||'.'||v_prcName); 
    --
    -- ����������� �������� ��������� ������� ��������� � BRM
    UPDATE EISUP_BALANCE_T E 
       SET E.BRM_LOAD_STATUS = -1,
           E.BRM_LOAD_NOTES  = NVL2(E.BRM_LOAD_NOTES,E.BRM_LOAD_NOTES||' | ', NULL)||'��������� contract_no � BRM'
     WHERE E.CONTRACT_NO IN (
        SELECT C.CONTRACT_NO 
          FROM CONTRACT_T C
         GROUP BY C.CONTRACT_NO
         HAVING COUNT(*) > 1
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.CONTRACT_NO: '||v_count||' rows duplicated in BRM', c_PkgName||'.'||v_prcName); 
    --
    -- �������� ��������, ������� ��� � BRM, �� ���� � �����
    UPDATE EISUP_BALANCE_T E 
       SET E.BRM_LOAD_STATUS = -1,
           E.BRM_LOAD_NOTES  = 'contract_no �� ������ BRM',
           E.BRM_LOAD_DATE   = SYSDATE
     WHERE E.BRM_LOAD_STATUS IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.CONTRACT_NO: '||v_count||' rows not find in BRM', c_PkgName||'.'||v_prcName);

    -- ---------------------------------------------------------------------- --
    -- ���� �� �������� �� ������� (��� �� �� �������� ����������)
    -- ---------------------------------------------------------------------- --
    -- ����������� ����� ����� � ������ BRM
    MERGE INTO EISUP_BALANCE_T E
     USING (
        SELECT E.FACTURE_NUM, B.BILL_ID  
          FROM EISUP_BALANCE_T E, BILL_T B
         WHERE E.BRM_LOAD_STATUS = 1
           AND E.FACTURE_NUM = B.BILL_NO
     ) EB
     ON (
        E.FACTURE_NUM = EB.FACTURE_NUM
     )
     WHEN MATCHED THEN UPDATE SET E.BRM_BILL_ID = EB.BILL_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.BRM_BILL_ID: '||v_count||' rows found', c_PkgName||'.'||v_prcName);
    /*
    -- ����������� ������� �������������� ������ �����, ������ ��������
    UPDATE EISUP_BALANCE_T E 
       SET E.BRM_LOAD_NOTES  = NVL2(E.BRM_LOAD_NOTES,E.BRM_LOAD_NOTES||' | ', NULL)||'�����.bill_no != BRM.bill_no'
     WHERE E.BRM_LOAD_STATUS = 1
       AND E.BRM_BILL_ID IS NOT NULL
       AND NOT EXISTS (
          SELECT * FROM BILL_T B
           WHERE E.BRM_BILL_ID     = B.BILL_ID
             AND E.BRM_CONTRACT_ID = B.CONTRACT_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.BRM_BILL_ID: '||v_count||' rows bind error', c_PkgName||'.'||v_prcName);
    */
    -- ��������� �� ������� �����
    UPDATE EISUP_BALANCE_T E
       SET E.BRM_LOAD_STATUS = 2
      WHERE E.BRM_LOAD_STATUS = 1;

    -- ---------------------------- LOAD_STATUS = 2 -------------------------- --
    -- ����������� ����� �/� ��� ��� ��������
    MERGE INTO EISUP_BALANCE_T E
    USING (
      SELECT RID, BRM_CONTRACT_ID, ACCOUNT_ID
        FROM (
        SELECT E.ROWID RID, BRM_CONTRACT_ID, AP.ACCOUNT_ID,
               ROW_NUMBER() OVER (PARTITION BY BRM_CONTRACT_ID ORDER BY AP.ACCOUNT_ID) RN 
          FROM EISUP_BALANCE_T E, ACCOUNT_PROFILE_T AP
         WHERE E.BRM_LOAD_STATUS = 2
           AND E.BRM_CONTRACT_ID = AP.CONTRACT_ID
           AND AP.DATE_FROM <= E.BALANCE_PERIOD
           AND (AP.DATE_TO IS NULL OR E.BALANCE_PERIOD <= AP.DATE_TO )
        )
       WHERE RN = 1
    ) EP
    ON (
        E.ROWID = EP.RID
    )
    WHEN MATCHED THEN UPDATE SET E.BRM_ACCOUNT_ID = EP.ACCOUNT_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.ACCOUNT_ID: '||v_count||' rows found', c_PkgName||'.'||v_prcName);

    -- ����������� �������, ��� �/� �� ������������ �� ��������
    MERGE INTO EISUP_BALANCE_T E
    USING (
        SELECT BRM_CONTRACT_ID, COUNT(*)+1 RN 
          FROM EISUP_BALANCE_T E, ACCOUNT_PROFILE_T AP
         WHERE E.BRM_LOAD_STATUS  = 2
           AND E.BRM_CONTRACT_ID  = AP.CONTRACT_ID
           AND E.BRM_ACCOUNT_ID  != AP.ACCOUNT_ID
           AND AP.DATE_FROM <= E.BALANCE_PERIOD
           AND (AP.DATE_TO IS NULL OR E.BALANCE_PERIOD <= AP.DATE_TO )
         GROUP BY BRM_CONTRACT_ID
    ) EP
    ON (
        E.BRM_CONTRACT_ID = EP.BRM_CONTRACT_ID AND 
        E.BRM_LOAD_STATUS = 2
    )
    WHEN MATCHED THEN UPDATE 
                         SET E.BRM_LOAD_NOTES = NVL2(E.BRM_LOAD_NOTES,E.BRM_LOAD_NOTES||' | ', NULL)||
                             '�� �������� > 1 �/�',
                             E.BRM_ACCOUNTS_NUM = EP.RN;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.ACCOUNT_ID: '||v_count||' rows not unique', c_PkgName||'.'||v_prcName);

    -- ����������� �������, ��� �/� ������������ �� ��������
    UPDATE EISUP_BALANCE_T E 
       SET E.BRM_ACCOUNTS_NUM = 1 
     WHERE E.BRM_LOAD_STATUS  = 2
       AND E.BRM_ACCOUNT_ID IS NOT NULL
       AND E.BRM_ACCOUNTS_NUM IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.ACCOUNT_ID: '||v_count||' rows unique', c_PkgName||'.'||v_prcName);

    -- ------------------------------------------------------------------------- --
    -- ����������� �������, ���� ��� �� �������� ��� �������� ��������
    UPDATE EISUP_BALANCE_T E 
       SET E.BRM_LOAD_STATUS = -2,
           E.BRM_LOAD_NOTES  = NVL2(E.BRM_LOAD_NOTES,E.BRM_LOAD_NOTES||' | ', NULL)||'�� ������ �������� �/� �� �������� � BRM',
           E.BRM_LOAD_DATE   = SYSDATE
     WHERE E.BRM_LOAD_STATUS  = 2
       AND E.BRM_CONTRACT_ID IS NOT NULL
       AND E.BRM_ACCOUNT_ID  IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'BRM.ACCOUNT_PROFILE: '||v_count||' rows not found active ', c_PkgName||'.'||v_prcName);

    -- ------------------------------------------------------------------------- --
    -- ����������� billing_id
    MERGE INTO EISUP_BALANCE_T E
    USING ( 
        SELECT E.BRM_ACCOUNT_ID, A.BILLING_ID
          FROM EISUP_BALANCE_T E, ACCOUNT_T A
         WHERE E.BRM_ACCOUNT_ID = A.ACCOUNT_ID
           AND E.BRM_ACCOUNT_ID IS NOT NULL
    ) EA
    ON (
        E.BRM_ACCOUNT_ID = EA.BRM_ACCOUNT_ID
    )
    WHEN MATCHED THEN UPDATE SET E.BRM_BILLING_ID = EA.BILLING_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.BILLING_ID: '||v_count||' rows set', c_PkgName||'.'||v_prcName);

    -- ------------------------------------------------------------------------- --
    -- ��������� �������, ��� �������� 2003 ��������
    UPDATE EISUP_BALANCE_T E
       SET E.BRM_LOAD_STATUS= -2,
           E.BRM_LOAD_NOTES = NVL2(E.BRM_LOAD_NOTES,E.BRM_LOAD_NOTES||' | ', NULL)||'billing_id = 2003 - ������ ������� � BRM',
           E.BRM_LOAD_DATE  = SYSDATE
     WHERE E.BRM_BILLING_ID = 2003
       AND E.BRM_LOAD_STATUS= 2;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.BILLING_ID=2003: '||v_count||' rows', c_PkgName||'.'||v_prcName);

    -- ------------------------------------------------------------------------- --
    -- �������� �� ������������ ������������ ����� � BRM
    MERGE INTO EISUP_BALANCE_T E
     USING (
        SELECT E.ROWID RID, E.BRM_CONTRACT_ID, CM.COMPANY_ID, CM.ERP_CODE 
          FROM EISUP_BALANCE_T E, COMPANY_T CM
         WHERE E.ERP_CONTRACT_ID = CM.ERP_CONTRACT_ID
           AND CM.DATE_FROM <= E.BALANCE_PERIOD
           AND (CM.DATE_TO IS NULL OR E.BALANCE_PERIOD <= CM.DATE_TO )
           AND E.BRM_LOAD_STATUS = 2
    ) EC
    ON (
        E.ROWID = EC.RID
    )
    WHEN MATCHED THEN UPDATE 
                         SET E.BRM_COMPANY_ID = EC.COMPANY_ID,
                             E.BRM_ERP_CODE   = EC.ERP_CODE;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.ERP_CODE: '||v_count||' rows ok', c_PkgName||'.'||v_prcName);

    -- ��������� ������������� ERP_CODE � ����� � BRM                             
    UPDATE EISUP_BALANCE_T E 
       SET --E.BRM_LOAD_STATUS= -2,     -- ������ ��������������
           E.BRM_LOAD_NOTES = NVL2(E.BRM_LOAD_NOTES,E.BRM_LOAD_NOTES||' | ', NULL)||' �����.ERP_CODE != BRM.ERP_CODE',
           E.BRM_LOAD_DATE  = SYSDATE
     WHERE E.ERP_CODE      != E.BRM_ERP_CODE
       AND E.BRM_LOAD_STATUS= 2;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T.ERP_CODE: '||v_count||' rows error', c_PkgName||'.'||v_prcName);

    Pk01_Syslog.Write_msg( 'STOP', c_PkgName||'.'||v_prcName);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- ------------------------------------------------------------------------ --
-- ����������� ������� ������ ��� ������� ���������� �������� ������
-- ------------------------------------------------------------------------ --
PROCEDURE Recalc_for_inBalances
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Recalc_for_inBalances';
    v_count       INTEGER := 0;
BEGIN
    -- ������������� ������� �/�
    MERGE INTO ACCOUNT_T A
    USING   
       (
       SELECT ACCOUNT_ID, SUM(RECVD-BILL_TOTAL) BALANCE,
               CASE
                   WHEN MAX(BILL_DATE) > MAX(PAYMENT_DATE) THEN MAX(BILL_DATE)
                   ELSE MAX(PAYMENT_DATE)
               END BALANCE_DATE 
        FROM (
            -- �������� ������ ������������� �� ������������ ������
            SELECT B.ACCOUNT_ID, 
                   B.TOTAL BILL_TOTAL, BILL_DATE, 
                   0 RECVD, TO_DATE('01.01.2000','dd.mm.yyyy') PAYMENT_DATE 
              FROM BILL_T B, INCOMING_BALANCE_T IB
             WHERE B.ACCOUNT_ID    = IB.ACCOUNT_ID
               AND B.BILL_DATE     > IB.BALANCE_DATE
               AND B.REP_PERIOD_ID > IB.REP_PERIOD_ID
            UNION ALL
            -- �������� ����� ����������� �� ������ ��������
            SELECT P.ACCOUNT_ID, 
                   0 BILL_TOTAL, TO_DATE('01.01.2000','dd.mm.yyyy') BILL_DATE,
                   P.RECVD, P.PAYMENT_DATE  
              FROM PAYMENT_T P, INCOMING_BALANCE_T IB
             WHERE P.ACCOUNT_ID    = IB.ACCOUNT_ID
               AND P.PAYMENT_DATE  > IB.BALANCE_DATE
               AND P.REP_PERIOD_ID > IB.REP_PERIOD_ID
            UNION ALL
            -- ��������� �������� ������
            SELECT IB.BALANCE,
                   IB.BALANCE BILL_TOTAL, IB.BALANCE_DATE BILL_DATE, 
                   0 RECVD, TO_DATE('01.01.2000','dd.mm.yyyy') PAYMENT_DATE
              FROM INCOMING_BALANCE_T IB
        )
        GROUP BY ACCOUNT_ID
    ) T
    ON (
       A.ACCOUNT_ID = T.ACCOUNT_ID
    )
    WHEN MATCHED THEN UPDATE SET A.BALANCE_DATE = T.BALANCE_DATE, A.BALANCE = T.BALANCE;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Updated '||v_count||' rows in ACCOUNT_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- ��������� �������� ������ � ��������
-- ��� ��� �����
-- ------------------------------------------------------------------------ --
PROCEDURE Set_in_balance(p_report_id IN INTEGER)
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Set_in_balance';
    v_count           INTEGER := 0;
    v_error           INTEGER := 0;
    v_ok              INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    FOR r_bl IN (
      SELECT A.ACCOUNT_ID BRM_ACCOUNT_ID, RP.C1_BALANCE_OUT BALANCE,
             ADD_MONTHS(TO_DATE(RP.REP_PERIOD_ID,'yyyymm'),1) BALANCE_DATE 
        FROM EISUP_RP4 RP, ACCOUNT_PROFILE_T AP, ACCOUNT_T A
       WHERE RP.BRM_CONTRACT_ID = AP.CONTRACT_ID
         AND AP.ACTUAL     = 'Y'
         AND RP.REPORT_ID  = p_report_id
         AND AP.ACCOUNT_ID = A.ACCOUNT_ID
         AND A.BILLING_ID != 2003
      ) 
    LOOP
      BEGIN
          -- ��������� ������������� ��������� �������
          SELECT COUNT(*) INTO v_count 
            FROM INCOMING_BALANCE_T IB
           WHERE IB.ACCOUNT_ID = r_bl.brm_account_id;

          IF v_count = 1 THEN
              -- ������� ������ � �������� ������� � ������������� ����� � �������
              PK05_ACCOUNT_BALANCE.Delete_incomming_balance( r_bl.brm_account_id );
          END IF;
          -- ��������� ����� ������
          PK05_ACCOUNT_BALANCE.Set_incomming_balance (
                                 p_account_id   => r_bl.brm_account_id,
                                 p_balance      => r_bl.balance,     -- �������� ������ �� ������ ����� ������ 00:00:00
                                 p_balance_date => r_bl.balance_date -- ������ ����� ������ 00:00:00 � ������� ����� � ��������
                             );
          --
          v_ok := v_ok + 1;
          --
      EXCEPTION
         -- -------------------------------------------------------- --
         -- ��������� ������ �������� ������ 
         WHEN OTHERS THEN
            Pk01_Syslog.Write_msg('brm_account_id='||r_bl.BRM_ACCOUNT_ID||
                                ' => ������ ��� ��������� ��������� �������'
                                , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );            

            v_error := v_error + 1;
      END;

      -- �����
      IF MOD(v_ok + v_error, 100) = 0 THEN
          Pk01_Syslog.Write_msg((v_ok + v_error)||' rows processed', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      END IF;

    END LOOP;
    
    Pk01_Syslog.Write_msg('Report: '||v_ok||' - OK, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    -- - - - - - - - - - - - - - - -- - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--============================================================================================
-- �������� �������� �������� �� ��������� � ������� ������ ����
--============================================================================================
PROCEDURE Set_in_balance
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'Set_in_balance';
    v_count           INTEGER := 0;
    v_error           INTEGER := 0;
    v_ok              INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    FOR r_bl IN c_BAL LOOP
      BEGIN
          -- ��������� ������������� ��������� �������
          SELECT COUNT(*) INTO v_count 
            FROM INCOMING_BALANCE_T IB
           WHERE IB.ACCOUNT_ID = r_bl.brm_account_id;

          IF v_count = 1 THEN
              -- ������� ������ � �������� ������� � ������������� ����� � �������
              PK05_ACCOUNT_BALANCE.Delete_incomming_balance( r_bl.brm_account_id );
          END IF;
          -- ��������� ����� ������
          PK05_ACCOUNT_BALANCE.Set_incomming_balance (
                                 p_account_id   => r_bl.brm_account_id,
                                 p_balance      => r_bl.balance,     -- �������� ������ �� ������ ����� ������ 00:00:00
                                 p_balance_date => r_bl.balance_date -- ������ ����� ������ 00:00:00 � ������� ����� � ��������
                             );

          -- ��������� ���������
          UPDATE EISUP_BALANCE_T EB
             SET BRM_LOAD_STATUS = 3,
                 BRM_LOAD_NOTES  = NVL2(EB.BRM_LOAD_NOTES,EB.BRM_LOAD_NOTES||' | ', NULL)|| 'OK',
                 BRM_LOAD_DATE   = SYSDATE
           WHERE CURRENT OF c_BAL;
          --
          v_ok := v_ok + 1;
          --
      EXCEPTION
         -- -------------------------------------------------------- --
         -- ��������� ������ �������� ������ 
         WHEN OTHERS THEN
            UPDATE EISUP_BALANCE_T EB
               SET BRM_LOAD_STATUS = -3,
                   BRM_LOAD_NOTES  = NVL2(EB.BRM_LOAD_NOTES,EB.BRM_LOAD_NOTES||' | ', NULL)|| '������ ��������� ��.�������',
                   BRM_LOAD_DATE   = SYSDATE
             WHERE CURRENT OF c_BAL;
            Pk01_Syslog.Write_msg('brm_account_id='||r_bl.BRM_ACCOUNT_ID||
                                ' => ������ ��� ��������� ��������� �������'
                                , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );            

            v_error := v_error + 1;
      END;

      -- �����
      IF MOD(v_ok + v_error, 100) = 0 THEN
          Pk01_Syslog.Write_msg((v_ok + v_error)||' rows processed', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      END IF;

    END LOOP;
    
    Pk01_Syslog.Write_msg('Report: '||v_ok||' - OK, '||v_error||' - err', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - -- - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR:'||v_count||' row', c_PkgName||'.'||v_prcName );
END;

/*
PROCEDURE Set_in_balance
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Set_in_balance';
    v_count       INTEGER := 0;
BEGIN
  
    FOR rb IN (
    )


    -- ��������� ������� � ��������� ��������� �/�
    MERGE INTO INCOMING_BALANCE_T IB
    USING (
        SELECT EB.BRM_ACCOUNT_ID, EB.BALANCE, EB.BALANCE_PERIOD
          FROM EISUP_BALANCE_T EB 
         WHERE EB.BRM_BILLING_ID != 2003
           AND EB.BRM_ACCOUNT_ID IS NOT NULL
           AND EB.BRM_LOAD_STATUS = c_J_STATUS_OK
    ) EB
    ON(
        IB.ACCOUNT_ID = EB.BRM_ACCOUNT_ID
    )
    WHEN MATCHED THEN UPDATE SET IB.BALANCE = EB.BALANCE, 
                                 IB.BALANCE_DATE = EB.BALANCE_PERIOD,
                                 IB.REP_PERIOD_ID = TO_CHAR(EB.BALANCE_PERIOD ,'yyyymm')
    WHEN NOT MATCHED THEN INSERT (ACCOUNT_ID, BALANCE, BALANCE_DATE, REP_PERIOD_ID)
                          VALUES (EB.BRM_ACCOUNT_ID, EB.BALANCE, EB.BALANCE_PERIOD,
                                 TO_NUMBER(TO_CHAR(EB.BALANCE_PERIOD ,'yyyymm'))
                                 )    
    ;
    
    
    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'INCOMING_BALANCE_T: '||v_count||' - rows inserted', c_PkgName||'.'||v_prcName);

    -- ------------------------------------------------------------------------ --    
    -- ����������� ������� ������ ��� ������� ���������� �������� ������
    Recalc_for_inBalances;
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
*/

-- ------------------------------------------------------------------------ --
-- ������� �������� ������ �� ��������
-- ------------------------------------------------------------------------ --
PROCEDURE Delete_in_balance
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_in_balance';
    v_count       INTEGER := 0;
BEGIN
    -- ������� �������� ������� ������������ �����������  
    FOR ib IN (
      SELECT EB.BRM_ACCOUNT_ID
        FROM EISUP_BALANCE_T EB
       WHERE EB.BRM_LOAD_STATUS = 3
    ) 
    LOOP
      PK05_ACCOUNT_BALANCE.Delete_incomming_balance( ib.brm_account_id );
      -- ��������� �������
      UPDATE EISUP_BALANCE_T EB 
         SET EB.BRM_LOAD_STATUS = 2
       WHERE EB.BRM_LOAD_STATUS = 3
         AND EB.BRM_ACCOUNT_ID = ib.brm_account_id;
    END LOOP;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'INCOMING_BALANCE_T: '||v_count||' - rows deleted', c_PkgName||'.'||v_prcName);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- ������� ������ � �����
-- ------------------------------------------------------------------------ --
PROCEDURE Move_to_archive
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Move_to_archive';
    v_count       INTEGER := 0;
BEGIN
    INSERT INTO EISUP_BALANCE_T_ARX (
           CONTRACT_ID, CONTRACT_NO, ACCOUNT_NO, ERP_CODE, FACTURE_NUM, EISUP_CODE, 
           BALANCE, BALANCE_PERIOD, CREATE_DATE, 
           BRM_CONTRACT_ID, BRM_ACCOUNT_ID, BRM_COMPANY_ID, BRM_LOAD_STATUS, 
           BRM_LOAD_NOTES, BRM_LOAD_DATE, BRM_BILLING_ID, 
           BRM_PERIOD_ID, BRM_BILL_ID, BRM_PAYMENT_ID, 
           BRM_ACCOUNTS_NUM, BRM_ERP_CODE, TASK_ID
    )
    SELECT CONTRACT_ID, CONTRACT_NO, ACCOUNT_NO, ERP_CODE, FACTURE_NUM, EISUP_CODE, 
           BALANCE, BALANCE_PERIOD, CREATE_DATE, 
           BRM_CONTRACT_ID, BRM_ACCOUNT_ID, BRM_COMPANY_ID, BRM_LOAD_STATUS, 
           BRM_LOAD_NOTES, BRM_LOAD_DATE, BRM_BILLING_ID, 
           BRM_PERIOD_ID, BRM_BILL_ID, BRM_PAYMENT_ID, 
           BRM_ACCOUNTS_NUM, BRM_ERP_CODE, TASK_ID 
      FROM EISUP_BALANCE_T;
    --  
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T_ARX: '||v_count||' - rows inserted', c_PkgName||'.'||v_prcName);
    --    
    DELETE FROM EISUP_BALANCE_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T: '||v_count||' - rows deleted', c_PkgName||'.'||v_prcName);
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ========================================================================== --
--                        ������
-- ======================================================================== --
-- ������ ����� � �������� ��������� BRM � �������� �������� �� �����
-- ------------------------------------------------------------------------ --
PROCEDURE Bind_report (
               p_recordset    OUT t_refc 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bind_report';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
      SELECT BC.CONTRACT_NO BRM_CONTRACT_NO,
             BC.CONTRACT_DATE BRM_CONTRACT_DATE, 
             BC.ERP_CODE BRM_ERP_CODE, 
             BC.COMPANY_NAME BRM_COMPANY_NAME, 
             BC.REP_PERIOD_ID, 
             BC.BILL_NO,
             BC.BILL_DATE,
             E.CONTRACT_ID EISUP_CONTRACT_ID, E.CONTRACT_NO EISUP_CONTRACT_NO, E.ERP_CODE EISUP_ERP_CODE, 
             E.FACTURE_NUM, E.BALANCE, E.BALANCE_PERIOD, 
             E.BRM_CONTRACT_ID, E.BRM_ACCOUNT_ID, E.BRM_ACCOUNTS_NUM, 
             E.BRM_COMPANY_ID, E.BRM_LOAD_STATUS, E.BRM_LOAD_NOTES
        FROM EISUP_CONTRACTS_T BC FULL OUTER JOIN EISUP_BALANCE_T E
          ON BC.CONTRACT_ID = E.BRM_CONTRACT_ID AND BC.RN = 1  
      ORDER BY NVL(BC.CONTRACT_NO, E.CONTRACT_NO)
      --ORDER BY BC.REP_PERIOD_ID DESC NULLS LAST, 
      --         BC.BILL_NO, BC.CONTRACT_NO, E.BRM_CONTRACT_ID
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- ######################################################################### --
-- ------------------------------------------------------------------------- --   
-- ������� ����� � ����������� �������� ���������� ����������� �� �����
-- ------------------------------------------------------------------------- --
PROCEDURE Bind_short_summary (
               p_recordset    OUT t_refc 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bind_short_summary';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
        SELECT BRM_LOAD_STATUS, BRM_LOAD_NOTES, COUNT(*) CNT
          FROM EISUP_BALANCE_T E
         GROUP BY BRM_LOAD_STATUS, BRM_LOAD_NOTES
         ORDER BY BRM_LOAD_STATUS, BRM_LOAD_NOTES
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
-- ������� ����� � ��������, ������� ���� � BRM, �� �� ������ � �������� ����� 
-- ------------------------------------------------------------------------- --
PROCEDURE Make_not_found_report
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_not_found_report';
    v_count      INTEGER;
BEGIN
    --  
    DELETE FROM EISUP_NOT_FOUND_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_NOT_FOUND_T: '||v_count||' - rows deleted', c_PkgName||'.'||v_prcName);
    --
    INSERT INTO EISUP_NOT_FOUND_T
    SELECT * FROM (
          WITH CB AS (
              SELECT C.ERP_CONTRACT_ID, C.CONTRACT_ID, 
                     C.CONTRACT_NO, C.CONTRACT_DATE, C.ERP_CODE, C.COMPANY_NAME, 
                     MAX(B.REP_PERIOD_ID) MAX_PERIOD_ID,
                     MAX(B.BILL_NO) MAX_BILL_NO
                FROM EISUP_CONTRACTS_T C, BILL_T B
               WHERE NOT EXISTS (
                  SELECT * FROM EISUP_BALANCE_T E
                   WHERE C.CONTRACT_ID = E.BRM_CONTRACT_ID
                ) 
                AND C.CONTRACT_ID = B.CONTRACT_ID(+)
              GROUP BY C.ERP_CONTRACT_ID, C.CONTRACT_ID, 
                    C.CONTRACT_NO, C.CONTRACT_DATE, C.ERP_CODE, C.COMPANY_NAME
          )
          SELECT * FROM (
          SELECT ERP_CONTRACT_ID, CONTRACT_ID, CONTRACT_NO, CONTRACT_DATE, 
                 ERP_CODE, COMPANY_NAME, MAX_BILL_NO,
                 CASE
                  WHEN MAX_PERIOD_ID IS NULL   THEN '3. ��� � �����, ���� � BRM, �� ��� ������������ ������' 
                  WHEN MAX_PERIOD_ID >= 201601 THEN '1. ��� � �����, ���� � BRM � ���� ����� ������������ � 2016 ����'
                  WHEN MAX_PERIOD_ID < 201601  THEN '2. ��� � �����, ���� � BRM � ���� ����� ������������ ����� 2016 ����' 
                 END BILL_EXISTS
            FROM CB
           )
           WHERE SUBSTR(BILL_EXISTS,1,1) < 3
           ORDER BY BILL_EXISTS, CONTRACT_NO
    );  
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_NOT_FOUND_T: '||v_count||' - rows inserted', c_PkgName||'.'||v_prcName);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE Not_found_in_EISUP_summary (
               p_recordset    OUT t_refc 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Contract_not_found_in_EISUP';
    v_retcode    INTEGER;
BEGIN
    --  
    Make_not_found_report;
    --
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
      WITH CB AS (
          SELECT NVL(EC.ERP_CONTRACT_ID, EC.CONTRACT_ID) ERP_CONTRACT_ID,
                 EC.CONTRACT_NO, EC.CONTRACT_DATE, EC.ERP_CODE, EC.COMPANY_NAME, MAX(B.REP_PERIOD_ID)  MAX_PERIOD_ID
            FROM EISUP_CONTRACTS_T EC, BILL_T B
           WHERE NOT EXISTS (
              SELECT * FROM EISUP_BALANCE_T E
               WHERE 1=1 
                 AND EC.ERP_CONTRACT_ID = E.BRM_CONTRACT_ID
            ) 
            AND EC.CONTRACT_ID = B.CONTRACT_ID(+)
          GROUP BY NVL(EC.ERP_CONTRACT_ID, EC.CONTRACT_ID), 
                EC.CONTRACT_NO, EC.CONTRACT_DATE, EC.ERP_CODE, EC.COMPANY_NAME
      )
      SELECT CASE
              WHEN MAX_PERIOD_ID IS NULL   THEN '3. ��� � �����, ���� � BRM, �� ��� ������������ ������' 
              WHEN MAX_PERIOD_ID >= 201601 THEN '1. ��� � �����, ���� � BRM � ���� ����� ������������ � 2016 ����'
              WHEN MAX_PERIOD_ID < 201601  THEN '2. ��� � �����, ���� � BRM � ���� ����� ������������ ����� 2016 ����' 
             END BILL_EXISTS,
             COUNT(*) CNT  
        FROM CB
       GROUP BY 
             CASE
              WHEN MAX_PERIOD_ID IS NULL   THEN '3. ��� � �����, ���� � BRM, �� ��� ������������ ������' 
              WHEN MAX_PERIOD_ID >= 201601 THEN '1. ��� � �����, ���� � BRM � ���� ����� ������������ � 2016 ����'
              WHEN MAX_PERIOD_ID < 201601  THEN '2. ��� � �����, ���� � BRM � ���� ����� ������������ ����� 2016 ����' 
             END 
       ORDER BY 1
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
-- ��������� ����� � ��������, ������� ���� � BRM, �� �� ������ � �������� �����
-- ------------------------------------------------------------------------- --
PROCEDURE Not_found_in_EISUP_list (
               p_recordset    OUT t_refc 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Contract_not_found_in_EISUP';
    v_retcode    INTEGER;
BEGIN
    --  
    Make_not_found_report;
    --
    OPEN p_recordset FOR
      SELECT NF.BILL_EXISTS, NF.COMPANY_NAME, 
             NF.ERP_CODE BRM_ERP_CODE, E.ERP_CODE, NF.MAX_BILL_NO,  
             NF.CONTRACT_NO BRM_CONTRACT_NO, E.CONTRACT_NO EISUP_CONTRACT_NO, 
             NF.CONTRACT_ID BRM_CONTRACT_ID, E.CONTRACT_ID EISUP_CONTRACT_ID,
             E.BALANCE, E.BRM_LOAD_STATUS, E.BRM_LOAD_NOTES 
        FROM EISUP_NOT_FOUND_T NF, EISUP_BALANCE_T E
       WHERE NF.CONTRACT_NO = E.CONTRACT_NO(+)
         AND E.BRM_LOAD_NOTES(+) NOT LIKE '%��������� contract_no � BRM%'
         AND E.BRM_LOAD_STATUS IS NOT NULL  
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- ������� ����� � ����������� �������� �������� �� �����
-- ------------------------------------------------------------------------ --
PROCEDURE Bind_report_summary (
               p_recordset    OUT t_refc 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bind_report_summary';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
    WITH R AS (    
        SELECT BC.CONTRACT_NO BRM_CONTRACT_NO,
               BC.CONTRACT_DATE BRM_CONTRACT_DATE, 
               BC.ERP_CODE BRM_ERP_CODE, 
               BC.COMPANY_NAME BRM_COMPANY_NAME, 
               BC.REP_PERIOD_ID, 
               BC.BILL_NO,
               BC.BILL_DATE,
               E.CONTRACT_ID EISUP_CONTRACT_ID, E.CONTRACT_NO EISUP_CONTRACT_NO, E.ERP_CODE EISUP_ERP_CODE, 
               E.FACTURE_NUM, E.BALANCE, E.BALANCE_PERIOD, 
               E.BRM_CONTRACT_ID, E.BRM_ACCOUNT_ID, E.BRM_ACCOUNTS_NUM, 
               E.BRM_COMPANY_ID, E.BRM_LOAD_STATUS, E.BRM_LOAD_NOTES
          FROM EISUP_CONTRACTS_T BC FULL OUTER JOIN EISUP_BALANCE_T E
            ON BC.CONTRACT_ID = E.BRM_CONTRACT_ID AND BC.RN = 1  
        ORDER BY BC.REP_PERIOD_ID DESC NULLS LAST, BC.BILL_NO, BC.CONTRACT_NO, E.BRM_CONTRACT_ID
    ) 
    SELECT 
        CASE
            WHEN BRM_CONTRACT_NO IS NOT NULL AND EISUP_CONTRACT_NO IS NULL AND REP_PERIOD_ID > 201601 THEN '01. ���� �������� � BRM, � 2016 ���� ���� ������������ �����, �� ��� � �������� �����'
            WHEN BRM_CONTRACT_NO IS NOT NULL AND EISUP_CONTRACT_NO IS NULL AND REP_PERIOD_ID IS NOT NULL THEN '02. ���� �������� � BRM �� 2016 ���� ������������ �����, �� ��� � �������� �����'
            WHEN BRM_CONTRACT_NO IS NOT NULL AND EISUP_CONTRACT_NO IS NULL THEN '03. ���� �������� � BRM, ������������ ������ ��� (�������� �� �����������), �� ��� � �������� �����'
            WHEN BRM_CONTRACT_NO IS NOT NULL AND BRM_LOAD_STATUS  = 2 THEN '04. OK'
            WHEN BRM_CONTRACT_NO IS NULL AND BRM_LOAD_STATUS = -2 AND BRM_LOAD_NOTES LIKE '%billing_id = 2003%' THEN  '05. ���� ������� � �������� �����, �� � BRM ������������� � �������� � ��������� (billing_id = 2003)'
            WHEN BRM_CONTRACT_NO IS NULL AND BRM_LOAD_STATUS = -2 AND BRM_LOAD_NOTES LIKE '%�� ������ �������� �/� �� �������� � BRM%' THEN  '06. ���� ������� � �������� �����, �� � BRM �� ������ �������� �/� �� �������� � BRM'
            WHEN BRM_CONTRACT_NO IS NULL AND BRM_LOAD_STATUS = -2 AND BRM_LOAD_NOTES LIKE '%�����.ERP_CODE != BRM.ERP_CODE%' THEN  '07. ���� ������� � �������� ����� � � BRM, �� �����.ERP_CODE != BRM.ERP_CODE'
            WHEN BRM_CONTRACT_NO IS NULL AND BRM_LOAD_STATUS = -1 AND BRM_LOAD_NOTES LIKE '%�� ���������� ����� �������� � BRM%' THEN '08. ���� ������� � �������� �����, �� � BRM �� ���������� ����� ��������'
            WHEN BRM_CONTRACT_NO IS NULL AND BRM_LOAD_STATUS = -1 AND BRM_LOAD_NOTES LIKE '%�� ���������� ID �������� � �����%' THEN '09. �� ���������� ID �������� � �����'
            WHEN BRM_CONTRACT_NO IS NULL AND BRM_LOAD_STATUS = -1 AND BRM_LOAD_NOTES LIKE '%�� ���������� ����� �������� � �����%' THEN '10. �� ���������� ����� �������� � �����'
            WHEN BRM_CONTRACT_NO IS NOT NULL AND BRM_LOAD_STATUS != 2 THEN '11. O����� ��������� ������ ������� �����, ��� �������� � BRM'
            WHEN BRM_CONTRACT_NO IS NULL AND EISUP_CONTRACT_NO IS NOT NULL THEN '12. ��� �������� � BRM, �� ���� � �������� �� �����'        
            WHEN BRM_CONTRACT_NO IS NULL AND EISUP_CONTRACT_NO IS NULL AND BALANCE IS NOT NULL THEN '13. � �������� ����� ����������� ����� ��������'
            ELSE '14. ������'
        END REPSTR,
        COUNT(*) 
      FROM R  
     GROUP BY 
        CASE
            WHEN BRM_CONTRACT_NO IS NOT NULL AND EISUP_CONTRACT_NO IS NULL AND REP_PERIOD_ID > 201601 THEN '01. ���� �������� � BRM, � 2016 ���� ���� ������������ �����, �� ��� � �������� �����'
            WHEN BRM_CONTRACT_NO IS NOT NULL AND EISUP_CONTRACT_NO IS NULL AND REP_PERIOD_ID IS NOT NULL THEN '02. ���� �������� � BRM �� 2016 ���� ������������ �����, �� ��� � �������� �����'
            WHEN BRM_CONTRACT_NO IS NOT NULL AND EISUP_CONTRACT_NO IS NULL THEN '03. ���� �������� � BRM, ������������ ������ ��� (�������� �� �����������), �� ��� � �������� �����'
            WHEN BRM_CONTRACT_NO IS NOT NULL AND BRM_LOAD_STATUS  = 2 THEN '04. OK'
            WHEN BRM_CONTRACT_NO IS NULL AND BRM_LOAD_STATUS = -2 AND BRM_LOAD_NOTES LIKE '%billing_id = 2003%' THEN  '05. ���� ������� � �������� �����, �� � BRM ������������� � �������� � ��������� (billing_id = 2003)'
            WHEN BRM_CONTRACT_NO IS NULL AND BRM_LOAD_STATUS = -2 AND BRM_LOAD_NOTES LIKE '%�� ������ �������� �/� �� �������� � BRM%' THEN  '06. ���� ������� � �������� �����, �� � BRM �� ������ �������� �/� �� �������� � BRM'
            WHEN BRM_CONTRACT_NO IS NULL AND BRM_LOAD_STATUS = -2 AND BRM_LOAD_NOTES LIKE '%�����.ERP_CODE != BRM.ERP_CODE%' THEN  '07. ���� ������� � �������� ����� � � BRM, �� �����.ERP_CODE != BRM.ERP_CODE'
            WHEN BRM_CONTRACT_NO IS NULL AND BRM_LOAD_STATUS = -1 AND BRM_LOAD_NOTES LIKE '%�� ���������� ����� �������� � BRM%' THEN '08. ���� ������� � �������� �����, �� � BRM �� ���������� ����� ��������'
            WHEN BRM_CONTRACT_NO IS NULL AND BRM_LOAD_STATUS = -1 AND BRM_LOAD_NOTES LIKE '%�� ���������� ID �������� � �����%' THEN '09. �� ���������� ID �������� � �����'
            WHEN BRM_CONTRACT_NO IS NULL AND BRM_LOAD_STATUS = -1 AND BRM_LOAD_NOTES LIKE '%�� ���������� ����� �������� � �����%' THEN '10. �� ���������� ����� �������� � �����'
            WHEN BRM_CONTRACT_NO IS NOT NULL AND BRM_LOAD_STATUS != 2 THEN '11. O����� ��������� ������ ������� �����, ��� �������� � BRM'
            WHEN BRM_CONTRACT_NO IS NULL AND EISUP_CONTRACT_NO IS NOT NULL THEN '12. ��� �������� � BRM, �� ���� � �������� �� �����'        
            WHEN BRM_CONTRACT_NO IS NULL AND EISUP_CONTRACT_NO IS NULL AND BALANCE IS NOT NULL THEN '13. � �������� ����� ����������� ����� ��������'
            ELSE '14. ������'
        END
    ORDER BY 1
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ======================================================================== --
-- ����� � ���������� ������� � ������ �����, 
-- ������ ��������� ��� ������� ���� � BRM
-- ------------------------------------------------------------------------ --
PROCEDURE Dup_report (
               p_recordset    OUT t_refc 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Dup_report';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
      SELECT E.* 
        FROM EISUP_BALANCE_T E
       WHERE BRM_LOAD_NOTES LIKE '%��������� contract_no � ����� | contract_no ���� � BRM%' 
       ORDER BY CONTRACT_NO  
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
 
-- --------------------------------------------------------------------------------------
-- ������ ��������� � BRM ��� ������� ���� ������������ ����� � 201601 
-- ------------------------------------------------------------------------ --
PROCEDURE BRM_contract_not_found (
               p_recordset    OUT t_refc 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'BRM_contract_not_found';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
        SELECT * FROM (
              SELECT BC.CONTRACT_NO BRM_CONTRACT_NO,
                     BC.CONTRACT_DATE BRM_CONTRACT_DATE, 
                     BC.ERP_CODE BRM_ERP_CODE, 
                     BC.COMPANY_NAME BRM_COMPANY_NAME, 
                     BC.REP_PERIOD_ID, 
                     BC.BILL_NO,
                     BC.BILL_DATE,
                     E.CONTRACT_ID EISUP_CONTRACT_ID, E.CONTRACT_NO EISUP_CONTRACT_NO, E.ERP_CODE EISUP_ERP_CODE, 
                     E.FACTURE_NUM, E.BALANCE, E.BALANCE_PERIOD, 
                     E.BRM_CONTRACT_ID, E.BRM_ACCOUNT_ID, E.BRM_ACCOUNTS_NUM, 
                     E.BRM_COMPANY_ID, E.BRM_LOAD_STATUS, E.BRM_LOAD_NOTES
                FROM EISUP_CONTRACTS_T BC FULL OUTER JOIN EISUP_BALANCE_T E
                  ON BC.CONTRACT_ID = E.BRM_CONTRACT_ID AND BC.RN = 1  
              ORDER BY NVL(BC.CONTRACT_NO, E.CONTRACT_NO)
        )
        WHERE EISUP_CONTRACT_NO IS NULL
          AND REP_PERIOD_ID >= 201601  
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- --------------------------------------------------------------------------------------
-- ������ ��������� � BRM ��� ������� ���� ������������ ����� � 201601 
-- ------------------------------------------------------------------------ --
PROCEDURE Other_error_report (
               p_recordset    OUT t_refc 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Other_error_report';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
        SELECT * FROM (
              SELECT E.CONTRACT_ID EISUP_CONTRACT_ID, E.CONTRACT_NO EISUP_CONTRACT_NO, E.ERP_CODE EISUP_ERP_CODE, 
                     E.FACTURE_NUM, E.BALANCE, E.BALANCE_PERIOD, 
                     E.BRM_CONTRACT_ID, E.BRM_ACCOUNT_ID, E.BRM_ACCOUNTS_NUM, 
                     E.BRM_COMPANY_ID, E.BRM_LOAD_STATUS, E.BRM_LOAD_NOTES,
                     BC.CONTRACT_NO BRM_CONTRACT_NO,
                     BC.CONTRACT_DATE BRM_CONTRACT_DATE, 
                     BC.ERP_CODE BRM_ERP_CODE, 
                     BC.COMPANY_NAME BRM_COMPANY_NAME, 
                     BC.REP_PERIOD_ID, 
                     BC.BILL_NO,
                     BC.BILL_DATE
                FROM EISUP_CONTRACTS_T BC FULL OUTER JOIN EISUP_BALANCE_T E
                  ON BC.CONTRACT_ID = E.BRM_CONTRACT_ID AND BC.RN = 1  
              ORDER BY NVL(BC.CONTRACT_NO, E.CONTRACT_NO)
        )
        WHERE BRM_LOAD_STATUS < 0
          AND BRM_LOAD_NOTES != 'contract_no �� ������ BRM'
          AND BRM_LOAD_NOTES != '��������� contract_no � �����'
          AND BRM_LOAD_NOTES NOT LIKE '%��������� contract_no � BRM%'
          AND BRM_LOAD_NOTES NOT LIKE '%��������� contract_no � ����� | contract_no ���� � BRM%'
          AND BRM_LOAD_NOTES NOT LIKE '%�� ������ �������� �/� �� �������� � BRM%'
          AND BRM_LOAD_NOTES NOT LIKE '%billing_id = 2003 - ������ ������� � BRM%'   
        ORDER BY NVL(BRM_CONTRACT_NO, EISUP_CONTRACT_NO)
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
-- ������ �������� �� ��������� ���� 
-- ------------------------------------------------------------------------- --
PROCEDURE Calc_balance ( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Calc_balance';
    v_count      INTEGER;
BEGIN
    -- ������ ������������� ������� 
    DELETE FROM EISUP_BALANCE_BRM_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_BRM_T: '||v_count||' - rows deleted', c_PkgName||'.'||v_prcName);
    --
    -- ��������� ������������� �������
    INSERT INTO EISUP_BALANCE_BRM_T(
        CONTRACT_ID, CONTRACT_NO, BILL_TOTAL, PAY_RECVD, BALANCE, PERIOD_ID, CREATE_DATE
    )
    WITH CT AS (
        SELECT PERIOD_ID, PROFILE_ID, ACCOUNT_ID, ACCOUNT_NO, CONTRACT_ID, CONTRACT_NO
          FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY AP.ACCOUNT_ID, P.PERIOD_ID ORDER BY AP.DATE_FROM DESC) RN,
                   P.PERIOD_ID, AP.PROFILE_ID, AP.ACCOUNT_ID, A.ACCOUNT_NO, AP.CONTRACT_ID, C.CONTRACT_NO  
              FROM PERIOD_T P, ACCOUNT_PROFILE_T AP, ACCOUNT_T A, CONTRACT_T C
             WHERE AP.DATE_FROM < P.PERIOD_TO
               AND (AP.DATE_TO IS NULL OR P.PERIOD_FROM < AP.DATE_TO)
               AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
               AND AP.CONTRACT_ID = C.CONTRACT_ID
               AND A.ACCOUNT_TYPE = 'J'
           )
           WHERE RN = 1
    ), BP AS (
        SELECT CT.CONTRACT_ID, CT.CONTRACT_NO, 
               SUM(NVL(TOTAL, 0)) BILL_TOTAL, 
               SUM(NVL(RECVD, 0)) PAY_RECVD, 
               SUM(NVL(RECVD, 0)) - SUM(NVL(TOTAL, 0)) BALANCE 
          FROM (
            SELECT B.ACCOUNT_ID, B.REP_PERIOD_ID,  
                   B.BILL_NO, B.BILL_DATE, B.TOTAL, B.GROSS, 
                   NULL DOC_ID, NULL PAYMENT_DATE, 0 RECVD
              FROM BILL_T B
            UNION ALL
            SELECT P.ACCOUNT_ID, P.REP_PERIOD_ID, 
                   NULL BILL_NO, NULL BILL_DATE, 0 TOTAL, 0 GROSS, 
                   P.DOC_ID, P.PAYMENT_DATE, P.RECVD
              FROM PAYMENT_T P
           ) BP, CT
           WHERE BP.ACCOUNT_ID = CT.ACCOUNT_ID
             AND NOT EXISTS (
                SELECT * 
                  FROM INCOMING_BALANCE_T IB
                 WHERE IB.ACCOUNT_ID    = BP.ACCOUNT_ID
                   AND IB.REP_PERIOD_ID > BP.REP_PERIOD_ID 
              )
             AND BP.REP_PERIOD_ID  = CT.PERIOD_ID
             AND BP.ACCOUNT_ID     = CT.ACCOUNT_ID
             AND BP.REP_PERIOD_ID  < p_period_id
           GROUP BY CT.CONTRACT_ID, CT.CONTRACT_NO
    )
    SELECT BP.CONTRACT_ID, BP.CONTRACT_NO, BP.BILL_TOTAL, BP.PAY_RECVD, BP.BALANCE,
           p_period_id PERIOD_ID, SYSDATE CREATE_DATE  
      FROM BP, EISUP_CONTRACTS_T EC
     WHERE BP.CONTRACT_ID = EC.CONTRACT_ID;  
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_BRM_T: '||v_count||' - rows inserted', c_PkgName||'.'||v_prcName);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ========================================================================= --
-- �������������� ������
-- ========================================================================= --
-- ------------------------------------------------------------------------- --
-- �������� ��������� �� ������� ���� �/�. ������� �� ��������. 
-- ------------------------------------------------------------------------- --
PROCEDURE View_simple_contracts (
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'View_simple_contracts';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
      WITH AP AS (
          SELECT CONTRACT_ID, ACCOUNT_ID, ACCOUNT_NO, CONTRACTOR_ID, BRANCH_ID, KPP, BILLING_ID
            FROM (
                SELECT AP.CONTRACT_ID, AP.ACCOUNT_ID, A.ACCOUNT_NO, AP.CONTRACTOR_ID, AP.BRANCH_ID, AP.KPP, A.BILLING_ID,
                       COUNT(*) OVER (PARTITION BY AP.CONTRACT_ID) ACCOUNT_COUNT,
                       ROW_NUMBER() OVER (PARTITION BY AP.CONTRACT_ID ORDER BY A.ACCOUNT_ID) RN
                  FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A
                 WHERE AP.ACCOUNT_ID  = A.ACCOUNT_ID
                   AND AP.ACTUAL      = 'Y'
                   AND A.BILLING_ID NOT IN (2000,2003)
                   AND A.ACCOUNT_TYPE = 'J'
            )   
           WHERE RN = 1
             AND ACCOUNT_COUNT = 1
      )
      SELECT AP.ACCOUNT_ID,   -- id �/� (����� �� ���������� � GUI)
             AP.ACCOUNT_NO,   -- ����� �/�
             C.CONTRACT_ID,   -- id ��������  (����� �� ���������� � GUI)
             C.CONTRACT_NO,   -- ����� ��������
             C.DATE_FROM CONTRACT_DATE,  -- ���� ��������
             CM.COMPANY_NAME, -- ��� ��������
             CM.SHORT_NAME,   -- ��� ��������(����)
             CM.INN,          -- ���
             AP.KPP,          -- ���
             CM.ERP_CODE,     -- ERP_CODE 
             CT.CONTRACTOR,   -- ��������
             CT.CONTRACTOR BRANCH, -- ������
             MIN(B.REP_PERIOD_ID) MIN_BILL_PERIOD, -- ������ ������� �����
             MAX(B.REP_PERIOD_ID) MAX_BILL_PERIOD, -- ������ ���������� �����
             COUNT(*) BILL_COUNT,                  -- ���-�� ������
             SUM(B.TOTAL) BILLS_TOTAL,             -- ����� ������
             AP.BILLING_ID                         -- id ��������
        FROM AP, CONTRACT_T C, COMPANY_T CM, CONTRACTOR_T CT, CONTRACTOR_T BR, BILL_T B
       WHERE C.CONTRACT_ID = CM.CONTRACT_ID
         AND CM.ACTUAL = 'Y'
         AND AP.CONTRACT_ID = C.CONTRACT_ID
         AND CT.CONTRACTOR_ID = AP.CONTRACTOR_ID
         AND BR.CONTRACTOR_ID = AP.BRANCH_ID
         AND B.ACCOUNT_ID     = AP.ACCOUNT_ID
         AND B.CONTRACT_ID    = AP.CONTRACT_ID 
      GROUP BY AP.ACCOUNT_ID, AP.ACCOUNT_NO,
               C.CONTRACT_ID, C.CONTRACT_NO, C.DATE_FROM, 
               CM.COMPANY_NAME, CM.SHORT_NAME, CM.INN, AP.KPP, CM.ERP_CODE,
               CT.CONTRACTOR, CT.CONTRACTOR, AP.BILLING_ID
      ORDER BY MAX(B.REP_PERIOD_ID) DESC, CONTRACT_NO
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
-- �������� �������� �� �������� �� ��������� ������ 
-- ------------------------------------------------------------------------- --
PROCEDURE View_contract_period_info (
               p_recordset    OUT t_refc,
               p_period_id    IN  NUMBER,
               p_contract_id  IN  INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'View_contract_period_info';
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
              FROM ACCOUNT_T A, --ACCOUNT_PROFILE_T AP, 
                   REP_PERIOD_INFO_T PI
             WHERE A.ACCOUNT_ID   = PI.ACCOUNT_ID(+)
               AND PI.REP_PERIOD_ID(+) <= p_period_id
               AND A.ACCOUNT_TYPE = 'J'
         )
         WHERE (REP_PERIOD_ID = MAX_PERIOD_ID OR REP_PERIOD_ID IS NULL)
      ), AB AS (
      SELECT p_period_id REP_PERIOD_ID,
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
      ), 
      CBA AS ( -- ������� ������� ������ �������� �� ��������� ������ - - --
      SELECT * FROM (
        SELECT AB.REP_PERIOD_ID, 
               AP.CONTRACT_ID, 
               AB.ACCOUNT_ID, 
               AB.ACCOUNT_NO, 
               AB.IN_BALANCE, 
               AB.TOTAL, 
               AB.RECVD, 
               AB.OUT_BALANCE,
               ROW_NUMBER() OVER (PARTITION BY AP.ACCOUNT_ID ORDER BY NVL(AP.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))) RN  
          FROM AB, ACCOUNT_PROFILE_T AP
         WHERE AP.DATE_FROM < PK04_PERIOD.PERIOD_TO(p_period_id)
           AND (AP.DATE_TO IS NULL OR PK04_PERIOD.PERIOD_FROM(p_period_id) <= AP.DATE_TO )
           AND AB.ACCOUNT_ID  = AP.ACCOUNT_ID
           AND AP.CONTRACT_ID = p_contract_id
        )
        WHERE RN = 1 
      ) -- ������� �� �������� �� ��������� ������ - - - - - - - - - - - - - --
      SELECT CBA.REP_PERIOD_ID,               -- id �������
           CBA.CONTRACT_ID,                 -- id ��������
           SUM(CBA.IN_BALANCE)  IN_BALANCE, -- �������� ������ (�� ������ ����  ������)
           SUM(CBA.TOTAL)       TOTAL,      -- ������� ������ �� �����
           SUM(CBA.RECVD)       RECVD,      -- ������� �������� �� �����
           SUM(CBA.OUT_BALANCE) OUT_BALANCE -- ��������� ������ (�� ��������� ������� ���������� ��� ������)
      FROM CBA 
      GROUP BY CBA.REP_PERIOD_ID, CBA.CONTRACT_ID
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
-- �������� �������� ��� ���������� �������� 
-- ------------------------------------------------------------------------- --
PROCEDURE View_contract_payments (
               p_recordset    OUT t_refc,
               p_period_id    IN  INTEGER,
               p_contract_id  IN  INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'View_contract_payments';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
      SELECT * FROM (
          SELECT P.REP_PERIOD_ID, -- id ������� ������� 
                 AP.CONTRACT_ID,  -- id ��������
                 A.ACCOUNT_ID,    -- id �/�
                 A.ACCOUNT_NO,    -- ����� �/�
                 P.DOC_ID,        -- ������������� ��������� / ����� ���������� ���������
                 P.PAYMENT_DATE,  -- ���� �������
                 P.RECVD,         -- ����� ������� 
                 P.PAY_DESCR,     -- �������� �������
                 P.PAYMENT_TYPE,  -- ��� �������
                 PS.PAYSYSTEM_NAME,  -- ��������� �������, ����� �������� ������ ������
                 A.BILLING_ID,    -- id �������� � ������� ������������� �/�
                 A.ACCOUNT_TYPE,  -- ��� �/�
                 A.STATUS ACCOUNT_STATUS, -- ������ �/�
                 ROW_NUMBER() OVER (PARTITION BY AP.ACCOUNT_ID ORDER BY NVL(AP.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))) RN
            FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A, PAYMENT_T P, PAYSYSTEM_T PS
           WHERE AP.DATE_FROM < PK04_PERIOD.PERIOD_TO(p_period_id)
             AND (AP.DATE_TO IS NULL OR PK04_PERIOD.PERIOD_FROM(p_period_id) <= AP.DATE_TO )
             AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
             AND AP.CONTRACT_ID = p_contract_id
             AND P.ACCOUNT_ID   = A.ACCOUNT_ID
             AND P.PAYSYSTEM_ID = PS.PAYSYSTEM_ID(+)
         )
       WHERE RN = 1
       ORDER BY CONTRACT_ID, ACCOUNT_NO, PAYMENT_DATE
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
-- �������� ������ ��� ���������� �������� �� ������
-- ------------------------------------------------------------------------- --
PROCEDURE View_contract_bills (
               p_recordset    OUT t_refc,
               p_period_id    IN  INTEGER,
               p_contract_id  IN  INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'View_contract_bills';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
      WITH TPI AS (   -- �������� �������� ������ � �����
          SELECT L.FACTURENUM,
                 H.HEADER_ID,
                 GR.NAME GROUP_NAME, 
                 GR.TABLE_NAME_H, 
                 GR.TABLE_NAME_L, 
                 H.JOURNAL_ID, 
                 H.SESSION_ID,
                 H.DATE_EXPORT_1C 
            FROM EXPORT_1C_HEADER_T H,
                 EXPORT_1C_GROUP_T GR,
                 (
                     SELECT HEADER_ID, FACTUREEXTERNALID FACTURENUM FROM EXPORT_1C_LINES_T
                      UNION ALL
                     SELECT HEADER_ID, FACTURENUM FROM EXPORT_1C_LINES_2003_T
                 ) L
           WHERE H.HEADER_ID = L.HEADER_ID
             AND GR.GROUP_ID = H.GROUP_ID       
             AND H.STATUS = 'EXPORT_DATA_OK'   
             AND (H.EXPORT_TYPE <> 'ERROR' OR H.EXPORT_TYPE IS NULL)           
             AND H.GROUP_ID NOT IN (0,-1,99)
             AND H.HEADER_ID IN (       
                  SELECT  HEADER_ID FROM (        
                      SELECT HEADER_ID, ROW_NUMBER() OVER (PARTITION BY GROUP_ID,PERIOD_ID ORDER BY VERSION DESC) RN 
                        FROM EXPORT_1C_HEADER_T H
                       WHERE (EXPORT_TYPE <> 'ADD' OR EXPORT_TYPE IS NULL) 
                         AND STATUS = 'EXPORT_DATA_OK'
                  )
                  WHERE RN = 1
                  UNION ALL
                  (
                  SELECT HEADER_ID
                    FROM EXPORT_1C_HEADER_T H
                   WHERE EXPORT_TYPE = 'ADD' 
                     AND STATUS = 'EXPORT_DATA_OK'
                  )        
              )
          )
      SELECT B.REP_PERIOD_ID,         -- id �������
             B.CONTRACT_ID,           -- id ��������
             A.ACCOUNT_ID,            -- id �/�
             A.ACCOUNT_NO,            -- ����� �/�
             B.BILL_NO,               -- ����� �����
             B.BILL_DATE,             -- ���� �����
             B.BILL_TYPE,             -- ��� �����
             B.BILL_STATUS,           -- ������ �����
             B.TOTAL,                 -- ����� �����
             A.BILLING_ID,            -- id �������� � ������� ������������� �/�
             A.ACCOUNT_TYPE,          -- ��� �/�
             A.STATUS ACCOUNT_STATUS, -- ������ �/�
             CT.CONTRACTOR_ID,        -- id ��������
             CT.CONTRACTOR,           -- �������� 
             BR.CONTRACTOR_ID BRANCH_ID, -- id �������
             BR.CONTRACTOR BRANCH,    -- ������,
             -- ���������� �������� � TPI �����
             TPI.HEADER_ID,           -- id ��������� ��������
             TPI.GROUP_NAME,          -- ������ ��������
             TPI.TABLE_NAME_H,        -- ������� ���� ��� ������� ���������
             TPI.TABLE_NAME_L,        -- ������� ���� ���� �������� ������ ��������
             TPI.JOURNAL_ID,          -- id ������� ��������
             TPI.SESSION_ID,          -- id ������ ��������
             TPI.DATE_EXPORT_1C       -- ���� �������� � 1�
        FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACTOR_T CT, CONTRACTOR_T BR, TPI 
       WHERE B.REP_PERIOD_ID = p_period_id   -- 201610
         AND B.CONTRACT_ID   = p_contract_id -- 184213448 
         AND B.ACCOUNT_ID    = A.ACCOUNT_ID
         AND B.CONTRACTOR_ID = CT.CONTRACTOR_ID
         AND B.PROFILE_ID    = AP.PROFILE_ID
         AND AP.BRANCH_ID    = BR.CONTRACTOR_ID
         AND B.BILL_NO       = TPI.FACTURENUM(+)
       ORDER BY B.CONTRACT_ID, A.ACCOUNT_NO, B.BILL_NO
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
-- �������� ���������� �� ������ ����������� � TPI �� ������
-- ------------------------------------------------------------------------- --
PROCEDURE View_tpi_bills (
               p_recordset    OUT t_refc,
               p_period_id    IN  INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'View_tpi_bills';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
      WITH TPL AS (
          SELECT DISTINCT
                 HEADER_ID,
                 SUBSTR(EXECUTIONPERIOD,1,4)||SUBSTR(EXECUTIONPERIOD,6,2)  PERIOD_ID,
                 PARTNERID ERP_CODE, 
                 CUSTNAME  COMPANY_NAME, 
                 INN, KPP,
                 SALES_NAME,
                 CLIENT_SH COMPANY_SHORT,
                 AUTO_NO CONTRACT_NO,
                 ACCOUNT_NO,
                 FACTUREEXTERNALID BILL_NO
            FROM EXPORT_1C_LINES_T
           WHERE EXECUTIONPERIOD = SUBSTR(p_period_id,1,4)||'.'||SUBSTR(p_period_id,5,2) --'2016.12'
      ), TPI AS (   -- �������� �������� ������ � �����
          SELECT L.PERIOD_ID,
                 L.ERP_CODE, 
                 L.INN, L.KPP,
                 L.COMPANY_NAME, 
                 L.COMPANY_SHORT,
                 L.SALES_NAME,
                 L.CONTRACT_NO,
                 L.ACCOUNT_NO,
                 L.BILL_NO,
                 H.HEADER_ID,
                 GR.NAME GROUP_NAME, 
                 GR.TABLE_NAME_H, 
                 GR.TABLE_NAME_L, 
                 H.JOURNAL_ID, 
                 H.SESSION_ID,
                 H.DATE_EXPORT_1C 
          FROM EXPORT_1C_HEADER_T H,
               EXPORT_1C_GROUP_T GR,
               TPL L
          WHERE H.HEADER_ID = L.HEADER_ID
           AND GR.GROUP_ID = H.GROUP_ID       
           AND H.STATUS = 'EXPORT_DATA_OK'   
           AND (H.EXPORT_TYPE <> 'ERROR' OR H.EXPORT_TYPE IS NULL)           
           AND H.GROUP_ID NOT IN (0,-1,99)
           AND H.HEADER_ID IN (       
                SELECT  HEADER_ID FROM (        
                    SELECT HEADER_ID, ROW_NUMBER() OVER (PARTITION BY GROUP_ID,PERIOD_ID ORDER BY VERSION DESC) RN 
                      FROM EXPORT_1C_HEADER_T H
                     WHERE (EXPORT_TYPE <> 'ADD' OR EXPORT_TYPE IS NULL) 
                       AND STATUS = 'EXPORT_DATA_OK'
                )
                WHERE RN = 1
                UNION ALL
                (
                SELECT HEADER_ID
                  FROM EXPORT_1C_HEADER_T H
                 WHERE EXPORT_TYPE = 'ADD' 
                   AND STATUS = 'EXPORT_DATA_OK'
                )        
            )
      )
      SELECT * FROM TPI
      ORDER BY JOURNAL_ID, CONTRACT_NO
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
-- ������� ��� ������ �������� 
-- ------------------------------------------------------------------------- --
PROCEDURE SQL_for_check_payments (
               p_recordset    OUT t_refc,
               p_period_id    IN  INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'SQL_for_check_payments';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ (���� ����� �������� �� �������������)
    OPEN p_recordset FOR
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      -- ������� ��� ������ ��������
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
      WITH AP AS (    -- ������� �� ����� ���������� �������
        SELECT * FROM (
          SELECT AP.*, A.ACCOUNT_NO, C.CONTRACT_NO, 
                 ROW_NUMBER() OVER (PARTITION BY AP.ACCOUNT_ID ORDER BY NVL(AP.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))) RN
            FROM ACCOUNT_PROFILE_T AP, CONTRACT_T C, ACCOUNT_T A
           WHERE 1=1
             AND AP.DATE_FROM < PK04_PERIOD.PERIOD_TO(p_period_id)
             AND (AP.DATE_TO IS NULL OR PK04_PERIOD.PERIOD_FROM(p_period_id) <= AP.DATE_TO )
             AND AP.CONTRACT_ID = C.CONTRACT_ID
             AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
             --AND A.BILLING_ID IN (2001,2002)
             AND A.BILLING_ID NOT IN (2000, 2003)
             AND A.ACCOUNT_ID > 10
          )
         WHERE RN = 1 
      ), P AS (   -- ������� BRM ����������� �� �/� � ��������� �������
        SELECT AP.CONTRACT_ID, AP.CONTRACT_NO, AP.ACCOUNT_NO, P.*
          FROM AP, PAYMENT_T P
         WHERE AP.ACCOUNT_ID = P.ACCOUNT_ID
           AND P.REP_PERIOD_ID = p_period_id
      ), PS AS (  -- ������� BRM ����������� �� ������� � ��������� �������
        SELECT P.CONTRACT_ID, P.CONTRACT_NO, SUM(RECVD) RECVD 
          FROM P
         GROUP BY P.CONTRACT_ID, P.CONTRACT_NO
      ), PBE AS ( -- ������ � ��������� �����
          SELECT PS.*, T.* 
            FROM PS FULL OUTER JOIN EISUP_RP_BALANCE_TMP_1 T ON (PS.CONTRACT_ID = T.BRM_CONTRACT_ID)
      ), PBE_OK AS (
          -- �������� �� ������� ������� ����� �������� 
          SELECT * FROM PBE
           WHERE CONTRACT_ID IS NOT NULL
             AND BRM_CONTRACT_ID IS NOT NULL
             AND RECVD = PAY_TOTAL    -- 291 - �����
             AND RECVD != 0
      ), PBE_BAD_SUM AS (
          -- �������� �� ������� ���� ������� � � BRM � � �����, �� ����� �� ���������
          SELECT * FROM PBE
           WHERE CONTRACT_ID IS NOT NULL
             AND BRM_CONTRACT_ID IS NOT NULL
             AND RECVD != PAY_TOTAL    -- 3.251 - �����
      ), PBE_BRM_NOT_FOUND AS (
          -- �������� �� ������� ���� ������ � �����, �� ��� � BRM 
          SELECT * FROM PBE
           WHERE CONTRACT_ID IS NULL
             AND BRM_CONTRACT_ID IS NOT NULL
             AND PAY_TOTAL != 0        -- 14.010
      ), PBE_EISUP_NOT_FOUND AS (
          -- �������� �� ������� ���� ������ � BRM, �� ��� � ����� 
          SELECT * FROM PBE
           WHERE CONTRACT_ID IS NOT NULL
             AND BRM_CONTRACT_ID IS NULL
             AND RECVD != 0             -- 6
      ), PBE_CHECK AS (
          -- ��������� ������������ PAYMENT_T � REP_PERIOD_INFO_T � BRM
          -- ����������� ����� ��������� ��� ������ � ��������� ������ ������ (���� �� ���������)
          SELECT BRM_PERIOD_ID, CONTRACT_ID, CONTRACT_NO, RECVD, BRM_PAY_TOTAL 
            FROM PBE
           WHERE CONTRACT_ID IS NOT NULL
             AND RECVD != BRM_PAY_TOTAL             -- 6
      )
      SELECT * FROM PBE_CHECK
      ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;



END PK10_BALANCE_EISUP;
/
