CREATE OR REPLACE PACKAGE PK31_BILLING_CTRL
IS
    --
    -- ����� ��� ��������� �������� ����������� ������ � �������� �������
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK31_BILLING_CTRL';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������ ������ �� ������ � ��������� � �������� � ����� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE View_period_bills (
                   p_recordset    OUT t_refc,
                   p_period_id    IN  INTEGER
               );

    -- ========================================================================= --
    -- �������� ������������ ���� ���������� ������ � ������������ ������
    -- ========================================================================= --
    -- ���������� �� ������ ���������
    PROCEDURE Bill_stat( 
                   p_recordset OUT t_refc,
                   p_period_id IN INTEGER
               );

    PROCEDURE Check_bill_sum( 
                   p_recordset OUT t_refc,
                   p_period_id IN INTEGER
               );
        
    PROCEDURE Check_bill_sum_detail( 
                   p_recordset OUT t_refc,
                   p_period_id IN INTEGER
               );
   
    -- ���� ��� � ���������� ������, �� ��������� ��������� �������, ��� ������� ������ 
    -- �� ��������� ������:
    PROCEDURE Fill_tmptable_bill_sum_detail( 
                   p_period_id_from IN INTEGER,
                   p_period_id_to   IN INTEGER
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������������ ���������� ����� B.TOTAL = B.GROSS + B.TAX 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Check_bills ( 
                   p_recordset OUT t_refc, 
                   p_period_id IN INTEGER -- ������� � ������ ������� �������
               );

    -- �������� ��� �� ITEM-� ����� � INVOICE_ITEM-� � ����� � ����� 
    -- �������� ����� ��������� ������ ��� ������ ������������� � �������� �����
    PROCEDURE Check_items ( 
                   p_recordset OUT t_refc, 
                   p_period_id IN INTEGER -- ������� � ������ ������� �������
               );
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ��� ��� ���� INVOICE_ITEM-�� ���� ITEM-� 
    -- �������� ����� ��������� ������ ��� ������ ������������� � �������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Check_invoice_items ( 
                   p_recordset OUT t_refc, 
                   p_period_id IN INTEGER -- ������� � ������ ������� �������
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������������ ��������� ������ � �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Check_bill_vat( 
                   p_recordset OUT t_refc,
                   p_period_id INTEGER
               );

    -- �������� ������ �������� ������ ������������ �������
    PROCEDURE Open_bill_list( 
                   p_recordset OUT t_refc
               );
    
    -- �������� ������������ ���������� ���������
    PROCEDURE Check_ABP ( 
                   p_recordset OUT t_refc, 
                   p_period_id  IN INTEGER, -- ������� � ������ ������� �������
                   p_billing_id IN INTEGER
               );
   
    -- �������� ������������ ���������� ������� �� ����������� ���������
    PROCEDURE Check_MIN ( 
                   p_recordset OUT t_refc, 
                   p_period_id  IN INTEGER, -- ������� � ������ ������� �������
                   p_billing_id IN INTEGER
               );

    -- �������� ������������ ����������� �������
    PROCEDURE Check_IDL ( 
                   p_recordset OUT t_refc, 
                   p_period_id IN INTEGER -- ������� � ������ ������� �������
               ); 

    -- �������� ������� �� �������� � ��������� ��������
    PROCEDURE Check_payments( 
                   p_recordset OUT t_refc,
                   p_period_id  IN INTEGER -- ��� ������ �������� �����
               );
    
    -- �������� ����� ������ �� ������ ������� BILL_T
    PROCEDURE Check_bill_payments( p_recordset OUT t_refc );
    
    -- �������� �������� �� ������������ ������ 
    -- �������� ����� ��������� ������ ��� ������ ������������� � �������� �����
    PROCEDURE Check_period_info( 
                   p_recordset    OUT t_refc, 
                   p_rep_period_id IN INTEGER -- ���-�� ������� �����
               );

    -- ���������� ��� 1�, ����� ����������� ������ �� ����������� � BRM � 1� 
    PROCEDURE Check_ErpCode( 
                   p_recordset OUT t_refc, 
                   p_period_id  IN INTEGER    -- ID ��������� �������
               );
    
    --=======================================================================================
    --              � � � � � � �   � � � � � � � � � �   � � � � � � � �  
    --                          ���� ���� �������� ��������
    --=======================================================================================
    
    -- ======================================================================= --
    -- �������� �������� �� ������� �����������
    -- ======================================================================= --
    -- ���������� � �������� �������
    --   - ������������� - ���-�� ��������� �������
    --   - ��� ������ ���������� ����������
    PROCEDURE Period_list( 
                   p_recordset OUT t_refc
               );

    -- �������� ��������� � ������� ����������� ��� ��������� �������
    PROCEDURE Msg_list(
                  p_recordset OUT t_refc,
                  p_function   IN VARCHAR2,                   -- ��� �������
                  p_date_from  IN DATE DEFAULT (SYSDATE-30)   -- ����� ������ ������� (���������������)
               );
               
    -- �������� ������� �������� �������������
    PROCEDURE Billing_history( 
                   p_recordset OUT t_refc, 
                   p_from_period_id IN INTEGER -- ������� � ������ ������� �������
               );
               
    
    -- �������� �������� �� ��������, �� ������ ������� REP_PERIOD_INFO_T
    PROCEDURE View_period_info( p_recordset OUT t_refc );
    
  
    -- �������� ������ ������, ���������� � ������ ������ 
    PROCEDURE Err_bill_list( 
                   p_recordset OUT t_refc, 
                   p_period_id  IN INTEGER    -- ID ��������� �������
               );
    

    
END PK31_BILLING_CTRL;
/
CREATE OR REPLACE PACKAGE BODY PK31_BILLING_CTRL
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������ ������ �� ������ � ��������� � �������� � ����� 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE View_period_bills (
               p_recordset    OUT t_refc,
               p_period_id    IN  INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'View_period_bills';
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
        FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, 
             CONTRACTOR_T CT, CONTRACTOR_T BR, TPI 
       WHERE B.REP_PERIOD_ID = p_period_id   -- 201610
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

-- ========================================================================= --
-- �������� ������� ������ � �������� �� ��������
-- ========================================================================= --
-- ���������� �� ������ ���������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Bill_stat( 
               p_recordset OUT t_refc,
               p_period_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bill_stat';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
      SELECT D.KEY, A.BILLING_ID, A.ACCOUNT_TYPE, 
             A.STATUS ACCOUNT_STATUS, B.BILL_STATUS, 
             COUNT(*), SUM(B.TOTAL) 
        FROM BILL_T B, ACCOUNT_T A, DICTIONARY_T D
       WHERE B.REP_PERIOD_ID = p_period_id
         AND B.ACCOUNT_ID    = A.ACCOUNT_ID
         AND A.BILLING_ID    = D.KEY_ID(+) 
       GROUP BY D.KEY, A.BILLING_ID, A.ACCOUNT_TYPE, A.STATUS, B.BILL_STATUS
       ORDER BY A.BILLING_ID, A.ACCOUNT_TYPE, A.STATUS, B.BILL_STATUS;
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ ���� ���������� ������ � ������������ ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_bill_sum( 
               p_recordset OUT t_refc,
               p_period_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_bill_sum';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
      WITH B AS (
          SELECT A.BILLING_ID, A.ACCOUNT_TYPE, 
                 SUM(B.TOTAL) B_TOTAL, SUM(B.TAX+B.GROSS) B_CALC_TOTAL, 
                 SUM(B.TAX) B_TAX, SUM(B.GROSS) B_GROSS 
            FROM BILL_T B, ACCOUNT_T A
           WHERE B.REP_PERIOD_ID = p_period_id
             --AND B.TOTAL != 0
             AND B.ACCOUNT_ID = A.ACCOUNT_ID
             AND A.STATUS = Pk00_Const.c_ACC_STATUS_BILL
           GROUP BY A.BILLING_ID, A.ACCOUNT_TYPE
      ), I AS (
          SELECT A.BILLING_ID,  A.ACCOUNT_TYPE,
                 SUM(I.BILL_TOTAL) I_TOTAL, 
                 SUM(I.REP_TAX) I_TAX, SUM(I.REP_GROSS) I_GROSS 
            FROM ITEM_T I, BILL_T B, ACCOUNT_T A
           WHERE B.REP_PERIOD_ID = p_period_id
             --AND B.TOTAL != 0
             AND B.ACCOUNT_ID = A.ACCOUNT_ID
             AND B.BILL_ID = I.BILL_ID
             AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND A.STATUS = Pk00_Const.c_ACC_STATUS_BILL
           GROUP BY A.BILLING_ID, A.ACCOUNT_TYPE
      ), INV AS (
          SELECT A.BILLING_ID,  A.ACCOUNT_TYPE,
                 SUM(II.TOTAL) INV_TOTAL,  
                 SUM(II.TAX) INV_TAX, SUM(II.GROSS) INV_GROSS 
            FROM INVOICE_ITEM_T II, BILL_T B, ACCOUNT_T A
           WHERE B.REP_PERIOD_ID = p_period_id
             --AND B.TOTAL != 0
             AND B.ACCOUNT_ID = A.ACCOUNT_ID
             AND B.BILL_ID = II.BILL_ID
             AND B.REP_PERIOD_ID = II.REP_PERIOD_ID
             AND A.STATUS = Pk00_Const.c_ACC_STATUS_BILL
           GROUP BY A.BILLING_ID,  A.ACCOUNT_TYPE
      )
      SELECT B.BILLING_ID, B.ACCOUNT_TYPE, 
             ROUND(B_TOTAL,2), 
             ROUND(INV_TOTAL,2), 
             ROUND(I_GROSS + I_TAX,2) I_TOTAL,
             ROUND(B_TOTAL - (I_GROSS + I_TAX),2) TOTAL_DELTA,
             ROUND(B_GROSS, 2),
             ROUND(INV_GROSS, 2),
             ROUND(I_GROSS, 2),
             ROUND(B_GROSS - I_GROSS,2) GROSS_DELTA,
             ROUND(B_TAX, 2),
             ROUND(INV_TAX, 2),
             ROUND(I_TAX,2),
             ROUND(B_TAX - I_TAX, 2) TAX_DELTA
        FROM B, I, INV
      WHERE B.BILLING_ID   = I.BILLING_ID
        AND B.BILLING_ID   = INV.BILLING_ID
        AND B.ACCOUNT_TYPE = I.ACCOUNT_TYPE
        AND B.ACCOUNT_TYPE = INV.ACCOUNT_TYPE
      ORDER BY BILLING_ID, ACCOUNT_TYPE;
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ ���� ���������� ������ � ������������ ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_bill_sum_detail( 
               p_recordset OUT t_refc,
               p_period_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_bill_sum_detail';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
      WITH B AS (
          SELECT A.BILLING_ID, A.ACCOUNT_ID, A.ACCOUNT_TYPE, 
                 B.BILL_ID, B.BILL_NO, B.BILL_TYPE,
                 SUM(B.TOTAL) B_TOTAL, SUM(B.TAX+B.GROSS) B_CALC_TOTAL, 
                 SUM(B.TAX) B_TAX, SUM(B.GROSS) B_GROSS 
            FROM BILL_T B, ACCOUNT_T A
           WHERE B.REP_PERIOD_ID = p_period_id
             --AND B.TOTAL != 0
             AND B.ACCOUNT_ID = A.ACCOUNT_ID
             AND A.STATUS = Pk00_Const.c_ACC_STATUS_BILL
           GROUP BY A.BILLING_ID, A.ACCOUNT_ID, A.ACCOUNT_TYPE, 
                    B.BILL_ID, B.BILL_NO, B.BILL_TYPE
      ), I AS (
          SELECT A.BILLING_ID, A.ACCOUNT_ID,  A.ACCOUNT_TYPE, B.BILL_ID,
                 SUM(I.BILL_TOTAL) I_TOTAL, 
                 SUM(I.REP_TAX) I_TAX, SUM(I.REP_GROSS) I_GROSS 
            FROM ITEM_T I, BILL_T B, ACCOUNT_T A
           WHERE B.REP_PERIOD_ID = p_period_id
             --AND B.TOTAL != 0
             AND B.ACCOUNT_ID = A.ACCOUNT_ID
             AND B.BILL_ID = I.BILL_ID
             AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND A.STATUS = Pk00_Const.c_ACC_STATUS_BILL
           GROUP BY A.BILLING_ID, A.ACCOUNT_ID, A.ACCOUNT_TYPE, B.BILL_ID
      ), INV AS (
          SELECT A.BILLING_ID, A.ACCOUNT_ID,  A.ACCOUNT_TYPE, B.BILL_ID,
                 SUM(II.TOTAL) INV_TOTAL, SUM(II.TAX+II.GROSS) INV_CALC_TOTAL, 
                 SUM(II.TAX) INV_TAX, SUM(II.GROSS) INV_GROSS 
            FROM INVOICE_ITEM_T II, BILL_T B, ACCOUNT_T A
           WHERE B.REP_PERIOD_ID = p_period_id
             --AND B.TOTAL != 0
             AND B.ACCOUNT_ID = A.ACCOUNT_ID
             AND B.BILL_ID = II.BILL_ID
             AND B.REP_PERIOD_ID = II.REP_PERIOD_ID
             AND A.STATUS = Pk00_Const.c_ACC_STATUS_BILL
           GROUP BY A.BILLING_ID, A.ACCOUNT_ID, A.ACCOUNT_TYPE, B.BILL_ID
      ), R AS (
      SELECT B.BILLING_ID, B.ACCOUNT_ID, B.ACCOUNT_TYPE, 
             B.BILL_ID, B.BILL_NO, B.BILL_TYPE,
             B_TOTAL, INV_TOTAL, (I_GROSS + I_TAX) I_CALC_TOTAL,
             B_GROSS, INV_GROSS, I_GROSS,
             B_TAX, INV_TAX, I_TAX,
             CASE
              WHEN B_TOTAL != INV_TOTAL THEN -2
              WHEN ROUND(B_TOTAL,2) = ROUND((I_GROSS + I_TAX),2) THEN 0
              WHEN ABS(B_TOTAL - (I_GROSS + I_TAX)) BETWEEN 0 AND 0.019 THEN 1
              WHEN ABS(B_TOTAL - (I_GROSS + I_TAX)) BETWEEN 0.02 AND 0.029 THEN 2
              ELSE -1
             END B_TOTAL_STATE,
             B_TOTAL - (I_GROSS + I_TAX) B_TOTAL_DELTA,
             CASE
              WHEN B_GROSS != INV_GROSS THEN -2
              WHEN B_GROSS = I_GROSS THEN 0
              WHEN ABS(B_GROSS - I_GROSS) BETWEEN 0 AND 0.019 THEN 1
              WHEN ABS(B_TOTAL - I_GROSS) BETWEEN 0.02 AND 0.029 THEN 2
              ELSE -1
             END B_GROSS_STATE,
             B_GROSS - I_GROSS B_GROSS_DELTA
        FROM B, I, INV
      WHERE B.BILLING_ID   = I.BILLING_ID
        AND B.BILL_ID      = I.BILL_ID
        AND B.BILLING_ID   = INV.BILLING_ID
        AND B.BILL_ID      = INV.BILL_ID
        AND B.ACCOUNT_TYPE = I.ACCOUNT_TYPE
        AND B.ACCOUNT_TYPE = INV.ACCOUNT_TYPE
      )
      SELECT BILLING_ID, ACCOUNT_ID, ACCOUNT_TYPE, 
             BILL_ID, BILL_NO, BILL_TYPE,
             B_TOTAL, INV_TOTAL, I_CALC_TOTAL, B_TOTAL_STATE, B_TOTAL_DELTA,
             B_GROSS, INV_GROSS, I_GROSS, B_GROSS_STATE, B_GROSS_DELTA,
             B_TAX, INV_TAX, I_TAX 
        FROM R
       WHERE (B_TOTAL_STATE < 0 OR B_GROSS_STATE < 0  ) 
       ORDER BY BILLING_ID, ACCOUNT_TYPE, B_TOTAL_STATE       
    ;
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���� ��� � ���������� ������, �� ��������� ��������� �������, ��� ������� ������ 
-- �� ��������� ������:
-- CREATE TABLE PK31_BILL_ERR_TMP (
--     BILLING_ID      INTEGER, 
--     ACCOUNT_ID      INTEGER, 
--     ACCOUNT_TYPE    CHAR(1), 
--     REP_PERIOD_ID   INTEGER, 
--     BILL_ID         INTEGER, 
--     B_TOTAL         NUMBER, 
--     INV_TOTAL       NUMBER, 
--     I_CALC_TOTAL    NUMBER, 
--     B_TOTAL_STATE   INTEGER, 
--     B_TOTAL_DELTA   NUMBER,
--     B_GROSS         NUMBER, 
--     INV_GROSS       NUMBER, 
--     I_GROSS         NUMBER, 
--     B_GROSS_STATE   INTEGER, 
--     B_GROSS_DELTA   NUMBER,
--     B_TAX           NUMBER, 
--     INV_TAX         NUMBER, 
--     I_TAX           NUMBER
-- );
--
PROCEDURE Fill_tmptable_bill_sum_detail( 
               p_period_id_from IN INTEGER,
               p_period_id_to   IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Fill_tmptable_bill_sum_detail';
    v_count      INTEGER;
    v_table      VARCHAR2(100) := 'PK31_BILL_ERR_TMP';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE '||v_table||' DROP STORAGE';

    INSERT INTO PK31_BILL_ERR_TMP
      WITH B AS (
          SELECT A.BILLING_ID, A.ACCOUNT_ID, A.ACCOUNT_TYPE, 
                 B.REP_PERIOD_ID, B.BILL_ID, B.BILL_NO, B.BILL_TYPE,
                 SUM(B.TOTAL) B_TOTAL, SUM(B.TAX+B.GROSS) B_CALC_TOTAL, 
                 SUM(B.TAX) B_TAX, SUM(B.GROSS) B_GROSS 
            FROM BILL_T B, ACCOUNT_T A
           WHERE B.REP_PERIOD_ID BETWEEN p_period_id_from AND p_period_id_to
             --AND B.TOTAL != 0
             AND B.ACCOUNT_ID = A.ACCOUNT_ID
             AND A.STATUS = Pk00_Const.c_ACC_STATUS_BILL
           GROUP BY A.BILLING_ID, A.ACCOUNT_ID, A.ACCOUNT_TYPE, B.REP_PERIOD_ID, 
                    B.BILL_ID, B.BILL_NO, B.BILL_TYPE
      ), I AS (
          SELECT A.BILLING_ID, A.ACCOUNT_ID,  A.ACCOUNT_TYPE, B.REP_PERIOD_ID, B.BILL_ID,
                 SUM(I.BILL_TOTAL) I_TOTAL, 
                 SUM(I.REP_TAX) I_TAX, SUM(I.REP_GROSS) I_GROSS 
            FROM ITEM_T I, BILL_T B, ACCOUNT_T A
           WHERE B.REP_PERIOD_ID BETWEEN p_period_id_from AND p_period_id_to
             --AND B.TOTAL != 0
             AND B.ACCOUNT_ID = A.ACCOUNT_ID
             AND B.BILL_ID = I.BILL_ID
             AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND A.STATUS = Pk00_Const.c_ACC_STATUS_BILL
           GROUP BY A.BILLING_ID, A.ACCOUNT_ID, A.ACCOUNT_TYPE, B.REP_PERIOD_ID, B.BILL_ID
      ), INV AS (
          SELECT A.BILLING_ID, A.ACCOUNT_ID,  A.ACCOUNT_TYPE, B.REP_PERIOD_ID, B.BILL_ID,
                 SUM(II.TOTAL) INV_TOTAL, SUM(II.TAX+II.GROSS) INV_CALC_TOTAL, 
                 SUM(II.TAX) INV_TAX, SUM(II.GROSS) INV_GROSS 
            FROM INVOICE_ITEM_T II, BILL_T B, ACCOUNT_T A
           WHERE B.REP_PERIOD_ID BETWEEN p_period_id_from AND p_period_id_to
             --AND B.TOTAL != 0
             AND B.ACCOUNT_ID = A.ACCOUNT_ID
             AND B.BILL_ID = II.BILL_ID
             AND B.REP_PERIOD_ID = II.REP_PERIOD_ID
             AND A.STATUS = Pk00_Const.c_ACC_STATUS_BILL
           GROUP BY A.BILLING_ID, A.ACCOUNT_ID, A.ACCOUNT_TYPE, B.REP_PERIOD_ID, B.BILL_ID
      ), R AS (
      SELECT B.BILLING_ID, B.ACCOUNT_ID, B.ACCOUNT_TYPE, B.REP_PERIOD_ID, 
             B.BILL_ID, B.BILL_NO, B.BILL_TYPE,
             B_TOTAL, INV_TOTAL, (I_GROSS + I_TAX) I_CALC_TOTAL,
             B_GROSS, INV_GROSS, I_GROSS,
             B_TAX, INV_TAX, I_TAX,
             CASE
              WHEN B_TOTAL != INV_TOTAL THEN -2
              WHEN ROUND(B_TOTAL,2) = ROUND((I_GROSS + I_TAX),2) THEN 0
              WHEN ABS(B_TOTAL - (I_GROSS + I_TAX)) BETWEEN 0 AND 0.019 THEN 1
              WHEN ABS(B_TOTAL - (I_GROSS + I_TAX)) BETWEEN 0.02 AND 0.029 THEN 2
              ELSE -1
             END B_TOTAL_STATE,
             B_TOTAL - (I_GROSS + I_TAX) B_TOTAL_DELTA,
             CASE
              WHEN B_GROSS != INV_GROSS THEN -2
              WHEN B_GROSS = I_GROSS THEN 0
              WHEN ABS(B_GROSS - I_GROSS) BETWEEN 0 AND 0.019 THEN 1
              WHEN ABS(B_TOTAL - I_GROSS) BETWEEN 0.02 AND 0.029 THEN 2
              ELSE -1
             END B_GROSS_STATE,
             B_GROSS - I_GROSS B_GROSS_DELTA
        FROM B, I, INV
      WHERE B.BILLING_ID   = I.BILLING_ID
        AND B.BILL_ID      = I.BILL_ID
        AND B.BILLING_ID   = INV.BILLING_ID
        AND B.BILL_ID      = INV.BILL_ID
        AND B.ACCOUNT_TYPE = I.ACCOUNT_TYPE
        AND B.ACCOUNT_TYPE = INV.ACCOUNT_TYPE
      )
      SELECT BILLING_ID, ACCOUNT_ID, ACCOUNT_TYPE, REP_PERIOD_ID, 
             BILL_ID, BILL_NO, BILL_TYPE,
             B_TOTAL, INV_TOTAL, I_CALC_TOTAL, B_TOTAL_STATE, B_TOTAL_DELTA,
             B_GROSS, INV_GROSS, I_GROSS, B_GROSS_STATE, B_GROSS_DELTA,
             B_TAX, INV_TAX, I_TAX 
        FROM R
       WHERE (B_TOTAL_STATE < 0 OR B_GROSS_STATE < 0  ) 
       ORDER BY BILLING_ID, ACCOUNT_TYPE, B_TOTAL_STATE;

    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg(v_table||' '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    COMMIT;

    Gather_Table_Stat(l_Tab_Name => v_table);

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ ���������� ����� B.TOTAL = B.GROSS + B.TAX 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_bills ( 
               p_recordset OUT t_refc, 
               p_period_id IN INTEGER -- ������� � ������ ������� �������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_bills';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
          SELECT A.BILLING_ID, B.CURRENCY_ID,
                 SUM(B.TOTAL) TOTAL, 
                 SUM(B.GROSS)+SUM(B.TAX) CALC_TOTAL,
                 SUM(B.GROSS) GROSS, SUM(B.TAX) TAX,
                 SUM(B.TOTAL)-(SUM(B.GROSS)+SUM(B.TAX)) DELTA,
                 COUNT(*) CNT
            FROM BILL_T B, ACCOUNT_T A
           WHERE B.ACCOUNT_ID = A.ACCOUNT_ID
             AND B.REP_PERIOD_ID = p_period_id
             AND A.BILLING_ID IN (2000,2001,2002,2006,2000,2003)
             AND B.BILL_STATUS IN ('CLOSED', 'READY')
             AND A.STATUS IN ('B','C')
          GROUP BY A.BILLING_ID, B.CURRENCY_ID
          ORDER BY A.BILLING_ID, B.CURRENCY_ID
      ;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ��� �� ITEM-� ����� � INVOICE_ITEM-� � ����� � ����� 
-- �������� ����� ��������� ������ ��� ������ ������������� � �������� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_items ( 
               p_recordset OUT t_refc, 
               p_period_id IN INTEGER -- ������� � ������ ������� �������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_items';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
      SELECT * 
        FROM (
          SELECT B.BILL_ID, B.BILL_TYPE, B.BILL_NO, B.BILL_STATUS, 
                 B.TOTAL BILL_TOTAL, I.BILL_TOTAL ITEM_BILL_TOTAL, V.TOTAL INV_ITEM_TOTAL, 
                 I.ITEM_ID, I.SERVICE_ID, I.INV_ITEM_ID I_INV_ITEM_ID,  
                 V.INV_ITEM_ID, I.BILL_TOTAL I_BILL_TOTAL
            FROM BILL_T B, ITEM_T I, INVOICE_ITEM_T V, ACCOUNT_T A
           WHERE B.REP_PERIOD_ID = p_period_id
             AND B.REP_PERIOD_ID = I.REP_PERIOD_ID(+)
             AND B.BILL_ID       = I.BILL_ID(+)
             AND B.REP_PERIOD_ID = V.REP_PERIOD_ID(+)
             AND B.BILL_ID       = V.BILL_ID(+)
             AND B.ACCOUNT_ID    = A.ACCOUNT_ID
             AND A.STATUS        = Pk00_Const.c_ACC_STATUS_BILL
          ORDER BY B.BILL_ID, I.ITEM_ID, I.INV_ITEM_ID
      )
      -- ��������� ������ �����
      WHERE NOT (BILL_TOTAL = 0 AND ITEM_ID IS NULL AND INV_ITEM_ID IS NULL) 
        AND (I_INV_ITEM_ID IS NULL OR I_BILL_TOTAL IS NULL);
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ��� ��� ���� INVOICE_ITEM-�� ���� ITEM-� 
-- �������� ����� ��������� ������ ��� ������ ������������� � �������� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_invoice_items ( 
               p_recordset OUT t_refc, 
               p_period_id IN INTEGER -- ������� � ������ ������� �������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_invoice_items';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
      SELECT B.BILL_NO, B.BILL_STATUS, B.BILL_TYPE, V.* 
        FROM INVOICE_ITEM_T V, BILL_T B
       WHERE V.REP_PERIOD_ID = 201606
         AND B.REP_PERIOD_ID = V.REP_PERIOD_ID
         AND B.BILL_ID       = V.BILL_ID
         AND B.BILL_STATUS   IN ('READY', 'CLOSED')
         AND NOT EXISTS (
           SELECT * FROM ITEM_T I
            WHERE I.REP_PERIOD_ID = V.REP_PERIOD_ID
              AND I.BILL_ID       = V.BILL_ID
              AND I.INV_ITEM_ID   = V.INV_ITEM_ID
         )
      ORDER BY BILL_NO;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ ��������� ������ � �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_bill_vat( 
               p_recordset OUT t_refc,
               p_period_id INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_bill_vat';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
      WITH V AS (
          SELECT REP_PERIOD_ID, BILL_ID, SUM(TOTAL) V_TOTAL, SUM(GROSS) V_GROSS, SUM(TAX) V_TAX 
            FROM INVOICE_ITEM_T V
           WHERE V.REP_PERIOD_ID = p_period_id
           GROUP BY REP_PERIOD_ID, BILL_ID
      ), CALC AS (
          SELECT B.REP_PERIOD_ID, 
                 B.BILL_ID, 
                 B.BILL_NO, 
                 V.V_TOTAL, 
                 B.TOTAL,  
                 V.V_GROSS,
                 B.GROSS, 
                 Pk09_Invoice.Calc_gross(B.TOTAL, 'Y', AP.VAT) C_GROSS,
                 V.V_TAX,
                 B.TAX, 
                 Pk09_Invoice.Calc_tax(B.TOTAL, 'Y', AP.VAT) C_TAX,
                 B.TAX - Pk09_Invoice.Calc_tax(B.TOTAL, 'Y', AP.VAT) DELTA_TAX,
                 B.TOTAL - V.V_TOTAL DELTA_INV_TOTAL
            FROM V, BILL_T B, ACCOUNT_PROFILE_T AP
           WHERE B.REP_PERIOD_ID = V.REP_PERIOD_ID
             AND B.BILL_ID       = V.BILL_ID
             AND B.PROFILE_ID    = AP.PROFILE_ID
      )
      SELECT * FROM CALC
       WHERE (TOTAL != V_TOTAL 
           OR GROSS != V_GROSS 
           OR TAX   != V_TAX
           OR GROSS != C_GROSS 
           OR TAX   != C_TAX
       );
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������ �������� ������ � ����������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Open_bill_list( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Open_bill_list';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
    WITH B AS (
        SELECT B.REP_PERIOD_ID, B.BILL_ID, B.BILL_NO, B.BILL_TYPE, B.BILL_DATE, B.BILL_STATUS,
               C.CONTRACT_NO, A.ACCOUNT_NO, CM.COMPANY_NAME, 
               CT.CONTRACTOR, BR.CONTRACTOR BRANCH
          FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP,
               CONTRACTOR_T CT, CONTRACTOR_T BR, CONTRACT_T C, COMPANY_T CM,
               PERIOD_T P
         WHERE P.POSITION     = 'BILL'
           AND B.REP_PERIOD_ID= P.PERIOD_ID
           AND B.ACCOUNT_ID   = A.ACCOUNT_ID
           AND A.BILLING_ID NOT IN (2000, 2008 )
           AND A.STATUS       = 'B'
           AND B.BILL_STATUS  NOT IN ( 'READY', 'CLOSED', 'PREPAID' )
           AND B.PROFILE_ID   = AP.PROFILE_ID
           AND AP.CONTRACTOR_ID = CT.CONTRACTOR_ID
           AND AP.BRANCH_ID   = BR.CONTRACTOR_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND CM.CONTRACT_ID = C.CONTRACT_ID
           AND CM.ACTUAL      = 'Y'
    )
    SELECT  B.REP_PERIOD_ID, B.BILL_ID, B.BILL_NO, B.BILL_DATE, B.BILL_STATUS, B.BILL_TYPE,
            SUM(I.ITEM_TOTAL) ITEM_TOTAL, MIN(I.ITEM_CURRENCY_ID) ITEM_CURRENCY_ID,
            B.CONTRACT_NO, B.COMPANY_NAME, B.CONTRACTOR, B.BRANCH
      FROM B, ITEM_T I
     WHERE B.REP_PERIOD_ID = I.REP_PERIOD_ID(+)
       AND B.BILL_ID = I.BILL_ID(+)   
     GROUP BY B.REP_PERIOD_ID, B.BILL_ID, B.BILL_NO, B.BILL_DATE, B.BILL_STATUS, B.BILL_TYPE,
              B.CONTRACT_NO, B.COMPANY_NAME, B.CONTRACTOR, B.BRANCH
     ORDER BY B.CONTRACTOR, B.BRANCH
    ;
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------- --
-- �������� ������ ��� ���������� �������� �� ������
-- ------------------------------------------------------------------------- --

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ ���������� ���������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_ABP ( 
               p_recordset OUT t_refc, 
               p_period_id  IN INTEGER, -- ������� � ������ ������� �������
               p_billing_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_ABP';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
      WITH OB AS (
      SELECT A.ACCOUNT_ID, O.ORDER_ID, OB.ORDER_BODY_ID,
             OB.RATE_RULE_ID, OB.RATE_VALUE, OB.QUANTITY, OB.CURRENCY_ID, OB.TAX_INCL,
             OB.DATE_FROM, OB.DATE_TO, A.ACCOUNT_NO, O.ORDER_NO, 
             O.DATE_FROM O_DATE_FROM, O.DATE_TO O_DATE_TO
        FROM ACCOUNT_T A, ORDER_T O, ORDER_BODY_T OB
       WHERE A.BILLING_ID = p_billing_id
         AND A.ACCOUNT_ID = O.ACCOUNT_ID
         AND O.ORDER_ID   = OB.ORDER_ID
         AND OB.CHARGE_TYPE = 'REC'
         AND OB.DATE_FROM < PK04_PERIOD.PERIOD_TO(p_period_id)
         AND (OB.DATE_TO IS NULL OR PK04_PERIOD.PERIOD_FROM(p_period_id) < OB.DATE_TO)
         AND O.DATE_FROM < PK04_PERIOD.PERIOD_TO(p_period_id)
         AND (O.DATE_TO IS NULL OR PK04_PERIOD.PERIOD_FROM(p_period_id) < O.DATE_TO)  
      ),
      IT AS (
      SELECT B.ACCOUNT_ID, B.BILL_ID, I.ITEM_ID, I.REP_PERIOD_ID, B.BILL_TYPE, I.ITEM_TYPE, 
             I.BILL_TOTAL, I.TAX_INCL, I.ORDER_ID, I.ORDER_BODY_ID, B.BILL_NO
        FROM BILL_T B, ITEM_T I
       WHERE B.REP_PERIOD_ID = I.REP_PERIOD_ID
         AND B.BILL_ID = I.BILL_ID
         AND I.CHARGE_TYPE = 'REC'
         AND B.REP_PERIOD_ID = p_period_id
      )
      SELECT ORDER_NO, BILL_NO, BILL_TYPE, CURRENCY_ID, TAX_INCL,  
                 RATE_VALUE, RATE_YE,
                 BILL_TOTAL, IT_TAX_INCL, RATE_RULE_ID,
                 OB_DATE_FROM, OB_DATE_TO, 
                 O_DATE_FROM, O_DATE_TO, QUANTITY 
        FROM (
          SELECT OB.ORDER_NO, IT.BILL_NO, IT.BILL_TYPE, OB.CURRENCY_ID, OB.TAX_INCL,  
                 OB.RATE_VALUE, DECODE(OB.CURRENCY_ID, 286, OB.RATE_VALUE * 28.6, OB.RATE_VALUE) RATE_YE,
                 IT.BILL_TOTAL, IT.TAX_INCL IT_TAX_INCL, OB.RATE_RULE_ID,
                 OB.DATE_FROM OB_DATE_FROM, OB.DATE_TO OB_DATE_TO, 
                 OB.O_DATE_FROM, OB.O_DATE_TO, OB.QUANTITY  
            FROM OB, IT
           WHERE OB.ORDER_ID = IT.ORDER_ID(+)
             AND OB.ORDER_BODY_ID = IT.ORDER_BODY_ID(+)
         )
      WHERE BILL_TOTAL <> ROUND((RATE_YE * QUANTITY),2)
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ ���������� ������� �� ����������� ���������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_MIN ( 
               p_recordset OUT t_refc, 
               p_period_id  IN INTEGER, -- ������� � ������ ������� �������
               p_billing_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_MIN';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
        WITH OB AS (
        SELECT A.ACCOUNT_ID, O.ORDER_ID, OB.ORDER_BODY_ID,
               OB.RATE_RULE_ID, OB.RATE_VALUE, OB.QUANTITY, OB.CURRENCY_ID, OB.TAX_INCL,
               OB.DATE_FROM, OB.DATE_TO, A.ACCOUNT_NO, O.ORDER_NO,
               O.DATE_FROM O_DATE_FROM, O.DATE_TO O_DATE_TO 
          FROM ACCOUNT_T A, ORDER_T O, ORDER_BODY_T OB
         WHERE A.BILLING_ID = p_billing_id
           AND A.ACCOUNT_ID = O.ACCOUNT_ID
           AND O.ORDER_ID   = OB.ORDER_ID
           AND OB.CHARGE_TYPE = 'MIN'
           AND OB.DATE_FROM < PK04_PERIOD.PERIOD_TO(p_period_id)
           AND (OB.DATE_TO IS NULL OR PK04_PERIOD.PERIOD_FROM(p_period_id) < OB.DATE_TO)
           AND O.DATE_FROM < PK04_PERIOD.PERIOD_TO(p_period_id)
           AND (O.DATE_TO IS NULL OR PK04_PERIOD.PERIOD_FROM(p_period_id) < O.DATE_TO)  
        ),
        IT AS (
        SELECT B.ACCOUNT_ID, B.BILL_ID, I.ITEM_ID, I.REP_PERIOD_ID, B.BILL_TYPE, I.ITEM_TYPE, 
               I.BILL_TOTAL, I.TAX_INCL, I.ORDER_ID, I.ORDER_BODY_ID, B.BILL_NO
          FROM BILL_T B, ITEM_T I
         WHERE B.REP_PERIOD_ID = I.REP_PERIOD_ID
           AND B.BILL_ID = I.BILL_ID
           AND I.CHARGE_TYPE = 'MIN'
           AND B.REP_PERIOD_ID = p_period_id
        ), 
        USG AS (
        SELECT B.BILL_NO, B.BILL_TYPE, I.TAX_INCL, I.ORDER_ID, SUM(I.BILL_TOTAL) USG_TOTAL
          FROM BILL_T B, ITEM_T I
         WHERE B.REP_PERIOD_ID = I.REP_PERIOD_ID
           AND B.BILL_ID = I.BILL_ID
           AND I.CHARGE_TYPE = 'USG'
           AND B.REP_PERIOD_ID = p_period_id
         GROUP BY B.BILL_NO, B.BILL_TYPE, I.TAX_INCL, I.ORDER_ID
        )
        SELECT * FROM (
            SELECT OB.ORDER_NO, 
                   NVL(IT.BILL_NO, USG.BILL_NO) BILL_NO, 
                   NVL(IT.BILL_TYPE, USG.BILL_TYPE) BILL_TYPE, 
                   OB.CURRENCY_ID, OB.TAX_INCL,  
                   OB.RATE_VALUE, DECODE(OB.CURRENCY_ID, 286, OB.RATE_VALUE * 28.6, OB.RATE_VALUE) RATE_YE,
                   IT.BILL_TOTAL, USG.USG_TOTAL, 
                   NVL(IT.TAX_INCL, USG.TAX_INCL) IT_TAX_INCL, 
                   OB.RATE_RULE_ID,
                   OB.DATE_FROM, OB.DATE_TO,
                   OB.O_DATE_FROM, OB.O_DATE_TO  
              FROM OB, IT, USG
             WHERE OB.ORDER_ID = IT.ORDER_ID(+)
               AND OB.ORDER_BODY_ID = IT.ORDER_BODY_ID(+)
               AND OB.ORDER_ID = USG.ORDER_ID(+)
        )
        WHERE RATE_VALUE <> (BILL_TOTAL + USG_TOTAL)
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ ����������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_IDL ( 
               p_recordset OUT t_refc, 
               p_period_id IN INTEGER -- ������� � ������ ������� �������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_IDL';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
      SELECT D.ORDER_NO, B.BILL_NO, D.VALUE, D.FLAGS, D.CHARGE_TYPE, 
             I.BILL_TOTAL, OB.CURRENCY_ID, R.BILL_TOTAL REC_TOTAL, 
             ROUND((R.BILL_TOTAL * ROUND(D.VALUE) / 720), 2) IDL_TEST,
             I.CHARGE_TYPE I_CHARGE_TYPE  
        FROM DOWNTIME_T D, ITEM_T I, ITEM_T R, BILL_T B, ORDER_BODY_T OB
       WHERE D.REP_PERIOD_ID = p_period_id
         AND D.ORDER_BODY_ID = I.ORDER_BODY_ID(+) 
         AND I.CHARGE_TYPE(+) IN ('IDL','SLA')
         AND I.REP_PERIOD_ID = p_period_id
         AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
         AND I.BILL_ID       = B.BILL_ID
         AND D.ACCOUNT_ID    = B.ACCOUNT_ID
         AND D.ORDER_BODY_ID = OB.ORDER_BODY_ID
         AND R.REP_PERIOD_ID = B.REP_PERIOD_ID
         AND R.BILL_ID       = B.BILL_ID
         AND R.CHARGE_TYPE   = 'REC'
         AND R.ORDER_ID      = D.ORDER_ID
         AND (I.BILL_TOTAL + ROUND((R.BILL_TOTAL * ROUND(D.VALUE) / 720), 2)) <> 0
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������� �� �������� � ��������� ��������
PROCEDURE Check_payments( 
               p_recordset OUT t_refc,
               p_period_id  IN INTEGER -- ��� ������ �������� �����
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_payments';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
      WITH P AS (
        SELECT P.REP_PERIOD_ID, 
               ROUND(SUM(P.RECVD)) SUM_RECVD, 
               ROUND(SUM(P.TRANSFERED)) P_SUM_TRANSFERED, 
               ROUND(SUM(P.ADVANCE)) SUM_ADVANCE, 
               COUNT(*) NUM_PAYMENTS 
          FROM PAYMENT_T P
        GROUP BY P.REP_PERIOD_ID
        ORDER BY P.REP_PERIOD_ID DESC
      ), T AS (
        SELECT PT.PAY_PERIOD_ID, 
               ROUND(SUM(TRANSFER_TOTAL)) T_SUM_TRANSFER_TOTAL, 
               COUNT(*) NUM_TRANSFERS 
          FROM PAY_TRANSFER_T PT
        GROUP BY PT.PAY_PERIOD_ID
        ORDER BY PT.PAY_PERIOD_ID DESC
      )
      SELECT P.REP_PERIOD_ID, P.SUM_RECVD, P.P_SUM_TRANSFERED, T.T_SUM_TRANSFER_TOTAL, T.NUM_TRANSFERS 
        FROM P, T
       WHERE P.REP_PERIOD_ID = T.PAY_PERIOD_ID
         AND P.REP_PERIOD_ID >= p_period_id
         AND T.PAY_PERIOD_ID >= p_period_id
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ����� ������ �� ������ ������� BILL_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_bill_payments( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_bill_payments';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
        SELECT REP_PERIOD_ID, NUM_ROWS, TOTAL, RECVD,
               CASE 
                WHEN TOTAL != 0 THEN ROUND(-(DUE/TOTAL)*100,2) 
                ELSE 0
               END PRC_DUE,
               CASE 
                WHEN TOTAL != 0 THEN ROUND(-(RECVD/TOTAL)*100,2) 
                ELSE 0
               END PRC_RECVD
          FROM (
            SELECT REP_PERIOD_ID, 
                   ROUND(SUM(B.TOTAL)) TOTAL,
                   ROUND(SUM(B.RECVD)) RECVD,
                   ROUND(SUM(B.DUE)) DUE,
                   COUNT(*) NUM_ROWS   
              FROM BILL_T B
                GROUP BY REP_PERIOD_ID
                ORDER BY REP_PERIOD_ID DESC
        );
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� �������� �� ������������ ������ 
-- �������� ����� ��������� ������ ��� ������ ������������� � �������� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_period_info( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER    -- ���-�� ������� �����
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_period_info';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
        WITH BILL AS 
        ( SELECT REP_PERIOD_ID, ACCOUNT_ID, SUM(TOTAL) TOTAL 
            FROM BILL_T
           WHERE REP_PERIOD_ID = p_rep_period_id
           GROUP BY REP_PERIOD_ID, ACCOUNT_ID
        ),
        REP AS 
        ( SELECT REP_PERIOD_ID, ACCOUNT_ID, SUM(TOTAL) TOTAL 
            FROM REP_PERIOD_INFO_T
           WHERE REP_PERIOD_ID = p_rep_period_id
           GROUP BY REP_PERIOD_ID, ACCOUNT_ID 
        )
        SELECT B.REP_PERIOD_ID,             -- ID �������
               B.ACCOUNT_ID,                -- ID �������� �����
               A.TOTAL PERIOD_INFO_TOTAL,   -- ����� ���������� �� ������ �� ��������
               B.TOTAL BILLS_TOTAL          -- ����� ���������� �� ������ �� ������
          FROM REP A, BILL B
        WHERE A.REP_PERIOD_ID(+) = B.REP_PERIOD_ID
          AND A.ACCOUNT_ID(+) = B.ACCOUNT_ID
          AND (A.TOTAL IS NULL OR B.TOTAL IS NULL OR A.TOTAL != B.TOTAL);
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ��� 1�, ����� ����������� ������ �� ����������� � BRM � 1� 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Check_ErpCode( 
               p_recordset OUT t_refc, 
               p_period_id  IN INTEGER    -- ID ��������� �������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_ErpCode';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
      WITH RP AS (
      SELECT B.REP_PERIOD_ID, B.BILL_NO, A.ACCOUNT_NO, C.CONTRACT_NO, C.DATE_FROM CONTRACT_DATE,
             CM.COMPANY_NAME, CM.ERP_CODE, CM.INN, AP.KPP, CT.CONTRACTOR, BR.CONTRACTOR BRANCH, A.BILLING_ID 
        FROM BILL_T B, ACCOUNT_T A, CONTRACT_T C, ACCOUNT_PROFILE_T AP, 
             CONTRACTOR_T CT, CONTRACTOR_T BR, COMPANY_T CM 
       WHERE B.REP_PERIOD_ID = p_period_id
         AND B.ACCOUNT_ID    = A.ACCOUNT_ID 
         AND B.CONTRACT_ID   = C.CONTRACT_ID 
         AND B.PROFILE_ID    = AP.PROFILE_ID
         AND AP.CONTRACTOR_ID= CT.CONTRACTOR_ID
         AND AP.BRANCH_ID    = BR.CONTRACTOR_ID
         AND B.CONTRACT_ID   = CM.CONTRACT_ID(+)
         AND CM.ACTUAL(+)    = 'Y'
         AND A.ACCOUNT_TYPE  = 'J'
         AND A.BILLING_ID NOT IN (2008)
       ORDER BY C.CONTRACT_NO, A.ACCOUNT_NO
      )
      SELECT * 
        FROM RP;
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

--=======================================================================================
-- �������� �������� �� ������� �����������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� � �������� �������
--   - ������������� - ���-�� ��������� �������
--   - ��� ������ ���������� ����������
PROCEDURE Period_list( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Period_list';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
        SELECT P.POSITION,        -- ������� �������
               P.PERIOD_ID,       -- ID ������������ ������
               P.PERIOD_FROM,     -- ���� ������ ���������� �������
               P.PERIOD_TO,       -- ���� ��������� ���������� ������� 
               P.CLOSE_REP_PERIOD -- ���� ���������� �������� �������
          FROM PERIOD_T P
         WHERE POSITION IS NOT NULL
        ORDER BY PERIOD_ID;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ��������� ��� ��������� ������� � ������� ����������� 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Msg_list(
              p_recordset OUT t_refc,
              p_function   IN VARCHAR2,                   -- ��� �������
              p_date_from  IN DATE DEFAULT (SYSDATE-30)   -- ����� ������ ������� (���������������)
           )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Msg_list';
    v_ssid    INTEGER; 
    v_id_from INTEGER; 
    v_id_to   INTEGER;
    v_retcode INTEGER;
BEGIN
    -- ���������� ������
    SELECT MAX(SSID), MIN(L01_ID), MAX(L01_ID) 
      INTO v_ssid, v_id_from, v_id_to
      FROM L01_MESSAGES
     WHERE MSG_SRC LIKE c_PkgName||'.'||p_function||'%'
       AND p_date_from < MSG_DATE 
       AND (MESSAGE LIKE 'Start%' OR MESSAGE LIKE 'Stop%');  
    -- �����������
    IF v_id_from IS NULL THEN
        OPEN p_recordset FOR
            SELECT 0, 'E', SYSDATE, '�� ������� ������: "Start" ', c_PkgName||'.'||p_function, TO_CHAR(NULL) 
              FROM DUAL;
    ELSIF v_id_from = v_id_to THEN -- �� ������� ������ ���� (������� ��� ������������)
        -- ���������� ������ �� ������ ������, ������� �� ������� ������ ������� 
        OPEN p_recordset FOR
            SELECT L01_ID,     -- ID ��������� � ������� �����������
                   MSG_LEVEL,  -- ������� ���������
                   MSG_DATE,   -- ���� ���������
                   MESSAGE,    -- ����� ���������
                   MSG_SRC,    -- �������� ��������� ����� + ���������
                   APP_USER    -- ������������ ����������
              FROM L01_MESSAGES L
             WHERE SSID = v_ssid
               AND L01_ID >= v_id_from
             ORDER BY L01_ID;
    ELSE
        -- ���������� ������ �� ������ ��������� �������, ����� �������� �-�� 
        OPEN p_recordset FOR
            SELECT L01_ID,     -- ID ��������� � ������� �����������
                   MSG_LEVEL,  -- ������� ���������
                   MSG_DATE,   -- ���� ���������
                   MESSAGE,    -- ����� ���������
                   MSG_SRC,    -- �������� ��������� ����� + ���������
                   APP_USER    -- ������������ ���������� 
              FROM L01_MESSAGES
             WHERE SSID = v_ssid
               AND L01_ID BETWEEN v_id_from AND v_id_to
             ORDER BY L01_ID;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������� �������� ������������� �� ��������� p_months �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Billing_history( 
               p_recordset     OUT t_refc, 
               p_from_period_id IN INTEGER -- ������� � ������ ������� �������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing_history';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
        SELECT B.REP_PERIOD_ID,       -- �������� ������
               P.POSITION PERIOD_TYPE,-- ������� ��������� �������
               A.ACCOUNT_TYPE,        -- ��� �������� �����
               B.BILL_STATUS,         -- ������ �����
               SUM(B.TOTAL) TOTAL,    -- ����� ����� ���������� �� ������ � ��������
               SUM(B.GROSS) GROSS,    -- ����� ����� ���������� �� ������ ��� �������
               SUM(B.TAX) TAX,        -- ����� ����� �������
               SUM(B.RECVD) RECVD,    -- ����� ����� ������ �� ������
               COUNT(*) NUM           -- ���-�� ������������ ������
          FROM BILL_T B, ACCOUNT_T A, PERIOD_T P
         WHERE A.ACCOUNT_ID = B.ACCOUNT_ID 
           AND B.REP_PERIOD_ID = P.PERIOD_ID
           AND P.PERIOD_ID >= p_from_period_id
        GROUP BY B.REP_PERIOD_ID, P.POSITION, A.ACCOUNT_TYPE, B.BILL_STATUS
         ORDER BY A.ACCOUNT_TYPE, B.REP_PERIOD_ID, B.BILL_STATUS DESC;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;



-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� �������� �� ��������, �� ������ ������� REP_PERIOD_INFO_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE View_period_info( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'View_period_info';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
        SELECT REP_PERIOD_ID, NUM_ROWS, TOTAL, RECVD, ADVANCE,
               CASE 
                WHEN TOTAL != 0 THEN ROUND((RECVD/TOTAL)*100,2) 
                ELSE 0
               END PRC_RECVD,
               CASE 
                WHEN TOTAL != 0 THEN ROUND((ADVANCE/TOTAL)*100,2) 
                ELSE 0
               END PRC_ADVANCE 
          FROM (
            SELECT RP.REP_PERIOD_ID,  
                   ROUND(SUM(RP.TOTAL)) TOTAL,
                   ROUND(SUM(RP.RECVD)) RECVD,
                   ROUND(SUM(RP.ADVANCE)) ADVANCE,
                   COUNT(*) NUM_ROWS
              FROM REP_PERIOD_INFO_T RP
            GROUP BY RP.REP_PERIOD_ID
            ORDER BY REP_PERIOD_ID DESC
        );
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;



-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������ ������, ���������� � ������ ������ 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Err_bill_list( 
               p_recordset OUT t_refc, 
               p_period_id  IN INTEGER    -- ID ��������� �������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Err_bill_list';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR 
        SELECT A.ACCOUNT_ID,     -- ID �������� �����
               A.ACCOUNT_NO,     -- ����� �������� �����
               A.ACCOUNT_TYPE,   -- ��� �������� �����
               B.REP_PERIOD_ID,  -- ID ������� �����
               B.BILL_NO,        -- ����� �����
               B.BILL_ID,        -- ID �����
               B.BILL_STATUS,    -- ������ �����
               B.CALC_DATE       -- ���� ������������ ������� �� �����  
        FROM BILL_T B, ACCOUNT_T A 
        WHERE B.BILL_STATUS = PK00_CONST.c_BILL_STATE_ERROR
          AND A.ACCOUNT_ID = B.ACCOUNT_ID
          AND B.REP_PERIOD_ID = p_period_id;
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('Stop.ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK31_BILLING_CTRL;
/
