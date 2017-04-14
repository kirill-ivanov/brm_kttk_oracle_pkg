CREATE OR REPLACE PACKAGE PK25_REPORT_1C IS
  
TYPE t_refc IS REF CURSOR;
 
c_PkgName     CONSTANT VARCHAR2(30) := 'PK25_EXPORT_TO_1C';

-- ------------------------------------------------------------------------- --
-- ERP - ����, ����������� � �������� �� ��������� �����
-- ------------------------------------------------------------------------- --
PROCEDURE New_erp_codes(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc);

-- ======================================================================= --
-- ������ ��������� ������������ BRM 
-- ======================================================================= --
PROCEDURE CONTRACT_NO_LIST( p_recordset OUT t_refc );

-- ======================================================================= --
-- ������ ������������ BRM 
-- ======================================================================= --
PROCEDURE COMPANY_LIST( p_recordset OUT t_refc );
  
-- ======================================================================= --
-- ������ ������������ ��� �������� � 1�
-- ��� ������� ������������ � 1�
-- ======================================================================= --
PROCEDURE COMPANY_LIST_BY_TPI( p_recordset OUT t_refc );

-- ======================================================================= --
-- ������ ��������� ��� �������� � 1�
-- ��� ������� ������������ � 1�
-- ======================================================================= --
PROCEDURE CONTRACT_LIST_BY_TPI( p_recordset OUT t_refc );

-- ======================================================================= --
-- �������� ������������ � ������� ��� ������ 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Load_accounts_queue( 
              p_from_period_id INTEGER,  -- ������ ������ ���� ������
              p_to_period_id   INTEGER   -- ������ ����� ���� ������
          );

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������ ������������ BRM ��� ������
PROCEDURE BRM_erp_code_list(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������ ��������� ������������ BRM ��� ������
PROCEDURE BRM_erp_contract_list(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- ======================================================================= --
-- ��� ������ (����������) 
-- ��������� ������� ������������ �� ��������� � �������
-- � ������� ���� ������, �� ��������� ������ � p_from_period_id �� p_to_period_id:
-- ERP_CODE,CONTRACT_NO,IN_BALANCE,BILL_TOTAL,RECVD,OUT_BALANCE
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Load_BRM_akt_data;

-- ����������� ���� ������ �� �/�
PROCEDURE Act_report_account( 
            p_result     OUT VARCHAR2,  
            p_recordset  OUT t_refc,
            p_account_no  IN VARCHAR2 DEFAULT NULL,
            p_contract_no IN VARCHAR2 DEFAULT NULL,
            p_erp_code    IN VARCHAR2 DEFAULT NULL
          );
          
-- ����������� ���� ������ �� �������� �����������
PROCEDURE Act_report_contract( 
            p_result     OUT VARCHAR2,  
            p_recordset  OUT t_refc,
            p_contract_no IN VARCHAR2 DEFAULT NULL,
            p_erp_code    IN VARCHAR2 DEFAULT NULL
          );

-- ����������� ���� ������ �� �������� �����������
PROCEDURE Act_report_erp_code( 
            p_result     OUT VARCHAR2,  
            p_recordset  OUT t_refc,
            p_erp_code    IN VARCHAR2 DEFAULT NULL
          );
          
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��� ������ (���������� 1� - ��������) 
-- ������ � ������� PK25_BAL_1C_T, ��������� ��������������, 
-- � ������� PLSQL Developer, ��� ������, 
-- ����� �������� NUMBER
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Load_1C_akt_data;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� �� 1�, � ������� ���� ������
--
PROCEDURE Export_1�_data(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ���������� �� �������� � ������� ��������� � BRM � 1�
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Common_stat_by_contract(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- ======================================================================= --
-- �������� ������ BRM �� ��������� ������� ��� ���������� ������ 
-- ======================================================================= --
PROCEDURE Load_BRM_bills;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������ �� ������������ �� ������ ������ � BRM �� ������� PK25_REPORT_BILL_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 1) ����� �� ���� ������ BRM, ������������ � ���������� � �����, 
--    ( ������ �� ������ ��������� ��� ������ ������������ )
PROCEDURE R1_BRM_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- 2) �������� � ������ ������������ � BRM � ������ ���������� � 1� 
PROCEDURE R2_BRM_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- 3) �������� � ������ ������ ������������ � BRM � ������ ���������� � 1�
PROCEDURE R3_BRM_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- 5) ����������� ������������ � BRM ������ �� ��������� 
PROCEDURE R5_BRM_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- 7) ����������� ������������ � BRM ������ �� ������������ 
PROCEDURE R7_BRM_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������ �� ������������ �� ������ ������ � BRM �� ������� PK25_REPORT_BILL_T
-- � ���������� �� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 1) ��������� �������� ������ �� ��������� ������������ � BRM � 1�
PROCEDURE R1_BRM_vs_1C_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- 2) ���������� �� ������� �������� ������ �� ��������� ������������ � BRM � 1�
PROCEDURE R2_BRM_vs_1C_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- 3) ��������� �������� ������ ������������ � BRM � 1�
PROCEDURE R3_BRM_vs_1C_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- 4) ���������� �� ������� �������� ������ ������������ � BRM � 1�
PROCEDURE R4_BRM_vs_1C_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- 5) ��������� ���������� � ������� ��������� �� ������� ��������� 
-- �� ������ �� ��������� ������������ � BRM � 1�
PROCEDURE R5_BRM_vs_1C_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- ========================================================================= --
-- ������ �������� � BRM � 1�
-- ------------------------------------------------------------------------- --
-- 1) ������� �������� � BRM �� ������
PROCEDURE RP1_BRM_payments(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- 2) ��������� �������� �� �������� ����������� �������� � BRM � 1�
PROCEDURE RP2_BRM_cont_pay_comp(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- 3) ���������� �� ����������� ��������� �������� �� �������� �����������
PROCEDURE RP3_BRM_cont_pay_stat(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );
          
-- 4) ��������� �������� �� ����������� �������� � BRM � 1�
PROCEDURE RP4_BRM_ecode_pay_comp(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

-- 5) ���������� �� ����������� ��������� �������� �� �����������
PROCEDURE RP5_BRM_ecode_pay_stat(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );
          
-- ------------------------------------------------------------------------- --
-- 6) ��������� ��������� �������� �� �������� ����������� �������� � BRM � 1�
-- ------------------------------------------------------------------------- --
PROCEDURE RP6_BRM_detail_pay_comp(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc );

END PK25_REPORT_1C;
/
CREATE OR REPLACE PACKAGE BODY PK25_REPORT_1C IS

-- ------------------------------------------------------------------------- --
-- ERP - ����, ����������� � �������� �� ��������� �����
-- ------------------------------------------------------------------------- --
PROCEDURE New_erp_codes(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc)
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'New_erp_codes';
BEGIN   
     OPEN p_recordset FOR
        SELECT DISTINCT C.CONTRACT_NO, CM.ERP_CODE, CM.INN, CM.KPP, 
               CM.COMPANY_NAME, CM.DATE_FROM, CM.DATE_TO 
          FROM COMPANY_T CM, CONTRACT_T C
         WHERE CM.DATE_FROM > TRUNC(SYSDATE,'mm')
           AND CM.COMPANY_NAME != '���. ����'
           AND CM.CONTRACT_ID = C.CONTRACT_ID
        ORDER BY COMPANY_NAME, ERP_CODE
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

--=======================================================================
-- ������ ��������� ������������ BRM 
--=======================================================================
PROCEDURE CONTRACT_NO_LIST( p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'CONTRACT_NO_LIST';
BEGIN   
     OPEN p_recordset FOR
        WITH CMP AS (
            SELECT --B.PROFILE_ID, 
                   --CM.COMPANY_ID, CM.COMPANY_NAME, CM.INN, CM.KPP, 
                   CM.ERP_CODE, C.CONTRACT_NO, C.DATE_FROM,
                   MIN(B.REP_PERIOD_ID) MIN_BILL_PERIOD_ID, 
                   MAX(B.REP_PERIOD_ID) MAX_BILL_PERIOD_ID, 
                   COUNT(*) NUM_BILLS, SUM (TOTAL) SUM_TOTAL 
              FROM BILL_T B, COMPANY_T CM, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C
             WHERE B.CONTRACT_ID = CM.CONTRACT_ID
               AND B.ACCOUNT_ID  = A.ACCOUNT_ID
               AND A.BILLING_ID != 2003
               AND B.PROFILE_ID  = AP.PROFILE_ID
               AND B.CONTRACT_ID = C.CONTRACT_ID
               AND AP.CONTRACTOR_ID = 1
             GROUP BY --CM.COMPANY_ID, CM.COMPANY_NAME, CM.INN, CM.KPP, 
                      CM.ERP_CODE, C.CONTRACT_NO, C.DATE_FROM
             HAVING SUM(TOTAL) > 0 AND MAX(B.REP_PERIOD_ID) >= 201401
             ORDER BY MAX(B.REP_PERIOD_ID) 
        )
        SELECT DISTINCT 
               ERP_CODE, CONTRACT_NO, TO_CHAR(DATE_FROM,'dd.mm.yyyy') CONTRACT_FROM, 
               MIN_BILL_PERIOD_ID, MAX_BILL_PERIOD_ID 
          FROM CMP
         WHERE CMP.ERP_CODE IS NOT NULL
         ORDER BY ERP_CODE, CONTRACT_NO
        ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

--=======================================================================
-- ������ ������������ BRM 
--=======================================================================
PROCEDURE COMPANY_LIST( p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'COMPANY_LIST';
BEGIN   
     OPEN p_recordset FOR
        WITH CMP AS (
            SELECT  
                   CM.ERP_CODE, CM.INN, CM.KPP, 
                   MIN(B.REP_PERIOD_ID) MIN_BILL_PERIOD_ID, 
                   MAX(B.REP_PERIOD_ID) MAX_BILL_PERIOD_ID, 
                   COUNT(*) NUM_BILLS, SUM (TOTAL) SUM_TOTAL,
                   CM.COMPANY_NAME, CM.COMPANY_ID, B.PROFILE_ID 
              FROM BILL_T B, COMPANY_T CM, ACCOUNT_T A
             WHERE B.CONTRACT_ID = CM.CONTRACT_ID
               AND B.ACCOUNT_ID = A.ACCOUNT_ID
               AND A.BILLING_ID NOT IN (2003)
             GROUP BY B.PROFILE_ID, CM.COMPANY_ID, CM.COMPANY_NAME, CM.INN, CM.KPP, CM.ERP_CODE
             HAVING SUM(TOTAL) > 0
        )
        SELECT * FROM CMP
         WHERE COMPANY_NAME NOT IN ('���. ����', '���������� ����')
         ORDER BY DECODE(CMP.ERP_CODE, NULL, 0, 1), 
                  MAX_BILL_PERIOD_ID DESC, COMPANY_NAME, ERP_CODE
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

--=======================================================================
-- ������ ������������ ��� �������� � 1�
--=======================================================================
PROCEDURE COMPANY_LIST_BY_TPI( p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'COMPANY_LIST_BY_TPI';
BEGIN   
     OPEN p_recordset FOR
      WITH TI AS (
          SELECT * FROM (
            SELECT G.GROUP_ID,
                   G.NAME,
                   G.TABLE_NAME_H,
                   G.TABLE_NAME_L,
                   PROV.CONTRACTOR_ID,
                   PROV.CONTRACTOR,
                   NULL BRANCH_ID,
                   NULL BRANCH,
                   AP.PROFILE_ID,
                   A.ACCOUNT_ID,
                   A.ACCOUNT_NO,
                   AP.CONTRACT_ID,
                   AP.KPP,
                   A.BILLING_ID
              FROM EXPORT_1C_GROUP_T G,
                   EXPORT_1C_GROUP_CONTRACTOR_T gc,
                   CONTRACTOR_T PROV,
                   ACCOUNT_T A,         
                   ACCOUNT_PROFILE_T AP
             WHERE G.ACTUAL = 'Y'
                   AND G.billing_id IS NULL
                   AND G.GROUP_ID = GC.GROUP_ID
                   AND PROV.CONTRACTOR_ID = GC.CONTRACTOR_ID
                   AND A.BILLING_ID IN (2001,2002)
                   AND A.ACCOUNT_TYPE = 'J'
                   AND A.STATUS <> 'T' 
                   AND AP.ACCOUNT_ID = A.ACCOUNT_ID
                   AND AP.ACTUAL = 'Y'
                   AND GC.CONTRACTOR_ID = AP.CONTRACTOR_ID         
          UNION ALL 
            SELECT G.GROUP_ID,
                   G.NAME,
                   G.TABLE_NAME_H,
                   G.TABLE_NAME_L,
                   PROV.CONTRACTOR_ID,
                   PROV.CONTRACTOR,
                   BR.CONTRACTOR_ID BRANCH_ID,
                   BR.CONTRACTOR BRANCH,
                   AP.PROFILE_ID,
                   A.ACCOUNT_ID,
                   A.ACCOUNT_NO,
                   AP.CONTRACT_ID,
                   AP.KPP,
                   A.BILLING_ID
              FROM EXPORT_1C_GROUP_T g,
                   EXPORT_1C_GROUP_CONTRACTOR_T gc,
                   CONTRACTOR_T PROV,
                   CONTRACTOR_T BR,
                   ACCOUNT_T A,         
                   ACCOUNT_PROFILE_T AP
             WHERE G.ACTUAL = 'Y'
                   AND G.billing_id = 2008
                   AND G.GROUP_ID = gc.GROUP_ID
                   AND prov.contractor_id = GC.CONTRACTOR_ID
                   AND br.CONTRACTOR_ID = gc.BRANCH_ID
                   AND (A.BILLING_ID IN (2005, 2006, 2007) OR (A.BILLING_ID IN (2004) AND AP.BRANCH_ID = 312))
                   AND A.ACCOUNT_TYPE = 'J'
                   AND A.STATUS <> 'T' 
                   AND AP.ACCOUNT_ID = A.ACCOUNT_ID
                   AND AP.ACTUAL = 'Y'
                   AND GC.CONTRACTOR_ID = AP.CONTRACTOR_ID
                   AND GC.BRANCH_ID = AP.BRANCH_ID         
          )         
          ORDER BY GROUP_ID
      )
      SELECT  
             CM.COMPANY_NAME,                  -- ��� ��������
             CM.ERP_CODE,                      -- ��� �������� � 1�
             CM.INN,                           -- ��� ��������
             CM.KPP COMPANY_KPP,               -- ��� ��������
             DECODE(TI.KPP, CM.KPP, NULL, TI.KPP) COMPANY_BRANCH_KPP, -- ��� ������� ��������
             TI.CONTRACTOR,                    -- �������� ( ���������� 1� )
             TI.BRANCH CONTRACTOR_BRANCH,      -- ������ ��������
             TI.NAME TPI_GROUP,                -- ������ � TPI 1C
             TI.TABLE_NAME_H TPI_TABLE_NAME_H, -- ������� ���������� �������� � TPI 
             TI.TABLE_NAME_L TPI_TABLE_NAME_L, -- ������� ����� �������� � TPI
             MAX(B.REP_PERIOD_ID) LAST_BILL_PERIOD -- ������ ���������� ������������� �����
        FROM TI, COMPANY_T CM, BILL_T B
       WHERE CM.ACTUAL = 'Y'
         AND CM.CONTRACT_ID = TI.CONTRACT_ID
         AND TI.ACCOUNT_ID = B.ACCOUNT_ID
      GROUP BY CM.COMPANY_NAME, CM.ERP_CODE, CM.INN, CM.KPP, 
               DECODE(TI.KPP, CM.KPP, NULL, TI.KPP),
               TI.CONTRACTOR, TI.BRANCH,
               TI.NAME, TI.TABLE_NAME_H, TI.TABLE_NAME_L
      ORDER BY DECODE(TI.NAME, '����', '1', TI.NAME), MAX(B.REP_PERIOD_ID) DESC, COMPANY_NAME
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

--=======================================================================
-- ������ ��������� ��� �������� � 1�
--=======================================================================
PROCEDURE CONTRACT_LIST_BY_TPI( p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'CONTRACT_LIST_BY_TPI';
BEGIN   
     OPEN p_recordset FOR
      WITH TI AS (
          SELECT * FROM (
            SELECT G.GROUP_ID,
                   G.NAME,
                   G.TABLE_NAME_H,
                   G.TABLE_NAME_L,
                   PROV.CONTRACTOR_ID,
                   PROV.CONTRACTOR,
                   NULL BRANCH_ID,
                   NULL BRANCH,
                   AP.PROFILE_ID,
                   A.ACCOUNT_ID,
                   A.ACCOUNT_NO,
                   AP.CONTRACT_ID,
                   AP.KPP,
                   A.BILLING_ID
              FROM EXPORT_1C_GROUP_T G,
                   EXPORT_1C_GROUP_CONTRACTOR_T gc,
                   CONTRACTOR_T PROV,
                   ACCOUNT_T A,         
                   ACCOUNT_PROFILE_T AP
             WHERE G.ACTUAL = 'Y'
                   AND G.billing_id IS NULL
                   AND G.GROUP_ID = GC.GROUP_ID
                   AND PROV.CONTRACTOR_ID = GC.CONTRACTOR_ID
                   AND A.BILLING_ID IN (2001,2002,2003)
                   AND A.ACCOUNT_TYPE = 'J'
                   AND A.STATUS <> 'T' 
                   AND AP.ACCOUNT_ID = A.ACCOUNT_ID
                   AND AP.ACTUAL = 'Y'
                   AND GC.CONTRACTOR_ID = AP.CONTRACTOR_ID         
          UNION ALL 
            SELECT G.GROUP_ID,
                   G.NAME,
                   G.TABLE_NAME_H,
                   G.TABLE_NAME_L,
                   PROV.CONTRACTOR_ID,
                   PROV.CONTRACTOR,
                   BR.CONTRACTOR_ID BRANCH_ID,
                   BR.CONTRACTOR BRANCH,
                   AP.PROFILE_ID,
                   A.ACCOUNT_ID,
                   A.ACCOUNT_NO,
                   AP.CONTRACT_ID,
                   AP.KPP,
                   A.BILLING_ID
              FROM EXPORT_1C_GROUP_T g,
                   EXPORT_1C_GROUP_CONTRACTOR_T gc,
                   CONTRACTOR_T PROV,
                   CONTRACTOR_T BR,
                   ACCOUNT_T A,         
                   ACCOUNT_PROFILE_T AP
             WHERE 
                   G.ACTUAL = 'Y'
                   AND g.billing_id = 2008
                   AND g.GROUP_ID = gc.GROUP_ID
                   AND prov.contractor_id = GC.CONTRACTOR_ID
                   AND br.CONTRACTOR_ID = gc.BRANCH_ID
                   AND (A.BILLING_ID IN (2005, 2006, 2007) OR (A.BILLING_ID IN (2004) AND AP.BRANCH_ID = 312))
                   AND A.ACCOUNT_TYPE = 'J'
                   AND A.STATUS <> 'T' 
                   AND AP.ACCOUNT_ID = A.ACCOUNT_ID
                   AND AP.ACTUAL = 'Y'
                   AND GC.CONTRACTOR_ID = AP.CONTRACTOR_ID
                   AND GC.BRANCH_ID = AP.BRANCH_ID         
          )         
          ORDER BY GROUP_ID
      )
      SELECT  
             C.CONTRACT_ID, C.CONTRACT_NO, 
             TO_CHAR(C.DATE_FROM,'dd.mm.yyyy') CONTRACT_DATE, 
             CM.COMPANY_NAME, CM.ERP_CODE, CM.INN, CM.KPP COMPANY_KPP, 
             DECODE(TI.KPP, CM.KPP, NULL, TI.KPP) COMPANY_BRANCH_KPP,
             TI.CONTRACTOR, TI.BRANCH CONTRACTOR_BRANCH,
             TI.NAME TPI_GROUP, TI.TABLE_NAME_H TPI_TABLE_NAME_H, TI.TABLE_NAME_L TPI_TABLE_NAME_L,
             MAX(B.REP_PERIOD_ID) LAST_BILL_PERIOD
        FROM TI, COMPANY_T CM, BILL_T B, CONTRACT_T C
       WHERE CM.ACTUAL = 'Y'
         AND CM.CONTRACT_ID = TI.CONTRACT_ID
         AND TI.ACCOUNT_ID = B.ACCOUNT_ID
         AND TI.CONTRACT_ID = C.CONTRACT_ID
      GROUP BY C.CONTRACT_NO, C.DATE_FROM, 
               CM.COMPANY_NAME, CM.ERP_CODE, CM.INN, CM.KPP, 
               DECODE(TI.KPP, CM.KPP, NULL, TI.KPP),
               TI.CONTRACTOR, TI.BRANCH,
               TI.NAME, TI.TABLE_NAME_H, TI.TABLE_NAME_L
      ORDER BY DECODE(TI.NAME, '����', '1', TI.NAME), MAX(B.REP_PERIOD_ID) DESC, COMPANY_NAME
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ � ������� ��� ������ 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Load_accounts_queue( 
              p_from_period_id INTEGER,  -- ������ ������ ���� ������
              p_to_period_id   INTEGER   -- ������ ����� ���� ������
          )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Load_accounts_queue';
    v_count       INTEGER;
    v_from_period DATE := Pk04_Period.Period_from(p_from_period_id);
    v_to_period   DATE := Pk04_Period.Period_to(p_to_period_id) ;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ������ � ������� ��������� ������
    DELETE FROM PK25_REPORT_PERIOD_T;
    INSERT INTO PK25_REPORT_PERIOD_T (
       PERIOD_ID_FROM, PERIOD_ID_TO, PERIOD_FROM, PERIOD_TO
    ) VALUES (
       p_from_period_id, p_to_period_id, v_from_period, v_to_period
    );

    -- �������� ������
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK25_REPORT_QUEUE_T DROP STORAGE';
    
    -- ��������� ������� �������
    INSERT INTO PK25_REPORT_QUEUE_T( ACCOUNT_ID, ACCOUNT_NO, CONTRACT_NO, ERP_CODE )
    SELECT A.ACCOUNT_ID, A.ACCOUNT_NO, C.CONTRACT_NO, CM.ERP_CODE--, A.NOTES, A.CREATE_DATE 
      FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A, CONTRACT_T C, COMPANY_T CM, PK25_REPORT_PERIOD_T RP
     WHERE A.BILLING_ID     IN (2001, 2002) -- ��������, ������ ������������ ��������
       AND A.ACCOUNT_TYPE   = 'J'
       AND A.STATUS         = 'B'
       --  ����� ��������������� �� Infranet �/� �� 1 ����, ����� ����� ������������� � ����� Infranet   
       AND A.CREATE_DATE   <= RP.PERIOD_FROM
       AND A.ACCOUNT_ID     = AP.ACCOUNT_ID
       -- ������� ������� �� ������ �������� �������
       AND (AP.DATE_TO IS NULL OR AP.DATE_TO >= ADD_MONTHS(RP.PERIOD_FROM, -2))
       AND AP.DATE_FROM    <= RP.PERIOD_TO 
       AND AP.CONTRACTOR_ID = 1 -- �/� ������ ������ ���� �� "�� "�������� ������������""
       AND AP.CONTRACT_ID   = C.CONTRACT_ID
       AND CM.CONTRACT_ID   = C.CONTRACT_ID
       AND CM.ERP_CODE      IS NOT NULL
       AND NOT EXISTS ( -- � �� ���������� � ������� ��� � �������� �� ������� ��� ������������ ������� �����������
         SELECT * FROM ACCOUNT_PROFILE_T APR, CONTRACT_T CR, COMPANY_T CMR
          WHERE APR.ACCOUNT_ID  = A.ACCOUNT_ID
            AND APR.CONTRACT_ID = CR.CONTRACT_ID
            AND APR.CONTRACT_ID = CMR.CONTRACT_ID
            AND (APR.CONTRACTOR_ID != 1 OR CR.CONTRACT_NO != C.CONTRACT_NO OR CMR.ERP_CODE != CM.ERP_CODE)
            AND (APR.DATE_TO IS NULL OR APR.DATE_TO >= ADD_MONTHS(RP.PERIOD_FROM, -2))
            AND APR.DATE_FROM  <= RP.PERIOD_TO
       )
       AND NOT EXISTS ( -- �� �������� ��� �/�, ���������� ����� ������ �������
         SELECT * FROM ACCOUNT_PROFILE_T APR, ACCOUNT_T AR
          WHERE APR.CONTRACT_ID = C.CONTRACT_ID
            AND APR.ACCOUNT_ID  = AR.ACCOUNT_ID
            AND AR.CREATE_DATE >= ADD_MONTHS(RP.PERIOD_FROM, -2)
       )
--       AND C.CONTRACT_NO = 'MS002617'
--       AND A.NOTES != 'Import from LOTUS'
--       AND A.NOTES LIKE '%Portal%'
--     ORDER BY TO_DATE(SUBSTR(A.NOTES,-10),'dd.mm.yyyy') DESC
     GROUP BY A.ACCOUNT_ID, A.ACCOUNT_NO, C.CONTRACT_NO, CM.ERP_CODE
     ORDER BY ACCOUNT_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK25_REPORT_QUEUE_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    COMMIT;
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ========================================================================= --
-- ����������� ��� ������
-- ------------------------------------------------------------------------- --
-- ������ ������������ BRM ��� ������
--
PROCEDURE BRM_erp_code_list(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'BRM_erp_code_list';
BEGIN   
     OPEN p_recordset FOR
        WITH Q AS (
        SELECT Q.ERP_CODE, C.COMPANY_NAME, C.INN, C.KPP, 
               MAX(LENGTH(C.COMPANY_NAME)) OVER (PARTITION BY Q.ERP_CODE) MAXL 
          FROM PK25_REPORT_QUEUE_T Q, COMPANY_T C
         WHERE Q.ERP_CODE = C.ERP_CODE
        ) 
        SELECT ERP_CODE, COMPANY_NAME, INN, KPP 
          FROM (
            SELECT Q.ERP_CODE, Q.COMPANY_NAME, Q.INN, Q.KPP, MAXL, 
                   ROW_NUMBER() OVER (PARTITION BY Q.ERP_CODE, LENGTH(Q.COMPANY_NAME) ORDER BY Q.COMPANY_NAME) RN  
              FROM Q  
             WHERE LENGTH(Q.COMPANY_NAME) = MAXL
        )
        WHERE RN = 1
        ORDER BY ERP_CODE
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ------------------------------------------------------------------------- --
-- ������ ��������� ������������ BRM ��� ������
-- ------------------------------------------------------------------------- --
PROCEDURE BRM_erp_contract_list(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'BRM_erp_contract_list';
BEGIN   
     OPEN p_recordset FOR
      SELECT DISTINCT Q.ERP_CODE, Q.CONTRACT_NO, CM.INN, 
             CM.KPP, CM.COMPANY_NAME, CM.DATE_FROM, CM.DATE_TO 
        FROM PK25_REPORT_QUEUE_T Q, CONTRACT_T C, COMPANY_T CM
       WHERE C.CONTRACT_ID = CM.CONTRACT_ID
         AND Q.CONTRACT_NO = C.CONTRACT_NO
         AND Q.ERP_CODE = CM.ERP_CODE
      ORDER BY Q.ERP_CODE, Q.CONTRACT_NO
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��� ������ (����������) 
-- ��������� ������� ������������ �� ��������� � �������
-- � ������� ���� ������, �� ��������� ������ � p_from_period_id �� p_to_period_id:
-- ERP_CODE,CONTRACT_NO,IN_BALANCE,BILL_TOTAL,RECVD,OUT_BALANCE
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Load_BRM_akt_data 
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Load_BRM_akt_data';
    v_count     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- �������� ������
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK25_AKT_BRM_T DROP STORAGE';
    -- ��������� ��������� �������
    INSERT INTO PK25_AKT_BRM_T (
        ACCOUNT_ID, ACCOUNT_NO, CONTRACT_NO, ERP_CODE, IN_BALANCE, OUT_BALANCE, 
        BILL_TOTAL, BILL_NO, RECVD, DOC_ID, DOC_DATE, REP_PERIOD_ID, 
        CURRENCY_ID, PAYSYSTEM_NAME, PAY_DESCR, STATUS
    )
    SELECT * FROM (
    WITH AB AS ( -- ��������
        SELECT Q.ACCOUNT_ID, Q.ACCOUNT_NO, Q.CONTRACT_NO, Q.ERP_CODE
          FROM PK25_REPORT_QUEUE_T Q   
    ), AI AS (  -- �������� � ��������� ���������
        SELECT AB.*, 
               NVL(IB.BALANCE,0) IN_BALANCE, 
               NVL(IB.REP_PERIOD_ID, 200001) IB_PERIOD_ID, 
               NVL(IB.BALANCE_DATE, TO_DATE('01.01.2000','dd.mm.yyyy')) IB_DATE,
               CASE
                WHEN IB.REP_PERIOD_ID IS NOT NULL AND IB.REP_PERIOD_ID < RP.PERIOD_ID_FROM THEN 0
                WHEN IB.REP_PERIOD_ID IS NOT NULL AND IB.REP_PERIOD_ID BETWEEN RP.PERIOD_ID_FROM AND RP.PERIOD_ID_TO THEN 1
                ELSE 2
               END STATUS,
               RP.PERIOD_ID_FROM, RP.PERIOD_ID_TO, RP.PERIOD_FROM, RP.PERIOD_TO 
          FROM AB, INCOMING_BALANCE_T IB, PK25_REPORT_PERIOD_T RP 
         WHERE AB.ACCOUNT_ID = IB.ACCOUNT_ID(+)
    ), B AS (  -- ���������� ������ ���������, � ��������� �� �������� ������ 
        SELECT AI.ACCOUNT_ID, AI.ACCOUNT_NO, AI.CONTRACT_NO, AI.ERP_CODE,  
               B.TOTAL, B.BILL_NO, B.BILL_DATE, B.REP_PERIOD_ID, B.BILL_TYPE, 
               B.BILL_STATUS, B.CURRENCY_ID,
               CASE
                WHEN B.REP_PERIOD_ID < AI.PERIOD_ID_FROM THEN 0
                WHEN B.REP_PERIOD_ID BETWEEN AI.PERIOD_ID_FROM AND AI.PERIOD_ID_TO THEN 1
                ELSE -1
               END STATUS,
               AI.PERIOD_ID_FROM, AI.PERIOD_ID_TO, AI.PERIOD_FROM, AI.PERIOD_TO
          FROM BILL_T B, AI
         WHERE AI.ACCOUNT_ID    = B.ACCOUNT_ID
           AND B.REP_PERIOD_ID >= AI.IB_PERIOD_ID
           AND B.TOTAL != 0
    ), P AS (  -- ������� ���������, � ��������� �� �������� ������
        SELECT AI.ACCOUNT_ID, AI.ACCOUNT_NO, AI.CONTRACT_NO, AI.ERP_CODE,
               P.RECVD, P.PAYMENT_DATE, P.REP_PERIOD_ID,  P.DOC_ID, PS.PAYSYSTEM_NAME, 
               P.PAY_DESCR, P.CREATE_DATE, P.NOTES, P.CURRENCY_ID,
               CASE
                WHEN P.REP_PERIOD_ID < AI.PERIOD_ID_FROM THEN 0
                WHEN P.REP_PERIOD_ID BETWEEN AI.PERIOD_ID_FROM AND AI.PERIOD_ID_TO THEN 1
                ELSE -1
               END STATUS,
               AI.PERIOD_ID_FROM, AI.PERIOD_ID_TO, AI.PERIOD_FROM, AI.PERIOD_TO
          FROM PAYMENT_T P, PAYSYSTEM_T PS, AI
         WHERE AI.ACCOUNT_ID    = P.ACCOUNT_ID
           AND P.REP_PERIOD_ID >= AI.IB_PERIOD_ID
           AND P.PAYSYSTEM_ID   = PS.PAYSYSTEM_ID(+)
    ), D AS (  -- ���������: ��.������� - ����� + �������
        SELECT B.ACCOUNT_ID, B.ACCOUNT_NO, B.CONTRACT_NO, B.ERP_CODE,
               NULL IN_BALANCE, 
               B.TOTAL BILL_TOTAL, B.BILL_NO, 
               NULL RECVD, NULL DOC_ID,
               B.BILL_DATE DOC_DATE , B.REP_PERIOD_ID, B.CURRENCY_ID,
               NULL PAYSYSTEM_NAME, NULL PAY_DESCR,
               B.STATUS,
               B.PERIOD_ID_FROM, B.PERIOD_ID_TO, B.PERIOD_FROM, B.PERIOD_TO
          FROM B    -- ����� ������������ �� ����, ������� ����������� ��������  
        UNION ALL
        SELECT P.ACCOUNT_ID, P.ACCOUNT_NO, P.CONTRACT_NO, P.ERP_CODE,
               NULL IN_BALANCE, 
               NULL BILL_TOTAL, NULL BILL_NO,
               P.RECVD, P.DOC_ID, 
               P.PAYMENT_DATE DOC_DATE, P.REP_PERIOD_ID, P.CURRENCY_ID,
               P.PAYSYSTEM_NAME, P.PAY_DESCR,
               P.STATUS,
               P.PERIOD_ID_FROM, P.PERIOD_ID_TO, P.PERIOD_FROM, P.PERIOD_TO   
          FROM P    -- ������� �������� �� ������ ������������ �� ����, ������� ����������� ��������
        UNION ALL
        SELECT AI.ACCOUNT_ID, AI.ACCOUNT_NO, AI.CONTRACT_NO, AI.ERP_CODE,
               AI.IN_BALANCE, 
               NULL BILL_TOTAL, NULL BILL_NO,
               NULL RECVD, NULL DOC_ID, 
               AI.IB_DATE DOC_DATE, AI.IB_PERIOD_ID REP_PERIOD_ID, NULL CURRENCY_ID,
               NULL PAYSYSTEM_NAME, NULL PAY_DESCR,
               AI.STATUS,
               AI.PERIOD_ID_FROM, AI.PERIOD_ID_TO, AI.PERIOD_FROM, AI.PERIOD_TO   
          FROM AI    -- ������� �������� �� ������ ������������ �� ����, ������� ����������� ��������
    ), RP AS (
        SELECT ACCOUNT_ID, ACCOUNT_NO, CONTRACT_NO, ERP_CODE,
               SUM(NVL(IN_BALANCE,0)) - SUM(NVL(BILL_TOTAL,0)) + SUM(NVL(RECVD,0)) IN_BALANCE, 
               NULL OUT_BALANCE,
               NULL BILL_TOTAL, NULL BILL_NO,
               NULL RECVD, NULL DOC_ID, 
               MIN(PERIOD_FROM)-1/86400 DOC_DATE, MAX(REP_PERIOD_ID) REP_PERIOD_ID, NULL CURRENCY_ID,
               NULL PAYSYSTEM_NAME, NULL PAY_DESCR,
               STATUS 
          FROM D
         WHERE D.STATUS = 0
         GROUP BY ACCOUNT_ID, ACCOUNT_NO, CONTRACT_NO, ERP_CODE, PERIOD_FROM, STATUS
        UNION ALL
        SELECT ACCOUNT_ID, ACCOUNT_NO, CONTRACT_NO, ERP_CODE,
               IN_BALANCE, NULL OUT_BALANCE, 
               BILL_TOTAL, BILL_NO,
               RECVD, DOC_ID, 
               DOC_DATE, REP_PERIOD_ID, CURRENCY_ID,
               PAYSYSTEM_NAME, PAY_DESCR,
               STATUS 
          FROM D
         WHERE D.STATUS = 1
        UNION ALL
        SELECT ACCOUNT_ID, ACCOUNT_NO, CONTRACT_NO, ERP_CODE,
               NULL IN_BALANCE,
               SUM(NVL(IN_BALANCE,0)) - SUM(NVL(BILL_TOTAL,0)) + SUM(NVL(RECVD,0)) OUT_BALANCE,
               NULL BILL_TOTAL, NULL BILL_NO,
               NULL RECVD, NULL DOC_ID, 
               MAX(PERIOD_TO) DOC_DATE, MAX(REP_PERIOD_ID) REP_PERIOD_ID, NULL CURRENCY_ID,
               NULL PAYSYSTEM_NAME, NULL PAY_DESCR,
               2 STATUS 
          FROM D
         WHERE D.STATUS IN (0, 1)
         GROUP BY ACCOUNT_ID, ACCOUNT_NO, CONTRACT_NO, ERP_CODE, PERIOD_FROM
    )
    SELECT * FROM RP
     WHERE 1=1
     ORDER BY ERP_CODE, CONTRACT_NO, ACCOUNT_NO, REP_PERIOD_ID, DOC_DATE, STATUS  
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK25_BAL_BRM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ��������� ������, ��� �� ������, �� ��������, �� ��������
    INSERT INTO PK25_AKT_BRM_T (
        ACCOUNT_ID, ACCOUNT_NO, CONTRACT_NO, ERP_CODE, IN_BALANCE, OUT_BALANCE, 
        BILL_TOTAL, BILL_NO, RECVD, DOC_ID, DOC_DATE, REP_PERIOD_ID, 
        CURRENCY_ID, PAYSYSTEM_NAME, PAY_DESCR, STATUS
    )
    SELECT Q.ACCOUNT_ID, Q.ACCOUNT_NO, Q.CONTRACT_NO, Q.ERP_CODE, 
           NULL IN_BALANCE, NULL OUT_BALANCE, 
           NULL BILL_TOTAL, NULL BILL_NO, 
           NULL RECVD, NULL DOC_ID, NULL DOC_DATE, 
           NULL REP_PERIOD_ID, NULL CURRENCY_ID, 
           NULL PAYSYSTEM_NAME, NULL PAY_DESCR, NULL STATUS
      FROM PK25_REPORT_QUEUE_T Q
     WHERE NOT EXISTS (
        SELECT * FROM PK25_AKT_BRM_T K
         WHERE Q.ACCOUNT_ID  = K.ACCOUNT_ID 
           AND Q.ACCOUNT_NO  = K.ACCOUNT_NO 
           AND Q.CONTRACT_NO = K.CONTRACT_NO 
           AND Q.ERP_CODE    = K.ERP_CODE
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK25_BAL_BRM_T: '||v_count||' null rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    COMMIT;
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- -------------------------------------------------------------------- --
-- ����������� ���� ������ �� �/�
-- -------------------------------------------------------------------- --
PROCEDURE Act_report_account( 
            p_result     OUT VARCHAR2,  
            p_recordset  OUT t_refc,
            p_account_no  IN VARCHAR2 DEFAULT NULL,
            p_contract_no IN VARCHAR2 DEFAULT NULL,
            p_erp_code    IN VARCHAR2 DEFAULT NULL
          )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Act_report_account';
BEGIN   
     OPEN p_recordset FOR
        SELECT     
            ACCOUNT_ID, ACCOUNT_NO, CONTRACT_NO, ERP_CODE, 
            IN_BALANCE, OUT_BALANCE, 
            BILL_TOTAL, BILL_NO, 
            RECVD, DOC_ID, DOC_DATE, 
            REP_PERIOD_ID, CURRENCY_ID, PAYSYSTEM_NAME, PAY_DESCR, STATUS STATUS
          FROM PK25_AKT_BRM_T 
         WHERE 1=1
           AND ( p_account_no  IS NULL OR p_account_no  = ACCOUNT_NO )
           AND ( p_contract_no IS NULL OR p_contract_no = CONTRACT_NO )
           AND ( p_erp_code    IS NULL OR p_erp_code    = ERP_CODE )
         ORDER BY ERP_CODE, CONTRACT_NO, ACCOUNT_NO, REP_PERIOD_ID, DOC_DATE, STATUS
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- -------------------------------------------------------------------- --
-- ����������� ���� ������ �� �������� �����������
-- -------------------------------------------------------------------- --
PROCEDURE Act_report_contract( 
            p_result     OUT VARCHAR2,  
            p_recordset  OUT t_refc,
            p_contract_no IN VARCHAR2 DEFAULT NULL,
            p_erp_code    IN VARCHAR2 DEFAULT NULL
          )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Act_report_contract';
BEGIN   
     OPEN p_recordset FOR
      SELECT     
          ERP_CODE, CONTRACT_NO,  
          SUM(IN_BALANCE)        IN_BALANCE, 
          SUM(OUT_BALANCE)       OUT_BALANCE, 
          SUM(NVL(BILL_TOTAL,0)) BILL_TOTAL, 
          SUM(NVL(RECVD,0))      RECVD 
        FROM PK25_AKT_BRM_T 
       WHERE 1=1
         AND ( p_contract_no IS NULL OR p_contract_no = CONTRACT_NO )
         AND ( p_erp_code    IS NULL OR p_erp_code    = ERP_CODE )
       GROUP BY ERP_CODE, CONTRACT_NO
       ORDER BY ERP_CODE, CONTRACT_NO
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- -------------------------------------------------------------------- --
-- ����������� ���� ������ �� �������� �����������
-- -------------------------------------------------------------------- --
PROCEDURE Act_report_erp_code( 
            p_result     OUT VARCHAR2,  
            p_recordset  OUT t_refc,
            p_erp_code    IN VARCHAR2 DEFAULT NULL
          )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Act_report_erp_code';
BEGIN   
     OPEN p_recordset FOR
      SELECT     
          ERP_CODE,  
          SUM(IN_BALANCE)        IN_BALANCE, 
          SUM(OUT_BALANCE)       OUT_BALANCE, 
          SUM(NVL(BILL_TOTAL,0)) BILL_TOTAL, 
          SUM(NVL(RECVD,0))      RECVD 
        FROM PK25_AKT_BRM_T 
       WHERE 1=1
         AND ( p_erp_code    IS NULL OR p_erp_code    = ERP_CODE )
       GROUP BY ERP_CODE
       ORDER BY ERP_CODE
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;
 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��� ������ (���������� 1� - ��������) 
-- ������ � ������� PK25_BAL_1C_T, ��������� ��������������, 
-- � ������� PLSQL Developer, ��� ������, 
-- ����� �������� NUMBER
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Load_1C_akt_data 
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Load_1C_akt_data';
    v_count     INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    DELETE FROM PK25_BAL_1C_T CT WHERE CT.IN_BALANCE_1C = '������� �� ������';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK25_BAL_1C_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
     
    UPDATE PK25_BAL_1C_T CT
       SET IN_BALANCE  = TO_NUMBER(NVL(IN_BALANCE_1C,0), '999G999G999G999G999D999999','NLS_NUMERIC_CHARACTERS = '', '''),
           BILL_TOTAL  = TO_NUMBER(NVL(BILL_TOTAL_1C,0), '999G999G999G999G999D999999','NLS_NUMERIC_CHARACTERS = '', '''),
           RECVD       = TO_NUMBER(NVL(RECVD_1C,0),      '999G999G999G999G999D999999','NLS_NUMERIC_CHARACTERS = '', '''),
           OUT_BALANCE = TO_NUMBER(NVL(OUT_BALANCE_1C,0),'999G999G999G999G999D999999','NLS_NUMERIC_CHARACTERS = '', '''),
           CONTRACT_NO = TRIM (CONTRACT_NO)
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK25_BAL_1C_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    COMMIT;
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� �� 1�, � ������� ���� ������
--
PROCEDURE Export_1�_data(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Export_1�_data';
BEGIN   
     OPEN p_recordset FOR
      SELECT T.ERP_CODE, T.CONTRACT_NO, T.IN_BALANCE, 
             T.BILL_TOTAL, T.RECVD, T.OUT_BALANCE 
        FROM PK25_BAL_1C_T T, PK25_REPORT_QUEUE_T Q
       WHERE T.ERP_CODE    = Q.ERP_CODE 
         AND T.CONTRACT_NO = Q.CONTRACT_NO
       ORDER BY T.ERP_CODE, T.CONTRACT_NO
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ========================================================================= --
-- ����� ���������� �� �������� � ������� ��������� � BRM � 1�
-- ========================================================================= --
PROCEDURE Common_stat_by_contract(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Common_stat_by_contract';
BEGIN   
     OPEN p_recordset FOR
        WITH BB AS (  
            SELECT  -- ���������� �� ��������� �� BRM 
                ERP_CODE, CONTRACT_NO,  
                SUM(IN_BALANCE)        IN_BALANCE, 
                SUM(OUT_BALANCE)       OUT_BALANCE, 
                SUM(NVL(BILL_TOTAL,0)) BILL_TOTAL, 
                SUM(NVL(RECVD,0))      RECVD 
              FROM PK25_AKT_BRM_T 
             WHERE 1=1
             GROUP BY ERP_CODE, CONTRACT_NO
             ORDER BY ERP_CODE, CONTRACT_NO
        ), CB AS (
            SELECT E.ERP_CODE, E.CONTRACT_NO, 
                   SUM(E.IN_BALANCE)  IN_BALANCE,
                   SUM(E.BILL_TOTAL)  BILL_TOTAL,
                   SUM(E.RECVD)       RECVD,
                   SUM(E.OUT_BALANCE) OUT_BALANCE
              FROM PK25_BAL_1C_T E
             GROUP BY E.ERP_CODE, E.CONTRACT_NO
        ), ST AS ( 
            SELECT 
                    NVL(CB.ERP_CODE, BB.ERP_CODE) ERP_CODE,
                    NVL(CB.CONTRACT_NO, BB.CONTRACT_NO) CONTRACT_NO,
                    CB.IN_BALANCE,
                    BB.IN_BALANCE IN_BALANCE_BRM,
                    CB.BILL_TOTAL,
                    BB.BILL_TOTAL BILL_TOTAL_BRM,
                    CB.RECVD,
                    BB.RECVD RECVD_BRM,
                    CB.OUT_BALANCE,
                    BB.OUT_BALANCE OUT_BALANCE_BRM
              FROM CB FULL OUTER JOIN BB
             ON (
                CB.ERP_CODE = BB.ERP_CODE AND
                CB.CONTRACT_NO = BB.CONTRACT_NO
             )
        )
        SELECT * FROM ST
         --WHERE IN_BALANCE IS NOT NULL
         ORDER BY CASE WHEN BILL_TOTAL = BILL_TOTAL_BRM THEN 1 ELSE 0 END, (BILL_TOTAL - BILL_TOTAL_BRM)
        ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;


-- ======================================================================= --
-- �������� ������ BRM �� ��������� ������� ��� ���������� ������ 
-- ======================================================================= --
PROCEDURE Load_BRM_bills
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Load_BRM_bills';
    v_count          INTEGER;
    v_from_period_id INTEGER;  -- ������ ������ ���� ������
    v_to_period_id   INTEGER;  -- ������ ����� ���� ������
    v_from_period    DATE;
    v_to_period      DATE;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    SELECT PERIOD_ID_FROM, PERIOD_ID_TO, PERIOD_FROM, PERIOD_TO
      INTO v_from_period_id, v_to_period_id, v_from_period, v_to_period
      FROM PK25_REPORT_PERIOD_T;

    -- �������� ������
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK25_REPORT_BILL_T DROP STORAGE';
    
    -- ��������� ������� � ����������� � ������
    INSERT INTO PK25_REPORT_BILL_T (
           ACCOUNT_ID, ACCOUNT_NO, 
           TPI_ACCOUNT_NO, CONTRACT_NO, TPI_CONTRACT_NO, 
           ERP_CODE, TPI_ERP_CODE, BILL_NO, TPI_BILL_NO, 
           REP_PERIOD_ID, TPI_PERIOD_ID, 
           TOTAL, TPI_GROSS_RUR, TPI_TAX_AMOUNT, TPI_DUE_RUR, DELTA_TOTAL, 
           CURRENCY_ID, TPI_CURRENCY_ID, JOURNAL_ID, SESSION_ID
    )
    SELECT ACCOUNT_ID, ACCOUNT_NO, 
           TPI_ACCOUNT_NO, CONTRACT_NO, TPI_CONTRACT_NO, 
           ERP_CODE, TPI_ERP_CODE, BILL_NO, TPI_BILL_NO, 
           REP_PERIOD_ID, TPI_PERIOD_ID, 
           TOTAL, TPI_GROSS_RUR, TPI_TAX_AMOUNT, TPI_DUE_RUR, DELTA_TOTAL, 
           CURRENCY_ID, TPI_CURRENCY_ID, JOURNAL_ID, SESSION_ID 
      FROM (
        WITH AB AS ( -- ��������
            SELECT ACCOUNT_ID, ACCOUNT_NO, CONTRACT_NO, ERP_CODE 
              FROM PK25_REPORT_QUEUE_T 
        ), TPI AS ( -- ����� ����������� � ����������� �� ��������� ������
            -- -----------------
            SELECT * FROM (
                SELECT 
                   HEADER_ID, PERIOD_ID, JOURNAL_ID, SESSION_ID,
                   TPI_BILL_NO, TPI_BILL_TOTAL, TPI_GROSS_RUR, TPI_TAX_AMOUNT, TPI_DUE_RUR, 
                   TPI_CONTRACT_NO, TPI_ACCOUNT_NO, TPI_ERP_CODE, TPI_CUSTNAME,
                   TPI_CURRENCY_ID 
                  FROM (
                    SELECT TP.*, 
                           ROW_NUMBER () OVER (PARTITION BY HEADER_ID,TPI_BILL_NO ORDER BY HEADER_ID DESC) RN  
                      FROM (
                    SELECT 
                           H.HEADER_ID,
                           H.PERIOD_ID,
                           H.JOURNAL_ID,
                           H.SESSION_ID,
                           L.FACTUREEXTERNALID TPI_BILL_NO,
                           SUM(L.NET_AMOUNT)   TPI_BILL_TOTAL,
                           SUM(L.GROSS_RUR)    TPI_GROSS_RUR,
                           SUM(L.TAX_AMOUNT)   TPI_TAX_AMOUNT,
                           SUM(L.DUE_RUR)      TPI_DUE_RUR,
                           --L.EXECUTIONPERIOD,
                           L.AUTO_NO TPI_CONTRACT_NO,
                           L.ACCOUNT_NO TPI_ACCOUNT_NO,
                           L.PARTNERID TPI_ERP_CODE,
                           L.CUSTNAME TPI_CUSTNAME,
                           L.CURRENCYCODE TPI_CURRENCY_ID       
                      FROM EXPORT_1C_LINES_T L, EXPORT_1C_HEADER_T H
                     WHERE H.HEADER_ID = L.HEADER_ID
                       AND H.PERIOD_ID BETWEEN v_from_period_id AND v_to_period_id
                       AND H.GROUP_ID > 0
                       AND H.STATUS = 'EXPORT_DATA_OK'
                       AND H.EXPORT_TYPE IS NULL
                    GROUP BY H.HEADER_ID,H.PERIOD_ID, H.JOURNAL_ID, H.SESSION_ID, 
                             L.FACTUREEXTERNALID, L.AUTO_NO, L.ACCOUNT_NO,L.PARTNERID, 
                             L.CUSTNAME, L.CURRENCYCODE                 
                   ) TP
                ) T
                WHERE T.RN = 1
                -- -----------------
                UNION ALL
                -- -----------------
                SELECT 
                       H.HEADER_ID,
                       H.PERIOD_ID,
                       H.JOURNAL_ID,
                       H.SESSION_ID,
                       L.FACTUREEXTERNALID TPI_BILL_NO, 
                       SUM(L.NET_AMOUNT)   TPI_BILL_TOTAL,
                       SUM(L.GROSS_RUR)    TPI_GROSS_RUR,
                       SUM(L.TAX_AMOUNT)   TPI_TAX_AMOUNT,
                       SUM(L.DUE_RUR)      TPI_DUE_RUR,
                       L.AUTO_NO           TPI_CONTRACT_NO,
                       L.ACCOUNT_NO        TPI_ACCOUNT_NO, 
                       L.PARTNERID         TPI_ERP_CODE,
                       L.CUSTNAME          TPI_CUSTNAME,
                       L.CURRENCYCODE TPI_CURRENCY_ID
                  FROM EXPORT_1C_LINES_T L, EXPORT_1C_HEADER_T H
                 WHERE H.HEADER_ID = L.HEADER_ID
                   AND H.PERIOD_ID BETWEEN v_from_period_id AND v_to_period_id
                   AND H.STATUS = 'EXPORT_DATA_OK'
                   AND H.EXPORT_TYPE = 'ADD'
                 GROUP BY H.HEADER_ID, PERIOD_ID, JOURNAL_ID, SESSION_ID,
                       FACTUREEXTERNALID, AUTO_NO, ACCOUNT_NO, PARTNERID, CUSTNAME, CURRENCYCODE
            ) T, AB
            WHERE AB.ACCOUNT_NO = T.TPI_ACCOUNT_NO
            -- -----------------
        ), BL AS ( -- ����� �������� �� ��������� ������
            SELECT AB.ERP_CODE, AB.CONTRACT_NO, AB.ACCOUNT_NO, AB.ACCOUNT_ID, 
                   B.BILL_ID, B.BILL_NO, B.REP_PERIOD_ID, B.BILL_DATE, 
                   B.TOTAL, B.TAX, B.GROSS, B.CURRENCY_ID  
              FROM BILL_T B, AB
             WHERE AB.ACCOUNT_ID = B.ACCOUNT_ID
               AND B.REP_PERIOD_ID BETWEEN v_from_period_id AND v_to_period_id
               AND B.TOTAL != 0
        ), TPI_VS_BL AS (
            SELECT 
                   BL.ACCOUNT_ID, 
                   BL.ACCOUNT_NO,
                   TPI.TPI_ACCOUNT_NO,
                   BL.CONTRACT_NO,
                   TPI.TPI_CONTRACT_NO,
                   BL.ERP_CODE,
                   TPI.TPI_ERP_CODE,
                   BL.BILL_NO,
                   TPI.TPI_BILL_NO,
                   BL.REP_PERIOD_ID,
                   TPI.PERIOD_ID TPI_PERIOD_ID,
                   TOTAL,
                   TPI_GROSS_RUR,
                   TPI_TAX_AMOUNT,
                   TPI_DUE_RUR,
                   (TOTAL - TPI_DUE_RUR) DELTA_TOTAL,
                   --TOTAL - (GROSS_RUR + TAX_AMOUNT) DELTA_TOTAL, 
                   BL.CURRENCY_ID,
                   TPI.TPI_CURRENCY_ID,
                   TPI.JOURNAL_ID, 
                   TPI.SESSION_ID
              FROM TPI FULL OUTER JOIN BL
              ON (
                   TPI.ACCOUNT_NO  = BL.ACCOUNT_NO  AND
                   TPI.CONTRACT_NO = BL.CONTRACT_NO AND
                   TPI.ERP_CODE    = BL.ERP_CODE    AND
                   TPI.TPI_BILL_NO = BL.BILL_NO
              )
        ), R1 AS (
            -- ����� �� ���� ������ ��������� ��������
            SELECT * FROM TPI_VS_BL V
        ), R2 AS (
            -- �������� � ������������ �� ������ ������ �������� � ���������� � 1�
            SELECT * FROM TPI_VS_BL V
             WHERE (V.BILL_NO IS NULL OR V.TPI_BILL_NO IS NULL)
        ), R3 AS (
            -- �������� � ������ ������������ �� ������ ������ �������� � ���������� � 1�
            SELECT * FROM TPI_VS_BL V
             WHERE DELTA_TOTAL != 0
        ), R4 AS (
            -- ����������� ������ �� ���������, �� ������� ���� ����������� �����
            SELECT NVL(V.CONTRACT_NO, V.TPI_CONTRACT_NO) CONTRACT_NO, 
                   SUM(V.TOTAL) BILL_TOTAL, SUM(V.TPI_DUE_RUR) TPI_TOTAL, 
                   SUM(V.TPI_GROSS_RUR) TPI_GROSS_RUR, SUM(V.TPI_TAX_AMOUNT) TPI_TAX_AMOUNT
              FROM TPI_VS_BL V
             GROUP BY NVL(V.CONTRACT_NO, V.TPI_CONTRACT_NO)
        ), R5 AS (
            -- ����������� ������ �� ����  ��������� 
            SELECT AB.CONTRACT_NO, 
                   BILL_TOTAL, TPI_TOTAL, 
                   TPI_GROSS_RUR, TPI_TAX_AMOUNT
              FROM R4, (SELECT DISTINCT CONTRACT_NO FROM AB) AB
             WHERE R4.CONTRACT_NO(+) = AB.CONTRACT_NO
        ), R6 AS (
            -- ����������� ������ �� ������������, �� ������� ���� ����������� �����
            SELECT NVL(V.ERP_CODE, V.TPI_ERP_CODE) ERP_CODE, 
                   SUM(V.TOTAL) BILL_TOTAL, SUM(V.TPI_DUE_RUR) TPI_TOTAL, 
                   SUM(V.TPI_GROSS_RUR) TPI_GROSS_RUR, SUM(V.TPI_TAX_AMOUNT) TPI_TAX_AMOUNT
              FROM TPI_VS_BL V
             GROUP BY NVL(V.ERP_CODE, V.TPI_ERP_CODE)
        ), R7 AS (
            -- ����������� ������ �� ���� ������������
            SELECT AB.ERP_CODE, 
                   BILL_TOTAL, TPI_TOTAL, 
                   TPI_GROSS_RUR, TPI_TAX_AMOUNT
              FROM R6, (SELECT DISTINCT ERP_CODE FROM AB) AB
             WHERE R6.ERP_CODE(+) = AB.ERP_CODE
        )
        SELECT * FROM R1
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK25_REPORT_BILL_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    COMMIT;
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������ �� ������������ �� ������ ������ � BRM �� ������� PK25_REPORT_BILL_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 1) ����� �� ���� ������������ � ���������� � ����� ������ 
--    ��������� ��� ������ ������������
PROCEDURE R1_BRM_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'R1_BRM_bills';
BEGIN   
     OPEN p_recordset FOR
        SELECT V.*,  
               CASE
                    WHEN TOTAL  = TPI_DUE_RUR                          THEN 0
                    WHEN TOTAL IS NOT NULL AND TPI_DUE_RUR IS     NULL THEN 1
                    WHEN TOTAL IS NULL     AND TPI_DUE_RUR IS NOT NULL THEN 2
                    WHEN TOTAL != TPI_DUE_RUR                          THEN 3
               END ERR_CODE
          FROM PK25_REPORT_BILL_T V
        ORDER BY CASE
                    WHEN TOTAL IS NOT NULL AND TPI_DUE_RUR IS     NULL THEN 1
                    WHEN TOTAL IS NULL     AND TPI_DUE_RUR IS NOT NULL THEN 2
                    WHEN TOTAL IS NOT NULL AND TPI_DUE_RUR IS NOT NULL THEN 3
                    WHEN TOTAL IS NULL     AND TPI_DUE_RUR IS     NULL THEN 4
                  END, 
                  NVL(ERP_CODE, TPI_ERP_CODE),
                  NVL(CONTRACT_NO, TPI_CONTRACT_NO)
        ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 2) �������� � ������������ �� ������ ������ �������� � ���������� � 1�
PROCEDURE R2_BRM_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'R2_BRM_bills';
BEGIN   
     OPEN p_recordset FOR
        SELECT * FROM PK25_REPORT_BILL_T V
         WHERE (V.BILL_NO IS NULL OR V.TPI_BILL_NO IS NULL)
        ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 3) �������� � ������ ������������ �� ������ ������ �������� � ���������� � 1�
PROCEDURE R3_BRM_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'R3_BRM_bills';
BEGIN   
     OPEN p_recordset FOR
        SELECT * FROM PK25_REPORT_BILL_T V
         WHERE DELTA_TOTAL != 0
        ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 5) ����������� ������������ � BRM ������ �� ��������� 
PROCEDURE R5_BRM_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'R5_BRM_bills';
BEGIN   
     OPEN p_recordset FOR
        WITH R4 AS (
            -- ����������� ������ �� ���������, �� ������� ���� ����������� �����
            SELECT NVL(V.ERP_CODE,    V.TPI_ERP_CODE)    ERP_CODE,
                   NVL(V.CONTRACT_NO, V.TPI_CONTRACT_NO) CONTRACT_NO,
                   SUM(V.TOTAL) BILL_TOTAL, SUM(V.TPI_DUE_RUR) TPI_TOTAL, 
                   SUM(V.TPI_GROSS_RUR) TPI_GROSS_RUR, SUM(V.TPI_TAX_AMOUNT) TPI_TAX_AMOUNT
              FROM PK25_REPORT_BILL_T V
             GROUP BY NVL(V.ERP_CODE, V.TPI_ERP_CODE), NVL(V.CONTRACT_NO, V.TPI_CONTRACT_NO)
        ), AB AS (
             SELECT DISTINCT ERP_CODE, CONTRACT_NO 
               FROM PK25_REPORT_QUEUE_T
        ), R5 AS (
        SELECT AB.ERP_CODE,
               AB.CONTRACT_NO, 
               BILL_TOTAL, TPI_TOTAL, 
               TPI_GROSS_RUR, TPI_TAX_AMOUNT,
               CASE
                    WHEN BILL_TOTAL = TPI_TOTAL                           THEN 0
                    WHEN BILL_TOTAL IS NOT NULL AND TPI_TOTAL IS     NULL THEN 1
                    WHEN BILL_TOTAL IS NULL     AND TPI_TOTAL IS NOT NULL THEN 2
                    WHEN BILL_TOTAL != TPI_TOTAL                          THEN 3
               END ERR_CODE
          FROM R4, AB
         WHERE R4.ERP_CODE(+)    = AB.ERP_CODE
           AND R4.CONTRACT_NO(+) = AB.CONTRACT_NO
        )
        SELECT * FROM R5
        ORDER BY CASE
                    WHEN BILL_TOTAL IS NOT NULL AND TPI_TOTAL IS     NULL THEN 1
                    WHEN BILL_TOTAL IS NULL     AND TPI_TOTAL IS NOT NULL THEN 2
                    WHEN BILL_TOTAL IS NOT NULL AND TPI_TOTAL IS NOT NULL THEN 3
                    WHEN BILL_TOTAL IS NULL     AND TPI_TOTAL IS     NULL THEN 4
                  END, 
                  ERP_CODE, CONTRACT_NO 
        ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 7) ����������� ������������ � BRM ������ �� ������������ 
PROCEDURE R7_BRM_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'R7_BRM_bills';
BEGIN   
     OPEN p_recordset FOR
        WITH R6 AS (
           -- ����������� ������ �� ������������, �� ������� ���� ����������� �����
            SELECT NVL(V.ERP_CODE, V.TPI_ERP_CODE) ERP_CODE, 
                   SUM(V.TOTAL) BILL_TOTAL, SUM(V.TPI_DUE_RUR) TPI_TOTAL, 
                   SUM(V.TPI_GROSS_RUR) TPI_GROSS_RUR, SUM(V.TPI_TAX_AMOUNT) TPI_TAX_AMOUNT
              FROM PK25_REPORT_BILL_T V
             GROUP BY NVL(V.ERP_CODE, V.TPI_ERP_CODE)
        ), AB AS (
             SELECT DISTINCT ERP_CODE 
               FROM PK25_REPORT_QUEUE_T
        ), R7 AS (
            SELECT AB.ERP_CODE, 
                   BILL_TOTAL, TPI_TOTAL, 
                   TPI_GROSS_RUR, TPI_TAX_AMOUNT,
                   CASE
                        WHEN BILL_TOTAL = TPI_TOTAL                           THEN 0
                        WHEN BILL_TOTAL IS NOT NULL AND TPI_TOTAL IS     NULL THEN 1
                        WHEN BILL_TOTAL IS NULL     AND TPI_TOTAL IS NOT NULL THEN 2
                        WHEN BILL_TOTAL != TPI_TOTAL                          THEN 3
                   END ERR_CODE
              FROM R6, AB
             WHERE R6.ERP_CODE(+) = AB.ERP_CODE
        )
        SELECT * FROM R7
        ORDER BY CASE
                    WHEN BILL_TOTAL IS NOT NULL AND TPI_TOTAL IS     NULL THEN 1
                    WHEN BILL_TOTAL IS NULL     AND TPI_TOTAL IS NOT NULL THEN 2
                    WHEN BILL_TOTAL IS NOT NULL AND TPI_TOTAL IS NOT NULL THEN 3
                    WHEN BILL_TOTAL IS NULL     AND TPI_TOTAL IS     NULL THEN 4
                  END, 
                  ERP_CODE
        ;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ========================================================================= --
-- ���������� ������ ������ � BRM � 1�
-- ========================================================================= --
-- ------------------------------------------------------------------------- --
-- ��������� �������� ������ �� ��������� ������������ � BRM � 1�
-- ------------------------------------------------------------------------- --
PROCEDURE R1_BRM_vs_1C_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'R1_BRM_vs_1C_bills';
BEGIN   
     OPEN p_recordset FOR
        WITH R4 AS (
            -- ����������� ������ �� ���������, �� ������� ���� ����������� �����
            SELECT NVL(V.ERP_CODE, V.TPI_ERP_CODE) ERP_CODE,
                   NVL(V.CONTRACT_NO, V.TPI_CONTRACT_NO) CONTRACT_NO, 
                   SUM(V.TOTAL) BILL_TOTAL, SUM(V.TPI_DUE_RUR) TPI_TOTAL, 
                   SUM(V.TPI_GROSS_RUR) TPI_GROSS_RUR, SUM(V.TPI_TAX_AMOUNT) TPI_TAX_AMOUNT
              FROM PK25_REPORT_BILL_T V
             GROUP BY NVL(V.ERP_CODE, V.TPI_ERP_CODE),
                      NVL(V.CONTRACT_NO, V.TPI_CONTRACT_NO)
        ), AB AS (  -- 10.269
            SELECT DISTINCT ERP_CODE, CONTRACT_NO 
              FROM PK25_REPORT_QUEUE_T
        ), R5 AS (  -- 10.269
            SELECT AB.ERP_CODE,
                   AB.CONTRACT_NO, 
                   BILL_TOTAL, TPI_TOTAL, 
                   TPI_GROSS_RUR, TPI_TAX_AMOUNT
              FROM R4, AB   
             WHERE R4.CONTRACT_NO(+) = AB.CONTRACT_NO
               AND R4.ERP_CODE(+)    = AB.ERP_CODE
        ), E AS (
            SELECT E.ERP_CODE, E.CONTRACT_NO, SUM(E.BILL_TOTAL) BILL_TOTAL_1C
              FROM PK25_BAL_1C_T E
             GROUP BY E.ERP_CODE, E.CONTRACT_NO
        ), RR5 AS (
            SELECT R5.ERP_CODE, R5.CONTRACT_NO, R5.BILL_TOTAL, E.BILL_TOTAL_1C
              FROM R5, E 
             WHERE R5.CONTRACT_NO = E.CONTRACT_NO(+)
               AND R5.ERP_CODE    = E.ERP_CODE(+)
        ), RR6 AS (
            SELECT RR5.*,
                    CASE
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                        THEN 0
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                        THEN 1
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 2
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN 3
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 4 
                    END ERR_CODE,
                    CASE
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                        THEN 'OK'
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                        THEN '������ ����� ������'
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN '����� ���� � BRM � ��� � 1�'
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN '����� ���� � 1� � ��� � BRM'
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN '��� ������ � 1� � ��� � BRM'
                    END ERR_MSG
              FROM RR5
             ORDER BY CASE
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                        THEN 0
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                        THEN 1
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 2
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN 3
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 4 
                      END
        ), RR7 AS (
            SELECT ERR_CODE, ERR_MSG, COUNT(*) CNT 
              FROM RR6
             GROUP BY ERR_CODE, ERR_MSG
             ORDER BY ERR_CODE, ERR_MSG    
        )
        SELECT * FROM RR6
        --SELECT * FROM RR7
        ;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ------------------------------------------------------------------------- --
-- ���������� �� ������� ��������� ������ �� ��������� ������������ � BRM � 1�
-- ------------------------------------------------------------------------- --
PROCEDURE R2_BRM_vs_1C_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'R2_BRM_vs_1C_bills';
BEGIN   
     OPEN p_recordset FOR
        WITH R4 AS (
            -- ����������� ������ �� ���������, �� ������� ���� ����������� �����
            SELECT NVL(V.ERP_CODE, V.TPI_ERP_CODE) ERP_CODE,
                   NVL(V.CONTRACT_NO, V.TPI_CONTRACT_NO) CONTRACT_NO, 
                   SUM(V.TOTAL) BILL_TOTAL, SUM(V.TPI_DUE_RUR) TPI_TOTAL, 
                   SUM(V.TPI_GROSS_RUR) TPI_GROSS_RUR, SUM(V.TPI_TAX_AMOUNT) TPI_TAX_AMOUNT
              FROM PK25_REPORT_BILL_T V
             GROUP BY NVL(V.ERP_CODE, V.TPI_ERP_CODE),
                      NVL(V.CONTRACT_NO, V.TPI_CONTRACT_NO)
        ), AB AS (  -- 10.269
            SELECT DISTINCT ERP_CODE, CONTRACT_NO 
              FROM PK25_REPORT_QUEUE_T
        ), R5 AS (  -- 10.269
            SELECT AB.ERP_CODE,
                   AB.CONTRACT_NO, 
                   BILL_TOTAL, TPI_TOTAL, 
                   TPI_GROSS_RUR, TPI_TAX_AMOUNT
              FROM R4, AB   
             WHERE R4.CONTRACT_NO(+) = AB.CONTRACT_NO
               AND R4.ERP_CODE(+)    = AB.ERP_CODE
        ), E AS (
            SELECT E.ERP_CODE, E.CONTRACT_NO, SUM(E.BILL_TOTAL) BILL_TOTAL_1C
              FROM PK25_BAL_1C_T E
             GROUP BY E.ERP_CODE, E.CONTRACT_NO
        ), RR5 AS (
            SELECT R5.ERP_CODE, R5.CONTRACT_NO, R5.BILL_TOTAL, E.BILL_TOTAL_1C
              FROM R5, E 
             WHERE R5.CONTRACT_NO = E.CONTRACT_NO(+)
               AND R5.ERP_CODE    = E.ERP_CODE(+)
        ), RR6 AS (
            SELECT RR5.*,
                    CASE
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                        THEN 0
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                        THEN 1
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 2
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN 3
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 4 
                    END ERR_CODE,
                    CASE
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                        THEN 'OK'
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                        THEN '������ ����� ������'
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN '����� ���� � BRM � ��� � 1�'
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN '����� ���� � 1� � ��� � BRM'
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN '��� ������ � 1� � ��� � BRM'
                    END ERR_MSG
              FROM RR5
             ORDER BY CASE
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                        THEN 0
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                        THEN 1
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 2
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN 3
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 4 
                      END
        ), RR7 AS (
            SELECT ERR_CODE, ERR_MSG, COUNT(*) CNT 
              FROM RR6
             GROUP BY ERR_CODE, ERR_MSG
             ORDER BY ERR_CODE, ERR_MSG    
        )
        --SELECT * FROM RR6
        SELECT * FROM RR7
        ;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ------------------------------------------------------------------------- --
-- ��������� �������� ������ ������������ � BRM � 1�
-- ------------------------------------------------------------------------- --
PROCEDURE R3_BRM_vs_1C_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'R3_BRM_vs_1C_bills';
BEGIN   
     OPEN p_recordset FOR
        WITH R4 AS (
            -- ����������� ������ �� ���������, �� ������� ���� ����������� �����
            SELECT NVL(V.ERP_CODE, V.TPI_ERP_CODE) ERP_CODE, 
                   SUM(V.TOTAL) BILL_TOTAL, SUM(V.TPI_DUE_RUR) TPI_TOTAL, 
                   SUM(V.TPI_GROSS_RUR) TPI_GROSS_RUR, SUM(V.TPI_TAX_AMOUNT) TPI_TAX_AMOUNT
              FROM PK25_REPORT_BILL_T V
             GROUP BY NVL(V.ERP_CODE, V.TPI_ERP_CODE)
        ), AB AS (
             SELECT DISTINCT ERP_CODE 
               FROM PK25_REPORT_QUEUE_T
        ), R5 AS (
            SELECT AB.ERP_CODE,
                   BILL_TOTAL, TPI_TOTAL, 
                   TPI_GROSS_RUR, TPI_TAX_AMOUNT
              FROM R4, AB
             WHERE R4.ERP_CODE(+)    = AB.ERP_CODE
        ), E AS (
            SELECT E.ERP_CODE, E.CONTRACT_NO, SUM(E.BILL_TOTAL) BILL_TOTAL_1C
              FROM PK25_BAL_1C_T E
             GROUP BY E.ERP_CODE, E.CONTRACT_NO
        ), RR5 AS (
            SELECT R5.ERP_CODE, R5.BILL_TOTAL, E.BILL_TOTAL_1C
              FROM R5, E 
             WHERE R5.ERP_CODE    = E.ERP_CODE(+)
        ), RR6 AS (
            SELECT RR5.*,
                    CASE
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                        THEN 0
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                        THEN 1
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 2
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN 3
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 4 
                    END ERR_CODE,
                    CASE
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                        THEN 'OK'
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                        THEN '������ ����� ������'
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN '����� ���� � BRM � ��� � 1�'
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN '����� ���� � 1� � ��� � BRM'
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN '��� ������ � 1� � ��� � BRM'
                    END ERR_MSG
              FROM RR5
             ORDER BY CASE
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                        THEN 0
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                        THEN 1
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 2
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN 3
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 4 
                      END
        ), RR7 AS (
            SELECT ERR_CODE, ERR_MSG, COUNT(*) CNT 
              FROM RR6
             GROUP BY ERR_CODE, ERR_MSG
             ORDER BY ERR_CODE, ERR_MSG
        )
        SELECT * FROM RR6
        --SELECT * FROM RR7
        ;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ------------------------------------------------------------------------- --
-- ���������� �� ������� �������� ������ ������������ � BRM � 1�
-- ------------------------------------------------------------------------- --
PROCEDURE R4_BRM_vs_1C_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'R4_BRM_vs_1C_bills';
BEGIN   
     OPEN p_recordset FOR
        WITH R4 AS (
            -- ����������� ������ �� ���������, �� ������� ���� ����������� �����
            SELECT NVL(V.ERP_CODE, V.TPI_ERP_CODE) ERP_CODE, 
                   SUM(V.TOTAL) BILL_TOTAL, SUM(V.TPI_DUE_RUR) TPI_TOTAL, 
                   SUM(V.TPI_GROSS_RUR) TPI_GROSS_RUR, SUM(V.TPI_TAX_AMOUNT) TPI_TAX_AMOUNT
              FROM PK25_REPORT_BILL_T V
             GROUP BY NVL(V.ERP_CODE, V.TPI_ERP_CODE)
        ), AB AS (
             SELECT DISTINCT ERP_CODE 
               FROM PK25_REPORT_QUEUE_T
        ), R5 AS (
            SELECT AB.ERP_CODE,
                   BILL_TOTAL, TPI_TOTAL, 
                   TPI_GROSS_RUR, TPI_TAX_AMOUNT
              FROM R4, AB
             WHERE R4.ERP_CODE(+)    = AB.ERP_CODE
        ), E AS (
            SELECT E.ERP_CODE, E.CONTRACT_NO, SUM(E.BILL_TOTAL) BILL_TOTAL_1C
              FROM PK25_BAL_1C_T E
             GROUP BY E.ERP_CODE, E.CONTRACT_NO
        ), RR5 AS (
            SELECT R5.ERP_CODE, R5.BILL_TOTAL, E.BILL_TOTAL_1C
              FROM R5, E 
             WHERE R5.ERP_CODE = E.ERP_CODE(+)  
        ), RR6 AS (
            SELECT RR5.*,
                    CASE
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                        THEN 0
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                        THEN 1
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 2
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN 3
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 4 
                    END ERR_CODE,
                    CASE
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                        THEN 'OK'
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                        THEN '������ ����� ������'
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN '����� ���� � BRM � ��� � 1�'
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN '����� ���� � 1� � ��� � BRM'
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN '��� ������ � 1� � ��� � BRM'
                    END ERR_MSG
              FROM RR5
             ORDER BY CASE
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                        THEN 0
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                         AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                         AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                        THEN 1
                        WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 2
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN 3
                        WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 4 
                      END
        ), RR7 AS (
            SELECT ERR_CODE, ERR_MSG, COUNT(*) CNT 
              FROM RR6
             GROUP BY ERR_CODE, ERR_MSG
             ORDER BY ERR_CODE, ERR_MSG
        )
        --SELECT * FROM RR6
        SELECT * FROM RR7
        ;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ------------------------------------------------------------------------- --
-- ��������� ���������� � ������� ��������� �� ������� ��������� 
-- �� ������ �� ��������� ������������ � BRM � 1�
-- ------------------------------------------------------------------------- --
PROCEDURE R5_BRM_vs_1C_bills(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'R5_BRM_vs_1C_bills';
BEGIN   
     OPEN p_recordset FOR
            WITH R4 AS (
                -- ����������� ������ �� ���������, �� ������� ���� ����������� �����
                SELECT NVL(V.ERP_CODE, V.TPI_ERP_CODE) ERP_CODE,
                       NVL(V.CONTRACT_NO, V.TPI_CONTRACT_NO) CONTRACT_NO, 
                       SUM(V.TOTAL) BILL_TOTAL, SUM(V.TPI_DUE_RUR) TPI_TOTAL, 
                       SUM(V.TPI_GROSS_RUR) TPI_GROSS_RUR, SUM(V.TPI_TAX_AMOUNT) TPI_TAX_AMOUNT
                  FROM PK25_REPORT_BILL_T V
                 GROUP BY NVL(V.ERP_CODE, V.TPI_ERP_CODE),
                          NVL(V.CONTRACT_NO, V.TPI_CONTRACT_NO)
            ), AB AS (  -- 10.269
                SELECT DISTINCT ERP_CODE, CONTRACT_NO 
                  FROM PK25_REPORT_QUEUE_T
            ), R5 AS (  -- 10.269
                SELECT AB.ERP_CODE,
                       AB.CONTRACT_NO, 
                       BILL_TOTAL, TPI_TOTAL, 
                       TPI_GROSS_RUR, TPI_TAX_AMOUNT
                  FROM R4, AB   
                 WHERE R4.CONTRACT_NO(+) = AB.CONTRACT_NO
                   AND R4.ERP_CODE(+)    = AB.ERP_CODE
            ), E AS (
                SELECT E.ERP_CODE, E.CONTRACT_NO, SUM(E.BILL_TOTAL) BILL_TOTAL_1C
                  FROM PK25_BAL_1C_T E
                 GROUP BY E.ERP_CODE, E.CONTRACT_NO
            ), RR5 AS (
                SELECT R5.ERP_CODE, R5.CONTRACT_NO, R5.BILL_TOTAL, E.BILL_TOTAL_1C
                  FROM R5, E 
                 WHERE R5.CONTRACT_NO = E.CONTRACT_NO(+)
                   AND R5.ERP_CODE    = E.ERP_CODE(+)
            ), RR6 AS (
                SELECT RR5.*,
                        CASE
                            WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                             AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                             AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                            THEN 0
                            WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                             AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                             AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                            THEN 1
                            WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 2
                            WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN 3
                            WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 4 
                        END ERR_CODE,
                        CASE
                            WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                             AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                             AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                            THEN 'OK'
                            WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                             AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                             AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                            THEN '������ ����� ������'
                            WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN '����� ���� � BRM � ��� � 1�'
                            WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN '����� ���� � 1� � ��� � BRM'
                            WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN '��� ������ � 1� � ��� � BRM'
                        END ERR_MSG
                  FROM RR5
                 ORDER BY CASE
                            WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                             AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                             AND RR5.BILL_TOTAL = RR5.BILL_TOTAL_1C  
                            THEN 0
                            WHEN NVL(RR5.BILL_TOTAL,0) != 0 
                             AND NVL(RR5.BILL_TOTAL_1C,0) != 0 
                             AND RR5.BILL_TOTAL != RR5.BILL_TOTAL_1C  
                            THEN 1
                            WHEN NVL(RR5.BILL_TOTAL,0) != 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 2
                            WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0) != 0 THEN 3
                            WHEN NVL(RR5.BILL_TOTAL,0)  = 0 AND NVL(RR5.BILL_TOTAL_1C,0)  = 0 THEN 4 
                          END
            ), RR7 AS (
                SELECT ERR_CODE, ERR_MSG, COUNT(*) CNT 
                  FROM RR6
                 GROUP BY ERR_CODE, ERR_MSG
                 ORDER BY ERR_CODE, ERR_MSG    
            ), RR8 AS (
                SELECT * FROM RR6
                 WHERE ERR_CODE = 1
                 ORDER BY ABS(BILL_TOTAL - BILL_TOTAL_1C) DESC
            ), RR9 AS (
                SELECT ERP_CODE, CONTRACT_NO, REP_PERIOD_ID, BILL_NO, TOTAL, CURRENCY_ID, JOURNAL_ID, SESSION_ID 
                  FROM PK25_REPORT_BILL_T V
                WHERE EXISTS (
                    SELECT * 
                      FROM RR8
                     WHERE RR8.ERP_CODE = V.ERP_CODE
                       AND RR8.CONTRACT_NO = V.CONTRACT_NO
                    ) 
            )
            SELECT RR9.ERP_CODE, RR9.CONTRACT_NO, RR9.REP_PERIOD_ID, RR9.BILL_NO,
                   RR9.TOTAL, RR9.CURRENCY_ID, RR9.JOURNAL_ID, RR9.SESSION_ID,
                   NULL TOTAL_BRM, NULL TOTAL_1C 
              FROM RR9
            UNION ALL
            SELECT RR8.ERP_CODE, RR8.CONTRACT_NO, NULL REP_PERIOD_ID, NULL BILL_NO,
                   NULL TOTAL, NULL CURRENCY_ID, NULL JOURNAL_ID, NULL SESSION_ID,
                   RR8.BILL_TOTAL BILL_TOTAL_BRM, RR8.BILL_TOTAL_1C 
              FROM RR8
            ORDER BY ERP_CODE, CONTRACT_NO, REP_PERIOD_ID NULLS LAST
        ;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ========================================================================= --
-- ������ �������� � BRM � 1�
-- ------------------------------------------------------------------------- --
-- ������� �������� � BRM �� ������
-- ------------------------------------------------------------------------- --
PROCEDURE RP1_BRM_payments(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc)
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'RP1_BRM_payments';
BEGIN   
     OPEN p_recordset FOR
        SELECT Q.ACCOUNT_ID, Q.ACCOUNT_NO, Q.CONTRACT_NO, Q.ERP_CODE, 
               P.REP_PERIOD_ID, P.RECVD, P.PAYMENT_DATE, P.DOC_ID, PS.PAYSYSTEM_NAME, 
               P.PAY_DESCR, P.CREATE_DATE, P.NOTES, P.CURRENCY_ID
          FROM PAYMENT_T P, PAYSYSTEM_T PS, PK25_REPORT_QUEUE_T Q, PK25_REPORT_PERIOD_T RP
         WHERE P.ACCOUNT_ID = Q.ACCOUNT_ID
           AND P.REP_PERIOD_ID BETWEEN RP.PERIOD_ID_FROM AND RP.PERIOD_ID_TO
           AND P.PAYSYSTEM_ID = PS.PAYSYSTEM_ID(+)
         ORDER BY Q.ACCOUNT_ID, Q.ACCOUNT_NO, Q.CONTRACT_NO, Q.ERP_CODE, P.PAYMENT_DATE
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ------------------------------------------------------------------------- --
-- ��������� �������� �� �������� ����������� �������� � BRM � 1�
-- ------------------------------------------------------------------------- --
PROCEDURE RP2_BRM_cont_pay_comp(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'RP2_BRM_cont_pay_comp';
BEGIN   
     OPEN p_recordset FOR
        WITH BP AS (
         SELECT Q.ERP_CODE, Q.CONTRACT_NO, SUM(P.RECVD) RECVD 
           FROM PAYMENT_T P, PK25_REPORT_QUEUE_T Q, PK25_REPORT_PERIOD_T RP
          WHERE P.ACCOUNT_ID = Q.ACCOUNT_ID
            AND P.REP_PERIOD_ID BETWEEN RP.PERIOD_ID_FROM AND RP.PERIOD_ID_TO
         GROUP BY Q.CONTRACT_NO, Q.ERP_CODE
        ), CP AS (
            SELECT ERP_CODE, CONTRACT_NO, RECVD FROM PK25_BAL_1C_T T
        ), RP AS ( 
        SELECT NVL(BP.ERP_CODE, CP.ERP_CODE) ERP_CODE,
               NVL(BP.CONTRACT_NO, CP.CONTRACT_NO) CONTRACT_NO,
               BP.RECVD RECVD_BRM, CP.RECVD RECVD_1C  
          FROM BP FULL OUTER JOIN CP ON (BP.ERP_CODE = CP.ERP_CODE AND BP.CONTRACT_NO = CP.CONTRACT_NO)
        ), CMP AS ( 
        SELECT ERP_CODE, CONTRACT_NO, RECVD_BRM, RECVD_1C,
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 0
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN 1
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN 2
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN 3
               END ERR_CODE,
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 'OK'
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN '������ ����� ��������'
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN '������� ���� � BRM � ��� � 1�'
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN '������� ���� � 1� � ��� � BRM'
               END ERR_MSG
          FROM RP
         ORDER BY 
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 0
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN 1
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN 2
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN 3
               END
        ), STAT AS (
        SELECT ERR_CODE, ERR_MSG, COUNT(*) CNT 
          FROM CMP
         GROUP BY ERR_CODE, ERR_MSG
         ORDER BY ERR_CODE
        )
        SELECT * FROM CMP
        --SELECT * FROM STAT
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ------------------------------------------------------------------------- --
-- ���������� �� ����������� ��������� �������� �� �������� �����������
-- ------------------------------------------------------------------------- --
PROCEDURE RP3_BRM_cont_pay_stat(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'RP3_BRM_cont_pay_stat';
BEGIN   
     OPEN p_recordset FOR
        WITH BP AS (
         SELECT Q.ERP_CODE, Q.CONTRACT_NO, SUM(P.RECVD) RECVD 
           FROM PAYMENT_T P, PK25_REPORT_QUEUE_T Q, PK25_REPORT_PERIOD_T RP
          WHERE P.ACCOUNT_ID = Q.ACCOUNT_ID
            AND P.REP_PERIOD_ID BETWEEN RP.PERIOD_ID_FROM AND RP.PERIOD_ID_TO
         GROUP BY Q.CONTRACT_NO, Q.ERP_CODE
        ), CP AS (
            SELECT ERP_CODE, CONTRACT_NO, RECVD FROM PK25_BAL_1C_T T
        ), RP AS ( 
        SELECT NVL(BP.ERP_CODE, CP.ERP_CODE) ERP_CODE,
               NVL(BP.CONTRACT_NO, CP.CONTRACT_NO) CONTRACT_NO,
               BP.RECVD RECVD_BRM, CP.RECVD RECVD_1C  
          FROM BP FULL OUTER JOIN CP ON (BP.ERP_CODE = CP.ERP_CODE AND BP.CONTRACT_NO = CP.CONTRACT_NO)
        ), CMP AS ( 
        SELECT ERP_CODE, CONTRACT_NO, RECVD_BRM, RECVD_1C,
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 0
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN 1
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN 2
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN 3
               END ERR_CODE,
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 'OK'
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN '������ ����� ��������'
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN '������� ���� � BRM � ��� � 1�'
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN '������� ���� � 1� � ��� � BRM'
               END ERR_MSG
          FROM RP
         ORDER BY 
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 0
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN 1
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN 2
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN 3
               END
        ), STAT AS (
        SELECT ERR_CODE, ERR_MSG, COUNT(*) CNT 
          FROM CMP
         GROUP BY ERR_CODE, ERR_MSG
         ORDER BY ERR_CODE
        )
        --SELECT * FROM CMP
        SELECT * FROM STAT
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ------------------------------------------------------------------------- --
-- ��������� �������� �� ����������� �������� � BRM � 1�
-- ------------------------------------------------------------------------- --
PROCEDURE RP4_BRM_ecode_pay_comp(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'RP2_BRM_ecode_pay_comp';
BEGIN   
     OPEN p_recordset FOR
        WITH BP AS (
         SELECT Q.ERP_CODE, SUM(P.RECVD) RECVD 
           FROM PAYMENT_T P, PK25_REPORT_QUEUE_T Q, PK25_REPORT_PERIOD_T RP
          WHERE P.ACCOUNT_ID = Q.ACCOUNT_ID
            AND P.REP_PERIOD_ID BETWEEN RP.PERIOD_ID_FROM AND RP.PERIOD_ID_TO
         GROUP BY Q.ERP_CODE
        ), CP AS (
            SELECT ERP_CODE, SUM(RECVD) RECVD 
              FROM PK25_BAL_1C_T T
             GROUP BY ERP_CODE
        ), RP AS ( 
        SELECT NVL(BP.ERP_CODE, CP.ERP_CODE) ERP_CODE,
               BP.RECVD RECVD_BRM, CP.RECVD RECVD_1C  
          FROM BP FULL OUTER JOIN CP ON (BP.ERP_CODE = CP.ERP_CODE)
        ), CMP AS ( 
        SELECT ERP_CODE, RECVD_BRM, RECVD_1C,
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 0
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN 1
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN 2
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN 3
               END ERR_CODE,
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 'OK'
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN '������ ����� ��������'
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN '������� ���� � BRM � ��� � 1�'
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN '������� ���� � 1� � ��� � BRM'
               END ERR_MSG
          FROM RP
         ORDER BY 
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 0
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN 1
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN 2
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN 3
               END
        ), STAT AS (
        SELECT ERR_CODE, ERR_MSG, COUNT(*) CNT 
          FROM CMP
         GROUP BY ERR_CODE, ERR_MSG
         ORDER BY ERR_CODE
        )
        SELECT * FROM CMP
        --SELECT * FROM STAT
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ------------------------------------------------------------------------- --
-- ���������� �� ����������� ��������� �������� �� �����������
-- ------------------------------------------------------------------------- --
PROCEDURE RP5_BRM_ecode_pay_stat(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'RP5_BRM_ecode_pay_stat';
BEGIN   
     OPEN p_recordset FOR
        WITH BP AS (
         SELECT Q.ERP_CODE, SUM(P.RECVD) RECVD 
           FROM PAYMENT_T P, PK25_REPORT_QUEUE_T Q, PK25_REPORT_PERIOD_T RP
          WHERE P.ACCOUNT_ID = Q.ACCOUNT_ID
            AND P.REP_PERIOD_ID BETWEEN RP.PERIOD_ID_FROM AND RP.PERIOD_ID_TO
         GROUP BY Q.ERP_CODE
        ), CP AS (
            SELECT ERP_CODE, SUM(RECVD) RECVD 
              FROM PK25_BAL_1C_T T
             GROUP BY ERP_CODE
        ), RP AS ( 
        SELECT NVL(BP.ERP_CODE, CP.ERP_CODE) ERP_CODE,
               BP.RECVD RECVD_BRM, CP.RECVD RECVD_1C  
          FROM BP FULL OUTER JOIN CP ON (BP.ERP_CODE = CP.ERP_CODE)
        ), CMP AS ( 
        SELECT ERP_CODE, RECVD_BRM, RECVD_1C,
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 0
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN 1
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN 2
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN 3
               END ERR_CODE,
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 'OK'
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN '������ ����� ��������'
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN '������� ���� � BRM � ��� � 1�'
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN '������� ���� � 1� � ��� � BRM'
               END ERR_MSG
          FROM RP
         ORDER BY 
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 0
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN 1
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN 2
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN 3
               END
        ), STAT AS (
        SELECT ERR_CODE, ERR_MSG, COUNT(*) CNT 
          FROM CMP
         GROUP BY ERR_CODE, ERR_MSG
         ORDER BY ERR_CODE
        )
        --SELECT * FROM CMP
        SELECT * FROM STAT
      ; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ------------------------------------------------------------------------- --
-- ��������� ��������� �������� �� �������� ����������� �������� � BRM � 1�
-- ------------------------------------------------------------------------- --
PROCEDURE RP6_BRM_detail_pay_comp(
          p_result    OUT VARCHAR2,  
          p_recordset OUT t_refc )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'RP6_BRM_detail_pay_comp';
BEGIN   
     OPEN p_recordset FOR
        WITH BP AS (
         SELECT Q.ERP_CODE, Q.CONTRACT_NO, SUM(P.RECVD) RECVD 
           FROM PAYMENT_T P, PK25_REPORT_QUEUE_T Q, PK25_REPORT_PERIOD_T RP
          WHERE P.ACCOUNT_ID = Q.ACCOUNT_ID
            AND P.REP_PERIOD_ID BETWEEN RP.PERIOD_ID_FROM AND RP.PERIOD_ID_TO
         GROUP BY Q.CONTRACT_NO, Q.ERP_CODE
        ), CP AS (
            SELECT ERP_CODE, CONTRACT_NO, RECVD FROM PK25_BAL_1C_T T
        ), RP AS ( 
        SELECT NVL(BP.ERP_CODE, CP.ERP_CODE) ERP_CODE,
               NVL(BP.CONTRACT_NO, CP.CONTRACT_NO) CONTRACT_NO,
               BP.RECVD RECVD_BRM, CP.RECVD RECVD_1C  
          FROM BP FULL OUTER JOIN CP ON (BP.ERP_CODE = CP.ERP_CODE AND BP.CONTRACT_NO = CP.CONTRACT_NO)
        ), CMP AS ( 
        SELECT ERP_CODE, CONTRACT_NO, RECVD_BRM, RECVD_1C,
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 0
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN 1
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN 2
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN 3
               END ERR_CODE,
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 'OK'
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN '������ ����� ��������'
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN '������� ���� � BRM � ��� � 1�'
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN '������� ���� � 1� � ��� � BRM'
               END ERR_MSG
          FROM RP
         ORDER BY 
               CASE
                WHEN RECVD_BRM = RECVD_1C THEN 0
                WHEN RECVD_BRM IS NOT NULL AND RECVD_1C IS NOT NULL AND RECVD_BRM != RECVD_1C THEN 1
                WHEN NVL(RECVD_BRM,0)!= 0 AND NVL(RECVD_1C,0) = 0 THEN 2
                WHEN NVL(RECVD_BRM,0) = 0 AND NVL(RECVD_1C,0)!= 0 THEN 3
               END
        ), STAT AS (
        SELECT ERR_CODE, ERR_MSG, COUNT(*) CNT 
          FROM CMP
         GROUP BY ERR_CODE, ERR_MSG
         ORDER BY ERR_CODE
        ), R1 AS (
            SELECT * FROM CMP
             WHERE CMP.ERR_CODE IN (1,2,3)
        ), R2 AS (
            SELECT Q.ACCOUNT_ID, Q.ACCOUNT_NO, Q.CONTRACT_NO, Q.ERP_CODE, 
                   P.REP_PERIOD_ID, P.RECVD, P.PAYMENT_DATE, P.DOC_ID, PS.PAYSYSTEM_NAME, 
                   P.PAY_DESCR, P.CREATE_DATE, P.NOTES, P.CURRENCY_ID
              FROM PAYMENT_T P, PAYSYSTEM_T PS, PK25_REPORT_QUEUE_T Q, PK25_REPORT_PERIOD_T RP, R1
             WHERE P.ACCOUNT_ID = Q.ACCOUNT_ID
               AND P.REP_PERIOD_ID BETWEEN RP.PERIOD_ID_FROM AND RP.PERIOD_ID_TO
               AND P.PAYSYSTEM_ID = PS.PAYSYSTEM_ID(+)
               AND Q.ERP_CODE = R1.ERP_CODE
               AND Q.CONTRACT_NO = R1.CONTRACT_NO
             ORDER BY Q.ACCOUNT_ID, Q.ACCOUNT_NO, Q.CONTRACT_NO, Q.ERP_CODE, P.PAYMENT_DATE
        )
        SELECT R1.ERP_CODE, R1.CONTRACT_NO, R1.RECVD_BRM, R1.RECVD_1C, R1.ERR_CODE, R1.ERR_MSG,
               NULL REP_PERIOD_ID, NULL RECVD, NULL PAYMENT_DATE, NULL DOC_ID, NULL PAYSYSTEM_NAME, 
               NULL PAY_DESCR, NULL CREATE_DATE, NULL NOTES, NULL CURRENCY_ID
          FROM R1
        UNION ALL 
        SELECT R2.ERP_CODE, R2.CONTRACT_NO, NULL RECVD_BRM, NULL RECVD_1C, NULL ERR_CODE, NULL ERR_MSG,
               R2.REP_PERIOD_ID, R2.RECVD, R2.PAYMENT_DATE, R2.DOC_ID, R2.PAYSYSTEM_NAME, 
               R2.PAY_DESCR, R2.CREATE_DATE, R2.NOTES, R2.CURRENCY_ID
          FROM R2
        ORDER BY ERP_CODE, CONTRACT_NO, ERR_CODE NULLS FIRST
      ;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;


END PK25_REPORT_1C;
/
