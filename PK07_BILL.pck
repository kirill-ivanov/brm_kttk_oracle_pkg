CREATE OR REPLACE PACKAGE PK07_BILL
IS
    --
    -- ����� ��� ������ � �������� "����", �������:
    -- bill_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK07_BILL';
    -- ==============================================================================
    type t_refc is ref cursor;
   
    -- �������� ������� �����
    PROCEDURE Check_bill_period(
                 p_bill_period_id IN INTEGER,
                 p_account_id     IN INTEGER
              );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- ��������� ������� � ��������� ���� ����� ��� ������� �����:
    --   - ������������� - ID ������� ����� 
    --   - ��� ������ ���������� ����������
    FUNCTION Get_manual_bill_period (
                   p_bill_date   IN OUT DATE   -- ���� ����� (������������ �������)
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- �������� ������ �������� �����, ����������:
    --   - ������������� - ID �������� �����, 
    --   - ��� ������ ���������� ����������
    FUNCTION Open_manual_bill (
                   p_account_id    IN INTEGER,   -- ID �������� �����
                   p_rep_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM
                   p_bill_no       IN VARCHAR2,  -- ����� �����
                   p_currency_id   IN INTEGER,   -- ID ������ �����
                   p_bill_date     IN DATE,      -- ���� ����� (������������ �������)
                   p_notes         IN VARCHAR2   -- ���������� � �����
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- �������!!! ����� ��������� � ���
    -- �������� ���������� �����, ����������:
    --   - ������������� - ID �������� �����, 
    --   - ��� ������ ���������� ����������
    FUNCTION Open_prepaid_bill (
                   p_account_id    IN INTEGER,   -- ID �������� �����
                   p_rep_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM
                   p_bill_no       IN VARCHAR2,  -- ����� �����
                   p_currency_id   IN INTEGER,   -- ID ������ �����
                   p_bill_date     IN DATE,      -- ���� ����� (������������ �������)
                   p_notes         IN VARCHAR2   -- ���������� � �����
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- �������� ���������� �����, ����������:
    --   - ������������� - ID �������� �����, 
    --   - ��� ������ ���������� ����������
    FUNCTION Open_prepaid_bill (
                   p_account_id    IN INTEGER,   -- ID �������� �����
                   p_bill_no       IN VARCHAR2,  -- ����� �����
                   p_currency_id   IN INTEGER,   -- ID ������ �����
                   p_bill_date     IN DATE,      -- ���� ����� (������������ �������)
                   p_notes         IN VARCHAR2   -- ���������� � �����
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���������� ����� � ��������� ������
    -- ���������� 
    --  bill_id - ������������� �����
    -- -1 - ������, ���� ����������� ��������� �������
    -- -2 - ������, �� ���� ���� ������, �������� �����-���� (BILL_T.NEXT_BILL_ID is not null)
    -- ���� p_force = 1, �� ������� �� ��������� ������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- 
    FUNCTION Move_prepaid_bill (
                  p_bill_id      IN INTEGER,
                  p_period_id_to IN INTEGER,
                  p_bill_date_to IN DATE
              ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������ �������������� �����, ����������:
    --   - ������������� - ID �������� �����, 
    --   - ��� ������ ���������� ����������
    FUNCTION Open_recuring_bill (
                p_account_id    IN INTEGER,   -- ID �������� �����
                p_rep_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM
                p_bill_no       IN VARCHAR2,  -- ����� �����
                p_currency_id   IN INTEGER,   -- ID ������ �����
                p_bill_date     IN DATE       -- ���� ����� (������������ �������)
          ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ���������� �� ������� �������������� �����, ����������:
    --   - ������������� - ID �������� �����, 
    --   - ��� ������ ���������� ����������
    FUNCTION Next_recuring_bill (
                   p_account_id    IN INTEGER,   -- ID �������� �����
                   p_rep_period_id IN INTEGER    -- ID ���������� ������� YYYYMM
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���� ��������� � ������ ���� �� �����-�� ������� ���� �� 
    -- ��������� ������ �� ��� ���������, � ������ �� ����� 
    -- ����� ���������� ���������� ����� � ��������� ����,
    -- � �� ������ ���������� ��������� � ������� ����
    -- ����������:
    --   - ������������� - ID �����, 
    --   - ��� ������ ���������� ����������
    FUNCTION Open_rec_bill_for_old_period (
                   p_account_id    IN INTEGER,   -- ID �������� �����
                   p_rep_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM
                   p_bill_no       IN VARCHAR2,  -- ����� �����
                   p_currency_id   IN INTEGER,   -- ID ������ �����
                   p_bill_date     IN DATE,       -- ���� ����� (������������ �������)
                   p_notes         IN VARCHAR2    DEFAULT NULL
                ) RETURN INTEGER;

    --================================================================================
    -- �������������� ����� �����
    PROCEDURE Bill_info_edit(
             p_bill_id           IN INTEGER,   -- ID ������� �����
             p_rep_period_id     IN INTEGER,    -- ID ������� �����
             p_bill_No           IN VARCHAR2,
             p_currency_id       IN INTEGER,
             p_bill_date         IN DATE,
             p_rep_period_id_new IN INTEGER,
             p_bill_type         IN VARCHAR2 DEFAULT NULL,
             p_notes             IN VARCHAR2 DEFAULT NULL
    );

    -- ===============================================================================
    -- �������� �����
    -- ����������:
    --  0 - ���� ������
    -- -1 - ������, ���� ����������� ��������� �������
    -- -2 - ������, �� ���� ���� ������, �������� �����-���� (BILL_T.NEXT_BILL_ID is not null)
    -- ���� ����� �������� p_force, �� ����� ������� ���� �� ��������� �������
    -- =============================================================================== 
    FUNCTION Delete_bill (
                  p_bill_id      IN INTEGER,
                  p_period_id    IN INTEGER,
                  p_force        IN INTEGER DEFAULT 0
              ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �����
    -- ��� ������ ���������� ���������� 
    PROCEDURE Set_status (
                   p_bill_id       IN INTEGER,   -- ID ����� 
                   p_rep_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM
                   p_bill_status   IN VARCHAR2   -- ������ �����
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������ �����, ����������
    -- - ������ �����
    -- - ��� ������ ���������� ���������� 
    FUNCTION Get_status (
                   p_bill_id       IN INTEGER,
                   p_rep_period_id IN INTEGER    -- ID ���������� ������� YYYYMM
               ) RETURN VARCHAR2;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���� 
    -- ��� ������ ���������� ����������
    PROCEDURE Close_bill( p_bill_id IN INTEGER, p_rep_period_id IN INTEGER );


    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������������ ��������� �������� �������
    -- ���������� ���������� ����� �����
    FUNCTION Check_region_prefix ( 
                 p_bill_id   IN INTEGER,
                 p_period_id IN INTEGER 
             ) RETURN VARCHAR2;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ��������� ����� �������������� ����� � BRM
    -- ������ ������ ����������� �� ������� �������: YYMM(� �/�)[A-Z]
    FUNCTION Next_rec_bill_no(
                   p_account_id     IN INTEGER,
                   p_bill_period_id IN INTEGER
               ) RETURN VARCHAR2;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ��������� ����� �������������� ����� � BRM
    -- ������ ������ ����������� �� ������� �������: YYMM(� �/�)[A-Z]
    FUNCTION Next_bill_no(
                   p_account_id     IN INTEGER,
                   p_bill_period_id IN INTEGER
               ) RETURN VARCHAR2;
    
    /*       
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� 31.12.2014
    -- �������� ��������� ����� �������������� �����,
    -- ������� ���������, ��� � �������� "���������" � "������ ��������" 
    -- ������ ������ ����������� �� ������ ��������:
    -- "���������" - CONTRACT_NO_XXXX, ��� XXXX - ���������� ����� �����
    --               ������� ���������, ��� �� ����� ��������, ����� ����
    --               ��������� ������� ������
    -- "������ ��������" - YYMM(� �/�)[A-Z]
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Next_bill_no(
                   p_account_id     IN INTEGER,
                   p_contract_id    IN INTEGER,
                   p_bill_period_id IN INTEGER  
               ) RETURN VARCHAR2;
    */
    
    -- ��������� ��������� �������������� ������ � ������� CONTRACT_BILL_SQ_T
    PROCEDURE Fill_contract_bill_sq_t;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ��������� ������ ��� ������ �/�
    --   - ��� ������ ���������� ����������
    PROCEDURE New_billinfo (
                   p_account_id       IN INTEGER,   -- ID �������� �����
                   p_currency_id      IN INTEGER,   -- ID ������ �����
                   p_delivery_id      IN INTEGER,   -- ID ������� �������� �����
                   p_days_for_payment IN INTEGER DEFAULT 30   -- ���-�� ���� �� ������ �����
               );

    /*
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ����: �������� ����� ���������� � ������������� ����� �� ��������
    -- � ���������� �������, ��� ���� ����������� , 
    -- �.�. ���������� �� ��� ������� �� ��������, 
    -- ����������� ������ ������
    -- ����������:
    -- - ���������� ����� ���������� � ������������� �� ����� (�� ��� ������ ���� ��������)
    -- - ��� ������ ���������� ����������
    FUNCTION Generate_bill (
                   p_bill_id       IN INTEGER,   -- ID ������� �����
                   p_rep_period_id IN INTEGER    -- ID ������� �����
               ) RETURN NUMBER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������������� ���� (������ ��� ������� READY): 
    -- �������� ����� ���������� � ������������� ����� �� ��������
    -- ������� ������ ����� �������, ���� ���� ������������
    -- � ������� ������� OPEN,
    -- �.�. ������� ���������� ���������� �� ��� ������� 
    -- ����������:
    --   - ������������� - ID �����, 
    --   - ��� ������ ���������� ����������
    FUNCTION Rollback_bill (
                   p_bill_id       IN INTEGER,   -- ID ������� �����
                   p_rep_period_id IN INTEGER    -- ID ������� �����
               ) RETURN NUMBER;

    --
    -- ����� �����. ��������������� �������. ����� ���� �����
    --
    FUNCTION Rollback_bill_force (
                   p_bill_id       IN INTEGER,   -- ID ������� �����
                   p_rep_period_id IN INTEGER    -- ID ������� �����
               ) RETURN NUMBER;
    */
    -- ===============================================================================
    -- ������� ����� � ������ ������ ������ �� ������� ���, 
    -- �.�. �������� ���� �� ������ ��� ������� � ����������
    -- ���������� 
    --  bill_id - ������������� �����
    -- -1 - ������, ���� ����������� ��������� �������
    -- -2 - ������, �� ���� ���� ������, �������� �����-���� (BILL_T.NEXT_BILL_ID is not null)
    -- ���� p_force = 1, �� ������� �� ��������� ������� ��������
    -- =============================================================================== 
    FUNCTION Move_bill (
                  p_bill_id      IN INTEGER,
                  p_period_id    IN INTEGER,
                  p_period_id_to IN INTEGER,
                  p_bill_date_to IN DATE,
                  p_force        IN INTEGER DEFAULT 1
              ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������������� �� �����
    -- ����������:
    -- - ���������� ����� ������������� �� �����
    -- - ��� ������ ���������� ����������
    FUNCTION Calculate_due(
                   p_bill_id       IN INTEGER,   -- ID ������� �����
                   p_rep_period_id IN INTEGER    -- ID ������� �����
               ) RETURN NUMBER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����� ��� ������� ���������� �����
    --   - ������������� - ���-�� ��������� �������
    --   - ��� ������ ���������� ����������
    FUNCTION Items_list( 
                   p_recordset OUT t_refc, 
                   p_bill_id       IN INTEGER,   -- ID ������� �����
                   p_rep_period_id IN INTEGER    -- ID ������� �����
               ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ��� ������� ���������� ����� (������ ����� ������ ����� ����� ���������� ��������)
    --   - ������������� - ���-�� ��������� �������
    --   - ��� ������ ���������� ����������
    FUNCTION Delete_items (
                   p_bill_id       IN INTEGER,   -- ID ������� �����
                   p_rep_period_id IN INTEGER    -- ID ������� �����
               ) RETURN INTEGER;
  
    -- =============================================================== --
    -- ��������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ �� ������� �/�
    --
    PROCEDURE Read_account_profile (
                   p_account_id    IN INTEGER,   -- ID �������� �����
                   p_bill_date     IN DATE,      -- ���� �����
                   p_profile_id    OUT INTEGER,  -- ID ������� �/�
                   p_contract_id   OUT INTEGER,  -- ID ��������
                   p_contractor_id OUT INTEGER,  -- ID ��������
                   p_bank_id       OUT INTEGER,  -- ID ����� ��������
                   p_vat           OUT INTEGER   -- ������ ���
               );

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� �� ������� ������������� ������ �� ������� ����� � �������� �������
FUNCTION Check_BillRec_AtPeriod_Has ( 
      p_account_id   IN INTEGER,
      p_period_id    IN INTEGER 
) RETURN INTEGER;

---------------------------------------------------------------
-- �������� ���������� � �����
---------------------------------------------------------------
FUNCTION GET_BILL_NOTES(p_bill_id IN NUMBER) RETURN VARCHAR2;


END PK07_BILL;
/
CREATE OR REPLACE PACKAGE BODY PK07_BILL
IS

-- �������� ������� �����
PROCEDURE Check_bill_period(
             p_bill_period_id IN INTEGER,
             p_account_id     IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_bill_period';
    v_position   PERIOD_T.POSITION%TYPE;
    v_billing_id INTEGER;
BEGIN
    SELECT P.POSITION 
      INTO v_position
      FROM PERIOD_T P
     WHERE P.PERIOD_ID = p_bill_period_id;
    IF v_position NOT IN ('BILL','OPEN','NEXT') THEN
       -- ������ ������, �������� ������ ���������, ����� ��������� ��������
       SELECT A.BILLING_ID
         INTO v_billing_id
         FROM ACCOUNT_T A
        WHERE A.ACCOUNT_ID = p_account_id;
       IF v_billing_id NOT IN ( 2008, 2009 ) THEN
          Pk01_Syslog.Raise_user_exception('period_id='||p_bill_period_id||' is closed' , c_PkgName||'.'||v_prcName);
       END IF;
    END IF;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- ������ �� �������� ��������, ��� �� ������ � PERIOD_T
        Pk01_Syslog.Raise_user_exception('period_id='||p_bill_period_id||' not found in period_t' , c_PkgName||'.'||v_prcName);
      
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������ �� ������� �/�
--
PROCEDURE Read_account_profile (
               p_account_id    IN INTEGER,   -- ID �������� �����
               p_bill_date     IN DATE,      -- ���� �����
               p_profile_id    OUT INTEGER,  -- ID ������� �/�
               p_contract_id   OUT INTEGER,  -- ID ��������
               p_contractor_id OUT INTEGER,  -- ID ��������
               p_bank_id       OUT INTEGER,  -- ID ����� ��������
               p_vat           OUT INTEGER   -- ������ ���
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Read_account_profile';
BEGIN
  SELECT 
         PROFILE_ID, CONTRACT_ID, 
         CONTRACTOR_ID, CONTRACTOR_BANK_ID, VAT
      INTO p_profile_id, p_contract_id,
           p_contractor_id, p_bank_id, p_vat FROM (
    SELECT AP.PROFILE_ID, AP.CONTRACT_ID,
           AP.CONTRACTOR_ID, AP.CONTRACTOR_BANK_ID, AP.VAT,
           ROW_NUMBER() OVER(PARTITION BY ACCOUNT_ID ORDER BY DECODE(ACTUAL,'Y',0,1)) rn      
      FROM ACCOUNT_PROFILE_T AP
     WHERE AP.ACCOUNT_ID = p_account_id
       AND AP.DATE_FROM <= p_bill_date
       AND (AP.DATE_TO IS NULL OR p_bill_date <= AP.DATE_TO 
             OR AP.ACTUAL = 'Y' -- ��������� �� ������������ � �������� �.�. 13.05.2016
           )
    )
  WHERE rn = 1; -- ��� ���������
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
-- ��������� ������� � ��������� ���� ����� ��� ������� �����:
--   - ������������� - ID ������� ����� 
--   - ��� ������ ���������� ����������
FUNCTION Get_manual_bill_period (
               p_bill_date   IN OUT DATE   -- ���� ����� (������������ �������)
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Get_manual_bill_period';
    v_period_id     INTEGER;
    v_min_period_id INTEGER;
    v_min_date_from DATE;
BEGIN
    --
    BEGIN
      -- ���� � ��������� ��� �������� ����� ��������
      SELECT PERIOD_ID 
        INTO v_period_id 
        FROM PERIOD_T
       WHERE p_bill_date BETWEEN PERIOD_FROM AND PERIOD_TO
         AND CLOSE_REP_PERIOD IS NULL
      ORDER BY PERIOD_ID DESC;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        -- ���� ����������� �������� ��� ������ ������
        SELECT MIN(PERIOD_ID), MIN(PERIOD_FROM) 
          INTO v_min_period_id, v_min_date_from
          FROM PERIOD_T
         WHERE CLOSE_REP_PERIOD IS NULL;
        IF p_bill_date < v_min_date_from THEN -- �������� ������
           p_bill_date := v_min_date_from;
           v_period_id := v_min_period_id;
        ELSE -- ������� ������
           v_period_id := Pk04_Period.Period_id(p_period => p_bill_date);
        END IF;
      END;
    END;
    RETURN v_period_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
-- �������� ������ �������� �����, ����������:
--   - ������������� - ID �������� �����, 
--   - ��� ������ ���������� ����������
FUNCTION Open_manual_bill (
               p_account_id    IN INTEGER,   -- ID �������� �����
               p_rep_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM
               p_bill_no       IN VARCHAR2,  -- ����� �����
               p_currency_id   IN INTEGER,   -- ID ������ �����
               p_bill_date     IN DATE,      -- ���� ����� (������������ �������)
               p_notes         IN VARCHAR2   -- ���������� � �����
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Open_manual_bill';
    v_bill_id       INTEGER; 
    v_profile_id    INTEGER;
    v_contract_id   INTEGER;
    v_contractor_id INTEGER;
    v_bank_id       INTEGER;
    
    v_vat         NUMBER;
BEGIN
    -- ��������� ������ ����� 
    Check_bill_period(p_rep_period_id, p_account_id);

    -- ��������� ID ������� (POID) ��� ���������� ������������ ������� 
    v_bill_id := Pk02_POID.Next_bill_id;
    
    -- �������� id �������� � ������ ���
    Read_account_profile (
               p_account_id    => p_account_id,
               p_bill_date     => p_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );
    
    -- C������ ������� ���� ��� ������������ ����������
    INSERT INTO BILL_T (
        CONTRACT_ID,     -- ID ��������
        ACCOUNT_ID,      -- ID �������� �����
        BILL_ID,         -- ID �������� �����
        REP_PERIOD_ID,   -- ID ���������� �������
        BILL_TYPE,       -- ��� �����
        BILL_NO,         -- ����� �����
        CURRENCY_ID,     -- ID ������ �����
        BILL_DATE,       -- ���� ����� (������������ �������)
        BILL_STATUS,     -- ��������� ����� - ������
        VAT,             -- ������ ��� 
        PROFILE_ID,      -- ID ������� �/�
        CONTRACTOR_ID,   -- ID ��������
        CONTRACTOR_BANK_ID, -- ID ����� ��������
        NOTES
    )VALUES(
        v_contract_id,
        p_account_id,
        v_bill_id,
        p_rep_period_id,
        PK00_CONST.c_BILL_TYPE_ONT,
        p_bill_no,
        p_currency_id,
        p_bill_date,
        PK00_CONST.c_BILL_STATE_OPEN,
        v_vat,
        v_profile_id,
        v_contractor_id,
        v_bank_id,
        p_notes
    );  
    
    INSERT INTO BILL_HISTORY_T(BILL_ID, REP_PERIOD_ID, ACTION) VALUES(v_bill_id, p_rep_period_id, Pk00_Const.c_BILL_HISTORY_CREATE);
    
    RETURN v_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
-- �������� ���������� �����, ����������:
--   - ������������� - ID �������� �����, 
--   - ��� ������ ���������� ����������
FUNCTION Open_prepaid_bill (
               p_account_id    IN INTEGER,   -- ID �������� �����
               p_rep_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM
               p_bill_no       IN VARCHAR2,  -- ����� �����
               p_currency_id   IN INTEGER,   -- ID ������ �����
               p_bill_date     IN DATE,      -- ���� ����� (������������ �������)
               p_notes         IN VARCHAR2   -- ���������� � �����
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Open_prepaid_bill';
    v_bill_id       INTEGER; 
    v_profile_id    INTEGER;
    v_contract_id   INTEGER;
    v_contractor_id INTEGER;
    v_bank_id       INTEGER;
    v_vat           NUMBER;
BEGIN
    -- ��������� ������ ����� 
    Check_bill_period(p_rep_period_id, p_account_id);  

    -- ��������� ID ������� (POID) ��� ���������� ������������ ������� 
    v_bill_id := Pk02_POID.Next_bill_id;
    
    -- �������� id �������� � ������ ���
    Read_account_profile (
               p_account_id    => p_account_id,
               p_bill_date     => p_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );
    
    -- C������ ������� ���� ��� ������������ ����������
    INSERT INTO BILL_T (
        CONTRACT_ID,     -- ID ��������
        ACCOUNT_ID,      -- ID �������� �����
        BILL_ID,         -- ID �������� �����
        REP_PERIOD_ID,   -- ID ���������� �������
        BILL_TYPE,       -- ��� �����
        BILL_NO,         -- ����� �����
        CURRENCY_ID,     -- ID ������ �����
        BILL_DATE,       -- ���� ����� (������������ �������)
        BILL_STATUS,     -- ��������� ����� - ������
        VAT,             -- ������ ��� 
        PROFILE_ID,      -- ID ������� �/�
        CONTRACTOR_ID,   -- ID ��������
        CONTRACTOR_BANK_ID, -- ID ����� ��������
        NOTES
    )VALUES(
        v_contract_id,
        p_account_id,
        v_bill_id,
        p_rep_period_id,
        PK00_CONST.c_BILL_TYPE_PRE,
        p_bill_no,
        p_currency_id,
        p_bill_date,
        PK00_CONST.c_BILL_STATE_OPEN,
        v_vat,
        v_profile_id,
        v_contractor_id,
        v_bank_id,
        p_notes
    );  
    
    INSERT INTO BILL_HISTORY_T(BILL_ID, REP_PERIOD_ID, ACTION) VALUES(v_bill_id, p_rep_period_id, Pk00_Const.c_BILL_HISTORY_CREATE);
    
    RETURN v_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
-- �������� ���������� �����, ����������:
--   - ������������� - ID �������� �����, 
--   - ��� ������ ���������� ����������
FUNCTION Open_prepaid_bill (
               p_account_id    IN INTEGER,   -- ID �������� �����
               p_bill_no       IN VARCHAR2,  -- ����� �����
               p_currency_id   IN INTEGER,   -- ID ������ �����
               p_bill_date     IN DATE,      -- ���� ����� (������������ �������)
               p_notes         IN VARCHAR2   -- ���������� � �����
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Open_prepaid_bill';
    v_bill_id       INTEGER; 
    v_profile_id    INTEGER;
    v_contract_id   INTEGER;
    v_contractor_id INTEGER;
    v_bank_id       INTEGER;
    v_vat           NUMBER;
    v_period_id     INTEGER := 205001; 
BEGIN

    -- ��������� ID ������� (POID) ��� ���������� ������������ ������� 
    v_bill_id := Pk02_POID.Next_bill_id;
    
    -- �������� id �������� � ������ ���
    Read_account_profile (
               p_account_id    => p_account_id,
               p_bill_date     => p_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );
    
    -- C������ ������� ���� ��� ������������ ����������
    INSERT INTO BILL_T (
        CONTRACT_ID,     -- ID ��������
        ACCOUNT_ID,      -- ID �������� �����
        BILL_ID,         -- ID �������� �����
        REP_PERIOD_ID,   -- ID ���������� �������
        BILL_TYPE,       -- ��� �����
        BILL_NO,         -- ����� �����
        CURRENCY_ID,     -- ID ������ �����
        BILL_DATE,       -- ���� ����� (������������ �������)
        BILL_STATUS,     -- ��������� ����� - ������
        VAT,             -- ������ ��� 
        PROFILE_ID,      -- ID ������� �/�
        CONTRACTOR_ID,   -- ID ��������
        CONTRACTOR_BANK_ID, -- ID ����� ��������
        NOTES
    )VALUES(
        v_contract_id,
        p_account_id,
        v_bill_id,
        v_period_id,
        PK00_CONST.c_BILL_TYPE_PRE,
        p_bill_no,
        p_currency_id,
        p_bill_date,
        PK00_CONST.c_BILL_STATE_OPEN,
        v_vat,
        v_profile_id,
        v_contractor_id,
        v_bank_id,
        p_notes
    );  
    
    INSERT INTO BILL_HISTORY_T(BILL_ID, REP_PERIOD_ID, ACTION) VALUES(v_bill_id, v_period_id, Pk00_Const.c_BILL_HISTORY_CREATE);
    
    RETURN v_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ===============================================================================
-- ������� ���������� ����� � ��������� ������
-- ���������� 
--  bill_id - ������������� �����
-- -1 - ������, ���� ����������� ��������� �������
-- -2 - ������, �� ���� ���� ������, �������� �����-���� (BILL_T.NEXT_BILL_ID is not null)
-- ���� p_force = 1, �� ������� �� ��������� ������� ��������
-- =============================================================================== 
FUNCTION Move_prepaid_bill (
              p_bill_id      IN INTEGER,
              p_period_id_to IN INTEGER,
              p_bill_date_to IN DATE
          ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Move_prepaid_bill';
    v_bill_id     INTEGER;
    v_period_id   INTEGER := 205001;
BEGIN
    -- ��������� ���� � ��������� ������
    v_bill_id := Move_bill (
              p_bill_id,
              v_period_id,
              p_period_id_to,
              p_bill_date_to
          );
    -- ������ ��� ����� �� �����������
    UPDATE BILL_T B
       SET B.BILL_TYPE = Pk00_Const.c_BILL_TYPE_ONT
     WHERE B.REP_PERIOD_ID = p_period_id_to
       AND B.BILL_ID = v_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������ �������������� �����, ����������:
--   - ������������� - ID �������� �����, 
--   - ��� ������ ���������� ����������
FUNCTION Open_recuring_bill (
               p_account_id    IN INTEGER,   -- ID �������� �����
               p_rep_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM
               p_bill_no       IN VARCHAR2,  -- ����� �����
               p_currency_id   IN INTEGER,   -- ID ������ �����
               p_bill_date     IN DATE       -- ���� ����� (������������ �������)
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Open_recuring_bill';
    v_bill_id       INTEGER;
    v_profile_id    INTEGER;
    v_contract_id   INTEGER;
    v_contractor_id INTEGER;
    v_bank_id       INTEGER;   
    v_vat           NUMBER;
BEGIN
    -- ��������� ������ ����� 
    Check_bill_period(p_rep_period_id, p_account_id);  

    -- ��������� ID ������� (POID) ��� ���������� ������������ ������� 
    v_bill_id := Pk02_POID.Next_bill_id;
    
    -- �������� id �������� � ������ ���
    Read_account_profile (
               p_account_id    => p_account_id,
               p_bill_date     => p_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );
    
    -- C������ ������������� ���� ��� ������������ ����������
    INSERT INTO BILL_T (
        CONTRACT_ID,     -- ID ��������
        ACCOUNT_ID,      -- ID �������� �����
        BILL_ID,         -- ID �������� �����
        REP_PERIOD_ID,   -- ID ���������� ������� YYYYMM
        BILL_TYPE,       -- ��� �����
        BILL_NO,         -- ����� �����
        CURRENCY_ID,     -- ID ������ �����
        BILL_DATE,       -- ���� ����� (������������ �������)
        BILL_STATUS,     -- ��������� �����
        PROFILE_ID,      -- ID ������� �/�
        CONTRACTOR_ID,    -- ID ��������
        CONTRACTOR_BANK_ID, -- ID ����� ��������
        VAT              -- ������ ���
    )VALUES(
        v_contract_id,
        p_account_id,
        v_bill_id,
        p_rep_period_id,
        PK00_CONST.c_BILL_TYPE_REC,
        p_bill_no,
        p_currency_id,
        p_bill_date,
        PK00_CONST.c_BILL_STATE_OPEN,
        v_profile_id,
        v_contractor_id,
        v_bank_id,
        v_vat
    );  
    
    INSERT INTO BILL_HISTORY_T(BILL_ID, REP_PERIOD_ID, ACTION) VALUES(v_bill_id, p_rep_period_id, Pk00_Const.c_BILL_HISTORY_CREATE);
    
    RETURN v_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ���������� �� ������� �������������� �����

-- �������� ��������� ����� �������������� �����,
-- ������� ���������, ��� � �������� "���������" � "������ ��������" 
-- ������ ������ ����������� �� ������ ��������:
-- "���������" - CONTRACT_NO_XXXX, ��� XXXX - ���������� ����� �����
--               ������� ���������, ��� �� ����� ��������, ����� ����
--               ��������� ������� ������
-- "������ ��������" - YYMM(� �/�)[A-Z]
-- ����������:
--   - ������������� - ID �������� �����, 
--   - ��� ������ ���������� ����������

FUNCTION Next_recuring_bill (
               p_account_id    IN INTEGER,   -- ID �������� �����
               p_rep_period_id IN INTEGER    -- ID ���������� ������� YYYYMM
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Next_recuring_bill';
    v_bill_id       INTEGER;
    v_bill_date     DATE;
    v_currency_id   INTEGER;
    v_bill_no       BILL_T.BILL_NO%TYPE := NULL;
    v_profile_id    INTEGER;
    v_contract_id   INTEGER;
    v_contractor_id INTEGER;
    v_bank_id       INTEGER;
    v_vat           NUMBER;
BEGIN
    -- ��������� ������ ����� 
    Check_bill_period(p_rep_period_id, p_account_id);  

    -- ���������� ���� �����
    v_bill_date := PK04_PERIOD.Period_to(p_rep_period_id);

    -- �������� id �������� � ������ ���
    Read_account_profile (
               p_account_id    => p_account_id,
               p_bill_date     => v_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );

    -- ��������� ����� ���������� �������������� �����
    v_bill_no := Next_rec_bill_no( p_account_id, p_rep_period_id);

    -- �������� ������ �����
    SELECT A.CURRENCY_ID 
      INTO v_currency_id 
      FROM ACCOUNT_T A
     WHERE A.ACCOUNT_ID = p_account_id;
    
    -- ��������� ID ������� (POID) ��� ���������� ������������ ������� 
    v_bill_id := Pk02_POID.Next_bill_id;
    
    -- C������ ������������� ���� ��� ������������ ����������
    INSERT INTO BILL_T (
        CONTRACT_ID,     -- ID ��������
        ACCOUNT_ID,      -- ID �������� �����
        BILL_ID,         -- ID �������� �����
        REP_PERIOD_ID,   -- ID ���������� ������� YYYYMM
        BILL_TYPE,       -- ��� �����
        BILL_NO,         -- ����� �����
        CURRENCY_ID,     -- ID ������ �����
        BILL_DATE,       -- ���� ����� (������������ �������)
        BILL_STATUS,     -- ��������� �����
        PROFILE_ID,      -- ID ������� �/�
        CONTRACTOR_ID,   -- ID ��������
        CONTRACTOR_BANK_ID, -- ID ����� ��������
        VAT              -- ������ ���
    )VALUES(
        v_contract_id,
        p_account_id,
        v_bill_id,
        p_rep_period_id,
        PK00_CONST.c_BILL_TYPE_REC,
        v_bill_no,
        v_currency_id,
        v_bill_date,
        PK00_CONST.c_BILL_STATE_OPEN,
        v_profile_id,
        v_contractor_id,
        v_bank_id,
        v_vat
    );  

    MERGE INTO BILLINFO_T BI
    USING (
        SELECT v_bill_id BILL_ID, p_rep_period_id REP_PERIOD_ID,
               p_account_id ACCOUNT_ID, 
               1 PERIOD_LENGTH, v_currency_id CURRENCY_ID, 
               30 DAYS_FOR_PAYMENT, 7701 INVOICE_RULE_ID
          FROM DUAL
    ) D
    ON (
       BI.ACCOUNT_ID = D.ACCOUNT_ID
    )
    WHEN MATCHED THEN UPDATE SET PERIOD_LENGTH    = D.PERIOD_LENGTH, 
                                 DAYS_FOR_PAYMENT = D.DAYS_FOR_PAYMENT
    WHEN NOT MATCHED THEN INSERT (
         ACCOUNT_ID, PERIOD_LENGTH, DAYS_FOR_PAYMENT
     ) VALUES (
         D.ACCOUNT_ID, D.PERIOD_LENGTH, D.DAYS_FOR_PAYMENT
    ); 

    /*
    -- �������� ��������� ����� (�������� ������������� � ���� �������)
    UPDATE BILLINFO_T BI
       SET BI.LAST_PERIOD_ID = p_rep_period_id,
           BI.LAST_BILL_ID = v_bill_id
     WHERE BI.ACCOUNT_ID = p_account_id;
     */

    INSERT INTO BILL_HISTORY_T(BILL_ID, REP_PERIOD_ID, ACTION) VALUES(v_bill_id, p_rep_period_id, Pk00_Const.c_BILL_HISTORY_CREATE);

    -- ���������� ����� �����
    RETURN v_bill_id;
    --     
EXCEPTION
    WHEN dup_val_on_index THEN
        Pk01_Syslog.Write_error(p_Msg => 'ERROR. Bill already exists. Account_Id: ' || TO_CHAR(p_Account_Id) || 
                                         ', Period_Id: ' || TO_CHAR(p_Rep_Period_Id), 
                                p_Src => 'pk07_bill.Next_Recuring_Bill'
                               ); 
        RAISE;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Account_Id: ' || TO_CHAR(p_Account_Id) || ', new bill_no: ' || v_bill_no || ', ' || SQLERRM, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���� ��������� � ������ ���� �� �����-�� ������� ���� �� 
-- ��������� ������ �� ��� ���������, � ������ ����� 
-- ����� ���������� ���������� ����� � ��������� ����,
-- � �� ������ ���������� ��������� � ������� ����
-- ����������:
--   - ������������� - ID �����, 
--   - ��� ������ ���������� ����������
FUNCTION Open_rec_bill_for_old_period (
               p_account_id    IN INTEGER,   -- ID �������� �����
               p_rep_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM
               p_bill_no       IN VARCHAR2,  -- ����� �����
               p_currency_id   IN INTEGER,   -- ID ������ �����
               p_bill_date     IN DATE,       -- ���� ����� (������������ �������)
               p_notes         IN VARCHAR2    DEFAULT NULL
            ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Open_rec_bill_for_old_period';
    v_bill_id       INTEGER;                   -- ������ POID: YYMM.XXX.XXX.XXX,
    v_profile_id    INTEGER;
    v_contract_id   INTEGER;
    v_contractor_id INTEGER;
    v_bank_id       INTEGER;
    v_vat           NUMBER;
    v_bill_no       BILL_T.BILL_NO%TYPE;
BEGIN
    -- ��������� ������ ����� 
    Check_bill_period(p_rep_period_id, p_account_id);  

    -- ��������� ID ������� (POID) ��� ���������� ������������ ������� 
    v_bill_id := Pk02_POID.Next_bill_id;
    
    -- ������������� ������ 'O' � ����� ������ �����
    IF SUBSTR(v_bill_no,-1) != 'O' THEN
      v_bill_no := v_bill_no || 'O';
    END IF;
    
    -- �������� id �������� � ������ ���
    Read_account_profile (
               p_account_id    => p_account_id,
               p_bill_date     => p_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );
    
    -- C������ ������������� ���� ��� ������������ ����������
    INSERT INTO BILL_T (
        ACCOUNT_ID,      -- ID �������� �����
        BILL_ID,         -- ID �������� �����
        REP_PERIOD_ID,   -- ID ���������� ������� YYYYMM
        BILL_TYPE,       -- ��� �����
        BILL_NO,         -- ����� �����
        CURRENCY_ID,     -- ID ������ �����
        BILL_DATE,       -- ���� ����� (������������ �������)
        BILL_STATUS,     -- ��������� �����
        PROFILE_ID,      -- ID ������� �/�
        CONTRACT_ID,     -- ID ��������
        CONTRACTOR_ID,   -- ID ��������
        CONTRACTOR_BANK_ID, -- ID ����� ��������
        VAT,
        NOTES
    )VALUES(
        p_account_id,
        v_bill_id,
        p_rep_period_id,
        PK00_CONST.c_BILL_TYPE_OLD,
        p_bill_no,
        p_currency_id,
        p_bill_date,
        PK00_CONST.c_BILL_STATE_OPEN,
        v_profile_id,
        v_contract_id,
        v_contractor_id,
        v_bank_id,
        v_vat,
        p_notes
    ); 
    
    INSERT INTO BILL_HISTORY_T(BILL_ID, REP_PERIOD_ID, ACTION) VALUES(v_bill_id, p_rep_period_id, Pk00_Const.c_BILL_HISTORY_CREATE);
     
    RETURN v_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--================================================================================
-- �������������� ����� �����
PROCEDURE Bill_info_edit(
         p_bill_id           IN INTEGER,   -- ID ������� �����
         p_rep_period_id     IN INTEGER,    -- ID ������� �����
         p_bill_No           IN VARCHAR2,
         p_currency_id       IN INTEGER,
         p_bill_date         IN DATE,
         p_rep_period_id_new IN INTEGER,
         p_bill_type         IN VARCHAR2 DEFAULT NULL,
         p_notes             IN VARCHAR2 DEFAULT NULL
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bill_info_edit';
BEGIN  
    UPDATE BILL_T
        SET BILL_NO = NVL(p_bill_no,BILL_NO),
            CURRENCY_ID = NVL(p_currency_id, CURRENCY_ID),
            BILL_DATE = NVL(p_bill_date, BILL_DATE),
            REP_PERIOD_ID = NVL(p_rep_period_id_new, REP_PERIOD_ID),
            BILL_TYPE = NVL(p_bill_type, BILL_TYPE),
            NOTES = NVL (p_notes, NOTES)
      WHERE BILL_ID = p_bill_id AND REP_PERIOD_ID = p_rep_period_id;      
      
    -- ���� ���������� ������ �����, ����� �������� ��� � �������
    IF p_rep_period_id <> p_rep_period_id_new THEN
        UPDATE BILL_HISTORY_T
          SET REP_PERIOD_ID = p_rep_period_id_new
        WHERE BILL_ID = p_bill_id 
          AND REP_PERIOD_ID = p_rep_period_id;
    END IF;    
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ===============================================================================
-- �������� �����
-- ����������:
--  0 - ���� ������
-- -1 - ������, ���� ����������� ��������� �������
-- -2 - ������, �� ���� ���� ������, �������� �����-���� (BILL_T.NEXT_BILL_ID is not null)
-- ���� ����� �������� p_force, �� ����� ������� ���� �� ��������� �������
-- =============================================================================== 
FUNCTION Delete_bill (
              p_bill_id      IN INTEGER,
              p_period_id    IN INTEGER,
              p_force        IN INTEGER DEFAULT 0
          ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Delete_bill';
    v_bill_type      BILL_T.BILL_TYPE%TYPE;
    v_prev_bill_id   INTEGER; 
    v_prev_period_id INTEGER;
    v_next_bill_id   INTEGER; 
    v_next_period_id INTEGER; 
    v_close_rep_period DATE;
    v_count          INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
   
    -- ������ ��������� �����
    SELECT B.BILL_TYPE, 
           B.PREV_BILL_ID, B.PREV_BILL_PERIOD_ID,
           B.NEXT_BILL_ID, B.NEXT_BILL_PERIOD_ID,
           P.CLOSE_REP_PERIOD
      INTO v_bill_type, 
           v_prev_bill_id, v_prev_period_id,
           v_next_bill_id, v_next_period_id, 
           v_close_rep_period
      FROM BILL_T B, PERIOD_T P
     WHERE B.REP_PERIOD_ID = P.PERIOD_ID
       AND B.BILL_ID = p_bill_id
       AND B.REP_PERIOD_ID = p_period_id;
    --
    -- ���������, ��� ������ ������
    IF v_close_rep_period IS NOT NULL THEN
        -- ��������� ������� �������� ���� ��������� �������
        Pk01_Syslog.Write_msg('bill_id='||p_bill_id||', period='||p_period_id||' - closed'
                             , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_warn );
        IF p_force = 0 THEN
            RETURN -1;
        END IF;  
    END IF;
    --
    -- ���������, ��� ���� ��������� � �������
    IF v_next_bill_id IS NOT NULL THEN
        Pk01_Syslog.Write_msg('bill_id='||p_bill_id||', period='||p_period_id||' - next_bill_id is not null'
                             , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );
        RETURN -2;
    END IF;

    -- ������� ������ �� ��������� ���� � �������������� ������
    IF v_prev_bill_id IS NOT NULL THEN
        UPDATE BILL_T B SET B.NEXT_BILL_ID = NULL, B.NEXT_BILL_PERIOD_ID = NULL
         WHERE B.REP_PERIOD_ID = v_prev_period_id
           AND B.BILL_ID = v_prev_bill_id
        ; 
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('BILL_T.NEXT_BILL_ID: '||v_count||' set to null', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    END IF;

    -- ������� ������� ����������
    DELETE FROM ITEM_T I 
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.BILL_ID = p_bill_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������� ����� 
    DELETE FROM INVOICE_ITEM_T V
     WHERE V.REP_PERIOD_ID = p_period_id
       AND V.BILL_ID = p_bill_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_count||' deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� �������� ����
    DELETE FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_period_id
       AND B.BILL_ID = p_bill_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    RETURN 0;
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ������ �����
-- ��� ������ ���������� ���������� 
PROCEDURE Set_status (
               p_bill_id       IN INTEGER,   -- ID ����� 
               p_rep_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM
               p_bill_status   IN VARCHAR2   -- ������ �����
           ) 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_status';
BEGIN
    UPDATE BILL_T SET BILL_STATUS = p_bill_status 
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������ �����, ����������
-- - ������ �����
-- - ��� ������ ���������� ���������� 
FUNCTION Get_status (
               p_bill_id       IN INTEGER,
               p_rep_period_id IN INTEGER    -- ID ���������� ������� YYYYMM
           ) RETURN VARCHAR2
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Get_status';
    v_bill_status BILL_T.BILL_STATUS%TYPE;
BEGIN
    SELECT BILL_STATUS INTO v_bill_status
      FROM BILL_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    RETURN v_bill_status;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� ���� 
-- ��� ������ ���������� ����������
PROCEDURE Close_bill( p_bill_id IN INTEGER, p_rep_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_bill';
BEGIN
    Set_status ( p_bill_id, p_rep_period_id, PK00_CONST.c_BILL_STATE_CLOSED );
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������������ ��������� �������� �������
FUNCTION Check_region_prefix ( 
             p_bill_id   IN INTEGER,
             p_period_id IN INTEGER 
         ) RETURN VARCHAR2
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Check_region_prefix';
    v_bill_no      BILL_T.BILL_NO%TYPE;
BEGIN
    -- �������� ��� ������� � ����� �����
    SELECT 
      CASE
        WHEN SUBSTR(B.BILL_NO,5,1) = '/' AND CR.REGION_ID != SUBSTR(B.BILL_NO,1,4) THEN
          -- ����������� ������ ������
          LPAD(TO_CHAR(CR.REGION_ID), 4,'0')||'/'||SUBSTR(B.BILL_NO,6)
        WHEN SUBSTR(B.BILL_NO,5,1) = '/' AND CR.REGION_ID IS NULL THEN
          -- ������ ������, � ��� ���� �� ������
          SUBSTR(B.BILL_NO,6)
        WHEN SUBSTR(B.BILL_NO,5,1) != '/' AND CR.REGION_ID IS NOT NULL THEN
          -- �� ������ ������, � ������ ����
          LPAD(TO_CHAR(CR.REGION_ID), 4,'0')||'/'||SUBSTR(B.BILL_NO,6)
        ELSE
          -- ��� � �������
          B.BILL_NO
       END BILL_NO
      INTO v_bill_no
      FROM CONTRACTOR_T CR, BILL_T B
     WHERE B.REP_PERIOD_ID  = p_period_id
       AND B.BILL_ID        = p_bill_id
       AND B.CONTRACTOR_ID  = CR.CONTRACTOR_ID
    ;
    RETURN v_bill_no;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ��������� ����� �������������� ����� � BRM
-- ������ ������ ����������� �� ������� �������: YYMM(� �/�)[A-Z]
FUNCTION Next_rec_bill_no(
               p_account_id     IN INTEGER,
               p_bill_period_id IN INTEGER
           ) RETURN VARCHAR2
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Next_rec_bill_no';
    v_region_id    INTEGER;
    v_date_from    DATE;
    v_date_to      DATE;
    v_bill_no      BILL_T.BILL_NO%TYPE := NULL;
    v_account_no   ACCOUNT_T.ACCOUNT_NO%TYPE;
BEGIN
    v_date_from := Pk04_Period.Period_from(p_bill_period_id);
    v_date_to   := Pk04_Period.Period_to(p_bill_period_id);
    -- �������� ��������������� ����������
    SELECT A.ACCOUNT_NO, CR.REGION_ID
      INTO v_account_no, v_region_id
      FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACTOR_T CR
     WHERE A.ACCOUNT_ID = p_account_id
       AND A.ACCOUNT_ID = AP.ACCOUNT_ID
       AND AP.DATE_FROM <= v_date_to
       AND (AP.DATE_TO IS NULL OR v_date_from <= AP.DATE_TO 
            OR ap.actual = 'Y' -- ��������� �� ������������ � �������� �.�. 13.05.2016 
           )
       AND AP.CONTRACTOR_ID = CR.CONTRACTOR_ID
       AND ROWNUM = 1;      -- ���������� �� ���������, ���� �� ���� �� ������

    -- ��������� ����� �����
    v_bill_no := SUBSTR(TO_CHAR(p_bill_period_id),3,4)||v_account_no;
    -- � ��������� �� ���������� ���������, ��������� ����� �������
    IF v_region_id IS NOT NULL THEN
        v_bill_no := v_region_id||'/'||v_bill_no;
    END IF;
    
    RETURN v_bill_no;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR(account_id=' ||p_account_id||
                                        ', period_id='  ||p_bill_period_id||')'
                                    , c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ��������� ����� ����� � BRM
-- ������ ������ ����������� �� ������� �������: YYMM(� �/�)[A-Z]
FUNCTION Next_bill_no(
               p_account_id     IN INTEGER,
               p_bill_period_id IN INTEGER
           ) RETURN VARCHAR2
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Next_bill_no';
    v_billing_id   INTEGER;
    v_region_id    INTEGER;
    v_date_from    DATE;
    v_date_to      DATE;
    v_bill_no      BILL_T.BILL_NO%TYPE := NULL;
    v_account_no   ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_count        INTEGER;
    v_next         INTEGER;
BEGIN
    v_date_from := Pk04_Period.Period_from(p_bill_period_id);
    v_date_to   := Pk04_Period.Period_to(p_bill_period_id);
    -- �������� ��������������� ����������
    SELECT A.ACCOUNT_NO, A.BILLING_ID, CR.REGION_ID
      INTO v_account_no, v_billing_id, v_region_id
      FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACTOR_T CR
     WHERE A.ACCOUNT_ID = p_account_id
       AND A.ACCOUNT_ID = AP.ACCOUNT_ID
       AND AP.DATE_FROM <= v_date_to
       AND (AP.DATE_TO IS NULL OR v_date_from <= AP.DATE_TO)
       AND AP.CONTRACTOR_ID = CR.CONTRACTOR_ID
       AND ROWNUM = 1;      -- ���������� �� ���������, ���� �� ���� �� ������

    -- ��������� ����� �����
    v_bill_no := SUBSTR(TO_CHAR(p_bill_period_id),3,4)||v_account_no;
    -- ��������� �� ������������
    v_next := 0;    
    LOOP
        -- ��������� ���������� �� �����
        SELECT COUNT(*) INTO v_count
          FROM BILL_T B
         WHERE B.BILL_NO = v_bill_no;  
        EXIT WHEN v_count = 0;  -- ��� ���������, ������� �� �����
        --
        -- ��������� ��������� �� ������� ����    
        -- � BRM ������� ������ ����� ��������� ������: YYMM(� �/�)[C,D,E-Z]
        -- �, D - ��������������� ��� ������/����� ���
        v_bill_no := SUBSTR(TO_CHAR(p_bill_period_id),3,4)
                         ||v_account_no||CHR(ASCII('D')+v_next);
        --
        v_next := v_next + 1;
    END LOOP;
    
    -- � ��������� �� ���������� ���������, ��������� ����� �������
    IF v_region_id IS NOT NULL THEN
        v_bill_no := v_region_id||'/'||v_bill_no;
    END IF;
    
    RETURN v_bill_no;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR(account_id=' ||p_account_id||
                                        ', period_id='  ||p_bill_period_id||')'
                                    , c_PkgName||'.'||v_prcName );
END;

/*
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ������ �� 31.12.2014
-- �������� ��������� ����� �������������� �����
-- ������� ���������, ��� � �������� "���������" � "������ ��������" 
-- ������ ������ ����������� �� ������ ��������:
-- "���������" - CONTRACT_NO_XXXX, ��� XXXX - ���������� ����� �����
--               ������� ���������, ��� �� ����� ��������, ����� ����
--               ��������� ������� ������
-- "������ ��������" - YYMM(� �/�)[A-Z]
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Next_bill_no(
               p_account_id     IN INTEGER,
               p_contract_id    IN INTEGER,
               p_bill_period_id IN INTEGER
           ) RETURN VARCHAR2
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Next_bill_no';
    v_billing_id   INTEGER;
    v_sq_bill_no   INTEGER;
    v_region_id    INTEGER;
    v_date_from    DATE;
    v_date_to      DATE;
    v_bill_no      BILL_T.BILL_NO%TYPE := NULL;
    v_account_no   ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_contract_no  CONTRACT_T.CONTRACT_NO%TYPE;
    v_count        INTEGER;
BEGIN
  
    v_date_from := Pk04_Period.Period_from(p_bill_period_id);
    v_date_to   := Pk04_Period.Period_to(p_bill_period_id);
    -- �������� ��������������� ����������
    SELECT A.ACCOUNT_NO, A.BILLING_ID, LPAD(TO_CHAR(CR.REGION_ID), 4,'0') 
      INTO v_account_no, v_billing_id, v_region_id
      FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACTOR_T CR
     WHERE A.ACCOUNT_ID = p_account_id
       AND A.ACCOUNT_ID = AP.ACCOUNT_ID
       AND AP.DATE_FROM <= v_date_to
       AND (AP.DATE_TO IS NULL OR v_date_from <= AP.DATE_TO)
       AND AP.CONTRACTOR_ID = CR.CONTRACTOR_ID
       AND ROWNUM = 1;      -- ���������� �� ���������, ���� �� ���� �� ������

    LOOP
        -- � ����������� �� ���� ��������, ������� ������������ ������ ����� ������
        IF v_billing_id = Pk00_Const.c_BILLING_MMTS THEN 
            SELECT C.CONTRACT_NO, BS.BILL_SQ --NVL(BS.BILL_SQ,0)+1
              INTO v_contract_no, v_sq_bill_no
              FROM CONTRACT_T C, CONTRACT_BILL_SQ_T BS 
             WHERE C.CONTRACT_ID = p_contract_id
               AND C.CONTRACT_NO = BS.CONTRACT_NO(+);
            --    
            IF v_sq_bill_no IS NULL THEN
            
               -- ����� ������ ���� � �������
                v_sq_bill_no := 1;
               -- ������ ����� ������ � ������� ������ ��� �������     
                INSERT INTO CONTRACT_BILL_SQ_T(CONTRACT_NO, BILL_SQ, MODIFY_DATE)
                VALUES(v_contract_no, v_sq_bill_no, SYSDATE);            

            ELSE            
            
               v_sq_bill_no := v_sq_bill_no + 1;
              -- ��������� ������� ������ �������
               UPDATE CONTRACT_BILL_SQ_T 
                  SET BILL_SQ = v_sq_bill_no,
                      modify_date = SYSDATE
                WHERE CONTRACT_NO = v_contract_no;            
            
            END IF;

           -- ��������� ���������� ����� �����            
            v_bill_no := v_contract_no||'-'||LPAD(TO_CHAR(v_sq_bill_no), 4,'0');  
            --

        ELSE -- ��� ��������� ������������ ��������� ������� YYMM(� �/�)[A-Z]
             v_bill_no := SUBSTR(TO_CHAR(p_bill_period_id),3,4)||v_account_no;
        END IF;    
    
        -- �.�. ���� �� ����� ���-�� ��� ������ ������ � bill_t, �� ���������, ��� �� ��������������� ������
        -- �������� �������, �� ��� ���� ���...
        SELECT COUNT(*) INTO v_count
          FROM BILL_T B
         WHERE B.BILL_NO = v_bill_no;
        EXIT WHEN v_count = 0;  -- ��� ���������, ������� �� �����
        
    END LOOP;
    
    -- � ��������� �� ���������� ���������, ��������� ����� �������
    IF v_region_id IS NOT NULL THEN
        v_bill_no := v_region_id||'/'||v_bill_no;
    END IF;
    
    RETURN v_bill_no;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR(account_id=' ||p_account_id||
                                        ', contract_id='||p_contract_id||
                                        ', period_id='  ||p_bill_period_id||')'
                                    , c_PkgName||'.'||v_prcName );
END;
*/

-- ��������� ��������� �������������� ������ � ������� CONTRACT_BILL_SQ_T
PROCEDURE Fill_contract_bill_sq_t
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Fill_contract_bill_sq_t';
    v_count          INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    MERGE INTO CONTRACT_BILL_SQ_T CB
    USING(
        WITH BN AS (
        SELECT C.CONTRACT_NO, A.BILLING_ID, B.BILL_NO,
               MAX(B.REP_PERIOD_ID) OVER (PARTITION BY C.CONTRACT_NO) MAX_PERIOD_ID,
               B.REP_PERIOD_ID, SUBSTR(BILL_NO, INSTR(BILL_NO, '-', -1)+1, 4) BILL_SQ     
          FROM BILL_T B, ACCOUNT_PROFILE_T AP, CONTRACT_T C, ACCOUNT_T A 
         WHERE B.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND A.ACCOUNT_ID   = AP.ACCOUNT_ID
           AND A.BILLING_ID  = Pk00_Const.c_BILLING_MMTS -- 2003
        )
        SELECT BN.CONTRACT_NO, MAX(BN.BILL_SQ) BILL_SQ
          FROM BN
         WHERE BN.REP_PERIOD_ID = BN.MAX_PERIOD_ID
           AND LTRIM(BN.BILL_SQ,'0123456789') IS NULL 
         GROUP BY BN.CONTRACT_NO
    ) SQ
    ON ( CB.CONTRACT_NO = SQ.CONTRACT_NO )
    WHEN MATCHED THEN UPDATE SET CB.BILL_SQ = SQ.BILL_SQ
    WHEN NOT MATCHED THEN INSERT (CB.CONTRACT_NO, CB.BILL_SQ) VALUES (SQ.CONTRACT_NO, SQ.BILL_SQ);
    --
    v_count := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg('Merged into CONTRACT_BILL_SQ_T '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


FUNCTION Move_BDR(p_Rep_Id_Old  number,
                  p_Rep_Id_New  number,
                  p_Bill_Id_Old number,
                  p_Bill_Id_New number,
                  p_Item_Id_Old number,
                  p_Item_Id_New number,
                  p_external_Id number
                 ) RETURN number
IS
    l_BDR_Table varchar2(30);
    l_Oper      number;
    l_Acc_Type  varchar2(1);
BEGIN
    SELECT bdr_table, oper
      INTO l_BDR_Table, l_Oper
      FROM bdr_types_t 
     WHERE bdr_type_id = p_external_id; 

   -- ������� BDR-��
    EXECUTE IMMEDIATE
        'UPDATE ' || l_BDR_Table || ' b ' || CHR(10) ||
        '   SET b.rep_period = (CASE WHEN :l_Rep_Date_New = TRUNC(b.local_time,''mm'') THEN b.local_time ' || CHR(10) ||
        '                            ELSE :l_Rep_Date_New ' || CHR(10) ||
        '                       END), ' || CHR(10) ||     
        '       b.bill_id = :p_Bill_Id_New, ' || CHR(10) ||
        '       b.item_id = :p_Item_Id_New ' || CHR(10) ||
        ' WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
        '   AND b.bill_id = :p_Bill_Id_Old ' || CHR(10) ||
        '   AND b.item_id = :p_Item_Id_Old '
        USING TO_DATE(TO_CHAR(p_Rep_Id_New),'YYYYMM'), TO_DATE(TO_CHAR(p_Rep_Id_New),'YYYYMM'),
              p_Bill_Id_New, p_Item_Id_New,
              TO_DATE(TO_CHAR(p_Rep_Id_Old),'YYYYMM'), LAST_DAY(TO_DATE(TO_CHAR(p_Rep_Id_Old),'YYYYMM'))+INTERVAL '0 23:59:59' DAY TO SECOND,
              p_Bill_Id_Old, p_Item_Id_Old;

    -- ������� ������� �����������
    IF l_Oper = 0 THEN
        -- �������� ��� �/����� (���. ���� ��� ����.)
        SELECT a.account_type 
          INTO l_Acc_Type
          FROM bill_t b,
               account_t a
         WHERE b.rep_period_id = p_Rep_Id_Old
           AND b.account_id = a.account_id;
           
        IF l_Acc_Type = 'J' THEN
        
            UPDATE DETAIL_MMTS_T_JUR
               SET rep_period_id = p_Rep_Id_New,
                   bill_id = p_Bill_Id_New,
                   item_id = p_Item_Id_New
             WHERE rep_period_id = p_Rep_Id_Old         
               AND bill_id = p_Bill_Id_Old
               AND item_id = p_Item_Id_Old;
        
        ELSIF l_Acc_Type = 'P' THEN

            UPDATE DETAIL_MMTS_T_FIZ
               SET rep_period_id = p_Rep_Id_New,
                   item_id = p_Item_Id_New
             WHERE rep_period_id = p_Rep_Id_Old         
               AND item_id = p_Item_Id_Old;        
        
        END IF;   
               
    ELSE -- ������������ �����������
    
        UPDATE DETAIL_OPER_T_JUR d
           SET d.rep_period_id = p_Rep_Id_New,
               d.item_id = p_Item_Id_New
         WHERE d.rep_period_id = p_Rep_Id_Old
           AND d.item_id = p_Item_Id_Old;
             
    END IF;

    RETURN SQL%ROWCOUNT;

EXCEPTION
    WHEN no_data_found THEN
        RETURN 0; -- ��� ����� BDR-��
END Move_BDR;     

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ��������� ������ ��� ������ �/�
--   - ��� ������ ���������� ����������
PROCEDURE New_billinfo (
               p_account_id       IN INTEGER,   -- ID �������� �����
               p_currency_id      IN INTEGER,   -- ID ������ �����
               p_delivery_id      IN INTEGER,   -- ID ������� �������� �����
               p_days_for_payment IN INTEGER DEFAULT 30   -- ���-�� ���� �� ������ �����
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'New_billinfo';
    v_period_length  INTEGER := 1;
    v_account_no     ACCOUNT_T.ACCOUNT_NO%TYPE := NULL;
    v_count          INTEGER;
    v_utc_date       DATE := SYSDATE;
    v_local_date     DATE := SYSDATE+GET_TZ_OFFSET;
BEGIN
    -- ������� �������������� ������ � ������ ��� ����� ���������� �/�
    INSERT INTO BILLINFO_T ( 
        ACCOUNT_ID, PERIOD_LENGTH,  DAYS_FOR_PAYMENT
    )
    WITH AC AS (
        SELECT AP.ACCOUNT_ID,
               AP.DATE_FROM, 
               AP.DATE_TO,
               ROW_NUMBER() OVER (PARTITION BY AP.ACCOUNT_ID ORDER BY AP.DATE_FROM) RN,
               CASE
               WHEN AP.DATE_FROM <= v_local_date AND (AP.DATE_TO IS NULL OR AP.DATE_TO < v_local_date) THEN 1
               WHEN v_utc_date <= AP.DATE_FROM AND AP.DATE_TO IS NULL THEN 2 -- ������ ������� ������
               ELSE 0
               END ITV
          FROM ACCOUNT_PROFILE_T AP, CONTRACT_T C, CONTRACTOR_T CT
         WHERE AP.CONTRACT_ID = C.CONTRACT_ID
           AND AP.BRANCH_ID   = CT.CONTRACTOR_ID
           AND AP.ACCOUNT_ID  = p_account_id
    )
    SELECT ACCOUNT_ID,  v_period_length, p_days_for_payment
      FROM AC
     WHERE (ITV = 1 OR (ITV = 2 AND RN = 1));

     -- ��������� ������ �������� ��� ��������� ����������
     INSERT INTO 
            ACCOUNT_DOCUMENTS_T (ACCOUNT_ID,DOC_BILL,DELIVERY_METHOD_ID) 
       VALUES (p_account_id, 'Y', p_delivery_id);

    /*
    SELECT AP.ACCOUNT_ID, C.CONTRACT_NO BILL_NAME, 0 SQ_BILL_NO,
           v_period_length, p_currency_id, p_days_for_payment,
           p_delivery_id
      FROM ACCOUNT_PROFILE_T AP, CONTRACT_T C, CONTRACTOR_T CT
     WHERE AP.CONTRACT_ID = C.CONTRACT_ID
       AND AP.ACCOUNT_ID  = p_account_id
       AND ( -- ���� �������� �� ������� ������ �������
           (AP.DATE_FROM <= (SYSDATE+1/6) AND (AP.DATE_TO IS NULL OR AP.DATE_TO < (SYSDATE+1/6))) 
           OR -- ���� ������� ������ ������� ������
           (SYSDATE <= AP.DATE_FROM AND AP.DATE_TO IS NULL)
       )
       AND AP.BRANCH_ID = CT.CONTRACTOR_ID;
    */
    -- 
    v_count := SQL%ROWCOUNT;
    IF v_count = 0 THEN
        -- �������� ������� ������ ������� ������
        INSERT INTO BILLINFO_T ( 
            ACCOUNT_ID, PERIOD_LENGTH, DAYS_FOR_PAYMENT
        )
        SELECT AP.ACCOUNT_ID, v_period_length, p_days_for_payment
          FROM ACCOUNT_PROFILE_T AP, CONTRACT_T C, CONTRACTOR_T CT
         WHERE AP.CONTRACT_ID = C.CONTRACT_ID
           AND AP.ACCOUNT_ID  = p_account_id
           AND ( 
               (AP.DATE_FROM <= v_local_date AND (AP.DATE_TO IS NULL OR AP.DATE_TO < v_local_date)) 
               OR
               (v_utc_date <= AP.DATE_FROM AND AP.DATE_TO IS NULL)
           )
           AND AP.BRANCH_ID = CT.CONTRACTOR_ID;
    
        SELECT ACCOUNT_NO INTO v_account_no 
          FROM ACCOUNT_T 
         WHERE ACCOUNT_ID = p_account_id;
        --         
        Pk01_Syslog.Raise_user_exception('account_id='||p_account_id||
               ', account_no='||v_account_no||'- ������ �� �������', 
               c_PkgName||'.'||v_prcName);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

/*
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������������ ����: �������� ����� ���������� � ������������� ����� �� ��������
-- � ���������� �������, ��� ���� ����������� , 
-- �.�. ���������� �� ��� ������� �� ��������, 
-- ����������� ������ ������
-- ����������:
-- - ���������� ����� ���������� � ������������� �� ����� (�� ��� ������ ���� ��������)
-- - ��� ������ ���������� ����������
FUNCTION Generate_bill (
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER    -- ID ������� �����
           ) RETURN NUMBER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Generate_bill';
    v_bill_total NUMBER;
    --
BEGIN
    -- ��������� ��� ������� �����-������� (��� �� �� ��������� �� �������): 
    UPDATE BILL_T B
       SET (TOTAL, GROSS, TAX, DUE, BILL_STATUS, CALC_DATE) = (
          SELECT SUM(II.TOTAL) TOTAL, SUM(II.GROSS) GROSS, SUM(II.TAX) TAX,
                 -(SUM(II.TOTAL)+SUM(II.GROSS)+SUM(II.TAX)) DUE,
                 PK00_CONST.c_BILL_STATE_READY,  SYSDATE
            FROM INVOICE_ITEM_T II
           WHERE II.BILL_ID = B.BILL_ID
             AND II.REP_PERIOD_ID = B.REP_PERIOD_ID
     )
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
    RETURNING TOTAL INTO v_bill_total;
     -- ���������� ����� ���������� � ������������� �� ����� (�� ��� ������ ���� ��������)
    RETURN v_bill_total;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������������� ���� (������ ��� ������� READY): 
-- �������� ����� ���������� � ������������� ����� �� ��������
-- ������� ������ ����� �������, ���� ���� ������������
-- � ������� ������� OPEN,
-- �.�. ������� ���������� ���������� �� ��� ������� 
-- ����������:
--   - ������������� - ID �����, 
--   - ��� ������ ���������� ����������
FUNCTION Rollback_bill (
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER    -- ID ������� �����
           ) RETURN NUMBER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Rollback_bill';
    v_bill_status BILL_T.BILL_STATUS%TYPE;
BEGIN
    -- ��������� ������ �����
    v_bill_status := Get_status(p_bill_id, p_rep_period_id);
    IF v_bill_status != PK00_CONST.c_BILL_STATE_READY THEN
        RAISE_APPLICATION_ERROR(-20000, '�������� ������ ����� (bill_id='||p_bill_id||'): '||v_bill_status);
    END IF;
    -- ������� ������ �� ������� ����� ������� �� ITEM
    UPDATE ITEM_T
       SET INV_ITEM_ID = NULL
     WHERE BILL_ID     = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- ������� ������� ����� �������
    DELETE FROM INVOICE_ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- �������� ����� ����� � ���������� ������ - ������
    UPDATE BILL_T
       SET TOTAL         = 0,
           GROSS         = 0,
           TAX           = 0,
           DUE           = 0, 
           ADJUSTED      = 0,
           BILL_STATUS   = PK00_CONST.c_BILL_STATE_OPEN
     WHERE BILL_ID       = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
     -- ���������� ����� ���������� � ������������� �� ����� (�� ��� ������ ���� ��������)
     RETURN p_bill_id;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--
-- ����� �����. ��������������� �������. ����� ���� �����
--
FUNCTION Rollback_bill_force (
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER    -- ID ������� �����
           ) RETURN NUMBER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Rollback_bill';
BEGIN
    -- ������� ������ �� ������� ����� ������� �� ITEM
    UPDATE ITEM_T
       SET INV_ITEM_ID = NULL
     WHERE BILL_ID     = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- ������� ������� ����� �������
    DELETE FROM INVOICE_ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- �������� ����� ����� � ���������� ������ - ������
    UPDATE BILL_T
       SET TOTAL         = 0,
           GROSS         = 0,
           TAX           = 0,
           DUE           = 0, 
           ADJUSTED      = 0,
           BILL_STATUS   = PK00_CONST.c_BILL_STATE_OPEN
     WHERE BILL_ID       = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
     -- ���������� ����� ���������� � ������������� �� ����� (�� ��� ������ ���� ��������)
     RETURN p_bill_id;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
*/

-- ===============================================================================
-- ������� ����� � ������ ������ ������ �� ������� ���, 
-- �.�. �������� ���� �� ������ ��� ������� � ����������
-- ���������� 
--  bill_id - ������������� �����
-- -1 - ������, ���� ����������� ��������� �������
-- -2 - ������, �� ���� ���� ������, �������� �����-���� (BILL_T.NEXT_BILL_ID is not null)
-- ���� p_force = 1, �� ������� �� ��������� ������� ��������
-- =============================================================================== 
FUNCTION Move_bill (
              p_bill_id      IN INTEGER,
              p_period_id    IN INTEGER,
              p_period_id_to IN INTEGER,
              p_bill_date_to IN DATE,
              p_force        IN INTEGER DEFAULT 1
          ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Move_bill';
    v_count          INTEGER;
    v_bill_no        BILL_T.BILL_NO%TYPE;
    v_bill_id        INTEGER;
    v_item_id        INTEGER;
    v_inv_id_old     INTEGER := NULL;
    v_inv_id_new     INTEGER := NULL;
    v_bill_count     INTEGER := 0;
    v_item_count     INTEGER := 0;
    v_inv_count      INTEGER := 0;
    v_bdr_count      integer := 0;
    v_close_rep_period DATE;
    v_next_bill_id   INTEGER; 
    v_next_period_id INTEGER;
    v_prev_bill_id   INTEGER; 
    v_prev_period_id INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('START.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������ ��������� ����� � ���������, ��� �������� ���������
    SELECT B.PREV_BILL_ID, B.PREV_BILL_PERIOD_ID,
           B.NEXT_BILL_ID, B.NEXT_BILL_PERIOD_ID,
           B.BILL_NO,
           P.CLOSE_REP_PERIOD
      INTO v_prev_bill_id, v_prev_period_id,
           v_next_bill_id, v_next_period_id,
           v_bill_no,
           v_close_rep_period
      FROM BILL_T B, PERIOD_T P
     WHERE B.REP_PERIOD_ID = P.PERIOD_ID
       AND B.BILL_ID = p_bill_id
       AND B.REP_PERIOD_ID = p_period_id;
    
    IF v_close_rep_period IS NOT NULL THEN
        -- ��������� ������� �������� ���� ��������� �������
        Pk01_Syslog.Write_msg('bill_id='||p_bill_id||', period='||p_period_id||' - closed'
                             , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_warn );
        IF p_force = 0 THEN
            RETURN -1;
        END IF;  
    END IF;

    -- ���������, ��� ���� ��������� � ������� - �� ����� !!!
    --IF v_next_bill_id IS NOT NULL THEN
    --    Pk01_Syslog.Write_msg('bill_id='||p_bill_id||', period='||p_period_id||' - next_bill_id IS NOT NULL'
    --                         , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );
    --    RETURN -2;
    --END IF;

    -- ������� ����� ����� � ��������� �������
    v_bill_id := Pk02_Poid.Next_bill_id;
    
    INSERT INTO BILL_T (    
        BILL_ID, REP_PERIOD_ID, ACCOUNT_ID,
        BILL_NO, BILL_DATE, BILL_TYPE, BILL_STATUS, CURRENCY_ID,
        TOTAL, GROSS, TAX, RECVD, DUE, DUE_DATE, PAID_TO,
        PREV_BILL_ID, PREV_BILL_PERIOD_ID, NEXT_BILL_ID, NEXT_BILL_PERIOD_ID,
        CALC_DATE, ACT_DATE_FROM, ACT_DATE_TO, NOTES, DELIVERY_DATE,
        ADJUSTED, CONTRACT_ID, VAT, CREATE_DATE, PROFILE_ID, 
        CONTRACTOR_ID, CONTRACTOR_BANK_ID
    )
    SELECT 
        v_bill_id, p_period_id_to, ACCOUNT_ID,
        BILL_NO||'.NEW',  -- �������� ������, ���� �����
        p_bill_date_to, BILL_TYPE, BILL_STATUS, CURRENCY_ID,
        TOTAL, GROSS, TAX, RECVD, DUE, DUE_DATE, PAID_TO,
        PREV_BILL_ID, PREV_BILL_PERIOD_ID, NEXT_BILL_ID, NEXT_BILL_PERIOD_ID,
        CALC_DATE, ACT_DATE_FROM, ACT_DATE_TO, NOTES, DELIVERY_DATE,
        ADJUSTED, CONTRACT_ID, VAT, CREATE_DATE, PROFILE_ID, 
        CONTRACTOR_ID, CONTRACTOR_BANK_ID
      FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_period_id
       AND B.BILL_ID       = p_bill_id;
       
    v_bill_count := SQL%ROWCOUNT;
    
    Pk01_Syslog.Write_msg('BILL_T: '||v_bill_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('BILL_T.BILL_ID = '||v_bill_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- ����������� ������ �� ����������� ���� � �������������� ������
    IF v_prev_bill_id IS NOT NULL THEN
        UPDATE BILL_T B 
           SET B.NEXT_BILL_ID = v_bill_id, 
               B.NEXT_BILL_PERIOD_ID = p_period_id_to
         WHERE B.REP_PERIOD_ID = v_prev_period_id
           AND B.BILL_ID = v_prev_bill_id
        ; 
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('BILL_T.NEXT_BILL_ID: '||v_count||' SET', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    END IF;
    -- ����������� ������ �� ����������� ���� � ����������� ������
    IF v_next_bill_id IS NOT NULL THEN
        UPDATE BILL_T B 
           SET B.PREV_BILL_ID = v_bill_id, B.PREV_BILL_PERIOD_ID = p_period_id_to
         WHERE B.REP_PERIOD_ID = v_next_period_id
           AND B.BILL_ID = v_next_bill_id
        ; 
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('BILL_T.PREV_BILL_ID: '||v_count||' SET', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    END IF;
    --
    FOR ri IN (
        SELECT ROW_NUMBER() OVER (PARTITION BY BILL_ID, INV_ITEM_ID ORDER BY ITEM_ID) RN,
               BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
               ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
               ITEM_TOTAL, RECVD, DATE_FROM, DATE_TO, 
               ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
               REP_GROSS, REP_TAX, TAX_INCL, 
               EXTERNAL_ID, NOTES, ORDER_BODY_ID, DESCR, QUANTITY, 
               BILL_TOTAL, ITEM_CURRENCY_ID, ITEM_CURRENCY_RATE
          FROM ITEM_T I
         WHERE I.REP_PERIOD_ID = p_period_id
           AND I.BILL_ID       = p_bill_id
         ORDER BY I.INV_ITEM_ID, I.ITEM_ID
    )LOOP
        IF ri.RN = 1 THEN
            -- �������� ������ � INVOICE_ITEM_T:
            v_inv_id_old := ri.INV_ITEM_ID;
            IF v_inv_id_old IS NOT NULL THEN
                v_inv_id_new := Pk02_Poid.Next_invoice_item_id;
                INSERT INTO INVOICE_ITEM_T(BILL_ID, REP_PERIOD_ID, INV_ITEM_ID, INV_ITEM_NO, 
                       SERVICE_ID, TOTAL, GROSS, TAX, VAT, INV_ITEM_NAME, 
                       DATE_FROM, DATE_TO)
                SELECT v_bill_id BILL_ID, p_period_id_to REP_PERIOD_ID, 
                       v_inv_id_new INV_ITEM_ID, V.INV_ITEM_NO, 
                       V.SERVICE_ID, V.TOTAL, V.GROSS, V.TAX, V.VAT, V.INV_ITEM_NAME, 
                       V.DATE_FROM, V.DATE_TO
                  FROM INVOICE_ITEM_T V
                 WHERE V.REP_PERIOD_ID = ri.Rep_Period_Id
                   AND V.BILL_ID       = ri.Bill_Id
                   AND V.INV_ITEM_ID   = ri.Inv_Item_Id;
                v_inv_count := v_inv_count + SQL%ROWCOUNT;

            ELSE
                v_inv_id_new := NULL;
            END IF;
        END IF;   
        -- �������� ������ � ITEM_T:
        v_item_id := Pk02_Poid.Next_item_id;
        --
        INSERT INTO ITEM_T (
            BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
            ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, ITEM_TOTAL, RECVD, 
            DATE_FROM, DATE_TO, ITEM_STATUS, CREATE_DATE, 
            LAST_MODIFIED, REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, 
            ORDER_BODY_ID, DESCR, QUANTITY, ITEM_CURRENCY_ID,
            BILL_TOTAL, ITEM_CURRENCY_RATE)
        SELECT 
            v_bill_id BILL_ID, p_period_id_to REP_PERIOD_ID, 
            v_item_id ITEM_ID, ITEM_TYPE, v_inv_id_new INV_ITEM_ID, 
            ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, ITEM_TOTAL, RECVD, 
            DATE_FROM, DATE_TO, ITEM_STATUS, CREATE_DATE, 
            SYSDATE LAST_MODIFIED, REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, 
            ORDER_BODY_ID, DESCR, QUANTITY, ITEM_CURRENCY_ID,
            BILL_TOTAL, ITEM_CURRENCY_RATE
          FROM ITEM_T I    
         WHERE I.REP_PERIOD_ID = ri.Rep_Period_Id
           AND I.BILL_ID       = ri.Bill_Id
           AND I.ITEM_ID       = ri.Item_Id;
       v_item_count := v_item_count + SQL%ROWCOUNT;
       
        IF ri.external_id > 0 THEN
            -- ������� BDR-��
            v_count := Move_BDR(p_Rep_Id_Old  => ri.Rep_Period_Id,
                                p_Rep_Id_New  => p_period_id_to,
                                p_Bill_Id_Old => ri.Bill_Id,
                                p_Bill_Id_New => v_bill_id,
                                p_Item_Id_Old => ri.Item_Id,
                                p_Item_Id_New => v_item_id,
                                p_external_Id => ri.external_id
                               );
            v_bdr_count := v_bdr_count + v_count;                       
        END IF;                                 
       
    END LOOP;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_inv_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    Pk01_Syslog.Write_msg('ITEM_T: '||v_item_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    Pk01_Syslog.Write_msg('BDR-��: '||v_bdr_count||' moved', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������� ����������
    DELETE FROM ITEM_T I 
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.BILL_ID = p_bill_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ������� ����� 
    DELETE FROM INVOICE_ITEM_T V
     WHERE V.REP_PERIOD_ID = p_period_id
       AND V.BILL_ID = p_bill_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_count||' deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ������� �������� ����
    DELETE FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_period_id
       AND B.BILL_ID = p_bill_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ���������� ����� �����
    UPDATE BILL_T B SET B.BILL_NO = v_bill_no
     WHERE B.BILL_ID = v_bill_id
       AND B.REP_PERIOD_ID = p_period_id_to;

    Pk01_Syslog.Write_msg('STOP', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    RETURN v_bill_id;
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������� ������������� �� �����
-- ����������:
-- - ���������� ����� ������������� �� �����
-- - ��� ������ ���������� ����������
FUNCTION Calculate_due(
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER    -- ID ������� �����
           ) RETURN NUMBER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Calculate_due';
    v_bill_type    INTEGER; 
    v_next_bill_id INTEGER;
    v_total        NUMBER;
    v_recvd        NUMBER;
    v_due_date     DATE;
    v_due          NUMBER;
BEGIN
    -- �������� ���������� � �����
    SELECT B.BILL_TYPE, B.NEXT_BILL_ID, B.TOTAL
      INTO v_bill_type, v_next_bill_id, v_total 
      FROM BILL_T B  
     WHERE B.BILL_ID = p_bill_id
       AND B.REP_PERIOD_ID = p_rep_period_id;
       
    -- ����������� ���-�� ����� ����������� �� ����
    SELECT SUM(PT.TRANSFER_TOTAL) RECVD, 
           MAX(PT.TRANSFER_DATE) DUE_DATE
      INTO v_recvd, v_due_date
      FROM PAY_TRANSFER_T PT
     WHERE PT.BILL_ID = p_bill_id
       AND PT.REP_PERIOD_ID = p_rep_period_id; 

    IF v_bill_type = 'B' AND v_next_bill_id IS NULL THEN
        v_due := v_recvd - v_total;
        -- ���������� �����, ��� ��� ��� �������������
        UPDATE BILL_T B SET B.ADJUSTED = 0, 
                            B.RECVD    = v_recvd,
                            B.DUE      = v_due,
                            B.DUE_DATE = v_due_date
         WHERE B.BILL_TYPE = 'B'
           AND B.ADJUSTED != 0
           AND B.NEXT_BILL_ID IS NULL
         RETURNING B.DUE INTO v_due;
    ELSIF v_bill_type = 'C' THEN
        v_due := 0;
        -- �� ������ ����� ����� ������ �� ������
        UPDATE BILL_T B SET B.DUE      = v_due, 
                            B.ADJUSTED = B.TOTAL - v_recvd,
                            B.RECVD    = v_recvd  -- ������ �� ������, ���������� �������� (�� �������!!!)
         WHERE B.BILL_TYPE = 'C';
    ELSIF v_next_bill_id IS NOT NULL THEN
        v_due := v_recvd - v_total;  -- ������������� ������� ����� ��� ������-���
        -- �� ����������������� ������ ���� ������������� ��� (�� ����� ����� �������� ��������)
        UPDATE BILL_T B SET B.DUE = 0, B.ADJUSTED = -B.TOTAL
         WHERE B.NEXT_BILL_ID IS NOT NULL
           AND B.RECVD = 0
           AND B.BILL_TYPE != 'C';
         
         
    ELSIF v_next_bill_id IS NOT NULL AND v_recvd = 0 THEN
        v_due := 0;
        -- �� ����������������� ������ ���� ������������� ��� (�� ����� ����� �������� ��������)
        UPDATE BILL_T B SET B.DUE = 0, B.ADJUSTED = -B.TOTAL
         WHERE B.NEXT_BILL_ID IS NOT NULL
           AND B.RECVD = 0
           AND B.BILL_TYPE != 'C';
    ELSIF v_next_bill_id IS NOT NULL AND v_recvd != 0 THEN
        v_due := NULL;
        -- ��������� ������ ���� �� ����� �������� �������� �� ����������������� ���� 
        Pk01_Syslog.Raise_user_exception('Bill_id = '||p_bill_id||' ��������������, �� � ���� �� ����� ������'
                                         , c_PkgName||'.'||v_prcName);
    END IF;
    -- ���������� ����� ���������� � ������������� �� ����� (�� ��� ������ ���� ��������)
    RETURN v_due;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ��� ������� ���������� �����
--   - ������������� - ���-�� ��������� �������
--   - ��� ������ ���������� ����������
FUNCTION Items_list( 
               p_recordset    OUT t_refc, 
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER    -- ID ������� �����
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Items_list';
    v_retcode    INTEGER;
BEGIN
    -- ��������� ���-�� �������
    SELECT COUNT(*) INTO v_retcode
      FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- ���������� ������
    OPEN p_recordset FOR
         SELECT ITEM_ID, ITEM_TYPE, BILL_ID, 
                ORDER_ID, SERVICE_ID, CHARGE_TYPE,  
                ITEM_TOTAL, RECVD,  
                DATE_FROM, DATE_TO, INV_ITEM_ID, ITEM_STATUS
           FROM ITEM_T
          WHERE BILL_ID = p_bill_id
            AND REP_PERIOD_ID = p_rep_period_id
          ORDER BY ITEM_ID;
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� ��� ������� ���������� ����� (������ ����� ������ ����� ����� ���������� ��������)
--   - ������������� - ���-�� ��������� �������
--   - ��� ������ ���������� ����������
FUNCTION Delete_items (
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER    -- ID ������� �����
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_items';
    v_count       INTEGER := 0;
    v_bill_status BILL_T.BILL_STATUS%TYPE;
BEGIN
    -- ��������� ������ �����
    v_bill_status := Get_status(p_bill_id, p_rep_period_id);
    IF v_bill_status != PK00_CONST.c_BILL_STATE_OPEN THEN
        RAISE_APPLICATION_ERROR(-20000, '�������� ������ ����� (bill_id='||p_bill_id||'): '||v_bill_status);
    END IF;  
    -- ��������� ��� �� ���������������� ������� ����� �������
    SELECT COUNT(1) INTO v_count
      FROM INVOICE_ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '������ ��� �������� ������� ����� (item)'
              ||' BILL_ID='|| p_bill_id ||', '
              ||', �������������� ���������� ������� ������� �����-������� (invoice-item)');
    END IF;
    
    -- ������� �� ��������� � ������� (event) � �������� �� �� ������� �����
    -- ... ������ �����

    -- ������� ��� ������� ���������� �����
    DELETE 
      FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- ���������� ���-�� ��������� �������
    RETURN SQL%ROWCOUNT;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� �� ������� ������������� ������ �� ������� ����� � �������� �������
FUNCTION Check_BillRec_AtPeriod_Has ( 
      p_account_id   IN INTEGER,
      p_period_id    IN INTEGER 
) RETURN INTEGER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Check_BillRec_AtPeriod_Has';
    v_bill_count   INTEGER;
BEGIN
    SELECT 
       COUNT(*) INTO v_bill_count
      FROM BILL_T B
     WHERE B.BILL_TYPE IN ('B')
       AND B.REP_PERIOD_ID  = p_period_id
       AND B.ACCOUNT_ID     = p_account_id
    ;
    
    RETURN v_bill_count;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

---------------------------------------------------------------
-- �������� ���������� � �����
---------------------------------------------------------------
FUNCTION GET_BILL_NOTES(p_bill_id IN NUMBER) RETURN VARCHAR2
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GET_BILL_NOTES';
    v_retcode       INTEGER;
    v_notes         VARCHAR2(2000);
BEGIN
    SELECT NOTES INTO v_notes FROM BILL_T WHERE BILL_ID = p_bill_id;
    RETURN v_notes;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN '';  
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK07_BILL;
/
