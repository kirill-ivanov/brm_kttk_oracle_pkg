CREATE OR REPLACE PACKAGE PK07_BILL_ADJUST
IS
    --
    -- ����� ��� ������ � �������� "����", �������:
    -- bill_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK07_BILL_ADJUST';
    -- ==============================================================================
    type t_refc is ref cursor;
   
    -- ������������ ����� ��� ������-����� ����
    FUNCTION Get_billno_for_credit_debit (
             p_src_bill_id       IN INTEGER,   -- ID ������-���� ��� ������� ��������� �����-���� (ID ������-����)
             p_src_period_id     IN INTEGER   -- ID ���������� ������� YYYYMM ������-���� 
    ) RETURN VARCHAR2;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ����� ������-���� (������������ ������������ ����), ����������:
    --   - ������������� - ID �������� �����, 
    --   - ��� ������ ���������� ����������
    -- ����� ������������ ������� ��������� �/� ( �.�.����� �� 15.06.2015 )
    FUNCTION Open_credit_note (
                   p_src_bill_id   IN INTEGER,   -- ID ����� ��� �������� ��������� ������-����
                   p_src_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM ���������
                   p_crd_period_id IN INTEGER,   -- ID ���������� ������� ������-���� YYYYMM
                   p_notes         IN VARCHAR2   -- ����������
               ) RETURN INTEGER;

    -- �������� ����� �����-����, ����������:
    --   - ������������� - ID �������� �����, 
    --   - ��� ������ ���������� ����������
    FUNCTION Open_debit_note (
                   p_crd_bill_id   IN INTEGER,   -- ID ������-���� ��� ������� ��������� �����-���� (ID ������-����)
                   p_crd_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM ������-����  
                   p_dbt_period_id IN INTEGER,   -- ID ���������� ������� �����-���� YYYYMM
                   is_items_create IN INTEGER,   -- ����� �� ��������� ������� � �����-����                   
                   p_notes          IN VARCHAR2  -- ����������
               ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� ����� - ����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Recalc_debit_note (
                   p_dbt_bill_id    IN INTEGER,   -- ID �����-����
                   p_dbt_period_id  IN INTEGER,   -- ID ���������� ������� �����-���� YYYYMM
                   p_crd_period_id  IN INTEGER    -- ID ���������� ������� ������-���� YYYYMM
               );
    
    
END PK07_BILL_ADJUST;
/
CREATE OR REPLACE PACKAGE BODY PK07_BILL_ADJUST
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������������ ����� ��� ������-����� ����
FUNCTION Get_billno_for_credit_debit (
         p_src_bill_id       IN INTEGER,   -- ID ������-���� ��� ������� ��������� �����-���� (ID ������-����)
         p_src_period_id     IN INTEGER    -- ID ���������� ������� YYYYMM ������-���� 
) RETURN VARCHAR2
IS
    v_prcName        CONSTANT VARCHAR2(30):= 'Get_billno_for_credit_debit';
    v_letter_slovar  CONSTANT VARCHAR2(30):= 'CDEFGHIKLMNOPQRSTUVWXYZ';
    v_bill_no        VARCHAR2(100);
    v_bill_no_main   VARCHAR2(100);
    v_prev_bill_id   INTEGER;
    v_res_temp       VARCHAR(1);
    v_letter_prev    VARCHAR2(1);
    v_letter_result_index  INTEGER;
BEGIN    
    -- 1. �������� ����� ����� BILL_NO
    SELECT BILL_NO, PREV_BILL_ID 
      INTO v_bill_no, v_prev_bill_id 
      FROM BILL_T    
     WHERE BILL_ID = p_src_bill_id
       AND REP_PERIOD_ID = p_src_period_id;
    
    -- 2. ����������� ����� �� ������ �����
    -- ���� v_prev_bill_id �� ������ - ������ ��� ������ ���������� �������������� ����
    IF v_prev_bill_id IS NOT NULL THEN
       v_letter_prev := SUBSTR(v_bill_no, LENGTH(v_bill_no), 1);
       v_bill_no_main := SUBSTR(v_bill_no,1,LENGTH(v_bill_no)-1);

       --�������� ������ ���������, �� ����� �� ��� (���� �����, ������ ����� �� ����� �� ���� � ����� ����� ������ ������)
       SELECT NVL2(TRANSLATE(v_letter_prev, 'A1234567890','A'), 'F', 'T') INTO v_res_temp FROM DUAL;
       IF v_res_temp = 'F' THEN
         v_letter_result_index := INSTR(v_letter_slovar,v_letter_prev);
         IF v_letter_result_index = 0 THEN v_letter_result_index := 1; END IF;
       ELSE
          v_letter_result_index := 0;
       END IF;        
    ELSE
       v_letter_result_index := 0; 
       v_bill_no_main := v_bill_no;
    END IF;
    -- ��������� ����� ����� � ���������� ���
    RETURN v_bill_no_main || SUBSTR(v_letter_slovar, v_letter_result_index + 1, 1);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- =============================================================== --
-- ������-����: ������������ ������������ ����
-- =============================================================== --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ����� ������-����, ����������:
--   - ������������� - ID �������� �����, 
--   - ��� ������ ���������� ����������
-- ����� ������������ ������� ��������� �/� ( �.�.����� �� 15.06.2015 )
FUNCTION Open_credit_note (
               p_src_bill_id   IN INTEGER,   -- ID ����� ��� �������� ��������� ������-����
               p_src_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM ���������
               p_crd_period_id IN INTEGER,   -- ID ���������� ������� ������-���� YYYYMM
               p_notes         IN VARCHAR2   -- ����������
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Open_credit_note';
    v_bill_id       INTEGER;                    -- ������ POID: YYMM.XXX.XXX.XXX,
    v_bill_no_new   VARCHAR2(100);
    v_next_bill_id  INTEGER;
    v_bill_status   VARCHAR2(10);
    v_bill_date     DATE;
    v_account_id    INTEGER;
    v_contract_id    INTEGER;
    v_profile_id     INTEGER;
    v_contractor_id  INTEGER;
    v_bank_id        INTEGER;
    v_vat            NUMBER;
    v_balance       NUMBER;
BEGIN
    Pk01_Syslog.Write_msg('src_bill_id='||p_src_bill_id||
                          ', src_period_id='||p_src_period_id||
                          ', dst_period_id='||p_crd_period_id, 
                          c_PkgName||'.'||v_prcName);

    -- ���������, ����� �� ������ ���������. 
    -- ���� ���� ���� ������ �/��� ����� ���� ��� ��������� ������������� - ������
    SELECT NEXT_BILL_ID, BILL_STATUS, ACCOUNT_ID 
      INTO v_next_bill_id, v_bill_status, v_account_id
      FROM BILL_T 
     WHERE BILL_ID = p_src_bill_id
       AND REP_PERIOD_ID = p_src_period_id;
    
    IF v_next_bill_id IS NOT NULL THEN
      Pk01_Syslog.Raise_user_exception(
        'p_src_bill_id='||p_src_bill_id||', p_src_period_id='||p_src_period_id||
        '. ������ �������������� ����, ������� ��� ��� ���������������.',
        c_PkgName||'.'||v_prcName);
    END IF;
    
    IF v_bill_status NOT IN (Pk00_const.c_BILL_STATE_CLOSED, Pk00_const.c_BILL_STATE_READY) THEN
    	Pk01_Syslog.Raise_user_exception(
        'p_src_bill_id='||p_src_bill_id||', p_src_period_id='||p_src_period_id||                                      
        '. ��� ��������� ����� ������ ������� �������������� ����!',
        c_PkgName||'.'||v_prcName);
    END IF;       
    
    -- ��������� ID ������� (POID) ��� ���������� ������������ ������� 
    v_bill_id     := Pk02_POID.Next_bill_id;
    v_bill_date   := Pk04_Period.Period_from(p_crd_period_id);
    v_bill_no_new := Get_billno_for_credit_debit(p_src_bill_id, p_src_period_id);
    
    -- ����� ������������ ������� ��������� �/� ( �.�.����� �� 15.06.2015 )
    -- �������� id �������� � ������ ���, ��� �������� �������
    Pk07_Bill.Read_account_profile (
               p_account_id    => v_account_id,
               p_bill_date     => v_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );
    
    -- C������ ������-����, ��� �������� ���������� ���������� �����
    INSERT INTO BILL_T (
        BILL_ID,             -- new
        REP_PERIOD_ID,       -- new
        ACCOUNT_ID,          -- old
        BILL_NO,             -- new!!!
        BILL_DATE,           -- new
        BILL_TYPE,           -- new
        BILL_STATUS,         -- new
        CURRENCY_ID,         -- old
        TOTAL,               -- new
        GROSS,               -- -old
        TAX,                 -- -old
        ADJUSTED,            -- -old total
        RECVD,               -- 0
        DUE,                 -- 0
        DUE_DATE,            -- SYSDATE
        PAID_TO,             -- NULL - ������� �� �����
        PREV_BILL_ID,        -- BILL_ID
        PREV_BILL_PERIOD_ID, -- REP_PERIOD_ID
        NEXT_BILL_ID,        -- NULL
        NEXT_BILL_PERIOD_ID, -- NULL
        CALC_DATE,           -- new
        NOTES,               -- new
        CONTRACT_ID,
        VAT,
        PROFILE_ID, 
        CONTRACTOR_ID, 
        CONTRACTOR_BANK_ID
    ) 
    SELECT v_bill_id,
           p_crd_period_id,
           ACCOUNT_ID,
           -- ��������� �� ��� �������
           CASE
              WHEN SUBSTR(v_bill_no_new,5,1) = '/' AND CR.REGION_ID != SUBSTR(v_bill_no_new,1,4) THEN
                -- ����������� ������ ������
                LPAD(TO_CHAR(CR.REGION_ID), 4,'0')||'/'||SUBSTR(v_bill_no_new,6)
              WHEN SUBSTR(v_bill_no_new,5,1) = '/' AND CR.REGION_ID IS NULL THEN
                -- ������ ������, � ��� ���� �� ������
                SUBSTR(v_bill_no_new,6)
              WHEN SUBSTR(v_bill_no_new,5,1) != '/' AND CR.REGION_ID IS NOT NULL THEN
                -- �� ������ ������, � ������ ����
                LPAD(TO_CHAR(CR.REGION_ID), 4,'0')||'/'||SUBSTR(v_bill_no_new,6)
              ELSE
                -- ��� � �������
                v_bill_no_new
           END BILL_NO,
           v_bill_date,       -- ���� ��������� � ��������� �������
           PK00_CONST.c_BILL_TYPE_CRD,
           PK00_CONST.c_BILL_STATE_CLOSED, -- ���� ������ (��� �� ��������)
           CURRENCY_ID,       -- ������ �����
           -TOTAL,            -- ���������� ����������
           -GROSS,            -- ���������� ���������� ��� ���
           -TAX,              -- ���������� ������
           -TOTAL,            -- �������������: ������������� ���� �� ������
           0,                 -- RECVD - ������������� ��� - ��� �����
           0,                 -- ������������� ���
           SYSDATE,           -- ���� �������������
           SYSDATE,           -- ������� �� �����
           p_src_bill_id,     -- ID - ������������� �����
           p_src_period_id,   -- ID - ������� ������������� �����
           NULL,              -- NEXT_BILL_ID
           NULL,              -- NEXT_BILL_PERIOD_ID
           SYSDATE,           -- CALC_DATE
           p_notes,
           v_contract_id,     -- ID ��������
           VAT,               -- ����� ������
           v_profile_id,      -- ID ������� �/�
           v_contractor_id,   -- ID ��������
           v_bank_id          -- ID ����� ��������
      FROM BILL_T B, CONTRACTOR_T CR
     WHERE B.BILL_ID        = p_src_bill_id
       AND B.REP_PERIOD_ID  = p_src_period_id
       AND CR.CONTRACTOR_ID = v_contractor_id;  
    --
    -- ���������� �������� ����������� �� �������� ���� ������� �� �������
    FOR tr IN (
        SELECT TRANSFER_ID, PAY_PERIOD_ID, PAYMENT_ID FROM PAY_TRANSFER_T
         WHERE BILL_ID       = p_src_bill_id
           AND REP_PERIOD_ID = p_src_period_id
      )
    LOOP
      PK10_PAYMENTS_TRANSFER.Delete_from_chain (
               p_pay_period_id => tr.pay_period_id,
               p_payment_id    => tr.payment_id,
               p_transfer_id   => tr.transfer_id
           );
    END LOOP;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ������ � �������� ����� �� ������-����
    -- � ������������ � ���� �������������
    UPDATE BILL_T B
      SET B.NEXT_BILL_ID = v_bill_id, -- ������ �� ������-����
          B.NEXT_BILL_PERIOD_ID = p_crd_period_id,
          B.RECVD    = 0,             -- ������ �� ������� �������
          B.DUE      = 0,             -- ������������� ���
          B.DUE_DATE = SYSDATE,
          B.ADJUSTED = B.TOTAL        -- ������������ � 0 ����������
     WHERE BILL_ID       = p_src_bill_id
       AND REP_PERIOD_ID = p_src_period_id;
     
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������������� ������� ��� ������� ITEM-��
    --
    DELETE FROM PK07_BILL_ADJUST_TMP T
     WHERE T.BILL_ID_SRC   = p_src_bill_id
       AND T.PERIOD_ID_SRC = p_src_period_id;

    INSERT INTO PK07_BILL_ADJUST_TMP(
            PERIOD_ID_SRC, BILL_ID_SRC, ITEM_ID_SRC, INV_ITEM_ID_SRC,
            PERIOD_ID_CRD, BILL_ID_CRD, ITEM_ID_CRD
         )
    SELECT REP_PERIOD_ID, BILL_ID, ITEM_ID, INV_ITEM_ID,
           p_crd_period_id PERIOD_ID_CRD, 
           v_bill_id BILL_ID_CRD, PK02_POID.NEXT_ITEM_ID ITEM_ID_CRD
      FROM ITEM_T I 
     WHERE I.BILL_ID = p_src_bill_id
       AND I.REP_PERIOD_ID = p_src_period_id;

    MERGE INTO PK07_BILL_ADJUST_TMP T
    USING (
        SELECT BILL_ID_SRC, INV_ITEM_ID_SRC, 
               PK02_POID.Next_invoice_item_id INV_ITEM_ID_CRD 
          FROM (
            SELECT BILL_ID_SRC, INV_ITEM_ID_SRC 
              FROM PK07_BILL_ADJUST_TMP
             WHERE BILL_ID_SRC   = p_src_bill_id
               AND PERIOD_ID_SRC = p_src_period_id
             GROUP BY BILL_ID_SRC, INV_ITEM_ID_SRC
        )
    ) TT
    ON (
        T.BILL_ID_SRC = TT.BILL_ID_SRC AND T.INV_ITEM_ID_SRC = TT.INV_ITEM_ID_SRC
    )
    WHEN MATCHED THEN UPDATE SET T.INV_ITEM_ID_CRD = TT.INV_ITEM_ID_CRD;
    --
    -- ��������� ������� ������-������. ����� �����������
    INSERT INTO INVOICE_ITEM_T (
           BILL_ID, REP_PERIOD_ID, INV_ITEM_ID, INV_ITEM_NO, SERVICE_ID,
           TOTAL, GROSS, TAX, 
           VAT, INV_ITEM_NAME, DATE_FROM, DATE_TO
           )
    WITH TMP AS (
        SELECT DISTINCT T.INV_ITEM_ID_SRC, T.INV_ITEM_ID_CRD 
          FROM PK07_BILL_ADJUST_TMP T
         WHERE T.BILL_ID_SRC   = p_src_bill_id 
           AND T.PERIOD_ID_SRC = p_src_period_id
    )
    SELECT v_bill_id, p_crd_period_id, 
           TMP.INV_ITEM_ID_CRD, 
           V.INV_ITEM_NO, V.SERVICE_ID,
          -V.TOTAL, -V.GROSS, -V.TAX, 
           V.VAT, V.INV_ITEM_NAME, V.DATE_FROM, V.DATE_TO
      FROM INVOICE_ITEM_T V, TMP
     WHERE V.BILL_ID       = p_src_bill_id 
       AND V.REP_PERIOD_ID = p_src_period_id
       AND V.INV_ITEM_ID   = TMP.INV_ITEM_ID_SRC;

    -- ��������� ������ ������� ���������� � �������������. ����� �����������
    INSERT INTO ITEM_T (
           BILL_ID, REP_PERIOD_ID,
           ITEM_ID, ITEM_TYPE, ORDER_ID,
           ITEM_TOTAL, RECVD,
           SERVICE_ID, CHARGE_TYPE,
           DATE_FROM, DATE_TO,
           INV_ITEM_ID,
           ITEM_STATUS,
           SUBSERVICE_ID,
           TAX_INCL,
           CREATE_DATE,
           LAST_MODIFIED,
           REP_GROSS, 
           REP_TAX,
           --EXTERNAL_ID,
           ORDER_BODY_ID,
           DESCR
           )
    SELECT v_bill_id, p_crd_period_id, 
           T.ITEM_ID_CRD ITEM_ID,
           ITEM_TYPE, ORDER_ID,
           -ITEM_TOTAL, 0, 
           SERVICE_ID, CHARGE_TYPE,
           DATE_FROM, DATE_TO,
           T.INV_ITEM_ID_CRD INV_ITEM_ID,
           ITEM_STATUS,
           SUBSERVICE_ID,
           TAX_INCL,
           SYSDATE,
           SYSDATE,
           -REP_GROSS,
           -REP_TAX,
           --EXTERNAL_ID,
           ORDER_BODY_ID,
           DESCR
      FROM ITEM_T I, PK07_BILL_ADJUST_TMP T 
     WHERE BILL_ID = p_src_bill_id
       AND REP_PERIOD_ID = p_src_period_id
       AND ITEM_TYPE IN (PK00_CONST.c_ITEM_TYPE_BILL, PK00_CONST.c_ITEM_TYPE_ADJUST)
       AND I.BILL_ID       = T.BILL_ID_SRC
       AND I.REP_PERIOD_ID = T.PERIOD_ID_SRC
       AND I.ITEM_ID       = T.ITEM_ID_SRC
    ;
    --
    -- ������������� ������ �������� �����
    v_balance := Pk05_Account_Balance.Refresh_balance(v_account_id);
    
    Pk01_Syslog.Write_msg('Stop, crd_bill_id='||v_bill_id, 
                          c_PkgName||'.'||v_prcName);
    RETURN v_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- �������� ����� �����-����, ����������:
--   - ������������� - ID �������� �����, 
--   - ��� ������ ���������� ����������
FUNCTION Open_debit_note (
               p_crd_bill_id   IN INTEGER,   -- ID ������-���� ��� ������� ��������� �����-���� (ID ������-����)
               p_crd_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM ������-����  
               p_dbt_period_id IN INTEGER,   -- ID ���������� ������� �����-���� YYYYMM
               is_items_create IN INTEGER,   -- ����� �� ��������� ������� � �����-����
               p_notes         IN VARCHAR2   -- ����������
           ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Open_debit_note';
    v_dbt_bill_id    INTEGER;
    v_bill_id        INTEGER;
    v_bill_period_id INTEGER;
    v_bill_no_new    VARCHAR2(100);
    v_bill_no        VARCHAR2(100);
    v_paid_to        DATE;
    v_bill_date      DATE;
    v_account_id     INTEGER;
    v_contract_id    INTEGER;
    v_profile_id     INTEGER;
    v_contractor_id  INTEGER;
    v_bank_id        INTEGER;
    v_vat            NUMBER;
    v_currency_id    INTEGER;
    v_result         NUMBER;
BEGIN
    -- �������� ACCOUNT_ID ������������� �����
    SELECT PREV_BILL_ID, PREV_BILL_PERIOD_ID, ACCOUNT_ID, CURRENCY_ID
      INTO v_bill_id, v_bill_period_id, v_account_id, v_currency_id
      FROM BILL_T
     WHERE BILL_ID = p_crd_bill_id
       AND REP_PERIOD_ID = p_crd_period_id;

    -- ��������� ID ������� ��� ���������� ������������ ������� 
    v_dbt_bill_id := Pk02_POID.Next_bill_id;
    v_bill_date   := Pk04_Period.Period_from(p_dbt_period_id);
    v_paid_to     := ADD_MONTHS(v_bill_date,1);
    v_bill_no_new := Get_billno_for_credit_debit(p_crd_bill_id,p_crd_period_id);
    
    -- �������� id �������� � ������ ���, ��� ������ �������
    Pk07_Bill.Read_account_profile (
               p_account_id    => v_account_id,
               p_bill_date     => v_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );
    
    -- ��������� �� ��������� �� ������� ������� (��� ���������� ������������ ����� ��������)
    SELECT 
      CASE
        WHEN SUBSTR(v_bill_no_new,5,1) = '/' AND CR.REGION_ID != SUBSTR(v_bill_no_new,1,4) THEN
          -- ����������� ������ ������
          LPAD(TO_CHAR(CR.REGION_ID), 4,'0')||'/'||SUBSTR(v_bill_no_new,6)
        WHEN SUBSTR(v_bill_no_new,5,1) = '/' AND CR.REGION_ID IS NULL THEN
          -- ������ ������, � ��� ���� �� ������
          SUBSTR(v_bill_no_new,6)
        WHEN SUBSTR(v_bill_no_new,5,1) != '/' AND CR.REGION_ID IS NOT NULL THEN
          -- �� ������ ������, � ������ ����
          LPAD(TO_CHAR(CR.REGION_ID), 4,'0')||'/'||v_bill_no_new
        ELSE
          -- ��� � �������
          v_bill_no_new
       END BILL_NO
      INTO v_bill_no
      FROM CONTRACTOR_T CR
     WHERE CR.CONTRACTOR_ID = v_contractor_id
    ;
    
    -- C������ ������� ����� ������-����
    INSERT INTO BILL_T (
        BILL_ID, REP_PERIOD_ID, ACCOUNT_ID, 
        BILL_NO, BILL_DATE, BILL_TYPE, 
        BILL_STATUS, CURRENCY_ID, 
        TOTAL, GROSS, TAX, ADJUSTED, 
        RECVD, DUE, DUE_DATE, PAID_TO,         
        PREV_BILL_ID, PREV_BILL_PERIOD_ID, 
        NEXT_BILL_ID, NEXT_BILL_PERIOD_ID,
        CALC_DATE, NOTES,
        CONTRACT_ID, VAT, PROFILE_ID, 
        CONTRACTOR_ID, CONTRACTOR_BANK_ID
    ) VALUES (
         v_dbt_bill_id,
         p_dbt_period_id, -- ID ���������� ������� YYYYMM
         v_account_id,
         v_bill_no,       --BILL_NO,         -- �������� ����� ���-�� �����������
         v_bill_date,     -- ���� ��������� � ��������� �������
         PK00_CONST.c_BILL_TYPE_DBT,
         PK00_CONST.c_BILL_STATE_OPEN, -- ���� ������, ������� INVOICE ���������� ��� ��������
         v_currency_id,   -- ������ �����
         0,               -- TOTAL
         0,               -- GROSS
         0,               -- TAX
         0,               -- ADJUSTED
         0,               -- RECVD
         0,               -- DUE
         SYSDATE,         -- DUE_DATE 
         v_paid_to,       -- PAID_TO
         p_crd_bill_id,   -- ID - ������-����
         p_crd_period_id, -- ID - ������� ������-����
         NULL,            -- NEXT_BILL_ID
         NULL,            -- NEXT_BILL_PERIOD_ID
         SYSDATE,         -- CALC_DATE
         p_notes,
         v_contract_id,
         v_vat,
         v_profile_id,
         v_contractor_id, 
         v_bank_id 
    );
     
    -- ����������� ������ � ������-���� ������ �� �����-����
    UPDATE BILL_T
      SET NEXT_BILL_ID  = v_dbt_bill_id,
          NEXT_BILL_PERIOD_ID = p_dbt_period_id
     WHERE BILL_ID = p_crd_bill_id
       AND REP_PERIOD_ID = p_crd_period_id;
    --
    IF is_items_create =1 THEN
        -- ��������� ������ ������� ����������.
        INSERT INTO ITEM_T (
               BILL_ID, REP_PERIOD_ID, 
               ITEM_ID, ITEM_TYPE, ORDER_ID,
               ITEM_TOTAL, RECVD,
               SERVICE_ID, SUBSERVICE_ID,
               CHARGE_TYPE,
               DATE_FROM, DATE_TO,
               INV_ITEM_ID,
               ITEM_STATUS,
               TAX_INCL,
               CREATE_DATE,
               LAST_MODIFIED,
               ORDER_BODY_ID,
               DESCR 
               )
        SELECT v_dbt_bill_id, p_dbt_period_id,
               PK02_POID.Next_item_id ITEM_ID,
               ITEM_TYPE, ORDER_ID,
               ITEM_TOTAL, 0,
               SERVICE_ID,
               SUBSERVICE_ID,
               CHARGE_TYPE,
               DATE_FROM, DATE_TO,
               NULL INV_ITEM_ID,
               PK00_CONST.c_ITEM_STATE_OPEN,
               TAX_INCL,
               SYSDATE,
               SYSDATE,
               ORDER_BODY_ID,
               DESCR
          FROM ITEM_T
         WHERE BILL_ID = v_bill_id
           AND REP_PERIOD_ID = v_bill_period_id
           AND ITEM_TYPE IN (PK00_CONST.c_ITEM_TYPE_BILL)
        ;
    END IF;
    RETURN v_dbt_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������� ����� - ����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Recalc_debit_note (
               p_dbt_bill_id    IN INTEGER,   -- ID �����-����
               p_dbt_period_id  IN INTEGER,   -- ID ���������� ������� �����-���� YYYYMM
               p_crd_period_id  IN INTEGER    -- ID ���������� ������� ������-���� YYYYMM
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Recalc_debit_note';
    v_task_id        INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start dbt_bill_id = '||p_dbt_bill_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk33_Billing_Account.Recalc_bill (
                p_bill_id        => p_dbt_bill_id,
                p_bill_period_id => p_dbt_period_id,
                p_data_period_id => p_crd_period_id
              );
    /*
    -- ������ ���� � ������� �� ���������
    v_task_id := Pk33_Billing_Account.push_Bill(
                                p_bill_id        => p_dbt_bill_id,
                                p_bill_period_id => p_dbt_period_id,
                                p_data_period_id => p_crd_period_id );
    
    -- �������������� ����, �� ������ ������� �����
    Pk30_Billing_Queue.Rollback_bills(v_task_id );

    -- ��������������� ������������� ����������,
    -- �� ����������� ��� ��� ����������� ����������� �������
    Pk30_Billing_Queue.Rollback_fixrates(v_task_id );

    -- ���������� ����������� ����� � ������� �� ����������� �����
    Pk36_Billing_Fixrate.Charge_fixrates( v_task_id );

    -- ��������� ���� 
    Pk30_Billing_Queue.Close_bills(v_task_id );

    -- ����������� ������ �������� �����
    Pk30_Billing_Base.Refresh_balance(v_task_id);

    -- ����������� �������
    Pk30_Billing_Queue.Close_task(v_task_id);
    */
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


END PK07_BILL_ADJUST;
/
