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
                   p_notes         IN VARCHAR2,  -- ����������
                   p_errcode_id    IN INTEGER DEFAULT NULL -- ��� ������
               ) RETURN INTEGER;

    -- ������� ������ ���� �� �������� ��� ������������� �������, ���� �� ��� ��� �������������
    PROCEDURE Delete_credit_note (
                   p_crd_bill_id   IN INTEGER,   -- ID ������-���� ��� ������� ��������� �����-���� (ID ������-����)
                   p_crd_period_id IN INTEGER   -- ID ���������� ������� YYYYMM ������-����  
               );

    -- �������� ����� �����-����, ����������:
    --   - ������������� - ID �������� �����, 
    --   - ��� ������ ���������� ����������
    FUNCTION Open_debit_note (
                   p_crd_bill_id   IN INTEGER,   -- ID ������-���� ��� ������� ��������� �����-���� (ID ������-����)
                   p_crd_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM ������-����  
                   p_dbt_period_id IN INTEGER,   -- ID ���������� ������� �����-���� YYYYMM
                   is_items_create IN INTEGER,   -- ����� �� ��������� ������� � �����-����                   
                   p_notes         IN VARCHAR2,  -- ����������
                   p_errcode_id    IN INTEGER DEFAULT NULL -- ��� ������
               ) RETURN INTEGER;
    
    -- �������� ����� �����-����, ����������:
    --   - ������������� - ID �������� �����, 
    --   - ��� ������ ���������� ����������
    FUNCTION Open_debit_note (
                   p_crd_bill_id   IN INTEGER,   -- ID ������-���� ��� ������� ��������� �����-���� (ID ������-����)
                   p_crd_period_id IN INTEGER,   -- ID ���������� ������� YYYYMM ������-����  
                   p_dbt_period_id IN INTEGER,   -- ID ���������� ������� �����-���� YYYYMM
                   p_dbt_bill_type IN CHAR,      -- D/A ��� �����-���� �����������/�������������
                   is_items_create IN INTEGER,   -- ����� �� ��������� ������� � �����-����
                   p_notes         IN VARCHAR2,  -- ����������
                   p_errcode_id    IN INTEGER DEFAULT NULL -- ��� ������
               ) RETURN INTEGER;
    
    -- ������� ����� ���� �� �������� ��� ������������� �������, ���� �� ��� ��� �������������
    PROCEDURE Delete_debet_note (
                   p_dbt_bill_id   IN INTEGER,   -- ID ������-���� ��� ������� ��������� �����-���� (ID ������-����)
                   p_dbt_period_id IN INTEGER   -- ID ���������� ������� YYYYMM ������-����  
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���� ������� ����� � ������� ������������ � ���������� ��� � BILL_T
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Find_first_for_debit_note (
                   p_dbt_bill_id    IN INTEGER,   -- ID �����-����
                   p_dbt_period_id  IN INTEGER    -- ID ���������� ������� �����-���� YYYYMM
               );
           
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� ����� - ����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Recalc_debit_note (
                   p_dbt_bill_id    IN INTEGER,   -- ID �����-����
                   p_dbt_period_id  IN INTEGER,   -- ID ���������� ������� �����-���� YYYYMM
                   p_crd_period_id  IN INTEGER    -- ID ���������� ������� ������-���� YYYYMM
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� CCAD BDR �� ������� ����� - ����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Move_ccad_bdr (
                   p_src_bill_id   IN INTEGER,    -- ID ����� ��� �������� ��������� �����-����
                   p_src_period_id IN INTEGER,    -- ID ���������� ������� YYYYMM ���������
                   p_dst_bill_id   IN INTEGER,    -- ID �����-����
                   p_dst_period_id IN INTEGER     -- ID ���������� ������� �����-���� YYYYMM
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �-��� ��� �������� ������������������ ���������� (BDR-�) 
    -- � ������ ����� �� ������ (���������� ������)
    -- ����������: 0  - ������� ���������
    --             -1 - ������ 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Move_BDR(p_Src_Bill_Id     IN INTEGER,
                      p_Src_Rep_Per_Id  IN INTEGER,
                      p_Dest_Bill_Id    IN INTEGER,
                      p_Dest_Rep_Per_Id IN INTEGER
                     ) RETURN INTEGER;
    
    -- = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = --
    --  ����������� ����� �� ��������� ������
    -- = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = --
    -- ���� ��������� � ������ ���� �� �����-�� ������� ���� �� 
    -- ��������� ������ �� ��� ���������, � ������ ����� 
    -- ����� ���������� ���������� ����� � ��������� ����,
    -- � �� ������ ���������� ��������� � ������� ����
    -- ����������:
    --   - ������������� - ID �����, 
    --   - ��� ������ ���������� ����������
    FUNCTION Open_rec_bill_for_old_period (  
                   p_account_id    IN INTEGER, -- ID �������� �����
                   p_period_id     IN INTEGER, -- ID �������� ���������� ������� YYYYMM
                   p_old_period_id IN INTEGER  -- ID ������� ���������� ������� YYYYMM
                ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ���������� �� ������ ( ����� ������ � ��� )
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ���������� �� ������, ���������, ������ ����������� ������� � ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_bill_for_old_period(  
                   p_bill_id       IN INTEGER, -- ID �������� �����
                   p_period_id     IN INTEGER, -- ID �������� ���������� ������� YYYYMM
                   p_old_period_id IN INTEGER  -- ID ������� ���������� ������� YYYYMM
                );

    -- = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = --
    --  ������ ��������� �� ��������� ������ � ��������� ����
    -- = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = --
    PROCEDURE Charge_abp_for_old_period(  
                   p_order_id      IN INTEGER, -- ID �����, ��� �������� ����� ������������� ������
                   p_bill_id       IN INTEGER, -- ID �������� �����
                   p_period_id     IN INTEGER, -- ID �������� ���������� ������� YYYYMM
                   p_old_period_id IN INTEGER  -- ID ������� ���������� ������� YYYYMM
                );

    -- ----------------------------------------------------------------------- --
    -- ������� ������������� �����
    -- ----------------------------------------------------------------------- --
    PROCEDURE Adjust_history (
                   p_recordset OUT t_refc,
                   p_bill_id    IN INTEGER, -- id ����������������� �����
                   p_period_id  IN INTEGER  -- ������ ����������������� �����
               );
               
    -- ----------------------------------------------------------------------- --
    -- ������� ������������� ����� � �������� ������� (�� �����/������ ���� � �����)
    -- ----------------------------------------------------------------------- --
    PROCEDURE Adjust_history_desc (
                   p_recordset    OUT t_refc,
                   p_ads_bill_id   IN INTEGER, -- id ����������������� �����
                   p_ads_period_id IN INTEGER  -- ������ ����������������� �����
               );

    -- ----------------------------------------------------------------------- --
    -- ������ ����� ������� ������������� ����� (�� Portal 6.5)
    -- ----------------------------------------------------------------------- --
    PROCEDURE Err_code_list (
                   p_recordset OUT t_refc
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

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������������ ������������ ������ � Item-��
-- ( ���� ���������, �� ������ )
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Correct_item_currency (
               p_dbt_bill_id    IN INTEGER,   -- ID �����-����
               p_dbt_period_id  IN INTEGER    -- ID ���������� ������� �����-���� YYYYMM
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Correct_item_currency';
BEGIN
    --
    MERGE INTO ITEM_T I
    USING (
        SELECT DISTINCT 
               O.ACCOUNT_ID, O.ORDER_ID, OB.ORDER_BODY_ID, OB.CHARGE_TYPE, 
               OB.RATEPLAN_ID, R.RATEPLAN_NAME,
               CASE
               WHEN OB.RATEPLAN_ID IS NOT NULL THEN R.CURRENCY_ID
               ELSE OB.CURRENCY_ID 
               END CURRENCY_ID, B.BILL_ID, B.REP_PERIOD_ID
          FROM ORDER_T O, ORDER_BODY_T OB, RATEPLAN_T R, BILL_T B
         WHERE O.ORDER_ID      = OB.ORDER_ID
           AND OB.RATEPLAN_ID  = R.RATEPLAN_ID(+)
           AND B.BILL_ID       = p_dbt_bill_id
           AND B.REP_PERIOD_ID = p_dbt_period_id
           AND B.ACCOUNT_ID    = O.ACCOUNT_ID
    ) BO
    ON (
        BO.BILL_ID = I.BILL_ID AND
        BO.REP_PERIOD_ID = I.REP_PERIOD_ID AND
        BO.ORDER_ID = I.ORDER_ID AND
        BO.ORDER_BODY_ID = I.ORDER_BODY_ID AND
        BO.CHARGE_TYPE = I.CHARGE_TYPE
    )
    WHEN MATCHED THEN UPDATE SET I.ITEM_CURRENCY_ID = BO.CURRENCY_ID;
    --
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
               p_notes         IN VARCHAR2,  -- ����������
               p_errcode_id    IN INTEGER DEFAULT NULL -- ��� ������
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Open_credit_note';
    v_bill_id       INTEGER;                    -- ������ POID: YYMM.XXX.XXX.XXX,
    v_bill_no_new   VARCHAR2(100);
    v_next_bill_id  INTEGER;
    v_bill_status   VARCHAR2(10);
    v_bill_date     DATE;
    v_account_id    INTEGER;
    v_contract_id   INTEGER;
    v_profile_id    INTEGER;
    v_contractor_id INTEGER;
    v_bank_id       INTEGER;
    v_vat           NUMBER;
    v_balance       NUMBER;
    v_inv_corr_date DATE; 
    v_inv_corr_num  INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('src_bill_id='||p_src_bill_id||
                          ', src_period_id='||p_src_period_id||
                          ', dst_period_id='||p_crd_period_id, 
                          c_PkgName||'.'||v_prcName);

    -- ���������, ����� �� ������ ���������. 
    -- ���� ���� ������ �/��� ����� ���� ��� ��������� ������������� - ������
    SELECT NEXT_BILL_ID, BILL_STATUS, ACCOUNT_ID 
      INTO v_next_bill_id, v_bill_status, v_account_id
      FROM BILL_T 
     WHERE BILL_ID = p_src_bill_id
       AND REP_PERIOD_ID = p_src_period_id;

    -- ��������� ������ ����� 
    Pk07_Bill.Check_bill_period(p_crd_period_id, v_account_id);
    
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
    
    -- ��������� ���� � ����� ����������� �����-������� (����� ����� � �� �����)
    SELECT BILL_DATE, CORR_NUM
      INTO v_inv_corr_date, v_inv_corr_num
    FROM (
        SELECT CONNECT_BY_ISLEAF LF, B.BILL_DATE, 
               SUM(DECODE(BILL_TYPE,'D', 1, 'A', 1, 0)) OVER (PARTITION BY CONNECT_BY_ROOT BILL_ID) CORR_NUM
          FROM BILL_T B
        CONNECT BY NOCYCLE PRIOR B.PREV_BILL_ID = B.BILL_ID 
                       AND PRIOR B.PREV_BILL_PERIOD_ID = B.REP_PERIOD_ID
          START WITH B.REP_PERIOD_ID = p_src_period_id 
                 AND B.BILL_ID       = p_src_bill_id
    )
    WHERE LF = 1;
    
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
        TOTAL,               -- -old
        GROSS,               -- -old
        TAX,                 -- -old
        ADJUSTED,            -- -old total - (- old recvd)
        RECVD,               -- -recvd
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
        CONTRACTOR_BANK_ID,
        ERR_CODE_ID,
        INVOICE_CORR_DATE,
        INVOICE_CORR_NUM
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
                TO_CHAR(CR.REGION_ID)||'/'|| v_bill_no_new  -- ���������� ������ �. �� 09.07.2015  LPAD(TO_CHAR(CR.REGION_ID), 4,'0')||'/'||v_bill_no_new
              ELSE
                -- ��� � �������
                v_bill_no_new
           END BILL_NO,
           v_bill_date,       -- ���� ��������� � ��������� �������
           PK00_CONST.c_BILL_TYPE_CRD,
           PK00_CONST.c_BILL_STATE_OPEN, -- ���� �������� ������, ��� ���������� item-s
           CURRENCY_ID,       -- ������ �����
           -TOTAL,            -- ���������� ����������
           -GROSS,            -- ���������� ���������� ��� ���
           -TAX,              -- ���������� ������
           RECVD - TOTAL,     -- �������������: ������������� ���� �� ������
           -RECVD,            -- RECVD - ������������� ��� - ��� �����
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
           v_bank_id,         -- ID ����� ��������
           p_errcode_id,      -- ��� ������, ������� ������ � ������������� �����
           v_inv_corr_date,   -- ���� ������� ��������������� �����
           v_inv_corr_num     -- ����� ����������� (����� ��� �����-����, ����� �� ����)
      FROM BILL_T B, CONTRACTOR_T CR
     WHERE B.BILL_ID        = p_src_bill_id
       AND B.REP_PERIOD_ID  = p_src_period_id
       AND CR.CONTRACTOR_ID = v_contractor_id;  
    --
    /*
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
    */
    -- ������������� ������� �������� �������� ��� �������� ������-����
    PK10_PAYMENTS_TRANSFER.Revers_bill_transfer (
               p_dst_period_id     => p_crd_period_id,
               p_bill_period_id => p_src_period_id,
               p_bill_id       => p_src_bill_id
           );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� ������ � �������� ����� �� ������-����
    -- � ������������ � ���� �������������
    UPDATE BILL_T B
      SET B.NEXT_BILL_ID = v_bill_id, -- ������ �� ������-����
          B.NEXT_BILL_PERIOD_ID = p_crd_period_id,
          --B.RECVD    = 0,             -- ������ �� ������� /�������/ ������������ �� ������-����
          B.DUE      = 0,             -- ������������� ���
          B.DUE_DATE = SYSDATE,
          B.ADJUSTED = B.RECVD-B.TOTAL -- ������������ � 0 ����������
     WHERE BILL_ID       = p_src_bill_id
       AND REP_PERIOD_ID = p_src_period_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������� ������ � ����������
    --
    FOR inv IN (
      SELECT v_bill_id       NEW_BILL_ID, 
             p_crd_period_id NEW_REP_PERIOD_ID, 
             PK02_POID.NEXT_INVOICE_ITEM_ID NEW_INV_ITEM_ID, 
             V.* 
        FROM INVOICE_ITEM_T V
       WHERE V.BILL_ID       = p_src_bill_id
         AND V.REP_PERIOD_ID = p_src_period_id
    )
    LOOP
      -- ��������� ������� ������-������. ����� �����������
      INSERT INTO INVOICE_ITEM_T (
             BILL_ID, REP_PERIOD_ID, 
             INV_ITEM_ID, INV_ITEM_NO, SERVICE_ID,
             TOTAL, GROSS, TAX, 
             VAT, INV_ITEM_NAME, DATE_FROM, DATE_TO
             )
      VALUES(
             inv.New_Bill_Id, inv.New_Rep_Period_Id, 
             inv.New_Inv_Item_Id, inv.Inv_Item_No, inv.Service_Id,
            -inv.Total, -inv.Gross, -inv.Tax, 
             inv.Vat, inv.Inv_Item_Name, inv.Date_From, inv.Date_To
      );
      -- ��������� ������� ����������. ����� �����������
      INSERT INTO ITEM_T (
             BILL_ID, REP_PERIOD_ID,
             ITEM_ID, ITEM_TYPE, ORDER_ID,
             ITEM_TOTAL, ITEM_CURRENCY_ID, BILL_TOTAL,
             RECVD,
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
      SELECT inv.New_Bill_Id, inv.New_Rep_Period_id, 
             Pk02_Poid.Next_item_id ITEM_ID,
             ITEM_TYPE, ORDER_ID,
             -ITEM_TOTAL, ITEM_CURRENCY_ID, -BILL_TOTAL, 0, 
             SERVICE_ID, CHARGE_TYPE,
             DATE_FROM, DATE_TO,
             inv.New_Inv_Item_Id INV_ITEM_ID,
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
        FROM ITEM_T I 
       WHERE BILL_ID         = p_src_bill_id
         AND REP_PERIOD_ID   = p_src_period_id
         AND I.INV_ITEM_ID   = inv.Inv_Item_Id
      ;
    END LOOP;
    --
    -- ��������� ���� (��������� � ��� ���������)
    UPDATE BILL_T B
       SET B.BILL_STATUS    = Pk00_Const.c_BILL_STATE_CLOSED
     WHERE B.BILL_ID        = v_bill_id
       AND B.REP_PERIOD_ID  = p_crd_period_id;
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

-- ������� ������ ���� �� �������� ��� ������������� �������, ���� �� ��� ��� �������������
PROCEDURE Delete_credit_note (
               p_crd_bill_id   IN INTEGER,   -- ID ������-���� ��� ������� ��������� �����-���� (ID ������-����)
               p_crd_period_id IN INTEGER   -- ID ���������� ������� YYYYMM ������-����  
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Delete_credit_note';
    v_next_bill_id   INTEGER;
    v_prev_bill_id   INTEGER;
    v_prev_period_id INTEGER;    
    v_account_id     INTEGER;
BEGIN
    -- ��������� ������� �����������
    SELECT B.ACCOUNT_ID, B.NEXT_BILL_ID, B.PREV_BILL_ID, B.PREV_BILL_PERIOD_ID
      INTO v_account_id, v_next_bill_id, v_prev_bill_id, v_prev_period_id  
      FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_crd_period_id
       AND B.BILL_ID       = p_crd_bill_id
       AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_CRD
    ;
    -- ��������� ������ ����� 
    Pk07_Bill.Check_bill_period(p_crd_period_id, v_account_id);
    
    -- ���������, ��� �� ���� ��� ������
    IF v_next_bill_id IS NOT NULL THEN
       Pk01_Syslog.Raise_user_exception('bill_id_id='||p_crd_bill_id||' - next_bill_id is not null' , c_PkgName||'.'||v_prcName);
    END IF;
    
    -- �������� ������ ���������� ����� � ��������� � ������ ������
    UPDATE BILL_T B 
       SET B.NEXT_BILL_ID = NULL, 
           B.NEXT_BILL_PERIOD_ID = NULL
     WHERE B.REP_PERIOD_ID = v_prev_period_id
       AND B.BILL_ID       = v_prev_bill_id;
    
    -- ������� ���� � ��� ����������
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_crd_period_id
       AND I.BILL_ID       = p_crd_bill_id;

    DELETE FROM INVOICE_ITEM_T V
     WHERE V.REP_PERIOD_ID = p_crd_period_id
       AND V.BILL_ID       = p_crd_bill_id;
    
    DELETE FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_crd_period_id
       AND B.BILL_ID       = p_crd_bill_id;
    
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
               p_notes         IN VARCHAR2,  -- ����������
               p_errcode_id    IN INTEGER DEFAULT NULL -- ��� ������
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
    v_inv_corr_date  DATE; 
    v_inv_corr_num   INTEGER;
BEGIN
    -- �������� ACCOUNT_ID ������������� �����
    SELECT PREV_BILL_ID, PREV_BILL_PERIOD_ID, ACCOUNT_ID, CURRENCY_ID
      INTO v_bill_id, v_bill_period_id, v_account_id, v_currency_id
      FROM BILL_T
     WHERE BILL_ID = p_crd_bill_id
       AND REP_PERIOD_ID = p_crd_period_id;

    -- ��������� ������ ����� 
    Pk07_Bill.Check_bill_period(p_dbt_period_id, v_account_id);

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
          TO_CHAR(CR.REGION_ID)||'/'|| v_bill_no_new  -- ���������� ������ �. �� 09.07.2015  LPAD(TO_CHAR(CR.REGION_ID), 4,'0')||'/'||v_bill_no_new
        ELSE
          -- ��� � �������
          v_bill_no_new
       END BILL_NO
      INTO v_bill_no
      FROM CONTRACTOR_T CR
     WHERE CR.CONTRACTOR_ID = v_contractor_id
    ;
    
    -- ��������� ���� � ����� ����������� �����-�������
    SELECT BILL_DATE, CORR_NUM
      INTO v_inv_corr_date, v_inv_corr_num
    FROM (
        SELECT CONNECT_BY_ISLEAF LF, B.BILL_DATE, 
               SUM(DECODE(BILL_TYPE,'D', 1, 'A', 1, 0)) OVER (PARTITION BY CONNECT_BY_ROOT BILL_ID) CORR_NUM
          FROM BILL_T B
        CONNECT BY NOCYCLE PRIOR B.PREV_BILL_ID = B.BILL_ID 
                       AND PRIOR B.PREV_BILL_PERIOD_ID = B.REP_PERIOD_ID
          START WITH B.REP_PERIOD_ID = p_crd_period_id 
                 AND B.BILL_ID       = p_crd_bill_id
    )
    WHERE LF = 1;
    
    -- C������ ������� ����� �����-����
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
        CONTRACTOR_ID, CONTRACTOR_BANK_ID,
        ERR_CODE_ID,
        INVOICE_CORR_DATE,
        INVOICE_CORR_NUM
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
         v_bank_id,
         p_errcode_id,
         v_inv_corr_date, 
         v_inv_corr_num
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
               ITEM_TOTAL, ITEM_CURRENCY_ID, BILL_TOTAL, RECVD,
               SERVICE_ID, SUBSERVICE_ID,
               CHARGE_TYPE,
               DATE_FROM, DATE_TO,
               INV_ITEM_ID,
               ITEM_STATUS,
               TAX_INCL,
               CREATE_DATE,
               LAST_MODIFIED,
               ORDER_BODY_ID,
               NOTES,
               DESCR
               )
        SELECT v_dbt_bill_id, p_dbt_period_id,
               PK02_POID.Next_item_id ITEM_ID,
               ITEM_TYPE, ORDER_ID,
               ITEM_TOTAL, ITEM_CURRENCY_ID, BILL_TOTAL, 0,
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
               NOTES,
               DESCR
          FROM ITEM_T
         WHERE BILL_ID = v_bill_id
           AND REP_PERIOD_ID = v_bill_period_id
           --AND ITEM_TYPE IN (PK00_CONST.c_ITEM_TYPE_BILL)    -- ���������� �. ������ �� 2015.07.09
        ;
        -- ��������� ������������ ������������ ������ � Item-�� ( ���� ���������, �� ������ )
        --Correct_item_currency (
        --       v_dbt_bill_id,     -- ID �����-����
        --       p_dbt_period_id    -- ID ���������� ������� �����-���� YYYYMM
        --   );
    END IF;
    RETURN v_dbt_bill_id;
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
               p_dbt_bill_type IN CHAR,      -- D/A ��� �����-���� �����������/�������������
               is_items_create IN INTEGER,   -- ����� �� ��������� ������� � �����-����
               p_notes         IN VARCHAR2,  -- ����������
               p_errcode_id    IN INTEGER DEFAULT NULL -- ��� ������
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
    v_dbt_bill_type  CHAR;
    v_inv_corr_date  DATE; 
    v_inv_corr_num   INTEGER;
BEGIN
    -- �������� ACCOUNT_ID ������������� �����
    SELECT PREV_BILL_ID, PREV_BILL_PERIOD_ID, ACCOUNT_ID, CURRENCY_ID
      INTO v_bill_id, v_bill_period_id, v_account_id, v_currency_id
      FROM BILL_T
     WHERE BILL_ID = p_crd_bill_id
       AND REP_PERIOD_ID = p_crd_period_id;

    -- ��������� ������ ����� 
    Pk07_Bill.Check_bill_period(p_dbt_period_id, v_account_id);

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
          TO_CHAR(CR.REGION_ID)||'/'|| v_bill_no_new  -- ���������� ������ �. �� 09.07.2015  LPAD(TO_CHAR(CR.REGION_ID), 4,'0')||'/'||v_bill_no_new
        ELSE
          -- ��� � �������
          v_bill_no_new
       END BILL_NO
      INTO v_bill_no
      FROM CONTRACTOR_T CR
     WHERE CR.CONTRACTOR_ID = v_contractor_id
    ;
    -- ��������� ��� �����, �� ������ ������
    IF p_dbt_bill_type IN ( Pk00_Const.c_BILL_TYPE_DBT, Pk00_Const.c_BILL_TYPE_ADS ) THEN
        v_dbt_bill_type := p_dbt_bill_type;
    ELSE
        v_dbt_bill_type := Pk00_Const.c_BILL_TYPE_DBT;
    END IF;
    
    -- ��������� ���� � ����� ����������� �����-�������
    SELECT BILL_DATE, CORR_NUM
      INTO v_inv_corr_date, v_inv_corr_num
    FROM (
        SELECT CONNECT_BY_ISLEAF LF, B.BILL_DATE, 
               SUM(DECODE(BILL_TYPE,'D', 1, 'A', 1, 0)) OVER (PARTITION BY CONNECT_BY_ROOT BILL_ID) CORR_NUM
          FROM BILL_T B
        CONNECT BY NOCYCLE PRIOR B.PREV_BILL_ID = B.BILL_ID 
                       AND PRIOR B.PREV_BILL_PERIOD_ID = B.REP_PERIOD_ID
          START WITH B.REP_PERIOD_ID = p_crd_period_id 
                 AND B.BILL_ID       = p_crd_bill_id
    )
    WHERE LF = 1;
    
    -- C������ ������� ����� �����-����
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
        CONTRACTOR_ID, CONTRACTOR_BANK_ID,
        ERR_CODE_ID,
        INVOICE_CORR_DATE,
        INVOICE_CORR_NUM
    ) VALUES (
         v_dbt_bill_id,
         p_dbt_period_id, -- ID ���������� ������� YYYYMM
         v_account_id,
         v_bill_no,       --BILL_NO,         -- �������� ����� ���-�� �����������
         v_bill_date,     -- ���� ��������� � ��������� �������
         v_dbt_bill_type,
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
         v_bank_id,
         p_errcode_id,
         v_inv_corr_date, 
         v_inv_corr_num
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
               ITEM_TOTAL, ITEM_CURRENCY_ID, BILL_TOTAL, RECVD,
               SERVICE_ID, SUBSERVICE_ID,
               CHARGE_TYPE,
               DATE_FROM, DATE_TO,
               INV_ITEM_ID,
               ITEM_STATUS,
               TAX_INCL,
               CREATE_DATE,
               LAST_MODIFIED,
               ORDER_BODY_ID,
               NOTES,
               DESCR
               )
        SELECT v_dbt_bill_id, p_dbt_period_id,
               PK02_POID.Next_item_id ITEM_ID,
               ITEM_TYPE, ORDER_ID,
               ITEM_TOTAL, ITEM_CURRENCY_ID, BILL_TOTAL, 0,
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
               NOTES,
               DESCR
          FROM ITEM_T
         WHERE BILL_ID = v_bill_id
           AND REP_PERIOD_ID = v_bill_period_id
           --AND ITEM_TYPE IN (PK00_CONST.c_ITEM_TYPE_BILL)    -- ���������� �. ������ �� 2015.07.09
        ;
        -- ��������� ������������ ������������ ������ � Item-�� ( ���� ���������, �� ������ )
        --Correct_item_currency (
        --       v_dbt_bill_id,     -- ID �����-����
        --       p_dbt_period_id    -- ID ���������� ������� �����-���� YYYYMM
        --   );
    END IF;
    RETURN v_dbt_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- ������� ����� ���� �� �������� ��� ������������� �������, ���� �� ��� ��� �������������
PROCEDURE Delete_debet_note (
               p_dbt_bill_id   IN INTEGER,  -- ID �����-����
               p_dbt_period_id IN INTEGER   -- ID ���������� ������� YYYYMM �����-����  
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Delete_debet_note';
    v_next_bill_id   INTEGER;
    v_prev_bill_id   INTEGER;
    v_prev_period_id INTEGER;    
    v_account_id     INTEGER;
BEGIN
    -- ��������� ������� �����������
    SELECT B.ACCOUNT_ID, B.NEXT_BILL_ID, B.PREV_BILL_ID, B.PREV_BILL_PERIOD_ID
      INTO v_account_id, v_next_bill_id, v_prev_bill_id, v_prev_period_id
      FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_dbt_period_id
       AND B.BILL_ID       = p_dbt_bill_id
       AND B.BILL_TYPE  IN ( Pk00_Const.c_BILL_TYPE_DBT, Pk00_Const.c_BILL_TYPE_ADS )
    ;
    -- ��������� ������ ����� 
    Pk07_Bill.Check_bill_period(p_dbt_period_id, v_account_id);
    
    -- ���������, ��� �� ���� ��� ������
    IF v_next_bill_id IS NOT NULL THEN
       Pk01_Syslog.Raise_user_exception('bill_id_id='||p_dbt_bill_id||' - next_bill_id is not null' , c_PkgName||'.'||v_prcName);
    END IF;
    -- �������� ������ ������� ����� � ��������� � ������ ������
    UPDATE BILL_T B 
       SET B.NEXT_BILL_ID = NULL, 
           B.NEXT_BILL_PERIOD_ID = NULL,
           B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_OPEN
     WHERE B.REP_PERIOD_ID = v_prev_period_id
       AND B.BILL_ID       = v_prev_bill_id;
    
    -- ������� ���� � ��� ����������
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_dbt_period_id
       AND I.BILL_ID       = p_dbt_bill_id;

    DELETE FROM INVOICE_ITEM_T V
     WHERE V.REP_PERIOD_ID = p_dbt_period_id
       AND V.BILL_ID       = p_dbt_bill_id;
    
    DELETE FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_dbt_period_id
       AND B.BILL_ID       = p_dbt_bill_id;
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���� ������� ����� � ������� ������������ � ���������� ��� � BILL_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Find_first_for_debit_note (
               p_dbt_bill_id    IN INTEGER,   -- ID �����-����
               p_dbt_period_id  IN INTEGER    -- ID ���������� ������� �����-���� YYYYMM
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Find_first_for_debit_note';
BEGIN
    UPDATE BILL_T B SET (INVOICE_CORR_DATE, INVOICE_CORR_NUM) = (
              SELECT BILL_DATE, CORR_NUM
                FROM (
                  SELECT CONNECT_BY_ISLEAF LF,  
                         SUM(DECODE(B.BILL_TYPE,'D',1,'A',1,0)) OVER (PARTITION BY CONNECT_BY_ROOT B.BILL_ID) CORR_NUM,
                         B.BILL_ID, B.REP_PERIOD_ID, B.BILL_DATE, B.BILL_NO, B.BILL_TYPE
                    FROM BILL_T B
                  CONNECT BY NOCYCLE PRIOR B.PREV_BILL_ID = B.BILL_ID AND PRIOR B.PREV_BILL_PERIOD_ID = B.REP_PERIOD_ID
                    START WITH B.REP_PERIOD_ID = p_dbt_period_id
                           AND B.BILL_ID       = p_dbt_bill_id
              )
              WHERE LF = 1  
           )
     WHERE B.REP_PERIOD_ID = p_dbt_period_id
       AND B.BILL_ID       = p_dbt_bill_id
    ;
    /*
    -- ������ ������ ��� �������� ����������
    MERGE INTO BILL_T B 
    USING (
        SELECT ROOT_BILL_ID, ROOT_PERIOD_ID, BILL_ID, REP_PERIOD_ID, BILL_DATE, CORR_NUM
          FROM (
            SELECT CONNECT_BY_ISLEAF LF,  
                   SUM(DECODE(B.BILL_TYPE,'D',1,'A',1,0)) OVER (PARTITION BY CONNECT_BY_ROOT B.BILL_ID) CORR_NUM,
                   CONNECT_BY_ROOT BILL_ID ROOT_BILL_ID,         -- p_dbt_bill_id
                   CONNECT_BY_ROOT REP_PERIOD_ID ROOT_PERIOD_ID, -- p_dbt_period_id
                   B.BILL_ID, B.REP_PERIOD_ID, B.BILL_DATE, B.BILL_NO, B.BILL_TYPE
              FROM BILL_T B
            CONNECT BY NOCYCLE PRIOR B.PREV_BILL_ID = B.BILL_ID AND PRIOR B.PREV_BILL_PERIOD_ID = B.REP_PERIOD_ID
              START WITH B.REP_PERIOD_ID = p_dbt_period_id
                     AND B.BILL_ID       = p_dbt_bill_id
        )
        WHERE LF = 1      
    ) D
    ON (
        B.BILL_ID       = D.ROOT_BILL_ID AND
        B.REP_PERIOD_ID = D.ROOT_PERIOD_ID AND
        B.BILL_TYPE IN (Pk00_Const.c_BILL_TYPE_DBT,
                        Pk00_Const.c_BILL_TYPE_ADS )
    )
    WHEN MATCHED THEN UPDATE SET B.INVOICE_CORR_DATE = D.BILL_DATE, 
                                 B.INVOICE_CORR_NUM = D.CORR_NUM
    ;
    */
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

    -- ������ ���� � ������� �� ���������
    v_task_id := Pk33_Billing_Account.push_Bill(
                      p_bill_id        => p_dbt_bill_id,
                      p_bill_period_id => p_dbt_period_id,
                      p_data_period_id => p_crd_period_id );

    -- �������������� ����, �� ������ ������� �����
    Pk30_Billing_Base.Rollback_bills(v_task_id );

    -- ��������������� ������������� ����������,
    -- �� ����������� ��� ��� ����������� ����������� �������
    Pk36_Billing_Fixrate.Rollback_fixrates(v_task_id );

    -- ���������� ����������� ����� � ������� �� ����������� �����
    Pk36_Billing_Fixrate.Charge_fixrates(v_task_id);

    -- ��������������� ����������� �� �������
    Pk38_Billing_Downtime.Rollback_downtimes(v_task_id);

    -- ������������ � ��������� ����������� �� ������� ��� �����-����
    Pk38_Billing_Downtime.Recharge_downtime_for_debet( 
                 p_dbt_bill_id    => p_dbt_bill_id,   -- id - ���������� �����, � ������� ������� ������
                 p_dbt_period_id => p_dbt_period_id, -- ������ ���������� �����
                 p_crd_period_id => p_crd_period_id  -- ������ ����������� �����, ��� �������� ������������� ������
              );

    -- ����� ������ ���������� � ����� �� �������
    Pk39_Billing_Discount.Rollback_bill_discount(v_task_id);

    -- ������������ � ��������� ������ ��� �����-����
    Pk39_Billing_Discount.Recalc_discount_for_debet( 
                 p_dbt_bill_id    => p_dbt_bill_id,  -- id - ���������� �����, � ������� ������� ������
                 p_dbt_period_id => p_dbt_period_id,-- ������ ���������� �����
                 p_crd_period_id => p_crd_period_id  -- ������ ����������� �����, ��� �������� ������������� ������
              );

    -- C����������� ���� 
    Pk30_Billing_Base.Make_bills(v_task_id );

    -- ����������� ������ �������� �����
    Pk30_Billing_Base.Refresh_balance(v_task_id);

    -- ����������� �������
    Pk30_Billing_Queue.Close_task(v_task_id);
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� CCAD BDR �� ������� ����� - ����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Move_ccad_bdr (
               p_src_bill_id   IN INTEGER,    -- ID ����� ��� �������� ��������� �����-����
               p_src_period_id IN INTEGER,    -- ID ���������� ������� YYYYMM ���������
               p_dst_bill_id   IN INTEGER,    -- ID �����-����
               p_dst_period_id IN INTEGER     -- ID ���������� ������� �����-���� YYYYMM
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Move_ccad_bdrs';
    v_count          INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start dst_bill_id = '||p_dst_bill_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- --------------------------------------------------------------------- --
    -- VPN (service_id = 106, subservice_id = 39)
    -- --------------------------------------------------------------------- -- 
    -- ������ ����� �������, ��� ������ �����
    INSERT INTO BDR_CCAD_T (
                BDR_ID, BDR_TYPE_ID, REP_PERIOD_ID, BILL_ID, ITEM_ID, 
                ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, 
                DATE_FROM, DATE_TO, RATEPLAN_ID, QUALITY_ID, 
                ZONE_OUT, ZONE_IN, VOLUME, VOLUME_UNIT_ID, PRICE, AMOUNT, CF, 
                BDR_STATUS_ID, CURRENCY_ID, TAX_INCL, CREATE_DATE, BYTES, RATE_RULE_ID, 
                BDR_PARENT_ID, NOTES )
    SELECT Pk02_Poid.Next_ccad_bdr_id BDR_ID, BDR_TYPE_ID, 
           p_dst_period_id REP_PERIOD_ID, p_dst_bill_id BILL_ID, NULL ITEM_ID, 
           ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, 
           DATE_FROM, DATE_TO, RATEPLAN_ID, QUALITY_ID, 
           ZONE_OUT, ZONE_IN, VOLUME, VOLUME_UNIT_ID, PRICE, AMOUNT, CF, 
           BDR_STATUS_ID, CURRENCY_ID, TAX_INCL, SYSDATE CREATE_DATE, BYTES, RATE_RULE_ID, 
           BDR_ID BDR_PARENT_ID, 'double record' NOTES
     FROM BDR_CCAD_T CB
    WHERE BILL_ID = p_src_bill_id
      AND REP_PERIOD_ID = p_src_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_VPN_T '||v_count||' rows duplicated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ������� ITEM-� 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_dst_period_id
       AND I.BILL_ID       = p_dst_bill_id
       AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG
       AND EXISTS (
           SELECT * FROM BDR_CCAD_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.BILL_ID       = I.BILL_ID
              AND CB.ORDER_ID      = I.ORDER_ID
              AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
              AND CB.SERVICE_ID    = I.SERVICE_ID
              AND CB.SUBSERVICE_ID = I.SUBSERVICE_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

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
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               CB.CURRENCY_ID,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_T CB
         WHERE CB.REP_PERIOD_ID = p_dst_period_id
           AND CB.BILL_ID       = p_dst_bill_id
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        BDR.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,
        CASE
           WHEN BDR.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
           ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE
      FROM BDR
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ����������� ��������� �� item-�    
    MERGE INTO BDR_CCAD_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_T CB
           WHERE I.REP_PERIOD_ID  = p_dst_period_id
             AND I.BILL_ID        = p_dst_bill_id
             AND I.CHARGE_TYPE    = Pk00_Const.c_CHARGE_TYPE_USG
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.BILL_ID       = I.BILL_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_VPN_T->ITEM_ID '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- -------------------------------------------------------------------- --
    -- IP_BURST (service_id = 104, subservice_id = 40)
    -- -------------------------------------------------------------------- --
    -- ������ ����� �������, ��� ������ �����
    INSERT INTO BDR_CCAD_T (BDR_ID, BDR_TYPE_ID, REP_PERIOD_ID, BILL_ID, ITEM_ID, ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, DATE_FROM, DATE_TO, RATEPLAN_ID, PAID_SPEED, PAID_SPEED_UNIT, EXCESS_SPEED, EXCESS_SPEED_UNIT, PRICE, AMOUNT, CF, INFO_WHEN, INFO_ROUTER_IP, INFO_DIRECTION, BDR_STATUS_ID, CURRENCY_ID, TAX_INCL, CREATE_DATE, RATE_RULE_ID, MAX_SPEED_BPS, PORT_SPEED_BPS, BDR_PARENT_ID, NOTES)
    SELECT  Pk02_Poid.Next_ccad_bdr_id BDR_ID, BDR_TYPE_ID, 
            p_dst_period_id REP_PERIOD_ID, p_dst_bill_id BILL_ID, NULL ITEM_ID,
            ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, 
            DATE_FROM, DATE_TO, RATEPLAN_ID, 
            PAID_SPEED, PAID_SPEED_UNIT, EXCESS_SPEED, EXCESS_SPEED_UNIT, 
            PRICE, AMOUNT, CF, INFO_WHEN, INFO_ROUTER_IP, INFO_DIRECTION, 
            BDR_STATUS_ID, CURRENCY_ID, TAX_INCL, SYSDATE CREATE_DATE, RATE_RULE_ID, 
            MAX_SPEED_BPS, PORT_SPEED_BPS, 
            BDR_ID BDR_PARENT_ID, 'double record' NOTES
     FROM BDR_CCAD_T CB
    WHERE BILL_ID = p_src_bill_id
      AND REP_PERIOD_ID = p_src_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_VPN_T '||v_count||' rows duplicated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ������� ITEM-� 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_dst_period_id
       AND I.BILL_ID       = p_dst_bill_id
       AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG
       AND EXISTS (
           SELECT * FROM BDR_CCAD_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.BILL_ID       = I.BILL_ID
              AND CB.ORDER_ID      = I.ORDER_ID
              AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
              AND CB.SERVICE_ID    = I.SERVICE_ID
              AND CB.SUBSERVICE_ID = I.SUBSERVICE_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

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
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               CB.CURRENCY_ID,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_T CB
         WHERE CB.REP_PERIOD_ID = p_dst_period_id
           AND CB.BILL_ID       = p_dst_bill_id
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        BDR.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,
        CASE
           WHEN BDR.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
           ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE
      FROM BDR
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ����������� ��������� �� item-�    
    MERGE INTO BDR_CCAD_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_T CB
           WHERE I.REP_PERIOD_ID  = p_dst_period_id
             AND I.BILL_ID        = p_dst_bill_id
             AND I.CHARGE_TYPE    = Pk00_Const.c_CHARGE_TYPE_USG
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.BILL_ID       = I.BILL_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_BURST_T->ITEM_ID '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- --------------------------------------------------------------------- --
    -- IP_VOLUME  - ����������� �� ������ (service_id = 104, subservice_id = 39)
    -- --------------------------------------------------------------------- --
    -- ������ ����� �������, ��� ������ �����
    INSERT INTO BDR_CCAD_T (
           BDR_ID, BDR_TYPE_ID, 
           REP_PERIOD_ID, BILL_ID, ITEM_ID, 
           ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, 
           DATE_FROM, DATE_TO, RATEPLAN_ID, VOLUME, VOLUME_UNIT_ID, PRICE, AMOUNT, 
           CF, BDR_STATUS_ID, CURRENCY_ID, TAX_INCL, STEP_MIN_BYTES, STEP_MAX_BYTES,
           BYTES, KV, KF, ABON_PLATA, CREATE_DATE, 
           VOLUME_IN, VOLUME_OUT, RATE_RULE_ID, 
           BDR_PARENT_ID, NOTES)
    SELECT 
           Pk02_Poid.Next_ccad_bdr_id BDR_ID, BDR_TYPE_ID, 
           p_dst_period_id REP_PERIOD_ID, p_dst_bill_id BILL_ID, NULL ITEM_ID, 
           ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, 
           DATE_FROM, DATE_TO, RATEPLAN_ID, VOLUME, VOLUME_UNIT_ID, PRICE, AMOUNT, 
           CF, BDR_STATUS_ID, CURRENCY_ID, TAX_INCL, STEP_MIN_BYTES, STEP_MAX_BYTES, 
           BYTES, KV, KF, ABON_PLATA, SYSDATE CREATE_DATE, 
           VOLUME_IN, VOLUME_OUT, RATE_RULE_ID, 
           BDR_ID BDR_PARENT_ID, 'double record' NOTES
     FROM BDR_CCAD_T CB
    WHERE BILL_ID = p_src_bill_id
      AND REP_PERIOD_ID = p_src_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_VPN_T '||v_count||' rows duplicated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ������� ITEM-� 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_dst_period_id
       AND I.BILL_ID       = p_dst_bill_id
       AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG
       AND EXISTS (
           SELECT * FROM BDR_CCAD_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.BILL_ID       = I.BILL_ID
              AND CB.ORDER_ID      = I.ORDER_ID
              AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
              AND CB.SERVICE_ID    = I.SERVICE_ID
              AND CB.SUBSERVICE_ID = I.SUBSERVICE_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

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
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               CB.CURRENCY_ID,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_T CB
         WHERE CB.REP_PERIOD_ID = p_dst_period_id
           AND CB.BILL_ID       = p_dst_bill_id
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        BDR.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,
        CASE
           WHEN BDR.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
           ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE
      FROM BDR
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ����������� ��������� �� item-�    
    MERGE INTO BDR_CCAD_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_T CB
           WHERE I.REP_PERIOD_ID  = p_dst_period_id
             AND I.BILL_ID        = p_dst_bill_id
             AND I.CHARGE_TYPE    = Pk00_Const.c_CHARGE_TYPE_USG
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.BILL_ID       = I.BILL_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_VOL_T->ITEM_ID '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- --------------------------------------------------------------------- --
    -- RT - ���������� ����������� (service_id = 104, subservice_id = 31)
    -- --------------------------------------------------------------------- --
    -- ������ ����� �������, ��� ������ �����
    INSERT INTO BDR_CCAD_T (BDR_ID,
                              BDR_TYPE_ID,
                              REP_PERIOD_ID,
                              BILL_ID,
                              ITEM_ID,
                              ORDER_ID    ,
                              SERVICE_ID,
                              ORDER_BODY_ID,
                              SUBSERVICE_ID ,
                              DATE_FROM    ,
                              DATE_TO    ,
                              RATEPLAN_ID ,
                              GR_1_BYTES   ,
                              GR_2_BYTES    ,
                              GR_31_BYTES    ,
                              GR_32_BYTES    ,
                              GR_33_BYTES    ,
                              GR_34_BYTES    ,
                              GR_1_PRICE    ,
                              GR_2_PRICE    ,
                              GR_31_PRICE    ,
                              GR_32_PRICE    ,
                              GR_33_PRICE    ,
                              GR_34_PRICE    ,
                              SPEED    ,
                              SPEED_UNIT,
                              AMOUNT    ,
                              CF    ,
                              BDR_STATUS_ID,
                              CURRENCY_ID  ,
                              TAX_INCL    ,
                              CREATE_DATE  ,
                              SPEED_DATE    ,
                              RATE_RULE_ID   ,
                              BDR_PARENT_ID   ,
                              NOTES)
    SELECT Pk02_Poid.Next_ccad_bdr_id BDR_ID, BDR_TYPE_ID, 
           p_dst_period_id REP_PERIOD_ID, p_dst_bill_id BILL_ID, NULL ITEM_ID, 
           ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, DATE_FROM, DATE_TO, 
           RATEPLAN_ID, GR_1_BYTES, GR_2_BYTES, GR_31_BYTES, GR_32_BYTES, GR_33_BYTES, 
           GR_34_BYTES, GR_1_PRICE, GR_2_PRICE, GR_31_PRICE, GR_32_PRICE, GR_33_PRICE, 
           GR_34_PRICE, SPEED, SPEED_UNIT, AMOUNT, CF, BDR_STATUS_ID, CURRENCY_ID, TAX_INCL, 
           SYSDATE CREATE_DATE, SPEED_DATE, RATE_RULE_ID, 
           BDR_ID BDR_PARENT_ID, 'double record' NOTES
     FROM BDR_CCAD_T CB
    WHERE BILL_ID = p_src_bill_id
      AND REP_PERIOD_ID = p_src_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_VPN_T '||v_count||' rows duplicated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ������� ITEM-� 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_dst_period_id
       AND I.BILL_ID       = p_dst_bill_id
       AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG
       AND EXISTS (
           SELECT * FROM BDR_CCAD_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.BILL_ID       = I.BILL_ID
              AND CB.ORDER_ID      = I.ORDER_ID
              AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
              AND CB.SERVICE_ID    = I.SERVICE_ID
              AND CB.SUBSERVICE_ID = I.SUBSERVICE_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

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
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               CB.CURRENCY_ID,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_T CB
         WHERE CB.REP_PERIOD_ID = p_dst_period_id
           AND CB.BILL_ID       = p_dst_bill_id
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        BDR.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,
        CASE
           WHEN BDR.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
           ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE
      FROM BDR
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ����������� ��������� �� item-�    
    MERGE INTO BDR_CCAD_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_T CB
           WHERE I.REP_PERIOD_ID  = p_dst_period_id
             AND I.BILL_ID        = p_dst_bill_id
             AND I.CHARGE_TYPE    = Pk00_Const.c_CHARGE_TYPE_USG
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.BILL_ID       = I.BILL_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_RT_T->ITEM_ID '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �-��� ��� �������� ������������������ ���������� (BDR-�) 
-- � ������ ����� �� ������ (���������� ������)
-- ����������: 0  - ������� ���������
--             -1 - ������ 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Move_BDR(p_Src_Bill_Id     IN INTEGER,
                  p_Src_Rep_Per_Id  IN INTEGER,
                  p_Dest_Bill_Id    IN INTEGER,
                  p_Dest_Rep_Per_Id IN INTEGER
                 ) RETURN INTEGER
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Move_BDR';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    RETURN PK111_RETRF_GUI.Move_BDR(
                  p_Src_Bill_Id,
                  p_Src_Rep_Per_Id,
                  p_Dest_Bill_Id,
                  p_Dest_Rep_Per_Id
                 );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = --
--  ����������� ����� �� ��������� ������
-- = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = --
-- ���� ��������� � ������ ���� �� �����-�� ������� ���� �� 
-- ��������� ������ �� ��� ���������, � ������ ����� 
-- ����� ���������� ���������� ����� � ��������� ����,
-- � �� ������ ���������� ��������� � ������� ����
-- ����������:
--   - ������������� - ID �����, 
--   - ��� ������ ���������� ����������
FUNCTION Open_rec_bill_for_old_period (  
               p_account_id    IN INTEGER, -- ID �������� �����
               p_period_id     IN INTEGER, -- ID �������� ���������� ������� YYYYMM
               p_old_period_id IN INTEGER  -- ID ������� ���������� ������� YYYYMM
            ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Open_rec_bill_for_old_period';
    v_bill_no     BILL_T.BILL_NO%TYPE;
    v_bill_date   DATE;
    v_bill_id     INTEGER;
    v_currency_id INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    
    -- ���� ������ �������� ���������� �������
    v_bill_date := Pk04_Period.Period_from(p_period_id);
    
    -- ������ �����
    SELECT A.CURRENCY_ID INTO v_currency_id
      FROM ACCOUNT_T A
     WHERE A.ACCOUNT_ID = p_account_id;
    
    -- �������� ����� ����� ��� ���������� �������
    v_bill_no := Pk07_Bill.Next_bill_no (
               p_account_id     => p_account_id,
               p_bill_period_id => p_old_period_id
           )||'O';
           
    -- ������ ���� ��� ���������� ������� � �������
    v_bill_id := Pk07_Bill.Open_rec_bill_for_old_period (
               p_account_id    => p_account_id,    -- ID �������� �����
               p_rep_period_id => p_period_id,     -- ID ���������� ������� YYYYMM
               p_bill_no       => v_bill_no,       -- ����� �����
               p_currency_id   => v_currency_id,   -- ID ������ �����
               p_bill_date     => v_bill_date      -- ���� ����� (������������ �������)
            );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    RETURN v_bill_id;
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ���������� �� ������ ( ����� ������ � ��� )
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ���������� �� ������, ���������, ������ ����������� ������� � ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Make_bill_for_old_period(  
               p_bill_id       IN INTEGER, -- ID �������� �����
               p_period_id     IN INTEGER, -- ID �������� ���������� ������� YYYYMM
               p_old_period_id IN INTEGER  -- ID ������� ���������� ������� YYYYMM
            )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Make_bill_for_old_period';
    v_task_id     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    
    -- ������ ���� � ������� �� ���������
    v_task_id := Pk33_Billing_Account.push_Bill(
                      p_bill_id        => p_bill_id,
                      p_bill_period_id => p_period_id,
                      p_data_period_id => p_old_period_id );

    -- ���������� ����������� ����� � ������� �� ����������� �����
    Pk36_Billing_Fixrate.Charge_fixrates(v_task_id);

    -- ������������ � ��������� ����������� �� ������� ��� �����-����
    Pk38_Billing_Downtime.Recharge_downtime_for_debet( 
                 p_dbt_bill_id   => p_bill_id,      -- id - ���������� �����, � ������� ������� ������
                 p_dbt_period_id => p_period_id,    -- ������ ���������� �����
                 p_crd_period_id => p_old_period_id -- ������ ����������� �����, ��� �������� ������������� ������
              );

    -- ������������ � ��������� ������ ��� �����-����
    Pk39_Billing_Discount.Recalc_discount_for_debet( 
                 p_dbt_bill_id   => p_bill_id,      -- id - ���������� �����, � ������� ������� ������
                 p_dbt_period_id => p_period_id,    -- ������ ���������� �����
                 p_crd_period_id => p_old_period_id -- ������ ����������� �����, ��� �������� ������������� ������
              );

    -- C����������� ���� 
    Pk30_Billing_Base.Rollback_bills(v_task_id );

    -- ����������� ������ �������� �����
    Pk30_Billing_Base.Refresh_balance(v_task_id);

    -- ����������� �������
    Pk30_Billing_Queue.Close_task(v_task_id);

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = --
--  ������ ��������� �� ��������� ������ � ��������� ����
-- = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = --
PROCEDURE Charge_abp_for_old_period(  
               p_order_id      IN INTEGER, -- ID �����, ��� �������� ����� ������������� ������
               p_bill_id       IN INTEGER, -- ID �������� �����
               p_period_id     IN INTEGER, -- ID �������� ���������� ������� YYYYMM
               p_old_period_id IN INTEGER  -- ID ������� ���������� ������� YYYYMM
            )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Charge_abp_for_old_period';
    v_task_id     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    
    -- ������ ���� � ������� �� ���������
    v_task_id := PK30_BILLING_QUEUE.Open_task;
    --    
    INSERT INTO BILLING_QUEUE_T (
           BILL_ID, ACCOUNT_ID, ORDER_ID,
           TASK_ID, REP_PERIOD_ID, DATA_PERIOD_ID
       )
    SELECT B.BILL_ID, B.ACCOUNT_ID, p_order_id,
           v_task_id, B.REP_PERIOD_ID, p_old_period_id
      FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_period_id
       AND B.BILL_ID       = p_bill_id
       AND B.BILL_STATUS   = Pk00_Const.c_BILL_STATE_OPEN
    ;

    -- ���������� ����������� ����� � ������� �� ����������� �����
    Pk36_Billing_Fixrate.Charge_fixrates(v_task_id);

    -- ����������� �������
    Pk30_Billing_Queue.Close_task(v_task_id);

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------------- --
-- ������� ������������� �����
-- ----------------------------------------------------------------------- --
PROCEDURE Adjust_history (
               p_recordset OUT t_refc,
               p_bill_id    IN INTEGER, -- id ����������������� �����
               p_period_id  IN INTEGER  -- ������ ����������������� �����
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Adjust_history';
    v_retcode    INTEGER;  
BEGIN
   OPEN p_recordset FOR
    SELECT B.* 
      FROM BILL_T B
    CONNECT BY NOCYCLE PRIOR B.BILL_ID = B.PREV_BILL_ID  AND PRIOR B.REP_PERIOD_ID = B.PREV_BILL_PERIOD_ID
        START WITH B.REP_PERIOD_ID = p_period_id 
               AND B.BILL_ID       = p_bill_id
   ;
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ----------------------------------------------------------------------- --
-- ������� ������������� ����� � �������� ������� (�� �����/������ ���� � �����)
-- ----------------------------------------------------------------------- --
PROCEDURE Adjust_history_desc (
               p_recordset    OUT t_refc,
               p_ads_bill_id   IN INTEGER, -- id ����������������� �����
               p_ads_period_id IN INTEGER  -- ������ ����������������� �����
           )

IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Adjust_history_desc';
    v_retcode    INTEGER;  
BEGIN
   OPEN p_recordset FOR
      SELECT B.* 
        FROM BILL_T B
      CONNECT BY NOCYCLE PRIOR B.PREV_BILL_ID = B.BILL_ID AND PRIOR B.PREV_BILL_PERIOD_ID = B.REP_PERIOD_ID
        START WITH B.REP_PERIOD_ID = p_ads_period_id 
               AND B.BILL_ID       = p_ads_bill_id
   ;
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- ----------------------------------------------------------------------- --
-- ������ ����� ������� ������������� ����� (�� Portal 6.5)
-- ----------------------------------------------------------------------- --
PROCEDURE Err_code_list (
               p_recordset OUT t_refc
           )

IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Err_code_list';
    v_retcode    INTEGER;  
BEGIN
   OPEN p_recordset FOR
    SELECT D.KEY_ID, D.KEY, CONNECT_BY_ROOT(NAME) CHAPTER, D.NAME   
      FROM DICTIONARY_T D
     WHERE CONNECT_BY_ISLEAF = 1
     CONNECT BY PRIOR D.KEY_ID = D.PARENT_ID 
     START WITH D.PARENT_ID = 28
   ;
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


END PK07_BILL_ADJUST;
/
