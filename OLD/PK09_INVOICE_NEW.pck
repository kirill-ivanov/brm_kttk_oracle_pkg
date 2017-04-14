CREATE OR REPLACE PACKAGE PK09_INVOICE_NEW
IS
    --
    -- ����� ��� ������ � �������� "������� �����-�������", �������:
    -- invoice_item_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK09_INVOICE';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �������/��������� ������
    -- 
    -- ���������� ����� ������ �� ��������� ����� (��� ������)
    FUNCTION Calc_tax(
                  p_taxfree_total IN NUMBER,  -- ����� ��� ������
                  p_tax_rate      IN NUMBER   -- ������ ������ � ���������
               ) RETURN NUMBER DETERMINISTIC;
               
    -- ���������� ����� ������ �� ��������� ����� (� �������)
    FUNCTION Allocate_tax(
                  p_total      IN NUMBER,     -- ����� � �������
                  p_tax_rate   IN NUMBER      -- ������ ������ � ���������
               ) RETURN NUMBER DETERMINISTIC;
        
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ��� ������� �����-������� ��� ������
    --   - ��� ������ ���������� ����������
    FUNCTION Get_item_name (
                  p_service_id  IN INTEGER,
                  p_account_id  IN INTEGER,
                  p_contract_id IN INTEGER,
                  p_customer_id IN INTEGER
               ) RETURN VARCHAR2;
    
    -- ��������� ������� �����-������� 
        --   - ������������� - ID invoice_item, 
        --   - ��� ������ ���������� ����������
    FUNCTION Calc_inv_item (
                   p_bill_id       IN INTEGER,   -- ID ������� �����
                   p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
                   p_service_id    IN INTEGER,   -- ID ������
                   p_inv_item_no   IN INTEGER,   -- ����� ������ � ����� �������
                   p_inv_item_name IN VARCHAR2,  -- ��� ������ � ����� �������
                   p_vat           IN NUMBER,    -- ������ ������ � ���������
                   p_date_from     IN DATE       -- ���� ������ �������� ������, 
               ) RETURN INTEGER;                 -- � �-� ����� ������� ������ ��������� 
                                                 -- � ������ �������� (�������������)
    
    -- ��������� ����-�������
    --   - �������� ���������� �� �������, ������� ����� 
    --   - ��� ������ ���������� ����������
    FUNCTION Calc_invoice (
                   p_bill_id       IN INTEGER,   -- ID ������� �����
                   p_rep_period_id IN INTEGER    -- ID ��������� ������� �����
               ) RETURN NUMBER;
               
    -- ����� ��� ������� ���������� �����-�������
    --   - ������������� - ���-�� ��������� �������
    --   - ��� ������ ���������� ����������
    FUNCTION Invoice_items_list( 
                   p_recordset OUT t_refc, 
                   p_bill_id       IN INTEGER,   -- ID �����
                   p_rep_period_id IN INTEGER    -- ID ��������� ������� �����
               ) RETURN INTEGER;
    
    -- ������� ��� ������� ���������� �����-�������
    --   - ������������� - ���-�� ��������� �������
    --   - ��� ������ ���������� ����������
    FUNCTION Delete_invoice_items (
                   p_bill_id       IN INTEGER,   -- ID �����
                   p_rep_period_id IN INTEGER    -- ID ��������� ������� �����
               ) RETURN INTEGER;
    
END PK09_INVOICE_NEW;
/
CREATE OR REPLACE PACKAGE BODY PK09_INVOICE_NEW
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ��� ������� �����-������� ��� ������
--   - ��� ������ ���������� ����������
FUNCTION Get_item_name (
              p_service_id  IN INTEGER,
              p_account_id  IN INTEGER,
              p_contract_id IN INTEGER,
              p_customer_id IN INTEGER
           ) RETURN VARCHAR2
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Get_item_name';
    v_item_name     INVOICE_ITEM_T.INV_ITEM_NAME%TYPE;
BEGIN
    SELECT SRV_NAME
      INTO v_item_name
      FROM (
            SELECT NVL(SA.SRV_NAME, S.SERVICE) SRV_NAME,
                   CASE
                     WHEN SA.ACCOUNT_ID  = p_account_id  THEN 1
                     WHEN SA.CONTRACT_ID = p_contract_id THEN 2
                     WHEN SA.CUSTOMER_ID = p_customer_id THEN 3
                     ELSE 0
                   END 
              FROM SERVICE_T S, SERVICE_ALIAS_T SA
             WHERE S.SERVICE_ID   = p_service_id
               AND S.SERVICE_ID   = SA.SERVICE_ID(+)
               ORDER BY 2
           )
     WHERE ROWNUM = 1;
     RETURN v_item_name;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� �������/��������� ������
-- 
-- ���������� ����� ������ �� ��������� ����� (��� ������)
FUNCTION Calc_tax(
              p_taxfree_total IN NUMBER,  -- ����� ��� ������
              p_tax_rate      IN NUMBER   -- ������ ������ � ���������
           ) RETURN NUMBER DETERMINISTIC
IS
BEGIN    
     RETURN  ROUND(p_taxfree_total * p_tax_rate / 100, 2);
END;

-- ���������� ����� ������ �� ��������� ����� (� �������)
FUNCTION Allocate_tax(
              p_total      IN NUMBER,     -- ����� � �������
              p_tax_rate   IN NUMBER      -- ������ ������ � ���������
           ) RETURN NUMBER DETERMINISTIC
IS
BEGIN    
     RETURN  p_total - ROUND(p_total /(1 + p_tax_rate / 100),2);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������� �����-������� 
    --   - ������������� - ID invoice_item, 
    --   - ��� ������ ���������� ����������
FUNCTION Calc_inv_item (
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
               p_service_id    IN INTEGER,   -- ID ������
               p_inv_item_no   IN INTEGER,   -- ����� ������ � ����� �������
               p_inv_item_name IN VARCHAR2,  -- ��� ������ � ����� �������
               p_vat           IN NUMBER,    -- ������ ������ � ���������
               p_date_from     IN DATE       -- ���� ������ �������� ������, 
           ) RETURN INTEGER                  -- � �-� ����� ������� ������ ��������� 
IS                                           -- � ������ �������� (�������������)
    v_prcName       CONSTANT VARCHAR2(30) := 'Calc_inv_item';
    v_date_from     DATE;
    v_date_to       DATE;
    v_inv_item_id   INTEGER;
    v_count         INTEGER;
    --
BEGIN
    v_date_from := TRUNC(p_date_from,'mm');
    v_date_to   := ADD_MONTHS(v_date_from,1)-1/86400;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� id ������ �����-������� �� id ������ �����
    v_inv_item_id := PK02_POID.Next_invoice_item_id;
        
    -- ��������� ������ �����-�������
    INSERT INTO INVOICE_ITEM_T (
       BILL_ID, REP_PERIOD_ID,
       INV_ITEM_ID, INV_ITEM_NO, SERVICE_ID, INV_ITEM_NAME, 
       VAT,         -- ������ ��� � ���������
       TOTAL,       -- ����� ���������� � �������
       GROSS,       -- ����� ���������� ��� ������
       TAX,         -- ����� ������
       DATE_FROM, DATE_TO
    )
    SELECT 
         --
         p_bill_id, p_rep_period_id, v_inv_item_id, p_inv_item_no, p_service_id, 
         p_inv_item_name, p_vat,
         -- ������ ����� ���������� � �������� -------------
         CASE
           WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN -- ����� ������� � ����������� �����
             AMOUNT
           WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_NOT_INCL THEN -- ����� �� �������
             AMOUNT + PK09_INVOICE.Calc_tax(AMOUNT, p_vat)
           ELSE -- ���� �� ����������
             NULL
         END TOTAL, 
         -- ����� ���������� ��� ������� -------------------
         CASE
           WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN -- ����� ������� � ����������� �����
             AMOUNT - PK09_INVOICE.Allocate_tax(AMOUNT, p_vat)
           ELSE -- ����� �� �������
             AMOUNT
         END GROSS, 
         -- ������ ����� ������ �� ����������� ���������� --
         CASE
           WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN -- ����� ������� � ����������� �����
             PK09_INVOICE.Allocate_tax(AMOUNT, p_vat)
           WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_NOT_INCL THEN -- ����� �� �������
             PK09_INVOICE.Calc_tax(AMOUNT, p_vat)
           ELSE -- ����� �� ���������
             NULL
         END TAX,
         -- ���������� � ������������ ���� �������� ������ --
         DATE_FROM, DATE_TO
      FROM (
        SELECT TAX_INCL, 
           SUM(ITEM_TOTAL) AMOUNT, 
           MIN(
             CASE 
               WHEN v_date_from < O.DATE_FROM THEN TRUNC(O.DATE_FROM)
               ELSE v_date_from  
             END
           ) DATE_FROM,
           MAX(
             CASE 
               WHEN O.DATE_TO < v_date_to THEN TRUNC(O.DATE_TO)
               ELSE v_date_to  
             END
           ) DATE_TO
        FROM ITEM_T I, ORDER_T O
         WHERE I.BILL_ID       = p_bill_id
           AND I.REP_PERIOD_ID = p_rep_period_id
           AND I.SERVICE_ID    = p_service_id
           AND I.ORDER_ID      = O.ORDER_ID
        GROUP BY TAX_INCL
    );
    v_count := SQL%ROWCOUNT;
    IF v_count = 1 THEN
        -- ����������� ������� ��������� � ����-�������, ������� ����� (item)
        UPDATE ITEM_T 
           SET INV_ITEM_ID = v_inv_item_id
         WHERE BILL_ID = p_bill_id
           AND REP_PERIOD_ID = p_rep_period_id
           AND SERVICE_ID = p_service_id
           AND DATE_FROM BETWEEN v_date_from AND v_date_to;
    END IF;   
    -- ���������� ID ��������� ������� �����
    RETURN v_inv_item_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Bill_id='||p_bill_id
                                  ||', service_id='||p_service_id, c_PkgName||'.'||v_prcName );
END;

/*
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������� �����-�������
    --   - ������������� - ����� ��������� ������ � �����-������� 
    --     ��� ����� ������, ����� ���� ����������� ����� ��� ���� ������ � �-�,
    --     �������� ����� � �-� ������ ������ �� ������ ��������
    --   - ��� ������ ���������� ����������
FUNCTION Calc_inv_items (
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
               p_service_id    IN INTEGER,   -- ID ������
               p_inv_item_no   IN INTEGER,   -- ����� ������ � ����� �������
               p_inv_item_name IN VARCHAR2,  -- ��� ������ � ����� �������
               p_vat           IN NUMBER,    -- ������ ������ � ���������
               p_account_type  IN ACCOUNT_T.ACCOUNT_TYPE%TYPE -- ��� �/�
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Calc_inv_item';
    v_item_total    NUMBER;
    v_adjusted      NUMBER;
    v_date_from     DATE;
    v_date_to       DATE;    
    v_inv_item_id   INTEGER;
    v_gross         NUMBER;               -- ����� ��� �������
    v_tax           NUMBER;               -- ���
    v_total         NUMBER;               -- ����� � �������
    --
BEGIN
  
    FOR r_item IN (
        SELECT TRUNC(DATE_FROM, 'mm') ITEM_MONTH, TAX_INCL, 
               SUM(ITEM_TOTAL+ADJUSTED) AMOUNT, 
               MIN(DATE_FROM) DATE_FROM, MAX(DATE_TO) DATE_TO
        FROM ITEM_T
         WHERE BILL_ID = p_bill_id
           AND REP_PERIOD_ID = p_rep_period_id
           AND SERVICE_ID = p_service_id
        GROUP BY TRUNC(DATE_FROM, 'mm'), TAX_INCL
    )
    LOOP
        -- ������ ����� ���������� � �������� -------------
        IF r_item.tax_incl THEN
            v_total := r_item.amount; 
            v_tax   := Allocate_tax(v_total, p_vat);
            v_gross := v_total - v_tax;
        ELSE -- ����� ���������� ��� ������� --------------
            v_gross := r_item.amount;
            v_tax   := Calc_tax(v_total, p_vat);
            v_total := v_gross + v_tax;
        END IF;
        v_date_from := r_item.date_from; 
        v_date_to   := r_item.date_to;
        -- - - - - - - - - - - - - - - - - - - - - - - - --
        -- ��������� id ������ �����-������� �� id ������ �����
        v_inv_item_id := PK02_POID.Next_invoice_item_id;
        
        -- ��������� ������ �����-�������
        INSERT INTO INVOICE_ITEM_T (
           BILL_ID, REP_PERIOD_ID,
           INV_ITEM_ID, INV_ITEM_NO, SERVICE_ID,
           VAT,         -- ������ ��� � ���������
           TAX,         -- ����� ������
           GROSS,       -- ����� ���������� ��� ������
           TOTAL,       -- ����� ���������� � �������
           INV_ITEM_NAME, DATE_FROM, DATE_TO
        )VALUES(
           p_bill_id, p_rep_period_id, v_inv_item_id, p_inv_item_no, p_service_id,
           p_vat, v_tax, v_gross, v_total, 
           p_inv_item_name, v_date_from, v_date_to
        );
    
        -- ����������� ������� ��������� � ����-�������, ������� ����� (item)
        UPDATE ITEM_T 
           SET INV_ITEM_ID = v_inv_item_id
         WHERE BILL_ID = p_bill_id
           AND REP_PERIOD_ID = p_rep_period_id
           AND SERVICE_ID = p_service_id;
        
        
    END LOOP;





    -- ��������� ���������� �� ��������� ������ c ������ ��������� ���
    SELECT 
           -- ������ ����� ���������� � �������� -------------
           CASE
             WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN -- ����� ������� � ����������� �����
               SUM(ITEM_TOTAL+ADJUSTED)
             ELSE -- ����� �� �������
               SUM(ITEM_TOTAL+ADJUSTED) + PK09_INVOICE.Calc_tax(SUM(ITEM_TOTAL+ADJUSTED), p_vat)
           END TOTAL, 
           -- ����� ���������� ��� ������� -------------------
           CASE
             WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN -- ����� ������� � ����������� �����
               SUM(ITEM_TOTAL+ADJUSTED) - PK09_INVOICE.Allocate_tax(SUM(ITEM_TOTAL+ADJUSTED), p_vat)
             ELSE -- ����� �� �������
               SUM(ITEM_TOTAL+ADJUSTED)
           END GROSS, 
           -- ������ ����� ������ �� ����������� ���������� --
           CASE
             WHEN TAX_INCL = PK00_CONST.c_RATEPLAN_TAX_INCL THEN -- ����� ������� � ����������� �����
               PK09_INVOICE.Allocate_tax(SUM(ITEM_TOTAL+ADJUSTED), p_vat)
             ELSE -- ����� �� �������
               PK09_INVOICE.Calc_tax(SUM(ITEM_TOTAL+ADJUSTED), p_vat);
           END TAX,
           -- ���������� � ������������ ���� �������� ������ --
           MIN(DATE_FROM), MAX(DATE_TO)
      INTO v_total, v_gross, v_tax, v_date_from, v_date_to
    FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND SERVICE_ID = p_service_id;

    
    
    
    
    
    
    SUM(ITEM_TOTAL), SUM(ADJUSTED),
           MIN(DATE_FROM), MAX(DATE_TO)
      INTO v_item_total, v_adjusted, v_date_from, v_date_to
      FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND SERVICE_ID = p_service_id;
    
    --
    SELECT SUM(ITEM_TOTAL), SUM(ADJUSTED),
           MIN(DATE_FROM), MAX(DATE_TO)
      INTO v_item_total, v_adjusted, v_date_from, v_date_to
      FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND SERVICE_ID = p_service_id;
       
    -- ---------------------------------------------------------------------------------
    -- ���-�� ����� �������� � ITEM_T ��� ������� ����� ����� ������� � �������� ��� ���
    -- ������� ������:
    -- - ��� ������� ��� ���. ��� - c �������� (account_t.business_type = 'P')
    -- - ��� ������� ��� ��. ���  - ��� ������� (account_t.business_type = 'J')
    -- ---------------------------------------------------------------------------------
    IF p_account_type = PK00_CONST.c_ACC_TYPE_P THEN
       -- � ������� ����� � ��������
       v_total := v_item_total + v_adjusted;
       v_tax   := Allocate_tax(v_total, p_vat);
       v_gross := v_total - v_tax;
    ELSE -- ��� ����������� ���:
       -- � ������� - ����� ��� �������
       v_gross := v_item_total + v_adjusted;
       v_tax   := Calc_tax(v_total, p_vat);
       v_total := v_gross + v_tax;
    END IF;
    
    -- ��������� id ������ �����-������� �� id ������ �����
    v_inv_item_id := PK02_POID.Next_invoice_item_id;
    
    -- ��������� ������ �����-�������
    INSERT INTO INVOICE_ITEM_T (
       BILL_ID, REP_PERIOD_ID,
       INV_ITEM_ID, INV_ITEM_NO, SERVICE_ID,
       VAT,         -- ������ ��� � ���������
       TAX,         -- ����� ������
       GROSS,       -- ����� ���������� ��� ������
       TOTAL,       -- ����� ���������� � �������
       INV_ITEM_NAME, DATE_FROM, DATE_TO
    )VALUES(
       p_bill_id, p_rep_period_id, v_inv_item_id, p_inv_item_no, p_service_id,
       p_vat, v_tax, v_gross, v_total, 
       p_inv_item_name, v_date_from, v_date_to
    );
    
    -- ����������� ������� ��������� � ����-�������, ������� ����� (item)
    UPDATE ITEM_T 
       SET INV_ITEM_ID = v_inv_item_id
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND SERVICE_ID = p_service_id;
       
    -- ���������� ID ��������� ������� �����
    RETURN v_inv_item_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ����-�������
--   - ������������� - ���-�� ����� � �����-�������
--   - ��� ������ ���������� ����������
FUNCTION Calc_invoice (
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
               p_account_type  IN ACCOUNT_T.ACCOUNT_TYPE%TYPE
           ) RETURN NUMBER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Calc_invoice';
    v_inv_item_id   INTEGER;
    v_count         INTEGER := 0; -- � ������� ������ ������ �� ���������� ������
    -- - - - - - - - - - -- 
    v_account_id    INTEGER;
    v_contract_id   INTEGER;
    v_customer_id   INTEGER;
    v_vat           NUMBER;
    v_inv_item_name INVOICE_ITEM_T.INV_ITEM_NAME%TYPE;
BEGIN
    -- �������� ��������� ������ �/� ����������� � ��������� ����������� �������
    SELECT AP.ACCOUNT_ID, AP.CONTRACT_ID, AP.CUSTOMER_ID, AP.VAT
      INTO v_account_id, v_contract_id, v_customer_id, v_vat
      FROM ACCOUNT_PROFILE_T AP, BILL_T B
     WHERE AP.ACCOUNT_ID   = B.ACCOUNT_ID
       AND B.BILL_ID       = p_bill_id
       AND B.REP_PERIOD_ID = p_rep_period_id
       AND AP.DATE_FROM   <= B.BILL_DATE
       AND (AP.DATE_TO IS NULL OR B.BILL_DATE <= AP.DATE_TO);
    
    -- ��������� ������ ����� ������� ��� ���� ����� �����    
    FOR i IN (
        SELECT DISTINCT SERVICE_ID
          FROM ITEM_T
         WHERE BILL_ID = p_bill_id
           AND REP_PERIOD_ID = p_rep_period_id
           AND SERVICE_ID IS NOT NULL
      )
    LOOP
        -- ��������� � �������
        v_count := v_count + 1;
        -- �������� ��� ������� ����� �������
        v_inv_item_name := Get_item_name (
                  p_service_id  => i.service_id,
                  p_account_id  => v_account_id,
                  p_contract_id => v_contract_id,
                  p_customer_id => v_customer_id
               );
        -- ID ������� ����� �������
        v_inv_item_id := Calc_inv_item (
                  p_bill_id       => p_bill_id,      -- ID ������� �����
                  p_rep_period_id => p_rep_period_id,-- ID ��������� ������� �����
                  p_service_id    => i.service_id,   -- ID ������
                  p_inv_item_no   => v_count,        -- ����� ������ � ����� �������
                  p_inv_item_name => v_inv_item_name,-- ��� ������ � ����� �������
                  p_vat           => v_vat,          -- ������ ������ � ���������
                  p_account_type  => p_account_type  -- ��� �/�
               );
    END LOOP;
    RETURN v_count;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ����-�������
--   - ������������� - ���-�� ����� � �����-�������
--   - ��� ������ ���������� ����������
FUNCTION Calc_invoice (
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER    -- ID ��������� ������� �����
           ) RETURN NUMBER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Calc_invoice';
    v_inv_item_id   INTEGER;
    v_count         INTEGER := 0; -- � ������� ������ ������ �� ���������� ������
    v_period_from   DATE;
    v_period_to     DATE;
    -- - - - - - - - - - -- 
    v_account_id    INTEGER;
    v_contract_id   INTEGER;
    v_customer_id   INTEGER;
    v_vat           NUMBER;
    v_inv_item_name INVOICE_ITEM_T.INV_ITEM_NAME%TYPE;
BEGIN
    --
    v_period_from := Pk04_Period.Period_from(p_rep_period_id);
    v_period_to   := Pk04_Period.Period_to(p_rep_period_id);
    
    -- �������� ��������� ������ �/� ����������� � ��������� ����������� �������
    -- ���������� ��������� ������ ������� � ����������� �������
    SELECT ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, VAT
      INTO v_account_id, v_contract_id, v_customer_id, v_vat
    FROM (
       SELECT AP.ACCOUNT_ID, AP.CONTRACT_ID, AP.CUSTOMER_ID, AP.VAT, AP.DATE_FROM,
              MAX(AP.DATE_FROM) OVER (PARTITION BY AP.ACCOUNT_ID) MAX_DATE_FROM
        FROM BILL_T B, ACCOUNT_PROFILE_T AP
       WHERE AP.ACCOUNT_ID   = B.ACCOUNT_ID
         AND B.BILL_ID       = p_bill_id
         AND B.REP_PERIOD_ID = p_rep_period_id
         AND AP.DATE_FROM   <= v_period_to
         AND (AP.DATE_TO IS NULL OR v_period_from <= AP.DATE_TO )
    ) WHERE DATE_FROM = MAX_DATE_FROM;
    
    -- ��������� ������ ����� ������� ��� ���� ����� �����    
    FOR i IN (
        SELECT SERVICE_ID, v_period_from DATE_FROM
          FROM ITEM_T
         WHERE BILL_ID = p_bill_id
           AND REP_PERIOD_ID = p_rep_period_id
           AND SERVICE_ID IS NOT NULL
           GROUP BY SERVICE_ID
           ORDER BY 1
      )
    LOOP
        -- ��������� � �������
        v_count := v_count + 1;
        -- �������� ��� ������� ����� �������
        v_inv_item_name := Get_item_name (
                  p_service_id  => i.service_id,
                  p_account_id  => v_account_id,
                  p_contract_id => v_contract_id,
                  p_customer_id => v_customer_id
               );
        -- ID ������� ����� �������
        v_inv_item_id := Calc_inv_item (
                  p_bill_id       => p_bill_id,      -- ID ������� �����
                  p_rep_period_id => p_rep_period_id,-- ID ��������� ������� �����
                  p_service_id    => i.service_id,   -- ID ������
                  p_inv_item_no   => v_count,        -- ����� ������ � ����� �������
                  p_inv_item_name => v_inv_item_name,-- ��� ������ � ����� �������
                  p_vat           => v_vat,          -- ������ ������ � ���������
                  p_date_from     => i.date_from     -- ��� �/�
               );
    END LOOP;
    RETURN v_count;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Bill_id='||p_bill_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ��� ������� ���������� �����-�������
--   - ������������� - ���-�� ��������� �������
--   - ��� ������ ���������� ����������
FUNCTION Invoice_items_list( 
               p_recordset OUT t_refc, 
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER    -- ID ��������� ������� �����
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Invoice_items_list';
    v_retcode    INTEGER;
BEGIN
    -- ��������� ���-�� �������
    SELECT COUNT(*) INTO v_retcode
      FROM INVOICE_ITEM_T
     WHERE BILL_ID = p_bill_id;
    -- ���������� ������
    OPEN p_recordset FOR
         SELECT BILL_ID, REP_PERIOD_ID,
                INV_ITEM_ID, INV_ITEM_NO, SERVICE_ID, 
                VAT, TAX, GROSS, TOTAL,
                INV_ITEM_NAME, DATE_FROM, DATE_TO
           FROM INVOICE_ITEM_T
          WHERE BILL_ID = p_bill_id
            AND REP_PERIOD_ID = p_rep_period_id
          ORDER BY INV_ITEM_NO;
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ������� ��� ������� ���������� �����-�������
--   - ������������� - ���-�� ��������� �������
--   - ��� ������ ���������� ����������
FUNCTION Delete_invoice_items (
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER    -- ID ��������� ������� �����
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_invoice_items';
BEGIN
    -- ������� ������� ��������� ������� ����� � ����-�������
    -- ���� ���� ������ � ITEM_T, �� ��� ��������
    -- ��������� constraint ITEM_T_INVOICE_ITEM_T_FK
    UPDATE ITEM_T SET INV_ITEM_ID = NULL
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- ������� ��� ������� ���������� �����-�������,
    DELETE 
      FROM INVOICE_ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- ���������� ���-�� ��������� �������
    RETURN SQL%ROWCOUNT;
EXCEPTION
    WHEN OTHERS THEN
        RETURN(-Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName));
END;


END PK09_INVOICE_NEW;
/
