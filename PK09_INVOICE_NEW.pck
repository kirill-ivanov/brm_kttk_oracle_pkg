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
    
    -- ������ ����� ���������� � �������� -------------
    FUNCTION Calc_total(
                  p_amount   IN NUMBER,     -- ����� � �������
                  p_tax_incl IN CHAR,       -- ����� ������� � ����� ��� �������� = 'Y'/'N'
                  p_vat      IN NUMBER      -- ������ ���
               ) RETURN NUMBER DETERMINISTIC;

    -- ����� ���������� ��� ������� -------------------
    FUNCTION Calc_gross(
                  p_amount   IN NUMBER,     -- ����� � �������
                  p_tax_incl IN CHAR,       -- ����� ������� � ����� ��� �������� = 'Y'/'N'
                  p_vat      IN NUMBER      -- ������ ���
               ) RETURN NUMBER DETERMINISTIC;

    -- ������ ����� ������ �� ����������� ���������� --
    FUNCTION Calc_tax(
                  p_amount   IN NUMBER,     -- ����� � �������
                  p_tax_incl IN CHAR,       -- ����� ������� � ����� ��� �������� = 'Y'/'N'
                  p_vat      IN NUMBER      -- ������ ���
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

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������� �����-�������, ��� ����������� ����� 'B'
    -- item-� �������������� �� invoice_item-� �� �������
    -- �� ������� 7701 (INV_STD):
    --   ������ invoice_item ������������� ���/���� ��������� itemo-��
    -- ����������:
    --   - ������������� - ID invoice_item, 
    --   - ��� ������ ���������� ����������
    PROCEDURE Calc_inv_item_std (
                   p_inv_item_no   IN OUT INTEGER, -- ����� ������ � ����� �������
                   p_bill_id       IN INTEGER,   -- ID ������� �����
                   p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
                   p_service_id    IN INTEGER,   -- ID ������
                   p_inv_item_name IN VARCHAR2,  -- ��� ������ � ����� �������
                   p_vat           IN NUMBER     -- ������ ������ � ���������
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������� �����-�������, ��� �����,
    -- �� ��������:
    -- 7702 (RULE_EXT) - ������ ������ ������������� ������� �����
    -- 7703 (RULE_PER) - ����������� ����� �������� �� ����������� ��������
    -- ���� p_date_from �� ����� �� ��������� ������� 7702:
    --    item-� ����� �� ������, ��������� � ������ �������� �������������� 
    --    �� invoice_item, ������������ �������
    -- ���� p_date_from ����� �� ��������� ������� 7703:
    --    item-� ����� �� ��������� ������ �������������� �� invoice_item ����� �������
    -- ����������:
        --   - ������������� - ID invoice_item, 
        --   - ��� ������ ���������� ����������
    PROCEDURE Calc_inv_item_ext (
                   p_inv_item_no   IN OUT INTEGER, -- ����� ������ � ����� �������
                   p_bill_id       IN INTEGER,     -- ID ������� �����
                   p_rep_period_id IN INTEGER,     -- ID ��������� ������� �����
                   p_service_id    IN INTEGER,     -- ID ������
                   p_inv_item_name IN VARCHAR2,    -- ��� ������ � ����� �������
                   p_vat           IN NUMBER,      -- ������ ������ � ���������
                   p_date_from     IN DATE DEFAULT NULL  -- ���� ������ �������� ������, 
               );  -- � �-� ����� ������� ������ ��������� � ������ �������� (�������������)

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������� �����-������� ��� ����������� �����, ���� 'B',
    -- �������: 7704 - ����������� ����� �� ����������� ������. ��� ��������� �������� �������������
    -- item-� �� ����� ������ ������������ �� ����������� ������ �������� �������,
    -- �� �������� �� ���� �������� ������
        --   - ������������� - ID invoice_item, 
        --   - ��� ������ ���������� ����������
    PROCEDURE Calc_inv_item_subsrv_std (
                   p_inv_item_no   IN OUT INTEGER, -- ����� ������ � ����� �������
                   p_bill_id       IN INTEGER,     -- ID ������� �����
                   p_rep_period_id IN INTEGER,     -- ID ��������� ������� �����
                   p_service_id    IN INTEGER,     -- ID ������
                   p_vat           IN NUMBER       -- ������ ������ � ���������
               );
    
    -- ������ ����������� ����������� �� ����������� �������� (��� ���������)
    PROCEDURE Calc_inv_item_subsrv_ext (
                   p_inv_item_no   IN OUT INTEGER, -- ����� ������ � ����� �������
                   p_bill_id       IN INTEGER,     -- ID ������� �����
                   p_rep_period_id IN INTEGER,     -- ID ��������� ������� �����
                   p_service_id    IN INTEGER,     -- ID ������
                   p_vat           IN NUMBER,     -- ������ ������ � ���������
                   p_date_from     IN DATE DEFAULT NULL  -- ���� ������ �������� ������,     
               );   -- � �-� ����� ������� ������ ��������� � ������ �������� (�������������)
    
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
                   p_recordset    OUT t_refc, 
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
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������ ������ ������������ ����� �����-������
    PROCEDURE Invoice_rules_list( 
                   p_recordset IN OUT SYS_REFCURSOR
               );
  
    -- �������������� ������ ����-�������
    PROCEDURE Edit_invoice_item_name ( 
              p_bill_id      IN    INTEGER,
              p_period_id    IN    INTEGER,
              p_invoice_id   IN    INTEGER,
              p_name         IN    VARCHAR2
    );
      
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
    WITH SA AS (
        SELECT SERVICE_ID, SRV_NAME, IDX
          FROM (
            SELECT CASE
                     WHEN SA.ACCOUNT_ID  IS NOT NULL THEN 1
                     WHEN SA.CONTRACT_ID IS NOT NULL THEN 2
                     WHEN SA.CUSTOMER_ID IS NOT NULL THEN 3
                     ELSE 0
                   END IDX,
                   SA.SERVICE_ID, SA.SRV_NAME 
              FROM SERVICE_ALIAS_T SA
             WHERE SA.SERVICE_ID    = p_service_id
               AND ( SA.ACCOUNT_ID  = p_account_id  OR
                     SA.CONTRACT_ID = p_contract_id OR
                     SA.CUSTOMER_ID = p_customer_id
                   )
              ORDER BY 1 
        )WHERE ROWNUM = 1
    )
    SELECT NVL(SRV_NAME, SERVICE) SERVICE 
      INTO v_item_name
      FROM SA, SERVICE_T S
     WHERE S.SERVICE_ID = SA.SERVICE_ID(+)
       AND S.SERVICE_ID = p_service_id;
     RETURN v_item_name;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� �������/��������� ������
-- (�� ������ ������ �������� ��������� �������� � �������: ���.���)

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
              p_total      IN NUMBER,   -- ����� � �������
              p_tax_rate   IN NUMBER    -- ������ ������ � ���������
           ) RETURN NUMBER DETERMINISTIC
IS
    v_total NUMBER := ROUND(p_total, 2);-- xxxxx.xx (���.���)
BEGIN    
    RETURN  v_total - ROUND(v_total /(1 + p_tax_rate / 100), 2);
END;

-- ������ ����� ���������� � �������� -------------
FUNCTION Calc_total(
              p_amount   IN NUMBER,     -- ����� � �������
              p_tax_incl IN CHAR,       -- ����� ������� � ����� ��� �������� = 'Y'/'N'
              p_vat      IN NUMBER      -- ������ ���
           ) RETURN NUMBER DETERMINISTIC
IS
    v_amount NUMBER := ROUND(p_amount, 2); -- xxxxx.xx (���.���)
BEGIN    
    IF p_tax_incl = PK00_CONST.c_RATEPLAN_TAX_INCL THEN      -- ����� ������� � ����������� �����
        RETURN v_amount;
    ELSIF p_tax_incl = PK00_CONST.c_RATEPLAN_TAX_NOT_INCL THEN -- ����� �� �������
        RETURN v_amount + PK09_INVOICE.Calc_tax(v_amount, p_vat);
    ELSE -- ������ �� ������
        RETURN NULL;
    END IF;
END;

-- ����� ���������� ��� ������� -------------------
FUNCTION Calc_gross(
              p_amount   IN NUMBER,     -- ����� � �������
              p_tax_incl IN CHAR,       -- ����� ������� � ����� ��� �������� = 'Y'/'N'
              p_vat      IN NUMBER      -- ������ ���
           ) RETURN NUMBER DETERMINISTIC
IS
    v_amount NUMBER := ROUND(p_amount, 2); -- xxxxx.xx (���.���)
BEGIN
    IF p_tax_incl = PK00_CONST.c_RATEPLAN_TAX_INCL THEN      -- ����� ������� � ����������� �����
        RETURN v_amount - PK09_INVOICE.Allocate_tax(v_amount, p_vat);
    ELSIF p_tax_incl = PK00_CONST.c_RATEPLAN_TAX_NOT_INCL THEN -- ����� �� �������
        RETURN v_amount;
    ELSE -- ������ �� ������
        RETURN NULL;
    END IF;
END;

-- ������ ����� ������ �� ����������� ���������� --
FUNCTION Calc_tax(
              p_amount   IN NUMBER,     -- ����� � �������
              p_tax_incl IN CHAR,       -- ����� ������� � ����� ��� �������� = 'Y'/'N'
              p_vat      IN NUMBER      -- ������ ���
           ) RETURN NUMBER DETERMINISTIC
IS
BEGIN    
    IF p_tax_incl = PK00_CONST.c_RATEPLAN_TAX_INCL THEN      -- ����� ������� � ����������� �����
        RETURN PK09_INVOICE.Allocate_tax(p_amount, p_vat);
    ELSIF p_tax_incl = PK00_CONST.c_RATEPLAN_TAX_NOT_INCL THEN -- ����� �� �������
        RETURN PK09_INVOICE.Calc_tax(p_amount, p_vat);
    ELSE -- ������ �� ������
        RETURN NULL;
    END IF;
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������� �����-�������, ��� ����������� ����� 'B'
-- item-� �������������� �� invoice_item-� �� �������
-- �� ������� 7701 (INV_STD):
--   ������ invoice_item ������������� ���/���� ��������� itemo-��
-- ����������:
--   - ������������� - ID invoice_item, 
--   - ��� ������ ���������� ����������
PROCEDURE Calc_inv_item_std (
               p_inv_item_no   IN OUT INTEGER, -- ����� ������ � ����� �������
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
               p_service_id    IN INTEGER,   -- ID ������
               p_inv_item_name IN VARCHAR2,  -- ��� ������ � ����� �������
               p_vat           IN NUMBER     -- ������ ������ � ���������
           ) 
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Calc_inv_item_std';
    v_inv_item_id   INTEGER;
    v_count         INTEGER;
    --
BEGIN
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
         p_bill_id, p_rep_period_id, v_inv_item_id, p_inv_item_no, p_service_id, 
         p_inv_item_name, p_vat,
         ROUND(SUM(TOTAL),2), 
         ROUND(SUM(GROSS),2), 
         ROUND(SUM(TAX),2), 
         MIN(DATE_FROM), MAX(DATE_TO)
      FROM (
        SELECT  
           -- ������ ����� ���������� � �������� -------------
           Calc_total( AMOUNT, TAX_INCL, p_vat ) TOTAL, 
           -- ����� ���������� ��� ������� -------------------
           Calc_gross( AMOUNT, TAX_INCL, p_vat ) GROSS, 
           -- ������ ����� ������ �� ����������� ���������� --
           Calc_tax( AMOUNT, TAX_INCL, p_vat ) TAX,
           -- ���������� � ������������ ���� �������� ������ --
           DATE_FROM, DATE_TO
        FROM (
          SELECT TAX_INCL, 
             SUM(BILL_TOTAL) AMOUNT, 
             MIN(GREATEST(TRUNC(I.DATE_FROM,'mm'), TRUNC(O.DATE_FROM))) DATE_FROM,
             MAX(LEAST((ADD_MONTHS(TRUNC(I.DATE_TO,'mm'),1)-1/86400), O.DATE_TO)) DATE_TO
          FROM ITEM_T I, ORDER_T O
           WHERE I.BILL_ID       = p_bill_id
             AND I.REP_PERIOD_ID = p_rep_period_id
             AND I.ORDER_ID      = O.ORDER_ID
             AND I.SERVICE_ID    = p_service_id
          GROUP BY TAX_INCL
      )
    );
    v_count := SQL%ROWCOUNT;
    IF v_count = 1 THEN
        -- ����������� ������� ��������� � ����-�������, ������� ����� (item)
        UPDATE ITEM_T 
           SET INV_ITEM_ID   = v_inv_item_id
         WHERE BILL_ID       = p_bill_id
           AND REP_PERIOD_ID = p_rep_period_id
           AND SERVICE_ID    = p_service_id;
          
        -- ��������� � ��������� ������� � �����
        p_inv_item_no := p_inv_item_no + 1;
        --
    END IF;   
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Bill_id='||p_bill_id
                                  ||', service_id='||p_service_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������� �����-�������, ��� �����,
-- �� ��������:
-- 7702 (RULE_EXT) - ������ ������ ������������� ������� �����
-- 7703 (RULE_PER) - ����������� ����� �������� �� ����������� ��������
-- ���� p_date_from �� ����� �� ��������� ������� 7702:
--    item-� ����� �� ������, ��������� � ������ �������� �������������� 
--    �� invoice_item, ������������ �������
-- ���� p_date_from ����� �� ��������� ������� 7703:
--    item-� ����� �� ��������� ������ �������������� �� invoice_item ����� �������
-- ����������:
    --   - ������������� - ID invoice_item, 
    --   - ��� ������ ���������� ����������
PROCEDURE Calc_inv_item_ext (
               p_inv_item_no   IN OUT INTEGER, -- ����� ������ � ����� �������
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
               p_service_id    IN INTEGER,   -- ID ������
               p_inv_item_name IN VARCHAR2,  -- ��� ������ � ����� �������
               p_vat           IN NUMBER,    -- ������ ������ � ���������
               p_date_from     IN DATE DEFAULT NULL  -- ���� ������ �������� ������, 
           )                                 -- � �-� ����� ������� ������ ��������� 
IS                                           -- � ������ �������� (�������������)
    v_prcName       CONSTANT VARCHAR2(30) := 'Calc_inv_item_ext';
    v_date_from     DATE;
    v_date_to       DATE;
    v_inv_item_id   INTEGER;
    v_count         INTEGER;
    --
BEGIN
    IF p_date_from IS NULL THEN  
        -- ����������� ������
        v_date_from := Pk04_Period.Period_from(p_rep_period_id);
        v_date_to   := Pk04_Period.Period_to(p_rep_period_id);
    ELSE
        -- ��������� ������
        v_date_from := TRUNC(p_date_from,'mm');
        v_date_to   := ADD_MONTHS(v_date_from,1)-1/86400;
    END IF;  
    
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
         p_bill_id, p_rep_period_id, v_inv_item_id, p_inv_item_no, p_service_id, 
         p_inv_item_name, p_vat,
         ROUND(SUM(TOTAL),2), 
         ROUND(SUM(GROSS),2), 
         ROUND(SUM(TAX),2), 
         MIN(DATE_FROM), MAX(DATE_TO)
      FROM (
        SELECT  
           -- ������ ����� ���������� � �������� -------------
           Calc_total( AMOUNT, TAX_INCL, p_vat ) TOTAL, 
           -- ����� ���������� ��� ������� -------------------
           Calc_gross( AMOUNT, TAX_INCL, p_vat ) GROSS, 
           -- ������ ����� ������ �� ����������� ���������� --
           Calc_tax( AMOUNT, TAX_INCL, p_vat ) TAX,
           -- ���������� � ������������ ���� �������� ������ --
           DATE_FROM, DATE_TO
        FROM (
          SELECT TAX_INCL, 
             SUM(BILL_TOTAL) AMOUNT, 
             MIN(GREATEST(v_date_from, TRUNC(O.DATE_FROM))) DATE_FROM,
             MAX(LEAST(v_date_to, O.DATE_TO)) DATE_TO
          FROM ITEM_T I, ORDER_T O
           WHERE I.BILL_ID = p_bill_id
             AND I.REP_PERIOD_ID = p_rep_period_id
             AND I.ORDER_ID = O.ORDER_ID
             AND I.SERVICE_ID = p_service_id
             AND (p_date_from IS NULL OR TRUNC(I.DATE_FROM,'mm') = v_date_from)
          GROUP BY TAX_INCL
      )
    );
    v_count := SQL%ROWCOUNT;
    IF v_count = 1 THEN
        -- ����������� ������� ��������� � ����-�������, ������� ����� (item)
        UPDATE ITEM_T 
           SET INV_ITEM_ID = v_inv_item_id
         WHERE BILL_ID = p_bill_id
           AND REP_PERIOD_ID = p_rep_period_id
           AND SERVICE_ID = p_service_id
           AND (p_date_from IS NULL OR TRUNC(DATE_FROM,'mm') = v_date_from);
          
        -- ��������� � ��������� ������� � �����
        p_inv_item_no := p_inv_item_no + 1;
        --
    END IF;   
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Bill_id='||p_bill_id
                                  ||', service_id='||p_service_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������� �����-������� ��� ����������� �����, ���� 'B',
-- �������: 7704 - ����������� ����� �� ����������� ������. ��� ��������� �������� �������������
-- item-� �� ����� ������ ������������ �� ����������� ������ �������� �������,
-- �� �������� �� ���� �������� ������
    --   - ������������� - ID invoice_item, 
    --   - ��� ������ ���������� ����������
PROCEDURE Calc_inv_item_subsrv_std (
               p_inv_item_no   IN OUT INTEGER,   -- ����� ������ � ����� �������
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
               p_service_id    IN INTEGER,   -- ID ������
               p_vat           IN NUMBER     -- ������ ������ � ���������
           ) 
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Calc_inv_item_subsrv_std';
    v_inv_item_id   INTEGER;
    --
BEGIN
    FOR c_item IN (
        SELECT 
             SUBSERVICE_ID,         
             ROUND(SUM(TOTAL),2) SUM_TOTAL, 
             ROUND(SUM(GROSS),2) SUM_GROSS, 
             ROUND(SUM(TAX),2)   SUM_TAX, 
             MIN(DATE_FROM)      DATE_FROM, 
             MAX(DATE_TO)        DATE_TO
          FROM (
            SELECT 
                 SUBSERVICE_ID,
                 -- ������ ����� ���������� � �������� -------------
                 Calc_total( AMOUNT, TAX_INCL, p_vat ) TOTAL, 
                 -- ����� ���������� ��� ������� -------------------
                 Calc_gross( AMOUNT, TAX_INCL, p_vat ) GROSS, 
                 -- ������ ����� ������ �� ����������� ���������� --
                 Calc_tax( AMOUNT, TAX_INCL, p_vat ) TAX,
                 -- ���������� � ������������ ���� �������� ������ --
                 DATE_FROM, DATE_TO
              FROM (
                SELECT I.SUBSERVICE_ID, I.TAX_INCL, 
                   SUM(BILL_TOTAL) AMOUNT, 
                   MIN(GREATEST(TRUNC(I.DATE_FROM,'mm'), TRUNC(O.DATE_FROM))) DATE_FROM,
                   MAX(LEAST((ADD_MONTHS(TRUNC(I.DATE_TO,'mm'),1)-1/86400), O.DATE_TO)) DATE_TO
                FROM ITEM_T I, ORDER_T O
                 WHERE I.BILL_ID = p_bill_id
                   AND I.REP_PERIOD_ID = p_rep_period_id
                   AND I.ORDER_ID = O.ORDER_ID
                   AND I.SERVICE_ID = p_service_id
                GROUP BY I.SUBSERVICE_ID, I.TAX_INCL
            )
        )
        GROUP BY SUBSERVICE_ID
    )
    LOOP
        -- ��������� id ������ �����-������� �� id ������ �����
        v_inv_item_id := PK02_POID.Next_invoice_item_id;
        
        -- ��������� �� ����������� ������
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
             p_bill_id, p_rep_period_id, 
             v_inv_item_id, p_inv_item_no, p_service_id, 
             SS.SUBSERVICE INV_ITEM_NAME, p_vat,
             c_item.SUM_TOTAL, c_item.SUM_GROSS, c_item.SUM_TAX, 
             c_item.DATE_FROM, c_item.DATE_TO
          FROM SUBSERVICE_T SS
         WHERE SS.SUBSERVICE_ID = c_item.SUBSERVICE_ID;
        --
        -- ����������� ������� ��������� � ����-�������, ������� ����� (item)
        UPDATE ITEM_T 
           SET INV_ITEM_ID   = v_inv_item_id
         WHERE BILL_ID       = p_bill_id
           AND REP_PERIOD_ID = p_rep_period_id
           AND SERVICE_ID    = p_service_id
           AND SUBSERVICE_ID = c_item.SUBSERVICE_ID;
         
        -- ��������� � ��������� ������� � �����
        p_inv_item_no := p_inv_item_no + 1;
        --

    END LOOP;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Bill_id='||p_bill_id
                                  ||', service_id='||p_service_id, c_PkgName||'.'||v_prcName );
END;

-- ������ ����������� ����������� �� ����������� �������� (��� ���������)
PROCEDURE Calc_inv_item_subsrv_ext (
               p_inv_item_no   IN OUT INTEGER,   -- ����� ������ � ����� �������
               p_bill_id       IN INTEGER,   -- ID ������� �����
               p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
               p_service_id    IN INTEGER,   -- ID ������
               p_vat           IN NUMBER,     -- ������ ������ � ���������
               p_date_from     IN DATE DEFAULT NULL  -- ���� ������ �������� ������,     
           )                                 -- � �-� ����� ������� ������ ��������� 
IS                                           -- � ������ �������� (�������������)
    v_prcName       CONSTANT VARCHAR2(30) := 'Calc_inv_item_subsrv_ext';
    v_date_from     DATE;
    v_date_to       DATE;
    v_inv_item_id   INTEGER;
    --
BEGIN
    IF p_date_from IS NULL THEN  
        -- ����������� ������
        v_date_from := Pk04_Period.Period_from(p_rep_period_id);
        v_date_to   := Pk04_Period.Period_to(p_rep_period_id);
    ELSE
        -- ��������� ������
        v_date_from := TRUNC(p_date_from,'mm');
        v_date_to   := ADD_MONTHS(v_date_from,1)-1/86400;
    END IF;

    FOR c_item IN (
        SELECT 
             SUBSERVICE_ID,         
             ROUND(SUM(TOTAL),2) SUM_TOTAL, 
             ROUND(SUM(GROSS),2) SUM_GROSS, 
             ROUND(SUM(TAX),2)   SUM_TAX, 
             MIN(DATE_FROM) DATE_FROM, 
             MAX(DATE_TO)   DATE_TO
          FROM (
            SELECT 
                 SUBSERVICE_ID,
                 -- ������ ����� ���������� � �������� -------------
                 Calc_total( AMOUNT, TAX_INCL, p_vat ) TOTAL, 
                 -- ����� ���������� ��� ������� -------------------
                 Calc_gross( AMOUNT, TAX_INCL, p_vat ) GROSS, 
                 -- ������ ����� ������ �� ����������� ���������� --
                 Calc_tax( AMOUNT, TAX_INCL, p_vat ) TAX,
                 -- ���������� � ������������ ���� �������� ������ --
                 DATE_FROM, DATE_TO
              FROM (
                SELECT I.SUBSERVICE_ID, I.TAX_INCL, 
                   SUM(BILL_TOTAL) AMOUNT, 
                   MIN(GREATEST(v_date_from, TRUNC(O.DATE_FROM))) DATE_FROM,
                   MAX(LEAST(v_date_to, O.DATE_TO)) DATE_TO
                FROM ITEM_T I, ORDER_T O
                 WHERE I.BILL_ID = p_bill_id
                   AND I.REP_PERIOD_ID = p_rep_period_id
                   AND I.ORDER_ID = O.ORDER_ID
                   AND I.SERVICE_ID = p_service_id
                   AND (p_date_from IS NULL OR TRUNC(I.DATE_FROM,'mm') = v_date_from)
                GROUP BY I.SUBSERVICE_ID, I.TAX_INCL
            )
        )
        GROUP BY SUBSERVICE_ID
    )
    LOOP
        -- ��������� id ������ �����-������� �� id ������ �����
        v_inv_item_id := PK02_POID.Next_invoice_item_id;
        
        -- ��������� �� ����������� ������
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
             p_bill_id, p_rep_period_id, 
             v_inv_item_id, p_inv_item_no, p_service_id, 
             SS.SUBSERVICE INV_ITEM_NAME, p_vat,
             c_item.SUM_TOTAL, c_item.SUM_GROSS, c_item.SUM_TAX, 
             c_item.DATE_FROM, c_item.DATE_TO
          FROM SUBSERVICE_T SS
         WHERE SS.SUBSERVICE_ID = c_item.SUBSERVICE_ID;
        --
        -- ����������� ������� ��������� � ����-�������, ������� ����� (item)
        UPDATE ITEM_T 
           SET INV_ITEM_ID   = v_inv_item_id
         WHERE BILL_ID       = p_bill_id
           AND REP_PERIOD_ID = p_rep_period_id
           AND SERVICE_ID    = p_service_id
           AND SUBSERVICE_ID = c_item.SUBSERVICE_ID
           AND (p_date_from IS NULL OR TRUNC(DATE_FROM,'mm') = v_date_from);
         
        -- ��������� � ��������� ������� � �����
        p_inv_item_no := p_inv_item_no + 1;
        --
         
    END LOOP;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Bill_id='||p_bill_id
                                  ||', service_id='||p_service_id, c_PkgName||'.'||v_prcName );
END;


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
    v_count         INTEGER := 0; -- � ������� ������ ������ �� ���������� ������
    -- - - - - - - - - - -- 
    v_account_id    INTEGER;
    v_contract_id   INTEGER;
    v_customer_id   INTEGER;
    v_vat           NUMBER;
    v_inv_item_name INVOICE_ITEM_T.INV_ITEM_NAME%TYPE;
    v_bill_type     CHAR(1);
    v_date_from     DATE;
    v_service_id    INTEGER;
    v_inv_rule_id   INTEGER;
    v_retcode       INTEGER;
    -- - - - - - - - - - --
    c_items         SYS_REFCURSOR;
BEGIN
    
    -- �������� ��������� ������ �/� ����������� � ��������� ����������� �������
    -- ���������� ������ �������, ��������� � �����
    SELECT B.ACCOUNT_ID, B.CONTRACT_ID, AP.CUSTOMER_ID, B.VAT, B.BILL_TYPE, B.INVOICE_RULE_ID
      INTO v_account_id, v_contract_id, v_customer_id, v_vat, v_bill_type, v_inv_rule_id
      FROM BILL_T B, ACCOUNT_PROFILE_T AP
     WHERE B.BILL_ID        = p_bill_id
       AND B.REP_PERIOD_ID  = p_rep_period_id
       AND B.PROFILE_ID     = AP.PROFILE_ID;

    -- ��������� ������� � ����������� �� ������� ������������ �����, ���������� � �����
    IF v_inv_rule_id IN ( Pk00_Const.c_INVOICE_RULE_EXT, 
                          Pk00_Const.c_INVOICE_RULE_SUB_EXT )
    THEN
        -- invoice_item-� ������������ �� �������� item-��
        OPEN c_items FOR
          SELECT I.SERVICE_ID, O.SERVICE_ALIAS, 
                 TRUNC(I.DATE_FROM,'mm') DATE_FROM 
            FROM ITEM_T I, ORDER_T O
           WHERE BILL_ID = p_bill_id
             AND I.REP_PERIOD_ID = p_rep_period_id
             AND I.ORDER_ID = O.ORDER_ID
             GROUP BY I.SERVICE_ID, O.SERVICE_ALIAS, TRUNC(I.DATE_FROM,'mm')
             ORDER BY 1,2
        ;
    ELSE 
         -- ��� item-� ������������ � ���� invoice_item 
         -- Pk00_Const.c_INVOICE_RULE_STD     -- 7701;
         -- Pk00_Const.c_INVOICE_RULE_BIL     -- 7702;
         -- Pk00_Const.c_INVOICE_RULE_SUB_STD -- 7704;
         -- Pk00_Const.c_INVOICE_RULE_SUB_BIL -- 7705;
        OPEN c_items FOR
          SELECT I.SERVICE_ID, O.SERVICE_ALIAS,
                 NULL DATE_FROM -- ������ �����������, ��� item-� ������������ � ���� invoice_item
            FROM ITEM_T I, ORDER_T O
           WHERE I.BILL_ID = p_bill_id
             AND I.REP_PERIOD_ID = p_rep_period_id
             AND I.ORDER_ID = O.ORDER_ID
             GROUP BY I.SERVICE_ID, O.SERVICE_ALIAS
             ORDER BY 1
        ;
    END IF;

    -- ��������� ������ ����� ������� ��� ���� ����� �����    
    -- ��������� � �������
    v_count := 1;
    --
    LOOP
        FETCH c_items INTO v_service_id, v_inv_item_name, v_date_from;
        EXIT WHEN c_items%NOTFOUND;

        -- �������� ��� ������� ����� �������
        IF v_inv_item_name IS NULL THEN
            v_inv_item_name := Get_item_name (
                  p_service_id  => v_service_id,
                  p_account_id  => v_account_id,
                  p_contract_id => v_contract_id,
                  p_customer_id => v_customer_id
               );
        END IF;

        -- ������ ������� ����� ������� �� ��������� � BILL_T.INVOICE_RULE_ID ��������
        IF    v_inv_rule_id = Pk00_Const.c_INVOICE_RULE_BIL THEN     -- 7702;
            Calc_inv_item_ext (
                p_inv_item_no   => v_count,        -- ����� ������ � ����� �������
                p_bill_id       => p_bill_id,      -- ID ������� �����
                p_rep_period_id => p_rep_period_id,-- ID ��������� ������� �����
                p_service_id    => v_service_id,   -- ID ������
                p_inv_item_name => v_inv_item_name,-- ��� ������ � ����� �������
                p_vat           => v_vat,          -- ������ ������ � ���������
                p_date_from     => v_date_from     -- ��� � ����������� ������
            );
        ELSIF v_inv_rule_id = Pk00_Const.c_INVOICE_RULE_EXT THEN
            Calc_inv_item_ext (
                p_inv_item_no   => v_count,        -- ����� ������ � ����� �������
                p_bill_id       => p_bill_id,      -- ID ������� �����
                p_rep_period_id => p_rep_period_id,-- ID ��������� ������� �����
                p_service_id    => v_service_id,   -- ID ������
                p_inv_item_name => v_inv_item_name,-- ��� ������ � ����� �������
                p_vat           => v_vat,          -- ������ ������ � ���������
                p_date_from     => v_date_from     -- ���� �������
            );
        ELSIF v_inv_rule_id = Pk00_Const.c_INVOICE_RULE_SUB_STD THEN -- 7704;
            Calc_inv_item_subsrv_std (
                p_inv_item_no   => v_count,        -- ����� ������ � ����� �������
                p_bill_id       => p_bill_id,      -- ID ������� �����
                p_rep_period_id => p_rep_period_id,-- ID ��������� ������� �����
                p_service_id    => v_service_id,   -- ID ������
                p_vat           => v_vat           -- ������ ������ � ���������
             );
        ELSIF v_inv_rule_id = Pk00_Const.c_INVOICE_RULE_SUB_BIL THEN -- 7705;
            Calc_inv_item_subsrv_ext (
                p_inv_item_no   => v_count,        -- ����� ������ � ����� �������
                p_bill_id       => p_bill_id,      -- ID ������� �����
                p_rep_period_id => p_rep_period_id,-- ID ��������� ������� �����
                p_service_id    => v_service_id,   -- ID ������
                p_vat           => v_vat,          -- ������ ������ � ���������
                p_date_from     => NULL            -- ��� � ����������� ������
             );
        ELSIF v_inv_rule_id = Pk00_Const.c_INVOICE_RULE_SUB_EXT THEN
            Calc_inv_item_subsrv_ext (
                p_inv_item_no   => v_count,        -- ����� ������ � ����� �������
                p_bill_id       => p_bill_id,      -- ID ������� �����
                p_rep_period_id => p_rep_period_id,-- ID ��������� ������� �����
                p_service_id    => v_service_id,   -- ID ������
                p_vat           => v_vat,          -- ������ ������ � ���������
                p_date_from     => v_date_from     -- ���� �������
             );
        --ELSIF v_inv_rule_id = Pk00_Const.c_INVOICE_RULE_STD 
        --  AND v_service_id = Pk00_Const.c_SERVICE_OP_LOCAL THEN
        --    -- ��� ���� �������� ��� ��������������� �����
        --    Calc_inv_item_subsrv_std (
        --        p_inv_item_no   => v_count,        -- ����� ������ � ����� �������
        --        p_bill_id       => p_bill_id,      -- ID ������� �����
        --        p_rep_period_id => p_rep_period_id,-- ID ��������� ������� �����
        --        p_service_id    => v_service_id,   -- ID ������
        --        p_vat           => v_vat           -- ������ ������ � ���������
        --     );
        ELSE               -- Pk00_Const.c_INVOICE_RULE_STD          -- 7701;
            Calc_inv_item_std (
                p_inv_item_no   => v_count,        -- ����� ������ � ����� �������
                p_bill_id       => p_bill_id,      -- ID ������� �����
                p_rep_period_id => p_rep_period_id,-- ID ��������� ������� �����
                p_service_id    => v_service_id,   -- ID ������
                p_inv_item_name => v_inv_item_name,-- ��� ������ � ����� �������
                p_vat           => v_vat           -- ������ ������ � ���������
            );
        END IF;

    END LOOP;
    
    -- ��������� ������
    CLOSE c_items;
    --
    RETURN v_count;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR. Bill_id='||p_bill_id, c_PkgName||'.'||v_prcName);
        IF c_items%ISOPEN THEN 
            CLOSE c_items;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);  
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

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������ ������ ������������ ����� �����-������
PROCEDURE Invoice_rules_list( 
               p_recordset IN OUT SYS_REFCURSOR
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Invoice_rules_list';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT KEY_ID INVOICE_RULE_ID, KEY INVOICE_RULE_KEY, NAME INVOICE_RULE, NOTES 
          FROM DICTIONARY_T
         WHERE PARENT_ID = Pk00_Const.k_DICT_INV_RULE
         ORDER BY 1;
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

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������������� ������ ����-�������
PROCEDURE Edit_invoice_item_name ( 
          p_bill_id      IN    INTEGER,
          p_period_id    IN    INTEGER,
          p_invoice_id   IN    INTEGER,
          p_name         IN    VARCHAR2
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Edit_invoice_item_name';
    v_retcode    INTEGER;
BEGIN
    UPDATE INVOICE_ITEM_T
       SET INV_ITEM_NAME = p_name
    WHERE 
         BILL_ID = p_bill_id 
       AND REP_PERIOD_ID = p_period_id
       AND INV_ITEM_ID = p_invoice_id;     
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK09_INVOICE_NEW;
/
