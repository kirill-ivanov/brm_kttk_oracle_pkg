CREATE OR REPLACE PACKAGE PK19_EVENT
IS
    --
    -- ����� ��� ������ � �������� "�������", �������:
    -- event_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK19_EVENT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- �������� ������� ����� ������� (item) �����, ����������:
    --   - ID ������� (event_id), 
    --   - ��� ������ ���������� ����������
    FUNCTION New_event( 
                   p_order_id       IN INTEGER,   -- ID ������ �� ������
                   p_item_id        IN INTEGER,   -- ID ������� �����
                   p_event_type_id  IN INTEGER,   -- ID ���� �������
                   p_charge_type_id IN INTEGER,   -- ID ���� ����������
                   p_date_from      IN DATE,      -- ���� ������ ������ �� ������
                   p_date_to        IN DATE,      -- ���� ��������� ������ �� ������
                   p_quantity       IN NUMBER,    -- ���-�� ������ � ����������� �����������
                   p_bill_amount    IN NUMBER,    -- ����� � ������ �����
                   p_tariff_amount  IN NUMBER     -- ����� � ������ ������
               ) RETURN INTEGER;
    
END PK19_EVENT;
/
CREATE OR REPLACE PACKAGE BODY PK19_EVENT
IS

-- �������� ������� ����� ������� (item) �����, ����������:
--   - ID ������� (event_id), 
--   - ��� ������ ���������� ����������
FUNCTION New_event( 
               p_order_id       IN INTEGER,   -- ID ������ �� ������
               p_item_id        IN INTEGER,   -- ID ������� �����
               p_event_type_id  IN INTEGER,   -- ID ���� �������
               p_charge_type_id IN INTEGER,   -- ID ���� ����������
               p_date_from      IN DATE,      -- ���� ������ ������ �� ������
               p_date_to        IN DATE,      -- ���� ��������� ������ �� ������
               p_quantity       IN NUMBER,    -- ���-�� ������ � ����������� �����������
               p_bill_amount    IN NUMBER,    -- ����� � ������ �����
               p_tariff_amount  IN NUMBER     -- ����� � ������ ������
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'New_event';
    v_item_id    INTEGER;
    v_date_from  DATE;
    v_date_to    DATE;
BEGIN
    -- �������� ID ������� �����
    v_item_id := PK02_POID.Item_Event_id_nextval(p_item_id);
    -- ������� ������ ������� �����
    INSERT INTO EVENT_T ( EVENT_ID, EVENT_TYPE_ID, ORDER_ID, ITEM_ID,
         DATE_FROM, DATE_TO, QUANTITY, BILL_AMOUNT, TARIFF_AMOUNT,
         CHARGE_TYPE_ID, SAVE_DATE
    )VALUES( 
         v_item_id, p_event_type_id, p_order_id, p_item_id,
         p_date_from, p_date_to, p_quantity, p_bill_amount, p_tariff_amount,
         p_charge_type_id, SYSDATE
    );
    -- ���������� ID ��������� ������� �����
    RETURN v_item_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


END PK19_EVENT;
/
