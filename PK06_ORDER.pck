CREATE OR REPLACE PACKAGE PK06_ORDER
IS
    --
    -- ����� ��� ������ � �������� "�����", �������:
    -- order_t
    --
    -- ==============================================================================
    c_PkgName   CONSTANT varchar2(30) := 'PK06_ORDER';
    -- ==============================================================================
    c_RET_OK    CONSTANT integer := 0;
    c_RET_ER		CONSTANT integer :=-1;
    
    TYPE t_refc IS REF CURSOR;
    
    -- ������� ����� ����� �� ������� �����, ���������� ��������
    --   - ������������� - ID ������ �� ������� ����� �������, 
    --   - ��� ������ ���������� ����������
    FUNCTION New_order(
                   p_account_id    IN INTEGER,   -- ID �������� �����
                   p_order_no      IN VARCHAR2,  -- ����� ������, ��� �� ������
                   p_service_id    IN INTEGER,   -- ID ������ �� ������� SERVICE_T
                   p_rateplan_id   IN INTEGER,   -- ID ��������� ����� �� RATEPLAN_T
                   p_time_zone     IN INTEGER,   -- GMT
                   p_date_from     IN DATE,      -- ���� ������ �������� ������
                   p_date_to       IN DATE DEFAULT Pk00_Const.c_DATE_MAX,
                   p_create_date   IN DATE DEFAULT SYSDATE,
                   p_note          IN varchar2 DEFAULT NULL
               ) RETURN INTEGER;
               
    -- ������������� ����� �� ������� �����, ���������� ��������
    --   - ������������� - ��, 
    --   - ������������� - id ��������� �� ����� � L01
    PROCEDURE Edit_order(
                   p_order_id      IN INTEGER,   -- ID ������
                   p_order_no      IN VARCHAR2,  -- ����� ������, ��� �� ������
                   p_service_id    IN INTEGER,   -- ID ������ �� ������� SERVICE_T
                   p_rateplan_id   IN INTEGER,   -- ID ��������� ����� �� RATEPLAN_T
                   p_date_from     IN DATE,      -- ���� ������ �������� ������
                   p_date_to       IN DATE
               );
               
-- ������������� ������� ������
   PROCEDURE Edit_order_body(
          p_order_body_id  IN INTEGER,   -- ID ������� ������
          p_rateplan_id    IN INTEGER,   -- ID ��������� ����� �� RATEPLAN_T
          p_abon_value     IN NUMBER,
          p_currency       IN INTEGER,
          p_quantity       IN NUMBER,
          p_tax_incl       IN VARCHAR2,
          p_date_from      IN DATE,      -- ���� ������ �������� ������
          p_date_to        IN DATE
    );                    
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ����� �� ������� ������ ���������� ��������
    --   - ������������� - OK 
    --   - ������������� - id ��������� �� ����� � L01
    --
    PROCEDURE Delete_order(
                   p_order_id    IN INTEGER      -- ID ������
               );
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ������� ������
    PROCEDURE Delete_order_body(
        p_order_id       IN INTEGER,      -- ID ������
        p_order_body_id  IN INTEGER,
        p_user_login     IN VARCHAR2
    );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ����� �������
    -- - ��� ������ ���������� ����������
    PROCEDURE Close_order (
                   p_order_id      IN INTEGER,
                   p_date_to       IN DATE DEFAULT SYSDATE
               );
                   
    PROCEDURE Close_order_by_no (
                   p_order_no      IN VARCHAR2,
                   p_date_to       IN DATE DEFAULT SYSDATE
               );
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� ��������� ���� � ������, ��� ������� ��������� � ������ ��
    -- �� ����� ���� ��������, ��� �� ������ - � ������ � �����, ��� � �� ��������� ������
    -- - ��� ������ ���������� ����������
    PROCEDURE Bind_rateplan (
                   p_rateplan_id   IN INTEGER, -- ID ��������� �����
                   p_order_id      IN INTEGER, -- ID ������ - ������
                   p_order_body_id IN INTEGER DEFAULT NULL -- ID ���� ������ - ���������� ������
               );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� ��������� IP ���� � ������, ��� ������� ��������� � ������ ��
    -- �� ����� ���� ��������, ��� �� ������ - � ������ � �����, ��� � �� ��������� ������
    -- - ��� ������ ���������� ����������
    --  !!! �������������� �������� � rate_rule_id !!!
    PROCEDURE Bind_rateplan_IP (
                   p_rateplan_id   IN INTEGER, -- ID ��������� �����
                   p_order_id      IN INTEGER, -- ID ������ 
                   p_order_body_id IN INTEGER -- ������������ �������� !
               );
           
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� ��������� ��������� ���� � ������
    -- - ��� ������ ���������� ����������
    PROCEDURE Bind_agent_rateplan (
                   p_rateplan_id   IN INTEGER, -- ID ��������� �����
                   p_order_id      IN INTEGER  -- ID ������ - ������
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� �����, ������� ��� ������������ ������� �������
    PROCEDURE Set_parent_order (
                   p_order_id    IN INTEGER, -- ID ������ - ������
                   p_parent_id   IN INTEGER  -- ID ������, ������� ��� ������������ ������� �������
               );
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������� �������������� ���������� ��� IP-������, ������: 
    -- - IP access service_id = 104
    -- - EPL service_id = 133
    -- - NPL,KLLM service_id = 101
    -- ������������ ��� ��� ������ ����������� ����������� � �����
    -- - ��� ������ ���������� ����������
    PROCEDURE Set_ip_channel_info (
                   p_order_id      IN INTEGER,  -- ID ������ - ������
                   p_point_src     IN VARCHAR2, -- 
                   p_point_dst     IN VARCHAR2,
                   p_speed_value   IN NUMBER,
                   p_speed_unit_id IN INTEGER
               );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ���������� � ���� IP VPN: 
    -- - ��� ������ ���������� ����������
    PROCEDURE Set_vpn_zone (
                   p_order_id      IN INTEGER,  -- ID ������
                   p_zone          IN VARCHAR2 
               );               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������� ������������� ���������� ���� ��� ������� � ������� �����  
    -- - ��� ������ ���������� ����������
    PROCEDURE Set_network_id (
                   p_order_id      IN INTEGER,  -- ID ������ - ������
                   p_network_id    IN INTEGER 
               );
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ���������� ������������� ����������� ��� ������� � ������� �����  
-- 
PROCEDURE Set_switch_id (
               p_order_id      IN INTEGER,  -- ID ������
               p_switch_id    IN INTEGER 
           );            
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� ��������� �������������� �����, ���������� ��������
    -- - ��� ������ ���������� ����������
    PROCEDURE Set_manager (
                   p_order_id   IN INTEGER,
                   p_manager_id IN INTEGER,
                   p_date_from  IN DATE DEFAULT SYSDATE
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ID ��������� �������������� �����, ���������� ��������
    -- - ������������� - ID ���������
    -- - NULL - ��� ������
    -- - ��� ������ ���������� ����������
    FUNCTION Get_manager_id (
                   p_order_id  IN INTEGER,
                   p_date      IN DATE DEFAULT SYSDATE
               ) RETURN INTEGER;


    -- -------------------------------------------------------------------- --
    -- ������ � ���������� ������
    -- -------------------------------------------------------------------- --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ��������� ������ �� �����, ���������� ��������
    -- - ������������� - ORDER_BODY_ID
    -- - ��� ������ ���������� ����������
    FUNCTION Add_subservice (
                   p_order_id      IN INTEGER,
                   p_subservice_id IN INTEGER,
                   p_charge_type   IN VARCHAR2,
                   p_rateplan_id   IN INTEGER DEFAULT NULL,
                   p_date_from     IN DATE DEFAULT SYSDATE,
                   p_date_to       IN DATE DEFAULT Pk00_Const.c_DATE_MAX,
                   p_notes         IN VARCHAR2 DEFAULT NULL,
                   p_currency_id   IN INTEGER DEFAULT Pk00_Const.c_CURRENCY_RUB
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ��������� ������ �� ������, ���������� ��������
    -- - ��� ������ ���������� ����������
    PROCEDURE Close_subservice (
                   p_order_id      IN INTEGER,
                   p_subservice_id IN INTEGER,
                   p_date_to       IN DATE DEFAULT SYSDATE
               );

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������ ��������� ��� ������ ��� ���������� ������ ������
-- - ���������� ID ������ ORDER_BODY_T
-- - ��� ������ ���������� ����������
FUNCTION Add_subs_abon (
               p_order_id      IN INTEGER, -- ID ������ - ������
               p_subservice_id IN INTEGER, -- ID ���������� ������
               p_value         IN NUMBER,  -- ����� ���������
               p_tax_incl      IN CHAR,    -- ������� �� ����� � ����� ���������
               p_currency_id   IN INTEGER, -- ������
               p_quantity      IN NUMBER,  -- ���-�� ������ � ����������� ���������
               p_date_from     IN DATE,
               p_date_to       IN DATE DEFAULT Pk00_Const.c_DATE_MAX
           ) RETURN INTEGER;

 -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������ ��������� ��� ��������� ������ (������)
    -- - ���������� ID ������ ORDER_BODY_T
    -- - ��� ������ ���������� ����������
    FUNCTION Add_subs_abon_voice (
                   p_order_id      IN INTEGER, -- ID ������ - ������
                   p_subservice_id IN INTEGER, -- ID ���������� ������
                   p_value         IN NUMBER,  -- ����� ���������
                   p_tax_incl      IN CHAR,    -- ������� �� ����� � ����� ���������
                   p_currency_id   IN INTEGER, -- ������
                   p_free_traffic  IN NUMBER,  -- ���-�� ������ � ����������� ���������
                   p_date_from     IN DATE DEFAULT SYSDATE
               ) RETURN INTEGER;  
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������ ������ ����������� ����� ������ ������ ��� ������
    -- - ���������� ID ������ ORDER_BODY_T
    -- - ��� ������ ���������� ����������
    FUNCTION Add_subs_min (
                   p_order_id      IN INTEGER, -- ID ������ - ������
                   p_subservice_id IN INTEGER, -- ID ���������� ������
                   p_value         IN NUMBER,  -- ����� ���������
                   p_tax_incl      IN CHAR,    -- ������� �� ����� � ����� ���������
                   p_currency_id   IN INTEGER, -- ������
                   p_rate_level_id IN INTEGER, -- ������� ��������: ���������/�����/������� ����
                   p_date_from     IN DATE DEFAULT SYSDATE
               ) RETURN INTEGER;
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ��� ��� ������ ���������� ����������� �������� �
    -- - ��� ������ ���������� ����������
    PROCEDURE Add_subs_downtime (
                   p_order_id      IN INTEGER, -- ID ������ - ������
                   p_charge_type   IN VARCHAR2,
                   p_free_value    IN NUMBER,  -- ���-�� ���������������� ����� ��������
                   p_descr         IN VARCHAR2,
                   p_date_from     IN DATE DEFAULT SYSDATE
               );

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������� ��� ��� ������ ���������� ����������� �������� �
-- - ��� ������ ���������� ����������
PROCEDURE Add_ob_downtime (
               p_order_id      IN INTEGER, -- ID ������ - ������
               p_charge_type   IN VARCHAR2,
               p_free_value    IN NUMBER,  -- ���-�� ���������������� ����� ��������
               p_descr         IN VARCHAR2,
               p_date_from     IN DATE DEFAULT SYSDATE
           );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������� ��� ��� ������ ������������� ������
    -- - ��� ������ ���������� ����������
    PROCEDURE Add_subs_discount (
                   p_order_id      IN INTEGER, -- ID ������ - ������
                   p_currency_id   IN INTEGER, -- ID ������ �������
                   p_date_from     IN DATE DEFAULT SYSDATE
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ��������� �� ���������� �� ������ ��� ����������� �������� ���
    -- ������� � ��� ��� - "MS107643/�CCA/353P11"
    -- ������ ������, �������� �������� �� �.������������ 16.09.2016
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Get_css_phone_dirrecion(
                                  p_order_id IN INTEGER,
                                  p_abn_a    IN VARCHAR2,
                                  p_descr    IN VARCHAR2
                               ) RETURN VARCHAR2;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ������� � ������
    -- - ������������� - ORDER_BODY_ID
    -- - ��� ������ ���������� ����������
    PROCEDURE Add_phone (
                 p_order_id      IN INTEGER,
                 p_phone         IN VARCHAR2,
                 p_date_from     IN DATE DEFAULT SYSDATE,
                 p_date_to       IN DATE DEFAULT  Pk00_Const.c_DATE_MAX,
                 p_descr         IN VARCHAR2 DEFAULT NULL
             );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ������� � ���������� ������
    -- - ������������� - ORDER_BODY_ID
    -- - ��� ������ ���������� ����������
    PROCEDURE Add_phone_ag (
                   p_order_id      IN INTEGER,
                   p_phone         IN VARCHAR2,
                   p_date_from     IN DATE DEFAULT SYSDATE,
                   p_date_to       IN DATE DEFAULT  Pk00_Const.c_DATE_MAX,
                   p_descr         IN VARCHAR2 DEFAULT NULL
               );               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ���������� ������� �����������, ��� ���������� ������
    PROCEDURE Set_rate_rule (
                 p_order_body_id IN INTEGER,
                 p_rate_rule_id  IN INTEGER,
                 p_currency_id   IN INTEGER,
                 p_tax_incl      IN CHAR
             );
           
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ������ ����������� ������ �� ������
    --   - ������������� - ����������� ������
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Subservice_list( 
                   p_recordset  OUT t_refc,
                   p_order_id   IN INTEGER,
                   p_open_only  IN BOOLEAN
               ) RETURN INTEGER;

    -- -------------------------------------------------------------------- --
    -- ���������� / ������������� ������
    -- -------------------------------------------------------------------- --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������������� �����, ���������� ��������
    -- - ������������� - ID ������ � ����������
    -- - ��� ������ ���������� ����������
    FUNCTION Lock_order (
                   p_order_id      IN INTEGER,
                   p_lock_type_id  IN INTEGER,
                   p_manager_login IN VARCHAR2,
                   p_date_from     IN DATE DEFAULT SYSDATE,
                   p_notes         IN VARCHAR2
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������������� �����, ���������� ��������
    -- - ������������� - ID ������ � ����������
    -- - ��� ������ ���������� ����������
    FUNCTION UnLock_order (
                   p_order_id      IN INTEGER,
                   p_manager_login IN VARCHAR2,
                   p_date_to       IN DATE DEFAULT SYSDATE,
                   p_notes         IN VARCHAR2
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ������ ���������� ������, ���������� ��������
    -- - ������������� - ID ���� ����������
    -- - NULL - ����� �� ������������
    -- - ��� ������ ���������� ����������
    FUNCTION GetLock_type (
                   p_order_id     IN INTEGER,
                   p_date         IN DATE DEFAULT SYSDATE
               ) RETURN INTEGER;
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������������� ���� ����������/�������������
    PROCEDURE UpdateLockDates(p_lock_id IN INTEGER, p_date_from IN DATE, p_date_to IN DATE);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- ������, ������� �������� �� ��, ��� ����� ����������� �����,
    -- ����� ��� ������� ��������� �� XML, ��������� ��� ��� ��������� �.�.�����:
    -- ACC xxx xxx xxx - nn
    -- (������ ������ ��������:: YY LD x xxx xxx - �� ����������)
    FUNCTION Make_order_No (p_account_no IN VARCHAR2) RETURN VARCHAR2;
    
    FUNCTION Get_number_from_order_no(p_order_no IN VARCHAR2) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ������� ���� �������, �� ������ ��
    PROCEDURE Refresh_statuses;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ������������� ���� ������ �� ������� �����
    PROCEDURE Edit_order_dates(
               p_order_id      IN INTEGER,   -- ID ������
               p_date_from     IN DATE,      -- ���� ������ �������� ������
               p_date_to       IN DATE
           );
           
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� ����� � ������ �������� ����� �� ������
    PROCEDURE Move_order(
               p_order_id       IN INTEGER,   -- ID ������
               p_account_id_dst IN INTEGER,   -- �/� �� ������� ��������� �����
               p_date_from      IN DATE       -- ���� ���������� ��������
           );
           

    -- �������� �������
    -- - ��� ������ ���������� ����������
    FUNCTION Get_downtime (
                   p_order_id  IN INTEGER
                   ) RETURN INTEGER;    
           
END PK06_ORDER;
/
CREATE OR REPLACE PACKAGE BODY PK06_ORDER
IS

-- ������� ����� ����� �� ������� �����, ���������� ��������
--   - ������������� - ID ������ �� ������� ����� �������, 
--   - ��� ������ ���������� ����������
FUNCTION New_order(
               p_account_id    IN INTEGER,   -- ID �������� �����
               p_order_no      IN VARCHAR2,  -- ����� ������, ��� �� ������
               p_service_id    IN INTEGER,   -- ID ������ �� ������� SERVICE_T
               p_rateplan_id   IN INTEGER,   -- ID ��������� ����� �� RATEPLAN_T
               p_time_zone     IN INTEGER,   -- GMT               
               p_date_from     IN DATE,      -- ���� ������ �������� ������
               p_date_to       IN DATE DEFAULT Pk00_Const.c_DATE_MAX,
               p_create_date   IN DATE DEFAULT SYSDATE,
               p_note          IN varchar2 DEFAULT NULL 
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'New_order';
    v_order_id    INTEGER;
		v_date_to			DATE := NVL(p_date_to, Pk00_Const.c_DATE_MAX);
BEGIN
    -- ������� ������ ������ �������� �����
    INSERT INTO ORDER_T (
       ORDER_ID, ORDER_NO, ACCOUNT_ID, SERVICE_ID, RATEPLAN_ID, DATE_FROM, DATE_TO,
       CREATE_DATE, MODIFY_DATE, TIME_ZONE, NOTES
    )VALUES(
       SQ_ORDER_ID.NEXTVAL, p_order_no, p_account_id, p_service_id, p_rateplan_id, p_date_from, v_date_to,
       NVL(p_Create_Date, SYSDATE), NVL(p_Create_Date, SYSDATE), p_time_zone, p_Note        
    )
    RETURNING ORDER_ID INTO v_order_id;
    
    RETURN v_order_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.account_id='||p_account_id||
                                    ',order_no='||p_order_no, c_PkgName||'.'||v_prcName );
END;

-- ������������� ����� �� ������� �����, ���������� ��������
--   - ��� ������ ���������� ����������
PROCEDURE Edit_order(
               p_order_id      IN INTEGER,   -- ID ������
               p_order_no      IN VARCHAR2,  -- ����� ������, ��� �� ������
               p_service_id    IN INTEGER,   -- ID ������ �� ������� SERVICE_T
               p_rateplan_id   IN INTEGER,   -- ID ��������� ����� �� RATEPLAN_T
               p_date_from     IN DATE,      -- ���� ������ �������� ������
               p_date_to       IN DATE
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Edit_order';
    v_bill_status VARCHAR2(100);
BEGIN
    SELECT STATUS INTO v_bill_status FROM ORDER_T
    where order_id = p_order_id;
    
    UPDATE ORDER_T 
       SET ORDER_NO    = NVL(p_order_no, ORDER_NO),
           SERVICE_ID  = NVL(p_service_id, SERVICE_ID),
           RATEPLAN_ID = NVL(p_rateplan_id, RATEPLAN_ID),
           DATE_FROM   = NVL(p_date_from, DATE_FROM),
           DATE_TO     = NVL(p_date_to, DATE_TO)
     WHERE ORDER_ID = p_order_id;  
    
    IF v_bill_status = 'CLOSED' AND (p_date_to is null or p_date_to >= TO_DATE('01.01.2050','DD.MM.YYYY')) THEN
      UPDATE ORDER_T
         SET STATUS = 'OPEN'
      WHERE ORDER_ID = p_order_id;
    end IF;
          
    IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(-20000, '� ������� ORDER_T ��� ������ � ORDER_ID='||p_order_id);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ������������� ������� ������
PROCEDURE Edit_order_body(
          p_order_body_id  IN INTEGER,   -- ID ������� ������
          p_rateplan_id    IN INTEGER,   -- ID ��������� ����� �� RATEPLAN_T
          p_abon_value     IN NUMBER,
          p_currency       IN INTEGER,
          p_quantity       IN NUMBER,
          p_tax_incl       IN VARCHAR2,
          p_date_from      IN DATE,      -- ���� ������ �������� ������
          p_date_to        IN DATE
    )  
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Edit_order_body';
    v_account_type VARCHAR2(1);    
BEGIN
    UPDATE ORDER_BODY_T 
       SET 
           RATEPLAN_ID = NVL(p_rateplan_id, RATEPLAN_ID),
           RATE_VALUE  = NVL(p_abon_value, RATE_VALUE),
           QUANTITY    = NVL(p_quantity, QUANTITY),
           CURRENCY_ID = NVL(p_currency, CURRENCY_ID),
           DATE_FROM   = NVL(p_date_from, DATE_FROM),
           DATE_TO     = NVL(p_date_to, DATE_TO),
           TAX_INCL    = NVL(p_tax_incl,'N'),
           MODIFY_DATE = SYSDATE
     WHERE ORDER_BODY_ID = p_order_body_id;  
    
     IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20000, '� ������� ORDER_BODY_T ��� ������ � ORDER_BODY_ID='||p_order_body_id);
     END IF;
    
     --���������� ��� �/�
     SELECT a.account_type 
                  INTO v_account_type
          FROM order_body_t ob, order_t o, account_t a
         WHERE order_body_id = p_order_body_id
               AND ob.order_id = o.order_id
               AND o.account_id = a.account_id;
     
     DELETE FROM PK30_CORRECT_TAX_INCL_T WHERE ORDER_BODY_ID = p_order_body_id;
     
     --���� ����������, ����� ��������� � ����������
     IF (v_account_type = 'J' AND p_tax_incl = 'Y') OR (v_account_type = 'P' AND p_tax_incl = 'N') THEN         
         INSERT INTO PK30_CORRECT_TAX_INCL_T (ORDER_BODY_ID, ACCOUNT_TYPE, TAX_INCL) VALUES(p_order_body_id, v_account_type, p_tax_incl);
     END IF;         
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������� ����� �� ������� ������ ���������� ��������
--   - ��� ������ ���������� ����������
--
PROCEDURE Delete_order(
               p_order_id    IN INTEGER      -- ID ������
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_order';
    v_count       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start. Order_id = '||p_order_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    DELETE FROM ORDER_PHONES_T WHERE ORDER_ID = p_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_PHONES_T '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    DELETE FROM ORDER_LOCK_T WHERE  ORDER_ID = p_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_LOCK_T '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    DELETE FROM ORDER_INFO_T WHERE ORDER_ID = p_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_INFO_T '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    DELETE FROM ORDER_BODY_T WHERE ORDER_ID = p_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    DELETE FROM ORDER_T WHERE ORDER_ID = p_order_id;
    IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(-20000, '� ������� ORDER_T ��� ������ � ORDER_ID='||p_order_id);
    END IF;
    
    Pk01_Syslog.Write_msg('Stop. Order_id = '||p_order_id||' - deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,order_id='||p_order_id,c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������� ����� �� ������� ������ ���������� ��������
--   - ��� ������ ���������� ����������
--
PROCEDURE Delete_order_body(
        p_order_id        IN INTEGER,      -- ID ������
        p_order_body_id   IN INTEGER,
        p_user_login      IN VARCHAR2
)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_order_body';
    v_count       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start. Order_id = '||p_order_id || ', Order_body_id = '|| p_order_body_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    INSERT INTO PIN.ORDER_BODY_DEL_T (
         CHARGE_TYPE, CREATE_DATE, CURRENCY_ID, 
         DATE_FROM, DATE_TO, FREE_VALUE, 
         NOTES, ORDER_BODY_ID, 
         ORDER_ID, QUANTITY, RATEPLAN_ID, 
         RATE_LEVEL_ID, RATE_RULE_ID, RATE_VALUE, 
         SUBSERVICE_ID, TAX_INCL, MODIFY_DATE, USER_LOGIN) 
    SELECT 
         CHARGE_TYPE, CREATE_DATE, CURRENCY_ID, 
         DATE_FROM, DATE_TO, FREE_VALUE, 
         NOTES, ORDER_BODY_ID, 
         ORDER_ID, QUANTITY, RATEPLAN_ID, 
         RATE_LEVEL_ID, RATE_RULE_ID, RATE_VALUE, 
         SUBSERVICE_ID, TAX_INCL, SYSDATE, p_user_login 
       FROM PIN.ORDER_BODY_T 
    WHERE
         ORDER_BODY_ID = p_order_body_id;

    DELETE FROM ORDER_BODY_T WHERE ORDER_ID = p_order_id AND ORDER_BODY_ID = p_order_body_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_BODY_T '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    Pk01_Syslog.Write_msg('Stop. Order_id = '||p_order_id||', Order_body_id = ' || p_order_body_id || ' - deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,order_id='||p_order_id,c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������� ����� �������
-- - ��� ������ ���������� ����������
PROCEDURE Close_order (
               p_order_id      IN INTEGER,
               p_date_to       IN DATE DEFAULT SYSDATE
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_order';
    v_date_to    DATE;
BEGIN
    -- ���������� ����� ���
    v_date_to := TRUNC(p_date_to)+1-1/86400;
    
    -- 1. ��������� ���������� ������ �� ������
    FOR cur IN (SELECT ORDER_ID, SUBSERVICE_ID FROM ORDER_BODY_T
              WHERE ORDER_ID = p_order_id AND (DATE_TO IS NULL OR DATE_TO = TO_DATE('01.01.2050','DD.MM.YYYY')))
    LOOP
       close_subservice(cur.order_id,cur.subservice_id,v_date_to);
    END LOOP;
    
    -- 2a.��������� �������� �� ������
    FOR cur IN ( SELECT ORDER_ID, PHONE_NUMBER FROM ORDER_PHONES_T
                 WHERE ORDER_ID = p_order_id AND (DATE_TO IS NULL OR DATE_TO = TO_DATE('01.01.2050','DD.MM.YYYY')))
    LOOP
       PK18_RESOURCE.close_phone(cur.order_id,cur.PHONE_NUMBER,v_date_to);
    END LOOP;
    
   -- 2b.��������� �������� �� ������
    FOR cur IN ( SELECT ORDER_ID, PHONE_NUMBER FROM AGENT_PHONES_T
                 WHERE ORDER_ID = p_order_id AND (DATE_TO IS NULL OR DATE_TO = TO_DATE('01.01.2050','DD.MM.YYYY')))
    LOOP
       PK18_RESOURCE.close_phone_ag(cur.order_id,cur.PHONE_NUMBER,v_date_to);
    END LOOP;
    
    -- 3. ��������� ��� �����
    UPDATE ORDER_T  O
        SET DATE_TO = v_date_to, STATUS = Pk00_Const.c_ORDER_STATE_CLOSED
    WHERE ORDER_ID = p_order_id;
    
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,order_id='||p_order_id, c_PkgName||'.'||v_prcName );
END;

PROCEDURE Close_order_by_no (
               p_order_no      IN VARCHAR2,
               p_date_to       IN DATE DEFAULT SYSDATE
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_order_by_no';
    v_order_id   INTEGER;
BEGIN
    SELECT ORDER_ID INTO v_order_id
      FROM ORDER_T
     WHERE ORDER_NO = p_order_no;
    -- ��������� ����� ����������� �������� 
    Close_order (
               p_order_id => v_order_id,
               p_date_to  => p_date_to
           );
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,order_no='||p_order_no, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ��������� ��������� ���� � ������, ��� ������� ��������� � ������ ��
-- �� ����� ���� ��������, ��� �� ������ - � ������ � �����, ��� � �� ��������� ������
-- - ��� ������ ���������� ����������
PROCEDURE Bind_rateplan (
               p_rateplan_id   IN INTEGER, -- ID ��������� �����
               p_order_id      IN INTEGER, -- ID ������ - ������
               p_order_body_id IN INTEGER DEFAULT NULL -- ID ���� ������ - ���������� ������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bind_rateplan';
    v_count      INTEGER;
BEGIN
    -- ��������� �� � ����� ������
    IF p_order_body_id IS NULL THEN
        UPDATE ORDER_T O SET O.RATEPLAN_ID = p_rateplan_id
         WHERE O.ORDER_ID = p_order_id
           AND EXISTS (   -- ��������� �� ������������ ����� �� � ������
               SELECT 1 FROM RATEPLAN_T R
                 WHERE R.RATEPLAN_ID = p_rateplan_id
                   AND R.SERVICE_ID  = O.SERVICE_ID
           )
        ;
        v_count := SQL%ROWCOUNT;
        IF v_count = 0 THEN 
            RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '�������� �� ���������');
        END IF;   
    ELSE
        -- ������������ �� � ���������� ������
        UPDATE ORDER_BODY_T OB SET OB.RATEPLAN_ID = p_rateplan_id
         WHERE OB.ORDER_ID = p_order_id
           AND EXISTS (   -- ��������� �� ������������ ���������� ������ �� � ������
               SELECT 1 FROM RATEPLAN_T R
                 WHERE R.RATEPLAN_ID = p_rateplan_id
                   AND R.SUBSERVICE_ID  = OB.SUBSERVICE_ID
           )
        ;
        v_count := SQL%ROWCOUNT;
        IF v_count = 0 THEN 
            RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '�������� �� ���������');
        END IF;
    END IF;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,rateplan_id='||p_rateplan_id||
                                    ',order_id='||p_order_id||
                                    ',order_body_id='||p_order_body_id, 
                                    c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ��������� ��������� ���� IP � ������, ��� ������� ��������� � ������ ��
-- �� ����� ���� ��������, ��� �� ������ - � ������ � �����, ��� � �� ��������� ������
-- - ��� ������ ���������� ����������
--  !!! �������������� �������� � rate_rule_id !!!
    /*
        ��������� ID ������ (����������-�������: IP01_SRV_DCT):
        p_LIPSRV_ID         service_id    subservice_id  rate_rule_id    
                                                

            1    IP_VOLUME      104         39               2407--         �� ������ �������
            2    IP_VPN         106         39               2408--         VPN
            3    IP_BURST       104,149     40               2409--         BURST
            4    IP_RT_VFIX     104         31               2410--         ��� ���������� ����������� � ����.��������� ����� � ������
            5    IP_RT_VBURST   104         31               2410--         ��� ���������� ����������� � ����������� ��������� �����
            6    IP_EPL_BURST   133         40               2411--         EPL BURST
            7    IP_FIX_VOLIN   104         --               2406           ������������� � ������ ������ ���������� �������
            8    IP_BURST_VOLIN 104                  2419(rec) 2418(usg)    BURST � ������ ������ ���������� �������
                                                              
      */
PROCEDURE Bind_rateplan_IP (
               p_rateplan_id   IN INTEGER, -- ID ��������� �����
               p_order_id      IN INTEGER, -- ID ������ - ������
               p_order_body_id IN INTEGER --DEFAULT NULL -- ID ���� ������ - ���������� ������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bind_rateplan_IP';
    v_count      INTEGER;
    v_RATE_RULE_ID_REC number:=null;
    v_RATE_RULE_ID_USG number:=null;
    v_LIPSRV_ID  number;
    --
    r_ORDER_BODY_T       ORDER_BODY_T%rowtype;
    r_ORDER_BODY_T_REC   ORDER_BODY_T%rowtype;
    r_ORDER_BODY_T_USG   ORDER_BODY_T%rowtype;
BEGIN
     
    -- ��������� �� � ����� ������
    IF p_order_body_id IS NULL THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '�� ������ ��������   p_order_body_id');
        --
        UPDATE ORDER_T O SET O.RATEPLAN_ID = p_rateplan_id
         WHERE O.ORDER_ID = p_order_id
--           AND EXISTS (   -- ��������� �� ������������ ����� �� � ������
--               SELECT 1 FROM RATEPLAN_T R
--                 WHERE R.RATEPLAN_ID = p_rateplan_id
--                   AND R.SERVICE_ID  = O.SERVICE_ID
--           )
        ;
        v_count := SQL%ROWCOUNT;
        IF v_count = 0 THEN 
            RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '�������� �� ���������');
        END IF;   
    ELSE

        -- ����� ��� ��������� ����� � RATE_RULE_ID  
        begin
            select LIPSRV_ID into v_LIPSRV_ID
            from TARIFF_IP.IP03_TRF_HEADER_ADD h
            where H.RATEPLAN_ID = p_rateplan_id;
            --
            if (v_LIPSRV_ID = 1) then       --1    IP_VOLUME      104         39               2407--         �� ������ �������
                v_RATE_RULE_ID_REC := null;
                v_RATE_RULE_ID_USG := 2407;
            elsif (v_LIPSRV_ID = 2) then    --2    IP_VPN         106         39               2408--         VPN
                v_RATE_RULE_ID_REC := null;
                v_RATE_RULE_ID_USG := 2408;
            elsif (v_LIPSRV_ID = 3) then    --3    IP_BURST       104,149     40               2409--         BURST
                v_RATE_RULE_ID_REC := null;
                v_RATE_RULE_ID_USG := 2409;
            elsif (v_LIPSRV_ID = 4) then    --4    IP_RT_VFIX     104         31               2410--         ��� ���������� ����������� � ����.��������� ����� � ������
                v_RATE_RULE_ID_REC := null;
                v_RATE_RULE_ID_USG := 2410;
            elsif (v_LIPSRV_ID = 5) then    --5    IP_RT_VBURST   104         31               2410--         ��� ���������� ����������� � ����������� ��������� �����
                v_RATE_RULE_ID_REC := null;
                v_RATE_RULE_ID_USG := 2410;
            elsif (v_LIPSRV_ID = 6) then    --6    IP_EPL_BURST   133         40               2411--         EPL BURST
                v_RATE_RULE_ID_REC := null;
                v_RATE_RULE_ID_USG := 2411;
            elsif (v_LIPSRV_ID = 7) then    --7    IP_FIX_VOLIN   104         --               2406           ������������� � ������ ������ ���������� �������
                v_RATE_RULE_ID_REC := 2406;
                v_RATE_RULE_ID_USG := null;
            elsif (v_LIPSRV_ID = 8) then    --8    IP_BURST_VOLIN 104                  2419(rec) 2418(usg)    BURST � ������ ������ ���������� �������
                v_RATE_RULE_ID_REC := 2419;
                v_RATE_RULE_ID_USG := 2418;
            else
                RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '���������������� ��� ������');
            end if;            
            
        exception when no_data_found then
            RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '�� ������ ����� p_rateplan_id='||p_rateplan_id);
            --v_LIPSRV_ID:=null; 
            --v_RATE_RULE_ID:=null;
        end;
        
        -- ������ ��������� order_body
        -- � ���� order_body  (rec+usg)
        begin
            select * into r_ORDER_BODY_T from ORDER_BODY_T OB where OB.ORDER_BODY_ID = p_order_body_id;
            --
            begin
                select * into r_ORDER_BODY_T_REC from ORDER_BODY_T OB 
                where 
                        OB.ORDER_ID = r_ORDER_BODY_T.ORDER_ID
                    and r_ORDER_BODY_T.date_from between OB.date_from and nvl(ob.date_to,OB.date_from)
                    and OB.CHARGE_TYPE = 'REC' 
                ;
            exception when no_data_found then
                r_ORDER_BODY_T_REC.ORDER_BODY_ID := null;
            end;
            begin
                select * into r_ORDER_BODY_T_USG from ORDER_BODY_T OB 
                where 
                        OB.ORDER_ID = r_ORDER_BODY_T.ORDER_ID
                    and r_ORDER_BODY_T.date_from between OB.date_from and nvl(ob.date_to,OB.date_from)
                    and OB.CHARGE_TYPE = 'USG' 
                ;
            exception when no_data_found then
                r_ORDER_BODY_T_USG.ORDER_BODY_ID := null;
            end;
        end;
        
        -- �������� ����� � rate_rule
        
        if r_ORDER_BODY_T_USG.ORDER_BODY_ID is not null then
            
            UPDATE ORDER_BODY_T OB 
                SET 
                    OB.RATEPLAN_ID  = p_rateplan_id,
                    OB.RATE_RULE_ID = v_RATE_RULE_ID_USG
             WHERE OB.ORDER_ID = p_order_id
                   and 
                   OB.ORDER_BODY_ID = r_ORDER_BODY_T_USG.ORDER_BODY_ID
             ; 
        end if;
        
        if (r_ORDER_BODY_T_REC.ORDER_BODY_ID is not null) and (v_RATE_RULE_ID_REC is not null) then
            
            UPDATE ORDER_BODY_T OB 
                SET 
                    OB.RATEPLAN_ID  = p_rateplan_id,
                    OB.RATE_RULE_ID = v_RATE_RULE_ID_REC
             WHERE OB.ORDER_ID = p_order_id
                   and 
                   OB.ORDER_BODY_ID = r_ORDER_BODY_T_REC.ORDER_BODY_ID
             ; 
        end if;
        
    END IF;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,rateplan_id='||p_rateplan_id||
                                    ',order_id='||p_order_id||
                                    ',order_body_id='||p_order_body_id, 
                                    c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ��������� ��������� ��������� ���� � ������
-- - ��� ������ ���������� ����������
PROCEDURE Bind_agent_rateplan (
               p_rateplan_id   IN INTEGER, -- ID ��������� �����
               p_order_id      IN INTEGER  -- ID ������ - ������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bind_agent_rateplan';
BEGIN
    -- ��������� �� � ����� ������
    UPDATE ORDER_T O
       SET O.AGENT_RATEPLAN_ID = p_rateplan_id
     WHERE O.ORDER_ID = p_order_id;
    --
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,rateplan_id='||p_rateplan_id||
                                    ',order_id='||p_order_id, 
                                    c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������� �����, ������� ��� ������������ ������� �������
PROCEDURE Set_parent_order (
               p_order_id    IN INTEGER, -- ID ������ - ������
               p_parent_id   IN INTEGER  -- ID ������, ������� ��� ������������ ������� �������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_parent_order';
BEGIN
    -- ��������� �� � ����� ������
    UPDATE ORDER_T O
       SET O.PARENT_ID = p_parent_id
     WHERE O.ORDER_ID  = p_order_id;
    --
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, parent_id='||p_parent_id||
                                    ' ,order_id='||p_order_id, 
                                    c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ��������� �������� ����� ������� �������������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
PROCEDURE Close_prev_fixrate (
               p_order_id      IN INTEGER, -- ID ������ - ������
               p_subservice_id IN INTEGER, -- ID ���������� ������
               p_charge_type   IN VARCHAR, -- ��� ��������� (REC, MIN, ...)
               p_date_from     IN DATE     -- ���� � ������� ��������� (������ ������ ���������� ������)
           )  
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Close_prev_fixrate';
    v_subservice_id INTEGER;
    v_order_body_id INTEGER;
    v_date_from     DATE;
    v_date_to       DATE;
BEGIN
    --
    -- ��������� ���� �� �������� ��������� ������ ��� ��������� �� ������
    SELECT ORDER_BODY_ID, DATE_FROM, DATE_TO
      INTO v_order_body_id, v_date_from, v_date_to
      FROM ORDER_BODY_T OB
     WHERE OB.ORDER_ID = p_order_id
       AND OB.SUBSERVICE_ID = p_subservice_id
       AND OB.SUBSERVICE_ID = v_subservice_id
       AND OB.CHARGE_TYPE   = p_charge_type
       AND OB.DATE_FROM    <= p_date_from 
       AND (OB.DATE_TO IS NULL OR p_date_from <= OB.DATE_TO);
    --
    -- ��������� ���������� ������ ������� ������ 
    -- ��� ������� ��, ���� ��� ������� � ������� ������
    IF v_date_from < TRUNC(p_date_from, 'mm') THEN
        v_date_from := TRUNC(p_date_from, 'mm');
        IF v_date_from <= v_date_to THEN
            v_date_to := v_date_from - 1/86400;
        END IF;
        UPDATE ORDER_BODY_T
           SET DATE_TO = v_date_to
         WHERE ORDER_BODY_ID = v_order_body_id;
    ELSE  -- ���������� ������ ������� � ������� ������, ������ ������� ��
        DELETE FROM ORDER_BODY_T WHERE ORDER_BODY_ID = v_order_body_id;
    END IF;
    --
EXCEPTION 
    WHEN NO_DATA_FOUND THEN
        NULL;
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ���������� �������������� ���������� ��� IP-������, ������: 
-- - IP access service_id = 104
-- - EPL service_id = 133
-- - NPL,KLLM service_id = 101
-- ������������ ��� ��� ������ ����������� ����������� � �����
-- - ��� ������ ���������� ����������
PROCEDURE Set_ip_channel_info (
               p_order_id      IN INTEGER,  -- ID ������ - ������
               p_point_src     IN VARCHAR2, -- 
               p_point_dst     IN VARCHAR2,
               p_speed_value   IN NUMBER,
               p_speed_unit_id IN INTEGER
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Set_ip_channel_info';
BEGIN
    MERGE INTO ORDER_INFO_T I
    USING (
        SELECT p_order_id                 ORDER_ID, 
               p_point_src                POINT_SRC, 
               p_point_dst                POINT_DST, 
               p_speed_value              SPEED_VALUE, 
               p_speed_unit_id            SPEED_UNIT_ID,
               p_speed_value||' '||D.NAME SPEED_STR
          FROM DICTIONARY_T D
         WHERE D.KEY_ID = p_speed_unit_id
    ) D
    ON(
        I.ORDER_ID = D.ORDER_ID
    )
    WHEN MATCHED THEN UPDATE SET I.POINT_SRC    = D.POINT_SRC, 
                                 I.POINT_DST    = D.POINT_DST, 
                                 I.SPEED_STR    = D.SPEED_STR,
                                 I.SPEED_VALUE  = D.SPEED_VALUE,
                                 I.SPEED_UNIT_ID= D.SPEED_UNIT_ID
    WHEN NOT MATCHED THEN INSERT (I.ORDER_ID, I.POINT_SRC, I.POINT_DST, 
                                  I.SPEED_STR, I.SPEED_VALUE, I.SPEED_UNIT_ID )
                          VALUES( p_order_id, D.POINT_SRC, D.POINT_DST, 
                                  D.SPEED_STR, D.SPEED_VALUE, D.SPEED_UNIT_ID )
    ;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, order_bodyid='||p_order_id, 
                                    c_PkgName||'.'||v_prcName );
END;
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ���������� � ���� IP VPN: 
-- - ��� ������ ���������� ����������
PROCEDURE Set_vpn_zone (
               p_order_id      IN INTEGER,  -- ID ������
               p_zone          IN VARCHAR2 
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Set_vpn_zone';
BEGIN
    UPDATE ORDER_INFO_T
    SET ROUTER_ZONE = p_zone
    WHERE ORDER_ID = p_order_id;
       
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, order_id='||p_order_id, 
                                    c_PkgName||'.'||v_prcName );
END;



-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ���������� ������������� ���������� ���� ��� ������� � ������� �����  
-- - ��� ������ ���������� ����������
PROCEDURE Set_network_id (
               p_order_id      IN INTEGER,  -- ID ������ - ������
               p_network_id    IN INTEGER 
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Set_network_id';
    v_order_body_id INTEGER;
    v_date_from     DATE;
BEGIN
    MERGE INTO ORDER_INFO_T N
    USING (
        SELECT p_order_id   ORDER_ID, 
               p_network_id NETWORK_ID
          FROM DUAL
    ) D
    ON(
        N.ORDER_ID = D.ORDER_ID
    )
    WHEN MATCHED THEN UPDATE SET N.NETWORK_ID = D.NETWORK_ID
    WHEN NOT MATCHED THEN INSERT (N.ORDER_ID, N.NETWORK_ID)
                          VALUES( D.ORDER_ID, D.NETWORK_ID )
    ;
   
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, order_id='||p_order_id, 
                                    c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ���������� ������������� ����������� ��� ������� � ������� �����  
-- 
PROCEDURE Set_switch_id (
               p_order_id      IN INTEGER,  -- ID ������
               p_switch_id    IN INTEGER 
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Set_switch_id';
BEGIN
    UPDATE ORDER_INFO_T
    SET SWITCH_ID = p_switch_id
    WHERE ORDER_ID = p_order_id;
       
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, order_id='||p_order_id, 
                                    c_PkgName||'.'||v_prcName );
END;



-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ��������� ��������� �������������� �����, ���������� ��������
-- - ��� ������ ���������� ����������
PROCEDURE Set_manager (
               p_order_id   IN INTEGER,
               p_manager_id IN INTEGER,
               p_date_from  IN DATE DEFAULT SYSDATE
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_manager';
    v_date_from  DATE := TRUNC(p_date_from);
BEGIN
    -- ��������� ���������� ������
    UPDATE SALE_CURATOR_T
       SET DATE_TO = v_date_from - 1/86400
     WHERE MANAGER_ID != p_manager_id
       AND ORDER_ID = p_order_id
       AND DATE_TO IS NULL;
    -- ��������� ����� ������
    INSERT INTO SALE_CURATOR_T (MANAGER_ID, ORDER_ID, DATE_FROM)
    VALUES(p_manager_id, p_order_id, v_date_from);
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ID ��������� �������������� �����, ���������� ��������
-- - ������������� - ID ���������
-- - NULL - ��� ������
-- - ��� ������ ���������� ����������
FUNCTION Get_manager_id (
               p_order_id  IN INTEGER,
               p_date      IN DATE DEFAULT SYSDATE
           ) RETURN INTEGER 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_manager_id';
    v_manager_id INTEGER;
BEGIN
    SELECT MANAGER_ID INTO v_manager_id
      FROM SALE_CURATOR_T
     WHERE ORDER_ID = p_order_id
       AND p_date BETWEEN DATE_FROM AND DATE_TO;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- -------------------------------------------------------------------- --
-- ������ � ���������� ������
-- -------------------------------------------------------------------- --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ��������� ������ �� �����, ���������� ��������
-- - ������������� - ORDER_BODY_ID
-- - ��� ������ ���������� ����������
FUNCTION Add_subservice (
               p_order_id      IN INTEGER,
               p_subservice_id IN INTEGER,
               p_charge_type   IN VARCHAR2,
               p_rateplan_id   IN INTEGER DEFAULT NULL,
               p_date_from     IN DATE DEFAULT SYSDATE,
               p_date_to       IN DATE DEFAULT  Pk00_Const.c_DATE_MAX,
               p_notes         IN VARCHAR2 DEFAULT NULL,
               p_currency_id   IN INTEGER DEFAULT Pk00_Const.c_CURRENCY_RUB
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Add_subservice';
    v_date_from     DATE := TRUNC(p_date_from);
    v_date_to       DATE;
    v_order_body_id INTEGER;
    v_count         INTEGER;
    v_charge_type   VARCHAR2(25) := p_charge_type;    
BEGIN
    v_date_to := NVL(p_date_to,Pk00_Const.c_DATE_MAX);

    IF v_date_to < v_date_from THEN
        Pk01_Syslog.Raise_user_exception(
          p_Msg => 'Date_To '||TO_CHAR(v_date_to,'dd.mm.yyyy')||
                   ' less then date_from '||TO_CHAR(v_date_from,'dd.mm.yyyy'),
          p_Src => c_PkgName||'.'||v_prcName);    
    END IF;

    IF p_charge_type IS NULL THEN
        SELECT CHARGE_TYPE into v_charge_type
               FROM subservice_t
           WHERE SUBSERVICE_ID = p_subservice_id;
    END IF;

    -- ���������, ������� �������� ���������� ������ �� ������
    SELECT COUNT(*) INTO v_count
      FROM ORDER_BODY_T OB
     WHERE OB.ORDER_ID = p_order_id
       AND OB.SUBSERVICE_ID = p_subservice_id
       AND OB.CHARGE_TYPE   = v_charge_type
       AND (
           OB.DATE_FROM BETWEEN p_date_from AND p_date_to
        OR 
           OB.DATE_TO BETWEEN p_date_from AND p_date_to
       );
    -- ��������� ���� �� ������
    IF v_count > 0 THEN
        Pk01_Syslog.Raise_user_exception(
          p_Msg => 'subservice'||p_subservice_id||' already exists on order_id='||p_order_id,
          p_Src => c_PkgName||'.'||v_prcName);
    END IF;
    -- ��������� ����� ������
    INSERT INTO ORDER_BODY_T (
        ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
        RATEPLAN_ID, DATE_FROM, DATE_TO, CREATE_DATE, MODIFY_DATE, NOTES
    )VALUES(
        SQ_ORDER_ID.NEXTVAL, p_order_id, p_subservice_id, v_charge_type, 
        p_rateplan_id, v_date_from, v_date_to, 
        SYSDATE, SYSDATE, p_notes
    ) RETURNING ORDER_BODY_ID INTO v_order_body_id;
    RETURN v_order_body_id;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,order_id='||p_order_id||
                                    ',subservice_id='||p_subservice_id, 
                                    c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������� ��������� ������ �� ������, ���������� ��������
-- - ��� ������ ���������� ����������
PROCEDURE Close_subservice (
               p_order_id      IN INTEGER,
               p_subservice_id IN INTEGER,
               p_date_to       IN DATE DEFAULT SYSDATE
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_subservice';
    v_date_to    DATE := TRUNC(p_date_to)+1-1/86400;
BEGIN
    -- ��������� ��������� ������
    UPDATE ORDER_BODY_T
       SET DATE_TO = v_date_to
     WHERE ORDER_ID = p_order_id
       AND SUBSERVICE_ID = p_subservice_id
       AND (DATE_TO IS NULL OR v_date_to < DATE_TO);
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,order_id='||p_order_id||
                                    ',subservice_id='||p_subservice_id, 
                                    c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������ ��������� ��� ������ ��� ���������� ������ ������
-- - ���������� ID ������ ORDER_BODY_T
-- - ��� ������ ���������� ����������
FUNCTION Add_subs_abon (
               p_order_id      IN INTEGER, -- ID ������ - ������
               p_subservice_id IN INTEGER, -- ID ���������� ������
               p_value         IN NUMBER,  -- ����� ���������
               p_tax_incl      IN CHAR,    -- ������� �� ����� � ����� ���������
               p_currency_id   IN INTEGER, -- ������
               p_quantity      IN NUMBER,  -- ���-�� ������ � ����������� ���������
               p_date_from     IN DATE,
               p_date_to       IN DATE DEFAULT Pk00_Const.c_DATE_MAX
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Add_subs_abon';
    v_subservice_id INTEGER;
    v_order_body_id INTEGER;
    v_date_from     DATE;
BEGIN
    -- ���� ��������� ������ �� �����, ����������� �����������
    IF p_subservice_id IS NOT NULL THEN
        v_subservice_id := p_subservice_id;
    ELSE
        v_subservice_id := Pk00_Const.c_SUBSRV_ABP;
    END IF;
    -- ��������� �������� ����� ������� �������������� �������
    Close_prev_fixrate (
               p_order_id      => p_order_id,      -- ID ������ - ������
               p_subservice_id => v_subservice_id, -- ID ���������� ������
               p_charge_type   => Pk00_Const.c_CHARGE_TYPE_REC, -- ��� ��������� (REC, MIN, ...)
               p_date_from     => p_date_from      -- ���� � ������� ��������� (������ ������ ���������� ������)
           );

    -- ��������� ������ c �������� ���������
    v_order_body_id := SQ_ORDER_ID.NEXTVAL;

--    v_date_from     := TRUNC(p_date_from, 'mm');         LL (�� 20.05.2015)
    v_date_from     := p_date_from;
    
    INSERT INTO ORDER_BODY_T(
        ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
        DATE_FROM, DATE_TO, RATE_VALUE, RATE_LEVEL_ID, TAX_INCL, CURRENCY_ID,
        QUANTITY, RATE_RULE_ID
    )
    SELECT v_order_body_id, O.ORDER_ID, p_subservice_id, Pk00_Const.c_CHARGE_TYPE_REC,
           CASE
             WHEN v_date_from < O.DATE_FROM THEN O.DATE_FROM
             ELSE v_date_from
           END DATE_FROM, p_date_to,
           p_value, Pk00_Const.c_RATE_LEVEL_ORDER, p_tax_incl, p_currency_id,
           p_quantity, Pk00_Const.c_RATE_RULE_ABP_STD
      FROM ORDER_T O
     WHERE O.ORDER_ID = p_order_id
       AND O.DATE_FROM <= v_date_from
       AND (O.DATE_TO IS NULL OR v_date_from < O.DATE_TO);
    --
    RETURN v_order_body_id;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, order_id='||p_order_id||
                                    ', order_body_id='||v_order_body_id||
                                    ',subservice_id='||p_subservice_id, 
                                    c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������ ��������� ��� ��������� ������ (������)
-- - ���������� ID ������ ORDER_BODY_T
-- - ��� ������ ���������� ����������
FUNCTION Add_subs_abon_voice (
                   p_order_id      IN INTEGER, -- ID ������ - ������
                   p_subservice_id IN INTEGER, -- ID ���������� ������
                   p_value         IN NUMBER,  -- ����� ���������
                   p_tax_incl      IN CHAR,    -- ������� �� ����� � ����� ���������
                   p_currency_id   IN INTEGER, -- ������
                   p_free_traffic  IN NUMBER,  -- ���-�� ������ � ����������� ���������
                   p_date_from     IN DATE DEFAULT SYSDATE
               ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Add_subs_abon_voice';
    v_quantity      INTEGER := 1; -- ���-�� ������ (������ �������)
    v_subservice_id INTEGER;
    v_order_body_id INTEGER;
    v_date_from     DATE;
BEGIN
    -- ���� ��������� ������ �� �����, ����������� �����������
    IF p_subservice_id IS NOT NULL THEN
        v_subservice_id := p_subservice_id;
    ELSE
        v_subservice_id := Pk00_Const.c_SUBSRV_ABP;
    END IF;  
    -- ��������� �������� ����� ������� �������������� �������
    Close_prev_fixrate (
               p_order_id      => p_order_id,      -- ID ������ - ������
               p_subservice_id => v_subservice_id, -- ID ���������� ������
               p_charge_type   => Pk00_Const.c_CHARGE_TYPE_REC, -- ��� ��������� (REC, MIN, ...)
               p_date_from     => p_date_from      -- ���� � ������� ��������� (������ ������ ���������� ������)
           );

    -- ��������� ������ c ��������� ���������
    v_order_body_id := SQ_ORDER_ID.NEXTVAL;
    v_date_from     := p_date_from;
    INSERT INTO ORDER_BODY_T(
        ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
        DATE_FROM, DATE_TO, RATE_VALUE, FREE_VALUE, RATE_LEVEL_ID, TAX_INCL, CURRENCY_ID,
        QUANTITY, RATE_RULE_ID
    )
    SELECT v_order_body_id, O.ORDER_ID, p_subservice_id, Pk00_Const.c_CHARGE_TYPE_REC,
           CASE
             WHEN v_date_from < O.DATE_FROM THEN O.DATE_FROM
             ELSE v_date_from
           END DATE_FROM, Pk00_Const.c_DATE_MAX,
           p_value, p_free_traffic, Pk00_Const.c_RATE_LEVEL_ORDER, p_tax_incl, p_currency_id, 
           v_quantity, Pk00_Const.c_RATE_RULE_ABP_FREE_MIN
      FROM ORDER_T O
     WHERE O.ORDER_ID = p_order_id
       AND O.DATE_FROM <= v_date_from
       AND (O.DATE_TO IS NULL OR v_date_from < O.DATE_TO);
    --
    RETURN v_order_body_id;
    --
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, order_id='||p_order_id, 
                                    c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������ ������ ����������� ����� ������ ������ ��� ������
-- - ���������� ID ������ ORDER_BODY_T
-- - ��� ������ ���������� ����������
FUNCTION Add_subs_min (
               p_order_id      IN INTEGER, -- ID ������ - ������
               p_subservice_id IN INTEGER, -- ID ���������� ������
               p_value         IN NUMBER,  -- ����� ���������
               p_tax_incl      IN CHAR,    -- ������� �� ����� � ����� ���������
               p_currency_id   IN INTEGER, -- ������
               p_rate_level_id IN INTEGER, -- ������� ��������: ���������/�����/������� ����
               p_date_from     IN DATE DEFAULT SYSDATE
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Add_subs_min';
    v_subservice_id INTEGER;
    v_order_body_id INTEGER;
    v_rate_level_id INTEGER;
    v_date_from     DATE;
BEGIN
    -- ���� ��������� ������ �� �����, ����������� �����������
    IF p_subservice_id IS NOT NULL THEN
        v_subservice_id := p_subservice_id;
    ELSE
        v_subservice_id := Pk00_Const.c_SUBSRV_MIN;
    END IF;
    -- ��������� �� ����������� ��������� ������
    IF p_rate_level_id IN (Pk00_Const.c_RATE_LEVEL_SUBSRV, 
                           Pk00_Const.c_RATE_LEVEL_ORDER, 
                           Pk00_Const.c_RATE_LEVEL_ACCOUNT)
    THEN
        v_rate_level_id := p_rate_level_id;
    ELSE -- ����� � ���������� ���������, �� ���� ������ �������� �� ���������
        v_rate_level_id := Pk00_Const.c_RATE_LEVEL_ORDER;
    END IF;
    -- ��������� �������� ����� ������� �������������� �������
    Close_prev_fixrate (
               p_order_id      => p_order_id,      -- ID ������ - ������
               p_subservice_id => v_subservice_id, -- ID ���������� ������
               p_charge_type   => Pk00_Const.c_CHARGE_TYPE_MIN, -- ��� ��������� (REC, MIN, ...)
               p_date_from     => p_date_from      -- ���� � ������� ��������� (������ ������ ���������� ������)
           );

    -- ��������� ������ c �������� ���������
    v_order_body_id := SQ_ORDER_ID.NEXTVAL;
    v_date_from     := p_date_from;
    INSERT INTO ORDER_BODY_T(
        ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
        DATE_FROM, DATE_TO, RATE_VALUE, RATE_LEVEL_ID, TAX_INCL, CURRENCY_ID,
        RATE_RULE_ID, CREATE_DATE, MODIFY_DATE
    )
    SELECT v_order_body_id, O.ORDER_ID, p_subservice_id, Pk00_Const.c_CHARGE_TYPE_MIN,
           CASE
             WHEN v_date_from < O.DATE_FROM THEN O.DATE_FROM
             ELSE v_date_from
           END DATE_FROM, Pk00_Const.c_DATE_MAX,
           p_value, v_rate_level_id, p_tax_incl, p_currency_id,
           Pk00_Const.c_RATE_RULE_MIN_STD,
           SYSDATE,
           SYSDATE
      FROM ORDER_T O
     WHERE O.ORDER_ID = p_order_id
       AND O.DATE_FROM <= v_date_from
       AND (O.DATE_TO IS NULL OR v_date_from < O.DATE_TO);
    --
    RETURN v_order_body_id;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, order_id='||p_order_id||
                                    ',subservice_id='||p_subservice_id, 
                                    c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������� ��� ��� ������ ���������� ����������� �������� �
-- - ��� ������ ���������� ����������
PROCEDURE Add_subs_downtime (
               p_order_id      IN INTEGER, -- ID ������ - ������
               p_charge_type   IN VARCHAR2,
               p_free_value    IN NUMBER,  -- ���-�� ���������������� ����� ��������
               p_descr         IN VARCHAR2,
               p_date_from     IN DATE DEFAULT SYSDATE
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Add_subs_downtime';
    v_order_body_id INTEGER;
    v_date_from     DATE;
BEGIN
    --
    v_date_from := p_date_from;
    --
    -- �������� ��������� ������
    MERGE INTO ORDER_INFO_T I
    USING (
          SELECT I.ORDER_ID
            FROM ORDER_INFO_T I
           WHERE I.ORDER_ID = p_order_id
    ) O
    ON (
       I.ORDER_ID = O.ORDER_ID
    )
    WHEN MATCHED THEN UPDATE SET I.DOWNTIME_FREE = p_free_value
    WHEN NOT MATCHED THEN INSERT (I.ORDER_ID, I.DOWNTIME_FREE) 
                          VALUES (p_order_id, p_free_value);
    -- ---------------------------------------------------------------
    -- ��������� ����� ��������
    -- ���������� �������� ����� ����������� �� ���� �������������, 
    -- ��� �����������
    -- ������� ��� SLA
    -- ---------------------------------------------------------------
    IF p_charge_type != Pk00_Const.c_CHARGE_TYPE_IDL THEN
        --
        -- ��������� �������� ����� ������� �������������� �������
        Close_prev_fixrate (
                   p_order_id      => p_order_id,      -- ID ������ - ������
                   p_subservice_id => Pk00_Const.c_SUBSRV_IDL, -- ID ���������� ������
                   p_charge_type   => p_charge_type,   -- ��� ��������� (REC, MIN, ...)
                   p_date_from     => v_date_from      -- ���� � ������� ��������� (������ ������ ���������� ������)
               );
               
        -- ��������� ����� ������
        v_order_body_id := SQ_ORDER_ID.NEXTVAL;
        INSERT INTO ORDER_BODY_T(
            ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
            DATE_FROM, DATE_TO, RATE_LEVEL_ID, 
            RATE_RULE_ID, FREE_VALUE, NOTES
        )
        SELECT v_order_body_id, O.ORDER_ID, Pk00_Const.c_SUBSRV_IDL, p_charge_type,
               CASE
                 WHEN v_date_from < O.DATE_FROM THEN O.DATE_FROM
                 ELSE v_date_from
               END DATE_FROM, Pk00_Const.c_DATE_MAX,
               Pk00_Const.c_RATE_LEVEL_ORDER,  
               Pk00_Const.c_RATE_RULE_IDL_STD, NVL(p_free_value, 0), p_descr
          FROM ORDER_T O
         WHERE O.ORDER_ID = p_order_id
           AND O.DATE_FROM <= v_date_from
           AND (O.DATE_TO IS NULL OR v_date_from <= O.DATE_TO);
    
    END IF;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, order_id='||p_order_id, 
                                    c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������� ��� ��� ������ ���������� ����������� �������� �
-- - ��� ������ ���������� ����������
PROCEDURE Add_ob_downtime (
               p_order_id      IN INTEGER, -- ID ������ - ������
               p_charge_type   IN VARCHAR2,
               p_free_value    IN NUMBER,  -- ���-�� ���������������� ����� ��������
               p_descr         IN VARCHAR2,
               p_date_from     IN DATE DEFAULT SYSDATE
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Add_ob_downtime';
    v_order_body_id INTEGER;
BEGIN
    PK06_ORDER.Add_subs_downtime(p_order_id     => p_order_id,
                                 p_charge_type  => p_charge_type,
                                 p_free_value   => p_free_value,
                                 p_descr        => p_descr,
                                 p_date_from    => p_date_from);
 
     -- ��������� ����� ������
    v_order_body_id := SQ_ORDER_ID.NEXTVAL;
    INSERT INTO ORDER_BODY_T(
        ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
        DATE_FROM, DATE_TO, RATE_LEVEL_ID, 
        RATE_RULE_ID, FREE_VALUE, NOTES
    )
    SELECT v_order_body_id, O.ORDER_ID, Pk00_Const.c_SUBSRV_IDL, p_charge_type,
           CASE
             WHEN p_date_from < O.DATE_FROM THEN O.DATE_FROM
             ELSE p_date_from
           END DATE_FROM, Pk00_Const.c_DATE_MAX,
           Pk00_Const.c_RATE_LEVEL_ORDER,  
           Pk00_Const.c_RATE_RULE_IDL_STD, NVL(p_free_value, 0), p_descr
      FROM ORDER_T O
     WHERE O.ORDER_ID = p_order_id
       AND O.DATE_FROM <= p_date_from
       AND (O.DATE_TO IS NULL OR p_date_from <= O.DATE_TO);
       
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, order_id='||p_order_id, 
                                    c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������� ��� ��� ������ ������������� ������
-- - ��� ������ ���������� ����������
PROCEDURE Add_subs_discount (
               p_order_id      IN INTEGER, -- ID ������ - ������
               p_currency_id   IN INTEGER, -- ID ������ �������
               p_date_from     IN DATE DEFAULT SYSDATE
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Add_subs_discount';
    v_order_body_id INTEGER;
    v_date_from     DATE;
    v_count         INTEGER;
BEGIN
    --
    v_date_from := p_date_from;
    -- ��������� ���� �� �������� ������ �� ��������� ������ �������
    SELECT COUNT(*) INTO v_count
      FROM ORDER_BODY_T OB
     WHERE OB.ORDER_ID = p_order_id
       AND OB.CHARGE_TYPE = PK00_CONST.c_CHARGE_TYPE_DIS
       AND OB.SUBSERVICE_ID = PK00_CONST.c_SUBSRV_DISC
       AND OB.DATE_FROM <= v_date_from
       AND (OB.DATE_TO IS NULL OR v_date_from <= OB.DATE_TO);
    --
    IF v_count = 0 THEN
        -- ������� ����� ������� ������
        v_order_body_id := SQ_ORDER_ID.NEXTVAL;
        INSERT INTO ORDER_BODY_T OB (
               ORDER_ID, ORDER_BODY_ID, CHARGE_TYPE, 
               SUBSERVICE_ID, DATE_FROM, DATE_TO, RATE_RULE_ID, 
               TAX_INCL, CURRENCY_ID
        )VALUES(
                p_order_id, v_order_body_id, PK00_CONST.c_CHARGE_TYPE_DIS,
                PK00_CONST.c_SUBSRV_DISC, v_date_from, TO_DATE('01.01.2050', 'dd.mm.yyyy'),
                PK00_CONST.c_RATE_RULE_DIS_STD,
                PK00_CONST.c_RATEPLAN_TAX_NOT_INCL,
                p_currency_id
        );
    ELSE
        -- ���������� ���� �������� ������������ ������
        UPDATE ORDER_BODY_T OB SET DATE_TO = PK00_CONST.c_DATE_MAX
         WHERE OB.ORDER_ID = p_order_id
           AND OB.CHARGE_TYPE = PK00_CONST.c_CHARGE_TYPE_DIS
           AND OB.SUBSERVICE_ID = PK00_CONST.c_SUBSRV_DISC
           AND OB.DATE_FROM <= v_date_from
           AND (OB.DATE_TO IS NULL OR v_date_from <= OB.DATE_TO);
    END IF;

EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, order_id='||p_order_id, 
                                    c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� ��������� �� ���������� �� ������ ��� ����������� �������� ���
-- ������� � ��� ��� - "MS107643/�CCA/353P11"
-- ������ ������, �������� �������� �� �.������������ 16.09.2016
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Get_css_phone_dirrecion(
                              p_order_id IN INTEGER,
                              p_abn_a    IN VARCHAR2,
                              p_descr    IN VARCHAR2
                           ) RETURN VARCHAR2
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Get_css_phone_dirrecion';
    v_css_account_id CONSTANT INTEGER := 1937070; -- �/� ��� ��� - "MS107643/�CCA/353P11"
    v_count          INTEGER;
    v_descr          VARCHAR2(40);
BEGIN
  -- �� ���������, ������ ��� ����
  v_descr := p_descr;
  
  -- ��������� �� �������������� � �������� � ��� ���  
  SELECT COUNT(*) INTO v_count
    FROM ORDER_T O
   WHERE O.ACCOUNT_ID = v_css_account_id
     AND O.ORDER_ID   = p_order_id;
  
  IF v_count > 0 THEN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 4) ����� �� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    SELECT COUNT(*) INTO v_count
      FROM INFRANET.CSS_DESCRIPTION_MAP M
     WHERE M.ANUMBER = p_abn_a;
    IF v_count = 1 THEN
        SELECT M.DESCR 
          INTO v_descr
          FROM INFRANET.CSS_DESCRIPTION_MAP M
         WHERE M.ANUMBER = p_abn_a;
    ELSIF 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3) ����������� �������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        p_abn_a LIKE '749926021%' OR
        p_abn_a LIKE '749926022%' OR
        p_abn_a LIKE '749926023%' OR
        p_abn_a LIKE '749926024%' OR
        p_abn_a LIKE '749926025%' OR
        p_abn_a LIKE '749926026%' OR
        p_abn_a LIKE '749926027%' OR    
        -- 4992609 100 - 4992609 699
        p_abn_a LIKE '749926091%' OR
        p_abn_a LIKE '749926092%' OR
        p_abn_a LIKE '749926093%' OR
        p_abn_a LIKE '749926094%' OR
        p_abn_a LIKE '749926095%' OR
        p_abn_a LIKE '749926096%' OR
        -- 4992609 800 - 4992609 999
        p_abn_a LIKE '749926098%' OR
        p_abn_a LIKE '749926099%'
    THEN
      v_descr := '����������� �������� �����';
    ELSIF
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2) ���������� �������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
       p_abn_a LIKE '7499266%'   OR
       p_abn_a LIKE '7499623%'   OR
       p_abn_a LIKE '74958463%'  OR
       p_abn_a LIKE '74958464%'  OR
       p_abn_a LIKE '74958465%'  OR
       p_abn_a IN  ('74992648564','74964115527')  OR
        -- ������ 22.06.2015 10:47
       p_abn_a IN  ('74991541059','74964635780','74987142645')
    THEN
      v_descr := '���������� �������� �����';

    ELSIF
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1) ��� - ��� �������:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
       p_abn_a LIKE '7495%'    OR
       p_abn_a LIKE '7496%'    OR
       p_abn_a LIKE '7499260%' OR
       p_abn_a LIKE '7499262%'
    THEN
      v_descr := '���';
    END IF;
  END IF;
  --
  RETURN v_descr;
  --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', v_prcName );
END Get_css_phone_dirrecion;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ������� � ������
-- - ������������� - ORDER_BODY_ID
-- - ��� ������ ���������� ����������
PROCEDURE Add_phone (
               p_order_id      IN INTEGER,
               p_phone         IN VARCHAR2,
               p_date_from     IN DATE DEFAULT SYSDATE,
               p_date_to       IN DATE DEFAULT  Pk00_Const.c_DATE_MAX,
               p_descr         IN VARCHAR2 DEFAULT NULL
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Add_phone';
    v_date_from  DATE := TRUNC(p_date_from);
    v_descr      VARCHAR2(40);
BEGIN
    -- ������������ �������� ��� ��������� ���
    v_descr := Get_css_phone_dirrecion(
                              p_order_id => p_order_id,
                              p_abn_a    => p_phone,
                              p_descr    => p_descr
                           );
    -- ��������� ����� ������
    INSERT INTO ORDER_PHONES_T (
        ORDER_ID, PHONE_NUMBER, DATE_FROM, DATE_TO, DESCR
    )VALUES(
        p_order_id, p_phone, v_date_from, p_date_to, v_descr
    );
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,order_id='||p_order_id||
                                    ',phone_number='||p_phone, 
                                    c_PkgName||'.'||v_prcName );
END;
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ������� � ���������� ������
-- - ������������� - ORDER_BODY_ID
-- - ��� ������ ���������� ����������
PROCEDURE Add_phone_ag (
               p_order_id      IN INTEGER,
               p_phone         IN VARCHAR2,
               p_date_from     IN DATE DEFAULT SYSDATE,
               p_date_to       IN DATE DEFAULT  Pk00_Const.c_DATE_MAX,
               p_descr         IN VARCHAR2 DEFAULT NULL
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Add_phone_ag';
    v_date_from  DATE := TRUNC(p_date_from);
    v_descr      VARCHAR2(40);
BEGIN
    -- ������������ �������� ��� ��������� ���
    v_descr := Get_css_phone_dirrecion(
                              p_order_id => p_order_id,
                              p_abn_a    => p_phone,
                              p_descr    => p_descr
                           );
    -- ��������� ����� ������
    INSERT INTO AGENT_PHONES_T (
        ORDER_ID, PHONE_NUMBER, DATE_FROM, DATE_TO, DESCR
    )VALUES(
        p_order_id, p_phone, v_date_from, p_date_to, p_descr
    );
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,order_id='||p_order_id||
                                    ',phone_number='||p_phone, 
                                    c_PkgName||'.'||v_prcName );
END;
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ���������� ������� �����������, ��� ���������� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
PROCEDURE Set_rate_rule (
               p_order_body_id IN INTEGER,
               p_rate_rule_id  IN INTEGER,
               p_currency_id   IN INTEGER,
               p_tax_incl      IN CHAR
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_rate_rule';
BEGIN
    -- �������� ������
    UPDATE ORDER_BODY_T OB 
       SET OB.RATE_RULE_ID = p_rate_rule_id,
           OB.CURRENCY_ID  = p_currency_id,
           OB.TAX_INCL     = p_tax_incl
     WHERE OB.ORDER_BODY_ID = p_order_body_id;
     
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,order_body_id='||p_order_body_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ������ ����������� ������ �� ������
--   - ������������� - ����������� ������
--   - ��� ������ ���������� ����������
--
FUNCTION Subservice_list( 
               p_recordset  OUT t_refc,
               p_order_id   IN INTEGER,
               p_open_only  IN BOOLEAN
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Subservice_list';
    v_retcode    INTEGER := c_RET_OK;
BEGIN
    IF p_open_only = TRUE THEN
        SELECT COUNT(*) INTO v_retcode
          FROM ORDER_BODY_T
         WHERE ORDER_ID = p_order_id
           AND DATE_TO IS NULL;
        -- 
        OPEN p_recordset FOR
             SELECT ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
                    DATE_FROM, DATE_TO, CREATE_DATE 
               FROM ORDER_BODY_T
              WHERE ORDER_ID = p_order_id
                AND DATE_TO IS NULL
              ORDER BY DATE_FROM;
           
    ELSE
        SELECT COUNT(*) INTO v_retcode
          FROM ORDER_BODY_T
         WHERE ORDER_ID = p_order_id;
        -- 
        OPEN p_recordset FOR
             SELECT ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
                    DATE_FROM, DATE_TO, CREATE_DATE 
               FROM ORDER_BODY_T
              WHERE ORDER_ID = p_order_id
              ORDER BY DATE_FROM;
    END IF;
    --
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);  
END;

-- -------------------------------------------------------------------- --
-- ���������� / ������������� ������
-- -------------------------------------------------------------------- --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������������� �����, ���������� ��������
-- - ������������� - ID ������ � ����������
-- - ��� ������ ���������� ����������
FUNCTION Lock_order (
               p_order_id      IN INTEGER,
               p_lock_type_id  IN INTEGER,
               p_manager_login IN VARCHAR2,
               p_date_from     IN DATE DEFAULT SYSDATE,
               p_notes         IN VARCHAR2
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Lock_order';
    v_date_from  DATE := TRUNC(p_date_from);
    v_count      INTEGER := 0;
    v_lock_id    INTEGER;
BEGIN
    -- ������������� ���������� �� �����
    UPDATE ORDER_T O
       SET O.STATUS = Pk00_Const.c_ORDER_STATE_LOCK
     WHERE O.ORDER_ID = p_order_id;
    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '����� ORDER_ID='||p_order_id||' - �� ������');
    END IF;
    -- �� ������ ������ ��������� ��������� ��������� ���������
    UPDATE ORDER_LOCK_T L
       SET L.DATE_TO = p_date_from - 1/86400
     WHERE L.ORDER_ID = p_order_id
       AND (L.DATE_TO IS NULL OR p_date_from <= L.DATE_TO);
    -- ��������� �����
    INSERT INTO ORDER_LOCK_T(
        ORDER_LOCK_ID,ORDER_ID,LOCK_TYPE_ID,DATE_FROM,DATE_TO,CREATE_DATE,LOCKED_BY,LOCK_REASON
    )VALUES(
        SQ_ORDER_ID.NEXTVAL, p_order_id, p_lock_type_id, v_date_from, NULL, SYSDATE, p_manager_login,p_notes
    ) RETURNING ORDER_LOCK_ID INTO v_lock_id;
    --
    RETURN v_lock_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,order_id='||p_order_id,c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������������� �����, ���������� ��������
-- - ������������� - ID ������ � ����������
-- - ��� ������ ���������� ����������
FUNCTION UnLock_order (
               p_order_id      IN INTEGER,
               p_manager_login IN VARCHAR2,
               p_date_to       IN DATE DEFAULT SYSDATE,
               p_notes         IN VARCHAR2
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'UnLock_order';
    v_lock_id    INTEGER;
    v_date_to    DATE := p_date_to;
BEGIN
    IF p_date_to IS NULL THEN 
       v_date_to := Trunc(SYSDATE);  --������ ��� ��������, ������ ��� ��� ��� - � p_date_to �����
    END IF;
    
    -- ������������ �����  
    UPDATE ORDER_T O
       SET O.STATUS = Pk00_Const.c_ORDER_STATE_OPEN
     WHERE O.ORDER_ID = p_order_id;
    -- ��������� ������������� � ������    
    UPDATE ORDER_LOCK_T
       SET DATE_TO = v_date_to,
           UNLOCKED_BY = p_manager_login,
           UNLOCK_REASON = p_notes
     WHERE ORDER_ID = p_order_id
       AND DATE_TO IS NULL
    RETURNING ORDER_LOCK_ID INTO v_lock_id;
    --
    RETURN v_lock_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,order_id='||p_order_id,c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ������ ���������� ������, ���������� ��������
-- - ������������� - ID ���� ����������
-- - NULL - ����� �� ������������
-- - ��� ������ ���������� ����������
FUNCTION GetLock_type (
               p_order_id     IN INTEGER,
               p_date         IN DATE DEFAULT SYSDATE
           ) RETURN INTEGER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'GetLock_type';
    v_lock_type_id INTEGER;
BEGIN
    -- ��������� �� ����������� �� ����������
    SELECT LOCK_TYPE_ID INTO v_lock_type_id
      FROM ORDER_LOCK_T
     WHERE ORDER_ID = p_order_id
       AND DATE_FROM <= p_date
       AND (p_date <= DATE_TO OR DATE_TO IS NULL);
    --
    RETURN v_lock_type_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR,order_id='||p_order_id,c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ������������� ���� ����������/�������������
PROCEDURE UpdateLockDates(p_lock_id IN INTEGER, p_date_from IN DATE, p_date_to IN DATE)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'UpdateLockDates';
    v_retcode       INTEGER;
BEGIN
  UPDATE ORDER_LOCK_T
  SET DATE_FROM = p_date_from,
      DATE_TO = p_date_to
  WHERE ORDER_LOCK_ID = p_lock_id;
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- ������, ������� �������� �� ��, ��� ����� ����������� �����,
-- ����� ��� ������� ��������� �� XML, ��������� ��� ��� ��������� �.�.�����:
-- ACC xxx xxx xxx - nn
-- (������ ������ ��������:: YY LD x xxx xxx - �� ����������)
FUNCTION Make_order_No (p_account_no IN VARCHAR2) RETURN VARCHAR2
IS
    v_order_no   ORDER_T.ORDER_NO%TYPE;
    v_order_max  VARCHAR2(100);
BEGIN
    SELECT MAX(GET_NUMBER_FROM_ORDER_NO(o.ORDER_NO)) INTO v_order_max
      FROM ACCOUNT_T A, ORDER_T O
     WHERE A.ACCOUNT_ID = O.ACCOUNT_ID
       AND A.ACCOUNT_NO = p_account_no;
       
    IF v_order_max IS NULL THEN
      v_order_max := 1;
    ELSE
      v_order_max := v_order_max + 1;
    END IF;
    
    v_order_no := p_account_no || '-' || v_order_max;    
    RETURN v_order_no;
END;

FUNCTION Get_number_from_order_no(p_order_no IN VARCHAR2) RETURN INTEGER
IS
   v_order_no_number      VARCHAR2(100);
   v_order_no_number_int  INTEGER;
BEGIN
   SELECT SUBSTR(p_order_no,INSTR(p_order_no, '-', -1)+1)INTO v_order_no_number FROM DUAL;
   BEGIN
          v_order_no_number_int := TO_NUMBER(v_order_no_number);          
          RETURN v_order_no_number_int;
   EXCEPTION WHEN OTHERS THEN
          RETURN NULL;
   END;      
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ������� ���� �������, �� ������ ��
-- �� ������ ������ ������������ ������, �� �������� "MOVED"
--
PROCEDURE Refresh_statuses
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Refresh_statuses';
    v_count    INTEGER;
BEGIN
    -- ��������� �� ����������� �� ���������� � ������ �� �����
    UPDATE ORDER_T O SET O.STATUS = Pk00_Const.c_ORDER_STATE_OPEN
    WHERE NOT EXISTS (
      SELECT * FROM ORDER_LOCK_T L
       WHERE O.ORDER_ID = L.ORDER_ID
         AND L.DATE_FROM < SYSDATE
         AND (L.DATE_TO IS NULL OR SYSDATE < L.DATE_TO ) 
    )
    AND (O.DATE_TO IS NULL OR SYSDATE < O.DATE_TO )
    AND O.STATUS != Pk00_Const.c_ORDER_STATE_MOVED;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Set status OPEN for '||v_count||' orders' ,c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� �� ����������� �� ���������� � ������ �� �����
    UPDATE ORDER_T O SET O.STATUS = Pk00_Const.c_ORDER_STATE_CLOSED
    WHERE NOT EXISTS (
      SELECT * FROM ORDER_LOCK_T L
       WHERE O.ORDER_ID = L.ORDER_ID
         AND L.DATE_FROM < SYSDATE
         AND (L.DATE_TO IS NULL OR SYSDATE < L.DATE_TO ) 
    )
    AND O.DATE_TO < SYSDATE
    AND O.STATUS != Pk00_Const.c_ORDER_STATE_MOVED;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Set status CLOSED for '||v_count||' orders' ,c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� ��� ����������� ����������
    UPDATE ORDER_T O SET O.STATUS = Pk00_Const.c_ORDER_STATE_LOCK
    WHERE EXISTS (
      SELECT * FROM ORDER_LOCK_T L
       WHERE O.ORDER_ID = L.ORDER_ID
         AND L.DATE_FROM < SYSDATE
         AND (L.DATE_TO IS NULL OR SYSDATE < L.DATE_TO ) 
    )
    AND O.STATUS != Pk00_Const.c_ORDER_STATE_MOVED;
    Pk01_Syslog.Write_msg('Set status LOCK for '||v_count||' orders' ,c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR',c_PkgName||'.'||v_prcName);
END;

-- ������������� ��� ������ �� ������� �����
PROCEDURE Edit_order_dates(
               p_order_id      IN INTEGER,   -- ID ������
               p_date_from     IN DATE,      -- ���� ������ �������� ������
               p_date_to       IN DATE
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Edit_order_dates';
    v_df          DATE;
    v_dt          DATE;
    v_ob          NUMBER;
    v_status      VARCHAR2(100);
BEGIN
    --���������, ���� �� � ����� ������ ����� ��������� �����
    -- ���� >=2, �� ����
    SELECT COUNT(*) INTO v_ob
        FROM order_body_t
       WHERE  order_Id = p_order_id
             AND (   date_to =
                        TO_DATE ('19.03.2015 23:59:59', 'DD.MM.YYYY HH24:MI:SS')
                  OR DATE_FROM = TO_DATE ('20.03.2015', 'DD.MM.YYYY'));

    
    SELECT DATE_FROM, DATE_TO INTO v_df, v_dt
           FROM ORDER_T
       WHERE ORDER_ID = p_order_Id;
    
    IF v_ob>=2 THEN 
      IF (((v_df < TO_DATE('20.03.2015','DD.MM.YYYY')) AND (p_date_from >= TO_DATE('20.03.2015','DD.MM.YYYY'))) OR
         ((v_df >= TO_DATE('20.03.2015','DD.MM.YYYY')) AND (p_date_from < TO_DATE('20.03.2015','DD.MM.YYYY'))) OR
         ((v_dt >= TO_DATE('20.03.2015','DD.MM.YYYY')) AND (p_date_to < TO_DATE('20.03.2015','DD.MM.YYYY'))) OR
         ((v_dt < TO_DATE('20.03.2015','DD.MM.YYYY')) AND (p_date_to >= TO_DATE('20.03.2015','DD.MM.YYYY')))) THEN
                RAISE_APPLICATION_ERROR(-20000, '������������� �������� ������ �� ������ ������ ������. ������ � ������ ������.');
      END IF;    
    END IF;
        
    IF (p_date_to IS NULL OR p_date_to >= TO_DATE('01.01.2050','DD.MM.YYYY')) THEN
       v_dt := TO_DATE('01.01.2050','DD.MM.YYYY');
    ELSE
       v_dt := p_date_to;
    END IF;

    UPDATE ORDER_T 
       SET DATE_FROM   = NVL(p_date_from, DATE_FROM),
           DATE_TO     = NVL(v_dt, DATE_TO)
     WHERE ORDER_ID = p_order_id;      
     
     UPDATE order_body_t
       SET DATE_FROM   = NVL(p_date_from, DATE_FROM)
         WHERE order_id = p_order_id
               AND date_from = (SELECT MIN(DATE_FROM)
                                  FROM order_body_t
                                 WHERE order_Id = p_order_id);
                                 
     UPDATE order_body_t
       SET DATE_TO     = NVL(v_dt, DATE_TO)
         WHERE order_id = p_order_id
               AND date_to = (SELECT MAX(DATE_TO)
                                  FROM order_body_t
                                 WHERE order_Id = p_order_id);                                      

     UPDATE ORDER_PHONES_T 
       SET DATE_FROM   = NVL(p_date_from, DATE_FROM),
           DATE_TO     = NVL(v_dt, DATE_TO)
     WHERE ORDER_ID = p_order_id;  
     
     -- ���� ����� ��� ������, � ������ ��������� - ��������� ������ �������
     select STATUS into v_status from order_t
            where order_id = p_order_id;
     
     IF v_status = 'CLOSED' AND (p_date_to is null OR p_date_to > SYSDATE) THEN
        UPDATE ORDER_T
          SET STATUS = 'OPEN'
        WHERE ORDER_ID = p_order_id;
     END IF;
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ��������� ����� � ������ �������� ����� �� ������
PROCEDURE Move_order(
             p_order_id       IN INTEGER,   -- ID ������
             p_account_id_dst IN INTEGER,   -- �/� �� ������� ��������� �����
             p_date_from      IN DATE       -- ���� ���������� ��������
         )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Move_order';
    v_count       INTEGER;
BEGIN
    -- ��������� ������� ����������� ��������
    INSERT INTO ORDER_MOVE_T (
        ORDER_ID, ORDER_NO, ACCOUNT_ID, ACCOUNT_ID_PREV, DATE_FROM, SAVE_DATE, NOTES, OS_USER, HOST_NAME
    )
    SELECT
         O.ORDER_ID, O.ORDER_NO, p_account_id_dst ACCOUNT_ID, O.ACCOUNT_ID ACCOUNT_ID_PREV, 
         SYSDATE DATE_FROM, 
         SYSDATE SAVE_DATE, '����������' NOTES, 
         SYS_CONTEXT('USERENV', 'OS_USER') OS_USER,
         SYS_CONTEXT('USERENV', 'HOST') HOST_NAME
     FROM ORDER_T O
    WHERE ORDER_ID = p_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_MOVE_T.order_id='||p_order_id||', '||v_count||' rows inserted' ,c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    -- ��������� ����� �� ��������� �/�
    UPDATE ORDER_T SET ACCOUNT_ID = p_account_id_dst
     WHERE ORDER_ID = p_order_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ORDER_T.account_id='||p_account_id_dst||', '||v_count||' rows updated' ,c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� �������
-- - ��� ������ ���������� ����������
FUNCTION Get_downtime (
               p_order_id  IN INTEGER
               ) RETURN INTEGER 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_downtime';
    v_downtime 	 INTEGER;
BEGIN
    SELECT DOWNTIME_FREE INTO v_downtime
      FROM ORDER_INFO_T
     WHERE ORDER_ID = p_order_id;
	  RETURN v_downtime;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
END PK06_ORDER;
/
