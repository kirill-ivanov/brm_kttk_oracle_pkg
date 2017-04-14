CREATE OR REPLACE PACKAGE PK08_ITEM
IS
    --
    -- ����� ��� ������ � �������� "������� �����", �������:
    -- item_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK08_ITEM';
    -- ==============================================================================
    type t_refc is ref cursor;
    
    -- ------------------------------------------------------------------ --
    -- �������� ������� ����� ����������� ������� (item) �����, ����������:
    --   - ID ������� ����� (item_id), 
    --   - ��� ������ ���������� ����������
    FUNCTION New_bill_item (
                   p_bill_id        IN INTEGER,   -- ID �����
                   p_rep_period_id  IN INTEGER,   -- ID ��������� ������� �����
                   p_order_id       IN INTEGER,   -- ID ������
                   p_service_id     IN INTEGER,   -- ID ������
                   p_subservice_id  IN INTEGER,   -- ID ���������� ������
                   p_charge_type    IN VARCHAR2,  -- ID ������� ���������� (��, ������,...)
                   p_tax_incl       IN CHAR,      -- ���������� �������� �����: "Y/N"
                   p_item_total     IN NUMBER DEFAULT 0, -- ����� ����� �� ������� �����
                   p_date_from      IN DATE DEFAULT NULL, -- ���� ������� ������� ������
                   p_date_to        IN DATE DEFAULT NULL  -- ���� ���������� ������� ������
               ) RETURN INTEGER;

      -- ���������� ����������� �� ������� (item) �����
      -- ���� ������� ���, �� ��� ���������
      --   - ���������� ITEM_ID
      --   - ��� ������ ���������� ����������
      FUNCTION Put_bill_item(
                   p_bill_id        IN INTEGER,   -- ID �����
                   p_rep_period_id  IN INTEGER,   -- ID ��������� ������� �����
                   p_order_id       IN INTEGER,   -- ID ������
                   p_service_id     IN INTEGER,   -- ID ������
                   p_subservice_id  IN INTEGER,   -- ID ���������� ������
                   p_charge_type    IN VARCHAR2,  -- ID ������� ���������� (��, ������,...)
                   p_tax_incl       IN CHAR,      -- ���������� �������� �����: "Y/N"
                   p_item_total     IN NUMBER DEFAULT 0, -- ����� ����� �� ������� �����
                   p_date_from      IN DATE DEFAULT NULL, -- ���� ������� ������� ������
                   p_date_to        IN DATE DEFAULT NULL  -- ���� ���������� ������� ������
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------ --
    -- �������� ������� ����� ������� ������������� (item) �����, ����������:
    --   - ID ������� ����� (item_id), 
    --   - ��� ������ ���������� ����������
    FUNCTION New_adjust_item (
                   p_bill_id        IN INTEGER,   -- ID �����
                   p_rep_period_id  IN INTEGER,   -- ID ��������� ������� �����
                   p_order_id       IN INTEGER,   -- ID ������
                   p_service_id     IN INTEGER,   -- ID ������
                   p_subservice_id  IN INTEGER,   -- ID ���������� ������
                   p_charge_type    IN VARCHAR2,  -- ID ������� ���������� (��, ������,...)
                   p_tax_incl       IN CHAR,      -- ���������� �������� �����: "Y/N"
                   p_adjusted       IN NUMBER DEFAULT 0,  -- ����� ������������
                   p_date_from      IN DATE DEFAULT NULL, -- ���� ������� ������� ������
                   p_date_to        IN DATE DEFAULT NULL, -- ���� ���������� ������� ������
                   p_notes          IN VARCHAR2 DEFAULT NULL
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------ --
    -- ���������� ������������� ��� ������� �����
    --   - �������� ������������� �� ������� DUE=ITEM_TOTAL+ADJUSTED-TRANSFERED-RECVD 
    --   - ��� ������ ���������� ����������
    FUNCTION Calculate_due(
                   p_bill_id       IN INTEGER,   -- ID �����
                   p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
                   p_item_id       IN INTEGER    -- ID ������� �����
               ) RETURN NUMBER; 

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� �������� �� ���������� � ������������� �� ITEM
    -- ���������� ��������, ���� ITEM ��� �� ����� � ���� �������
    --   - ��� ������ ���������� ����������
    FUNCTION Is_chargable (
                   p_bill_id       IN INTEGER,   -- ID �����
                   p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
                   p_item_id       IN INTEGER    -- ID ������� �����
               ) RETURN BOOLEAN;
    
    -- ���������� ���������� �� ������� ����� (item), ����������:
    --   - �������� ������������� �� ������� DUE=ITEM_TOTAL+ADJUSTED-TRANSFERED-RECVD 
    --   - ��� ������ ���������� ����������
    FUNCTION Charge_item_value (
                   p_bill_id       IN INTEGER,   -- ID �����
                   p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
                   p_item_id       IN INTEGER,   -- ID ������� �����
                   p_value         IN NUMBER,    -- ����� ���������� �� ������� �����
                   p_date_from     IN DATE,      -- ��������� �������� ��������� ������
                   p_date_to       IN DATE       -- �� event_t
               ) RETURN NUMBER;

    -- ���������� ������������� ����� ������� �����, ����������:
    --   - �������� ������������� �� ������� DUE=ITEM_TOTAL+ADJUSTED-TRANSFERED-RECVD 
    --   - ��� ������ ���������� ����������
    FUNCTION Adjust_item_value (
                   p_bill_id       IN INTEGER,   -- ID �����
                   p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
                   p_item_id       IN INTEGER,   -- ID ������� �����
                   p_value         IN NUMBER     -- ����� ���������� �� ������� �����
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ����� ������� �� ������� ������ �����, ����������:
    --   - �������� ������������� �� ������� DUE=ITEM_TOTAL+ADJUSTED+TRANSFERED+RECVD 
    --   - ��� ������ ���������� ����������
    FUNCTION Recvd_item_value (
                   p_bill_id       IN INTEGER,   -- ID �����
                   p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
                   p_item_id       IN INTEGER,   -- ID ������� �����
                   p_value         IN NUMBER     -- ����� ���������� �� ������� �����
               ) RETURN INTEGER;
    --==================================================================================--
-- ������� ������� �����, ��� ������� �� ������������ ������� �������, 
-- �.�. ��� �� ����� � �������� ����
--   - ��� ������ ���������� ����������
-- (����� ��� �������� ������� ����������� �� �������� � �������)
PROCEDURE Delete_item (
               p_bill_id       IN INTEGER,   -- ID �����
               p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
               p_item_id       IN INTEGER    -- ID ������� �����
          );
END PK08_ITEM;
/
CREATE OR REPLACE PACKAGE BODY PK08_ITEM
IS

--==================================================================================--
-- ��������� ������ �����, ������ �������� ������ � �������� ������, 
-- ���� ������ != 'OPEN' - ������������ ����������
PROCEDURE Check_bill_status (
          p_bill_id  IN INTEGER, 
          p_rep_period_id IN INTEGER
       )
IS
    v_bill_status BILL_T.BILL_STATUS%TYPE;
BEGIN
    -- ��������� ������ �����, ������ �������� ������ � �������� ������
    SELECT B.BILL_STATUS
      INTO v_bill_status 
      FROM BILL_T B
     WHERE B.BILL_ID = p_bill_id
       AND B.REP_PERIOD_ID = p_rep_period_id;
     IF v_bill_status != Pk00_Const.c_BILL_STATE_OPEN THEN
         RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
                      'BILL_ID='||p_bill_id||
                      ', BILL_STATUS='||v_bill_status||
                      ' - ������ � ��������� ����� ���������');         
     END IF;
END;

--==================================================================================--
-- �������� ������� ����� ������� (item) �����, ����������:
--   - ID ������� ����� (item_id), 
--   - ��� ������ ���������� ����������
FUNCTION New_bill_item (
               p_bill_id        IN INTEGER,   -- ID �����
               p_rep_period_id  IN INTEGER,   -- ID ��������� ������� �����
               p_order_id       IN INTEGER,   -- ID ������
               p_service_id     IN INTEGER,   -- ID ������
               p_subservice_id  IN INTEGER,   -- ID ���������� ������
               p_charge_type    IN VARCHAR2,  -- ID ������� ���������� (��, ������,...)
               p_tax_incl       IN CHAR,      -- ���������� �������� �����: "Y/N"
               p_item_total     IN NUMBER DEFAULT 0, -- ����� ����� �� ������� �����
               p_date_from      IN DATE DEFAULT NULL, -- ���� ������� ������� ������
               p_date_to        IN DATE DEFAULT NULL  -- ���� ���������� ������� ������
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'New_bill';
    v_item_id     INTEGER;
    v_item_type   ITEM_T.ITEM_TYPE%TYPE;
BEGIN
    -- ������ �������� ������ � �������� ������
    Check_bill_status (p_bill_id, p_rep_period_id);
    -- �������� ID ������� �����
    v_item_id := PK02_POID.Next_item_id;
    -- ��� ������� �����
    v_item_type := Pk00_Const.c_ITEM_TYPE_BILL;
    -- ������� ������ ������� �����
    INSERT INTO ITEM_T (
       BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, 
       ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE,
       ITEM_TOTAL, RECVD, 
       DATE_FROM, DATE_TO, LAST_MODIFIED,
       INV_ITEM_ID, ITEM_STATUS, TAX_INCL
    )VALUES(
       p_bill_id, p_rep_period_id, v_item_id, v_item_type,
       p_order_id, p_service_id, p_subservice_id, p_charge_type,
       p_item_total, 0,
       p_date_from, p_date_to, SYSDATE,
       NULL, Pk00_Const.c_ITEM_STATE_OPEN, p_tax_incl
    );
    -- ���������� ID ��������� ������� �����
    RETURN v_item_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================--
-- ���������� ����������� �� ������� (item) �����
-- ���� ������� ���, �� ��� ���������
--   - ���������� ITEM_ID
--   - ��� ������ ���������� ����������
FUNCTION Put_bill_item(
               p_bill_id        IN INTEGER,   -- ID �����
               p_rep_period_id  IN INTEGER,   -- ID ��������� ������� �����
               p_order_id       IN INTEGER,   -- ID ������
               p_service_id     IN INTEGER,   -- ID ������
               p_subservice_id  IN INTEGER,   -- ID ���������� ������
               p_charge_type    IN VARCHAR2,  -- ID ������� ���������� (��, ������,...)
               p_tax_incl       IN CHAR,      -- ���������� �������� �����: "Y/N"
               p_item_total     IN NUMBER DEFAULT 0, -- ����� ����� �� ������� �����
               p_date_from      IN DATE DEFAULT NULL, -- ���� ������� ������� ������
               p_date_to        IN DATE DEFAULT NULL  -- ���� ���������� ������� ������
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Put_bill_item';
    v_item_type  CONSTANT ITEM_T.ITEM_TYPE%TYPE := Pk00_Const.c_ITEM_TYPE_BILL;
    v_item_id    INTEGER;
    v_count      INTEGER := 0;
BEGIN
    -- ������ �������� ������ � �������� ������
    Check_bill_status (p_bill_id, p_rep_period_id);
    -- �������� ��������� ��������� ������������ ������� �����
    UPDATE ITEM_T
      SET ITEM_TOTAL= ITEM_TOTAL + p_item_total,
          DATE_FROM = CASE 
                        WHEN DATE_FROM IS NULL THEN p_date_from
                        WHEN p_date_from < DATE_FROM THEN p_date_from 
                      END,
          DATE_TO   = CASE 
                        WHEN DATE_TO IS NULL THEN p_date_to
                        WHEN DATE_TO < p_date_to THEN p_date_to 
                      END,
          LAST_MODIFIED = SYSDATE
     WHERE  BILL_ID       = p_bill_id
        AND REP_PERIOD_ID = p_rep_period_id
        AND ITEM_TYPE     = v_item_type
        AND ORDER_ID      = p_order_id 
        AND SERVICE_ID    = p_service_id
        AND SUBSERVICE_ID = p_subservice_id 
        AND CHARGE_TYPE   = p_charge_type 
        AND ( DATE_FROM IS NULL OR  -- ����������� ������ ������������ �������
              TRUNC(DATE_FROM,'mm') = TRUNC(p_date_from,'mm') 
            )
    RETURNING ITEM_ID INTO v_item_id;
    v_count := SQL%ROWCOUNT; 
    IF v_count = 0 THEN
        -- ������� ������� �� ����� ��� �� �������, �������
        v_item_id := PK02_POID.Next_item_id;
        --
        INSERT INTO ITEM_T (
           BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, 
           ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE,
           ITEM_TOTAL, RECVD, 
           DATE_FROM, DATE_TO,
           INV_ITEM_ID, TAX_INCL
        )VALUES(
           p_bill_id, p_rep_period_id, v_item_id, v_item_type,
           p_order_id, p_service_id, p_subservice_id, p_charge_type,
           p_item_total, 0,
           p_date_from, p_date_to,
           NULL, p_tax_incl
        );
    ELSIF v_count > 1 THEN
        -- item ������� �� ����� �� �������� - ��� ���� �� ������
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
                        '�� ���������� ITEM ���������� �� ����� BILL_T.BILL_ID='||p_bill_id); 
    END IF;
    RETURN v_item_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================--
-- �������� ������� ����� ������� ������������� (item) �����, ����������:
--   - ID ������� ����� (item_id), 
--   - ��� ������ ���������� ����������
FUNCTION New_adjust_item (
               p_bill_id        IN INTEGER,   -- ID �����
               p_rep_period_id  IN INTEGER,   -- ID ��������� ������� �����
               p_order_id       IN INTEGER,   -- ID ������
               p_service_id     IN INTEGER,   -- ID ������
               p_subservice_id  IN INTEGER,   -- ID ���������� ������
               p_charge_type    IN VARCHAR2,  -- ID ������� ���������� (��, ������,...)
               p_tax_incl       IN CHAR,      -- ���������� �������� �����: "Y/N"
               p_adjusted       IN NUMBER DEFAULT 0,  -- ����� ������������
               p_date_from      IN DATE DEFAULT NULL, -- ���� ������� ������� ������
               p_date_to        IN DATE DEFAULT NULL, -- ���� ���������� ������� ������
               p_notes          IN VARCHAR2 DEFAULT NULL
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Put_adjust_item';
    v_item_id    INTEGER;
    v_item_type  ITEM_T.ITEM_TYPE%TYPE;
BEGIN
    -- ������ �������� ������ � �������� ������
    Check_bill_status (p_bill_id, p_rep_period_id);
    -- �������� ID ������� �����
    v_item_id := PK02_POID.Next_item_id;
    -- ���������� ��� ������� ������������� �����
    v_item_type := PK00_CONST.c_ITEM_TYPE_ADJUST;
    -- ������� ������ ������� �����
    INSERT INTO ITEM_T (
       BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE,  
       ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE,
       ITEM_TOTAL, RECVD, ITEM_STATUS, 
       DATE_FROM, DATE_TO,
       INV_ITEM_ID, TAX_INCL, NOTES
    )VALUES(
       p_bill_id, p_rep_period_id, v_item_id, v_item_type,
       p_order_id, p_service_id, p_subservice_id, p_charge_type,
       p_adjusted, 0,'OPEN',
       p_date_from, p_date_to,
       NULL, p_tax_incl, p_notes
    );
    -- ���������� ID ��������� ������� �����
    RETURN v_item_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================--
-- ���������� ������������� ��� ������� �����
--   - �������� ������������� �� ������� DUE=ITEM_TOTAL+ADJUSTED+TRANSFERED+RECVD 
--   - ��� ������ ���������� ����������
FUNCTION Calculate_due(
               p_bill_id       IN INTEGER,   -- ID �����
               p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
               p_item_id       IN INTEGER    -- ID ������� �����
           ) RETURN NUMBER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Calculate_due';
    v_due        NUMBER;
BEGIN
    SELECT RECVD-ITEM_TOTAL 
      INTO v_due
      FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND ITEM_ID = p_item_id;
     -- ���������� ����� ���������� � ������������� �� ����� (�� ��� ������ ���� ��������)
    RETURN v_due;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ��������� �������� �� ���������� � ������������� �� ITEM
-- ���������� ��������, ���� ITEM ��� �� ����� � ���� �������
--   - ��� ������ ���������� ����������
FUNCTION Is_chargable (
               p_bill_id       IN INTEGER,  -- ID �����
               p_rep_period_id IN INTEGER,  -- ID ��������� ������� �����
               p_item_id       IN INTEGER   -- ID ������� �����
           ) RETURN BOOLEAN
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Is_chargable';
    v_inv_item_id INTEGER;
    v_retcode     BOOLEAN;
BEGIN
    SELECT INV_ITEM_ID INTO v_inv_item_id
      FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND ITEM_ID = p_item_id;
    IF v_inv_item_id IS NULL THEN
        v_retcode := TRUE;   -- �������� - ITEM �� ����� � ����/������� 
    ELSE
        v_retcode := FALSE;  -- �� �������� - ITEM ��� ����� � ����/�������
    END IF;
    RETURN v_retcode;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================--
-- ���������� ���������� �� ������� ����� (item), ����������:
--   - �������� ������������� �� ������� DUE=ITEM_TOTAL+ADJUSTED-RECVD 
--   - ��� ������ ���������� ����������
FUNCTION Charge_item_value (
               p_bill_id       IN INTEGER,   -- ID �����
               p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
               p_item_id       IN INTEGER,   -- ID ������� �����
               p_value         IN NUMBER,    -- ����� ���������� �� ������� �����
               p_date_from     IN DATE,      -- ��������� �������� ��������� ������
               p_date_to       IN DATE       -- �� event_t
           ) RETURN NUMBER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Charge_item_value';
    v_value      NUMBER := NVL(p_value,0);
    v_count      INTEGER;
    v_due        NUMBER := 0;
BEGIN
    -- ���������� ���������� �� ������� �����, ���� ��� ��� �� ����� � ����-�������
    UPDATE ITEM_T
       SET ITEM_TOTAL = ITEM_TOTAL + v_value,
           DATE_FROM = CASE 
                          WHEN DATE_FROM IS NULL THEN p_date_from
                          WHEN p_date_from < DATE_FROM THEN p_date_from 
                       END,
           DATE_TO   = CASE 
                          WHEN DATE_TO IS NULL THEN p_date_to
                          WHEN DATE_TO < p_date_to THEN p_date_to 
                       END,
           LAST_MODIFIED = SYSDATE
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND ITEM_ID = p_item_id
       AND ITEM_TYPE = PK00_CONST.c_ITEM_TYPE_BILL
       AND INV_ITEM_ID IS NULL   -- ������, ��� �� ����� � ����-�������
    RETURNING ITEM_TOTAL-RECVD INTO v_due;
    -- ������� ���������� ���������� ����������
    IF SQL%ROWCOUNT = 0 THEN
        SELECT COUNT(*) INTO v_count FROM ITEM_T WHERE ITEM_ID = p_item_id;
        IF v_count = 0 THEN
            RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '����������� ������ ITEM_T.ITEM_ID='||p_item_id);
        ELSE
            RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '������ ITEM_T.ITEM_ID='||p_item_id
                                           ||' - ��� ����� � ����-������� ��� �������� ITEM_TYPE');
        END IF;
    END IF; 
    -- ���������� ����� ����������� ��������������      
    RETURN v_due;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );  
END;

-- ���������� ������������� ����� ������� �����, ����������:
--   - �������� ������������� �� ������� DUE=ITEM_TOTAL+ADJUSTED-RECVD
--   - ��� ������ ���������� ����������
FUNCTION Adjust_item_value (
               p_bill_id       IN INTEGER,   -- ID �����
               p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
               p_item_id       IN INTEGER,   -- ID ������� �����
               p_value         IN NUMBER     -- ����� ����� ������� ����
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Adjust_item_value';
    v_value      NUMBER := NVL(p_value,0);
    v_count      INTEGER;
    v_due        INTEGER := 0;
BEGIN
    -- ���������� ������������� ����� ������� �����, ���� ��� ��� �� ����� � ����-�������
    UPDATE ITEM_T
       SET ITEM_TOTAL =  v_value,
           LAST_MODIFIED = SYSDATE
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND ITEM_ID = p_item_id
--       AND ITEM_TYPE = PK00_CONST.c_ITEM_TYPE_ADJUST
       AND INV_ITEM_ID IS NULL   -- ������, ��� �� ����� � ����-�������
    RETURNING ITEM_TOTAL-RECVD INTO v_due;
    -- ������� ���������� ���������� ����������
    IF SQL%ROWCOUNT = 0 THEN
        SELECT COUNT(*) INTO v_count FROM ITEM_T WHERE ITEM_ID = p_item_id;
        IF v_count = 0 THEN
            RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '����������� ������ ITEM_T.ITEM_ID='||p_item_id);
        ELSE
            RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '������ ITEM_T.ITEM_ID='||p_item_id
                                           ||' - ��� ����� � ����-������� ��� �������� ITEM_TYPE');
        END IF;
    END IF; 
    -- ���������� ����� ����������� ��������������      
    RETURN v_due;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );  
END;

-- ���������� ����� ������� �� ������� �����, ����������:
--   - �������� ����� ����� �������� �� ������� �������� 
--   - ��� ������ ���������� ����������
FUNCTION Recvd_item_value (
               p_bill_id       IN INTEGER,   -- ID �����
               p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
               p_item_id       IN INTEGER,   -- ID ������� �����
               p_value         IN NUMBER     -- ����� ���������� �� ������� �����
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Recvd_item_value';
    v_value      NUMBER := NVL(p_value,0);
    v_recvd      NUMBER := 0;
BEGIN
    -- ���������� ���������� �� ������� �����, ���� ��� ��� �� ����� � ����-�������
    UPDATE ITEM_T
       SET RECVD = RECVD + v_value,
           LAST_MODIFIED = SYSDATE
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
       AND ITEM_ID = p_item_id
     RETURNING RECVD INTO v_recvd; 
    -- ������� ���������� ���������� ����������
    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, '����������� ������ ITEM_T(PAYMENT).ITEM_ID='||p_item_id);
    END IF; 
    -- ���������� ����� ����������� ��������������      
    RETURN v_recvd;  
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--==================================================================================--
-- ������� ������� �����, ��� ������� �� ������������ ������� �������, 
-- �.�. ��� �� ����� � �������� ����
--   - ��� ������ ���������� ����������
-- (����� ��� �������� ������� ����������� �� �������� � �������)
PROCEDURE Delete_item(
               p_bill_id       IN INTEGER,   -- ID �����
               p_rep_period_id IN INTEGER,   -- ID ��������� ������� �����
               p_item_id       IN INTEGER    -- ID ������� �����
          )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_item';
    v_item_type   ITEM_T.ITEM_TYPE%TYPE;
    v_inv_item_id ITEM_T.INV_ITEM_ID%TYPE;
    v_item_status ITEM_T.ITEM_STATUS%TYPE;
BEGIN
    -- ��������� �������� ����������� ��������
    SELECT I.ITEM_TYPE, I.INV_ITEM_ID, I.ITEM_STATUS
      INTO v_item_type, v_inv_item_id, v_item_status
      FROM ITEM_T I
     WHERE I.ITEM_ID = p_item_id
       AND BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- ������ �������� ������ � �������� ������
    Check_bill_status (p_bill_id, p_rep_period_id);
    --      
    IF v_inv_item_id IS NOT NULL THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
                     '�������� ����������, ITEM_ID='||p_item_id||
                     ' ��� ����� � INV_TEM_ID='||v_inv_item_id);
    END IF;
    IF v_item_status != Pk00_Const.c_ITEM_STATE_OPEN THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 
                     '�������� ����������, ITEM_ID='||p_item_id||
                     ' ����� ITEM_STATUS='||v_item_status);
    END IF;
    -- �������� ������� �����
    DELETE FROM ITEM_T 
     WHERE ITEM_ID = p_item_id
       AND BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );  
END;


END PK08_ITEM;
/
