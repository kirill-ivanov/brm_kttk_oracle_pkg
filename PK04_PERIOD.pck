CREATE OR REPLACE PACKAGE PK04_PERIOD
IS
    --
    -- ����� ��� ������ � �������� "������", �������:
    -- period_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK04_PERIOD';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- ========================================================================= --
    -- ����� ������� ������������ �������������� ������� 'YYYYMM'
    -- ========================================================================= --
    -- �������� ID ������� �� ��������� ����
    FUNCTION Period_id(p_period IN DATE) RETURN INTEGER DETERMINISTIC;
    -- �������� ID ������� ���������� �� ��������� ������
    FUNCTION Make_next_id(p_period_id IN INTEGER) RETURN INTEGER DETERMINISTIC;
    -- �������� ID ������� ����� ��������� ��������
    FUNCTION Make_prev_id(p_period_id IN INTEGER) RETURN INTEGER DETERMINISTIC;
    -- ������� ������ ������� �� ID 
    FUNCTION Period_from(p_period_id IN INTEGER) RETURN DATE DETERMINISTIC;
    -- ������� ����� ������� �� ID
    FUNCTION Period_to(p_period_id IN INTEGER) RETURN DATE DETERMINISTIC;
    -- ��������� ���� � ����� ��������� ������� (��������� ������� ������),
    -- ��������, ��� ���� 2013:  31.07.2013 23:59:59
    FUNCTION Period_end_date(p_month IN DATE) RETURN DATE DETERMINISTIC;
    -- ��������� ���-�� ���� � �������
    FUNCTION Period_days(p_period_id IN INTEGER) RETURN INTEGER DETERMINISTIC;

    -- ========================================================================= --
    -- �������� ������� �������
    -- ========================================================================= --
    -- �������� ID ���������� �������� �������
    -- ��� ������ ���������� ����������
    FUNCTION Open_period_id RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ID ���������� ��������� ������� � �������
    -- ��� ������ ���������� ����������
    FUNCTION Last_period_id RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ID ���������� �� ������� �������� �������
    -- ��� ������ ���������� ����������
    FUNCTION Next_period_id RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ID ������������� ������� �������, 
    -- ���� ������ ���������� � ������� �������� �� ����� ����� � �� �������� ����������� �������
    -- ����������:
    -- - ID �������
    -- - NULL - ����������� ������ ������
    -- ��� ������ ���������� ����������
    FUNCTION Bill_period_id RETURN INTEGER;

    -- ========================================================================= --
    -- ������ � ��������� �������
    -- ========================================================================= --
    -- ������� � ���������� ������������ �������
    PROCEDURE Next_bill_period;
    -- ������� ���������� ������
    PROCEDURE Close_fin_period;
    -- ������� �������� ������
    PROCEDURE Close_rep_period;
    
    -- ------------------------------------------------------------------------ --
    -- �������� ������ �� ���������� ������
    FUNCTION Is_closed(p_period_id IN INTEGER) RETURN BOOLEAN;
    
    -- ------------------------------------------------------------------------ --
    -- ������ ��������
    --   - ��� ������ ���������� ����������
     PROCEDURE Load_period_list (
               p_recordset    OUT t_refc
           );
        
END PK04_PERIOD;
/
CREATE OR REPLACE PACKAGE BODY PK04_PERIOD
IS

-- ========================================================================= --
-- ����� ������� ������������ �������������� ������� 'YYYYMM'
-- ========================================================================= --
-- �������� ID ������� �� ��������� ����
FUNCTION Period_id(p_period IN DATE) RETURN INTEGER DETERMINISTIC IS
BEGIN
    RETURN TO_NUMBER(TO_CHAR(p_period, 'yyyymm'));
END;

-- �������� ID ������� ���������� �� ��������� ������
FUNCTION Make_next_id(p_period_id IN INTEGER) RETURN INTEGER DETERMINISTIC IS
BEGIN
    RETURN TO_NUMBER(TO_CHAR(ADD_MONTHS(TO_DATE(p_period_id, 'yyyymm'),1), 'yyyymm'));
END;

-- �������� ID ������� ����� ��������� ��������
FUNCTION Make_prev_id(p_period_id IN INTEGER) RETURN INTEGER DETERMINISTIC IS
BEGIN
    RETURN TO_NUMBER(TO_CHAR(ADD_MONTHS(TO_DATE(p_period_id, 'yyyymm'),-1), 'yyyymm'));
END;

-- ������� ������ ������� �� ID 
FUNCTION Period_from(p_period_id IN INTEGER) RETURN DATE DETERMINISTIC IS
BEGIN
    RETURN TO_DATE(p_period_id, 'yyyymm');
END;

-- ������� ����� ������� �� ID
FUNCTION Period_to(p_period_id IN INTEGER) RETURN DATE DETERMINISTIC IS
BEGIN
    RETURN ADD_MONTHS(TO_DATE(p_period_id, 'yyyymm'),1)-1/86400;
END;

-- ��������� ���� � ����� ��������� ������� (��������� ������� ������),
-- ��������, ��� ���� 2013:  31.07.2013 23:59:59
FUNCTION Period_end_date(p_month IN DATE) RETURN DATE DETERMINISTIC IS
BEGIN
    RETURN ADD_MONTHS(TRUNC(p_month,'mm'),1)-1/86400;
END;

-- ��������� ���-�� ���� � �������
FUNCTION Period_days(p_period_id IN INTEGER) RETURN INTEGER DETERMINISTIC IS
BEGIN
    RETURN TO_CHAR(LAST_DAY(TO_DATE(p_period_id,'yyyymm')), 'DD');
END;

-- ========================================================================= --
-- �������� ������� �������
-- ========================================================================= --
-- �������� ID ���������� �������� �������
-- ��� ������ ���������� ����������
FUNCTION Open_period_id RETURN INTEGER IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Open_period_id';
    v_period_id INTEGER;
BEGIN
    SELECT P.PERIOD_ID INTO v_period_id
      FROM PERIOD_T P
      WHERE P.POSITION = PK00_CONST.c_PERIOD_OPEN;
    RETURN v_period_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.Raise_exception('ERROR', c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ID ���������� ��������� ������� � �������
-- ��� ������ ���������� ����������
FUNCTION Last_period_id RETURN INTEGER IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Last_period_id';
    v_period_id INTEGER;
BEGIN
    SELECT P.PERIOD_ID INTO v_period_id
      FROM PERIOD_T P
      WHERE P.POSITION = PK00_CONST.c_PERIOD_LAST;
    RETURN v_period_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.Raise_exception('ERROR', c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ID ���������� �� ������� �������� �������
-- ��� ������ ���������� ����������
FUNCTION Next_period_id RETURN INTEGER IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Next_period_id';
    v_period_id INTEGER;
BEGIN
    SELECT P.PERIOD_ID INTO v_period_id
      FROM PERIOD_T P
      WHERE P.POSITION = PK00_CONST.c_PERIOD_NEXT;
    RETURN v_period_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.Raise_exception('ERROR', c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ID ������������� ������� �������, 
-- ���� ������ ���������� � ������� �������� �� ����� ����� � �� �������� ����������� �������
-- ����������:
-- - ID �������
-- - NULL - ����������� ������ ������
-- ��� ������ ���������� ����������
FUNCTION Bill_period_id RETURN INTEGER IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Bill_period_id';
    v_period_id INTEGER;
BEGIN
    SELECT P.PERIOD_ID INTO v_period_id
      FROM PERIOD_T P
      WHERE P.POSITION = PK00_CONST.c_PERIOD_NEXT;
    RETURN v_period_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        Pk01_SysLog.Raise_exception('ERROR', c_PkgName||'.'||v_prcName);
END;

-- ========================================================================= --
-- ������� � ���������� ������������ �������
PROCEDURE Next_bill_period
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Next_bill_period';
    v_period_id   INTEGER;
    v_period_from DATE;
    v_period_to   DATE;
BEGIN
    -- �������� ������� �������:
    -- LAST - ��� ��������� �� �������� ����������� �������
    -- BILL <- OPEN 
    UPDATE PERIOD_T SET POSITION = Pk00_Const.c_PERIOD_BILL
    WHERE POSITION = Pk00_Const.c_PERIOD_OPEN;
    -- OPEN <- NEXT
    UPDATE PERIOD_T SET POSITION = Pk00_Const.c_PERIOD_OPEN
    WHERE POSITION = Pk00_Const.c_PERIOD_NEXT
    RETURNING PERIOD_FROM INTO v_period_from;
    -- new NEXT
    v_period_from:= ADD_MONTHS(v_period_from,1);
    v_period_to  := ADD_MONTHS(v_period_from,1)-1/86400;
    v_period_id  := Period_id(v_period_from);
    INSERT INTO PERIOD_T(PERIOD_ID, PERIOD_FROM, PERIOD_TO, POSITION)  
    VALUES(v_period_id, v_period_from, v_period_to, Pk00_Const.c_PERIOD_NEXT);
    -- �������� ���������
    Pk01_Syslog.Write_msg('set period_id='||Make_prev_id(v_period_id)||
                          ', next_period_id='||v_period_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.Raise_exception('ERROR', c_PkgName||'.'||v_prcName);
        RAISE;
END;

-- ������� ���������� ������
PROCEDURE Close_fin_period
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_fin_period';
BEGIN
    -- ���������� NULL<-LAST
    UPDATE PERIOD_T SET POSITION = NULL
     WHERE POSITION = Pk00_Const.c_PERIOD_LAST;
    -- ���������� LAST->BILL
    UPDATE PERIOD_T 
       SET POSITION = Pk00_Const.c_PERIOD_LAST, 
           CLOSE_FIN_PERIOD = SYSDATE
     WHERE POSITION = Pk00_Const.c_PERIOD_BILL;
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error('ERROR', c_PkgName||'.'||v_prcName);
END;

-- ������� �������� ������
PROCEDURE Close_rep_period
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_rep_period';
BEGIN
    UPDATE PERIOD_T SET CLOSE_REP_PERIOD = SYSDATE
     WHERE POSITION = Pk00_Const.c_PERIOD_LAST;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error('ERROR', c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- �������� ������ �� ���������� ������
FUNCTION Is_closed(p_period_id IN INTEGER) RETURN BOOLEAN
IS
    v_fin_period   DATE;
BEGIN
    SELECT CLOSE_FIN_PERIOD INTO v_fin_period
      FROM PERIOD_T
     WHERE PERIOD_ID = p_period_id;
    IF v_fin_period IS NOT NULL THEN
        RETURN TRUE;   -- ������ ����������� ��������� ���. �������
    ELSE
        RETURN FALSE;  -- ������ ����������� ��������� ���. �������
    END IF;
END;

-- ------------------------------------------------------------------------ --
-- ������ ��������
--   - ��� ������ ���������� ����������
PROCEDURE Load_period_list (
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Load_period_list';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR
          SELECT * FROM PERIOD_T P 
          ORDER BY P.PERIOD_ID;    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;

        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);

END;

END PK04_PERIOD;
/
