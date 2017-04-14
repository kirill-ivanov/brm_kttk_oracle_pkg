CREATE OR REPLACE PACKAGE PK03_PARTITIONING
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK03_PARTITIONING';
    -- ==============================================================================
    --
    -- ������ � ����������������� ���������
    -- �������� ���������������:
    -- ������ ���� ���������������� ������, �� ����������� BILL_T 
    -- ������������ � ����� �� �����.
    -- ���� ��� ������ ���� ������ �������� � ����� TABLESPACE, �������
    -- ����������: TBS_BILL_YYYY, �������� TBS_BILL_2013, TBS_BILL_2014
    -- ������ BILL_T �������� � ��������� TABLESPACE - TBS_BILL
    -- �������� ����� ��� ���������� ������ �������� PK ������, 
    -- ������ PK: YYMM.xxx.xxx.xxx, 
    -- ��� YYMM - ���/�����, xxx.xxx.xxx - ������� �� ������������������ SQ_BILL_ID
    -- ������������������ ��������� �� 000.000.000 �� 999.999.999 
    -- � � ������ ������ �� ����������. 1 ����. �������� �������� ������ �� ��������� �������.
    -- ������ PK: 1309.000.123.456 - 2013���, ��������, SQ_BILL_ID.NEXTVAL = 000123456 
    -- ������ ������ �������� �� ����� (PYYYY), ��� ������ 
    --        bill_t,
    --        item_t,
    --        item_transfer_t,
    --        payorder_t
    --        pay_transfer_t
    -- � �� ������� (PYYYYMM), ��� ������
    --        event_t
    --
    -- PS: �������� ����� ������� ��������������� ������ �������.
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ��������� �������� ������ � �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    PROCEDURE Make_month_partition(p_period IN DATE);
    
    -- ...
    
END PK03_PARTITIONING;
/
CREATE OR REPLACE PACKAGE BODY PK03_PARTITIONING
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ��������� �������� ������ � �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
PROCEDURE Make_month_partition(p_period IN DATE)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_month_partition';
BEGIN
    NULL;    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error('ERROR', c_PkgName||'.'||v_prcName);
END;


END PK03_PARTITIONING;
/
