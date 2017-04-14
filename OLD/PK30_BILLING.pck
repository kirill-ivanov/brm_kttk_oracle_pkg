CREATE OR REPLACE PACKAGE PK30_BILLING
IS
    --
    -- ����� ��� ��������� �������� ����������� ������ � �������� �������
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK30_BILLING';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���������� �������� �������:
    -- 1) Next_Bill_Period - ����������� � 00:01 UTS - ������� ������� ���� ������
    -- 2) Billing - ����������� � 02:00 UTS (�� ��������� �������� � ����������� �������� ������� �� ���������� �����)
    -- 3) ����� � ��������� 'READY' - ����������, ����������, ������������ ��������
    -- 4) Close_Financial_Period - ���-�� 6-7 ����� ����������� ���. ������, ��� ����:
    -- 4.1) Pk04_Period.Close_fin_period - ������� ���������� ������: FIN_PERIOD = BILL_PERIOD_LAST    
    -- 4.2) ������� ������� ����� �������� ������� (��������� � ��� ��� ���������)
    -- 4.3) Calc_advance - ��������� ������ �� �������� �� ������ ���������� ������
    -- 4.4) Period_info - ���������� ������ �� ������ ��� ���� ������� ������
    -- 4.5) Refresh_balance - ����������� �������, ���� ������� ������
    -- � �������� ��������, 
    --   * ���������� ��������� ������, 
    --   * ������� ������, 
    --   * �������� �������� �������� �������� 
    --   * ��������� ��������� ������������ ����� 
    -- ����������!!!
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �������� �� ��������� ����������� ������ (������� ����� �����)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Next_Bill_Period;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������������ ������ ��� ���� ���������, ������ �������� �� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Billing( p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������������� �������� (��������� �����������!!!)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Begin_Billing( p_bill_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE End_Billing( p_bill_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ����� � ������ ����� � ������ (READY) �� ������� �������� (CHECK)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Set_billstatus_ready( p_bill_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� � ������� �� ����������� ������ 
    -- �������� ���. ���
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_person_bills( p_period_id  IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� � ������� �� ����������� ������ 
    -- �������� ��.��� �������� ���� (���������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_MMTS_bills( p_period_id IN INTEGER );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� � ������� �� ����������� ������ 
    -- �������� ��.��� �������� ���� (PORTAL 6.5 + �����)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_KTTK_bills( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� � ������� �� ����������� ������ 
    -- �������� ��.��� ������� � ��� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_SPB_bills( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� � ������� �� ����������� ������ 
    -- �������� ��.��� ����������� (���� ������) 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Make_NTK_bills( p_period_id IN INTEGER );
    
    -- ------------------------------------------------------------------------- --
    -- ��������� ��������� ������ 
    -- ------------------------------------------------------------------------- --
    PROCEDURE Make_discounts( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� � ����� ����������� �� ������� 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Downtime_processing( p_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ����������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Close_Financial_Period;

    
    -- ------------------------------------------------------------------------- --
    --                   � � � � � � � � �   � � � � � � � � �
    -- ------------------------------------------------------------------------- --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ����������� ������ (����� ���������� ���������� ������) 
    --
    FUNCTION Period_for_close RETURN INTEGER;
    
    -- ------------------------------------------------------------------------- --
    -- ������������ ������ ��� �������� ���.���
    -- ------------------------------------------------------------------------- --
    PROCEDURE Billing_person( p_task_id IN INTEGER DEFAULT Pk00_Const.c_BILLING_MMTS );
                              
    -- ------------------------------------------------------------------------- --
    -- ������������ ������ ��� �������� ��.��� � ������ ������� � ��������
    -- ------------------------------------------------------------------------- --
    PROCEDURE Billing_jur_balance( p_task_id IN INTEGER DEFAULT Pk00_Const.c_BILLING_MMTS );
                                   
    -- ------------------------------------------------------------------------- --
    -- ������������ ������ ��� �������� ��.��� ��� ����� ������� � ��������
    -- ------------------------------------------------------------------------- --
    PROCEDURE Billing_jur( p_task_id IN INTEGER );                 
    
    -- ------------------------------------------------------------------------- --
    -- ��� ������� �������� ����� ������ ���� ������� ������ � BILLINFO_T
    -- ��������� ������ ���������� ������
    -- ------------------------------------------------------------------------- --
    PROCEDURE Check_Billinfo;
    
    -- ------------------------------------------------------------------------- --
    -- ��� ������ ������������ �� �������� �������� ������������� 
    -- ������ ������ ������ ����������� �� ������� 7704,
    -- ������������ �� ����������� �����
    -- ��� ������� �����, ������� ��������� �������������
    -- �� �������� ������
    -- ------------------------------------------------------------------------- --
    PROCEDURE Check_invoice_rule( p_bill_period_id IN INTEGER );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ �� �������� �� ������ ���������� ������
    -- �� ������� ������ ������� ��������� ����� �������, ������� �� ����� 
    -- �� �������� ������ ������������, � ������ ������� ��� ����� ������ ������� 
    --
    PROCEDURE Calc_advance( p_bill_period_id IN INTEGER );
    
    -- ===================================================================== --
    --              � � � � � �    � � �    � � � � � � � �                  --
    -- ===================================================================== --
    -- ��������������� ������, ��� �������� ������������� ���������� 
    PROCEDURE Rollback_bills( p_task_id IN INTEGER );

    -- ��������������� ������, ��� ���������� ����������
    PROCEDURE Remake_bills( p_task_id IN INTEGER );

    -- �������� �������� �������� ��������, ��� �������� ����
    PROCEDURE Rollback_paytransfer( p_task_id IN INTEGER );

    -- ��������������� ������ � �������� ������� ������������� ����������
    PROCEDURE Rollback_billing( p_task_id IN INTEGER );

    -- �������������� ������, � ��������� ����������� ������������� ����������
    PROCEDURE Remake_billing( p_task_id IN INTEGER );

   
END PK30_BILLING;
/
CREATE OR REPLACE PACKAGE BODY PK30_BILLING
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� �������� �� ��������� ������
-- ����� ��������� �� �������� ��������, ������� 
-- UPDATE BILLINFO_T �� ��������� ������ last <- bill, bill <- next, next <- null
-- �����, ����������, ������� ����� ��� next
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 
PROCEDURE Next_Bill_Period 
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Next_Bill_Period';
BEGIN
    --
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������� �� ��������� ����������� ������
    Pk04_Period.Next_bill_period;
    --
    COMMIT;
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------------- --
--                               � � � � � � �
-- ----------------------------------------------------------------------- --
-- ��������� ������������ ������ ��� ���� ���������,
-- ������ �������� �� ������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Billing( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Begin_Billing';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ���������������� �������� (��������� �����������!!!)
    Begin_Billing( p_bill_period_id );

    -- ������������ ����� �������� ���. ���
    Make_person_bills( p_bill_period_id );
    
    -- ������������ ����� �������� ��.��� �������� ���� (���������)
    Make_MMTS_bills( p_bill_period_id );

    -- ������������ ����� �������� ��.��� �������� ���� (PORTAL 6.5 + �����)
    Make_KTTK_bills( p_bill_period_id );

    -- ������������ ����� �������� ��.��� ������� � ��� 
    Make_SPB_bills( p_bill_period_id );
    
    -- ������������ ����� �������� ��.��� ����������� (���� ������) 
    Make_NTK_bills( p_bill_period_id );

    -- ��������� � ����� ����������� �� ������� 
    Downtime_processing( p_period_id => p_bill_period_id );

    -- ������ ��������� ������
    Make_discounts( p_bill_period_id );

    -- �������������� ��������
    End_Billing( p_bill_period_id );
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ���������������� �������� (��������� �����������!!!)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Begin_Billing( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Begin_Billing';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ��� ������� �������� ����� ������ ���� ������� ������ � BILLINFO_T
    -- ��������� ������ ���������� ������
    Check_Billinfo;
    
    -- ��� ������ ������������ �� �������� �������� ������������� 
    -- ������ ������ ������ ����������� �� ������� 7704,
    -- ������������ �� ����������� �����
    -- ��� ������� �����, ������� ��������� �������������
    -- �� �������� ������
    Check_invoice_rule( p_bill_period_id => p_bill_period_id );
    
    --    
    -- ���������� ����, ��������� ������� � ����������
    -- �� ��� ��� ���� ���� ������� - ��� ���������
    PK30_BILLING_BASE.Correct_tax_incl( p_period_id => p_bill_period_id );
    --
    -- ���������/���������� ���� �������� � ������
    PK30_BILLING_BASE.Correct_region_bill( p_period_id  => p_bill_period_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������������� ������ ��� �������� �������
    -- ��������� ��� ������� �� ����������� ���������
    -- � ����������� ������� p_period_id
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK36_BILLING_FIXRATE.Make_bills_for_fixrates(p_period_id => p_bill_period_id );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� BDR �� ���� � ������������ Item-��
    PK24_CCAD.Load_BDRs(p_period_id => p_bill_period_id);
    --
   
    -- ������ ������� 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
    --
    COMMIT;
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������������� ��������
-- ������ ��������� - ��������� ����, �� ��������� �������� �� PORTAL6.5
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE End_Billing( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'End_Billing';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� ���� ������� ������
    -- ������ �������� ��������, �� ���������� ������� ������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK34_BILLING_UNOFFICIAL.Recalc_all_period_info ( p_bill_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� �������, ���� ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK34_BILLING_UNOFFICIAL.Recalc_all_balances;
    --
    --    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ����� � ������ ����� � ������ (READY) �� ������� �������� (CHECK)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Set_billstatus_ready( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_billstate_ready';
    v_count      INTEGER;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- ��������� �������������� � ����� �� ������� CHECK � READY
    UPDATE BILL_T B  
       SET B.BILL_STATUS = Pk00_Const.c_BILL_STATE_READY 
    WHERE B.BILL_STATUS  = Pk00_Const.c_BILL_STATE_CHECK
      AND B.REP_PERIOD_ID = p_bill_period_id
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Processed: '||v_count||' - bills', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- ������������ ����� �������� ���. ���
-- ------------------------------------------------------------------------- --
PROCEDURE Make_person_bills( p_period_id  IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_person_bills';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ��������� �����
    Billing_person ( p_task_id   => Pk00_Const.c_BILLING_MMTS );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- ������������ ����� �������� ��.��� �������� ���� (���������)
-- ------------------------------------------------------------------------- --
PROCEDURE Make_MMTS_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_MMTS_bills';
    v_task_id    CONSTANT INTEGER := Pk00_Const.c_BILLING_MMTS;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ������ ������� 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
    -- ������ � ������� �� ������������ ����� ��.��� �������� ���� (���������)
    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => PK00_CONST.c_BILLING_MMTS, 
                                  p_task_id      => PK00_CONST.c_BILLING_MMTS,
                                  p_account_type => PK00_CONST.c_ACC_TYPE_J);
    -- ��������� �����
    Billing_jur_balance( p_task_id  =>  v_task_id);
    
    -- ��������� ����� � ������ �������� (CHECK) - ����������� ����������� ������
    Pk30_Billing_Base.Set_billstatus_check( p_task_id => v_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- ������������ ����� �������� ��.��� �������� ���� (PORTAL 6.5 + �����)
-- ------------------------------------------------------------------------- --
PROCEDURE Make_KTTK_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_KTTK_bills';
    v_task_id    CONSTANT INTEGER := Pk00_Const.c_BILLING_KTTK;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ������ ������� 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';
    -- ������� "�������" �������� (PORTAL 6.5)
    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => PK00_CONST.c_BILLING_OLD,
                                  p_task_id      => PK00_CONST.c_BILLING_KTTK, 
                                  p_account_type => PK00_CONST.c_ACC_TYPE_J);
    -- ������� ��������� ��������������� � BRM
    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => PK00_CONST.c_BILLING_KTTK, 
                                  p_task_id      => PK00_CONST.c_BILLING_KTTK,
                                  p_account_type => PK00_CONST.c_ACC_TYPE_J);

    -- ��������� �����
    Billing_jur( p_task_id   => v_task_id );

    -- ��������� ����� � ������ �������� (CHECK) - ����������� ����������� ������
    Pk30_Billing_Base.Set_billstatus_check( p_task_id => v_task_id );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- ������������ ����� �������� ��.��� ������� � ��� 
-- ------------------------------------------------------------------------- --
PROCEDURE Make_SPB_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_SPB_bills';
    v_task_id    CONSTANT INTEGER := Pk00_Const.c_BILLING_SPB;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ������ ������� 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';

    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => PK00_CONST.c_BILLING_SPB,
                                  p_task_id      => PK00_CONST.c_BILLING_SPB, 
                                  p_account_type => PK00_CONST.c_ACC_TYPE_J);
    -- ��������� �����
    Billing_jur( p_task_id   => v_task_id );
    
    -- ��������� ����� � ������ �������� (CHECK) - ����������� ����������� ������
    Pk30_Billing_Base.Set_billstatus_check( p_task_id => v_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- ������������ ����� �������� ��.��� ����������� (���� ������) 
-- ------------------------------------------------------------------------- --
PROCEDURE Make_NTK_bills( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_NTK_bills';
    v_task_id    CONSTANT INTEGER := Pk00_Const.c_BILLING_ACCESS;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ������ ������� 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BILLING_QUEUE_T DROP STORAGE';

    PK30_BILLING_BASE.Mark_bills( p_period_id    => p_period_id, 
                                  p_billing_id   => PK00_CONST.c_BILLING_ACCESS, 
                                  p_task_id      => PK00_CONST.c_BILLING_ACCESS,
                                  p_account_type => PK00_CONST.c_ACC_TYPE_J);
    
    -- ��������� �����
    Billing_jur( p_task_id   => v_task_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- ��������� ��������� ������ 
-- ------------------------------------------------------------------------- --
PROCEDURE Make_discounts( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Make_discounts';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������������ ������� ������    
    PK39_BILLING_DISCOUNT.Apply_discounts( p_period_id => p_period_id );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ---------------------------------------------------------------------- --
-- ��������� � ����� ����������� �� ������� 
-- ---------------------------------------------------------------------- --
PROCEDURE Downtime_processing( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_Financial_Period';
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    PK38_BILLING_DOWNTIME.Downtime_processing( p_period_id );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ����������� �������
-- ������� ������� ����� �������� ������� (�������� ������ �� CLOSED - ��������� � ��� ��� ���������)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Close_Financial_Period
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_Financial_Period';
    v_period_id  INTEGER;
    v_count      INTEGER;
BEGIN
    -- �������� ���� ������� (����� ���������� ���������� ������)
    v_period_id := Period_for_close;
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||v_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk04_Period.Close_fin_period;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� �������� (��������������) ������ (���� ��������� � ����������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk04_Period.Close_rep_period;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ������� ����� �������� ������� (��������� � ��� ��� ���������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE ITEM_T
       SET ITEM_STATUS = Pk00_Const.c_ITEM_STATE_CLOSED
     WHERE REP_PERIOD_ID = v_period_id
       AND ITEM_TYPE IN (Pk00_Const.c_ITEM_TYPE_BILL, Pk00_Const.c_ITEM_TYPE_ADJUST);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('������� '||v_count||' ������� ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������� ����� �������� ������� (��������� � ��� ��� ���������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    UPDATE BILL_T 
       SET BILL_STATUS = Pk00_Const.c_BILL_STATE_CLOSED
     WHERE REP_PERIOD_ID = v_period_id
       AND BILL_STATUS IN ( Pk00_Const.c_BILL_STATE_READY, PK00_CONST.c_BILL_STATE_EMPTY);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('������� '||v_count||' ������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ �� �������� �� ������ ���������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Calc_advance( v_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� ���� ������� ������
    -- ������ �������� ��������, �� ���������� ������� ������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk34_Billing_Unofficial.Recalc_all_period_info ( v_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� �������, ���� ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk34_Billing_Unofficial.Recalc_all_balances;
    --
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
--                   � � � � � � � � �   � � � � � � � � �
-- ------------------------------------------------------------------------- --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ����������� ������ (����� ���������� ���������� ������) 
--
FUNCTION Period_for_close RETURN INTEGER
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Period_for_close';
    v_period_id INTEGER;
BEGIN
    SELECT PERIOD_ID
      INTO v_period_id
      FROM PERIOD_T
     WHERE CLOSE_FIN_PERIOD IS NULL
       AND POSITION = PK00_CONST.c_PERIOD_BILL;
    RETURN v_period_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- ------------------------------------------------------------------------- --
-- ������������ ������ ��� �������� ���.���
-- ------------------------------------------------------------------------- --
PROCEDURE Billing_person( p_task_id IN INTEGER DEFAULT Pk00_Const.c_BILLING_MMTS )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing_person';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ����������� ����� � ������� �� ����������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Charge_fixrates( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Make_bills( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- �������� ��� ��� �������� �� �������� �� ����������� ������ �� �������������� �����
    -- �������� ������ �/� �� �������� ������ �/� �� ��������
    -- �������� ��� '�' �������������� FIFO, '�'-������ ����� ��� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK10_PAYMENTS_TRANSFER.Method_fifo;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ������ �� �������� �� ������ ���������� ������
    -- �� ������� ������ ������� ��������� ����� �������, ������� �� ����� 
    -- �� �������� ������ ������������, � ������ ������� ��� ����� ������ ������� 
    --
    --PK30_BILLING_BASE.Calc_advance( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� ���� ������� ������
    -- ������ �������� ��������, �� ���������� ������� ������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --PK34_BILLING_UNOFFICIAL.Recalc_all_period_info ( p_period_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ����������� �������, ���� ������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    --PK34_BILLING_UNOFFICIAL.Recalc_all_balances;
    --
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- ������������ ������ ��� �������� ��.��� � ������ ������� � ��������
-- ------------------------------------------------------------------------- --
PROCEDURE Billing_jur_balance( p_task_id IN INTEGER DEFAULT Pk00_Const.c_BILLING_MMTS )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing_jur_balance';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ����������� ����� � ������� �� ����������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Charge_fixrates( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Make_bills( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ��������� �������� ������� (����� ����� ������ ��� � ���.���)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Close_period( p_task_id );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- 
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- ������������ ������ ��� �������� ��.��� ��� ����� ������� � ��������
-- ------------------------------------------------------------------------- --
PROCEDURE Billing_jur( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Billing_jur';
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ����������� ����� � ������� �� ����������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Charge_fixrates( p_task_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������ ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Make_bills( p_task_id );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���������� ������ �� ������ ��� �/� ������, ������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PK30_BILLING_BASE.Period_info( p_task_id ); 
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- ��� ������� �������� ����� ������ ���� ������� ������ � BILLINFO_T
-- ��������� ������ ���������� ������
-- ------------------------------------------------------------------------- --
PROCEDURE Check_Billinfo
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_Billinfo';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    MERGE INTO BILLINFO_T BI
    USING (
        SELECT A.ACCOUNT_ID, A.CURRENCY_ID 
          FROM ACCOUNT_T A
         WHERE A.STATUS = PK00_CONST.c_ACC_STATUS_BILL
    ) AA
    ON ( AA.ACCOUNT_ID = BI.ACCOUNT_ID)
    WHEN NOT MATCHED THEN 
        INSERT (
            ACCOUNT_ID,      -- ID �������� �����
            PERIOD_LENGTH,
            CURRENCY_ID,
            DAYS_FOR_PAYMENT
        ) VALUES (
            AA.ACCOUNT_ID,
            1,
            AA.CURRENCY_ID,
            NULL    -- �� ��������� �����    
        );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLINFO_T '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------- --
-- ��� ������ ������������ �� �������� �������� ������������� 
-- ������ ������ ������ ����������� �� ������� 7704,
-- ������������ �� ����������� �����
-- ��� ������� �����, ������� ��������� �������������
-- �� �������� ������
-- ------------------------------------------------------------------------- --
PROCEDURE Check_invoice_rule( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_invoice_rule';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ����������� ������� ��� ������
    UPDATE BILL_T B
       SET B.INVOICE_RULE_ID = Pk00_Const.c_INVOICE_RULE_SUB_STD
     WHERE EXISTS (
       SELECT * 
         FROM ORDER_T O
        WHERE O.ACCOUNT_ID = B.ACCOUNT_ID
          AND O.SERVICE_ID = Pk00_Const.c_SERVICE_OP_LOCAL -- 7
       )
       AND B.REP_PERIOD_ID    = p_bill_period_id
       AND B.BILL_TYPE        = Pk00_Const.c_BILL_TYPE_REC -- 'B';
       AND B.INVOICE_RULE_ID != Pk00_Const.c_INVOICE_RULE_SUB_STD
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- ����������� ������� ��� �/�
    UPDATE BILLINFO_T BI
       SET BI.INVOICE_RULE_ID = Pk00_Const.c_INVOICE_RULE_SUB_STD
     WHERE EXISTS (
       SELECT * 
         FROM ORDER_T O
        WHERE O.ACCOUNT_ID = BI.ACCOUNT_ID
          AND O.SERVICE_ID = Pk00_Const.c_SERVICE_OP_LOCAL -- 7
       )
       AND BI.INVOICE_RULE_ID != Pk00_Const.c_INVOICE_RULE_SUB_STD
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILLINFO_T '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������ �� �������� �� ������ ���������� ������
-- �� ������� ������ ������� ��������� ����� �������, ������� �� ����� 
-- �� �������� ������ ������������, � ������ ������� ��� ����� ������ ������� 
--
PROCEDURE Calc_advance( p_bill_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Calc_advance';
    v_count      INTEGER;
    v_period     DATE;
BEGIN
    --
    Pk01_Syslog.Write_msg('Start, period_id = '||p_bill_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    v_period := PK04_PERIOD.Period_from(p_bill_period_id);
    --
    -- ���������� ��� ��������� ������
    UPDATE PAYMENT_T P SET P.ADVANCE = 0, P.ADVANCE_DATE = v_period
     WHERE P.REP_PERIOD_ID= p_bill_period_id
     ;
    MERGE INTO PAYMENT_T P
    USING (
        SELECT PAYMENT_ID, PAY_PERIOD_ID, SUM(TRANSFER_TOTAL) FOR_SERVICE 
          FROM PAY_TRANSFER_T T
        WHERE PAY_PERIOD_ID >= REP_PERIOD_ID     -- �� ��������� ������
          AND PAY_PERIOD_ID = p_bill_period_id
        GROUP BY PAYMENT_ID, PAY_PERIOD_ID
    ) T
    ON (P.PAYMENT_ID = T.PAYMENT_ID
        AND P.REP_PERIOD_ID = T.PAY_PERIOD_ID
       )
    WHEN MATCHED THEN UPDATE SET P.ADVANCE = P.RECVD-T.FOR_SERVICE, 
         P.ADVANCE_DATE = ADD_MONTHS(TRUNC(P.PAYMENT_DATE,'mm'),1)-1/86400;
    v_count := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg('Stop, processed: '||v_count||' payments', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
--              � � � � � �    � � �    � � � � � � � �                  --
--     ����������� ����������� ���������� PK30_BILLING.END_BILLING       --
-- --------------------------------------------------------------------- --
-- ��������������� ������, ��� �������� ������������� ���������� 
PROCEDURE Rollback_bills( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_bills';
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk30_Billing_Queue.Rollback_bills(p_task_id);
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������������� ������, ��� ���������� ����������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Remake_bills( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Remake_bills';
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk30_Billing_Base.Make_bills(p_task_id);
    
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������������� ������ � �������� ������� ������������� ����������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_billing( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Rollback_billing';
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ���������������� �����
    Pk30_Billing_Queue.Rollback_bills(p_task_id);
    -- ������� ������������� ���������� - ������ � ���������
    Pk36_Billing_Fixrate.Rollback_fixrates(p_task_id);
    -- ������� ����������� �� �������
    Pk38_Billing_Downtime.Rollback_downtimes(p_task_id);
    --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������������� ������, � ��������� ����������� ������������� ����������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Remake_billing( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Remake_billing';
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- �������� ������������� ����������: ������ � ���������
    Pk36_Billing_Fixrate.Charge_fixrates(p_task_id);
    -- �������� ���������� ����������� �� ������
    Pk38_Billing_Downtime.Recharge_downtimes(p_task_id);
    -- ��������� �����
    Pk30_Billing_Base.Make_bills(p_task_id);
    
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� �������� �������� ��������, ��� �������� ����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Rollback_paytransfer( p_task_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Remake_billing';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ������� ��� �������� ��������
    FOR tr IN (
        SELECT T.TRANSFER_ID, T.PAY_PERIOD_ID, T.PAYMENT_ID 
          FROM PAY_TRANSFER_T T, BILLING_QUEUE_T Q
         WHERE Q.BILL_ID       = T.BILL_ID
           AND Q.REP_PERIOD_ID = T.REP_PERIOD_ID
           AND Q.TASK_ID       = p_task_id
      )
    LOOP
      PK10_PAYMENTS_TRANSFER.Delete_from_chain (
               p_pay_period_id => tr.pay_period_id,
               p_payment_id    => tr.payment_id,
               p_transfer_id   => tr.transfer_id
           );
       v_count := v_count + 1;
    END LOOP;

    Pk01_Syslog.Write_msg('pay_transfer_t '|| v_count ||' rows deleted',
                        c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;



END PK30_BILLING;
/
