CREATE OR REPLACE PACKAGE PK10_BALANCE_OPERATION
IS
    --
    -- ����� ��� ������ �� ��������� �������, 
    -- ��� ������ � �����������/������������ �������������� (��/��)
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_BALANCE_OPERATION';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- ������� ������ DICTIONARY_T:
    k_dict_balance_oper  CONSTANT INTEGER := 33;   -- �������� � ��������� (��/��)
    c_op_bl_move         CONSTANT INTEGER := 3301; -- �������� �� �������� �������� ��/��(�������)
    c_op_bl_reset        CONSTANT INTEGER := 3302; -- �������� �� �������� ��/��(�������)
    c_op_bl_recovery     CONSTANT INTEGER := 3303; -- �������� �� �������������� ��/��(�������)
    c_op_bl_vz           CONSTANT INTEGER := 3304; -- ���������� �� �������� ��������������
    c_op_bl_adjust       CONSTANT INTEGER := 3305; -- ������������� ��������� ������ �� �����

    k_dict_bill_type     CONSTANT INTEGER := 3;    -- ���� ������ � ��������
    c_bill_type_x        CONSTANT CHAR    := 'X';  -- ������������� ������� (�����)

    k_dict_pay_type      CONSTANT INTEGER := 62;   -- ���� �������� � ��������

    -- �������������:
    c_vz_paysystem_id    CONSTANT INTEGER := 51;   -- ��������� ������� ��� �������������� (PAYSYSTEM_T)
    k_dict_vz_payment    CONSTANT INTEGER := 6208; -- ������������� � �������
    c_vz_payment_type    CONSTANT VARCHAR2(20) := 'AVZT'; 

    -- �������� ������������ ��/��:
    c_kz_paysystem_id    CONSTANT INTEGER := 52;   -- ��������� ������� ������������� ��/�� (PAYSYSTEM_T)
    k_dict_kz_payment    CONSTANT INTEGER := 6209; -- ������������ ������������� � �������
    c_kz_payment_type    CONSTANT VARCHAR2(20) := 'KZ'; -- ������������ �������������
        
    -- ������������� ��������� ������ �� �����
    c_1c_paysystem_id    CONSTANT INTEGER := 53;   -- ��������� ������� ������������� �� ����� (PAYSYSTEM_T)
    k_dict_1c_payment    CONSTANT INTEGER := 6210; -- ������������� �� �����
    c_1c_payment_type    CONSTANT VARCHAR2(20) := 'EISUP'; -- ������������� �� �����
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� �� �������� �������� ��/��(�������) � ������ ����������� 
    -- �� ������� � ������ ������������� ��� ����������� ����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Oper_move (
          p_src_account_id IN INTEGER, -- ������� � �/�
          p_dst_account_id IN INTEGER, -- �� �/�
          p_period_id      IN INTEGER, -- ������� � �������� ���������� ����� �����
          p_doc_date       IN DATE,    -- ���� ���������� ����������� ��������
          p_notes          IN VARCHAR2,-- ���������� � ��������
          p_manager        IN VARCHAR2 -- ��������� ����������� ��������
      ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� �� �������� ��/�� (�������) � �������� ������ ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Oper_reset (
          p_account_id     IN INTEGER, -- ������� � �/�
          p_period_id      IN INTEGER, -- ������� � �������� ���������� ����� �����
          p_doc_date       IN DATE,    -- ���� ���������� ����������� ��������
          p_notes          IN VARCHAR2,-- ���������� � ��������
          p_manager        IN VARCHAR2 -- ��������� ����������� ��������
      ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� �� �������������� ����� ��������� ��/��
    -- (�������������� �������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Oper_recovery (
          p_account_id     IN INTEGER, -- ������� � �/�
          p_period_id      IN INTEGER, -- ������� � �������� ���������� ����� �����
          p_doc_date       IN DATE,    -- ���� ���������� ����������� ��������
          p_notes          IN VARCHAR2,-- ���������� � ��������
          p_manager        IN VARCHAR2 -- ��������� ����������� ��������
      ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� �� �������� ��������������
    -- (��������� ������� ��� ��������������)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Oper_vz (
          p_account_id     IN INTEGER, -- ������� � �/�
          p_period_id      IN INTEGER, -- ������� � �������� ���������� ����� �����
          p_doc_date       IN DATE,    -- ���� ���������� ����������� ��������
          p_amount         IN NUMBER,  -- ����� ��������
          p_notes          IN VARCHAR2,-- ���������� � ��������
          p_descr          IN VARCHAR2,-- �������� ��������
          p_manager        IN VARCHAR2 -- ��������� ����������� ��������
      ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ������� ������������� �������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Delete_Oper_vz (
          p_oper_id        IN INTEGER  -- id ��������� �������� 
      );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� ��� ���������� ������������� ������� �� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Oper_1c (
          p_account_id     IN INTEGER, -- ������� � �/�
          p_period_id      IN INTEGER, -- ������� � �������� ���������� ����� �����
          p_doc_date       IN DATE,    -- ���� ���������� ����������� ��������
          p_amount         IN NUMBER,  -- ����� ��������
          p_descr          IN VARCHAR2,-- �������� ��������
          p_manager        IN VARCHAR2 -- ��������� ����������� ��������
      ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� �������� ��� ���������� ������������� ������� �� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Delete_Oper_1c (
          p_oper_id        IN INTEGER  -- id ��������� �������� 
      );


    -- ============================================================= --
    -- ��������� ���������
    -- ============================================================= --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ��������� �� ������ ���������� �� ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Read_ErrMsg (
          p_errmsg_id IN INTEGER
      ) RETURN VARCHAR2;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ���� ��� ������������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Create_adjust_bill_x (  
                   p_account_id    IN INTEGER, -- ID �������� �����
                   p_period_id     IN INTEGER, -- ID �������� ���������� ������� YYYYMM
                   p_bill_date     IN DATE,    -- ���� �����
                   p_bill_total    IN NUMBER,  -- ����� ���������� � ��������
                   p_notes         IN VARCHAR2,
                   p_manager       IN VARCHAR2
                ) RETURN INTEGER;
        
    -- ------------------------------------------------------------- --
    -- ������� ������ ��� ������������� ������� 
    -- ��� ������������� ������������ �������������
    -- ------------------------------------------------------------- --
    FUNCTION Create_adjust_payment_kz (  
                   p_account_id    IN INTEGER, -- ID �������� �����
                   p_period_id     IN INTEGER, -- ID �������� ���������� ������� YYYYMM
                   p_payment_date  IN DATE,    -- ���� �������
                   p_amount        IN NUMBER,  -- ����� �������
                   p_manager       IN VARCHAR2,
                   p_notes         IN VARCHAR2,-- ���������� (����� �����)
                   p_descr         IN VARCHAR2 DEFAULT NULL -- �������� ������� (���. ��������)
                ) RETURN INTEGER;

    -- ------------------------------------------------------------- --
    -- ������� ������ ��� ���������� �������������� 
    -- ------------------------------------------------------------- --
    FUNCTION Create_adjust_payment_vz (  
                   p_account_id    IN INTEGER, -- ID �������� �����
                   p_period_id     IN INTEGER, -- ID �������� ���������� ������� YYYYMM
                   p_payment_date  IN DATE,    -- ���� �������
                   p_amount        IN NUMBER,  -- ����� �������
                   p_manager       IN VARCHAR2,
                   p_notes         IN VARCHAR2,-- ���������� (����� �����)
                   p_descr         IN VARCHAR2 DEFAULT NULL -- �������� ������� (���. ��������)
                ) RETURN INTEGER;

    -- ------------------------------------------------------------- --
    -- ������� ������ ��� ���������� ������������� ������� �� ����� 
    -- ------------------------------------------------------------- --
    FUNCTION Create_adjust_payment_1c (  
                   p_account_id    IN INTEGER, -- ID �������� �����
                   p_period_id     IN INTEGER, -- ID �������� ���������� ������� YYYYMM
                   p_payment_date  IN DATE,    -- ���� �������
                   p_amount        IN NUMBER,  -- ����� �������
                   p_manager       IN VARCHAR2,
                   p_notes         IN VARCHAR2,-- ���������� (����� �����)
                   p_descr         IN VARCHAR2 DEFAULT NULL -- �������� ������� (���. ��������)
                ) RETURN INTEGER;

    -- ------------------------------------------------------------- --
    -- ������ �������� �� ������ (������)
    -- ------------------------------------------------------------- --
    PROCEDURE Balance_oper_list( 
                   p_recordset    OUT t_refc, 
                   p_rep_period_id IN INTEGER,    -- ID ������� �����
                   p_oper_type_id  in number
               );

    -- ------------------------------------------------------------- --
    -- ����� ID �������� ����� �� ������( ������������ � GUI )
    -- ------------------------------------------------------------- --
    FUNCTION get_account_id (
          p_account_no     IN VARCHAR2 -- �/�
      ) RETURN INTEGER;


END PK10_BALANCE_OPERATION;
/
CREATE OR REPLACE PACKAGE BODY PK10_BALANCE_OPERATION
IS

-- ------------------------------------------------------------- --
-- �������� �� �������� �������� ��/��(�������) � ������ ����������� 
-- �� ������� � ������ ������������� ��� ����������� ����
-- ------------------------------------------------------------- --
FUNCTION Oper_move (
      p_src_account_id IN INTEGER, -- ������� � �/�
      p_dst_account_id IN INTEGER, -- �� �/�
      p_period_id      IN INTEGER, -- ������� � �������� ���������� ����� �����
      p_doc_date       IN DATE,    -- ���� ���������� ����������� ��������
      p_notes          IN VARCHAR2,-- ���������� � ��������
      p_manager        IN VARCHAR2 -- ��������� ����������� ��������
  ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Oper_move';
    v_oper_id        INTEGER;
    v_balance        NUMBER;
    v_src_bill_id    INTEGER;
    v_dst_bill_id    INTEGER;
    v_src_payment_id INTEGER;
    v_dst_payment_id INTEGER;
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1) �������� � ��������� ������� �/� ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_balance := PK05_ACCOUNT_BALANCE.Refresh_balance(p_src_account_id);
        
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3) ��������: ��������� �������������� ���� ��� (+)������������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    IF v_balance > 0 THEN
      v_src_bill_id := Create_adjust_bill_x(
               p_account_id => p_src_account_id,
               p_period_id  => p_period_id,
               p_bill_date  => p_doc_date,
               p_bill_total => v_balance,
               p_notes      => p_notes,
               p_manager    => p_manager
            );
    END IF;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3) ��������: ��������� �������������� ������ ��� (-)������������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    IF v_balance < 0 THEN
      v_src_payment_id := Create_adjust_payment_kz (  
               p_account_id   => p_src_account_id,
               p_period_id    => p_period_id,
               p_payment_date => p_doc_date,
               p_amount       => -v_balance,
               p_notes        => p_notes,
               p_manager      => p_manager
            );
    END IF;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 4) ����������: ��������� �������������� ���� ��� (-)������������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    IF v_balance < 0 THEN
      v_dst_bill_id := Create_adjust_bill_x(
               p_account_id => p_dst_account_id,
               p_period_id  => p_period_id,
               p_bill_date  => p_doc_date,
               p_bill_total => -v_balance,
               p_notes      => p_notes,
               p_manager    => p_manager
            );
    END IF;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 5) ����������: ��������� �������������� ������ ��� (+)������������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    IF v_balance < 0 THEN
      v_dst_payment_id := Create_adjust_payment_kz (  
               p_account_id   => p_dst_account_id,
               p_period_id    => p_period_id,
               p_payment_date => p_doc_date,
               p_amount       => v_balance,
               p_notes        => p_notes,
               p_manager      => p_manager
            );
    END IF;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 6) ������������ ������ � ����������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_oper_id := Pk02_Poid.Next_operation_id;
    --
    INSERT INTO PAYMENT_OPERATION_T(
      OPER_ID, OPER_TYPE_ID, OPER_DATE, OPER_TOTAL, 
      SRC_PAYMENT_ID, SRC_REP_PERIOD_ID, 
      DST_PAYMENT_ID, DST_REP_PERIOD_ID, 
      CREATED_BY, NOTES, 
      SRC_ACCOUNT_ID, DST_ACCOUNT_ID, 
      SRC_BILL_ID, DST_BILL_ID
    )VALUES(
      v_oper_id, c_op_bl_move, SYSDATE, v_balance,
      v_src_payment_id, p_period_id,
      v_dst_payment_id, p_period_id,
      p_manager, p_notes,
      p_src_account_id, p_src_account_id,
      v_src_bill_id, v_dst_bill_id
    );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 7) �������� � ��������� ������� �/� ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_balance := PK05_ACCOUNT_BALANCE.Refresh_balance(p_src_account_id);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 8) �������� � ��������� ������� �/� ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_balance := PK05_ACCOUNT_BALANCE.Refresh_balance(p_dst_account_id);
    
    RETURN v_oper_id;
EXCEPTION
    WHEN OTHERS THEN
        --RETURN -Pk01_Syslog.Fn_write_error('ERROR', c_PkgName||'.'||v_prcName );
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- ------------------------------------------------------------- --
-- �������� �� �������� ��/�� (�������) � �������� ������ ��������
-- ����������:
-- >0: id - ��������
-- <0: ��������������� id ��������� �� ������, ������� ������ �������� "Read_ErrMsg"
-- ------------------------------------------------------------- --
FUNCTION Oper_reset (
      p_account_id     IN INTEGER, -- ������� � �/�
      p_period_id      IN INTEGER, -- ������� � ������� ���������� ��������
      p_doc_date       IN DATE,    -- ���� ���������� ����������� ��������
      p_notes          IN VARCHAR2,-- ���������� � ��������
      p_manager        IN VARCHAR2 -- ��������� ����������� ��������
  ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Oper_move';
    v_oper_id    INTEGER;
    v_balance    NUMBER;
    v_bal_date   DATE;
    v_bill_id    INTEGER := NULL;
    v_payment_id INTEGER := NULL;
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1) �� ������ ������ ������������� ������ � �������� �������� �������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_balance := PK05_ACCOUNT_BALANCE.Refresh_balance(p_account_id);
  
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --  
    -- 2) ��������� ��� ������ �������� �� ���� �������� ������� �� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    SELECT A.BALANCE_DATE INTO v_bal_date
      FROM ACCOUNT_T A
     WHERE A.ACCOUNT_ID = p_account_id;
     
    IF v_bal_date > p_doc_date THEN
      --RETURN -Pk01_Syslog.Fn_write_msg('������, ����� ������� �������� ���� �������� �������', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err);
      raise_application_error(-20001,'������, ����� ������� �������� ���� �������� �������');
    END IF;
   
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3) ��������� �������������� ���� ��� (+) ������������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    IF v_balance > 0 THEN
      v_bill_id := Create_adjust_bill_x(
               p_account_id => p_account_id,
               p_period_id  => p_period_id,
               p_bill_date  => p_doc_date,
               p_bill_total => v_balance,
               p_notes      => p_notes,
               p_manager    => p_manager
            );
    END IF;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 4) ��������� �������������� ������ ��� ������������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    IF v_balance < 0 THEN
      v_payment_id := Create_adjust_payment_kz (  
               p_account_id   => p_account_id, -- ID �������� �����
               p_period_id    => p_period_id, -- ID �������� ���������� ������� YYYYMM
               p_payment_date => p_doc_date,    -- ���� �������
               p_amount       => -v_balance,  -- ����� �������
               p_notes        => p_notes,
               p_manager      => p_manager
            );
    END IF;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 5) ��������� ����� � ����������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_oper_id := Pk02_Poid.Next_operation_id;
    --
    INSERT INTO PAYMENT_OPERATION_T(
      OPER_ID, OPER_TYPE_ID, OPER_DATE, OPER_TOTAL, 
      SRC_PAYMENT_ID, SRC_REP_PERIOD_ID, 
      DST_PAYMENT_ID, DST_REP_PERIOD_ID, 
      CREATED_BY, NOTES, 
      SRC_ACCOUNT_ID, DST_ACCOUNT_ID, 
      SRC_BILL_ID, DST_BILL_ID
    )VALUES(
      v_oper_id, c_op_bl_reset, SYSDATE, v_balance,
      NULL, p_period_id,
      v_payment_id, p_period_id,
      p_manager, p_notes,
      p_account_id, p_account_id,
      NULL, v_bill_id
    );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 6) �� ������ ������ ������������� ������ � �������� �������� �������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_balance := PK05_ACCOUNT_BALANCE.Refresh_balance(p_account_id);
    
    RETURN v_oper_id;
EXCEPTION
    WHEN OTHERS THEN
        --RETURN -Pk01_Syslog.Fn_write_error('ERROR', c_PkgName||'.'||v_prcName );
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- ------------------------------------------------------------- --
-- �������� �� �������������� ����� ��������� ��/��
-- (�������������� �������)
-- ------------------------------------------------------------- --
FUNCTION Oper_recovery (
      p_account_id     IN INTEGER, -- ������� � �/�
      p_period_id      IN INTEGER, -- ������� � �������� ���������� ����� �����
      p_doc_date       IN DATE,    -- ���� ���������� ����������� ��������
      p_notes          IN VARCHAR2,-- ���������� � ��������
      p_manager        IN VARCHAR2 -- ��������� ����������� ��������
  ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Oper_recovery';
    v_oper_id        INTEGER;
    v_total          NUMBER;
    v_balance        NUMBER;
    v_src_period_id  INTEGER := NULL;
    v_src_bill_id    INTEGER := NULL;
    v_dst_bill_id    INTEGER := NULL;
    v_src_payment_id INTEGER := NULL;
    v_dst_payment_id INTEGER := NULL;
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1) ����� ���������� ����� ��������������� ������
    SELECT MAX(B.BILL_ID) INTO v_src_bill_id
      FROM BILL_T B
     WHERE B.ACCOUNT_ID = p_account_id
       AND B.BILL_TYPE  = 'X';
   
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2) ������������ ��������������� �������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    IF v_src_bill_id IS NOT NULL THEN
      -- ������ ��������� ��������������� �����
      SELECT B.REP_PERIOD_ID, B.TOTAL
        INTO v_src_period_id, v_total 
        FROM BILL_T B
       WHERE B.BILL_ID = v_src_bill_id;
       
      -- ��������� �������������� ������
      v_dst_payment_id := Create_adjust_payment_kz (  
               p_account_id   => p_account_id, -- ID �������� �����
               p_period_id    => p_period_id,  -- ID �������� ���������� ������� YYYYMM
               p_payment_date => p_doc_date,   -- ���� �������
               p_amount       => v_total,      -- ����� �������
               p_notes        => p_notes,
               p_manager      => p_manager
            );
       
    END IF;
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3) ����� ���������� ������� ��������������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    SELECT MAX(P.PAYMENT_ID) INTO v_src_payment_id
      FROM PAYMENT_T P
     WHERE P.ACCOUNT_ID   = p_account_id
       AND P.PAYMENT_TYPE = Pk00_Const.c_PAY_TYPE_ADJUST_BALANCE
       AND P.PAYSYSTEM_ID = Pk00_Const.c_PAYSYSTEM_CORRECT_ID;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 4) ������������ ��������������� �����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    IF v_src_payment_id IS NOT NULL THEN
      -- ������ ��������� ��������������� �������
      SELECT P.REP_PERIOD_ID, P.RECVD
        INTO v_src_period_id, v_total 
        FROM PAYMENT_T P
       WHERE P.PAYMENT_ID = v_src_payment_id;
       -- ��������� �������������� ����
      v_dst_bill_id := Create_adjust_bill_x(
               p_account_id => p_account_id,
               p_period_id  => p_period_id,
               p_bill_date  => p_doc_date,
               p_bill_total => v_total,
               p_notes      => p_notes,
               p_manager    => p_manager
            );
    END IF;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 5) ������������ ������ � ����������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_oper_id := Pk02_Poid.Next_operation_id;
    --
    INSERT INTO PAYMENT_OPERATION_T(
      OPER_ID, OPER_TYPE_ID, OPER_DATE, OPER_TOTAL, 
      SRC_PAYMENT_ID, SRC_REP_PERIOD_ID, 
      DST_PAYMENT_ID, DST_REP_PERIOD_ID, 
      CREATED_BY, NOTES, 
      SRC_ACCOUNT_ID, DST_ACCOUNT_ID, 
      SRC_BILL_ID, DST_BILL_ID
    )VALUES(
      v_oper_id, c_op_bl_recovery, SYSDATE, v_total,
      v_src_payment_id, v_src_period_id,
      v_dst_payment_id, p_period_id,
      p_manager, p_notes,
      p_account_id, p_account_id,
      v_src_bill_id, v_dst_bill_id
    );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 6) �� ������ ������ ������������� ������ � �������� �������� �������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_balance := PK05_ACCOUNT_BALANCE.Refresh_balance(p_account_id);
    
    RETURN v_oper_id;
EXCEPTION
    WHEN OTHERS THEN
        --RETURN -Pk01_Syslog.Fn_write_error('ERROR', c_PkgName||'.'||v_prcName );
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� �� �������� ��������������, 
-- ��������� ������ ������������ �������������
-- (��������� ������� ��� ��������������)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Oper_vz (
      p_account_id     IN INTEGER, -- ������� � �/�
      p_period_id      IN INTEGER, -- ������� � �������� ���������� ����� �����
      p_doc_date       IN DATE,    -- ���� ���������� ����������� ��������
      p_amount         IN NUMBER,  -- ����� ��������
      p_notes          IN VARCHAR2,-- ���������� � ��������
      p_descr          IN VARCHAR2,-- �������� ��������
      p_manager        IN VARCHAR2 -- ��������� ����������� ��������
  ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Oper_vz';
    v_oper_id        INTEGER;
    v_dst_payment_id INTEGER;
    v_balance        NUMBER;
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� ���������������� ������ ��� ��������� ��������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_dst_payment_id := Create_adjust_payment_vz (  
             p_account_id   => p_account_id, -- ID �������� �����
             p_period_id    => p_period_id,  -- ID �������� ���������� ������� YYYYMM
             p_payment_date => p_doc_date,   -- ���� �������
             p_amount       => p_amount,     -- ����� �������
             p_notes        => p_notes,
             p_descr        => p_descr,
             p_manager      => p_manager
          );
          
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 5) ������������ ������ � ����������� ��������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_oper_id := Pk02_Poid.Next_operation_id;
    --
    INSERT INTO PAYMENT_OPERATION_T(
      OPER_ID, OPER_TYPE_ID, OPER_DATE, OPER_TOTAL, 
      SRC_PAYMENT_ID, SRC_REP_PERIOD_ID, 
      DST_PAYMENT_ID, DST_REP_PERIOD_ID, 
      CREATED_BY, NOTES, 
      SRC_ACCOUNT_ID, DST_ACCOUNT_ID, 
      SRC_BILL_ID, DST_BILL_ID
    )VALUES(
      v_oper_id, c_op_bl_vz, SYSDATE, p_amount,
      NULL, p_period_id,
      v_dst_payment_id, p_period_id,
      p_manager, p_notes,
      p_account_id, p_account_id,
      NULL, NULL
    );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 6) �� ������ ������ ������������� ������ � �������� �������� �������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    v_balance := PK05_ACCOUNT_BALANCE.Refresh_balance(p_account_id);
    
    RETURN v_oper_id;
EXCEPTION
    WHEN OTHERS THEN
        --RETURN -Pk01_Syslog.Fn_write_error('ERROR', c_PkgName||'.'||v_prcName );
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;  

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������� ������������� �������������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Delete_Oper_vz (
      p_oper_id        IN INTEGER  -- id ��������� �������� 
  )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Delete_Oper_vz';
    v_oper_id        INTEGER;
    v_account_id     INTEGER;
    v_dst_payment_id INTEGER;
    v_dst_bill_id    INTEGER;
    v_period_id      INTEGER;
    v_retcode        INTEGER;
    v_balance        NUMBER;
BEGIN
    -- �������� ���������� �� ��������
    SELECT PO.DST_REP_PERIOD_ID, 
           PO.DST_PAYMENT_ID, PO.DST_BILL_ID, PO.DST_ACCOUNT_ID
      INTO v_period_id, v_dst_payment_id, v_dst_bill_id, v_account_id 
      FROM PAYMENT_OPERATION_T PO
     WHERE PO.OPER_ID = p_oper_id;

    -- ������� ������ �� ��������
    DELETE FROM PAYMENT_OPERATION_T PO
     WHERE PO.OPER_ID = p_oper_id;
    
    -- �������������� ������:
    IF v_dst_payment_id IS NOT NULL THEN
      -- ������� ��� ������� �������� �������
      PK10_PAYMENTS_TRANSFER.Delete_transfer_chain (
               p_pay_period_id => v_period_id,
               p_payment_id    => v_dst_payment_id
           );
      -- ������� �������������� ������
      DELETE FROM PAYMENT_T P 
       WHERE P.REP_PERIOD_ID = v_period_id
         AND P.PAYMENT_ID    = v_dst_payment_id;

    END IF;    

    -- ������������� ������ ����� ������ ��������
    v_balance := PK05_ACCOUNT_BALANCE.Refresh_balance(v_account_id);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ��� ���������� ������������� ������� �� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Oper_1c (
      p_account_id     IN INTEGER, -- �/�
      p_period_id      IN INTEGER, -- ������� 
      p_doc_date       IN DATE,    -- ���� ���������
      p_amount         IN NUMBER,  -- ����� �������� (+ ��������� ������  - ��������� ������)
      p_descr          IN VARCHAR2,-- �������� ��������
      p_manager        IN VARCHAR2 -- ��������� ����������� ��������
  ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Oper_1�';
    v_oper_id        INTEGER := NULL;
    v_dst_payment_id INTEGER;
    v_dst_bill_id    INTEGER;
    v_balance        NUMBER;
BEGIN
    -- ��� ������������� ����� �������� ��������� �������������� ������
    IF p_amount > 0 THEN
        v_dst_payment_id := Create_adjust_payment_1c (  
               p_account_id   => p_account_id, -- ID �������� �����
               p_period_id    => p_period_id,  -- ID �������� ���������� ������� YYYYMM
               p_payment_date => p_doc_date,   -- ���� �������
               p_amount       => p_amount,     -- ����� �������
               p_notes        => p_descr,
               p_descr        => p_descr,
               p_manager      => p_manager
            );
    -- ��� ������������� ����� �������� ��������� �������������� ����
    ELSIF p_amount < 0 THEN
        v_dst_bill_id := Create_adjust_bill_x(
               p_account_id => p_account_id,
               p_period_id  => p_period_id,
               p_bill_date  => p_doc_date,
               p_bill_total => -p_amount,
               p_notes      => p_descr,
               p_manager    => p_manager
            );
    END IF;
    -- ��������� ���������� ��������
    IF p_amount != 0 THEN
        v_oper_id := Pk02_Poid.Next_operation_id;
        --
        INSERT INTO PAYMENT_OPERATION_T(
          OPER_ID, OPER_TYPE_ID, OPER_DATE, OPER_TOTAL, 
          SRC_PAYMENT_ID, SRC_REP_PERIOD_ID, 
          DST_PAYMENT_ID, DST_REP_PERIOD_ID, 
          CREATED_BY, NOTES, 
          SRC_ACCOUNT_ID, DST_ACCOUNT_ID, 
          SRC_BILL_ID, DST_BILL_ID
        )VALUES(
          v_oper_id, c_op_bl_adjust, SYSDATE, p_amount,
          NULL, p_period_id,
          v_dst_payment_id, p_period_id,
          p_manager, p_descr,
          p_account_id, p_account_id,
          NULL, v_dst_bill_id
        );
        
        -- �� ������ ������ ������������� ������ � �������� �������� �������������
        v_balance := PK05_ACCOUNT_BALANCE.Refresh_balance(p_account_id);
        
    END IF;
    
    RETURN v_oper_id;
EXCEPTION
    WHEN OTHERS THEN
        --RETURN -Pk01_Syslog.Fn_write_error('ERROR', c_PkgName||'.'||v_prcName );
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ����� ID �������� ����� �� ������
-- ������������ � GUI
FUNCTION get_account_id (
      p_account_no     IN VARCHAR2 -- �/�
  ) RETURN INTEGER
IS
  v_account_id INTEGER;
BEGIN
    SELECT ACCOUNT_ID
      INTO v_account_id 
      FROM ACCOUNT_T A 
     WHERE A.ACCOUNT_NO = p_account_no;
    RETURN v_account_id; 
EXCEPTION 
    WHEN NO_DATA_FOUND THEN
        RAISE_APPLICATION_ERROR(-20001,'�� ������ �/� '||p_account_no);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� �������� ��� ���������� ������������� ������� �� �����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Delete_Oper_1c (
      p_oper_id        IN INTEGER  -- id ��������� �������� 
  )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Delete_Oper_1�';
    v_oper_id        INTEGER;
    v_account_id     INTEGER;
    v_dst_payment_id INTEGER;
    v_dst_bill_id    INTEGER;
    v_period_id      INTEGER;
    v_retcode        INTEGER;
    v_balance        NUMBER;
BEGIN
    -- �������� ���������� �� ��������
    SELECT PO.DST_REP_PERIOD_ID, PO.DST_PAYMENT_ID, PO.DST_BILL_ID, PO.DST_ACCOUNT_ID
      INTO v_period_id, v_dst_payment_id, v_dst_bill_id, v_account_id
      FROM PAYMENT_OPERATION_T PO
     WHERE PO.OPER_ID = p_oper_id;

    -- ������� ������ �� ��������
    DELETE FROM PAYMENT_OPERATION_T PO
     WHERE PO.OPER_ID = p_oper_id;
    
    -- �������������� ������:
    IF v_dst_payment_id IS NOT NULL THEN
      -- ������� ��� ������� �������� �������
      PK10_PAYMENTS_TRANSFER.Delete_transfer_chain (
               p_pay_period_id => v_period_id,
               p_payment_id    => v_dst_payment_id
           );
      -- ������� �������������� ������
      DELETE FROM PAYMENT_T P 
       WHERE P.REP_PERIOD_ID = v_period_id
         AND P.PAYMENT_ID    = v_dst_payment_id;
    
    -- �������������� ����:
    ELSIF v_dst_bill_id IS NOT NULL THEN
      -- ������� �������� �������� �� �������������� ����
      PK10_PAYMENTS_TRANSFER.Delete_transfer_bill (
               p_period_id => v_period_id,
               p_bill_id   => v_dst_bill_id
           );
      -- ������� �������������� ����
      v_retcode := Pk07_Bill.Delete_bill(
              p_bill_id   => v_dst_bill_id,
              p_period_id => v_period_id,
              p_force     => 0
           );
    END IF;

    -- ������������� ������ ����� ������ ��������
    v_balance := PK05_ACCOUNT_BALANCE.Refresh_balance(v_account_id);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
  
-- ============================================================= --
--         � � � � � � � � � � � � � � �   � � � � � � �         --
-- ============================================================= --
-- ------------------------------------------------------------- --
-- ��������� ��������� �� ������ ���������� �� ���������
-- ------------------------------------------------------------- --
FUNCTION Read_ErrMsg (
      p_errmsg_id IN INTEGER
  ) RETURN VARCHAR2
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Read_ErrMsg';
    v_message VARCHAR2(2000);
BEGIN
    --
    SELECT M.MESSAGE INTO v_message
      FROM L01_MESSAGES M
     WHERE M.L01_ID = -p_errmsg_id;
    --
    RETURN v_message;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------- --
-- ������� ���� ��� ������������� �������
-- ------------------------------------------------------------- --
FUNCTION Create_adjust_bill_x (  
               p_account_id    IN INTEGER, -- ID �������� �����
               p_period_id     IN INTEGER, -- ID �������� ���������� ������� YYYYMM
               p_bill_date     IN DATE,    -- ���� �����
               p_bill_total    IN NUMBER,  -- ����� ���������� � ��������
               p_notes         IN VARCHAR2,
               p_manager       IN VARCHAR2
            ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Create_adjust_bill_x';
    v_bill_no       BILL_T.BILL_NO%TYPE;
    v_bill_id       INTEGER;
    v_item_id       INTEGER;
    v_profile_id    INTEGER;
    v_contract_id   INTEGER;
    v_contractor_id INTEGER;
    v_bank_id       INTEGER;   
    v_vat           NUMBER;
    v_currency_id   INTEGER;
BEGIN
    -- ��������� ������ ����� 
    Pk07_Bill.Check_bill_period(p_period_id, p_account_id);  

    -- �������� ID ����� 
    v_bill_id := Pk02_POID.Next_bill_id;
    
    -- �������� ����� ����� ��� ���������� �������
    v_bill_no := Pk07_Bill.Next_bill_no (
               p_account_id     => p_account_id,
               p_bill_period_id => p_period_id
           )||'X';
        
    -- ������ ��������� �/� �� �������
    Pk07_Bill.Read_account_profile (
               p_account_id    => p_account_id,
               p_bill_date     => p_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );
           
    -- �������� ������ �����
    SELECT CURRENCY_ID INTO v_currency_id
      FROM ACCOUNT_T A
     WHERE A.ACCOUNT_ID = p_account_id;
    
    -- C������ ������������� ���� ��� ������������ ����������
    INSERT INTO BILL_T (
        CONTRACT_ID,     -- ID ��������
        ACCOUNT_ID,      -- ID �������� �����
        BILL_ID,         -- ID �������� �����
        REP_PERIOD_ID,   -- ID ���������� ������� YYYYMM
        BILL_TYPE,       -- ��� �����
        BILL_NO,         -- ����� �����
        CURRENCY_ID,     -- ID ������ �����
        BILL_DATE,       -- ���� ����� (������������ �������)
        BILL_STATUS,     -- ��������� �����
        PROFILE_ID,      -- ID ������� �/�
        CONTRACTOR_ID,   -- ID ��������
        CONTRACTOR_BANK_ID, -- ID ����� ��������
        VAT,             -- ������ ���
        NOTES
    )VALUES(
        v_contract_id,
        p_account_id,
        v_bill_id,
        p_period_id,
        'X',
        v_bill_no,
        v_currency_id,
        p_bill_date,
        PK00_CONST.c_BILL_STATE_OPEN,
        v_profile_id,
        v_contractor_id,
        v_bank_id,
        v_vat,
        NVL(p_notes,'������������� �������')||'('||p_manager||')'
    );  
    -- ��������� �������� ��� �������
    INSERT INTO BILL_HISTORY_T(BILL_ID, REP_PERIOD_ID, ACTION) VALUES(v_bill_id, p_period_id, Pk00_Const.c_BILL_HISTORY_CREATE);

    -- ������� ������� ����� � item_t
    v_item_id := Pk02_Poid.Next_item_id;
    
    INSERT INTO ITEM_T(
           BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE,
           ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID,
           CHARGE_TYPE, ITEM_TOTAL, RECVD, DATE_FROM, DATE_TO,
           CREATE_DATE, LAST_MODIFIED, TAX_INCL, NOTES, ITEM_CURRENCY_ID
    )
    SELECT * 
      FROM (
        SELECT I.BILL_ID, I.REP_PERIOD_ID, v_item_id ITEM_ID, 'A' ITEM_TYPE,
               I.ORDER_ID, I.SERVICE_ID, I.ORDER_BODY_ID, I.SUBSERVICE_ID,
               'ONT' CHARGE_TYPE, p_bill_total ITEM_TOTAL, 0 RECVD, 
               TRUNC(p_bill_date, 'mm') DATE_FROM, p_bill_date DATE_TO,
               SYSDATE CREATE_DATE, SYSDATE LAST_MODIFIED, 'Y' TAX_INCL,
               '������������� �������' NOTES, v_currency_id
          FROM ITEM_T I, BILL_T B
         WHERE I.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND I.BILL_ID       = B.BILL_ID
           AND B.ACCOUNT_ID    = p_account_id
         ORDER BY I.DATE_FROM DESC, I.ITEM_TOTAL DESC 
    )
    WHERE ROWNUM = 1;
    
    -- ��������� �������������� ���� � ��������������� ������
    Pk33_Billing_Account.Make_Bill( v_bill_id, p_period_id );

    RETURN v_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------- --
-- ������� ������ ��� ������������� ������� 
-- ��� ������������� ������������ �������������
-- ------------------------------------------------------------- --
FUNCTION Create_adjust_payment_kz (  
               p_account_id    IN INTEGER, -- ID �������� �����
               p_period_id     IN INTEGER, -- ID �������� ���������� ������� YYYYMM
               p_payment_date  IN DATE,    -- ���� �������
               p_amount        IN NUMBER,  -- ����� �������
               p_manager       IN VARCHAR2,
               p_notes         IN VARCHAR2,-- ���������� (����� �����)
               p_descr         IN VARCHAR2 DEFAULT NULL -- �������� ������� (���. ��������)
            ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Create_adjust_payment_kz';
  	v_payment_id	NUMBER;
    v_doc_id      PAYMENT_T.DOC_ID%TYPE;
    v_balance     NUMBER;
BEGIN
    -- doc_id - �������� ��������� ��� ����� �����
    v_doc_id := Pk07_Bill.Next_bill_no (
               p_account_id     => p_account_id,
               p_bill_period_id => p_period_id
           )||'_KZ';

    -- ��������� ������
    v_payment_id := PK10_PAYMENT.Add_payment(
                      p_account_id, 
                      p_period_id, 
                      p_payment_date, 
                      c_kz_payment_type,
                      p_amount, 
                      c_kz_paysystem_id,
                      v_doc_id, 
                      pk00_const.c_PAY_STATE_OPEN, 
                      p_manager, 
                      p_notes,
                      p_descr
                   );
                   
	  RETURN v_payment_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------- --
-- ������� ������ ��� ���������� �������������� 
-- ------------------------------------------------------------- --
FUNCTION Create_adjust_payment_vz (  
               p_account_id    IN INTEGER, -- ID �������� �����
               p_period_id     IN INTEGER, -- ID �������� ���������� ������� YYYYMM
               p_payment_date  IN DATE,    -- ���� �������
               p_amount        IN NUMBER,  -- ����� �������
               p_manager       IN VARCHAR2,
               p_notes         IN VARCHAR2,-- ���������� (����� �����)
               p_descr         IN VARCHAR2 DEFAULT NULL -- �������� ������� (���. ��������)
            ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Create_adjust_payment_vz';
  	v_payment_id	NUMBER;
    v_doc_id      PAYMENT_T.DOC_ID%TYPE;
    v_balance     NUMBER;
BEGIN
    -- doc_id - �������� ��������� ��� ����� �����
    v_doc_id := Pk07_Bill.Next_bill_no (
               p_account_id     => p_account_id,
               p_bill_period_id => p_period_id
           )||'_VZ';

    -- ��������� ������
    v_payment_id := PK10_PAYMENT.Add_payment(
                      p_account_id, 
                      p_period_id, 
                      p_payment_date, 
                      c_vz_payment_type,
                      p_amount, 
                      c_vz_paysystem_id,
                      v_doc_id, 
                      pk00_const.c_PAY_STATE_OPEN, 
                      p_manager, 
                      p_notes,
                      p_descr
                   );
                   
	  RETURN v_payment_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------- --
-- ������� ������ ��� ���������� ������������� ������� �� ����� 
-- ------------------------------------------------------------- --
FUNCTION Create_adjust_payment_1c (  
               p_account_id    IN INTEGER, -- ID �������� �����
               p_period_id     IN INTEGER, -- ID �������� ���������� ������� YYYYMM
               p_payment_date  IN DATE,    -- ���� �������
               p_amount        IN NUMBER,  -- ����� �������
               p_manager       IN VARCHAR2,
               p_notes         IN VARCHAR2,-- ���������� (����� �����)
               p_descr         IN VARCHAR2 DEFAULT NULL -- �������� ������� (���. ��������)
            ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Create_adjust_payment_vz';
  	v_payment_id	NUMBER;
    v_doc_id      PAYMENT_T.DOC_ID%TYPE;
    v_balance     NUMBER;
BEGIN
    -- doc_id - �������� ��������� ��� ����� �����
    v_doc_id := Pk07_Bill.Next_bill_no (
               p_account_id     => p_account_id,
               p_bill_period_id => p_period_id
           )||'_1C';

    -- ��������� ������
    v_payment_id := PK10_PAYMENT.Add_payment(
                      p_account_id, 
                      p_period_id, 
                      p_payment_date, 
                      c_1c_payment_type,
                      p_amount, 
                      c_1c_paysystem_id,
                      v_doc_id, 
                      pk00_const.c_PAY_STATE_OPEN, 
                      p_manager, 
                      p_notes,
                      p_descr
                   );
                   
	  RETURN v_payment_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;            

-- ------------------------------------------------------------- --
-- ������ �������� �� ������ (������)
-- ------------------------------------------------------------- --
PROCEDURE Balance_oper_list( 
               p_recordset    OUT t_refc, 
               p_rep_period_id IN INTEGER,    -- ID ������� �����
               p_oper_type_id  in number
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Balance_oper_list';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
      SELECT 
             PT.OPER_NAME           OPER_TYPE_NAME,
             PO.OPER_ID, 
             PO.OPER_DATE,
             PO.CREATED_BY, 
             PO.OPER_TOTAL,
             PO.NOTES,
             --
             PO.SRC_REP_PERIOD_ID   SRC_REP_PERIOD_ID, 
             BS.BILL_TYPE           SRC_BILL_TYPE, 
             BS.BILL_NO             SRC_BILL_NO, 
             PS.PAYMENT_ID          SRC_PAYMENT_ID, 
             PS.PAYMENT_TYPE        SRC_PAYMENT_TYPE, 
             PS.DOC_ID              SRC_DOC_ID, 
             PS.PAYMENT_DATE        SRC_PAYMENT_DATE, 
             PSS.PAYSYSTEM_CODE     SRC_PAYSYSTEM, 
             --PS.PAY_DESCR SRC_PAY_DESCR,  
             PO.DST_REP_PERIOD_ID   DST_REP_PERIOD_ID,
             BD.BILL_TYPE           DST_BILL_TYPE, 
             BD.BILL_NO             DST_BILL_NO,
             PD.PAYMENT_ID          DST_PAYMENT_ID, 
             PD.PAYMENT_TYPE        DST_PAYMENT_TYPE, 
             PD.DOC_ID              DST_DOC_ID, 
             PD.PAYMENT_DATE        DST_PAYMENT_DATE, 
             PSS.PAYSYSTEM_CODE     DST_PAYSYSTEM,
              --PD.PAY_DESCR DST_PAY_DESCR,  
             PO.OPER_TYPE_ID
        FROM 
                PAYMENT_OPERATION_T PO, 
                PAYMENT_OPERATION_TYPE_T PT, 
                BILL_T BS, 
                BILL_T BD, 
                PAYMENT_T PS, 
                PAYMENT_T PD,
                PAYSYSTEM_T PSS, 
                PAYSYSTEM_T PSD
       WHERE PO.OPER_TYPE_ID      = PT.OPER_TYPE_ID
         AND PO.SRC_BILL_ID       = BS.BILL_ID(+)
         AND PO.SRC_REP_PERIOD_ID = BS.REP_PERIOD_ID(+)
         AND PO.DST_BILL_ID       = BD.BILL_ID(+)
         AND PO.DST_REP_PERIOD_ID = BD.REP_PERIOD_ID(+)
         AND PO.SRC_PAYMENT_ID    = PS.PAYMENT_ID(+)
         AND PO.SRC_REP_PERIOD_ID = PS.REP_PERIOD_ID(+)
         AND PO.DST_PAYMENT_ID    = PD.PAYMENT_ID(+)
         AND PO.DST_REP_PERIOD_ID = PD.REP_PERIOD_ID(+)
         AND PSS.PAYSYSTEM_ID(+)  = PS.PAYSYSTEM_ID
         AND PSD.PAYSYSTEM_ID(+)  = PD.PAYSYSTEM_ID
         AND PO.OPER_TYPE_ID      > 10
         AND PO.DST_REP_PERIOD_ID = p_rep_period_id
         and (p_oper_type_id is null or PO.OPER_TYPE_ID = p_oper_type_id )
      ORDER BY OPER_DATE DESC;
 
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK10_BALANCE_OPERATION;
/
