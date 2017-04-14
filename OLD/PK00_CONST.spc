CREATE OR REPLACE PACKAGE PK00_CONST
IS
    -- ����� ��� �������� ��������
    -- ==============================================================================
    c_PkgName   CONSTANT varchar2(30) := 'PK00_CONST';
    -- ==============================================================================
    c_RET_OK    CONSTANT integer := 0;
    c_RET_ER    CONSTANT integer :=-1;

    --=============================================================================--
    -- ������� DICTIONARY_T
    --=============================================================================--
    -- --------------------------------------------------------------------------- --
    -- ������� ����� (ACCOUNT_T)
    -- --------------------------------------------------------------------------- --
    c_ACC_TYPE_P CONSTANT char(1) := 'P';          -- ���������� ����
    c_ACC_TYPE_J CONSTANT char(1) := 'J';          -- ����������� ����

    c_ACC_STATUS_BILL CONSTANT varchar2(10):= 'B'; -- ������ - ������������ (�������)
    c_ACC_STATUS_TEST CONSTANT varchar2(10):= 'T'; -- ������ - �������� (�� ������������)

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���� ������:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_BILL_TYPE_ONT CONSTANT char(1) := 'M';       -- ������� ���� �� ������
    c_BILL_TYPE_REC CONSTANT char(1) := 'B';       -- ����������� ���� �� ������
    c_BILL_TYPE_CRD CONSTANT char(1) := 'C';       -- ������ ���� (������������ ����)
    c_BILL_TYPE_DBT CONSTANT char(1) := 'D';       -- ����� ���� (������������ ����)

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� �����:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���� ���������, ������ ������, ��������� ���������� ����������, 
    -- ������������� ������ ����� ������������ �����/������ ���
    -- ��������, ������ ����� �������� � ������ � ��������������
    c_BILL_STATE_CLOSED  CONSTANT varchar2(20) := 'CLOSED';
    c_ITEM_STATE_CLOSED  CONSTANT varchar2(20) := 'CLOSED';  
    -- ���� �����, �� �������� ������ ��� �� ������, �������� �������� ����������
    c_BILL_STATE_READY   CONSTANT varchar2(20) := 'READY';
    c_ITEM_STATE_RE�DY   CONSTANT varchar2(20) := 'READY';
    -- ���� ������, �� �������� ������ ��� �� ������, �������� �������� ����������
    c_BILL_STATE_EMPTY   CONSTANT varchar2(20) := 'EMPTY';
    c_ITEM_STATE_EMPTY   CONSTANT varchar2(20) := 'EMPTY';
    -- ���� �������� - ������ ��� ���������� � ������ �������� 
    c_BILL_STATE_OPEN    CONSTANT varchar2(20) := 'OPEN';
    c_ITEM_STATE_OPEN    CONSTANT varchar2(20) := 'OPEN';
    -- ������ ��� ������������ �����
    c_BILL_STATE_ERROR   CONSTANT varchar2(20) := 'ERROR';
    c_ITEM_STATE_ERROR   CONSTANT varchar2(20) := 'ERROR';

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- c�������� ������� STATUS �� ������� PAYMENT_T:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_PAY_STATE_OPEN   CONSTANT varchar2(20) := 'OPEN';  -- �� ��������� �����������
    c_PAY_STATE_CLOSE  CONSTANT varchar2(20) := 'CLOSE'; -- ������ �����������
    c_PAY_STATE_ERROR  CONSTANT varchar2(20) := 'ERROR'; -- ������

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���� ������� ����� ITEM_TYPE �� ������� ITEM_T
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������, ������� �� ������ � ������������ ����, �� ����� ��� ������ � ���
    c_ITEM_TYPE_PAYMENT  CONSTANT char(1) := 'P';  -- ������ (PAYMENT)
    --c_ITEM_TYPE_TRANSFER CONSTANT char(1) := 'T';  -- ������� �������� �������
    --c_ITEM_TYPE_ADVANCE  CONSTANT char(1) := 'A';  -- ����� �� ������� ����������� �������
    -- �������, ������� ������ � ������������ ����
    c_ITEM_TYPE_BILL     CONSTANT char(1) := 'B';  -- ������� ���������� �� ������
    c_ITEM_TYPE_ADJUST   CONSTANT char(1) := 'C';  -- ������� ������������

    c_INV_ITEM_BALANCE   CONSTANT INTEGER := 0;    -- ����� ������ ������� � INV_ITEM_NO
    c_VAT                CONSTANT NUMBER  := 18;   -- ������ ��� 

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���� ��������� CHARGE_TYPE �� ������ ORDER_BODY_T, ITEM_T 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_CHARGE_TYPE_REC CONSTANT char(3) := 'REC';   -- �������������� ������ �� ���������
    c_CHARGE_TYPE_ONT CONSTANT char(3) := 'ONT';   -- ������� ��������
    c_CHARGE_TYPE_USG CONSTANT char(3) := 'USG';   -- ������ �������
    c_CHARGE_TYPE_MIN CONSTANT char(3) := 'MIN';   -- ������� �� ����������� �����
    c_CHARGE_TYPE_DIS CONSTANT char(3) := 'DIS';   -- ������
    c_CHARGE_TYPE_IDL CONSTANT char(3) := 'IDL';   -- �������

    c_CHARGE_TYPE_IDL CONSTANT char(3) := 'PAY';   -- ����������� ������
    c_CHARGE_TYPE_IDL CONSTANT char(3) := 'PAY';   -- ����������� ������

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���� ������� ADDRESS_TYPE �� ������ ACCOUNT_NAME_INFO_T, CUSTOMER_T, CONTRACTOR_T 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_ADDR_TYPE_JUR CONSTANT char(3) := 'JUR';     -- ����������� ����� ��� ��. ���
    c_ADDR_TYPE_REG CONSTANT char(3) := 'REG';     -- ����� ����������� ��� ���. ���
    c_ADDR_TYPE_DLV CONSTANT char(3) := 'DLV';     -- ����� ��������
    c_ADDR_TYPE_GRP CONSTANT char(3) := 'GRP';     -- ����� ���������������
    c_ADDR_TYPE_SET CONSTANT char(3) := 'SET';     -- ����� ��������� ������������

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_RATESYS_MMTS_ID CONSTANT integer := 1201;    -- ����������� ��������� ���� ����
    c_RATESYS_TOPS_ID CONSTANT integer := 1202;    -- ����������� ������� ������������ ��������� ���� 
    
    --=============================================================================--
    -- ���� ������������ CONTRACTOR_T
    --=============================================================================--
    c_CTR_TYPE_KTTK  CONSTANT varchar(20) := '����';
    c_CTR_TYPE_XTTK  CONSTANT varchar(20) := 'X���';
    c_CTR_TYPE_ZTTK  CONSTANT varchar(20) := 'ZTTK';
    c_CTR_TYPE_BRAND CONSTANT varchar(20) := 'BRAND';
    c_CTR_TYPE_AGENT CONSTANT varchar(20) := 'AGENT';
    
    --=============================================================================--
    -- ID ������� ����������� ���� CLIENT_T (������ ���� �� ����)
    --=============================================================================--
    c_CLIENT_PERSON_ID   CONSTANT integer := 1;
    
    --=============================================================================--
    -- ID ���������� ��� ����������� ���� CUSTOMER_T (������ ���� �� ����)
    --=============================================================================--
    c_CUSTOMER_PERSON_ID CONSTANT integer := 1;    

    --=============================================================================--
    -- ���������� ����� CURRENCY_T
    --=============================================================================--
    c_CURRENCY_RUB    CONSTANT integer := 810;  -- ���������� �����
    c_CURRENCY_USD    CONSTANT integer := 840;  -- ������ ���
    c_CURRENCY_EUR    CONSTANT integer := 978;  -- ����
    c_CURRENCY_YE     CONSTANT integer := 36;   -- �������� ������� ������
    c_CURRENCY_YEE    CONSTANT integer := 250;  -- �������� ������� ����
    c_CURRENCY_YE_FIX CONSTANT integer := 286;  -- �������� ������� ������ �� ����� 28,6

    --=============================================================================--
    -- ���� ������ �� ������� SERVICE_T
    --=============================================================================--
    -- ��������: ID ����� ��� ������ (�������� ������� ����� ���������� ������� ��������)
    c_SERVICE_CALL_MGMN   CONSTANT integer := 1;   -- ������ �������������/������������� ���������� �����
    c_SERVICE_CALL_FREE   CONSTANT integer := 2;   -- ������ ����� �� 8-800
    c_SERVICE_CALL_ZONE   CONSTANT integer := 3;   -- ������ �������� ������
    c_SERVICE_CALL_LOCAL  CONSTANT integer := 4;   -- ������ �������� ������
    c_SERVICE_CALL_LZ     CONSTANT integer := 6;   -- ������ ������� � ������� �����
    c_SERVICE_OP_LOCAL    CONSTANT integer := 7;   -- ������ ������������� �� ������� ������
    
    --=============================================================================--
    -- ���� ����������� ������ �� ������� SUBSERVICE_T
    --=============================================================================--
    c_SUBSRV_MG  CONSTANT integer := 1;   -- �������������� ������������� ���������� ����������    
    c_SUBSRV_MN  CONSTANT integer := 2;   -- �������������� ������������� ���������� ����������
    c_SUBSRV_MIN CONSTANT integer := 3;   -- ������� �� ���. ����������� ���������
    c_SUBSRV_DET CONSTANT integer := 4;   -- '����������� �����������'

    --=============================================================================--
    -- ���� ������� �� ������� EVENT_TYPE_T
    --=============================================================================--
    c_EVENT_TYPE_CALL_LOCAL  CONSTANT integer := 1;  -- ������� ������
    c_EVENT_TYPE_CALL_ZONE   CONSTANT integer := 2;  -- ������� ������
    c_EVENT_TYPE_CALL_MG     CONSTANT integer := 3;  -- ������������� ������
    c_EVENT_TYPE_CALL_MN     CONSTANT integer := 4;  -- ������������� ������
    c_EVENT_TYPE_FREE_CALL   CONSTANT integer := 5;  -- ����� �� 8-800

    --=============================================================================--
    -- ������ �������� ������� ������
    --=============================================================================--
    c_ORDER_NOT_FOUND        CONSTANT integer := -1; -- �� ������ �����
    c_ERR_SRV_NOT_FOUND      CONSTANT integer := -2; -- �� ������� ������ 
    c_NOT_ZONE_JOINT         CONSTANT integer := -3; -- ���� �� �������

    --=============================================================================--
    -- ���� BDR-��
    --=============================================================================--
    c_BDR_Ph_A               CONSTANT integer := 1;
    
    --=============================================================================--
    -- ������ ����������
    --=============================================================================--
    c_ACC_NOT_FOUND          CONSTANT integer := -4; -- �� �/���� �� ������
    c_NF_TAR_PLAN            CONSTANT integer := -5; -- �� ������ �������� ���� 
    c_TARIFF_NOT_FOUND       CONSTANT integer := -6; -- �� ������ �������� ���� � �� 
    c_PRICE_NOT_FOUND        CONSTANT integer := -7; -- �� ������� �������� ��� ������ ������� � � �
    

END PK00_CONST;
/
