CREATE OR REPLACE PACKAGE PK00_CONST
IS
    -- ����� ��� �������� ��������
    -- ==============================================================================
    c_PkgName   CONSTANT varchar2(30) := 'PK00_CONST';
    -- ==============================================================================
    c_RET_OK    CONSTANT integer := 0;
    c_RET_ER    CONSTANT integer :=-1;

    -- ������� � �������������� �������� ������� (order_no)
    c_Order_Test_Pref CONSTANT varchar2(3) := 'KH_';
    
    -- ����������, ��������������� ��� ������� �������/������
    c_Bill_Lock CONSTANT varchar2(16) := 'BILL_LOCK';    

    c_Mask_Length     CONSTANT number := 6; -- ���-�� ������ ��� ������������� ����������

    -- ����, ������� ����������� � DATE_TO ������ NULL
    c_DATE_MAX CONSTANT date := TO_DATE('01.01.2050','dd.mm.yyyy');

    --:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::--
    -- � � � � � � � � � � �
    --:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::--
    --=============================================================================--
    -- CURRENCY_T  -  ���������� ����� 
    --=============================================================================--
    c_CURRENCY_RUB    CONSTANT integer := 810;  -- ���������� �����
    c_CURRENCY_USD    CONSTANT integer := 840;  -- ������ ���
    c_CURRENCY_EUR    CONSTANT integer := 978;  -- ����
    c_CURRENCY_YE     CONSTANT integer := 36;   -- �������� ������� ������
    c_CURRENCY_YEE    CONSTANT integer := 250;  -- �������� ������� ����
    c_CURRENCY_YE_FIX CONSTANT integer := 286;  -- �������� ������� ������ �� ����� 28,6

    --=============================================================================--
    -- SERVICE_T  -  ���� ����� 
    --=============================================================================--
    -- ��������: ID ����� ��� ������ (�������� ������� ����� ���������� ������� ��������)
    c_SERVICE_CALL_MGMN   CONSTANT integer := 1;   -- ������ �������������/������������� ���������� �����
    c_SERVICE_CALL_FREE   CONSTANT integer := 2;   -- ������ ����� �� 8-800
    c_SERVICE_CALL_ZONE   CONSTANT integer := 140; -- ������ �������� ������
    c_SERVICE_CALL_LOCAL  CONSTANT integer := 125; -- ������ �������� ������
    c_SERVICE_OP_LOCAL    CONSTANT integer := 7;   -- ������ ������������� �� ������� ������
    c_SERVICE_NPL         CONSTANT integer := 101; -- �������������� ������������� �������� ������� ����� (����/NPL)
    c_SERVICE_IP_ACCESS   CONSTANT integer := 104; -- ������ � ��������
    c_SERVICE_VPN         CONSTANT integer := 106; -- ����������� ������� ���� (���/IP VPN)
    c_SERVICE_LM          CONSTANT integer := 108; -- �������������� ������ ������� � ������ ������������� �������� ���� ����� (�����)
    c_SERVICE_SYNC        CONSTANT integer := 113; -- ������������� ����� �����
    c_SERVICE_DPL         CONSTANT integer := 126; -- �������������� �������� ������� ����� (���)
    c_SERVICE_EPL         CONSTANT integer := 133; -- ����������� ����� Ethernet (EPL)

    --=============================================================================--
    -- SUBSERVICE_T - ���� ����������� ����� 
    --=============================================================================--
    c_SUBSRV_MG     CONSTANT integer := 1;   -- �������������� ������������� ���������� ����������    
    c_SUBSRV_MN     CONSTANT integer := 2;   -- �������������� ������������� ���������� ����������
    c_SUBSRV_MIN    CONSTANT integer := 3;   -- ������� �� ���. ����������� ���������
    c_SUBSRV_DET    CONSTANT integer := 4;   -- '����������� �����������'
    c_SUBSRV_LOCAL  CONSTANT integer := 5;   -- �������������� ������� ���������� ����������
    c_SUBSRV_ZONE   CONSTANT integer := 6;   -- �������������� ������� ���������� ����������
    c_SUBSRV_INST   CONSTANT integer := 7;   -- �����������
    c_SUBSRV_FREE   CONSTANT integer := 8;   -- ������ "���������� �����"
    c_SUBSRV_ABP    CONSTANT integer := 9;   -- ��������� �� ���������� �����
    c_SUBSRV_IRT    CONSTANT integer := 31;  -- ���������� �����������
    c_SUBSRV_DISC   CONSTANT integer := 32;  -- ������
    c_SUBSRV_IDL    CONSTANT integer := 36;  -- ����������� ��������
    c_SUBSRV_BACK   CONSTANT integer := 37;  -- ������� ������� ����������� � ���������
    c_SUBSRV_RT8    CONSTANT integer := 38;  -- ������������� ���� �� 8 IP �������
    c_SUBSRV_VOLUME CONSTANT integer := 39;  -- ����������� �� ������
    c_SUBSRV_BURST  CONSTANT integer := 40;  -- ����������� �� ������
    c_SUBSRV_REC    CONSTANT integer := 41;  -- ����������� ������
    
    --=============================================================================--
    -- ������� D I C T I O N A R Y _ T
    --:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::--
    -- ID ������ (��������) ������� ������
    --=============================================================================--
    c_DICT_KEY_ACCOUNT_TYPE    CONSTANT integer := 1;  -- ��� �������� �����
    c_DICT_KEY_ACCOUNT_STATUS  CONSTANT integer := 2;  -- ������ �������� �����
    c_DICT_KEY_BILL_TYPE       CONSTANT integer := 3;  -- ��� �����
    c_DICT_KEY_BILL_STATUS     CONSTANT integer := 4;  -- ������ �����
    c_DICT_KEY_ITEM_TYPE       CONSTANT integer := 5;  -- ��� ������� �����
    c_DICT_KEY_ITEM_STATUS     CONSTANT integer := 6;  -- ������ ������� �����
    c_DICT_KEY_CHARGE_TYPE     CONSTANT integer := 7;  -- ���� ����������
    c_DICT_KEY_ADDRESS_TYPE    CONSTANT integer := 8;  -- ��� ������
    c_DICT_KEY_ORDER_LOCK_TYPE CONSTANT integer := 9;  -- ��� ���������� ������
    c_DICT_KEY_DELIVERY_TYPE   CONSTANT integer := 10; -- ������ �������� �����
    c_DICT_KEY_CONTRACTOR_TYPE CONSTANT integer := 11; -- ��� �����������
    c_DICT_KEY_RATESYSTEM      CONSTANT integer := 12; -- ������������
    c_DICT_KEY_CDR_SERVICE_ID  CONSTANT integer := 13; -- �����. �������������� ������ ����� � CDR ������ ��������
    c_DICT_KEY_CDR_BIND_ERROR  CONSTANT integer := 14; -- ������ �������� CDR � �/�
    c_DICT_KEY_BDR_RATE_ERROR  CONSTANT integer := 15; -- ������ ����������� BDR
    c_DICT_KEY_CB_SRV_TYPE     CONSTANT integer := 50; -- ������������ ����� �������� �������� service_id CDR-�
    c_DICT_KEY_CB_SUBSRV_TYPE  CONSTANT integer := 60; -- Subservices, ������������ ��� ���������� BDR-��
    c_DICT_KEY_SIGNER_ROLE     CONSTANT integer := 61; -- ���� ����������
    c_DICT_PAYMENT_TYPE        CONSTANT integer := 62; -- ��� �������
    c_DICT_MARKET_SEGMENT      CONSTANT integer := 63; -- ������� �����
    c_DICT_CLIENT_TYPE         CONSTANT integer := 64; -- ��� �������
    c_DICT_DELIVERY_METHOD     CONSTANT integer := 65; -- ������ ��������
    
    FUNCTION k_DICT_KEY_ACCOUNT_TYPE    RETURN INTEGER DETERMINISTIC; -- 1;  -- ��� �������� �����
    FUNCTION k_DICT_KEY_ACCOUNT_STATUS  RETURN INTEGER DETERMINISTIC; -- 2;  -- ������ �������� �����
    FUNCTION k_DICT_KEY_BILL_TYPE       RETURN INTEGER DETERMINISTIC; -- 3;  -- ��� �����
    FUNCTION k_DICT_KEY_BILL_STATUS     RETURN INTEGER DETERMINISTIC; -- 4;  -- ������ �����
    FUNCTION k_DICT_KEY_ITEM_TYPE       RETURN INTEGER DETERMINISTIC; -- 5;  -- ��� ������� �����
    FUNCTION k_DICT_KEY_ITEM_STATUS     RETURN INTEGER DETERMINISTIC; -- 6;  -- ������ ������� �����
    FUNCTION k_DICT_KEY_CHARGE_TYPE     RETURN INTEGER DETERMINISTIC; -- 7;  -- ���� ����������
    FUNCTION k_DICT_KEY_ADDRESS_TYPE    RETURN INTEGER DETERMINISTIC; -- 8;  -- ��� ������
    FUNCTION k_DICT_KEY_ORDER_LOCK_TYPE RETURN INTEGER DETERMINISTIC; -- 9;  -- ��� ���������� ������
    FUNCTION k_DICT_KEY_DELIVERY_TYPE   RETURN INTEGER DETERMINISTIC; -- 10; -- ������ �������� �����
    FUNCTION k_DICT_KEY_CONTRACTOR_TYPE RETURN INTEGER DETERMINISTIC; -- 11; -- ��� �����������
    FUNCTION k_DICT_KEY_RATESYSTEM      RETURN INTEGER DETERMINISTIC; -- 12; -- ������������
    FUNCTION k_DICT_KEY_CDR_SERVICE_ID  RETURN INTEGER DETERMINISTIC; -- 13; -- �����. �������������� ������ ����� � CDR ������ ��������
    FUNCTION k_DICT_KEY_CDR_BIND_ERROR  RETURN INTEGER DETERMINISTIC; -- 14; -- ������ �������� CDR � �/�
    FUNCTION k_DICT_KEY_BDR_RATE_ERROR  RETURN INTEGER DETERMINISTIC; -- 15; -- ������ ����������� BDR
    FUNCTION k_DICT_KEY_CALENDAR        RETURN INTEGER DETERMINISTIC; -- 19; -- ���� ���� ���������
    FUNCTION k_DICT_KEY_BILLING         RETURN INTEGER DETERMINISTIC; -- 20; -- �������, ������� ����������� ������ ��������
    FUNCTION k_DICT_KEY_TRF_STAT        RETURN INTEGER DETERMINISTIC; -- 21; -- ���� ������ BDR
    FUNCTION k_DICT_KEY_BIND_ERR        RETURN INTEGER DETERMINISTIC; -- 22; -- ���� ������ �������� CDR
    FUNCTION k_DICT_KEY_RATE_LEVEL      RETURN INTEGER DETERMINISTIC; -- 23; -- ������� ����������� � ORDER_BODY_T.RATE_LAVEL_ID
    FUNCTION k_DICT_KEY_RATE_RULE       RETURN INTEGER DETERMINISTIC; -- 24; -- ������� ����������� � ORDER_BODY_T.RATE_RUEL_ID
    FUNCTION k_DICT_KEY_DISCOUNT_RULE   RETURN INTEGER DETERMINISTIC; -- 25; -- ������� �������������� ��������� ������
    FUNCTION k_DICT_KEY_CUR_CONV        RETURN INTEGER DETERMINISTIC; -- 26; -- ������� ����������� ������
    FUNCTION k_DICT_KEY_DETAIL_TYPE     RETURN INTEGER DETERMINISTIC; -- 27; -- ���� ����������� ��� ������������ ������
    FUNCTION k_DICT_KEY_CB_SRV_TYPE     RETURN INTEGER DETERMINISTIC; -- 50; -- ������������ ����� �������� �������� service_id CDR-�
    FUNCTION k_DICT_KEY_CB_SUBSRV_TYPE  RETURN INTEGER DETERMINISTIC; -- 60; -- Subservices, ������������ ��� ���������� BDR-��
    FUNCTION k_DICT_KEY_SIGNER_ROLE     RETURN INTEGER DETERMINISTIC; -- 61; -- ���� ����������
    FUNCTION k_DICT_PAYMENT_TYPE        RETURN INTEGER DETERMINISTIC; -- 62; -- ��� �������
    FUNCTION k_DICT_CLIENT_TYPE         RETURN INTEGER DETERMINISTIC; -- 63; -- ��� �������
    FUNCTION k_DICT_MARKET_SEGMENT      RETURN INTEGER DETERMINISTIC; -- 64; -- ������� �����
    FUNCTION k_DICT_DELIVERY_METHOD     RETURN INTEGER DETERMINISTIC; -- 65; -- ������ ��������
    FUNCTION k_DICT_PAYMENT_OPERATIONS  RETURN INTEGER DETERMINISTIC; -- 66  -- �������� � ���������
    FUNCTION k_DICT_SPEED_UNIT          RETURN INTEGER DETERMINISTIC; -- 67  -- ������� ��������� �������� ������
    FUNCTION k_DICT_IP_SERVICE          RETURN INTEGER DETERMINISTIC; -- 68  -- IP - ������
    FUNCTION k_DICT_VOICE_UNIT          RETURN INTEGER DETERMINISTIC; -- 69; -- ������ ��������    
    FUNCTION k_DICT_IP_VOLUME_UNIT      RETURN INTEGER DETERMINISTIC; -- 70; --����� ������� (�����������)
    FUNCTION k_DICT_CONTRACT_TYPE       RETURN INTEGER DETERMINISTIC; -- 71; --��� ��������
    FUNCTION k_DICT_TERMINATING_REASON  RETURN INTEGER DETERMINISTIC; -- 72; --'������� ���������� ������ ����������� Huawei SoftX3000'
    FUNCTION k_DICT_Q850                RETURN INTEGER DETERMINISTIC; -- 73; -- Q.850 - ����������� ��� ���������� ������
    FUNCTION k_DICT_TERMINATION_CODE    RETURN INTEGER DETERMINISTIC; -- 74; -- SoftX3000 - ��� ���������� ������
    FUNCTION k_DICT_QoS                 RETURN INTEGER DETERMINISTIC; -- 75; -- �������� �������������� ������ �� IP - ������
    FUNCTION k_DICT_BILL_HISTORY_TYPE   RETURN INTEGER DETERMINISTIC; -- 76	 --	��� �������� ������� �����
    FUNCTION k_DICT_INV_RULE            RETURN INTEGER DETERMINISTIC; -- 77	 -- ������� ������������ ����� �����-�������

    -- --------------------------------------------------------------------------- --
    -- ������ (PERIOD_T)
    -- --------------------------------------------------------------------------- --
    c_PERIOD_LAST CONSTANT varchar2(4) := 'LAST';  -- ��������� �������� ������
    c_PERIOD_OPEN CONSTANT varchar2(4) := 'OPEN';  -- ������� ������
    c_PERIOD_NEXT CONSTANT varchar2(4) := 'NEXT';  -- ��������� �� ������� ������
    c_PERIOD_BILL CONSTANT varchar2(4) := 'BILL';  -- ������������, ���������� ������ 
                                                   -- �� ������� �������� ��� �������
    -- --------------------------------------------------------------------------- --
    -- �������� (ACCOUNT_T.BILLING_ID)
    -- --------------------------------------------------------------------------- --
    c_BILLING_OLD_NO_1C  CONSTANT INTEGER := 2000;     -- ������ ������� - ��� �������� � 1�
    c_BILLING_KTTK       CONSTANT INTEGER := 2001;     -- ������������ ������� BRM ���� (��������)
    c_BILLING_OLD        CONSTANT INTEGER := 2002;     -- PORTAL 6.5 - "������ �������"
    c_BILLING_MMTS       CONSTANT INTEGER := 2003;     -- CBRM 7.5 - "����� �������"
    c_BILLING_ACCESS     CONSTANT INTEGER := 2004;     -- ������� ����� ������
    c_BILLING_RP_VOICE   CONSTANT INTEGER := 2005;     -- ������� (��) - �����
    c_BILLING_SPB        CONSTANT INTEGER := 2006;     -- ������� ��� ��� ��������
    c_BILLING_RP_BALANCE CONSTANT INTEGER := 2007;     -- ������� (��) � ��������
    c_BILLING_RP         CONSTANT INTEGER := 2008;     -- ������� (��) - �� �����
    -- --------------------------------------------------------------------------- --
    -- ������� ����� (ACCOUNT_T)
    -- --------------------------------------------------------------------------- --
    c_ACC_TYPE_P CONSTANT char(1) := 'P';          -- ���������� ����
    c_ACC_TYPE_J CONSTANT char(1) := 'J';          -- ����������� ����
    c_ACC_TYPE_T CONSTANT char(1) := 'T';          -- ��������
    c_ACC_TYPE_I CONSTANT char(1) := 'I';          -- ���������� �����������

    c_ACC_STATUS_BILL   CONSTANT varchar2(10):= 'B'; -- ������ - ������������ (�������)
    c_ACC_STATUS_TEST   CONSTANT varchar2(10):= 'T'; -- ������ - �������� (�� ������������)
    c_ACC_STATUS_CLOSED CONSTANT varchar2(10):= 'C'; -- ������ - ������
    
    -- --------------------------------------------------------------------------- --
    -- ������ (ORDER_T)
    -- --------------------------------------------------------------------------- --
    c_ORDER_STATE_OPEN    CONSTANT varchar2(20) := 'OPEN';   -- ������
    c_ORDER_STATE_LOCK    CONSTANT varchar2(20) := 'LOCK';   -- ������������
    c_ORDER_STATE_CLOSED  CONSTANT varchar2(20) := 'CLOSED'; -- ������
    c_ORDER_STATE_MOVED   CONSTANT varchar2(20) := 'MOVED';  -- ���������
    
    -- --------------------------------------------------------------------------- --
    -- ������� ����������� �������������� ������ � ORDER_BODY_T.RATE_LAVEL_ID (���������/���/...)
    -- --------------------------------------------------------------------------- --
    c_RATE_LEVEL_SUBSRV    CONSTANT INTEGER := 2301; -- ����� ������ �� ��������� ������
    c_RATE_LEVEL_ORDER     CONSTANT INTEGER := 2302; -- ����� ������ �� �����
    c_RATE_LEVEL_ACCOUNT   CONSTANT INTEGER := 2303; -- ����� ������ �� ������� ����
    c_RATE_LEVEL_CONTRACT  CONSTANT INTEGER := 2304; -- ����� ������ �� �������
      
    -- --------------------------------------------------------------------------- --
    -- ������� ����������� � ORDER_BODY_T.RATE_PLAN_ID 
    -- --------------------------------------------------------------------------- --
    c_RATE_RULE_MIN_STD      CONSTANT INTEGER := 2401; -- ����������� ���������� �� ����������� �����
    c_RATE_RULE_ABP_STD      CONSTANT INTEGER := 2402; -- ����������� ���������� ���������
    c_RATE_RULE_ABP_FREE_MIN CONSTANT INTEGER := 2403; -- ���������� ���������, ���������� ������ � �������
    c_RATE_RULE_IDL_STD      CONSTANT INTEGER := 2404; -- ����������� ��������, ����������� �����
    c_RATE_RULE_ABP_MON      CONSTANT INTEGER := 2405; -- ���������� ���������, �� ����������� ������ (��� �����������)
    c_RATE_RULE_IP_FIX_VOLIN CONSTANT INTEGER := 2406; -- ������������� � ������ ������ ���������� �������'    
    c_RATE_RULE_IP_VOLUME    CONSTANT INTEGER := 2407; -- �� ������ �������'
    c_RATE_RULE_IP_VPN       CONSTANT INTEGER := 2408; -- VPN'
    c_RATE_RULE_IP_BURST     CONSTANT INTEGER := 2409; -- BURST'
    c_RATE_RULE_IP_RT        CONSTANT INTEGER := 2410; -- ��� ���������� �����������'
    c_RATE_RULE_EPL_BURST    CONSTANT INTEGER := 2411; -- EPL BURST'
    c_RATE_RULE_DIS_STD      CONSTANT INTEGER := 2412; -- ���������� ��������� ������
    c_RATE_RULE_SLA_H        CONSTANT INTEGER := 2413; -- ������� ����� � �����
    c_RATE_RULE_SLA_K        CONSTANT INTEGER := 2414; -- ����� ����������� �����������
    c_RATE_RULE_MIN_SUBS     CONSTANT INTEGER := 2415; -- ���������� �� ����������� ����� �� ���������� ������
    c_RATE_RULE_MIN_ACC      CONSTANT INTEGER := 2416; -- ���������� �� ����������� ����� �� ����� �/�
    c_RATE_RULE_ABP_30DAYS   CONSTANT INTEGER := 2417; -- ���������� ��������� �� ������� 1 ���� = 1/30 ������
    c_RR_IP_BURST_VOLIN_USG  CONSTANT INTEGER := 2418; -- BURST � ������ ������ ���������� ������� 
    c_RR_IP_BURST_VOLIN_REC  CONSTANT INTEGER := 2419; -- ��������� ��� BURST � ������ ������ ���������� �������
    c_RR_BILSRV              CONSTANT INTEGER := 2420; -- ����������� � MS SQL BillingServer �.������������

    -- ������� ��� ������������� �������:
    c_RATE_RULE_RP001190     CONSTANT INTEGER := 2430; -- ���: �/� RP001190 ��� "����� �������" 
    c_RATE_RULE_RP001186	   CONSTANT INTEGER := 2431; --	���: �/� RP001186 ��� "VMB-������"
    
    -- --------------------------------------------------------------------------- --
    -- ������� ������������ ����� �����-������� BILLINFO_T.INVOICE_RULE_ID 
    -- --------------------------------------------------------------------------- --
    c_INVOICE_RULE_STD       CONSTANT INTEGER := 7701; -- ������ ������ ������������� ���/���� ��������� ����� �����
    c_INVOICE_RULE_BIL       CONSTANT INTEGER := 7702; -- ������ ������ ������������� ������� �����
    c_INVOICE_RULE_EXT       CONSTANT INTEGER := 7703; -- ����������� ����� �������� �� ����������� ��������
    c_INVOICE_RULE_SUB_STD   CONSTANT INTEGER := 7704; -- ����������� ����� �� ����������� ������, ������� ����������� �� 7701
    c_INVOICE_RULE_SUB_BIL   CONSTANT INTEGER := 7705; -- ����������� ����� �� ����������� ������, ������� �� 7702
    c_INVOICE_RULE_SUB_EXT   CONSTANT INTEGER := 7706; -- ����������� ����� �� ����������� ������, ������� �� 7703

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���� ������:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_BILL_TYPE_ONT CONSTANT char(1) := 'M';  -- ������� ���� �� ������
    c_BILL_TYPE_REC CONSTANT char(1) := 'B';  -- ����������� ���� �� ������
    c_BILL_TYPE_CRD CONSTANT char(1) := 'C';  -- ������ ���� (������������ ����)
    c_BILL_TYPE_DBT CONSTANT char(1) := 'D';  -- ����� ���� (������������ ����)
    c_BILL_TYPE_ADS CONSTANT char(1) := 'A';  -- ���������������� ���� (������ ��� ������������ ��������, ��� ������� ������)
    c_BILL_TYPE_OLD CONSTANT char(1) := 'O';  -- ����������� ���� �� ������ ��������� ��������    
    c_BILL_TYPE_PRE CONSTANT char(1) := 'P';  -- ��������� ���� �� ������ ������� ��������

    -- ������ ����� ������ (bill_type) �� ������� ����� ���� ������
    c_BTYPE_FOR_BDR CONSTANT varchar(16) := '''B'',''D'',''O'',''A''';  

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���� �������� ������� ��������� ������:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_BILL_HISTORY_CREATE          CONSTANT VARCHAR2(20) := 'CREATE';          -- �������� �����
    c_BILL_HISTORY_CHANGE_STATUS   CONSTANT VARCHAR2(20) := 'CHANGE_STATUS';   -- ����� �������
    c_BILL_HISTORY_MAKE            CONSTANT VARCHAR2(20) := 'MAKE';            -- ������������ ����
    c_BILL_HISTORY_ROLLBACK        CONSTANT VARCHAR2(20) := 'ROLLBACK';        -- �������������� ����
    c_BILL_HISTORY_CHARGE_FIX      CONSTANT VARCHAR2(20) := 'CHARGE_FIXRATE';  -- ������������ ����.����� � ����������� �������
    c_BILL_HISTORY_ROLLBACK_FIX    CONSTANT VARCHAR2(20) := 'ROLLBACK_FIXRATE';-- �������������� ����. ����� � ����������� �������
    c_BILL_HISTORY_RECALC          CONSTANT VARCHAR2(20) := 'RECALC';          -- �������� �����

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
    -- ���� �����, �� �������� ������ ���������, �� ��������� �������� � �������� � READY 
    c_BILL_STATE_CHECK   CONSTANT varchar2(20) := 'CHECK';
    c_ITEM_STATE_CHECK   CONSTANT varchar2(20) := 'CHECK';
    -- ���� ������, �� �������� ������ ��� �� ������, �������� �������� ����������
    --c_BILL_STATE_EMPTY   CONSTANT varchar2(20) := 'EMPTY';
    --c_ITEM_STATE_EMPTY   CONSTANT varchar2(20) := 'EMPTY';
    -- ���� �������� - ������ ��� ���������� � ������ �������� 
    c_BILL_STATE_OPEN    CONSTANT varchar2(20) := 'OPEN';
    c_ITEM_STATE_OPEN    CONSTANT varchar2(20) := 'OPEN';
    -- ������ ��� ������������ �����
    c_BILL_STATE_ERROR   CONSTANT varchar2(20) := 'ERROR';
    c_ITEM_STATE_ERROR   CONSTANT varchar2(20) := 'ERROR';
    -- ���� ������� ��� ���������, ������� ����������� ��������
    c_BILL_STATE_DELETED CONSTANT varchar2(20) := 'DELETED';
    -- ����, �� ����� �� �������� �� ��������� � ����������� ���������
    c_BILL_STATE_REJECT  CONSTANT varchar2(20) := 'REJECT';
    -- ����, ���������, �� ������ ����������� � ���������� � �������������
    c_BILL_STATE_PREPAID CONSTANT varchar2(20) := 'PREPAID';

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ��������� �� �/� �������������� ��������    
    c_UNKNOWN_PAY_ACCOUNT_ID CONSTANT INTEGER      := 2; 
    c_UNKNOWN_PAY_ACCOUNT_NO CONSTANT VARCHAR2(20) := 'ACC000000002';
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- id �� (��������� �������) ��� ���������� �������������
    c_PAYSYSTEM_CORRECT_ID INTEGER := 12;
    c_PAYSYSTEM_CORRECT_CODE VARCHAR2(40) := '�������������';
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- c�������� ������� STATUS �� ������� PAYMENT_T:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_PAY_STATE_OPEN   CONSTANT varchar2(20) := 'OPEN';     -- ������ ����������� ��������� �������
    c_PAY_STATE_CLOSE  CONSTANT varchar2(20) := 'CLOSE';    -- ����� ������������, ���. ������ ������
    c_PAY_STATE_ERROR  CONSTANT varchar2(20) := 'ERROR';    -- ������
    c_PAY_STATE_REVERS CONSTANT varchar2(20) := 'REVERS';   -- ������ �����������

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���� �������� PAY_TYPE �� ������� PAYMENT_T:
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_PAY_TYPE_OPEN    CONSTANT varchar2(20) := 'OPEN';    -- �������� ������ �� "������ ��������"
    c_PAY_TYPE_PAYMENT CONSTANT varchar2(20) := 'PAYMENT'; -- ������ 
    c_PAY_TYPE_ADJUST  CONSTANT varchar2(20) := 'ADJUST';  -- �������������
    c_PAY_TYPE_REVERS  CONSTANT varchar2(20) := 'REVERS';  -- ������������ ������
    c_PAY_TYPE_REFUND  CONSTANT varchar2(20) := 'REFUND';  -- �������
    c_PAY_TYPE_MOVE    CONSTANT varchar2(20) := 'MOVE';    -- �������
	c_PAY_TYPE_ADJUST_BALANCE  CONSTANT varchar2(20) := 'ADJUST_BALANCE';  -- ������������� �������

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������� � ���������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_PAY_OP_REFUND   			CONSTANT INTEGER := 1;  -- ������� ����� � �������
    c_PAY_OP_REVERS   			CONSTANT INTEGER := 2;  -- ������������� (�������������)
    c_PAY_OP_MOVE     			CONSTANT INTEGER := 3;  -- ������� ������� � ������ �/� �� ������
    c_PAY_OP_ADJUST   			CONSTANT INTEGER := 4;  -- �������������
    c_PAY_OP_INPUT    			CONSTANT INTEGER := 5;  -- ������ ���� �������
    c_PAY_OP_TRANSFER 			CONSTANT INTEGER := 6;  -- ������������� ������� �� ������ �������
	c_PAY_OP_ADJUST_BALANCE		CONSTANT INTEGER := 7;  -- ������������� �������

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���� ������� ����� ITEM_TYPE �� ������� ITEM_T
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- �������, ������� �� ������ � ������������ ����, �� ����� ��� ������ � ���
    --c_ITEM_TYPE_PAYMENT  CONSTANT char(1) := 'P';  -- ������ (PAYMENT)
    --c_ITEM_TYPE_TRANSFER CONSTANT char(1) := 'T';  -- ������� �������� �������
    --c_ITEM_TYPE_ADVANCE  CONSTANT char(1) := 'A';  -- ����� �� ������� ����������� �������
    -- �������, ������� ������ � ������������ ����
    c_ITEM_TYPE_BILL     CONSTANT char(1) := 'B';  -- ������� ���������� �� ������
    c_ITEM_TYPE_ADJUST   CONSTANT char(1) := 'A';  -- ������� ������������

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
    c_CHARGE_TYPE_SLA CONSTANT char(3) := 'SLA';   -- ������� SLA
    
    -- ���������� ������
    c_ORDER_LOCK_NEW   CONSTANT integer := 901;    -- ���������� ������ ������
    c_ORDER_LOCK_NOPAY CONSTANT integer := 902;    -- ���������� �� ��������
    c_ORDER_LOCK_TECH  CONSTANT integer := 903;    -- �� ����������� �������
    c_ORDER_LOCK_SELF  CONSTANT integer := 904;    -- �������������� (������� ��������)
    c_ORDER_LOCK_OM    CONSTANT integer := 905;    -- ������� ����������, ���������� �� ������� ������� (OM)
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���� ������� ADDRESS_TYPE �� ������ ACCOUNT_NAME_INFO_T, CUSTOMER_T, CONTRACTOR_T 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_ADDR_TYPE_JUR CONSTANT char(3) := 'JUR';     -- ����������� ����� ��� ��. ���
    c_ADDR_TYPE_LOC CONSTANT char(3) := 'LOC';     -- ����� ������������ ���������� ��� ���. ���
    c_ADDR_TYPE_REG CONSTANT char(3) := 'REG';     -- ����� ����������� ��� ���. ���
    c_ADDR_TYPE_DLV CONSTANT char(3) := 'DLV';     -- ����� ��������
    c_ADDR_TYPE_GRP CONSTANT char(3) := 'GRP';     -- ����� ���������������
    c_ADDR_TYPE_SET CONSTANT char(3) := 'SET';     -- ����� ��������� ������������

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_RATESYS_MMTS_ID      CONSTANT integer := 1201; -- ����������� ��������� ���� ����
    c_RATESYS_TOPS_ID      CONSTANT integer := 1202; -- ����������� ������� ������������ ��������� ���� 
    c_RATESYS_CCAD_ID      CONSTANT integer := 1203; -- ����������� IP
    c_RATESYS_XTTK_CLNT_ID CONSTANT integer := 1204; -- ����������� ��� spb_axe_10 ����������
    �_RATESYS_CL_OPR_ID    CONSTANT integer := 1205; -- ������ � �����������, ������� ������� � ������� ��� �������
    �_RATESYS_MON_TRF_ID   CONSTANT integer := 1206; -- ����������� ������, ������� ����� �� ������� (��� �����������)
    �_RATESYS_ABP_ID       CONSTANT integer := 1207; -- ����������� ������ ���������, ����� � ORDER_BODY_T
    �_RATESYS_SLA_ID       CONSTANT integer := 1208; -- SLA - ������ ����������� �������� 
    
    --=============================================================================--
    -- ���� ������������ CONTRACTOR_T
    --=============================================================================--
    c_CTR_TYPE_KTTK  CONSTANT varchar(20) := '����';
    c_CTR_TYPE_XTTK  CONSTANT varchar(20) := 'X���';
    c_CTR_TYPE_ZTTK  CONSTANT varchar(20) := 'ZTTK';
    c_CTR_TYPE_BRAND CONSTANT varchar(20) := 'BRAND';
    c_CTR_TYPE_AGENT CONSTANT varchar(20) := 'AGENT';
    c_CTR_TYPE_CSS   CONSTANT varchar(20) := 'CSS';
    
    --=============================================================================--
    -- ID ������� ����������� ���� CLIENT_T (������ ���� �� ����)
    --=============================================================================--
    c_CLIENT_PERSON_ID   CONSTANT integer := 1;
    c_SUBS_RESIDENT      CONSTANT integer := 1;  -- ���. ���� ��������
    c_SUBS_FOREINGER     CONSTANT integer := 2;  -- ���. ���� �� ��������

    --=============================================================================--
    -- ID ���������� ��� ����������� ���� CUSTOMER_T (������ ���� �� ����)
    --=============================================================================--
    c_CUSTOMER_PERSON_ID CONSTANT integer := 1; 
    c_DOC_PASSPORT       CONSTANT varchar2(40) := 'PASSPORT';   

    --=============================================================================--
    -- ���� ���� � ��������� CALENDAR_T
    --=============================================================================--
    c_CALENDAR_WEEKDAY_ID  CONSTANT integer := 1901;  -- �����
    c_CALENDAR_HOLIDAY_ID  CONSTANT integer := 1902;  -- �������� � ���������
    
    --=============================================================================--
    -- ������� ����������� ������
    --=============================================================================--    
    c_CUR_ORIG_ID          CONSTANT integer := 2601;  -- ������ � ������ �����
    c_CUR_BILL_DATE_ID     CONSTANT integer := 2602;  -- ����������� �� ���� ����������� �����
    c_CUR_PAY_DATE_ID      CONSTANT integer := 2603;  -- ����������� �� ���� �������
    
    --=============================================================================--
    -- �������� QoS (Quality of service)
    --=============================================================================--
    c_QoS_STD_ID           CONSTANT integer := 7501;  -- '�������� ������ - ������������'
    c_QoS_PRM_ID           CONSTANT integer := 7502;  -- '�������� ������ - �����������'
    c_QoS_RT_ID            CONSTANT integer := 7503;  -- '�������� ������ - �������� �����'
    
    --=============================================================================--
    -- ���� ������� �� ������� EVENT_TYPE_T
    --=============================================================================--
    c_EVENT_TYPE_CALL_LOCAL  CONSTANT integer := 1;  -- ������� ������
    c_EVENT_TYPE_CALL_ZONE   CONSTANT integer := 2;  -- ������� ������
    c_EVENT_TYPE_CALL_MG     CONSTANT integer := 3;  -- ������������� ������
    c_EVENT_TYPE_CALL_MN     CONSTANT integer := 4;  -- ������������� ������
    c_EVENT_TYPE_FREE_CALL   CONSTANT integer := 5;  -- ����� �� 8-800

    --=============================================================================--
    -- ���� ��������� ����� (������������ ��� �������� �� ������������ - � ���� ���� service_id)
    --=============================================================================--
    c_Local_TG         CONSTANT number := 1; -- ������� ����� ��� ������������� �������� (�� ����������� MSKL ��� ������������� ������������ ��, �� SI2000 ��. ������, ��������� � ������ >> ��) 
    c_�ommercial_TG    CONSTANT number := 2; -- ������������ ����� 
    c_Joint_TG         CONSTANT number := 3; -- �������������� � ���������� ����� 
    c_Not�ommercial_TG CONSTANT number := 4; -- �������������� ����� ( �������� � ����.) 
    c_Another_TG       CONSTANT number := 0; -- ��������� 


    --=============================================================================--
    -- ������ �������� ������� ������
    --=============================================================================--
    c_SW_NOT_FOUND           CONSTANT integer := -4; -- ���������� �� ������
    c_ORDER_NOT_FOUND        CONSTANT integer := -1; -- �� ������ �����
    c_ERR_SRV_NOT_FOUND      CONSTANT integer := -2; -- �� ������� ������ 
    c_NOT_ZONE_JOINT         CONSTANT integer := -3; -- ���� �� �������
    c_TOO_MANY_ORDERS        CONSTANT integer := -5; -- ��������� ������� (������ ��������)

    --=============================================================================--
    -- ���� BDR-��
    --=============================================================================--
    c_BDR_MMTS_A             CONSTANT integer := 1; -- ���������� ������� � (����. MDV.T01_MMTS_CDR)
    c_BDR_Agent_MMTS_A       CONSTANT integer := 5; -- ���������� ������� � ��� ��������� ������� (����. MDV.T01_MMTS_CDR)
    c_BDR_Samara_A           CONSTANT integer := 2; -- ��� ������� ������ ���������� ������ � (����. MDV.X03_XTTK_CDR)
    c_BDR_SPb                CONSTANT integer := 3; -- ����� �������� ��� ���. ������������ ���� �� ������� ��� ��� �����������
    c_BDR_Zone_A             CONSTANT integer := 4; -- ���������� ������� � (����. MDV.Z01_ZONE_CDR)         
    c_BDR_NovTk              CONSTANT integer := 5; -- ����� �������� ��� �����������. ������������ ���� �� ������� ��� ��� �����������
    c_BDR_SPb_Clients        CONSTANT integer := 6; -- ���������� ������ � ����������� ���
    
    -- ==============================================================================
    -- �������� ��������� ����� (id �� �������� Service_T � SubService_T ), 
    -- ��� ������� �� ������� MDV.T03_MMTS_CDR
    -- ==============================================================================
    c_MMTS_Serv_id           CONSTANT varchar2(8) := '1,2';
    c_MMTS_SubServ_id        CONSTANT varchar2(8) := '1,2';
    
    --=============================================================================--
    -- ����������� XTTK
    --=============================================================================--    
    c_NovTk_SW               CONSTANT varchar2(16) := 'novtk_si2000';
    c_NovTk_SW_Id            CONSTANT number       := 6; -- Id ����������� ���������� �� ������� PIN.SWITCH_T
    c_Samara_SW              CONSTANT varchar2(16) := 'samara_si3000';
    c_SPb_SW                 CONSTANT varchar2(16) := 'spb_axe10';
    c_SPb_SW_Id              CONSTANT number       := 7810; -- Id ����������� SPb �� ������� PIN.SWITCH_T
    
    --=============================================================================--
    -- �������, ������������ ��� �����������
    --=============================================================================--    
    /*c_CDR_Table  CONSTANT varchar2(16) := 'MDV.T03_MMTS_CDR';
    c_XTTK_Table CONSTANT varchar2(16) := 'MDV.X03_XTTK_CDR';    
    c_Zone_Table CONSTANT varchar2(16) := 'MDV.Z03_ZONE_CDR';
    
    c_BDR_MMTS_Table   CONSTANT varchar2(18) := 'PIN.E04_BDR_MMTS_T';
    c_BDR_XTTK_Table   CONSTANT varchar2(16) := 'PIN.E02_BDR_XTTK';    
    c_BDR_SPb_Table    CONSTANT varchar2(16) := 'PIN.E03_BDR_SPB';
    c_BDR_NovTk_Table  CONSTANT varchar2(20) := 'PIN.E03_BDR_NOVTK';
    c_BDR_SPb_Cl_Table CONSTANT varchar2(32) := 'PIN.E05_BDR_SPB_CLIENTS';*/
    
    --=============================================================================--
    -- �������� ���� Service_Id � CDR-e, ������������ ������ �� 8800
    --=============================================================================--    
    c_FreeCall_Id    CONSTANT number := 6;
    
    --=============================================================================--
    -- ������ ����������
    --=============================================================================--
    c_ERR_ITEM               CONSTANT integer := 100; -- ������� �� ����������, ������ �� �/������ �� ��������� � ������� � item_t  
    c_ACC_NOT_FOUND          CONSTANT integer := -4; -- �� ������ �/���� �� ������
    c_NF_TAR_PLAN            CONSTANT integer := -5; -- �� ������ �������� ���� 
    c_TARIFF_NOT_FOUND       CONSTANT integer := -6; -- �� ������ �������� ���� � �� 
    c_PRICE_NOT_FOUND        CONSTANT integer := -7; -- �� ������� �������� ��� ������ ������� � � �
    c_VOL_PRICE_NOT_FOUND    CONSTANT integer := -8; -- �� ��������� �������� ��� ��������� ������
    �_TZONE_NOT_FOUND        CONSTANT integer := -9; -- �� ������� ���� ���������� ������   
    c_ABC_NOT_FOUND          CONSTANT integer := -10; -- ����������� �� ������� ��� ������ � DEF
    c_DEF_NOT_FOUND          CONSTANT integer := -11; -- ����������� �� ������� ��� ������ �� DEF  
    �_SRV_NOT_FOUND          CONSTANT integer := -12; -- �� ������� ������     
    �_ZMDL_MP_NOT_FOUND      CONSTANT integer := -13; -- �� ������� ������� ������ ��� ���������� ������
    �_ZMDL_TRF_NOT_FOUND     CONSTANT integer := -14; -- �� ������� ������� ������, ��������� ��� ������
    c_BILL_NOT_FOUND         CONSTANT integer := 1;  -- ������ ����������������� �������, �� �� ������ bill_id  
    c_BILL_TOO_MANY          CONSTANT integer := 2;  -- ������ ����������������� �������, �� ���� ��������� �������� bill_id     
    c_TOO_MANY_BILLS         CONSTANT integer := -15; -- ���� ��������� �������� bill_id ��� ��������� ������    
    c_BILL_IS_CLOSED         CONSTANT integer := -16;  -- ������ ����������������� �������, �� ����, � ������� ��� ������ ����, ������
    c_BILL_NOT_CORRECT       CONSTANT integer := -17;  -- �������� ���� �� ����������� ������ ��� ������
    c_NO_BILL_FOUND          CONSTANT integer := -18;  -- bill_id �� ������ 

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ID ��������� ������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_PS_KTTK_201401           CONSTANT integer := 0;	  -- ��������� ������� "������ ��������" �� 01.01.2014
    c_PS_SBRF_ONLINE           CONSTANT integer := 1;	  -- SBRF-Online
    c_PS_SBRF_IA               CONSTANT integer := 2;	  -- SBRF-Internet Acquiring
    c_PS_COMPAY                CONSTANT integer := 3;	  -- Comepay
    c_PS_RAPIDA                CONSTANT integer := 4;	  -- Rapida
    c_PS_VTB_24                CONSTANT integer := 5;	  -- VTB-24
    c_PS_CYBERPLAT             CONSTANT integer := 6;	  -- Cyberplat
    c_PS_PSB                   CONSTANT integer := 7;	  -- PSB
    c_PS_OSMP                  CONSTANT integer := 8;	  -- OSMP
    c_PS_WESTURAL_SBRF         CONSTANT integer := 9;	  -- WestUral SBRF
    c_PS_FREECASHDESK          CONSTANT integer := 10;	-- FreeCashDesk
    c_PS_SBRF                  CONSTANT integer := 11;	-- SB RF
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ���� �����������
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    c_SIGNER_HEAD              CONSTANT integer := 6101;-- ������������
    c_SIGNER_BOOKER            CONSTANT integer := 6102;-- ���������
    c_PS_SBRF_SELLER           CONSTANT integer := 6103;-- ��������

    --=============================================================================--
    -- ���� ����� �������� ������� ��� �������� ������������� (������� �����: OP_CONTRACT_POOLS_T)
    --=============================================================================--
    c_OP_POOL_TYPE_NORMAL      CONSTANT integer := 1; -- ��� ������� 11-�� ������� �������
    c_OP_POOL_TYPE_SHORT       CONSTANT integer := 2; -- ��� �������� ������� (����� ������ ����� ���� �����) 
   
    --=============================================================================--
    -- ���� ������� ���  �������� ������������� (������� �����: OP_RATE_PLAN)
    --=============================================================================--
    -- ��� ������� ��������
    --    c_OP_RATE_PLAN_TYPE_D      CONSTANT integer := 1; -- �������� ����� 
    --    c_OP_RATE_PLAN_TYPE_R      CONSTANT integer := 2; -- ��������� ����� 
    
    -- ��� ������ ��������  (������� ���������� ������ ������: X07_OP_RATE_PLAN)
    c_OP_RATE_PLAN_TYPE_DT     CONSTANT integer := 1; -- �������� ����� ����������  (������� �)
    c_OP_RATE_PLAN_TYPE_DI     CONSTANT integer := 2; -- �������� ����� ���������   (������� �)
    c_OP_RATE_PLAN_TYPE_DIP    CONSTANT integer := 5; -- �������� ����� ��������� �� ��-��������� ���� (������� �)

    c_OP_RATE_PLAN_TYPE_RT     CONSTANT integer := 3; -- ��������� ����� ���������� (������� �)
    c_OP_RATE_PLAN_TYPE_RI     CONSTANT integer := 4; -- ��������� ����� ���������  (������� �)
    c_OP_RATE_PLAN_TYPE_RIP    CONSTANT integer := 6; -- ��������� ����� ��������� �� ��-��������� ��� (������� �)
    
    --������ �� 4-� ������ ������ �������������� ����� ����������:
    --      ��� ������
    --      ������� ������� ���������������� ������ �� �������
    -- ���� ������ 
    c_TARIF_VOL_TYPE_NO      CONSTANT INTEGER := 0; -- ������� ������� ����� (������ � ����� ����� � X07_ORD_PRICE_T) 
    c_TARIF_VOL_TYPE_VOL     CONSTANT INTEGER := 1; -- �������� �����        (������ � ����� ����� � X07_ORD_PRICE_V_T)

    -- ������� ������� ���������������� ������ �� �������
    c_FLAG_GARANT_VOL_NO     CONSTANT INTEGER := 0; -- ��� ���������������� ������
    c_FLAG_GARANT_VOL_YES    CONSTANT INTEGER := 1; -- � ��������������� �������
   
    -- ������� ��������� ������ � �����
    c_RATEPLAN_TAX_INCL     CONSTANT CHAR(1 BYTE) := 'Y';
    c_RATEPLAN_TAX_NOT_INCL CONSTANT CHAR(1 BYTE) := 'N';

    -- �������� ������������� ��������� �����
    c_NOT_SHARE_TG   CONSTANT number := 1; -- �� ����������� �� (���� ������ ���� �� ��������� ��) 
    c_SHARE_TG       CONSTANT number := 2; -- ������� ����������� �� (���� � ������ ������� �� ���� 
                                           -- ������� ����� ��������, �� ������ ������� �� ��������� ������ ��-����� ��������,
                                           -- ���� ���, �� �� ��������� ��)
    c_FULL_SHARE_TG  CONSTANT number := 3; -- ���������� ����������� �� (���� ������ ���� �� ��������� ������ ��������)  


    --=============================================================================--
    -- ID �������� ��� ����������� ���� CONTRACTOR_T (�������� ���� �� ����)
    c_CONTRACTOR_KTTK_ID CONSTANT integer := 1;
    c_KTTK_J_BANK_ID     CONSTANT integer := 1; -- ID ����� ��� ������ ���.���
    c_KTTK_P_BANK_ID     CONSTANT integer := 2; -- ID ����� ��� ������ ��.���
    
    -- ID ��������� ��� ����������� ���� MANAGER_T (���� ���� �� ����)
    c_MANAGER_SIEBEL_ID  CONSTANT integer := 1;

    -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    --               ��������� � �������
    -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    -- ������� ��� ��������� �������������� Rep_Period_Id �� ������� ����
    FUNCTION Get_Period_Id(p_Date IN date) RETURN INTEGER DETERMINISTIC PARALLEL_ENABLE;


END PK00_CONST;
/
CREATE OR REPLACE PACKAGE BODY PK00_CONST IS

-- --------------------------------------------------------------------------------- --
-- ID ������ ������� D I C T I O N A R Y _ T
-- --------------------------------------------------------------------------------- --    
-- ��� �������� �����
FUNCTION k_DICT_KEY_ACCOUNT_TYPE    RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 1; END;
-- ������ �������� �����
FUNCTION k_DICT_KEY_ACCOUNT_STATUS  RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 2; END;
-- ��� �����
FUNCTION k_DICT_KEY_BILL_TYPE       RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 3; END;
-- ������ �����
FUNCTION k_DICT_KEY_BILL_STATUS     RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 4; END;
-- ��� ������� �����
FUNCTION k_DICT_KEY_ITEM_TYPE       RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 5; END;
-- ������ ������� �����
FUNCTION k_DICT_KEY_ITEM_STATUS     RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 6; END;
-- ���� ����������
FUNCTION k_DICT_KEY_CHARGE_TYPE     RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 7; END;
-- ��� ������
FUNCTION k_DICT_KEY_ADDRESS_TYPE    RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 8; END;
-- ��� ���������� ������
FUNCTION k_DICT_KEY_ORDER_LOCK_TYPE RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 9; END;
-- ������ �������� �����
FUNCTION k_DICT_KEY_DELIVERY_TYPE   RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 10; END;
-- ��� �����������
FUNCTION k_DICT_KEY_CONTRACTOR_TYPE RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 11; END;
-- ������������
FUNCTION k_DICT_KEY_RATESYSTEM      RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 12; END;
-- �����. �������������� ������ ����� � CDR ������ ��������
FUNCTION k_DICT_KEY_CDR_SERVICE_ID  RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 13; END;
-- ������ �������� CDR � �/�
FUNCTION k_DICT_KEY_CDR_BIND_ERROR  RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 14; END;
-- ������ ����������� BDR
FUNCTION k_DICT_KEY_BDR_RATE_ERROR  RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 15; END;
-- ���� ���� ���������
FUNCTION k_DICT_KEY_CALENDAR        RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 19; END;
-- �������, ������� ����������� ������ ��������
FUNCTION k_DICT_KEY_BILLING         RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 20; END;
-- ���� ������ BDR 
FUNCTION k_DICT_KEY_TRF_STAT        RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 21; END;
-- ���� ������ �������� CDR
FUNCTION k_DICT_KEY_BIND_ERR        RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 22; END;
-- ������� ����������� � ORDER_BODY_T.RATE_LAVEL_ID
FUNCTION k_DICT_KEY_RATE_LEVEL      RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 23; END;
-- ������� ����������� � ORDER_BODY_T.RATE_RUEL_ID
FUNCTION k_DICT_KEY_RATE_RULE       RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 24; END; 
-- ������� �������������� ��������� ������
FUNCTION k_DICT_KEY_DISCOUNT_RULE   RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 25; END;
-- ������� ����������� ������
FUNCTION k_DICT_KEY_CUR_CONV        RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 26; END;
-- ���� ����������� ��� ������������ ������
FUNCTION k_DICT_KEY_DETAIL_TYPE     RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 27; END;
-- ������������ ����� �������� �������� service_id CDR-�
FUNCTION k_DICT_KEY_CB_SRV_TYPE     RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 50; END;
-- Subservices, ������������ ��� ���������� BDR-��
FUNCTION k_DICT_KEY_CB_SUBSRV_TYPE  RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 60; END;
-- ���� ����������
FUNCTION k_DICT_KEY_SIGNER_ROLE     RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 61; END;
-- ��� �������
FUNCTION k_DICT_PAYMENT_TYPE        RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 62; END;
-- ��� �������
FUNCTION k_DICT_CLIENT_TYPE         RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 63; END;
-- ������� �����
FUNCTION k_DICT_MARKET_SEGMENT      RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 64; END;
-- ������ ��������
FUNCTION k_DICT_DELIVERY_METHOD     RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 65; END;
-- �������� � ���������
FUNCTION k_DICT_PAYMENT_OPERATIONS  RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 66; END;
-- ������� ��������� �������� ������ 
FUNCTION k_DICT_SPEED_UNIT          RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 67; END;
-- IP - ������
FUNCTION k_DICT_IP_SERVICE          RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 68; END;
-- ������ ��������
FUNCTION k_DICT_VOICE_UNIT          RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 69; END;
-- ����� ������� (�����������)
FUNCTION k_DICT_IP_VOLUME_UNIT      RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 70; END; 
-- ��� ��������
FUNCTION k_DICT_CONTRACT_TYPE       RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 71; END; 
-- ������� ���������� ������ ����������� Huawei SoftX3000'
FUNCTION k_DICT_TERMINATING_REASON  RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 72; END;  
-- Q.850 - ����������� ��� ���������� ������
FUNCTION k_DICT_Q850                RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 73; END;
-- SoftX3000 - ��� ���������� ������
FUNCTION k_DICT_TERMINATION_CODE    RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 74; END;
-- �������� �������������� ������ �� IP - ������
FUNCTION k_DICT_QoS                 RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 75; END;
-- ��� �������� ������� �����
FUNCTION k_DICT_BILL_HISTORY_TYPE   RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 76; END; 
-- ������� ������������ ����� �����-�������
FUNCTION k_DICT_INV_RULE            RETURN INTEGER DETERMINISTIC IS BEGIN RETURN 77; END;

-- --------------------------------------------------------------------------------- --
-- ������� ��� ��������� �������������� Rep_Period_Id �� ������� ����
FUNCTION Get_Period_Id(p_Date IN date
                      ) RETURN INTEGER DETERMINISTIC PARALLEL_ENABLE 
IS 
BEGIN 
    RETURN TO_NUMBER(TO_CHAR(p_Date,'YYYYMM')); 
END;

END PK00_CONST;
/
