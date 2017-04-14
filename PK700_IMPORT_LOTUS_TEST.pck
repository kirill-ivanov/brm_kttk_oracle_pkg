create or replace package PK700_IMPORT_LOTUS_TEST is
  -- ����� ��� ������� ������ (�����, ��������) �� ������
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK700_IMPORT_LOTUS';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;

    type t_refc    is ref cursor;
    
-- �������� ����� ID ��� ��������� ������
FUNCTION Next_import_id RETURN INTEGER;
-- �������� ����� ID �������� ������ �� OM
FUNCTION Next_close_order_id RETURN INTEGER;  
-- �������� ����� ID ���������� ������ �� OM
FUNCTION Next_block_order_id RETURN INTEGER;
-- �������� ����� ID ���� ������ �� OM
FUNCTION Next_tarif_id RETURN INTEGER;   
-- ������� ������ �� ������ ��������
PROCEDURE Split_string (
        p_table  OUT vc100_table_t,
        p_list   VARCHAR2,
        p_delim  VARCHAR2 DEFAULT ','
    );
-- ������� CLOB �� ������ ��������
PROCEDURE Split_CLOB (
        p_table  OUT vc100_table_t,
        p_list   CLOB,
        p_delim  VARCHAR2 DEFAULT ','
    );
    
PROCEDURE IMPORT_MMTS_ORDERS
  (
    p_order_no       IN VARCHAR2,
    p_order_phones   IN VARCHAR2,
    p_service_name   IN VARCHAR2,
    p_order_date     IN DATE,
    p_time_zone      IN NUMBER,
    p_agreement_no   IN VARCHAR2,
    p_agreement_date IN DATE,
    p_agreement_end_date IN DATE DEFAULT NULL,
    p_currency       IN INTEGER DEFAULT 810,
    p_jur_zip    IN VARCHAR2, p_jur_state    IN VARCHAR2, p_jur_city    IN VARCHAR2, p_jur_address    IN VARCHAR2,
    p_dlv_zip    IN VARCHAR2, p_dlv_state    IN VARCHAR2, p_dlv_city    IN VARCHAR2, p_dlv_address    IN VARCHAR2,
    p_grp_zip    IN VARCHAR2, p_grp_state    IN VARCHAR2, p_grp_city    IN VARCHAR2, p_grp_address    IN VARCHAR2,
    p_inn            IN VARCHAR2,
    p_kpp            IN VARCHAR2,
    p_vat            IN NUMBER,
    p_customer_name  IN VARCHAR2,
    p_customer_short_name                   IN VARCHAR2,
    p_erp_code                              IN VARCHAR2,
    p_contract_manager                      IN VARCHAR2,
    p_contact_person                        IN VARCHAR2,
    p_contact_phone                         IN VARCHAR2,
    p_contact_fax                           IN VARCHAR2,
    p_contact_email                         IN VARCHAR2,
    p_contractor                            IN VARCHAR2,
    p_min_sum                               IN NUMBER,
    p_abon                                  IN NUMBER,
    p_speed                                 IN NUMBER,
    p_speed_unit                            IN VARCHAR2,
    p_point_1                               IN VARCHAR2,
    p_point_2                               IN VARCHAR2,
    p_point_1_address                       IN VARCHAR2,
    p_point_2_address                       IN VARCHAR2,
    p_ont_bill_summ                         IN NUMBER,
    p_ont_bill_date                         IN DATE DEFAULT NULL,    
    p_account_type                          IN VARCHAR2 DEFAULT 'J',
    p_agent_account_no                      IN VARCHAR2 DEFAULT NULL,
    p_m_z_price                             IN NUMBER,
    p_M_PREPAYED                            IN NUMBER,
    p_OM_VERSION                            IN INTEGER DEFAULT 1,
    p_z_price_def                           IN NUMBER,
    p_tariff_cur                            IN NUMBER,
    p_brand                                 IN VARCHAR2,
    p_pay_currency                          IN INTEGER,
    p_bill_currency                         IN INTEGER,
    p_tariff_type                           IN VARCHAR2,
    p_order_code                            IN VARCHAR2,
    p_old_order_no                          IN VARCHAR2,
    p_old_order_code                        IN VARCHAR2,
    p_order_level                           IN INTEGER,
    p_order_type                            IN VARCHAR2,
    p_vpn_zone                              IN INTEGER,
    p_market_segment                        IN VARCHAR2,
    p_client_type                           IN VARCHAR2,
    p_agreement_code                        IN VARCHAR2,
    p_network_id                            IN INTEGER,
    p_switch_id                             IN INTEGER,     
    p_result                                OUT INTEGER
  );
-----------------------------------------------
-- ������� ������ �������������� ������
-----------------------------------------------
PROCEDURE GetImportData(p_recordset OUT t_refc);
-----------------------------------------------------------------
-- ������� ������ ����� ������ (�������� ���������������� �������)
-----------------------------------------------------------------
PROCEDURE GetImportData_secure(p_recordset OUT t_refc, p_contractor_list IN VARCHAR2);
-----------------------------------------------
-- ������� ������ �� ID
-----------------------------------------------
PROCEDURE GetImportDataById(p_recordset OUT t_refc, p_ID IN INTEGER);

-------------------------------------------------
-- ����� �������� � �������� �� ������
-------------------------------------------------
PROCEDURE GetContractByNumber(p_recordset OUT t_refc, p_contract_no IN varchar2);
PROCEDURE GetContractByNumber(p_recordset OUT t_refc, p_contract_no IN varchar2, p_erp_code IN VARCHAR2);
------------------------------------------------------
-- ����� ������� ������ � �������� �� ������ ��������
------------------------------------------------------
PROCEDURE GetAccountsByContractNo(p_recordset OUT t_refc, p_contract_no IN varchar2);
PROCEDURE GetAccountsByContractNo(p_recordset OUT t_refc, p_contract_no IN varchar2, p_erp_code IN varchar2);
------------------------------------------
-- �������� ������ �����
------------------------------------------
PROCEDURE GetServices(p_recordset OUT t_refc);

------------------------------------------
-- �������� ������ ����������� ������
------------------------------------------
PROCEDURE GetSubServices(p_recordset OUT t_refc, p_service_id IN INTEGER);
  
---------------------------------------
-- �������� ������ �� �� ��������
---------------------------------------
PROCEDURE GetRatePlanByPrefix(p_recordset OUT t_refc, p_prefix IN VARCHAR2);
-----------------------------------------------------
--- �� �������� + service
PROCEDURE GetRatePlanByPrefix(p_recordset OUT t_refc, p_prefix IN VARCHAR2, p_service_id IN INTEGER);
------------------------------------------------
-- �������� ��� ��������� ����� �� ID
------------------------------------------------
FUNCTION GetRatePlanNameById(p_rateplan_id IN INTEGER) RETURN VARCHAR2;

-----------------------------------------------------
-- �������� ������ �������� (��� BCR) �� ��������
-----------------------------------------------------
PROCEDURE GetClientsByPrefix(p_recordset OUT t_refc, p_prefix IN VARCHAR2);
-----------------------------------------------------
-- �������� ������ ���� �������� (��� BCR)
-----------------------------------------------------
PROCEDURE GetAllClients(p_recordset OUT t_refc);
-----------------------------------------------------
-- �������� ������ �� ���  �� ��������
-----------------------------------------------------
PROCEDURE GetCustomersByPrefix(p_recordset OUT t_refc, p_prefix IN VARCHAR2);
-----------------------------------------------------
-- �������� ������ ���������� �� ��������
-----------------------------------------------------
PROCEDURE GetManagersByPrefix(p_recordset OUT t_refc, p_prefix IN VARCHAR2);
---------------------------------------------------------
-- �������� ������ ���������� �� �������� � Contractor'�
--------------------------------------------------------
PROCEDURE GetManagersByPrefixC(p_recordset OUT t_refc, p_prefix IN VARCHAR2, p_contractor_id IN INTEGER);
-----------------------------------------------------
-- �������� ������ �����������-��������� (SALES + KTTK)
-----------------------------------------------------
PROCEDURE GetContractorsSales(p_recordset OUT t_refc);
-----------------------------------------------------
-- �������� ������ ����������� �� ��������
-----------------------------------------------------
PROCEDURE GetContractorsByPrefix(p_recordset OUT t_refc, p_prefix IN VARCHAR2);
------------------------------------------------------
-- ����� ������� (customer) �� ���/���
-- ���� �� ������, ������� ���������!
------------------------------------------------------
PROCEDURE GetCustomerIDByInnKpp(
          p_customer_id       OUT INTEGER, 
          p_erp_code          IN VARCHAR2,
          p_inn               IN VARCHAR2,
          p_kpp               IN VARCHAR2, 
          p_name              IN VARCHAR2,
          p_short_name        IN VARCHAR2,
          p_error             OUT INTEGER,
          p_short_name_out    OUT VARCHAR2);
------------------------------------------------
-- ����� ������� (��� BCR - CLIENT_T)
------------------------------------------------
FUNCTION GetBCRClient(p_client_name  IN VARCHAR2) RETURN INTEGER;
-------------------------------------------
-- ���������� ������ �������
-------------------------------------------
PROCEDURE GetBrands(p_recordset OUT t_refc);  
------------------------------------------------
-- �������� ������ ��������� �����
------------------------------------------------
PROCEDURE GetMarketSegments(p_recordset OUT t_refc);
------------------------------------------------
-- �������� ������ ����� ��������
------------------------------------------------
PROCEDURE GetClientTypes(p_recordset OUT t_refc);
------------------------------------------------
-- �������� ������ �������� ��������
------------------------------------------------
PROCEDURE GetDeliveryMethods(p_recordset OUT t_refc);
------------------------------------------------
-- �������� ID KTTK
------------------------------------------------
FUNCTION GetKTTKId RETURN INTEGER;
--------------------------------------------------
-- �������� ������ �����������-�������
--------------------------------------------------
PROCEDURE GetContractorsBrands(p_recordset OUT t_refc);
--------------------------------------------------
-- �������� ������ �����������-���� �� �������� XTTK
--------------------------------------------------
PROCEDURE GetContractorsXTTK(p_recordset OUT t_refc);
--------------------------------------------------
-- �������� ������ ������� �� ID ����������
--------------------------------------------------
PROCEDURE GetAgentsByContractorID(p_recordset OUT t_refc, p_contractor_id IN INTEGER);
--------------------------------------------------
-- �������� ������ ������ ����������
--------------------------------------------------
PROCEDURE GetBanksByContractorID(p_recordset OUT t_refc, p_contractor_id IN INTEGER);
PROCEDURE GetBanksByContractorID(p_recordset OUT t_refc, p_contractor_id IN INTEGER, p_billing_id IN INTEGER);
--------------------------------------------------
-- �������� ������ ���������� ������
--------------------------------------------------
PROCEDURE GetBanks2ByContractorID(p_recordset OUT t_refc, p_bank_id IN INTEGER);
--------------------------------------------------
-- ������ ������ ������
--------------------------------------------------
PROCEDURE ChangeImportStatus(p_import_id IN INTEGER, p_import_status IN VARCHAR2, p_import_status_info IN VARCHAR2);
-----------------------------------------------------------------------------------------------------
-- ������ ������ ������, ��� �������� � �� � ������ BILLING, ���� � ������� ������� ������ NEW!!!
-- ������ ������, ����� ������ �� ������ � BRM, � ������ ��������� �� � ������� � ��. 
-- ������ �������� � ������� ������� � ��������....
-----------------------------------------------------------------------------------------------------
PROCEDURE RemoveOrdersFromImport(p_order_no IN VARCHAR2, p_result OUT INTEGER);
-----------------------------------------------------
-- ����� �������� ����� �� ������
-----------------------------------------------------
FUNCTION Find_account_by_no(p_account_no  IN VARCHAR2) RETURN INTEGER;


--------------------------------------------------
-- ������ ���������� ������� � EXT01_TMP_PHONES
--------------------------------------------------
PROCEDURE Write_TMP_phones(
        p_list   CLOB,
        p_delim  VARCHAR2 DEFAULT ','
    );
------------------------------------------------
-- ���������� ������������ �-�������
------------------------------------------------
PROCEDURE UpdateParsedPnones(p_import_id IN INTEGER, p_phones IN BLOB);

---------------------------------------------------
-- �������� - ������� ������ ��� ���?
---------------------------------------------------
FUNCTION CheckParsedPhones(p_line_id IN INTEGER) RETURN INTEGER;

---------------------------------------------------
-- �������� �� ����������� �-�������
---------------------------------------------------
FUNCTION CheckCrossedPhones(p_line_id IN INTEGER) RETURN INTEGER;
--------------------------------------------------
-- �������� ������ ����������� �������
--------------------------------------------------
PROCEDURE GetCrossedPhones(p_recordset OUT t_refc, ImportId IN INTEGER, p_order_date IN DATE DEFAULT SYSDATE);
-----------------------------------------------------------------
-- �������� ������ ����������� �������, �������� SUBSERVICE
-----------------------------------------------------------------
PROCEDURE GetCrossedPhonesSubs(p_recordset OUT t_refc, ImportId IN INTEGER, p_order_date IN DATE DEFAULT SYSDATE, p_subservice_id IN VARCHAR2);
PROCEDURE GetCrossedPhonesSubs(p_recordset OUT t_refc, ImportId IN INTEGER, p_order_date IN DATE DEFAULT SYSDATE, p_subservice_id IN VARCHAR2,p_phones IN CLOB);
-----------------------------------------------------------------
-- �������� ������ ����������� ��������� �������, �������� SUBSERVICE
-----------------------------------------------------------------
PROCEDURE GetCrossedPhonesSubsAG(p_recordset OUT t_refc, 
                               ImportId IN INTEGER, 
                               p_order_date IN DATE DEFAULT SYSDATE, 
                               p_subservice_id IN VARCHAR2,
                               p_phones IN STRING);
-------------------------------------------------
-- �������� ������ ������� �� ��������� �������
-------------------------------------------------
PROCEDURE GetTMPPhones(p_recordset OUT t_refc);

------------------------------------------------
-- �������� ID ������ �� ����� ������ � ��
------------------------------------------------
FUNCTION GetServiceIdByOMName(p_om_service_name  IN VARCHAR2) RETURN INTEGER;

------------------------------------------------
-- �������������� CLOB � BLOB � ��������
------------------------------------------------
FUNCTION CLOB_TO_BLOB (p_clob CLOB) RETURN BLOB;
FUNCTION BLOB_TO_CLOB (blob_in IN BLOB) RETURN CLOB;
--------------------------------------------
-- TEST
--------------------------------------------
--------------------------------------------
-- ��� ������ ��������������
--------------------------------------------
PROCEDURE Get_Akt(p_recordset OUT t_refc, 
                  p_start_bill_date IN DATE, 
                  p_end_bill_date IN DATE,
                  p_client_id IN INTEGER );
-- �������� �������� ��� �� ���� �������
PROCEDURE IMPORT_MMTS_ORDERS_SPB
  (
    --p_result_str     OUT VARCHAR2,
    p_order_no       IN VARCHAR2,
    p_order_phones   IN VARCHAR2,
    p_service_name   IN VARCHAR2,
    p_order_date     IN DATE,
    p_time_zone      IN NUMBER,
    p_agreement_no   IN VARCHAR2,
    p_agreement_date IN DATE,
    p_agreement_end_date IN DATE DEFAULT NULL,
    p_currency       IN INTEGER,
    p_jur_zip    IN VARCHAR2, p_jur_state    IN VARCHAR2, p_jur_city    IN VARCHAR2, p_jur_address    IN VARCHAR2,
    p_dlv_zip    IN VARCHAR2, p_dlv_state    IN VARCHAR2, p_dlv_city    IN VARCHAR2, p_dlv_address    IN VARCHAR2,
    p_grp_zip    IN VARCHAR2, p_grp_state    IN VARCHAR2, p_grp_city    IN VARCHAR2, p_grp_address    IN VARCHAR2,
    p_inn            IN VARCHAR2,
    p_kpp            IN VARCHAR2,
    p_vat            IN NUMBER,
    p_customer_name  IN VARCHAR2,
    p_customer_short_name                   IN VARCHAR2,
    p_erp_code                              IN VARCHAR2,
    p_contract_manager                      IN VARCHAR2,
    p_contact_person                        IN VARCHAR2,
    p_contact_phone                         IN VARCHAR2,
    p_contact_fax                           IN VARCHAR2,
    p_contact_email                         IN VARCHAR2,
    p_contractor                            IN VARCHAR2,
    p_min_sum                               IN NUMBER,
    p_abon                                  IN NUMBER,
    p_speed                                 IN NUMBER,
    p_speed_unit                            IN VARCHAR2,
    p_point_1                               IN VARCHAR2,
    p_point_2                               IN VARCHAR2,
    p_point_1_address                       IN VARCHAR2,
    p_point_2_address                       IN VARCHAR2,
    p_ont_bill_summ                         IN NUMBER,
    p_ont_bill_date                         IN DATE DEFAULT NULL,
    p_result                                OUT INTEGER
  );
-------------------------------------------------------------
-- �������� �������� ������� �� OM
-------------------------------------------------------------
PROCEDURE IMPORT_CLOSED_ORDERS
  ( p_order_no       IN VARCHAR2,
    p_close_date     IN DATE,
    p_result         OUT INTEGER
  );
-------------------------------------------------------------
--  ����������/������������� ������� �� OM
-------------------------------------------------------------
PROCEDURE IMPORT_BLOCK_ORDERS
  ( p_order_no       IN VARCHAR2, -- ����� ������
    p_action         IN VARCHAR2, -- �������� (BLOCK | UNBLOCK)
    p_action_date    IN DATE,     -- ���� ���������� / �������������
    p_manager        IN VARCHAR2,  -- ��� ���������� / �������������
    p_result         OUT INTEGER
  );  
-------------------------------------------------------------
-- �������� �������� ������� �� OM ���� ���� ����
-------------------------------------------------------------
PROCEDURE IMPORT_CLOSED_ORDERS_TEST
  ( p_order_no       IN VARCHAR2,
    p_close_date     IN DATE,
    p_result         OUT INTEGER
  );
------------------------------------------------
-- ������� "�����" ���� ������
------------------------------------------------
PROCEDURE New_mgmn_tarif(
              p_ORDER_NO IN VARCHAR2, -- ����� ������
              p_ZONE_INIT_NAME IN VARCHAR2, -- ������������ ���� �������������
              p_ZONE_INIT_ABC IN VARCHAR2, -- �������  ���� �������������
              p_DIST_REG IN NUMBER, -- 1=�������������  2=������������
              p_TAX_INCLUDE IN NUMBER, -- 1=����� ������� � ���� 0=�� �������  
              p_round_v_id IN NUMBER, 
              /*        1    UP    ���������� �� ������ �����
                        2    DOWN    ���������� �� ������ ����
                        0    NONE    �� ����������� ���������� �� �����
                        3    UP Sec    ���������� �� ������� �����
                        4    UP 10Sec    ���������� ����� �� 10 ������
                        5    UP 2    ���������� �� ������ ����� �����    */                       
              p_currency_id IN NUMBER, -- ID ������ (�������� 810 - ��� �����)
              p_unpaid_seconds IN NUMBER, -- ���������� ����� (� ��������)
              p_is_tm_not_std  in varchar2,  -- NULL - ���� ������ ������� �����������
                                             -- Y - ���� ������ ������� �������������. ��� ���� ������ ���� ��������� ����. 4 ����:
                                             -- ������ ����������: DD:DD , ��� D-�����
              p_BT_MG_FROM        in varchar2,  -- ������ ������-������� ��� ��    (08:00)  
              p_BT_MG_TO          in varchar2,  -- ��������� ������-������� ��� �� (19:00)
              p_BT_MN_FROM        in varchar2,  -- ������ ������-������� ��� ��    (08:00)
              p_BT_MN_TO          in varchar2,   -- ��������� ������-������� ��� �� (20:00)
              p_is_zm_not_std     in varchar2,  -- NULL - ���� ������� ������ �����������
                                               -- Y - ���� ������� ������ �� �����������                                   
              p_TARIF_HTML IN CLOB, -- ��� �����
               p_result         OUT INTEGER
           );
------------------------------------------------
-- �������� ���� �����
------------------------------------------------
PROCEDURE Get_mgmn_tarif(p_recordset OUT t_refc, p_ORDER_NO IN VARCHAR2);           
------------------------------------------------------------------------
-- ������� ����� ��� IP Access InterConnect 
-- (���������� � ������ ������ ����� ��������� ������� 
-- ��� ���������� �� ������ � ������ ������ ����� ��������� �������)
------------------------------------------------------------------------
PROCEDURE New_ip02_tarif(           
              p_order_no IN varchar2,
              p_tarif    IN varchar2,
              p_service_type  IN varchar2,
              p_result OUT INTEGER);
------------------------------------------------
-- �������� IP �����
------------------------------------------------
PROCEDURE Get_ip_tarif(p_recordset OUT t_refc, p_ORDER_NO IN VARCHAR2);              
--------------------------------------------------
-- ��������� ID ��
--------------------------------------------------
PROCEDURE UpdateTariffId(p_order_no IN VARCHAR2, p_tariff_id IN INTEGER);          
------------------------------------------------
-- ��������� ID ���������� ��
--------------------------------------------------
PROCEDURE UpdateAGTariffId(p_order_no IN VARCHAR2, p_tariff_id IN INTEGER);
-- �������� ������ �������� �������� ������
------------------------------------------------
PROCEDURE Get_delivery_desc(p_recordset OUT t_refc);   
------------------------------------------------
-- �������� ID ������ �� ������ ��
------------------------------------------------
FUNCTION Find_account_by_id(p_account_id  IN VARCHAR2) RETURN INTEGER;
--------------------------------------------------------------------
-- �������� ID ��������� ������������� �� ������ � � �������� � ��
--------------------------------------------------------------------
FUNCTION Find_agent_id(p_account_no  IN VARCHAR2, p_brand_id IN INTEGER) RETURN INTEGER;          
-------------------------------------------
-- ���������� ������ �����
-------------------------------------------
PROCEDURE GetNetworks(p_recordset OUT t_refc);
---
PROCEDURE GetNetworks(p_recordset OUT t_refc, p_branch_id IN INTEGER);
----
PROCEDURE GetNetworksByContractors(p_recordset OUT t_refc, p_contractor_list IN VARCHAR2);
-------------------------------------------
-- ���������� ������  ������������ �� ID ����
-------------------------------------------
PROCEDURE GetSwitch(p_recordset OUT t_refc, p_network_id IN INTEGER);
---------------------------------------------
-- ����� ID ��������� ������ �� ID ������
---------------------------------------------
PROCEDURE GetSubservicesIdByOrderId(p_recordset OUT t_refc, p_order_id IN INTEGER);
---------------------------------------------
-- ����� Order_Body_ID ��� item'� �������� �����
---------------------------------------------
FUNCTION GetOrderBodyId4Item(p_order_id IN INTEGER) RETURN INTEGER;
---------------------------------------------
-- ����� ����� ������ �� ��� ID
---------------------------------------------
FUNCTION GetOrderNoById(p_order_id IN INTEGER) RETURN VARCHAR2;
---------------------------------------------
-- ����� ��� ������ �� ��������
---------------------------------------------
PROCEDURE GetOrdersByContractNo(p_recordset OUT t_refc, p_contract_no IN VARCHAR2);
------------------------------------------------
-- ����� ������� ������� ��� ������ �����
------------------------------------------------
FUNCTION GetRegionCode(p_account_id IN INTEGER) RETURN VARCHAR2;
------------------------------------------------
-- ����� ������� ����������� ��� IP
------------------------------------------------
PROCEDURE GetIpTariffRules(p_recordset OUT t_refc, p_lotus_tariff_type IN VARCHAR2, p_brm_service IN VARCHAR2);
---------------------------------------------------
-- �������� - ��� ����� ������������ ��� ���
---------------------------------------------------
FUNCTION CheckPhoneToUse(p_phone_num IN varchar2) RETURN INTEGER;
---------------------------------------------
-- ������ ������� ��� �������
---------------------------------------------
PROCEDURE GetRatePlanListP(p_recordset OUT t_refc);
---------------------------------------------
-- �������� ������������ ��� ������ ��
---------------------------------------------
FUNCTION GetNewRatePlanName(p_RP_name IN VARCHAR2) RETURN VARCHAR2;
---------------------------------------------------
-- �������� ������ �� �������� ���� � ORDER_BODY_T
---------------------------------------------------
PROCEDURE UpdateRatePlan(p_order_id IN INTEGER, p_order_body_id IN INTEGER, p_rateplan_id IN INTEGER);
-----------------------------------------------------
-- �������� ����������-�������� �� �������
-----------------------------------------------------
PROCEDURE GetContractorSaleByRegion(p_recordset OUT t_refc, p_contractor_id IN INTEGER);
------------------------------------------------
-- ���������� ������� "���. ��������"
------------------------------------------------
FUNCTION SetContractGos(p_contract_id IN INTEGER) RETURN INTEGER;
---------------------------------------------------------
-- ����������� ���������� ����� � ������ ������ �� ������
---------------------------------------------------------
PROCEDURE CopyOrderBody(p_order_id_from IN INTEGER, p_order_id_to IN INTEGER, p_order_date IN DATE);
------------------------------------------------------------------------------------
-- ��������� ������� ���������� PK30_CORRECT_TAX_INCL_T 
-- (������ � ������������� ��������� TAX_INCL � �������)
------------------------------------------------------------------------------------
PROCEDURE FIll_tax_incl(p_orderPbody_id IN INTEGER, p_account_type IN CHAR, p_tax_incl IN CHAR);
----------------------------------------------------------------------------------------
-- ���������� �������� ��� �������� ����������� ��������� ����������� �� �������� �����
----------------------------------------------------------------------------------------
PROCEDURE SendDetailByEmail(p_account_id IN INTEGER, p_email_type IN VARCHAR2);




PROCEDURE get_contractors(p_recordset OUT t_refc, p_contractor_list IN VARCHAR2);
PROCEDURE Get_address( 
               p_recordset     OUT t_refc, 
               p_contractor_id  IN INTEGER,
               p_date           IN DATE DEFAULT SYSDATE
             );


PROCEDURE Move_ccad_bdr (
               p_src_bill_id   IN INTEGER,    -- ID ����� ��� �������� ��������� �����-����
               p_src_period_id IN INTEGER,    -- ID ���������� ������� YYYYMM ���������
               p_dst_bill_id   IN INTEGER,    -- ID �����-����
               p_dst_period_id IN INTEGER     -- ID ���������� ������� �����-���� YYYYMM
           );
-----------------------------------------------------
-- �������� ���������� �������� ���
-----------------------------------------------------
PROCEDURE GetCSSDirections(p_recordset OUT t_refc);     
-----------------------------------------------------
-- �������� ������ �������� ��� �� ������
-----------------------------------------------------
PROCEDURE GetCSSDirectionsByOrder(p_recordset OUT t_refc, p_order_id IN INTEGER);
--------------------------------------------
-- ��������� ����������� � ��������
--------------------------------------------
PROCEDURE UpdateProfileContractors(
          p_src_brach_id     IN INTEGER,  
          p_dst_seller_id    IN INTEGER,
          p_dst_bank_id      IN INTEGER,
          p_dst_date_from    IN DATE);      
          
end PK700_IMPORT_LOTUS_TEST;
/
create or replace package body PK700_IMPORT_LOTUS_TEST
is
------------------------------------------
-- �������� ����� ID ��� ������
------------------------------------------
FUNCTION Next_import_id RETURN INTEGER IS
BEGIN
    RETURN SQ_IMPORT_LOTUS_ID.NEXTVAL; 
END;
------------------------------------------
-- �������� ����� ID �������� ������ �� OM
------------------------------------------
FUNCTION Next_close_order_id RETURN INTEGER IS
BEGIN
    RETURN SQ_IMPORT_LOTUS_CLOSE_ORDER_ID.NEXTVAL; 
END;
------------------------------------------
-- �������� ����� ID ���������� ������ �� OM
------------------------------------------
FUNCTION Next_block_order_id RETURN INTEGER IS
BEGIN
    RETURN SQ_IMPORT_LOTUS_BLOCK_ORDER_ID.NEXTVAL; 
END;
------------------------------------------
-- �������� ����� ID ���� ������ �� OM
------------------------------------------
FUNCTION Next_tarif_id RETURN INTEGER IS
BEGIN
    RETURN SQ_IMPORT_LOTUS_TARIF_ID.NEXTVAL; 
END;
------------------------------------------
-- ������� ������ � ������������� �� �����
------------------------------------------
FUNCTION Get_token(
        p_list   VARCHAR2,
        p_index  NUMBER,
        p_delim  VARCHAR2 DEFAULT ','
    ) RETURN VARCHAR2
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Get_token';
    v_start_pos NUMBER;
    v_end_pos   NUMBER;
BEGIN
    IF p_index = 1 THEN
        v_start_pos := 1;
    ELSE
        v_start_pos := INSTR(p_list, p_delim, 1, p_index - 1);
        IF v_start_pos > 0 THEN
            v_start_pos := v_start_pos + LENGTH(p_delim);            
        ELSE
            RETURN NULL;
        END IF;
    END IF;
    v_end_pos := INSTR(p_list, p_delim, v_start_pos, 1);
    IF v_end_pos > 0 THEN
        RETURN SUBSTR(p_list, v_start_pos, v_end_pos - v_start_pos);
    ELSE
        RETURN SUBSTR(p_list, v_start_pos);        
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.', c_PkgName||'.'||v_prcName );
END;

------------------------------------
-- ������� ������ �� ������ ��������
------------------------------------
PROCEDURE Split_string (
        p_table  OUT vc100_table_t,
        p_list   VARCHAR2,
        p_delim  VARCHAR2 DEFAULT ','
    )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Split_string';
    v_token     VARCHAR2(40);
    i           PLS_INTEGER := 1;
BEGIN
    -- �� ������ ������ ������ �������
    p_table := vc100_table_t();
    IF p_list IS NOT NULL THEN
        LOOP
            v_token := get_token( p_list, i , p_delim) ;
            EXIT WHEN v_token IS NULL ;
            p_table.EXTEND;
            p_table(i) := v_token;
            i := i + 1 ;
        END LOOP ;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_list='||p_list, c_PkgName||'.'||v_prcName );
END;

------------------------------------------
-- ������� CLOB � ������������� �� �����
------------------------------------------
FUNCTION Get_token_clob(
        p_list   CLOB,
        p_index  NUMBER,
        p_delim  VARCHAR2 DEFAULT ','
    ) RETURN VARCHAR2
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Get_token_clob';
    v_start_pos NUMBER;
    v_end_pos   NUMBER;
BEGIN
    IF p_index = 1 THEN
        v_start_pos := 1;
    ELSE
        v_start_pos := INSTR(p_list, p_delim, 1, p_index - 1);
        IF v_start_pos > 0 THEN
            v_start_pos := v_start_pos + LENGTH(p_delim);            
        ELSE
            RETURN NULL;
        END IF;
    END IF;
    v_end_pos := INSTR(p_list, p_delim, v_start_pos, 1);
    IF v_end_pos > 0 THEN
        RETURN SUBSTR(p_list, v_start_pos, v_end_pos - v_start_pos);
    ELSE
        RETURN SUBSTR(p_list, v_start_pos);        
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR.', c_PkgName||'.'||v_prcName );
END;



------------------------------------
-- ������� CLOB �� ������ ��������
------------------------------------
PROCEDURE Split_CLOB (
        p_table  OUT vc100_table_t,
        p_list   CLOB,
        p_delim  VARCHAR2 DEFAULT ','
    )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Split_CLOB';
    v_token     VARCHAR2(40);
    i           PLS_INTEGER := 1;
BEGIN
    p_table := vc100_table_t();
    IF p_list IS NOT NULL THEN
        LOOP
            v_token := Get_token_clob( p_list, i , p_delim) ;
            EXIT WHEN v_token IS NULL ;
            p_table.EXTEND;
            p_table(i) := v_token;
            i := i + 1 ;
        END LOOP ;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_list='||p_list, c_PkgName||'.'||v_prcName );
END;


-------------------------------------------------------------
-- ��������� �������� ������ �� ������� ������� (Lotus Notes)
-------------------------------------------------------------
PROCEDURE IMPORT_MMTS_ORDERS
  (
    p_order_no       IN VARCHAR2,
    p_order_phones   IN VARCHAR2,
    p_service_name   IN VARCHAR2,
    p_order_date     IN DATE,
    p_time_zone      IN NUMBER,
    p_agreement_no   IN VARCHAR2,
    p_agreement_date IN DATE,
    p_agreement_end_date IN DATE DEFAULT NULL,
    p_currency       IN INTEGER DEFAULT 810,
    p_jur_zip    IN VARCHAR2, p_jur_state    IN VARCHAR2, p_jur_city    IN VARCHAR2, p_jur_address    IN VARCHAR2,
    p_dlv_zip    IN VARCHAR2, p_dlv_state    IN VARCHAR2, p_dlv_city    IN VARCHAR2, p_dlv_address    IN VARCHAR2,
    p_grp_zip    IN VARCHAR2, p_grp_state    IN VARCHAR2, p_grp_city    IN VARCHAR2, p_grp_address    IN VARCHAR2,
    p_inn            IN VARCHAR2,
    p_kpp            IN VARCHAR2,
    p_vat            IN NUMBER,
    p_customer_name  IN VARCHAR2,
    p_customer_short_name                   IN VARCHAR2,
    p_erp_code                              IN VARCHAR2,
    p_contract_manager                      IN VARCHAR2,
    p_contact_person                        IN VARCHAR2,
    p_contact_phone                         IN VARCHAR2,
    p_contact_fax                           IN VARCHAR2,
    p_contact_email                         IN VARCHAR2,
    p_contractor                            IN VARCHAR2,
    p_min_sum                               IN NUMBER,
    p_abon                                  IN NUMBER,
    p_speed                                 IN NUMBER,
    p_speed_unit                            IN VARCHAR2,
    p_point_1                               IN VARCHAR2,
    p_point_2                               IN VARCHAR2,
    p_point_1_address                       IN VARCHAR2,
    p_point_2_address                       IN VARCHAR2,
    p_ont_bill_summ                         IN NUMBER,
    p_ont_bill_date                         IN DATE DEFAULT NULL,
    p_account_type                          IN VARCHAR2 DEFAULT 'J',
    p_agent_account_no                      IN VARCHAR2 DEFAULT NULL,
    p_m_z_price                             IN NUMBER, --ABC
    p_M_PREPAYED                            IN NUMBER,
    p_OM_VERSION                            IN INTEGER DEFAULT 1,
    p_z_price_def                           IN NUMBER, --DEF
    p_tariff_cur                            IN NUMBER,
    p_brand                                 IN VARCHAR2,
    p_pay_currency                          IN INTEGER,
    p_bill_currency                         IN INTEGER,
    p_tariff_type                           IN VARCHAR2,
    p_order_code                            IN VARCHAR2,
    p_old_order_no                          IN VARCHAR2,
    p_old_order_code                        IN VARCHAR2,
    p_order_level                           IN INTEGER,
    p_order_type                            IN VARCHAR2,
    p_vpn_zone                              IN INTEGER,
    p_market_segment                        IN VARCHAR2,
    p_client_type                           IN VARCHAR2,
    p_agreement_code                        IN VARCHAR2,
    p_network_id                            IN INTEGER,
    p_switch_id                             IN INTEGER,    
    p_result                                OUT INTEGER
  )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'IMPORT_MMTS_ORDERS';
    v_retcode       INTEGER;
    v_err_phone     INTEGER;
    v_err_order     INTEGER;
    v_err_order_imp INTEGER;
    v_tmp_clob CLOB;
    v_exists_6_5    INTEGER;
    v_exists_BRM    INTEGER;
    v_contract_no   VARCHAR2(50);
BEGIN
 
  -- ��������� �� ����������� � ������
  -- � ��������
  SELECT count(ORDER_ID) INTO v_err_order FROM ORDER_T WHERE ORDER_NO = p_order_no AND DATE_TO > SYSDATE;
  -- � ������� �������
  SELECT count(ORDER_NO) INTO v_err_order_imp FROM EXT01_LOTUS_MGMN WHERE ORDER_NO = p_order_no AND status ='NEW';
  -- ��������� ���� �� ������� � ������ �������� (6.5)
  select count(AUTO_NO) INTO v_exists_6_5 from contract_info_t@PINDB.WORLD WHERE AUTO_NO = p_agreement_no
  AND AUTO_NO <> 'KB000040' AND CLOSE_DATE is NULL-- �� ����� ������ ���������� �����������
  ;
  
  -- ������� ��� ������ "�������" ��������
  if p_agreement_no = '28/13/KZ141204' then
    v_contract_no := 'KZ141204';
  else
    v_contract_no := p_agreement_no;
  end if;
  
  
  -- ��������� ���� �� ������� � BRM
  select count(contract_no) INTO v_exists_BRM from contract_t where contract_no = p_agreement_no;
  
  if v_err_phone > 0 then
     p_result := 1;---1;
     --p_result_str := 'CROSSED PHONE NUMBERS!';  
  elsif v_err_order > 0 then
     p_result := 1;----2;
     --p_result_str := 'ORDER ALREADY EXISTS IN BILLING!';
  /*-----------��������, ���� �� ������� ������---------------*/
  elsif v_exists_BRM = 0 AND v_exists_6_5 > 0 and 
                             (p_service_name = 'NPL' OR p_service_name like '%Last mile%' OR p_service_name = 'EPL'
                             OR p_service_name = 'IPL' OR p_service_name like '%���%') THEN
  --elsif v_exists_6_5 > 0 and p_OM_VERSION = 2  and p_service_name <> 'Free Phone'  and p_service_name <> '��/�� �����' then
    p_result := 1; -- 3
    --p_result_str := 'AGREEMENT EXISTS IN PORTAL 6.5!';
  else
    if v_err_order_imp > 0 then --<--'ORDER ALREADY EXISTS IN IMPORT TABLE!'
        DELETE FROM
        EXT01_LOTUS_MGMN 
        WHERE ORDER_NO = p_order_no;
    end if;
        v_tmp_clob := TO_CLOB(p_order_phones);
        INSERT INTO EXT01_LOTUS_MGMN(ID,
                                ORDER_NO,
                                ORDER_PHONES,
                                SERVICE_NAME,
                                ORDER_DATE,
                                TIME_ZONE,
                                AGREEMENT_NO,
                                AGREEMENT_DATE,
                                AGREEMENT_END_DATE,
                                CURRENCY,
                                JUR_ZIP, JUR_STATE, JUR_CITY, JUR_ADDRESS,
                                DLV_ZIP, DLV_STATE, DLV_CITY, DLV_ADDRESS,
                                GRP_ZIP, GRP_STATE, GRP_CITY, GRP_ADDRESS,
                                INN,
                                KPP,
                                VAT,
                                STATUS,
                                STATUS_INFO,
                                STATUS_DATE,
                                CUSTOMER_NAME,
                                CUSTOMER_SHORT_NAME,
                                ERP_CODE,
                                CONTRACT_MANAGER,
                                CONTACT_PERSON, CONTACT_PHONE, CONTACT_FAX, CONTACT_EMAIL,
                                CONTRACTOR, MIN_SUM, ABON, SPEED, SPEED_UNIT, POINT_1, POINT_2, 
                                POINT_1_ADDRESS, POINT_2_ADDRESS, ONT_BILL_SUMM, ONT_BILL_DATE,
                                ACCOUNT_TYPE, AGENT_ACCOUNT_NO, M_Z_PRICE, M_PREPAYED, OM_VERSION,
                                Z_PRICE_DEF, TARIFF_CUR, BRAND, PAY_CURRENCY, BILL_CURRENCY, TARIFF_TYPE,
                                ORDER_CODE, OLD_ORDER_NO, OLD_ORDER_CODE, ORDER_LEVEL, ORDER_TYPE, VPN_ZONE,
                                MARKET_SEGMENT, CLIENT_TYPE, AGREEMENT_CODE, NETWORK_ID, SWITCH_ID
                                )
                         VALUES(Next_import_id,
                                p_order_no,
                                CLOB_TO_BLOB(v_tmp_clob),
                                p_service_name,
                                p_order_date,
                                p_time_zone,
                                v_contract_no, --p_agreement_no,
                                p_agreement_date,
                                p_agreement_end_date,
                                p_currency,
                                p_jur_zip, p_jur_state, p_jur_city, p_jur_address,
                                p_dlv_zip, p_dlv_state, p_dlv_city, p_dlv_address,
                                p_grp_zip, p_grp_state, p_grp_city, p_grp_address,
                                p_inn,
                                p_kpp,
                                p_vat,
                                'NEW',
                                'NEW',
                                SYSDATE,
                                p_customer_name,
                                p_customer_short_name,
                                p_erp_code,
                                p_contract_manager,
                                p_contact_person, p_contact_phone, p_contact_fax, p_contact_email,
                                p_contractor, p_min_sum, p_abon, p_speed, p_speed_unit, p_point_1, p_point_2, 
                                p_point_1_address, p_point_2_address, p_ont_bill_summ, p_ont_bill_date,
                                p_account_type, p_agent_account_no, p_m_z_price, p_M_PREPAYED, p_OM_VERSION,
                                p_z_price_def, p_tariff_cur, p_brand, p_pay_currency, p_bill_currency, p_tariff_type,
                                p_order_code, p_old_order_no, p_old_order_code, p_order_level, p_order_type, p_vpn_zone,
                                p_market_segment, p_client_type, p_agreement_code, p_network_id, p_switch_id
                         );
      p_result := 1;
      --p_result_str := 'OK';    
  end if;
  
   
  EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName||'-'||p_order_no);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
        p_result := 1;
       -- p_result_str := 'ERROR';
END;

-----------------------------------------------
-- ������� ������ ����� ������
-----------------------------------------------
PROCEDURE GetImportData(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetImportData';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
          select  ID, ORDER_NO,
          BLOB_TO_CLOB(order_phones) as order_phones, 
          SERVICE_NAME,ORDER_DATE,TIME_ZONE,AGREEMENT_NO,
          AGREEMENT_DATE, AGREEMENT_END_DATE,CURRENCY,JUR_ADDRESS,DLV_ADDRESS,INN,KPP, VAT,
          STATUS,STATUS_INFO,STATUS_DATE, CUSTOMER_NAME, CUSTOMER_SHORT_NAME, ERP_CODE, CONTRACT_MANAGER
          from EXT01_LOTUS_MGMN
          where STATUS = 'NEW'
          -----------------------------------------------------------------
          --and AGREEMENT_NO <> 'MS107643/�CCA/353P11' -- ��������� ���!!!!
          -----------------------------------------------------------------
          order by STATUS_DATE;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-----------------------------------------------------------------
-- ������� ������ ����� ������ (�������� ���������������� �������)
-----------------------------------------------------------------
PROCEDURE GetImportData_secure(p_recordset OUT t_refc, p_contractor_list IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetImportData';
    v_retcode       INTEGER;   
    v_sql           VARCHAR2(4000);
BEGIN
  
 v_sql := 'select  ID, ORDER_NO, (SELECT contractor FROM contractor_t WHERE contractor_id = (SELECT contractor_id FROM EXT01_LOTUS_REGIONS WHERE lotus_region = BRAND)) BRAND,
          PK700_IMPORT_LOTUS.BLOB_TO_CLOB(order_phones) as order_phones, 
          SERVICE_NAME,ORDER_DATE,TIME_ZONE,AGREEMENT_NO,
          AGREEMENT_DATE, AGREEMENT_END_DATE,CURRENCY,JUR_ADDRESS,DLV_ADDRESS,INN,KPP, VAT,
          STATUS,STATUS_INFO,STATUS_DATE, CUSTOMER_NAME, CUSTOMER_SHORT_NAME, ERP_CODE, CONTRACT_MANAGER
          from EXT01_LOTUS_MGMN
          where STATUS = ''NEW''
          and brand in (SELECT LOTUS_REGION FROM EXT01_LOTUS_REGIONS WHERE CONTRACTOR_ID IN (' || p_contractor_list || '))
          order by STATUS_DATE' ; 
    OPEN p_recordset FOR v_sql;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-----------------------------------------------
-- ������� ������ �� ID
-----------------------------------------------
PROCEDURE GetImportDataById(p_recordset OUT t_refc, p_ID IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetImportData';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
          select  ID, ORDER_NO,BLOB_TO_CLOB(order_phones) as order_phones, 
          SERVICE_NAME,ORDER_DATE,TIME_ZONE,AGREEMENT_NO,
          AGREEMENT_DATE, AGREEMENT_END_DATE,CURRENCY,
          CASE WHEN JUR_ZIP = ' , ' THEN '' ELSE JUR_ZIP END AS JUR_ZIP, 
          CASE WHEN JUR_STATE = ' , ' THEN '' ELSE JUR_STATE END AS JUR_STATE, 
          CASE WHEN JUR_CITY = ' , ' THEN '' ELSE JUR_CITY END AS JUR_CITY, 
          CASE WHEN JUR_ADDRESS = ' , ' THEN '' ELSE JUR_ADDRESS END AS JUR_ADDRESS,
          CASE WHEN DLV_ZIP = ' , ' THEN '' ELSE DLV_ZIP END AS DLV_ZIP, 
          CASE WHEN DLV_STATE = ' , ' THEN '' ELSE DLV_STATE END AS DLV_STATE, 
          CASE WHEN DLV_CITY = ' , ' THEN '' ELSE DLV_CITY END AS DLV_CITY, 
          CASE WHEN DLV_ADDRESS = ' , ' THEN '' ELSE DLV_ADDRESS END AS DLV_ADDRESS, 
          CASE WHEN GRP_ZIP = ' , ' THEN '' ELSE GRP_ZIP END AS GRP_ZIP, 
          CASE WHEN GRP_STATE = ' , ' THEN '' ELSE GRP_STATE END AS GRP_STATE, 
          CASE WHEN GRP_CITY = ' , ' THEN '' ELSE GRP_CITY END AS GRP_CITY, 
          CASE WHEN GRP_ADDRESS = ' , ' THEN '' ELSE GRP_ADDRESS END AS GRP_ADDRESS,
          INN,KPP, VAT,
          STATUS,STATUS_INFO,STATUS_DATE, CUSTOMER_NAME, CUSTOMER_SHORT_NAME, ERP_CODE, CONTRACT_MANAGER,
          CONTACT_PERSON, CONTACT_PHONE, CONTACT_FAX, CONTACT_EMAIL, CONTRACTOR, MIN_SUM, ABON,
          SPEED, SPEED_UNIT, POINT_1, POINT_2, 
          POINT_1_ADDRESS, 
          POINT_2_ADDRESS, 
          ONT_BILL_SUMM, ONT_BILL_DATE, decode(RATE_PLAN_ID, null, 0,RATE_PLAN_ID) RATE_PLAN_ID,
          ACCOUNT_TYPE, AGENT_ACCOUNT_NO, M_Z_PRICE, M_PREPAYED, AG_RATE_PLAN_ID, Z_PRICE_DEF, TARIFF_CUR,
          (SELECT CONTRACTOR_ID FROM EXT01_LOTUS_REGIONS where lotus_region = BRAND) CONTRACTOR_ID,
          (SELECT CONTRACTOR_ID FROM CONTRACTOR_T WHERE XTTK_ID = (SELECT CONTRACTOR_ID FROM EXT01_LOTUS_REGIONS where lotus_region = BRAND) AND CONTRACTOR_TYPE = 'SELLER') MKR_REG_ID,
          PAY_CURRENCY, BILL_CURRENCY, OM_VERSION, TARIFF_TYPE, VPN_ZONE, MARKET_SEGMENT, CLIENT_TYPE, AGREEMENT_CODE, NETWORK_ID, SWITCH_ID
          from EXT01_LOTUS_MGMN
          where ID = p_ID;
EXCEPTION 
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

------------------------------------------------------------------------------------
-- ����� ���������� � �������� � ������� � �������� �� ������ ��������
------------------------------------------------------------------------------------
PROCEDURE GetContractByNumber(p_recordset OUT t_refc, p_contract_no IN varchar2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetContractByNumber';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      select ct.*, CL.CLIENT_NAME 
      from CONTRACT_T ct, CLIENT_T cl
      where contract_no = p_contract_no
      and CL.CLIENT_ID = CT.CLIENT_ID;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------------------------------------------------------------------------------------
-- ����� ���������� � �������� � ������� � �������� �� ������ ��������
------------------------------------------------------------------------------------
PROCEDURE GetContractByNumber(p_recordset OUT t_refc, p_contract_no IN varchar2, p_erp_code IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetContractByNumber';
    v_retcode       INTEGER;   
BEGIN
  OPEN p_recordset FOR
      select ct.*, CL.CLIENT_NAME 
      from CONTRACT_T ct, CLIENT_T cl
      where contract_no = p_contract_no
      and CL.CLIENT_ID = CT.CLIENT_ID;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


------------------------------------------------------
-- ����� ������� ������ � �������� �� ������ ��������
------------------------------------------------------
PROCEDURE GetAccountsByContractNo(p_recordset OUT t_refc, p_contract_no IN varchar2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetAccountsByContractNo';
    v_retcode       INTEGER;   
    v_account_count INTEGER;
BEGIN
      select count(a.account_id) into v_account_count
        from ACCOUNT_T a, ACCOUNT_PROFILE_T ap, CONTRACT_T c
        where A.ACCOUNT_ID = AP.ACCOUNT_ID
        and C.CONTRACT_ID = AP.CONTRACT_ID
        and C.CONTRACT_NO = p_contract_no
        AND ap.actual = 'Y'
        and (a.status = PK00_CONST.c_ACC_STATUS_BILL  OR a.status = PK00_CONST.c_ACC_STATUS_TEST OR a.status = 'A');
      
    
         IF v_account_count > 1 THEN
           OPEN p_recordset FOR
            --select -1 account_id, '... ������� �� ������' account_no from dual
            --union
            select a.account_id, 
                   case
                   when A.COMMENTARY is not null then a.account_no || ' (' || A.COMMENTARY || ')' 
                   else a.account_no
                   end as account_no                
            from ACCOUNT_T a, ACCOUNT_PROFILE_T ap, CONTRACT_T c
            where A.ACCOUNT_ID = AP.ACCOUNT_ID
            and C.CONTRACT_ID = AP.CONTRACT_ID
            and C.CONTRACT_NO = p_contract_no
            AND ap.actual = 'Y'
            and (a.status = PK00_CONST.c_ACC_STATUS_BILL  OR a.status = PK00_CONST.c_ACC_STATUS_TEST OR a.status = 'A')
            order by account_no;           
         ELSE
           OPEN p_recordset FOR
          select a.account_id, 
                 case
                 when A.COMMENTARY is not null then a.account_no || ' (' || A.COMMENTARY || ')' 
                 else a.account_no
                 end as account_no          
--          a.account_no
          from ACCOUNT_T a, ACCOUNT_PROFILE_T ap, CONTRACT_T c
          where A.ACCOUNT_ID = AP.ACCOUNT_ID
          and C.CONTRACT_ID = AP.CONTRACT_ID
          and C.CONTRACT_NO = p_contract_no
          AND ap.actual = 'Y'
          and (a.status = PK00_CONST.c_ACC_STATUS_BILL  OR a.status = PK00_CONST.c_ACC_STATUS_TEST OR a.status = 'A')
          order by account_no;           
         END IF;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------------------------------------------------------
-- ����� ������� ������ � �������� �� ������ ��������
------------------------------------------------------
PROCEDURE GetAccountsByContractNo(p_recordset OUT t_refc, p_contract_no IN varchar2, p_erp_code IN varchar2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetAccountsByContractNo';
    v_retcode       INTEGER;   
    v_account_count INTEGER;
BEGIN
      SELECT COUNT(a.account_id)
        INTO v_account_count
        FROM ACCOUNT_T a,
             ACCOUNT_PROFILE_T ap,
             CONTRACT_T c,
             COMPANY_T CM
       WHERE A.ACCOUNT_ID = AP.ACCOUNT_ID
             AND C.CONTRACT_ID = AP.CONTRACT_ID
             AND C.CONTRACT_NO = p_contract_no
             AND C.CONTRACT_ID = CM.CONTRACT_ID
             AND CM.ERP_CODE = p_erp_code
             AND AP.ACTUAL = 'Y'
             AND (a.status = 'B' OR a.status = 'T' OR a.status = 'A');
        
    
         IF v_account_count > 1 THEN
           OPEN p_recordset FOR
            --select -1 account_id, '... ������� �� ������' account_no from dual
            --union
            select a.account_id, 
                   case
                   when A.COMMENTARY is not null then a.account_no || ' (' || A.COMMENTARY || ')' 
                   else a.account_no
                   end as account_no                
            from ACCOUNT_T a, ACCOUNT_PROFILE_T ap, CONTRACT_T c
            where A.ACCOUNT_ID = AP.ACCOUNT_ID
            and C.CONTRACT_ID = AP.CONTRACT_ID
            and C.CONTRACT_NO = p_contract_no
            AND AP.ACTUAL = 'Y'
            and (a.status = PK00_CONST.c_ACC_STATUS_BILL  OR a.status = PK00_CONST.c_ACC_STATUS_TEST OR a.status = 'A')
            order by account_no;           
         ELSE
           OPEN p_recordset FOR
          select a.account_id, 
                 case
                 when A.COMMENTARY is not null then a.account_no || ' (' || A.COMMENTARY || ')' 
                 else a.account_no
                 end as account_no          
--          a.account_no
          from ACCOUNT_T a, ACCOUNT_PROFILE_T ap, CONTRACT_T c
          where A.ACCOUNT_ID = AP.ACCOUNT_ID
          and C.CONTRACT_ID = AP.CONTRACT_ID
          and C.CONTRACT_NO = p_contract_no
          AND AP.ACTUAL = 'Y'
          and (a.status = PK00_CONST.c_ACC_STATUS_BILL  OR a.status = PK00_CONST.c_ACC_STATUS_TEST OR a.status = 'A')
          order by account_no;           
         END IF;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------------------------------------------
-- �������� ������ �����
------------------------------------------
PROCEDURE GetServices(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetServices';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      select service_id, service service_short
      from SERVICE_T
      order by service;  
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

------------------------------------------
-- �������� ������ ����������� ������
------------------------------------------
PROCEDURE GetSubServices(p_recordset OUT t_refc, p_service_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetSubServices';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      select S.SUBSERVICE_ID, S.SUBSERVICE 
      from SUBSERVICE_T s, SERVICE_SUBSERVICE_T ss
      where S.SUBSERVICE_ID = SS.SUBSERVICE_ID
      and SS.SERVICE_ID = p_service_id
      ORDER BY SUBSERVICE;  
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


---------------------------------------
-- �������� ������ �� �� ��������
---------------------------------------
PROCEDURE GetRatePlanByPrefix(p_recordset OUT t_refc, p_prefix IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetRatePlanByPrefix';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      select * 
      from RATEPLAN_T
      where lower(rateplan_name) LIKE lower('%' || p_prefix || '%')
      order by rateplan_name;  
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------------------------------------------------------------------------------------------------------
--- �� �������� + service
------------------------------------------------------------------------------------------------------
PROCEDURE GetRatePlanByPrefix(p_recordset OUT t_refc, p_prefix IN VARCHAR2, p_service_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetRatePlanByPrefix';
    v_retcode       INTEGER;   
BEGIN
    
         IF p_service_id = 1 OR p_service_id = 2  OR p_service_id = 125  OR p_service_id = 140 THEN
              -- ��������� ������
            OPEN p_recordset FOR
              select 
               rp.*
              from 
                 pin.rateplan_t           rp,
                 TARIFF_PH.D41_TRF_HEADER th,
                 TARIFF_PH.D21_ZONE_MODEL zm
              where
                 lower(rateplan_name) LIKE lower('%' || p_prefix || '%')
                 AND RP.RATEPLAN_ID = th.RATEPLAN_ID
                 and
                 TH.ZMDL_ID = ZM.ZMDL_ID
                 and
                 decode(p_service_id,
                      1,1,  -- ���� 
                      125,5, -- �������
                      140,6, -- �������������
                      2,8 -- 8800
                      ) = 
                 decode(ZM.DIST_REGN,
                      1,1,
                      2,1,
                      4,8,
                      8,6,
                      16,5
                      );
          ELSE
              -- ��������� ������
           OPEN p_recordset FOR
              select * 
              from RATEPLAN_T
              where lower(rateplan_name) LIKE lower('%' || p_prefix || '%')
              order by rateplan_name;  
          END IF;
        
        
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------------------------------------------------
-- �������� ��� ��������� ����� �� ID
------------------------------------------------
FUNCTION GetRatePlanNameById(p_rateplan_id IN INTEGER) RETURN VARCHAR2
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'GetRatePlanNameById';
    v_retcode    INTEGER := c_RET_OK;
    v_rateplan_name VARCHAR2(1000);
BEGIN
    select RATEPLAN_NAME into v_rateplan_name from RATEPLAN_T 
    WHERE RATEPLAN_ID = p_rateplan_id;
    RETURN v_rateplan_name;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
        RETURN '';
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-----------------------------------------------------
-- �������� ������ �������� (��� BCR) �� ��������
-----------------------------------------------------
PROCEDURE GetClientsByPrefix(p_recordset OUT t_refc, p_prefix IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetClientsByPrefix';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      select client_id, client_name 
      from CLIENT_T
      where lower(client_name) LIKE lower('%' || p_prefix || '%')
      order by client_name;  
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-----------------------------------------------------
-- �������� ������ ���� �������� (��� BCR)
-----------------------------------------------------
PROCEDURE GetAllClients(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetAllClients';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      select *
      from CLIENT_T
      order by client_name;  
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-----------------------------------------------------
-- �������� ������ �� ���  �� ��������
-----------------------------------------------------
PROCEDURE GetCustomersByPrefix(p_recordset OUT t_refc, p_prefix IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetCustomersByPrefix';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      select company_id, company_name 
      from company_t
      where lower(company_name) LIKE lower('%' || p_prefix || '%')
      and erp_code is not null
      order by company_name;  
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-----------------------------------------------------
-- �������� ������ ���������� �� ��������
-----------------------------------------------------
PROCEDURE GetManagersByPrefix(p_recordset OUT t_refc, p_prefix IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetManagersByPrefix';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      select M.MANAGER_ID, C.SHORT_NAME as CONTRACTOR_NAME, m.LAST_NAME, m.FIRST_NAME, m.MIDDLE_NAME,
      m.LAST_NAME || ' ' || m.FIRST_NAME || ' ' || m.MIDDLE_NAME as FIO
      from MANAGER_T m, contractor_t c
      where M.CONTRACTOR_ID = C.CONTRACTOR_ID
      and m.date_to is null
      and lower(m.LAST_NAME || ' ' || m.FIRST_NAME || ' ' || m.MIDDLE_NAME) like lower('%' || p_prefix || '%')
      order by m.LAST_NAME || ' ' || m.FIRST_NAME || ' ' || m.MIDDLE_NAME;  
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
---------------------------------------------------------
-- �������� ������ ���������� �� �������� � Contractor'�
--------------------------------------------------------
PROCEDURE GetManagersByPrefixC(p_recordset OUT t_refc, p_prefix IN VARCHAR2, p_contractor_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetManagersByPrefixC';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      /*select M.MANAGER_ID, C.SHORT_NAME as CONTRACTOR_NAME, m.LAST_NAME, m.FIRST_NAME, m.MIDDLE_NAME,
      m.LAST_NAME || ' ' || m.FIRST_NAME || ' ' || m.MIDDLE_NAME as FIO
      from MANAGER_T m, contractor_t c
      where M.CONTRACTOR_ID = C.CONTRACTOR_ID
      and C.CONTRACTOR_ID = p_contractor_id
      and m.date_to is null
      and lower(m.LAST_NAME || ' ' || m.FIRST_NAME || ' ' || m.MIDDLE_NAME || ' ' || C.SHORT_NAME) like lower('%' || p_prefix || '%')
      order by m.LAST_NAME || ' ' || m.FIRST_NAME || ' ' || m.MIDDLE_NAME; */
      select M.MANAGER_ID, C.SHORT_NAME as CONTRACTOR_NAME, m.LAST_NAME, m.FIRST_NAME, m.MIDDLE_NAME,
      m.LAST_NAME || ' ' || m.FIRST_NAME || ' ' || m.MIDDLE_NAME as FIO
      from MANAGER_T m, contractor_t c
      where M.CONTRACTOR_ID = C.CONTRACTOR_ID
      and m.date_to is null
      and lower(m.LAST_NAME || ' ' || m.FIRST_NAME || ' ' || m.MIDDLE_NAME) like lower('%' || p_prefix || '%')
      order by m.LAST_NAME || ' ' || m.FIRST_NAME || ' ' || m.MIDDLE_NAME;   
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
-----------------------------------------------------
-- �������� ������ �����������-��������� (SALES + KTTK)
-----------------------------------------------------
PROCEDURE GetContractorsSales(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetContractorsSales';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      select -1  contractor_id, -1  parent_id, '' contractor_type, '' erp_code, '' inn, '' kpp, '...' contractor,
      '' short_name, -1 external_id, -1 region_id, '' notes, -1 xttk_id, -1 seller_id, '' agent_contract_no from dual
      union
      select 
         CONTRACTOR_ID, PARENT_ID, CONTRACTOR_TYPE, 
         ERP_CODE, INN, KPP, 
         CONTRACTOR, SHORT_NAME, EXTERNAL_ID, 
         REGION_ID, NOTES, XTTK_ID, 
         SELLER_ID, AGENT_CONTRACT_NO
      from contractor_t
      where contractor_type = 'KTTK' or contractor_type = 'SELLER'
      --and contractor_id <> 1524383 -- ��������� ���-������ (�.������� ��������� 03.02.2016)
      order by contractor;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
-----------------------------------------------------
-- �������� ������ ����������� �� ��������
-----------------------------------------------------
PROCEDURE GetContractorsByPrefix(p_recordset OUT t_refc, p_prefix IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetContractorsByPrefix';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
         select * from contractor_t
         where lower(contractor) like lower('%' || p_prefix || '%')
         order by contractor;
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

------------------------------------------------------
-- ����� ������� (customer) �� ���/���
-- ���� �� ������, ������� ���������!
------------------------------------------------------
PROCEDURE GetCustomerIDByInnKpp(
          p_customer_id    OUT INTEGER, 
          p_erp_code       IN VARCHAR2,
          p_inn            IN VARCHAR2,
          p_kpp            IN VARCHAR2, 
          p_name           IN VARCHAR2,
          p_short_name     IN VARCHAR2,
          p_error          OUT INTEGER,
          p_short_name_out          OUT VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetCustomerIDByInnKpp';
    v_retcode       INTEGER;   
    v_count         INTEGER;
    v_tmp_inn       VARCHAR2(10);
    v_tmp_kpp       VARCHAR2(10);    
BEGIN
    p_error := 0;
    
    IF p_kpp is null AND p_inn is null THEN
       SELECT COUNT(CUSTOMER_ID) INTO v_count from CUSTOMER_T
       where erp_code = p_erp_code; 
    ELSIF p_kpp is null THEN
        SELECT COUNT(CUSTOMER_ID) INTO v_count from CUSTOMER_T where inn = p_inn and kpp is null;  
    ELSE
        SELECT COUNT(CUSTOMER_ID) INTO v_count from CUSTOMER_T where inn = p_inn and kpp = p_kpp;
    END IF;
    
    -- ���� �� �����, �� ������� � ���������� ��� ID
    IF v_count = 0 THEN  
       p_customer_id := PK13_CUSTOMER.New_customer(p_erp_code, p_inn, p_kpp, p_name, p_short_name, 'Import from LOTUS');
       
       IF p_kpp is null AND p_inn is null THEN
              p_short_name_out := p_short_name;
       ELSIF p_kpp is null THEN
                select SHORT_NAME into p_short_name_out  from CUSTOMER_T
                 where inn = p_inn and kpp is null;              
       ELSE
                select SHORT_NAME into p_short_name_out  from CUSTOMER_T
                 where inn = p_inn and kpp = p_kpp;
       END IF;
       
    -- ���� ����� ������, ���������� ��� ID
    ELSIF v_count = 1 THEN
          IF p_inn is null AND p_kpp is null THEN
             select CUSTOMER_ID, SHORT_NAME 
             into p_customer_id, p_short_name_out  
             from CUSTOMER_T
             where customer = p_name;
          ELSIF p_kpp is null THEN
             select CUSTOMER_ID, SHORT_NAME 
             into p_customer_id, p_short_name_out  
             from CUSTOMER_T
             where inn = p_inn and kpp is null;            
          ELSE
             select CUSTOMER_ID, SHORT_NAME 
             into p_customer_id, p_short_name_out  
             from CUSTOMER_T
             where inn = p_inn and kpp = p_kpp;
          END IF;

    -- ���� ����� >1, ����� ������! �� ��� ����� ������� ! :)
    ELSE
       /*p_error := -1;
       p_customer_id := -1;
       p_short_name_out := '';*/
       p_customer_id := PK13_CUSTOMER.New_customer(p_erp_code, p_inn, p_kpp, p_name, p_short_name, 'Import from LOTUS. More than one!!');
       select SHORT_NAME into p_short_name_out  from CUSTOMER_T where customer_id = p_customer_id;
      
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------------------------------------------------
-- ����� ������� (��� BCR - CLIENT_T)
------------------------------------------------
FUNCTION GetBCRClient(p_client_name  IN VARCHAR2) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'GetBCRClient';
    v_retcode    INTEGER;
    v_client_id  INTEGER;
BEGIN
	SELECT MAX(CLIENT_ID) INTO v_client_id
	FROM CLIENT_T 
	WHERE UPPER(CLIENT_NAME) = UPPER(p_client_name);
    RETURN v_client_id;
EXCEPTION
WHEN NO_DATA_FOUND THEN
        RETURN 0;
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
-------------------------------------------
-- ���������� ������ �������
-------------------------------------------
PROCEDURE GetBrands(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetBrands';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
         select * from BRAND_T order by BRAND;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

------------------------------------------------
-- �������� ������ ��������� �����
------------------------------------------------
PROCEDURE GetMarketSegments(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetMarketSegments';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
         select * from DICTIONARY_T WHERE PARENT_ID = PK00_CONST.c_DICT_MARKET_SEGMENT order by NAME;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

------------------------------------------------
-- �������� ������ ����� ��������
------------------------------------------------
PROCEDURE GetClientTypes(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetClientTypes';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
         select * from DICTIONARY_T WHERE PARENT_ID = PK00_CONST.c_DICT_CLIENT_TYPE order by NAME;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

------------------------------------------------
-- �������� ������ �������� ��������
------------------------------------------------
PROCEDURE GetDeliveryMethods(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetDeliveryMethods';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
         select * from DICTIONARY_T WHERE PARENT_ID = PK00_CONST.c_DICT_DELIVERY_METHOD order by NAME;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

------------------------------------------------
-- �������� ID KTTK
------------------------------------------------
FUNCTION GetKTTKId RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'GetKTTKId';
    v_retcode    INTEGER := c_RET_OK;
BEGIN
	SELECT CONTRACTOR_ID INTO v_retcode
	FROM CONTRACTOR_T 
	WHERE CONTRACTOR_TYPE = 'KTTK';

    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

--------------------------------------------------
-- �������� ������ �����������-�������
--------------------------------------------------
PROCEDURE GetContractorsBrands(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetContractorsBrands';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      select CONTRACTOR_ID, CONTRACTOR, SHORT_NAME, BRAND_ID from
      (SELECT CONTRACTOR_ID, CONTRACTOR as SHORT_NAME, SHORT_NAME  as CONTRACTOR, 0 as BRAND_ID
       FROM CONTRACTOR_T WHERE CONTRACTOR = '�-���'  and contractor_type = 'XTTK')
      union
      (select  C.CONTRACTOR_ID, C.CONTRACTOR as SHORT_NAME, C.SHORT_NAME  as CONTRACTOR,  B.BRAND_ID
      from CONTRACTOR_BRAND_T cb, CONTRACTOR_T c, brand_t b
      where B.BRAND_ID = CB.BRAND_ID
      and CB.CONTRACTOR_ID = C.CONTRACTOR_ID
      and c.contractor_type = 'XTTK')
      order by SHORT_NAME;            
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

--------------------------------------------------
-- �������� ������ �����������-���� �� �������� XTTK
--------------------------------------------------
PROCEDURE GetContractorsXTTK(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetContractorsXTTK';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      select * from CONTRACTOR_T WHERE contractor_type = 'XTTK' 
      order by SHORT_NAME;            
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

--------------------------------------------------
-- �������� ������ ������� �� ID ����������
--------------------------------------------------
PROCEDURE GetAgentsByContractorID(p_recordset OUT t_refc, p_contractor_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetAgentsByContractorID';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      SELECT CONTRACTOR_ID, CONTRACTOR, SHORT_NAME
      FROM  CONTRACTOR_T
      WHERE CONTRACTOR_TYPE = 'AGENT'
      AND PARENT_ID = p_contractor_id
      ORDER BY CONTRACTOR;
      
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

--------------------------------------------------
-- �������� ������ ������ ����������
--------------------------------------------------
PROCEDURE GetBanksByContractorID(p_recordset OUT t_refc, p_contractor_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetBanksByContractorID';
    v_retcode       INTEGER;   
BEGIN

    OPEN p_recordset FOR
      select * from CONTRACTOR_BANK_T
      WHERE CONTRACTOR_ID = p_contractor_id
      AND date_to is null
      AND BANK_ID <> 3 -- ������� ���
      AND PARENT_ID is null -- ����������� ����� �� �����
      order by notes  ;    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
--------------------------------------------------
-- �������� ������ ���������� ������
--------------------------------------------------
PROCEDURE GetBanks2ByContractorID(p_recordset OUT t_refc, p_bank_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetBanksByContractorID';
    v_retcode       INTEGER;   
    v_count_banks   INTEGER;
BEGIN

    select count(*) INTO v_count_banks from CONTRACTOR_BANK_T
                             WHERE PARENT_ID = p_bank_id
                           AND date_to is null;

    if v_count_banks = 0 then
      OPEN p_recordset FOR
            select 
            -1 BANK_ID, 
            '' BANK_NAME, 
            '' BANK_CODE, 
            '' BANK_CORR_ACCOUNT, 
            '' BANK_SETTLEMENT, 
            null DATE_FROM, 
            -1 CONTRACTOR_ID, 
            '' NOTES
      from dual;
    else
      OPEN p_recordset FOR
            select * from CONTRACTOR_BANK_T
                   WHERE PARENT_ID = p_bank_id
                   AND date_to is null
                   order by notes  ; 
    end if;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


PROCEDURE GetBanksByContractorID(p_recordset OUT t_refc, p_contractor_id IN INTEGER, p_billing_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetBanksByContractorID';
    v_retcode       INTEGER;   
BEGIN

    OPEN p_recordset FOR
     SELECT *
         FROM contractor_bank_t
          WHERE  contractor_id = p_contractor_id
          AND date_to IS NULL       
          AND ((p_billing_Id = 2003 AND BANK_ID IN (1, 2)) OR p_billing_id <> 2003)
          AND PARENT_ID is null -- ����������� ����� �� �����
      ;    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
--------------------------------------------------
-- ������ ������ ������
--------------------------------------------------
PROCEDURE ChangeImportStatus(p_import_id IN INTEGER, p_import_status IN VARCHAR2, p_import_status_info IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'ChangeImportStatus';
    v_retcode       INTEGER;   
BEGIN
    UPDATE EXT01_LOTUS_MGMN
    SET STATUS = p_import_status,
        STATUS_INFO = p_import_status_info,
        STATUS_DATE = SYSDATE
    WHERE ID = p_import_id;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-----------------------------------------------------------------------------------------------------
-- ������ ������ ������, ��� �������� � �� � ������ BILLING, ���� � ������� ������� ������ NEW!!!
-- ������ ������, ����� ������ �� ������ � BRM, � ������ ��������� �� � ������� � ��. 
-- ������ �������� � ������� ������� � ��������....
-----------------------------------------------------------------------------------------------------
PROCEDURE RemoveOrdersFromImport(p_order_no IN VARCHAR2, p_result OUT INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'RemoveOrdersFromImport';
    v_retcode       INTEGER;   
    v_order_count   INTEGER;
BEGIN
    v_order_count := 0;
    SELECT COUNT(ORDER_NO) INTO v_order_count FROM EXT01_LOTUS_MGMN
           WHERE ORDER_NO = p_order_no AND STATUS = 'NEW' 
           --and SERVICE_NAME <> '��/�� �����' 
           and SERVICE_NAME <> 'Free Phone'; -- 8800 �������� ��� ���������� �����������!!
                   
    IF v_order_count > 0 THEN
          UPDATE EXT01_LOTUS_MGMN
                 SET STATUS = 'REMOVED',
                 STATUS_INFO = 'REMOVED FROM IMPORT',
                 STATUS_DATE = SYSDATE
             WHERE ORDER_NO = p_order_no;
    END IF;           
   
   /*SELECT login into v_order_count FROM service_t@PINDB.WORLD WHERE login = p_order_no;      
   IF v_order_count > 0 THEN
      UPDATE EXT01_LOTUS_MGMN
         SET STATUS = 'IN_6_5',
         STATUS_INFO = 'REMOVED FROM IMPORT',
         STATUS_DATE = SYSDATE
     WHERE ORDER_NO = p_order_no;              
   END IF;             */
-- ������ �� �����....

    

    p_result := 1;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
        p_result := 1;
END;

-----------------------------------------------------
-- ����� �������� ����� �� ������
-----------------------------------------------------
FUNCTION Find_account_by_no(p_account_no  IN VARCHAR2) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find_account_by_no';
    v_account_id INTEGER;
BEGIN
    SELECT ACCOUNT_ID  INTO v_account_id
      FROM ACCOUNT_T
     WHERE ACCOUNT_NO = p_account_no;
    RETURN v_account_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN -1;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;



--------------------------------------------------
-- ������ ���������� ������� � EXT01_TMP_PHONES
--------------------------------------------------
PROCEDURE Write_TMP_phones(
        p_list   CLOB,
        p_delim  VARCHAR2 DEFAULT ','
    )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Write_TMP_phones';
    v_token     VARCHAR2(40);
    i           PLS_INTEGER := 1;
    v_tmp       INTEGER;
BEGIN
    IF p_list IS NOT NULL THEN
        LOOP
            v_token := Get_token_clob( p_list, i , p_delim) ;
            EXIT WHEN v_token IS NULL ;
            INSERT INTO EXT01_TMP_PHONES(PHONE) VALUES(v_token);
            i := i + 1 ;
        END LOOP ;
    END IF;
    SELECT COUNT(PHONE) into v_tmp FROM EXT01_TMP_PHONES;
    --Pk01_Syslog.Write_msg('Checked phones: ' || v_tmp, c_PkgName||'.'||v_prcName, 'I' );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_list='||p_list, c_PkgName||'.'||v_prcName );
END;


------------------------------------------------
-- ���������� ������������ �-�������
------------------------------------------------
PROCEDURE UpdateParsedPnones(p_import_id IN INTEGER, p_phones IN BLOB)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'UpdateParsedPnones';
    v_retcode       INTEGER;   
BEGIN
    UPDATE EXT01_LOTUS_MGMN
    SET PARSED_ORDER_PHONES = p_phones
    WHERE ID = p_import_id;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

---------------------------------------------------
-- �������� - ������� ������ ��� ���?
---------------------------------------------------
FUNCTION CheckParsedPhones(p_line_id IN INTEGER) RETURN INTEGER
IS
    v_prcName             CONSTANT VARCHAR2(30) := 'CheckParsedPhones';
    v_is_parsed           INTEGER DEFAULT 0;
BEGIN
    SELECT COUNT(ID) into v_is_parsed from EXT01_LOTUS_MGMN where id = p_line_id AND PARSED_ORDER_PHONES is not null;
    RETURN v_is_parsed;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

---------------------------------------------------
-- �������� �� ����������� �-�������
---------------------------------------------------
FUNCTION CheckCrossedPhones(p_line_id IN INTEGER) RETURN INTEGER
IS
    v_prcName             CONSTANT VARCHAR2(30) := 'CheckCrossedPhones';
    v_res                 INTEGER;
    v_tmp_phones          CLOB;
    v_tmp_in_phones       BLOB;
BEGIN
    SELECT PARSED_ORDER_PHONES into v_tmp_in_phones from EXT01_LOTUS_MGMN where id = p_line_id;
    v_tmp_phones := BLOB_TO_CLOB(v_tmp_in_phones);
    
    --��������� �� ����������� ���������� �������
    Write_TMP_phones(v_tmp_phones, ',');
    SELECT COUNT(ORDER_ID) INTO v_res 
    FROM ORDER_PHONES_T PH
    WHERE PHONE_NUMBER IN (SELECT * FROM EXT01_TMP_PHONES) 
    AND DATE_TO > SYSDATE; 
    --WHERE EXISTS
    --(SELECT PHONE FROM EXT01_TMP_PHONES T WHERE PH.PHONE_NUMBER = T.PHONE );
    
    
    --WHERE PHONE_NUMBER IN (SELECT * FROM EXT01_TMP_PHONES) 
    --AND DATE_TO > SYSDATE;        
    
    RETURN v_res;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


--------------------------------------------------
-- �������� ������ ����������� �������
--------------------------------------------------
PROCEDURE GetCrossedPhones(p_recordset OUT t_refc, ImportId IN INTEGER, p_order_date IN DATE DEFAULT SYSDATE)
IS
    v_prcName                          CONSTANT VARCHAR2(30) := 'GetCrossedPhones';
    v_retcode                          INTEGER;   
    v_tmp_phones                       CLOB;
    v_blob_phones                      BLOB;
BEGIN                 
    SELECT PARSED_ORDER_PHONES into v_blob_phones 
           FROM EXT01_LOTUS_MGMN
           WHERE ID = ImportId;
    v_tmp_phones := BLOB_TO_CLOB(v_blob_phones);
    Write_TMP_phones(v_tmp_phones, ',');
    OPEN p_recordset FOR
          SELECT op.PHONE_NUMBER, o.Order_No
          FROM ORDER_PHONES_T op, ORDER_T o
          WHERE op.PHONE_NUMBER IN (SELECT * FROM EXT01_TMP_PHONES) AND op.DATE_TO > p_order_date --SYSDATE
          AND o.order_id = op.order_id;   
          DELETE FROM EXT01_TMP_PHONES;            
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
-----------------------------------------------------------------
-- �������� ������ ����������� �������, �������� SUBSERVICE
-----------------------------------------------------------------
PROCEDURE GetCrossedPhonesSubs(p_recordset OUT t_refc, ImportId IN INTEGER, p_order_date IN DATE DEFAULT SYSDATE, p_subservice_id IN VARCHAR2)
IS
    v_prcName                          CONSTANT VARCHAR2(30) := 'GetCrossedPhones';
    v_retcode                          INTEGER;   
    v_tmp_phones                       CLOB;
    v_blob_phones                      BLOB;
    v_sql                              VARCHAR2(4000);
BEGIN                 
    SELECT PARSED_ORDER_PHONES into v_blob_phones 
           FROM EXT01_LOTUS_MGMN
           WHERE ID = ImportId;
    v_tmp_phones := BLOB_TO_CLOB(v_blob_phones);
    Write_TMP_phones(v_tmp_phones, ',');
    
 v_sql := 'SELECT DISTINCT op.PHONE_NUMBER, o.Order_No
          FROM ORDER_PHONES_T op, ORDER_T o, ORDER_BODY_T ob
          WHERE op.PHONE_NUMBER IN (SELECT * FROM EXT01_TMP_PHONES) AND op.DATE_TO > to_date(''' || p_order_date || ''' , ''dd.mm.yy'') 
          AND o.order_id = op.order_id
          AND ob.order_id = o.order_id
          AND ob.subservice_id in  (' || p_subservice_id || ')
          AND ob.DATE_TO >= SYSDATE'
          ; 
    OPEN p_recordset FOR v_sql;
          /*SELECT op.PHONE_NUMBER, o.Order_No
          FROM ORDER_PHONES_T op, ORDER_T o, ORDER_BODY_T ob
          WHERE op.PHONE_NUMBER IN (SELECT * FROM EXT01_TMP_PHONES) AND op.DATE_TO > p_order_date --SYSDATE
          AND o.order_id = op.order_id
          AND ob.order_id = o.order_id
          AND ob.subservice_id in  p_subservice_id 
          ;   */
          DELETE FROM EXT01_TMP_PHONES;            
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        Pk01_SysLog.Write_msg(v_sql, c_PkgName||'.'||v_prcName, 'E');
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

PROCEDURE GetCrossedPhonesSubs(p_recordset OUT t_refc, 
                               ImportId IN INTEGER, 
                               p_order_date IN DATE DEFAULT SYSDATE, 
                               p_subservice_id IN VARCHAR2,
                               p_phones IN CLOB)
IS
    v_prcName                          CONSTANT VARCHAR2(30) := 'GetCrossedPhones';
    v_retcode                          INTEGER;   
    v_sql                              VARCHAR2(4000);
BEGIN                 
    Write_TMP_phones(p_phones, ',');
    
 v_sql := 'SELECT DISTINCT op.PHONE_NUMBER, o.Order_No
          FROM ORDER_PHONES_T op, ORDER_T o, ORDER_BODY_T ob
          WHERE op.PHONE_NUMBER IN (SELECT * FROM EXT01_TMP_PHONES) AND op.DATE_TO > to_date(''' || p_order_date || ''' , ''dd.mm.yy'') 
          AND o.order_id = op.order_id
          AND ob.order_id = o.order_id
          AND ob.subservice_id in  (' || p_subservice_id || ')
          AND ob.DATE_TO >= SYSDATE'; 
    OPEN p_recordset FOR v_sql;
          DELETE FROM EXT01_TMP_PHONES;            
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        Pk01_SysLog.Write_msg(v_sql, c_PkgName||'.'||v_prcName, 'E');
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-----------------------------------------------------------------
-- �������� ������ ����������� ��������� �������, �������� SUBSERVICE
-----------------------------------------------------------------
PROCEDURE GetCrossedPhonesSubsAG(p_recordset OUT t_refc, 
                               ImportId IN INTEGER, 
                               p_order_date IN DATE DEFAULT SYSDATE, 
                               p_subservice_id IN VARCHAR2,
                               p_phones IN STRING)
IS
    v_prcName                          CONSTANT VARCHAR2(30) := 'GetCrossedPhonesSubsAG';
    v_retcode                          INTEGER;   
    v_sql                              VARCHAR2(4000);
    v_tmp_phones                       CLOB;
    v_blob_phones                      BLOB;    
BEGIN
    DELETE FROM EXT01_TMP_PHONES;
    IF ImportId > -1 THEN
      SELECT PARSED_ORDER_PHONES into v_blob_phones 
        FROM EXT01_LOTUS_MGMN
        WHERE ID = ImportId;
      v_tmp_phones := BLOB_TO_CLOB(v_blob_phones);
      Write_TMP_phones(v_tmp_phones, ',');
      v_sql := 'SELECT DISTINCT op.PHONE_NUMBER, o.Order_No
                FROM AGENT_PHONES_T op, ORDER_T o, ORDER_BODY_T ob
                WHERE op.PHONE_NUMBER IN (SELECT * FROM EXT01_TMP_PHONES) AND op.DATE_TO > to_date(''' || p_order_date || ''' , ''dd.mm.yy'') 
                AND o.order_id = op.order_id
                AND ob.order_id = o.order_id
                AND ob.subservice_id in  (' || p_subservice_id || ')
                AND ob.DATE_TO >= SYSDATE'; 
      OPEN p_recordset FOR v_sql;
      DELETE FROM EXT01_TMP_PHONES;        
    ELSE
      Write_TMP_phones(p_phones, ',');
      v_sql := 'SELECT DISTINCT op.PHONE_NUMBER, o.Order_No
              FROM AGENT_PHONES_T op, ORDER_T o, ORDER_BODY_T ob
              WHERE op.PHONE_NUMBER IN (SELECT * FROM EXT01_TMP_PHONES) AND op.DATE_TO > to_date(''' || p_order_date || ''' , ''dd.mm.yy'') 
              AND o.order_id = op.order_id
              AND ob.order_id = o.order_id
              AND ob.subservice_id in  (' || p_subservice_id || ')
              AND ob.DATE_TO >= SYSDATE'; 
      OPEN p_recordset FOR v_sql;
      DELETE FROM EXT01_TMP_PHONES;         
    END IF;                 
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        Pk01_SysLog.Write_msg(v_sql, c_PkgName||'.'||v_prcName, 'E');
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-------------------------------------------------
-- �������� ������ ������� �� ��������� �������
-------------------------------------------------
PROCEDURE GetTMPPhones(p_recordset OUT t_refc)
IS
    v_prcName                          CONSTANT VARCHAR2(30) := 'GetTMPPhones';
    v_retcode                          INTEGER;   
BEGIN
    OPEN p_recordset FOR
          SELECT PHONE FROM EXT01_TMP_PHONES;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

------------------------------------------------
-- �������� ID ������ �� ����� ������ � ��
------------------------------------------------
FUNCTION GetServiceIdByOMName(p_om_service_name  IN VARCHAR2) RETURN INTEGER
IS
    v_prcName             CONSTANT VARCHAR2(30) := 'GetServiceIdByOMName';
    v_res                 INTEGER;
BEGIN
    SELECT SERVICE_ID INTO v_res FROM EXT01_LOTUS_SERVICES
    WHERE SERVICE_OM = p_om_service_name;
    RETURN v_res;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
------------------------------------------------
-- �������������� CLOB � BLOB
------------------------------------------------
FUNCTION CLOB_TO_BLOB (p_clob CLOB) RETURN BLOB
as
 l_blob          blob;
 l_dest_offset   integer := 1;
 l_source_offset integer := 1;
 l_lang_context  integer := DBMS_LOB.DEFAULT_LANG_CTX;
 l_warning       integer := DBMS_LOB.WARN_INCONVERTIBLE_CHAR;
BEGIN

  DBMS_LOB.CREATETEMPORARY(l_blob, TRUE);
  DBMS_LOB.CONVERTTOBLOB
  (
   dest_lob    =>l_blob,
   src_clob    =>p_clob,
   amount      =>DBMS_LOB.LOBMAXSIZE,
   dest_offset =>l_dest_offset,
   src_offset  =>l_source_offset,
   blob_csid   =>DBMS_LOB.DEFAULT_CSID,
   lang_context=>l_lang_context,
   warning     =>l_warning
  );
  return l_blob;
END;


FUNCTION BLOB_TO_CLOB (blob_in IN BLOB) RETURN CLOB
AS
     v_clob    CLOB;
     v_varchar VARCHAR2(32767);
     v_start      PLS_INTEGER := 1;
     v_buffer  PLS_INTEGER := 32767;
BEGIN
     DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);
     
     FOR i IN 1..CEIL(DBMS_LOB.GETLENGTH(blob_in) / v_buffer)
     LOOP
          
        v_varchar := UTL_RAW.CAST_TO_VARCHAR2(DBMS_LOB.SUBSTR(blob_in, v_buffer, v_start));

           DBMS_LOB.WRITEAPPEND(v_clob, LENGTH(v_varchar), v_varchar);

          v_start := v_start + v_buffer;
     END LOOP;
     
   RETURN v_clob;
END BLOB_TO_CLOB;

------------------------------------------------
-- TEST (���������)
------------------------------------------------
--------------------------------------------
-- ��� ������ ��������������
--------------------------------------------
PROCEDURE Get_Akt(p_recordset OUT t_refc,
                  p_start_bill_date IN DATE, 
                  p_end_bill_date IN DATE,
                  p_client_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_Akt';
    v_retcode    INTEGER;  
BEGIN
   OPEN p_recordset FOR
   SELECT B.BILL_NO,
        TRUNC (B.BILL_DATE) BILL_DATE,
        B.TOTAL BILL_TOTAL,
        0 SUM_PAY,
        '' PAY_NUM,
        TO_DATE ('01.01.1970', 'dd.mm.yyyy') PAY_DATE
         FROM bill_t b,
              account_t a,
              account_profile_t ap,
              PAY_TRANSFER_T pt,
              PAYMENT_T p
        WHERE     B.REP_PERIOD_ID BETWEEN PK04_PERIOD.Period_id(p_start_bill_date) AND PK04_PERIOD.Period_id(p_end_bill_date)
              AND B.ACCOUNT_ID = A.ACCOUNT_ID
              AND A.ACCOUNT_TYPE = 'J'
              AND AP.ACCOUNT_ID = A.ACCOUNT_ID
              AND AP.CUSTOMER_ID = p_client_id
              AND PT.BILL_ID(+) = B.BILL_ID
              AND P.PAYMENT_ID(+) = PT.PAYMENT_ID
              AND TRUNC (b.BILL_DATE) >= p_start_bill_date
              AND TRUNC (b.BILL_DATE) <= p_end_bill_date
        UNION
    SELECT B.BILL_NO,
            TO_DATE ('01.01.2050', 'dd.mm.yyyy'),
            0,
            PT.TRANSFER_TOTAL SUM_PAY,
            SUBSTR (P.DOC_ID, 1, INSTR (P.DOC_ID, '-') - 1) PAY_NUM,
            TRUNC (P.PAYMENT_DATE) PAY_DATE
       FROM bill_t b,
            account_t a,
            account_profile_t ap,
            PAY_TRANSFER_T pt,
            PAYMENT_T p
      WHERE     B.REP_PERIOD_ID BETWEEN PK04_PERIOD.Period_id(p_start_bill_date) AND PK04_PERIOD.Period_id(p_end_bill_date)
            AND B.ACCOUNT_ID = A.ACCOUNT_ID
            AND A.ACCOUNT_TYPE = 'J'
            AND AP.ACCOUNT_ID = A.ACCOUNT_ID
            AND AP.CUSTOMER_ID = p_client_id
            AND PT.BILL_ID(+) = B.BILL_ID
            AND P.PAYMENT_ID(+) = PT.PAYMENT_ID
            AND TRUNC (b.BILL_DATE) >= p_start_bill_date
            AND TRUNC (b.BILL_DATE) <= p_end_bill_date;
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;



-------------------------------------------------------------
-- ��������� �������� ������ �� ������� ������� (Lotus Notes)
-------------------------------------------------------------
PROCEDURE IMPORT_MMTS_ORDERS_SPB
  (
    --p_result_str     OUT VARCHAR2,
    p_order_no       IN VARCHAR2,
    p_order_phones   IN VARCHAR2,
    p_service_name   IN VARCHAR2,
    p_order_date     IN DATE,
    p_time_zone      IN NUMBER,
    p_agreement_no   IN VARCHAR2,
    p_agreement_date IN DATE,
    p_agreement_end_date IN DATE DEFAULT NULL,
    p_currency       IN INTEGER,
    p_jur_zip    IN VARCHAR2, p_jur_state    IN VARCHAR2, p_jur_city    IN VARCHAR2, p_jur_address    IN VARCHAR2,
    p_dlv_zip    IN VARCHAR2, p_dlv_state    IN VARCHAR2, p_dlv_city    IN VARCHAR2, p_dlv_address    IN VARCHAR2,
    p_grp_zip    IN VARCHAR2, p_grp_state    IN VARCHAR2, p_grp_city    IN VARCHAR2, p_grp_address    IN VARCHAR2,
    p_inn            IN VARCHAR2,
    p_kpp            IN VARCHAR2,
    p_vat            IN NUMBER,
    p_customer_name  IN VARCHAR2,
    p_customer_short_name                   IN VARCHAR2,
    p_erp_code                              IN VARCHAR2,
    p_contract_manager                      IN VARCHAR2,
    p_contact_person                        IN VARCHAR2,
    p_contact_phone                         IN VARCHAR2,
    p_contact_fax                           IN VARCHAR2,
    p_contact_email                         IN VARCHAR2,
    p_contractor                            IN VARCHAR2,
    p_min_sum                               IN NUMBER,
    p_abon                                  IN NUMBER,
    p_speed                                 IN NUMBER,
    p_speed_unit                            IN VARCHAR2,
    p_point_1                               IN VARCHAR2,
    p_point_2                               IN VARCHAR2,
    p_point_1_address                       IN VARCHAR2,
    p_point_2_address                       IN VARCHAR2,
    p_ont_bill_summ                         IN NUMBER,
    p_ont_bill_date                         IN DATE DEFAULT NULL,
    p_result                                OUT INTEGER
  )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'IMPORT_MMTS_ORDERS_SPB';
    v_retcode       INTEGER;
    v_err_phone     INTEGER;
    v_err_order     INTEGER;
    v_err_order_imp INTEGER;
    --v_abn_table     vc100_table_t := vc100_table_t ();
    v_tmp_clob CLOB;
BEGIN
 
  -- ��������� �� ����������� � ������
  -- � ��������
  SELECT count(ORDER_ID) INTO v_err_order FROM ORDER_T WHERE ORDER_NO = p_order_no AND DATE_TO > SYSDATE;
  -- � ������� �������
  SELECT count(ORDER_NO) INTO v_err_order_imp FROM EXT01_LOTUS_MGMN WHERE ORDER_NO = p_order_no;
  
  if v_err_phone > 0 then
     p_result := 1;---1;
     --p_result_str := 'CROSSED PHONE NUMBERS!';  
  elsif v_err_order > 0 then
     p_result := 1;----2;
     --p_result_str := 'ORDER ALREADY EXISTS IN BILLING!';
  elsif v_err_order_imp > 0 then
     p_result := 1;----3;
     --p_result_str := 'ORDER ALREADY EXISTS IN IMPORT TABLE!'; 
  else
        v_tmp_clob := TO_CLOB(p_order_phones);
        INSERT INTO EXT01_LOTUS_MGMN(ID,
                                ORDER_NO,
                                ORDER_PHONES,
                                SERVICE_NAME,
                                ORDER_DATE,
                                TIME_ZONE,
                                AGREEMENT_NO,
                                AGREEMENT_DATE,
                                AGREEMENT_END_DATE,
                                CURRENCY,
                                JUR_ZIP, JUR_STATE, JUR_CITY, JUR_ADDRESS,
                                DLV_ZIP, DLV_STATE, DLV_CITY, DLV_ADDRESS,
                                GRP_ZIP, GRP_STATE, GRP_CITY, GRP_ADDRESS,
                                INN,
                                KPP,
                                VAT,
                                STATUS,
                                STATUS_INFO,
                                STATUS_DATE,
                                CUSTOMER_NAME,
                                CUSTOMER_SHORT_NAME,
                                ERP_CODE,
                                CONTRACT_MANAGER,
                                CONTACT_PERSON, CONTACT_PHONE, CONTACT_FAX, CONTACT_EMAIL,
                                CONTRACTOR, MIN_SUM, ABON, SPEED, SPEED_UNIT, POINT_1, POINT_2, 
                                POINT_1_ADDRESS, POINT_2_ADDRESS, ONT_BILL_SUMM, ONT_BILL_DATE
                                )
                         VALUES(Next_import_id,
                                p_order_no,
                                CLOB_TO_BLOB(v_tmp_clob),
                                p_service_name,
                                p_order_date,
                                p_time_zone,
                                p_agreement_no,
                                p_agreement_date,
                                p_agreement_end_date,
                                p_currency,
                                p_jur_zip, p_jur_state, p_jur_city, p_jur_address,
                                p_dlv_zip, p_dlv_state, p_dlv_city, p_dlv_address,
                                p_grp_zip, p_grp_state, p_grp_city, p_grp_address,
                                p_inn,
                                p_kpp,
                                p_vat,
                                'NEW_SPB',
                                'NEW_SPB',
                                SYSDATE,
                                p_customer_name,
                                p_customer_short_name,
                                p_erp_code,
                                p_contract_manager,
                                p_contact_person, p_contact_phone, p_contact_fax, p_contact_email,
                                p_contractor, p_min_sum, p_abon, p_speed, p_speed_unit, p_point_1, p_point_2, 
                                p_point_1_address, p_point_2_address, p_ont_bill_summ, p_ont_bill_date
                         );
      p_result := 1;
      --p_result_str := 'OK';    
  end if;
  
   
  EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName||'-'||p_order_no);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
        p_result := 1;
       -- p_result_str := 'ERROR';
END;

-------------------------------------------------------------
-- �������� �������� ������� �� OM
-------------------------------------------------------------
PROCEDURE IMPORT_CLOSED_ORDERS
  ( p_order_no       IN VARCHAR2,
    p_close_date     IN DATE,
    p_result         OUT INTEGER
  )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'IMPORT_CLOSED_ORDERS';
    v_retcode       INTEGER;
    v_order_id      INTEGER;
BEGIN

    BEGIN
       SELECT ORDER_ID 
        INTO v_order_id 
        FROM ORDER_T
       WHERE ORDER_NO = p_order_no
       AND (date_to is null or date_to = to_date('01.01.2050', 'dd.mm.yyyy'));
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        v_order_id := NULL;
    END;
                                     
    IF v_order_id IS NOT NULL then
       INSERT INTO EXT01_LOTUS_CLOSE_ORDERS_T(ID, ORDER_NO, CLOSE_DATE, LOADED)
              VALUES(Next_close_order_id, p_order_no, p_close_date, SYSDATE);
              
        Pk06_Order.Close_order(
                p_order_id => v_order_id,
                p_date_to  => p_close_date-1 
             );
    
        UPDATE EXT01_LOTUS_CLOSE_ORDERS_T
        SET CLOSED = SYSDATE
        WHERE ORDER_NO = p_order_no;        
    END IF;
    
    UPDATE EXT01_LOTUS_MGMN
    SET STATUS = 'CLOSED',
        STATUS_INFO = 'CLOSED FROM OM',
        STATUS_DATE = SYSDATE
    WHERE ORDER_NO = p_order_no;     
    
    p_result := 1;  
  EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName||'-'||p_order_no);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-------------------------------------------------------------
--  ����������/������������� ������� �� OM
-------------------------------------------------------------
PROCEDURE IMPORT_BLOCK_ORDERS
  ( p_order_no       IN VARCHAR2, -- ����� ������
    p_action         IN VARCHAR2, -- �������� (BLOCK | UNBLOCK)
    p_action_date    IN DATE,     -- ���� ���������� / �������������
    p_manager        IN VARCHAR2,  -- ��� ���������� / �������������
    p_result         OUT INTEGER
  )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'IMPORT_BLOCK_ORDERS';
    v_retcode       INTEGER;
    v_id            INTEGER;
    v_order_id      INTEGER;
    v_order_count   INTEGER;
    v_notes         VARCHAR2(500) default '';
    v_res_block     INTEGER;
    v_order_status  varchar(20);
    v_unblock       DATE := p_action_date + 23/24 + 59/1440 + 59/86400 ;
BEGIN
    v_id := Next_block_order_id;
    INSERT INTO EXT01_LOTUS_BLOCK_ORDERS_T(ID, ORDER_NO, ACTION, ACTION_DATE, MANAGER)
                                   VALUES(v_id, p_order_no, p_action, p_action_date, p_manager);
                                                                      
    SELECT COUNT(ORDER_ID) into v_order_count FROM ORDER_T
    WHERE ORDER_NO = p_order_no;
    
    IF v_order_count > 0 THEN
       SELECT ORDER_ID into v_order_id FROM ORDER_T
       WHERE ORDER_NO = p_order_no;
       
       SELECT STATUS into v_order_status FROM ORDER_T
       WHERE ORDER_NO = p_order_no;      
    
        IF p_action = 'BLOCK' AND (v_order_status <> 'LOCK' OR v_order_status is null)  THEN
          -- ��������� �����---------------
             v_res_block := PK06_ORDER.Lock_order(
                p_order_id => v_order_id,
                p_lock_type_id => 905,
                p_manager_login => p_manager,
                p_date_from => p_action_date, -- + 1,
                p_notes => v_notes
             );        
          ---------------------------------
          UPDATE EXT01_LOTUS_BLOCK_ORDERS_T
          SET DT = SYSDATE,
          RESULT = 'Blocked'
          WHERE ID = v_id;
          
          
        ELSIF p_action = 'UNBLOCK'  THEN
          -- ������������ �����----------
             v_res_block := PK06_ORDER.UnLock_order(
                p_order_id => v_order_id,
                p_manager_login => p_manager,
                p_date_to => v_unblock,
                p_notes => v_notes
             );        
          --------------------------------
          UPDATE EXT01_LOTUS_BLOCK_ORDERS_T
          SET DT = SYSDATE,
          RESULT = 'UnBlocked'
          WHERE ID = v_id;  
        ELSE
          -- ����������, ��� ���� ���� �������
          UPDATE EXT01_LOTUS_BLOCK_ORDERS_T
          SET DT = SYSDATE,
          RESULT = 'UNKNOWN ACTION'
          WHERE ID = v_id;                   
        END IF;                       
    ELSE
          -- ��� ������ ������ � BRM
          UPDATE EXT01_LOTUS_BLOCK_ORDERS_T
          SET DT = SYSDATE,
          RESULT = 'ORDER NOT FOUND'
          WHERE ID = v_id; 
    END IF;                                                       
        p_result := 1;  
EXCEPTION
  WHEN OTHERS THEN
      v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName||'-'||p_order_no);
      RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;                                   

-------------------------------------------------------------
-- �������� �������� ������� �� OM ���� ���� ����
-------------------------------------------------------------
PROCEDURE IMPORT_CLOSED_ORDERS_TEST
  ( p_order_no       IN VARCHAR2,
    p_close_date     IN DATE,
    p_result         OUT INTEGER
  )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'IMPORT_CLOSED_ORDERS_TEST';
    v_retcode       INTEGER;
BEGIN
    INSERT INTO EXT01_LOTUS_CLOSE_ORDERS_T(ID, ORDER_NO, CLOSE_DATE, LOADED)
                                   VALUES(Next_close_order_id, p_order_no || '_test', p_close_date, SYSDATE);
    
   -----------------------------------------------------
   --- ��� ������!!!! ��� ��������� �������� ������!!!
   -----------------------------------------------------
       INSERT INTO EXT01_LOTUS_CLOSE_ORDERS_T(ID, ORDER_NO, CLOSE_DATE, LOADED)
              VALUES(Next_close_order_id, 'test_' + p_order_no, p_close_date, SYSDATE);
        
    
        UPDATE EXT01_LOTUS_CLOSE_ORDERS_T
        SET CLOSED = SYSDATE
        WHERE ORDER_NO = p_order_no;           
 
    p_result := 1;  
  EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName||'-'||p_order_no);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------------------------------------------------
-- �������  ���� �����
------------------------------------------------
PROCEDURE New_mgmn_tarif(
              p_ORDER_NO IN VARCHAR2, -- ����� ������
              p_ZONE_INIT_NAME IN VARCHAR2, -- ������������ ���� �������������
              p_ZONE_INIT_ABC IN VARCHAR2, -- �������  ���� �������������
              p_DIST_REG IN NUMBER, -- 1=�������������  2=������������
              p_TAX_INCLUDE IN NUMBER, -- 1=����� ������� � ���� 0=�� �������  
              p_round_v_id IN NUMBER, 
              /*        1    UP    ���������� �� ������ �����
                        2    DOWN    ���������� �� ������ ����
                        0    NONE    �� ����������� ���������� �� �����
                        3    UP Sec    ���������� �� ������� �����
                        4    UP 10Sec    ���������� ����� �� 10 ������
                        5    UP 2    ���������� �� ������ ����� �����    */                       
              p_currency_id IN NUMBER, -- ID ������ (�������� 810 - ��� �����)
              p_unpaid_seconds IN NUMBER, -- ���������� ����� (� ��������)
              p_is_tm_not_std  in varchar2,  -- NULL - ���� ������ ������� �����������
                                             -- Y - ���� ������ ������� �������������. ��� ���� ������ ���� ��������� ����. 4 ����:
                                             -- ������ ����������: DD:DD , ��� D-�����
              p_BT_MG_FROM        in varchar2,  -- ������ ������-������� ��� ��    (08:00)  
              p_BT_MG_TO          in varchar2,  -- ��������� ������-������� ��� �� (19:00)
              p_BT_MN_FROM        in varchar2,  -- ������ ������-������� ��� ��    (08:00)
              p_BT_MN_TO          in varchar2,   -- ��������� ������-������� ��� �� (20:00)
              p_is_zm_not_std     in varchar2,  -- NULL - ���� ������� ������ �����������
                                               -- Y - ���� ������� ������ �� �����������                                   
              p_TARIF_HTML IN CLOB, -- ��� �����
              p_result         OUT INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'New_mgmn_tarif_header';
    --v_tarif_id INTEGER;
    v_retcode       INTEGER;
BEGIN
    DELETE FROM EXT01_MGMN_TARIF WHERE ORDER_NO = p_ORDER_NO;
    
    INSERT INTO EXT01_MGMN_TARIF(ID, ORDER_NO, ZONE_INIT_NAME, ZONE_INIT_ABC, DIST_REG, TAX_INCLUDE, TARIF_HTML, ROUND_V_ID, 
    CURRENCY_ID, UNPAID_SECONDS, IS_TM_NOT_STD, BT_MG_FROM, BT_MG_TO, BT_MN_FROM, BT_MN_TO, IS_ZM_NOT_STD)
           VALUES (Next_tarif_id, p_ORDER_NO, p_ZONE_INIT_NAME, p_ZONE_INIT_ABC, p_DIST_REG, p_TAX_INCLUDE, p_TARIF_HTML, p_round_v_id, 
           p_currency_id, p_unpaid_seconds, p_is_tm_not_std, p_BT_MG_FROM, p_BT_MG_TO, p_BT_MN_FROM, p_BT_MN_TO, p_is_zm_not_std);
           --RETURNING ID INTO v_tarif_id;
    p_result := 1; --v_tarif_id;
EXCEPTION
    WHEN OTHERS THEN
        p_result :=  -1;
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName||'-'||p_order_no);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

------------------------------------------------
-- �������� ���� �����
------------------------------------------------
PROCEDURE Get_mgmn_tarif(p_recordset OUT t_refc, p_ORDER_NO IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Get_mgmn_tarif';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
         SELECT * FROM EXT01_MGMN_TARIF
         WHERE ORDER_NO = p_ORDER_NO AND ROWNUM = 1;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------
------------------------------------------------------------------------
-- ������� ����� ��� IP Access InterConnect 
-- (���������� � ������ ������ ����� ��������� ������� 
-- ��� ���������� �� ������ � ������ ������ ����� ��������� �������)
------------------------------------------------------------------------
PROCEDURE New_ip02_tarif(           
              p_order_no      IN varchar2,
              p_tarif         IN varchar2,
              p_service_type  IN varchar2,
              p_result        OUT INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'New_ip_intercon_tarif';
    v_retcode       INTEGER;
BEGIN
    -- ������ �� ������ ������, ���� ����� � �������  ����� ������������� ������
    DELETE FROM EXT01_IP_TARIF WHERE ORDER_NO = p_order_no;
    -- � ������� �����
    INSERT INTO EXT01_IP_TARIF(ORDER_NO, TARIF, SERVICE_TYPE)
           VALUES (p_order_no, p_tarif, p_service_type);
           p_result := 1;           
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------------------------------------------------
-- �������� IP �����
------------------------------------------------
PROCEDURE Get_ip_tarif(p_recordset OUT t_refc, p_ORDER_NO IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Get_ip_tarif';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
         SELECT * FROM EXT01_IP_TARIF
         WHERE ORDER_NO = p_ORDER_NO;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
--------------------------------------------------
-- ��������� ID ��
--------------------------------------------------
PROCEDURE UpdateTariffId(p_order_no IN VARCHAR2, p_tariff_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'UpdateTariffId';
    v_retcode       INTEGER;   
BEGIN
    UPDATE EXT01_LOTUS_MGMN
    SET RATE_PLAN_ID = p_tariff_id
    WHERE ORDER_NO = p_order_no;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
--------------------------------------------------
-- ��������� ID ���������� ��
--------------------------------------------------
PROCEDURE UpdateAGTariffId(p_order_no IN VARCHAR2, p_tariff_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'UpdateAGTariffId';
    v_retcode       INTEGER;   
BEGIN
    UPDATE EXT01_LOTUS_MGMN
    SET AG_RATE_PLAN_ID = p_tariff_id
    WHERE ORDER_NO = p_order_no;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------------------------------------------------
-- �������� ������ �������� �������� ������
------------------------------------------------
PROCEDURE Get_delivery_desc(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Get_delivery_desc';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
         SELECT key_id, notes FROM DICTIONARY_T d
          where parent_id = PK00_CONST.c_DICT_DELIVERY_METHOD
          order by notes;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------------------------------------------------
-- �������� ID ������ �� ID ��
------------------------------------------------
FUNCTION Find_account_by_id(p_account_id  IN VARCHAR2) RETURN INTEGER
IS
    v_prcName             CONSTANT VARCHAR2(30) := 'GetCurrencyByAccountId';
    v_res                 INTEGER;
BEGIN
    select currency_id  INTO v_res  from account_t
    where account_id = p_account_id;
    RETURN v_res;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--------------------------------------------------------------------
-- �������� ID ��������� ������������� �� ������ � � �������� � ��
--------------------------------------------------------------------
FUNCTION Find_agent_id(p_account_no  IN VARCHAR2, p_brand_id IN INTEGER) RETURN INTEGER
IS
    v_prcName             CONSTANT VARCHAR2(30) := 'Find_agent_id';
    v_res                 INTEGER;
BEGIN
  select contractor_id  INTO v_res   from contractor_t
  where notes = p_account_no
  and parent_id = p_brand_id;
  RETURN v_res;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
-------------------------------------------
-- ���������� ������ �����
-------------------------------------------
PROCEDURE GetNetworks(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetNetworks';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
         select network_id, network from network_t
         order by network;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
---
PROCEDURE GetNetworks(p_recordset OUT t_refc, p_branch_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetNetworks';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      select n.network_id, n.network 
             from network_t n, CONTRACTOR_NETWORK_T cn
             where CN.NETWORK_ID = N.NETWORK_ID
             and CN.CONTRACTOR_ID = p_branch_id
             order by network;  
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
----
PROCEDURE GetNetworksByContractors(p_recordset OUT t_refc, p_contractor_list IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetNetworksByContractors';
    v_retcode       INTEGER;  
    v_sql           VARCHAR(2000);
BEGIN
 v_sql := 'select distinct n.network_id, n.network 
             from network_t n, CONTRACTOR_NETWORK_T cn
             where CN.NETWORK_ID = N.NETWORK_ID
             and CN.CONTRACTOR_ID IN (' || p_contractor_list || ')
             order by network' ; 
    OPEN p_recordset FOR v_sql;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
-------------------------------------------
-- ���������� ������  ������������ �� ID ����
-------------------------------------------
PROCEDURE GetSwitch(p_recordset OUT t_refc, p_network_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetSwitch';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
         select * from switch_t
         where network_id = p_network_id
         order by switch_name;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
---------------------------------------------
-- ����� ID ��������� ������ �� ID ������
---------------------------------------------
PROCEDURE GetSubservicesIdByOrderId(p_recordset OUT t_refc, p_order_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetSubservicesIdByOrderId';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      select subservice_id from order_body_t
      where charge_type = 'USG'
      and order_id = p_order_id
      and date_to = to_date('01.01.2050', 'dd.mm.yyyy');
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
---------------------------------------------
-- ����� Order_Body_ID ��� item'�  �������� �����
---------------------------------------------
FUNCTION GetOrderBodyId4Item(p_order_id IN INTEGER) RETURN INTEGER
IS
    v_prcName             CONSTANT VARCHAR2(30) := 'GetOrderBodyId4Item';
    v_res                 INTEGER;
BEGIN
      select order_body_id into v_res from order_body_t
          where order_id = p_order_id
          and charge_type='USG'
          and subservice_id = (
              select min(subservice_id) from order_body_t
              where order_id = p_order_id
              and charge_type='USG'
          );
  RETURN v_res;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

---------------------------------------------
-- ����� ����� ������ �� ��� ID
---------------------------------------------
FUNCTION GetOrderNoById(p_order_id IN INTEGER) RETURN VARCHAR2
IS
    v_prcName             CONSTANT VARCHAR2(30) := 'GetOrderNoById';
    v_res                 VARCHAR2(100);
BEGIN
      select order_no into v_res from order_t
          where order_id = p_order_id;
  RETURN v_res;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN '';
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
---------------------------------------------
-- ����� ��� ������ �� ��������
---------------------------------------------
PROCEDURE GetOrdersByContractNo(p_recordset OUT t_refc, p_contract_no IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetOrdersByContractId';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
SELECT o.*
  FROM order_t o, account_profile_t ap, contract_t c
     WHERE O.ACCOUNT_ID = AP.ACCOUNT_ID 
     AND ap.contract_id = c.contract_id
     and c.contract_no = p_contract_no;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------------------------------------------------
-- ����� ������� ������� ��� ������ �����
------------------------------------------------
FUNCTION GetRegionCode(p_account_id IN INTEGER) RETURN VARCHAR2
IS
    v_prcName             CONSTANT VARCHAR2(30) := 'GetRegionCode';
    v_res                 VARCHAR2(10);
BEGIN
  select region_id  into v_res from contractor_t
         where contractor_id =(
               select contractor_id from account_profile_t
                 where account_id = p_account_id
                  AND ACTUAL = 'Y'
          );
  RETURN v_res;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN '';
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

------------------------------------------------
-- ����� ������� ����������� ��� IP
------------------------------------------------
PROCEDURE GetIpTariffRules(p_recordset OUT t_refc, p_lotus_tariff_type IN VARCHAR2, p_brm_service IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetIpTariffRules';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
    SELECT *  FROM EXT01_LOTUS_IP_TARIFF_TYPES
         WHERE LOTUS_TYPE = p_lotus_tariff_type 
         AND INSTR(BRM_SERVICE_LIST, p_brm_service) <> 0
         AND ROWNUM = 1; --<<-- �� ������ ������...
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
---------------------------------------------------
-- �������� - ��� ����� ������������ ��� ���
---------------------------------------------------
FUNCTION CheckPhoneToUse(p_phone_num IN varchar2) RETURN INTEGER
IS
    v_prcName             CONSTANT VARCHAR2(30) := 'CheckPhoneToUse';
    v_used                INTEGER DEFAULT 0;
BEGIN
    select count(1) into v_used from order_phones_t
    where phone_number = p_phone_num
    and (date_to > sysdate or date_to is null);   
    RETURN v_used;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

---------------------------------------------
-- ������ ������� ��� �������
---------------------------------------------
PROCEDURE GetRatePlanListP(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetRatePlanListP';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
            SELECT *
             FROM RATEPLAN_T
            WHERE rateplan_type = 'KTTK_P'   
            ORDER BY RATEPLAN_NAME;         
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

---------------------------------------------
-- �������� ������������ ��� ������ ��
---------------------------------------------
FUNCTION GetNewRatePlanName(p_RP_name IN VARCHAR2) RETURN VARCHAR2
IS
    v_prcName             CONSTANT VARCHAR2(30) := 'GetNewRatePlanName';
    v_new_name            VARCHAR2(50);
    v_max_RP              VARCHAR2(50);
BEGIN
  
  SELECT NVL (MAX (RATEPLAN_NAME), p_RP_name) into v_max_RP
         FROM RATEPLAN_T
         WHERE RATEPLAN_NAME LIKE
             NVL (SUBSTR (p_RP_name, 1, INSTR (p_RP_name, '-', -1)),
                  p_RP_name || '-') || '%';

   SELECT    NVL (SUBSTR (v_max_RP, 1, INSTR (v_max_RP, '-', -1)),
               v_max_RP || '-') 
             || NVL (
               TO_NUMBER_OR_NULL (
                  SUBSTR (v_max_RP, INSTR (v_max_RP, '-', -1) + 1))
             + 1,
             1) into v_new_name from dual;                  

    RETURN v_new_name;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
---------------------------------------------------
-- �������� ������ �� �������� ���� � ORDER_BODY_T
---------------------------------------------------
PROCEDURE UpdateRatePlan(p_order_id IN INTEGER, p_order_body_id IN INTEGER, p_rateplan_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'UpdateRatePlan';
    v_retcode       INTEGER;   
BEGIN
    UPDATE ORDER_BODY_T
    SET RATEPLAN_ID = p_rateplan_id
    WHERE ORDER_ID = p_order_id AND ORDER_BODY_ID = p_order_body_id;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
-----------------------------------------------------
-- �������� ����������-�������� �� �������
-----------------------------------------------------
PROCEDURE GetContractorSaleByRegion(p_recordset OUT t_refc, p_contractor_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetContractorSaleByRegion';
    v_retcode       INTEGER;
    v_contractor_id   INTEGER;
BEGIN
  if p_contractor_id = 11 then
    v_contractor_id := 1;
  else
    v_contractor_id :=p_contractor_id;
  end if ;

      -- ��������!!! 
    v_contractor_id := 1;
  
    OPEN p_recordset FOR
            SELECT *
             FROM contractor_t
              where XTTK_ID = v_contractor_id
              order by contractor;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------------------------------------------------
-- ���������� ������� "���. ��������"
------------------------------------------------
FUNCTION SetContractGos(p_contract_id IN INTEGER) RETURN INTEGER
IS
    v_prcName             CONSTANT VARCHAR2(30) := 'SetContractGos';
BEGIN
    /*UPDATE CONTRACT_T
    SET GOVERMENT_TYPE = 1
    WHERE CONTRACT_ID = p_contract_id;   */
    RETURN 1;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

---------------------------------------------------------
-- ����������� ���������� ����� � ������ ������ �� ������
---------------------------------------------------------
  
PROCEDURE CopyOrderBody (
               p_order_id_from    IN INTEGER,
               p_order_id_to   IN INTEGER,
               p_order_date    IN DATE
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'CopyOrderBody';
BEGIN
   INSERT INTO ORDER_BODY_T(ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, DATE_FROM, DATE_TO, RATEPLAN_ID, RATE_VALUE, FREE_VALUE, RATE_RULE_ID, RATE_LEVEL_ID, TAX_INCL, QUANTITY, CREATE_DATE, MODIFY_DATE, CURRENCY_ID, NOTES, RATEPLAN_PCK_ID)
         SELECT SQ_ORDER_ID.NEXTVAL, p_order_id_to , SUBSERVICE_ID, CHARGE_TYPE, p_order_date, to_date('01.01.2050', 'dd.mm.yyyy'), RATEPLAN_ID, RATE_VALUE, FREE_VALUE, RATE_RULE_ID, RATE_LEVEL_ID, TAX_INCL, QUANTITY, CREATE_DATE, MODIFY_DATE, CURRENCY_ID, NOTES, RATEPLAN_PCK_ID
                FROM ORDER_BODY_T 
                WHERE ORDER_ID = p_order_id_from;
   -- ��������� ������ �� �� � ��� ����� (ORDER_T)
   -- ������ ������ ����� ���� � �� ����!
   UPDATE ORDER_T o
   SET o.rateplan_id = (SELECT rateplan_id FROM ORDER_T WHERE ORDER_ID = p_order_id_from)
   WHERE o.ORDER_ID =  p_order_id_to;               
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
       Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

------------------------------------------------------------------------------------
-- ��������� ������� ���������� PK30_CORRECT_TAX_INCL_T 
-- (������ � ������������� ��������� TAX_INCL � �������)
------------------------------------------------------------------------------------
PROCEDURE FIll_tax_incl(p_orderPbody_id IN INTEGER, p_account_type IN CHAR, p_tax_incl IN CHAR)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'FIll_tax_incl';
    v_retcode       INTEGER;   
BEGIN
    INSERT INTO PK30_CORRECT_TAX_INCL_T(ORDER_BODY_ID, ACCOUNT_TYPE, TAX_INCL)
                VALUES(p_orderPbody_id, p_account_type, p_tax_incl);
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

----------------------------------------------------------------------------------------
-- ���������� �������� ��� �������� ����������� ��������� ����������� �� �������� �����
----------------------------------------------------------------------------------------
PROCEDURE SendDetailByEmail(p_account_id IN INTEGER, p_email_type IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'SendDetailByEmail';
    v_retcode       INTEGER;
    v_email_incl     INTEGER;   
BEGIN
    SELECT COUNT(*) INTO v_email_incl FROM ACCOUNT_DOCUMENTS_T 
           WHERE ACCOUNT_ID = p_account_id
           AND DELIVERY_METHOD_ID = 6501;
    IF v_email_incl > 0 THEN
      UPDATE ACCOUNT_DOCUMENTS_T
       SET DOC_CALLS = p_email_type
       WHERE ACCOUNT_ID = p_account_id; 
    ELSE
       INSERT INTO ACCOUNT_DOCUMENTS_T(ACCOUNT_ID, DOC_BILL, DOC_DETAIL, DOC_CALLS, DELIVERY_METHOD_ID, ORDER_ID, CONTRACTOR_ID, EMAIL)
         VALUES(p_account_id, null , null, p_email_type, 6501, null, null, null);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;








PROCEDURE get_contractors(p_recordset OUT t_refc, p_contractor_list IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'get_contractors';
    v_retcode       INTEGER;   
    v_sql           VARCHAR2(4000);
BEGIN
  
 v_sql := 'SELECT * FROM CONTRACTOR_T
             WHERE CONTRACTOR_ID IN (' || p_contractor_list || '))
             ORDER BY CONTRACTOR' ; 
    OPEN p_recordset FOR v_sql;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;




PROCEDURE Get_address( 
               p_recordset     OUT t_refc, 
               p_contractor_id  IN INTEGER,
               p_date           IN DATE DEFAULT SYSDATE
             )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_address';
    v_retcode    INTEGER;
BEGIN
    OPEN p_recordset FOR
         SELECT ADDRESS_ID, ADDRESS_TYPE, COUNTRY, ZIP, STATE, CITY, ADDRESS,
                PHONE_ACCOUNT, PHONE_BILLING, FAX, EMAIL, 
                DATE_FROM, DATE_TO, CONTRACTOR_ID 
           FROM CONTRACTOR_ADDRESS_T
          WHERE CONTRACTOR_ID = p_contractor_id
            AND p_date >= DATE_FROM AND p_date <= NVL(DATE_TO,TO_DATE('01.01.2050','DD.MM.YYYY'))
          ORDER BY DATE_FROM;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ������� CCAD BDR �� ������� ����� - ����
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Move_ccad_bdr (
               p_src_bill_id   IN INTEGER,    -- ID ����� ��� �������� ��������� �����-����
               p_src_period_id IN INTEGER,    -- ID ���������� ������� YYYYMM ���������
               p_dst_bill_id   IN INTEGER,    -- ID �����-����
               p_dst_period_id IN INTEGER     -- ID ���������� ������� �����-���� YYYYMM
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Move_ccad_bdrs';
    v_count          INTEGER;
BEGIN


    --
    Pk01_Syslog.Write_msg('Start dst_bill_id = '||p_dst_bill_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- --------------------------------------------------------------------- --
    -- VPN (service_id = 106, subservice_id = 39)
    -- --------------------------------------------------------------------- -- 
    -- ������ ����� �������, ��� ������ �����
    INSERT INTO BDR_CCAD_T (
                BDR_ID, BDR_TYPE_ID, REP_PERIOD_ID, BILL_ID, ITEM_ID, 
                ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, 
                DATE_FROM, DATE_TO, RATEPLAN_ID, QUALITY_ID, 
                ZONE_OUT, ZONE_IN, VOLUME, VOLUME_UNIT_ID, PRICE, AMOUNT, CF, 
                BDR_STATUS_ID, CURRENCY_ID, TAX_INCL, CREATE_DATE, BYTES, RATE_RULE_ID, 
                BDR_PARENT_ID, NOTES )
    SELECT Pk02_Poid.Next_ccad_bdr_id BDR_ID, BDR_TYPE_ID, 
           p_dst_period_id REP_PERIOD_ID, p_dst_bill_id BILL_ID, NULL ITEM_ID, 
           ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, 
           DATE_FROM, DATE_TO, RATEPLAN_ID, QUALITY_ID, 
           ZONE_OUT, ZONE_IN, VOLUME, VOLUME_UNIT_ID, PRICE, AMOUNT, CF, 
           BDR_STATUS_ID, CURRENCY_ID, TAX_INCL, SYSDATE CREATE_DATE, BYTES, RATE_RULE_ID, 
           BDR_ID BDR_PARENT_ID, 'double record' NOTES
     FROM BDR_CCAD_T CB
    WHERE BILL_ID = p_src_bill_id
      AND REP_PERIOD_ID = p_src_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_VPN_T '||v_count||' rows duplicated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ������� ITEM-� 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_dst_period_id
       AND I.BILL_ID       = p_dst_bill_id
       AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG
       AND EXISTS (
           SELECT * FROM BDR_CCAD_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.BILL_ID       = I.BILL_ID
              AND CB.ORDER_ID      = I.ORDER_ID
              AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
              AND CB.SERVICE_ID    = I.SERVICE_ID
              AND CB.SUBSERVICE_ID = I.SUBSERVICE_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� item-�
    INSERT INTO ITEM_T I (
        I.REP_PERIOD_ID,
        I.BILL_ID,
        I.ITEM_ID,
        I.ITEM_TYPE,
        I.INV_ITEM_ID,
        I.ORDER_ID, 
        I.SERVICE_ID,
        I.ORDER_BODY_ID,
        I.SUBSERVICE_ID,
        I.CHARGE_TYPE,
        I.ITEM_TOTAL,
        I.DATE_FROM,
        I.DATE_TO,
        I.ITEM_STATUS,
        I.TAX_INCL,
        I.CREATE_DATE,
        ITEM_CURRENCY_ID
    )
    WITH BDR AS (
        SELECT CB.REP_PERIOD_ID,
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               CB.CURRENCY_ID,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_T CB
         WHERE CB.REP_PERIOD_ID = p_dst_period_id
           AND CB.BILL_ID       = p_dst_bill_id
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               CB.CURRENCY_ID
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        BDR.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,
        CASE
           WHEN BDR.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
           ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE,
        BDR.CURRENCY_ID
      FROM BDR
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ����������� ��������� �� item-�    
    MERGE INTO BDR_CCAD_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_T CB
           WHERE I.REP_PERIOD_ID  = p_dst_period_id
             AND I.BILL_ID        = p_dst_bill_id
             AND I.CHARGE_TYPE    = Pk00_Const.c_CHARGE_TYPE_USG
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.BILL_ID       = I.BILL_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_VPN_T->ITEM_ID '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );









/*


    -- -------------------------------------------------------------------- --
    -- IP_BURST (service_id = 104, subservice_id = 40)
    -- -------------------------------------------------------------------- --
    -- ������ ����� �������, ��� ������ �����
    INSERT INTO BDR_CCAD_T (BDR_ID, BDR_TYPE_ID, REP_PERIOD_ID, BILL_ID, ITEM_ID, ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, DATE_FROM, DATE_TO, RATEPLAN_ID, PAID_SPEED, PAID_SPEED_UNIT, EXCESS_SPEED, EXCESS_SPEED_UNIT, PRICE, AMOUNT, CF, INFO_WHEN, INFO_ROUTER_IP, INFO_DIRECTION, BDR_STATUS_ID, CURRENCY_ID, TAX_INCL, CREATE_DATE, RATE_RULE_ID, MAX_SPEED_BPS, PORT_SPEED_BPS, BDR_PARENT_ID, NOTES)
    SELECT  Pk02_Poid.Next_ccad_bdr_id BDR_ID, BDR_TYPE_ID, 
            p_dst_period_id REP_PERIOD_ID, p_dst_bill_id BILL_ID, NULL ITEM_ID,
            ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, 
            DATE_FROM, DATE_TO, RATEPLAN_ID, 
            PAID_SPEED, PAID_SPEED_UNIT, EXCESS_SPEED, EXCESS_SPEED_UNIT, 
            PRICE, AMOUNT, CF, INFO_WHEN, INFO_ROUTER_IP, INFO_DIRECTION, 
            BDR_STATUS_ID, CURRENCY_ID, TAX_INCL, SYSDATE CREATE_DATE, RATE_RULE_ID, 
            MAX_SPEED_BPS, PORT_SPEED_BPS, 
            BDR_ID BDR_PARENT_ID, 'double record' NOTES
     FROM BDR_CCAD_T CB
    WHERE BILL_ID = p_src_bill_id
      AND REP_PERIOD_ID = p_src_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_VPN_T '||v_count||' rows duplicated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ������� ITEM-� 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_dst_period_id
       AND I.BILL_ID       = p_dst_bill_id
       AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG
       AND EXISTS (
           SELECT * FROM BDR_CCAD_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.BILL_ID       = I.BILL_ID
              AND CB.ORDER_ID      = I.ORDER_ID
              AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
              AND CB.SERVICE_ID    = I.SERVICE_ID
              AND CB.SUBSERVICE_ID = I.SUBSERVICE_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� item-�
    INSERT INTO ITEM_T I (
        I.REP_PERIOD_ID,
        I.BILL_ID,
        I.ITEM_ID,
        I.ITEM_TYPE,
        I.INV_ITEM_ID,
        I.ORDER_ID, 
        I.SERVICE_ID,
        I.ORDER_BODY_ID,
        I.SUBSERVICE_ID,
        I.CHARGE_TYPE,
        I.ITEM_TOTAL,
        I.DATE_FROM,
        I.DATE_TO,
        I.ITEM_STATUS,
        I.TAX_INCL,
        I.CREATE_DATE
    )
    WITH BDR AS (
        SELECT CB.REP_PERIOD_ID,
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               CB.CURRENCY_ID,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_T CB
         WHERE CB.REP_PERIOD_ID = p_dst_period_id
           AND CB.BILL_ID       = p_dst_bill_id
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        BDR.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,
        CASE
           WHEN BDR.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
           ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE
      FROM BDR
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ����������� ��������� �� item-�    
    MERGE INTO BDR_CCAD_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_T CB
           WHERE I.REP_PERIOD_ID  = p_dst_period_id
             AND I.BILL_ID        = p_dst_bill_id
             AND I.CHARGE_TYPE    = Pk00_Const.c_CHARGE_TYPE_USG
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.BILL_ID       = I.BILL_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_BURST_T->ITEM_ID '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- --------------------------------------------------------------------- --
    -- IP_VOLUME  - ����������� �� ������ (service_id = 104, subservice_id = 39)
    -- --------------------------------------------------------------------- --
    -- ������ ����� �������, ��� ������ �����
    INSERT INTO BDR_CCAD_T (
           BDR_ID, BDR_TYPE_ID, 
           REP_PERIOD_ID, BILL_ID, ITEM_ID, 
           ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, 
           DATE_FROM, DATE_TO, RATEPLAN_ID, VOLUME, VOLUME_UNIT_ID, PRICE, AMOUNT, 
           CF, BDR_STATUS_ID, CURRENCY_ID, TAX_INCL, STEP_MIN_BYTES, STEP_MAX_BYTES,
           BYTES, KV, KF, ABON_PLATA, CREATE_DATE, 
           VOLUME_IN, VOLUME_OUT, RATE_RULE_ID, 
           BDR_PARENT_ID, NOTES)
    SELECT 
           Pk02_Poid.Next_ccad_bdr_id BDR_ID, BDR_TYPE_ID, 
           p_dst_period_id REP_PERIOD_ID, p_dst_bill_id BILL_ID, NULL ITEM_ID, 
           ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, 
           DATE_FROM, DATE_TO, RATEPLAN_ID, VOLUME, VOLUME_UNIT_ID, PRICE, AMOUNT, 
           CF, BDR_STATUS_ID, CURRENCY_ID, TAX_INCL, STEP_MIN_BYTES, STEP_MAX_BYTES, 
           BYTES, KV, KF, ABON_PLATA, SYSDATE CREATE_DATE, 
           VOLUME_IN, VOLUME_OUT, RATE_RULE_ID, 
           BDR_ID BDR_PARENT_ID, 'double record' NOTES
     FROM BDR_CCAD_T CB
    WHERE BILL_ID = p_src_bill_id
      AND REP_PERIOD_ID = p_src_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_VPN_T '||v_count||' rows duplicated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ������� ITEM-� 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_dst_period_id
       AND I.BILL_ID       = p_dst_bill_id
       AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG
       AND EXISTS (
           SELECT * FROM BDR_CCAD_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.BILL_ID       = I.BILL_ID
              AND CB.ORDER_ID      = I.ORDER_ID
              AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
              AND CB.SERVICE_ID    = I.SERVICE_ID
              AND CB.SUBSERVICE_ID = I.SUBSERVICE_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� item-�
    INSERT INTO ITEM_T I (
        I.REP_PERIOD_ID,
        I.BILL_ID,
        I.ITEM_ID,
        I.ITEM_TYPE,
        I.INV_ITEM_ID,
        I.ORDER_ID, 
        I.SERVICE_ID,
        I.ORDER_BODY_ID,
        I.SUBSERVICE_ID,
        I.CHARGE_TYPE,
        I.ITEM_TOTAL,
        I.DATE_FROM,
        I.DATE_TO,
        I.ITEM_STATUS,
        I.TAX_INCL,
        I.CREATE_DATE
    )
    WITH BDR AS (
        SELECT CB.REP_PERIOD_ID,
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               CB.CURRENCY_ID,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_T CB
         WHERE CB.REP_PERIOD_ID = p_dst_period_id
           AND CB.BILL_ID       = p_dst_bill_id
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        BDR.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,
        CASE
           WHEN BDR.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
           ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE
      FROM BDR
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ����������� ��������� �� item-�    
    MERGE INTO BDR_CCAD_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_T CB
           WHERE I.REP_PERIOD_ID  = p_dst_period_id
             AND I.BILL_ID        = p_dst_bill_id
             AND I.CHARGE_TYPE    = Pk00_Const.c_CHARGE_TYPE_USG
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.BILL_ID       = I.BILL_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_VOL_T->ITEM_ID '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- --------------------------------------------------------------------- --
    -- RT - ���������� ����������� (service_id = 104, subservice_id = 31)
    -- --------------------------------------------------------------------- --
    -- ������ ����� �������, ��� ������ �����
    INSERT INTO BDR_CCAD_T (BDR_ID,
                              BDR_TYPE_ID,
                              REP_PERIOD_ID,
                              BILL_ID,
                              ITEM_ID,
                              ORDER_ID    ,
                              SERVICE_ID,
                              ORDER_BODY_ID,
                              SUBSERVICE_ID ,
                              DATE_FROM    ,
                              DATE_TO    ,
                              RATEPLAN_ID ,
                              GR_1_BYTES   ,
                              GR_2_BYTES    ,
                              GR_31_BYTES    ,
                              GR_32_BYTES    ,
                              GR_33_BYTES    ,
                              GR_34_BYTES    ,
                              GR_1_PRICE    ,
                              GR_2_PRICE    ,
                              GR_31_PRICE    ,
                              GR_32_PRICE    ,
                              GR_33_PRICE    ,
                              GR_34_PRICE    ,
                              SPEED    ,
                              SPEED_UNIT,
                              AMOUNT    ,
                              CF    ,
                              BDR_STATUS_ID,
                              CURRENCY_ID  ,
                              TAX_INCL    ,
                              CREATE_DATE  ,
                              SPEED_DATE    ,
                              RATE_RULE_ID   ,
                              BDR_PARENT_ID   ,
                              NOTES)
    SELECT Pk02_Poid.Next_ccad_bdr_id BDR_ID, BDR_TYPE_ID, 
           p_dst_period_id REP_PERIOD_ID, p_dst_bill_id BILL_ID, NULL ITEM_ID, 
           ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, DATE_FROM, DATE_TO, 
           RATEPLAN_ID, GR_1_BYTES, GR_2_BYTES, GR_31_BYTES, GR_32_BYTES, GR_33_BYTES, 
           GR_34_BYTES, GR_1_PRICE, GR_2_PRICE, GR_31_PRICE, GR_32_PRICE, GR_33_PRICE, 
           GR_34_PRICE, SPEED, SPEED_UNIT, AMOUNT, CF, BDR_STATUS_ID, CURRENCY_ID, TAX_INCL, 
           SYSDATE CREATE_DATE, SPEED_DATE, RATE_RULE_ID, 
           BDR_ID BDR_PARENT_ID, 'double record' NOTES
     FROM BDR_CCAD_T CB
    WHERE BILL_ID = p_src_bill_id
      AND REP_PERIOD_ID = p_src_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_VPN_T '||v_count||' rows duplicated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ������� ITEM-� 
    DELETE FROM ITEM_T I
     WHERE I.REP_PERIOD_ID = p_dst_period_id
       AND I.BILL_ID       = p_dst_bill_id
       AND I.CHARGE_TYPE   = Pk00_Const.c_CHARGE_TYPE_USG
       AND EXISTS (
           SELECT * FROM BDR_CCAD_T CB
            WHERE CB.REP_PERIOD_ID = I.REP_PERIOD_ID
              AND CB.BILL_ID       = I.BILL_ID
              AND CB.ORDER_ID      = I.ORDER_ID
              AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
              AND CB.SERVICE_ID    = I.SERVICE_ID
              AND CB.SUBSERVICE_ID = I.SUBSERVICE_ID
       );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ��������� item-�
    INSERT INTO ITEM_T I (
        I.REP_PERIOD_ID,
        I.BILL_ID,
        I.ITEM_ID,
        I.ITEM_TYPE,
        I.INV_ITEM_ID,
        I.ORDER_ID, 
        I.SERVICE_ID,
        I.ORDER_BODY_ID,
        I.SUBSERVICE_ID,
        I.CHARGE_TYPE,
        I.ITEM_TOTAL,
        I.DATE_FROM,
        I.DATE_TO,
        I.ITEM_STATUS,
        I.TAX_INCL,
        I.CREATE_DATE
    )
    WITH BDR AS (
        SELECT CB.REP_PERIOD_ID,
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL,
               CB.CURRENCY_ID,
               SUM(CB.AMOUNT)    ITEM_TOTAL,
               MIN(CB.DATE_FROM) DATE_FROM,
               MAX(CB.DATE_TO)   DATE_TO
          FROM BDR_CCAD_T CB
         WHERE CB.REP_PERIOD_ID = p_dst_period_id
           AND CB.BILL_ID       = p_dst_bill_id
          GROUP BY 
               CB.REP_PERIOD_ID,
               CB.BILL_ID,
               CB.ORDER_ID, 
               CB.SERVICE_ID,
               CB.ORDER_BODY_ID,
               CB.SUBSERVICE_ID,
               CB.TAX_INCL
    )   
    SELECT 
        BDR.REP_PERIOD_ID,
        BDR.BILL_ID,
        SQ_ITEM_ID.NEXTVAL           ITEM_ID,
        Pk00_Const.c_ITEM_TYPE_BILL  ITEM_TYPE,
        NULL                         INV_ITEM_ID,
        BDR.ORDER_ID, 
        BDR.SERVICE_ID,
        BDR.ORDER_BODY_ID,
        BDR.SUBSERVICE_ID,
        Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,
        CASE
           WHEN BDR.CURRENCY_ID = Pk00_Const.c_CURRENCY_YE_FIX THEN ROUND(BDR.ITEM_TOTAL*28.6,2)
           ELSE ROUND(BDR.ITEM_TOTAL,2)
        END ITEM_TOTAL,
        BDR.DATE_FROM,
        BDR.DATE_TO,
        Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,
        BDR.TAX_INCL,
        SYSDATE                      CREATE_DATE
      FROM BDR
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    -- ����������� ��������� �� item-�    
    MERGE INTO BDR_CCAD_T CB
    USING (
          SELECT DISTINCT CB.BDR_ID, I.ITEM_ID
            FROM ITEM_T I, BDR_CCAD_T CB
           WHERE I.REP_PERIOD_ID  = p_dst_period_id
             AND I.BILL_ID        = p_dst_bill_id
             AND I.CHARGE_TYPE    = Pk00_Const.c_CHARGE_TYPE_USG
             AND CB.REP_PERIOD_ID = I.REP_PERIOD_ID
             AND CB.BILL_ID       = I.BILL_ID
             AND CB.ORDER_ID      = I.ORDER_ID
             AND CB.ORDER_BODY_ID = I.ORDER_BODY_ID
    ) I
    ON (
       CB.BDR_ID = I.BDR_ID
    )
    WHEN MATCHED THEN UPDATE SET CB.ITEM_ID = I.ITEM_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BDR_CCAD_RT_T->ITEM_ID '||v_count||' rows merged', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
 */
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-----------------------------------------------------
-- �������� ���������� �������� ���
-----------------------------------------------------
PROCEDURE GetCSSDirections(p_recordset OUT t_refc)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetCSSDirections';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      SELECT KEY_ID, PARENT_ID, KEY, NAME, NOTES
      FROM DICTIONARY_T D
      WHERE D.PARENT_ID = 30;  
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-----------------------------------------------------
-- �������� ������ �������� ��� �� ������
-----------------------------------------------------
PROCEDURE GetCSSDirectionsByOrder(p_recordset OUT t_refc, p_order_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetCSSDirectionsByOrder';
    v_retcode       INTEGER;   
BEGIN
    OPEN p_recordset FOR
      SELECT DISTINCT DESCR FROM ORDER_PHONES_T
      WHERE ORDER_ID = p_order_id;  
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

--------------------------------------------
-- ��������� ����������� � ��������
--------------------------------------------
PROCEDURE UpdateProfileContractors(
          p_src_brach_id     IN INTEGER,  
          p_dst_seller_id    IN INTEGER,
          p_dst_bank_id      IN INTEGER,
          p_dst_date_from    IN DATE)
IS
    v_prcName           CONSTANT VARCHAR2(30) := 'UpdateProfileContractors';
    v_retcode           INTEGER;   
    v_old_profile_id    INTEGER;
    v_new_profile_id    INTEGER;
    v_period_id         INTEGER;
    v_region_id         INTEGER;
BEGIN
  
    SELECT MIN(PERIOD_ID) INTO v_period_id
      FROM PERIOD_T P
     WHERE CLOSE_REP_PERIOD IS NULL;
    
    SELECT CT.REGION_ID INTO v_region_id 
      FROM CONTRACTOR_T CT
     WHERE CT.CONTRACTOR_ID = p_dst_seller_id;

    DELETE FROM TMP_CONTRACTOR_CHANGE_20160901;
    
    FOR accounts IN (
      /*
      SELECT ap.profile_id,
          A.ACCOUNT_ID, ap.BRAND_ID, ap.contract_id, cmp.company_id, ap.subscriber_id,
          p_new_contractor_id CONTRACTOR_ID,  ap.branch_id, ap.agent_id, 
          (select bank_id from CONTRACTOR_BANK_T where contractor_id = p_new_contractor_id) contractor_bank_id,
          AP.VAT,
          p_new_profile_date DATE_FROM, null DATE_TO, 
          1 close_prev_profile, ap.kpp, ap.erp_code, AP.CORRESPONDENT_BANK_ID
        FROM account_t a,
             account_profile_t ap,
             contract_t ct,
             contractor_t ctr,
             contractor_t branch,
             company_t cmp
       WHERE     AP.ACCOUNT_ID = A.ACCOUNT_ID
             AND ap.contract_id = ct.contract_id
             AND ap.contract_id = cmp.contract_id
             AND ctr.contractor_id = ap.contractor_id
             AND branch.contractor_id = ap.branch_id
             AND ap.branch_id = p_old_brach_id
             AND ap.actual = 'Y'
             AND A.ACCOUNT_TYPE = 'J'
             AND cmp.actual = 'Y'
             AND ap.contractor_id <> 1
      */
        SELECT * FROM (          
            WITH SL AS (    -- �������� �� ������� ������ �������� � �������
                SELECT CT.CONTRACTOR_ID DST_SELLER_ID, 
                       CB.BANK_ID DST_BANK_ID,
                       p_dst_date_from DST_DATE_FROM,
                       NULL DST_DATE_TO,
                       1 CLOSE_PREV_PROFILE
                  FROM CONTRACTOR_T CT, CONTRACTOR_BANK_T CB
                 WHERE CT.CONTRACTOR_ID = CB.CONTRACTOR_ID
                   AND CT.CONTRACTOR_ID = p_dst_seller_id -- 1524374
                   AND CB.BANK_ID       = p_dst_bank_id   -- 1524375
            )
            SELECT AP.PROFILE_ID, 
                   AP.ACCOUNT_ID, 
                   AP.BRAND_ID, 
                   AP.CONTRACT_ID, 
                   CM.COMPANY_ID, 
                   AP.SUBSCRIBER_ID,
                   SL.DST_SELLER_ID,
                   AP.BRANCH_ID, 
                   AP.AGENT_ID,
                   SL.DST_BANK_ID,
                   AP.VAT,
                   SL.DST_DATE_FROM,
                   SL.DST_DATE_TO,
                   SL.CLOSE_PREV_PROFILE,
                   AP.KPP,
                   AP.ERP_CODE,
                   AP.CORRESPONDENT_BANK_ID
              FROM ACCOUNT_PROFILE_T AP,
                   COMPANY_T CM,
                   SL
             WHERE AP.ACTUAL         = 'Y'
               AND AP.CONTRACT_ID    = CM.CONTRACT_ID
               AND CM.ACTUAL         = 'Y'
               AND AP.CONTRACTOR_ID != 1    -- �� ���� ������ � ������� �� ������
               AND AP.BRANCH_ID      = p_src_brach_id -- 297
        )
      )
   LOOP
     v_old_profile_id := accounts.profile_id;
     v_new_profile_id := pk51_w_account_edit.New_profile(
           p_account_id    => accounts.account_id,
           p_brand_id      => accounts.brand_id,
           p_contract_id   => accounts.contract_id,
           p_company_id    => accounts.company_id,
           p_subscriber_id => accounts.subscriber_id,
           p_contractor_id => accounts.dst_seller_ID,
           p_branch_id     => accounts.branch_id,
           p_agent_id      => accounts.agent_id,
           p_contractor_bank_id => accounts.dst_bank_id,
           p_vat           => accounts.vat,
           p_date_from     => accounts.dst_date_from,
           p_date_to       => accounts.dst_date_to,
           close_prev_profile => accounts.close_prev_profile,
           p_kpp           => accounts.kpp,
           p_erp_code      => accounts.erp_code,
           p_correspondent_bank_id => accounts.CORRESPONDENT_BANK_ID
     );        
     INSERT INTO TMP_CONTRACTOR_CHANGE_20160901(old_profile_id, New_Profile_Id, dt)
     VALUES(v_old_profile_id, v_new_profile_id, SYSDATE);
     
     -- ��������� ������ � ������������ � �������� ������� ������
     UPDATE BILL_T B
        SET B.PROFILE_ID         = v_new_profile_id, 
            B.CONTRACTOR_ID      = p_dst_seller_id, 
            B.CONTRACTOR_BANK_ID = p_dst_bank_id,
            B.BILL_NO            = LPAD(TO_CHAR(v_region_id), 4,'0')||SUBSTR(B.BILL_NO,5)
      WHERE B.REP_PERIOD_ID >= v_period_id
        AND B.ACCOUNT_ID = accounts.account_id
        AND B.PROFILE_ID = v_old_profile_id
        AND B.BILL_TYPE != 'C'
        AND B.BILL_DATE >= accounts.dst_date_from;
     
   END LOOP;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


end PK700_IMPORT_LOTUS_TEST;
/
