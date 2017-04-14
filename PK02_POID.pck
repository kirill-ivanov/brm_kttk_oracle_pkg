CREATE OR REPLACE PACKAGE PK02_POID
IS
    -- ==============================================================================
    c_PkgName   CONSTANT varchar2(30) := 'PK02_POID';
    
-- ========================================================================= --
-- ������ (����������)
-- ========================================================================= --
FUNCTION Next_client_id RETURN INTEGER;
FUNCTION Next_customer_id RETURN INTEGER;
FUNCTION Next_subscriber_id RETURN INTEGER;

-- ========================================================================= --
-- ��������
-- ========================================================================= --
FUNCTION Next_contractor_id RETURN INTEGER;
FUNCTION Next_bank_id RETURN INTEGER;
FUNCTION Next_signer_id RETURN INTEGER;
FUNCTION Next_signature_id RETURN INTEGER;
FUNCTION Next_manager_id RETURN INTEGER;

-- ========================================================================= --
-- ������� ���� + �����
-- ========================================================================= --
FUNCTION Next_contract_id RETURN INTEGER;
FUNCTION Next_account_id RETURN INTEGER;
FUNCTION Next_account_profile_id RETURN INTEGER;
FUNCTION Next_order_id RETURN INTEGER;
FUNCTION Next_order_body_id RETURN INTEGER;
FUNCTION Next_address_id RETURN INTEGER;
FUNCTION Next_rateplan_id RETURN INTEGER;

-- ========================================================================= --
-- �����
-- ========================================================================= --
FUNCTION Next_bill_id RETURN INTEGER;
FUNCTION Next_item_id RETURN INTEGER;
FUNCTION Next_invoice_item_id RETURN INTEGER;
FUNCTION Next_transfer_id RETURN INTEGER;

-- ========================================================================= --
-- �������
-- ========================================================================= --
FUNCTION Next_Payment_Doc_Id(p_ps_id	IN NUMBER DEFAULT 12 ) RETURN VARCHAR2;
FUNCTION Next_payment_id RETURN INTEGER;

-- ========================================================================= --
-- ������
-- ========================================================================= --
FUNCTION Next_downtime_id RETURN INTEGER;
FUNCTION Next_ccad_bdr_id RETURN INTEGER;
FUNCTION Next_operation_id RETURN INTEGER;


END PK02_POID;
/
CREATE OR REPLACE PACKAGE BODY PK02_POID
IS
-- ========================================================================= --
-- �������
-- ========================================================================= --
FUNCTION Next_Payment_Doc_Id(				
	p_ps_id			IN NUMBER DEFAULT 12
	) RETURN VARCHAR2 IS
	RESULT		VARCHAR2(100);
BEGIN
	SELECT lpad(nvl(p_ps_id, 12), 2, '0') || '-' || sq_payment_doc_id.nextval || '-' || to_char(sysdate, 'ddmmyyhh24miss')
		INTO result 
	FROM dual;
  RETURN(result);
END Next_Payment_Doc_Id;

FUNCTION Next_payment_id RETURN INTEGER IS
BEGIN
    RETURN SQ_PAYMENT_ID.NEXTVAL; 
END;

-- ========================================================================= --
-- ������ (����������)
-- ========================================================================= --
FUNCTION Next_client_id RETURN INTEGER IS
BEGIN
    RETURN SQ_CLIENT_ID.NEXTVAL; 
END;

FUNCTION Next_customer_id RETURN INTEGER IS
BEGIN
    RETURN SQ_CLIENT_ID.NEXTVAL; 
END;

FUNCTION Next_subscriber_id RETURN INTEGER IS
BEGIN
    RETURN SQ_CLIENT_ID.NEXTVAL; 
END;

-- ========================================================================= --
-- ��������
-- ========================================================================= --
FUNCTION Next_contractor_id RETURN INTEGER IS
BEGIN
    RETURN SQ_CLIENT_ID.NEXTVAL; 
END;

FUNCTION Next_bank_id RETURN INTEGER IS
BEGIN
    RETURN SQ_POOL_ID.NEXTVAL; 
END;

FUNCTION Next_signer_id RETURN INTEGER IS
BEGIN
    RETURN SQ_POOL_ID.NEXTVAL; 
END;

FUNCTION Next_signature_id RETURN INTEGER IS
BEGIN
    RETURN SQ_POOL_ID.NEXTVAL; 
END;

FUNCTION Next_picture_id RETURN INTEGER IS
BEGIN
    RETURN SQ_PICTURE_ID.NEXTVAL; 
END;

FUNCTION Next_manager_id RETURN INTEGER IS
BEGIN
    RETURN SQ_MANAGER_ID.NEXTVAL; 
END;

-- ========================================================================= --
-- ������� ���� + �����
-- ========================================================================= --
FUNCTION Next_contract_id RETURN INTEGER IS
BEGIN
    RETURN SQ_CONTRACT_ID.NEXTVAL; 
END;

FUNCTION Next_account_id RETURN INTEGER IS
BEGIN
    RETURN SQ_ACCOUNT_ID.NEXTVAL; 
END;

FUNCTION Next_account_profile_id RETURN INTEGER IS
BEGIN
    RETURN SQ_ACCOUNT_ID.NEXTVAL; 
END;

FUNCTION Next_order_id RETURN INTEGER IS
BEGIN
    RETURN SQ_ORDER_ID.NEXTVAL; 
END;

FUNCTION Next_order_body_id RETURN INTEGER IS
BEGIN
    RETURN SQ_ORDER_ID.NEXTVAL; 
END;

FUNCTION Next_address_id RETURN INTEGER IS
BEGIN
    RETURN SQ_ADDRESS_ID.NEXTVAL; 
END;

FUNCTION Next_rateplan_id RETURN INTEGER IS
BEGIN
    RETURN SQ_RATEPLAN_ID.NEXTVAL; 
END;

-- ========================================================================= --
-- �����
-- ========================================================================= --
FUNCTION Next_bill_id RETURN INTEGER IS
BEGIN
    RETURN SQ_BILL_ID.NEXTVAL; 
END;

FUNCTION Next_item_id RETURN INTEGER IS
BEGIN
    RETURN SQ_ITEM_ID.NEXTVAL; 
END;

FUNCTION Next_invoice_item_id RETURN INTEGER IS
BEGIN
    RETURN SQ_INVOICE_ITEM_ID.NEXTVAL; 
END;

-- ========================================================================= --
-- �������
-- ========================================================================= --
FUNCTION Next_transfer_id RETURN INTEGER IS
BEGIN
    RETURN SQ_TRANSFER_ID.NEXTVAL; 
END;

-- ========================================================================= --
-- ������
-- ========================================================================= --
FUNCTION Next_downtime_id RETURN INTEGER IS
BEGIN
    RETURN SQ_DOWNTIME_ID.NEXTVAL; 
END;

FUNCTION Next_ccad_bdr_id RETURN INTEGER IS
BEGIN
    RETURN SQ_CCAD_BDR_ID.NEXTVAL; 
END;

FUNCTION Next_operation_id RETURN INTEGER IS
BEGIN
    RETURN SQ_OPERATION_T.NEXTVAL; 
END;



END PK02_POID;
/
