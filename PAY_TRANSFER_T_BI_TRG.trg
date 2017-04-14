CREATE OR REPLACE TRIGGER PAY_TRANSFER_T_BI_TRG
BEFORE INSERT OR UPDATE
ON PIN.PAY_TRANSFER_T
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
c_tg_name     CONSTANT VARCHAR2(30) := 'PAY_TRANSFER_T_BI_TRG';
/******************************************************************************
��������� ��������� ���������� �������������� ������ 
��������� �������� ������ ����� ����������������
******************************************************************************/
   v_period_id  INTEGER;
BEGIN
   IF :NEW.PERIOD_ID IS NULL THEN
      -- ���� ������ ����������� ������ �������� ������ ����, ���� ��� � ��������
      SELECT PERIOD_ID 
        INTO v_period_id
        FROM ( 
          SELECT P.PERIOD_ID 
            FROM PERIOD_T P
           WHERE P.POSITION IN ('OPEN','BILL')             
           ORDER BY P.PERIOD_ID
       )
       WHERE ROWNUM = 1
      ;  
      :NEW.PERIOD_ID := v_period_id;
   END IF;

   -- �� ������ ������ ����������� ���������
   :NEW.TRANSFER_DATE := SYSDATE;
   
END INV_ITEM_T_BU_TRG;
/
