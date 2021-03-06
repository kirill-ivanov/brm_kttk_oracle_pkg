CREATE OR REPLACE PACKAGE PK102_RECEIPT_FOR_PAYMENT
IS
    --
    -- ����� ��� ������ ��������� �� ������ ��� ���������� ���
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK102_RECEIPT_FOR_PAYMENT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- ����� ���������� ���� + ������ �������� + ������ �������:
    --   - ��� ������ ���������� ����������
    PROCEDURE kvitok_header( 
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,      -- ID �������� �����            
                   p_period_id  IN INTEGER       -- ID ��������� �������
               );          

    -- �������� ������ ��� ������: "����� �� ������"
    --   - ��� ������ ���������� ����������
    PROCEDURE kvitok_summ( 
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,      -- ID �������� �����
                   p_period_id  IN INTEGER       -- ID ��������� �������
               );
    
    -- �������� ������ ��� ������ ������� ����� (����������)
    --   - ��� ������ ���������� ����������
    PROCEDURE kvitok_invoices( 
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,      -- ID �������� �����
                   p_period_id  IN INTEGER       -- ID ��������� �������
               );

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������� ����� (item-�)
    PROCEDURE kvitok_bill_items( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,     -- ����� �������� �����
         p_rep_period_id IN NUMBER      -- ������ �����         
    );
    
    -- �������� ������ ��� ������ ������� ����������� �������
    --   - ��� ������ ���������� ����������
    PROCEDURE kvitok_detail( 
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,      -- ID �����
                   p_period_id  IN INTEGER       -- ID ��������� �������
               );                       
      
END PK102_RECEIPT_FOR_PAYMENT;
/
CREATE OR REPLACE PACKAGE BODY PK102_RECEIPT_FOR_PAYMENT
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� ���������� ���� + ������ �������� + ������ �������:
--   - ��� ������ ���������� ����������
PROCEDURE kvitok_header( 
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,    -- ID �������� �����            
               p_period_id  IN INTEGER     -- ID ��������� �������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'kvitok_header';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        -- ����� ���������� ���� + ������ �������� + ������ �������:
         SELECT 
               AP.ACCOUNT_ID,                -- ID �/� �������
               A.ACCOUNT_NO,                 -- � �������� �����
               AP.CONTRACTOR_ID,             -- ID ��������
               CR.SHORT_NAME COMPANY_NAME,   -- �������� ��������
               CR.INN COMPANY_INN,           -- ��� ��������
               CB.BANK_NAME,                 -- ����
               CB.BANK_SETTLEMENT,           -- � �/� 
               CB.BANK_CORR_ACCOUNT,         -- � �/�
               CB.BANK_CODE,                 -- ���
               SUB.SUBSCRIBER_ID,            -- ID �������
               SUB.LAST_NAME SUB_LAST_NAME,     -- �.�.�.
               SUB.FIRST_NAME SUB_FIRST_NAME,   -- �.�.�.
               SUB.MIDDLE_NAME SUB_MIDDLE_NAME, -- �.�.�.
               AC.COUNTRY      DLV_ADDR_COUNTRY,
               AC.ZIP          DLV_ADDR_ZIP,
               AC.STATE        DLV_ADDR_STATE,
               AC.CITY         DLV_ADDR_CITY,
               AC.ADDRESS      DLV_ADDR_ADDRESS,
               NULL            DLV_ADDR_PERSON,
               AC.PHONES CLIENT_PHONE,
               CRA.PHONE_BILLING,
               CRA.PHONE_ACCOUNT,                             
               P.PERIOD_FROM, 
               P.PERIOD_TO,               
               A.CURRENCY_ID                 -- ID ������ �����               
          FROM ACCOUNT_T A,
               ACCOUNT_PROFILE_T AP,
               CONTRACTOR_T CR,
               CONTRACTOR_BANK_T CB,
               CONTRACTOR_ADDRESS_T CRA,
               ACCOUNT_CONTACT_T AC,
               SUBSCRIBER_T SUB,
               PERIOD_T P
         WHERE P.PERIOD_ID = p_period_id
           AND A.ACCOUNT_ID = p_account_id
           AND A.ACCOUNT_TYPE = 'P'
           AND AP.ACCOUNT_ID = A.ACCOUNT_ID
           AND AC.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AC.DATE_FROM <= P.PERIOD_TO
           AND SUB.SUBSCRIBER_ID = AP.SUBSCRIBER_ID
           AND (AC.DATE_TO IS NULL OR P.PERIOD_FROM < AC.DATE_TO )
           AND AC.ADDRESS_TYPE = 'DLV'
           AND AP.DATE_FROM <= P.PERIOD_TO 
           AND (AP.DATE_TO IS NULL OR P.PERIOD_FROM < AP.DATE_TO )
           AND CR.CONTRACTOR_ID = AP.CONTRACTOR_ID
           AND CRA.CONTRACTOR_ID = CR.CONTRACTOR_ID
           --AND CB.BANK_ID = AP.CONTRACTOR_BANK_ID
           AND CB.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND CB.DATE_FROM <= P.PERIOD_TO 
           AND (CB.DATE_TO IS NULL OR P.PERIOD_FROM < CB.DATE_TO )
         ;    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������ ��� ������: "����� �� ������"
-- ������� ����� � ������ ������������, ������� ���������, ������ ������������, 
-- ����� ����������� ������ ������
--   - ��� ������ ���������� ����������
PROCEDURE kvitok_summ( 
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,    -- ID �������� �����
               p_period_id  IN INTEGER     -- ID ��������� �������
           )
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'kvitok_summ';
    v_balance         NUMBER; 
    v_open_balance    NUMBER;  -- �������� ������ �� ������ ������� ����������� �����
    v_close_balance   NUMBER;  -- ��������� ������ �� ����� ������� ����������� �����
    v_open_due        NUMBER;  -- ������������� �� ���������� ������
    v_bill_total      NUMBER;  -- ��������� �� ������� ������
    v_recvd           NUMBER;  -- ������� �������� �� ������ �����
    v_last_period_id  INTEGER;
    v_retcode         INTEGER;
    
    v_balance_ac         NUMBER; 
BEGIN
    -- �������� ID ����������� �������
    v_last_period_id := PK04_PERIOD.Make_prev_id(p_period_id);

    -- �������� �������� ������ �������
    SELECT NVL(SUM(CLOSE_BALANCE),0) 
           into v_open_balance
           FROM (  
               SELECT *
                FROM REP_PERIOD_INFO_T
               WHERE rep_period_id <= v_last_period_id 
                AND account_id = p_account_id
        ORDER BY REP_PERIOD_ID DESC)
     WHERE ROWNUM = 1;
     
    -- �������� ����� ���������� �� ������ �� ������ (����� ��������� ����� ���� �������������)
    SELECT NVL(SUM(B.TOTAL),0)
      INTO v_bill_total
      FROM BILL_T B
     WHERE B.ACCOUNT_ID = p_account_id
       AND B.REP_PERIOD_ID = p_period_id;
    -- ����� �������� � ������, �� ������� ��������� ���� 
    SELECT NVL(SUM(P.RECVD),0)
      INTO v_recvd 
      FROM PAYMENT_T P
     WHERE P.ACCOUNT_ID = p_account_id
       AND P.REP_PERIOD_ID = p_period_id;
    

--- add 31.07.2014    
/*    if v_open_balance = 0 and v_bill_total = 0 and v_recvd = 0 then
    
     --   SELECT  AC.BALANCE into v_open_balance  
     --        FROM ACCOUNT_T ac      
     --       WHERE ACCOUNT_ID = p_account_id;
            
        SELECT case 
                   when NVL(SUM(CLOSE_BALANCE),0) < 0 then NVL(SUM(CLOSE_BALANCE),0)
                   else 0
               end 
            INTO v_open_balance
            FROM REP_PERIOD_INFO_T rt 
            WHERE ACCOUNT_ID = p_account_id AND REP_PERIOD_ID = 
                (SELECT max(REP_PERIOD_ID) 
                     FROM REP_PERIOD_INFO_T  
                     WHERE REP_PERIOD_ID < p_period_id
                     AND  ACCOUNT_ID = p_account_id) ;   
             
    end if;
*/    
--- add 31.07.2014       
       
    -- �������������
    v_open_due := -(v_open_balance + v_recvd);

    -- ��������� ������
    v_close_balance := v_open_balance + v_recvd - v_bill_total;
        
    -- ���������� ������
    OPEN p_recordset FOR
         SELECT 
              v_open_balance    OPEN_BALANCE,      -- �����/���� (�������� ������ �� ����������� �������)
              v_recvd           RECVD,             -- ������� �������� �� ������
              v_open_due        OPEN_DUE,          -- ������������� �� ���������� ������
              v_bill_total      BILL_TOTAL,        -- ���������� �� ������ �� ������
              v_close_balance   CLOSE_BALANCE,     -- ������ � ����� �������
              CASE WHEN v_close_balance < 0 THEN  
                   -v_close_balance ELSE 0 END PAY_SUMM  -- � ������ 
           FROM DUAL;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������ ��� ������ ������� ����� (����������)
--   - ��� ������ ���������� ����������
PROCEDURE kvitok_invoices( 
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,   -- ID �������� �����
               p_period_id  IN INTEGER    -- ID ��������� �������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'kvitok_invoices';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT DECODE(BILL_TYPE,2,'�������������. ') || SERVICE_NAME SERVICE_NAME,         
                 SUM (GROSS) GROSS,
                 SUM (TAX) TAX,
                 SUM (TOTAL) TOTAL
            FROM (SELECT S.SERVICE SERVICE_NAME,
                         II.GROSS,
                         II.TAX,
                         II.TOTAL,
                         DECODE (b.bill_type, 'B', 1, 2) BILL_TYPE
                    FROM BILL_T B, INVOICE_ITEM_T II, SERVICE_T S
                   WHERE     B.ACCOUNT_ID = p_account_id
                         AND B.REP_PERIOD_ID = p_period_id
                         AND II.BILL_ID = B.BILL_ID
                         AND II.REP_PERIOD_ID = B.REP_PERIOD_ID
                         AND II.SERVICE_ID = S.SERVICE_ID)
        GROUP BY BILL_TYPE, SERVICE_NAME;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ��������� ������� ����� (item-�)
    PROCEDURE kvitok_bill_items( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,     -- ����� �������� �����
         p_rep_period_id IN NUMBER      -- ������ �����         
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'kvitok_bill_items';
    v_retcode            INTEGER;
begin    
    open p_recordset for
         SELECT 
               b.bill_id,
               b.rep_period_id,
               b.bill_no,
               i.item_id,
               i.item_type item_type_id,
               dict_type.name item_type,
               i.charge_type charge_type_id,
               dict_charge_type.NAME charge_type,
               i.item_total,
               0 adjusted,
               NULL TRANSFERED,
               i.recvd,               
               i.date_from,
               i.date_to,
               i.item_status item_status_id,
               dict_status.name item_status,
               i.service_id,
               S.SERVICE SERVICE_NAME,
               S.SERVICE_CODE SERVICE_KEY,
               S.SERVICE_SHORT SERVICE_NAME_SHORT,
               S.ERP_PRODCODE SERVICE_ERP_CODE,
               i.subservice_id,               
               SS.SUBSERVICE SUBSERVICE_NAME,
               SS.SHORTNAME SUBSERVICE_NAME_SHORT,               
               SS.SUBSERVICE_KEY,
               i.order_id,
               i.TAX_INCL,
               i.NOTES,
               o.order_no
          FROM 
               bill_t b,
               item_t i, 
               service_t s, 
               subservice_t ss,
               dictionary_t dict_type,
               dictionary_t dict_status,              
               dictionary_t dict_charge_type,
               ORDER_T o              
         WHERE b.bill_id = i.bill_id
               AND i.service_id = S.SERVICE_ID(+)
               AND i.ORDER_ID = o.ORDER_ID (+)
               AND i.subservice_id = SS.SUBSERVICE_ID(+)
               AND dict_charge_type.key (+)= i.CHARGE_TYPE
               AND dict_charge_type.PARENT_ID (+)= 7
               AND dict_type.key (+)= i.item_type
               AND DICT_TYPE.PARENT_ID (+)= 5
               AND dict_status.key (+)= i.item_status
               AND dict_status.PARENT_ID (+)= 6
--               AND b.bill_id = p_bill_id
               AND b.account_id = p_account_id
               AND b.rep_period_id = p_rep_period_id;  
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end; 

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- �������� ������ ��� ������ ������� ����������� �������
--   - ��� ������ ���������� ����������
PROCEDURE kvitok_detail( 
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,    -- ID �������� �����
               p_period_id  IN INTEGER     -- ID ��������� �������
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'kvitok_detail';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������ 
    OPEN p_recordset FOR        
          SELECT PREFIX_B,
               TERM_Z_NAME,                 
               CALL_DAY START_TIME,
               CALLS_COUNT,
               MINS_SUM,
               AMOUNT_SUM,
               SUBSERVICE_KEY
            FROM DETAIL_MMTS_T_FIZ
           WHERE rep_period_ID = p_period_id
                 AND account_id = p_account_id
        ORDER BY START_TIME, PREFIX_B;         
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK102_RECEIPT_FOR_PAYMENT;
/
