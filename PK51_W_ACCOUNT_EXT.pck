CREATE OR REPLACE PACKAGE PK51_W_ACCOUNT_EXT
IS
    --
    -- ����� ��� ��������� ������� ������ �� ��
    -- event_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK51_W_ACCOUNT_EXT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

--================================================================================================================
-- ��������� ��������� ���������� �� ���������
    PROCEDURE CONTRACT_INFO( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_contract_id   IN NUMBER    -- ����� ���������
    );
    
--================================================================================================================
-- ��������� ������ ������� �� �������� �����
    PROCEDURE ACCOUNT_ADDRESS_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- ����� �������� �����
         p_date          IN DATE
    ); 

--================================================================================================================
-- ��������� ��� ������� ������� �� �������� �����
    PROCEDURE ACCOUNT_ADDRESS_HISTORY_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- ����� �������� �����
    ); 
    
--================================================================================================================
-- ��������� ������� �� ������� �����
    PROCEDURE ORDER_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- ����� �������� �����
         p_order_id      IN NUMBER     -- ����� �������� �����    
    );
--================================================================================================================
-- ��������� ���������� ��  ������ (��� �������)
    PROCEDURE ORDER_BODY( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_order_id      IN NUMBER    -- ����� ������
    ); 
--================================================================================================================
-- ��������� ������ ������
    PROCEDURE BILL_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- ����� �������� �����
         p_bill_id       IN NUMBER,     -- ����� �����         
         p_rep_period_id IN NUMBER      -- ������ �����                  
    ); 
    
--================================================================================================================
-- ��������� ������� ����� (item-�)
    PROCEDURE BILL_ITEM_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_bill_id       IN NUMBER,    -- ����� �����
         p_rep_period_id IN NUMBER      -- ������ �����                  
    );    

--================================================================================================================
-- ��������� invoice �����
    PROCEDURE BILL_INVOICE_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_bill_id       IN NUMBER,    -- ����� �����
         p_rep_period_id IN NUMBER      -- ������ �����                  
    );

--================================================================================================================
-- ��������� ������ ���������
    PROCEDURE PHONES_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- ����� �������� �����
         p_order_id      IN NUMBER    -- ����� �������� �����         
    );   
    
--================================================================================================================
-- ��������� ������ ��������� � �������� ���������
    PROCEDURE PHONES_LIST_WITH_ADR_SET( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- ����� �������� �����
         p_order_id      IN NUMBER     -- ����� �������� �����         
    );     
 
--================================================================================================================
-- ��������� ������ ��������� (�����������)
    PROCEDURE PHONES_RANGE( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- ����� �������� �����
         p_order_id      IN NUMBER    -- ����� �������� �����    
    ); 
    
--================================================================================================================
-- ��������� ������ �����������
    PROCEDURE SIGNER_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- ����� �������� �����
    );           
    
--================================================================================================================
-- ��������� ������� �������� ����� 
-- ������������ �� �������� ������ ����� ������� ����������
    PROCEDURE ACCOUNT_BALANCE_ONLINE( 
         p_balance_online     OUT NUMBER, 
         p_account_id         IN NUMBER    -- ����� �������� ����� 
    );    
    
--================================================================================================================
-- ��������� ������� �������� ����� �� ������ ��������
-- ����� ��� ������������������
    PROCEDURE ACCOUNT_BALANCE_BY_PHONENUMBER( 
         p_balance        OUT NUMBER, 
         p_phone_number   IN VARCHAR2    -- ����� ����������� ������
    );    
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ������ �������� �� �����������
-- ���������� ���-�� �����������
PROCEDURE Check_phonenumber_cross (
        p_count         OUT INTEGER,
        p_phonenumber   IN VARCHAR2,
        p_date_from     IN DATE DEFAULT SYSDATE
    );
END PK51_W_ACCOUNT_EXT;
/
CREATE OR REPLACE PACKAGE BODY PK51_W_ACCOUNT_EXT
IS

-- �������������� �������� ��������� ��� WEB-����������
-- ��������� ���� � �� �� ��� ��� ������, ��� � ��� �����
--================================================================================================================
-- ��������� ��������� ���������� �� ��������� 
    PROCEDURE CONTRACT_INFO( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_contract_id   IN NUMBER    -- ����� ���������
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'CONTRACT_INFO';
    v_retcode            INTEGER;
    v_sql                VARCHAR2(10000);
    v_date_temp          VARCHAR2(20);
    v_cnt                INTEGER;
begin
    open p_recordset for
          SELECT 
                C.CONTRACT_ID,
                C.CONTRACT_NO,
                C.DATE_FROM,
                C.DATE_TO,
                CL.CLIENT_ID,
                CL.CLIENT_NAME,
                M.MANAGER_ID,
                M.LAST_NAME || ' ' || M.FIRST_NAME || ' ' || M.MIDDLE_NAME MANAGER_NAME,
                C.MARKET_SEGMENT_ID,
                DICT_SEGMENT.NAME MARKET_SEGMENT_NAME,
                C.CLIENT_TYPE_ID,
                dict_client_type.name CLIENT_TYPE_NAME,
                C.XTTK_TYPE_ID,
                NULL XTTK_TYPE_NAME                
            FROM
               CONTRACT_T C,
               CLIENT_T CL,
               SALE_CURATOR_T MI,
               MANAGER_T M,
               (SELECT *
                  FROM DICTIONARY_T
                 WHERE parent_id = 63) dict_segment,
               (SELECT *
                  FROM DICTIONARY_T
                 WHERE parent_id = 64) dict_client_type
        WHERE
            C.CLIENT_ID = CL.CLIENT_ID
            AND C.CONTRACT_ID = MI.CONTRACT_ID(+)
            AND MI.MANAGER_ID = M.MANAGER_ID(+)
            AND dict_segment.key_id(+) = C.MARKET_SEGMENT_ID
            AND dict_client_type.key_id(+) = C.CLIENT_TYPE_ID
            AND C.CONTRACT_ID = p_contract_id;  
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;

--================================================================================================================
-- ��������� ������ ������� �� �������� ����� �� ������������� ����
    PROCEDURE ACCOUNT_ADDRESS_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- ����� �������� �����
         p_date          IN DATE       -- ����, �� ������� ����� �������� ������
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_ADDRESS_LIST';
    v_retcode            INTEGER;
    v_date               DATE;
begin
    IF p_date IS NULL THEN
       v_date := SYSDATE;
    ELSE
       v_date := p_date;
    END IF;  

    open p_recordset for
         SELECT
              AC.CONTACT_ID ADDRESS_ID,
              AC.ADDRESS_TYPE,
              AC.COUNTRY,
              AC.ZIP,
              AC.STATE,
              AC.CITY,
              AC.ADDRESS,
              AC.DATE_FROM,
              AC.DATE_TO,
              AC.PERSON,
              CASE
                WHEN AC.ADDRESS_TYPE='DLV' THEN 0
                  ELSE 1
                  END SORT_ID                  
            FROM
                  ACCOUNT_CONTACT_T AC,
                  ACCOUNT_T ACC
          WHERE ACC.ACCOUNT_ID = AC.ACCOUNT_ID     
              AND ((AC.DATE_TO IS NULL AND TRUNC(AC.DATE_FROM) <= v_date) OR v_date BETWEEN AC.DATE_FROM AND AC.DATE_TO)
              AND ACC.ACCOUNT_ID = p_account_id  
              ORDER BY SORT_ID;
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;
--================================================================================================================
-- ��������� ��� ������� ������� �� �������� �����
    PROCEDURE ACCOUNT_ADDRESS_HISTORY_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- ����� �������� �����
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_ADDRESS_HISTORY_LIST';
    v_retcode            INTEGER;
begin
    open p_recordset for
         SELECT
              AC.CONTACT_ID ADDRESS_ID,
              AC.ADDRESS_TYPE,              
              AC.COUNTRY,
              AC.ZIP,
              AC.STATE,
              AC.CITY,
              AC.ADDRESS,
              AC.DATE_FROM,
              AC.DATE_TO,
              AC.PERSON,
              CASE
                WHEN AC.ADDRESS_TYPE='DLV' THEN 0
                  ELSE 1
                  END SORT_ID                  
            FROM
                  ACCOUNT_CONTACT_T AC,
                  ACCOUNT_T ACC
          WHERE ACC.ACCOUNT_ID = AC.ACCOUNT_ID     
              AND ACC.ACCOUNT_ID = p_account_id  
              ORDER BY SORT_ID, DATE_FROM, DATE_TO;
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;

--================================================================================================================
-- ��������� ������� �� ������� �����
    PROCEDURE ORDER_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- ����� �������� �����
         p_order_id      IN NUMBER    -- ����� �������� �����         
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ORDER_LIST';
    v_retcode            INTEGER;
    v_sql                VARCHAR2(10000);    
begin
    v_sql := 'SELECT 
                 a.account_id,
                 a.account_no,
                 o.order_id,
                 o.order_no,
                 o.date_from,
                 CASE WHEN o.DATE_TO >= TO_DATE (''01.01.2050'', ''DD.MM.YYYY'') THEN NULL
                    ELSE o.DATE_TO END DATE_TO,
                 o.service_id,
                 S.SERVICE_CODE SERVICE_KEY,
                 S.SERVICE SERVICE_NAME,
                 S.SERVICE SERVICE_NAME_SHORT,
                 S.ERP_PRODCODE SERVICE_ERP_CODE,
                 O.RATEPLAN_ID,
                 TRF.RATEPLAN_NAME,
                 TRF.RATESYSTEM_ID RATEPLAN_SYSTEM_ID,
                 TRF.RATEPLAN_CODE RATEPLAN_CODE,
                 ord_ph.CNT PHONE_COUNT,
                 o.STATUS,
                 ol.LOCK_TYPE_ID,
                 ol.LOCK_TYPE_NAME,
                 ol.LOCK_REASON,
                 ol.LOCKED_BY
            FROM 
                 account_t a,
                 order_t o,
                 service_t s,
                 rateplan_t trf,
                 (  SELECT order_Id, COUNT (phone_number) CNT
                      FROM ORDER_PHONES_T
                  GROUP BY ORDER_ID) ord_ph,
                  (
                 SELECT ord_lock.order_id, ORD_LOCK.LOCK_TYPE_ID, ORD_LOCK.LOCK_REASON,ORD_LOCK.LOCKED_BY, DICT.NAME LOCK_TYPE_NAME
                   FROM ORDER_LOCK_T ord_lock, dictionary_t dict
                  WHERE (SYSDATE BETWEEN ORD_LOCK.DATE_FROM AND ORD_LOCK.DATE_TO OR (ORD_LOCK.DATE_FROM<=SYSDATE AND ORD_LOCK.DATE_TO IS NULL))
                        AND DICT.PARENT_ID = 9
                        AND DICT.KEY_ID = ORD_LOCK.LOCK_TYPE_ID) ol
           WHERE a.ACCOUNT_ID = o.account_Id
                 AND O.SERVICE_ID = S.SERVICE_ID(+)
                 AND o.order_id = ORD_PH.ORDER_ID(+)
                 AND o.rateplan_id = TRF.RATEPLAN_ID (+)
                 AND O.ORDER_ID = ol.ORDER_ID(+)';

    IF p_account_id IS NOT NULL THEN 
          v_sql := v_sql || ' AND o.account_id = ' || p_account_id;
     ELSE
          v_sql := v_sql || ' AND o.order_id = ' || p_order_id;       
     END IF;
     
     open p_recordset for v_sql;
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;    
--================================================================================================================
-- ��������� ���������� ��  ������ (��� �������)
    PROCEDURE ORDER_BODY( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_order_id      IN NUMBER    -- ����� ������
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ORDER_BODY';
    v_retcode            INTEGER;
begin
    open p_recordset for
         SELECT 
                 OB.ORDER_BODY_ID,
                 OB.ORDER_ID,
                 O.ORDER_NO,
                 OB.SUBSERVICE_ID,
                 SS.SUBSERVICE SUBSERVICE_NAME,
                 SS.SHORTNAME SUBSERVICE_NAME_SHORT,                 
                 SS.SUBSERVICE_KEY,
                 OB.CHARGE_TYPE CHARGE_TYPE_ID,
                 DICT.NAME CHARGE_TYPE,
                 ob.date_from,
                 CASE
                     WHEN ob.DATE_TO >= TO_DATE ('01.01.2050', 'DD.MM.YYYY') THEN NULL
                            ELSE ob.DATE_TO
                         END DATE_TO
            FROM order_t o, 
                 order_body_t ob, 
                 subservice_t ss,
                 dictionary_t dict
           WHERE     o.order_id = ob.ORDER_ID
                 AND OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
                 AND dict.key (+)= ob.charge_type
                 AND dict.parent_id (+)= 7
                 AND ob.order_id = p_order_id;  
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;     
--================================================================================================================
-- ��������� ������ ������
    PROCEDURE BILL_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,     -- ����� �������� �����
         p_bill_id       IN NUMBER,     -- ����� �����         
         p_rep_period_id IN NUMBER      -- ������ �����
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'BILL_LIST';
    v_retcode            INTEGER;
    v_sql                VARCHAR2(10000);
begin
    v_sql := 'SELECT b.bill_id,
                   b.rep_period_id,
                   b.bill_no,
                   b.account_id,
                   a.account_no,
                   b.bill_date,
                   b.paid_to,
                   B.BILL_TYPE bill_type_id,
                   dic_type.name bill_type,
                   B.BILL_STATUS bill_status_id,
                   dic_status.name bill_status,
                   b.currency_id,
                   b.total,
                   b.recvd,
                   b.due,
                   b.due_date,
                   per_dictionary.position bill_type_position
              FROM bill_t b,
                   account_t a,
                   dictionary_t dic_type,
                   dictionary_t dic_status,
                   period_t per_dictionary
             WHERE A.ACCOUNT_ID = B.ACCOUNT_ID
                   AND B.REP_PERIOD_ID = PER_DICTIONARY.PERIOD_ID (+)
                   AND DIC_TYPE.KEY(+) = B.BILL_TYPE
                   AND dic_type.parent_id(+) = 3
                   AND DIC_status.KEY(+) = B.BILL_STATUS
                   AND dic_status.parent_id(+) = 4';
    IF p_account_id IS NOT NULL THEN
      v_sql := v_sql || ' AND A.ACCOUNT_ID = ' || p_account_id;      
    ELSE       
      v_sql := v_sql || ' AND B.bill_id = ' || p_bill_id || ' AND B.rep_period_id = ' || p_rep_period_id;          
    END IF;
    
    v_sql := v_sql || ' ORDER BY b.rep_period_id desc';
    
    open p_recordset for v_sql;         
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;      
    
--================================================================================================================
-- ��������� ������� ����� (item-�)
    PROCEDURE BILL_ITEM_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_bill_id       IN NUMBER,     -- ����� �����
         p_rep_period_id IN NUMBER      -- ������ �����         
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'BILL_ITEM_LIST';
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
               NULL TRANSFERED,--i.transfered,
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
               SS.SUBSERVICE_KEY
          FROM 
               bill_t b,
               item_t i, 
               service_t s, 
               subservice_t ss,
               dictionary_t dict_type,
               dictionary_t dict_status,              
               dictionary_t dict_charge_type               
         WHERE b.bill_id = i.bill_id
               AND i.service_id = S.SERVICE_ID(+)
               AND i.subservice_id = SS.SUBSERVICE_ID(+)
               AND dict_charge_type.key (+)= i.CHARGE_TYPE
               AND dict_charge_type.PARENT_ID (+)= 7
               AND dict_type.key (+)= i.item_type
               AND DICT_TYPE.PARENT_ID (+)= 5
               AND dict_status.key (+)= i.item_status
               AND dict_status.PARENT_ID (+)= 6
               AND b.bill_id = p_bill_id
               AND b.rep_period_id = p_rep_period_id;  
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;    

--================================================================================================================
-- ��������� invoice �����
    PROCEDURE BILL_INVOICE_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_bill_id       IN NUMBER,    -- ����� �����
         p_rep_period_id IN NUMBER     -- ������ �����
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'BILL_INVOICE_LIST';
    v_retcode            INTEGER;
begin
    open p_recordset for
         SELECT 
               INV.INV_ITEM_ID,
               INV.INV_ITEM_NAME,         
               INV.DATE_FROM,
               INV.DATE_TO,
               INV.SERVICE_ID,         
               S.SERVICE_CODE SERVICE_KEY,
               S.SERVICE SERVICE_NAME ,
               S.SERVICE_SHORT SERVICE_NAME_SHORT,
               S.ERP_PRODCODE SERVICE_ERP_CODE,
               INV.VAT,         
               INV.TAX,
               INV.GROSS,
               INV.TOTAL,
               INV.BILL_ID,
               B.BILL_NO
          FROM 
              invoice_item_t inv, 
              bill_t b, 
              service_t s
         WHERE B.BILL_ID = INV.BILL_ID
               AND S.SERVICE_ID(+) = INV.SERVICE_ID
               AND inv.bill_id = p_bill_id
               AND b.rep_period_id = p_rep_period_id 
         ORDER BY INV_ITEM_NO;
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;   

--================================================================================================================
-- ��������� ������ ���������
    PROCEDURE PHONES_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- ����� �������� �����
         p_order_id      IN NUMBER     -- ����� �������� �����         
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'PHONES_LIST';
    v_retcode            INTEGER;
begin
    IF p_account_id IS NOT NULL THEN    
        open p_recordset for
            SELECT op.*
              FROM order_phones_t op, order_t ord
             WHERE op.order_id = ord.order_id
                   AND account_id = p_account_id
                   AND ((op.DATE_TO IS NULL AND TRUNC(op.DATE_FROM) < SYSDATE) OR SYSDATE BETWEEN TRUNC(op.DATE_FROM) AND op.DATE_TO)
                   ;
     ELSE
        open p_recordset for
             SELECT *
                FROM order_phones_t op
             WHERE op.order_id = p_order_id
                   AND ((op.DATE_TO IS NULL AND TRUNC(op.DATE_FROM) < SYSDATE) OR SYSDATE BETWEEN TRUNC(op.DATE_FROM) AND op.DATE_TO)
                   ; 

     END IF;               
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end; 

--================================================================================================================
-- ��������� ������ ��������� � �������� ���������
    PROCEDURE PHONES_LIST_WITH_ADR_SET( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- ����� �������� �����
         p_order_id      IN NUMBER     -- ����� �������� �����         
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'PHONES_LIST_WITH_ADR_SET';
    v_retcode            INTEGER;
begin
    IF p_account_id IS NOT NULL THEN    
        open p_recordset for
            SELECT op.*, ph_adr.*
              FROM order_phones_t op, order_t ord, phone_address_t ph_adr 
             WHERE op.order_id = ord.order_id
                   AND ph_adr.address_id (+)= op.address_id
                   AND PH_ADR.ADDRESS_TYPE (+)= 'SET'
                   AND account_id = p_account_id
                   AND ((op.DATE_TO IS NULL AND op.DATE_FROM < SYSDATE) OR SYSDATE BETWEEN op.DATE_FROM AND op.DATE_TO)
                   ;
     ELSE
        open p_recordset for
             SELECT op.*, ph_adr.*
                FROM order_phones_t op, phone_address_t ph_adr 
               WHERE op.order_id = p_order_id
                     AND ph_adr.address_id (+)= op.address_id
                     AND PH_ADR.ADDRESS_TYPE (+)= 'SET'
                     AND ((op.DATE_TO IS NULL AND op.DATE_FROM < SYSDATE) OR SYSDATE BETWEEN op.DATE_FROM AND op.DATE_TO)
                     ; 

     END IF;               
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;  

--================================================================================================================
-- ��������� ������ ��������� (�����������)
    PROCEDURE PHONES_RANGE( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- ����� �������� �����
         p_order_id      IN NUMBER    -- ����� �������� �����    
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'PHONES_RANGE';
    v_retcode            INTEGER;
begin
    IF p_account_id IS NOT NULL THEN    
       open p_recordset for
          SELECT PHONE_FROM, DECODE(PHONE_TO, PHONE_FROM, NULL, PHONE_TO) PHONE_TO
            FROM (
            SELECT MIN(REPLACE(PHONE_NUMBER,'KH_','')) PHONE_FROM, MAX(REPLACE(PHONE_NUMBER,'KH_','')) PHONE_TO 
              FROM ORDER_PHONES_T op
             WHERE 1=1 
               AND (SYSDATE BETWEEN DATE_FROM AND DATE_TO OR DATE_TO IS NULL)
               AND exists (
                   select * from order_t ord
                   where ord.order_id = op.order_id
                   and account_id = p_account_id                    
               )
            GROUP BY (REPLACE(PHONE_NUMBER,'KH_','') - ROWNUM + 1)
        ) ORDER BY PHONE_TO;   
     ELSE
       open p_recordset for
          SELECT PHONE_FROM, DECODE(PHONE_TO, PHONE_FROM, NULL, PHONE_TO) PHONE_TO
            FROM (
            SELECT MIN(REPLACE(PHONE_NUMBER,'KH_','')) PHONE_FROM, MAX(REPLACE(PHONE_NUMBER,'KH_','')) PHONE_TO 
              FROM ORDER_PHONES_T op
             WHERE 1=1 
               AND SYSDATE BETWEEN DATE_FROM AND DATE_TO
               AND ORDER_ID = p_order_id
            GROUP BY (REPLACE(PHONE_NUMBER,'KH_','') - ROWNUM + 1)
        ) ORDER BY PHONE_TO;   

     END IF;                       
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;

--================================================================================================================
-- ��������� ������ �����������
    PROCEDURE SIGNER_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- ����� �������� ����� 
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'SIGNER_LIST';
    v_retcode            INTEGER;
begin    
    open p_recordset for
         SELECT 
               S.SIGNER_ID,
               S.SIGNER_ROLE_ID,
               S.SIGNER_ROLE,
               S.SIGNER_NAME,
               S.ATTORNEY_NO,
               S.MANAGER_ID,
               S.DATE_FROM,
               S.DATE_TO,
               S.CONTRACTOR_ID,
               C.CONTRACTOR CONTRACTOR_NAME,
               S.PRIORITY
          FROM 
                SIGNER_T S, 
                ACCOUNT_PROFILE_T AP,
                CONTRACTOR_T C
         WHERE AP.CONTRACTOR_ID = S.CONTRACTOR_ID
               AND C.CONTRACTOR_ID = S.CONTRACTOR_ID
               AND (S.DATE_FROM <= SYSDATE AND (S.DATE_TO >= SYSDATE OR S.DATE_TO IS NULL))
               AND AP.ACCOUNT_ID = p_account_id     
               ORDER BY PRIORITY ;  
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;   

--================================================================================================================
-- ��������� ������� �������� ����� 
-- ������������ �� �������� ������ ����� ������� ����������
    PROCEDURE ACCOUNT_BALANCE_ONLINE( 
         p_balance_online     OUT NUMBER, 
         p_account_id         IN NUMBER    -- ����� �������� ����� 
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_BALANCE_ONLINE';
    v_retcode            INTEGER;
    v_balance            NUMBER;
    v_total_summ         NUMBER;
    
    v_date_from          DATE;
    v_date_to            DATE;
    
begin    
      --����� ������� ������
      SELECT BALANCE INTO v_balance
             FROM ACCOUNT_T
          WHERE ACCOUNT_ID = p_account_id;

      -- ������� ����� ������� ����������
      v_date_from := TRUNC(SYSDATE,'MM');
      v_date_to := LAST_DAY (TRUNC(SYSDATE,'MM')) + INTERVAL '00 23:59:59' DAY TO SECOND;

       SELECT 
             NVL(SUM(AMOUNT),0) INTO v_total_summ
        FROM 
             E04_BDR_MMTS_T e 
       WHERE 
           BDR_STATUS = 0
           AND e.rep_period BETWEEN v_date_from AND v_date_to
           AND e.account_id = p_account_id;

      p_balance_online := v_balance - v_total_summ;                              
exception
   WHEN OTHERS THEN  
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;  

--================================================================================================================
-- ��������� ������� �������� ����� �� ������ ��������
-- ����� ��� ������������������
    PROCEDURE ACCOUNT_BALANCE_BY_PHONENUMBER( 
         p_balance        OUT NUMBER, 
         p_phone_number   IN VARCHAR2    -- ����� ����������� ������
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_BALANCE_BY_PHONENUMBER';
    v_retcode            INTEGER;
begin    
      SELECT A.BALANCE INTO p_balance
          FROM account_t a, order_t o, order_phones_t op
         WHERE a.account_id = o.account_id
               AND o.order_id = op.order_id
               AND op.phone_number = p_phone_number
               AND (
                   (SYSDATE BETWEEN op.DATE_FROM AND op.DATE_TO)
                    OR (op.DATE_FROM <= SYSDATE AND op.DATE_TO IS NULL))
               AND ROWNUM=1;                          
exception   
   WHEN OTHERS THEN  
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;    

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ������ �������� �� �����������
-- ���������� ���-�� �����������
PROCEDURE Check_phonenumber_cross (
        p_count         OUT INTEGER,
        p_phonenumber   IN VARCHAR2,
        p_date_from     IN DATE DEFAULT SYSDATE
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_phonenumber_cross';
    v_cnt        INTEGER;
BEGIN
    SELECT COUNT (*) INTO v_cnt
      FROM order_phones_t
       WHERE 
              phone_number = p_phonenumber
              AND p_date_from BETWEEN DATE_FROM AND DATE_TO;
    p_count:= v_cnt;
EXCEPTION   -- ��� ������ ���������� ����������
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_phonenumber='||p_phonenumber||                                    
                                    c_PkgName||'.'||v_prcName );
END;

END PK51_W_ACCOUNT_EXT;
/
