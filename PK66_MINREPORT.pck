CREATE OR REPLACE PACKAGE PK66_MINREPORT
IS
    --
    -- ����� ��� ��������� ������� ������ �� ��
    -- event_t
    --
    -- ==============================================================================
    c_PkgName   CONSTANT varchar2(30) := 'PK66_MINREPORT';
    -- ==============================================================================
    c_RET_OK    CONSTANT integer := 0;
    c_RET_ER    CONSTANT integer :=-1;
    
    TYPE t_refc IS REF CURSOR;

    -- --------------------------------------------------------------------------------- --
    -- �������� ���������� ��������
    -- --------------------------------------------------------------------------------- --
    FUNCTION Get_sales_curator (
               p_branch_id     IN INTEGER,
               p_agent_id      IN INTEGER,
               p_contract_id   IN INTEGER,
               p_account_id    IN INTEGER,
               p_order_id      IN INTEGER,
               p_date          IN DATE
             ) RETURN VARCHAR2;

    -- ��������� ����������� �� BDR (�����������)
    PROCEDURE REP_DETAIL_BDR( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_date_from     IN  DATE,      -- ������ � ( >= )
         p_date_to       IN  DATE,      -- ������ �� ( < )
         p_order_no      IN VARCHAR2,     -- ����� ������ (�� ������� ����������)
         p_phone_number  IN VARCHAR2      -- ����� �������� (�� ������� ����������)
    );
    
    -- ��������� ������ ��������� � ����������� �� �������� � �.�.
    PROCEDURE REP_BY_ACCOUNT_JUR( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_has_open_order  IN  NUMBER
    );
    
    -- ������������ ������ ���-���, ������� �������� ��� ��������� ��������  
    PROCEDURE REP_BY_ACCOUNT_FIZ( 
         p_result           OUT VARCHAR2, 
         p_recordset        OUT t_refc, 
         p_contractor_id    IN  VARCHAR2,
         p_has_open_order   IN  NUMBER
    );
    
    -- ��������� ������ ������, ������� ���������� ������� �� ������������ ������
    PROCEDURE REP_BY_BILL_JUR( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER,
         p_bill_type       IN  VARCHAR2
    );
 
    -- ��������� ������ ������ �� �������, ������� ���������� ������� �� ������������ ������
    PROCEDURE REP_BY_ORDER_JUR( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER
    );
   
-- ��������� ������ ������, ������� ���������� ������� �� ������������ ������
    PROCEDURE REP_BY_BILL_WITH_TYPE_JUR( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER
    );
        
-- ��������� ������ ������ �� ���������� �����, ������� ���������� ������� �� ������������ ������
    PROCEDURE REP_BY_BILL_FIZ( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER,
         p_bill_type       IN  VARCHAR2
    );    

-- ����� �� �������� email
    PROCEDURE REP_EMAIL_SENDING( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER,
         p_result_type     IN  VARCHAR2       -- null - ����� ���, 'OK' - ������ ��������, 'ERROR' - � �������, 'NOTSEND'- �� ������������
    );

-- ����� �� ������������ ������ ��� �������
    PROCEDURE REP_TENZOR_GENERATE( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER,
         p_result_type     IN  VARCHAR2       -- null - ����� ���, 'OK' - ������ ��������, 'ERROR' - � �������, 'NOTSEND'- �� ������������
    );

-- ����������� ����������� �� ���
    PROCEDURE REP_CDR_SPB( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_order_id        IN NUMBER,
         p_date_from       IN DATE,
         p_date_to         IN DATE
    );
-- ������ ������� � CDR �� �������� ������� ������ (��/��)
    PROCEDURE REP_FIND_CDR_BY_ORDER_NO( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_order_no        IN  VARCHAR2,
         p_date_from       IN DATE,
         p_date_to         IN DATE
    );  
    
-- ������ ������� � BDR �� �������� ������� ������ (��/��)
    PROCEDURE REP_FIND_BDR_BY_ORDER_NO( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_order_no        IN  VARCHAR2,
         p_date_from       IN DATE,
         p_date_to         IN DATE
    );  
    
-- ����� �� ��������� ������ ������
    PROCEDURE REP_ORDER_AUDIT( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_order_no        IN  VARCHAR2,
         p_order_id        IN  NUMBER
    );         
    
-- ����� �� ��������� ������ ������� �������� (����� ���� �� ������ ������, ���� �� ������ ��������)
    PROCEDURE REP_ORDER_PHONE_AUDIT( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_order_no        IN  VARCHAR2,
         p_order_id        IN  NUMBER,
         p_phone_number    IN  VARCHAR2
    ); 
    
--=====================================================================================
-- ��������� ��������� � ����� ��������� ����� (20.03.2015)
-- ������ �������, ������� �������� ��� ����� ��    
    PROCEDURE REP_ORDER_TRF_CHANGE( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2
    );
    
-- ������ ���������, ������� �������� � ����������
    PROCEDURE REP_CONTRACT_EXCL_TRF_CHANGE( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2
    );    
    
--==============================================================================================================
-- ������ �������, ������������� ���������� ��� "�������������"
    PROCEDURE REP_ORDER_AUDIT_RETRF( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_date_from       IN  DATE,
         p_date_to         IN  DATE
    );
--==============================================================================================================
-- ���������� �� ���
    PROCEDURE REP_STAT_EDO( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc
    );   
    
-- ���������� �� ������ ��������� PonyExpress
    PROCEDURE REP_STAT_PONYEXPRESS( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc,
         p_period_id       IN  NUMBER
    );    
    
-- ��������� ������ �� ������-��������
  PROCEDURE REPORT_PAY_ONLINE( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_date_from         DATE,
          p_date_to           DATE,
          p_region_id         INTEGER,
          p_ps_id             INTEGER,
          p_only_error        INTEGER
  ); 
  
--=======================================================================================================
-- ����� "����� ������" �� ������������ ������ �� ������������� ���������� (������������� ����� �� ��� ���������)
PROCEDURE REP_BY_BOOK_SALE( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER
);                      

--=======================================================================================================
-- ����� �� ������ (������������� ����� �� ��� ���������)
PROCEDURE REP_BY_BILL( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER
); 

--=======================================================================================================
-- ����� �� ������ (������������� ����� �� ��� ���������). ������ ���������� ��������������.
-- ������������ ������� �.
PROCEDURE REP_BY_BILL_AGENT_VIEW( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER
); 

-- ------------------------------------------------------------------------------
-- 1) BRM KTTK. ����� �� ������������ ������ (��. ����)
-- ------------------------------------------------------------------------------
-- ��������� ������ �� ������-��������
PROCEDURE REP_BY_BILL_NEW( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_contractor_id     VARCHAR2,
          p_period_id         INTEGER
  );
  
-- -------------------------------------------------------------------------
-- 2) BRM KTTK. ����� �� ������������ ������ (��. ����) [�� �������]
-- -------------------------------------------------------------------------
PROCEDURE REP_BY_BILL_ORDERS( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_contractor_id     VARCHAR2,
          p_period_id         INTEGER
  );

-- ���������� �� �������� � �� �� ������ ��. ���, ������� ������������ ����� ������-����
PROCEDURE REP_AKKORD_POST_Y_BY_REGION( 
     p_result          OUT VARCHAR2, 
     p_recordset       OUT t_refc, 
     p_job_id            IN  NUMBER
);

-- ���������� �� �������� � �� �� ������ ���. ���, ������� ������������ ����� ������-����
PROCEDURE REP_AKKORD_POST_F_BY_REGION( 
     p_result          OUT VARCHAR2, 
     p_recordset       OUT t_refc, 
     p_job_id          IN  NUMBER
);

-- ------------------------------------------------------------------------------
-- ������ �� �������� �������
-- ����� ��������   
-- ������ (�������� �� ����/��� ��� ����)
-- ��� �/� (���./��. ����)
-- ����� ������
-- ���� ������ �������� ������
-- ������ ������ (����� ��������: ��������,���������������, �����, ��������)
-- ���� ������ �������� �������� ������� ������
-- ����� ��������
-- �������� ���� (��������)
PROCEDURE REP_BY_PHONE_LIST( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_contractor_id     VARCHAR2
);

-- ����� ����� �� ��. ����� ���
    PROCEDURE REP_CSS_BY_JUR( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc
    );

-- ����� ����� �� ���. ����� ���
    PROCEDURE REP_CSS_BY_FIZ( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc
    );
    
-- -------------------------------------------------------------------------  
-- ����� �� ���������������� ������ �� ������
-- -------------------------------------------------------------------------
PROCEDURE REP_BILL_DEBET_CREDIT(
               p_message           OUT VARCHAR2,                
               p_recordset         OUT t_refc,
               p_contractor_id   IN  VARCHAR2,
               p_period_id         IN INTEGER
           );
    
-- ����� �� ���������� ��������
    PROCEDURE REP_BILL_QUEUE_LOG( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc,
         p_date_from       IN  DATE,
         p_date_to         IN  DATE,
         p_is_process      IN  NUMBER
    );    

-- -------------------------------------------------------------------------------------
-- ����� �� ������� ��� �.������
-- -------------------------------------------------------------------------------------
PROCEDURE REP_ORDERS_FOR_LAPIN( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_contractor_id     IN  VARCHAR2
  );
  
-- -------------------------------------------------------------------------------------
-- ����� �� ������������� �������� ��� ������������� �.�.
-- ��������� ������ ��������� � �������������
-- ����������� ��������� � ����������
-- -------------------------------------------------------------------------------------
PROCEDURE REP_RP_FOR_MIKHAYLOVSKY( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_period_id         IN INTEGER
  );

--=================================================================================
-- �������� �� ��������� ������ � ��������
-- ��������� ������, � ������� �� ��������� ������������ ��������
PROCEDURE CHECK_BILLS_BY_COMPANY( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_contractor_id     VARCHAR2,
          p_period_id_from    INTEGER,
          p_period_id_to      INTEGER
  );

--=================================================================================
-- �������� �� ������ ���� � ���
PROCEDURE CHECK_CFO_IS_NULL( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_contractor_id     VARCHAR2,
          is_only_order_open  INTEGER,           --0/1
          is_only_cfo_null    INTEGER,           --0/1
          is_only_live        INTEGER            --������ ����� ������, �.�. ��, ������� ������� ����� 01/01/2016 � ���� ���������� �� 2016 ���
  );

--=================================================================================
-- �������� �� ������ ���� � ��� (� ���������)
PROCEDURE CHECK_CFU_IS_NULL( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_contractor_id     VARCHAR2,
          is_has_order_open   INTEGER,           --0/1
          is_only_cfu_null    INTEGER            --0/1
  );

--======================================================================
-- ��������� ������ �����
PROCEDURE REPORT_SERVICE_LIST( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc
  );

--=========================================================================
-- ����������� �� ������ � ��������������� ��������
-- ������ ��� ��������� �������
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� �� �������� BRM
--
PROCEDURE REP_TTK_ABONENTS( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc
  );

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� �� ����������� ��������� BRM
--
PROCEDURE REP_TTK_BILL_ITEMS(
          p_message         OUT VARCHAR2, 
          p_recordset       OUT t_refc,
          p_from_period_id  IN INTEGER,
          p_to_period_id    IN INTEGER
  );
--======================================================================
-- ��������� ������ ������, � ������� �� ���������� ERP_CODE � �������
PROCEDURE REP_BILL_WITH_KKODE_NULL( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_period_id         IN  NUMBER
);

--======================================================================
-- ��������� ������ ������, ������� ����� �������� � ����� � ��������� �������
PROCEDURE REP_BILL_FOR_EISUP( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_period_id         IN  NUMBER
);

END PK66_MINREPORT;
/
CREATE OR REPLACE PACKAGE BODY PK66_MINREPORT
IS

-- --------------------------------------------------------------------------------- --
-- �������� ���������� ��������
-- --------------------------------------------------------------------------------- --
FUNCTION Get_sales_curator (
           p_branch_id     IN INTEGER,
           p_agent_id      IN INTEGER,
           p_contract_id   IN INTEGER,
           p_account_id    IN INTEGER,
           p_order_id      IN INTEGER,
           p_date          IN DATE
         ) RETURN VARCHAR2
IS
    v_mgr VARCHAR2(300);
BEGIN
      SELECT TRIM(
             LAST_NAME||' '||
             SUBSTR(UPPER(FIRST_NAME),1,1)||DECODE(FIRST_NAME,NULL,'','.')||
             SUBSTR(UPPER(MIDDLE_NAME),1,1)||DECODE(MIDDLE_NAME,NULL,'','.')
             ) MGR_NAME
        INTO v_mgr
        FROM (
          SELECT M.LAST_NAME, M.FIRST_NAME, M.MIDDLE_NAME,
                 CASE 
                   WHEN SC.CONTRACTOR_ID = p_branch_id THEN 1
                   WHEN SC.CONTRACTOR_ID = p_agent_id  THEN 2
                   WHEN SC.CONTRACT_ID   IS NOT NULL   THEN 3
                   WHEN SC.ACCOUNT_ID    IS NOT NULL   THEN 4
                   WHEN SC.ORDER_ID      IS NOT NULL   THEN 5
                   ELSE 0
                 END  WT
            FROM SALE_CURATOR_T SC, MANAGER_T M
           WHERE M.MANAGER_ID = SC.MANAGER_ID
             AND NVL(p_date,SYSDATE) BETWEEN SC.DATE_FROM AND NVL(SC.DATE_TO,SYSDATE) 
             AND (SC.CONTRACTOR_ID = p_branch_id   OR
                  SC.CONTRACTOR_ID = p_agent_id    OR
                  SC.CONTRACT_ID   = p_contract_id OR 
                  SC.ACCOUNT_ID    = p_account_id  OR 
                  SC.ORDER_ID      = p_order_id )
          ORDER BY WT DESC
      )
      WHERE ROWNUM = 1
    ;  
    RETURN v_mgr;
EXCEPTION 
  WHEN NO_DATA_FOUND THEN
    RETURN NULL;
END;



    -- ��������� ����������� �� BDR (�����������)
    PROCEDURE REP_DETAIL_BDR( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_date_from     IN  DATE,      -- ������ � ( >= )
         p_date_to       IN  DATE,      -- ������ �� ( < )
         p_order_no      IN VARCHAR2,     -- ����� ������ (�� ������� ����������)
         p_phone_number  IN VARCHAR2     -- ����� �������� (�� ������� ����������)
    )IS
    BEGIN
      OPEN p_recordset FOR
/*          ������ ����������
          ����������� ������������ ���������� (�� CDR)
          ����� ����������� ��������
          ����� ����������� ��������
          ����� �������� ����� � ��������
          ����� ������
          ������
          �������� ����
          �������� ����
          ��� ������
          ������������, ���
          ���������, ���*/
        SELECT 
             t.*,
             r.rateplan_name TRF_NAME FROM   (
          SELECT start_time,
                 duration,
                 abn_a,
                 abn_b,
                 account_no,
                 order_no,
                 subservice,
                 trf_code,
                 term_z_name,
                 subservice_id,
                 bill_minutes,
                 amount,
                 bdr_status, -- id ���������� �����������
                 trf_status  -- �������� ���������� ����������������
            FROM (
                  SELECT b.start_time,
                         b.duration,
                         b.abn_a,
                         b.abn_b,
                         a.account_no,
                         b.order_no,
                         ss.subservice,
                         b.trf_code,
                         b.term_z_name,
                         ss.subservice_id,
                         b.bill_minutes,
                         b.amount,
                         b.bdr_status,
                         NVL(d.NAME,'������ �� ����������') trf_status,
                         row_number() OVER (PARTITION BY b.cdr_id ORDER BY rep_period DESC) rn
                    FROM BDR_VOICE_T b,
                         account_t a,
                         subservice_t ss,
                         (SELECT external_id bdr_status, NAME
                            FROM DICTIONARY_T
                           START WITH KEY = 'TRF_STAT' 
                         CONNECT BY PRIOR key_id = parent_id
                         ) d 
                   WHERE b.subservice_id = ss.subservice_id 
                     AND b.account_id = a.account_id
                     AND b.bdr_status = d.bdr_status(+)
                     AND b.rep_period >= TRUNC(p_date_from,'mm')
                     AND b.start_time BETWEEN p_date_from AND p_date_to
                     AND b.order_no = p_order_no
                     AND (b.abn_a = p_phone_number OR p_phone_number IS NULL)
                 )
           WHERE rn = 1
      ) t, 
        rateplan_t r
      WHERE t.trf_code = r.rateplan_code(+);          
          
    END;
    
    -- ��������� ������ ��������� � ����������� �� �������� � �.�.
    PROCEDURE REP_BY_ACCOUNT_JUR( 
         p_result           OUT VARCHAR2, 
         p_recordset        OUT t_refc, 
         p_contractor_id    IN  VARCHAR2,
         p_has_open_order   IN  NUMBER
    )IS
    v_sql                VARCHAR2(10000);    
    BEGIN
      v_sql := '
           WITH 
              AD AS (
                 SELECT *
                     FROM account_documents_t
                   WHERE doc_calls IS NOT NULL AND delivery_method_id = 6501),
              AD_BILL AS (
                  SELECT acd.account_id,ACD.DELIVERY_METHOD_ID,DD.NAME DELIVERY_METHOD_NAME
                     FROM account_documents_t acd, dictionary_t dd
                    WHERE acd.delivery_method_id = DD.KEY_ID 
                       and  acd.doc_bill = ''Y''),
               AC AS (SELECT *
                     FROM account_contact_t
                    WHERE ADDRESS_TYPE = ''DLV''),
               ACJ AS (SELECT *
                     FROM account_contact_t
                    WHERE ADDRESS_TYPE = ''JUR'')
          SELECT contr.contract_no,
                 contr.date_from contract_date_from,
                 contr.date_to contract_date_to,
                 a.account_no,
                 cus.company_name CUSTOMER,
                 ap.date_from,
                 ap.date_to,
                 br.contractor BRANCH,
                 AG.CONTRACTOR AGENT,
                 AD_BILL.DELIVERY_METHOD_NAME,
                 AD.DOC_CALLS,
                 ACJ.ZIP||'' ''||ACJ.STATE||'' ''||ACJ.CITY||'' ''||ACJ.ADDRESS ADDR_JUR,
                 AC.ZIP||'' ''||AC.STATE||'' ''||AC.CITY||'' ''||AC.ADDRESS ADDR_DLV,
                 ac.email,
                 AC.PHONES,
                 cus.inn,
                 cus.erp_code,
                 provider.contractor,
                 A.COMMENTARY,
                 CFO.NOTES CFO,
                 TSFU.NOTES TSFU                
            FROM ACCOUNT_PROFILE_T ap,
                 account_t a,
                 company_t cus,
                 contract_t contr,
                 contractor_t br,
                 contractor_t ag,
                 contractor_t provider,
                 ad,
                 ac,
                 acj,
                 AD_BILL,
                 DICTIONARY_T CFO,
                 DICTIONARY_T TSFU
           WHERE ap.account_id = a.account_id
                 AND ap.contract_id = cus.contract_id
                 AND ap.contract_id = contr.contract_id
                 AND ap.contractor_id = provider.contractor_id(+)
                 AND ap.branch_id = br.contractor_id(+)                 
                 AND ap.agent_id = ag.contractor_id(+)
                 AND ad.account_id(+) = a.account_id
                 AND ac.account_id(+) = ap.account_id
                 AND acj.account_id(+) = ap.account_id
                 AND AD_BILL.account_id (+) = ap.account_id 
                 AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
                 AND SYSDATE BETWEEN ap.DATE_FROM AND NVL(ap.Date_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                 AND SYSDATE BETWEEN cus.DATE_FROM AND NVL(cus.Date_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                 AND a.account_type = ''J''
                 AND A.STATUS <> ''T''
                 AND CONTR.CFO_ID = CFO.KEY_ID (+)
                 AND CONTR.CFU_ID = TSFU.KEY_ID (+)'; 

      IF p_has_open_order = 1 THEN
         v_sql:= v_sql || 
                 ' and exists (
                     select * from order_t oo
                     where SYSDATE between date_from and date_to
                     and oo.account_id = a.account_id
                 )';
      END IF;
      
      OPEN p_recordset FOR 
           v_sql;
    END;   
  
-- ������������ ������ ���-���, ������� �������� ��� ��������� ��������  
    PROCEDURE REP_BY_ACCOUNT_FIZ( 
         p_result           OUT VARCHAR2, 
         p_recordset        OUT t_refc, 
         p_contractor_id    IN  VARCHAR2,
         p_has_open_order   IN  NUMBER
    )IS
v_sql                VARCHAR2(10000);    
    BEGIN
      v_sql := '
           WITH 
              AD_BILL AS (
                  SELECT acd.account_id,ACD.DELIVERY_METHOD_ID,DD.NAME DELIVERY_METHOD_NAME
                     FROM account_documents_t acd, dictionary_t dd
                    WHERE acd.delivery_method_id = DD.KEY_ID 
                       and  acd.doc_bill = ''Y''),
               AC AS (SELECT *
                     FROM account_contact_t
                    WHERE ADDRESS_TYPE = ''DLV'')
          SELECT contr.contract_no,
                 a.account_no,
                 S.LAST_NAME || '' '' || S.FIRST_NAME || '' '' || S.MIDDLE_NAME FIO,
                 ap.date_from,
                 ap.date_to,
                 br.contractor BRANCH,
                 AG.CONTRACTOR AGENT,
                 AD_BILL.DELIVERY_METHOD_NAME,
                 ac.email                 
            FROM ACCOUNT_PROFILE_T ap,
                 account_t a,
                 contract_t contr,
                 contractor_t br,
                 contractor_t ag,
                 ac,
                 AD_BILL,
                 subscriber_t s
           WHERE ap.account_id = a.account_id                 
                 AND ap.SUBSCRIBER_ID = s.SUBSCRIBER_ID
                 AND ap.contract_id = contr.contract_id
                 AND ap.branch_id = br.contractor_id(+)
                 AND ap.agent_id = ag.contractor_id(+)
                 AND ad_bill.account_id(+) = a.account_id
                 AND ac.account_id(+) = ap.account_id
                 AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
                 AND a.account_type = ''P''
                 AND A.STATUS <> ''T'''; 

      IF p_has_open_order = 1 THEN
         v_sql:= v_sql || 
                 ' and exists (
                     select * from order_t oo
                     where SYSDATE between date_from and date_to
                     and oo.account_id = a.account_id
                 )';
      END IF;
      
     OPEN p_recordset FOR 
           v_sql;
    END;   
    
    
-- ��������� ������ ������, ������� ���������� ������� �� ������������ ������
    PROCEDURE REP_BY_BILL_JUR( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER,
         p_bill_type       IN  VARCHAR2
    )IS
         v_sql                VARCHAR2(10000);    
    BEGIN
      IF p_period_id IS NULL THEN
         p_result := '������� �������� ������';
      ELSE
	          v_sql := '
              WITH AD_BILL AS (
                  SELECT 
                           acd.account_id,ACD.DELIVERY_METHOD_ID,DD.NAME DELIVERY_METHOD_NAME
                      FROM 
                           account_documents_t acd, 
                           dictionary_t dd
                    WHERE 
                          acd.delivery_method_id = DD.KEY_ID 
                          and  acd.doc_bill = ''Y''),
                AC AS (
                    SELECT * FROM ACCOUNT_CONTACT_T
                    WHERE ADDRESS_TYPE = ''DLV''
                ),
                ADR_JUR AS (
                    select * from account_contact_t
                    where address_type = ''JUR''
                )
               SELECT CUS.COMPANY_NAME CUSTOMER,
                     C.CONTRACT_NO,
                     B.BILL_NO,
                     B.BILL_DATE,
                     D_BILL_TYPE.NAME || '' ('' || B.BILL_TYPE || '')'' BILL_TYPE,
                     B.GROSS,
                     B.TAX,
                     B.TOTAL,
                     CUR.CURRENCY_NAME CURRENCY,
                     AD_BILL.DELIVERY_METHOD_NAME,
                     BR.CONTRACTOR REGION,
                     AG.CONTRACTOR AGENT,
                     PK112_print.address_to_string(AC.ZIP,AC.STATE,AC.CITY,AC.ADDRESS) DLV_ADDRESS,
                     PK112_print.address_to_string(ADR_JUR.ZIP,ADR_JUR.STATE,ADR_JUR.CITY,ADR_JUR.ADDRESS) JUR_ADDRESS,
                     AC.PHONES,
                     AC.FAX
                FROM ACCOUNT_PROFILE_T AP,
                     ACCOUNT_T A,
                     COMPANY_T CUS,
                     CONTRACT_T C,
                     BILL_T B,
                     DICTIONARY_T D_BILL_TYPE,
                     CURRENCY_T CUR,
                     AD_BILL,
                     CONTRACTOR_T BR,
                     CONTRACTOR_T AG,
                     AC,
                     ADR_JUR
               WHERE b.ACCOUNT_ID = A.ACCOUNT_ID
                     AND B.PROFILE_ID = AP.PROFILE_ID
                     AND b.CONTRACT_ID = C.CONTRACT_ID
                     AND AP.CONTRACT_ID = CUS.CONTRACT_ID
                     AND b.ACCOUNT_ID = A.ACCOUNT_ID
                     AND D_BILL_TYPE.PARENT_ID = 3
                     AND d_bill_type.key = B.BILL_TYPE
                     AND CUR.CURRENCY_ID = B.CURRENCY_ID
                     AND AC.ACCOUNT_ID = A.ACCOUNT_ID
                     AND A.ACCOUNT_TYPE = ''J''
                     AND A.STATUS <> ''T''
                     AND BR.CONTRACTOR_ID (+)= AP.BRANCH_ID 
                     AND AG.CONTRACTOR_ID (+)= AP.AGENT_ID                      
                     AND B.BILL_STATUS IN (''READY'',''CLOSED'')
                     AND AD_BILL.ACCOUNT_ID (+)= AP.ACCOUNT_ID
                     AND ADR_JUR.ACCOUNT_ID (+)= AP.ACCOUNT_ID
                     AND B.BILL_DATE BETWEEN cus.DATE_FROM AND NVL(cus.Date_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                     AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
                     AND B.REP_PERIOD_ID = '|| p_period_id; 
           IF  p_bill_type IS NOT NULL AND  p_bill_type <> '-1' THEN
              IF p_bill_type = 'B' THEN
                v_sql := v_sql || ' AND B.BILL_TYPE = ''B'' ';
              END IF;
           END IF;
          
         OPEN p_recordset FOR 
               v_sql;
      END IF;
    END; 
    
    
    -- ��������� ������ ������ �� �������, ������� ���������� ������� �� ������������ ������
    PROCEDURE REP_BY_ORDER_JUR( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER
    )IS
         v_sql                VARCHAR2(10000);    
    BEGIN
      IF p_period_id IS NULL THEN
         p_result := '������� �������� ������';
      ELSE
            v_sql := '
            SELECT   cus.company_name CUSTOMER,
                     c.contract_no,
                     o.order_no,
                     b.bill_no,
                     B.REP_PERIOD_ID
                FROM contract_t c,
                     company_t cus,
                     account_profile_t ap,
                     bill_t b,
                     order_t o,
                     account_t a
               WHERE ap.contract_id = c.contract_id
                     AND ap.contract_id = cus.contract_id
                     AND ap.account_id = a.account_id
                     AND b.account_id = ap.account_id
                     AND o.account_id = ap.account_id
                     AND A.ACCOUNT_TYPE = ''J''
                     AND A.STATUS <> ''T''
                     AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
                     AND SYSDATE BETWEEN ap.DATE_FROM AND NVL(ap.Date_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))                     
                     AND SYSDATE BETWEEN cus.DATE_FROM AND NVL(cus.Date_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                     AND B.REP_PERIOD_ID = '|| p_period_id ||
            ' ORDER BY o.order_no';
            
         OPEN p_recordset FOR 
               v_sql;
      END IF;
    END; 
    
-- ��������� ������ ������, ������� ���������� ������� �� ������������ ������
    PROCEDURE REP_BY_BILL_WITH_TYPE_JUR( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER
    )IS
         v_sql                VARCHAR2(10000);    
    BEGIN
      IF p_period_id IS NULL THEN
         p_result := '������� �������� ������';
      ELSE
            v_sql := '
              WITH AD_BILL
                   AS (SELECT acd.account_id,
                              ACD.DELIVERY_METHOD_ID,
                              DD.NAME DELIVERY_METHOD_NAME
                         FROM account_documents_t acd, dictionary_t dd
                        WHERE acd.delivery_method_id = DD.KEY_ID AND acd.doc_bill = ''Y''),
                   AC AS (SELECT *
                         FROM ACCOUNT_CONTACT_T
                        WHERE ADDRESS_TYPE = ''DLV''),
                   TP_CL AS (SELECT *
                         FROM DICTIONARY_T
                        WHERE PARENT_ID = 64),
                   SGM_R AS (SELECT *
                         FROM DICTIONARY_T
                        WHERE PARENT_ID = 63)
              SELECT CUS.COMPANY_NAME CUSTOMER,
                     C.CONTRACT_NO,
                     B.BILL_NO,
                     BR.CONTRACTOR REGION,
                     AG.CONTRACTOR AGENT,
                     TP_CL.NAME SGM_R,
                     SGM_R.NAME TP_CLIENT
                FROM ACCOUNT_PROFILE_T AP,
                     ACCOUNT_T A,
                     COMPANY_T CUS,
                     CONTRACT_T C,
                     BILL_T B,
                     DICTIONARY_T D_BILL_TYPE,
                     CURRENCY_T CUR,
                     AD_BILL,
                     CONTRACTOR_T BR,
                     CONTRACTOR_T AG,
                     TP_CL,
                     SGM_R,
                     AC 
               WHERE AP.ACCOUNT_ID = A.ACCOUNT_ID
                     AND AP.CONTRACT_ID = C.CONTRACT_ID
                     AND AP.CONTRACT_ID = CUS.CONTRACT_ID
                     AND b.ACCOUNT_ID = A.ACCOUNT_ID
                     AND D_BILL_TYPE.PARENT_ID = 3
                     AND d_bill_type.key = B.BILL_TYPE
                     AND CUR.CURRENCY_ID = B.CURRENCY_ID
                     AND AC.ACCOUNT_ID = A.ACCOUNT_ID
                     AND A.ACCOUNT_TYPE = ''J''
                     AND A.STATUS <> ''T''
                     AND BR.CONTRACTOR_ID(+) = AP.BRANCH_ID
                     AND AG.CONTRACTOR_ID(+) = AP.AGENT_ID
                     AND TP_CL.KEY_ID(+) = C.CLIENT_TYPE_ID
                     AND SGM_R.KEY_ID(+)=C.MARKET_SEGMENT_ID
                     AND B.BILL_STATUS IN (''READY'', ''CLOSED'')
                     AND AD_BILL.ACCOUNT_ID(+) = AP.ACCOUNT_ID
                     AND B.BILL_DATE BETWEEN cus.DATE_FROM AND NVL(cus.Date_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                     AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
                     AND B.REP_PERIOD_ID = '|| p_period_id; 
          
         OPEN p_recordset FOR 
               v_sql;
      END IF;
    END;     
    
-- ��������� ������ ������ �� ���������� �����, ������� ���������� ������� �� ������������ ������
    PROCEDURE REP_BY_BILL_FIZ( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER,
         p_bill_type       IN  VARCHAR2
    )IS
         v_sql                VARCHAR2(10000);    
    BEGIN
      IF p_period_id IS NULL THEN
         p_result := '������� �������� ������';
      ELSE
            v_sql := '
             SELECT SUB.LAST_NAME || '' '' || SUB.FIRST_NAME || '' '' || SUB.MIDDLE_NAME SUBSCRIBER_FIO,
                     C.CONTRACT_NO,
                     A.ACCOUNT_NO,
                     B.BILL_NO,
                     B.BILL_DATE,
                     D_BILL_TYPE.NAME || '' ('' || B.BILL_TYPE || '')'' BILL_TYPE,
                     B.GROSS,
                     B.TAX,
                     B.TOTAL,
                     CUR.CURRENCY_NAME CURRENCY,
                     BR.CONTRACTOR REGION,
                     AG.CONTRACTOR AGENT
                FROM ACCOUNT_PROFILE_T AP,
                     ACCOUNT_T A,
                     SUBSCRIBER_T SUB,
                     CONTRACT_T C,
                     BILL_T B,
                     DICTIONARY_T D_BILL_TYPE,
                     CURRENCY_T CUR,
                     CONTRACTOR_T BR,
                     CONTRACTOR_T AG
               WHERE AP.ACCOUNT_ID = A.ACCOUNT_ID
                     AND AP.CONTRACT_ID = C.CONTRACT_ID
                     AND AP.SUBSCRIBER_ID = SUB.SUBSCRIBER_ID
                     AND b.ACCOUNT_ID = A.ACCOUNT_ID
                     AND D_BILL_TYPE.PARENT_ID = 3
                     AND d_bill_type.key = B.BILL_TYPE
                     AND CUR.CURRENCY_ID = B.CURRENCY_ID
                     AND A.ACCOUNT_TYPE = ''P''
                     AND A.STATUS <> ''T''
                     AND BR.CONTRACTOR_ID (+)= AP.BRANCH_ID
                     AND AG.CONTRACTOR_ID (+)= AP.AGENT_ID
                     AND B.BILL_STATUS IN (''READY'',''CLOSED'')                     
                     AND B.TOTAL > 0
                     AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
                     AND B.REP_PERIOD_ID = '|| p_period_id;          
         OPEN p_recordset FOR 
               v_sql;
      END IF;
    END;    
    
-- ����� �� email-�������� ������ ����������� ��� 
    PROCEDURE REP_EMAIL_SENDING( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER,
         p_result_type     IN  VARCHAR2       -- null - ����� ���, 'OK' - ������ ��������, 'ERROR' - � �������, 'NOTSEND'- �� ������������
    )IS
        v_sql                VARCHAR2(10000);    
    BEGIN 
       IF p_period_id IS NULL THEN
         p_result := '������� �������� ������';
      ELSE
            v_sql := '               
               SELECT *
                    FROM (SELECT CUS.COMPANY_NAME CUSTOMER,
                                 cont.contract_no,
                                 b.bill_no,
                                 B.BILL_DATE,
                                 B.BILL_TYPE,
                                 b.gross,
                                 B.TAX,
                                 B.TOTAL,
                                 CUR.CURRENCY_NAME,
                                 PRINT_DATE,
                                 PRINT_STATUS,
                                 t.NOTES,
                                 JOB_NAME,
                                 ROW_NUMBER ()
                                    OVER (PARTITION BY t.bill_id ORDER BY t.print_date DESC)
                                    rn,
                                 BR.CONTRACTOR BRANCH_NAME,
                                 ag.contractor AGENT_NAME
                            FROM account_t a,
                                 ACCOUNT_PROFILE_T ap,
                                 bill_t b,
                                 company_t cus,
                                 contract_t cont,
                                 CURRENCY_T cur,
                                 contractor_t br,
                                 contractor_t ag,
                                 (SELECT bp.*,
                                         BPJ.JOB_NAME,
                                         BPJ.BILL_ALL,
                                         BPJ.BILL_OK,
                                         BPJ.BILL_ERROR
                                    FROM bill_print_t bp, bill_print_job_t bpj
                                   WHERE  bp.job_id = bpj.job_id 
                                         AND bp.rep_period_id = ' || p_period_id ||
                                       '  AND job_type = ''EMAIL''
                                         AND DELIVERY_ID = 6501
                                         AND UPPER (job_name) NOT LIKE ''%TEST%''
                                         AND job_status = ''OK''
                                         AND bpj.job_id = bp.job_id) t
                           WHERE     b.account_id = ap.account_Id
                                 AND a.account_id = ap.account_id
                                 AND a.account_type = ''J''
                                 AND A.STATUS <> ''T''
                                 AND ap.branch_id = br.CONTRACTOR_ID (+)
                                 AND ap.agent_id = ag.CONTRACTOR_ID (+)
                                 AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id IN ('|| p_contractor_id ||'))
                                 AND b.bill_id = t.bill_id(+)
                                 AND b.rep_period_id = t.rep_period_id(+)
                                 AND b.rep_period_id = ' || p_period_id ||
                               '  AND B.BILL_TYPE = ''B''
                                 AND B.BILL_STATUS IN (''READY'', ''CLOSED'')';
                  
               IF p_result_type IS NOT NULL THEN
                  IF p_result_type = 'OK' THEN
                     v_sql := v_sql || ' AND PRINT_STATUS = ''OK''';
                  ELSIF p_result_type = 'ERROR' THEN
                     v_sql := v_sql || ' AND PRINT_STATUS = ''ERROR''';
                  ELSIF p_result_type = 'NOTSEND' THEN
                     v_sql := v_sql || ' AND PRINT_STATUS IS NULL';
                  END IF;
               END IF;                                                
                  v_sql:= v_sql || '  AND CUS.CONTRACT_ID = AP.CONTRACT_ID
                                 AND B.BILL_DATE BETWEEN cus.DATE_FROM AND NVL(cus.Date_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                                 AND CONT.CONTRACT_ID = AP.CONTRACT_ID
                                 AND cur.currency_id(+) = B.CURRENCY_ID)
                   WHERE (rn = 1 OR job_name IS NULL)';
                   
          OPEN p_recordset FOR 
              v_sql;
        END IF;
    END;  
    
-- ����� �� ������������ ������ ��� �������
    PROCEDURE REP_TENZOR_GENERATE( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER,
         p_result_type     IN  VARCHAR2       -- null - ����� ���, 'OK' - ������ ��������, 'ERROR' - � �������, 'NOTSEND'- �� ������������
    )IS
        v_sql                VARCHAR2(10000);    
    BEGIN 
       IF p_period_id IS NULL THEN
         p_result := '������� �������� ������';
      ELSE
            v_sql := '               
               SELECT *
                    FROM (SELECT CUS.COMPANY_NAME CUSTOMER,
                                 cont.contract_no,
                                 b.bill_no,
                                 B.BILL_DATE,
                                 B.BILL_TYPE,
                                 b.gross,
                                 B.TAX,
                                 B.TOTAL,
                                 CUR.CURRENCY_NAME,
                                 PRINT_DATE,
                                 PRINT_STATUS,
                                 t.NOTES,
                                 JOB_NAME,
                                 ROW_NUMBER ()
                                    OVER (PARTITION BY t.bill_id ORDER BY t.print_date DESC)
                                    rn,
                                 BR.CONTRACTOR BRANCH_NAME,
                                 ag.contractor AGENT_NAME
                            FROM account_t a,
                                 ACCOUNT_PROFILE_T ap,
                                 bill_t b,
                                 company_t cus,
                                 contract_t cont,
                                 CURRENCY_T cur,
                                 contractor_t br,
                                 contractor_t ag,
                                 (SELECT bp.*,
                                         BPJ.JOB_NAME,
                                         BPJ.BILL_ALL,
                                         BPJ.BILL_OK,
                                         BPJ.BILL_ERROR
                                    FROM bill_print_t bp, bill_print_job_t bpj
                                   WHERE  bp.job_id = bpj.job_id 
                                         AND bp.rep_period_id = ' || p_period_id ||
                                       ' AND DELIVERY_ID = 6515
                                         AND UPPER (job_name) NOT LIKE ''%TEST%''
                                         AND job_status = ''OK''
                                         AND bpj.job_id = bp.job_id) t
                           WHERE     b.account_id = ap.account_Id
                                 AND a.account_id = ap.account_id
                                 AND a.account_type = ''J''
                                 AND ap.branch_id = br.CONTRACTOR_ID (+)
                                 AND ap.agent_id = ag.CONTRACTOR_ID (+)
                                 AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id IN ('|| p_contractor_id ||'))
                                 AND b.bill_id = t.bill_id(+)
                                 AND b.rep_period_id = t.rep_period_id(+)
                                 AND b.rep_period_id = ' || p_period_id ||
                               '  AND B.BILL_TYPE = ''B''
                                 AND B.BILL_STATUS IN (''READY'', ''CLOSED'')';
                  
               IF p_result_type IS NOT NULL THEN
                  IF p_result_type = 'OK' THEN
                     v_sql := v_sql || ' AND PRINT_STATUS = ''OK''';
                  ELSIF p_result_type = 'ERROR' THEN
                     v_sql := v_sql || ' AND PRINT_STATUS IN (''ERROR'',''WARNING'')';
                  ELSIF p_result_type = 'WARNING' THEN
                     v_sql := v_sql || ' AND PRINT_STATUS = ''WARNING''';
                  ELSIF p_result_type = 'NOTSEND' THEN
                     v_sql := v_sql || ' AND PRINT_STATUS IS NULL';
                  END IF;
               END IF;                                                
                  v_sql:= v_sql || '  AND CUS.CONTRACT_ID = AP.CONTRACT_ID
                                 AND B.BILL_DATE BETWEEN cus.DATE_FROM AND NVL(cus.Date_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                                 AND CONT.CONTRACT_ID = AP.CONTRACT_ID
                                 AND cur.currency_id(+) = B.CURRENCY_ID)
                   WHERE (rn = 1 OR job_name IS NULL)';
                   
          OPEN p_recordset FOR 
              v_sql;
        END IF;
    END;      
    
-- ����������� ����������� �� ���
    PROCEDURE REP_CDR_SPB( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_order_id        IN  NUMBER,
         p_date_from       IN DATE,
         p_date_to         IN DATE
    )IS
         v_date_from       DATE;
         v_date_to         DATE;
         v_bill_id         number;
         v_service_id      NUMBER;
    BEGIN                
         SELECT --MAX(rep_period_id),
                i.bill_id,
                i.service_id,
               (CASE WHEN MAX(rep_period_id) = pk00_const.Get_Period_Id(p_date_from) THEN TRUNC(p_date_from,'mm')
                     ELSE TO_DATE(TO_CHAR(MAX(rep_period_id)),'YYYYMM')
                END) period_from,     
               (CASE WHEN MAX(rep_period_id) = pk00_const.Get_Period_Id(p_date_from) 
                     THEN LAST_DAY(TRUNC(p_date_from,'mm'))+INTERVAL '0 23:59:59' DAY TO SECOND
                     ELSE TO_DATE(TO_CHAR(MAX(rep_period_id)),'YYYYMM')+INTERVAL '0 23:59:59' DAY TO SECOND
                END) period_to INTO v_bill_id, v_service_id, v_date_from, v_date_to                        
          FROM item_t i
         WHERE i.order_id = p_order_id
           AND i.rep_period_id >= pk00_const.Get_Period_Id(TRUNC(p_date_from,'mm'))
           AND i.external_id is not null
           AND i.charge_type = 'USG'    
           AND p_date_from BETWEEN TRUNC(date_from,'mm') AND LAST_DAY(TRUNC(date_to,'mm'))+INTERVAL '0 23:59:59' DAY TO SECOND
         GROUP BY i.bill_id, i.SERVICE_ID;  

         IF v_service_id = 7 THEN
               OPEN p_recordset FOR               
                  SELECT /*+ parallel(b 5) */
                             CASE 
                               WHEN SUBSTR(B.ABN_A,1,1) = '7' THEN '8'||SUBSTR(B.ABN_A,2) 
                                ELSE B.ABN_A 
                             END ABN_A,                
                             b.LOCAL_TIME,
                             CASE 
                               WHEN SUBSTR(B.ABN_B,1,1) = '7' THEN '8'||SUBSTR(B.ABN_B,2) 
                                ELSE B.ABN_B 
                             END ABN_B, 
                             CASE 
                               WHEN SUBSTR(B.PREFIX_B,1,1) = '7' THEN '8'||SUBSTR(B.PREFIX_B,2) 
                                ELSE B.PREFIX_B 
                             END prefix_b,               
                             B.TERM_Z_NAME Z_NAME,                 
                             B.DURATION,
                             b.AMOUNT
                        FROM BDR_OPER_T b
                       WHERE B.ITEM_ID IS NOT NULL
                             AND b.bdr_status = 0
                             AND B.AMOUNT > 0
                             AND B.rep_period BETWEEN NVL(v_date_from, p_date_from) AND NVL(v_date_to,p_date_to)
                             AND B.LOCAL_TIME BETWEEN p_date_from AND p_date_to
                             AND trf_type IN (1, 2, 5)                       
                             AND b.ORDER_ID = p_order_id
                             AND b.bill_id = v_bill_id
                   ORDER BY b.LOCAL_TIME;
          ELSIF v_service_id = 140 THEN
                 OPEN p_recordset FOR               
                  SELECT /*+ parallel(b 5) */
                             CASE 
                               WHEN SUBSTR(B.ABN_A,1,1) = '7' THEN '8'||SUBSTR(B.ABN_A,2) 
                                ELSE B.ABN_A 
                             END ABN_A,                
                             b.LOCAL_TIME,
                             CASE 
                               WHEN SUBSTR(B.ABN_B,1,1) = '7' THEN '8'||SUBSTR(B.ABN_B,2) 
                                ELSE B.ABN_B 
                             END ABN_B, 
                             CASE 
                               WHEN SUBSTR(B.PREFIX_B,1,1) = '7' THEN '8'||SUBSTR(B.PREFIX_B,2) 
                                ELSE B.PREFIX_B 
                             END prefix_b,               
                             B.TERM_Z_NAME Z_NAME,                 
                             B.DURATION,
                             b.AMOUNT
                        FROM BDR_VOICE_T b
                       WHERE B.ITEM_ID IS NOT NULL
                             AND b.bdr_status = 0
                             AND B.AMOUNT > 0
                             AND B.rep_period BETWEEN NVL(v_date_from, p_date_from) AND NVL(v_date_to,p_date_to)
                             AND B.LOCAL_TIME BETWEEN p_date_from AND p_date_to
                             AND b.ORDER_ID = p_order_id
                             AND b.bill_id = v_bill_id
                   ORDER BY b.LOCAL_TIME;
          END IF;
    END;  
    
-- ������ ������� � CDR �� �������� ������� ������ (��/��)
    PROCEDURE REP_FIND_CDR_BY_ORDER_NO( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_order_no        IN  VARCHAR2,
         p_date_from       IN DATE,
         p_date_to         IN DATE
    )IS
         v_date_from  DATE;
         v_date_to    DATE;
    BEGIN                
         v_date_from := TRUNC(p_date_from);
         v_date_to := LAST_DAY (p_date_to) + INTERVAL '00 23:59:59' DAY TO SECOND;   
    
         OPEN p_recordset FOR 
               SELECT       
                    DECODE(c.cl_a_order_id, -1, '����� �� ����������� �� ������ ������',
                                     -3, '���� �� �������',
                                     c.cl_a_order_id) order_id,
                     O.ORDER_NO,                   
                     c.subs_a,
                     COUNT(*) CALL_COUNT,
                     SUM(c.i_conversation_time) SUM_SECONDS
                FROM mdv.t03_mmts_cdr c,
                     order_t o,
                     order_phones_t p
               WHERE c.i_ans_time BETWEEN v_date_from AND v_date_to
                 AND o.order_no = p_order_no
                 AND o.order_id = p.order_id
                 AND pk120_bind_clients.Get_MMTS_Ph_A(c.i_service_id, c.i_dial_number, c.Subs_A) = p.phone_number
                 AND c.i_ans_time BETWEEN p.date_from AND NVL(p.date_to, TO_DATE('01.01.2050','dd.mm.yyyy'))   
                 AND c.cl_a_order_id <> -3
              GROUP BY c.cl_a_order_id, O.ORDER_NO,c.subs_a
            ORDER BY c.subs_a,order_id;
    END; 
    
-- ������ ������� � BDR �� �������� ������� ������ (��/��)
    PROCEDURE REP_FIND_BDR_BY_ORDER_NO( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_order_no        IN  VARCHAR2,
         p_date_from       IN DATE,
         p_date_to         IN DATE
    )IS
         v_date_from  DATE;
         v_date_to    DATE;
    BEGIN                
         v_date_from := TRUNC(p_date_from);
         v_date_to := LAST_DAY (p_date_to) + INTERVAL '00 23:59:59' DAY TO SECOND;   
    
         OPEN p_recordset FOR 
               SELECT 
                        E.ORDER_ID,
                        o.ORDER_NO,
                        e.bill_id,
                        B.BILL_NO,
                        E.BDR_STATUS,
                        D.NAME BDR_NAME,
                        COUNT(*) CALL_COUNT, 
                        SUM(e.amount) AMOUNT 
                    FROM 
                        BDR_VOICE_T e, 
                        ORDER_T o,
                        BILL_T b,
                        dictionary_t d
                WHERE 1=1
                    AND d.parent_id (+)=21
                    AND D.EXTERNAL_ID (+)= e.bdr_status
                    AND o.order_id = e.order_id
                    AND b.bill_id (+)=e.bill_Id
                    AND rep_period >= TRUNC(v_date_from,'mm')
                    AND local_time BETWEEN v_date_from AND v_date_to
                    AND o.order_no = p_order_no
                    GROUP BY  
                            E.ORDER_ID,
                            o.ORDER_NO,
                            e.bill_id,
                            B.BILL_NO,
                            E.BDR_STATUS,
                            D.NAME
                    ORDER BY o.order_no,e.bdr_status DESC ;                           
    END;   
 
-- ����� �� ��������� ������ ������   
    PROCEDURE REP_ORDER_AUDIT( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_order_no        IN  VARCHAR2,
         p_order_id        IN  NUMBER
    )IS
    BEGIN                
         OPEN p_recordset FOR 
               SELECT
                   R.ORDER_ID,
                   R.ORDER_NO,
                   S.SERVICE,
                   TRF.RATEPLAN_NAME,
                   r.date_from,
                   r.date_to,
                   r.status,
                   r.save_date,     
                   DECODE(r.ACTION,-1,'�������',1,'���������') ACTION        
               FROM RS02_ORDER_AUDIT r,
                    ORDER_T O,
                    SERVICE_T S,
                    rateplan_t trf
              WHERE 
                  R.ORDER_NO = NVL(p_order_no, R.ORDER_NO)
                  AND R.ORDER_ID = NVL(p_order_id, R.ORDER_ID)
                  AND R.SERVICE_ID = s.service_id (+)
                  AND r.rateplan_id = TRF.RATEPLAN_ID (+)
              ORDER BY save_date, action DESC ;                           
    END;   
  
-- ����� �� ��������� ������ ������� �������� (����� ���� �� ������ ������, ���� �� ������ ��������)
    PROCEDURE REP_ORDER_PHONE_AUDIT( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_order_no        IN  VARCHAR2,
         p_order_id        IN  NUMBER,
         p_phone_number    IN  VARCHAR2
    ) IS
         v_date_from  DATE;
         v_date_to    DATE;
    BEGIN                
    
         OPEN p_recordset FOR 
                 SELECT 
                      O.ORDER_ID,
                      O.ORDER_NO,
                      r.PHONE_NUMBER,
                      r.DATE_FROM,
                      r.date_to,
                      r.save_date,
                      DECODE(r.ACTION,-1,'�������',1,'���������') ACTION 
                  FROM 
                      RS03_ORDER_PHONES_AUDIT r,
                      ORDER_T O
                  WHERE 1=1
                      AND r.order_id = NVL(p_order_id, r.order_id)
                      AND o.order_no = NVL(p_order_no, o.order_no)
                      AND r.phone_number = NVL(p_phone_number, r.phone_number)
                      AND o.order_id = r.order_id
                  ORDER BY SAVE_DATE, ACTION DESC;        
    END; 
--=====================================================================================
-- ��������� ��������� � ����� ��������� ����� (20.03.2015)
-- ������ �������, ������� �������� ��� ����� ��    
    PROCEDURE REP_ORDER_TRF_CHANGE( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2
    ) IS
        v_sql              VARCHAR2(10000);
    BEGIN                    
         v_sql :=
             'SELECT O.ORDER_NO,
                 O.STATUS,
                 O.DATE_FROM ORDER_DATE_FROM,       
                 CASE 
                  WHEN O.DATE_TO>= TO_DATE(''01.01.2050'',''DD.MM.YYYY'')THEN NULL
                  ELSE O.DATE_TO END 
                 ORDER_DATE_TO,
                 ct.CONTRACT_NO,
                 CT.DATE_FROM CONTRACT_DATE_FROM,
                 CUST.SHORT_NAME CUSTOMER_NAME,
                 BR.SHORT_NAME BRANCH_NAME,
                 AG.SHORT_NAME AGENT_NAME
            FROM pin.CONTRACT_T ct,
                 pin.ACCOUNT_PROFILE_T ap,
                 pin.ACCOUNT_T at,
                 pin.ORDER_T o,
                 pin.CUSTOMER_T cust,
                 pin.contractor_t br,
                 pin.contractor_t ag
           WHERE     AP.CONTRACT_ID = ct.CONTRACT_ID
                 AND TO_DATE (''05.03.2015'', ''dd.mm.yyyy'') > ap.DATE_FROM
                 AND TO_DATE (''19.03.2015'', ''dd.mm.yyyy'') <
                        NVL (ap.DATE_TO, TO_DATE (''05.03.2050'', ''dd.mm.yyyy''))
                 AND O.ACCOUNT_ID = AP.ACCOUNT_ID
                 AND TO_DATE (''05.03.2015'', ''dd.mm.yyyy'') > O.DATE_FROM
                 AND TO_DATE (''19.03.2015'', ''dd.mm.yyyy'') < O.DATE_TO
                 AND (o.STATUS IS NULL OR o.STATUS = ''OPEN'')
                 AND CUST.CUSTOMER_ID = AP.CUSTOMER_ID
                 AND AP.BRANCH_ID = br.contractor_id (+)
                 AND AP.AGENT_ID = ag.contractor_Id (+)
                 AND AP.ACCOUNT_ID = AT.ACCOUNT_ID
                 AND AT.ACCOUNT_TYPE = ''J''
                 AND EXISTS
                        (SELECT 1
                           FROM pin.ORDER_BODY_T ob
                          WHERE     OB.ORDER_ID = O.ORDER_ID
                                AND OB.SUBSERVICE_ID IN (1, 2)
                                AND TO_DATE (''05.03.2015'', ''dd.mm.yyyy'') > Ob.DATE_FROM
                                AND TO_DATE (''19.03.2015'', ''dd.mm.yyyy'') < Ob.DATE_TO)
                 AND NOT EXISTS
                        (    -- ��������� ������, ��� ������� ��������� ��������� ������
                         SELECT 1
                           FROM TARIFF_PH.ZZZ_UPDATED_TRF_ORDER updo
                          WHERE updo.ORDER_NO = O.ORDER_NO)
                 AND NOT EXISTS
                        (                                -- ��������� ��������� ��������
                         SELECT 1
                           FROM pin.CONTRACT_T exclc
                          WHERE exclc.EXCL_TARIFF_CHANGE = 1
                              AND exclc.CONTRACT_ID = ct.CONTRACT_ID)
                 AND NOT EXISTS
                        (                                  -- ��������� ��������� ������
                         SELECT 1
                           FROM TARIFF_PH.ZZZ_UPDATED_TRF_ORDER_excl o_excl
                          WHERE o_excl.order_no = O.ORDER_NO)
                  AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))'                                                                      
                ;                          
         OPEN p_recordset FOR 
              v_sql;
    END; 
    
-- ������ ���������, ������� �������� � ����������
    PROCEDURE REP_CONTRACT_EXCL_TRF_CHANGE( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2
    ) IS
        v_sql              VARCHAR2(10000);
    BEGIN                    
         v_sql :=
             'select distinct 
                    C.CONTRACT_NO,
                    CUS.COMPANY_NAME CUSTOMER_NAME,
                    C.DATE_FROM CONTRACT_DATE_FROM,
                    BR.SHORT_NAME BRANCH_NAME,
                    AG.SHORT_NAME AGENT_NAME
                from 
                contract_t c, 
                company_t cus,
                account_profile_t ap,
                account_t a,
                contractor_t br,
                contractor_t ag
            where 
                c.contract_id = ap.contract_id
                AND c.CONTRACT_ID = cus.CONTRACT_ID
                AND SYSDATE BETWEEN cus.DATE_FROM AND NVL(cus.date_to,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                AND C.EXCL_TARIFF_CHANGE = 1
                AND SYSDATE BETWEEN ap.DATE_FROM AND NVL(ap.date_to,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                and a.account_Id = ap.account_id
                and A.ACCOUNT_TYPE = ''J''
                and ap.branch_id = br.contractor_id (+)    
                and ap.agent_id = ag.contractor_id (+)  
                  AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))'                                                                      
                ;                          
         OPEN p_recordset FOR 
              v_sql;
    END;     

--==============================================================================================================
-- ������ �������, ������������� ���������� ��� "�������������"
    PROCEDURE REP_ORDER_AUDIT_RETRF( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_date_from       IN  DATE,
         p_date_to         IN  DATE
    ) IS
    BEGIN                    
         OPEN p_recordset FOR 
                SELECT 
                   a.account_id,
                   a.account_no, 
                   o.order_id,
                   o.order_no,       
                   o.date_from order_date_from,
                   o.date_to order_date_to,
                   t.period_from,
                   period_to,
                   t.status,
                   t.CREATE_DATE JOB_CREATE_DATE
              FROM (SELECT d.order_id_new,
                           q.date_from period_from,
                           q.date_to period_to,
                           (CASE
                               WHEN q.end_time IS NOT NULL THEN '���������'
                               ELSE '� ��������'
                            END)
                              status,
                              Q.CREATE_DATE
                      FROM q00_retrf_JOB q, Q01_RETRF_JOB_DETAIL d
                     WHERE     q.note = '�������������� ��������'
                           AND q.task_id = d.task_id) t,
                   order_t o,
                   account_t a
             WHERE t.order_id_new = o.order_id 
                   AND o.account_id = a.account_id
                   AND t.CREATE_DATE BETWEEN p_date_from AND p_date_to
             ORDER BY t.CREATE_DATE, O.ORDER_NO;
    END; 
    
--==============================================================================================================
-- ���������� �� ���
    PROCEDURE REP_STAT_EDO( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc
    ) IS
    BEGIN                    
         OPEN p_recordset FOR 
            SELECT CS.COMPANY_NAME CUSTOMER, A.ACCOUNT_NO, AD.*, SYSDATE DATE_SAVE, EDO.NAME EDO_NAME
                  FROM ACCOUNT_DOCUMENTS_T AD, ACCOUNT_T A, 
                       ACCOUNT_PROFILE_T AP, COMPANY_T CS,
                       DICTIONARY_T EDO
                 WHERE DELIVERY_METHOD_ID IN (6515, 6549)
                   AND EDO.KEY_ID = ad.delivery_method_id
                   AND AD.ACCOUNT_ID = A.ACCOUNT_ID
                   AND AD.ACCOUNT_ID = AP.ACCOUNT_ID
                   AND AP.CONTRACT_ID = CS.CONTRACT_ID
                   AND CS.ACTUAL = 'Y'
                   AND AP.ACTUAL = 'Y'
                 ORDER BY CS.COMPANY_NAME;
    END;     

--==============================================================================================================
-- ���������� �� ������ ��������� PonyExpress
    PROCEDURE REP_STAT_PONYEXPRESS( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc,
         p_period_id       IN  NUMBER
    ) IS
    BEGIN                    
         OPEN p_recordset FOR 
            SELECT B.PONY_EXPRESS_KONVERT_ID, C.CONTRACT_NO, B.BILL_NO, COMP.COMPANY_NAME
                FROM  bill_t b, 
                      contract_t c, 
                      company_t comp
               WHERE 
                  rep_period_id = p_period_id 
                  AND pony_express_konvert_id IS NOT NULL
                  and b.contract_id = c.contract_id
                  and comp.contract_id = c.contract_id
                  and B.bill_date between comp.date_from and NVL(comp.date_to,TO_DATE('01.01.2050','DD.MM.YYYY'))
              ORDER BY CONTRACT_NO;  
    END;        
    
--======================================================================
-- ��������� ������ �� ������-��������
PROCEDURE REPORT_PAY_ONLINE( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_date_from         DATE,
          p_date_to           DATE,
          p_region_id         INTEGER,
          p_ps_id             INTEGER,
          p_only_error        INTEGER
  )
IS
    v_prcName   CONSTANT varchar2(30) := 'REPORT_PAY_ONLINE';
BEGIN
    OPEN p_recordset FOR
         SELECT *
            FROM (
            /*SELECT 
                         PAYMENT_GATE.get_region_id (in_number) REGION_ID,
                         PAYMENT_GATE.get_region_name (in_number) REGION_NAME,
                         ps.ps_id,
                         l.requester_name ps_name,
                         l.action_start_t + 3 / 24 action_start_t,
                         l.in_action,
                         l.in_number,
                         l.in_amount,
                         l.in_receipt,
                         CASE
                            WHEN requester_name = 'SBRF-Online'
                            THEN
                                 DECODE (out_code,  1, 0,  0, 10)
                            ELSE
                                 OUT_CODE
                         END code,
                         l.out_message
                    FROM PAYMENT_GATE.payment_requests_log l,
                         payment_gate.PS_PAYSYSTEM ps
                   WHERE     l.action_start_t BETWEEN p_date_from - 3 / 24 AND p_date_to - 3 / 24
                         AND l.in_action LIKE 'pay%'
                         AND PAYMENT_GATE.get_region_name (in_number) IS NOT NULL
                         AND DECODE (out_code, 1, 0, out_code) IS NOT NULL
                         AND ps.ps_name = l.requester_name(+)                         
                   UNION  */                 
                    SELECT 
                        EPS.REG_ID REGION_ID,
                        R.REG_NAME REGION_NAME,
                        PS.PS_ID,
                        PS.PS_NAME,
                        EPS.SYS_DATE + 3 / 24 ACTION_START_T,
                        'payment' IN_ACTION,
                        EPS.ABONENT_ID IN_NUMBER,
                        EPS.AMOUNT,
                        EPS.RECEIPT IN_RECEIPT,
                        EPS.STATUS CODE,
                        EPS.RESULT OUT_MESSAGE 
                    FROM payment_gate.eps_payments_actual eps, 
                         payment_gate.REGIONS r, 
                         payment_gate.PS_PAYSYSTEM ps
                    WHERE R.REG_ID = EPS.REG_ID
                         AND PS.PS_ID = EPS.PS_ID 
                         AND eps.sys_date BETWEEN p_date_from - 3 / 24 AND p_date_to - 3 / 24   
                   )
           WHERE (PAYMENT_GATE.get_region_id (in_number) = p_region_id OR p_region_id IS NULL)
                 AND (ps_id = p_ps_id OR p_ps_id IS NULL)
                 AND ((CODE <> 0 AND p_only_error=1) OR (p_only_error IS NULL OR p_only_error=0))
        ORDER BY action_start_t DESC;
EXCEPTION
    WHEN others THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;            

--=======================================================================================================
-- ����� "����� ������" �� ������������ ������ �� ������������� ����������
PROCEDURE REP_BY_BOOK_SALE( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER
    )IS
         v_sql                VARCHAR2(10000);    
BEGIN
    IF p_period_id IS NULL THEN
       p_result := '������ ������ ���� �����';
    ELSE
          v_sql := '
             SELECT CUS.COMPANY_NAME CUSTOMER,
                 a.account_no,
                 b.bill_no,
                 b.total,
                 i.MAX_DATE_TO,
                 CFO.NOTES CFO,
                 TSFU.NOTES TSFU  
            FROM account_profile_t ap,
                 contract_t contr,
                 bill_t b,
                 company_t cus,
                 account_t a,
                 (  SELECT bill_id, MAX (date_to) MAX_DATE_TO
                      FROM item_t
                     WHERE REP_PERIOD_ID = ' || p_period_id ||
           ' GROUP BY BILL_ID) i,
                 DICTIONARY_T CFO,
                 DICTIONARY_T TSFU
           WHERE     b.profile_id = ap.profile_id
                 AND b.rep_period_id = ' || p_period_id || 
           '     AND cus.contract_id = ap.contract_id
                 AND B.BILL_DATE BETWEEN cus.DATE_FROM AND NVL(cus.Date_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                 AND a.account_Id = ap.account_id
                 AND i.bill_id = b.bill_Id
                 AND a.account_type = ''J''
                 AND A.STATUS <> ''T''
                 AND CONTR.CONTRACT_ID = B.CONTRACT_ID
                 AND CONTR.CFO_ID = CFO.KEY_ID (+)
                 AND CONTR.CFU_ID = TSFU.KEY_ID (+)
                 AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
                 AND B.REP_PERIOD_ID = '|| p_period_id;          
       OPEN p_recordset FOR 
             v_sql;
    END IF;
END; 

--=======================================================================================================
-- ����� �� ������ (������������� ����� �� ��� ���������)
PROCEDURE REP_BY_BILL( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER
    )IS
         v_sql                VARCHAR2(10000);    
BEGIN
    IF p_period_id IS NULL THEN
       p_result := '������ ������ ���� �����';
    ELSE
         v_sql := '
             SELECT 
                 CUS.COMPANY_NAME CUSTOMER,
                 c.contract_no,
                 substr(o.order_no, instr(o.order_no,''-'') + 1, length(o.order_no) - instr(o.order_no,''-'')) ORDER_NO,
                 b.bill_no,
                 I.REP_GROSS,
                 SS.SUBSERVICE,
                 S.SERVICE,
                 I.DATE_TO,
                 PK402_BCR_DATA.Get_sales_curator(ap.BRANCH_ID, ap.AGENT_ID, ap.CONTRACT_ID, ap.ACCOUNT_ID, NULL, B.BILL_DATE) SALER_NAME,
                 dict_client_type.NAME CLIENT_TYPE_NAME,
                 del_type.delivery_method_name       
            FROM account_profile_t ap,
                 bill_t b,
                 contract_t c,
                 company_t cus,
                 account_t a,
                 order_T o,
                 item_t i,
                 service_t s,
                 subservice_t ss,
                 (SELECT *
                        FROM DICTIONARY_T
                       WHERE parent_id = 64) dict_client_type,
                 (
                    SELECT ad.account_id, ad.delivery_method_id,D.NAME delivery_method_name
                       FROM account_documents_t ad, dictionary_t d                      
                    WHERE d.key_id = ad.delivery_method_id AND ad.doc_bill = ''Y'') del_type    
           WHERE 1=1
                 AND b.contract_id = c.contract_id
                 AND b.profile_id = ap.profile_id
                 AND cus.contract_id = ap.contract_id
                 AND B.BILL_DATE BETWEEN cus.DATE_FROM AND NVL(cus.Date_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                 AND B.BILL_DATE BETWEEN ap.DATE_FROM and NVL(ap.date_to,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                 and C.CLIENT_TYPE_ID = dict_client_type.KEY_ID (+)
                 AND a.account_Id = ap.account_id
                 and s.service_id = I.SERVICE_ID
                 and ss.subservice_id = I.SUBSERVICE_ID
                 and a.account_id = del_type.account_id (+)
                 AND i.bill_id = b.bill_Id
                 and I.ORDER_ID = o.order_id
                 AND a.account_type = ''J''
                 AND A.STATUS <> ''T''
                 and I.ITEM_TOTAL <> 0
                 AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
                 AND B.REP_PERIOD_ID = '|| p_period_id;          
       OPEN p_recordset FOR 
             v_sql;
    END IF;
END;    
   
--=======================================================================================================
-- ����� �� ������ (������������� ����� �� ��� ���������). ������ ���������� ��������������.
-- ������������ ������� �.
PROCEDURE REP_BY_BILL_AGENT_VIEW( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_contractor_id   IN  VARCHAR2,
         p_period_id       IN  NUMBER
    )IS
         v_sql                VARCHAR2(10000);    
BEGIN
    IF p_period_id IS NULL THEN
       p_result := '������ ������ ���� �����';
    ELSE
          v_sql := '
             SELECT 
                   CUS.COMPANY_NAME CUSTOMER,
                   c.contract_no,
                   ''����� � '' || substr(o.order_no, instr(o.order_no,''-'') + 1, length(o.order_no) - instr(o.order_no,''-'')) ORDER_NO,
                   b.bill_no,
                   I.REP_GROSS,
                   SS.SUBSERVICE,
                   S.SERVICE,
                   NULL PERCENT,
                   NULL AGENT_GROSS,
                   I.DATE_TO       
              FROM account_profile_t ap,
                   bill_t b,
                   contract_t c,
                   company_t cus,
                   account_t a,
                   order_T o,
                   item_t i,
                   service_t s,
                   subservice_t ss
             WHERE 1=1
                   AND b.contract_id = c.contract_id
                   AND b.profile_id = ap.profile_id
                   AND cus.contract_id = ap.contract_id
                   AND B.BILL_DATE BETWEEN cus.DATE_FROM AND NVL(cus.Date_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                   AND a.account_Id = ap.account_id
                   and s.service_id = I.SERVICE_ID
                   and ss.subservice_id = I.SUBSERVICE_ID       
                   AND i.bill_id = b.bill_Id
                   and I.ORDER_ID = o.order_id
                   AND a.account_type = ''J''
                   AND A.STATUS <> ''T''
                   and I.ITEM_TOTAL <> 0
                   AND B.BILL_TYPE IN (''B'', ''D'', ''O'', ''M'',''C'',''A'')    
                   AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
                   AND B.REP_PERIOD_ID = '|| p_period_id;          
       OPEN p_recordset FOR 
             v_sql;
    END IF;
END;   

-- ------------------------------------------------------------------------------
-- 1) BRM KTTK. ����� �� ������������ ������ (��. ����)
-- ------------------------------------------------------------------------------
-- ��������� ������ �� ������-��������
PROCEDURE REP_BY_BILL_NEW( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_contractor_id     VARCHAR2,
          p_period_id         INTEGER
  )
IS
    v_prcName   CONSTANT varchar2(30) := 'REP_BY_BILL_NEW';
    v_sql                VARCHAR2(10000);    
BEGIN
    IF p_period_id IS NULL THEN
       p_message := '������ ������ ���� �����';
    ELSE
       v_sql := '
          SELECT 
                  ERP_CODE,     --��� �����������
                  CUSTOMER,     --����������
                  CONTRACT_NO,  --� ��������    
                  REGION,       --������    
                  AGENT,        --�������� ��������������    
                  CLIENT_TYPE, --��� �������    
                  MARKET_SEGM,  --������� �����    
                  CONTRACTOR,   --���������    
                  DLV_ADDR,     --�������� �����    
                  JUR_ADDR,     --����������� �����    
                  PHONES,       -- ���.�    
                  FAX,          --����    
                  BILL_NO,      --� �����    
                  BILL_DATE,    --���� �����    
                  BILL_TYPE,    --��� �����    
                  BILL_GROSS,   --����� ����� (��� ���)�    
                  BILL_VAT,     --��Ѡ    
                  BILL_TOTAL,   --����� �����    
                  CURRENCY,     --������    
                  DLV_METHOD,   --������ ��������
                  SALES_CURATOR, -- ��������,
                  CFO,
                  TSFU
            FROM (
              SELECT 
                  A.ACCOUNT_TYPE,
                  CASE 
                  WHEN CM.ERP_CODE IS NULL AND A.ACCOUNT_TYPE = ''P'' THEN ''K081982''
                  ELSE CM.ERP_CODE
                  END ERP_CODE,     --��� �����������
                  CASE 
                  WHEN A.ACCOUNT_TYPE = ''P'' THEN ''���������� ����''
                  ELSE CM.COMPANY_NAME
                  END CUSTOMER, --����������
                  C.CONTRACT_NO, --� ��������    
                  X.CONTRACTOR REGION, --������    
                  AG.CONTRACTOR AGENT, --�������� ��������������    
                  DT.NAME CLIENT_TYPE,--��� �������    
                  DM.NAME MARKET_SEGM, --������� �����    
                  CT.CONTRACTOR, --���������    
                  AD.ZIP||'',''||AD.COUNTRY||'',''||AD.STATE||'',''||AD.CITY||'',''||AD.ADDRESS DLV_ADDR, --�������� �����    
                  AJ.ZIP||'',''||AJ.COUNTRY||'',''||AJ.STATE||'',''||AJ.CITY||'',''||AJ.ADDRESS JUR_ADDR, --����������� �����    
                  AJ.PHONES, -- ���.�    
                  AJ.FAX, --����    
                  B.BILL_NO, --� �����    
                  B.BILL_DATE, --���� �����    
                  B.BILL_TYPE, --��� �����    
                  B.GROSS BILL_GROSS, --����� ����� (��� ���)�    
                  B.VAT BILL_VAT, --��Ѡ    
                  B.TOTAL BILL_TOTAL, --����� �����    
                  CR.CURRENCY_CODE CURRENCY, --������    
                  dc.DELIVERY_METHOD_NAME DLV_METHOD, --������ ��������
                  PK66_MINREPORT.Get_sales_curator (
                     p_branch_id     => AP.BRANCH_ID,
                     p_agent_id      => AP.AGENT_ID,
                     p_contract_id   => AP.CONTRACT_ID,
                     p_account_id    => AP.ACCOUNT_ID,
                     p_order_id      => NULL,
                     p_date          => B.BILL_DATE
                  ) SALES_CURATOR,   -- ��������
                  CFO.NOTES CFO,
                  TSFU.NOTES TSFU,
                  ROW_NUMBER() OVER (PARTITION BY B.BILL_ID ORDER BY DC.DELIVERY_METHOD_ID) RN
              FROM BILL_T B, ACCOUNT_PROFILE_T AP, 
                   CURRENCY_T CR, 
                   CONTRACTOR_T CT, 
                   COMPANY_T CM, 
                   CONTRACT_T C,
                   CONTRACTOR_T X, CONTRACTOR_T AG,
                   ACCOUNT_T A, DICTIONARY_T DM, DICTIONARY_T DT,
                   ACCOUNT_CONTACT_T AD, ACCOUNT_CONTACT_T AJ,
                   (
                     select ad.account_id, ad.DELIVERY_METHOD_ID, dc.NOTES DELIVERY_METHOD_NAME from account_documents_t ad, dictionary_t dc
                     where doc_bill = ''Y''
                     and ad.delivery_method_id = dc.key_id
                  ) DC,
                   DICTIONARY_T CFO,
                   DICTIONARY_T TSFU
              WHERE B.CURRENCY_ID   = CR.CURRENCY_ID
                AND B.CONTRACTOR_ID = CT.CONTRACTOR_ID
                AND B.PROFILE_ID    = AP.PROFILE_ID
                AND AP.CONTRACT_ID  = CM.CONTRACT_ID(+)
                AND B.CONTRACT_ID   = C.CONTRACT_ID
                AND AP.BRANCH_ID    = X.CONTRACTOR_ID(+)
                AND AP.AGENT_ID     = AG.CONTRACTOR_ID(+)
                AND A.ACCOUNT_ID    = B.ACCOUNT_ID 
                AND C.MARKET_SEGMENT_ID = DM.KEY_ID(+)
                AND C.CLIENT_TYPE_ID= DT.KEY_ID(+)
                AND A.ACCOUNT_ID    = AD.ACCOUNT_ID(+)
                AND AD.ADDRESS_TYPE(+) = ''DLV''
                AND A.ACCOUNT_ID    = AJ.ACCOUNT_ID(+)
                AND AJ.ADDRESS_TYPE(+) = ''DLV''
                AND A.ACCOUNT_ID    = DC.ACCOUNT_ID(+) 
                AND A.ACCOUNT_TYPE  = ''J'' 
                AND A.STATUS <> ''T''
                AND B.BILL_TYPE <> ''P''
                AND C.CFO_ID = CFO.KEY_ID (+)
                AND C.CFU_ID = TSFU.KEY_ID (+)
                AND B.BILL_DATE BETWEEN CM.DATE_FROM AND NVL(CM.DATE_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
                AND B.REP_PERIOD_ID = '|| p_period_id ||          
          ')
          WHERE RN = 1
          ORDER BY CONTRACTOR, REGION, AGENT, CONTRACT_NO';          
              
       OPEN p_recordset FOR 
             v_sql;
    END IF;  

EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- -------------------------------------------------------------------------
-- 2) BRM KTTK. ����� �� ������������ ������ (��. ����) [�� �������]
-- -------------------------------------------------------------------------
PROCEDURE REP_BY_BILL_ORDERS( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_contractor_id     VARCHAR2,
          p_period_id         INTEGER
  )
IS
    v_prcName   CONSTANT varchar2(30) := 'REP_BY_BILL_ORDERS';
    v_sql                VARCHAR2(10000);        
BEGIN
    IF p_period_id IS NULL THEN
       p_message := '������ ������ ���� �����';
    ELSE
       v_sql := '
          SELECT 
                  ERP_CODE,     --��� �����������
                  CUSTOMER,     --����������
                  CONTRACT_NO,  --� ��������   
                  CONTRACT_DATE_FROM, 
                  REGION,       --������    
                  AGENT,        --�������� ��������������    
                  CLIENT_TYPE, --��� �������    
                  MARKET_SEGM,  --������� �����    
                  CONTRACTOR,   --��������� 
                  BILL_NO,      --� �����    
                  BILL_DATE,    --���� �����    
                  BILL_TYPE,    --��� �����    
                  ORDER_NO,     -- � ������    
                  SERVICE,      --������    
                  SUBSERVICE,   --���������    
                  GROSS,        --����� (��� ���)
                  CURRENCY,     --������    
                  SRV_DATE_FROM,--������ �������� ������
                  SRV_DATE_TO,  --������ �������� ������
                  SPEED_STR,    --��������
                  POINT_SRC,    --����� 1
                  POINT_DST,    --����� 2
                  SALES_CURATOR, -- ��������
                  CFO,
                  CFU
            FROM (
              SELECT 
                  A.ACCOUNT_TYPE,
                  CASE 
                  WHEN A.ACCOUNT_TYPE = ''P'' THEN ''K081982''
                  ELSE CM.ERP_CODE
                  END ERP_CODE,     --��� �����������
                  CASE 
                  WHEN A.ACCOUNT_TYPE = ''P'' THEN ''���������� ����''
                  ELSE CM.COMPANY_NAME
                  END CUSTOMER, --����������
                  C.CONTRACT_NO, --� ��������    
                  C.DATE_FROM CONTRACT_DATE_FROM,
                  X.CONTRACTOR REGION, --������    
                  AG.CONTRACTOR AGENT, --�������� ��������������    
                  DT.NAME CLIENT_TYPE,--��� �������    
                  DM.NAME MARKET_SEGM, --������� �����    
                  CT.CONTRACTOR, --���������    
                  B.BILL_NO, --� �����    
                  B.BILL_DATE, --���� �����    
                  B.BILL_TYPE, --��� �����
                  CASE 
                    WHEN SUBSTR(A.ACCOUNT_NO,1,8)||''-'' = SUBSTR(ORDER_NO,1,9) THEN SUBSTR(ORDER_NO,10)
                    ELSE O.ORDER_NO 
                  END ORDER_NO, -- � ������
                  S.SERVICE SERVICE, --������    
                  SS.SUBSERVICE SUBSERVICE, -- ���������    
                  I.REP_GROSS GROSS, -- ����� (��� ���)
                  CR.CURRENCY_CODE CURRENCY,     --������    
                  I.DATE_FROM SRV_DATE_FROM, --������ �������� ������
                  I.DATE_TO SRV_DATE_TO, --������ �������� ������
                  OI.SPEED_STR, --��������
                  OI.POINT_SRC POINT_SRC, --����� ���������
                  OI.POINT_DST POINT_DST, --����� ���������
                  PK66_MINREPORT.Get_sales_curator (
                     p_branch_id     => AP.BRANCH_ID,
                     p_agent_id      => AP.AGENT_ID,
                     p_contract_id   => AP.CONTRACT_ID,
                     p_account_id    => AP.ACCOUNT_ID,
                     p_order_id      => O.ORDER_ID,
                     p_date          => B.BILL_DATE
                  ) SALES_CURATOR,   -- ��������                  
                  CFO.NAME CFO,
                  CFU.NAME CFU
              FROM BILL_T B, 
                   ACCOUNT_PROFILE_T AP, 
                   CURRENCY_T CR, 
                   CONTRACTOR_T CT, 
                   COMPANY_T CM,
                   CONTRACT_T C,
                   CONTRACTOR_T X, CONTRACTOR_T AG,
                   ACCOUNT_T A, DICTIONARY_T DM, DICTIONARY_T DT,
                   ITEM_T I,
                   ORDER_T O,
                   ORDER_BODY_T OB,
                   SERVICE_T S,
                   SUBSERVICE_T SS,
                   ORDER_INFO_T OI,
                   DICTIONARY_T CFO,
                   DICTIONARY_T CFU
              WHERE B.CONTRACTOR_ID = CT.CONTRACTOR_ID
                AND B.PROFILE_ID    = AP.PROFILE_ID
                AND B.CONTRACT_ID   = C.CONTRACT_ID
                AND B.CONTRACT_ID   = CM.CONTRACT_ID
                AND B.BILL_DATE BETWEEN CM.DATE_FROM AND NVL(CM.DATE_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                AND O.CFO_ID        = CFO.KEY_ID (+)
                AND C.CFU_ID        = CFU.KEY_ID (+)                
                AND AP.BRANCH_ID    = X.CONTRACTOR_ID(+)
                AND AP.AGENT_ID     = AG.CONTRACTOR_ID(+)
                AND A.ACCOUNT_ID    = B.ACCOUNT_ID 
                AND C.MARKET_SEGMENT_ID = DM.KEY_ID(+)
                AND C.CLIENT_TYPE_ID= DT.KEY_ID(+)
                AND A.ACCOUNT_TYPE  = ''J''
                AND A.STATUS <> ''T''
                AND B.BILL_TYPE <> ''P''
                AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
                AND I.BILL_ID       = B.BILL_ID
                AND B.CURRENCY_ID   = CR.CURRENCY_ID
                AND O.ORDER_ID      = I.ORDER_ID
                AND OB.ORDER_BODY_ID (+)= I.ORDER_BODY_ID
                AND S.SERVICE_ID    = O.SERVICE_ID
                AND SS.SUBSERVICE_ID (+)= OB.SUBSERVICE_ID
                AND O.ORDER_ID      = OI.ORDER_ID(+)
                AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
                AND B.REP_PERIOD_ID = '|| p_period_id ||         
          ')
          ORDER BY CONTRACTOR, REGION, AGENT, CONTRACT_NO';
       
       OPEN p_recordset FOR 
             v_sql;
    END IF;  

EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ���������� �� �������� � �� �� ������ ��. ���, ������� ������������ ����� ������-����
    PROCEDURE REP_AKKORD_POST_Y_BY_REGION( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_job_id            IN  NUMBER
    )IS
         v_date_from  DATE;
         v_date_to    DATE;
    BEGIN                
         OPEN p_recordset FOR 
               SELECT br.contractor_id BRANCH_ID,
                     ag.contractor_id AGENT_ID,
                     br.contractor branch,
                     ag.contractor agent,
                     COUNT (*) CNT
                FROM bill_print_t bp,
                     bill_t b,
                     account_profile_t ap,
                     contractor_t br,
                     contractor_t ag
               WHERE job_id = p_job_id
                     AND bp.bill_id = b.bill_Id
                     AND b.account_id = ap.account_id
                     AND SYSDATE BETWEEN ap.DATE_FROM
                                     AND NVL (ap.DATE_TO,
                                              TO_DATE ('01.01.2050', 'DD.MM.YYYY'))
                     AND br.contractor_id(+) = ap.branch_id
                     AND ag.contractor_id(+) = ap.agent_id
                     AND bp.print_status = 'OK'
            GROUP BY br.contractor_id,
                     ag.contractor_id,
                     br.contractor,
                     ag.contractor
            ORDER BY br.contractor, ag.contractor NULLS FIRST;
    END; 

-- ���������� �� �������� � �� �� ������ ��. ���, ������� ������������ ����� ������-����
    PROCEDURE REP_AKKORD_POST_F_BY_REGION( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc, 
         p_job_id          IN  NUMBER
    )IS
         v_date_from  DATE;
         v_date_to    DATE;
    BEGIN                
         OPEN p_recordset FOR 
                 SELECT br.contractor_id branch_id,
                     ag.contractor_id agent_id,
                     br.contractor BRANCH,
                     ag.contractor AGENT,
                     COUNT (*) CNT
                FROM bill_print_t bp,         
                     account_profile_t ap,
                     contractor_t br,
                     contractor_t ag
               WHERE     job_id = p_job_id
                     AND bp.account_id = ap.account_id
                     AND SYSDATE BETWEEN ap.DATE_FROM AND NVL (ap.DATE_TO, TO_DATE ('01.01.2050', 'DD.MM.YYYY'))
                     AND br.contractor_id(+) = ap.branch_id
                     AND ag.contractor_id(+) = ap.agent_id
                     AND bp.print_status = 'OK'                     
            GROUP BY br.contractor_id,
                     ag.contractor_id,
                     br.contractor,
                     ag.contractor
            ORDER BY br.contractor, ag.contractor NULLS FIRST;
    END; 

-- ------------------------------------------------------------------------------
-- ������ �� �������� �������
-- ����� ��������   
-- ������ (�������� �� ����/��� ��� ����)
-- ��� �/� (���./��. ����)
-- ����� ������
-- ���� ������ �������� ������
-- ������ ������ (����� ��������: ��������,���������������, �����, ��������)
-- ���� ������ �������� �������� ������� ������
-- ����� ��������
-- �������� ���� (��������)
PROCEDURE REP_BY_PHONE_LIST( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_contractor_id     VARCHAR2
  )
IS
    v_prcName   CONSTANT varchar2(30) := 'REP_BY_PHONE_LIST';
    v_sql                VARCHAR2(10000);    
BEGIN
       v_sql := '
          WITH RO AS (
                SELECT DISTINCT OB.ORDER_ID, RP.RATEPLAN_NAME 
                  FROM ORDER_BODY_T OB, RATEPLAN_T RP
                 WHERE OB.CHARGE_TYPE = ''USG'' 
                   AND OB.RATEPLAN_ID = RP.RATEPLAN_ID
            )
            SELECT DISTINCT
                   CT.CONTRACTOR,
                   C.CONTRACT_NO,
                   CM.COMPANY_NAME, 
                   SU.LAST_NAME||'' ''||SU.FIRST_NAME||'' ''||SU.MIDDLE_NAME SUBSCRIBER,
                   O.ORDER_NO,
                   O.DATE_FROM,
                   O.STATUS,
                   PH.PHONE_NUMBER,
                   PH.DATE_FROM PH_DATE_FROM,
                   PH.DATE_TO   PH_DATE_TO,
                   RO.RATEPLAN_NAME 
              FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, CONTRACTOR_T CT,
                   COMPANY_T CM, 
                   SUBSCRIBER_T SU, 
                   ORDER_T O, ORDER_PHONES_T PH, RO
             WHERE A.ACCOUNT_ID   = AP.ACCOUNT_ID
               AND AP.CONTRACT_ID = C.CONTRACT_ID
               AND AP.CONTRACT_ID = CM.CONTRACT_ID
               AND SYSDATE BETWEEN CM.DATE_FROM AND NVL(CM.DATE_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
               AND AP.SUBSCRIBER_ID = SU.SUBSCRIBER_ID(+)
               AND O.ACCOUNT_ID   = A.ACCOUNT_ID
               AND O.ORDER_ID     = PH.ORDER_ID
               AND O.ORDER_ID     = RO.ORDER_ID(+)   
               AND AP.BRANCH_ID   = CT.CONTRACTOR_ID
               AND SYSDATE BETWEEN PH.DATE_FROM AND PH.DATE_TO
               AND O.DATE_FROM   <= SYSDATE
               AND (O.DATE_TO IS NULL OR O.DATE_TO >= SYSDATE)
               AND AP.DATE_FROM  <= SYSDATE
               AND (AP.DATE_TO IS NULL OR AP.DATE_TO >= SYSDATE)
               AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
               ORDER BY CT.CONTRACTOR, PH.PHONE_NUMBER'; 
       
       OPEN p_recordset FOR 
             v_sql; 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

--=============================================================================

-- ����� ����� �� ��. ����� ���
    PROCEDURE REP_CSS_BY_JUR( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc
    )IS
    BEGIN                
         OPEN p_recordset FOR 
                 SELECT c.contract_no,
                       a.account_no,
                       CUS.SHORT_NAME CUSTOMER_NAME,
                       BR.CONTRACTOR BRANCH,
                       ag.contractor AGENT,
                       a.balance
                  FROM account_profile_t ap,
                       account_t a,
                       contract_t c,
                       contractor_t br,
                       contractor_t ag,
                       company_t cus
                 WHERE     ap.account_id = a.account_id
                       AND ap.contract_id = c.contract_id
                       AND ap.branch_id = br.contractor_id(+)
                       AND ap.agent_id = ag.contractor_id(+)
                       AND ap.contract_id = cus.contract_id
                       AND cus.DATE_FROM < SYSDATE
                       AND (cus.DATE_TO IS NULL OR SYSDATE < cus.DATE_TO)
                       AND a.account_type = 'J'
                       AND (ap.branch_id = 200 OR ap.agent_id = 200);
    END;

-- ����� ����� �� ���. ����� ���
    PROCEDURE REP_CSS_BY_FIZ( 
         p_result          OUT VARCHAR2, 
         p_recordset       OUT t_refc
    )IS
    BEGIN                
         OPEN p_recordset FOR 
                 SELECT c.contract_no,
                     a.account_no,
                     SUB.LAST_NAME || ' ' || SUB.FIRST_NAME || ' ' || SUB.MIDDLE_NAME FIO,
                     BR.CONTRACTOR BRANCH,
                     ag.contractor AGENT,
                     a.balance,
                     o.order_no,
                     op.phone_number
                FROM account_profile_t ap,
                     account_t a,
                     contract_t c,
                     contractor_t br,
                     contractor_t ag,
                     subscriber_t sub,
                     order_t o,
                     order_phones_t op
               WHERE ap.account_id = a.account_id
                     AND ap.contract_id = c.contract_id
                     AND ap.branch_id = br.contractor_id(+)
                     AND ap.agent_id = ag.contractor_id(+)
                     AND ap.subscriber_id = sub.subscriber_id(+)
                     AND a.account_type = 'P'
                     AND A.STATUS <> 'T'
                     AND o.account_id = a.account_id
                     AND o.order_id = op.order_id 
                     AND SYSDATE BETWEEN o.DATE_FROM AND NVL(o.DATE_TO,TO_DATE('01.01.2050','DD.MM.YYYY'))
                     AND (ap.branch_id = 200 OR ap.agent_id = 200); 
    END;
  
-- -------------------------------------------------------------------------  
-- ����� �� ���������������� ������ �� ������
-- -------------------------------------------------------------------------
PROCEDURE REP_BILL_DEBET_CREDIT(
               p_message           OUT VARCHAR2,                
               p_recordset         OUT t_refc,
               p_contractor_id     IN  VARCHAR2,
               p_period_id         IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'REP_BILL_DEBET_CREDIT';
    v_retcode    INTEGER;  
    v_sql        VARCHAR2(10000);
BEGIN
      IF p_period_id IS NULL THEN
         p_message := '������� �������� ������';
      ELSE
            v_sql := '
             WITH ERR AS (
                  SELECT D.KEY_ID, D.KEY CODE, DP.NAME TYPE, D.NAME REASON 
                    FROM DICTIONARY_T D, DICTIONARY_T DP
                   WHERE D.PARENT_ID = DP.KEY_ID
              )
              SELECT CM.COMPANY_NAME,     -- �������� ���������
                     C.CONTRACT_NO,       -- � �������
                     B.BILL_NO,           -- � �����
                     B.REP_PERIOD_ID,     -- �������� ������
                     B.TOTAL,             -- �����
                     CR.CURRENCY_CODE,    -- ������
                     PK66_MINREPORT.Get_sales_curator (
                         p_branch_id   => AP.BRANCH_ID,
                         p_agent_id    => AP.AGENT_ID,
                         p_contract_id => B.CONTRACT_ID,
                         p_account_id  => B.ACCOUNT_ID,
                         p_order_id    => NULL,
                         p_date        => B.BILL_DATE
                       ) SALE_CURATOR,   -- �������� 
                     M.LAST_NAME||'' ''||M.FIRST_NAME||'' ''||M.MIDDLE_NAME BILLING_CURATOR, -- ������� �������
                     ERR.CODE,           -- ��� ������
                     ERR.TYPE,           -- ��� ������
                     ERR.REASON,         -- �������������
                     B.BILL_DATE,        -- ���� �����
                     B.NOTES,             -- ����������
                     BR.CONTRACTOR        BRANCH,
                     AG.CONTRACTOR        AGENT
                FROM BILL_T B, ACCOUNT_T A, CONTRACT_T C, COMPANY_T CM, CURRENCY_T CR,
                     BILLING_CURATOR_T BC, MANAGER_T M, ERR,
                     ACCOUNT_PROFILE_T AP,
                     CONTRACTOR_T BR,
                     CONTRACTOR_T AG
               WHERE B.REP_PERIOD_ID = '|| p_period_id ||'
                 AND B.BILL_TYPE IN (''C'',''D'',''A'')
                 AND A.STATUS <> ''T''
                 AND B.ACCOUNT_ID  = A.ACCOUNT_ID
                 AND B.CONTRACT_ID = C.CONTRACT_ID
                 AND B.CONTRACT_ID = CM.CONTRACT_ID
                 AND B.CONTRACT_ID = BC.CONTRACT_ID(+)
                 AND BC.MANAGER_ID = M.MANAGER_ID(+)
                 AND B.CURRENCY_ID = CR.CURRENCY_ID
                 AND B.ERR_CODE_ID = ERR.KEY_ID(+)
                 AND B.PROFILE_ID  = AP.PROFILE_ID
                 AND ap.branch_id = br.contractor_id (+)
                 AND ap.agent_id = ag.contractor_id (+)
                 AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
              ORDER BY COMPANY_NAME, BILL_NO';                      
         OPEN p_recordset FOR 
               v_sql;
      END IF;  
      
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ����� �� ���������� ��������
PROCEDURE REP_BILL_QUEUE_LOG( 
     p_result          OUT VARCHAR2, 
     p_recordset       OUT t_refc,
     p_date_from       IN  DATE,
     p_date_to         IN  DATE,
     p_is_process      IN  NUMBER
)IS
    v_prcName    CONSTANT VARCHAR2(30) := 'REP_BILL_QUEUE_LOG';
    v_retcode    INTEGER;
BEGIN                
     OPEN p_recordset FOR 
          SELECT *
              FROM LL_H_BILL_QUEUE_LOG ll
            WHERE DATE_START BETWEEN TRUNC(p_date_from) AND TRUNC(p_date_to)+1-1/84600
                AND ((p_is_process IS NULL OR p_is_process <> 1) OR (p_is_process = 1 AND ll.queue_status <> 'FINISH'))
          ORDER BY ID DESC;
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- -------------------------------------------------------------------------------------
-- ����� �� ������� ��� �.������
-- -------------------------------------------------------------------------------------
PROCEDURE REP_ORDERS_FOR_LAPIN( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_contractor_id     IN  VARCHAR2
  )
IS
    v_prcName   CONSTANT varchar2(30) := 'REP_ORDERS_FOR_LAPIN';
    v_sql        VARCHAR2(10000);
BEGIN
    v_sql := '
      SELECT CT.CONTRACTOR, C.CONTRACT_NO, C.DATE_FROM CONTRACT_FROM, CM.COMPANY_NAME, 
             O.ORDER_NO, O.DATE_FROM ORDER_DATE_FROM, O.DATE_TO ORDER_DATE_TO, OI.SPEED_STR, OI.POINT_SRC, OI.POINT_DST, AC.PHONES, AC.PERSON 
        FROM CONTRACT_T C, COMPANY_T CM, 
             ACCOUNT_PROFILE_T AP, ACCOUNT_CONTACT_T AC, CONTRACTOR_T CT, 
             ORDER_T O, ORDER_INFO_T OI
       WHERE C.CONTRACT_ID = CM.CONTRACT_ID
         AND CM.DATE_FROM <= SYSDATE
         AND (CM.DATE_TO IS NULL OR SYSDATE < CM.DATE_TO)
         AND C.CONTRACT_ID = AP.CONTRACT_ID
         AND AP.DATE_FROM <= SYSDATE
         AND (AP.DATE_TO IS NULL OR SYSDATE < AP.DATE_TO)
         AND AP.ACCOUNT_ID = O.ACCOUNT_ID
         AND O.ORDER_ID    = OI.ORDER_ID
         AND AP.ACCOUNT_ID = AC.ACCOUNT_ID
         AND AC.ADDRESS_TYPE = ''DLV''
         AND AP.BRANCH_ID  = CT.CONTRACTOR_ID
         AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
      ORDER BY CT.CONTRACTOR, C.CONTRACT_NO, ORDER_NO';
      
      OPEN p_recordset FOR 
               v_sql;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- -------------------------------------------------------------------------------------
-- ����� �� ������������� �������� ��� ������������� �.�.
-- ��������� ������ ��������� � �������������
-- ����������� ��������� � ����������
-- -------------------------------------------------------------------------------------
PROCEDURE REP_RP_FOR_MIKHAYLOVSKY( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_period_id         IN INTEGER
  )
IS
    v_prcName   CONSTANT varchar2(30) := 'REP_RP_FOR_MIKHAYLOVSKY';
    v_sql        VARCHAR2(10000);
BEGIN
      OPEN p_recordset FOR 
        SELECT C.CONTRACT_NO, CL.CLIENT_NAME, CM.COMPANY_NAME,
               DM.NAME MARKET_SEGMENT, DC.NAME CSTTYPE,
               CA.CONTRACTOR AGENT,
               O.ORDER_NO,
               S.SERVICE,
               OI.POINT_SRC S_RGN,    
               NULL S_AGN,    
               OI.POINT_DST D_RGN,    
               NULL D_AGN,   
               OI.SPEED_STR,   
               NULL VOLUME,    
               I.REP_GROSS GROSS,    
               I.CHARGE_TYPE TYPE_OF_USAGE,    
               B.REP_PERIOD_ID,   
               B.BILL_DATE,    
               Pk402_Bcr_File.Get_sales_curator (
                   p_branch_id     => AP.BRANCH_ID,
                   p_agent_id      => CA.CONTRACTOR_ID,
                   p_contract_id   => C.CONTRACT_ID,
                   p_account_id    => AP.ACCOUNT_ID,
                   p_order_id      => O.ORDER_ID,
                   p_date          => B.BILL_DATE
                 ) SALES_NAME,    
               OI.SPEED_VALUE,    
               CT.CONTRACTOR BRAND,    
               B.BILL_ID
          FROM CONTRACTOR_T CT, ACCOUNT_PROFILE_T AP,
               CONTRACT_T C, CLIENT_T CL, COMPANY_T CM,
               DICTIONARY_T DM, DICTIONARY_T DC,
               CONTRACTOR_T CA, BILL_T B, ITEM_T I, ORDER_T O,
               SERVICE_T S, ORDER_INFO_T OI
         WHERE CT.CONTRACTOR LIKE '%(��)%'
           AND AP.BRANCH_ID    = CT.CONTRACTOR_ID
           AND AP.CONTRACT_ID  = C.CONTRACT_ID
           AND CL.CLIENT_ID    = C.CLIENT_ID
           AND CM.CONTRACT_ID  = C.CONTRACT_ID
           AND CM.ACTUAL       = 'Y'
           AND C.MARKET_SEGMENT_ID = DM.KEY_ID(+)
           AND C.CLIENT_TYPE_ID    = DC.KEY_ID(+)
           AND AP.AGENT_ID     = CA.CONTRACTOR_ID(+)
           AND AP.ACCOUNT_ID   = B.ACCOUNT_ID
           AND B.REP_PERIOD_ID = p_period_id
           AND B.BILL_ID       = I.BILL_ID
           AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
           AND I.ORDER_ID      = O.ORDER_ID
           AND O.SERVICE_ID    = S.SERVICE_ID
           AND O.ORDER_ID      = OI.ORDER_ID(+)
        ORDER BY CT.CONTRACTOR, C.CONTRACT_NO, B.BILL_ID, O.ORDER_NO
        ;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

--=================================================================================
-- �������� �� ��������� ������ � ��������
-- ��������� ������, � ������� �� ��������� ������������ ��������
PROCEDURE CHECK_BILLS_BY_COMPANY( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_contractor_id     VARCHAR2,
          p_period_id_from    INTEGER,
          p_period_id_to      INTEGER
  )
IS
    v_prcName   CONSTANT varchar2(30) := 'CHECK_BILL_NO_COMPANY';
    v_sql       VARCHAR2(10000);    
BEGIN
    IF p_period_id_from IS NULL OR p_period_id_to IS NULL THEN
       p_message := '������ ������ ���� �����';
    ELSE
       v_sql := '
          SELECT B.BILL_ID,
                 B.BILL_NO,
                 B.BILL_DATE,
                 B.REP_PERIOD_ID,
                 B.BILL_TYPE,
                 B.BILL_STATUS,
                 B.TOTAL,
                 A.ACCOUNT_NO,
                 BR.CONTRACTOR BRANCH
            FROM bill_t b,
                 account_profile_t ap,
                 contractor_t br,
                 account_t a
           WHERE     b.profile_id = ap.profile_id
                 AND br.contractor_id = ap.branch_id
                 AND A.ACCOUNT_ID = b.account_id
                 AND a.account_type = ''J''                 
                 AND rep_period_id BETWEEN '|| p_period_id_from ||' AND ' || p_period_id_to || '
                 AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))
                 AND NOT EXISTS
                            (SELECT *
                               FROM company_t comp
                              WHERE comp.contract_id = b.contract_id 
                                    AND TRIM (b.bill_date) BETWEEN COMP.DATE_FROM AND NVL ( comp.date_to, TO_DATE (''01.01.2050'',''DD.MM.YYYY'')))
        ORDER BY br.CONTRACTOR, B.REP_PERIOD_ID, B.BILL_NO';          
          
       OPEN p_recordset FOR 
             v_sql;
    END IF;  

EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

--=================================================================================
-- �������� �� ������ ���� � ��� (� �������)
PROCEDURE CHECK_CFO_IS_NULL( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_contractor_id     VARCHAR2,
          is_only_order_open  INTEGER,           --0/1
          is_only_cfo_null    INTEGER,           --0/1
          is_only_live        INTEGER            --������ ����� ������, �.�. ��, ������� ������� ����� 01/01/2016 � ���� ���������� �� 2016 ���
  )
IS
    v_prcName   CONSTANT varchar2(30) := 'CHECK_CFO_IS_NULL';
    v_sql       VARCHAR2(10000);    
BEGIN
     v_sql := '
        SELECT 
                O.ORDER_NO,
                O.DATE_FROM ORDER_DATE_FROM,
                O.DATE_TO ORDER_DATE_TO,
                O.STATUS ORDER_STATUS, 
                A.ACCOUNT_NO,
                CONTR.CONTRACT_NO,
                CONTR.DATE_FROM CONTRACT_DATE_FROM,
                COMP.COMPANY_NAME,
                CL.CLIENT_NAME,
                PK402_BCR_DATA.Get_sales_curator(ap.BRANCH_ID, ap.AGENT_ID, ap.CONTRACT_ID, ap.ACCOUNT_ID, NULL, SYSDATE) SALER_NAME,
                CFO.CFO_NAME,
                CFU.CFU_NAME,
                BR.CONTRACTOR BRANCH,
                O.ORDER_ID, 
                O.CFO_ID,
                A.ACCOUNT_ID,
                CONTR.CONTRACT_ID,
                A.BILLING_ID,
                BR.CONTRACTOR_ID BRANCH_ID        
          FROM ORDER_T O,
               ACCOUNT_T A,       
               ACCOUNT_PROFILE_T AP,
               CONTRACT_T CONTR,
               COMPANY_T COMP,
               CONTRACTOR_T BR,
               CLIENT_T CL,
               (
                  select KEY_ID CFO_ID, NAME CFO_NAME from dictionary_t 
                  where parent_id = 31
               ) cfo,
               (
                  select KEY_ID CFU_ID, NAME CFU_NAME from dictionary_t 
                  where parent_id = 32
               ) cfu
         WHERE 
            O.ACCOUNT_ID = A.ACCOUNT_ID
            AND A.ACCOUNT_ID = AP.ACCOUNT_ID
            AND AP.ACTUAL = ''Y''
            AND AP.CONTRACT_ID = CONTR.CONTRACT_ID
            AND AP.CONTRACT_ID = COMP.CONTRACT_ID
            AND COMP.ACTUAL = ''Y''
            AND CONTR.CLIENT_ID = CL.CLIENT_ID
            AND AP.BRANCH_ID = BR.CONTRACTOR_ID 
            AND A.ACCOUNT_TYPE = ''J''
            AND O.CFO_ID = cfo.cfo_id (+)
            AND CONTR.CFU_ID = cfu.cfu_id (+)';
            
      IF is_only_live =1 THEN
          v_sql := v_sql || ' AND (O.DATE_FROM >= TO_DATE(''01.01.2016'',''DD.MM.YYYY'') OR O.DATE_TO IS NULL OR O.DATE_TO >= TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
            AND EXISTS (
               select * from item_t i
               where rep_period_id >= 201601
               and I.ORDER_ID = O.ORDER_ID
            )';
      END IF;      

      IF p_contractor_id IS NOT NULL THEN
         v_sql := v_sql || 'AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))'  ;
      END IF;                
            
      IF is_only_cfo_null = 1 THEN
          v_sql := v_sql || ' AND CFO_NAME IS NULL';
      END IF;      
      
      IF is_only_order_open = 1 THEN
          v_sql := v_sql || ' AND O.DATE_TO >= SYSDATE';
      END IF;      

     INSERT INTO TMP_SQL_LOG(SQL) VALUES(v_sql);
     COMMIT;

     OPEN p_recordset FOR 
           v_sql;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

--=================================================================================
-- �������� �� ������ ���� � ��� (� ���������)
PROCEDURE CHECK_CFU_IS_NULL( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_contractor_id     VARCHAR2,
          is_has_order_open   INTEGER,           --0/1
          is_only_cfu_null    INTEGER            --0/1
  )
IS
    v_prcName   CONSTANT varchar2(30) := 'CHECK_CFU_IS_NULL';
    v_sql       VARCHAR2(10000);    
BEGIN
     v_sql := '
        SELECT * FROM  (
              SELECT          
                      A.ACCOUNT_NO,
                      CONTR.CONTRACT_NO,
                      CONTR.DATE_FROM CONTRACT_DATE_FROM,
                      COMP.COMPANY_NAME,
                      CL.CLIENT_NAME,
                      PK402_BCR_DATA.Get_sales_curator(ap.BRANCH_ID, ap.AGENT_ID, ap.CONTRACT_ID, ap.ACCOUNT_ID, NULL, SYSDATE) SALER_NAME,
                      CFU.CFU_NAME,
                      BR.CONTRACTOR BRANCH,
                      NVL(OC.CNT,0) ORDER_COUNT,
                      CONTR.CFU_ID,       
                      A.ACCOUNT_ID,
                      CONTR.CONTRACT_ID,
                      A.BILLING_ID,
                      BR.CONTRACTOR_ID BRANCH_ID            
                FROM ACCOUNT_T A,       
                     ACCOUNT_PROFILE_T AP,
                     CONTRACT_T CONTR,
                     COMPANY_T COMP,
                     CONTRACTOR_T BR,
                     CLIENT_T CL,
                     (
                        select KEY_ID CFU_ID, NAME CFU_NAME from dictionary_t 
                        where parent_id = 32
                     ) cfu,
                     (
                          SELECT ACCOUNT_ID, COUNT(*) CNT FROM ORDER_T';
      
          IF is_has_order_open = 1 THEN
              v_sql := v_sql || ' WHERE DATE_TO > SYSDATE';
          END IF;                            
                          
            v_sql := v_sql || ' GROUP BY ACCOUNT_ID
                      ) OC 
               WHERE 
                  A.ACCOUNT_ID = AP.ACCOUNT_ID
                  AND AP.ACTUAL = ''Y''
                  AND AP.CONTRACT_ID = CONTR.CONTRACT_ID
                  AND AP.CONTRACT_ID = COMP.CONTRACT_ID
                  AND COMP.ACTUAL = ''Y''
                  AND CONTR.CLIENT_ID = CL.CLIENT_ID
                  AND AP.BRANCH_ID = BR.CONTRACTOR_ID 
                  AND A.ACCOUNT_TYPE = ''J''
                  AND CONTR.CFU_ID = cfu.cfu_id (+)
                  AND A.ACCOUNT_ID = OC.ACCOUNT_ID (+)';
        IF p_contractor_id IS NOT NULL THEN
           v_sql := v_sql || 'AND (ap.branch_id IN ('|| p_contractor_id ||') OR ap.agent_id in ('|| p_contractor_id ||'))'  ;
        END IF;          
                  
       v_sql := v_sql ||  ' )    
          WHERE
               ORDER_COUNT > 0';
                                          
      IF is_only_cfu_null = 1 THEN
          v_sql := v_sql || ' AND CFU_NAME IS NULL';
      END IF;      

     OPEN p_recordset FOR 
           v_sql;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

--======================================================================
-- ��������� ������ �����
PROCEDURE REPORT_SERVICE_LIST( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc
)
IS
    v_prcName   CONSTANT varchar2(30) := 'REPORT_SERVICE_LIST';
BEGIN
    OPEN p_recordset FOR
         SELECT SERVICE_ID, SERVICE, SERVICE_ERP
              FROM service_t
             WHERE service_id > 0
          ORDER BY 1;
EXCEPTION
    WHEN others THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;     

--=========================================================================
-- ������ ��� ��������� ������� (����������� �� ������ � ��� ���������)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ����� �� �������� BRM
--
PROCEDURE REP_TTK_ABONENTS( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc
  )
IS
    v_prcName   CONSTANT varchar2(30) := 'REP_TTK_ABONENTS';
    v_sql        VARCHAR2(10000);
BEGIN
      OPEN p_recordset FOR 
        SELECT 'BRM' SYSTEM, A.ACCOUNT_ID, A.STATUS ACCOUNT_STATUS, CM.COMPANY_NAME, CT.CONTRACTOR, BR.CONTRACTOR BRANCH, 
               C.DATE_FROM CONTRACT_DATE, C.CONTRACT_NO, A.ACCOUNT_NO, NULL SEGMENT, NULL SUBSEGMENT,
               CM.INN, AP.KPP, 
               PK402_BCR_FILE.GET_SALES_CURATOR(AP.BRANCH_ID, AP.AGENT_ID, C.CONTRACT_ID, A.ACCOUNT_ID, NULL, SYSDATE) SALES_CURATOR,
               NULL MANAGER_TOOLS, NULL CONTRACT_STATUS, NULL COTRACTOR_STATUS_DATE, NULL VECTOR,  
               AJ.ZIP||', '||AJ.STATE||', '||AJ.CITY||', '||AJ.ADDRESS  JUR_ADDRESS,
               AD.ZIP||', '||AD.STATE||', '||AD.CITY||', '||AD.ADDRESS  DLV_ADDRESS,
               CFU.NAME CFU, CFU.KEY CFU_SEGMENT, CM.ERP_CODE, CE.EISUP_CONTRACT_CODE,
               (
                 SELECT MAX(B.REP_PERIOD_ID) FROM BILL_T B
                  WHERE B.ACCOUNT_ID = A.ACCOUNT_ID
               ) MAX_BILL_PERIOD_ID     
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, COMPANY_T CM,
               CONTRACTOR_T CT, CONTRACTOR_T BR, 
               ACCOUNT_CONTACT_T AJ, ACCOUNT_CONTACT_T AD, DICTIONARY_T CFU,
               CONTRACT_EISUP_T CE
         WHERE A.ACCOUNT_ID    = AP.ACCOUNT_ID
           AND AP.ACTUAL       = 'Y' 
           AND AP.CONTRACT_ID  = C.CONTRACT_ID
           AND AP.CONTRACT_ID  = CM.CONTRACT_ID
           AND CM.ACTUAL       = 'Y'
           AND AP.CONTRACTOR_ID= CT.CONTRACTOR_ID
           AND AP.BRANCH_ID    = BR.CONTRACTOR_ID
           AND A.ACCOUNT_TYPE  = 'J'
           AND A.ACCOUNT_ID    = AJ.ACCOUNT_ID
           AND AJ.DATE_FROM    < SYSDATE
           AND (AJ.DATE_TO IS NULL OR SYSDATE < AJ.DATE_TO)
           AND AJ.ADDRESS_TYPE = 'JUR'
           AND A.ACCOUNT_ID    = AD.ACCOUNT_ID
           AND AD.DATE_FROM    < SYSDATE
           AND (AD.DATE_TO IS NULL OR SYSDATE < AD.DATE_TO)
           AND AD.ADDRESS_TYPE = 'DLV'
           AND CFU.KEY_ID(+)   = C.CFU_ID
           AND CFU.PARENT_ID(+)= 32
           AND A.ACCOUNT_ID    = CE.ACCOUNT_ID(+)
         ORDER BY C.DATE_FROM DESC, C.CONTRACT_NO
        ;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;
--
-- ����� �� ����������� BRM
--
PROCEDURE REP_TTK_BILL_ITEMS( 
          p_message         OUT VARCHAR2, 
          p_recordset       OUT t_refc,
          p_from_period_id  IN INTEGER,
          p_to_period_id    IN INTEGER
  )
IS
    v_prcName   CONSTANT varchar2(30) := 'REP_TTK_BILL_ITEMS';
    v_sql        VARCHAR2(10000);
BEGIN
      OPEN p_recordset FOR 
        SELECT * 
          FROM (
            SELECT B.ACCOUNT_ID, A.ACCOUNT_NO, 
                   B.REP_PERIOD_ID BILL_PERIOD_ID, B.BILL_NO, B.BILL_DATE, BT.NAME BILL_TYPE,  
                   IC.NAME CHARGE_TYPE, 
                   IT.NAME ITEM_TYPE, S.ERP_PRODCODE, S.SERVICE, SS.SUBSERVICE, I.DESCR ITEM_DESCR, 
                   I.DATE_FROM ITEM_DATE_FROM, I.DATE_TO ITEM_DATE_TO, 
                   CASE
                    WHEN I.CHARGE_TYPE = 'REC' AND OB.QUANTITY IS NOT NULL THEN '��'
                    ELSE NULL
                   END UNIT,  
                   OB.QUANTITY,
                   I.REP_GROSS, (I.REP_GROSS + I.REP_TAX) TOTAL,
                   CASE
                      WHEN I.CHARGE_TYPE = 'REC' THEN PK09_INVOICE.Calc_gross(
                          OB.RATE_VALUE,
                          OB.TAX_INCL,
                          AP.VAT
                       )  
                   END ABP_GROSS,
                   CASE
                      WHEN I.CHARGE_TYPE = 'REC' THEN PK09_INVOICE.Calc_total(
                          OB.RATE_VALUE,
                          OB.TAX_INCL,
                          AP.VAT
                       )  
                   END ABP_TOTAL,
                   OB.TAX_INCL, 
                   O.ORDER_NO,
                   O.DATE_FROM ORDER_DATE_FROM,
                   O.DATE_TO ORDER_DATE_TO,
                   R.RATEPLAN_NAME,
                   CFO.KEY CFO_CODE, CFO.NAME CFO
              FROM ACCOUNT_T A, BILL_T B, ITEM_T I, SERVICE_T S, ORDER_T O, 
                   SUBSERVICE_T SS, ORDER_BODY_T OB, RATEPLAN_T R, ACCOUNT_PROFILE_T AP, 
                   DICTIONARY_T CFO, DICTIONARY_T BT, DICTIONARY_T IC, DICTIONARY_T IT 
             WHERE A.ACCOUNT_ID    = B.ACCOUNT_ID
               AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
               AND I.BILL_ID       = B.BILL_ID
               AND B.REP_PERIOD_ID BETWEEN p_from_period_id AND p_to_period_id  
               AND I.SERVICE_ID    = S.SERVICE_ID
               AND I.ORDER_ID      = O.ORDER_ID
               AND I.ORDER_BODY_ID = OB.ORDER_BODY_ID
               AND OB.RATEPLAN_ID  = R.RATEPLAN_ID(+)
               AND B.PROFILE_ID    = AP.PROFILE_ID
               AND O.CFO_ID        = CFO.KEY_ID(+)
               AND BT.PARENT_ID    = 3
               AND BT.KEY          = B.BILL_TYPE
               AND IC.PARENT_ID    = 7
               AND IC.KEY          = I.CHARGE_TYPE
               AND I.SUBSERVICE_ID = SS.SUBSERVICE_ID
               AND IT.PARENT_ID    = 5
               AND IT.KEY          = I.ITEM_TYPE
        );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

--======================================================================
-- ��������� ������ ������, � ������� �� ���������� ERP_CODE � �������
PROCEDURE REP_BILL_WITH_KKODE_NULL( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_period_id         IN  NUMBER
)
IS
    v_prcName   CONSTANT varchar2(30) := 'REP_BILL_WITH_KKODE_NULL';
BEGIN
    OPEN p_recordset FOR
         SELECT B.BILL_ID, B.BILL_NO, B.BILL_DATE, B.BILL_TYPE,B.BILL_STATUS, B.TOTAL, A.ACCOUNT_NO, C.CONTRACT_NO, C.date_from contract_date, COMP.COMPANY_NAME, COMP.INN, AP.KPP, br.contractor region,
                 PK402_BCR_DATA.Get_sales_curator(ap.BRANCH_ID, ap.AGENT_ID, ap.CONTRACT_ID, ap.ACCOUNT_ID, NULL, B.BILL_DATE) SALES_NAME
            FROM bill_t b,
                 ACCOUNT_PROFILE_T AP,
                 COMPANY_T COMP,
                 CONTRACT_T C,
                 ACCOUNT_T A,
                 CONTRACTOR_T BR
           WHERE     b.profile_id = ap.profile_id
                 AND comp.contract_id = ap.contract_id
                 AND A.ACCOUNT_ID = AP.ACCOUNT_ID
                 AND b.contract_id = c.contract_id
                 AND ap.branch_id = BR.CONTRACTOR_ID       
                 AND B.BILL_DATE BETWEEN COMP.DATE_FROM AND NVL (COMP.DATE_TO, TO_DATE ('01.01.2050', 'DD.MM.YYYY'))
                 AND COMP.ERP_CODE IS NULL
                 AND A.BILLING_ID NOT IN (2000,2003,2008)
                 AND A.ACCOUNT_TYPE = 'J'
                 AND B.REP_PERIOD_ID = p_period_id;
EXCEPTION
    WHEN others THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END; 

--======================================================================
-- ��������� ������ ������, ������� ����� �������� � ����� � ��������� �������
PROCEDURE REP_BILL_FOR_EISUP( 
          p_message           OUT VARCHAR2, 
          p_recordset         OUT t_refc,
          p_period_id         IN  NUMBER
)
IS
    v_prcName   CONSTANT varchar2(30) := 'REP_BILL_FOR_EISUP';
BEGIN
    OPEN p_recordset FOR
         SELECT B.BILL_ID, B.BILL_NO, B.BILL_DATE, 
                B.BILL_TYPE,B.BILL_STATUS, B.TOTAL, 
                A.ACCOUNT_NO, C.CONTRACT_NO, 
                C.DATE_FROM CONTRACT_DATE, CM.COMPANY_NAME, 
                CM.INN, AP.KPP, BR.CONTRACTOR REGION,
                 PK402_BCR_DATA.Get_sales_curator(AP.BRANCH_ID, AP.AGENT_ID, AP.CONTRACT_ID, AP.ACCOUNT_ID, NULL, B.BILL_DATE) SALES_NAME
            FROM BILL_T B,
                 ACCOUNT_PROFILE_T AP,
                 COMPANY_T CM,
                 CONTRACT_T C,
                 ACCOUNT_T A,
                 CONTRACTOR_T BR
           WHERE B.PROFILE_ID   = AP.PROFILE_ID
             AND CM.CONTRACT_ID = AP.CONTRACT_ID
             AND A.ACCOUNT_ID   = AP.ACCOUNT_ID
             AND b.contract_id  = C.CONTRACT_ID
             AND AP.BRANCH_ID   = BR.CONTRACTOR_ID       
             AND B.BILL_DATE BETWEEN CM.DATE_FROM AND NVL (CM.DATE_TO, TO_DATE ('01.01.2050', 'DD.MM.YYYY'))
             AND A.BILLING_ID NOT IN (2000,2003,2008)
             AND A.ACCOUNT_TYPE = 'J'
             AND B.REP_PERIOD_ID = p_period_id;
EXCEPTION
    WHEN others THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;





END PK66_MINREPORT;
/
