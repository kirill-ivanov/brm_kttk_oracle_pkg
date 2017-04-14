CREATE OR REPLACE PACKAGE PK402_BCR_FILE
IS
    --
    -- � � � � � �   � � �   � � � � � � � �   �   B C R  ( �. �. ����� )
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK402_BCR_FILE';
    -- ==============================================================================
    c_RET_OK      CONSTANT INTEGER     := 0;
    c_RET_ER		  CONSTANT INTEGER     :=-1;
    �_BS_DATA_DIR CONSTANT VARCHAR2(11):= 'BS_DATA_DIR';
    �_BCR_DIR     CONSTANT VARCHAR2(7) := 'BCR_DIR';
    �_AGENT_DIR   CONSTANT VARCHAR2(9) := 'AGENT_DIR';
    
    type t_refc is ref cursor;
    
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

    -- ITEMS
    PROCEDURE Brm_items_to_file(p_period_id IN INTEGER);

    -- BILLS
    PROCEDURE Brm_bills_to_file(p_period_id IN INTEGER);
      
    -- ORDERS
    PROCEDURE Brm_orders_to_file;--(p_period_id IN INTEGER);

    -- ACCOUNTS
    PROCEDURE Brm_accounts_to_file; --(p_period_id IN INTEGER);

    -- CLIENTS
    PROCEDURE Brm_clients_to_file;

    -- PHONES
    PROCEDURE Brm_phones_to_file;

    -- DELIVERY
    PROCEDURE Brm_delivery_to_file;
    
END PK402_BCR_FILE;
/
CREATE OR REPLACE PACKAGE BODY PK402_BCR_FILE
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

-- --------------------------------------------------------------------------------
-- ITEMS
-- --------------------------------------------------------------------------------
PROCEDURE Brm_items_to_file(p_period_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Export_items_to_file';
    v_output      UTL_FILE.file_type;
    v_dir         VARCHAR2(100)      := �_BCR_DIR;
    v_file_name   VARCHAR2(100 CHAR) := 'items.csv';
    v_file_tmp    VARCHAR2(100 CHAR) := 'items.tmp';
    v_count       INTEGER;
    v_period_from DATE;
    v_period_to   DATE;
    v_hdr         VARCHAR2(2000);
    
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);
    -- ------------------------------------------------------------------ --
    -- ���������� ���������� � ����
    -- ------------------------------------------------------------------ --
    v_count := 0;
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W', 32767 );

    -- ��������� ���������
    v_hdr :='REP_PERIOD;BILL_ID;BILL_NO;ORDER_ID;ORDER_NO;'||
            'INV_ITEM_ID;INV_ITEM_NAME;IV_DATE_FROM;IV_DATE_TO;'||
            'SERVICE_ID;SERVICE;SERVICE_CODE;SERVICE_REP_ID;'||
            'SUBSERVICE_ID;SUBSERVICE;SUBSERVICE_KEY;'||
            'ITEM_ID;ITEM_TYPE;CHARGE_TYPE;I_DATE_FROM;I_DATE_TO;REP_GROSS;REP_TAX;'||
            'MINS;SALES_MANAGER';
    
    UTL_FILE.put_line(v_output, CONVERT(v_hdr, 'CL8MSWIN1251'));
    
    -- ��������� ������
    FOR item IN (
        WITH BDR AS( -- ������ ������
            -- ������� ��. ����
            SELECT DJ.ITEM_ID, SUM(MINUTES) MINS 
              FROM DETAIL_MMTS_T_JUR DJ
             WHERE DJ.REP_PERIOD_ID = p_period_id
             GROUP BY DJ.ITEM_ID
            UNION ALL
            -- ������� ���. ����
            SELECT DF.ITEM_ID, SUM(DF.MINS_SUM) MINS
              FROM DETAIL_MMTS_T_FIZ DF
             WHERE DF.REP_PERIOD_ID = p_period_id
             GROUP BY DF.ITEM_ID
            UNION ALL
            -- ���������
            SELECT ITEM_ID, SUM(BILL_MINUTES) MINS
              FROM BDR_OPER_T
             WHERE REP_PERIOD BETWEEN v_period_from AND v_period_to
            GROUP BY ITEM_ID ),
         PF AS (
            SELECT AP.ACCOUNT_ID, 
                   AP.PROFILE_ID,
                   AP.CONTRACT_ID,
                   AP.CONTRACTOR_ID, 
                   CA.CONTRACTOR, 
                   AP.CONTRACTOR_BANK_ID BANK_ID, 
                   CB.NOTES BANK,
                   NULL BRAND_ID, -- AP.BRAND_ID, -- ������ �� ������� � BRAND_T!!!
                   NULL BRAND,    -- BR.BRAND,    -- ������ �� ������� � BRAND_T!!!
                   BRANCH.CONTRACTOR_ID XTTK_ID, 
                   BRANCH.CONTRACTOR XTTK, 
                   BRANCH.EXTERNAL_ID XTTK_OBJ_ID0,
                   AGENT.CONTRACTOR_ID AGENT_ID,      
                   AGENT.CONTRACTOR  AGENT, 
                   AGENT.EXTERNAL_ID AGENT_OBJ_ID0,
                   AP.DATE_FROM, 
                   NVL(AP.DATE_TO,SYSDATE) DATE_TO
              FROM ACCOUNT_PROFILE_T AP, --BRAND_T BR, 
                   CONTRACTOR_BANK_T CB,
                   CONTRACTOR_T BRANCH, 
                   CONTRACTOR_T AGENT, 
                   CONTRACTOR_T CA  
             WHERE AP.CONTRACTOR_ID      = CA.CONTRACTOR_ID 
               --AND AP.BRAND_ID           = BR.BRAND_ID(+)
               AND DECODE(AP.BRANCH_ID,200,11,AP.BRANCH_ID)= BRANCH.CONTRACTOR_ID(+)
               AND DECODE(AP.BRANCH_ID,200,AP.BRANCH_ID,NVL(AP.AGENT_ID,AP.BRANCH_ID)) = AGENT.CONTRACTOR_ID(+)
               AND AP.CONTRACTOR_BANK_ID = CB.BANK_ID(+)
        )
        SELECT TO_CHAR(TRUNC(v_period_to),'yyyy.mm.dd')||';'|| -- REP_PERIOD, 
               B.BILL_ID||';'||
               '"'||REPLACE(B.BILL_NO,'"','""')||'";'||
               O.ORDER_ID||';'||
               '"'||REPLACE(O.ORDER_NO,'"','""')||'";'||
               IV.INV_ITEM_ID||';'||
               '"'||REPLACE(IV.INV_ITEM_NAME,'"','""')||'";'||
               TO_CHAR(IV.DATE_FROM,'yyyy.mm.dd')||';'||           -- IV_DATE_FROM,
               TO_CHAR(TRUNC(IV.DATE_TO,'DD'),'yyyy.mm.dd')||';'|| --  IV_DATE_TO,
               I.SERVICE_ID||';'||
               '"'||REPLACE(S.SERVICE,'"','""')||'";'||
               '"'||REPLACE(S.SERVICE_CODE,'"','""')||'";'||
               NVL(S.EXTERNAL_ID,9999)||';'|| -- SERVICE_REP_ID,
               I.SUBSERVICE_ID||';'||
               '"'||REPLACE(SS.SUBSERVICE,'"','""')||'";'||
               '"'||REPLACE(SS.SUBSERVICE_KEY,'"','""')||'";'||
               I.ITEM_ID||';'||
               '"'||REPLACE(I.ITEM_TYPE,'"','""')||'";'||
               '"'||REPLACE(I.CHARGE_TYPE,'"','""')||'";'||
               TO_CHAR(I.DATE_FROM,'yyyy.mm.dd')||';'|| -- I_DATE_FROM, 
               TO_CHAR(I.DATE_TO,'yyyy.mm.dd')||';'|| --  I_DATE_TO, 
               I.REP_GROSS||';'|| 
               I.REP_TAX||';'||
               ROUND(BDR.MINS,6)||';'|| -- MINS,
               PK402_BCR_DATA.Get_sales_curator (
                       p_branch_id      => PF.XTTK_ID,
                       p_agent_id       => PF.AGENT_ID,
                       p_contract_id    => PF.CONTRACT_ID,
                       p_account_id     => B.ACCOUNT_ID,
                       p_order_id       => O.ORDER_ID,
                       p_date           => v_period_to
                     ) --SALES_MANAGER
            AS TXT
          FROM BILL_T B, 
               ACCOUNT_T A, 
               ITEM_T I, 
               ORDER_T O, 
               BDR, 
               PF, 
               INVOICE_ITEM_T IV, 
               SERVICE_T S, 
               SUBSERVICE_T SS
         WHERE B.REP_PERIOD_ID = p_period_id
           AND B.ACCOUNT_ID    = A.ACCOUNT_ID
           AND B.BILL_ID       = I.BILL_ID
           AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
           AND O.ORDER_ID      = I.ORDER_ID
           AND B.PROFILE_ID    = PF.PROFILE_ID
           AND I.ITEM_ID       = BDR.ITEM_ID(+)
           AND I.REP_PERIOD_ID = IV.REP_PERIOD_ID
           AND I.INV_ITEM_ID   = IV.INV_ITEM_ID
           AND I.SERVICE_ID    = S.SERVICE_ID
           AND I.SUBSERVICE_ID = SS.SUBSERVICE_ID
           AND B.TOTAL <> 0 
           AND B.BILL_STATUS IN (
                   Pk00_Const.c_BILL_STATE_READY,
                   Pk00_Const.c_BILL_STATE_CLOSED
               )
           AND A.BILLING_ID IN (2000,2001,2002,2003,2006) 
           AND A.STATUS = Pk00_Const.c_ACC_STATUS_BILL -- 'B'
    ) LOOP
        UTL_FILE.put_line( v_output, CONVERT(item.txt,'CL8MSWIN1251') ) ;
        v_count := v_count + 1;
        
        IF MOD(v_count,10000) = 0 THEN
            Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------------------
-- BILLS
-- --------------------------------------------------------------------------------
PROCEDURE Brm_bills_to_file(p_period_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Brm_bills_to_file';
    v_output      UTL_FILE.file_type;
    v_dir         VARCHAR2(100)      := �_BCR_DIR;
    v_file_name   VARCHAR2(100 CHAR) := 'bills_raw.csv';
    v_file_tmp    VARCHAR2(100 CHAR) := 'bills_raw.tmp';
    v_count       INTEGER;
    v_period_to   DATE;
    v_hdr         VARCHAR2(400);
    v_buffer      RAW(1000);
    
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    v_period_to   := Pk04_Period.Period_to(p_period_id);
    -- ------------------------------------------------------------------ --
    -- ���������� ���������� � ����
    -- ------------------------------------------------------------------ --
    v_count := 0;
    
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W', 32767 );

    -- ��������� ���������
    v_hdr :='REP_PERIOD;BILL_ID;ACCOUNT_STATUS;'||
            'PREV_BILL_ID;NEXT_BILL_ID;'||
            'BILL_TYPE;BILL_NO;BILL_DATE;ACT_DATE_FROM;ACT_DATE_TO;'||
            'CURRENCY_ID;TOTAL;GROSS;TAX;'||
            'ACCOUNT_ID;ACCOUNT_NO;ACCOUNT_TYPE;'||
            'CONTRACTOR_ID;CONTRACTOR;BRAND_ID;BRAND;'||
            'XTTK_ID;XTTK;XTTK_OBJ_ID0;'||
            'AGENT_ID;AGENT;AGENT_OBJ_ID0;'||
            'BANK_ID;BANK';
    --UTL_FILE.put_line(v_output, CONVERT(v_hdr,'CL8MSWIN1251'));
    UTL_FILE.put_line(v_output, v_hdr);
    -- ��������� ������
    FOR bill IN (
        SELECT TO_CHAR(v_period_to,'yyyy.mm.dd')||';'||   --REP_PERIOD, 
               B.BILL_ID||';'|| 
               A.STATUS||';'|| -- account_status,
               B.PREV_BILL_ID||';'||
               B.NEXT_BILL_ID||';'||
               B.BILL_TYPE||';'||
               '"'||REPLACE(B.BILL_NO,'"','""')||'";'||
               TO_CHAR(B.BILL_DATE,'yyyy.mm.dd')||';'||   --BILL_DATE, 
               TO_CHAR(B.ACT_DATE_FROM,'yyyy.mm.dd')||';'||
               TO_CHAR(B.ACT_DATE_TO,'yyyy.mm.dd')||';'|| -- ACT_DATE_TO,
               B.CURRENCY_ID||';'||
               B.TOTAL||';'||
               B.GROSS||';'||
               B.TAX||';'||
               B.ACCOUNT_ID||';'|| 
               --';'
               '"'||REPLACE(A.ACCOUNT_NO,'"','""')||'";'||
               '"'||REPLACE(A.ACCOUNT_TYPE,'"','""')||'";'||
               B.CONTRACTOR_ID||';'||
               '"'||REPLACE(CA.CONTRACTOR,'"','""')||'";'||
               ';'||                       -- AP.BRAND_ID, 
               ';'||                       -- NULL BR.BRAND,
               BRANCH.CONTRACTOR_ID||';'|| -- XTTK_ID, 
               '"'||REPLACE(BRANCH.CONTRACTOR,'"','""')||'";'||    -- XTTK, 
               --';'
               BRANCH.EXTERNAL_ID||';'||   -- XTTK_OBJ_ID0,
               AGENT.CONTRACTOR_ID||';'||  -- AGENT_ID, 
               '"'||REPLACE(AGENT.CONTRACTOR,'"','""')||'";'||     -- AGENT, 
               AGENT.EXTERNAL_ID||';'||    -- AGENT_OBJ_ID0,
               B.CONTRACTOR_BANK_ID||';'|| -- BANK_ID, 
               '"'||REPLACE(TRIM(CB.NOTES),'"','""')||'"'          -- BANK
               AS TXT
          FROM BILL_T B, 
               ACCOUNT_T A, 
               ACCOUNT_PROFILE_T AP, 
               --BRAND_T BR,               -- ������ �� �������
               CONTRACTOR_BANK_T CB,
               CONTRACTOR_T BRANCH, 
               CONTRACTOR_T AGENT, 
               CONTRACTOR_T CA
         WHERE B.REP_PERIOD_ID = p_period_id
           AND B.ACCOUNT_ID    = A.ACCOUNT_ID
           AND B.PROFILE_ID    = AP.PROFILE_ID
           AND B.CONTRACTOR_ID = CA.CONTRACTOR_ID
           AND B.CONTRACTOR_BANK_ID = CB.BANK_ID
           --AND AP.BRAND_ID = BR.BRAND_ID(+)
           AND DECODE(AP.BRANCH_ID,200,11,AP.BRANCH_ID) = BRANCH.CONTRACTOR_ID(+)
           AND DECODE(AP.BRANCH_ID,200,AP.BRANCH_ID,NVL(AP.AGENT_ID,AP.BRANCH_ID)) = AGENT.CONTRACTOR_ID(+)
           AND B.TOTAL <> 0 
           AND B.BILL_STATUS IN (
                   Pk00_Const.c_BILL_STATE_READY,
                   Pk00_Const.c_BILL_STATE_CLOSED
               )
           AND A.BILLING_ID IN (2000,2001,2002,2003,2006)
           AND A.STATUS = Pk00_Const.c_ACC_STATUS_BILL -- 'B'
    ) LOOP
--        BEGIN
          --UTL_FILE.put_line( v_output, CONVERT(bill.txt,'CL8MSWIN1251')) ;
          --UTL_FILE.put_line( v_output, bill.txt) ;
          -- ����� � ����, ������ ���, �� ��� ������� ����� �����
          v_buffer := UTL_RAW.convert( UTL_RAW.cast_to_raw(bill.txt),
                   'AMERICAN_AMERICA.CL8MSWIN1251','AMERICAN_AMERICA.AL32UTF8'); 
          UTL_FILE.put_line( v_output, UTL_RAW.cast_to_varchar2(v_buffer)) ;
--        EXCEPTION 
--          WHEN OTHERS THEN
--            Pk01_Syslog.Write_error('ERR:', c_PkgName||'.'||v_prcName );
--            Pk01_Syslog.Write_msg('ERR: ['||bill.txt||']', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );
--        END;
        v_count := v_count + 1;
        
        IF MOD(v_count,10000) = 0 THEN
            Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR. Count = '||v_count, c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------------------
-- ORDERS
-- --------------------------------------------------------------------------------
PROCEDURE Brm_orders_to_file
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Brm_orders_to_file';
    v_output      UTL_FILE.file_type;
    v_dir         VARCHAR2(100)      := �_BCR_DIR;
    v_file_name   VARCHAR2(100 CHAR) := 'orders.csv';
    v_file_tmp    VARCHAR2(100 CHAR) := 'orders.tmp';
    v_count       INTEGER;
    v_hdr         VARCHAR2(2000);
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ------------------------------------------------------------------ --
    -- ���������� ���������� � ����
    -- ------------------------------------------------------------------ --
    v_count := 0;
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W', 32767 );

    -- ��������� ���������
    v_hdr := 'ORDER_ID;ACCOUNT_ID;ORDER_NO;DATE_FROM;DATE_TO;'||
             'SERVICE_ID;SERVICE_CODE;SERVICE_REP_ID;'||
             'RATEPLAN_ID;RATEPLAN_NAME;AGENT_PERCENT;'||
             'POINT_SRC;POINT_DST;SPEED_STR;SPEED_VALUE';
    
    UTL_FILE.put_line(v_output, CONVERT(v_hdr,'CL8MSWIN1251'));
    
    -- ��������� ������
    FOR ord IN (
        WITH D AS (
            SELECT 
                O.ORDER_ID,
                O.ACCOUNT_ID,
                O.ORDER_NO, 
                O.DATE_FROM,
                TRUNC(DECODE(SIGN(O.DATE_TO-O.DATE_FROM),-1,O.DATE_FROM,O.DATE_TO),'DD') DATE_TO, 
                O.SERVICE_ID,
                S.SERVICE_CODE,
                S.EXTERNAL_ID SERVICE_REP_ID,
                P.RATEPLAN_ID,
                P.RATEPLAN_NAME,
                P.AGENT_PERCENT,
                ROW_NUMBER() OVER (PARTITION BY OB.ORDER_ID ORDER BY DECODE(OB.CHARGE_TYPE, 'USG', 1, 'REC', 2, 3)) RN,
                I.POINT_SRC, I.POINT_DST, I.SPEED_STR, 
                I.SPEED_VALUE * POWER(1024, 2 - d.external_id) SPEED_VALUE
             FROM ORDER_T O, 
                  SERVICE_T S, 
                  ACCOUNT_T A, 
                  ORDER_BODY_T OB, 
                  RATEPLAN_T P,
                  ORDER_INFO_T I,
                  DICTIONARY_T D
            WHERE O.SERVICE_ID = S.SERVICE_ID
              AND O.ORDER_NO IS NOT NULL
              AND O.ACCOUNT_ID = A.ACCOUNT_ID         
              AND A.BILLING_ID IN (2000,2001,2002,2003,2006) 
              AND O.ORDER_ID = OB.ORDER_ID
              AND OB.RATEPLAN_ID = P.RATEPLAN_ID (+)
              AND O.ORDER_ID = I.ORDER_ID (+)
              AND D.PARENT_ID(+) = 67
              AND I.SPEED_UNIT_ID = D.KEY_ID (+)
        )
        SELECT ORDER_ID||';'||
               ACCOUNT_ID||';'||
               '"'||REPLACE(ORDER_NO,'"','""')||'";'||
               TO_CHAR(DATE_FROM,'yyyy.mm.dd')||';'||
               TO_CHAR(DATE_TO,'yyyy.mm.dd')||';'||
               SERVICE_ID||';'||
               '"'||REPLACE(SERVICE_CODE,'"','""')||'";'||
               SERVICE_REP_ID||';'||
               RATEPLAN_ID||';'||
               '"'||REPLACE(RATEPLAN_NAME,'"','""')||'";'||
               AGENT_PERCENT||';'||
               '"'||REPLACE(POINT_SRC,'"','""')||'";'||
               '"'||REPLACE(POINT_DST,'"','""')||'";'||
               '"'||REPLACE(SPEED_STR,'"','""')||'";'||
               SPEED_VALUE
            AS TXT
          FROM D
         WHERE D.RN = 1
    ) LOOP
        UTL_FILE.put_line( v_output, CONVERT(ord.txt,'CL8MSWIN1251') ) ;
        v_count := v_count + 1;
        
        IF MOD(v_count,10000) = 0 THEN
            Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------------------
-- ACCOUNTS
-- --------------------------------------------------------------------------------
PROCEDURE Brm_accounts_to_file
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Brm_accounts_to_file';
    v_output      UTL_FILE.file_type;
    v_dir         VARCHAR2(100)      := �_BCR_DIR;
    v_file_name   VARCHAR2(100 CHAR) := 'accounts.csv';
    v_file_tmp    VARCHAR2(100 CHAR) := 'accounts.tmp';
    v_count       INTEGER;
    v_period_to   DATE;
    v_hdr         VARCHAR2(2000);
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    v_period_to := LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE),-1));
    -- ------------------------------------------------------------------ --
    -- ���������� ���������� � ����
    -- ------------------------------------------------------------------ --
    v_count := 0;
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W', 32767 );

    -- ��������� ���������
    v_hdr := 'ACCOUNT_ID;ACCOUNT_TYPE;ACCOUNT_NO;'||
            'CONTRACT_ID;CONTRACT_NO;CUST_NAME;CLIENT_ID;DATE_FROM;DATE_TO;'||
            'MSEG_ID;MSEG_NAME;TYPE_ID;TYPE_NAME;BRAND_ID;BRAND;'||
            'CONTRACTOR_ID;CONTRACTOR;BANK_ID;BANK;'||
            'XTTK_ID;XTTK;XTTK_OBJ_ID0;'||
            'AGENT_ID;AGENT;AGENT_OBJ_ID0;'||
            'CURRENCY_ID;VAT;'||
            'ERP_CODE;INN;KPP;'||
            'DLV_COUNTRY;DLV_ZIP;DLV_STATE;DLV_CITY;DLV_ADDRESS;'||
            'DLV_PERSON;DLV_PHONES;DLV_EMAIL;DLV_NOTES;'||
            'BILLING_ID;SALES_MANAGER;'||
            'DLV_METHOD';
    
    UTL_FILE.put_line(v_output, CONVERT(v_hdr,'CL8MSWIN1251'));

    -- ��������� ������
    FOR account IN (
        WITH 
          AP_MAX AS ( -- ��������� ������� �/� ������������������ � BRM  
              SELECT ACCOUNT_ID, PROFILE_ID 
                FROM (
                  SELECT ACCOUNT_ID, PROFILE_ID,
                         ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY DATE_FROM) RN
                    FROM ACCOUNT_PROFILE_T
                 )
                WHERE RN = 1 
          ),          
          D_SEG AS (
              SELECT KEY_ID MSEG_ID, NAME MSEG_NAME FROM PIN.DICTIONARY_T D 
               WHERE D.PARENT_ID = 63
          ), 
          D_TYP AS (
              SELECT KEY_ID TYPE_ID, NAME TYPE_NAME FROM PIN.DICTIONARY_T D 
               WHERE D.PARENT_ID = 64
          ), 
          D_DLV AS (
              SELECT KEY_ID DLV_ID, NAME DLV_METHOD FROM PIN.DICTIONARY_T D 
               WHERE D.PARENT_ID = 65
          ),
          ACC AS (
              SELECT DISTINCT 
                     A.ACCOUNT_ID, 
                     A.ACCOUNT_TYPE, 
                     A.ACCOUNT_NO, 
                     C.CONTRACT_ID, 
                     C.CONTRACT_NO, 
                     DECODE(A.ACCOUNT_TYPE, 
                            'P', SU.LAST_NAME ||' '||SU.FIRST_NAME||' '||SU.MIDDLE_NAME, 
                            'J', CU.CUSTOMER, NULL) CUST_NAME,
                     C.CLIENT_ID, 
                     C.DATE_FROM, 
                     C.DATE_TO, 
                     D_SEG.MSEG_ID, 
                     D_SEG.MSEG_NAME, 
                     D_TYP.TYPE_ID, 
                     D_TYP.TYPE_NAME,
                     NULL, -- BR.BRAND_ID, 
                     NULL, -- BR.BRAND, 
                     CT.CONTRACTOR_ID, 
                     CT.CONTRACTOR, 
                     CB.BANK_ID, 
                     CB.NOTES BANK,
                     XTTK.CONTRACTOR_ID XTTK_ID, 
                     XTTK.CONTRACTOR XTTK, 
                     XTTK.EXTERNAL_ID XTTK_OBJ_ID0,
                     AG.CONTRACTOR_ID AGENT_ID, 
                     AG.CONTRACTOR AGENT, 
                     AG.EXTERNAL_ID AGENT_OBJ_ID0,
                     A.CURRENCY_ID, 
                     AP.VAT, 
                     CU.ERP_CODE, 
                     CU.INN, 
                     CU.KPP,
                     AD.COUNTRY DLV_COUNTRY, 
                     AD.ZIP DLV_ZIP, 
                     AD.STATE DLV_STATE, 
                     AD.CITY DLV_CITY, 
                     AD.ADDRESS DLV_ADDRESS, 
                     AD.PERSON DLV_PERSON, 
                     AD.PHONES DLV_PHONES, 
                     AD.EMAIL DLV_EMAIL, 
                     AD.NOTES DLV_NOTES, 
                     A.BILLING_ID,
                     PK402_BCR_DATA.Get_sales_curator (
                                       XTTK.CONTRACTOR_ID, 
                                       AG.CONTRACTOR_ID, 
                                       C.CONTRACT_ID, 
                                       A.ACCOUNT_ID, 
                                       NULL,
                                       v_period_to
                                   ) SALES_MANAGER, 
                     D_DLV.DLV_METHOD
                FROM ACCOUNT_T A, 
                     ACCOUNT_PROFILE_T AP, 
                     --BRAND_T BR,  -- �� ��������������
                     CONTRACT_T C, 
                     CUSTOMER_T CU, 
                     SUBSCRIBER_T SU, 
                     D_SEG, 
                     D_TYP, 
                     CONTRACTOR_T CT, 
                     CONTRACTOR_T XTTK, 
                     CONTRACTOR_T AG, 
                     ACCOUNT_CONTACT_T AD, 
                     CONTRACTOR_BANK_T CB,
                     ACCOUNT_DOCUMENTS_T AL, 
                     D_DLV, 
                     AP_MAX        
               WHERE A.ACCOUNT_ID        = AP_MAX.ACCOUNT_ID   
                 AND AP.PROFILE_ID       = AP_MAX.PROFILE_ID
                 --AND AP.BRAND_ID       = BR.BRAND_ID(+)
                 AND AP.CONTRACT_ID      = C.CONTRACT_ID
                 AND AP.CUSTOMER_ID      = CU.CUSTOMER_ID(+)
                 AND AP.SUBSCRIBER_ID    = SU.SUBSCRIBER_ID(+)
                 AND C.MARKET_SEGMENT_ID = D_SEG.MSEG_ID(+)
                 AND C.CLIENT_TYPE_ID    = D_TYP.TYPE_ID(+)
                 AND AP.CONTRACTOR_ID    = CT.CONTRACTOR_ID(+)
                 AND DECODE(AP.BRANCH_ID,200,11,AP.BRANCH_ID) = XTTK.CONTRACTOR_ID(+)
                 AND DECODE(AP.BRANCH_ID,200,AP.BRANCH_ID,NVL(AP.AGENT_ID,AP.BRANCH_ID)) = AG.CONTRACTOR_ID(+)
                 AND A.ACCOUNT_ID        = AD.ACCOUNT_ID(+) 
                 AND AD.ADDRESS_TYPE(+)  = Pk00_Const.c_ADDR_TYPE_DLV
                 AND A.BILLING_ID IN (2000,2001,2002,2003,2006) 
                 AND AP.CONTRACTOR_BANK_ID = CB.BANK_ID(+)
                 AND A.ACCOUNT_ID = AL.ACCOUNT_ID(+) 
                 AND AL.DOC_BILL(+) = 'Y'
                 AND AL.DELIVERY_METHOD_ID = D_DLV.DLV_ID(+)
          )
          SELECT 
                ACCOUNT_ID||';'|| 
                '"'||REPLACE(ACCOUNT_TYPE,'"','""')||'";'||
                '"'||REPLACE(ACCOUNT_NO,'"','""')||'";'||
                CONTRACT_ID||';'||
                '"'||REPLACE(CONTRACT_NO,'"','""')||'";'||
                '"'||REPLACE(CUST_NAME,'"','""')||'";'||
                CLIENT_ID||';'||
                TO_CHAR(DATE_FROM,'yyyy.mm.dd')||';'||
                TO_CHAR(DATE_TO,'yyyy.mm.dd')||';'||
                MSEG_ID||';'||
                '"'||REPLACE(MSEG_NAME,'"','""')||'";'||
                TYPE_ID||';'||
                '"'||REPLACE(TYPE_NAME,'"','""')||'";'||
                ';'||                -- NBR.BRAND_ID
                ';'||                -- BR.BRAND
                CONTRACTOR_ID||';'||
                '"'||REPLACE(CONTRACTOR,'"','""')||'";'||
                BANK_ID||';'||
                '"'||REPLACE(BANK,'"','""')||'";'||
                XTTK_ID||';'||
                '"'||REPLACE(XTTK,'"','""')||'";'||
                XTTK_OBJ_ID0||';'||
                AGENT_ID||';'||
                '"'||REPLACE(AGENT,'"','""')||'";'||
                AGENT_OBJ_ID0||';'||
                CURRENCY_ID||';'||
                VAT||';'||
                '"'||REPLACE(ERP_CODE,'"','""')||'";'||
                '"'||REPLACE(INN,'"','""')||'";'||
                '"'||REPLACE(KPP,'"','""')||'";'||
                '"'||REPLACE(DLV_COUNTRY,'"','""')||'";'||
                '"'||REPLACE(DLV_ZIP,'"','""')||'";'||
                '"'||REPLACE(DLV_STATE,'"','""')||'";'||
                '"'||REPLACE(DLV_CITY,'"','""')||'";'||
                '"'||REPLACE(DLV_ADDRESS,'"','""')||'";'||
                '"'||REPLACE(DLV_PERSON,'"','""')||'";'||
                '"'||REPLACE(DLV_PHONES,'"','""')||'";'||
                '"'||REPLACE(DLV_EMAIL,'"','""')||'";'||
                '"'||REPLACE(DLV_NOTES,'"','""')||'";'||
                BILLING_ID||';'||
                '"'||REPLACE(SALES_MANAGER,'"','""')||'";'||
                '"'||REPLACE(DLV_METHOD,'"','""')||'"'
              AS TXT
            FROM ACC
    ) LOOP
        UTL_FILE.put_line( v_output,  CONVERT(account.txt,'CL8MSWIN1251')) ;
        v_count := v_count + 1;
        
        IF MOD(v_count,10000) = 0 THEN
            Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------------------
-- CLIENTS
-- --------------------------------------------------------------------------------
PROCEDURE Brm_clients_to_file
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Brm_clients_to_file';
    v_output      UTL_FILE.file_type;
    v_dir         VARCHAR2(100)      := �_BCR_DIR;
    v_file_name   VARCHAR2(100 CHAR) := 'clients.csv';
    v_file_tmp    VARCHAR2(100 CHAR) := 'clients.tmp';
    v_count       INTEGER;
    v_hdr         VARCHAR2(2000);
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ------------------------------------------------------------------ --
    -- ���������� ���������� � ����
    -- ------------------------------------------------------------------ --
    v_count := 0;
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W' );

    -- ��������� ���������
    v_hdr := 'CLIENT_ID;CLIENT_NAME';
    UTL_FILE.put_line(v_output, CONVERT(v_hdr,'CL8MSWIN1251'));

    -- ��������� ������
    FOR client IN (
        SELECT CLIENT_ID||';"'||REPLACE(REPLACE(CLIENT_NAME,CHR(10),' '),'"','""')||'"' AS TXT FROM CLIENT_T
    ) LOOP
        UTL_FILE.put_line( v_output, CONVERT(client.txt,'CL8MSWIN1251'));
        v_count := v_count + 1;
        IF MOD(v_count,10000) = 0 THEN
            Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------------------
-- PHONES
-- --------------------------------------------------------------------------------
PROCEDURE Brm_phones_to_file
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Brm_phones_to_file';
    v_output      UTL_FILE.file_type;
    v_dir         VARCHAR2(100)      := �_BCR_DIR;
    v_file_name   VARCHAR2(100 CHAR) := 'phones.csv';
    v_file_tmp    VARCHAR2(100 CHAR) := 'phones.tmp';
    v_count       INTEGER;
    v_hdr         VARCHAR2(2000);
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ------------------------------------------------------------------ --
    -- ���������� ���������� � ����
    -- ------------------------------------------------------------------ --
    v_count := 0;
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W' );

    -- ��������� ���������
    v_hdr := 'ORDER_ID;ORDER_NO;PHONE_NUMBER;DATE_FROM;DATE_TO';
    
    UTL_FILE.put_line(v_output, CONVERT(v_hdr,'CL8MSWIN1251'));
    
    -- ��������� ������
    FOR phone IN (
        SELECT O.ORDER_ID||';'|| 
               '"'||REPLACE(O.ORDER_NO,'"','""')||'";'||
               '"'||REPLACE(OP.PHONE_NUMBER,'"','""')||'";'||
               TO_CHAR(OP.DATE_FROM,'yyyy.mm.dd')||';'||
               TO_CHAR(OP.DATE_TO,'yyyy.mm.dd') 
            AS TXT
          FROM ORDER_PHONES_T OP, 
               ORDER_T O, 
               ACCOUNT_T A
         WHERE OP.ORDER_ID = O.ORDER_ID
           AND O.ACCOUNT_ID = A.ACCOUNT_ID         
           AND A.BILLING_ID IN (2000,2001,2002,2003,2006) 
    ) LOOP
        UTL_FILE.put_line( v_output, CONVERT(phone.txt,'CL8MSWIN1251')) ;
        v_count := v_count + 1;
        
        IF MOD(v_count,10000) = 0 THEN
            Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


-- --------------------------------------------------------------------------------
-- DELIVERY
-- --------------------------------------------------------------------------------
PROCEDURE Brm_delivery_to_file
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Brm_delivery_to_file';
    v_output      UTL_FILE.file_type;
    v_dir         VARCHAR2(100)      := �_BCR_DIR;
    v_file_name   VARCHAR2(100 CHAR) := 'delivery.csv';
    v_file_tmp    VARCHAR2(100 CHAR) := 'delivery.tmp';
    v_count       INTEGER;
    v_hdr         VARCHAR2(2000);
BEGIN    
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- ------------------------------------------------------------------ --
    -- ���������� ���������� � ����
    -- ------------------------------------------------------------------ --
    v_count := 0;
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W' );

    -- ��������� ���������
    v_hdr := 'ACCOUNT_ID;DLV_ID;DLV_NAME;DLV_OBJ_ID0';

    UTL_FILE.put_line(v_output, CONVERT(v_hdr,'CL8MSWIN1251'));
 
    -- ��������� ������
    FOR dlv IN (
        SELECT A.ACCOUNT_ID||';'|| 
               D.KEY_ID||';'|| -- DLV_ID, 
               '"'||REPLACE(D.NAME,'"','""')||'";'|| -- DLV_NAME, 
               D.EXTERNAL_ID   -- DLV_OBJ_ID0 
            AS TXT
          FROM ACCOUNT_DOCUMENTS_T a, DICTIONARY_T d
        WHERE D.PARENT_ID = 65
          AND A.DELIVERY_METHOD_ID = D.KEY_ID
    ) LOOP
        UTL_FILE.put_line( v_output, CONVERT(dlv.txt,'CL8MSWIN1251') ) ;
        v_count := v_count + 1;
        
        IF MOD(v_count,10000) = 0 THEN
            Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        END IF;
        
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('file '||v_file_name||' - '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;


END PK402_BCR_FILE;
/
