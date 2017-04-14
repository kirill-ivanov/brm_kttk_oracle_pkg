CREATE OR REPLACE PACKAGE PK37_BILLING_EXPORT_TO_1C
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK37_BILLING_EXPORT_TO_1C';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;

    TYPE t_refc IS REF CURSOR;
    --=============================================================================--
    -- Экспорт данных в 1C
    --=============================================================================--
    PROCEDURE Report( 
               p_recordset  OUT t_refc,
               p_period_id   IN INTEGER
           );
    
END PK37_BILLING_EXPORT_TO_1C;
/
CREATE OR REPLACE PACKAGE BODY PK37_BILLING_EXPORT_TO_1C
IS


FUNCTION Create_header(
                     p_region_id IN INTEGER,
                     p_period_id IN INTEGER
                  ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Create_header';
    v_session_id    VARCHAR2(100):= 'INFR' ||TO_CHAR(SYSDATE,'DDMMYY');
    v_journal_id    VARCHAR2(100):= TO_CHAR(SYSDATE,'HH24MMSS');
    v_header_id     INTEGER;
    v_version       INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, region_id = '||p_region_id||
                                ' period_id = '||p_period_id, 
                                c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    /*
    -- поолучаем номер версии
    SELECT NVL(MAX(VERSION),0)+1 INTO v_version
             FROM INV_EXPORT_1C_HEADER_T
      WHERE REGION_ID = p_region_id
        AND PERIOD    = p_period_id;
    -- записываем заголовок
    INSERT INTO INV_EXPORT_1C_HEADER_T (
        HEADER_ID,
        PERIOD,
        REGION_ID,
        VERSION,
        JOURNAL_ID,
        SESSION_ID,
        STATUS
    )VALUES(
        SQ_INV_HEADER.NEXTVAL, 
        p_period,
        v_region,
        v_version,
        v_session_id,
        v_journal_id,
        NULL
    ) RETURNING HEADER_ID INTO v_header_id;
    */
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    RETURN v_header_id;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE Write_lines(
                     p_region_id IN INTEGER,
                     p_period_id IN INTEGER
                  ) 
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Create_header';
    v_session_id    VARCHAR2(100):= 'INFR' ||TO_CHAR(SYSDATE,'DDMMYY');
    v_journal_id    VARCHAR2(100):= TO_CHAR(SYSDATE,'HH24MMSS');
    v_header_id     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, region_id = '||p_region_id||
                                ' period_id = '||p_period_id, 
                                c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    /*
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- поолучаем номер версии
    SELECT NVL(MAX(VERSION),0)+1 INTO v_version
             FROM INV_EXPORT_1C_HEADER_T
      WHERE REGION_ID = p_region
        AND PERIOD    = p_period;
    -- записываем заголовок
    INSERT INTO INV_EXPORT_1C_HEADER (
        HEADER_ID,
        PERIOD,
        REGION_ID,
        VERSION,
        JOURNAL_ID,
        SESSION_ID,
        STATUS
    )VALUES(
        SQ_INV_HEADER.NEXTVAL, 
        p_period,
        p_region,
        v_version,
        v_session_id,
        v_journal_id,
        NULL
    ) RETURNING ID INTO v_header_id;
    */
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Запрос для выгрузки данных в 1С
-- ------------------------------------------------------------------------ --
PROCEDURE Report( 
               p_recordset  OUT t_refc,
               p_period_id   IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Subservice_list';
    v_retcode    INTEGER := c_RET_OK;
    v_date_from  DATE;
    v_date_to    DATE;
BEGIN
    v_date_from := Pk04_Period.Period_from(p_period_id);
    v_date_to   := Pk04_Period.Period_to(p_period_id);    
    --
    OPEN p_recordset FOR
    WITH INV AS (
        -- это базовые записи
        SELECT A.ACCOUNT_ID, A.ACCOUNT_NO,
               B.REP_PERIOD_ID, B.BILL_ID, B.BILL_NO, B.BILL_DATE, 
               B.BILL_TYPE, B.CURRENCY_ID,
               II.INV_ITEM_ID, II.TOTAL, II.GROSS, II.TAX, II.SERVICE_ID,
               II.DATE_FROM, II.DATE_TO,
               -- контрольные поля
               B.TOTAL BILL_TOTAL, SUM(II.TOTAL) OVER (PARTITION BY B.BILL_ID) II_SUM_TOTAL
           FROM BILL_T B, ACCOUNT_T A, INVOICE_ITEM_T II
         WHERE B.REP_PERIOD_ID = p_period_id
           AND B.ACCOUNT_ID = A.ACCOUNT_ID 
           AND A.BILLING_ID IN(Pk00_Const.c_BILLING_KTTK, Pk00_Const.c_BILLING_OLD ) -- 2001, 2002
           AND II.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND II.BILL_ID   = B.BILL_ID
    ), AP AS (
        -- исключаем возможные задвоения
        SELECT ACCOUNT_ID,
               CONTRACT_ID,
               CONTRACTOR_ID,
               CUSTOMER_ID,
               BRANCH_ID,
               VAT
         FROM (
            SELECT ACCOUNT_ID,
                   CONTRACT_ID,
                   CONTRACTOR_ID,
                   CUSTOMER_ID,
                   BRANCH_ID,
                   VAT,
                   ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY DATE_FROM DESC) RN 
              FROM ACCOUNT_PROFILE_T 
              WHERE DATE_FROM < v_date_to 
                AND (DATE_TO IS NULL OR DATE_TO >= v_date_from)
        )
        WHERE RN = 1
    ), SALES AS (
        SELECT CONTRACT_ID, SALES_NAME 
          FROM (
          SELECT SC.CONTRACT_ID, 
                 M.LAST_NAME||' '||SUBSTR(M.FIRST_NAME,1,1)||'.'||SUBSTR(M.MIDDLE_NAME,1,1)||'.' SALES_NAME, 
                 ROW_NUMBER() OVER (PARTITION BY SC.CONTRACT_ID ORDER BY M.DATE_FROM DESC) RN 
            FROM SALE_CURATOR_T SC, MANAGER_T M
           WHERE SC.MANAGER_ID = M.MANAGER_ID
             AND SC.CONTRACT_ID IS NOT NULL
             AND SC.DATE_FROM < v_date_to 
             AND (SC.DATE_TO IS NULL OR SC.DATE_TO >= v_date_from)
        )WHERE RN = 1
    ), ADR AS (
        SELECT ACCOUNT_ID, CUSTADRESS
          FROM (
          SELECT ACCOUNT_ID, (COUNTRY|| ', ' ||CITY|| ', ' ||ADDRESS) CUSTADRESS,
                 ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY DATE_FROM DESC) RN   
            FROM ACCOUNT_CONTACT_T 
           WHERE ADDRESS_TYPE = 'JUR'
             AND DATE_FROM < v_date_to 
             AND (DATE_TO IS NULL OR DATE_TO >= v_date_from)
        )WHERE RN = 1
    )
    SELECT -- ------------------------------------------------------------------ --
           INV.GROSS         NET_AMOUNT, 
           INV.GROSS         GROSS_RUR, 
           INV.TAX           TAX_AMOUNT,
           S.ERP_PRODCODE    BILLINGGLCODE, 
           TO_CHAR(INV.BILL_DATE,'yyyy.mm') EXECUTIONPERIOD, 
           INV.BILL_NO       FACTUREEXTERNALID, 
           NVL(CS.ERP_CODE,'-') PARTNERID, 
           '-'               RCONTRACTEXTERNALID, 
           DECODE(INV.BILL_TYPE, 'C', '1', '0' ) INVOICESTORNO, 
           NVL(CS.CUSTOMER,'-')    CUSTNAME, 
           NVL(ADR.CUSTADRESS,'-') CUSTADRESS, 
           NVL(CS.INN,'-')   INN, 
           NVL(CS.KPP,'-')   KPP, 
           TO_CHAR(INV.CURRENCY_ID)   CURRENCYCODE, 
           DECODE           (C.CLIENT_TYPE_ID ,6403, '62.25.11', DECODE (INV.CURRENCY_ID,810, '62.23.11',36, '62.23.11',124, '62.23.11','62.23.12')) BAL_GR, 
           TO_CHAR(AP.VAT)   TAX_GR, 
           INV.INV_ITEM_ID   EXTERNALLINEID, 
           INV.BILL_DATE     BILL_END, 
           C.CONTRACT_NO     AUTO_NO, 
           C.DATE_FROM       CUST_DATE,
           NVL(SALES.SALES_NAME,'-') SALES_NAME,
           NVL(CL.CLIENT_NAME,'-')   CLIENT_SH, 
           INV.BILL_TYPE     TYPE, 
           INV.ACCOUNT_NO    ACCOUNT_NO, 
           ROW_NUMBER() OVER (ORDER BY C.CONTRACT_NO, INV.INV_ITEM_ID)  LINEID, 
           INV.TOTAL         DUE_RUR, 
           S.SERVICE         STRINGNAME,
           BR.CONTRACTOR     REGION, 
           0                 CURRENCY_RATE
           -- ------------------------------------------------------------------ --
      FROM INV, AP, SALES, ADR, 
           CONTRACT_T C, CUSTOMER_T CS, CONTRACTOR_T BR, SERVICE_T S, CLIENT_T CL 
     WHERE AP.ACCOUNT_ID  = INV.ACCOUNT_ID 
       AND AP.ACCOUNT_ID  = ADR.ACCOUNT_ID(+)
       AND AP.CONTRACT_ID = SALES.CONTRACT_ID(+)
       AND AP.CONTRACT_ID = C.CONTRACT_ID
       AND AP.CUSTOMER_ID = CS.CUSTOMER_ID(+)
       AND AP.BRANCH_ID   = BR.CONTRACTOR_ID(+)
       AND INV.SERVICE_ID = S.SERVICE_ID 
       AND C.CLIENT_ID    = CL.CLIENT_ID(+)
    ;
    --
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);  
END;



END PK37_BILLING_EXPORT_TO_1C;
/
