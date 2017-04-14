CREATE OR REPLACE PACKAGE PK10_PAYMENT_EISUP_CTRL
IS
    --
    -- Пакет для поддержки импорта из ЕИСУП
    -- Контроль загрузки платежей
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_PAYMENT_EISUP_CTRL';
    -- ==============================================================================
   
    type t_refc is ref cursor;

    -- ------------------------------------------------------------------------ --
    -- Статистика по загруженным входным балансам
    -- p_flag: 
    --      - NULL - показать все
    --      - 1    - показать л/с где загружены балансы
    --      - 2    - показать л/с где НЕ загружены балансы
    -- ------------------------------------------------------------------------ --
    PROCEDURE Ctrl_in_balance (
                   p_recordset OUT t_refc, 
                   p_flag       IN INTEGER DEFAULT NULL 
               );

    -- ------------------------------------------------------------------------ --
    -- распределение платежей по биллингам
    -- ------------------------------------------------------------------------ --
    PROCEDURE Journal_lst (
                   p_recordset    OUT t_refc
               );

    -- ------------------------------------------------------------------------ --
    -- распределение платежей по биллингам
    -- ------------------------------------------------------------------------ --
    PROCEDURE Payments_billing (
                   p_recordset    OUT t_refc, 
                   p_journal_id   IN INTEGER
               );

    -- ------------------------------------------------------------------------ --
    -- платежи для которых найдены ACCOUNT_NO + CONTRACT_NO, но неверный ERP_CODE
    -- ------------------------------------------------------------------------ --
    PROCEDURE Payments_erp_code_error (
                   p_recordset    OUT t_refc, 
                   p_journal_id   IN INTEGER
               );
    
    -- ------------------------------------------------------------------------ --
    -- платежи для которых найдены ACCOUNT_NO, но неверный CONTRACT_NO
    -- ------------------------------------------------------------------------ --
    PROCEDURE Payments_contract_error (
                   p_recordset    OUT t_refc, 
                   p_journal_id   IN INTEGER
               );

    -- ------------------------------------------------------------------------ --
    -- платежи для которых найдены ACCOUNT_NO и ERP_CODE, но неверный CONTRACT_NO
    -- ------------------------------------------------------------------------ --
    PROCEDURE Payments_account_error (
                   p_recordset    OUT t_refc, 
                   p_journal_id   IN INTEGER
               );
                   
    -- ------------------------------------------------------------------------ --
    -- Статистика по приязке платежей журнала
    -- ------------------------------------------------------------------------ --
    PROCEDURE Payments_bind_stat (
                   p_recordset    OUT t_refc, 
                   p_journal_id   IN INTEGER
               );

END PK10_PAYMENT_EISUP_CTRL;
/
CREATE OR REPLACE PACKAGE BODY PK10_PAYMENT_EISUP_CTRL
IS

-- ------------------------------------------------------------------------ --
-- Статистика по загруженным входным балансам
-- p_flag: 
--      - NULL - показать все
--      - 1    - показать л/с где загружены балансы
--      - 2    - показать л/с где НЕ загружены балансы
-- ------------------------------------------------------------------------ --
PROCEDURE Ctrl_in_balance (
               p_recordset OUT t_refc, 
               p_flag       IN INTEGER DEFAULT NULL 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Ctrl_in_balance';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
        SELECT BILLING_ID, ACCOUNT_ID,   
               IN_BALANCE, IN_BALANCE_DATE, 
               BALANCE ACC_BALANCE, BALANCE_DATE,
               ACCOUNT_NO, CONTRACT_NO, ERP_CODE, INN, KPP, CUSTOMER,
               MAX_PERIOD_ID, MAX_BILL_DATE, TOTAL_FROM_201501
          FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY A.ACCOUNT_ID 
                                      ORDER BY B.BILL_DATE DESC, AP.DATE_FROM DESC) RN,  
                   A.BILLING_ID, A.ACCOUNT_ID, A.BALANCE, A.BALANCE_DATE, 
                   IB.BALANCE IN_BALANCE, IB.BALANCE_DATE IN_BALANCE_DATE,
                   A.ACCOUNT_NO, C.CONTRACT_NO, CS.ERP_CODE, CS.INN, CS.KPP, CS.CUSTOMER, 
                   MAX(B.REP_PERIOD_ID) OVER (PARTITION BY A.ACCOUNT_ID) MAX_PERIOD_ID, 
                   MAX(B.BILL_DATE)     OVER (PARTITION BY A.ACCOUNT_ID) MAX_BILL_DATE,
                   SUM(B.TOTAL)         OVER (PARTITION BY A.ACCOUNT_ID) TOTAL_FROM_201501
              FROM ACCOUNT_T A, BILL_T B, INCOMING_BALANCE_T IB,
                   ACCOUNT_PROFILE_T AP, CUSTOMER_T CS, CONTRACT_T C
             WHERE A.BILLING_ID IN ( 2001, 2002 )
               AND A.STATUS         = 'B'
               AND A.ACCOUNT_ID     = B.ACCOUNT_ID
               AND B.REP_PERIOD_ID >= 201501 -- считаем, что год достаточно
               AND A.ACCOUNT_ID     = IB.ACCOUNT_ID(+)
               AND A.ACCOUNT_ID     = AP.ACCOUNT_ID
               AND AP.CONTRACT_ID   = C.CONTRACT_ID
               AND AP.CUSTOMER_ID   = CS.CUSTOMER_ID
         ) BL
         WHERE RN = 1
           AND 
             CASE
               WHEN p_flag IS NULL THEN 1
               WHEN p_flag = 1 AND IN_BALANCE IS NOT NULL THEN 1
               WHEN p_flag = 2 AND IN_BALANCE IS NULL THEN 1
               ELSE 0
             END = 1
           AND EXISTS ( -- интересны, только открытые л/с
              SELECT * FROM ORDER_T O
               WHERE BL.ACCOUNT_ID = O.ACCOUNT_ID
                 AND O.DATE_FROM < SYSDATE
                 AND ( O.DATE_TO IS NULL OR SYSDATE < O.DATE_TO ) 
           )
         ORDER BY MAX_PERIOD_ID, IN_BALANCE
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- распределение платежей по биллингам
-- ------------------------------------------------------------------------ --
PROCEDURE Journal_lst (
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Journal_lst';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
        SELECT JOURNAL_ID, DATE_FROM, DATE_TO, CREATE_DATE, LOAD_DATE, STATUS, PAYMENTS, TRANSFERS, LOADING_RESULT 
          FROM EISUP_JOURNAL_T EP
         WHERE STATUS = 'OK'
        ORDER BY JOURNAL_ID DESC
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- распределение платежей по биллингам
-- ------------------------------------------------------------------------ --
PROCEDURE Payments_billing (
               p_recordset    OUT t_refc, 
               p_journal_id   IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Payments_billing';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
        SELECT A.BILLING_ID, D.NAME, COUNT(*) NUM
          FROM EISUP_PAYMENT_T EP, ACCOUNT_T A, DICTIONARY_T D
         WHERE EP.JOURNAL_ID = 28
           AND EP.BRM_ACCOUNT_ID IS NOT NULL
           AND EP.BRM_ACCOUNT_ID = A.ACCOUNT_ID
           AND A.BILLING_ID = D.KEY_ID
           AND D.PARENT_ID  = 20
        GROUP BY A.BILLING_ID, D.NAME
        ORDER BY A.BILLING_ID
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- платежи для которых найдены ACCOUNT_NO + CONTRACT_NO, но неверный ERP_CODE
-- ------------------------------------------------------------------------ --
PROCEDURE Payments_erp_code_error (
               p_recordset    OUT t_refc, 
               p_journal_id   IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Ppayments_erp_code_error';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
        -- платежи для которых найдены ACCOUNT_NO + CONTRACT_NO, но неверный ERP_CODE
        SELECT DISTINCT
               EP.ACCOUNT_NO, EP.ERP_CODE, CS.ERP_CODE BRM_ERP_CODE, 
               EP.CONTRACT_NO, C.CONTRACT_NO BRM_CONTRACT_NO, 
               CS.CUSTOMER BRM_CUSTOMER, CS.CUSTOMER_ID BRM_CUSTONER_ID 
          FROM EISUP_PAYMENT_T EP, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CUSTOMER_T CS, CONTRACT_T C
         WHERE EP.JOURNAL_ID  = p_journal_id
           AND EP.BRM_ACCOUNT_ID IS NULL
           AND EP.ACCOUNT_NO  = A.ACCOUNT_NO
           AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
           AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- платежи для которых найдены ACCOUNT_NO и ERP_CODE, но неверный CONTRACT_NO
-- ------------------------------------------------------------------------ --
PROCEDURE Payments_contract_error (
               p_recordset    OUT t_refc, 
               p_journal_id   IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Payments_contract_error';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
        -- платежи для которых найдены ACCOUNT_NO и ERP_CODE, но неверный CONTRACT_NO
        SELECT DISTINCT EP.ERP_CODE, EP.ACCOUNT_NO, EP.CONTRACT_NO, EP.BRM_CONTRACT_NO, CM.COMPANY_NAME--, CS.CUSTOMER 
          FROM EISUP_PAYMENT_T EP, ACCOUNT_PROFILE_T AP, CUSTOMER_T CS, CONTRACT_T C, COMPANY_T CM
         WHERE EP.JOURNAL_ID = p_journal_id
           AND EP.BRM_ACCOUNT_ID  IS NOT NULL
           AND EP.BRM_CUSTOMER_ID IS NOT NULL
           AND EP.BRM_CONTRACT_ID IS NULL
           AND EP.ERP_CODE    = CS.ERP_CODE
           AND AP.ACCOUNT_ID  = EP.BRM_ACCOUNT_ID
           AND AP.CUSTOMER_ID = EP.BRM_CUSTOMER_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID 
           AND CM.CONTRACT_ID = C.CONTRACT_ID
         ORDER BY CONTRACT_NO
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- ------------------------------------------------------------------------ --
-- платежи для которых найдены CONTRACT_NO, но не найден ACCOUNT_NO
-- ------------------------------------------------------------------------ --
PROCEDURE Payments_account_error (
               p_recordset    OUT t_refc, 
               p_journal_id   IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Payments_account_error';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
          WITH BEP AS (
              SELECT ROW_NUMBER() OVER (PARTITION BY EP.ROWID ORDER BY A.ACCOUNT_NO, AP.DATE_FROM ) RN,
                     C.CONTRACT_NO, C.CONTRACT_ID,
                     EP.ACCOUNT_NO EISUP_ACCOUNT_NO, 
                     A.ACCOUNT_NO BRM_ACCOUNT_NO, 
                     A.ACCOUNT_ID BRM_ACCOUNT_ID, 
                     AP.DATE_FROM, AP.DATE_TO,
                     EP.ERP_CODE,
                     EP.DOCUMENT_ID, EP.PAYMENT_AMOUNT, EP.PAYMENT_DATE
                FROM CONTRACT_T C, ACCOUNT_PROFILE_T AP, ACCOUNT_T A, EISUP_PAYMENT_T EP
               WHERE EP.JOURNAL_ID  = p_journal_id
                 AND TRIM(C.CONTRACT_NO) = TRIM(EP.CONTRACT_NO)
                 AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
                 AND AP.CONTRACT_ID = C.CONTRACT_ID
                 AND AP.DATE_FROM  <= EP.PAYMENT_DATE
                 AND (AP.DATE_TO IS NULL OR EP.PAYMENT_DATE <= AP.DATE_TO )
                 AND NOT EXISTS (
                     SELECT * 
                       FROM ACCOUNT_T A
                      WHERE A.ACCOUNT_NO = EP.ACCOUNT_NO
                 )
          )
          SELECT * FROM BEP
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- Статистика по приязке платежей журнала
-- ------------------------------------------------------------------------ --
PROCEDURE Payments_bind_stat (
               p_recordset    OUT t_refc, 
               p_journal_id   IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Payments_bind_stat';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
        SELECT PERIOD, BRM_LOAD_CODE, 
               DECODE(BRM_ACCOUNT_ID,  NULL, 0, 1)  BRM_ACCOUNT_ID,
               DECODE(BRM_CUSTOMER_ID, NULL, 0, 1) BRM_CUSTOMER_ID,
               DECODE(BRM_CONTRACT_ID, NULL, 0, 1) BRM_CONTRACT_ID, 
               COUNT(*) FROM EISUP_PAYMENT_T EP
         WHERE EP.JOURNAL_ID = p_journal_id
           --AND BRM_LOAD_CODE IS NOT NULL
         GROUP BY PERIOD, BRM_LOAD_CODE, 
                  DECODE(BRM_ACCOUNT_ID, NULL, 0, 1), 
                  DECODE(BRM_CONTRACT_ID, NULL, 0, 1),
                  DECODE(BRM_CUSTOMER_ID, NULL, 0, 1)
        ORDER BY PERIOD, BRM_LOAD_CODE
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;



END PK10_PAYMENT_EISUP_CTRL;
/
