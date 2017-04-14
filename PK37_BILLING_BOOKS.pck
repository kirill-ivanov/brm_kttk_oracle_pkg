CREATE OR REPLACE PACKAGE PK37_BILLING_BOOKS
IS
    --
    -- Пакет для работы с объектом "СОБЫТИЕ", таблицы:
    -- event_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK37_BOOK';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- 
    -- Создать заголовок сессии выгрузки в ЕИСУП
    -- Возвращает номер сессии
    -- 'Статус сессии: 
    --    1 - выгружено из биллинга,  
    --    2 – Отменен биллингом, 
    --    3 – Есть в данных (выставляется из ЕИСУП), 
    --    5 – Загружено в ЕИСУП без ошибок';
    FUNCTION New_session (
       p_org_id    IN VARCHAR2,
       p_period_id IN INTEGER
    ) RETURN INTEGER;
    
    -- ------------------------------------------------------------------------- --
    -- книга Продаж (9 раздел декларации), таблица TPI_R9_BOOK_SALES_T
    -- ------------------------------------------------------------------------- --
    PROCEDURE Fill_book_sales (
               p_session_id IN INTEGER
             );
    
    -- ------------------------------------------------------------------------- --
    -- книга Покупок (8 раздел декларации), таблица TPI_R8_PURCHASE_BOOK_T
    -- ------------------------------------------------------------------------- --
    PROCEDURE Fill_purchase_book (
               p_session_id IN INTEGER
             );
   
END PK37_BILLING_BOOKS;
/
CREATE OR REPLACE PACKAGE BODY PK37_BILLING_BOOKS
IS

-- ------------------------------------------------------------------------- -- 
-- Создать заголовок сессии выгрузки в ЕИСУП
-- Возвращает номер сессии
-- 'Статус сессии: 
--    1 - выгружено из биллинга,  
--    2 – Отменен биллингом, 
--    3 – Есть в данных (выставляется из ЕИСУП), 
--    5 – Загружено в ЕИСУП без ошибок';
-- ------------------------------------------------------------------------- --
FUNCTION New_session (
             p_org_id    IN VARCHAR2,
             p_period_id IN INTEGER
          ) RETURN INTEGER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'New_session';
    v_session_id   INTEGER;
    v_date_from    DATE;
    v_date_to      DATE;
    v_session_code VARCHAR2(10);
    v_billing_type VARCHAR2(10) := 'BRM';
    v_status       INTEGER := 0;
BEGIN
    v_date_from := Pk04_Period.Period_from(p_period_id);
    v_date_to   := Pk04_Period.Period_to(p_period_id);
    v_session_id := SQ_TPI_SESSION_ID.NEXTVAL;

    -- сохраняем описатель сессии выгрузки в ЕИСУП
    INSERT INTO TPI_BOOK_SESSION_T (
      SESSION_ID, SESSION_CODE, ERP_ORG_ID,
      DATE_FROM, DATE_TO, BILLING_TYPE,
      STATUS, PERIOD_ID
    )VALUES(
      v_session_id, v_session_code, p_org_id,
      v_date_from, v_date_to, v_billing_type,
      v_status, p_period_id
    );
    --
    RETURN v_session_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- ------------------------------------------------------------------------- --
-- книга Продаж (9 раздел декларации), таблица TPI_R9_BOOK_SALES_T
-- ------------------------------------------------------------------------- --
PROCEDURE Fill_book_sales (
           p_session_id IN INTEGER
         )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Fill_book_sales';
    v_count     INTEGER;
    v_period_id INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- читаем из заголовка сессии недостающие данные
    SELECT S.PERIOD_ID INTO v_period_id
      FROM TPI_BOOK_SESSION_T S
     WHERE S.SESSION_ID = p_session_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --     
    -- выгрузка регулярных счетов-фактур
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    INSERT INTO TPI_R9_BOOK_SALES_T (
      SESSION_ID, F1,F2,F3,F4,F5,F6,F7,F8,F12,F13a,F13b,F14,F15,F16,F17,F18,F19,
      REP_PERIOD_ID, BILL_ID, ADVANCE_ID, ACCOUNT_ID, PROFILE_ID, COMPANY_ID
    )
    SELECT p_session_id SESSION_ID,
           NULL F1,
           1 F2,
           B.BILL_NO||';'||TO_CHAR(B.BILL_DATE,'dd.mm.yyyy') F3,
           NULL F4,
           NULL F5,
           NULL F6,
           CM.COMPANY_NAME F7,
           CASE
             WHEN AP.KPP IS NULL THEN CM.INN
             ELSE CM.INN||'/'||AP.KPP  
           END F8,
           B.CURRENCY_ID F12,
           CASE
             WHEN B.CURRENCY_ID != 810 THEN B.TOTAL
             ELSE NULL 
           END F13a,
           CASE
             WHEN B.CURRENCY_ID = 810 THEN B.TOTAL
             ELSE NULL 
           END F13b,
           CASE
             WHEN AP.VAT = 18 THEN B.GROSS
             ELSE NULL
           END F14,
           CASE
             WHEN AP.VAT = 10 THEN B.GROSS
             ELSE NULL
           END F15,
           CASE
             WHEN AP.VAT = 0 THEN B.GROSS
             ELSE NULL
           END F16,
           CASE
             WHEN AP.VAT = 18 THEN B.TAX
             ELSE NULL
           END F17,
           CASE
             WHEN AP.VAT = 10 THEN B.TAX
             ELSE NULL
           END F18,
           CASE
             WHEN AP.VAT = 0 THEN B.TAX
             ELSE NULL
           END F19,
           B.REP_PERIOD_ID, 
           B.BILL_ID, 
           NULL ADVANCE_ID, 
           B.ACCOUNT_ID, 
           B.PROFILE_ID, 
           CM.COMPANY_ID
      FROM BILL_T B, COMPANY_T CM, ACCOUNT_PROFILE_T AP
     WHERE B.REP_PERIOD_ID = v_period_id
       AND B.BILL_TYPE NOT IN ('C','D', 'A', 'I')
       AND B.CONTRACT_ID = CM.CONTRACT_ID
       AND CM.ACTUAL     = 'Y' 
       AND B.PROFILE_ID  = AP.PROFILE_ID
       AND B.BILL_STATUS IN ('CLOSED', 'READY');
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('TPI_R9_BOOK_SALES.BILL_B= '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- F4. Исправления счета фактуры
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    INSERT INTO TPI_R9_BOOK_SALES_T (
      SESSION_ID, F1,F2,F3,F4,F5,F6,F7,F8,F12,F13a,F13b,F14,F15,F16,F17,F18,F19,
      REP_PERIOD_ID, BILL_ID, ADVANCE_ID, ACCOUNT_ID, PROFILE_ID, COMPANY_ID
    )
    SELECT p_session_id SESSION_ID,
           NULL F1,
           1 F2,
           NULL F3,
           SUBSTR(B.BILL_NO,1,LENGTH(B.BILL_NO)-1)||'испр №'
           ||DECODE(SUBSTR(B.BILL_NO,-1),'D',1,'F',2, 'H', 3, 'J', 4)
           ||';'||TO_CHAR(B.BILL_DATE,'dd.mm.yyyy') F4,
           NULL F5,
           NULL F6,
           CM.COMPANY_NAME F7,
           CASE
             WHEN AP.KPP IS NULL THEN CM.INN
             ELSE CM.INN||'/'||AP.KPP  
           END F8,
           B.CURRENCY_ID F12,
           CASE
             WHEN B.CURRENCY_ID != 810 THEN B.TOTAL
             ELSE NULL 
           END F13a,
           CASE
             WHEN B.CURRENCY_ID = 810 THEN B.TOTAL
             ELSE NULL 
           END F13b,
           CASE
             WHEN AP.VAT = 18 THEN B.GROSS
             ELSE NULL
           END F14,
           CASE
             WHEN AP.VAT = 10 THEN B.GROSS
             ELSE NULL
           END F15,
           CASE
             WHEN AP.VAT = 0 THEN B.GROSS
             ELSE NULL
           END F16,
           CASE
             WHEN AP.VAT = 18 THEN B.TAX
             ELSE NULL
           END F17,
           CASE
             WHEN AP.VAT = 10 THEN B.TAX
             ELSE NULL
           END F18,
           CASE
             WHEN AP.VAT = 0 THEN B.TAX
             ELSE NULL
           END F19,
           B.REP_PERIOD_ID, 
           B.BILL_ID, 
           NULL ADVANCE_ID, 
           B.ACCOUNT_ID, 
           B.PROFILE_ID, 
           CM.COMPANY_ID
      FROM BILL_T B, COMPANY_T CM, ACCOUNT_PROFILE_T AP
     WHERE B.REP_PERIOD_ID = v_period_id
       AND B.BILL_TYPE   = 'A'
       AND B.CONTRACT_ID = CM.CONTRACT_ID
       AND CM.ACTUAL     = 'Y' 
       AND B.PROFILE_ID  = AP.PROFILE_ID
       AND B.BILL_STATUS IN ('CLOSED', 'READY');
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('TPI_R9_BOOK_SALES.BILL_A= '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- F5. Корректировочная счет-фактура 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    INSERT INTO TPI_R9_BOOK_SALES_T (
      SESSION_ID, F1,F2,F3,F4,F5,F6,F7,F8,F12,F13a,F13b,F14,F15,F16,F17,F18,F19,
      REP_PERIOD_ID, BILL_ID, ADVANCE_ID, ACCOUNT_ID, PROFILE_ID, COMPANY_ID
    )
    SELECT p_session_id SESSION_ID,
           NULL F1,
           1 F2,
           NULL F3,
           NULL F4,
           B.BILL_NO||';'||TO_CHAR(B.BILL_DATE,'dd.mm.yyyy') F5,
           NULL F6,
           CM.COMPANY_NAME F7,
           CASE
             WHEN AP.KPP IS NULL THEN CM.INN
             ELSE CM.INN||'/'||AP.KPP  
           END F8,
           B.CURRENCY_ID F12,
           CASE
             WHEN B.CURRENCY_ID != 810 THEN B.TOTAL - BC.TOTAL
             ELSE NULL 
           END F13a,
           CASE
             WHEN B.CURRENCY_ID = 810 THEN B.TOTAL - BC.TOTAL
             ELSE NULL 
           END F13b,
           CASE
             WHEN AP.VAT = 18 THEN B.GROSS - BC.GROSS
             ELSE NULL
           END F14,
           CASE
             WHEN AP.VAT = 10 THEN B.GROSS - BC.GROSS
             ELSE NULL
           END F15,
           CASE
             WHEN AP.VAT = 0 THEN B.GROSS - BC.GROSS
             ELSE NULL
           END F16,
           CASE
             WHEN AP.VAT = 18 THEN B.TAX + BC.TAX
             ELSE NULL
           END F17,
           CASE
             WHEN AP.VAT = 10 THEN B.TAX + BC.TAX
             ELSE NULL
           END F18,
           CASE
             WHEN AP.VAT = 0 THEN B.TAX + BC.TAX
             ELSE NULL
           END F19,
           B.REP_PERIOD_ID, 
           B.BILL_ID, 
           NULL ADVANCE_ID, 
           B.ACCOUNT_ID, 
           B.PROFILE_ID, 
           CM.COMPANY_ID
      FROM BILL_T B, BILL_T BC, COMPANY_T CM, ACCOUNT_PROFILE_T AP
     WHERE B.REP_PERIOD_ID = v_period_id
       AND B.BILL_TYPE   = 'D'
       AND B.CONTRACT_ID = CM.CONTRACT_ID
       AND CM.ACTUAL     = 'Y' 
       AND B.PROFILE_ID  = AP.PROFILE_ID
       AND B.BILL_STATUS IN ('CLOSED', 'READY')
       AND SUBSTR(B.BILL_NO,-1) = 'D' -- только первая корректировка
       AND B.PREV_BILL_ID = BC.BILL_ID
       AND B.PREV_BILL_PERIOD_ID = BC.REP_PERIOD_ID;

    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('TPI_R9_BOOK_SALES.BILL_D= '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- F6. Корректировка корректировочной счет-фактуры 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    INSERT INTO TPI_R9_BOOK_SALES_T (
      SESSION_ID, F1,F2,F3,F4,F5,F6,F7,F8,F12,F13a,F13b,F14,F15,F16,F17,F18,F19,
      REP_PERIOD_ID, BILL_ID, ADVANCE_ID, ACCOUNT_ID, PROFILE_ID, COMPANY_ID
    )
    SELECT p_session_id SESSION_ID,
           NULL F1,
           1 F2,
           NULL F3,
           NULL F4,
           NULL F5,
           B.BILL_NO||';'||TO_CHAR(B.BILL_DATE,'dd.mm.yyyy') F6,
           CM.COMPANY_NAME F7,
           CASE
             WHEN AP.KPP IS NULL THEN CM.INN
             ELSE CM.INN||'/'||AP.KPP  
           END F8,
           B.CURRENCY_ID F12,
           CASE
             WHEN B.CURRENCY_ID != 810 THEN B.TOTAL - BC.TOTAL
             ELSE NULL 
           END F13a,
           CASE
             WHEN B.CURRENCY_ID = 810 THEN B.TOTAL - BC.TOTAL
             ELSE NULL 
           END F13b,
           CASE
             WHEN AP.VAT = 18 THEN B.GROSS - BC.GROSS
             ELSE NULL
           END F14,
           CASE
             WHEN AP.VAT = 10 THEN B.GROSS - BC.GROSS
             ELSE NULL
           END F15,
           CASE
             WHEN AP.VAT = 0 THEN B.GROSS - BC.GROSS
             ELSE NULL
           END F16,
           CASE
             WHEN AP.VAT = 18 THEN B.TAX - BC.TAX
             ELSE NULL
           END F17,
           CASE
             WHEN AP.VAT = 10 THEN B.TAX - BC.TAX
             ELSE NULL
           END F18,
           CASE
             WHEN AP.VAT = 0 THEN B.TAX - BC.TAX
             ELSE NULL
           END F19,
           B.REP_PERIOD_ID, 
           B.BILL_ID, 
           NULL ADVANCE_ID, 
           B.ACCOUNT_ID, 
           B.PROFILE_ID, 
           CM.COMPANY_ID
      FROM BILL_T B, BILL_T BC, COMPANY_T CM, ACCOUNT_PROFILE_T AP
     WHERE B.REP_PERIOD_ID = v_period_id
       AND B.BILL_TYPE   = 'D'
       AND B.CONTRACT_ID = CM.CONTRACT_ID
       AND CM.ACTUAL     = 'Y' 
       AND B.PROFILE_ID  = AP.PROFILE_ID
       AND B.BILL_STATUS IN ('CLOSED', 'READY')
       AND SUBSTR(B.BILL_NO,-1) IN ('F','H', 'J') -- только первая корректировка
       AND B.PREV_BILL_ID = BC.BILL_ID
       AND B.PREV_BILL_PERIOD_ID = BC.REP_PERIOD_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('TPI_R9_BOOK_SALES.BILL_F = '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Авансы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    INSERT INTO TPI_R9_BOOK_SALES_T (
      SESSION_ID, F1,F2,F3,F4,F5,F6,F7,F8,F12,F13a,F13b,F14,F15,F16,F17,F18,F19,
      REP_PERIOD_ID, BILL_ID, ADVANCE_ID, ACCOUNT_ID, PROFILE_ID, COMPANY_ID
    )
    SELECT p_session_id SESSION_ID,
           NULL F1,
           2 F2,
           V.ADVANCE_NO||';'||TO_CHAR(V.ADVANCE_DATE,'dd.mm.yyyy') F3,
           NULL F4,
           NULL F5,
           NULL F6, 
           CM.COMPANY_NAME F7,
           CASE
             WHEN AP.KPP IS NULL THEN CM.INN
             ELSE CM.INN||'/'||AP.KPP  
           END F8,
           V.CURRENCY_ID F12,
           NULL F13a,
           V.TOTAL F13b,
           CASE
             WHEN AP.VAT = 18 THEN V.GROSS
             ELSE NULL
           END F14,
           CASE
             WHEN AP.VAT = 10 THEN V.GROSS
             ELSE NULL
           END F15,
           CASE
             WHEN AP.VAT = 0 THEN V.GROSS
             ELSE NULL
           END F16,
           CASE
             WHEN AP.VAT = 18 THEN V.TAX
             ELSE NULL
           END F17,
           CASE
             WHEN AP.VAT = 10 THEN V.TAX
             ELSE NULL
           END F18,
           CASE
             WHEN AP.VAT = 0 THEN V.TAX
             ELSE NULL
           END F19,
           V.REP_PERIOD_ID, 
           NULL BILL_ID, 
           V.ADVANCE_ID, 
           V.ACCOUNT_ID, 
           AP.PROFILE_ID, 
           CM.COMPANY_ID 
      FROM ADVANCE_T V, ACCOUNT_PROFILE_T AP, COMPANY_T CM
     WHERE V.REP_PERIOD_ID = v_period_id
       AND V.ADVANCE_STATUS IN ('READY','CLOSED')
       AND V.ACCOUNT_ID   = AP.ACCOUNT_ID
       AND AP.ACTUAL      = 'Y'
       AND AP.CONTRACT_ID = CM.CONTRACT_ID
       AND AP.ACTUAL      = 'Y';
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('TPI_R9_BOOK_SALES.ADVANCE = '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Нумеруем строки
    MERGE INTO TPI_R9_BOOK_SALES_T R9
    USING (
        SELECT T.ROWID RID, ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY F8, F3, F4, F5) LINE_ID 
          FROM TPI_R9_BOOK_SALES_T T
    ) T
    ON (
        R9.ROWID = T.ROWID
    )
    WHEN MATCHED THEN UPDATE SET R9.LINE_ID = T.LINE_ID;
        
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ========================================================================= --
-- книга Покупок (8 раздел декларации), таблица TPI_R8_PURCHASE_BOOK_T
-- ========================================================================= --
PROCEDURE Fill_purchase_book (
           p_session_id IN INTEGER
         )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Fill_purchase_book';
    v_count     INTEGER;
    v_period_id INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- читаем из заголовка сессии недостающие данные
    SELECT S.PERIOD_ID INTO v_period_id
      FROM TPI_BOOK_SESSION_T S
     WHERE S.SESSION_ID = p_session_id;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --     
    -- выгрузка стандартных строк для зачета авансов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    INSERT INTO TPI_R8_PURCHASE_BOOK_T (
      SESSION_ID,
      F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,F12,F13,F14,F15,F16,
      BOOK_PERIOD_ID, ADVANCE_ID, TRANSFER_ID
    )
    SELECT NULL F1, 22 F2, 
           AD.ADVANCE_NO||';'||TO_CHAR(AD.ADVANCE_DATE,'dd.mm.yyyy') F3,
           NULL F4, NULL F5, NULL F6, 
           P.DOC_ID||';'||TO_CHAR(P.PAYMENT_DATE,'dd.mm.yyyy') F7, 
           NULL F8, NVL(CT.CONTRACTOR, SL.CONTRACTOR) F9, 
           SL.INN||'/'||SL.KPP F10, NULL F11, NULL F12, AD.CURRENCY_ID F14,
           CASE
             WHEN AD.CURRENCY_ID != 810 THEN T.TRANSFER_TOTAL
           END F15,
           CASE
             WHEN AD.CURRENCY_ID = 810 THEN T.TRANSFER_TOTAL
           END F16,
           PK09_INVOICE.CALC_TAX(T.TRANSFER_TOTAL, 'Y', AP.VAT) F17,
           T.REP_PERIOD_ID BOOK_PERIOD_ID, AD.ADVANCE_ID, P.PAYMENT_ID, TRANSFER_ID
      FROM ADVANCE_T AD, PAYMENT_T P, PAY_TRANSFER_T T, 
           ACCOUNT_PROFILE_T AP, CONTRACTOR_T SL, CONTRACTOR_T CT
     WHERE AD.PAYMENT_ID   = P.PAYMENT_ID
       AND AD.PAY_PERIOD_ID= P.REP_PERIOD_ID
       AND P.PAYMENT_ID    = T.PAYMENT_ID
       AND T.PAY_PERIOD_ID < T.REP_PERIOD_ID
       AND T.REP_PERIOD_ID = v_period_id
       AND AD.PROFILE_ID   = AP.PROFILE_ID
       AND AP.CONTRACTOR_ID= SL.CONTRACTOR_ID
       AND SL.PARENT_ID    = CT.CONTRACTOR_ID(+);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --     
    -- выгрузка корректировок счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --


    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Нумеруем строки
    MERGE INTO TPI_R8_PURCHASE_BOOK_T R8
    USING (
        SELECT T.ROWID RID, ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY F3) LINE_ID 
          FROM TPI_R8_PURCHASE_BOOK_T T
    ) T
    ON (
        R8.ROWID = T.ROWID
    )
    WHEN MATCHED THEN UPDATE SET R8.LINE_ID = T.LINE_ID;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


/*
WITH B AS ( -- набор выставленных счетов фактур за период
    SELECT C.CONTRACT_ID, C.CONTRACT_NO, A.ACCOUNT_ID, A.ACCOUNT_NO, 
           B.REP_PERIOD_ID, B.BILL_ID, B.BILL_NO, B.BILL_DATE, 
           B.BILL_TYPE, B.BILL_STATUS,
           B.TOTAL, B.GROSS, B.TAX, B.DUE 
      FROM ACCOUNT_T A, BILL_T B, CONTRACT_T C
     WHERE A.BILLING_ID    = 2003
       AND A.ACCOUNT_TYPE  = 'J'
       AND A.ACCOUNT_ID    = B.ACCOUNT_ID
       AND B.REP_PERIOD_ID = 201612
       AND B.CONTRACT_ID   = C.CONTRACT_ID
), BP AS ( -- отчет по выставленным счета/фактурам с указанием на платежные документы
    SELECT ROW_NUMBER() OVER (PARTITION BY B.BILL_ID ORDER BY P.PAYMENT_DATE) RN, 
           B.*, P.PAYMENT_ID, P.PAYMENT_DATE, P.DOC_ID, T.TRANSFER_TOTAL 
      FROM PAY_TRANSFER_T T, PAYMENT_T P, B
     WHERE B.BILL_ID       = T.BILL_ID(+)
       AND B.REP_PERIOD_ID = T.REP_PERIOD_ID(+)
       AND T.PAYMENT_ID    = P.PAYMENT_ID(+)
       AND T.PAY_PERIOD_ID = P.REP_PERIOD_ID(+) 
       AND B.REP_PERIOD_ID>= T.PAY_PERIOD_ID(+) -- cчет оплачен платежами текущего периода и авансами
)
SELECT * FROM BP -- 590
 ORDER BY BP.BILL_ID

-- ------------------------------------------------------------------------- --
-- данные для счетов фактур на аванс 
-- ------------------------------------------------------------------------- --
-- расчет аванса для платежей принятых за период
--
MERGE INTO PAYMENT_T P
USING (
   SELECT P.PAYMENT_ID, P.REP_PERIOD_ID, 
          P.RECVD, NVL(SUM(T.TRANSFER_TOTAL),0) TRANSFER_TOTAL,
          P.RECVD - NVL(SUM(T.TRANSFER_TOTAL),0) ADVANCE 
      FROM PAYMENT_T P, PAY_TRANSFER_T T, ACCOUNT_T A
     WHERE P.PAYMENT_ID    = T.PAYMENT_ID(+)
       AND P.REP_PERIOD_ID = T.PAY_PERIOD_ID(+)
       AND P.REP_PERIOD_ID>= T.REP_PERIOD_ID(+)
       AND P.REP_PERIOD_ID = 201612 -- 15.109
       AND P.ACCOUNT_ID    = A.ACCOUNT_ID
       AND A.BILLING_ID    = 2003
       AND A.ACCOUNT_TYPE  = 'J'
     GROUP BY P.PAYMENT_ID, P.REP_PERIOD_ID, P.RECVD
) PA
ON (
    P.PAYMENT_ID = PA.PAYMENT_ID
)
WHEN MATCHED THEN UPDATE SET P.ADVANCE = PA.ADVANCE 

-- ------------------------------------------------------------------------- --
-- формирование счетов фактур на аванс
-- ------------------------------------------------------------------------- --
-- формируем счета фактуры на аванс
WITH PA AS (
SELECT ROW_NUMBER() OVER (PARTITION BY A.ACCOUNT_ID ORDER BY P.PAYMENT_DATE) RN,
       P.REP_PERIOD_ID,
       A.ACCOUNT_ID, A.ACCOUNT_NO, 
       P.REP_PERIOD_ID||'/'||A.ACCOUNT_NO||'A' ADV_FACTURE_NO,
       PK04_PERIOD.PERIOD_TO(P.REP_PERIOD_ID) ADV_FACTURE_DATE, 
       SUM(P.ADVANCE) OVER (PARTITION BY A.ACCOUNT_ID) ACCOUNT_ADVANCE,
       P.ADVANCE PAYMENT_ADVANCE, 
       P.PAYMENT_DATE, P.DOC_ID 
  FROM PAYMENT_T P, ACCOUNT_T A
 WHERE P.ACCOUNT_ID = A.ACCOUNT_ID
   AND P.ADVANCE > 0
   AND P.REP_PERIOD_ID = 201612
   AND P.ADVANCE_ID IS NULL
 ORDER BY A.ACCOUNT_NO, P.PAYMENT_DATE
)
SELECT --SQ_BILL_ID.NEXTVAL ADVANCE_ID,
       REP_PERIOD_ID, ACCOUNT_ID, ADV_FACTURE_NO, ADV_FACTURE_DATE, ACCOUNT_ADVANCE 
  FROM PA
 WHERE PA.RN = 1
 
SELECT * FROM ADVANCE_T

SELECT A.ACCOUNT_ID, 
       P.REP_PERIOD_ID||'/'||A.ACCOUNT_NO||'A' ADV_FACTURE_NO,
       PK04_PERIOD.PERIOD_TO(P.REP_PERIOD_ID) ADV_FACTURE_DATE, 
       SUM(P.ADVANCE) ADVANCE,
       'Q-'||:p_task_id ADVANCE_STATUS 
  FROM PAYMENT_T P, ACCOUNT_T A
 WHERE P.ACCOUNT_ID = A.ACCOUNT_ID
   AND P.ADVANCE > 0
   AND P.REP_PERIOD_ID = 201612
   AND P.ADVANCE_ID IS NULL
 GROUP BY A.ACCOUNT_ID, A.ACCOUNT_NO, P.REP_PERIOD_ID
*/



END PK37_BILLING_BOOKS;
/
