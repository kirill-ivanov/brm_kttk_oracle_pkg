CREATE OR REPLACE PACKAGE PK90_JOBS_DAILY
IS
    --
    -- Ежедневные работы, для биллинга
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK90_JOBS_DAILY';
    -- ==============================================================================
    type t_refc is ref cursor;
    
    -- ----------------------------------------------------------------- --
    -- ежедневная профилактическая процедура
    -- ----------------------------------------------------------------- --
    PROCEDURE Operation;
   
    -- проверка признака актуальности профилей
    PROCEDURE Check_profile_actual;

    -- проверка профилей на пересечение интервалов дат
    PROCEDURE Check_profile_date;
    
    -- Список профилей с пересекающимися датами
    PROCEDURE Intersected_profile_list( 
                   p_recordset    OUT t_refc
               );

    -- проверка признака актуальности для компаний
    PROCEDURE Check_company_actual;
    
    -- проверка компаний на пересечение интервалов дат
    PROCEDURE Check_company_date;

    -- проверка привязки открытых счетов к профилям
    PROCEDURE Check_bill_profile;

END PK90_JOBS_DAILY;
/
CREATE OR REPLACE PACKAGE BODY PK90_JOBS_DAILY
IS

-- ----------------------------------------------------------------- --
-- ежедневная профилактическая процедура
-- ----------------------------------------------------------------- --
PROCEDURE Operation
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Operation';
    v_count      INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start.', v_prcName, Pk01_Syslog.L_info );
    
    -- проверка признака актуальности профилей
    Check_profile_actual;

    -- проверка профилей на пересечение интервалов дат
    Check_profile_date;
    
    -- проверка признака актуальности для компаний
    Check_company_actual;
    
    -- проверка компаний на пересечение интервалов дат
    Check_company_date;

    -- проверка привязки открытых счетов к профилям
    Check_bill_profile;
    
    -- подтверждаем изменения
    Commit;
    
    Pk01_Syslog.Write_msg('Stop.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  
EXCEPTION
    WHEN OTHERS THEN
        Rollback;
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------- --
-- проверка признака актуальности профилей
-- ----------------------------------------------------------------- --
PROCEDURE Check_profile_actual
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_profile_actual';
    v_count      INTEGER;
BEGIN
    -- для дневной работы достаточно, все остальное сделает триггер
    UPDATE ACCOUNT_PROFILE_T AP SET AP.ACTUAL = 'Y'
     WHERE AP.PROFILE_ID IN (
        SELECT PROFILE_ID 
          FROM (
        SELECT ROW_NUMBER() OVER (PARTITION BY AP.ACCOUNT_ID ORDER BY AP.PROFILE_ID DESC) RN,
               AP.PROFILE_ID, AP.ACTUAL 
          FROM ACCOUNT_PROFILE_T AP
         WHERE AP.DATE_FROM <= SYSDATE
           AND (AP.DATE_TO IS NULL OR SYSDATE < AP.DATE_TO)
        )
        WHERE RN = 1
          AND ACTUAL != 'Y'
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_PROFILE_T '||v_count||' current rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    /*
    -- в принципе это тоже не нужно, триггер должен справиться
    UPDATE ACCOUNT_PROFILE_T AP SET AP.ACTUAL = 'Y'
    WHERE AP.PROFILE_ID IN (
        WITH AP AS (
            SELECT PROFILE_ID, ACCOUNT_ID, DATE_FROM, DATE_TO, ACTUAL, 
                   MAX(NVL(DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))) OVER (PARTITION BY AP.ACCOUNT_ID) MAX_DATE 
              FROM ACCOUNT_PROFILE_T AP
        )
        SELECT AP.PROFILE_ID FROM AP
         WHERE AP.MAX_DATE < SYSDATE
           AND AP.MAX_DATE = NVL(AP.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
           AND AP.ACTUAL IS NULL
    );  
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ACCOUNT_PROFILE_T '||v_count||' old rows updated', v_prcName, Pk01_Syslog.L_info );
    */
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------- --
-- проверка профилей на пересечение интервалов дат
-- ----------------------------------------------------------------- --
PROCEDURE Check_profile_date
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_profile_date';
    v_count      INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count
           -- AP.ACCOUNT_ID, AP.PROFILE_ID, AP.DATE_FROM, AP.DATE_TO, AP.MODIFY_DATE, AP.MODIFIED_BY 
      FROM ACCOUNT_PROFILE_T AP
     WHERE EXISTS (
        SELECT * FROM ACCOUNT_PROFILE_T P
         WHERE AP.ACCOUNT_ID  = P.ACCOUNT_ID
           AND AP.PROFILE_ID != P.PROFILE_ID
           AND (AP.DATE_FROM BETWEEN P.DATE_FROM AND NVL(P.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
            OR NVL(AP.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy')) BETWEEN P.DATE_FROM AND NVL(P.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
            OR P.DATE_FROM BETWEEN AP.DATE_FROM AND NVL(AP.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
           ) 
     )
    --ORDER BY AP.ACCOUNT_ID, AP.PROFILE_ID
    ;
    Pk01_Syslog.Write_msg('ACCOUNT_PROFILE_T '||v_count||' rows intersected', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Список профилей с пересекающимися датами
PROCEDURE Intersected_profile_list( 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Intersected_profile_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
      SELECT AP.ACCOUNT_ID, AP.PROFILE_ID, AP.DATE_FROM, AP.DATE_TO, AP.MODIFY_DATE, AP.MODIFIED_BY 
        FROM ACCOUNT_PROFILE_T AP
       WHERE EXISTS (
          SELECT * FROM ACCOUNT_PROFILE_T P
           WHERE AP.ACCOUNT_ID  = P.ACCOUNT_ID
             AND AP.PROFILE_ID != P.PROFILE_ID
             AND (AP.DATE_FROM BETWEEN P.DATE_FROM AND NVL(P.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
              OR NVL(AP.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy')) BETWEEN P.DATE_FROM AND NVL(P.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
              OR P.DATE_FROM BETWEEN AP.DATE_FROM AND NVL(AP.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
             ) 
     )
    ORDER BY AP.ACCOUNT_ID, AP.PROFILE_ID;
    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ----------------------------------------------------------------- --
-- проверка признака актуальности для компаний
-- ----------------------------------------------------------------- --
PROCEDURE Check_company_actual
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_company_actual';
    v_count      INTEGER;
BEGIN
    -- для дневной работы достаточно, все остальное сделает триггер
    UPDATE COMPANY_T CM SET CM.ACTUAL = 'Y'
     WHERE CM.COMPANY_ID IN (
        SELECT COMPANY_ID 
          FROM (
        SELECT ROW_NUMBER() OVER (PARTITION BY CONTRACT_ID ORDER BY COMPANY_ID DESC) RN,
               COMPANY_ID, ACTUAL 
          FROM COMPANY_T
         WHERE DATE_FROM <= SYSDATE
           AND (DATE_TO IS NULL OR SYSDATE < DATE_TO)
        )
        WHERE RN = 1
          AND ACTUAL != 'Y'
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('COMPANY_T '||v_count||' current rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- ----------------------------------------------------------------- --
-- проверка компаний на пересечение интервалов дат
-- ----------------------------------------------------------------- --
PROCEDURE Check_company_date
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_company_date';
    v_count      INTEGER;
BEGIN
    -- проверка на пересечение интервалов
    SELECT COUNT(*) INTO v_count
        -- CM.COMPANY_ID, CM.CONTRACT_ID, CM.COMPANY_NAME, CM.DATE_FROM, CM.DATE_TO
      FROM COMPANY_T CM
     WHERE EXISTS (
        SELECT * 
          FROM COMPANY_T D
         WHERE CM.CONTRACT_ID = D.CONTRACT_ID
           AND CM.COMPANY_ID != D.COMPANY_ID
           AND (CM.DATE_FROM BETWEEN D.DATE_FROM AND NVL(D.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
            OR NVL(CM.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy')) BETWEEN D.DATE_FROM AND NVL(D.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
            OR D.DATE_FROM BETWEEN CM.DATE_FROM AND NVL(CM.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
           )
    );
    Pk01_Syslog.Write_msg('COMPANY_T '||v_count||' rows intersected', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
 
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ----------------------------------------------------------------- --
-- проверка привязки открытых счетов к профилям
-- ----------------------------------------------------------------- --
PROCEDURE Check_bill_profile
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_bill_profile';
    v_count      INTEGER;
BEGIN
    MERGE INTO BILL_T B
    USING (
        SELECT B.BILL_ID, B.REP_PERIOD_ID, B.BILL_DATE, B.BILL_TYPE, 
               --B.PROFILE_ID, B.CONTRACT_ID, B.CONTRACTOR_ID, B.CONTRACTOR_BANK_ID,
               --AP.PROFILE_ID, AP.CONTRACT_ID, AP.CONTRACTOR_ID, AP.CONTRACTOR_BANK_ID
               APN.PROFILE_ID, APN.CONTRACT_ID, APN.CONTRACTOR_ID, APN.CONTRACTOR_BANK_ID 
          FROM BILL_T B, PERIOD_T P, ACCOUNT_PROFILE_T AP, ACCOUNT_PROFILE_T APN
         WHERE B.REP_PERIOD_ID = P.PERIOD_ID
           AND P.POSITION IN ('OPEN','BILL')
           AND B.BILL_STATUS = 'OPEN'
           AND B.BILL_TYPE   = 'B'  -- на всякий случай
           AND AP.ACCOUNT_ID = B.ACCOUNT_ID
           AND AP.DATE_FROM <= B.BILL_DATE
           AND (AP.DATE_TO IS NULL OR B.BILL_DATE <= AP.DATE_TO)
           AND (B.PROFILE_ID    != AP.PROFILE_ID
            OR  B.CONTRACT_ID   != AP.CONTRACT_ID
            OR  B.CONTRACTOR_ID != AP.CONTRACTOR_ID
            OR  B.CONTRACTOR_BANK_ID != AP.CONTRACTOR_BANK_ID
           )
           AND  B.ACCOUNT_ID   = APN.ACCOUNT_ID   
           AND  APN.DATE_FROM <= B.BILL_DATE
           AND (APN.DATE_TO IS NULL OR B.BILL_DATE <= APN.DATE_TO)
    ) AP
    ON (
      B.BILL_ID       = AP.BILL_ID AND 
      B.REP_PERIOD_ID = AP.REP_PERIOD_ID
    )
    WHEN MATCHED THEN UPDATE SET B.PROFILE_ID         = AP.PROFILE_ID, 
                                 B.CONTRACT_ID        = AP.CONTRACT_ID, 
                                 B.CONTRACTOR_ID      = AP.CONTRACTOR_ID,  
                                 B.CONTRACTOR_BANK_ID = AP.CONTRACTOR_BANK_ID
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T.PROFILE_ID was changed for '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

END PK90_JOBS_DAILY;
/
