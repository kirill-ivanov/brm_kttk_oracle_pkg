CREATE OR REPLACE PACKAGE PK400_DEBITORS
IS
    --
    -- Пакет для печати КВИТАНЦИЙ на оплату для ФИЗИЧЕСКИХ ЛИЦ
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK400_DEBITORS';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- получить список услуг на лицевом счете в одну строку
    --
    FUNCTION Get_AccSrv_list(p_account_id IN VARCHAR2) RETURN VARCHAR2;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Список клиентов ЮРИДИЧЕСКИХ лиц
    --   - при ошибке выставляет исключение
    PROCEDURE Customer_list( 
                   p_recordset OUT t_refc
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Список клиентов ФИЗИЧЕСКИХ лиц
    --   - при ошибке выставляет исключение
    PROCEDURE Subscriber_list( 
                   p_recordset OUT t_refc
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Развернутый отчет по должникам ФИЗИКАМ
    --   - при ошибке выставляет исключение
    PROCEDURE Subs_detail_report( 
                   p_recordset OUT t_refc
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Развернутый отчет по должникам ЮРИКАМ
    --   - при ошибке выставляет исключение
    PROCEDURE Cust_detail_report( 
                   p_recordset OUT t_refc
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Список должников ФИЗИЧЕСКИХ лиц, для обзвона
    --   - при ошибке выставляет исключение
    PROCEDURE Subs_debitors_for_call( 
                   p_recordset OUT t_refc--,
                   --p_debt_days IN INTEGER, -- кол-во дней просроченной задолженности
                   --p_debt_min  IN NUMBER   -- порог дебиторской задолженности
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Список должников ФИЗИЧЕСКИХ лиц, для блокировки
    --   - при ошибке выставляет исключение
    PROCEDURE Subs_debitors_for_lock( 
                   p_recordset OUT t_refc,
                   p_debt_min  IN NUMBER   -- порог дебиторской задолженности
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Список должников ФИЗИЧЕСКИХ лиц, для разблокировки
    --   - при ошибке выставляет исключение
    PROCEDURE Subs_debitors_for_unlock( 
                   p_recordset OUT t_refc,
                   p_debt_min  IN NUMBER   -- порог дебиторской задолженности
               );
               
-- Платежи ЮЛ за период
PROCEDURE DZ_JL_PAYMENTS(p_recordset OUT t_refc, p_start_pay_date IN DATE, p_end_pay_date IN DATE);
-- Платежи Физ.лиц за период
PROCEDURE DZ_FL_PAYMENTS(p_recordset OUT t_refc, p_start_pay_date IN DATE, p_end_pay_date IN DATE);
-- Отчет по неразнесенным сумам платежей ЮЛ
PROCEDURE DZ_JL_UNKNOWN_PAYS(p_recordset OUT t_refc);  
-- создать новую запись об обработанном файле  
FUNCTION Process_new_call_file (
               p_file_name     IN VARCHAR2,
               p_call_type	   IN VARCHAR2,  
               p_client_type   IN CHAR, 
               p_start_process   IN DATE,
               p_end_process     IN DATE,
               p_file_date       IN DATE  
           ) RETURN INTEGER;
-- Обновляем время окончания процесса обработки файла
PROCEDURE ChangeFileEndProcess(p_file_id IN INTEGER, p_end_process IN DATE);          
-- Пишем историю обзвонов
PROCEDURE WriteCallHistory( p_file_id IN INTEGER, 
                            p_account_id IN INTEGER, 
                            p_phone IN VARCHAR2, 
                            p_call_date IN DATE, 
                            p_result_info IN VARCHAR2, 
                            p_result IN INTEGER);
-- Детализированный отчет об информировании по ДЗ (ФЛ)
PROCEDURE DZ_FL_DETAIL_INFO(p_recordset OUT t_refc);                            
-- Агрегированный отчет о покрытии платежами выставленных счетов за месяц
PROCEDURE Agr_bill_payments(p_recordset OUT t_refc, p_rep_period_id IN INTEGER); 
END PK400_DEBITORS;
/
CREATE OR REPLACE PACKAGE BODY PK400_DEBITORS
IS
------------------------------------------
-- Получаем новый ID для файла
------------------------------------------
FUNCTION Next_deb_file_id RETURN INTEGER IS
BEGIN
    RETURN SQ_DEB_CALL_FILE_ID.NEXTVAL; 
END;

------------------------------------------
-- Получаем новый ID для результатов обзвона
------------------------------------------
FUNCTION Next_deb_result_id RETURN INTEGER IS
BEGIN
    RETURN SQ_DEB_CALL_RESULT_ID.NEXTVAL; 
END;

------------------------------------------
-- Получаем новый ID для истории обзвона
------------------------------------------
FUNCTION Next_deb_history_id RETURN INTEGER IS
BEGIN
    RETURN SQ_DEB_CALL_HISTORY_ID.NEXTVAL; 
END;
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- получить список услуг на лицевом счете в одну строку
--
FUNCTION Get_AccSrv_list(p_account_id IN VARCHAR2) RETURN VARCHAR2
IS
    v_services   VARCHAR2(200);
    v_count      INTEGER := 0;
BEGIN
    FOR r_ord IN (
         SELECT S.SERVICE_CODE, COUNT(*) NBR
           FROM ORDER_T O, SERVICE_T S
          WHERE O.SERVICE_ID = S.SERVICE_ID
            AND O.ACCOUNT_ID = p_account_id
            AND SYSDATE BETWEEN O.DATE_FROM AND O.DATE_TO
          GROUP BY S.SERVICE_CODE
      )
    LOOP
        IF v_count > 0 THEN
            v_services := v_services||',';
        END IF;
        v_services := SUBSTR(v_services || r_ord.service_code ||'-'||r_ord.nbr, 1, 200);
        v_count := v_count + 1;
    END LOOP;
    RETURN v_services;
END Get_AccSrv_list;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Список клиентов ЮРИДИЧЕСКИХ лиц
--   - при ошибке выставляет исключение
PROCEDURE Customer_list( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Customer_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
          SELECT C.CONTRACT_ID, C.CONTRACT_NO,
                 A.ACCOUNT_ID, A.ACCOUNT_NO, A.ACCOUNT_TYPE, A.BALANCE, A.BALANCE_DATE, 
                 BR.SHORT_NAME BRAND_NAME, AG.SHORT_NAME AGENT_NAME,
                 CS.SHORT_NAME, CS.INN, CS.KPP,
                 AA.CITY, AA.ADDRESS, AA.PHONES, AA.EMAIL, O.ORDERS_NUM, O.LOCKS_NUM,
                 PK400_DEBITORS.Get_AccSrv_list(A.ACCOUNT_ID) SERVICES
            FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C,
                 CUSTOMER_T CS, ACCOUNT_CONTACT_T AA,
                 CONTRACTOR_T BR, CONTRACTOR_T AG,
                 (
                    SELECT O.ACCOUNT_ID, 
                           SUM(DECODE(O.STATUS, Pk00_Const.c_ORDER_STATE_LOCK,1,0)) LOCKS_NUM, 
                           COUNT(*) ORDERS_NUM
                      FROM ORDER_T O
                     WHERE O.DATE_FROM <= SYSDATE
                       AND (O.DATE_TO IS NULL OR SYSDATE <= O.DATE_TO)
                    GROUP BY O.ACCOUNT_ID
                 ) O
           WHERE A.ACCOUNT_ID = AP.ACCOUNT_ID
             AND A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_J
             AND AP.CONTRACT_ID = C.CONTRACT_ID
             AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
             AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
             AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
             AND AP.AGENT_ID  = AG.CONTRACTOR_ID(+)
             AND A.ACCOUNT_ID = AA.ACCOUNT_ID
             AND AA.ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_JUR
             AND A.ACCOUNT_ID = O.ACCOUNT_ID
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
-- Список клиентов ФИЗИЧЕСКИХ лиц
--   - при ошибке выставляет исключение
PROCEDURE Subscriber_list( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Subscriber_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
          SELECT C.CONTRACT_ID, C.CONTRACT_NO,
                 A.ACCOUNT_ID, A.ACCOUNT_NO, A.ACCOUNT_TYPE, A.BALANCE, A.BALANCE_DATE, 
                 INITCAP(S.LAST_NAME)||' '||INITCAP(S.FIRST_NAME)||' '||INITCAP(S.MIDDLE_NAME) SUBS_NAME,
                 BR.SHORT_NAME BRAND_NAME, AG.SHORT_NAME AGENT_NAME,
                 AA.CITY, AA.ADDRESS, AA.PHONES, AA.EMAIL, O.ORDERS_NUM, O.LOCKS_NUM,
                 PK400_DEBITORS.Get_AccSrv_list(A.ACCOUNT_ID) SERVICES
            FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C,
                 SUBSCRIBER_T S, ACCOUNT_CONTACT_T AA,
                 CONTRACTOR_T BR, CONTRACTOR_T AG,
                 (
                    SELECT O.ACCOUNT_ID, 
                           SUM(DECODE(O.STATUS, Pk00_Const.c_ORDER_STATE_LOCK,1,0)) LOCKS_NUM, 
                           COUNT(*) ORDERS_NUM
                      FROM ORDER_T O
                     WHERE O.DATE_FROM <= SYSDATE
                       AND (O.DATE_TO IS NULL OR SYSDATE <= O.DATE_TO)
                    GROUP BY O.ACCOUNT_ID
                 ) O
           WHERE A.ACCOUNT_ID    = AP.ACCOUNT_ID
             AND A.ACCOUNT_TYPE  = Pk00_Const.c_ACC_TYPE_P
             AND AP.CONTRACT_ID  = C.CONTRACT_ID
             AND AP.SUBSCRIBER_ID= S.SUBSCRIBER_ID
             AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
             AND AP.BRANCH_ID    = BR.CONTRACTOR_ID(+)
             AND AP.AGENT_ID     = AG.CONTRACTOR_ID(+)
             AND A.ACCOUNT_ID    = AA.ACCOUNT_ID
             AND AA.ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_REG
             AND A.ACCOUNT_ID    = O.ACCOUNT_ID
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
-- Развернутый отчет по должникам ФИЗИКАМ
--   - при ошибке выставляет исключение
PROCEDURE Subs_detail_report( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Subs_detail_report';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
          WITH ABN AS (
              SELECT A.ACCOUNT_ID,
                     A.BALANCE,
                     A.ACCOUNT_NO,
                     DECODE (A.STATUS, 'B', 'Активный','C', 'Неактивный') ACCOUNT_STATUS,
                     C.CONTRACT_NO,
                     INITCAP(S.LAST_NAME)||' '||INITCAP(S.FIRST_NAME)||' '||INITCAP(S.MIDDLE_NAME) SUBS_NAME,   -- Ф.И.О.
                     AA.CITY,
                     AA.ADDRESS,
                     AA.PHONES,
                     BR.SHORT_NAME BRAND_NAME,     -- бренд
                     AG.SHORT_NAME AGENT_NAME      -- агент
                FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, 
                     CONTRACT_T C, SUBSCRIBER_T S,
                     CONTRACTOR_T BR, CONTRACTOR_T AG,
                     ACCOUNT_CONTACT_T AA
               WHERE A.ACCOUNT_TYPE = PK00_CONST.C_ACC_TYPE_P
                 AND A.ACCOUNT_ID = AP.ACCOUNT_ID
                 AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
                 AND AP.CONTRACT_ID = C.CONTRACT_ID
                 AND AP.SUBSCRIBER_ID = S.SUBSCRIBER_ID
                 AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
                 AND AP.AGENT_ID  = AG.CONTRACTOR_ID(+)
                 AND A.ACCOUNT_ID = AA.ACCOUNT_ID
                 AND AA.ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_REG -- 'REG'
          ),
          BL AS (
              SELECT B.ACCOUNT_ID, MIN(B.BILL_DATE) DEB_BILL_DATE  
                FROM BILL_T B 
               WHERE B.DUE < 0 
              GROUP BY B.ACCOUNT_ID
          ),
          LK AS (
              SELECT O.ACCOUNT_ID, 
                     SUM(DECODE(O.STATUS, 'LOCK',1,0)) LOCKS_NUM, --Pk00_Const.c_ORDER_STATE_LOCK 
                     COUNT(*) ORDERS_NUM
                FROM ORDER_T O
               WHERE (O.DATE_TO IS NULL OR SYSDATE BETWEEN O.DATE_FROM AND O.DATE_TO)
              GROUP BY O.ACCOUNT_ID
          )
          SELECT ABN.ACCOUNT_ID,
                 ABN.BALANCE,
                 ABN.ACCOUNT_NO,
                 ABN.ACCOUNT_STATUS,
                 ABN.CONTRACT_NO,
                 ABN.SUBS_NAME,   -- Ф.И.О.
                 ABN.CITY,
                 ABN.ADDRESS,
                 ABN.PHONES,
                 ABN.BRAND_NAME,
                 ABN.AGENT_NAME,
                 BL.DEB_BILL_DATE,
                 ROUND(MONTHS_BETWEEN(SYSDATE, BL.DEB_BILL_DATE)) DBT_MONTH,
                 CASE
                     WHEN LK.LOCKS_NUM = 0 THEN 'ACTIVE'
                     WHEN LK.LOCKS_NUM = LK.ORDERS_NUM THEN 'LOCKED'
                     WHEN LK.LOCKS_NUM < LK.ORDERS_NUM THEN 'PARTLY LOCKED'
                 END LOCKS
            FROM ABN, LK, BL -- PI
           WHERE ABN.ACCOUNT_ID = BL.ACCOUNT_ID 
             AND ABN.ACCOUNT_ID = LK.ACCOUNT_ID
             AND ABN.BALANCE < -100    -- потенциалные должники (будем передавать как параметр)
             AND LK.LOCKS_NUM = 0      -- только не заблокированные клиенты 
          ORDER BY MONTHS_BETWEEN(SYSDATE, BL.DEB_BILL_DATE) DESC, ABN.BALANCE
         ;    
/*
SELECT A.ACCOUNT_ID,
       A.BALANCE,                           -- текущий баланс на лицевом счете
       RP.CLOSE_BALANCE LAST_PERIOD_BALANCE, -- исходящий баланс последнего закрытого периода
       TRUNC (B.DEB_BILL_DATE, 'mm') DEB_BILL_DATE, -- дата последнего незакрытого счета
       ROUND (MONTHS_BETWEEN (SYSDATE, B.DEB_BILL_DATE), 1) DEB_MONTHS, -- кол-во месяцев задолженности
          INITCAP (S.LAST_NAME)
       || ' '
       || INITCAP (S.FIRST_NAME)
       || ' '
       || INITCAP (S.MIDDLE_NAME)
          SUBS_NAME,                                                 -- Ф.И.О.
       BR.SHORT_NAME BRAND_NAME,                                      -- бренд
       AG.SHORT_NAME AGENT_NAME,                                      -- агент
       AA.CITY,
       AA.ADDRESS,
       AA.PHONES,
       DECODE (A.STATUS,
               'B', 'Активный',
               'C', 'Неактивный')
          ACCOUNT_STATUS                                              -- адрес
  FROM ACCOUNT_T A,
       ACCOUNT_PROFILE_T AP,
       SUBSCRIBER_T S,
       ACCOUNT_CONTACT_T AA,
       CONTRACTOR_T BR,
       CONTRACTOR_T AG,
       REP_PERIOD_INFO_T RP,
       PERIOD_T PR,
       (  SELECT B.ACCOUNT_ID, MIN (B.BILL_DATE) DEB_BILL_DATE
            FROM BILL_T B
           WHERE B.DUE < 0
        GROUP BY B.ACCOUNT_ID) B
 WHERE     A.ACCOUNT_ID = AP.ACCOUNT_ID
       AND A.ACCOUNT_TYPE = 'P'
       AND AP.SUBSCRIBER_ID = S.SUBSCRIBER_ID
       AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
       AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
       AND AP.AGENT_ID = AG.CONTRACTOR_ID(+)
       AND A.ACCOUNT_ID = AA.ACCOUNT_ID
       AND AA.ADDRESS_TYPE = 'REG'
       AND PR.POSITION = 'LAST'
       AND RP.REP_PERIOD_ID = PR.PERIOD_ID
       AND RP.ACCOUNT_ID = A.ACCOUNT_ID
       AND A.ACCOUNT_ID = B.ACCOUNT_ID
       AND a.BALANCE < 0
       AND A.STATUS <> 'T'

*/          
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Развернутый отчет по должникам ЮРИКАМ
--   - при ошибке выставляет исключение
PROCEDURE Cust_detail_report( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Cust_detail_report';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
          WITH ABN AS (
              SELECT A.ACCOUNT_ID,
                     A.BALANCE,
                     A.ACCOUNT_NO,
                     DECODE (A.STATUS, 'B', 'Активный','C', 'Неактивный') ACCOUNT_STATUS,
                     C.CONTRACT_NO,
                     CS.CUSTOMER,                  -- компания, ИНН, КПП 
                     CS.INN, 
                     CS.KPP,  
                     AA.CITY,
                     AA.ADDRESS,
                     AA.PHONES,
                     BR.SHORT_NAME BRAND_NAME,     -- бренд
                     AG.SHORT_NAME AGENT_NAME      -- агент
                FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, 
                     CONTRACT_T C, CUSTOMER_T CS,
                     CONTRACTOR_T BR, CONTRACTOR_T AG,
                     ACCOUNT_CONTACT_T AA
               WHERE A.ACCOUNT_TYPE = PK00_CONST.C_ACC_TYPE_J
                 AND A.ACCOUNT_ID = AP.ACCOUNT_ID
                 AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
                 AND AP.CONTRACT_ID = C.CONTRACT_ID
                 AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
                 AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
                 AND AP.AGENT_ID  = AG.CONTRACTOR_ID(+)
                 AND A.ACCOUNT_ID = AA.ACCOUNT_ID
                 AND AA.ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_JUR -- 'JUR'
          ),
          BL AS (
              SELECT B.ACCOUNT_ID, MIN(B.BILL_DATE) DEB_BILL_DATE  
                FROM BILL_T B 
               WHERE B.DUE < 0 
              GROUP BY B.ACCOUNT_ID
          ),
          LK AS (
              SELECT O.ACCOUNT_ID, 
                     SUM(DECODE(O.STATUS, 'LOCK',1,0)) LOCKS_NUM, --Pk00_Const.c_ORDER_STATE_LOCK 
                     COUNT(*) ORDERS_NUM
                FROM ORDER_T O
               WHERE (O.DATE_TO IS NULL OR SYSDATE BETWEEN O.DATE_FROM AND O.DATE_TO)
              GROUP BY O.ACCOUNT_ID
          )
          SELECT ABN.ACCOUNT_ID,
                 ABN.BALANCE,
                 ABN.ACCOUNT_NO,
                 ABN.ACCOUNT_STATUS,
                 ABN.CONTRACT_NO,
                 ABN.CUSTOMER, 
                 ABN.INN, 
                 ABN.KPP,
                 ABN.CITY,
                 ABN.ADDRESS,
                 ABN.PHONES,
                 ABN.BRAND_NAME,
                 ABN.AGENT_NAME,
                 BL.DEB_BILL_DATE,
                 TRUNC(SYSDATE - BL.DEB_BILL_DATE) DEB_DAYS, -- кол-во дней задолженности
                 ROUND(MONTHS_BETWEEN(SYSDATE, BL.DEB_BILL_DATE)) DBT_MONTH, -- кол-во месяцев задолженности
                 CASE
                     WHEN LK.LOCKS_NUM = 0 THEN 'ACTIVE'
                     WHEN LK.LOCKS_NUM = LK.ORDERS_NUM THEN 'LOCKED'
                     WHEN LK.LOCKS_NUM < LK.ORDERS_NUM THEN 'PARTLY LOCKED'
                 END LOCKS,                                  -- 
                 LK.ORDERS_NUM,                              -- кол-во заказов на л/с
                 LK.LOCKS_NUM,                               -- кол-во заблокированных заказов на л/с
                 PK400_DEBITORS.Get_AccSrv_list(ABN.ACCOUNT_ID) SERVICES -- услуги на л/с
            FROM ABN, LK, BL -- PI
           WHERE ABN.ACCOUNT_ID = BL.ACCOUNT_ID 
             AND ABN.ACCOUNT_ID = LK.ACCOUNT_ID
             AND ABN.BALANCE < -100    -- потенциалные должники (будем передавать как параметр)
             AND LK.LOCKS_NUM = 0      -- только не заблокированные клиенты 
          ORDER BY MONTHS_BETWEEN(SYSDATE, BL.DEB_BILL_DATE) DESC, ABN.BALANCE
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
-- Список должников ФИЗИЧЕСКИХ лиц, для обзвона
--   - при ошибке выставляет исключение
-- 
-- По вопросу информирования должников имею сказать следующее:
-- 1. Список формируется интеграционным процессом Debitors
-- 2. Для обзвона используется файл fl_in.csv или ul_in.csv, которые расположены в каталоге /export/home/tibco/IVR/OutDir
-- 3. Формат полей следующий:
--   "Id,phonenumber1,phonenumber2,timezone,sum_debt" , где
--        - Id- идентификатор
--        - phonenumber1 – телефонный номер абонента
--        - phonenumber2 – альтернативный телефонный номер
--        - timezone – GMT-смещение
--        - sum_debt   - сумма задолженности.
--
PROCEDURE Subs_debitors_for_call( 
               p_recordset OUT t_refc--,
               -- эти два параметра есть в запросе!
              -- p_debt_days IN INTEGER, -- кол-во дней просроченной задолженности
              -- p_debt_min  IN NUMBER   -- порог дебиторской задолженности
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Subs_debitors_for_call';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        WITH 
        LK AS (
            SELECT O.ACCOUNT_ID, 
                   SUM(DECODE(O.STATUS, 'LOCK',1,0)) LOCKS_NUM, --Pk00_Const.c_ORDER_STATE_LOCK 
                   COUNT(*) ORDERS_NUM
              FROM ORDER_T O
             WHERE (O.DATE_TO IS NULL OR SYSDATE BETWEEN O.DATE_FROM AND O.DATE_TO)
            GROUP BY O.ACCOUNT_ID
        ),
        PH AS (
            SELECT O.ACCOUNT_ID, 
                   MIN(O.TIME_ZONE)     TIME_ZONE,
                   MIN(OT.PHONE_NUMBER) PHONE_NUMBER,
                   MAX(OT.PHONE_NUMBER) ALT_PHONE_NUMBER -- может будет альтернативным 
              FROM ORDER_T O, ORDER_PHONES_T OT
             WHERE O.ORDER_ID = OT.ORDER_ID
               AND SYSDATE BETWEEN OT.DATE_FROM AND OT.DATE_TO
               AND SYSDATE BETWEEN O.DATE_FROM AND O.DATE_TO
             GROUP BY O.ACCOUNT_ID
        ),
        DB AS (
            SELECT B.ACCOUNT_ID, MIN(B.BILL_DATE) DEB_BILL_DATE  
              FROM BILL_T B 
             WHERE B.DUE < 0 
            GROUP BY B.ACCOUNT_ID
        )
        SELECT A.ACCOUNT_ID   Id,           -- идентификатор
               PH.PHONE_NUMBER phonenumber1, -- телефонный номер абонента
               NVL(AC.PHONES, PH.ALT_PHONE_NUMBER) phonenumber2, -- альтернативный телефонный номер (из адреса доставки)
               PH.TIME_ZONE    timezone,     -- timezone – GMT-смещение
               A.BALANCE       sum_debt,     -- сумма задолженности.
               TRUNC(SYSDATE - DB.DEB_BILL_DATE)   deb_days      -- кол-во дней задолженности (не отображаем в отчете) 
          FROM ACCOUNT_T A, ACCOUNT_CONTACT_T AC, LK, PH, DB
         WHERE A.ACCOUNT_ID = PH.ACCOUNT_ID
           AND A.ACCOUNT_ID = DB.ACCOUNT_ID
           AND A.ACCOUNT_ID = LK.ACCOUNT_ID(+)
           AND A.ACCOUNT_ID = AC.ACCOUNT_ID
           AND A.ACCOUNT_TYPE  = Pk00_Const.c_ACC_TYPE_P
           AND AC.ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_DLV
           AND (AC.DATE_TO IS NULL OR (SYSDATE BETWEEN AC.DATE_FROM AND AC.DATE_TO));

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Список должников ФИЗИЧЕСКИХ лиц, для блокировки
--   - при ошибке выставляет исключение
PROCEDURE Subs_debitors_for_lock( 
               p_recordset OUT t_refc,
               p_debt_min  IN NUMBER   -- порог дебиторской задолженности
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Cust_debitors_for_lock';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        WITH BL AS (
            SELECT B.ACCOUNT_ID, 
                   SUM(B.GROSS + B.TAX) TOTAL -- сумма начислений с налогами, не входящая   
              FROM BILL_T B                   -- в просроченную задолженность
             WHERE B.BILL_STATUS = Pk00_Const.c_BILL_STATE_CLOSED -- счет выставлен
               AND B.PAID_TO > SYSDATE        -- не просроченная задолженность
            GROUP BY B.ACCOUNT_ID
        )
        SELECT A.ACCOUNT_ID,
               A.ACCOUNT_NO,                 
               A.BALANCE,                     -- текущая задолженность
               A.BALANCE + BL.TOTAL AS OLD_DUE,-- просроченная задолженность
               O.ORDER_NO,                    -- номер заказа
               OP.PHONE_NUMBER                -- номер телефона  
          FROM ACCOUNT_T A, BL, ORDER_T O, ORDER_PHONES_T OP
         WHERE A.ACCOUNT_ID = BL.ACCOUNT_ID
           AND A.BALANCE < p_debt_min         -- если текущий баланс не попадает под санкции, то дальше и проверять нечего
           AND (A.BALANCE + BL.TOTAL) < p_debt_min
           AND A.ACCOUNT_ID = O.ACCOUNT_ID
           AND O.DATE_FROM < SYSDATE 
           AND (O.DATE_TO IS NULL OR SYSDATE < O.DATE_TO)
           AND O.STATUS = Pk00_Const.c_ORDER_STATE_OPEN
           AND OP.ORDER_ID = O.ORDER_ID
           AND SYSDATE BETWEEN OP.DATE_FROM AND OP.DATE_TO
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
-- Список должников ФИЗИЧЕСКИХ лиц, для разблокировки
--   - при ошибке выставляет исключение
PROCEDURE Subs_debitors_for_unlock( 
               p_recordset OUT t_refc,
               p_debt_min  IN NUMBER   -- порог дебиторской задолженности
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Cust_debitors_for_lock';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT A.ACCOUNT_ID,
               A.ACCOUNT_NO,                 
               A.BALANCE,                     -- текущая задолженность
               A.BALANCE + B.TOTAL AS OLD_DUE,-- просроченная задолженность
               O.ORDER_NO,                    -- номер заказа
               OP.PHONE_NUMBER                -- номер телефона  
          FROM ACCOUNT_T A, (
            SELECT B.ACCOUNT_ID, 
                   SUM(B.GROSS + B.TAX) TOTAL -- сумма начислений с налогами, не входящая   
              FROM BILL_T B                   -- в просроченную задолженность
             WHERE B.BILL_STATUS = Pk00_Const.c_BILL_STATE_CLOSED -- счет выставлен
               AND B.PAID_TO > SYSDATE        -- не просроченная задолженность
            GROUP BY B.ACCOUNT_ID
          ) B, 
          ORDER_T O, ORDER_PHONES_T OP
         WHERE A.ACCOUNT_ID = B.ACCOUNT_ID
           AND A.BALANCE > p_debt_min         -- если текущий баланс позволяет снять блокировку, то дальше и проверять нечего
           AND (A.BALANCE + B.TOTAL) > p_debt_min
           AND A.ACCOUNT_ID = O.ACCOUNT_ID
           AND O.DATE_FROM < SYSDATE 
           AND (O.DATE_TO IS NULL OR SYSDATE < O.DATE_TO)
           AND O.STATUS = Pk00_Const.c_ORDER_STATE_OPEN
           AND OP.ORDER_ID = O.ORDER_ID
           AND SYSDATE BETWEEN OP.DATE_FROM AND OP.DATE_TO
         ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- Платежи ЮЛ за период
PROCEDURE DZ_JL_PAYMENTS(p_recordset OUT t_refc, 
                         p_start_pay_date IN DATE, 
                         p_end_pay_date IN DATE)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DZ_JL_PAYMENTS';
    v_retcode        INTEGER;
    v_period_id_from INTEGER := PK04_PERIOD.Period_id(p_start_pay_date);
    v_period_id_to   INTEGER := PK04_PERIOD.Period_id(p_end_pay_date);  
BEGIN
   OPEN p_recordset FOR
      SELECT CST.SHORT_NAME  as NAME, 
             A.ACCOUNT_NO, 
             B.BILL_NO, 
             ROUND(P.TRANSFERED,2) as SUM_RAZ , 
             ROUND(P.RECVD,2) as SUM_PAY, 
             TRUNC(P.PAYMENT_DATE) as PAY_DATE, 
             TO_DATE('01.01.1970','dd.mm.yyyy') as VYP_DATE, -- ??????????????????????
             CT.SHORT_NAME as AGENT,
             P.PAYSYSTEM_CODE as BANK_CODE
        FROM PAYMENT_T P, PAY_TRANSFER_T PT, BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CUSTOMER_T CST, CONTRACTOR_T CT
       WHERE P.REP_PERIOD_ID BETWEEN v_period_id_from AND v_period_id_to
         AND P.PAYMENT_DATE  BETWEEN p_start_pay_date AND p_end_pay_date
         AND PT.PAYMENT_ID = P.PAYMENT_ID
         AND PT.BILL_ID = B.BILL_ID
         AND A.ACCOUNT_ID = B.ACCOUNT_ID
         AND A.ACCOUNT_TYPE = PK00_CONST.c_ACC_TYPE_J
         AND A.ACCOUNT_ID = AP.ACCOUNT_ID
         AND AP.CUSTOMER_ID = CST.CUSTOMER_ID
         AND AP.AGENT_ID = CT.CONTRACTOR_ID
       ORDER BY PAY_DATE;
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- Платежи Физ.лиц за период
PROCEDURE DZ_FL_PAYMENTS(p_recordset OUT t_refc, 
                         p_start_pay_date IN DATE, 
                         p_end_pay_date IN DATE)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DZ_FL_PAYMENTS';
    v_retcode        INTEGER;  
    v_period_id_from INTEGER := PK04_PERIOD.Period_id(p_start_pay_date);
    v_period_id_to   INTEGER := PK04_PERIOD.Period_id(p_end_pay_date);
BEGIN
   OPEN p_recordset FOR
      SELECT CST.LAST_NAME || ' ' || CST.FIRST_NAME || ' ' || CST.MIDDLE_NAME  as NAME, 
            OP.PHONE_NUMBER as PHONE,
            A.ACCOUNT_NO, 
            P.TRANSFERED as SUM_RAZ , 
            P.RECVD as SUM_PAY, 
            TRUNC(P.PAYMENT_DATE) as PAY_DATE, 
            TO_DATE('01.01.1970','dd.mm.yyyy') as VYP_DATE, -- ??????????????????????
            CT.SHORT_NAME as AGENT,
            P.PAYSYSTEM_CODE as BANK_CODE
        FROM PAYMENT_T P, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, SUBSCRIBER_T CST, 
             CONTRACTOR_T CT, ORDER_T O, ORDER_PHONES_T OP
       WHERE P.REP_PERIOD_ID BETWEEN v_period_id_from AND v_period_id_to
         AND P.PAYMENT_DATE BETWEEN p_start_pay_date AND p_end_pay_date
         AND A.ACCOUNT_ID = P.ACCOUNT_ID
         AND A.ACCOUNT_TYPE = PK00_CONST.c_ACC_TYPE_P
         AND A.ACCOUNT_ID = AP.ACCOUNT_ID
         AND AP.SUBSCRIBER_ID = CST.SUBSCRIBER_ID
         AND AP.AGENT_ID = CT.CONTRACTOR_ID
         AND O.ACCOUNT_ID = A.ACCOUNT_ID
         AND OP.ORDER_ID = O.ORDER_ID
         AND O.DATE_TO > SYSDATE
         AND OP.DATE_TO > SYSDATE
         ORDER BY PAY_DATE, NAME;             
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
-- Отчет по неразнесенным сумам платежей ЮЛ
PROCEDURE DZ_JL_UNKNOWN_PAYS(p_recordset OUT t_refc)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DZ_JL_UNKNOWN_PAYS';
    v_retcode    INTEGER;  
BEGIN
   OPEN p_recordset FOR
      SELECT P.RECVD      as SUM_PAY, 
             P.TRANSFERED as SUM_MAPPED, 
             P.BALANCE    as SUM_LEFT, 
             P.DOC_ID     as PAY_ID, 
             A.ACCOUNT_NO
        FROM PAYMENT_T P, ACCOUNT_T A
       WHERE P.ACCOUNT_ID = A.ACCOUNT_ID
         AND P.BALANCE <> 0
         AND A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_J
       ORDER BY A.ACCOUNT_NO;
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

---------------------------------------------------------------------  
-- создать новую запись об обработанном файле
---------------------------------------------------------------------
FUNCTION Process_new_call_file (
               p_file_name     IN VARCHAR2,
               p_call_type	   IN VARCHAR2,  
               p_client_type   IN CHAR, 
               p_start_process   IN DATE,
               p_end_process     IN DATE,
               p_file_date       IN DATE 
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Process_new_call_file';
    v_file_id    INTEGER;
BEGIN
    
    v_file_id := Next_deb_file_id;
   
    INSERT INTO EXT02_DEB_CALL_FILES (
       FILE_ID,
       FILE_NAME,
       CALL_TYPE,
       CLIENT_TYPE,
       START_PROCESS,
       END_PROCESS,
       FILE_DATE
    )VALUES(
       v_file_id,
       p_file_name,
       p_call_type,
       p_client_type,
       p_start_process,
       p_end_process,
       p_file_date
    );  
    RETURN v_file_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-------------------------------------------------------
-- Обновляем время окончания процесса обработки файла
-------------------------------------------------------
PROCEDURE ChangeFileEndProcess(p_file_id IN INTEGER, p_end_process IN DATE)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'ChangeFileEndProcess';
    v_retcode       INTEGER;   
BEGIN
    UPDATE EXT02_DEB_CALL_FILES
    SET END_PROCESS = p_end_process
    WHERE FILE_ID = p_file_id;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-----------------------------------------------------------------
-- Пишем историю обзвонов
-----------------------------------------------------------------
PROCEDURE WriteCallHistory( p_file_id IN INTEGER, 
                            p_account_id IN INTEGER, 
                            p_phone IN VARCHAR2, 
                            p_call_date IN DATE, 
                            p_result_info IN VARCHAR2, 
                            p_result IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'WriteCallHistory';
    v_retcode       INTEGER;   
BEGIN
   INSERT INTO EXT02_DEB_CALL_HISTORY(FILE_ID, ACCOUNT_ID, PHONE, CALL_DATE, RESULT_INFO, RESULT)
                             VALUES(p_file_id, p_account_id, p_phone, p_call_date, p_result_info, p_result);
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- Детализированный отчет об информировании по ДЗ (ФЛ)
PROCEDURE DZ_FL_DETAIL_INFO(p_recordset OUT t_refc)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DZ_FL_DETAIL_INFO';
    v_retcode    INTEGER;  
    v_max_file_id INTEGER;
BEGIN
  SELECT MAX(FILE_ID) INTO v_max_file_id FROM EXT02_DEB_CALL_FILES WHERE CALL_TYPE = 'Inform' AND CLIENT_TYPE = 'P';
   OPEN p_recordset FOR
        SELECT H.PHONE,
               (   S.LAST_NAME
                || ' '
                || SUBSTR (S.FIRST_NAME, 1, 1)
                || '.'
                || SUBSTR (S.MIDDLE_NAME, 1, 1)
                || '.')
                  AS NAME,
               AG.SHORT_NAME AGENT,
               BR.SHORT_NAME BRAND,
               DECODE (MAX (h.RESULT), 1, 'Успешно', 'Неуспешно')
                  AS INFORM_RESULT,
               MAX (h.CALL_DATE) INFORM_LAST_DATE,
               A.BALANCE SUM_FORM,
               CEIL (MONTHS_BETWEEN (SYSDATE, B.DEB_BILL_DATE)) PERIOD_COUNT
          FROM EXT02_DEB_CALL_HISTORY H,
               ACCOUNT_T A,
               account_profile_t ap,
               subscriber_t s,
               CONTRACTOR_T BR,
               CONTRACTOR_T AG,
               (  SELECT B.ACCOUNT_ID, MIN (B.BILL_DATE) DEB_BILL_DATE
                    FROM BILL_T B
                   WHERE B.DUE < 0
                GROUP BY B.ACCOUNT_ID) B
         WHERE     AP.ACCOUNT_ID = h.ACCOUNT_ID
               AND h.ACCOUNT_ID = A.ACCOUNT_ID
               AND S.SUBSCRIBER_ID = AP.SUBSCRIBER_ID
               AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
               AND AP.AGENT_ID = AG.CONTRACTOR_ID(+)
               AND H.ACCOUNT_ID = B.ACCOUNT_ID(+)
               AND H.FILE_ID = v_max_file_id
      GROUP BY h.ACCOUNT_ID,
               (   S.LAST_NAME
                || ' '
                || SUBSTR (S.FIRST_NAME, 1, 1)
                || '.'
                || SUBSTR (S.MIDDLE_NAME, 1, 1)
                || '.'),
               BR.SHORT_NAME,
               AG.SHORT_NAME,
               H.PHONE,
               A.BALANCE,
               CEIL (MONTHS_BETWEEN (SYSDATE, B.DEB_BILL_DATE));     
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- Агрегированный отчет о покрытии платежами выставленных счетов за месяц
PROCEDURE Agr_bill_payments(p_recordset OUT t_refc, p_rep_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Agr_bill_payments';
    v_retcode    INTEGER;  
BEGIN
  OPEN p_recordset FOR

  SELECT BR.SHORT_NAME BRAND,
         AG.SHORT_NAME AGENT,         
         A.ACCOUNT_NO,
         SUM (b.total) SUM_BILL,
         SUM (PT.TRANSFER_TOTAL) SUM_RAZ,
         SUM (b.total) - SUM (PT.TRANSFER_TOTAL) SUM_DEB
    FROM bill_t b,
         account_t a,
         ACCOUNT_PROFILE_T AP,
         pay_transfer_t pt,
         CONTRACTOR_T BR,
         CONTRACTOR_T AG
   WHERE     B.REP_PERIOD_ID = p_rep_period_id
         AND B.ACCOUNT_ID = A.ACCOUNT_ID
         AND AP.ACCOUNT_ID = A.ACCOUNT_ID
         AND B.BILL_ID = PT.BILL_ID
         AND A.ACCOUNT_TYPE = 'P'
         AND B.TOTAL > 0
         AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
         AND AP.AGENT_ID = AG.CONTRACTOR_ID(+)
GROUP BY A.ACCOUNT_NO, BR.SHORT_NAME, AG.SHORT_NAME
ORDER BY BRAND, ACCOUNT_NO;    
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK400_DEBITORS;
/
