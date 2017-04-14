CREATE OR REPLACE PACKAGE PK400_DEBITORS_BAK
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
    -- Развернутый отчет по должникам ФИЗИКАМ НА КОНКРЕТНЫЙ ОТЧ. ПЕРИОД
    --   - при ошибке выставляет исключение
    PROCEDURE Subs_detail_report_period( 
                   p_recordset OUT t_refc,
                   p_rep_period_id IN INTEGER
               );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Развернутый отчет по должникам ЮРИКАМ
    --   - при ошибке выставляет исключение
    PROCEDURE Cust_detail_report( 
                   p_recordset OUT t_refc
               );
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Развернутый отчет по должникам ЮРИКАМ СГРУППИРОВАННЫЙ !!
--   - при ошибке выставляет исключение
PROCEDURE Cust_detail_report_group( 
               p_recordset OUT t_refc
           );               
PROCEDURE Cust_detail_report_group_test( 
               p_recordset OUT t_refc
           );                
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Развернутый отчет по должникам ЮРИКАМ НА КОНКРЕТНЫЙ ОТЧ. ПЕРИОД
--   - при ошибке выставляет исключение
PROCEDURE Cust_detail_report_period( 
               p_recordset OUT t_refc,
               p_rep_period_id IN INTEGER
           );               

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Развернутый отчет по должникам ЮРИКАМ НА КОНКРЕТНЫЙ ОТЧ. ПЕРИОД СГРУППИРОВАННЫЙ !!
--   - при ошибке выставляет исключение
PROCEDURE Cust_det_rep_period_group( 
               p_recordset OUT t_refc,
               p_rep_period_id IN INTEGER
           );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Список должников ФИЗИЧЕСКИХ лиц, для обзвона
    --   - при ошибке выставляет исключение
PROCEDURE Subs_debitors_for_call( 
               p_recordset OUT t_refc
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
                            p_result IN INTEGER,
                            p_sum_debt IN NUMBER);
-- Детализированный отчет об информировании по ДЗ (ФЛ)
PROCEDURE DZ_FL_DETAIL_INFO(p_recordset OUT t_refc, p_rep_reriod_id IN INTEGER); 
-- Детализированный отчет об информировании (блокировки) по ДЗ (ФЛ)
PROCEDURE DZ_FL_DETAIL_INFO_BLOCK(p_recordset OUT t_refc); 
-- Отчет о блокировках за период
PROCEDURE DZ_FL_DETAIL_BLOCK_BY_PERIOD(p_recordset OUT t_refc, p_start_date IN DATE, p_end_date IN DATE);                          
-- Агрегированный отчет о покрытии платежами выставленных счетов за месяц (ФЛ)
PROCEDURE Agr_bill_payments(p_recordset OUT t_refc, p_rep_period_id IN INTEGER); 
-- Агрегированный отчет о покрытии платежами выставленных счетов за месяц (ЮЛ)
PROCEDURE Agr_bill_payments_jl(p_recordset OUT t_refc, p_rep_period_id IN INTEGER);
-----------------------------------------------------
-- Получить список Клиентов по префиксу
-----------------------------------------------------
PROCEDURE GetClientsByPrefix(p_recordset OUT t_refc, p_prefix IN VARCHAR2, p_contractor_list IN VARCHAR2);
-- Акт сверки
PROCEDURE Get_Akt(p_recordset OUT t_refc,
                  p_start_bill_date IN DATE, 
                  p_end_bill_date IN DATE,
                  p_contract_no IN VARCHAR2);
-- для ЛС
PROCEDURE Get_Akt_account(p_recordset OUT t_refc,
                  p_start_bill_date IN DATE, 
                  p_end_bill_date IN DATE,
                  p_account_no IN VARCHAR2);
------------------------------------------------
-- проверить, доступен ли Договор пользователю
------------------------------------------------
FUNCTION Check_Contract_to_view(p_contractor_list IN VARCHAR2, p_contract_no IN VARCHAR2) RETURN  INTEGER;   
---------------------------------------------------------------------------------
-- проверить, доступен ли ЛС  пользователю (для формирования акта сверки по ЛС)
---------------------------------------------------------------------------------
FUNCTION Check_Account_to_view(p_contractor_list IN VARCHAR2, p_account_no IN VARCHAR2) RETURN INTEGER;    
------------------------------------------------
-- откуда формировать акт сверки? 
-- 1 или больше - BRM 
-- 0 - 1C 
------------------------------------------------
FUNCTION Check_where_act(p_contract_no IN VARCHAR2) RETURN INTEGER;           
--------------------------------------------
-- для ЛС!!
FUNCTION Check_where_act_account(p_account_no IN VARCHAR2) RETURN INTEGER;
-- Акт сверки взаиморасчетов НОВЫЙ !!!! UPD неправильный (((
--------------------------------------------
/*PROCEDURE Get_Akt_new(p_recordset OUT t_refc,
                  p_start_bill_date IN DATE, 
                  p_end_bill_date IN DATE,
                  p_contract_no IN VARCHAR2);    */
---------------------------------------------
-- Получить входящее сальдо (для акта сверки)
---------------------------------------------
FUNCTION GetIncomeBalance (p_start_bill_date IN DATE, p_contract_no VARCHAR2) RETURN NUMBER;
---------------------------------------------
-- Получить входящее сальдо по ЛС (для акта сверки)
---------------------------------------------
FUNCTION GetIncomeBalanceAcc (p_start_bill_date IN DATE, p_account_no VARCHAR2) RETURN NUMBER;                                
--------------------------------------------
-- Выписка по тел. номеру ФЛ
--------------------------------------------
PROCEDURE Get_Akt_FL(p_recordset OUT t_refc,
                  p_start_bill_date IN DATE, 
                  p_end_bill_date IN DATE,
                  p_phone IN VARCHAR2) ;    
                  
-----------------------------------------------------------------------
-- Получить наименование Клиента по номеру Договора (для акта сверки)
-------------------------------------------------------------------------
FUNCTION Get_Customer_By_Contract(p_conract_no IN VARCHAR2) RETURN VARCHAR2;
-----------------------------------------------------------------------
-- Получить наименование Клиента по номеру ЛС (для акта сверки)
-------------------------------------------------------------------------
FUNCTION Get_Customer_By_Account(p_account_no IN VARCHAR2) RETURN VARCHAR2;       
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Список должников ФИЗИЧЕСКИХ лиц, для блокировки автоматом (25 числа каждого месяца)
--   - при ошибке выставляет исключение
PROCEDURE Subs_debitors_for_lock_auto( 
               p_recordset      OUT t_refc,
               p_rep_reriod_id  IN NUMBER   -- период
           );
---------------------------------------------------------------------
-- Дебиторка более 3-х лет (Отчет Списание дебиторской задолженности)
---------------------------------------------------------------------
PROCEDURE Get_DZ_JL_36(p_recordset OUT t_refc);   

-- ----------------------------------------------------------------------- --
-- 'Оборотно сальдовая ведомость'
-- Клиент    
-- Лицевой счет    
-- Начальное сальдо (дебет)    
-- Начальное сальдо (кредит)    
-- Оборот (дебет)    
-- Оборот (кредит)    
-- Конечное сальдо (дебет)    
-- Конечное сальдо (кредит)
-- ----------------------------------------------------------------------- --
PROCEDURE DZ_JL_OBOROTY(
               p_recordset OUT t_refc,
               p_period_id IN INTEGER,
               p_branch_id IN INTEGER,
               p_agent_id  IN INTEGER  DEFAULT NULL
           );

                    
END PK400_DEBITORS_BAK;
/
CREATE OR REPLACE PACKAGE BODY PK400_DEBITORS_BAK
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
            AND O.STATUS <> 'LOCK'
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
                     BR.CONTRACTOR BRAND_NAME,     -- бренд
                     AG.CONTRACTOR AGENT_NAME,      -- агент
                     DECODE(C.CLIENT_TYPE_ID, 6409, 'VIP', 'Физические лица') VIP -- VIP клиент
                FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, 
                     CONTRACT_T C, SUBSCRIBER_T S,
                     CONTRACTOR_T BR, CONTRACTOR_T AG,
                     ACCOUNT_CONTACT_T AA
               WHERE A.ACCOUNT_TYPE = PK00_CONST.C_ACC_TYPE_P
                 AND A.BILLING_ID <> 2007
                 AND A.ACCOUNT_ID = AP.ACCOUNT_ID
                 AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
                 AND AP.CONTRACT_ID = C.CONTRACT_ID
                 AND AP.SUBSCRIBER_ID = S.SUBSCRIBER_ID
                 AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
                 AND NVL(AP.AGENT_ID, AP.BRANCH_ID)  = AG.CONTRACTOR_ID(+)
                 AND A.ACCOUNT_ID = AA.ACCOUNT_ID
                 AND AA.ADDRESS_TYPE = NVL('DLV', 'REG')
                 --AND AA.ADDRESS_TYPE = Pk00_Const.c_ADDR_TYPE_REG -- 'REG'
                 AND A.STATUS <> 'T'
          ),
          BL AS (
              SELECT B.ACCOUNT_ID, MIN(B.BILL_DATE) DEB_BILL_DATE
                FROM BILL_T B 
               WHERE B.DUE < 0 
              GROUP BY B.ACCOUNT_ID
          ),
          B_CUR
          AS (SELECT B.DUE, B.ACCOUNT_ID
              FROM BILL_T B
                 WHERE B.REP_PERIOD_ID =  to_number(to_char(ADD_MONTHS(sysdate,-1), 'YYYY') || to_char(ADD_MONTHS(sysdate,-1), 'mm'))
                 ),           
          LK AS (
              SELECT O.ACCOUNT_ID, 
                     SUM(
                        CASE WHEN (select COUNT(ORDER_ID) from order_lock_t where LOCK_type_ID <> 901 and order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED') is null THEN 0
                        ELSE (select COUNT(ORDER_ID) from order_lock_t where LOCK_type_ID <> 901 and order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED') END
                     --DECODE(O.STATUS, 'LOCK',1,0)
                     ) LOCKS_NUM,
                     SUM(DECODE(O.STATUS, 'CLOSED',1,0)) CLOSED_NUM,
                     COUNT(*) ORDERS_NUM
                FROM ORDER_T O
              GROUP BY O.ACCOUNT_ID
          )
          SELECT ABN.ACCOUNT_ID,
                 ABN.BALANCE - decode(B_CUR.DUE, NULL, 0, B_CUR.DUE) BALANCE,
                 ABN.ACCOUNT_NO,
                 --ABN.ACCOUNT_STATUS,
                 ABN.CONTRACT_NO,
                 ABN.SUBS_NAME,   -- Ф.И.О.
                 decode(ABN.CITY, null, '-', ABN.CITY) CITY,
                 decode(ABN.ADDRESS, null, '-', ABN.ADDRESS) ADDRESS,
                 decode(ABN.PHONES, null, '-',ABN.PHONES) PHONES,
                 decode(ABN.BRAND_NAME, null, '-', ABN.BRAND_NAME) BRAND_NAME,
                 decode(ABN.AGENT_NAME, null, '-', ABN.AGENT_NAME) AGENT_NAME,                
                 BL.DEB_BILL_DATE,
                 --B_CUR.DUE CUR_DUE,
                 decode(B_CUR.DUE, NULL, 0, B_CUR.DUE) CUR_DUE,
                 ROUND (MONTHS_BETWEEN (PK04_PERIOD.Period_end_date(ADD_MONTHS(sysdate,-1)), BL.DEB_BILL_DATE)) DBT_MONTH,
                 CASE
                     WHEN LK.LOCKS_NUM = 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) < LK.ORDERS_NUM THEN 'Активный'
                     WHEN LK.LOCKS_NUM > 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) = LK.ORDERS_NUM THEN 'Заблокирован'
                     WHEN LK.LOCKS_NUM > 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) < LK.ORDERS_NUM THEN 'Частично заблокирован'
                     --WHEN LK.CLOSED_NUM > 0 AND LK.LOCKS_NUM = 0 THEN 'Закрыт'
                     WHEN LK.CLOSED_NUM = LK.ORDERS_NUM THEN 'Закрыт'
                 END ACCOUNT_STATUS,
                 ABN.VIP
            FROM ABN, LK, BL, B_CUR  
           WHERE ABN.ACCOUNT_ID = BL.ACCOUNT_ID 
             AND ABN.ACCOUNT_ID = LK.ACCOUNT_ID
             AND ABN.ACCOUNT_ID = B_CUR.ACCOUNT_ID(+)
             --AND ABN.BALANCE - decode(B_CUR.DUE, NULL, 0, B_CUR.DUE) < 0
             --AND LK.LOCKS_NUM <> ORDERS_NUM      -- у кого не все заказы заблокированы
             --AND AGENT_NAME <> 'Восточно-Сибирская ЖД'
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
-- Развернутый отчет по должникам ФИЗИКАМ НА КОНКРЕТНЫЙ ОТЧ. ПЕРИОД
--   - при ошибке выставляет исключение
PROCEDURE Subs_detail_report_period( 
               p_recordset OUT t_refc,
               p_rep_period_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Subs_detail_report';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
      WITH ABN
           AS (SELECT A.ACCOUNT_ID, 
                      A.BALANCE CUR_BALANCE,
                      RP.CLOSE_BALANCE BALANCE,
                      A.ACCOUNT_NO,
                      DECODE (A.STATUS,
                              'B', 'Активный',
                              'C', 'Неактивный')
                         ACCOUNT_STATUS,
                      C.CONTRACT_NO,
                         INITCAP (S.LAST_NAME)
                      || ' '
                      || INITCAP (S.FIRST_NAME)
                      || ' '
                      || INITCAP (S.MIDDLE_NAME)
                         SUBS_NAME,                                        -- Ф.И.О.
                      AA.CITY,
                      AA.ADDRESS,
                      AA.PHONES,
                      BR.CONTRACTOR BRAND_NAME,                             -- бренд
                      AG.CONTRACTOR AGENT_NAME,                              -- агент
                      DECODE(C.CLIENT_TYPE_ID, 6409, 'VIP', 'Физические лица') VIP -- VIP клиент
                 FROM ACCOUNT_T A,
                      ACCOUNT_PROFILE_T AP,
                      CONTRACT_T C,
                      SUBSCRIBER_T S,
                      CONTRACTOR_T BR,
                      CONTRACTOR_T AG,
                      ACCOUNT_CONTACT_T AA,
                      REP_PERIOD_INFO_T RP
                WHERE     A.ACCOUNT_TYPE = 'P'
                      AND A.BILLING_ID <> 2007
                      AND A.ACCOUNT_ID = AP.ACCOUNT_ID
                      AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
                      AND AP.CONTRACT_ID = C.CONTRACT_ID
                      AND AP.SUBSCRIBER_ID = S.SUBSCRIBER_ID
                      AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
                      AND NVL(AP.AGENT_ID, AP.BRANCH_ID) = AG.CONTRACTOR_ID(+)
                      AND A.ACCOUNT_ID = AA.ACCOUNT_ID
                      AND AA.ADDRESS_TYPE = NVL('DLV', 'REG')
                      AND RP.ACCOUNT_ID = A.ACCOUNT_ID
                      AND RP.REP_PERIOD_ID = 
                      (select max(REP_PERIOD_ID) FROM REP_PERIOD_INFO_T WHERE REP_PERIOD_ID <= p_rep_period_id AND ACCOUNT_ID = A.ACCOUNT_ID)
                      AND A.STATUS <> 'T'
                      ),
           BL
           AS (  SELECT B.ACCOUNT_ID, MIN (B.BILL_DATE) DEB_BILL_DATE
                   FROM BILL_T B
                  WHERE B.DUE < 0
               GROUP BY B.ACCOUNT_ID ),
           B_CUR
           AS (SELECT B.DUE, B.ACCOUNT_ID
                 FROM BILL_T B
                WHERE B.REP_PERIOD_ID =
                         TO_NUMBER (
                               TO_CHAR (ADD_MONTHS (SYSDATE, -1), 'YYYY')
                            || TO_CHAR (ADD_MONTHS (SYSDATE, -1), 'mm'))),
           LK
           AS (  SELECT O.ACCOUNT_ID,
                     SUM(
                        CASE WHEN (select COUNT(ORDER_ID) from order_lock_t where LOCK_type_ID <> 901 and order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED') is  null THEN 0
                        ELSE (select COUNT(ORDER_ID) from order_lock_t where LOCK_type_ID <> 901 and order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED') END
                     --DECODE(O.STATUS, 'LOCK',1,0)
                     ) LOCKS_NUM,
                        SUM(DECODE(O.STATUS, 'CLOSED',1,0)) CLOSED_NUM,
                        COUNT (*) ORDERS_NUM
                   FROM ORDER_T O
               GROUP BY O.ACCOUNT_ID)
        SELECT ABN.ACCOUNT_ID,
               ABN.BALANCE,
               ABN.CUR_BALANCE - decode(B_CUR.DUE, null,0, B_CUR.DUE) CUR_BALANCE,
               ABN.ACCOUNT_NO,
               --ABN.ACCOUNT_STATUS,
               ABN.CONTRACT_NO,
               ABN.SUBS_NAME,                                              -- Ф.И.О.
                 decode(ABN.CITY, null, '-', ABN.CITY) CITY,
                 decode(ABN.ADDRESS, null, '-', ABN.ADDRESS) ADDRESS,
                 decode(ABN.PHONES, null, '-',ABN.PHONES) PHONES,
                 decode(ABN.BRAND_NAME, null, '-', ABN.BRAND_NAME) BRAND_NAME,
                 decode(ABN.AGENT_NAME, null, '-', ABN.AGENT_NAME) AGENT_NAME,
               BL.DEB_BILL_DATE,
               decode(B_CUR.DUE,null,0, B_CUR.DUE) CUR_DUE,
               case 
               when ADD_MONTHS(PK04_PERIOD.Period_to(p_rep_period_id),-1) >  BL.DEB_BILL_DATE then
               ROUND (MONTHS_BETWEEN (ADD_MONTHS(PK04_PERIOD.Period_to(p_rep_period_id),-1), BL.DEB_BILL_DATE)) 
               else 0 end DBT_MONTH,
               --ROUND (MONTHS_BETWEEN (ADD_MONTHS(PK04_PERIOD.Period_to(p_rep_period_id),-1), BL.DEB_BILL_DATE)) DBT_MONTH,
                 CASE
                     WHEN LK.LOCKS_NUM = 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) < LK.ORDERS_NUM THEN 'Активный'
                     WHEN LK.LOCKS_NUM > 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) = LK.ORDERS_NUM THEN 'Заблокирован'
                     WHEN LK.LOCKS_NUM > 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) < LK.ORDERS_NUM THEN 'Частично заблокирован'
                     WHEN LK.CLOSED_NUM = LK.ORDERS_NUM THEN 'Закрыт'
                     --WHEN LK.CLOSED_NUM > 0 AND LK.LOCKS_NUM = 0 THEN 'Закрыт'
                 END ACCOUNT_STATUS,
                  ABN.VIP
          FROM ABN, LK, BL, B_CUR                                                     -- PI
         WHERE     ABN.ACCOUNT_ID = BL.ACCOUNT_ID
               AND ABN.ACCOUNT_ID = LK.ACCOUNT_ID
               AND ABN.ACCOUNT_ID = B_CUR.ACCOUNT_ID(+)
               --AND ABN.BALANCE + (-1) * decode(B_CUR.DUE, null,0, B_CUR.DUE) < 0
      ORDER BY MONTHS_BETWEEN (SYSDATE, BL.DEB_BILL_DATE) DESC, ABN.BALANCE    
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
                     C.CONTRACT_ID, ------------------------------------ <<<<
                     CS.CUSTOMER,                  -- компания, ИНН, КПП 
                     CS.INN, 
                     CS.KPP,  
                     AA.CITY,
                     AA.ADDRESS,
                     AA.PHONES,
                     BR.CONTRACTOR BRAND_NAME,     -- бренд
                     AG.CONTRACTOR AGENT_NAME,      -- агент
                     DECODE(C.CLIENT_TYPE_ID, 6409, 'VIP', 'Юридические лица') VIP, -- VIP клиент
                     AP.BRANCH_ID, AP.AGENT_ID
                FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, 
                     CONTRACT_T C, CUSTOMER_T CS,
                     CONTRACTOR_T BR, CONTRACTOR_T AG,
                     ACCOUNT_CONTACT_T AA
               WHERE A.ACCOUNT_TYPE = 'J'
                 AND A.ACCOUNT_ID = AP.ACCOUNT_ID
                 --AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
                 AND AP.CONTRACT_ID = C.CONTRACT_ID
                 AND (AP.DATE_TO is null or AP.DATE_TO > SYSDATE)
                 AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
                 AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
                 AND NVL(AP.AGENT_ID, AP.BRANCH_ID)  = AG.CONTRACTOR_ID(+)
                 AND A.ACCOUNT_ID = AA.ACCOUNT_ID
                 AND AA.ADDRESS_TYPE = 'JUR'
                 AND A.STATUS <> 'T'
                 --AND A.Billing_Id <> PK00_CONST.c_BILLING_OLD --2002
                 AND A.Billing_Id = PK00_CONST.c_BILLING_MMTS
          ),
          BL AS (
              SELECT B.ACCOUNT_ID, MIN(B.BILL_DATE) DEB_BILL_DATE
                FROM BILL_T B 
               WHERE B.DUE < 0 
              GROUP BY B.ACCOUNT_ID
          ),
          B_CUR
          AS (SELECT sum(B.DUE) DUE, B.ACCOUNT_ID
              FROM BILL_T B
               WHERE B.REP_PERIOD_ID = TO_NUMBER (TO_CHAR (ADD_MONTHS (SYSDATE, 
               (SELECT (days_for_payment/30) *(-1) from billinfo_t where account_id = b.account_id )
--               -1
               ), 'YYYYmm')) 
               GROUP BY B.ACCOUNT_ID         
                 ),                         
          LK AS (
              SELECT O.ACCOUNT_ID, 
                     SUM(
                        CASE WHEN (select COUNT(ORDER_ID) from order_lock_t where LOCK_type_ID <> 901 and order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED') is null THEN 0
                        ELSE (select COUNT(ORDER_ID) from order_lock_t where LOCK_type_ID <> 901 and order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED') END
                     --DECODE(O.STATUS, 'LOCK',1,0)
                     ) LOCKS_NUM,
                     SUM(DECODE(O.STATUS, 'CLOSED',1,0)) CLOSED_NUM,
                     COUNT(*) ORDERS_NUM
                FROM ORDER_T O
              GROUP BY O.ACCOUNT_ID
          )
          SELECT ABN.ACCOUNT_ID,
                 ABN.BALANCE - decode(B_CUR.DUE, NULL, 0, B_CUR.DUE) BALANCE,
                 ABN.ACCOUNT_NO,
                 ABN.CONTRACT_NO,
                 decode(ABN.CUSTOMER,  null, '-', ABN.CUSTOMER) CUSTOMER,
                 decode(ABN.INN,  null, '-', ABN.INN) INN,
                 decode(ABN.KPP, null, '-', ABN.KPP) KPP,
                 decode(ABN.CITY, null, '-', ABN.CITY) CITY,
                 decode(ABN.ADDRESS, null, '-', ABN.ADDRESS) ADDRESS, 
                 decode(ABN.PHONES, null, '-', ABN.PHONES) PHONES,
                 decode(ABN.BRAND_NAME, null, '-',ABN.BRAND_NAME) BRAND_NAME,
                 decode(ABN.AGENT_NAME, null, '-',ABN.AGENT_NAME) AGENT_NAME,
                 BL.DEB_BILL_DATE,
                 decode(B_CUR.DUE, NULL, 0, B_CUR.DUE) CUR_DUE,
                 TRUNC(SYSDATE - BL.DEB_BILL_DATE) DEB_DAYS, -- кол-во дней задолженности
                 ROUND (MONTHS_BETWEEN (PK04_PERIOD.Period_end_date(ADD_MONTHS(sysdate,-1)), BL.DEB_BILL_DATE)) DBT_MONTH, -- кол-во месяцев задолженности
                 DECODE(LK.LOCKS_NUM, null, 'Неактивный', 
                 CASE
                     WHEN LK.LOCKS_NUM = 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) < LK.ORDERS_NUM THEN 'Активный'
                     WHEN LK.LOCKS_NUM > 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) = LK.ORDERS_NUM THEN 'Заблокирован'
                     WHEN LK.LOCKS_NUM > 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) < LK.ORDERS_NUM THEN 'Частично заблокирован'
                     WHEN LK.CLOSED_NUM = LK.ORDERS_NUM THEN 'Закрыт' -- > 0 AND LK.LOCKS_NUM = 0 THEN 'Закрыт'
                 END) ACCOUNT_STATUS, --LOCKS,                                  -- 
                 --LK.ORDERS_NUM,                              -- кол-во заказов на л/с
                 --LK.LOCKS_NUM,                               -- кол-во заблокированных заказов на л/с
                 decode(PK400_DEBITORS.Get_AccSrv_list(ABN.ACCOUNT_ID), null, '-', PK400_DEBITORS.Get_AccSrv_list(ABN.ACCOUNT_ID))  SERVICES, -- услуги на л/с
                 ABN.VIP,
                 PK402_BCR_DATA.Get_sales_curator(ABN.BRANCH_ID, ABN.AGENT_ID, ABN.CONTRACT_ID, ABN.ACCOUNT_ID, NULL, NULL) SALE_NAME  ---------------------------------------------------------------------------------------------<<<
            FROM ABN, LK, BL, B_CUR
           WHERE ABN.ACCOUNT_ID = BL.ACCOUNT_ID (+)
             AND ABN.ACCOUNT_ID = LK.ACCOUNT_ID (+)
             AND ABN.ACCOUNT_ID = B_CUR.ACCOUNT_ID(+)
             --AND ABN.BALANCE  + (-1) * decode(B_CUR.DUE, NULL, 0, B_CUR.DUE)< 0       
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
-- Развернутый отчет по должникам ЮРИКАМ СГРУППИРОВАННЫЙ !!
--   - при ошибке выставляет исключение
PROCEDURE Cust_detail_report_group( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Cust_detail_report_group';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
   SELECT COUNT (account_id) account_no,
         SUM (balance) balance,
         contract_no,
         MAX (customer) customer,
         MAX (inn) inn,
         MAX (kpp) kpp,
         MAX (agent_name) agent_name,
         MAX (brand_name) brand_name,
         MAX (phones) phones,
         MAX (city) city,
         MAX (address) address,
         SUM (cur_due) cur_due,
         decode(MAX (DBT_MONTH), null, 0, MAX (DBT_MONTH)) DBT_MONTH,
         vip,
         DECODE (
            SUM (LOCKS_NUM),
            NULL, 'Неактивный',
            CASE
               WHEN     SUM (LOCKS_NUM) = 0
                    AND (SUM (LOCKS_NUM) + SUM (CLOSED_NUM)) < SUM (ORDERS_NUM)
               THEN
                  'Активный'
               WHEN     SUM (LOCKS_NUM) > 0
                    AND (SUM (LOCKS_NUM) + SUM (CLOSED_NUM)) = SUM (ORDERS_NUM)
               THEN
                  'Заблокирован'
               WHEN     SUM (LOCKS_NUM) > 0
                    AND (SUM (LOCKS_NUM) + SUM (CLOSED_NUM)) < SUM (ORDERS_NUM)
               THEN
                  'Частично заблокирован'
               WHEN SUM (CLOSED_NUM) = SUM (ORDERS_NUM)
               THEN
                  'Закрыт'
            END)
            ACCOUNT_STATUS,
            max(nvl(SAL_NAME, 'Не указан')) SALE_NAME---------------------------------------------------------<<<<<<
    FROM (WITH ABN
               AS (SELECT A.ACCOUNT_ID,
                          A.BALANCE,
                          A.ACCOUNT_NO,
                          DECODE (A.STATUS,
                                  'B', 'Активный',
                                  'C', 'Неактивный')
                             ACCOUNT_STATUS,
                          C.CONTRACT_NO,
                          C.CONTRACT_ID, ------------------------------------ <<<<
                          CS.CUSTOMER,                   -- компания, ИНН, КПП
                          CS.INN,
                          CS.KPP,
                          AA.CITY,
                          AA.ADDRESS,
                          AA.PHONES,
                          BR.CONTRACTOR BRAND_NAME,                   -- бренд
                          AG.CONTRACTOR AGENT_NAME,                   -- агент
                          DECODE (C.CLIENT_TYPE_ID,
                                  6409, 'VIP',
                                  'Юридические лица')
                             VIP,                                 -- VIP клиент
                             AP.AGENT_ID, AP.BRANCH_ID ---------------------------------<<<<<
                     FROM ACCOUNT_T A,
                          ACCOUNT_PROFILE_T AP,
                          CONTRACT_T C,
                          CUSTOMER_T CS,
                          CONTRACTOR_T BR,
                          CONTRACTOR_T AG,
                          ACCOUNT_CONTACT_T AA
                    WHERE     A.ACCOUNT_TYPE = 'J'
                          AND A.ACCOUNT_ID = AP.ACCOUNT_ID
                          --AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
                          AND AP.CONTRACT_ID = C.CONTRACT_ID
                          AND (AP.DATE_TO is null or AP.DATE_TO > SYSDATE)
                          AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
                          AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
                          AND NVL (AP.AGENT_ID, AP.BRANCH_ID) =
                                 AG.CONTRACTOR_ID(+)
                          AND A.ACCOUNT_ID = AA.ACCOUNT_ID
                          AND AA.ADDRESS_TYPE = 'JUR'
                          AND A.STATUS <> 'T' AND A.Billing_Id = PK00_CONST.c_BILLING_MMTS), -- <> PK00_CONST.c_BILLING_OLD),
               BL
               AS (  SELECT B.ACCOUNT_ID, MIN (B.BILL_DATE) DEB_BILL_DATE
                       FROM BILL_T B
                      WHERE B.DUE < 0
                   GROUP BY B.ACCOUNT_ID),
          B_CUR
          AS (SELECT sum(B.DUE) DUE, B.ACCOUNT_ID
              FROM BILL_T B
               WHERE B.REP_PERIOD_ID = TO_NUMBER (TO_CHAR (ADD_MONTHS (SYSDATE, 
               (SELECT (days_for_payment/30) *(-1) from billinfo_t where account_id = b.account_id )
--               -1
               ), 'YYYYmm')) 
               GROUP BY B.ACCOUNT_ID         
                 ),                     
               LK
               AS (  SELECT O.ACCOUNT_ID,
                            SUM (
                               CASE
                                  WHEN (SELECT COUNT (ORDER_ID)
                                          FROM order_lock_t
                                         WHERE     LOCK_type_ID <> 901
                                               AND order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED')
                                          IS NULL
                                  THEN
                                     0
                                  ELSE
                                     (SELECT COUNT (ORDER_ID)
                                          FROM order_lock_t
                                         WHERE     LOCK_type_ID <> 901
                                               AND order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED') 
                               END--DECODE(O.STATUS, 'LOCK',1,0)
                               )
                               LOCKS_NUM,
                            SUM (DECODE (O.STATUS, 'CLOSED', 1, 0)) CLOSED_NUM,
                            COUNT (*) ORDERS_NUM
                       FROM ORDER_T O
                   GROUP BY O.ACCOUNT_ID)
          SELECT ABN.ACCOUNT_ID,
                 ABN.BALANCE - DECODE (B_CUR.DUE, NULL, 0, B_CUR.DUE)
                    BALANCE,
                 ABN.ACCOUNT_NO,
                 --
                 ABN.CONTRACT_NO,
                 ABN.CUSTOMER,
                 ABN.INN,
                 ABN.KPP,
                 ABN.CITY,
                 ABN.ADDRESS,
                 ABN.PHONES,
                 DECODE (ABN.BRAND_NAME, NULL, '-', ABN.BRAND_NAME) BRAND_NAME,
                 DECODE (ABN.AGENT_NAME, NULL, '-', ABN.AGENT_NAME) AGENT_NAME,
                 BL.DEB_BILL_DATE,
                 DECODE (B_CUR.DUE, NULL, 0, B_CUR.DUE) CUR_DUE,
                 TRUNC (SYSDATE - BL.DEB_BILL_DATE) DEB_DAYS, -- кол-во дней задолженности
                 ROUND (
                    MONTHS_BETWEEN (
                       PK04_PERIOD.Period_end_date (ADD_MONTHS (SYSDATE, -1)),
                       BL.DEB_BILL_DATE))
                    DBT_MONTH,                 -- кол-во месяцев задолженности
                 DECODE (
                    LK.LOCKS_NUM,
                    NULL, 'Неактивный',
                    CASE
                       WHEN     LK.LOCKS_NUM = 0
                            AND (LK.LOCKS_NUM + LK.CLOSED_NUM) < LK.ORDERS_NUM
                       THEN
                          'Активный'
                       WHEN     LK.LOCKS_NUM > 0
                            AND (LK.LOCKS_NUM + LK.CLOSED_NUM) = LK.ORDERS_NUM
                       THEN
                          'Заблокирован'
                       WHEN     LK.LOCKS_NUM > 0
                            AND (LK.LOCKS_NUM + LK.CLOSED_NUM) < LK.ORDERS_NUM
                       THEN
                          'Частично заблокирован'
                       WHEN LK.CLOSED_NUM = LK.ORDERS_NUM
                       THEN
                          'Закрыт' -- > 0 AND LK.LOCKS_NUM = 0 THEN 'Закрыт'
                    END)
                    ACCOUNT_STATUS, --LOCKS,                                  --
                 LK.ORDERS_NUM,                       -- кол-во заказов на л/с
                 LK.LOCKS_NUM,
                 LK.CLOSED_NUM,       -- кол-во заблокированных заказов на л/с
                 DECODE (PK400_DEBITORS.Get_AccSrv_list (ABN.ACCOUNT_ID),
                         NULL, '-',
                         PK400_DEBITORS.Get_AccSrv_list (ABN.ACCOUNT_ID))
                    SERVICES,                                 -- услуги на л/с
                 ABN.VIP,
                 --SAL.SAL_NAME  ---------------------------------------------------------------------------------------------<<<
                 PK402_BCR_DATA.Get_sales_curator(ABN.BRANCH_ID, ABN.AGENT_ID, ABN.CONTRACT_ID, ABN.ACCOUNT_ID, NULL, NULL) SAL_NAME
            FROM ABN,
                 LK,
                 BL,
                 B_CUR
           WHERE     ABN.ACCOUNT_ID = BL.ACCOUNT_ID(+)
                 AND ABN.ACCOUNT_ID = LK.ACCOUNT_ID(+)
                 AND ABN.ACCOUNT_ID = B_CUR.ACCOUNT_ID(+))
                 GROUP BY contract_no, vip;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- !!!! !!! TEST !!! !! Развернутый отчет по должникам ЮРИКАМ СГРУППИРОВАННЫЙ !!!! !!! TEST !!! !!
--   - при ошибке выставляет исключение !!!! !!! TEST !!! !! !!!! !!! TEST !!! !!
PROCEDURE Cust_detail_report_group_test( 
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Cust_detail_report_group';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
   SELECT COUNT (account_id) account_no,
         SUM (balance) balance,
         contract_no,
         MAX (customer) customer,
         MAX (inn) inn,
         MAX (kpp) kpp,
         MAX (agent_name) agent_name,
         MAX (brand_name) brand_name,
         MAX (phones) phones,
         MAX (city) city,
         MAX (address) address,
         SUM (cur_due) cur_due,
         decode(MAX (DBT_MONTH), null, 0, MAX (DBT_MONTH)) DBT_MONTH,
         vip,
         DECODE (
            SUM (LOCKS_NUM),
            NULL, 'Неактивный',
            CASE
               WHEN     SUM (LOCKS_NUM) = 0
                    AND (SUM (LOCKS_NUM) + SUM (CLOSED_NUM)) < SUM (ORDERS_NUM)
               THEN
                  'Активный'
               WHEN     SUM (LOCKS_NUM) > 0
                    AND (SUM (LOCKS_NUM) + SUM (CLOSED_NUM)) = SUM (ORDERS_NUM)
               THEN
                  'Заблокирован'
               WHEN     SUM (LOCKS_NUM) > 0
                    AND (SUM (LOCKS_NUM) + SUM (CLOSED_NUM)) < SUM (ORDERS_NUM)
               THEN
                  'Частично заблокирован'
               WHEN SUM (CLOSED_NUM) = SUM (ORDERS_NUM)
               THEN
                  'Закрыт'
            END)
            ACCOUNT_STATUS
    FROM (WITH ABN
               AS (SELECT A.ACCOUNT_ID,
                          A.BALANCE,
                          A.ACCOUNT_NO,
                          DECODE (A.STATUS,
                                  'B', 'Активный',
                                  'C', 'Неактивный')
                             ACCOUNT_STATUS,
                          C.CONTRACT_NO,
                          CS.CUSTOMER,                   -- компания, ИНН, КПП
                          CS.INN,
                          CS.KPP,
                          AA.CITY,
                          AA.ADDRESS,
                          AA.PHONES,
                          BR.CONTRACTOR BRAND_NAME,                   -- бренд
                          AG.CONTRACTOR AGENT_NAME,                   -- агент
                          DECODE (C.CLIENT_TYPE_ID,
                                  6409, 'VIP',
                                  'Юридические лица')
                             VIP                                 -- VIP клиент
                     FROM ACCOUNT_T A,
                          ACCOUNT_PROFILE_T AP,
                          CONTRACT_T C,
                          CUSTOMER_T CS,
                          CONTRACTOR_T BR,
                          CONTRACTOR_T AG,
                          ACCOUNT_CONTACT_T AA
                    WHERE     A.ACCOUNT_TYPE = 'J'
                          AND A.ACCOUNT_ID = AP.ACCOUNT_ID
                          --AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
                          AND AP.CONTRACT_ID = C.CONTRACT_ID
                          AND (AP.DATE_TO is null or AP.DATE_TO > SYSDATE)
                          AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
                          AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
                          AND NVL (AP.AGENT_ID, AP.BRANCH_ID) =
                                 AG.CONTRACTOR_ID(+)
                          AND A.ACCOUNT_ID = AA.ACCOUNT_ID
                          AND AA.ADDRESS_TYPE = 'JUR'
                          AND A.STATUS <> 'T' 
                          AND A.Billing_Id = PK00_CONST.c_BILLING_MMTS), --<> PK00_CONST.c_BILLING_OLD),
               BL
               AS (  SELECT B.ACCOUNT_ID, MIN (B.BILL_DATE) DEB_BILL_DATE
                       FROM BILL_T B
                      WHERE B.DUE < 0
                   GROUP BY B.ACCOUNT_ID),
               B_CUR
               AS (SELECT B.DUE, B.ACCOUNT_ID
                     FROM BILL_T B
                    WHERE B.REP_PERIOD_ID =
                             TO_NUMBER (
                                TO_CHAR (ADD_MONTHS (SYSDATE, -1), 'YYYYmm'))),
               LK
               AS (  SELECT O.ACCOUNT_ID,
                            SUM (
                               CASE
                                  WHEN (SELECT COUNT (ORDER_ID)
                                          FROM order_lock_t
                                         WHERE     LOCK_type_ID <> 901
                                               AND order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED')
                                          IS NULL
                                  THEN
                                     0
                                  ELSE
                                     (SELECT COUNT (ORDER_ID)
                                          FROM order_lock_t
                                         WHERE     LOCK_type_ID <> 901
                                               AND order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED') 
                               END--DECODE(O.STATUS, 'LOCK',1,0)
                               )
                               LOCKS_NUM,
                            SUM (DECODE (O.STATUS, 'CLOSED', 1, 0)) CLOSED_NUM,
                            COUNT (*) ORDERS_NUM
                       FROM ORDER_T O
                   GROUP BY O.ACCOUNT_ID)
          SELECT ABN.ACCOUNT_ID,
                 ABN.BALANCE + (-1) * DECODE (B_CUR.DUE, NULL, 0, B_CUR.DUE)
                    BALANCE,
                 ABN.ACCOUNT_NO,
                 --
                 ABN.CONTRACT_NO,
                 ABN.CUSTOMER,
                 ABN.INN,
                 ABN.KPP,
                 ABN.CITY,
                 ABN.ADDRESS,
                 ABN.PHONES,
                 DECODE (ABN.BRAND_NAME, NULL, '-', ABN.BRAND_NAME) BRAND_NAME,
                 DECODE (ABN.AGENT_NAME, NULL, '-', ABN.AGENT_NAME) AGENT_NAME,
                 BL.DEB_BILL_DATE,
                 DECODE (B_CUR.DUE, NULL, 0, B_CUR.DUE) CUR_DUE,
                 TRUNC (SYSDATE - BL.DEB_BILL_DATE) DEB_DAYS, -- кол-во дней задолженности
                 ROUND (
                    MONTHS_BETWEEN (
                       PK04_PERIOD.Period_end_date (ADD_MONTHS (SYSDATE, -1)),
                       BL.DEB_BILL_DATE))
                    DBT_MONTH,                 -- кол-во месяцев задолженности
                 DECODE (
                    LK.LOCKS_NUM,
                    NULL, 'Неактивный',
                    CASE
                       WHEN     LK.LOCKS_NUM = 0
                            AND (LK.LOCKS_NUM + LK.CLOSED_NUM) < LK.ORDERS_NUM
                       THEN
                          'Активный'
                       WHEN     LK.LOCKS_NUM > 0
                            AND (LK.LOCKS_NUM + LK.CLOSED_NUM) = LK.ORDERS_NUM
                       THEN
                          'Заблокирован'
                       WHEN     LK.LOCKS_NUM > 0
                            AND (LK.LOCKS_NUM + LK.CLOSED_NUM) < LK.ORDERS_NUM
                       THEN
                          'Частично заблокирован'
                       WHEN LK.CLOSED_NUM = LK.ORDERS_NUM
                       THEN
                          'Закрыт' -- > 0 AND LK.LOCKS_NUM = 0 THEN 'Закрыт'
                    END)
                    ACCOUNT_STATUS, --LOCKS,                                  --
                 LK.ORDERS_NUM,                       -- кол-во заказов на л/с
                 LK.LOCKS_NUM,
                 LK.CLOSED_NUM,       -- кол-во заблокированных заказов на л/с
                 DECODE (PK400_DEBITORS.Get_AccSrv_list (ABN.ACCOUNT_ID),
                         NULL, '-',
                         PK400_DEBITORS.Get_AccSrv_list (ABN.ACCOUNT_ID))
                    SERVICES,                                 -- услуги на л/с
                 ABN.VIP
            FROM ABN,
                 LK,
                 BL,
                 B_CUR
           WHERE     ABN.ACCOUNT_ID = BL.ACCOUNT_ID(+)
                 AND ABN.ACCOUNT_ID = LK.ACCOUNT_ID(+)
                 AND ABN.ACCOUNT_ID = B_CUR.ACCOUNT_ID(+))
                 WHERE contract_no = 'KE130477'
                 GROUP BY contract_no, vip;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Развернутый отчет по должникам ЮРИКАМ НА КОНКРЕТНЫЙ ОТЧ. ПЕРИОД
--   - при ошибке выставляет исключение
PROCEDURE Cust_detail_report_period( 
               p_recordset OUT t_refc,
               p_rep_period_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Cust_detail_report_period';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
     WITH ABN AS (
              SELECT A.ACCOUNT_ID,
                     A.BALANCE CUR_BALANCE,
                     RP.CLOSE_BALANCE BALANCE, 
                     A.ACCOUNT_NO,
                     DECODE (A.STATUS, 'B', 'Активный','C', 'Неактивный') ACCOUNT_STATUS,
                     C.CONTRACT_NO,
                     C.CONTRACT_ID, ------------------------------------ <<<<
                     CS.CUSTOMER,                  -- компания, ИНН, КПП 
                     CS.INN, 
                     CS.KPP,  
                     AA.CITY,
                     AA.ADDRESS,
                     AA.PHONES,
                     BR.CONTRACTOR BRAND_NAME,     -- бренд
                     AG.CONTRACTOR AGENT_NAME,      -- агент
                     DECODE(C.CLIENT_TYPE_ID, 6409, 'VIP', 'Юридические лица') VIP, -- VIP клиент
                     AP.BRANCH_ID, AP.AGENT_ID
                FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, 
                     CONTRACT_T C, CUSTOMER_T CS,
                     CONTRACTOR_T BR, CONTRACTOR_T AG,
                     ACCOUNT_CONTACT_T AA,
                     REP_PERIOD_INFO_T RP
               WHERE A.ACCOUNT_TYPE = 'J'
                 AND A.ACCOUNT_ID = AP.ACCOUNT_ID
                 AND (AP.DATE_TO is null or AP.DATE_TO > SYSDATE)
                 AND AP.CONTRACT_ID = C.CONTRACT_ID
                 AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
                 AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
                 AND NVL(AP.AGENT_ID, AP.BRANCH_ID)  = AG.CONTRACTOR_ID(+)
                 AND A.ACCOUNT_ID = AA.ACCOUNT_ID
                 AND AA.ADDRESS_TYPE = 'JUR'
                 AND RP.ACCOUNT_ID = A.ACCOUNT_ID
                 AND RP.REP_PERIOD_ID = 
                 (select max(REP_PERIOD_ID) FROM REP_PERIOD_INFO_T WHERE REP_PERIOD_ID <= p_rep_period_id AND  ACCOUNT_ID = A.ACCOUNT_ID)
                 AND A.STATUS <> 'T'
                 AND A.Billing_Id = PK00_CONST.c_BILLING_MMTS -- <> PK00_CONST.c_BILLING_OLD
          ),
          BL AS (
              SELECT B.ACCOUNT_ID, MIN(B.BILL_DATE) DEB_BILL_DATE 
                FROM BILL_T B 
               WHERE B.DUE < 0 
              GROUP BY B.ACCOUNT_ID
          ),
          B_CUR
          AS (SELECT sum(B.DUE) DUE, B.ACCOUNT_ID
              FROM BILL_T B
               WHERE B.REP_PERIOD_ID = TO_NUMBER (TO_CHAR (ADD_MONTHS (SYSDATE, 
               (SELECT (days_for_payment/30) *(-1) from billinfo_t where account_id = b.account_id )
--               -1
               ), 'YYYYmm')) 
               GROUP BY B.ACCOUNT_ID         
                 ),           
          LK AS (
              SELECT O.ACCOUNT_ID, 
                     SUM(
                        CASE WHEN (select COUNT(ORDER_ID) from order_lock_t where LOCK_type_ID <> 901 and order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED') is null THEN 0
                        ELSE (select COUNT(ORDER_ID) from order_lock_t where LOCK_type_ID <> 901 and order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED') END
                     --DECODE(O.STATUS, 'LOCK',1,0)
                     ) LOCKS_NUM,
                     SUM(DECODE(O.STATUS, 'CLOSED',1,0)) CLOSED_NUM,
                     COUNT(*) ORDERS_NUM
                FROM ORDER_T O
              GROUP BY O.ACCOUNT_ID
          )
          SELECT ABN.ACCOUNT_ID,
                 ABN.BALANCE,
                 ABN.CUR_BALANCE - decode(B_CUR.DUE, null,0, B_CUR.DUE) CUR_BALANCE,
                 ABN.ACCOUNT_NO,
                 ABN.CONTRACT_NO,
                 decode(ABN.CUSTOMER,  null, '-', ABN.CUSTOMER) CUSTOMER,
                 decode(ABN.INN,  null, '-', ABN.INN) INN,
                 decode(ABN.KPP, null, '-', ABN.KPP) KPP,
                 decode(ABN.CITY, null, '-', ABN.CITY) CITY,
                 decode(ABN.ADDRESS, null, '-', ABN.ADDRESS) ADDRESS, 
                 decode(ABN.PHONES, null, '-', ABN.PHONES) PHONES,
                 decode(ABN.BRAND_NAME, null, '-',ABN.BRAND_NAME) BRAND_NAME,
                 decode(ABN.AGENT_NAME, null, '-',ABN.AGENT_NAME) AGENT_NAME,
                 BL.DEB_BILL_DATE,
                 decode(B_CUR.DUE, null,0, B_CUR.DUE) CUR_DUE,
                 TRUNC(SYSDATE - BL.DEB_BILL_DATE) DEB_DAYS, -- кол-во дней задолженности
                 case 
                 when ADD_MONTHS(PK04_PERIOD.Period_to(p_rep_period_id),-1) >  BL.DEB_BILL_DATE then
                 ROUND (MONTHS_BETWEEN (ADD_MONTHS(PK04_PERIOD.Period_to(p_rep_period_id),-1), BL.DEB_BILL_DATE)) 
                 else 0 end DBT_MONTH,                 
                 --ROUND (MONTHS_BETWEEN (ADD_MONTHS(PK04_PERIOD.Period_to(p_rep_period_id),-1), BL.DEB_BILL_DATE)) DBT_MONTH, -- кол-во месяцев задолженности
                 DECODE(LK.LOCKS_NUM, null, 'Неактивный', 
                 CASE
                     WHEN LK.LOCKS_NUM = 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) < LK.ORDERS_NUM THEN 'Активный'
                     WHEN LK.LOCKS_NUM > 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) = LK.ORDERS_NUM THEN 'Заблокирован'
                     WHEN LK.LOCKS_NUM > 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) < LK.ORDERS_NUM THEN 'Частично заблокирован'
                     WHEN LK.CLOSED_NUM = LK.ORDERS_NUM THEN 'Закрыт'
                     --WHEN LK.CLOSED_NUM > 0 AND LK.LOCKS_NUM = 0 THEN 'Закрыт'
                 END) ACCOUNT_STATUS, 
                 LK.ORDERS_NUM,                              -- кол-во заказов на л/с
                 LK.LOCKS_NUM,                               -- кол-во заблокированных заказов на л/с
                 decode(PK400_DEBITORS.Get_AccSrv_list(ABN.ACCOUNT_ID), null, '-', PK400_DEBITORS.Get_AccSrv_list(ABN.ACCOUNT_ID))  SERVICES,  
                 ABN.VIP,
                 PK402_BCR_DATA.Get_sales_curator(ABN.BRANCH_ID, ABN.AGENT_ID, ABN.CONTRACT_ID, ABN.ACCOUNT_ID, NULL, NULL) SALE_NAME  ---------------------------------------------------------------------------------------------<<<
            FROM ABN, LK, BL, B_CUR 
           WHERE ABN.ACCOUNT_ID = BL.ACCOUNT_ID (+)
             AND ABN.ACCOUNT_ID = LK.ACCOUNT_ID(+)
             AND ABN.ACCOUNT_ID = B_CUR.ACCOUNT_ID(+)
             --AND ABN.BALANCE  + (-1) * decode(B_CUR.DUE, NULL, 0, B_CUR.DUE)< 0 
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
-- Развернутый отчет по должникам ЮРИКАМ НА КОНКРЕТНЫЙ ОТЧ. ПЕРИОД СГРУППИРОВАННЫЙ !!
--   - при ошибке выставляет исключение
PROCEDURE Cust_det_rep_period_group( 
               p_recordset OUT t_refc,
               p_rep_period_id IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Cust_det_rep_period_group';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
    SELECT COUNT (account_id) account_no,
         SUM (balance) balance,
         SUM(CUR_BALANCE) CUR_BALANCE,
         contract_no,
         MAX (customer) customer,
         MAX (inn) inn,
         MAX (kpp) kpp,
         MAX (agent_name) agent_name,
         MAX (brand_name) brand_name,
         MAX (phones) phones,
         MAX (city) city,
         MAX (address) address,
         SUM (cur_due) cur_due,
         decode(MAX (DBT_MONTH), null, 0, MAX (DBT_MONTH)) DBT_MONTH,
         vip,
         DECODE (
            SUM (LOCKS_NUM),
            NULL, 'Неактивный',
            CASE
               WHEN     SUM (LOCKS_NUM) = 0
                    AND (SUM (LOCKS_NUM) + SUM (CLOSED_NUM)) < SUM (ORDERS_NUM)
               THEN
                  'Активный'
               WHEN     SUM (LOCKS_NUM) > 0
                    AND (SUM (LOCKS_NUM) + SUM (CLOSED_NUM)) = SUM (ORDERS_NUM)
               THEN
                  'Заблокирован'
               WHEN     SUM (LOCKS_NUM) > 0
                    AND (SUM (LOCKS_NUM) + SUM (CLOSED_NUM)) < SUM (ORDERS_NUM)
               THEN
                  'Частично заблокирован'
               WHEN SUM (CLOSED_NUM) = SUM (ORDERS_NUM)
               THEN
                  'Закрыт'
            END)
            ACCOUNT_STATUS,
            MAX(SALE_NAME) SALE_NAME
    FROM (     
     WITH ABN AS (
              SELECT A.ACCOUNT_ID,
                     A.BALANCE CUR_BALANCE,
                     RP.CLOSE_BALANCE BALANCE, 
                     A.ACCOUNT_NO,
                     DECODE (A.STATUS, 'B', 'Активный','C', 'Неактивный') ACCOUNT_STATUS,
                     C.CONTRACT_NO,
                     C.CONTRACT_ID, ------------------------------------ <<<<
                     CS.CUSTOMER,                  -- компания, ИНН, КПП 
                     CS.INN, 
                     CS.KPP,  
                     AA.CITY,
                     AA.ADDRESS,
                     AA.PHONES,
                     BR.CONTRACTOR BRAND_NAME,     -- бренд
                     AG.CONTRACTOR AGENT_NAME,      -- агент
                     DECODE(C.CLIENT_TYPE_ID, 6409, 'VIP', 'Юридические лица') VIP, -- VIP клиент
                     AP.AGENT_ID, AP.BRANCH_ID
                FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, 
                     CONTRACT_T C, CUSTOMER_T CS,
                     CONTRACTOR_T BR, CONTRACTOR_T AG,
                     ACCOUNT_CONTACT_T AA,
                     REP_PERIOD_INFO_T RP
               WHERE A.ACCOUNT_TYPE = 'J'
                 AND A.ACCOUNT_ID = AP.ACCOUNT_ID
                 AND AP.CONTRACT_ID = C.CONTRACT_ID
                 AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
                 AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
                 AND NVL(AP.AGENT_ID, AP.BRANCH_ID)  = AG.CONTRACTOR_ID(+)
                 AND A.ACCOUNT_ID = AA.ACCOUNT_ID
                 AND AA.ADDRESS_TYPE = 'JUR'
                 AND RP.ACCOUNT_ID = A.ACCOUNT_ID
                 AND (AP.DATE_TO is null or AP.DATE_TO > SYSDATE)
                 AND RP.REP_PERIOD_ID = 
                 (select max(REP_PERIOD_ID) FROM REP_PERIOD_INFO_T WHERE REP_PERIOD_ID <= p_rep_period_id AND  ACCOUNT_ID = A.ACCOUNT_ID)
                 AND A.STATUS <> 'T' 
                 AND A.Billing_Id = PK00_CONST.c_BILLING_MMTS --  <> PK00_CONST.c_BILLING_OLD
          ),
          BL AS (
              SELECT B.ACCOUNT_ID, MIN(B.BILL_DATE) DEB_BILL_DATE 
                FROM BILL_T B 
               WHERE B.DUE < 0 
              GROUP BY B.ACCOUNT_ID
          ),
          B_CUR
          AS (SELECT sum(B.DUE) DUE, B.ACCOUNT_ID
              FROM BILL_T B
               WHERE B.REP_PERIOD_ID = TO_NUMBER (TO_CHAR (ADD_MONTHS (SYSDATE, 
               (SELECT (days_for_payment/30) *(-1) from billinfo_t where account_id = b.account_id )
--               -1
               ), 'YYYYmm')) 
               GROUP BY B.ACCOUNT_ID         
                 ),
          LK AS (
              SELECT O.ACCOUNT_ID, 
                     SUM(
                        CASE WHEN (select COUNT(ORDER_ID) from order_lock_t where LOCK_type_ID <> 901 and order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED') is null THEN 0
                        ELSE (select COUNT(ORDER_ID) from order_lock_t where LOCK_type_ID <> 901 and order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED') END
                     --DECODE(O.STATUS, 'LOCK',1,0)
                     ) LOCKS_NUM,
                     SUM(DECODE(O.STATUS, 'CLOSED',1,0)) CLOSED_NUM,
                     COUNT(*) ORDERS_NUM
                FROM ORDER_T O
              GROUP BY O.ACCOUNT_ID
          )
          SELECT ABN.ACCOUNT_ID,
                 ABN.BALANCE,
                 ABN.CUR_BALANCE - decode(B_CUR.DUE, null,0, B_CUR.DUE) CUR_BALANCE,
                 ABN.ACCOUNT_NO,
                 --ABN.ACCOUNT_STATUS,
                 ABN.CONTRACT_NO,
                 ABN.CUSTOMER, 
                 ABN.INN, 
                 ABN.KPP,
                 ABN.CITY,
                 ABN.ADDRESS,
                 ABN.PHONES,
                 decode(ABN.BRAND_NAME, null, '-',ABN.BRAND_NAME) BRAND_NAME,
                 decode(ABN.AGENT_NAME, null, '-',ABN.AGENT_NAME) AGENT_NAME,
                 BL.DEB_BILL_DATE,
                 decode(B_CUR.DUE, null,0, B_CUR.DUE) CUR_DUE,
                 TRUNC(SYSDATE - BL.DEB_BILL_DATE) DEB_DAYS, -- кол-во дней задолженности
                 case 
                 when ADD_MONTHS(PK04_PERIOD.Period_to(p_rep_period_id),-1) >  BL.DEB_BILL_DATE then
                 ROUND (MONTHS_BETWEEN (ADD_MONTHS(PK04_PERIOD.Period_to(p_rep_period_id),-1), BL.DEB_BILL_DATE)) 
                 else 0 end DBT_MONTH,                 
                 --ROUND (MONTHS_BETWEEN (ADD_MONTHS(PK04_PERIOD.Period_to(p_rep_period_id),-1), BL.DEB_BILL_DATE)) DBT_MONTH, -- кол-во месяцев задолженности
                 DECODE(LK.LOCKS_NUM, null, 'Неактивный', 
                 CASE
                     WHEN LK.LOCKS_NUM = 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) < LK.ORDERS_NUM THEN 'Активный'
                     WHEN LK.LOCKS_NUM > 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) = LK.ORDERS_NUM THEN 'Заблокирован'
                     WHEN LK.LOCKS_NUM > 0 AND (LK.LOCKS_NUM + LK.CLOSED_NUM) < LK.ORDERS_NUM THEN 'Частично заблокирован'
                     WHEN LK.CLOSED_NUM = LK.ORDERS_NUM THEN 'Закрыт'
                     --WHEN LK.CLOSED_NUM > 0 AND LK.LOCKS_NUM = 0 THEN 'Закрыт'
                 END) ACCOUNT_STATUS, 
                 LK.ORDERS_NUM,                              -- кол-во заказов на л/с
                 LK.LOCKS_NUM,                               -- кол-во заблокированных заказов на л/с
                 LK.CLOSED_NUM,
                 decode(PK400_DEBITORS.Get_AccSrv_list(ABN.ACCOUNT_ID), null, '-', PK400_DEBITORS.Get_AccSrv_list(ABN.ACCOUNT_ID))  SERVICES,  
                 ABN.VIP,
                 PK402_BCR_DATA.Get_sales_curator(ABN.BRANCH_ID, ABN.AGENT_ID, ABN.CONTRACT_ID, ABN.ACCOUNT_ID, NULL, NULL) SALE_NAME  ---------------------------------------------------------------------------------------------<<<
            FROM ABN, LK, BL, B_CUR
           WHERE ABN.ACCOUNT_ID = BL.ACCOUNT_ID (+)
             AND ABN.ACCOUNT_ID = LK.ACCOUNT_ID(+)
             AND ABN.ACCOUNT_ID = B_CUR.ACCOUNT_ID(+)
             ) GROUP BY contract_no, vip;
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
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Subs_debitors_for_call';
    v_retcode    INTEGER;
    v_period_id  INTEGER := to_number(to_char(ADD_MONTHS(sysdate,-1), 'YYYY') || to_char(ADD_MONTHS(sysdate,-1), 'mm'));
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
    WITH ABN
     AS (SELECT A.ACCOUNT_ID,
                A.BALANCE,
                A.ACCOUNT_NO,
                DECODE (A.STATUS,
                        'B', 'Активный',
                        'C', 'Закрытый')
                   ACCOUNT_STATUS,
                C.CONTRACT_NO,
                C.CLIENT_TYPE_ID,
                   INITCAP (S.LAST_NAME)
                || ' '
                || INITCAP (S.FIRST_NAME)
                || ' '
                || INITCAP (S.MIDDLE_NAME)
                   SUBS_NAME,                                        
                AA.CITY,
                AA.ADDRESS,
                AA.PHONES,
                BR.SHORT_NAME BRAND_NAME,                             
                decode(AG.SHORT_NAME, null, '.',AG.SHORT_NAME) AGENT_NAME                              
           FROM ACCOUNT_T A,
                ACCOUNT_PROFILE_T AP,
                CONTRACT_T C,
                SUBSCRIBER_T S,
                CONTRACTOR_T BR,
                CONTRACTOR_T AG,
                ACCOUNT_CONTACT_T AA
          WHERE     A.ACCOUNT_TYPE = 'P'
                    AND A.BILLING_ID <> 2007
                AND A.ACCOUNT_ID = AP.ACCOUNT_ID
                AND (   AP.DATE_TO IS NULL
                     OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
                AND AP.CONTRACT_ID = C.CONTRACT_ID
                AND AP.SUBSCRIBER_ID = S.SUBSCRIBER_ID
                AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
                AND NVL(AP.AGENT_ID, AP.BRANCH_ID) = AG.CONTRACTOR_ID(+)
                AND A.ACCOUNT_ID = AA.ACCOUNT_ID
                AND AA.ADDRESS_TYPE = 'DLV'
                ), 
     BL
     AS (  SELECT B.ACCOUNT_ID, MIN (B.BILL_DATE) DEB_BILL_DATE
             FROM BILL_T B
            WHERE B.DUE < 0
            AND B.REP_PERIOD_ID < v_period_id
         GROUP BY B.ACCOUNT_ID),
     B_CUR
     AS (  SELECT B.DUE, B.ACCOUNT_ID
             FROM BILL_T B
             WHERE B.REP_PERIOD_ID =  v_period_id
         ),
     LK
     AS (  SELECT O.ACCOUNT_ID,
                  SUM (DECODE (O.STATUS, 'LOCK', 1, 0)) LOCKS_NUM, 
                  COUNT (*) ORDERS_NUM,
                  O.TIME_ZONE
             FROM ORDER_T O
            WHERE (   O.DATE_TO IS NULL
                   OR SYSDATE BETWEEN O.DATE_FROM AND O.DATE_TO)
                   AND O.SERVICE_ID = 1
         GROUP BY O.ACCOUNT_ID, O.TIME_ZONE)
  SELECT ABN.ACCOUNT_ID Id,
         ABN.PHONES phonenumber1,
         ABN.PHONES phonenumber2,
         LK.TIME_ZONE timezone,
         ROUND(ABN.BALANCE + (-1) * decode(B_CUR.DUE, NULL, 0, B_CUR.DUE), 2) sum_debt
    FROM ABN, LK, BL , B_CUR                                                    
   WHERE     ABN.ACCOUNT_ID = BL.ACCOUNT_ID
         AND ABN.ACCOUNT_ID = LK.ACCOUNT_ID
         AND ABN.ACCOUNT_ID = B_CUR.ACCOUNT_ID(+)
         AND ABN.BALANCE + (-1) * decode(B_CUR.DUE, NULL, 0, B_CUR.DUE) < -100
         AND LK.LOCKS_NUM = 0            
         --AND (v_period_id - PK04_PERIOD.Period_id(BL.DEB_BILL_DATE)) = 1
         and MONTHS_BETWEEN(to_date(v_period_id,'YYYYmm'), to_date(PK04_PERIOD.Period_id(BL.DEB_BILL_DATE),'YYYYmm')) = 1
         and LK.TIME_ZONE is not null
         AND AGENT_NAME <> 'Восточно-Сибирская ЖД'
         AND (ABN.CLIENT_TYPE_ID is null or  ABN.CLIENT_TYPE_ID <> 6409) --Исключим на всякий случай VIP-ов
         order by ABN.ACCOUNT_ID;
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
             ROUND(PT.TRANSFER_TOTAL,2) as SUM_RAZ , 
             ROUND(P.RECVD,2) as SUM_PAY, 
             TRUNC(P.PAYMENT_DATE) as PAY_DATE, 
             TO_DATE('01.01.1970','dd.mm.yyyy') as VYP_DATE, 
             CT.SHORT_NAME as AGENT,
             PS.PAYSYSTEM_NAME as BANK_CODE
        FROM PAYMENT_T P, PAY_TRANSFER_T PT, BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CUSTOMER_T CST, CONTRACTOR_T CT, PAYSYSTEM_T PS
       WHERE P.REP_PERIOD_ID BETWEEN v_period_id_from AND v_period_id_to
         AND P.PAYMENT_DATE  BETWEEN p_start_pay_date AND (p_end_pay_date  + 23/24  + 59/1440 + 59/86400)
         AND PT.PAYMENT_ID = P.PAYMENT_ID
         AND PT.BILL_ID = B.BILL_ID
         AND A.ACCOUNT_ID = B.ACCOUNT_ID
         AND A.ACCOUNT_TYPE = PK00_CONST.c_ACC_TYPE_J
         AND A.ACCOUNT_ID = AP.ACCOUNT_ID
         AND AP.CUSTOMER_ID = CST.CUSTOMER_ID
         AND NVL(AP.AGENT_ID, AP.BRANCH_ID) = CT.CONTRACTOR_ID
         AND PS.PAYSYSTEM_ID = p.paysystem_id
         AND A.STATUS <> 'T'
         AND A.Billing_Id = PK00_CONST.c_BILLING_MMTS -- <> PK00_CONST.c_BILLING_OLD
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
      WITH OP_M AS (
             SELECT MAX (op.date_from) dd, A.ACCOUNT_ID
             FROM ORDER_PHONES_T op, account_t a, order_t o
             where OP.ORDER_ID = o.ORDER_ID
             and O.ACCOUNT_ID = a.ACCOUNT_ID
             GROUP BY a.ACCOUNT_ID
          )   
      SELECT CST.LAST_NAME || ' ' || CST.FIRST_NAME || ' ' || CST.MIDDLE_NAME  as NAME, 
            OP.PHONE_NUMBER as PHONE,
            A.ACCOUNT_NO, 
            P.TRANSFERED as SUM_RAZ , 
            P.RECVD as SUM_PAY, 
            TRUNC(P.PAYMENT_DATE) as PAY_DATE, 
            TO_DATE('01.01.1970','dd.mm.yyyy') as VYP_DATE, -- ??????????????????????
            CT.SHORT_NAME as AGENT,
            --P.PAYSYSTEM_CODE as BANK_CODE
            PS.PAYSYSTEM_NAME as BANK_CODE
        FROM PAYMENT_T P, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, SUBSCRIBER_T CST, 
             CONTRACTOR_T CT, ORDER_T O, ORDER_PHONES_T OP, PAYSYSTEM_T PS, OP_M
       WHERE P.REP_PERIOD_ID BETWEEN v_period_id_from AND v_period_id_to
         AND P.PAYMENT_DATE BETWEEN p_start_pay_date AND p_end_pay_date
         AND A.ACCOUNT_ID = P.ACCOUNT_ID
         AND A.ACCOUNT_TYPE = PK00_CONST.c_ACC_TYPE_P
         AND A.ACCOUNT_ID = AP.ACCOUNT_ID
         AND AP.SUBSCRIBER_ID = CST.SUBSCRIBER_ID
         AND NVL(AP.AGENT_ID, AP.BRANCH_ID) = CT.CONTRACTOR_ID
         AND O.ACCOUNT_ID = A.ACCOUNT_ID
         AND OP.ORDER_ID = O.ORDER_ID
         --AND O.DATE_TO > SYSDATE
         --AND OP.DATE_TO > SYSDATE
         AND OP_M.ACCOUNT_ID = A.ACCOUNT_ID
         AND OP_M.dd = OP.date_from         
         AND PS.PAYSYSTEM_ID = p.paysystem_id
         AND A.STATUS <> 'T'
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
             A.ACCOUNT_NO, A.BALANCE
        FROM PAYMENT_T P, ACCOUNT_T A
       WHERE P.ACCOUNT_ID = A.ACCOUNT_ID
         AND P.BALANCE <> 0
         AND A.ACCOUNT_TYPE = Pk00_Const.c_ACC_TYPE_J
         AND A.STATUS <> 'T'
         AND A.Billing_Id = PK00_CONST.c_BILLING_MMTS --<> PK00_CONST.c_BILLING_OLD
         --AND A.BALANCE < 0
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
                            p_result IN INTEGER,
                            p_sum_debt IN NUMBER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'WriteCallHistory';
    v_retcode       INTEGER;   
BEGIN
   INSERT INTO EXT02_DEB_CALL_HISTORY(FILE_ID, ACCOUNT_ID, PHONE, CALL_DATE, RESULT_INFO, RESULT, SUM_DEBT)
                             VALUES(p_file_id, p_account_id, p_phone, p_call_date, p_result_info, p_result, p_sum_debt);
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- Детализированный отчет об информировании по ДЗ (ФЛ)
PROCEDURE DZ_FL_DETAIL_INFO(p_recordset OUT t_refc, p_rep_reriod_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DZ_FL_DETAIL_INFO';
    v_retcode    INTEGER;  
    v_max_file_date DATE;
    v_max_file_id INTEGER;
    v_max_file_date_block DATE;
    v_max_file_id_block INTEGER;    
BEGIN
  SELECT MAX(FILE_DATE), MAX(FILE_ID) INTO v_max_file_date, v_max_file_id FROM EXT02_DEB_CALL_FILES WHERE CALL_TYPE = 'Inform' AND CLIENT_TYPE = 'P';
  SELECT decode(MAX(FILE_DATE), null , v_max_file_date, MAX(FILE_DATE))  , decode(MAX(FILE_ID), null, v_max_file_id , MAX(FILE_ID))  INTO v_max_file_date_block, v_max_file_id_block FROM EXT02_DEB_CALL_FILES WHERE CALL_TYPE = 'Block' AND CLIENT_TYPE = 'P' AND FILE_ID > v_max_file_id;
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
               --(p_rep_reriod_id - PK04_PERIOD.Period_id(B.DEB_BILL_DATE)) PERIOD_COUNT
               MONTHS_BETWEEN(to_date(p_rep_reriod_id,'YYYYmm'), to_date(PK04_PERIOD.Period_id(B.DEB_BILL_DATE),'YYYYmm')) PERIOD_COUNT
          FROM EXT02_DEB_CALL_HISTORY H,
               ACCOUNT_T A,
               account_profile_t ap,
               subscriber_t s,
               CONTRACTOR_T BR,
               CONTRACTOR_T AG,
               (  SELECT B.ACCOUNT_ID, MIN (B.BILL_DATE) DEB_BILL_DATE
                    FROM BILL_T B
                   WHERE B.DUE < 0
                   AND B.REP_PERIOD_ID < p_rep_reriod_id
                GROUP BY B.ACCOUNT_ID) B
         WHERE     AP.ACCOUNT_ID = h.ACCOUNT_ID
               --AND A.BILLING_ID <> 2007
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
               MONTHS_BETWEEN(to_date(p_rep_reriod_id,'YYYYmm'), to_date(PK04_PERIOD.Period_id(B.DEB_BILL_DATE),'YYYYmm'))
               --(p_rep_reriod_id - PK04_PERIOD.Period_id(B.DEB_BILL_DATE))
       UNION
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
               --(p_rep_reriod_id - PK04_PERIOD.Period_id(B.DEB_BILL_DATE)) PERIOD_COUNT
               MONTHS_BETWEEN(to_date(p_rep_reriod_id,'YYYYmm'), to_date(PK04_PERIOD.Period_id(B.DEB_BILL_DATE),'YYYYmm')) PERIOD_COUNT
          FROM EXT02_DEB_CALL_HISTORY H,
               ACCOUNT_T A,
               account_profile_t ap,
               subscriber_t s,
               CONTRACTOR_T BR,
               CONTRACTOR_T AG,
               (  SELECT B.ACCOUNT_ID, MIN (B.BILL_DATE) DEB_BILL_DATE
                    FROM BILL_T B
                   WHERE B.DUE < 0
                   AND B.REP_PERIOD_ID < p_rep_reriod_id
                GROUP BY B.ACCOUNT_ID) B
         WHERE     AP.ACCOUNT_ID = h.ACCOUNT_ID
               AND h.ACCOUNT_ID = A.ACCOUNT_ID
               AND S.SUBSCRIBER_ID = AP.SUBSCRIBER_ID
               AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
               AND AP.AGENT_ID = AG.CONTRACTOR_ID(+)
               AND H.ACCOUNT_ID = B.ACCOUNT_ID(+)
               AND H.FILE_ID = v_max_file_id_block
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
               --(p_rep_reriod_id - PK04_PERIOD.Period_id(B.DEB_BILL_DATE))                      
               MONTHS_BETWEEN(to_date(p_rep_reriod_id,'YYYYmm'), to_date(PK04_PERIOD.Period_id(B.DEB_BILL_DATE),'YYYYmm'))
               ;     
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
-- Детализированный отчет об информировании (блокировки) по ДЗ (ФЛ)
PROCEDURE DZ_FL_DETAIL_INFO_BLOCK(p_recordset OUT t_refc)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DZ_FL_DETAIL_INFO_BLOCK';
    v_retcode    INTEGER;  
    v_max_file_date DATE;
    v_max_file_id INTEGER;    
BEGIN
  SELECT MAX(FILE_DATE), MAX(FILE_ID) INTO v_max_file_date, v_max_file_id FROM EXT02_DEB_CALL_FILES WHERE CALL_TYPE = 'Block' AND CLIENT_TYPE = 'P';
   OPEN p_recordset FOR
      WITH BL AS (
                    SELECT B.ACCOUNT_ID, MIN(B.BILL_DATE) DEB_BILL_DATE
                      FROM BILL_T B 
                     WHERE B.DUE < 0 
                    GROUP BY B.ACCOUNT_ID
                )
      SELECT A.ACCOUNT_NO ACCOUNT_NO,
             AA.PHONES PHONE,
                INITCAP (S.LAST_NAME)
             || ' '
             || INITCAP (S.FIRST_NAME)
             || ' '
             || INITCAP (S.MIDDLE_NAME)
                NAME,
             AA.CITY,
             AA.ADDRESS,
             BR.SHORT_NAME BRAND,
             DECODE (AG.SHORT_NAME, NULL, '-', AG.SHORT_NAME) AGENT,
             --DECODE(O.STATUS,'LOCK', EX.SUM_DEBT, 0) SUM_FORM,
             EX.SUM_DEBT SUM_FORM,
             DECODE(O.STATUS,'LOCK', 'Блокирован', 'Активен') SERVICE_STATUS,
             OL.CREATE_DATE + 4/24 REQUEST_BLOCK_DATE,
             DECODE(O.STATUS,'LOCK', ROUND(MONTHS_BETWEEN(ADD_MONTHS(sysdate,-1), BL.DEB_BILL_DATE)), 0) PERIOD_COUNT
        FROM EXT02_DEB_CALL_HISTORY ex,
             ACCOUNT_T A,
             ACCOUNT_PROFILE_T AP,
             CONTRACT_T C,
             SUBSCRIBER_T S,
             CONTRACTOR_T BR,
             CONTRACTOR_T AG,
             ACCOUNT_CONTACT_T AA,
             ORDER_LOCK_T OL,
             ORDER_T O, BL
       WHERE EX.FILE_ID = v_max_file_id
             AND  EX.ACCOUNT_ID = A.ACCOUNT_ID
             AND A.ACCOUNT_TYPE = 'P'
             AND A.ACCOUNT_ID = AP.ACCOUNT_ID
             AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
             AND AP.CONTRACT_ID = C.CONTRACT_ID
             AND AP.SUBSCRIBER_ID = S.SUBSCRIBER_ID
             AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
             AND AP.AGENT_ID = AG.CONTRACTOR_ID(+)
             AND A.ACCOUNT_ID = AA.ACCOUNT_ID
             AND AA.ADDRESS_TYPE = 'DLV'
             AND O.ACCOUNT_ID = A.ACCOUNT_ID
             AND OL.ORDER_ID(+) = O.ORDER_ID
             AND A.ACCOUNT_ID = BL.ACCOUNT_ID(+)
             AND O.STATUS <> 'CLOSED';        
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
-------------------------------------------------
-- Отчет о блокировках за период
-------------------------------------------------
PROCEDURE DZ_FL_DETAIL_BLOCK_BY_PERIOD(p_recordset OUT t_refc, p_start_date IN DATE, p_end_date IN DATE)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DZ_FL_DETAIL_BLOCK_BY_PERIOD';
    v_retcode    INTEGER;    
BEGIN
    OPEN p_recordset FOR
        WITH BL AS (
                      SELECT B.ACCOUNT_ID, MIN(B.BILL_DATE) DEB_BILL_DATE
                        FROM BILL_T B 
                       WHERE B.DUE < 0 
                      GROUP BY B.ACCOUNT_ID
                  ),
              B_CUR
                AS (SELECT B.DUE, B.ACCOUNT_ID
                    FROM BILL_T B
                       WHERE B.REP_PERIOD_ID =  to_number(to_char(ADD_MONTHS(sysdate,-1), 'YYYY') || to_char(ADD_MONTHS(sysdate,-1), 'mm'))
                       )              
        SELECT A.ACCOUNT_NO ACCOUNT_NO,
               --NVL(AA.PHONES, '-') PHONE,
               OP.PHONE_NUMBER PHONE,
                  INITCAP (S.LAST_NAME)
               || ' '
               || INITCAP (S.FIRST_NAME)
               || ' '
               || INITCAP (S.MIDDLE_NAME)
                  NAME,
               NVL(AA.CITY, '-') CITY,
               NVL(AA.ADDRESS, '-') ADDRESS,
               BR.SHORT_NAME BRAND,
               DECODE (AG.SHORT_NAME, NULL, '-', AG.SHORT_NAME) AGENT,
               DECODE(O.STATUS,'LOCK', 'Блокирован', 'Активен') SERVICE_STATUS,
               OL.CREATE_DATE + 4/24 REQUEST_BLOCK_DATE,
               case 
                   when ADD_MONTHS(sysdate,-1) >  BL.DEB_BILL_DATE then
                   ROUND (MONTHS_BETWEEN (ADD_MONTHS(sysdate,-1), BL.DEB_BILL_DATE)) 
                   else 0 
               end PERIOD_COUNT,         
               --DECODE(O.STATUS,'LOCK', ROUND(MONTHS_BETWEEN(ADD_MONTHS(sysdate,-1), BL.DEB_BILL_DATE)), 0) PERIOD_COUNT,
               --(A.BALANCE  + (-1) * B_CUR.DUE) * (-1) sum_debt
              -- decode(B_CUR.DUE, null, 0, (A.BALANCE  + (-1) * B_CUR.DUE) * (-1)) sum_debt
              decode(B_CUR.DUE, null, 0, B_CUR.DUE * (-1)) sum_debt, -- текущая задолженность!
              (A.BALANCE + (-1) * decode(B_CUR.DUE, null,0, B_CUR.DUE)) *(-1) sum_debt_old -- просроченная задолженность
          FROM ACCOUNT_T A,
               ACCOUNT_PROFILE_T AP,
               CONTRACT_T C,
               SUBSCRIBER_T S,
               CONTRACTOR_T BR,
               CONTRACTOR_T AG,
               ACCOUNT_CONTACT_T AA,
               ORDER_LOCK_T OL,
               ORDER_T O, BL, B_CUR,
               ORDER_PHONES_T OP
         WHERE A.ACCOUNT_TYPE = 'P'
               AND A.ACCOUNT_ID = AP.ACCOUNT_ID
               --AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
               AND AP.CONTRACT_ID = C.CONTRACT_ID
               AND AP.SUBSCRIBER_ID = S.SUBSCRIBER_ID
               AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
               AND AP.AGENT_ID = AG.CONTRACTOR_ID(+)
               AND A.ACCOUNT_ID = AA.ACCOUNT_ID
               AND AA.ADDRESS_TYPE = 'DLV'
               AND O.ACCOUNT_ID = A.ACCOUNT_ID
               AND OL.ORDER_ID(+) = O.ORDER_ID
               AND A.ACCOUNT_ID = BL.ACCOUNT_ID(+)
               AND A.ACCOUNT_ID = B_CUR.ACCOUNT_ID(+)
               AND O.STATUS <> 'CLOSED'
               AND trunc(OL.CREATE_DATE) BETWEEN p_start_date and p_end_date
               AND OL.LOCK_type_id <> 901 -- 'Блокировка при создании заказа'
               AND OL.LOCK_REASON <> 'Ручная блокировка. Сверка с ИН-платформой'
               AND OL.LOCKED_BY <> 'IMPORT'
               AND A.BILLING_ID = 2003
               AND (AP.DATE_TO is null OR AP.DATE_TO = TO_DATE('01.01.2050', 'dd.mm.yyyy'))
               AND OP.ORDER_ID = O.ORDER_ID
               AND (OP.DATE_TO is null OR OP.DATE_TO = TO_DATE('01.01.2050', 'dd.mm.yyyy'))
               /*and o.order_id not in 
               (
                    select distinct order_id from ORDER_LOCK_T 
                    where date_from = trunc(date_to)
                    and date_from = to_date('27.10.2014', 'dd.mm.yyyy')               
               ) */              
               --AND DECODE (AG.SHORT_NAME, NULL, '-', AG.SHORT_NAME) <> 'Восточно-Сибирская ЖД'
               ORDER BY OL.CREATE_DATE;         
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
--------------------------------------------------------------------------------
-- Агрегированный отчет о покрытии платежами выставленных счетов за месяц (ФЛ)
--------------------------------------------------------------------------------
PROCEDURE Agr_bill_payments(p_recordset OUT t_refc, p_rep_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Agr_bill_payments_fl';
    v_retcode    INTEGER;  
BEGIN
  OPEN p_recordset FOR
SELECT * FROM (
SELECT BR.CONTRACTOR BRAND,
         AG.CONTRACTOR AGENT,         
         A.ACCOUNT_NO,
         b.total SUM_BILL,
         decode(SUM (PT.TRANSFER_TOTAL), null, 0, (SUM (PT.TRANSFER_TOTAL))) SUM_RAZ,
         decode(b.total - SUM (PT.TRANSFER_TOTAL), null, b.total, b.total - SUM (PT.TRANSFER_TOTAL)) SUM_DEB
    FROM bill_t b,
         account_t a,
         ACCOUNT_PROFILE_T AP,
         pay_transfer_t pt,
         CONTRACTOR_T BR,
         CONTRACTOR_T AG
   WHERE     B.REP_PERIOD_ID = p_rep_period_id
         AND B.ACCOUNT_ID = A.ACCOUNT_ID
         AND AP.ACCOUNT_ID = A.ACCOUNT_ID
         AND B.BILL_ID = PT.BILL_ID(+)
         AND A.ACCOUNT_TYPE = 'P'
         --AND A.BALANCE < 0
         AND B.TOTAL <> 0
         AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
         AND AP.AGENT_ID = AG.CONTRACTOR_ID(+)
         AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
GROUP BY A.ACCOUNT_NO, BR.CONTRACTOR, AG.CONTRACTOR, b.total)
--WHERE SUM_DEB <> 0
ORDER BY BRAND, ACCOUNT_NO;    
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- Агрегированный отчет о покрытии платежами выставленных счетов за месяц (ЮЛ)
PROCEDURE Agr_bill_payments_jl(p_recordset OUT t_refc, p_rep_period_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Agr_bill_payments_jl';
    v_retcode    INTEGER;  
BEGIN
  OPEN p_recordset FOR
SELECT * FROM(
SELECT BR.CONTRACTOR BRAND,
         AG.CONTRACTOR AGENT,         
         A.ACCOUNT_NO,
         b.total SUM_BILL,
         decode(SUM (PT.TRANSFER_TOTAL), null, 0, (SUM (PT.TRANSFER_TOTAL))) SUM_RAZ,
         decode(b.total - SUM (PT.TRANSFER_TOTAL), null, b.total, b.total - SUM (PT.TRANSFER_TOTAL)) SUM_DEB
    FROM bill_t b,
         account_t a,
         ACCOUNT_PROFILE_T AP,
         pay_transfer_t pt,
         CONTRACTOR_T BR,
         CONTRACTOR_T AG
   WHERE     B.REP_PERIOD_ID = p_rep_period_id
         AND B.ACCOUNT_ID = A.ACCOUNT_ID
         AND AP.ACCOUNT_ID = A.ACCOUNT_ID
         AND B.BILL_ID = PT.BILL_ID(+)
         AND A.ACCOUNT_TYPE = 'J'
         AND A.Billing_Id =PK00_CONST.c_BILLING_MMTS -- <> PK00_CONST.c_BILLING_OLD
         --AND A.BALANCE < 0
         AND B.TOTAL <> 0
         AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
         AND AP.AGENT_ID = AG.CONTRACTOR_ID(+)
GROUP BY A.ACCOUNT_NO, BR.CONTRACTOR, AG.CONTRACTOR, b.total)
--WHERE SUM_DEB <> 0
ORDER BY BRAND, ACCOUNT_NO;    
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-----------------------------------------------------
-- Получить список Клиентов по префиксу
-----------------------------------------------------
PROCEDURE GetClientsByPrefix(p_recordset OUT t_refc, p_prefix IN VARCHAR2, p_contractor_list IN VARCHAR2)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GetClientsByPrefix';
    v_retcode       INTEGER;   
    v_sql VARCHAR2(4000);
BEGIN
    v_sql := 
    'SELECT DISTINCT CS.CUSTOMER, CS.INN 
        FROM customer_t cs, account_profile_t ap, contractor_t c
       WHERE     (C.CONTRACTOR_ID = AP.CONTRACTOR_ID
                  OR C.CONTRACTOR_ID = AP.AGENT_ID
                  OR C.CONTRACTOR_ID = AP.BRANCH_ID)
             AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
             AND (C.CONTRACTOR_ID in (' || p_contractor_list || ') OR C.PARENT_ID in (' || p_contractor_list || '))
             AND LOWER (CUSTOMER) LIKE lower(''%' || p_prefix || '%'') 
             order by CUSTOMER';

    INSERT INTO TMP_SQL_LOG(SQL) VALUES(v_sql);
    commit;

    OPEN p_recordset FOR v_sql;
      
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
--------------------------------------------
-- Акт сверки взаиморасчетов
--------------------------------------------
PROCEDURE Get_Akt(p_recordset OUT t_refc,
                  p_start_bill_date IN DATE, 
                  p_end_bill_date IN DATE,
                  p_contract_no IN VARCHAR2)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_Akt';
    v_retcode    INTEGER;  
BEGIN
   OPEN p_recordset FOR
   SELECT * FROM 
   (
   SELECT B.BILL_NO,
       TRUNC (B.BILL_DATE) BILL_DATE,
       B.TOTAL BILL_TOTAL,
       0 SUM_PAY,
       '' PAY_NUM,
       TO_DATE ('01.01.1970', 'dd.mm.yyyy') PAY_DATE
  FROM bill_t b,
       account_t a,
       account_profile_t ap,
       PAY_TRANSFER_T pt,
       PAYMENT_T p
 WHERE     B.REP_PERIOD_ID BETWEEN PK04_PERIOD.Period_id (p_start_bill_date)
                               AND PK04_PERIOD.Period_id (p_end_bill_date)
       AND B.ACCOUNT_ID = A.ACCOUNT_ID
       AND A.ACCOUNT_TYPE = 'J'
       AND AP.ACCOUNT_ID = A.ACCOUNT_ID
       AND AP.CONTRACT_ID =
              (SELECT MAX (CONTRACT_ID)
                 FROM CONTRACT_T
                WHERE CONTRACT_NO = p_contract_no)
       AND PT.BILL_ID(+) = B.BILL_ID
       AND P.PAYMENT_ID(+) = PT.PAYMENT_ID
       AND TRUNC (b.BILL_DATE) >= p_start_bill_date
       AND TRUNC (b.BILL_DATE) <=p_end_bill_date
       AND B.TOTAL <> 0
  GROUP BY B.BILL_NO,
         TRUNC (B.BILL_DATE),
         B.TOTAL ,
         0 ,
         '' ,
         TO_DATE ('01.01.1970', 'dd.mm.yyyy')       
UNION ALL
SELECT '' BILL_NO,
       TRUNC (P.PAYMENT_DATE),
       0,
       P.RECVD SUM_PAY,
       NVL(SUBSTR (P.DOC_ID, 1, INSTR (P.DOC_ID, '-') - 1), '-') PAY_NUM,
       TRUNC (P.PAYMENT_DATE) PAY_DATE
  FROM account_t a, ACCOUNT_PROFILE_T AP, PAYMENT_T p
 WHERE     P.REP_PERIOD_ID BETWEEN PK04_PERIOD.Period_id (p_start_bill_date)
                               AND PK04_PERIOD.Period_id (p_end_bill_date)
       AND A.ACCOUNT_TYPE = 'J'
       AND AP.ACCOUNT_ID = A.ACCOUNT_ID
       AND AP.CONTRACT_ID = (SELECT MAX (CONTRACT_ID)
                               FROM CONTRACT_T
                              WHERE CONTRACT_NO = p_contract_no)
       AND (AP.DATE_TO is null or AP.DATE_TO = TO_DATE('01.01.2050', 'dd.mm.yyyy'))
       AND P.ACCOUNT_ID = AP.ACCOUNT_ID
       AND TRUNC (p.PAYMENT_DATE) >= p_start_bill_date
       AND TRUNC (p.PAYMENT_DATE) <= p_end_bill_date
       ) ORDER BY BILL_DATE;            
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- для ЛС
PROCEDURE Get_Akt_account(p_recordset OUT t_refc,
                  p_start_bill_date IN DATE, 
                  p_end_bill_date IN DATE,
                  p_account_no IN VARCHAR2)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_Akt_account';
    v_retcode    INTEGER;  
BEGIN
   OPEN p_recordset FOR
   SELECT * FROM 
   (
   SELECT B.BILL_NO,
       TRUNC (B.BILL_DATE) BILL_DATE,
       B.TOTAL BILL_TOTAL,
       0 SUM_PAY,
       '' PAY_NUM,
       TO_DATE ('01.01.1970', 'dd.mm.yyyy') PAY_DATE
  FROM bill_t b,
       account_t a,
       account_profile_t ap,
       PAY_TRANSFER_T pt,
       PAYMENT_T p
 WHERE     B.REP_PERIOD_ID BETWEEN PK04_PERIOD.Period_id (p_start_bill_date)
                               AND PK04_PERIOD.Period_id (p_end_bill_date)
       AND B.ACCOUNT_ID = A.ACCOUNT_ID
       AND A.ACCOUNT_TYPE = 'J'
       AND AP.ACCOUNT_ID = A.ACCOUNT_ID
       AND AP.ACCOUNT_ID =
              (SELECT MAX (ACCOUNT_ID)
                 FROM ACCOUNT_T
                WHERE ACCOUNT_NO = p_account_no)
       AND PT.BILL_ID(+) = B.BILL_ID
       AND P.PAYMENT_ID(+) = PT.PAYMENT_ID
       AND TRUNC (b.BILL_DATE) >= p_start_bill_date
       AND TRUNC (b.BILL_DATE) <=p_end_bill_date
       AND B.TOTAL <> 0
UNION
SELECT '' BILL_NO,
       TRUNC (P.PAYMENT_DATE),
       0,
       P.RECVD SUM_PAY,
       SUBSTR (P.DOC_ID, 1, INSTR (P.DOC_ID, '-') - 1) PAY_NUM,
       TRUNC (P.PAYMENT_DATE) PAY_DATE
  FROM account_t a, ACCOUNT_PROFILE_T AP, PAYMENT_T p
 WHERE     P.REP_PERIOD_ID BETWEEN PK04_PERIOD.Period_id (p_start_bill_date)
                               AND PK04_PERIOD.Period_id (p_end_bill_date)
       AND A.ACCOUNT_TYPE = 'J'
       AND AP.ACCOUNT_ID = A.ACCOUNT_ID
       AND AP.ACCOUNT_ID = (SELECT MAX (ACCOUNT_ID)
                               FROM ACCOUNT_T
                              WHERE ACCOUNT_NO = p_account_no)
       AND P.ACCOUNT_ID = AP.ACCOUNT_ID
       AND TRUNC (p.PAYMENT_DATE) >= p_start_bill_date
       AND TRUNC (p.PAYMENT_DATE) <= p_end_bill_date
       ) ORDER BY BILL_DATE;            
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
------------------------------------------------
-- проверить, доступен ли Договор пользователю
------------------------------------------------
FUNCTION Check_Contract_to_view(p_contractor_list IN VARCHAR2, p_contract_no IN VARCHAR2) RETURN INTEGER
IS
    v_count      INTEGER := 0;
    --v_sql        VARCHAR2(10000);
BEGIN                   
    execute immediate ' SELECT COUNT (C.CONTRACT_NO) 
                      FROM contract_t c, 
                           account_profile_t ap, 
                           contractor_t ct, 
                           account_t a 
                      WHERE     C.CONTRACT_ID = AP.CONTRACT_ID 
                           AND (AP.BRANCH_ID = CT.CONTRACTOR_ID OR AP.AGENT_ID = CT.CONTRACTOR_ID) 
                           AND (   CT.CONTRACTOR_ID IN (' || p_contractor_list || ') 
                                OR CT.PARENT_ID IN (' || p_contractor_list || ')  ) 
                           AND A.ACCOUNT_ID = AP.ACCOUNT_ID 
                           AND A.ACCOUNT_TYPE = ''' || PK00_CONST.c_ACC_TYPE_J || ''' 
                           AND C.CONTRACT_NO = ''' ||  p_contract_no || ''''  into v_count;
    RETURN v_count;
END Check_Contract_to_view;

---------------------------------------------------------------------------------
-- проверить, доступен ли ЛС  пользователю (для формирования акта сверки по ЛС)
---------------------------------------------------------------------------------
FUNCTION Check_Account_to_view(p_contractor_list IN VARCHAR2, p_account_no IN VARCHAR2) RETURN INTEGER
IS
    v_count      INTEGER := 0;
BEGIN                   
    execute immediate ' SELECT COUNT (A.ACCOUNT_NO) 
                      FROM account_profile_t ap, 
                           contractor_t ct, 
                           account_t a 
                      WHERE A.ACCOUNT_ID = AP.ACCOUNT_ID 
                           AND (AP.BRANCH_ID = CT.CONTRACTOR_ID OR AP.AGENT_ID = CT.CONTRACTOR_ID) 
                           AND (   CT.CONTRACTOR_ID IN (' || p_contractor_list || ') 
                                OR CT.PARENT_ID IN (' || p_contractor_list || ')  ) 
                           AND A.ACCOUNT_TYPE = ''' || PK00_CONST.c_ACC_TYPE_J || ''' 
                           AND A.ACCOUNT_NO = ''' ||  p_account_no || ''''  into v_count;
    RETURN v_count;
END;


------------------------------------------------
-- откуда формировать акт сверки? 
-- 1 или больше - BRM 
-- 0 - 1C 
------------------------------------------------
FUNCTION Check_where_act(p_contract_no IN VARCHAR2) RETURN INTEGER
IS
    v_count      INTEGER := 0;
BEGIN           
  
  execute immediate ' SELECT COUNT (C.CONTRACT_NO) 
                      FROM contract_t c, 
                           account_profile_t ap, 
                           account_t a 
                      WHERE     C.CONTRACT_ID = AP.CONTRACT_ID 
                           AND A.ACCOUNT_ID = AP.ACCOUNT_ID 
                           AND A.ACCOUNT_TYPE = ''' || PK00_CONST.c_ACC_TYPE_J || ''' 
                           AND C.CONTRACT_NO =  ''' ||  p_contract_no || '''
                           and a.billing_id = 2003 and a.status =  ''' || PK00_CONST.c_ACC_STATUS_BILL || ''' '  into v_count;   
    RETURN v_count;
END Check_where_act;

-- для ЛС!!
FUNCTION Check_where_act_account(p_account_no IN VARCHAR2) RETURN INTEGER
IS
    v_count      INTEGER := 0;
BEGIN           
  
  execute immediate ' SELECT COUNT (A.ACCOUNT_NO) 
                      FROM account_profile_t ap, 
                           account_t a 
                      WHERE A.ACCOUNT_ID = AP.ACCOUNT_ID 
                           AND A.ACCOUNT_TYPE = ''' || PK00_CONST.c_ACC_TYPE_J || ''' 
                           AND A.ACCOUNT_NO =  ''' ||  p_account_no || '''
                           and a.billing_id = 2003' into v_count;   
    RETURN v_count;
END;


--------------------------------------------
-- Акт сверки взаиморасчетов НОВЫЙ !!!! UPD. Неправильный !!! (((
--------------------------------------------
/*PROCEDURE Get_Akt_new(p_recordset OUT t_refc,
                  p_start_bill_date IN DATE, 
                  p_end_bill_date IN DATE,
                  p_contract_no IN VARCHAR2)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_Akt_new';
    v_retcode    INTEGER;  
BEGIN
   OPEN p_recordset FOR
   SELECT B.BILL_NO,
       TRUNC (B.BILL_DATE) BILL_DATE,
       B.TOTAL BILL_TOTAL,
       0 SUM_PAY,
       '' PAY_NUM,
       TO_DATE ('01.01.1970', 'dd.mm.yyyy') PAY_DATE
  FROM bill_t b,
       account_t a,
       account_profile_t ap,
       PAY_TRANSFER_T pt,
       PAYMENT_T p
 WHERE     B.REP_PERIOD_ID BETWEEN PK04_PERIOD.Period_id (p_start_bill_date)
                               AND PK04_PERIOD.Period_id (p_end_bill_date)
       AND B.ACCOUNT_ID = A.ACCOUNT_ID
       AND A.ACCOUNT_TYPE = 'J'
       AND AP.ACCOUNT_ID = A.ACCOUNT_ID
       AND AP.CONTRACT_ID =
              (SELECT MAX (CONTRACT_ID)
                 FROM CONTRACT_T
                WHERE CONTRACT_NO = p_contract_no)
       AND PT.BILL_ID(+) = B.BILL_ID
       AND P.PAYMENT_ID(+) = PT.PAYMENT_ID
       AND TRUNC (b.BILL_DATE) >= p_start_bill_date
       AND TRUNC (b.BILL_DATE) <= p_end_bill_date
       AND B.TOTAL <> 0
UNION
SELECT B.BILL_NO,
       TO_DATE ('01.01.2050', 'dd.mm.yyyy'),
       0,
       PT.TRANSFER_TOTAL SUM_PAY,
       SUBSTR (P.DOC_ID, 1, INSTR (P.DOC_ID, '-') - 1) PAY_NUM,
       TRUNC (P.PAYMENT_DATE) PAY_DATE
  FROM bill_t b,
       account_t a,
       account_profile_t ap,
       PAY_TRANSFER_T pt,
       PAYMENT_T p
 WHERE     B.REP_PERIOD_ID BETWEEN PK04_PERIOD.Period_id (p_start_bill_date)
                               AND PK04_PERIOD.Period_id (p_end_bill_date)
       AND B.ACCOUNT_ID = A.ACCOUNT_ID
       AND A.ACCOUNT_TYPE = 'J'
       AND AP.ACCOUNT_ID = A.ACCOUNT_ID
      AND AP.CONTRACT_ID =
              (SELECT MAX (CONTRACT_ID)
                 FROM CONTRACT_T
                WHERE CONTRACT_NO = p_contract_no)
       AND PT.BILL_ID = B.BILL_ID(+)
       AND P.PAYMENT_ID = PT.PAYMENT_ID(+)
       AND TRUNC (b.BILL_DATE) >= p_start_bill_date
       AND TRUNC (b.BILL_DATE) <= p_end_bill_date
       AND B.TOTAL <> 0;
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;*/

---------------------------------------------
-- Получить входящее сальдо (для акта сверки)
---------------------------------------------
FUNCTION GetIncomeBalance (p_start_bill_date IN DATE, p_contract_no VARCHAR2) RETURN NUMBER
IS
    v_prcName             CONSTANT VARCHAR2(30) := 'GetIncomeBalance';
    v_income_balance      NUMBER;
    v_income_bill_total   NUMBER;
    v_income_pay_total   NUMBER;
BEGIN
       select 
        case when sum(B.TOTAL) is null then 0 
        else sum(B.TOTAL) end       
       --sum(B.TOTAL) 
       into v_income_bill_total
       from bill_t b
       where TRUNC (b.BILL_DATE) < p_start_bill_date
       and B.ACCOUNT_ID IN
              (SELECT account_id
                 FROM account_profile_t
                WHERE contract_id IN (SELECT contract_id
                                        FROM contract_t
                                       WHERE contract_no = p_contract_no));
       
       
       
       select 
            case when sum(P.RECVD) is null then 0 
            else sum(P.RECVD) end
            into v_income_pay_total
       from payment_t p, account_profile_t ap, contract_t c
       where C.CONTRACT_NO = p_contract_no
       and p_start_bill_date between ap.date_from and nvl(ap.date_to, to_date('01.01.2050', 'dd.mm.yyyy'))
       and C.CONTRACT_ID = AP.CONTRACT_ID
       and AP.ACCOUNT_ID = P.ACCOUNT_ID
       and P.PAYMENT_DATE < p_start_bill_date;      
       
       
       v_income_balance := v_income_bill_total - v_income_pay_total;

    RETURN v_income_balance;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

---------------------------------------------
-- Получить входящее сальдо по ЛС (для акта сверки)
---------------------------------------------
FUNCTION GetIncomeBalanceAcc (p_start_bill_date IN DATE, p_account_no VARCHAR2) RETURN NUMBER
IS
    v_prcName             CONSTANT VARCHAR2(30) := 'GetIncomeBalanceAcc';
    v_income_balance      NUMBER;
    v_income_bill_total   NUMBER;
    v_income_pay_total   NUMBER;
BEGIN
       select 
        case when sum(B.TOTAL) is null then 0 
        else sum(B.TOTAL) end       
       --sum(B.TOTAL) 
       into v_income_bill_total
       from bill_t b
       where TRUNC (b.BILL_DATE) < p_start_bill_date
       and B.ACCOUNT_ID IN
              (SELECT account_id
                 FROM account_t
                WHERE account_no = p_account_no);
       
       
       select 
            case when sum(P.RECVD) is null then 0 
            else sum(P.RECVD) end
            into v_income_pay_total
       from payment_t p, account_profile_t ap, account_t a
       where a.ACCOUNT_NO = p_account_no
       and p_start_bill_date between ap.date_from and nvl(ap.date_to, to_date('01.01.2050', 'dd.mm.yyyy'))
       and A.ACCOUNT_ID = AP.ACCOUNT_ID
       and AP.ACCOUNT_ID = P.ACCOUNT_ID
       and P.PAYMENT_DATE < p_start_bill_date;      
       
       
       v_income_balance := v_income_bill_total - v_income_pay_total;

    RETURN v_income_balance;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--------------------------------------------
-- Выписка по тел. номеру ФЛ
--------------------------------------------
PROCEDURE Get_Akt_FL(p_recordset OUT t_refc,
                  p_start_bill_date IN DATE, 
                  p_end_bill_date IN DATE,
                  p_phone IN VARCHAR2)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_Akt_FL';
    v_retcode    INTEGER;  
BEGIN
   OPEN p_recordset FOR
      SELECT TRUNC(B.BILL_DATE) EVENT_DATE, '1' EVENT_TYPE,
              B.TOTAL BILL_TOTAL, 0 PAY_TOTAL,
              A.ACCOUNT_NO 
         FROM bill_t b,
              account_t a,
              order_t o,
              ORDER_PHONES_T op
        WHERE     B.ACCOUNT_ID = A.ACCOUNT_ID
              AND A.ACCOUNT_TYPE = 'P'
              AND B.REP_PERIOD_ID BETWEEN PK04_PERIOD.Period_id(p_start_bill_date) AND PK04_PERIOD.Period_id(p_end_bill_date)
              AND TRUNC (b.BILL_DATE) >= p_start_bill_date
              AND TRUNC (b.BILL_DATE) <= p_end_bill_date
              AND B.TOTAL <> 0
              AND O.ACCOUNT_ID = A.ACCOUNT_ID
              AND O.ORDER_ID = OP.ORDER_ID
              AND OP.PHONE_NUMBER = p_phone
      UNION
        SELECT TRUNC(P.PAYMENT_DATE) EVENT_DATE, '2' EVENT_TYPE,
                0 BILL_TOTAL, P.recvd PAY_TOTAL,
                A.ACCOUNT_NO
           FROM bill_t b,
                account_t a,
                order_t o,
                ORDER_PHONES_T op,
                PAYMENT_T p,
                PAY_TRANSFER_T pt
          WHERE     B.ACCOUNT_ID = A.ACCOUNT_ID
                AND A.ACCOUNT_TYPE = 'P'
                AND B.REP_PERIOD_ID BETWEEN PK04_PERIOD.Period_id(p_start_bill_date) AND PK04_PERIOD.Period_id(p_end_bill_date)
                AND TRUNC (b.BILL_DATE) >= p_start_bill_date
                AND TRUNC (b.BILL_DATE) <= p_end_bill_date                
                AND O.ACCOUNT_ID = A.ACCOUNT_ID
                AND O.ORDER_ID = OP.ORDER_ID
                AND OP.PHONE_NUMBER = p_phone
                AND P.PAYMENT_ID = PT.PAYMENT_ID
                AND PT.BILL_ID = B.BILL_ID
      ;
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
-----------------------------------------------------------------------
-- Получить наименование Клиента по номеру Договора (для акта сверки)
-------------------------------------------------------------------------
FUNCTION Get_Customer_By_Contract(p_conract_no IN VARCHAR2) RETURN VARCHAR2
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Get_Customer_By_Contract';
    v_customer_name VARCHAR2(1000);
BEGIN
    SELECT distinct CST.CUSTOMER into v_customer_name
      FROM contract_t c, account_profile_t ap, customer_t cst
     WHERE     CST.CUSTOMER_ID = AP.CUSTOMER_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND C.CONTRACT_NO = p_conract_no
           and rownum = 1;
    RETURN v_customer_name;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR : ' || p_conract_no, c_PkgName||'.'||v_prcName );   
END Get_Customer_By_Contract;

-----------------------------------------------------------------------
-- Получить наименование Клиента по номеру ЛС (для акта сверки)
-------------------------------------------------------------------------
FUNCTION Get_Customer_By_Account(p_account_no IN VARCHAR2) RETURN VARCHAR2
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Get_Customer_By_Account';
    v_customer_name VARCHAR2(1000);
BEGIN
  SELECT DISTINCT CST.CUSTOMER
    INTO v_customer_name
    FROM account_t a, account_profile_t ap, customer_t cst
   WHERE     CST.CUSTOMER_ID = AP.CUSTOMER_ID
         AND AP.ACCOUNT_ID = A.ACCOUNT_ID
         AND A.ACCOUNT_NO = p_account_no
         AND ROWNUM = 1;
    RETURN v_customer_name;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR : ' || p_account_no, c_PkgName||'.'||v_prcName );   
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Список должников ФИЗИЧЕСКИХ лиц, для блокировки автоматом (25 числа каждого месяца)
--   - при ошибке выставляет исключение
PROCEDURE Subs_debitors_for_lock_auto( 
               p_recordset      OUT t_refc,
               p_rep_reriod_id  IN NUMBER   -- период
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Subs_debitors_for_lock_auto';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        WITH ABN
             AS (SELECT A.ACCOUNT_ID,
                        A.BALANCE,
                        A.ACCOUNT_NO,
                        DECODE (A.STATUS,
                                'B', 'Активный',
                                'C', 'Закрытый')
                           ACCOUNT_STATUS,
                        C.CONTRACT_NO,
                        C.CLIENT_TYPE_ID,                                      
                        BR.SHORT_NAME BRAND_NAME,                                             
                        decode(AG.SHORT_NAME, null, '.',AG.SHORT_NAME) AGENT_NAME,      
                        O.ORDER_ID,O.ORDER_NO, 
                        O.STATUS ORDER_STATUS,
                        AP.BRANCH_ID,
                        AP.AGENT_ID                        
                   FROM ACCOUNT_T A,           
                        ACCOUNT_PROFILE_T AP,
                        CONTRACT_T C,
                        CONTRACTOR_T BR,
                        CONTRACTOR_T AG,
                        ORDER_T O                
                  WHERE A.ACCOUNT_TYPE = 'P'
                        AND A.ACCOUNT_ID = AP.ACCOUNT_ID
                        AND (AP.DATE_TO IS NULL
                             OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
                        AND AP.CONTRACT_ID = C.CONTRACT_ID
                        AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
                        AND AP.AGENT_ID = AG.CONTRACTOR_ID(+)
                        AND O.ACCOUNT_ID = a.ACCOUNT_ID
                        AND O.SERVICE_ID = Pk00_CONST.c_SERVICE_CALL_MGMN
                        ), 
             BL
             AS (  SELECT B.ACCOUNT_ID, MIN (B.BILL_DATE) DEB_BILL_DATE
                     FROM BILL_T B
                    WHERE B.DUE < 0
                    AND B.REP_PERIOD_ID < p_rep_reriod_id
                 GROUP BY B.ACCOUNT_ID),
             B_CUR
             AS (  SELECT B.DUE, B.ACCOUNT_ID
                     FROM BILL_T B
                     WHERE B.REP_PERIOD_ID =  p_rep_reriod_id
                 ),
             LK
             AS (  SELECT O.ACCOUNT_ID,
                          SUM (DECODE (O.STATUS, 'LOCK', 1, 0)) LOCKS_NUM, 
                          COUNT (*) ORDERS_NUM,
                          O.TIME_ZONE
                     FROM ORDER_T O
                    WHERE (   O.DATE_TO IS NULL
                           OR SYSDATE BETWEEN O.DATE_FROM AND O.DATE_TO)
                 GROUP BY O.ACCOUNT_ID, O.TIME_ZONE)
          SELECT distinct ABN.ACCOUNT_ID Id,
                 ABN.ACCOUNT_NO,
                 ABN.ACCOUNT_STATUS,
                 ABN.ORDER_ID,
                 ABN.ORDER_NO,
                 ABN.ORDER_STATUS,         
                 ABN.BALANCE + (-1) * NVL(B_CUR.DUE, 0) sum_debt
            FROM ABN, LK, BL , B_CUR
           WHERE     ABN.ACCOUNT_ID = BL.ACCOUNT_ID
                 AND ABN.ACCOUNT_ID = LK.ACCOUNT_ID
                 AND ABN.ACCOUNT_ID = B_CUR.ACCOUNT_ID (+)
                 AND ABN.BALANCE + (-1) * NVL(B_CUR.DUE, 0) < -100
                 AND LK.LOCKS_NUM = 0            
                 --AND (p_rep_reriod_id - PK04_PERIOD.Period_id(BL.DEB_BILL_DATE)) = 1
                 AND MONTHS_BETWEEN(to_date(p_rep_reriod_id,'YYYYmm'), to_date(PK04_PERIOD.Period_id(BL.DEB_BILL_DATE),'YYYYmm')) >= 1
                 and LK.TIME_ZONE is not null
--                 AND (ABN.BRANCH_ID NOT IN (27, 113, 84, 1530129) AND (ABN.AGENT_ID NOT IN (27, 113, 84, 1530129) OR ABN.AGENT_ID IS NULL))
                 AND (ABN.BRANCH_ID NOT IN (27) AND (ABN.AGENT_ID NOT IN (27) OR ABN.AGENT_ID IS NULL))
                 AND (ABN.CLIENT_TYPE_ID is null or  ABN.CLIENT_TYPE_ID <> 6409) --Исключим на всякий случай VIP-ов
                 order by ABN.ACCOUNT_ID
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
-- корр. счет-фактура
---------------------------------------------------------------

---------------------------------------------------------------------
-- Дебиторка более 3-х лет (Отчет Списание дебиторской задолженности)
---------------------------------------------------------------------
PROCEDURE Get_DZ_JL_36(p_recordset OUT t_refc)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Get_DZ_JL_36';
    v_retcode    INTEGER;  
BEGIN
   OPEN p_recordset FOR
      WITH LK AS (
            SELECT O.ACCOUNT_ID, 
                   SUM(
                      CASE WHEN (select COUNT(ORDER_ID) from order_lock_t where LOCK_type_ID <> 901 and order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED') is null THEN 0
                      ELSE (select COUNT(ORDER_ID) from order_lock_t where LOCK_type_ID <> 901 and order_id = o.order_id  and date_to is null and O.STATUS <> 'CLOSED') END
                   --DECODE(O.STATUS, 'LOCK',1,0)
                   ) LOCKS_NUM,
                   SUM(DECODE(O.STATUS, 'CLOSED',1,0)) CLOSED_NUM,
                   COUNT(*) ORDERS_NUM
              FROM ORDER_T O
            GROUP BY O.ACCOUNT_ID
      )
      SELECT max(CUST.CUSTOMER) CUSTOMER,
           max(CUST.INN) INN,
           max(-1 * B.DUE) AS DUE,
           max(TRUNC (B.BILL_DATE)) BILL_DATE,
           B.BILL_NO,
           CONT.CONTRACT_NO,
           DECODE(SUM(LK.LOCKS_NUM), null, 'Неактивный', 
                       CASE
                           WHEN SUM(LK.LOCKS_NUM) = 0 AND SUM(LK.LOCKS_NUM + LK.CLOSED_NUM) < SUM(LK.ORDERS_NUM) THEN 'Активный'
                           WHEN SUM(LK.LOCKS_NUM) > 0 AND SUM(LK.LOCKS_NUM + LK.CLOSED_NUM) = SUM(LK.ORDERS_NUM) THEN 'Заблокирован'
                           WHEN SUM(LK.LOCKS_NUM) > 0 AND SUM(LK.LOCKS_NUM + LK.CLOSED_NUM) < SUM(LK.ORDERS_NUM) THEN 'Активный' --'Частично заблокирован'
                           WHEN SUM(LK.CLOSED_NUM) = SUM(LK.ORDERS_NUM) THEN 'Закрыт'
                       END) STATUS,
            CTR.CONTRACTOR CONTRACTOR                        
      FROM bill_t b,
           account_profile_t ap,
           account_t a,
           contract_t cont,
           customer_t cust, 
           LK,
           contractor_t ctr
      WHERE A.ACCOUNT_ID = B.ACCOUNT_ID
           AND A.ACCOUNT_TYPE = 'J'
           AND AP.ACCOUNT_ID = B.ACCOUNT_ID
           AND CONT.CONTRACT_ID = AP.CONTRACT_ID
           AND CUST.CUSTOMER_ID = AP.CUSTOMER_ID
           AND CTR.CONTRACTOR_ID = AP.BRANCH_ID
           AND b.due < 0
           AND B.BILL_DATE < ADD_MONTHS (TRUNC (SYSDATE, 'month') - 1, -36)
           AND B.ACCOUNT_ID = LK.ACCOUNT_ID (+)
           AND A.BILLING_ID = 2003
           AND A.BALANCE < 0
           AND LK.LOCKS_NUM is not null
           and (AP.DATE_TO is null or AP.DATE_TO > SYSDATE)
           GROUP BY CONTRACT_NO, BILL_NO, CTR.CONTRACTOR
      ORDER BY CONTRACT_NO, bill_date;
EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ----------------------------------------------------------------------- --
-- 'Оборотно сальдовая ведомость'
-- ----------------------------------------------------------------------- --
PROCEDURE DZ_JL_OBOROTY(
               p_recordset OUT t_refc,
               p_period_id IN INTEGER,
               p_branch_id IN INTEGER,
               p_agent_id  IN INTEGER DEFAULT NULL
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'DZ_JL_OBOROTY';
    v_retcode    INTEGER;  
BEGIN
   OPEN p_recordset FOR
      WITH IDB AS (
          SELECT B.ACCOUNT_ID, SUM(B.TOTAL) IN_BILL_TOTAL
            FROM BILL_T B
           WHERE B.REP_PERIOD_ID < p_period_id
           GROUP BY B.ACCOUNT_ID
      ), ICR AS (
          SELECT P.ACCOUNT_ID, SUM(P.RECVD) IN_PAY_TOTAL 
            FROM PAYMENT_T P 
           WHERE P.REP_PERIOD_ID < p_period_id
           GROUP BY P.ACCOUNT_ID
      ), BIL AS (
          SELECT B.ACCOUNT_ID, SUM(B.TOTAL) BILL_TOTAL
            FROM BILL_T B 
           WHERE B.REP_PERIOD_ID = p_period_id
           GROUP BY B.ACCOUNT_ID
      ), PAY AS (
          SELECT P.ACCOUNT_ID, SUM(P.RECVD) PAY_TOTAL 
            FROM PAYMENT_T P 
           WHERE P.REP_PERIOD_ID = p_period_id
           GROUP BY P.ACCOUNT_ID
      )
      SELECT CM.COMPANY_NAME,                    -- Клиент
             A.ACCOUNT_NO,                       -- Лицевой счет
             --A.ACCOUNT_ID,  
             NVL(IDB.IN_BILL_TOTAL, 0) IN_DEBET, -- Начальное сальдо (дебет)
             NVL(ICR.IN_PAY_TOTAL, 0) IN_CREDIT, -- Начальное сальдо (кредит)
             NVL(BIL.BILL_TOTAL, 0) BILL_TOTAL,  -- Оборот (дебет)
             NVL(PAY.PAY_TOTAL, 0) PAY_TOTAL,    -- Оборот (кредит)
             NVL(IDB.IN_BILL_TOTAL, 0) + NVL(BIL.BILL_TOTAL, 0) OUT_DEBET, -- Конечное сальдо (дебет)
             NVL(ICR.IN_PAY_TOTAL, 0) + NVL(PAY.PAY_TOTAL, 0) OUT_CREDIT   -- Конечное сальдо (кредит)
        FROM ACCOUNT_T A, IDB, ICR, BIL, PAY,
             ACCOUNT_PROFILE_T AP,
             COMPANY_T CM,
             PERIOD_T P 
       WHERE A.ACCOUNT_TYPE = 'J'
         AND a.status <> 'T'
         AND A.ACCOUNT_ID   = IDB.ACCOUNT_ID(+)
         AND A.ACCOUNT_ID   = ICR.ACCOUNT_ID(+)
         AND A.ACCOUNT_ID   = BIL.ACCOUNT_ID(+)
         AND A.ACCOUNT_ID   = PAY.ACCOUNT_ID(+)
         AND A.ACCOUNT_ID   = AP.ACCOUNT_ID
         AND P.PERIOD_ID    = p_period_id
         AND AP.DATE_FROM   < P.PERIOD_TO
         AND (
                CASE 
                  WHEN AP.DATE_TO IS NOT NULL AND AP.DATE_TO > P.PERIOD_FROM THEN 1
                  WHEN AP.DATE_TO IS NULL THEN 1
                  ELSE 0
                END
             ) = 1
         AND AP.CONTRACT_ID = CM.CONTRACT_ID
         AND CM.DATE_FROM   < P.PERIOD_TO
         AND (
                CASE 
                  WHEN CM.DATE_TO IS NOT NULL AND CM.DATE_TO > P.PERIOD_FROM THEN 1
                  WHEN CM.DATE_TO IS NULL THEN 1
                  ELSE 0
                END
             ) = 1
         AND AP.BRANCH_ID   = p_branch_id
         AND ( AP.AGENT_ID  = p_agent_id OR p_agent_id IS NULL)
       ORDER BY CM.COMPANY_NAME, A.ACCOUNT_NO;      

EXCEPTION        
    WHEN OTHERS THEN
       v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK400_DEBITORS_BAK;
/
