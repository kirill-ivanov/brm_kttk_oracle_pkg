CREATE OR REPLACE PACKAGE PK50_W_ACCOUNT_INFO
IS
    --
    -- Пакет получения данных по лицевому счету (физика + юрики)
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK50_W_ACCOUNT_INFO';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- Поиск лицевых счетов для физика
    -- Поиск ищется как по полном совпадению, так и НЕполному (если указать *) 
    PROCEDURE ACCOUNT_SEARCH_F( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,      -- ID лицевого счета         
         p_account_no    IN VARCHAR2,    -- лицевой счет
         p_contract_no   IN VARCHAR2,    -- номер контракта
         p_order_no      IN VARCHAR2,    -- номер заказ, который должен быть в л/с         
         p_date_from     IN DATE,        -- дата создания с
         p_date_to       IN DATE,        -- дата создания по  
         p_client_f      IN VARCHAR2,    -- фамилия клиента
         p_client_n      IN VARCHAR2,    -- имя клиента
         p_client_p      IN VARCHAR2,    -- отчество клиента
         p_phone_number  IN VARCHAR2,     -- номер телефона
         p_contractors   IN VARCHAR2,      -- список плставщиков через запятую
         p_only_in_branch   IN INTEGER -- искать только в регионах 
    ); 
    
--=========================================================================================
--Поиск лицевых счетов для ФИЗИКА согласно кураторским права менеджера
PROCEDURE ACCOUNT_SEARCH_F_BY_MGR_RULE (  
      p_result          OUT VARCHAR2, 
      p_recordset       OUT t_refc, 
      p_account_id      IN NUMBER,      -- ID лицевого счета
      p_account_no      IN VARCHAR2,    -- лицевой счет
      p_contract_no     IN VARCHAR2,    -- номер контракта
      p_order_no        IN VARCHAR2,    -- номер заказ, который должен быть в л/с
      p_date_from       IN DATE,        -- дата создания с
      p_date_to         IN DATE,        -- дата создания по  
      p_client_f        IN VARCHAR2,    -- фамилия клиента
      p_client_n        IN VARCHAR2,    -- имя клиента
      p_client_p        IN VARCHAR2,    -- отчество клиента
      p_phone_number    IN VARCHAR2,    -- номер телефона
      p_manager_id      IN INTEGER      -- менеджер ID (sale-куратор)
);

--====================================================================================   
-- Сначала определяем, какого типа лицевой счет (физик или юрик), а потом вытаскиваем нужные данные
    PROCEDURE ACCOUNT_INFO( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- лицевой счет
    );
--====================================================================================   
-- Получение детальной информации по л/с для физика
    PROCEDURE ACCOUNT_INFO_F( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- лицевой счет
    );

--====================================================================================    
    -- Поиск лицевых счетов для ЮРИКА
    -- Поиск ищется как по полном совпадению, так и НЕполному (если указать *) 
    PROCEDURE ACCOUNT_SEARCH_Y(  
      p_result        OUT VARCHAR2, 
      p_recordset     OUT t_refc, 
      p_account_id    IN NUMBER,      -- ID лицевого счета
      p_account_no    IN VARCHAR2,    -- лицевой счет
      p_contract_no   IN VARCHAR2,    -- номер контракта
      p_date_from     IN DATE,        -- дата создания с
      p_date_to       IN DATE,        -- дата создания по  
      p_order_no      IN VARCHAR2,    -- номер заказ, который должен быть в л/с      
      p_customer_name IN VARCHAR2,    -- название компании
      p_customer_inn  IN VARCHAR2,    -- ИНН компании
      p_customer_kpp  IN VARCHAR2,    -- КПП компании
      p_phone_number  IN VARCHAR2,    -- номер телефона
      p_contractors   IN VARCHAR2,      -- список плставщиков через запятую
      p_only_in_branch   IN INTEGER -- искать только в регионах    
);

--======================================================================================================================================
    -- Поиск лицевых счетов для ЮРИКА согласно кураторским права ме
    -- Поиск ищется как по полном совпадению, так и НЕполному (если указать *) 
    PROCEDURE ACCOUNT_SEARCH_Y_BY_MGR_RULE(  
      p_result        OUT VARCHAR2, 
      p_recordset     OUT t_refc, 
      p_account_id    IN NUMBER,      -- ID лицевого счета
      p_account_no    IN VARCHAR2,    -- лицевой счет
      p_contract_no   IN VARCHAR2,    -- номер контракта
      p_date_from     IN DATE,        -- дата создания с
      p_date_to       IN DATE,        -- дата создания по  
      p_order_no      IN VARCHAR2,    -- номер заказ, который должен быть в л/с      
      p_customer_name IN VARCHAR2,    -- название компании
      p_customer_inn  IN VARCHAR2,    -- ИНН компании
      p_customer_kpp  IN VARCHAR2,    -- КПП компании
      p_phone_number  IN VARCHAR2,    -- номер телефона
      p_manager_id    IN INTEGER      -- ID менеджера (sale-кураторы)
);

--================================================================================================================
-- Получение детальной информации по л/с для ЮРИКА
    PROCEDURE ACCOUNT_INFO_Y( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- лицевой счет
    );        

--==============================================================================================================
    
    -- Поиск контракта
    PROCEDURE SEARCH_CONTRACT_LIST( 
         p_recordset     OUT t_refc, 
         p_contract_no   IN VARCHAR2,    -- номер контракта
         p_date_from     IN DATE,
         p_date_to       IN DATE
    );
    
        -- Поиск поставщика
    PROCEDURE SEARCH_CUSTOMER_LIST( 
         p_recordset     OUT t_refc, 
         p_name          IN VARCHAR2,    -- имя поставщика
         p_inn           IN VARCHAR2,
         p_kpp           IN VARCHAR2
    );
    
    -- Поиск клиента
    PROCEDURE SEARCH_CLIENT_LIST( 
         p_recordset     OUT t_refc, 
         p_clientname    IN VARCHAR2
    );

--=======================================================================
-- Загрузка списка счетов физ. лиц по фильтру
--=======================================================================
procedure BILL_FIZ_SEARCH(
          p_recordset           OUT t_refc,
          p_rep_period_id       IN  INTEGER,
          p_account_no          IN  VARCHAR2,
          p_contractor_list_id  IN  VARCHAR2,
          p_contractor_only_branch IN VARCHAR2,
          p_show_bill0          IN  INTEGER,
          sort_by               IN  VARCHAR2 DEFAULT NULL
);
    
--=======================================================================
--Загрузка списка счетов по фильтру
procedure BILL_JUR_SEARCH(
          p_recordset           OUT t_refc,
          p_rep_period_id       IN  INTEGER,
          p_contract_no         IN  VARCHAR2,
          p_account_no          IN  VARCHAR2,
          p_bill_no             IN  VARCHAR2,
          p_bill_type           IN  VARCHAR2,
          p_bill_status         IN  VARCHAR2,          
          p_contractor_list_id  IN  VARCHAR2,
          p_contractor_only_branch IN VARCHAR2,
          p_show_bill0          IN  INTEGER,
          p_delivery_method_id  IN  INTEGER,
          sort_by               IN  VARCHAR2 DEFAULT NULL
); 

--=======================================================================
--Загрузка списка счетов по фильтру с учетом прав менеджера
procedure BILL_JUR_SEARCH_BY_MGR_RULE(
          p_recordset           OUT t_refc,
          p_rep_period_id       IN  INTEGER,
          p_contract_no         IN  VARCHAR2,
          p_account_no          IN  VARCHAR2,
          p_bill_no             IN  VARCHAR2,
          p_bill_type           IN  VARCHAR2,
          p_bill_status         IN  VARCHAR2,          
          p_manager_id          IN  INTEGER,
          p_show_bill0          IN  INTEGER,
          p_delivery_method_id  IN  INTEGER,
          sort_by               IN  VARCHAR2 DEFAULT NULL
); 

END PK50_W_ACCOUNT_INFO;
/
CREATE OR REPLACE PACKAGE BODY PK50_W_ACCOUNT_INFO
IS

-- Пакет WEB-интерфейсов
--=============================================================
    -- Поиск лицевых счетов для физика
    -- Поиск ищется как по полном совпадению, так и НЕполному (если указать *) 
PROCEDURE ACCOUNT_SEARCH_F(  
      p_result        OUT VARCHAR2, 
      p_recordset     OUT t_refc, 
      p_account_id    IN NUMBER,      -- ID лицевого счета
      p_account_no    IN VARCHAR2,    -- лицевой счет
      p_contract_no   IN VARCHAR2,    -- номер контракта
      p_order_no      IN VARCHAR2,    -- номер заказ, который должен быть в л/с
      p_date_from     IN DATE,        -- дата создания с
      p_date_to       IN DATE,        -- дата создания по  
      p_client_f      IN VARCHAR2,    -- фамилия клиента
      p_client_n      IN VARCHAR2,    -- имя клиента
      p_client_p      IN VARCHAR2,    -- отчество клиента
      p_phone_number  IN VARCHAR2,     -- номер телефона
      p_contractors   IN VARCHAR2,      -- список плставщиков через запятую
      p_only_in_branch   IN INTEGER -- искать только в регионах 
)
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_SEARCH_F';
    v_retcode            INTEGER;
    v_sql                VARCHAR2(10000);
    v_sql2               VARCHAR2(10000);
    v_sql_exec           VARCHAR2(10000);
    v_contractor_header  VARCHAR2(10000);
    v_date_temp          VARCHAR2(20);
    v_cnt                INTEGER;
BEGIN   
    -- Формируем SQL-запрос в строке
    v_sql := ' 
              FROM account_t acc,
                   account_profile_t prof,
                   contract_t c,
                   subscriber_t sub,
                   contractor_t contr,
                   account_contact_t ac,
                   dictionary_t dict';
                   
     IF p_phone_number IS NOT NULL THEN
        v_sql := v_sql || ', order_phones_t ph 
                          , order_t ord';
     ELSE
       IF p_order_no IS NOT NULL THEN                                
         v_sql := v_sql || ', order_t ord';         
       END IF;
     END IF;   
     
/*     IF p_contractors IS NOT NULL THEN
        v_sql := v_sql || ', brand b';
     END IF;  */
                                                                 
     v_sql := v_sql || ' WHERE acc.account_id = PROF.ACCOUNT_ID
                   AND C.CONTRACT_ID = PROF.CONTRACT_ID
                   AND SYSDATE BETWEEN prof.DATE_FROM AND NVL(prof.DATE_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                   AND SUB.SUBSCRIBER_ID = PROF.SUBSCRIBER_ID
                   AND CONTR.CONTRACTOR_ID = PROF.CONTRACTOR_ID
                   AND DICT.PARENT_ID = 2
                   AND DICT.KEY = ACC.STATUS
                   AND ac.account_id (+)= acc.account_id
                   AND AC.ADDRESS_TYPE (+)= ''DLV''
                   AND ACC.STATUS <> ''DBL''
                   AND acc.ACCOUNT_TYPE = ''P''';                                                             
    
    IF p_phone_number IS NOT NULL THEN
        v_sql := v_sql || 
                   ' AND ph.order_id = ord.order_id
                     AND ord.account_id = acc.account_id';
     ELSE
       IF p_order_no IS NOT NULL THEN                                
         v_sql := v_sql || ' AND ord.account_id = acc.account_id';
       END IF;
     END IF;     
    
    IF p_account_id IS NOT NULL THEN
      v_sql := v_sql || ' AND ACC.ACCOUNT_ID = ' || p_account_id;
    END IF;
    
    IF p_account_no IS NOT NULL THEN       
       v_sql := v_sql || ' AND ACC.ACCOUNT_NO';    
       IF INSTR(p_account_no,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || UPPER(REPLACE(p_account_no,'*','%'))||'''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_account_no || '''';
       END IF;
    END IF;
    
    IF p_contract_no IS NOT NULL THEN       
       v_sql := v_sql || ' AND C.CONTRACT_NO';    
       IF INSTR(p_contract_no,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || UPPER(REPLACE(p_contract_no,'*','%')) || '''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_contract_no || '''';
       END IF;
    END IF;
    
    IF p_order_no IS NOT NULL THEN       
       v_sql := v_sql || ' AND ord.ORDER_NO';    
       IF INSTR(p_order_no,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || UPPER(REPLACE(p_order_no,'*','%')) || '''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_order_no || '''';
       END IF;
    END IF;
    
    IF p_client_f IS NOT NULL THEN       
       v_sql := v_sql || ' AND UPPER(TRIM(SUB.LAST_NAME))';
       IF INSTR(p_client_f,'*') > 0 THEN
          v_sql := v_sql || ' LIKE UPPER('''||REPLACE(p_client_f,'*','%')||''')';         
       ELSE
          v_sql := v_sql || ' = UPPER(''' || p_client_f || ''')';
       END IF;
    END IF;
    
    IF p_client_n IS NOT NULL THEN       
       v_sql := v_sql || ' AND UPPER(TRIM(SUB.FIRST_NAME))';    
       IF INSTR(p_client_n,'*') > 0 THEN
          v_sql := v_sql || ' LIKE UPPER('''||REPLACE(p_client_n,'*','%')||''')';
       ELSE
          v_sql := v_sql || ' = UPPER(''' || p_client_n || ''')';
       END IF;
    END IF;
    
    IF p_client_p IS NOT NULL THEN       
       v_sql := v_sql || ' AND UPPER(TRIM(SUB.MIDDLE_NAME))';
       IF INSTR(p_client_p,'*') > 0 THEN
          v_sql := v_sql || ' LIKE UPPER('''||REPLACE(p_client_p,'*','%')||''')';
       ELSE
          v_sql := v_sql || ' = UPPER(''' || p_client_p || ''')';
       END IF;
    END IF;

    IF p_date_from IS NOT NULL THEN
       v_date_temp := TO_CHAR(p_date_from,'DD.MM.YYYY HH24:MI:SS');
       v_sql := v_sql || ' AND prof.DATE_FROM >= TO_DATE('''|| v_date_temp || ''',''DD.MM.YYYY HH24:MI:SS'')';
    END IF;
    
    IF p_date_to IS NOT NULL THEN
       v_date_temp := TO_CHAR(p_date_to,'DD.MM.YYYY HH24:MI:SS');
       v_sql := v_sql || ' AND prof.DATE_FROM <= TO_DATE('''|| v_date_temp || ''',''DD.MM.YYYY HH24:MI:SS'')';
    END IF;    
    
    IF p_phone_number IS NOT NULL THEN
      v_sql := v_sql || 
            ' AND (SYSDATE BETWEEN ph.DATE_FROM AND ph.DATE_TO OR ( SYSDATE >= ph.DATE_FROM AND ph.DATE_TO IS NULL))
            AND ph.PHONE_NUMBER';                            

          IF INSTR(p_phone_number,'*') > 0 THEN
             v_sql := v_sql || ' LIKE '''||REPLACE(p_phone_number,'*','%')||'''';
          ELSE
             v_sql := v_sql || ' = ''' || p_phone_number || '''';
          END IF;                                 
    END IF;
    
    IF p_contractors IS NOT NULL THEN
       IF p_only_in_branch = 1 THEN
            v_sql :=  v_sql || 'AND PROF.BRANCH_ID IN ('|| p_contractors ||')';              
       ELSE       
            v_sql :=  v_sql || 'AND (PROF.BRANCH_ID IN ('|| p_contractors ||') OR PROF.AGENT_ID IN ('|| p_contractors ||'))';         
       END IF;
    END IF;  
    
    -- Считаем, сколько строк возвращает запрос
    IF p_contractors IS NOT NULL THEN
      v_sql_exec := v_contractor_header || 'SELECT COUNT(*) '|| v_sql || ' AND ROWNUM <=501';
    ELSE
      v_sql_exec := 'SELECT COUNT(*) '|| v_sql || ' AND ROWNUM <=501';
    END IF;        

    EXECUTE IMMEDIATE v_sql_exec INTO v_cnt;                 
    dbms_output.put_line(v_sql_exec);
    -- Если больше 500 - говорим, что много и нужно уточнить поиск
    IF v_cnt > 500 THEN
       p_result := 'Найдено больше 500 записей. Уточните поиск';
    ELSE
    -- Если все ОК - формируем SQL
    v_sql2 := 'SELECT acc.ACCOUNT_ID,
                   acc.ACCOUNT_NO,
                   C.CONTRACT_ID,
                   C.CONTRACT_NO,
                   acc.ACCOUNT_TYPE,
                   acc.CREATE_DATE,
                   acc.STATUS STATUS_ID,
                   dict.NAME STATUS,
                   acc.BALANCE,
                   prof.DATE_FROM CREATE_DATE,
                   sub.LAST_NAME,
                   sub.FIRST_NAME,
                   sub.MIDDLE_NAME ';
    
    IF p_phone_number IS NOT NULL THEN
        v_sql2 := v_sql2 || ', ord.ORDER_ID, ord.ORDER_NO, 
                              ph.PHONE_NUMBER,ph.DATE_FROM PHONE_DATE_FROM, ph.DATE_TO PHONE_DATE_TO';
     ELSE
       IF p_order_no IS NOT NULL THEN                                
        v_sql2 := v_sql2 || ', ord.ORDER_ID, ord.ORDER_NO';
       END IF;
     END IF;  
                    
     v_sql2 := v_sql2 || v_sql;
 
    open p_recordset for v_sql2;           
    p_result := '';
    END IF;  
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

--=========================================================================================
-- Поиск лицевых счетов для ФИЗИКА согласно кураторским права менеджера
PROCEDURE ACCOUNT_SEARCH_F_BY_MGR_RULE (  
      p_result          OUT VARCHAR2, 
      p_recordset       OUT t_refc, 
      p_account_id      IN NUMBER,      -- ID лицевого счета
      p_account_no      IN VARCHAR2,    -- лицевой счет
      p_contract_no     IN VARCHAR2,    -- номер контракта
      p_order_no        IN VARCHAR2,    -- номер заказ, который должен быть в л/с
      p_date_from       IN DATE,        -- дата создания с
      p_date_to         IN DATE,        -- дата создания по  
      p_client_f        IN VARCHAR2,    -- фамилия клиента
      p_client_n        IN VARCHAR2,    -- имя клиента
      p_client_p        IN VARCHAR2,    -- отчество клиента
      p_phone_number    IN VARCHAR2,    -- номер телефона
      p_manager_id      IN INTEGER      -- менеджер ID (sale-куратор)
)
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_SEARCH_MANAGER_F';
    v_retcode            INTEGER;
    v_sql                VARCHAR2(10000);
    v_sql2               VARCHAR2(10000);
    v_sql_exec           VARCHAR2(10000);
    v_date_temp          VARCHAR2(20);
    v_cnt                INTEGER;
BEGIN   
    -- Формируем SQL-запрос в строке
    v_sql := ' 
              FROM account_t acc,
                   account_profile_t prof,
                   contract_t c,
                   subscriber_t sub,
                   contractor_t contr,
                   account_contact_t ac,
                   dictionary_t dict';
                   
     IF p_phone_number IS NOT NULL THEN
        v_sql := v_sql || ', order_phones_t ph 
                          , order_t ord';
     ELSE
       IF p_order_no IS NOT NULL THEN                                
         v_sql := v_sql || ', order_t ord';         
       END IF;
     END IF;   
     
/*     IF p_contractors IS NOT NULL THEN
        v_sql := v_sql || ', brand b';
     END IF;  */
                                                                 
     v_sql := v_sql || ' WHERE acc.account_id = PROF.ACCOUNT_ID
                   AND C.CONTRACT_ID = PROF.CONTRACT_ID
                   AND SYSDATE BETWEEN prof.DATE_FROM AND NVL(prof.DATE_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                   AND SUB.SUBSCRIBER_ID = PROF.SUBSCRIBER_ID
                   AND CONTR.CONTRACTOR_ID = PROF.CONTRACTOR_ID
                   AND DICT.PARENT_ID = 2
                   AND DICT.KEY = ACC.STATUS
                   AND ac.account_id (+)= acc.account_id
                   AND AC.ADDRESS_TYPE (+)= ''DLV''
                   AND ACC.STATUS <> ''DBL''
                   AND acc.ACCOUNT_TYPE = ''P''';                                                             
    
    IF p_phone_number IS NOT NULL THEN
        v_sql := v_sql || 
                   ' AND ph.order_id = ord.order_id
                     AND ord.account_id = acc.account_id';
     ELSE
       IF p_order_no IS NOT NULL THEN                                
         v_sql := v_sql || ' AND ord.account_id = acc.account_id';
       END IF;
     END IF;     
    
    IF p_account_id IS NOT NULL THEN
      v_sql := v_sql || ' AND ACC.ACCOUNT_ID = ' || p_account_id;
    END IF;
    
    IF p_account_no IS NOT NULL THEN       
       v_sql := v_sql || ' AND ACC.ACCOUNT_NO';    
       IF INSTR(p_account_no,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || UPPER(REPLACE(p_account_no,'*','%'))||'''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_account_no || '''';
       END IF;
    END IF;
    
    IF p_contract_no IS NOT NULL THEN       
       v_sql := v_sql || ' AND C.CONTRACT_NO';    
       IF INSTR(p_contract_no,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || UPPER(REPLACE(p_contract_no,'*','%')) || '''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_contract_no || '''';
       END IF;
    END IF;
    
    IF p_order_no IS NOT NULL THEN       
       v_sql := v_sql || ' AND ord.ORDER_NO';    
       IF INSTR(p_order_no,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || UPPER(REPLACE(p_order_no,'*','%')) || '''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_order_no || '''';
       END IF;
    END IF;
    
    IF p_client_f IS NOT NULL THEN       
       v_sql := v_sql || ' AND UPPER(TRIM(SUB.LAST_NAME))';
       IF INSTR(p_client_f,'*') > 0 THEN
          v_sql := v_sql || ' LIKE UPPER('''||REPLACE(p_client_f,'*','%')||''')';         
       ELSE
          v_sql := v_sql || ' = UPPER(''' || p_client_f || ''')';
       END IF;
    END IF;
    
    IF p_client_n IS NOT NULL THEN       
       v_sql := v_sql || ' AND UPPER(TRIM(SUB.FIRST_NAME))';    
       IF INSTR(p_client_n,'*') > 0 THEN
          v_sql := v_sql || ' LIKE UPPER('''||REPLACE(p_client_n,'*','%')||''')';
       ELSE
          v_sql := v_sql || ' = UPPER(''' || p_client_n || ''')';
       END IF;
    END IF;
    
    IF p_client_p IS NOT NULL THEN       
       v_sql := v_sql || ' AND UPPER(TRIM(SUB.MIDDLE_NAME))';
       IF INSTR(p_client_p,'*') > 0 THEN
          v_sql := v_sql || ' LIKE UPPER('''||REPLACE(p_client_p,'*','%')||''')';
       ELSE
          v_sql := v_sql || ' = UPPER(''' || p_client_p || ''')';
       END IF;
    END IF;

    IF p_date_from IS NOT NULL THEN
       v_date_temp := TO_CHAR(p_date_from,'DD.MM.YYYY HH24:MI:SS');
       v_sql := v_sql || ' AND prof.DATE_FROM >= TO_DATE('''|| v_date_temp || ''',''DD.MM.YYYY HH24:MI:SS'')';
    END IF;
    
    IF p_date_to IS NOT NULL THEN
       v_date_temp := TO_CHAR(p_date_to,'DD.MM.YYYY HH24:MI:SS');
       v_sql := v_sql || ' AND prof.DATE_FROM <= TO_DATE('''|| v_date_temp || ''',''DD.MM.YYYY HH24:MI:SS'')';
    END IF;    
    
    IF p_phone_number IS NOT NULL THEN
      v_sql := v_sql || 
            ' AND (SYSDATE BETWEEN ph.DATE_FROM AND ph.DATE_TO OR ( SYSDATE >= ph.DATE_FROM AND ph.DATE_TO IS NULL))
            AND ph.PHONE_NUMBER';                            

          IF INSTR(p_phone_number,'*') > 0 THEN
             v_sql := v_sql || ' LIKE '''||REPLACE(p_phone_number,'*','%')||'''';
          ELSE
             v_sql := v_sql || ' = ''' || p_phone_number || '''';
          END IF;                                 
    END IF;
    
    IF p_manager_id IS NOT NULL THEN
       v_sql := v_sql ||
           ' AND EXISTS (
                SELECT *
                  FROM (SELECT ap.*
                          FROM account_profile_t ap, sale_curator_t s
                         WHERE s.manager_id = ' || p_manager_id || ' AND ap.account_id = s.account_id
                        UNION
                        SELECT ap.*
                          FROM account_profile_t ap, sale_curator_t s
                         WHERE     s.manager_id = ' || p_manager_id || '
                               AND ap.contract_id = s.contract_id
                               AND NOT EXISTS
                                          (SELECT *
                                             FROM account_profile_t ap2, sale_curator_t s2
                                            WHERE     s2.manager_id <> ' || p_manager_id || '
                                                  AND ap2.account_id = s2.account_id
                                                  AND ap2.account_id = ap.account_id)
                        UNION
                        SELECT ap.*
                          FROM account_profile_t ap, sale_curator_t s
                         WHERE     s.manager_id = ' || p_manager_id || '
                               AND ap.agent_id = s.contractor_id
                               AND NOT EXISTS
                                          (SELECT *
                                             FROM account_profile_t ap2, sale_curator_t s2
                                            WHERE     s2.manager_id <> ' || p_manager_id || '
                                                  AND ap2.account_id = s2.account_id
                                                  AND ap2.account_id = ap.account_id)
                               AND NOT EXISTS
                                          (SELECT *
                                             FROM account_profile_t ap2, sale_curator_t s2
                                            WHERE     s2.manager_id <> ' || p_manager_id || '
                                                  AND ap2.contract_id = s2.contract_id
                                                  AND ap2.account_id = ap.account_id)
                        UNION
                        SELECT ap.*
                          FROM account_profile_t ap, sale_curator_t s
                         WHERE     s.manager_id = ' || p_manager_id || '
                               AND ap.agent_id = s.contractor_id
                               AND NOT EXISTS
                                          (SELECT *
                                             FROM account_profile_t ap2, sale_curator_t s2
                                            WHERE     s2.manager_id <> ' || p_manager_id || '
                                                  AND ap2.account_id = s2.account_id
                                                  AND ap2.account_id = ap.account_id)
                               AND NOT EXISTS
                                          (SELECT *
                                             FROM account_profile_t ap2, sale_curator_t s2
                                            WHERE     s2.manager_id <> ' || p_manager_id || '
                                                  AND ap2.contract_id = s2.contract_id
                                                  AND ap2.account_id = ap.account_id)
                               AND NOT EXISTS
                                          (SELECT *
                                             FROM account_profile_t ap2, sale_curator_t s2
                                            WHERE     s2.manager_id <> ' || p_manager_id || '
                                                  AND ap2.agent_id = s2.contractor_id
                                                  AND ap2.account_id = ap.account_id)) t
                   WHERE T.ACCOUNT_ID = ACC.ACCOUNT_ID)';           
    END IF;
            
    -- Считаем, сколько строк возвращает запрос
    v_sql_exec := 'SELECT COUNT(*) '|| v_sql || ' AND ROWNUM <=501';    

    EXECUTE IMMEDIATE v_sql_exec INTO v_cnt;                 
    -- Если больше 500 - говорим, что много и нужно уточнить поиск
    IF v_cnt > 500 THEN
       p_result := 'Найдено больше 500 записей. Уточните поиск';
    ELSE
    -- Если все ОК - формируем SQL
    v_sql2 := 'SELECT acc.ACCOUNT_ID,
                   acc.ACCOUNT_NO,
                   C.CONTRACT_ID,
                   C.CONTRACT_NO,
                   acc.ACCOUNT_TYPE,
                   acc.CREATE_DATE,
                   acc.STATUS STATUS_ID,
                   dict.NAME STATUS,
                   acc.BALANCE,
                   prof.DATE_FROM CREATE_DATE,
                   sub.LAST_NAME,
                   sub.FIRST_NAME,
                   sub.MIDDLE_NAME ';
    
    IF p_phone_number IS NOT NULL THEN
        v_sql2 := v_sql2 || ', ord.ORDER_ID, ord.ORDER_NO, 
                              ph.PHONE_NUMBER,ph.DATE_FROM PHONE_DATE_FROM, ph.DATE_TO PHONE_DATE_TO';
     ELSE
       IF p_order_no IS NOT NULL THEN                                
        v_sql2 := v_sql2 || ', ord.ORDER_ID, ord.ORDER_NO';
       END IF;
     END IF;  
                    
     v_sql2 := v_sql2 || v_sql;
 
     open p_recordset for v_sql2;           
     p_result := '';
     END IF;  
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

--================================================================================================================
-- Сначала определяем, какого типа лицевой счет (физик или юрик), а потом вытаскиваем нужные данные
PROCEDURE ACCOUNT_INFO( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- лицевой счет
    )
IS    
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_INFO';
    v_retcode            INTEGER;
    v_account_type       VARCHAR2(10);
begin
    -- находим тип лицевого счета
    SELECT ACCOUNT_TYPE
      INTO v_account_type
    FROM ACCOUNT_T
      WHERE ACCOUNT_ID = p_account_id;

    IF v_account_type = 'P' THEN
       ACCOUNT_INFO_F (p_result,p_recordset,p_account_id); 
    ELSE
       ACCOUNT_INFO_Y (p_result,p_recordset,p_account_id);      
    END IF;

end;
--================================================================================================================
-- Получение детальной информации по л/с для физика
    PROCEDURE ACCOUNT_INFO_F( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- лицевой счет
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_INFO_F';
    v_retcode            INTEGER;
    v_sql                VARCHAR2(10000);
    v_date_temp          VARCHAR2(20);
    v_cnt                INTEGER;
begin
    open p_recordset for
         SELECT
              ACC.ACCOUNT_ID,
              ACC.ACCOUNT_NO,
              AP.CONTRACT_ID,
              ACC.ACCOUNT_TYPE,
              D_ACC_TYPE.NAME ACCOUNT_TYPE_NAME,
              ACC.BALANCE,
              ACC.BALANCE_DATE,
              ACC.CREATE_DATE,
              ACC.CURRENCY_ID,
              ACC.BILLING_ID,
              ACC.IDL_ENB,
--              CUR.CURRENCY_NAME,
              AP.VAT,        
              ACC.STATUS,
              ACC.NOTES,
              ACC.COMMENTARY,
              D_ACC.NAME STATUS_NAME,
              NULL DELIVERY_ID,
              NULL DELIVERY_METHOD_NAME,
              NULL MANAGER_ID,                    -- M.MANAGER_ID,
              NULL MANAGER_NAME,                  -- M.LAST_NAME || ' ' || M.FIRST_NAME || ' ' || M.MIDDLE_NAME MANAGER_NAME,
              CTR.CONTRACTOR_ID,
              CTR.PARENT_ID CONTRACTOR_PARENT_ID,
              CTR.CONTRACTOR_TYPE,
              CTR.ERP_CODE CONTRACTOR_ERP_CODE,
              CTR.INN CONTRACTOR_INN,
              CTR.KPP CONTRACTOR_KPP,
              CTR.CONTRACTOR CONTRACTOR_NAME,
              CTR.SHORT_NAME CONTRACTOR_NAME_SHORT,              
              CTR_BANK.BANK_ID CONTRACTOR_BANK_ID,
              CTR_BANK.BANK_NAME CONTRACTOR_BANK_NAME,
              CTR_BANK.BANK_CODE CONTRACTOR_BANK_CODE,
              CTR_BANK.BANK_CORR_ACCOUNT CONTRACTOR_BANK_CORR_ACCOUNT,
              CTR_BANK.BANK_SETTLEMENT CONTRACTOR_BANK_SETTLEMENT,                            
              BRANCH_CTR.CONTRACTOR_ID BRANCH_ID,
              BRANCH_CTR.CONTRACTOR_TYPE BRANCH_TYPE,
              BRANCH_CTR.ERP_CODE BRANCH_ERP_CODE,
              BRANCH_CTR.INN BRANCH_INN,
              BRANCH_CTR.KPP BRANCH_KPP,
              BRANCH_CTR.CONTRACTOR BRANCH_NAME,
              BRANCH_CTR.SHORT_NAME BRANCH_NAME_SHORT,              
              AGENT_CTR.CONTRACTOR_ID AGENT_ID,
              AGENT_CTR.CONTRACTOR_TYPE AGENT_TYPE,
              AGENT_CTR.ERP_CODE AGENT_ERP_CODE,
              AGENT_CTR.INN AGENT_INN,
              AGENT_CTR.KPP AGENT_KPP,
              AGENT_CTR.CONTRACTOR AGENT_NAME,
              AGENT_CTR.SHORT_NAME AGENT_NAME_SHORT,              
              ACC_CONT.PHONES,
              ACC_CONT.FAX,
              ACC_CONT.EMAIL,
              SUB.SUBSCRIBER_ID CLIENT_F_ID,
              SUB.LAST_NAME CLIENT_F_LAST_NAME,
              SUB.FIRST_NAME CLIENT_F_FIRST_NAME,    
              SUB.MIDDLE_NAME CLIENT_F_MIDDLE_NAME,
              AP.BRAND_ID,
              BR.BRAND BRAND_NAME,
              BR.PARENT_BRAND_ID BRAND_PARENT_ID
           FROM 
                 ACCOUNT_T ACC,
                 ACCOUNT_PROFILE_T AP,
                 DICTIONARY_T D_ACC,
                 DICTIONARY_T D_ACC_TYPE,
                 CONTRACTOR_T CTR,
                 CONTRACTOR_BANK_T CTR_BANK,                 
                 CONTRACTOR_T BRANCH_CTR,
                 CONTRACTOR_T AGENT_CTR,
                 ACCOUNT_CONTACT_T ACC_CONT,
                 SUBSCRIBER_T SUB,
                 BRAND_T BR
          WHERE
              ACC.STATUS <> 'DBL'
              AND br.BRAND_ID (+)= AP.BRAND_ID
              AND AP.ACCOUNT_ID = ACC.ACCOUNT_ID
              AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN TRUNC(AP.DATE_FROM) AND (TRUNC(AP.DATE_TO) - NUMTODSINTERVAL(1,'SECOND')))
              AND D_ACC.KEY (+)= ACC.STATUS
              AND D_ACC.PARENT_ID = 2                    -- Статус лицевого счета 
              AND D_ACC_TYPE.KEY (+)= ACC.ACCOUNT_TYPE 
              AND D_ACC_TYPE.PARENT_ID = 1               -- Тип лицевого счета
              AND CTR.CONTRACTOR_ID (+)= AP.CONTRACTOR_ID
              AND CTR_BANK.BANK_ID (+)= AP.CONTRACTOR_BANK_ID
              AND BRANCH_CTR.CONTRACTOR_ID (+)= AP.BRANCH_ID
              AND AGENT_CTR.CONTRACTOR_ID (+)= AP.AGENT_ID
              AND ACC_CONT.ACCOUNT_ID (+)= ACC.ACCOUNT_ID
              AND ACC_CONT.ADDRESS_TYPE (+)= 'DLV'
              AND SUB.SUBSCRIBER_ID = AP.SUBSCRIBER_ID
              AND (ACC_CONT.DATE_TO IS NULL OR SYSDATE BETWEEN TRUNC(ACC_CONT.DATE_FROM) AND (TRUNC(ACC_CONT.DATE_TO) - NUMTODSINTERVAL(1,'SECOND')))
              AND ACC.ACCOUNT_ID = p_account_id;  
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;

--======================================================================================================================================
    -- Поиск лицевых счетов для ЮРИКА
    -- Поиск ищется как по полном совпадению, так и НЕполному (если указать *) 
    PROCEDURE ACCOUNT_SEARCH_Y(  
      p_result        OUT VARCHAR2, 
      p_recordset     OUT t_refc, 
      p_account_id    IN NUMBER,      -- ID лицевого счета
      p_account_no    IN VARCHAR2,    -- лицевой счет
      p_contract_no   IN VARCHAR2,    -- номер контракта
      p_date_from     IN DATE,        -- дата создания с
      p_date_to       IN DATE,        -- дата создания по  
      p_order_no      IN VARCHAR2,    -- номер заказ, который должен быть в л/с      
      p_customer_name IN VARCHAR2,    -- название компании
      p_customer_inn  IN VARCHAR2,    -- ИНН компании
      p_customer_kpp  IN VARCHAR2,    -- КПП компании
      p_phone_number  IN VARCHAR2,    -- номер телефона
      p_contractors   IN VARCHAR2,      -- список плставщиков через запятую
      p_only_in_branch   IN INTEGER -- искать только в регионах 
)
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_SEARCH_Y';
    v_retcode            INTEGER;
    v_sql                VARCHAR2(10000);
    v_sql2               VARCHAR2(10000);
    v_sql_exec           VARCHAR2(10000);
    v_contractor_header  VARCHAR2(10000);
    v_date_temp          VARCHAR2(20);
    v_cnt                INTEGER;
BEGIN
    -- Формируем SQL-запрос в строке
    v_sql := ' 
              FROM account_t acc,
                   account_profile_t prof,
                   contract_t c,
                   customer_t cus,
                   contractor_t contr,
                   account_contact_t ac,
                   dictionary_t dict';
     
    IF p_phone_number IS NOT NULL THEN
        v_sql := v_sql || ', order_phones_t ph 
                          , order_t ord';
    ELSE
       IF p_order_no IS NOT NULL THEN                                
         v_sql := v_sql || ', order_t ord';         
       END IF;

    END IF;                   
    
/*    IF p_contractors IS NOT NULL THEN
       v_sql := v_sql || ', brand b';
    END IF; 
*/                   
    v_sql := v_sql || ' WHERE acc.account_id = PROF.ACCOUNT_ID                   
                   AND C.CONTRACT_ID = PROF.CONTRACT_ID
                   AND CUS.CUSTOMER_ID = PROF.CUSTOMER_ID
                   AND CONTR.CONTRACTOR_ID = PROF.CONTRACTOR_ID
                   AND SYSDATE BETWEEN prof.date_from and NVL(prof.date_to,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                   AND DICT.PARENT_ID = 2
                   AND DICT.KEY = ACC.STATUS
                   AND ac.account_id (+)= acc.account_id
                   AND AC.ADDRESS_TYPE (+)= ''DLV''
                   AND ACC.STATUS <> ''DBL''
                   AND acc.ACCOUNT_TYPE = ''J''';    
    
    IF p_phone_number IS NOT NULL THEN
        v_sql := v_sql || 
                   ' AND ph.order_id = ord.order_id
                     AND ord.account_id = acc.account_id';
    ELSE
       IF p_order_no IS NOT NULL THEN                                
         v_sql := v_sql || ' AND ord.account_id = acc.account_id';
       END IF;

    END IF; 
    
    IF p_account_id IS NOT NULL THEN
      v_sql := v_sql || ' AND ACC.ACCOUNT_ID = ' || p_account_id;
    END IF;

  
  
    IF p_account_no IS NOT NULL THEN       
       v_sql := v_sql || ' AND ACC.ACCOUNT_NO';    
       IF INSTR(p_account_no,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || REPLACE(p_account_no,'*','%')||'''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_account_no || '''';

       END IF;

    END IF;

    
    IF p_contract_no IS NOT NULL THEN       
       v_sql := v_sql || ' AND C.CONTRACT_NO';    
       IF INSTR(p_contract_no,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || REPLACE(p_contract_no,'*','%') || '''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_contract_no || '''';

       END IF;

    END IF;

    
    IF p_customer_name IS NOT NULL THEN       
       v_sql := v_sql || ' AND UPPER(CUS.SHORT_NAME)';    
       IF INSTR(p_customer_name,'*') > 0 THEN
          v_sql := v_sql || ' LIKE UPPER('''||REPLACE(p_customer_name,'*','%')||''')';         
       ELSE
          v_sql := v_sql || ' = UPPER(''' || p_customer_name || ''')';

       END IF;

    END IF;

    
    IF p_customer_inn IS NOT NULL THEN       
       v_sql := v_sql || ' AND CUS.INN';    
       IF INSTR(p_customer_inn,'*') > 0 THEN
          v_sql := v_sql || ' LIKE '''||REPLACE(p_customer_inn,'*','%')||'''';
       ELSE
          v_sql := v_sql || ' = ''' || p_customer_inn || '''';

       END IF;

    END IF;

    
    IF p_customer_kpp IS NOT NULL THEN       
       v_sql := v_sql || ' AND CUS.KPP';    
       IF INSTR(p_customer_kpp,'*') > 0 THEN
          v_sql := v_sql || ' LIKE '''||REPLACE(p_customer_kpp,'*','%')||'''';
       ELSE
          v_sql := v_sql || ' = ''' || p_customer_kpp || '''';

       END IF;

    END IF;


    IF p_date_from IS NOT NULL THEN
       v_date_temp := TO_CHAR(p_date_from,'DD.MM.YYYY HH24:MI:SS');
       v_sql := v_sql || ' AND prof.DATE_FROM >= TO_DATE('''|| v_date_temp || ''',''DD.MM.YYYY HH24:MI:SS'')';
    END IF;

    
    IF p_date_to IS NOT NULL THEN
       v_date_temp := TO_CHAR(p_date_to,'DD.MM.YYYY HH24:MI:SS');
       v_sql := v_sql || ' AND prof.DATE_FROM <= TO_DATE('''|| v_date_temp || ''',''DD.MM.YYYY HH24:MI:SS'')';
    END IF;    
    
    IF p_order_no IS NOT NULL THEN       
       v_sql := v_sql || ' AND ord.ORDER_NO';    
       IF INSTR(p_order_no,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || REPLACE(p_order_no,'*','%') || '''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_order_no || '''';

       END IF;

    END IF;

    
    IF p_phone_number IS NOT NULL THEN
      v_sql := v_sql || 
            ' AND (GET_MOSCOW_TIME BETWEEN ph.DATE_FROM AND ph.DATE_TO OR ( GET_MOSCOW_TIME >= ph.DATE_FROM AND ph.DATE_TO IS NULL))
            AND ph.PHONE_NUMBER';                            

          IF INSTR(p_phone_number,'*') > 0 THEN
             v_sql := v_sql || ' LIKE '''||REPLACE(p_phone_number,'*','%')||'''';
          ELSE
             v_sql := v_sql || ' = ''' || p_phone_number || '''';

          END IF;                                 
    END IF;

    IF p_contractors IS NOT NULL THEN
       IF p_only_in_branch = 1 THEN
            v_sql :=  v_sql || 'AND PROF.BRANCH_ID IN ('|| p_contractors ||')';              
       ELSE       
            v_sql :=  v_sql || 'AND (PROF.BRANCH_ID IN ('|| p_contractors ||') OR PROF.AGENT_ID IN ('|| p_contractors ||'))';         
       END IF;
    END IF;  

    v_sql_exec := 'SELECT COUNT(*) '|| v_sql || ' AND ROWNUM <=501';        
    
    -- Считаем, сколько строк возвращает запрос
    EXECUTE IMMEDIATE v_sql_exec INTO v_cnt;      
        
    -- Если больше 500 - говорим, что много и нужно уточнить поиск
    IF v_cnt > 500 THEN
       p_result := 'Найдено больше 500 записей. Уточните поиск';
    ELSE
    -- Если все ОК - формируем SQL
        v_sql2 := 'SELECT acc.ACCOUNT_ID,
                   acc.ACCOUNT_NO,
                   C.CONTRACT_ID,
                   C.CONTRACT_NO,
                   acc.ACCOUNT_TYPE,
                   prof.DATE_FROM CREATE_DATE,
                   acc.STATUS STATUS_ID,
                   dict.NAME STATUS,
                   acc.BALANCE,
                   acc.CREATE_DATE,
                   CUS.CUSTOMER_ID,
                   CUS.CUSTOMER CUSTOMER_NAME,
                   NVL(CUS.SHORT_NAME, CUS.CUSTOMER) CUSTOMER_NAME_SHORT,
                   CUS.ERP_CODE CUSTOMER_ERP_CODE,
                   CUS.INN CUSTOMER_INN,
                   CUS.KPP CUSTOMER_KPP';
         IF p_phone_number IS NOT NULL THEN
            v_sql2 := v_sql2 || ', ord.ORDER_ID, ord.ORDER_NO, 
                                  ph.PHONE_NUMBER,ph.DATE_FROM PHONE_DATE_FROM, ph.DATE_TO PHONE_DATE_TO';
         ELSE
           IF p_order_no IS NOT NULL THEN                                
            v_sql2 := v_sql2 || ', ord.ORDER_ID, ord.ORDER_NO';
           END IF;

         END IF;     
                   
         v_sql2 := v_sql2 || v_sql;
         
         open p_recordset for v_sql2;           
         p_result := '';

    END IF;  
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;

        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);

END;

--======================================================================================================================================
    -- Поиск лицевых счетов для ЮРИКА согласно кураторским права менеджера
    -- Поиск ищется как по полном совпадению, так и НЕполному (если указать *) 
    PROCEDURE ACCOUNT_SEARCH_Y_BY_MGR_RULE(  
      p_result        OUT VARCHAR2, 
      p_recordset     OUT t_refc, 
      p_account_id    IN NUMBER,      -- ID лицевого счета
      p_account_no    IN VARCHAR2,    -- лицевой счет
      p_contract_no   IN VARCHAR2,    -- номер контракта
      p_date_from     IN DATE,        -- дата создания с
      p_date_to       IN DATE,        -- дата создания по  
      p_order_no      IN VARCHAR2,    -- номер заказ, который должен быть в л/с      
      p_customer_name IN VARCHAR2,    -- название компании
      p_customer_inn  IN VARCHAR2,    -- ИНН компании
      p_customer_kpp  IN VARCHAR2,    -- КПП компании
      p_phone_number  IN VARCHAR2,    -- номер телефона
      p_manager_id    IN INTEGER      -- ID менеджера (sale-кураторы)
)
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_SEARCH_MANAGER_Y';
    v_retcode            INTEGER;
    v_sql                VARCHAR2(10000);
    v_sql2               VARCHAR2(10000);
    v_sql_exec           VARCHAR2(10000);
    v_date_temp          VARCHAR2(20);
    v_cnt                INTEGER;
BEGIN
    -- Формируем SQL-запрос в строке
    v_sql := ' 
              FROM account_t acc,
                   account_profile_t prof,
                   contract_t c,
                   customer_t cus,
                   contractor_t contr,
                   account_contact_t ac,
                   dictionary_t dict';
     
    IF p_phone_number IS NOT NULL THEN
        v_sql := v_sql || ', order_phones_t ph 
                          , order_t ord';
    ELSE
       IF p_order_no IS NOT NULL THEN                                
         v_sql := v_sql || ', order_t ord';         
       END IF;

    END IF;                   
                      
    v_sql := v_sql || ' WHERE acc.account_id = PROF.ACCOUNT_ID                   
                   AND C.CONTRACT_ID = PROF.CONTRACT_ID
                   AND CUS.CUSTOMER_ID = PROF.CUSTOMER_ID
                   AND CONTR.CONTRACTOR_ID = PROF.CONTRACTOR_ID
                   AND SYSDATE BETWEEN prof.date_from and NVL(prof.date_to,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                   AND DICT.PARENT_ID = 2
                   AND DICT.KEY = ACC.STATUS
                   AND ac.account_id (+)= acc.account_id
                   AND AC.ADDRESS_TYPE (+)= ''DLV''
                   AND ACC.STATUS <> ''DBL''
                   AND acc.ACCOUNT_TYPE = ''J''';    
    
    IF p_phone_number IS NOT NULL THEN
        v_sql := v_sql || 
                   ' AND ph.order_id = ord.order_id
                     AND ord.account_id = acc.account_id';
    ELSE
       IF p_order_no IS NOT NULL THEN                                
         v_sql := v_sql || ' AND ord.account_id = acc.account_id';
       END IF;

    END IF; 
    
    IF p_account_id IS NOT NULL THEN
      v_sql := v_sql || ' AND ACC.ACCOUNT_ID = ' || p_account_id;
    END IF;

  
  
    IF p_account_no IS NOT NULL THEN       
       v_sql := v_sql || ' AND ACC.ACCOUNT_NO';    
       IF INSTR(p_account_no,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || REPLACE(p_account_no,'*','%')||'''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_account_no || '''';

       END IF;

    END IF;

    
    IF p_contract_no IS NOT NULL THEN       
       v_sql := v_sql || ' AND C.CONTRACT_NO';    
       IF INSTR(p_contract_no,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || REPLACE(p_contract_no,'*','%') || '''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_contract_no || '''';

       END IF;

    END IF;

    
    IF p_customer_name IS NOT NULL THEN       
       v_sql := v_sql || ' AND UPPER(CUS.SHORT_NAME)';    
       IF INSTR(p_customer_name,'*') > 0 THEN
          v_sql := v_sql || ' LIKE UPPER('''||REPLACE(p_customer_name,'*','%')||''')';         
       ELSE
          v_sql := v_sql || ' = UPPER(''' || p_customer_name || ''')';

       END IF;

    END IF;

    
    IF p_customer_inn IS NOT NULL THEN       
       v_sql := v_sql || ' AND CUS.INN';    
       IF INSTR(p_customer_inn,'*') > 0 THEN
          v_sql := v_sql || ' LIKE '''||REPLACE(p_customer_inn,'*','%')||'''';
       ELSE
          v_sql := v_sql || ' = ''' || p_customer_inn || '''';

       END IF;

    END IF;

    
    IF p_customer_kpp IS NOT NULL THEN       
       v_sql := v_sql || ' AND CUS.KPP';    
       IF INSTR(p_customer_kpp,'*') > 0 THEN
          v_sql := v_sql || ' LIKE '''||REPLACE(p_customer_kpp,'*','%')||'''';
       ELSE
          v_sql := v_sql || ' = ''' || p_customer_kpp || '''';

       END IF;

    END IF;


    IF p_date_from IS NOT NULL THEN
       v_date_temp := TO_CHAR(p_date_from,'DD.MM.YYYY HH24:MI:SS');
       v_sql := v_sql || ' AND prof.DATE_FROM >= TO_DATE('''|| v_date_temp || ''',''DD.MM.YYYY HH24:MI:SS'')';
    END IF;

    
    IF p_date_to IS NOT NULL THEN
       v_date_temp := TO_CHAR(p_date_to,'DD.MM.YYYY HH24:MI:SS');
       v_sql := v_sql || ' AND prof.DATE_FROM <= TO_DATE('''|| v_date_temp || ''',''DD.MM.YYYY HH24:MI:SS'')';
    END IF;    
    
    IF p_order_no IS NOT NULL THEN       
       v_sql := v_sql || ' AND ord.ORDER_NO';    
       IF INSTR(p_order_no,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || REPLACE(p_order_no,'*','%') || '''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_order_no || '''';

       END IF;

    END IF;

    
    IF p_phone_number IS NOT NULL THEN
      v_sql := v_sql || 
            ' AND (GET_MOSCOW_TIME BETWEEN ph.DATE_FROM AND ph.DATE_TO OR ( GET_MOSCOW_TIME >= ph.DATE_FROM AND ph.DATE_TO IS NULL))
            AND ph.PHONE_NUMBER';                            

          IF INSTR(p_phone_number,'*') > 0 THEN
             v_sql := v_sql || ' LIKE '''||REPLACE(p_phone_number,'*','%')||'''';
          ELSE
             v_sql := v_sql || ' = ''' || p_phone_number || '''';

          END IF;                                 
    END IF;

    IF p_manager_id IS NOT NULL THEN
       v_sql := v_sql ||
           ' AND EXISTS (
                SELECT *
                  FROM (SELECT ap.*
                          FROM account_profile_t ap, sale_curator_t s
                         WHERE s.manager_id = ' || p_manager_id || ' AND ap.account_id = s.account_id
                        UNION
                        SELECT ap.*
                          FROM account_profile_t ap, sale_curator_t s
                         WHERE     s.manager_id = ' || p_manager_id || '
                               AND ap.contract_id = s.contract_id
                               AND NOT EXISTS
                                          (SELECT *
                                             FROM account_profile_t ap2, sale_curator_t s2
                                            WHERE     s2.manager_id <> ' || p_manager_id || '
                                                  AND ap2.account_id = s2.account_id
                                                  AND ap2.account_id = ap.account_id)
                        UNION
                        SELECT ap.*
                          FROM account_profile_t ap, sale_curator_t s
                         WHERE     s.manager_id = ' || p_manager_id || '
                               AND ap.agent_id = s.contractor_id
                               AND NOT EXISTS
                                          (SELECT *
                                             FROM account_profile_t ap2, sale_curator_t s2
                                            WHERE     s2.manager_id <> ' || p_manager_id || '
                                                  AND ap2.account_id = s2.account_id
                                                  AND ap2.account_id = ap.account_id)
                               AND NOT EXISTS
                                          (SELECT *
                                             FROM account_profile_t ap2, sale_curator_t s2
                                            WHERE     s2.manager_id <> ' || p_manager_id || '
                                                  AND ap2.contract_id = s2.contract_id
                                                  AND ap2.account_id = ap.account_id)
                        UNION
                        SELECT ap.*
                          FROM account_profile_t ap, sale_curator_t s
                         WHERE     s.manager_id = ' || p_manager_id || '
                               AND ap.agent_id = s.contractor_id
                               AND NOT EXISTS
                                          (SELECT *
                                             FROM account_profile_t ap2, sale_curator_t s2
                                            WHERE     s2.manager_id <> ' || p_manager_id || '
                                                  AND ap2.account_id = s2.account_id
                                                  AND ap2.account_id = ap.account_id)
                               AND NOT EXISTS
                                          (SELECT *
                                             FROM account_profile_t ap2, sale_curator_t s2
                                            WHERE     s2.manager_id <> ' || p_manager_id || '
                                                  AND ap2.contract_id = s2.contract_id
                                                  AND ap2.account_id = ap.account_id)
                               AND NOT EXISTS
                                          (SELECT *
                                             FROM account_profile_t ap2, sale_curator_t s2
                                            WHERE     s2.manager_id <> ' || p_manager_id || '
                                                  AND ap2.agent_id = s2.contractor_id
                                                  AND ap2.account_id = ap.account_id)) t
                   WHERE T.ACCOUNT_ID = ACC.ACCOUNT_ID)';           
    END IF; 

    v_sql_exec := 'SELECT COUNT(*) '|| v_sql || ' AND ROWNUM <=501';        
    
    -- Считаем, сколько строк возвращает запрос
    EXECUTE IMMEDIATE v_sql_exec INTO v_cnt;      
        
    -- Если больше 500 - говорим, что много и нужно уточнить поиск
    IF v_cnt > 500 THEN
       p_result := 'Найдено больше 500 записей. Уточните поиск';
    ELSE
    -- Если все ОК - формируем SQL
        v_sql2 := 'SELECT acc.ACCOUNT_ID,
                   acc.ACCOUNT_NO,
                   C.CONTRACT_ID,
                   C.CONTRACT_NO,
                   acc.ACCOUNT_TYPE,
                   prof.DATE_FROM CREATE_DATE,
                   acc.STATUS STATUS_ID,
                   dict.NAME STATUS,
                   acc.BALANCE,
                   acc.CREATE_DATE,
                   CUS.CUSTOMER_ID,
                   CUS.CUSTOMER CUSTOMER_NAME,
                   NVL(CUS.SHORT_NAME, CUS.CUSTOMER) CUSTOMER_NAME_SHORT,
                   CUS.ERP_CODE CUSTOMER_ERP_CODE,
                   CUS.INN CUSTOMER_INN,
                   CUS.KPP CUSTOMER_KPP';
         IF p_phone_number IS NOT NULL THEN
            v_sql2 := v_sql2 || ', ord.ORDER_ID, ord.ORDER_NO, 
                                  ph.PHONE_NUMBER,ph.DATE_FROM PHONE_DATE_FROM, ph.DATE_TO PHONE_DATE_TO';
         ELSE
           IF p_order_no IS NOT NULL THEN                                
            v_sql2 := v_sql2 || ', ord.ORDER_ID, ord.ORDER_NO';
           END IF;

         END IF;     
                   
         v_sql2 := v_sql2 || v_sql;
         
         open p_recordset for v_sql2;           
         p_result := '';

    END IF;  
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;

        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);

END;

--================================================================================================================
-- Получение детальной информации по л/с для ЮРИКА
PROCEDURE ACCOUNT_INFO_Y( 
     p_result        OUT VARCHAR2, 
     p_recordset     OUT t_refc, 
     p_account_id    IN NUMBER    -- лицевой счет
)
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_INFO_Y';
    v_retcode            INTEGER;
    v_sql                VARCHAR2(10000);
    v_date_temp          VARCHAR2(20);
    v_cnt                INTEGER;
begin
    open p_recordset for
         SELECT
              ACC.ACCOUNT_ID,
              ACC.ACCOUNT_NO,
              AP.CONTRACT_ID,
              ACC.ACCOUNT_TYPE,
              D_ACC_TYPE.NAME ACCOUNT_TYPE_NAME,
              ACC.BALANCE,
              ACC.BALANCE_DATE,
              ACC.CREATE_DATE,
              ACC.CURRENCY_ID,
              ACC.BILLING_ID,
              ACC.NOTES,
              ACC.COMMENTARY,
              ACC.IDL_ENB,
              AP.VAT,
              ACC.STATUS,
              D_ACC.NAME STATUS_NAME,
              NULL DELIVERY_ID,
              NULL DELIVERY_METHOD_NAME,
              NULL MANAGER_ID,                 --M.MANAGER_ID,
              NULL MANAGER_NAME,               --M.LAST_NAME || ' ' || M.FIRST_NAME || ' ' || M.MIDDLE_NAME MANAGER_NAME,
              CTR.CONTRACTOR_ID,
              CTR.PARENT_ID CONTRACTOR_PARENT_ID,              
              CTR.CONTRACTOR_TYPE,
              CTR.ERP_CODE CONTRACTOR_ERP_CODE,
              CTR.INN CONTRACTOR_INN,
              CTR.KPP CONTRACTOR_KPP,
              CTR.CONTRACTOR CONTRACTOR_NAME,
              NVL(CTR.SHORT_NAME,CTR.CONTRACTOR) CONTRACTOR_NAME_SHORT,
              CTR_BANK.BANK_ID CONTRACTOR_BANK_ID,
              CTR_BANK.BANK_NAME CONTRACTOR_BANK_NAME,
              CTR_BANK.BANK_CODE CONTRACTOR_BANK_CODE,
              CTR_BANK.BANK_CORR_ACCOUNT CONTRACTOR_BANK_CORR_ACCOUNT,
              CTR_BANK.BANK_SETTLEMENT CONTRACTOR_BANK_SETTLEMENT,                                       
              BRANCH_CTR.CONTRACTOR_ID BRANCH_ID,
              BRANCH_CTR.CONTRACTOR_TYPE BRANCH_TYPE,
              BRANCH_CTR.ERP_CODE BRANCH_ERP_CODE,
              BRANCH_CTR.INN BRANCH_INN,
              BRANCH_CTR.KPP BRANCH_KPP,
              BRANCH_CTR.CONTRACTOR BRANCH_NAME,
              BRANCH_CTR.SHORT_NAME BRANCH_NAME_SHORT,              
              AGENT_CTR.CONTRACTOR_ID AGENT_ID,
              AGENT_CTR.CONTRACTOR_TYPE AGENT_TYPE,
              AGENT_CTR.ERP_CODE AGENT_ERP_CODE,
              AGENT_CTR.INN AGENT_INN,
              AGENT_CTR.KPP AGENT_KPP,
              AGENT_CTR.CONTRACTOR AGENT_NAME,
              AGENT_CTR.SHORT_NAME AGENT_NAME_SHORT,              
              ACC_CONT.PHONES,
              ACC_CONT.FAX,
              ACC_CONT.EMAIL,
              CUS.CUSTOMER_ID,
              CM.COMPANY_NAME CUSTOMER_NAME,
              CM.SHORT_NAME CUSTOMER_NAME_SHORT,
              CUS.ERP_CODE CUSTOMER_ERP_CODE,
              CUS.INN CUSTOMER_INN,
              CUS.KPP CUSTOMER_KPP,
              ap.BRAND_ID,
              br.BRAND BRAND_NAME,
              br.PARENT_BRAND_ID BRAND_PARENT_ID
           FROM 
                 ACCOUNT_T ACC,
                 ACCOUNT_PROFILE_T AP,
                 DICTIONARY_T D_ACC,
                 DICTIONARY_T D_ACC_TYPE,
                 CONTRACTOR_T CTR,
                 CONTRACTOR_BANK_T CTR_BANK,
                 CONTRACTOR_T BRANCH_CTR,
                 CONTRACTOR_T AGENT_CTR,
                 ACCOUNT_CONTACT_T ACC_CONT,
                 CUSTOMER_T CUS,
                 COMPANY_T CM,
                 BRAND_T BR
          WHERE
               ACC.STATUS <> 'DBL'
              AND br.BRAND_ID (+)= AP.BRAND_ID 
              AND AP.ACCOUNT_ID = ACC.ACCOUNT_ID
              AND CUS.CUSTOMER_ID = AP.CUSTOMER_ID
              AND SYSDATE BETWEEN AP.DATE_FROM AND NVL(AP.DATE_TO,TO_DATE('01.01.2050','DD.MM.YYYY'))
              AND D_ACC.KEY (+)= ACC.STATUS
              AND D_ACC.PARENT_ID = PK00_Const.c_DICT_KEY_ACCOUNT_STATUS                    -- Статус лицевого счета 
              AND D_ACC_TYPE.KEY (+)= ACC.ACCOUNT_TYPE 
              AND D_ACC_TYPE.PARENT_ID = PK00_Const.c_DICT_KEY_ACCOUNT_TYPE               -- Тип лицевого счетавого счета
              AND CTR.CONTRACTOR_ID (+)= AP.CONTRACTOR_ID
              AND CTR_BANK.BANK_ID (+)= AP.CONTRACTOR_BANK_ID
              AND BRANCH_CTR.CONTRACTOR_ID (+)= AP.BRANCH_ID
              AND AGENT_CTR.CONTRACTOR_ID (+)= AP.AGENT_ID
              AND ACC_CONT.ACCOUNT_ID (+)= ACC.ACCOUNT_ID 
              AND ACC_CONT.ADDRESS_TYPE (+)= 'DLV'--TO_CHAR(PK00_Const.c_ADDR_TYPE_DLV)
              AND (ACC_CONT.DATE_TO IS NULL OR SYSDATE BETWEEN ACC_CONT.DATE_FROM AND ACC_CONT.DATE_TO)
              AND ACC.ACCOUNT_ID = p_account_id
              AND AP.CONTRACT_ID = CM.CONTRACT_ID
              AND CM.DATE_FROM <= SYSDATE
              AND (CM.DATE_TO IS NULL OR SYSDATE <= CM.DATE_TO)
              ;  
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;

--================================================================================================================
-- Поиск контракта
    PROCEDURE SEARCH_CONTRACT_LIST( 
         p_recordset     OUT t_refc, 
         p_contract_no   IN VARCHAR2,    -- номер контракта
         p_date_from     IN DATE,
         p_date_to       IN DATE
    )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'SEARCH_CONTRACT_LIST';
    v_date_from   VARCHAR2(100);
    v_date_to     VARCHAR2(100);
    v_sql                VARCHAR2(10000);
    v_retcode            INTEGER;
begin
    v_sql := 'SELECT 
                    C.CONTRACT_ID,
                    C.CONTRACT_NO,
                    C.DATE_FROM,
                    C.DATE_TO,
                    CL.CLIENT_ID,
                    CL.CLIENT_NAME,
                    M.MANAGER_ID,
                    M.LAST_NAME || '' '' || M.FIRST_NAME || '' '' || M.MIDDLE_NAME MANAGER_NAME,
                    C.MARKET_SEGMENT_ID,
                    NULL MARKET_SEGMENT_NAME,
                    C.CLIENT_TYPE_ID,
                    NULL CLIENT_TYPE_NAME,
                    C.XTTK_TYPE_ID,
                    NULL XTTK_TYPE_NAME,
                    NULL CONTRACT_TYPE_ID,
                    NULL CONTRACT_TYPE_NAME              
                FROM
                   CONTRACT_T C,
                   CLIENT_T CL,
                   SALE_CURATOR_T MI,
                   MANAGER_T M,
                   DICTIONARY_T DICT_SEGMENT,
                   DICTIONARY_T DICT_CLIENT_TYPE               
            WHERE
                C.CLIENT_ID = CL.CLIENT_ID
                AND C.CONTRACT_ID = MI.CONTRACT_ID(+)
                AND MI.MANAGER_ID = M.MANAGER_ID(+)
                AND DICT_SEGMENT.KEY (+)= C.MARKET_SEGMENT_ID
                AND DICT_SEGMENT.PARENT_ID (+)= 63
                AND DICT_CLIENT_TYPE.KEY (+)= C.CLIENT_TYPE_ID
                AND DICT_CLIENT_TYPE.PARENT_ID (+)= 64';    
    
    IF p_contract_no IS NOT NULL THEN       
       v_sql := v_sql || ' AND C.CONTRACT_NO';    
       IF INSTR(p_contract_no,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || REPLACE(p_contract_no,'*','%')||'''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_contract_no  || '''';
       END IF;
    END IF;
       
    IF p_date_from IS NOT NULL THEN
       v_date_from := TO_CHAR(p_date_from,'DD.MM.YYYY HH24:MI:SS');
       v_sql := v_sql || ' AND C.DATE_FROM >= TO_DATE('''|| v_date_from || ''',''DD.MM.YYYY HH24:MI:SS'')';
    END IF;
    
    IF p_date_to IS NOT NULL THEN
       v_date_to := TO_CHAR(p_date_to,'DD.MM.YYYY HH24:MI:SS');
       v_sql := v_sql || ' AND C.DATE_TO <= TO_DATE('''|| v_date_to || ''',''DD.MM.YYYY HH24:MI:SS'')';
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
-- Поиск поставщика
   PROCEDURE SEARCH_CUSTOMER_LIST( 
         p_recordset     OUT t_refc, 
         p_name          IN VARCHAR2,    -- имя поставщика
         p_inn           IN VARCHAR2,
         p_kpp           IN VARCHAR2
    )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'SEARCH_CUSTOMER_LIST';
    v_date_from   VARCHAR2(100);
    v_date_to     VARCHAR2(100);
    v_sql         VARCHAR2(10000);
    v_retcode     INTEGER;
begin
    v_sql := 'SELECT CUSTOMER_ID,
                   ERP_CODE CUSTOMER_ERP_CODE,
                   INN CUSTOMER_INN,
                   KPP CUSTOMER_KPP,
                   CUSTOMER CUSTOMER_NAME,
                   SHORT_NAME CUSTOMER_NAME_SHORT
              FROM customer_t WHERE 1=1';    
    
    IF p_name IS NOT NULL THEN       
       v_sql := v_sql || ' AND CUSTOMER';    
       IF INSTR(p_name,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || REPLACE(p_name,'*','%')||'''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_name  || '''';
       END IF;
    END IF;
       
    IF p_inn IS NOT NULL THEN       
       v_sql := v_sql || ' AND INN';    
       IF INSTR(p_name,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || REPLACE(p_inn,'*','%')||'''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_inn  || '''';
       END IF;
    END IF;
    
    IF p_kpp IS NOT NULL THEN       
       v_sql := v_sql || ' AND KPP';    
       IF INSTR(p_name,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || REPLACE(p_kpp,'*','%')||'''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_kpp  || '''';
       END IF;
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
-- Поиск клиента
    PROCEDURE SEARCH_CLIENT_LIST( 
         p_recordset     OUT t_refc, 
         p_clientname    IN VARCHAR2
    )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'SEARCH_CLIENT_LIST';
    v_sql         VARCHAR2(10000);
    v_retcode     INTEGER;
begin
    v_sql := 'select * from CLIENT_T';    
    
    IF p_clientname IS NOT NULL THEN       
       v_sql := v_sql || ' WHERE UPPER(CLIENT_NAME)';    
       IF INSTR(p_clientname,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || UPPER(REPLACE(p_clientname,'*','%'))||'''';         
       ELSE
          v_sql := v_sql || ' = ''' || UPPER(p_clientname)  || '''';
       END IF;
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

--=======================================================================
-- Загрузка списка счетов физ. лиц по фильтру
--=======================================================================
procedure BILL_FIZ_SEARCH(
          p_recordset           OUT t_refc,
          p_rep_period_id       IN  INTEGER,
          p_account_no          IN  VARCHAR2,
          p_contractor_list_id  IN  VARCHAR2,
          p_contractor_only_branch IN VARCHAR2,
          p_show_bill0          IN  INTEGER,
          sort_by               IN  VARCHAR2 DEFAULT NULL
)
is
    v_prcName   constant varchar2(30) := 'BILL_FIZ_SEARCH';
    v_sql VARCHAR2(4000);

begin
    v_sql := '    
           SELECT 
                   a.account_id,
                   a.account_no,
                   a.billing_id,
                   B.BILL_ID,
                   b.bill_no,
                   ap.contract_id,
                   c.contract_no,
                   b.rep_period_id,
                   b.bill_date,
                   b.PAID_TO,
                   b.TOTAL,
                   b.recvd,
                   b.due,
                   b.gross,
                   b.tax,
                   b.due_date,
                   b.BILL_STATUS BILL_STATUS_ID,
                   b_STATUS.NAME BILL_STATUS,
                   B.BILL_TYPE BILL_TYPE_ID,
                   b_type.NAME BILL_TYPE,
                   b.CURRENCY_ID,
                   per.POSITION BILL_TYPE_POSITION,
                   SUB.LAST_NAME || '' '' || SUB.FIRST_NAME || '' '' || SUB.MIDDLE_NAME SUBSCRIBER_NAME,
                   ac.zip,
                   br.CONTRACTOR BRANCH_NAME,
                   ag.CONTRACTOR AGENT_NAME
              FROM bill_t b,
                   account_t a,
                   account_profile_t ap,
                   SUBSCRIBER_T SUB,
                   contract_t c,
                   account_contact_t ac,
                   contractor_t br,
                   contractor_t ag,
                   (SELECT KEY, NAME || '' ('' || KEY || '')'' NAME
                      FROM dictionary_t
                     WHERE parent_id = 4) b_status,
                   (SELECT KEY, NAME || '' ('' || KEY || '')'' NAME
                      FROM dictionary_t
                     WHERE parent_id = 3) b_type,
                     PERIOD_T per
            WHERE  b_status.KEY(+) = B.BILL_STATUS
                   AND SYSDATE BETWEEN AP.DATE_FROM AND NVL(AP.DATE_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                   AND b_type.KEY(+) = B.BILL_TYPE
                   AND b.account_id = a.account_id
                   AND br.CONTRACTOR_ID (+)= ap.branch_id
                   AND ag.CONTRACTOR_ID (+)=ap.agent_ID
                   AND ap.account_id = a.account_id
                   AND c.contract_id = ap.contract_id
                   AND per.PERIOD_ID = b.REP_PERIOD_ID
                   AND sub.SUBSCRIBER_ID = ap.SUBSCRIBER_ID
                   AND ac.ACCOUNT_ID = a.account_ID
                   AND ac.ADDRESS_TYPE = ''DLV''
                   AND a.account_type = ''P''                  
                   AND B.BILL_STATUS IN (''READY'',''CLOSED'')
                   AND B.BILL_TYPE NOT IN (''C'')
                   AND A.STATUS NOT IN (''DBL'',''T'')';
            
        IF p_account_no IS NOT NULL THEN       
           v_sql := v_sql || ' AND A.ACCOUNT_NO';    
           IF INSTR(p_account_no,'*') > 0 THEN
              v_sql := v_sql || ' LIKE ''' || UPPER(REPLACE(p_account_no,'*','%'))||'''';         
           ELSE
              v_sql := v_sql || ' = ''' || p_account_no || '''';
           END IF;
        END IF;                   
       
       IF (p_rep_period_id IS NOT NULL) THEN
          v_sql := v_sql || ' AND b.REP_PERIOD_ID = '|| p_rep_period_id;       
       END IF;
       
       IF (p_show_bill0 != 1) THEN
          v_sql := v_sql || ' AND b.total <> 0';
       END IF;
       
       IF (p_contractor_list_id IS NOT NULL) THEN
            IF p_contractor_only_branch = 0  THEN
               v_sql := v_sql || ' AND (ap.branch_id IN ('|| p_contractor_list_id || ') OR (ap.agent_id IN ('|| p_contractor_list_id || ')))';                              
          ELSE
               v_sql := v_sql || ' AND (ap.branch_id IN ('|| p_contractor_list_id || ') AND (ap.agent_id IS NULL))';                              
          END IF;
       END IF;
       
       IF sort_by IS NULL THEN
          v_sql := v_sql || ' ORDER BY a.account_no, b.bill_no, b.rep_period_id desc';       
       ELSIF sort_by = 'ACCOUNT_NO' THEN
          v_sql := v_sql || ' ORDER BY a.account_no';
       ELSIF sort_by = 'SUBSCRIBER_NAME' THEN
          v_sql := v_sql || ' ORDER BY SUB.LAST_NAME, SUB.FIRST_NAME, SUB.MIDDLE_NAME';
       ELSIF sort_by = 'POST_INDEX' THEN
          v_sql := v_sql || ' ORDER BY ac.ZIP';
       ELSIF sort_by = 'AGENT_NAME' THEN
          v_sql := v_sql || ' ORDER BY ag.CONTRACTOR';          
       END IF;
       
  open p_recordset for v_sql;
exception
    when others then
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        if p_recordset%ISOPEN then 
            close p_recordset;
        end if;
end;


--=======================================================================
-- Загрузка списка счетов юр. лиц по фильтру
--=======================================================================
procedure BILL_JUR_SEARCH(
          p_recordset           OUT t_refc,
          p_rep_period_id       IN  INTEGER,
          p_contract_no         IN  VARCHAR2,
          p_account_no          IN  VARCHAR2,
          p_bill_no             IN  VARCHAR2,
          p_bill_type           IN  VARCHAR2,
          p_bill_status         IN  VARCHAR2,          
          p_contractor_list_id  IN  VARCHAR2,
          p_contractor_only_branch IN VARCHAR2,
          p_show_bill0          IN  INTEGER,
          p_delivery_method_id  IN  INTEGER,
          sort_by               IN  VARCHAR2 DEFAULT NULL
)
is
    v_prcName   constant varchar2(30) := 'BILL_JUR_SEARCH';
    v_sql VARCHAR2(4000);

begin
    v_sql := v_sql || '
        WITH AD as (
              SELECT ad.account_id, ad.delivery_method_id,D.NAME delivery_method_name
                 FROM account_documents_t ad, dictionary_t d                      
              WHERE d.key_id = ad.delivery_method_id AND ad.doc_bill = ''Y'')        
           SELECT 
                   a.account_id,
                   a.account_no,
                   a.billing_id,
                   B.BILL_ID,
                   b.bill_no,
                   ap.contract_id,
                   c.contract_no,
                   b.rep_period_id,
                   b.bill_date,
                   b.PAID_TO,
                   b.TOTAL,
                   b.gross,
                   b.tax,
                   b.recvd,
                   b.due,
                   b.due_date,
                   ad.DELIVERY_METHOD_ID,
                   ad.DELIVERY_METHOD_NAME,
                   b.BILL_STATUS BILL_STATUS_ID,
                   b_STATUS.NAME BILL_STATUS,
                   B.BILL_TYPE BILL_TYPE_ID,
                   b_type.NAME BILL_TYPE,
                   b.CURRENCY_ID,
                   per.POSITION BILL_TYPE_POSITION,
                   NVL(cus.SHORT_NAME,cus.CUSTOMER) CUSTOMER_NAME,
                   ac.zip,
                   br.CONTRACTOR BRANCH_NAME,
                   ag.CONTRACTOR AGENT_NAME
              FROM bill_t b,
                   account_t a,
                   account_profile_t ap,
                   customer_t cus,
                   contract_t c,
                   account_contact_t ac,
                   contractor_t br,
                   contractor_t ag,
                   (SELECT KEY, NAME || '' ('' || KEY || '')'' NAME
                      FROM dictionary_t
                     WHERE parent_id = 4) b_status,
                   (SELECT KEY_ID, KEY, NAME || '' ('' || KEY || '')'' NAME
                      FROM dictionary_t
                     WHERE parent_id = 3) b_type,
                     PERIOD_T per,
                     ad
            WHERE  b_status.KEY(+) = B.BILL_STATUS
                   AND SYSDATE BETWEEN AP.DATE_FROM AND NVL(AP.DATE_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                   AND b_type.KEY(+) = B.BILL_TYPE
                   AND b.account_id = a.account_id
                   AND br.CONTRACTOR_ID (+)= ap.branch_id
                   AND ag.CONTRACTOR_ID (+)=ap.agent_ID
                   AND ap.account_id = a.account_id
                   AND c.contract_id = ap.contract_id
                   AND per.PERIOD_ID = b.REP_PERIOD_ID
                   AND cus.CUSTOMER_ID = ap.CUSTOMER_ID
                   AND ac.ACCOUNT_ID = a.account_ID
                   AND ac.ADDRESS_TYPE = ''DLV''
                   AND a.account_type = ''J''                  
                   AND B.BILL_STATUS IN (''READY'',''CLOSED'')
                   AND B.BILL_TYPE NOT IN (''C'')
                   AND ad.account_Id (+) = a.account_id 
                   AND A.STATUS NOT IN (''DBL'',''T'')';
            
      IF (p_bill_type IS NOT NULL) THEN
--          v_sql := v_sql || ' AND B.BILL_TYPE = ''' ||p_bill_type || '''';             
          v_sql := v_sql || ' AND B_TYPE.KEY_ID = ''' ||p_bill_type || '''';             
       END IF;
       
       /* 
       IF (p_bill_status IS NOT NULL) THEN
          v_sql := v_sql || ' AND B.BILL_STATUS = ''' ||p_bill_status || '''';             
       END IF;*/
       
        IF p_account_no IS NOT NULL THEN       
           v_sql := v_sql || ' AND A.ACCOUNT_NO';    
           IF INSTR(p_account_no,'*') > 0 THEN
              v_sql := v_sql || ' LIKE ''' || UPPER(REPLACE(p_account_no,'*','%'))||'''';         
           ELSE
              v_sql := v_sql || ' = ''' || p_account_no || '''';
           END IF;
        END IF;
        
        IF p_contract_no IS NOT NULL THEN       
           v_sql := v_sql || ' AND C.CONTRACT_NO';    
           IF INSTR(p_contract_no,'*') > 0 THEN
              v_sql := v_sql || ' LIKE ''' || UPPER(REPLACE(p_contract_no,'*','%')) || '''';         
           ELSE
              v_sql := v_sql || ' = ''' || p_contract_no || '''';
           END IF;
        END IF;
        
        IF p_bill_no IS NOT NULL THEN       
           v_sql := v_sql || ' AND b.BILL_NO';    
           IF INSTR(p_bill_no,'*') > 0 THEN
              v_sql := v_sql || ' LIKE ''' || UPPER(REPLACE(p_bill_no,'*','%')) || '''';         
           ELSE
              v_sql := v_sql || ' = ''' || p_bill_no || '''';
           END IF;
        END IF;       
       
       IF (p_rep_period_id IS NOT NULL) THEN
          v_sql := v_sql || ' AND b.REP_PERIOD_ID = '|| p_rep_period_id;       
       END IF;
       
       IF (p_show_bill0 != 1) THEN
          v_sql := v_sql || ' AND b.total <> 0';
       END IF;
       
       IF (p_contractor_list_id IS NOT NULL) THEN
            IF p_contractor_only_branch = 0  THEN
               v_sql := v_sql || ' AND (ap.branch_id IN ('|| p_contractor_list_id || ') OR (ap.agent_id IN ('|| p_contractor_list_id || ')))';                              
          ELSE
               v_sql := v_sql || ' AND (ap.branch_id IN ('|| p_contractor_list_id || ') AND (ap.agent_id IS NULL))';                              
          END IF;
       END IF;
       
       IF (p_delivery_method_id IS NOT NULL) THEN
          v_sql := v_sql || ' AND ad.delivery_method_id = '|| p_delivery_method_id;       
       END IF;
       
       IF sort_by IS NULL THEN
          v_sql := v_sql || ' ORDER BY c.contract_no, b.bill_no, b.rep_period_id desc';
       ELSIF sort_by = 'CONTRACT_NO' THEN
          v_sql := v_sql || ' ORDER BY c.contract_no';                  
       ELSIF sort_by = 'ACCOUNT_NO' THEN
          v_sql := v_sql || ' ORDER BY a.account_no';
       ELSIF sort_by = 'CUSTOMER_NAME' THEN
          v_sql := v_sql || ' ORDER BY cus.customer';
       ELSIF sort_by = 'POST_INDEX' THEN
          v_sql := v_sql || ' ORDER BY ac.ZIP';
       ELSIF sort_by = 'AGENT_NAME' THEN
          v_sql := v_sql || ' ORDER BY ag.CONTRACTOR';          
       END IF;
     
  open p_recordset for v_sql;
exception
    when others then
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        if p_recordset%ISOPEN then 
            close p_recordset;
        end if;
end;

--=======================================================================
--Загрузка списка счетов по фильтру с учетом прав менеджера
procedure BILL_JUR_SEARCH_BY_MGR_RULE(
          p_recordset           OUT t_refc,
          p_rep_period_id       IN  INTEGER,
          p_contract_no         IN  VARCHAR2,
          p_account_no          IN  VARCHAR2,
          p_bill_no             IN  VARCHAR2,
          p_bill_type           IN  VARCHAR2,
          p_bill_status         IN  VARCHAR2,          
          p_manager_id          IN  INTEGER,
          p_show_bill0          IN  INTEGER,
          p_delivery_method_id  IN  INTEGER,
          sort_by               IN  VARCHAR2 DEFAULT NULL
)
is
    v_prcName   constant varchar2(30) := 'BILL_JUR_SEARCH_BY_MGR_RULE';
    v_sql VARCHAR2(10000);

begin
    v_sql := v_sql || '
        WITH AD as (
              SELECT ad.account_id, ad.delivery_method_id,D.NAME delivery_method_name
                 FROM account_documents_t ad, dictionary_t d                      
              WHERE d.key_id = ad.delivery_method_id AND ad.doc_bill = ''Y'')        
           SELECT 
                   a.account_id,
                   a.account_no,
                   a.billing_id,
                   B.BILL_ID,
                   b.bill_no,
                   ap.contract_id,
                   c.contract_no,
                   b.rep_period_id,
                   b.bill_date,
                   b.PAID_TO,
                   b.TOTAL,
                   b.gross,
                   b.tax,
                   b.recvd,
                   b.due,
                   b.due_date,
                   ad.DELIVERY_METHOD_ID,
                   ad.DELIVERY_METHOD_NAME,
                   b.BILL_STATUS BILL_STATUS_ID,
                   b_STATUS.NAME BILL_STATUS,
                   B.BILL_TYPE BILL_TYPE_ID,
                   b_type.NAME BILL_TYPE,
                   b.CURRENCY_ID,
                   per.POSITION BILL_TYPE_POSITION,
                   NVL(cus.SHORT_NAME,cus.CUSTOMER) CUSTOMER_NAME,
                   ac.zip,
                   br.CONTRACTOR BRANCH_NAME,
                   ag.CONTRACTOR AGENT_NAME
              FROM bill_t b,
                   account_t a,
                   account_profile_t ap,
                   customer_t cus,
                   contract_t c,
                   account_contact_t ac,
                   contractor_t br,
                   contractor_t ag,
                   (SELECT KEY, NAME || '' ('' || KEY || '')'' NAME
                      FROM dictionary_t
                     WHERE parent_id = 4) b_status,
                   (SELECT KEY, NAME || '' ('' || KEY || '')'' NAME
                      FROM dictionary_t
                     WHERE parent_id = 3) b_type,
                     PERIOD_T per,
                     ad
            WHERE  b_status.KEY(+) = B.BILL_STATUS
                   AND SYSDATE BETWEEN AP.DATE_FROM AND NVL(AP.DATE_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                   AND b_type.KEY(+) = B.BILL_TYPE
                   AND b.account_id = a.account_id
                   AND br.CONTRACTOR_ID (+)= ap.branch_id
                   AND ag.CONTRACTOR_ID (+)=ap.agent_ID
                   AND ap.account_id = a.account_id
                   AND c.contract_id = ap.contract_id
                   AND per.PERIOD_ID = b.REP_PERIOD_ID
                   AND cus.CUSTOMER_ID = ap.CUSTOMER_ID
                   AND ac.ACCOUNT_ID = a.account_ID
                   AND ac.ADDRESS_TYPE = ''DLV''
                   AND a.account_type = ''J''                  
                   AND B.BILL_STATUS IN (''READY'',''CLOSED'')
                   AND B.BILL_TYPE NOT IN (''C'')
                   AND ad.account_Id (+) = a.account_id 
                   AND A.STATUS NOT IN (''DBL'',''T'')';
            
      IF (p_bill_type IS NOT NULL) THEN
          v_sql := v_sql || ' AND B.BILL_TYPE = ''' ||p_bill_type || '''';             
       END IF;
       
        IF p_account_no IS NOT NULL THEN       
           v_sql := v_sql || ' AND A.ACCOUNT_NO';    
           IF INSTR(p_account_no,'*') > 0 THEN
              v_sql := v_sql || ' LIKE ''' || UPPER(REPLACE(p_account_no,'*','%'))||'''';         
           ELSE
              v_sql := v_sql || ' = ''' || p_account_no || '''';
           END IF;
        END IF;
        
        IF p_contract_no IS NOT NULL THEN       
           v_sql := v_sql || ' AND C.CONTRACT_NO';    
           IF INSTR(p_contract_no,'*') > 0 THEN
              v_sql := v_sql || ' LIKE ''' || UPPER(REPLACE(p_contract_no,'*','%')) || '''';         
           ELSE
              v_sql := v_sql || ' = ''' || p_contract_no || '''';
           END IF;
        END IF;
        
        IF p_bill_no IS NOT NULL THEN       
           v_sql := v_sql || ' AND b.BILL_NO';    
           IF INSTR(p_bill_no,'*') > 0 THEN
              v_sql := v_sql || ' LIKE ''' || UPPER(REPLACE(p_bill_no,'*','%')) || '''';         
           ELSE
              v_sql := v_sql || ' = ''' || p_bill_no || '''';
           END IF;
        END IF;       
       
       IF (p_rep_period_id IS NOT NULL) THEN
          v_sql := v_sql || ' AND b.REP_PERIOD_ID = '|| p_rep_period_id;       
       END IF;
       
       IF (p_show_bill0 != 1) THEN
          v_sql := v_sql || ' AND b.total <> 0';
       END IF;
       
       IF p_manager_id IS NOT NULL THEN
             v_sql := v_sql ||
               ' AND EXISTS (
                    SELECT *
                      FROM (SELECT ap.*
                              FROM account_profile_t ap, sale_curator_t s
                             WHERE s.manager_id = ' || p_manager_id || ' AND ap.account_id = s.account_id
                            UNION
                            SELECT ap.*
                              FROM account_profile_t ap, sale_curator_t s
                             WHERE     s.manager_id = ' || p_manager_id || '
                                   AND ap.contract_id = s.contract_id
                                   AND NOT EXISTS
                                              (SELECT *
                                                 FROM account_profile_t ap2, sale_curator_t s2
                                                WHERE     s2.manager_id <> ' || p_manager_id || '
                                                      AND ap2.account_id = s2.account_id
                                                      AND ap2.account_id = ap.account_id)
                            UNION
                            SELECT ap.*
                              FROM account_profile_t ap, sale_curator_t s
                             WHERE     s.manager_id = ' || p_manager_id || '
                                   AND ap.agent_id = s.contractor_id
                                   AND NOT EXISTS
                                              (SELECT *
                                                 FROM account_profile_t ap2, sale_curator_t s2
                                                WHERE     s2.manager_id <> ' || p_manager_id || '
                                                      AND ap2.account_id = s2.account_id
                                                      AND ap2.account_id = ap.account_id)
                                   AND NOT EXISTS
                                              (SELECT *
                                                 FROM account_profile_t ap2, sale_curator_t s2
                                                WHERE     s2.manager_id <> ' || p_manager_id || '
                                                      AND ap2.contract_id = s2.contract_id
                                                      AND ap2.account_id = ap.account_id)
                            UNION
                            SELECT ap.*
                              FROM account_profile_t ap, sale_curator_t s
                             WHERE     s.manager_id = ' || p_manager_id || '
                                   AND ap.agent_id = s.contractor_id
                                   AND NOT EXISTS
                                              (SELECT *
                                                 FROM account_profile_t ap2, sale_curator_t s2
                                                WHERE     s2.manager_id <> ' || p_manager_id || '
                                                      AND ap2.account_id = s2.account_id
                                                      AND ap2.account_id = ap.account_id)
                                   AND NOT EXISTS
                                              (SELECT *
                                                 FROM account_profile_t ap2, sale_curator_t s2
                                                WHERE     s2.manager_id <> ' || p_manager_id || '
                                                      AND ap2.contract_id = s2.contract_id
                                                      AND ap2.account_id = ap.account_id)
                                   AND NOT EXISTS
                                              (SELECT *
                                                 FROM account_profile_t ap2, sale_curator_t s2
                                                WHERE     s2.manager_id <> ' || p_manager_id || '
                                                      AND ap2.agent_id = s2.contractor_id
                                                      AND ap2.account_id = ap.account_id)) t
                       WHERE T.ACCOUNT_ID = A.ACCOUNT_ID)';           
        END IF; 
       
       IF (p_delivery_method_id IS NOT NULL) THEN
          v_sql := v_sql || ' AND ad.delivery_method_id = '|| p_delivery_method_id;       
       END IF;
       
       IF sort_by IS NULL THEN
          v_sql := v_sql || ' ORDER BY c.contract_no, b.bill_no, b.rep_period_id desc';
       ELSIF sort_by = 'CONTRACT_NO' THEN
          v_sql := v_sql || ' ORDER BY c.contract_no';                  
       ELSIF sort_by = 'ACCOUNT_NO' THEN
          v_sql := v_sql || ' ORDER BY a.account_no';
       ELSIF sort_by = 'CUSTOMER_NAME' THEN
          v_sql := v_sql || ' ORDER BY cus.customer';
       ELSIF sort_by = 'POST_INDEX' THEN
          v_sql := v_sql || ' ORDER BY ac.ZIP';
       ELSIF sort_by = 'AGENT_NAME' THEN
          v_sql := v_sql || ' ORDER BY ag.CONTRACTOR';          
       END IF;
     
       open p_recordset for v_sql;
exception
    when others then
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        if p_recordset%ISOPEN then 
            close p_recordset;
        end if;
end;

END PK50_W_ACCOUNT_INFO;
/
