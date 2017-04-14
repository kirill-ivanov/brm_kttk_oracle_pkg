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
         p_phone_number  IN VARCHAR2     -- номер телефона
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
      p_customer_name IN VARCHAR2,    -- название компании
      p_customer_inn  IN VARCHAR2,    -- ИНН компании
      p_customer_kpp  IN VARCHAR2,    -- КПП компании
      p_phone_number  IN VARCHAR2     -- номер телефона
);

--================================================================================================================
-- Получение детальной информации по л/с для ЮРИКА
    PROCEDURE ACCOUNT_INFO_Y( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- лицевой счет
    );     
END PK50_W_ACCOUNT_INFO;
/
CREATE OR REPLACE PACKAGE BODY PK50_W_ACCOUNT_INFO
IS

-- Пакет WEB-интерфейсов
--
--
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
      p_phone_number  IN VARCHAR2     -- номер телефона
)
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_SEARCH_F';
    v_retcode            INTEGER;
    v_sql                VARCHAR2(10000);
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
                   dictionary_t dict
             WHERE acc.account_id = PROF.ACCOUNT_ID
                   AND C.CONTRACT_ID = PROF.CONTRACT_ID
                   AND SUB.SUBSCRIBER_ID = PROF.SUBSCRIBER_ID
                   AND CONTR.CONTRACTOR_ID = PROF.CONTRACTOR_ID
                   AND DICT.PARENT_ID = 2
                   AND DICT.KEY = ACC.STATUS
                   AND ac.account_id = acc.account_id
                   AND AC.ADDRESS_TYPE = ''DLV''
                   AND acc.ACCOUNT_TYPE = ''P''';    
    
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
    
    IF p_order_no IS NOT NULL THEN       
       v_sql := v_sql || ' AND EXISTS (
                        SELECT *
                          FROM order_t o
                         WHERE o.ACCOUNT_ID = acc.account_id 
                               AND o.ORDER_NO';    
       IF INSTR(p_order_no,'*') > 0 THEN
          v_sql := v_sql || ' LIKE ''' || REPLACE(p_order_no,'*','%') || '''';         
       ELSE
          v_sql := v_sql || ' = ''' || p_order_no || '''';
       END IF;
       v_sql := v_sql || ')';
    END IF;
    
    IF p_client_f IS NOT NULL THEN       
       v_sql := v_sql || ' AND UPPER(SUB.LAST_NAME)';    
       IF INSTR(p_client_f,'*') > 0 THEN
          v_sql := v_sql || ' LIKE UPPER('''||REPLACE(p_client_f,'*','%')||''')';         
       ELSE
          v_sql := v_sql || ' = UPPER(''' || p_client_f || ''')';
       END IF;
    END IF;
    
    IF p_client_n IS NOT NULL THEN       
       v_sql := v_sql || ' AND UPPER(SUB.FIRST_NAME)';    
       IF INSTR(p_client_n,'*') > 0 THEN
          v_sql := v_sql || ' LIKE UPPER('''||REPLACE(p_client_n,'*','%')||''')';
       ELSE
          v_sql := v_sql || ' = UPPER(''' || p_client_n || ''')';
       END IF;
    END IF;
    
    IF p_client_p IS NOT NULL THEN       
       v_sql := v_sql || ' AND UPPER(SUB.MIDDLE_NAME)';
       IF INSTR(p_client_p,'*') > 0 THEN
          v_sql := v_sql || ' LIKE UPPER('''||REPLACE(p_client_p,'*','%')||''')';
       ELSE
          v_sql := v_sql || ' = UPPER(''' || p_client_p || ''')';
       END IF;
    END IF;

    IF p_date_from IS NOT NULL THEN
       v_date_temp := TO_CHAR(p_date_from,'DD.MM.YYYY HH24:MI:SS');
       v_sql := v_sql || ' AND acc.CREATE_DATE >= TO_DATE('''|| v_date_temp || ''',''DD.MM.YYYY HH24:MI:SS'')';
    END IF;
    
    IF p_date_to IS NOT NULL THEN
       v_date_temp := TO_CHAR(p_date_to,'DD.MM.YYYY HH24:MI:SS');
       v_sql := v_sql || ' AND acc.CREATE_DATE <= TO_DATE('''|| v_date_temp || ''',''DD.MM.YYYY HH24:MI:SS'')';
    END IF;    
    
    IF p_phone_number IS NOT NULL THEN
      v_sql := v_sql || 
            ' AND EXISTS (
                    SELECT 
                        * FROM 
                            ORDER_T o,
                            ORDER_PHONES_T op
                         WHERE 
                            O.ORDER_ID = op.order_id
                            AND PHONE_NUMBER';                            

          IF INSTR(p_phone_number,'*') > 0 THEN
             v_sql := v_sql || ' LIKE '''||REPLACE(p_phone_number,'*','%')||'';
          ELSE
             v_sql := v_sql || ' = ''' || p_phone_number || '''';
          END IF; 
      
                           
        v_sql := v_sql || ' AND acc.account_id = o.account_id)';      
    END IF;
    
    -- Считаем, сколько строк возвращает запрос
    EXECUTE IMMEDIATE 'SELECT COUNT(*) '|| v_sql || ' AND ROWNUM <=501' INTO v_cnt;            
    
    -- Если больше 500 - говорим, что много и нужно уточнить поиск
    IF v_cnt > 500 THEN
       p_result := 'Найдено больше 500 записей. Уточните поиск';
    ELSE
    -- Если все ОК - формируем SQL
      open p_recordset for 
           'SELECT acc.ACCOUNT_ID,
                   acc.ACCOUNT_NO,
                   C.CONTRACT_ID,
                   C.CONTRACT_NO,
                   acc.ACCOUNT_TYPE,
                   acc.CREATE_DATE,
                   acc.STATUS STATUS_ID,
                   dict.NAME STATUS,
                   acc.BALANCE,
                   acc.CREATE_DATE,
                   sub.LAST_NAME,
                   sub.FIRST_NAME,
                   sub.MIDDLE_NAME ' || v_sql;
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
--              CUR.CURRENCY_NAME,
              AP.VAT,        
              ACC.STATUS,
              D_ACC.NAME STATUS_NAME,
              M.MANAGER_ID,
              M.LAST_NAME || ' ' || M.FIRST_NAME || ' ' || M.MIDDLE_NAME MANAGER_NAME,
              CTR.CONTRACTOR_ID,
              CTR.CONTRACTOR_TYPE,
              CTR.ERP_CODE CONTRACTOR_ERP_CODE,
              CTR.INN CONTRACTOR_INN,
              CTR.KPP CONTRACTOR_KPP,
              CTR.CONTRACTOR CONTRACTOR_NAME,
              CTR.SHORT_NAME CONTRACTOR_NAME_SHORT,              
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
              SUB.MIDDLE_NAME CLIENT_F_MIDDLE_NAME  
           FROM 
                 ACCOUNT_T ACC,
                 SALE_CURATOR_T MI,
                 MANAGER_T M,
                 ACCOUNT_PROFILE_T AP,
                 DICTIONARY_T D_ACC,
                 DICTIONARY_T D_ACC_TYPE,
                 CONTRACTOR_T CTR,
                 CONTRACTOR_T BRANCH_CTR,
                 CONTRACTOR_T AGENT_CTR,
                 ACCOUNT_CONTACT_T ACC_CONT,
                 SUBSCRIBER_T SUB
          WHERE
              ACC.ACCOUNT_ID = MI.ACCOUNT_ID(+)    
              AND MI.MANAGER_ID = M.MANAGER_ID(+)
              AND AP.ACCOUNT_ID = ACC.ACCOUNT_ID
              AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
              AND D_ACC.KEY (+)= ACC.STATUS
              AND D_ACC.PARENT_ID = 2                    -- Статус лицевого счета 
              AND D_ACC_TYPE.KEY (+)= ACC.ACCOUNT_TYPE 
              AND D_ACC_TYPE.PARENT_ID = 1               -- Тип лицевого счетавого счета
              AND CTR.CONTRACTOR_ID (+)= AP.CONTRACTOR_ID
              AND BRANCH_CTR.CONTRACTOR_ID (+)= AP.BRANCH_ID
              AND AGENT_CTR.CONTRACTOR_ID (+)= AP.AGENT_ID
              AND ACC_CONT.ACCOUNT_ID = ACC.ACCOUNT_ID (+)
              AND ACC_CONT.ADDRESS_TYPE = 'DLV'
              AND SUB.SUBSCRIBER_ID = AP.SUBSCRIBER_ID
              AND (ACC_CONT.DATE_TO IS NULL OR SYSDATE BETWEEN ACC_CONT.DATE_FROM AND ACC_CONT.DATE_TO)
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
      p_customer_name IN VARCHAR2,    -- название компании
      p_customer_inn  IN VARCHAR2,    -- ИНН компании
      p_customer_kpp  IN VARCHAR2,    -- КПП компании
      p_phone_number  IN VARCHAR2     -- номер телефона
)
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_SEARCH_Y';
    v_retcode            INTEGER;
    v_sql                VARCHAR2(10000);
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
                   account_contact_t ac
             WHERE acc.account_id = PROF.ACCOUNT_ID
                   AND C.CONTRACT_ID = PROF.CONTRACT_ID
                   AND CUS.CUSTOMER_ID = PROF.CUSTOMER_ID
                   AND CONTR.CONTRACTOR_ID = PROF.CONTRACTOR_ID
                   AND ac.account_id = acc.account_id
                   AND AC.ADDRESS_TYPE = ''DLV''
                   AND acc.ACCOUNT_TYPE = ''J''';    
    
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
       v_sql := v_sql || ' AND acc.CREATE_DATE >= TO_DATE('''|| v_date_temp || ''',''DD.MM.YYYY HH24:MI:SS'')';
    END IF;
    
    IF p_date_to IS NOT NULL THEN
       v_date_temp := TO_CHAR(p_date_to,'DD.MM.YYYY HH24:MI:SS');
       v_sql := v_sql || ' AND acc.CREATE_DATE <= TO_DATE('''|| v_date_temp || ''',''DD.MM.YYYY HH24:MI:SS'')';
    END IF;    
    
    IF p_phone_number IS NOT NULL THEN
      v_sql := v_sql || 
            ' AND EXISTS (
                    SELECT 
                        * FROM 
                            ORDER_T o,
                            ORDER_PHONES_T op
                         WHERE 
                            O.ORDER_ID = op.order_id
                            AND PHONE_NUMBER';                            

          IF INSTR(p_phone_number,'*') > 0 THEN
             v_sql := v_sql || ' LIKE '''||REPLACE(p_phone_number,'*','%')||'';
          ELSE
             v_sql := v_sql || ' = ''' || p_phone_number || '''';
          END IF; 
      
                           
        v_sql := v_sql || ' AND acc.account_id = o.account_id)';      
    END IF;
    
    -- Считаем, сколько строк возвращает запрос
    EXECUTE IMMEDIATE 'SELECT COUNT(*) '|| v_sql || ' AND ROWNUM <=501' INTO v_cnt;            
    
    -- Если больше 500 - говорим, что много и нужно уточнить поиск
    IF v_cnt > 500 THEN
       p_result := 'Найдено больше 500 записей. Уточните поиск';
    ELSE
    -- Если все ОК - формируем SQL
      open p_recordset for 
           'SELECT acc.ACCOUNT_ID,
                   acc.ACCOUNT_NO,
                   C.CONTRACT_ID,
                   C.CONTRACT_NO,
                   acc.ACCOUNT_TYPE,
                   acc.CREATE_DATE,
                   acc.STATUS,
                   acc.BALANCE,
                   acc.CREATE_DATE,
                   CUS.CUSTOMER CUSTOMER_NAME,
                   CUS.SHORT_NAME CUSTOMER_NAME_SHORT,
                   CUS.INN CUSTOMER_INN,
                   CUS.KPP CUSTOMER_KPP ' || v_sql;
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
              AP.VAT,
              ACC.STATUS,
              D_ACC.NAME STATUS_NAME,
              M.MANAGER_ID,
              M.LAST_NAME || ' ' || M.FIRST_NAME || ' ' || M.MIDDLE_NAME MANAGER_NAME,
              CTR.CONTRACTOR_ID,
              CTR.CONTRACTOR_TYPE,
              CTR.ERP_CODE CONTRACTOR_ERP_CODE,
              CTR.INN CONTRACTOR_INN,
              CTR.KPP CONTRACTOR_KPP,
              CTR.CONTRACTOR CONTRACTOR_NAME,
              CTR.SHORT_NAME CONTRACTOR_NAME_SHORT,              
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
              CUS.CUSTOMER CUSTOMER_NAME,
              CUS.SHORT_NAME CUSTOMER_NAME_SHORT,
              CUS.ERP_CODE CUSTOMER_ERP_CODE,
              CUS.INN CUSTOMER_INN,
              CUS.KPP CUSTOMER_KPP
           FROM 
                 ACCOUNT_T ACC,
                 SALE_CURATOR_T MI,
                 MANAGER_T M,
                 ACCOUNT_PROFILE_T AP,
                 DICTIONARY_T D_ACC,
                 DICTIONARY_T D_ACC_TYPE,
                 CONTRACTOR_T CTR,
                 CONTRACTOR_T BRANCH_CTR,
                 CONTRACTOR_T AGENT_CTR,
                 ACCOUNT_CONTACT_T ACC_CONT,
                 CUSTOMER_T CUS
          WHERE
              ACC.ACCOUNT_ID = MI.ACCOUNT_ID(+)    
              AND MI.MANAGER_ID = M.MANAGER_ID(+)
              AND AP.ACCOUNT_ID = ACC.ACCOUNT_ID
              AND CUS.CUSTOMER_ID = AP.CUSTOMER_ID
              AND (AP.DATE_TO IS NULL OR SYSDATE BETWEEN AP.DATE_FROM AND AP.DATE_TO)
              AND D_ACC.KEY (+)= ACC.STATUS
              AND D_ACC.PARENT_ID = 2                    -- Статус лицевого счета 
              AND D_ACC_TYPE.KEY (+)= ACC.ACCOUNT_TYPE 
              AND D_ACC_TYPE.PARENT_ID = 1               -- Тип лицевого счетавого счета
              AND CTR.CONTRACTOR_ID (+)= AP.CONTRACTOR_ID
              AND BRANCH_CTR.CONTRACTOR_ID (+)= AP.BRANCH_ID
              AND AGENT_CTR.CONTRACTOR_ID (+)= AP.AGENT_ID
              AND ACC_CONT.ACCOUNT_ID = ACC.ACCOUNT_ID (+)
              AND ACC_CONT.ADDRESS_TYPE = 'DLV'
              AND (ACC_CONT.DATE_TO IS NULL OR SYSDATE BETWEEN ACC_CONT.DATE_FROM AND ACC_CONT.DATE_TO)
              AND ACC.ACCOUNT_ID = p_account_id;  
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;

END PK50_W_ACCOUNT_INFO;
/
