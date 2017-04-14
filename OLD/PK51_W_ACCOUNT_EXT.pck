CREATE OR REPLACE PACKAGE PK51_W_ACCOUNT_EXT
IS
    --
    -- Пакет для поддержки импорта данных из НБ
    -- event_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK51_W_ACCOUNT_EXT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

--================================================================================================================
-- Получение детальной информации по контракту
    PROCEDURE CONTRACT_INFO( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_contract_id   IN NUMBER    -- номер контракта
    );
    
--================================================================================================================
-- Получение списка адресов по лицевому счету
    PROCEDURE ACCOUNT_ADDRESS_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- номер лицевого счета
         p_date          IN DATE
    ); 

--================================================================================================================
-- Получение всю историю адресов по лицевому счету
    PROCEDURE ACCOUNT_ADDRESS_HISTORY_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- номер лицевого счета
    ); 
    
--================================================================================================================
-- Получение заказов на лицевом счете
    PROCEDURE ORDER_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- номер лицевого счета
         p_order_id      IN NUMBER     -- номер лицевого счета    
    );

--================================================================================================================
-- Получение информации по  заказу (его начинка)
    PROCEDURE ORDER_BODY( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_order_id      IN NUMBER    -- номер заказа
    );
    
    -- Получение информации по  заказу (его начинка)
    PROCEDURE ORDER_BODY( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_order_id      IN NUMBER,    -- номер заказа
         p_order_body_id IN NUMBER     -- ID компонента
    );
        
--================================================================================================================
-- Получение доп. информации по  заказу (блокировки)
    PROCEDURE ORDER_LOCK_HISTORY( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_order_id      IN NUMBER    -- номер заказа
    ) ;

--================================================================================================================
-- Получение списка счетов
    PROCEDURE BILL_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- номер лицевого счета
         p_bill_id       IN NUMBER,     -- номер счета         
         p_rep_period_id IN NUMBER      -- период счета                  
    ); 
    
--================================================================================================================
-- Получение позиций счета (item-ы)
    PROCEDURE BILL_ITEM_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_bill_id       IN NUMBER,    -- номер счета
         p_rep_period_id IN NUMBER      -- период счета                  
    );    

--================================================================================================================
-- Получение invoice счета
    PROCEDURE BILL_INVOICE_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_bill_id       IN NUMBER,    -- номер счета
         p_rep_period_id IN NUMBER      -- период счета                  
    );

--================================================================================================================
-- Получение списка телефонов
    PROCEDURE PHONES_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- номер лицевого счета
         p_order_id      IN NUMBER    -- номер лицевого счета         
    ); 

--================================================================================================================
-- Получение списка телефонов
    PROCEDURE PHONES_LIST_BY_PERIOD( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- номер лицевого счета
         p_order_id      IN NUMBER,     -- номер заказа      
         p_date_from     IN DATE,
         p_date_to       IN DATE
    );

--================================================================================================================
-- Получение списка телефонов по заказу (с историзмом)
    PROCEDURE PHONES_LIST_BY_ORDER( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_order_id      IN NUMBER     -- номер заказа        
    );      
    
--================================================================================================================
-- Получение списка телефонов с адресами установок
    PROCEDURE PHONES_LIST_WITH_ADR_SET( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- номер лицевого счета
         p_order_id      IN NUMBER     -- номер лицевого счета         
    );     
 
--================================================================================================================
-- Получение списка телефонов (интервалами)
    PROCEDURE PHONES_RANGE( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- номер лицевого счета
         p_order_id      IN NUMBER    -- номер лицевого счета    
    ); 
    
--================================================================================================================
-- Получение списка подписантов
    PROCEDURE SIGNER_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- номер лицевого счета
    );           
    
--================================================================================================================    
    -- Получение способов доставки для лицевого счета
    PROCEDURE DELIVERY_METHOD_BY_ACCOUNT( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- номер лицевого счета
    );    
    
--================================================================================================================
-- Получение баланса лицевого счета 
-- Складывается из текущего баланс минус текущие начисления
    PROCEDURE ACCOUNT_BALANCE_ONLINE( 
         p_balance_online     OUT NUMBER, 
         p_account_id         IN NUMBER    -- номер лицевого счета 
    );    
    
--================================================================================================================
-- Получение баланса лицевого счета по номеру телефона
-- Нужно для автоинформирования
    PROCEDURE ACCOUNT_BALANCE_BY_PHONENUMBER( 
         p_balance        OUT NUMBER, 
         p_phone_number   IN VARCHAR2    -- номер телефонного номера
    );    
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Проверка номера телефона на пересечение
-- Возвращает кол-во пересечений
PROCEDURE Check_phonenumber_cross (
        p_count         OUT INTEGER,
        p_phonenumber   IN VARCHAR2,
        p_date_from     IN DATE DEFAULT SYSDATE
    );
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Получить список лицевых счетов, на котором висит заданный контрагент
-- за исключением текущего лицевого счета
PROCEDURE GET_ACCOUNT_LIST_BY_CUSTOMER (
        p_recordset     OUT t_refc, 
        p_customer_id         IN NUMBER,
        p_account_id_exclude  IN NUMBER
);
END PK51_W_ACCOUNT_EXT;
/
CREATE OR REPLACE PACKAGE BODY PK51_W_ACCOUNT_EXT
IS

-- Дополнительные основные процедуры для WEB-интерфейса
-- Процедуры одни и те же как для физика, так и для юрика
--================================================================================================================
-- Получение детальной информации по контракту 
    PROCEDURE CONTRACT_INFO( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_contract_id   IN NUMBER    -- номер контракта
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'CONTRACT_INFO';
    v_retcode            INTEGER;
    v_sql                VARCHAR2(10000);
    v_date_temp          VARCHAR2(20);
    v_cnt                INTEGER;
begin
    open p_recordset for
          SELECT 
                C.CONTRACT_ID,
                C.CONTRACT_NO,
                C.DATE_FROM,
                C.DATE_TO,
                CL.CLIENT_ID,
                CL.CLIENT_NAME,
                M.MANAGER_ID,
                M.LAST_NAME || ' ' || M.FIRST_NAME || ' ' || M.MIDDLE_NAME MANAGER_NAME,
                C.MARKET_SEGMENT_ID,
                DICT_SEGMENT.NAME MARKET_SEGMENT_NAME,
                C.CLIENT_TYPE_ID,
                dict_client_type.name CLIENT_TYPE_NAME,
                C.XTTK_TYPE_ID,
                NULL XTTK_TYPE_NAME,
                GOVERMENT_TYPE,
                EXCL_TARIFF_CHANGE
            FROM
               CONTRACT_T C,
               CLIENT_T CL,
               SALE_CURATOR_T MI,
               MANAGER_T M,
               (SELECT *
                  FROM DICTIONARY_T
                 WHERE parent_id = 63) dict_segment,
               (SELECT *
                  FROM DICTIONARY_T
                 WHERE parent_id = 64) dict_client_type
        WHERE
            C.CLIENT_ID = CL.CLIENT_ID
            AND C.CONTRACT_ID = MI.CONTRACT_ID(+)
            AND MI.MANAGER_ID = M.MANAGER_ID(+)
            AND dict_segment.key_id(+) = C.MARKET_SEGMENT_ID
            AND dict_client_type.key_id(+) = C.CLIENT_TYPE_ID
            AND C.CONTRACT_ID = p_contract_id;  
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;

--================================================================================================================
-- Получение списка адресов по лицевому счету на опеределенную дату
    PROCEDURE ACCOUNT_ADDRESS_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- номер лицевого счета
         p_date          IN DATE       -- дата, на которую нужно получить адреса
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_ADDRESS_LIST';
    v_retcode            INTEGER;
    v_date               DATE;
begin
    IF p_date IS NULL THEN
       v_date := SYSDATE;
    ELSE
       v_date := p_date;
    END IF;  

    open p_recordset for
         SELECT
              AC.CONTACT_ID ADDRESS_ID,
              AC.ADDRESS_TYPE,
              AC.COUNTRY,
              AC.ZIP,
              AC.STATE,
              AC.CITY,
              AC.ADDRESS,
              AC.DATE_FROM,
              AC.DATE_TO,
              AC.PERSON,
              CASE
                WHEN AC.ADDRESS_TYPE='DLV' THEN 0
                  ELSE 1
                  END SORT_ID                  
            FROM
                  ACCOUNT_CONTACT_T AC,
                  ACCOUNT_T ACC
          WHERE ACC.ACCOUNT_ID = AC.ACCOUNT_ID     
              AND ((AC.DATE_TO IS NULL AND TRUNC(AC.DATE_FROM) <= v_date) OR v_date BETWEEN AC.DATE_FROM AND AC.DATE_TO)
              AND ACC.ACCOUNT_ID = p_account_id  
              ORDER BY SORT_ID;
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;
--================================================================================================================
-- Получение всю историю адресов по лицевому счету
    PROCEDURE ACCOUNT_ADDRESS_HISTORY_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- номер лицевого счета
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_ADDRESS_HISTORY_LIST';
    v_retcode            INTEGER;
begin
    open p_recordset for
         SELECT
              AC.CONTACT_ID ADDRESS_ID,
              AC.ADDRESS_TYPE,              
              AC.COUNTRY,
              AC.ZIP,
              AC.STATE,
              AC.CITY,
              AC.ADDRESS,
              AC.DATE_FROM,
              AC.DATE_TO,
              AC.PERSON,
              CASE
                WHEN AC.ADDRESS_TYPE='DLV' THEN 0
                  ELSE 1
                  END SORT_ID                  
            FROM
                  ACCOUNT_CONTACT_T AC,
                  ACCOUNT_T ACC
          WHERE ACC.ACCOUNT_ID = AC.ACCOUNT_ID     
              AND ACC.ACCOUNT_ID = p_account_id  
              ORDER BY SORT_ID, DATE_FROM, DATE_TO;
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;

--================================================================================================================
-- Получение заказов на лицевом счете
    PROCEDURE ORDER_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- номер лицевого счета
         p_order_id      IN NUMBER    -- номер лицевого счета         
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ORDER_LIST';
    v_retcode            INTEGER;
    v_sql                VARCHAR2(10000);    
begin
    v_sql := 'SELECT 
                 a.account_id,
                 a.account_no,
                 o.order_id,
                 o.order_no,
                 o.date_from,
                 CASE WHEN o.DATE_TO >= TO_DATE (''01.01.2050'', ''DD.MM.YYYY'') THEN NULL
                    ELSE o.DATE_TO END DATE_TO,
                 o.service_id,
                 S.SERVICE_CODE SERVICE_KEY,
                 S.SERVICE SERVICE_NAME,
                 S.SERVICE SERVICE_NAME_SHORT,
                 S.ERP_PRODCODE SERVICE_ERP_CODE,
                 O.RATEPLAN_ID,
                 TRF.RATEPLAN_NAME,
                 TRF.RATESYSTEM_ID RATEPLAN_SYSTEM_ID,
                 TRF.RATEPLAN_CODE RATEPLAN_CODE,
                 O.AGENT_RATEPLAN_ID,
                 TRF_AGENT.RATEPLAN_NAME AGENT_RATEPLAN_NAME,
                 TRF_AGENT.RATESYSTEM_ID AGENT_RATEPLAN_SYSTEM_ID,
                 TRF_AGENT.RATEPLAN_CODE AGENT_RATEPLAN_CODE,
                 ord_ph.CNT PHONE_COUNT,
                 o.STATUS,
                 ol.LOCK_TYPE_ID,
                 ol.LOCK_TYPE_NAME,
                 ol.LOCK_REASON,
                 ol.LOCKED_BY,
                 oi.POINT_SRC,
                 oi.POINT_DST,
                 OI.SPEED_STR,
                 OI.SPEED_UNIT_ID,
                 OI.SPEED_VALUE
            FROM 
                 account_t a,
                 order_t o,
                 service_t s,
                 rateplan_t trf,
                 rateplan_t trf_agent,
                 (  SELECT order_Id, COUNT (phone_number) CNT
                      FROM ORDER_PHONES_T
                  GROUP BY ORDER_ID) ord_ph,
                  (
                 SELECT ord_lock.order_id, ORD_LOCK.LOCK_TYPE_ID, ORD_LOCK.LOCK_REASON,ORD_LOCK.LOCKED_BY, DICT.NAME LOCK_TYPE_NAME
                   FROM ORDER_LOCK_T ord_lock, dictionary_t dict
                  WHERE (SYSDATE BETWEEN ORD_LOCK.DATE_FROM AND ORD_LOCK.DATE_TO OR (ORD_LOCK.DATE_FROM<=SYSDATE AND ORD_LOCK.DATE_TO IS NULL))
                        AND DICT.PARENT_ID = 9
                        AND DICT.KEY_ID = ORD_LOCK.LOCK_TYPE_ID) ol,
                  ORDER_INFO_T OI
           WHERE a.ACCOUNT_ID = o.account_Id
                 AND O.SERVICE_ID = S.SERVICE_ID(+)
                 AND o.order_id = ORD_PH.ORDER_ID(+)
                 AND o.rateplan_id = TRF.RATEPLAN_ID (+)
                 and o.agent_rateplan_id = trf_agent.rateplan_id (+)
                 AND O.ORDER_ID = OI.ORDER_ID (+) 
                 AND O.ORDER_ID = ol.ORDER_ID(+)';

    IF p_account_id IS NOT NULL THEN 
          v_sql := v_sql || ' AND o.account_id = ' || p_account_id;
     ELSE
          v_sql := v_sql || ' AND o.order_id = ' || p_order_id;       
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
-- Получение информации по  заказу (его начинка)
    PROCEDURE ORDER_BODY( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_order_id      IN NUMBER    -- номер заказа
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ORDER_BODY';
    v_retcode            INTEGER;
begin
    open p_recordset for
         SELECT 
                 OB.ORDER_BODY_ID,
                 OB.ORDER_ID,
                 O.ORDER_NO,
                 O.SERVICE_ID,
                 OB.CHARGE_TYPE CHARGE_TYPE_ID,
                 DICT.NAME CHARGE_TYPE,
                 OB.SUBSERVICE_ID,
                 CASE 
                     WHEN OB.SUBSERVICE_ID = 36 THEN SS.SUBSERVICE || ' ('||OB.FREE_VALUE||' мин.)'
                 ELSE SS.SUBSERVICE
                   END SUBSERVICE_NAME,
                 CASE 
                     WHEN OB.SUBSERVICE_ID = 36 THEN SS.SHORTNAME || ' ('||OB.FREE_VALUE||' мин.)'
                 ELSE SS.SHORTNAME
                   END SUBSERVICE_NAME_SHORT,
                 SS.SUBSERVICE_KEY,
                 OB.RATEPLAN_ID,
                 TRF.RATEPLAN_NAME,
                 TRF.RATESYSTEM_ID RATEPLAN_SYSTEM_ID,
                 TRF.RATEPLAN_CODE RATEPLAN_CODE,                                                   
                 IP.POINT_SRC,
                 IP.POINT_DST,
                 IP.SPEED_STR,
                 OB.QUANTITY,                 
                 RATE_VALUE,  
                 OB.RATE_RULE_ID,
                 dict_rate_rule.name RATE_RULE_NAME,
                 OB.RATE_LEVEL_ID,
                 dict_rate_level.name RATE_LEVEL_NAME,                 
                 OB.CURRENCY_ID,
                 ob.date_from,
                 CASE
                     WHEN ob.DATE_TO >= TO_DATE ('01.01.2050', 'DD.MM.YYYY') THEN NULL
                            ELSE ob.DATE_TO
                         END DATE_TO
            FROM order_t o, 
                 order_body_t ob, 
                 subservice_t ss,
                 dictionary_t dict,
                 dictionary_t dict_rate_level,
                 dictionary_t dict_rate_rule,
                 rateplan_t trf,      
                 order_info_t ip           
           WHERE o.order_id = ob.ORDER_ID
                 AND ob.rateplan_id = trf.rateplan_id (+)
                 AND OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
                 AND dict.key (+)= ob.charge_type
                 AND dict.parent_id (+)= 7
                 AND dict_rate_rule.key_id (+)= ob.RATE_RULE_ID
                 AND dict_rate_rule.parent_id (+)= 24
                 AND dict_rate_level.key_id (+)= ob.RATE_LEVEL_ID
                 AND dict_rate_level.parent_id (+)= 23                 
                 AND o.order_id = IP.ORDER_ID (+) 
                 AND (OB.SUBSERVICE_ID <> 32 AND OB.CHARGE_TYPE <> 'ONT')
                 AND ob.order_id = p_order_id;  
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;       

--================================================================================================================
-- Получение информации по  заказу (его начинка)
    PROCEDURE ORDER_BODY( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_order_id      IN NUMBER,    -- номер заказа
         p_order_body_id IN NUMBER
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ORDER_BODY';
    v_retcode            INTEGER;
begin
    open p_recordset for
         SELECT 
                 OB.ORDER_BODY_ID,
                 OB.ORDER_ID,
                 O.ORDER_NO,
                 O.SERVICE_ID,
                 OB.CHARGE_TYPE CHARGE_TYPE_ID,
                 DICT.NAME CHARGE_TYPE,
                 OB.SUBSERVICE_ID,
                 CASE 
                     WHEN OB.SUBSERVICE_ID = 36 THEN SS.SUBSERVICE || ' ('||OB.FREE_VALUE||' мин.)'
                 ELSE SS.SUBSERVICE
                   END SUBSERVICE_NAME,
                 CASE 
                     WHEN OB.SUBSERVICE_ID = 36 THEN SS.SHORTNAME || ' ('||OB.FREE_VALUE||' мин.)'
                 ELSE SS.SHORTNAME
                   END SUBSERVICE_NAME_SHORT,
                 SS.SUBSERVICE_KEY,
                 OB.RATEPLAN_ID,
                 TRF.RATEPLAN_NAME,
                 TRF.RATESYSTEM_ID RATEPLAN_SYSTEM_ID,
                 TRF.RATEPLAN_CODE RATEPLAN_CODE,                                                   
--                 IP.POINT_SRC,
--                 IP.POINT_DST,
--                 IP.SPEED_STR,
                 OB.QUANTITY,                 
                 RATE_VALUE,  
                 OB.RATE_RULE_ID,
                 dict_rate_rule.name RATE_RULE_NAME,
                 OB.RATE_LEVEL_ID,
                 dict_rate_level.name RATE_LEVEL_NAME,                 
                 OB.CURRENCY_ID,
                 ob.date_from,
                 CASE
                     WHEN ob.DATE_TO >= TO_DATE ('01.01.2050', 'DD.MM.YYYY') THEN NULL
                            ELSE ob.DATE_TO
                         END DATE_TO
            FROM order_t o, 
                 order_body_t ob, 
                 subservice_t ss,
                 dictionary_t dict,
                 dictionary_t dict_rate_level,
                 dictionary_t dict_rate_rule,
                 rateplan_t trf                 
           WHERE o.order_id = ob.ORDER_ID
                 AND ob.rateplan_id = trf.rateplan_id (+)
                 AND OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
                 AND dict.key (+)= ob.charge_type
                 AND dict.parent_id (+)= 7
                 AND dict_rate_rule.key_id (+)= ob.RATE_RULE_ID
                 AND dict_rate_rule.parent_id (+)= 24
                 AND dict_rate_level.key_id (+)= ob.RATE_LEVEL_ID
                 AND dict_rate_level.parent_id (+)= 23                 
                 AND (OB.SUBSERVICE_ID <> 32 AND OB.CHARGE_TYPE <> 'ONT')
                 AND (ob.order_id = p_order_id OR p_order_id IS NULL)
                 AND (ob.order_body_id = p_order_body_id OR p_order_body_id IS NULL);
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;   
--================================================================================================================
-- Получение доп. информации по  заказу (блокировки)
    PROCEDURE ORDER_LOCK_HISTORY( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_order_id      IN NUMBER    -- номер заказа
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ORDER_LOCK_HISTORY';
    v_retcode            INTEGER;
begin
    open p_recordset for
         SELECT ol.*, d.NAME LOCK_TYPE_NAME
              FROM order_lock_t ol, dictionary_t d
             WHERE ol.lock_type_id = D.KEY_ID 
                   AND D.PARENT_ID = 9 
                   AND order_Id = p_order_id
          ORDER BY DATE_FROM;
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end; 

--================================================================================================================
-- Получение списка счетов
    PROCEDURE BILL_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,     -- номер лицевого счета
         p_bill_id       IN NUMBER,     -- номер счета         
         p_rep_period_id IN NUMBER      -- период счета
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'BILL_LIST';
    v_retcode            INTEGER;
    v_sql                VARCHAR2(10000);
begin
    v_sql := 'SELECT b.bill_id,
                   b.rep_period_id,
                   b.bill_no,
                   b.account_id,
                   a.account_no,
                   c.contract_id,
                   c.contract_no,
                   b.bill_date,
                   b.paid_to,
                   B.BILL_TYPE bill_type_id,
                   dic_type.name bill_type,
                   B.BILL_STATUS bill_status_id,
                   dic_status.name bill_status,
                   b.currency_id,
                   b.total,
                   b.recvd,
                   b.due,
                   b.due_date,
                   per_dictionary.position bill_type_position,
                   A.BILLING_ID
              FROM bill_t b,
                   account_t a,
                   account_profile_t ap,
                   contract_t c,
                   dictionary_t dic_type,
                   dictionary_t dic_status,
                   period_t per_dictionary
             WHERE A.ACCOUNT_ID = B.ACCOUNT_ID
                   AND SYSDATE BETWEEN ap.DATE_FROM AND NVL(ap.DATE_TO,TO_DATE(''01.01.2050'',''DD.MM.YYYY''))
                   AND ap.account_id = a.account_id
                   and ap.contract_id = c.contract_id
                   AND B.REP_PERIOD_ID = PER_DICTIONARY.PERIOD_ID (+)
                   AND DIC_TYPE.KEY(+) = B.BILL_TYPE
                   AND dic_type.parent_id(+) = 3
                   AND DIC_status.KEY(+) = B.BILL_STATUS
                   AND dic_status.parent_id(+) = 4';
    IF p_account_id IS NOT NULL THEN
      v_sql := v_sql || ' AND A.ACCOUNT_ID = ' || p_account_id;      
    ELSE       
      v_sql := v_sql || ' AND B.bill_id = ' || p_bill_id || ' AND B.rep_period_id = ' || p_rep_period_id;          
    END IF;
    
    v_sql := v_sql || ' ORDER BY b.rep_period_id desc';
    
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
-- Получение позиций счета (item-ы)
    PROCEDURE BILL_ITEM_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_bill_id       IN NUMBER,     -- номер счета
         p_rep_period_id IN NUMBER      -- период счета         
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'BILL_ITEM_LIST';
    v_retcode            INTEGER;
begin    
    open p_recordset for
         SELECT 
               b.bill_id,
               b.rep_period_id,
               b.bill_no,
               I.ORDER_ID,
               O.ORDER_NO,
               i.item_id,
               i.item_type item_type_id,
               dict_type.name item_type,
               i.charge_type charge_type_id,
               dict_charge_type.NAME charge_type,
               i.item_total,
               0 adjusted,
               NULL TRANSFERED,--i.transfered,
               i.recvd,               
               i.date_from,
               i.date_to,
               i.item_status item_status_id,
               dict_status.name item_status,
               i.service_id,
               S.SERVICE SERVICE_NAME,
               S.SERVICE_CODE SERVICE_KEY,
               S.SERVICE_SHORT SERVICE_NAME_SHORT,
               S.ERP_PRODCODE SERVICE_ERP_CODE,
               i.subservice_id,               
               SS.SUBSERVICE SUBSERVICE_NAME,
               SS.SHORTNAME SUBSERVICE_NAME_SHORT,               
               SS.SUBSERVICE_KEY,
               i.TAX_INCL,
               i.NOTES
          FROM 
               bill_t b,
               order_t o,
               item_t i, 
               service_t s, 
               subservice_t ss,
               dictionary_t dict_type,
               dictionary_t dict_status,              
               dictionary_t dict_charge_type               
         WHERE b.bill_id = i.bill_id
               AND o.ORDER_ID (+)= i.ORDER_ID
               AND i.service_id = S.SERVICE_ID(+)
               AND i.subservice_id = SS.SUBSERVICE_ID(+)
               AND dict_charge_type.key (+)= i.CHARGE_TYPE
               AND dict_charge_type.PARENT_ID (+)= 7
               AND dict_type.key (+)= i.item_type
               AND DICT_TYPE.PARENT_ID (+)= 5
               AND dict_status.key (+)= i.item_status
               AND dict_status.PARENT_ID (+)= 6
               AND b.bill_id = p_bill_id
               AND b.rep_period_id = p_rep_period_id;  
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;    

--================================================================================================================
-- Получение invoice счета
    PROCEDURE BILL_INVOICE_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_bill_id       IN NUMBER,    -- номер счета
         p_rep_period_id IN NUMBER     -- период счета
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'BILL_INVOICE_LIST';
    v_retcode            INTEGER;
begin
    open p_recordset for
         SELECT 
               INV.INV_ITEM_ID,
               INV.INV_ITEM_NAME,         
               INV.DATE_FROM,
               INV.DATE_TO,
               INV.SERVICE_ID,         
               S.SERVICE_CODE SERVICE_KEY,
               S.SERVICE SERVICE_NAME ,
               S.SERVICE_SHORT SERVICE_NAME_SHORT,
               S.ERP_PRODCODE SERVICE_ERP_CODE,
               INV.VAT,         
               INV.TAX,
               INV.GROSS,
               INV.TOTAL,
               INV.BILL_ID,
               B.BILL_NO
          FROM 
              invoice_item_t inv, 
              bill_t b, 
              service_t s
         WHERE B.BILL_ID = INV.BILL_ID
               AND S.SERVICE_ID(+) = INV.SERVICE_ID
               AND inv.bill_id = p_bill_id
               AND b.rep_period_id = p_rep_period_id 
         ORDER BY INV_ITEM_NO;
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;   

--================================================================================================================
-- Получение списка телефонов
    PROCEDURE PHONES_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- номер лицевого счета
         p_order_id      IN NUMBER     -- номер заказа      
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'PHONES_LIST';
    v_retcode            INTEGER;
begin
    IF p_account_id IS NOT NULL THEN    
        open p_recordset for
            SELECT op.*
              FROM order_phones_t op, order_t ord
             WHERE op.order_id = ord.order_id
                   AND account_id = p_account_id
                   AND ((op.DATE_TO IS NULL AND TRUNC(op.DATE_FROM) < SYSDATE) OR SYSDATE BETWEEN TRUNC(op.DATE_FROM) AND op.DATE_TO)
                   ;
     ELSE
        open p_recordset for
             SELECT *
                FROM order_phones_t op
             WHERE op.order_id = p_order_id
                   AND ((op.DATE_TO IS NULL AND TRUNC(op.DATE_FROM) < SYSDATE) OR SYSDATE BETWEEN TRUNC(op.DATE_FROM) AND op.DATE_TO)
                   ; 

     END IF;               
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end; 

--================================================================================================================
-- Получение списка телефонов
    PROCEDURE PHONES_LIST_BY_PERIOD( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- номер лицевого счета
         p_order_id      IN NUMBER,     -- номер заказа      
         p_date_from     IN DATE,
         p_date_to       IN DATE
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'PHONES_LIST_BY_PERIOD';
    v_retcode            INTEGER;
begin
    IF p_account_id IS NOT NULL THEN    
        /*
        open p_recordset for
            SELECT op.*
              FROM order_phones_t op, order_t ord
             WHERE op.order_id = ord.order_id
                   AND account_id = p_account_id
                   AND (op.date_from <p_date_to AND NVL(op.date_to,TO_DATE('01.01.2050','DD.MM.YYYY')) > p_date_from)
                   ;
         */
         
        open p_recordset for            
            WITH PH AS ( 
            SELECT ROW_NUMBER() OVER (PARTITION BY P.PHONE_NUMBER ORDER BY P.DATE_FROM DESC) RN, 
                P.*
                FROM ORDER_PHONES_T P, ORDER_T O
                WHERE O.ORDER_ID = P.ORDER_ID
                AND O.ACCOUNT_ID = p_account_id
           )
           SELECT *
               FROM PH
               WHERE PH.RN = 1;         
                   
     ELSE
        open p_recordset for
             SELECT *
                FROM order_phones_t op
             WHERE op.order_id = p_order_id
                   AND (op.date_from <p_date_to AND NVL(op.date_to,TO_DATE('01.01.2050','DD.MM.YYYY')) > p_date_from)
                   ; 

     END IF;               
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end; 

--================================================================================================================
-- Получение списка телефонов по заказу (с историзмом)
    PROCEDURE PHONES_LIST_BY_ORDER( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_order_id      IN NUMBER     -- номер заказа        
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'PHONES_LIST_BY_ORDER';
    v_retcode            INTEGER;
begin
        open p_recordset for
             SELECT *
                FROM order_phones_t op
             WHERE op.order_id = p_order_id  
             ORDER BY DATE_FROM DESC, DATE_TO DESC                 
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
-- Получение списка телефонов с адресами установок
    PROCEDURE PHONES_LIST_WITH_ADR_SET( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- номер лицевого счета
         p_order_id      IN NUMBER     -- номер лицевого счета         
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'PHONES_LIST_WITH_ADR_SET';
    v_retcode            INTEGER;
begin
    IF p_account_id IS NOT NULL THEN    
        open p_recordset for
            SELECT op.*, ph_adr.*
              FROM order_phones_t op, order_t ord, phone_address_t ph_adr 
             WHERE op.order_id = ord.order_id
                   AND ph_adr.address_id (+)= op.address_id
                   AND PH_ADR.ADDRESS_TYPE (+)= 'SET'
                   AND account_id = p_account_id
                   AND ((op.DATE_TO IS NULL AND op.DATE_FROM < SYSDATE) OR SYSDATE BETWEEN op.DATE_FROM AND op.DATE_TO)
                   ;
     ELSE
        open p_recordset for
             SELECT op.*, ph_adr.*
                FROM order_phones_t op, phone_address_t ph_adr 
               WHERE op.order_id = p_order_id
                     AND ph_adr.address_id (+)= op.address_id
                     AND PH_ADR.ADDRESS_TYPE (+)= 'SET'
                     AND ((op.DATE_TO IS NULL AND op.DATE_FROM < SYSDATE) OR SYSDATE BETWEEN op.DATE_FROM AND op.DATE_TO)
                     ; 

     END IF;               
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;  

--================================================================================================================
-- Получение списка телефонов (интервалами)
    PROCEDURE PHONES_RANGE( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER,    -- номер лицевого счета
         p_order_id      IN NUMBER    -- номер лицевого счета    
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'PHONES_RANGE';
    v_retcode            INTEGER;
begin
    IF p_account_id IS NOT NULL THEN    
       open p_recordset for
          SELECT PHONE_FROM, DECODE(PHONE_TO, PHONE_FROM, NULL, PHONE_TO) PHONE_TO
            FROM (
            SELECT MIN(REPLACE(PHONE_NUMBER,'KH_','')) PHONE_FROM, MAX(REPLACE(PHONE_NUMBER,'KH_','')) PHONE_TO 
              FROM ORDER_PHONES_T op
             WHERE 1=1 
               AND (SYSDATE BETWEEN DATE_FROM AND DATE_TO OR DATE_TO IS NULL)
               AND exists (
                   select * from order_t ord
                   where ord.order_id = op.order_id
                   and account_id = p_account_id                    
               )
            GROUP BY (REPLACE(PHONE_NUMBER,'KH_','') - ROWNUM + 1)
        ) ORDER BY PHONE_TO;   
     ELSE
       open p_recordset for
          SELECT PHONE_FROM, DECODE(PHONE_TO, PHONE_FROM, NULL, PHONE_TO) PHONE_TO
            FROM (
            SELECT MIN(REPLACE(PHONE_NUMBER,'KH_','')) PHONE_FROM, MAX(REPLACE(PHONE_NUMBER,'KH_','')) PHONE_TO 
              FROM ORDER_PHONES_T op
             WHERE 1=1 
               AND SYSDATE BETWEEN DATE_FROM AND DATE_TO
               AND ORDER_ID = p_order_id
            GROUP BY (REPLACE(PHONE_NUMBER,'KH_','') - ROWNUM + 1)
        ) ORDER BY PHONE_TO;   

     END IF;                       
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;

--================================================================================================================
-- Получение списка подписантов
    PROCEDURE SIGNER_LIST( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- номер лицевого счета 
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'SIGNER_LIST';
    v_retcode            INTEGER;
begin    
    open p_recordset for
         select *  from (
            SELECT 
                          S.SIGNER_ID,
                          S.CONTRACTOR_ID,
                          CASE WHEN S.CONTRACTOR_ID=1 THEN 'KTTK'
                            ELSE C.SHORT_NAME
                          END CONTRACTOR_NAME,
                          S.MANAGER_ID,
                          S.SIGNER_NAME,
                          S.ATTORNEY_NO,
                          S.SIGNER_ROLE_ID,
                          S.SIGNER_ROLE,
                          S.DATE_FROM,
                          S.DATE_TO,CASE 
                                   WHEN S.CONTRACTOR_ID = 1 THEN 9999999 
                                   ELSE S.PRIORITY 
                              END PRIORITY, 
                          M.SIGN_PICTURE_ID,
                          ROW_NUMBER() OVER(PARTITION BY SIGNER_ROLE_ID ORDER BY PRIORITY) rn
                        FROM
                           SIGNER_T S,
                           CONTRACTOR_T C,
                           MANAGER_T M,
                           PICTURE_T P,
                           ACCOUNT_PROFILE_T AP
                    WHERE
                        C.CONTRACTOR_ID = S.CONTRACTOR_ID
                        AND M.MANAGER_ID = S.MANAGER_ID
                        AND P.PICTURE_ID (+)= M.SIGN_PICTURE_ID  
                        AND (S.DATE_FROM <= SYSDATE AND (S.DATE_TO >= SYSDATE OR S.DATE_TO IS NULL))
                        AND S.CONTRACTOR_ID IN(AP.BRANCH_ID,1)
                        AND AP.ACCOUNT_ID = p_account_id                 
        )
        WHERE rn = 1 ;  
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;   

--================================================================================================================
-- Получение способов доставки для лицевого счета
    PROCEDURE DELIVERY_METHOD_BY_ACCOUNT( 
         p_result        OUT VARCHAR2, 
         p_recordset     OUT t_refc, 
         p_account_id    IN NUMBER    -- номер лицевого счета
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'DELIVERY_METHOD_BY_ACCOUNT';
    v_retcode            INTEGER;
begin
      open p_recordset for
          SELECT ad.account_id,
               ad.TYPE,
               ad.label,
               ad.DELIVERY_METHOD_ID,
               d.notes DELIVERY_METHOD_NAME
          FROM (SELECT 
                       account_id,
                       'DOC_BILL' TYPE,
                       'Комплект счетов' LABEL,
                       delivery_method_id
                  FROM account_documents_t
                 WHERE doc_bill = 'Y'
                UNION
                SELECT account_id,
                       'DOC_CALLS' TYPE,
                       'Позвонковка [' || doc_calls || ']' LABEL,
                       delivery_method_id
                  FROM account_documents_t
                 WHERE doc_calls IS NOT NULL) ad,
               dictionary_t d
         WHERE d.key_id = ad.delivery_method_id
               and ad.account_id = p_account_id;
exception
   WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end; 

--================================================================================================================
-- Получение баланса лицевого счета 
-- Складывается из текущего баланс минус текущие начисления
    PROCEDURE ACCOUNT_BALANCE_ONLINE( 
         p_balance_online     OUT NUMBER, 
         p_account_id         IN NUMBER    -- номер лицевого счета 
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_BALANCE_ONLINE';
    v_retcode            INTEGER;
    v_balance            NUMBER;
    v_total_summ         NUMBER;
    
    v_date_from          DATE;
    v_date_to            DATE;
    
begin    
      --Берем текущий баланс
      SELECT BALANCE INTO v_balance
             FROM ACCOUNT_T
          WHERE ACCOUNT_ID = p_account_id;

      -- Находим сумму текущий начислений
      v_date_from := TRUNC(SYSDATE,'MM');
      v_date_to := LAST_DAY (TRUNC(SYSDATE,'MM')) + INTERVAL '00 23:59:59' DAY TO SECOND;

       SELECT 
             NVL(SUM(AMOUNT),0) INTO v_total_summ
        FROM 
             BDR_VOICE_T e 
       WHERE 
           BDR_STATUS = 0
           AND e.rep_period BETWEEN v_date_from AND v_date_to
           AND e.account_id = p_account_id;      

      p_balance_online := v_balance - v_total_summ;                              
exception
   WHEN OTHERS THEN  
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;  

--================================================================================================================
-- Получение баланса лицевого счета по номеру телефона
-- Нужно для автоинформирования
    PROCEDURE ACCOUNT_BALANCE_BY_PHONENUMBER( 
         p_balance        OUT NUMBER, 
         p_phone_number   IN VARCHAR2    -- номер телефонного номера
    )
IS
    v_prcName            CONSTANT VARCHAR2(30) := 'ACCOUNT_BALANCE_BY_PHONENUMBER';
    v_retcode            INTEGER;
begin    
      SELECT A.BALANCE INTO p_balance
          FROM account_t a, order_t o, order_phones_t op
         WHERE a.account_id = o.account_id
               AND o.order_id = op.order_id
               AND op.phone_number = p_phone_number
               AND (
                   (SYSDATE BETWEEN op.DATE_FROM AND op.DATE_TO)
                    OR (op.DATE_FROM <= SYSDATE AND op.DATE_TO IS NULL))
               AND ROWNUM=1;                          
exception   
   WHEN OTHERS THEN  
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;    

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Проверка номера телефона на пересечение
-- Возвращает кол-во пересечений
PROCEDURE Check_phonenumber_cross (
        p_count         OUT INTEGER,
        p_phonenumber   IN VARCHAR2,
        p_date_from     IN DATE DEFAULT SYSDATE
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_phonenumber_cross';
    v_cnt        INTEGER;
BEGIN
    SELECT COUNT (*) INTO v_cnt
      FROM order_phones_t
       WHERE 
              phone_number = p_phonenumber
              AND p_date_from BETWEEN DATE_FROM AND DATE_TO;
    p_count:= v_cnt;
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_phonenumber='||p_phonenumber||                                    
                                    c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Получить список лицевых счетов, на котором висит заданный контрагент
-- за исключением текущего лицевого счета
PROCEDURE GET_ACCOUNT_LIST_BY_CUSTOMER (
        p_recordset           OUT t_refc, 
        p_customer_id         IN NUMBER,
        p_account_id_exclude  IN NUMBER
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'GET_ACCOUNT_LIST_BY_CUSTOMER';
    v_cnt        INTEGER;
BEGIN
    open p_recordset for
       SELECT C.contract_no,
                 a.account_no,
                 ap.date_from,
                 ap.date_to,
                 CUS.ERP_CODE,
                 cus.customer
            FROM account_profile_t ap,
                 contract_t c,
                 customer_t cus,
                 account_t a
           WHERE     AP.CONTRACT_ID = c.contract_id
                 AND cus.customer_id = ap.customer_id
                 AND a.account_id = ap.account_id
                 AND cus.customer_id = p_customer_id
                 AND (A.ACCOUNT_ID <> p_account_id_exclude OR p_account_id_exclude IS NULL)
        ORDER BY ACCOUNT_NO;
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_customer_id='||p_customer_id||                                    
                                    c_PkgName||'.'||v_prcName );
END;

END PK51_W_ACCOUNT_EXT;
/
