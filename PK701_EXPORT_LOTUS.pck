CREATE OR REPLACE PACKAGE PK701_EXPORT_LOTUS
IS
    --
    -- Пакет для ручных работ сотрудников ДРУ
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK701_EXPORT_LOTUS';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    --
    -- Процедура экспорта данных о клиентах
    PROCEDURE Export_clients ( 
             p_recordset OUT t_refc,
             p_branch_id IN INTEGER
          );
    
    -- --------------------------------------------------------------------------------- --
    -- Процедура экспорта данных о клиентах (РП - тест)
    -- --------------------------------------------------------------------------------- --
    --
    PROCEDURE Export_clients_RP_test ( 
             p_recordset OUT t_refc,
             p_branch_id IN INTEGER
          );  
    
    
END PK701_EXPORT_LOTUS;
/
CREATE OR REPLACE PACKAGE BODY PK701_EXPORT_LOTUS
IS

-- ========================================================================= --
--  Экспорт данных в ЛОТУС
-- ========================================================================= --
--
-- Процедура экспорта данных о клиентах
--
PROCEDURE Export_clients ( 
             p_recordset OUT t_refc,
             p_branch_id IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_clients';
    v_retcode    INTEGER;
BEGIN
    -- построить курсор
    OPEN p_recordset FOR
    SELECT 
        CT.CONTRACTOR BRANCH,       --  филиал 
        CS.ERP_CODE ERP_CODE,       -- код клиента в 1С
        CS.CUSTOMER CLIENT,         -- имя клиета
        CS.INN,                     -- ИНН клиента
        CS.KPP,                     -- КПП клиента
        C.CONTRACT_NO CONTRACT_NO,  -- № договора
        C.DATE_FROM CONTRACT_DATE,  -- дата договора
        A.ACCOUNT_ID,               -- id лицевого счета в биллинге (для разборок) 
        A.ACCOUNT_NO,               -- № лицевого счета в биллинге
        O.ORDER_ID,                 -- id заказа в биллинге (для разборок)
        O.ORDER_NO,                 -- № заказа в биллинге
        O.DATE_FROM ORDER_DATE_FROM,-- дата начала действия заказа
        O.DATE_TO   ORDER_DATE_TO,  -- дата окончания действия заказа (заказ прекращает действие в 23:59:59)
        S.SERVICE_ID,               -- id услуги в справочнике биллинга (для разборок)
        S.SERVICE,                  -- услуга из справочника в биллинге
        LS.SERVICE_OM,              -- услуга из справочника в ОМ ЛОТУС
        SA.SRV_NAME SERVICE_ALIAS,  -- услуга, как она будет напечатана в счете
        OI.POINT_SRC,               -- исходная точка
        OI.POINT_DST,               -- конечная точка
        OI.SPEED_STR,               -- скорость канала
        OB.SUBSERVICE_ID,           -- id компоненты услугииз справочника в биллинге (для разборок)
        SS.SUBSERVICE,              -- компонента услуги из справочника в биллинге
        OB.CHARGE_TYPE,             -- тип начислений: REC(абонка)/USG(трафик)/MIN(минималка) (для разборок)
        OB.RATE_VALUE,              -- абонплата / доплата до минимальной стоимости, в зависимости от CHARGE_TYPE 
        OB.CURRENCY_ID,             -- id валюты в биллинге, как в Portal6.5
        D.NAME RATE_RULE,           -- правило проведения начислений
        OB.DATE_FROM OB_DATE_FROM,  -- дата начала действия компоненты услуги
        OB.DATE_TO OB_DATE_TO,      -- дата окончания действия компоненты услуги
        OB.RATEPLAN_ID RATEPLAN_ID, -- id тарифного плана в биллинге (для разборок)
        P.RATEPLAN_NAME,            -- имя тарифного плана  в биллинге
        DECODE(SCA.MANAGER_ID, NULL, MC.LAST_NAME||' '||MC.FIRST_NAME||MC.MIDDLE_NAME, 
                                     MA.LAST_NAME||' '||MA.FIRST_NAME||MA.MIDDLE_NAME) MANAGER -- продавец
      FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, ORDER_T O, ORDER_INFO_T OI, RATEPLAN_T P,  
           ORDER_BODY_T OB, SUBSERVICE_T SS, DICTIONARY_T D, 
           SERVICE_T S, EXT01_LOTUS_SERVICES LS, SERVICE_ALIAS_T SA,
           CONTRACT_T C, CONTRACTOR_T CT, CUSTOMER_T CS, SALE_CURATOR_T SCA, SALE_CURATOR_T SCC,
           MANAGER_T MC, MANAGER_T MA  
     WHERE A.BILLING_ID     = 2006  -- боевой биллинг РП регионы
       AND A.ACCOUNT_ID     = AP.ACCOUNT_ID
       AND A.ACCOUNT_ID     = O.ACCOUNT_ID
       AND O.SERVICE_ID     = S.SERVICE_ID
       AND S.SERVICE_ID     = LS.SERVICE_ID(+)
       --AND LS.SERVICE_OM NOT IN ('ZVI','Агентский Free Phone','Агентский МГ/МН (с Условной стоимостью)')
       AND OI.ORDER_ID      = O.ORDER_ID
       AND O.ORDER_ID       = OB.ORDER_ID
       AND OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
       AND OB.RATE_RULE_ID  = D.KEY_ID(+)
       AND OB.RATEPLAN_ID   = P.RATEPLAN_ID(+)
       AND AP.CONTRACT_ID   = C.CONTRACT_ID
       AND AP.ACTUAL        = 'Y'
       AND AP.BRANCH_ID     = CT.CONTRACTOR_ID
       --AND CT.CONTRACTOR  != 'Филиал «Макрорегион Север» (РП Блока Доступ)'
       --AND CT.CONTRACTOR   = 'Филиал «Макрорегион Кавказ» (РП)'
       --AND CT.CONTRACTOR   = 'Филиал «Макрорегион Кавказ» (РП, тест)'
       --AND CT.CONTRACTOR   LIKE '%Кавказ%'
       AND CT.CONTRACTOR_ID = p_branch_id
       AND AP.CUSTOMER_ID   = CS.CUSTOMER_ID
       AND A.ACCOUNT_ID     = SCA.ACCOUNT_ID(+)
       AND C.CONTRACT_ID    = SCC.CONTRACT_ID(+)
       AND MC.MANAGER_ID(+) =  SCC.MANAGER_ID
       AND MA.MANAGER_ID(+) =  SCA.MANAGER_ID
       AND SA.SERVICE_ID(+) = O.SERVICE_ID
       AND SA.ACCOUNT_ID(+) = O.ACCOUNT_ID
       ORDER BY CT.CONTRACTOR,       --  филиал 
                CS.CUSTOMER,         -- имя клиета
                A.ACCOUNT_NO,
                O.ORDER_NO,
                S.SERVICE,
                SS.SUBSERVICE
       ;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- --------------------------------------------------------------------------------- --
-- Процедура экспорта данных о клиентах (РП - тест)
-- --------------------------------------------------------------------------------- --
--
PROCEDURE Export_clients_RP_test ( 
             p_recordset OUT t_refc,
             p_branch_id IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_clients_RP_test';
    v_retcode    INTEGER;
BEGIN
    -- построить курсор
    OPEN p_recordset FOR
    WITH ADR AS (
        SELECT A.ACCOUNT_ID, 
               AD.COUNTRY||', '||AD.ZIP||', '||AD.STATE||', '||AD.CITY||', '||AD.ADDRESS||', Email: '||AD.EMAIL DLV_ADDERESS, 
               AJ.COUNTRY||', '||AJ.ZIP||', '||AJ.STATE||', '||AJ.CITY||', '||AJ.ADDRESS JUR_ADDERESS
          FROM ACCOUNT_T A,
               ACCOUNT_CONTACT_T AD, 
               ACCOUNT_CONTACT_T AJ 
         WHERE A.ACCOUNT_ID IN (
            SELECT AP.ACCOUNT_ID 
              FROM ACCOUNT_PROFILE_T AP
             WHERE AP.BRANCH_ID = p_branch_id
         )
         AND AD.ADDRESS_TYPE(+) = 'DLV'
         AND AJ.ADDRESS_TYPE(+) = 'JUR'
         AND A.ACCOUNT_ID = AD.ACCOUNT_ID(+)
         AND A.ACCOUNT_ID = AJ.ACCOUNT_ID(+)
         AND AD.DATE_TO IS NULL
         AND AJ.DATE_TO IS NULL
    )
    SELECT 
        CT.CONTRACTOR BRANCH,       --  филиал 
        CS.ERP_CODE ERP_CODE,       -- код клиента в 1С
        CS.CUSTOMER CLIENT,         -- имя клиета
        CS.INN,                     -- ИНН клиента
        CS.KPP,                     -- КПП клиента
        C.CONTRACT_NO CONTRACT_NO,  -- № договора
        C.DATE_FROM CONTRACT_DATE,  -- дата договора
        A.ACCOUNT_ID,               -- id лицевого счета в биллинге (для разборок) 
        A.ACCOUNT_NO,               -- № лицевого счета в биллинге
        O.ORDER_ID,                 -- id заказа в биллинге (для разборок)
        O.ORDER_NO,                 -- № заказа в биллинге
        O.DATE_FROM ORDER_DATE_FROM,-- дата начала действия заказа
        O.DATE_TO   ORDER_DATE_TO,  -- дата окончания действия заказа (заказ прекращает действие в 23:59:59)
        S.SERVICE_ID,               -- id услуги в справочнике биллинга (для разборок)
        S.SERVICE,                  -- услуга из справочника в биллинге
        LS.SERVICE_OM,              -- услуга из справочника в ОМ ЛОТУС
        SA.SRV_NAME SERVICE_ALIAS,  -- услуга, как она будет напечатана в счете
        OI.POINT_SRC,               -- исходная точка
        OI.POINT_DST,               -- конечная точка
        OI.SPEED_STR,               -- скорость канала
        OB.SUBSERVICE_ID,           -- id компоненты услугииз справочника в биллинге (для разборок)
        SS.SUBSERVICE,              -- компонента услуги из справочника в биллинге
        OB.CHARGE_TYPE,             -- тип начислений: REC(абонка)/USG(трафик)/MIN(минималка) (для разборок)
        OB.RATE_VALUE,              -- абонплата / доплата до минимальной стоимости, в зависимости от CHARGE_TYPE 
        OB.CURRENCY_ID,             -- id валюты в биллинге, как в Portal6.5
        D.NAME RATE_RULE,           -- правило проведения начислений
        OB.DATE_FROM OB_DATE_FROM,  -- дата начала действия компоненты услуги
        OB.DATE_TO OB_DATE_TO,      -- дата окончания действия компоненты услуги
        OB.RATEPLAN_ID RATEPLAN_ID, -- id тарифного плана в биллинге (для разборок)
        P.RATEPLAN_NAME,            -- имя тарифного плана  в биллинге
        DECODE(SCA.MANAGER_ID, NULL, MC.LAST_NAME||' '||MC.FIRST_NAME||MC.MIDDLE_NAME, 
                                     MA.LAST_NAME||' '||MA.FIRST_NAME||MA.MIDDLE_NAME) MANAGER, -- продавец
        ADR.JUR_ADDERESS,
        ADR.DLV_ADDERESS
      FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, 
           ORDER_T O, ORDER_INFO_T OI, RATEPLAN_T P,  
           ORDER_BODY_T OB, SUBSERVICE_T SS, DICTIONARY_T D, 
           SERVICE_T S, EXT01_LOTUS_SERVICES LS, SERVICE_ALIAS_T SA,
           CONTRACT_T C, CONTRACTOR_T CT, CUSTOMER_T CS, 
           SALE_CURATOR_T SCA, SALE_CURATOR_T SCC,
           MANAGER_T MC, MANAGER_T MA, ADR
     WHERE A.BILLING_ID     = 2008  -- тестовый биллинг РП регионы
       AND A.ACCOUNT_ID     = AP.ACCOUNT_ID
       AND A.ACCOUNT_ID     = O.ACCOUNT_ID
       AND O.SERVICE_ID     = S.SERVICE_ID
       AND S.SERVICE_ID     = LS.SERVICE_ID(+)
       --AND LS.SERVICE_OM NOT IN ('ZVI','Агентский Free Phone','Агентский МГ/МН (с Условной стоимостью)')
       AND OI.ORDER_ID      = O.ORDER_ID
       AND O.ORDER_ID       = OB.ORDER_ID
       AND OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
       AND OB.RATE_RULE_ID  = D.KEY_ID(+)
       AND OB.RATEPLAN_ID   = P.RATEPLAN_ID(+)
       AND AP.CONTRACT_ID   = C.CONTRACT_ID
       AND AP.ACTUAL        = 'Y'
       AND AP.BRANCH_ID     = CT.CONTRACTOR_ID
       --AND CT.CONTRACTOR  != 'Филиал «Макрорегион Север» (РП Блока Доступ)'
       --AND CT.CONTRACTOR   = 'Филиал «Макрорегион Кавказ» (РП)'
       --AND CT.CONTRACTOR   = 'Филиал «Макрорегион Кавказ» (РП, тест)'
       --AND CT.CONTRACTOR   LIKE '%Кавказ%'
       AND CT.CONTRACTOR_ID = p_branch_id
       AND AP.CUSTOMER_ID   = CS.CUSTOMER_ID
       AND A.ACCOUNT_ID     = SCA.ACCOUNT_ID(+)
       AND C.CONTRACT_ID    = SCC.CONTRACT_ID(+)
       AND MC.MANAGER_ID(+) =  SCC.MANAGER_ID
       AND MA.MANAGER_ID(+) =  SCA.MANAGER_ID
       AND SA.SERVICE_ID(+) = O.SERVICE_ID
       AND SA.ACCOUNT_ID(+) = O.ACCOUNT_ID
       AND A.ACCOUNT_ID     = ADR.ACCOUNT_ID(+)
       ORDER BY CT.CONTRACTOR,       --  филиал 
                CS.CUSTOMER,         -- имя клиета
                A.ACCOUNT_NO,
                O.ORDER_NO,
                S.SERVICE,
                SS.SUBSERVICE
       ;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;



END PK701_EXPORT_LOTUS;
/
