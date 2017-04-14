CREATE OR REPLACE PACKAGE PK203_SPB_OPERATOR_REPORT
IS
    --
    -- Пакет для создания клиентов бренда xTTK «Санкт-Петербургский ТЕЛЕПОРТ»
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK203_SPB_OPERATOR_REPORT';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    -- ID КТТК
    c_CONTRACTOR_KTTK_ID         constant integer := 1;
    -- Поставщик для "СПБ ТЕЛЕПОРТ" 
    c_CONTRACTOR_SPB_TELEPORT_ID constant integer := 19;
    -- Бренд для "СПБ ТЕЛЕПОРТ"
    c_SPB_TTK_Root               constant integer := 21;
    
    -- Услуги присоединения и пропуска трафика на местном и/или зоновом уровне
    c_SERVICE_OPLOCL CONSTANT INTEGER := 7;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1. выгрузить в excel все данные по договорам (номер, название, л/с, адреса, эл.почта, доставка, манагер, типы, сегменты...)
    --
    PROCEDURE Export_contracts( 
                   p_recordset    OUT t_refc
               );    

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2. выгрузить в excel все данные по заказам (номер и название договора, номер заказа, услуга, компонента, тариф, валюта...)
    --    причем, если получится, то вывести не только абон. платы, но и тарифы по межоператорке (без номерной емкости). 
    --
    PROCEDURE Export_orders( 
                   p_recordset    OUT t_refc
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3. отдельным списком вывести перечень минималок и скидок (хотя этих вроде не было).
    PROCEDURE Export_fixrates( 
                   p_recordset    OUT t_refc
               );

    -- ========================================================================= --
    --                    С В О Д Н Ы Е   О Т Ч Е Т Ы
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 4. Д О Х О Д
    PROCEDURE Export_revenue( 
                   p_recordset    OUT t_refc,
                   p_period_id    IN INTEGER
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 5. Р А С Х О Д
    PROCEDURE Export_debt( 
                   p_recordset    OUT t_refc,
                   p_period_id    IN INTEGER
               );



END PK203_SPB_OPERATOR_REPORT;
/
CREATE OR REPLACE PACKAGE BODY PK203_SPB_OPERATOR_REPORT
IS
-- ========================================================================= --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Отчет о проведенной миграции
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- ========================================================================= --
-- 1. выгрузить в excel все данные по договорам (номер, название, л/с, адреса, эл.почта, доставка, манагер, типы, сегменты...)
--
PROCEDURE Export_contracts( 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_contracts';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT '`'||C.CONTRACT_NO CONTRACT_NO, TO_CHAR(C.DATE_FROM,'dd.mm.yyyy') DATE_FROM, 
               A.ACCOUNT_NO, DM.NAME MARKET_SEGMENT, DC.NAME CLIENT_TYPE,
               CS.CUSTOMER, CS.INN, CS.KPP,
               --SC.MANAGER_ID, BC.MANAGER_ID, 
               MSC.LAST_NAME||' '||MSC.FIRST_NAME||' '||MSC.MIDDLE_NAME SALE_CURATOR, 
               MBC.LAST_NAME||' '||MBC.FIRST_NAME||' '||MBC.MIDDLE_NAME BILL_CURATOR,
               NVL(AJ.EMAIL,NVL(AD.EMAIL,AG.EMAIL)) EMAIL,
               AJ.ZIP||', '||AJ.STATE||', '||AJ.CITY||','||AJ.ADDRESS ADDR_JUR,
               AD.ZIP||', '||AD.STATE||', '||AD.CITY||','||AD.ADDRESS ADDR_DLV,
               AG.ZIP||', '||AG.STATE||', '||AG.CITY||','||AG.ADDRESS ADDR_GRP
          FROM ACCOUNT_T A, 
               ACCOUNT_PROFILE_T AP, 
               CONTRACT_T C, 
               SALE_CURATOR_T SC, MANAGER_T MSC, 
               BILLING_CURATOR_T BC, MANAGER_T MBC,
               CUSTOMER_T CS, DICTIONARY_T DM, DICTIONARY_T DC,
               ACCOUNT_CONTACT_T AJ, -- ADDR_JUR,
               ACCOUNT_CONTACT_T AD, -- ADDR_DLV,
               ACCOUNT_CONTACT_T AG  -- ADDR_GRP
         WHERE A.BILLING_ID = 2006
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AP.DATE_TO IS NULL
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND C.CONTRACT_ID  = SC.CONTRACT_ID(+)
           AND C.CONTRACT_ID  = BC.CONTRACT_ID(+)
           AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
           AND C.MARKET_SEGMENT_ID = DM.KEY_ID(+)
           AND C.CLIENT_TYPE_ID    = DC.KEY_ID(+)
           AND MSC.MANAGER_ID(+) = SC.MANAGER_ID
           AND MBC.MANAGER_ID(+) = BC.MANAGER_ID
           AND AJ.ACCOUNT_ID(+)  = A.ACCOUNT_ID
           AND AJ.ADDRESS_TYPE(+)= 'JUR'
           AND AD.ACCOUNT_ID(+)  = A.ACCOUNT_ID
           AND AD.ADDRESS_TYPE(+)= 'DLV'
           AND AG.ACCOUNT_ID(+)  = A.ACCOUNT_ID
           AND AG.ADDRESS_TYPE(+)= 'GRP'
        ORDER BY C.CONTRACT_NO, A.ACCOUNT_NO
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 2. выгрузить в excel все данные по заказам (номер и название договора, номер заказа, услуга, компонента, тариф, валюта...)
--    причем, если получится, то вывести не только абон. платы, но и тарифы по межоператорке (без номерной емкости). 
--
PROCEDURE Export_orders( 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_orders';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT '`'||C.CONTRACT_NO CONTRACT_NO, 
               A.ACCOUNT_NO, O.ORDER_NO, 
               O.DATE_FROM, 
               TO_CHAR(O.DATE_FROM,'dd.mm.yyyy') ORDER_DATE_FROM,
               TO_CHAR(O.DATE_TO,  'dd.mm.yyyy') ORDER_DATE_TO,
               S.SERVICE_SHORT, SA.SRV_NAME SERVICE_ALIAS,
               OB.SUBSERVICE_ID, 
               TO_CHAR(OB.DATE_FROM,'dd.mm.yyyy') SSRV_DATE_FROM, 
               TO_CHAR(OB.DATE_TO,'dd.mm.yyyy') SSRV_DATE_TO, 
               OB.CHARGE_TYPE, OB.RATE_VALUE, OB.QUANTITY, OB.TAX_INCL, OB.CURRENCY_ID 
          FROM ACCOUNT_T A, 
               ACCOUNT_PROFILE_T AP, 
               CONTRACT_T C,
               ORDER_T O,
               SERVICE_T S,
               SERVICE_ALIAS_T SA,
               ORDER_BODY_T OB, 
               SUBSERVICE_T SS
         WHERE A.BILLING_ID = 2006
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AP.DATE_TO IS NULL
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND A.ACCOUNT_ID   = O.ACCOUNT_ID
           AND O.SERVICE_ID   = S.SERVICE_ID
           AND O.ORDER_ID     = OB.ORDER_ID
           AND OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
           AND OB.CHARGE_TYPE IN ('MIN', 'REC')
           AND SA.ACCOUNT_ID(+) = O.ACCOUNT_ID
           AND SA.SERVICE_ID(+) = O.SERVICE_ID
        ORDER BY C.CONTRACT_NO, A.ACCOUNT_NO, O.ORDER_NO, OB.CHARGE_TYPE, OB.DATE_FROM
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 3. отдельным списком вывести перечень минималок и скидок (хотя этих вроде не было).
PROCEDURE Export_fixrates( 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_fixrates';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT C.CONTRACT_NO, A.ACCOUNT_NO, O.ORDER_NO, O.SERVICE_ID, O.DATE_FROM, O.DATE_TO, S.SERVICE_SHORT,
               OB.SUBSERVICE_ID, OB.DATE_FROM, OB.DATE_TO, SS.SUBSERVICE, 'RUR' CURRENCY_ID 
          FROM ACCOUNT_T A, 
               ACCOUNT_PROFILE_T AP, 
               CONTRACT_T C,
               ORDER_T O,
               SERVICE_T S,
               ORDER_BODY_T OB, 
               SUBSERVICE_T SS
         WHERE A.BILLING_ID = 2006
           AND A.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AP.DATE_TO IS NULL
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND A.ACCOUNT_ID   = O.ACCOUNT_ID
           AND O.SERVICE_ID   = S.SERVICE_ID
           AND O.ORDER_ID     = OB.ORDER_ID
           AND OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 4. Д О Х О Д
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Export_revenue( 
               p_recordset    OUT t_refc,
               p_period_id    IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_revenue';
    v_retcode    INTEGER;
    v_date_from  DATE;
    v_date_to    DATE;
BEGIN
    v_date_from  := Pk04_Period.Period_from(p_period_id);
    v_date_to    := Pk04_Period.Period_to(p_period_id);
    -- возвращаем курсор
    OPEN p_recordset FOR
        -- --------------------------------------------------------------- --
        WITH BDR AS (
            SELECT --+ parallel(b 10)
                  TRUNC(b.local_time,'mm') debt_month,
                  account_id, item_id, 
                   (CASE trf_type  WHEN 1 THEN 'Завершение'
                                   WHEN 2 THEN 'Инициирование'
                                   WHEN 5 THEN 'Инициирование на платформу'
                    END) srv_name,
                    COUNT(1) calls,
                    ROUND(SUM(bill_minutes),2) bill_minutes,
                    ROUND(SUM(duration),2) cdr_seconds,
                    ROUND(SUM(amount),2) amount,
                    MIN(local_time) first_call,
                    MAX(local_time) last_call
              FROM bdr_oper_t b
             WHERE rep_period BETWEEN v_date_from AND v_date_to
               AND bdr_status = 0
               AND bdr_type_id = 3
               AND trf_type IN (1,2,5)
             GROUP BY TRUNC(b.local_time,'mm'), account_id, item_id, trf_type
        ), IT AS (
            SELECT '`'||C.CONTRACT_NO CONTRACT_NO, CS.CUSTOMER,
                   B.BILL_NO, B.BILL_TYPE, TO_DATE(B.BILL_DATE,'dd.mm.yyyy') BILL_DATE,
                   S.SERVICE, SS.SUBSERVICE, I.CHARGE_TYPE,
                   I.ITEM_ID, SUM(I.ITEM_TOTAL) AMOUNT
              FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP,
                   BILL_T B, ITEM_T I, 
                   CONTRACT_T C, CUSTOMER_T CS, 
                   SERVICE_T S, SUBSERVICE_T SS
             WHERE B.REP_PERIOD_ID = p_period_id -- 201505
               AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
               AND B.BILL_ID       = I.BILL_ID
               AND B.ACCOUNT_ID    = A.ACCOUNT_ID
               AND A.STATUS        = Pk00_Const.c_ACC_STATUS_BILL -- 'B'
               AND A.BILLING_ID    = Pk00_Const.c_BILLING_SPB -- 2006
               AND B.CONTRACT_ID   = C.CONTRACT_ID
               AND I.SERVICE_ID    = S.SERVICE_ID
               AND I.SUBSERVICE_ID = SS.SUBSERVICE_ID 
               AND B.PROFILE_ID    = AP.PROFILE_ID
               AND CS.CUSTOMER_ID  = AP.CUSTOMER_ID
               --AND EXISTS (
               --    SELECT * FROM ORDER_T O
               --     WHERE O.ACCOUNT_ID = A.ACCOUNT_ID
               --       AND O.SERVICE_ID = Pk00_Const.c_SERVICE_OP_LOCAL -- 7
               --)
            GROUP BY C.CONTRACT_NO, CS.CUSTOMER, -- CL.CLIENT_NAME, 
                     B.BILL_NO, B.BILL_TYPE, TO_DATE(B.BILL_DATE,'dd.mm.yyyy'),
                     S.SERVICE, SS.SUBSERVICE, I.CHARGE_TYPE, I.ITEM_ID
        )
        SELECT  IT.CONTRACT_NO, IT.CUSTOMER, 
                IT.BILL_NO, IT.BILL_TYPE, IT.BILL_DATE,
                IT.SERVICE, IT.SUBSERVICE, 
                IT.CHARGE_TYPE,
                bdr.calls,
                bdr.bill_minutes,
                bdr.cdr_seconds,
                IT.AMOUNT,
                bdr.first_call,
                bdr.last_call
          FROM BDR, IT
         WHERE IT.ITEM_ID = BDR.ITEM_ID(+)
        ORDER BY IT.CONTRACT_NO, IT.CUSTOMER, BDR.DEBT_MONTH, IT.BILL_NO, IT.SERVICE
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 5. Р А С Х О Д
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Export_debt( 
               p_recordset    OUT t_refc,
               p_period_id    IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Export_debt';
    v_retcode    INTEGER;
    v_date_from  DATE;
    v_date_to    DATE;
BEGIN
    v_date_from  := Pk04_Period.Period_from(p_period_id);
    v_date_to    := Pk04_Period.Period_to(p_period_id);
    -- возвращаем курсор
    OPEN p_recordset FOR
        WITH bdr AS ( 
            SELECT --+ parallel(b 10) 
                  account_id, --bill_id, order_id, -- bill_id для расхода может не считаться, т.к. расход в счета не кладется 
                  trf_type, service_id, subservice_id, parent_subsrv_id, 
                   (CASE trf_type  WHEN 3 THEN 'Завершение' 
                                   WHEN 4 THEN 'Инициирование' 
                                   WHEN 6 THEN 'Инициирование на платформу' 
                    END) srv_name, 
                    COUNT(1) calls, 
                    SUM(bill_minutes) bill_minutes, 
                    SUM(duration) cdr_seconds, 
                    SUM(amount) amount, 
                    MIN(local_time) first_call, 
                    MAX(local_time) last_call 
              FROM bdr_oper_t b 
             WHERE rep_period BETWEEN v_date_from AND v_date_to 
               AND bdr_status = 0 
               AND bdr_type_id = 3 
               AND trf_type IN (3,4,6) 
             GROUP BY account_id, --bill_id, order_id, 
                      trf_type, service_id, subservice_id, parent_subsrv_id 
        ) 
        SELECT  DISTINCT 
                '`'||c.CONTRACT_NO CONTRACT_NO, CS.CUSTOMER, S.SERVICE, 
                NVL(SS.SRV_NAME, SR.SUBSERVICE) SUBSERVICE, 
                bdr.calls, 
                bdr.bill_minutes, 
                bdr.cdr_seconds, 
                bdr.amount, 
                bdr.first_call, 
                bdr.last_call 
          FROM bdr, X07_SRV_DCT ss, SERVICE_T S, SUBSERVICE_T SR, --BILL_T B, 
               ACCOUNT_PROFILE_T p, CONTRACT_T C, CUSTOMER_T CS 
         WHERE BDR.SERVICE_ID  = S.SERVICE_ID 
           AND BDR.SUBSERVICE_ID = ss.SRV_ID(+)
           AND BDR.PARENT_SUBSRV_ID = SR.SUBSERVICE_ID 
           AND BDR.ACCOUNT_ID = P.ACCOUNT_ID
           AND P.CONTRACT_ID  = C.CONTRACT_ID
           AND P.CUSTOMER_ID  = CS.CUSTOMER_ID
          -- AND BDR.BILL_ID     = B.BILL_ID
          -- AND B.REP_PERIOD_ID = p_period_id 
          -- AND B.ACCOUNT_ID    = bdr.ACCOUNT_ID 
          -- AND B.CONTRACT_ID   = C.CONTRACT_ID 
          -- AND B.BILL_TYPE     = Pk00_Const.c_BILL_TYPE_REC
        ORDER BY CONTRACT_NO, CS.CUSTOMER, S.SERVICE
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;



END PK203_SPB_OPERATOR_REPORT;
/
