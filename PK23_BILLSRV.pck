CREATE OR REPLACE PACKAGE PK23_BILLSRV
IS
    --
    -- Пакет для работы с Billing Server В.Малиновского
    -- для межоператорских расчетов
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK23_BILLSRV';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- ВНИМАНИЕ!!! 
    -- Все заказы которые обслуживаются в BillingServer метятся тарифным планом 6
    c_RATEPLAN_BILSRV_ID  CONSTANT INTEGER := 6;  -- 'тариф BillingServer'

    -- статусы обработки данных
    c_STAT_BIND_OK            CONSTANT integer :=  1;
    c_STAT_ADD_INFO_OK        CONSTANT integer :=  2;
    c_STAT_MK_BILL_OK         CONSTANT integer :=  3;
    c_STAT_MK_ITEM_OK         CONSTANT integer :=  4;

    --DIRECTION  = 0 / 1 (терминация/оригинация)
    --International = 0/1/10 ( 0-нумерация РФ, 1- нумерация не РФ, 10 - то же, что и 0, но для услуги ВЗ связи заказы ZV)

    -- --------------------------------------------------------------------- --
    --  Процедура полного цикла для обработки данных Billing Server-а
    -- --------------------------------------------------------------------- --
    PROCEDURE Processing(
                   p_period_id IN INTEGER
               ); 

    -- --------------------------------------------------------------------- --
    --  корректировка данных 2009 биллинга
    -- --------------------------------------------------------------------- --
    PROCEDURE Correct_Billing;

    -- --------------------------------------------------------------------- --
    --  Загрузка детализаций из Billing Server в BRM
    -- --------------------------------------------------------------------- --
    PROCEDURE Load_Details(
                   p_period_id IN INTEGER
               );
               
    -- --------------------------------------------------------------------- --
    --  Привязка детализаций к заказам
    -- --------------------------------------------------------------------- --
    FUNCTION Bind_Details(
                   p_period_id IN INTEGER
               ) RETURN INTEGER;

    -- ------------------------------------------------------------------------ --
    -- проставляем компоненты для местного (125) и зонового трафика (140, 167)
    -- ------------------------------------------------------------------------ --
    PROCEDURE Set_LZ_OrdeBody(
                   p_task_id IN INTEGER
               );

    -- ------------------------------------------------------------------------ --
    -- проставляем компоненты для мг/мн (1,2)
    -- ------------------------------------------------------------------------ --
    PROCEDURE Set_MgMn_OrdeBody(
                   p_task_id IN INTEGER
               );

    -- ------------------------------------------------------------------------ --
    -- проставляем компоненты для услуг местного присоединения (142)
    -- ------------------------------------------------------------------------ --
    PROCEDURE Set_OperL_OrdeBody(
                   p_task_id IN INTEGER
               );

    -- --------------------------------------------------------------------- --
    --  Добавить дополнительную информацию в детализацию
    -- --------------------------------------------------------------------- --
    PROCEDURE Add_info_to_Details(
                   p_task_id IN INTEGER
               );

    -- --------------------------------------------------------------------- --
    --  Добавить счета
    -- --------------------------------------------------------------------- --
    PROCEDURE Add_Bills(
                   p_task_id IN INTEGER
               );

    -- --------------------------------------------------------------------- --
    --  Добавить начисленния в item-ы
    -- --------------------------------------------------------------------- --
    PROCEDURE Add_Items(
                   p_task_id IN INTEGER
               );

    -- --------------------------------------------------------------------- --
    --  Удалить начисленния из item-ов
    -- --------------------------------------------------------------------- --
    PROCEDURE Del_Items(
                   p_task_id IN INTEGER
               );

    -- --------------------------------------------------------------------- --
    --  Проставить статус загрузки в Billing Server В.Малиновского
    -- --------------------------------------------------------------------- --
    PROCEDURE Callback(
                   p_task_id IN INTEGER
               );
               
    -- --------------------------------------------------------------------- --
    --  Выгрузка отчета в Billing Server В.Малиновского
    -- --------------------------------------------------------------------- --
    PROCEDURE Report(
                   p_period_id IN INTEGER
               );
    
END PK23_BILLSRV;
/
CREATE OR REPLACE PACKAGE BODY PK23_BILLSRV
IS

-- --------------------------------------------------------------------- --
--  Процедура полного цикла для обработки данных Billing Server-а
-- --------------------------------------------------------------------- --
PROCEDURE Processing(
               p_period_id IN INTEGER
           ) 
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Processing';
    v_task_id  INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- --------------------------------------------------------------------- --
    --  корректировка данных 2009 биллинга
    -- --------------------------------------------------------------------- --
    Correct_Billing;

    -- --------------------------------------------------------------------- --
    --  Загрузка детализаций из Billing Server в BRM
    -- --------------------------------------------------------------------- --
    Load_Details( p_period_id );
               
    -- --------------------------------------------------------------------- --
    --  Привязка детализаций к заказам
    -- --------------------------------------------------------------------- --
    v_task_id := Bind_Details( p_period_id );
    
    -- --------------------------------------------------------------------- --
    --  Добавить дополнительную информацию в детализацию
    -- --------------------------------------------------------------------- --
    Add_info_to_Details( v_task_id );

    -- --------------------------------------------------------------------- --
    --  Добавить счета
    -- --------------------------------------------------------------------- --
    Add_Bills( v_task_id );

    -- --------------------------------------------------------------------- --
    --  Добавить начисленния в item-ы
    -- --------------------------------------------------------------------- --
    Add_Items( v_task_id );

    -- --------------------------------------------------------------------- --
    --  Удалить начисленния из item-ов
    -- --------------------------------------------------------------------- --
    --Del_Items( v_task_id );

    -- --------------------------------------------------------------------- --
    --  Проставить статус загрузки в Billing Server В.Малиновского
    -- либо перенесем в финальную процедуру биллинга, либо нужно как-то ускорять
    -- --------------------------------------------------------------------- --
    Callback( v_task_id );

    -- --------------------------------------------------------------------- --
    --  Выгрузка отчета в Billing Server В.Малиновского
    -- --------------------------------------------------------------------- --
    Report(p_period_id);

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
--  корректировка данных 2009 биллинга
-- --------------------------------------------------------------------- --
PROCEDURE Correct_Billing
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Correct_Billing';
    v_count    INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- проставляем тарифный план на заказы 2009 биллинга, где его нет
    MERGE INTO ORDER_T O
    USING (
          SELECT O.ORDER_ID
            FROM ORDER_T O, ACCOUNT_T A
           WHERE O.ACCOUNT_ID = A.ACCOUNT_ID
             AND A.BILLING_ID = 2009
             AND (O.RATEPLAN_ID IS NULL OR O.RATEPLAN_ID != c_RATEPLAN_BILSRV_ID)
    ) T
    ON (
       O.ORDER_ID = T.ORDER_ID
    )
    WHEN MATCHED THEN UPDATE SET O.RATEPLAN_ID = c_RATEPLAN_BILSRV_ID;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Order_t.rateplan: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- корректируем компоненты услуг трафика, который обрабатывается в 2009 биллинге
    MERGE INTO ORDER_BODY_T OB
    USING (
        SELECT OB.ORDER_BODY_ID
          FROM ORDER_T O, ACCOUNT_T A, ORDER_BODY_T OB
         WHERE O.ACCOUNT_ID = A.ACCOUNT_ID
           AND A.BILLING_ID = 2009
           AND O.ORDER_ID = OB.ORDER_ID
           AND OB.CHARGE_TYPE = 'USG'
           AND ( OB.RATEPLAN_ID IS NULL OR 
                 OB.RATEPLAN_ID  != 6 OR
                 OB.RATE_RULE_ID IS NULL OR
                 OB.RATE_RULE_ID != 2420
               )
    ) T
    ON (
        OB.ORDER_BODY_ID = T.ORDER_BODY_ID
    )
    WHEN MATCHED THEN UPDATE SET OB.RATEPLAN_ID = 6, RATE_RULE_ID = 2420;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Order_body_t.rateplan/rate_rule_id: '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- --------------------------------------------------------------------- --
--  Загрузка детализаций из Billing Server в BRM
-- --------------------------------------------------------------------- --
PROCEDURE Load_Details(
               p_period_id IN INTEGER
           ) 
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Load_Details';
    v_count    INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    INSERT INTO DETAIL_BSRV_T(
        RECORD_ID, PERIOD_ID, ORDER_NO, DATE_FROM, DATE_TO, PREFIX, ZONE, CALLS_NUM,
        MINS, TARIFF_MIN, GROSS, DIRECTION, INTERNATIONAL, CURRENCY_ID, DATE_OF_TARIFF,
        CREATED_BY, ACTION, STATUS_DATE
    )
    SELECT     
        RECORD_ID, PERIOD_ID, ORDER_NO, DATE_FROM, DATE_TO, PREFIX, ZONE, CALLS_NUM,
        MINS, TARIFF_MIN, GROSS, DIRECTION, INTERNATIONAL, 
        DECODE(CURRENCY_ID, 250, 960, CURRENCY_ID), DATE_OF_TARIFF,
        CREATED_BY, ACTION, SYSDATE STATUS_DATE 
      FROM tbl_brm_input@SQLMMTSDB.WORLD
     WHERE PERIOD_ID = p_period_id
       AND STATUS IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop, '||v_count||' rows loaded', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
--  Привязка детализаций к заказам, возвращает TASK_ID
-- --------------------------------------------------------------------- --
FUNCTION Bind_Details(
               p_period_id IN INTEGER
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Bind_Details';
    v_count       INTEGER;
    v_task_id     INTEGER;

BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    v_task_id     := SQ_BILLING_QUEUE_T.NEXTVAL;
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- ставим данные в очередь на обработку
    UPDATE DETAIL_BSRV_T D SET TASK_ID = v_task_id
     WHERE D.PERIOD_ID = p_period_id
       AND D.STATUS IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('task_id = '||v_task_id||' set for '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- привязываем строки детализации к заказам (status = 1)
    MERGE INTO DETAIL_BSRV_T D
    USING (
        SELECT D.RECORD_ID, O.ORDER_ID, O.SERVICE_ID, O.DATE_FROM, O.DATE_TO 
          FROM ORDER_T O, ACCOUNT_T A, DETAIL_BSRV_T D
         WHERE 1=1 
           -- на даты заказов внимания не обращаем
           --AND O.DATE_FROM  < TO_DATE('31.05.2016 23:59:58','dd.mm.yyyy hh24:mi:ss')    -- 26.696 / 51292
           --AND (O.DATE_TO IS NULL OR TO_DATE('01.05.2016','dd.mm.yyyy') < O.DATE_TO )         -- 51.259
           AND O.ORDER_NO    = D.ORDER_NO 
           AND D.PERIOD_ID   = p_period_id
           AND D.TASK_ID     = v_task_id
           AND D.STATUS      IS NULL
           AND O.RATEPLAN_ID = c_RATEPLAN_BILSRV_ID -- обязательный признак внешнего обслуживания
           AND O.ACCOUNT_ID = A.ACCOUNT_ID
           AND A.BILLING_ID = 2009  -- биллинг для MSSQL BILLING_SERVER
    ) OD
    ON (
       D.RECORD_ID = OD.RECORD_ID
    )
    WHEN MATCHED THEN UPDATE SET 
         D.ORDER_ID    = OD.ORDER_ID, 
         D.SERVICE_ID  = OD.SERVICE_ID, 
         D.STATUS      = c_STAT_BIND_OK, 
         D.STATUS_DATE = SYSDATE
    ;    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Order bind for '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    -- проставляем статус - ошибка привязки
    UPDATE DETAIL_BSRV_T D
       SET D.STATUS  = -c_STAT_BIND_OK
     WHERE D.TASK_ID = v_task_id
       AND D.STATUS  IS NULL
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Order not bind for '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- 
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    RETURN v_task_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- проставляем компоненты для местного (125) и зонового трафика (140, 167)
-- ------------------------------------------------------------------------ --
PROCEDURE Set_LZ_OrdeBody(
               p_task_id IN INTEGER
           )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Set_LZ_OrdeBody';
    v_count     INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    MERGE INTO DETAIL_BSRV_T D
    USING (
        WITH OO AS (
            SELECT A.ACCOUNT_NO, A.ACCOUNT_ID,
                   O.ORDER_NO, O.ORDER_ID,
                   O.SERVICE_ID, 
                   OB.ORDER_BODY_ID,
                   OB.SUBSERVICE_ID
              FROM ACCOUNT_T A, 
                   ORDER_T O, 
                   ORDER_BODY_T OB
             WHERE A.BILLING_ID = 2009
               AND A.ACCOUNT_ID = O.ACCOUNT_ID
               AND O.ORDER_ID   = OB.ORDER_ID
        ) 
        SELECT D.RECORD_ID, OO.SUBSERVICE_ID, OO.ORDER_BODY_ID 
          FROM OO, DETAIL_BSRV_T D
         WHERE OO.ORDER_NO   = D.ORDER_NO
           AND OO.ORDER_ID   = D.ORDER_ID
           AND OO.SERVICE_ID = D.SERVICE_ID
           AND D.STATUS      = c_STAT_BIND_OK
           AND D.SERVICE_ID IN (125, 140, 167)
           AND D.TASK_ID     = p_task_id
    ) OD
    ON (
        D.RECORD_ID = OD.RECORD_ID
    )
    WHEN MATCHED THEN UPDATE 
                         SET D.SUBSERVICE_ID = OD.SUBSERVICE_ID,
                             D.ORDER_BODY_ID = OD.ORDER_BODY_ID,
                             D.STATUS        = c_STAT_ADD_INFO_OK
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Set '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- проставляем компоненты для мг/мн (1,2)
-- ------------------------------------------------------------------------ --
PROCEDURE Set_MgMn_OrdeBody(
               p_task_id IN INTEGER
           )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Set_MgMn_OrdeBody';
    v_count     INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    MERGE INTO DETAIL_BSRV_T D
    USING (
        WITH OO AS (
            SELECT A.ACCOUNT_NO, A.ACCOUNT_ID,
                   O.ORDER_NO, O.ORDER_ID,
                   O.SERVICE_ID,
                   OB.ORDER_BODY_ID, 
                   OB.SUBSERVICE_ID
              FROM ACCOUNT_T A, 
                   ORDER_T O, 
                   ORDER_BODY_T OB
             WHERE A.BILLING_ID = 2009
               AND A.ACCOUNT_ID = O.ACCOUNT_ID
               AND O.ORDER_ID   = OB.ORDER_ID
        ) 
        SELECT D.RECORD_ID, OO.SUBSERVICE_ID, OO.ORDER_BODY_ID 
          FROM OO, DETAIL_BSRV_T D
         WHERE OO.ORDER_NO   = D.ORDER_NO
           AND OO.ORDER_ID   = D.ORDER_ID
           AND OO.SERVICE_ID = D.SERVICE_ID
           AND D.STATUS      = c_STAT_BIND_OK
           AND D.SERVICE_ID NOT IN (125, 140, 167, 142)
           AND D.TASK_ID     = p_task_id
           AND OO.SUBSERVICE_ID = CASE
                                     WHEN D.INTERNATIONAL = 0  THEN 1 -- (MG) нумерация РФ
                                     WHEN D.INTERNATIONAL = 1  THEN 2 -- (MN) нумерация не РФ
                                  END
    ) OD
    ON (
        D.RECORD_ID = OD.RECORD_ID
    )
    WHEN MATCHED THEN UPDATE 
                         SET D.SUBSERVICE_ID = OD.SUBSERVICE_ID,
                             D.ORDER_BODY_ID = OD.ORDER_BODY_ID,
                             D.STATUS        = c_STAT_ADD_INFO_OK
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Set '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- проставляем компоненты для услуг местного присоединения (142)
-- ------------------------------------------------------------------------ --
PROCEDURE Set_OperL_OrdeBody(
               p_task_id IN INTEGER
           )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Set_OperL_OrdeBody';
    v_count     INTEGER := 0;
    v_ssmax_id  INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ------------------------------------------------------------------------ --
    -- добавляем недостающие компоненты для услуг местного присоединения (142)
    -- ------------------------------------------------------------------------ --
    SELECT SQ_SUBSERVICE_ID.NEXTVAL  INTO v_ssmax_id
      FROM DUAL;

    INSERT INTO SUBSERVICE_T(
        SUBSERVICE_ID, SUBSERVICE_KEY, SUBSERVICE, SHORTNAME, PARENT_ID, CHARGE_TYPE
    )
    SELECT SQ_SUBSERVICE_ID.NEXTVAL SUBSERVICE_ID, 
           'LTERM-'||v_ssmax_id  SUBSERVICE_KEY,-- (v_ssmax_id + ROWNUM) SUBSERVICE_KEY, 
           SUBSERVICE, SUBSERVICE SHORTNAME, 1001 PARENT_ID, 'USG' CHARGE_TYPE
      FROM ( 
        SELECT DISTINCT D.ZONE SUBSERVICE 
          FROM DETAIL_BSRV_T D
         WHERE D.SERVICE_ID = 142
           AND D.STATUS     = c_STAT_BIND_OK
           AND D.TASK_ID    = p_task_id
           AND NOT EXISTS (
              SELECT * FROM SUBSERVICE_T SS
               WHERE SS.SUBSERVICE = D.ZONE
           )
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Add new subservices: '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ------------------------------------------------------------------------ --
    -- добавляем в заказ недостающие компоненты для услуг местного присоединения (142)
    -- ------------------------------------------------------------------------ --
    INSERT INTO ORDER_BODY_T(
        ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
        DATE_FROM, DATE_TO, 
        RATE_RULE_ID, RATEPLAN_ID,
        CREATE_DATE, MODIFY_DATE
    )
    SELECT PK02_POID.NEXT_ORDER_BODY_ID ORDER_BODY_ID,
           ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, 
           DATE_FROM, DATE_TO, 
           RATE_RULE_ID, RATEPLAN_ID,
           CREATE_DATE, MODIFY_DATE
      FROM ( 
        SELECT DISTINCT OO.ORDER_ID, SS.SUBSERVICE_ID, 'USG' CHARGE_TYPE,
               OO.DATE_FROM, OO.DATE_TO, 2420 RATE_RULE_ID, 6 RATEPLAN_ID,
               SYSDATE CREATE_DATE, SYSDATE MODIFY_DATE
          FROM DETAIL_BSRV_T D, SUBSERVICE_T SS,
              (
                SELECT A.ACCOUNT_NO, A.ACCOUNT_ID,
                       O.ORDER_NO, O.ORDER_ID,
                       O.SERVICE_ID, O.DATE_FROM, O.DATE_TO
                  FROM ACCOUNT_T A, ORDER_T O
                 WHERE A.BILLING_ID = 2009
                   AND A.ACCOUNT_ID = O.ACCOUNT_ID
                   AND O.SERVICE_ID = 142
              ) OO 
         WHERE OO.ORDER_NO   = D.ORDER_NO
           AND OO.ORDER_ID   = D.ORDER_ID
           AND OO.SERVICE_ID = D.SERVICE_ID
           AND D.STATUS      = c_STAT_BIND_OK
           AND D.SERVICE_ID  = 142
           AND D.TASK_ID     = p_task_id
           AND SS.SUBSERVICE =  D.ZONE
           AND NOT EXISTS (
               SELECT * FROM ORDER_BODY_T OB
                WHERE OB.ORDER_ID      = OO.ORDER_ID
                  AND OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
                  AND OB.CHARGE_TYPE   = 'USG'
                  AND OB.RATE_RULE_ID  = 2420
                  AND OB.RATEPLAN_ID   = 6
           )
        );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Add new order_body: '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- ------------------------------------------------------------------------ --
    -- проставляем компоненты для услуг местного присоединения (142)
    -- ------------------------------------------------------------------------ --
    MERGE INTO DETAIL_BSRV_T D
    USING (
        WITH OO AS (
            SELECT A.ACCOUNT_NO, A.ACCOUNT_ID,
                   O.ORDER_NO, O.ORDER_ID,
                   O.SERVICE_ID, S.SERVICE, 
                   OB.ORDER_BODY_ID,
                   OB.SUBSERVICE_ID, SS.SUBSERVICE
              FROM ACCOUNT_T A, 
                   ORDER_T O, SERVICE_T S, 
                   ORDER_BODY_T OB, SUBSERVICE_T SS
             WHERE A.BILLING_ID = 2009
               AND A.ACCOUNT_ID = O.ACCOUNT_ID
               AND O.SERVICE_ID = S.SERVICE_ID
               AND O.ORDER_ID   = OB.ORDER_ID
               AND OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
               AND O.SERVICE_ID = 142
        ) 
        SELECT D.RECORD_ID, OO.SUBSERVICE_ID, OO.ORDER_BODY_ID
          FROM OO, DETAIL_BSRV_T D
         WHERE OO.ORDER_NO   = D.ORDER_NO
           AND OO.ORDER_ID   = D.ORDER_ID
           AND OO.SERVICE_ID = D.SERVICE_ID
           AND D.STATUS      = c_STAT_BIND_OK
           AND D.SERVICE_ID  = 142
           AND D.TASK_ID     = p_task_id
           AND OO.SUBSERVICE = D.ZONE
    ) OD
    ON (
        D.RECORD_ID = OD.RECORD_ID
    )
    WHEN MATCHED THEN UPDATE 
                         SET D.SUBSERVICE_ID = OD.SUBSERVICE_ID,
                             D.ORDER_BODY_ID = OD.ORDER_BODY_ID,
                             D.STATUS        = c_STAT_ADD_INFO_OK
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Set '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
--  Добавить дополнительную информацию в детализацию
-- --------------------------------------------------------------------- --
PROCEDURE Add_info_to_Details(
               p_task_id IN INTEGER
           )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Add_info_to_Details';
    v_count     INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- проставляем компоненты для местного (125) и зонового трафика (140, 167)
    Set_LZ_OrdeBody( p_task_id );

    -- проставляем компоненты для мг/мн (1,2)
    Set_MgMn_OrdeBody( p_task_id );

    -- проставляем компоненты для услуг местного присоединения (142)
    Set_OperL_OrdeBody( p_task_id );

    -- проставляем статус - ошибка обогащения
    UPDATE DETAIL_BSRV_T D
       SET D.STATUS  = -c_STAT_ADD_INFO_OK
     WHERE D.TASK_ID = p_task_id
       AND D.STATUS  = c_STAT_BIND_OK
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Order not bind for '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
--  Добавить счета
-- --------------------------------------------------------------------- --
PROCEDURE Add_Bills(
               p_task_id IN INTEGER
           )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Add_Bills';
    v_count     INTEGER := 0;
    v_ok        INTEGER := 0;
    v_err       INTEGER := 0;
    v_bill_id   INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- создаем недостающие счета
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FOR rb IN (
        SELECT D.PERIOD_ID, O.ACCOUNT_ID 
          FROM DETAIL_BSRV_T D, ORDER_T O
         WHERE D.STATUS   = c_STAT_ADD_INFO_OK
           AND D.ORDER_ID = O.ORDER_ID
           AND D.TASK_ID  = p_task_id
           AND NOT EXISTS (
               SELECT * 
                 FROM BILL_T B, PERIOD_T P
                WHERE B.REP_PERIOD_ID = D.PERIOD_ID
                  AND B.ACCOUNT_ID    = O.ACCOUNT_ID
                  AND B.BILL_TYPE     = 'B'
                  AND B.REP_PERIOD_ID = P.PERIOD_ID
                  AND P.POSITION IN ('LAST','BILL','OPEN')
           )
         GROUP BY D.PERIOD_ID, O.ACCOUNT_ID
    )
    LOOP
      BEGIN
          v_bill_id := PK07_BILL.Next_recuring_bill (
                         p_account_id    => rb.account_id,
                         p_rep_period_id => rb.period_id
                     );
          v_ok := v_ok + 1;
      EXCEPTION
          WHEN OTHERS THEN
            v_err := v_err + 1;
            UPDATE DETAIL_BSRV_T D 
               SET D.STATUS   = -c_STAT_MK_BILL_OK
             WHERE D.STATUS   =  c_STAT_ADD_INFO_OK
               AND D.STATUS_DATE = SYSDATE;
      END;
    END LOOP;
    Pk01_Syslog.Write_msg('Add bills: '||v_ok||' ok, '||v_err||' err'
                          , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
                          
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- проставляем номера счетов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    MERGE INTO DETAIL_BSRV_T D
    USING (
        SELECT D.RECORD_ID, B.REP_PERIOD_ID, B.BILL_ID, B.BILL_NO
          FROM DETAIL_BSRV_T D, ORDER_T O, BILL_T B, PERIOD_T P
         WHERE D.STATUS        = c_STAT_ADD_INFO_OK
           AND D.ORDER_ID      = O.ORDER_ID
           AND D.TASK_ID       = p_task_id
           AND B.REP_PERIOD_ID = D.PERIOD_ID
           AND B.ACCOUNT_ID    = O.ACCOUNT_ID
           AND B.BILL_TYPE     = 'B'
           AND B.REP_PERIOD_ID = P.PERIOD_ID
           AND P.POSITION   IN ('LAST','BILL','OPEN')
    ) X
    ON (
       D.RECORD_ID = X.RECORD_ID    
    )
    WHEN MATCHED THEN UPDATE SET D.BILL_ID = X.BILL_ID, 
                                 D.BILL_NO = X.BILL_NO,
                                 D.STATUS  = c_STAT_MK_BILL_OK
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('set bill_no for '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
--  Добавить начисленния в item-ы
-- --------------------------------------------------------------------- --
PROCEDURE Add_Items(
               p_task_id IN INTEGER
           )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Add_Items';
    v_count_ins INTEGER := 0;
    v_count_upd INTEGER := 0;
    v_count_err INTEGER := 0;
    v_item_id   INTEGER := NULL;
    v_date_from DATE; 
    v_date_to   DATE;
    v_status    INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- формируем позиции счетов (item_t)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FOR ri IN (
        SELECT D.BILL_ID, D.PERIOD_ID, 
               Pk00_Const.c_ITEM_TYPE_BILL ITEM_TYPE,       -- 'B'
               D.ORDER_ID, D.SERVICE_ID, D.ORDER_BODY_ID, 
               D.SUBSERVICE_ID, 
               Pk00_Const.c_CHARGE_TYPE_USG CHARGE_TYPE,    -- 'USG'
               SUM(GROSS) ITEM_TOTAL, 
               TRUNC(MIN(DATE_FROM)) DATE_FROM, TRUNC(MAX(DATE_TO))+ 86399/86400 DATE_TO, 
               Pk00_Const.c_ITEM_STATE_OPEN ITEM_STATUS,    -- 'OPEN'
               SYSDATE CREATE_DATE, SYSDATE MODIFY_DATE,
               Pk00_Const.c_RATEPLAN_TAX_NOT_INCL TAX_INCL, -- 'N'
               10 EXTERNAL_ID, 
               CURRENCY_ID, 
               COUNT(*) CNT 
          FROM DETAIL_BSRV_T D
         WHERE STATUS  = c_STAT_MK_BILL_OK
           AND TASK_ID = p_task_id
         GROUP BY  D.BILL_ID, D.PERIOD_ID, D.ITEM_ID,
                   D.ORDER_ID, D.SERVICE_ID, D.ORDER_BODY_ID, D.SUBSERVICE_ID,
                   TRUNC(DATE_FROM, 'mm'),
                   D.CURRENCY_ID
         ORDER BY D.PERIOD_ID, D.BILL_ID, D.SERVICE_ID, D.SUBSERVICE_ID
    )
    LOOP
      BEGIN  
        -- попытка найти подходящую позицию счета
        SELECT MAX(I.ITEM_ID), MIN(I.DATE_FROM), MAX(I.DATE_TO)
          INTO v_item_id, v_date_from, v_date_to
          FROM ITEM_T I
         WHERE I.REP_PERIOD_ID         = ri.Period_Id
           AND I.BILL_ID               = ri.Bill_Id
           AND I.ORDER_ID              = ri.Order_Id
           AND I.SERVICE_ID            = ri.Service_Id
           AND I.ORDER_BODY_ID         = ri.Order_Body_Id
           AND I.SUBSERVICE_ID         = ri.Subservice_Id
           AND I.ITEM_CURRENCY_ID      = ri.CURRENCY_ID
           AND TRUNC(I.DATE_FROM,'mm') = TRUNC(ri.DATE_FROM,'mm')
        ;
        IF v_item_id IS NOT NULL THEN 
          -- изменяем существующий item
          UPDATE ITEM_T I 
             SET I.ITEM_TOTAL = I.ITEM_TOTAL + ri.ITEM_TOTAL,
                 I.DATE_FROM  = LEAST(I.DATE_FROM, ri.DATE_FROM),
                 I.DATE_TO    = GREATEST(I.DATE_TO, ri.DATE_TO) 
           WHERE I.ITEM_ID    = v_item_id
             AND I.REP_PERIOD_ID = ri.Period_Id
          ;
          v_count_upd := v_count_upd + 1;
        ELSE
          v_item_id := Pk02_Poid.Next_item_id;
          -- добавляем новый item
          INSERT INTO ITEM_T I(
               BILL_ID, REP_PERIOD_ID, ITEM_ID, 
               ORDER_ID, SERVICE_ID, ORDER_BODY_ID, SUBSERVICE_ID, 
               CHARGE_TYPE, ITEM_TOTAL, ITEM_CURRENCY_ID, 
               DATE_FROM, DATE_TO, TAX_INCL, ITEM_TYPE,
               ITEM_STATUS
            )VALUES (
               ri.BILL_ID, ri.PERIOD_ID, v_item_id, 
               ri.ORDER_ID, ri.SERVICE_ID, ri.ORDER_BODY_ID, ri.SUBSERVICE_ID, 
               ri.CHARGE_TYPE, ri.ITEM_TOTAL, ri.CURRENCY_ID, 
               ri.DATE_FROM, ri.DATE_TO, ri.TAX_INCL, ri.ITEM_TYPE,
               ri.ITEM_STATUS
            )
          ;
          v_count_ins := v_count_ins + 1;
        END IF;
        --
        v_status := c_STAT_MK_BILL_OK;
        
      EXCEPTION
          WHEN OTHERS THEN
              Pk01_Syslog.Write_error('ERROR', c_PkgName||'.'||v_prcName);
              v_count_err := v_count_err + 1;
              v_status := -c_STAT_MK_BILL_OK;
      END;

      -- заполняем отчет 
      UPDATE DETAIL_BSRV_T D 
         SET D.ITEM_ID       = v_item_id,
             D.STATUS        = c_STAT_MK_ITEM_OK,
             D.STATUS_DATE   = SYSDATE
       WHERE D.BILL_ID       = ri.BILL_ID   
         AND D.PERIOD_ID     = ri.PERIOD_ID 
         AND D.ORDER_ID      = ri.ORDER_ID 
         AND D.SERVICE_ID    = ri.SERVICE_ID 
         AND D.ORDER_BODY_ID = ri.ORDER_BODY_ID 
         AND D.SUBSERVICE_ID = ri.SUBSERVICE_ID
         AND TRUNC(D.DATE_FROM, 'mm') = TRUNC(ri.DATE_FROM, 'mm')
         AND D.CURRENCY_ID   = ri.CURRENCY_ID
         AND STATUS          = v_status
         AND TASK_ID         = p_task_id
      ;

    END LOOP;

    Pk01_Syslog.Write_msg('Items: '||v_count_ins||' ins, '
                                   ||v_count_upd||' upd, '
                                   ||v_count_err||' err'
                          , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
--  Удалить начисленния из item-ов
-- --------------------------------------------------------------------- --
PROCEDURE Del_Items(
               p_task_id IN INTEGER
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Del_Items';
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- привязыаем item-ы для удаления к записям типа 'D'
    -- уменьшаем значение item-а на указанную сумму
    -- удаляем item, если остаток равен 0
    -- ставим отметки в исходной таблице 

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
--  Проставить статус загрузки в Billing Server В.Малиновского
-- --------------------------------------------------------------------- --
PROCEDURE Callback(
               p_task_id IN INTEGER
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Callback';
    v_count    INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, task_id = '||p_task_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- на всякий случай
    DELETE FROM INFRANET.DETAIL_BSRV_REPORT_T R
     WHERE R.TASK_ID = p_task_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.DETAIL_BSRV_REPORT_T: '||v_count||' rows deleted, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- заполняем данными отчет
    INSERT INTO INFRANET.DETAIL_BSRV_REPORT_T (
      RECORD_ID, STATUS, STATUS_DATE,
      BILL_NO, BILL_ID, ITEM_ID, 
      ORDER_ID, SERVICE_ID,
      ORDER_BODY_ID, SUBSERVICE_ID,
      TASK_ID
    )
     SELECT D.RECORD_ID, 
            D.STATUS, D.STATUS_DATE,
            D.BILL_NO, D.BILL_ID, ITEM_ID, 
            D.ORDER_ID, D.SERVICE_ID,
            D.ORDER_BODY_ID, D.SUBSERVICE_ID,
            D.TASK_ID 
       FROM DETAIL_BSRV_T D
      WHERE D.TASK_ID = p_task_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INFRANET.DETAIL_BSRV_REPORT_T: '||v_count||' rows inserted, task_id = '||p_task_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    /*
    FOR ms IN (
         SELECT D.RECORD_ID, D.BILL_NO, D.BILL_ID, D.ITEM_ID, D.STATUS, D.STATUS_DATE 
           FROM DETAIL_BSRV_T D
          WHERE D.TASK_ID     = p_task_id )
    LOOP
      UPDATE tbl_brm_input@SQLMMTSDB.WORLD BS
         SET BS.BILL_NO = ms.Bill_No, 
             BS.BILL_ID = ms.Bill_Id, 
             BS.ITEM_ID = ms.Item_Id, 
             BS.STATUS  = ms.status, 
             BS.STATUS_DATE = ms.STATUS_DATE
       WHERE BS.RECORD_ID = ms.record_id;
      v_count := v_count + 1;
      IF MOD(v_count, 100) = 0 THEN
          Pk01_Syslog.Write_msg('tbl_brm_input: '||v_count||' rows update', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
      END IF;
    END LOOP;
    Pk01_Syslog.Write_msg('tbl_brm_input: '||v_count||' rows update', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    */    
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------- --
--  Выгрузка отчета в Billing Server В.Малиновского
-- --------------------------------------------------------------------- --
PROCEDURE Report(
               p_period_id IN INTEGER
           )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Report';
    v_count    INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start, period_id = '||p_period_id, 
                          c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    DELETE FROM INFRANET.BRM_REPORT_T
     WHERE REP_PERIOD_ID = p_period_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('brm_report_t: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    INSERT INTO INFRANET.BRM_REPORT_T (
          ACCOUNT_NO, REP_PERIOD_ID, BILL_NO, BILL_TYPE, ORDER_NO, ORDER_ID, 
          SERVICE, SERVICE_ID, SUBSERVICE, SUBSERVICE_ID,
          ITEM_ID, ITEM_TYPE, CHARGE_TYPE, TAX_INCL, ITEM_TOTAL, REP_GROSS, REP_TAX, ITEM_CURRENCY_ID, 
          ITEM_CURRENCY_RATE, BILL_CURRENCY_ID, BILL_TOTAL
    )
    SELECT A.ACCOUNT_NO, B.REP_PERIOD_ID, B.BILL_NO, B.BILL_TYPE, O.ORDER_NO, O.ORDER_ID, 
           S.SERVICE, S.SERVICE_ID, SS.SUBSERVICE, SS.SUBSERVICE_ID,
           I.ITEM_ID, I.ITEM_TYPE, I.CHARGE_TYPE, I.TAX_INCL, I.ITEM_TOTAL, I.REP_GROSS, I.REP_TAX, I.ITEM_CURRENCY_ID, 
           I.ITEM_CURRENCY_RATE, B.CURRENCY_ID BILL_CURRENCY_ID, I.BILL_TOTAL
      FROM ACCOUNT_T A, ORDER_T O, ORDER_BODY_T OB,
           SERVICE_T S, SUBSERVICE_T SS, 
           BILL_T B, ITEM_T I
     WHERE B.REP_PERIOD_ID = p_period_id
       AND B.REP_PERIOD_ID = I.REP_PERIOD_ID
       AND B.BILL_ID       = I.BILL_ID
       AND B.ACCOUNT_ID    = A.ACCOUNT_ID
       AND A.BILLING_ID    = 2009
       AND O.ACCOUNT_ID    = A.ACCOUNT_ID
       AND I.ORDER_ID      = O.ORDER_ID
       AND O.SERVICE_ID    = S.SERVICE_ID
       AND I.ORDER_BODY_ID = OB.ORDER_BODY_ID
       AND OB.SUBSERVICE_ID= SS.SUBSERVICE_ID
     ORDER BY A.ACCOUNT_NO, B.BILL_NO, O.ORDER_NO, S.SERVICE, SS.SUBSERVICE
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('brm_report_t: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR ('||v_count||' row): ', c_PkgName||'.'||v_prcName );
END;


END PK23_BILLSRV;
/
