CREATE OR REPLACE PACKAGE PK104_X2_TOPS
IS
    --
    -- Пакет для работы с топологией присоединенных операторов связи ( ТОПС )
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK104_X2_TOPS';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER        constant integer :=-1;
    
    type t_refc is ref cursor;

    -- ID тарификатора ТОПС (DICTIONARY_T)
    с_RATESYS_ID CONSTANT INTEGER := Pk00_Const.c_RATESYS_TOPS_ID;

    -- ID услуги местная и зоновая связь
    c_SERVICE_ID CONSTANT INTEGER := Pk00_Const.c_SERVICE_OP_LOCAL;

            -- типы тарифа 
            cTARIF_VOL_TYPE_NO      CONSTANT INTEGER := 0; -- обычный плоский тариф (данные о ценах лежат в X07_ORD_PRICE_T) 
            cTARIF_VOL_TYPE_VOL     CONSTANT INTEGER := 1; -- объемный тариф        (данные о ценах лежат в X07_ORD_PRICE_V_T)

            -- признак наличия гарантированного объема по услугам
            cFLAG_GARANT_VOL_NO     CONSTANT INTEGER := 0; -- без гарантированного объема
            cFLAG_GARANT_VOL_YES    CONSTANT INTEGER := 1; -- с гарантированным объемом


    -- получить значение c_SERVICE_ID 
    function get_SERVICE_ID return number;


-- для логирования операций
     gv_app_user   L01_MESSAGES.APP_USER%TYPE;
     gv_message   varchar2(32000);

    procedure log_init(
        p_OS_USER   L01_MESSAGES.OS_USER%TYPE,
        p_APP_USER   L01_MESSAGES.APP_USER %TYPE
    );


    -- получить данные о допустимых компонентах услуги (SUBSERVICE_ID)
    --   - при ошибке выставляет исключение
    PROCEDURE Subservice_list( 
                   p_recordset  OUT t_refc, 
                   p_service_id IN INTEGER DEFAULT PK104_TOPS.c_SERVICE_ID
               );

    -- добавить компонентах услуги (SUBSERVICE_ID)
    --   - при ошибке выставляет исключение
    PROCEDURE Subservice_Add( 
                   p_subservice_id  IN INTEGER,    -- ID
                   p_subservice_key IN VARCHAR2,   -- код - краткое имя компонента услуги
                   p_subservice     IN VARCHAR2,   -- полное имя компонента услуги
                   p_service_id     IN INTEGER DEFAULT PK104_TOPS.c_SERVICE_ID
               );
    PROCEDURE Subservice_Update ( 
                   p_subservice_id  IN INTEGER,    -- ID
                   p_subservice_key IN VARCHAR2,   -- код - краткое имя компонента услуги
                   p_subservice     IN VARCHAR2      -- полное имя компонента услуги
               );

    PROCEDURE Subservice_Delete ( 
                   p_subservice_id  IN INTEGER    -- ID
               );


    -- список локальных услуг подсистемы ведения тарифов.
    -- именно эти услуги проставляются в тарифе.
    -- локальные услуги привязаны к стандартным услугам (ссылка на subservice_t)

    PROCEDURE lservice_list( 
                   p_recordset  OUT t_refc 
               );

    PROCEDURE lservice_Add( 
                   p_id             IN INTEGER,    -- ID
                   p_key            IN VARCHAR2,   -- код - краткое имя компонента услуги
                   p_name           IN VARCHAR2,   -- полное имя компонента услуги
                   p_subservice_id  IN INTEGER 
               );

    PROCEDURE lservice_Update ( 
                   p_id             IN INTEGER,    -- ID
                   p_key            IN VARCHAR2,   -- код - краткое имя компонента услуги
                   p_name           IN VARCHAR2,   -- полное имя компонента услуги
                   p_subservice_id  IN INTEGER 
               );

    PROCEDURE lservice_Delete ( 
                   p_id  IN INTEGER    -- ID
               );


    -- получить список коммутаторов для указанного xTTK
    PROCEDURE Switch_list(
                   p_recordset  OUT t_refc, 
                   p_xttk_id    IN INTEGER       -- CONTRACTOR_T.CONTRACTOR_ID
               );

    -- поиск за период по номеру договора, ЛС, Заказа
    procedure ContrAccOrder_search(
                   p_recordset  OUT t_refc, 
                   p_contract_no IN VARCHAR2, 
                   p_account_no IN VARCHAR2,      -- ACCOUNT_T.ACCOUNT_NO
                   p_order_no    IN VARCHAR2,
                   p_dt_from     IN date,            --between ACCOUNT_PROFILE_T.DATE_FROM  &  DATE_TO  -- peresetsky
                   p_dt_to         IN date
               );

    -- получить информацию о лицевом счета
    PROCEDURE Account_info (
                   p_recordset  OUT t_refc, 
                   p_account_no   IN VARCHAR2,      -- ACCOUNT_T.ACCOUNT_NO
                   p_account_id    in integer,
                   p_contract_id   in integer,
                   p_account_date   IN date            --between ACCOUNT_PROFILE_T.DATE_FROM  &  DATE_TO  -- peresetsky
               );
    PROCEDURE Account_info (
                   p_recordset  OUT t_refc, 
                   p_account_no IN VARCHAR2,      -- ACCOUNT_T.ACCOUNT_NO
                   p_account_id  in integer,
                   p_contract_id in integer,
                   p_dt_from     IN date,            --between ACCOUNT_PROFILE_T.DATE_FROM  &  DATE_TO  -- peresetsky
                   p_dt_to         IN date
               );

    -- получить информацию по договору
--    PROCEDURE Contract_info (
--                   p_recordset  OUT t_refc, 
--                   p_contract_no IN VARCHAR2     -- CONTRACT_T.CONTRACT_NO
--               );
    PROCEDURE Contract_info (
                   p_recordset  OUT t_refc, 
                   p_contract_no      IN VARCHAR2,      -- CONTRACT_T.CONTRACT_NO
                   p_contract_date   IN date,                 --between CONTRACT_T.DATE_FROM  &  DATE_TO  -- peresetsky
                   p_contract_id       in integer,
                   p_account_id       in integer
               );
    PROCEDURE Contract_info (
                   p_recordset  OUT t_refc, 
                   p_contract_no      IN VARCHAR2,      -- CONTRACT_T.CONTRACT_NO
                   p_dt_from           IN date,                 --between CONTRACT_T.DATE_FROM  &  DATE_TO  -- peresetsky
                   p_dt_to               IN date,
                   p_contract_id       in integer,
                   p_account_id       in integer
           );
    PROCEDURE Contract_info (  -- список контрактов для справочника
                   p_recordset  OUT t_refc 
            );

    -- список заказов на л/с
--    PROCEDURE Order_list (
--                   p_recordset  OUT t_refc, 
--                   p_account_id IN INTEGER       -- ACCOUNT_T.ACCOUNT_ID
--               );
    PROCEDURE Order_list (
                   p_recordset  OUT t_refc, 
                   p_account_id IN INTEGER,       -- ACCOUNT_T.ACCOUNT_ID
                   p_order_no    in varchar2,       --ORDER_T.ORDER_NO   //peresetsky
                   p_order_date in date,              -- between ORDER_T.DATE_FROM & DATE_TO //peresetsky
                   p_order_id    in  integer
               );
    PROCEDURE Order_list (
                   p_recordset  OUT t_refc, 
                   p_account_id IN INTEGER,       -- ACCOUNT_T.ACCOUNT_ID
                   p_order_no    in varchar2,       --ORDER_T.ORDER_NO   --peresetsky
                   p_dt_from      in date,              -- between ORDER_T.DATE_FROM & DATE_TO   --peresetsky
                   p_dt_to          IN date,
                   p_order_id    in  integer
               );

    -- список компонентов услуг на заказе
    PROCEDURE Order_body (
                   p_recordset  OUT t_refc, 
                   p_order_id   IN INTEGER       -- ORDER_T.ORDER_ID
               );

    -- коммутаторы оператора
    PROCEDURE OpSwitch_list (
                   p_recordset      out t_refc, 
                   p_contract_id    in integer,
                   p_op_sw_id       in integer 
               );
    function OpSwitch_Add(
                   p_op_sw_id        in integer,
                   p_contract_id     in integer, 
                   p_op_sw_code      in varchar2, 
                   p_note            in varchar2,
                   p_name            in varchar2
               ) return number;  --p_rateplan_id;
    procedure OpSwitch_Update(
                   p_op_sw_id        in integer,
                   p_contract_id     in integer, 
                   p_op_sw_code      in varchar2, 
                   p_note            in varchar2,
                   p_name            in varchar2
               ); 
    procedure OpSwitch_Delete(
                   p_op_sw_id        in integer 
               );


    -- список транковых групп коммутаторов на заказе
    PROCEDURE Order_TG_list (
                   p_recordset  OUT t_refc, 
                   p_order_id   IN INTEGER       -- ORDER_T.ORDER_ID
               );
    function Order_TG_Add(
                    p_id                IN INTEGER,
                    p_order_id          IN INTEGER, 
                    p_switch_id         IN INTEGER, 
                    p_trunkgroup        IN VARCHAR2, 
                    p_trunkgroup_no     IN INTEGER, 
                    p_op_switch_id      IN INTEGER,
                    p_date_from         IN DATE, 
                    p_date_to           IN DATE
               ) return integer;
    PROCEDURE Order_TG_Update(
                    p_id                IN INTEGER,
                    p_order_id          IN INTEGER, 
                    p_switch_id         IN INTEGER, 
                    p_trunkgroup        IN VARCHAR2, 
                    p_trunkgroup_no     IN INTEGER, 
                    p_op_switch_id      IN INTEGER,
                    p_date_from         IN DATE, 
                    p_date_to           IN DATE
               );
    PROCEDURE Order_TG_Delete(
                   p_id                     IN INTEGER
               );


    -- тарифы  заголовок RATEPLAN_T
    PROCEDURE RatePlan_list (
                   p_recordset  out t_refc, 
                   p_rateplan_id in integer,
                   p_rateplan_name in varchar2
               )
               ;
    function RatePlan_Add(
                   p_rateplan_id        in integer, 
                   p_rateplan_name  in varchar2, 
                   p_note                   in varchar2
               ) return number  --p_rateplan_id;
               ;
    procedure RatePlan_Update(
                   p_rateplan_id        in integer, 
                   p_rateplan_name  in varchar2, 
                   p_note                   in varchar2
               ) 
               ;
    procedure RatePlan_Delete(
                   p_rateplan_id        in integer 
               )
               ;

    -- тарифы  заголовок OP_RATE_PLAN   (определяет тип тарифа)
    PROCEDURE OP_RatePlan_list (
                   p_recordset          out t_refc, 
                   p_op_rateplan_id     in integer,
                   p_rateplan_id        in integer,
                   p_op_rate_plan_type  in integer,
                   p_tarif_type         in varchar2  -- D или R (доход или расход)
               );
    function OP_RatePlan_Add(
                   p_op_rateplan_id     in integer,
                   p_rateplan_id        in integer,
                   p_op_rate_plan_type  in integer, 
                   p_tarif_vol_type     in integer,
                   p_flag_garant_vol    in integer
               ) return number  --p_op_rateplan_id;
               ;
    procedure OP_RatePlan_Update(
                   p_op_rateplan_id     in integer,
                   p_rateplan_id        in integer,
                   p_op_rate_plan_type  in integer, 
                   p_tarif_vol_type     in integer,
                   p_flag_garant_vol    in integer
               ); 
    procedure OP_RatePlan_Delete(
                   p_op_rate_plan_id        in integer 
               );
               

    -- доходный тариф
    PROCEDURE OrderService_D_list (
                   p_recordset          out t_refc,
                   p_op_rateplan_id     in integer,
                   p_sw_id              in integer,
                   p_date               in date
               );
    function OrderService_D_Add(
                    p_ID                in integer, 
                    p_OP_RATE_PLAN_ID   in integer, 
                    p_PHONE_FROM        in varchar2, 
                    p_PHONE_TO          in varchar2, 
                    p_SWITCH_ID         in varchar2, 
                    p_SUBSERVICE_ID     in varchar2, 
                    p_DATE_FROM         in date, 
                    p_DATE_TO           in date
               ) return number;  --p_ID;
    procedure OrderService_D_Update (
                    p_ID                in integer, 
                    p_OP_RATE_PLAN_ID   in integer, 
                    p_PHONE_FROM        in varchar2, 
                    p_PHONE_TO          in varchar2, 
                    p_SWITCH_ID         in varchar2, 
                    p_SUBSERVICE_ID     in varchar2, 
                    p_DATE_FROM         in date, 
                    p_DATE_TO           in date
               );
    procedure OrderService_D_Delete(
                   p_id        in integer 
               );

    -- расходный тариф
    PROCEDURE OrderService_R_list (
                   p_recordset          out t_refc,
                   p_op_rateplan_id     in integer,
                   p_sw_id              in integer,
                   p_date               in date
               );
    function OrderService_R_Add(
                    p_ID                in integer, 
                    p_OP_RATE_PLAN_ID   in integer, 
                    p_PHONE_FROM        in varchar2, 
                    p_PHONE_TO          in varchar2, 
                    p_OP_SW_ID          in varchar2, 
                    p_SUBSERVICE_ID     in varchar2, 
                    p_DATE_FROM         in date, 
                    p_DATE_TO           in date
               ) return number;  --p_ID;
    procedure OrderService_R_Update (
                    p_ID                in integer, 
                    p_OP_RATE_PLAN_ID   in integer, 
                    p_PHONE_FROM        in varchar2, 
                    p_PHONE_TO          in varchar2, 
                    p_OP_SW_ID         in varchar2, 
                    p_SUBSERVICE_ID     in varchar2, 
                    p_DATE_FROM         in date, 
                    p_DATE_TO           in date
               );
    procedure OrderService_R_Delete(
                   p_id        in integer 
               );

    -- цены для услуг X07_ORD_PRICE_T
    PROCEDURE OrderPrice_list (
                   p_recordset          out t_refc,
                   p_op_rateplan_id     in integer,
                   p_subservice_id      in integer,
                   p_date               in date
               );
    function OrderPrice_Add(
                    p_ID                in integer, 
                    p_OP_RATE_PLAN_ID   in integer, 
                    p_SUBSERVICE_ID     in integer, 
                    p_PRICE             in number, 
                    p_DATE_FROM         in date, 
                    p_DATE_TO           in date
               ) return number;  --p_ID;
    procedure OrderPrice_Update (
                    p_ID                in integer, 
                    p_OP_RATE_PLAN_ID   in integer, 
                    p_SUBSERVICE_ID     in integer, 
                    p_PRICE             in number, 
                    p_DATE_FROM         in date, 
                    p_DATE_TO           in date
               );
    procedure OrderPrice_Delete(
                   p_id        in integer 
               );

    -- цены для услуг по объему X07_ORD_PRICE_T
    PROCEDURE OrderPriceV_list (
                   p_recordset          out t_refc,
                   p_op_rateplan_id     in integer,
                   p_subservice_id      in integer,
                   p_date               in date
               );
    function OrderPriceV_Add(
                    p_ID                in integer, 
                    p_OP_RATE_PLAN_ID   in integer, 
                    p_SUBSERVICE_ID     in integer,
                    p_VOL               in integer, 
                    p_PRICE             in number, 
                    p_DATE_FROM         in date, 
                    p_DATE_TO           in date
               ) return number;  --p_ID;
    procedure OrderPriceV_Update (
                    p_ID                in integer, 
                    p_OP_RATE_PLAN_ID   in integer, 
                    p_SUBSERVICE_ID     in integer, 
                    p_VOL               in integer, 
                    p_PRICE             in number, 
                    p_DATE_FROM         in date, 
                    p_DATE_TO           in date
               );
    procedure OrderPriceV_Delete(
                   p_id        in integer 
               );

    -- гаранированные объемы X07_ORD_GV_T
    PROCEDURE GarantVolume_list (
                   p_recordset          out t_refc,
                   p_op_rateplan_id     in integer,
                   p_subservice_id      in integer,
                   p_date               in date
               );
    function GarantVolume_Add(
                    p_ID                in integer, 
                    p_OP_RATE_PLAN_ID   in integer, 
                    p_SUBSERVICE_ID     in integer, 
                    p_SUMMA             in number, 
                    p_DATE_FROM         in date, 
                    p_DATE_TO           in date
               ) return number;  --p_ID;
    procedure GarantVolume_Update (
                    p_ID                in integer, 
                    p_OP_RATE_PLAN_ID   in integer, 
                    p_SUBSERVICE_ID     in integer, 
                    p_SUMMA             in number, 
                    p_DATE_FROM         in date, 
                    p_DATE_TO           in date
               );
    procedure GarantVolume_Delete(
                   p_id        in integer 
               );
           

    -- проверка пересечений диапазонов номеров в доходных тарифах
    function check_pool_phone_range_D (
        p_op_rate_plan_id   in number,
        p_switch_id         in number, 
        p_phone_from        in varchar2, 
        p_phone_to          in varchar2,
        p_date_from         in date, 
        p_date_to           in date,
        p_id_exclude        in number -- эту запись не проверять. ее сейчас обновляем !
    ) return varchar2 -- error message
    ;
    -- проверка пересечений диапазонов номеров в расходных тарифах
    function check_pool_phone_range_R (
        p_op_rate_plan_id   in number,
        p_switch_id         in number, 
        p_phone_from        in varchar2, 
        p_phone_to          in varchar2,
        p_date_from         in date, 
        p_date_to           in date,
        p_id_exclude        in number -- эту запись не проверять. ее сейчас обновляем !
    ) return varchar2 -- error message
    ;
    -- проверка пересечений цен в тарифе
    function check_op_rate (
            p_op_rate_plan_id       in integer,
            p_subservice_id     in integer, 
            p_price                 in number, 
            p_date_from          in date, 
            p_date_to              in date,  
            p_rate_id             in number -- !=null => эту запись не проверять. ее сейчас обновляем !
    ) return varchar2 -- error message
    ;
 
    -- проверка пересечений сумм в гарантированном объеме
    function check_op_gv (
            p_op_rate_plan_id       in integer,
            p_subservice_id         in integer, 
            p_summa                 in number, 
            p_date_from             in date, 
            p_date_to               in date,  
            p_rate_id               in number -- !=null => эту запись не проверять. ее сейчас обновляем !
    ) return varchar2 -- error message
    ;
    
    
END PK104_X2_TOPS;
/
CREATE OR REPLACE PACKAGE BODY PK104_X2_TOPS
IS

    function get_pattern_like(p_str in varchar2) return varchar2
    is
      v_ret varchar2(255);
    begin
      v_ret := lower(trim(p_str));
      if v_ret is null then
        return null; 
      end if;

      v_ret := replace(v_ret,'*','%');
      v_ret := replace(v_ret,'?','_');
      
      return v_ret; 
    end;


-- диапазон дат для лога
   function date_range(p_D_from in date, p_D_to in date) return varchar2
   is
   begin
       return to_char(p_D_from,'dd.mm.yyyy')||'-'||to_char(p_D_to,'dd.mm.yyyy');
   end;   

    -- получить значение c_SERVICE_ID 
    function get_SERVICE_ID return number
    is
    begin
        return c_SERVICE_ID;
    end;


-- для логирования операций
    procedure log_init(
        p_OS_USER   L01_MESSAGES.OS_USER%TYPE,
        p_APP_USER   L01_MESSAGES.APP_USER %TYPE
    )
    is
    begin
        pk01_syslog.g_OS_USER := p_OS_USER;
        gv_app_user := p_APP_USER;  
    end;   

-- получить данные о допустимых компонентах услуги (SUBSERVICE_ID)
--   - при ошибке выставляет исключение
PROCEDURE Subservice_list( 
               p_recordset  OUT t_refc, 
               p_service_id IN INTEGER DEFAULT PK104_TOPS.c_SERVICE_ID
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Subservice_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
--        SELECT SS.SUBSERVICE_ID, SS.SUBSERVICE_KEY, SS.SUBSERVICE  
--          FROM SERVICE_SUBSERVICE_T SSS, SUBSERVICE_T SS
--         WHERE SSS.SERVICE_ID = p_service_id
--           AND SSS.SUBSERVICE_ID = SS.SUBSERVICE_ID
        SELECT SS.SUBSERVICE_ID, SS.SUBSERVICE_KEY, SS.SUBSERVICE  
        FROM 
            SERVICE_SUBSERVICE_T SSS, 
            SUBSERVICE_T SS, 
           (
            select * from service_t s
            where 
             --S.SERVICE_ID = 7 or S.PARENT_ID = 7
             S.SERVICE_ID = p_service_id or S.PARENT_ID = p_service_id
           ) s
        WHERE 
               SSS.SERVICE_ID = S.SERVICE_ID
           AND SSS.SUBSERVICE_ID = SS.SUBSERVICE_ID
                   --and SS.SUBSERVICE like '%иниции%'
           and SS.SUBSERVICE_ID between 1000 and 1099
        order by SS.SUBSERVICE_ID
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- добавить компонентах услуги (SUBSERVICE_ID)
--   - при ошибке выставляет исключение
PROCEDURE Subservice_Add( 
               p_subservice_id  IN INTEGER,    -- ID
               p_subservice_key IN VARCHAR2,   -- код - краткое имя компонента услуги
               p_subservice     IN VARCHAR2,   -- полное имя компонента услуги
               p_service_id     IN INTEGER DEFAULT PK104_TOPS.c_SERVICE_ID
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Subservice_Add';
    v_retcode    INTEGER;
BEGIN
    -- добавляем компонент
    INSERT INTO SUBSERVICE_T SS (SS.SUBSERVICE_ID, SS.SUBSERVICE_KEY, SS.SUBSERVICE)
    VALUES (p_subservice_id, p_subservice_key, p_subservice);
    -- привязываем компонет к услуге 
    INSERT INTO SERVICE_SUBSERVICE_T(SERVICE_ID, SUBSERVICE_ID)
    VALUES (p_service_id, p_subservice_id);
    
    gv_message:='{'||p_subservice_id||'}{'||p_subservice_key||'}{'||p_subservice||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE Subservice_Update ( 
               p_subservice_id  IN INTEGER,    -- ID
               p_subservice_key IN VARCHAR2,   -- код - краткое имя компонента услуги
               p_subservice     IN VARCHAR2      -- полное имя компонента услуги
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Subservice_Update';
    v_retcode    INTEGER;
    v_pr SUBSERVICE_T%rowtype;
BEGIN

    begin
            select * into v_pr from SUBSERVICE_T where  SUBSERVICE_ID = p_subservice_id;
        exception when no_data_found then
            raise_application_error(-20001,'Не найдена обновляемая услуга  id='|| p_subservice_id);
    end;  

    update SUBSERVICE_T SS 
       set
        SS.SUBSERVICE_KEY   = p_subservice_key,
        SS.SUBSERVICE          = p_subservice
    where SS.SUBSERVICE_ID = p_subservice_id
    ;
    
    gv_message:='{'||v_pr.subservice_id||'}{'||v_pr.subservice_key||'}{'||v_pr.subservice
                 ||'}->{'||p_subservice_key||'}{'||p_subservice||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE Subservice_Delete ( 
               p_subservice_id  IN INTEGER    -- ID
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Subservice_Delete';
    v_retcode    INTEGER;
    v_pr SUBSERVICE_T%rowtype;
BEGIN

    begin
            select * into v_pr from SUBSERVICE_T where  SUBSERVICE_ID = p_subservice_id;
        exception when no_data_found then
             null;
    end;  

    delete  SERVICE_SUBSERVICE_T 
    where SUBSERVICE_ID = p_subservice_id;

    delete SUBSERVICE_T SS 
    where SS.SUBSERVICE_ID = p_subservice_id
    ;
    
    gv_message:='{'||v_pr.subservice_id||'}{'||v_pr.subservice_key||'}{'||v_pr.subservice||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- список локальных услуг подсистемы ведения тарифов.
-- именно эти услуги проставляются в тарифе.
-- локальные услуги привязаны к стандартным услугам (ссылка на subservice_t)

PROCEDURE lservice_list( 
               p_recordset  OUT t_refc 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'lservice_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT SRV_ID, SRV_KEY, SRV_NAME, SUBSERVICE_ID
          FROM X07_SRV_DCT
        order by SRV_KEY
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

PROCEDURE lservice_Add( 
               p_id             IN INTEGER,    -- ID
               p_key            IN VARCHAR2,   -- код - краткое имя компонента услуги
               p_name           IN VARCHAR2,   -- полное имя компонента услуги
               p_subservice_id  IN INTEGER 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'lservice_Add';
    v_retcode    INTEGER;
BEGIN
    -- добавляем компонент
    INSERT INTO X07_SRV_DCT (SRV_ID, SRV_KEY, SRV_NAME, SUBSERVICE_ID)
    VALUES (p_id, p_key, p_name, p_subservice_id);
    
    gv_message:='{'||p_id||'}{'||p_key||'}{'||p_name||'}{'||p_subservice_id||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE lservice_Update ( 
               p_id             IN INTEGER,    -- ID
               p_key            IN VARCHAR2,   -- код - краткое имя компонента услуги
               p_name           IN VARCHAR2,   -- полное имя компонента услуги
               p_subservice_id  IN INTEGER 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'lservice_Update';
    v_retcode    INTEGER;
    v_pr X07_SRV_DCT%rowtype;
BEGIN

    begin
            select * into v_pr from X07_SRV_DCT where  SRV_ID = p_id;
        exception when no_data_found then
            raise_application_error(-20001,'Не найдена обновляемая услуга  id='|| p_id);
    end;  

    update X07_SRV_DCT 
       set
        SRV_KEY             = p_key,
        SRV_NAME            = p_name,
        SUBSERVICE_ID       = p_subservice_id 
    where SRV_ID = p_id
    ;
    
    gv_message:='{'||v_pr.srv_id||'}{'||v_pr.srv_key||'}{'||v_pr.srv_name||'}{'||v_pr.subservice_id 
                 ||'}->{'||p_key||'}{'||p_name||'}{'||p_subservice_id||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE lservice_Delete ( 
               p_id  IN INTEGER    -- ID
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'lservice_Delete';
    v_retcode    INTEGER;
    v_pr X07_SRV_DCT%rowtype;
BEGIN

    begin
            select * into v_pr from X07_SRV_DCT where  SRV_ID = p_id;
        exception when no_data_found then
             null;
    end;  

    delete  X07_SRV_DCT 
    where SRV_ID = p_id;

    
    gv_message:='{'||v_pr.srv_id||'}{'||v_pr.srv_key||'}{'||v_pr.srv_name||'}{'||v_pr.subservice_id||'}' ;    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--===============================================================================================



-- получить список коммутаторов для указанного xTTK
PROCEDURE Switch_list(
               p_recordset  OUT t_refc, 
               p_xttk_id    IN INTEGER       -- CONTRACTOR_T.CONTRACTOR_ID
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Switch_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT 
                SW.SWITCH_ID,
                SW.SWITCH_CODE, 
                SW.SWITCH_NAME, 
                C.CONTRACTOR_ID, 
                C.SHORT_NAME
          FROM SWITCH_T SW, CONTRACTOR_T C
         WHERE SW.CONTRACTOR_ID = C.CONTRACTOR_ID
           AND C.CONTRACTOR_TYPE = 'XTTK'
           AND (p_xttk_id IS NULL OR C.CONTRACTOR_ID = p_xttk_id)
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- поиск за период по номеру договора, ЛС, Заказа
procedure ContrAccOrder_search(
               p_recordset  OUT t_refc, 
               p_contract_no IN VARCHAR2, 
               p_account_no IN VARCHAR2,      -- ACCOUNT_T.ACCOUNT_NO
               p_order_no    IN VARCHAR2,
               p_dt_from     IN date,            --between ACCOUNT_PROFILE_T.DATE_FROM  &  DATE_TO  -- peresetsky
               p_dt_to         IN date
)
is
    v_prcName    CONSTANT VARCHAR2(30) := 'ContrAccOrder_search';
    v_retcode    INTEGER;
    v_dt_from    date;  
    v_dt_to        date;
    v_sd            date;
BEGIN
    v_sd := sysdate;
    v_dt_from := trunc(nvl(p_dt_from,v_sd-20000));
    v_dt_to     := trunc(nvl(v_dt_to,v_sd+20000))+1-1/(24*60*60);

    OPEN p_recordset FOR
        SELECT 
               O.ORDER_ID,                                                                                      --   1 
               O.ORDER_NO,                                                                                     --   2
               O.DATE_FROM,                                                                                   --   3
               O.DATE_TO,                                                                                       --   4
               O.NOTES,                                                                                             --   5
               O.SERVICE_ID,                                                                                    --   6
               S.SERVICE,                                                                                         --   7
               O.RATEPLAN_ID,                                                                                 --   8
               R.RATEPLAN_NAME,                                                                            --   9
               R.RATESYSTEM_ID,                                                                             --  10
               R.NOTE,                                                                                              --  11
               R.RATEPLAN_CODE,                                                                             --  12
               --
               O.ACCOUNT_ID,                                                                                   --  13
               A.ACCOUNT_NO,                                                                                  --  14
               CU.CUSTOMER_ID,                                                                               --  15
               CU.SHORT_NAME,                                                                                --  16
               CT.CONTRACTOR_ID,                                        --КТТК                         --  17
               CT.CONTRACTOR,                                                                                --  18
               BR.CONTRACTOR_ID BR_CONTRACTOR_ID,         -- xTTK                        --  19
               BR.CONTRACTOR BR_CONTRACTOR,                                                     --  20
               AG.CONTRACTOR_ID AG_CONTRACTOR_ID,       -- Агент                        --  21
               AG.CONTRACTOR AG_CONTRACTOR,                                                     --  22
               AP.DATE_FROM,                                                                                  --  23
               AP.DATE_TO,                                                                                      --  24
               --
               C.CONTRACT_ID,                                                                                 --  25
               C.CONTRACT_NO,                                                                                --  26
               C.DATE_FROM,                                                                                    --  27
               C.DATE_TO,                                                                                        --  28
               --
               A.CURRENCY_ID                                                                                   --  29
          FROM 
                ORDER_T O, 
                RATEPLAN_T R, 
                SERVICE_T S,
                ACCOUNT_T A, 
                ACCOUNT_PROFILE_T AP, 
                CONTRACT_T C, 
                CUSTOMER_T CU,
                CONTRACTOR_T CT, 
                CONTRACTOR_T BR, 
                CONTRACTOR_T AG
         WHERE 
                  O.SERVICE_ID = 7--c_SERVICE_ID
           AND O.SERVICE_ID = S.SERVICE_ID
           AND O.RATEPLAN_ID= R.RATEPLAN_ID(+)   --O.RATEPLAN_ID может быть null  --peresetsky
           --AND (p_account_id is null or O.ACCOUNT_ID = p_account_id)               --peresetsky
           AND (p_order_no is null or O.ORDER_NO like p_order_no)                    --peresetsky
           --and (p_order_id is null or p_order_id = O.ORDER_ID)
           AND (
                         v_dt_from between nvl(O.DATE_FROM,v_sd-20000) and nvl(O.DATE_TO,v_sd+20000 )
                    or  v_dt_to     between nvl(O.DATE_FROM,v_sd-20000) and nvl(O.DATE_TO,v_sd+20000 )
                    or  nvl(O.DATE_FROM,v_sd-20000) between v_dt_from and v_dt_to
                    or  nvl(O.DATE_TO,v_sd+20000 )   between v_dt_from and v_dt_to  
                  )
           AND A.ACCOUNT_ID = O.ACCOUNT_ID
           AND (p_account_no is null or   A.ACCOUNT_NO = p_account_no)
           AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND AP.CUSTOMER_ID = CU.CUSTOMER_ID
           AND AP.CONTRACTOR_ID = CT.CONTRACTOR_ID
           AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
           AND AP.AGENT_ID  = AG.CONTRACTOR_ID(+)
           --AND (p_account_id is null or A.ACCOUNT_ID = p_account_id)               --peresetsky
           AND (
                         v_dt_from between nvl(AP.DATE_FROM,v_sd-20000) and nvl(AP.DATE_TO,v_sd+20000 )
                    or  v_dt_to     between nvl(AP.DATE_FROM,v_sd-20000) and nvl(AP.DATE_TO,v_sd+20000 )
                    or  nvl(AP.DATE_FROM,v_sd-20000) between v_dt_from and v_dt_to
                    or  nvl(AP.DATE_TO,v_sd+20000 )   between v_dt_from and v_dt_to  
                  )
           AND (p_contract_no is null or C.CONTRACT_NO  = p_contract_no)
           --AND (p_contract_id is null or p_contract_id = C.CONTRACT_ID)  -- peresetsky
           AND (
                         v_dt_from between nvl(C.DATE_FROM,v_sd-20000) and nvl(C.DATE_TO,v_sd+20000 )
                    or  v_dt_to     between nvl(C.DATE_FROM,v_sd-20000) and nvl(C.DATE_TO,v_sd+20000 )
                    or  nvl(C.DATE_FROM,v_sd-20000) between v_dt_from and v_dt_to
                    or  nvl(C.DATE_TO,v_sd+20000 )   between v_dt_from and v_dt_to  
                  )
         order by O.DATE_FROM
       ;
       
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);

end;


-- получить информацию о лицевом счета
PROCEDURE Account_info (
               p_recordset  OUT t_refc, 
               p_account_no IN VARCHAR2,      -- ACCOUNT_T.ACCOUNT_NO
               p_account_id  in integer,
               p_contract_id in integer,
               p_account_date   IN date            --between ACCOUNT_PROFILE_T.DATE_FROM  &  DATE_TO  -- peresetsky
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Account_info';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT distinct
               A.ACCOUNT_ID, 
               A.ACCOUNT_NO,
               C.CONTRACT_ID, 
               C.CONTRACT_NO, 
               CU.CUSTOMER_ID, 
               CU.SHORT_NAME,
               CT.CONTRACTOR_ID,                                        --КТТК
               CT.CONTRACTOR, 
               BR.CONTRACTOR_ID BR_CONTRACTOR_ID,         -- xTTK
               BR.CONTRACTOR BR_CONTRACTOR,
               AG.CONTRACTOR_ID AG_CONTRACTOR_ID,       -- Агент
               AG.CONTRACTOR AG_CONTRACTOR,
               AP.DATE_FROM,
               AP.DATE_TO
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, CUSTOMER_T CU,
                   CONTRACTOR_T CT, CONTRACTOR_T BR, CONTRACTOR_T AG
         WHERE A.ACCOUNT_NO = p_account_no
           AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND AP.CUSTOMER_ID = CU.CUSTOMER_ID
           AND AP.CONTRACTOR_ID = CT.CONTRACTOR_ID
           AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
           AND AP.AGENT_ID  = AG.CONTRACTOR_ID(+)
           AND (p_account_id is null or A.ACCOUNT_ID = p_account_id)               --peresetsky
           AND (p_contract_id is null or p_contract_id = C.CONTRACT_ID)            --peresetsky
           AND (p_account_date is null or 
                            p_account_date between 
                                    nvl(AP.DATE_FROM,sysdate-20000) and nvl(AP.DATE_TO,sysdate+20000 ) )    -- peresetsky
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

PROCEDURE Account_info (
               p_recordset  OUT t_refc, 
               p_account_no IN VARCHAR2,      -- ACCOUNT_T.ACCOUNT_NO
               p_account_id  in integer,
               p_contract_id in integer,
               p_dt_from     IN date,            --between ACCOUNT_PROFILE_T.DATE_FROM  &  DATE_TO  -- peresetsky
               p_dt_to         IN date
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Account_info';
    v_retcode    INTEGER;
    v_dt_from    date;  
    v_dt_to        date;
    v_sd            date;
BEGIN
    v_sd := sysdate;
    v_dt_from := trunc(nvl(p_dt_from,v_sd-20000));
    v_dt_to     := trunc(nvl(v_dt_to,v_sd+20000))+1-1/(24*60*60);
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT distinct
               A.ACCOUNT_ID, 
               A.ACCOUNT_NO,
               C.CONTRACT_ID, 
               C.CONTRACT_NO, 
               CU.CUSTOMER_ID, 
               CU.SHORT_NAME,
               CT.CONTRACTOR_ID,                                        --КТТК
               CT.CONTRACTOR, 
               BR.CONTRACTOR_ID BR_CONTRACTOR_ID,         -- xTTK
               BR.CONTRACTOR BR_CONTRACTOR,
               AG.CONTRACTOR_ID AG_CONTRACTOR_ID,       -- Агент
               AG.CONTRACTOR AG_CONTRACTOR,
               AP.DATE_FROM,
               AP.DATE_TO,
               A.CURRENCY_ID
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, CUSTOMER_T CU,
                   CONTRACTOR_T CT, CONTRACTOR_T BR, CONTRACTOR_T AG
         WHERE 
                  (p_account_no is null or   A.ACCOUNT_NO = p_account_no)
           AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND AP.CUSTOMER_ID = CU.CUSTOMER_ID
           AND AP.CONTRACTOR_ID = CT.CONTRACTOR_ID
           AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
           AND AP.AGENT_ID  = AG.CONTRACTOR_ID(+)
           AND (p_account_id is null or A.ACCOUNT_ID = p_account_id)               --peresetsky
           AND (p_contract_id is null or p_contract_id = C.CONTRACT_ID)            --peresetsky
           AND (
                         v_dt_from between nvl(AP.DATE_FROM,v_sd-20000) and nvl(AP.DATE_TO,v_sd+20000 )
                    or  v_dt_to     between nvl(AP.DATE_FROM,v_sd-20000) and nvl(AP.DATE_TO,v_sd+20000 )
                    or  nvl(AP.DATE_FROM,v_sd-20000) between v_dt_from and v_dt_to
                    or  nvl(AP.DATE_TO,v_sd+20000 )   between v_dt_from and v_dt_to  
                  )
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;



-- получить информацию по договору
PROCEDURE Contract_info (
               p_recordset  OUT t_refc, 
               p_contract_no      IN VARCHAR2,      -- CONTRACT_T.CONTRACT_NO
               p_contract_date   IN date,                 --between CONTRACT_T.DATE_FROM  &  DATE_TO  -- peresetsky
               p_contract_id       in integer,
               p_account_id       in integer
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Contract_info';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT distinct
                C.CONTRACT_ID, 
                C.CONTRACT_NO,
                C.DATE_FROM,
                C.DATE_TO 
                --A.ACCOUNT_ID,
                --A.ACCOUNT_NO, 
                --CU.CUSTOMER_ID, 
                --CU.SHORT_NAME,
                --CT.CONTRACTOR_ID,                                           --- КТТК
                --CT.CONTRACTOR, 
                --BR.CONTRACTOR_ID      BR_CONTRACTOR_ID,      -- xTTK
                --BR.CONTRACTOR           BR_CONTRACTOR,
                --AG.CONTRACTOR_ID      AG_CONTRACTOR_ID,     --Агент
                --AG.CONTRACTOR           AG_CONTRACTOR
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, CUSTOMER_T CU,
               CONTRACTOR_T CT, CONTRACTOR_T BR, CONTRACTOR_T AG
         WHERE 
                  C.CONTRACT_NO  = p_contract_no
           AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND AP.CUSTOMER_ID = CU.CUSTOMER_ID
           AND AP.CONTRACTOR_ID = CT.CONTRACTOR_ID
           AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
           AND AP.AGENT_ID  = AG.CONTRACTOR_ID(+)
           AND (p_contract_date is null or 
                            p_contract_date between 
                                    nvl(C.DATE_FROM,sysdate-20000) and nvl(C.DATE_TO,sysdate+20000 ) )    -- peresetsky
           AND (p_contract_id is null or p_contract_id = C.CONTRACT_ID)  -- peresetsky
           AND (p_account_id is null or p_account_id = A.ACCOUNT_ID)    -- peresetsky
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

PROCEDURE Contract_info (
               p_recordset  OUT t_refc, 
               p_contract_no      IN VARCHAR2,      -- CONTRACT_T.CONTRACT_NO
               p_dt_from           IN date,                 --between CONTRACT_T.DATE_FROM  &  DATE_TO  -- peresetsky
               p_dt_to               IN date,
               p_contract_id       in integer,
               p_account_id       in integer
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Contract_info';
    v_retcode    INTEGER;
    v_dt_from    date;  
    v_dt_to        date;
    v_sd            date;
BEGIN
    v_sd := sysdate;
    v_dt_from := trunc(nvl(p_dt_from,v_sd-20000));
    v_dt_to     := trunc(nvl(v_dt_to,v_sd+20000))+1-1/(24*60*60);
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT distinct
                C.CONTRACT_ID, 
                C.CONTRACT_NO,
                C.DATE_FROM,
                C.DATE_TO 
                --A.ACCOUNT_ID,
                --A.ACCOUNT_NO, 
                --CU.CUSTOMER_ID, 
                --CU.SHORT_NAME,
                --CT.CONTRACTOR_ID,                                           --- КТТК
                --CT.CONTRACTOR, 
                --BR.CONTRACTOR_ID      BR_CONTRACTOR_ID,      -- xTTK
                --BR.CONTRACTOR           BR_CONTRACTOR,
                --AG.CONTRACTOR_ID      AG_CONTRACTOR_ID,     --Агент
                --AG.CONTRACTOR           AG_CONTRACTOR
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, CUSTOMER_T CU,
               CONTRACTOR_T CT, CONTRACTOR_T BR, CONTRACTOR_T AG
         WHERE 
                 (p_contract_no is null or C.CONTRACT_NO  = p_contract_no)
           AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND AP.CUSTOMER_ID = CU.CUSTOMER_ID
           AND AP.CONTRACTOR_ID = CT.CONTRACTOR_ID
           AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
           AND AP.AGENT_ID  = AG.CONTRACTOR_ID(+)
           AND (p_contract_id is null or p_contract_id = C.CONTRACT_ID)  -- peresetsky
           AND (p_account_id is null or p_account_id = A.ACCOUNT_ID)    -- peresetsky
           AND (
                         v_dt_from between nvl(C.DATE_FROM,v_sd-20000) and nvl(C.DATE_TO,v_sd+20000 )
                    or  v_dt_to     between nvl(C.DATE_FROM,v_sd-20000) and nvl(C.DATE_TO,v_sd+20000 )
                    or  nvl(C.DATE_FROM,v_sd-20000) between v_dt_from and v_dt_to
                    or  nvl(C.DATE_TO,v_sd+20000 )   between v_dt_from and v_dt_to  
                  )
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

PROCEDURE Contract_info (
               p_recordset  OUT t_refc 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Contract_info';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT distinct
                C.CONTRACT_ID, 
                C.CONTRACT_NO,
                C.DATE_FROM,
                C.DATE_TO, 
                A.ACCOUNT_ID,
                A.ACCOUNT_NO, 
                CU.CUSTOMER_ID, 
                CU.SHORT_NAME,
                CT.CONTRACTOR_ID,                                           --- КТТК
                CT.CONTRACTOR, 
                BR.CONTRACTOR_ID      BR_CONTRACTOR_ID,      -- xTTK
                BR.CONTRACTOR           BR_CONTRACTOR,
                AG.CONTRACTOR_ID      AG_CONTRACTOR_ID,     --Агент
                AG.CONTRACTOR           AG_CONTRACTOR
          FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, CUSTOMER_T CU,
               CONTRACTOR_T CT, CONTRACTOR_T BR, CONTRACTOR_T AG, ORDER_T O
         WHERE 
                  AP.ACCOUNT_ID  = A.ACCOUNT_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND AP.CUSTOMER_ID = CU.CUSTOMER_ID
           AND AP.CONTRACTOR_ID = CT.CONTRACTOR_ID
           AND O.ACCOUNT_ID = A.ACCOUNT_ID
           AND O.SERVICE_ID = 7
           AND AP.BRANCH_ID = BR.CONTRACTOR_ID(+)
           AND AP.AGENT_ID  = AG.CONTRACTOR_ID(+)
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- список заказов на л/с
PROCEDURE Order_list (
               p_recordset  OUT t_refc, 
               p_account_id IN INTEGER,       -- ACCOUNT_T.ACCOUNT_ID
               p_order_no    in varchar2,       --ORDER_T.ORDER_NO   --peresetsky
               p_order_date in date,              -- between ORDER_T.DATE_FROM & DATE_TO   --peresetsky
               p_order_id    in  integer
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Order_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT 
               O.ORDER_ID, 
               O.ORDER_NO, 
               O.ACCOUNT_ID,
               O.DATE_FROM, 
               O.DATE_TO, 
               O.NOTES,
               O.SERVICE_ID, 
               S.SERVICE, 
               O.RATEPLAN_ID, 
               R.RATEPLAN_NAME, 
               R.RATESYSTEM_ID, 
               R.NOTE,
               R.RATEPLAN_CODE
          FROM ORDER_T O, RATEPLAN_T R, SERVICE_T S
         WHERE 
                  O.SERVICE_ID = c_SERVICE_ID
           AND O.SERVICE_ID = S.SERVICE_ID
           AND O.RATEPLAN_ID= R.RATEPLAN_ID(+)   --O.RATEPLAN_ID может быть null  --peresetsky
           AND (p_account_id is null or O.ACCOUNT_ID = p_account_id)               --peresetsky
           AND (p_order_no is null or O.ORDER_NO like p_order_no)                    --peresetsky
           AND (p_order_date is null 
                            or p_order_date between 
                                    nvl(O.DATE_FROM,sysdate-20000) and nvl(O.DATE_TO,sysdate+20000) )   --peresetsky
           and (p_order_id is null or p_order_id = O.ORDER_ID)
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

PROCEDURE Order_list (
               p_recordset  OUT t_refc, 
               p_account_id IN INTEGER,       -- ACCOUNT_T.ACCOUNT_ID
               p_order_no    in varchar2,       --ORDER_T.ORDER_NO   --peresetsky
               p_dt_from      in date,              -- between ORDER_T.DATE_FROM & DATE_TO   --peresetsky
               p_dt_to          IN date,
               p_order_id    in  integer
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Order_list';
    v_retcode    INTEGER;
    v_dt_from    date;  
    v_dt_to        date;
    v_sd            date;
BEGIN
    v_sd := sysdate;
    v_dt_from := trunc(nvl(p_dt_from,v_sd-20000));
    v_dt_to     := trunc(nvl(v_dt_to,v_sd+20000))+1-1/(24*60*60);
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT 
               O.ORDER_ID, 
               O.ORDER_NO, 
               O.ACCOUNT_ID,
               O.DATE_FROM, 
               O.DATE_TO, 
               O.NOTES,
               O.SERVICE_ID, 
               S.SERVICE, 
               O.RATEPLAN_ID, 
               R.RATEPLAN_NAME, 
               R.RATESYSTEM_ID, 
               R.NOTE,
               R.RATEPLAN_CODE
          FROM ORDER_T O, RATEPLAN_T R, SERVICE_T S
         WHERE 
                  O.SERVICE_ID = c_SERVICE_ID
           AND O.SERVICE_ID = S.SERVICE_ID
           AND O.RATEPLAN_ID= R.RATEPLAN_ID(+)   --O.RATEPLAN_ID может быть null  --peresetsky
           AND (p_account_id is null or O.ACCOUNT_ID = p_account_id)               --peresetsky
           AND (p_order_no is null or O.ORDER_NO like p_order_no)                    --peresetsky
           and (p_order_id is null or p_order_id = O.ORDER_ID)
           AND (
                         v_dt_from between nvl(O.DATE_FROM,v_sd-20000) and nvl(O.DATE_TO,v_sd+20000 )
                    or  v_dt_to     between nvl(O.DATE_FROM,v_sd-20000) and nvl(O.DATE_TO,v_sd+20000 )
                    or  nvl(O.DATE_FROM,v_sd-20000) between v_dt_from and v_dt_to
                    or  nvl(O.DATE_TO,v_sd+20000 )   between v_dt_from and v_dt_to  
                  )
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- список компонентов услуг на заказе
PROCEDURE Order_body (
               p_recordset  OUT t_refc, 
               p_order_id   IN INTEGER       -- ORDER_T.ORDER_ID
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Order_body';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT 
                OB.ORDER_BODY_ID,
                OB.SUBSERVICE_ID, 
                SS.SUBSERVICE_KEY, 
                SS.SUBSERVICE, 
                OB.CHARGE_TYPE,             --ТИП НАЧИСЛЕНИЯ 
                OB.DATE_FROM, 
                OB.DATE_TO 
          FROM ORDER_BODY_T OB, SUBSERVICE_T SS
         WHERE OB.SUBSERVICE_ID = SS.SUBSERVICE_ID
           AND OB.ORDER_ID = p_order_id
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- коммутаторы оператора  

PROCEDURE OpSwitch_list (
               p_recordset      out t_refc, 
               p_contract_id    in integer,
               p_op_sw_id       in integer 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OpSwitch_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT 
                OP_SW_ID, 
                CONTRACT_ID, 
                OP_SW_CODE, 
                OP_SW_NOTE,
                OP_SW_NAME
          FROM X07_OP_SWITCH_T D
         WHERE 
                 (p_contract_id is null or CONTRACT_ID  = p_contract_id )
            and  (p_op_sw_id is null or OP_SW_ID   = p_op_sw_id )
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

function OpSwitch_Add(
               p_op_sw_id        in integer,
               p_contract_id     in integer, 
               p_op_sw_code      in varchar2, 
               p_note            in varchar2,
               p_name            in varchar2
           ) return number  --p_rateplan_id;
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OpSwitch_Add';
    v_retcode    INTEGER;
    
    v_op_sw_id integer;
BEGIN
 
    v_op_sw_id := p_op_sw_id;
    if (v_op_sw_id is null) then select SQ_op_sw_id.nextval into v_op_sw_id from dual; end if;   

    INSERT INTO X07_OP_SWITCH_T d 
        (OP_SW_ID, CONTRACT_ID, OP_SW_CODE, OP_SW_NOTE,OP_SW_NAME)
    VALUES 
        (v_op_sw_id, p_contract_id, p_op_sw_code, p_note, p_name);
    
    gv_message:='{'||v_op_sw_id||'}{'||p_contract_id||'}{'||p_op_sw_code||'}{'||p_note||'}{'||p_name||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

    return v_op_sw_id; 

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

procedure OpSwitch_Update(
               p_op_sw_id        in integer,
               p_contract_id     in integer, 
               p_op_sw_code      in varchar2, 
               p_note            in varchar2,
               p_name            in varchar2
           ) 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OpSwitch_Update';
    v_retcode    INTEGER;
    v_p  X07_OP_SWITCH_T%rowtype;
BEGIN
 
     begin
        select * into v_p from  X07_OP_SWITCH_T where OP_SW_ID = p_OP_SW_ID;
        exception when no_data_found then
              raise_application_error(-20001,'Не найден id='||p_OP_SW_ID);
     end; 
    
    update X07_OP_SWITCH_T d
    set
        OP_SW_CODE      = p_OP_SW_CODE,  
        OP_SW_NOTE      = p_NOTE,
        OP_SW_NAME      = p_name
    where 
        OP_SW_ID = p_OP_SW_ID
    ; 
 
    gv_message:='{'||v_p.OP_SW_ID||'}{'||v_p.OP_SW_CODE||'}{'||v_p.OP_SW_NOTE||'}{'||v_p.OP_SW_NAME||'}'     
                   ||'->{'||p_OP_SW_ID||'}{'||p_OP_SW_CODE||'}{'||p_note||'}{'||p_name||'}';
 
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

procedure OpSwitch_Delete(
               p_op_sw_id        in integer 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OpSwitch_Delete';
    v_retcode    INTEGER;
    v_p  X07_OP_SWITCH_T%rowtype;
BEGIN
 
     begin
        select * into v_p from  X07_OP_SWITCH_T where OP_SW_ID  = p_op_sw_id;
        exception when no_data_found then
              raise_application_error(-20001,'Не найден id='||p_op_sw_id);
     end; 
    
    delete X07_OP_SWITCH_T  d
    where 
        OP_SW_ID  = p_op_sw_id
    ; 
 
    gv_message:='{'||v_p.OP_SW_ID ||'}{'||v_p.OP_SW_CODE ||'}{'||v_p.OP_SW_NOTE ||'}{'||v_p.OP_SW_NAME||'}';    

    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- список транковых групп коммутаторов на заказе
PROCEDURE Order_TG_list (
               p_recordset  OUT t_refc, 
               p_order_id   IN INTEGER       -- ORDER_T.ORDER_ID
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Order_TG_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT  
               TG.ORDER_SWTG_ID,  
               TG.ORDER_ID,
               TG.SWITCH_ID, 
               SW.SWITCH_NAME,    
               TG.TRUNKGROUP, 
               TG.TRUNKGROUP_NO,
               TG.OP_SW_ID,
               OSW.OP_SW_CODE,
               TG.DATE_FROM, 
               TG.DATE_TO 
          FROM X07_ORDER_SWTG_T TG, SWITCH_T SW, X07_OP_SWITCH_T OSW
         WHERE TG.ORDER_ID = p_order_id
           AND TG.SWITCH_ID = SW.SWITCH_ID
           AND TG.OP_SW_ID = OSW.OP_SW_ID
         order by SW.SWITCH_NAME,TG.TRUNKGROUP,TG.TRUNKGROUP_NO 
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

function Order_TG_Add(
               p_id                 IN INTEGER,
               p_order_id           IN INTEGER, 
               p_switch_id          IN INTEGER, 
               p_trunkgroup         IN VARCHAR2, 
               p_trunkgroup_no      IN INTEGER, 
               p_op_switch_id       IN INTEGER,
               p_date_from          IN DATE, 
               p_date_to            IN DATE
           ) return integer  --id
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Order_TG_Add';
    v_retcode    INTEGER;
    
    v_date_from          DATE; 
    v_date_to              DATE;
    
    v_id     INTEGER;
    
    v_swtg_row X07_ORDER_SWTG_T%rowtype;
    v_sw_name   varchar2(200);
    v_order    ORDER_T%rowtype;
    v_message   varchar2(2000);   

BEGIN
    v_id := p_id;
    if (v_id is null) then select SQ_ORDER_SWTG_ID.nextval into v_id from dual; end if;   

    v_date_from := trunc(p_date_from);
    v_date_to     := trunc(p_date_to)+1-1/(24*60*60);

       -- TG не может использоваться дважды в одно и то же время
       begin
           select SWITCH_NAME into v_sw_name from SWITCH_T where SWITCH_ID = p_switch_id;
           
           select * into v_swtg_row from X07_ORDER_SWTG_T
           where 
                SWITCH_ID  =  p_switch_id
                and
                upper(nvl(TRUNKGROUP,'*')) = upper(nvl(p_trunkgroup,'*'))
                and
                nvl(TRUNKGROUP_NO,-1) = nvl(p_trunkgroup_no,-1)
                --and
                --RS01_ID != v_id
                and
                (
                    v_date_from between DATE_FROM and DATE_TO
                    or
                    v_date_to between DATE_FROM and DATE_TO
                    or
                    DATE_FROM between v_DATE_FROM and v_DATE_TO
                    or
                    DATE_TO between v_DATE_FROM and v_DATE_TO
                );
                
                select * into v_order from ORDER_T where ORDER_ID = v_swtg_row.ORDER_ID;
                
                v_message := 'транк '||v_sw_name||':'||p_trunkgroup ||chr(13)
                            ||' используется в период '||date_range(v_swtg_row.date_from,v_swtg_row.date_to)||chr(13)
                            ||' для заказа '||v_order.ORDER_NO ||chr(13)
                            ;
                raise_application_error(-20001,v_message);
         exception when no_data_found then
             null;
       end;


    
    INSERT INTO X07_ORDER_SWTG_T d (ORDER_SWTG_ID, ORDER_ID, SWITCH_ID, TRUNKGROUP, TRUNKGROUP_NO, OP_SW_ID, DATE_FROM, DATE_TO)
    VALUES (v_id, p_order_id, p_switch_id, p_trunkgroup, p_trunkgroup_no, p_op_switch_id, v_date_from, v_date_to);
    
    gv_message:='{'||v_id||'}{'||p_order_id||'}{'||p_switch_id||'}{'||p_trunkgroup||'}{'||p_trunkgroup_no||'}{'||p_op_switch_id||'}{'||date_range(v_date_from,v_date_to)||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

    return v_id;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

PROCEDURE  Order_TG_Update ( 
               p_id                 IN INTEGER,
               p_order_id           IN INTEGER, 
               p_switch_id          IN INTEGER, 
               p_trunkgroup         IN VARCHAR2, 
               p_trunkgroup_no      IN INTEGER, 
               p_op_switch_id       IN INTEGER,
               p_date_from          IN DATE, 
               p_date_to            IN DATE
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := ' Order_TG_Update';
    v_retcode    INTEGER;
    v_pr X07_ORDER_SWTG_T%rowtype;
    v_date_from          DATE; 
    v_date_to              DATE;

    v_swtg_row X07_ORDER_SWTG_T%rowtype;
    v_sw_name   varchar2(200);
    v_order    ORDER_T%rowtype;
    v_message   varchar2(2000);   

BEGIN

    begin
            select * into v_pr from X07_ORDER_SWTG_T where  ORDER_SWTG_ID = p_id;
        exception when no_data_found then
             raise_application_error(-20001,'Не найден id='|| p_id);
    end;  


    v_date_from := trunc(p_date_from);
    v_date_to     := trunc(p_date_to)+1-1/(24*60*60);


       -- TG не может использоваться дважды в одно и то же время
       begin
           select SWITCH_NAME into v_sw_name from SWITCH_T where SWITCH_ID = p_switch_id;
           
                       
           select * into v_swtg_row from X07_ORDER_SWTG_T
           where 
                SWITCH_ID  =  p_switch_id
                and
                upper(nvl(TRUNKGROUP,'*')) = upper(nvl(p_trunkgroup,'*'))
                and
                nvl(TRUNKGROUP_NO,-1) = nvl(p_trunkgroup_no,-1)
                and
                ORDER_SWTG_ID != p_id
                and
                (
                    v_date_from between DATE_FROM and DATE_TO
                    or
                    v_date_to between DATE_FROM and DATE_TO
                    or
                    DATE_FROM between v_DATE_FROM and v_DATE_TO
                    or
                    DATE_TO between v_DATE_FROM and v_DATE_TO
                );
                
           
           select * into v_order from ORDER_T where ORDER_ID = v_swtg_row.ORDER_ID;
           
           v_message := 'транк '||v_sw_name||':'||p_trunkgroup ||chr(13)
                            ||' используется в период '||date_range(v_swtg_row.date_from,v_swtg_row.date_to)||chr(13)
                            ||' для заказа '||v_order.ORDER_NO ||chr(13)
                            ;
                raise_application_error(-20001,v_message);
         exception when no_data_found then
             null;
       end;



    update X07_ORDER_SWTG_T SS 
       set
            ORDER_ID                = p_order_id, 
            SWITCH_ID               = p_switch_id, 
            TRUNKGROUP              = p_trunkgroup, 
            TRUNKGROUP_NO           = p_trunkgroup_no,
            OP_SW_ID                = p_op_switch_id,
            DATE_FROM               = v_date_from, 
            DATE_TO                 = v_date_to
    where 
            ORDER_SWTG_ID = p_id
    ;
    
    gv_message:='{'||p_id||'}{'||v_pr.order_id||'}{'||v_pr.switch_id||'}{'||v_pr.trunkgroup||'}{'||v_pr.trunkgroup_no||'}{'||v_pr.op_sw_id||'}{'||date_range(v_pr.date_from,v_pr.date_to)||'}'
                ||'->{'||p_id||'}{'||p_order_id||'}{'||p_switch_id||'}{'||p_trunkgroup||'}{'||p_trunkgroup_no||'}{'||p_op_switch_id||'}{'||date_range(v_date_from,v_date_to)||'}';

    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;



PROCEDURE Order_TG_Delete(
               p_id                     IN INTEGER 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Order_TG_Delete';
    v_retcode    INTEGER;
    
    v_pr X07_ORDER_SWTG_T%rowtype;
BEGIN

    begin
            select * into v_pr from X07_ORDER_SWTG_T where  ORDER_SWTG_ID = p_id;
        exception when no_data_found then
             return;
    end;  

    delete X07_ORDER_SWTG_T d
    where
                ORDER_SWTG_ID = p_id
    ; 
    
    gv_message:='{'||p_id||'}{'||v_pr.order_swtg_id||'}{'||v_pr.order_id||'}{'||v_pr.switch_id||'}{'||v_pr.trunkgroup||'}{'||v_pr.trunkgroup_no||'}{'||v_pr.op_sw_id||'}{'||date_range(v_pr.date_from,v_pr.date_to)||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;



-- тарифы  заголовок RATEPLAN_T
PROCEDURE RatePlan_list (
               p_recordset  out t_refc, 
               p_rateplan_id in integer,
               p_rateplan_name in varchar2
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'RatePlan_list';
    v_retcode    INTEGER;
    v_rateplan_name varchar2(200);
BEGIN
    v_rateplan_name := get_pattern_like(p_rateplan_name);
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT 
                RATEPLAN_ID, 
                RATEPLAN_NAME, 
                --RATESYSTEM_ID, 
                NOTE  
                --, RATEPLAN_CODE
          FROM RATEPLAN_T D
         WHERE 
                    D.RATESYSTEM_ID = с_RATESYS_ID
            and  (p_rateplan_id is null or RATEPLAN_ID = p_rateplan_id )
            and  (v_rateplan_name is null or lower(RATEPLAN_NAME) like v_rateplan_name)
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

function RatePlan_Add(
               p_rateplan_id        in integer, 
               p_rateplan_name  in varchar2, 
               p_note                   in varchar2
           ) return number  --p_rateplan_id;
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'RatePlan_Add';
    v_retcode    INTEGER;
    
    v_rateplan_id integer;
BEGIN
 
    v_rateplan_id := p_rateplan_id;
    if (v_rateplan_id is null) then select SQ_RATEPLAN_ID.nextval into v_rateplan_id from dual; end if;   

    INSERT INTO RATEPLAN_T d 
        (RATEPLAN_ID, RATEPLAN_NAME, RATESYSTEM_ID, NOTE, RATEPLAN_CODE)
    VALUES 
        (v_rateplan_id, p_rateplan_name, с_RATESYS_ID, p_note, null);
    
    gv_message:='{'||v_rateplan_id||'}{'||p_rateplan_name||'}{'||с_RATESYS_ID||'}{'||p_note||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

    return v_rateplan_id; 

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

procedure RatePlan_Update(
               p_rateplan_id        in integer, 
               p_rateplan_name  in varchar2, 
               p_note                   in varchar2
           ) 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'RatePlan_Update';
    v_retcode    INTEGER;
    v_rateplan_id integer;
    v_p  RATEPLAN_T%rowtype;
BEGIN
 
    v_rateplan_id := p_rateplan_id;
    
     begin
        select * into v_p from  RATEPLAN_T where RATEPLAN_ID = p_rateplan_id;
        exception when no_data_found then
              raise_application_error(-20001,'Не найден тариф id='||p_rateplan_id);
     end; 
    
    update RATEPLAN_T d
    set
        RATEPLAN_NAME   = p_rateplan_name, 
        NOTE                    = p_note
    where 
        RATEPLAN_ID = p_rateplan_id
    ; 
 
    gv_message:='{'||v_p.rateplan_id||'}{'||v_p.rateplan_name||'}{'||v_p.note||'}'    
                   ||'->{'||p_rateplan_id||'}{'||p_rateplan_name||'}{'||p_note||'}';
 
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

procedure RatePlan_Delete(
               p_rateplan_id        in integer 
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'RatePlan_Delete';
    v_retcode    INTEGER;
    v_rateplan_id integer;
    v_p  RATEPLAN_T%rowtype;
BEGIN
 
    v_rateplan_id := p_rateplan_id;
    
     begin
        select * into v_p from  RATEPLAN_T where RATEPLAN_ID = p_rateplan_id;
        exception when no_data_found then
              raise_application_error(-20001,'Не найден тариф id='||p_rateplan_id);
     end; 
    
    delete RATEPLAN_T d
    where 
        RATEPLAN_ID = p_rateplan_id
    ; 
 
    gv_message:='{'||v_p.rateplan_id||'}{'||v_p.rateplan_name||'}{'||v_p.note||'}';    

    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;



-- тарифы  заголовок OP_RATE_PLAN   (определяет тип тарифа)
PROCEDURE OP_RatePlan_list (
               p_recordset          out t_refc, 
               p_op_rateplan_id     in integer,
               p_rateplan_id        in integer,
               p_op_rate_plan_type  in integer,
               p_tarif_type         in varchar2  -- D или R
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OP_RatePlan_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    if p_tarif_type is null then
        OPEN p_recordset FOR
            SELECT 
                    OP_RATE_PLAN_ID, 
                    RATEPLAN_ID, 
                    OP_RATE_PLAN_TYPE,
                    TARIF_VOL_TYPE, 
                    FLAG_GARANT_VOL
              FROM X07_OP_RATE_PLAN D
             WHERE 
                     (p_rateplan_id is null or RATEPLAN_ID = p_rateplan_id )
                and  (p_op_rateplan_id is null or OP_RATE_PLAN_ID = p_op_rateplan_id )     
                and  (p_op_rate_plan_type is null or OP_RATE_PLAN_TYPE = p_op_rate_plan_type )
            ;
    elsif p_tarif_type = 'D' then
        OPEN p_recordset FOR
            SELECT 
                    OP_RATE_PLAN_ID, 
                    RATEPLAN_ID, 
                    OP_RATE_PLAN_TYPE,
                    TARIF_VOL_TYPE, 
                    FLAG_GARANT_VOL
              FROM X07_OP_RATE_PLAN D
             WHERE 
                     (p_rateplan_id is null or RATEPLAN_ID = p_rateplan_id )
                and  (p_op_rateplan_id is null or OP_RATE_PLAN_ID = p_op_rateplan_id )     
                and  (p_op_rate_plan_type is null or OP_RATE_PLAN_TYPE = p_op_rate_plan_type )
                and  (OP_RATE_PLAN_TYPE in (1,2,5))
            ;
    elsif p_tarif_type = 'R' then
        OPEN p_recordset FOR
            SELECT 
                    OP_RATE_PLAN_ID, 
                    RATEPLAN_ID, 
                    OP_RATE_PLAN_TYPE,
                    TARIF_VOL_TYPE, 
                    FLAG_GARANT_VOL
              FROM X07_OP_RATE_PLAN D
             WHERE 
                     (p_rateplan_id is null or RATEPLAN_ID = p_rateplan_id )
                and  (p_op_rateplan_id is null or OP_RATE_PLAN_ID = p_op_rateplan_id )     
                and  (p_op_rate_plan_type is null or OP_RATE_PLAN_TYPE = p_op_rate_plan_type )
                and  (OP_RATE_PLAN_TYPE in (3,4,6))
            ;
    end if;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

function OP_RatePlan_Add(
               p_op_rateplan_id     in integer,
               p_rateplan_id        in integer,
               p_op_rate_plan_type  in integer,
               p_tarif_vol_type     in integer,
               p_flag_garant_vol    in integer
           ) return number  --p_op_rateplan_id;
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OP_RatePlan_Add';
    v_retcode    INTEGER;
    
    v_op_rateplan_id integer;
BEGIN
 
    v_op_rateplan_id := p_op_rateplan_id;
    if (v_op_rateplan_id is null) then select SQ_OP_RATE_PLAN_ID.nextval into v_op_rateplan_id from dual; end if;   

    INSERT INTO X07_OP_RATE_PLAN d 
        (OP_RATE_PLAN_ID, RATEPLAN_ID, OP_RATE_PLAN_TYPE, TARIF_VOL_TYPE, FLAG_GARANT_VOL)
    VALUES 
        (v_op_rateplan_id, p_rateplan_id, p_op_rate_plan_type, p_tarif_vol_type, p_flag_garant_vol);
    
    gv_message:='{'||v_op_rateplan_id||'}{'||p_rateplan_id||'}{'||p_op_rate_plan_type||'}{'||p_tarif_vol_type||'}{'||p_flag_garant_vol||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

    return v_op_rateplan_id; 

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

procedure OP_RatePlan_Update(
               p_op_rateplan_id     in integer,
               p_rateplan_id        in integer,
               p_op_rate_plan_type  in integer, 
               p_tarif_vol_type     in integer,
               p_flag_garant_vol    in integer
           ) 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OP_RatePlan_Update';
    v_retcode    INTEGER;
    v_op_rateplan_id integer;
    v_p  X07_OP_RATE_PLAN%rowtype;
BEGIN
 
    v_op_rateplan_id := p_op_rateplan_id;
    
     begin
        select * into v_p from  X07_OP_RATE_PLAN where OP_RATE_PLAN_ID  = p_op_rateplan_id;
        exception when no_data_found then
              raise_application_error(-20001,'Не найден тариф id='||p_op_rateplan_id);
     end; 
    
    update X07_OP_RATE_PLAN  d
    set
        OP_RATE_PLAN_TYPE   = p_op_rate_plan_type,
        RATEPLAN_ID         = p_rateplan_id,
        TARIF_VOL_TYPE      = p_tarif_vol_type, 
        FLAG_GARANT_VOL     = p_flag_garant_vol 
    where 
        OP_RATE_PLAN_ID = p_op_rateplan_id
    ; 
 
    gv_message:='{'||v_p.OP_RATE_PLAN_ID||'}{'||v_p.RATEPLAN_ID||'}{'||v_p.OP_RATE_PLAN_TYPE||'}{'||v_p.tarif_vol_type||'}{'||v_p.flag_garant_vol||'}'    
                   ||'->{'||p_op_rateplan_id||'}{'||p_rateplan_id||'}{'||p_op_rate_plan_type||'}{'||p_tarif_vol_type||'}{'||p_flag_garant_vol||'}';
 
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

procedure OP_RatePlan_Delete(
               p_op_rate_plan_id        in integer 
           )
IS
    v_prcName           CONSTANT VARCHAR2(30) := 'OP_RatePlan_Delete';
    v_retcode           INTEGER;
    v_op_rate_plan_id   integer;
    v_p                 X07_OP_RATE_PLAN%rowtype;
BEGIN
 
    v_op_rate_plan_id := p_op_rate_plan_id;
    
     begin
        select * into v_p from  X07_OP_RATE_PLAN where OP_RATE_PLAN_ID = p_op_rate_plan_id;
        exception when no_data_found then
              raise_application_error(-20001,'Не найден тариф id='||p_op_rate_plan_id);
     end; 
    
    
    -- TODO удалить дочерние записи
        
    delete X07_OP_RATE_PLAN d
    where 
        OP_RATE_PLAN_ID = p_op_rate_plan_id
    ; 
 
    gv_message:='{'||v_p.OP_RATE_PLAN_ID||'}{'||v_p.RATEPLAN_ID||'}{'||v_p.OP_RATE_PLAN_TYPE||'}{'||v_p.tarif_vol_type||'}{'||v_p.flag_garant_vol||'}';    

    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- доходный тариф
PROCEDURE OrderService_D_list (
               p_recordset          out t_refc,
               p_op_rateplan_id     in integer,
               p_sw_id              in integer,
               p_date               in date
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OrderService_D_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT 
                D.REC_ID, 
                D.OP_RATE_PLAN_ID, 
                D.PHONE_FROM, 
                D.PHONE_TO, 
                D.SWITCH_ID, 
                SW.SWITCH_CODE,
                D.SRV_ID, 
                D.DATE_FROM, 
                D.DATE_TO
          FROM X07_ORD_SERVICE_D_T D, SWITCH_T sw
         WHERE 
                 (p_op_rateplan_id is null or OP_RATE_PLAN_ID = p_op_rateplan_id )     
            and  (p_sw_id is null or nvl(D.SWITCH_ID,-1)  = nvl(p_sw_id,-1) ) 
            and  (p_date is null or p_date between D.DATE_FROM and D.DATE_TO )
            and  SW.SWITCH_ID(+) = D.SWITCH_ID    
        order by D.PHONE_FROM,D.PHONE_TO
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

function OrderService_D_Add(
                p_ID                in integer, 
                p_OP_RATE_PLAN_ID   in integer, 
                p_PHONE_FROM        in varchar2, 
                p_PHONE_TO          in varchar2, 
                p_SWITCH_ID         in varchar2, 
                p_SUBSERVICE_ID     in varchar2, 
                p_DATE_FROM         in date, 
                p_DATE_TO           in date
           ) return number  --p_ID;
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OrderService_D_Add';
    v_retcode    INTEGER;
    
    v_date_from     DATE; 
    v_date_to       DATE;
    v_id            integer;
BEGIN
 
    v_date_from   := trunc(p_date_from);
    v_date_to     := trunc(p_date_to)+1-1/(24*60*60);
 
    gv_message:=check_pool_phone_range_d(p_OP_RATE_PLAN_ID,p_SWITCH_ID, p_phone_from, p_phone_to, v_date_from, v_date_to,null);
    if (gv_message is not null) then
        raise_application_error(-20001,gv_message);
    end if;


    v_id := p_id;
    if (v_id is null) then select SQ_ORD_SERVICE_D_T_ID.nextval into v_id from dual; end if;   

    INSERT INTO X07_ORD_SERVICE_D_T d 
        (REC_ID, OP_RATE_PLAN_ID, PHONE_FROM, PHONE_TO, SWITCH_ID, SRV_ID, DATE_FROM, DATE_TO)
    VALUES 
        (v_id, p_OP_RATE_PLAN_ID, p_PHONE_FROM, p_PHONE_TO, p_SWITCH_ID, p_SUBSERVICE_ID, p_DATE_FROM, p_DATE_TO);
    
    gv_message:='{'||v_id||'}{'||p_OP_RATE_PLAN_ID||'}{'||p_phone_from||'-'||p_phone_to||'}{'||p_SWITCH_ID||'}{'||p_SUBSERVICE_ID||'}{'||date_range(v_date_from,v_date_to)||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

    return v_id; 

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

procedure OrderService_D_Update (
                p_ID                in integer, 
                p_OP_RATE_PLAN_ID   in integer, 
                p_PHONE_FROM        in varchar2, 
                p_PHONE_TO          in varchar2, 
                p_SWITCH_ID         in varchar2, 
                p_SUBSERVICE_ID     in varchar2, 
                p_DATE_FROM         in date, 
                p_DATE_TO           in date
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OrderService_D_Update';
    v_retcode    INTEGER;
    
    v_date_from     DATE; 
    v_date_to       DATE;
    v_p             X07_ORD_SERVICE_D_T%rowtype;
BEGIN
 
    v_date_from   := trunc(p_date_from);
    v_date_to     := trunc(p_date_to)+1-1/(24*60*60);

    begin
       select * into v_p from  X07_ORD_SERVICE_D_T where REC_ID  = p_ID;
       exception when no_data_found then
             raise_application_error(-20001,'Не найден id='||p_ID);
    end; 

    gv_message:=check_pool_phone_range_d(p_OP_RATE_PLAN_ID,p_SWITCH_ID, p_phone_from, p_phone_to, v_date_from, v_date_to,p_id);
    if (gv_message is not null) then
        raise_application_error(-20001,gv_message);
    end if;

    update X07_ORD_SERVICE_D_T
    set
        OP_RATE_PLAN_ID = p_OP_RATE_PLAN_ID, 
        PHONE_FROM      = p_PHONE_FROM, 
        PHONE_TO        = p_PHONE_TO, 
        SWITCH_ID       = p_SWITCH_ID, 
        SRV_ID          = p_SUBSERVICE_ID, 
        DATE_FROM       = v_date_from, 
        DATE_TO         = v_date_to
    where 
        REC_ID = p_id;
    
    gv_message:='{'||v_p.REC_ID||'}{'||v_p.OP_RATE_PLAN_ID||'}{'||v_p.phone_from||'-'||v_p.phone_to||'}{'||v_p.SWITCH_ID||'}{'||v_p.SRV_ID||'}{'||date_range(v_p.date_from,v_p.date_to)||'}'
            ||'->{'||p_id||'}{'||p_OP_RATE_PLAN_ID||'}{'||p_phone_from||'-'||p_phone_to||'}{'||p_SWITCH_ID||'}{'||p_SUBSERVICE_ID||'}{'||date_range(v_date_from,v_date_to)||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;




procedure OrderService_D_Delete(
               p_id        in integer 
           )
IS
    v_prcName           CONSTANT VARCHAR2(30) := 'OrderService_D_Delete';
    v_retcode           INTEGER;
    v_p                 X07_ORD_SERVICE_D_T%rowtype;
BEGIN
 
     begin
        select * into v_p from X07_ORD_SERVICE_D_T where REC_ID = p_id;
        exception when no_data_found then
              raise_application_error(-20001,'Не найден id='||p_id);
     end; 
    
    
    delete X07_ORD_SERVICE_D_T  d
    where 
        REC_ID = p_id
    ; 
 
    gv_message:='{'||v_p.REC_ID||'}{'||v_p.OP_RATE_PLAN_ID||'}{'||v_p.phone_from||'-'||v_p.phone_to||'}{'||v_p.SWITCH_ID||'}{'||v_p.SRV_ID||'}{'||date_range(v_p.date_from,v_p.date_to)||'}';    

    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- расходный тариф
PROCEDURE OrderService_R_list (
               p_recordset          out t_refc,
               p_op_rateplan_id     in integer,
               p_sw_id              in integer,
               p_date               in date
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OrderService_R_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT 
                D.REC_ID, 
                D.OP_RATE_PLAN_ID, 
                D.PHONE_FROM, 
                D.PHONE_TO, 
                D.OP_SW_ID, 
                SW.OP_SW_CODE,
                D.SRV_ID, 
                D.DATE_FROM, 
                D.DATE_TO
        FROM X07_ORD_SERVICE_R_T D, X07_OP_SWITCH_T sw
        WHERE 
                 (p_op_rateplan_id is null or OP_RATE_PLAN_ID = p_op_rateplan_id )     
            and  (p_sw_id is null or nvl(D.OP_SW_ID,-1)  = nvl(p_sw_id,-1) ) 
            and  (p_date is null or p_date between D.DATE_FROM and D.DATE_TO )
            and  SW.OP_SW_ID(+) = D.OP_SW_ID   
        order by D.PHONE_FROM,D.PHONE_TO 
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

function OrderService_R_Add(
                p_ID                in integer, 
                p_OP_RATE_PLAN_ID   in integer, 
                p_PHONE_FROM        in varchar2, 
                p_PHONE_TO          in varchar2, 
                p_OP_SW_ID          in varchar2, 
                p_SUBSERVICE_ID     in varchar2, 
                p_DATE_FROM         in date, 
                p_DATE_TO           in date
           ) return number  --p_ID;
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OrderService_R_Add';
    v_retcode    INTEGER;
    
    v_date_from     DATE; 
    v_date_to       DATE;
    v_id            integer;
BEGIN
 
    v_date_from   := trunc(p_date_from);
    v_date_to     := trunc(p_date_to)+1-1/(24*60*60);

    gv_message:=check_pool_phone_range_r(p_OP_RATE_PLAN_ID,p_OP_SW_ID, p_phone_from, p_phone_to, v_date_from, v_date_to,null);
    if (gv_message is not null) then
        raise_application_error(-20001,gv_message);
    end if;


    v_id := p_id;
    if (v_id is null) then select SQ_ORD_SERVICE_R_T_ID.nextval into v_id from dual; end if;   

    INSERT INTO X07_ORD_SERVICE_R_T d 
        (REC_ID, OP_RATE_PLAN_ID, PHONE_FROM, PHONE_TO, OP_SW_ID, SRV_ID, DATE_FROM, DATE_TO)
    VALUES 
        (v_id, p_OP_RATE_PLAN_ID, p_PHONE_FROM, p_PHONE_TO, p_OP_SW_ID, p_SUBSERVICE_ID, p_DATE_FROM, p_DATE_TO);
    
    gv_message:='{'||v_id||'}{'||p_OP_RATE_PLAN_ID||'}{'||p_phone_from||'-'||p_phone_to||'}{'||p_OP_SW_ID||'}{'||p_SUBSERVICE_ID||'}{'||date_range(v_date_from,v_date_to)||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

    return v_id; 

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

procedure OrderService_R_Update (
                p_ID                in integer, 
                p_OP_RATE_PLAN_ID   in integer, 
                p_PHONE_FROM        in varchar2, 
                p_PHONE_TO          in varchar2, 
                p_OP_SW_ID         in varchar2, 
                p_SUBSERVICE_ID     in varchar2, 
                p_DATE_FROM         in date, 
                p_DATE_TO           in date
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OrderService_R_Update';
    v_retcode    INTEGER;
    
    v_date_from     DATE; 
    v_date_to       DATE;
    v_p             X07_ORD_SERVICE_R_T%rowtype;
BEGIN
 
    v_date_from   := trunc(p_date_from);
    v_date_to     := trunc(p_date_to)+1-1/(24*60*60);

    begin
       select * into v_p from  X07_ORD_SERVICE_R_T where REC_ID  = p_ID;
       exception when no_data_found then
             raise_application_error(-20001,'Не найден id='||p_ID);
    end; 

    gv_message:=check_pool_phone_range_r(p_OP_RATE_PLAN_ID,p_OP_SW_ID, p_phone_from, p_phone_to, v_date_from, v_date_to,p_id);
    if (gv_message is not null) then
        raise_application_error(-20001,gv_message);
    end if;


    update X07_ORD_SERVICE_R_T
    set
        OP_RATE_PLAN_ID = p_OP_RATE_PLAN_ID, 
        PHONE_FROM      = p_PHONE_FROM, 
        PHONE_TO        = p_PHONE_TO, 
        OP_SW_ID        = p_OP_SW_ID, 
        SRV_ID   = p_SUBSERVICE_ID, 
        DATE_FROM       = v_date_from, 
        DATE_TO         = v_date_to
    where 
        REC_ID = p_id;
    
    gv_message:='{'||v_p.REC_ID||'}{'||v_p.OP_RATE_PLAN_ID||'}{'||v_p.phone_from||'-'||v_p.phone_to||'}{'||v_p.OP_SW_ID||'}{'||v_p.SRV_ID||'}{'||date_range(v_p.date_from,v_p.date_to)||'}'
            ||'->{'||p_id||'}{'||p_OP_RATE_PLAN_ID||'}{'||p_phone_from||'-'||p_phone_to||'}{'||p_OP_SW_ID||'}{'||p_SUBSERVICE_ID||'}{'||date_range(v_date_from,v_date_to)||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;




procedure OrderService_R_Delete(
               p_id        in integer 
           )
IS
    v_prcName           CONSTANT VARCHAR2(30) := 'OrderService_R_Delete';
    v_retcode           INTEGER;
    v_p                 X07_ORD_SERVICE_R_T%rowtype;
BEGIN
 
     begin
        select * into v_p from X07_ORD_SERVICE_R_T where REC_ID = p_id;
        exception when no_data_found then
              raise_application_error(-20001,'Не найден id='||p_id);
     end; 
    
    
    delete X07_ORD_SERVICE_R_T  d
    where 
        REC_ID = p_id
    ; 
 
    gv_message:='{'||v_p.REC_ID||'}{'||v_p.OP_RATE_PLAN_ID||'}{'||v_p.phone_from||'-'||v_p.phone_to||'}{'||v_p.OP_SW_ID||'}{'||v_p.SRV_ID||'}{'||date_range(v_p.date_from,v_p.date_to)||'}';    

    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- цены для услуг X07_ORD_PRICE_T
PROCEDURE OrderPrice_list (
               p_recordset          out t_refc,
               p_op_rateplan_id     in integer,
               p_subservice_id      in integer,
               p_date               in date
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OrderPrice_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT
                D.REC_ID, 
                D.OP_RATE_PLAN_ID, 
                D.SUBSERVICE_ID, 
                D.PRICE, 
                D.DATE_FROM, 
                D.DATE_TO 
          FROM X07_ORD_PRICE_T D
         WHERE 
                 (p_op_rateplan_id is null or OP_RATE_PLAN_ID = p_op_rateplan_id )     
            and  (p_subservice_id is null or D.SUBSERVICE_ID   = p_subservice_id ) 
            and  (p_date is null or p_date between D.DATE_FROM and D.DATE_TO )
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

function OrderPrice_Add(
                p_ID                in integer, 
                p_OP_RATE_PLAN_ID   in integer, 
                p_SUBSERVICE_ID     in integer, 
                p_PRICE             in number, 
                p_DATE_FROM         in date, 
                p_DATE_TO           in date
           ) return number  --p_ID;
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OrderPrice_Add';
    v_retcode    INTEGER;
    
    v_date_from     DATE; 
    v_date_to       DATE;
    v_id            integer;
BEGIN
 
    v_date_from   := trunc(p_date_from);
    v_date_to     := trunc(p_date_to)+1-1/(24*60*60);

    gv_message:=check_op_rate(               
               p_op_rate_plan_id,
               p_subservice_id, 
               p_price, 
               p_date_from, 
               p_date_to,
               p_id  
    );
    if (gv_message is not null) then
        raise_application_error(-20001,gv_message);
    end if;


    v_id := p_id;
    if (v_id is null) then select SQ_ORD_PRICE_T_ID.nextval into v_id from dual; end if;   

    INSERT INTO X07_ORD_PRICE_T d 
        (REC_ID, OP_RATE_PLAN_ID, SUBSERVICE_ID, PRICE, DATE_FROM, DATE_TO)
    VALUES 
        (v_id, p_OP_RATE_PLAN_ID, p_SUBSERVICE_ID, p_PRICE, p_DATE_FROM, p_DATE_TO);
    
    gv_message:='{'||v_id||'}{'||p_OP_RATE_PLAN_ID||'}{'||p_SUBSERVICE_ID||'}{'||p_PRICE||'}{'||date_range(v_date_from,v_date_to)||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

    return v_id; 

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

procedure OrderPrice_Update (
                p_ID                in integer, 
                p_OP_RATE_PLAN_ID   in integer, 
                p_SUBSERVICE_ID     in integer, 
                p_PRICE             in number, 
                p_DATE_FROM         in date, 
                p_DATE_TO           in date
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OrderPrice_Update';
    v_retcode    INTEGER;
    
    v_date_from     DATE; 
    v_date_to       DATE;
    v_p             X07_ORD_PRICE_T%rowtype;
BEGIN
 
    v_date_from   := trunc(p_date_from);
    v_date_to     := trunc(p_date_to)+1-1/(24*60*60);

    begin
       select * into v_p from  X07_ORD_PRICE_T where REC_ID  = p_ID;
       exception when no_data_found then
             raise_application_error(-20001,'Не найден id='||p_ID);
    end; 

    gv_message:=check_op_rate(               
               p_op_rate_plan_id,
               p_subservice_id, 
               p_price, 
               p_date_from, 
               p_date_to,
               p_id  
    );
    if (gv_message is not null) then
        raise_application_error(-20001,gv_message);
    end if;


    update X07_ORD_PRICE_T
    set
        OP_RATE_PLAN_ID = p_OP_RATE_PLAN_ID, 
        SUBSERVICE_ID   = p_SUBSERVICE_ID, 
        PRICE           = p_PRICE, 
        DATE_FROM       = v_date_from, 
        DATE_TO         = v_date_to
    where 
        REC_ID = p_id;
    
    gv_message:='{'||v_p.REC_ID||'}{'||v_p.OP_RATE_PLAN_ID||'}{'||v_p.subservice_id||'}{'||v_p.PRICE||'}{'||date_range(v_p.date_from,v_p.date_to)||'}'
            ||'->{'||p_id||'}{'||p_OP_RATE_PLAN_ID||'}{'||p_subservice_id||'}{'||p_price||'}{'||date_range(v_date_from,v_date_to)||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;




procedure OrderPrice_Delete(
               p_id        in integer 
           )
IS
    v_prcName           CONSTANT VARCHAR2(30) := 'OrderPrice_Delete';
    v_retcode           INTEGER;
    v_p                 X07_ORD_PRICE_T%rowtype;
BEGIN
 
     begin
        select * into v_p from X07_ORD_PRICE_T where REC_ID = p_id;
        exception when no_data_found then
              raise_application_error(-20001,'Не найден id='||p_id);
     end; 
    
    
    delete X07_ORD_PRICE_T  d
    where 
        REC_ID = p_id
    ; 
 
    gv_message:='{'||v_p.REC_ID||'}{'||v_p.OP_RATE_PLAN_ID||'}{'||v_p.subservice_id||'}{'||v_p.PRICE ||'}{'||date_range(v_p.date_from,v_p.date_to)||'}';    

    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- цены для услуг по объему X07_ORD_PRICE_V_T
PROCEDURE OrderPriceV_list (
               p_recordset          out t_refc,
               p_op_rateplan_id     in integer,
               p_subservice_id      in integer,
               p_date               in date
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OrderPriceV_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT
                D.REC_ID, 
                D.OP_RATE_PLAN_ID, 
                D.SUBSERVICE_ID,
                D.VOL, 
                D.PRICE, 
                D.DATE_FROM, 
                D.DATE_TO 
          FROM X07_ORD_PRICE_V_T D
         WHERE 
                 (p_op_rateplan_id is null or OP_RATE_PLAN_ID = p_op_rateplan_id )     
            and  (p_subservice_id is null or D.SUBSERVICE_ID   = p_subservice_id ) 
            and  (p_date is null or p_date between D.DATE_FROM and D.DATE_TO )
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

function OrderPriceV_Add(
                p_ID                in integer, 
                p_OP_RATE_PLAN_ID   in integer, 
                p_SUBSERVICE_ID     in integer,
                p_VOL               in integer, 
                p_PRICE             in number, 
                p_DATE_FROM         in date, 
                p_DATE_TO           in date
           ) return number  --p_ID;
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OrderPriceV_Add';
    v_retcode    INTEGER;
    
    v_date_from     DATE; 
    v_date_to       DATE;
    v_id            integer;
BEGIN
 
    v_date_from   := trunc(p_date_from);
    v_date_to     := trunc(p_date_to)+1-1/(24*60*60);

--    gv_message:=check_op_rate_V(               
--               p_op_rate_plan_id,
--               p_subservice_id, 
--               p_vol,
--               p_price, 
--               p_date_from, 
--               p_date_to,
--               p_id  
--    );
--
--    if (gv_message is not null) then
--        raise_application_error(-20001,gv_message);
--    end if;


    v_id := p_id;
    if (v_id is null) then select SQ_ORD_PRICE_T_ID.nextval into v_id from dual; end if;   

    INSERT INTO X07_ORD_PRICE_V_T d 
        (REC_ID, OP_RATE_PLAN_ID, SUBSERVICE_ID, VOL, PRICE, DATE_FROM, DATE_TO)
    VALUES 
        (v_id, p_OP_RATE_PLAN_ID, p_SUBSERVICE_ID, p_vol,  p_PRICE, p_DATE_FROM, p_DATE_TO);
    
    gv_message:='{'||v_id||'}{'||p_OP_RATE_PLAN_ID||'}{'||p_SUBSERVICE_ID||'}{'||p_vol||'}{'||p_PRICE||'}{'||date_range(v_date_from,v_date_to)||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

    return v_id; 

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

procedure OrderPriceV_Update (
                p_ID                in integer, 
                p_OP_RATE_PLAN_ID   in integer, 
                p_SUBSERVICE_ID     in integer, 
                p_VOL               in integer, 
                p_PRICE             in number, 
                p_DATE_FROM         in date, 
                p_DATE_TO           in date
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'OrderPriceV_Update';
    v_retcode    INTEGER;
    
    v_date_from     DATE; 
    v_date_to       DATE;
    v_p             X07_ORD_PRICE_V_T%rowtype;
BEGIN
 
    v_date_from   := trunc(p_date_from);
    v_date_to     := trunc(p_date_to)+1-1/(24*60*60);

    begin
       select * into v_p from  X07_ORD_PRICE_V_T where REC_ID  = p_ID;
       exception when no_data_found then
             raise_application_error(-20001,'Не найден id='||p_ID);
    end; 

--    gv_message:=check_op_rate(               
--               p_op_rate_plan_id,
--               p_subservice_id, 
--               p_vol,
--               p_price, 
--               p_date_from, 
--               p_date_to,
--               p_id  
--    );
--    if (gv_message is not null) then
--        raise_application_error(-20001,gv_message);
--    end if;


    update X07_ORD_PRICE_V_T
    set
        OP_RATE_PLAN_ID = p_OP_RATE_PLAN_ID, 
        SUBSERVICE_ID   = p_SUBSERVICE_ID,
        VOL             = p_vol, 
        PRICE           = p_PRICE, 
        DATE_FROM       = v_date_from, 
        DATE_TO         = v_date_to
    where 
        REC_ID = p_id;
    
    gv_message:='{'||v_p.REC_ID||'}{'||v_p.OP_RATE_PLAN_ID||'}{'||v_p.subservice_id||'}{'||v_p.vol||'}{'||v_p.PRICE||'}{'||date_range(v_p.date_from,v_p.date_to)||'}'
            ||'->{'||p_id||'}{'||p_OP_RATE_PLAN_ID||'}{'||p_subservice_id||'}{'||p_vol||'}{'||p_price||'}{'||date_range(v_date_from,v_date_to)||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;




procedure OrderPriceV_Delete(
               p_id        in integer 
           )
IS
    v_prcName           CONSTANT VARCHAR2(30) := 'OrderPriceV_Delete';
    v_retcode           INTEGER;
    v_p                 X07_ORD_PRICE_V_T%rowtype;
BEGIN
 
     begin
        select * into v_p from X07_ORD_PRICE_V_T where REC_ID = p_id;
        exception when no_data_found then
              raise_application_error(-20001,'Не найден id='||p_id);
     end; 
    
    
    delete X07_ORD_PRICE_V_T  d
    where 
        REC_ID = p_id
    ; 
 
    gv_message:='{'||v_p.REC_ID||'}{'||v_p.OP_RATE_PLAN_ID||'}{'||v_p.subservice_id||'}{'||v_p.vol||'}{'||v_p.PRICE ||'}{'||date_range(v_p.date_from,v_p.date_to)||'}';    

    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- гаранированные объемы X07_ORD_GV_T
PROCEDURE GarantVolume_list (
               p_recordset          out t_refc,
               p_op_rateplan_id     in integer,
               p_subservice_id      in integer,
               p_date               in date
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'GarantVolume_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT
                D.REC_ID, 
                D.OP_RATE_PLAN_ID, 
                D.SUBSERVICE_ID, 
                D.SUMMA, 
                D.DATE_FROM, 
                D.DATE_TO 
          FROM X07_ORD_GV_T D
         WHERE 
                 (p_op_rateplan_id is null or OP_RATE_PLAN_ID = p_op_rateplan_id )     
            and  (p_subservice_id is null or D.SUBSERVICE_ID   = p_subservice_id ) 
            and  (p_date is null or p_date between D.DATE_FROM and D.DATE_TO )
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

function GarantVolume_Add(
                p_ID                in integer, 
                p_OP_RATE_PLAN_ID   in integer, 
                p_SUBSERVICE_ID     in integer, 
                p_SUMMA             in number, 
                p_DATE_FROM         in date, 
                p_DATE_TO           in date
           ) return number  --p_ID;
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'GarantVolume_Add';
    v_retcode    INTEGER;
    
    v_date_from     DATE; 
    v_date_to       DATE;
    v_id            integer;
BEGIN
 
    v_date_from   := trunc(p_date_from);
    v_date_to     := trunc(p_date_to)+1-1/(24*60*60);

    gv_message:=check_op_gv(               
               p_op_rate_plan_id,
               p_subservice_id, 
               p_summa, 
               p_date_from, 
               p_date_to,
               p_id  
    );
    if (gv_message is not null) then
        raise_application_error(-20001,gv_message);
    end if;


    v_id := p_id;
    if (v_id is null) then select SQ_ORD_PRICE_T_ID.nextval into v_id from dual; end if;   

    INSERT INTO X07_ORD_GV_T d 
        (REC_ID, OP_RATE_PLAN_ID, SUBSERVICE_ID, SUMMA, DATE_FROM, DATE_TO)
    VALUES 
        (v_id, p_OP_RATE_PLAN_ID, p_SUBSERVICE_ID, p_SUMMA, p_DATE_FROM, p_DATE_TO);
    
    gv_message:='{'||v_id||'}{'||p_OP_RATE_PLAN_ID||'}{'||p_SUBSERVICE_ID||'}{'||p_SUMMA||'}{'||date_range(v_date_from,v_date_to)||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

    return v_id; 

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

procedure GarantVolume_Update (
                p_ID                in integer, 
                p_OP_RATE_PLAN_ID   in integer, 
                p_SUBSERVICE_ID     in integer, 
                p_SUMMA             in number, 
                p_DATE_FROM         in date, 
                p_DATE_TO           in date
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'GarantVolume_Update';
    v_retcode    INTEGER;
    
    v_date_from     DATE; 
    v_date_to       DATE;
    v_p             X07_ORD_GV_T%rowtype;
BEGIN
 
    v_date_from   := trunc(p_date_from);
    v_date_to     := trunc(p_date_to)+1-1/(24*60*60);

    begin
       select * into v_p from  X07_ORD_GV_T where REC_ID  = p_ID;
       exception when no_data_found then
             raise_application_error(-20001,'Не найден id='||p_ID);
    end; 

    gv_message:=check_op_gv(               
               p_op_rate_plan_id,
               p_subservice_id, 
               p_summa, 
               p_date_from, 
               p_date_to,
               p_id  
    );
    if (gv_message is not null) then
        raise_application_error(-20001,gv_message);
    end if;


    update X07_ORD_GV_T
    set
        OP_RATE_PLAN_ID = p_OP_RATE_PLAN_ID, 
        SUBSERVICE_ID   = p_SUBSERVICE_ID, 
        SUMMA           = p_SUMMA, 
        DATE_FROM       = v_date_from, 
        DATE_TO         = v_date_to
    where 
        REC_ID = p_id;
    
    gv_message:='{'||v_p.REC_ID||'}{'||v_p.OP_RATE_PLAN_ID||'}{'||v_p.subservice_id||'}{'||v_p.SUMMA||'}{'||date_range(v_p.date_from,v_p.date_to)||'}'
            ||'->{'||p_id||'}{'||p_OP_RATE_PLAN_ID||'}{'||p_subservice_id||'}{'||p_summa||'}{'||date_range(v_date_from,v_date_to)||'}';    
    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;




procedure GarantVolume_Delete(
               p_id        in integer 
           )
IS
    v_prcName           CONSTANT VARCHAR2(30) := 'GarantVolume_Delete';
    v_retcode           INTEGER;
    v_p                 X07_ORD_GV_T%rowtype;
BEGIN
 
     begin
        select * into v_p from X07_ORD_GV_T where REC_ID = p_id;
        exception when no_data_found then
              raise_application_error(-20001,'Не найден id='||p_id);
     end; 
    
    
    delete X07_ORD_GV_T  d
    where 
        REC_ID = p_id
    ; 
 
    gv_message:='{'||v_p.REC_ID||'}{'||v_p.OP_RATE_PLAN_ID||'}{'||v_p.subservice_id||'}{'||v_p.SUMMA ||'}{'||date_range(v_p.date_from,v_p.date_to)||'}';    

    pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;





-- проверка пересечений диапазонов номеров в доходных тарифах
function check_pool_phone_range_D (
    p_op_rate_plan_id   in number,
    p_switch_id         in number, 
    p_phone_from        in varchar2, 
    p_phone_to          in varchar2,
    p_date_from         in date, 
    p_date_to           in date,
    p_id_exclude        in number -- эту запись не проверять. ее сейчас обновляем !
) return varchar2 -- error message
is
    v_mess    varchar2(32000) := '';
    vdn       date := sysdate+20000;
begin

    for rec in (
                select
                        d.PHONE_FROM,
                        d.PHONE_TO,
                        d.DATE_FROM,
                        d.DATE_TO,
                        d.SWITCH_ID,
                        SW.SWITCH_CODE
                from X07_ORD_SERVICE_D_T d, SWITCH_T sw
                where
                            d.SWITCH_ID         = sw.SWITCH_ID
                    and   d.OP_RATE_PLAN_ID     = p_op_rate_plan_id
                    and   nvl(D.SWITCH_ID,-1)   = nvl(p_switch_id,-1) 
                    and   (p_id_exclude is null or d.REC_ID != p_id_exclude)
                    and   (
                                       p_date_from      between  d.date_from and nvl(d.date_to,vdn)
                                or nvl(p_date_to,vdn)   between  d.date_from and nvl(d.date_to,vdn)
                                or     d.date_from      between  p_date_from and nvl(p_date_to,vdn)
                                or nvl(d.date_to,vdn)   between  p_date_from and nvl(p_date_to,vdn)
                            )
                    
                    and   (
                                (-- если диапазоны номеров совпадают
                                         p_phone_from   =  d.phone_from
                                    and  p_phone_to     =  d.phone_to
                                )
                                or
                                (-- если диапазоны номеров пересекаются (НЕ ВЛОЖЕНЫ)
                                         p_phone_from     <        d.phone_from
                                    and  p_phone_to       between  d.phone_from and d.phone_to
                                )
                                or
                                (-- если диапазоны номеров пересекаются (НЕ ВЛОЖЕНЫ)
                                           p_phone_to     >        d.phone_to
                                    and  p_phone_from     between  d.phone_from and d.phone_to
                                )
                            )
    ) loop
        
        v_mess := v_mess ||rec.PHONE_FROM || '-' || rec.PHONE_TO || ' период [' || date_range(rec.DATE_FROM,rec.DATE_TO) || '] КУ:'||rec.SWITCH_CODE || chr(10)||chr(13);
    
    end loop;
   
   if (v_mess is not null) then
       v_mess := 'Пересечение или совпадение диапазона номеров '|| p_PHONE_FROM || '-' || p_PHONE_TO  ||chr(10)||chr(13) 
                || 'c диапазонами: ' || chr(10)||chr(13) || v_mess;
   end if;
             
   return v_mess;
    
end;      

-- проверка пересечений диапазонов номеров в расходных тарифах
function check_pool_phone_range_R (
    p_op_rate_plan_id   in number,
    p_switch_id         in number, 
    p_phone_from        in varchar2, 
    p_phone_to          in varchar2,
    p_date_from         in date, 
    p_date_to           in date,
    p_id_exclude        in number -- эту запись не проверять. ее сейчас обновляем !
) return varchar2 -- error message
is
    v_mess    varchar2(32000) := '';
    vdn       date := sysdate+20000;
begin

    for rec in (
                select
                        d.PHONE_FROM,
                        d.PHONE_TO,
                        d.DATE_FROM,
                        d.DATE_TO,
                        d.OP_SW_ID,
                        SW.OP_SW_CODE
                from X07_ORD_SERVICE_R_T d, X07_OP_SWITCH_T sw
                where
                            d.OP_SW_ID          = sw.OP_SW_ID
                    and   d.OP_RATE_PLAN_ID     = p_op_rate_plan_id
                    and   nvl(D.OP_SW_ID,-1)    = nvl(p_switch_id,-1) 
                    and   (p_id_exclude is null or d.REC_ID != p_id_exclude)
                    and   (
                                       p_date_from      between  d.date_from and nvl(d.date_to,vdn)
                                or nvl(p_date_to,vdn)   between  d.date_from and nvl(d.date_to,vdn)
                                or     d.date_from      between  p_date_from and nvl(p_date_to,vdn)
                                or nvl(d.date_to,vdn)   between  p_date_from and nvl(p_date_to,vdn)
                            )
                    
                    and   (
                                (-- если диапазоны номеров совпадают
                                         p_phone_from   =  d.phone_from
                                    and  p_phone_to     =  d.phone_to
                                )
                                or
                                (-- если диапазоны номеров пересекаются (НЕ ВЛОЖЕНЫ)
                                         p_phone_from     <        d.phone_from
                                    and  p_phone_to       between  d.phone_from and d.phone_to
                                )
                                or
                                (-- если диапазоны номеров пересекаются (НЕ ВЛОЖЕНЫ)
                                           p_phone_to     >        d.phone_to
                                    and  p_phone_from     between  d.phone_from and d.phone_to
                                )
                            )
    ) loop
        
        v_mess := v_mess ||rec.PHONE_FROM || '-' || rec.PHONE_TO || ' период [' || date_range(rec.DATE_FROM,rec.DATE_TO) || '] КУ:'||rec.OP_SW_CODE || chr(10)||chr(13);
    
    end loop;
   
   if (v_mess is not null) then
       v_mess := 'Пересечение или совпадение диапазона номеров '|| p_PHONE_FROM || '-' || p_PHONE_TO  ||chr(10)||chr(13) 
                || 'c диапазонами: ' || chr(10)||chr(13) || v_mess;
   end if;
             
   return v_mess;
    
end;      

-- проверка пересечений цен в тарифе
function check_op_rate (
        p_op_rate_plan_id       in integer,
        p_subservice_id     in integer, 
        p_price                 in number, 
        p_date_from          in date, 
        p_date_to              in date,  
        p_rate_id             in number -- !=null => эту запись не проверять. ее сейчас обновляем !
) return varchar2 -- error message
is
    v_mess    varchar2(32000) := '';
    v_contract_id  number;
    vdn       date := sysdate+20000;
begin
    for rec in (
                select
                        op.SUBSERVICE_ID, 
                        op.PRICE, 
                        op.DATE_FROM, 
                        op.DATE_TO,
                        ss.SRV_KEY 
                from X07_ORD_PRICE_T op, X07_SRV_DCT ss
                where
                          op.OP_RATE_PLAN_ID  = p_op_rate_plan_id
                    and   op.SUBSERVICE_ID    = p_subservice_id
                    and   SS.SRV_ID           = OP.SUBSERVICE_ID
                    --    
                    and   (p_rate_id is null or op.REC_ID != p_rate_id)
                    and   (
                                       p_date_from      between  date_from      and nvl(date_to,vdn)
                                or nvl(p_date_to,vdn)   between  date_from      and nvl(date_to,vdn)
                                or     date_from        between  p_date_from    and nvl(p_date_to,vdn)
                                or nvl(date_to,vdn)     between  p_date_from    and nvl(p_date_to,vdn)
                            )
    ) loop
        
        v_mess := v_mess ||rec.SRV_KEY || ' период [' || date_range(rec.DATE_FROM,rec.DATE_TO) || '] стоимость:'||trim(to_char(rec.PRICE,'9999999.9999')) || chr(10)||chr(13);
    
    end loop;
   
   if (v_mess is not null) then
       v_mess := 'Неоднозначное определение стоимости услуги '|| chr(10)||chr(13) 
                || chr(10)||chr(13) || v_mess;
   end if;
             
   return v_mess;
    
end;      

-- проверка пересечений сумм в гарантированном объеме
function check_op_gv (
        p_op_rate_plan_id       in integer,
        p_subservice_id         in integer, 
        p_summa                 in number, 
        p_date_from             in date, 
        p_date_to               in date,  
        p_rate_id               in number -- !=null => эту запись не проверять. ее сейчас обновляем !
) return varchar2 -- error message
is
    v_mess    varchar2(32000) := '';
    v_contract_id  number;
    vdn       date := sysdate+20000;
begin
    for rec in (
                select
                        op.SUBSERVICE_ID, 
                        op.summa, 
                        op.DATE_FROM, 
                        op.DATE_TO,
                        ss.SRV_KEY 
                from X07_ORD_GV_T op, X07_SRV_DCT ss
                where
                          op.OP_RATE_PLAN_ID  = p_op_rate_plan_id
                    and   op.SUBSERVICE_ID    = p_subservice_id
                    and   SS.SRV_ID    = OP.SUBSERVICE_ID
                    --    
                    and   (p_rate_id is null or op.REC_ID != p_rate_id)
                    and   (
                                       p_date_from      between  date_from      and nvl(date_to,vdn)
                                or nvl(p_date_to,vdn)   between  date_from      and nvl(date_to,vdn)
                                or     date_from        between  p_date_from    and nvl(p_date_to,vdn)
                                or nvl(date_to,vdn)     between  p_date_from    and nvl(p_date_to,vdn)
                            )
    ) loop
        
        v_mess := v_mess ||rec.SRV_KEY || ' период [' || date_range(rec.DATE_FROM,rec.DATE_TO) || '] сумма:'||trim(to_char(rec.summa,'9999999.99')) || chr(10)||chr(13);
    
    end loop;
   
   if (v_mess is not null) then
       v_mess := 'Неоднозначное определение суммы '|| chr(10)||chr(13) 
                || chr(10)||chr(13) || v_mess;
   end if;
             
   return v_mess;
    
end;      



END PK104_X2_TOPS;
/
