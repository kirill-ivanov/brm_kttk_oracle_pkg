CREATE OR REPLACE PACKAGE PK39_BILLING_DISCOUNT_GUI
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK39_BILLING_DISCOUNT_GUI';
    -- ==============================================================================
    type t_refc is ref cursor;
    -- Расчет групповых скидок, рачсет ведется после выставления счетов:
    -- Данные для расчетов в таблицах:
    -- discount_group_t, dsc_grp_contract_t, dsc_grp_percent_t, dsc_grp_service_t
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Ключ в словаре описывающий 
    c_KEY_DISCOUNT_RULE CONSTANT INTEGER := PK00_CONST.k_DICT_KEY_DISCOUNT_RULE;

    c_DISC_STD CONSTANT INTEGER := 2501; --'Стандартный расчет скидки по таблице DG_PERCENT_T'
    c_DISC_MTC CONSTANT INTEGER := 2502; --'Расчет скидки для МТС'
    c_DISC_BEE CONSTANT INTEGER := 2503; --'Расчет скидки для Билайн', NULL
    c_DISC_BEE CONSTANT INTEGER := 2504; --'Расчет скидки для Мегафона', NULL

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создать группу скидок
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Create_group (
                p_group_name    IN VARCHAR2,
                p_rule_id       IN INTEGER,
                p_date_from     IN DATE,
                p_notes         IN VARCHAR2
             ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Закрыть группу скидок
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Close_group (
                p_group_id IN INTEGER,
                p_date_to  IN DATE
             );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- добавить лицевой счет в группу
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Add_account(
                p_group_id   IN INTEGER,
                p_account_id IN INTEGER,
                p_date_from  IN DATE
             ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- добавить заказ в группу
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Add_order(
                p_dg_account_id  IN INTEGER,
                p_order_id       IN INTEGER,
                p_date_from      IN DATE
             ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Редактировать таблицу процентов скидок
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- получить номер строки
    FUNCTION Get_next_prc_no(
                p_group_id  IN INTEGER
             ) RETURN INTEGER;

    -- Добавить величину
    PROCEDURE Add_percent(
                p_group_id  IN NUMBER,
                p_row_no    IN NUMBER,
                p_value_min IN NUMBER,
                p_value_max IN NUMBER,
                p_percent   IN NUMBER
             );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Список лицевых счетов в биллинге
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Аccount_list( 
                   p_recordset    OUT t_refc, 
                   p_account_no    IN VARCHAR2 -- первые символы номера договора
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Список заказов на лицевых счетах в биллинге
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    function Order_list( 
               p_account_no   	IN VARCHAR2,
               p_order_no     	IN VARCHAR2,
			   p_columns		out varchar2
           ) return t_refc;


    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Список заказов c канальными услугами на лицевых счетах в биллинге
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Order_channel_list( 
                   p_recordset  OUT t_refc, 
                   p_order_no    IN VARCHAR2 -- первые символы номера заказа
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Информация для выборки, пока к сведению
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Info_list( 
                   p_recordset    OUT t_refc
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Процедура создания компонента услуги идентифицирующего скидку
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Add_discount_to_order (
                   p_order_id  IN INTEGER,
                   p_date_from IN DATE
               );
               
    -- удалить заказ из группы скидок
    PROCEDURE Remove_discount_from_order (
                   p_order_id  IN INTEGER,
                   p_date_to IN DATE
               );
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
	-- Список групп
	-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    function Group_List(
	p_active			in number,
	p_ls_no				in varchar2,
	p_order_no			in varchar2,
	p_service			in varchar2,
	p_speed				in number,
	p_speed_unit_id		in number,
	p_columns			out varchar2
	) return t_refc;

		
	/* ******************************************************
	* список процентов по группе
	*/
	function Group_Percent_List(
		p_dg_id		in number,
		p_columns	out varchar2
		) return t_refc;

	/* ******************************************************
	* список лицевых счетов по группе
	*/
	function Group_Account_List(
		p_dg_id		in number,
		p_columns	out varchar2
		) return t_refc;

		
	/* *************************************************************
	* список л/с и заказов в группе
	**/
	procedure Account_Order_List(
		p_dg_id		in number,
		p_ret		out t_refc
		);

	/* ******************************************************************
	* получить данные группы
	*/
	procedure Get_Group_Data(
		p_dg_id		in number,
		p_dg_name	out varchar2,
		p_date_from	out varchar2,
		p_date_to	out varchar2,
		p_dt_id		out number,
		p_dt_name	out varchar2,
		p_note		out varchar2
		);

END PK39_BILLING_DISCOUNT_GUI;
/
CREATE OR REPLACE PACKAGE BODY PK39_BILLING_DISCOUNT_GUI
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Создать группу скидок
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Create_group (
            p_group_name    IN VARCHAR2,
            p_rule_id       IN INTEGER,
            p_date_from     IN DATE,
            p_notes         IN VARCHAR2
         ) RETURN INTEGER
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Create_group';
    v_group_id  INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, group_name = '||p_group_name, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- получить ID - для группы скидок
    SELECT MAX(DG_ID) INTO v_group_id
      FROM DISCOUNT_GROUP_T;
    RETURN v_group_id;
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    INSERT INTO DISCOUNT_GROUP_T (
        DG_ID, DG_NAME, DG_RULE_ID, DATE_FROM, DATE_TO, NOTES
    )VALUES(
        v_group_id, p_group_name, p_rule_id, p_date_from, NULL, p_notes
    );
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    Pk01_Syslog.Write_msg('Stop, group_id = '||v_group_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    RETURN v_group_id;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Закрыть группу скидок
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Close_group (
            p_group_id IN INTEGER,
            p_date_to  IN DATE
         )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Close_group';
BEGIN
    UPDATE DISCOUNT_GROUP_T SET DATE_TO = p_date_to
     WHERE DG_ID = p_group_id;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR. Group_id='||p_group_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- добавить лицевой счет в группу
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Add_account(
            p_group_id   IN INTEGER,
            p_account_id IN INTEGER,
            p_date_from  IN DATE
         ) RETURN INTEGER
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Add_account';
    v_dg_account_id INTEGER;
BEGIN
    -- получаем номер строки
    v_dg_account_id := SQ_DG_ID.NEXTVAL;
    -- добавляем строку
    INSERT INTO DG_ACCOUNT_T (DG_ID, ACCOUNT_ID, DATE_FROM)
    VALUES (p_group_id, p_account_id, p_date_from);  
    -- возвращаем номер строки
    RETURN v_dg_account_id;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR. Group_id='||p_group_id||', account_id'||p_account_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- добавить заказ в группу
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Add_order(
            p_dg_account_id  IN INTEGER,
            p_order_id       IN INTEGER,
            p_date_from      IN DATE
         ) RETURN INTEGER
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Add_order';
    v_dg_order_id INTEGER;
BEGIN
    -- получаем номер строки
    v_dg_order_id := SQ_DG_ID.NEXTVAL;
    -- добавляем строку
	/*
    INSERT INTO DG_ORDER_T (DG_ACCOUNT_ID, DG_ORDER_ID, ORDER_ID, DATE_FROM)
    VALUES (p_dg_account_id, v_dg_order_id, p_order_id, p_date_from);  
	*/
    -- возвращаем номер строки
    RETURN v_dg_order_id;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR. order_id ='||p_order_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Редактировать таблицу процентов скидок
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- получить номер строки
FUNCTION Get_next_prc_no(
            p_group_id  IN INTEGER
         ) RETURN INTEGER
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Get_next_prc_no';
    v_row_no    INTEGER;
BEGIN
    -- получаем номер строки
    SELECT MAX(ROW_NO) INTO v_row_no
      FROM DG_PERCENT_T DP
     WHERE DP.DG_ID = p_group_id;
    -- возвращаем номер строки
    RETURN v_row_no;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR. Group_id='||p_group_id, c_PkgName||'.'||v_prcName );
END;

-- Добавить величину
PROCEDURE Add_percent(
            p_group_id  IN NUMBER,
            p_row_no    IN NUMBER,
            p_value_min IN NUMBER,
            p_value_max IN NUMBER,
            p_percent   IN NUMBER
         )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Add_percent';
BEGIN
    -- добавляем строку
    INSERT INTO DG_PERCENT_T (DG_ID, ROW_NO, VALUE_MIN, VALUE_MAX, DISCOUNT_PRC)
    VALUES (p_group_id, p_row_no, p_value_min, p_value_max, p_percent);  
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR. Group_id='||p_group_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Список лицевых счетов в биллинге
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Аccount_list( 
               p_recordset    OUT t_refc, 
               p_account_no    IN VARCHAR2 -- первые символы номера договора
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Аccount_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
         SELECT ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, BILLING_ID
           FROM ACCOUNT_T
          WHERE ACCOUNT_NO LIKE p_account_no||'%'
          ORDER BY ACCOUNT_NO;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Список заказов на лицевых счетах в биллинге
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
function Order_list( 
		p_account_no	IN VARCHAR2,
		p_order_no		IN VARCHAR2, 
		p_columns		out varchar2	
	) return t_refc
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Order_list';
	p_recordset		t_refc;
	v_order_no		varchar2(100) := '%' || upper(p_order_no) || '%';
	v_account_no	varchar2(100) := '%' || upper(p_account_no) || '%';
	v_sysdate_1		date := sysdate + 1;
BEGIN
	p_columns := '<column_data><columns>';
	p_columns := p_columns || 
		'<column name="account_no" label="Лицевой счет" visible="1"/>';
	p_columns := p_columns || 
		'<column name="account_id" label="account_id" visible="0"/>';
	p_columns := p_columns || 
		'<column name="ORDER_ID" label="order_id" visible="0"/>';
	p_columns := p_columns || 
		'<column name="order_no" label="Номер заказа" visible="1"/>';
	p_columns := p_columns || 
		'<column name="service" label="Услуга" visible="1"/>';
	p_columns := p_columns || 
		'<column name="date_from" label="Дата начала" visible="1"/>';
	p_columns := p_columns || 
		'<column name="date_to" label="Дата конца" visible="1"/>';
	p_columns := p_columns || '</columns></column_data>';
    -- возвращаем курсор
	OPEN p_recordset FOR
		SELECT a.account_no, a.account_id, O.ORDER_ID, O.ORDER_NO, 
			s.service,
			to_char(O.DATE_FROM, 'dd.mm.yyyy') date_from, to_char(O.DATE_TO, 'dd.mm.yyyy') date_to
		FROM ORDER_T O, account_t a, order_body_t ob, service_t s
        WHERE (p_order_no is null or upper(O.ORDER_NO) LIKE v_order_no)
			and (p_account_no is null or upper(a.account_no) like v_account_no)
			AND O.ACCOUNT_ID  = a.account_id
			and a.billing_id in(2001,2002)
			and o.order_id = ob.order_id
			and ob.subservice_id = Pk00_Const.c_SUBSRV_DISC
			and ob.charge_type = Pk00_Const.c_CHARGE_TYPE_DIS
			and sysdate between o.date_from and nvl(o.date_to, v_sysdate_1)
			and sysdate between ob.date_from and nvl(ob.date_to, v_sysdate_1)
			and s.service_id = o.service_id
		ORDER BY account_no, ORDER_NO;
	return p_recordset;
END;

/*
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Список заказов на лицевых счетах в биллинге
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE OrderList( 
               p_recordset    OUT t_refc, 
               p_order_no      IN VARCHAR2, -- первые символы номера заказа
			   p_account_id		in number
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Order_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
         SELECT O.ORDER_ID, O.ORDER_NO, O.DATE_FROM, O.DATE_TO, S.SERVICE_ID, S.SERVICE
           FROM ORDER_T O, SERVICE_T S
          WHERE O.ORDER_NO LIKE p_order_no||'%'
            AND O.ACCOUNT_ID  = p_account_id
            AND O.SERVICE_ID = S.SERVICE_ID
          ORDER BY ORDER_NO;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
*/

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Список заказов c канальными услугами на лицевых счетах в биллинге
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Order_channel_list( 
               p_recordset  OUT t_refc, 
               p_order_no    IN VARCHAR2 -- первые символы номера заказа
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Order_channel_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
      SELECT O.ORDER_ID, O.ORDER_NO, O.DATE_FROM, O.DATE_TO, 
             S.SERVICE, CI.SPEED_VALUE, CI.SPEED_UNIT_ID, D.NAME
        FROM ORDER_T O, SERVICE_T S, 
             ORDER_BODY_T OB, IP_CHANNEL_INFO_T CI, DICTIONARY_T D
       WHERE O.SERVICE_ID = S.SERVICE_ID
         AND O.ORDER_ID   = OB.ORDER_ID
         AND CI.ORDER_BODY_ID = OB.ORDER_BODY_ID
         AND D.PARENT_ID = Pk00_Const.k_DICT_SPEED_UNIT -- 67
         AND D.KEY_ID = CI.SPEED_UNIT_ID
         AND O.ORDER_NO LIKE p_order_no||'%';

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Информация для выборки, пока к сведению
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Info_list( 
               p_recordset    OUT t_refc
             )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Info_list';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
      SELECT CL.CLIENT_ID,   CL.CLIENT_NAME,
             CT.CONTRACT_ID, CT.CONTRACT_NO,
             A.ACCOUNT_ID,   A.ACCOUNT_NO,
             O.ORDER_ID,     O.ORDER_NO,
             S.SERVICE_ID,   S.SERVICE,
             MIN(I.SPEED_STR)
        FROM ACCOUNT_T A,         -- 40.030
             ACCOUNT_PROFILE_T AP,-- 40.030
             CONTRACT_T   CT,     -- 40.030
             CLIENT_T     CL,     -- 40.030
             ORDER_T      O,      -- 31.256 / 72.866
             SERVICE_T    S,
             ORDER_BODY_T OB,     -- 61.525 / 112.428
             IP_CHANNEL_INFO_T I  -- 61.525
       WHERE A.ACCOUNT_TYPE   = 'J'
         AND A.STATUS         = 'B'
         AND A.BILLING_ID  IN ( 2001,2002 )
         AND O.SERVICE_ID NOT IN (1,2,7)  -- 1.240   (кроме голосовой связи МГ/МН/Местн/Зоновой)
         AND AP.ACCOUNT_ID    = A.ACCOUNT_ID
         AND AP.DATE_FROM    <= SYSDATE
         AND (AP.DATE_TO IS NULL OR SYSDATE <= AP.DATE_TO)
         AND CT.CONTRACT_ID   = AP.CONTRACT_ID
         AND CL.CLIENT_ID     = CT.CLIENT_ID
         AND O.ACCOUNT_ID     = A.ACCOUNT_ID
         AND O.DATE_FROM     <= SYSDATE
         AND (O.DATE_TO IS NULL OR SYSDATE <= O.DATE_TO)
         AND O.SERVICE_ID     = S.SERVICE_ID
         AND OB.ORDER_ID      = O.ORDER_ID
         AND OB.DATE_FROM    <= SYSDATE
         AND (OB.DATE_TO IS NULL OR SYSDATE <= OB.DATE_TO)
         AND OB.ORDER_BODY_ID = I.ORDER_BODY_ID(+)
       GROUP BY   -- 31.256
             CL.CLIENT_ID,   CL.CLIENT_NAME,
             CT.CONTRACT_ID, CT.CONTRACT_NO,
             A.ACCOUNT_ID,   A.ACCOUNT_NO,
             O.ORDER_ID,     O.ORDER_NO,
             S.SERVICE_ID,   S.SERVICE;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Процедура создания компонента услуги идентифицирующего скидку
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Add_discount_to_order (
               p_order_id  IN INTEGER,
               p_date_from IN DATE
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Add_discount_to_order';
    v_order_body_id INTEGER;
    v_currency_id   INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, order_id = '||p_order_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    -- получаем код валюты
    SELECT MIN(OB.CURRENCY_ID) INTO v_currency_id
      FROM ORDER_BODY_T OB
     WHERE OB.ORDER_ID = p_order_id
       AND OB.CHARGE_TYPE = PK00_CONST.c_CHARGE_TYPE_REC;

    -- cоздаем позицию скидки
    Pk06_ORDER.Add_subs_discount (
                   p_order_id      => p_order_id, -- ID заказа - услуги
                   p_currency_id   => v_currency_id, -- ID валюты позиции
                   p_date_from     => p_date_from
               );

    Pk01_Syslog.Write_msg('Stop'||p_order_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR. Order_id='||p_order_id, c_PkgName||'.'||v_prcName );
END;

-- удалить заказ из группы скидок
PROCEDURE Remove_discount_from_order (
               p_order_id  IN INTEGER,
               p_date_to IN DATE
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Remove_discount_from_order';
    v_count         INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start, order_id = '||p_order_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );  

    UPDATE ORDER_BODY_T OB SET OB.DATE_TO = p_date_to
     WHERE OB.ORDER_ID = p_order_id
       AND OB.CHARGE_TYPE = PK00_CONST.c_CHARGE_TYPE_DIS
       AND OB.SUBSERVICE_ID = PK00_CONST.c_SUBSRV_DISC
       AND OB.DATE_FROM <= p_date_to
       AND (OB.DATE_TO IS NULL OR p_date_to <= OB.DATE_TO);
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Stop '||v_count||' rows updated', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR. Order_id='||p_order_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Список групп
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
function Group_List(
	p_active			in number,
	p_ls_no				in varchar2,
	p_order_no			in varchar2,
	p_service			in varchar2,
	p_speed				in number,
	p_speed_unit_id		in number,
	p_columns			out varchar2
	) return t_refc is
	v_sql			varchar2(4000);
	v_ret			t_refc;
begin
	-- колонки
	p_columns := '<column_data><columns>';
	p_columns := p_columns || 
		'<column name="dg_id" label="Id" visible="1"/>';
	p_columns := p_columns || 
		'<column name="dg_name" label="Название" visible="1"/>';
	p_columns := p_columns || 
		'<column name="date_from" label="Начало действия" visible="1"/>';
	p_columns := p_columns || 
		'<column name="date_to" label="Конец действия" visible="1"/>';
	p_columns := p_columns || 
		'<column name="notes" label="Примечание" visible="1"/>';
	p_columns := p_columns || 
		'<column name="dg_rule_name" label="Тип скидки" visible="1"/>';
	p_columns := p_columns || 
		'<column name="dg_rule_id" label="Rule Id" visible="0"/>';
	p_columns := p_columns || '</columns></column_data>';
	-- запрос
	v_sql := 
		' select dg_id, dg_name, to_char(d.date_from, ''dd.mm.yyyy'') date_from, to_char(d.date_to, ''dd.mm.yyyy'') date_to, d.notes, dd.name dg_rule_name, d.dg_rule_id  ' ||
		' from discount_group_t d, dictionary_t dd ' ||
		' where d.dg_rule_id=dd.key_id and dd.parent_id=25 ';
	if p_ls_no is not null then
		-- filter by LS
		v_sql := v_sql ||
			' and exists( ' ||
			' 		select 1 from dg_account_t da, account_t a ' ||
			'			and upper(a.account_no) like ''%'' || upper(p_account_no) || ''%'' ' ||
			'			and da.dg_id = d.dg_id and da.account_id = a.account_id ' ||
			'	) ';
	end if;
	if p_order_no is not null then
		-- filter by order_no
		v_sql := v_sql ||
			' and  ' ||
			'	exists( ' ||
			'		select 1  ' ||
			'		from order_t o, order_body_t ob, dg_account_t da, account_t a ' ||
			'		where 				 ' ||
			'			and a.billing_id in( 2001,2002 ) ' ||
			'			and da.dg_id = d.dg_id and da.account_id = a.account_id ' ||
			'			and upper(o.order_no) like ''%'' || upper(p_order_no) || ''%''  ' || 
			'			and o.order_id = ob.order_id ' ||
			'			and ob.subservice_id = Pk00_Const.c_SUBSRV_DISC ' ||
			'			and ob.charge_type = Pk00_Const.c_CHARGE_TYPE_DIS ' ||
			'	) ';
	end if;
	if p_speed is not null then
		-- filter by speed
		v_sql := v_sql ||
			' and  ' ||
			'	exists( ' ||
			'		select 1 ' || 
			'		from dg_account_t da, account_t a, order_t o, order_body_t ob, ip_channel_info_t i ' ||
			'		where da.dg_id = d.dg_id and da.account_id = a.account_id ' ||
			'			and a.billing_id in( 2001,2002 ) ' ||
			'			and upper(o.order_no) like upper(''%'' || p_order_no || ''%'') ' ||
			'			and o.order_id = ob.order_id ' ||
			'			and ob.order_body_id = i.order_body_id ' ||
			'			and ob.subservice_id = Pk00_Const.c_SUBSRV_DISC ' ||
			'			and ob.charge_type = Pk00_Const.c_CHARGE_TYPE_DIS ' ||
			'			and i.speed_value = p_speed and i.speed_unit_id=' || p_speed_unit_id ||
			'	) ';
	end if;
	if p_service is not null then
		-- filter by services
		v_sql := v_sql ||
			' and ' ||
			'	exists( ' ||
			'		select 1 ' || 
			'		from dg_account_t da, account_t a, order_t o, service_t s ' ||
			'		where da.dg_id = d.dg_id and da.account_id = a.account_id ' ||
			'			and upper(o.order_no) like upper(''%'' || p_order_no || ''%'') ' ||
			'			and o.order_id = ob.order_id ' ||
			'			and s.service_id=o.service_id ' ||
			'			and s.service_id = p_service_id ' ||
			'			and s.service like ''%'' || upper(p_service) || ''%'' ' ||
			'	) ';
	end if;
	if p_active = 1 then
		-- filter by active
		v_sql := v_sql ||
			' and sysdate between d.date_from and nvl(d.date_to, sysdate+1) ';
	end if;
	v_sql := v_sql || ' order by dg_name';
	open v_ret for v_sql;
	return v_ret;
end;

/* ******************************************************
* список лицевых счетов по группе
*/
function Group_Account_List(
	p_dg_id		in number,
	p_columns	out varchar2
	) return t_refc is
	p_ret		t_refc;
begin
	-- колонки
	p_columns := '<column_account><columns>';
	p_columns := p_columns || 
		'<column name="dg_id" label="Dg_Id" visible="0"/>';
	p_columns := p_columns || 
		'<column name="account_id" label="Account Id" visible="0"/>';
	p_columns := p_columns || 
		'<column name="account_no" label="Номер лицевого счета" visible="1"/>';
	p_columns := p_columns || 
		'<column name="date_from" label="Дата начала" visible="1"/>';
	p_columns := p_columns || 
		'<column name="date_to" label="Дата конца" visible="1"/>';
	p_columns := p_columns || 
		'<column name="notes" label="Примечание" visible="1"/>';
	p_columns := p_columns || '</columns></column_account>';
	open p_ret for
		select d.dg_id, d.account_id, a.account_no, to_char(d.date_from, 'dd.mm.yyyy') date_from, 
			to_char(d.date_to, 'dd.mm.yyyy') date_to, d.notes
		from dg_account_t d, account_t a
		where d.dg_id = p_dg_id and d.account_id=a.account_id
			and sysdate between date_from and nvl(date_to, sysdate+1);
	return p_ret;
end ;



/* ******************************************************
* список процентов по группе
*/
function Group_Percent_List(
	p_dg_id		in number,
	p_columns	out varchar2
	) return t_refc is
	p_ret		t_refc;
begin
	-- колонки
	p_columns := '<column_percent><columns>';
	p_columns := p_columns || 
		'<column name="dg_id" label="Dg_Id" visible="0"/>';
	p_columns := p_columns || 
		'<column name="row_no" label="Номер строки" visible="1"/>';
	p_columns := p_columns || 
		'<column name="value_min" label="Мин. значение" visible="1"/>';
	p_columns := p_columns || 
		'<column name="value_max" label="Макс. значение" visible="1"/>';
	p_columns := p_columns || 
		'<column name="discount_percent" label="Процент" visible="1"/>';
	p_columns := p_columns || '</columns></column_percent>';
	open p_ret for
		select d.dg_id, d.row_no, d.value_min, d.value_max, d.discount_prc 
		from dg_percent_t d
		where d.dg_id = p_dg_id
		order by d.row_no;
	return p_ret;
end ;

/* *************************************************************
* список л/с и заказов в группе
**/
procedure Account_Order_List(
	p_dg_id		in number,
	p_ret		out t_refc
	) is
begin
	open p_ret for
		SELECT da.account_id, da.date_from, da.date_to, da.notes, a.account_no,
			O.ORDER_ID, O.ORDER_NO, O.DATE_FROM, O.DATE_TO
		FROM dg_account_t da, account_t a, ORDER_T O
		WHERE da.account_id=a.account_id
			and O.ACCOUNT_ID  = a.account_id
			and sysdate between da.date_from and nvl(da.date_to, sysdate+1)
			and sysdate between o.date_from and nvl(o.date_to, sysdate+1)
			and exists( 
					select 1 from order_body_t ob
					where ob.subservice_id = 32
						and ob.charge_type = 'DIS'
						and ob.order_id = o.order_id
						and sysdate between ob.date_from and nvl(ob.date_to, sysdate+1))
		ORDER BY ACCOUNT_NO, o.order_no;
end;


/* ******************************************************************
* получить данные группы
*/
procedure Get_Group_Data(
	p_dg_id		in number,
	p_dg_name	out varchar2,
	p_date_from	out varchar2,
	p_date_to	out varchar2,
	p_dt_id		out number,
	p_dt_name	out varchar2,
	p_note		out varchar2
	) is
begin
	select d.dg_name, d.dg_rule_id, dd.name, 
		to_char(d.date_from, 'dd.mm.yyyy'), to_char(d.date_to, 'dd.mm.yyyy'), d.notes
	into p_dg_name, p_dt_id, p_dt_name, p_date_from, p_date_to, p_note
	from discount_group_t d, dictionary_t dd
	where d.dg_id = p_dg_id and d.dg_rule_id = dd.key_id;
end;

-- *************************************************************
END PK39_BILLING_DISCOUNT_GUI;
/
