CREATE OR REPLACE PACKAGE PK38_BILLING_DOWNTIME_GUI
IS



    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK38_BILLING_DOWNTIME_GUI';
    -- ==============================================================================
    type t_refc is ref cursor;
    --

	-- ---------------------------------------------------------------------------
	-- список тарифных планов
	-- ---------------------------------------------------------------------------
	function Rateplan_List(
		p_rateplan_name	in varchar2,
		p_rateplan_note	in varchar2		
		) return t_refc;
	-- --------------------------------------------------------------------------
	-- получить данные тарифного плана
	-- --------------------------------------------------------------------------
	function Get_Rateplan_Data(
		p_rateplan_id	in number,
		p_name			out varchar2,
		p_note			out varchar2,
		p_tax_incl		out varchar2,
		p_currency_id	out number,
		p_rateplan_type	out varchar2
		) return t_refc;


    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Добавить в заказ запись с описанием SLA
    -- SLA может бытть задан:
    -- 1) в часах - RATE_RULE_ID = 2413 (c_RATE_RULE_SLA_H)
    -- 2) коэффициентом доступности - RATE_RULE_ID = 2414 (c_RATE_RULE_SLA_K)
    -- Возвращает order_body_id
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Add_SLA_to_order (
                  p_order_id      IN INTEGER,
                  p_rate_rule_id  IN NUMBER,  -- тип задания  SLA
                  p_date_from     IN DATE,
                  p_rateplan_id   IN INTEGER, -- тарифный план
                  p_free_downtime IN NUMBER,  -- некомпенсируемое время простоя
                  p_notes         IN VARCHAR2,
				  p_currency_id		IN NUMBER,
				  p_tax_incl		IN VARCHAR2
              ) RETURN INTEGER;

	-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
	-- Заурыть запись с описанием SLA
	-- SLA может бытть задан:
	-- 1) в часах - RATE_RULE_ID = 2413 (c_RATE_RULE_SLA_H)
	-- 2) коэффициентом доступности - RATE_RULE_ID = 2414 (c_RATE_RULE_SLA_K)
	-- Возвращает order_body_id
	-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
	function Close_SLA_order (
              p_order_id      	IN INTEGER,
              p_date_to			IN DATE,
              p_notes         	IN VARCHAR2
          ) return varchar2;

	-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Создать тарифный план для SLA 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Create_SLA_rateplan (
				p_rateplan_id 	IN NUMBER,
				p_rateplan_code IN VARCHAR2,
				p_rateplan_name IN VARCHAR2,
                p_notes         IN VARCHAR2,
				p_currency_id	in number,
				p_rateplan_type	in varchar2,
				p_tax_inc		in varchar2
              ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Заполнение тарифа. Работа со строками таблици SLA_PERCENT_T
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Add_SLA_percent (
                  p_rateplan_id  IN INTEGER,
                  p_recno        IN INTEGER, -- номер строки по порядку
                  p_percent      IN NUMBER,
                  p_k_min        IN NUMBER,
                  p_k_max        IN NUMBER
              );

    -- удаление строки тарифа
    PROCEDURE Del_SLA_percent(
                  p_rateplan_id  IN INTEGER,
                  p_recno        IN INTEGER  -- номер строки по порядку
              );
	-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
	-- Список  заказов с фильтром
	-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    function Order_List(
		p_account_no	in varchar2,
		p_order_no		in varchar2,
		p_sla			in integer, 
		p_columns		out varchar2
		) return t_refc;
	
	-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
	-- Данные тарифа для заказа
	-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
	procedure Get_Order_SLA(
		p_order_id			in number,
		v_ob_data			out t_refc,
		v_rateplan			out t_refc,
		v_percent			out t_refc
		) ;


	-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
	-- Создать/изменить тарифный план для заказа с SLA
	-- p_percent - xml format
	-- <percents>
	--		<percent>
	--			<p_recno/>      INTEGER, -- номер строки по порядку
	--          <p_percent/>    NUMBER,
	--          <p_k_min/>      NUMBER,
	--          <p_k_max/>		number 
	-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
	function Edit_Rateplan(
		p_rateplan_id		in number,
		p_name				in varchar2,
		p_currency_id		in number,
		p_rateplan_type		in varchar2,
		p_tax_inc			in varchar2,
		p_notes				in varchar2,
		p_percent_list		in varchar2
		) return number;

	-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
	-- Добавить в заказ запись с описанием SLA
	-- SLA может бытть задан:
	-- 1) в часах - RATE_RULE_ID = 2413 (c_RATE_RULE_SLA_H)
	-- 2) коэффициентом доступности - RATE_RULE_ID = 2414 (c_RATE_RULE_SLA_K)
	-- Возвращает order_body_id
	-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
	PROCEDURE Save_SLA (
			p_order_list    IN VARCHAR2,
			--p_rate_rule_id  IN NUMBER,  -- тип задания  SLA
			p_date_from     IN DATE,
			--p_free_downtime IN NUMBER,  -- некомпенсируемое время простоя в часах
			p_notes         IN VARCHAR2,
			--p_percent_list	IN VARCHAR2,
			p_rateplan_id	in number
			  );
		
/* ================================================================================ */		
END PK38_BILLING_DOWNTIME_GUI;
/
CREATE OR REPLACE PACKAGE BODY PK38_BILLING_DOWNTIME_GUI
IS

	c_RATEPLAN_SYSTEM_ID constant number := 1208;
	c_TAX_INC constant varchar2(1) := 'N';
	c_CURRENCY_ID constant number := 810;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Добавить в заказ запись с описанием SLA
-- SLA может бытть задан:
-- 1) в часах - RATE_RULE_ID = 2413 (c_RATE_RULE_SLA_H)
-- 2) коэффициентом доступности - RATE_RULE_ID = 2414 (c_RATE_RULE_SLA_K)
-- p_order_list = 
--	<order_list>
--		<order>
--			<order_id>
-- Возвращает order_body_id
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Save_SLA (
		p_order_list    IN VARCHAR2,
--		p_rate_rule_id  IN NUMBER,  -- тип задания  SLA
		p_date_from     IN DATE,
--		p_free_downtime IN NUMBER,  -- некомпенсируемое время простоя в часах
		p_notes         IN VARCHAR2,
--		p_percent_list	IN VARCHAR2,
		p_rateplan_id	in number
          ) IS
    v_order_body_id INTEGER;
	v_rateplan_id	INTEGER;
	v_xml				xmltype;
	TYPE orders_type IS RECORD (
		order_id		number
	);
	v_cur				t_refc;
	v_orders			orders_type;
BEGIN
	
	v_xml := xmltype(p_order_list);
	open v_cur for 
		select 
			extractValue(value(t),'order/order_id') v_order_id
		from table(XMLSequence(v_xml.extract('order_list/order'))) t
		order by 1;
	loop
		fetch v_cur into v_orders;
		exit when v_cur%notfound;
		-- проверить наличие SLA у заказа
		begin
			select ob.order_body_id, ob.rateplan_id
				into v_order_body_id, v_rateplan_id
			from order_body_t ob
			where ob.order_id = v_orders.order_id
				and ob.subservice_id = Pk00_Const.c_SUBSRV_IDL
				and ob.charge_type = Pk00_Const.c_CHARGE_TYPE_SLA
				and sysdate <= nvl(ob.date_to, sysdate+1);
			-- изменить тариф
			/*if p_rateplan_id is null then
				v_rateplan_id := Edit_Rateplan(v_rateplan_id, null, c_CURRENCY_ID, 'HOUR', c_TAX_INC, p_notes, p_percent_list);
			else*/
				v_rateplan_id := p_rateplan_id;
			/*end if;*/
			-- SLA есть - редактируем
			update order_body_t ob
			set --ob.rate_rule_id = p_rate_rule_id,
				ob.date_from = p_date_from,
				--ob.free_value = p_free_downtime,
				ob.rateplan_id = v_rateplan_id
			where ob.order_body_id = v_order_body_id;
		exception when no_data_found then
			-- SLA нет - добавляем
			-- проверить закрытый SLA
			begin
				select ob.order_body_id
					into v_order_body_id
				from order_body_t ob
				where ob.order_id = v_orders.order_id
					and ob.subservice_id = Pk00_Const.c_SUBSRV_IDL
					and ob.charge_type = Pk00_Const.c_CHARGE_TYPE_SLA
					and p_date_from between ob.date_from and nvl(ob.date_to, p_date_from+1);
				-- на указанную дату был SLA
				RAISE_APPLICATION_ERROR(pk01_syslog.n_APP_EXCEPTION, 'На указанную дату есть закрытый SLA');
			exception when no_data_found then
				null;
			end;
			-- создать тарифный план
			/*if p_rateplan_id is null then
				v_rateplan_id := Edit_Rateplan(null, null, c_CURRENCY_ID, 'HOUR', c_TAX_INC, p_notes, p_percent_list);
			else*/
				v_rateplan_id := p_rateplan_id;
			/*end if;*/
			-- добавить SLA
			v_order_body_id := Add_SLA_to_order(
				v_orders.order_id, null/*p_rate_rule_id*/, p_date_from, v_rateplan_id, null/*p_free_downtime*/, p_notes,
				c_CURRENCY_ID, c_TAX_INC
				);
		end;
	end loop;
END;

-- ---------------------------------------------------------------------------
-- список тарифных планов
-- ---------------------------------------------------------------------------
function Rateplan_List(
	p_rateplan_name	in varchar2,
	p_rateplan_note	in varchar2
	) return t_refc is
	v_rateplan_name	varchar2(1000) := '%' || upper(p_rateplan_name) || '%';
	v_rateplan_note	varchar2(1000) := '%' || upper(p_rateplan_note) || '%';	
	v_ret	t_refc;
begin
	open v_ret for
		select r.*, c.currency_code 
		from rateplan_t r, currency_t c
		where r.ratesystem_id = 1208
			and (p_rateplan_name is null or upper(r.rateplan_name) like v_rateplan_name)
			and (p_rateplan_note is null or upper(r.note) like v_rateplan_note)
			and c.currency_id=r.currency_id
		order by r.rateplan_name;
	return v_ret;
end;

-- --------------------------------------------------------------------------
-- получить данные тарифного плана
-- --------------------------------------------------------------------------
function Get_Rateplan_Data(
	p_rateplan_id	in number,
	p_name			out varchar2,
	p_note			out varchar2,
	p_tax_incl		out varchar2,
	p_currency_id	out number,
	p_rateplan_type	out varchar2
	) return t_refc is
	c_cur 			t_refc;
begin
	-- данные тп
	select r.rateplan_name, r.note, r.tax_incl, r.currency_id, r.rateplan_type
	into p_name, p_note, p_tax_incl, p_currency_id, p_rateplan_type
	from rateplan_t r
	where r.rateplan_id = p_rateplan_id;
	-- проценты
	open c_cur for 		
		select * from sla_percent_t s where s.rateplan_id=p_rateplan_id order by s.rec_id;
	return c_cur;
end;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Создать тарифный план для заказа с SLA
-- p_percent - xml format
-- <percents>
--		<percent>
--			<recno/>      INTEGER, -- номер строки по порядку
--          <percent/>    NUMBER,
--          <k_min/>      NUMBER,
--          <k_max/>		number 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
function Edit_Rateplan(
	p_rateplan_id		in number,
	p_name				in varchar2,
	p_currency_id		in number,
	p_rateplan_type		in varchar2,
	p_tax_inc			in varchar2,
	p_notes				in varchar2,
	p_percent_list		in varchar2
	) return number is
	v_rateplan_id		number := p_rateplan_id;
	v_rateplan_name		varchar2(1024);
	v_rateplan_code		varchar2(1024);
	v_xml				xmltype;
	TYPE percent_type IS RECORD (
		rec_no			number,
		percent			number,
		k_min			number,
		k_max			number
	);
	v_cur				t_refc;
	v_percent			percent_type;
	v_k_prev			number;
begin
	if v_rateplan_id is null then 		-- новый ТП
		-- сформировать название и код тарифного плана SLA-ob_id-rp_id
		v_rateplan_id := Pk02_POID.Next_rateplan_id;
		v_rateplan_code := to_char(systimestamp, 'yymmddhh24missff') || '-' || v_rateplan_id;
		if p_name is null then
			v_rateplan_name := 'SLA-' || v_rateplan_code;
		else
			v_rateplan_name := p_name;
		end if;
		-- создать ТП
		v_rateplan_id := Create_SLA_rateplan (
					v_rateplan_id,
					v_rateplan_code,
					v_rateplan_name,
					p_notes, 
					p_currency_id,
					p_rateplan_type,
					p_tax_inc);
	else -- редактирование тарифного плана
		update rateplan_t r 
		set r.rateplan_name = p_name,
			r.note = p_notes,
			r.tax_incl = p_tax_inc,
			r.currency_id = p_currency_id,
			r.rateplan_type=p_rateplan_type
		where r.rateplan_id=p_rateplan_id;
	end if;
	-- удалить данные ТП
	DELETE FROM SLA_PERCENT_T 
	     WHERE RATEPLAN_ID = v_rateplan_id;
	-- создать записи ТП
	v_xml := xmltype(p_percent_list);
	v_k_prev := -100;
	open v_cur for 
		select 
			extractValue(value(t),'percent/rec_no') rec_no,
			extractValue(value(t),'percent/percent') percent,
			extractValue(value(t),'percent/k_min') k_min,
			extractValue(value(t),'percent/k_max') k_max
		from table(XMLSequence(v_xml.extract('percents/percent'))) t
		order by 1;
	loop
		fetch v_cur into v_percent;
		exit when v_cur%notfound;
		if v_k_prev!=-100 and v_k_prev != v_percent.k_min or v_percent.k_min > v_percent.k_max then
			RAISE_APPLICATION_ERROR(pk01_syslog.n_APP_EXCEPTION, 'Неправильно указаны данные тарифа');
		end if;
		Add_SLA_percent (
		 	v_rateplan_id, 
			v_percent.rec_no,
			v_percent.percent,
			v_percent.k_min,
			v_percent.k_max
          );
		 v_k_prev := v_percent.k_max;
	end loop;
	--	
	return v_rateplan_id;
end;
	
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Данные тарифа для заказа
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
procedure Get_Order_SLA(
	p_order_id			in number,
	v_ob_data			out t_refc,
	v_rateplan			out t_refc,
	v_percent			out t_refc
	)  is
	v_ret	t_refc;
	v_rateplan_id		number;
begin
	--
	select ob.rateplan_id into v_rateplan_id
	from order_body_t ob
	where sysdate <= nvl(ob.date_to, sysdate+1)
		and ob.order_id = p_order_id
		and ob.charge_type = pk00_const.c_CHARGE_TYPE_SLA
		and ob.subservice_id = Pk00_Const.c_SUBSRV_IDL;
	--
	open v_ob_data for
		select *
		from order_body_t ob
		where sysdate <= nvl(ob.date_to, sysdate+1)
			and ob.order_id = p_order_id
			and ob.charge_type = pk00_const.c_CHARGE_TYPE_SLA
			and ob.subservice_id = Pk00_Const.c_SUBSRV_IDL;
	--
	open v_rateplan for
		select r.*
		from rateplan_t r
		where r.rateplan_id = v_rateplan_id ;
	open v_percent for
		select s.*
		from SLA_PERCENT_T s
		where s.rateplan_id = v_rateplan_id
		order by s.rec_id;
end;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Список  заказов с фильтром
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
function Order_List(
	p_account_no	in varchar2,
	p_order_no		in varchar2,
	p_sla			in integer, 
	p_columns		out varchar2
	) return t_refc is
	p_recordset		t_refc;
	v_order_no		varchar2(100);
	v_sysdate_1		date := sysdate + 1;
	v_account_no	varchar2(100) := upper(p_account_no);
BEGIN
	select decode(p_order_no, null, '%', '%' || upper(p_order_no) || '%')
	into v_order_no
	from dual;	

	p_columns := '<column_data><columns>';
	p_columns := p_columns || 
		'<column name="account_no" label="Лицевой счет" visible="1"/>';
	p_columns := p_columns || 
		'<column name="account_id" label="account_id" visible="0"/>';
	p_columns := p_columns || 
		'<column name="service" label="Услуга" visible="1"/>';
	p_columns := p_columns || 
		'<column name="order_id" label="order_id" visible="0"/>';
	p_columns := p_columns || 
		'<column name="order_no" label="Номер заказа" visible="1"/>';
	p_columns := p_columns || 
		'<column name="order_date" label="Дата заказа" visible="1"/>';
	p_columns := p_columns || 
		'<column name="downtime" label="Простой" visible="1"/>';
	p_columns := p_columns || 
		'<column name="downtime_date" label="Дата простоя" visible="1"/>';
	p_columns := p_columns || 
		'<column name="rateplan_id" label="rateplan_id" visible="0"/>';		
	p_columns := p_columns || 
		'<column name="order_body_id" label="order_body_id" visible="0"/>';
	p_columns := p_columns || '</columns></column_data>';
    -- возвращаем курсор
	OPEN p_recordset FOR
		SELECT 
			a.account_no, a.account_id, 
			s.service, 
			O.ORDER_ID, O.ORDER_NO,  
			to_char(O.DATE_FROM, 'dd.mm.yyyy')||'-'||
				decode(to_char(O.DATE_TO, 'dd.mm.yyyy'), '', to_char(O.DATE_TO, 'dd.mm.yyyy')) order_date,
			(select decode(count(1),1,'Да','Нет') from order_body_t ob 
						where ob.subservice_id = Pk00_Const.c_SUBSRV_IDL
							and ob.charge_type = Pk00_Const.c_CHARGE_TYPE_SLA
							and ob.order_id = o.order_id
							and sysdate <= nvl(ob.date_to, sysdate+1)) downtime,
			(select to_char(Ob.DATE_FROM, 'dd.mm.yyyy')||'-'||
				decode(to_char(Ob.DATE_TO, 'dd.mm.yyyy'), '01.01.2050', '',to_char(Ob.DATE_TO, 'dd.mm.yyyy')) 
				from order_body_t ob
				where ob.subservice_id = Pk00_Const.c_SUBSRV_IDL
					and ob.charge_type = Pk00_Const.c_CHARGE_TYPE_SLA
					and ob.order_id = o.order_id
					and sysdate <= nvl(ob.date_to, sysdate+1)
			) downtime_date,
			(select ob.rateplan_id
				from order_body_t ob
				where ob.subservice_id = Pk00_Const.c_SUBSRV_IDL
					and ob.charge_type = Pk00_Const.c_CHARGE_TYPE_SLA
					and ob.order_id = o.order_id
					and sysdate <= nvl(ob.date_to, sysdate+1)
			) rateplan_id,
			(select ob.order_body_id
				from order_body_t ob
				where ob.subservice_id = Pk00_Const.c_SUBSRV_IDL
					and ob.charge_type = Pk00_Const.c_CHARGE_TYPE_SLA
					and ob.order_id = o.order_id
					and sysdate between ob.date_from and nvl(ob.date_to, sysdate+1)
			) order_body_id
		FROM ORDER_T O, account_t a, service_t s
        WHERE upper(O.ORDER_NO) LIKE v_order_no
			and (
				p_sla is null 
					or
				p_sla=1 and exists(select 1 from order_body_t ob 
									where ob.order_id=o.order_id
										and ob.subservice_id = Pk00_Const.c_SUBSRV_IDL
										and ob.charge_type = Pk00_Const.c_CHARGE_TYPE_SLA
										and sysdate <= nvl(ob.date_to, sysdate+1))
					or
				p_sla=0 and not exists(select 1 from order_body_t ob 
											where ob.order_id=o.order_id
												and ob.subservice_id = Pk00_Const.c_SUBSRV_IDL
												and ob.charge_type = Pk00_Const.c_CHARGE_TYPE_SLA)
				)
			and (v_account_no is null or v_account_no = a.account_no)
			AND O.ACCOUNT_ID  = a.account_id
			and a.billing_id in(2001,2002)
			and sysdate between o.date_from and nvl(o.date_to, v_sysdate_1)
			and s.service_id = o.service_id
			and exists(
				SELECT * FROM ORDER_BODY_T OB
	            WHERE OB.ORDER_ID = O.ORDER_ID
    		        AND OB.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_REC, 
                                     Pk00_Const.c_CHARGE_TYPE_MIN)
            		AND OB.DATE_FROM <= sysdate
					AND (OB.DATE_TO IS NULL OR  sysdate <= OB.DATE_TO)
       			)
		ORDER BY ORDER_NO;
	return p_recordset;
END;
	
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Добавить в заказ запись с описанием SLA
-- SLA может бытть задан:
-- 1) в часах - RATE_RULE_ID = 2413 (c_RATE_RULE_SLA_H)
-- 2) коэффициентом доступности - RATE_RULE_ID = 2414 (c_RATE_RULE_SLA_K)
-- Возвращает order_body_id
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Add_SLA_to_order (
              p_order_id      	IN INTEGER,
              p_rate_rule_id  	IN NUMBER,  -- тип задания  SLA
              p_date_from     	IN DATE,
              p_rateplan_id   	IN INTEGER, -- тарифный план
              p_free_downtime 	IN NUMBER,  -- некомпенсируемое время простоя в часах
              p_notes         	IN VARCHAR2,
			  p_currency_id		IN NUMBER,
			  p_tax_incl		IN VARCHAR2
          ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Add_SLA_to_order';
    v_order_body_id INTEGER;
    v_free_downtime NUMBER;
	  v_idl_id		    INTEGER;
BEGIN
    v_order_body_id := Pk02_Poid.Next_order_body_id;
    IF p_free_downtime IS NULL THEN
        v_free_downtime := 0; -- значение по умолчанию
    ELSE 
        v_free_downtime := p_free_downtime;
    END IF;
    
    -- удалить записи с IDL, если они в этом же периоде, что и устанавливаемая SLA
    delete order_body_t
    where order_id = p_order_id
      and charge_type='IDL'
      and date_from >= trunc(p_date_from, 'mm');
    -- для остальных - закрыть IDL
    update order_body_t b
      set date_to = (p_date_from - 1/(24*3600))
    where order_id = p_order_id
      and charge_type='IDL'
      and b.date_from < trunc(p_date_from, 'mm')
      and (b.date_to is null or b.date_to >= trunc(p_date_from, 'mm'));
      
    -- добавить SLA
    INSERT INTO ORDER_BODY_T(
        ORDER_BODY_ID, ORDER_ID, SUBSERVICE_ID, CHARGE_TYPE, DATE_FROM, DATE_TO, 
        RATEPLAN_ID, RATE_VALUE, FREE_VALUE, RATE_RULE_ID, RATE_LEVEL_ID, 
        TAX_INCL, QUANTITY, CREATE_DATE, MODIFY_DATE, CURRENCY_ID, NOTES
    )
    SELECT v_order_body_id, p_order_id, 
           Pk00_Const.c_SUBSRV_IDL, 
           Pk00_Const.c_CHARGE_TYPE_SLA CHARGE_TYPE,
           TRUNC(p_date_from) DATE_FROM, 
           O.DATE_TO, 
           p_rateplan_id RATEPLAN_ID, 
           NULL RATE_VALUE, 
           v_free_downtime FREE_VALUE, 
           p_rate_rule_id RATE_RULE_ID,
           Pk00_Const.c_RATE_LEVEL_ORDER RATE_LEVEL_ID, 
           p_TAX_INCL, 1 QUANTITY, 
           SYSDATE CREATE_DATE, 
           SYSDATE MODIFY_DATE, 
           p_CURRENCY_ID, 
           p_notes NOTES
      FROM ORDER_T O
     WHERE O.ORDER_ID = p_order_id
       AND p_date_from between O.DATE_FROM and nvl(O.DATE_TO, p_date_from +1)
       AND EXISTS (    
           SELECT * FROM ORDER_BODY_T OB
            WHERE OB.ORDER_ID = O.ORDER_ID
              AND OB.CHARGE_TYPE IN (Pk00_Const.c_CHARGE_TYPE_REC, 
                                     Pk00_Const.c_CHARGE_TYPE_MIN)
              AND p_date_from between OB.DATE_FROM AND nvl(OB.DATE_TO, p_date_from +1)
       );
    RETURN v_order_body_id;
    
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Заурыть запись с описанием SLA
-- SLA может бытть задан:
-- 1) в часах - RATE_RULE_ID = 2413 (c_RATE_RULE_SLA_H)
-- 2) коэффициентом доступности - RATE_RULE_ID = 2414 (c_RATE_RULE_SLA_K)
-- Возвращает order_body_id
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
function Close_SLA_order (
              p_order_id      	IN INTEGER,
              p_date_to			IN DATE,
              p_notes         	IN VARCHAR2
          ) return varchar2 IS
	v_date_to		date := trunc(p_date_to)+1-1/(24*3600);
	v_cnt			number;
BEGIN
	-- проверить наличие других записей на дату закрытия
	select count(1) into v_cnt
	from ORDER_BODY_T ob
	where Ob.ORDER_ID = p_order_id
		and v_date_to between Ob.DATE_FROM and nvl(v_date_to, sysdate+1)
		AND sysdate between Ob.DATE_FROM and nvl(ob.date_to, sysdate+1)
		AND Ob.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_SLA
	;
	if v_cnt > 1 then
		return 'На указанную дату есть несколько записей SLA у данного заказа';
	elsif v_cnt < 1 then
		return 'На указанную дату нет записей SLA у данного заказа';
	end if;
    -- закрыть запись об sla
	UPDATE ORDER_BODY_T ob
	set ob.date_to = v_date_to,
		ob.notes = nvl(p_notes, ob.notes)
	WHERE Ob.ORDER_ID = p_order_id
		and v_date_to between Ob.DATE_FROM and nvl(v_date_to, sysdate+1)
		AND sysdate between Ob.DATE_FROM and nvl(ob.date_to, sysdate+1)
		AND Ob.CHARGE_TYPE = Pk00_Const.c_CHARGE_TYPE_SLA
	;
	if sql%rowcount > 1 then
		return 'Закрыто более одной записи';
	elsif sql%rowcount = 0 then
		return 'Нет записей для закрытия';
	else
		return 'OK';
	end if;
EXCEPTION WHEN OTHERS THEN
    return sqlerrm;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Создать тарифный план для SLA 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Create_SLA_rateplan (
			p_rateplan_id	in number,
			p_rateplan_code IN VARCHAR2,
            p_rateplan_name IN VARCHAR2,
            p_notes         IN varchar2,
			p_currency_id	in number,
			p_rateplan_type	in varchar2,
			p_tax_inc		in varchar2
          ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Add_SLA_percent';
    v_ratesystem_id CONSTANT INTEGER := c_RATEPLAN_SYSTEM_ID; -- Расчет компенсации простоев по методу SLA
BEGIN

    INSERT INTO RATEPLAN_T ( 
        RATEPLAN_ID, RATEPLAN_NAME, RATESYSTEM_ID, NOTE, RATEPLAN_CODE, Tax_Incl, Currency_Id, RATEPLAN_TYPE
    )VALUES(
        p_rateplan_id, p_rateplan_name, v_ratesystem_id, p_notes, p_rateplan_code, p_tax_inc, p_currency_id, p_rateplan_type
    );
        
    RETURN p_rateplan_id;

EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Заполнение тарифа. Работа со строками таблици SLA_PERCENT_T
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Add_SLA_percent (
              p_rateplan_id  IN INTEGER,
              p_recno        IN INTEGER, -- номер строки по порядку
              p_percent      IN NUMBER,
              p_k_min        IN NUMBER,
              p_k_max        IN NUMBER
          )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Add_SLA_percent';
BEGIN
    INSERT INTO SLA_PERCENT_T (
        RATEPLAN_ID, REC_ID, SLA_PERCENT, K_MIN, K_MAX
    )
    VALUES (p_rateplan_id, p_recno, p_percent, p_k_min, p_k_max);
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- удаление строки тарифа
PROCEDURE Del_SLA_percent(
              p_rateplan_id  IN INTEGER,
              p_recno        IN INTEGER  -- номер строки по порядку
          )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Del_SLA_percent';
BEGIN
    DELETE FROM SLA_PERCENT_T 
     WHERE RATEPLAN_ID = p_rateplan_id
       AND REC_ID = p_recno;
EXCEPTION WHEN OTHERS THEN
    Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


/************************************************************************************
1. Простои.

- Все простои в ДРУ собираются в Access базу и из нее загружаются в АСР.
Вот пример этой базы

Перед записью в базу все простои агрегируются по заказам за месяц.
В БД поля

ID (чтобы 2 раза не грузить, присваивают в ДРУ при создании записи)
номер заказа
дата начала периода
дата окончания периода
простой в часах (с десятыми и сотыми, а не с минутами)
flags    - показывает в чем прстой, в часах или К.доступности. О нем ниже ...

- Простои из БД опкодом грузятся в АСР. Есть спец АРМ для такой загрузки.
	в опкоде :
- по номеру заказа определяется, нет ли этого заказа в таблицах SLA. Если нет, то это обычный простой.
Тогда: 

- по номеру заказа находится его абонплата (для каналов) или
	минималка (для VPN и интернет).
- длительность простоя округляется до целых часов (причем целые полчаса округляются в меньшую сторону)
- абонплата (или минималка) делится на 720 (среднее число часов в месяце) и умножается на часы простоя (целые).
	получаем вычет за простой.


Вычет за прстой загружается в АСР со знаком минус, создается запись с доп. информацией. Например:
cycle_part		Downtime (без округления)      Percent		Flag
17.080555555555555	0.699999999999999		0		NULL
Эта информация для деталировки.

Для случая SLA Percent и Flag имеют другие значения.

Есть договор исключение (со Сбербанком) у которого длительность простоя не округляется.
а стоимость простоя вычисляется по неокругленной длительности.

2. SLA.
Если заказ опкодом найден в списке заказов SLA. То возвращаемая часть абонплаты или минималки вычисляется по-другому.
В этом случае по длительности простоя вычисляется коэффициент доступности.
коэф. доступности вычисляется по формуле
(100 * (1 - (часы простоя / (24 * дней в месяце)))
полученный результат находится в пределах от 0 до 1.
В таблице SLA для каждого заказа вычисляется (введенные заранее параметры) пороги скидки в процентах
за определенный коэффициент доступности. Пример :

Процент скидки	Макс К			Мин К
			
1 			99.69                                    	99.65
3 			99.64                                    	99.6 
5 			99.59                                    	99.55
7 			99.54                                    	99.5 
10			99.489			0   .

При вычислении интревала, в котрый попал К доступности определяем процент.
- Вычисляем абонплату или минималку и делаем вычет на величину полученного процента от них.

В этом случае в АСР грузится в поле Percent - величина вычисленного процента скидки.

Есть случай когда в БД Аccess записан уже К.Доступности. И в поле flags стоит 1.
Тогда К доступности не вычисляем, как в предыдущем случае, а сразу
ищем интервал длоя определения процента скидки.


SLA работает именно на заказ. Т.к. не все заказы в договоре могут иметь соглашение с клиентом на SLA.
Но. Чащще всего вносятся по списку заказов и т.д. Удобство ведения и конструкцию этой базы (заказы, пороги и т.д.)
нужно бы продумать отдельно. может и не так, как сделано сейчас.
************************************************************************************/


/***********************************************************************************
Экспорт порогов из СТАРОГО БИЛЛИНГА

1. Все заказы на котрые подписан SLA

SELECT a.obj_id0, s.login
  FROM sla_services_t a, service_t s 
  where s.poid_id0 = a.service_obj_id0

Здесь a.obj_id0 - id связки на таблицу с порогами
процентами по данному заказу.
s.login - № Заказа.

- Связь может быть многие ко многим, т.е. на набор порогом может быть
несколько заказов.


1. Пороги-проценты

SELECT a.obj_id0, a.rec_id, a.percent, round(a.step_max,3), round(a.step_min,3)
  FROM percents_t a 

Здесь a.obj_id0 - id связки на таблицу с номерами заказов
a.rec_id  - id массива
a.percent - % скидки
round(a.step_max,3) - верхнее значение интервала
round(a.step_min,3) - нижнее значение интервала

С уважением, К.Федоров
************************************************************************************/

END PK38_BILLING_DOWNTIME_GUI;
/
