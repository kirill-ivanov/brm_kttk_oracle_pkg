create or replace package PK300_PS_PAYMENT is

  -- Author  : KHROMYKH
  -- Created : 05.03.2014 12.06.34
  -- Purpose : 
  
  -- Public type declarations
	type forcursor is ref cursor;
	c_TIMEZONE		CONSTANT integer := GET_TZ_OFFSET;
	c_PkgName   constant varchar2(30) := 'PK300_PS_PAYMENT';
	
	/* **************************************************************
	* Получить текущий период 
	*/
	FUNCTION Get_period(
		p_position		in varchar2
		) RETURN INTEGER;


	/* ***************************************************************
	* Список платежей по лицевому счету
	*   - положительное - кол-во выбранных записей
	*   - при ошибке выставляет исключение
	*/
	procedure Web_account_payment_list (
               p_account_id IN INTEGER,   -- ID лицевого счета
               p_period_from	in date,
               p_period_to		in date,
               p_payments 		OUT forcursor,
							 p_ajusts				out forcursor
							 );
	/* ************************************************************
	* поиск платежа
	*/
	function Find_Bill_Payment(
		p_ps_id			in integer,
		p_receipt		in varchar2,
		p_payment_id	out number
		) return varchar2;
	/* ************************************************************
	* поиск абонента
	*/
	function Find_AbonentID_By_Phone(
		p_phone			in varchar2,
		p_ps_id			in number,
		p_date			in date
		) return integer;

	/* ************************************************************
	* проведение платежа
	*/ 
	function Add_payment(
		p_account_id		in number,
		p_phone					in varchar2,
		p_payment_date	in date,
		p_amount				in number,
		p_ps_id					in number,
		p_receipt				in varchar2
		) return number;
		
	/* **************************************************************
	* запись в лог
	*/
	procedure Write_Log(
		p_abonent_id		in varchar2,
		p_receipt				in varchar2,
		p_payment_date	in date,
		p_amount				in number,
		p_ps_id					in number,
		p_action				in varchar2,
		p_result				in varchar2
		);
	
	/* *********************************************************************
	* получить данные платежа
	*/
	procedure Web_Payment_Data(
			p_payment_id		in number,
			p_period_id			in number,
			p_amount				out number,
			p_payment_date	out date,
			p_create_date		out date,
			p_desc					out varchar2,
			p_receipt				out varchar2,
			p_trans_amount	out number,
			p_balance				out number,
			p_type					out varchar2,
			p_operator			out varchar2,
			p_bills					out forcursor,
			p_operations		out forcursor,
			p_ps_id				  out number,
			p_ps_name				out varchar2,
      p_notes					out varchar2
			);


end PK300_PS_PAYMENT;
/
create or replace package body PK300_PS_PAYMENT is

	v_retcode       	INTEGER;
	c_PERIOD_LAST 		CONSTANT varchar2(4) := 'LAST';  -- последний закрытый период
    c_PERIOD_OPEN 		CONSTANT varchar2(4) := 'OPEN';  -- текущий период
    c_PERIOD_NEXT 		CONSTANT varchar2(4) := 'NEXT';  -- следующий за текущим период
    c_PERIOD_BILL 		CONSTANT varchar2(4) := 'BILL';		

	c_PAY_STATE_OPEN	CONSTANT varchar2(20) := 'OPEN';     -- платеж принадлежит открытому периоду
	
	c_PAY_TYPE_OPEN		CONSTANT varchar2(20) := 'OPEN';    -- входящий платеж от "Нового биллинга"
    c_PAY_TYPE_PAYMENT	CONSTANT varchar2(20) := 'PAYMENT'; -- платеж 
    c_PAY_TYPE_ADJUST	CONSTANT varchar2(20) := 'ADJUST';  -- корректировка
    c_PAY_TYPE_REVERS	CONSTANT varchar2(20) := 'REVERS';  -- сторнирующий платеж
    c_PAY_TYPE_REFUND	CONSTANT varchar2(20) := 'REFUND';  -- возврат
    c_PAY_TYPE_MOVE		CONSTANT varchar2(20) := 'MOVE';    -- перенос
	
/* **************************************************************
* вернуть правильный номер
*/
function Get_True_phone(
	p_phone in varchar2) return varchar2 is
begin
	if p_phone is null then
		return null;
	elsif	length(p_phone) = 10 then
		return '7' || p_phone;
	elsif length(p_phone) < 10 or length(p_phone) > 11 then
		return null;
	else
		return p_phone;
	end if;
end;
	
/* **************************************************************
* Получить текущий период 
*/
FUNCTION Get_period(
	p_position		in varchar2
	) RETURN INTEGER
IS
		v_period	INTEGER;
BEGIN
		SELECT period_id
			INTO v_period
		FROM period_t
		WHERE position = p_position;
		--
		RETURN v_period;
EXCEPTION 
		WHEN OTHERS THEN
			RETURN NULL;
END;

/* ************************************************************
* поиск платежа
*/
function Find_Bill_Payment(
	p_ps_id				in integer,
	p_receipt			in varchar2,
	p_payment_id	out number
	) return varchar2 is
	
	v_last_period_id		number;
	v_open_period_id		number;
	v_outcode						varchar2(1024) := 'OK';
	v_prcName					varchar2(100) := 'Find_Bill_Payment';
begin
	--
	select p.payment_id into p_payment_id
	from payment_t p, period_t pp
	where p.rep_period_id = pp.period_id
		and pp.position in(c_PERIOD_LAST, c_PERIOD_OPEN, c_PERIOD_BILL)
		and p.paysystem_id = p_ps_id and p.doc_id = p_receipt;
	
	return v_outcode; 
exception 
	when too_many_rows then
		return 'OK';
	when no_data_found then
		return 'not found';
	when others then
		Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
end;

/* ************************************************************
* поиск абонента
*/
function Find_AbonentID_By_Phone(
	p_phone			in varchar2,
	p_ps_id			in number,
	p_date			in date
	) return integer is
	v_date 					date := nvl(p_date + c_TIMEZONE, sysdate) ;
	v_account_id		integer;
	v_prcName				varchar2(100) := 'Find_AbonentID_By_Phone';
	v_phone					varchar2(50) := get_true_phone(p_phone);
begin
	if v_phone is null or v_phone ='71111111111' then
		return null;
	end if;
	v_account_id := pk10_payment.Find_account_by_phone(v_phone, v_date);
	if v_account_id is not null then
		Write_Log(v_phone, null, null, null, p_ps_id, 'check', 'OK');
	else
		Write_Log(v_phone, null, null, null, p_ps_id, 'check', 'not found');
	end if;
	return v_account_id;
exception when others then
		Write_Log(p_phone, null, null, null, p_ps_id, 'check', sqlerrm);
		Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
end;
	

/* ************************************************************
* проведение платежа
*/ 
function Add_payment(
	p_account_id		in number,
	p_phone					in varchar2,
	p_payment_date	in date,
	p_amount				in number,
	p_ps_id					in number,
	p_receipt				in varchar2
	) return number is
	v_payment_id			number;
	v_prcName					varchar2(100) := 'Add_payment';
	v_rep_period_id		number := Get_period(c_PERIOD_OPEN);
	v_payment_date		date := p_payment_date + c_TIMEZONE;
	v_res							varchar2(1024);
	v_phone 					varchar2(100) := Get_True_phone(p_phone);
begin
	-- проверить наличие платежа
	v_res := Find_Bill_Payment(p_ps_id, p_receipt, v_payment_id);
	if v_res ='OK' then
			Write_Log(v_phone, p_receipt, p_payment_date, p_amount, p_ps_id, 'payment', 'Платеж существует');
	else
		v_payment_id := pk10_payment.Add_payment(
										p_account_id, v_rep_period_id, v_payment_date, c_PAY_TYPE_PAYMENT,
										p_amount, p_ps_id, p_receipt, c_PAY_STATE_OPEN, 'payment_gate', v_phone);
		Write_Log(v_phone, p_receipt, p_payment_date, p_amount, p_ps_id, 'payment', 'OK');
	end if;
	return v_payment_id;
exception when others then
	Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
end;

/* **************************************************************
* запись в лог
*/
procedure Write_Log(
	p_abonent_id		in varchar2,
	p_receipt				in varchar2,
	p_payment_date	in date,
	p_amount				in number,
	p_ps_id					in number,
	p_action				in varchar2,
	p_result				in varchar2
	) is
	PRAGMA AUTONOMOUS_TRANSACTION;
begin
	insert into tmp_eps_payment_log(
		abonent_id, receipt, payment_date, amount, ps_id, action, result)
	values(
			p_abonent_id, p_receipt, p_payment_date, p_amount, p_ps_id, p_action, p_result
	);
	commit;
exception when others then
	null;		
end;

-- ------------------------------------------------------------------------ --
-- Список платежей по лицевому счету
--   - положительное - кол-во выбранных записей
--   - при ошибке выставляет исключение
procedure Web_account_payment_list (
               p_account_id IN INTEGER,   -- ID лицевого счета
               p_period_from	in date,
               p_period_to		in date,
               p_payments 		OUT forcursor,
							 p_ajusts				out forcursor
           ) is
    v_prcName       CONSTANT VARCHAR2(30) := 'Web_account_payment_list';
    v_min_period_id INTEGER;
    v_max_period_id INTEGER;
BEGIN
    -- выставляем границы диапазона
		if p_period_from is null then
			-- выбрать все платежи 
			v_min_period_id := 200001;
		else
			v_min_period_id := to_number(to_char(p_period_from, 'yyyymm'));
		end if;
		if p_period_to is null then
			-- выбрать все платежи 
			v_max_period_id := 205001;
		else
			v_max_period_id := to_number(to_char(p_period_to, 'yyyymm'));
		end if;
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_payments FOR
          SELECT 
						p.create_date, 			-- дата зачисления
						p.payment_date,			-- дата платежа
						p.recvd,						-- сумма платежа
						p.transfered,				-- распределено
						p.balance,					-- не распределено
						ps.paysystem_name,	-- ПС
						(select name from dictionary_t d 
						where parent_id = 62 and p.payment_type=d.key)
						/*
						case to_char(p.payment_type)
							when 'OPEN' then 'входящий платеж от "Нового биллинга'
    						when 'PAYMENT' then 'Платеж'
							when 'ADJUST' then 'Корректировка'
    						when 'REVERS' then 'Сторнирующий платеж'
    						when 'REFUND' then 'Возврат'
    						when 'MOVE' then 'Перенос'
							when 'IMP' then 'Импорт'
						end*/ payment_type,
						p.pay_descr,				-- описание
						p.doc_id,						-- номер транзакции
						p.payment_id,				-- id платежа
						p.notes,						-- примечание
            			p.rep_period_id,
						(select count(1) from payment_operation_t o 
							where o.src_payment_id=p.payment_id or o.dst_payment_id=p.payment_id
								and rownum <2) marked,
           A.advance_id
          FROM PAYMENT_T P, PAYSYSTEM_T PS, ADVANCE_T A, ADVANCE_ITEM_T AI
          WHERE p.ACCOUNT_ID = p_account_id
            AND p.REP_PERIOD_ID BETWEEN v_min_period_id AND v_max_period_id
            AND P.PAYSYSTEM_ID = PS.PAYSYSTEM_ID
						and P.PAYMENT_TYPE IN (c_PAY_TYPE_PAYMENT, 'IMP')
            AND P.PAYMENT_ID     = AI.PAYMENT_ID(+)
            AND P.REP_PERIOD_ID  = AI.PAY_PERIOD_ID(+)
            AND AI.ADVANCE_ID    = A.ADVANCE_ID(+)
          ORDER BY create_date DESC;
          
			open p_ajusts for
				SELECT 
						p.create_date, 			-- дата зачисления
						p.payment_date,			-- дата платежа
						p.recvd,						-- сумма платежа
						p.transfered,				-- распределено
						p.balance,					-- не распределено
						ps.paysystem_name,	-- ПС
						(select name from dictionary_t d 
						where parent_id = 62 and p.payment_type=d.key)
						/*
						case to_char(p.payment_type)
							when 'OPEN' then 'входящий платеж от "Нового биллинга'
    						when 'PAYMENT' then 'Платеж'
							when 'ADJUST' then 'Корректировка'
    						when 'REVERS' then 'Сторнирующий платеж'
    						when 'REFUND' then 'Возврат'
    						when 'MOVE' then 'Перенос'
							when 'IMP' then 'Импорт'
						end*/ payment_type,
						p.pay_descr,				-- описание
						p.doc_id,						-- номер транзакции
						p.payment_id,				-- id платежа
            			p.rep_period_id,
						p.notes,						-- примечание
						(select count(1) from payment_operation_t o 
							where o.src_payment_id=p.payment_id or o.dst_payment_id=p.payment_id
								and rownum <2) marked,
            a.advance_id
          FROM PAYMENT_T P, PAYSYSTEM_T PS, ADVANCE_T A, ADVANCE_ITEM_T AI
          WHERE p.ACCOUNT_ID = p_account_id
            AND p.REP_PERIOD_ID BETWEEN v_min_period_id AND v_max_period_id
            AND P.PAYSYSTEM_ID = PS.PAYSYSTEM_ID
						and p.payment_type not in(c_PAY_TYPE_PAYMENT, 'IMP')
            AND P.PAYMENT_ID   = AI.PAYMENT_ID(+)
            AND P.REP_PERIOD_ID= AI.PAY_PERIOD_ID(+)
            AND AI.ADVANCE_ID  = A.ADVANCE_ID(+)
          ORDER BY create_date DESC;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_payments%ISOPEN THEN 
            CLOSE p_payments;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;  

/* *********************************************************************
* получить данные платежа
*/
procedure Web_Payment_Data(
	p_payment_id		in number,
	p_period_id			in number,
	p_amount				out number,
	p_payment_date	out date,
	p_create_date		out date,
	p_desc					out varchar2,
	p_receipt				out varchar2,
	p_trans_amount	out number,
	p_balance				out number,
	p_type					out varchar2,
	p_operator			out varchar2,
	p_bills					out forcursor,
	p_operations		out forcursor,
	p_ps_id				  out number,
	p_ps_name				out varchar2,
  p_notes					out varchar2
	) is
	v_prcName 			varchar2(100) := 'PK300_PS_PAYMENT.WEB_PAYMENT_DATA';
begin
	-- данные платежа
	select 
		p.recvd, p.payment_date, p.create_date, p.pay_descr,
		p.doc_id, p.transfered, p.balance, p.created_by,
		ps.paysystem_id, ps.paysystem_name, p.payment_type, p.notes
	into 
		p_amount, p_payment_date, p_create_date, p_desc,
		p_receipt, p_trans_amount, p_balance, p_operator,
		p_ps_id, p_ps_name, p_type,p_notes
	from payment_t p, paysystem_t ps
	where p.payment_id = p_payment_id and p.rep_period_id=p_period_id
		and p.paysystem_id=ps.paysystem_id(+);
	-- счета
	open p_bills for
		select pt.transfer_id, pt.transfer_total, pt.transfer_date, b.bill_no, b.bill_date, b.total
		from pay_transfer_t pt, bill_t b
		where pt.payment_id = p_payment_id and pt.pay_period_id=p_period_id
			and pt.bill_id = b.bill_id and pt.rep_period_id=b.rep_period_id
		order by b.bill_id;
	-- операции
	open p_operations for
		select o.*, t.oper_name,
			(select account_no || chr(9) || doc_id 
				from payment_t p, account_t a
				where p.account_id=a.account_id and p.payment_id=o.src_payment_id and p.rep_period_id=o.src_rep_period_id) src_payment,
			(select account_no || chr(9) || doc_id 
				from payment_t p, account_t a
				where p.account_id=a.account_id and p.payment_id=o.dst_payment_id and p.rep_period_id=o.dst_rep_period_id) dst_payment
		from payment_operation_t o, payment_operation_type_t t
		where (o.src_payment_id = p_payment_id and o.src_rep_period_id = p_period_id
			or o.dst_payment_id = p_payment_id and o.dst_rep_period_id = p_period_id)
			and o.oper_type_id=t.oper_type_id
		order by o.oper_id;
exception 
	when no_data_found then
		v_retcode := Pk01_SysLog.Fn_write_Error('Payment not found', c_PkgName||'.'||v_prcName);
		RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
	when others then
		v_retcode := Pk01_SysLog.Fn_write_Error(sqlerrm, c_PkgName||'.'||v_prcName);
		RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;



end PK300_PS_PAYMENT;
/
