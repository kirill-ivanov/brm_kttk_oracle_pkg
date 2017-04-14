create or replace package PK10_PAYMENT_EISUP_LOAD is

  -- Author  : KHROMYKH
  -- Created : 23.12.2015 14.55.27
  -- Purpose : 
  
  -- ==============================================================================
  c_PkgName   constant varchar2(30) := 'PK10_PAYMENT_EISUP_LOAD';
  -- ==============================================================================
  
  -- Public type declarations
  type t_refc is ref cursor;
  
	/* ******************************************************
	* получить список контрактов
	*/
	function getContractData return t_refc;
	
	/* ****************************************************
	* очистить список платежей
	*/
	procedure Clear_Payments;
	
	/* ****************************************************
	* добавить платеж
	*/
	procedure Add_Payment(
		p_record_id			in number, 
		p_document_id		in varchar2, 
		p_doc_type			in varchar2, 
		p_doc_no			in varchar2, 
		p_doc_date			in date, 
		p_bank_account		in varchar2, 
		p_clnt_account		in varchar2, 
		p_erp_code			in varchar2, 
		p_contract_no		in varchar2, 
		p_account_no		in varchar2, 
		p_document_no		in varchar2, 
		p_document_date		in date, 
		p_payment_date		in date, 
		p_payment_amount	in number, 
		p_currency_id		in number, 
		p_pay_descr			in varchar2, 
		p_period			in number,
		p_journal_id		in number
		);
	/* ****************************************************
	* добавить распределение платежа на счет
	*/
	procedure Add_Transfer(
		p_document_id		in varchar2, 
		p_bill_no			in varchar2, 
		p_bill_date			in date, 
		p_transfer_total	in number, 
		p_operation			varchar2, 
		p_journal_id		number
		);
	
	/* *******************************************************
	* получить ид последнего загруженного журнала
	*/
	function Get_Last_Journal_Loaded return number;
	/* *******************************************************
	* начало загрузки платежей
	*/
	procedure Start_Loading(
		p_journal_start	in date,
		p_journal_stop	in date
		);
	/* ***************************************************
	* окончание загрузки платежей
	*/
	procedure Stop_Loading(
		p_journal_id	in number,
		p_journal_start	in date,
		p_journal_stop	in date
		);





end PK10_PAYMENT_EISUP_LOAD;
/
create or replace package body PK10_PAYMENT_EISUP_LOAD is

/* *******************************************************
* получить ид последнего загруженного журнала
*/
function Get_Last_Journal_Loaded return number is
	l_journal_id	number;
begin
	select max(e.journal_id)
		into l_journal_id
	from eisup_journal_t e;
	return l_journal_id;
exception when no_data_found then
	return -1;
end;

/* *******************************************************
* начало загрузки платежей
*/
procedure Start_Loading(
	p_journal_start	in date,
	p_journal_stop	in date
	) is
  v_prcName       constant varchar2(30) := 'Start_Loading';
	l_journal_stop	date := trunc(p_journal_stop) + 1 - 1/(24*3600);
  v_count         integer;
begin
	-- удалить предыдущие данные загрузки
	---- разноска
	delete from eisup_pay_transfer_tmp e
	where e.journal_id in(
		select journal_id 
		from eisup_journal_t j 
		where j.date_from <= l_journal_stop and j.date_to >=p_journal_start);
  v_count := sql%rowcount;
  Pk01_Syslog.Write_msg( 'EISUP_PAY_TRANSFER_TMP: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName);
	---- плдатежи
	delete from eisup_payment_tmp e
	where e.journal_id in(
		select journal_id 
		from eisup_journal_t j 
		where j.date_from <= l_journal_stop and j.date_to >=p_journal_start);
  v_count := sql%rowcount;
  Pk01_Syslog.Write_msg( 'EISUP_PAYMENT_TMP: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName);
exception
    when others then
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
end;

/* ***************************************************
* окончание загрузки платежей
*/
procedure Stop_Loading(
	p_journal_id	in number,
	p_journal_start	in date,
	p_journal_stop	in date
	) is
  v_prcName       constant varchar2(30) := 'Stop_Loading';
	l_journal_stop	date := p_journal_stop + 1 - 1/(24*3600);
begin
	-- записать новый журнал
	insert into eisup_journal_t(journal_id, date_from, date_to)
	values(p_journal_id, p_journal_start, l_journal_stop);
	commit;
  Pk01_Syslog.Write_msg( 'journal_id =: '||p_journal_id, c_PkgName||'.'||v_prcName);
exception
    when others then
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
end;
/* ******************************************************
* получить список контрактов
*/
function getContractData return t_refc is
  v_prcName constant varchar2(30) := 'getContractData';
	p_refc		t_refc;
  v_retcode integer;
begin
	open p_refc for
		SELECT CS.ERP_CODE, CS.INN, CS.KPP, C.CONTRACT_NO, C.DATE_FROM, A.ACCOUNT_NO, B.BILL_NO, 'brm' billing
		FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACT_T C, CUSTOMER_T CS
		WHERE B.REP_PERIOD_ID >= 201501
			AND B.ACCOUNT_ID  = A.ACCOUNT_ID
			AND B.CONTRACT_ID = C.CONTRACT_ID
			AND B.PROFILE_ID  = AP.PROFILE_ID
			AND AP.CUSTOMER_ID= CS.CUSTOMER_ID
			AND A.BILLING_ID != 2003
			AND A.STATUS      = 'B'
				UNION
		select c.k_kode, c.inn, c.kpp, c.auto_no contract_no, i2d@PINDB(c.cust_date) contract_date, a.account_no, bill_no, 'portal' billing
		from bill_t@PINDB b inner join profile_t@PINDB p on b.account_obj_id0 = p.account_obj_id0
			inner join contract_info_t@PINDB c on p.poid_id0 = c.obj_id0
			inner join account_t@PINDB a on b.account_obj_id0 = a.poid_id0
		where b.rep_date >= d2i@PINDB(to_date('01012015','ddmmyyyy')) and b.due != 0 and b.bill_no is not null;
	rollback;
	return p_refc;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_refc%ISOPEN THEN 
            CLOSE p_refc;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
end;

/* ****************************************************
* очистить список платежей
*/
procedure Clear_Payments is
begin
	execute immediate 'truncate table eisup_payment_transfer';
	delete eisup_payment;
end;

/* ****************************************************
* добавить платеж
*/
procedure Add_Payment(
	p_record_id			in number, 
	p_document_id		in varchar2, 
	p_doc_type			in varchar2, 
	p_doc_no			in varchar2, 
	p_doc_date			in date, 
	p_bank_account		in varchar2, 
	p_clnt_account		in varchar2, 
	p_erp_code			in varchar2, 
	p_contract_no		in varchar2, 
	p_account_no		in varchar2, 
	p_document_no		in varchar2, 
	p_document_date		in date, 
	p_payment_date		in date, 
	p_payment_amount	in number, 
	p_currency_id		in number, 
	p_pay_descr			in varchar2, 
	p_period			in number,
	p_journal_id		in number
	) is
  v_prcName constant varchar2(30) := 'Add_Payment';
begin
	insert into eisup_payment_tmp(
		record_id, document_id, doc_type, doc_no, doc_date, bank_account, 
		clnt_account, erp_code, contract_no, account_no, document_no, document_date, 
		payment_date, payment_amount, currency_id, pay_descr, period, journal_id)
	values(	
		p_record_id, 
		p_document_id, 
		p_doc_type, 
		p_doc_no, 
		p_doc_date, 
		p_bank_account, 
		p_clnt_account, 
		p_erp_code, 
		p_contract_no, 
		p_account_no, 
		p_document_no, 
		p_document_date, 
		p_payment_date, 
		p_payment_amount, 
		p_currency_id, 
		p_pay_descr, 
		p_period,
		p_journal_id);
exception
    when others then
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
end;


/* ****************************************************
* добавить распределение платежа на счет
*/
procedure Add_Transfer(
	p_document_id		in varchar2, 
	p_bill_no			in varchar2, 
	p_bill_date			in date, 
	p_transfer_total	in number, 
	p_operation			varchar2, 
	p_journal_id		number
	) is
  v_prcName constant varchar2(30) := 'Add_transfer';
begin	
	insert into eisup_pay_transfer_tmp(
		document_id, bill_no, bill_date, transfer_total, operation, journal_id)
	values(	
		p_document_id, 
		p_bill_no, 
		p_bill_date, 
		p_transfer_total, 
		p_operation, 
		p_journal_id);
exception
    when others then
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
end;

end PK10_PAYMENT_EISUP_LOAD;
/
