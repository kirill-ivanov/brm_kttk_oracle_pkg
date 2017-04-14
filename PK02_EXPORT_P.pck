CREATE OR REPLACE PACKAGE PK02_EXPORT_P IS
    --
    -- Author  : SMAKEEV
    -- Created : 05.02.2014 12:31:19
    -- Purpose : Экспорт абонентов физ.лиц
    -- ==============================================================================
    c_PkgName   CONSTANT varchar2(30) := 'PK02_EXPORT_P';
    -- ==============================================================================
    c_RET_OK    CONSTANT integer := 0;
    c_RET_ER    CONSTANT integer :=-1;
    
    TYPE t_refc IS REF CURSOR;
    
    -- Статусы лицевых счетов в новом биллинге
    c_ACCOUNT_OPEN  CONSTANT INTEGER := 10100;
    c_ACCOUNT_LOCK  CONSTANT INTEGER := 10102;
    c_ACCOUNT_CLOSE CONSTANT INTEGER := 10103;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- полный экспорт данных
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Full_Export;
    
    -- экспорт данных об абоненте физ.лице:
    --   - при ошибке выставляет исключение
    PROCEDURE Exp_subs_info;

    -- экспорт данных о заказах абонента физ.лице:
    --   - при ошибке выставляет исключение
    PROCEDURE Exp_subs_order;

    -- экспорт данных о платежах абонента, за указанный период, возвращает:
    -- - на входе интервалы POID для месяца - берем из описания секций
    -- - при ошибке выставляет исключение
    PROCEDURE Exp_payments( p_month  IN DATE );

    -- экспорт данных о входящем балансе на начало указанного месяца
    -- - при ошибке выставляет исключение
    -- 2 вар-т (время работы ~1 час)
    PROCEDURE Exp_open_balance(
                      p_month IN DATE 
                  );
                  
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- полезные функции
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- получить верхнюю границу ключа секции
    FUNCTION Get_part_high_value(
                      p_month  IN DATE,
                      p_table  IN VARCHAR2
                  )RETURN INTEGER;
    
    -- получить сумму по выставленным счетам на указанное время
    FUNCTION Get_account_billed(
                      p_account_no IN VARCHAR2,
                      p_date       IN DATE 
                  ) RETURN NUMBER;

    -- получить сумму по корректировкам на указанное время
    FUNCTION Get_account_adjusted(
                      p_account_no IN VARCHAR2,
                      p_date       IN DATE 
                  )  RETURN NUMBER;

    -- получить сумму платежей на указанное время
    FUNCTION Get_account_payed(
                      p_account_no IN VARCHAR2,
                      p_date       IN DATE 
                  )  RETURN NUMBER;

END PK02_EXPORT_P;
/
CREATE OR REPLACE PACKAGE BODY PK02_EXPORT_P IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- полный экспорт данных
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Full_Export
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Full_Export';
    v_count    INTEGER;
    v_period   DATE := TO_DATE('01.01.2014','dd.mm.yyyy');
BEGIN
    Pk01_Syslog.Write_to_log('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);

    -- экспорт данных об абоненте физ.лице:
    Exp_subs_info;
    COMMIT;
    
    -- экспорт данных о заказах абонента физ.лице:
    Exp_subs_order;
    COMMIT;
    
    -- экспорт данных о платежах абонента, за указанный период, возвращает:
    -- платежи за январь 2014
    Exp_payments( v_period );
    COMMIT;
    
    -- экспорт данных о входящем балансе на начало указанного месяца
    -- 2 вар-т (время работы ~1 час)
    Exp_open_balance( v_period );
    COMMIT;
    
    Pk01_Syslog.Write_to_log('Stop. '||v_count||' rows inserted.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    COMMIT;    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.Err_to_log('ERROR', c_PkgName||'.'||v_prcName );
        ROLLBACK;
END;

-- экспорт данных об абоненте физ.лице:
--   - при ошибке выставляет исключение
PROCEDURE Exp_subs_info
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Exp_subs_info';
    v_count    INTEGER;
BEGIN
    Pk01_Syslog.Write_to_log('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    --
    DELETE FROM P_SUBS_INFO_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_to_log('P_SUBS_INFO_T '||v_count||' rows deleted', 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    --
    INSERT INTO P_SUBS_INFO_T
     SELECT a.account_no,
            an1.last_name,
            an1.first_name,
            an1.middle_name,
            c.contract_num contract_no,
            TRUNC (pin.u2d (c.valid_from)) contract_date,
            b.NAME brand_name,
            pci.service_provider_id0 service_provider,
            an1.zip reg_zip,
            an1.state reg_reg,
            an1.city reg_city,
            an1.address reg_addr,
            pii.zip bill_zip,
            pii.state bill_reg,
            pii.city bill_city,
            pii.address bill_addr,
            an2.zip set_zip,
            an2.state set_reg,
            an2.city set_city,
            an2.address set_addr,
            (SELECT ap.phone
               FROM pin.account_phones_t ap
              WHERE ap.obj_id0 = a.poid_id0 AND ap.rec_id = 1 AND ap.TYPE = 1)
               contact_phone,
            siebel8.GET_EXT_SRC_BY_CONTRACT@siebel8 (c.contract_num)
               ext_source,
            siebel8.GET_EXT_ID_BY_CONTRACT@siebel8 (c.contract_num)
               ext_id,
            a.status
       FROM pin.account_t a,
            pin.account_t b,
            pin.billinfo_t bi,
            pin.account_nameinfo_t an1,
            pin.account_nameinfo_t an2,
            pin.contract_t c,
            pin.profile_t p,
            pin.profile_contract_info_t pci,
            pin.payinfo_t pi,
            pin.payinfo_inv_t pii
      WHERE     bi.poid_id0 = bi.ar_billinfo_obj_id0
            AND bi.account_obj_id0 = a.poid_id0
            AND a.business_type = 1
--            AND a.status != 10103  -- выбираем только открытые счета
            AND b.poid_id0 = a.brand_obj_id0
            AND an1.obj_id0(+) = a.poid_id0
            AND an1.rec_id(+) = 1
            AND an2.obj_id0(+) = a.poid_id0
            AND an2.rec_id(+) = 1
            AND c.poid_id0(+) = a.contract_obj_id0
            AND p.account_obj_id0(+) = a.poid_id0
            AND p.poid_type = '/profile/contract_info'
            AND pci.obj_id0(+) = p.poid_id0
            AND pi.account_obj_id0(+) = a.poid_id0
            AND pi.poid_type(+) = '/payinfo/invoice'
            AND pii.obj_id0(+) = pi.poid_id0
            AND pii.rec_id(+) = 0;
    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_to_log('Stop. '||v_count||' rows inserted.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.Err_to_log('ERROR', c_PkgName||'.'||v_prcName );
END;

-- экспорт данных о заказах абонента физ.лице:
--   - при ошибке выставляет исключение
PROCEDURE Exp_subs_order
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Exp_subs_order';
    v_count    INTEGER;
BEGIN
    Pk01_Syslog.Write_to_log('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);

    DELETE FROM P_SUBS_ORDER_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_to_log('P_SUBS_ORDER_T '||v_count||' rows deleted', 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    --
    INSERT INTO P_SUBS_ORDER_T
     SELECT ap.account_no,
            sal.NAME anumber,
            pci.order_num order_no,
            TRUNC (pin.u2d (p.effective_t)) order_date,
            (SELECT DISTINCT p.NAME
               FROM pin.purchased_product_t pp, pin.plan_t p
              WHERE     pp.account_obj_id0 = ac.poid_id0
                    AND pp.service_obj_id0 = s.poid_id0
                    AND pp.status IN (1, 2)
                    AND pp.plan_obj_id0 = p.poid_id0            --AND ROWNUM = 1
                                                    )
               plan_name,
            (SELECT COUNT (1)
               FROM pin.purchased_product_t pp, pin.plan_t p
              WHERE     pp.account_obj_id0 = ac.poid_id0
                    AND pp.service_obj_id0 = s.poid_id0
                    AND pp.status IN (1, 2)
                    AND pp.plan_obj_id0 = p.poid_id0)
               plan_count,
            siebel8.GET_CURATOR_FIO_BY_ORDER@siebel8 (pci.order_num)
               curator_fio
       FROM pin.account_t ac,
            pin.account_t ap,
            pin.billinfo_t bic,
            pin.billinfo_t bip,
            pin.service_t s,
            pin.service_alias_list_t sal,
            pin.profile_t p,
            pin.profile_contract_info_t pci
      WHERE     ac.business_type = 1
            AND ac.status != 10103
            AND s.account_obj_id0 = ac.poid_id0
            AND s.poid_type = '/service/telco/gsm/telephony'
            AND sal.obj_id0 = s.poid_id0
            AND p.account_obj_id0 = ac.poid_id0
            AND pci.obj_id0 = p.poid_id0
            AND ac.poid_id0 = bic.account_obj_id0
            AND bic.ar_billinfo_obj_id0 = bip.poid_id0
            AND bip.account_obj_id0 = ap.poid_id0;

    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_to_log('Stop. '||v_count||' rows inserted.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.Err_to_log('ERROR', c_PkgName||'.'||v_prcName );
END;

--
-- получить верхнюю границу ключа секции
--
FUNCTION Get_part_high_value(
                  p_month  IN DATE,
                  p_table  IN VARCHAR2
              )RETURN INTEGER
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Get_part_high_value';
    v_pname     VARCHAR2(100);
    v_poid_id0  INTEGER;
BEGIN
    v_pname := 'P_R_'||TO_CHAR(TRUNC(p_month,'mm'),'MMDDYYYY');  
    --
    SELECT HIGH_VALUE 
      INTO v_poid_id0
      FROM ALL_TAB_PARTITIONS
     WHERE TABLE_OWNER = 'PIN'
       AND TABLE_NAME  = p_table
       AND PARTITION_NAME = v_pname;
    RETURN v_poid_id0;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.Err_to_log(p_table||'('||v_pname||')', c_PkgName||'.'||v_prcName );
        RAISE;
END;

-- экспорт данных о платежах абонента, за указанный период, возвращает:
-- - на входе интервалы POID для месяца - берем из описания секций
-- - при ошибке выставляет исключение
PROCEDURE Exp_payments( p_month  IN DATE )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'Exp_payments';
    v_count     INTEGER;
    v_poid_ent_min  PIN.EVENT_T.POID_ID0%TYPE;
    v_poid_ent_max  PIN.EVENT_T.POID_ID0%TYPE;
    v_poid_e_min    PIN.EVENT_BAL_IMPACTS_T.OBJ_ID0%TYPE;
    v_poid_e_max    PIN.EVENT_BAL_IMPACTS_T.OBJ_ID0%TYPE;
    v_poid_po_min   PIN.EVENT_BILLING_PAYMENT_PAYORD_T.OBJ_ID0%TYPE;
    v_poid_po_max   PIN.EVENT_BILLING_PAYMENT_PAYORD_T.OBJ_ID0%TYPE;
    v_pdate_from    DATE;
    v_pdate_to      DATE;
BEGIN
    Pk01_Syslog.Write_to_log('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    --
    v_pdate_from := ADD_MONTHS(TRUNC(p_month,'mm'),-1); -- верхняя граница предыдущего периода
    v_pdate_to   := TRUNC(p_month,'mm'); -- верхняя граница текущего периода
    
    Pk01_Syslog.Write_to_log('date_from='||TO_CHAR(v_pdate_from,'yyyymmdd')||
                             ', date_to='||TO_CHAR(v_pdate_to,  'yyyymmdd'), 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    
    v_poid_ent_min := Get_part_high_value( v_pdate_from, 'EVENT_T');
    v_poid_ent_max := Get_part_high_value( v_pdate_to, 'EVENT_T')-1;
    
    Pk01_Syslog.Write_to_log('EVENT_T from='||v_poid_ent_min||', date_to='||v_poid_ent_max, 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    
    v_poid_e_min := Get_part_high_value( v_pdate_from, 'EVENT_BAL_IMPACTS_T');
    v_poid_e_max := Get_part_high_value( v_pdate_to, 'EVENT_BAL_IMPACTS_T')-1;
    
    Pk01_Syslog.Write_to_log('EVENT_BAL_IMPACTS_T from='||v_poid_e_min||', date_to='||v_poid_e_max, 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    
    v_poid_po_min := Get_part_high_value( v_pdate_from, 'EVENT_BILLING_PAYMENT_PAYORD_T');
    v_poid_po_max := Get_part_high_value( v_pdate_to, 'EVENT_BILLING_PAYMENT_PAYORD_T')-1;
    
    Pk01_Syslog.Write_to_log('EVENT_BILLING_PAYMENT_PAYORD_T from='||v_poid_po_min||', date_to='||v_poid_po_max, 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    
 --   DELETE FROM p_payment_t;
 --   v_count := SQL%ROWCOUNT;
 --   Pk01_Syslog.Write_to_log('P_PAYMENT_T '||v_count||' rows deleted', 
 --                            c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    
    INSERT INTO p_payment_t 
    SELECT 
        mm.poid_id0      pay_poid_id0,                    -- POID платежа
        TO_NUMBER(TO_CHAR(date_t,'yyyymm')) rep_period_id,-- ID отчетного периода   
        mm.date_t        payment_date,                    -- дата платежа (из документа) 
        mm.ACCOUNT_NO    account_no,                      -- Номер Л/С
        -1*mm.amount     pay_amount,                      -- cумма платежа(из документа)
        -1*mm.balance    pay_balance,                     -- баланс после разноски
        -1*mm.transfered pay_transfered,                  -- разнесено
        mm.bank_code     bank_code,                       -- название платежной системы
        mm.doc_id        doc_id,                          -- № платежки
        created_t        created_t,                       -- создан в системе 
        mod_t            modify_t,                        -- модифицирован
        mm.descr         pay_descr                        -- текстовое назначение платежа
    FROM (
            SELECT
            i.poid_id0,
            a.account_no,
            po.bank_code,
            SUBSTR(ent.descr,1,1024) descr,
            po.order_id doc_id,
            pin.u2d(po.tstamp_val) date_t,
            pin.u2d(ent.created_t) created_t,
            pin.u2d(ent.mod_t) mod_t,
            i.item_total+i.recvd amount,
            i.transfered,
            ent.poid_type,
            i.due balance
        FROM
            pin.item_t i,
            pin.account_t a,
            pin.event_bal_impacts_t e,
            pin.EVENT_BILLING_PAYMENT_PAYORD_T po,
            pin.event_t ent
        WHERE a.business_type = 1
          AND i.account_obj_id0 = a.poid_id0
          AND i.bill_obj_id0 = 0
          AND ent.poid_id0 = e.obj_id0
          AND e.item_obj_id0 = i.poid_id0
          AND po.obj_id0 = e.obj_id0
          AND i.item_total+i.recvd <> 0
          AND a.poid_id0 <> '251460'          -- нераспознанные платежи
          AND e.obj_id0    BETWEEN v_poid_e_min AND v_poid_e_max
          AND ent.poid_id0 BETWEEN v_poid_ent_min AND v_poid_ent_max
          AND po.obj_id0   BETWEEN v_poid_po_min AND v_poid_po_max
    ) mm;
    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_to_log('Stop. '||v_count||' rows inserted.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.Err_to_log('ERROR', c_PkgName||'.'||v_prcName );
END;

-- экспорт данных о входящем балансе на начало указанного месяца
-- - при ошибке выставляет исключение
PROCEDURE Exp_open_balance_dump(
                  p_month IN DATE 
              )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Exp_open_balance_dump';
    v_count    INTEGER;
    v_period   DATE;
BEGIN
    Pk01_Syslog.Write_to_log('Start. Period '||TO_CHAR(p_month,'dd.mm.yyyy'), c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    v_period := TRUNC(p_month, 'mm');
    --
    DELETE FROM P_SUBS_PERIOD_INFO_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_to_log('P_SUBS_PERIOD_INFO_T '||v_count||' rows deleted', 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    --
    INSERT INTO P_SUBS_PERIOD_INFO_T (
           ACCOUNT_NO, OPEN_BALANCE, BILLED, ADJUSTED, RECVD, PERIOD)
    SELECT ACCOUNT_NO, BILLED+PAYED+ADJUSTED OPEN_BALANCE, 
           BILLED, ADJUSTED, PAYED, v_period
    FROM (
        SELECT a.account_no, 
          PK02_EXPORT_P.Get_account_billed(a.account_no,TRUNC(SYSDATE,'MM')) billed,
          PK02_EXPORT_P.Get_account_payed(a.account_no,TRUNC(SYSDATE,'MM')) payed,
          PK02_EXPORT_P.Get_account_adjusted(a.account_no,TRUNC(SYSDATE,'MM')) adjusted
        FROM P_SUBS_INFO_T a
    );
    --    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_to_log('Stop. '||v_count||' rows inserted.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.Err_to_log('ERROR', c_PkgName||'.'||v_prcName );
END;



-- экспорт данных о входящем балансе на начало указанного месяца
-- - при ошибке выставляет исключение
-- 2 вар-т (время работы ~1 час)
PROCEDURE Exp_open_balance(
                  p_month IN DATE 
              )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Exp_open_balance';
    v_count    INTEGER;
    v_period   DATE;
BEGIN
    Pk01_Syslog.Write_to_log('Start. Period '||TO_CHAR(p_month,'dd.mm.yyyy'), c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    v_period := TRUNC(p_month, 'mm');
    --
 --   DELETE FROM P_SUBS_PERIOD_INFO_T;
 --   v_count := SQL%ROWCOUNT;
 --   Pk01_Syslog.Write_to_log('P_SUBS_PERIOD_INFO_T '||v_count||' rows deleted', 
 --                            c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
                             
 --   COMMIT;
    
    -- очищаем временную таблицу
    EXECUTE IMMEDIATE
        'TRUNCATE TABLE TMP_ITEM_T DROP STORAGE';

    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
        
    -- выбираем данные по item_t
    INSERT INTO TMP_ITEM_T 
        (poid_id0, item_total, recvd, ar_billinfo_obj_id0, effective_t, poid_type)
    SELECT i.poid_id0, i.item_total, i.recvd, i.ar_billinfo_obj_id0, i.effective_t, i.poid_type
      FROM pin.item_t i
     WHERE i.poid_type IN ('/item/payment','/item/adjustment')
       AND i.bill_obj_id0 = 0;
    v_count := SQL%ROWCOUNT;   
    Pk01_Syslog.Write_to_log('TMP_ITEM_T '||v_count||' rows inserted', 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    
    Gather_Table_Stat(l_Tab_Name => 'TMP_ITEM_T');
    
    -- get account billed
    INSERT INTO p_subs_period_info_t (account_no, open_balance, 
              billed, 
              adjusted, recvd, 
              period)     
     SELECT /*+ full(b) */
            am.account_no, NULL,
            NVL(SUM(b.current_total + b.subords_total), 0),
            0, 0, 
            v_period -- TRUNC(SYSDATE,'MM')
      FROM mdv_adm.p_subs_info_t am,
           pin.account_t a,
           pin.bill_t b 
     WHERE b.end_t > 0 
       AND b.end_t <= pin.d2u(v_period) -- TRUNC(SYSDATE,'MM')
       AND b.account_obj_id0 = a.poid_id0
       AND a.account_no = am.account_no
     GROUP BY am.account_no;
     
    v_count := SQL%ROWCOUNT;  
    Pk01_Syslog.Write_to_log('P_SUBS_PERIOD_INFO_T '||v_count||' rows inserted', 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    
   -- добавляем счета, которые не попали в пред. запрос
    INSERT INTO p_subs_period_info_t (account_no, open_balance, 
              billed, adjusted, recvd, 
              period)           
     SELECT am.account_no, NULL,
            0, 0, 0, 
            v_period  -- TRUNC(SYSDATE,'MM')
      FROM mdv_adm.p_subs_info_t am
     WHERE NOT EXISTS (SELECT 1
                         FROM p_subs_period_info_t t       
                        WHERE t.account_no = am.account_no);
    
    v_count := v_count + SQL%ROWCOUNT;
    Pk01_Syslog.Write_to_log('P_SUBS_PERIOD_INFO_T '||v_count||' rows inserted', 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    
    Gather_Table_Stat(l_Tab_Name => 'P_SUBS_PERIOD_INFO_T');    
     
     -- Get account payed
     MERGE INTO p_subs_period_info_t i
     USING (SELECT /*+ ordered */
                   am.account_no, 
                   NVL(SUM(i.item_total + i.recvd), 0) payed 
              FROM mdv_adm.p_subs_info_t am,
                   pin.account_t a,
                   pin.billinfo_t bi,
                   TMP_ITEM_T i, --pin.item_t i, 
                   pin.event_bal_impacts_t ebi, 
                   pin.event_billing_payment_payord_t ebpp
             WHERE a.account_no = am.account_no
               AND a.poid_id0 = bi.account_obj_id0 
               AND i.poid_type = '/item/payment'
           --    AND i.bill_obj_id0 = 0
               AND bi.poid_id0 = i.ar_billinfo_obj_id0
               AND ebi.rec_id = 0
               AND i.poid_id0 = ebi.item_obj_id0
               AND ebpp.rec_id = 0
               AND ebpp.tstamp_val < pin.d2u(v_period) -- TRUNC(SYSDATE,'MM')
               AND ebi.obj_id0 = ebpp.obj_id0 
             GROUP BY am.account_no
           ) p  
     ON (i.account_no = p.account_no)
     WHEN MATCHED THEN UPDATE
      SET i.recvd = p.payed;  
    
    Pk01_Syslog.Write_to_log('P_SUBS_PERIOD_INFO_T - Get account payed', 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    
     -- Get account adjusted  
     MERGE INTO p_subs_period_info_t i
     USING (  
        SELECT am.account_no,
               NVL(SUM(i.item_total), 0) adjusted
          FROM  TMP_ITEM_T i, --pin.item_t i, 
               pin.account_t a, 
               pin.billinfo_t bi,
               mdv_adm.p_subs_info_t am 
         WHERE i.effective_t < pin.d2u(v_period) -- TRUNC(SYSDATE,'MM')
           AND i.poid_type = '/item/adjustment'
           --AND i.bill_obj_id0 = 0
           AND i.ar_billinfo_obj_id0 = bi.poid_id0
           AND bi.account_obj_id0 = a.poid_id0
           AND a.account_no = am.account_no
         GROUP BY am.account_no 
        ) p
     ON (i.account_no = p.account_no)
     WHEN MATCHED THEN UPDATE
      SET i.adjusted = p.adjusted;
    
    Pk01_Syslog.Write_to_log('P_SUBS_PERIOD_INFO_T - Get account adjusted', 
                             c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    
     -- Получаем итоги
     UPDATE p_subs_period_info_t
        SET open_balance = NVL(billed,0)+NVL(recvd,0)+NVL(adjusted,0);

     v_count := SQL%ROWCOUNT;      
     Pk01_Syslog.Write_to_log('P_SUBS_PERIOD_INFO_T '||v_count||' rows updated', 
                               c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
      
     COMMIT;
   
     -- удаляем промежуточные данные
     EXECUTE IMMEDIATE
        'TRUNCATE TABLE TMP_ITEM_T DROP STORAGE';    

     Pk01_Syslog.Write_to_log('Stop. '||v_count||' rows inserted.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.Err_to_log('ERROR', c_PkgName||'.'||v_prcName );
        
END Exp_open_balance;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- полезные функции
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- получить сумму по выставленным счетам на указанное время
FUNCTION Get_account_billed(
                  p_account_no IN VARCHAR2,
                  p_date       IN DATE 
              ) RETURN NUMBER
IS
    v_value NUMBER;
BEGIN
    SELECT NVL(SUM(b.current_total + b.subords_total), 0) 
      INTO v_value
      FROM pin.bill_t b, pin.account_t a
     WHERE b.end_t > 0 AND b.end_t <= pin.d2u(p_date)
       AND b.account_obj_id0 = a.poid_id0
       AND a.account_no = p_account_no;
    RETURN v_value ;
END;

-- получить сумму по корректировкам на указанное время
FUNCTION Get_account_adjusted(
                  p_account_no IN VARCHAR2,
                  p_date       IN DATE 
              )  RETURN NUMBER
IS
    v_value NUMBER;
   -- Declare program variables as shown above
BEGIN
    SELECT NVL(SUM(i.item_total), 0) 
      INTO v_value
      FROM pin.item_t i, pin.account_t a, pin.billinfo_t bi
     WHERE i.effective_t < pin.d2u(p_date)
       AND i.poid_type = '/item/adjustment'
       AND i.ar_billinfo_obj_id0 = bi.poid_id0
       AND i.bill_obj_id0 = 0
       AND bi.account_obj_id0 = a.poid_id0
       AND a.account_no = p_account_no;
    RETURN v_value ;
END;

-- получить сумму платежей на указанное время
FUNCTION Get_account_payed(
                  p_account_no IN VARCHAR2,
                  p_date       IN DATE 
              )  RETURN NUMBER
IS
   v_value                 number;
BEGIN
    SELECT NVL(SUM(i.item_total + i.recvd), 0) 
      INTO v_value
      FROM pin.item_t i, 
           pin.event_bal_impacts_t ebi, 
           pin.event_billing_payment_payord_t ebpp,
           pin.account_t a,
           pin.billinfo_t bi
     WHERE ebpp.tstamp_val < pin.d2u(p_date)
       AND i.poid_id0 = ebi.item_obj_id0
       AND ebi.obj_id0 = ebpp.obj_id0
       AND ebpp.rec_id = 0
       AND ebi.rec_id = 0
       AND i.poid_type = '/item/payment'
       AND i.bill_obj_id0 = 0
       AND i.ar_billinfo_obj_id0 = bi.poid_id0
       AND bi.account_obj_id0 = a.poid_id0
       AND a.account_no = p_account_no;
    RETURN v_value ;
END;

END PK02_EXPORT_P;
/
