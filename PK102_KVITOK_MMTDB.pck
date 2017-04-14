CREATE OR REPLACE PACKAGE PK102_KVITOK_MMTDB
IS
    --
    -- Пакет для печати КВИТАНЦИЙ на оплату для ФИЗИЧЕСКИХ ЛИЦ
    -- Пакет печати квитанций из старого биллинга МИКРОТЕСТА
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK102_KVITOK_MMTDB';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- Общая информация счет + данные продавца + данные клиента:
    --   - при ошибке выставляет исключение
    PROCEDURE kvitok_header( 
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,    -- ID лицевого счета            
                   p_period_id  IN INTEGER     -- ID отчетного периода
               );

    -- получить данные для печати: "Итоги за период"
    --   - при ошибке выставляет исключение
    PROCEDURE kvitok_summ( 
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,    -- ID лицевого счета            
                   p_period_id  IN INTEGER     -- ID отчетного периода
               );
    
    -- получить данные для печати позиций счета (начисления)
    --   - при ошибке выставляет исключение
    PROCEDURE kvitok_invoices( 
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,    -- ID лицевого счета            
                   p_period_id  IN INTEGER     -- ID отчетного периода
               );
               
    -- получить данные для печати краткой детализации трафика
    --   - при ошибке выставляет исключение
    PROCEDURE kvitok_detail( 
                   p_recordset OUT t_refc, 
                   p_account_id IN INTEGER,    -- ID лицевого счета            
                   p_period_id  IN INTEGER     -- ID отчетного периода
               );                         
      
END PK102_KVITOK_MMTDB;
/
CREATE OR REPLACE PACKAGE BODY PK102_KVITOK_MMTDB
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Общая информация счет + данные продавца + данные клиента:
--   - при ошибке выставляет исключение
PROCEDURE kvitok_header( 
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,    -- ID лицевого счета            
               p_period_id  IN INTEGER     -- ID отчетного периода
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'kvitok_header';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        -- Общая информация счет + данные продавца + данные клиента:
         SELECT 
               AP.ACCOUNT_ID,                -- ID л/с клиента
               A.ACCOUNT_NO,                 -- № лицевого счета
               AP.CONTRACTOR_ID,             -- ID продавца
               CR.SHORT_NAME COMPANY_NAME,   -- Компания продавец
               CR.INN COMPANY_INN,           -- ИНН продавца
               CB.BANK_NAME,                 -- Банк
               CB.BANK_SETTLEMENT,           -- № Р/с 
               CB.BANK_CORR_ACCOUNT,         -- № К/с
               CB.BANK_CODE,                 -- БИК
               SUB.SUBSCRIBER_ID,            -- ID клиента
               SUB.LAST_NAME SUB_LAST_NAME,     -- Ф.И.О.
               SUB.FIRST_NAME SUB_FIRST_NAME,   -- Ф.И.О.
               SUB.MIDDLE_NAME SUB_MIDDLE_NAME, -- Ф.И.О.
               AC.COUNTRY      DLV_ADDR_COUNTRY,
               AC.ZIP          DLV_ADDR_ZIP,
               AC.STATE        DLV_ADDR_STATE,
               AC.CITY         DLV_ADDR_CITY,
               AC.ADDRESS      DLV_ADDR_ADDRESS,
               NULL            DLV_ADDR_PERSON,
               AC.PHONES CLIENT_PHONE,
               CRA.PHONE_BILLING,
               CRA.PHONE_ACCOUNT,                             
               P.PERIOD_FROM, 
               P.PERIOD_TO - 1/(24*60*60) PERIOD_TO,               
               A.CURRENCY_ID                 -- ID валюты счета               
          FROM ACCOUNT_T A,
               ACCOUNT_PROFILE_T AP,
               CONTRACTOR_T CR,
               CONTRACTOR_BANK_T CB,
               CONTRACTOR_ADDRESS_T CRA,
               ACCOUNT_CONTACT_T AC,
               SUBSCRIBER_T SUB,
               PERIOD_T P
         WHERE P.PERIOD_ID = p_period_id
           AND A.ACCOUNT_ID = p_account_id           
           AND A.ACCOUNT_TYPE = 'P'
           AND AP.ACCOUNT_ID = A.ACCOUNT_ID
           AND AC.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AC.DATE_FROM <= P.PERIOD_TO
           AND SUB.SUBSCRIBER_ID = AP.SUBSCRIBER_ID
           AND (AC.DATE_TO IS NULL OR P.PERIOD_FROM < AC.DATE_TO )
           AND AC.ADDRESS_TYPE = 'DLV'
           AND AP.DATE_FROM <= P.PERIOD_TO 
           AND (AP.DATE_TO IS NULL OR P.PERIOD_FROM < AP.DATE_TO )
           AND CR.CONTRACTOR_ID = AP.CONTRACTOR_ID
           AND CRA.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND CB.BANK_ID = AP.CONTRACTOR_BANK_ID
           AND CB.CONTRACTOR_ID = CR.CONTRACTOR_ID
           AND CB.DATE_FROM <= P.PERIOD_TO 
           AND (CB.DATE_TO IS NULL OR P.PERIOD_FROM < CB.DATE_TO );    
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- получить данные для печати: "Итоги за период"
-- позиции счета и ивойса сформированы, платежи разнесены, авансы сформированы, 
-- новый биллинговый период открыт
--   - при ошибке выставляет исключение
PROCEDURE kvitok_summ( 
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,    -- ID лицевого счета            
               p_period_id  IN INTEGER     -- ID отчетного периода
           )
IS
    v_prcName         CONSTANT VARCHAR2(30) := 'kvitok_summ';
    v_balance         NUMBER; 
    v_open_balance    NUMBER;  -- входящий баланс на начало периода выставления счета
    v_close_balance   NUMBER;  -- выходяший баланс на конец периода выставления счета
    v_open_due        NUMBER;  -- задолженность за предыдущий период
    v_bill_total      NUMBER;  -- начислено за текущий период
    v_recvd           NUMBER;  -- принято платежей за период счета
    v_last_period_id  INTEGER;
    v_retcode         INTEGER;
    
    v_account_no      VARCHAR2(100);

    v_balance_ac         NUMBER; 
BEGIN
    SELECT ACCOUNT_NO INTO V_ACCOUNT_NO 
           FROM ACCOUNT_T 
      WHERE ACCOUNT_ID = p_account_id;
    
    -- получаем ID предыдущего периода
    v_last_period_id := PK04_PERIOD.Make_prev_id(p_period_id);   

    -- получаем входящий баланс периода
    BEGIN    
      SELECT balance
             into v_open_balance
             FROM TMP_SVERKA_FIZ_OSTATKI_FINAL
             WHERE ACCOUNT_NO = v_account_no
                  AND REP_PERIOD_ID = p_period_id;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      v_open_balance := 0;
    END;   
  
    -- получаем сумму начислений по счетам за период (кроме основного могут быть корректировки)
    BEGIN
      SELECT t.total+t.adjusted TOTAL,PAYED_RECVD,BALANCE
             into v_bill_total, v_recvd,v_close_balance
             FROM TMP_SVERKA_FIZ_OSTATKI_FINAL t
             WHERE ACCOUNT_NO = v_account_no
                   AND REP_PERIOD_ID = p_period_id;   
   EXCEPTION WHEN NO_DATA_FOUND THEN
       v_bill_total := 0;
       v_recvd := 0;
       v_close_balance :=0;
   END;
       
    -- задолженность
    v_open_due := -(v_open_balance + v_recvd);
        
    -- возвращаем курсор
    OPEN p_recordset FOR
         SELECT 
              v_open_balance    OPEN_BALANCE,      -- аванс/долг (входящий баланс от предыдущего периода)
              v_recvd           RECVD,             -- принято платежей за период
              v_open_due        OPEN_DUE,          -- задолженность за предыдущий период
              v_bill_total      BILL_TOTAL,        -- начисления по счетам за период
              v_close_balance   CLOSE_BALANCE,     -- баланс в конце периода
              CASE WHEN v_close_balance < 0 THEN  
                   -v_close_balance ELSE 0 END PAY_SUMM  -- к оплате 
           FROM DUAL;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;
 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- получить данные для печати позиций счета (начисления)
--   - при ошибке выставляет исключение
PROCEDURE kvitok_invoices( 
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,    -- ID лицевого счета            
               p_period_id  IN INTEGER     -- ID отчетного периода
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'kvitok_invoices';
    v_retcode    INTEGER;
    v_account_no VARCHAR2(100);
BEGIN
    select account_no INTO v_account_no from account_t where account_id = p_account_id;

    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT bill_id,
              name SERVICE_NAME,
              DECODE (tax_all, NULL, item_netto - item_tax, item_netto) GROSS,
              item_tax TAX,
              DECODE (tax_all, NULL, item_netto, item_total) TOTAL,
              NVL (tax_all, amount_taxed) tax_all,
              usage_start,
              usage_end
         FROM (SELECT bill_id,
                      name,
                      amount_taxed,
                      item_netto,
                      DECODE (tax_all,
                              NULL, ROUND (ROUND (item_netto, 2) * 18 / 118, 2),
                              ROUND (ROUND (item_netto, 2) * .18, 2))
                         item_tax,
                      DECODE (tax_all,
                              NULL, ROUND (item_netto, 2),
                              ROUND (item_netto * 1.18, 2))
                         item_total,
                      tax_all,
                      TRUNC (u2d (effective_t) - 1, 'MM') usage_start,
                      u2d (effective_t) - 1 usage_end
                 FROM (  SELECT i.ar_bill_obj_id0 bill_id,
                                SUM (
                                     i.item_total
                                   + i.adjusted
                                   + i.transfered
                                   - i.delta_due)
                                   item_netto,
                                MAX (
                                     itax.item_total
                                   + i.adjusted
                                   + i.transfered
                                   - i.delta_due)
                                   tax_all,
                                i.name,
                                MIN (b.amount_taxed) amount_taxed,
                                MAX (i.effective_t) effective_t
                           FROM bill_t@mmtdb.world b, item_t@mmtdb.world i, item_t@mmtdb.world itax, account_t@mmtdb.world a
                          WHERE     i.poid_type <> '/item/tax'
                                and i.ar_bill_obj_id0 = b.poid_id0
                                AND i.item_total <> 0
                                AND itax.poid_type(+) = '/item/tax'
                                AND itax.ar_bill_obj_id0(+) = i.ar_bill_obj_id0
                                --and itax.item_total(+) <> 0
                                AND b.poid_id0 = i.ar_bill_obj_id0
                                AND a.poid_id0 = b.account_obj_id0
                                AND a.account_no = v_account_no
                                AND b.end_t-1 between pin.d2u@mmtdb.world(TO_DATE (p_period_id, 'YYYYMM')) and pin.d2u@mmtdb.world( LAST_DAY (TO_DATE (p_period_id, 'YYYYMM')) + INTERVAL '00 23:59:59' DAY TO SECOND)
                       GROUP BY i.name, i.ar_bill_obj_id0));    
        
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- получить данные для печати краткой детализации трафика
--   - при ошибке выставляет исключение
PROCEDURE kvitok_detail( 
               p_recordset OUT t_refc, 
               p_account_id IN INTEGER,    -- ID лицевого счета            
               p_period_id  IN INTEGER     -- ID отчетного периода
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'kvitok_detail';
    v_retcode    INTEGER;
    v_account_no VARCHAR2(100);
BEGIN
    select 
         account_no INTO v_account_no 
       from account_t 
    where account_id = p_account_id;
    -- возвращаем курсор 
    OPEN p_recordset FOR        
       SELECT 
                 terminate_code PREFIX_B,
                 zone_name TERM_Z_NAME,
                 date_t START_TIME,
                 COUNT (network_session_id) calls_count,
                 --bill_id,
                 --poid_id0,
                 --order_num,
                 --service,         
                 --primary_msid,                                           
                 CASE
                    WHEN item_type = '/item/usage/min_summ' THEN 0
                    ELSE SUM (duration)
                 END
                    mins_sum,
                 SUM (cost_rub) amount_sum,
                 NULL SUBSERVICE_KEY
                 --service_type,
                 --item_type,
                 --'' direction         
            FROM (SELECT DISTINCT
                         e.poid_id0 e_poid,
                         i.ar_bill_obj_id0 bill_id,
                         i.poid_id0,
                         bi.product_obj_id0,
                         pci.order_num,
                         it.descr service,
                         CASE
                            WHEN s.poid_type = '/service/telco/gsm/telephony'
                            THEN
                               tl.primary_msid
                            WHEN s.poid_type = '/service/telco/gsm/telephony/freecall'
                            THEN
                               sa.name
                            WHEN s.poid_type = '/service/telco/gsm/telephony/zone'
                            THEN
                               tl.primary_msid
                            ELSE
                               NULL
                         END
                            primary_msid,
                         tl.network_session_id,
                         TRUNC (u2d (e.start_t)) date_t,
                         TRIM (LPAD (e.descr, INSTR (e.descr, '-') - 1)) terminate_code,
                         SUBSTR (e.descr, INSTR (e.descr, '-') + 1) zone_name,
                         SIGN (bi.amount) * ROUND (e.net_quantity / 60, 2) duration,
                         ROUND (bi.amount, 2) cost_rub,
                         s.poid_type service_type,
                         i.poid_type item_type,
                         i.effective_t
                    FROM account_t@mmtdb.world a,
                         bill_t@mmtdb.world b,
                         item_t@mmtdb.world i,
                         profile_t@mmtdb.world pr,
                         profile_contract_info_t@mmtdb.world pci,
                         event_bal_impacts_t@mmtdb.world bi,
                         event_t@mmtdb.world e,
                         event_dlay_sess_tlcs_t@mmtdb.world tl,
                         config_item_types_t@mmtdb.world it,
                         service_t@mmtdb.world s,
                         service_alias_list_t@mmtdb.world sa
                   WHERE  i.account_obj_id0 = pr.account_obj_id0
                         AND i.ar_bill_obj_id0 = b.poid_id0
                         AND pci.obj_id0 = pr.poid_id0
                         AND e.item_obj_id0 = i.poid_id0
                         AND e.poid_id0 = bi.obj_id0
                         AND tl.obj_id0(+) = e.poid_id0
                         AND i.poid_type <> '/item/tax'
                         AND bi.impact_type <> 4
                         AND bi.resource_id = 810
                         AND s.poid_id0(+) = i.service_obj_id0
                         AND sa.obj_id0(+) = s.poid_id0
                         AND it.item_type(+) = i.poid_type
                         AND a.poid_id0 = b.account_obj_id0
                         AND a.account_no = v_account_no 
                         AND b.end_t-1 between pin.d2u@mmtdb.world(TO_DATE (p_period_id, 'YYYYMM')) and pin.d2u@mmtdb.world( LAST_DAY (TO_DATE (p_period_id, 'YYYYMM')) + INTERVAL '00 23:59:59' DAY TO SECOND)
                         )                         
        GROUP BY bill_id,
                 poid_id0,
                 order_num,
                 product_obj_id0,
                 service,
                 primary_msid,
                 date_t,
                 zone_name,
                 terminate_code,
                 service_type,
                 item_type;          
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK102_KVITOK_MMTDB;
/
