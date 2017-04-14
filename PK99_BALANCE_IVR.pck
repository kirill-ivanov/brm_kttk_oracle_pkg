CREATE OR REPLACE PACKAGE PK99_BALANCE_IVR
IS
    --
    -- Пакет для автоинформатора баланса
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK99_BALANCE_IVR';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc    is ref cursor;
    
    PROCEDURE GET_BALANCE_INFO (
           p_recordset      OUT t_refc, 
           p_retcode        OUT NUMBER,
           p_phone_number   IN  VARCHAR2
    );
END PK99_BALANCE_IVR;
/
CREATE OR REPLACE PACKAGE BODY PK99_BALANCE_IVR
IS

PROCEDURE GET_BALANCE_INFO( 
           p_recordset      OUT t_refc, 
           p_retcode        OUT NUMBER,
           p_phone_number   IN  VARCHAR2
)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'View_CDR_MMTS';    
    v_retcode       NUMBER;
    v_count         NUMBER;        
BEGIN
    SELECT count(*) INTO v_count 
           FROM 
               ORDER_PHONES_T OP 
      WHERE 
               PHONE_NUMBER = p_phone_number
               AND (OP.DATE_FROM <= SYSDATE AND (OP.DATE_TO IS NULL OR OP.DATE_TO >= SYSDATE));
    
    IF v_count = 1 THEN
          p_retcode := 0;
          OPEN p_recordset FOR
              WITH
                 NOPAY_BILL AS (
                        SELECT account_id,sum(TOTAL) TOTAL 
                            FROM BILL_T B
                            WHERE SYSDATE <= B.PAID_TO
                        GROUP BY ACCOUNT_ID
                     ),
                 IT AS (
                      SELECT B.ACCOUNT_ID,
                           SUM (
                               CASE
                                 WHEN B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN --'OPEN'
                                   AND I.TAX_INCL = Pk00_Const.c_RATEPLAN_TAX_INCL --'Y'
                                   THEN I.ITEM_TOTAL
                                 WHEN B.BILL_STATUS = Pk00_Const.c_BILL_STATE_OPEN --'OPEN'
                                   AND I.TAX_INCL = Pk00_Const.c_RATEPLAN_TAX_NOT_INCL --'N'
                                   THEN ROUND(I.ITEM_TOTAL * (1+NVL(AP.VAT,0)/100),2)
                                 ELSE 0
                               END 
                           ) TOTAL
                      FROM ITEM_T I, BILL_T B, ACCOUNT_PROFILE_T AP, PERIOD_T P
                     WHERE B.REP_PERIOD_ID > P.PERIOD_ID
                       AND I.BILL_ID = B.BILL_ID
                       AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
                       AND B.ACCOUNT_ID = AP.ACCOUNT_ID
                       AND P.POSITION = Pk00_Const.c_PERIOD_LAST -- 'LAST'
                    GROUP BY B.ACCOUNT_ID
                 ),
                 PAY AS (
                        SELECT account_id,sum(recvd) SUM_RECVD 
                            FROM PAYMENT_T P ,PERIOD_T PER
                        WHERE PER.POSITION = Pk00_Const.c_BILL_STATE_OPEN   --'OPEN'
                            AND PER.PERIOD_ID = P.REP_PERIOD_ID
                        GROUP BY ACCOUNT_ID
                     )
            SELECT a.account_id,
                   NVL(pay.SUM_RECVD,0)                 RECVD,          -- 1. суммарное кол-во платежей в текущем месяце
                   -(a.balance+NVL(nopay_bill.TOTAL,0)) DUE,            -- 2. текущая задолженность(исключаем непросроченные счета)(ЗНАК ОБРАТНЫЙ)
                   -(A.BALANCE-NVL(IT.TOTAL,0))         BALANCE_ONLINE, -- 3. баланс-онлайн (ЗНАК ОБРАТНЫЙ)
                   Pk00_Const.c_CURRENCY_RUB            CURRENCY_ID,    -- 4. ID валюты
                   CASE 
                     WHEN A.BAlANCE < 0 THEN -A.BALANCE
                     ELSE 0
                   END                                  PAY_TO,         -- 5. к оплате (долг на конец биллингуемого месяца; если долга нет - пишем 0) (ЗНАК ОБРАТНЫЙ)
                   NVL(IT.TOTAL,0)                      OPENBILL_DUE,   -- 6. начисления в текущем месяце                  
                   0                                    DISPUTED,       -- 7. оспариваемо
                   CASE 
                     WHEN A.BAlANCE > 0 THEN -A.BALANCE
                     ELSE 0
                   END                                  CLOSE_BALANCE   -- 8. АВАНС - остаток средств на конец последнего биллингуемого периода (если остатка есть, т.е. долг - 0)(ЗНАК ОБРАТНЫЙ)       
              FROM order_phones_t op,
                   order_t o,
                   account_t a,       
                   pay,
                   IT,
                   NOPAY_BILL
             WHERE     op.order_id = o.order_id
                   AND op.phone_number = p_phone_number       
                   AND IT.account_Id (+) = o.account_id
                   AND pay.account_Id (+) = o.account_id
                   AND NOPAY_BILL.account_id (+)= o.account_id       
                   AND a.account_id = o.account_id
                   AND (    OP.DATE_FROM <= SYSDATE
                        AND (OP.DATE_TO IS NULL OR OP.DATE_TO >= SYSDATE));
       ELSIF v_count=0 THEN
          p_retcode := 2;
       ELSIF v_count > 1 THEN
          p_retcode := 6;
   END IF;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
    p_retcode :=6;
--        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK99_BALANCE_IVR;
/
