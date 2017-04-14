CREATE OR REPLACE PACKAGE PK10_PAYMENT_EISUP
IS
    --
    -- Пакет для поддержки импорта из ЕИСУП
    -- eisup_payment_t, eisup_pay_transfer_t
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_PAYMENT_EISUP';
    -- ==============================================================================
   
    type t_refc is ref cursor;
    
    -- Платежная система для платежей импортированных из ЕИСУП:
    c_PAYSYSTEM_EISUP_ID CONSTANT INTEGER := 19;
    
    -- Коды загрузки
    c_CODE_LOAD_ERR CONSTANT INTEGER := -2;    
    c_CODE_BIND_ERR CONSTANT INTEGER := -1;
    c_CODE_NULL     CONSTANT INTEGER :=  NULL;
    c_CODE_BIND_OK  CONSTANT INTEGER :=  1;
    c_CODE_LOAD_OK  CONSTANT INTEGER :=  2;
    
    -- Статусы записей журнала
    c_J_STATUS_OK    CONSTANT VARCHAR2(10) := 'OK';   -- успешно загружен в биллинг
    c_J_STATUS_NEW   CONSTANT VARCHAR2(10) := 'NEW';  -- готов к загрузке
    c_J_STATUS_SKIP  CONSTANT VARCHAR2(10) := 'SKIP'; -- пропущен
    c_J_STATUS_ERR   CONSTANT VARCHAR2(10) := 'ERR';  -- ошибка при загрузке
    c_J_STATUS_OLD   CONSTANT VARCHAR2(10) := 'OLD';  -- устарел до загрузки в биллинг
    c_J_STATUS_DEL   CONSTANT VARCHAR2(10) := 'DEL';  -- удален из биллинга
    
    -- ------------------------------------------------------------------------ --
    -- Занрузка в биллинг очередных платежей пришедших из EISUP
    -- ------------------------------------------------------------------------ --
    PROCEDURE Import_eisup_payments;
    
    -- ------------------------------------------------------------------------ --
    -- Контроль процесса загрузки платежей
    -- ------------------------------------------------------------------------ --
    PROCEDURE Import_ctrl (
                   p_recordset    OUT t_refc
               );
    
    -- ------------------------------------------------------------------------ --
    -- Удаление из биллинга платежей и операций разноски, для указанного журнала
    -- ------------------------------------------------------------------------ --
    PROCEDURE Drop_journal_payments( p_journal_id IN INTEGER );
    
    -- ------------------------------------------------------------------------ --
    -- Удалить старые записи из таблиц, по данным журнала загрузок, статус 'DEL'
    -- ------------------------------------------------------------------------ --
    PROCEDURE Clean_journal_payments;
  
    -- ------------------------------------------------------------------------ --
    -- Занрузка в биллинг платежей из журнала
    -- ------------------------------------------------------------------------ --
    PROCEDURE Load_journal_payments( p_journal_id IN INTEGER );
    
    -- ------------------------------------------------------------------------ --
    -- Привязка платежей ЕИСУП к л/с BRM
    -- ------------------------------------------------------------------------ --
    PROCEDURE Bind_payment( p_journal_id IN INTEGER );
    
    -- ------------------------------------------------------------------------ --
    -- Привязка операций разноски платежей ЕИСУП к л/с BRM
    -- ------------------------------------------------------------------------ --
    PROCEDURE Bind_pay_transfer( p_journal_id IN INTEGER );
    
    -- ------------------------------------------------------------------------ --
    -- Добавить привязанные платежи на Л/С клиента 
    --
    PROCEDURE Add_payments(p_journal_id IN INTEGER);

    -- ------------------------------------------------------------------------ --
    -- Добавить разноску на привязанные платежи
    --
    PROCEDURE Pay_transfer( p_journal_id IN INTEGER );

    -- ------------------------------------------------------------------------ --
    -- Удалить операции разноски, для записей с указанным статусом
    --
    PROCEDURE Delete_transfer( p_journal_id IN INTEGER );

    -- ------------------------------------------------------------------------ --
    -- Удалить операции разноски, для записей с указанным статусом
    -- Отвязать записи от счетов 
    PROCEDURE Reset_transfer( 
            p_journal_id IN INTEGER,
            p_load_code  IN INTEGER DEFAULT NULL -- по умолчанию весь журнал
          );

    -- ------------------------------------------------------------------------ --
    -- Удалить платежи из журнала
    --
    PROCEDURE Delete_payments( 
            p_journal_id IN INTEGER
          );
    
    -- ------------------------------------------------------------------------ --
    -- Отвязать записи от лицевых счетов 
    PROCEDURE Reset_payments( 
            p_journal_id IN INTEGER
          );

    -- ======================================================================== --
    -- Работа с входным балансом
    -- ------------------------------------------------------------------------ --
    PROCEDURE Load_in_balance;
    
    -- ------------------------------------------------------------------------ --
    -- пересчитать балансы счетов для которых установлен входящий баланс
    PROCEDURE Recalc_for_inBalances;
    
    -- ------------------------------------------------------------------------ --
    -- применить входящий баланс в биллинге
    PROCEDURE Set_in_balance;
    
    -- ------------------------------------------------------------------------ --
    -- удалить входящий баланс из биллинга
    PROCEDURE Delete_in_balance;


    -- ======================================================================== --
    -- Просмотр чего-нибудь (шаблон)
    --   - при ошибке выставляет исключение
    PROCEDURE Eisup_transfer_list (
            p_recordset    OUT t_refc, 
            p_journal_id   IN INTEGER,
            p_load_code    IN INTEGER DEFAULT NULL
          );

    -- ------------------------------------------------------------------------ --
    -- Статистика по приязке платежей журнала
    -- ------------------------------------------------------------------------ --
    PROCEDURE Payments_bind_stat (
                   p_recordset    OUT t_refc, 
                   p_journal_id   IN INTEGER
               );


END PK10_PAYMENT_EISUP;
/
CREATE OR REPLACE PACKAGE BODY PK10_PAYMENT_EISUP
IS

-- ------------------------------------------------------------------------ --
-- Занрузка в биллинг очередных платежей пришедших из EISUP
-- ------------------------------------------------------------------------ --
PROCEDURE Import_eisup_payments
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Import_eisup_payments';
    v_count       INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg( 'Start', c_PkgName||'.'||v_prcName);  
    --
    -- готовим журнал к загрузке
    MERGE INTO EISUP_JOURNAL_T J
    USING (
      SELECT DECODE(RN, 1, c_J_STATUS_NEW, c_J_STATUS_SKIP) STATUS, JOURNAL_ID
        FROM (
        SELECT ROW_NUMBER() OVER (PARTITION BY TRUNC(DATE_FROM,'mm') ORDER BY CREATE_DATE DESC) RN, 
               J.JOURNAL_ID 
          FROM EISUP_JOURNAL_T J
         WHERE J.LOAD_DATE IS NULL
           AND J.STATUS IS NULL
        )
    ) EJ
    ON (
        EJ.JOURNAL_ID = J.JOURNAL_ID
    ) 
    WHEN MATCHED THEN UPDATE SET J.STATUS = EJ.STATUS;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_JOURNAL_T: '||v_count||' rows merged', c_PkgName||'.'||v_prcName);
    --
    -- проставляем кол-во записей в загрузке, для истории
    UPDATE EISUP_JOURNAL_T J 
       SET J.LOAD_DATE = SYSDATE,
           J.PAYMENTS  = (
               SELECT COUNT(*) FROM EISUP_PAYMENT_TMP EP WHERE EP.JOURNAL_ID = J.JOURNAL_ID 
           ),
           J.TRANSFERS  = (
               SELECT COUNT(*) FROM EISUP_PAY_TRANSFER_TMP ET WHERE ET.JOURNAL_ID = J.JOURNAL_ID 
           ) 
     WHERE STATUS IN (c_J_STATUS_NEW, c_J_STATUS_SKIP)
       AND LOAD_DATE IS NULL;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_JOURNAL_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName);
    --
    -- ставим в очередь на удаление, загруженные ранее журналы
    UPDATE EISUP_JOURNAL_T J 
       SET J.STATUS = c_J_STATUS_OLD 
     WHERE J.JOURNAL_ID IN (
        SELECT JT.JOURNAL_ID 
          FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY TRUNC(DATE_FROM,'mm') ORDER BY CREATE_DATE) RN,
                   COUNT(*) OVER (PARTITION BY TRUNC(DATE_FROM,'mm')) CN,
                   J.JOURNAL_ID 
              FROM EISUP_JOURNAL_T J
             WHERE J.STATUS IN (c_J_STATUS_NEW,c_J_STATUS_OK)
          ) JT
         WHERE JT.RN < JT.CN  
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_JOURNAL_T: '||v_count||' rows to OLD', c_PkgName||'.'||v_prcName);
    --
    -- удаляем старые журналы
    v_count := 0;
    FOR r_old IN (
        SELECT J.JOURNAL_ID 
          FROM EISUP_JOURNAL_T J
         WHERE J.STATUS = c_J_STATUS_OLD
         ORDER BY J.CREATE_DATE
      )
    LOOP
      Drop_journal_payments(r_old.journal_id);
      v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg( 'EISUP_JOURNAL_T: '||v_count||' journals deleted', c_PkgName||'.'||v_prcName);
    --
    -- грузим новые журналы
    v_count := 0;    
    FOR r_new IN (
        SELECT J.JOURNAL_ID 
          FROM EISUP_JOURNAL_T J
         WHERE J.STATUS = c_J_STATUS_NEW
         ORDER BY J.CREATE_DATE
      )
    LOOP
      Load_journal_payments(r_new.journal_id);
      v_count := v_count + 1;
    END LOOP;
    Pk01_Syslog.Write_msg( 'EISUP_JOURNAL_T: '||v_count||' journals loaded', c_PkgName||'.'||v_prcName);

    Pk01_Syslog.Write_msg( 'Stop', c_PkgName||'.'||v_prcName);
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Контроль процесса загрузки платежей
-- ------------------------------------------------------------------------ --
PROCEDURE Import_ctrl (
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Import_ctrl';
    v_retcode    INTEGER;
    v_ssid       INTEGER;
    v_l01_id     INTEGER;
BEGIN
    SELECT MAX(SSID), MAX(L01_ID) 
      INTO v_ssid, v_l01_id
      FROM L01_MESSAGES 
    WHERE MSG_SRC LIKE 'PK10_PAYMENT_EISUP.Import_eisup_payments'
      AND MESSAGE = 'Start';
  
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
        SELECT MSG_DATE, MSG_LEVEL, MESSAGE, MSG_SRC 
          FROM L01_MESSAGES L01
         WHERE L01.SSID = v_ssid
           AND L01.L01_ID >= v_l01_id
         ORDER BY L01.L01_ID DESC
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- Удаление из биллинга платежей и операций разноски, для указанного журнала
-- ------------------------------------------------------------------------ --
PROCEDURE Drop_journal_payments(p_journal_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Drop_journal_payments';
    v_count       INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg( 'Start journal_id = '||p_journal_id, c_PkgName||'.'||v_prcName);

    -- удаляем операции разноски
    Delete_transfer( p_journal_id );
    --
    UPDATE EISUP_PAY_TRANSFER_T
       SET BRM_LOAD_CODE       = NULL, 
           BRM_LOAD_DATE       = NULL, 
           BRM_PAYMENT_ID      = NULL, 
           BRM_PAY_PERIOD_ID   = NULL, 
           BRM_PAY_TRANSFER_ID = NULL, 
           BRM_BILL_ID         = NULL, 
           BRM_BILL_PERIOD_ID  = NULL
     WHERE JOURNAL_ID = p_journal_id;
    -- 
    --DELETE FROM EISUP_PAY_TRANSFER_T ET
    -- WHERE ET.JOURNAL_ID = p_journal_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAY_TRANSFER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName);
    -- удаляем платежи
    Delete_payments( p_journal_id );
    --
    UPDATE EISUP_PAYMENT_T
       SET BRM_LOAD_CODE   = NULL, 
           BRM_LOAD_DATE   = NULL, 
           BRM_ACCOUNT_ID  = NULL, 
           BRM_CONTRACT_ID = NULL, 
           BRM_CUSTOMER_ID = NULL, 
           BRM_PAYMENT_ID  = NULL
     WHERE JOURNAL_ID = p_journal_id;
    --DELETE FROM EISUP_PAYMENT_T EP
    -- WHERE EP.JOURNAL_ID = p_journal_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAYMENT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName);
    --
    -- отмечаем строку в журнале
    UPDATE EISUP_JOURNAL_T J 
       SET J.STATUS = c_J_STATUS_DEL
     WHERE J.JOURNAL_ID = p_journal_id;

    Pk01_Syslog.Write_msg( 'Stop', c_PkgName||'.'||v_prcName);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Удалить старые записи из таблиц, по данным журнала загрузок, статус 'DEL'
-- ------------------------------------------------------------------------ --
PROCEDURE Clean_journal_payments
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Clean_journal_payments';
    v_count       INTEGER := 0;
BEGIN

    -- удаляем операции разноски из временных таблиц
    DELETE FROM EISUP_PAY_TRANSFER_TMP ET
     WHERE ET.JOURNAL_ID IN (
        SELECT J.JOURNAL_ID
          FROM EISUP_JOURNAL_T J
         WHERE J.STATUS IN (c_J_STATUS_DEL, c_J_STATUS_SKIP)
     )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAY_TRANSFER_TMP: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName);

    DELETE FROM EISUP_PAY_TRANSFER_T ET
     WHERE ET.JOURNAL_ID IN (
        SELECT J.JOURNAL_ID
          FROM EISUP_JOURNAL_T J
         WHERE J.STATUS IN (c_J_STATUS_DEL, c_J_STATUS_SKIP)
     )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAY_TRANSFER_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName);

    -- удаляем платежи из временных таблиц
    DELETE FROM EISUP_PAYMENT_TMP EP
     WHERE EP.JOURNAL_ID IN (
        SELECT J.JOURNAL_ID
          FROM EISUP_JOURNAL_T J
         WHERE J.STATUS IN (c_J_STATUS_DEL, c_J_STATUS_SKIP)
     )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAYMENT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName);

    DELETE FROM EISUP_PAYMENT_T EP
     WHERE EP.JOURNAL_ID IN (
        SELECT J.JOURNAL_ID
          FROM EISUP_JOURNAL_T J
         WHERE J.STATUS IN (c_J_STATUS_DEL, c_J_STATUS_SKIP)
     )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAYMENT_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Занрузка в биллинг платежей из журнала
-- ------------------------------------------------------------------------ --
PROCEDURE Load_journal_payments(p_journal_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Load_journal_payments';
    v_count       INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg( 'Start journal_id = '||p_journal_id, c_PkgName||'.'||v_prcName);  

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Платежи
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Перенос данных из временной таблицы  
    INSERT INTO EISUP_PAYMENT_T (
        JOURNAL_ID, RECORD_ID, DOCUMENT_ID, DOC_TYPE, DOC_NO, DOC_DATE,
        BANK_ACCOUNT, CLNT_ACCOUNT, ERP_CODE, CONTRACT_NO, ACCOUNT_NO,
        DOCUMENT_NO, DOCUMENT_DATE, PAYMENT_DATE, PAYMENT_AMOUNT,
        CURRENCY_ID, PAY_DESCR, PERIOD
    )
    SELECT 
        JOURNAL_ID, RECORD_ID, DOCUMENT_ID, DOC_TYPE, TRIM(DOC_NO), DOC_DATE,
        TRIM(BANK_ACCOUNT), TRIM(CLNT_ACCOUNT), 
        TRIM(ERP_CODE), TRIM(CONTRACT_NO), TRIM(ACCOUNT_NO), TRIM(DOCUMENT_NO), 
        DOCUMENT_DATE, PAYMENT_DATE, PAYMENT_AMOUNT,
        CURRENCY_ID, TRIM(PAY_DESCR), PERIOD
      FROM EISUP_PAYMENT_TMP PT
     WHERE PT.JOURNAL_ID = p_journal_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAYMENT_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName);

    -- Привязка платежей ЕИСУП к л/с BRM
    Bind_payment( p_journal_id );
    -- Добавить привязанные платежи на Л/С клиента 
    Add_payments( p_journal_id );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Операции разноски платежей
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Перенос данных из временной таблицы
    INSERT INTO EISUP_PAY_TRANSFER_T (
           JOURNAL_ID, DOCUMENT_ID, BILL_NO, BILL_DATE,
           TRANSFER_TOTAL, OPERATION, MOVEMENT_TYPE
    )
    SELECT JOURNAL_ID, DOCUMENT_ID, BILL_NO, BILL_DATE,
           TRANSFER_TOTAL, OPERATION, MOVEMENT_TYPE
      FROM EISUP_PAY_TRANSFER_TMP TT
     WHERE TT.JOURNAL_ID = p_journal_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAY_TRANSFER_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName);

    -- Привязка операций разноски платежей ЕИСУП к л/с BRM
    Bind_pay_transfer( p_journal_id );
    
    -- Добавить разноску на привязанные платежи
    Pay_transfer( p_journal_id );
    --
    -- отмечаем строку в журнале
    UPDATE EISUP_JOURNAL_T J 
       SET J.STATUS = c_J_STATUS_OK
     WHERE J.JOURNAL_ID = p_journal_id;
    --
    Pk01_Syslog.Write_msg( 'Stop', c_PkgName||'.'||v_prcName);
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Привязка платежей ЕИСУП к л/с BRM
-- ------------------------------------------------------------------------ --
PROCEDURE Bind_payment(p_journal_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Bind_payment';
    v_count       INTEGER := 0;
BEGIN
    --
    -- привязка по ERP_CODE + ACCOUNT_NO + CONTRACT_NO(opt)
    --
    MERGE INTO EISUP_PAYMENT_T EP
    USING (
        SELECT ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, JOURNAL_ID, DOCUMENT_ID, BRM_CONTRACT_NO  
          FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY EP.DOCUMENT_ID ORDER BY
                                        CASE
                                            WHEN UPPER(EP.CONTRACT_NO) = TRIM(UPPER(C.CONTRACT_NO)) THEN 0
                                            ELSE 1
                                        END,
                                        CASE
                                            WHEN AP.DATE_FROM <= EP.PAYMENT_DATE  
                                             AND (AP.DATE_TO IS NULL OR EP.PAYMENT_DATE <= AP.DATE_TO ) THEN 0
                                            ELSE 1
                                        END, 
                                        AP.DATE_FROM DESC 
                                     ) RN,
                   AP.DATE_FROM, AP.DATE_TO,
                   A.ACCOUNT_ID, CS.CUSTOMER_ID, AP.PROFILE_ID, 
                   CS.ERP_CODE, A.ACCOUNT_NO, 
                   CASE
                       WHEN UPPER(EP.CONTRACT_NO) = TRIM(UPPER(C.CONTRACT_NO)) THEN C.CONTRACT_ID
                       ELSE NULL
                   END CONTRACT_ID,
                   C.CONTRACT_NO BRM_CONTRACT_NO, 
                   EP.CONTRACT_NO EP_CONTRACT_NO, 
                   EP.JOURNAL_ID, EP.DOCUMENT_ID
              FROM EISUP_PAYMENT_T EP, CUSTOMER_T CS, CONTRACT_T C, 
                   ACCOUNT_T A, ACCOUNT_PROFILE_T AP
             WHERE EP.JOURNAL_ID = p_journal_id
               AND ( EP.ERP_CODE = CS.ERP_CODE 
                     OR (EP.ERP_CODE IS NULL AND CS.ERP_CODE IS NULL))
               AND EP.ACCOUNT_NO = A.ACCOUNT_NO
               AND AP.ACCOUNT_ID = A.ACCOUNT_ID
               AND AP.CUSTOMER_ID= CS.CUSTOMER_ID
               AND AP.CONTRACT_ID= C.CONTRACT_ID
        )
        WHERE RN = 1
    ) EPT
    ON (
        EP.JOURNAL_ID  = EPT.JOURNAL_ID AND
        EP.DOCUMENT_ID = EPT.DOCUMENT_ID
    )
    WHEN MATCHED THEN UPDATE SET EP.BRM_ACCOUNT_ID  = EPT.ACCOUNT_ID, 
                                 EP.BRM_CONTRACT_ID = EPT.CONTRACT_ID, 
                                 EP.BRM_CUSTOMER_ID = EPT.CUSTOMER_ID,
                                 EP.BRM_CONTRACT_NO = EPT.BRM_CONTRACT_NO
    ;
    /*
    -- совсем честная привязка
    MERGE INTO EISUP_PAYMENT_T EP
    USING (
        SELECT ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, JOURNAL_ID, 
               DOCUMENT_ID
        FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID, CONTRACT_ID ORDER BY NVL(CUSTOMER_ID,0) DESC) RN,
                   ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, JOURNAL_ID, DOCUMENT_ID
              FROM (
                  SELECT A.ACCOUNT_ID, C.CONTRACT_ID, 
                         DECODE(TRIM(CS.ERP_CODE), TRIM(EP.ERP_CODE), CS.CUSTOMER_ID, NULL) CUSTOMER_ID,
                         EP.JOURNAL_ID, EP.DOCUMENT_ID
                    FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A, CONTRACT_T C, CUSTOMER_T CS, EISUP_PAYMENT_T EP
                   WHERE AP.ACCOUNT_ID  = A.ACCOUNT_ID
                     AND AP.CONTRACT_ID = C.CONTRACT_ID
                     AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
                     AND AP.DATE_FROM  <= EP.PAYMENT_DATE
                     AND (AP.DATE_TO IS NULL OR EP.PAYMENT_DATE <= AP.DATE_TO )
                     AND A.ACCOUNT_NO   = EP.ACCOUNT_NO
                     AND C.CONTRACT_NO  = EP.CONTRACT_NO
                     AND EP.JOURNAL_ID  = p_journal_id
            )
        )
        WHERE RN = 1
    ) EPT
    ON (
        EP.JOURNAL_ID  = EPT.JOURNAL_ID AND
        EP.DOCUMENT_ID = EPT.DOCUMENT_ID
    )
    WHEN MATCHED THEN UPDATE SET EP.BRM_ACCOUNT_ID    = EPT.ACCOUNT_ID, 
                                 EP.BRM_CONTRACT_ID   = EPT.CONTRACT_ID, 
                                 EP.BRM_CUSTOMER_ID   = EPT.CUSTOMER_ID 
    ;
    */
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAYMENT_T.ACCOUNT_ID: '||v_count||' rows set', c_PkgName||'.'||v_prcName);
    -- 
    -- проставляем ID платежей, номер договора необязательный
    UPDATE EISUP_PAYMENT_T EP
       SET EP.BRM_PAYMENT_ID  = PK02_POID.Next_payment_id,
           EP.BRM_LOAD_CODE   = c_CODE_BIND_OK
     WHERE EP.BRM_ACCOUNT_ID  IS NOT NULL 
       AND EP.BRM_CUSTOMER_ID IS NOT NULL
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAYMENT_T.ACCOUNT_ID: '||v_count||' rows set payment_id', c_PkgName||'.'||v_prcName);
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Привязка операций разноски платежей ЕИСУП к л/с BRM
-- ------------------------------------------------------------------------ --
PROCEDURE Bind_pay_transfer(p_journal_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Bind_pay_transfer';
    v_count       INTEGER := 0;
BEGIN
    -- честная привязка операции разноски к счету
    UPDATE EISUP_PAY_TRANSFER_T ET
       SET (ET.BRM_BILL_ID, ET.BRM_BILL_PERIOD_ID) = 
           ( 
             SELECT B.BILL_ID, B.REP_PERIOD_ID
               FROM BILL_T B 
              WHERE B.BILL_NO = TRIM(ET.BILL_NO)
           ),
           ET.BRM_LOAD_DATE  = SYSDATE
     WHERE EXISTS (
        SELECT * FROM EISUP_PAYMENT_T EP
         WHERE EP.DOCUMENT_ID    = ET.DOCUMENT_ID
           AND EP.BRM_LOAD_CODE >= c_CODE_BIND_OK
      )
      AND EXISTS (
        SELECT * FROM BILL_T B
         WHERE B.BILL_NO = TRIM(ET.BILL_NO)
      )
      AND ET.BRM_LOAD_CODE IS NULL
      AND ET.JOURNAL_ID  = p_journal_id
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAY_TRANSFER_T.BILL_ID: '||v_count||' rows set', c_PkgName||'.'||v_prcName);

    -- добавляем id платежа в таблицу разносок
    MERGE INTO EISUP_PAY_TRANSFER_T ET
    USING (
        SELECT RID, BRM_PAYMENT_ID, REP_PERIOD_ID
          FROM (
               SELECT ET.ROWID RID, EP.BRM_PAYMENT_ID, EP.PERIOD REP_PERIOD_ID,
                      ROW_NUMBER() OVER (PARTITION BY ET.ROWID ORDER BY EP.DOCUMENT_DATE DESC) RN
                 FROM EISUP_PAYMENT_T EP, EISUP_PAY_TRANSFER_T ET, EISUP_JOURNAL_T J
                WHERE EP.BRM_LOAD_CODE IN (c_CODE_BIND_OK, c_CODE_LOAD_OK)
                  AND EP.BRM_PAYMENT_ID IS NOT NULL
                  AND EP.DOCUMENT_ID = ET.DOCUMENT_ID
                  AND ET.JOURNAL_ID = p_journal_id
                  AND EP.JOURNAL_ID = J.JOURNAL_ID
                  AND J.STATUS IN (c_J_STATUS_OK, c_J_STATUS_NEW)
            )
         WHERE RN = 1     
    ) EPT
    ON (
          ET.ROWID = EPT.RID 
    )
    WHEN MATCHED THEN UPDATE SET ET.BRM_PAYMENT_ID    = EPT.BRM_PAYMENT_ID,
                                 ET.BRM_PAY_PERIOD_ID = EPT.REP_PERIOD_ID
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAY_TRANSFER_T.BRM_PAYMENT_ID: '||v_count||' rows set', c_PkgName||'.'||v_prcName);

    -- проставляем id и статус операции разноски
    UPDATE EISUP_PAY_TRANSFER_T ET
       SET ET.BRM_LOAD_CODE       = c_CODE_BIND_OK,
           ET.BRM_PAY_TRANSFER_ID = Pk02_Poid.Next_transfer_id
     WHERE ET.JOURNAL_ID          = p_journal_id
       AND ET.BRM_PAYMENT_ID      IS NOT NULL
       AND ET.BRM_PAY_PERIOD_ID   IS NOT NULL
       AND ET.BRM_BILL_ID         IS NOT NULL
       AND ET.BRM_BILL_PERIOD_ID  IS NOT NULL
       AND ET.BRM_LOAD_CODE       IS NULL
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAY_TRANSFER_T: '||v_count||' rows binded', c_PkgName||'.'||v_prcName);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Выполняем проверку данных операций разноски
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- 
    -- проверяем соответствие суммы разноски сумме платежа
    -- по хорошему нужно проверять сумму всех разносок, но для начала и так хорошо
    MERGE INTO EISUP_PAY_TRANSFER_T ET
     USING (
        SELECT ET.JOURNAL_ID, EP.DOCUMENT_ID, ET.BRM_BILL_ID, 
               ET.BRM_BILL_PERIOD_ID, EP.PAYMENT_AMOUNT, ET.TRANSFER_TOTAL  
          FROM EISUP_PAY_TRANSFER_T ET, EISUP_PAYMENT_T EP, EISUP_JOURNAL_T J 
         WHERE ET.JOURNAL_ID = p_journal_id
           AND EP.JOURNAL_ID = J.JOURNAL_ID
           AND J.STATUS IN (c_J_STATUS_NEW, c_J_STATUS_OK)
           AND EP.PAYMENT_AMOUNT < ET.TRANSFER_TOTAL
           AND EP.DOCUMENT_ID = ET.DOCUMENT_ID
           AND ET.BRM_BILL_ID IS NOT NULL
           AND ET.BRM_LOAD_CODE= c_CODE_BIND_OK
     ) ETT
    ON (
        ET.JOURNAL_ID  = ETT.JOURNAL_ID  AND
        ET.DOCUMENT_ID = ETT.DOCUMENT_ID AND 
        ET.BRM_BILL_ID = ETT.BRM_BILL_ID AND
        ET.BRM_BILL_PERIOD_ID = ETT.BRM_BILL_PERIOD_ID 
    )
    WHEN MATCHED THEN 
      UPDATE SET ET.BRM_LOAD_CODE =  c_CODE_BIND_ERR, 
                 ET.BRM_LOAD_NOTES = 'Ошибка. Сумма разноски превышает сумму платежа: '||
                 ETT.TRANSFER_TOTAL||' > '||ETT.PAYMENT_AMOUNT
    ;  
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAY_TRANSFER_T.Сумма разноски превышает сумму платежа: '||v_count||' rows set', c_PkgName||'.'||v_prcName);
    --
    -- проверяем соответствие суммы разноски сумме выставленного счета
    MERGE INTO EISUP_PAY_TRANSFER_T ET
    USING (
        SELECT ET.JOURNAL_ID, ET.DOCUMENT_ID, ET.BRM_BILL_ID, ET.BRM_BILL_PERIOD_ID, 
               B.BILL_NO, B.TOTAL, B.DUE, ET.TRANSFER_TOTAL, B.ADJUSTED 
          FROM EISUP_PAY_TRANSFER_T ET, BILL_T B
         WHERE ET.JOURNAL_ID    = p_journal_id
           AND ET.BRM_LOAD_CODE = c_CODE_BIND_OK
           AND ET.BRM_BILL_ID   = B.BILL_ID
           AND ET.BRM_BILL_PERIOD_ID = B.REP_PERIOD_ID
           AND B.TOTAL < ET.TRANSFER_TOTAL
     )ETT
    ON (
        ET.JOURNAL_ID  = ETT.JOURNAL_ID  AND
        ET.DOCUMENT_ID = ETT.DOCUMENT_ID AND 
        ET.BRM_BILL_ID = ETT.BRM_BILL_ID AND
        ET.BRM_BILL_PERIOD_ID = ETT.BRM_BILL_PERIOD_ID
    )
    WHEN MATCHED THEN UPDATE SET ET.BRM_LOAD_NOTES = 'Предупреждение. Сумма разноски превышает сумму счета: '||
                                                     ETT.TRANSFER_TOTAL||' > '||ETT.TOTAL
    ;  
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAY_TRANSFER_T.Сумма разноски превышает сумму счета: '||v_count||' rows set', c_PkgName||'.'||v_prcName);

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Добавить привязанные платежи на Л/С клиента 
--
PROCEDURE Add_payments(p_journal_id IN INTEGER)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Add_payments';
    v_count       INTEGER := 0;
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- создаем записи платежей
    INSERT INTO PAYMENT_T (
        PAYMENT_ID, REP_PERIOD_ID, PAYMENT_TYPE,
        PAYMENT_DATE, ACCOUNT_ID, RECVD,
        ADVANCE, ADVANCE_DATE, BALANCE, TRANSFERED, REFUND,
        DATE_FROM, DATE_TO,
        PAYSYSTEM_ID, DOC_ID,
        STATUS, STATUS_DATE, CREATE_DATE, LAST_MODIFIED,
        CREATED_BY, NOTES, Pay_Descr
    )
    SELECT 
          EP.BRM_PAYMENT_ID,
          EP.PERIOD,
          'PAYMENT' PAYMENT_TYPE,
          EP.PAYMENT_DATE,
          EP.BRM_ACCOUNT_ID,
          EP.PAYMENT_AMOUNT,
          EP.PAYMENT_AMOUNT,
          EP.PAYMENT_DATE,
          EP.PAYMENT_AMOUNT,
          0 TRANSFERED,
          0 REFUND, 
          NULL DATE_FROM,
          NULL DATE_TO,
          c_PAYSYSTEM_EISUP_ID,
          EP.DOCUMENT_ID,
          'OPEN',
          SYSDATE,SYSDATE,SYSDATE,
          'Импорт из ЕИСУП' CREATED_BY,
          NULL NOTES,
          EP.PAY_DESCR
      FROM EISUP_PAYMENT_T EP
     WHERE EP.BRM_LOAD_CODE = c_CODE_BIND_OK
       AND EP.JOURNAL_ID    = p_journal_id
       AND EP.BRM_PAYMENT_ID IS NOT NULL
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'PAYMENT_T: '||v_count||' - rows inserted', c_PkgName||'.'||v_prcName);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- меняем балансы лицевых счетов
    MERGE INTO ACCOUNT_T A
    USING (
        SELECT EP.BRM_ACCOUNT_ID, SUM(EP.PAYMENT_AMOUNT) PAY_AMOUNT 
          FROM EISUP_PAYMENT_T EP
         WHERE EP.BRM_LOAD_CODE = c_CODE_BIND_OK
           AND EP.JOURNAL_ID    = p_journal_id
           AND EP.BRM_PAYMENT_ID IS NOT NULL
         GROUP BY EP.BRM_ACCOUNT_ID
     ) EP
    ON (
        A.ACCOUNT_ID = EP.BRM_ACCOUNT_ID
    )
    WHEN MATCHED THEN UPDATE SET A.BALANCE = A.BALANCE + EP.PAY_AMOUNT,
                                 A.BALANCE_DATE = SYSDATE
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'ACCOUNT_T.BALANCE: '||v_count||' - rows updated', c_PkgName||'.'||v_prcName);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- меняем статус занрузки
    UPDATE EISUP_PAYMENT_T EP
       SET EP.BRM_LOAD_CODE = c_CODE_LOAD_OK
     WHERE EP.BRM_LOAD_CODE = c_CODE_BIND_OK
       AND EP.JOURNAL_ID    = p_journal_id
       AND EP.BRM_PAYMENT_ID IS NOT NULL
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAYMENT_T.BRM_LOAD_CODE: '||v_count||' - rows updated', c_PkgName||'.'||v_prcName);

    --
    Pk01_Syslog.Write_msg( v_count||' - rows processed', c_PkgName||'.'||v_prcName);
    --   
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Добавить разноску на привязанные платежи
--
PROCEDURE Pay_transfer(p_journal_id IN INTEGER)
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Pay_transfer';
    v_count         INTEGER := 0;
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- изменяем остаток на платежах
    MERGE INTO PAYMENT_T P
    USING (
       SELECT ET.BRM_PAYMENT_ID,
              ET.BRM_PAY_PERIOD_ID,
              SUM(ET.TRANSFER_TOTAL) TRANSFER_TOTAL
         FROM EISUP_PAY_TRANSFER_T ET
        WHERE ET.BRM_LOAD_CODE = c_CODE_BIND_OK
          AND ET.BRM_PAYMENT_ID IS NOT NULL
          AND ET.BRM_BILL_ID    IS NOT NULL
          AND ET.JOURNAL_ID    = p_journal_id
        GROUP BY ET.BRM_PAYMENT_ID,
                 ET.BRM_PAY_PERIOD_ID
    ) ET
    ON (
       P.PAYMENT_ID    = ET.BRM_PAYMENT_ID AND
       P.REP_PERIOD_ID = ET.BRM_PAY_PERIOD_ID
    )
    WHEN MATCHED THEN UPDATE 
                         SET P.TRANSFERED = P.TRANSFERED + ET.TRANSFER_TOTAL,
                             P.BALANCE    = P.BALANCE    - ET.TRANSFER_TOTAL
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'PAYMENT_T.TRANSFERED: '||v_count||' - rows merged', c_PkgName||'.'||v_prcName);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- изменяем задолженность по счетам 
    MERGE INTO BILL_T B
    USING (
       SELECT ET.BRM_BILL_ID,
              ET.BRM_BILL_PERIOD_ID,
              SUM(ET.TRANSFER_TOTAL) TRANSFER_TOTAL
         FROM EISUP_PAY_TRANSFER_T ET
        WHERE ET.BRM_LOAD_CODE = c_CODE_BIND_OK
          AND ET.BRM_PAYMENT_ID IS NOT NULL
          AND ET.BRM_BILL_ID    IS NOT NULL
          AND ET.JOURNAL_ID    = p_journal_id
        GROUP BY ET.BRM_BILL_ID,
                 ET.BRM_BILL_PERIOD_ID
    ) ET
    ON (
       B.REP_PERIOD_ID = ET.BRM_BILL_PERIOD_ID AND
       B.BILL_ID       = ET.BRM_BILL_ID
    )
    WHEN MATCHED THEN UPDATE 
                         SET B.RECVD = B.RECVD + ET.TRANSFER_TOTAL,
                             B.DUE   = B.DUE   -  ET.TRANSFER_TOTAL
    ;    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'BILL_T.RECVD: '||v_count||' - rows merged', c_PkgName||'.'||v_prcName);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- добавляем операции разноски
    INSERT INTO PAY_TRANSFER_T (
           TRANSFER_ID, 
           PAYMENT_ID, PAY_PERIOD_ID,
           BILL_ID, REP_PERIOD_ID,
           TRANSFER_TOTAL,
           TRANSFER_DATE, NOTES
    )
    SELECT ET.BRM_PAY_TRANSFER_ID,
           ET.BRM_PAYMENT_ID, ET.BRM_PAY_PERIOD_ID,
           ET.BRM_BILL_ID, ET.BRM_BILL_PERIOD_ID,
           ET.TRANSFER_TOTAL,
           SYSDATE, 'Импорт из ЕИСУП'
      FROM EISUP_PAY_TRANSFER_T ET
     WHERE ET.BRM_LOAD_CODE = c_CODE_BIND_OK
       AND ET.BRM_PAYMENT_ID IS NOT NULL
       AND ET.BRM_BILL_ID    IS NOT NULL
       AND ET.JOURNAL_ID    = p_journal_id
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'PAY_TRANSFER_T: '||v_count||' - rows inserted', c_PkgName||'.'||v_prcName);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- фиксируем изменения в исходной таблице
    UPDATE EISUP_PAY_TRANSFER_T ET
       SET ET.BRM_LOAD_CODE = c_CODE_LOAD_OK
     WHERE ET.BRM_LOAD_CODE = c_CODE_BIND_OK
       AND ET.BRM_PAYMENT_ID IS NOT NULL
       AND ET.BRM_BILL_ID    IS NOT NULL
       AND ET.JOURNAL_ID    = p_journal_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAY_TRANSFER_T: '||v_count||' - rows updated', c_PkgName||'.'||v_prcName);
    --   
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Удалить операции разноски, для записей с указанным статусом
--
PROCEDURE Delete_transfer( 
            p_journal_id IN INTEGER
          )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_transfer';
    v_count       INTEGER := 0;
BEGIN
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- изменяем остаток на платежах
    MERGE INTO PAYMENT_T P
    USING (
       SELECT ET.BRM_PAYMENT_ID,
              ET.BRM_PAY_PERIOD_ID,
              SUM(ET.TRANSFER_TOTAL) TRANSFER_TOTAL
         FROM EISUP_PAY_TRANSFER_T ET
        WHERE ET.BRM_LOAD_CODE = c_CODE_LOAD_OK
          AND ET.BRM_PAYMENT_ID IS NOT NULL
          AND ET.BRM_BILL_ID    IS NOT NULL
          AND ET.JOURNAL_ID    = p_journal_id
        GROUP BY ET.BRM_PAYMENT_ID,
                 ET.BRM_PAY_PERIOD_ID
    ) ET
    ON (
       P.PAYMENT_ID    = ET.BRM_PAYMENT_ID AND
       P.REP_PERIOD_ID = ET.BRM_PAY_PERIOD_ID
    )
    WHEN MATCHED THEN UPDATE 
                         SET P.TRANSFERED = P.TRANSFERED - ET.TRANSFER_TOTAL,
                             P.BALANCE    = P.BALANCE    + ET.TRANSFER_TOTAL
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'PAYMENT_T.TRANSFERED: '||v_count||' - rows merged', c_PkgName||'.'||v_prcName);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- изменяем задолженность по счетам 
    MERGE INTO BILL_T B
    USING (
       SELECT ET.BRM_BILL_ID,
              ET.BRM_BILL_PERIOD_ID,
              SUM(ET.TRANSFER_TOTAL) TRANSFER_TOTAL
         FROM EISUP_PAY_TRANSFER_T ET
        WHERE ET.BRM_LOAD_CODE = c_CODE_LOAD_OK
          AND ET.BRM_PAYMENT_ID IS NOT NULL
          AND ET.BRM_BILL_ID    IS NOT NULL
          AND ET.JOURNAL_ID    = p_journal_id
        GROUP BY ET.BRM_BILL_ID,
                 ET.BRM_BILL_PERIOD_ID
    ) ET
    ON (
       B.REP_PERIOD_ID = ET.BRM_BILL_PERIOD_ID AND
       B.BILL_ID       = ET.BRM_BILL_ID
    )
    WHEN MATCHED THEN UPDATE 
                         SET B.RECVD = B.RECVD - ET.TRANSFER_TOTAL,
                             B.DUE   = B.DUE   +  ET.TRANSFER_TOTAL
    ;    
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'BILL_T.RECVD: '||v_count||' - rows merged', c_PkgName||'.'||v_prcName);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- удаляем операции разноски
    DELETE FROM PAY_TRANSFER_T P
     WHERE EXISTS (
        SELECT * 
          FROM EISUP_PAY_TRANSFER_T ET
         WHERE ET.BRM_LOAD_CODE = c_CODE_LOAD_OK
           AND ET.BRM_PAYMENT_ID IS NOT NULL
           AND ET.BRM_BILL_ID    IS NOT NULL
           AND ET.JOURNAL_ID    = p_journal_id
           AND P.PAYMENT_ID     = ET.BRM_PAYMENT_ID 
           AND P.PAY_PERIOD_ID  = ET.BRM_PAY_PERIOD_ID
    );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'PAY_TRANSFER_T: '||v_count||' - rows deleted', c_PkgName||'.'||v_prcName);
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- фиксируем изменения в исходной таблице
    UPDATE EISUP_PAY_TRANSFER_T ET
       SET ET.BRM_LOAD_CODE = c_CODE_BIND_OK
     WHERE ET.BRM_LOAD_CODE = c_CODE_LOAD_OK
       AND ET.BRM_PAYMENT_ID IS NOT NULL
       AND ET.BRM_BILL_ID    IS NOT NULL
       AND ET.JOURNAL_ID    = p_journal_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAY_TRANSFER_T: '||v_count||' - rows updated', c_PkgName||'.'||v_prcName);
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Удалить операции разноски, для записей с указанным статусом
-- Отвязать записи от счетов 
PROCEDURE Reset_transfer( 
            p_journal_id IN INTEGER,
            p_load_code  IN INTEGER DEFAULT NULL -- по умолчанию весь журнал
          )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Reset_transfer';
    v_count       INTEGER := 0;
BEGIN
    -- удаляем операции разноски
    Delete_transfer( p_journal_id );

    -- отвязываем записи от счетов
    UPDATE EISUP_PAY_TRANSFER_T ET
       SET ET.BRM_LOAD_CODE       = NULL, 
           ET.BRM_LOAD_DATE       = NULL, 
           ET.BRM_PAY_TRANSFER_ID = NULL,
           ET.BRM_BILL_ID         = NULL,
           ET.BRM_BILL_PERIOD_ID  = NULL,
           ET.BRM_PAYMENT_ID      = NULL,
           ET.BRM_PAY_PERIOD_ID   = NULL
     WHERE ET.JOURNAL_ID          = p_journal_id
        AND (p_load_code IS NULL OR ET.BRM_LOAD_CODE = p_load_code)
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAY_TRANSFER_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName);
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Удалить платежи из журнала
--
PROCEDURE Delete_payments( 
            p_journal_id IN INTEGER
          )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_payments';
    v_count       INTEGER := 0;
BEGIN
    -- ------------------------------------------------------------ --
    -- вернуть балансы лицевым счетам
    MERGE INTO ACCOUNT_T A
    USING (
        SELECT EP.BRM_ACCOUNT_ID, SUM(EP.PAYMENT_AMOUNT) PAY_AMOUNT 
          FROM EISUP_PAYMENT_T EP
         WHERE EP.BRM_LOAD_CODE = c_CODE_LOAD_OK
           AND EP.JOURNAL_ID    = p_journal_id
           AND EP.BRM_PAYMENT_ID IS NOT NULL
         GROUP BY EP.BRM_ACCOUNT_ID
     ) EP
    ON (
        A.ACCOUNT_ID = EP.BRM_ACCOUNT_ID
    )
    WHEN MATCHED THEN UPDATE SET A.BALANCE = A.BALANCE - EP.PAY_AMOUNT,
                                 A.BALANCE_DATE = SYSDATE
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'ACCOUNT_T.BALANCE: '||v_count||' - rows updated', c_PkgName||'.'||v_prcName);

    -- ------------------------------------------------------------ --
    -- удалить платежи
    DELETE FROM PAYMENT_T P
     WHERE EXISTS (
        SELECT * FROM EISUP_PAYMENT_T EP
         WHERE EP.BRM_LOAD_CODE  = c_CODE_LOAD_OK
           AND EP.JOURNAL_ID     = p_journal_id
           AND EP.BRM_PAYMENT_ID = P.PAYMENT_ID
           AND EP.PERIOD         = P.REP_PERIOD_ID
     );
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'PAYMENT_T: '||v_count||' - rows deleted', c_PkgName||'.'||v_prcName);
    
    -- ------------------------------------------------------------ --
    -- возвращаем статус занрузки
    UPDATE EISUP_PAYMENT_T EP
       SET EP.BRM_LOAD_CODE = c_CODE_BIND_OK
     WHERE EP.BRM_LOAD_CODE = c_CODE_LOAD_OK
       AND EP.JOURNAL_ID    = p_journal_id
       AND EP.BRM_PAYMENT_ID IS NOT NULL
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAYMENT_T.BRM_LOAD_CODE: '||v_count||' - rows updated', c_PkgName||'.'||v_prcName);
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- Отвязать записи от лицевых счетов 
PROCEDURE Reset_payments( 
            p_journal_id IN INTEGER
          )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Reset_payments';
    v_count       INTEGER := 0;
BEGIN
    -- удаляем платежи
    Delete_payments(p_journal_id);  
    
    -- отвязываем записи от л/с
    UPDATE EISUP_PAYMENT_T EP
       SET BRM_LOAD_CODE   = NULL, 
           BRM_LOAD_DATE   = NULL, 
           BRM_ACCOUNT_ID  = NULL, 
           BRM_CONTRACT_ID = NULL, 
           BRM_CUSTOMER_ID = NULL, 
           BRM_PAYMENT_ID  = NULL
     WHERE EP.JOURNAL_ID = p_journal_id
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_PAYMENT_T: '||v_count||' rows updated', c_PkgName||'.'||v_prcName);
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ========================================================================== --
-- Работа с входным балансом
-- ========================================================================== --
PROCEDURE Load_in_balance
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Load_in_balance';
    v_count       INTEGER := 0;
BEGIN
    -- чистим табицу
    DELETE FROM EISUP_BALANCE_T;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T: '||v_count||' rows deleted', c_PkgName||'.'||v_prcName);

    -- перезаливка данных из временной таблицы
    INSERT INTO EISUP_BALANCE_T (CONTRACT_NO, ACCOUNT_NO, ERP_CODE, BALANCE, BALANCE_PERIOD, CREATE_DATE)
    SELECT CONTRACT_NO, ACCOUNT_NO, ERP_CODE, BALANCE, BALANCE_PERIOD, CREATE_DATE 
      FROM EISUP_BALANCE_TMP;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T: '||v_count||' rows inserted', c_PkgName||'.'||v_prcName);

    -- привязка балансов к данным биллинга
    MERGE INTO EISUP_BALANCE_T EB
    USING (
        SELECT ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, 
               ACCOUNT_NO, CONTRACT_NO, ERP_CODE, 
               BILLING_ID, EB_RID
        FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY EB_RID 
                                          ORDER BY DECODE(CONTRACT_ID, NULL, 0, 1) DESC,
                                                   DATE_FROM DESC
                                     ) RN,
                   ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, CONTRACT_NO, ACCOUNT_NO, ERP_CODE, BILLING_ID,
                   DATE_FROM, DATE_TO,
                   EB_RID
              FROM (
                  SELECT EB.ROWID EB_RID,
                         A.ACCOUNT_ID, 
                         CS.ERP_CODE,  CS.CUSTOMER_ID,
                         DECODE(UPPER(TRIM(C.CONTRACT_NO)),UPPER(TRIM(EB.CONTRACT_NO)),C.CONTRACT_ID, NULL) CONTRACT_ID,
                         EB.CONTRACT_NO, EB.ACCOUNT_NO, A.BILLING_ID, AP.DATE_FROM, AP.DATE_TO
                    FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A, 
                         CONTRACT_T C, CUSTOMER_T CS, 
                         EISUP_BALANCE_T EB
                   WHERE AP.ACCOUNT_ID  = A.ACCOUNT_ID
                     AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
                     AND AP.CONTRACT_ID = C.CONTRACT_ID
                     --AND AP.DATE_FROM  <= EB.BALANCE_PERIOD
                     --AND (AP.DATE_TO IS NULL OR EB.BALANCE_PERIOD <= AP.DATE_TO )
                     AND A.ACCOUNT_NO   = EB.ACCOUNT_NO
                     AND CS.ERP_CODE    = TRIM(EB.ERP_CODE)
            )
        )
        WHERE RN = 1
        /*    
        SELECT ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, CONTRACT_NO, ACCOUNT_NO, ERP_CODE, BILLING_ID
        FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID, CONTRACT_ID ORDER BY NVL(CUSTOMER_ID,0) DESC) RN,
                   ACCOUNT_ID, CONTRACT_ID, CUSTOMER_ID, CONTRACT_NO, ACCOUNT_NO, ERP_CODE, BILLING_ID
              FROM (
                  SELECT A.ACCOUNT_ID, C.CONTRACT_ID, 
                         DECODE(TRIM(CS.ERP_CODE), TRIM(EB.ERP_CODE), CS.CUSTOMER_ID, NULL) CUSTOMER_ID,
                         EB.CONTRACT_NO, EB.ACCOUNT_NO, TRIM(EB.ERP_CODE) ERP_CODE, A.BILLING_ID
                    FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A, CONTRACT_T C, CUSTOMER_T CS, EISUP_BALANCE_T EB
                   WHERE AP.ACCOUNT_ID  = A.ACCOUNT_ID
                     AND AP.CONTRACT_ID = C.CONTRACT_ID
                     AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
                     AND AP.DATE_FROM  <= EB.BALANCE_PERIOD
                     AND (AP.DATE_TO IS NULL OR EB.BALANCE_PERIOD <= AP.DATE_TO )
                     AND A.ACCOUNT_NO   = EB.ACCOUNT_NO
                     AND C.CONTRACT_NO  = EB.CONTRACT_NO
            )
        )
        WHERE RN = 1
        */
    ) EBT
    ON (
        --EBT.CONTRACT_NO = EB.CONTRACT_NO AND  
        --EBT.ACCOUNT_NO  = EB.ACCOUNT_NO
        EBT.EB_RID = EB.ROWID
    )
    WHEN MATCHED THEN UPDATE SET EB.BRM_ACCOUNT_ID  = EBT.ACCOUNT_ID, 
                                 EB.BRM_CONTRACT_ID = EBT.CONTRACT_ID, 
                                 EB.BRM_CUSTOMER_ID = EBT.CUSTOMER_ID, 
                                 EB.BRM_LOAD_STATUS = c_J_STATUS_OK,
                                 EB.BRM_LOAD_DATE   = SYSDATE,
                                 EB.BRM_BILLING_ID  = EBT.BILLING_ID,
                                 EB.BRM_PERIOD_ID   = TO_CHAR(EB.BALANCE_PERIOD,'yyyymm')
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T: '||v_count||' rows binded', c_PkgName||'.'||v_prcName);
    --
    -- проверяем на задвоение записей л/с
    UPDATE EISUP_BALANCE_T EB SET EB.BRM_LOAD_STATUS = 'ERR.DUP'
     WHERE EB.BRM_ACCOUNT_ID IN (
        SELECT BRM_ACCOUNT_ID FROM EISUP_BALANCE_T
         WHERE BRM_BILLING_ID != 2003 
         GROUP BY BRM_ACCOUNT_ID
         HAVING COUNT(*) > 1
     )
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T: '||v_count||' rows duplicated', c_PkgName||'.'||v_prcName);
    /*
    -- проставляем id корректировочных платежей 
    UPDATE EISUP_BALANCE_T EB 
       SET EB.BRM_PAYMENT_ID = Pk02_Poid.Next_payment_id
     WHERE EB.BRM_BILLING_ID != 2003
       AND EB.BRM_ACCOUNT_ID IS NOT NULL
       AND EB.BRM_LOAD_STATUS = c_J_STATUS_OK;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'EISUP_BALANCE_T: '||v_count||' rows set payment_id', c_PkgName||'.'||v_prcName);
    */
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- пересчитать балансы счетов для которых установлен входящий баланс
-- ------------------------------------------------------------------------ --
PROCEDURE Recalc_for_inBalances
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Recalc_for_inBalances';
    v_count       INTEGER := 0;
BEGIN
    -- пересчитываем балансы л/с
    MERGE INTO ACCOUNT_T A
    USING   
       (
       SELECT ACCOUNT_ID, SUM(RECVD-BILL_TOTAL) BALANCE,
               CASE
                   WHEN MAX(BILL_DATE) > MAX(PAYMENT_DATE) THEN MAX(BILL_DATE)
                   ELSE MAX(PAYMENT_DATE)
               END BALANCE_DATE 
        FROM (
            -- получаем полную задолженность по выставленным счетам
            SELECT B.ACCOUNT_ID, 
                   B.TOTAL BILL_TOTAL, BILL_DATE, 
                   0 RECVD, TO_DATE('01.01.2000','dd.mm.yyyy') PAYMENT_DATE 
              FROM BILL_T B, INCOMING_BALANCE_T IB
             WHERE B.ACCOUNT_ID    = IB.ACCOUNT_ID
               AND B.BILL_DATE     > IB.BALANCE_DATE
               AND B.REP_PERIOD_ID > IB.REP_PERIOD_ID
            UNION ALL
            -- получаем сумму поступивших за период платежей
            SELECT P.ACCOUNT_ID, 
                   0 BILL_TOTAL, TO_DATE('01.01.2000','dd.mm.yyyy') BILL_DATE,
                   P.RECVD, P.PAYMENT_DATE  
              FROM PAYMENT_T P, INCOMING_BALANCE_T IB
             WHERE P.ACCOUNT_ID    = IB.ACCOUNT_ID
               AND P.PAYMENT_DATE  > IB.BALANCE_DATE
               AND P.REP_PERIOD_ID > IB.REP_PERIOD_ID
            UNION ALL
            -- учитываем входящий баланс
            SELECT IB.BALANCE,
                   IB.BALANCE BILL_TOTAL, IB.BALANCE_DATE BILL_DATE, 
                   0 RECVD, TO_DATE('01.01.2000','dd.mm.yyyy') PAYMENT_DATE
              FROM INCOMING_BALANCE_T IB
        )
        GROUP BY ACCOUNT_ID
    ) T
    ON (
       A.ACCOUNT_ID = T.ACCOUNT_ID
    )
    WHEN MATCHED THEN UPDATE SET A.BALANCE_DATE = T.BALANCE_DATE, A.BALANCE = T.BALANCE;
    --
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Updated '||v_count||' rows in ACCOUNT_T', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- применить входящий баланс в биллинге
-- ------------------------------------------------------------------------ --
PROCEDURE Set_in_balance
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Set_in_balance';
    v_count       INTEGER := 0;
BEGIN
    -- заполняем таблицу с входящими балансами л/с
    MERGE INTO INCOMING_BALANCE_T IB
    USING (
        SELECT EB.BRM_ACCOUNT_ID, EB.BALANCE, EB.BALANCE_PERIOD
          FROM EISUP_BALANCE_T EB 
         WHERE EB.BRM_BILLING_ID != 2003
           AND EB.BRM_ACCOUNT_ID IS NOT NULL
           AND EB.BRM_LOAD_STATUS = c_J_STATUS_OK
    ) EB
    ON(
        IB.ACCOUNT_ID = EB.BRM_ACCOUNT_ID
    )
    WHEN MATCHED THEN UPDATE SET IB.BALANCE = EB.BALANCE, 
                                 IB.BALANCE_DATE = EB.BALANCE_PERIOD,
                                 IB.REP_PERIOD_ID = TO_CHAR(EB.BALANCE_PERIOD ,'yyyymm')
    WHEN NOT MATCHED THEN INSERT (ACCOUNT_ID, BALANCE, BALANCE_DATE, REP_PERIOD_ID)
                          VALUES (EB.BRM_ACCOUNT_ID, EB.BALANCE, EB.BALANCE_PERIOD,
                                 TO_NUMBER(TO_CHAR(EB.BALANCE_PERIOD ,'yyyymm'))
                                 )    
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'INCOMING_BALANCE_T: '||v_count||' - rows inserted', c_PkgName||'.'||v_prcName);

    -- ------------------------------------------------------------------------ --    
    -- пересчитать балансы счетов для которых установлен входящий баланс
    Recalc_for_inBalances;
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ------------------------------------------------------------------------ --
-- удалить входящий баланс из биллинга
-- ------------------------------------------------------------------------ --
PROCEDURE Delete_in_balance
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_in_balance';
    v_count       INTEGER := 0;
BEGIN
    -- сбрасываем входящие балансы   
    UPDATE INCOMING_BALANCE_T IB SET IB.BALANCE = 0;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'INCOMING_BALANCE_T: '||v_count||' - rows cleaned', c_PkgName||'.'||v_prcName);

    -- пересчитываем балансы счетов 
    Recalc_for_inBalances;

    -- удаляем записи из таблицы
    DELETE FROM INCOMING_BALANCE_T IB;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg( 'INCOMING_BALANCE_T: '||v_count||' - rows deleted', c_PkgName||'.'||v_prcName);
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ========================================================================== --

-- ======================================================================== --
-- Просмотр чего-нибудь (шаблон)
--   - при ошибке выставляет исключение
-- ------------------------------------------------------------------------ --
PROCEDURE Eisup_transfer_list (
               p_recordset    OUT t_refc, 
               p_journal_id   IN INTEGER,
               p_load_code    IN INTEGER DEFAULT NULL
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Eisup_transfer_list';
    v_retcode    INTEGER;
BEGIN

    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
          SELECT *
            FROM EISUP_PAYMENT_T EP
           WHERE EP.JOURNAL_ID = p_journal_id
             AND (p_load_code IS NULL OR EP.BRM_LOAD_CODE = p_load_code)
          ORDER BY EP.RECORD_ID;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ------------------------------------------------------------------------ --
-- Статистика по приязке платежей журнала
-- ------------------------------------------------------------------------ --
PROCEDURE Payments_bind_stat (
               p_recordset    OUT t_refc, 
               p_journal_id   IN INTEGER
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Payments_bind_stat';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор (поля можем изменить по необходимости)
    OPEN p_recordset FOR
        SELECT PERIOD, BRM_LOAD_CODE, 
               DECODE(BRM_ACCOUNT_ID, NULL, 0, 1)  BRM_ACCOUNT_ID,
               DECODE(BRM_CONTRACT_ID, NULL, 0, 1) BRM_CONTRACT_ID, 
               DECODE(BRM_CUSTOMER_ID, NULL, 0, 1) BRM_CUSTOMER_ID,
               COUNT(*) FROM EISUP_PAYMENT_T EP
         WHERE EP.JOURNAL_ID = p_journal_id
           --AND BRM_LOAD_CODE IS NOT NULL
         GROUP BY PERIOD, BRM_LOAD_CODE, 
                  DECODE(BRM_ACCOUNT_ID, NULL, 0, 1), 
                  DECODE(BRM_CONTRACT_ID, NULL, 0, 1),
                  DECODE(BRM_CUSTOMER_ID, NULL, 0, 1)
        ORDER BY PERIOD, BRM_LOAD_CODE
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;



END PK10_PAYMENT_EISUP;
/
