CREATE OR REPLACE PACKAGE PK403_LCR_DATA
IS
    --
    -- Д А Н Н Ы Е   Д Л Я   Э К С П О Р Т А   В   L C R  ( И. Юарасев )
    --
    -- 10.110.32.160
    -- user: lcr
    -- passwd: a12345lcr
    -- /home/lcr
    --
    -- ==============================================================================
    c_PkgName   CONSTANT varchar2(30) := 'PK403_LCR_DATA';
    -- ==============================================================================
    c_RET_OK    CONSTANT integer := 0;
    c_RET_ER		CONSTANT integer :=-1;
    
    TYPE t_refc IS REF CURSOR;
    
    -- --------------------------------------------------------------------------------- --
    -- 1. Справочник заказов
    PROCEDURE Export_orders;
    -- прочитать заказы из временной таблицы    
    PROCEDURE Read_orders( p_recordset OUT t_refc );
    -- Выгрузить таблицу заказов в CSV файл
    PROCEDURE export_Orders_to_file;


    -- --------------------------------------------------------------------------------- --
    -- 2. Данные о вызовах
    PROCEDURE view_BDR( 
                   p_recordset OUT t_refc,
                   p_date_from IN DATE,
                   p_date_to   IN DATE
               );
               
    -- --------------------------------------------------------------------------------- --
    -- 3. Экспорт в BDR файл 
    /*
        p_Date_From   - начало rep_period, за который выгружаются данные
        p_Date_To     - конец rep_period, за который выгружаются данные
        p_Contract_No - номер договора
    */
    FUNCTION export_BDR_to_file(p_Date_From   date     DEFAULT NULL,
                                p_Date_To     date     DEFAULT NULL,
                                p_Contract_No varchar2 DEFAULT NULL
                               ) RETURN INTEGER;
    
    -- Экспорт всех вновь созданных BDR по всем выгрузкам в файлы 
    PROCEDURE export_all_BDRs;
    
    -- --------------------------------------------------------------------------------- --
    -- Контроль за процессом отгрузки по логам задач
    PROCEDURE export_Ctrl( p_recordset OUT t_refc );

    -- --------------------------------------------------------------------------------- --
    -- Контроль за процессом отгрузки по системе логирования биллинга
    PROCEDURE export_Ctrl_by_syslog( p_recordset OUT t_refc );
    
    -- --------------------------------------------------------------------------------- --
    -- Выгрузить таблицу счетов в CSV файл
    -- --------------------------------------------------------------------------------- --
    PROCEDURE export_Bills_to_file( p_period_id IN INTEGER );
    
    FUNCTION test_BDR_to_file RETURN INTEGER;
    
END ;
/
CREATE OR REPLACE PACKAGE BODY PK403_LCR_DATA
IS

-- --------------------------------------------------------------------------------- --
-- 1. Справочник заказов
-- --------------------------------------------------------------------------------- --
--  1.    Заказы, загружаемым в LCR из «нового биллинга».
-- Структура таблицы
-- Наименование поля    Тип данных      Описание
-- ID_ACCOUNT           DECIMAL(38,0)   Идентификатор заказа (аккаунта)
-- ORDER_NUM            VARCHAR(60)     Номер заказа
-- CONTRACT_NUM         VARCHAR(60)     Номер договора
-- CLIENT_NAME          VARCHAR(1000)   Наименование клиента
-- BRAND                VARCHAR(255)    Бренд
-- BUSINESS_TYPE        TINYINT         Тип клиента (физлицо – 1, юрлицо – 2)
-- --------------------------------------------------------------------------------- --
PROCEDURE export_Orders
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'export_Orders';
    v_count      INTEGER;
BEGIN
    -- удаляем старые данные
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PK403_LCR_ORDERS DROP STORAGE';
    -- заполняем таблицу
    INSERT INTO PK403_LCR_ORDERS(
        ORDER_ID, ORDER_NO, CONTRACT_NO, CLIENT_NAME, BRAND, BUSINESS_TYPE, SALE_CURATOR
    )
    WITH AC AS (
        SELECT O.ORDER_ID, O.ORDER_NO, C.CONTRACT_NO,
               CT.CONTRACTOR BRAND,
               DECODE(A.ACCOUNT_TYPE,'P',1,2) BUSINESS_TYPE,
               AP.CUSTOMER_ID, AP.SUBSCRIBER_ID,
               PK402_BCR_FILE.Get_sales_curator (
                 p_branch_id     => NULL,
                 p_agent_id      => NULL,
                 p_contract_id   => AP.CONTRACT_ID,
                 p_account_id    => AP.ACCOUNT_ID,
                 p_order_id      => O.ORDER_ID,
                 p_date          => SYSDATE
               ) SALE_CURATOR
          FROM ORDER_T O, ACCOUNT_T A, ACCOUNT_PROFILE_T AP,
               CONTRACT_T C, CONTRACTOR_T CT
        WHERE A.ACCOUNT_ID = O.ACCOUNT_ID
          AND A.ACCOUNT_ID = AP.ACCOUNT_ID
          AND AP.DATE_FROM <= SYSDATE
          AND (AP.DATE_TO IS NULL OR SYSDATE < AP.DATE_TO)
          AND AP.CONTRACT_ID = C.CONTRACT_ID
          AND CT.CONTRACTOR_ID(+) = NVL(AP.AGENT_ID,AP.BRANCH_ID)
          AND A.ACCOUNT_TYPE IN ('P','J')
          AND O.SERVICE_ID IN (1,2,7)                             -- Исправлено К. Иванов 09.07.2015
    )
    SELECT AC.ORDER_ID, AC.ORDER_NO, AC.CONTRACT_NO,
           CS.CUSTOMER CLIENT_NAME,
           AC.BRAND,
           AC.BUSINESS_TYPE,
           AC.SALE_CURATOR
      FROM AC, CUSTOMER_T CS
     WHERE AC.CUSTOMER_ID = CS.CUSTOMER_ID
       AND AC.BUSINESS_TYPE = 2
    UNION ALL
    SELECT AC.ORDER_ID, AC.ORDER_NO, AC.CONTRACT_NO,
           SB.LAST_NAME||' '||SB.FIRST_NAME||' '||SB.MIDDLE_NAME CLIENT_NAME,
           AC.BRAND,
           AC.BUSINESS_TYPE,
           AC.SALE_CURATOR
      FROM AC, SUBSCRIBER_T SB
     WHERE AC.SUBSCRIBER_ID = SB.SUBSCRIBER_ID
       AND AC.BUSINESS_TYPE = 1
    ;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('Inserted '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------------------- --
-- Прочитать, то что заполнили предыдущей процедурой
-- --------------------------------------------------------------------------------- --
PROCEDURE read_Orders(
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'read_Orders';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
      SELECT ORDER_ID, ORDER_NO, CONTRACT_NO, CLIENT_NAME, BRAND, BUSINESS_TYPE
        FROM PK403_LCR_ORDERS
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- --------------------------------------------------------------------------------- --
-- Выгрузить таблицу заказов в CSV файл
-- --------------------------------------------------------------------------------- --
PROCEDURE export_Orders_to_file
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'export_Orders_to_file';
    v_output     UTL_FILE.file_type;
    v_dir        VARCHAR2(100) := 'LCR_DIR';
    v_file_name  VARCHAR2(100) := 'orders.csv';
    v_file_tmp   VARCHAR2(100) := 'orders.tmp';
    v_count      INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W' );
    FOR ord IN (
      SELECT ORDER_ID||';'||ORDER_NO||';'||CONTRACT_NO||';'||
             REPLACE(CLIENT_NAME, ';', ',')||';'||BRAND||';'||BUSINESS_TYPE||';'||
             SALE_CURATOR||';' TXT
      FROM (
          WITH AC AS (
              SELECT O.ORDER_ID, O.ORDER_NO, C.CONTRACT_NO,
                     CT.CONTRACTOR BRAND,
                     DECODE(A.ACCOUNT_TYPE,'P',1,2) BUSINESS_TYPE,
                     AP.CONTRACT_ID, AP.SUBSCRIBER_ID,
                     PK402_BCR_FILE.Get_sales_curator (
                       p_branch_id     => NULL,
                       p_agent_id      => NULL,
                       p_contract_id   => AP.CONTRACT_ID,
                       p_account_id    => AP.ACCOUNT_ID,
                       p_order_id      => O.ORDER_ID,
                       p_date          => SYSDATE
                     ) SALE_CURATOR
                FROM ORDER_T O, ACCOUNT_T A, ACCOUNT_PROFILE_T AP,
                     CONTRACT_T C, CONTRACTOR_T CT
              WHERE A.ACCOUNT_ID = O.ACCOUNT_ID
                AND A.ACCOUNT_ID = AP.ACCOUNT_ID
                AND AP.ACTUAL = 'Y'
                AND AP.CONTRACT_ID = C.CONTRACT_ID
          AND CT.CONTRACTOR_ID(+) = NVL(AP.AGENT_ID,AP.BRANCH_ID) 
                AND A.ACCOUNT_TYPE IN ('P','J')
                AND A.BILLING_ID IN (Pk00_Const.c_BILLING_MMTS, 
                                     Pk00_Const.c_BILLING_OLD, 
                                     Pk00_Const.c_BILLING_KTTK,
                                     2009)
                AND O.SERVICE_ID IN (1,2,7)                       -- Исправлено К. Иванов 09.07.2015
          )
          SELECT AC.ORDER_ID, AC.ORDER_NO, AC.CONTRACT_NO,
                 COMP.COMPANY_NAME CLIENT_NAME,
                 AC.BRAND,
                 AC.BUSINESS_TYPE,
                 AC.SALE_CURATOR
            FROM AC, COMPANY_T COMP
           WHERE AC.CONTRACT_ID = COMP.CONTRACT_ID
             AND COMP.ACTUAL = 'Y'
             AND AC.BUSINESS_TYPE = 2
          UNION ALL
          SELECT AC.ORDER_ID, AC.ORDER_NO, AC.CONTRACT_NO,
                 SB.LAST_NAME||' '||SB.FIRST_NAME||' '||SB.MIDDLE_NAME CLIENT_NAME,
                 AC.BRAND,
                 AC.BUSINESS_TYPE,
                 AC.SALE_CURATOR
            FROM AC, SUBSCRIBER_T SB
           WHERE AC.SUBSCRIBER_ID = SB.SUBSCRIBER_ID
             AND AC.BUSINESS_TYPE = 1
      )
    ) LOOP
        UTL_FILE.put_line( v_output, ord.txt ) ;
        v_count := v_count + 1;
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('LCR.ORDERS: '||v_file_name||' - '||v_count||' rows - OK', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------------------- --
-- 2. Данные о вызовах
-- --------------------------------------------------------------------------------- --
PROCEDURE view_BDR(
               p_recordset OUT t_refc,
               p_date_from IN DATE,
               p_date_to   IN DATE
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'view_BDR';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
        SELECT C.I_ANS_TIME GMT_TIME, -- время вызова по GMT
               B.LOCAL_TIME,    -- местное время вызова
               F.FILE_NAME,     -- имя файла
               C.I_CSN,         -- CSN
               C.CDR_ID,        -- ID CDR
               B.ORDER_ID,      -- ID заказа
               O.ORDER_NO,      -- номер заказа
               B.PREFIX_B,      -- код напрвления доступа
               B.PRICE,         -- цена
               B.BILL_MINUTES,  -- кол-во тарифных минут
               B.AMOUNT,        -- сумма за вызов
               B.TERM_Z_NAME,   -- имя тарифной зоны
               R.RATEPLAN_NAME,--T.TRF_NAME,      -- имя тарифного плана
               810 CURRENCY_ID, -- ID валюты
               B.SAVE_DATE      -- дата начала выгрузки
          FROM BDR_VOICE_T B,
               ORDER_T O,
               --TARIFF_PH.D41_TRF_HEADER T,
               RATEPLAN_T r,
               MDV.T03_MMTS_CDR C,
               MDV.T01_CDR_FILES F
        WHERE B.REP_PERIOD BETWEEN p_date_from AND p_date_to
           AND C.I_ANS_TIME BETWEEN p_date_from AND p_date_to
           AND C.CDR_ID      = B.CDR_ID
           AND NVL(b.BDR_TYPE_ID,1) = 1
           AND B.ORDER_ID    = O.ORDER_ID
          -- AND B.TRF_ID      = T.TRF_ID
           AND B.RATEPLAN_ID = R.RATEPLAN_ID
           AND C.CDR_FILE_ID = F.CDR_FILE_ID
           ORDER BY C.I_ANS_TIME
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- --------------------------------------------------------------------------------- --
-- Экспорт в BDR файл
-- --------------------------------------------------------------------------------- --
/*
    p_Date_From   - начало rep_period, за который выгружаются данные
    p_Date_To     - конец rep_period, за который выгружаются данные
    p_Contract_No - номер договора
*/
FUNCTION export_BDR_to_file(p_Date_From   date     DEFAULT NULL,
                            p_Date_To     date     DEFAULT NULL,
                            p_Contract_No varchar2 DEFAULT NULL
                           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'export_BDR_to_file';
    
    v_count      INTEGER;
    v_export_id  INTEGER;
    --
    v_date_from     DATE;
    v_date_to       DATE;
    v_date_save     DATE;
    v_bdr_save_date date;
    v_num_rows      INTEGER;
    --
    v_output     UTL_FILE.file_type;
    v_dir        VARCHAR2(100) := 'LCR_DIR';
    v_file_name  VARCHAR2(100 CHAR);
    v_file_tmp   VARCHAR2(100 CHAR) := 'bdr.tmp';
    v_file_end   VARCHAR2(100 CHAR) := 'TheEnd';
    -- флаги файла
    v_fexists      BOOLEAN;
    v_file_length  NUMBER;
    v_block_size   BINARY_INTEGER;
    
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

   -- проверка на корректность введенных данных
    IF p_Date_From IS NOT NULL AND p_Date_To IS NULL THEN
        RAISE_APPLICATION_ERROR(-20000, 'Date_to not found');
    ELSIF p_Date_From IS NULL AND p_Date_To IS NOT NULL THEN
        RAISE_APPLICATION_ERROR(-20001, 'Date_from not found');
    ELSIF p_Date_From > p_Date_To THEN        
        RAISE_APPLICATION_ERROR(-20002, 'Date_to less then date_from');
    END IF;    
        
    IF p_Date_From IS NULL THEN
        -- читаем период, начиная с которого могут происходить изменения
        SELECT MIN(PERIOD_FROM), MAX(PERIOD_TO)
          INTO v_date_from, v_date_to
          FROM PERIOD_T
         WHERE POSITION IN (Pk00_Const.c_PERIOD_OPEN, Pk00_Const.c_PERIOD_BILL);

        -- получаем дату тарификации последнего выгруженного блока CDR
        SELECT MAX(BDR_SAVE_DATE) BDR_SAVE_DATE
          INTO v_date_save
          FROM PK403_LCR_EXPORT_BDR
         WHERE EXPORT_STATUS = 'OK'
           AND NOTES IS NULL;
    ELSE
        v_Date_From := p_Date_From;
        v_Date_To   := p_Date_To;
        v_date_save := NULL; 
    END IF;


    -- получаем информацию об очередной порции данных
  /*  WITH E AS (
        SELECT E.SAVE_DATE,
               MIN(E.REP_PERIOD)  DATE_FROM,
               MAX(E.REP_PERIOD) DATE_TO,
               COUNT(*) NUM_ROWS
          FROM E04_BDR_MMTS_T E
         WHERE E.REP_PERIOD BETWEEN v_date_from AND v_date_to
           AND E.SAVE_DATE > v_date_save
           AND NVL(E.BDR_TYPE,1) = 1
         GROUP BY E.SAVE_DATE
        ORDER BY  E.SAVE_DATE
    )
    SELECT E.SAVE_DATE, E.DATE_FROM, E.DATE_TO, E.NUM_ROWS
      INTO v_save_date, v_date_from, v_date_to, v_num_rows
      FROM E
    WHERE ROWNUM = 1; */

    SELECT --+ parallel(e 10)
           MAX(E.SAVE_DATE),
           MIN(E.REP_PERIOD) DATE_FROM,
           MAX(E.REP_PERIOD) DATE_TO,
           COUNT(*) NUM_ROWS
      INTO v_bdr_save_date, v_date_from, v_date_to, v_num_rows
      FROM BDR_VOICE_T E,
           ACCOUNT_T a
     WHERE E.REP_PERIOD BETWEEN v_date_from AND v_date_to
       AND (E.MODIFY_DATE > v_date_save OR v_date_save IS NULL) -- E.SAVE_DATE до 09.10.2015
       AND NVL(E.BDR_TYPE_ID,1) = 1
       AND e.account_id = a.account_id
       AND a.billing_id IN (Pk00_Const.c_BILLING_MMTS, -- 2003 
                            Pk00_Const.c_BILLING_OLD, -- 2002 
                            Pk00_Const.c_BILLING_KTTK, --2001
                            2009
                           ) 
       AND (p_Contract_No IS NULL 
              OR -- если задан номер договора, то получаем по нему будет поиск
            EXISTS (SELECT a.account_id
                      FROM contract_t c,
                           account_profile_t p
                     WHERE c.contract_no = p_Contract_No
                       AND c.contract_id = p.contract_id 
                       AND p.account_id = e.account_id)  
           );

    --  получаем ID задачи экспорта
    v_export_id := SQ_LCR_EXPORT_ID.NEXTVAL;

    -- формируем имя файла
    IF p_Contract_No IS NOT NULL THEN
        v_file_name := 'bdr_'||p_contract_no||'_'||TO_CHAR(NVL(p_Date_From,v_bdr_save_date), 'yyyymmddhh24mi')||'_'|| TO_CHAR(v_export_id)||'.csv';
    ELSIF p_Contract_No IS NULL AND p_Date_From IS NOT NULL THEN
        v_file_name := 'bdr_'||TO_CHAR(p_Date_From, 'yyyymmddhh24mi')||'_'|| TO_CHAR(v_export_id)||'.csv';        
    ELSE
        v_file_name := 'bdr_'||TO_CHAR(v_bdr_save_date, 'yyyymmddhh24mi')||'.csv';
    END IF;    

    -- фиксируем начало операции
    INSERT INTO PK403_LCR_EXPORT_BDR (
        EXPORT_ID, BDR_SAVE_DATE,
        FILE_NAME, FILE_SIZE, FILE_DATE, EXPORT_DATE, EXPORT_STATUS,
        DATE_FROM, DATE_TO, 
        NOTES
    )VALUES(
        v_export_id, v_bdr_save_date, v_file_name, v_num_rows, SYSDATE, SYSDATE, NULL,
        v_date_from, v_date_to, 
        NVL2(p_Date_From,TO_CHAR(p_Date_From,'dd.mm.yyyy hh24:mi:ss') || '-' || TO_CHAR(p_Date_To,'dd.mm.yyyy hh24:mi:ss'),NULL) ||
        NVL2(p_Contract_No, ', Cntr_No: ' || p_Contract_No,NULL) 
    );

    COMMIT;

    -- ------------------------------------------------------------------ --
    -- Удаляем флаг окончания предыдущей загрузки, если он устанвлен
    -- ------------------------------------------------------------------ --
    UTL_FILE.fgetattr(v_dir, v_file_end, v_fexists, v_file_length, v_block_size);
    IF v_fexists = TRUE THEN
        UTL_FILE.fremove( v_dir, v_file_end );
    END IF;

    -- ------------------------------------------------------------------ --
    -- записываем информацию в Файл
    -- ------------------------------------------------------------------ --

    v_count := 0;
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W' );
    FOR bdr IN (
       -- до 16.09.2015
       --  SELECT /*+ ordered */
       /*        TO_CHAR(C.I_ANS_TIME,'yyyy.mm.dd hh24:mi:ss')||';'|| -- время вызова по GMT
               SUBSTR(F.FILE_NAME, 1, 14)||'.DET'||';'|| -- имя файла - VARCHAR2(100)
               C.I_CSN||';'||         -- CSN - INTEGER
               C.CDR_ID||';'||        -- ID CDR - INTEGER
               B.ORDER_ID||';'||      -- ID заказа - INTEGER
               O.ORDER_NO||';'||      -- номер заказа - VARCHAR2(60)
               B.PREFIX_B||';'||      -- код напрвления доступа - VARCHAR2(34)
               B.PRICE||';'||         -- цена - NUMBER
               B.BILL_MINUTES||';'||  -- кол-во тарифных минут - NUMBER
               B.AMOUNT||';'||        -- сумма за вызов - NUMBER
               B.TERM_Z_NAME||';'||   -- имя тарифной зоны - VARCHAR2(200)
               T.TRF_NAME||';'||      -- имя тарифного плана
               '810'||';'||           -- ID валюты - INTEGER
               TO_CHAR(B.SAVE_DATE, 'yyyy.mm.dd hh24:mi:ss')||';'|| -- дата начала выгрузки - DATE
               TO_CHAR(B.LOCAL_TIME,'yyyy.mm.dd hh24:mi:ss')||';'||   -- местное время вызова
               TO_CHAR(O.DATE_FROM,'yyyy.mm.dd hh24:mi:ss')||';' -- дата начал действия тар. плана
               TXT
          FROM BDR_VOICE_T B,
               TARIFF_PH.D41_TRF_HEADER T,
               ORDER_T O,
               MDV.T03_MMTS_CDR C,
               MDV.T01_CDR_FILES F,
               ACCOUNT_T a
        WHERE  B.REP_PERIOD BETWEEN v_date_from AND v_date_to
           AND C.I_ANS_TIME BETWEEN v_date_from AND v_date_to
           AND B.SAVE_DATE   > v_date_save
           AND B.SAVE_DATE <= v_bdr_save_date
           AND NVL(b.BDR_TYPE_ID,1) = 1
           AND B.TRF_ID      = T.TRF_ID
           AND B.ORDER_ID    = O.ORDER_ID
           AND B.CDR_ID      = C.CDR_ID
           AND C.CDR_FILE_ID = F.CDR_FILE_ID
           AND b.account_id = a.account_id
           AND a.billing_id IN (Pk00_Const.c_BILLING_MMTS, Pk00_Const.c_BILLING_OLD, Pk00_Const.c_BILLING_KTTK) --2003
           ORDER BY C.I_ANS_TIME  */
           -- с 16.09.2015 
            WITH tbl_bdr AS 
                (SELECT /*+ ordered parallel(b 10) parallel(c 10) */
                           C.I_ANS_TIME, -- время вызова по GMT
                           SUBSTR(F.FILE_NAME, 1, 14)||'.DET' file_name, -- имя файла - VARCHAR2(100)
                           C.I_CSN,         -- CSN - INTEGER
                           C.CDR_ID,        -- ID CDR - INTEGER
                           B.ORDER_ID,      -- ID заказа - INTEGER
                           O.ORDER_NO,      -- номер заказа - VARCHAR2(60)
                           B.PREFIX_B,      -- код направления доступа - VARCHAR2(34)
                           B.PRICE,         -- цена - NUMBER
                           B.BILL_MINUTES,  -- кол-во тарифных минут - NUMBER
                           B.AMOUNT,        -- сумма за вызов - NUMBER
                           B.TERM_Z_NAME,  -- имя тарифной зоны - VARCHAR2(200)
                           B.SAVE_DATE,    -- дата начала выгрузки - DATE
                           B.LOCAL_TIME,   -- местное время вызова
                           B.ORDER_BODY_ID, B.TRF_ID, B.TD_ID,
                           B.RATESYSTEM_ID,
                           O.RATEPLAN_ID ORD_RATEPLAN_ID,
                           B.RATEPLAN_ID
                      FROM BDR_VOICE_T B,
                           ORDER_T O,
                           MDV.T03_MMTS_CDR C,
                           MDV.T01_CDR_FILES F,
                           ACCOUNT_T a
                    WHERE  B.REP_PERIOD BETWEEN v_date_from AND v_date_to
                       AND C.I_ANS_TIME BETWEEN v_date_from AND v_date_to
                       AND (B.MODIFY_DATE > v_date_save OR v_date_save IS NULL) -- B.SAVE_DATE до 09.10.2015
                       AND B.MODIFY_DATE <= v_bdr_save_date -- B.SAVE_DATE до 09.10.2015
                       AND NVL(b.BDR_TYPE_ID,1) = 1
                       AND B.ORDER_ID      = O.ORDER_ID
                       AND B.CDR_ID        = C.CDR_ID
                       AND C.CDR_FILE_ID   = F.CDR_FILE_ID
                       AND b.account_id    = a.account_id
                       AND a.billing_id IN (Pk00_Const.c_BILLING_MMTS, -- 2003 
                                            Pk00_Const.c_BILLING_OLD, -- 2002 
                                            Pk00_Const.c_BILLING_KTTK, --2001
                                            2009
                                           )  
                       AND (p_Contract_No IS NULL 
                              OR -- если задан номер договора, то получаем по нему будет поиск
                            EXISTS (SELECT 1
                                      FROM contract_t c,
                                           account_profile_t p
                                     WHERE c.contract_no = p_Contract_No
                                       AND c.contract_id = p.contract_id 
                                       AND p.account_id = b.account_id)                               
                            )
                   )         
            SELECT TO_CHAR(I_ANS_TIME,'yyyy.mm.dd hh24:mi:ss')||';'|| -- время вызова по GMT
                   SUBSTR(FILE_NAME, 1, 14)||'.DET'||';'|| -- имя файла - VARCHAR2(100)
                   I_CSN||';'||         -- CSN - INTEGER
                   CDR_ID||';'||        -- ID CDR - INTEGER
                   ORDER_ID||';'||      -- ID заказа - INTEGER
                   ORDER_NO||';'||      -- номер заказа - VARCHAR2(60)
                   PREFIX_B||';'||      -- код направления доступа - VARCHAR2(34)
                   PRICE||';'||         -- цена - NUMBER
                   BILL_MINUTES||';'||  -- кол-во тарифных минут - NUMBER
                   AMOUNT||';'||        -- сумма за вызов - NUMBER
                   TERM_Z_NAME||';'||   -- имя тарифной зоны - VARCHAR2(200)
                   TRF_NAME||';'||      -- имя тарифного плана
                   '810'||';'||           -- ID валюты - INTEGER
                   TO_CHAR(SAVE_DATE, 'yyyy.mm.dd hh24:mi:ss')||';'|| -- дата начала выгрузки - DATE
                   TO_CHAR(LOCAL_TIME,'yyyy.mm.dd hh24:mi:ss')||';'||   -- местное время вызова
                   TO_CHAR(DATE_FROM,'yyyy.mm.dd hh24:mi:ss')||';' -- дата начал действия тар. плана
                   TXT
              FROM (SELECT b.*, -- клиентские тарифы
                           OB.DATE_FROM,   -- дата начал действия тар. плана
                           r.rateplan_name trf_name -- t.trf_name -- имя тарифного плана
                      FROM tbl_bdr b,
                           ORDER_BODY_T ob,
                           RATEPLAN_T r --TARIFF_PH.D41_TRF_HEADER T
                     WHERE NVL(b.ratesystem_id,0) != 1205
                       AND B.ORDER_BODY_ID = OB.ORDER_BODY_ID
                       AND B.RATEPLAN_ID   = R.RATEPLAN_ID --B.TRF_ID        = T.TRF_ID
                   UNION ALL     
                    SELECT b.*,  -- тарификация по смете
                           t.date_from,   -- дата начал действия тар. плана
                           r.rateplan_name trf_name -- имя тарифного плана
                      FROM tbl_bdr b,
                           RATEPLAN_T r,
                           TTK_SMETA.SM14_TARIFF t
                     WHERE b.ratesystem_id = 1205
                       AND b.ord_rateplan_id = r.rateplan_id
                       AND b.td_id = t.sm14_id(+)  
                  )
            ORDER BY i_ans_time           
    ) LOOP
        UTL_FILE.put_line( v_output, bdr.txt ) ;
        v_count := v_count + 1;
        
        IF MOD(v_Count, 100) = 0 THEN
            DBMS_APPLICATION_INFO.SET_ACTION('Fetch rows: ' || TO_CHAR(v_Count));
        END IF;
                
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    -- ------------------------------------------------------------------ --
    -- фиксируем изменения в системе логирования задач
    UPDATE PK403_LCR_EXPORT_BDR E
       SET E.EXPORT_STATUS = 'OK', E.FILE_SIZE = v_count, E.EXPORT_DATE = SYSDATE
     WHERE E.EXPORT_ID = v_export_id;

    -- ------------------------------------------------------------------ --
    -- проставляем признак окончания выполнения задачи
    v_output := UTL_FILE.fopen( v_dir, v_file_end, 'W' );
    UTL_FILE.put_line( v_output, TO_CHAR(SYSDATE, 'yyyy.mm.dd hh24:mi:ss'));
    UTL_FILE.fclose( v_output ) ;

    Pk01_Syslog.Write_msg('LCR.BDR: '||v_file_name||' - '||v_count||' rows - OK', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    RETURN 1;
    --

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- нет новых данных для выгрузки
        Pk01_Syslog.Write_msg('Stop, no data found', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        RETURN 0;
 --   WHEN OTHERS THEN
 --       Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- Экспорт всех вновь созданных BDR по всем выгрузкам в файлы
PROCEDURE export_all_BDRs
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'export_all_BDRs';
    v_count      INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    LOOP
      EXIT WHEN export_BDR_to_file = 0;
    END LOOP;
    Pk01_Syslog.Write_msg('Exported '||v_count||' BDR - files', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

-- --------------------------------------------------------------------------------- --
-- Контроль за процессом отгрузки по логам задач
-- --------------------------------------------------------------------------------- --
PROCEDURE export_Ctrl(
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'export_Ctrl';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
      SELECT EXPORT_ID, BDR_SAVE_DATE, FILE_NAME, FILE_DATE, EXPORT_STATUS
        FROM PK403_LCR_EXPORT_BDR
       ORDER BY EXPORT_ID DESC
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- --------------------------------------------------------------------------------- --
-- Контроль за процессом отгрузки по системе логирования биллинга
-- --------------------------------------------------------------------------------- --
PROCEDURE export_Ctrl_by_syslog(
               p_recordset OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'export_Ctrl_by_syslog';
    v_retcode    INTEGER;
BEGIN
    -- возвращаем курсор
    OPEN p_recordset FOR
      SELECT L01_ID, MSG_LEVEL, MSG_DATE, MSG_SRC, MESSAGE, OS_USER, PROGRAM
        FROM L01_MESSAGES
       WHERE MSG_SRC LIKE 'PK403_LCR_DATA%'
      ORDER BY L01_ID DESC
    ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- --------------------------------------------------------------------------------- --
-- Выгрузить таблицу счетов в CSV файл
-- --------------------------------------------------------------------------------- --
PROCEDURE export_Bills_to_file( p_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'export_Bills_to_file';
    v_output     UTL_FILE.file_type;
    v_dir        VARCHAR2(100) := 'LCR_DIR';
    v_file_name  VARCHAR2(100) := 'bills.csv';
    v_file_tmp   VARCHAR2(100) := 'bills.tmp';
    v_count      INTEGER := 0;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W' );
    
    FOR bill IN (
      SELECT BILL_NO||';'||BILL_DATE||';'||TOTAL||';'||CURRENCY_ID||';'||
             CONTRACT_NO||';'||ACCOUNT_NO||';'||ERP_CODE||';'||COMPANY_NAME AS TXT
        FROM (
          SELECT ROW_NUMBER() OVER (PARTITION BY BILL_NO ORDER BY CM.COMPANY_NAME) RN,
                 B.BILL_NO, B.BILL_DATE, B.TOTAL, B.CURRENCY_ID, 
                 C.CONTRACT_NO, A.ACCOUNT_NO, CS.ERP_CODE, 
                 CM.COMPANY_NAME 
            FROM BILL_T B, CONTRACT_T C, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CUSTOMER_T CS, COMPANY_T CM 
           WHERE B.REP_PERIOD_ID = p_period_id
             AND BILL_STATUS = 'CLOSED' 
             AND B.CONTRACT_ID = C.CONTRACT_ID
             AND B.ACCOUNT_ID  = A.ACCOUNT_ID
             AND B.PROFILE_ID  = AP.PROFILE_ID
             AND AP.CUSTOMER_ID = CS.CUSTOMER_ID
             AND C.CONTRACT_ID = CM.CONTRACT_ID
             AND CM.DATE_FROM <= B.BILL_DATE
             AND (CM.DATE_TO IS NULL OR B.BILL_DATE <= CM.DATE_TO )
      )
      WHERE RN = 1 
      ORDER BY BILL_NO
    ) LOOP
        UTL_FILE.put_line( v_output, bill.txt ) ;
        v_count := v_count + 1;
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    Pk01_Syslog.Write_msg('LCR.ORDERS: '||v_file_name||' - '||v_count||' rows - OK', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--==========================================

-- --------------------------------------------------------------------------------- --
-- Экспорт в BDR файл
-- --------------------------------------------------------------------------------- --
FUNCTION test_BDR_to_file RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'test_BDR_to_file';
    v_count      INTEGER;
    v_export_id  INTEGER;
    --
    v_date_from     DATE := TO_DATE('24.02.2015','DD.MM.YYYY');
    v_date_to       DATE := TO_DATE('26.02.2015','DD.MM.YYYY')-1/(24*60*60);
    v_date_save_from DATE := TO_DATE('24.02.2015','DD.MM.YYYY');
    v_date_save_to   DATE := TO_DATE('26.02.2015','DD.MM.YYYY')-1/(24*60*60);
    v_bdr_save_date  DATE;
    v_num_rows      INTEGER;
    --
    v_output     UTL_FILE.file_type;
    v_dir        VARCHAR2(100) := 'LCR_DIR';
    v_file_name  VARCHAR2(100 CHAR);
    v_file_tmp   VARCHAR2(100 CHAR) := 'bdr.tmp';
    v_file_end   VARCHAR2(100 CHAR) := 'TheEnd';
    -- флаги файла
    v_fexists      BOOLEAN;
    v_file_length  NUMBER;
    v_block_size   BINARY_INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- получаем информацию об очередной порции данных
  /*  WITH E AS (
        SELECT E.SAVE_DATE,
               MIN(E.REP_PERIOD)  DATE_FROM,
               MAX(E.REP_PERIOD) DATE_TO,
               COUNT(*) NUM_ROWS
          FROM E04_BDR_MMTS_T E
         WHERE E.REP_PERIOD BETWEEN v_date_from AND v_date_to
           AND E.SAVE_DATE > v_date_save
           AND NVL(E.BDR_TYPE,1) = 1
         GROUP BY E.SAVE_DATE
        ORDER BY  E.SAVE_DATE
    )
    SELECT E.SAVE_DATE, E.DATE_FROM, E.DATE_TO, E.NUM_ROWS
      INTO v_save_date, v_date_from, v_date_to, v_num_rows
      FROM E
    WHERE ROWNUM = 1; */

    SELECT MAX(E.SAVE_DATE),
           MIN(E.REP_PERIOD)  DATE_FROM,
           MAX(E.REP_PERIOD) DATE_TO,
           COUNT(*) NUM_ROWS
      INTO v_bdr_save_date, v_date_from, v_date_to, v_num_rows
      FROM BDR_VOICE_T E,
           ACCOUNT_T a
     WHERE E.REP_PERIOD BETWEEN v_date_from AND v_date_to
       AND E.SAVE_DATE BETWEEN v_date_save_from AND v_date_save_to
       AND NVL(E.BDR_TYPE_ID,1) = 1
       AND e.account_id = a.account_id
       AND a.billing_id = 2003;

    --  получаем ID задачи экспорта
    v_export_id := SQ_LCR_EXPORT_ID.NEXTVAL;

    -- формируем имя файла
    v_file_name := 'bdr_'||TO_CHAR(v_bdr_save_date, 'yyyymmddhh24mi')||'.csv';

    -- фиксируем начало операции
    INSERT INTO PK403_LCR_EXPORT_BDR (
        EXPORT_ID, BDR_SAVE_DATE,
        FILE_NAME, FILE_SIZE, FILE_DATE, EXPORT_DATE, EXPORT_STATUS,
        DATE_FROM, DATE_TO, NOTES
    )VALUES(
        v_export_id, v_bdr_save_date, v_file_name, v_num_rows, SYSDATE, SYSDATE, NULL,
        v_date_from, v_date_to, NULL
    );

    COMMIT;

    -- ------------------------------------------------------------------ --
    -- Удаляем флаг окончания предедущей загрузки, если он устанвлен
    -- ------------------------------------------------------------------ --
    UTL_FILE.fgetattr(v_dir, v_file_end, v_fexists, v_file_length, v_block_size);
    IF v_fexists = TRUE THEN
        UTL_FILE.fremove( v_dir, v_file_end );
    END IF;

    -- ------------------------------------------------------------------ --
    -- записываем информацию в Файл
    -- ------------------------------------------------------------------ --

    v_count := 0;
    v_output := UTL_FILE.fopen( v_dir, v_file_tmp, 'W' );
    FOR bdr IN (
        SELECT /*+ ordered */
               TO_CHAR(C.I_ANS_TIME,'yyyy.mm.dd hh24:mi:ss')||';'|| -- время вызова по GMT
               SUBSTR(F.FILE_NAME, 1, 14)||'.DET'||';'|| -- имя файла - VARCHAR2(100)
               C.I_CSN||';'||         -- CSN - INTEGER
               C.CDR_ID||';'||        -- ID CDR - INTEGER
               B.ORDER_ID||';'||      -- ID заказа - INTEGER
               O.ORDER_NO||';'||      -- номер заказа - VARCHAR2(60)
               B.PREFIX_B||';'||      -- код напрвления доступа - VARCHAR2(34)
               B.PRICE||';'||         -- цена - NUMBER
               B.BILL_MINUTES||';'||  -- кол-во тарифных минут - NUMBER
               B.AMOUNT||';'||        -- сумма за вызов - NUMBER
               B.TERM_Z_NAME||';'||   -- имя тарифной зоны - VARCHAR2(200)
               R.RATEPLAN_NAME||';'||      -- имя тарифного плана  TRF_NAME
               '810'||';'||           -- ID валюты - INTEGER
               TO_CHAR(B.SAVE_DATE, 'yyyy.mm.dd hh24:mi:ss')||';'|| -- дата начала выгрузки - DATE
               TO_CHAR(B.LOCAL_TIME,'yyyy.mm.dd hh24:mi:ss')||';'||   -- местное время вызова
               TO_CHAR(O.DATE_FROM,'yyyy.mm.dd hh24:mi:ss')||';' -- дата начал действия тар. плана
               TXT
          FROM BDR_VOICE_T B,
               RATEPLAN_T R, -- TARIFF_PH.D41_TRF_HEADER T,
               ORDER_T O,
               MDV.T03_MMTS_CDR C,
               MDV.T01_CDR_FILES F,
               ACCOUNT_T a
        WHERE  B.REP_PERIOD BETWEEN v_date_from AND v_date_to
           AND C.I_ANS_TIME BETWEEN v_date_from AND v_date_to
--           AND B.SAVE_DATE   BETWEEN v_date_save_from AND v_date_save_to
           AND NVL(b.BDR_TYPE_ID,1) = 1
           AND B.RATEPLAN_ID = R.RATEPLAN_ID --B.TRF_ID      = T.TRF_ID
           AND B.ORDER_ID    = O.ORDER_ID
           AND B.CDR_ID      = C.CDR_ID
           AND C.CDR_FILE_ID = F.CDR_FILE_ID
           AND b.account_id = a.account_id
           AND a.billing_id IN (Pk00_Const.c_BILLING_MMTS, 
                                Pk00_Const.c_BILLING_OLD, 
                                Pk00_Const.c_BILLING_KTTK,
                                2009
                                ) --2003
           AND 
               a.account_no IN 
               ('SA137658')
           ORDER BY C.I_ANS_TIME
    ) LOOP
        UTL_FILE.put_line( v_output, bdr.txt ) ;
        v_count := v_count + 1;
    END LOOP;
    UTL_FILE.fclose( v_output ) ;

    UTL_FILE.frename(src_location => v_dir ,src_filename => v_file_tmp ,
                     dest_location => v_dir ,dest_filename => v_file_name ,overwrite => TRUE);

    -- ------------------------------------------------------------------ --
    -- фиксируем изменения в системе логирования задач
    UPDATE PK403_LCR_EXPORT_BDR E
       SET E.EXPORT_STATUS = 'OK', E.FILE_SIZE = v_count, E.EXPORT_DATE = SYSDATE
     WHERE E.EXPORT_ID = v_export_id;

    -- ------------------------------------------------------------------ --
    -- проставляем признак окончания выполнения задачи
    v_output := UTL_FILE.fopen( v_dir, v_file_end, 'W' );
    UTL_FILE.put_line( v_output, TO_CHAR(SYSDATE, 'yyyy.mm.dd hh24:mi:ss'));
    UTL_FILE.fclose( v_output ) ;

    Pk01_Syslog.Write_msg('LCR.BDR: '||v_file_name||' - '||v_count||' rows - OK', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    COMMIT;

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    RETURN 1;
    --

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- нет новых данных для выгрузки
        Pk01_Syslog.Write_msg('Stop, no data found', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
        RETURN 0;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('Stop.ERROR', c_PkgName||'.'||v_prcName );
END;

--=========================================




END ;
/
