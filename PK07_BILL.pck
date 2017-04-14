CREATE OR REPLACE PACKAGE PK07_BILL
IS
    --
    -- Пакет для работы с объектом "СЧЕТ", таблицы:
    -- bill_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK07_BILL';
    -- ==============================================================================
    type t_refc is ref cursor;
   
    -- проверка периода счета
    PROCEDURE Check_bill_period(
                 p_bill_period_id IN INTEGER,
                 p_account_id     IN INTEGER
              );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- Получение периода и уточнение даты счета для ручного счета:
    --   - положительное - ID периода счета 
    --   - при ошибке выставляет исключение
    FUNCTION Get_manual_bill_period (
                   p_bill_date   IN OUT DATE   -- Дата счета (биллингового периода)
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- создание нового разового счета, возвращает:
    --   - положительное - ID текущего счета, 
    --   - при ошибке выставляет исключение
    FUNCTION Open_manual_bill (
                   p_account_id    IN INTEGER,   -- ID лицевого счета
                   p_rep_period_id IN INTEGER,   -- ID расчетного периода YYYYMM
                   p_bill_no       IN VARCHAR2,  -- Номер счета
                   p_currency_id   IN INTEGER,   -- ID валюты счета
                   p_bill_date     IN DATE,      -- Дата счета (биллингового периода)
                   p_notes         IN VARCHAR2   -- примечания к счету
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- УДАЛИТЬ!!! после изменения в АРМ
    -- создание авансового счета, возвращает:
    --   - положительное - ID текущего счета, 
    --   - при ошибке выставляет исключение
    FUNCTION Open_prepaid_bill (
                   p_account_id    IN INTEGER,   -- ID лицевого счета
                   p_rep_period_id IN INTEGER,   -- ID расчетного периода YYYYMM
                   p_bill_no       IN VARCHAR2,  -- Номер счета
                   p_currency_id   IN INTEGER,   -- ID валюты счета
                   p_bill_date     IN DATE,      -- Дата счета (биллингового периода)
                   p_notes         IN VARCHAR2   -- примечания к счету
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
    -- создание авансового счета, возвращает:
    --   - положительное - ID текущего счета, 
    --   - при ошибке выставляет исключение
    FUNCTION Open_prepaid_bill (
                   p_account_id    IN INTEGER,   -- ID лицевого счета
                   p_bill_no       IN VARCHAR2,  -- Номер счета
                   p_currency_id   IN INTEGER,   -- ID валюты счета
                   p_bill_date     IN DATE,      -- Дата счета (биллингового периода)
                   p_notes         IN VARCHAR2   -- примечания к счету
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Перенос авансового счета в указанный период
    -- Возвращает 
    --  bill_id - перенесенного счета
    -- -1 - ошибка, счет принадлежит закрытому периоду
    -- -2 - ошибка, на счет есть ссылка, например Дебет-нота (BILL_T.NEXT_BILL_ID is not null)
    -- если p_force = 1, то перенос из закрытого периода разрешен
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- 
    FUNCTION Move_prepaid_bill (
                  p_bill_id      IN INTEGER,
                  p_period_id_to IN INTEGER,
                  p_bill_date_to IN DATE
              ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- создание нового периодического счета, возвращает:
    --   - положительное - ID текущего счета, 
    --   - при ошибке выставляет исключение
    FUNCTION Open_recuring_bill (
                p_account_id    IN INTEGER,   -- ID лицевого счета
                p_rep_period_id IN INTEGER,   -- ID расчетного периода YYYYMM
                p_bill_no       IN VARCHAR2,  -- Номер счета
                p_currency_id   IN INTEGER,   -- ID валюты счета
                p_bill_date     IN DATE       -- Дата счета (биллингового периода)
          ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- создание следующего по порядку периодического счета, возвращает:
    --   - положительное - ID текущего счета, 
    --   - при ошибке выставляет исключение
    FUNCTION Next_recuring_bill (
                   p_account_id    IN INTEGER,   -- ID лицевого счета
                   p_rep_period_id IN INTEGER    -- ID расчетного периода YYYYMM
               ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- счет создается в случае если по какой-то причине счет за 
    -- прошедший период не был выставлен, а клиент не хочет 
    -- чтобы потерянные начисления вошли в отдельный счет,
    -- а не попали отдельными позициями в текущий счет
    -- возвращает:
    --   - положительное - ID счета, 
    --   - при ошибке выставляет исключение
    FUNCTION Open_rec_bill_for_old_period (
                   p_account_id    IN INTEGER,   -- ID лицевого счета
                   p_rep_period_id IN INTEGER,   -- ID расчетного периода YYYYMM
                   p_bill_no       IN VARCHAR2,  -- Номер счета
                   p_currency_id   IN INTEGER,   -- ID валюты счета
                   p_bill_date     IN DATE,       -- Дата счета (биллингового периода)
                   p_notes         IN VARCHAR2    DEFAULT NULL
                ) RETURN INTEGER;

    --================================================================================
    -- Редактирование шапки счета
    PROCEDURE Bill_info_edit(
             p_bill_id           IN INTEGER,   -- ID позиции счета
             p_rep_period_id     IN INTEGER,    -- ID периода счета
             p_bill_No           IN VARCHAR2,
             p_currency_id       IN INTEGER,
             p_bill_date         IN DATE,
             p_rep_period_id_new IN INTEGER,
             p_bill_type         IN VARCHAR2 DEFAULT NULL,
             p_notes             IN VARCHAR2 DEFAULT NULL
    );

    -- ===============================================================================
    -- Удаление счета
    -- Возвращает:
    --  0 - счет удален
    -- -1 - ошибка, счет принадлежит закрытому периоду
    -- -2 - ошибка, на счет есть ссылка, например Дебет-нота (BILL_T.NEXT_BILL_ID is not null)
    -- если задан параметр p_force, то можно удалить счет из закрытого периода
    -- =============================================================================== 
    FUNCTION Delete_bill (
                  p_bill_id      IN INTEGER,
                  p_period_id    IN INTEGER,
                  p_force        IN INTEGER DEFAULT 0
              ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- установить статус счета
    -- при ошибке выставляем исключение 
    PROCEDURE Set_status (
                   p_bill_id       IN INTEGER,   -- ID счета 
                   p_rep_period_id IN INTEGER,   -- ID расчетного периода YYYYMM
                   p_bill_status   IN VARCHAR2   -- статус счета
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- получить статус счета, возвращает
    -- - статус счета
    -- - при ошибке выставляем исключение 
    FUNCTION Get_status (
                   p_bill_id       IN INTEGER,
                   p_rep_period_id IN INTEGER    -- ID расчетного периода YYYYMM
               ) RETURN VARCHAR2;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- закрыть счет 
    -- при ошибке выставляем исключение
    PROCEDURE Close_bill( p_bill_id IN INTEGER, p_rep_period_id IN INTEGER );


    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Проверка правильности установки префикса региона
    -- возвращает корректный номер счета
    FUNCTION Check_region_prefix ( 
                 p_bill_id   IN INTEGER,
                 p_period_id IN INTEGER 
             ) RETURN VARCHAR2;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Получить следующий номер периодического счета в BRM
    -- номера счетов формируются по единому правилу: YYMM(№ Л/С)[A-Z]
    FUNCTION Next_rec_bill_no(
                   p_account_id     IN INTEGER,
                   p_bill_period_id IN INTEGER
               ) RETURN VARCHAR2;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Получить следующий номер периодического счета в BRM
    -- номера счетов формируются по единому правилу: YYMM(№ Л/С)[A-Z]
    FUNCTION Next_bill_no(
                   p_account_id     IN INTEGER,
                   p_bill_period_id IN INTEGER
               ) RETURN VARCHAR2;
    
    /*       
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- УСТАРЕВШАЯ ВЕРСИЯ ДО 31.12.2014
    -- Получить следующий номер периодического счета,
    -- следует учитывать, что в биллинге "Микротест" и "старом биллинге" 
    -- номера счетов формируются по разным правилам:
    -- "Микротест" - CONTRACT_NO_XXXX, где XXXX - порядковый номер счета
    --               следует учитывать, что на одном договоре, может быть
    --               несколько лицевых счетов
    -- "старом биллинге" - YYMM(№ Л/С)[A-Z]
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    FUNCTION Next_bill_no(
                   p_account_id     IN INTEGER,
                   p_contract_id    IN INTEGER,
                   p_bill_period_id IN INTEGER  
               ) RETURN VARCHAR2;
    */
    
    -- служебная процедура восстановления данных в таблице CONTRACT_BILL_SQ_T
    PROCEDURE Fill_contract_bill_sq_t;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- создание описателя счетов для нового Л/С
    --   - при ошибке выставляет исключение
    PROCEDURE New_billinfo (
                   p_account_id       IN INTEGER,   -- ID лицевого счета
                   p_currency_id      IN INTEGER,   -- ID валюты счета
                   p_delivery_id      IN INTEGER,   -- ID способа доставки счета
                   p_days_for_payment IN INTEGER DEFAULT 30   -- кол-во дней на оплату счета
               );

    /*
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- сформировать счет: получить сумму начислений и корректировок счета по позициям
    -- и проставить признак, что счет сформирован , 
    -- т.е. начисления на его позиции не возможны, 
    -- принимаются только оплаты
    -- возвращает:
    -- - возвращаем сумму начислений и корректировок по счету (то что должно быть оплачено)
    -- - при ошибке выставляет исключение
    FUNCTION Generate_bill (
                   p_bill_id       IN INTEGER,   -- ID позиции счета
                   p_rep_period_id IN INTEGER    -- ID периода счета
               ) RETURN NUMBER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассформировать счет (только для статуса READY): 
    -- обнулить сумму начислений и корректировок счета по позициям
    -- удалить строки счета фактуры, если были сформированы
    -- и вернуть признак OPEN,
    -- т.е. сделать возможными начисления на его позиции 
    -- возвращает:
    --   - положительное - ID счета, 
    --   - при ошибке выставляет исключение
    FUNCTION Rollback_bill (
                   p_bill_id       IN INTEGER,   -- ID позиции счета
                   p_rep_period_id IN INTEGER    -- ID периода счета
               ) RETURN NUMBER;

    --
    -- Откат счета. Технологическая функция. Нужно пото убить
    --
    FUNCTION Rollback_bill_force (
                   p_bill_id       IN INTEGER,   -- ID позиции счета
                   p_rep_period_id IN INTEGER    -- ID периода счета
               ) RETURN NUMBER;
    */
    -- ===============================================================================
    -- Перенос счета в другой период строго по команде ДРУ, 
    -- т.к. исходный счет не должен был попасть в отчетность
    -- Возвращает 
    --  bill_id - перенесенного счета
    -- -1 - ошибка, счет принадлежит закрытому периоду
    -- -2 - ошибка, на счет есть ссылка, например Дебет-нота (BILL_T.NEXT_BILL_ID is not null)
    -- если p_force = 1, то перенос из закрытого периода разрешен
    -- =============================================================================== 
    FUNCTION Move_bill (
                  p_bill_id      IN INTEGER,
                  p_period_id    IN INTEGER,
                  p_period_id_to IN INTEGER,
                  p_bill_date_to IN DATE,
                  p_force        IN INTEGER DEFAULT 1
              ) RETURN INTEGER;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Рассчитать задолженность по счету
    -- возвращает:
    -- - возвращаем сумму задолженности по счету
    -- - при ошибке выставляет исключение
    FUNCTION Calculate_due(
                   p_bill_id       IN INTEGER,   -- ID позиции счета
                   p_rep_period_id IN INTEGER    -- ID периода счета
               ) RETURN NUMBER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- найти все позиции указанного счета
    --   - положительное - кол-во выбранных записей
    --   - при ошибке выставляет исключение
    FUNCTION Items_list( 
                   p_recordset OUT t_refc, 
                   p_bill_id       IN INTEGER,   -- ID позиции счета
                   p_rep_period_id IN INTEGER    -- ID периода счета
               ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Удалить все позиции указанного счета (скорее всего только сразу после ошибочного создания)
    --   - положительное - кол-во удаленных записей
    --   - при ошибке выставляет исключение
    FUNCTION Delete_items (
                   p_bill_id       IN INTEGER,   -- ID позиции счета
                   p_rep_period_id IN INTEGER    -- ID периода счета
               ) RETURN INTEGER;
  
    -- =============================================================== --
    -- Служебные функции
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Прочитать данные из профиля л/с
    --
    PROCEDURE Read_account_profile (
                   p_account_id    IN INTEGER,   -- ID лицевого счета
                   p_bill_date     IN DATE,      -- дата счета
                   p_profile_id    OUT INTEGER,  -- ID профиля л/с
                   p_contract_id   OUT INTEGER,  -- ID договора
                   p_contractor_id OUT INTEGER,  -- ID продавца
                   p_bank_id       OUT INTEGER,  -- ID банка продавца
                   p_vat           OUT INTEGER   -- ставка НДС
               );

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Проверка на наличие периодических счетов на лицевом счете в заданном периоде
FUNCTION Check_BillRec_AtPeriod_Has ( 
      p_account_id   IN INTEGER,
      p_period_id    IN INTEGER 
) RETURN INTEGER;

---------------------------------------------------------------
-- получить примечание к счету
---------------------------------------------------------------
FUNCTION GET_BILL_NOTES(p_bill_id IN NUMBER) RETURN VARCHAR2;


END PK07_BILL;
/
CREATE OR REPLACE PACKAGE BODY PK07_BILL
IS

-- проверка периода счета
PROCEDURE Check_bill_period(
             p_bill_period_id IN INTEGER,
             p_account_id     IN INTEGER
          )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Check_bill_period';
    v_position   PERIOD_T.POSITION%TYPE;
    v_billing_id INTEGER;
BEGIN
    SELECT P.POSITION 
      INTO v_position
      FROM PERIOD_T P
     WHERE P.PERIOD_ID = p_bill_period_id;
    IF v_position NOT IN ('BILL','OPEN','NEXT') THEN
       -- период закрыт, создание счетов запрещено, кроме тестового биллинга
       SELECT A.BILLING_ID
         INTO v_billing_id
         FROM ACCOUNT_T A
        WHERE A.ACCOUNT_ID = p_account_id;
       IF v_billing_id NOT IN ( 2008, 2009 ) THEN
          Pk01_Syslog.Raise_user_exception('period_id='||p_bill_period_id||' is closed' , c_PkgName||'.'||v_prcName);
       END IF;
    END IF;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- период из далекого будущего, еще не описан в PERIOD_T
        Pk01_Syslog.Raise_user_exception('period_id='||p_bill_period_id||' not found in period_t' , c_PkgName||'.'||v_prcName);
      
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Прочитать данные из профиля л/с
--
PROCEDURE Read_account_profile (
               p_account_id    IN INTEGER,   -- ID лицевого счета
               p_bill_date     IN DATE,      -- дата счета
               p_profile_id    OUT INTEGER,  -- ID профиля л/с
               p_contract_id   OUT INTEGER,  -- ID договора
               p_contractor_id OUT INTEGER,  -- ID продавца
               p_bank_id       OUT INTEGER,  -- ID банка продавца
               p_vat           OUT INTEGER   -- ставка НДС
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Read_account_profile';
BEGIN
  SELECT 
         PROFILE_ID, CONTRACT_ID, 
         CONTRACTOR_ID, CONTRACTOR_BANK_ID, VAT
      INTO p_profile_id, p_contract_id,
           p_contractor_id, p_bank_id, p_vat FROM (
    SELECT AP.PROFILE_ID, AP.CONTRACT_ID,
           AP.CONTRACTOR_ID, AP.CONTRACTOR_BANK_ID, AP.VAT,
           ROW_NUMBER() OVER(PARTITION BY ACCOUNT_ID ORDER BY DECODE(ACTUAL,'Y',0,1)) rn      
      FROM ACCOUNT_PROFILE_T AP
     WHERE AP.ACCOUNT_ID = p_account_id
       AND AP.DATE_FROM <= p_bill_date
       AND (AP.DATE_TO IS NULL OR p_bill_date <= AP.DATE_TO 
             OR AP.ACTUAL = 'Y' -- добавлено по согласованию с Макеевым С.В. 13.05.2016
           )
    )
  WHERE rn = 1; -- для страховки
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
-- Получение периода и уточнение даты счета для ручного счета:
--   - положительное - ID периода счета 
--   - при ошибке выставляет исключение
FUNCTION Get_manual_bill_period (
               p_bill_date   IN OUT DATE   -- Дата счета (биллингового периода)
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Get_manual_bill_period';
    v_period_id     INTEGER;
    v_min_period_id INTEGER;
    v_min_date_from DATE;
BEGIN
    --
    BEGIN
      -- ищем в доступных для создания счета периодах
      SELECT PERIOD_ID 
        INTO v_period_id 
        FROM PERIOD_T
       WHERE p_bill_date BETWEEN PERIOD_FROM AND PERIOD_TO
         AND CLOSE_REP_PERIOD IS NULL
      ORDER BY PERIOD_ID DESC;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        -- ищем минимальный открытый для записи период
        SELECT MIN(PERIOD_ID), MIN(PERIOD_FROM) 
          INTO v_min_period_id, v_min_date_from
          FROM PERIOD_T
         WHERE CLOSE_REP_PERIOD IS NULL;
        IF p_bill_date < v_min_date_from THEN -- закрытый период
           p_bill_date := v_min_date_from;
           v_period_id := v_min_period_id;
        ELSE -- будущий период
           v_period_id := Pk04_Period.Period_id(p_period => p_bill_date);
        END IF;
      END;
    END;
    RETURN v_period_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
-- создание нового разового счета, возвращает:
--   - положительное - ID текущего счета, 
--   - при ошибке выставляет исключение
FUNCTION Open_manual_bill (
               p_account_id    IN INTEGER,   -- ID лицевого счета
               p_rep_period_id IN INTEGER,   -- ID расчетного периода YYYYMM
               p_bill_no       IN VARCHAR2,  -- Номер счета
               p_currency_id   IN INTEGER,   -- ID валюты счета
               p_bill_date     IN DATE,      -- Дата счета (биллингового периода)
               p_notes         IN VARCHAR2   -- примечания к счету
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Open_manual_bill';
    v_bill_id       INTEGER; 
    v_profile_id    INTEGER;
    v_contract_id   INTEGER;
    v_contractor_id INTEGER;
    v_bank_id       INTEGER;
    
    v_vat         NUMBER;
BEGIN
    -- проверяем период счета 
    Check_bill_period(p_rep_period_id, p_account_id);

    -- Формируем ID объекта (POID) для указанного биллингового периода 
    v_bill_id := Pk02_POID.Next_bill_id;
    
    -- получаем id договора и ставку НДС
    Read_account_profile (
               p_account_id    => p_account_id,
               p_bill_date     => p_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );
    
    -- Cоздаем разовый счет для последующего заполнения
    INSERT INTO BILL_T (
        CONTRACT_ID,     -- ID договора
        ACCOUNT_ID,      -- ID лицевого счета
        BILL_ID,         -- ID лицевого счета
        REP_PERIOD_ID,   -- ID расчетного периода
        BILL_TYPE,       -- Тип счета
        BILL_NO,         -- Номер счета
        CURRENCY_ID,     -- ID валюты счета
        BILL_DATE,       -- Дата счета (биллингового периода)
        BILL_STATUS,     -- Состояние счета - ОТКРЫТ
        VAT,             -- ставка НДС 
        PROFILE_ID,      -- ID профиля л/с
        CONTRACTOR_ID,   -- ID продавца
        CONTRACTOR_BANK_ID, -- ID банка продавца
        NOTES
    )VALUES(
        v_contract_id,
        p_account_id,
        v_bill_id,
        p_rep_period_id,
        PK00_CONST.c_BILL_TYPE_ONT,
        p_bill_no,
        p_currency_id,
        p_bill_date,
        PK00_CONST.c_BILL_STATE_OPEN,
        v_vat,
        v_profile_id,
        v_contractor_id,
        v_bank_id,
        p_notes
    );  
    
    INSERT INTO BILL_HISTORY_T(BILL_ID, REP_PERIOD_ID, ACTION) VALUES(v_bill_id, p_rep_period_id, Pk00_Const.c_BILL_HISTORY_CREATE);
    
    RETURN v_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
-- создание авансового счета, возвращает:
--   - положительное - ID текущего счета, 
--   - при ошибке выставляет исключение
FUNCTION Open_prepaid_bill (
               p_account_id    IN INTEGER,   -- ID лицевого счета
               p_rep_period_id IN INTEGER,   -- ID расчетного периода YYYYMM
               p_bill_no       IN VARCHAR2,  -- Номер счета
               p_currency_id   IN INTEGER,   -- ID валюты счета
               p_bill_date     IN DATE,      -- Дата счета (биллингового периода)
               p_notes         IN VARCHAR2   -- примечания к счету
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Open_prepaid_bill';
    v_bill_id       INTEGER; 
    v_profile_id    INTEGER;
    v_contract_id   INTEGER;
    v_contractor_id INTEGER;
    v_bank_id       INTEGER;
    v_vat           NUMBER;
BEGIN
    -- проверяем период счета 
    Check_bill_period(p_rep_period_id, p_account_id);  

    -- Формируем ID объекта (POID) для указанного биллингового периода 
    v_bill_id := Pk02_POID.Next_bill_id;
    
    -- получаем id договора и ставку НДС
    Read_account_profile (
               p_account_id    => p_account_id,
               p_bill_date     => p_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );
    
    -- Cоздаем разовый счет для последующего заполнения
    INSERT INTO BILL_T (
        CONTRACT_ID,     -- ID договора
        ACCOUNT_ID,      -- ID лицевого счета
        BILL_ID,         -- ID лицевого счета
        REP_PERIOD_ID,   -- ID расчетного периода
        BILL_TYPE,       -- Тип счета
        BILL_NO,         -- Номер счета
        CURRENCY_ID,     -- ID валюты счета
        BILL_DATE,       -- Дата счета (биллингового периода)
        BILL_STATUS,     -- Состояние счета - ОТКРЫТ
        VAT,             -- ставка НДС 
        PROFILE_ID,      -- ID профиля л/с
        CONTRACTOR_ID,   -- ID продавца
        CONTRACTOR_BANK_ID, -- ID банка продавца
        NOTES
    )VALUES(
        v_contract_id,
        p_account_id,
        v_bill_id,
        p_rep_period_id,
        PK00_CONST.c_BILL_TYPE_PRE,
        p_bill_no,
        p_currency_id,
        p_bill_date,
        PK00_CONST.c_BILL_STATE_OPEN,
        v_vat,
        v_profile_id,
        v_contractor_id,
        v_bank_id,
        p_notes
    );  
    
    INSERT INTO BILL_HISTORY_T(BILL_ID, REP_PERIOD_ID, ACTION) VALUES(v_bill_id, p_rep_period_id, Pk00_Const.c_BILL_HISTORY_CREATE);
    
    RETURN v_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --    
-- создание авансового счета, возвращает:
--   - положительное - ID текущего счета, 
--   - при ошибке выставляет исключение
FUNCTION Open_prepaid_bill (
               p_account_id    IN INTEGER,   -- ID лицевого счета
               p_bill_no       IN VARCHAR2,  -- Номер счета
               p_currency_id   IN INTEGER,   -- ID валюты счета
               p_bill_date     IN DATE,      -- Дата счета (биллингового периода)
               p_notes         IN VARCHAR2   -- примечания к счету
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Open_prepaid_bill';
    v_bill_id       INTEGER; 
    v_profile_id    INTEGER;
    v_contract_id   INTEGER;
    v_contractor_id INTEGER;
    v_bank_id       INTEGER;
    v_vat           NUMBER;
    v_period_id     INTEGER := 205001; 
BEGIN

    -- Формируем ID объекта (POID) для указанного биллингового периода 
    v_bill_id := Pk02_POID.Next_bill_id;
    
    -- получаем id договора и ставку НДС
    Read_account_profile (
               p_account_id    => p_account_id,
               p_bill_date     => p_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );
    
    -- Cоздаем разовый счет для последующего заполнения
    INSERT INTO BILL_T (
        CONTRACT_ID,     -- ID договора
        ACCOUNT_ID,      -- ID лицевого счета
        BILL_ID,         -- ID лицевого счета
        REP_PERIOD_ID,   -- ID расчетного периода
        BILL_TYPE,       -- Тип счета
        BILL_NO,         -- Номер счета
        CURRENCY_ID,     -- ID валюты счета
        BILL_DATE,       -- Дата счета (биллингового периода)
        BILL_STATUS,     -- Состояние счета - ОТКРЫТ
        VAT,             -- ставка НДС 
        PROFILE_ID,      -- ID профиля л/с
        CONTRACTOR_ID,   -- ID продавца
        CONTRACTOR_BANK_ID, -- ID банка продавца
        NOTES
    )VALUES(
        v_contract_id,
        p_account_id,
        v_bill_id,
        v_period_id,
        PK00_CONST.c_BILL_TYPE_PRE,
        p_bill_no,
        p_currency_id,
        p_bill_date,
        PK00_CONST.c_BILL_STATE_OPEN,
        v_vat,
        v_profile_id,
        v_contractor_id,
        v_bank_id,
        p_notes
    );  
    
    INSERT INTO BILL_HISTORY_T(BILL_ID, REP_PERIOD_ID, ACTION) VALUES(v_bill_id, v_period_id, Pk00_Const.c_BILL_HISTORY_CREATE);
    
    RETURN v_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ===============================================================================
-- Перенос авансового счета в указанный период
-- Возвращает 
--  bill_id - перенесенного счета
-- -1 - ошибка, счет принадлежит закрытому периоду
-- -2 - ошибка, на счет есть ссылка, например Дебет-нота (BILL_T.NEXT_BILL_ID is not null)
-- если p_force = 1, то перенос из закрытого периода разрешен
-- =============================================================================== 
FUNCTION Move_prepaid_bill (
              p_bill_id      IN INTEGER,
              p_period_id_to IN INTEGER,
              p_bill_date_to IN DATE
          ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Move_prepaid_bill';
    v_bill_id     INTEGER;
    v_period_id   INTEGER := 205001;
BEGIN
    -- переносим счет в указанный период
    v_bill_id := Move_bill (
              p_bill_id,
              v_period_id,
              p_period_id_to,
              p_bill_date_to
          );
    -- меняем тип счета на одноразовый
    UPDATE BILL_T B
       SET B.BILL_TYPE = Pk00_Const.c_BILL_TYPE_ONT
     WHERE B.REP_PERIOD_ID = p_period_id_to
       AND B.BILL_ID = v_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- создание нового периодического счета, возвращает:
--   - положительное - ID текущего счета, 
--   - при ошибке выставляет исключение
FUNCTION Open_recuring_bill (
               p_account_id    IN INTEGER,   -- ID лицевого счета
               p_rep_period_id IN INTEGER,   -- ID расчетного периода YYYYMM
               p_bill_no       IN VARCHAR2,  -- Номер счета
               p_currency_id   IN INTEGER,   -- ID валюты счета
               p_bill_date     IN DATE       -- Дата счета (биллингового периода)
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Open_recuring_bill';
    v_bill_id       INTEGER;
    v_profile_id    INTEGER;
    v_contract_id   INTEGER;
    v_contractor_id INTEGER;
    v_bank_id       INTEGER;   
    v_vat           NUMBER;
BEGIN
    -- проверяем период счета 
    Check_bill_period(p_rep_period_id, p_account_id);  

    -- Формируем ID объекта (POID) для указанного биллингового периода 
    v_bill_id := Pk02_POID.Next_bill_id;
    
    -- получаем id договора и ставку НДС
    Read_account_profile (
               p_account_id    => p_account_id,
               p_bill_date     => p_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );
    
    -- Cоздаем периодический счет для последующего заполнения
    INSERT INTO BILL_T (
        CONTRACT_ID,     -- ID договора
        ACCOUNT_ID,      -- ID лицевого счета
        BILL_ID,         -- ID лицевого счета
        REP_PERIOD_ID,   -- ID расчетного периода YYYYMM
        BILL_TYPE,       -- Тип счета
        BILL_NO,         -- Номер счета
        CURRENCY_ID,     -- ID валюты счета
        BILL_DATE,       -- Дата счета (биллингового периода)
        BILL_STATUS,     -- состояние счета
        PROFILE_ID,      -- ID профиля л/с
        CONTRACTOR_ID,    -- ID продавца
        CONTRACTOR_BANK_ID, -- ID банка продавца
        VAT              -- ставка НДС
    )VALUES(
        v_contract_id,
        p_account_id,
        v_bill_id,
        p_rep_period_id,
        PK00_CONST.c_BILL_TYPE_REC,
        p_bill_no,
        p_currency_id,
        p_bill_date,
        PK00_CONST.c_BILL_STATE_OPEN,
        v_profile_id,
        v_contractor_id,
        v_bank_id,
        v_vat
    );  
    
    INSERT INTO BILL_HISTORY_T(BILL_ID, REP_PERIOD_ID, ACTION) VALUES(v_bill_id, p_rep_period_id, Pk00_Const.c_BILL_HISTORY_CREATE);
    
    RETURN v_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- создание следующего по порядку периодического счета

-- Получить следующий номер периодического счета,
-- следует учитывать, что в биллинге "Микротест" и "старом биллинге" 
-- номера счетов формируются по разным правилам:
-- "Микротест" - CONTRACT_NO_XXXX, где XXXX - порядковый номер счета
--               следует учитывать, что на одном договоре, может быть
--               несколько лицевых счетов
-- "старом биллинге" - YYMM(№ Л/С)[A-Z]
-- Возвращает:
--   - положительное - ID текущего счета, 
--   - при ошибке выставляет исключение

FUNCTION Next_recuring_bill (
               p_account_id    IN INTEGER,   -- ID лицевого счета
               p_rep_period_id IN INTEGER    -- ID расчетного периода YYYYMM
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Next_recuring_bill';
    v_bill_id       INTEGER;
    v_bill_date     DATE;
    v_currency_id   INTEGER;
    v_bill_no       BILL_T.BILL_NO%TYPE := NULL;
    v_profile_id    INTEGER;
    v_contract_id   INTEGER;
    v_contractor_id INTEGER;
    v_bank_id       INTEGER;
    v_vat           NUMBER;
BEGIN
    -- проверяем период счета 
    Check_bill_period(p_rep_period_id, p_account_id);  

    -- определяем дату счета
    v_bill_date := PK04_PERIOD.Period_to(p_rep_period_id);

    -- получаем id договора и ставку НДС
    Read_account_profile (
               p_account_id    => p_account_id,
               p_bill_date     => v_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );

    -- вычисляем номер очередного периодического счета
    v_bill_no := Next_rec_bill_no( p_account_id, p_rep_period_id);

    -- получаем валюту счета
    SELECT A.CURRENCY_ID 
      INTO v_currency_id 
      FROM ACCOUNT_T A
     WHERE A.ACCOUNT_ID = p_account_id;
    
    -- Формируем ID объекта (POID) для указанного биллингового периода 
    v_bill_id := Pk02_POID.Next_bill_id;
    
    -- Cоздаем периодический счет для последующего заполнения
    INSERT INTO BILL_T (
        CONTRACT_ID,     -- ID договора
        ACCOUNT_ID,      -- ID лицевого счета
        BILL_ID,         -- ID лицевого счета
        REP_PERIOD_ID,   -- ID расчетного периода YYYYMM
        BILL_TYPE,       -- Тип счета
        BILL_NO,         -- Номер счета
        CURRENCY_ID,     -- ID валюты счета
        BILL_DATE,       -- Дата счета (биллингового периода)
        BILL_STATUS,     -- состояние счета
        PROFILE_ID,      -- ID профиля л/с
        CONTRACTOR_ID,   -- ID продавца
        CONTRACTOR_BANK_ID, -- ID банка продавца
        VAT              -- ставка НДС
    )VALUES(
        v_contract_id,
        p_account_id,
        v_bill_id,
        p_rep_period_id,
        PK00_CONST.c_BILL_TYPE_REC,
        v_bill_no,
        v_currency_id,
        v_bill_date,
        PK00_CONST.c_BILL_STATE_OPEN,
        v_profile_id,
        v_contractor_id,
        v_bank_id,
        v_vat
    );  

    MERGE INTO BILLINFO_T BI
    USING (
        SELECT v_bill_id BILL_ID, p_rep_period_id REP_PERIOD_ID,
               p_account_id ACCOUNT_ID, 
               1 PERIOD_LENGTH, v_currency_id CURRENCY_ID, 
               30 DAYS_FOR_PAYMENT, 7701 INVOICE_RULE_ID
          FROM DUAL
    ) D
    ON (
       BI.ACCOUNT_ID = D.ACCOUNT_ID
    )
    WHEN MATCHED THEN UPDATE SET PERIOD_LENGTH    = D.PERIOD_LENGTH, 
                                 DAYS_FOR_PAYMENT = D.DAYS_FOR_PAYMENT
    WHEN NOT MATCHED THEN INSERT (
         ACCOUNT_ID, PERIOD_LENGTH, DAYS_FOR_PAYMENT
     ) VALUES (
         D.ACCOUNT_ID, D.PERIOD_LENGTH, D.DAYS_FOR_PAYMENT
    ); 

    /*
    -- изменяем описатель счета (возможно необходимость в этом отпадет)
    UPDATE BILLINFO_T BI
       SET BI.LAST_PERIOD_ID = p_rep_period_id,
           BI.LAST_BILL_ID = v_bill_id
     WHERE BI.ACCOUNT_ID = p_account_id;
     */

    INSERT INTO BILL_HISTORY_T(BILL_ID, REP_PERIOD_ID, ACTION) VALUES(v_bill_id, p_rep_period_id, Pk00_Const.c_BILL_HISTORY_CREATE);

    -- возвращаем номер счета
    RETURN v_bill_id;
    --     
EXCEPTION
    WHEN dup_val_on_index THEN
        Pk01_Syslog.Write_error(p_Msg => 'ERROR. Bill already exists. Account_Id: ' || TO_CHAR(p_Account_Id) || 
                                         ', Period_Id: ' || TO_CHAR(p_Rep_Period_Id), 
                                p_Src => 'pk07_bill.Next_Recuring_Bill'
                               ); 
        RAISE;
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR. Account_Id: ' || TO_CHAR(p_Account_Id) || ', new bill_no: ' || v_bill_no || ', ' || SQLERRM, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- счет создается в случае если по какой-то причине счет за 
-- прошедший период не был выставлен, а клиент хочет 
-- чтобы потерянные начисления вошли в отдельный счет,
-- а не попали отдельными позициями в текущий счет
-- возвращает:
--   - положительное - ID счета, 
--   - при ошибке выставляет исключение
FUNCTION Open_rec_bill_for_old_period (
               p_account_id    IN INTEGER,   -- ID лицевого счета
               p_rep_period_id IN INTEGER,   -- ID расчетного периода YYYYMM
               p_bill_no       IN VARCHAR2,  -- Номер счета
               p_currency_id   IN INTEGER,   -- ID валюты счета
               p_bill_date     IN DATE,       -- Дата счета (биллингового периода)
               p_notes         IN VARCHAR2    DEFAULT NULL
            ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Open_rec_bill_for_old_period';
    v_bill_id       INTEGER;                   -- формат POID: YYMM.XXX.XXX.XXX,
    v_profile_id    INTEGER;
    v_contract_id   INTEGER;
    v_contractor_id INTEGER;
    v_bank_id       INTEGER;
    v_vat           NUMBER;
    v_bill_no       BILL_T.BILL_NO%TYPE;
BEGIN
    -- проверяем период счета 
    Check_bill_period(p_rep_period_id, p_account_id);  

    -- Формируем ID объекта (POID) для указанного биллингового периода 
    v_bill_id := Pk02_POID.Next_bill_id;
    
    -- принудительно ставим 'O' в конце номера счета
    IF SUBSTR(v_bill_no,-1) != 'O' THEN
      v_bill_no := v_bill_no || 'O';
    END IF;
    
    -- получаем id договора и ставку НДС
    Read_account_profile (
               p_account_id    => p_account_id,
               p_bill_date     => p_bill_date,
               p_profile_id    => v_profile_id,
               p_contract_id   => v_contract_id,
               p_contractor_id => v_contractor_id,
               p_bank_id       => v_bank_id,
               p_vat           => v_vat
           );
    
    -- Cоздаем периодический счет для последующего заполнения
    INSERT INTO BILL_T (
        ACCOUNT_ID,      -- ID лицевого счета
        BILL_ID,         -- ID лицевого счета
        REP_PERIOD_ID,   -- ID расчетного периода YYYYMM
        BILL_TYPE,       -- Тип счета
        BILL_NO,         -- Номер счета
        CURRENCY_ID,     -- ID валюты счета
        BILL_DATE,       -- Дата счета (биллингового периода)
        BILL_STATUS,     -- состояние счета
        PROFILE_ID,      -- ID профиля л/с
        CONTRACT_ID,     -- ID договора
        CONTRACTOR_ID,   -- ID продавца
        CONTRACTOR_BANK_ID, -- ID банка продавца
        VAT,
        NOTES
    )VALUES(
        p_account_id,
        v_bill_id,
        p_rep_period_id,
        PK00_CONST.c_BILL_TYPE_OLD,
        p_bill_no,
        p_currency_id,
        p_bill_date,
        PK00_CONST.c_BILL_STATE_OPEN,
        v_profile_id,
        v_contract_id,
        v_contractor_id,
        v_bank_id,
        v_vat,
        p_notes
    ); 
    
    INSERT INTO BILL_HISTORY_T(BILL_ID, REP_PERIOD_ID, ACTION) VALUES(v_bill_id, p_rep_period_id, Pk00_Const.c_BILL_HISTORY_CREATE);
     
    RETURN v_bill_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--================================================================================
-- Редактирование шапки счета
PROCEDURE Bill_info_edit(
         p_bill_id           IN INTEGER,   -- ID позиции счета
         p_rep_period_id     IN INTEGER,    -- ID периода счета
         p_bill_No           IN VARCHAR2,
         p_currency_id       IN INTEGER,
         p_bill_date         IN DATE,
         p_rep_period_id_new IN INTEGER,
         p_bill_type         IN VARCHAR2 DEFAULT NULL,
         p_notes             IN VARCHAR2 DEFAULT NULL
)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bill_info_edit';
BEGIN  
    UPDATE BILL_T
        SET BILL_NO = NVL(p_bill_no,BILL_NO),
            CURRENCY_ID = NVL(p_currency_id, CURRENCY_ID),
            BILL_DATE = NVL(p_bill_date, BILL_DATE),
            REP_PERIOD_ID = NVL(p_rep_period_id_new, REP_PERIOD_ID),
            BILL_TYPE = NVL(p_bill_type, BILL_TYPE),
            NOTES = NVL (p_notes, NOTES)
      WHERE BILL_ID = p_bill_id AND REP_PERIOD_ID = p_rep_period_id;      
      
    -- Если изменяется период счета, нужно обновить его в истории
    IF p_rep_period_id <> p_rep_period_id_new THEN
        UPDATE BILL_HISTORY_T
          SET REP_PERIOD_ID = p_rep_period_id_new
        WHERE BILL_ID = p_bill_id 
          AND REP_PERIOD_ID = p_rep_period_id;
    END IF;    
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ===============================================================================
-- Удаление счета
-- Возвращает:
--  0 - счет удален
-- -1 - ошибка, счет принадлежит закрытому периоду
-- -2 - ошибка, на счет есть ссылка, например Дебет-нота (BILL_T.NEXT_BILL_ID is not null)
-- если задан параметр p_force, то можно удалить счет из закрытого периода
-- =============================================================================== 
FUNCTION Delete_bill (
              p_bill_id      IN INTEGER,
              p_period_id    IN INTEGER,
              p_force        IN INTEGER DEFAULT 0
          ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Delete_bill';
    v_bill_type      BILL_T.BILL_TYPE%TYPE;
    v_prev_bill_id   INTEGER; 
    v_prev_period_id INTEGER;
    v_next_bill_id   INTEGER; 
    v_next_period_id INTEGER; 
    v_close_rep_period DATE;
    v_count          INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
   
    -- читаем параметры счета
    SELECT B.BILL_TYPE, 
           B.PREV_BILL_ID, B.PREV_BILL_PERIOD_ID,
           B.NEXT_BILL_ID, B.NEXT_BILL_PERIOD_ID,
           P.CLOSE_REP_PERIOD
      INTO v_bill_type, 
           v_prev_bill_id, v_prev_period_id,
           v_next_bill_id, v_next_period_id, 
           v_close_rep_period
      FROM BILL_T B, PERIOD_T P
     WHERE B.REP_PERIOD_ID = P.PERIOD_ID
       AND B.BILL_ID = p_bill_id
       AND B.REP_PERIOD_ID = p_period_id;
    --
    -- проверяем, что период открыт
    IF v_close_rep_period IS NOT NULL THEN
        -- фиксируем попытку изменить счет закрытого периода
        Pk01_Syslog.Write_msg('bill_id='||p_bill_id||', period='||p_period_id||' - closed'
                             , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_warn );
        IF p_force = 0 THEN
            RETURN -1;
        END IF;  
    END IF;
    --
    -- проверяем, что счет последний в цепочке
    IF v_next_bill_id IS NOT NULL THEN
        Pk01_Syslog.Write_msg('bill_id='||p_bill_id||', period='||p_period_id||' - next_bill_id is not null'
                             , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );
        RETURN -2;
    END IF;

    -- удаляем ссылку на удаляемый счет у предшествующих счетов
    IF v_prev_bill_id IS NOT NULL THEN
        UPDATE BILL_T B SET B.NEXT_BILL_ID = NULL, B.NEXT_BILL_PERIOD_ID = NULL
         WHERE B.REP_PERIOD_ID = v_prev_period_id
           AND B.BILL_ID = v_prev_bill_id
        ; 
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('BILL_T.NEXT_BILL_ID: '||v_count||' set to null', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    END IF;

    -- удаляем позиции начислений
    DELETE FROM ITEM_T I 
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.BILL_ID = p_bill_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем позиции счета 
    DELETE FROM INVOICE_ITEM_T V
     WHERE V.REP_PERIOD_ID = p_period_id
       AND V.BILL_ID = p_bill_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_count||' deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем исходный счет
    DELETE FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_period_id
       AND B.BILL_ID = p_bill_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    RETURN 0;
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- установить статус счета
-- при ошибке выставляем исключение 
PROCEDURE Set_status (
               p_bill_id       IN INTEGER,   -- ID счета 
               p_rep_period_id IN INTEGER,   -- ID расчетного периода YYYYMM
               p_bill_status   IN VARCHAR2   -- статус счета
           ) 
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_status';
BEGIN
    UPDATE BILL_T SET BILL_STATUS = p_bill_status 
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- получить статус счета, возвращает
-- - статус счета
-- - при ошибке выставляем исключение 
FUNCTION Get_status (
               p_bill_id       IN INTEGER,
               p_rep_period_id IN INTEGER    -- ID расчетного периода YYYYMM
           ) RETURN VARCHAR2
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Get_status';
    v_bill_status BILL_T.BILL_STATUS%TYPE;
BEGIN
    SELECT BILL_STATUS INTO v_bill_status
      FROM BILL_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    RETURN v_bill_status;
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- закрыть счет 
-- при ошибке выставляем исключение
PROCEDURE Close_bill( p_bill_id IN INTEGER, p_rep_period_id IN INTEGER )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Close_bill';
BEGIN
    Set_status ( p_bill_id, p_rep_period_id, PK00_CONST.c_BILL_STATE_CLOSED );
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Проверка правильности установки префикса региона
FUNCTION Check_region_prefix ( 
             p_bill_id   IN INTEGER,
             p_period_id IN INTEGER 
         ) RETURN VARCHAR2
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Check_region_prefix';
    v_bill_no      BILL_T.BILL_NO%TYPE;
BEGIN
    -- получаем код региона и номер счета
    SELECT 
      CASE
        WHEN SUBSTR(B.BILL_NO,5,1) = '/' AND CR.REGION_ID != SUBSTR(B.BILL_NO,1,4) THEN
          -- некорректно указан регион
          LPAD(TO_CHAR(CR.REGION_ID), 4,'0')||'/'||SUBSTR(B.BILL_NO,6)
        WHEN SUBSTR(B.BILL_NO,5,1) = '/' AND CR.REGION_ID IS NULL THEN
          -- регион указан, а его быть не должно
          SUBSTR(B.BILL_NO,6)
        WHEN SUBSTR(B.BILL_NO,5,1) != '/' AND CR.REGION_ID IS NOT NULL THEN
          -- не указан регион, а должен быть
          LPAD(TO_CHAR(CR.REGION_ID), 4,'0')||'/'||SUBSTR(B.BILL_NO,6)
        ELSE
          -- все в порядке
          B.BILL_NO
       END BILL_NO
      INTO v_bill_no
      FROM CONTRACTOR_T CR, BILL_T B
     WHERE B.REP_PERIOD_ID  = p_period_id
       AND B.BILL_ID        = p_bill_id
       AND B.CONTRACTOR_ID  = CR.CONTRACTOR_ID
    ;
    RETURN v_bill_no;
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Получить следующий номер периодического счета в BRM
-- номера счетов формируются по единому правилу: YYMM(№ Л/С)[A-Z]
FUNCTION Next_rec_bill_no(
               p_account_id     IN INTEGER,
               p_bill_period_id IN INTEGER
           ) RETURN VARCHAR2
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Next_rec_bill_no';
    v_region_id    INTEGER;
    v_date_from    DATE;
    v_date_to      DATE;
    v_bill_no      BILL_T.BILL_NO%TYPE := NULL;
    v_account_no   ACCOUNT_T.ACCOUNT_NO%TYPE;
BEGIN
    v_date_from := Pk04_Period.Period_from(p_bill_period_id);
    v_date_to   := Pk04_Period.Period_to(p_bill_period_id);
    -- получаем допольнительную информацию
    SELECT A.ACCOUNT_NO, CR.REGION_ID
      INTO v_account_no, v_region_id
      FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACTOR_T CR
     WHERE A.ACCOUNT_ID = p_account_id
       AND A.ACCOUNT_ID = AP.ACCOUNT_ID
       AND AP.DATE_FROM <= v_date_to
       AND (AP.DATE_TO IS NULL OR v_date_from <= AP.DATE_TO 
            OR ap.actual = 'Y' -- добавлено по согласованию с Макеевым С.В. 13.05.2016 
           )
       AND AP.CONTRACTOR_ID = CR.CONTRACTOR_ID
       AND ROWNUM = 1;      -- страхуемся от задвоений, хотя их быть не должно

    -- формируем номер счета
    v_bill_no := SUBSTR(TO_CHAR(p_bill_period_id),3,4)||v_account_no;
    -- с переходом на филиальную структуру, добавляем номер региона
    IF v_region_id IS NOT NULL THEN
        v_bill_no := v_region_id||'/'||v_bill_no;
    END IF;
    
    RETURN v_bill_no;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR(account_id=' ||p_account_id||
                                        ', period_id='  ||p_bill_period_id||')'
                                    , c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Получить следующий номер счета в BRM
-- номера счетов формируются по единому правилу: YYMM(№ Л/С)[A-Z]
FUNCTION Next_bill_no(
               p_account_id     IN INTEGER,
               p_bill_period_id IN INTEGER
           ) RETURN VARCHAR2
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Next_bill_no';
    v_billing_id   INTEGER;
    v_region_id    INTEGER;
    v_date_from    DATE;
    v_date_to      DATE;
    v_bill_no      BILL_T.BILL_NO%TYPE := NULL;
    v_account_no   ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_count        INTEGER;
    v_next         INTEGER;
BEGIN
    v_date_from := Pk04_Period.Period_from(p_bill_period_id);
    v_date_to   := Pk04_Period.Period_to(p_bill_period_id);
    -- получаем допольнительную информацию
    SELECT A.ACCOUNT_NO, A.BILLING_ID, CR.REGION_ID
      INTO v_account_no, v_billing_id, v_region_id
      FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACTOR_T CR
     WHERE A.ACCOUNT_ID = p_account_id
       AND A.ACCOUNT_ID = AP.ACCOUNT_ID
       AND AP.DATE_FROM <= v_date_to
       AND (AP.DATE_TO IS NULL OR v_date_from <= AP.DATE_TO)
       AND AP.CONTRACTOR_ID = CR.CONTRACTOR_ID
       AND ROWNUM = 1;      -- страхуемся от задвоений, хотя их быть не должно

    -- формируем номер счета
    v_bill_no := SUBSTR(TO_CHAR(p_bill_period_id),3,4)||v_account_no;
    -- проверяем на уникальность
    v_next := 0;    
    LOOP
        -- проверяем существует ли номер
        SELECT COUNT(*) INTO v_count
          FROM BILL_T B
         WHERE B.BILL_NO = v_bill_no;  
        EXIT WHEN v_count = 0;  -- все нормально, выходим из цикла
        --
        -- формируем следующий по порядку счет    
        -- в BRM принята единая схема нумерации счетов: YYMM(№ Л/С)[C,D,E-Z]
        -- С, D - зарезеовированы для кредит/дебит нот
        v_bill_no := SUBSTR(TO_CHAR(p_bill_period_id),3,4)
                         ||v_account_no||CHR(ASCII('D')+v_next);
        --
        v_next := v_next + 1;
    END LOOP;
    
    -- с переходом на филиальную структуру, добавляем номер региона
    IF v_region_id IS NOT NULL THEN
        v_bill_no := v_region_id||'/'||v_bill_no;
    END IF;
    
    RETURN v_bill_no;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR(account_id=' ||p_account_id||
                                        ', period_id='  ||p_bill_period_id||')'
                                    , c_PkgName||'.'||v_prcName );
END;

/*
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- УСТАРЕВШАЯ ВЕРСИЯ ДО 31.12.2014
-- Получить следующий номер периодического счета
-- следует учитывать, что в биллинге "Микротест" и "старом биллинге" 
-- номера счетов формируются по разным правилам:
-- "Микротест" - CONTRACT_NO_XXXX, где XXXX - порядковый номер счета
--               следует учитывать, что на одном договоре, может быть
--               несколько лицевых счетов
-- "старом биллинге" - YYMM(№ Л/С)[A-Z]
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
FUNCTION Next_bill_no(
               p_account_id     IN INTEGER,
               p_contract_id    IN INTEGER,
               p_bill_period_id IN INTEGER
           ) RETURN VARCHAR2
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Next_bill_no';
    v_billing_id   INTEGER;
    v_sq_bill_no   INTEGER;
    v_region_id    INTEGER;
    v_date_from    DATE;
    v_date_to      DATE;
    v_bill_no      BILL_T.BILL_NO%TYPE := NULL;
    v_account_no   ACCOUNT_T.ACCOUNT_NO%TYPE;
    v_contract_no  CONTRACT_T.CONTRACT_NO%TYPE;
    v_count        INTEGER;
BEGIN
  
    v_date_from := Pk04_Period.Period_from(p_bill_period_id);
    v_date_to   := Pk04_Period.Period_to(p_bill_period_id);
    -- получаем допольнительную информацию
    SELECT A.ACCOUNT_NO, A.BILLING_ID, LPAD(TO_CHAR(CR.REGION_ID), 4,'0') 
      INTO v_account_no, v_billing_id, v_region_id
      FROM ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACTOR_T CR
     WHERE A.ACCOUNT_ID = p_account_id
       AND A.ACCOUNT_ID = AP.ACCOUNT_ID
       AND AP.DATE_FROM <= v_date_to
       AND (AP.DATE_TO IS NULL OR v_date_from <= AP.DATE_TO)
       AND AP.CONTRACTOR_ID = CR.CONTRACTOR_ID
       AND ROWNUM = 1;      -- страхуемся от задвоений, хотя их быть не должно

    LOOP
        -- в зависимости от типа биллинга, правила формирования номера счета разные
        IF v_billing_id = Pk00_Const.c_BILLING_MMTS THEN 
            SELECT C.CONTRACT_NO, BS.BILL_SQ --NVL(BS.BILL_SQ,0)+1
              INTO v_contract_no, v_sq_bill_no
              FROM CONTRACT_T C, CONTRACT_BILL_SQ_T BS 
             WHERE C.CONTRACT_ID = p_contract_id
               AND C.CONTRACT_NO = BS.CONTRACT_NO(+);
            --    
            IF v_sq_bill_no IS NULL THEN
            
               -- самый первый счет у клиента
                v_sq_bill_no := 1;
               -- вносим новую запись в счетчик счетов для клиента     
                INSERT INTO CONTRACT_BILL_SQ_T(CONTRACT_NO, BILL_SQ, MODIFY_DATE)
                VALUES(v_contract_no, v_sq_bill_no, SYSDATE);            

            ELSE            
            
               v_sq_bill_no := v_sq_bill_no + 1;
              -- обновляем счетчик счетов клиента
               UPDATE CONTRACT_BILL_SQ_T 
                  SET BILL_SQ = v_sq_bill_no,
                      modify_date = SYSDATE
                WHERE CONTRACT_NO = v_contract_no;            
            
            END IF;

           -- формируем уникальный номер счета            
            v_bill_no := v_contract_no||'-'||LPAD(TO_CHAR(v_sq_bill_no), 4,'0');  
            --

        ELSE -- для остальных предполагаем применять правило YYMM(№ Л/С)[A-Z]
             v_bill_no := SUBSTR(TO_CHAR(p_bill_period_id),3,4)||v_account_no;
        END IF;    
    
        -- т.к. судя по всему кто-то еще вносит данные в bill_t, то проверяем, нет ли сформированного номера
        -- глупость конечно, но это пока так...
        SELECT COUNT(*) INTO v_count
          FROM BILL_T B
         WHERE B.BILL_NO = v_bill_no;
        EXIT WHEN v_count = 0;  -- все нормально, выходим из цикла
        
    END LOOP;
    
    -- с переходом на филиальную структуру, добавляем номер региона
    IF v_region_id IS NOT NULL THEN
        v_bill_no := v_region_id||'/'||v_bill_no;
    END IF;
    
    RETURN v_bill_no;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR(account_id=' ||p_account_id||
                                        ', contract_id='||p_contract_id||
                                        ', period_id='  ||p_bill_period_id||')'
                                    , c_PkgName||'.'||v_prcName );
END;
*/

-- служебная процедура восстановления данных в таблице CONTRACT_BILL_SQ_T
PROCEDURE Fill_contract_bill_sq_t
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Fill_contract_bill_sq_t';
    v_count          INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --
    MERGE INTO CONTRACT_BILL_SQ_T CB
    USING(
        WITH BN AS (
        SELECT C.CONTRACT_NO, A.BILLING_ID, B.BILL_NO,
               MAX(B.REP_PERIOD_ID) OVER (PARTITION BY C.CONTRACT_NO) MAX_PERIOD_ID,
               B.REP_PERIOD_ID, SUBSTR(BILL_NO, INSTR(BILL_NO, '-', -1)+1, 4) BILL_SQ     
          FROM BILL_T B, ACCOUNT_PROFILE_T AP, CONTRACT_T C, ACCOUNT_T A 
         WHERE B.ACCOUNT_ID = AP.ACCOUNT_ID
           AND AP.CONTRACT_ID = C.CONTRACT_ID
           AND A.ACCOUNT_ID   = AP.ACCOUNT_ID
           AND A.BILLING_ID  = Pk00_Const.c_BILLING_MMTS -- 2003
        )
        SELECT BN.CONTRACT_NO, MAX(BN.BILL_SQ) BILL_SQ
          FROM BN
         WHERE BN.REP_PERIOD_ID = BN.MAX_PERIOD_ID
           AND LTRIM(BN.BILL_SQ,'0123456789') IS NULL 
         GROUP BY BN.CONTRACT_NO
    ) SQ
    ON ( CB.CONTRACT_NO = SQ.CONTRACT_NO )
    WHEN MATCHED THEN UPDATE SET CB.BILL_SQ = SQ.BILL_SQ
    WHEN NOT MATCHED THEN INSERT (CB.CONTRACT_NO, CB.BILL_SQ) VALUES (SQ.CONTRACT_NO, SQ.BILL_SQ);
    --
    v_count := SQL%ROWCOUNT;
    --
    Pk01_Syslog.Write_msg('Merged into CONTRACT_BILL_SQ_T '||v_count||' rows', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('Stop', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


FUNCTION Move_BDR(p_Rep_Id_Old  number,
                  p_Rep_Id_New  number,
                  p_Bill_Id_Old number,
                  p_Bill_Id_New number,
                  p_Item_Id_Old number,
                  p_Item_Id_New number,
                  p_external_Id number
                 ) RETURN number
IS
    l_BDR_Table varchar2(30);
    l_Oper      number;
    l_Acc_Type  varchar2(1);
BEGIN
    SELECT bdr_table, oper
      INTO l_BDR_Table, l_Oper
      FROM bdr_types_t 
     WHERE bdr_type_id = p_external_id; 

   -- перенос BDR-ов
    EXECUTE IMMEDIATE
        'UPDATE ' || l_BDR_Table || ' b ' || CHR(10) ||
        '   SET b.rep_period = (CASE WHEN :l_Rep_Date_New = TRUNC(b.local_time,''mm'') THEN b.local_time ' || CHR(10) ||
        '                            ELSE :l_Rep_Date_New ' || CHR(10) ||
        '                       END), ' || CHR(10) ||     
        '       b.bill_id = :p_Bill_Id_New, ' || CHR(10) ||
        '       b.item_id = :p_Item_Id_New ' || CHR(10) ||
        ' WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
        '   AND b.bill_id = :p_Bill_Id_Old ' || CHR(10) ||
        '   AND b.item_id = :p_Item_Id_Old '
        USING TO_DATE(TO_CHAR(p_Rep_Id_New),'YYYYMM'), TO_DATE(TO_CHAR(p_Rep_Id_New),'YYYYMM'),
              p_Bill_Id_New, p_Item_Id_New,
              TO_DATE(TO_CHAR(p_Rep_Id_Old),'YYYYMM'), LAST_DAY(TO_DATE(TO_CHAR(p_Rep_Id_Old),'YYYYMM'))+INTERVAL '0 23:59:59' DAY TO SECOND,
              p_Bill_Id_Old, p_Item_Id_Old;

    -- перенос записей детализаций
    IF l_Oper = 0 THEN
        -- получаем тип л/счета (физ. лицо или юрид.)
        SELECT a.account_type 
          INTO l_Acc_Type
          FROM bill_t b,
               account_t a
         WHERE b.rep_period_id = p_Rep_Id_Old
           AND b.account_id = a.account_id;
           
        IF l_Acc_Type = 'J' THEN
        
            UPDATE DETAIL_MMTS_T_JUR
               SET rep_period_id = p_Rep_Id_New,
                   bill_id = p_Bill_Id_New,
                   item_id = p_Item_Id_New
             WHERE rep_period_id = p_Rep_Id_Old         
               AND bill_id = p_Bill_Id_Old
               AND item_id = p_Item_Id_Old;
        
        ELSIF l_Acc_Type = 'P' THEN

            UPDATE DETAIL_MMTS_T_FIZ
               SET rep_period_id = p_Rep_Id_New,
                   item_id = p_Item_Id_New
             WHERE rep_period_id = p_Rep_Id_Old         
               AND item_id = p_Item_Id_Old;        
        
        END IF;   
               
    ELSE -- операторская группировка
    
        UPDATE DETAIL_OPER_T_JUR d
           SET d.rep_period_id = p_Rep_Id_New,
               d.item_id = p_Item_Id_New
         WHERE d.rep_period_id = p_Rep_Id_Old
           AND d.item_id = p_Item_Id_Old;
             
    END IF;

    RETURN SQL%ROWCOUNT;

EXCEPTION
    WHEN no_data_found THEN
        RETURN 0; -- нет таких BDR-ов
END Move_BDR;     

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- создание описателя счетов для нового Л/С
--   - при ошибке выставляет исключение
PROCEDURE New_billinfo (
               p_account_id       IN INTEGER,   -- ID лицевого счета
               p_currency_id      IN INTEGER,   -- ID валюты счета
               p_delivery_id      IN INTEGER,   -- ID способа доставки счета
               p_days_for_payment IN INTEGER DEFAULT 30   -- кол-во дней на оплату счета
           )
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'New_billinfo';
    v_period_length  INTEGER := 1;
    v_account_no     ACCOUNT_T.ACCOUNT_NO%TYPE := NULL;
    v_count          INTEGER;
    v_utc_date       DATE := SYSDATE;
    v_local_date     DATE := SYSDATE+GET_TZ_OFFSET;
BEGIN
    -- создаем информационную запись о счетах для вновь созданного Л/С
    INSERT INTO BILLINFO_T ( 
        ACCOUNT_ID, PERIOD_LENGTH,  DAYS_FOR_PAYMENT
    )
    WITH AC AS (
        SELECT AP.ACCOUNT_ID,
               AP.DATE_FROM, 
               AP.DATE_TO,
               ROW_NUMBER() OVER (PARTITION BY AP.ACCOUNT_ID ORDER BY AP.DATE_FROM) RN,
               CASE
               WHEN AP.DATE_FROM <= v_local_date AND (AP.DATE_TO IS NULL OR AP.DATE_TO < v_local_date) THEN 1
               WHEN v_utc_date <= AP.DATE_FROM AND AP.DATE_TO IS NULL THEN 2 -- открыт будущим числом
               ELSE 0
               END ITV
          FROM ACCOUNT_PROFILE_T AP, CONTRACT_T C, CONTRACTOR_T CT
         WHERE AP.CONTRACT_ID = C.CONTRACT_ID
           AND AP.BRANCH_ID   = CT.CONTRACTOR_ID
           AND AP.ACCOUNT_ID  = p_account_id
    )
    SELECT ACCOUNT_ID,  v_period_length, p_days_for_payment
      FROM AC
     WHERE (ITV = 1 OR (ITV = 2 AND RN = 1));

     -- Добавляем способ доставки для комплекта документов
     INSERT INTO 
            ACCOUNT_DOCUMENTS_T (ACCOUNT_ID,DOC_BILL,DELIVERY_METHOD_ID) 
       VALUES (p_account_id, 'Y', p_delivery_id);

    /*
    SELECT AP.ACCOUNT_ID, C.CONTRACT_NO BILL_NAME, 0 SQ_BILL_NO,
           v_period_length, p_currency_id, p_days_for_payment,
           p_delivery_id
      FROM ACCOUNT_PROFILE_T AP, CONTRACT_T C, CONTRACTOR_T CT
     WHERE AP.CONTRACT_ID = C.CONTRACT_ID
       AND AP.ACCOUNT_ID  = p_account_id
       AND ( -- ищем открытую на текущий момент позицию
           (AP.DATE_FROM <= (SYSDATE+1/6) AND (AP.DATE_TO IS NULL OR AP.DATE_TO < (SYSDATE+1/6))) 
           OR -- если договор открыт будущим числом
           (SYSDATE <= AP.DATE_FROM AND AP.DATE_TO IS NULL)
       )
       AND AP.BRANCH_ID = CT.CONTRACTOR_ID;
    */
    -- 
    v_count := SQL%ROWCOUNT;
    IF v_count = 0 THEN
        -- возможно договор создан будущим числом
        INSERT INTO BILLINFO_T ( 
            ACCOUNT_ID, PERIOD_LENGTH, DAYS_FOR_PAYMENT
        )
        SELECT AP.ACCOUNT_ID, v_period_length, p_days_for_payment
          FROM ACCOUNT_PROFILE_T AP, CONTRACT_T C, CONTRACTOR_T CT
         WHERE AP.CONTRACT_ID = C.CONTRACT_ID
           AND AP.ACCOUNT_ID  = p_account_id
           AND ( 
               (AP.DATE_FROM <= v_local_date AND (AP.DATE_TO IS NULL OR AP.DATE_TO < v_local_date)) 
               OR
               (v_utc_date <= AP.DATE_FROM AND AP.DATE_TO IS NULL)
           )
           AND AP.BRANCH_ID = CT.CONTRACTOR_ID;
    
        SELECT ACCOUNT_NO INTO v_account_no 
          FROM ACCOUNT_T 
         WHERE ACCOUNT_ID = p_account_id;
        --         
        Pk01_Syslog.Raise_user_exception('account_id='||p_account_id||
               ', account_no='||v_account_no||'- данные не найдены', 
               c_PkgName||'.'||v_prcName);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

/*
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- сформировать счет: получить сумму начислений и корректировок счета по позициям
-- и проставить признак, что счет сформирован , 
-- т.е. начисления на его позиции не возможны, 
-- принимаются только оплаты
-- возвращает:
-- - возвращаем сумму начислений и корректировок по счету (то что должно быть оплачено)
-- - при ошибке выставляет исключение
FUNCTION Generate_bill (
               p_bill_id       IN INTEGER,   -- ID позиции счета
               p_rep_period_id IN INTEGER    -- ID периода счета
           ) RETURN NUMBER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Generate_bill';
    v_bill_total NUMBER;
    --
BEGIN
    -- суммируем все позиции счета-фактуры (что бы не разойтись по налогам): 
    UPDATE BILL_T B
       SET (TOTAL, GROSS, TAX, DUE, BILL_STATUS, CALC_DATE) = (
          SELECT SUM(II.TOTAL) TOTAL, SUM(II.GROSS) GROSS, SUM(II.TAX) TAX,
                 -(SUM(II.TOTAL)+SUM(II.GROSS)+SUM(II.TAX)) DUE,
                 PK00_CONST.c_BILL_STATE_READY,  SYSDATE
            FROM INVOICE_ITEM_T II
           WHERE II.BILL_ID = B.BILL_ID
             AND II.REP_PERIOD_ID = B.REP_PERIOD_ID
     )
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id
    RETURNING TOTAL INTO v_bill_total;
     -- возвращаем сумму начислений и корректировок по счету (то что должно быть оплачено)
    RETURN v_bill_total;
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Рассформировать счет (только для статуса READY): 
-- обнулить сумму начислений и корректировок счета по позициям
-- удалить строки счета фактуры, если были сформированы
-- и вернуть признак OPEN,
-- т.е. сделать возможными начисления на его позиции 
-- возвращает:
--   - положительное - ID счета, 
--   - при ошибке выставляет исключение
FUNCTION Rollback_bill (
               p_bill_id       IN INTEGER,   -- ID позиции счета
               p_rep_period_id IN INTEGER    -- ID периода счета
           ) RETURN NUMBER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Rollback_bill';
    v_bill_status BILL_T.BILL_STATUS%TYPE;
BEGIN
    -- проверяем статус счета
    v_bill_status := Get_status(p_bill_id, p_rep_period_id);
    IF v_bill_status != PK00_CONST.c_BILL_STATE_READY THEN
        RAISE_APPLICATION_ERROR(-20000, 'Неверный статус счета (bill_id='||p_bill_id||'): '||v_bill_status);
    END IF;
    -- удаляем ссылки на позиции счета фактуры из ITEM
    UPDATE ITEM_T
       SET INV_ITEM_ID = NULL
     WHERE BILL_ID     = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- удаляем позиции счета фактуры
    DELETE FROM INVOICE_ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- обнуляем суммы счета и возвращаем статус - открыт
    UPDATE BILL_T
       SET TOTAL         = 0,
           GROSS         = 0,
           TAX           = 0,
           DUE           = 0, 
           ADJUSTED      = 0,
           BILL_STATUS   = PK00_CONST.c_BILL_STATE_OPEN
     WHERE BILL_ID       = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
     -- возвращаем сумму начислений и корректировок по счету (то что должно быть оплачено)
     RETURN p_bill_id;
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

--
-- Откат счета. Технологическая функция. Нужно пото убить
--
FUNCTION Rollback_bill_force (
               p_bill_id       IN INTEGER,   -- ID позиции счета
               p_rep_period_id IN INTEGER    -- ID периода счета
           ) RETURN NUMBER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Rollback_bill';
BEGIN
    -- удаляем ссылки на позиции счета фактуры из ITEM
    UPDATE ITEM_T
       SET INV_ITEM_ID = NULL
     WHERE BILL_ID     = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- удаляем позиции счета фактуры
    DELETE FROM INVOICE_ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- обнуляем суммы счета и возвращаем статус - открыт
    UPDATE BILL_T
       SET TOTAL         = 0,
           GROSS         = 0,
           TAX           = 0,
           DUE           = 0, 
           ADJUSTED      = 0,
           BILL_STATUS   = PK00_CONST.c_BILL_STATE_OPEN
     WHERE BILL_ID       = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
     -- возвращаем сумму начислений и корректировок по счету (то что должно быть оплачено)
     RETURN p_bill_id;
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
*/

-- ===============================================================================
-- Перенос счета в другой период строго по команде ДРУ, 
-- т.к. исходный счет не должен был попасть в отчетность
-- Возвращает 
--  bill_id - перенесенного счета
-- -1 - ошибка, счет принадлежит закрытому периоду
-- -2 - ошибка, на счет есть ссылка, например Дебет-нота (BILL_T.NEXT_BILL_ID is not null)
-- если p_force = 1, то перенос из закрытого периода разрешен
-- =============================================================================== 
FUNCTION Move_bill (
              p_bill_id      IN INTEGER,
              p_period_id    IN INTEGER,
              p_period_id_to IN INTEGER,
              p_bill_date_to IN DATE,
              p_force        IN INTEGER DEFAULT 1
          ) RETURN INTEGER
IS
    v_prcName        CONSTANT VARCHAR2(30) := 'Move_bill';
    v_count          INTEGER;
    v_bill_no        BILL_T.BILL_NO%TYPE;
    v_bill_id        INTEGER;
    v_item_id        INTEGER;
    v_inv_id_old     INTEGER := NULL;
    v_inv_id_new     INTEGER := NULL;
    v_bill_count     INTEGER := 0;
    v_item_count     INTEGER := 0;
    v_inv_count      INTEGER := 0;
    v_bdr_count      integer := 0;
    v_close_rep_period DATE;
    v_next_bill_id   INTEGER; 
    v_next_period_id INTEGER;
    v_prev_bill_id   INTEGER; 
    v_prev_period_id INTEGER;
BEGIN
    Pk01_Syslog.Write_msg('START.', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- читаем параметры счета и проверяем, что операция допустима
    SELECT B.PREV_BILL_ID, B.PREV_BILL_PERIOD_ID,
           B.NEXT_BILL_ID, B.NEXT_BILL_PERIOD_ID,
           B.BILL_NO,
           P.CLOSE_REP_PERIOD
      INTO v_prev_bill_id, v_prev_period_id,
           v_next_bill_id, v_next_period_id,
           v_bill_no,
           v_close_rep_period
      FROM BILL_T B, PERIOD_T P
     WHERE B.REP_PERIOD_ID = P.PERIOD_ID
       AND B.BILL_ID = p_bill_id
       AND B.REP_PERIOD_ID = p_period_id;
    
    IF v_close_rep_period IS NOT NULL THEN
        -- фиксируем попытку изменить счет закрытого периода
        Pk01_Syslog.Write_msg('bill_id='||p_bill_id||', period='||p_period_id||' - closed'
                             , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_warn );
        IF p_force = 0 THEN
            RETURN -1;
        END IF;  
    END IF;

    -- проверяем, что счет последний в цепочке - НЕ ВАЖНО !!!
    --IF v_next_bill_id IS NOT NULL THEN
    --    Pk01_Syslog.Write_msg('bill_id='||p_bill_id||', period='||p_period_id||' - next_bill_id IS NOT NULL'
    --                         , c_PkgName||'.'||v_prcName, Pk01_Syslog.L_err );
    --    RETURN -2;
    --END IF;

    -- создаем копию счета в указанном периоде
    v_bill_id := Pk02_Poid.Next_bill_id;
    
    INSERT INTO BILL_T (    
        BILL_ID, REP_PERIOD_ID, ACCOUNT_ID,
        BILL_NO, BILL_DATE, BILL_TYPE, BILL_STATUS, CURRENCY_ID,
        TOTAL, GROSS, TAX, RECVD, DUE, DUE_DATE, PAID_TO,
        PREV_BILL_ID, PREV_BILL_PERIOD_ID, NEXT_BILL_ID, NEXT_BILL_PERIOD_ID,
        CALC_DATE, ACT_DATE_FROM, ACT_DATE_TO, NOTES, DELIVERY_DATE,
        ADJUSTED, CONTRACT_ID, VAT, CREATE_DATE, PROFILE_ID, 
        CONTRACTOR_ID, CONTRACTOR_BANK_ID
    )
    SELECT 
        v_bill_id, p_period_id_to, ACCOUNT_ID,
        BILL_NO||'.NEW',  -- поправим руками, если нужно
        p_bill_date_to, BILL_TYPE, BILL_STATUS, CURRENCY_ID,
        TOTAL, GROSS, TAX, RECVD, DUE, DUE_DATE, PAID_TO,
        PREV_BILL_ID, PREV_BILL_PERIOD_ID, NEXT_BILL_ID, NEXT_BILL_PERIOD_ID,
        CALC_DATE, ACT_DATE_FROM, ACT_DATE_TO, NOTES, DELIVERY_DATE,
        ADJUSTED, CONTRACT_ID, VAT, CREATE_DATE, PROFILE_ID, 
        CONTRACTOR_ID, CONTRACTOR_BANK_ID
      FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_period_id
       AND B.BILL_ID       = p_bill_id;
       
    v_bill_count := SQL%ROWCOUNT;
    
    Pk01_Syslog.Write_msg('BILL_T: '||v_bill_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    Pk01_Syslog.Write_msg('BILL_T.BILL_ID = '||v_bill_id, c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    --
    -- проставляем ссылку на переносимый счет у предшествующих счетов
    IF v_prev_bill_id IS NOT NULL THEN
        UPDATE BILL_T B 
           SET B.NEXT_BILL_ID = v_bill_id, 
               B.NEXT_BILL_PERIOD_ID = p_period_id_to
         WHERE B.REP_PERIOD_ID = v_prev_period_id
           AND B.BILL_ID = v_prev_bill_id
        ; 
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('BILL_T.NEXT_BILL_ID: '||v_count||' SET', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    END IF;
    -- проставляем ссылку на переносимый счет у последующих счетов
    IF v_next_bill_id IS NOT NULL THEN
        UPDATE BILL_T B 
           SET B.PREV_BILL_ID = v_bill_id, B.PREV_BILL_PERIOD_ID = p_period_id_to
         WHERE B.REP_PERIOD_ID = v_next_period_id
           AND B.BILL_ID = v_next_bill_id
        ; 
        v_count := SQL%ROWCOUNT;
        Pk01_Syslog.Write_msg('BILL_T.PREV_BILL_ID: '||v_count||' SET', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    END IF;
    --
    FOR ri IN (
        SELECT ROW_NUMBER() OVER (PARTITION BY BILL_ID, INV_ITEM_ID ORDER BY ITEM_ID) RN,
               BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
               ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
               ITEM_TOTAL, RECVD, DATE_FROM, DATE_TO, 
               ITEM_STATUS, CREATE_DATE, LAST_MODIFIED, 
               REP_GROSS, REP_TAX, TAX_INCL, 
               EXTERNAL_ID, NOTES, ORDER_BODY_ID, DESCR, QUANTITY, 
               BILL_TOTAL, ITEM_CURRENCY_ID, ITEM_CURRENCY_RATE
          FROM ITEM_T I
         WHERE I.REP_PERIOD_ID = p_period_id
           AND I.BILL_ID       = p_bill_id
         ORDER BY I.INV_ITEM_ID, I.ITEM_ID
    )LOOP
        IF ri.RN = 1 THEN
            -- копируем запись в INVOICE_ITEM_T:
            v_inv_id_old := ri.INV_ITEM_ID;
            IF v_inv_id_old IS NOT NULL THEN
                v_inv_id_new := Pk02_Poid.Next_invoice_item_id;
                INSERT INTO INVOICE_ITEM_T(BILL_ID, REP_PERIOD_ID, INV_ITEM_ID, INV_ITEM_NO, 
                       SERVICE_ID, TOTAL, GROSS, TAX, VAT, INV_ITEM_NAME, 
                       DATE_FROM, DATE_TO)
                SELECT v_bill_id BILL_ID, p_period_id_to REP_PERIOD_ID, 
                       v_inv_id_new INV_ITEM_ID, V.INV_ITEM_NO, 
                       V.SERVICE_ID, V.TOTAL, V.GROSS, V.TAX, V.VAT, V.INV_ITEM_NAME, 
                       V.DATE_FROM, V.DATE_TO
                  FROM INVOICE_ITEM_T V
                 WHERE V.REP_PERIOD_ID = ri.Rep_Period_Id
                   AND V.BILL_ID       = ri.Bill_Id
                   AND V.INV_ITEM_ID   = ri.Inv_Item_Id;
                v_inv_count := v_inv_count + SQL%ROWCOUNT;

            ELSE
                v_inv_id_new := NULL;
            END IF;
        END IF;   
        -- копируем запись в ITEM_T:
        v_item_id := Pk02_Poid.Next_item_id;
        --
        INSERT INTO ITEM_T (
            BILL_ID, REP_PERIOD_ID, ITEM_ID, ITEM_TYPE, INV_ITEM_ID, 
            ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, ITEM_TOTAL, RECVD, 
            DATE_FROM, DATE_TO, ITEM_STATUS, CREATE_DATE, 
            LAST_MODIFIED, REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, 
            ORDER_BODY_ID, DESCR, QUANTITY, ITEM_CURRENCY_ID,
            BILL_TOTAL, ITEM_CURRENCY_RATE)
        SELECT 
            v_bill_id BILL_ID, p_period_id_to REP_PERIOD_ID, 
            v_item_id ITEM_ID, ITEM_TYPE, v_inv_id_new INV_ITEM_ID, 
            ORDER_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, ITEM_TOTAL, RECVD, 
            DATE_FROM, DATE_TO, ITEM_STATUS, CREATE_DATE, 
            SYSDATE LAST_MODIFIED, REP_GROSS, REP_TAX, TAX_INCL, EXTERNAL_ID, NOTES, 
            ORDER_BODY_ID, DESCR, QUANTITY, ITEM_CURRENCY_ID,
            BILL_TOTAL, ITEM_CURRENCY_RATE
          FROM ITEM_T I    
         WHERE I.REP_PERIOD_ID = ri.Rep_Period_Id
           AND I.BILL_ID       = ri.Bill_Id
           AND I.ITEM_ID       = ri.Item_Id;
       v_item_count := v_item_count + SQL%ROWCOUNT;
       
        IF ri.external_id > 0 THEN
            -- перенос BDR-ов
            v_count := Move_BDR(p_Rep_Id_Old  => ri.Rep_Period_Id,
                                p_Rep_Id_New  => p_period_id_to,
                                p_Bill_Id_Old => ri.Bill_Id,
                                p_Bill_Id_New => v_bill_id,
                                p_Item_Id_Old => ri.Item_Id,
                                p_Item_Id_New => v_item_id,
                                p_external_Id => ri.external_id
                               );
            v_bdr_count := v_bdr_count + v_count;                       
        END IF;                                 
       
    END LOOP;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_inv_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    Pk01_Syslog.Write_msg('ITEM_T: '||v_item_count||' created', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );    
    Pk01_Syslog.Write_msg('BDR-ов: '||v_bdr_count||' moved', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем позиции начислений
    DELETE FROM ITEM_T I 
     WHERE I.REP_PERIOD_ID = p_period_id
       AND I.BILL_ID = p_bill_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('ITEM_T: '||v_count||' deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- удаляем позиции счета 
    DELETE FROM INVOICE_ITEM_T V
     WHERE V.REP_PERIOD_ID = p_period_id
       AND V.BILL_ID = p_bill_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('INVOICE_ITEM_T: '||v_count||' deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    
    -- удаляем исходный счет
    DELETE FROM BILL_T B
     WHERE B.REP_PERIOD_ID = p_period_id
       AND B.BILL_ID = p_bill_id;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('BILL_T: '||v_count||' deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );

    -- возвращаем счету номер
    UPDATE BILL_T B SET B.BILL_NO = v_bill_no
     WHERE B.BILL_ID = v_bill_id
       AND B.REP_PERIOD_ID = p_period_id_to;

    Pk01_Syslog.Write_msg('STOP', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    --    
    RETURN v_bill_id;
    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Рассчитать задолженность по счету
-- возвращает:
-- - возвращаем сумму задолженности по счету
-- - при ошибке выставляет исключение
FUNCTION Calculate_due(
               p_bill_id       IN INTEGER,   -- ID позиции счета
               p_rep_period_id IN INTEGER    -- ID периода счета
           ) RETURN NUMBER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Calculate_due';
    v_bill_type    INTEGER; 
    v_next_bill_id INTEGER;
    v_total        NUMBER;
    v_recvd        NUMBER;
    v_due_date     DATE;
    v_due          NUMBER;
BEGIN
    -- получаем информацию о счете
    SELECT B.BILL_TYPE, B.NEXT_BILL_ID, B.TOTAL
      INTO v_bill_type, v_next_bill_id, v_total 
      FROM BILL_T B  
     WHERE B.BILL_ID = p_bill_id
       AND B.REP_PERIOD_ID = p_rep_period_id;
       
    -- посчитываем кол-во денег разнесенных на счет
    SELECT SUM(PT.TRANSFER_TOTAL) RECVD, 
           MAX(PT.TRANSFER_DATE) DUE_DATE
      INTO v_recvd, v_due_date
      FROM PAY_TRANSFER_T PT
     WHERE PT.BILL_ID = p_bill_id
       AND PT.REP_PERIOD_ID = p_rep_period_id; 

    IF v_bill_type = 'B' AND v_next_bill_id IS NULL THEN
        v_due := v_recvd - v_total;
        -- регулярные счета, для них нет корректировок
        UPDATE BILL_T B SET B.ADJUSTED = 0, 
                            B.RECVD    = v_recvd,
                            B.DUE      = v_due,
                            B.DUE_DATE = v_due_date
         WHERE B.BILL_TYPE = 'B'
           AND B.ADJUSTED != 0
           AND B.NEXT_BILL_ID IS NULL
         RETURNING B.DUE INTO v_due;
    ELSIF v_bill_type = 'C' THEN
        v_due := 0;
        -- по кредит нотам никто никому не должен
        UPDATE BILL_T B SET B.DUE      = v_due, 
                            B.ADJUSTED = B.TOTAL - v_recvd,
                            B.RECVD    = v_recvd  -- вместе со счетом, сторнируем разноски (не удаляем!!!)
         WHERE B.BILL_TYPE = 'C';
    ELSIF v_next_bill_id IS NOT NULL THEN
        v_due := v_recvd - v_total;  -- корректировка баланса тоько для кредит-нот
        -- по скорректированным счетам тоже задолженности нет (не везде сняты разноски платежей)
        UPDATE BILL_T B SET B.DUE = 0, B.ADJUSTED = -B.TOTAL
         WHERE B.NEXT_BILL_ID IS NOT NULL
           AND B.RECVD = 0
           AND B.BILL_TYPE != 'C';
         
         
    ELSIF v_next_bill_id IS NOT NULL AND v_recvd = 0 THEN
        v_due := 0;
        -- по скорректированным счетам тоже задолженности нет (не везде сняты разноски платежей)
        UPDATE BILL_T B SET B.DUE = 0, B.ADJUSTED = -B.TOTAL
         WHERE B.NEXT_BILL_ID IS NOT NULL
           AND B.RECVD = 0
           AND B.BILL_TYPE != 'C';
    ELSIF v_next_bill_id IS NOT NULL AND v_recvd != 0 THEN
        v_due := NULL;
        -- фиксируем ошибку если не сняты разноски платежей на скорректированный счет 
        Pk01_Syslog.Raise_user_exception('Bill_id = '||p_bill_id||' скорректирован, но с него не сняты оплаты'
                                         , c_PkgName||'.'||v_prcName);
    END IF;
    -- возвращаем сумму начислений и корректировок по счету (то что должно быть оплачено)
    RETURN v_due;
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- найти все позиции указанного счета
--   - положительное - кол-во выбранных записей
--   - при ошибке выставляет исключение
FUNCTION Items_list( 
               p_recordset    OUT t_refc, 
               p_bill_id       IN INTEGER,   -- ID позиции счета
               p_rep_period_id IN INTEGER    -- ID периода счета
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Items_list';
    v_retcode    INTEGER;
BEGIN
    -- вычисляем кол-во записей
    SELECT COUNT(*) INTO v_retcode
      FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- возвращаем курсор
    OPEN p_recordset FOR
         SELECT ITEM_ID, ITEM_TYPE, BILL_ID, 
                ORDER_ID, SERVICE_ID, CHARGE_TYPE,  
                ITEM_TOTAL, RECVD,  
                DATE_FROM, DATE_TO, INV_ITEM_ID, ITEM_STATUS
           FROM ITEM_T
          WHERE BILL_ID = p_bill_id
            AND REP_PERIOD_ID = p_rep_period_id
          ORDER BY ITEM_ID;
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Удалить все позиции указанного счета (скорее всего только сразу после ошибочного создания)
--   - положительное - кол-во удаленных записей
--   - при ошибке выставляет исключение
FUNCTION Delete_items (
               p_bill_id       IN INTEGER,   -- ID позиции счета
               p_rep_period_id IN INTEGER    -- ID периода счета
           ) RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Delete_items';
    v_count       INTEGER := 0;
    v_bill_status BILL_T.BILL_STATUS%TYPE;
BEGIN
    -- проверяем статус счета
    v_bill_status := Get_status(p_bill_id, p_rep_period_id);
    IF v_bill_status != PK00_CONST.c_BILL_STATE_OPEN THEN
        RAISE_APPLICATION_ERROR(-20000, 'Неверный статус счета (bill_id='||p_bill_id||'): '||v_bill_status);
    END IF;  
    -- проверяем нет ли сформировананных позиций счета фактуры
    SELECT COUNT(1) INTO v_count
      FROM INVOICE_ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(Pk01_Syslog.n_APP_EXCEPTION, 'Ошибка при удалении позиций счета (item)'
              ||' BILL_ID='|| p_bill_id ||', '
              ||', предварительно необходимо удалить позиции счета-фактуры (invoice-item)');
    END IF;
    
    -- неплохо бы проверить и события (event) и отвязать их от позиций счета
    -- ... сделаю позже

    -- удаляем все позиции указанного счета
    DELETE 
      FROM ITEM_T
     WHERE BILL_ID = p_bill_id
       AND REP_PERIOD_ID = p_rep_period_id;
    -- возвращает кол-во удаленных записей
    RETURN SQL%ROWCOUNT;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Проверка на наличие периодических счетов на лицевом счете в заданном периоде
FUNCTION Check_BillRec_AtPeriod_Has ( 
      p_account_id   IN INTEGER,
      p_period_id    IN INTEGER 
) RETURN INTEGER
IS
    v_prcName      CONSTANT VARCHAR2(30) := 'Check_BillRec_AtPeriod_Has';
    v_bill_count   INTEGER;
BEGIN
    SELECT 
       COUNT(*) INTO v_bill_count
      FROM BILL_T B
     WHERE B.BILL_TYPE IN ('B')
       AND B.REP_PERIOD_ID  = p_period_id
       AND B.ACCOUNT_ID     = p_account_id
    ;
    
    RETURN v_bill_count;
EXCEPTION   -- при ошибке выставляем исключение
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

---------------------------------------------------------------
-- получить примечание к счету
---------------------------------------------------------------
FUNCTION GET_BILL_NOTES(p_bill_id IN NUMBER) RETURN VARCHAR2
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'GET_BILL_NOTES';
    v_retcode       INTEGER;
    v_notes         VARCHAR2(2000);
BEGIN
    SELECT NOTES INTO v_notes FROM BILL_T WHERE BILL_ID = p_bill_id;
    RETURN v_notes;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN '';  
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK07_BILL;
/
