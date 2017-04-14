CREATE OR REPLACE PACKAGE PK92_COMPANY_TG
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK92_COMPANY_TG';
    -- ==============================================================================
    -- ключ - profile_id, в таблице account_id
    TYPE tbl_integer IS TABLE OF INTEGER INDEX BY BINARY_INTEGER;

    -- Pl/sql- таблица для хранения координат изменившихся строк
    vt_company tbl_integer;
    vt_actual  tbl_integer;   
    vt_delete  tbl_integer;

    -- Защелка для предупреждения повторного вызова триггеров.
    latch BOOLEAN := false;

    -- Удаление всех записей из pl/sql-таблицы перед началом обновления.
    PROCEDURE clear_tbls;

    -- Добавить запись о компании в таблицу
    PROCEDURE add_company ( 
                  p_company_id IN BINARY_INTEGER,
                  p_contract_id IN INTEGER
              );
              
    -- Добавить запись об актуальной компании в таблицу
    PROCEDURE add_actual ( 
                  p_company_id IN BINARY_INTEGER,
                  p_contract_id IN INTEGER
              );
              
    -- Добавить запись об удаленной компании в таблицу
    PROCEDURE del_company ( 
                  p_company_id IN BINARY_INTEGER,
                  p_contract_id IN INTEGER
              );

    -- ----------------------------------------------------------------- --
    -- Проставляем признак актуальной компании на договоре
    -- ----------------------------------------------------------------- --
    PROCEDURE set_actual_company( 
                  p_contract_id    IN INTEGER,
                  p_not_company_id IN BINARY_INTEGER DEFAULT NULL -- кроме указанной компании
              );

    -- Проверка на пересечение интервалов дат
    PROCEDURE update_company_t;
 
END PK92_COMPANY_TG;
/
CREATE OR REPLACE PACKAGE BODY PK92_COMPANY_TG
IS

-- ----------------------------------------------------------------- --
-- Удаление всех записей из pl/sql-таблицы перед началом обновления.
-- ----------------------------------------------------------------- --
PROCEDURE clear_tbls
IS
BEGIN
    vt_company.delete();
    vt_actual.delete();
    vt_delete.delete();
END;

-- ----------------------------------------------------------------- --
-- Добавить запись о профиле в таблицу
-- ----------------------------------------------------------------- --
PROCEDURE add_company ( 
              p_company_id  IN BINARY_INTEGER,
              p_contract_id IN INTEGER -- NULL - для записей у которых ACTUAL IS NULL
          ) 
IS
BEGIN
   vt_company(p_company_id) := p_contract_id;
END;

-- ----------------------------------------------------------------- --
-- Добавить запись об актуальном профиле в таблицу
-- ----------------------------------------------------------------- --
PROCEDURE add_actual ( 
              p_company_id  IN BINARY_INTEGER,
              p_contract_id IN INTEGER -- NULL - для записей у которых ACTUAL IS NULL
          ) 
IS
BEGIN
   vt_actual(p_company_id) := p_contract_id;
END;

-- ----------------------------------------------------------------- --
-- Добавить запись об удаленной компании в таблицу
-- ----------------------------------------------------------------- --
PROCEDURE del_company ( 
              p_company_id IN BINARY_INTEGER,
              p_contract_id IN INTEGER
          )
IS
BEGIN
   vt_delete(p_company_id) := p_contract_id;
END;

-- ----------------------------------------------------------------- --
-- Проставляем признак актуальной компании на договоре
-- ----------------------------------------------------------------- --
PROCEDURE set_actual_company( 
              p_contract_id    IN INTEGER,
              p_not_company_id IN BINARY_INTEGER DEFAULT NULL -- кроме указанной компании
          )
IS
    v_count INTEGER;
BEGIN
    -- находим актуальную комапнию на текущий момент
    SELECT COUNT(*) INTO v_count
      FROM COMPANY_T CM
     WHERE SYSDATE BETWEEN CM.DATE_FROM AND NVL(CM.DATE_TO, SYSDATE)
       AND CONTRACT_ID = p_contract_id
       AND CM.ACTUAL   = 'Y'
       AND (p_not_company_id IS NULL OR COMPANY_ID != p_not_company_id)
    ;
    IF v_count = 0 THEN
        -- удаляем старые признаки
        UPDATE COMPANY_T CM SET CM.ACTUAL = NULL
         WHERE CONTRACT_ID = p_contract_id;
        -- проставляем признак у компании актуальной на текущий момент
        UPDATE COMPANY_T CM SET CM.ACTUAL = 'Y'
         WHERE SYSDATE BETWEEN CM.DATE_FROM AND NVL(CM.DATE_TO, SYSDATE)
           AND CONTRACT_ID = p_contract_id
           AND (p_not_company_id IS NULL OR COMPANY_ID != p_not_company_id);
        IF SQL%ROWCOUNT = 0 THEN
          -- проставляем признак у последней бывшей актуальной компании
          UPDATE COMPANY_T CM SET CM.ACTUAL = 'Y'
           WHERE EXISTS (
              SELECT CONTRACT_ID, COMPANY_ID
                FROM
                (
                  SELECT ROW_NUMBER() OVER (PARTITION BY CONTRACT_ID ORDER BY NVL(DATE_TO, SYSDATE) DESC) RN, 
                         COMPANY_ID 
                    FROM COMPANY_T 
                   WHERE CONTRACT_ID = p_contract_id
                     AND (p_not_company_id IS NULL OR COMPANY_ID != p_not_company_id)
                ) CMY
               WHERE RN = 1
                 AND CM.COMPANY_ID  = CMY.COMPANY_ID
             )
             AND CM.CONTRACT_ID = p_contract_id
          ;
        END IF;
    END IF;
END;

-- ----------------------------------------------------------------- --
-- Изменить целевую таблицу
-- ----------------------------------------------------------------- --
PROCEDURE update_company_t
IS
    v_company_id  BINARY_INTEGER; 
    v_contract_id INTEGER;
    v_count       INTEGER;
BEGIN
   -- устанавливаем блокировку
   latch := true;
   /*
   -- удаляем устаревшие признаки актуальности --
   IF (vt_actual.count > 0) THEN  
      v_company_id := vt_actual.first;
      WHILE v_company_id IS NOT NULL LOOP
          v_contract_id := vt_actual(v_company_id);
          --
          UPDATE COMPANY_T CM
             SET CM.ACTUAL = NULL
           WHERE CM.COMPANY_ID != v_company_id
             AND CM.CONTRACT_ID = v_contract_id
             AND CM.ACTUAL = 'Y'
          ;
          v_company_id := vt_actual.next(v_company_id);
      END LOOP;
   END IF;
   */
   --
   -- проставляем признаки актуальности строкам оставшимся после удаления
   IF (vt_delete.count > 0) THEN  
      v_company_id := vt_delete.first;
      WHILE v_company_id IS NOT NULL LOOP
          v_contract_id := vt_delete(v_company_id);
          --
          set_actual_company( p_contract_id => v_contract_id, 
                              p_not_company_id => v_company_id );

          v_company_id := vt_delete.next(v_company_id);
      END LOOP;
   END IF;
   -- работаем с добавленными или измененными записями
   IF (vt_company.count > 0) THEN  
      v_company_id := vt_company.first;
      WHILE v_company_id IS NOT NULL LOOP
          v_contract_id := vt_company(v_company_id);
          -- проверяем на пересечения интервалов
          SELECT COUNT(*) INTO v_count 
            FROM COMPANY_T CM, COMPANY_T D
           WHERE CM.COMPANY_ID  = v_company_id
             AND CM.CONTRACT_ID = v_contract_id
             AND D.COMPANY_ID  != CM.COMPANY_ID
             AND D.CONTRACT_ID  = CM.CONTRACT_ID
             AND (CM.DATE_FROM BETWEEN D.DATE_FROM AND NVL(D.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
              OR NVL(CM.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy')) BETWEEN D.DATE_FROM AND NVL(D.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
              OR D.DATE_FROM BETWEEN CM.DATE_FROM AND NVL(CM.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
            );
          IF v_count > 0 THEN
              Pk01_Syslog.Write_msg(
                    'Company_id  = '||v_company_id||','||
                    'contract_id = '||v_contract_id||
                    ' - interval intersection'
                    , 'ACCOUNT_company_T_TRG', Pk01_Syslog.L_err );
              RAISE_APPLICATION_ERROR(-20100, 
                    'Company_id  = '||v_company_id||','||
                    'contract_id = '||v_contract_id||
                    ' - interval intersection');
          END IF;
          -- проставляем признак актуальности компании для договора
          set_actual_company( p_contract_id => v_contract_id );
          
          -- переходим к очередной записи
          v_company_id := vt_company.next(v_company_id); 
      END LOOP;
   END IF;  

   -- снимаем блокировку   
   latch := false;
   
EXCEPTION WHEN OTHERS THEN
   latch := false;
   RAISE;
END;

END PK92_COMPANY_TG;
/
