CREATE OR REPLACE PACKAGE PK91_ACCOUNT_PROFILE_TG
IS
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK91_ACCOUNT_PROFILE_TG';
    -- ==============================================================================
    -- ключ - profile_id, в таблице account_id
    TYPE tbl_integer IS TABLE OF INTEGER INDEX BY BINARY_INTEGER;

    -- Pl/sql- таблица для хранения координат изменившихся строк
    vt_profile tbl_integer;
    vt_actual  tbl_integer;   
    vt_delete  tbl_integer;

    -- Защелка для предупреждения повторного вызова триггеров.
    latch BOOLEAN := false;

    -- Удаление всех записей из pl/sql-таблицы перед началом обновления.
    PROCEDURE clear_tbls;

    -- Добавить запись о профиле в таблицу
    PROCEDURE add_profile ( 
                  p_profile_id IN BINARY_INTEGER,
                  p_account_id IN INTEGER
              );
              
    -- Добавить запись об актуальном профиле в таблицу
    PROCEDURE add_actual ( 
                  p_profile_id IN BINARY_INTEGER,
                  p_account_id IN INTEGER
              );

    -- Добавить запись об удаленной компании в таблицу
    PROCEDURE del_profile ( 
                  p_profile_id IN BINARY_INTEGER,
                  p_account_id IN INTEGER
              );

    -- Проверка на пересечение интервалов дат
    PROCEDURE update_profile_t;
 
END PK91_ACCOUNT_PROFILE_TG;
/
CREATE OR REPLACE PACKAGE BODY PK91_ACCOUNT_PROFILE_TG
IS

-- ----------------------------------------------------------------- --
-- Удаление всех записей из pl/sql-таблицы перед началом обновления.
-- ----------------------------------------------------------------- --
PROCEDURE clear_tbls
IS
BEGIN
    vt_profile.delete();
    vt_actual.delete();
    vt_delete.delete();
END;

-- ----------------------------------------------------------------- --
-- Добавить запись о профиле в таблицу
-- ----------------------------------------------------------------- --
PROCEDURE add_profile ( 
              p_profile_id IN BINARY_INTEGER,
              p_account_id IN INTEGER -- NULL - для записей у которых ACTUAL IS NULL
          ) 
IS
BEGIN
   vt_profile(p_profile_id) := p_account_id;
END;

-- ----------------------------------------------------------------- --
-- Добавить запись об актуальном профиле в таблицу
-- ----------------------------------------------------------------- --
PROCEDURE add_actual ( 
              p_profile_id IN BINARY_INTEGER,
              p_account_id IN INTEGER -- NULL - для записей у которых ACTUAL IS NULL
          ) 
IS
BEGIN
   vt_actual(p_profile_id) := p_account_id;
END;

-- ----------------------------------------------------------------- --
-- Добавить запись об удаленной компании в таблицу
-- ----------------------------------------------------------------- --
PROCEDURE del_profile ( 
              p_profile_id IN BINARY_INTEGER,
              p_account_id IN INTEGER
          )
IS
BEGIN
   vt_delete(p_profile_id) := p_account_id;
END;

-- ----------------------------------------------------------------- --
-- Проставляем признак актуальной компании на договоре
-- ----------------------------------------------------------------- --
PROCEDURE set_actual_profile( 
              p_account_id     IN INTEGER,
              p_not_profile_id IN BINARY_INTEGER DEFAULT NULL -- кроме указанного профиля
          )
IS
    v_count INTEGER;
BEGIN
    -- находим актуальную комапнию на текущий момент
    SELECT COUNT(*) INTO v_count
      FROM ACCOUNT_PROFILE_T AP
     WHERE SYSDATE BETWEEN AP.DATE_FROM AND NVL(AP.DATE_TO, SYSDATE)
       AND AP.ACCOUNT_ID = p_account_id
       AND AP.ACTUAL   = 'Y'
       AND (p_not_profile_id IS NULL OR AP.PROFILE_ID != p_not_profile_id)
    ;
    IF v_count = 0 THEN
        -- удаляем старые признаки
        UPDATE ACCOUNT_PROFILE_T AP SET AP.ACTUAL = NULL
         WHERE AP.ACCOUNT_ID = p_account_id;
        -- проставляем признак у профиля актуального на текущий момент
        UPDATE ACCOUNT_PROFILE_T AP SET AP.ACTUAL = 'Y'
         WHERE SYSDATE BETWEEN AP.DATE_FROM AND NVL(AP.DATE_TO, SYSDATE)
           AND AP.ACCOUNT_ID = p_account_id
           AND (p_not_profile_id IS NULL OR AP.PROFILE_ID != p_not_profile_id);
        IF SQL%ROWCOUNT = 0 THEN
          -- проставляем признак у последнего бывшего актуального профиля
          UPDATE ACCOUNT_PROFILE_T AP SET AP.ACTUAL = 'Y'
           WHERE EXISTS (
              SELECT *
                FROM
                (
                  SELECT ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY NVL(DATE_TO, SYSDATE) DESC) RN, 
                         PROFILE_ID 
                    FROM ACCOUNT_PROFILE_T 
                   WHERE ACCOUNT_ID = p_account_id
                     AND (p_not_profile_id IS NULL OR PROFILE_ID != p_not_profile_id)
                ) APY
               WHERE RN = 1
                 AND AP.PROFILE_ID  = APY.PROFILE_ID
             )
             AND AP.ACCOUNT_ID = p_account_id
          ;
        END IF;
    END IF;
END;

-- ----------------------------------------------------------------- --
-- Изменить целевую таблицу
-- ----------------------------------------------------------------- --
PROCEDURE update_profile_t
IS
    v_profile_id BINARY_INTEGER; 
    v_account_id INTEGER;
    v_count      INTEGER;
BEGIN                        
   latch := true;
   /*
   -- удаляем устаревшие признаки актуальности --
   IF (vt_actual.count > 0) THEN  
      v_profile_id := vt_actual.first;
      WHILE v_profile_id IS NOT NULL LOOP
          v_account_id := vt_actual(v_profile_id);
          --
          UPDATE ACCOUNT_PROFILE_T AP
             SET AP.ACTUAL = NULL
           WHERE AP.PROFILE_ID != v_profile_id
             AND AP.ACCOUNT_ID  = v_account_id
             AND AP.ACTUAL = 'Y'
          ;
          v_profile_id := vt_actual.next(v_profile_id);
      END LOOP;
   END IF;
   */
   -- проставляем признаки актуальности строкам оставшимся после удаления
   IF (vt_delete.count > 0) THEN  
      v_profile_id := vt_delete.first;
      WHILE v_profile_id IS NOT NULL LOOP
          v_account_id := vt_delete(v_profile_id);
          --
          set_actual_profile( 
              p_account_id     => v_account_id,
              p_not_profile_id => v_profile_id -- кроме указанного профиля
          );
          /*
          -- пытаемся найти текущую запись
          UPDATE ACCOUNT_PROFILE_T AP SET AP.ACTUAL = 'Y'
           WHERE SYSDATE BETWEEN AP.DATE_FROM AND NVL(AP.DATE_TO, SYSDATE);
          IF SQL%ROWCOUNT = 0 THEN
            -- обновляем последнюю закрытую запись
            UPDATE ACCOUNT_PROFILE_T AP SET AP.ACTUAL = 'Y'
             WHERE EXISTS (
                SELECT *
                  FROM
                  (
                    SELECT ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY NVL(DATE_TO, SYSDATE) DESC) RN, 
                           PROFILE_ID 
                      FROM ACCOUNT_PROFILE_T 
                     WHERE ACCOUNT_ID = v_account_id
                       AND PROFILE_ID!= v_profile_id
                  ) APY
                 WHERE RN = 1
                   AND AP.PROFILE_ID  = APY.PROFILE_ID
               )
               AND AP.ACCOUNT_ID = v_account_id
            ;
          END IF;
          */
          v_profile_id := vt_delete.next(v_profile_id);
      END LOOP;  
   END IF;
   
   -- проверяем на пересечения интервалов
   IF (vt_profile.count > 0) THEN  
      v_profile_id := vt_profile.first;
      WHILE v_profile_id IS NOT NULL LOOP
          v_account_id := vt_profile(v_profile_id);
          --
          SELECT COUNT(*) INTO v_count 
            FROM ACCOUNT_PROFILE_T P, 
                 ACCOUNT_PROFILE_T D
           WHERE P.ACCOUNT_ID = v_account_id
             AND P.PROFILE_ID = v_profile_id
             AND D.ACCOUNT_ID = P.ACCOUNT_ID
             AND D.PROFILE_ID!= P.PROFILE_ID
             AND (P.DATE_FROM BETWEEN D.DATE_FROM AND NVL(D.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
              OR NVL(P.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy')) BETWEEN D.DATE_FROM AND NVL(D.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
              OR D.DATE_FROM BETWEEN P.DATE_FROM AND NVL(P.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))
            );
          IF v_count > 0 THEN
              Pk01_Syslog.Write_msg(
                    'Account_id = '||v_account_id||','||
                    'profile_id = '||v_profile_id||
                    ' - interval intersection'
                    , 'ACCOUNT_PROFILE_T_TRG', Pk01_Syslog.L_err );
              RAISE_APPLICATION_ERROR(-20100, 
                    'Account_id = '||v_account_id||','||
                    'profile_id = '||v_profile_id||
                    ' - interval intersection');
          END IF;
          -- проставляем актуальный профиль при добавлении или изменении записи
          set_actual_profile( 
              p_account_id     => v_account_id
          );
          -- переходим к очередной записи
          v_profile_id := vt_profile.next(v_profile_id); 
      END LOOP;
   END IF;  
   
   latch := false;
EXCEPTION WHEN OTHERS THEN
   latch := false;
   RAISE;
END;

END PK91_ACCOUNT_PROFILE_TG;
/
