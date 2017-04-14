CREATE OR REPLACE PACKAGE PK03_LOADCTL
IS
    --
    -- Пакет для контроля загрузки трафика в БД
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK03_LOADCTL';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    -- Удалить дубликаты файлов
    -- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    PROCEDURE RemoveDupFiles(p_date_from in date default (sysdate-7));

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- просмотр ошибочных+исправленных загрузок 
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    PROCEDURE ListErrLoad(
         p_recordset out t_refc,
         p_date_from in date default trunc(sysdate-7)
      );
      
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- Список задвоенных файлов
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    PROCEDURE ListDupFiles(
         p_recordset out t_refc,
         p_date_from in date default trunc(sysdate-7)
      );

    -- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    -- Удалить полные дубликаты файлов, принятых от зонового коммутатора КTTK
    -- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    PROCEDURE ZoneKTTK_RemoveDupFiles(p_date_from in date default (sysdate-7));


    -- <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< X T T K >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> --
    -- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    -- Удалить полные дубликаты файлов, принятых от коммутаторов XTTK
    -- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    PROCEDURE xTTK_RemoveDupFiles(p_date_from in date default (sysdate-7));

END PK03_LOADCTL;
/
CREATE OR REPLACE PACKAGE BODY PK03_LOADCTL
IS


-- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
-- Удалить полные дубликаты файлов
-- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
PROCEDURE RemoveDupFiles(p_date_from in date default (sysdate-7))
IS
    v_prcName       constant varchar2(30) := 'RemoveDupFiles';
    v_count         integer;
    v_fcount        integer;
    v_date          date := TRUNC(p_date_from);
BEGIN
    Pk01_SysLog.write_Msg( 'Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_normal);  

    -- очищаем временную таблицу
    COMMIT;
    
    -- заполняем актуальными данными
    INSERT INTO TMP01_DUP_FILES (
        FNUM, FPOS, SW_ID, CDR_FILE_ID, FILENAME, MIN_DATE, MAX_DATE
    )
    SELECT FNUM, FPOS, SW_ID, CDR_FILE_ID, FILE_NAME, MIN_DATE, MAX_DATE
    FROM (
        SELECT 
            COUNT(*)  OVER (PARTITION BY (SUBSTR(FILE_NAME,1,14))) FNUM,
            ROW_NUMBER() OVER (PARTITION BY (SUBSTR(FILE_NAME,1,14)) ORDER BY LOAD_START) FPOS,
            SW_ID, CDR_FILE_ID, FILE_NAME, STATE, LOAD_START, LOAD_STOP, NOTES, RECORDS, MIN_DATE, MAX_DATE
          FROM T01_CDR_FILES T01
         WHERE LOAD_START >= v_date
           AND STATE > 0
    ) 
    WHERE FNUM > 1
    AND   FPOS > 1 
    ORDER BY FILE_NAME, FPOS;
    -- сохраняем отчет
    v_count := SQL%ROWCOUNT;
    
    Pk01_SysLog.write_Msg(
              p_Msg     => 'Found '||v_count||' dup files', 
              p_Src     => c_PkgName||'.'||v_prcName,
              p_Level   => Pk01_Syslog.L_warn
            );
    IF v_count > 0 THEN
      DELETE /*+parallel (t02 10)*/
      FROM T01_CDR_FILES T01
      WHERE EXISTS (
          SELECT * FROM TMP01_DUP_FILES tmp
          WHERE TMP.CDR_FILE_ID = T01.CDR_FILE_ID
      );
      v_count := SQL%ROWCOUNT;
      
      Pk01_SysLog.write_Msg(
                p_Msg     => v_count||' dup files deleted', 
                p_Src     => c_PkgName||'.'||v_prcName,
                p_Level   => Pk01_Syslog.L_warn
              );

      DELETE /*+parallel (t02 10)*/
      FROM T02_FILE_STAT T02
      WHERE EXISTS (
          SELECT * FROM TMP01_DUP_FILES tmp
          WHERE TMP.CDR_FILE_ID = T02.CDR_FILE_ID
      );    
      v_count := SQL%ROWCOUNT;
      
      Pk01_SysLog.write_Msg(
                p_Msg     => v_count||' dup CDR-stat records deleted', 
                p_Src     => c_PkgName||'.'||v_prcName,
                p_Level   => Pk01_Syslog.L_warn
              );
   
      -- удаляем содержимое файлов
      v_fcount := 0;
      FOR l_cur IN ( 
        SELECT SW_ID, FILENAME, CDR_FILE_ID, MIN_DATE, MAX_DATE 
          FROM TMP01_DUP_FILES
      )
      LOOP
        DELETE /*+parallel (t03 10)*/
          FROM T03_MMTS_CDR T03
         WHERE I_ANS_TIME >= l_cur.min_date
           AND I_ANS_TIME <= l_cur.max_date
           AND t03.cdr_file_id = l_cur.cdr_file_id;
        v_count := SQL%ROWCOUNT;
        Pk01_SysLog.write_Msg(
                p_Msg     => l_cur.filename||': '||v_count||' CDR records deleted', 
                p_Src     => c_PkgName||'.'||v_prcName,
                p_Level   => Pk01_Syslog.L_warn
              );
        v_fcount := v_fcount + 1;
      END LOOP;
    END IF;
    
    Pk01_SysLog.write_Msg( 'Stop. '||v_fcount||' files deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_normal);
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error('ERROR', c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- просмотр ошибочных+исправленных загрузок 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
PROCEDURE ListErrLoad(
     p_recordset out t_refc,
     p_date_from in date default trunc(sysdate-7)
  )
IS
    v_prcName   constant varchar2(30) := 'ListErrLoad';
    v_date      date := TRUNC(p_date_from);
BEGIN
    OPEN p_recordset FOR
      SELECT t01.SW_ID, t01.FILE_NAME, t01.LOAD_START, t01.LOAD_STOP, 
             t01.STATE, t01.NOTES, t01.RECORDS, t01.MIN_DATE, t01.MAX_DATE 
      FROM T01_CDR_FILES t01, (
         SELECT (substr(FILE_NAME,1,14)) FILE_NAME
           FROM T01_CDR_FILES
          WHERE STATE != 1
            AND LOAD_START > v_date
      ) t01e
      WHERE T01.LOAD_START > v_date
        AND T01.FILE_NAME LIKE t01e.FILE_NAME||'%'
      ORDER BY t01.FILE_NAME
      ;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;
  
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Список задвоенных файлов
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
PROCEDURE ListDupFiles(
     p_recordset out t_refc,
     p_date_from in date default trunc(sysdate-7)
  )
IS
    v_prcName   constant varchar2(30) := 'ListDupFiles';
    v_date      date := TRUNC(p_date_from);
BEGIN
    OPEN p_recordset FOR
      SELECT * FROM (
          SELECT 
              COUNT(*)  OVER (PARTITION BY (SUBSTR(T01.FILE_NAME,1,14))) FNUM,
              ROW_NUMBER() OVER (PARTITION BY (SUBSTR(T01.FILE_NAME,1,14)) ORDER BY T01.LOAD_START) FPOS,
              FILE_NAME, STATE, LOAD_START, LOAD_STOP, NOTES, RECORDS, MIN_DATE, MAX_DATE
            FROM T01_CDR_FILES t01
           WHERE T01.LOAD_START > v_date
             AND T01.STATE > 0
      ) WHERE FNUM > 1
      ORDER BY FILE_NAME, FPOS
      ;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
-- Удалить полные дубликаты файлов, принятых от зонового коммутатора КTTK
-- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
PROCEDURE ZoneKTTK_RemoveDupFiles(p_date_from in date default (sysdate-7))
IS
    v_prcName       constant varchar2(30) := 'ZoneKTTK_RemoveDupFiles';
    v_count         integer;
    v_fcount        integer;
    v_date          date := TRUNC(p_date_from);
BEGIN
    Pk01_Syslog.write_Msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);  

    COMMIT;
    
    -- заполняем актуальными данными
    INSERT INTO TMP01_DUP_FILES (
        FNUM, FPOS, SW_ID, CDR_FILE_ID, FILENAME, MIN_DATE, MAX_DATE
    )
    SELECT FNUM, FPOS, SW_ID, CDR_FILE_ID, FILE_NAME, MIN_DATE, MAX_DATE
    FROM (
        SELECT 
            COUNT(*)  OVER (PARTITION BY FILE_NAME) FNUM,
            ROW_NUMBER() OVER (PARTITION BY FILE_NAME ORDER BY LOAD_START) FPOS,
            SW_ID, CDR_FILE_ID, FILE_NAME, STATE, LOAD_START, LOAD_STOP, NOTES, RECORDS, MIN_DATE, MAX_DATE
          FROM Z01_CDR_FILES Z01
         WHERE Z01.LOAD_START >= v_date
           AND Z01.STATE > 0
    ) 
    WHERE FNUM > 1
    AND   FPOS > 1 
    ORDER BY FILE_NAME, FPOS;

    v_count := SQL%ROWCOUNT;
    Pk01_SysLog.write_Msg(
              p_Msg     => 'Found '||v_count||' dup files', 
              p_Src     => c_PkgName||'.'||v_prcName,
              p_Level   => Pk01_Syslog.L_warn
            );
    
    IF v_count > 0 THEN
      DELETE /*+parallel (z01 10)*/
        FROM Z01_CDR_FILES Z01
       WHERE EXISTS (
          SELECT * 
            FROM TMP01_DUP_FILES tmp
           WHERE TMP.CDR_FILE_ID = Z01.CDR_FILE_ID
      );
      v_count := SQL%ROWCOUNT;
      Pk01_SysLog.write_Msg(
                p_Msg     => v_count||' dup files deleted', 
                p_Src     => c_PkgName||'.'||v_prcName,
                p_Level   => Pk01_Syslog.L_warn
              );

      DELETE /*+parallel (Z02 10)*/
      FROM Z02_FILE_STAT Z02
      WHERE EXISTS (
          SELECT * FROM TMP01_DUP_FILES tmp
          WHERE TMP.CDR_FILE_ID = Z02.CDR_FILE_ID
      );    
      v_count := SQL%ROWCOUNT;
      
      Pk01_SysLog.write_Msg(
                p_Msg     => v_count||' dup CDR-stat records deleted', 
                p_Src     => c_PkgName||'.'||v_prcName,
                p_Level   => Pk01_Syslog.L_warn
              );
   
      v_fcount := 0;
      FOR l_cur IN ( 
        SELECT SW_ID, FILENAME, CDR_FILE_ID, MIN_DATE, MAX_DATE 
          FROM TMP01_DUP_FILES
      )
      LOOP
        DELETE /*+parallel (z03 10)*/
          FROM Z03_ZONE_CDR Z03
         WHERE Z03.ANS_TIME >= l_cur.min_date
           AND Z03.ANS_TIME <= l_cur.max_date
           AND Z03.CDR_FILE_ID = l_cur.cdr_file_id;
        v_count := SQL%ROWCOUNT;
        Pk01_SysLog.write_Msg(
                p_Msg     => l_cur.filename||': '||v_count||' CDR records deleted', 
                p_Src     => c_PkgName||'.'||v_prcName,
                p_Level   => Pk01_Syslog.L_warn
              );
      
        v_fcount := v_fcount + 1;
      END LOOP;
              
    END IF;
    COMMIT;
    Pk01_SysLog.write_Msg( 'Stop. '||v_fcount||' files deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_normal);

    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error('ERROR', c_PkgName||'.'||v_prcName);
END;

-- <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< X T T K >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> --
-- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
-- Удалить полные дубликаты файлов, принятых от коммутаторов XTTK
-- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
PROCEDURE xTTK_RemoveDupFiles(p_date_from in date default (sysdate-7))
IS
    v_prcName       constant varchar2(30) := 'xTTK_RemoveDupFiles';
    v_count         integer;
    v_fcount        integer;
    v_date          date := TRUNC(p_date_from);
BEGIN
    Pk01_Syslog.write_Msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info);  

    COMMIT;
    
    -- заполняем актуальными данными
    INSERT INTO TMP01_DUP_FILES (
        FNUM, FPOS, SW_ID, CDR_FILE_ID, FILENAME, MIN_DATE, MAX_DATE
    )
    SELECT FNUM, FPOS, SW_ID, CDR_FILE_ID, FILE_NAME, MIN_DATE, MAX_DATE
    FROM (
        SELECT 
            COUNT(*)  OVER (PARTITION BY FILE_NAME) FNUM,
            ROW_NUMBER() OVER (PARTITION BY FILE_NAME ORDER BY LOAD_START) FPOS,
            SW_ID, CDR_FILE_ID, FILE_NAME, STATE, LOAD_START, LOAD_STOP, NOTES, RECORDS, MIN_DATE, MAX_DATE
          FROM X01_CDR_FILES X01
         WHERE X01.LOAD_START >= v_date
           AND X01.STATE > 0
    ) 
    WHERE FNUM > 1
    AND   FPOS > 1 
    ORDER BY FILE_NAME, FPOS;

    v_count := SQL%ROWCOUNT;
    Pk01_SysLog.write_Msg(
              p_Msg     => 'Found '||v_count||' dup files', 
              p_Src     => c_PkgName||'.'||v_prcName,
              p_Level   => Pk01_Syslog.L_warn
            );
    
    IF v_count > 0 THEN
      DELETE /*+parallel (x01 10)*/
        FROM X01_CDR_FILES X01
       WHERE EXISTS (
          SELECT * 
            FROM TMP01_DUP_FILES tmp
           WHERE TMP.CDR_FILE_ID = X01.CDR_FILE_ID
      );
      v_count := SQL%ROWCOUNT;
      Pk01_SysLog.write_Msg(
                p_Msg     => v_count||' dup files deleted', 
                p_Src     => c_PkgName||'.'||v_prcName,
                p_Level   => Pk01_Syslog.L_warn
              );

      DELETE /*+parallel (x02 10)*/
      FROM X02_FILE_STAT X02
      WHERE EXISTS (
          SELECT * FROM TMP01_DUP_FILES tmp
          WHERE TMP.CDR_FILE_ID = X02.CDR_FILE_ID
      );    
      v_count := SQL%ROWCOUNT;
      
      Pk01_SysLog.write_Msg(
                p_Msg     => v_count||' dup CDR-stat records deleted', 
                p_Src     => c_PkgName||'.'||v_prcName,
                p_Level   => Pk01_Syslog.L_warn
              );
   
      v_fcount := 0;
      FOR l_cur IN ( 
        SELECT SW_ID, FILENAME, CDR_FILE_ID, MIN_DATE, MAX_DATE 
          FROM TMP01_DUP_FILES
      )
      LOOP
        DELETE /*+parallel (x03 10)*/
          FROM X03_XTTK_CDR X03
         WHERE X03.ANS_TIME >= l_cur.min_date
           AND X03.ANS_TIME <= l_cur.max_date
           AND X03.CDR_FILE_ID = l_cur.cdr_file_id;
        v_count := SQL%ROWCOUNT;
        Pk01_SysLog.write_Msg(
                p_Msg     => l_cur.filename||': '||v_count||' CDR records deleted', 
                p_Src     => c_PkgName||'.'||v_prcName,
                p_Level   => Pk01_Syslog.L_warn
              );
      
        v_fcount := v_fcount + 1;
      END LOOP;
              
    END IF;
    COMMIT;
    Pk01_SysLog.write_Msg( 'Stop. '||v_fcount||' files deleted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_normal);

    
EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error('ERROR', c_PkgName||'.'||v_prcName);
END;


END PK03_LOADCTL;
/
