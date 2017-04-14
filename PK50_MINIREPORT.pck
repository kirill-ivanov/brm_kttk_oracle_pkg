CREATE OR REPLACE PACKAGE PK50_MINIREPORT IS
  
  type t_refc is ref cursor;
 
  c_PkgName     constant varchar2(30) := 'PK50_MINIREPORT';

  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
  -- Получение выборки для графиков graphMakerMMTS
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
  procedure LOAD_DATA_FOR_GRAPH_MMTS( 
          p_message     out VARCHAR2, 
          p_recordset     out t_refc,
          p_sw_id         INTEGER,
          p_date_from     DATE,
          p_date_to       DATE,
          p_is_only_error NUMBER
  );
  
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
  -- проверка на разрывы в загрузке последовательностей файлов
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
  PROCEDURE FIND_SQ_DROP (
          p_recordset     OUT t_refc,
          p_sw_id         INTEGER,
          p_date_from     DATE,
          p_date_to       DATE,
          p_trf_type      VARCHAR2 DEFAULT 'DET' -- 'FLR'
    );
                
END PK50_MINIREPORT;
/
create or replace package body PK50_MINIREPORT is

--======================================================================
-- Получение выборки для графиков graphMakerMMTS
procedure LOAD_DATA_FOR_GRAPH_MMTS( 
          p_message       OUT VARCHAR2, 
          p_recordset     OUT t_refc,
          p_sw_id         INTEGER,
          p_date_from     DATE,
          p_date_to       DATE,
          p_is_only_error NUMBER
)
is
    v_prcName   constant varchar2(30) := 'LOAD_DATA_FOR_GRAPH_MMTS';
    v_date_from DATE;
    v_date_to   DATE;
begin
    v_date_from:= TO_DATE(TO_CHAR(p_date_from,'DD.MM.YYYY')||' 00:00:00','DD.MM.YYYY HH24:MI:SS');
    v_date_to:= TO_DATE(TO_CHAR(p_date_to,'DD.MM.YYYY')||' 23:59:59','DD.MM.YYYY HH24:MI:SS');
    
    open p_recordset for        
         SELECT 
             ROWNO,
             SW_ID,             
             SW_NAME,
             FILE_NO,
             FILE_NO_PREV,
             CASE WHEN ROWNO = 1 THEN 1 ELSE FILE_NO - FILE_NO_PREV END FILE_NO_DIFF,
             FILE_NAME,
             FILE_NAME_AUD,
             FILE_DATE,
             LOAD_START+1/6 LOAD_START,
             RECORDS, 
             MIN_DATE,
             MAX_DATE,
             END_DATE               
      FROM (
          SELECT ROW_NUMBER () OVER (PARTITION BY SW_NAME ORDER BY END_DATE) ROWNO,
                 SW_ID,                 
                 SUBSTR(FILE_NAME, 1,4) SW_NAME,  
                 SUBSTR(FILE_NAME, 11,4) FILE_NO,
                 LAG(SUBSTR(FILE_NAME, 11,4), 1, 0) OVER (ORDER BY SW_ID, END_DATE) FILE_NO_PREV,
                 FILE_NAME,
                 SUBSTR(FILE_NAME, 1, 14)||'.DET' FILE_NAME_AUD, 
                 TO_DATE(SUBSTR(FILE_NAME, 5, 6), 'yymmdd') FILE_DATE, 
                 LOAD_START,
                 RECORDS, 
                 MIN_DATE,
                 MAX_DATE,
                 END_DATE           
            FROM T01_CDR_FILES T01
           WHERE STATE > 0
             AND (SW_ID = p_sw_id OR p_sw_id IS NULL)              
             --AND LOAD_START BETWEEN v_date_from - 1/6 AND v_date_to - 1/6
             AND END_DATE BETWEEN v_date_from-1/6 AND v_date_to - 1/6 
             AND T01.FILE_NAME NOT LIKE '%.FLR'                    
      ) a
      WHERE
          FILE_NO - FILE_NO_PREV <> 1 OR p_is_only_error IS NULL
      ORDER BY SW_ID, END_DATE;      
      COMMIT;
      --p_message := 'OK';
exception
    when others then
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        if p_recordset%ISOPEN then 
            close p_recordset;
        end if;
end;     

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- проверка на разрывы в загрузке последовательностей файлов
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE FIND_SQ_DROP (
        p_recordset     OUT t_refc,
        p_sw_id         INTEGER,
        p_date_from     DATE,
        p_date_to       DATE,
        p_trf_type      VARCHAR2 DEFAULT 'DET' -- 'FLR'
  )
IS
    v_prcName   CONSTANT VARCHAR2(30) := 'FIND_SQ_DROP';
    v_date_from INTEGER;
    v_date_to   INTEGER;
BEGIN
    v_date_from:= TO_CHAR(p_date_from, 'yyyymmdd');
    v_date_to  := TO_CHAR(p_date_to,   'yyyymmdd');

    OPEN p_recordset FOR
      SELECT *
        FROM ( 
            SELECT FILE_NAME, FN,
                   lEAD(FN) OVER (PARTITION BY FD ORDER BY FN) LF,
                   lEAD(FILE_NAME) OVER (PARTITION BY FD ORDER BY FN) LFNAME,
                   FD 
              FROM (
                    SELECT FILE_NAME, SUBSTR(FILE_NAME,11,4) FN, SUBSTR(FILE_NAME, 5, 6) FD
                      FROM T01_CDR_FILES
                     WHERE SW_ID = p_sw_id    
                       AND SUBSTR(FILE_NAME, 5, 6) BETWEEN v_date_from AND v_date_to 
                       AND SUBSTR(FILE_NAME, -3) = p_trf_type
                       AND STATE > 0
                    )
            )
       WHERE LF-FN != 1
       ORDER BY FILE_NAME
     ;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

end PK50_MINIREPORT;
/
