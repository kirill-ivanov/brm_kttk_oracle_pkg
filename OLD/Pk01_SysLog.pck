create or replace package Pk01_SysLog is

  -- Author  : SMAKEEV
  -- Created : 11.11.2011 10:55:14
  -- Purpose : Система логирования
  
  --=========================================================================================
  -- Система логирования работает за рамками текущего сеанса,
  -- поэтому операции WRITE сохраняют данные, 
  -- независимо от того было подтверждение в вызывающей транзакции или нет.
  --=========================================================================================
  --
  -- Уровни сообщений об ошибках
  L_err      constant varchar2(1) :=  'E'; -- Другие состояния ошибок
  L_normal   constant varchar2(1) :=  'M'; -- Нормальное функционирование
  L_warn     constant varchar2(1) :=  'W'; -- Предупреждающее сообщение
  L_info     constant varchar2(1) :=  'I'; -- Информационные сообщения
  L_debug    constant varchar2(1) :=  'D'; -- Отладочные сообщения
  --
  n_CONSOLE  constant varchar2(3) := 'CON';
  n_BLANK    constant varchar2(3) := ' ';
  --
  n_APP_EXCEPTION constant number := -20100;  -- номер исключения в приложения пользователя
  --
  -- Глобальные переменные
  --
  n_L01_SSID   integer;        -- идентификатор сеанса связи с ORACLE
  n_L01_USER   varchar2(30);   -- имя пользователя
  n_L01_UID    integer;        -- идентификатор пользователя
  g_DB_USER    L01_MESSAGES.DB_USER%TYPE; 
  g_IP_ADDRESS L01_MESSAGES.IP_ADDRESS%TYPE; 
  g_HOST_NAME  L01_MESSAGES.HOST_NAME%TYPE; 
  g_OS_USER    L01_MESSAGES.OS_USER%TYPE; 
  g_MODULE     L01_MESSAGES.PROGRAM%TYPE;
  --
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- С л у же б н ы е
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- возвращает номер сеанса из последовательности
  FUNCTION get_SSID RETURN INTEGER;

  -- Получить текст ошибки ORACLE
  FUNCTION get_OraErrTxt(p_Txt in varchar2 default n_BLANK) RETURN VARCHAR2;

  -- Получить разницу во времени в секундах
  FUNCTION get_TimeDiff(p_tsfrom in timestamp, p_tsto in timestamp) RETURN INTEGER DETERMINISTIC;

  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- П р о ц е д у р ы
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  --
  -- Поднять иключение ORACLE с привязкой к источнику (пакет+процедура)
  PROCEDURE Raise_exception(
              p_Msg IN VARCHAR2,
              p_Src IN VARCHAR2 DEFAULT n_CONSOLE
            );

  -- Запись сообщения в обработчике ошибок текущего модуля
  PROCEDURE Write_error(  
              p_Msg IN VARCHAR2 DEFAULT n_BLANK, 
              p_Src IN VARCHAR2 DEFAULT n_CONSOLE
            );
  --
  PROCEDURE Insert_Error(  
              p_Msg IN VARCHAR2 DEFAULT n_BLANK, 
              p_Src IN VARCHAR2 DEFAULT n_CONSOLE
            );

  -- Записать сообщение в Log-File, если описатель задачи не указан вывод на консоль
  PROCEDURE Write_msg(
              p_Msg    IN VARCHAR2,                   -- сообщение
              p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- источник (пакет+функция)
              p_Level  IN VARCHAR2 DEFAULT L_info,    -- уровень сообщения
              p_AppUsr IN VARCHAR2 DEFAULT NULL       -- пользователь приложения 
            );

  -- Записать сообщение в открытый Log-File
  PROCEDURE Insert_msg(
              p_Msg    IN VARCHAR2,                   -- сообщение
              p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- источник (пакет+функция)
              p_Level  IN VARCHAR2 DEFAULT L_info,    -- уровень сообщения
              p_AppUsr IN VARCHAR2 DEFAULT NULL       -- пользователь приложения 
            );

  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Ф у н к ц и и (возвращают L01_ID)
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Записать сообщение в Log-File, если описатель задачи не указан вывод на консоль
  -- без COMMIT
  FUNCTION Fn_insert_msg(
              p_Msg    IN VARCHAR2,                   -- сообщение
              p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- источник (пакет+функция)
              p_Level  IN VARCHAR2 DEFAULT L_info,    -- уровень сообщения
              p_AppUsr IN VARCHAR2 DEFAULT NULL       -- пользователь приложения
            ) RETURN INTEGER;

  -- Записать сообщение в Log-File, если описатель задачи не указан вывод на консоль
  FUNCTION Fn_write_msg(
              p_Msg    IN VARCHAR2,                   -- сообщение
              p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- источник (пакет+функция)
              p_Level  IN VARCHAR2 DEFAULT L_info,    -- уровень сообщения
              p_AppUsr IN VARCHAR2 DEFAULT NULL       -- пользователь приложения 
            ) RETURN INTEGER;

  -- Запись сообщения в обработчике ошибок текущего модуля
  FUNCTION Fn_insert_error(
              p_Msg  IN VARCHAR2 DEFAULT n_BLANK, 
              p_Src  IN VARCHAR2 DEFAULT n_CONSOLE
            ) RETURN INTEGER;

  -- Запись сообщения в обработчике ошибок текущего модуля
  FUNCTION Fn_write_error(
              p_Msg  IN VARCHAR2 DEFAULT n_BLANK, 
              p_Src  IN VARCHAR2 DEFAULT n_CONSOLE
            ) RETURN INTEGER;
  
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Дополнительные
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Прочитать сообщение из Log-File
  PROCEDURE Read_msg(
              p_Level     OUT VARCHAR2,
              p_Date      OUT DATE,
              p_Msg       OUT VARCHAR2,
              p_msg_id    IN INTEGER
            );
                          
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Удалить из лог-файла записи старше p_days
  PROCEDURE Clean_log(p_days IN INTEGER);


end Pk01_SysLog;
/
create or replace package body Pk01_SysLog is

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- С л у же б н ы е
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- возвращает номер сеанса из последовательности
FUNCTION get_SSID RETURN INTEGER IS
BEGIN
  RETURN n_L01_SSID;
END;
--
-- Получить текст ошибки ORACLE
--
FUNCTION get_OraErrTxt(p_Txt in varchar2 default n_BLANK) RETURN VARCHAR2
IS
    v_ErrorText  VARCHAR2(2000);    -- текст сообщения об ошибке
BEGIN
    v_ErrorText  := SUBSTRB(p_Txt||'=>'||SQLERRM, 1, 2000);  -- текст сообщения об ошибке
    RETURN v_ErrorText;
END;

--
-- Получить разницу во времени в секундах
--
FUNCTION get_TimeDiff(p_tsfrom in timestamp, p_tsto in timestamp) RETURN INTEGER DETERMINISTIC
IS
    v_diff interval day to second := p_tsto - p_tsfrom;
    v_secs number;
BEGIN
    v_secs := extract(day from v_diff) * 86400
        + extract(hour from v_diff) * 3600
        + extract(minute from v_diff) * 60
        + extract(second from v_diff);
    RETURN round(v_secs);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Ф у н к ц и и (возвращают L01_ID)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Записать сообщение в Log-File, если описатель задачи не указан вывод на консоль
-- без COMMIT
FUNCTION Fn_insert_msg(
                    p_Msg    IN VARCHAR2,                   -- сообщение
                    p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- источник (пакет+функция)
                    p_Level  IN VARCHAR2 DEFAULT L_info,    -- уровень сообщения
                    p_AppUsr IN VARCHAR2 DEFAULT NULL       -- пользователь приложения
                ) RETURN INTEGER 
IS
    v_l01_id  INTEGER;
BEGIN
    INSERT INTO L01_MESSAGES m (
        m.L01_ID, m.SSID, m.USER_ID,
        m.MSG_LEVEL,m.MSG_DATE, m.MSG_SRC, m.MESSAGE,
        m.DB_USER, m.OS_USER, m.IP_ADDRESS, m.HOST_NAME, 
        m.PROGRAM, m.APP_USER
    )VALUES(
        SQ_L01_ID.NEXTVAL, n_L01_SSID, n_L01_UID, 
        p_Level, LOCALTIMESTAMP, p_Src, SUBSTR(p_Msg,1,2000),
        g_DB_USER, g_OS_USER, g_IP_ADDRESS, g_HOST_NAME,  
        g_MODULE, p_AppUsr
    ) RETURNING L01_ID INTO v_l01_id;
    RETURN v_l01_id;
END;

-- Записать сообщение в Log-File, если описатель задачи не указан вывод на консоль
FUNCTION Fn_write_msg(
                    p_Msg    IN VARCHAR2,                   -- сообщение
                    p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- источник (пакет+функция)
                    p_Level  IN VARCHAR2 DEFAULT L_info,    -- уровень сообщения
                    p_AppUsr IN VARCHAR2 DEFAULT NULL       -- пользователь приложения 
                ) RETURN INTEGER 
IS
    v_l01_id  INTEGER;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    v_l01_id := Fn_insert_msg(p_Msg, p_Src, p_Level, p_AppUsr);
    COMMIT;
    return v_l01_id;
END;

-- Запись сообщения в обработчике ошибок текущего модуля
FUNCTION Fn_insert_error(
                      p_Msg  IN VARCHAR2 DEFAULT n_BLANK, 
                      p_Src  IN VARCHAR2 DEFAULT n_CONSOLE
                    ) RETURN INTEGER 
IS
BEGIN
    RETURN Fn_insert_msg(Get_OraErrTxt(p_Msg), p_Src, L_err);
END;

-- Запись сообщения в обработчике ошибок текущего модуля
FUNCTION Fn_write_error(
                      p_Msg  IN VARCHAR2 DEFAULT n_BLANK, 
                      p_Src  IN VARCHAR2 DEFAULT n_CONSOLE
                    ) RETURN INTEGER 
IS
BEGIN
    RETURN Fn_write_msg(Get_OraErrTxt(p_Msg), p_Src, L_err);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- П р о ц е д у р ы
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- Поднять иключение ORACLE с привязкой к источнику (пакет+процедура)
--
PROCEDURE Raise_exception(
            p_Msg IN VARCHAR2,
            p_Src IN VARCHAR2 DEFAULT n_CONSOLE
          )
IS
    v_ErrorCode  NUMBER;           -- код сообщения об ошибке
    v_msg_id     INTEGER;
BEGIN
    v_ErrorCode := SQLCODE;
    v_msg_id := Fn_write_error(p_Msg, p_Src);
    RAISE_APPLICATION_ERROR(n_APP_EXCEPTION, 'msg_id='||v_msg_id||':'||p_Src);
END;

--
-- Запись сообщения в обработчике ошибок текущего модуля
--
PROCEDURE Write_error(  
            p_Msg IN VARCHAR2 DEFAULT n_BLANK, 
            p_Src IN VARCHAR2 DEFAULT n_CONSOLE
          ) 
IS
    v_l01_id INTEGER;
BEGIN
    v_l01_id := Fn_write_msg(Get_OraErrTxt(p_Msg), p_Src, L_err);
END;
--
--
PROCEDURE Insert_Error(  
            p_Msg IN VARCHAR2 DEFAULT n_BLANK, 
            p_Src IN VARCHAR2 DEFAULT n_CONSOLE
          ) 
IS
    v_l01_id INTEGER;
BEGIN
    v_l01_id := Fn_insert_msg(Get_OraErrTxt(p_Msg), p_Src, L_err);
END;

--
-- Записать сообщение в Log-File, если описатель задачи не указан вывод на консоль
--
PROCEDURE Write_msg(
                    p_Msg    IN VARCHAR2,                   -- сообщение
                    p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- источник (пакет+функция)
                    p_Level  IN VARCHAR2 DEFAULT L_info,    -- уровень сообщения
                    p_AppUsr IN VARCHAR2 DEFAULT NULL       -- пользователь приложения 
                ) 
IS
    v_l01_id  INTEGER;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    v_l01_id := Fn_insert_msg(p_Msg, p_Src, p_Level, p_AppUsr);
    COMMIT;
END;

--
-- Записать сообщение в открытый Log-File
--
PROCEDURE Insert_msg(
                    p_Msg    IN VARCHAR2,                   -- сообщение
                    p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- источник (пакет+функция)
                    p_Level  IN VARCHAR2 DEFAULT L_info,    -- уровень сообщения
                    p_AppUsr IN VARCHAR2 DEFAULT NULL       -- пользователь приложения 
                ) 
IS
    v_l01_id  INTEGER;
BEGIN
    v_l01_id := Fn_insert_msg(p_Msg, p_Src, p_Level, p_AppUsr);
END;

--
-- Прочитать сообщение из Log-File
--
PROCEDURE Read_msg(
            p_Level     OUT VARCHAR2,
            p_Date      OUT DATE,
            p_Msg       OUT VARCHAR2,
            p_msg_id    IN INTEGER
          ) 
IS
BEGIN
    SELECT m.msg_level, m.msg_date, m.message
      INTO p_Level, p_Date, p_Msg
      FROM L01_MESSAGES m
     WHERE m.L01_ID = p_msg_id;
EXCEPTION
    WHEN OTHERS THEN
         Write_error('Pk01_SysLog.read_Msg: ');
         COMMIT;
END;

--
-- Удалить из лог-файла записи старше p_days
--
PROCEDURE Clean_log(p_days IN INTEGER) 
IS
    v_count INTEGER;
    v_date  DATE;
BEGIN
    v_date := TRUNC(SYSDATE) - p_days;
    -- удаляем все сообщения из файла
    DELETE FROM L01_MESSAGES WHERE Msg_Date < v_date;
    v_count := SQL%ROWCOUNT;
    write_Msg('Report: deleted '||v_count||' rows, '||
           'oldest date is '||TO_CHAR(v_date,'dd.mm.yyyy')
          ,'Pk01_SysLog.Clean_log', L_info);
    COMMIT;    
EXCEPTION
    WHEN OTHERS THEN
         Write_error('Pk01_SysLog.Clean_log: ');
         COMMIT;
END;

-- инициализация пакета
BEGIN
  -- получаю номер сеанса из последовательности
  SELECT SQ_L01_SSID.NEXTVAL, USER, UID INTO n_L01_SSID, n_L01_USER, n_L01_UID FROM dual;
  -- получаю информацию о пользователе:
  SELECT USER as "DB_USER",                                        -- 30
         SYS_CONTEXT('USERENV', 'IP_ADDRESS'),
         SYS_CONTEXT('USERENV', 'HOST'),
         SYS_CONTEXT('USERENV', 'OS_USER'),
         SYS_CONTEXT('USERENV', 'MODULE')
    INTO g_DB_USER, g_IP_ADDRESS, g_HOST_NAME, g_OS_USER, g_MODULE
    FROM dual;  

END Pk01_SysLog;
/
