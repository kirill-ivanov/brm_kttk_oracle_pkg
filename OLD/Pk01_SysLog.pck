create or replace package Pk01_SysLog is

  -- Author  : SMAKEEV
  -- Created : 11.11.2011 10:55:14
  -- Purpose : ������� �����������
  
  --=========================================================================================
  -- ������� ����������� �������� �� ������� �������� ������,
  -- ������� �������� WRITE ��������� ������, 
  -- ���������� �� ���� ���� ������������� � ���������� ���������� ��� ���.
  --=========================================================================================
  --
  -- ������ ��������� �� �������
  L_err      constant varchar2(1) :=  'E'; -- ������ ��������� ������
  L_normal   constant varchar2(1) :=  'M'; -- ���������� ����������������
  L_warn     constant varchar2(1) :=  'W'; -- ��������������� ���������
  L_info     constant varchar2(1) :=  'I'; -- �������������� ���������
  L_debug    constant varchar2(1) :=  'D'; -- ���������� ���������
  --
  n_CONSOLE  constant varchar2(3) := 'CON';
  n_BLANK    constant varchar2(3) := ' ';
  --
  n_APP_EXCEPTION constant number := -20100;  -- ����� ���������� � ���������� ������������
  --
  -- ���������� ����������
  --
  n_L01_SSID   integer;        -- ������������� ������ ����� � ORACLE
  n_L01_USER   varchar2(30);   -- ��� ������������
  n_L01_UID    integer;        -- ������������� ������������
  g_DB_USER    L01_MESSAGES.DB_USER%TYPE; 
  g_IP_ADDRESS L01_MESSAGES.IP_ADDRESS%TYPE; 
  g_HOST_NAME  L01_MESSAGES.HOST_NAME%TYPE; 
  g_OS_USER    L01_MESSAGES.OS_USER%TYPE; 
  g_MODULE     L01_MESSAGES.PROGRAM%TYPE;
  --
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- � � � �� � � � �
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- ���������� ����� ������ �� ������������������
  FUNCTION get_SSID RETURN INTEGER;

  -- �������� ����� ������ ORACLE
  FUNCTION get_OraErrTxt(p_Txt in varchar2 default n_BLANK) RETURN VARCHAR2;

  -- �������� ������� �� ������� � ��������
  FUNCTION get_TimeDiff(p_tsfrom in timestamp, p_tsto in timestamp) RETURN INTEGER DETERMINISTIC;

  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- � � � � � � � � �
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  --
  -- ������� ��������� ORACLE � ��������� � ��������� (�����+���������)
  PROCEDURE Raise_exception(
              p_Msg IN VARCHAR2,
              p_Src IN VARCHAR2 DEFAULT n_CONSOLE
            );

  -- ������ ��������� � ����������� ������ �������� ������
  PROCEDURE Write_error(  
              p_Msg IN VARCHAR2 DEFAULT n_BLANK, 
              p_Src IN VARCHAR2 DEFAULT n_CONSOLE
            );
  --
  PROCEDURE Insert_Error(  
              p_Msg IN VARCHAR2 DEFAULT n_BLANK, 
              p_Src IN VARCHAR2 DEFAULT n_CONSOLE
            );

  -- �������� ��������� � Log-File, ���� ��������� ������ �� ������ ����� �� �������
  PROCEDURE Write_msg(
              p_Msg    IN VARCHAR2,                   -- ���������
              p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- �������� (�����+�������)
              p_Level  IN VARCHAR2 DEFAULT L_info,    -- ������� ���������
              p_AppUsr IN VARCHAR2 DEFAULT NULL       -- ������������ ���������� 
            );

  -- �������� ��������� � �������� Log-File
  PROCEDURE Insert_msg(
              p_Msg    IN VARCHAR2,                   -- ���������
              p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- �������� (�����+�������)
              p_Level  IN VARCHAR2 DEFAULT L_info,    -- ������� ���������
              p_AppUsr IN VARCHAR2 DEFAULT NULL       -- ������������ ���������� 
            );

  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- � � � � � � � (���������� L01_ID)
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- �������� ��������� � Log-File, ���� ��������� ������ �� ������ ����� �� �������
  -- ��� COMMIT
  FUNCTION Fn_insert_msg(
              p_Msg    IN VARCHAR2,                   -- ���������
              p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- �������� (�����+�������)
              p_Level  IN VARCHAR2 DEFAULT L_info,    -- ������� ���������
              p_AppUsr IN VARCHAR2 DEFAULT NULL       -- ������������ ����������
            ) RETURN INTEGER;

  -- �������� ��������� � Log-File, ���� ��������� ������ �� ������ ����� �� �������
  FUNCTION Fn_write_msg(
              p_Msg    IN VARCHAR2,                   -- ���������
              p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- �������� (�����+�������)
              p_Level  IN VARCHAR2 DEFAULT L_info,    -- ������� ���������
              p_AppUsr IN VARCHAR2 DEFAULT NULL       -- ������������ ���������� 
            ) RETURN INTEGER;

  -- ������ ��������� � ����������� ������ �������� ������
  FUNCTION Fn_insert_error(
              p_Msg  IN VARCHAR2 DEFAULT n_BLANK, 
              p_Src  IN VARCHAR2 DEFAULT n_CONSOLE
            ) RETURN INTEGER;

  -- ������ ��������� � ����������� ������ �������� ������
  FUNCTION Fn_write_error(
              p_Msg  IN VARCHAR2 DEFAULT n_BLANK, 
              p_Src  IN VARCHAR2 DEFAULT n_CONSOLE
            ) RETURN INTEGER;
  
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- ��������������
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- ��������� ��������� �� Log-File
  PROCEDURE Read_msg(
              p_Level     OUT VARCHAR2,
              p_Date      OUT DATE,
              p_Msg       OUT VARCHAR2,
              p_msg_id    IN INTEGER
            );
                          
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- ������� �� ���-����� ������ ������ p_days
  PROCEDURE Clean_log(p_days IN INTEGER);


end Pk01_SysLog;
/
create or replace package body Pk01_SysLog is

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- � � � �� � � � �
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- ���������� ����� ������ �� ������������������
FUNCTION get_SSID RETURN INTEGER IS
BEGIN
  RETURN n_L01_SSID;
END;
--
-- �������� ����� ������ ORACLE
--
FUNCTION get_OraErrTxt(p_Txt in varchar2 default n_BLANK) RETURN VARCHAR2
IS
    v_ErrorText  VARCHAR2(2000);    -- ����� ��������� �� ������
BEGIN
    v_ErrorText  := SUBSTRB(p_Txt||'=>'||SQLERRM, 1, 2000);  -- ����� ��������� �� ������
    RETURN v_ErrorText;
END;

--
-- �������� ������� �� ������� � ��������
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
-- � � � � � � � (���������� L01_ID)
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- �������� ��������� � Log-File, ���� ��������� ������ �� ������ ����� �� �������
-- ��� COMMIT
FUNCTION Fn_insert_msg(
                    p_Msg    IN VARCHAR2,                   -- ���������
                    p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- �������� (�����+�������)
                    p_Level  IN VARCHAR2 DEFAULT L_info,    -- ������� ���������
                    p_AppUsr IN VARCHAR2 DEFAULT NULL       -- ������������ ����������
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

-- �������� ��������� � Log-File, ���� ��������� ������ �� ������ ����� �� �������
FUNCTION Fn_write_msg(
                    p_Msg    IN VARCHAR2,                   -- ���������
                    p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- �������� (�����+�������)
                    p_Level  IN VARCHAR2 DEFAULT L_info,    -- ������� ���������
                    p_AppUsr IN VARCHAR2 DEFAULT NULL       -- ������������ ���������� 
                ) RETURN INTEGER 
IS
    v_l01_id  INTEGER;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    v_l01_id := Fn_insert_msg(p_Msg, p_Src, p_Level, p_AppUsr);
    COMMIT;
    return v_l01_id;
END;

-- ������ ��������� � ����������� ������ �������� ������
FUNCTION Fn_insert_error(
                      p_Msg  IN VARCHAR2 DEFAULT n_BLANK, 
                      p_Src  IN VARCHAR2 DEFAULT n_CONSOLE
                    ) RETURN INTEGER 
IS
BEGIN
    RETURN Fn_insert_msg(Get_OraErrTxt(p_Msg), p_Src, L_err);
END;

-- ������ ��������� � ����������� ������ �������� ������
FUNCTION Fn_write_error(
                      p_Msg  IN VARCHAR2 DEFAULT n_BLANK, 
                      p_Src  IN VARCHAR2 DEFAULT n_CONSOLE
                    ) RETURN INTEGER 
IS
BEGIN
    RETURN Fn_write_msg(Get_OraErrTxt(p_Msg), p_Src, L_err);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- � � � � � � � � �
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- ������� ��������� ORACLE � ��������� � ��������� (�����+���������)
--
PROCEDURE Raise_exception(
            p_Msg IN VARCHAR2,
            p_Src IN VARCHAR2 DEFAULT n_CONSOLE
          )
IS
    v_ErrorCode  NUMBER;           -- ��� ��������� �� ������
    v_msg_id     INTEGER;
BEGIN
    v_ErrorCode := SQLCODE;
    v_msg_id := Fn_write_error(p_Msg, p_Src);
    RAISE_APPLICATION_ERROR(n_APP_EXCEPTION, 'msg_id='||v_msg_id||':'||p_Src);
END;

--
-- ������ ��������� � ����������� ������ �������� ������
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
-- �������� ��������� � Log-File, ���� ��������� ������ �� ������ ����� �� �������
--
PROCEDURE Write_msg(
                    p_Msg    IN VARCHAR2,                   -- ���������
                    p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- �������� (�����+�������)
                    p_Level  IN VARCHAR2 DEFAULT L_info,    -- ������� ���������
                    p_AppUsr IN VARCHAR2 DEFAULT NULL       -- ������������ ���������� 
                ) 
IS
    v_l01_id  INTEGER;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    v_l01_id := Fn_insert_msg(p_Msg, p_Src, p_Level, p_AppUsr);
    COMMIT;
END;

--
-- �������� ��������� � �������� Log-File
--
PROCEDURE Insert_msg(
                    p_Msg    IN VARCHAR2,                   -- ���������
                    p_Src    IN VARCHAR2 DEFAULT n_CONSOLE, -- �������� (�����+�������)
                    p_Level  IN VARCHAR2 DEFAULT L_info,    -- ������� ���������
                    p_AppUsr IN VARCHAR2 DEFAULT NULL       -- ������������ ���������� 
                ) 
IS
    v_l01_id  INTEGER;
BEGIN
    v_l01_id := Fn_insert_msg(p_Msg, p_Src, p_Level, p_AppUsr);
END;

--
-- ��������� ��������� �� Log-File
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
-- ������� �� ���-����� ������ ������ p_days
--
PROCEDURE Clean_log(p_days IN INTEGER) 
IS
    v_count INTEGER;
    v_date  DATE;
BEGIN
    v_date := TRUNC(SYSDATE) - p_days;
    -- ������� ��� ��������� �� �����
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

-- ������������� ������
BEGIN
  -- ������� ����� ������ �� ������������������
  SELECT SQ_L01_SSID.NEXTVAL, USER, UID INTO n_L01_SSID, n_L01_USER, n_L01_UID FROM dual;
  -- ������� ���������� � ������������:
  SELECT USER as "DB_USER",                                        -- 30
         SYS_CONTEXT('USERENV', 'IP_ADDRESS'),
         SYS_CONTEXT('USERENV', 'HOST'),
         SYS_CONTEXT('USERENV', 'OS_USER'),
         SYS_CONTEXT('USERENV', 'MODULE')
    INTO g_DB_USER, g_IP_ADDRESS, g_HOST_NAME, g_OS_USER, g_MODULE
    FROM dual;  

END Pk01_SysLog;
/
