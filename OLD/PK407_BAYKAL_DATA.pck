CREATE OR REPLACE PACKAGE PK407_BAYKAL_DATA
IS
    --
    -- ���������� ����� �� ����� �� ����� n.kuripko@baikal.ttk.ru
    --
    -- �.������� �����:
    -- "��� ���� (���������) ���������� ��������� ����������� �����������, ������� ��� 
    -- ��������� � ���� ������. ������� ����� ������������ � ������� ����� (�����, �������, ������� � �.�.)
    -- ��������� �� 2-� ������������. ��������� ���� �� �������� � �������� �������������.
    -- �.� ������� �� ����� 4 �����".
    --
    -- 1. ����������  - "����"���������������"
    -- 2. ����
    
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK407_BAYKAL_DATA';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- ��������� �����
    c_FL_HEADER CONSTANT VARCHAR2(200) := '"������","�������","����","�����","���. �����","����","��������� ��� ���","���������, ���","������","�����"';

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- ������� ������ � ������� ���������� ������ �� �������� � SLA
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    TYPE t_data IS RECORD (
              START_TIME     VARCHAR2(20),   -- "������",
              PREFIX_B       VARCHAR2(20),   -- "�������",
              ZONE_B         VARCHAR2(100),  -- "����",
              ABN_B          VARCHAR2(40),   -- "�����",
              BILL_MINUTES   VARCHAR2(20),   -- "���. �����",
              PRICE          VARCHAR2(20),   -- "����",
              GROSS          VARCHAR2(20),   -- "��������� ��� ���",
              AMOUNT         VARCHAR2(20),   -- "���������, ���",
              ABN_A          VARCHAR2(40),   -- "������",
              TARIFF         VARCHAR2(200)   -- "�����"
         );
    TYPE rc_data IS REF CURSOR RETURN t_data;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    -- 1. ����������  - "����"���������������", ������� �� ������ �������� (�����)
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- air_national.csv
    PROCEDURE Get_air_mg( 
                   p_recordset     IN OUT rc_data, --SYS_REFCURSOR,
                   p_period_id     IN INTEGER
               );

    -- air_international.csv    
    PROCEDURE Get_air_mn( 
                   p_recordset     IN OUT rc_data, --SYS_REFCURSOR,
                   p_period_id     IN INTEGER
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    -- 2. ����������  - ���� - ���.����
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- national.csv
    PROCEDURE Get_fz_mg( 
                   p_recordset     IN OUT rc_data, --SYS_REFCURSOR,
                   p_period_id     IN INTEGER
               );
               
    -- international.csv    
    PROCEDURE Get_fz_mn( 
                   p_recordset     IN OUT rc_data, --SYS_REFCURSOR,
                   p_period_id     IN INTEGER
               );


END PK407_BAYKAL_DATA;
/
CREATE OR REPLACE PACKAGE BODY PK407_BAYKAL_DATA
IS

PROCEDURE Get_air_data( 
               p_recordset     IN OUT SYS_REFCURSOR,
               p_period_id     IN INTEGER,
               p_subservice_id IN INTEGER
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Get_air_data';
    v_tariff      CONSTANT VARCHAR2(100) := '"����" �������� �������""';
    v_retcode     INTEGER;
    v_period_from DATE;
    v_period_to   DATE;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);    
    
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT 
            '"' || TO_CHAR( BDR.START_TIME,'dd/mm/yyyy hh24:mi:ss') || '"' START_TIME,
            BDR.PREFIX_B,
            '"' || REPLACE(BDR.TERM_Z_NAME,',','/') || '"' ZONE_B,
            BDR.ABN_B, 
            '"' || BDR.BILL_MINUTES || ':00"' BILL_MINUTES,
            '"' || TO_CHAR (TRUNC (BDR.AMOUNT/BDR.BILL_MINUTES))|| ','
                || TO_CHAR ( (ROUND (BDR.AMOUNT/BDR.BILL_MINUTES, 2) - TRUNC (ROUND (BDR.AMOUNT/BDR.BILL_MINUTES, 2))) * 1000000)|| ' ���."' PRICE,
            '"' || TO_CHAR (TRUNC (ROUND ((BDR.AMOUNT), 2) * 100 / 118))|| ','     
                || TO_CHAR (  ROUND (  ROUND ( (BDR.AMOUNT), 2) * 100 / 118 - TRUNC (ROUND ( (BDR.AMOUNT), 2) * 100 / 118), 6) * 1000000)|| ' ���."' GROSS,
            REPLACE(TO_CHAR(ROUND(BDR.AMOUNT,6)),',','.') AMOUNT,
            LTRIM (BDR.ABN_A, '7') ABN_A, 
            v_tariff TARIFF
          FROM BILL_T B, ACCOUNT_T A, ITEM_T I, BDR_VOICE_T BDR
         WHERE A.ACCOUNT_NO    = 'IR102508'
           AND B.ACCOUNT_ID    = A.ACCOUNT_ID
           AND B.REP_PERIOD_ID = p_period_id 
           AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
           AND I.BILL_ID       = B.BILL_ID
           AND BDR.ITEM_ID     = I.ITEM_ID
           AND BDR.REP_PERIOD BETWEEN v_period_from AND v_period_to
           AND BDR.BILL_MINUTES != 0
           AND I.SUBSERVICE_ID = p_subservice_id -- 1 -- MG
           --AND I.SUBSERVICE_ID = 2 -- MN
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

    
PROCEDURE Get_air_mg( 
               p_recordset     IN OUT rc_data, --SYS_REFCURSOR,
               p_period_id     IN INTEGER
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Get_data';
BEGIN
    Get_air_data( p_recordset, p_period_id, Pk00_Const.c_SUBSRV_MG ); --1
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;


PROCEDURE Get_air_mn( 
               p_recordset     IN OUT rc_data, --SYS_REFCURSOR,
               p_period_id     IN INTEGER
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Get_data';
BEGIN
    Get_air_data( p_recordset, p_period_id, Pk00_Const.c_SUBSRV_MN ); --2
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
-- 2. ����������  - ���� - ���.����
-- national.csv   
-- international.csv
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
PROCEDURE Get_fz_data( 
               p_recordset     IN OUT SYS_REFCURSOR,
               p_period_id     IN INTEGER,
               p_subservice_id IN INTEGER
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Get_fz_data';
    v_tariff      CONSTANT VARCHAR2(100) := '"��� - ���������� ����(�������)"';
    v_retcode     INTEGER;
    v_period_from DATE;
    v_period_to   DATE;
BEGIN
    Pk01_Syslog.Write_msg('Start', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    v_period_from := Pk04_Period.Period_from(p_period_id);
    v_period_to   := Pk04_Period.Period_to(p_period_id);    
    
    -- ���������� ������
    OPEN p_recordset FOR
        SELECT 
            '"' || TO_CHAR( BDR.START_TIME,'dd/mm/yyyy hh24:mi:ss') || '"' START_TIME,
            BDR.PREFIX_B,
            '"' || REPLACE(BDR.TERM_Z_NAME,',','/') || '"' ZONE_B,
            BDR.ABN_B, 
            '"' || BDR.BILL_MINUTES || ':00"' BILL_MINUTES,
            '"' || TO_CHAR (TRUNC (BDR.AMOUNT/BDR.BILL_MINUTES))|| ','
                || TO_CHAR ( (ROUND (BDR.AMOUNT/BDR.BILL_MINUTES, 2) - TRUNC (ROUND (BDR.AMOUNT/BDR.BILL_MINUTES, 2))) * 1000000)|| ' ���."' PRICE,
            '"' || TO_CHAR (TRUNC (ROUND ((BDR.AMOUNT), 2) * 100 / 118))|| ','     
                || TO_CHAR (  ROUND (  ROUND ( (BDR.AMOUNT), 2) * 100 / 118 - TRUNC (ROUND ( (BDR.AMOUNT), 2) * 100 / 118), 6) * 1000000)|| ' ���."' GROSS,
            REPLACE(TO_CHAR(ROUND(BDR.AMOUNT,6)),',','.') AMOUNT,
            LTRIM (BDR.ABN_A, '7') ABN_A, 
            v_tariff TARIFF
          FROM BILL_T B, ACCOUNT_T A, ITEM_T I, BDR_VOICE_T BDR,
               ACCOUNT_PROFILE_T AP
        WHERE A.ACCOUNT_TYPE = 'P'
          AND A.ACCOUNT_ID = B.ACCOUNT_ID
          AND B.REP_PERIOD_ID = p_period_id
          AND I.REP_PERIOD_ID = B.REP_PERIOD_ID
          AND I.BILL_ID       = B.BILL_ID
          AND BDR.ITEM_ID     = I.ITEM_ID
          AND BDR.REP_PERIOD BETWEEN v_period_from AND v_period_to
          AND BDR.BILL_MINUTES != 0
          AND BDR.AMOUNT     != 0
          AND AP.PROFILE_ID   = B.PROFILE_ID
          AND AP.AGENT_ID     = 27 -- '��������-��������� ��' 
          AND I.SUBSERVICE_ID = p_subservice_id
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

PROCEDURE Get_fz_mg( 
               p_recordset     IN OUT rc_data, --SYS_REFCURSOR,
               p_period_id     IN INTEGER
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Get_fz_mg';
BEGIN
    Get_fz_data( p_recordset, p_period_id, Pk00_Const.c_SUBSRV_MG); -- 1
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;
    
PROCEDURE Get_fz_mn( 
               p_recordset     IN OUT rc_data, --SYS_REFCURSOR,
               p_period_id     IN INTEGER
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Get_fz_mn';
BEGIN
    Get_fz_data( p_recordset, p_period_id, Pk00_Const.c_SUBSRV_MN ); --2
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;



END PK407_BAYKAL_DATA;
/
