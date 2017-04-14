CREATE OR REPLACE PACKAGE PK21_SUBSCRIBER
IS
    --
    -- Пакет для работы с объектом "ПОКУПАТЕЛЬ-ФИЗ.ЛИЦО", таблицы:
    -- customer_t, customer_addres_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK21_SUBSCRIBER';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- создать нового покупателя-физ.лицо, возвращает значения
    --   - положительное - ID покупателя, 
    --   - при ошибке выставляет исключение
    FUNCTION New_subscriber(
                   p_last_name   IN VARCHAR2,   -- фамилия
                   p_first_name  IN VARCHAR2,   -- имя 
                   p_middle_name IN VARCHAR2,   -- отчество
                   p_category    IN INTEGER DEFAULT Pk00_Const.c_SUBS_RESIDENT  -- категория 1/2 = резидент/нерезидент
               ) RETURN INTEGER;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Изменить параметры покупателя-физ. лицо, возвращает значения
--   - при ошибке выставляет исключение
--
     PROCEDURE Edit_subscriber(
          p_subscriber_id    IN INTEGER,
          p_last_name        IN VARCHAR2,   -- фамилия
          p_first_name       IN VARCHAR2,   -- имя 
          p_middle_name      IN VARCHAR2,   -- отчество
          p_category         IN INTEGER DEFAULT Pk00_Const.c_SUBS_RESIDENT  -- категория 1/2 = резидент/нерезидент
     );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- добавить документ клиента
    --   - положительное - ID покупателя, 
    --   - при ошибке выставляет исключение
    PROCEDURE Add_document(
                   p_subscriber_id  IN INTEGER,
                   p_doc_type       IN VARCHAR2, -- тип документа
                   p_doc_serial     IN VARCHAR2, -- серия документа
                   p_doc_no         IN VARCHAR2, -- номер документа
                   p_doc_issuer     IN VARCHAR2, -- кем выдан документ
                   p_doc_issue_date IN DATE      -- дата выдачи документа
               );
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- добавить адрес клиента
    --   - при ошибке выставляет исключение
    FUNCTION Add_address(
                   p_subscriber_id IN INTEGER,
                   p_address_type  IN VARCHAR2,
                   p_country       IN VARCHAR2,
                   p_zip           IN VARCHAR2,
                   p_state         IN VARCHAR2,
                   p_city          IN VARCHAR2,
                   p_address       IN VARCHAR2,
                   p_person        IN VARCHAR2,
                   p_phones        IN VARCHAR2,
                   p_fax           IN VARCHAR2,
                   p_email         IN VARCHAR2,
                   p_date_from     IN DATE,
                   p_date_to       IN DATE DEFAULT NULL ,
                   p_notes         IN VARCHAR2 DEFAULT NULL
               ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    -- прочитать данные клиента
    --   - при ошибке выставляет исключение
    PROCEDURE Read_subscriber( 
                   p_recordset     OUT t_refc, 
                   p_subscriber_id  IN INTEGER
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- найти клиента - физ.лицо по документу
    --   - при ошибке выставляет исключение
    PROCEDURE Find_by_doc(
                   p_recordset     OUT t_refc,
                   p_doc_type       IN VARCHAR2, -- тип документа
                   p_doc_serial     IN VARCHAR2, -- серия документа
                   p_doc_no         IN VARCHAR2  -- номер документа
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- найти клиента - по Ф.И.О.
    --   - при ошибке выставляет исключение
    PROCEDURE Find_by_name(
                   p_recordset  OUT t_refc,
                   p_last_name   IN VARCHAR2,   -- фамилия
                   p_first_name  IN VARCHAR2,   -- имя 
                   p_middle_name IN VARCHAR2    -- отчество
               );
               
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Получить список адресов (контактов) на лицевом счете
    --   - положительное - кол-во строк
    --   - при ошибке выставляет ислючение
    --
    FUNCTION Address_list( 
                   p_recordset    OUT t_refc,
                   p_subscriber_id IN INTEGER,
                   p_date          IN DATE DEFAULT SYSDATE
               ) RETURN INTEGER;
    
END PK21_SUBSCRIBER;
/
CREATE OR REPLACE PACKAGE BODY PK21_SUBSCRIBER
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- создать нового покупателя-физ.лицо, возвращает значения
--   - положительное - ID покупателя, 
--   - при ошибке выставляет исключение
FUNCTION New_subscriber(
               p_last_name   IN VARCHAR2,   -- фамилия
               p_first_name  IN VARCHAR2,   -- имя 
               p_middle_name IN VARCHAR2,   -- отчество
               p_category    IN INTEGER DEFAULT Pk00_Const.c_SUBS_RESIDENT  -- категория 1/2 = резидент/нерезидент
           ) RETURN INTEGER
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'New_subscriber';
    v_subscriber_id INTEGER;
BEGIN
    v_subscriber_id := Pk02_Poid.Next_subscriber_id;
    INSERT INTO SUBSCRIBER_T(SUBSCRIBER_ID,LAST_NAME,FIRST_NAME,MIDDLE_NAME,CATEGORY)
    VALUES(v_subscriber_id, p_last_name, p_first_name, p_middle_name, p_category);
    RETURN v_subscriber_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_last_name='||p_last_name, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Изменить параметры покупателя-физ. лицо, возвращает значения
--   - при ошибке выставляет исключение
--
PROCEDURE Edit_subscriber(
          p_subscriber_id    IN INTEGER,
          p_last_name        IN VARCHAR2,   -- фамилия
          p_first_name       IN VARCHAR2,   -- имя 
          p_middle_name      IN VARCHAR2,   -- отчество
          p_category         IN INTEGER DEFAULT Pk00_Const.c_SUBS_RESIDENT  -- категория 1/2 = резидент/нерезидент
)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Edit_subscriber';
BEGIN
    UPDATE SUBSCRIBER_T 
       SET LAST_NAME       = NVL(p_last_name, LAST_NAME), 
           FIRST_NAME      = NVL(p_first_name, FIRST_NAME), 
           MIDDLE_NAME     = NVL(p_middle_name, MIDDLE_NAME),  
           CATEGORY        = NVL(p_category, CATEGORY) 
     WHERE SUBSCRIBER_ID = p_subscriber_id;  
    IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(-20000, 'В таблице SUBSCRIBER_T нет записи с SUBSCRIBER_ID = ' || p_subscriber_id);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_subscriber_id='||p_subscriber_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- добавить документ клиента
--   - при ошибке выставляет исключение
PROCEDURE Add_document(
               p_subscriber_id  IN INTEGER,
               p_doc_type       IN VARCHAR2, -- тип документа
               p_doc_serial     IN VARCHAR2, -- серия документа
               p_doc_no         IN VARCHAR2, -- номер документа
               p_doc_issuer     IN VARCHAR2, -- кем выдан документ
               p_doc_issue_date IN DATE      -- дата выдачи документа
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Add_document';
BEGIN
    INSERT INTO SUBSCRIBER_DOC_T(
        SUBSCRIBER_ID,DOCUMENT_TYPE,DOC_SERIAL,DOC_NO,DOC_ISSUER,DOC_ISSUE_DATE
    )VALUES(
        p_subscriber_id, p_doc_type, p_doc_serial, p_doc_no, p_doc_issuer, p_doc_issue_date   
    );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_subscriber_id='||p_subscriber_id, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- добавить адрес клиента
--   - при ошибке выставляет исключение
FUNCTION Add_address(
               p_subscriber_id IN INTEGER,
               p_address_type  IN VARCHAR2,
               p_country       IN VARCHAR2,
               p_zip           IN VARCHAR2,
               p_state         IN VARCHAR2,
               p_city          IN VARCHAR2,
               p_address       IN VARCHAR2,
               p_person        IN VARCHAR2,
               p_phones        IN VARCHAR2,
               p_fax           IN VARCHAR2,
               p_email         IN VARCHAR2,
               p_date_from     IN DATE,
               p_date_to       IN DATE DEFAULT NULL ,
               p_notes         IN VARCHAR2 DEFAULT NULL
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Add_address';
    v_account_id INTEGER;
BEGIN
    -- находим лицевой счет
    SELECT ACCOUNT_ID INTO v_account_id
      FROM ACCOUNT_PROFILE_T
     WHERE SUBSCRIBER_ID = p_subscriber_id
       AND p_date_from <= NVL(DATE_TO, TO_DATE('01.01.2050','dd.mm.yy'))
       AND DATE_FROM <= NVL(p_date_to, TO_DATE('01.01.2050','dd.mm.yy'));
    -- добавляем адрес
    RETURN Pk05_Account.Add_address(
               v_account_id,
               p_address_type,
               p_country,
               p_zip,
               p_state,
               p_city,
               p_address,
               p_person,
               p_phones,
               p_fax,
               p_email,
               p_date_from,
               p_date_to,
               p_notes
           );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_subscriber_id='||p_subscriber_id, c_PkgName||'.'||v_prcName );
END;



-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- прочитать данные клиента
--   - при ошибке выставляет исключение
--
PROCEDURE Read_subscriber( 
               p_recordset     OUT t_refc, 
               p_subscriber_id  IN INTEGER
             )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Read_subscriber';
    v_retcode INTEGER;
BEGIN
    OPEN p_recordset FOR
         SELECT S.SUBSCRIBER_ID,S.LAST_NAME,S.FIRST_NAME,S.MIDDLE_NAME,S.CATEGORY, 
                D.DOCUMENT_TYPE,D.DOC_SERIAL,D.DOC_NO,D.DOC_ISSUER,D.DOC_ISSUE_DATE
           FROM SUBSCRIBER_T S, SUBSCRIBER_DOC_T D
          WHERE S.SUBSCRIBER_ID = p_subscriber_id;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR, p_subscriber_id='||p_subscriber_id, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- найти клиента - физ.лицо по документу
--   - при ошибке выставляет исключение
PROCEDURE Find_by_doc(
               p_recordset     OUT t_refc,
               p_doc_type       IN VARCHAR2, -- тип документа
               p_doc_serial     IN VARCHAR2, -- серия документа
               p_doc_no         IN VARCHAR2  -- номер документа
           )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Find_by_doc';
    v_retcode INTEGER;
BEGIN
    OPEN p_recordset FOR
         SELECT S.SUBSCRIBER_ID,S.LAST_NAME,S.FIRST_NAME,S.MIDDLE_NAME,S.CATEGORY, 
                D.DOCUMENT_TYPE,D.DOC_SERIAL,D.DOC_NO,D.DOC_ISSUER,D.DOC_ISSUE_DATE
           FROM SUBSCRIBER_T S, SUBSCRIBER_DOC_T D
          WHERE D.DOCUMENT_TYPE = p_doc_type
            AND D.DOC_SERIAL = p_doc_serial
            AND D.DOC_NO = p_doc_no;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- найти клиента - по Ф.И.О.
--   - при ошибке выставляет исключение
PROCEDURE Find_by_name(
               p_recordset  OUT t_refc,
               p_last_name   IN VARCHAR2,   -- фамилия
               p_first_name  IN VARCHAR2,   -- имя 
               p_middle_name IN VARCHAR2    -- отчество
           ) 
IS
    v_prcName CONSTANT VARCHAR2(30) := 'Find_by_doc';
    v_retcode INTEGER;
BEGIN
    OPEN p_recordset FOR
         SELECT S.SUBSCRIBER_ID,S.LAST_NAME,S.FIRST_NAME,S.MIDDLE_NAME,S.CATEGORY, 
                D.DOCUMENT_TYPE,D.DOC_SERIAL,D.DOC_NO,D.DOC_ISSUER,D.DOC_ISSUE_DATE
           FROM SUBSCRIBER_T S, SUBSCRIBER_DOC_T D
          WHERE S.LAST_NAME   LIKE p_last_name
            AND S.FIRST_NAME  LIKE p_first_name 
            AND S.MIDDLE_NAME LIKE p_middle_name;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Получить список адресов (контактов) на лицевом счете
--   - положительное - кол-во строк
--   - при ошибке выставляет ислючение
--
FUNCTION Address_list( 
               p_recordset    OUT t_refc,
               p_subscriber_id IN INTEGER,
               p_date          IN DATE DEFAULT SYSDATE
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Address_list';
    v_account_id INTEGER;
BEGIN
    -- находим лицевой счет
    SELECT ACCOUNT_ID INTO v_account_id
      FROM ACCOUNT_PROFILE_T
     WHERE SUBSCRIBER_ID = p_subscriber_id
       AND p_date >= DATE_FROM
       AND (DATE_TO IS NULL OR p_date <= DATE_TO );  
    -- извлечь список адресов
    RETURN Pk05_Account.Address_list(p_recordset,v_account_id);
    --
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;




END PK21_SUBSCRIBER;
/
