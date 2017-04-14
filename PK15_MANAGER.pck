CREATE OR REPLACE PACKAGE PK15_MANAGER
IS
    --
    -- ����� ��� ������ � �������� "��������", �������:
    -- manager_t, manager_info_t
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK15_MANAGER';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- ������� ������ ���������, ���������� ��������
    --   - ������������� - ID ���������, 
    --   - ��� ������ ���������� ����������
    FUNCTION New_manager(
                   p_contractor_id    IN INTEGER,
                   p_department       IN VARCHAR2,
                   p_position         IN VARCHAR2, 
                   p_last_name        IN VARCHAR2, -- �������
                   p_first_name       IN VARCHAR2, -- ��� 
                   p_middle_name      IN VARCHAR2, -- ��������
                   p_phones           IN VARCHAR2,
                   p_email            IN VARCHAR2,
                   p_date_from        IN DATE,
                   p_date_to          IN DATE DEFAULT NULL
               ) RETURN INTEGER;

    -- ��������� ������ ���������, ���������� ��������
    --   - ��� ������ ���������� ����������
    PROCEDURE Update_manager(
                   p_manager_id       IN INTEGER,
                   p_contractor_id    IN INTEGER,
                   p_department       IN VARCHAR2,
                   p_position         IN VARCHAR2, 
                   p_last_name        IN VARCHAR2, -- �������
                   p_first_name       IN VARCHAR2, -- ��� 
                   p_middle_name      IN VARCHAR2, -- ��������
                   p_phones           IN VARCHAR2,
                   p_email            IN VARCHAR2,
                   p_date_from        IN DATE,
                   p_date_to          IN DATE DEFAULT NULL
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� �������� ������� � ���������
    --   - ��� ������ ���������� ����������
    FUNCTION Update_manager_picture(
             p_manager_id       IN INTEGER,
             p_file             IN BLOB,       -- ���� 
             p_file_size        IN INTEGER     -- ������ �����
         )RETURN INTEGER;
               
    -- ������� ���������, ���������� ��������
    --   - ��� ������ ���������� ����������
    PROCEDURE Delete_manager(p_manager_id IN INTEGER);

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    -- ��������� ��������� �� �������/������� ����/�����
    --   - c_RET_OK - OK,
    --   - ��� ������ ���������� ����������
    --
    PROCEDURE Set_manager_info( 
                 p_manager_id  IN INTEGER,
                 p_contract_id IN INTEGER,
                 p_account_id  IN INTEGER,
                 p_order_id    IN INTEGER,
                 p_date_from   IN DATE,
                 p_date_to     IN DATE DEFAULT NULL
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    -- ����� ID ��������� �� ������ ������� (����� � ���������) � id �����������
    --   - ������������� - ���-�� �������,
    --   - ��� ������ ���������� ����������
    --
    FUNCTION Find(
                 p_recordset     OUT t_refc, 
                 p_manager_id    IN  INTEGER,               
                 p_last_name     IN VARCHAR2,
                 p_contractor_id IN INTEGER
               ) RETURN INTEGER;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    -- �������� ���������� ��� SALES_CURATOR
    --
    FUNCTION Get_sales_curator (
                 p_contract_id IN INTEGER,
                 p_account_id  IN INTEGER,
                 p_order_id    IN INTEGER,
                 p_date        IN DATE
               ) RETURN VARCHAR2;
  
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    -- �������� ������ ��������� �� �������� �����
    --
    PROCEDURE GET_SALE_CURATOR_BY_ACCOUNT( 
                  p_recordset     OUT t_refc,               
                  p_account_id    IN INTEGER,
                  p_date          IN DATE
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    -- �������� ������ ����������
    --
    PROCEDURE Manager_list( 
                   p_recordset     OUT t_refc
               );
  
END PK15_MANAGER;
/
CREATE OR REPLACE PACKAGE BODY PK15_MANAGER
IS

-- ������� ������ ���������, ���������� ��������
--   - ������������� - ID ���������, 
--   - ��� ������ ���������� ����������
FUNCTION New_manager(
               p_contractor_id    IN INTEGER,
               p_department       IN VARCHAR2,
               p_position         IN VARCHAR2, 
               p_last_name        IN VARCHAR2, -- �������
               p_first_name       IN VARCHAR2, -- ��� 
               p_middle_name      IN VARCHAR2, -- ��������
               p_phones           IN VARCHAR2,
               p_email            IN VARCHAR2,
               p_date_from        IN DATE,
               p_date_to          IN DATE DEFAULT NULL
           ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'New_manager';
    v_manager_id INTEGER;
BEGIN
    INSERT INTO MANAGER_T (
        MANAGER_ID, CONTRACTOR_ID, DEPARTMENT, POSITION, 
        LAST_NAME, FIRST_NAME, MIDDLE_NAME, PHONES, EMAIL, DATE_FROM, DATE_TO 
    )VALUES(
        SQ_CLIENT_ID.NEXTVAL, p_contractor_id, p_department, p_position,
        p_last_name, p_first_name, p_middle_name, p_phones, p_email, p_date_from, p_date_to
    ) RETURNING MANAGER_ID INTO v_manager_id;
    RETURN v_manager_id;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ��������� ������ ���������, ���������� ��������
--   - ��� ������ ���������� ����������
PROCEDURE Update_manager(
               p_manager_id       IN INTEGER,
               p_contractor_id    IN INTEGER,
               p_department       IN VARCHAR2,
               p_position         IN VARCHAR2, 
               p_last_name        IN VARCHAR2, -- �������
               p_first_name       IN VARCHAR2, -- ��� 
               p_middle_name      IN VARCHAR2, -- ��������
               p_phones           IN VARCHAR2,
               p_email            IN VARCHAR2,
               p_date_from        IN DATE,
               p_date_to          IN DATE DEFAULT NULL
           )
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Update_manager';
BEGIN
    UPDATE MANAGER_T 
       SET CONTRACTOR_ID = p_contractor_id,
           DEPARTMENT    = p_department,
           POSITION      = p_position,
           LAST_NAME     = p_last_name,
           FIRST_NAME    = p_first_name,
           MIDDLE_NAME   = p_middle_name,
           PHONES        = p_phones,
           EMAIL         = p_email,
           DATE_FROM     = p_date_from,
           DATE_TO       = p_date_to
     WHERE MANAGER_ID = p_manager_id;  
    IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(-20000, '� ������� MANAGER_T ��� ������ � MANAGER_ID='||p_manager_id);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- �������� �������� ������� � ���������
--   - ��� ������ ���������� ����������
FUNCTION Update_manager_picture(
             p_manager_id       IN INTEGER,
             p_file             IN BLOB,       -- ���� 
             p_file_size        IN INTEGER     -- ������ �����
         )RETURN INTEGER
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'UPDATE_MANAGER_PICTURE';
    v_picture_id  INTEGER;
    v_action      VARCHAR2(100);
BEGIN
    BEGIN
      SELECT PICTURE_ID INTO v_picture_id FROM
             MANAGER_T M,
             PICTURE_T P
         WHERE P.PICTURE_ID = M.SIGN_PICTURE_ID
               AND M.MANAGER_ID = p_manager_id;
      EXCEPTION 
         WHEN NO_DATA_FOUND THEN v_picture_id := NULL;
      END;
    
    IF v_picture_id IS NULL THEN
       IF p_file IS NOT NULL THEN
          v_action := 'INSERT';
       END IF;      
    ELSE
       IF p_file IS NOT NULL THEN
          v_action := 'UPDATE';
       ELSE
          v_action := 'DELETE';
       END IF;         
    END IF;
    
    IF v_action = 'DELETE' OR v_action = 'UPDATE' THEN
       UPDATE 
           MANAGER_T 
         SET SIGN_PICTURE_ID = NULL 
       WHERE MANAGER_ID = p_manager_id;    
    
       DELETE FROM PICTURE_T WHERE PICTURE_ID = v_picture_id;
    END IF;
    
    IF v_action = 'UPDATE' OR v_action = 'INSERT' THEN
       INSERT INTO PICTURE_T (PICTURE_ID, PICTURE, PICTURE_SIZE)
            VALUES(SQ_POOL_ID.NEXTVAL, p_file, p_file_size)
       RETURNING PICTURE_ID INTO v_picture_id;      
       
       UPDATE 
           MANAGER_T 
         SET SIGN_PICTURE_ID = v_picture_id 
         WHERE MANAGER_ID = p_manager_id;
    END IF;    

    RETURN V_PICTURE_ID;        
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- ������� ���������, ���������� ��������
--   - ��� ������ ���������� ����������
PROCEDURE Delete_manager(p_manager_id IN INTEGER)
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Delete_manager';
    v_picture_id INTEGER;
BEGIN
    SELECT 
       SIGN_PICTURE_ID into v_picture_id 
    from manager_t 
       where manager_Id = p_manager_id;       

    DELETE MANAGER_T WHERE MANAGER_ID = p_manager_id;
    IF SQL%ROWCOUNT = 0 THEN
       RAISE_APPLICATION_ERROR(-20000, '� ������� MANAGER_T ��� ������ � MANAGER_ID='||p_manager_id);
    END IF;
    
    DELETE FROM PICTURE_T WHERE PICTURE_ID = v_picture_Id;        
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;  

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- ��������� ��������� �� �������/������� ����/�����
--   - ��� ������ ���������� ����������
PROCEDURE Set_manager_info( 
               p_manager_id  IN INTEGER,
               p_contract_id IN INTEGER,
               p_account_id  IN INTEGER,
               p_order_id    IN INTEGER,
               p_date_from   IN DATE,
               p_date_to     IN DATE DEFAULT NULL
             )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Set_manager_info';
BEGIN
    INSERT INTO SALE_CURATOR_T (
        MANAGER_ID, CONTRACT_ID, ACCOUNT_ID, ORDER_ID, DATE_FROM, DATE_TO 
    )VALUES(
        p_manager_id, p_contract_id, p_account_id, p_order_id, p_date_from, p_date_to
    );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR', c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- ����� ID ��������� �� ������ ������� (����� � ���������) � id �����������
--   - ������������� - ���-�� �������,
--   - ��� ������ ���������� ����������
--
FUNCTION Find( 
               p_recordset     OUT t_refc, 
               p_manager_id    IN  INTEGER,               
               p_last_name     IN VARCHAR2,
               p_contractor_id IN INTEGER
             ) RETURN INTEGER
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Find';
    v_retcode    INTEGER := c_RET_OK;
BEGIN
    SELECT COUNT(*) INTO v_retcode
     FROM MANAGER_T
    WHERE UPPER(LAST_NAME) LIKE UPPER(p_last_name)||'%'
      AND CONTRACTOR_ID = NVL(p_contractor_id, CONTRACTOR_ID);

    OPEN p_recordset FOR
         SELECT MANAGER_ID, M.CONTRACTOR_ID, CONTRACTOR CONTRACTOR_NAME, M.SIGN_PICTURE_ID PICTURE_ID, P.PICTURE,
                DEPARTMENT, POSITION, LAST_NAME, FIRST_NAME, MIDDLE_NAME, 
                PHONES, EMAIL, DATE_FROM, DATE_TO
           FROM MANAGER_T M,CONTRACTOR_T C,PICTURE_T P
          WHERE M.CONTRACTOR_ID = C.CONTRACTOR_ID
            AND P.PICTURE_ID (+)= M.SIGN_PICTURE_ID 
            AND UPPER(LAST_NAME) LIKE UPPER(p_last_name)||'%'
            AND (MANAGER_ID = p_manager_id  OR p_manager_id IS NULL)
            AND M.CONTRACTOR_ID = NVL(p_contractor_id, M.CONTRACTOR_ID)
          ORDER BY LAST_NAME, FIRST_NAME;
    RETURN v_retcode;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- --------------------------------------------------------------------------------- --
-- �������� ���������� ��� SALES_CURATOR
-- --------------------------------------------------------------------------------- --
FUNCTION Get_sales_curator (
           p_contract_id IN INTEGER,
           p_account_id  IN INTEGER,
           p_order_id    IN INTEGER,
           p_date        IN DATE
         ) RETURN VARCHAR2
IS
    v_mgr VARCHAR2(300);
BEGIN
    SELECT 
           LAST_NAME||' '||
           SUBSTR(UPPER(FIRST_NAME),1,1)||DECODE(FIRST_NAME,NULL,'','.')||
           SUBSTR(UPPER(MIDDLE_NAME),1,1)||DECODE(MIDDLE_NAME,NULL,'','.')
           MGR_NAME 
      INTO v_mgr
      FROM (
        SELECT M.LAST_NAME, M.FIRST_NAME, M.MIDDLE_NAME,
               MAX(
                 CASE 
                   WHEN SC.CONTRACT_ID IS NOT NULL THEN 1
                   WHEN SC.ACCOUNT_ID  IS NOT NULL THEN 2
                   WHEN SC.ORDER_ID    IS NOT NULL THEN 3
                   ELSE 0
                 END)  WT
          FROM SALE_CURATOR_T SC, MANAGER_T M
         WHERE M.MANAGER_ID = SC.MANAGER_ID
           AND SC.DATE_FROM <= p_date
           AND (SC.DATE_TO IS NULL OR p_date <= SC.DATE_TO )
           AND (SC.CONTRACT_ID = p_contract_id OR 
                SC.ACCOUNT_ID  = p_account_id  OR 
                SC.ORDER_ID    = p_order_id )
        GROUP BY M.LAST_NAME, M.FIRST_NAME, M.MIDDLE_NAME
    );
    RETURN v_mgr;
EXCEPTION 
  WHEN NO_DATA_FOUND THEN
    RETURN NULL;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- �������� ������ ��������� �� �������� �����
--
PROCEDURE Get_sale_curator_by_account( 
        p_recordset     OUT t_refc,               
        p_account_id    IN INTEGER,
        p_date          IN DATE
)
IS
    v_prcName     CONSTANT VARCHAR2(30) := 'Get_sale_curator_by_account';
    v_retcode     INTEGER := c_RET_OK;
    
    v_branch_id   INTEGER;
    v_agent_id    INTEGER;
    v_contract_id INTEGER;
    v_order_id    INTEGER;
BEGIN
  
    select BRANCH_ID, AGENT_ID, CONTRACT_ID  INTO
           v_branch_id, v_agent_id, v_contract_id
      from account_profile_t ap
     where NVL(p_date, SYSDATE) between ap.date_from 
                                    and NVL(ap.date_to,TO_DATE('01.01.2050','DD.MM.YYYY'))
       and ap.account_id = p_account_Id;
    
    OPEN p_recordset FOR
         SELECT *
            FROM (
              SELECT M.MANAGER_ID, M.LAST_NAME, M.FIRST_NAME, M.MIDDLE_NAME,
                     CASE 
                       WHEN SC.CONTRACTOR_ID = v_branch_id THEN 1
                       WHEN SC.CONTRACTOR_ID = v_agent_id THEN 2
                       WHEN SC.CONTRACT_ID   IS NOT NULL THEN 3
                       WHEN SC.ACCOUNT_ID    IS NOT NULL THEN 4
                       WHEN SC.ORDER_ID      IS NOT NULL THEN 5
                       ELSE 0
                     END  WT
                FROM SALE_CURATOR_T SC, MANAGER_T M
               WHERE M.MANAGER_ID = SC.MANAGER_ID
                 AND NVL(p_date,SYSDATE) BETWEEN SC.DATE_FROM AND NVL(SC.DATE_TO,SYSDATE) 
                 AND (SC.CONTRACTOR_ID = v_branch_id OR
                      SC.CONTRACTOR_ID = v_agent_id OR
                      SC.CONTRACT_ID = v_contract_id OR 
                      SC.ACCOUNT_ID  = p_account_id  OR 
                      SC.ORDER_ID    = v_order_id )
              ORDER BY WT DESC
          )
          WHERE ROWNUM = 1;                 
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
-- �������� ������ ����������
--
PROCEDURE Manager_list( 
               p_recordset     OUT t_refc
             )
IS
    v_prcName  CONSTANT VARCHAR2(30) := 'Manager_list';
    v_retcode  INTEGER;
BEGIN
    OPEN p_recordset FOR
    WITH ORG AS ( 
        SELECT DEP_ID, ORG_NAME, DEP_NAME, DEP_PATH 
          FROM ( 
            SELECT ROW_NUMBER() OVER (PARTITION BY DEP_ID ORDER BY LVL DESC) RN, O.* 
              FROM (
                SELECT LEVEL LVL, O.STRUCT_ID,
                       CONNECT_BY_ROOT(STRUCT_ID) DEP_ID, 
                       SYS_CONNECT_BY_PATH(NAME,'|') DEP_PATH,
                       CONNECT_BY_ROOT(NAME) DEP_NAME,
                       NAME ORG_NAME
                  FROM ORG_STRUCT_T O
                 CONNECT BY PRIOR O.PARENT_ID = O.STRUCT_ID  
            ) O
        ) O 
        WHERE RN = 1
    )
    SELECT M.MANAGER_ID,
           M.LAST_NAME, M.FIRST_NAME, M.MIDDLE_NAME, MT.KEY,  
           ORG.DEP_PATH, ORG.ORG_NAME, CT.CONTRACTOR
      FROM ORG, MANAGER_T M, DICTIONARY_T MT, CONTRACTOR_T CT
     WHERE M.ORG_STRUCT_ID = ORG.DEP_ID(+)
       AND M.MGR_TYPE_ID = MT.KEY_ID(+)
       AND M.CONTRACTOR_ID = CT.CONTRACTOR_ID(+)
     ORDER BY M.LAST_NAME, M.FIRST_NAME
    ;

EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;



END PK15_MANAGER;
/
