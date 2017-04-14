CREATE OR REPLACE PACKAGE PK22_FIRST_CALL
IS
    --
    -- Реализация схемы "Активация заказа МГМН по первому звонку"
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK22_FIRST_CALL';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Получить список заказов/телефонов в очереди FIRST_CALL_T
    --
    PROCEDURE FirstCall_list ( 
                   p_recordset    OUT t_refc,
                   p_order_no      IN VARCHAR2,
                   p_number        IN VARCHAR2,
                   p_status        IN VARCHAR2,
                   p_date_from     IN DATE DEFAULT SYSDATE-30
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- создать новую строку: заказ + телефон
    --
    PROCEDURE Add_data (
                   p_order_no      IN VARCHAR2,
                   p_number        IN VARCHAR2,
                   p_notes         IN VARCHAR2
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- Ихзменить статус заказа:
    c_ST_OPEN   CONSTANT VARCHAR2(10) := 'OPEN';   -- заявка открыта, ожидаем первого вызова 
    c_ST_CLOSED CONSTANT VARCHAR2(10) := 'CLOSED'; -- заявка отработана, данные в биллинге
    c_ST_READY  CONSTANT VARCHAR2(10) := 'READY';  -- вызов произведен 
    c_ST_ERROR  CONSTANT VARCHAR2(10) := 'ERROR';  -- ошибка
    --
    PROCEDURE Change_status (
                   p_order_no      IN VARCHAR2,
                   p_number        IN VARCHAR2,
                   p_status        IN VARCHAR2
               );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
    -- Удалить данные по заказу
    --
    PROCEDURE Delete_order (
                   p_order_no      IN VARCHAR2
               );

    
END PK22_FIRST_CALL;
/
CREATE OR REPLACE PACKAGE BODY PK22_FIRST_CALL
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Реализация схемы "Активация заказа МГМН по первому звонку"
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Получить список заказов/телефонов в очереди FIRST_CALL_T
--
PROCEDURE FirstCall_list ( 
               p_recordset    OUT t_refc,
               p_order_no      IN VARCHAR2,
               p_number        IN VARCHAR2,
               p_status        IN VARCHAR2,
               p_date_from     IN DATE DEFAULT SYSDATE-30
           )
IS
    v_prcName CONSTANT VARCHAR2(30) := 'FirstCall_list';
    v_retcode INTEGER;
BEGIN
    OPEN p_recordset FOR
         SELECT ORDER_NO, PHONE_NUMBER, CREATE_DATE, CALL_DATE, STATUS, NOTES
           FROM FIRST_CALL_T FC
          WHERE (FC.ORDER_NO     = p_order_no OR p_order_no IS NULL )
            AND (FC.PHONE_NUMBER = p_number OR p_number IS NULL )
            AND (FC.STATUS       = p_status OR p_status IS NULL)
            AND (FC.CREATE_DATE > p_date_from OR p_date_from IS NULL)
         ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- создать новую строку: заказ + телефон
--
PROCEDURE Add_data (
               p_order_no      IN VARCHAR2,
               p_number        IN VARCHAR2,
               p_notes         IN VARCHAR2
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Add_data';
BEGIN
    INSERT INTO FIRST_CALL_T(  
      ORDER_NO, PHONE_NUMBER, CREATE_DATE, CALL_DATE, STATUS, NOTES
    )VALUES(
      p_order_no, p_number, SYSDATE, TO_DATE(NULL), c_ST_OPEN, p_notes
    );
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_order_no='||p_order_no||'p_number='||p_number, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- Ихзменить статус заказа
--
PROCEDURE Change_status (
               p_order_no      IN VARCHAR2,
               p_number        IN VARCHAR2,
               p_status        IN VARCHAR2
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Change_status';
BEGIN
    IF p_order_no IS NULL AND p_number IS NULL THEN
      Pk01_Syslog.Raise_user_exception('p_order_no OR p_number must be given', c_PkgName||'.'||v_prcName);
    END IF;  

    UPDATE FIRST_CALL_T FC 
       SET STATUS = p_status
     WHERE (FC.ORDER_NO     = p_order_no OR p_order_no IS NULL )
       AND (FC.PHONE_NUMBER = p_number OR p_number IS NULL )
    ;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_order_no='||p_order_no||'p_number='||p_number, c_PkgName||'.'||v_prcName );
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -    
-- Удалить данные по заказу
--
PROCEDURE Delete_order (
               p_order_no      IN VARCHAR2
           )
IS
    v_prcName       CONSTANT VARCHAR2(30) := 'Delete_order';
BEGIN
    DELETE FROM FIRST_CALL_T FC
     WHERE FC.ORDER_NO     = p_order_no
    ;
EXCEPTION
    WHEN OTHERS THEN
        Pk01_Syslog.raise_Exception('ERROR, p_order_no='||p_order_no, c_PkgName||'.'||v_prcName );
END;


END PK22_FIRST_CALL;
/
