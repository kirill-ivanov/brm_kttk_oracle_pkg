CREATE OR REPLACE PACKAGE PK1110_OPR_RETRF_GUI
IS

    gc_PkgName CONSTANT varchar2(32) := 'PK1110_OPR_RETRF_GUI';

    /* Функция для создания задания на перепривязку. Возвращает id созданного задания или -1 при ошибке. 
       Входные парметры:
          p_Data_Type - Параметр из: 
                             SELECT bdr_code
                               FROM BDR_TYPES_T b
                              WHERE b.oper = 1
          p_ReCalc_Month - месяц, трафик в котором подлежит пересчету (любая датаэтого месяца)                    
          p_Dest_Period  - отчетный период, в который кладется трафик. Если не задан то берется текущий месяц.
          p_Not_Bill    - 1 - пересчитывать только вызовы, не попавшие в счета.
                          0 (по умолчанию) или любое другое значение - пересчитываются все соединения
          p_Trf_Type    - тип соединений, подлежащих пересчёту
                            D - доход, R - расход, NULL - пересчитывать всё 
                            или указать через запятую кода типов по схеме X07 (инициирование, иниц. на ИП, завершение и т.п.)                
          p_Date_From   - начало периода расчета. Для уточнения периода за который считается тарфик.
                          Если параметр задан, то p_ReCalc_Month игнорируется 
          p_Date_To     - конец периода расчета. Для уточнения периода за который считается тарфик. Используется только
                          при заданном p_Date_From. Если параметр не задан, то берется последний день месяца 
                          даты p_Date_From                      
          p_Msg         - OUT - сообщение (OK - все нормально или информация об ошибке)                
                          
    */
    FUNCTION Create_Job(p_Data_Type    IN varchar2,
                        p_ReCalc_Month IN date,
                        p_Dest_Period  IN date   DEFAULT NULL,
                        p_Not_Bill     IN number DEFAULT NULL,
                        p_Trf_Type     IN varchar2 DEFAULT NULL,
                        p_Date_From    IN date DEFAULT NULL,
                        p_Date_To      IN date   DEFAULT NULL,                    
                        p_Msg          OUT varchar2
                       ) RETURN number;
                       
    /* Функция для создания строк задания на перепривязку - т.е. какой заказ и на какой надо менять.
       Возвращает id созданной строки детализации или -1 в случае ошибки (p_Msg - расшифровка ошибки)
       p_Task_Id       - id заголовка задания
       p_Order_Id      - id заказа (order_id), на который должен лечь трафик
       p_Dest_Bill_Id  - id счета (bill_id), в который должен попасть успешно протарифицированный трафик.
                         Если не указан, то трафик ляжет в текущий (месяца, заданного в заданиий) открытый счет                                            
    */                           
    FUNCTION Create_Job_Detail(p_Task_Id      IN number,
                               p_Order_Id     IN number,
                               p_Dest_Bill_Id IN number,
                               p_Msg          OUT varchar2
                              ) RETURN number;

    -- Процедура, выполняющая пересчет трафика
    PROCEDURE Run_Job(p_Task_Id IN NUMBER);

    -- Запуск задания на пересчет трафика
    FUNCTION Submit_Job(
                     p_otxt    OUT VARCHAR2,    -- текст поясняющий код возврата (описание ошибки и т.д.)
                     p_task_id IN NUMBER
                ) RETURN INTEGER; -- код возврата: c_RET_OK/c_RET_ERR

END PK1110_OPR_RETRF_GUI;
/
CREATE OR REPLACE PACKAGE BODY PK1110_OPR_RETRF_GUI
IS

    gc_DATE_END CONSTANT date := TO_DATE('01.01.2050','dd.mm.yyyy');


/* Функция для создания задания на перепривязку. Возвращает id созданного задания или -1 при ошибке. 
   Входные парметры:
      p_Data_Type - Параметр из: 
                         SELECT bdr_code
                           FROM BDR_TYPES_T b
                          WHERE b.oper = 1
      p_ReCalc_Month - месяц, трафик в котором подлежит пересчету (любая датаэтого месяца)                    
      p_Dest_Period  - отчетный период, в который кладется трафик. Если не задан то берется текущий месяц.
      p_Not_Bill    - 1 - пересчитывать только вызовы, не попавшие в счета.
                      0 (по умолчанию) или любое другое значение - пересчитываются все соединения
      p_Trf_Type    - тип соединений, подлежащих пересчёту
                        D - доход, R - расход, NULL - пересчитывать всё 
                        или указать через запятую кода типов по схеме X07 (инициирование, иниц. на ИП, завершение и т.п.)                
      p_Date_From   - начало периода расчета. Для уточнения периода за который считается тарфик.
                      Если параметр задан, то p_ReCalc_Month игнорируется 
      p_Date_To     - конец периода расчета. Для уточнения периода за который считается тарфик. Используется только
                      при заданном p_Date_From. Если параметр не задан, то берется последний день месяца 
                      даты p_Date_From                      
      p_Msg         - OUT - сообщение (OK - все нормально или информация об ошибке)                
                      
*/
FUNCTION Create_Job(p_Data_Type    IN varchar2,
                    p_ReCalc_Month IN date,
                    p_Dest_Period  IN date   DEFAULT NULL,
                    p_Not_Bill     IN number DEFAULT NULL,
                    p_Trf_Type     IN varchar2 DEFAULT NULL,
                    p_Date_From    IN date DEFAULT NULL,
                    p_Date_To      IN date   DEFAULT NULL,                    
                    p_Msg          OUT varchar2
                   ) RETURN number
IS

    l_Result      number;
    l_BDR_Type_Id number;
    l_Trf_Type    varchar2(16);
    l_Date_From   date;
    l_Date_To     date;
    l_Dest_Period date;
    
    PERIOD_NOT_CORRECT EXCEPTION;
    
BEGIN

   -- определяем id типа оператора
    p_Msg := 'Тип оператора задан неправильно.'; -- ошибка если по no_data_found выйдет
   
    SELECT b.bdr_type_id
      INTO l_BDR_Type_Id
      FROM BDR_TYPES_T b
     WHERE b.oper = 1
       AND b.bdr_code = p_Data_Type;

   -- Определяем тип трафика, который надо пересчитывать
    IF p_Trf_Type IN ('D','R') THEN
    
        l_Trf_Type := pk114_items.Get_List_BDR_Types(p_Data_Type => p_Data_Type,
                                                     p_Side      => NULL,
                                                     p_In_Out    => p_Trf_Type -- D - доход, R - расход
                                                    ); 
        
    ELSIF p_Trf_Type IS NULL THEN
        
        l_Trf_Type := pk114_items.Get_List_BDR_Types(p_Data_Type, NULL, 'D')  -- доход, расход
                    || ',' || pk114_items.Get_List_BDR_Types(p_Data_Type, NULL, 'R');  -- расход
    ELSE
    
        l_Trf_Type := p_Trf_Type;
       
    END IF;                                                     

   -- определяем период, за который расчитывается трафик
    IF p_Date_From IS NULL THEN
    
        l_Date_From := TRUNC(p_ReCalc_Month,'mm');
        l_Date_To   := LAST_DAY(l_Date_From) + INTERVAL '0 23:59:59' DAY TO SECOND;
        
    ELSE
    
        -- проверка корректности введенного периода
        IF p_Date_From < NVL(p_Date_To, gc_DATE_END) THEN
            RAISE PERIOD_NOT_CORRECT;
        END IF;
            
        l_Date_From := p_Date_From;
        l_Date_To   := NVL(p_Date_To, LAST_DAY(TRUNC(l_Date_From)) + INTERVAL '0 23:59:59' DAY TO SECOND);
        
    END IF;    


    l_Dest_Period := NVL(p_Dest_Period, SYSDATE);
    
    -- получаем id отчетного периода (ищется по дате только среди открытых)
    p_Msg := 'Отчетный период не задан или закрыт.'; -- ошибка если по no_data_found выйдет
    
    SELECT period_id
      INTO l_Result
      FROM period_t p
     WHERE p.period_id = pk00_const.Get_Period_Id(l_Dest_Period)
       AND p.close_rep_period IS NULL;

    p_Msg := 'Insert new job';

    INSERT INTO PIN.Q00_RETRF_JOB (
           TASK_ID, CREATE_DATE, DATA_TYPE, REP_PERIOD,
           DATE_FROM, DATE_TO, NOT_BILL, Opr_Trf_Type,
           START_TIME, END_TIME, STATUS) 
    VALUES (PIN.SQ_RETRF_TASK_ID.NEXTVAL, SYSDATE, l_BDR_Type_Id, l_Dest_Period,
            l_Date_From, l_Date_To, p_Not_Bill, l_Trf_Type,
            NULL, NULL, 'Формирование задания')
    RETURN task_id INTO l_Result;

    COMMIT;

    p_Msg := 'OK';

    RETURN l_Result;

EXCEPTION
    WHEN no_data_found THEN
        RETURN -1;
        
    WHEN PERIOD_NOT_CORRECT THEN
        p_Msg := 'Период расчета задан неправильно.'; 
        RETURN -1;     

END Create_Job;
             
/* Функция для создания строк задания на перепривязку - т.е. какой заказ и на какой надо менять.
   Возвращает id созданной строки детализации или -1 в случае ошибки (p_Msg - расшифровка ошибки)
   p_Task_Id       - id заголовка задания
   p_Order_Id      - id заказа (order_id), на который должен лечь трафик
   p_Dest_Bill_Id  - id счета (bill_id), в который должен попасть успешно протарифицированный трафик.
                     Если не указан, то трафик ляжет в текущий (месяца, заданного в заданиий) открытый счет                                            
*/                           
FUNCTION Create_Job_Detail(p_Task_Id      IN number,
                           p_Order_Id     IN number,
                           p_Dest_Bill_Id IN number,
                           p_Msg          OUT varchar2
                          ) RETURN number
IS

    l_Result      number;
    l_Rep_Period  date;
    l_Bill_Status PIN.BILL_T.BILL_STATUS%TYPE;

    BILL_IS_CLOSED EXCEPTION;

BEGIN

    IF p_Dest_Bill_Id > 0 THEN
       -- 
       -- Если задан счет, то проверяем его наличие и корректность
       ---
         
        -- ищем id периода
        p_Msg := 'Ошибка поиска заголовка задания.'; -- вернется ошибка если no_data_found
       
        SELECT rep_period
          INTO l_Rep_Period
          FROM q00_retrf_job
         WHERE task_id = p_Task_Id; 

        -- проверяем счет и его статус (открыт/закрыт)
        p_Msg := 'Указанный счет не существует, не принадлежит заданному заказу '
                 || 'или находится в отчетном периоде, отличном от заданного в задании.'; -- вернется ошибка если no_data_found 
        
        SELECT b.bill_status
          INTO l_Bill_Status 
          FROM bill_t b,
               order_t o
          WHERE b.rep_period_id = pk00_const.Get_Period_Id(l_Rep_Period)
            AND b.bill_id  = p_Dest_Bill_Id
            AND b.account_id = o.account_id 
            AND o.order_id = p_Order_Id;        
        
        IF l_Bill_Status NOT IN (pk00_const.c_BILL_STATE_OPEN)
        THEN
            RAISE BILL_IS_CLOSED;
        END IF;
           
       -- +++++++++++++++++++++++++++++++++++++++++++++       
       -- Проверка периода
       
        
        IF pk00_const.Get_Period_Id(l_Rep_Period) != l_Result -- и заданный р/период не совпадает с периодом счета
        THEN   
           -- ошибка
            p_Msg := 'Отчетные периоды счета и задания не совпадают.';
            RAISE no_data_found;
            
        END IF;                  
           
    END IF;                  

    p_Msg := NULL;

    INSERT INTO PIN.Q01_RETRF_JOB_DETAIL (
           Q01_ID, TASK_ID, BILL_ID, 
           ORDER_ID_NEW) 
    VALUES (PIN.SQ_RETRF_JOB_ID.NEXTVAL, p_Task_Id, p_Dest_Bill_Id,
            p_Order_Id)
    RETURN q01_id INTO l_Result;

    p_Msg := 'OK';
    
    COMMIT;

    RETURN l_Result;

EXCEPTION
    WHEN no_data_found THEN
        
        RETURN -1;

    WHEN BILL_IS_CLOSED THEN
        p_Msg := 'Заданный счет закрыт.';
        RETURN -1;

END Create_Job_Detail;


-- ==================================================================================================
-- Процедура, выполняющая пересчет трафика
PROCEDURE Run_Job(p_Task_Id IN NUMBER)
IS
    lc_Prc_Name  CONSTANT VARCHAR2(32) := 'Run_Job';
    lr_Task      Q00_RETRF_JOB%ROWTYPE;
    l_Data_Type   VARCHAR2(16);
    l_Loc        BOOLEAN := FALSE;
    l_MNMG       BOOLEAN := FALSE; 
    l_TrfBDR     BOOLEAN := FALSE;
    
    NO_TASK_FOUND EXCEPTION;
    
BEGIN
    -- сохраняем отчет
    pk01_syslog.Write_Msg(p_Msg => 'Start. Task_id='||p_Task_Id, 
                          p_Src => gc_PkgName || '.' || lc_Prc_Name);
                          
    --
    -- фиксируем в задании дату начала расчетов
    UPDATE Q00_RETRF_JOB
       SET start_time = SYSDATE
     WHERE task_id = p_Task_Id
    RETURNING TASK_ID, DATE_FROM, DATE_TO, DATA_TYPE, CREATE_DATE, 
              START_TIME, END_TIME, NOT_BILL, STATUS, REP_PERIOD, ELAPS_PERC, NOTE, OPR_TRF_TYPE   
         INTO lr_Task; -- заодно сразу получаем что делать надо
     
    IF SQL%ROWCOUNT = 0 THEN
       RAISE NO_TASK_FOUND;
    END IF;
    
    COMMIT; -- фиксируем update
    
    SELECT bdr_code
      INTO l_Data_Type
      FROM BDR_TYPES_T b 
     WHERE bdr_type_id = lr_Task.Data_Type;
    
    pk1110_opr_retrf.ReBind_Opr_Orders(
                            p_Data_Type  => l_Data_Type,
                            p_Date_From  => lr_Task.date_from,
                            p_Date_To    => lr_Task.date_to,
                            p_Task_Id    => lr_Task.task_id,
                            p_Rep_Period => lr_Task.rep_period,
                            p_LOG        => FALSE,
                            p_Test_Tbl   => NULL,
                            p_Load_Res   => FALSE,
                            p_Load_Items => TRUE,
                            p_Chunk      => 'MONTH'
                           );    
    
    -- фиксируем в задании дату окончания расчетов и новый статус
    UPDATE Q00_RETRF_JOB
       SET end_time = SYSDATE,
           status   = 'Выполнено'
     WHERE task_id = p_Task_Id;
    
    -- сохраняем отчет
    pk01_syslog.Write_Msg(p_Msg => 'Stop. Task_id='||p_Task_Id, p_Src => gc_PkgName || '.' || lc_Prc_Name);
    --    
    COMMIT;
    
EXCEPTION    
    WHEN NO_TASK_FOUND THEN
        pk01_syslog.Write_Msg(p_Msg => 'Задание = '||p_Task_Id ||
                                       ' не найдено.', 
                          p_Src => gc_PkgName || '.' || lc_Prc_Name,
                          p_Level => pk01_syslog.l_Warn);         
    WHEN OTHERS THEN
        pk01_syslog.Insert_Error(p_Src => gc_PkgName || '.' || lc_Prc_Name);
END Run_Job;


-- Запуск задания на пересчет трафика
FUNCTION Submit_Job(
                 p_otxt    OUT VARCHAR2,    -- текст поясняющий код возврата (описание ошибки и т.д.)
                 p_task_id IN NUMBER
            ) RETURN INTEGER -- код возврата: c_RET_OK/c_RET_ERR
IS
    lc_Prc_Name CONSTANT VARCHAR2(32) := 'Submit_Job';
    l_Job       BINARY_INTEGER;
    l_Task_Type NUMBER;
BEGIN
    -- 
    pk01_syslog.Write_Msg(p_Msg => 'Start. Task_id='||p_Task_Id, p_Src => gc_PkgName || '.' || lc_Prc_Name);
    --
    DBMS_JOB.SUBMIT(l_Job, 'PIN.Pk111_ReTrf_GUI.Run_Job(' || p_Task_Id || ');');
        
    COMMIT; -- запускаем Job
    
    p_otxt := 'ОК'|| ' id='||p_task_id; 
    
    RETURN pk00_const.c_RET_OK; 
EXCEPTION    
    WHEN OTHERS THEN
    
        IF p_otxt IS NULL THEN
           p_otxt := SQLERRM;
        END IF;
        
        pk01_syslog.Insert_Error(p_Src => gc_PkgName || '.' || lc_Prc_Name);
        
        RETURN pk00_const.c_RET_ER;
        
END Submit_Job;

END PK1110_OPR_RETRF_GUI;
/
