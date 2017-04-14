CREATE OR REPLACE PACKAGE Pk1110_Opr_ReTrf AS

    gc_PkgName    CONSTANT varchar2(30) := 'Pk1110_Opr_ReTrf'; 

    gc_SRC_AUDIT  CONSTANT NUMBER := 0; -- данные для перерасчета брать из таблиц аудита
    gc_ALL_ERRORS CONSTANT NUMBER := -1; -- переподобрать все ошибки    
    gc_NEW_CDR    CONSTANT NUMBER := -2; -- подобарть только все новые CDR-ы, т.е. которые еще не попадали в расчеты

    gc_SPB_Orders   CONSTANT NUMBER := 0; -- Операторы коммутатора СПб
    gc_NovTk_Orders CONSTANT NUMBER := 1; -- клиентская сторона А, таблица MDV.Z03_ZONE_CDR 

    
    /* ------------------------------------------------------------------------------   
      Процедура для перепривязки л/с клиентов (таблица T03_MMTS_CDR) по стороне А
      Входные параметры:
         p_Data_Type - тип данных (SPB, NOVTK)
         p_Side      - по стороне А или B тарификация      
         p_Date_From - дата начала тарифицируемого периода 
         p_Date_To   - дата конца тарифицируемого периода
         p_Task_Id - источник данных для перетарификации
              -2 (c_NEW_CDR) - подбор только новых CDR-ов 
              -1 (c_ALL_ERRORS) - перетарифицировать все ошибки (ошибки привязки)
              0 (c_SRC_AUDIT) - искать по таблицам аудита все изменения с начала заданного периода. Все 
                         текущие данные по которым найдены какие-либо изменения пересчитываются.     
              >0 - Код Task_Id из таблицы Q01_RETRF_JOB_DETAIL. В этой таблице указываются параметры, по которым 
           подбираются данные для перетарификаци. 
             В задании на перепривязку могут быть заданы: 
                 sw_name - коммутатор (поле Q01_RETRF_JOB_DETAIL.SW_ID). Т.е. если данный параметр указан, то берутся только
              соединения заданного коммутатора. Если NULL - то все коммутаторы.   
                 tg - входящая ТГ (поле Q01_RETRF_JOB_DETAIL.TG). Т.е. если данный параметр указан, то берутся только
              соединения, имеющие входящую TG_IN которая указана. Если NULL - то любые TG_IN.   
                 Abn_A  - оригинальный номер Abn_A (поле Q01_RETRF_JOB_DETAIL.ORIG_NUM_A).          
                 sub_id - текущий id лицевого счета (поле Q01_RETRF_JOB_DETAIL.SUB_ID). Т.е. если данный параметр указан, 
              то берутся только соединения, уже привязанные к заданному л/с или не привязанные ни к какому другому л/с
              (имеющие значение в поле л/счтета NULL, т.е. еще ни разу не привязывались по какой-либо причине или 
              отрицательное значение, т.е. была ошибка привязки). Если NULL - то любые CDR-ы без учета текущего л/с       
                 sub_id_new - id лицевого счета, который должен получиться в рез-те пересчета (поле Q01_RETRF_JOB_DETAIL.SUB_ID_NEW).
              Если новый л/с будет не совпадать с указанным, то соединение будет проигнорировано. Если стоит NULL, то рез-тат
              может быть любой. Если число < 0, то остаются только записи, которые не привязались ни к какому л/с.
                 ПРИМЕЧАНИЕ: заданием парметров sub_id и sub_id_new осуществялется перенос трафика с одного л/с (sub_id)
              на другой (sub_id_new)
                 ОСОБЫЙ СЛУЧАЙ: в задании задано sub_id = sub_id_new. Считаем что полностью хотят пересчитать
             такого клиента и в этом случае CDR-ы, которые были к привязаны к заданному коду и в рез-те расчетов в 
             соотв. с текущими данными стали принадлежать другим клиентам, получат в кач-ве кода клиента -11, чтобы
             избежать изменения уже существующих счетов этих клиентов.        
         p_Rep_Period - расчетный период (всегда будет переназначен ей первый день месяца). 
                   Определяет в какую партицию BDR-ов ляжет трафик. Если NULL, то будет = LOCAL_TIME (время соединения)     
         p_LOG  - FALSE - не писать логи что изменилось
                  TRUE  - писать логи что изменилось 
         p_Test_Tbl - тестовая или еще какая тарификация если не NULL. Все данные кладутся в таблицу, указанную 
                 в этом параметре (таблица должна иметь формат BDR). Так же при значении параметры p_MNMG и p_BILL 
                 игнорируются (определяется как FALSE) и никакие данные в таблице CDR не обновляются              
         p_Load_Res - TRUE - загрузить новые данные по ресурсам.
                      FALSE - для расчетов используются текущие данные   
         p_Load_Items - TRUE - пересчитывать item-ы. Параметр, используемый по-умолчанию. Item-ы пересчитываются в соответсвии с новыми
                                    результатами   
                        FALSE - не пересчитывать item-ы
         p_Chunk      - период, который берется для расчета за один проход (DAY, WEEK, MONTH)                                                                                                                              
    */
    PROCEDURE ReBind_Opr_Orders(p_Data_Type  IN varchar2,
                                p_Date_From  IN DATE     DEFAULT TRUNC(SYSDATE - 1, 'mm'),
                                p_Date_To    IN DATE     DEFAULT TRUNC(SYSDATE) - 1/86400,
                                p_Task_Id    IN NUMBER   DEFAULT 0,
                                p_Rep_Period IN DATE     DEFAULT NULL,
                                p_LOG        IN BOOLEAN  DEFAULT FALSE,
                                p_Test_Tbl   IN VARCHAR2 DEFAULT NULL,
                                p_Load_Res   IN BOOLEAN  DEFAULT FALSE,
                                p_Load_Items IN BOOLEAN  DEFAULT TRUE,
                                p_Chunk      IN varchar2 DEFAULT 'MONTH'
                               );

    /* ------------------------------------------------------------------------------   
      Процедура для перетарификации соединений (таблица B01_BDR)
      Входные параметры:
         p_Data_Type - тип данных, которые надо перетарифицировать (SPB, NOVTK)
         p_Date_From - дата начала тарифицируемого периода (start_time)
         p_Date_To   - дата конца тарифицируемого периода (start_time)
             -- p_Month, p_Bill - параметры были исключены что бы в данной процедуре не мумукаться с поддержкой таблицы линков.
             --                  все переносы только через перепривязку.      
         p_Source - источник данных для перетарификации
                    -4 - перетарифицировать все соединения у которых не нашли условия тарификации     
                    -1 (c_ALL_ERRORS) - перетарифицировать все ошибки (ошибки тарификации)
                    > 0 - подготовить данные по клиентам/номерам, заданным в таблице Q03_RETRF. В кач-ве значения Task_Id
                    0 - искать по таблицам аудита
         p_BDR_Type - тип BDR-ов, которые надо перетарифицировать. Если NULL, то перетарифицируются все.
         p_Load_Res - загрузить новые данные по ресурсам из схемы billing.      
         p_Log      - в таблицу с логами кладутся кода sub_id_mnmg записи, 
                      у которых изменились какие-либо данные    
         p_Load_Items - TRUE - пересчитывать item-ы. Параметр, используемый по-умолчанию. Item-ы пересчитываются в соответсвии с новыми
                                    результатами   
                        FALSE - не пересчитывать item-ы                                                                                                                                                
         p_Chunk      - период, который берется для расчета за один проход (DAY, WEEK, MONTH)
    */ 
    PROCEDURE ReTrfBDR(p_Data_Type  IN varchar2,
                       p_Date_From  IN DATE     DEFAULT TRUNC(SYSDATE - 1, 'mm'),
                       p_Date_To    IN DATE     DEFAULT TRUNC(SYSDATE) - 1/86400,
                       p_Task_Id    IN NUMBER   DEFAULT 0,
                       p_Rep_Period IN DATE     DEFAULT NULL,
                       p_Load_Res   IN BOOLEAN  DEFAULT FALSE,
                       p_Log        IN BOOLEAN  DEFAULT FALSE,
                       p_Load_Items IN BOOLEAN  DEFAULT TRUE,
                       p_Chunk      IN varchar2 DEFAULT 'MONTH',
                       p_Test_BDR_Table IN varchar2 DEFAULT NULL
                      );

END Pk1110_Opr_ReTrf;
/
CREATE OR REPLACE PACKAGE BODY Pk1110_Opr_ReTrf AS

    gc_DATE_END CONSTANT date := TO_DATE('01.01.2050','dd.mm.yyyy');

    CLOSED_PERIOD EXCEPTION; -- попытка перетарификации по таблицам аудита в закрытом периоде

    -- источник критерия поиска CDR-ов на перепривязку
    gc_New_Data     CONSTANT number := 0;
    gc_Q01_RETRF    CONSTANT number := 2; -- заданы данные на перетрификацию (не аудит)
    gc_RSX50_TG     CONSTANT number := 3;
    gc_SIP_PHNUM    CONSTANT number := 4;
    gc_SWTG         CONSTANT number := 5;
     
    
/*     
    gc_ORDER_PHONE       number := 1;
        
    TYPE t_Date     IS TABLE OF DATE;
    TYPE t_Num      IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
*/  

--
-- Функция для определение данных, по которым ищутся CDR подлежащие перетарификации
-- Входные параметры:
--    p_Date_From - дата с которой учитываются изменения
--    p_Task_Id   - источник данных для перетарификации
--                      0 - таблицы аудита
--                     >0 - id здания на перетарификацию
--    p_Client_Type - тип л/счетов, которые хотят перепривязать
--                     0 - Sub_A, 1 - Op_A, 2 - Op_B                         
-- Возвращает кол-во найденных данных или при ошибе значение < 0 (код ошибки)
FUNCTION Get_Resources_Rebind(p_Data_Type   IN varchar2,
                              p_Side        IN varchar2,
                              p_Date_From   IN DATE,
                              p_Date_To     IN DATE    DEFAULT gc_DATE_END,
                              p_Task_Id     IN NUMBER  DEFAULT 0,
                              p_Add_Data    IN BOOLEAN DEFAULT FALSE
                             ) RETURN INTEGER
IS
    v_prcName CONSTANT VARCHAR2(20) := 'Get_Resorces_Rebind'; 
    l_Result  INTEGER;
    l_Network_Id number;

BEGIN
    
    -- получаем id сети, для которой ищутся данные
    SELECT network_id
      INTO l_Network_Id
      FROM bdr_types_t
     WHERE bdr_code = UPPER(p_Data_Type);

    
    IF p_Add_Data = FALSE THEN
        DELETE FROM TMP01_REBIND_CDR;
    END IF;        
        
    IF p_Task_Id = gc_SRC_AUDIT AND UPPER(p_Data_Type) IN ('SPB', 'NOVTK') THEN
        -- пересчитываются все данные л/с клиентов стороны А, которые изменились 
         -- с заданного момента (p_Date_From) Данные об изменениях ищутся по таблицам аудита.

        IF UPPER(p_Data_Type) = 'SPB' AND p_Side = 'A' THEN
           -- Определяем данные по номерам SIP, подлежащие пересчету 
            INSERT INTO TMP01_REBIND_CDR (
                   SW_NAME, TG_IN, TG_OUT, 
                   DATE_TO, SOURCE_TYPE, ORDER_TYPE,
                   ABN_A, CALLER_NUMBER, 
                   ABN_B, ORDER_ID, ERR_CODE, SERVICE_ID, TRF_SIDE)
            SELECT Pk120_Bind_Operators.gc_SIP_SW, Pk120_Bind_Operators.gc_SIP_TG, NULL, 
                   TO_DATE(a.add_value,'dd.mm.yyyy hh24:mi:ss'), gc_RSX50_TG, p_Data_Type,
                   Pk120_Bind_Operators.gc_SIP_Main_Ph, a.num_value caller_number, 
                   NULL, NULL, NULL, NULL, p_Side
              FROM TABLE(CAST(mdv.pck_tools.DistribRange(CURSOR(
                                             SELECT a.phone_from, a.phone_to, TO_CHAR(MAX(a.date_save),'dd.mm.yyyy hh24:mi:ss')
                                               FROM RS50_ORDER_SIP_PHONES_AUDIT a
                                              WHERE a.DATE_SAVE >= p_Date_From  
                                              GROUP BY a.phone_from, a.phone_to)
                         ) AS MDV.NUMBER_COLL)) a;
        END IF;                              
               
       -- данные по разделяемости ТГ 
        INSERT INTO TMP01_REBIND_CDR (
               SW_NAME, 
               TG_IN, 
               TG_OUT, 
               DATE_TO, SOURCE_TYPE, ORDER_TYPE,
               ABN_A, ABN_B, ORDER_ID, ERR_CODE, SERVICE_ID, TRF_SIDE) 
        SELECT s.switch_code, 
               DECODE(p_Side,'A',a.trunkgroup,NULL) tg_in,
               DECODE(p_Side,'B',a.trunkgroup,NULL) tg_out, 
               MAX(a.date_save), gc_SWTG, p_Data_Type,
               NULL, NULL, NULL, NULL, NULL, p_Side
          FROM PIN.RSX07_SWTG_AUDIT a,
               PIN.SWITCH_T s
         WHERE s.network_id = l_Network_Id
           AND a.switch_id = s.Switch_Id
           AND a.date_save >= p_Date_From -- только эти записи могли принимать участие в привязке клиентов
           AND a.date_to >= p_Date_From
         GROUP BY s.switch_code, a.trunkgroup;        
        
       -- данные по ТГ
        INSERT INTO TMP01_REBIND_CDR (
               SW_NAME, 
               TG_IN, 
               TG_OUT, 
               DATE_TO, SOURCE_TYPE, ORDER_TYPE,
               ABN_A, 
               ABN_B, 
               ORDER_ID, ERR_CODE, SERVICE_ID, TRF_SIDE) 
        SELECT s.switch_code, 
               DECODE(p_Side,'A',a.trunkgroup,NULL) tg_in,
               DECODE(p_Side,'B',a.trunkgroup,NULL) tg_out, 
               MAX(a.date_save), gc_SWTG, p_Data_Type,
               DECODE(p_Side,'A',a.trunkgroup_no,NULL) abn_a,
               DECODE(p_Side,'B',a.trunkgroup_no,NULL) abn_b, 
               NULL, NULL, NULL, p_Side
          FROM PIN.RSX07_ORDER_SWTG_AUDIT a,
               PIN.SWITCH_T s
         WHERE s.network_id = l_Network_Id
           AND a.switch_id = s.Switch_Id
           AND a.date_save >= p_Date_From -- только эти записи могли принимать участие в привязке клиентов
           AND a.date_to >= p_Date_From
           AND NOT EXISTS (SELECT 1 -- эти ТГ, загруженные на пред. шаге и так будут пересчитаны целиком
                             FROM TMP01_REBIND_CDR t
                            WHERE t.source_type = gc_SWTG
                              AND t.trf_side = p_Side   
                              AND t.sw_name = s.switch_code
                              AND DECODE(p_Side,'A',t.tg_in,'B',t.tg_out,NULL) = a.trunkgroup
                           )        
         GROUP BY s.switch_code, a.trunkgroup, a.trunkgroup_no; 
         
       -- Условие для подбора всех новых соединений
        INSERT INTO TMP01_REBIND_CDR (
               SW_NAME, 
               TG_IN, TG_OUT, DATE_TO, SOURCE_TYPE, ORDER_TYPE,
               ABN_A, ABN_B, ORDER_ID, ERR_CODE, SERVICE_ID, TRF_SIDE)     
        SELECT s.switch_code,
               NULL, NULL, TO_DATE('01.01.2050','dd.mm.yyyy'), gc_NEW_Data, UPPER(p_Data_Type),
               NULL, NULL, 0, NULL, NULL, p_Side
          FROM switch_t s
         WHERE s.network_id = l_Network_id;
        
    ELSIF p_Task_Id != gc_SRC_AUDIT AND UPPER(p_Data_Type) IN ('SPB', 'NOVTK') THEN    

        IF UPPER(p_Data_Type) = 'SPB' AND p_Side = 'A' THEN
        
           -- Загружаем все номера номера SIP, которые могут быть у данного л/счета 
            INSERT INTO TMP01_REBIND_CDR (
                   SW_NAME, TG_IN, TG_OUT, 
                   DATE_TO, SOURCE_TYPE, ORDER_TYPE,
                   ABN_A, CALLER_NUMBER, 
                   ABN_B, 
                   ORDER_ID, 
                   ERR_CODE, SERVICE_ID,
                   TRF_SIDE)
            SELECT Pk120_Bind_Operators.gc_SIP_SW, NVL(q.tg, Pk120_Bind_Operators.gc_SIP_TG), NULL, 
                   gc_DATE_END, gc_SIP_PHNUM, UPPER(p_Data_Type),
                   Pk120_Bind_Operators.gc_SIP_Main_Ph, a.num_value caller_number,
                   q.abn_b, 
                   q.order_id, -- клиент, чьи записи пересчитываются
                   NULL, NULL,
                   p_Side
              FROM TABLE(CAST(mdv.pck_tools.DistribRange(CURSOR(
                                             SELECT r.phone_from, r.phone_to, TO_CHAR(r.order_id)
                                               FROM ORDER_SIP_PHONES_T r,
                                                    Q01_RETRF_JOB_DETAIL q 
                                              WHERE q.task_id = p_Task_Id 
                                                AND q.order_id_new IS NOT NULL
                                                AND q.order_id_new = r.order_id
                                                AND (q.tg = Pk120_Bind_Operators.gc_SIP_TG OR q.tg IS NULL)
                                                AND (q.abn_a BETWEEN r.phone_from AND r.phone_to
                                                      OR
                                                     q.abn_a IS NULL 
                                                    ) 
                                              GROUP BY phone_from, phone_to, TO_CHAR(r.order_id))
                         ) AS MDV.NUMBER_COLL)) a,
                   Q01_RETRF_JOB_DETAIL q
             WHERE q.task_id = p_Task_Id 
               AND q.order_id_new IS NOT NULL
               AND q.order_id_new = TO_NUMBER(a.add_value)
               AND (q.abn_a = a.num_value 
                     OR
                    q.abn_a IS NULL);                         

        END IF;                                         
           
       -- данные по ТГ
        INSERT INTO TMP01_REBIND_CDR (
               SW_NAME, 
               TG_IN, 
               TG_OUT, 
               DATE_TO, SOURCE_TYPE, ORDER_TYPE,
               ABN_A, ABN_B, ORDER_ID, ERR_CODE, SERVICE_ID,
               TRF_SIDE) 
        SELECT s.Switch_Code sw_name, 
               DECODE(p_Side,'A',a.trunkgroup,NULL) tg_in,
               DECODE(p_Side,'B',a.trunkgroup,NULL) tg_out, 
               MAX(NVL(a.date_to,gc_DATE_END)), gc_SWTG, UPPER(p_Data_Type),
               NULL, NULL, NULL, NULL, NULL,
               p_Side
          FROM Q01_RETRF_JOB_DETAIL q,
               PIN.X07_ORDER_SWTG_T a,
               SWITCH_T s
         WHERE s.network_id = l_Network_Id
           AND a.switch_id = s.Switch_Id
           AND q.task_id = p_Task_Id 
           AND q.order_id_new IS NOT NULL
           AND q.abn_a IS NULL
           AND NVL(a.date_to, gc_DATE_END) >= p_Date_From
           AND a.date_from <= p_Date_To
           AND q.order_id_new = a.order_id
        GROUP BY s.Switch_Code, a.trunkgroup;
                   
       -- вносим все остальные варианты, по которым должны быть подобраны CDR  
        MERGE INTO PIN.TMP01_REBIND_CDR q1    
        USING (SELECT q.sw_name, TO_NUMBER(q.tg) tg, q.abn_a, q.abn_b, q.order_id,
                      q.err_code, q.dial_number  
                 FROM Q01_RETRF_JOB_DETAIL q 
                WHERE q.task_id = p_Task_Id
                  AND q.order_id_new IS NULL  -- по sub_id_new определены исходные данные на предыдущих шагах
              ) t
           ON (NVL(q1.sw_name, '-1')     = NVL(t.sw_name, '-1') AND  
               NVL(q1.tg_in, -1)         = NVL(DECODE(p_Side,'A',t.Tg,NULL), -1) AND
               NVL(q1.tg_out, -1)        = NVL(DECODE(p_Side,'B',t.Tg,NULL), -1) AND
               NVL(q1.abn_a, '-1')       = NVL(t.abn_a, '-1') AND  
               NVL(q1.abn_b, '-1')       = NVL(t.abn_b, '-1') AND
               NVL(q1.dial_number, '-1') = NVL(t.dial_number, '-1') AND
               NVL(q1.order_id, -1)      = NVL(t.Order_Id, -1) AND
               NVL(q1.order_type, -1)    = UPPER(p_Data_Type)
              )
         WHEN MATCHED THEN UPDATE 
          SET q1.date_to = gc_DATE_END               
         WHEN NOT MATCHED THEN INSERT
         VALUES(t.Sw_Name, 
                DECODE(p_Side,'A',t.Tg,NULL), -- tg_in 
                DECODE(p_Side,'B',t.Tg,NULL), -- tg_out 
                t.abn_a, t.abn_b, 
                gc_DATE_END, -- дата, которая вносится в поле date_to
                     -- (ставим c_DATE_END чтобы любые данные с гарантией попали под пересчет)
                gc_Q01_RETRF, -- Source_Type
                UPPER(p_Data_Type), -- order_type
                t.Order_Id,                
                NULL, -- err_code
                NULL, --service_id
                t.dial_number,
                NULL,
                p_Side 
               );             
        
    END IF;       
        
    SELECT COUNT(1) INTO l_Result
      FROM TMP01_REBIND_CDR
     WHERE trf_side = p_Side;
            
    IF l_Result = 0 THEN  
        Pk01_Syslog.Write_Msg(p_Msg => 'Данные для перепривязки не найдены. ' ||
                                       'Сторона: ' || p_Side ||
                                       ', Task_ID: ' || TO_CHAR(p_Task_Id), 
                              p_Src => gc_PkgName || '.' || v_prcName);    
    ELSE                              
        Pk01_Syslog.Write_Msg(p_Msg => 'Queue was prepared successfully. ' ||
                                       'Side: ' || p_Side ||
                                       ', Task_ID: ' || TO_CHAR(p_Task_Id) || ', Count: ' || TO_CHAR(l_Result), 
                              p_Src => gc_PkgName || '.' || v_prcName);    
    END IF;
    
    RETURN l_Result;
    
/*EXCEPTION
    WHEN OTHERS THEN
        PK01_SYSLOG.Write_Error(p_Src => gc_PkgName || '.' || v_prcName);
        RETURN SQLCODE;    */
END Get_Resources_Rebind;


/*
   Функция для определения CDR табл. MDV.T03_MMTS_CDR, подлежащих перерасчету. Данные кладутся в temporary table. 
   Ф-ция возвращает кол-во вставленных данных
     p_Date_From - дата начала периода за который ищутся данные
     p_Date_To   - дата конца периода за который ищутся данные  
     p_Source - источник данных для перетарификации
               -1 (c_ALL_ERRORS) - перетарифицировать все ошибки (ошибки привязки)
                0 (c_SRC_AUDIT) - искать по таблицам аудита все изменения с начала заданного периода. Все 
                          текущие данные по которым найдены какие-либо изменения пересчитываются.     
               >0 - Код Task_Id из таблицы Q02_ADD_LOAD. В этой таблице указываются параметры, по которым подбираются 
            данные для перетарификаци.
     p_TrfType - тип трафика. Список требуемых значений значений через запятую ('0,7,8,9'). Если передан NULL,
                  то определяется автоматически.

*/      
FUNCTION Get_Opr_CDR_ReBind(p_Data_Type   IN varchar2,
                            p_Date_From   IN DATE,
                            p_Date_To     IN DATE,
                            p_Task_Id     IN NUMBER   DEFAULT gc_SRC_AUDIT,
                            p_CDR_Table   IN VARCHAR2 DEFAULT 'MDV.X03_XTTK_CDR'
                           ) RETURN NUMBER
IS

   l_Queue_Src_Tbl CONSTANT VARCHAR2(32) := 'TMP01_REBIND_CDR';

   l_SQL       VARCHAR2(32500);
   l_Union     VARCHAR2(16);
   l_Count     NUMBER;
   l_IdCursor  NUMBER;
   l_Network_Code pin.network_t.network_code%TYPE;
   l_Network_Id   pin.network_t.network_id%TYPE; 
BEGIN

    l_Network_Code := pin.Get_Network(p_Data_Type,
                                      l_Network_Id);   
 
  ---- ======================================================================================
  ---- Составление запроса для подбора CDR-ов подлежащих тарификации 
   l_SQL := 'WITH RETRF_CDR_TEMP AS ' || CHR(13) ||  
                 '(SELECT /*++ parallel(c 5) */ c.ROWID row_id, c.cdr_id, c.ans_time, ' || CHR(13) || 
                 '      c.sw_name, c.trunk_group_in, c.trunk_group_out, ' ||
                 '      c.norm_a_subs_num abn_a, ' ||  --'      pin.Norm_Ph_Number(''' || l_Network_Code || ''',c.a_subscriber_number) abn_a, ' ||
                 '      c.norm_b_subs_num abn_b, ' ||  --'      pin.Norm_Ph_Number(''' || l_Network_Code || ''',c.b_subscriber_number) abn_b, ' ||
                 '      c.norm_caller_num caller_number, ' || --'      pin.Norm_Ph_Number(''' || l_Network_Code || ''',c.caller_number) caller_number, ' ||
                 '      c.norm_called_num called_number, ' || --'      pin.Norm_Ph_Number(''' || l_Network_Code || ''',c.called_number) called_number, ' ||                 
                 '      c.op_a_order_id, ' ||
                 '      c.op_b_order_id, ' ||
                 '      c.op_a_calc_date, ' ||
                 '      c.op_b_calc_date' ||
                 ' FROM ' || p_CDR_Table || ' c ' || CHR(13) || 
                 'WHERE c.ans_time BETWEEN :l_Date_From AND :l_Date_To ' || CHR(13) ||
                 '  AND c.network_id = :l_Network_Id'  ||
                 ')';
      
   l_SQL := l_SQL ||'SELECT rd, MAX(date_rule) date_rule, MIN(ans_time), cdr_id, 0 Bdr_Type, ' ||
                         -- формируем строку с сторонами, подлежащими перепривязке 
                          '(CASE WHEN MIN(trf_side) = ''A'' AND MAX(trf_side) = ''B'' THEN ''A,B'' ' ||
                          '      WHEN MIN(trf_side) = ''A'' AND MAX(trf_side) = ''A'' THEN ''A''  ' ||
                          '      WHEN MIN(trf_side) = ''B'' AND MAX(trf_side) = ''B'' THEN ''B'' ' ||
                          '      ELSE ''A,B'' ' ||
                          ' END) trf_side ' || CHR(13) ||    
                    ' FROM ( ';

  --- """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
  --- Индивидуальные условия
  ---
    
   -- Подбор всех новых соединений по стороне A
    FOR l_cur IN (SELECT trf_side
                    FROM TMP01_REBIND_CDR b
                   WHERE b.source_type = gc_New_Data
                     AND b.order_type = UPPER(p_Data_Type)
                   GROUP BY trf_side
                 )
    LOOP                   
   
       -- если такие данные есть
        l_SQL := l_SQL || CHR(10) || l_Union || CHR(10) || 
                 '  SELECT /*++ parallel(c 10) */ c.row_id rd, TO_DATE(''01.01.2000'',''dd.mm.yyyy'') date_rule, ' ||
                 '         c.ans_time, c.cdr_id, ' ||
                           '''' || l_cur.trf_side || ''' trf_side ' || CHR(13) ||
                 '    FROM RETRF_CDR_TEMP c ' || CHR(13) ||
                 '   WHERE ' || (CASE 
                                   WHEN l_cur.trf_side = 'A' THEN 
                                      ' c.op_a_calc_date IS NULL '
                                   WHEN l_cur.trf_side = 'B' THEN 
                                      ' c.op_b_calc_date IS NULL ' -- подбор новых соединений
                                 END);

        IF l_Union IS NULL THEN
           -- перед след запросом надо поставить UNION ALL
            l_Union := ' UNION ALL ';
        
        END IF;
        
    END LOOP;             
    
   --
   --- """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""" 
    -- получаем возможные комбинации условий для записей    
    FOR l_cur IN (SELECT sw_name, tg_in, tg_out, abn_a, abn_b, caller_number, order_id, trf_side 
                    FROM (
                          SELECT a.order_type,
                                 NVL2(a.sw_name, 1, 0) sw_name,
                                 NVL2(a.tg_in, 1, 0) tg_in,
                                 NVL2(a.tg_out, 1, 0) tg_out,
                                 NVL2(a.abn_a, 1, 0) abn_a,
                                 NVL2(a.abn_b, 1, 0) abn_b,
                                 NVL2(a.caller_number, 1, 0) caller_number,
                                 SIGN(a.order_id) order_id,
                                 TRF_SIDE
                            FROM TMP01_REBIND_CDR a
                           WHERE a.order_type = UPPER(p_Data_Type)
                             AND a.source_type NOT IN (gc_New_Data)      
                             AND (sw_name       IS NOT NULL OR
                                  tg_in         IS NOT NULL OR
                                  tg_out        IS NOT NULL OR
                                  abn_a         IS NOT NULL OR -- у которых search_type = 2 (для like)
                                  abn_b         IS NOT NULL OR
                                  order_id      IS NOT NULL OR
                                  caller_number IS NOT NULL) 
                          )
                     GROUP BY sw_name, tg_in, tg_out, abn_a, abn_b, caller_number, order_id, trf_side
                 )
    LOOP
    
        l_SQL := l_SQL || CHR(10) || l_Union || CHR(10) ||  
             'SELECT /*++ parallel(c 10) */ ' || CHR(13) || 
             '      c.row_id rd, t.date_to date_rule, c.ans_time, c.cdr_id, t.trf_side ' || CHR(13) ||
             ' FROM RETRF_CDR_TEMP c, ' || CHR(13) ||
                    l_Queue_Src_Tbl || ' t' || CHR(13) ||
             'WHERE t.date_to >= ' || CHR(13) ||
                             (CASE 
                                WHEN l_cur.trf_side = 'A' THEN 
                                   ' NVL(c.op_a_calc_date, TO_DATE(''01.01.2000'',''dd.mm.yyyy'')) '
                                WHEN l_cur.trf_side = 'B' THEN 
                                   ' NVL(c.op_b_calc_date, TO_DATE(''01.01.2000'',''dd.mm.yyyy'')) ' -- подбор новых соединений
                              END) || CHR(10) ||                  
             '  AND ' || (CASE 
                              WHEN l_Cur.order_id = 1 AND l_cur.trf_side = 'A' THEN 
                                  ' t.order_id = c.op_a_order_id '
                              WHEN l_Cur.order_id = 0 AND l_cur.trf_side = 'A' THEN 
                                  ' c.op_a_order_date IS NULL ' -- подбор новых соединений
                              WHEN l_Cur.order_id = -1 AND l_cur.trf_side = 'A' THEN 
                                  ' (c.op_a_order_id IS NULL OR c.op_a_order_id < ''0'') ' -- подбор новых и ошибок  
                              WHEN l_Cur.order_id = 1 AND l_cur.trf_side = 'B' THEN 
                                  ' t.order_id = c.op_b_order_id '
                              WHEN l_Cur.order_id = 0 AND l_cur.trf_side = 'B' THEN 
                                  ' c.op_b_order_date IS NULL ' -- подбор новых соединений
                              WHEN l_Cur.order_id = -1 AND l_cur.trf_side = 'B' THEN 
                                  ' (c.op_b_order_id IS NULL OR c.op_b_order_id < ''0'') ' -- подбор новых и ошибок                                  
                              ELSE 
                                  ' t.order_id IS NULL ' 
                          END) || CHR(13) ||          
             '  AND t.sw_name ' || (CASE l_Cur.sw_name 
                                      WHEN 1 THEN ' = c.Sw_Name '
                                      ELSE ' IS NULL ' 
                                    END) || CHR(13) ||
             '  AND UPPER(t.tg_in) ' || (CASE l_Cur.tg_in 
                                           WHEN 1 THEN ' = UPPER(c.trunk_group_in) '
                                           ELSE ' IS NULL ' 
                                         END) || CHR(13) ||
             '  AND UPPER(t.tg_out) ' || (CASE l_Cur.tg_out 
                                               WHEN 1 THEN ' = UPPER(c.trunk_group_out) '
                                               ELSE ' IS NULL ' 
                                           END) || CHR(13) ||
             '  AND t.abn_a ' || (CASE l_Cur.abn_a 
                                    WHEN 1 THEN ' = c.abn_a '
                                    ELSE ' IS NULL ' 
                                  END) || CHR(13) ||                                        
             '  AND t.caller_number ' || (CASE l_Cur.caller_number 
                                            WHEN 1 THEN ' = c.caller_number '
                                            ELSE ' IS NULL ' 
                                          END) || CHR(13) ||                                  
             '  AND t.abn_b ' || (CASE l_Cur.abn_b 
                                    WHEN 1 THEN ' = c.called_number '
                                    ELSE ' IS NULL ' 
                                  END);           
         
        IF l_Union IS NULL THEN
           -- перед след запросом надо поставить UNION ALL
            l_Union := ' UNION ALL ';
        
        END IF;
             
    END LOOP;                             

   --- <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<    
    
    l_SQL := l_SQL || '    ) GROUP BY rd, cdr_id'; 
    
    DBMS_STATS.GATHER_TABLE_STATS(Ownname => 'PIN',
                                  tabname => l_Queue_Src_Tbl, 
                                  estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
                                  CASCADE => TRUE);    
    
    l_IdCursor := DBMS_SQL.OPEN_CURSOR;
              
    DBMS_SQL.PARSE(C => l_IdCursor,
                   STATEMENT => 'INSERT INTO PIN.TMP02_ROWS_CALC(row_id, date_to, start_time, cdr_id, bdr_type, trf_side) ' || l_SQL,
                   language_flag => DBMS_SQL.NATIVE);
                   
    DBMS_SQL.BIND_VARIABLE(C => l_IdCursor,
                           NAME => 'l_Date_From',
                           VALUE => p_Date_From);
                           
    DBMS_SQL.BIND_VARIABLE(C => l_IdCursor,
                           NAME => 'l_Date_To',
                           VALUE => p_Date_To);
                           
    DBMS_SQL.BIND_VARIABLE(C => l_IdCursor,
                           NAME => 'l_Network_Id',
                           VALUE => l_Network_Id);                           
                           
    l_Count := DBMS_SQL.EXECUTE(l_IdCursor);
    DBMS_SQL.CLOSE_CURSOR(l_IdCursor);    

--    Set_Tbl_Stat('MDV', 'TMP02_ROWS_CALC', l_Count);

--INSERT INTO TMP02_ROWS_CALC_TEST SELECT * FROM TMP02_ROWS_CALC;  
--INSERT INTO TMP01_REBIND_CDR_TEST SELECT * FROM TMP01_REBIND_CDR;      
--INSERT INTO MDV.MS_SQL VALUES (0,l_SQL);
--COMMIT;  
--RAISE NO_DATA_FOUND;  
     
    RETURN l_Count;
/*    
EXCEPTION
    WHEN OTHERS THEN
        IF DBMS_SQL.IS_OPEN(l_IdCursor) = TRUE THEN
            DBMS_SQL.CLOSE_CURSOR(l_IdCursor);
        END IF;
        RAISE;        */
END Get_Opr_CDR_ReBind;

/* ------------------------------------------------------------------------------   
  Процедура для перепривязки л/с клиентов старого биллинга (таблица T03_MMTS_CDR) по стороне А
  Входные параметры:
     p_Data_Type - тип данных (SPB, NOVTK)
     p_Side      - по стороне А или B тарификация
     p_Date_From - дата начала тарифицируемого периода 
     p_Date_To   - дата конца тарифицируемого периода
     p_Task_Id   - источник данных для перетарификации
          -2 (c_NEW_CDR) - подбор только новых CDR-ов 
          -1 (c_ALL_ERRORS) - перетарифицировать все ошибки (ошибки привязки)
          0 (c_SRC_AUDIT) - искать по таблицам аудита все изменения с начала заданного периода. Все 
                     текущие данные по которым найдены какие-либо изменения пересчитываются.     
          >0 - Код Task_Id из таблицы Q01_RETRF_JOB_DETAIL. В этой таблице указываются параметры, по которым 
       подбираются данные для перетарификаци. 
         В задании на перепривязку могут быть заданы: 
             sw_name - коммутатор (поле Q01_RETRF_JOB_DETAIL.SW_ID). Т.е. если данный параметр указан, то берутся только
          соединения заданного коммутатора. Если NULL - то все коммутаторы.   
             tg - входящая ТГ (поле Q01_RETRF_JOB_DETAIL.TG). Т.е. если данный параметр указан, то берутся только
          соединения, имеющие входящую TG_IN которая указана. Если NULL - то любые TG_IN.   
             Abn_A  - оригинальный номер Abn_A (поле Q01_RETRF_JOB_DETAIL.ORIG_NUM_A).          
             sub_id - текущий id лицевого счета (поле Q01_RETRF_JOB_DETAIL.SUB_ID). Т.е. если данный параметр указан, 
          то берутся только соединения, уже привязанные к заданному л/с или не привязанные ни к какому другому л/с
          (имеющие значение в поле л/счтета NULL, т.е. еще ни разу не привязывались по какой-либо причине или 
          отрицательное значение, т.е. была ошибка привязки). Если NULL - то любые CDR-ы без учета текущего л/с       
             sub_id_new - id лицевого счета, который должен получиться в рез-те пересчета (поле Q01_RETRF_JOB_DETAIL.SUB_ID_NEW).
          Если новый л/с будет не совпадать с указанным, то соединение будет проигнорировано. Если стоит NULL, то рез-тат
          может быть любой. Если число < 0, то остаются только записи, которые не привязались ни к какому л/с.
             ПРИМЕЧАНИЕ: заданием парметров sub_id и sub_id_new осуществялется перенос трафика с одного л/с (sub_id)
          на другой (sub_id_new)
             ОСОБЫЙ СЛУЧАЙ: в задании задано sub_id = sub_id_new. Считаем что полностью хотят пересчитать
         такого клиента и в этом случае CDR-ы, которые были к привязаны к заданному коду и в рез-те расчетов в 
         соотв. с текущими данными стали принадлежать другим клиентам, получат в кач-ве кода клиента -11, чтобы
         избежать изменения уже существующих счетов этих клиентов.        
     p_Rep_Period - расчетный период (всегда будет переназначен ей первый день месяца). 
               Определяет в какую партицию BDR-ов ляжет трафик. Если NULL, то будет = LOCAL_TIME (время соединения)     
     p_LOG  - FALSE - не писать логи что изменилось
              TRUE  - писать логи что изменилось 
     p_Test_Tbl - тестовая или еще какая тарификация если не NULL. Все данные кладутся в таблицу, указанную 
             в этом параметре (таблица должна иметь формат BDR). Так же при значении параметры p_MNMG и p_BILL 
             игнорируются (определяется как FALSE) и никакие данные в таблице CDR не обновляются              
     p_Load_Res - TRUE - загрузить новые данные по ресурсам.
                  FALSE - для расчетов используются текущие данные         
     p_Load_Items - TRUE - пересчитывать item-ы. Параметр, используемый по-умолчанию. Item-ы пересчитываются в соответсвии с новыми
                                результатами   
                    FALSE - не пересчитывать item-ы,
     p_Chunk      - период, который берется для расчета за один проход (DAY, WEEK, MONTH)                                                                                                            
*/
PROCEDURE ReBind_Opr_Orders(p_Data_Type  IN varchar2,
                            p_Date_From  IN DATE     DEFAULT TRUNC(SYSDATE - 1, 'mm'),
                            p_Date_To    IN DATE     DEFAULT TRUNC(SYSDATE) - 1/86400,
                            p_Task_Id    IN NUMBER   DEFAULT 0,
                            p_Rep_Period IN DATE     DEFAULT NULL,
                            p_LOG        IN BOOLEAN  DEFAULT FALSE,
                            p_Test_Tbl   IN VARCHAR2 DEFAULT NULL,
                            p_Load_Res   IN BOOLEAN  DEFAULT FALSE,
                            p_Load_Items IN BOOLEAN  DEFAULT TRUE,
                            p_Chunk      IN varchar2 DEFAULT 'MONTH'
                           )
IS
    v_prcName    CONSTANT VARCHAR2(20) := 'ReBind_Opr_Orders';

    l_Tmp_Table   VARCHAR2(34);
    l_SQL         VARCHAR2(2000);

    l_Date_From   DATE := p_Date_From;
    l_Date_To     DATE;
    l_Count       NUMBER;
    l_Update      number;
    l_Calc_Date   DATE;
    l_Rep_Period  date;
    l_Rep_From    date;
    l_Rep_To      date;
    l_Prev_RP     date;
    
    l_Result      number;
    l_SQL_Cnt     number;
    l_TbsStat     VARCHAR2(16);
    l_SID         NUMBER;
    l_Tbs_Stat    VARCHAR2(16);
    l_BDR_Tbl     VARCHAR2(32);
    l_BDR_Type_id number;
    l_Link_Tbl    VARCHAR2(32);
    l_CDR_Tbl     VARCHAR2(32);
    
    l_Network_Code VARCHAR2(16);
    l_Network_Id   NUMBER;
    
    l_Interval    INTERVAL DAY TO SECOND(0); 
    
    l_Days_Cnt    NUMBER := TRUNC(p_Date_To) - TRUNC(p_Date_From) + 1; -- кол-во дней, входящих в заданный период (для логов) 
    l_Curr_Day    NUMBER := 0; -- счетчик перетарфицированных дней (для логов)    

    ERR_DATE EXCEPTION;
    
    -- функция проверяет надо ли рассчитывать тарифк по указанной стороне (p_Side) для
    -- заданного задания (p_Task_Id)
    FUNCTION Check_Side(lp_Side    varchar2,
                        lp_Task_Id number
                       ) RETURN BOOLEAN
    IS
        l_Trf_Type   pin.q00_retrf_job.opr_trf_type%TYPE;
        lt_Load_Type NUM_COLL;
        l_Result     BOOLEAN := FALSE;
        l_Idx        PLS_INTEGER;
    BEGIN
    
        IF lp_Task_Id = 0 THEN 
            RETURN TRUE;
        END IF;
 
       -- получаем все части тарифов, которые подлежат расчету           
        SELECT opr_trf_type
          INTO l_Trf_Type
          FROM pin.q00_retrf_job
         WHERE task_id = lp_Task_Id; 
        
       -- загружаем для удобства в коллекцию полученые части тарифов 
        EXECUTE IMMEDIATE 'BEGIN :t := NUM_COLL(' || l_Trf_Type || '); END;'
          USING OUT lt_Load_Type;        
          
        l_Idx := lt_Load_Type.FIRST;  
          
        LOOP -- переибираем все заданные типы частей тарифа, подлежащие расчету
        
            EXIT WHEN l_Result = TRUE OR l_Idx IS NULL;
        
            IF lt_Load_Type(l_Idx) IN (pk00_const.c_OP_RATE_PLAN_TYPE_DT,
                                       pk00_const.c_OP_RATE_PLAN_TYPE_RI,
                                       pk00_const.c_OP_RATE_PLAN_TYPE_RIP
                                      )
               AND
               lp_Side = 'A'    
            THEN
              -- если сторона А и есть тип, который считается по этой стороне в задании
                l_Result := TRUE;
            
            ELSIF lt_Load_Type(l_Idx) IN (pk00_const.c_OP_RATE_PLAN_TYPE_DI,
                                          pk00_const.c_OP_RATE_PLAN_TYPE_DIP,
                                          pk00_const.c_OP_RATE_PLAN_TYPE_RT)
               AND
               lp_Side = 'B'
            THEN
              -- если сторона B и есть тип, который считается по этой стороне в задании 
               l_Result := TRUE;
               
            END IF;                             
                             
            l_Idx := lt_Load_Type.NEXT(l_Idx);
                    
        END LOOP;   
        
        RETURN l_Result;
        
    EXCEPTION
        WHEN no_data_found THEN
        
            RETURN FALSE;
    
    END Check_Side;                    
    
BEGIN

   --
   -- Проверка корректности периода, куда надо положить протарифицированные записи.
    IF pk114_items.Check_Rep_Period(p_Date_From, p_Date_To, p_Rep_Period) = 0
    THEN
        RAISE ERR_DATE;
    END IF;    

    -- получаем текущий отчетный период
    IF p_Task_Id = gc_SRC_AUDIT OR
       (p_Task_Id != gc_SRC_AUDIT AND p_Rep_Period IS NULL)
    THEN    
        l_Rep_Period := pk114_items.Get_Period_Date(p_Date_From, SYSDATE);
    ELSE
       --округляем до первого дня месяца
        l_Rep_Period := TRUNC(p_Rep_Period,'MM');     
    END IF;
    
    IF p_Load_Res = TRUE THEN -- Если стоит флаг загрузки ресурсов, то подтягиваем новые данные из billing
    
        pk1001_resources.Job_Daily_Operators;
        
    END IF;

    -- установить блокировку для таблиц RS, чтобы предотвратить их изменения во время работы процедуры
    /*mdv.pk21_lock.Wait_Req_Lock(p_Mode      => DBMS_LOCK.SX_MODE,
                                p_Lock_Name => mdv.pk21_lock.c_LOCK_RS_Smeta);
    mdv.pk21_lock.Lock_Resource(p_Mode      => DBMS_LOCK.SX_MODE,
                                p_Lock_Name => mdv.pk21_lock.c_LOCK_RS_Smeta);*/

    Pk01_Syslog.Write_Msg(p_Msg => 'Begin ReBind_Opr_Orders. Period: ' || TO_CHAR(p_Date_From,'dd.mm.yyyy hh24:mi:ss') || 
                                   ' - ' || TO_CHAR(p_Date_To,'dd.mm.yyyy hh24:mi:ss') ||
                                   ', Src: ' || p_Data_Type ||
                                   ', Task_ID: ' || TO_CHAR(p_Task_Id),     
                          p_Src => gc_PkgName || '.' || v_prcName);    

    l_Curr_Day := l_Curr_Day + 1;

   --- получаем SID сессии
    SELECT SID INTO l_SID
      FROM v$mystat
     WHERE ROWNUM = 1;

    IF p_LOG = TRUE THEN -- очищаем старые данные из лога
        EXECUTE IMMEDIATE
            'DELETE FROM PIN.Z13_LOG_RETRF  ' || CHR(13) ||
            ' WHERE TASK_ID = :l_Source '
        USING p_Task_Id;   
                       
    END IF;    
    
    
    -- получаем сеть, данные с которой тарифицируем 
    l_Network_Code := Get_Network(p_Data_Type => p_Data_Type,
                                  p_Network_Id => l_Network_Id -- OUT
                                 );

    --- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::    
    -- подготовка данных, по которым будут искаться CDR для перерасчета
     -- загрузка ресурсов для стороны А
    IF Check_Side('A', p_Task_Id) = TRUE THEN     
        l_Count := Get_Resources_Rebind(p_Data_Type => p_Data_Type, 
                                        p_Side      => 'A', 
                                        p_Date_From => p_Date_From,  
                                        p_Date_To   => p_Date_To, 
                                        p_Task_Id   => p_Task_Id, 
                                        p_Add_Data  => FALSE);
    END IF;
    
      -- загрузка ресурсов для стороны B
    IF Check_Side('B', p_Task_Id) = TRUE THEN  
        l_SQL_Cnt := Get_Resources_Rebind(p_Data_Type => p_Data_Type, 
                                          p_Side      => 'B', 
                                          p_Date_From => p_Date_From,  
                                          p_Date_To   => p_Date_To, 
                                          p_Task_Id   => p_Task_Id, 
                                          p_Add_Data  => TRUE);
    END IF;                                          
    
    IF l_Count <= 0 AND l_SQL_Cnt <= 0 THEN
      --  mdv.pk21_lock.UnLock_Resource(p_Lock_Name => mdv.pk21_lock.c_LOCK_RS_Smeta);
        RETURN; -- ошибка загрузки или нет данных для расчетов.
    END IF;
    --- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::    
    
   -- Получаем имя таблицы BDR-ов  
    l_BDR_Type_Id := PIN.Get_BDR_Type(p_Data_Type => p_Data_Type,
                                      p_BDR_Table => l_BDR_Tbl, -- out
                                      p_Agent     => l_Count,   -- out (здесь параметр не важен)
                                      p_Items     => l_Result   -- out (здесь параметр не важен)
                                     );         
    
    l_Date_To := LEAST(pin.Get_End_Period(l_Date_From,p_Chunk), 
                       p_Date_To);
    LOOP  -- перебор по временных диапазонов в заданном периоде
    
        l_Count := 0;    
    
       -- уточняем отчетного период, т.е. период где могут лежать BDR-ы
        IF TRUNC(l_Rep_Period,'MM') = TRUNC(l_Date_From,'MM') THEN
           -- если месяц звонка = месяцу отчетного периода, то кладем по дате звонка 
            l_Rep_From := l_Date_From; 
            l_Rep_To   := l_Date_To;

        ELSE
            l_Rep_From := l_Rep_Period; 
            l_Rep_To   := l_Rep_Period;
        END IF; 

      /*  IF p_Source > 0 THEN
           -- отметка о ходе выполнения в таблицу с заданием
            UpdateStatGUI(p_Task_Id => p_Source, 
                          p_Msg     => 'Расчет MMTS. День: ' || TO_CHAR(l_Curr_Day + 1) || ' из ' || TO_CHAR(l_Days_Cnt),
                          p_Percent => ROUND(100/l_Days_Cnt*l_Curr_Day, 1)
                         );
            l_Curr_Day := l_Curr_Day + 1;             
        END IF; */        
    
       -- проверка, есть ли отдельная таблица, где может лежать трафик за заданный день (ищем по дате конца) 
        l_CDR_Tbl := PIN.Get_CDR_Table_Name(p_Data_Type    => p_Data_Type,
                                            p_Day          => l_Date_To,
                                            p_Tbs_Stat     => l_TbsStat
                                           );     
        
        l_Calc_Date := SYSDATE;
        
        EXECUTE IMMEDIATE 'TRUNCATE TABLE PIN.TMP02_ROWS_CALC';
                
        --
        --- формируем имя таблицы, где будут ссылки на пересчитанные CDR-ы с новыми данные
        l_Tmp_Table := 'QT' || TO_CHAR(SYSDATE,'ddmmyyyyhh24miss') || TO_CHAR(l_SID);        
        
        IF p_Task_Id = gc_SRC_AUDIT THEN 
           /* +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                ПЕРЕСЧЕТ ДАННЫХ ПО ТАБЛИЦАМ АУДИТА
            ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/    
         
            -- определение СDR, подлежащих перерасчету        
            l_Count := Get_Opr_CDR_ReBind(p_Data_Type   => p_Data_Type,
                                          p_Date_From   => l_Date_From,
                                          p_Date_To     => l_Date_To,
                                          p_Task_Id     => p_Task_Id,
                                          p_CDR_Table   => l_CDR_Tbl
                                         ); 

            Pk01_Syslog.Write_Msg(p_Msg => 'Period: ' || TO_CHAR(l_Date_From,'dd.mm.yyyy hh24:mi:ss') || 
                                           ' - ' || TO_CHAR(l_Date_To,'dd.mm.yyyy hh24:mi:ss') ||
                                           ', Src: ' || p_Data_Type || 
                                           ', For rebind: ' || TO_CHAR(l_Count) || 
                                           ', Task_ID: ' || TO_CHAR(p_Task_Id), 
                                  p_Src => gc_PkgName || '.' || v_prcName);
                                   
         -- если идет пересчет по таблицам аудита, то может потребоваться
         -- синхронизация данных в CDR и BDR, т.е. данные с несовпадающими л/счетами
         -- отправляем на пересчет 
                INSERT INTO PIN.TMP02_ROWS_CALC(row_id, date_to, start_time, cdr_id, bdr_type, 
                                                trf_side) 
                SELECT rd, NULL, MIN(ans_time), cdr_id, l_BDR_Type_Id,
                       MIN(side_a) || 
                         (CASE WHEN MIN(side_a) IS NOT NULL AND MIN(side_b) IS NOT NULL THEN
                                  ','
                               ELSE NULL
                          END) ||
                        MIN(side_b) trf_side       
                  FROM (
                        SELECT  
                               c.ROWID rd, c.ans_time, c.cdr_id, 
                               c.op_a_order_id, c.op_b_order_id,
                               ba.bdr_order bdr_a_order_id,  
                               bb.bdr_order bdr_b_order_id,
                               (CASE WHEN c.op_a_order_id = pin.pk00_const.c_TOO_MANY_ORDERS
                                           OR
                                          (c.op_a_order_id > 0 AND NVL(ba.bdr_order, -1) < 0) 
                                           OR   
                                          (NVL(c.op_a_order_id, -1) < 0 AND ba.bdr_order IS NOT NULL) 
                                     THEN
                                            'A'
                                     ELSE
                                         NULL
                                END) side_a,
                               (CASE WHEN c.op_b_order_id = pin.pk00_const.c_TOO_MANY_ORDERS
                                           OR
                                          (c.op_b_order_id > 0 AND NVL(bb.bdr_order, -1) < 0) 
                                           OR   
                                          (NVL(c.op_b_order_id, -1) < 0 AND bb.bdr_order IS NOT NULL)                                
                                     THEN
                                            'B'
                                     ELSE
                                         NULL
                                END) side_b
                          FROM (SELECT b.order_id bdr_order, b.cdr_id, 'A' trf_side 
                                  FROM BDR_OPER_T b 
                                 WHERE b.rep_period BETWEEN l_Rep_From AND l_Rep_To   
                                   AND b.start_time BETWEEN l_Date_From AND l_Date_To 
                                   AND b.trf_type IN (1,4,6) -- pk114_items.Get_List_BDR_Types(p_Data_Type, p_Side)
                                   AND b.bdr_type_id = l_BDR_Type_Id
                                 GROUP BY b.cdr_id, b.order_id  
                                ) ba,
                               (SELECT b.order_id bdr_order, b.cdr_id, 'B' trf_side 
                                  FROM BDR_OPER_T b 
                                 WHERE b.rep_period BETWEEN l_Rep_From AND l_Rep_To   
                                   AND b.start_time BETWEEN l_Date_From AND l_Date_To 
                                   AND b.trf_type IN (2,3,5) -- pk114_items.Get_List_BDR_Types(p_Data_Type, p_Side)
                                   AND b.bdr_type_id = l_BDR_Type_Id
                                 GROUP BY b.cdr_id, b.order_id  
                                ) bb,                                
                               mdv.x03_xttk_cdr c
                         WHERE c.ans_time BETWEEN l_Date_From AND l_Date_To 
                           AND c.network_id = l_Network_Id
                           AND c.ROWID NOT IN (SELECT row_id   -- исключаем cdr-ы, которые уже подбраны на тарификацию
                                                 FROM PIN.TMP02_ROWS_CALC 
                                                WHERE row_id IS NOT NULL)  
                           AND c.cdr_id = ba.cdr_id(+)
                           AND c.cdr_id = bb.cdr_id(+) 
                        ) c  
                  WHERE -- считаем несхождение только если 
                         -- 1. несколько заказов на ресурсе и BDR-ов нет вообще или там ошибки (в дргуом случаем зверинец из непротарифицированных) 
                         (c.op_a_order_id = pin.pk00_const.c_TOO_MANY_ORDERS 
                           AND 
                          NVL(c.bdr_a_order_id,-100) < 0
                         )
                     OR  (c.op_b_order_id = pin.pk00_const.c_TOO_MANY_ORDERS 
                           AND 
                          NVL(c.bdr_b_order_id,-100) < 0
                         )
                     OR  -- 2. для всех других, нормальных, где ресурс = 1 заказу любое несовпадение на пересчет     
                         (NVL(c.op_a_order_id, -100) != pin.pk00_const.c_TOO_MANY_ORDERS 
                           AND
                          (
                           (c.op_a_order_id > 0 AND c.op_a_order_id != NVL(c.bdr_a_order_id,-100))
                            OR
                           (NVL(c.op_a_order_id, -1) < 0 AND c.bdr_a_order_id IS NOT NULL) 
                          )
                         ) 
                     OR (NVL(c.op_b_order_id,-100) != pin.pk00_const.c_TOO_MANY_ORDERS
                          AND 
                          (
                           (c.op_b_order_id > 0 AND c.op_b_order_id != NVL(c.bdr_b_order_id,-100))
                            OR
                           (NVL(c.op_b_order_id, -1) < 0 AND c.bdr_b_order_id IS NOT NULL) 
                          )
                        ) 
                  GROUP BY rd, cdr_id;         

            l_SQL_Cnt := SQL%ROWCOUNT;
            
            IF l_SQL_Cnt > 0 THEN

                Pk01_Syslog.Write_Msg(p_Msg => 'Period: ' || TO_CHAR(l_Date_From,'dd.mm.yyyy hh24:mi:ss') || 
                                               ' - ' || TO_CHAR(l_Date_To,'dd.mm.yyyy hh24:mi:ss') ||
                                               ', Src: ' || p_Data_Type || 
                                               ', add diff. cdr-bdr accounts: ' || TO_CHAR(l_SQL_Cnt) || 
                                               ', Task_ID: ' || TO_CHAR(p_Task_Id), 
                                      p_Src => gc_PkgName || '.' || v_prcName);            
            
                l_Count := l_SQL_Cnt + SQL%ROWCOUNT;
                
            END IF;    


            IF l_Count > 0 THEN
            
               -- перепривязка л/счетов
                Pk120_Bind_Operators.Bind_XTTK_Opers(p_Data_Type  => p_Data_Type,
                                                         p_Date_From  => l_Date_From,
                                                         p_Date_To    => l_Date_To,
                                                         p_Pivot_Tbl  => 'PIN.TMP02_ROWS_CALC',
                                                         p_Result_Tbl => l_Tmp_Table,
                                                         p_Upd_CDR    => FALSE,
                                                         p_Id_Log     => (CASE WHEN p_LOG = TRUE THEN p_Task_Id
                                                                               ELSE 0 
                                                                          END),
                                                         p_Full_Bind  => FALSE 
                                                        );

--               COMMIT;
--               RAISE no_data_found;            

            END IF;
         
        ELSIF p_Task_Id > 0 THEN  
           -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
           --     ПЕРЕСЧЕТ ДАННЫХ ПО ЗАДАННЫМ УСЛОВИЯМ
           -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            
            DELETE FROM TMP02_ROWS_CALC;
            DELETE FROM TMP03_CDR_BIND;

            -- определение СDR, подлежащих перерасчету        
            l_Count := Get_Opr_CDR_ReBind(p_Data_Type   => p_Data_Type,
                                           p_Date_From   => l_Date_From,
                                           p_Date_To     => l_Date_To,
                                           p_Task_Id     => p_Task_Id,
                                           p_CDR_Table   => l_CDR_Tbl
                                          );

            IF l_Count > 0 THEN

                Pk01_Syslog.Write_Msg(p_Msg => 'Day: ' || TO_CHAR(l_Date_From,'dd.mm.yyyy') ||
                                               ', Src: ' || p_Data_Type || 
                                               ', For rebind ' || TO_CHAR(l_Count) ||
                                               ', Data_Type: ' || p_Data_Type ||  
                                               ', Task_ID: ' || TO_CHAR(p_Task_Id), 
                                         p_Src => gc_PkgName || v_prcName);
                
               -- перепривязка л/счетов
                Pk120_Bind_Operators.Bind_XTTK_Opers(p_Data_Type  => p_Data_Type,
                                                     p_Date_From      => l_Date_From,
                                                     p_Date_To        => l_Date_To,
                                                     p_Pivot_Tbl  => 'PIN.TMP02_ROWS_CALC',
                                                     p_Result_Tbl => l_Tmp_Table,
                                                     p_Upd_CDR    => FALSE,
                                                     p_Id_Log     => (CASE WHEN p_LOG = TRUE THEN p_Task_Id
                                                                           ELSE 0 
                                                                      END),
                                                     p_Full_Bind  => FALSE 
                                                    );               

--INSERT INTO TMP02_ROWS_CALC_TEST SELECT * FROM TMP02_ROWS_CALC;
--INSERT INTO MDV.MS_SQL VALUES(1, l_SQL);
--COMMIT; 
--RAISE no_data_found;                 
                 
            ELSE
            
                Pk01_Syslog.Write_Msg(p_Msg => 'Day: ' || TO_CHAR(l_Date_From,'dd.mm.yyyy') ||
                                               ', Src: ' || p_Data_Type || 
                                               ', Data for rebind is not found.', 
                                      p_Src => gc_PkgName || '.' || v_prcName);                                  
            
            END IF;
            
        END IF;
                   
        IF --p_Test_Tbl IS NULL AND 
           l_Count > 0 THEN -- не тестовая тарификация и были перепривязаны данные
           
            PK1110_OPR_TARIFFING.Load_BDR_XTTK(p_Data_Type      => p_Data_Type,
                                                   p_Data_Table     => l_Tmp_Table,
                                                   p_Date_From      => l_Date_From,
                                                   p_Date_To        => l_Date_To,
                                                   p_Rep_Period     => l_Rep_Period,
                                                   p_Task_Id        => p_Task_Id,
                                                   p_Test_BDR_Table => p_Test_Tbl --NULL
                                                  ); 

           -- обновляем данные о л/с в таблице CDR-ов
            l_Update := Pk120_Bind_Operators.Update_CDR_Op_Id(p_Data_Type    => p_Data_Type,
                                                                  p_Data_Table   => l_Tmp_Table,
                                                                  p_Date_From    => l_Date_From,
                                                                  p_Date_To      => l_Date_To,
                                                                  p_Id_Log       => 0
                                                                 );

            pin.Pk01_Syslog.Write_Msg(p_Msg => 'CDR updated: ' || TO_CHAR(l_Update) ||
                                               ', Src: ' || p_Data_Type,  
                                      p_Src => 'Pk120_Bind_Operators.Update_CDR_Op_Id');                                                      

            COMMIT;
                
        END IF;

        IF l_Count > 0 THEN
           -- удаляем промежуточную таблицу
            EXECUTE IMMEDIATE 'DROP TABLE ' || l_Tmp_Table || ' PURGE ';
        END IF;
            
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP02_ROWS_CALC ';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP03_CDR_BIND ';
        
        
        EXIT WHEN l_Date_To >= p_Date_To; -- выход, если все в заданном периоде загружено 
        
        IF p_Test_Tbl IS NULL AND 
           l_Prev_RP IS NOT NULL AND l_Prev_RP != TRUNC(l_Rep_Period,'mm') 
        THEN
        
            PK1110_OPR_TARIFFING.Recalc_Op_V_Tariff(p_Data_Type   => p_Data_Type,
                                                        p_Rep_Period  => l_Prev_RP,
                                                        p_Modify_Date => SYSDATE);        
        
            IF p_Load_Items = TRUE THEN
               -- Обновление строк счетов, т.к. сменился отчетный период
                pk114_items.Load_BDR_to_Item(p_Data_Type  => p_Data_Type,
                                             p_Period     => l_Prev_RP,
                                             p_Account_Id => NULL);        

                pk114_items.Load_Op_MinPay(p_Data_Type  => p_Data_Type,
                                           p_Rep_Period => l_Prev_Rp,
                                           p_Call_Month => NULL
                                          );                       
                                                                 
            END IF;                                                 
            
        END IF;
                    
        l_Prev_RP := TRUNC(l_Rep_Period,'mm');        
        
        l_Date_From := l_Date_To + 1/86400;
        l_Date_To   := LEAST(pin.Get_End_Period(l_Date_From,p_Chunk), 
                             p_Date_To);

        -- Перед началом расчета следующего дня проверяем нет ли запросов на обновление ресурсов 
      /*  IF mdv.pk21_lock.Check_Lock_Req(p_Mode      => DBMS_LOCK.SX_MODE, 
                                        p_Lock_Name => mdv.pk21_lock.c_LOCK_RS) > 0 
        THEN
            
         /*   IF p_Source > 0 THEN
                UpdateStatGUI(p_Task_Id => p_Source, 
                              p_Msg     => 'Ожидание разрешения на доступ к ресурсам.',
                              p_Percent => ROUND(100/l_Days_Cnt*l_Curr_Day,1));        
            END IF; */            
        /*   
            -- снимаем текущую блокировку
            mdv.pk21_lock.Unlock_Resource; 
            -- ждем пока не будут запущены процессы с более высоким приоритетом
            mdv.pk21_lock.Wait_Req_Lock(p_Mode      => DBMS_LOCK.SX_MODE,
                                        p_Lock_Name => mdv.pk21_lock.c_LOCK_RS); 
            -- устанавливаем блокировку
            mdv.pk21_lock.LOCK_RESOURCE(p_Mode      => DBMS_LOCK.SX_MODE,
                                        p_Lock_Name => mdv.pk21_lock.c_LOCK_RS);
          */                              
            -- загружаем обновленные данные
/*            IF Get_Resources_Rebind(UPPER(p_Data_Type), p_Side, l_Date_From, p_Date_To, p_Task_Id) <= 0 THEN
                mdv.pk21_lock.Unlock_Resource;
                RETURN; -- ошибка загрузки или нет данных для расчетов.
            END IF; */ 
 
        --END IF;   

        l_Curr_Day := l_Curr_Day + 1;

    END LOOP; -- перебор дней в заданном периоде

   -- +++++++++++++++++++++++++++++++++++++++++++
   -- Обновление строк счетов
    IF p_Test_Tbl IS NULL THEN
       PK1110_OPR_TARIFFING.Recalc_Op_V_Tariff(p_Data_Type   => p_Data_Type,
                                                   p_Rep_Period  => l_Rep_Period,
                                                   p_Modify_Date => SYSDATE);   
                                                      
        IF p_Load_Items = TRUE THEN        
        
            pk114_items.Load_BDR_to_Item(p_Data_Type  => p_Data_Type,
                                         p_Period     => l_Rep_Period,
                                         p_Account_Id => NULL);             

            pk114_items.Load_Op_MinPay(p_Data_Type  => p_Data_Type,
                                       p_Rep_Period => l_Rep_Period,
                                       p_Call_Month => NULL
                                      );                  

        END IF;                                           
   
    END IF;               

    -- снимаем установленную блокировку        
  --  mdv.pk21_lock.Unlock_Resource;
    
 /*   IF p_Source > 0 THEN
       -- отметка о ходе выполнения в таблицу с заданием
        UpdateStatGUI(p_Task_Id => p_Source, p_Msg => 'Выполнено.', p_Percent => 100);
    END IF;      */  
    
EXCEPTION
    WHEN ERR_DATE THEN
        NULL;
        
END ReBind_Opr_Orders;                             



/* ------------------------------------------------------------------------------   
  Процедура для перетарификации соединений (таблица B01_BDR)
  Входные параметры:
     p_Data_Type - тип данных, которые надо перетарифицировать (SPB, NOVTK)
     p_Date_From - дата начала тарифицируемого периода (start_time)
     p_Date_To   - дата конца тарифицируемого периода (start_time)
         -- p_Month, p_Bill - параметры были исключены что бы в данной процедуре не мумукаться с поддержкой таблицы линков.
         --                  все переносы только через перепривязку.      
     p_Source - источник данных для перетарификации
                -4 - перетарифицировать все соединения у которых не нашли условия тарификации     
                -1 (c_ALL_ERRORS) - перетарифицировать все ошибки (ошибки тарификации)
                > 0 - подготовить данные по клиентам/номерам, заданным в таблице Q03_RETRF. В кач-ве значения Task_Id
                0 - искать по таблицам аудита
     p_BDR_Source - источник BDR-ов, которые надо перетарифицировать. SPB, NOVTK и т.д.
     p_Load_Res - загрузить новые данные по ресурсам из схемы billing.      
     p_Log      - в таблицу с логами кладутся кода sub_id_mnmg записи, 
                  у которых изменились какие-либо данные   
     p_Load_Items - TRUE - пересчитывать item-ы. Параметр, используемый по-умолчанию. Item-ы пересчитываются в соответсвии с новыми
                                результатами   
                    FALSE - не пересчитывать item-ы                                                                                                                                             
     p_Chunk      - период, который берется для расчета за один проход (DAY, WEEK, MONTH)
*/ 
PROCEDURE ReTrfBDR(p_Data_Type  IN varchar2,
                   p_Date_From  IN DATE     DEFAULT TRUNC(SYSDATE - 1, 'mm'),
                   p_Date_To    IN DATE     DEFAULT TRUNC(SYSDATE) - 1/86400,
                   p_Task_Id    IN NUMBER   DEFAULT 0,
                   p_Rep_Period IN DATE     DEFAULT NULL,
                   p_Load_Res   IN BOOLEAN  DEFAULT FALSE,
                   p_Log        IN BOOLEAN  DEFAULT FALSE,
                   p_Load_Items IN BOOLEAN  DEFAULT TRUE,
                   p_Chunk      IN varchar2 DEFAULT 'MONTH',
                   p_Test_BDR_Table IN varchar2 DEFAULT NULL
                  )
IS
    v_prcName   CONSTANT VARCHAR2(20) := 'ReTrfBDR';

    l_Modify_Date DATE := SYSDATE;

    l_BDR_Table   VARCHAR2(32);

    l_Date_From   DATE;
    l_Date_To     DATE;
    l_Rep_Period  date;
    l_Rep_From    date;
    l_Rep_To      date;    
    l_Prev_RP     date;
    l_Trf_Type    varchar2(16);
    l_PrepCnt     number;
    l_External_Id number;
    l_BDR_Type_Id number;
    
    l_SQL         VARCHAR2(10000);
    l_IdCursor    NUMBER;
    --l_Count       NUMBER;
    l_Update      NUMBER;
    
    l_Network_Code pin.network_t.network_code%TYPE;
    
    l_TbsStat     varchar2(64);
    l_CDR_Tbl     varchar2(64);
    l_Tmp_Table   varchar2(64);
    l_SID         number;

    l_Days_Cnt    NUMBER := TRUNC(p_Date_To) - TRUNC(p_Date_From) + 1; -- кол-во дней, входящих в заданный период (для логов)
    l_Curr_Day    number := 0;

    ERR_DATA EXCEPTION;
   
BEGIN

    --
   -- Проверка корректности периода, куда надо положить протарифицированные записи.
   IF pk114_items.Check_Rep_Period(p_Date_From, p_Date_To, p_Rep_Period) = 0 AND
      p_Test_BDR_Table IS NULL
   THEN
        RAISE ERR_DATA;
   END IF;     

    -- получаем текущий отчетный период
    IF p_Task_Id = gc_SRC_AUDIT OR
       (p_Task_Id != gc_SRC_AUDIT AND p_Rep_Period IS NULL)
    THEN    
        l_Rep_Period := pk114_items.Get_Period_Date(p_Date_From, SYSDATE);
    ELSE
       --округляем до первого дня месяца
        l_Rep_Period := TRUNC(p_Rep_Period,'MM');     
    END IF;

    l_Curr_Day := l_Curr_Day + 1;

    IF p_Load_Res = TRUE THEN
    
     --   IF p_Source > 0 THEN -- отметка в таблицу с заданием
     --       UpdateStatGUI(p_Task_Id => p_Source, p_Msg => 'Загрузка ресурсов.', p_Percent => 0);
     --   END IF; 

        pk1001_resources.job_daily_operators;
        
    END IF;

    -- установить блокировку для таблиц RS, чтобы предотвратить их изменения во время работы процедуры
    /*mdv.pk21_lock.Wait_Req_Lock(p_Mode      => DBMS_LOCK.SX_MODE,
                                p_Lock_Name => mdv.pk21_lock.c_LOCK_RS);
    mdv.pk21_lock.Lock_Resource(p_Mode      => DBMS_LOCK.SX_MODE,
                                p_Lock_Name => mdv.pk21_lock.c_LOCK_RS);*/
    
    Pk01_Syslog.Write_Msg(p_Msg => 'Begin ReTrfBDR. Period: ' || TO_CHAR(p_Date_From,'dd.mm.yyyy hh24:mi:ss') || 
                                      ' - ' || TO_CHAR(p_Date_To,'dd.mm.yyyy hh24:mi:ss') ||
                                      ', Src: ' || p_Data_Type,     
                          p_Src => gc_PkgName || '.' || v_prcName);    


    -- получаем список типов BDR-ов на перетарификацию 
    /*l_Trf_Type := pk114_items.Get_List_BDR_Types(p_Data_Type => p_Data_Type,
                                                 p_Side     => p_Side,
                                                 p_In_Out   => NULL  -- D - доход, R - расход
                                                ); 

    IF l_Trf_Type IS NULL THEN
        Pk01_Syslog.Write_Msg(p_Msg => 'Тип BDR-ов не определен. Проверьте тип данных и сторону. ' || 
                                        '(Тип данных: ' || p_Data_Type || ')',     
                              p_Src => gc_PkgName || '.' || v_prcName,
                              p_Level => pk01_syslog.L_warn);
        RAISE ERR_DATA;                   
    END IF;                              */


    IF p_LOG = TRUE THEN -- очищаем старые данные из лога
        EXECUTE IMMEDIATE
            'DELETE FROM MDV.Z13_LOG_RETRF  ' || CHR(13) ||
            ' WHERE TASK_ID = :l_Task_Id '
        USING p_Task_Id;   
                       
    END IF;    
    
    -- получаем имя таблицы BDR-ов, где должны быть данные за текущий период без переносов
    l_BDR_Type_Id := PIN.Get_BDR_Type(p_Data_Type => p_Data_Type,
                                      p_BDR_Table => l_BDR_Table, -- out 
                                      p_Agent     => l_PrepCnt,    -- out (здесь параметр не важен)
                                      p_Items     => l_External_Id -- out (здесь параметр не важен)
                                     );          

    -- получаем идентификатор для item-ов    
    l_External_Id := pk114_items.Get_External_Id(p_BDR_Type => p_Data_Type);
    
    -- получем сеть, данные которой перетарифицируются
    l_Network_Code := pin.Get_Network_Code(p_Data_Type);

    l_Date_From := p_Date_From;
    l_Date_To   := LEAST(pin.Get_End_Period(l_Date_From,p_Chunk), 
                         p_Date_To);
            
    LOOP  -- перебор дней в заданном периоде
    
      /*  IF p_Source > 0 THEN
           -- отметка о ходе выполнения в таблицу с заданием
            UpdateStatGUI(p_Task_Id => p_Source, 
                          p_Msg     => 'Расчет TDM. День: ' || TO_CHAR(l_Curr_Day + 1) || ' из ' || TO_CHAR(l_Days_Cnt),
                          p_Percent => ROUND(100/l_Days_Cnt*l_Curr_Day, 1)
                         );
            l_Curr_Day := l_Curr_Day + 1;             
        END IF;      */      
      
       -- уточняем отчетного период, т.е. период где могут лежать BDR-ы
        IF TRUNC(l_Rep_Period,'MM') = TRUNC(l_Date_From,'MM') THEN
           -- если месяц звонка = месяцу отчетного периода, то кладем по дате звонка 
            l_Rep_From := l_Date_From; 
            l_Rep_To   := l_Date_To;

        ELSE
            l_Rep_From := l_Rep_Period; 
            l_Rep_To   := l_Rep_Period;
        END IF; 
      
      -- получаем имя таблицы B01_BDR-ов, где должны быть данные за текущий период без переносов
      --  l_BDR_Table := 'PIN.E04_BDR_MMTS_T'; --pk00_const.c_BDR_MMTS_Table; 
      
        EXECUTE IMMEDIATE 'TRUNCATE TABLE PIN.TMP03_CDR_BIND';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE PIN.TMP02_ROWS_CALC';
                   
        l_PrepCnt := 0;
        
        -- определение BDR, подлежащих перетарификации
        IF p_Task_Id = gc_ALL_ERRORS THEN
        
           -- выбираем все ошибки тарификации
            EXECUTE IMMEDIATE
                'INSERT INTO PIN.TMP03_CDR_BIND(row_id) ' || CHR(13) || 
                'SELECT /*++ parallel(b 10) */ ' || CHR(13) ||
                '       b.ROWID rd ' || CHR(13) ||
                '  FROM ' || l_BDR_Table || ' b ' || CHR(13) ||
                ' WHERE b.rep_period BETWEEN :l_RDate_From AND :l_RDate_To ' || CHR(13) ||
                '   AND b.start_time BETWEEN :l_Date_From AND :l_Date_To ' || CHR(13) ||
                '   AND b.bdr_type_id = :l_BDR_Type_Id ' || CHR(10) ||
                '   AND b.trf_type IN (' || l_Trf_Type || ') ' || CHR(13) ||
                '   AND b.order_id > 0 ' || CHR(13) ||
                '   AND b.bdr_status != :l_OK ' -- 0 - нет ошибок
             USING l_Rep_From, l_Rep_To,
                   l_Date_From, l_Date_To,
                   l_BDR_Type_Id,
                   pk00_const.c_RET_OK;

            l_PrepCnt := SQL%ROWCOUNT;

        ELSIF p_Task_Id = -5 THEN
           -- всякие дополнительные условия, которые нельзя задать кроме как тут в коде подправить :-)
           -- фича вообщем.
            EXECUTE IMMEDIATE
                'INSERT INTO PIN.TMP03_CDR_BIND(row_id) ' || CHR(13) || 
                'SELECT /*++ parallel(b 10) */ ' || CHR(13) ||
                '       b.ROWID rd ' || CHR(13) ||
                '  FROM ' || l_BDR_Table || ' b ' || CHR(13) ||
                ' WHERE b.rep_period BETWEEN :l_RDate_From AND :l_RDate_To ' || CHR(13) ||
                '   AND b.local_time BETWEEN :l_Date_From AND :l_Date_To ' || CHR(13) ||
                '   AND b.trf_type IN (1,2,3,4,5,6) ' || CHR(13) ||
                '   AND b.bdr_type_id = :l_BDR_Type ' || CHR(10) ||
                '   and B.ACCOUNT_ID = 1945842'
             --   '   AND (length(b.abn_a) <= 6 or length(b.abn_b) <= 6) '
             USING l_Rep_From, l_Rep_To,
                   l_Date_From, l_Date_To,
                   l_BDR_Type_Id; 

            l_PrepCnt := SQL%ROWCOUNT;

        ELSIF p_Task_Id > 0 THEN -- данные на перетарификацию ищутся для заданных клиентов/номеров
            
            l_SQL := NULL;         
    
           -- формируем запрос 
            FOR l_cur IN (SELECT abn_a, abn_b, order_id, err_code, service_id, subservice_id
                            FROM (
                                  SELECT NVL2(q.abn_a, 1, 0)         abn_a,
                                         NVL2(q.abn_b, 1, 0)         abn_b,
                                         NVL2(q.order_id, 1, 0)      order_id,
                                         NVL2(q.service_id, 1, 0)    service_id,
                                         NVL2(q.subservice_id, 1, 0) subservice_id,
                                         DECODE(q.err_code, -1000, 2, NULL, 0, 1) err_code
                                    FROM Q01_RETRF_JOB_DETAIL q
                                   WHERE q.Task_Id = p_Task_Id
                                     AND (q.abn_a    IS NOT NULL OR
                                          q.abn_b    IS NOT NULL OR 
                                          q.order_id IS NOT NULL OR
                                          q.service_id    IS NOT NULL OR 
                                          q.subservice_id IS NOT NULL OR
                                          q.err_code IS NOT NULL)
                                  )
                           GROUP BY abn_a, abn_b, order_id, err_code,
                                    service_id, subservice_id
                         )        
            LOOP
                IF l_SQL IS NOT NULL THEN
                    l_SQL := l_SQL || ' UNION ' || CHR(13);
                END IF;    
                    
                l_SQL := l_SQL || 
                    'SELECT /*++ parallel(b 5) */ ' || CHR(13) ||
                    '       b.ROWID rd ' || CHR(13) ||
                    '  FROM ' || l_BDR_Table || ' b, ' || CHR(13) ||
                    '       Q01_RETRF_JOB_DETAIL q ' || CHR(13) ||
                    ' WHERE b.rep_period BETWEEN :l_RDate_From AND :l_RDate_To ' || CHR(13) ||
                    '   AND b.start_time BETWEEN :l_Date_From AND :l_Date_To ' || CHR(13) ||
                    '   AND b.bdr_type_id = :l_BDR_Type ' || CHR(10) ||                    
                    '   AND b.trf_type IN (' || l_Trf_Type || ') ' || CHR(13) ||
                    '   AND b.order_id > 0 ' || CHR(13) ||
                    '   AND q.TASK_ID = :p_Source ' || CHR(13) ||
                    '   AND TO_CHAR(q.abn_a) ' || (CASE l_Cur.abn_a 
                                                       WHEN 1 THEN ' = b.abn_a '
                                                       ELSE ' IS NULL ' 
                                                     END) || CHR(13) ||
                    '   AND TO_CHAR(q.abn_b) ' || (CASE l_Cur.abn_b 
                                                       WHEN 1 THEN ' = b.abn_b '
                                                       ELSE ' IS NULL ' 
                                                     END) || CHR(13) ||                                                     
                    '   AND q.order_id ' || (CASE l_Cur.order_id 
                                                WHEN 1 THEN ' = b.order_id '
                                                ELSE ' IS NULL ' 
                                             END) || CHR(13) ||
                    '   AND q.service_id ' || (CASE l_Cur.service_id 
                                                  WHEN 1 THEN ' = b.service_id '
                                                  ELSE ' IS NULL ' 
                                               END) || CHR(13) ||
                    '   AND q.subservice_id ' || (CASE l_Cur.subservice_id 
                                                     WHEN 1 THEN ' = b.subservice_id '
                                                     ELSE ' IS NULL ' 
                                                  END) || CHR(13) ||                                                                                  
                    '   AND q.err_code ' || (CASE l_Cur.err_code
                                               WHEN 2 THEN ' = -1000 AND b.bdr_status != 0 '
                                               WHEN 1 THEN ' = b.sub_status '
                                               ELSE ' IS NULL ' 
                                             END) || CHR(13);                                     
                
            END LOOP;
                    
            l_SQL := 'INSERT INTO PIN.TMP03_CDR_BIND(row_id) ' || CHR(13) ||
                     '     SELECT rd FROM (' || CHR(13) || l_SQL || CHR(13) || ' ) ' || CHR(13) ||
                     ' GROUP BY rd'; 
                
            l_IdCursor := DBMS_SQL.OPEN_CURSOR;
                          
            DBMS_SQL.PARSE(C => l_IdCursor,
                           STATEMENT => l_SQL,
                           language_flag => DBMS_SQL.NATIVE);
                               
            DBMS_SQL.BIND_VARIABLE(C => l_IdCursor,
                                   NAME => 'l_RDate_From',
                                   VALUE => l_Rep_From);
                                       
            DBMS_SQL.BIND_VARIABLE(C => l_IdCursor,
                                   NAME => 'l_RDate_To',
                                   VALUE => l_Rep_To);
                                                                  
            DBMS_SQL.BIND_VARIABLE(C => l_IdCursor,
                                   NAME => 'l_Date_From',
                                   VALUE => l_Date_From);
                                       
            DBMS_SQL.BIND_VARIABLE(C => l_IdCursor,
                                   NAME => 'l_Date_To',
                                   VALUE => l_Date_To);
                                       
            DBMS_SQL.BIND_VARIABLE(C => l_IdCursor,
                                   NAME => 'p_Source',
                                   VALUE => p_Task_Id);                                   
                                   
            DBMS_SQL.BIND_VARIABLE(C => l_IdCursor,
                                   NAME => 'l_BDR_Type',
                                   VALUE => l_BDR_Type_Id);                                   
                                       
            l_PrepCnt := DBMS_SQL.EXECUTE(l_IdCursor);
                
            DBMS_SQL.CLOSE_CURSOR(l_IdCursor);

        ELSIF p_Task_Id = 0 THEN  -- данные на перетарификацию ищутся по таблицам аудита
        
           -- проверяем на существование незакрытых периодов у заданных источников трафика 
        --    IF pk114_items.Check_Rep_Period(l_Date_From, SYSDATE) = 1 THEN
               -- если есть попытка переподбора закрытого периода, то ошибку инициируем
         --       RAISE CLOSED_PERIOD; 
         --   END IF;  
                         
                INSERT INTO PIN.TMP03_CDR_BIND(row_id, cdr_id, item_id, bill_id) 
                SELECT rd, cdr_id, item_id, bill_id 
                  FROM ( 
                    -- возможные изменения тар. планов и маршрутизации (параметры switch_id и op_sw_id)
                        SELECT /*++ parallel(b 5) */  
                               b.ROWID rd, b.cdr_id, b.item_id, b.bill_id 
                          FROM BDR_OPER_T b, 
                               PIN.RSX07_ORDER_SWTG_AUDIT a 
                         WHERE b.rep_period BETWEEN l_Rep_From AND l_Rep_To 
                           AND b.local_time BETWEEN l_Date_From AND l_Date_To 
                           AND b.bdr_type_id = l_BDR_Type_Id                     
                         --  AND b.trf_type IN (' || l_Trf_Type || ') 
                           AND a.date_save >= l_Date_From  
                           AND b.modify_date <= a.date_save 
                           AND b.start_time BETWEEN a.date_from AND NVL(a.date_to, gc_DATE_END)                     
                           AND b.order_swtg_id = a.order_swtg_id 
                        UNION ALL  -- изменение тарифного плана л/счета (rateplan_id)                   
                        SELECT /*++ parallel(b 5) */  
                               b.ROWID rd, b.cdr_id, b.item_id, b.bill_id 
                          FROM BDR_OPER_T b, 
                               PIN.RS02_ORDER_AUDIT a 
                         WHERE b.rep_period BETWEEN l_Rep_From AND l_Rep_To 
                           AND b.local_time BETWEEN l_Date_From AND l_Date_To 
                           AND b.bdr_type_id = l_BDR_Type_Id 
                         --  AND b.trf_type IN (' || l_Trf_Type || ') 
                           AND a.save_date >= l_Date_From  
                           AND b.modify_date <= a.save_date 
                           AND b.start_time BETWEEN a.date_from AND NVL(a.date_to, gc_DATE_END)                     
                           AND b.order_id = a.order_id 
                        UNION ALL  -- изменение order_body_id                    
                        SELECT /*++ parallel(b 5) */  
                               b.ROWID rd, b.cdr_id, b.item_id, b.bill_id 
                          FROM BDR_OPER_T b, 
                               PIN.RS04_ORDER_BODY_AUDIT a 
                         WHERE b.rep_period BETWEEN l_Rep_From AND l_Rep_To 
                           AND b.local_time BETWEEN l_Date_From AND l_Date_To 
                           AND b.bdr_type_id = l_BDR_Type_Id 
                        --   AND b.trf_type IN (' || l_Trf_Type || ') 
                           AND a.save_date >= l_Date_From  
                           AND b.modify_date <= a.save_date 
                           AND b.start_time BETWEEN a.date_from AND NVL(a.date_to, gc_DATE_END)                     
                           AND b.order_id = a.order_id                     
                        UNION ALL  -- изменения в справочнике Питерских DEF
                        SELECT /*++ parallel(b 5) */  
                               b.ROWID rd, b.cdr_id, b.item_id, b.bill_id 
                          FROM BDR_OPER_T b, 
                               PIN.RS20_ZONES_AUDIT a 
                         WHERE b.rep_period BETWEEN l_Rep_From AND l_Rep_To 
                           AND b.local_time BETWEEN l_Date_From AND l_Date_To 
                           AND b.bdr_type_id = l_BDR_Type_Id
                       --    AND b.trf_type IN (' || l_Trf_Type || ') 
                           AND b.trf_type IN (pin.pk00_const.c_OP_RATE_PLAN_TYPE_RI, -- только для этих типов используется Питерский DEF в расчетах
                                              pin.pk00_const.c_OP_RATE_PLAN_TYPE_DI) 
                           AND a.def_h_id = DECODE(p_Data_Type, 'SPB', pk1110_opr_tariffing.gc_Zone_Def_SPb_Id,
                                                                'NOVTK', pk1110_opr_tariffing.gc_Zone_Def_Nvr_Id
                                                  )              
                           AND a.save_date >= l_Date_From  
                           AND b.modify_date <= a.save_date 
                           AND b.abn_b LIKE a.prefix || '%'  
                        UNION ALL  -- изменения в справочнике услуг операторов
                        SELECT /*++ parallel(b 5) */  
                               b.ROWID rd, b.cdr_id, b.item_id, b.bill_id 
                          FROM BDR_OPER_T b, 
                               PIN.RSX07_SRV_DCT_AUDIT a 
                         WHERE b.rep_period BETWEEN l_Rep_From AND l_Rep_To 
                           AND b.local_time BETWEEN l_Date_From AND l_Date_To 
                           AND b.bdr_type_id = l_BDR_Type_Id 
                        --   AND b.trf_type IN (' || l_Trf_Type || ') 
                           AND a.date_save >= l_Date_From  
                           AND b.modify_date <= a.date_save 
                           AND b.subservice_id = a.srv_id    
                         UNION ALL  -- изменения в общем справочнике услуг
                        SELECT /*++ parallel(b 5) */  
                               b.ROWID rd, b.cdr_id, b.item_id, b.bill_id 
                          FROM BDR_OPER_T b, 
                               PIN.RS51_SERV_SUBSERV_AUDIT a 
                         WHERE b.rep_period BETWEEN l_Rep_From AND l_Rep_To 
                           AND b.local_time BETWEEN l_Date_From AND l_Date_To 
                           AND b.bdr_type_id = l_BDR_Type_Id 
                        --   AND b.trf_type IN (' || l_Trf_Type || ') 
                           AND a.date_save >= l_Date_From  
                           AND b.modify_date <= a.date_save 
                           AND b.parent_subsrv_id = a.subservice_id  -- в parent_subsrv_id лежит x07_srv_dct.subservice_id, через который ищется услуга        
                         UNION ALL  -- изменения по общим данным тарифов
                        SELECT /*++ parallel(b 5) */  
                               b.ROWID rd, b.cdr_id, b.item_id, b.bill_id 
                          FROM BDR_OPER_T b, 
                               PIN.RSX07_OP_RATE_PLAN_AUDIT a 
                         WHERE b.rep_period BETWEEN l_Rep_From AND l_Rep_To 
                           AND b.local_time BETWEEN l_Date_From AND l_Date_To 
                           AND b.bdr_type_id = l_BDR_Type_Id 
                        --   AND b.trf_type IN (' || l_Trf_Type || ') 
                           AND a.date_save >= l_Date_From  
                           AND b.modify_date <= a.date_save 
                           AND b.op_rate_plan_id = a.op_rate_plan_id 
                         UNION ALL  -- изменения по данным доходных тарифов
                    -- ограничиваем изменения диапазонов только теми ТП, которые могут принадлжать тарифам данного заказа
                    -- иначе при внесении диапазонов вида 78121000000-78124999999 большие пересчеты идут.
                    -- Возможные ТП ищем по текущей X07_OP_RATE_PLAN. Изменения по самим ТП отслеживаются выше, по таблице X07_OP_RATE_PLAN_AUDIT 
                        SELECT /*++ parallel(b 5) ordered */  
                               b.ROWID rd, b.cdr_id, b.item_id, b.bill_id 
                          FROM BDR_OPER_T b, 
                               PIN.X07_OP_RATE_PLAN p,  
                               PIN.RSX07_ORD_SERVICE_D_TM_AUDIT a   
                         WHERE b.rep_period BETWEEN l_Rep_From AND l_Rep_To 
                           AND b.local_time BETWEEN l_Date_From AND l_Date_To 
                           AND b.bdr_type_id = l_BDR_Type_Id 
                           AND b.trf_type IN (pin.pk00_const.c_OP_RATE_PLAN_TYPE_DT,
                                              pin.pk00_const.c_OP_RATE_PLAN_TYPE_DI,
                                              pin.pk00_const.c_OP_RATE_PLAN_TYPE_DIP) 
                           AND b.rateplan_id = p.rateplan_id 
                           AND b.trf_type = p.op_rate_plan_type 
                           AND p.op_rate_plan_id = a.op_rate_plan_id                                                                    
                           AND a.date_save >= l_Date_From  
                           AND b.modify_date <= a.date_save 
                           AND SUBSTR(
                                   DECODE(b.trf_type, pin.pk00_const.c_OP_RATE_PLAN_TYPE_DT, b.abn_b,  -- 1
                                                      pin.pk00_const.c_OP_RATE_PLAN_TYPE_DI, b.abn_a,  -- 2
                                                      pin.pk00_const.c_OP_RATE_PLAN_TYPE_DIP, b.abn_b) -- 5
                                   ,1,pin.pk00_const.c_Mask_Length) = TO_CHAR(a.mask_value) 
                         UNION ALL  -- изменения по данным расходных тарифов
                    -- ограничиваем изменения диапазонов только теми ТП, которые могут принадлжать тарифам данного заказа
                    -- иначе при внесении диапазонов вида 78121000000-78124999999 большие пересчеты идут.
                    -- Возможные ТП ищем по текущей X07_OP_RATE_PLAN. Изменения по самим ТП отслеживаются выше, по таблице X07_OP_RATE_PLAN_AUDIT                    
                        SELECT /*+ ordered */ -- parallel(b 5)  
                               b.ROWID rd, b.cdr_id, b.item_id, b.bill_id 
                          FROM BDR_OPER_T b, 
                               PIN.X07_OP_RATE_PLAN p,  
                               PIN.RSX07_ORD_SERVICE_R_TM_AUDIT a 
                         WHERE b.rep_period BETWEEN l_Rep_From AND l_Rep_To 
                           AND b.local_time BETWEEN l_Date_From AND l_Date_To 
                           AND b.bdr_type_id = l_BDR_Type_Id 
                           AND b.trf_type IN (pin.pk00_const.c_OP_RATE_PLAN_TYPE_RT,
                                              pin.pk00_const.c_OP_RATE_PLAN_TYPE_RI, 
                                              pin.pk00_const.c_OP_RATE_PLAN_TYPE_RIP) 
                           AND b.rateplan_id = p.rateplan_id 
                           AND b.trf_type = p.op_rate_plan_type 
                           AND p.op_rate_plan_id = a.op_rate_plan_id                                                                                   
                           AND a.date_save >= l_Date_From  
                           AND b.modify_date <= a.date_save 
                           AND SUBSTR(
                                  DECODE(b.trf_type,pin.pk00_const.c_OP_RATE_PLAN_TYPE_RT, b.abn_b,  -- 3
                                                    pin.pk00_const.c_OP_RATE_PLAN_TYPE_RI, b.abn_a,  -- 4
                                                    pin.pk00_const.c_OP_RATE_PLAN_TYPE_RIP, b.abn_b) -- 6
                                    ,1,pin.pk00_const.c_Mask_Length) = TO_CHAR(a.mask_value)                                   
                         UNION ALL  -- изменения по расценкам стандартных (необъемных) тарифов
                        SELECT /*++ parallel(b 5) */  
                               b.ROWID rd, b.cdr_id, b.item_id, b.bill_id 
                          FROM BDR_OPER_T b, 
                               PIN.RSX07_ORD_PRICE_AUDIT a 
                         WHERE b.rep_period BETWEEN l_Rep_From AND l_Rep_To 
                           AND b.local_time BETWEEN l_Date_From AND l_Date_To 
                           AND b.bdr_type_id = l_BDR_Type_Id 
                         --  AND b.trf_type IN (' || l_Trf_Type || ') 
                           AND a.date_save >= l_Date_From  
                           AND b.modify_date <= a.date_save 
                           AND b.price_id = a.rec_id 
                         UNION ALL  -- изменения по номерам заказов, которые исключаются из счетов
                        SELECT /*++ parallel(b 5) */  
                               b.ROWID rd, b.cdr_id, b.item_id, b.bill_id 
                          FROM BDR_OPER_T b, 
                               PIN.RSX07_BILL_EXCLUDE_AUDIT a 
                         WHERE b.rep_period BETWEEN l_Rep_From AND l_Rep_To 
                           AND b.local_time BETWEEN l_Date_From AND l_Date_To 
                           AND b.bdr_type_id = l_BDR_Type_Id 
                         --  AND b.trf_type IN (' || l_Trf_Type || ') 
                           AND a.date_save >= l_Date_From  
                           AND b.modify_date <= a.date_save 
                           AND b.abn_a = a.phone_num 
                           AND b.order_id = a.order_id                     
                      )      
                 GROUP BY rd, cdr_id, item_id, bill_id;

            l_PrepCnt := SQL%ROWCOUNT;

          /*  l_IdCursor := DBMS_SQL.OPEN_CURSOR;
            
            DBMS_SQL.PARSE(C             => l_IdCursor,
                           STATEMENT     => l_SQL,
                           language_flag => DBMS_SQL.NATIVE);
            DBMS_SQL.BIND_VARIABLE(C     => l_IdCursor,
                                   NAME  => 'l_RDate_From',
                                   VALUE => l_Rep_Period);
            DBMS_SQL.BIND_VARIABLE(C     => l_IdCursor,
                                   NAME  => 'l_RDate_To',
                                   VALUE => l_Rep_Period+1-1/86400);                           
            DBMS_SQL.BIND_VARIABLE(C     => l_IdCursor,
                                   NAME  => 'l_Date_From',
                                   VALUE => l_Date_From);
            DBMS_SQL.BIND_VARIABLE(C     => l_IdCursor,
                                   NAME  => 'l_Date_To',
                                   VALUE => l_Date_To);
            DBMS_SQL.BIND_VARIABLE(C     => l_IdCursor,
                                   NAME  => 'l_Max_Date',
                                   VALUE => gc_DATE_END);                                                                      
            DBMS_SQL.BIND_VARIABLE(C     => l_IdCursor,
                                   NAME  => 'l_BDR_Type',
                                   VALUE => l_BDR_Type_Id);                                   
              
            l_PrepCnt := DBMS_SQL.EXECUTE(l_IdCursor);
            DBMS_SQL.CLOSE_CURSOR(l_IdCursor);*/
           -- 
           -- Удаляем данные из закрытых счетов
           --
            DELETE FROM PIN.TMP03_CDR_BIND t 
             WHERE EXISTS (SELECT 1
                             FROM BILL_T b 
                            WHERE b.rep_period_id = pk00_const.Get_Period_Id(l_Rep_Period)
                              AND b.bill_status NOT IN (PK00_CONST.c_BILL_STATE_OPEN)
                              AND b.bill_type   = PK00_CONST.c_BILL_TYPE_REC
                              AND b.bill_id = t.bill_id
                          );
                                    
            l_PrepCnt := l_PrepCnt - SQL%ROWCOUNT;
            
        END IF;

       
        IF l_PrepCnt > 0 THEN -- если есть данные, предназначенные для перетарификаци, то пересчитываем их
            
            -- Делаем полную перепривязку, т.к. не охота пока мучится с л/счетами у которых задвоенные ресурсы. 
            -- Потом может переделаю, сейчас некогда
            
           -- проверка, есть ли отдельная таблица, где может лежать трафик за заданный день (ищем по дате конца) 
            l_CDR_Tbl := PIN.Get_CDR_Table_Name(p_Data_Type  => p_Data_Type,
                                                p_Day        => l_Date_To,
                                                p_Tbs_Stat   => l_TbsStat
                                               );     

           --- получаем SID сессии
            SELECT SID INTO l_SID
              FROM v$mystat
             WHERE ROWNUM = 1;

            --- формируем имя таблицы, где будут ссылки на пересчитанные CDR-ы с новыми данные
            l_Tmp_Table := 'QT' || TO_CHAR(SYSDATE,'ddmmyyyyhh24miss') || TO_CHAR(l_SID);     
        
            -- получаем rowid cdr-ов 
            EXECUTE IMMEDIATE
                'INSERT INTO PIN.TMP02_ROWS_CALC(row_id, trf_side) ' ||  
                'SELECT c.rowid, ' ||
                '       (CASE WHEN MIN(t.trf_side) != MAX(t.trf_side) THEN MIN(t.trf_side) || '','' || MAX(trf_side)' ||
                '             ELSE MIN(t.trf_side)' ||
                '        END) trf_side ' || CHR(10) ||       
                '  FROM TMP03_CDR_BIND t, ' ||
                        l_BDR_Table || ' b, ' || CHR(13) ||
                        l_CDR_Tbl || ' c ' || CHR(13) ||            
                ' WHERE b.rep_period BETWEEN :l_RDate_From AND :l_RDate_To ' || CHR(13) ||
                '   AND b.local_time BETWEEN :l_Date_From AND :l_Date_To ' || CHR(13) ||
                '   AND b.bdr_type_id = :l_BDR_Type ' || CHR(10) ||
                '   AND c.ans_time BETWEEN :l_Date_From AND :l_Date_To '  || CHR(13) ||
                '   AND t.row_id = b.ROWID '  || CHR(13) ||
                '   AND b.cdr_id = c.cdr_id '  || CHR(13) ||
                ' GROUP BY c.rowid'
            USING l_Rep_From, l_Rep_To,
                  l_Date_From, l_Date_To,
                  l_BDR_Type_Id,
                  l_Date_From, l_Date_To;       
        
            Pk01_Syslog.Write_Msg(p_Msg => 'ОK Period: ' || TO_CHAR(l_Date_From,'dd.mm.yyyy') ||
                                           ' - ' || TO_CHAR(l_Date_To,'dd.mm.yyyy') ||
                                           ' (' || pk00_const.Get_Period_Id(l_Rep_Period) || ') ' ||
                                           ', Prep: ' || TO_CHAR(l_PrepCnt) ||
                                           ', CDR found: ' || TO_CHAR(SQL%ROWCOUNT) || 
                                           ', Task_Id: ' || TO_CHAR(p_Task_Id) ||
                                           ', Src: ' || p_Data_Type,   
                                  p_Src => gc_PkgName || '.' || v_prcName);                          
        
           -- перепривязка л/счетов
            Pk120_Bind_Operators.Bind_XTTK_Opers(p_Data_Type  => p_Data_Type,
                                                     p_Date_From  => l_Date_From,
                                                     p_Date_To    => l_Date_To,
                                                     p_Pivot_Tbl  => 'PIN.TMP02_ROWS_CALC',
                                                     p_Result_Tbl => l_Tmp_Table,
                                                     p_Upd_CDR    => FALSE,
                                                     p_Id_Log     => (CASE WHEN p_LOG = TRUE THEN p_Task_Id
                                                                           ELSE 0 
                                                                      END),
                                                     p_Full_Bind  => FALSE 
                                                    );
                                                                          
            PK1110_OPR_TARIFFING.Load_BDR_XTTK(p_Data_Type      => p_Data_Type,
                                                   p_Data_Table     => l_Tmp_Table,
                                                   p_Date_From      => l_Date_From,
                                                   p_Date_To        => l_Date_To,
                                                   p_Rep_Period     => l_Rep_Period,
                                                   p_Task_Id        => p_Task_Id,
                                                   p_Test_BDR_Table => p_Test_BDR_Table
                                                  );

            IF p_Test_BDR_Table IS NULL THEN

               -- обновляем данные о л/с в таблице CDR-ов
                l_Update := Pk120_Bind_Operators.Update_CDR_Op_Id(p_Data_Type    => p_Data_Type,
                                                                      p_Data_Table   => l_Tmp_Table,
                                                                      p_Date_From    => l_Date_From,
                                                                      p_Date_To      => l_Date_To,
                                                                      p_Id_Log       => 0
                                                                     );           
               
                pin.Pk01_Syslog.Write_Msg(p_Msg => 'CDR updated: ' || TO_CHAR(l_Update), 
                                          p_Src => 'Pk120_Bind_Operators.Update_CDR_Op_Id');                                                       

            END IF;

            COMMIT;                                               

           -- удаляем промежуточную таблицу
            EXECUTE IMMEDIATE 'DROP TABLE ' || l_Tmp_Table || ' PURGE ';
            
        ELSE   

            Pk01_Syslog.Write_Msg(p_Msg => 'ОK Period: ' || TO_CHAR(l_Date_From,'dd.mm.yyyy hh24:mi:ss') || 
                                           ' - ' || TO_CHAR(l_Date_To,'dd.mm.yyyy hh24:mi:ss') ||
                                           ' (' || pk00_const.Get_Period_Id(l_Rep_Period) || '). ' ||
                                           ' Data for retrf not found.' ||
                                           '  Task_Id: ' || TO_CHAR(p_Task_Id) ||
                                           ', Src: ' || p_Data_Type,   
                                  p_Src => gc_PkgName || '.' || v_prcName);                             
        END IF; -- были найдены данные на перетарификацию   
           
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP03_CDR_BIND ';        
        EXECUTE IMMEDIATE 'TRUNCATE TABLE PIN.TMP02_ROWS_CALC';        
        
        l_Date_From := l_Date_To + 1/86400;
        l_Date_To   := LEAST(pin.Get_End_Period(l_Date_From,p_Chunk), 
                             p_Date_To);


        EXIT WHEN l_Date_From > p_Date_To; -- выход, если все в заданном периоде загружено

        IF l_Prev_RP IS NOT NULL AND l_Prev_RP != TRUNC(l_Rep_Period,'mm') AND
           p_Test_BDR_Table IS NULL
        THEN
        
            PK1110_OPR_TARIFFING.Recalc_Op_V_Tariff(p_Data_Type   => p_Data_Type,
                                                        p_Rep_Period  => l_Prev_RP,
                                                        p_Modify_Date => SYSDATE);        
        
            IF p_Load_Items = TRUE THEN
               -- Обновление строк счетов, т.к. сменился отчетный период
                pk114_items.Load_BDR_to_Item(p_Data_Type  => p_Data_Type,
                                             p_Period     => l_Prev_RP,
                                             p_Account_Id => NULL);        
                                                      
                pk114_items.Load_Op_MinPay(p_Data_Type  => p_Data_Type,
                                           p_Rep_Period => l_Prev_Rp,
                                           p_Call_Month => NULL
                                          );                           
            END IF;
            
        END IF;
                    
        l_Prev_RP := TRUNC(l_Rep_Period,'mm');
            
        -- Перед началом расчета следующего периода проверяем нет ли запросов на обновление ресурсов в МТУ 
     /*   IF mdv.pk21_lock.Check_Lock_Req(p_Mode      => DBMS_LOCK.SX_MODE, 
                                        p_Lock_Name => mdv.pk21_lock.c_LOCK_RS) > 0 
        THEN
          /*  IF p_Source > 0 THEN
                UpdateStatGUI(p_Task_Id => p_Source, 
                              p_Msg     => 'Ожидание разрешения на доступ к ресурсам.',
                              p_Percent => ROUND(100/l_Days_Cnt*l_Curr_Day, 1)
                              );        
            END IF;*/             
         /*      
            -- снимаем текущую блокировку
            mdv.pk21_lock.Unlock_Resource; 
            -- ждем пока не будут запущены процессы с более высоким приоритетом
            mdv.pk21_lock.Wait_Req_Lock(p_Mode      => DBMS_LOCK.SX_MODE,
                                        p_Lock_Name => mdv.pk21_lock.c_LOCK_RS); 
            -- устанавливаем блокировку
            mdv.pk21_lock.LOCK_RESOURCE(p_Mode      => DBMS_LOCK.SX_MODE,
                                        p_Lock_Name => mdv.pk21_lock.c_LOCK_RS);
        END IF;  */  

        l_Curr_Day := l_Curr_Day + 1;

    END LOOP; -- перебор дней в заданном периоде

   -- EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP05_HISTORY_OF_TARIFF';
    IF p_Test_BDR_Table IS NULL THEN
        PK1110_OPR_TARIFFING.Recalc_Op_V_Tariff(p_Data_Type   => p_Data_Type,
                                                    p_Rep_Period  => l_Rep_Period,
                                                    p_Modify_Date => SYSDATE);   
    END IF;                                                

    Pk01_Syslog.Write_Msg(p_Msg => 'Period was calculated successfully.',     
                          p_Src => gc_PkgName || '.' || v_prcName);                          
       
    IF p_Load_Items = TRUE AND p_Test_BDR_Table IS NULL THEN    
    
       -- Обновление строк счетов, т.к. сменился отчетный период
        pk114_items.Load_BDR_to_Item(p_Data_Type  => p_Data_Type,
                                     p_Period     => l_Rep_Period,
                                     p_Account_Id => NULL);        
        
        pk114_items.Load_Op_MinPay(p_Data_Type  => p_Data_Type,
                                   p_Rep_Period => l_Rep_Period,
                                   p_Call_Month => NULL
                                  );                        
    END IF;                                                                         
   
    -- снимаем блокировку     
 --   mdv.pk21_lock.Unlock_Resource;
    
  --  IF p_Source > 0 THEN
       -- отметка о ходе выполнения в таблицу с заданием
  --      UpdateStatGUI(p_Task_Id => p_Source, p_Msg => 'Выполнено.', p_Percent => 100);
  --  END IF;                

 
EXCEPTION
    WHEN ERR_DATA THEN
        NULL;

/*    WHEN ERR_BDR_TYPE THEN 
        mdv.pk21_lock.Unlock_Resource;
    WHEN CLOSED_PERIOD THEN
        mdv.pk21_lock.Unlock_Resource;
        Pk01_Syslog.Write_to_log(p_Msg => 'Попытка перетарификации закрытого периода', 
                                 p_Src => c_PkgName || v_prcName);    
    WHEN BDR_RO THEN
        mdv.pk21_lock.Unlock_Resource;
        Pk01_Syslog.Write_to_log(p_Msg => 'BDR TABLE (' || l_BDR_Table || ') IS READ ONLY', 
                                 p_Src => c_PkgName || v_prcName);
                                 
    WHEN OTHERS THEN
        mdv.pk21_lock.Unlock_Resource;    
        Pk01_Syslog.Err_to_log(p_src => c_PkgName || v_prcName);
        ROLLBACK;
        IF DBMS_SQL.IS_OPEN(l_IdCursor) = TRUE THEN
            DBMS_SQL.CLOSE_CURSOR(l_IdCursor);
        END IF;
        RAISE;       */                                          
END ReTrfBDR;




END Pk1110_Opr_ReTrf;
/
