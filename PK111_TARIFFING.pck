CREATE OR REPLACE PACKAGE PK111_TARIFFING
IS

    gc_PkgName       CONSTANT varchar2(36) := 'PK111_TARIFFING';
    gc_MaxDate       CONSTANT date := TO_DATE('01.01.2050','dd.mm.yyyy'); 
    gc_MinDate       CONSTANT date := TO_DATE('01.01.1900','dd.mm.yyyy');    

    gc_DEL_DBL_FROM_TMP CONSTANT NUMBER := 1; -- если при загрузке привязанных CDR эти же записи уже
          -- были внесены в таблицу BDR раньше, то надо оставить старые записи в BDR. 
          -- Новые, соответственно удаляются.
    gc_DEL_DBL_FROM_BDR CONSTANT NUMBER := 2; -- если при загрузке привязанных CDR эти же записи уже
          -- были внесены в таблицу BDR раньше, то надо удалить старые записи в BDR и внести новые данные.

    -- имена таблиц
   -- g_Tbl_BDR_MNMG   varchar2(30) := 'E01_BDR_MMTS_T'; -- таблица с BDR-ами

    -- Исходные данные привязки CDR к субъекту и тарификации
    TYPE rec_cdr IS RECORD (Cdr_Id                number,
                            sw_name               varchar2(16),
                            Data_Type             varchar2(16),
                            Bill_Date             date,
                            UTC_Offset            INTERVAL DAY(0) TO SECOND(0),
                            Duration              number,
                            Abn_A                 varchar2(34),
                            Abn_B                 varchar2(34),
                            Called_Num            varchar2(34),
                            Called_Address_Nature number,
                            terminating_reason    integer,
                            termination_code      integer,
                            Service_Id            number,
                            Order_Id              varchar2(32),
                            Order_Ph              varchar2(34),
                            row_id                rowid,
                            bill_id               number  
                           );
                                  
    TYPE ref_cdr IS REF CURSOR RETURN rec_cdr;



    /* ------------------------------------------------------------------------------   
      Процедура для привязки/тарификации клиентских л/с по стороне А по данным схемы TRIFF_PH
      Входные параметры:
         p_Data_Type - тип данных (по таблице pin.bdr_types_t.bdr_code)      
         p_Date_From - дата начала тарифицируемого периода
         p_Date_To   - дата конца тарифицируемого периода
         p_Month     - Дата, определяющая в какую партицию B01_BDR ляжет трафик. По умолчанию NULL - 
             т.е. B01_BDR.CALC_DATE = T03_MMTS_CDR.START_TIME. Если указано значение IS NOT NULL, то данные лягут 
             в последний день месяца указанной даты. Например задано значение 21.10.2007 23:12:01 - данные лягут
             с calc_date = 31.10.2007 (это сделано для уменьшения кол-ва просмотриваемых партиций в таблице B01_BDR при
             проверках на задвой). Так же, если задана дата расчета и текущий месяц и месяц указанной даты  не 
             совпадают, то местный трафик будет положен в BDR, т.к. в противном случае он может не попасть в 
             расчеты из-за невозможности обновления таблиц CDR-ов.
         p_Bill - TRUE - данные тарифицируются для биллинга. В дальнейшем эти данные 
                         будут исключаться из каких-либо расчетов. (кроме перетарификаций)
                  FALSE - тестовые или какие либо другие тарификации, напрмер подготовка предварительных данных для 
                          просмотра ошибок и т.п.        
         p_Test_BDR_Table   - NULL - стандартная тарификация, имя таблица ВDR - тестовая тарификция, т.е.  
                        данные кладутся в указанную таблицу BDR-ов, 
                        а параметры p_Bill, p_Month игнорируются, т.е. будут использованы их 
                        значения по-умолчанию. А так же не будет произведено проверок на задвои.
         p_Load_Resources - обновление ресурсов         
         p_Load_Items - TRUE - пересчитывать item-ы. Параметр, используемый по-умолчанию. Item-ы пересчитываются в соответсвии с новыми
                                    результатами   
                        FALSE - не пересчитывать item-ы                      
      Примечание: при тарификации заданный период разбивается на дни и загружается уже по дням, как раз данные
          будут браться по партициям                         
      ------------------------------------------------------------------------------- */                           
    PROCEDURE Trf_Cl_A(p_Data_Type      IN varchar2,
                       p_Date_From      IN DATE     DEFAULT TRUNC(SYSDATE) - 1,  -- дата начала тарифицируемого периода
                       p_Date_To        IN DATE     DEFAULT TRUNC(SYSDATE) - 1/86400,  -- дата конца тарифицируемого периода
                       p_Test_BDR_Table IN varchar2 DEFAULT NULL,
                       p_Load_Resources IN BOOLEAN  DEFAULT FALSE,
                       p_Load_Items     IN BOOLEAN  DEFAULT TRUE
                      );


    /*
     процедура для загрузки привязанных данных в BDR и обновления соответствующих данных в CDR. (Сторона А)
       Входные параметры:
           p_Data_Type   - что тарифицируем (MMTS, SPB, ZONE, Самара и т.д.). 
                           Доступные значния в dictionary_t key = BDR_SOURCES    
           p_Data_Table - таблица с привязанными данными, которые надо протарифицировать и загрузить в ВDR-ы
           p_Day        - обрабатываемый день. Т.е. это день, соединени яза который
                          находятся в табл., имя которой передано в параметре p_Data_Table 
           p_Rep_Period - отчетный период, куда надо положить BDR-ы. Если NULL - то текущий.
           p_Task_Id    - идентификатор задания. Используется просто для удобства просмотра логов                       
    */
    PROCEDURE Load_BDR_Cl_A(p_Data_Type      IN varchar2,
                            p_Data_Table     IN VARCHAR2,
                            p_Day            IN DATE,
                            p_Rep_Period     IN OUT DATE,
                            p_Task_Id        IN NUMBER   DEFAULT NULL,
                            p_Test_BDR_Table IN varchar2 DEFAULT NULL
                           );

    -- Расчет предоплаченного местного тарфика
    -- (процедура обнуляет значения суммы и тарифцируемых минут у соединений, вошедших в указанный объем) 
    PROCEDURE Calc_Local_Free_Traffic(p_Period    date,
                                      p_Data_Type varchar2);


    FUNCTION Trf_Cl_A_Table(pr_Call     ref_cdr,
                            p_Period_Id number,
                            p_Task_Id   number,
                            p_Agent number DEFAULT 0
                           )
                           RETURN PIN.BDR_PH_COLL
                           PIPELINED PARALLEL_ENABLE (PARTITION pr_Call BY ANY);
                           
    FUNCTION Trf_MMTS(pr_Call rec_cdr,
                      p_Agent number DEFAULT 0
                     ) RETURN PIN.BDR_PH_TYPE;

    FUNCTION Trf_Zone(pr_Call rec_cdr,
                      p_Agent number DEFAULT 0
                     ) RETURN PIN.BDR_PH_TYPE;

    -- Возвращет тип соединения (МГ(1), МН(2)) по таблице subservice_t  
    FUNCTION Get_MMTS_SubSrv_Type(p_Abn_A varchar2,
                                  p_Abn_B varchar2
                                 ) RETURN number PARALLEL_ENABLE;
                                 
    FUNCTION Get_SPb_SubService_Id(p_Abn_A varchar2,
                                   p_Abn_B varchar2) RETURN number;                                 

    FUNCTION Get_Msc_SubService_Id(p_Abn_A varchar2,
                                   p_Abn_B varchar2) RETURN number;

   -- Ищет префикс заданного телевонного номера среди московских номеров и если находит,
    -- то возвращает префикс макс. длины. Если не находит - NULL
    FUNCTION Get_Msc_Local(p_Ph_Num IN varchar2
                          ) RETURN varchar2; 

    -- Функция для поиска направления (не DEF) по номеру телефона.
    -- Возвращает 0  - направление найдено,
    --            -1 - направленпие не найдено   
 /*   FUNCTION Get_ABC_Pref(p_PH_Num      IN  varchar2,
                          p_Prefix      OUT varchar2,
                          p_Z_Id        OUT number,
                          p_Z_Name      OUT varchar2,
                          p_Z_Id_Parent OUT number,
                          p_City_FZ     OUT varchar2                          
                         ) RETURN number;
                         
    -- Функция для поиска направления (не DEF) по номеру телефона.
    -- Прим: если указан параметр p_ABC_H_Id, то ищет номера DEF в указанном регионе только
    -- Возвращает 0  - направление найдено,
    --            -1 - направление не найдено   
    FUNCTION Get_DEF_Pref(p_PH_Num      IN  varchar2,
                          p_ABC_H_Id    IN  varchar2 DEFAULT NULL,
                          p_Prefix      OUT varchar2,
                          p_Z_Id        OUT number,
                          p_Z_Name      OUT varchar2,
                          p_Z_Id_Parent OUT number,
                          p_City_FZ     OUT varchar2                          
                         ) RETURN number;   */                        

   -- Ф-ция для получения услуги по номерам А и Б.
    FUNCTION Get_SubSrv_Id(p_PhNum_A  IN varchar2,
                           p_PhNum_B  IN varchar2
                          ) RETURN number;

   -- Ф-ция для получения услуги по номерам А и Б. Возвращает так же данные по зонам инициирования и завершения
    FUNCTION Get_SubSrv_Id_With_Dir(p_PhNum_A  IN varchar2,
                                    p_PhNum_B  IN varchar2,
                                    p_Z_Id_A   OUT number,
                                    p_Z_Name_A OUT varchar2,                                
                                    p_Prefix_A OUT varchar2,
                                    p_Z_Id_B   OUT number,
                                    p_Z_Name_B OUT varchar2,                                
                                    p_Prefix_B OUT varchar2,
                                    p_Type_B   OUT varchar2 -- A - звонок на ABC, B - звонок на DEF                                
                                   ) RETURN number;

  -- Функция для поиска направления по номеру телефона.
  -- Возвращает Z_ID - id направления, если найдено,
  --            NULL - направление не найдено   
  FUNCTION Get_Dir(p_PH_Num      IN  varchar2,
                   p_Prefix      OUT varchar2,
                   p_Z_Name      OUT varchar2
                  ) RETURN number;

    -- Загрузка данных для тарифцикации в массивы
    PROCEDURE Load_Trf_Data;
                                   
END PK111_TARIFFING;
/
CREATE OR REPLACE PACKAGE BODY PK111_TARIFFING
IS

   ------
   --- Типы
   -----
    TYPE t_Date IS TABLE OF DATE;
  
    TYPE t_Ref  IS REF CURSOR;

    TYPE t_Num   IS TABLE OF number INDEX BY PLS_INTEGER;
    TYPE t_Varch IS TABLE OF varchar2(128) INDEX BY varchar2(100);
    TYPE t_VNum  IS TABLE OF number INDEX BY varchar2(20);

    t_BDR_Status   t_Varch;
    t_Subservice   t_Varch;
    t_Reason_Name  t_Varch;
    t_Network_Code t_Varch;
    t_Switch       t_Varch;
    gt_Calendar    t_VNum; -- данные о праздниках и переносах
    
    gt_Order_Smeta     t_Num; -- список заказов с признаком расчетов по СМЕТА
    gt_Ag_Order_Smeta  t_Num; -- список агентских заказов с признаком расчетов по СМЕТА
    
    TYPE t_VVarch IS TABLE OF t_Varch INDEX BY varchar2(20);
    
    t_Term_Reason t_VVarch;
    
    TYPE t_Dict IS RECORD (key_id integer,
                           NAME   varchar2(64)
                          );
                          
    TYPE t_Q805 IS TABLE OF t_Dict INDEX BY varchar2(16);                      
    
    t_q805_Name   t_Q805;    

  -- Массив для правил округления
    gt_Rounding t_Num;

  -- Массив для кэширования данных по направлениям
    TYPE tr_Zone IS RECORD(z_id        number, 
                           z_name      varchar2(128), 
                           z_id_parent number, 
                           city_fz     varchar2(1), 
                           mg_mn       number, 
                           ph_type     varchar2(1),
                           h_id        number
                          );
                          
    TYPE t_Zone IS TABLE OF tr_Zone INDEX BY varchar2(32);      
    
    gt_Zone t_Zone;                  
    
   -- Массив для кэширования тарифов
    TYPE tr_Tariff IS RECORD(trf_id         number, 
                             round_v_id     number, 
                             unpaid_seconds number, 
                             zmdl_id        number, 
                             is_8800_MP     varchar2(1)
                            );

    TYPE t_Tariff IS TABLE OF tr_Tariff INDEX BY varchar2(128);      
    
    gt_Tariff t_Tariff;                  

   -- Тарифы с индивидуальным льготным временем 
    TYPE r_TrfPrefTime IS RECORD(bt_mg_from  varchar2(12),
                                 bt_mg_to    varchar2(12),
                                 bt_mn_from  varchar2(12),
                                 bt_mn_to    varchar2(12)
                                );

    TYPE t_TrfPrefTime IS TABLE OF r_TrfPrefTime INDEX BY PLS_INTEGER;      
    
    gt_TrfPrefTime t_TrfPrefTime;                  

   -- Для хранения рабочего времени зоновых моделей
    TYPE r_Bis_Time IS RECORD(time_from INTERVAL DAY(0) TO SECOND(0),
                              time_to   INTERVAL DAY(0) TO SECOND(0)
                             );
    TYPE t_Bis_Time IS TABLE OF r_Bis_Time;
    
    TYPE t_Zone_Bis_Time IS TABLE OF t_Bis_Time INDEX BY PLS_INTEGER;    
    
    gt_Mn_Bis_Time t_Zone_Bis_Time; 
    gt_Mg_Bis_Time t_Zone_Bis_Time;

   ------
   --- Используемые exceptions
   -----
    BDR_RO         EXCEPTION; -- таблица с BDR в READ ONLY

/* ------------------------------------------------------------------------------   
  Процедура для привязки/тарификации клиентских л/с по стороне А по данным схемы TRIFF_PH
  (таблица T03_MMTS_CDR)
  Входные параметры:
     p_Data_Type - тип данных (по таблице pin.bdr_types_t.bdr_code)
     p_Date_From - дата начала тарифицируемого периода
     p_Date_To   - дата конца тарифицируемого периода
     p_Month     - Дата, определяющая в какую партицию B01_BDR ляжет трафик. По умолчанию NULL - 
         т.е. B01_BDR.CALC_DATE = T03_MMTS_CDR.START_TIME. Если указано значение IS NOT NULL, то данные лягут 
         в последний день месяца указанной даты. Например задано значение 21.10.2007 23:12:01 - данные лягут
         с calc_date = 31.10.2007 (это сделано для уменьшения кол-ва просмотриваемых партиций в таблице B01_BDR при
         проверках на задвой). Так же, если задана дата расчета и текущий месяц и месяц указанной даты  не 
         совпадают, то местный трафик будет положен в BDR, т.к. в противном случае он может не попасть в 
         расчеты из-за невозможности обновления таблиц CDR-ов.
     p_Bill - TRUE - данные тарифицируются для биллинга. В дальнейшем эти данные 
                     будут исключаться из каких-либо расчетов. (кроме перетарификаций)
              FALSE - тестовые или какие либо другие тарификации, напрмер подготовка предварительных данных для 
                      просмотра ошибок и т.п.        
     p_Test_BDR_Table   - NULL - стандартная тарификация, имя таблица ВDR - тестовая тарификция, т.е.  
                    данные кладутся в указанную таблицу BDR-ов, 
                    а параметры p_Bill, p_Month игнорируются, т.е. будут использованы их 
                    значения по-умолчанию. А так же не будет произведено проверок на задвои.
     p_Load_Resources - обновление ресурсов         
     p_Load_Items - TRUE - пересчитывать item-ы. Параметр, используемый по-умолчанию. Item-ы пересчитываются в соответсвии с новыми
                                результатами   
                    FALSE - не пересчитывать item-ы                      
  Примечание: при тарификации заданный период разбивается на дни и загружается уже по дням, как раз данные
      будут браться по партициям         */                
PROCEDURE Trf_Cl_A(
              p_Data_Type      IN varchar2,
              p_Date_From      IN DATE     DEFAULT TRUNC(SYSDATE) - 1,  -- дата начала тарифицируемого периода
              p_Date_To        IN DATE     DEFAULT TRUNC(SYSDATE) - 1/86400,  -- дата конца тарифицируемого периода
              p_Test_BDR_Table IN varchar2 DEFAULT NULL,
              p_Load_Resources IN BOOLEAN  DEFAULT FALSE,
              p_Load_Items     IN BOOLEAN  DEFAULT TRUE 
             )  
IS
    v_prcName    CONSTANT VARCHAR2(16) := 'Trf_Cl_A';
    
    l_Tmp_Table  VARCHAR2(32);
    l_Date_From  DATE := p_Date_From;
    l_Date_To    DATE;
    l_Count      number;
   
    l_Sid        NUMBER;
    l_Rep_Period date;
    l_Prev_RP    date;
    
    l_Days_Cnt   NUMBER := TRUNC(p_Date_To) - TRUNC(p_Date_From) + 1; -- кол-во дней, входящих в заданный период (для логов) 
    l_Curr_Day   NUMBER := 0; -- счетчик перетарфицированных дней (для логов)        
    
BEGIN

   -- Проверка есть ли закрытые периоды в заданном диапазоне дат
    IF p_Test_BDR_Table IS NULL THEN
        IF pk114_items.Check_Rep_Period(p_Date_From, p_Date_To, NULL) = 0
        THEN
            RETURN;
        END IF;     
    END IF;    

    IF p_Load_Resources = TRUE THEN
    
        Pk01_Syslog.Write_Msg(p_Msg => 'Load resources.',     
                              p_Src => gc_PkgName || '.' || v_prcName);        
    
        pk1001_resources.job_daily;
        
        Pk01_Syslog.Write_Msg(p_Msg => 'Resources loaded',     
                              p_Src => gc_PkgName || '.' || v_prcName);        
        
    ELSIF p_Load_Resources = FALSE THEN        
        
        -- Для агентских договоров, тариф. по Смете, грузим в любом случае, т.к. данные используются при тарификации
        pk1001_resources.Load_Res_Smeta;  
      --  NULL;
    END IF;    

    ------------------------------------------------------------------------------------------------------
    -- установить блокировку для таблиц RS, чтобы предотвратить их изменения во время работы процедуры
   -- mdv.pk21_lock.LOCK_RESOURCE(p_Mode      => DBMS_LOCK.SX_MODE,
   --                             p_Lock_Name => mdv.pk21_lock.c_Lock_RS);
    --
    ------------------------------------------------------------------------------------------------------

    Pk01_Syslog.Write_Msg(p_Msg => 'Begin period: ' || p_Data_Type || ', ' ||  TO_CHAR(p_Date_From,'dd.mm.yyyy hh24:mi:ss') || 
                                   ' - ' || TO_CHAR(p_Date_To,'dd.mm.yyyy hh24:mi:ss'),     
                          p_Src => gc_PkgName || '.' || v_prcName);    

    l_Curr_Day := l_Curr_Day + 1;

    --
    --- получаем SID сессии
    SELECT SID INTO l_SID
      FROM v$mystat
     WHERE ROWNUM = 1;

    l_Prev_RP := NULL;

    -- получаем дату окончание первого дня в заданном периоде 
    l_Date_To := LEAST(TRUNC(l_Date_From + 1) - 1/86400, p_Date_To);    
        
    LOOP  -- перебор дней в заданном периоде
        EXIT WHEN l_Date_From > p_Date_To; -- выход, если все в заданном периоде загружено

       ---
       -- если задан расчет по клиентской стороне А, то сначала определяем/привязываем клиентский трафик
       --
        -- Формируем имя темповой таблицы (SYSDATE + SID), где будут храниться ссылки на привязанные CDR-ы
        -- с новыми данными    
        l_Tmp_Table := 'QT' || TO_CHAR(SYSDATE,'ddmmyyyyhh24miss') || TO_CHAR(l_SID);
       
       -- привязываем соединения     
        IF p_Data_Type IN ('MMTS','MMTS_FLT','AG_MMTS') THEN
        
            Pk120_Bind_Clients.Bind_Order_A(p_Data_Type  => p_Data_Type, 
                                            p_Day        => l_Date_From,
                                            p_Pivot_Tbl  => NULL,
                                            p_Result_Tbl => l_Tmp_Table,
                                            p_Upd_CDR    => FALSE,
                                            p_Full_Bind  => (CASE 
                                                              WHEN p_Test_BDR_Table IS NOT NULL THEN
                                                               -- при тестовых расчетах все соединения
                                                                 TRUE
                                                              ELSE 
                                                                 FALSE
                                                             END),    
                                            p_Id_Log     => 0
                                           );
                                                    
        ELSIF p_Data_Type IN ('ZONE','AG_ZONE','ZONE_FLT') THEN
        
            Pk120_Bind_Clients.Bind_Zone_Order_A(p_Data_Type  => p_Data_Type, 
                                                 p_Day        => l_Date_From,
                                                 p_Pivot_Tbl  => NULL,
                                                 p_Result_Tbl => l_Tmp_Table,
                                                 p_Upd_CDR    => FALSE,
                                                 p_Full_Bind  => (CASE 
                                                                    WHEN p_Test_BDR_Table IS NOT NULL THEN
                                                                      -- при тестовых расчетах все соединения
                                                                       TRUE
                                                                    ELSE 
                                                                       FALSE
                                                                  END),    
                                                 p_Id_Log     => 0
                                                );                 
        
        ELSIF p_Data_Type IN ('SPBCLN') THEN
        
            pk120_Bind_Clients.Bind_SPb_Clients(
                                              p_Day        => l_Date_From,
                                              p_Pivot_Tbl  => NULL,
                                              p_Result_Tbl => l_Tmp_Table,
                                              p_Upd_CDR    => FALSE,
                                              p_Full_Bind  => (CASE 
                                                                  WHEN p_Test_BDR_Table IS NOT NULL THEN
                                                                   -- при тестовых расчетах все соединения
                                                                     TRUE
                                                                  ELSE 
                                                                     FALSE
                                                               END),
                                              p_Id_Log     => 0
                                             );         
        
        END IF;            
        
       -- тарификация 
        l_Rep_Period := NULL;
        
        PK111_TARIFFING.Load_BDR_Cl_A(p_Data_Type    => p_Data_Type,
                                      p_Data_Table   => l_Tmp_Table,
                                      p_Day          => l_Date_From,
                                      p_Rep_period   => l_Rep_Period, -- IN OUT
                                      p_Task_Id      => 0,
                                      p_Test_BDR_Table => p_Test_BDR_Table
                                     );
                                     
        IF p_Data_Type IN ('MMTS','ZONE') THEN                                     
           -- для сети MMTS считаем сразу агентские договора                              
            PK111_TARIFFING.Load_BDR_Cl_A(p_Data_Type    => (CASE p_Data_Type 
                                                                  WHEN 'MMTS' THEN 'AG_MMTS'
                                                                  WHEN 'ZONE' THEN 'AG_ZONE'
                                                             END),     
                                          p_Data_Table   => l_Tmp_Table,
                                          p_Day          => l_Date_From,
                                          p_Rep_period   => l_Rep_Period, -- IN OUT
                                          p_Task_Id      => 0,
                                          p_Test_BDR_Table => p_Test_BDR_Table
                                         );   
        END IF;                                                                   
        
        IF p_Test_BDR_Table IS NULL THEN
            
           -- обновляем данные о л/с в таблице CDR-ов
            l_Count := Pk120_Bind_Clients.Update_Order_A_CDR(p_Data_Type  => p_Data_Type,
                                                             p_Data_Table => l_Tmp_Table,
                                                             p_Day        => l_Date_From,
                                                             p_Id_Log     => NULL
                                                            ); 
                                                         
            Pk01_Syslog.Write_Msg(p_Msg => 'Update cdr. '|| p_Data_Type || ', Day: ' || TO_CHAR(l_Date_From,'dd.mm.yyyy') ||
                                           ', count: ' || TO_CHAR(l_Count), 
                                  p_Src => 'Pk120_Bind_Clients.Update_Order_A_CDR');                                                                         

            IF (l_Prev_RP IS NOT NULL AND l_Prev_RP != TRUNC(l_Rep_Period,'mm'))
            THEN
            
                IF p_Data_Type NOT IN ('MMTS','MMTS_FLT','AG_MMTS','ZONE_FLT') -- здесь местного не бывает или не нужен
                THEN
                   -- расчет предоплаченного местного трафика
                    Calc_Local_Free_Traffic(p_Period => l_Prev_RP,
                                            p_Data_Type => p_Data_Type 
                                           );
                END IF;    


                IF p_Load_Items = TRUE THEN          
                   -- Обновление строк счетов, т.к. сменился отчетный период
                    pk114_items.Load_BDR_to_Item(p_Period    => l_Prev_RP,
                                                 p_Data_Type => p_Data_Type);
                END IF;                                 
                                 
            END IF; 

        END IF;        
        
        l_Prev_RP := TRUNC(l_Rep_Period,'mm');
        
        COMMIT;

       -- удаляем промежуточную таблицу
   --     IF p_Test_BDR_Table IS NULL THEN       
            EXECUTE IMMEDIATE 'DROP TABLE ' || l_Tmp_Table || ' PURGE ';
   --     END IF;    

        -- Перед началом расчета следующего периода проверяем нет ли запросов на обновление ресурсов в МТУ 
    /*    IF mdv.pk21_lock.Check_Lock_Req(p_Mode      => DBMS_LOCK.SX_MODE, 
                                        p_Lock_Name => mdv.pk21_lock.c_LOCK_RS) > 0 
        THEN
            -- снимаем все текущие блокировки
            mdv.pk21_lock.Unlock_Resource; 
            -- ждем пока не будут запущены процессы с более высоким приоритетом
            mdv.pk21_lock.Wait_Req_Lock(p_Mode      => DBMS_LOCK.SX_MODE,
                                        p_Lock_Name => mdv.pk21_lock.c_LOCK_RS); 
            -- устанавливаем блокировку
            mdv.pk21_lock.LOCK_RESOURCE(p_Mode      => DBMS_LOCK.SX_MODE,
                                        p_Lock_Name => mdv.pk21_lock.c_LOCK_RS);
        END IF;    */

        l_Date_From := l_Date_To + 1/86400;
        l_Date_To   := LEAST(TRUNC(l_Date_From + 1) - 1/86400,  p_Date_To);        

        l_Curr_Day := l_Curr_Day + 1;

    END LOOP; -- перебор дней в заданном периоде

    -- снимаем установленную блокировку        
   -- mdv.pk21_lock.UNLOCK_RESOURCE; 

   -- +++++++++++++++++++++++++++++++++++++++++++
   -- Обновление строк счетов
    IF p_Test_BDR_Table IS NULL 
    THEN

        IF p_Data_Type NOT IN ('MMTS','MMTS_FLT','AG_MMTS','ZONE_FLT') -- здесь местного не бывает или не нужен
        THEN
           -- расчет предоплаченного местного трафика
            Calc_Local_Free_Traffic(p_Period => l_rep_period,
                                    p_Data_Type => p_Data_Type 
                                   );
        END IF;    
 
        IF p_Load_Items = TRUE THEN
    
            pk114_items.Load_BDR_to_Item(p_Period    => l_rep_period,
                                         p_Data_Type => p_Data_type);
        
        END IF;                                 
   
    END IF;  
    
    -- очищаем таблицу
  --  DELETE FROM TMP05_REP_PERIOD_LOG;
    
    --
    -- -- +++++++++++++++++++++++++++++++++++++++++++ 


    Pk01_Syslog.Write_Msg(p_Msg => 'Period was calculated successfully: ' || p_Data_Type || ', ' || TO_CHAR(p_Date_From,'dd.mm.yyyy hh24:mi:ss') || 
                                   ' - ' || TO_CHAR(p_Date_To,'dd.mm.yyyy hh24:mi:ss'), 
                          p_Src => gc_PkgName || '.' || v_prcName);  
                              
                          
/*    
EXCEPTION
    WHEN ERR_PARAM THEN
        mdv.pk21_lock.UNLOCK_RESOURCE;
        Pk01_Syslog.Write_to_log(p_Msg   => l_SQL, 
                                 p_Src   => c_PkgName || v_prcName,
                                 p_Level => Pk01_Syslog.l_Err);    
    WHEN ERR_BILL_PARAM THEN
        mdv.pk21_lock.UNLOCK_RESOURCE;
        Pk01_Syslog.Write_to_log(p_Msg => 'Ошибка. В закрытом периоде параметр p_Bill не может быть FALSE.', 
                                 p_Src => c_PkgName || v_prcName);
    WHEN OTHERS THEN
        mdv.pk21_lock.UNLOCK_RESOURCE;    
        Pk01_Syslog.Err_to_log(p_src => c_PkgName || v_prcName);
        RAISE;         */
END Trf_Cl_A;                   

/*
 процедура для загрузки привязанных данных в BDR и обновления соответствующих данных в CDR. (Сторона А)
   Входные параметры:
       p_Data_Type   - что тарифицируем (MMTS, SPB, ZONE, Самара и т.д.). 
                       Доступные значния в dictionary_t key = BDR_SOURCES    
       p_Data_Table - таблица с привязанными данными, которые надо протарифицировать и загрузить в ВDR-ы
       p_Day        - обрабатываемый день. Т.е. это день, соединени яза который
                      находятся в табл., имя которой передано в параметре p_Data_Table 
       p_Rep_Period - отчетный период, куда надо положить BDR-ы. Если NULL - то текущий.
       p_Task_Id    - идентификатор задания. Используется просто для удобства просмотра логов                       
*/
PROCEDURE Load_BDR_Cl_A(p_Data_Type      IN varchar2,
                        p_Data_Table     IN VARCHAR2,
                        p_Day            IN DATE,
                        p_Rep_Period     IN OUT DATE,
                        p_Task_Id        IN NUMBER   DEFAULT NULL,
                        p_Test_BDR_Table IN varchar2 DEFAULT NULL
                       )
IS
    v_prcName   CONSTANT VARCHAR2(24) := 'Load_BDR_Cl_A';
    
    l_BDR_Table     VARCHAR2(32);
    l_Data_Type     VARCHAR2(32);
    l_BDR_Type_Id   number;
    l_Calc_Date     DATE   := SYSDATE;
    l_Agent         number;
    l_InsBDRCnt     PLS_INTEGER;
    l_Lock_Name     VARCHAR2(64);
    l_Rep_Period    DATE;
    l_Rep_Period_Id number;
    l_RDate_From    date;
    l_LDate_From    date;
    l_RDate_To      date;
    l_LDate_To      date;
    l_Delete        number;
    l_Update        number;
    l_Items         number;
    
    l_SQL VARCHAR2(4000);
    
    lt_Date      t_Date;
    
    ERR_NET EXCEPTION;
    
BEGIN

   --- 
   ---- ========================================================================
   ---  Загрузка МН,МГ и зоновых соединений в таблицу BDR-ов
   ---  ========================================================================   
   ---
    l_InsBDRCnt := 0;

   -- Получаем имя таблицы BDR-ов, используемой для данного типа расчетов и
   -- тип bdr-ов, под которым будут внесены протарифицированные записи
    l_BDR_Type_Id := PIN.Get_BDR_Type(p_Data_Type => p_Data_Type,
                                      p_BDR_Table => l_BDR_Table, -- out
                                      p_Agent     => l_Agent,      -- out
                                      p_Items     => l_Items 
                                     );     
    
    IF p_Test_BDR_Table IS NOT NULL THEN
    
        l_BDR_Table := p_Test_BDR_Table;
    
    END IF;                  
   
    IF p_Test_BDR_Table IS NULL THEN 
       --- Получение блокировки партиции, куда будет положен результат тарифкации. Делается чтобы 
       -- при одновременном расчете не получилось задвоя трафика.
        l_Lock_Name := mdv.Lock_Partition(p_Owner     => SUBSTR(l_BDR_Table,1,INSTR(l_BDR_Table,'.')-1),
                                          p_Table     => SUBSTR(l_BDR_Table,INSTR(l_BDR_Table,'.')+1),
                                          p_Day       => p_Day,
                                          p_Add_Value => 'A' -- сторона А тарифицируется
                                         );    
    END IF;                                     
    
   -- Получаем отчетный период  
    IF p_Rep_Period IS NULL THEN    
        
        p_Rep_Period := PK114_ITEMS.Get_Period_Date(p_Day       => p_Day,
                                                    p_Calc_Date => l_Calc_Date 
                                                   );        
        
    ELSE
       -- Задан отчетный период. Проверка правильности (закрыт/не закрыт).
        CASE pin.Check_Cls_Period(p_Rep_Period, l_Calc_Date) 
            WHEN 1 THEN
                Pk01_Syslog.Write_Msg(p_Msg   => 'Заданный отчетный период закрыт.',
                                      p_src   => gc_PkgName || '.' || v_prcName,
                                      p_Level => Pk01_Syslog.L_err);
                RETURN;                      
            WHEN 2 THEN                           
                Pk01_Syslog.Write_Msg(p_Msg   => 'В заданном отчетном периоде счета еще не сформированы.',
                                      p_src   => gc_PkgName || '.' || v_prcName,
                                      p_Level => Pk01_Syslog.L_err);
                RETURN;                                                                
            ELSE
               -- Период корректен. Приводим к первому дню месяца или текущему.  
                IF TRUNC(p_Rep_Period,'mm') != TRUNC(p_Day,'mm') THEN      
                    p_Rep_Period := TRUNC(p_Rep_Period,'mm');
                ELSIF TRUNC(p_Rep_Period,'mm') = TRUNC(p_Day,'mm') THEN
                    p_Rep_Period := TRUNC(p_Day);
                END IF;   
                
        END CASE;            
            
    END IF;
    
    
    IF l_Items = 1     -- есть признак группировки в item_t
    THEN   
       -- проставляем bill_id записям, предназначенным для тарификации   
        pk114_items.Set_Bill_Id(p_Data_Table    => p_Data_Table,
                                p_Rep_Period_Id => pk00_const.Get_Period_Id(p_Rep_Period),
                                p_Task_Id       => p_Task_Id
                                );                     
    END IF;                            

    IF TRUNC(p_Rep_Period,'mm') = TRUNC(p_Day,'mm') THEN
       -- для удобства дальнейших расчетов. Если параметр NULL, то партиция в BDR-ах
       -- будет определяться временем соединения
        l_Rep_Period := NULL;
    ELSE
        l_Rep_Period := p_Rep_Period;    
    END IF;    

    l_Rep_Period_Id := TO_NUMBER(TO_CHAR(p_Rep_Period,'YYYYMM'));

   -- Проверка на задвой BDR-ов в таблице для мн,мг и зонов. соед.
    IF p_Test_BDR_Table IS NULL THEN -- В тестовую таблицу кладем все подряд
        
        -- Ищем макс. и миним. даты звонков
        -- (к началу и концу расчетного дня прибаляем макс. диапазоны, на которые могло измениться время)  
        IF p_Data_Type != 'SPBCLN' THEN  
            l_LDate_From := TRUNC(p_Day) - (PIN.Get_Lim_Utc_Offset('MAX') - PIN.Get_Lim_Utc_Offset('MIN'));
        
            l_LDate_To := TRUNC(p_Day) + INTERVAL '00 23:59:59' DAY TO SECOND + 
                                      (PIN.Get_Lim_Utc_Offset('MAX') - PIN.Get_Lim_Utc_Offset('MIN'));
        ELSE
            l_LDate_From := TRUNC(p_Day);
            l_LDate_To := TRUNC(p_Day) + INTERVAL '00 23:59:59' DAY TO SECOND;
        END IF;                              
    
       -- получаем даты периода, где могут быть задвои
        IF TRUNC(p_Rep_Period,'mm') = TRUNC(p_Day,'mm') THEN
            -- кладем в месяц соединения.   
            
            l_RDate_From := l_LDate_From; 
            l_RDate_To   := l_LDate_To;
        
        ELSE
           -- если данные переносятся в другой отчетный период, то берем даты периода, куда переносим
            l_RDate_From := p_Rep_Period; 
            l_RDate_To   := p_Rep_Period + INTERVAL '00 23:59:59' DAY TO SECOND;
            
        END IF;    
    
        IF p_Task_Id > 0 THEN
                
            BEGIN
                SELECT 1
                  INTO l_Delete
                  FROM PIN.Q00_RETRF_JOB j
                 WHERE j.not_bill = 1 -- тарифцировать только соединения, которых нет ни в одном счете
                   AND j.task_id = p_Task_Id; 
                    
                -- Если соединение, попавшее в расчет, уже в каком-либо отчетном периоде попадало
                -- в счет и стоит признак расчета только не попавших в счет соединений
                -- (pin.q00_retrf_job.not_bill = 1), то удалем такие вызовы из расчета 
                EXECUTE IMMEDIATE 
                    'DELETE FROM ' || p_Data_Table || ' t ' || CHR(10) ||
                    ' WHERE EXISTS (SELECT /*+ parallel(b 5) */ ' || CHR(10) ||
                    '                      1 ' || CHR(10) || -- в "родном" для звонка отчетном периоде
                    '                 FROM ' || l_BDR_Table || ' b ' || CHR(10) ||
                    '                WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
                    '                  AND b.bdr_type_id = :l_BDR_Type_Id ' || CHR(10) ||
                    '                  AND b.bdr_status = :l_OK ' || CHR(10) ||
                    '                  AND b.item_id IS NOT NULL ' || CHR(10) ||
                    '                  AND b.cdr_id = t.cdr_id ' || CHR(10) ||
                    '              ) ' || CHR(10) ||
                    '    OR EXISTS (SELECT /*+ parallel(b 5) */ ' || CHR(10) || 
                    '                      1 ' || CHR(10) || -- возможные переносы в последующие расчетные периоды
                    '                 FROM ' || l_BDR_Table || ' b, ' || CHR(10) ||
                    '                      ( ' || CHR(10) ||-- перечисление месяцев, в которые могли попасть звонки
                    '                       SELECT ADD_MONTHS(TRUNC(:l_Date_From,''mm''),LEVEL) curr_month ' || CHR(10) ||
                    '                         FROM dual ' || CHR(10) ||
                    '                       CONNECT BY LEVEL <= MONTHS_BETWEEN(TRUNC(SYSDATE,''mm''), TRUNC(:l_Date_From,''mm'')) ' || CHR(10) ||
                    '                      ) p  ' || CHR(10) ||
                    '                WHERE b.rep_period = p.curr_month ' || CHR(10) || 
                    '                  AND b.bdr_type_id = :l_BDR_Type_Id ' || CHR(10) ||
                    '                  AND b.bdr_status = :l_OK ' || CHR(10) ||
                    '                  AND b.item_id IS NOT NULL ' || CHR(10) ||
                    '                  AND b.cdr_id = t.cdr_id ' || CHR(10) ||
                    '              )'
                USING l_LDate_From, l_LDate_To, 
                      l_BDR_Type_Id,
                      t_BDR_Status('OK'),
                      p_Day, p_Day,
                      l_BDR_Type_Id,
                      t_BDR_Status('OK');                                         
  
                l_Delete := SQL%ROWCOUNT;
                        
                IF l_Delete > 0 THEN
                    Pk01_Syslog.Write_Msg(p_Msg => 'Day: ' || TO_CHAR(p_Day,'dd.mm.yyyy') ||
                                                   ', Соед. уже в счетах. Удалено: ' || TO_CHAR(l_Delete) ||
                                                   ', Task_Id: ' || TO_CHAR(p_Task_Id), 
                                          p_Src => gc_PkgName || '.' || v_prcName);               
                END IF;                                
                    
            EXCEPTION
                WHEN no_data_found THEN
                   -- ничего делать не надо, дальше идем 
                    NULL;
            END;               
                
        END IF;    
    
    
        -- удаляем все соединения, относящиеся к закрытым счетам
        IF l_Items = 1     -- есть признак группировки в item_t
        THEN   
        
            EXECUTE IMMEDIATE
                'DELETE FROM ' || p_Data_Table ||
                ' WHERE bill_id < 0 ' ||
                '   AND bill_id NOT IN (' || TO_CHAR(t_BDR_Status('BILL_NF')) || ')';

            l_Delete := SQL%ROWCOUNT;
                
            IF l_Delete > 0 THEN
                Pk01_Syslog.Write_Msg(p_Msg   => 'Day: ' || TO_CHAR(p_Day,'dd.mm.yyyy') ||
                                                 ', Попытка загрузки в закрытые или неправильно заданные счета: ' || TO_CHAR(l_Delete) ||
                                                 ', Task_Id: ' || TO_CHAR(p_Task_Id) ||
                                                 ', Src: ' || p_Data_Type,
                                      p_Src   => gc_PkgName || '.' || v_prcName,
                                      p_Level => Pk01_Syslog.l_warn);         
            END IF;          
    
            -- устанавливаем статус ошибки item-ам, bdr-ы которых удаляем или
            -- будем вносить новые
            EXECUTE IMMEDIATE                         
                'UPDATE item_t i ' || CHR(10) ||
                '   SET item_status = :l_Error ' || CHR(10) ||
                ' WHERE i.rep_period_id = :l_Rep_Period_Id ' || CHR(10) ||
                '   AND item_status = :l_Open ' || CHR(10) ||
                '   AND item_id IN (SELECT /*+ parallel(b 5) no_index(b bdr_voice_bill_id_ie_i) */ ' ||
                '                          b.item_id ' || CHR(10) ||
                '                     FROM ' || l_BDR_Table || ' b, ' || 
                                           p_Data_Table || ' t ' || CHR(10) ||
                '                    WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
                '                      AND b.bdr_type_id = :l_BDR_Type ' || CHR(10) ||
                '                      AND b.bill_id = t.bill_id ' || CHR(10) ||
                '                      AND b.cdr_id = t.cdr_id)'
            USING pk00_const.c_ITEM_STATE_ERROR,
                  pk00_const.Get_Period_Id(p_Rep_Period),                
                  pk00_const.c_ITEM_STATE_OPEN,
                  l_RDate_From, l_RDate_To,
                  l_BDR_Type_Id;
                     
            l_Update := SQL%ROWCOUNT;      
               
        END IF;    
            
        -- удаляем bdr-ы, которые могут быть задвоены      
        EXECUTE IMMEDIATE                         
            'DELETE /*+ parallel(b 5) no_index(b bdr_voice_bill_id_ie_i) */ ' ||
            '  FROM ' || l_BDR_Table || ' b ' || CHR(10) ||
            ' WHERE b.rep_period BETWEEN :l_RDate_From AND :l_RDate_To ' || CHR(10) ||
            '   AND b.bdr_type_id = :l_BDR_Type ' || CHR(10) ||
            '   AND (' ||
            '        NOT EXISTS (SELECT 1 ' || CHR(10) ||
            '                      FROM pin.bill_t bl ' || CHR(10) ||
            '                     WHERE bl.rep_period_id = :l_rep_period_id ' || CHR(10) ||
            '                       AND bl.bill_status NOT IN (:l_Open ) ' || CHR(10) ||
            '                       AND bl.bill_id = b.bill_id ' || CHR(10) ||
            '                   ) ' || CHR(10) ||
            '         OR ' ||
            '        :p_Items = 0 ' ||
            '       )' || CHR(10) || -- агентские договоры в счета не грузятся
            '   AND EXISTS (SELECT 1 ' ||
            '                     FROM ' || p_Data_Table || ' t ' || CHR(10) || 
            '                    WHERE b.cdr_id = t.cdr_id ' || CHR(10) ||
          -- если оставить нижеследующее  условие, то BDR-ы, у CDR-ов которых нет привязки к заказу (удален или не внесен из-за какой-то ошибки)
          --  не будут удаляться и будут задвоены (закомментарено 17.06.2015)
            '                      AND (t.old_local_time BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
            '                            OR t.old_local_time IS NULL) ' ||            
            '                  )'
        USING l_RDate_From, l_RDate_To,
              l_BDR_Type_Id, 
              l_Rep_Period_Id,
              pk00_const.c_BILL_STATE_OPEN,
              l_Items,
              TRUNC(p_Day), TRUNC(p_Day)+INTERVAL '00 23:59:59' DAY TO SECOND;

        l_Delete := SQL%ROWCOUNT;

        Pk01_Syslog.Write_Msg(p_Msg => 'Day: ' || TO_CHAR(p_Day,'dd.mm.yyyy') ||
                                       ', Src: ' || p_Data_Type ||
                                       ', Rep_Period: ' || NVL(TO_CHAR(p_Rep_Period,'dd.mm.yyyy'),'NULL') ||
                                       ', Set items err: ' || TO_CHAR(l_Update) ||
                                       ', dlt BDRs: ' || TO_CHAR(l_Delete), 
                              p_Src => gc_PkgName || '.' || v_prcName);      
                                   
    END IF;    

    --- тарификация и загрузка привязанных МН, МГ и зоновых соединений 
    l_SQL :=   
       ' INSERT ' || --ALL ' ||
       '      INTO ' || l_BDR_Table || CHR(13) ||
                 ' (REP_PERIOD, SAVE_DATE, MODIFY_DATE, BDR_TYPE_ID, BDR_STATUS, ' || CHR(10) ||
                 '  CDR_ID, START_TIME, LOCAL_TIME, UTC_OFFSET, DURATION, ABN_A, ABN_B, ABN_F, ' || CHR(10) ||  
                 '  ACCOUNT_ID, ORDER_ID, ORDER_NO, TRF_ID, TRF_CODE, BILL_MINUTES, CALC_BILL_MINUTES, ' || CHR(10) ||
                 '  MP_8800_ID, INIT_Z_ID, PREFIX_A, INIT_Z_NAME, TERM_Z_ID, PREFIX_B, TERM_Z_NAME, TD_ID, ZMDL_ID, ' || CHR(10) ||
                 '  PRICE, AMOUNT, CDR_SERVICE_ID, ' || CHR(10) ||
                 '  SERVICE_ID, SUBSERVICE_ID, CALL_TYPE, BILL_ID, RATESYSTEM_ID, ORDER_BODY_ID, ' || CHR(10) ||
                 '  Q805_CODE, Q805_NAME, TERM_REASON_CODE, TERM_REASON_NAME) ' || CHR(10) ||
       '     SELECT NVL(:l_Rep_Period, x.bill_date) rep_period, ' ||
           '        :l_Save_Date calc_date, :l_Save_Date modify_date, ' || CHR(13) || 
           '        :l_BDR_Type bdr_type_id, x.BDR_Status, x.cdr_id, x.start_time, ' || CHR(10) ||
           '        x.bill_date, x.utc_offset, x.duration, x.abn_a, x.abn_b, x.order_ph, ' || CHR(13) || 
           '        x.account_id, x.Order_Id, x.Order_No, x.Trf_Id, x.Trf_Code, x.bill_minutes, x.bill_minutes, ' || CHR(13) ||
           '        x.mp_8800_id, x.init_z_id, x.prefix_a, x.init_z_name, x.term_z_id, x.prefix_b, x.term_z_name, x.td_id, x.zmdl_id, ' || CHR(10) ||
           '        x.Price, x.Amount, x.cdr_service_id, ' || 
           '        x.service_id, x.subservice_id, x.call_type, x.bill_id, x.ratesystem_id, x.order_body_id, ' || CHR(13) ||
           '        x.q805_code, x.q805_name, x.term_reason_code, x.term_reason_name ' || CHR(10) ||
       '       FROM TABLE(CAST(PK111_TARIFFING.Trf_Cl_A_Table( ' || CHR(13) ||
              '    CURSOR(SELECT /*+ PARALLEL(t 10) */ ' || CHR(13) ||
              '                  t.cdr_id, t.sw_name, t.data_type, ' ||
              '                  t.local_time, t.utc_offset,' || CHR(13) ||
              '                  t.duration, t.subs_a, t.subs_b, ' ||
              '                  t.i_called_number, t.i_called_address_nature, ' || CHR(13) || 
              '                  t.terminating_reason, t.termination_code, ' || CHR(13) ||
              '                  t.i_service_id, t.new_order_id, t.order_ph, t.row_id, t.bill_id ' || CHR(13) ||
              '             FROM ' || p_Data_Table || ' t ' || CHR(13) ||
              '            WHERE t.new_order_id >= 0 ' || CHR(13) ||  
              '              AND t.local_time BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
              '         ), :l_Rep_Period_Id, :p_Task_Id, :p_Agent) AS BDR_PH_COLL) ' || CHR(13) || 
              '       ) x ';
                      
/*        INSERT INTO MDV.MS_SQL(SQL_TEXT) VALUES(l_SQL);
        COMMIT;
                 
    Pk01_Syslog.Write_Msg(p_Msg => 'DEBUG: TMP_Table: ' || p_Data_Table || ', BDR_Table: ' || TO_CHAR(l_BDR_Table) ||
                                  '; Rep_Period: ' || NVL(TO_CHAR(l_Rep_Period),'NULL') ||
                                  ', Agent: ' || TO_CHAR(l_Agent), 
                          p_Src => gc_PkgName || '.' || v_prcName);
       raise no_data_found;                       */   
        
    EXECUTE IMMEDIATE l_SQL                 
      USING l_Rep_Period,  
            l_Calc_Date, l_Calc_Date,  
            l_BDR_Type_Id,
            TRUNC(p_Day), TRUNC(p_Day)+INTERVAL '00 23:59:59' DAY TO SECOND, 
            l_Rep_Period_Id, p_Task_Id,
            l_Agent;    
            
    l_InsBdrCnt := SQL%ROWCOUNT;         

    Pk01_Syslog.Write_Msg(p_Msg => 'ОK Dаy: ' || TO_CHAR(p_Day,'dd.mm.yyyy') ||
                                   ', Src: ' || p_Data_Type ||
                                   '; Ins. '|| TO_CHAR(l_InsBDRCnt) || ' (' || l_BDR_Table || ') ' || 
                                   '; Task_Id: ' || TO_CHAR(p_Task_Id), 
                          p_Src => gc_PkgName || '.' || v_prcName);    

    -- -- +++++++++++++++++++++++++++++++++++++++++++

    COMMIT;
    
    mdv.pk21_lock.UnLock_Resource(p_Lock_Name => l_Lock_Name);                             
                                             
/*    
EXCEPTION
    WHEN ERR_BILL_PARAM THEN
          Pk01_Syslog.Write_to_log(p_Msg => 'Ошибка. В закрытом периоде параметр p_Bill не может быть FALSE.', 
                                   p_Src => c_PkgName || v_prcName);
     WHEN BDR_RO THEN
          Pk01_Syslog.Write_to_log(p_Msg => 'BDR TABLE (' || l_BDR_Tbl || ') IS READ ONLY', 
                                   p_Src => c_PkgName || v_prcName);               
     WHEN OTHERS THEN
          ROLLBACK;
          IF l_Lock_Name IS NOT NULL THEN
             pk21_lock.UnLock_Resource(p_Lock_Name => l_Lock_Name);
          END IF;  
          Pk01_Syslog.Err_to_log(p_Txt => 'Tmp.TABLE: ' || p_Day, 
                                 p_Src => c_PkgName || v_prcName);   */
END Load_BDR_Cl_A;

-- Расчет предоплаченного местного тарфика
-- (процедура обнуляет значения суммы и тарифцируемых минут у соединений, вошедших в указанный объем) 
PROCEDURE Calc_Local_Free_Traffic(p_Period    date,
                                  p_Data_Type varchar2)
IS

    v_prcName CONSTANT varchar2(32) := 'Calc_Local_Free_Traffic'; 

    l_Subservice_Id number; 
    l_BDR_Type_Id   number;
    
BEGIN

    -- получаем идентификатор службы, по которому определяем местный трафик
    SELECT subservice_id
      INTO l_Subservice_Id
      FROM subservice_t
     WHERE subservice_key = 'LOCAL';     

    -- получаем id типа bdr-ов
    l_BDR_Type_Id := pin.get_bdr_type_id(p_Data_Type);

   -- Расчет и обновление данных BDR-ов
    MERGE INTO bdr_voice_t b
    USING (
          WITH bdr_t AS (SELECT b.rowid row_id,
                                b.order_id, b.local_time, b.bill_minutes, b.calc_bill_minutes, b.ob_free_value_id
                           FROM bdr_voice_t b
                          WHERE b.rep_period BETWEEN TRUNC(p_Period,'mm') AND LAST_DAY(TRUNC(p_Period,'mm'))+1-1/86400
                            AND b.bill_minutes > 0
                            AND b.bdr_type_id = l_BDR_Type_Id
                            AND b.subservice_id = l_Subservice_Id
                            AND (NVL(b.bill_id,-1) < 0
                                  OR -- модификация данных только открытых счетов 
                                 EXISTS (SELECT 1
                                           FROM bill_t bl
                                          WHERE bl.rep_period_id >= TO_NUMBER(TO_CHAR(p_Period,'YYYYMM'))
                                            AND bl.bill_status IN (pk00_const.c_BILL_STATE_OPEN)
                                            AND bl.bill_id = b.bill_id
                                        )
                                ) 
                         )                    
           SELECT NVL(np.row_id, op.row_id) row_id, -- если нового значения нет, то возвращаем строку с предыдущем расчетом, что бы вернуть данные в стандарт. состояние
                  NVL(np.pay_minutes, op.calc_bill_minutes) pay_minutes, -- если нового значения нет, то возвращаем кол-во минут, которое должно быть оплачено при стандартном расчете
                  np.order_body_id new_ob_free_value_id,
                  op.ob_free_value_id,
                  op.calc_bill_minutes
             FROM ( -- здесь полный расчет данных на текущий момент
                    SELECT (CASE WHEN t.sm - t.free_value < 0 THEN 0 -- все что до порога тарификации будет 0
                                 ELSE t.sm - t.free_value -- это остаток который надо протарифицировать, 
                                                          -- если порог превышен в момент соединения
                            END) pay_minutes,
                           t.row_id, 
                           t.order_body_id
                      FROM (    
                            SELECT b.row_id,
                                   b.bill_minutes,
                                   SUM(b.bill_minutes) OVER (PARTITION BY b.order_id, TRUNC(b.local_time,'mm') ORDER BY b.local_time) sm,
                                   ob.free_value, ob.order_body_id
                              FROM bdr_t b,
                                   order_body_t ob
                             WHERE b.order_id = ob.order_id
                               AND ob.charge_type = pk00_const.c_CHARGE_TYPE_REC  
                               AND ob.rate_rule_id = pk00_const.c_RATE_RULE_ABP_FREE_MIN
                               AND NVL(ob.free_value,-1) > 0
                            ) t
                     WHERE t.sm < t.free_value -- набегающая сумма меньше порога тарификации
                        OR (t.bill_minutes > (t.sm - t.free_value)) -- порог попал на момент соединения и надо будет протариф. остаток
                  ) np 
                  FULL OUTER JOIN 
                  (-- все что вошло в предоплаченный трафик в прошлые расчеты 
                    SELECT b.row_id,
                           b.calc_bill_minutes,
                           b.ob_free_value_id,
                           b.bill_minutes
                      FROM bdr_t b
                     WHERE NVL(b.ob_free_value_id,-1) > 0
                  ) op
                  ON (np.row_id = op.row_id)
            WHERE -- возвращаем только записи, где нужно обновление
                  NVL(np.order_body_id, -1) != NVL(op.ob_free_value_id, -1)
               OR NVL(np.pay_minutes, -1) != NVL(op.calc_bill_minutes, -1)   
          ) d
      ON (b.rowid = d.row_id)
    WHEN MATCHED THEN UPDATE  
     SET b.ob_free_value_id = d.new_ob_free_value_id,
         b.amount = NVL(b.price,0) * d.pay_minutes,
         b.calc_bill_minutes = NVL2(d.new_ob_free_value_id, d.pay_minutes, b.bill_minutes);  -- возвращаем станд. значение тарифных минут если строка не попала 
                                                -- в новый расчет или ставим новое значение оплач. минут, если попала в предоплач. трафик                     
       
    Pk01_Syslog.Write_Msg(p_Msg => 'Расчет предоплаченного местного трафика. ' ||
                                   'Период: ' || TO_CHAR(p_Period,'dd.mm.yyyy') ||
                                   ', Тип данных: ' || p_Data_Type || ' (' || TO_CHAR(l_BDR_Type_Id) || ')' ||
                                   ', Кол-во: ' || TO_CHAR(SQL%ROWCOUNT), 
                          p_Src => gc_PkgName || '.' || v_prcName);                
    
    COMMIT;
    
END Calc_Local_Free_Traffic;


-- Функция для поиска направления по номеру телефона.
-- Возвращает 0  - направление найдено,
--            -1 - направление не найдено   
FUNCTION Get_Dir_Full(p_PH_Num      IN  varchar2,
                      p_Prefix      OUT varchar2,
                      p_Z_Id        OUT number,
                      p_Z_Name      OUT varchar2,
                      p_Z_Id_Parent OUT number,
                      p_City_FZ     OUT varchar2,
                      p_MgMn        OUT number,
                      p_Ph_Type     OUT varchar2, -- 'A' - ABC, 'D' - DEF
                      p_H_Id        OUT number
                     ) RETURN number 
IS
    l_Counter PLS_INTEGER;
BEGIN
   /*
    SELECT z_id, prefix, z_name, z_id_parent, city_fz, mg_mn, ph_type   
      INTO p_Z_Id, p_Prefix, p_Z_Name, p_Z_Id_Parent, p_City_FZ, p_MgMn, p_Ph_Type 
      FROM ( 
            SELECT z.z_id, z.mg_mn, d.prefix, z.z_name, z.z_id_parent, z.city_fz,
                   (CASE WHEN d.abc_h_id IS NOT NULL THEN 'A'
                         WHEN d.def_h_id IS NOT NULL THEN 'D'
                    END) ph_type     
              FROM tariff_ph.d01_Zone z,
                   (SELECT abc_h_id, NULL def_h_id, prefix  
                      FROM tariff_ph.d02_zone_abc a
                     WHERE a.prefix IS NOT NULL
                       AND p_Ph_Num LIKE a.prefix || '%' 
                    UNION ALL
                    SELECT NULL abc_h_id, def_h_id, prefix  
                      FROM tariff_ph.d03_zone_def d
                     WHERE d.prefix IS NOT NULL
                       AND p_Ph_Num LIKE d.prefix || '%'
                    ) d                   
             WHERE z.abc_h_id = d.abc_h_id
                OR z.def_h_id = d.def_h_id
             ORDER BY LENGTH(d.prefix) DESC
           )
     WHERE ROWNUM = 1; */
             
    p_Prefix := p_Ph_Num;
    
    LOOP
        
        BEGIN
            p_Z_Id        := gt_Zone(p_Prefix).z_id;    
            p_Z_Name      := gt_Zone(p_Prefix).z_name; 
            p_Z_Id_Parent := gt_Zone(p_Prefix).z_id_parent; 
            p_City_FZ     := gt_Zone(p_Prefix).city_fz;  
            p_MgMn        := gt_Zone(p_Prefix).mg_mn; 
            p_Ph_Type     := gt_Zone(p_Prefix).ph_type;
            p_H_Id        := gt_Zone(p_Prefix).h_id;
        EXCEPTION
            WHEN no_data_found THEN
                l_Counter := LENGTH(p_Prefix) - 1;
                p_Prefix := NULL;
            WHEN others THEN
                IF SQLCODE = -6502 THEN    
                    l_Counter := 0; -- выходим, т.к. передали номер = NULL
                ELSE
                    RAISE;    
                END IF;                     
        END;        
    
        EXIT WHEN l_Counter = 0 OR p_Prefix IS NOT NULL;
        
        p_Prefix := SUBSTR(p_Ph_Num, 1, l_Counter); 
        
    END LOOP;
     
    IF p_Prefix IS NOT NULL THEN 
        RETURN pk00_const.c_Ret_OK;
    ELSE                 
        RETURN pk00_const.c_Ret_Er;
    END IF;         
        
EXCEPTION
    WHEN no_data_found THEN
        RETURN pk00_const.c_Ret_Er;

   /*WHEN others THEN
            Pk01_Syslog.Write_Msg(p_Msg => 'Phone: ' || p_Ph_Num,
                                  p_Src => 'Debug Get_Dir_Full');
            RAISE;        */                            

END Get_Dir_Full;


-- Функция для поиска направления по номеру телефона.
-- Возвращает Z_ID - id направления, если найдено,
--            NULL - направление не найдено   
FUNCTION Get_Dir(p_PH_Num      IN  varchar2,
                 p_Prefix      OUT varchar2,
                 p_Z_Name      OUT varchar2
                ) RETURN number
IS
    l_Result      number;
    l_Z_Id        number;
    l_Z_Id_Parent number;
    l_City_FZ     varchar2(1);
    l_MgMn        number;
    l_Ph_Type     varchar2(1);
    l_H_Id        number;
BEGIN

    l_Result := Get_Dir_Full(p_PH_Num      => p_PH_Num, -- in
                             p_Prefix      => p_Prefix, -- out
                             p_Z_Id        => l_Z_Id,
                             p_Z_Name      => p_Z_Name,
                             p_Z_Id_Parent => l_Z_Id_Parent,
                             p_City_FZ     => l_City_FZ,
                             p_MgMn        => l_MgMn,
                             p_Ph_Type     => l_Ph_Type, -- 'A' - ABC, 'D' - DEF
                             p_H_Id        => l_H_Id
                            );
                            
    IF l_Result >= 0 THEN
       -- направление найдено
        l_Result := l_Z_Id; -- возвращаем идентификатор
    ELSE
        l_Result := NULL;
    END IF;       
           
    RETURN l_Result;
                             
END Get_Dir;
                  

-- Возвращет тип соединения (МГ(1), МН(2)) по таблице subservice_t  
FUNCTION Get_MMTS_SubSrv_Type(p_Abn_A varchar2,
                              p_Abn_B varchar2
                             ) RETURN number PARALLEL_ENABLE
IS
    l_MgMn_A number;
    l_MgMn_B number;
    l_Result number;
    
    FUNCTION Get_Type(l_Ph_Num varchar2 
                     ) RETURN number
    IS
        l_MgMn number;
        l_Res  number;
        l_Prefix  varchar2(34);
        l_Z_Id        number;
        l_Z_Name      varchar2(128);
        l_Z_Id_Parent number;
        l_City_FZ     varchar2(1);
        l_Ph_Type     varchar2(1); 
        l_H_Id        number;
    BEGIN
         
       /* SELECT DECODE(mg_mn,1,t_Subservice('MG'), --pin.pk00_const.c_SUBSRV_MG,
                            2,t_Subservice('MN')) --pin.pk00_const.c_SUBSRV_MN)
          INTO l_MgMn
          FROM (          
                SELECT z.mg_mn
                  FROM PIN.RS20_ZONES z
                 WHERE z.prefix IS NOT NULL
                   AND (z.prefix = SUBSTR(l_Ph_Num,1,1) OR
                        z.prefix = SUBSTR(l_Ph_Num,1,2) OR
                        z.prefix = SUBSTR(l_Ph_Num,1,3) OR
                        z.prefix = SUBSTR(l_Ph_Num,1,4) OR
                        z.prefix = SUBSTR(l_Ph_Num,1,5) OR
                        z.prefix = SUBSTR(l_Ph_Num,1,6) OR
                        z.prefix = SUBSTR(l_Ph_Num,1,7) OR
                        z.prefix = SUBSTR(l_Ph_Num,1,8) OR
                        z.prefix = SUBSTR(l_Ph_Num,1,9) OR
                        z.prefix = SUBSTR(l_Ph_Num,1,10) OR
                        z.prefix = SUBSTR(l_Ph_Num,1,11)
                       ) 
                 ORDER BY LENGTH(z.prefix) DESC         
               )     
         WHERE ROWNUM = 1; */         
         
         l_Res := Get_Dir_Full(p_PH_Num      => l_Ph_Num, -- in
                      p_Prefix      => l_Prefix,
                      p_Z_Id        => l_Z_Id,
                      p_Z_Name      => l_Z_Name,
                      p_Z_Id_Parent => l_Z_Id_Parent,
                      p_City_FZ     => l_City_FZ,
                      p_MgMn        => l_MgMn,
                      p_Ph_Type     => l_Ph_Type, -- 'A' - ABC, 'D' - DEF
                      p_H_Id        => l_H_Id
                     );
                              
        IF l_Res = pk00_const.c_Ret_OK THEN 

            l_MgMn := (CASE l_MgMn WHEN 1 THEN t_Subservice('MG') --pin.pk00_const.c_SUBSRV_MG,
                                  WHEN 2 THEN t_Subservice('MN')
                       END);        
        
            RETURN l_MgMn;
        ELSE
            RETURN t_Subservice('MN'); 
        END IF;
         
    EXCEPTION
        WHEN no_data_found THEN         
            RETURN t_Subservice('MN'); --pin.pk00_const.c_SUBSRV_MN; -- MN
    END;                 
    
BEGIN


   -- получаем тип звонка по номеру А
    l_MgMn_A := Get_Type(p_Abn_A); 
    
   -- получаем тип звонка по номеру А
    l_MgMn_B := Get_Type(p_Abn_B);
    
    IF l_MgMn_A = l_MgMn_B THEN       
        l_Result := l_MgMn_B;
    ELSE
        l_Result := pin.pk00_const.c_SUBSRV_MN; -- MN
    END IF;
    
    RETURN l_Result;
         
END Get_MMTS_SubSrv_Type; 


-- Ищет префикс заданного телевонного номера среди Питерских DEF-ов и если находит,
-- то возвращает префикс макс. длины. Если не находит - NULL
FUNCTION Get_SPb_Def(p_Ph_Num IN varchar2
                    ) RETURN varchar2 
IS

   -- id зоны с Питерскими DEF-кодами
    c_DEF_SPB_Code CONSTANT number := 65;

    l_Prefix varchar2(16);
    
BEGIN
    -- Проверяем номер Б Питерский DEF или нет
    SELECT prefix
      INTO l_Prefix
      FROM (SELECT z.prefix
              FROM tariff_ph.D03_ZONE_DEF z
             WHERE z.DEF_H_ID = c_DEF_SPB_Code
               AND p_Ph_Num LIKE z.prefix || '%'
             ORDER BY LENGTH(z.prefix) DESC
           )  
     WHERE ROWNUM = 1;
     
    RETURN l_Prefix; 
     
EXCEPTION
    WHEN no_data_found THEN
        RETURN NULL;       
END Get_SPb_Def;                               


FUNCTION Get_SPb_SubService_Id(p_Abn_A varchar2,
                               p_Abn_B varchar2) RETURN number
IS
    l_SubService_Id number;                               
    l_Prefix        varchar2(16);
BEGIN

    IF SUBSTR(p_Abn_B,1,4) IN ('7812','8812') THEN -- местный вызов        
        l_SubService_Id := pin.pk00_const.c_SUBSRV_LOCAL;
    ELSE
        
       -- Проверяем номер Б Питерский DEF или нет
        l_Prefix := Get_SPb_Def(p_Abn_B);
        
        IF l_Prefix IS NOT NULL THEN -- питерский DEF, значит
            -- зоновое соединение
            l_SubService_Id := pin.pk00_const.c_SUBSRV_ZONE;
        ELSE
            -- определяем МГ или МН соединение     
            l_SubService_Id := Get_MMTS_SubSrv_Type(pin.Norm_Ph_Number('SPB',p_Abn_A),
                                                    p_Abn_B);
        END IF;
        
    END IF;            
    
    RETURN l_SubService_Id;
    
END Get_SPb_SubService_Id;


-- Ищет префикс заданного телевонного номера среди московских номеров и если находит,
-- то возвращает префикс макс. длины. Если не находит - NULL
FUNCTION Get_Msc_Local(p_Ph_Num IN varchar2
                      ) RETURN varchar2 
IS

   -- id зоны с московскими кодами
    c_Loc_Msk_Code CONSTANT number := 48;

    l_Prefix varchar2(16);
    
BEGIN
    -- Проверяем номер Б московским местным или нет
    SELECT prefix
      INTO l_Prefix
      FROM (SELECT z.prefix
              FROM tariff_ph.D02_ZONE_ABC z
             WHERE z.ABC_H_ID = c_Loc_Msk_Code
               AND p_Ph_Num LIKE z.prefix || '%'
             ORDER BY LENGTH(z.prefix) DESC
           )  
     WHERE ROWNUM = 1;
     
    RETURN l_Prefix; 
     
EXCEPTION
    WHEN no_data_found THEN
        RETURN NULL;       
END Get_Msc_Local;                               

FUNCTION Get_Msc_SubService_Id(p_Abn_A varchar2,
                               p_Abn_B varchar2) RETURN number
IS

    c_Loc_Msk_Code CONSTANT number := 48; -- abc_h_id Москвы в справочнике направлений
    c_DEF_Msk_Code CONSTANT number := 68; -- def_h_id Москвы и Моск. области в справочнике направлений

    l_SubService_Id number;                               
    l_Prefix        varchar2(16);
BEGIN

    -- проверка является ли номер Б местным московским
    l_Prefix := Get_Msc_Local(p_Ph_Num => p_Abn_B);
       
    IF l_Prefix IS NULL THEN
        RAISE no_data_found;
    END IF;
    
    RETURN pin.pk00_const.c_SUBSRV_LOCAL; -- номер местный московский   

EXCEPTION
    WHEN no_data_found THEN
        BEGIN
           -- трафик не местный, проверяем Б DEF московский или нет 
            SELECT pin.pk00_const.c_SUBSRV_ZONE
              INTO l_SubService_Id
              FROM tariff_ph.D03_ZONE_DEF z
             WHERE z.DEF_H_ID = c_DEF_Msk_Code
               AND p_Abn_B LIKE z.prefix || '%'
               AND ROWNUM = 1;    

             RETURN l_SubService_Id;
    
        EXCEPTION
            WHEN no_data_found THEN
                -- определяем МГ или МН соединение по стандартной схеме    
                l_SubService_Id := Get_MMTS_SubSrv_Type(pin.Norm_Ph_Number('SPB',p_Abn_A),
                                                        p_Abn_B);
                
                RETURN l_SubService_Id;
                                                        
        END;
    
END Get_Msc_SubService_Id;


FUNCTION Get_SubSrv_Id(p_PhNum_A  IN varchar2,
                       p_PhNum_B  IN varchar2
                      ) RETURN number
IS
    l_Result   number;
    
    l_Z_Id_A   number;
    l_Z_Name_A varchar2(255);                                
    l_Prefix_A varchar2(34);
    l_Z_Id_B   number;
    l_Z_Name_B varchar2(255);                                
    l_Prefix_B varchar2(34);   
    l_Type_B   varchar2(1);
    
BEGIN

    l_Result := Get_SubSrv_Id_With_Dir(p_PhNum_A  => p_PhNum_A,
                                       p_PhNum_B  => p_PhNum_B,
                                       p_Z_Id_A   => l_Z_Id_A,
                                       p_Z_Name_A => l_Z_Name_A,                                
                                       p_Prefix_A => l_Prefix_A,
                                       p_Z_Id_B   => l_Z_Id_B,
                                       p_Z_Name_B => l_Z_Name_B,                                
                                       p_Prefix_B => l_Prefix_B,
                                       p_Type_B   => l_Type_B                                
                                      );
                                       
    RETURN l_Result;                                       

END Get_SubSrv_Id;


FUNCTION Get_SubSrv_Id_With_Dir(p_PhNum_A  IN varchar2,
                                p_PhNum_B  IN varchar2,
                                p_Z_Id_A   OUT number,
                                p_Z_Name_A OUT varchar2,                                
                                p_Prefix_A OUT varchar2,
                                p_Z_Id_B   OUT number,
                                p_Z_Name_B OUT varchar2,                                
                                p_Prefix_B OUT varchar2,
                                p_Type_B   OUT varchar2                                
                               ) RETURN number
IS

    l_Z_Id_Parent_A number;
    l_City_FZ_A     varchar2(1);
    l_Type_A        varchar2(1);
    l_MgMn_A        number;
        
    l_Z_Id_Parent_B number; 
    l_City_FZ_B     varchar2(1); 
    l_MgMn_B        number;
    l_H_Id          number;

    l_Result        number;
    
BEGIN

   -- получаем направление по номеру А
    l_Result := Get_Dir_Full(p_PH_Num      => p_PhNum_A,
                             p_Prefix      => p_Prefix_A, -- out
                             p_Z_Id        => p_Z_Id_A,
                             p_Z_Name      => p_Z_Name_A,
                             p_Z_Id_Parent => l_Z_Id_Parent_A,
                             p_City_FZ     => l_City_FZ_A,
                             p_MgMn        => l_MgMn_A,
                             p_Ph_Type     => l_Type_A,
                             p_H_Id        => l_H_Id
                            ); 
                            
    IF l_Result = pk00_const.c_Ret_Er THEN                        
       
        RETURN t_BDR_Status('SUBSRV_A_NF'); -- возвращаем ошибку поиска зоны инициирования  
        
    END IF;                             
                            
   -- получаем направление по номеру B
    l_Result := Get_Dir_Full(p_PH_Num      => p_PhNum_B,
                             p_Prefix      => p_Prefix_B, -- out
                             p_Z_Id        => p_Z_Id_B,
                             p_Z_Name      => p_Z_Name_B,
                             p_Z_Id_Parent => l_Z_Id_Parent_B,
                             p_City_FZ     => l_City_FZ_B,
                             p_MgMn        => l_MgMn_B,
                             p_Ph_Type     => p_Type_B,    -- A (звонок на ABC) или D (звонок на DEF)
                             p_H_Id        => l_H_Id                         
                            ); 
                            
    IF l_Result = pk00_const.c_Ret_Er THEN                        
       
        RETURN t_BDR_Status('SUBSRV_B_NF'); -- возвращаем ошибку поиска зоны инициирования  
        
    END IF; 

   -- Определяем по полученным направлениям услугу
    IF p_Z_Id_A = p_Z_Id_B AND p_Type_B = 'A' AND l_Type_A = 'A'
    THEN
       -- звонок местный
        l_Result := t_Subservice('LOCAL');
               
    ELSIF (
           (l_City_FZ_A = 'Y' AND p_Type_B = 'D')
             OR 
           (l_Type_A = 'D' AND l_City_FZ_B = 'Y')
          ) 
           AND 
          NVL(l_Z_Id_Parent_A, p_Z_Id_A) = NVL(l_Z_Id_Parent_B, p_Z_Id_B)
    THEN 
        -- звонок зоновый (из города федерального значения на DEF-ы, принадлежащие области или городу области
        --                 и наоборот)
        l_Result := t_Subservice('ZONE');

    ELSIF NVL(l_City_FZ_A,'N') != 'Y' AND NVL(l_City_FZ_B,'N') != 'Y' AND 
          NVL(l_Z_Id_Parent_A, p_Z_Id_A) = NVL(l_Z_Id_Parent_B, p_Z_Id_B)
    THEN      
        -- звонок зоновый (не из/в город(а) федерального значения, но внутри области)
        l_Result := t_Subservice('ZONE');

    ELSIF l_MgMn_A = tariff_ph.pk_const.c_ZONE_TERM_TYPE_MG AND
          l_MgMn_B = tariff_ph.pk_const.c_ZONE_TERM_TYPE_MG 
    THEN      
        -- звонок МГ
        l_Result := t_Subservice('MG');

    ELSIF l_MgMn_A != tariff_ph.pk_const.c_ZONE_TERM_TYPE_MG OR
          l_MgMn_B != tariff_ph.pk_const.c_ZONE_TERM_TYPE_MG 
    THEN      
        -- звонок МН
        l_Result := t_Subservice('MN');

    END IF;
    
    RETURN l_Result;
    
END Get_SubSrv_Id_With_Dir;      

/*
-- Функция для поиска направления (не DEF) по номеру телефона.
-- Возвращает 0  - направление найдено,
--            -1 - направленпие не найдено   
FUNCTION Get_ABC_Pref(p_PH_Num      IN  varchar2,
                      p_Prefix      OUT varchar2,
                      p_Z_Id        OUT number,
                      p_Z_Name      OUT varchar2,
                      p_Z_Id_Parent OUT number,
                      p_City_FZ     OUT varchar2
                     ) RETURN number 
IS
BEGIN

    SELECT z_id, prefix, z_name, z_id_parent, city_fz
      INTO p_Z_Id, p_Prefix, p_Z_Name, p_Z_Id_Parent, p_City_FZ
      FROM ( 
            SELECT z.z_id, p.prefix, z.z_name, z.z_id_parent, z.city_fz
              FROM tariff_ph.d01_Zone z,
                   tariff_ph.d02_zone_abc p
             WHERE z.abc_h_id = p.abc_h_id
               AND p.prefix IS NOT NULL
               AND p_PH_Num LIKE p.prefix || '%'
             ORDER BY LENGTH(p.prefix) DESC
           )
     WHERE ROWNUM = 1; 
             
    RETURN pk00_const.c_Ret_OK;             
             
EXCEPTION
    WHEN no_data_found THEN
        RETURN pk00_const.c_Ret_Er;

END Get_ABC_Pref;


-- Функция для поиска направления (не DEF) по номеру телефона.
-- Прим: если указан параметр p_ABC_H_Id, то ищет номера DEF в указанном регионе только
-- Возвращает 0  - направление найдено,
--            -1 - направление не найдено   
FUNCTION Get_DEF_Pref(p_PH_Num      IN  varchar2,
                      p_ABC_H_Id    IN  varchar2 DEFAULT NULL,
                      p_Prefix      OUT varchar2,
                      p_Z_Id        OUT number,
                      p_Z_Name      OUT varchar2,
                      p_Z_Id_Parent OUT number,
                      p_City_FZ     OUT varchar2                      
                     ) RETURN number 
IS
BEGIN

    SELECT z_id, prefix, z_name, z_id_parent, city_fz
      INTO p_Z_Id, p_Prefix, p_Z_Name, p_Z_Id_Parent, p_City_FZ
      FROM ( 
            SELECT z.z_id, p.prefix, z.z_name, z.z_id_parent, z.city_fz
              FROM tariff_ph.d01_Zone z,
                   tariff_ph.d03_zone_def p
             WHERE z.def_h_id = p.def_h_id
               AND p.prefix IS NOT NULL
               AND p_PH_Num LIKE p.prefix || '%'
               AND z.abc_h_id = NVL(p_ABC_H_Id, z.abc_h_id)
             ORDER BY LENGTH(p.prefix) DESC
           )
     WHERE ROWNUM = 1; 
             
    RETURN pk00_const.c_Ret_OK;             
             
EXCEPTION
    WHEN no_data_found THEN
        RETURN pk00_const.c_Ret_Er;

END Get_DEF_Pref;*/


--- Поиск направления звонка 
-- 0 - направление нашли
-- <0 - направление не нашли. Код ошибки "не найдена зона завершения вызова"

FUNCTION Get_Zone_Old(p_Trf_Id      IN  number,
                  p_Zmdl_Id     IN  number,
                  p_Mdl_Type    IN  number,
                  p_PhNum_A     IN  varchar2,
                  p_PhNum_B     IN  varchar2,
                  p_With_Price  IN  varchar2, -- 'Y' - направления только с расценками , 'N' - первые по детализации 
                  p_TD_Id       OUT number,
                  p_Z_Id_I      OUT number,
                  p_Prefix_A    OUT varchar2,
                  p_Init_Z_Name OUT varchar2,                  
                  p_Z_Id_T      OUT number,
                  p_Prefix_B    OUT varchar2,                  
                  p_Term_Z_Name OUT varchar2
                 ) RETURN number
IS

    l_Dist_Regn number;

BEGIN

    -- 1. Поиск по ABC
    SELECT td_id, z_id, prefix, z_name
      INTO p_TD_Id, p_Z_Id_T, p_Prefix_B, p_Term_Z_Name
      FROM (
            SELECT d.td_id, z.z_id, p.prefix, z.z_name
              FROM tariff_ph.d22_zone_model_trf_zone tz,
                   tariff_ph.d11_trf_direction_b d,
                   tariff_ph.d01_zone z,
                   tariff_ph.d02_zone_abc p
             WHERE d.prefix_type IN (tariff_ph.pk_const.c_PREFIX_ABC, tariff_ph.pk_const.c_PREFIX_ABC_DEF)
               AND tz.zmdl_id = p_Zmdl_Id 
               AND tz.td_id = d.td_id
               AND d.z_id = z.z_id
               AND z.abc_h_id = p.abc_h_id
               AND p_PhNum_B LIKE p.prefix || '%'
               AND (p_With_Price = 'N'
                     OR
                    EXISTS (SELECT 1
                              FROM tariff_ph.d42_trf_price pr
                             WHERE pr.trf_id = p_Trf_Id
                               AND pr.td_id = d.td_id)
                   )     
             ORDER BY LENGTH(p.prefix) DESC  
           )
     WHERE ROWNUM = 1;          
                
    RETURN 0; 
    
EXCEPTION
    WHEN no_data_found THEN
        
        BEGIN
        
           -- 2. Поиск по DEF
            SELECT td_id, z_id, prefix, z_name
              INTO p_TD_Id, p_Z_Id_T, p_Prefix_B, p_Term_Z_Name
              FROM (
                    SELECT d.td_id, z.z_id, p.prefix, z.z_name
                      FROM tariff_ph.d22_zone_model_trf_zone tz,
                           tariff_ph.d11_trf_direction_b d,
                           tariff_ph.d01_zone z,
                           tariff_ph.d03_zone_def p
                     WHERE d.prefix_type IN (tariff_ph.pk_const.c_PREFIX_DEF, tariff_ph.pk_const.c_PREFIX_ABC_DEF)
                       AND tz.zmdl_id = p_Zmdl_Id 
                       AND tz.td_id = d.td_id
                       AND d.z_id = z.z_id
                       AND z.def_h_id = p.def_h_id
                       AND p_PhNum_B LIKE p.prefix || '%'
                       AND (p_With_Price = 'N'
                             OR
                            EXISTS (SELECT 1
                                      FROM tariff_ph.d42_trf_price pr
                                     WHERE pr.trf_id = p_Trf_Id
                                       AND pr.td_id = d.td_id)
                           )                            
                     ORDER BY LENGTH(p.prefix) DESC 
                   )
             WHERE ROWNUM = 1;                        

            RETURN 0;
            
        EXCEPTION
            WHEN no_data_found THEN
            
                -- если зона инициирования не задана, то дальнейшие расчеты бессмысленны
                IF p_Mdl_Type = tariff_ph.pk_const.c_TYPE_DIST
                THEN
                       
                  -- Модель дистанционная, пытаемся найти по расстояниям  
                    BEGIN
                        SELECT td_id, 
                               z_i_id, prefix_a, z_i_name,  
                               z_t_id, prefix_b, z_t_name 
                          INTO p_TD_Id, 
                               p_Z_Id_I, p_Prefix_A, p_Init_Z_Name,                  
                               p_Z_Id_T, p_Prefix_B, p_Term_Z_Name
                          FROM (
                                SELECT d.td_id, 
                                       zi.z_id z_i_id, zi.z_name z_i_name, pa.prefix prefix_a, 
                                       zt.z_id z_t_id, zt.z_name z_t_name, pb.prefix prefix_b
                                  FROM tariff_ph.d22_zone_model_trf_zone tz,
                                       tariff_ph.d08_dist_mg_l d,
                                       tariff_ph.d01_Zone zi,
                                       (SELECT abc_h_id, NULL def_h_id, prefix  
                                          FROM tariff_ph.d02_zone_abc a
                                         WHERE p_PhNum_A LIKE a.prefix || '%' 
                                        UNION ALL
                                        SELECT NULL abc_h_id, def_h_id, prefix  
                                          FROM tariff_ph.d03_zone_def d
                                         WHERE p_PhNum_A LIKE d.prefix || '%'
                                        ) pa,
                                        tariff_ph.d01_Zone zt,
                                       (SELECT abc_h_id, NULL def_h_id, prefix
                                          FROM tariff_ph.d02_zone_abc a
                                         WHERE p_PhNum_B LIKE a.prefix || '%' 
                                        UNION ALL
                                        SELECT NULL abc_h_id, def_h_id, prefix
                                          FROM tariff_ph.d03_zone_def d
                                         WHERE p_PhNum_B LIKE d.prefix || '%'
                                        ) pb        
                                  WHERE tz.zmdl_id = p_Zmdl_Id
                                    AND tz.td_id  = d.td_id
                                    AND d.z_id_zt = zt.z_id   
                                    AND d.z_id_zi = zi.z_id
                                    AND zi.is_zi = 1
                                    AND (
                                         (zi.abc_h_id = pa.abc_h_id)
                                           OR
                                         (zi.def_h_id = pa.def_h_id)
                                        ) 
                                    AND zt.is_zt = 1
                                    AND (
                                         (zt.abc_h_id = pb.abc_h_id)
                                           OR
                                         (zt.def_h_id = pb.def_h_id)
                                        )
                                    AND (p_With_Price = 'N'
                                          OR
                                         EXISTS (SELECT 1
                                                   FROM tariff_ph.d42_trf_price pr
                                                  WHERE pr.trf_id = p_Trf_Id
                                                    AND pr.td_id = d.td_id)
                                        )                                             
                                  ORDER BY LENGTH(pb.prefix) DESC, LENGTH(pa.prefix) DESC  
                                )                           
                          WHERE ROWNUM = 1;
                    
                        RETURN 0;
                    
                    EXCEPTION
                        WHEN no_data_found THEN
                                
                            RETURN pk00_const.с_TZONE_NOT_FOUND; -- ничего не нашли
                    END;                         
                    
                ELSE
                    
                    RETURN pk00_const.с_TZONE_NOT_FOUND; -- ничего не нашли
                    
                END IF;
                                      
        END;    
             
END Get_Zone_Old;


--- Поиск направления звонка 
-- 0 - направление нашли
-- <0 - направление не нашли. Код ошибки "не найдена зона завершения вызова"
/*
FUNCTION Get_Zone(p_Trf_Id      IN  number,
                  p_Zmdl_Id     IN  number,
                  p_Mdl_Type    IN  number,
                  p_PhNum_A     IN  varchar2,
                  p_PhNum_B     IN  varchar2,
                  p_With_Price  IN  varchar2, -- 'Y' - направления только с расценками , 'N' - первые по детализации 
                  p_TD_Id       OUT number,
                  p_Z_Id_I      OUT number,
                  p_Prefix_A    OUT varchar2,
                  p_Init_Z_Name OUT varchar2,                  
                  p_Z_Id_T      OUT number,
                  p_Prefix_B    OUT varchar2,                  
                  p_Term_Z_Name OUT varchar2
                 ) RETURN number
IS

    l_Dist_Regn number;

BEGIN

    -- 1. Поиск по ABC
    SELECT td_id, z_id, prefix, z_name
      INTO p_TD_Id, p_Z_Id_T, p_Prefix_B, p_Term_Z_Name
      FROM (
            SELECT d.td_id, z.z_id, p.prefix, z.z_name
              FROM tariff_ph.d22_zone_model_trf_zone tz,
                   tariff_ph.d11_trf_direction_b d,
                   tariff_ph.d01_zone z,
                   (SELECT abc_h_id, NULL def_h_id, prefix  
                      FROM tariff_ph.d02_zone_abc a
                     --WHERE p_PhNum_B LIKE a.prefix || '%'
                     WHERE SUBSTR(p_PhNum_B,1,1) = a.prefix 
                        OR SUBSTR(p_PhNum_B,1,2) = a.prefix
                        OR SUBSTR(p_PhNum_B,1,3) = a.prefix
                        OR SUBSTR(p_PhNum_B,1,4) = a.prefix
                        OR SUBSTR(p_PhNum_B,1,5) = a.prefix
                        OR SUBSTR(p_PhNum_B,1,6) = a.prefix
                        OR SUBSTR(p_PhNum_B,1,7) = a.prefix
                        OR SUBSTR(p_PhNum_B,1,8) = a.prefix
                        OR SUBSTR(p_PhNum_B,1,9) = a.prefix
                        OR SUBSTR(p_PhNum_B,1,10) = a.prefix
                        OR SUBSTR(p_PhNum_B,1,11) = a.prefix                      
                    UNION ALL
                    SELECT NULL abc_h_id, def_h_id, prefix  
                      FROM tariff_ph.d03_zone_def d
                     --WHERE p_PhNum_B LIKE d.prefix || '%'
                     WHERE SUBSTR(p_PhNum_B,1,1) = d.prefix 
                        OR SUBSTR(p_PhNum_B,1,2) = d.prefix
                        OR SUBSTR(p_PhNum_B,1,3) = d.prefix
                        OR SUBSTR(p_PhNum_B,1,4) = d.prefix
                        OR SUBSTR(p_PhNum_B,1,5) = d.prefix
                        OR SUBSTR(p_PhNum_B,1,6) = d.prefix
                        OR SUBSTR(p_PhNum_B,1,7) = d.prefix
                        OR SUBSTR(p_PhNum_B,1,8) = d.prefix
                        OR SUBSTR(p_PhNum_B,1,9) = d.prefix
                        OR SUBSTR(p_PhNum_B,1,10) = d.prefix
                        OR SUBSTR(p_PhNum_B,1,11) = d.prefix                     
                   ) p 
             WHERE tz.zmdl_id = p_Zmdl_Id 
               AND tz.td_id = d.td_id
               AND d.z_id = z.z_id
               AND (
                    (z.abc_h_id = p.abc_h_id AND d.prefix_type IN (tariff_ph.pk_const.c_PREFIX_ABC, tariff_ph.pk_const.c_PREFIX_ABC_DEF))
                      OR
                    (z.def_h_id = p.def_h_id AND d.prefix_type IN (tariff_ph.pk_const.c_PREFIX_DEF, tariff_ph.pk_const.c_PREFIX_ABC_DEF))                      
                    )   
               AND (p_With_Price = 'N'
                     OR
                    EXISTS (SELECT 1
                              FROM tariff_ph.d42_trf_price pr
                             WHERE pr.trf_id = p_Trf_Id
                               AND pr.td_id = d.td_id)
                   )     
             ORDER BY LENGTH(p.prefix) DESC,
                      -- у def-ов префикс принадлежит и городам и областям, но т.к. def-ы даются на 
                      -- области, то и пытаемся выводить название области
                       (CASE WHEN z.def_h_id IS NOT NULL THEN z.z_type -- 1 - область, 2 - город
                             ELSE 0
                        END)                   
           )
     WHERE ROWNUM = 1;          
                
    RETURN 0; 
    
EXCEPTION
    WHEN no_data_found THEN
        
        -- если зона инициирования не задана, то дальнейшие расчеты бессмысленны
        IF p_Mdl_Type = tariff_ph.pk_const.c_TYPE_DIST
        THEN
                       
          -- Модель дистанционная, пытаемся найти по расстояниям  
            BEGIN
                SELECT td_id, 
                       z_i_id, prefix_a, z_i_name,  
                       z_t_id, prefix_b, z_t_name 
                  INTO p_TD_Id, 
                       p_Z_Id_I, p_Prefix_A, p_Init_Z_Name,                  
                       p_Z_Id_T, p_Prefix_B, p_Term_Z_Name
                  FROM (
                        SELECT d.td_id, 
                               zi.z_id z_i_id, zi.z_name z_i_name, pa.prefix prefix_a, 
                               zt.z_id z_t_id, zt.z_name z_t_name, pb.prefix prefix_b
                          FROM tariff_ph.d22_zone_model_trf_zone tz,
                               tariff_ph.d08_dist_mg_l d,
                               tariff_ph.d01_Zone zi,
                               (SELECT abc_h_id, NULL def_h_id, prefix  
                                  FROM tariff_ph.d02_zone_abc a
                                 WHERE p_PhNum_A LIKE a.prefix || '%' 
                                UNION ALL
                                SELECT NULL abc_h_id, def_h_id, prefix  
                                  FROM tariff_ph.d03_zone_def d
                                 WHERE p_PhNum_A LIKE d.prefix || '%'
                                ) pa,
                                tariff_ph.d01_Zone zt,
                               (SELECT abc_h_id, NULL def_h_id, prefix
                                  FROM tariff_ph.d02_zone_abc a
                                 WHERE p_PhNum_B LIKE a.prefix || '%' 
                                UNION ALL
                                SELECT NULL abc_h_id, def_h_id, prefix
                                  FROM tariff_ph.d03_zone_def d
                                 WHERE p_PhNum_B LIKE d.prefix || '%'
                                ) pb        
                          WHERE tz.zmdl_id = p_Zmdl_Id
                            AND tz.td_id  = d.td_id
                            AND d.z_id_zt = zt.z_id   
                            AND d.z_id_zi = zi.z_id
                            AND zi.is_zi = 1
                            AND (
                                 (zi.abc_h_id = pa.abc_h_id)
                                   OR
                                 (zi.def_h_id = pa.def_h_id)
                                ) 
                            AND zt.is_zt = 1
                            AND (
                                 (zt.abc_h_id = pb.abc_h_id)
                                   OR
                                 (zt.def_h_id = pb.def_h_id)
                                )
                            AND (p_With_Price = 'N'
                                  OR
                                 EXISTS (SELECT 1
                                           FROM tariff_ph.d42_trf_price pr
                                          WHERE pr.trf_id = p_Trf_Id
                                            AND pr.td_id = d.td_id)
                                )                                             
                          ORDER BY LENGTH(pb.prefix) DESC, LENGTH(pa.prefix) DESC,
                                  -- у def-ов префикс принадлежит и городам и областям, но т.к. def-ы даются на 
                                  -- области, то и пытаемся выводить название области
                                   (CASE WHEN zi.def_h_id IS NOT NULL THEN zi.z_type -- 1 - область, 2 - город
                                         ELSE 0
                                    END),                   
                                   (CASE WHEN zt.def_h_id IS NOT NULL THEN zt.z_type
                                         ELSE 0
                                    END)                                                                        
                        )                           
                  WHERE ROWNUM = 1;
                    
                RETURN 0;
                    
            EXCEPTION
                WHEN no_data_found THEN
                                
                    RETURN pk00_const.с_TZONE_NOT_FOUND; -- ничего не нашли
            END;                         
                    
        ELSE
                    
            RETURN pk00_const.с_TZONE_NOT_FOUND; -- ничего не нашли
                    
        END IF;
                                      
END Get_Zone;
*/

--- Поиск направления звонка 
-- 0 - направление нашли
-- <0 - направление не нашли. Код ошибки "не найдена зона завершения вызова"
FUNCTION Get_Zone_New(p_Trf_Id      IN  number,
                      p_Zmdl_Id     IN  number,
                      p_Mdl_Type    IN  number,
                      p_PhNum_A     IN  varchar2,
                      p_PhNum_B     IN  varchar2,
                      p_With_Price  IN  varchar2, -- 'Y' - направления только с расценками , 'N' - первые по детализации 
                      p_TD_Id       OUT number,
                      p_Z_Id_I      OUT number,
                      p_Prefix_A    OUT varchar2,
                      p_Init_Z_Name OUT varchar2,                  
                      p_Z_Id_T      OUT number,
                      p_Prefix_B    OUT varchar2,                  
                      p_Term_Z_Name OUT varchar2
                     ) RETURN number
IS

    l_Dist_Regn   number;
    l_Prefix      varchar2(32);
    l_Z_Name      varchar2(256);
    l_Z_Id        number;
    l_Z_Id_Parent number;
    l_City_FZ     varchar2(8);
    l_MgMn        number;
    l_Ph_Type     varchar2(1);
    l_Result      number;
    l_Ph_Num      varchar2(34);
    l_H_Id        number;
    
    TYPE r_Zone_B IS RECORD (pref_b varchar2(34),
                             z_id   number,
                             z_name varchar2(256)
                            );
                            
    TYPE t_Zone_B IS TABLE OF r_Zone_B;
    
    lt_Zone_B t_Zone_B;                          
    l_Idx     PLS_INTEGER;
BEGIN

    IF p_Mdl_Type = tariff_ph.pk_const.c_TYPE_DIST THEN
      -- инициализируем массив для данных по зонома завершения для возможного дальнейшего поиска по номеру Б в дистанционной модели
        lt_Zone_B := t_Zone_B();
    END IF;

    l_Ph_Num := p_PhNum_B;

     -- получаем Z_ID 
    l_Result := Get_Dir_Full(p_PH_Num      => l_Ph_Num,
                             p_Prefix      => l_Prefix,
                             p_Z_Id        => l_Z_Id,
                             p_Z_Name      => l_Z_Name,
                             p_Z_Id_Parent => l_Z_Id_Parent,
                             p_City_FZ     => l_City_FZ,
                             p_MgMn        => l_MgMn,
                             p_Ph_Type     => l_Ph_Type, -- 'A' - ABC, 'D' - DEF
                             p_H_Id        => l_H_Id
                            );

    LOOP
                             
        EXIT WHEN l_Result = -1 OR l_Ph_Num IS NULL;

/*                Pk01_Syslog.Write_Msg(p_Msg   => 'Debug: ' || p_PhNum_B || ',' || l_Ph_Num || ',' || l_Ph_Type ||
                                                 ', ' || TO_CHAR(p_Trf_Id) ||
                                                 ', ' || TO_CHAR(p_Zmdl_Id) ||
                                                 ', ' || TO_CHAR(l_Z_Id),
                                      p_Src   => gc_PkgName || '.Test',
                                      p_Level => Pk01_Syslog.l_warn);       */  

        BEGIN
            -- 1. Поиск по префиксу
            SELECT td_id, z_id  --, prefix, z_name
              INTO p_TD_Id, p_Z_Id_T   --, p_Prefix_B, p_Term_Z_Name
             FROM ( 
                    SELECT d.td_id, z.z_id   --, l_Prefix prefix, z.z_name
                      FROM tariff_ph.d22_zone_model_trf_zone tz, 
                           tariff_ph.d11_trf_direction_b d, 
                           tariff_ph.d01_zone z 
                     WHERE tz.zmdl_id = p_Zmdl_Id 
                       AND tz.td_id = d.td_id 
                       AND d.z_id = z.z_id
                       AND (
                            (l_Ph_Type = 'A' AND d.prefix_type IN (tariff_ph.pk_const.c_PREFIX_ABC, tariff_ph.pk_const.c_PREFIX_ABC_DEF))
                              OR
                            (l_Ph_Type = 'D' AND d.prefix_type IN (tariff_ph.pk_const.c_PREFIX_DEF, tariff_ph.pk_const.c_PREFIX_ABC_DEF))
                           )      
                       AND ( 
                            (l_Ph_Type = 'A' AND z.abc_h_id = l_H_Id)
                              OR
                            (l_Ph_Type = 'D' AND z.def_h_id = l_H_Id)
                           ) 
                       AND (p_With_Price = 'N'
                             OR
                            EXISTS (SELECT 1
                                      FROM tariff_ph.d42_trf_price pr
                                     WHERE pr.trf_id = p_Trf_Id
                                       AND pr.td_id = d.td_id)
                           ) 
                     ORDER BY z.z_type DESC
                    ) 
              WHERE ROWNUM = 1;

            p_Prefix_B    := l_Prefix; 
            p_Term_Z_Name := l_Z_Name;

            l_Result := -1; -- данные найдены, выходим

        EXCEPTION
            WHEN no_data_found THEN
            
                IF p_Mdl_Type = tariff_ph.pk_const.c_TYPE_DIST THEN
                 -- кэшируем данные для возможного дальнейшего поиска по номеру Б в дистанционной модели
                    lt_Zone_B.EXTEND;
                    l_Idx := lt_Zone_B.LAST;
                    lt_Zone_B(l_Idx).pref_b := l_Prefix;
                    lt_Zone_B(l_Idx).z_id   := l_Z_Id;
                    lt_Zone_B(l_Idx).z_name := l_Z_Name;
                
                END IF;
            
                l_Ph_Num := SUBSTR(p_PhNum_B,1, LENGTH(l_Prefix)-1);
                
               -- получаем Z_ID 
                IF l_Ph_Num IS NOT NULL THEN
                  l_Result := Get_Dir_Full(p_PH_Num    => l_Ph_Num,
                                         p_Prefix      => l_Prefix,
                                         p_Z_Id        => l_Z_Id,
                                         p_Z_Name      => l_Z_Name,
                                         p_Z_Id_Parent => l_Z_Id_Parent,
                                         p_City_FZ     => l_City_FZ,
                                         p_MgMn        => l_MgMn,
                                         p_Ph_Type     => l_Ph_Type, -- 'A' - ABC, 'D' - DEF
                                         p_H_Id        => l_H_Id
                                        );                
                ELSE
                  l_Result := -1; -- искать нечего, все вар-ты перепробованны
                END IF;  
        END;
        
    END LOOP;
              
    IF p_TD_Id IS NULL THEN
        RAISE no_data_found;
    ELSE
        RETURN 0;
    END IF;     
    
EXCEPTION
    WHEN no_data_found THEN

       -- заранее ставим код ошибки, который вернется если далее ничего не будет найдено        
        l_Result := pk00_const.с_TZONE_NOT_FOUND; 
    
        -- если зона инициирования не задана, то дальнейшие расчеты бессмысленны
        IF p_Mdl_Type = tariff_ph.pk_const.c_TYPE_DIST
        THEN
                       
          -- Модель дистанционная, пытаемся найти по расстояниям
            l_Idx := lt_Zone_B.FIRST;
              
            LOOP -- цикл перебора возможных направлений Б
          
                EXIT WHEN l_Idx IS NULL;    
                
                l_Ph_Num := p_PhNum_A; -- номер, с которого начинаем поиск направлений А
                
                LOOP -- перебор возможных направлений А
                
                    l_Z_Id := Get_Dir(p_PH_Num      => l_Ph_Num,
                                      p_Prefix      => l_Prefix,
                                      p_Z_Name      => l_Z_Name
                                     );
                
                    EXIT WHEN l_Z_Id IS NULL OR l_Ph_Num IS NULL OR
                              l_Result = pk00_const.c_RET_OK;
                
                    BEGIN
        
                        SELECT d.td_id, 
                               zi.z_id z_i_id, zi.z_name z_i_name, 
                               zt.z_id z_t_id, zt.z_name z_t_name
                          INTO p_TD_Id, 
                               p_Z_Id_I, p_Init_Z_Name,                  
                               p_Z_Id_T, p_Term_Z_Name                                       
                          FROM tariff_ph.d22_zone_model_trf_zone tz,
                               tariff_ph.d08_dist_mg_l d,
                               tariff_ph.d01_Zone zi,
                               tariff_ph.d01_Zone zt
                          WHERE tz.zmdl_id = p_Zmdl_Id
                            AND tz.td_id  = d.td_id
                            AND d.z_id_zt = zt.z_id   
                            AND d.z_id_zi = zi.z_id
                            AND zi.is_zi = 1
                            AND zi.z_id = l_Z_Id
                            AND zt.is_zt = 1
                            AND zt.z_id = lt_Zone_B(l_Idx).z_id
                            AND (p_With_Price = 'N'
                                  OR
                                 EXISTS (SELECT 1
                                           FROM tariff_ph.d42_trf_price pr
                                          WHERE pr.trf_id = p_Trf_Id
                                            AND pr.td_id = d.td_id)
                                );                           
                       
                       -- найдена дист. модель, запоминанием успешные префиксы номеров     
                        p_Prefix_A := l_Prefix;    
                        p_Prefix_B := lt_Zone_B(l_Idx).pref_b;
                            
                        l_Result := pk00_const.c_RET_OK; -- возвращаем 0, всё найдено
                            
                    EXCEPTION
                        WHEN no_data_found THEN
                                        
                            l_Ph_Num := SUBSTR(p_PhNum_A,1, LENGTH(l_Prefix)-1);
                            
                           -- получаем Z_ID 
                            IF l_Ph_Num IS NOT NULL THEN
                                l_Z_Id := Get_Dir(p_PH_Num      => l_Ph_Num,
                                                  p_Prefix      => l_Prefix,
                                                  p_Z_Name      => l_Z_Name
                                                 );

                            END IF;  

                    END;                         

                END LOOP; -- перебор всех направлений номера А
                
                l_Idx := lt_Zone_B.NEXT(l_Idx);

            END LOOP; -- перебор всех направлений номера Б
            
        END IF;
                
        RETURN l_Result;
                              
END Get_Zone_New;

--- Поиск зоновой модели для составных тарифов
--   0 - успешное завершение
-- < 0 - код ошибки для "не найдена золновая модель для составного тарифа"  
FUNCTION Get_ZMDL_MP_Trf(p_Trf_Id      IN  number,
                         p_Ph_Num      IN  varchar2, 
                         p_MP_8800_Id  OUT number,
                         p_Zmdl_Id     OUT number
                        ) RETURN number
IS

BEGIN

    -- 1. Поиск по ABC
    SELECT MP_8800_ID, zmdl_id
      INTO p_MP_8800_Id, p_Zmdl_Id 
      FROM (
            SELECT mp.MP_8800_ID, mp.zmdl_id 
              FROM tariff_ph.D44_8800_MP_P mp,
                   tariff_ph.d01_zone z,
                   (SELECT abc_h_id, NULL def_h_id, prefix  
                      FROM tariff_ph.d02_zone_abc a
                     WHERE p_Ph_Num LIKE a.prefix || '%' 
                    UNION ALL
                    SELECT NULL abc_h_id, def_h_id, prefix  
                      FROM tariff_ph.d03_zone_def d
                     WHERE p_Ph_Num LIKE d.prefix || '%'
                   ) p                    
             WHERE z.z_id = mp.z_id
               AND (
                    (z.abc_h_id = p.abc_h_id AND mp.prefix_type = tariff_ph.pk_const.c_PREFIX_ABC)
                      OR
                    (z.def_h_id = p.def_h_id AND mp.prefix_type = tariff_ph.pk_const.c_PREFIX_DEF)                      
                    )
               AND mp.trf_id = p_Trf_Id 
            ORDER BY LENGTH(p.prefix) DESC     
           )
     WHERE ROWNUM = 1;          
                
    RETURN 0; 
    
EXCEPTION
    WHEN no_data_found THEN
        
        BEGIN
           -- ищем значение по-умолчанию
            SELECT MP_8800_ID, zmdl_id
              INTO p_MP_8800_Id, p_Zmdl_Id 
              FROM tariff_ph.D44_8800_MP_P mp
             WHERE mp.is_default = 'Y' 
               AND mp.trf_id = p_Trf_Id;
            
            RETURN 0;
                        
        EXCEPTION
            WHEN no_data_found THEN      
                                 
               RETURN pk00_const.с_ZMDL_MP_NOT_FOUND; -- ничего не нашли  
        END;                            
             
END Get_ZMDL_MP_Trf;


-- Определение льготное или не льготное время для заданной зоны инициирования
-- 1 - тариф льготный
-- 0 - тариф не льготный (рабочее время) 
FUNCTION Check_Pref_Time(p_Trf_Id    number,
                         p_Z_Id      number,
                         p_Call_Type number,
                         p_Call_Time date
                        ) RETURN number 
IS

    c_Not_Work_Time CONSTANT number := 1; 
    c_Work_Time     CONSTANT number := 0;

    l_Result number;
    l_Time_From INTERVAL DAY(0) TO SECOND(0);
    l_Time_To   INTERVAL DAY(0) TO SECOND(0);
    
    l_Idx       PLS_INTEGER;
    lt_Bis_Time t_Zone_Bis_Time;
    
BEGIN

    -- проверка праздник/перенос
    BEGIN
        /*SELECT c.date_type_id
          INTO l_Result
          FROM calendar_t c
         WHERE c.calendar_date = TRUNC(p_Call_Time);    */

        l_Result := gt_Calendar(TO_CHAR(TRUNC(p_Call_Time),'dd.mm.yyyy'));

        IF l_Result = pk00_const.c_CALENDAR_WEEKDAY_ID THEN -- рабочий день
            l_Result := c_Work_Time;
        ELSIF l_Result = pk00_const.c_CALENDAR_HOLIDAY_ID THEN -- праздник
            l_Result := c_Not_Work_Time;
        ELSE
           -- В календаре дня нет или тип какой-то непонятный, то идем дальше по стандартной схеме дальше 
            l_Result := NULL; 
        END IF;

    EXCEPTION
        WHEN no_data_found THEN
            NULL;
    END;    

   --
    IF l_Result IS NULL AND -- по календарю тип дня не определен 
       TRIM(TO_CHAR(p_Call_Time,'DAY')) IN ('SATURDAY','SUNDAY') 
    THEN
       -- выходные дни - всё время нерабочее
        l_Result := c_Not_Work_Time;   
    
    ELSIF l_Result IS NULL OR -- по календарю тип дня не определен  
          l_Result = c_Work_Time -- или день рабочий
    THEN
    
        BEGIN
           -- проверяем сначала нет ли индивидуальных значений рабочего времени на ТП 
            /*SELECT DECODE(p_Call_Type, 7, h.BT_MG_FROM, 8, h.BT_MN_FROM, NULL),
                   DECODE(p_Call_Type, 7, h.BT_MG_TO, 8, h.BT_MN_TO, NULL)
              INTO l_Time_From, l_Time_To
              FROM tariff_ph.D41_TRF_HEADER h
             WHERE h.is_tm_not_std = 'Y'
               AND h.trf_id = p_Trf_Id;*/
                   
            IF p_Call_Type = 7 THEN
                l_Time_From := gt_TrfPrefTime(p_Trf_Id).bt_mg_from;     
                l_Time_To   := gt_TrfPrefTime(p_Trf_Id).bt_mg_to;
            ELSIF p_Call_Type = 8 THEN
                l_Time_From := gt_TrfPrefTime(p_Trf_Id).bt_mn_from;     
                l_Time_To   := gt_TrfPrefTime(p_Trf_Id).bt_mn_to;                
            ELSE
                RAISE no_data_found;
            END IF;    
              
          --- На тарифе есть индивидуальное время. Проверяем входит ли текущий момент в рабочий период или нет. 
            IF p_Call_Time BETWEEN TRUNC(p_Call_Time) + l_Time_From AND TRUNC(p_Call_Time) + l_Time_To
            THEN
                l_Result := c_Work_Time; -- рабочее время
            ELSE
                l_Result := c_Not_Work_Time; -- льготное время
            END IF;
            
        EXCEPTION
            WHEN no_data_found THEN
              -- индивидуальных значений льготного времени на ТП нет, идем по стандартной схеме - для зоновой модели
                BEGIN
                   -- проверяем, является ли время рабочим
                   -- (т.к. все парметры в справочнике лежат скопом и хрен занет как их доставать и какому началу диа-
                   -- пазона какой принадлежит конец, если вдруг диапазонов больше 1, то предполагаем что первому началу
                   -- (по сортировке) принадлежит первый же конец и т.д. Диапазонов с переходом через границу дня, 
                   -- по согласованию, быть не должно)  
                  /*  SELECT 0
                      INTO l_Result
                      FROM ( 
                            SELECT ROWNUM rn_f, TO_DSINTERVAL(vf.VALUE) time_from
                              FROM tariff_ph.D04_ZONE_INIT_PARAM_VAL vf,
                                   tariff_ph.d05_dct_param pf
                             WHERE vf.z_id = p_Z_Id
                               AND pf.par_name = DECODE(p_Call_Type, 7, 'BT_MG_FROM', 8, 'BT_MN_FROM', NULL)  
                               AND vf.par_id = pf.par_id
                             ORDER BY vf.VALUE  
                           ) f,    
                           (    
                            SELECT ROWNUM rn_t, TO_DSINTERVAL(vt.VALUE) time_to
                              FROM tariff_ph.D04_ZONE_INIT_PARAM_VAL vt,
                                   tariff_ph.d05_dct_param pt
                             WHERE vt.z_id = p_Z_Id
                               AND pt.par_name = DECODE(p_Call_Type, 7, 'BT_MG_TO', 8, 'BT_MN_TO', NULL)
                               AND vt.par_id = pt.par_id
                              ORDER BY vt.VALUE  
                           ) t   
                     WHERE f.rn_f = t.rn_t   
                       AND p_Call_Time BETWEEN TRUNC(p_Call_Time) + f.time_from AND TRUNC(p_Call_Time) + t.time_to;*/

                    IF p_Call_Type = 7 THEN
                        lt_Bis_Time := gt_Mg_Bis_Time;
                    ELSIF p_Call_Type = 8 THEN
                        lt_Bis_Time := gt_Mn_Bis_Time;
                    END IF;
                        
                    l_Result := c_Not_Work_Time; -- устанавливаем время как нерабочее
                    
                    l_Idx := lt_Bis_Time(p_Z_Id).FIRST;

                    LOOP -- перебираем все диапазоны рабочих времен для заданной зоновой модели 
                    
                        IF p_Call_Time BETWEEN TRUNC(p_Call_Time)+lt_Bis_Time(p_Z_Id)(l_Idx).time_from
                                           AND TRUNC(p_Call_Time)+lt_Bis_Time(p_Z_Id)(l_Idx).time_to
                        THEN              
                            l_Result := c_Work_Time; -- в текущей зоновой модели время звонка попадает в диапазон рабочего времени 
                        END IF;
                            
                        l_Idx := lt_Bis_Time(p_Z_Id).NEXT(l_Idx);
                            
                       -- выходим если все диапазоны просмотрены или время звонка явно определено как рабочее 
                        EXIT WHEN l_Idx IS NULL OR l_Result = c_Work_Time;
                                
                    END LOOP;    

                EXCEPTION
                    WHEN no_data_found THEN
                        l_Result := c_Not_Work_Time;  --     
                    WHEN others THEN
                        IF SQLCODE = -6502 THEN   
                            l_Result := c_Not_Work_Time;
                        END IF;                                       
                END;
        END;    
                 
    END IF;
    
    RETURN l_Result;      

END Check_Pref_Time;


FUNCTION Trf_MMTS(pr_Call rec_cdr,
                  p_Agent number DEFAULT 0
                 ) RETURN PIN.BDR_PH_TYPE
IS

    c_prcName       CONSTANT varchar2(32) := 'Trf_MMTS'; 
      
    ret_rec         PIN.BDR_PH_TYPE := PIN.BDR_PH_TYPE(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
                                                      );

    l_Day           date := TO_DATE('01.01.2000','dd.mm.yyyy');   
    
    l_Result        number;
    l_Round_Id      number;
    l_Unpaid_Sec    number;
    l_Holiday       number;
    l_Model_Type    number;
    l_Abn_A         varchar2(34);
    l_Abn_B         varchar2(34);
    l_8800_MP       varchar2(1);
    l_I_Z_Id_Parent number;
    l_I_City_FZ     varchar2(1);
    l_T_Z_Id_Parent number;
    l_T_City_FZ     varchar2(1);

    TARIFF_NOT_FOUND  EXCEPTION;
    TARIFF_NOT_UNIQUE EXCEPTION;
    TRF_ERROR         EXCEPTION;
    CALL_TYPE_ERR     EXCEPTION;

BEGIN
    
    ret_rec.row_id          := ROWIDTOCHAR(pr_Call.row_id);
    ret_rec.CDR_Service_Id  := pr_Call.Service_Id;
    ret_rec.Cdr_Id          := pr_Call.cdr_id;
    ret_rec.start_time      := pr_Call.Bill_Date - pr_Call.UTC_Offset; 
    ret_rec.UTC_Offset      := pr_Call.UTC_Offset; -- IntDayToSec_to_UTCOffset()
    ret_rec.Bill_Date       := pr_Call.Bill_Date;
    ret_rec.Order_Id        := pr_Call.order_id;
    ret_rec.Order_Ph        := pr_Call.order_ph;  -- номер телефона, по которому был определён л/счет      
    ret_rec.Duration        := pr_Call.duration;

    IF pr_Call.Service_Id = pin.pk00_const.c_FreeCall_Id AND
       pr_Call.Called_Num IS NOT NULL
    THEN
       -- индивидуальная подмена номера B, т.к. тарификация не по номеру привязки (8800), а по номеру приземления                                       
        IF pr_Call.called_address_nature = 2 THEN -- признак МГ соединения
           ret_rec.Abn_B  := '7' || pr_Call.Called_Num;                    
        ELSE   
           ret_rec.Abn_B  := pr_Call.Called_Num;
        END IF;    
                    
    ELSE
                                                
        ret_rec.Abn_B    := pr_Call.Abn_B;                    
                        
    END IF;

    IF pr_Call.data_type IN ('SPB','SPBCLN') THEN

        ret_rec.Abn_A := pin.Norm_Ph_Number('SPB',pr_Call.Abn_A); --pk1110_tariff_xttk.Norm_Ph_SPb(pr_Call.Abn_A);
        ret_rec.Abn_B := pin.Norm_Ph_Number('SPB',pr_Call.Abn_B); --pk1110_tariff_xttk.Norm_Ph_SPb(pr_Call.Abn_B);

    ELSE
    
        ret_rec.Abn_A := pin.Norm_Ph_Number('KTTK_MMTS',pr_Call.Abn_A,'A');--PK111_TARIFFING.Normalize_A(pr_Call.Abn_A);
        ret_rec.Abn_B := pin.Norm_Ph_Number('KTTK_MMTS',ret_rec.Abn_B,'B');
        
    END IF;        

   -- если звонки на 8800, то переподстановка номеров (так внесены тарифы для совместимости с обычными тарифами)  
    IF pr_Call.Service_Id = pin.pk00_const.c_FreeCall_Id THEN
       l_Abn_A := ret_rec.abn_b;
       l_Abn_B := pin.Norm_Ph_Number('KTTK_MMTS',ret_rec.abn_a,'A');
       
       ret_rec.SubService_Id := pk00_const.c_SUBSRV_FREE;
       
    ELSE
       l_Abn_A := ret_rec.abn_a;
       l_Abn_B := ret_rec.abn_b;    
       
       -- опред. МГ или МН вызов
       ret_rec.SubService_Id := Get_MMTS_SubSrv_Type(p_Abn_A => l_Abn_A,
                                                     p_Abn_B => l_Abn_B
                                                    );        
       
    END IF;

    BEGIN        

       -- определяем по заказу лицевой счет и тарифный план
       -- (тарифы МГ,МН и freecall)
        IF NVL(p_Agent,0) != 1 THEN           
           -- Если указан тариф на услугу, то берем его, если нет - то тариф службы
            SELECT o.Account_Id, o.order_no, r.rateplan_code, o.service_id, r.ratesystem_id,
                   o.order_body_id
              INTO ret_rec.Account_Id, ret_rec.order_no, ret_rec.Trf_Code, ret_rec.Service_Id, ret_rec.ratesystem_id,
                   ret_rec.order_body_id
              FROM (
                    SELECT o.Account_Id, o.order_no, NVL(b.rateplan_id, o.rateplan_id) rateplan_id, o.service_id,
                           b.order_body_id,
                           -- берем самый свежий по дате, ибо задвои появляются - задолбало!
                           row_number() OVER (PARTITION BY o.order_id ORDER BY NVL(b.modify_date, b.create_date) DESC) rn
                      FROM order_t o,
                           order_body_t b 
                     WHERE o.order_id = ret_rec.Order_Id 
                       AND o.order_id = b.order_id(+)
                       AND b.charge_type(+) = 'USG'
                       AND b.subservice_id(+) = ret_rec.SubService_Id 
                       AND ret_rec.bill_date BETWEEN b.date_from(+) AND b.date_to(+) 
                    ) o,
                    rateplan_t r
              WHERE o.rn = 1
                AND o.rateplan_id = r.rateplan_id(+);
              
        ELSE 
          -- надо считать агентские договора     
            SELECT o.Account_Id, o.order_no, r.rateplan_code, o.service_id, r.ratesystem_id
              INTO ret_rec.Account_Id, ret_rec.order_no, ret_rec.Trf_Code, ret_rec.Service_Id, ret_rec.ratesystem_id
              FROM order_t o,
                   rateplan_t r
             WHERE o.order_id = ret_rec.Order_Id 
               AND o.agent_rateplan_id = r.rateplan_id(+);
                             
        END IF;      
           
        IF ret_rec.Trf_Code IS NULL THEN
          RAISE no_data_found; 
        END IF;    

        BEGIN
           --- Поиск тарифного плана в схеме тарифов               
            ret_rec.trf_id  := gt_Tariff(ret_rec.Trf_Code).trf_id; 
            l_Round_Id      := gt_Tariff(ret_rec.Trf_Code).round_v_id; 
            l_Unpaid_Sec    := gt_Tariff(ret_rec.Trf_Code).unpaid_seconds; 
            ret_rec.zmdl_id := gt_Tariff(ret_rec.Trf_Code).zmdl_id; 
            l_8800_MP       := gt_Tariff(ret_rec.Trf_Code).is_8800_MP;
             
        EXCEPTION
            WHEN no_data_found THEN
                RAISE TARIFF_NOT_FOUND;

        END;        
                                                 
       -- получаем тарифицируемые минуты    
        BEGIN
           -- получаем правило округления
            ret_rec.bill_minutes := rounding(ret_rec.Duration, 
                                             gt_Rounding(l_Round_Id) -- получаем sec_base по id правила округления
                                            )/60; 
             
        EXCEPTION
            WHEN no_data_found THEN
               -- по умолчанию ставим до минуты округление
                ret_rec.bill_minutes := rounding(ret_rec.Duration, 60)/60;
               -- пошем в лог предупреждение  
                Pk01_Syslog.write_Msg(p_Msg   => 'Не найдено правило округления для тарифа ' || TO_CHAR(ret_rec.trf_id), 
                                      p_Src   => gc_PkgName || '.' || c_prcName,
                                      p_level => Pk01_Syslog.L_err);                                                
                    
        END;

       -- 
       --- Определяем направление куда звонили
       ---
           
        IF l_8800_MP = 'Y' THEN 
            -- составной тариф для 8800. Получаем зоновую модель (zmdl_id), т.к. она зависит в данном случае от номера Б
            ret_rec.bdr_status := Get_ZMDL_MP_Trf(p_Trf_Id      => ret_rec.trf_id, -- in
                                                  p_Ph_Num      => ret_rec.abn_b, -- in 
                                                  p_MP_8800_Id  => ret_rec.mp_8800_id, -- out
                                                  p_Zmdl_Id     => ret_rec.zmdl_id -- out
                                                 );                    
        
            IF ret_rec.bdr_status < 0 THEN
               -- не нашли зоновую модель - выходим
                RAISE TRF_ERROR;
            END IF;    
            
        END IF;

        BEGIN
            -- 1. Получаем тип зоновой модели
            SELECT z.dist_regn
              INTO l_Model_Type
              FROM tariff_ph.d21_zone_model z
             WHERE z.zmdl_id = ret_rec.zmdl_id;

        EXCEPTION
            WHEN no_data_found THEN
                ret_rec.BDR_Status := t_BDR_Status('ZMDL_TRF_NF'); -- -14 (не найденна зоновая модель, указанная для тарифа)
                RAISE TRF_ERROR;        
        END;     
            
       -- 2. определяем направление      
        IF l_8800_MP = 'Y' THEN
                                               
            ret_rec.BDR_Status := Get_Zone_New(p_Trf_Id      => ret_rec.trf_id,    -- in
                                           p_Zmdl_Id     => ret_rec.zmdl_id,   -- in
                                           p_Mdl_Type    => l_Model_Type,      -- in
                                           p_PhNum_A     => l_Abn_A,           -- in
                                           p_PhNum_B     => l_Abn_B,           -- in
                                           p_With_Price  => 'N',  -- направления только с расценками 
                                           p_TD_Id       => ret_rec.TD_Id,      -- out
                                           p_Z_Id_I      => ret_rec.Init_Z_ID,  -- out
                                           p_Prefix_A    => ret_rec.Prefix_A,   -- out
                                           p_Init_Z_Name => ret_rec.Init_Z_Name,-- out                 
                                           p_Z_Id_T      => ret_rec.Term_Z_Id,  -- out
                                           p_Prefix_B    => ret_rec.Prefix_B,   -- out
                                           p_Term_Z_Name => ret_rec.Term_Z_Name -- out
                                          );                                                                                       
        
        ELSE  
                     
            ret_rec.BDR_Status := Get_Zone_New(p_Trf_Id      => ret_rec.trf_id,    -- in
                                           p_Zmdl_Id     => ret_rec.zmdl_id,   -- in
                                           p_Mdl_Type    => l_Model_Type,      -- in
                                           p_PhNum_A     => l_Abn_A,           -- in
                                           p_PhNum_B     => l_Abn_B,           -- in
                                           p_With_Price  => 'Y',  -- направления только с расценками 
                                           p_TD_Id       => ret_rec.TD_Id,      -- out
                                           p_Z_Id_I      => ret_rec.Init_Z_ID,  -- out
                                           p_Prefix_A    => ret_rec.Prefix_A,   -- out
                                           p_Init_Z_Name => ret_rec.Init_Z_Name,-- out                 
                                           p_Z_Id_T      => ret_rec.Term_Z_Id,  -- out
                                           p_Prefix_B    => ret_rec.Prefix_B,   -- out
                                           p_Term_Z_Name => ret_rec.Term_Z_Name -- out
                                          );                                                          
                
        END IF;
                                 
        -- Определяем откуда звонок, если направление не определилось 
        IF ret_rec.Init_Z_ID IS NULL THEN
            -- сначала ищем по стационарн. номерам
          /*  l_Result := Get_ABC_Pref(p_PH_Num      => l_Abn_A,     -- in
                                     p_Prefix      => ret_rec.prefix_a,  -- out
                                     p_Z_Id        => ret_rec.init_z_id, -- out
                                     p_Z_Name      => ret_rec.Init_Z_Name, -- out
                                     p_Z_Id_Parent => l_I_Z_Id_Parent,
                                     p_City_FZ     => l_I_City_Fz
                                    );     
                                    
            IF ret_rec.init_z_id IS NULL THEN
               -- если не нашли, то по мобильным
                l_Result := Get_DEF_Pref(p_PH_Num => l_Abn_A,     -- in
                                         p_Prefix => ret_rec.prefix_a,  -- out
                                         p_Z_Id   => ret_rec.init_z_id, -- out
                                         p_Z_Name => ret_rec.Init_Z_Name, -- out
                                         p_Z_Id_Parent => l_T_Z_Id_Parent,
                                         p_City_FZ     => l_T_City_Fz
                                        );                 
            
            END IF;        */                   
                                    
            ret_rec.init_z_id := Get_Dir(p_PH_Num  => l_Abn_A,
                                         p_Prefix  => ret_rec.prefix_a, --out
                                         p_Z_Name  => ret_rec.Init_Z_Name
                                        );
            
        END IF;                            
        
        
        IF ret_rec.BDR_Status = 0 THEN -- завершаем тарификацию                         

            -- получаем тип соединения (МН, МГ и т.д.)
            IF pr_Call.Service_Id = pin.pk00_const.c_FreeCall_Id THEN
                
                ret_rec.call_type := tariff_ph.pk_const.c_TYPE_8800; -- 4
                    
            ELSIF l_Model_Type = tariff_ph.pk_const.c_TYPE_ZONE THEN
                   
                ret_rec.call_type := 9;
                   
            ELSE
                
                SELECT DECODE(d.mg_mn, tariff_ph.pk_const.c_ZONE_TERM_TYPE_MN, 8,
                                       tariff_ph.pk_const.c_ZONE_TERM_TYPE_MG, 7,
                                       -1)
                  INTO ret_rec.call_type
                  FROM tariff_ph.d10_trf_direction d
                 WHERE d.td_id = ret_rec.TD_Id; 
                     
              --  ret_rec.Service_Id := pk00_const.c_SERVICE_CALL_MGMN; 
                     
            END IF;     
            
            -- определяем подуслугу ЦБ        
            IF ret_rec.SubService_Id IS NULL THEN
                IF ret_rec.Call_Type = 8 THEN
                    ret_rec.SubService_Id := pk00_const.c_SUBSRV_MN; --gt_SubSrv_Type('MN');
                ELSIF ret_rec.Call_Type = 7 THEN   
                    ret_rec.SubService_Id := pk00_const.c_SUBSRV_MG; --gt_SubSrv_Type('MG');
                ELSIF ret_rec.Call_Type = 9 THEN   
                    ret_rec.SubService_Id := pk00_const.c_SUBSRV_ZONE; 
                ELSIF ret_rec.Call_Type = 4 THEN -- 8800
                    ret_rec.SubService_Id := pk00_const.c_SUBSRV_FREE;                    
                ELSE    
                    ret_rec.SubService_Id := NULL;
                END IF;            
            END IF;

            -- определяем льготное/не льготное время
            l_Holiday := Check_Pref_Time(p_Trf_Id    => ret_rec.trf_id,
                                         p_Z_Id      => ret_rec.Init_Z_Id,
                                         p_Call_Type => ret_rec.call_type,
                                         p_Call_Time => ret_rec.bill_date
                                        );
             
            BEGIN
                -- получаем цену
                IF l_8800_MP = 'Y' THEN
                
                    SELECT p.price_0
                      INTO ret_rec.price
                      FROM tariff_ph.d45_8800_mp_price p
                     WHERE p.mp_8800_id = ret_rec.mp_8800_id
                       AND p.td_id      = ret_rec.td_id;
                
                ELSE 
                
                    SELECT (CASE WHEN l_Holiday = 1 THEN NVL(p.price_1, p.price_0)
                                 WHEN l_Holiday = 0 THEN p.price_0
                                 ELSE NVL(p.price_1, p.price_0)
                            END)
                      INTO ret_rec.price
                      FROM tariff_ph.d42_trf_price p
                     WHERE p.trf_id = ret_rec.trf_id
                       AND p.td_id  = ret_rec.td_id;
                       
                END IF;       
                       
                IF ret_rec.price IS NULL THEN
                    RAISE no_data_found;
                
                ELSE
                
                    -- получаем стоимость соединения
                    IF ret_rec.Duration <= l_Unpaid_Sec                     
                    THEN
                        ret_rec.Amount := 0;
                        ret_rec.Bill_Minutes := 0; -- что бы в детализации клиентам не показать что вызов 
                                                   -- минута и стоимость = 0
                    ELSE
                        ret_rec.Amount       := ret_rec.Price * ret_rec.Bill_Minutes;
                        IF ret_rec.Amount != TRUNC(ret_rec.Amount,2) THEN
                           -- округление до ближайших бОльших копеек
                           ret_rec.Amount := TRUNC(ret_rec.Amount,2) + 0.01;
                        END IF;        
                    END IF;                    

                    ret_rec.BDR_Status := t_BDR_Status('OK'); -- OK;                    
                    
                END IF;       
                   
            EXCEPTION
                WHEN no_data_found THEN
                    ret_rec.BDR_Status := t_BDR_Status('PRICE_NF'); -- -7 (не найдены расценки)
            END;            
                
        END IF; -- найдена или нет зона терминации
            
    EXCEPTION -- не найден тарифный план
        WHEN no_data_found THEN

            ret_rec.BDR_Status := t_BDR_Status('ORD_TP_NF'); -- (-6) нет ТП у заказа
            
        WHEN TARIFF_NOT_FOUND THEN
                
            ret_rec.BDR_Status := t_BDR_Status('TP_NF'); -- (-5) нет ТП в схеме тарифов    
        
        WHEN TARIFF_NOT_UNIQUE THEN    
        
            ret_rec.BDR_Status := t_BDR_Status('TP_NU'); -- (-19) несколько ТП
        
        WHEN CALL_TYPE_ERR THEN
                
            ret_rec.BDR_Status := t_BDR_Status('LZ_CALL'); -- (-16) местное или зонове соедирение на МГМН-коммутаторе        
        
        WHEN TRF_ERROR THEN
            NULL; -- выходим                  
            
    END;     

    IF pr_Call.Data_Type = 'MMTS_FLT' -- тарификация несостоявшихся соединений. Суммы не нужны
    THEN
        ret_rec.Amount := 0;
        ret_rec.Bill_Minutes := 0;    
        ret_rec.BDR_Status := t_BDR_Status('OK'); -- все показываются как нормальные
    END IF;

    RETURN ret_rec; -- возвращаем протарифицированную запись

END Trf_MMTS;
-----
-- Тарификация по схеме TTK_SMETA
--
FUNCTION Trf_AG_MMTS(pr_Call rec_cdr
                    ) RETURN PIN.BDR_PH_TYPE
IS

    c_prcName       CONSTANT varchar2(32) := 'Trf_AG_MMTS'; 
      
    ret_rec         PIN.BDR_PH_TYPE := PIN.BDR_PH_TYPE(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
                                                      );

    c_Round_Rule_Id number := 1; -- id правила округления по таблице ttk_smeta.sm30_rule_type

    l_Day           date := TO_DATE('01.01.2000','dd.mm.yyyy');   
    
    l_Result        number;
    l_Abn_A         varchar2(34);
    l_Abn_B         varchar2(34);

    l_Min_Dur        number;
    l_First_interval number;
    l_Round_To       number;
    l_Type_B         varchar2(8);

    TARIFF_NOT_FOUND  EXCEPTION;
    TARIFF_NOT_UNIQUE EXCEPTION;
    DIR_NOT_FOUND     EXCEPTION;
    PRICE_NOT_FOUND   EXCEPTION;
    TOO_MANY_PRICES   EXCEPTION;
    
BEGIN
    
    ret_rec.row_id          := ROWIDTOCHAR(pr_Call.row_id);
    ret_rec.CDR_Service_Id  := pr_Call.Service_Id;
    ret_rec.Cdr_Id          := pr_Call.cdr_id;
    ret_rec.start_time      := pr_Call.Bill_Date - pr_Call.UTC_Offset; 
    ret_rec.UTC_Offset      := pr_Call.UTC_Offset; -- IntDayToSec_to_UTCOffset()
    ret_rec.Bill_Date       := pr_Call.Bill_Date;
    ret_rec.Order_Id        := pr_Call.order_id;
    ret_rec.Order_Ph        := pr_Call.order_ph;  -- номер телефона, по которому был определён л/счет      
    ret_rec.Duration        := pr_Call.duration;

    IF pr_Call.Service_Id = pin.pk00_const.c_FreeCall_Id AND
       pr_Call.Called_Num IS NOT NULL
    THEN
       -- индивидуальная подмена номера B, т.к. тарификация не по номеру привязки (8800), а по номеру приземления                                       
        IF pr_Call.called_address_nature = 2 THEN -- признак МГ соединения
           ret_rec.Abn_B  := '7' || pr_Call.Called_Num;                    
        ELSE   
           ret_rec.Abn_B  := pr_Call.Called_Num;
        END IF;    
                    
    ELSE
                                                
        ret_rec.Abn_B    := pr_Call.Abn_B;                    
                        
    END IF;

    ret_rec.Abn_A := pin.Norm_Ph_Number('KTTK_MMTS',pr_Call.Abn_A,'A');--PK111_TARIFFING.Normalize_A(pr_Call.Abn_A);
    ret_rec.Abn_B := pin.Norm_Ph_Number('KTTK_MMTS',ret_rec.Abn_B,'B');
        

   -- если звонки на 8800, то переподстановка номеров (так внесены тарифы для совместимости с обычными тарифами)  
    IF pr_Call.Service_Id = pin.pk00_const.c_FreeCall_Id THEN
       l_Abn_A := ret_rec.abn_b;
       l_Abn_B := pin.Norm_Ph_Number('KTTK_MMTS',ret_rec.abn_a,'A');
    ELSE
       l_Abn_A := ret_rec.abn_a;
       l_Abn_B := ret_rec.abn_b;    
    END IF;

    BEGIN        

       --- Поиск тарифного плана в схеме тарифов               
        SELECT h.tariff_id, o.order_no, o.account_id,
               o.service_id
          INTO ret_rec.trf_id, ret_rec.order_no, ret_rec.account_id,
               ret_rec.service_id
          FROM pin.rss01_ord_trf h,
               pin.order_t o
         WHERE h.tariff_type_id = 1 -- Доходный тариф
           AND h.order_id = o.order_id
           AND o.order_id = ret_rec.Order_Id;
        --   AND ret_rec.Bill_Date BETWEEN h.date_from AND h.date_to; -- дат в исходных данных вообще нет 
                    
        -- определяем услугу
        l_Result := Get_SubSrv_Id_With_Dir(p_PhNum_A  => ret_rec.abn_a,
                                            p_PhNum_B  => ret_rec.abn_b,
                                            p_Z_Id_A   => ret_rec.init_z_id,  -- out
                                            p_Z_Name_A => ret_rec.Init_Z_Name,-- out                                
                                            p_Prefix_A => ret_rec.prefix_a,   -- out
                                            p_Z_Id_B   => ret_rec.term_z_id,  -- out
                                            p_Z_Name_B => ret_rec.Term_Z_Name,-- out                                
                                            p_Prefix_B => ret_rec.prefix_b,    -- out
                                            p_Type_B   => l_Type_B -- A (звонок на ABC) или D (звонок на DEF)                            
                                           );
       
        IF l_Result IN (t_Subservice('LOCAL'), t_Subservice('ZONE')) THEN
           -- считается что трафик должен быть только МГМН, посему ставим как межгород если 
           -- почему-то определился как местный или зоновый
            ret_rec.subservice_id := t_Subservice('MG');
        ELSE
            ret_rec.subservice_id := l_Result;  
        END IF;
        
        BEGIN   
           -- Ищем направление, по которому тарифицируется соединение
            ret_rec.term_z_id   := NULL;  
            ret_rec.Term_Z_Name := NULL;                                
            ret_rec.prefix_b    := NULL;           
                      
            SELECT direction_id, direction_name, dn
              INTO ret_rec.Term_Z_Id, ret_rec.Term_Z_Name, ret_rec.Prefix_B
              FROM (
                    SELECT d.direction_id, d.direction_name, c.dn
                      FROM ttk_smeta.sm11_direction d,
                           ttk_smeta.sm13_dn_cod c
                     WHERE d.direction_id = c.direction_id
                       AND d.tariff_id = ret_rec.trf_id
                       AND ret_rec.Bill_Date BETWEEN d.date_from AND d.date_to
                       AND ret_rec.Bill_Date BETWEEN c.date_from AND c.date_to
                       AND (
                            SUBSTR(l_Abn_B,1,1)  = c.dn OR
                            SUBSTR(l_Abn_B,1,2)  = c.dn OR
                            SUBSTR(l_Abn_B,1,3)  = c.dn OR
                            SUBSTR(l_Abn_B,1,4)  = c.dn OR
                            SUBSTR(l_Abn_B,1,5)  = c.dn OR
                            SUBSTR(l_Abn_B,1,6)  = c.dn OR
                            SUBSTR(l_Abn_B,1,7)  = c.dn OR
                            SUBSTR(l_Abn_B,1,8)  = c.dn OR
                            SUBSTR(l_Abn_B,1,9)  = c.dn OR
                            SUBSTR(l_Abn_B,1,10) = c.dn OR
                            SUBSTR(l_Abn_B,1,11) = c.dn OR
                            SUBSTR(l_Abn_B,1,12) = c.dn OR
                            SUBSTR(l_Abn_B,1,13) = c.dn OR
                            SUBSTR(l_Abn_B,1,14) = c.dn OR
                            SUBSTR(l_Abn_B,1,15) = c.dn 
                          )
                    ORDER BY LENGTH(c.dn) DESC     
                   )
             WHERE ROWNUM = 1;
                         
        EXCEPTION
            WHEN no_data_found THEN
                RAISE DIR_NOT_FOUND; 
        END;             
                                     
        
        BEGIN   
           -- ищем расценки на найденное направление   
            SELECT t.sm14_id, t.tariff_0 
              INTO ret_rec.TD_Id, ret_rec.price
              FROM TTK_SMETA.SM14_TARIFF t
             WHERE t.direction_id = ret_rec.Term_Z_Id
               AND ret_rec.Bill_Date BETWEEN t.date_from AND t.date_to;        
                 
        EXCEPTION
            WHEN no_data_found THEN
                RAISE PRICE_NOT_FOUND;
            WHEN too_many_rows THEN
                RAISE TOO_MANY_PRICES; 
        END;         
                    
        -- ищем правило округления
        BEGIN
        
            SELECT FILTER, first_interval, round_to
              INTO l_Min_Dur, l_First_interval, l_Round_To
              FROM (
                    SELECT r.rule_value_id, rr.FILTER, rr.first_interval, rr.round_to
                      FROM (
                            SELECT t.rule_value_id, 2 priority
                              FROM ttk_smeta.sm31_ordr_tarif_rule t
                             WHERE t.tarif_id = ret_rec.trf_id
                               AND ret_rec.bill_date BETWEEN t.date_from AND t.date_to
                            UNION ALL
                            SELECT d.rule_value_id, 1 priority
                              FROM ttk_smeta.sm32_direction_rule d
                             WHERE d.direction_id = ret_rec.Term_Z_Id
                               AND ret_rec.bill_date BETWEEN d.date_from AND d.date_to                       
                           ) r,
                           ttk_smeta.rl_rule rl,
                           ttk_smeta.rlrnd01_rule_round rr
                     WHERE rl.rule_type_id = c_Round_Rule_Id -- правила округления
                       AND rl.rule_value_id = r.rule_value_id
                       AND r.rule_value_id = rr.rule_value_id
                     ORDER BY r.priority
                   )
             WHERE ROWNUM = 1;                        
                   
        EXCEPTION
            WHEN no_data_found THEN
               -- значения по-умолчанию
                l_Min_Dur := 0;
                l_First_interval := 1;
                l_Round_To := 1;
        END;    
                
        -- получаем тарифные минуты
        IF ret_rec.Duration <= l_Min_Dur THEN
            ret_rec.bill_minutes := 0; -- длительность меньше порога тарификации
             
        ELSIF ret_rec.Duration > l_Min_Dur AND ret_rec.Duration <= l_First_interval THEN 
        
            ret_rec.bill_minutes := l_First_interval;
        
        ELSIF ret_rec.Duration > l_First_interval THEN
        
            ret_rec.bill_minutes := CEIL(ret_rec.Duration/l_Round_To)*l_Round_To; --  округление с шагом
               
        END IF;        
        
        -- приводим к минутам
        ret_rec.bill_minutes := ROUND(ret_rec.bill_minutes/60,4);
        
        -- получаем стоимость соединения
        ret_rec.amount := ret_rec.price * ret_rec.bill_minutes; 
        
        ret_rec.BDR_Status := t_BDR_Status('OK');
        
    EXCEPTION -- не найден тарифный план
        WHEN no_data_found THEN
            ret_rec.BDR_Status := t_BDR_Status('TP_NF'); -- (-5) нет ТП в схеме тарифов    
        WHEN too_many_rows THEN    
            ret_rec.BDR_Status := t_BDR_Status('TP_NU'); -- (-19) несколько ТП 
        WHEN DIR_NOT_FOUND THEN
            ret_rec.BDR_Status := t_BDR_Status('PREF_B_NF'); -- (-9) не найдена зона завершения вызова        
        WHEN PRICE_NOT_FOUND THEN
            ret_rec.BDR_Status := t_BDR_Status('PRICE_NF'); -- (-7) не найдены расценки на направление                  
        WHEN TOO_MANY_PRICES THEN
            ret_rec.BDR_Status := t_BDR_Status('PRICE_TM'); -- (-7) не найдены расценки на направление
            
    END;     

    RETURN ret_rec; -- возвращаем протарифицированную запись

END Trf_AG_MMTS;


FUNCTION Trf_Zone(pr_Call rec_cdr,
                  p_Agent number DEFAULT 0
                 ) RETURN PIN.BDR_PH_TYPE
IS

    c_prcName       CONSTANT varchar2(16) := 'Trf_Zone'; 

    l_Result        number;
    l_Round_Id      number;
    l_Unpaid_Sec    number;
   
    l_Holiday       number;
    l_Model_Type    number;
    l_Abn_A         varchar2(34);
    l_Abn_B         varchar2(34);
    l_I_Z_Id_Parent number;
    l_I_City_FZ     varchar2(1);
    l_T_Z_Id_Parent number;
    l_T_City_FZ     varchar2(1);    
    l_Type_B        varchar2(1);
    l_Network_Code  varchar2(16);

    ret_rec         PIN.BDR_PH_TYPE := PIN.BDR_PH_TYPE(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
                                                      );

    l_Ref_Trf_Hd    t_Ref; 

    TARIFF_NOT_FOUND EXCEPTION;

    FUNCTION Norm_PH_Num(p_Ph_Num IN varchar2
                        ) RETURN varchar2
    IS
        l_Result varchar2(34);    
    BEGIN

        IF LENGTH(p_Ph_Num) = 11 AND p_Ph_Num LIKE '8%' THEN
            l_Result := '7' || SUBSTR(p_Ph_Num,2);
        ELSIF LENGTH(p_Ph_Num) = 7 THEN  
            l_Result := '7495' || p_Ph_Num;
        ELSE
            l_Result := p_Ph_Num;   
        END IF;       
    
        RETURN l_Result;
    
    END Norm_PH_Num;
                        
BEGIN

    ret_rec.row_id          := ROWIDTOCHAR(pr_Call.row_id);
    ret_rec.CDR_Service_Id  := pr_Call.Service_Id;
    ret_rec.Cdr_Id          := pr_Call.cdr_id;
    ret_rec.start_time      := pr_Call.Bill_Date - pr_Call.UTC_Offset; 
    ret_rec.UTC_Offset      := pr_Call.UTC_Offset; -- IntDayToSec_to_UTCOffset()
    ret_rec.Bill_Date       := pr_Call.Bill_Date;
    ret_rec.Order_Id        := pr_Call.order_id;
    ret_rec.Order_Ph        := pr_Call.order_ph;  -- номер телефона, по которому был определён л/счет      
    ret_rec.Duration        := pr_Call.duration;

   -- Приведение номеров А и Б к стандартному виду
    ret_rec.Abn_A := pin.Norm_Ph_Number(t_Network_Code(pr_Call.Data_Type), pr_Call.Abn_A, 'A'); --pk1110_tariff_xttk.Norm_Ph_SPb(pr_Call.Abn_A);
    ret_rec.Abn_B := pin.Norm_Ph_Number(t_Network_Code(pr_Call.Data_Type), pr_Call.Abn_B, 'B');
        
    IF pr_Call.Data_Type = 'MMTS' THEN
       -- дополнительная нормализация. Приведение в Московским номерам
        ret_rec.Abn_A := Norm_PH_Num(ret_rec.Abn_A);
        ret_rec.Abn_B := Norm_PH_Num(ret_rec.Abn_B);
        
    END IF;    

    BEGIN        

        -- Получаем услугу по типу звонка (местный-зоновый) и направления
        /* расчет до 04.09.2015 
        ret_rec.SubService_Id := Get_SubSrv_Id_With_Dir(p_PhNum_A  => ret_rec.abn_a,
                                                        p_PhNum_B  => ret_rec.abn_b,
                                                        p_Z_Id_A   => ret_rec.init_z_id,  -- out
                                                        p_Z_Name_A => ret_rec.Init_Z_Name,-- out                                
                                                        p_Prefix_A => ret_rec.prefix_a,   -- out
                                                        p_Z_Id_B   => ret_rec.term_z_id,  -- out
                                                        p_Z_Name_B => ret_rec.Term_Z_Name,-- out                                
                                                        p_Prefix_B => ret_rec.prefix_b,    -- out
                                                        p_Type_B   => l_Type_B -- A (звонок на ABC) или D (звонок на DEF)                            
                                                       ); */

      -- =======================================================================
      -- расчет с 04.09.2015
        ret_rec.SubService_Id := Get_Msc_SubService_Id(p_Abn_A  => ret_rec.abn_a,
                                                       p_Abn_B  => ret_rec.abn_b
                                                      ); 

        -- Id и название направление по стороне А   
        ret_rec.init_z_id := Get_Dir(p_PH_Num => ret_rec.abn_a,
                                     p_Prefix => ret_rec.prefix_a,
                                     p_Z_Name => ret_rec.Init_Z_Name
                                    );
                                    
        -- Id и название направление по стороне Б   
        ret_rec.term_z_id := Get_Dir(p_PH_Num => ret_rec.abn_b,
                                     p_Prefix => ret_rec.prefix_b,
                                     p_Z_Name => ret_rec.Term_Z_Name
                                    );                        
       --
       -- =======================================================================                                           
                                    
        IF ret_rec.SubService_Id = t_Subservice('ZONE') THEN
           -- Проставляем признак зонового соединения
            ret_rec.call_type := 9;
        ELSIF ret_rec.SubService_Id = t_Subservice('LOCAL') THEN
           -- Проставляем признак зонового соединения
            ret_rec.call_type := 0;
        ELSE
            NULL;
        END IF;    

       -- 
       -- 2. определяем по заказу лицевой счет и тарифный план
       -- (для зоновых соединений ищем через body)
        ret_rec.bdr_status := t_BDR_Status('ORD_TP_NF'); -- -6  не указан ТП для заказа
        
        IF NVL(p_Agent,0) != 1 THEN
           
            SELECT Account_Id, o.order_no, r.rateplan_code, o.service_id, r.ratesystem_id,
                   o.order_body_id
              INTO ret_rec.Account_Id, ret_rec.Order_No, ret_rec.Trf_Code, ret_rec.Service_Id, ret_rec.ratesystem_id,
                   ret_rec.order_body_id
              FROM (
                    SELECT Account_Id, o.order_no, NVL(b.rateplan_id, o.rateplan_id) rateplan_id, o.service_id,
                           b.order_body_id
                      FROM order_t o,
                           service_subservice_t ss,
                           order_body_t b
                     WHERE o.order_id = pr_Call.order_id
                       AND o.service_id = ss.service_id
                       AND ss.subservice_id = ret_rec.SubService_Id
                       AND o.order_id = b.order_id(+)
                       AND b.subservice_id(+) = ret_rec.SubService_Id --pk00_const.c_SUBSRV_ZONE
                       AND ret_rec.Bill_Date BETWEEN b.date_from(+) AND NVL(b.date_to(+), gc_MaxDate)
                     ORDER BY o.rateplan_id NULLS LAST  
                   ) o,
                   rateplan_t r                    
             WHERE o.rateplan_id = r.rateplan_id
               AND ROWNUM = 1; -- беру первый попавшийся ТП ибо задолбалооо!!!!!!      
                
        ELSE 
          -- надо считать агентские договора     
            SELECT o.Account_Id, o.order_no, r.rateplan_code, o.service_id, r.ratesystem_id
              INTO ret_rec.Account_Id, ret_rec.order_no, ret_rec.Trf_Code, ret_rec.Service_Id, ret_rec.ratesystem_id
              FROM order_t o,
                   rateplan_t r
             WHERE o.order_id = ret_rec.Order_Id 
               AND o.agent_rateplan_id = r.rateplan_id(+);
                             
        END IF;          

        IF ret_rec.Trf_Code IS NULL THEN
            RAISE no_data_found;
        END IF;       

       -- 
       --- 3. Поиск тарифного плана в тарифах 
       --         
        -- ошибка ТП не найден или найденный ТП не зоновый
        ret_rec.bdr_status := t_BDR_Status('TP_NF'); -- (-5) ТП не найден в схеме заказов или есть такой, но не зоновый 

        SELECT h.trf_id, h.round_v_id, h.unpaid_seconds, h.zmdl_id
          INTO ret_rec.trf_id, l_Round_Id, l_Unpaid_Sec, ret_rec.zmdl_id  
          FROM tariff_ph.d41_trf_header h,
               tariff_ph.d21_zone_model z 
         WHERE h.zmdl_id = z.zmdl_id
           AND z.dist_regn = (CASE WHEN ret_rec.SubService_Id = t_Subservice('ZONE') THEN
                                        tariff_ph.pk_const.c_TYPE_ZONE  
                                   WHEN ret_rec.SubService_Id = t_Subservice('LOCAL') THEN     
                                        tariff_ph.pk_const.c_TYPE_MESTN
                              END)           
           AND h.code = ret_rec.Trf_Code; 
                       
       -- получаем тарифицируемые минуты    
        BEGIN
           -- получаем правило округления
            SELECT rounding(ret_rec.Duration, r.sec_base)/60
              INTO ret_rec.bill_minutes 
              FROM tariff_ph.DCT03_ROUND_V r
             WHERE r.round_v_id = l_Round_Id; 
                            
        EXCEPTION
            WHEN no_data_found THEN
               -- по умолчанию ставим до минуты округление
                ret_rec.bill_minutes := rounding(ret_rec.Duration, 60)/60;
               -- пошем в лог предупреждение  
                Pk01_Syslog.write_Msg(p_Msg   => 'Не найдено правило округления для тарифа ' || TO_CHAR(ret_rec.trf_id) ||
                                                 ' (взято по умолч. до минуты)', 
                                      p_Src   => gc_PkgName || '.' || c_prcName,
                                      p_level => Pk01_Syslog.L_err);                                                
                    
        END;

        -- 5. определяем льготное/не льготное время
        l_Holiday := Check_Pref_Time(p_Trf_Id    => ret_rec.trf_id,
                                     p_Z_Id      => ret_rec.Init_Z_Id,
                                     p_Call_Type => ret_rec.call_type,
                                     p_Call_Time => ret_rec.bill_date
                                    );
             
        -- 6. получаем цену
        ret_rec.BDR_Status := t_BDR_Status('PRICE_NF'); -- (-7) расценки не найдены
            
        SELECT (CASE WHEN l_Holiday = 1 THEN NVL(t.price_1, t.price_0)
                     WHEN l_Holiday = 0 THEN t.price_0
                     ELSE NVL(t.price_1, t.price_0)
                END),
                t.td_id
          INTO ret_rec.price, ret_rec.td_id
          FROM (  
                SELECT p.price_1, p.price_0, td.td_id,
                    -- Есть тариф, где зоновый вызов на ABC и DEF имеют разную цену. Явного признака какая
                    -- цена к какому типу относится нет. Пересцкий Игорь сказал прибить гвоздиком id направлений (17.02.2015). 
                       row_number() OVER (ORDER BY (CASE WHEN td.td_id = 548 AND l_Type_B = 'A' THEN 1  
                                                         WHEN td.td_id = 549 AND l_Type_B = 'D' THEN 1
                                                         ELSE 2
                                                   END), price_0 DESC) rn       
                  FROM tariff_ph.d22_zone_model_trf_zone z,
                       tariff_ph.d10_trf_direction td,
                       tariff_ph.d42_trf_price p
                 WHERE z.zmdl_id = ret_rec.zmdl_id
                   AND z.td_id = td.td_id
                   AND z.td_id = p.td_id
                   AND p.trf_id = ret_rec.trf_id
                 ) t
          WHERE rn = 1;

        IF ret_rec.price IS NULL THEN
            RAISE no_data_found;
        END IF;       
                       
        -- получаем стоимость соединения
        IF ret_rec.Duration <= l_Unpaid_Sec THEN
            ret_rec.Amount := 0;
            ret_rec.Bill_Minutes := 0; -- что бы в детализации клиентам не показать что вызов 
                                       -- минута и стоимость = 0                
        ELSE
            ret_rec.Amount       := ret_rec.Price * ret_rec.Bill_Minutes;
            IF ret_rec.Amount != TRUNC(ret_rec.Amount,2) THEN
               -- округление до ближайших бОльших копеек
               ret_rec.Amount := TRUNC(ret_rec.Amount,2) + 0.01;
            END IF;        
        END IF;                    

        ret_rec.BDR_Status := t_BDR_Status('OK');
                
    EXCEPTION -- не найден тарифный план
        WHEN no_data_found THEN

            NULL;  
                          
    END;     

    IF pr_Call.Data_Type = 'ZONE_FLT' -- тарификация несостоявшихся соединений. Суммы не нужны
    THEN
        ret_rec.Amount := 0;
        ret_rec.Bill_Minutes := 0;    
        ret_rec.BDR_Status := t_BDR_Status('OK'); -- все показываются как нормальные
    END IF;
    
    RETURN ret_rec;

END Trf_Zone;

-- p_Agent     0 - обычная тарификации, 1 - тарифкация агентских договоров
FUNCTION Trf_Cl_A_Table(pr_Call     ref_cdr,
                        p_Period_Id number,
                        p_Task_Id   number,
                        p_Agent     number DEFAULT 0
                       )
                       RETURN PIN.BDR_PH_COLL
                       PIPELINED PARALLEL_ENABLE (PARTITION pr_Call BY ANY)                             
IS

    c_prcName       CONSTANT varchar2(32) := 'Trf_Cl_A_Table'; 
      
    l_Counter       PLS_INTEGER;

    l_cur           rec_cdr; --pr_Call%ROWTYPE;
    ret_rec         PIN.BDR_PH_TYPE := PIN.BDR_PH_TYPE(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
                                                      );

    l_Day           date := TO_DATE('01.01.2000','dd.mm.yyyy');   
    l_Result        number;
    l_Smeta         number;
    l_Sw_Name       varchar2(128);
    
    -- для хранения уже найденных счетов
    lt_Bill t_Num;  
    
BEGIN
    
    l_Counter := 0;
    
    -- загрузка данных для тарификации в массивы
    Load_Trf_Data;    
    
    FETCH pr_Call INTO l_cur;
    
    LOOP
        EXIT WHEN pr_Call%NOTFOUND;    
    
        ret_rec := PIN.BDR_PH_TYPE(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                   NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                   NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                   NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
        
        IF l_cur.data_type IN ('MMTS','ZONE',
                               'AG_MMTS','AG_ZONE' -- добавлено 03.09.2015
                              ) 
        THEN
        
            -- определяем схему расчета договора (TTK_Smeta или по клиентским тарифы)
            BEGIN
                
                /*SELECT r.ratesystem_id
                  INTO l_Smeta
                  FROM order_t o,
                       rateplan_t r,
                       (SELECT key_id, KEY
                          FROM dictionary_t
                         CONNECT BY PRIOR key_id = parent_id
                         START WITH KEY = 'RATESYSTEM'
                       ) d
                 WHERE o.order_id = l_cur.order_id
                   AND DECODE(p_Agent, 1, o.agent_rateplan_id, o.rateplan_id) = r.rateplan_id
                   AND r.ratesystem_id = d.key_id
                   AND d.KEY = 'CL_OPR';*/
                  
                IF p_Agent = 1 THEN -- расчет агентских договоров
                    l_Smeta := gt_Ag_Order_Smeta(l_cur.order_id);
                ELSE
                    l_Smeta := gt_Order_Smeta(l_cur.order_id);
                END IF;    
                 
            EXCEPTION
                WHEN no_data_found THEN

                    l_Smeta := 0; -- флаг расчета по обычной (tariff_ph) схеме 
                
            END;    
        
        ELSE
            l_Smeta := 0;
        END IF;      
        
        IF l_cur.data_type IN ('MMTS','ZONE',
                               'AG_MMTS','AG_ZONE' -- добавлено 03.09.2015
                              ) 
           AND l_Smeta = pk00_const.с_RATESYS_CL_OPR_ID -- 1205
        THEN
            ret_rec := Trf_AG_MMTS(l_cur);   
            -- присваиваем полученный ratesystem_id, т.к. при тарификации по Смете в cхему pin смотреть не за чем      
            ret_rec.ratesystem_id := l_Smeta;
        ELSIF (
                (l_cur.data_type IN ('MMTS','AG_MMTS') -- AG_MMTS добавлено 03.09.2015
                    AND NVL(l_Smeta,-1) != pk00_const.с_RATESYS_CL_OPR_ID
                )
                  OR                
                 l_cur.data_type = 'MMTS_FLT'
              )    
        THEN
         
            ret_rec := Trf_MMTS(l_cur, p_Agent);
            
        ELSIF (
               (l_cur.data_type IN ('ZONE','AG_ZONE') -- AG_ZONE добавлено 03.09.2015 
                    AND NVL(l_Smeta,-1) != pk00_const.с_RATESYS_CL_OPR_ID
               )
                  OR                
               l_cur.data_type = 'ZONE_FLT'
              )                
        THEN
        
            ret_rec := Trf_Zone(l_cur, p_Agent);
            
        ELSIF l_cur.data_type IN ('SPB','SPBCLN') THEN
        
           -- получаем тип соединения
            l_Result := Get_SPb_SubService_Id(pin.Norm_Ph_Number('SPB',l_cur.Abn_A),
                                              pin.Norm_Ph_Number('SPB',NVL(l_cur.Called_Num,l_cur.Abn_B))
                                             );
                                              
            IF l_Result IN (pin.pk00_const.c_SUBSRV_ZONE, pin.pk00_const.c_SUBSRV_LOCAL) THEN -- вызов зоновый
                ret_rec := Trf_Zone(l_cur, p_Agent);
            ELSE
                ret_rec := Trf_MMTS(l_cur, p_Agent);
                
            END IF;      

        ELSE
           -- возвращем основные данные звонка с ошибкой тарифкации 
            ret_rec.row_id          := ROWIDTOCHAR(l_cur.row_id);
            ret_rec.CDR_Service_Id  := l_cur.Service_Id;
            ret_rec.Cdr_Id          := l_cur.cdr_id;
            ret_rec.start_time      := l_cur.Bill_Date - l_cur.UTC_Offset; 
            ret_rec.UTC_Offset      := l_cur.UTC_Offset; -- IntDayToSec_to_UTCOffset()
            ret_rec.Bill_Date       := l_cur.Bill_Date;
            ret_rec.Order_Id        := l_cur.order_id;
            ret_rec.Order_Ph        := l_cur.order_ph;  -- номер телефона, по которому был определён л/счет      
            ret_rec.Duration        := l_cur.duration;            
            ret_rec.Abn_A           := l_cur.Abn_A;
            ret_rec.Abn_B           := l_cur.Abn_B;
                                            
            -- ошибка -23 - не определён спосб тарификации
            ret_rec.bdr_status      := t_BDR_Status('MT_NF');
                             
        END IF;    


       -- IF l_cur.data_type IN ('MMTS', 'MMTS_FLT') THEN

        IF ret_rec.Account_Id IS NULL OR ret_rec.Order_No IS NULL THEN
            
            BEGIN
                SELECT o.account_id, o.order_no
                  INTO ret_rec.Account_Id, ret_rec.order_no
                  FROM order_t o
                 WHERE o.order_id = ret_rec.order_id; 
                
            EXCEPTION
                WHEN no_data_found THEN
                    ret_rec.bdr_status := t_BDR_Status('ACC_NF');
            END;    
            
        END IF;
        
       -- получаем кода завершения по справочнику
        BEGIN
               
            IF l_cur.termination_code IS NULL THEN
                RAISE no_data_found;
            ELSE    
                ret_rec.q805_code    := t_q805_Name(l_cur.termination_code).key_id;
                ret_rec.q805_name    := t_q805_Name(l_cur.termination_code).NAME;
            END IF;
                    
        EXCEPTION    
            WHEN no_data_found THEN
                ret_rec.q805_code := 16;
                ret_rec.q805_name := 'NORMAL CALL CLEARING';
        END;

        ret_rec.term_reason_code := l_cur.terminating_reason;
            
        BEGIN            
                
            IF ret_rec.term_reason_code IS NULL OR l_cur.sw_name IS NULL
            THEN
                RAISE no_data_found;
            ELSE
                --ret_rec.term_reason_name := t_Reason_Name(ret_rec.term_reason_code);
                -- получаем имя коммутатора по справочнику
                l_Sw_Name := t_Switch(l_cur.sw_name);
                    
                -- получаем код завершения вызова
                ret_rec.term_reason_name := t_Term_Reason(l_Sw_Name)(ret_rec.term_reason_code);
                
                -- индивидуальная замена для "Интренет Технологии" по письму Сорокина от 06.10.2015 
                IF ret_rec.account_id = 2301691 THEN
                
                    IF ret_rec.term_reason_name IN ('CALLER_PARTY_RELEASE','PEER_CALLER_RELEASE') THEN
                   
                        ret_rec.term_reason_name := 'CALLER_RELEASE';   
                
                    ELSIF ret_rec.term_reason_name IN ('CALLED_PARTY_RELEASE','PEER_CALLED_RELEASE') THEN
                   
                        ret_rec.term_reason_name := 'CALLED_RELEASE';
                   
                    END IF;
                   
                END IF;   
                     
            END IF;
                          
        EXCEPTION    
            WHEN no_data_found THEN
                ret_rec.term_reason_name := NULL;
        END;

     --   END IF;
        IF NVL(p_Agent,0) != 1 THEN -- для агентских расчетов счета не привязываются 
         
            IF l_cur.bill_id IS NULL AND ret_rec.Order_Id > 0 THEN
               -- если доход, то возвращаем bill_id
                BEGIN
                    -- ищем в уже сохраненных
                    ret_rec.bill_id := lt_Bill(ret_rec.Order_Id);
                EXCEPTION
                    WHEN no_data_found THEN                
                       -- если bill_id не задан, то получаем 
                        ret_rec.bill_id := pk114_items.Get_Bill_Id(p_Order_Id  => ret_rec.order_id,
                                                                   p_Period_Id => p_Period_Id,
                                                                   p_Job_Id    => p_Task_Id
                                                                  );
                        lt_Bill(ret_rec.Order_Id) := ret_rec.bill_id;                                                                  
                END;                                                   
            ELSE                                       
                -- если запись протарифцирована успешно, но счет, куда её слудет положить, не найден, то ставим ошибку
                -- поиска счета. В любом другом слуае остается статус, полученный в резта тарификации (ошибка или ОК)
                ret_rec.bill_id := l_cur.bill_id; 
                
            END IF;
                        
            IF ret_rec.order_body_id IS NULL AND 
               ret_rec.bdr_status = t_BDR_Status('OK') -- только для успешно протарифицированных записей
            THEN
            
                BEGIN
                
                    SELECT b.order_body_id
                      INTO ret_rec.order_body_id
                      FROM order_body_t b
                     WHERE b.order_id = ret_rec.order_id
                       AND b.charge_type = pk00_const.c_CHARGE_TYPE_USG
                       AND b.subservice_id = ret_rec.subservice_id
                       AND ret_rec.bill_date BETWEEN b.date_from AND b.date_to; 
                
                EXCEPTION
                    WHEN no_data_found THEN
                             
                       -- Есть данные в order_body_t, но нет для искомого периода. 
                       -- Ставим ошибку поиска order_body_id
                        ret_rec.bdr_status := t_BDR_Status('OB_ID_ERR');
   
                END;
                    
            END IF;        
            
        END IF;                      
        
        IF NVL(p_Agent,0) != 1 OR ret_rec.BDR_Status != t_BDR_Status('ORD_TP_NF')
        THEN
          -- если ТП при попытке расчета агентского договра не найден, то запись не возвращаем. 
          -- Считаем просто что нет такого агентского договора.                
            PIPE ROW (ret_rec); -- возвращаем протарифицированную запись
        END IF;

        l_Counter := l_Counter + 1;
        
        IF MOD(l_Counter, 100) = 0 THEN
            DBMS_APPLICATION_INFO.SET_ACTION(TO_CHAR(l_cur.Bill_Date,'dd.mm.yyyy') || ' SCAN ROWS: ' || TO_CHAR(l_Counter));
        END IF;
                
        FETCH pr_Call INTO l_cur;
        
    END LOOP;    

END Trf_Cl_A_Table;

-- Загрузка данных для тарифицкации в массивы
PROCEDURE Load_Trf_Data
IS
BEGIN

  -- Инициализация массива с праздниками
    gt_Calendar.DELETE;
    
    FOR l_cur IN (SELECT c.calendar_date, c.date_type_id
                    FROM calendar_t c
                 )     
    LOOP
        gt_Calendar(TO_CHAR(l_cur.calendar_date,'dd.mm.yyyy')) := l_cur.date_type_id; 
    END LOOP;

   -- инициализация тарифов с индивидуальным льготным временем
    gt_TrfPrefTime.DELETE;
    
    FOR l_cur IN (SELECT h.trf_id, h.BT_MG_FROM, h.BT_MN_FROM, h.BT_MG_TO, h.BT_MN_TO
                    FROM tariff_ph.D41_TRF_HEADER h
                   WHERE h.is_tm_not_std = 'Y'
                 )
    LOOP
        gt_TrfPrefTime(l_cur.trf_id).bt_mg_from := l_cur.bt_mg_from;
        gt_TrfPrefTime(l_cur.trf_id).bt_mg_to   := l_cur.bt_mg_to;
        gt_TrfPrefTime(l_cur.trf_id).bt_mn_from := l_cur.bt_mn_from;
        gt_TrfPrefTime(l_cur.trf_id).bt_mn_to   := l_cur.bt_mn_to;
    END LOOP;                       

   -- рабочее время для зоновых моделей МН-соединений
    FOR l_cur IN ( SELECT f.z_id, f.rn, f.time_from, t.time_to
                      FROM ( 
                            SELECT vf.z_id, 
                                   vf.VALUE time_from,
                                   row_number() OVER (PARTITION BY vf.z_id ORDER BY vf.VALUE) rn  
                              FROM tariff_ph.D04_ZONE_INIT_PARAM_VAL vf,
                                   tariff_ph.d05_dct_param pf
                             WHERE pf.par_name = 'BT_MN_FROM' --DECODE(p_Call_Type, 7, 'BT_MG_FROM', 8, 'BT_MN_FROM', NULL)  
                               AND vf.par_id = pf.par_id
                           ) f,    
                           (    
                            SELECT vt.z_id, 
                                   vt.VALUE time_to,
                                   row_number() OVER (PARTITION BY vt.z_id ORDER BY vt.VALUE) rn
                              FROM tariff_ph.D04_ZONE_INIT_PARAM_VAL vt,
                                   tariff_ph.d05_dct_param pt
                             WHERE pt.par_name = 'BT_MN_TO' --DECODE(p_Call_Type, 7, 'BT_MG_TO', 8, 'BT_MN_TO', NULL)
                               AND vt.par_id = pt.par_id
                           ) t   
                     WHERE f.z_id = t.z_id
                       AND f.rn = t.rn
                       AND f.time_from IS NOT NULL
                       AND t.time_to   IS NOT NULL
                 )
     
    LOOP
    
        IF l_cur.rn = 1 THEN
            gt_Mn_Bis_Time(l_cur.z_id) := t_Bis_Time();
        END IF;     
        gt_Mn_Bis_Time(l_cur.z_id).EXTEND;
        gt_Mn_Bis_Time(l_cur.z_id)(l_cur.rn).time_from := TO_DSINTERVAL(l_cur.time_from);
        gt_Mn_Bis_Time(l_cur.z_id)(l_cur.rn).time_to := TO_DSINTERVAL(l_cur.time_to);
    END LOOP;                           

   -- рабочее время для зоновых моделей МГ-соединений
    FOR l_cur IN ( SELECT f.z_id, f.rn, f.time_from, t.time_to
                      FROM ( 
                            SELECT vf.z_id, 
                                   vf.VALUE time_from,
                                   row_number() OVER (PARTITION BY vf.z_id ORDER BY vf.VALUE) rn  
                              FROM tariff_ph.D04_ZONE_INIT_PARAM_VAL vf,
                                   tariff_ph.d05_dct_param pf
                             WHERE pf.par_name = 'BT_MG_FROM' --DECODE(p_Call_Type, 7, 'BT_MG_FROM', 8, 'BT_MN_FROM', NULL)  
                               AND vf.par_id = pf.par_id
                           ) f,    
                           (    
                            SELECT vt.z_id, 
                                   vt.VALUE time_to,
                                   row_number() OVER (PARTITION BY vt.z_id ORDER BY vt.VALUE) rn
                              FROM tariff_ph.D04_ZONE_INIT_PARAM_VAL vt,
                                   tariff_ph.d05_dct_param pt
                             WHERE pt.par_name = 'BT_MG_TO' --DECODE(p_Call_Type, 7, 'BT_MG_TO', 8, 'BT_MN_TO', NULL)
                               AND vt.par_id = pt.par_id
                           ) t   
                     WHERE f.z_id = t.z_id
                       AND f.rn = t.rn
                       AND f.time_from IS NOT NULL
                       AND t.time_to   IS NOT NULL
                 )
     
    LOOP
    
        IF l_cur.rn = 1 THEN
            gt_Mg_Bis_Time(l_cur.z_id) := t_Bis_Time();
        END IF;     
        gt_Mg_Bis_Time(l_cur.z_id).EXTEND;
        gt_Mg_Bis_Time(l_cur.z_id)(l_cur.rn).time_from := TO_DSINTERVAL(l_cur.time_from);
        gt_Mg_Bis_Time(l_cur.z_id)(l_cur.rn).time_to := TO_DSINTERVAL(l_cur.time_to);
    END LOOP;                           


  -- Инициализация массива с направлениями
    gt_Zone.DELETE;
  
    FOR l_cur IN (SELECT prefix, z_id, z_name, z_id_parent, city_fz, mg_mn, ph_type, h_id   
                    FROM ( 
                          SELECT z.z_id, z.mg_mn, d.prefix, z.z_name, z.z_id_parent, z.city_fz, d.ph_type,
                                 (CASE WHEN d.ph_type = 'A' THEN d.abc_h_id
                                       WHEN d.ph_type = 'D' THEN d.def_h_id
                                  END) h_id,      
                                 row_number() OVER (PARTITION BY prefix ORDER BY z.z_id_parent ASC NULLS FIRST) rn     
                            FROM tariff_ph.d01_Zone z,
                                 (SELECT abc_h_id, NULL def_h_id, prefix, 'A' ph_type  
                                    FROM tariff_ph.d02_zone_abc a
                                   WHERE a.prefix IS NOT NULL
                                  UNION ALL
                                  SELECT NULL abc_h_id, def_h_id, prefix, 'D' ph_type  
                                    FROM tariff_ph.d03_zone_def d
                                  WHERE d.prefix IS NOT NULL
                                 ) d                   
                          WHERE z.abc_h_id = d.abc_h_id
                             OR (z.def_h_id = d.def_h_id AND z.z_type = 1) -- def-ы принадлежат только области
                         )
                  WHERE rn = 1
                 ) 
    LOOP
        gt_Zone(l_cur.prefix).z_id        := l_cur.z_id; 
        gt_Zone(l_cur.prefix).z_name      := l_cur.z_name;
        gt_Zone(l_cur.prefix).z_id_parent := l_cur.z_id_parent; 
        gt_Zone(l_cur.prefix).city_fz     := l_cur.city_fz;
        gt_Zone(l_cur.prefix).mg_mn       := l_cur.mg_mn; 
        gt_Zone(l_cur.prefix).ph_type     := l_cur.ph_type; 
        gt_Zone(l_cur.prefix).h_id        := l_cur.h_id;
    
    END LOOP;

   -- инициализация массива с тарифами
    gt_Tariff.DELETE;
    
    FOR l_cur IN (SELECT h.code, h.trf_id, h.round_v_id, h.unpaid_seconds, h.zmdl_id, h.is_8800_MP
                    FROM tariff_ph.d41_trf_header h
                 )
    LOOP
    
        gt_Tariff(l_cur.code).trf_id         := l_cur.trf_id;
        gt_Tariff(l_cur.code).round_v_id     := l_cur.round_v_id;
        gt_Tariff(l_cur.code).unpaid_seconds := l_cur.unpaid_seconds;
        gt_Tariff(l_cur.code).zmdl_id        := l_cur.zmdl_id;
        gt_Tariff(l_cur.code).is_8800_MP     := l_cur.is_8800_MP;
    
    END LOOP;                 

  -- массив с правилами округления
    gt_Rounding.DELETE;
    
    FOR l_cur IN (SELECT round_v_id, sec_base
                    FROM tariff_ph.DCT03_ROUND_V r)
    LOOP
        gt_Rounding(l_cur.round_v_id) := l_cur.sec_base;
    END LOOP;
    
   -- список заказов с ТП, которые считаются по TTK_SMETA
    gt_Order_Smeta.DELETE;
    
    FOR l_cur IN (SELECT o.order_id, r.ratesystem_id
                    FROM order_t o,
                         rateplan_t r,
                         (SELECT key_id, KEY
                            FROM dictionary_t
                           CONNECT BY PRIOR key_id = parent_id
                            START WITH KEY = 'RATESYSTEM'
                         ) d
                   WHERE o.rateplan_id = r.rateplan_id
                     AND r.ratesystem_id = d.key_id
                     AND d.KEY = 'CL_OPR'
                  )
    LOOP                     
        gt_Order_Smeta(l_cur.order_id) := l_cur.ratesystem_id;
    END LOOP;
               
   -- список заказов с агентскими ТП, которые считаются по TTK_SMETA
    gt_Ag_Order_Smeta.DELETE;
    
    FOR l_cur IN (SELECT o.order_id, r.ratesystem_id
                    FROM order_t o,
                         rateplan_t r,
                         (SELECT key_id, KEY
                            FROM dictionary_t
                          CONNECT BY PRIOR key_id = parent_id
                           START WITH KEY = 'RATESYSTEM'
                         ) d
                   WHERE o.agent_rateplan_id = r.rateplan_id
                     AND r.ratesystem_id = d.key_id
                     AND d.KEY = 'CL_OPR'
                 )
    LOOP
        gt_Ag_Order_Smeta(l_cur.order_id) := l_cur.ratesystem_id;
    END LOOP;                                                  
    
END Load_Trf_Data;


BEGIN

   -- инициализация массива с типами сетей
    FOR l_cur IN (SELECT b.bdr_code, n.network_code
                    FROM PIN.NETWORK_T n,
                         PIN.BDR_TYPES_T b 
                   WHERE b.network_id = n.network_id
                 )
    LOOP
        t_Network_Code(l_cur.bdr_code) := l_cur.network_code; 
    END LOOP;        

    -- инициализация массива с ошибками BDR-ов
    FOR l_cur IN (SELECT d.KEY, d.external_id
                    FROM PIN.DICTIONARY_T d
                   WHERE LEVEL = 2
                 CONNECT BY PRIOR d.key_id = d.parent_id
                   START WITH d.KEY = 'TRF_STAT'
                 )
    LOOP
        t_BDR_Status(l_cur.KEY) := l_cur.external_id; 
    END LOOP;        

    -- инициализация массива с услугами
    FOR l_cur IN (SELECT s.subservice_key, s.subservice_id
                    FROM PIN.SUBSERVICE_T s
                 )
    LOOP
        t_Subservice(l_cur.subservice_key) := l_cur.subservice_id; 
    END LOOP;        

    -- инициализация массива с коммутаторами
    FOR l_cur IN (SELECT switch_code, switch_type
                    FROM SWITCH_T 
                 )
    LOOP
        t_Switch(l_cur.switch_code) := l_cur.switch_type; 
    END LOOP;        

    -- инициализация массива с именами termination_reason
    FOR l_cur IN (/*SELECT KEY, NAME REASON 
                    FROM DICTIONARY_T 
                   WHERE PARENT_ID = 72*/
                   SELECT switch_type, code, term_reason 
                     FROM SW_TERM_REASON_T   
                 )
    LOOP
       -- t_Reason_Name(l_cur.KEY) := l_cur.reason;
        t_Term_Reason(l_cur.switch_type)(TO_CHAR(l_cur.code)) := l_cur.term_reason;         
    END LOOP;        

    -- инициализация массива с именами termination_code
    FOR l_cur IN (SELECT T.KEY, Q.KEY Q805_CODE, Q.NAME Q805_NAME 
                    FROM DICTIONARY_T T, DICTIONARY_T Q 
                   WHERE T.PARENT_ID = 74 -- SOFTX300 TERMINATION_CODE 
                     AND Q.PARENT_ID = 73 -- Q.805 - TERMINATION_CODE 
                     AND Q.KEY = T.EXTERNAL_ID
                 )
    LOOP
        t_q805_Name(l_cur.KEY).key_id := l_cur.q805_code; 
        t_q805_Name(l_cur.KEY).NAME := l_cur.q805_name;
    END LOOP;                      

   -- Загрузка данных для тарифцикации в массивы
    Load_Trf_Data;

END PK111_TARIFFING;
/
