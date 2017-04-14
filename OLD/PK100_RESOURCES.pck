CREATE OR REPLACE PACKAGE PK100_RESOURCES
IS

    PROCEDURE Load_MGMN_RatePlans;

    PROCEDURE Load_BindInfo_PH;

    -- Загрузка всех изменений по МН зонам России в префиксе 7 в таблицу аудита (MN_ZONE_7_AUDIT)
    -- Изменения выбираются с момента последней загрузки
    PROCEDURE Get_MN_Zone_Diff;

    PROCEDURE ReLoad_Orders_Full;
    
    PROCEDURE Load_Orders;    
    
    -- Загрузка заказов с услугой зоновой телефонной связи
    PROCEDURE Load_Orders_Zone;
    
    -- Процедура для добавления новых (которые появились в новом биллинге и нет в трешере)
    -- лицевых счетов
    PROCEDURE Add_Accounts;    

    -- для тарификации - загрузка и обработка номерных емкостей из схемы X07
    PROCEDURE Load_Oper_Phones;

END PK100_RESOURCES;
/
CREATE OR REPLACE PACKAGE BODY PK100_RESOURCES
IS

    gc_Service_Type CONSTANT varchar2(32) := '/service/telco/gsm/telephony';
    gc_SrvFree_Type CONSTANT varchar2(50) := '/service/telco/gsm/telephony/freecall';
    gc_SrvZone_Type CONSTANT varchar2(50) := '/service/telco/gsm/telephony/zone';    
    
    gc_Event_Type   CONSTANT varchar2(32) := '/event/delayed/session/telco/gsm';

    gc_MinDate CONSTANT date := TO_DATE('01.01.2000','dd.mm.yyyy');
    gc_MaxDate CONSTANT date := TO_DATE('01.01.2050','dd.mm.yyyy'); 
    c_PkgName  CONSTANT varchar2(36) := 'PK100_RESOURCES';

   -- Возможные статусы записей в службах (service_t)
    gc_Active    CONSTANT number := 10100;
    gc_NotActive CONSTANT number := 10102;
    gc_Closed    CONSTANT number := 10103;
    
    -- В таблицах аудита признак удаленных и вставленных записей
    gc_Insert    CONSTANT number := 1;
    gc_Delete    CONSTANT number := -1;

    -- Подпись для имен тарифных планов, которые импортированы из MMTDB
    gc_RatePlan_Sign CONSTANT varchar2(20) := 'export from MMTDB';

PROCEDURE Load_MGMN_RatePlans
IS

    c_prcName CONSTANT varchar2(32) := 'Load_MGMN_RatePlans';

    l_Curr_Date date;
    l_Ins_Cnt   number;
    l_Upd_Cnt   number;
    l_Del_Cnt   number;
    
BEGIN

    l_Curr_Date := SYSDATE;

    Pk01_Syslog.write_Msg( p_Msg => 'Start',    
                           p_Src => c_PkgName||'.'||c_prcName );
                              
    DELETE FROM TMP_RATEPLAN_T; 

    -- загрузка индивидуальных тарифных планов
    INSERT INTO TMP_RATEPLAN_T
        (order_id, rateplan_name, rateplan_code)
    SELECT order_id, NAME, code
      FROM (
            SELECT pino.order_id, rp.NAME, rp.code, -- RP434044D6
                   row_number() OVER (PARTITION BY pino.order_id ORDER BY pin.u2d@mmtdb(pae.valid_to) DESC) rn
              FROM pin.account_t@mmtdb a, 
                   pin.profile_t@mmtdb p, 
                   pin.profile_acct_extrating_data_t@mmtdb pae,
                   integrate.ifw_rateplan@mmtdb rp,
                   pin.account_t pina,
                   pin.order_t pino,
                   pin.service_t pins
             WHERE p.NAME = 'RATEPLAN'
               AND pae.obj_id0 = p.poid_id0
               AND p.account_obj_id0 = a.poid_id0
               AND rp.status = 'A'
               AND rp.code = pae.VALUE   
               AND a.account_no = pina.account_no
               AND pins.service_code = 'MGMN'
               AND pino.service_id = pins.service_id       
               AND pina.account_id = pino.account_id   
           )    
     WHERE rn = 1;

    Pk01_Syslog.write_Msg( p_Msg => 'Debug: Ind. TP is loaded (' || TO_CHAR(SQL%ROWCOUNT) || ')',    
                           p_Src => c_PkgName||'.'||c_prcName );

    -- загрузка общих тарифных планов
    INSERT INTO TMP_RATEPLAN_T
          (order_id, rateplan_name, rateplan_code)
    SELECT order_id, NAME, code
      FROM (
            SELECT -- COUNT(1) -- 248272
                    order_id, rp.NAME, rp.code,
                    row_number() OVER (PARTITION BY order_id ORDER BY NVL(pin.U2D@mmtdb(pp.usage_end_t), gc_MaxDate) DESC) rn
              FROM  pin.account_t@mmtdb a,
                    pin.purchased_product_t@mmtdb pp, 
                    pin.product_usage_map_t@mmtdb pum,
                    pin.service_t@mmtdb s,
                    integrate.ifw_rateplan@mmtdb rp, -- описатель rateplan связка с PIN.PRODUCT_USAGE_MAP_T
                    pin.account_t pina,
                    pin.order_t pino,
                    pin.service_t pins
            WHERE 1=1
              AND PP.ACCOUNT_OBJ_ID0 = A.POID_ID0 
              --and P.POID_ID0  = PP.PRODUCT_OBJ_ID0 
              AND PUM.OBJ_ID0 = PP.PRODUCT_OBJ_ID0
              AND PUM.EVENT_TYPE = '/event/delayed/session/telco/gsm'
              AND s.poid_type = gc_Service_Type
              AND S.POID_ID0 = PP.SERVICE_OBJ_ID0
              AND rp.status(+) = 'A'
              AND rp.code(+) = pum.rate_plan_name
              AND a.account_no = pina.account_no
              AND pins.service_code = 'MGMN'
              AND pino.service_id = pins.service_id       
              AND pina.account_id = pino.account_id
              AND NOT EXISTS (SELECT 1
                                FROM tmp_rateplan_t t
                               WHERE t.order_id = pino.order_id 
                             )                           
           )
     WHERE rn = 1;          

    Pk01_Syslog.write_Msg( p_Msg => 'Debug: General TP is loaded (' || TO_CHAR(SQL%ROWCOUNT) || ')',    
                           p_Src => c_PkgName||'.'||c_prcName );

    COMMIT;

    -- проставляем идентификаторы ТП, которые уже есть
    UPDATE PIN.TMP_RATEPLAN_T a
       SET a.rateplan_id = (SELECT r.rateplan_id
                              FROM PIN.RATEPLAN_T r
                             WHERE /*r.rateplan_name = a.rateplan_name
                               AND*/ r.rateplan_code = a.rateplan_code
                           );  

    Pk01_Syslog.write_Msg( p_Msg => 'Debug: ID of exists TP is updated (' || TO_CHAR(SQL%ROWCOUNT) || ')',    
                           p_Src => c_PkgName||'.'||c_prcName );
    
    -- добавляем новые тарифные планы в справочник    
    INSERT INTO rateplan_t
           (rateplan_id, rateplan_name, rateplan_code, note) 
    SELECT SQ_RATEPLAN_ID.NEXTVAL, rateplan_name, rateplan_code, gc_RatePlan_Sign
      FROM (SELECT rateplan_name, rateplan_code
              FROM PIN.TMP_RATEPLAN_T  
             WHERE rateplan_id IS NULL
             GROUP BY rateplan_name, rateplan_code
           );
    
    l_Ins_Cnt := SQL%ROWCOUNT;    

    Pk01_Syslog.write_Msg( p_Msg => 'Debug: New TP is inserted (' || TO_CHAR(l_Ins_Cnt) || ')',    
                           p_Src => c_PkgName||'.'||c_prcName );
    
    -- проставляем идентификаторы новым ТП, которые были добавлены
    UPDATE PIN.TMP_RATEPLAN_T a
       SET a.rateplan_id = (SELECT r.rateplan_id
                              FROM PIN.RATEPLAN_T r
                             WHERE r.note = gc_RatePlan_Sign
                               AND /*r.rateplan_name = a.rateplan_name
                               AND*/ r.rateplan_code = a.rateplan_code
                           )
     WHERE a.rateplan_id IS NULL;        
    
    Pk01_Syslog.write_Msg( p_Msg => 'Debug: ID of new TP is updated (' || TO_CHAR(SQL%ROWCOUNT) || ')',    
                           p_Src => c_PkgName||'.'||c_prcName );    
    
    -- удаляем ТП, которые больше не привязаны 
    DELETE FROM PIN.RATEPLAN_T p
     WHERE p.note = gc_RatePlan_Sign 
       AND NOT EXISTS (SELECT 1
                         FROM PIN.TMP_RATEPLAN_T t 
                        WHERE t.rateplan_id   = p.rateplan_id
                      );

    l_Del_Cnt := SQL%ROWCOUNT;    

    Pk01_Syslog.write_Msg( p_Msg => 'Debug: Old TP is deleted (' || TO_CHAR(l_Del_Cnt) || ')',    
                           p_Src => c_PkgName||'.'||c_prcName );    
    
    -- обновляем данные по тарифным планам у заказов
    UPDATE order_t o
       SET o.rateplan_id = (SELECT t.rateplan_id      
                              FROM tmp_rateplan_t t
                             WHERE t.order_id = o.order_id
                           ),  
           o.notes = NVL2(o.rateplan_id, 'Старый ТП: ' || o.rateplan_id, NULL)
     WHERE EXISTS (SELECT 1     
                     FROM tmp_rateplan_t t
                    WHERE t.order_id = o.order_id  
                      AND NVL(t.rateplan_id, -1) != NVL(o.rateplan_id, -1)
                  );     
      
    l_Upd_Cnt := SQL%ROWCOUNT;  
      
    Pk01_Syslog.write_Msg(p_Msg => 'удлаено ТП: ' || TO_CHAR(l_Del_Cnt) || ', ' ||
                                   'добавлено ТП: ' || TO_CHAR(l_Ins_Cnt) || ', ' ||
                                   'обновлено заказов: ' || TO_CHAR(l_Upd_Cnt),     
                          p_Src => c_PkgName||'.'||c_prcName );           
           
    COMMIT;

END Load_MGMN_RatePlans;


PROCEDURE Load_BindInfo_PH_OLd
IS
    c_prcName CONSTANT varchar2(32) := 'Load_BindInfo_PH'; 

    l_Curr_Date date;
    l_Count     PLS_INTEGER;


BEGIN

    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    l_Curr_Date := SYSDATE;

    Pk01_Syslog.write_Msg( p_Msg => 'Start',    
                           p_Src => c_PkgName||'.'||c_prcName );
                              

    -- очищаем временную таблицу
    DELETE FROM TMP_ORDER_PHONES_T;

    --   
    -- вносим все текущие данные 
    --
    -- 1. Текущие активные и не активные
    INSERT INTO TMP_ORDER_PHONES_T(
               order_id, phone_number, 
               date_from, 
               date_to,
               srv_poid_id0, srv_poid_type, srv_status,
               srv_revision, au_created_t, rec_source)
        SELECT pino.order_id, sal.NAME ph_num, 
               TRUNC(NVL(pin.u2d@mmtdb(s.effective_t), pin.u2d@mmtdb(s.created_t))) 
                 + DECODE(s.status, gc_Active, 0,
                                    gc_NotActive, 1-1/86400) date_from, 
               NULL date_to, 
               s.poid_id0, s.poid_type, s.status, 
               MAX(aus.au_parent_obj_rev) revision, gc_MaxDate, 1 SRC
               -- DECODE(a.business_type,1,'Ф',2,'Ю') 
          FROM pin.account_t@mmtdb a,
               pin.service_t@mmtdb s, 
               pin.service_alias_list_t@mmtdb sal,
               pin.au_service_t@mmtdb aus,
               pin.account_t pina,
               pin.order_t pino,
               pin.service_t pins
         WHERE s.poid_type = gc_Service_Type -- LIKE '/service/telco/gsm/telephony%'
           AND s.status IN (gc_Active, gc_NotActive)
           AND a.poid_id0 = s.account_obj_id0
           AND s.poid_id0 = sal.obj_id0          
           AND s.poid_id0 = aus.au_parent_obj_id0(+)   
           AND a.account_no = pina.account_no
           AND pins.service_code = 'MGMN'
           AND pino.service_id = pins.service_id       
           AND pina.account_id = pino.account_id             
         GROUP BY pino.order_id, sal.NAME, 
                  NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t)),
                  s.poid_id0, s.poid_type, s.status;
              
    l_Count := SQL%ROWCOUNT;               

    -- 2. Текущие закрытые
    INSERT INTO TMP_ORDER_PHONES_T(
               order_id, phone_number, 
               date_from, 
               date_to,
               srv_poid_id0, srv_poid_type, srv_status,
               srv_revision, au_created_t, rec_source)               
    SELECT pino.order_id, MIN(sal.NAME) ph_num, 
           TRUNC(NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t))) 
               + INTERVAL '0 23:59:59' DAY TO SECOND date_from, 
           NULL date_to,
           s.poid_id0, s.poid_type, s.status, 
           MAX(aus.au_parent_obj_rev) revision, gc_MaxDate, 1 SRC
           -- DECODE(a.business_type,1,'Ф',2,'Ю') 
      FROM pin.account_t@mmtdb a,
           pin.service_t@mmtdb s, 
           pin.au_service_alias_list_t@mmtdb sal,
           pin.au_service_t@mmtdb aus,
           pin.account_t pina,
           pin.order_t pino,
           pin.service_t pins
     WHERE s.poid_type = gc_Service_Type -- LIKE '/service/telco/gsm/telephony%' 
       AND s.status = gc_Closed
       AND a.poid_id0 = s.account_obj_id0
       AND aus.poid_id0 = sal.obj_id0          
       AND s.poid_id0 = aus.au_parent_obj_id0    
       AND a.account_no = pina.account_no
       AND pins.service_code = 'MGMN'
       AND pino.service_id = pins.service_id       
       AND pina.account_id = pino.account_id        
     GROUP BY pino.order_id, s.poid_id0, s.poid_type, s.status, 
              NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t));

    COMMIT;

    dbms_stats.gather_table_stats('PIN','TMP_ORDER_PHONES_T');

    ----          
    -- Добавляем всю историю из архивных таблиц для данных, загруженных на пред. шаге 
    --
    INSERT INTO TMP_ORDER_PHONES_T(
               order_id, phone_number, 
               date_from, date_to,
               srv_poid_id0, srv_poid_type, srv_status,
               srv_revision, au_srv_poid_id0, au_created_t, rec_source)           
    SELECT --+ use_hash(t)
           t.order_id,
           sal.NAME                                 ph_num,
           TRUNC(NVL(pin.u2d@mmtdb(s.effective_t),
                     pin.u2d@mmtdb(s.created_t)))   
               + DECODE(s.status, gc_Active, 0,
                                  gc_NotActive, 1-1/86400,
                                  gc_Closed, 1-1/86400
                                                  ) date_from,      
           NULL                                     date_to,
           s.au_parent_obj_id0                      srv_poid_id0,
           s.au_parent_obj_type                     srv_poid_type,
           s.status                                 srv_status,
           s.au_parent_obj_rev                      srv_revision,
           s.poid_id0                               au_srv_poid_id0,
           pin.u2d@mmtdb(s.created_t)               au_created_t,           
           2                                        rec_source
           --DECODE(a.business_type,1,'Ф',2,'Ю')      account_type
      FROM TMP_ORDER_PHONES_T t,
           pin.au_service_alias_list_t@mmtdb sal,
           pin.au_service_t@mmtdb s
     WHERE 1=1
       AND t.srv_poid_id0 = s.au_parent_obj_id0
       AND (
            (t.srv_revision != s.au_parent_obj_rev)
             OR
            (t.SRV_STATUS = gc_Closed AND t.srv_revision = s.au_parent_obj_rev AND t.SRV_STATUS != s.status )
           )            
       AND s.poid_id0 = sal.obj_id0;              
   
    l_Count := l_Count + SQL%ROWCOUNT;

    Pk01_Syslog.write_Msg(p_Msg => 'импортировано ' || TO_CHAR(l_Count) || ' записей', 
                          p_Src => c_PkgName||'.'||c_prcName );           

    COMMIT;

    -- Выравниваем диапазоны, чтобы не было пересечений. 
    -- Принцип: все данные делятся по группам 
    --  номер_телефона-тип_соединения (ph_num-srv_poid_type)-идентификатор(poid) записи из текущих данных . 
    -- Внутри группы записи имеют приоритет от высшего к низшему: 
    --     1. текущие данные по открытым номерам (тбл. service_t) (поле rec_source)
    --     2. данные по закрытым номерам по номерам версий (srv_revision) 
    --  внутри каждой из указанных групп по дате создания, т.е. чем свежее данные, тем выше приоритет  
    --  Берется запись с высшим приоритетом, у нее из даты открытия вычитается 1сек. 
    --  Это будет дата закрытия след., более ранней записи
    --  Может возникнуть такая ситуация, что дата окончания станет меньше даты начала. Такие записи  
    --  будут просто удалены.  
    MERGE INTO TMP_ORDER_PHONES_T tg
    USING (
            SELECT t.rd,
                   t.date_from, next_df,
                   NVL(t.next_df-1/86400, gc_MaxDate) new_date_to,
                   date_to, 
                   --next_status, 
                   --srv_status_to, 
                   srv_status, 
                   next_poid, 
                   --srv_poid_id0_to, 
                   srv_poid_id0, au_srv_poid_id0,    
                   rec_source, au_created_t, srv_revision
              FROM (        
                    SELECT r.phone_number, r.srv_poid_type,
                           lag(r.date_from) OVER (PARTITION BY r.phone_number, r.srv_poid_type, r.srv_poid_id0
                                                      ORDER BY r.rec_source, r.srv_revision DESC, r.au_created_t DESC, r.date_from DESC) next_df,
                           lag(r.srv_status) OVER (PARTITION BY r.phone_number, r.srv_poid_type, r.srv_poid_id0
                                                       ORDER BY r.rec_source, r.srv_revision DESC, r.au_created_t DESC, r.date_from DESC) next_status,                                      
                           lag(NVL(r.au_srv_poid_id0, r.srv_poid_id0)) 
                                             OVER (PARTITION BY r.phone_number, r.srv_poid_type, r.srv_poid_id0
                                                       ORDER BY r.rec_source, r.srv_revision DESC, r.au_created_t DESC, r.date_from DESC) next_poid,                                                  
                           r.date_from,
                           r.date_to,
                           r.ROWID rd,
                           r.srv_status, r.rec_source, r.au_created_t, r.srv_revision,
                           --r.srv_poid_id0_to, 
                           --r.srv_status_to,
                           r.srv_poid_id0, r.au_srv_poid_id0
                      FROM TMP_ORDER_PHONES_T r
                  --   WHERE r.ph_num = '73532744168' -- '73437461015' --'73425727922' --
                    ) t
              ORDER BY t.srv_poid_id0, t.rec_source, t.srv_revision DESC, t.au_created_t DESC, t.date_from, t.date_to   
          ) t 
      ON (tg.ROWID = t.rd)
    WHEN MATCHED THEN UPDATE 
    SET tg.date_to = t.new_date_to/*,
        tg.srv_poid_id0_to = t.next_poid,
        tg.srv_status_to = t.next_status*/;
    

    -- удаляем все диапазоны в которых номера закрыты или неактивны. 
    -- Они всё равно не принимают участие в привязках л/счетов
    DELETE FROM TMP_ORDER_PHONES_T
    WHERE srv_status != gc_Active;

    l_Count := l_Count - SQL%ROWCOUNT;

    -- удаляем записи, у которые дата закрытия стала меньше даты открытия
    DELETE FROM TMP_ORDER_PHONES_T
    WHERE date_from > date_to;

    l_Count := l_Count - SQL%ROWCOUNT;

    -- удаляем записи, у которых диапазон входит в другой или дата начала больше даты конца
    DELETE FROM TMP_ORDER_PHONES_T
    WHERE rowid IN (  
                SELECT t2.rowid rd
                  FROM TMP_ORDER_PHONES_T t1,
                       TMP_ORDER_PHONES_T t2
                 WHERE t1.rowid != t2.rowid
                   AND t1.srv_poid_type = t2.srv_poid_type 
                   AND t1.phone_number = t2.phone_number
                   AND t2.rec_source != 1
                   AND t1.rec_source <= t2.rec_source -- по приоритету при прочих равных остаются открытые
                   AND t1.au_created_t > t2.au_created_t -- и с наиболее свежей датой изменения
                   AND t1.date_from <= t2.date_from
                   AND t1.date_to >= t2.date_to
                 );     

    l_Count := l_Count - SQL%ROWCOUNT;
    
    -- Теперь выравниваем дипазоны группируя только по номер_телефона - тип_соединения
    MERGE INTO TMP_ORDER_PHONES_T tg
    USING (
           SELECT rd, new_date_to
                 FROM (
                        SELECT t.rd,
                               --t.date_from,
                               (CASE
                                   WHEN t.date_to >= t.next_df  -- если начало след. интервала раньше, чем заканчивается текущий
                                   THEN
                                      t.next_df - 1/86400 -- ставим текущей конец периода как дата начала следующего - 1 сек.
                                   ELSE
                                      t.date_to -- оставляем текущую дату конца.
                               END) new_date_to,
                               date_to
                          FROM (
                                SELECT --r.ph_num, r.srv_poid_type,
                                       lag(r.date_from) OVER (PARTITION BY r.phone_number, r.srv_poid_type 
                                                                  ORDER BY r.rec_source, r.au_created_t DESC, r.date_from DESC) next_df,
                                       r.date_from,
                                       r.date_to,
                                       r.ROWID rd
                                  FROM TMP_ORDER_PHONES_T r
                                 --WHERE r.ph_num = '73425727922'
                               ) t
                      ) tt
                WHERE new_date_to != date_to
         ) t       
      ON (tg.ROWID = t.rd)
    WHEN MATCHED THEN UPDATE 
    SET tg.date_to = t.new_date_to;    
    

    -- удаляем записи, у которые дата закрытия стала меньше даты открытия
    DELETE FROM TMP_ORDER_PHONES_T
    WHERE date_from > date_to;

    l_Count := l_Count - SQL%ROWCOUNT;

    -- удаляем записи, у которых диапазон входит в другой или дата начала больше даты конца
    DELETE FROM TMP_ORDER_PHONES_T
    WHERE rowid IN (  
                SELECT t2.rowid rd
                  FROM TMP_ORDER_PHONES_T t1,
                       TMP_ORDER_PHONES_T t2
                 WHERE t1.rowid != t2.rowid
                   AND t1.srv_poid_type = t2.srv_poid_type 
                   AND t1.phone_number = t2.phone_number
                   AND t2.rec_source != 1
                   AND t1.rec_source <= t2.rec_source -- по приоритету при прочих равных остаются открытые
                   AND t1.au_created_t > t2.au_created_t -- и с наиболее свежей датой изменения
                   AND t1.date_from <= t2.date_from
                   AND t1.date_to >= t2.date_to
                 );     

    l_Count := l_Count - SQL%ROWCOUNT;
        
    -- отметка об особых условиях у клиентов для тарификации соединений на 8800
    -- (запрос сделан c поиском через rowid, т.к. иначе не получалось добиться hash_sj в плане.
    --  оракл сваливался на nl и все начинало дико тормозить   
 /*   UPDATE TMP_ORDER_PHONES_T tt
       SET tt.a_number_orig = 1
     WHERE tt.rowid IN (
                   SELECT t.rowid
                     FROM pin.profile_t@mmtdb p, 
                          pin.profile_acct_extrating_data_t@mmtdb pae,
                          TMP_ORDER_PHONES_T t 
                    WHERE p.NAME = 'A_NUMBER_ZONE'
                      AND pae.VALUE = 'A_NUMBER_ORIG'
                      AND pae.obj_id0 = p.poid_id0
                      AND p.account_obj_id0 = t.account_id
                      AND t.date_from >= NVL(u2d(pae.valid_from),gc_MinDate)
                      AND (t.date_to <= NVL(u2d(pae.valid_to),gc_MaxDate) 
                              OR u2d(pae.valid_to) > TO_DATE('01.01.2030','dd.mm.yyyy hh24:mi:ss')) 
                   );    
*/

    Pk01_Syslog.write_Msg(p_Msg => 'Подготовлено к загрузке ' || TO_CHAR(l_Count) || ' записей', 
                          p_Src => c_PkgName||'.'||c_prcName );               

    -- удаляем устаревшие данные   
    DELETE FROM PIN.ORDER_PHONES_T p
     WHERE p.order_id NOT IN (SELECT o.order_id
                                FROM order_t o
                               WHERE o.account_id = Pk120_Bind_Clients.gc_Samara_Acc_Id)
       AND NOT EXISTS (SELECT 1
                         FROM PIN.TMP_ORDER_PHONES_T t 
                        WHERE t.order_id     = p.order_id
                          AND t.phone_number = p.phone_number 
                          AND t.date_from    = p.date_from 
                          AND t.date_to      = p.date_to
                      );

    l_Count := SQL%ROWCOUNT;

    INSERT INTO PIN.ORDER_PHONES_T (
           order_id, phone_number, date_from, date_to) 
    SELECT order_id, phone_number, date_from, date_to
      FROM PIN.TMP_ORDER_PHONES_T t
     WHERE NOT EXISTS (SELECT 1
                         FROM PIN.ORDER_PHONES_T p 
                        WHERE t.order_id     = p.order_id
                          AND t.phone_number = p.phone_number 
                          AND t.date_from    = p.date_from 
                          AND t.date_to      = p.date_to
                      ); 

    Pk01_Syslog.write_Msg(p_Msg => 'добавлено ' || TO_CHAR(SQL%ROWCOUNT) || ', удалено ' || TO_CHAR(l_Count) || ' записей, ' ||
                                   'итого: ' || TO_CHAR(SQL%ROWCOUNT+l_Count), 
                          p_Src => c_PkgName||'.'||c_prcName );           

    COMMIT;   
    
  --  EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_ORDER_PHONES_T';
    
END Load_BindInfo_PH_Old;

-- Загрузка всех изменений по МН зонам России в префиксе 7 в таблицу аудита (MN_ZONE_7_AUDIT)
-- Изменения выбираются с момента последней загрузки
PROCEDURE Get_MN_Zone_Diff
IS

    l_Calc_Date date := SYSDATE; 

BEGIN

    INSERT INTO mn_zone_7_audit
         (dn_code, direction, date_from, date_to, date_save, action)
    WITH aud AS (
        SELECT *
          FROM mn_zone_7_audit a
         WHERE a.action = 1  
           AND NOT EXISTS (SELECT 1
                             FROM mn_zone_7_audit d
                            WHERE d.action = -1
                              AND a.dn_code = d.dn_code
                              AND a.date_from = d.date_from
                              AND a.date_to = d.date_to
                              AND d.date_save >= a.date_save 
                           )
             )
    SELECT dn_code, direction, date_from, date_to, l_Calc_Date, gc_Delete 
      FROM (         
            SELECT dn_code, direction, date_from, date_to
              FROM aud
            MINUS
            SELECT dn_code, direction, date_from, date_to
              FROM mn_zone_7                    
           )   
    UNION ALL
    SELECT dn_code, direction, date_from, date_to, l_Calc_Date, gc_Insert
      FROM (         
            SELECT dn_code, direction, date_from, date_to
              FROM mn_zone_7
            MINUS
            SELECT dn_code, direction, date_from, date_to
              FROM aud                   
           );

    Pk01_Syslog.write_Msg(p_Msg => 'добавлено в аудит ' || TO_CHAR(SQL%ROWCOUNT), 
                          p_Src => c_PkgName||'.Get_MN_Zone_Diff' );           
           
    COMMIT;           
              
    
END Get_MN_Zone_Diff;



PROCEDURE Load_BindInfo_PH
IS
    c_prcName CONSTANT varchar2(32) := 'Load_BindInfo_PH'; 

    l_Curr_Date date;
    l_Count     PLS_INTEGER;


BEGIN

    l_Curr_Date := SYSDATE;

    Pk01_Syslog.write_Msg( p_Msg => 'Start',    
                           p_Src => c_PkgName||'.'||c_prcName );
                              
    -- очищаем временную таблицу
    DELETE FROM TMP_ORDER_PHONES_T;

    COMMIT;

 --   SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
    --   
    -- вносим все текущие данные 
    --
    -- 1. Текущие активные и не активные
    INSERT INTO TMP_ORDER_PHONES_T(
               order_id, phone_number, 
               date_from, 
               date_to,
               srv_poid_id0, srv_poid_type, srv_status,
               srv_revision, au_created_t, rec_source, 
               not_transform)
        SELECT pino.order_id, sal.NAME ph_num, 
               GREATEST(TRUNC(NVL(pin.u2d@mmtdb(s.effective_t), pin.u2d@mmtdb(s.created_t))) 
                         + DECODE(s.status, gc_Active, 0,
                                            gc_NotActive, 1-1/86400),
                        pino.date_from) date_from, 
               pino.date_to date_to, 
               s.poid_id0, s.poid_type, s.status, 
               MAX(aus.au_parent_obj_rev) revision, gc_MaxDate, 1 SRC,
               0 not_transform 
               -- DECODE(a.business_type,1,'Ф',2,'Ю') 
          FROM pin.order_t pino,
               pin.profile_contract_info_t@mmtdb pci,
               pin.profile_t@mmtdb p,
               pin.service_t@mmtdb s, 
               pin.service_alias_list_t@mmtdb sal,
               pin.au_service_t@mmtdb aus
         WHERE pino.order_no = pci.order_num
           AND pci.obj_id0 = p.poid_id0
           AND p.account_obj_id0 = s.account_obj_id0        
           AND s.poid_type IN (gc_Service_Type,gc_SrvFree_Type,gc_SrvZone_Type) -- LIKE '/service/telco/gsm/telephony%'
           AND s.status IN (gc_Active, gc_NotActive)
           AND s.poid_id0 = sal.obj_id0          
           AND s.poid_id0 = aus.au_parent_obj_id0(+)   
         GROUP BY pino.order_id, pino.date_from, pino.date_to, sal.NAME, 
                  NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t)),
                  s.poid_id0, s.poid_type, s.status;
              
    l_Count := SQL%ROWCOUNT;               

    Pk01_Syslog.write_Msg( p_Msg => 'Step1: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );

   -- аудит profile
    INSERT INTO TMP_ORDER_PHONES_T(
               order_id, phone_number, 
               date_from, 
               date_to,
               srv_poid_id0, srv_poid_type, srv_status,
               srv_revision, au_created_t, rec_source,
               not_transform)
        SELECT pino.order_id, sal.NAME ph_num, 
               GREATEST(TRUNC(NVL(pin.u2d@mmtdb(s.effective_t), pin.u2d@mmtdb(s.created_t))) 
                             + DECODE(s.status, gc_Active, 0,
                                                gc_NotActive, 1-1/86400),
                        pino.date_from) date_from, 
               pino.date_to date_to, 
               s.poid_id0, s.poid_type, s.status, 
               MAX(aus.au_parent_obj_rev) revision, gc_MaxDate, 1 SRC,
               0 not_transform
               -- DECODE(a.business_type,1,'Ф',2,'Ю') 
          FROM pin.order_t pino,
               pin.au_profile_contract_info_t@mmtdb pci,
               pin.au_profile_t@mmtdb p,
               pin.service_t@mmtdb s, 
               pin.service_alias_list_t@mmtdb sal,
               pin.au_service_t@mmtdb aus
         WHERE pino.order_no = pci.order_num
           AND pci.obj_id0 = p.poid_id0
           AND p.account_obj_id0 = s.account_obj_id0        
           AND s.poid_type IN (gc_Service_Type,gc_SrvFree_Type,gc_SrvZone_Type)
           AND s.status IN (gc_Active, gc_NotActive)
           AND s.poid_id0 = sal.obj_id0          
           AND s.poid_id0 = aus.au_parent_obj_id0(+)   
           AND pino.order_id NOT IN (SELECT ot.order_id
                                       FROM TMP_ORDER_PHONES_T ot)
         GROUP BY pino.order_id, pino.date_from, pino.date_to, sal.NAME, 
                  NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t)),
                  s.poid_id0, s.poid_type, s.status;

    l_Count := SQL%ROWCOUNT;

    Pk01_Syslog.write_Msg( p_Msg => 'Step2: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );

    -- 2. Текущие закрытые
    INSERT INTO TMP_ORDER_PHONES_T(
               order_id, phone_number, 
               date_from, 
               date_to,
               srv_poid_id0, srv_poid_type, srv_status,
               srv_revision, au_created_t, rec_source,
               not_transform)               
    SELECT pino.order_id, MIN(sal.NAME) ph_num,
           pino.date_from,
           LEAST(TRUNC(NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t))) 
                         + INTERVAL '0 23:59:59' DAY TO SECOND,
                    pino.date_to) date_to,            
         -- до 03.06.2014  
          /* GREATEST(TRUNC(NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t))) 
                         + INTERVAL '0 23:59:59' DAY TO SECOND,
                    pino.date_from) date_from, 
           pino.date_to date_to, */
           s.poid_id0, s.poid_type, s.status, 
           MAX(aus.au_parent_obj_rev) revision, 
           NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t)), --gc_MaxDate, 
           1 SRC,
           0 not_transform
           -- DECODE(a.business_type,1,'Ф',2,'Ю') 
      FROM pin.order_t pino,
           pin.profile_contract_info_t@mmtdb pci,
           pin.profile_t@mmtdb p,
           pin.service_t@mmtdb s,
           pin.au_service_t@mmtdb aus, 
           pin.au_service_alias_list_t@mmtdb sal
     WHERE pino.order_no = pci.order_num
       AND pci.obj_id0 = p.poid_id0
       AND p.account_obj_id0 = s.account_obj_id0 
       AND s.poid_type IN (gc_Service_Type, gc_SrvFree_Type,gc_SrvZone_Type) -- LIKE '/service/telco/gsm/telephony%' 
       AND s.status = gc_Closed
       AND s.poid_id0 = aus.au_parent_obj_id0       
       AND aus.poid_id0 = sal.obj_id0          
     GROUP BY pino.order_id, pino.date_from, pino.date_to, s.poid_id0, s.poid_type, s.status, 
              NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t));

  --  COMMIT;

    Pk01_Syslog.write_Msg( p_Msg => 'Step3: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );

    PIN.Gather_Table_Stat(l_Tab_Name => 'TMP_ORDER_PHONES_T');

    Pk01_Syslog.write_Msg( p_Msg => 'Step4: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );

    ----          
    -- Добавляем всю историю из архивных таблиц для данных, загруженных на пред. шаге 
    --
    -- Прим.: след. 2 запроса вместо одногго, что бы использовать hash
    INSERT INTO TMP_ORDER_PHONES_T(
               order_id, phone_number, 
               date_from, date_to,
               srv_poid_id0, srv_poid_type, srv_status,
               srv_revision, au_srv_poid_id0, au_created_t, rec_source,
               not_transform)   
    SELECT ---+ use_hash(s) 
           t.order_id,
           sal.NAME                                 ph_num,
           TRUNC(NVL(pin.u2d@mmtdb(s.effective_t),
                     pin.u2d@mmtdb(s.created_t)))   
               + DECODE(s.status, gc_Active, 0,
                                  gc_NotActive, 1-1/86400,
                                  gc_Closed, 1-1/86400
                                                  ) date_from,      
           NULL                                     date_to,
           s.au_parent_obj_id0                      srv_poid_id0,
           s.au_parent_obj_type                     srv_poid_type,
           s.status                                 srv_status,
           s.au_parent_obj_rev                      srv_revision,
           s.poid_id0                               au_srv_poid_id0,
           pin.u2d@mmtdb(s.created_t)               au_created_t,           
           2                                        rec_source,
           0                                        not_transform 
           --DECODE(a.business_type,1,'Ф',2,'Ю')      account_type
      FROM TMP_ORDER_PHONES_T t,
           pin.au_service_alias_list_t@mmtdb sal,
           pin.au_service_t@mmtdb s
     WHERE 1=1
       AND t.srv_poid_id0 = s.au_parent_obj_id0
       AND t.srv_revision != s.au_parent_obj_rev
       AND s.poid_id0 = sal.obj_id0;

    l_Count := l_Count + SQL%ROWCOUNT;       

    Pk01_Syslog.write_Msg( p_Msg => 'Step5a: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );

    INSERT INTO TMP_ORDER_PHONES_T(
               order_id, phone_number, 
               date_from, date_to,
               srv_poid_id0, srv_poid_type, srv_status,
               srv_revision, au_srv_poid_id0, au_created_t, rec_source,
               not_transform)   
    SELECT --+ use_hash(s) 
           t.order_id,
           sal.NAME                                 ph_num,
           TRUNC(NVL(pin.u2d@mmtdb(s.effective_t),
                     pin.u2d@mmtdb(s.created_t)))   
               + DECODE(s.status, gc_Active, 0,
                                  gc_NotActive, 1-1/86400,
                                  gc_Closed, 1-1/86400
                                                  ) date_from,      
           NULL                                     date_to,
           s.au_parent_obj_id0                      srv_poid_id0,
           s.au_parent_obj_type                     srv_poid_type,
           s.status                                 srv_status,
           s.au_parent_obj_rev                      srv_revision,
           s.poid_id0                               au_srv_poid_id0,
           pin.u2d@mmtdb(s.created_t)               au_created_t,           
           2                                        rec_source,
           0                                        not_transform 
           --DECODE(a.business_type,1,'Ф',2,'Ю')      account_type
      FROM TMP_ORDER_PHONES_T t,
           pin.au_service_alias_list_t@mmtdb sal,
           pin.au_service_t@mmtdb s
     WHERE 1=1
       AND t.srv_poid_id0 = s.au_parent_obj_id0
       AND t.SRV_STATUS = gc_Closed 
       AND t.srv_revision = s.au_parent_obj_rev 
       AND t.SRV_STATUS = s.status
       AND s.poid_id0 = sal.obj_id0
       AND NOT EXISTS (SELECT 1
                         FROM TMP_ORDER_PHONES_T ot
                        WHERE ot.au_srv_poid_id0 = s.poid_id0);       
   
    l_Count := l_Count + SQL%ROWCOUNT;
    
    COMMIT;

    Pk01_Syslog.write_Msg( p_Msg => 'Step5b: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );

    -- Добавляем странные номера, у которых служба открыта, но в alias их нет, а есть только в таблице аудита
    INSERT INTO TMP_ORDER_PHONES_T(
               order_id, phone_number, 
               date_from, 
               date_to,
               srv_poid_id0, srv_poid_type, srv_status,
               srv_revision, au_created_t, rec_source,
               not_transform)    
    SELECT pino.order_id, MIN(sal.NAME) ph_num, 
           GREATEST(TRUNC(NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t))) 
                         + INTERVAL '0 23:59:59' DAY TO SECOND,
                    pino.date_from) date_from, 
           GREATEST(TRUNC(NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t))) 
                         + INTERVAL '0 23:59:59' DAY TO SECOND,
                    pino.date_from) date_to, -- ставим сразу дату закрытия, чтобы такие номера в тарификацию не попали                    
           s.poid_id0, s.poid_type, s.status, 
           MAX(aus.au_parent_obj_rev) revision, gc_MaxDate, 1 SRC,
           1 not_transform
           -- DECODE(a.business_type,1,'Ф',2,'Ю') 
      FROM pin.order_t pino,
           pin.profile_contract_info_t@mmtdb pci,
           pin.profile_t@mmtdb p,
           pin.service_t@mmtdb s,
           pin.au_service_t@mmtdb aus, 
           pin.au_service_alias_list_t@mmtdb sal
     WHERE pino.order_no = pci.order_num
       AND pci.obj_id0 = p.poid_id0
       AND p.account_obj_id0 = s.account_obj_id0 
       AND s.poid_type IN (gc_Service_Type, gc_SrvFree_Type,gc_SrvZone_Type) --'/service/telco/gsm/telephony' 
       AND s.status = gc_Active
       AND s.poid_id0 = aus.au_parent_obj_id0       
       AND aus.poid_id0 = sal.obj_id0          
       AND NOT EXISTS (SELECT 1
                         FROM pin.service_alias_list_t@mmtdb sal
                        WHERE s.poid_id0 = sal.obj_id0
                      )          
       AND NOT EXISTS (SELECT 1
                         FROM TMP_ORDER_PHONES_T t
                        WHERE t.srv_poid_id0 = s.poid_id0)
     GROUP BY pino.order_id, pino.date_from, pino.date_to, s.poid_id0, s.poid_type, s.status, 
              NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t));    
  
    l_Count := l_Count + SQL%ROWCOUNT;

    Pk01_Syslog.write_Msg(p_Msg => 'импортировано ' || TO_CHAR(l_Count) || ' записей', 
                          p_Src => c_PkgName||'.'||c_prcName );           

   -- удаляем получившиеся некорректные записи
    DELETE FROM TMP_ORDER_PHONES_T
     WHERE date_from > date_to;
   
    Pk01_Syslog.write_Msg( p_Msg => 'Step7: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );   
   
    PIN.Gather_Table_Stat(l_Tab_Name => 'TMP_ORDER_PHONES_T');

    Pk01_Syslog.write_Msg( p_Msg => 'Step8: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );   

   -- COMMIT;

    -- Выравниваем диапазоны, чтобы не было пересечений. 
    -- Принцип: все данные делятся по группам 
    --  номер_телефона-тип_соединения (ph_num-srv_poid_type)-идентификатор(poid) записи из текущих данных . 
    -- Внутри группы записи имеют приоритет от высшего к низшему: 
    --     1. текущие данные по открытым номерам (тбл. service_t) (поле rec_source)
    --     2. данные по закрытым номерам по номерам версий (srv_revision) 
    --  внутри каждой из указанных групп по дате создания, т.е. чем свежее данные, тем выше приоритет  
    --  Берется запись с высшим приоритетом, у нее из даты открытия вычитается 1сек. 
    --  Это будет дата закрытия след., более ранней записи
    --  Может возникнуть такая ситуация, что дата окончания станет меньше даты начала. Такие записи  
    --  будут просто удалены.  
    MERGE INTO TMP_ORDER_PHONES_T tg
    USING (
            SELECT t.rd,
                   t.date_from, next_df,
                   NVL(t.next_df-1/86400, gc_MaxDate) new_date_to,
                   date_to, 
                   --next_status, 
                   --srv_status_to, 
                   srv_status, 
                   next_poid, 
                   --srv_poid_id0_to, 
                   srv_poid_id0, au_srv_poid_id0,    
                   rec_source, au_created_t, srv_revision
              FROM (        
                    SELECT r.phone_number, r.srv_poid_type,
                           lag(r.date_from) OVER (PARTITION BY r.phone_number, r.srv_poid_type, r.srv_poid_id0
                                                      ORDER BY r.rec_source, r.srv_revision DESC, r.au_created_t DESC, r.date_from DESC, r.date_to DESC NULLS LAST) next_df,
                           lag(r.srv_status) OVER (PARTITION BY r.phone_number, r.srv_poid_type, r.srv_poid_id0
                                                       ORDER BY r.rec_source, r.srv_revision DESC, r.au_created_t DESC, r.date_from DESC, r.date_to DESC NULLS LAST) next_status,                                      
                           lag(NVL(r.au_srv_poid_id0, r.srv_poid_id0)) 
                                             OVER (PARTITION BY r.phone_number, r.srv_poid_type, r.srv_poid_id0
                                                       ORDER BY r.rec_source, r.srv_revision DESC, r.au_created_t DESC, r.date_from DESC, r.date_to DESC NULLS LAST) next_poid,                                                  
                           r.date_from,
                           r.date_to,
                           r.ROWID rd,
                           r.srv_status, r.rec_source, r.au_created_t, r.srv_revision,
                           --r.srv_poid_id0_to, 
                           --r.srv_status_to,
                           r.srv_poid_id0, r.au_srv_poid_id0
                      FROM TMP_ORDER_PHONES_T r
                     WHERE not_transform != 1 
                  --   WHERE r.ph_num = '73532744168' -- '73437461015' --'73425727922' --
                    ) t
              ORDER BY t.srv_poid_id0, t.rec_source, t.srv_revision DESC, t.au_created_t DESC, t.date_from, t.date_to   
          ) t 
      ON (tg.ROWID = t.rd)
    WHEN MATCHED THEN UPDATE 
    SET tg.date_to = t.new_date_to/*,
        tg.srv_poid_id0_to = t.next_poid,
        tg.srv_status_to = t.next_status*/;

    Pk01_Syslog.write_Msg( p_Msg => 'Step9: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );       

    -- удаляем все диапазоны в которых номера закрыты или неактивны. 
    -- Они всё равно не принимают участие в привязках л/счетов
    -- и удаляем записи, у которые дата закрытия стала меньше даты открытия
    DELETE FROM TMP_ORDER_PHONES_T
    WHERE date_from > date_to;
      -- OR srv_status != gc_Active;

    l_Count := l_Count - SQL%ROWCOUNT;

    Pk01_Syslog.write_Msg( p_Msg => 'Step10: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );   

    -- удаляем записи, у которых диапазон входит в другой или дата начала больше даты конца
    DELETE FROM TMP_ORDER_PHONES_T
    WHERE not_transform != 1
      AND rowid IN (  
                SELECT t2.rowid rd
                  FROM TMP_ORDER_PHONES_T t1,
                       TMP_ORDER_PHONES_T t2
                 WHERE t1.not_transform != 1
                   AND t2.not_transform != 2
                   AND t1.rowid != t2.rowid
                   AND t1.srv_poid_type = t2.srv_poid_type 
                   AND t1.phone_number = t2.phone_number
                   AND t2.rec_source != 1
                   AND t1.rec_source <= t2.rec_source -- по приоритету при прочих равных остаются открытые
                   AND t1.au_created_t > t2.au_created_t -- и с наиболее свежей датой изменения
                   AND t1.date_from <= t2.date_from
                   AND t1.date_to >= t2.date_to
                 );     

    l_Count := l_Count - SQL%ROWCOUNT;
   
    Pk01_Syslog.write_Msg( p_Msg => 'Step11: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );      
    
    -- Теперь выравниваем дипазоны группируя только по номер_телефона - тип_соединения
    MERGE INTO TMP_ORDER_PHONES_T tg
    USING (
           SELECT rd, new_date_to
                 FROM (
                        SELECT t.rd,
                               --t.date_from,
                               (CASE
                                   WHEN t.date_to >= t.next_df  -- если начало след. интервала раньше, чем заканчивается текущий
                                   THEN
                                      t.next_df - 1/86400 -- ставим текущей конец периода как дата начала следующего - 1 сек.
                                   ELSE
                                      t.date_to -- оставляем текущую дату конца.
                               END) new_date_to,
                               date_to
                          FROM (
                                SELECT --r.ph_num, r.srv_poid_type,
                                       lag(r.date_from) OVER (PARTITION BY r.phone_number, r.srv_poid_type 
                                                                  ORDER BY r.rec_source, r.au_created_t DESC, r.date_from DESC) next_df,
                                       r.date_from,
                                       r.date_to,
                                       r.ROWID rd
                                  FROM TMP_ORDER_PHONES_T r
                                 WHERE r.not_transform != 1 
                                 --WHERE r.ph_num = '73425727922'
                               ) t
                      ) tt
                WHERE new_date_to != date_to
         ) t       
      ON (tg.ROWID = t.rd)
    WHEN MATCHED THEN UPDATE 
    SET tg.date_to = t.new_date_to;    
    
    Pk01_Syslog.write_Msg( p_Msg => 'Step12: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );   

    -- удаляем записи, у которые дата закрытия стала меньше даты открытия
    DELETE FROM TMP_ORDER_PHONES_T
    WHERE date_from > date_to;

    l_Count := l_Count - SQL%ROWCOUNT;

    Pk01_Syslog.write_Msg( p_Msg => 'Step13: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );   

    -- удаляем записи, у которых диапазон входит в другой или дата начала больше даты конца
    DELETE FROM TMP_ORDER_PHONES_T
    WHERE not_transform != 1
      AND rowid IN (  
                SELECT t2.rowid rd
                  FROM TMP_ORDER_PHONES_T t1,
                       TMP_ORDER_PHONES_T t2
                 WHERE t1.not_transform != 1
                   AND t2.not_transform != 1
                   AND t1.rowid != t2.rowid
                   AND t1.srv_poid_type = t2.srv_poid_type 
                   AND t1.phone_number = t2.phone_number
                   AND t2.rec_source != 1
                   AND t1.rec_source <= t2.rec_source -- по приоритету при прочих равных остаются открытые
                   AND t1.au_created_t > t2.au_created_t -- и с наиболее свежей датой изменения
                   AND t1.date_from <= t2.date_from
                   AND t1.date_to >= t2.date_to
                 );     

    l_Count := l_Count - SQL%ROWCOUNT;
        
    Pk01_Syslog.write_Msg( p_Msg => 'Step14: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );       
    
    -- отметка об особых условиях у клиентов для тарификации соединений на 8800
    -- (запрос сделан c поиском через rowid, т.к. иначе не получалось добиться hash_sj в плане.
    --  оракл сваливался на nl и все начинало дико тормозить   
 /*   UPDATE TMP_ORDER_PHONES_T tt
       SET tt.a_number_orig = 1
     WHERE tt.rowid IN (
                   SELECT t.rowid
                     FROM pin.profile_t@mmtdb p, 
                          pin.profile_acct_extrating_data_t@mmtdb pae,
                          TMP_ORDER_PHONES_T t 
                    WHERE p.NAME = 'A_NUMBER_ZONE'
                      AND pae.VALUE = 'A_NUMBER_ORIG'
                      AND pae.obj_id0 = p.poid_id0
                      AND p.account_obj_id0 = t.account_id
                      AND t.date_from >= NVL(u2d(pae.valid_from),gc_MinDate)
                      AND (t.date_to <= NVL(u2d(pae.valid_to),gc_MaxDate) 
                              OR u2d(pae.valid_to) > TO_DATE('01.01.2030','dd.mm.yyyy hh24:mi:ss')) 
                   );    
*/

    Pk01_Syslog.write_Msg(p_Msg => 'Подготовлено к загрузке ' || TO_CHAR(l_Count) || ' записей', 
                          p_Src => c_PkgName||'.'||c_prcName );               

    COMMIT;

    -- удаляем устаревшие данные   
    DELETE FROM PIN.ORDER_PHONES_T p
     WHERE p.order_id NOT IN (SELECT o.order_id
                                FROM order_t o
                               WHERE o.account_id IN (SELECT TO_NUMBER(d.KEY)
                                                        FROM DICTIONARY_T d
                                                       WHERE LEVEL = 2
                                                     CONNECT BY PRIOR d.key_id = d.parent_id
                                                       START WITH d.KEY = 'LOCK_ACC')
                              )
       AND p.phone_number NOT LIKE pin.pk00_const.c_Order_Test_Pref || '%' 
       AND NOT EXISTS (SELECT 1
                         FROM PIN.TMP_ORDER_PHONES_T t 
                        WHERE t.order_id     = p.order_id
                          AND t.phone_number = p.phone_number 
                          AND t.date_from    = p.date_from 
                          AND t.date_to      = p.date_to
                      );

    l_Count := SQL%ROWCOUNT;

    Pk01_Syslog.write_Msg( p_Msg => 'Step15: ' || TO_CHAR(SYSDATE,'dd.mm.yyyy hh24:mi:ss'),    
                           p_Src => c_PkgName||'.'||c_prcName );   

    INSERT INTO PIN.ORDER_PHONES_T (
           order_id, phone_number, date_from, date_to) 
    SELECT order_id, phone_number, date_from, date_to
      FROM PIN.TMP_ORDER_PHONES_T t
     WHERE NOT EXISTS (SELECT 1
                         FROM PIN.ORDER_PHONES_T p 
                        WHERE t.order_id     = p.order_id
                          AND t.phone_number = p.phone_number 
                          AND t.date_from    = p.date_from 
                          AND t.date_to      = p.date_to
                      ); 

    Pk01_Syslog.write_Msg(p_Msg => 'добавлено ' || TO_CHAR(SQL%ROWCOUNT) || ', удалено ' || TO_CHAR(l_Count) || ' записей, ' ||
                                   'итого: ' || TO_CHAR(SQL%ROWCOUNT+l_Count), 
                          p_Src => c_PkgName||'.'||c_prcName );           

    COMMIT;   
    
  --  EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_ORDER_PHONES_T';
    
END Load_BindInfo_PH;



PROCEDURE Load_BindInfo_PH_Old_2
IS
    c_prcName CONSTANT varchar2(32) := 'Load_BindInfo_PH'; 

    l_Curr_Date date;
    l_Count     PLS_INTEGER;


BEGIN

    l_Curr_Date := SYSDATE;

    Pk01_Syslog.write_Msg( p_Msg => 'Start',    
                           p_Src => c_PkgName||'.'||c_prcName );
                              
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    -- очищаем временную таблицу
    DELETE FROM TMP_ORDER_PHONES_T;

    --   
    -- вносим все текущие данные 
    --
    -- 1. Текущие активные и не активные
    INSERT INTO TMP_ORDER_PHONES_T(
               order_id, phone_number, 
               date_from, 
               date_to,
               srv_poid_id0, srv_poid_type, srv_status,
               srv_revision, au_created_t, rec_source)
        SELECT pino.order_id, sal.NAME ph_num, 
               GREATEST(TRUNC(NVL(pin.u2d@mmtdb(s.effective_t), pin.u2d@mmtdb(s.created_t))) 
                         + DECODE(s.status, gc_Active, 0,
                                            gc_NotActive, 1-1/86400),
                        pino.date_from) date_from, 
               NULL date_to, 
               s.poid_id0, s.poid_type, s.status, 
               MAX(aus.au_parent_obj_rev) revision, gc_MaxDate, 1 SRC
               -- DECODE(a.business_type,1,'Ф',2,'Ю') 
          FROM pin.account_t pina,
               pin.order_t pino,
               --pin.profile_contract_info_t@mmtdb pci,
               --pin.profile_t@mmtdb p,
               pin.service_t@mmtdb s, 
               pin.service_alias_list_t@mmtdb sal,
               pin.au_service_t@mmtdb aus
         WHERE pina.account_type != pk00_const.c_ACC_TYPE_J
           AND pina.account_id = pino.account_id 
           AND pino.order_no NOT LIKE pin.pk00_const.c_Order_Test_Pref || '%' 
           --pci.order_num
           --AND pci.obj_id0 = p.poid_id0
           AND TO_CHAR(s.account_obj_id0) = pino.notes -- здесь poid аккунта заказа (не родителя) НБ        
           AND s.poid_type = gc_Service_Type -- LIKE '/service/telco/gsm/telephony%'
           AND s.status IN (gc_Active, gc_NotActive)
           AND s.poid_id0 = sal.obj_id0          
           AND s.poid_id0 = aus.au_parent_obj_id0(+)   
         GROUP BY pino.order_id, pino.date_from, sal.NAME, 
                  NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t)),
                  s.poid_id0, s.poid_type, s.status;
              
    l_Count := SQL%ROWCOUNT;               

    -- 2. Текущие закрытые
    INSERT INTO TMP_ORDER_PHONES_T(
               order_id, phone_number, 
               date_from, 
               date_to,
               srv_poid_id0, srv_poid_type, srv_status,
               srv_revision, au_created_t, rec_source)               
    SELECT pino.order_id, MIN(sal.NAME) ph_num, 
           GREATEST(TRUNC(NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t))) 
                          + INTERVAL '0 23:59:59' DAY TO SECOND,
                    pino.date_from) date_from, 
           NULL date_to,
           s.poid_id0, s.poid_type, s.status, 
           MAX(aus.au_parent_obj_rev) revision, gc_MaxDate, 1 SRC
           -- DECODE(a.business_type,1,'Ф',2,'Ю') 
      FROM pin.account_t pina,
           pin.order_t pino,
       --    pin.profile_contract_info_t@mmtdb pci,
       --    pin.profile_t@mmtdb p,
           pin.service_t@mmtdb s,
           pin.au_service_t@mmtdb aus, 
           pin.au_service_alias_list_t@mmtdb sal
     WHERE pina.account_type != pk00_const.c_ACC_TYPE_J
       AND pina.account_id = pino.account_id 
       AND pino.order_no NOT LIKE pin.pk00_const.c_Order_Test_Pref || '%' 
       AND TO_CHAR(s.account_obj_id0) = pino.notes -- здесь poid аккунта заказа (не родителя) НБ
       AND s.poid_type = gc_Service_Type -- LIKE '/service/telco/gsm/telephony%' 
       AND s.status = gc_Closed
       AND s.poid_id0 = aus.au_parent_obj_id0       
       AND aus.poid_id0 = sal.obj_id0          
     GROUP BY pino.order_id, pino.date_from, s.poid_id0, s.poid_type, s.status, 
              NVL(pin.u2d@mmtdb(s.effective_t),pin.u2d@mmtdb(s.created_t));

  --  COMMIT;

    PIN.Gather_Table_Stat(l_Tab_Name => 'TMP_ORDER_PHONES_T');

    ----          
    -- Добавляем всю историю из архивных таблиц для данных, загруженных на пред. шаге 
    --
    INSERT INTO TMP_ORDER_PHONES_T(
               order_id, phone_number, 
               date_from, date_to,
               srv_poid_id0, srv_poid_type, srv_status,
               srv_revision, au_srv_poid_id0, au_created_t, rec_source)           
    SELECT --+ use_hash(t)
           t.order_id,
           sal.NAME                                 ph_num,
           TRUNC(NVL(pin.u2d@mmtdb(s.effective_t),
                     pin.u2d@mmtdb(s.created_t)))   
               + DECODE(s.status, gc_Active, 0,
                                  gc_NotActive, 1-1/86400,
                                  gc_Closed, 1-1/86400
                                                  ) date_from,      
           NULL                                     date_to,
           s.au_parent_obj_id0                      srv_poid_id0,
           s.au_parent_obj_type                     srv_poid_type,
           s.status                                 srv_status,
           s.au_parent_obj_rev                      srv_revision,
           s.poid_id0                               au_srv_poid_id0,
           pin.u2d@mmtdb(s.created_t)               au_created_t,           
           2                                        rec_source
           --DECODE(a.business_type,1,'Ф',2,'Ю')      account_type
      FROM TMP_ORDER_PHONES_T t,
           pin.au_service_alias_list_t@mmtdb sal,
           pin.au_service_t@mmtdb s
     WHERE 1=1
       AND t.srv_poid_id0 = s.au_parent_obj_id0
       AND (
            (t.srv_revision != s.au_parent_obj_rev)
             OR
            (t.SRV_STATUS = gc_Closed AND t.srv_revision = s.au_parent_obj_rev AND t.SRV_STATUS != s.status )
           )            
       AND s.poid_id0 = sal.obj_id0;              
   
    l_Count := l_Count + SQL%ROWCOUNT;

    Pk01_Syslog.write_Msg(p_Msg => 'импортировано ' || TO_CHAR(l_Count) || ' записей', 
                          p_Src => c_PkgName||'.'||c_prcName );           

   -- COMMIT;

    -- Выравниваем диапазоны, чтобы не было пересечений. 
    -- Принцип: все данные делятся по группам 
    --  номер_телефона-тип_соединения (ph_num-srv_poid_type)-идентификатор(poid) записи из текущих данных . 
    -- Внутри группы записи имеют приоритет от высшего к низшему: 
    --     1. текущие данные по открытым номерам (тбл. service_t) (поле rec_source)
    --     2. данные по закрытым номерам по номерам версий (srv_revision) 
    --  внутри каждой из указанных групп по дате создания, т.е. чем свежее данные, тем выше приоритет  
    --  Берется запись с высшим приоритетом, у нее из даты открытия вычитается 1сек. 
    --  Это будет дата закрытия след., более ранней записи
    --  Может возникнуть такая ситуация, что дата окончания станет меньше даты начала. Такие записи  
    --  будут просто удалены.  
    MERGE INTO TMP_ORDER_PHONES_T tg
    USING (
            SELECT t.rd,
                   t.date_from, next_df,
                   NVL(t.next_df-1/86400, gc_MaxDate) new_date_to,
                   date_to, 
                   --next_status, 
                   --srv_status_to, 
                   srv_status, 
                   next_poid, 
                   --srv_poid_id0_to, 
                   srv_poid_id0, au_srv_poid_id0,    
                   rec_source, au_created_t, srv_revision
              FROM (        
                    SELECT r.phone_number, r.srv_poid_type,
                           lag(r.date_from) OVER (PARTITION BY r.phone_number, r.srv_poid_type, r.srv_poid_id0
                                                      ORDER BY r.rec_source, r.srv_revision DESC, r.au_created_t DESC, r.date_from DESC) next_df,
                           lag(r.srv_status) OVER (PARTITION BY r.phone_number, r.srv_poid_type, r.srv_poid_id0
                                                       ORDER BY r.rec_source, r.srv_revision DESC, r.au_created_t DESC, r.date_from DESC) next_status,                                      
                           lag(NVL(r.au_srv_poid_id0, r.srv_poid_id0)) 
                                             OVER (PARTITION BY r.phone_number, r.srv_poid_type, r.srv_poid_id0
                                                       ORDER BY r.rec_source, r.srv_revision DESC, r.au_created_t DESC, r.date_from DESC) next_poid,                                                  
                           r.date_from,
                           r.date_to,
                           r.ROWID rd,
                           r.srv_status, r.rec_source, r.au_created_t, r.srv_revision,
                           --r.srv_poid_id0_to, 
                           --r.srv_status_to,
                           r.srv_poid_id0, r.au_srv_poid_id0
                      FROM TMP_ORDER_PHONES_T r
                  --   WHERE r.ph_num = '73532744168' -- '73437461015' --'73425727922' --
                    ) t
              ORDER BY t.srv_poid_id0, t.rec_source, t.srv_revision DESC, t.au_created_t DESC, t.date_from, t.date_to   
          ) t 
      ON (tg.ROWID = t.rd)
    WHEN MATCHED THEN UPDATE 
    SET tg.date_to = t.new_date_to/*,
        tg.srv_poid_id0_to = t.next_poid,
        tg.srv_status_to = t.next_status*/;
    

    -- удаляем все диапазоны в которых номера закрыты или неактивны. 
    -- Они всё равно не принимают участие в привязках л/счетов
    DELETE FROM TMP_ORDER_PHONES_T
    WHERE srv_status != gc_Active;

    l_Count := l_Count - SQL%ROWCOUNT;

    -- удаляем записи, у которые дата закрытия стала меньше даты открытия
    DELETE FROM TMP_ORDER_PHONES_T
    WHERE date_from > date_to;

    l_Count := l_Count - SQL%ROWCOUNT;

    -- удаляем записи, у которых диапазон входит в другой или дата начала больше даты конца
    DELETE FROM TMP_ORDER_PHONES_T
    WHERE rowid IN (  
                SELECT t2.rowid rd
                  FROM TMP_ORDER_PHONES_T t1,
                       TMP_ORDER_PHONES_T t2
                 WHERE t1.rowid != t2.rowid
                   AND t1.srv_poid_type = t2.srv_poid_type 
                   AND t1.phone_number = t2.phone_number
                   AND t2.rec_source != 1
                   AND t1.rec_source <= t2.rec_source -- по приоритету при прочих равных остаются открытые
                   AND t1.au_created_t > t2.au_created_t -- и с наиболее свежей датой изменения
                   AND t1.date_from <= t2.date_from
                   AND t1.date_to >= t2.date_to
                 );     

    l_Count := l_Count - SQL%ROWCOUNT;
    
    -- Теперь выравниваем дипазоны группируя только по номер_телефона - тип_соединения
    MERGE INTO TMP_ORDER_PHONES_T tg
    USING (
           SELECT rd, new_date_to
                 FROM (
                        SELECT t.rd,
                               --t.date_from,
                               (CASE
                                   WHEN t.date_to >= t.next_df  -- если начало след. интервала раньше, чем заканчивается текущий
                                   THEN
                                      t.next_df - 1/86400 -- ставим текущей конец периода как дата начала следующего - 1 сек.
                                   ELSE
                                      t.date_to -- оставляем текущую дату конца.
                               END) new_date_to,
                               date_to
                          FROM (
                                SELECT --r.ph_num, r.srv_poid_type,
                                       lag(r.date_from) OVER (PARTITION BY r.phone_number, r.srv_poid_type 
                                                                  ORDER BY r.rec_source, r.au_created_t DESC, r.date_from DESC) next_df,
                                       r.date_from,
                                       r.date_to,
                                       r.ROWID rd
                                  FROM TMP_ORDER_PHONES_T r
                                 --WHERE r.ph_num = '73425727922'
                               ) t
                      ) tt
                WHERE new_date_to != date_to
         ) t       
      ON (tg.ROWID = t.rd)
    WHEN MATCHED THEN UPDATE 
    SET tg.date_to = t.new_date_to;    
    

    -- удаляем записи, у которые дата закрытия стала меньше даты открытия
    DELETE FROM TMP_ORDER_PHONES_T
    WHERE date_from > date_to;

    l_Count := l_Count - SQL%ROWCOUNT;

    -- удаляем записи, у которых диапазон входит в другой или дата начала больше даты конца
    DELETE FROM TMP_ORDER_PHONES_T
    WHERE rowid IN (  
                SELECT t2.rowid rd
                  FROM TMP_ORDER_PHONES_T t1,
                       TMP_ORDER_PHONES_T t2
                 WHERE t1.rowid != t2.rowid
                   AND t1.srv_poid_type = t2.srv_poid_type 
                   AND t1.phone_number = t2.phone_number
                   AND t2.rec_source != 1
                   AND t1.rec_source <= t2.rec_source -- по приоритету при прочих равных остаются открытые
                   AND t1.au_created_t > t2.au_created_t -- и с наиболее свежей датой изменения
                   AND t1.date_from <= t2.date_from
                   AND t1.date_to >= t2.date_to
                 );     

    l_Count := l_Count - SQL%ROWCOUNT;
        
    -- отметка об особых условиях у клиентов для тарификации соединений на 8800
    -- (запрос сделан c поиском через rowid, т.к. иначе не получалось добиться hash_sj в плане.
    --  оракл сваливался на nl и все начинало дико тормозить   
 /*   UPDATE TMP_ORDER_PHONES_T tt
       SET tt.a_number_orig = 1
     WHERE tt.rowid IN (
                   SELECT t.rowid
                     FROM pin.profile_t@mmtdb p, 
                          pin.profile_acct_extrating_data_t@mmtdb pae,
                          TMP_ORDER_PHONES_T t 
                    WHERE p.NAME = 'A_NUMBER_ZONE'
                      AND pae.VALUE = 'A_NUMBER_ORIG'
                      AND pae.obj_id0 = p.poid_id0
                      AND p.account_obj_id0 = t.account_id
                      AND t.date_from >= NVL(u2d(pae.valid_from),gc_MinDate)
                      AND (t.date_to <= NVL(u2d(pae.valid_to),gc_MaxDate) 
                              OR u2d(pae.valid_to) > TO_DATE('01.01.2030','dd.mm.yyyy hh24:mi:ss')) 
                   );    
*/

    Pk01_Syslog.write_Msg(p_Msg => 'Подготовлено к загрузке ' || TO_CHAR(l_Count) || ' записей', 
                          p_Src => c_PkgName||'.'||c_prcName );               

    COMMIT;

    -- удаляем устаревшие данные   
    DELETE FROM PIN.ORDER_PHONES_T p
     WHERE p.order_id NOT IN (SELECT o.order_id
                                FROM order_t o,
                                     account_t a
                               WHERE o.account_id = a.account_id --Pk120_Bind_Clients.gc_Samara_Acc_Id)
                                 AND a.account_type = pk00_const.c_ACC_TYPE_J
                              )
       AND p.phone_number NOT LIKE pin.pk00_const.c_Order_Test_Pref || '%' 
       AND NOT EXISTS (SELECT 1
                         FROM PIN.TMP_ORDER_PHONES_T t 
                        WHERE t.order_id     = p.order_id
                          AND t.phone_number = p.phone_number 
                          AND t.date_from    = p.date_from 
                          AND t.date_to      = p.date_to
                      );

    l_Count := SQL%ROWCOUNT;

    INSERT INTO PIN.ORDER_PHONES_T (
           order_id, phone_number, date_from, date_to) 
    SELECT order_id, phone_number, date_from, date_to
      FROM PIN.TMP_ORDER_PHONES_T t
     WHERE NOT EXISTS (SELECT 1
                         FROM PIN.ORDER_PHONES_T p 
                        WHERE t.order_id     = p.order_id
                          AND t.phone_number = p.phone_number 
                          AND t.date_from    = p.date_from 
                          AND t.date_to      = p.date_to
                      ); 

    Pk01_Syslog.write_Msg(p_Msg => 'добавлено ' || TO_CHAR(SQL%ROWCOUNT) || ', удалено ' || TO_CHAR(l_Count) || ' записей, ' ||
                                   'итого: ' || TO_CHAR(SQL%ROWCOUNT+l_Count), 
                          p_Src => c_PkgName||'.'||c_prcName );           

    COMMIT;   
    
  --  EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_ORDER_PHONES_T';
    
END Load_BindInfo_PH_Old_2;



PROCEDURE ReLoad_Orders_Full
IS

    c_prcName CONSTANT varchar2(32) := 'ReLoad_Orders_Full';
    
    l_RP_Code       varchar2(128);
    l_RP_Id         number;
    l_Date_From     date;
    l_Date_To       date;
    l_Order_Id      number;
    l_Order_Body_Id number;
    l_Account_Id    number;
    l_Count         PLS_INTEGER;
    l_Cnt_Closed    PLS_INTEGER;
    
    TYPE t_Num IS TABLE OF number INDEX BY varchar2(16);
    
    lt_Ind_RP t_Num; 

BEGIN

  -- ++++++++++++++++++++++++++++++++++++
  -- очищаем таблицу Order_t
  -- ++++++++++++++++++++++++++++++++++++      

   -- удаляем данные из item_t      
    DELETE FROM detail_mmts_t d
     WHERE d.item_id IN (SELECT i.item_id
                           FROM account_t a,
                                order_t o,
                                item_t i
                          WHERE a.account_type != pk00_const.c_ACC_TYPE_J
                            AND a.account_id = o.order_id
                            AND o.order_id = i.order_id
                          )
       AND NOT EXISTS (SELECT 1
                         FROM item_t i
                        WHERE I.INV_ITEM_ID IS NOT NULL
                          AND i.item_id = d.item_id); 
    
    Pk01_Syslog.write_Msg(p_Msg => 'удалено из detail_mmts_t: ' || TO_CHAR(SQL%ROWCOUNT), 
                          p_Src => c_PkgName||'.'||c_prcName );           
    
   -- удаляем данные из item_t      
    DELETE FROM item_t i
     WHERE i.inv_item_id IS NOT NULL
       AND i.order_id IN (SELECT o.order_id
                            FROM account_t a,
                                 order_t o
                           WHERE a.account_type != pk00_const.c_ACC_TYPE_J
                             AND a.account_id = o.order_id
                          );    
                              
    Pk01_Syslog.write_Msg(p_Msg => 'удалено из item_t: ' || TO_CHAR(SQL%ROWCOUNT), 
                          p_Src => c_PkgName||'.'||c_prcName );                                         
                              
   -- удаляем номерные емкости
    DELETE FROM order_phones_t p
     WHERE EXISTS (SELECT o.order_id
                     FROM account_t a,
                          order_t o
                    WHERE a.account_type != 'J' --pk00_const.c_ACC_TYPE_J
                      AND a.account_id = o.account_id
                      AND o.order_id = p.order_id
                 );
                              
    Pk01_Syslog.write_Msg(p_Msg => 'удалено из order_phones_t: ' || TO_CHAR(SQL%ROWCOUNT), 
                          p_Src => c_PkgName||'.'||c_prcName );                                                 

   -- доп. данные заказа  
    DELETE FROM order_body_t b
     WHERE EXISTS (SELECT 1
                     FROM account_t a,
                          order_t o
                    WHERE a.account_type != 'J' --pk00_const.c_ACC_TYPE_J
                      AND a.account_id = o.account_id
                      AND o.order_id = b.order_id 
                  )                 
       AND NOT EXISTS (SELECT 1
                         FROM item_t i
                        WHERE I.INV_ITEM_ID IS NOT NULL
                          AND i.order_id = b.order_id);
                              
    Pk01_Syslog.write_Msg(p_Msg => 'удалено из order_body_t: ' || TO_CHAR(SQL%ROWCOUNT), 
                          p_Src => c_PkgName||'.'||c_prcName );                                                         
                              
   -- удаляем заказы
    DELETE FROM order_t o
     WHERE EXISTS (SELECT 1
                     FROM account_t a
                    WHERE a.account_type != 'J' --pk00_const.c_ACC_TYPE_J
                      AND a.account_id = o.account_id
                  )         
       AND NOT EXISTS (SELECT 1
                         FROM item_t i
                        WHERE I.INV_ITEM_ID IS NOT NULL
                          AND i.order_id = o.order_id);

    Pk01_Syslog.write_Msg(p_Msg => 'удалено заказов ' || TO_CHAR(SQL%ROWCOUNT), 
                          p_Src => c_PkgName||'.'||c_prcName );           
                              
    
    -- +++++++++++++++++++++++++++++++++++++++++++++++++++
    -- получаем список л/с, у которых есть индивидуальные ТП
    -- (для ускорения в память загоняем ибо их мало)
    FOR l_cur IN (SELECT p.account_obj_id0 
                    FROM pin.profile_t@mmtdb p, 
                         pin.profile_acct_extrating_data_t@mmtdb pae,
                         INTEGRATE.IFW_RATEPLAN@mmtdb rp
                   WHERE p.NAME = 'RATEPLAN' 
                     AND pae.obj_id0 = p.poid_id0
                     AND rp.status = 'A'
                     AND rp.code = pae.VALUE
                   GROUP BY p.account_obj_id0  
                 )        
    LOOP
    
        lt_Ind_Rp(TO_CHAR(l_cur.account_obj_id0)) := 1;    
    
    END LOOP;
    
    --
    -- +++++++++++++++++++++++++++++++++++++++++++++++++++
    
    
    
    -- = = = = = = = = = = = = = = = = = = = = = = = = = =
    -- Добавление заказов
    -- = = = = = = = = = = = = = = = = = = = = = = = = = =
    
    l_Count := 0;
    
    -- Добавление открытых заказов
    FOR l_cur IN (
                    SELECT --a.account_id,
                           ap.account_no,
                           ac.account_no acc_ch,
                           ac.poid_id0   acc_ch_id,
                           pci.order_num order_no,
                           TRUNC (pin.u2d@mmtdb(MIN(p.effective_t))) order_date
                      FROM pin.account_t@mmtdb ac,
                           pin.account_t@mmtdb ap,
                           pin.billinfo_t@mmtdb bic,
                           pin.billinfo_t@mmtdb bip,
                           pin.service_t@mmtdb s,
                           pin.profile_t@mmtdb p,
                           pin.profile_contract_info_t@mmtdb pci
                           --pin.account_t a
                     WHERE ac.business_type = 1
                       AND ac.status != 10103
                       AND ac.poid_id0 = bic.account_obj_id0
                       AND bic.ar_billinfo_obj_id0 = bip.poid_id0
                       AND bip.account_obj_id0 = ap.poid_id0   
                       AND s.poid_type = gc_Service_Type --'/service/telco/gsm/telephony'   
                       AND ac.poid_id0 = s.account_obj_id0 
                       AND ac.poid_id0 = p.account_obj_id0 
                       AND p.poid_id0 = pci.obj_id0
                    --   AND a.account_type != PK00_CONST.C_ACC_TYPE_J
                    --   AND ap.account_no = a.account_no 
                    GROUP BY --a.account_id,
                             ap.account_no, ac.account_no, ac.poid_id0,
                             pci.order_num  
                    --HAVING pci.order_num NOT IN (SELECT order_no
                    --                               FROM pin.order_t)         
                )
    LOOP
    
        BEGIN
            -- проверяем наличие л/счета в текущем списке
            SELECT account_id 
              INTO l_Account_Id
              FROM account_t a
             WHERE account_no = l_cur.account_no;
    
    
            BEGIN
                -- проверяем есть ли заказ с указанным номером
                SELECT 1 INTO l_RP_Id 
                  FROM ORDER_T
                 WHERE ORDER_NO = l_cur.order_no;
 
                /* Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                                 ' уже существует.', 
                                      p_Src   => c_PkgName||'.'||c_prcName,
                                      p_Level => Pk01_Syslog.L_err ); */
            
            EXCEPTION
                WHEN no_data_found THEN             
            
                    -- получаем ТП
                    BEGIN
                    
                       -- проверка, есть ли инд. ТП
                        l_RP_Id := lt_Ind_Rp(TO_CHAR(l_cur.acc_ch_id));
                    
                       -- получаем индивидуальный ТП
                        SELECT rp.code,
                               u2d(pae.valid_from) date_from,
                               NVL(u2d(pae.valid_to), gc_MaxDate) date_to 
                          INTO l_RP_Code, l_Date_From, l_Date_To     
                          FROM pin.profile_t@mmtdb p, 
                               pin.profile_acct_extrating_data_t@mmtdb pae,
                               INTEGRATE.IFW_RATEPLAN@mmtdb rp
                         WHERE p.NAME = 'RATEPLAN' 
                           AND pae.obj_id0 = p.poid_id0
                           AND p.account_obj_id0 = l_cur.acc_ch_id
                           AND rp.status = 'A'
                           AND rp.code = pae.VALUE;
                           
                        IF l_Date_From > l_cur.order_date THEN
                            Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                                             ' (acc. ' || l_cur.acc_ch || ')' ||
                                                             ': дата ТП больше даты заказа ' ||
                                                             '(' || TO_CHAR(l_Date_From,'dd.mm.yyyy') ||
                                                             '>' || TO_CHAR(l_cur.order_date,'dd.mm.yyyy') || ')', 
                                                  p_Src   => c_PkgName||'.'||c_prcName,
                                                  p_Level => Pk01_Syslog.L_err );
                                                  
                        ELSIF NVL(l_Date_To, gc_MaxDate) < gc_MaxDate THEN
                                                                                         
                            Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                                             ' (acc. ' || l_cur.acc_ch || ')' ||
                                                             ': дата окончания ТП меньше даты заказа ' ||
                                                             '(' || TO_CHAR(l_Date_To,'dd.mm.yyyy') ||
                                                             '<' || TO_CHAR(gc_MaxDate,'dd.mm.yyyy') || ')', 
                                                  p_Src   => c_PkgName||'.'||c_prcName,
                                                  p_Level => Pk01_Syslog.L_err );            
                        
                        END IF;        
                           
                    EXCEPTION
                        WHEN no_data_found THEN
                        
                            BEGIN
                                SELECT code, date_from, date_to
                                  INTO l_RP_Code, l_Date_From, l_Date_To 
                                  FROM (
                                        SELECT rp.code,
                                               U2D(pp.usage_start_t) date_from,
                                               NVL(U2D(pp.usage_end_t),gc_MaxDate) date_to
                                                               
                                          FROM pin.purchased_product_t@mmtdb pp, 
                                               pin.product_usage_map_t@mmtdb pum,
                                               pin.service_t@mmtdb s,
                                               integrate.ifw_rateplan@mmtdb rp -- описатель rateplan связка с PIN.PRODUCT_USAGE_MAP_T
                                         WHERE 1=1
                                           AND (pp.usage_start_t < pp.usage_end_t OR pp.usage_end_t IS NULL OR pp.usage_end_t = 0)
                                           AND pp.account_obj_id0 = l_cur.acc_ch_id
                                           AND pp.product_obj_id0 = pum.obj_id0 
                                           AND pum.event_type = gc_Event_Type -- '/event/delayed/session/telco/gsm'
                                           AND s.poid_type = gc_Service_Type  -- '/service/telco/gsm/telephony'
                                           AND pp.service_obj_id0 = s.poid_id0 
                                           AND rp.status = 'A'
                                           AND rp.code = pum.rate_plan_name
                                         ORDER BY date_from DESC  
                                       )
                                  WHERE ROWNUM = 1;
                                   
                                IF l_Date_From > l_cur.order_date THEN                   
                                    
                                    Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                                                     ' (acc. ' || l_cur.acc_ch || ')' ||
                                                                     ': дата ТП больше даты заказа ' ||
                                                                     '(' || TO_CHAR(l_Date_From,'dd.mm.yyyy') ||
                                                                     '>' || TO_CHAR(l_cur.order_date,'dd.mm.yyyy') || ')', 
                                                          p_Src   => c_PkgName||'.'||c_prcName,
                                                          p_Level => Pk01_Syslog.L_err );
                                       
                                                                               
                                ELSIF NVL(l_Date_To, gc_MaxDate) < gc_MaxDate THEN
                                                                                                 
                                    Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                                                     ' (acc. ' || l_cur.acc_ch || ')' ||
                                                                     ': дата окончания ТП меньше даты заказа ' ||
                                                                     '(' || TO_CHAR(l_Date_To,'dd.mm.yyyy') ||
                                                                     '<' || TO_CHAR(gc_MaxDate,'dd.mm.yyyy') || ')', 
                                                          p_Src   => c_PkgName||'.'||c_prcName,
                                                          p_Level => Pk01_Syslog.L_err );                       
                                END IF;
                                
                            EXCEPTION
                                WHEN no_data_found THEN
                                    Pk01_Syslog.write_Msg(p_Msg   => 'Не найден ТП для заказа ' || l_cur.order_no ||
                                                                     ' (ACC. ' || l_cur.acc_ch || ')', 
                                                          p_Src   => c_PkgName||'.'||c_prcName,
                                                          p_Level => Pk01_Syslog.L_err );
                                WHEN others THEN
                                    Pk01_Syslog.write_Msg(p_Msg   => 'Ошибка поиска ТП для заказа ' || l_cur.order_no ||
                                                                     ' (ACC. ' || l_cur.acc_ch || ')', 
                                                          p_Src   => c_PkgName||'.'||c_prcName,
                                                          p_Level => Pk01_Syslog.L_err );
                                    RAISE;                                                      
                                                                                                                         
                            END;                       
                        
                    END;           
                    
                    -- получаем идентификатор тарифного плана
                    BEGIN
                    
                        SELECT RATEPLAN_ID 
                          INTO l_RP_Id
                          FROM RATEPLAN_T
                         WHERE RATEPLAN_CODE = NVL(l_RP_Code,'0000000000');
                         
                    EXCEPTION 
                        WHEN NO_DATA_FOUND THEN
                           -- добавляем ТП
                            l_RP_Id := pk17_rateplane.Add_rateplan(
                                           p_rateplan_id    => NULL,  -- ID тарифного планы в системе ведения тарифов
                                           p_tax_incl       => 'Y',     -- Налоги включены в ТП: "Y/N"
                                           p_rateplan_name  => NVL(l_RP_Code,'Неизвестный'), -- имя тарифного плана
                                           p_ratesystem_id  => NULL,   -- ID платежной системы
                                           p_service_id     => NULL,  -- ID услуги
                                           p_subservice_id  => NULL,
                                           p_rateplan_code  => NVL(l_RP_Code,'0000000000')
                                       );
                             
                    END;          
                        
                    -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
                    -- создаем заказ на услуги МГ/МН связи
                    l_Order_Id := PK06_ORDER.New_order(
                                          p_account_id => l_Account_Id,
                                          p_order_no   => l_cur.order_no,
                                          p_service_id => PK00_CONST.c_SERVICE_CALL_MGMN,
                                          p_rateplan_id=> l_RP_Id,
                                          p_time_zone  => NULL,
                                          p_date_from  => l_cur.order_date,
                                          p_date_to    => gc_MaxDate
                                       );
                    -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
                    -- создаем строку заказа для МГ
                    l_Order_Body_Id := PK06_ORDER.Add_subservice(
                                           p_order_id      => l_Order_Id,
                                           p_subservice_id => PK00_CONST.c_SUBSRV_MG,
                                           p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
                                           p_date_from     => l_cur.order_date,
                                           p_date_to       => gc_MaxDate
                                       );
                    -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
                    -- создаем строку заказа для МН
                    l_Order_Body_Id := PK06_ORDER.Add_subservice(
                                           p_order_id      => l_Order_Id,
                                           p_subservice_id => PK00_CONST.c_SUBSRV_MN,
                                           p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
                                           p_date_from     => l_cur.order_date,
                                           p_date_to       => gc_MaxDate
                                       );    
                
                    l_Count := l_Count + 1;
                
                    IF MOD(l_Count, 100) = 0 THEN
                        DBMS_APPLICATION_INFO.SET_ACTION('Add open ord.: ' || TO_CHAR(l_Count));
                    END IF;    
            END;
            
            COMMIT;
            
        EXCEPTION
            WHEN no_data_found THEN
                NULL; -- аккаунта нет в списке л.счетов
        END;            
        
    END LOOP;                               
    
    
    -- Добавление закрытых заказов
    l_Cnt_Closed := 0;
    FOR l_cur IN (
                    SELECT ap.account_no,
                           ac.account_no acc_ch,
                           ac.poid_id0   acc_ch_id,
                           pci.order_num order_no,
                           TRUNC (pin.u2d@mmtdb(MIN(p.effective_t))) order_date_from,
                           TRUNC (pin.u2d@mmtdb(MIN(ac.last_status_t))) + 
                                   INTERVAL '00 23:59:59' DAY TO SECOND order_date_to
                      FROM pin.account_t@mmtdb ac,
                           pin.account_t@mmtdb ap,
                           pin.billinfo_t@mmtdb bic,
                           pin.billinfo_t@mmtdb bip,
                           pin.service_t@mmtdb s,
                           pin.profile_t@mmtdb p,
                           pin.profile_contract_info_t@mmtdb pci
                     WHERE ac.business_type = 1
                       AND ac.status = 10103
                       AND ac.poid_id0 = bic.account_obj_id0
                       AND bic.ar_billinfo_obj_id0 = bip.poid_id0
                       AND bip.account_obj_id0 = ap.poid_id0   
                       AND s.poid_type = gc_Service_Type -- '/service/telco/gsm/telephony'   
                       AND ac.poid_id0 = s.account_obj_id0 
                       AND ac.poid_id0 = p.account_obj_id0 
                       AND p.poid_id0 = pci.obj_id0
                       AND ac.last_status_t >= 1356998400 -- 01.01.2013
                    GROUP BY ap.account_no, ac.account_no, ac.poid_id0,
                             pci.order_num  
                    HAVING NOT EXISTS (SELECT 1
                                         FROM pin.order_t o
                                        WHERE o.order_no = pci.order_num)
                       AND EXISTS (SELECT 1
                                     FROM account_t a
                                    WHERE a.account_type != PK00_CONST.C_ACC_TYPE_J
                                      AND a.account_no = ap.account_no)                                       
                )
    LOOP
    
        -- получаем id л/счета по биллингу
        SELECT account_id 
          INTO l_Account_Id
          FROM account_t a
         WHERE account_no = l_cur.account_no;    
    
        -- получаем ТП
        BEGIN
                    
           -- проверка, есть ли инд. ТП
            l_RP_Id := lt_Ind_Rp(TO_CHAR(l_cur.acc_ch_id));
                    
           -- получаем индивидуальный ТП
            SELECT rp.code,
                   u2d(pae.valid_from) date_from,
                   NVL(u2d(pae.valid_to), gc_MaxDate) date_to 
              INTO l_RP_Code, l_Date_From, l_Date_To     
              FROM pin.profile_t@mmtdb p, 
                   pin.profile_acct_extrating_data_t@mmtdb pae,
                   INTEGRATE.IFW_RATEPLAN@mmtdb rp
             WHERE p.NAME = 'RATEPLAN' 
               AND pae.obj_id0 = p.poid_id0
               AND p.account_obj_id0 = l_cur.acc_ch_id
               AND rp.status = 'A'
               AND rp.code = pae.VALUE;
                           
            IF l_Date_From > l_cur.order_date_from THEN
                Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                                 ' (acc. ' || l_cur.acc_ch || ')' ||
                                                 ': дата ТП больше даты заказа ' ||
                                                 '(' || TO_CHAR(l_Date_From,'dd.mm.yyyy') ||
                                                 '>' || TO_CHAR(l_cur.order_date_from,'dd.mm.yyyy') || ')', 
                                      p_Src   => c_PkgName||'.'||c_prcName,
                                      p_Level => Pk01_Syslog.L_err );
                                                  
            ELSIF NVL(l_Date_To, gc_MaxDate) < l_cur.order_date_to THEN
                                                                                         
                Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                                 ' (acc. ' || l_cur.acc_ch || ')' ||
                                                 ': дата окончания ТП меньше даты заказа ' ||
                                                 '(' || TO_CHAR(l_Date_To,'dd.mm.yyyy') ||
                                                 '<' || TO_CHAR(gc_MaxDate,'dd.mm.yyyy') || ')', 
                                      p_Src   => c_PkgName||'.'||c_prcName,
                                      p_Level => Pk01_Syslog.L_err );            
                        
            END IF;        
                           
        EXCEPTION
            WHEN no_data_found THEN
                        
                BEGIN
                    SELECT code, date_from, date_to
                      INTO l_RP_Code, l_Date_From, l_Date_To 
                      FROM (
                            SELECT rp.code,
                                   U2D(pp.usage_start_t) date_from,
                                   NVL(U2D(pp.usage_end_t),gc_MaxDate) date_to
                                                               
                              FROM pin.purchased_product_t@mmtdb pp, 
                                   pin.product_usage_map_t@mmtdb pum,
                                   pin.service_t@mmtdb s,
                                   integrate.ifw_rateplan@mmtdb rp -- описатель rateplan связка с PIN.PRODUCT_USAGE_MAP_T
                             WHERE 1=1
                               AND (pp.usage_start_t < pp.usage_end_t OR pp.usage_end_t IS NULL OR pp.usage_end_t = 0)
                               AND pp.account_obj_id0 = l_cur.acc_ch_id
                               AND pp.product_obj_id0 = pum.obj_id0 
                               AND pum.event_type = gc_Event_Type -- '/event/delayed/session/telco/gsm'
                               AND s.poid_type = gc_Service_Type  -- '/service/telco/gsm/telephony'
                               AND pp.service_obj_id0 = s.poid_id0 
                               AND rp.status = 'A'
                               AND rp.code = pum.rate_plan_name
                             ORDER BY date_from DESC  
                           )
                      WHERE ROWNUM = 1;
                                   
                    IF l_Date_From > l_cur.order_date_from THEN                   
                                    
                        Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                                         ' (acc. ' || l_cur.acc_ch || ')' ||
                                                         ': дата ТП больше даты заказа ' ||
                                                         '(' || TO_CHAR(l_Date_From,'dd.mm.yyyy') ||
                                                         '>' || TO_CHAR(l_cur.order_date_from,'dd.mm.yyyy') || ')', 
                                              p_Src   => c_PkgName||'.'||c_prcName,
                                              p_Level => Pk01_Syslog.L_err );
                                       
                                                                               
                    ELSIF NVL(l_Date_To, gc_MaxDate) < l_cur.order_date_to THEN
                                                                                                 
                        Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                                         ' (acc. ' || l_cur.acc_ch || ')' ||
                                                         ': дата окончания ТП меньше даты заказа ' ||
                                                         '(' || TO_CHAR(l_Date_To,'dd.mm.yyyy') ||
                                                         '<' || TO_CHAR(gc_MaxDate,'dd.mm.yyyy') || ')', 
                                              p_Src   => c_PkgName||'.'||c_prcName,
                                              p_Level => Pk01_Syslog.L_err );                       
                    END IF;
                                
                EXCEPTION
                    WHEN no_data_found THEN
                        Pk01_Syslog.write_Msg(p_Msg   => 'Не найден ТП для заказа ' || l_cur.order_no ||
                                                         ' (ACC. ' || l_cur.acc_ch || ')', 
                                              p_Src   => c_PkgName||'.'||c_prcName,
                                              p_Level => Pk01_Syslog.L_err );
                    WHEN others THEN
                        Pk01_Syslog.write_Msg(p_Msg   => 'Ошибка поиска ТП для заказа ' || l_cur.order_no ||
                                                         ' (ACC. ' || l_cur.acc_ch || ')', 
                                              p_Src   => c_PkgName||'.'||c_prcName,
                                              p_Level => Pk01_Syslog.L_err );
                        RAISE;                                                      
                                                                                                                         
                END;                       
                        
        END;           
                    
        -- получаем идентификатор тарифного плана
        BEGIN
                    
            SELECT RATEPLAN_ID 
              INTO l_RP_Id
              FROM RATEPLAN_T
             WHERE RATEPLAN_CODE = NVL(l_RP_Code,'0000000000');
                         
        EXCEPTION 
            WHEN NO_DATA_FOUND THEN
               -- добавляем ТП
                l_RP_Id := pk17_rateplane.Add_rateplan(
                               p_rateplan_id    => NULL,  -- ID тарифного планы в системе ведения тарифов
                               p_tax_incl       => 'Y',     -- Налоги включены в ТП: "Y/N"                               
                               p_rateplan_name  => NVL(l_RP_Code,'Неизвестный'), -- имя тарифного плана
                               p_ratesystem_id  => NULL,   -- ID платежной системы
                               p_service_id     => NULL,  -- ID услуги
                               p_subservice_id  => NULL,
                               p_rateplan_code  => NVL(l_RP_Code,'0000000000')
                           );
                             
        END;          
                        
        -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
        -- создаем заказ на услуги МГ/МН связи
        l_Order_Id := PK06_ORDER.New_order(
                              p_account_id => l_Account_Id,
                              p_order_no   => l_cur.order_no,
                              p_service_id => PK00_CONST.c_SERVICE_CALL_MGMN,
                              p_rateplan_id=> l_RP_Id,
                              p_time_zone  => NULL,
                              p_date_from  => l_cur.order_date_from,
                              p_date_to    => l_cur.order_date_to
                           );
        -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
        -- создаем строку заказа для МГ
        l_Order_Body_Id := PK06_ORDER.Add_subservice(
                               p_order_id      => l_Order_Id,
                               p_subservice_id => PK00_CONST.c_SUBSRV_MG,
                               p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
                               p_date_from     => l_cur.order_date_from,
                               p_date_to       => l_cur.order_date_to
                           );
        -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
        -- создаем строку заказа для МН
        l_Order_Body_Id := PK06_ORDER.Add_subservice(
                               p_order_id      => l_Order_Id,
                               p_subservice_id => PK00_CONST.c_SUBSRV_MN,
                               p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
                               p_date_from     => l_cur.order_date_from,
                               p_date_to       => l_cur.order_date_to
                           );    
                
        l_Cnt_Closed := l_Cnt_Closed + 1;
                
        IF MOD(l_Cnt_Closed, 100) = 0 THEN
            DBMS_APPLICATION_INFO.SET_ACTION('Add closed ord.: ' || TO_CHAR(l_Cnt_Closed));
        END IF;    
            
        COMMIT;
            
    END LOOP;                          
    
    Pk01_Syslog.write_Msg(p_Msg => 'добавлено заказов: открытых ' || TO_CHAR(l_Count) || 
                                   ', закрытых ' || TO_CHAR(l_Cnt_Closed), 
                          p_Src => c_PkgName||'.'||c_prcName );               
    
END ReLoad_Orders_Full;

PROCEDURE Load_Orders_Old
IS

    c_prcName CONSTANT varchar2(16) := 'Load_Orders';
    
    l_RP_Id         number;
    l_Calc_Date     date;
    l_Account_Id    number;
    l_Order_Id      number;
    l_Order_Body_Id number;

    l_Count         PLS_INTEGER;
    l_Update        number;
    l_Upd_Acc       number;
    
BEGIN

    Pk01_Syslog.write_Msg(p_Msg => 'Обновления данных по заказам.', 
                          p_Src => c_PkgName||'.'||c_prcName );               


    l_Calc_Date := SYSDATE;

    -- очищаем временную таблицу
    DELETE FROM TMP05_ORDER;

    -- загружаем действующие заказы
    INSERT INTO TMP05_ORDER
           (account_no, acc_ch_id, order_no, date_from, date_to,
            prf_poid_id0, prf_revision)
    SELECT ap.account_no,
           ac.poid_id0,
           pci.order_num order_no,
           TRUNC (u2d (MIN(p.effective_t))) order_date,
           gc_MaxDate,
           p.poid_id0 prf_poid_id0,
           MAX(aup.au_parent_obj_rev) prf_revision            
      FROM pin.account_t@mmtdb ac,
           pin.account_t@mmtdb ap,
           pin.billinfo_t@mmtdb bic,
           pin.billinfo_t@mmtdb bip,
           pin.service_t@mmtdb s,
           pin.profile_t@mmtdb p,
           pin.profile_contract_info_t@mmtdb pci,
           pin.au_profile_t@mmtdb aup 
     WHERE ac.business_type = 1
       AND ac.status != 10103
       AND s.account_obj_id0 = ac.poid_id0
       AND s.poid_type = gc_Service_Type --'/service/telco/gsm/telephony'
       AND p.account_obj_id0 = ac.poid_id0
       AND pci.obj_id0 = p.poid_id0
       AND ac.poid_id0 = bic.account_obj_id0
       AND bic.ar_billinfo_obj_id0 = bip.poid_id0
       AND bip.account_obj_id0 = ap.poid_id0
       AND p.poid_id0 = aup.au_parent_obj_id0(+)
       AND EXISTS (SELECT 1
                     FROM pin.service_alias_list_t@mmtdb sal
                    WHERE sal.obj_id0 = s.poid_id0)   
    GROUP BY ap.account_no,
             ac.poid_id0,
             pci.order_num,
             p.poid_id0;
     
    l_Count := SQL%ROWCOUNT; 
    
   -- загружаем историю по действующим          
    INSERT INTO TMP05_ORDER
           (account_no, acc_ch_id, order_no, date_from, date_to, 
            prf_poid_id0, prf_revision)
    SELECT o.account_no,
           o.acc_ch_id,
           apc.order_num order_no,
           TRUNC (u2d (aup.effective_t)) date_from,
           NULL date_to,
           aup.poid_id0 poid_id0,
           aup.au_parent_obj_rev au_prf_revision
      FROM TMP05_ORDER o,
           pin.au_profile_t@mmtdb aup,
           pin.au_profile_contract_info_t@mmtdb apc
     WHERE o.prf_poid_id0 = aup.au_parent_obj_id0
       AND aup.poid_id0 = apc.obj_id0
       AND o.prf_revision != aup.au_parent_obj_rev
     GROUP BY o.account_no, o.acc_ch_id, o.date_from, apc.order_num, aup.effective_t, o.prf_poid_id0,
              aup.poid_id0, aup.au_parent_obj_rev;
          
    l_Count := l_Count + SQL%ROWCOUNT; 
     
   -- проставляем дату окончания заказам из истории
    MERGE INTO TMP05_ORDER t  
    USING (SELECT rd, new_date_to
             FROM (
                   SELECT rowid rd,
                          lead(date_from-1/86400) OVER (PARTITION BY account_no, acc_ch_id 
                                                            ORDER BY prf_revision) new_date_to,
                          date_to                                     
                     FROM TMP05_ORDER 
                  )
            WHERE date_to IS NULL
          ) tt
       ON (t.rowid = tt.rd)
   WHEN MATCHED THEN UPDATE
    SET t.date_to = tt.new_date_to;                                      
                                         
   -- удалем все некорректные данные 
    DELETE FROM TMP05_ORDER
     WHERE date_from > date_to;
             
    l_Count := l_Count - SQL%ROWCOUNT; 
     
   ---- 
   -- схлопывание диапазонов (много одних и тех же заказов идущих друг за другом подряд)
   
   -- 1. Проставляем минимальную дату начала
    MERGE INTO TMP05_ORDER t
    USING (
           SELECT oo.rd, oo.grp_date
             FROM (
                    SELECT oo.rd, oo.date_from,
                           (date_to - sm) - NUMTODSINTERVAL(rn-1,'SECOND') grp_date,
                           row_number() OVER (PARTITION BY account_no, acc_ch_id, order_no, (date_to - sm) - NUMTODSINTERVAL(rn-1,'SECOND') 
                                                  ORDER BY date_from DESC) rn_grp
                      FROM (
                            SELECT o.rowid rd, 
                                   o.account_no, o.acc_ch_id, o.order_no, o.date_from, o.date_to,
                                   row_number() OVER (PARTITION BY account_no, acc_ch_id, order_no ORDER BY date_from ASC) rn,
                                   SUM(date_to - date_from) OVER (PARTITION BY account_no, acc_ch_id, order_no ORDER BY date_from) sm
                              FROM TMP05_ORDER o
                           ) oo
                  ) oo         
            WHERE rn_grp = 1
              AND grp_date < date_from
         ) tt              
      ON (t.rowid = tt.rd)
    WHEN MATCHED THEN UPDATE
     SET t.date_from = tt.grp_date;
             
   -- 2. удаляем излишки  
    DELETE FROM TMP05_ORDER t
     WHERE EXISTS (SELECT 1
                     FROM TMP05_ORDER tt
                    WHERE t.rowid != tt.rowid
                      AND t.account_no = tt.account_no   
                      AND t.acc_ch_id  = tt.acc_ch_id
                      AND t.order_no   = tt.order_no
                      AND t.date_from  >= tt.date_from
                      AND t.date_to    < tt.date_to
                  );
    
    l_Count := l_Count - SQL%ROWCOUNT;
    
   -------------------------------- 
   -- загружаем закрытые заказы
    INSERT INTO TMP05_ORDER
           (account_no, acc_ch_id, order_no, date_from, date_to)   
    SELECT ap.account_no,
           ac.poid_id0,
           pci.order_num order_no,
           TRUNC (pin.u2d@mmtdb(MIN(p.effective_t))) order_date_from,
           TRUNC (pin.u2d@mmtdb(MIN(ac.last_status_t))) + 
                               INTERVAL '00 23:59:59' DAY TO SECOND order_date_to
      FROM pin.account_t@mmtdb ac,
           pin.account_t@mmtdb ap,
           pin.billinfo_t@mmtdb bic,
           pin.billinfo_t@mmtdb bip,
           pin.service_t@mmtdb s,
           pin.profile_t@mmtdb p,
           pin.profile_contract_info_t@mmtdb pci
     WHERE ac.business_type = 1
       AND ac.status = 10103
       AND ac.poid_id0 = bic.account_obj_id0
       AND bic.ar_billinfo_obj_id0 = bip.poid_id0
       AND bip.account_obj_id0 = ap.poid_id0   
       AND s.poid_type = gc_Service_Type -- '/service/telco/gsm/telephony'   
       AND ac.poid_id0 = s.account_obj_id0 
       AND ac.poid_id0 = p.account_obj_id0 
       AND p.poid_id0 = pci.obj_id0
       AND ac.last_status_t >= 1262304000 -- 01.01.2010
       AND (EXISTS (SELECT 1
                      FROM pin.service_alias_list_t@mmtdb sal
                     WHERE sal.obj_id0 = s.poid_id0)     
             OR
            EXISTS (SELECT 1
                      FROM pin.au_service_t@mmtdb aus,
                           pin.au_service_alias_list_t@mmtdb sal
                     WHERE aus.poid_id0 = sal.obj_id0
                       AND aus.au_parent_obj_id0 = s.poid_id0)         
          )            
    GROUP BY ap.account_no, ac.account_no, ac.poid_id0,
             pci.order_num  
    HAVING NOT EXISTS (SELECT 1
                         FROM TMP05_ORDER o
                        WHERE o.order_no = pci.order_num)
       AND EXISTS (SELECT 1
                     FROM account_t a
                    WHERE a.account_type != PK00_CONST.C_ACC_TYPE_J
                      AND a.account_no = ap.account_no);   

    l_Count := l_Count + SQL%ROWCOUNT;

    -- удаляем задвои
    DELETE FROM TMP05_ORDER o
     WHERE rowid IN (SELECT rd
                           FROM (
                                 SELECT rowid rd,
                                        row_number() OVER (PARTITION BY order_no ORDER BY rp_date_from DESC) rn 
                                   FROM TMP05_ORDER t
                                )   
                          WHERE rn > 1  
                        );
                        
    l_Count := l_Count - SQL%ROWCOUNT;                        

    Pk01_Syslog.write_Msg(p_Msg => 'Найдено заказов в НБ: ' || TO_CHAR(l_Count), 
                          p_Src => c_PkgName||'.'||c_prcName );               


   -- Проверка, все ли заказы найдены
/*    FOR l_cur IN (SELECT o.order_no
                    FROM ORDER_T o, 
                         ACCOUNT_T a
                   WHERE a.account_type = 'P'
                     AND o.account_id = a.account_id
                     AND o.order_no NOT LIKE pin.pk00_const.c_Order_Test_Pref || '%'
                     AND o.order_no NOT IN ('ACC000175785-TEST','ACC000390969-1')
                     AND NOT EXISTS (SELECT 1
                                       FROM TMP05_ORDER t
                                      WHERE t.order_no = o.order_no)
                 )                         
    LOOP
    
        Pk01_Syslog.write_Msg(p_Msg   => 'Не найден в НБ заказ: ' || l_cur.order_no, 
                              p_Src   => c_PkgName||'.'||c_prcName,
                              p_Level => pk01_syslog.L_err
                             );
                                                
    END LOOP;*/
                      
                     
    --- ================================================
    -- Проверка совпадения л/счетов у заказа
    UPDATE ORDER_T o
       SET (account_id, notes, modify_date) = 
                                (SELECT a.account_id, TO_CHAR(t.acc_ch_id), l_Calc_Date
                                   FROM TMP05_ORDER t,
                                        ACCOUNT_T a
                                  WHERE t.account_no = a.account_no
                                    AND t.order_no = o.order_no    
                                    AND (a.account_id != o.account_id
                                          OR
                                         NVL(o.notes,'-1') != TO_CHAR(t.acc_ch_id)
                                        )  
                                 )     
    WHERE EXISTS (SELECT 1
                    FROM TMP05_ORDER t,
                         ACCOUNT_T a
                   WHERE t.account_no = a.account_no
                     AND t.order_no = o.order_no    
                     AND (a.account_id != o.account_id
                           OR
                          NVL(o.notes,'-1') != TO_CHAR(t.acc_ch_id)
                         )  
                  );     
    
    l_Upd_Acc := SQL%ROWCOUNT;
        
    -- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    -- получаем ТП
    ----
    --   получаем индивидуальный ТП
    UPDATE TMP05_ORDER o
       SET (rp_code, rp_date_from, rp_date_to) = 
                     (SELECT rp.code,
                             u2d(pae.valid_from) date_from,
                             NVL(u2d(pae.valid_to), gc_MaxDate) date_to 
                        FROM pin.profile_t@mmtdb p, 
                             pin.profile_acct_extrating_data_t@mmtdb pae,
                             INTEGRATE.IFW_RATEPLAN@mmtdb rp
                       WHERE p.NAME = 'RATEPLAN' 
                         AND pae.obj_id0 = p.poid_id0
                         AND p.account_obj_id0 = o.acc_ch_id
                         AND rp.code = pae.VALUE
                     )
      WHERE EXISTS (SELECT rp.code,
                           u2d(pae.valid_from) date_from,
                           NVL(u2d(pae.valid_to), gc_MaxDate) date_to 
                      FROM pin.profile_t@mmtdb p, 
                           pin.profile_acct_extrating_data_t@mmtdb pae,
                           INTEGRATE.IFW_RATEPLAN@mmtdb rp
                     WHERE p.NAME = 'RATEPLAN' 
                       AND pae.obj_id0 = p.poid_id0
                       AND p.account_obj_id0 = o.acc_ch_id
                       AND rp.code = pae.VALUE
                   );    
    
     -- получаем ТП для л/с, у которых нет индивидуального    
    UPDATE TMP05_ORDER o
       SET (rp_code, rp_date_from, rp_date_to) =
                                (SELECT code, date_from, date_to
                                  FROM (
                                        SELECT pp.account_obj_id0,
                                               pum.rate_plan_name code,
                                               U2D(pp.usage_start_t) date_from,
                                               NVL(U2D(pp.usage_end_t),gc_MaxDate) date_to,
                                               row_number() OVER (PARTITION BY pp.account_obj_id0
                                                                      ORDER BY pp.usage_start_t DESC) rn                 
                                          FROM pin.purchased_product_t@mmtdb pp, 
                                               pin.product_usage_map_t@mmtdb pum,
                                               pin.service_t@mmtdb s
                                         WHERE 1=1
                                           AND (pp.usage_start_t < pp.usage_end_t OR pp.usage_end_t IS NULL OR pp.usage_end_t = 0)
                                         --  AND pp.account_obj_id0 = l_cur.acc_ch_id
                                           AND pp.product_obj_id0 = pum.obj_id0 
                                           AND pum.event_type = '/event/delayed/session/telco/gsm'
                                           AND s.poid_type = '/service/telco/gsm/telephony'
                                           AND pp.service_obj_id0 = s.poid_id0 
                                       ) t
                                  WHERE t.RN = 1
                                    AND t.account_obj_id0 = o.acc_ch_id
                                )
     WHERE rp_code IS NULL;
                                                
     
    -- тарифы из таблиц аудита
    UPDATE TMP05_ORDER o
       SET (rp_code, rp_date_from, rp_date_to) =
                                (SELECT code, date_from, date_to
                                  FROM (
                                        SELECT pp.account_obj_id0,
                                               pum.rate_plan_name code,
                                               U2D(pp.usage_start_t) date_from,
                                               NVL(U2D(pp.usage_end_t),gc_MaxDate) date_to,
                                               row_number() OVER (PARTITION BY pp.account_obj_id0
                                                                      ORDER BY pp.au_parent_obj_rev DESC, pp.usage_start_t DESC) rn                 
                                          FROM TMP05_ORDER o,
                                               pin.au_purchased_product_t@mmtdb pp, 
                                               pin.product_usage_map_t@mmtdb pum,
                                               pin.service_t@mmtdb s
                                         WHERE 1=1
                                           AND o.rp_code IS NULL
                                           AND o.acc_ch_id = pp.account_obj_id0 
                                           AND (pp.usage_start_t < pp.usage_end_t OR pp.usage_end_t IS NULL OR pp.usage_end_t = 0)
                                         --  AND pp.account_obj_id0 = l_cur.acc_ch_id
                                           AND pp.product_obj_id0 = pum.obj_id0 
                                           AND pum.event_type = '/event/delayed/session/telco/gsm'
                                           AND s.poid_type = '/service/telco/gsm/telephony'
                                           AND pp.service_obj_id0 = s.poid_id0 
                                       ) t
                                  WHERE t.RN = 1
                                    AND t.account_obj_id0 = o.acc_ch_id
                                )
     WHERE rp_code IS NULL;      
     
     
    -- Проверка - если есть у заказа разные ТП, то это ошибка
    FOR l_cur IN (SELECT o.order_no, COUNT(1) cnt
                    FROM (
                         SELECT o.order_no, rp_code
                            FROM TMP05_ORDER o
                           GROUP BY o.order_no, rp_code
                         ) o
                   GROUP BY o.order_no       
                   HAVING COUNT(1) > 1
                )
    LOOP                         

        Pk01_Syslog.write_Msg(p_Msg   => 'У заказа ' || l_cur.order_no ||
                                         ' найдено ' || TO_CHAR(l_cur.cnt) || ' тар. планов)', 
                              p_Src   => c_PkgName||'.'||c_prcName,
                              p_Level => Pk01_Syslog.L_err );
                
    END LOOP;    

    -- = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    --- Догружаем ТП, которых еще нет в ЦБ    
    FOR l_cur IN (SELECT o.rp_code, rp.NAME
                    FROM TMP05_ORDER o,
                         INTEGRATE.IFW_RATEPLAN@mmtdb rp
                   WHERE NOT EXISTS (SELECT 1
                                       FROM rateplan_t r
                                      WHERE r.rateplan_code = NVL(o.rp_code,'0000000000'))
                     AND o.rp_code = rp.code                  
                   GROUP BY o.rp_code, rp.NAME                 
                 )
    LOOP
    
       -- добавляем ТП
        l_RP_Id := pk17_rateplane.Add_rateplan(
                       p_rateplan_id    => NULL,  -- ID тарифного планы в системе ведения тарифов
                       p_tax_incl       => 'Y',     -- Налоги включены в ТП: "Y/N"
                       p_rateplan_name  => NVL(l_cur.NAME,'Неизвестный'), -- имя тарифного плана
                       p_ratesystem_id  => NULL,   -- ID платежной системы
                       p_service_id     => NULL,  -- ID услуги
                       p_subservice_id  => NULL,                       
                       p_rateplan_code  => NVL(l_cur.RP_Code,'0000000000')
                   );    
    
    END LOOP;                                  
     
   
    -- получаем id тарифных планов по трешеру
    UPDATE TMP05_ORDER o
       SET o.rateplan_id = (SELECT r.rateplan_id
                            FROM rateplan_t r 
                           WHERE r.rateplan_code = o.rp_code);
    
   -- обновляем заказы, у которых были изменения 
    UPDATE order_t o
       SET (o.date_from, o.date_to, o.rateplan_id, modify_date) =
                  (SELECT t.date_from, t.date_to, t.rateplan_id, l_Calc_Date
                     FROM TMP05_ORDER t
                    WHERE o.order_no = t.order_no
                  )         
     WHERE EXISTS (SELECT 1
                     FROM TMP05_ORDER t
                    WHERE o.order_no = t.order_no
                      AND (o.date_from   != t.date_from OR     
                           o.date_to     != t.date_to   OR
                           o.rateplan_id != t.rateplan_id 
                          )
                  );
                  
    l_Update := SQL%ROWCOUNT;              
                  
    -- добавляем новые             
    l_Count := 0;
      
    FOR l_cur IN (  SELECT account_no, order_no, acc_ch_id,
                           date_from, date_to,
                           rateplan_id
                      FROM TMP05_ORDER t
                     WHERE NOT EXISTS (SELECT 1
                                         FROM order_t o
                                        WHERE o.order_no = t.order_no)          
                 )
    LOOP
    
        BEGIN
            -- проверяем наличие л/счета в текущем списке
            SELECT account_id 
              INTO l_Account_Id
              FROM account_t a
             WHERE account_no = l_cur.account_no;
    
            -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
            -- создаем заказ на услуги МГ/МН связи
            l_Order_Id := PK06_ORDER.New_order(
                                  p_account_id  => l_Account_Id,
                                  p_order_no    => l_cur.order_no,
                                  p_service_id  => PK00_CONST.c_SERVICE_CALL_MGMN,
                                  p_rateplan_id => l_cur.rateplan_id,
                                  p_time_zone   => NULL,
                                  p_date_from   => l_cur.date_from,
                                  p_date_to     => l_cur.date_to,
                                  p_create_date => l_Calc_Date,
                                  p_note        => TO_CHAR(l_cur.acc_ch_id)
                               );
            -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
            -- создаем строку заказа для МГ
            l_Order_Body_Id := PK06_ORDER.Add_subservice(
                                   p_order_id      => l_Order_Id,
                                   p_subservice_id => PK00_CONST.c_SUBSRV_MG,
                                   p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
                                   p_date_from     => l_cur.date_from,
                                   p_date_to       => l_cur.date_to
                               );
            -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
            -- создаем строку заказа для МН
            l_Order_Body_Id := PK06_ORDER.Add_subservice(
                                   p_order_id      => l_Order_Id,
                                   p_subservice_id => PK00_CONST.c_SUBSRV_MN,
                                   p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
                                   p_date_from     => l_cur.date_from,
                                   p_date_to       => l_cur.date_to
                               );    
                
            l_Count := l_Count + 1;
                
        EXCEPTION
            WHEN no_data_found THEN
                Pk01_Syslog.write_Msg(p_Msg   => 'Л/счет ' || l_cur.account_no ||
                                                 ' не найден в списке.', 
                                      p_Src   => c_PkgName||'.'||c_prcName,
                                      p_Level => Pk01_Syslog.L_err );
        END;            
        
    END LOOP;                       
                           
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - -  -
    -- Проверка данных и логирование ошибок
    ---
     --- Заказы у которых нет ТП
    FOR l_cur IN (SELECT t.acc_ch_id, t.order_no, t.rateplan_id, 
                         t.date_from, t.rp_date_from, t.date_to, t.rp_date_to
                    FROM TMP05_ORDER t
                   WHERE (t.rateplan_id IS NULL OR
                          t.date_from < t.rp_date_from OR 
                          t.date_to > t.rp_date_to)
                     AND EXISTS (SELECT 1
                                   FROM order_t o
                                  WHERE o.modify_date = l_Calc_date
                                    AND o.order_no = t.order_no)  
                   ORDER BY t.rateplan_id NULLS LAST
                 )   
    LOOP

        IF l_cur.rateplan_id IS NULL THEN

            Pk01_Syslog.write_Msg(p_Msg   => 'Не найден ТП для заказа ' || l_cur.order_no ||
                                             ' (Acc.ch.: ' || l_cur.acc_ch_id || ')', 
                                  p_Src   => c_PkgName||'.'||c_prcName,
                                  p_Level => Pk01_Syslog.L_err );
                                  
      /*  ELSIF trunc(l_cur.rp_date_from) > trunc(l_cur.date_from) THEN                   
                                    
            Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                             ' (acc. ' || l_cur.acc_ch_id || ')' ||
                                             ': дата ТП больше даты заказа ' ||
                                             '(' || TO_CHAR(l_cur.rp_date_from,'dd.mm.yyyy') ||
                                             '>' || TO_CHAR(l_cur.date_from,'dd.mm.yyyy') || ')', 
                                  p_Src   => c_PkgName||'.'||c_prcName,
                                  p_Level => Pk01_Syslog.L_err ); */
                                                                                                                      
        ELSIF TRUNC(l_cur.rp_date_to) < TRUNC(l_cur.date_to) THEN
                                                                                                 
            Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                             ' (acc. ' || l_cur.acc_ch_id || ')' ||
                                             ': дата окончания ТП меньше даты заказа ' ||
                                             '(' || TO_CHAR(l_cur.rp_date_to,'dd.mm.yyyy') ||
                                             '<' || TO_CHAR(l_cur.date_to,'dd.mm.yyyy') || ')', 
                                  p_Src   => c_PkgName||'.'||c_prcName,
                                  p_Level => Pk01_Syslog.L_err );                       
        END IF;                                  

    END LOOP;
    
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - -  -

    COMMIT;

    Pk01_Syslog.write_Msg(p_Msg => 'Добавлено заказов: ' || TO_CHAR(l_Count) || 
                                   ', обновлено: ' || TO_CHAR(l_Update) ||
                                   ', изм. привязка заказ-л/с: ' || TO_CHAR(l_Upd_Acc), 
                          p_Src => c_PkgName||'.'||c_prcName );               
    
END Load_Orders_Old;



PROCEDURE Load_Orders
IS

    c_prcName CONSTANT varchar2(16) := 'Load_Orders';
    
    l_RP_Id         number;
    l_Calc_Date     date;
    l_Account_Id    number;
    l_Order_Id      number;
    l_Order_Body_Id number;

    l_Count         PLS_INTEGER;
    l_Update        number;
    l_Upd_Acc       number;
    
    l_NF_F          PLS_INTEGER := 0;
    l_NF_Y          PLS_INTEGER := 0;
    
BEGIN

    Pk01_Syslog.write_Msg(p_Msg => 'Обновления данных по заказам.', 
                          p_Src => c_PkgName||'.'||c_prcName );               


    l_Calc_Date := SYSDATE;

    -- очищаем временную таблицу
    DELETE FROM TMP05_ORDER;

    -- загружаем действующие заказы
    INSERT INTO TMP05_ORDER
           (account_no, acc_ch_id, order_no, date_from, date_to,
            prf_poid_id0, prf_revision,
            service_id,
            business_type)
    SELECT ap.account_no,
           ac.poid_id0,
           pci.order_num order_no,
           TRUNC (u2d (MIN(p.effective_t))) order_date,
           gc_MaxDate,
           p.poid_id0 prf_poid_id0,
           MAX(aup.au_parent_obj_rev) prf_revision,
           DECODE(s.poid_type, gc_Service_Type, PK00_CONST.c_SERVICE_CALL_MGMN, 
                               gc_SrvFree_Type, PK00_CONST.c_SERVICE_CALL_FREE,
                               gc_SrvZone_Type, PK00_CONST.c_SERVICE_CALL_ZONE
                 ) service_id,
           ac.business_type      
      FROM pin.account_t@mmtdb ac,
           pin.account_t@mmtdb ap,
           pin.billinfo_t@mmtdb bic,
           pin.billinfo_t@mmtdb bip,
           pin.service_t@mmtdb s,
           pin.profile_t@mmtdb p,
           pin.profile_contract_info_t@mmtdb pci,
           pin.au_profile_t@mmtdb aup 
     WHERE ac.business_type IN (1, 2) -- 1 - физики, 2 - юрики
       AND ac.status != 10103
       AND s.account_obj_id0 = ac.poid_id0
       AND s.poid_type IN (gc_Service_Type, gc_SrvFree_Type) --, gc_SrvZone_Type) --'/service/telco/gsm/telephony'
       AND p.account_obj_id0 = ac.poid_id0
       AND pci.obj_id0 = p.poid_id0
       AND ac.poid_id0 = bic.account_obj_id0
       AND bic.ar_billinfo_obj_id0 = bip.poid_id0
       AND bip.account_obj_id0 = ap.poid_id0
       AND p.poid_id0 = aup.au_parent_obj_id0(+)
       AND EXISTS (SELECT 1
                     FROM pin.service_alias_list_t@mmtdb sal
                    WHERE sal.obj_id0 = s.poid_id0)   
    GROUP BY ap.account_no,
             ac.poid_id0,
             pci.order_num,
             p.poid_id0,
             s.poid_type, ac.business_type;
     
    l_Count := SQL%ROWCOUNT; 
    
   -- загружаем историю по действующим          
    INSERT INTO TMP05_ORDER
           (account_no, acc_ch_id, order_no, date_from, date_to, 
            prf_poid_id0, prf_revision,
            service_id, business_type)
    SELECT o.account_no,
           o.acc_ch_id,
           apc.order_num order_no,
           TRUNC (u2d (aup.effective_t)) date_from,
           NULL date_to,
           aup.poid_id0 poid_id0,
           aup.au_parent_obj_rev au_prf_revision,
           o.service_id, o.business_type
      FROM TMP05_ORDER o,
           pin.au_profile_t@mmtdb aup,
           pin.au_profile_contract_info_t@mmtdb apc
     WHERE o.prf_poid_id0 = aup.au_parent_obj_id0
       AND aup.poid_id0 = apc.obj_id0
       AND o.prf_revision != aup.au_parent_obj_rev
     GROUP BY o.account_no, o.acc_ch_id, o.date_from, apc.order_num, aup.effective_t, o.prf_poid_id0,
              aup.poid_id0, aup.au_parent_obj_rev, 
              o.service_id, o.business_type;
          
    l_Count := l_Count + SQL%ROWCOUNT; 
     
   -- проставляем дату окончания заказам из истории
    MERGE INTO TMP05_ORDER t  
    USING (SELECT rd, new_date_to
             FROM (
                   SELECT rowid rd,
                          lead(date_from-1/86400) OVER (PARTITION BY account_no, acc_ch_id 
                                                            ORDER BY prf_revision) new_date_to,
                          date_to                                     
                     FROM TMP05_ORDER 
                  )
            WHERE date_to IS NULL
          ) tt
       ON (t.rowid = tt.rd)
   WHEN MATCHED THEN UPDATE
    SET t.date_to = tt.new_date_to;                                      
                                         
   -- удалем все некорректные данные 
    DELETE FROM TMP05_ORDER
     WHERE date_from > date_to;
             
    l_Count := l_Count - SQL%ROWCOUNT; 
     
   ---- 
   -- схлопывание диапазонов (много одних и тех же заказов идущих друг за другом подряд)
   
   -- 1. Проставляем минимальную дату начала
    MERGE INTO TMP05_ORDER t
    USING (
           SELECT oo.rd, oo.grp_date
             FROM (
                    SELECT oo.rd, oo.date_from,
                           (date_to - sm) - NUMTODSINTERVAL(rn-1,'SECOND') grp_date,
                           row_number() OVER (PARTITION BY account_no, acc_ch_id, order_no, (date_to - sm) - NUMTODSINTERVAL(rn-1,'SECOND') 
                                                  ORDER BY date_from DESC) rn_grp
                      FROM (
                            SELECT o.rowid rd, 
                                   o.account_no, o.acc_ch_id, o.order_no, o.date_from, o.date_to,
                                   row_number() OVER (PARTITION BY account_no, acc_ch_id, order_no ORDER BY date_from ASC) rn,
                                   SUM(date_to - date_from) OVER (PARTITION BY account_no, acc_ch_id, order_no ORDER BY date_from) sm
                              FROM TMP05_ORDER o
                           ) oo
                  ) oo         
            WHERE rn_grp = 1
              AND grp_date < date_from
         ) tt              
      ON (t.rowid = tt.rd)
    WHEN MATCHED THEN UPDATE
     SET t.date_from = tt.grp_date;
             
   -- 2. удаляем излишки  
    DELETE FROM TMP05_ORDER t
     WHERE EXISTS (SELECT 1
                     FROM TMP05_ORDER tt
                    WHERE t.rowid != tt.rowid
                      AND t.account_no = tt.account_no   
                      AND t.acc_ch_id  = tt.acc_ch_id
                      AND t.order_no   = tt.order_no
                      AND t.date_from  >= tt.date_from
                      AND t.date_to    < tt.date_to
                  );
    
    l_Count := l_Count - SQL%ROWCOUNT;
    
   -------------------------------- 
   -- загружаем закрытые заказы
    INSERT INTO TMP05_ORDER
           (account_no, acc_ch_id, order_no, date_from, date_to,
            service_id, business_type)   
    SELECT ap.account_no,
           ac.poid_id0,
           pci.order_num order_no,
           TRUNC (pin.u2d@mmtdb(MIN(p.effective_t))) order_date_from,
           TRUNC (pin.u2d@mmtdb(MIN(ac.last_status_t))) + 
                               INTERVAL '00 23:59:59' DAY TO SECOND order_date_to,
           DECODE(s.poid_type, gc_Service_Type, PK00_CONST.c_SERVICE_CALL_MGMN, 
                               gc_SrvFree_Type, PK00_CONST.c_SERVICE_CALL_FREE,
                               gc_SrvZone_Type, PK00_CONST.c_SERVICE_CALL_ZONE
                 ) service_id,
           ac.business_type                
      FROM pin.account_t@mmtdb ac,
           pin.account_t@mmtdb ap,
           pin.billinfo_t@mmtdb bic,
           pin.billinfo_t@mmtdb bip,
           pin.service_t@mmtdb s,
           pin.profile_t@mmtdb p,
           pin.profile_contract_info_t@mmtdb pci
     WHERE ac.business_type IN (1, 2) -- 1 - физики, 2 - юрики
       AND ac.status = 10103
       AND ac.poid_id0 = bic.account_obj_id0
       AND bic.ar_billinfo_obj_id0 = bip.poid_id0
       AND bip.account_obj_id0 = ap.poid_id0   
       AND s.poid_type IN (gc_Service_Type, gc_SrvFree_Type) --, gc_SrvZone_Type) -- '/service/telco/gsm/telephony'   
       AND ac.poid_id0 = s.account_obj_id0 
       AND ac.poid_id0 = p.account_obj_id0 
       AND p.poid_id0 = pci.obj_id0
       AND ac.last_status_t >= 1254355200 -- 01.10.2009
       AND (EXISTS (SELECT 1
                      FROM pin.service_alias_list_t@mmtdb sal
                     WHERE sal.obj_id0 = s.poid_id0)     
             OR
            EXISTS (SELECT 1
                      FROM pin.au_service_t@mmtdb aus,
                           pin.au_service_alias_list_t@mmtdb sal
                     WHERE aus.poid_id0 = sal.obj_id0
                       AND aus.au_parent_obj_id0 = s.poid_id0)         
          )            
    GROUP BY ap.account_no, ac.account_no, ac.poid_id0,
             pci.order_num, s.poid_type, ac.business_type  
    HAVING NOT EXISTS (SELECT 1
                         FROM TMP05_ORDER o
                        WHERE o.order_no = pci.order_num)
       AND EXISTS (SELECT 1
                     FROM account_t a
                    WHERE /*a.account_type != PK00_CONST.C_ACC_TYPE_J
                      AND*/ a.account_no = ap.account_no);   

    l_Count := l_Count + SQL%ROWCOUNT;

    -- удаляем задвои
    DELETE FROM TMP05_ORDER o
     WHERE rowid IN (SELECT rd
                           FROM (
                                 SELECT rowid rd,
                                        row_number() OVER (PARTITION BY order_no ORDER BY rp_date_from DESC) rn 
                                   FROM TMP05_ORDER t
                                )   
                          WHERE rn > 1  
                        );
                        
    l_Count := l_Count - SQL%ROWCOUNT;                        

   -- удалем все некорректные данные 
    DELETE FROM TMP05_ORDER
     WHERE date_from > date_to;
     
    l_Count := l_Count - SQL%ROWCOUNT; 

    Pk01_Syslog.write_Msg(p_Msg => 'Найдено заказов в НБ: ' || TO_CHAR(l_Count), 
                          p_Src => c_PkgName||'.'||c_prcName );               


    --- ================================================
    -- Проверка совпадения л/счетов у заказа
    UPDATE ORDER_T o
       SET (account_id, notes, modify_date) = 
                                (SELECT a.account_id, TO_CHAR(t.acc_ch_id), l_Calc_Date
                                   FROM TMP05_ORDER t,
                                        ACCOUNT_T a
                                  WHERE t.account_no = a.account_no
                                    AND t.order_no = o.order_no    
                                    AND (a.account_id != o.account_id
                                          OR
                                         NVL(o.notes,'-1') != TO_CHAR(t.acc_ch_id)
                                        )  
                                 )     
    WHERE EXISTS (SELECT 1
                    FROM TMP05_ORDER t,
                         ACCOUNT_T a
                   WHERE t.account_no = a.account_no
                     AND t.order_no = o.order_no    
                     AND (a.account_id != o.account_id
                           OR
                          NVL(o.notes,'-1') != TO_CHAR(t.acc_ch_id)
                         )  
                  );     
    
    l_Upd_Acc := SQL%ROWCOUNT;
        
    -- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    -- получаем ТП
    ----
    --   получаем индивидуальный ТП (делаем через пром. таблицу - так намного меньше тормозов из-за линков
    DELETE FROM TMP09_ORDER_PH;

    INSERT INTO TMP09_ORDER_PH
    (account_id, rp_code, date_from, date_to)                   
    SELECT p.account_obj_id0,
           rp.code,
               u2d(pae.valid_from) date_from,
               NVL(u2d(pae.valid_to), gc_MaxDate) date_to 
          FROM pin.profile_t@mmtdb p, 
               pin.profile_acct_extrating_data_t@mmtdb pae,
               INTEGRATE.IFW_RATEPLAN@mmtdb rp
         WHERE p.NAME = 'RATEPLAN' 
           AND pae.obj_id0 = p.poid_id0
           AND rp.code = pae.VALUE;  


    UPDATE TMP05_ORDER o
           SET (rp_code, rp_date_from, rp_date_to) = 
                         (SELECT t.rp_code,
                                 t.date_from,
                                 t.date_to 
                            FROM TMP09_ORDER_PH t
                           WHERE t.account_id = o.acc_ch_id
                             AND o.date_from < t.date_to 
                             AND o.date_to   > t.date_from                   
                         )
          WHERE o.rp_code IS NULL
            AND EXISTS (SELECT 1 
                          FROM TMP09_ORDER_PH t
                         WHERE t.account_id = o.acc_ch_id
                             AND o.date_from < t.date_to 
                             AND o.date_to   > t.date_from                    
                       );
 
    
     -- получаем ТП для л/с, у которых нет индивидуального    
     -- (делаем через пром. таблицу - так намного меньше тормозов из-за линков)
    DELETE FROM TMP09_ORDER_PH;

    INSERT INTO TMP09_ORDER_PH
    (account_id, rp_code, date_from, date_to)
        SELECT pp.account_obj_id0,
               pum.rate_plan_name code,
               U2D(pp.usage_start_t) date_from,
               NVL(U2D(pp.usage_end_t),gc_MaxDate) date_to
          FROM pin.purchased_product_t@mmtdb pp, 
               pin.product_usage_map_t@mmtdb pum,
               pin.service_t@mmtdb s
         WHERE 1=1
           AND (pp.usage_start_t < pp.usage_end_t OR pp.usage_end_t IS NULL OR pp.usage_end_t = 0)
           AND pp.product_obj_id0 = pum.obj_id0 
           AND pum.event_type = '/event/delayed/session/telco/gsm'
           AND s.poid_type IN (gc_Service_Type, gc_SrvFree_Type) --, :gc_SrvZone_Type)
           AND pp.service_obj_id0 = s.poid_id0;

                       
    MERGE INTO TMP05_ORDER o
     USING      
            (SELECT rd, rp_code, date_from, date_to
              FROM 
                   (
                    SELECT o.rowid rd, -- order_no, который в partition, уникальный, посему берем для скорости rowid 
                           t.rp_code,
                           t.date_from,
                           t.date_to,
                           row_number() OVER (PARTITION BY o.order_no
                                                  ORDER BY t.date_from DESC) rn                  
                      FROM TMP09_ORDER_PH t,
                           TMP05_ORDER o
                     WHERE 1=1
                       AND o.rp_code IS NULL
                       AND o.acc_ch_id = t.account_id 
                       AND o.date_from < t.date_to 
                       AND o.date_to   >= t.date_from  
                   ) t 
              WHERE t.RN = 1
            ) t
      ON (o.rowid = t.rd)
    WHEN MATCHED THEN UPDATE          
    SET o.rp_code = t.rp_code, 
        o.rp_date_from = t.date_from, 
        o.rp_date_to = t.date_to;                                      
     
    -- тарифы из таблиц аудита
    -- (делаем через пром. таблицу - так намного меньше тормозов из-за линков)
    DELETE FROM TMP09_ORDER_PH;
     
    INSERT INTO TMP09_ORDER_PH
    (account_id, rp_code, date_from, date_to)    
    SELECT --o.rowid rd,
           pp.account_obj_id0,
           pum.rate_plan_name code,
           U2D(pp.usage_start_t) date_from,
           NVL(U2D(pp.usage_end_t), gc_MaxDate) date_to
      FROM --TMP05_ORDER o,
           pin.au_purchased_product_t@mmtdb pp, 
           pin.product_usage_map_t@mmtdb pum,
           pin.service_t@mmtdb s
     WHERE 1=1
       --AND o.acc_ch_id = pp.account_obj_id0 
       AND (pp.usage_start_t < pp.usage_end_t OR pp.usage_end_t IS NULL OR pp.usage_end_t = 0)
     --  AND pp.account_obj_id0 = l_cur.acc_ch_id
       AND pp.product_obj_id0 = pum.obj_id0 
       AND pum.event_type = '/event/delayed/session/telco/gsm'
       AND s.poid_type IN (gc_Service_Type, gc_SrvFree_Type) --, gc_SrvZone_Type)
       AND pp.service_obj_id0 = s.poid_id0;

    MERGE INTO TMP05_ORDER o
     USING      
            (SELECT rd, rp_code, date_from, date_to
              FROM 
                   (
                    SELECT o.rowid rd, -- order_no, который в partition, уникальный, посему берем для скорости rowid
                           t.rp_code,
                           t.date_from,
                           t.date_to,
                           row_number() OVER (PARTITION BY o.order_no
                                                  ORDER BY t.date_from DESC) rn                  
                      FROM TMP09_ORDER_PH t,
                           TMP05_ORDER o
                     WHERE 1=1
                       AND o.rp_code IS NULL
                       AND o.acc_ch_id = t.account_id 
                       AND o.date_from < t.date_to 
                       AND o.date_to   >= t.date_from  
                   ) t 
              WHERE t.RN = 1
            ) t
      ON (o.rowid = t.rd)
    WHEN MATCHED THEN UPDATE          
    SET o.rp_code = t.rp_code, 
        o.rp_date_from = t.date_from, 
        o.rp_date_to = t.date_to;      
     
     
    -- Проверка - если есть у заказа разные ТП, то это ошибка
    FOR l_cur IN (SELECT o.order_no, COUNT(1) cnt
                    FROM (
                         SELECT o.order_no, rp_code
                            FROM TMP05_ORDER o
                           GROUP BY o.order_no, rp_code
                         ) o
                   GROUP BY o.order_no       
                   HAVING COUNT(1) > 1
                )
    LOOP                         

        Pk01_Syslog.write_Msg(p_Msg   => 'У заказа ' || l_cur.order_no ||
                                         ' найдено ' || TO_CHAR(l_cur.cnt) || ' тар. планов)', 
                              p_Src   => c_PkgName||'.'||c_prcName,
                              p_Level => Pk01_Syslog.L_err );
                
    END LOOP;    

    -- = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    --- Догружаем ТП, которых еще нет в ЦБ    
    FOR l_cur IN (SELECT o.rp_code, rp.NAME
                    FROM TMP05_ORDER o,
                         INTEGRATE.IFW_RATEPLAN@mmtdb rp
                   WHERE NOT EXISTS (SELECT 1
                                       FROM rateplan_t r
                                      WHERE r.rateplan_code = NVL(o.rp_code,'0000000000'))
                     AND o.rp_code = rp.code                  
                   GROUP BY o.rp_code, rp.NAME                 
                 )
    LOOP
    
       -- добавляем ТП
        l_RP_Id := pk17_rateplane.Add_rateplan(
                       p_rateplan_id    => NULL,  -- ID тарифного планы в системе ведения тарифов
                       p_tax_incl       => 'Y',     -- Налоги включены в ТП: "Y/N"
                       p_rateplan_name  => NVL(l_cur.NAME,'Неизвестный'), -- имя тарифного плана
                       p_ratesystem_id  => NULL,   -- ID платежной системы
                       p_service_id     => NULL,  -- ID услуги
                       p_subservice_id  => NULL,                       
                       p_rateplan_code  => NVL(l_cur.RP_Code,'0000000000')
                   );    
    
    END LOOP;                                  
     
   
    -- получаем id тарифных планов по трешеру
    UPDATE TMP05_ORDER o
       SET o.rateplan_id = (SELECT r.rateplan_id
                            FROM rateplan_t r 
                           WHERE r.rateplan_code = o.rp_code);
    
   -- обновляем заказы, у которых были изменения 
    UPDATE order_t o
       SET (o.date_from, o.date_to, o.rateplan_id, modify_date) =
                  (SELECT t.date_from, t.date_to, t.rateplan_id, l_Calc_Date
                     FROM TMP05_ORDER t
                    WHERE o.order_no = t.order_no
                  )         
     WHERE EXISTS (SELECT 1
                     FROM TMP05_ORDER t
                    WHERE o.order_no = t.order_no
                      AND (o.date_from   != t.date_from OR     
                           o.date_to     != t.date_to   OR
                           o.rateplan_id != t.rateplan_id 
                          )
                  );
                  
    l_Update := SQL%ROWCOUNT;              
                  
    -- добавляем новые             
    l_Count := 0;
      
    FOR l_cur IN (  SELECT account_no, order_no, acc_ch_id,
                           date_from, date_to,
                           rateplan_id, service_id, business_type
                      FROM TMP05_ORDER t
                     WHERE NOT EXISTS (SELECT 1
                                         FROM order_t o
                                        WHERE o.order_no = t.order_no)          
                 )
    LOOP
    
        BEGIN
            -- проверяем наличие л/счета в текущем списке
            SELECT account_id 
              INTO l_Account_Id
              FROM account_t a
             WHERE account_no = l_cur.account_no;
    
            -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
            -- создаем заказ на услуги МГ/МН связи
            l_Order_Id := PK06_ORDER.New_order(
                                  p_account_id  => l_Account_Id,
                                  p_order_no    => l_cur.order_no,
                                  p_service_id  => l_cur.service_id, --PK00_CONST.c_SERVICE_CALL_MGMN,
                                  p_rateplan_id => l_cur.rateplan_id,
                                  p_time_zone   => NULL,
                                  p_date_from   => l_cur.date_from,
                                  p_date_to     => l_cur.date_to,
                                  p_create_date => l_Calc_Date,
                                  p_note        => TO_CHAR(l_cur.acc_ch_id)
                               );
                               
            IF l_cur.service_id = PK00_CONST.c_SERVICE_CALL_MGMN THEN                   
                -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
                -- создаем строку заказа для МГ
                l_Order_Body_Id := PK06_ORDER.Add_subservice(
                                       p_order_id      => l_Order_Id,
                                       p_subservice_id => PK00_CONST.c_SUBSRV_MG,
                                       p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
                                       p_rateplan_id   => l_cur.rateplan_id,
                                       p_date_from     => l_cur.date_from,
                                       p_date_to       => l_cur.date_to
                                   );
                -- -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
                -- создаем строку заказа для МН
                l_Order_Body_Id := PK06_ORDER.Add_subservice(
                                       p_order_id      => l_Order_Id,
                                       p_subservice_id => PK00_CONST.c_SUBSRV_MN,
                                       p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
                                       p_rateplan_id   => l_cur.rateplan_id,
                                       p_date_from     => l_cur.date_from,
                                       p_date_to       => l_cur.date_to
                                   );    
             
            ELSIF l_cur.service_id = PK00_CONST.c_SERVICE_CALL_FREE THEN
            
                -- создаем строку заказа для freecall
                l_Order_Body_Id := PK06_ORDER.Add_subservice(
                                       p_order_id      => l_Order_Id,
                                       p_subservice_id => PK00_CONST.c_SUBSRV_FREE,
                                       p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
                                       p_rateplan_id   => l_cur.rateplan_id,
                                       p_date_from     => l_cur.date_from,
                                       p_date_to       => l_cur.date_to
                                   );                
            
            END IF;
                
            l_Count := l_Count + 1;
                
        EXCEPTION
            WHEN no_data_found THEN
            
                IF l_cur.business_type = 1 THEN
                    l_NF_F := l_NF_F + 1; 
                ELSIF l_cur.business_type = 2 THEN
                    l_NF_Y := l_NF_Y + 1;
                END IF;
            
                Pk01_Syslog.write_Msg(p_Msg   => 'Л/счет ' || l_cur.account_no ||
                                                 ' не найден в списке.', 
                                      p_Src   => c_PkgName||'.'||c_prcName,
                                      p_Level => Pk01_Syslog.L_err );
        END;            
        
    END LOOP;                       
                           
    -- итоговый лог по ненаденным л/счетам
    IF l_NF_F > 0 OR l_NF_Y > 0 THEN
        Pk01_Syslog.write_Msg(p_Msg   => 'Итого не найдено л/счетов ' || 
                                         ' физ.лиц ' || TO_CHAR(l_NF_F) ||
                                         ', юр.лиц ' || TO_CHAR(l_NF_Y), 
                              p_Src   => c_PkgName||'.'||c_prcName,
                              p_Level => Pk01_Syslog.L_err );    
    END IF;
    
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - -  -
    -- Проверка данных и логирование ошибок
    ---
     --- Заказы у которых нет ТП
    FOR l_cur IN (SELECT t.acc_ch_id, t.order_no, t.rateplan_id, 
                         t.date_from, t.rp_date_from, t.date_to, t.rp_date_to
                    FROM TMP05_ORDER t
                   WHERE (t.rateplan_id IS NULL OR
                          t.date_from < t.rp_date_from OR 
                          t.date_to > t.rp_date_to)
                     AND EXISTS (SELECT 1
                                   FROM order_t o
                                  WHERE o.modify_date = l_Calc_date
                                    AND o.order_no = t.order_no)  
                   ORDER BY t.rateplan_id NULLS LAST
                 )   
    LOOP

        IF l_cur.rateplan_id IS NULL THEN

            Pk01_Syslog.write_Msg(p_Msg   => 'Не найден ТП для заказа ' || l_cur.order_no ||
                                             ' (Acc.ch.: ' || l_cur.acc_ch_id || ')', 
                                  p_Src   => c_PkgName||'.'||c_prcName,
                                  p_Level => Pk01_Syslog.L_err );
                                  
      /*  ELSIF trunc(l_cur.rp_date_from) > trunc(l_cur.date_from) THEN                   
                                    
            Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                             ' (ACC. ' || l_cur.acc_ch_id || ')' ||
                                             ': дата ТП больше даты заказа ' ||
                                             '(' || TO_CHAR(l_cur.rp_date_from,'dd.mm.yyyy') ||
                                             '>' || TO_CHAR(l_cur.date_from,'dd.mm.yyyy') || ')', 
                                  p_Src   => c_PkgName||'.'||c_prcName,
                                  p_Level => Pk01_Syslog.L_err ); */
                                                                                                                      
        ELSIF TRUNC(l_cur.rp_date_to) < TRUNC(l_cur.date_to) THEN
                                                                                                 
            Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                             ' (Acc. ' || l_cur.acc_ch_id || ')' ||
                                             ': дата окончания ТП меньше даты заказа ' ||
                                             '(' || TO_CHAR(l_cur.rp_date_to,'dd.mm.yyyy') ||
                                             '<' || TO_CHAR(l_cur.date_to,'dd.mm.yyyy') || ')', 
                                  p_Src   => c_PkgName||'.'||c_prcName,
                                  p_Level => Pk01_Syslog.L_err );                       
        END IF;                                  

    END LOOP;
    
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - -  -

    COMMIT;

    Pk01_Syslog.write_Msg(p_Msg => 'Добавлено заказов: ' || TO_CHAR(l_Count) || 
                                   ', обновлено: ' || TO_CHAR(l_Update) ||
                                   ', изм. привязка заказ-л/с: ' || TO_CHAR(l_Upd_Acc), 
                          p_Src => c_PkgName||'.'||c_prcName );               
    
END Load_Orders;


-- Загрузка заказов с услугой зоновой телефонной связи
PROCEDURE Load_Orders_Zone
IS

    c_prcName CONSTANT varchar2(16) := 'Load_Orders_Zone';
    
    l_RP_Id         number;
    l_Calc_Date     date;
    l_Account_Id    number;
    l_Order_Id      number;
    l_Order_Body_Id number;

    l_Count         PLS_INTEGER;
    l_Update        number;
    l_Upd_Acc       number;
    
    l_NF_F          PLS_INTEGER := 0;
    l_NF_Y          PLS_INTEGER := 0;
    
BEGIN

    Pk01_Syslog.write_Msg(p_Msg => 'Обновления данных по заказам.', 
                          p_Src => c_PkgName||'.'||c_prcName );               


    l_Calc_Date := SYSDATE;

    -- очищаем временную таблицу
    DELETE FROM TMP05_ORDER;

    -- загружаем действующие заказы
    INSERT INTO TMP05_ORDER
           (account_no, acc_ch_id, order_no, date_from, date_to,
            prf_poid_id0, prf_revision,
            service_id,
            business_type)
    SELECT ap.account_no,
           ac.poid_id0,
           pci.order_num order_no,
           TRUNC (u2d (MIN(p.effective_t))) order_date,
           gc_MaxDate,
           p.poid_id0 prf_poid_id0,
           MAX(aup.au_parent_obj_rev) prf_revision,
           DECODE(s.poid_type, gc_Service_Type, PK00_CONST.c_SERVICE_CALL_MGMN, 
                               gc_SrvFree_Type, PK00_CONST.c_SERVICE_CALL_FREE,
                               gc_SrvZone_Type, PK00_CONST.c_SERVICE_CALL_ZONE
                 ) service_id,
           ac.business_type      
      FROM pin.account_t@mmtdb ac,
           pin.account_t@mmtdb ap,
           pin.billinfo_t@mmtdb bic,
           pin.billinfo_t@mmtdb bip,
           pin.service_t@mmtdb s,
           pin.profile_t@mmtdb p,
           pin.profile_contract_info_t@mmtdb pci,
           pin.au_profile_t@mmtdb aup 
     WHERE ac.business_type IN (1, 2) -- 1 - физики, 2 - юрики
       AND ac.status != 10103
       AND s.account_obj_id0 = ac.poid_id0
       AND s.poid_type = gc_SrvZone_Type --'/service/telco/gsm/telephony'
       AND p.account_obj_id0 = ac.poid_id0
       AND pci.obj_id0 = p.poid_id0
       AND ac.poid_id0 = bic.account_obj_id0
       AND bic.ar_billinfo_obj_id0 = bip.poid_id0
       AND bip.account_obj_id0 = ap.poid_id0
       AND p.poid_id0 = aup.au_parent_obj_id0(+)
       AND EXISTS (SELECT 1
                     FROM pin.service_alias_list_t@mmtdb sal
                    WHERE sal.obj_id0 = s.poid_id0)   
    GROUP BY ap.account_no,
             ac.poid_id0,
             pci.order_num,
             p.poid_id0,
             s.poid_type, ac.business_type;
     
    l_Count := SQL%ROWCOUNT; 
    
   -- загружаем историю по действующим          
    INSERT INTO TMP05_ORDER
           (account_no, acc_ch_id, order_no, date_from, date_to, 
            prf_poid_id0, prf_revision,
            service_id, business_type)
    SELECT o.account_no,
           o.acc_ch_id,
           apc.order_num order_no,
           TRUNC (u2d (aup.effective_t)) date_from,
           NULL date_to,
           aup.poid_id0 poid_id0,
           aup.au_parent_obj_rev au_prf_revision,
           o.service_id, o.business_type
      FROM TMP05_ORDER o,
           pin.au_profile_t@mmtdb aup,
           pin.au_profile_contract_info_t@mmtdb apc
     WHERE o.prf_poid_id0 = aup.au_parent_obj_id0
       AND aup.poid_id0 = apc.obj_id0
       AND o.prf_revision != aup.au_parent_obj_rev
     GROUP BY o.account_no, o.acc_ch_id, o.date_from, apc.order_num, aup.effective_t, o.prf_poid_id0,
              aup.poid_id0, aup.au_parent_obj_rev, 
              o.service_id, o.business_type;
          
    l_Count := l_Count + SQL%ROWCOUNT; 
     
   -- проставляем дату окончания заказам из истории
    MERGE INTO TMP05_ORDER t  
    USING (SELECT rd, new_date_to
             FROM (
                   SELECT rowid rd,
                          lead(date_from-1/86400) OVER (PARTITION BY account_no, acc_ch_id 
                                                            ORDER BY prf_revision) new_date_to,
                          date_to                                     
                     FROM TMP05_ORDER 
                  )
            WHERE date_to IS NULL
          ) tt
       ON (t.rowid = tt.rd)
   WHEN MATCHED THEN UPDATE
    SET t.date_to = tt.new_date_to;                                      
                                         
   -- удалем все некорректные данные 
    DELETE FROM TMP05_ORDER
     WHERE date_from > date_to;
             
    l_Count := l_Count - SQL%ROWCOUNT; 
     
   ---- 
   -- схлопывание диапазонов (много одних и тех же заказов идущих друг за другом подряд)
   
   -- 1. Проставляем минимальную дату начала
    MERGE INTO TMP05_ORDER t
    USING (
           SELECT oo.rd, oo.grp_date
             FROM (
                    SELECT oo.rd, oo.date_from,
                           (date_to - sm) - NUMTODSINTERVAL(rn-1,'SECOND') grp_date,
                           row_number() OVER (PARTITION BY account_no, acc_ch_id, order_no, (date_to - sm) - NUMTODSINTERVAL(rn-1,'SECOND') 
                                                  ORDER BY date_from DESC) rn_grp
                      FROM (
                            SELECT o.rowid rd, 
                                   o.account_no, o.acc_ch_id, o.order_no, o.date_from, o.date_to,
                                   row_number() OVER (PARTITION BY account_no, acc_ch_id, order_no ORDER BY date_from ASC) rn,
                                   SUM(date_to - date_from) OVER (PARTITION BY account_no, acc_ch_id, order_no ORDER BY date_from) sm
                              FROM TMP05_ORDER o
                           ) oo
                  ) oo         
            WHERE rn_grp = 1
              AND grp_date < date_from
         ) tt              
      ON (t.rowid = tt.rd)
    WHEN MATCHED THEN UPDATE
     SET t.date_from = tt.grp_date;
             
   -- 2. удаляем излишки  
    DELETE FROM TMP05_ORDER t
     WHERE EXISTS (SELECT 1
                     FROM TMP05_ORDER tt
                    WHERE t.rowid != tt.rowid
                      AND t.account_no = tt.account_no   
                      AND t.acc_ch_id  = tt.acc_ch_id
                      AND t.order_no   = tt.order_no
                      AND t.date_from  >= tt.date_from
                      AND t.date_to    < tt.date_to
                  );
    
    l_Count := l_Count - SQL%ROWCOUNT;
    
   -------------------------------- 
   -- загружаем закрытые заказы
    INSERT INTO TMP05_ORDER
           (account_no, acc_ch_id, order_no, date_from, date_to,
            service_id, business_type)   
    SELECT ap.account_no,
           ac.poid_id0,
           pci.order_num order_no,
           TRUNC (pin.u2d@mmtdb(MIN(p.effective_t))) order_date_from,
           TRUNC (pin.u2d@mmtdb(MIN(ac.last_status_t))) + 
                               INTERVAL '00 23:59:59' DAY TO SECOND order_date_to,
           DECODE(s.poid_type, gc_Service_Type, PK00_CONST.c_SERVICE_CALL_MGMN, 
                               gc_SrvFree_Type, PK00_CONST.c_SERVICE_CALL_FREE,
                               gc_SrvZone_Type, PK00_CONST.c_SERVICE_CALL_ZONE
                 ) service_id,
           ac.business_type                
      FROM pin.account_t@mmtdb ac,
           pin.account_t@mmtdb ap,
           pin.billinfo_t@mmtdb bic,
           pin.billinfo_t@mmtdb bip,
           pin.service_t@mmtdb s,
           pin.profile_t@mmtdb p,
           pin.profile_contract_info_t@mmtdb pci
     WHERE ac.business_type IN (1, 2) -- 1 - физики, 2 - юрики
       AND ac.status = 10103
       AND ac.poid_id0 = bic.account_obj_id0
       AND bic.ar_billinfo_obj_id0 = bip.poid_id0
       AND bip.account_obj_id0 = ap.poid_id0   
       AND s.poid_type = gc_SrvZone_Type -- '/service/telco/gsm/telephony'   
       AND ac.poid_id0 = s.account_obj_id0 
       AND ac.poid_id0 = p.account_obj_id0 
       AND p.poid_id0 = pci.obj_id0
       AND ac.last_status_t >= 1254355200 -- 01.10.2009
       AND (EXISTS (SELECT 1
                      FROM pin.service_alias_list_t@mmtdb sal
                     WHERE sal.obj_id0 = s.poid_id0)     
             OR
            EXISTS (SELECT 1
                      FROM pin.au_service_t@mmtdb aus,
                           pin.au_service_alias_list_t@mmtdb sal
                     WHERE aus.poid_id0 = sal.obj_id0
                       AND aus.au_parent_obj_id0 = s.poid_id0)         
          )            
    GROUP BY ap.account_no, ac.account_no, ac.poid_id0,
             pci.order_num, s.poid_type, ac.business_type  
    HAVING NOT EXISTS (SELECT 1
                         FROM TMP05_ORDER o
                        WHERE o.order_no = pci.order_num)
       AND EXISTS (SELECT 1
                     FROM account_t a
                    WHERE /*a.account_type != PK00_CONST.C_ACC_TYPE_J
                      AND*/ a.account_no = ap.account_no);   

    l_Count := l_Count + SQL%ROWCOUNT;

    -- удаляем задвои
    DELETE FROM TMP05_ORDER o
     WHERE rowid IN (SELECT rd
                           FROM (
                                 SELECT rowid rd,
                                        row_number() OVER (PARTITION BY order_no ORDER BY rp_date_from DESC) rn 
                                   FROM TMP05_ORDER t
                                )   
                          WHERE rn > 1  
                        );
                        
    l_Count := l_Count - SQL%ROWCOUNT;                        

   -- удалем все некорректные данные 
    DELETE FROM TMP05_ORDER
     WHERE date_from > date_to;
     
    l_Count := l_Count - SQL%ROWCOUNT; 

    Pk01_Syslog.write_Msg(p_Msg => 'Найдено заказов в НБ: ' || TO_CHAR(l_Count), 
                          p_Src => c_PkgName||'.'||c_prcName );               


    --- ================================================
    -- Проверка совпадения л/счетов у заказа
/*    UPDATE ORDER_T o
       SET (account_id, notes, modify_date) = 
                                (SELECT a.account_id, TO_CHAR(t.acc_ch_id), l_Calc_Date
                                   FROM TMP05_ORDER t,
                                        ACCOUNT_T a
                                  WHERE t.account_no = a.account_no
                                    AND t.order_no = o.order_no    
                                    AND (a.account_id != o.account_id
                                          OR
                                         NVL(o.notes,'-1') != TO_CHAR(t.acc_ch_id)
                                        )  
                                 )     
    WHERE EXISTS (SELECT 1
                    FROM TMP05_ORDER t,
                         ACCOUNT_T a
                   WHERE t.account_no = a.account_no
                     AND t.order_no = o.order_no    
                     AND (a.account_id != o.account_id
                           OR
                          NVL(o.notes,'-1') != TO_CHAR(t.acc_ch_id)
                         )  
                  );     
    
    l_Upd_Acc := SQL%ROWCOUNT;*/
        
    -- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    -- получаем ТП
    ----
    --   получаем индивидуальный ТП (делаем через пром. таблицу - так намного меньше тормозов из-за линков
    DELETE FROM TMP09_ORDER_PH;

    INSERT INTO TMP09_ORDER_PH
    (account_id, rp_code, date_from, date_to)                   
    SELECT p.account_obj_id0,
           rp.code,
               u2d(pae.valid_from) date_from,
               NVL(u2d(pae.valid_to), gc_MaxDate) date_to 
          FROM pin.profile_t@mmtdb p, 
               pin.profile_acct_extrating_data_t@mmtdb pae,
               INTEGRATE.IFW_RATEPLAN@mmtdb rp
         WHERE p.NAME = 'RATEPLAN' 
           AND pae.obj_id0 = p.poid_id0
           AND rp.code = pae.VALUE;  


    UPDATE TMP05_ORDER o
           SET (rp_code, rp_date_from, rp_date_to) = 
                         (SELECT t.rp_code,
                                 t.date_from,
                                 t.date_to 
                            FROM TMP09_ORDER_PH t
                           WHERE t.account_id = o.acc_ch_id
                             AND o.date_from < t.date_to 
                             AND o.date_to   > t.date_from                   
                         )
          WHERE o.rp_code IS NULL
            AND EXISTS (SELECT 1 
                          FROM TMP09_ORDER_PH t
                         WHERE t.account_id = o.acc_ch_id
                             AND o.date_from < t.date_to 
                             AND o.date_to   > t.date_from                    
                       );
 
    
     -- получаем ТП для л/с, у которых нет индивидуального    
     -- (делаем через пром. таблицу - так намного меньше тормозов из-за линков)
    DELETE FROM TMP09_ORDER_PH;

    INSERT INTO TMP09_ORDER_PH
    (account_id, rp_code, date_from, date_to)
        SELECT pp.account_obj_id0,
               pum.rate_plan_name code,
               U2D(pp.usage_start_t) date_from,
               NVL(U2D(pp.usage_end_t),gc_MaxDate) date_to
          FROM pin.purchased_product_t@mmtdb pp, 
               pin.product_usage_map_t@mmtdb pum,
               pin.service_t@mmtdb s
         WHERE 1=1
           AND (pp.usage_start_t < pp.usage_end_t OR pp.usage_end_t IS NULL OR pp.usage_end_t = 0)
           AND pp.product_obj_id0 = pum.obj_id0 
           AND pum.event_type = '/event/delayed/session/telco/gsm'
           AND s.poid_type = gc_SrvZone_Type
           AND pp.service_obj_id0 = s.poid_id0;

                       
    MERGE INTO TMP05_ORDER o
     USING      
            (SELECT rd, rp_code, date_from, date_to
              FROM 
                   (
                    SELECT o.rowid rd, -- order_no, который в partition, уникальный, посему берем для скорости rowid 
                           t.rp_code,
                           t.date_from,
                           t.date_to,
                           row_number() OVER (PARTITION BY o.order_no
                                                  ORDER BY t.date_from DESC) rn                  
                      FROM TMP09_ORDER_PH t,
                           TMP05_ORDER o
                     WHERE 1=1
                       AND o.rp_code IS NULL
                       AND o.acc_ch_id = t.account_id 
                       AND o.date_from < t.date_to 
                       AND o.date_to   >= t.date_from  
                   ) t 
              WHERE t.RN = 1
            ) t
      ON (o.rowid = t.rd)
    WHEN MATCHED THEN UPDATE          
    SET o.rp_code = t.rp_code, 
        o.rp_date_from = t.date_from, 
        o.rp_date_to = t.date_to;                                      
     
    -- тарифы из таблиц аудита
    -- (делаем через пром. таблицу - так намного меньше тормозов из-за линков)
    DELETE FROM TMP09_ORDER_PH;
     
    INSERT INTO TMP09_ORDER_PH
    (account_id, rp_code, date_from, date_to)    
    SELECT --o.rowid rd,
           pp.account_obj_id0,
           pum.rate_plan_name code,
           U2D(pp.usage_start_t) date_from,
           NVL(U2D(pp.usage_end_t), gc_MaxDate) date_to
      FROM --TMP05_ORDER o,
           pin.au_purchased_product_t@mmtdb pp, 
           pin.product_usage_map_t@mmtdb pum,
           pin.service_t@mmtdb s
     WHERE 1=1
       --AND o.acc_ch_id = pp.account_obj_id0 
       AND (pp.usage_start_t < pp.usage_end_t OR pp.usage_end_t IS NULL OR pp.usage_end_t = 0)
     --  AND pp.account_obj_id0 = l_cur.acc_ch_id
       AND pp.product_obj_id0 = pum.obj_id0 
       AND pum.event_type = '/event/delayed/session/telco/gsm'
       AND s.poid_type = gc_SrvZone_Type
       AND pp.service_obj_id0 = s.poid_id0;

    MERGE INTO TMP05_ORDER o
     USING      
            (SELECT rd, rp_code, date_from, date_to
              FROM 
                   (
                    SELECT o.rowid rd, -- order_no, который в partition, уникальный, посему берем для скорости rowid
                           t.rp_code,
                           t.date_from,
                           t.date_to,
                           row_number() OVER (PARTITION BY o.order_no
                                                  ORDER BY t.date_from DESC) rn                  
                      FROM TMP09_ORDER_PH t,
                           TMP05_ORDER o
                     WHERE 1=1
                       AND o.rp_code IS NULL
                       AND o.acc_ch_id = t.account_id 
                       AND o.date_from < t.date_to 
                       AND o.date_to   >= t.date_from  
                   ) t 
              WHERE t.RN = 1
            ) t
      ON (o.rowid = t.rd)
    WHEN MATCHED THEN UPDATE          
    SET o.rp_code = t.rp_code, 
        o.rp_date_from = t.date_from, 
        o.rp_date_to = t.date_to;      
     
     
    -- Проверка - если есть у заказа разные ТП, то это ошибка
    FOR l_cur IN (SELECT o.order_no, COUNT(1) cnt
                    FROM (
                         SELECT o.order_no, rp_code
                            FROM TMP05_ORDER o
                           GROUP BY o.order_no, rp_code
                         ) o
                   GROUP BY o.order_no       
                   HAVING COUNT(1) > 1
                )
    LOOP                         

        Pk01_Syslog.write_Msg(p_Msg   => 'У заказа ' || l_cur.order_no ||
                                         ' найдено ' || TO_CHAR(l_cur.cnt) || ' тар. планов)', 
                              p_Src   => c_PkgName||'.'||c_prcName,
                              p_Level => Pk01_Syslog.L_err );
                
    END LOOP;    

    -- = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    --- Догружаем ТП, которых еще нет в ЦБ    
    FOR l_cur IN (SELECT o.rp_code, rp.NAME
                    FROM TMP05_ORDER o,
                         INTEGRATE.IFW_RATEPLAN@mmtdb rp
                   WHERE NOT EXISTS (SELECT 1
                                       FROM rateplan_t r
                                      WHERE r.rateplan_code = NVL(o.rp_code,'0000000000'))
                     AND o.rp_code = rp.code                  
                   GROUP BY o.rp_code, rp.NAME                 
                 )
    LOOP
    
       -- добавляем ТП
        l_RP_Id := pk17_rateplane.Add_rateplan(
                       p_rateplan_id    => NULL,  -- ID тарифного планы в системе ведения тарифов
                       p_tax_incl       => 'Y',     -- Налоги включены в ТП: "Y/N"
                       p_rateplan_name  => NVL(l_cur.NAME,'Неизвестный'), -- имя тарифного плана
                       p_ratesystem_id  => NULL,   -- ID платежной системы
                       p_service_id     => NULL,  -- ID услуги
                       p_subservice_id  => NULL,                       
                       p_rateplan_code  => NVL(l_cur.RP_Code,'0000000000')
                   );    
    
    END LOOP;                                  
     
   
    -- получаем id тарифных планов по трешеру
    UPDATE TMP05_ORDER o
       SET o.rateplan_id = (SELECT r.rateplan_id
                            FROM rateplan_t r 
                           WHERE r.rateplan_code = o.rp_code);
    
   -- обновляем заказы, у которых были изменения 
    UPDATE order_body_t b
       SET (b.date_from, b.date_to, b.rateplan_id, b.modify_date) =
                  (SELECT t.date_from, t.date_to, t.rateplan_id, l_Calc_Date
                     FROM TMP05_ORDER t,
                          ORDER_T o
                    WHERE o.order_no = t.order_no
                      AND o.order_id = b.order_id 
                  )         
     WHERE b.subservice_id = PK00_CONST.c_SUBSRV_ZONE
       AND EXISTS (SELECT 1
                     FROM TMP05_ORDER t,
                          ORDER_T o
                    WHERE o.order_no = t.order_no
                      AND o.order_id = b.order_id
                      AND (b.date_from   != t.date_from OR     
                           b.date_to     != t.date_to   OR
                           b.rateplan_id != t.rateplan_id 
                          )
                  );
                  
    l_Update := SQL%ROWCOUNT;              
                  
    -- добавляем новые             
    l_Count := 0;
      
    FOR l_cur IN (  SELECT o.order_id, 
                           t.date_from, t.date_to,
                           t.rateplan_id 
                      FROM TMP05_ORDER t,
                           order_t o
                     WHERE o.order_no = t.order_no
                       AND NOT EXISTS (SELECT 1
                                         FROM order_body_t b
                                        WHERE b.subservice_id = PK00_CONST.c_SUBSRV_ZONE
                                          AND b.order_id = o.order_id)          
                 )
    LOOP

        l_Order_Body_Id := PK06_ORDER.Add_subservice(
                                       p_order_id      => l_cur.order_id,
                                       p_subservice_id => PK00_CONST.c_SUBSRV_ZONE,
                                       p_charge_type   => PK00_CONST.c_CHARGE_TYPE_USG,
                                       p_rateplan_id   => l_cur.rateplan_id,
                                       p_date_from     => l_cur.date_from,
                                       p_date_to       => l_cur.date_to
                                   );
    
        l_Count := l_Count + 1;
                
    END LOOP;                       
                           
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - -  -
    -- Проверка данных и логирование ошибок
    ---
     --- Заказы у которых нет ТП
    FOR l_cur IN (SELECT t.acc_ch_id, t.order_no, t.rateplan_id, 
                         t.date_from, t.rp_date_from, t.date_to, t.rp_date_to
                    FROM TMP05_ORDER t
                   WHERE (t.rateplan_id IS NULL OR
                          t.date_from < t.rp_date_from OR 
                          t.date_to > t.rp_date_to)
                     AND EXISTS (SELECT 1
                                   FROM order_t o
                                  WHERE o.modify_date = l_Calc_date
                                    AND o.order_no = t.order_no)  
                   ORDER BY t.rateplan_id NULLS LAST
                 )   
    LOOP

        IF l_cur.rateplan_id IS NULL THEN

            Pk01_Syslog.write_Msg(p_Msg   => 'Не найден ТП для заказа ' || l_cur.order_no ||
                                             ' (Acc.ch.: ' || l_cur.acc_ch_id || ')', 
                                  p_Src   => c_PkgName||'.'||c_prcName,
                                  p_Level => Pk01_Syslog.L_err );
                                  
      /*  ELSIF trunc(l_cur.rp_date_from) > trunc(l_cur.date_from) THEN                   
                                    
            Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                             ' (ACC. ' || l_cur.acc_ch_id || ')' ||
                                             ': дата ТП больше даты заказа ' ||
                                             '(' || TO_CHAR(l_cur.rp_date_from,'dd.mm.yyyy') ||
                                             '>' || TO_CHAR(l_cur.date_from,'dd.mm.yyyy') || ')', 
                                  p_Src   => c_PkgName||'.'||c_prcName,
                                  p_Level => Pk01_Syslog.L_err ); */
                                                                                                                      
        ELSIF TRUNC(l_cur.rp_date_to) < TRUNC(l_cur.date_to) THEN
                                                                                                 
            Pk01_Syslog.write_Msg(p_Msg   => 'Заказ ' || l_cur.order_no ||
                                             ' (Acc. ' || l_cur.acc_ch_id || ')' ||
                                             ': дата окончания ТП меньше даты заказа ' ||
                                             '(' || TO_CHAR(l_cur.rp_date_to,'dd.mm.yyyy') ||
                                             '<' || TO_CHAR(l_cur.date_to,'dd.mm.yyyy') || ')', 
                                  p_Src   => c_PkgName||'.'||c_prcName,
                                  p_Level => Pk01_Syslog.L_err );                       
        END IF;                                  

    END LOOP;
    
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - -  -

    COMMIT;

    Pk01_Syslog.write_Msg(p_Msg => 'Добавлено заказов: ' || TO_CHAR(l_Count) || 
                                   ', обновлено: ' || TO_CHAR(l_Update) ||
                                   ', изм. привязка заказ-л/с: ' || TO_CHAR(l_Upd_Acc), 
                          p_Src => c_PkgName||'.'||c_prcName );               
    
END Load_Orders_Zone;




-- Процедура для добавления новых (которые появились в новом биллинге и нет в трешере)
-- лицевых счетов
PROCEDURE Add_Accounts
IS

    c_prcName CONSTANT varchar2(16) := 'Add_Accounts';
    
    l_Count         PLS_INTEGER;
    l_Account_Id    number;
    l_Contractor_Id number; 
    l_Subscriber_Id number;
    l_Parent_Id     number;
    l_Contract_Id   number;
    l_Result        number;
    v_brand_id      INTEGER;
    
BEGIN

    Pk01_Syslog.write_Msg(p_Msg => 'Добавление новых л/счетов.', 
                          p_Src => c_PkgName||'.'||c_prcName );               
   
   -- загрузка промежуточных данных в mmtdb
    mdv_adm.pk02_export_p.exp_subs_info@mmtdb;     

    COMMIT;

    l_Count := 0;

   -- экспорт данных
    FOR l_cur IN (
       SELECT ACCOUNT_NO, 
              LAST_NAME, FIRST_NAME, MIDDLE_NAME, 
              CONTRACT_NO, CONTRACT_DATE, 
              BRAND_NAME, SERVICE_PROVIDER, 
              REG_ZIP, REG_REG, REG_CITY, REG_ADDR, 
              BILL_ZIP, BILL_REG, BILL_CITY, BILL_ADDR, 
              SET_ZIP, SET_REG, SET_CITY, SET_ADDR, 
              CONTACT_PHONE, 
              EXT_SOURCE, EXT_ID, 
              ACCOUNT_STATUS  
         FROM MDV_ADM.P_SUBS_INFO_T@MMTDB t 
         WHERE t.ACCOUNT_NO  IS NOT NULL
         --  AND t.CONTRACT_NO IS NOT NULL
           AND NOT EXISTS (SELECT 1
                             FROM account_t a
                            WHERE a.account_no = t.account_no)  
    )
    LOOP
        l_Count := l_Count + 1;
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- CONTRACT_T - создать договор
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        BEGIN
            -- получить ID договора
            SELECT CONTRACT_ID INTO l_Contract_Id
              FROM CONTRACT_T
             WHERE CONTRACT_NO = NVL(l_cur.CONTRACT_NO,'EMPTY');
             
        EXCEPTION WHEN NO_DATA_FOUND THEN
            l_Contract_Id := PK12_CONTRACT.Open_contract(
                                 p_contract_no=> l_cur.Contract_No,
                                 p_date_from  => l_cur.Contract_Date,
                                 p_date_to    => NULL,
                                 p_client_id  => PK00_CONST.c_CLIENT_PERSON_ID,
                                 p_manager_id => pk00_const.c_MANAGER_SIEBEL_ID
                               );
        END;
        
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- Создаем клиента Физ.лицо
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        l_Subscriber_Id := PK21_SUBSCRIBER.New_subscriber(
               p_last_name   => l_cur.last_name,   -- фамилия
               p_first_name  => l_cur.first_name,   -- имя 
               p_middle_name => l_cur.middle_name,  -- отчество
               p_category    => Pk00_Const.c_SUBS_RESIDENT  -- категория 1/2 = резидент/нерезидент
           );

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- Добавить адрес регистрации Физ.лица
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        --v_document_id := PK21_SUBSCRIBER.Add_document(
        --                         p_subscriber_id => v_subscriber_id,
        --                         p_doc_type      => NULL,
        --                         p_doc_serial    => NULL,
        --                         p_doc_no        => NULL,
        --                         p_doc_issuer    => NULL,
        --                         p_doc_issue_date=> NULL
        --\                       );

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- ACCOUNT_T - создать лицевой счет
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        l_Account_Id := PK05_ACCOUNT.New_account(
                                 p_account_no   => l_cur.Account_No,
                                 p_account_type => PK00_CONST.c_ACC_TYPE_P,
                                 p_currency_id  => PK00_CONST.c_CURRENCY_RUB,
                                 p_status       => PK00_CONST.c_ACC_STATUS_BILL,
                                 p_parent_id    => NULL
                               );
        
        -- создаем счета для нового л/с
        pk200_import.New_billinfo (p_account_id => l_Account_id);
        
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- ACCOUNT_PROFILE_T - создать профиль лицевого счета
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- получаем данные о филиале и агенте
        BEGIN
            SELECT CONTRACTOR_ID, PARENT_ID 
              INTO l_Contractor_Id, l_Parent_Id
              FROM CONTRACTOR_T
             WHERE SHORT_NAME = l_cur.Brand_Name;
        EXCEPTION WHEN NO_DATA_FOUND THEN
            -- не нашли - позже исправим
            l_Contractor_Id := NULL;
            l_Parent_Id := NULL;
        END;
            
        -- получаем данные о бренде
        BEGIN
            SELECT BRAND_ID 
              INTO v_brand_id
              FROM BRAND_T
             WHERE BRAND = l_cur.Brand_Name;
        EXCEPTION WHEN NO_DATA_FOUND THEN
            -- не нашли - позже исправим
            v_brand_id := NULL;
        END;
        
        
        -- создать профиль лицевого счета
        l_Result := PK05_ACCOUNT.Set_profile(
                             p_account_id    => l_Account_Id,
                             p_brand_id      => v_brand_id,
                             p_contract_id   => l_Contract_Id,
                             p_customer_id   => PK00_CONST.c_CUSTOMER_PERSON_ID,
                             p_subscriber_id => l_Subscriber_Id,
                             p_contractor_id => pk00_const.c_CONTRACTOR_KTTK_ID,
                             p_branch_id     => l_Parent_Id,
                             p_agent_id      => l_Contractor_Id,
                             p_contractor_bank_id => NULL,
                             p_vat           => Pk00_Const.c_VAT,
                             p_date_from     => l_cur.Contract_Date,
                             p_date_to       => NULL
                           );

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- ACCOUNT_CONTACT_T - добавить адреса на л/с
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
        -- добавить адрес регистрации
        l_Result := PK05_ACCOUNT.Add_address(
                             p_account_id   => l_Account_Id,
                             p_address_type => PK00_CONST.c_ADDR_TYPE_REG,
                             p_country      => 'РФ',
                             p_zip          => l_cur.reg_zip,
                             p_state        => l_cur.reg_reg,
                             p_city         => l_cur.reg_city,
                             p_address      => l_cur.reg_addr,
                             p_person       => l_cur.last_name||' '||l_cur.first_name||' '||l_cur.middle_name,
                             p_phones       => l_cur.contact_phone,
                             p_fax          => NULL,
                             p_email        => NULL,
                             p_date_from    => l_cur.Contract_Date,
                             p_date_to      => NULL
                          );
                          
        -- добавить адрес доставки счета
        l_Result := PK05_ACCOUNT.Add_address(
                             p_account_id   => l_Account_Id,
                             p_address_type => PK00_CONST.c_ADDR_TYPE_DLV,
                             p_country      => 'РФ',
                             p_zip          => l_cur.bill_zip,
                             p_state        => l_cur.bill_reg,
                             p_city         => l_cur.bill_city,
                             p_address      => l_cur.bill_addr,
                             p_person       => l_cur.last_name||' '||l_cur.first_name||' '||l_cur.middle_name,
                             p_phones       => l_cur.contact_phone,
                             p_fax          => NULL,
                             p_email        => NULL,
                             p_date_from    => l_cur.Contract_Date,
                             p_date_to      => NULL
                          );
                                  
        -- добавить адрес установки оборудования
        l_Result := PK05_ACCOUNT.Add_address(
                             p_account_id   => l_Account_Id,
                             p_address_type => PK00_CONST.c_ADDR_TYPE_SET,
                             p_country      => 'РФ',
                             p_zip          => l_cur.set_zip,
                             p_state        => l_cur.set_reg,
                             p_city         => l_cur.set_city,
                             p_address      => l_cur.set_addr,
                             p_person       => l_cur.last_name||' '||l_cur.first_name||' '||l_cur.middle_name,
                             p_phones       => NULL,
                             p_fax          => NULL,
                             p_email        => NULL,
                             p_date_from    => l_cur.Contract_Date,
                             p_date_to      => NULL
                          );
               

    END LOOP;       

    COMMIT;

    Pk01_Syslog.write_Msg(p_Msg => 'Добавлено л/счетов: ' || TO_CHAR(l_Count), 
                          p_Src => c_PkgName||'.'||c_prcName );               
    
END Add_Accounts;



-- для тарификации - загрузка и обработка номерных емкостей из схемы X07
PROCEDURE Load_Oper_Phones
IS
    c_prcName CONSTANT varchar2(32) := 'Load_Oper_Phones'; 

    c_Mask_Length CONSTANT number := 6; 

    l_Curr_Date date;
    l_Count     PLS_INTEGER;

BEGIN

    l_Curr_Date := SYSDATE;

    pin.Pk01_Syslog.write_Msg( p_Msg => 'START',    
                               p_Src => c_PkgName||'.'||c_prcName );
                      
   -- = = = = = = = = = = = = = = = = = = = = = = = = = = =         
   -- Расходные тарифы 
   -- = = = = = = = = = = = = = = = = = = = = = = = = = = =
    -- вносим данные об изменениях в номерах расходных тарифов таблицу аудита 
    INSERT INTO PIN.RSX07_ORD_SERVICE_R_TM_AUDIT
                (rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
                 op_sw_id, srv_id, date_from, date_to,
                 action, date_save)     
    WITH t_Phones AS (SELECT o.rec_id, o.op_rate_plan_id, t.mask_value, o.phone_from, o.phone_to,
                             o.op_sw_id, o.srv_id, o.date_from, o.date_to
                         FROM TABLE(CAST(mdv.pck_tools.MaskNumFixLng(
                                           CURSOR(SELECT phone_from, phone_to
                                                    FROM x07_ord_service_r_t
                                                   GROUP BY phone_from, phone_to  
                                                  ), pin.pk00_const.c_Mask_Length
                                         ) AS MDV.GTYPE_MASK_NUM
                                    )) t,
                              x07_ord_service_r_t o      
                        WHERE o.phone_from = t.start_value
                          AND o.phone_to = t.end_value         
                       ) 
    SELECT rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
           op_sw_id, srv_id, date_from, date_to,  
           gc_Delete action, l_Curr_Date date_save
      FROM (
            SELECT rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
                   op_sw_id, srv_id, date_from, date_to  
              FROM RSX07_ORD_SERVICE_R_TM
            MINUS       
            SELECT rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
                   op_sw_id, srv_id, date_from, date_to  
              FROM t_Phones
           )
    UNION ALL
    SELECT rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
           op_sw_id, srv_id, date_from, date_to,  
           gc_Insert action, l_Curr_Date 
      FROM (
            SELECT rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
                   op_sw_id, srv_id, date_from, date_to  
              FROM t_Phones
            MINUS                
            SELECT rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
                   op_sw_id, srv_id, date_from, date_to  
              FROM RSX07_ORD_SERVICE_R_TM
           );
                  
    pin.Pk01_Syslog.write_Msg(p_Msg => 'Расход: добавлено в аудит ' || TO_CHAR(SQL%ROWCOUNT) || ' записей', 
                              p_Src => c_PkgName||'.'||c_prcName );                 
      
    -- удаляем устаревшие данные   
    DELETE FROM RSX07_ORD_SERVICE_R_TM p
     WHERE EXISTS (SELECT 1
                     FROM RSX07_ORD_SERVICE_R_TM_AUDIT a 
                    WHERE a.action           = gc_Delete
                      AND a.date_save        = l_Curr_Date
                      AND a.rec_id           = p.rec_id 
                      AND NVL(a.op_rate_plan_id,-1)  = NVL(p.op_rate_plan_id,-1) 
                      AND NVL(a.mask_value,-1)       = NVL(p.mask_value,-1) 
                      AND NVL(a.phone_from,'-1')     = NVL(p.phone_from,'-1') 
                      AND NVL(a.phone_to,'-1')       = NVL(p.phone_to,'-1')
                      AND NVL(a.op_sw_id,-1)         = NVL(p.op_sw_id,-1)  
                      AND NVL(a.srv_id,-1)           = NVL(p.srv_id,-1)  
                      AND NVL(a.date_from,gc_MinDate) = NVL(p.date_from,gc_MinDate) 
                      AND NVL(a.date_to,gc_MaxDate)   = NVL(p.date_to,gc_MaxDate)  
                  );

    l_Count := SQL%ROWCOUNT;

    -- вносим новые данные
    INSERT INTO RSX07_ORD_SERVICE_R_TM (
           rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
           op_sw_id, srv_id, date_from, date_to,  
           date_save) 
    SELECT rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
           op_sw_id, srv_id, date_from, date_to,  
           date_save
      FROM RSX07_ORD_SERVICE_R_TM_AUDIT a
     WHERE a.action = gc_Insert
       AND a.date_save = l_Curr_Date;  

    pin.Pk01_Syslog.write_Msg(p_Msg => 'Расход: добавлено ' || TO_CHAR(SQL%ROWCOUNT) || ', удалено ' || TO_CHAR(l_Count) || ' записей, ' ||
                                       'итого: ' || TO_CHAR(SQL%ROWCOUNT+l_Count), 
                              p_Src => c_PkgName||'.'||c_prcName );           

    COMMIT;   
    
    
   -- = = = = = = = = = = = = = = = = = = = = = = = = = = =         
   -- Доходные тарифы 
   -- = = = = = = = = = = = = = = = = = = = = = = = = = = =
    -- вносим данные об изменениях в номерах расходных тарифов таблицу аудита 
    INSERT INTO PIN.RSX07_ORD_SERVICE_D_TM_AUDIT
                (rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
                 switch_id, srv_id, date_from, date_to,
                 action, date_save)     
    WITH t_Phones AS (SELECT o.rec_id, o.op_rate_plan_id, t.mask_value, o.phone_from, o.phone_to,
                             o.switch_id, o.srv_id, o.date_from, o.date_to
                         FROM TABLE(CAST(mdv.pck_tools.MaskNumFixLng(
                                           CURSOR(SELECT phone_from, phone_to
                                                    FROM x07_ord_service_d_t
                                                   GROUP BY phone_from, phone_to  
                                                  ), pin.pk00_const.c_Mask_Length
                                        ) AS MDV.GTYPE_MASK_NUM
                                    )) t,
                              x07_ord_service_d_t o      
                        WHERE o.phone_from = t.start_value
                          AND o.phone_to = t.end_value         
                       ) 
    SELECT rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
           switch_id, srv_id, date_from, date_to,  
           gc_Delete action, l_Curr_Date date_save
      FROM (
            SELECT rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
                   switch_id, srv_id, date_from, date_to  
              FROM RSX07_ORD_SERVICE_D_TM
            MINUS       
            SELECT rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
                   switch_id, srv_id, date_from, date_to  
              FROM t_Phones
           )
    UNION ALL
    SELECT rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
           switch_id, srv_id, date_from, date_to,  
           gc_Insert action, l_Curr_Date 
      FROM (
            SELECT rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
                   switch_id, srv_id, date_from, date_to  
              FROM t_Phones
            MINUS                
            SELECT rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
                   switch_id, srv_id, date_from, date_to  
              FROM RSX07_ORD_SERVICE_D_TM
           );
                  
    pin.Pk01_Syslog.write_Msg(p_Msg => 'Доход: добавлено в аудит ' || TO_CHAR(SQL%ROWCOUNT) || ' записей', 
                              p_Src => c_PkgName||'.'||c_prcName );                 
      
    -- удаляем устаревшие данные   
    DELETE FROM RSX07_ORD_SERVICE_D_TM p
     WHERE EXISTS (SELECT 1
                     FROM RSX07_ORD_SERVICE_D_TM_AUDIT a 
                    WHERE a.action           = gc_Delete
                      AND a.date_save        = l_Curr_Date
                      AND a.rec_id           = p.rec_id 
                      AND NVL(a.op_rate_plan_id,-1)  = NVL(p.op_rate_plan_id,-1) 
                      AND NVL(a.mask_value,-1)       = NVL(p.mask_value,-1) 
                      AND NVL(a.phone_from,'-1')     = NVL(p.phone_from,'-1') 
                      AND NVL(a.phone_to,'-1')       = NVL(p.phone_to,'-1')
                      AND NVL(a.switch_id,-1)        = NVL(p.switch_id,-1)  
                      AND NVL(a.srv_id,-1)           = NVL(p.srv_id,-1)  
                      AND NVL(a.date_from,gc_MinDate) = NVL(p.date_from,gc_MinDate) 
                      AND NVL(a.date_to,gc_MaxDate)   = NVL(p.date_to,gc_MaxDate)  
                  );

    l_Count := SQL%ROWCOUNT;

    -- вносим новые данные
    INSERT INTO RSX07_ORD_SERVICE_D_TM (
           rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
           switch_id, srv_id, date_from, date_to,  
           date_save) 
    SELECT rec_id, op_rate_plan_id, mask_value, phone_from, phone_to,
           switch_id, srv_id, date_from, date_to,  
           date_save
      FROM RSX07_ORD_SERVICE_D_TM_AUDIT a
     WHERE a.action = gc_Insert
       AND a.date_save = l_Curr_Date;  

    pin.Pk01_Syslog.write_Msg(p_Msg => 'Доход: добавлено ' || TO_CHAR(SQL%ROWCOUNT) || ', удалено ' || TO_CHAR(l_Count) || ' записей, ' ||
                                       'итого: ' || TO_CHAR(SQL%ROWCOUNT+l_Count), 
                              p_Src => c_PkgName||'.'||c_prcName );           

    COMMIT;       
    
END Load_Oper_Phones;


END PK100_RESOURCES;
/
